//! Actual staged conditional intents, terminal replay, and committed repair.
use super::*;
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteDispatch, DfsWriteRepair};
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
use crate::*;
use sinomo_core::DfsWriteRequest;
use sinomo_core::node::NodeId;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PATH: &str = "/conditional-native-note.md";
const BASE: &[u8] = b"actual original visible native head";
const STAGED: &[u8] = b"actual original guarded bytes retained while W is pending";

fn setup(name: &str) -> (DfsTestRuntime, Vec<NodeId>, NodeId, NodeId, NodeId) {
    let (native, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let primary = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    let caller = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let third = nodes
        .iter()
        .find(|node| **node != primary && **node != caller)
        .unwrap()
        .clone();
    native.shared_control_plane().unwrap().with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        let expected = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let mut desired = expected.clone();
        upsert_placement_rule(
            &mut desired.subtree_policies,
            normalize_placement_rule(
                "/",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 3)
                    .unwrap(),
            )
            .unwrap(),
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&expected), &desired)
                .unwrap()
        );
    });
    let data = Arc::new(native.data_dir(&caller).clone());
    let host = crate::browser_service::dfs_browser_component_host(&data).unwrap();
    native.runtime_handle().block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data, &native.runtime_handle()),
        )
        .await
        .unwrap()
        .unwrap();
    });
    drop(host);
    (native, nodes, primary, caller, third)
}

fn options(nodes: &[NodeId]) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.to_vec(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    }
}

fn deny_content(native: &DfsTestRuntime, source: &NodeId, target: &NodeId, routes: &[&str]) {
    for route in routes {
        native.install_fault(FaultRule {
            source: Some(source.clone()),
            target: Some(target.clone()),
            route: Some((*route).into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: FaultAction::BudgetExhausted,
        });
    }
}

fn record(native: &DfsTestRuntime, object: &DfsObjectId, operation: &str) -> DfsWriteIntentRecord {
    let inspect = native.metadata_primary_service(PATH).unwrap();
    let records = inspect
        .metadata_backend()
        .unwrap()
        .intents_for_object(object)
        .unwrap()
        .into_iter()
        .filter(|row| row.operation_id.as_deref() == Some(operation))
        .collect::<Vec<_>>();
    assert_eq!(
        records.len(),
        1,
        "actual original operation has one durable intent"
    );
    records[0].clone()
}

fn consume_failed(
    writer: &mut NetworkedDfsService<DfsNodeTransport>,
    dispatch: Result<NetworkedDfsWriteOutcome, DfsError>,
) -> DfsWritePreconditionFailure {
    let error = match dispatch {
        Err(error) => error,
        Ok(_) => writer
            .process_incoming_until_pending_routed_writes_ready(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .expect_err("the original must remain failed"),
    };
    match error {
        DfsError::WritePreconditionFailed { failure } => failure,
        error => panic!("expected original typed precondition failure: {error:?}"),
    }
}

fn export(native: &DfsTestRuntime, caller: &NodeId, name: &str, value: serde_json::Value) {
    std::fs::write(
        native.data_dir(caller).root().join(name),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

#[test]
fn native_late_directory_failure_survives_receipt_compaction_restart_and_exact_replay() {
    let (mut native, nodes, primary, caller, third) = setup("native-late-write-precondition");
    let mut writer = native.client(&caller).unwrap();
    let base = writer
        .posix_write_file(PATH, BASE, options(&nodes))
        .unwrap();
    let planning = writer.namespace_execution.begin_planning();
    let opened = writer
        .posix_open_beneath_no_links_distributed("/", PATH)
        .unwrap();
    let reads = writer.namespace_execution.captured_reads().unwrap();
    let condition = scoped_from_reads(
        "/",
        PATH,
        &reads,
        &base.file_system_id,
        Target::Match {
            expected: P {
                object_id: opened.object_id.clone(),
                version: opened.version.clone(),
            },
        },
    )
    .unwrap();
    drop(planning);
    let publication = DfsWritePublication::ConditionalCurrent {
        precondition: Box::new(condition.clone()),
    };
    let operation = format!("native_late_condition_{}", sinomo_core::Id::generate());
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id.clone(),
        object_id: base.object_id.clone(),
        target_version: crate::intent::stable_write_target_version(
            &caller,
            &base.object_id,
            &operation,
        ),
        requesting_node_id: caller.clone(),
    };
    // The same production-owned manifest capture used by routed writes: actual
    // bytes are durable at the requester, with no inserted metadata or receipt.
    let manifest = writer
        .content()
        .put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            STAGED,
        )
        .unwrap();
    let background = native
        .deny_background_replica_before_claim(&primary)
        .unwrap();
    deny_content(&native, &primary, &caller, &[DFS_DATA_FETCH_BLOB_ROUTE]);
    deny_content(
        &native,
        &primary,
        &third,
        &[DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE],
    );
    let dispatched = writer
        .write_distributed_manifest_with_candidates(
            request.clone(),
            Some(base.version.clone()),
            manifest.content_hash.clone(),
            manifest.size_bytes,
            nodes.clone(),
            DfsReplicationPolicy::new(2, 3).unwrap(),
            DfsDistributedWriteOptions {
                publication: publication.clone(),
                repair: DfsWriteRepair::Background,
                dispatch: DfsWriteDispatch::AwaitResponse,
                operation_id: Some(operation.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        dispatched,
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    writer.process_incoming().unwrap();
    let staged = record(&native, &base.object_id, &operation);
    assert!(matches!(
        staged.status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    ));
    assert_eq!(staged.publication, publication);
    assert_eq!(
        staged.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    let inspect = native.metadata_primary_service(PATH).unwrap();
    let receipts = inspect
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&base.object_id, &request.target_version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|row| row.replica_node_id.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([caller.clone()])
    );
    drop(inspect);
    let mut actor = native.metadata_primary_service("/").unwrap();
    let root_before = actor.posix_stat_distributed("/").unwrap();
    let changed = actor.posix_set_mode("/", root_before.mode ^ 0o010).unwrap();
    assert_ne!(changed.version, root_before.version);
    assert_eq!(actor.posix_read_file_distributed(PATH).unwrap(), BASE);
    drop(actor);
    native.clear_faults();
    drop(background);
    native.request_repair_reconciliation(&primary).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let failed = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "real pending owner did not persist the late mismatch"
        );
        native
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
        let current = record(&native, &base.object_id, &operation);
        if matches!(
            current.status,
            DfsWriteIntentStatus::PreconditionFailed { .. }
        ) {
            break current;
        }
        std::thread::yield_now();
    };
    native.checkpoint_rpc_observation_window().unwrap();
    let failure_preparation = native
        .metadata_primary_service(PATH)
        .unwrap()
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&format!("precondition-failed:{}", failed.intent_id))
        .unwrap()
        .unwrap();
    assert_eq!(
        failure_preparation.status,
        DfsPreparedOperationStatus::Committed
    );
    let DfsWriteIntentStatus::PreconditionFailed { failure } = &failed.status else {
        unreachable!()
    };
    assert_eq!(failure.path(), "/");
    let received = consume_failed(&mut writer, Ok(dispatched));
    assert_eq!(&received, failure);
    let mut actor = native.metadata_primary_service("/").unwrap();
    let restored = actor.posix_set_mode("/", root_before.mode).unwrap();
    // Restoration is a real operation with a newer version, not a forged old
    // generation. Exact replay must keep the original durable failure cause.
    assert_ne!(restored.version, changed.version);
    assert_eq!(actor.posix_read_file_distributed(PATH).unwrap(), BASE);
    let gc = actor
        .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
            Duration::ZERO,
            1,
        ))
        .unwrap();
    assert!(gc.compacted_intent_count > 0);
    let compact = actor
        .metadata_backend()
        .unwrap()
        .get_intent_receipt(&base.object_id, &failed.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(compact.status, failed.status);
    assert_eq!(compact.publication, publication);
    let compacted_tables = actor
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("root"))
        .unwrap()
        .unwrap();
    assert!(
        compacted_tables
            .tables
            .intents
            .iter()
            .all(|intent| intent.intent_id != failed.intent_id)
    );
    assert!(compacted_tables.tables.intent_receipts.contains(&compact));
    drop(actor);
    drop(writer);
    native.stop_node(&primary).unwrap();
    native.reopen_node(primary.clone()).unwrap();
    let mut retry = native.client(&caller).unwrap();
    let replay = retry.write_distributed_manifest_with_candidates(
        request.clone(),
        Some(base.version.clone()),
        manifest.content_hash.clone(),
        manifest.size_bytes,
        nodes,
        DfsReplicationPolicy::new(2, 3).unwrap(),
        DfsDistributedWriteOptions {
            publication: publication.clone(),
            repair: DfsWriteRepair::Background,
            dispatch: DfsWriteDispatch::AwaitResponse,
            operation_id: Some(operation.clone()),
            ..Default::default()
        },
    );
    let replayed_failure = consume_failed(&mut retry, replay);
    assert_eq!(&replayed_failure, failure);
    let mut inspect = native.metadata_primary_service(PATH).unwrap();
    let final_head = inspect.posix_stat_distributed(PATH).unwrap();
    assert_eq!(final_head.object_id, base.object_id);
    assert_eq!(final_head.version, base.version);
    assert_eq!(inspect.posix_read_file_distributed(PATH).unwrap(), BASE);
    let retained = inspect
        .metadata_backend()
        .unwrap()
        .get_intent_receipt(&base.object_id, &failed.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained, compact);
    export(
        &native,
        &caller,
        "late-precondition-result.json",
        serde_json::json!({
            "staged":staged,"actual_staged_receipts":receipts,"changed_root":format!("{changed:?}"),
            "failed":failed,"negative_preparation":failure_preparation,"restored_root":format!("{restored:?}"),"compact_receipt":compact,
            "replayed_failure":replayed_failure,"final_head":format!("{final_head:?}"),
            "qualification":"actual native staged original; real later directory mutation; exact same operation/publication/content after normal compaction and restart"
        }),
    );
    native.checkpoint_rpc_observation_window().unwrap();
    drop(inspect);
    drop(retry);
    native.close().unwrap();
}

#[test]
fn native_committed_guarded_create_replay_and_n_repair_ignore_its_own_parent_touch() {
    committed_guarded_create_and_n_repair(false, false);
}

#[test]
fn native_committed_guarded_create_automatic_n_repair_uses_the_normal_timer() {
    committed_guarded_create_and_n_repair(true, false);
}

#[test]
fn native_committed_main_repair_checkpoint_survives_primary_restart_before_n_repair() {
    committed_guarded_create_and_n_repair(true, true);
}

fn committed_guarded_create_and_n_repair(automatic: bool, restart_primary: bool) {
    let name = if restart_primary {
        "native-restarted-main-committed-write-precondition"
    } else if automatic {
        "native-automatic-committed-write-precondition"
    } else {
        "native-committed-write-precondition"
    };
    let (mut native, nodes, primary, caller, third) = setup(name);
    // Retain the normal Main's newly declared queue fact across shutdown. This
    // existing negative claim seam prevents background dispatch, while the
    // actual Original enrollment/attempt may precede it. It creates no job,
    // receipt, cursor, delivery classification, or fabricated Original.
    let restart_claim_hold = restart_primary.then(|| {
        native
            .deny_background_replica_before_claim(&primary)
            .unwrap()
    });
    deny_content(
        &native,
        &primary,
        &third,
        &[DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE],
    );
    let data = Arc::new(native.data_dir(&caller).clone());
    let backend =
        DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &native.runtime_handle())
            .unwrap();
    // Coherent Ready is not a claim that a newly committed fixture setting
    // has reached this browser yet. Observe its real admitted policy before
    // the original content operation begins; no caller options are supplied.
    let host = crate::browser_service::dfs_browser_component_host(&data).unwrap();
    let configuration_deadline =
        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut observed_configuration = None;
    let mut configuration_error = None;
    let expected_configuration_ready = loop {
        let readiness = native.runtime_handle().block_on(async {
            tokio::time::timeout_at(
                configuration_deadline,
                host.ensure_write_configuration_ready(&data, &native.runtime_handle()),
            )
            .await
        });
        match readiness {
            Ok(Ok(())) => {}
            other => {
                configuration_error = Some(format!("{other:?}"));
                break false;
            }
        }
        let attempt = backend.try_with_configured_mutation_service(
            &[PATH],
            (),
            |service, configuration, ()| {
                let options = backend.write_options_for_path_with_configuration(
                    service,
                    PATH,
                    configuration,
                )?;
                let file_system = match configuration {
                    Some(view) => {
                        view.file_system_config(&sifs_file_system_id())
                            .map_err(|error| {
                                sinomo_core::service::ServiceError::Service(error.to_string())
                            })?
                    }
                    None => None,
                };
                Ok((options.policy, options.replica_candidates, file_system))
            },
        );
        match attempt {
            Ok(crate::DfsBrowserMutationAttempt::Complete(observed)) => {
                let ready = observed.0.min_commit_replicas == 2
                    && observed.0.desired_replicas == 3
                    && observed.1.len() == 3
                    && observed
                        .1
                        .iter()
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>()
                        == nodes.iter().cloned().collect();
                observed_configuration = Some(observed);
                if ready {
                    break true;
                }
            }
            Ok(crate::DfsBrowserMutationAttempt::ConfigurationPending(())) => {}
            Err(error) => configuration_error = Some(error.to_string()),
        }
        if tokio::time::Instant::now() >= configuration_deadline {
            break false;
        }
        std::thread::yield_now();
    };
    let configuration_rpc_records = native.checkpoint_rpc_observation_window().unwrap();
    if configuration_rpc_records > 0 {
        native
            .export_latest_rpc_observation_checkpoint(
                &native
                    .data_dir(&caller)
                    .root()
                    .join("committed-precondition-configuration-rpc.json"),
            )
            .unwrap();
    }
    export(
        &native,
        &caller,
        "committed-precondition-configuration.json",
        serde_json::json!({
            "ready":expected_configuration_ready,"actual_browser_configuration":observed_configuration,
            "last_error":configuration_error,"checkpointed_rpc_records":configuration_rpc_records,
            "qualification":"bounded preeffect observation of the same real browser configuration and managed options; no supplied write policy or content dispatch"
        }),
    );
    assert!(
        expected_configuration_ready,
        "actual browser must admit the fixture W2/N3 before content dispatch"
    );
    drop(host);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    backend
        .lock_service()
        .unwrap()
        .set_transport_operation_deadline(Some(deadline));
    let created = backend
        .no_links(DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: None,
            root: "/".into(),
            path: PATH.into(),
            operation: DaemonDfsBrowserNoLinksOperation::CreateNew {
                bytes: STAGED.to_vec(),
            },
        })
        .unwrap();
    let DaemonDfsBrowserNoLinksResponse::Metadata { metadata } = created else {
        panic!("actual guarded create: {created:?}")
    };
    assert!(Instant::now() < deadline);
    let object = DfsObjectId::new(&metadata.object_id).unwrap();
    let mut inspect = native.metadata_primary_service(PATH).unwrap();
    let originals = inspect
        .metadata_backend()
        .unwrap()
        .intents_for_object(&object)
        .unwrap();
    let committed = originals
        .into_iter()
        .find(|row| row.target_version.as_str() == metadata.version)
        .unwrap();
    assert_eq!(committed.status, DfsWriteIntentStatus::Committed);
    let condition = committed.publication.current_precondition().unwrap();
    let root_after = inspect.posix_stat_distributed("/").unwrap();
    let (_, root_expected) = directories(condition)
        .into_iter()
        .find(|(path, _)| *path == "/")
        .unwrap();
    assert_ne!(
        root_after.version, root_expected.version,
        "the original create must really invalidate its old parent version by its own touch"
    );
    assert!(
        mismatch(condition, |path| inspect
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&committed.file_system_id, path))
        .unwrap()
        .is_some()
    );
    let original_operation = format!("write:{}", committed.intent_id);
    let prepared = inspect
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original_operation)
        .unwrap()
        .unwrap();
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Committed);
    let replay = inspect
        .commit_stored_prepared_operation(&prepared.digest)
        .unwrap();
    assert!(
        !replay.published,
        "exact committed body replay has no fresh namespace effect"
    );
    assert_eq!(replay.payload, prepared.payload);
    // Keep the actual native read owner open. One ordinary reconciliation wake
    // owns progress; this observer does not launch a second broad tick.
    let queue_path = crate::default_dfs_repair_attempt_snapshot_path(native.data_dir(&primary));
    let queue = LocalDfsRepairQueueStore::new(&queue_path);
    if restart_primary {
        // This is the actual served Main checkpoint, before an explicit wake.
        // Unlike the private pending component finalizer, that execution owns
        // the initial NotSent maps and persists them before releasing its lane.
        let before = queue.view().unwrap();
        let rows = before
            .records
            .iter()
            .filter(|row| row.intent_id == committed.intent_id)
            .cloned()
            .collect::<Vec<_>>();
        let before_original = queue
            .original_repair_for_slot(&committed.intent_id, &third)
            .unwrap();
        let before_payload = before_original
            .as_ref()
            .map(|birth| -> Result<Vec<u8>, String> {
                inspect
                    .content()
                    .inspect_repair_payload_root(&birth.root)
                    .map_err(|error| error.to_string())?;
                let mut reader = inspect
                    .content()
                    .open_retained_repair_payload(&birth.root)
                    .map_err(|error| error.to_string())?;
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut bytes)
                    .map_err(|error| error.to_string())?;
                Ok(bytes)
            })
            .transpose();
        export(
            &native,
            &caller,
            "committed-main-before-restart.json",
            serde_json::json!({
                "committed":committed,"original_preparation":prepared,
                "queue_cut":{"head_sequence":before.head_sequence,"records":before.records},
                "actual_original":before_original,"actual_retained_payload":before_payload,
                "qualification":"actual registered Main committed W2/N3 and persisted positive first Repair declaration; actual immutable Original may already be enrolled/attempted before claim denial; complete native birth and physical retained bytes preserved; C data fault retained through restart; no private finalizer or semantic retry"
            }),
        );
        assert_eq!(
            rows.len(),
            1,
            "normal Main must persist the exact N-W goal before shutdown"
        );
        let row = &rows[0];
        assert_eq!(row.object_id, object);
        assert_eq!(row.version, committed.target_version);
        assert_eq!(Some(&row.content_hash), committed.content_hash.as_ref());
        assert_eq!(row.missing_replica_node_id, third);
        assert_eq!(
            row.transfer_kind,
            Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
        );
        assert_eq!(
            row.transfer_delivery_state,
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
        );
        assert_eq!(row.attempts, 0);
        assert_eq!(row.last_attempt_unix_nanos, 0);
        assert!(!row.in_flight);
        assert!(row.transfer_request_id.is_none() && row.completed_transfer_request_id.is_none());
        assert!(row.next_retry_unix_nanos > 0 && row.next_retry_unix_nanos != u64::MAX);
        let birth = before_original
            .as_ref()
            .expect("normal Main already enrolled the genuine Original");
        assert!(birth.closed_at_sequence.is_none() && birth.observed.is_empty());
        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
            panic!("actual enrolled current Repair must retain its native command")
        };
        assert_eq!(birth.original.source_node_id, primary);
        assert_eq!(birth.original.target_node_id, third);
        assert_eq!(command.intent_id, committed.intent_id);
        assert_eq!(command.file_system_id, committed.file_system_id);
        assert_eq!(command.object_id, object);
        assert_eq!(command.version, committed.target_version);
        assert_eq!(Some(&command.content_hash), committed.content_hash.as_ref());
        assert_eq!(birth.root.subject.request_id, command.request_id);
        assert_eq!(birth.root.content_hash(), command.content_hash);
        assert_eq!(birth.root.size_bytes(), STAGED.len() as u64);
        assert_eq!(before_payload.unwrap().as_deref(), Some(STAGED));
        drop(inspect);
        native.stop_node(&primary).unwrap();
        native.reopen_node(primary.clone()).unwrap();
        inspect = native.metadata_primary_service(PATH).unwrap();
        let after = queue.view().unwrap();
        let after_rows = after
            .records
            .iter()
            .filter(|row| row.intent_id == committed.intent_id)
            .cloned()
            .collect::<Vec<_>>();
        let reopened = inspect
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&original_operation)
            .unwrap()
            .unwrap();
        let after_original = queue
            .original_repair_for_slot(&committed.intent_id, &third)
            .unwrap();
        let after_payload = after_original
            .as_ref()
            .map(|birth| -> Result<Vec<u8>, String> {
                inspect
                    .content()
                    .inspect_repair_payload_root(&birth.root)
                    .map_err(|error| error.to_string())?;
                let mut reader = inspect
                    .content()
                    .open_retained_repair_payload(&birth.root)
                    .map_err(|error| error.to_string())?;
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut bytes)
                    .map_err(|error| error.to_string())?;
                Ok(bytes)
            })
            .transpose();
        export(
            &native,
            &caller,
            "committed-main-after-restart.json",
            serde_json::json!({
                "before_rows":rows,"after_rows":after_rows,
                "queue_cut":{"head_sequence":after.head_sequence,"records":after.records},
                "reopened_original":reopened,"before_native_original":before_original,
                "after_native_original":after_original,"after_retained_payload":after_payload,
                "qualification":"actual stop/reopen, content fault and same before-claim hold retained; no request resubmission or injected queue facts"
            }),
        );
        assert_eq!(
            after_rows, rows,
            "cold reconciliation must retain Main's exact positive declaration"
        );
        assert_eq!(
            reopened, prepared,
            "restart must retain the same committed native body and digest"
        );
        let after_birth = after_original
            .as_ref()
            .expect("cold Main must preserve the same Original");
        assert_eq!(after_birth.original, birth.original);
        assert_eq!(after_birth.attachment, birth.attachment);
        assert_eq!(after_birth.root, birth.root);
        assert_eq!(after_birth.observed, birth.observed);
        assert_eq!(after_birth.closed_at_sequence, birth.closed_at_sequence);
        assert!(
            after_birth.attempts.starts_with(&birth.attempts),
            "restart cannot erase or rewrite an actual earlier Original attempt"
        );
        assert_eq!(after_payload.unwrap().as_deref(), Some(STAGED));
    }
    drop(restart_claim_hold);
    // Close preserves a settled cursor and its historical delivery evidence.
    // This is only a candidate cut; the stopped native history below must prove
    // the same actual Original was closed at or before that exact queue head.
    let nonactive_cursor = |row: &DfsRepairAttemptRecord| {
        row.intent_id == committed.intent_id
            && row.object_id == object
            && row.version == committed.target_version
            && Some(&row.content_hash) == committed.content_hash.as_ref()
            && committed
                .desired_replica_node_ids
                .contains(&row.missing_replica_node_id)
            && row.attempts > 0
            && row.last_attempt_unix_nanos > 0
            && !row.in_flight
            && row.transfer_request_id.is_none()
            && row
                .completed_transfer_request_id
                .as_ref()
                .is_some_and(|id| !id.is_empty())
            && row.next_retry_unix_nanos == 0
            && row.transfer_kind
                == Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
            && row.transfer_delivery_state.is_some()
    };
    let no_active_original_at_cut = |view: &DfsRepairQueueView| {
        committed.desired_replica_node_ids.iter().all(|target| {
            queue
                .original_repair_for_slot(&committed.intent_id, target)
                .unwrap()
                .is_none()
        }) && queue.view().unwrap().head_sequence == view.head_sequence
    };
    // One normal owner discovers the actual missing target. It need not be
    // the fixture's assumed third node, and it may finish before observation.
    native.request_repair_reconciliation(&primary).unwrap();
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (setup_receipts, setup_queue, retry_row, already_complete) = loop {
        let receipts = inspect
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&object, &committed.target_version)
            .unwrap();
        let view = queue.view().unwrap();
        let complete = receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            == nodes.iter().cloned().collect()
            && view
                .records
                .iter()
                .filter(|row| row.intent_id == committed.intent_id)
                .all(nonactive_cursor)
            && no_active_original_at_cut(&view);
        let row = view
            .records
            .iter()
            .filter(|row| {
                row.intent_id == committed.intent_id
                    && row.object_id == object
                    && row.version == committed.target_version
                    && Some(&row.content_hash) == committed.content_hash.as_ref()
                    && committed
                        .desired_replica_node_ids
                        .contains(&row.missing_replica_node_id)
                    && row.attempts > 0
                    && row.next_retry_unix_nanos > 0
                    && row.next_retry_unix_nanos != u64::MAX
            })
            .min_by_key(|row| row.next_retry_unix_nanos)
            .cloned();
        // Logical in-flight state is not a different retry deadline. Preserve
        // the actual carrier's complete DATA rather than require a guessed bit.
        if complete || row.is_some() || Instant::now() >= setup_deadline {
            break (receipts, view, row, complete);
        }
        std::thread::yield_now();
    };
    let setup_rpc_records = native.checkpoint_rpc_observation_window().unwrap();
    if setup_rpc_records > 0 {
        native
            .export_latest_rpc_observation_checkpoint(
                &native
                    .data_dir(&caller)
                    .root()
                    .join("committed-precondition-retry-setup-rpc.json"),
            )
            .unwrap();
    }
    export(
        &native,
        &caller,
        "committed-precondition-retry-setup.json",
        serde_json::json!({
            "original_committed":committed,"actual_retry_row":retry_row,"checkpointed_rpc_records":setup_rpc_records,
            "actual_receipts":setup_receipts,"already_complete":already_complete,
            "queue_cut":{"head_sequence":setup_queue.head_sequence,"records":setup_queue.records},
            "qualification":"one normal reconciliation wake; actual native receipts and all queue rows before qualification; precise settled cursor DATA requires no active Original slot and a stable re-read head, then strict same-Original Close proof below"
        }),
    );
    assert_eq!(committed.policy.min_commit_replicas, 2);
    assert_eq!(committed.policy.desired_replicas, 3);
    assert_eq!(
        committed
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        nodes.iter().cloned().collect()
    );
    assert!(
        already_complete || retry_row.is_some(),
        "the native owner must expose complete N or its actual finite retry deadline"
    );
    let start_unix_nanos = crate::dfs_now_unix_nanos();
    let actual_retry_due_unix_nanos = retry_row.as_ref().map(|row| row.next_retry_unix_nanos);
    let retry_remaining = actual_retry_due_unix_nanos.map_or(Duration::ZERO, |due| {
        Duration::from_nanos(due.saturating_sub(start_unix_nanos))
    });
    let started = Instant::now();
    let normal_interval_allowance = if automatic {
        crate::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
    } else {
        Duration::ZERO
    };
    let action_allowance =
        normal_interval_allowance + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let goal_unix_nanos = actual_retry_due_unix_nanos
        .unwrap_or(start_unix_nanos)
        .saturating_add(u64::try_from(action_allowance.as_nanos()).unwrap());
    let goal_window = Duration::from_nanos(goal_unix_nanos.saturating_sub(start_unix_nanos));
    let deadline = started + goal_window;
    native.clear_faults();
    while actual_retry_due_unix_nanos.is_some_and(|due| crate::dfs_now_unix_nanos() < due) {
        std::thread::yield_now();
    }
    let observe_goal = || {
        loop {
            let receipts = inspect
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&object, &committed.target_version)
                .unwrap();
            let view = queue.view().unwrap();
            let now = Instant::now();
            let goal = receipts
                .iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                == nodes.iter().cloned().collect()
                && view
                    .records
                    .iter()
                    .filter(|row| row.intent_id == committed.intent_id)
                    .all(nonactive_cursor)
                && no_active_original_at_cut(&view);
            if goal || now >= deadline {
                break (
                    receipts,
                    view,
                    goal && now < deadline,
                    now.duration_since(started),
                );
            }
            std::thread::yield_now();
        }
    };
    let (mut native, tick_observation, observed_goal) = if automatic {
        // Heal once and leave progress to the mounted component's real timer.
        // There is no explicit tick or extra notification in this branch.
        let observed_goal = observe_goal();
        (native, None, observed_goal)
    } else {
        let tick_primary = primary.clone();
        std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                let observation = native
                    .maintenance_tick(&tick_primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
                (native, observation)
            });
            let observed_goal = observe_goal();
            // Timestamp the goal before unrelated tick completion or drain.
            let (native, observation) = worker.join().unwrap();
            (native, Some(observation), observed_goal)
        })
    };
    let (repaired_receipts, observed_queue, observed_in_budget, observed_elapsed) = observed_goal;
    let goal_rpc_records = native.checkpoint_rpc_observation_window().unwrap();
    if goal_rpc_records > 0 {
        native
            .export_latest_rpc_observation_checkpoint(
                &native
                    .data_dir(&caller)
                    .root()
                    .join("committed-precondition-repair-cut-rpc.json"),
            )
            .unwrap();
    }
    export(
        &native,
        &caller,
        "committed-precondition-repair-cut.json",
        serde_json::json!({
            "original_committed":committed,"exact_preparation":prepared,
            "repaired_receipts":repaired_receipts,"queue_cut":{"head_sequence":observed_queue.head_sequence,"records":observed_queue.records},
            "observed_in_budget":observed_in_budget,"goal_observed_elapsed_micros":observed_elapsed.as_micros(),
            "diagnostic_export_elapsed_micros":started.elapsed().as_micros(),
            "actual_retry_row":retry_row,"start_unix_nanos":start_unix_nanos,
            "actual_retry_due_unix_nanos":actual_retry_due_unix_nanos,"already_complete_before_heal":already_complete,
            "goal_unix_nanos":goal_unix_nanos,"retry_remaining_nanos":retry_remaining.as_nanos(),
            "per_rpc_budget_nanos":sinomo_core::network::DEFAULT_NETWORK_TIMEOUT.as_nanos(),
            "automatic_component_timer":automatic,"normal_interval_allowance_nanos":normal_interval_allowance.as_nanos(),
            "checkpointed_rpc_records":goal_rpc_records,
            "ordinary_tick_observation":format!("{tick_observation:#?}"),
            "qualification":if automatic {
                "actual mounted component timer, healed once with no extra wake or explicit tick; passive N3 and nonactive cursor candidate cut by recorded due plus existing normal interval plus unchanged default RPC action budget; exact Original Close at/before this head is required below"
            } else {
                "one explicit ordinary native maintenance invocation after the actual durable retry deadline; concurrent passive exact receipts and nonactive cursor candidate cut by actual due plus unchanged default RPC budget; exact Original Close at/before this head is required below; automatic progress is not inferred"
            }
        }),
    );
    assert!(
        observed_in_budget,
        "normal committed N repair must reach N3 without active work inside its captured due, scheduler and RPC allowances"
    );
    let terminal = inspect
        .metadata_backend()
        .unwrap()
        .get_intent(&object, &committed.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, DfsWriteIntentStatus::Committed);
    assert_eq!(terminal.publication, committed.publication);
    let final_head = inspect.posix_stat_distributed(PATH).unwrap();
    assert_eq!(final_head.version.as_str(), metadata.version);
    assert_eq!(inspect.posix_read_file_distributed(PATH).unwrap(), STAGED);
    drop(inspect);
    drop(backend);
    native.stop_node(&primary).unwrap();
    let history = if observed_queue.head_sequence == 0 {
        Vec::new()
    } else {
        crate::repair_queue_store::tests::native_queue_history(&queue_path)
    };
    let originals = history
        .iter()
        .flat_map(|(_, operations)| operations)
        .filter_map(|operation| {
            let crate::repair_queue_store::DfsRepairQueueOperation::RepairCustody { change } =
                operation
            else {
                return None;
            };
            let crate::repair_queue_store::RepairCustodyChange::Enroll { birth } = change.as_ref()
            else {
                return None;
            };
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                return None;
            };
            (command.intent_id == committed.intent_id
                && command.object_id == object
                && command.file_system_id == committed.file_system_id
                && command.version == committed.target_version
                && Some(&command.content_hash) == committed.content_hash.as_ref()
                && birth.original.source_node_id == primary
                && committed
                    .desired_replica_node_ids
                    .contains(&birth.original.target_node_id))
            .then(|| birth.as_ref().clone())
        })
        .collect::<Vec<_>>();
    export(
        &native,
        &caller,
        "committed-precondition-queue-history.json",
        serde_json::json!({
            "history":history,"matched_originals":originals,"observed_queue_cut":{"head_sequence":observed_queue.head_sequence,"records":observed_queue.records}
        }),
    );
    let retained_cursors_match_originals = observed_queue
        .records
        .iter()
        .filter(|row| row.intent_id == committed.intent_id)
        .all(|row| {
            nonactive_cursor(row)
                && originals.iter().any(|original| {
                    row.completed_transfer_request_id.as_deref()
                        == Some(original.root.subject.request_id.as_str())
                        && original.original.target_node_id == row.missing_replica_node_id
                })
        });
    let settlements = originals
        .iter()
        .map(|original| {
            let closed_before_cut = history.iter().any(|(sequence, operations)|
                *sequence <= observed_queue.head_sequence && operations.iter().any(|operation|
                    matches!(operation, crate::repair_queue_store::DfsRepairQueueOperation::RepairCustody { change }
                        if matches!(change.as_ref(), crate::repair_queue_store::RepairCustodyChange::Close { subject, .. }
                            if subject == &original.root.subject))));
            let settled = queue.original_repair(&original.original).unwrap();
            let actual_closed = queue
                .closed_repair_birth(&original.root.subject)
                .unwrap()
                .is_some();
            let valid = closed_before_cut && actual_closed && settled.as_ref().is_some_and(|settled| {
                settled.original == original.original
                    && settled.closed_at_sequence.is_some_and(|sequence|
                        sequence <= observed_queue.head_sequence)
            });
            (valid, serde_json::json!({
                "original":original,"settled_original":settled,
                "closed_before_cut":closed_before_cut,"actual_closed_birth":actual_closed
            }))
        })
        .collect::<Vec<_>>();
    export(
        &native,
        &caller,
        "committed-precondition-result.json",
        serde_json::json!({
            "created":metadata,"original_committed":committed,
            "parent_after_own_touch":format!("{root_after:?}"),
            "committed_body_replay_published":replay.published,"repaired_receipts":repaired_receipts,
            "terminal":terminal,"final_head":format!("{final_head:?}"),
            "matched_originals":originals,"settlements":settlements,
            "retained_nonactive_cursors_match_same_originals":retained_cursors_match_originals,
            "observed_queue_cut":{"head_sequence":observed_queue.head_sequence,"records":observed_queue.records},
            "ordinary_tick_observation":format!("{tick_observation:#?}"),
            "automatic_component_timer":automatic,"normal_interval_allowance_nanos":normal_interval_allowance.as_nanos(),
            "qualification":"actual W2/N3 receipt goal with no active own work or Original slot and a stable re-read queue head; precise completed cursor DATA and historical PossiblyApplied evidence may remain only when matching the same immutable Repair Original. Unchanged stopped native-history proof requires every real Original Close no later than the in-budget candidate head and actual ClosedRepairBirth. No cursor is deleted to force success; automatic timer and explicit ordinary tick remain distinct; no Stage-only alternate success"
        }),
    );
    assert!(
        !originals.is_empty(),
        "this repaired-Original control requires at least one actual native Repair Original"
    );
    assert_eq!(
        originals
            .iter()
            .map(|original| &original.original.target_node_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        originals.len(),
        "each actual missing target retains one immutable Original"
    );
    assert!(
        retained_cursors_match_originals,
        "every retained completed cursor must name its actual immutable Original"
    );
    assert!(settlements.iter().all(|(valid, _)| *valid));
    native.checkpoint_rpc_observation_window().unwrap();
    native.close().unwrap();
}
