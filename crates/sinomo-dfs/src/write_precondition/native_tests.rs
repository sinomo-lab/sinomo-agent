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
    // Hold the primary's background replica work through a restart, so the
    // restarted executor is what first attempts the missing replica.
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
    if restart_primary {
        // The committed body survives the restart unchanged; the repair need
        // lives only in the shard tables, which the restarted primary's
        // executor reads in full at its first observation.
        drop(inspect);
        native.stop_node(&primary).unwrap();
        native.reopen_node(primary.clone()).unwrap();
        inspect = native.metadata_primary_service(PATH).unwrap();
        let reopened = inspect
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&original_operation)
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened.digest, prepared.digest,
            "restart must retain the same committed native body and digest"
        );
    }
    let receipt_nodes = |inspect: &crate::NetworkedDfsService<crate::DfsNodeTransport>| {
        inspect
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&object, &committed.target_version)
            .unwrap()
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_ne!(
        receipt_nodes(&inspect),
        nodes.iter().cloned().collect(),
        "the denied third replica is still missing"
    );
    // Heal once. The automatic case leaves progress to the mounted
    // component's own executor wake; the other drives ordinary maintenance.
    drop(restart_claim_hold);
    native.clear_faults();
    let normal_interval_allowance = if automatic {
        crate::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
    } else {
        Duration::ZERO
    };
    // An attempt that already failed against the denied target backs off
    // on the executor's single timer; the allowance starts when it is due.
    let retry_due = native
        .repair_executor_next_due_unix_nanos(&primary)
        .unwrap();
    let until_due = retry_due.map_or(Duration::ZERO, |due| {
        Duration::from_nanos(due.saturating_sub(crate::dfs_now_unix_nanos()))
    });
    let deadline = Instant::now()
        + until_due
        + normal_interval_allowance
        + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        if receipt_nodes(&inspect) == nodes.iter().cloned().collect() {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal committed N repair must reach N3 within its scheduler and RPC allowances"
        );
        if automatic {
            std::thread::sleep(Duration::from_millis(5));
        } else {
            native
                .maintenance_tick(
                    &primary,
                    remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                )
                .unwrap()
                .outcome
                .unwrap();
        }
    }
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
    let root_final = inspect.posix_stat_distributed("/").unwrap();
    assert_eq!(
        root_final.version, root_after.version,
        "N repair never touches the parent again"
    );
    drop(inspect);
    drop(backend);
    native.checkpoint_rpc_observation_window().unwrap();
    native.close().unwrap();
}
