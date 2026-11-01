// Native owner selection from real managed repair and Core retention producers.
use super::*;
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteDispatch, DfsWriteRepair};
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin};
use std::io::Read;
use std::sync::Mutex;

const V2_BYTES: &[u8] = b"native retained managed original version two";
const LATER_BYTES: &[u8] = b"different actual canonical content after capture";

struct OwnedReplyLoss<'a> {
    native: &'a DfsTestRuntime,
    captured: Arc<Mutex<Vec<DfsRoutedMessage>>>,
}
impl OwnedReplyLoss<'_> {
    fn remove(&self) -> usize {
        self.native.remove_faults_matching(|rule| {
            matches!(&rule.action, FaultAction::CaptureTransferReplyAndLoseAfterDispatch { captured, .. }
                if Arc::ptr_eq(captured, &self.captured))
        })
    }
}
impl Drop for OwnedReplyLoss<'_> {
    fn drop(&mut self) {
        self.remove();
    }
}

#[test]
fn native_managed_original_becomes_nonactive_history_after_core_capture_and_head_departure() {
    let (native, mut initial_service, nodes, initial) = base("native-retained-managed-base-some");
    let fs_owner = initial_service
        .current_placement_file_system_config()
        .unwrap()
        .owner_node_id;
    let primary = nodes
        .iter()
        .find(|node| **node != fs_owner)
        .unwrap()
        .clone();
    let extra = nodes
        .iter()
        .find(|node| **node != primary && **node != fs_owner)
        .unwrap()
        .clone();
    drop(initial_service);
    let catalog = native.metadata_shards().unwrap();
    let root = catalog
        .iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let active = native
        .reassign_metadata_shard(
            &root.file_system_id,
            &root.shard_id,
            primary.clone(),
            nodes
                .iter()
                .filter(|node| **node != primary)
                .cloned()
                .collect(),
        )
        .unwrap();
    assert_eq!(active.primary_node_id, primary);
    let mut writer = native.client(&primary).unwrap();
    assert_ne!(primary, fs_owner);
    assert_eq!(
        writer
            .current_placement_file_system_config()
            .unwrap()
            .owner_node_id,
        fs_owner
    );

    // One actual foreground ordinary W1/N2 source at A+C. The required-C
    // constraint is OLD input policy; NEW managed policy is unpinned.
    let operation = format!("retained-base-v2-{}", sinomo_core::Id::generate());
    let version =
        crate::intent::stable_write_target_version(&primary, &initial.object_id, &operation);
    let request = DfsWriteRequest {
        file_system_id: initial.file_system_id.clone(),
        object_id: initial.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: primary.clone(),
    };
    let mut old_policy = DfsReplicationPolicy::new(1, 2).unwrap();
    old_policy.required_node_ids = vec![extra.clone()];
    let old_candidates = vec![primary.clone(), extra.clone()];
    let admitted = writer
        .plan_distributed_eventual_replicas(&request, old_candidates.clone(), old_policy.clone())
        .unwrap();
    evidence(
        &native,
        &primary,
        "retained-baseline-source-plan",
        serde_json::json!({
            "primary":primary,"filesystem_owner":fs_owner,"extra":extra,"normal_old_plan":admitted
        }),
    );
    assert_eq!(
        admitted
            .desired_replica_nodes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), extra.clone()])
    );
    let manifest = writer
        .content()
        .put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            V2_BYTES,
        )
        .unwrap();
    let seed_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    writer.set_transport_operation_deadline(Some(seed_deadline));
    let seed = writer.write_distributed_manifest_with_candidates(
        request,
        Some(initial.version.clone()),
        manifest.content_hash,
        manifest.size_bytes,
        old_candidates,
        old_policy.clone(),
        DfsDistributedWriteOptions {
            publication: DfsWritePublication::Current,
            repair: DfsWriteRepair::Foreground,
            dispatch: DfsWriteDispatch::AwaitCommit,
            operation_id: Some(operation.clone()),
            ..Default::default()
        },
    );
    let seed_shard_id = DfsMetadataShardId::new(&active.shard_id);
    let seed_shard_root =
        crate::default_dfs_metadata_shards_root(native.data_dir(&primary)).join(&active.shard_id);
    let seed_client_token = match &seed {
        Ok(NetworkedDfsWriteOutcome::Committed { intent_id, .. })
        | Ok(NetworkedDfsWriteOutcome::Pending { intent_id, .. }) => Some(intent_id.clone()),
        Err(_) => None,
    };
    let seed_intent_id = Some(crate::intent::stable_write_intent_id(
        &primary,
        &initial.object_id,
        &operation,
    ));
    let mut seed_poll_error = None;
    let (
        seed_receipts,
        seed_full,
        seed_compact,
        seed_committed,
        seed_history,
        seed_snapshot,
        seed_published,
    ) = loop {
        // The effect opener's resident export can lag its fresh getters.
        // Observe one actual durable read-only cut, then drop it before polling.
        let mut inspection = LocalSobjMetadataBackend::open_for_inspection(
            seed_shard_id.clone(),
            seed_shard_root.clone(),
        )
        .unwrap();
        inspection.begin_inspection_snapshot().unwrap();
        let backend = &inspection;
        let full = seed_intent_id
            .as_ref()
            .and_then(|id| backend.get_intent(&initial.object_id, id).unwrap());
        let compact = seed_intent_id
            .as_ref()
            .and_then(|id| backend.get_intent_receipt(&initial.object_id, id).unwrap());
        let committed = full.as_ref().is_some_and(|record| {
            Some(&record.intent_id) == seed_intent_id.as_ref()
                && record.status == DfsWriteIntentStatus::Committed
                && record.file_system_id == initial.file_system_id
                && record.object_id == initial.object_id
                && record.target_version == version
                && record.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
        }) || compact.as_ref().is_some_and(|record| {
            Some(&record.intent_id) == seed_intent_id.as_ref()
                && record.status == DfsWriteIntentStatus::Committed
                && record.file_system_id == initial.file_system_id
                && record.object_id == initial.object_id
                && record.target_version == version
                && record.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
        });
        let history = backend
            .object_write_intent_history(&initial.object_id)
            .unwrap();
        let snapshot = backend
            .export_shard_table_snapshot(&DfsMetadataShardId::new(&active.shard_id))
            .unwrap()
            .unwrap();
        let raw_snapshot = serde_json::to_value(&snapshot).unwrap();
        let tables = crate::metadata_table_store::metadata_tables_from_table_snapshot(
            snapshot,
            "actual original seed publication cut",
        )
        .unwrap();
        let receipts = tables
            .receipts
            .values()
            .filter(|receipt| {
                receipt.receipt.file_system_id == initial.file_system_id
                    && receipt.receipt.object_id == initial.object_id
                    && receipt.receipt.committed_version == version
                    && receipt.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        // A visible native head/version in a validated prefix proves publication
        // even when normal terminal retention no longer keeps the full row.
        let published = tables
            .objects
            .get(&initial.object_id)
            .zip(
                tables
                    .versions
                    .get(&(initial.object_id.clone(), version.clone())),
            )
            .is_some_and(|(head, recorded)| {
                head.file_system_id == initial.file_system_id
                    && head.object_id == initial.object_id
                    && head.path == PATH
                    && head.kind == DfsObjectKind::File
                    && head.tombstone_provenance.is_none()
                    && head.version == version
                    && head.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
                    && head.content_size_bytes == Some(V2_BYTES.len() as u64)
                    && recorded.visible
                    && recorded.file_system_id == initial.file_system_id
                    && recorded.object_id == initial.object_id
                    && recorded.version == version
                    && recorded.content_hash == head.content_hash
                    && recorded.size_bytes == head.content_size_bytes
                    && recorded.created_by_event_id.is_some()
                    && recorded.created_by_event_id == head.last_modified_by_event_id
                    && recorded
                        .content_durability
                        .as_ref()
                        .is_some_and(|durability| {
                            durability.source_version == version
                                && durability.policy.min_commit_replicas == 1
                                && durability.policy.desired_replicas == 2
                                && durability.policy.required_node_ids == vec![extra.clone()]
                                && durability
                                    .desired_replica_node_ids
                                    .iter()
                                    .cloned()
                                    .collect::<BTreeSet<_>>()
                                    == BTreeSet::from([primary.clone(), extra.clone()])
                        })
            });
        let has_n = [primary.clone(), extra.clone()].iter().all(|node| {
            receipts.iter().any(|receipt| {
                &receipt.replica_node_id == node
                    && receipt.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
            })
        });
        inspection.end_inspection_snapshot();
        drop(inspection);
        if (has_n && published) || Instant::now() >= seed_deadline || seed.is_err() {
            break (
                receipts,
                full,
                compact,
                committed,
                history,
                raw_snapshot,
                published,
            );
        }
        if let Err(error) = writer.process_incoming() {
            seed_poll_error = Some(error.to_string());
            break (
                receipts,
                full,
                compact,
                committed,
                history,
                raw_snapshot,
                published,
            );
        }
        std::thread::yield_now();
    };
    writer.set_transport_operation_deadline(None);
    let mut service = native.metadata_primary_service(PATH).unwrap();
    let object = service
        .current_read_object_metadata(&initial.object_id)
        .unwrap();
    evidence(
        &native,
        &primary,
        "retained-baseline-source-complete",
        serde_json::json!({
            "seed_outcome":format!("{seed:?}"),"actual_object":object,"actual_seed_receipts":seed_receipts,
            "seed_poll_error":seed_poll_error,"original_seed_operation":operation,
            "original_returned_seed_token":seed_client_token,"actual_native_seed_intent_id":seed_intent_id,
            "actual_seed_full_terminal":seed_full,"actual_seed_compact_terminal":seed_compact,
            "actual_same_seed_terminal_committed":seed_committed,
            "actual_same_seed_native_publication":seed_published,
            "actual_seed_history":seed_history,"actual_seed_validated_table_snapshot":seed_snapshot,
            "actual_seed_reader_root":seed_shard_root,
            "qualification":"original Pending is retained; native stable version, visible table prefix, event, frozen durability and exact receipts prove publication, not receipt of a Committed caller result"
        }),
    );
    assert!(seed_poll_error.is_none());
    assert!(
        seed.is_ok() && seed_published,
        "the exact seed must be natively published; its original Pending return remains recorded: {seed:?}"
    );
    assert!([primary.clone(), extra.clone()].iter().all(|node| {
        seed_receipts.iter().any(|receipt| {
            &receipt.replica_node_id == node
                && receipt.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
        })
    }));
    assert_eq!(object.version, version);
    assert_eq!(
        object
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), extra.clone()])
    );
    assert_eq!(
        object.content_hash.as_deref(),
        Some(content_hash(V2_BYTES).as_str())
    );
    let seed_durability = service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &version)
        .unwrap()
        .unwrap()
        .content_durability
        .unwrap();
    assert_eq!(seed_durability.source_version, version);
    assert_eq!(seed_durability.policy.min_commit_replicas, 1);
    assert_eq!(seed_durability.policy.desired_replicas, 2);
    assert_eq!(
        seed_durability.policy.required_node_ids,
        vec![extra.clone()]
    );
    assert_eq!(
        seed_durability
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), extra.clone()])
    );
    drop(writer);

    // Normal captured telemetry and an unpinned accepted managed plan. Its
    // real coordinator is FS owner B, while metadata authority/source is A.
    let (expected, job) = proposal(&native, &mut service);
    let (resolved, replication, placement) = plan(&job, &object);
    let targets = placement.plan.desired_replica_nodes;
    evidence(
        &native,
        &primary,
        "retained-baseline-managed-plan",
        serde_json::json!({
            "actual_job":job,"actual_targets":targets,"actual_replication":replication,"actual_resolved":resolved
        }),
    );
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 2);
    assert!(replication.required_node_ids.is_empty() && replication.forbidden_node_ids.is_empty());
    assert!(job.unavailable_node_ids.is_empty() && job.placement_snapshot.is_some());
    assert_eq!(job.candidate_snapshot.len(), 3);
    assert_eq!(targets.len(), 2);
    assert!(targets.contains(&fs_owner));
    assert!(!object.current_locations.contains(&fs_owner));
    let hold = native
        .deny_background_replica_before_claim(&primary)
        .unwrap();
    let authority_trace = crate::native_shard_trace::capture_native_shard_trace(
        &native.data_dir(&primary).root(),
        DfsMetadataShardId::new(&active.shard_id),
    )
    .unwrap();
    let desired = placement_config_with_override(
        &expected,
        PATH,
        DfsPlacementScope::SelfOnly,
        Some(DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap()),
    )
    .unwrap();
    // The SAME actual writer observes the job/config immediately after its
    // one real CAS. No alternative publication or job identity is introduced.
    let (accepted_job, accepted_config) =
        native.shared_control_plane().unwrap().with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            assert!(
                store
                    .compare_and_put_placement_update(&expected, &desired, &job)
                    .unwrap()
            );
            (
                store
                    .placement_reconciliation(&job.file_system_id, &job.job_id)
                    .map_err(|error| error.to_string()),
                store
                    .get_file_system_config(&job.file_system_id)
                    .map_err(|error| error.to_string()),
            )
        });
    let expected_managed_policy = replication.clone();
    let expected_managed_targets = targets.clone();
    let refreshed = service
        .refresh_control_plane_latest()
        .map_err(|error| error.to_string());
    let private_job = service.control_plane.as_mut().map(|control| {
        DfsControlPlaneStore::new(control.as_mut())
            .placement_reconciliation(&job.file_system_id, &job.job_id)
            .map_err(|error| error.to_string())
    });
    // The accepted job belongs to a NEW logical operation. A normal caller
    // dispatches through registered Main, whose fresh admission owns that cut.
    // The old private effect opener deliberately remains only diagnostic DATA.
    drop(service);
    let mut registered = native.client(&primary).unwrap();
    assert!(registered.transport.delegates_local_namespace_authority());
    let visible_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (visible_job, visible_config, accepted_visible) = loop {
        let control = registered.control_plane.as_mut().unwrap();
        let mut store = DfsControlPlaneStore::new(control.as_mut());
        let visible_job = store
            .placement_reconciliation(&job.file_system_id, &job.job_id)
            .map_err(|error| error.to_string());
        let visible_config = store
            .get_file_system_config(&job.file_system_id)
            .map_err(|error| error.to_string());
        let visible = visible_job
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .is_some_and(|actual| {
                actual.job_id == job.job_id
                    && actual.file_system_id == job.file_system_id
                    && actual.accepted_config == job.accepted_config
                    && actual.candidate_snapshot == job.candidate_snapshot
                    && actual.placement_snapshot == job.placement_snapshot
                    && actual.unavailable_node_ids == job.unavailable_node_ids
            })
            && visible_config.as_ref().ok().and_then(Option::as_ref) == Some(&desired);
        if visible || Instant::now() >= visible_deadline {
            break (visible_job, visible_config, visible);
        }
        std::thread::yield_now();
    };
    evidence(
        &native,
        &primary,
        "retained-baseline-registered-job-visible",
        serde_json::json!({
            "actual_job":visible_job,"actual_config":visible_config,"accepted_visible":accepted_visible,
        }),
    );
    assert!(
        accepted_visible,
        "normal registered caller must observe the actually accepted job before dispatch"
    );
    let reconcile_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    registered.set_transport_operation_deadline(Some(reconcile_deadline));
    // Exactly ONE semantic reconcile for the same accepted job and V2 tuple.
    let reconcile_result = registered
        .route_namespace_operation(
            primary.clone(),
            DfsNamespaceOperation::ReconcilePlacement {
                job_id: job.job_id.clone(),
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                expected_version: object.version.clone(),
                object_policy: object.policy.clone(),
                replication_policy: replication,
                desired_replica_node_ids: targets,
            },
        )
        .map_err(|error| error.to_string());
    registered.set_transport_operation_deadline(None);
    drop(registered);
    let mut raw_trace = std::fs::File::create(
        native
            .data_dir(&primary)
            .root()
            .join("retained-baseline-placement-authority-native.json"),
    )
    .unwrap();
    authority_trace.export_raw(&mut raw_trace).unwrap();
    authority_trace.close();
    evidence(
        &native,
        &primary,
        "retained-baseline-placement-authority",
        serde_json::json!({
            "constructed_job_id":job.job_id,"metadata_primary":primary,"filesystem_owner":fs_owner,
            "accepted_actual_job":accepted_job,"accepted_actual_config":accepted_config,
            "private_refresh":refreshed,"private_actual_job_after_refresh":private_job,
            "original_reconcile_result":reconcile_result.as_ref().map(|_| ()).map_err(|error| error.to_string()),
        }),
    );
    reconcile_result.unwrap();
    // Internal Original preparation starts from a fresh ordinary effect owner
    // AFTER the actual registered Main publication, never the pre-CAS opener.
    let mut service = native.metadata_primary_service(PATH).unwrap();
    let managed = actual_record(&service, &job, &object);
    evidence(
        &native,
        &primary,
        "retained-baseline-managed-stored",
        serde_json::json!({
            "actual_managed":managed,"expected_managed_policy":expected_managed_policy,
            "expected_managed_targets":expected_managed_targets,"actual_seed_durability":seed_durability
        }),
    );
    assert_eq!(managed.status, DfsWriteIntentStatus::Committed);
    assert_eq!(managed.policy, expected_managed_policy);
    assert_eq!(managed.desired_replica_node_ids, expected_managed_targets);
    assert_eq!(managed.base_version.as_ref(), Some(&version));
    let queue = service.owned_repair_queue_store().unwrap().unwrap();
    service
        .retry_pending_repairs_bounded_with_attempts(0, &queue.view().unwrap().records)
        .unwrap();
    let intent = service
        .coordinator()
        .intent(&managed.intent_id)
        .unwrap()
        .clone();
    let pending = service
        .writes
        .pending_writes
        .get(&managed.intent_id)
        .unwrap()
        .clone();
    let existing = queue
        .original_repair_for_slot(&managed.intent_id, &fs_owner)
        .unwrap();
    let request_id = existing
        .as_ref()
        .map(|birth| {
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                panic!("actual managed Repair");
            };
            command.request_id.clone()
        })
        .unwrap_or_else(|| service.next_request_id());
    let prepared = service
        .prepare_current_background_repair_replica_sends(
            &[(fs_owner.clone(), request_id.clone())],
            &intent,
            &pending,
            object.clone(),
        )
        .unwrap();
    assert!(prepared.is_current_background_repair());
    let birth = queue
        .original_repair_for_slot(&managed.intent_id, &fs_owner)
        .unwrap()
        .unwrap();
    let DfsNetworkMessage::RepairReplica(original) = &birth.original.message else {
        panic!("actual enrolled managed Repair");
    };
    assert_eq!(original.expected_base_version.as_ref(), Some(&version));
    assert_eq!(original.version, version);
    assert!(birth.observed.is_empty() && birth.closed_at_sequence.is_none());

    let replies = Arc::new(Mutex::new(Vec::new()));
    native.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(fs_owner.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: Some(managed.intent_id.clone()),
            captured: Arc::clone(&replies),
        },
    });
    let owned_loss = OwnedReplyLoss {
        native: &native,
        captured: Arc::clone(&replies),
    };
    drop(hold);
    let outcomes = crate::test_runtime::executor()
        .block_on(service.dispatch_prepared_repair_replica_sends(prepared, false))
        .unwrap();
    let hold = native
        .deny_background_replica_before_claim(&primary)
        .unwrap();
    let removed = owned_loss.remove();
    drop(owned_loss);
    assert_eq!(outcomes.len(), 1);
    assert!(!outcomes[0].sent);
    assert_eq!(
        outcomes[0].delivery_state,
        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
    );
    // The low-level dispatch already observed the actual delivery into this
    // producer's state. Consume the returned failure as the normal repair
    // controller does, then checkpoint its actual projection before head work.
    service.record_repair_retry_scheduled_with_error(
        &managed.intent_id,
        &fs_owner,
        outcomes[0].last_error.clone(),
    );
    let actual_cursor = service.durable_repair_queue_records();
    let (_, observed_cursor) = queue.view_for_maintenance().unwrap();
    assert!(matches!(
        queue
            .reconcile_observed(&observed_cursor, &actual_cursor)
            .unwrap(),
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_)
    ));
    let actual_replies = replies.lock().unwrap().clone();
    let unknown = queue
        .original_repair_for_slot(&managed.intent_id, &fs_owner)
        .unwrap()
        .unwrap();
    let unknown_queue = queue.view().unwrap();
    evidence(
        &native,
        &primary,
        "retained-baseline-unknown-before-capture",
        serde_json::json!({
            "managed":managed,"actual_birth":birth,"actual_replies":actual_replies,
            "actual_dispatch_outcomes":format!("{outcomes:?}"),"unknown":unknown,
            "queue":{"head_sequence":unknown_queue.head_sequence,"records":unknown_queue.records}
        }),
    );
    assert_eq!(outcomes.len(), 1);
    assert_eq!(removed, 1);
    assert_eq!(
        outcomes[0].delivery_state,
        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
    );
    assert_eq!(actual_replies.len(), 1);
    let DfsNetworkMessage::RepairReplicaResponse(reply) = &actual_replies[0].message else {
        panic!("actual completed native reply");
    };
    assert_eq!(reply.request_id, request_id);
    assert_eq!(reply.intent_id, managed.intent_id);
    assert!(
        matches!(&reply.result,DfsBlobTransferResult::Committed {content_hash:hash}
        if hash==&content_hash(V2_BYTES))
    );
    assert!(unknown.observed.is_empty() && unknown.closed_at_sequence.is_none());
    assert_eq!(unknown.original, birth.original);
    assert!(
        unknown_queue.records.iter().any(|row| {
            row.intent_id == managed.intent_id
                && row.missing_replica_node_id == fs_owner
                && row.transfer_request_id.as_deref() == Some(request_id.as_str())
                && row.transfer_delivery_state
                    == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
                && row.attempts == 1
                && row.last_attempt_unix_nanos > 0
        }),
        "the actual lost-reply cursor must be durable BEFORE capture/head departure"
    );
    drop(service);

    // Core reference API, with no feature-history crate/host involved.
    let mut caller = native.client(&primary).unwrap();
    caller
        .retain_content_revision(DfsContentReferenceRequest {
            namespace: "vendor.retained-baseline".into(),
            path: PATH.into(),
            identity: DfsRevisionReference {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: version.clone(),
            },
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "baseline-pin".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    let later = caller
        .posix_write_file(
            PATH,
            LATER_BYTES,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    assert_ne!(later.version, version);
    assert_ne!(later.content_hash, object.content_hash);
    let deleted = caller
        .posix_unlink_if_generation(
            PATH,
            sinomo_core::Id::generate().to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: later.object_id.clone(),
                version: later.version.clone(),
            },
        )
        .unwrap();
    assert_eq!(deleted.kind, DfsObjectKind::Tombstone);
    drop(caller);
    let mut current = native.metadata_primary_service(PATH).unwrap();
    let raw_before = queue.view().unwrap();
    current
        .retry_pending_repairs_bounded_with_attempts(0, &raw_before.records)
        .unwrap();
    let reconciliation = current.reconcile_file_history_repair_root(&object.object_id, &version);
    let backend = current.metadata_backend().unwrap();
    let retained_version = backend
        .get_version(&object.object_id, &version)
        .unwrap()
        .unwrap();
    let owner_id = crate::file_history::retained_repair_intent_id(&retained_version);
    let history = backend
        .object_write_intent_history(&object.object_id)
        .unwrap();
    let retained = history
        .intents
        .iter()
        .find(|record| record.intent_id == owner_id)
        .cloned();
    let inspector = DfsOperatorInspector::new(backend);
    let view = inspector.object(&object.object_id).unwrap().unwrap();
    let snapshot = backend
        .export_shard_table_snapshot(&DfsMetadataShardId::new(&active.shard_id))
        .unwrap()
        .unwrap();
    let snapshot_prefix = snapshot.prefix_proof.clone();
    let tables = crate::metadata_table_store::metadata_tables_from_table_snapshot(
        snapshot,
        "actual retained-owner baseline snapshot",
    )
    .unwrap();
    let scanned = crate::metadata_summary::metadata_summary_from_tables::<
        _,
        std::convert::Infallible,
    >(&tables, |_| Ok(true))
    .unwrap();
    let (indexed, indexed_primary) = backend.metadata_status_summaries().unwrap();
    let pins = backend.version_pins_for_object(&object.object_id).unwrap();
    let retained_receipts = backend
        .receipts_for_version(&object.object_id, &seed_durability.source_version)
        .unwrap();
    let physical = [primary.clone(), extra.clone()]
        .into_iter()
        .map(|node| {
            let result = native.client(&node).and_then(|reader| {
                reader
                    .content()
                    .read_object_version(object.object_id.as_str(), version.as_str())
                    .map(|(manifest, bytes)| (manifest.content_hash, manifest.size_bytes, bytes))
                    .map_err(|error| error.to_string())
            });
            (node, result)
        })
        .collect::<Vec<_>>();
    let active = current
        .coordinator()
        .repair_jobs()
        .iter()
        .filter(|repair| repair.object_id == object.object_id && repair.version == version)
        .cloned()
        .collect::<Vec<_>>();
    let after = queue
        .original_repair_for_slot(&managed.intent_id, &fs_owner)
        .unwrap()
        .unwrap();
    let after_queue = queue.view().unwrap();
    let mut payload = Vec::new();
    current
        .content()
        .open_retained_repair_payload(&after.root)
        .unwrap()
        .read_to_end(&mut payload)
        .unwrap();
    evidence(
        &native,
        &primary,
        "retained-baseline-after-departure",
        serde_json::json!({
            "actual_current":current.current_read_object_metadata(&object.object_id).map_err(|error|error.to_string()),
        "actual_reconciliation":format!("{reconciliation:?}"),"actual_version":retained_version,
            "actual_full_history":history,"actual_retained_owner":retained,"actual_object_view":view,
            "actual_scan_summary":scanned,"actual_snapshot_prefix":snapshot_prefix,"actual_pins":pins,
            "actual_retained_receipts":retained_receipts,"physical_recorded_holders":physical,
            "actual_index_summary":indexed,"actual_primary_index_summary":indexed_primary,
            "actual_active_jobs":active,"old_original":after,
            "actual_queue":{"head_sequence":after_queue.head_sequence,"records":after_queue.records},
            "payload_hash":content_hash(&payload),"payload_bytes":payload.len(),
            "expected":"one actual recorded retained owner; obsolete Current managed work inactive; original unknown history remains unclosed"
        }),
    );
    // These are the prospective product oracles. Baseline failure must be
    // preserved; do not substitute a derived value for an absent owner row.
    assert!(
        reconciliation.is_ok(),
        "normal retained-root reconciliation must not fail"
    );
    let retained = retained.expect("actual durable recorded retained owner must exist");
    assert_eq!(
        retained_version.content_durability.as_ref(),
        Some(&seed_durability)
    );
    assert!(retained_version.has_retention_grant());
    assert_eq!(
        pins.iter()
            .filter(|pin| pin.version == version
                && pin.object_id == object.object_id
                && pin.file_system_id == object.file_system_id)
            .count(),
        1
    );
    assert!(crate::file_history::retained_repair_plan_matches(
        &retained,
        &current
            .recorded_retained_repair_owner(&retained_version)
            .unwrap()
    ));
    assert!(
        active.is_empty(),
        "stale Current or already-satisfied retained root is not active repair work"
    );
    assert!(view.repairs.is_empty());
    assert_eq!(scanned.pending_repair_count, 0);
    assert_eq!(indexed.pending_repair_count, scanned.pending_repair_count);
    assert_eq!(
        indexed_primary.pending_repair_count,
        scanned.pending_repair_count
    );
    assert_eq!(after.original, birth.original);
    assert_eq!(after.attachment, birth.attachment);
    assert_eq!(after.root, birth.root);
    assert!(after.observed.is_empty() && after.closed_at_sequence.is_none());
    assert!(
        queue
            .closed_repair_birth(&after.root.subject)
            .unwrap()
            .is_none()
    );
    assert!(
        after_queue
            .records
            .iter()
            .any(|row| row.intent_id == managed.intent_id
                && row.transfer_request_id.as_deref() == Some(request_id.as_str())
                && row.transfer_delivery_state
                    == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied))
    );
    assert!([primary.clone(), extra.clone()].iter().all(|node| {
        retained_receipts.iter().any(|receipt| {
            &receipt.replica_node_id == node
                && receipt.content_hash.as_deref() == Some(content_hash(V2_BYTES).as_str())
        })
    }));
    for (_, result) in physical {
        let (hash, size, bytes) = result.expect("actual recorded retained holder bytes");
        assert_eq!(hash, content_hash(V2_BYTES));
        assert_eq!(size, V2_BYTES.len() as u64);
        assert_eq!(bytes, V2_BYTES);
    }
    assert_eq!(payload, V2_BYTES);
    drop(current);
    drop(hold);
    native.close().unwrap();
}
