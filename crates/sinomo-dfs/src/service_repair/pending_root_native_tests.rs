//! Real mounted producers cover pending namespace publication. The explicit
//! missing-view negative below changes only a temporary selector input; it
//! does not claim that native storage legally lost a committed anchor.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cut(
    native: &DfsTestRuntime,
    owner: &NodeId,
    shard: &DfsMetadataShardConfig,
) -> DfsMetadataTableSnapshot {
    let root = default_dfs_metadata_shards_root(native.data_dir(owner))
        .join(crate::metadata_keys::key_segment(&shard.shard_id));
    let mut backend = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&shard.shard_id),
        root,
    )
    .unwrap();
    backend.begin_inspection_snapshot().unwrap();
    let result = backend.export_table_snapshot().unwrap();
    backend.end_inspection_snapshot();
    result
}

fn closed_gate() -> Arc<tokio::sync::Semaphore> {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    gate.close();
    gate
}

fn deny_content(
    native: &DfsTestRuntime,
    owner: &NodeId,
    target: &NodeId,
    gate: &Arc<tokio::sync::Semaphore>,
) {
    for route in [
        crate::routes::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::routes::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        native.install_fault(FaultRule {
            source: Some(owner.clone()),
            target: Some(target.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(gate)),
        });
    }
}

fn remove_gate(native: &DfsTestRuntime, gate: &Arc<tokio::sync::Semaphore>) {
    native.remove_faults_matching(|rule| {
        matches!(&rule.action,
        FaultAction::PauseBeforeDispatch(actual) if Arc::ptr_eq(actual, gate))
    });
}

#[test]
fn native_pending_create_component_eligibility_and_same_body_namespace_restart() {
    let name = "pending-root-native-create";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
    drop(initial);
    let owner = shard.primary_node_id.clone();
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let missing = shard.replica_node_ids[1].clone();
    let content_gate = closed_gate();
    deny_content(&runtime, &owner, &missing, &content_gate);
    let background_hold = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let path = "/pending-root-create.bin";
    let mut writer = runtime.metadata_primary_service(path).unwrap();
    let execution = writer
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: format!("pending-root-seed:{name}"),
        })
        .unwrap()
        .unwrap();
    let metadata_gate = closed_gate();
    let captured = Arc::new(Mutex::new(None));
    let runtime = Rc::new(RefCell::new(runtime));
    let in_hook = Rc::clone(&runtime);
    let captured_hook = Arc::clone(&captured);
    let metadata_hook = Arc::clone(&metadata_gate);
    let source = owner.clone();
    let target = missing.clone();
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move |payload| {
            *captured_hook.lock().unwrap() = Some(payload.clone());
            // Staging and genuine W receipts already succeeded. Deny the
            // actual final native prepare confirmation, never fabricate it.
            in_hook.borrow().install_fault(FaultRule {
                source: Some(source),
                target: Some(target),
                route: None,
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 32,
                action: FaultAction::PauseBeforeDispatch(metadata_hook),
            });
        }));
    });
    let bytes = b"one original unpublished native create";
    let operation = format!("pending-create-{}", sinomo_core::Id::generate());
    let original_result = writer.posix_create_file_with_operation_id(
        path,
        bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 3).unwrap(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
        &operation,
    );
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|slot| {
        slot.borrow_mut().take();
    });
    let mut runtime = Rc::try_unwrap(runtime)
        .ok()
        .expect("hook released fixture")
        .into_inner();
    let payload = captured
        .lock()
        .unwrap()
        .clone()
        .expect("actual W2 create reached final prepare");
    let pending_intent = payload.prepared_intent.as_ref().unwrap();
    let intent_id = pending_intent.intent_id.clone();
    let before = cut(&runtime, &owner, &shard);
    let original = before
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.payload.as_ref() == &payload)
        .cloned()
        .expect("actual saved native Original body");
    original.validate().unwrap();
    let durable = before
        .tables
        .intents
        .iter()
        .find(|intent| intent.intent_id == intent_id)
        .unwrap();
    let initial_jobs = writer.coordinator.repair_jobs().to_vec();
    let initial_coordinator = writer.coordinator.intent(&intent_id).cloned();
    let initial_receipts = writer.coordinator.receipts_for_intent(&intent_id);
    let ready_at_w = writer.coordinator.is_commit_ready(&intent_id);
    let actual_rpc = runtime
        .rpc_observations()
        .iter()
        .map(|event| format!("{event:?}"))
        .collect::<Vec<_>>();
    std::fs::write(
        runtime.data_dir(&owner).root().join("pending-root-actual-producer.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "scope":"actual native producer before the explicit volatile-component seam",
            "original_result":format!("{original_result:?}"),"original":original,
            "durable_intent":durable,"coordinator_intent":initial_coordinator,
            "actual_jobs":initial_jobs,"actual_receipts":initial_receipts,
            "actual_w_ready":format!("{ready_at_w:?}"),"actual_rpc":actual_rpc,
            "qualification":"normal final-publication failure does not itself create committed repair jobs; no automatic Main transition is claimed"
        })).unwrap(),
    ).unwrap();
    // Explicit volatile-component seam, not a native commit or Main outcome:
    // derive the canonical job with the existing production finalizer using
    // only its actual already-observed W receipts. No row/state/ACK is edited.
    assert!(
        writer
            .coordinator
            .finalize_commit_if_ready(&intent_id)
            .unwrap()
    );
    let job = writer
        .coordinator
        .repair_jobs()
        .iter()
        .find(|job| job.intent_id == intent_id && job.missing_replica_node_id == missing)
        .cloned()
        .expect("real W-to-N producer job, not a fabricated selector target");
    let pending = writer
        .writes
        .pending_writes
        .get(&intent_id)
        .cloned()
        .unwrap();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let queue_before = queue.view().unwrap();
    let tick_before = writer.repair_retry_tick;
    let actual_anchor_lookup = writer.current_read_object_metadata(&pending.object_id);
    let eligible = writer.pending_repair_targets_current_content(&job, &pending);
    let queue_after = queue.view().unwrap();
    std::fs::write(runtime.data_dir(&owner).root().join("pending-root-before-restart.json"), serde_json::to_vec_pretty(
        &serde_json::json!({"original_result":format!("{original_result:?}"),"original":original,
            "durable_intent":durable,"job":job,"eligible":format!("{eligible:?}"),
            "actual_anchor_lookup":format!("{actual_anchor_lookup:?}"),
            "anchor_absent":!before.tables.objects.iter().any(|object| object.object_id == pending.object_id),
            "queue_before":{"head":queue_before.head_sequence,"rows":queue_before.records},
            "queue_after":{"head":queue_after.head_sequence,"rows":queue_after.records}})
    ).unwrap()).unwrap();
    assert!(
        original_result.is_err(),
        "final assigned participant was genuinely unavailable"
    );
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    assert!(matches!(
        durable.status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    ));
    assert!(durable.base_version.is_none());
    assert!(
        !before
            .tables
            .objects
            .iter()
            .any(|object| object.object_id == pending.object_id)
    );
    assert!(writer.metadata.object(&pending.object_id).is_none());
    assert!(
        matches!(&actual_anchor_lookup, Err(DfsError::UnknownObject { object_id })
        if object_id == &pending.object_id),
        "the real producer must exercise the actual missing-anchor branch"
    );
    assert_eq!(
        payload.content_receipts.len(),
        2,
        "actual distinct W bytes precede publication"
    );
    assert_eq!(durable.policy.min_commit_replicas, 2);
    assert_eq!(durable.policy.desired_replicas, 3);
    assert_eq!(eligible.unwrap(), true);
    assert_eq!(queue_after, queue_before);
    assert_eq!(writer.repair_retry_tick, tick_before);
    drop(execution);
    drop(writer);
    drop(background_hold);
    // Stop while the metadata fault is still live, retaining the exact hidden
    // body. Restore delivery only after actual owner teardown, then allow the
    // ordinary mounted recovery to resume it. No caller resubmits the write.
    runtime.stop_node(&owner).unwrap();
    let stopped = cut(&runtime, &owner, &shard);
    assert!(stopped.tables.prepared_operations_v7.contains(&original));
    remove_gate(&runtime, &metadata_gate);
    runtime.reopen_node(owner.clone()).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let current = cut(&runtime, &owner, &shard);
        if let Some(record) = current
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| record.digest == original.digest)
            && record.status == DfsPreparedOperationStatus::Committed
        {
            assert_eq!(record.payload, original.payload);
            assert!(
                current
                    .tables
                    .objects
                    .iter()
                    .any(|object| object.object_id == pending.object_id
                        && object.version == pending.version)
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "same saved native body did not resume after restart"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    runtime.assert_fault_budgets().unwrap();
    remove_gate(&runtime, &content_gate);
    // The private component finalizer above never runs the registered Main
    // report which durably checkpoints its fresh transfer observations. Dropping
    // that private lane can leave cold reconstruction with unknown producer DATA.
    // This control proves eligibility and exact namespace-body recovery only.
    // The separate registered Main checkpoint/restart control proves automatic
    // N repair with the actual preserved Original and retained payload owner.
    let recovered_cut = cut(&runtime, &owner, &shard);
    let recovered_queue = queue
        .view()
        .map(|view| serde_json::json!({"head":view.head_sequence,"rows":view.records}))
        .map_err(|error| error.to_string());
    std::fs::write(
        runtime.data_dir(&owner).root().join("pending-root-namespace-recovery.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "scope":"private volatile-component eligibility plus actual same-body native namespace restart recovery",
            "original_digest":original.digest,"native_cut":recovered_cut,
            "typed_queue":recovered_queue,
            "actual_rpc":runtime.rpc_observations().iter().map(|event| format!("{event:?}")).collect::<Vec<_>>(),
            "qualification":"No automatic N claim: private component finalization does not execute registered Main fresh-repair checkpoint ownership; NULL/unknown producer DATA remains unchanged. The preserved native14e/14f passive 35s failures remain diagnostic evidence.",
            "automatic_n_counterpart":"write_precondition::native_tests::native_committed_main_repair_checkpoint_survives_primary_restart_before_n_repair"
        })).unwrap(),
    ).unwrap();
    runtime.close().unwrap();
}

#[test]
fn data_missing_anchor_of_a_real_committed_owner_is_unavailable_without_retiring_it() {
    let name = "pending-root-committed-missing-view";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
    drop(initial);
    let owner = shard.primary_node_id.clone();
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let missing = shard.replica_node_ids[1].clone();
    let gate = closed_gate();
    deny_content(&runtime, &owner, &missing, &gate);
    let background_hold = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let path = "/committed-missing-instance-view.bin";
    let mut service = runtime.metadata_primary_service(path).unwrap();
    let execution = service
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: format!("committed-missing-view:{name}"),
        })
        .unwrap()
        .unwrap();
    let stat = service
        .posix_write_file(
            path,
            b"actual committed owner before a DATA-only unavailable view",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let job = service
        .coordinator
        .repair_jobs()
        .iter()
        .find(|job| job.object_id == stat.object_id && job.missing_replica_node_id == missing)
        .cloned()
        .unwrap();
    let pending = service
        .writes
        .pending_writes
        .get(&job.intent_id)
        .cloned()
        .unwrap();
    let before = cut(&runtime, &owner, &shard);
    let original = before
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| {
            record.status == DfsPreparedOperationStatus::Committed
                && record
                    .terminal_intent
                    .as_ref()
                    .is_some_and(|intent| intent.intent_id == job.intent_id)
        })
        .cloned()
        .unwrap();
    original.validate().unwrap();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let queue_before = queue.view().unwrap();
    let jobs_before = service.coordinator.repair_jobs().to_vec();
    let tick_before = service.repair_retry_tick;
    let pending_before = service.writes.pending_writes.len();
    // Explicit negative DATA input: no legal native operation deleted this
    // committed anchor. Temporarily withhold this instance's views, then
    // restore both before any assertion or lifecycle operation.
    let actual_backend = service.metadata_backend.take();
    let actual_metadata = std::mem::replace(&mut service.metadata, DfsMetadataStore::new());
    let missing_view = service.pending_repair_targets_current_content(&job, &pending);
    service.metadata = actual_metadata;
    service.metadata_backend = actual_backend;
    let queue_after = queue.view().unwrap();
    let after = cut(&runtime, &owner, &shard);
    std::fs::write(
        runtime
            .data_dir(&owner)
            .root()
            .join("committed-missing-view-negative.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "qualification":"DATA-only withheld instance views; native anchor remains present",
            "result":format!("{missing_view:?}"),"original":original,
            "queue_before":{"head":queue_before.head_sequence,"rows":queue_before.records},
            "queue_after":{"head":queue_after.head_sequence,"rows":queue_after.records},
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(
        matches!(
            missing_view,
            Err(DfsError::Metadata { .. } | DfsError::UnknownObject { .. })
        ),
        "unavailable anchor is not evidence to retire a committed owner"
    );
    assert_eq!(queue_before, queue_after);
    assert_eq!(service.coordinator.repair_jobs(), jobs_before.as_slice());
    assert_eq!(service.repair_retry_tick, tick_before);
    assert_eq!(service.writes.pending_writes.len(), pending_before);
    assert!(after.tables.prepared_operations_v7.contains(&original));
    assert!(
        after
            .tables
            .objects
            .iter()
            .any(|object| object.object_id == stat.object_id && object.version == stat.version)
    );
    runtime.assert_fault_budgets().unwrap();
    remove_gate(&runtime, &gate);
    drop(execution);
    drop(service);
    drop(background_hold);
    runtime.close().unwrap();
}
