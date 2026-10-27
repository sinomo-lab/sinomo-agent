//! Regression for a manifest-only create whose requester is outside W1/N2.
//! Uses fresh native storage; never replays an uncertain retained operation.
#![cfg(feature = "test-support")]

use sinomo_core::DfsObjectPolicy;
use sinomo_dfs::test_support::{
    DfsTestFaultAction, DfsTestFaultRule, DfsTestRpcOrigin, DfsTestRpcPhase, DfsTestRuntime,
};
use sinomo_dfs::{DfsPlacementPolicy, DfsPlacementScope, DfsWriteOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn promoted_services_primary_fetches_new_create_from_outside_replica_plan() {
    let (runtime, nodes) =
        DfsTestRuntime::start_cluster("post-handoff-manifest-create", 6).unwrap();
    for node in &nodes {
        eprintln!(
            "STORAGE node={node} root={:?}",
            runtime.data_dir(node).root()
        );
    }
    let before = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let requester = before.primary_node_id.clone();
    let selected = nodes
        .iter()
        .filter(|node| **node != requester)
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    let promoted = selected[0].clone();
    let path = "/services/post-handoff-manifest-create/activation.txt";
    // Open a real mutation lane on the future primary while it is a replica,
    // and create the parent through ordinary namespace authority.
    let mut replica_client = runtime.client(&promoted).unwrap();
    replica_client
        .posix_mkdir_all("/services/post-handoff-manifest-create")
        .unwrap();
    drop(replica_client);
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = selected.clone();
    policy.forbidden_node_ids = vec![requester.clone()];
    runtime
        .apply_placement_policy(&requester, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let after = runtime
        .reassign_metadata_shard(
            &before.file_system_id,
            "services",
            promoted.clone(),
            nodes
                .iter()
                .filter(|node| **node != promoted)
                .cloned()
                .collect(),
        )
        .expect("normal frozen-prefix handoff with both assignment quorums");
    assert_eq!(after.primary_node_id, promoted);
    assert_eq!(after.epoch, before.epoch + 1);
    eprintln!("HANDOFF before={before:?} after={after:?}");
    let mut writer = runtime.client(&requester).unwrap();
    let (_, _, policy) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, nodes.len())
        .unwrap();
    assert_eq!(policy.min_commit_replicas, 1);
    assert_eq!(policy.desired_replicas, 2);
    assert_eq!(policy.required_node_ids, selected);
    assert_eq!(policy.forbidden_node_ids, vec![requester.clone()]);
    let start = runtime.rpc_observations().len();
    // The captured-reader entry point deterministically exercises the same
    // manifest-only route as a byte create replanned away from its requester.
    // An explicit placement rule alone makes the byte entry point attach the
    // body, which does not reproduce the original boundary. Capture once under
    // one fresh operation identity; do not resubmit on an uncertain outcome.
    let operation = sinomo_core::Id::generate().to_string();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    writer.set_transport_operation_deadline(Some(deadline));
    let fetch_gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(DfsTestFaultRule {
        source: Some(promoted.clone()),
        target: Some(requester.clone()),
        route: Some(sinomo_dfs::DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: DfsTestFaultAction::PauseBeforeDispatch(Arc::clone(&fetch_gate)),
    });
    let write_operation = operation.clone();
    let (mut writer, result, captured) = std::thread::scope(|scope| {
        let waiting = scope.spawn(move || {
            let result = writer.posix_create_file_from_reader_with_operation_id(
                path,
                std::io::Cursor::new(b"new-primary-active"),
                DfsWriteOptions {
                    policy,
                    replica_candidates: nodes,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
                &write_operation,
            );
            (writer, result)
        });
        // Observe a real native Fetch attempt before inspecting the negative
        // cut. PutIntent alone must not expose this newly created path.
        let captured = (|| -> Result<sinomo_dfs::DfsWriteIntentRecord, String> {
            loop {
                if Instant::now() >= deadline {
                    return Err("no fetch dispatch within the original write window".into());
                }
                if runtime.rpc_observations()[start..].iter().any(|event| {
                    event.source == promoted
                        && event.target == requester
                        && event.route == sinomo_dfs::DFS_DATA_FETCH_BLOB_ROUTE
                        && event.phase == DfsTestRpcPhase::BeforeDispatch
                }) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let owner = runtime.metadata_primary_service(path)?;
            let backend = owner.metadata_backend().unwrap();
            let records = backend.all_intents().map_err(|error| error.to_string())?;
            let matching = records
                .iter()
                .filter(|record| record.operation_id.as_deref() == Some(operation.as_str()))
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(format!("expected one actual intent: {matching:?}"));
            }
            let record = matching[0];
            let object = backend
                .get_object(&record.object_id)
                .map_err(|error| error.to_string())?;
            let receipts = backend
                .receipts_for_version(&record.object_id, &record.target_version)
                .map_err(|error| error.to_string())?;
            let evidence = serde_json::json!({"phase":"fetch-withheld", "intent":record,
                "object":object, "receipts":receipts, "handoff_before":before, "handoff_after":after});
            std::fs::write(
                runtime
                    .data_dir(&requester)
                    .root()
                    .join("post-handoff-negative.json"),
                serde_json::to_vec_pretty(&evidence).unwrap(),
            )
            .map_err(|error| error.to_string())?;
            if record.status != sinomo_dfs::DfsWriteIntentStatus::ReceiptsPending
                || object.is_some()
                || !receipts.is_empty()
                || record.desired_replica_node_ids != selected
                || record.owner_epoch != after.epoch
            {
                return Err(format!(
                    "unverified content crossed publication: {evidence}"
                ));
            }
            Ok(record.clone())
        })();
        // Release even on a failed observation, then join the one original
        // caller. Recovery never initiates a second create or changes its ID.
        runtime.clear_faults();
        fetch_gate.add_permits(64);
        let (writer, result) = waiting.join().expect("original create caller");
        (writer, result, captured)
    });
    eprintln!("CREATE_RESULT {result:?}");
    for observation in &runtime.rpc_observations()[start..] {
        if observation.route.contains("write")
            || observation.route.contains("fetch")
            || observation.route.contains("stage")
        {
            eprintln!("CONTENT_RPC {observation:?}");
        }
    }
    let owner = runtime.metadata_primary_service(path).unwrap();
    let backend = owner.metadata_backend().unwrap();
    let intents = backend.all_intents().unwrap();
    let retained = intents
        .iter()
        .filter(|intent| {
            intent
                .object_metadata
                .as_ref()
                .is_some_and(|object| object.path == path)
        })
        .collect::<Vec<_>>();
    eprintln!("RETAINED_INTENTS {retained:#?}");
    assert_eq!(
        retained.len(),
        1,
        "one logical operation, including on uncertainty"
    );
    let intent = retained[0];
    assert_eq!(intent.operation_id.as_deref(), Some(operation.as_str()));
    let receipts = backend
        .receipts_for_version(&intent.object_id, &intent.target_version)
        .unwrap();
    let object = backend.get_object(&intent.object_id).unwrap();
    eprintln!("RECEIPTS {receipts:#?} OBJECT {object:#?}");
    let evidence = serde_json::json!({"phase":"fetch-released", "intent":intent,
        "object":object, "receipts":receipts, "result":format!("{result:?}")});
    std::fs::write(
        runtime
            .data_dir(&requester)
            .root()
            .join("post-handoff-recovery.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    let captured = captured.expect("withheld bytes stay invisible without receipts");
    assert_eq!(intent.intent_id, captured.intent_id);
    assert_eq!(intent.target_version, captured.target_version);
    assert_eq!(intent.status, sinomo_dfs::DfsWriteIntentStatus::Committed);
    assert!(
        Instant::now() < deadline,
        "negative and recovery share the original window"
    );
    let repair_object = intent.object_id.clone();
    let repair_version = intent.target_version.clone();
    let repair_intent = intent.intent_id.clone();
    let repair_hash = intent.content_hash.clone();
    // Evaluate the success expectation after closing the actual native runtime,
    // so a failure retains DATA without orphaning fixture owners.
    drop(owner);
    let bytes = writer.posix_read_file_distributed(path);
    drop(writer);
    let queue = sinomo_dfs::LocalDfsRepairQueueStore::new(
        sinomo_dfs::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&promoted)),
    );
    let initial_queue = queue.view().unwrap();
    let mut ticks = Vec::new();
    let repair_result = (|| -> Result<(), String> {
        loop {
            let owner = runtime.metadata_primary_service(path)?;
            let backend = owner.metadata_backend().unwrap();
            let actual = backend
                .receipts_for_version(&repair_object, &repair_version)
                .map_err(|error| error.to_string())?;
            let complete = selected.iter().all(|target| {
                actual.iter().any(|receipt| {
                    &receipt.replica_node_id == target
                        && receipt.receipt.file_system_id == captured.file_system_id
                        && receipt.receipt.object_id == repair_object
                        && receipt.receipt.committed_version == repair_version
                        && receipt.content_hash == repair_hash
                })
            });
            drop(owner);
            if complete {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("missing N receipt after the original absolute write/repair window".into());
            }
            let tick = runtime.maintenance_tick(&promoted, remaining);
            if ticks.len() == 8 {
                ticks.remove(0);
            }
            ticks.push(format!("{tick:?}"));
            tick?;
        }
    })();
    let final_queue = queue.view().unwrap();
    let replica_bytes = selected
        .iter()
        .map(|target| {
            let bytes = runtime.client(target).and_then(|replica| {
                replica.content()
                    .read_object_version(repair_object.as_str(), repair_version.as_str())
                    .map_err(|error| error.to_string())
            });
            (target.clone(), bytes)
        })
        .collect::<Vec<_>>();
    std::fs::write(
        runtime.data_dir(&promoted).root().join("post-handoff-n-repair.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "intent_id":repair_intent,"object_id":repair_object,"version":repair_version,
            "expected_targets":selected,"initial_queue":initial_queue.records,
            "final_queue":final_queue.records,"result":repair_result,"last_ticks":ticks,
            "replica_bytes":replica_bytes,
            "qualification":"actual normal Main maintenance, same original absolute deadline; no second create or supplied transfer evidence"
        })).unwrap(),
    ).unwrap();
    runtime.close().unwrap();
    let written = result.expect("promoted primary must fetch, obtain W, and publish the create");
    let object = object.expect("a returned commit has an actual visible object");
    assert_eq!(object.version, written.version);
    assert_eq!(object.content_size_bytes, Some(18));
    assert_eq!(
        object.content_hash,
        Some(format!("blake3:{}", blake3::hash(b"new-primary-active")))
    );
    assert!(!object.current_locations.contains(&requester));
    assert_eq!(bytes.unwrap(), b"new-primary-active");
    repair_result.expect("promoted primary must retain fresh producer evidence and complete N");
    for (target, bytes) in replica_bytes {
        let (manifest, bytes) = bytes.unwrap();
        assert_eq!(Some(manifest.content_hash), repair_hash, "actual replica {target}");
        assert_eq!(manifest.size_bytes, 18, "actual replica {target}");
        assert_eq!(bytes, b"new-primary-active", "actual replica {target}");
    }
    assert!(
        receipts
            .iter()
            .any(|receipt| selected.contains(&receipt.replica_node_id)
                && receipt.receipt.file_system_id == captured.file_system_id
                && receipt.receipt.object_id == captured.object_id
                && receipt.receipt.committed_version == captured.target_version
                && receipt.content_hash == captured.content_hash)
    );
}
