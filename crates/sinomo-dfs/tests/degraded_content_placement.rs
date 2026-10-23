//! Native rejection gate. Historical copies and advisory path observations do
//! not admit a new deferred target. Positive degraded-write availability awaits
//! a coherent admitted target inventory; all metadata participants stay alive.

use sinomo_core::{DfsObjectPolicy, node::NodeId};
use sinomo_dfs::test_support::DfsTestRuntime;
use sinomo_dfs::*;
use std::collections::BTreeSet;

fn options(nodes: &[NodeId]) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.to_vec(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    }
}

#[test]
fn native_degraded_write_rejects_unadmitted_targets_without_erasing_recorded_copies() {
    let (runtime, nodes) =
        DfsTestRuntime::start_cluster("native-degraded-content-admission-gate", 3).unwrap();
    let path = "/services/native-degraded-content-admission-gate.bin";
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    assert_eq!(
        std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect(),
    );
    let b = shard.primary_node_id.clone();
    let a = nodes.iter().find(|node| **node != b).unwrap().clone();
    runtime
        .apply_placement_policy(
            &b,
            path,
            DfsPlacementScope::Subtree,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap(),
        )
        .unwrap();
    // Seed through registered Main completion, which publishes the caller's
    // live binding. The owning-shard Core opener alone does not promise that
    // caller-local response effect even when all N receipts are durable.
    let mut seed = runtime.client(&b).unwrap();
    let original = seed
        .posix_write_file(path, b"original all-three bytes", options(&nodes))
        .unwrap();
    let baseline_sync = seed.posix_sync_file(path, DfsPosixSyncKind::File).unwrap();
    assert_eq!(baseline_sync.target.version, original.version);
    assert_eq!(baseline_sync.content_evidence.required_receipts, 3);
    // Establish live exact-version bindings before injecting an observation.
    // Receipts alone do not prove the raw reader's live-binding precondition.
    for node in &nodes {
        let reader = runtime.client(node).unwrap();
        let (manifest, bytes) = reader
            .content()
            .read_object_version(original.object_id.as_str(), original.version.as_str())
            .unwrap_or_else(|error| {
                panic!("baseline replica {node}, registered caller {b}: {error}")
            });
        assert_eq!(Some(manifest.content_hash), original.content_hash);
        assert_eq!(bytes, b"original all-three bytes");
    }
    drop(seed);
    let seed = runtime.metadata_primary_service(path).unwrap();
    let backend = seed.metadata_backend().unwrap();
    let object_before = backend.get_object(&original.object_id).unwrap().unwrap();
    let version_before = backend
        .get_version(&original.object_id, &original.version)
        .unwrap()
        .unwrap();
    let receipts_before = backend
        .receipts_for_version(&original.object_id, &original.version)
        .unwrap();
    assert_eq!(receipts_before.len(), 3);
    let history_before = backend
        .object_write_intent_history(&original.object_id)
        .unwrap();
    let intent_ids_before = history_before
        .intents
        .iter()
        .map(|intent| intent.intent_id.clone())
        .collect::<BTreeSet<_>>();
    let mut snapshot = (*seed.placement_snapshot_cache().snapshot().unwrap()).clone();
    let now = time::OffsetDateTime::now_utc();
    snapshot
        .paths
        .retain(|path| path.source_node_id != b || path.target_node_id != a);
    snapshot.paths.push(DfsPlacementPathObservation {
        source_node_id: b.clone(),
        target_node_id: a.clone(),
        state: DfsPlacementPathState::Unreachable,
        latency_micros: None,
        delivered_bytes_per_second: None,
        metering: DfsNetworkMetering::Unknown,
        observed_at: now,
        expires_at: now + time::Duration::minutes(5),
    });
    // Establish the exact typed planning failure independently of the POSIX
    // wrapper, which retains only the broader distributed-error category.
    let request = DfsReplicaPlacementRequest {
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        target_version: DfsVersion::generate(),
        coordinator_node_id: b.clone(),
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        candidates: nodes
            .iter()
            .cloned()
            .map(DfsReplicaCandidate::new)
            .collect(),
    };
    assert!(matches!(
        explain_replicas_with_placement_snapshot(
            &request,
            &snapshot,
            &object_before.current_locations,
            &[],
        ),
        Err(DfsDistributedError::InvalidReplicaSet { .. })
    ));
    let private = DfsPlacementSnapshotCache::default();
    private.publish(snapshot.clone());
    drop(seed);

    // Providers and metadata transport remain operational. An unsafe fallback
    // cannot be hidden by a later content or FullShard failure in this fixture.
    let operation = "unadmitted-deferred-target-original";
    let payload = b"same original request after path recovery";
    let mut core = runtime
        .metadata_primary_service(path)
        .unwrap()
        .with_placement_snapshot_cache(private.clone());
    let cursor = runtime.rpc_observations().len();
    let error = core
        .posix_write_file_with_operation_id(path, payload, options(&nodes), operation)
        .unwrap_err();
    assert!(
        matches!(&error, DfsPosixError::Dfs(DfsError::Distributed { .. })),
        "{error}"
    );
    let backend = core.metadata_backend().unwrap();
    assert_eq!(
        backend.get_object(&original.object_id).unwrap().unwrap(),
        object_before
    );
    assert_eq!(
        backend
            .get_version(&original.object_id, &original.version)
            .unwrap()
            .unwrap(),
        version_before
    );
    assert_eq!(
        backend
            .receipts_for_version(&original.object_id, &original.version)
            .unwrap(),
        receipts_before
    );
    let history_after = backend
        .object_write_intent_history(&original.object_id)
        .unwrap();
    assert_eq!(
        history_after
            .intents
            .iter()
            .map(|intent| intent.intent_id.clone())
            .collect::<BTreeSet<_>>(),
        intent_ids_before
    );
    assert!(
        history_after
            .intents
            .iter()
            .all(|intent| intent.operation_id.as_deref() != Some(operation))
    );
    assert!(
        !runtime.rpc_observations()[cursor..]
            .iter()
            .any(|call| call.source == b
                && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_STAGE_PATCH_ROUTE]
                    .contains(&call.route.as_str())),
        "rejected planning must precede remote staging"
    );
    assert_eq!(core.posix_stat(path).unwrap().version, original.version);
    drop(core);
    for node in &nodes {
        let reader = runtime.client(node).unwrap();
        let (manifest, bytes) = reader
            .content()
            .read_object_version(original.object_id.as_str(), original.version.as_str())
            .unwrap();
        assert_eq!(Some(manifest.content_hash), original.content_hash);
        assert_eq!(bytes, b"original all-three bytes");
    }

    // Retry the exact original identity/body once ordinary strict N-target
    // planning succeeds. A fresh identity must not obscure prior effects.
    snapshot
        .paths
        .iter_mut()
        .filter(|path| path.source_node_id == b && path.target_node_id == a)
        .for_each(|path| path.state = DfsPlacementPathState::Healthy);
    private.publish(snapshot);
    let mut healed = runtime
        .metadata_primary_service(path)
        .unwrap()
        .with_placement_snapshot_cache(private);
    let current = healed
        .posix_write_file_with_operation_id(path, payload, options(&nodes), operation)
        .unwrap();
    assert_ne!(current.version, original.version);
    let backend = healed.metadata_backend().unwrap();
    let receipts = backend
        .receipts_for_version(&current.object_id, &current.version)
        .unwrap();
    assert_eq!(receipts.len(), 3);
    let history = backend
        .object_write_intent_history(&current.object_id)
        .unwrap();
    let matching = history
        .intents
        .iter()
        .filter(|intent| intent.operation_id.as_deref() == Some(operation))
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].status, DfsWriteIntentStatus::Committed);
    assert_eq!(matching[0].target_version, current.version);
    assert_eq!(matching[0].policy, DfsReplicationPolicy::new(2, 3).unwrap());
    assert_eq!(
        matching[0]
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect(),
    );
    let prepared_id = format!("write:{}", matching[0].intent_id);
    let prepared_before = backend
        .prepared_operation_record(&prepared_id)
        .unwrap()
        .unwrap();
    prepared_before.validate().unwrap();
    assert_eq!(
        prepared_before.status,
        DfsPreparedOperationStatus::Committed
    );
    drop(healed);
    // The Core effect used the healed private observation above. Complete the
    // registered caller path with the same caller, operation, payload/options.
    // Its observed-base envelope may refresh; the sealed prepared outcome must
    // retain its original digest, body and exact terminal reference.
    let mut caller = runtime.client(&b).unwrap();
    let replay = caller
        .posix_write_file_with_operation_id(path, payload, options(&nodes), operation)
        .unwrap();
    assert_eq!(replay.object_id, current.object_id);
    assert_eq!(replay.version, current.version);
    assert_eq!(replay.content_hash, current.content_hash);
    assert_eq!(caller.posix_stat(path).unwrap().version, current.version);
    let prepared_after = caller
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&prepared_id)
        .unwrap()
        .unwrap();
    prepared_after.validate().unwrap();
    assert_eq!(prepared_after.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(prepared_after.digest, prepared_before.digest);
    assert_eq!(prepared_after.payload, prepared_before.payload);
    assert_eq!(
        prepared_after.committed_entry,
        prepared_before.committed_entry
    );
    assert_eq!(
        caller
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&current.object_id)
            .unwrap()
            .intents
            .iter()
            .filter(|intent| intent.operation_id.as_deref() == Some(operation))
            .count(),
        1
    );
    drop(caller);
    for node in &nodes {
        let reader = runtime.client(node).unwrap();
        let (manifest, bytes) = reader
            .content()
            .read_object_version(current.object_id.as_str(), current.version.as_str())
            .unwrap();
        assert_eq!(Some(manifest.content_hash), current.content_hash);
        assert_eq!(bytes, payload);
    }
    let active = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|row| row.shard_id == shard.shard_id)
        .unwrap();
    assert_eq!(active, shard);
    runtime.close().unwrap();
}
