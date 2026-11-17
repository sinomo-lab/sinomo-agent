//! Write-set transactions on the native test runtime: actual Raft-backed
//! cluster KV, the daemon control-plane client and real shard directories.

use super::*;
use crate::test_support::{DfsTestRuntime, current_native_metadata_service};
use crate::{
    DfsMetadataBackend as _, DfsMetadataShardId, DfsWriteOptions, LocalSobjMetadataBackend,
};
use std::time::{Duration, Instant};

fn members(config: &DfsMetadataShardConfig) -> Vec<NodeId> {
    metadata_shard_configured_node_ids(config)
        .into_iter()
        .collect()
}

fn cut(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> DfsMetadataShardRecordCut {
    let mut client = runtime.configuration_publisher(node);
    DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(&config.file_system_id, &config.shard_id, &members(config))
        .unwrap()
        .expect("the shard exists")
}

fn submit(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    change: &DfsMetadataWriteSetChangeRequest,
) -> DfsMetadataWriteSetTxnOutcome {
    let mut client = runtime.configuration_publisher(node);
    DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(change)
}

fn object_row(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
) -> Option<crate::DfsObjectMetadata> {
    let shard_root = crate::default_dfs_metadata_shards_root(runtime.data_dir(node))
        .join(crate::metadata_keys::key_segment(&config.shard_id));
    LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(config.shard_id.clone()),
        shard_root,
    )
    .ok()?
    .get_object(object_id)
    .ok()?
}

fn settle(runtime: &DfsTestRuntime, nodes: &[&NodeId], done: &dyn Fn() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "{what} did not settle");
        for node in nodes {
            runtime
                .maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap();
        }
    }
}

#[test]
fn native_dormant_write_set_gate_switch_reduction_and_lost_swap_reply() {
    let name = "native-write-set-gate-and-swap";
    let (runtime, mut primary_service, config) =
        current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    let stat = primary_service
        .posix_write_file(
            "/services/write-set.txt",
            b"write-set row",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(primary_service);
    let replicas = config.replica_node_ids.clone();
    assert_eq!(
        replicas.len(),
        2,
        "every metadata-capable node replicates the shard"
    );
    settle(
        &runtime,
        &[&primary, &replicas[0], &replicas[1]],
        &|| {
            replicas
                .iter()
                .all(|node| object_row(&runtime, node, &config, &stat.object_id).is_some())
        },
        "replication",
    );

    // Model the older member advertisements explicitly. Current Main supports
    // write sets, but a mixed-capability assignment must still refuse switching.
    for node in members(&config) {
        runtime
            .withhold_metadata_write_set_capability(&node)
            .unwrap();
    }
    // No write-set state exists on any member before the guarded switch.
    let dormant = cut(&runtime, &primary, &config);
    assert_eq!(
        dormant.write_set_state(),
        DfsMetadataShardWriteSetState::Unswitched
    );
    let all: BTreeSet<_> = members(&config).into_iter().collect();
    assert!(build_metadata_write_set_switch(&dormant, &all).is_err());
    for node in members(&config) {
        let state = crate::default_dfs_metadata_shards_root(runtime.data_dir(&node))
            .join(crate::metadata_keys::key_segment(&config.shard_id))
            .join("coordination")
            .join("write-set-member-state.sobj");
        assert!(!state.exists());
    }

    // The gate waits for every configured member.
    runtime
        .advertise_metadata_write_set_capability(&primary)
        .unwrap();
    runtime
        .advertise_metadata_write_set_capability(&replicas[0])
        .unwrap();
    assert!(build_metadata_write_set_switch(&cut(&runtime, &primary, &config), &all).is_err());
    runtime
        .advertise_metadata_write_set_capability(&replicas[1])
        .unwrap();
    let before = cut(&runtime, &primary, &config);
    let switch = build_metadata_write_set_switch(&before, &all).unwrap();
    assert_eq!(
        submit(&runtime, &primary, &switch),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    let switched = cut(&runtime, &primary, &config);
    assert_eq!(switched.shard_value, before.shard_value);
    assert_eq!(
        switched
            .write_set_state()
            .switched()
            .unwrap()
            .write_set
            .len(),
        3
    );

    // Reduction to two members: the removed member deletes its copy through
    // the ordinary placement-removal path.
    let removed = replicas[0].clone();
    let reduction = build_metadata_write_set_remove(
        &switched,
        &[removed.clone()].into_iter().collect(),
        &DfsMetadataShardBounds { w_floor: 1, n: 2 },
        2,
        false,
    )
    .unwrap();
    assert_eq!(
        submit(&runtime, &primary, &reduction),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    settle(
        &runtime,
        &[&removed],
        &|| object_row(&runtime, &removed, &config, &stat.object_id).is_none(),
        "discarding the removed copy",
    );
    // The removed member's store reopens without the row, rather than being
    // unreadable.
    let removed_root = crate::default_dfs_metadata_shards_root(runtime.data_dir(&removed))
        .join(crate::metadata_keys::key_segment(&config.shard_id));
    let inspected = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(config.shard_id.clone()),
        removed_root,
    )
    .expect("the discarded copy reopens empty");
    assert!(inspected.get_object(&stat.object_id).unwrap().is_none());
    assert!(
        object_row(&runtime, &replicas[1], &config, &stat.object_id).is_some(),
        "the kept member keeps its copy"
    );
    let reduced = cut(&runtime, &primary, &config);
    assert_eq!(
        reduced.shard.epoch, config.epoch,
        "reduction never advances the epoch"
    );
    assert_eq!(reduced.shard.replica_node_ids, vec![replicas[1].clone()]);

    // A swap whose reply is lost after it committed is settled from its own
    // outcome: resubmitting the identical request replays the committed reply.
    let candidate = replicas[1].clone();
    let request_id = KvRequestId::generate();
    let epoch_start = crate::DfsMetadataPrefixProof::from_digest(
        1,
        format!("dfs-metadata-prefix-v2:blake3:{}", "7".repeat(64)),
    )
    .unwrap();
    let swap =
        build_metadata_takeover_swap(&reduced, &candidate, &epoch_start, &request_id).unwrap();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook = {
        let dropped = std::sync::Arc::clone(&dropped);
        let target = request_id.clone();
        runtime
            .native_kv_reply_hook(&candidate, move |request| {
                if request.request_id == target
                    && !dropped.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(sinomo_core::service::ServiceError::Service(
                        "reply lost after commit".into(),
                    ));
                }
                Ok(())
            })
            .unwrap()
    };
    assert!(matches!(
        submit(&runtime, &candidate, &swap),
        DfsMetadataWriteSetTxnOutcome::Unknown { .. }
    ));
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    let mut client = runtime.configuration_publisher(&candidate);
    let settlement = DfsControlPlaneStore::new(client.as_mut())
        .settle_metadata_takeover_swap(&reduced, &candidate, &epoch_start, &request_id)
        .unwrap();
    assert_eq!(settlement, DfsMetadataTakeoverSettlement::Applied);
    drop(hook);
    let swapped = cut(&runtime, &candidate, &config);
    assert_eq!(swapped.shard.primary_node_id, candidate);
    assert_eq!(
        swapped.shard.epoch,
        config.epoch + 1,
        "the swap applied exactly once"
    );
    let write_set = swapped.write_set_state().switched().cloned().unwrap();
    assert_eq!(
        write_set.last_change.change_id,
        request_id.as_id().to_string()
    );
    assert_eq!(write_set.epoch_start, Some(epoch_start));
    runtime.close().unwrap();
}
