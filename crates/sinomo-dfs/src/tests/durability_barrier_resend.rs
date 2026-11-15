//! Re-sends of a durability barrier to a member it cannot reach.

use super::*;
use crate::test_runtime::Partition;
use std::time::{Duration, Instant};

/// A barrier whose certificate must include a member cut off from every DFS
/// route sends to it on a backing-off re-send interval, a bounded number of
/// times, rather than on every pump round; and once the network reports every
/// member it still waits for unreachable, the barrier ends there instead of at
/// its deadline.
#[test]
fn native_durability_barrier_backs_off_an_unreachable_member() {
    let name = "native-barrier-resend";
    let (runtime, mut client, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let cut_off = config.replica_node_ids[0].clone();
    client.posix_mkdir_all("/services/barrier-resend").unwrap();
    drop(client);
    let mut service = runtime.metadata_primary_service("/services").unwrap();
    runtime.partition_node(
        &cut_off,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == config.shard_id)
        .unwrap();
    let mut replicas = shard.replica_node_ids.clone();
    replicas.sort();
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let head = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard_id)
        .unwrap()
        .expect("the primary has a prefix");
    let target = crate::DfsMetadataQuorumSyncTarget {
        file_system_id: shard.file_system_id.clone(),
        shard_id,
        primary_node_id: shard.primary_node_id.clone(),
        authority_epoch: Some(shard.epoch),
        replica_node_ids: replicas,
        expected_shard: Some(shard.clone()),
        journal_sequence: head.journal_sequence,
        barrier_id: sinomo_core::Id::generate().to_string(),
        state_fingerprint: head.digest.clone(),
        checkpoint_snapshot: None,
    };
    let observed = runtime.rpc_observations().len();
    let started = Instant::now();
    let synced = service.sync_metadata_shard_to_quorum_intersecting(
        &target,
        std::slice::from_ref(&cut_off),
        Duration::from_secs(30),
    );
    let elapsed = started.elapsed();
    let attempts = runtime
        .rpc_observations()
        .into_iter()
        .skip(observed)
        .filter(|event| {
            event.target == cut_off
                && event.phase == crate::test_runtime::RpcPhase::Constructed
                && (event.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    || event.route == crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE)
        })
        .count();
    match synced {
        Err(DfsError::NetworkDelivery { delivery_state, .. }) => assert_eq!(
            delivery_state,
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent
        ),
        other => panic!("the barrier ends on the network's verdict: {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(10),
        "the network's verdict ends the barrier before its deadline: {elapsed:?}"
    );
    assert!(attempts <= 16, "{attempts} sends to the unreachable member");
    runtime.partition_node(&cut_off, Partition::default());
    drop(service);
    runtime.close().unwrap();
}
