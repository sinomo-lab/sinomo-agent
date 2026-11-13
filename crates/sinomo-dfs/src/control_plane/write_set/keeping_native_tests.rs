//! Keeping W on the native test runtime: real Raft-backed cluster KV, real
//! shard stores and the real member acknowledgement path.

use super::commit_native_tests::{catch_up, cut, switched, write_set_of};
use super::*;
use crate::DfsMetadataShardId;
use std::time::{Duration, Instant};

fn member_probe(
    config: &DfsMetadataShardConfig,
    target: &NodeId,
    prefix: &crate::DfsMetadataPrefixProof,
    generation: u64,
) -> sinomo_core::network::ObjectRpcRequest {
    let command = crate::metadata_replication::dfs_metadata_write_set_member_probe_command(
        &config.primary_node_id,
        target,
        &config.file_system_id,
        &DfsMetadataShardId::new(config.shard_id.clone()),
        config.epoch,
        prefix,
        generation,
    );
    crate::dfs_metadata_journal_batch_rpc_request(&command).unwrap()
}

/// A catch-up probe naming the record's current generation is answered at
/// once, and its view names that generation. A probe naming a generation the
/// member has not observed is answered only once the member's own view
/// reaches it: the member waits on its publication, not on a timer.
#[test]
fn native_write_set_probe_waits_for_the_named_generation() {
    let (runtime, config) = switched("native-write-set-probe-generation", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let head = catch_up(&runtime, &config, &b);
    let current = write_set_of(&runtime, &config);

    let reply = runtime
        .rpc_request(
            &config.primary_node_id,
            &b,
            member_probe(&config, &b, &head, current.generation),
            Duration::from_secs(30),
        )
        .unwrap();
    let response = crate::decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, crate::DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    assert_eq!(
        response.member_view,
        Some(crate::DfsMetadataMemberAckView {
            epoch: config.epoch,
            write_set_generation: current.generation,
            durable_prefix: head.clone(),
        })
    );

    let next = current.generation + 1;
    // Moving the record to the next generation is what ends the wait. It is
    // submitted from another thread while this one waits on the probe.
    let change = build_metadata_write_set_shrink(
        &cut(&runtime, &config),
        &c,
        &DfsMetadataShardBounds { w_floor: 1, n: 3 },
    )
    .unwrap();
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    let submitter = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&change)
    });
    let started = Instant::now();
    let reply = runtime
        .rpc_request(
            &config.primary_node_id,
            &b,
            member_probe(&config, &b, &head, next),
            Duration::from_secs(60),
        )
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "the member answers only once its view reaches the named generation"
    );
    assert_eq!(
        submitter.join().unwrap(),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    let response = crate::decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, crate::DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        response.member_view,
        Some(crate::DfsMetadataMemberAckView {
            epoch: config.epoch,
            write_set_generation: next,
            durable_prefix: head.clone(),
        })
    );
    assert_eq!(write_set_of(&runtime, &config).generation, next);
}
