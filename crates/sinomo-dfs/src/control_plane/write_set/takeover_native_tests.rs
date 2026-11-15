//! Takeover of a write-set shard's primary on the native test runtime: real
//! Raft-backed cluster KV, real shard stores and the real member paths.

use super::commit_native_tests::{catch_up, cut, members, primary, shard_root, shrink, switched};
use super::*;
use crate::DfsMetadataShardId;
use sinomo_core::kv::KvRequestId;
use std::time::{Duration, Instant};

fn shard_id_of(config: &DfsMetadataShardConfig) -> DfsMetadataShardId {
    DfsMetadataShardId::new(config.shard_id.clone())
}

fn member_state_path(
    runtime: &crate::test_support::DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> std::path::PathBuf {
    shard_root(runtime, node, config)
        .join("coordination")
        .join("write-set-member-state.sobj")
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !done() {
        assert!(Instant::now() < deadline, "{what} did not happen");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A takeover driven by hand through the step-3 entry points: `candidate`
/// promises on its resident Main service from an authoritative cut, then the
/// swap is submitted. Returns the promise's request id and epoch start.
pub(super) fn hand_takeover(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    candidate: &NodeId,
) -> (KvRequestId, crate::DfsMetadataPrefixProof) {
    let record = cut(runtime, config);
    let request_id = KvRequestId::generate();
    let shard_id = shard_id_of(config);
    let start = runtime
        .with_main_maintenance_service(candidate, |service| {
            service
                .metadata_backend_mut()
                .unwrap()
                .promise_metadata_takeover(&shard_id, &record, &request_id)
        })
        .unwrap()
        .expect("the candidate promises");
    let swap = build_metadata_takeover_swap(&record, candidate, &start, &request_id).unwrap();
    let mut client = runtime.configuration_publisher(candidate);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&swap),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    for node in members(config) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
    (request_id, start)
}

/// After a takeover, the candidate's kept acceptance fence is retired once its
/// own view reaches it, and the deposed primary releases the pending add the
/// epoch advance superseded, so neither keeps a member-state file.
#[test]
fn native_takeover_retires_the_member_fence_and_the_deposed_primary_releases_pending_adds() {
    let (runtime, config) = switched("native-takeover-fence-retire", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let shard_id = shard_id_of(&config);
    // P holds a pending add of C.
    shrink(&runtime, &config, &c, 1);
    let head = catch_up(&runtime, &config, &c);
    let record = cut(&runtime, &config);
    let evidence = crate::DfsMetadataMemberAckView {
        epoch: config.epoch,
        write_set_generation: record.write_set_state().switched().unwrap().generation,
        durable_prefix: head,
    };
    let mut service = primary(&runtime);
    service
        .metadata_backend_mut()
        .unwrap()
        .record_pending_write_set_add(&shard_id, &record, &c, &evidence)
        .unwrap();
    drop(service);
    assert!(member_state_path(&runtime, &p, &config).exists());

    let (request_id, _) = hand_takeover(&runtime, &config, &b);
    runtime
        .with_main_maintenance_service(&b, |service| {
            service
                .metadata_backend_mut()
                .unwrap()
                .settle_metadata_takeover_promise(
                    &shard_id,
                    &request_id,
                    crate::DfsMetadataTakeoverPromiseEnd::Kept,
                )
        })
        .unwrap()
        .unwrap();
    assert!(member_state_path(&runtime, &b, &config).exists(), "B keeps its fence");

    runtime.enable_write_set_keeping(&b);
    runtime.enable_write_set_keeping(&p);
    wait_for("B's fence is retired", || {
        !member_state_path(&runtime, &b, &config).exists()
    });
    wait_for("P's superseded pending add is released", || {
        !member_state_path(&runtime, &p, &config).exists()
    });
    runtime.close().unwrap();
}
