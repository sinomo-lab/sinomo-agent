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

/// How often `node`'s copy of the shard was opened: a rebind in place leaves
/// this unchanged, a reopen raises it.
fn opens(
    runtime: &crate::test_support::DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> usize {
    crate::metadata_sharded_backend::metadata_shard_backend_opens_for_test(&shard_root(
        runtime, node, config,
    ))
}

/// Write `path` through `node`'s client, which keeps no local store and
/// delegates to its Main daemon, so the write runs on the current primary's
/// resident store. A publication that advanced under the attempt, or a
/// commit whose outcome the takeover left unknown, is retried under the same
/// path; the content is idempotent.
fn client_write(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    node: &NodeId,
    path: &str,
) -> crate::DfsObjectId {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let primary = cut(runtime, config).shard.primary_node_id;
        let mut client = runtime.client(node).unwrap();
        let written = client.posix_write_file(
            path,
            path.as_bytes(),
            crate::DfsWriteOptions::replicated_eventual(vec![primary]),
        );
        match written {
            Ok(written) => return written.object_id,
            Err(error) => {
                assert!(Instant::now() < deadline, "{path} was not written: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn holds_object(
    runtime: &crate::test_support::DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
) -> bool {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .ok()
    .and_then(|backend| backend.get_object(object_id).ok().flatten())
    .is_some()
}

/// A takeover changes the primary, epoch and replica ring of a write-set
/// shard, but no member's placement: every member rebinds its existing store
/// in place. The new primary commits through that store, the followers
/// acknowledge through theirs, and no copy is reopened.
#[test]
fn native_takeover_rebinds_unaffected_stores_in_place() {
    let (runtime, config) = switched("native-takeover-rebind", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    client_write(&runtime, &config, &c, "/services/rebind-before.txt");
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let before = members(&config)
        .into_iter()
        .map(|node| (node.clone(), opens(&runtime, &node, &config)))
        .collect::<Vec<_>>();
    hand_takeover(&runtime, &config, &b);
    let after = client_write(&runtime, &config, &c, "/services/rebind-after.txt");
    let current = cut(&runtime, &config);
    assert_eq!(current.shard.primary_node_id, b);
    wait_for("the new primary's commit reaches its store", || {
        holds_object(&runtime, &b, &config, &after)
    });
    for (node, count) in before {
        assert_eq!(
            opens(&runtime, &node, &config),
            count,
            "{node} rebinds its copy in place instead of reopening it"
        );
    }
    runtime.close().unwrap();
}

/// Freeze markers the record moved past are removed: the candidate's
/// activated marker from an earlier handoff no longer fences its writer, and
/// the old primary's pending marker for a handoff the swap deleted no longer
/// refuses the new primary's journal.
#[test]
fn native_takeover_clears_freeze_markers_the_record_moved_past() {
    let (runtime, config) = switched("native-takeover-markers", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let shard_id = shard_id_of(&config);
    client_write(&runtime, &config, &c, "/services/markers-before.txt");
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // B keeps an activated marker at the current epoch, as the retiring side
    // of an earlier rotation would; P froze for a handoff that never ran.
    runtime
        .with_main_maintenance_service(&b, |service| {
            let backend = service.metadata_backend_mut().unwrap();
            backend
                .freeze_metadata_shard_for_membership_handoff(&shard_id, "earlier", config.epoch - 1)
                .unwrap();
            backend
                .mark_metadata_shard_membership_handoff_activated(&shard_id, "earlier", config.epoch)
                .unwrap()
        })
        .unwrap();
    runtime
        .with_main_maintenance_service(&p, |service| {
            service
                .metadata_backend_mut()
                .unwrap()
                .freeze_metadata_shard_for_membership_handoff(&shard_id, "abandoned", config.epoch)
                .unwrap()
        })
        .unwrap();
    let marker = |node: &NodeId| {
        shard_root(&runtime, node, &config)
            .join("journal")
            .join("membership-handoff-write-freeze.sobj")
    };
    assert!(marker(&b).exists() && marker(&p).exists());

    hand_takeover(&runtime, &config, &b);
    // B's writer is not fenced by its earlier marker.
    let after = client_write(&runtime, &config, &c, "/services/markers-after.txt");
    wait_for("the new primary commits", || holds_object(&runtime, &b, &config, &after));
    assert!(!marker(&b).exists(), "B's activated marker is removed");
    // P follows the new primary: its pending marker no longer refuses ingest.
    catch_up(&runtime, &config, &p);
    assert!(!marker(&p).exists(), "P's pending marker is removed");
    assert!(holds_object(&runtime, &p, &config, &after));
    runtime.close().unwrap();
}
