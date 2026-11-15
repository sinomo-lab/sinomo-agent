//! Takeover of a write-set shard's primary on the native test runtime: real
//! Raft-backed cluster KV, real shard stores and the real member paths.

use super::commit_native_tests::{
    catch_up, cut, directory, durable_prefix, fail_journal_calls, members, mode_change, primary,
    shard_root, shrink, switched,
};
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

/// Align `member` with the current epoch start from `primary`'s resident
/// service, as the new primary's keeper does.
fn align(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    primary: &NodeId,
    member: &NodeId,
) -> crate::DfsWriteSetAlignment {
    let record = cut(runtime, config);
    let data_dir = runtime.data_dir(primary).clone();
    let handle = runtime.runtime_handle();
    runtime
        .with_main_maintenance_service(primary, |service| {
            let (peer, _) = crate::runtime_host::daemon_dfs_peer_transports_on_runtime(&data_dir, &handle);
            let sender =
                crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
                    &data_dir, peer,
                );
            service.align_write_set_member(&record, member, &sender)
        })
        .unwrap()
        .unwrap()
}

/// Commit one mode change to `current` at the current primary with a short
/// caller deadline; members whose journal routes are faulted leave it
/// prepared.
fn prepare_unknown(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    current: &crate::DfsObjectMetadata,
    operation: &str,
) {
    let mut service = primary(runtime);
    service
        .transport
        .set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));
    let outcome = service.publish_prepared_operation(mode_change(config, current, 0o640, operation));
    service.transport.set_operation_deadline(None);
    match outcome {
        Err(crate::DfsError::MetadataCommitOutcomeUnknown { .. }) => {}
        Err(other) => panic!("{operation} stays prepared, not {other:?}"),
        Ok(_) => panic!("{operation} stays prepared, not committed"),
    }
}

/// After a takeover the new primary aligns the members with its epoch start.
/// The old primary's tail past it, a change nobody else acknowledged, is
/// dropped and its journal ends exactly at the epoch start. A member whose
/// journal ends before the epoch start reports its head; the new primary
/// checks it against its own journal and confirms it. Both then follow the
/// new primary incrementally: a write commits with the behind member's
/// acknowledgement and no snapshot is installed.
#[test]
fn native_takeover_aligns_members_with_the_epoch_start() {
    let (runtime, config) = switched("native-takeover-align", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    client_write(&runtime, &config, &c, "/services/align-before.txt");
    let (z, z2) = {
        let mut service = primary(&runtime);
        (
            directory(&mut service, "/services/align-z"),
            directory(&mut service, "/services/align-z2"),
        )
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // Z reaches C but not B; Z2 reaches nobody.
    fail_journal_calls(&runtime, &config, &b);
    prepare_unknown(&runtime, &config, &z, "align-z");
    fail_journal_calls(&runtime, &config, &c);
    prepare_unknown(&runtime, &config, &z2, "align-z2");
    let (_, start) = hand_takeover(&runtime, &config, &c);
    runtime.clear_faults();
    assert!(durable_prefix(&runtime, &p, &config).unwrap().journal_sequence > start.journal_sequence);
    assert!(durable_prefix(&runtime, &b, &config).unwrap().journal_sequence < start.journal_sequence);

    // The old primary drops its unacknowledged tail.
    match align(&runtime, &config, &c, &p) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, start),
        other => panic!("the old primary aligns: {other:?}"),
    }
    assert_eq!(durable_prefix(&runtime, &p, &config).as_ref(), Some(&start));
    // The behind member is checked and confirmed below the epoch start.
    let behind = durable_prefix(&runtime, &b, &config).unwrap();
    match align(&runtime, &config, &c, &b) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, behind),
        other => panic!("the behind member is confirmed: {other:?}"),
    }
    // Both follow incrementally; no snapshot is installed.
    let after = client_write(&runtime, &config, &b, "/services/align-after.txt");
    wait_for("the new primary commits with B", || {
        holds_object(&runtime, &c, &config, &after)
    });
    for (node, journal) in [&p, &b].map(|node| (node.clone(), shard_root(&runtime, node, &config))) {
        assert!(
            !std::fs::read_dir(&journal)
                .unwrap()
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().contains("archive")),
            "{node} kept its store: no snapshot install archived it"
        );
    }
    runtime.close().unwrap();
}

/// A write-set member whose journal runs past the epoch start aligns inline
/// on the new primary's first batch of the epoch: under the same writer lock
/// it drops its unacknowledged tail and then ingests, so the first write of
/// the epoch commits with it and its journal becomes the new primary's.
#[test]
fn native_takeover_member_aligns_inline_on_the_new_epochs_first_batch() {
    let (runtime, config) = switched("native-takeover-inline", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    client_write(&runtime, &config, &c, "/services/inline-before.txt");
    let z = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/inline-z")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // Z reaches B but not C.
    fail_journal_calls(&runtime, &config, &c);
    prepare_unknown(&runtime, &config, &z, "inline-z");
    let (_, start) = hand_takeover(&runtime, &config, &c);
    runtime.clear_faults();
    assert!(durable_prefix(&runtime, &b, &config).unwrap().journal_sequence > start.journal_sequence);

    // No alignment probe is sent: the epoch's first write reaches B first.
    let after = client_write(&runtime, &config, &b, "/services/inline-after.txt");
    wait_for("B holds the new primary's write", || {
        holds_object(&runtime, &b, &config, &after)
    });
    wait_for("B's journal is the new primary's", || {
        durable_prefix(&runtime, &b, &config) == durable_prefix(&runtime, &c, &config)
    });
    assert!(
        !std::fs::read_dir(shard_root(&runtime, &b, &config))
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().contains("archive")),
        "B kept its store: no snapshot install archived it"
    );
    runtime.close().unwrap();
}

/// Alignment writes the new epoch's floors before it cuts the journal. A
/// crash between the two leaves floors at the epoch start below the head: the
/// store opens, and the next alignment cuts.
#[test]
fn native_takeover_alignment_cuts_after_a_crash_between_its_floors_and_the_cut() {
    let (mut runtime, config) = switched("native-takeover-align-crash", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let shard_id = shard_id_of(&config);
    client_write(&runtime, &config, &c, "/services/crash-before.txt");
    let tail = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/crash-tail")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // The tail reaches nobody but the old primary.
    fail_journal_calls(&runtime, &config, &b);
    fail_journal_calls(&runtime, &config, &c);
    prepare_unknown(&runtime, &config, &tail, "crash-tail");
    let (_, start) = hand_takeover(&runtime, &config, &c);
    runtime.clear_faults();
    let epoch = cut(&runtime, &config).shard.epoch;

    // The crash cut: P wrote both floors at the epoch start and kept its tail.
    runtime
        .with_main_maintenance_service(&p, |service| {
            let backend = service.metadata_backend_mut().unwrap();
            backend.put_metadata_prefix_ack_floor(&crate::DfsMetadataPrefixAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: start.journal_sequence,
                state_fingerprint: String::new(),
                durable_prefix_proof: start.clone(),
                authority_epoch: Some(epoch),
            })?;
            backend.put_metadata_authority_ack_floor(&crate::DfsMetadataAuthorityAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: start.journal_sequence,
                durable_prefix_proof: start.clone(),
                authority_epoch: Some(epoch),
            })
        })
        .unwrap()
        .unwrap();
    runtime.stop_node(&p).unwrap();
    runtime.reopen_node(p.clone()).unwrap();
    assert!(
        durable_prefix(&runtime, &p, &config).unwrap().journal_sequence > start.journal_sequence,
        "the reopened store keeps its tail"
    );

    match align(&runtime, &config, &c, &p) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, start),
        other => panic!("the next alignment cuts: {other:?}"),
    }
    assert_eq!(durable_prefix(&runtime, &p, &config).as_ref(), Some(&start));
    runtime.close().unwrap();
}
