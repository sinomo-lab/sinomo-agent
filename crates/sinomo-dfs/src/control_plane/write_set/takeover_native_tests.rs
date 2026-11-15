//! Takeover of a write-set shard's primary on the native test runtime: real
//! Raft-backed cluster KV, real shard stores and the real member paths.

use super::commit_native_tests::{
    catch_up, cut, directory, durable_prefix, fail_journal_calls, members, mode_change, primary,
    shard_root, shrink, switched, write_set_of,
};
use super::*;
use crate::DfsMetadataShardId;
use sinomo_core::kv::KvRequestId;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(super) fn shard_id_of(config: &DfsMetadataShardConfig) -> DfsMetadataShardId {
    DfsMetadataShardId::new(config.shard_id.clone())
}

pub(super) fn member_state_path(
    runtime: &crate::test_support::DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> std::path::PathBuf {
    shard_root(runtime, node, config)
        .join("coordination")
        .join("write-set-member-state.sobj")
}

pub(super) fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
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
    assert!(
        member_state_path(&runtime, &b, &config).exists(),
        "B keeps its fence"
    );

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
    write_retried(runtime, config, path, |primary| {
        runtime.client(node).unwrap().posix_write_file(
            path,
            path.as_bytes(),
            crate::DfsWriteOptions::replicated_eventual(vec![primary]),
        )
    })
}

/// [`client_write`] through a client opened without first waiting for every
/// node's startup pass, which a node whose lane the test holds cannot finish.
fn client_write_beside_a_held_node(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    node: &NodeId,
    path: &str,
) -> crate::DfsObjectId {
    write_retried(runtime, config, path, |primary| {
        crate::runtime_host::open_dfs_runtime_service(
            runtime.data_dir(node),
            Some(crate::test_runtime::executor().handle()),
        )
        .unwrap()
        .posix_write_file(
            path,
            path.as_bytes(),
            crate::DfsWriteOptions::replicated_eventual(vec![primary]),
        )
    })
}

fn write_retried<E: std::fmt::Display>(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    path: &str,
    mut write: impl FnMut(NodeId) -> Result<crate::DfsStat, E>,
) -> crate::DfsObjectId {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match write(cut(runtime, config).shard.primary_node_id) {
            Ok(written) => return written.object_id,
            Err(error) => {
                assert!(Instant::now() < deadline, "{path} was not written: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

pub(super) fn holds_object(
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

/// A member whose resident router still names a switched shard's previous
/// epoch answers the new primary's batches while local exact work holds one of
/// its lanes, as a keeper join or a commit holds its lane while it waits on a
/// peer. A global writer on that member waits for the held lane, so the answer
/// must not need one: it rebinds that one shard in place under its own lane.
#[test]
fn native_takeover_member_answers_the_new_epoch_while_a_local_lane_is_held() {
    let (runtime, config) = switched("native-takeover-held-lane", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    client_write(&runtime, &config, &c, "/services/held-lane-before.txt");
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // No global writer runs on C from here until the hold ends, so nothing on
    // C rebinds the shard to the new epoch but C's answers themselves. The
    // hold ends when released, or after a bound past any wait below.
    let held = runtime
        .hold_main_exact_admission(&c, &config.file_system_id, "native-held-lane")
        .unwrap();
    let ended = Arc::new(AtomicBool::new(false));
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = {
        let ended = Arc::clone(&ended);
        std::thread::spawn(move || {
            let _ = released.recv_timeout(Duration::from_secs(90));
            ended.store(true, Ordering::SeqCst);
            drop(held);
        })
    };
    hand_takeover(&runtime, &config, &b);
    // Every member of the write set acknowledges a commit, so the write
    // returns only once C has answered the new epoch's batches. Applying them
    // to C's object table is C's own later work, which waits for the lane.
    let after =
        client_write_beside_a_held_node(&runtime, &config, &b, "/services/held-lane-after.txt");
    let answered_under_hold = !ended.load(Ordering::SeqCst);
    let _ = release.send(());
    holder.join().unwrap();
    assert!(
        answered_under_hold,
        "C answers the new epoch's batches while one of its lanes is held"
    );
    assert!(write_set_of(&runtime, &config).contains(&c));
    wait_for("C applies the write once its lane is free", || {
        holds_object(&runtime, &c, &config, &after)
    });
    assert_eq!(cut(&runtime, &config).shard.primary_node_id, b);
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
                .freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    "earlier",
                    config.epoch - 1,
                )
                .unwrap();
            backend
                .mark_metadata_shard_membership_handoff_activated(
                    &shard_id,
                    "earlier",
                    config.epoch,
                )
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
    wait_for("the new primary commits", || {
        holds_object(&runtime, &b, &config, &after)
    });
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
            let (peer, _) =
                crate::runtime_host::daemon_dfs_peer_transports_on_runtime(&data_dir, &handle);
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
pub(super) fn prepare_unknown(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    current: &crate::DfsObjectMetadata,
    operation: &str,
) {
    let mut service = primary(runtime);
    service
        .transport
        .set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));
    let outcome =
        service.publish_prepared_operation(mode_change(config, current, 0o640, operation));
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
    // Before the takeover nothing else writes these journals: P runs past
    // C's head and B ends before it.
    let candidate = durable_prefix(&runtime, &c, &config)
        .unwrap()
        .journal_sequence;
    assert!(
        durable_prefix(&runtime, &p, &config)
            .unwrap()
            .journal_sequence
            > candidate
    );
    let behind = durable_prefix(&runtime, &b, &config).unwrap();
    assert!(behind.journal_sequence < candidate);
    let (_, start) = hand_takeover(&runtime, &config, &c);
    assert_eq!(start.journal_sequence, candidate);
    runtime.clear_faults();

    // The old primary drops its unacknowledged tail.
    match align(&runtime, &config, &c, &p) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, start),
        other => panic!("the old primary aligns: {other:?}"),
    }
    assert_eq!(durable_prefix(&runtime, &p, &config).as_ref(), Some(&start));
    // The behind member is checked and confirmed below the epoch start.
    match align(&runtime, &config, &c, &b) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, behind),
        other => panic!("the behind member is confirmed: {other:?}"),
    }
    // Both follow incrementally; no snapshot is installed.
    let after = client_write(&runtime, &config, &b, "/services/align-after.txt");
    wait_for("the new primary commits with B", || {
        holds_object(&runtime, &c, &config, &after)
    });
    for (node, journal) in [&p, &b].map(|node| (node.clone(), shard_root(&runtime, node, &config)))
    {
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
    let candidate = durable_prefix(&runtime, &c, &config)
        .unwrap()
        .journal_sequence;
    assert!(
        durable_prefix(&runtime, &b, &config)
            .unwrap()
            .journal_sequence
            > candidate
    );
    hand_takeover(&runtime, &config, &c);
    runtime.clear_faults();

    // No alignment probe is sent: the epoch's first batch to B aligns it.
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
    // The store opens with floors below its head.
    runtime.stop_node(&p).unwrap();
    runtime.reopen_node(p.clone()).unwrap();

    // The next alignment cuts: this probe, or an epoch batch from C that
    // reached P first and aligned it inline. C wrote nothing after the epoch
    // start, so either way P ends exactly there.
    match align(&runtime, &config, &c, &p) {
        crate::DfsWriteSetAlignment::Aligned { head } => assert_eq!(head, start),
        other => panic!("the next alignment cuts: {other:?}"),
    }
    assert_eq!(durable_prefix(&runtime, &p, &config).as_ref(), Some(&start));
    runtime.close().unwrap();
}

/// Hold the primary's journal calls to `member` before they reach it.
fn pause_journal_calls(
    runtime: &crate::test_support::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
) -> std::sync::Arc<tokio::sync::Semaphore> {
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(config.primary_node_id.clone()),
            target: Some(member.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::PauseBeforeDispatch(std::sync::Arc::clone(
                &gate,
            )),
        });
    }
    gate
}

/// Send `command` from `source`'s peer transport as its daemon would.
fn send_from(
    runtime: &crate::test_support::DfsTestRuntime,
    source: &NodeId,
    target: &NodeId,
    command: &crate::DfsMetadataJournalBatchCommand,
) -> crate::DfsMetadataJournalBatchResponse {
    let data_dir = runtime.data_dir(source).clone();
    let (peer, _) = crate::runtime_host::daemon_dfs_peer_transports_on_runtime(
        &data_dir,
        &runtime.runtime_handle(),
    );
    let sender = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        &data_dir, peer,
    );
    sender(target, command, None).expect("the member answers")
}

/// A member whose record moved past the old primary's epoch refuses its
/// prepare with that record. The old primary's attempt then ends unknown as a
/// record change naming the new primary, without a catch-up round, and its
/// body stays prepared. Any other command from the old epoch is refused the
/// same way, and one from an epoch the member has not reached says its view
/// is behind.
#[test]
fn native_takeover_member_refusal_names_the_new_primary_to_the_old_one() {
    let (runtime, config) = switched("native-takeover-refusal", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let shard_id = shard_id_of(&config);
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/refusal")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let mut service = primary(&runtime);
    // C first sees P's prepare after B took over.
    let gate = pause_journal_calls(&runtime, &config, &c);
    let payload = mode_change(&config, &current, 0o640, "refusal");
    let outcome = std::thread::scope(|scope| {
        let commit = scope.spawn(|| service.publish_prepared_operation(payload.clone()));
        std::thread::sleep(Duration::from_millis(500));
        hand_takeover(&runtime, &config, &b);
        gate.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
        commit.join().unwrap()
    });
    let record = cut(&runtime, &config);
    match outcome {
        Err(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch },
            current_primary_node_id,
            ..
        }) => {
            assert_eq!(epoch, record.shard.epoch);
            assert_eq!(current_primary_node_id, Some(b.clone()));
        }
        Err(other) => panic!("the attempt ends as a record change, not {other:?}"),
        Ok(_) => panic!("a deposed primary cannot commit"),
    }
    let prepared = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record("refusal")
        .unwrap()
        .expect("the body stays durable");
    assert_eq!(prepared.status, crate::DfsPreparedOperationStatus::Prepared);
    drop(service);
    runtime.clear_faults();

    let write_set = record.write_set_state().switched().cloned().unwrap();
    let expected = crate::DfsMetadataMemberRecordView {
        epoch: record.shard.epoch,
        generation: write_set.generation,
        primary_node_id: b.clone(),
    };
    let head = durable_prefix(&runtime, &c, &config).unwrap();
    for (epoch, code) in [
        (
            config.epoch,
            crate::DfsMetadataRejectionCode::SourceEpochSuperseded,
        ),
        (
            record.shard.epoch + 2,
            crate::DfsMetadataRejectionCode::WriteSetViewBehind,
        ),
    ] {
        let command = crate::metadata_replication::dfs_metadata_caught_up_probe_command_for(
            &p,
            &c,
            &config.file_system_id,
            &shard_id,
            epoch,
            &head,
        );
        let response = send_from(&runtime, &p, &c, &command);
        assert_eq!(response.result.rejection_code(), Some(code), "{response:?}");
        assert_eq!(response.member_record.as_ref(), Some(&expected));
    }
    runtime.close().unwrap();
}

/// A member that promised a takeover candidate refuses the primary's epoch.
/// The primary's attempt ends unknown as a record change with no other
/// primary known, and its keeper shrinks that member above the floor, so the
/// same body then commits without it. The candidate's swap can no longer
/// apply; once it releases its promise, the primary's next write brings it
/// back and the keeper re-adds it.
#[test]
fn native_takeover_promise_refuses_the_primary_whose_keeper_shrinks_the_candidate() {
    let (runtime, config) = switched("native-takeover-fenced", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let shard_id = shard_id_of(&config);
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/fenced")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    runtime.enable_write_set_keeping(&p);
    for member in [&b, &c] {
        runtime.set_peer_verdict(
            &p,
            member,
            sinomo_core::network::PeerPathVerdict::Path,
            false,
        );
    }
    // B promises a candidate's takeover; the swap is not submitted yet.
    let promised = cut(&runtime, &config);
    let request_id = KvRequestId::generate();
    let start = runtime
        .with_main_maintenance_service(&b, |service| {
            service
                .metadata_backend_mut()
                .unwrap()
                .promise_metadata_takeover(&shard_id, &promised, &request_id)
        })
        .unwrap()
        .expect("B promises");

    let payload = mode_change(&config, &current, 0o640, "fenced");
    let mut service = primary(&runtime);
    match service.publish_prepared_operation(payload.clone()) {
        Err(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch },
            current_primary_node_id: None,
            ..
        }) => assert_eq!(epoch, config.epoch + 1),
        Err(other) => panic!("the attempt ends as a record change, not {other:?}"),
        Ok(_) => panic!("a fenced member cannot acknowledge"),
    }
    drop(service);
    wait_for("P's keeper shrinks B", || {
        !super::commit_native_tests::write_set_of(&runtime, &config).contains(&b)
    });
    wait_for("the shrink's event names the fence as its cause", || {
        runtime
            .write_set_keeping_events(&p)
            .iter()
            .any(|(name, event)| {
                name == crate::DfsMetadataWriteSetEvent::SHRUNK
                    && event.cause == "member-fenced"
                    && event.node_ids.contains(&b)
            })
    });
    // The same body commits without B, by this retry or by the recovery pass
    // the shrink requests.
    let mut service = primary(&runtime);
    service
        .publish_prepared_operation(payload)
        .expect("the same body commits without B");
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .prepared_operation_record("fenced")
            .unwrap()
            .expect("the body is durable")
            .status,
        crate::DfsPreparedOperationStatus::Committed
    );
    drop(service);
    assert_eq!(cut(&runtime, &config).shard.epoch, config.epoch);

    // The candidate's swap compares the record it promised on, which moved.
    let swap = build_metadata_takeover_swap(&promised, &b, &start, &request_id).unwrap();
    let mut client = runtime.configuration_publisher(&b);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&swap),
        DfsMetadataWriteSetTxnOutcome::Rejected
    );
    runtime
        .with_main_maintenance_service(&b, |service| {
            service
                .metadata_backend_mut()
                .unwrap()
                .settle_metadata_takeover_promise(
                    &shard_id,
                    &request_id,
                    crate::DfsMetadataTakeoverPromiseEnd::Released,
                )
        })
        .unwrap()
        .unwrap();
    assert_eq!(cut(&runtime, &config).shard.epoch, config.epoch);
    client_write(&runtime, &config, &c, "/services/fenced-after.txt");
    wait_for("P's keeper re-adds B", || {
        super::commit_native_tests::write_set_of(&runtime, &config).contains(&b)
    });
    runtime.close().unwrap();
}

pub(super) fn corroboration_request(
    config: &DfsMetadataShardConfig,
    record: &DfsMetadataShardRecordCut,
    candidate: &NodeId,
) -> crate::takeover_corroboration::DfsMetadataTakeoverCorroborationRequest {
    crate::takeover_corroboration::DfsMetadataTakeoverCorroborationRequest {
        format: crate::takeover_corroboration::DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT,
        attempt_id: sinomo_core::Id::generate().to_string(),
        kind: crate::takeover_corroboration::DfsTakeoverCorroborationKind::Query,
        file_system_id: config.file_system_id.clone(),
        shard_id: shard_id_of(config),
        epoch: record.shard.epoch,
        write_set_generation: record.write_set_state().switched().unwrap().generation,
        primary_node_id: record.shard.primary_node_id.clone(),
        candidate_node_id: candidate.clone(),
    }
}

pub(super) fn ask(
    runtime: &crate::test_support::DfsTestRuntime,
    asker: &NodeId,
    member: &NodeId,
    request: &crate::takeover_corroboration::DfsMetadataTakeoverCorroborationRequest,
) -> crate::takeover_corroboration::DfsMetadataTakeoverCorroborationReply {
    crate::runtime_host::ask_dfs_takeover_corroboration_for_test(
        runtime.data_dir(asker),
        &runtime.runtime_handle(),
        member,
        request,
    )
    .expect("the member answers")
}

/// A member answers whether it still reaches the primary from its own record
/// and its own verdict: a path vetoes, a settled no-path is a loss that
/// carries its durable head, a later record tells the asker to follow, and a
/// caller that does not name itself as the candidate is refused.
#[test]
fn native_corroboration_answers_from_the_members_record_and_verdict() {
    use crate::takeover_corroboration::{DfsTakeoverAbstainReason, DfsTakeoverPrimaryReach};
    use sinomo_core::network::PeerPathVerdict;
    let (runtime, config) = switched("native-takeover-corroboration", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    client_write(&runtime, &config, &c, "/services/corroboration.txt");
    let head = catch_up(&runtime, &config, &c);
    let record = cut(&runtime, &config);
    let request = corroboration_request(&config, &record, &b);
    let expected = crate::DfsMetadataMemberRecordView {
        epoch: record.shard.epoch,
        generation: request.write_set_generation,
        primary_node_id: p.clone(),
    };

    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::Path, false);
    let reply = ask(&runtime, &b, &c, &request);
    assert_eq!(reply.answer, DfsTakeoverPrimaryReach::Reaches, "{reply:?}");
    assert_eq!(reply.member_node_id, c);
    assert_eq!(reply.attempt_id, request.attempt_id);
    assert_eq!(reply.record_view.as_ref(), Some(&expected));
    assert!(reply.verdict_evidence.is_some());
    // Unless its verdict already settled no path, the member asked the
    // network to verify the primary first.
    assert!(runtime.peer_verification_requests(&c).contains(&p));

    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::NoPath, false);
    let reply = ask(&runtime, &b, &c, &request);
    assert_eq!(reply.answer, DfsTakeoverPrimaryReach::Lost, "{reply:?}");
    assert_eq!(reply.durable_sequence, Some(head.journal_sequence));

    let stale = crate::takeover_corroboration::DfsMetadataTakeoverCorroborationRequest {
        epoch: record.shard.epoch - 1,
        ..request.clone()
    };
    let reply = ask(&runtime, &b, &c, &stale);
    assert_eq!(
        reply.answer,
        DfsTakeoverPrimaryReach::Abstain {
            reason: DfsTakeoverAbstainReason::NewerRecord
        }
    );
    assert_eq!(reply.record_view.as_ref(), Some(&expected));

    let forged = crate::takeover_corroboration::DfsMetadataTakeoverCorroborationRequest {
        candidate_node_id: c.clone(),
        ..request
    };
    assert_eq!(
        ask(&runtime, &b, &c, &forged).answer,
        DfsTakeoverPrimaryReach::Abstain {
            reason: DfsTakeoverAbstainReason::Malformed
        }
    );
    runtime.close().unwrap();
}

/// A member whose network is still verifying the primary answers once the
/// verdict settles, not from the verdict it held when it was asked.
#[test]
fn native_corroboration_waits_for_a_verdict_being_verified() {
    use crate::takeover_corroboration::DfsTakeoverPrimaryReach;
    use sinomo_core::network::PeerPathVerdict;
    let (runtime, config) = switched("native-takeover-corroboration-wait", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let record = cut(&runtime, &config);
    let request = corroboration_request(&config, &record, &b);
    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::Path, true);
    let data_dir = runtime.data_dir(&b).clone();
    let handle = runtime.runtime_handle();
    let reply = std::thread::scope(|scope| {
        let asked = scope.spawn(|| {
            crate::runtime_host::ask_dfs_takeover_corroboration_for_test(
                &data_dir, &handle, &c, &request,
            )
            .expect("the member answers")
        });
        std::thread::sleep(Duration::from_millis(500));
        assert!(!asked.is_finished(), "the answer waits for the verdict");
        runtime.set_peer_verdict(&c, &p, PeerPathVerdict::NoPath, false);
        asked.join().unwrap()
    });
    assert_eq!(reply.answer, DfsTakeoverPrimaryReach::Lost);
    runtime.close().unwrap();
}
