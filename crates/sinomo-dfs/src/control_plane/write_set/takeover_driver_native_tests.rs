//! The takeover driver on the native test runtime: write-set members judge
//! the primary from their own verdicts, corroborate, promise, swap and
//! activate on their own, over real Raft-backed cluster KV and real stores.

use super::commit_native_tests::{
    catch_up, directory, fail_journal_calls, members, primary, shard_root, shrink, switched,
};
use super::takeover_native_tests::{
    holds_object, member_state_path, prepare_unknown, shard_id_of, wait_for,
};
use super::*;
use crate::DfsMetadataTakeoverEvent;
use crate::test_support::DfsTestRuntime;
use sinomo_core::network::PeerPathVerdict;
use std::time::{Duration, Instant};

/// The shard's record as `reader` reads it from cluster KV.
fn record_from(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    reader: &NodeId,
) -> DfsMetadataShardRecordCut {
    let mut client = runtime.configuration_publisher(reader);
    DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(&config.file_system_id, &config.shard_id, &members(config))
        .unwrap()
        .expect("the shard exists")
}

/// Every member sees a path to every other; `keepers` keep their shards.
fn keep_with_paths(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, keepers: &[&NodeId]) {
    let nodes = members(config);
    for observer in &nodes {
        for peer in nodes.iter().filter(|peer| *peer != observer) {
            runtime.set_peer_verdict(observer, peer, PeerPathVerdict::Path, false);
        }
    }
    for keeper in keepers {
        runtime.enable_write_set_keeping(keeper);
    }
}

fn event_log(runtime: &DfsTestRuntime, nodes: &[&NodeId]) -> String {
    nodes
        .iter()
        .map(|node| {
            let names = runtime
                .write_set_takeover_events(node)
                .into_iter()
                .map(|(name, event)| {
                    format!(
                        "{name}:{}:{:?}:{:?}",
                        event.cause, event.node_ids, event.answers
                    )
                })
                .collect::<Vec<_>>();
            format!(
                "{node}: {names:?}; failures {:?}; trace {:?}",
                runtime.write_set_keeping_failures(node),
                runtime.write_set_takeover_trace(node)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn lose(runtime: &DfsTestRuntime, observers: &[&NodeId], lost: &NodeId) {
    for observer in observers {
        runtime.set_peer_verdict(observer, lost, PeerPathVerdict::NoPath, false);
    }
}

fn takeover_events(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    name: &str,
) -> Vec<DfsMetadataTakeoverEvent> {
    runtime
        .write_set_takeover_events(node)
        .into_iter()
        .filter(|(event, _)| event == name)
        .map(|(_, event)| event)
        .collect()
}

/// Write `path` through `node`'s client to whichever member `reader` reads
/// as primary; retried while the record moves under it.
fn write_via(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    node: &NodeId,
    reader: &NodeId,
    path: &str,
) -> crate::DfsObjectId {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let primary = record_from(runtime, config, reader).shard.primary_node_id;
        let written = runtime.client(node).unwrap().posix_write_file(
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

fn object_mode(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    object_id: &crate::DfsObjectId,
) -> Option<u32> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .ok()?
    .get_object(object_id)
    .ok()
    .flatten()
    .map(|object| object.mode)
}

/// TK1: the primary is lost while a change it prepared is durable on one
/// member only. That member has the longer journal, ranks first and takes
/// over: it promises, swaps and activates, and the reconciliation commits the
/// prepared change at the new epoch. A new write commits on the remaining
/// write set; the old primary stays configured outside it, its copy intact.
///
/// The old primary stays up with every member judging it lost: in this
/// fixture it also leads cluster KV, which a takeover needs.
#[test]
fn native_takeover_successor_completes_a_prepared_change() {
    let (runtime, config) = switched("native-driver-loss", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/loss")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // X is prepared on P and C; B never sees it.
    fail_journal_calls(&runtime, &config, &b);
    prepare_unknown(&runtime, &config, &current, "loss-x");
    runtime.clear_faults();
    keep_with_paths(&runtime, &config, &[&b, &c]);

    lose(&runtime, &[&b, &c], &p);
    let deadline = Instant::now() + Duration::from_secs(60);
    while record_from(&runtime, &config, &b).shard.primary_node_id != c {
        assert!(
            Instant::now() < deadline,
            "C takes over: {:?}\n{}",
            record_from(&runtime, &config, &b).shard.primary_node_id,
            event_log(&runtime, &[&b, &c])
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let record = record_from(&runtime, &config, &b);
    assert_eq!(record.shard.epoch, config.epoch + 1);
    let write_set = record.write_set_state().switched().cloned().unwrap();
    assert_eq!(write_set.write_set, {
        let mut remaining = vec![b.clone(), c.clone()];
        remaining.sort();
        remaining
    });
    wait_for("the prepared change commits at the new epoch", || {
        object_mode(&runtime, &c, &config, &current.object_id) == Some(0o640)
    });
    wait_for("C activates", || {
        !takeover_events(&runtime, &c, DfsMetadataTakeoverEvent::ACTIVATED).is_empty()
    });
    for name in [
        DfsMetadataTakeoverEvent::CORROBORATION_STARTED,
        DfsMetadataTakeoverEvent::PROMISED,
        DfsMetadataTakeoverEvent::SWAPPED,
    ] {
        assert_eq!(takeover_events(&runtime, &c, name).len(), 1, "{name}");
    }
    assert!(takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::SWAPPED).is_empty());
    assert!(
        shard_root(&runtime, &p, &config).exists(),
        "P's copy is left intact"
    );

    let after = write_via(&runtime, &config, &b, &b, "/services/loss-after.txt");
    wait_for("a write commits on the remaining write set", || {
        holds_object(&runtime, &c, &config, &after) && holds_object(&runtime, &b, &config, &after)
    });
    wait_for("the new primary's fence is retired", || {
        !member_state_path(&runtime, &c, &config).exists()
    });
    runtime.close().unwrap();
}

/// TK3: a member that still reaches the primary vetoes the attempt: nothing
/// is promised and the record is unchanged. Once that member judges the
/// primary lost too, its own attempt proceeds.
#[test]
fn native_takeover_is_vetoed_by_a_member_that_reaches_the_primary() {
    let (runtime, config) = switched("native-driver-veto", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    lose(&runtime, &[&b], &p);
    wait_for("C vetoes B's attempt", || {
        takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::CORROBORATION_VETOED)
            .iter()
            .any(|event| event.node_ids == vec![c.clone()])
    });
    assert!(
        takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::PROMISED).is_empty(),
        "{}",
        event_log(&runtime, &[&b, &c])
    );
    assert!(
        !member_state_path(&runtime, &b, &config).exists(),
        "B promised nothing"
    );
    let record = record_from(&runtime, &config, &p);
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (p.clone(), config.epoch)
    );

    lose(&runtime, &[&c], &p);
    wait_for("the takeover proceeds", || {
        let primary = record_from(&runtime, &config, &b).shard.primary_node_id;
        primary == b || primary == c
    });
    runtime.close().unwrap();
}

/// TK4, TK10 and TK13: with no traffic at all, the verdict alone starts a
/// takeover. Both remaining members judge the primary lost at once, and
/// exactly one swap applies: the epoch advances by one. A two-member write
/// set asks nobody.
#[test]
fn native_takeover_on_the_verdict_alone_produces_exactly_one_swap() {
    let (runtime, config) = switched("native-driver-idle", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    lose(&runtime, &[&b, &c], &p);
    wait_for("one member takes over", || {
        record_from(&runtime, &config, &p).shard.primary_node_id != p
    });
    let record = record_from(&runtime, &config, &p);
    let winner = record.shard.primary_node_id.clone();
    assert_eq!(record.shard.epoch, config.epoch + 1);
    let deadline = Instant::now() + Duration::from_secs(60);
    while takeover_events(&runtime, &winner, DfsMetadataTakeoverEvent::ACTIVATED).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the winner activates:\n{}",
            event_log(&runtime, &[&b, &c])
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let swaps = [&b, &c]
        .into_iter()
        .map(|node| takeover_events(&runtime, node, DfsMetadataTakeoverEvent::SWAPPED).len())
        .sum::<usize>();
    assert_eq!(
        swaps,
        1,
        "exactly one swap applies:\n{}",
        event_log(&runtime, &[&b, &c])
    );
    assert_eq!(
        takeover_events(&runtime, &winner, DfsMetadataTakeoverEvent::SWAPPED).len(),
        1
    );
    // TK12: once settled, no member keeps a fence or a promise, and the idle
    // cluster sends nothing for the shard.
    wait_for("every fence is retired", || {
        [&b, &c]
            .into_iter()
            .all(|node| !member_state_path(&runtime, node, &config).exists())
    });
    let quiet = |runtime: &DfsTestRuntime| {
        runtime
            .rpc_observations()
            .iter()
            .filter(|event| {
                event.route == crate::DFS_METADATA_TAKEOVER_CORROBORATION_ROUTE
                    || event.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    || event.route == sinomo_core::kv::CLUSTER_KV_TXN_ROUTE
            })
            .count()
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let before = quiet(&runtime);
        std::thread::sleep(Duration::from_millis(1500));
        if quiet(&runtime) == before {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cluster does not go quiet after the takeover"
        );
    }
    runtime.close().unwrap();
}

#[test]
fn native_takeover_of_a_two_member_write_set_asks_nobody() {
    let (runtime, config) = switched("native-driver-two", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    // P keeps no shard, so C stays outside the write set.
    keep_with_paths(&runtime, &config, &[&b, &c]);
    // C is outside the write set: only B may take over, and it asks nobody.
    lose(&runtime, &[&b, &c], &p);
    wait_for("B takes over", || {
        record_from(&runtime, &config, &p).shard.primary_node_id == b
    });
    assert!(
        takeover_events(
            &runtime,
            &c,
            DfsMetadataTakeoverEvent::CORROBORATION_STARTED
        )
        .is_empty()
    );
    let started = takeover_events(
        &runtime,
        &b,
        DfsMetadataTakeoverEvent::CORROBORATION_STARTED,
    );
    assert!(
        started.iter().all(|event| event.node_ids.is_empty()),
        "{started:?}"
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .all(|event| event.route != crate::DFS_METADATA_TAKEOVER_CORROBORATION_ROUTE),
        "no corroboration call"
    );
    runtime.close().unwrap();
}

/// TK11: a primary every member still reaches is never deposed, however its
/// writes go: no attempt starts.
#[test]
fn native_takeover_never_deposes_a_reachable_primary() {
    let (runtime, config) = switched("native-driver-reachable", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&p, &b, &c]);
    for index in 0..5 {
        write_via(
            &runtime,
            &config,
            &c,
            &p,
            &format!("/services/reachable-{index}.txt"),
        );
    }
    for node in [&b, &c] {
        assert!(
            runtime.write_set_takeover_events(node).is_empty(),
            "{node} started nothing"
        );
    }
    let record = record_from(&runtime, &config, &p);
    assert_eq!(
        (record.shard.primary_node_id, record.shard.epoch),
        (p, config.epoch)
    );
    runtime.close().unwrap();
}

/// TK16: a candidate that stops between its durable promise and its swap
/// resumes the identical swap when it restarts: one promise, one swap.
#[test]
fn native_takeover_resumes_the_identical_swap_after_a_crash_after_the_promise() {
    let (mut runtime, config) = switched("native-driver-resume", Some((1, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    keep_with_paths(&runtime, &config, &[&b]);
    runtime.stop_takeover_after_promise(&b, true);
    lose(&runtime, &[&b], &p);
    wait_for("B promises", || {
        member_state_path(&runtime, &b, &config).exists()
    });
    let promised = takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::PROMISED);
    assert!(
        promised.is_empty(),
        "the swap's outcome reports the promise"
    );
    let record = record_from(&runtime, &config, &p);
    assert_eq!(record.shard.primary_node_id, p, "nothing was submitted");

    runtime.stop_node(&b).unwrap();
    runtime.stop_takeover_after_promise(&b, false);
    runtime.reopen_node(b.clone()).unwrap();
    wait_for("B's resumed swap applies", || {
        record_from(&runtime, &config, &p).shard.primary_node_id == b
    });
    // The swap is reported once its promise has ended, which can follow the
    // publication of the new record.
    wait_for("B reports its swap", || {
        !takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::SWAPPED).is_empty()
    });
    let swapped = takeover_events(&runtime, &b, DfsMetadataTakeoverEvent::SWAPPED);
    assert_eq!(swapped.len(), 1, "{}", event_log(&runtime, &[&b]));
    assert_eq!(swapped[0].cause, "restart-resume");
    let record = record_from(&runtime, &config, &p);
    assert_eq!(
        record.shard.epoch,
        config.epoch + 1,
        "exactly one epoch advance"
    );
    // The new primary's keeper may already have changed the record further.
    let last = record
        .write_set_state()
        .switched()
        .unwrap()
        .last_change
        .clone();
    if matches!(
        last.kind,
        crate::DfsMetadataShardWriteSetChangeKind::Takeover { .. }
    ) {
        assert_eq!(Some(last.change_id), swapped[0].swap_request_id);
    }
    wait_for("B's fence is retired", || {
        !member_state_path(&runtime, &b, &config).exists()
    });
    runtime.close().unwrap();
}

/// TK18: a primary the cluster no longer authorizes has no verdict at all; its
/// departure alone starts the takeover.
#[test]
fn native_takeover_of_a_departed_primary() {
    let (runtime, config) = switched("native-driver-departed", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    let member_key = sinomo_core::node::cluster_member_key(&p);
    let mut client = runtime.configuration_publisher(&b);
    let entry = client
        .get_authoritative_consistent(std::slice::from_ref(&member_key))
        .unwrap()
        .remove(0)
        .unwrap();
    let mut member = entry.decode::<sinomo_core::ClusterMemberRecord>().unwrap();
    member.state = sinomo_core::ClusterMemberState::Removed;
    let response = client
        .txn(sinomo_core::kv::KvTxn {
            compares: vec![sinomo_core::kv::KvCompare::ValueEquals {
                key: member_key.clone(),
                value: entry.value,
            }],
            success: vec![sinomo_core::kv::KvOp::put_serde(member_key, &member, None).unwrap()],
            failure: Vec::new(),
        })
        .unwrap();
    assert!(response.succeeded);
    wait_for("a member takes over the departed primary", || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    let started = runtime
        .write_set_takeover_events(&winner)
        .into_iter()
        .filter(|(name, _)| name == DfsMetadataTakeoverEvent::CORROBORATION_STARTED)
        .map(|(_, event)| event.cause)
        .collect::<Vec<_>>();
    assert!(
        started.iter().any(|cause| cause == "member-departed"),
        "{started:?}"
    );
    runtime.close().unwrap();
}

/// TK19: a request this node could not deliver to the primary asks the
/// network to verify it; the settled verdict then starts the takeover.
#[test]
fn native_takeover_delivery_failure_asks_the_network_to_verify_the_primary() {
    let (runtime, config) = switched("native-driver-hint", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(b.clone()),
        target: Some(p.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 16,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    let refused = runtime
        .client(&b)
        .unwrap()
        .posix_mkdir_all("/services/hint");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !runtime.peer_verification_requests(&b).contains(&p) {
        assert!(
            Instant::now() < deadline,
            "B asks the network to verify P after {refused:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    runtime.clear_faults();
    lose(&runtime, &[&b, &c], &p);
    wait_for("the settled verdict starts the takeover", || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    runtime.close().unwrap();
}

/// TK5, Q1: the deposed primary is never made primary again automatically.
/// Once the new primary reaches it, its keeper aligns it with the epoch
/// start, which drops the old primary's unacknowledged tail, catches it up
/// and re-adds it to the write set as an ordinary member.
#[test]
fn native_takeover_old_primary_rejoins_after_dropping_its_tail() {
    // N is three, so the old primary is wanted back rather than surplus.
    let (runtime, config) = switched("native-driver-rejoin", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/rejoin")
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    // The tail reaches nobody but the old primary.
    fail_journal_calls(&runtime, &config, &b);
    fail_journal_calls(&runtime, &config, &c);
    prepare_unknown(&runtime, &config, &current, "rejoin-tail");
    runtime.clear_faults();
    let tail = super::commit_native_tests::durable_prefix(&runtime, &p, &config)
        .unwrap()
        .journal_sequence;
    let candidate = super::commit_native_tests::durable_prefix(&runtime, &c, &config)
        .unwrap()
        .journal_sequence;
    assert!(
        candidate < tail,
        "the old primary holds a tail no member has"
    );
    keep_with_paths(&runtime, &config, &[&b, &c]);
    lose(&runtime, &[&b, &c], &p);
    wait_for("a member takes over", || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    let start = record_from(&runtime, &config, &b)
        .write_set_state()
        .switched()
        .and_then(|write_set| write_set.epoch_start.clone())
        .expect("the takeover epoch names its start");
    assert!(
        start.journal_sequence < tail,
        "the old primary's tail is past the start"
    );

    // The new primary reaches P again: P is aligned, caught up and re-added.
    runtime.set_peer_verdict(&winner, &p, PeerPathVerdict::Path, false);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !record_from(&runtime, &config, &b)
        .write_set_state()
        .switched()
        .is_some_and(|write_set| write_set.contains(&p))
    {
        assert!(
            Instant::now() < deadline,
            "P rejoins the write set: {:?}\n{}\nkeeping {:?}",
            record_from(&runtime, &config, &b).write_set_state(),
            event_log(&runtime, &[&b, &c]),
            runtime.write_set_keeping_events(&winner)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        record.shard.primary_node_id, winner,
        "P is not made primary again"
    );
    let mut service = crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(&config),
        shard_root(&runtime, &p, &config),
    )
    .unwrap();
    use crate::DfsMetadataBackend as _;
    assert_ne!(
        service
            .prepared_operation_record("rejoin-tail")
            .ok()
            .flatten()
            .map(|record| record.status),
        Some(crate::DfsPreparedOperationStatus::Committed),
        "the old primary's own tail did not survive"
    );
    let _ = &mut service;
    runtime.close().unwrap();
}
