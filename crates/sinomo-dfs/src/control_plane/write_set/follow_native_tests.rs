//! A requester follows its shard's record, on the native test runtime: one
//! call under one operation id waits for the record to change and is sent
//! again to the primary the newer record names, and a re-send timer sends it
//! to the current primary again while the record has not changed.

use super::commit_native_tests::{
    catch_up, directory, fail_journal_calls, members, primary, release, switched,
    switched_with_kv_voters,
};
use super::takeover_driver_native_tests::{event_log, keep_with_paths, lose};
use super::takeover_loss_native_tests::{record_from, reelect, try_record_from, wait_until};
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, Partition};
use crate::test_support::DfsTestRuntime;
use sinomo_core::network::PeerPathVerdict;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

/// Long enough for a call that gives up at its first network error, or at
/// the network's liveness interval, to have returned.
const STILL_WAITING: Duration = Duration::from_secs(8);

/// The shard's own placement rule: W of 2 over its three members where the
/// test set that rule, so the content commits on the members the primary
/// serving the write reaches; otherwise one copy on the primary.
fn write_options(
    config: &DfsMetadataShardConfig,
    rule: Option<(usize, usize)>,
) -> crate::DfsWriteOptions {
    match rule {
        Some((w, n)) => crate::DfsWriteOptions {
            policy: crate::DfsReplicationPolicy::new(w, n).unwrap(),
            replica_candidates: members(config),
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
        None => crate::DfsWriteOptions {
            policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![config.primary_node_id.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    }
}

fn bytes_of(operation: &str) -> Vec<u8> {
    format!("{operation} bytes").into_bytes()
}

/// One content write to `path` from `from` under `operation`, on its own
/// thread, bounded only by `budget` when one is given.
fn write_from(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    rule: Option<(usize, usize)>,
    from: &NodeId,
    path: &'static str,
    operation: &'static str,
    budget: Option<Duration>,
) -> mpsc::Receiver<Result<crate::DfsStat, crate::DfsPosixError>> {
    let data_dir = runtime.data_dir(from).clone();
    let options = write_options(config, rule);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client = crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        )
        .expect("the requester opens its client");
        if let Some(budget) = budget {
            client.set_transport_operation_deadline(Some(Instant::now() + budget));
        }
        let written = client.posix_write_file_with_operation_id(
            path,
            &bytes_of(operation),
            options,
            operation,
        );
        let _ = sender.send(written);
    });
    receiver
}

/// How many calls carrying `operation` the requester made to `target`.
fn sends_to(runtime: &DfsTestRuntime, from: &NodeId, target: &NodeId, operation: &str) -> usize {
    runtime
        .rpc_observations()
        .iter()
        .filter(|call| {
            call.source == *from
                && call.target == *target
                && call.dfs_request_id.as_deref() == Some(operation)
        })
        .map(|call| call.call_id)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// The file `operation` wrote, read back through `reader`'s client: its
/// bytes, and the one version the operation id derives for `requester`.
fn assert_written_once(
    runtime: &DfsTestRuntime,
    reader: &NodeId,
    requester: &NodeId,
    path: &str,
    operation: &str,
    written: &crate::DfsStat,
) {
    let mut client = runtime.client(reader).unwrap();
    let started = Instant::now();
    let stat = loop {
        match client.posix_stat_distributed(path) {
            Ok(stat) => break stat,
            Err(error) => {
                assert!(
                    started.elapsed() < EVENT_BOUND,
                    "{reader} cannot read {path}: {error}"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    assert_eq!(stat.object_id, written.object_id, "{path} is one object");
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(requester, &stat.object_id, operation),
        "{path} holds the one version {operation} derives"
    );
    assert_eq!(
        stat.version, written.version,
        "the caller was answered with it"
    );
    assert_eq!(
        client.posix_read_file_distributed(path).unwrap(),
        bytes_of(operation),
        "{path} holds the operation's bytes"
    );
}

/// What a write that crossed a takeover returned. The write committed either
/// way. A caller without a metadata store then reads the committed object
/// from every shard's primary, and in these fixtures the cut-off node is
/// still primary of the shards that did not switch, so that read can fail
/// until the node is back; the same operation id then returns the result.
fn committed_across_takeover(
    outcome: Result<crate::DfsStat, crate::DfsPosixError>,
    log: impl FnOnce() -> String,
) -> Option<crate::DfsStat> {
    match outcome {
        Ok(stat) => Some(stat),
        Err(crate::DfsPosixError::Dfs(crate::DfsError::NetworkDelivery { message, .. }))
            if message.contains("the write committed; reading its result failed") =>
        {
            None
        }
        Err(error) => panic!(
            "the call follows the record to the new primary: {error}\n{}",
            log()
        ),
    }
}

/// Break the requester's write calls to the primary once the returned gate
/// opens, as the connection of a node that is cut off breaks: the caller
/// sees an ambiguous delivery and the primary sees its requester leave.
fn break_write_calls(
    runtime: &DfsTestRuntime,
    from: &NodeId,
    primary: &NodeId,
) -> Arc<tokio::sync::Semaphore> {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for route in [
        crate::DFS_DATA_NAMESPACE_ROUTE,
        crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    ] {
        runtime.install_fault(FaultRule {
            source: Some(from.clone()),
            target: Some(primary.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::BreakStreamDuringDispatch(Arc::clone(&gate)),
        });
    }
    gate
}

/// Wait until the primary is serving a content write of the requester's.
fn await_write_at_the_primary(runtime: &DfsTestRuntime, from: &NodeId, primary: &NodeId) {
    let started = Instant::now();
    while !runtime.rpc_observations().iter().any(|call| {
        call.source == *from
            && call.target == *primary
            && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
            && call.dfs_request_id.is_some()
    }) {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "the write did not reach the primary"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The requester's calls of the observation window by target, route, last
/// phase and error, with their counts, for a failure message.
fn calls_from(runtime: &DfsTestRuntime, from: &NodeId) -> Vec<(String, usize)> {
    let mut latest = std::collections::BTreeMap::new();
    for call in runtime.rpc_observations() {
        if call.source == *from {
            latest.insert(call.call_id, call);
        }
    }
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for call in latest.into_values() {
        *counts
            .entry(format!(
                "{} {} {:?} {:?} {:?}",
                call.target, call.route, call.phase, call.dfs_request_id, call.outcome_error
            ))
            .or_default() += 1;
    }
    counts.into_iter().collect()
}

/// Let the cut-off node back in: first its cluster KV, so it reads the record
/// that deposed it, then the rest, and wait until the new primary has it in
/// its write set again, so the cluster closes with every voter in one term.
fn heal(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    old: &NodeId,
    others: [&NodeId; 2],
) {
    runtime.partition_node(
        old,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    // Cut off, the node kept campaigning; its higher term makes the voters
    // elect again before reads succeed.
    wait_until(
        "the old primary reads the new record",
        runtime,
        &others,
        || {
            try_record_from(runtime, config, old)
                .is_some_and(|record| &record.shard.primary_node_id != old)
        },
    );
    runtime.partition_node(old, Partition::default());
    for observer in others {
        runtime.set_peer_verdict(observer, old, PeerPathVerdict::Path, false);
    }
    wait_until(
        "the old primary rejoins the write set",
        runtime,
        &others,
        || {
            try_record_from(runtime, config, others[0]).is_some_and(|record| {
                record
                    .write_set_state()
                    .switched()
                    .is_some_and(|write_set| write_set.contains(old))
            })
        },
    );
    for node in [old, others[0], others[1]] {
        reelect(runtime, node);
    }
}

/// One caller, one operation id, across a real takeover: the primary is cut
/// off while it serves the write, and the caller's call to it breaks with an
/// ambiguous delivery. The caller's one call keeps the write, follows the
/// record to the member that took over and commits it there. The write
/// applied once: the same operation id is answered from the commit.
#[test]
fn native_one_operation_id_across_a_takeover_commits_once() {
    let (runtime, config) = switched_with_kv_voters("native-follow-takeover", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/follow");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    // Nothing the cut-off primary prepared reaches a member: the successor
    // holds no trace of the write and commits the caller's retry afresh.
    for member in [&b, &c] {
        fail_journal_calls(&runtime, &config, member);
    }
    let broken = break_write_calls(&runtime, &c, &p);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let path = "/services/follow/takeover.txt";
    let write = write_from(
        &runtime,
        &config,
        Some((2, 3)),
        &c,
        path,
        "follow-takeover",
        None,
    );
    await_write_at_the_primary(&runtime, &c, &p);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    release(&broken);
    lose(&runtime, &[&b, &c], &p);
    let answered = committed_across_takeover(
        write.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
            panic!(
                "the caller is answered once a member has taken over; record {:?}\ncalls {:?}\n{}",
                record_from(&runtime, &config, &b).shard,
                calls_from(&runtime, &c),
                event_log(&runtime, &[&b, &c])
            )
        }),
        || event_log(&runtime, &[&b, &c]),
    );
    runtime.clear_faults();
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    assert_ne!(winner, p, "a member took over");
    assert!(
        winner == c || sends_to(&runtime, &c, &winner, "follow-takeover") >= 1,
        "the same operation id went to the new primary"
    );
    heal(&runtime, &config, &p, [&b, &c]);
    // The same call again is answered from the commit.
    let again = write_from(
        &runtime,
        &config,
        Some((2, 3)),
        &c,
        path,
        "follow-takeover",
        None,
    )
    .recv_timeout(EVENT_BOUND)
    .expect("a retry of a committed operation is answered")
    .expect("a retry of a committed operation succeeds");
    if let Some(answered) = answered {
        assert_eq!(answered.version, again.version);
    }
    assert_written_once(&runtime, &winner, &c, path, "follow-takeover", &again);
    runtime.close().unwrap();
}

/// Live old primary partitioned: a caller on the quorum side cannot deliver
/// its write. It asks the network to verify the primary and keeps waiting;
/// once a member has taken over, the same call is answered by it.
#[test]
fn native_caller_follows_the_record_past_a_partitioned_primary() {
    let (runtime, config) = switched_with_kv_voters("native-follow-partitioned", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/cut");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    let path = "/services/cut/partitioned.txt";
    let write = write_from(
        &runtime,
        &config,
        Some((2, 3)),
        &b,
        path,
        "follow-partitioned",
        None,
    );
    // The network still reports a path: nobody takes over, and the caller
    // neither gives up nor is answered.
    let started = Instant::now();
    while !runtime.peer_verification_requests(&b).contains(&p) {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "a write that cannot be delivered asks the network to verify the primary"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        write.recv_timeout(STILL_WAITING).is_err(),
        "the caller waits for the record while the primary is only unreachable"
    );
    assert_eq!(record_from(&runtime, &config, &b).shard.primary_node_id, p);
    lose(&runtime, &[&b, &c], &p);
    let answered = committed_across_takeover(
        write
            .recv_timeout(EVENT_BOUND * 2)
            .expect("the caller is answered once a member has taken over"),
        || event_log(&runtime, &[&b, &c]),
    );
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    assert_ne!(winner, p);
    assert!(
        winner == b || sends_to(&runtime, &b, &winner, "follow-partitioned") >= 1,
        "the same operation id went to the new primary"
    );
    heal(&runtime, &config, &p, [&b, &c]);
    let again = write_from(
        &runtime,
        &config,
        Some((2, 3)),
        &b,
        path,
        "follow-partitioned",
        None,
    )
    .recv_timeout(EVENT_BOUND)
    .expect("a retry of a committed operation is answered")
    .expect("a retry of a committed operation succeeds");
    if let Some(answered) = answered {
        assert_eq!(answered.version, again.version);
    }
    assert_written_once(&runtime, &winner, &b, path, "follow-partitioned", &again);
    runtime.close().unwrap();
}

/// Slow but reachable primary: its replies to the caller are held past the
/// network's liveness interval. The caller, which set no budget, does not
/// give up: its one call is answered, the write applied once, and nobody
/// started a takeover.
#[test]
fn native_caller_without_a_budget_waits_for_a_slow_primary() {
    let (runtime, config) = switched_with_kv_voters("native-follow-slow", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/slow");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&p, &b, &c]);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for route in [
        crate::DFS_DATA_NAMESPACE_ROUTE,
        crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    ] {
        runtime.install_fault(FaultRule {
            source: Some(c.clone()),
            target: Some(p.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
        });
    }
    let path = "/services/slow/held.txt";
    let write = write_from(&runtime, &config, None, &c, path, "follow-slow", None);
    assert!(
        write.recv_timeout(STILL_WAITING).is_err(),
        "the caller waits for a primary that is only slow"
    );
    release(&gate);
    let written = write
        .recv_timeout(EVENT_BOUND)
        .expect("the caller is answered once the primary's replies arrive")
        .expect("a slow primary's write commits");
    runtime.clear_faults();
    for node in [&b, &c] {
        assert!(
            runtime
                .write_set_takeover_events(node)
                .iter()
                .all(|(_, event)| event.shard_id != config.shard_id),
            "{node} started nothing:\n{}",
            event_log(&runtime, &[&b, &c])
        );
    }
    let record = record_from(&runtime, &config, &p);
    assert_eq!(
        (record.shard.primary_node_id, record.shard.epoch),
        (p.clone(), config.epoch)
    );
    assert_written_once(&runtime, &p, &c, path, "follow-slow", &written);
    runtime.close().unwrap();
}

/// An unknown outcome with no record change: a required member cannot be
/// reached and nobody changes the write set. The caller's one call is sent
/// again to the same primary on the re-send timer and is answered once the
/// member returns; the write applied once.
#[test]
fn native_unknown_outcome_without_a_record_change_is_sent_again_until_the_member_returns() {
    let (runtime, config) = switched("native-follow-unknown", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/unknown");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    runtime.partition_node(
        &c,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    let path = "/services/unknown/resent.txt";
    let write = write_from(&runtime, &config, None, &b, path, "follow-unknown", None);
    assert!(
        write.recv_timeout(STILL_WAITING).is_err(),
        "an unknown outcome is not the caller's answer while it has no budget"
    );
    let sent = sends_to(&runtime, &b, &p, "follow-unknown");
    assert!(sent >= 2, "the re-send timer sent the write again ({sent})");
    assert!(
        sent <= 8,
        "the timer backs off to the liveness interval ({sent} sends)"
    );
    let record = record_from(&runtime, &config, &b);
    assert_eq!(
        (record.shard.primary_node_id.clone(), record.shard.epoch),
        (p.clone(), config.epoch),
        "the record did not change"
    );
    runtime.partition_node(&c, Partition::default());
    let written = write
        .recv_timeout(EVENT_BOUND)
        .expect("the caller is answered once the member returns")
        .expect("the write commits once the member returns");
    assert_written_once(&runtime, &p, &b, path, "follow-unknown", &written);
    runtime.close().unwrap();
}

/// The caller's budget is the only bound on its wait: a call whose primary
/// stays unreachable ends at its budget, not before, with the delivery
/// failure it holds.
#[test]
fn native_callers_budget_ends_its_wait_for_the_record() {
    let (runtime, config) = switched("native-follow-budget", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/budget");
    }
    catch_up(&runtime, &config, &b);
    runtime.partition_node(
        &p,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    let budget = Duration::from_secs(3);
    let started = Instant::now();
    let outcome = write_from(
        &runtime,
        &config,
        None,
        &b,
        "/services/budget/ended.txt",
        "follow-budget",
        Some(budget),
    )
    .recv_timeout(EVENT_BOUND)
    .expect("a budgeted call ends at its budget");
    assert!(
        started.elapsed() >= budget,
        "the call waited for its whole budget ({:?})",
        started.elapsed()
    );
    assert!(
        started.elapsed() < budget + Duration::from_secs(10),
        "the budget ended the wait ({:?})",
        started.elapsed()
    );
    assert!(
        matches!(
            outcome,
            Err(crate::DfsPosixError::Dfs(
                crate::DfsError::Network { .. } | crate::DfsError::NetworkDelivery { .. }
            ))
        ),
        "the call ends with the delivery failure it holds: {outcome:?}"
    );
    runtime.partition_node(&p, Partition::default());
    runtime.close().unwrap();
}

/// A mounted client of `node`, as a FUSE mount or an NFS export opens one:
/// its configuration comes from the node's announcements and its requests
/// leave through the node's Main.
fn mounted(runtime: &DfsTestRuntime, node: &NodeId) -> crate::MountedSifsService {
    let (sync, asynchronous) = runtime.peer_transports(node).unwrap();
    crate::open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
        runtime.data_dir(node).clone(),
        sync,
        asynchronous,
        None,
    )
    .expect("the mounted client opens")
    .service
}

/// A mount survives a takeover in the middle of a write: the mounted
/// client's one call, under the one id the engine minted for it, follows the
/// record to the member that took over and succeeds. The same client then
/// serves the next call.
#[test]
fn native_mounted_client_write_survives_a_takeover() {
    let (runtime, config) = switched_with_kv_voters("native-follow-mounted", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/mounted");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    // Nothing the cut-off primary prepared reaches a member: the successor
    // holds no trace of the write and commits the caller's retry afresh.
    for member in [&b, &c] {
        fail_journal_calls(&runtime, &config, member);
    }
    let broken = break_write_calls(&runtime, &c, &p);
    let mut mount = mounted(&runtime, &c);
    let options = write_options(&config, Some((2, 3)));
    runtime.checkpoint_rpc_observation_window().unwrap();
    let (sender, receiver) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let written = mount.posix_write_file("/services/mounted/kept.txt", b"kept", options);
        let _ = sender.send(written);
        mount
    });
    // The primary is serving the write when it is cut off.
    await_write_at_the_primary(&runtime, &c, &p);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    release(&broken);
    lose(&runtime, &[&b, &c], &p);
    committed_across_takeover(
        receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
            panic!(
                "the syscall returns once a member has taken over; calls {:?}\n{}",
                calls_from(&runtime, &c),
                event_log(&runtime, &[&b, &c])
            )
        }),
        || event_log(&runtime, &[&b, &c]),
    );
    runtime.clear_faults();
    let mut mount = writer.join().unwrap();
    let winner = record_from(&runtime, &config, &b).shard.primary_node_id;
    assert_ne!(winner, p);
    heal(&runtime, &config, &p, [&b, &c]);
    assert_eq!(
        runtime
            .client(&winner)
            .unwrap()
            .posix_read_file_distributed("/services/mounted/kept.txt")
            .unwrap(),
        b"kept"
    );
    // The mount keeps serving.
    mount
        .posix_mkdir("/services/mounted/after")
        .expect("the mount serves the next call");
    drop(mount);
    runtime.close().unwrap();
}

/// The mount's operation budget ends a syscall: with the primary unreachable
/// and nobody taking over, a call under a budget returns at the budget, and
/// the same mounted client serves the next call once the primary is back.
#[test]
fn native_mounted_client_budget_ends_a_syscall_and_the_mount_keeps_serving() {
    let (runtime, config) = switched("native-follow-mounted-budget", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/budgeted");
    }
    catch_up(&runtime, &config, &b);
    let mut mount = mounted(&runtime, &b);
    runtime.partition_node(
        &p,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    let budget = Duration::from_secs(3);
    let started = Instant::now();
    let previous = mount.set_transport_operation_deadline(Some(started + budget));
    let refused = mount.posix_mkdir("/services/budgeted/ended");
    mount.set_transport_operation_deadline(previous);
    assert!(
        refused.is_err(),
        "nothing answers while the primary is cut off"
    );
    assert!(
        started.elapsed() >= budget && started.elapsed() < budget + Duration::from_secs(10),
        "the budget, and nothing sooner, ended the syscall: {:?} {refused:?}",
        started.elapsed()
    );
    runtime.partition_node(&p, Partition::default());
    let served = Instant::now();
    loop {
        match mount.posix_mkdir("/services/budgeted/after") {
            Ok(_) => break,
            Err(crate::DfsPosixError::AlreadyExists { .. }) => break,
            Err(error) => {
                assert!(
                    served.elapsed() < EVENT_BOUND,
                    "the mount serves the next call: {error}"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
    drop(mount);
    runtime.close().unwrap();
}

/// A mounted process cannot post a hint to its node's keeper, which runs in
/// Main. Its undelivered request sends the node a primary-verification
/// request instead; the keeper asks the network to verify the primary, the
/// settled verdict starts the takeover, and the mounted call follows the
/// record to the new primary.
#[test]
fn native_mounted_client_request_asks_its_node_to_verify_the_primary() {
    let (runtime, config) = switched_with_kv_voters("native-follow-mounted-hint", Some((2, 3)));
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/hinted");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    runtime.checkpoint_rpc_observation_window().unwrap();
    let mut mount = mounted(&runtime, &b);
    let options = write_options(&config, Some((2, 3)));
    let (sender, receiver) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let written = mount.posix_write_file("/services/hinted/kept.txt", b"kept", options);
        let _ = sender.send(written);
    });
    let started = Instant::now();
    while !runtime.peer_verification_requests(&b).contains(&p) {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "the mounted client's request made its node ask for verification; calls {:?}",
            calls_from(&runtime, &b)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        runtime.rpc_observations().iter().any(|call| {
            call.source == b && call.target == b && call.route == crate::DFS_DATA_READ_ROUTE
        }),
        "the hint travelled as a DFS message to the client's own node"
    );
    // The verdict settles: the takeover starts, and the call follows it.
    lose(&runtime, &[&b, &c], &p);
    committed_across_takeover(
        receiver.recv_timeout(EVENT_BOUND * 2).unwrap_or_else(|_| {
            panic!(
                "the syscall returns once a member has taken over; calls {:?}\n{}",
                calls_from(&runtime, &b),
                event_log(&runtime, &[&b, &c])
            )
        }),
        || event_log(&runtime, &[&b, &c]),
    );
    writer.join().unwrap();
    assert_ne!(record_from(&runtime, &config, &b).shard.primary_node_id, p);
    heal(&runtime, &config, &p, [&b, &c]);
    runtime.close().unwrap();
}
