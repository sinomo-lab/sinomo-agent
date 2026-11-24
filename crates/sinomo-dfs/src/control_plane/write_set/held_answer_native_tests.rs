//! The answer to a held routed write, on the native test runtime: the primary
//! answered the write `Pending` because it could not stage a second copy, and
//! answers again when the write commits. That later answer reaches the
//! requester's Main, which keeps it for a live call whose owner is not
//! attached, and announces it when no owner is registered at all; a requester
//! that may have lost it sends the write again under its id.

use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcPhase};
use crate::test_support::DfsTestRuntime;
use sinomo_core::network::PeerPathVerdict;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

/// Long enough for a call that would answer early to have answered.
const STILL_WAITING: Duration = Duration::from_secs(5);

const STAGE_ROUTES: [&str; 3] = [
    crate::DFS_DATA_STAGE_BLOB_ROUTE,
    crate::DFS_DATA_STAGE_PATCH_ROUTE,
    crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
];

/// A three-node shard holding `/services`, with KV voters on every node so
/// that one node may stop. Returns its primary and a requester node.
fn fixture(name: &str) -> (DfsTestRuntime, DfsMetadataShardConfig, NodeId, NodeId) {
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service_with_kv_voters(name, 3, "/services")
            .unwrap();
    drop(service);
    let p = config.primary_node_id.clone();
    let c = config.replica_node_ids[1].clone();
    (runtime, config, p, c)
}

/// W of 2 over the shard's three members, waiting for the commit.
fn options(config: &DfsMetadataShardConfig) -> crate::DfsWriteOptions {
    crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: std::iter::once(config.primary_node_id.clone())
            .chain(config.replica_node_ids.iter().cloned())
            .collect(),
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

/// The primary cannot stage a second copy, so it answers a write `Pending`.
fn hold_writes_at_the_primary(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) {
    for target in &config.replica_node_ids {
        for route in STAGE_ROUTES {
            runtime.install_fault(FaultRule {
                source: Some(config.primary_node_id.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
}

/// The primary may stage again; its maintenance commits the held write.
/// Runs the primary's maintenance until `done`.
fn commit_at_the_primary(runtime: &DfsTestRuntime, p: &NodeId, mut done: impl FnMut() -> bool) {
    runtime.remove_faults_matching(|rule| {
        rule.route
            .as_deref()
            .is_some_and(|route| STAGE_ROUTES.contains(&route))
    });
    let _ = runtime.request_repair_reconciliation(p);
    let deadline = Instant::now() + EVENT_BOUND;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "the primary commits the held write"
        );
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
        let _ = runtime.maintenance_tick(p, remaining);
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The calls `from` made to `target` under `operation`.
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

/// Wait until the primary has answered one of `from`'s calls under
/// `operation`.
fn await_the_primary_answer(runtime: &DfsTestRuntime, from: &NodeId, p: &NodeId, operation: &str) {
    let started = Instant::now();
    while !{
        let calls = runtime.rpc_observations();
        calls.iter().any(|call| {
            call.source == *from
                && call.target == *p
                && call.dfs_request_id.as_deref() == Some(operation)
                && calls.iter().any(|answer| {
                    answer.call_id == call.call_id && answer.phase == RpcPhase::Completed
                })
        })
    } {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "the primary answers {operation}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A mounted client of `node` over `peer`, with a cancellation a failing
/// test uses to end its call.
fn mounted_over(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    peer: (
        Arc<dyn crate::DfsPeerTransport>,
        Arc<dyn crate::AsyncDfsPeerTransport>,
    ),
    cancel: &crate::DfsCancellationFlag,
) -> crate::MountedSifsLock<crate::MountedSifsService> {
    crate::MountedSifsLock::new(
        crate::open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
            runtime.data_dir(node).clone(),
            peer.0,
            peer.1,
            Some(cancel.clone()),
        )
        .expect("the mounted client opens")
        .service,
    )
}

/// Run one mounted write call on its own thread until it no longer hands a
/// wait back. Counts its runs, and sends its result on `answer`.
fn spawn_mounted_write<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    service: &'scope crate::MountedSifsLock<crate::MountedSifsService>,
    runs: &'scope AtomicUsize,
    path: &'static str,
    operation: &'static str,
    options: crate::DfsWriteOptions,
    answer: mpsc::Sender<Result<crate::DfsStat, crate::DfsPosixError>>,
) {
    scope.spawn(move || {
        let mut replay = crate::MountedSifsCallReplay::new();
        loop {
            runs.fetch_add(1, Ordering::AcqRel);
            let result = replay.run(&mut *service.lock().unwrap(), |service| {
                service.posix_write_file_with_operation_id(
                    path,
                    operation.as_bytes(),
                    options.clone(),
                    operation,
                )
            });
            match replay.take_record_wait() {
                Some(wait) => wait.wait(),
                None => {
                    let _ = answer.send(result);
                    return;
                }
            }
        }
    });
}

/// A peer transport that reaches Main as a requester outside Main does: it
/// has no response owner there, so an answer that comes later than its call's
/// own reply reaches Main unclaimed.
struct WithoutResponseOwner(
    Arc<dyn crate::DfsPeerTransport>,
    Arc<dyn crate::AsyncDfsPeerTransport>,
);

impl crate::DfsPeerTransport for WithoutResponseOwner {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: sinomo_core::network::ObjectRpcCall,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<sinomo_core::network::ObjectRpcResult, crate::DfsPeerTransportError> {
        self.0.call_object_rpc(target_node_id, call, options)
    }

    fn call_metadata_prefix_probe(
        &self,
        attempt: &crate::DfsMetadataPrefixProbeAttempt,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<crate::DfsMetadataPrefixProbeReturn, crate::DfsPeerTransportError> {
        self.0.call_metadata_prefix_probe(attempt, options)
    }
}

impl crate::AsyncDfsPeerTransport for WithoutResponseOwner {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: sinomo_core::network::ObjectRpcCall,
        options: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<
        'a,
        Result<sinomo_core::network::ObjectRpcResult, crate::DfsPeerTransportError>,
    > {
        self.1.call_object_rpc_async(target_node_id, call, options)
    }
}

/// The node's Main keeps the commit answer of a mounted call's held write
/// while the call's response owner is not attached, and announces it. The
/// announcement runs the call again, which registers its owner again and
/// takes the kept answer: the call is answered without sending the write
/// again. The kept answer leaves Main with it.
#[test]
fn native_answer_kept_for_an_unattached_owner_reaches_the_call_when_it_runs_again() {
    let (runtime, config, p, c) = fixture("native-held-answer-kept");
    hold_writes_at_the_primary(&runtime, &config);
    // The requester reaches the primary throughout.
    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::Path, false);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let operation = "held-answer-kept";
    let path = "/services/held-kept.txt";
    let cancel = crate::DfsCancellationFlag::new();
    let service = mounted_over(&runtime, &c, runtime.peer_transports(&c).unwrap(), &cancel);
    let delivery = Arc::clone(
        &crate::runtime_host::registered_dfs_runtime_host(runtime.data_dir(&c))
            .unwrap()
            .expect("the requester's Main runs")
            .response_delivery,
    );
    let runs = AtomicUsize::new(0);
    let (answer, answered) = mpsc::channel();
    std::thread::scope(|scope| {
        spawn_mounted_write(
            scope,
            &service,
            &runs,
            path,
            operation,
            options(&config),
            answer,
        );
        await_the_primary_answer(&runtime, &c, &p, operation);
        let early = answered.recv_timeout(STILL_WAITING).ok();
        // The call holds the write with its wait handed back. Its owner is
        // no longer attached when the commit answer comes, and the test holds
        // the mount's lock, so no run of the call can register it again
        // before then.
        let runs_held = runs.load(Ordering::Acquire);
        let mut held = service.lock().unwrap();
        held.transport.detach_response_owner_for_test();
        let mut kept = false;
        commit_at_the_primary(&runtime, &p, || {
            kept = delivery.kept_answer_count() > 0;
            kept
        });
        drop(held);
        // Main's announcement of the kept answer runs the call again.
        let outcome = answered.recv_timeout(EVENT_BOUND).ok();
        if outcome.is_none() {
            cancel.cancel();
        }
        assert!(early.is_none(), "the primary holds the write: {early:?}");
        assert!(
            kept,
            "Main kept the answer while the call's owner was not attached"
        );
        let stat = outcome
            .expect("the call is answered once it runs again")
            .expect("the held write commits");
        assert!(
            runs.load(Ordering::Acquire) > runs_held,
            "the call ran again to take the answer"
        );
        assert_eq!(
            stat.version,
            crate::intent::stable_write_target_version(&c, &stat.object_id, operation)
        );
        assert_eq!(
            sends_to(&runtime, &c, &p, operation),
            1,
            "the kept answer settled the call: the write was not sent again"
        );
        assert_eq!(delivery.kept_answer_count(), 0, "the answer left Main");
    });
    drop(service);
    runtime.close().unwrap();
}

/// A held write whose primary restarted with its commit answer still queued
/// for the requester: the answer is lost with the restart. When the
/// requester's verdict for the primary changes back to a path, the call,
/// which is not a mounted call, sends the write again under its id, and the
/// primary answers from the write's receipt: the write commits once.
#[test]
fn native_held_write_is_sent_again_when_its_restarted_primary_is_reachable_again() {
    let (mut runtime, config, p, c) = fixture("native-held-answer-restart");
    hold_writes_at_the_primary(&runtime, &config);
    // The commit answer cannot reach the requester.
    runtime.install_fault(FaultRule {
        source: Some(p.clone()),
        target: Some(c.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::BudgetExhausted,
    });
    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::Path, false);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let operation = "held-answer-restart";
    let path = "/services/held-restart.txt";
    let data_dir = runtime.data_dir(&c).clone();
    let call_options = options(&config);
    let (answer, answered) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client = crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        )
        .expect("the requester opens its client");
        let _ = answer.send(client.posix_write_file_with_operation_id(
            path,
            operation.as_bytes(),
            call_options,
            operation,
        ));
    });
    await_the_primary_answer(&runtime, &c, &p, operation);
    assert!(
        answered.recv_timeout(STILL_WAITING).is_err(),
        "the primary holds the write"
    );
    let answers_sent = || {
        runtime
            .rpc_observations()
            .iter()
            .filter(|call| {
                call.source == p
                    && call.target == c
                    && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
            })
            .count()
    };
    commit_at_the_primary(&runtime, &p, || answers_sent() > 0);
    assert!(
        answered.recv_timeout(STILL_WAITING).is_err(),
        "the commit answer did not reach the requester"
    );
    let sent_before_restart = sends_to(&runtime, &c, &p, operation);
    // The primary restarts, and its queued answer goes with it.
    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::NoPath, false);
    runtime.stop_node(&p).unwrap();
    runtime.reopen_node(p.clone()).unwrap();
    runtime.clear_faults();
    assert!(
        answered.recv_timeout(STILL_WAITING).is_err(),
        "nothing sends the write again while the path is down"
    );
    assert_eq!(sends_to(&runtime, &c, &p, operation), sent_before_restart);
    runtime.set_peer_verdict(&c, &p, PeerPathVerdict::Path, false);
    let stat = answered
        .recv_timeout(EVENT_BOUND)
        .expect("the call is answered once the path is back")
        .expect("the write committed");
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&c, &stat.object_id, operation)
    );
    assert_eq!(
        sends_to(&runtime, &c, &p, operation),
        sent_before_restart + 1,
        "the write was sent again once, under its id"
    );
    runtime.close().unwrap();
}

/// A mount that registers no response owner at Main, as one outside Main:
/// the commit answer of its held write reaches Main unclaimed, Main announces
/// it as `dfs.write.answer_unclaimed`, and the mount sends the write again
/// under its id. The primary answers from the write's receipt inline, and the
/// call completes once.
#[test]
fn native_mount_without_a_response_owner_sends_its_held_write_again_on_the_announcement() {
    let (runtime, config, p, c) = fixture("native-held-answer-unclaimed");
    hold_writes_at_the_primary(&runtime, &config);
    runtime.checkpoint_rpc_observation_window().unwrap();
    let operation = "held-answer-unclaimed";
    let path = "/services/held-unclaimed.txt";
    let (bus, _) = crate::mounted_sifs::mounted_sifs_test_event_bus(runtime.data_dir(&c))
        .expect("the requester's component bus");
    let mut announcements = bus.subscribe_host();
    let (sync, asynchronous) = runtime.peer_transports(&c).unwrap();
    let peer = Arc::new(WithoutResponseOwner(sync, asynchronous));
    let cancel = crate::DfsCancellationFlag::new();
    let service = mounted_over(&runtime, &c, (peer.clone(), peer), &cancel);
    let runs = AtomicUsize::new(0);
    let (answer, answered) = mpsc::channel();
    std::thread::scope(|scope| {
        spawn_mounted_write(
            scope,
            &service,
            &runs,
            path,
            operation,
            options(&config),
            answer,
        );
        await_the_primary_answer(&runtime, &c, &p, operation);
        let early = answered.recv_timeout(STILL_WAITING).ok();
        let sent_while_held = sends_to(&runtime, &c, &p, operation);
        commit_at_the_primary(&runtime, &p, || {
            answered_or_announced(&mut announcements, operation)
        });
        let outcome = answered.recv_timeout(EVENT_BOUND).ok();
        if outcome.is_none() {
            cancel.cancel();
        }
        assert!(early.is_none(), "the primary holds the write: {early:?}");
        assert_eq!(sent_while_held, 1);
        let stat = outcome
            .expect("the announcement sends the held write again")
            .expect("the held write commits");
        assert_eq!(
            stat.version,
            crate::intent::stable_write_target_version(&c, &stat.object_id, operation)
        );
        assert_eq!(
            sends_to(&runtime, &c, &p, operation),
            2,
            "the write was sent again once, under its id"
        );
    });
    drop(service);
    runtime.close().unwrap();
}

/// Whether the requester's Main announced an unclaimed answer to
/// `operation`.
fn answered_or_announced(
    announcements: &mut tokio::sync::broadcast::Receiver<Arc<sinomo_module::ComponentEvent>>,
    operation: &str,
) -> bool {
    loop {
        match announcements.try_recv() {
            Ok(event)
                if event.envelope.kind.as_str() == crate::DFS_WRITE_ANSWER_UNCLAIMED_EVENT_KIND =>
            {
                let answer: crate::DfsWriteAnswerUnclaimed =
                    serde_json::from_value(event.envelope.payload.clone()).unwrap();
                if answer.request_id == operation {
                    return true;
                }
            }
            Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(_) => return false,
        }
    }
}
