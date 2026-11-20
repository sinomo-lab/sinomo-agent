//! A served commit's wait ends with its requester, on the native test runtime:
//! the requester's budget and its departure reach the primary serving its
//! request, end that request's wait for a write-set member and release its
//! admission. The change keeps the outcome it has; a retry with the same
//! operation id resolves it.

use super::commit_native_tests::{hold_replies, release, switched};
use super::*;
use crate::test_support::DfsTestRuntime;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Far above anything that ends a served wait on its own while every member
/// stays reachable, and the bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(30);

/// How soon after its requester has left a served wait must have ended. The
/// native runtime carries a departure at once.
const PROMPT: Duration = Duration::from_secs(5);

/// One copy, on the primary: the write leaves no replica to stage or repair,
/// so the only thing its commit waits for is its write set's acknowledgement.
fn write_options(config: &DfsMetadataShardConfig) -> crate::DfsWriteOptions {
    crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![config.primary_node_id.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

/// What bounds one requester's write: its own budget, a budget only its
/// request carries, or its cancellation.
#[derive(Clone, Default)]
struct Requester {
    budget: Option<Duration>,
    stamped_budget: Option<Duration>,
    cancellation: Option<crate::DfsCancellationFlag>,
}

/// One content write to `path` from `from`, under `operation`, on its own
/// thread.
fn write_from(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    from: &NodeId,
    path: &'static str,
    operation: &'static str,
    requester: Requester,
) -> mpsc::Receiver<Result<(), crate::DfsPosixError>> {
    let data_dir = runtime.data_dir(from).clone();
    let options = write_options(config);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client = crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        )
        .expect("the requester opens its client");
        // These cases are about the wait of the primary serving the request:
        // the requester reports what its one attempt returned.
        client.single_attempt_for_test(true);
        if let Some(cancellation) = requester.cancellation {
            client.set_cancellation(Some(cancellation));
        }
        if let Some(budget) = requester.budget {
            client.set_transport_operation_deadline(Some(Instant::now() + budget));
        }
        crate::node_transport::stamp_request_budget_for_test(requester.stamped_budget);
        let written = client
            .posix_write_file_with_operation_id(path, operation.as_bytes(), options, operation)
            .map(drop);
        crate::node_transport::stamp_request_budget_for_test(None);
        let _ = sender.send(written);
    });
    receiver
}

fn write_settled(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    from: &NodeId,
    path: &'static str,
    operation: &'static str,
) {
    let started = Instant::now();
    loop {
        let outcome = write_from(runtime, config, from, path, operation, Requester::default())
            .recv_timeout(EVENT_BOUND * 3)
            .expect("an unbounded write ends once its members answer");
        match outcome {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    started.elapsed() < EVENT_BOUND * 3,
                    "{path} was not written under {operation}: {error}"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn lane_held(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) -> bool {
    runtime
        .main_lane_held(
            &config.primary_node_id,
            &config.file_system_id,
            &config.shard_id,
        )
        .unwrap()
}

/// Wait for the primary to hold (or to have released) its shard lane.
fn await_lane(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, held: bool, what: &str) {
    let started = Instant::now();
    while lane_held(runtime, config) != held {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "{what}; calls still in flight: {:?}",
            calls_in_flight(runtime)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The calls of the current observation window that have not ended, as
/// (source, target, route).
fn calls_in_flight(runtime: &DfsTestRuntime) -> Vec<(NodeId, NodeId, String)> {
    let mut latest = std::collections::BTreeMap::new();
    for call in runtime.rpc_observations() {
        latest.insert(call.call_id, call);
    }
    latest
        .into_values()
        .filter(|call| {
            !matches!(
                call.phase,
                crate::test_runtime::RpcPhase::Completed
                    | crate::test_runtime::RpcPhase::EndedWithoutResponse
            )
        })
        .map(|call| (call.source, call.target, call.route))
        .collect()
}

/// Whether a prepare from the primary has reached `member`, whose answer the
/// test holds.
fn prepare_reached(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
) -> bool {
    runtime.rpc_observations().iter().any(|call| {
        call.source == config.primary_node_id
            && call.target == *member
            && call.phase == crate::test_runtime::RpcPhase::AfterDispatch
            && [
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            ]
            .contains(&call.route.as_str())
    })
}

/// The primary's executions of one requester's mutations, observed where
/// they happen.
struct Served {
    /// Each execution, and each release of the admission an execution held,
    /// in order.
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
    executing: mpsc::Receiver<()>,
    proceed: mpsc::Sender<()>,
    _hook: crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
}

impl Served {
    /// Observe `requester`'s content writes and mkdirs as the primary
    /// executes them. The first execution pauses, holding its admission,
    /// until [`Self::hold_member`] has run.
    fn observe(
        runtime: &DfsTestRuntime,
        config: &DfsMetadataShardConfig,
        requester: &NodeId,
    ) -> Self {
        use crate::runtime_host::DaemonDfsPreDispatchTestPoint as Point;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let requester = requester.clone();
        let (executing_sender, executing) = mpsc::channel();
        let (proceed, proceed_receiver) = mpsc::channel::<()>();
        let handshake = std::sync::Mutex::new(Some((executing_sender, proceed_receiver)));
        let hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            runtime.data_dir(&config.primary_node_id),
            Arc::new(move |point, command, _| {
                let mutation = command.is_some_and(|command| {
                    command.routed_message.source_node_id == requester
                        && match &command.routed_message.message {
                            crate::DfsNetworkMessage::NamespaceRequest(request) => matches!(
                                request.operation,
                                crate::DfsNamespaceOperation::WriteFile { .. }
                                    | crate::DfsNamespaceOperation::Mkdir { .. }
                                    | crate::DfsNamespaceOperation::MkdirWithAttrs { .. }
                            ),
                            crate::DfsNetworkMessage::DistributedWriteRequest(_)
                            | crate::DfsNetworkMessage::ExclusiveCreateRequest(_) => true,
                            _ => false,
                        }
                });
                if !mutation {
                    return Ok(());
                }
                match point {
                    Point::BeforeExecute { .. } => {
                        observed.lock().unwrap().push("execute");
                        if let Some((executing, proceed)) = handshake.lock().unwrap().take() {
                            let _ = executing.send(());
                            let _ = proceed.recv_timeout(EVENT_BOUND);
                        }
                    }
                    Point::ReplicaAdmissionReleased { released: true } => {
                        observed.lock().unwrap().push("released")
                    }
                    _ => {}
                }
                Ok(())
            }),
        );
        Self {
            events,
            executing,
            proceed,
            _hook: hook,
        }
    }

    /// Once the primary executes the requester's mutation under its lane,
    /// start holding `member`'s answers, then let the execution go on. No
    /// other work holds the lane then, so the wait the test observes is this
    /// request's wait for the held member.
    fn hold_member(
        &self,
        runtime: &DfsTestRuntime,
        config: &DfsMetadataShardConfig,
        member: &NodeId,
    ) -> Arc<tokio::sync::Semaphore> {
        self.executing
            .recv_timeout(EVENT_BOUND)
            .expect("the primary executes the requester's mutation");
        runtime.checkpoint_rpc_observation_window().unwrap();
        let gate = hold_replies(runtime, config, member);
        let _ = self.proceed.send(());
        let started = Instant::now();
        while !(lane_held(runtime, config) && prepare_reached(runtime, config, member)) {
            assert!(
                started.elapsed() < EVENT_BOUND,
                "the primary waits for the held member under its lane"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        gate
    }

    /// Wait until the observed events are exactly `expected`.
    fn events_are(
        &self,
        runtime: &DfsTestRuntime,
        config: &DfsMetadataShardConfig,
        expected: &[&str],
        what: &str,
    ) {
        let started = Instant::now();
        loop {
            let now = self.events.lock().unwrap().clone();
            if now.len() >= expected.len() {
                assert_eq!(now, expected, "{what}");
                return;
            }
            assert!(
                started.elapsed() < EVENT_BOUND,
                "{what}: saw {now:?}; calls still in flight: {:?}; lane held: {}",
                calls_in_flight(runtime),
                lane_held(runtime, config)
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// A three-member write-set shard whose primary has served one write, the
/// member whose answers a test holds back, and the requester.
fn serving(name: &str) -> (DfsTestRuntime, DfsMetadataShardConfig, NodeId, NodeId) {
    let (runtime, config) = switched(name, None);
    let held_member = config.replica_node_ids[0].clone();
    let requester = config.replica_node_ids[1].clone();
    write_settled(
        &runtime,
        &config,
        &requester,
        "/services/budget-before.txt",
        "budget-before",
    );
    (runtime, config, held_member, requester)
}

/// The requester hangs up in the middle of its commit: its blocking send
/// drops the call, the primary's handler is dropped with it, and the served
/// wait for the held member ends and releases the shard lane while that
/// member is still held. A retry with the same operation id then resolves the
/// change.
#[test]
fn native_served_commit_ends_when_its_requester_hangs_up() {
    let (runtime, config, held_member, requester) = serving("native-budget-hang-up");
    let served = Served::observe(&runtime, &config, &requester);
    let cancellation = crate::DfsCancellationFlag::new();
    let write = write_from(
        &runtime,
        &config,
        &requester,
        "/services/hang-up.txt",
        "hang-up",
        Requester {
            cancellation: Some(cancellation.clone()),
            ..Default::default()
        },
    );
    let gate = served.hold_member(&runtime, &config, &held_member);
    // Nothing ends the served wait on its own while the member is held.
    assert!(
        write.recv_timeout(Duration::from_secs(2)).is_err(),
        "the write waits for its held member"
    );
    assert!(lane_held(&runtime, &config));

    let left = Instant::now();
    cancellation.cancel();
    let outcome = write
        .recv_timeout(PROMPT)
        .expect("the requester's own wait ends when it leaves");
    assert!(
        outcome.is_err(),
        "a write its requester abandoned reports no success"
    );
    await_lane(
        &runtime,
        &config,
        false,
        "the served wait ends and releases the lane when its requester leaves",
    );
    assert!(
        left.elapsed() < PROMPT,
        "the served wait ended {:?} after its requester left",
        left.elapsed()
    );

    // The member answers again: the same operation resolves, and the lane
    // serves a later request.
    runtime.clear_faults();
    release(&gate);
    write_settled(
        &runtime,
        &config,
        &requester,
        "/services/hang-up.txt",
        "hang-up",
    );
    write_settled(
        &runtime,
        &config,
        &requester,
        "/services/hang-up-after.txt",
        "hang-up-after",
    );
    runtime.close().unwrap();
}

/// The requester's budget ends in the middle of its commit. The primary
/// measures the budget its request carried from arrival and answers at it
/// with the typed outcome "the caller's deadline ended the wait", leaving the
/// change prepared; the lane is released while the member is still held. A
/// requester whose own call ends at the budget gets no success either. The
/// same operation then resolves.
#[test]
fn native_served_commit_ends_at_its_requesters_budget_with_a_typed_outcome() {
    let (runtime, config, held_member, requester) = serving("native-budget-expires");
    let served = Served::observe(&runtime, &config, &requester);
    let budget = Duration::from_secs(3);

    // The request carries the budget while the requester's own call stays
    // open, so the answer the primary gives at the budget is read here.
    let started = Instant::now();
    let write = write_from(
        &runtime,
        &config,
        &requester,
        "/services/budget.txt",
        "budget",
        Requester {
            stamped_budget: Some(budget),
            ..Default::default()
        },
    );
    let gate = served.hold_member(&runtime, &config, &held_member);
    let outcome = write
        .recv_timeout(budget + PROMPT)
        .expect("the served wait ends at the budget its request carried");
    assert!(
        started.elapsed() >= budget - Duration::from_millis(200),
        "nothing ended the wait before the budget: {:?}",
        started.elapsed()
    );
    match outcome {
        Err(crate::DfsPosixError::Dfs(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause,
            ..
        })) => assert_eq!(cause, crate::DfsCommitOutcomeUnknownCause::Deadline),
        other => panic!("expected the typed budget outcome, got {other:?}"),
    }
    await_lane(
        &runtime,
        &config,
        false,
        "the budget releases the lane while the member is still held",
    );

    runtime.clear_faults();
    release(&gate);
    write_settled(
        &runtime,
        &config,
        &requester,
        "/services/budget.txt",
        "budget",
    );
    runtime.close().unwrap();
}

/// A requester whose own call ends at its budget, as every budgeted caller's
/// does: the primary learns from the budget its request carried and from the
/// dropped call, ends the served wait and releases the lane while the member
/// is still held. The same operation then resolves.
#[test]
fn native_served_commit_ends_when_its_requesters_own_budget_does() {
    let (runtime, config, held_member, requester) = serving("native-budget-own");
    let served = Served::observe(&runtime, &config, &requester);
    let budget = Duration::from_secs(3);
    let write = write_from(
        &runtime,
        &config,
        &requester,
        "/services/own-budget.txt",
        "own-budget",
        Requester {
            budget: Some(budget),
            ..Default::default()
        },
    );
    let gate = served.hold_member(&runtime, &config, &held_member);
    let outcome = write
        .recv_timeout(budget + PROMPT)
        .expect("the requester's wait ends at its own budget");
    assert!(
        outcome.is_err(),
        "a write past its budget reports no success"
    );
    await_lane(
        &runtime,
        &config,
        false,
        "the requester's budget releases the lane",
    );
    runtime.clear_faults();
    release(&gate);
    write_settled(
        &runtime,
        &config,
        &requester,
        "/services/own-budget.txt",
        "own-budget",
    );
    runtime.close().unwrap();
}

/// Longer than every default deadline DFS used to add to a request: 45 s for
/// a POSIX commit, 50 s for a sync and 60 s for a routed namespace request.
const SLOWER_THAN_EVERY_REMOVED_DEFAULT: Duration = Duration::from_secs(65);

/// A requester without a budget and a member that is slow but reachable: the
/// commit waits for the member for longer than any deadline DFS used to add,
/// nothing times out, and it commits when the member answers.
#[test]
fn native_commit_without_a_budget_waits_for_a_slow_member() {
    let (runtime, config, held_member, requester) = serving("native-budget-none");
    let served = Served::observe(&runtime, &config, &requester);
    let started = Instant::now();
    let write = write_from(
        &runtime,
        &config,
        &requester,
        "/services/no-budget.txt",
        "no-budget",
        Requester::default(),
    );
    let gate = served.hold_member(&runtime, &config, &held_member);
    assert!(
        write
            .recv_timeout(SLOWER_THAN_EVERY_REMOVED_DEFAULT)
            .is_err(),
        "nothing ends a commit that waits for a slow, reachable member"
    );
    assert!(lane_held(&runtime, &config), "the primary still serves it");
    served.events_are(
        &runtime,
        &config,
        &["execute"],
        "the one attempt is still the only one",
    );
    runtime.clear_faults();
    release(&gate);
    let outcome = write
        .recv_timeout(EVENT_BOUND)
        .expect("the write ends when its member answers");
    assert!(
        outcome.is_ok(),
        "the slow member's answer commits the write: {outcome:?}"
    );
    assert!(started.elapsed() >= SLOWER_THAN_EVERY_REMOVED_DEFAULT);
    runtime.close().unwrap();
}

/// A response-retry-safe request whose stream breaks in the middle of its
/// commit is replayed inside the requester's budget. The first attempt's
/// handler is dropped with its stream, so its served wait ends and its
/// admission is released while its member is still held, and only then does
/// the replay run: the two attempts never hold the lane together. The replay
/// finds the first attempt's change by its request id.
#[test]
fn native_exact_replay_never_holds_admission_beside_its_first_attempt() {
    let (runtime, config, held_member, requester) = serving("native-budget-replay");
    let served = Served::observe(&runtime, &config, &requester);
    let primary = config.primary_node_id.clone();
    // Break the stream of the requester's first call to the primary once the
    // primary is serving it.
    let broken = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(requester.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BreakStreamDuringDispatch(Arc::clone(&broken)),
    });
    let data_dir = runtime.data_dir(&requester).clone();
    let (sender, mkdir) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client = crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        )
        .expect("the requester opens its client");
        client.set_transport_operation_deadline(Some(Instant::now() + EVENT_BOUND * 3));
        let _ = sender.send(client.posix_mkdir("/services/replay-dir").map(drop));
    });
    let gate = served.hold_member(&runtime, &config, &held_member);
    served.events_are(
        &runtime,
        &config,
        &["execute"],
        "the primary executes the first attempt",
    );
    broken.add_permits(1);
    // With the member still held: the first attempt ends and releases its
    // admission, and only then does the replay execute.
    served.events_are(
        &runtime,
        &config,
        &["execute", "released", "execute"],
        "the first attempt releases its admission before the replay runs",
    );
    runtime.clear_faults();
    release(&gate);
    let outcome = mkdir
        .recv_timeout(EVENT_BOUND * 3)
        .expect("the replayed request ends once its member answers");
    assert!(
        outcome.is_ok(),
        "the replay resolves the first attempt's change: {outcome:?}"
    );
    runtime.close().unwrap();
}
