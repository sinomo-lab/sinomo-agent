//! Detached promotion observations from actual native receivers.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use crate::{DfsWriteIntentStatus, DfsWriteOptions};

struct NativePromotions {
    runtime: DfsTestRuntime,
    source: NodeId,
    targets: Vec<NodeId>,
    transport: DfsNodeTransport,
    commands: Vec<crate::DfsCommitVersionCommand>,
}

impl NativePromotions {
    fn new(label: &str, nodes: usize, objects: usize) -> Self {
        let roster = DfsTestRuntime::ordered_node_ids(label, nodes);
        let mut runtime = DfsTestRuntime::new(label, roster[0].clone()).unwrap();
        for node in &roster[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let source = shard.primary_node_id.clone();
        let targets = roster
            .iter()
            .filter(|node| **node != source)
            .cloned()
            .collect::<Vec<_>>();
        let mut service = runtime.client(&source).unwrap();
        let mut commands = Vec::new();
        for index in 0..objects {
            let stat = service
                .posix_write_file(
                    &format!("/promotion-{index}"),
                    format!("native promotion body {index}").as_bytes(),
                    DfsWriteOptions::replicated_eventual(roster.clone()),
                )
                .unwrap();
            let backend = service.metadata_backend().unwrap();
            let object = backend.get_object(&stat.object_id).unwrap().unwrap();
            let intent = backend
                .intents_for_object(&stat.object_id)
                .unwrap()
                .into_iter()
                .find(|intent| {
                    intent.target_version == stat.version
                        && intent.status == DfsWriteIntentStatus::Committed
                })
                .unwrap();
            commands.push(crate::DfsCommitVersionCommand {
                request_id: sinomo_core::Id::generate().to_string(),
                intent_id: intent.intent_id,
                file_system_id: intent.file_system_id,
                object_id: intent.object_id,
                expected_base_version: intent.base_version,
                version: intent.target_version,
                content_hash: intent.content_hash.unwrap(),
                object_metadata: Some(object),
                authority_shard: Some(shard.clone()),
            });
        }
        drop(service);
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        for target in &targets {
            let reader = runtime.client(target).unwrap();
            for command in &commands {
                loop {
                    if reader
                        .content
                        .read_object_version(command.object_id.as_str(), command.version.as_str())
                        .is_ok()
                    {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "normal foreground placement must finish before observing an exact promotion replay"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        let (sync, asynchronous) = runtime.peer_transports(&source).unwrap();
        let transport = DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
            source.clone(),
            targets.clone(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sync,
            asynchronous,
            DfsNodeDetachedSendState::new(),
        );
        Self {
            runtime,
            source,
            targets,
            transport,
            commands,
        }
    }
    fn enqueue(&mut self, object: usize, target: usize) {
        let message = crate::routes::build_dfs_routed_message(
            &self.source,
            &self.targets[target],
            object as u64 + 1,
            object as u64 + 1,
            DfsNetworkMessage::CommitVersion(self.commands[object].clone()),
        );
        self.transport
            .send_detached_routed_message(&self.targets[target], message)
            .unwrap();
    }
    fn fault(&self, target: usize, action: FaultAction) {
        self.runtime.install_fault(FaultRule {
            source: Some(self.source.clone()),
            target: Some(self.targets[target].clone()),
            route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action,
        });
    }
    fn finish(self) {
        self.runtime.assert_fault_budgets().unwrap();
        assert_eq!(self.runtime.observation_dropped_count(), 0);
        drop(self.transport);
        self.runtime.close().unwrap();
    }
}

#[test]
fn native_promotion_exact_ack_settles_and_suppresses_duplicate_identity() {
    let mut case = NativePromotions::new("native-promotion-ack", 2, 1);
    case.enqueue(0, 0);
    case.enqueue(0, 0);
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    case.transport.flush_detached_sends().unwrap();
    assert_eq!(case.transport.pending_commit_promotion_count(), 0);
    assert_eq!(case.transport.settled_commit_promotion_count(), 1);
    assert_eq!(case.transport.detached_send_count(), 0);
    case.enqueue(0, 0);
    assert_eq!(case.transport.detached_send_count(), 0);
    case.finish();
}

fn native_failed_promotion_retries_actual_receiver(action: FaultAction, label: &str) {
    let mut case = NativePromotions::new(label, 2, 1);
    case.fault(0, action);
    case.enqueue(0, 0);
    let start = Instant::now();
    assert!(
        case.transport
            .flush_detached_peer_sends_with_now(&|| start)
            .is_err()
    );
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    assert_eq!(case.transport.settled_commit_promotion_count(), 0);
    let baseline = case.runtime.rpc_observations().len();
    case.transport
        .flush_detached_peer_sends_with_now(&|| start)
        .unwrap();
    assert_eq!(
        case.runtime.rpc_observations().len(),
        baseline,
        "cooling makes no new dispatch"
    );
    case.enqueue(0, 0);
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    case.runtime.clear_faults();
    case.transport
        .flush_detached_peer_sends_with_now(&|| start + DFS_DETACHED_SEND_RETRY_DELAY)
        .unwrap();
    assert_eq!(case.transport.pending_commit_promotion_count(), 0);
    assert_eq!(case.transport.settled_commit_promotion_count(), 1);
    case.finish();
}

#[test]
fn native_promotion_lost_actual_ack_retries_without_early_settlement() {
    native_failed_promotion_retries_actual_receiver(
        FaultAction::LoseReplyAfterDispatch,
        "native-promotion-lost-ack",
    );
}

#[test]
fn native_promotion_unsent_failure_retains_original_work() {
    native_failed_promotion_retries_actual_receiver(
        FaultAction::BudgetExhausted,
        "native-promotion-unsent",
    );
}

#[test]
fn native_cooling_promotion_does_not_starve_other_real_receiver() {
    let mut case = NativePromotions::new("native-promotion-bypass", 3, 2);
    case.fault(0, FaultAction::BudgetExhausted);
    case.enqueue(0, 0);
    case.enqueue(1, 1);
    let start = Instant::now();
    assert!(
        case.transport
            .flush_detached_peer_sends_with_now(&|| start)
            .is_err()
    );
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    assert_eq!(case.transport.settled_commit_promotion_count(), 1);
    let calls = case.runtime.rpc_observations();
    assert!(calls.iter().any(|event| event.target == case.targets[1]
        && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
        && event.phase == RpcPhase::AfterDispatch));
    case.runtime.clear_faults();
    case.transport
        .flush_detached_peer_sends_with_now(&|| start + DFS_DETACHED_SEND_RETRY_DELAY)
        .unwrap();
    assert_eq!(case.transport.settled_commit_promotion_count(), 2);
    case.finish();
}

#[test]
fn native_promotion_rejects_missing_or_corrupted_captured_acknowledgements() {
    use crate::test_runtime::CommitAckFault;
    for (fault, label) in [
        (CommitAckFault::Missing, "native-promotion-missing-ack"),
        (CommitAckFault::Durable, "native-promotion-durable-ack"),
        (
            CommitAckFault::WrongContent,
            "native-promotion-wrong-content",
        ),
        (CommitAckFault::WrongObject, "native-promotion-wrong-object"),
        (CommitAckFault::WrongSender, "native-promotion-wrong-sender"),
    ] {
        native_failed_promotion_retries_actual_receiver(
            FaultAction::AlterCommitAckAfterDispatch(fault),
            label,
        );
    }
}

#[test]
fn native_shared_drains_limit_inflight_promotions_and_yield_to_readers() {
    let mut case = NativePromotions::new("native-promotion-global-cap", 4, 3);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for target in 0..3 {
        case.fault(target, FaultAction::PauseBeforeDispatch(gate.clone()));
        case.enqueue(target, target);
    }
    let baseline = case.runtime.rpc_observations().len();
    let mut worker = case.transport.fork_detached_send_worker();
    let mut transport = case.transport;
    let first = std::thread::spawn(move || {
        let result = transport.flush_detached_sends();
        (transport, result)
    });
    let second = std::thread::spawn(move || worker.drain());
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let dispatched = case
            .runtime
            .rpc_observations()
            .iter()
            .skip(baseline)
            .filter(|event| {
                event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                    && event.phase == RpcPhase::BeforeDispatch
            })
            .count();
        assert!(
            dispatched <= 2,
            "the shared normal outbox must retain the global in-flight bound"
        );
        if dispatched == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "normal sends must reach their gated receivers"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut foreground = case.runtime.client(&case.source).unwrap();
    assert_eq!(
        foreground.posix_read_file("/promotion-0").unwrap(),
        b"native promotion body 0"
    );
    drop(foreground);
    gate.add_permits(32);
    let (transport, result) = first.join().unwrap();
    case.transport = transport;
    result.unwrap();
    second.join().unwrap().unwrap();
    case.runtime.clear_faults();
    case.transport.flush_detached_sends().unwrap();
    assert_eq!(case.transport.settled_commit_promotion_count(), 3);
    assert_eq!(case.transport.pending_commit_promotion_count(), 0);
    case.finish();
}
