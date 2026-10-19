//! Detached promotion observations from actual native receivers.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use crate::{DfsWriteIntentStatus, DfsWriteOptions};
use std::sync::Mutex;

// Count only attempts made by this fixture's real promotion outbox. The
// adapter forwards the original owned SOBJ call and response untouched to the
// same registered native transport; unrelated host RPCs use their own adapter.
struct ObservedPromotionPeer {
    inner: Arc<dyn DfsPeerTransport>,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}
impl DfsPeerTransport for ObservedPromotionPeer {
    fn response_delivery_owner(&self) -> Option<Arc<crate::DfsRoutedResponseDelivery>> {
        self.inner.response_delivery_owner()
    }
    fn call_object_rpc(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        assert_eq!(call.request.route, crate::DFS_DATA_COMMIT_VERSION_ROUTE);
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.call_object_rpc(target, call, options)
    }
}

struct NativePromotions {
    runtime: DfsTestRuntime,
    source: NodeId,
    targets: Vec<NodeId>,
    transport: DfsNodeTransport,
    commands: Vec<crate::DfsCommitVersionCommand>,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
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
                    DfsWriteOptions {
                        // This fixture later promotes on every listed receiver.
                        // The convenience Eventual helper caps desired replicas
                        // at two, which cannot seed the three/four-node cases.
                        policy: crate::DfsReplicationPolicy::new(1, roster.len()).unwrap(),
                        replica_candidates: roster.clone(),
                        wait_for_commit: true,
                        foreground_replica_repair: true,
                    },
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
            assert_eq!(intent.policy.desired_replicas, roster.len());
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
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    assert!(
                        !remaining.is_zero(),
                        "normal foreground placement must finish before observing an exact promotion replay"
                    );
                    // W=1 can return while the rest of the desired copies are
                    // queued. Drive the same normal served owner inside the
                    // original setup budget, without changing startup timers.
                    let pass = runtime.maintenance_tick(&source, remaining).unwrap();
                    assert!(
                        pass.outcome.is_ok(),
                        "native promotion setup: {:?}",
                        pass.outcome
                    );
                    assert_eq!(pass.omitted_kv_event_count, 0);
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        let (sync, asynchronous) = runtime.peer_transports(&source).unwrap();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sync = Arc::new(ObservedPromotionPeer {
            inner: sync,
            attempts: Arc::clone(&attempts),
        });
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
            attempts,
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
    fn promotion_dispatch_count(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
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

#[test]
fn native_deferred_worker_cancellation_releases_claim_and_preserves_real_outbox() {
    let mut case = NativePromotions::new("native-promotion-worker-cancellation", 2, 2);
    let runtime = Arc::new(crate::runtime_message::DfsDetachedSendRuntime::new());
    case.transport.detached_send_state = runtime.detached_state();
    case.enqueue(0, 0);
    let worker = case.transport.fork_detached_send_worker();
    let mut unpolled = None;
    runtime.schedule_deferred_work(
        worker.clone(),
        BTreeSet::new(),
        Duration::from_millis(10),
        |task| unpolled = Some(task),
    );
    assert!(runtime.worker_is_active());
    drop(
        unpolled
            .take()
            .expect("the actual scheduled future has not been polled"),
    );
    assert!(
        !runtime.worker_is_active(),
        "unpolled cancellation releases the active claim"
    );
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    assert_eq!(case.transport.settled_commit_promotion_count(), 0);

    let executor = case.runtime.runtime_handle();
    for expected_settled in 1..=2 {
        if expected_settled == 2 {
            case.enqueue(1, 0);
        }
        let mut started = None;
        runtime.schedule_deferred_work(
            worker.clone(),
            BTreeSet::new(),
            Duration::from_millis(10),
            |task| started = Some(executor.spawn(task)),
        );
        let task = started.expect("the cancelled worker cannot suppress a normal replacement");
        executor.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while case.transport.settled_commit_promotion_count() != expected_settled {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("actual receiver ACK settles the retained original promotion");
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        });
        assert!(
            !runtime.worker_is_active(),
            "polled cancellation also releases its claim"
        );
        assert_eq!(case.transport.pending_commit_promotion_count(), 0);
    }
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
    let baseline = case.promotion_dispatch_count();
    assert_eq!(
        baseline, 1,
        "the original promotion attempt reached the native transport exactly once"
    );
    // An ordinary namespace lookup is independent of the frozen promotion
    // retry clock. Keep real unrelated traffic in the observation window.
    let before_lookup = case.runtime.rpc_observations().len();
    let mut browser = case.runtime.client(&case.source).unwrap();
    browser.posix_stat_distributed("/").unwrap();
    drop(browser);
    assert!(
        case.runtime.rpc_observations()[before_lookup..]
            .iter()
            .any(|event| {
                event.route == crate::DFS_DATA_NAMESPACE_ROUTE
                    && event.phase == RpcPhase::BeforeDispatch
            })
    );
    case.transport
        .flush_detached_peer_sends_with_now(&|| start)
        .unwrap();
    assert_eq!(
        case.promotion_dispatch_count(),
        baseline,
        "cooling makes no new dispatch of this actual promotion"
    );
    case.enqueue(0, 0);
    assert_eq!(case.transport.pending_commit_promotion_count(), 1);
    case.runtime.clear_faults();
    case.transport
        .flush_detached_peer_sends_with_now(&|| start + DFS_DETACHED_SEND_RETRY_DELAY)
        .unwrap();
    assert_eq!(case.transport.pending_commit_promotion_count(), 0);
    assert_eq!(case.transport.settled_commit_promotion_count(), 1);
    assert_eq!(
        case.promotion_dispatch_count(),
        baseline + 1,
        "the same captured promotion retries only after its unchanged deadline"
    );
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

// The failure is after a genuine native Fetch has returned, at the admitted
// local completion boundary. The observer never supplies a receipt or authority.
#[test]
fn native_detached_fetch_reply_survives_local_completion_failure() {
    let mut case = NativePromotions::new("native-fetch-local-completion", 2, 1);
    let (sync, asynchronous) = case.runtime.peer_transports(&case.source).unwrap();
    case.transport = DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
        case.source.clone(),
        case.targets.clone(),
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync,
        asynchronous,
        DfsNodeDetachedSendState::new(),
    );
    let actual = &case.commands[0];
    let command = crate::DfsFetchBlobCommand {
        request_id: sinomo_core::Id::generate().to_string(),
        intent_id: actual.intent_id.clone(),
        object_id: actual.object_id.clone(),
        version: actual.version.clone(),
        content_hash: actual.content_hash.clone(),
        authority_shard: actual.authority_shard.clone(),
    };
    let request = crate::routes::build_dfs_routed_message(
        &case.source,
        &case.targets[0],
        1,
        1,
        DfsNetworkMessage::FetchBlob(command.clone()),
    );
    case.transport
        .send_detached_routed_message(&case.targets[0], request)
        .unwrap();
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&delivered);
    let expected_source = case.targets[0].clone();
    let handler = move |response: DfsRoutedMessage, original: Option<&DfsRoutedMessage>| {
        assert!(
            original.is_none(),
            "a received Fetch is not an outbound repair capability"
        );
        assert_eq!(response.source_node_id, expected_source);
        let DfsNetworkMessage::FetchBlobResponse(body) = &response.message else {
            panic!("actual Fetch response")
        };
        assert_eq!(body.request_id, command.request_id);
        assert_eq!(body.intent_id, command.intent_id);
        assert_eq!(body.object_id, command.object_id);
        assert_eq!(body.version, command.version);
        assert!(
            matches!(&body.result, crate::DfsFetchBlobResult::Found { content_hash, .. } if *content_hash == command.content_hash)
        );
        let bytes = serde_json::to_vec(&response).unwrap();
        let mut seen = seen.lock().unwrap();
        if let Some(first) = seen.first() {
            assert_eq!(first, &bytes, "exact received frame survives retries");
        }
        seen.push(bytes);
        if seen.len() < 3 {
            Err(DfsError::Network {
                message: "injected local completion failure after actual Fetch".into(),
            })
        } else {
            Ok(())
        }
    };
    let before = case.runtime.rpc_observations().len();
    let start = Instant::now();
    assert!(
        case.transport
            .flush_detached_peer_sends_with_now_mode(
                &|| start,
                false,
                &BTreeSet::new(),
                Some(&handler)
            )
            .is_err()
    );
    assert_eq!(delivered.lock().unwrap().len(), 1);
    assert_eq!(
        case.transport.detached_send_count(),
        1,
        "failed local completion retains the original received reply"
    );
    case.transport
        .flush_detached_peer_sends_with_now_mode(&|| start, false, &BTreeSet::new(), Some(&handler))
        .unwrap();
    assert_eq!(
        delivered.lock().unwrap().len(),
        1,
        "unchanged retry cooling"
    );
    // Dropping the worker does not cancel the shared outbox's exact delivery.
    let worker = case.transport.fork_detached_send_worker();
    drop(worker);
    let mut replacement = case.transport.fork_detached_send_worker();
    // A clone without the admitted delivery callback cannot silently resend or
    // consume this already-returned work.
    assert!(
        replacement
            .transport
            .flush_detached_peer_sends_with_now(&|| start + DFS_DETACHED_SEND_RETRY_DELAY)
            .is_err()
    );
    assert_eq!(delivered.lock().unwrap().len(), 1);
    assert_eq!(case.transport.detached_send_count(), 1);
    let retry = start + DFS_DETACHED_SEND_RETRY_DELAY + dfs_detached_send_retry_delay(2);
    assert!(
        replacement
            .transport
            .flush_detached_peer_sends_with_now_mode(
                &|| retry,
                false,
                &BTreeSet::new(),
                Some(&handler)
            )
            .is_err()
    );
    assert_eq!(delivered.lock().unwrap().len(), 2);
    let next = retry + dfs_detached_send_retry_delay(3);
    replacement
        .transport
        .flush_detached_peer_sends_with_now_mode(&|| next, false, &BTreeSet::new(), Some(&handler))
        .unwrap();
    assert_eq!(delivered.lock().unwrap().len(), 3);
    assert_eq!(case.transport.detached_send_count(), 0);
    assert_eq!(
        case.runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.route == crate::DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == RpcPhase::BeforeDispatch)
            .count(),
        1,
        "local completion replays the actual received frame without a new remote request"
    );
    drop(replacement);
    case.finish();
}

#[test]
fn native_background_repair_reply_survives_local_completion_failure() {
    let mut case = NativePromotions::new("native-repair-local-completion", 2, 1);
    let (sync, asynchronous) = case.runtime.peer_transports(&case.source).unwrap();
    case.transport = DfsNodeTransport::with_peer_transports_for_nodes_and_detached_state(
        case.source.clone(),
        case.targets.clone(),
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync,
        asynchronous,
        DfsNodeDetachedSendState::new(),
    );
    let actual = &case.commands[0];
    let source = case.runtime.client(&case.source).unwrap();
    let (_, bytes) = source
        .content
        .read_object_version(actual.object_id.as_str(), actual.version.as_str())
        .unwrap();
    assert_eq!(crate::content_hash(&bytes), actual.content_hash);
    drop(source);
    let command = crate::DfsRepairReplicaCommand {
        background_repair: true,
        request_id: sinomo_core::Id::generate().to_string(),
        intent_id: actual.intent_id.clone(),
        file_system_id: actual.file_system_id.clone(),
        object_id: actual.object_id.clone(),
        expected_base_version: actual.expected_base_version.clone(),
        version: actual.version.clone(),
        content_hash: actual.content_hash.clone(),
        bytes: Vec::new(),
        object_metadata: actual.object_metadata.clone(),
        authority_shard: actual.authority_shard.clone(),
    };
    let original = crate::routes::build_dfs_routed_message(
        &case.source,
        &case.targets[0],
        1,
        1,
        DfsNetworkMessage::RepairReplica(command.clone()),
    );
    let mut worker = case.transport.fork_detached_send_worker();
    // Actual native content and publication identity supply this owned reader.
    // No response, receipt, authority or delivery outcome is fabricated here.
    worker
        .prepared_replica_calls
        .lock()
        .unwrap()
        .push(DfsCheckpointedReplicaCall {
            request: DfsRoutedSend::new(
                case.targets[0].clone(),
                DfsRoutedCall::with_attachment(
                    original.clone(),
                    DfsOutboundAttachment::from_bytes(bytes, actual.content_hash.clone()),
                ),
            ),
            producer_delivery: std::sync::OnceLock::new(),
        });
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&delivered);
    let expected = original.clone();
    let handler = move |response: DfsRoutedMessage, original: Option<&DfsRoutedMessage>| {
        assert_eq!(original, Some(&expected), "exact original local call DATA");
        assert_eq!(response.source_node_id, expected.target_node_id);
        assert_eq!(response.target_node_id, expected.source_node_id);
        let DfsNetworkMessage::RepairReplicaResponse(body) = &response.message else {
            panic!("actual native RepairReplica response")
        };
        assert_eq!(body.request_id, command.request_id);
        assert_eq!(body.intent_id, command.intent_id);
        assert_eq!(body.file_system_id, command.file_system_id);
        assert_eq!(body.object_id, command.object_id);
        assert_eq!(body.version, command.version);
        assert_eq!(body.replica_node_id, expected.target_node_id);
        assert!(
            matches!(&body.result, DfsBlobTransferResult::Committed { content_hash }
            if *content_hash == command.content_hash)
        );
        let frame = serde_json::to_vec(&response).unwrap();
        let mut seen = seen.lock().unwrap();
        if let Some(first) = seen.first() {
            assert_eq!(first, &frame, "same received frame, not another RPC result");
        }
        seen.push(frame);
        if seen.len() < 3 {
            Err(DfsError::Network {
                message: "injected local publication failure after genuine repair reply".into(),
            })
        } else {
            Ok(())
        }
    };
    let handler: Arc<
        dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError> + Send + Sync,
    > = Arc::new(handler);
    worker.background_response_handler = Some(Arc::clone(&handler));
    let before = case.runtime.rpc_observations().len();
    assert!(worker.drain().is_err());
    assert_eq!(delivered.lock().unwrap().len(), 1);
    assert_eq!(
        case.transport.detached_send_count(),
        1,
        "local publication failure must retain the genuine repair reply"
    );
    {
        let outbox = case.transport.detached_send_state.inner.lock().unwrap();
        let retained = outbox.cooling_retries.values().flatten().next().unwrap();
        assert!(
            retained.sender.is_none(),
            "consumed attachment has no resend path"
        );
        assert_eq!(retained.routed_message, original);
    }
    // Negative controls change copies of this ACTUAL receiver frame only. None
    // may replace the original completion or become evidence for another call.
    let actual_response: DfsRoutedMessage =
        serde_json::from_slice(&delivered.lock().unwrap()[0]).unwrap();
    for field in [
        "source",
        "target",
        "request",
        "intent",
        "filesystem",
        "object",
        "version",
        "replica",
        "hash",
    ] {
        let mut changed = actual_response.clone();
        if field == "source" {
            changed.source_node_id = case.source.clone();
        } else if field == "target" {
            changed.target_node_id = case.targets[0].clone();
        } else {
            let DfsNetworkMessage::RepairReplicaResponse(body) = &mut changed.message else {
                unreachable!("copied actual repair response")
            };
            match field {
                "request" => body.request_id.push_str("-changed"),
                "intent" => body.intent_id.push_str("-changed"),
                "filesystem" => body.file_system_id = crate::DfsFileSystemId::generate(),
                "object" => body.object_id = crate::DfsObjectId::generate(),
                "version" => body.version = crate::DfsVersion::generate(),
                "replica" => body.replica_node_id = case.source.clone(),
                "hash" => {
                    body.result = DfsBlobTransferResult::Committed {
                        content_hash: crate::content_hash(b"different actual bytes"),
                    };
                }
                _ => unreachable!(),
            }
        }
        assert!(
            case.transport
                .retain_background_replica_completion(original.clone(), changed)
                .is_err(),
            "reject changed {field} without replacing original receipt DATA"
        );
        assert_eq!(case.transport.detached_send_count(), 1);
    }
    let mut changed_call = original.clone();
    let DfsNetworkMessage::RepairReplica(body) = &mut changed_call.message else {
        unreachable!()
    };
    body.background_repair = false;
    assert!(
        case.transport
            .retain_background_replica_completion(changed_call, actual_response)
            .is_err()
    );
    assert_eq!(case.transport.detached_send_count(), 1);
    assert!(worker.prepared_replica_calls.lock().unwrap().is_empty());
    let (ready, deadline, _) = worker.scheduling_snapshot().unwrap();
    assert!(!ready, "the existing retry cooling remains in force");
    let deadline = deadline.expect("retained completion cooling deadline");
    worker.drain_at(deadline - Duration::from_nanos(1)).unwrap();
    assert_eq!(delivered.lock().unwrap().len(), 1);
    drop(worker);
    let mut replacement = case.transport.fork_detached_send_worker();
    assert!(replacement.drain_at(deadline).is_err());
    assert_eq!(delivered.lock().unwrap().len(), 1);
    assert_eq!(case.transport.detached_send_count(), 1);
    replacement.background_response_handler = Some(handler);
    let retry = deadline + dfs_detached_send_retry_delay(2);
    assert!(replacement.drain_at(retry).is_err());
    assert_eq!(delivered.lock().unwrap().len(), 2);
    replacement
        .drain_at(retry + dfs_detached_send_retry_delay(3))
        .unwrap();
    assert_eq!(delivered.lock().unwrap().len(), 3);
    assert_eq!(case.transport.detached_send_count(), 0);
    let events = case.runtime.rpc_observations();
    assert_eq!(
        events[before..]
            .iter()
            .filter(|event| event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == RpcPhase::BeforeDispatch)
            .count(),
        1,
        "retry only local completion; never resend the consumed repair attachment"
    );
    drop(replacement);
    case.finish();
}
