//! Async placement session tests. The remote effect uses the real durable metadata
//! backend; transport timing is controlled and does not claim receiver-protocol
//! or Raft integration coverage.
use super::*;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::kv::InProcessKvStore;
use sinomo_core::network::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum Mode {
    PauseBeforeEffect,
    PauseAfterEffect,
    PauseWhileWaiting,
    Applied,
    LostReply,
}

#[derive(Default)]
struct Observed {
    commands: Vec<DfsNamespaceCommand>,
    targets: Vec<NodeId>,
    dispatch_deadlines: Vec<Option<Instant>>,
    dropped_sends: usize,
}

struct SendLifetime(Arc<Mutex<Observed>>);
impl Drop for SendLifetime {
    fn drop(&mut self) {
        self.0.lock().unwrap().dropped_sends += 1;
    }
}

struct OwnedTransport {
    local: NodeId,
    mode: Mode,
    deadline: Option<Instant>,
    remote_root: PathBuf,
    reached: Arc<tokio::sync::Notify>,
    observed: Arc<Mutex<Observed>>,
    incoming: Vec<DfsRoutedMessage>,
}

impl DfsTransportIdentity for OwnedTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local
    }
}
impl DfsMessageTransport for OwnedTransport {
    fn send_routed_call(&mut self, _: &NodeId, _: DfsRoutedCall) -> Result<(), NetworkError> {
        panic!("placement must use async send")
    }
    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        panic!("placement must use async drain")
    }
    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        std::mem::replace(&mut self.deadline, deadline)
    }
    fn operation_deadline(&self) -> Option<Instant> {
        self.deadline
    }
}
impl OwnedTransport {
    fn apply_remote_effect(&self, command: &DfsNamespaceCommand) {
        let DfsNamespaceOperation::ReconcilePlacement {
            object_id,
            expected_version,
            path,
            object_policy,
            ..
        } = &command.operation
        else {
            panic!("placement command")
        };
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &self.remote_root)
                .unwrap();
        let mut object = backend.get_object(object_id).unwrap().unwrap();
        assert_eq!(&object.path, path);
        assert_eq!(&object.version, expected_version);
        assert_eq!(object.file_system_id, command.file_system_id);
        object.policy = object_policy.clone();
        backend.put_object(&object).unwrap();
        // This constructor/put/drop exercises durable metadata, not an atomic
        // flag that a cancelled sender could mistake for committed state.
    }
}
impl AsyncDfsMessageTransport for OwnedTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local
    }
    fn send_routed_call_async<'a>(
        &'a mut self,
        target: &'a NodeId,
        call: DfsRoutedCall,
    ) -> crate::DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let _lifetime = SendLifetime(Arc::clone(&self.observed));
            assert!(call.attachment.is_none());
            let routed = call.message;
            let DfsNetworkMessage::NamespaceRequest(command) = routed.message else {
                panic!("namespace request")
            };
            {
                let mut observed = self.observed.lock().unwrap();
                observed.commands.push(command.clone());
                observed.targets.push(target.clone());
                observed.dispatch_deadlines.push(self.deadline);
            }
            if matches!(self.mode, Mode::PauseBeforeEffect) {
                self.reached.notify_one();
                std::future::pending::<()>().await;
            }
            self.apply_remote_effect(&command);
            if matches!(self.mode, Mode::PauseAfterEffect) {
                self.reached.notify_one();
                std::future::pending::<()>().await;
            }
            if matches!(self.mode, Mode::LostReply) {
                return Err(NetworkError::timeout(
                    "owned placement reply lost after durable effect",
                    NetworkTimeoutPhase::Overall,
                    Duration::from_millis(1),
                    NetworkDeliveryState::PossiblyApplied,
                ));
            }
            if matches!(self.mode, Mode::Applied) {
                self.incoming.push(DfsRoutedMessage {
                    source_node_id: target.clone(),
                    target_node_id: self.local.clone(),
                    frame_id: routed.frame_id,
                    message_id: routed.message_id,
                    message: DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                        request_id: command.request_id,
                        result: DfsNamespaceResult::Applied,
                        synced_path_object: None,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: Some(false),
                        posix_coordination_result: None,
                    }),
                });
            }
            Ok(())
        })
    }
    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> crate::DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move { Ok(std::mem::take(&mut self.incoming)) })
    }
    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> crate::DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { Ok(Some(self.incoming.len())) })
    }
    fn wait_for_incoming_async<'a>(
        &'a mut self,
        _: Duration,
    ) -> crate::DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move {
            assert!(matches!(self.mode, Mode::PauseWhileWaiting));
            self.reached.notify_one();
            std::future::pending::<Result<bool, NetworkError>>().await
        })
    }
}

struct Fixture {
    service: NetworkedDfsService<OwnedTransport>,
    primary: NodeId,
    object: DfsObjectMetadata,
    shard: DfsMetadataShardConfig,
    prior_deadline: Instant,
    observed: Arc<Mutex<Observed>>,
    reached: Arc<tokio::sync::Notify>,
    remote_root: PathBuf,
}
fn fixture(root: &Path, mode: Mode) -> Fixture {
    let local = NodeId::from_legacy_named("async-placement-requester").unwrap();
    let primary = NodeId::from_legacy_named("async-placement-primary").unwrap();
    let fs = distributed_sifs(primary.clone());
    let mut object = distributed_sifs_bootstrap_objects(primary.clone()).remove(0);
    object.object_id = DfsObjectId::from_legacy_named("async-placement-object").unwrap();
    object.path = "/services/async-placement".into();
    object.policy = DfsObjectPolicy::LocalOwned;
    let remote_root = root.join("remote-metadata");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &remote_root).unwrap();
    backend.put_file_system(&fs).unwrap();
    backend.put_object(&object).unwrap();
    drop(backend);
    let shard = DfsMetadataShardConfig {
        file_system_id: fs.file_system_id,
        shard_id: "root".into(),
        primary_node_id: primary.clone(),
        replica_node_ids: Vec::new(),
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let cp = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    cp.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .unwrap()
    });
    let observed = Arc::new(Mutex::new(Observed::default()));
    let reached = Arc::new(tokio::sync::Notify::new());
    let prior_deadline = Instant::now() + Duration::from_secs(60);
    let transport = OwnedTransport {
        local,
        mode,
        deadline: Some(prior_deadline),
        remote_root: remote_root.clone(),
        reached: Arc::clone(&reached),
        observed: Arc::clone(&observed),
        incoming: Vec::new(),
    };
    let mut service = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_control_plane(cp);
    service.routed_namespace_settle_depth = 3;
    service.pending_routed_namespace_ops.insert(
        "unrelated".into(),
        PendingRoutedNamespaceOp {
            primary_node_id: primary.clone(),
            dispatch_authority_shard: Some(shard.clone()),
            authority_shards: vec![shard.clone()],
            replay_journal_batches: false,
            apply_synced_path_object: false,
            result: None,
        },
    );
    Fixture {
        service,
        primary,
        object,
        shard,
        prior_deadline,
        observed,
        reached,
        remote_root,
    }
}
fn operation(f: &Fixture) -> DfsNamespaceOperation {
    DfsNamespaceOperation::ReconcilePlacement {
        job_id: "placement-job".into(),
        path: f.object.path.clone(),
        object_id: f.object.object_id.clone(),
        expected_version: f.object.version.clone(),
        object_policy: DfsObjectPolicy::ReplicatedEventual,
        replication_policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        desired_replica_node_ids: vec![f.primary.clone()],
    }
}
fn assert_restored(f: &Fixture) {
    assert_eq!(
        f.service.transport.operation_deadline(),
        Some(f.prior_deadline)
    );
    assert_eq!(f.service.routed_namespace_settle_depth, 3);
    assert_eq!(
        f.service
            .pending_routed_namespace_ops
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["unrelated"]
    );
}
fn remote_policy(f: &Fixture) -> DfsObjectPolicy {
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &f.remote_root).unwrap();
    backend
        .get_object(&f.object.object_id)
        .unwrap()
        .unwrap()
        .policy
}

#[tokio::test]
async fn async_placement_cancel_before_remote_effect_restores_owned_state() {
    let root = sinomo_testdir::TestDir::new("async-placement-cancel-before");
    let mut f = fixture(root.path(), Mode::PauseBeforeEffect);
    let op = operation(&f);
    let reached = Arc::clone(&f.reached);
    let mut request = Box::pin(
        f.service
            .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
                "request-before".into(),
                f.object.file_system_id.clone(),
                f.primary.clone(),
                op,
                Duration::from_secs(120),
                false,
                false,
            ),
    );
    tokio::select! { biased; _ = reached.notified() => {}, result = &mut request => panic!("must pause: {result:?}") }
    drop(request);
    assert_restored(&f);
    assert_eq!(remote_policy(&f), DfsObjectPolicy::LocalOwned);
    assert_eq!(f.observed.lock().unwrap().dropped_sends, 1);
}

#[tokio::test]
async fn async_placement_cancel_after_durable_effect_never_rolls_it_back() {
    let root = sinomo_testdir::TestDir::new("async-placement-cancel-after");
    for (index, mode) in [Mode::PauseAfterEffect, Mode::PauseWhileWaiting]
        .into_iter()
        .enumerate()
    {
        let mut f = fixture(&root.path().join(index.to_string()), mode);
        let op = operation(&f);
        let reached = Arc::clone(&f.reached);
        let mut request = Box::pin(f.service.route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "request-after".into(), f.object.file_system_id.clone(), f.primary.clone(), op,
            Duration::from_secs(120), false, false,
        ));
        tokio::select! { biased; _ = reached.notified() => {}, result = &mut request => panic!("must pause: {result:?}") }
        drop(request);
        assert_restored(&f);
        assert_eq!(remote_policy(&f), DfsObjectPolicy::ReplicatedEventual);
    }
}

#[tokio::test]
async fn async_placement_unknown_delivery_keeps_exact_request_and_durable_effect() {
    let root = sinomo_testdir::TestDir::new("async-placement-unknown-result");
    let mut f = fixture(root.path(), Mode::LostReply);
    let op = operation(&f);
    let error = f
        .service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "request-unknown".into(),
            f.object.file_system_id.clone(),
            f.primary.clone(),
            op.clone(),
            Duration::from_secs(120),
            false,
            false,
        )
        .await
        .expect_err("reply lost after commit");
    assert!(matches!(
        error,
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::PossiblyApplied,
            ..
        }
    ));
    assert_restored(&f);
    assert_eq!(remote_policy(&f), DfsObjectPolicy::ReplicatedEventual);
    let observed = f.observed.lock().unwrap();
    assert_eq!(
        observed.commands.len(),
        1,
        "lower layer never retries with a fresh identity"
    );
    assert_eq!(observed.commands[0].request_id, "request-unknown");
    assert_eq!(observed.commands[0].operation, op);
    assert_eq!(observed.commands[0].authority_shards, vec![f.shard.clone()]);
    assert_eq!(observed.dispatch_deadlines, vec![Some(f.prior_deadline)]);
}

#[tokio::test]
async fn async_placement_applied_reply_uses_unchanged_response_validation_and_restores_session() {
    let root = sinomo_testdir::TestDir::new("async-placement-applied");
    let mut f = fixture(root.path(), Mode::Applied);
    let op = operation(&f);
    let result = f
        .service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "request-applied".into(),
            f.object.file_system_id.clone(),
            f.primary.clone(),
            op.clone(),
            Duration::from_secs(120),
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.result, DfsNamespaceResult::Applied);
    assert_restored(&f);
    let observed = f.observed.lock().unwrap();
    assert_eq!(observed.commands[0].request_id, "request-applied");
    assert_eq!(observed.commands[0].operation, op);
    assert!(observed.commands[0].include_metadata_journal_batches);
    assert_eq!(observed.commands[0].authority_shards, vec![f.shard.clone()]);
    assert_eq!(observed.targets, vec![f.primary.clone()]);
}

#[tokio::test]
async fn async_placement_rejects_other_operation_and_existing_slot_without_mutation() {
    let root = sinomo_testdir::TestDir::new("async-placement-narrow-api");
    let mut f = fixture(root.path(), Mode::Applied);
    let error = f
        .service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "not-placement".into(),
            f.object.file_system_id.clone(),
            f.primary.clone(),
            DfsNamespaceOperation::LookupPath {
                path: f.object.path.clone(),
                inline_range: None,
            },
            Duration::from_secs(1),
            false,
            false,
        )
        .await
        .expect_err("non-placement rejected");
    assert!(error.to_string().contains("only ReconcilePlacement"));
    let op = operation(&f);
    f.service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "unrelated".into(),
            f.object.file_system_id.clone(),
            f.primary.clone(),
            op,
            Duration::from_secs(1),
            false,
            false,
        )
        .await
        .expect_err("existing response slot is not owned by this call");
    assert_restored(&f);
    assert!(f.observed.lock().unwrap().commands.is_empty());
}

#[tokio::test]
async fn async_placement_expired_existing_deadline_never_dispatches() {
    let root = sinomo_testdir::TestDir::new("async-placement-expired-deadline");
    let mut f = fixture(root.path(), Mode::Applied);
    let expired = Instant::now() - Duration::from_secs(1);
    f.prior_deadline = expired;
    f.service.transport.set_operation_deadline(Some(expired));
    let op = operation(&f);
    let error = f
        .service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "request-expired".into(),
            f.object.file_system_id.clone(),
            f.primary.clone(),
            op,
            Duration::from_secs(120),
            false,
            false,
        )
        .await
        .expect_err("existing absolute deadline is never extended");
    assert!(matches!(
        error,
        DfsError::NetworkDelivery {
            delivery_state: NetworkDeliveryState::DefinitelyNotSent,
            ..
        }
    ));
    assert_restored(&f);
    assert!(f.observed.lock().unwrap().commands.is_empty());
    assert_eq!(remote_policy(&f), DfsObjectPolicy::LocalOwned);
}
