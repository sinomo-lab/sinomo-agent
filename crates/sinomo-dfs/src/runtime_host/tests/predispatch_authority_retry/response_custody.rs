//! A native private requester must receive completion delivered to its daemon.
//!
//! The carrier schedules real primary/source services deterministically. It
//! never fabricates Pending or terminal write outcomes and never retries the
//! original write. Only the primary's generated terminal frames cross resident
//! daemon ingress, as they do when routine fetch completes a browser write.

use super::*;
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteRepair};
use crate::{
    DfsDistributedWriteResult, DfsMessageTransport, DfsWriteIntentStatus, NetworkedDfsService,
    NetworkedDfsWriteOutcome, SharedDfsControlPlaneKvClient,
};
use sinomo_core::kv::InProcessKvStore;
use sinomo_core::network::{
    ClusterObjectRpcOptions, InProcessNetwork, InProcessTransport, NetworkError, ObjectRpcResponse,
};

use sinomo_core::service::{
    ResponseSink, ServiceContext, ServiceError, ServiceRegistry, ServiceSigner,
};

const PAYLOAD: &[u8] = b"private requester completion belongs to the original native service";
const OPERATION: &str = "native-private-response-custody";

type PeerService = NetworkedDfsService<InProcessTransport>;

struct PrimaryState {
    primary: PeerService,
    source: PeerService,
    held_fetches: Vec<DfsRoutedMessage>,
    intent_id: Option<String>,
    network: InProcessNetwork,
    control: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    root: std::path::PathBuf,
    original_dispatches: usize,
    terminal_frames: usize,
}

struct PrimaryCarrier {
    state: Mutex<PrimaryState>,
    runtime: tokio::runtime::Handle,
    registry: ServiceRegistry,
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    terminal_before_inline_pending: bool,
}

impl PrimaryCarrier {
    fn complete_and_deliver(&self) {
        let terminal = {
            let mut state = self.state.lock().unwrap();
            let intent_id = state.intent_id.as_ref().unwrap().clone();
            let intent = state
                .primary
                .metadata_backend
                .as_ref()
                .unwrap()
                .get_intent(
                    &state.primary.writes.pending_writes[&intent_id].object_id,
                    &intent_id,
                )
                .unwrap()
                .expect("the primary durably recorded the actual pending manifest");
            assert_eq!(intent.status, DfsWriteIntentStatus::ReceiptsPending);
            assert!(
                !intent
                    .desired_replica_node_ids
                    .contains(&state.source.node_id)
            );
            state
                .source
                .metadata_backend
                .as_mut()
                .unwrap()
                .put_intent(&intent)
                .unwrap();
            let fetches = std::mem::take(&mut state.held_fetches);
            assert_eq!(fetches.len(), 1, "one real requester fetch is sufficient");
            state
                .source
                .process_routed_messages_with_options(fetches, false)
                .unwrap();
            let source_responses = state.primary.transport.drain_incoming_messages().unwrap();
            assert_eq!(source_responses.len(), 1);
            // Lose the primary's volatile routed-request owner, just as the
            // daemon's routine lane can reopen a durable pending intent.
            state.primary.metadata_backend.take();
            let primary_node = state.primary.node_id.clone();
            state.primary = NetworkedDfsService::new(
                state.network.add_node(primary_node),
                LocalContentStore::new(state.root.join("primary-content")).unwrap(),
            )
            .with_control_plane(state.control.clone())
            .with_metadata_backend_lazy(
                crate::open_root_local_metadata_backend(state.root.join("primary-metadata"))
                    .unwrap(),
            )
            .unwrap();
            assert!(
                state
                    .primary
                    .writes
                    .pending_remote_write_responses
                    .is_empty()
            );
            // The real source emitted its response before the transport was
            // replaced; preserve that wire frame across the service reopen.
            state
                .primary
                .process_routed_messages_with_options(source_responses, false)
                .unwrap();
            state
                .primary
                .process_incoming_without_deferred_parent_touches()
                .unwrap();
            state.primary.retry_pending_repairs().unwrap();
            state
                .primary
                .process_incoming_without_deferred_parent_touches()
                .unwrap();
            let durable = state
                .primary
                .metadata_backend
                .as_ref()
                .unwrap()
                .get_intent(&intent.object_id, &intent.intent_id)
                .unwrap()
                .unwrap();
            assert_eq!(durable.status, DfsWriteIntentStatus::Committed);
            assert_eq!(
                state.primary.coordinator.durable_receipt_count(&intent_id),
                1
            );
            assert_eq!(
                state
                    .primary
                    .content
                    .read_object_version(intent.object_id.as_str(), intent.target_version.as_str(),)
                    .unwrap()
                    .1,
                PAYLOAD,
            );
            let terminal: Vec<_> = state
                .source
                .transport
                .drain_incoming_messages()
                .unwrap()
                .into_iter()
                .filter(|message| {
                    matches!(
                        &message.message,
                        DfsNetworkMessage::DistributedWriteResponse(response)
                            if matches!(response.result, DfsDistributedWriteResult::Outcome {
                                outcome: NetworkedDfsWriteOutcome::Committed { .. }
                            })
                    )
                })
                .collect();
            assert!(
                !terminal.is_empty(),
                "actual primary completion must emit a response"
            );
            state.terminal_frames += terminal.len();
            terminal
        };
        for message in terminal {
            let DfsNetworkMessage::DistributedWriteResponse(ref terminal_response) =
                message.message
            else {
                unreachable!()
            };
            assert_eq!(
                terminal_response.request_id,
                self.state
                    .lock()
                    .unwrap()
                    .intent_id
                    .as_ref()
                    .unwrap()
                    .as_str(),
                "a reopened primary completes with the durable intent alias"
            );
            let source = message.source_node_id.clone();
            let sink = VecSink::default();
            let frames = Arc::clone(&sink.frames);
            let context = ServiceContext::new(
                Arc::clone(&self.data_dir),
                Some(Arc::clone(&self.cluster)),
                ServiceSigner::Remote {
                    node_id: source.to_string(),
                    public_key: format!("pk-{source}"),
                },
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(sink),
            );
            let command = test_dfs_message_command("native-private-terminal", message);
            let request = sinomo_core::network::ObjectRpcRequest::encode_body(
                crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE,
                crate::DFS_RPC_CATEGORY,
                &command,
            )
            .unwrap();
            self.runtime
                .block_on(self.registry.dispatch(context, request))
                .expect("authenticated service dispatch through the actual native ingress");
            let outgoing = frames.lock().unwrap().pop().expect("resident ingress ACK");
            let mut response = outgoing.response;
            if response.body.is_empty() {
                response.body = self
                    .runtime
                    .block_on(outgoing.body.collect_to_vec(1024 * 1024))
                    .unwrap();
            }
            let response: DfsMessageResponse = response.decode_body().unwrap();
            assert!(response.outbound_messages.is_empty());
        }
    }
}

impl ComponentObjectRpcTransport for PrimaryCarrier {
    fn call(
        &self,
        target: &NodeId,
        mut call: ObjectRpcCall,
        _options: ClusterObjectRpcOptions,
        _observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError> {
        if call.request.body.is_empty() {
            call.request.body = self
                .runtime
                .block_on(call.body.collect_to_vec(1024 * 1024))?;
        }
        let command: DfsMessageCommand = call.request.decode_body()?;
        let pending = {
            let mut state = self.state.lock().unwrap();
            assert_eq!(target, &state.primary.node_id);
            assert!(matches!(
                command.routed_message.message,
                DfsNetworkMessage::DistributedWriteRequest(_)
            ));
            state.original_dispatches += 1;
            state
                .primary
                .process_routed_messages_with_options(vec![command.routed_message], false)
                .unwrap();
            let mut pending = Vec::new();
            for message in state.source.transport.drain_incoming_messages().unwrap() {
                match &message.message {
                    DfsNetworkMessage::FetchBlob(_) => state.held_fetches.push(message),
                    DfsNetworkMessage::DistributedWriteResponse(response) => {
                        if let DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Pending { intent_id, .. },
                        } = &response.result
                        {
                            state.intent_id = Some(intent_id.clone());
                        }
                        assert!(
                            matches!(
                                response.result,
                                DfsDistributedWriteResult::Outcome {
                                    outcome: NetworkedDfsWriteOutcome::Pending {
                                        durable_receipts: 0,
                                        required_receipts: 1,
                                        ..
                                    }
                                }
                            ),
                            "the initial outcome must be the real primary Pending: {response:?}"
                        );
                        pending.push(message);
                    }
                    other => panic!("unexpected initial primary message: {other:?}"),
                }
            }
            assert_eq!(pending.len(), 1);
            pending
        };
        if self.terminal_before_inline_pending {
            self.complete_and_deliver();
        }
        Ok(ObjectRpcResult::value(ObjectRpcResponse::encode_body(
            call.request.route,
            &DfsMessageResponse {
                request_id: command.request_id,
                outbound_messages: pending,
            },
        )?))
    }

    fn call_async<'a>(
        &'a self,
        target: &'a NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a>,
    > {
        Box::pin(async move { self.call(target, call, options, observe_connectivity) })
    }
}

#[derive(Default)]
struct VecSink {
    frames: Arc<Mutex<Vec<sinomo_core::network::ObjectRpcOutgoingResponse>>>,
}
impl ResponseSink for VecSink {
    fn send(
        self: Box<Self>,
        response: sinomo_core::network::ObjectRpcOutgoingResponse,
    ) -> Result<(), ServiceError> {
        self.frames.lock().unwrap().push(response);
        Ok(())
    }
}

fn run_private_requester_custody(terminal_before_inline_pending: bool) {
    let fixture = RetryFixture::with_write_configuration(
        if terminal_before_inline_pending {
            "custody-terminal-first"
        } else {
            "custody-pending-first"
        },
        true,
    );
    let root = fixture._root.join("remote-write");
    let file_system = distributed_sifs(fixture.remote.clone());
    let shard = DfsMetadataShardConfig {
        primary_node_id: fixture.remote.clone(),
        replica_node_ids: Vec::new(),
        ..fixture.shard.clone()
    };
    let native_config = append_cluster_raft_command(
        &fixture.cluster_paths,
        &fixture.local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            failure: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .unwrap(),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .unwrap(),
            ],
        }),
    )
    .unwrap();
    assert!(fixture._authority.install_authoritative_projection(
        test_dfs_authoritative_prefix_projection(&native_config)
    ));
    evict_daemon_dfs_message_service_cache(&fixture.data_dir).unwrap();
    let control = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control.with_client(|kv| {
        let mut store = crate::DfsControlPlaneStore::new(kv);
        store
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .unwrap();
        store.put_metadata_shard(&shard).unwrap();
    });
    let object = pending_object_metadata_for_test(
        DfsObjectId::generate(),
        file_system.file_system_id.clone(),
        DfsVersion::generate(),
        fixture.remote.clone(),
        "/private-custody.bin",
    );
    let network = InProcessNetwork::new();
    let peer_service = |node: NodeId, name: &str| {
        let mut metadata =
            crate::open_root_local_metadata_backend(root.join(format!("{name}-metadata"))).unwrap();
        metadata.put_file_system(&file_system).unwrap();
        metadata.put_object(&object).unwrap();
        NetworkedDfsService::new(
            network.add_node(node),
            LocalContentStore::new(root.join(format!("{name}-content"))).unwrap(),
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(metadata)
        .unwrap()
    };
    let primary = peer_service(fixture.remote.clone(), "primary");
    let source = peer_service(fixture.local.clone(), "source");
    let target_version = DfsVersion::generate();
    source
        .content
        .put_object_version(object.object_id.as_str(), target_version.as_str(), PAYLOAD)
        .unwrap();
    let baseline_host = test_dfs_runtime_host(&fixture.data_dir);
    let carrier = Arc::new(PrimaryCarrier {
        state: Mutex::new(PrimaryState {
            primary,
            source,
            held_fetches: Vec::new(),
            intent_id: None,
            network,
            control: control.clone(),
            root: root.clone(),
            original_dispatches: 0,
            terminal_frames: 0,
        }),
        runtime: fixture.runtime.handle().clone(),
        registry: crate::service::register_data_plane_services(
            ServiceRegistry::builder(),
            Arc::new(DaemonDfsDataPlaneHost::new_on_runtime(
                fixture.paths.clone(),
                fixture.runtime.handle(),
            )),
        )
        .build(),
        data_dir: Arc::new(fixture.data_dir.clone()),
        cluster: Arc::clone(&baseline_host.cluster),
        terminal_before_inline_pending,
    });
    let host = Arc::new(DfsRuntimeHost::new(
        Arc::clone(&baseline_host.cluster),
        carrier.clone(),
        None,
    ));
    register_dfs_runtime_host(&fixture.data_dir, &host).unwrap();
    let (sync, asynchronous) =
        daemon_dfs_peer_transports_on_runtime(&fixture.data_dir, fixture.runtime.handle());
    let mut private = NetworkedDfsService::new(
        DfsNodeTransport::with_peer_transports_for_nodes(
            fixture.local.clone(),
            [fixture.remote.clone()],
            Duration::from_secs(2),
            sync,
            asynchronous,
        ),
        LocalContentStore::new(root.join("source-content")).unwrap(),
    )
    .with_control_plane(control);
    let initial = private
        .route_distributed_manifest(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: fixture.local.clone(),
            },
            Some(object.version.clone()),
            crate::content_hash(PAYLOAD),
            PAYLOAD.len() as u64,
            vec![fixture.remote.clone()],
            DfsReplicationPolicy::new(1, 1).unwrap(),
            object,
            shard,
            DfsDistributedWriteOptions {
                operation_id: Some(OPERATION.into()),
                repair: DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(initial, NetworkedDfsWriteOutcome::Pending { .. }));
    if !terminal_before_inline_pending {
        private
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        assert!(matches!(
            private.write_outcome(OPERATION).unwrap(),
            NetworkedDfsWriteOutcome::Pending { .. }
        ));
        carrier.complete_and_deliver();
    }
    private
        .process_incoming_without_deferred_parent_touches()
        .unwrap();
    let observed = private.write_outcome(OPERATION).unwrap();
    let state = carrier.state.lock().unwrap();
    assert_eq!(
        state.original_dispatches, 1,
        "completion never replays the mutation"
    );
    assert!(state.terminal_frames > 0);
    assert!(
        matches!(observed, NetworkedDfsWriteOutcome::Committed { ref version, .. } if version == &target_version),
        "the original private native requester must adopt resident-delivered completion; got {observed:?}"
    );
    unregister_dfs_runtime_host(&fixture.data_dir);
}

#[test]
fn native_private_requester_adopts_resident_terminal_after_inline_pending() {
    run_private_requester_custody(false);
}

#[test]
fn native_private_requester_adopts_resident_terminal_before_inline_pending() {
    run_private_requester_custody(true);
}
