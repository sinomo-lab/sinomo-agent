use sinomo_runtime::{
    dfs::{
        self, DfsMessageTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsRoutedCall,
        DfsRoutedMessage, DfsTransportIdentity,
    },
    network::{self, NetworkError},
    node::NodeId,
};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
pub(crate) const CLOUD_FILES_DFS_CALL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct DfsCloudFilesEndpointTransport {
    local_node_id: NodeId,
    known_remote_nodes: BTreeSet<NodeId>,
    daemon_peer: Option<Arc<dyn DfsPeerTransport>>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    operation_deadline: Option<Instant>,
}

impl std::fmt::Debug for DfsCloudFilesEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsCloudFilesEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("known_remote_nodes", &self.known_remote_nodes)
            .field("incoming_messages", &self.incoming_messages)
            .field("operation_deadline", &self.operation_deadline)
            .finish()
    }
}

impl DfsCloudFilesEndpointTransport {
    pub(crate) fn new(
        local_node_id: NodeId,
        known_remote_nodes: impl IntoIterator<Item = NodeId>,
        daemon_peer: Option<Arc<dyn DfsPeerTransport>>,
    ) -> Self {
        Self {
            known_remote_nodes: known_remote_nodes
                .into_iter()
                .filter(|node_id| node_id != &local_node_id)
                .collect(),
            local_node_id,
            daemon_peer,
            incoming_messages: VecDeque::new(),
            operation_deadline: None,
        }
    }

    fn delegates_namespace_request(&self, message: &DfsRoutedMessage) -> bool {
        self.daemon_peer.is_some()
            && matches!(
                message.message,
                dfs::DfsNetworkMessage::NamespaceRequest(_)
                    | dfs::DfsNetworkMessage::StableNamespaceReconciliationRequest(_)
                    | dfs::DfsNetworkMessage::DistributedWriteRequest(_)
                    | dfs::DfsNetworkMessage::StableWriteReconciliationRequest(_)
            )
    }

    pub(crate) fn send_timeouts(
        &self,
    ) -> std::result::Result<network::DaemonRpcTimeouts, NetworkError> {
        let mut connect_timeout =
            network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        let mut idle_timeout = network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        if let Some(deadline) = self.operation_deadline {
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
            else {
                return Err(NetworkError::Io {
                    operation: "send Cloud Files DFS message".to_string(),
                    detail: "DFS operation deadline expired before endpoint send".to_string(),
                });
            };
            connect_timeout = connect_timeout.min(remaining);
            idle_timeout = idle_timeout.min(remaining);
        }
        Ok(network::DaemonRpcTimeouts {
            connect_timeout: Some(connect_timeout),
            idle_timeout: Some(idle_timeout),
        })
    }

    pub(crate) fn send_options(
        &self,
    ) -> std::result::Result<DfsObjectRpcSendOptions, NetworkError> {
        let timeouts = self.send_timeouts()?;
        let mut options = DfsObjectRpcSendOptions::new(
            timeouts
                .connect_timeout
                .unwrap_or(network::DEFAULT_NETWORK_TIMEOUT),
        );
        if let Some(deadline) = self.operation_deadline {
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
            else {
                return Err(NetworkError::Io {
                    operation: "send Cloud Files DFS message".to_string(),
                    detail: "DFS operation deadline expired before endpoint send".to_string(),
                });
            };
            options = options.with_overall_timeout(remaining.min(CLOUD_FILES_DFS_CALL_TIMEOUT));
        }
        Ok(options)
    }

    fn deliver_returned_message(
        &mut self,
        message: DfsRoutedMessage,
    ) -> std::result::Result<(), NetworkError> {
        let target_node_id = message.target_node_id.clone();
        if target_node_id == self.local_node_id && !self.delegates_namespace_request(&message) {
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.send_routed_call_to_target(&target_node_id, DfsRoutedCall::value(message), false)
    }

    fn send_routed_call_to_target(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
    ) -> std::result::Result<(), NetworkError> {
        if target_node_id == &self.local_node_id && !self.delegates_namespace_request(&call.message)
        {
            self.incoming_messages
                .push_back(dfs::materialize_dfs_routed_call_sync(call)?);
            return Ok(());
        }
        let message = &call.message;
        if require_local_source && message.source_node_id != self.local_node_id {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: message.source_node_id.clone(),
            });
        }
        if !require_local_source
            && message.source_node_id != self.local_node_id
            && !self.known_remote_nodes.contains(&message.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: message.source_node_id.clone(),
            });
        }
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        if target_node_id != &self.local_node_id
            && !self.known_remote_nodes.contains(target_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        let (_, call) = dfs::dfs_routed_call_rpc_call(call)?;
        let peer = self.daemon_peer.as_ref().ok_or_else(|| NetworkError::Io {
            operation: "send Cloud Files DFS message".to_string(),
            detail: "clustered Cloud Files requires the local daemon DFS service path".to_string(),
        })?;
        let options = self.send_options()?;
        let response = peer
            .call_object_rpc(target_node_id, call, options)
            .map_err(|source| NetworkError::Io {
                operation: format!("send Cloud Files DFS call to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let response =
            dfs::dfs_message_response_from_rpc_result_sync(response).map_err(|source| {
                NetworkError::Io {
                    operation: format!("decode Cloud Files DFS response from {target_node_id}"),
                    detail: source.to_string(),
                }
            })?;
        for returned_message in response.outbound_messages {
            self.deliver_returned_message(returned_message)?;
        }
        Ok(())
    }
}

impl DfsTransportIdentity for DfsCloudFilesEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DfsCloudFilesEndpointTransport {
    fn delegates_local_namespace_authority(&self) -> bool {
        self.daemon_peer.is_some()
    }

    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> std::result::Result<(), NetworkError> {
        self.send_routed_call_to_target(target_node_id, call, true)
    }

    fn drain_incoming_messages(
        &mut self,
    ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming_messages.drain(..).collect())
    }

    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        let deadline =
            deadline.map(|requested| requested.min(Instant::now() + CLOUD_FILES_DFS_CALL_TIMEOUT));
        std::mem::replace(&mut self.operation_deadline, deadline)
    }

    fn operation_deadline(&self) -> Option<Instant> {
        self.operation_deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::network::{ObjectRpcCall, ObjectRpcResponse, ObjectRpcResult};
    use std::sync::Mutex;

    struct AuthorityPeer {
        objects: BTreeSet<String>,
        current: Option<dfs::DfsObjectMetadata>,
        calls: Mutex<Vec<dfs::DfsNamespaceOperation>>,
    }

    impl DfsPeerTransport for AuthorityPeer {
        fn call_object_rpc(
            &self,
            target: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, dfs::DfsPeerTransportError> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("peer runtime");
            let body = runtime
                .block_on(call.body.collect_to_vec(1024 * 1024))
                .expect("request body");
            let command: dfs::DfsMessageCommand =
                sinomo_module_api::decode_sobj(&body).expect("typed DFS request");
            let routed = command.routed_message;
            assert_eq!(&routed.target_node_id, target);
            let dfs::DfsNetworkMessage::NamespaceRequest(request) = routed.message else {
                panic!("only namespace requests may reach the authority fixture");
            };
            self.calls
                .lock()
                .expect("calls")
                .push(request.operation.clone());
            let (object, result) = match request.operation {
                dfs::DfsNamespaceOperation::LookupPath { ref path, .. } => {
                    let object = self
                        .current
                        .as_ref()
                        .filter(|object| object.path == *path)
                        .cloned()
                        .or_else(|| {
                            self.objects
                                .contains(path)
                                .then(|| dfs::directory_object(path, target.clone()))
                        });
                    (object, None)
                }
                dfs::DfsNamespaceOperation::PollNamespaceChanges { ref request } => {
                    let mut cursor = request.cursor.clone();
                    cursor.after_sequence = 42;
                    (
                        None,
                        Some(dfs::DfsPosixCoordinationResult::NamespaceChanges(
                            dfs::DfsNamespaceChangePollResponse {
                                batch: dfs::DfsNamespaceChangeBatch {
                                    next_cursor: cursor,
                                    events: Vec::new(),
                                    journal_head_sequence: 42,
                                    has_more: false,
                                },
                            },
                        )),
                    )
                }
                ref operation => panic!("unexpected namespace operation {operation:?}"),
            };
            let response = dfs::DfsMessageResponse {
                request_id: command.request_id,
                outbound_messages: vec![dfs::DfsRoutedMessage {
                    source_node_id: target.clone(),
                    target_node_id: routed.source_node_id,
                    frame_id: routed.frame_id + 1,
                    message_id: routed.message_id + 1,
                    message: dfs::DfsNetworkMessage::NamespaceResponse(dfs::DfsNamespaceResponse {
                        request_id: request.request_id,
                        result: dfs::DfsNamespaceResult::Applied,
                        synced_path_object: object,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: Some(false),
                        posix_coordination_result: result,
                    }),
                }],
            };
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::encode_body(dfs::DFS_DATA_NAMESPACE_RESPONSE_ROUTE, &response)
                    .expect("typed response"),
            ))
        }
    }

    fn primary_service(
        label: &str,
    ) -> (
        dfs::NetworkedDfsService<DfsCloudFilesEndpointTransport>,
        Arc<AuthorityPeer>,
        std::path::PathBuf,
    ) {
        let local = NodeId::from_legacy_named("cloud-files-local-primary").expect("node");
        let root = std::env::temp_dir().join(format!("cloud-files-{label}-{}", NodeId::generate()));
        let mut old = dfs::directory_object("/document.txt", local.clone());
        old.kind = dfs::DfsObjectKind::File;
        old.version = dfs::DfsVersion::from_legacy_named("old-version").expect("version");
        old.content_hash = Some("blake3:old".into());
        old.content_size_bytes = Some(3);
        let mut current = old.clone();
        current.version = dfs::DfsVersion::from_legacy_named("current-version").expect("version");
        current.content_hash = Some("blake3:current".into());
        current.content_size_bytes = Some(7);
        let peer = Arc::new(AuthorityPeer {
            objects: BTreeSet::from(["/".into()]),
            current: Some(current),
            calls: Mutex::new(Vec::new()),
        });
        let mut service = dfs::NetworkedDfsService::new(
            DfsCloudFilesEndpointTransport::new(local.clone(), Vec::new(), Some(peer.clone())),
            dfs::LocalContentStore::new(root.join("content")).expect("content"),
        );
        service
            .add_file_system(dfs::distributed_sifs(local.clone()))
            .expect("filesystem");
        service
            .add_object_metadata(dfs::directory_object("/", local.clone()))
            .expect("root");
        service
            .add_object_metadata(old)
            .expect("stale private metadata");
        let control =
            dfs::SharedDfsControlPlaneKvClient::new(sinomo_runtime::kv::InProcessKvStore::new());
        control.with_client(|kv| {
            dfs::DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&dfs::DfsMetadataShardConfig {
                    file_system_id: dfs::sifs_file_system_id(),
                    shard_id: "root".into(),
                    primary_node_id: local,
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: dfs::DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
                })
                .expect("authority shard")
        });
        service = service.with_control_plane(control);
        crate::windows_sync::configure_cloud_files_mounted_read_policy(&mut service);
        (service, peer, root)
    }

    #[test]
    fn local_primary_stat_reads_current_daemon_generation_over_private_metadata() {
        let (mut service, peer, root) = primary_service("local-stat");
        let stat = service
            .posix_lstat_distributed("/document.txt")
            .expect("authoritative stat");
        assert_eq!(
            stat.version.to_string(),
            NodeId::from_legacy_named("current-version")
                .expect("version")
                .to_string()
        );
        assert_eq!(stat.content_hash.as_deref(), Some("blake3:current"));
        assert_eq!(stat.size_bytes, Some(7));
        assert!(
            peer.calls
                .lock()
                .expect("calls")
                .iter()
                .all(|operation| matches!(
                    operation,
                    dfs::DfsNamespaceOperation::LookupPath { .. }
                ))
        );
        drop(service);
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn local_primary_feed_reads_resident_daemon_head_and_queues_only_response() {
        let (mut service, peer, root) = primary_service("local-feed");
        let cursor = dfs::DfsNamespaceChangeCursor {
            file_system_id: dfs::sifs_file_system_id(),
            shard_id: dfs::DfsMetadataShardId::new("root"),
            authority_epoch: 1,
            after_sequence: 1,
        };
        let batches = service
            .poll_namespace_changes_for_subtree(dfs::sifs_file_system_id(), "/", &[cursor])
            .expect("daemon feed");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].next_cursor.after_sequence, 42);
        assert_eq!(batches[0].journal_head_sequence, 42);
        assert!(matches!(
            peer.calls.lock().expect("calls").as_slice(),
            [dfs::DfsNamespaceOperation::PollNamespaceChanges { .. }]
        ));
        drop(service);
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
