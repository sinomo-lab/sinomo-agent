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

#[cfg(test)]
#[path = "endpoint_transport/main_query_tests.rs"]
mod main_query_tests;

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
                    | dfs::DfsNetworkMessage::RetainedWriteRequest(_)
                    | dfs::DfsNetworkMessage::ExclusiveCreateRequest(_)
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
    fn send_local_metadata_query_to_main(
        &mut self,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        if message.source_node_id != self.local_node_id
            || message.target_node_id != self.local_node_id
            || !matches!(&message.message,
                dfs::DfsNetworkMessage::NamespaceRequest(command)
                if command.requesting_node_id == self.local_node_id
                    && !command.include_metadata_journal_batches
                    && command.metadata_journal_after_sequences.is_empty()
                    && matches!(command.operation, dfs::DfsNamespaceOperation::MetadataQuery { .. }))
        {
            return Err(NetworkError::Io {
                operation: "local metadata query to Main".into(),
                detail: "only an exact local read-only metadata query may use Main dispatch".into(),
            });
        }
        if self.daemon_peer.is_none() {
            return Err(NetworkError::Io {
                operation: "local metadata query to Main".into(),
                detail: "registered Main query has no authenticated sender".into(),
            });
        }
        // Reuse the daemon peer's Main affinity, deadline and response custody.
        // Without that peer, ordinary self-dispatch would enqueue locally;
        // mounted metadata queries may never substitute that private authority.
        let local = self.local_node_id.clone();
        self.send_routed_call_to_target(&local, DfsRoutedCall::value(message), true)
    }

    fn take_namespace_response(
        &mut self,
        request_id: &str,
    ) -> Result<Option<DfsRoutedMessage>, NetworkError> {
        let position = self.incoming_messages.iter().position(|message| {
            matches!(&message.message, dfs::DfsNetworkMessage::NamespaceResponse(response)
                if response.request_id == request_id)
        });
        Ok(position.and_then(|position| self.incoming_messages.remove(position)))
    }

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
    use dfs::DfsMetadataBackend;
    use dfs::test_support::{DfsTestNamespaceReadCapture, DfsTestRuntime};

    const ROOT: &str = "/services/cloud-files-endpoint";
    const DOCUMENT: &str = "/services/cloud-files-endpoint/document.txt";

    struct PrimaryFixture {
        runtime: DfsTestRuntime,
        service: dfs::NetworkedDfsService<DfsCloudFilesEndpointTransport>,
        reads: DfsTestNamespaceReadCapture,
        local: NodeId,
        shard: dfs::DfsMetadataShardConfig,
        old: dfs::DfsObjectMetadata,
        current: dfs::DfsObjectMetadata,
        durable_head: u64,
        feed_cursors: Vec<dfs::DfsNamespaceChangeCursor>,
    }
    impl PrimaryFixture {
        fn close(self) {
            drop(self.reads);
            drop(self.service);
            self.runtime.close().expect("normal native shutdown");
        }
        fn requests(&self) -> Vec<dfs::DfsNamespaceCommand> {
            self.reads
                .records()
                .into_iter()
                .map(|message| {
                    assert_eq!(message.source_node_id, self.local);
                    assert_eq!(message.target_node_id, self.local);
                    let dfs::DfsNetworkMessage::NamespaceRequest(request) = message.message else {
                        panic!("capture retains only actual namespace read heads");
                    };
                    assert_eq!(request.requesting_node_id, self.local);
                    assert!(
                        !request.include_metadata_journal_batches,
                        "same-node delegated reads cannot ingest the owner's journal"
                    );
                    assert!(request.metadata_journal_after_sequences.is_empty());
                    request
                })
                .collect()
        }
    }

    fn primary_service(label: &str, with_private_backend: bool) -> PrimaryFixture {
        let (runtime, nodes) =
            DfsTestRuntime::start_cluster(label, 1).expect("normal Main fixture");
        let local = nodes[0].clone();
        let mut main = runtime.client(&local).expect("normal registered client");
        main.posix_mkdir_all(ROOT)
            .expect("normal directory publication");
        let configs = runtime.metadata_shards().expect("actual assignments");
        let feed_shards = dfs::dfs_namespace_change_shards_for_subtree(
            &runtime.file_system().file_system_id,
            ROOT,
            &configs,
        )
        .expect("actual subtree feed scopes");
        let initial_cursors = feed_shards
            .iter()
            .map(|config| dfs::DfsNamespaceChangeCursor::from_shard_config(config, 0))
            .collect::<Vec<_>>();
        let shard = dfs::select_metadata_shard_config_for_path(
            &runtime.file_system().file_system_id,
            &configs,
            DOCUMENT,
        )
        .expect("validated routing")
        .expect("actual owning scope");
        assert_eq!(shard.primary_node_id, local);
        let (peer, _) = runtime
            .peer_transports(&local)
            .expect("normal native peer adapter");
        let control_plane = runtime
            .control_plane_client()
            .expect("same registered native control-plane factory");
        let mut service = dfs::NetworkedDfsService::new(
            DfsCloudFilesEndpointTransport::new(local.clone(), Vec::new(), Some(peer)),
            dfs::LocalContentStore::new(runtime.data_dir(&local).dfs_dir().join("content"))
                .expect("actual current content store"),
        )
        .with_boxed_control_plane(control_plane);
        if with_private_backend {
            // Match the production Cloud Files opener. Main still owns every
            // mutation; this client's ordinary completion reads the durable
            // backend into its private metadata projection.
            let backend = dfs::open_local_sharded_metadata_backend(
                dfs::default_dfs_metadata_shards_root(&runtime.data_dir(&local)),
                configs,
            )
            .expect("normal Cloud Files metadata backend");
            service = service
                .with_boxed_metadata_backend_lazy(Box::new(backend))
                .expect("normal lazy Cloud Files backend attachment");
            service
                .bootstrap_distributed_sifs()
                .expect("delegated configuration bootstrap");
        }
        let options = dfs::DfsWriteOptions::replicated_eventual(vec![local.clone()]);
        let old_stat = if with_private_backend {
            service
                .posix_write_file(DOCUMENT, b"old", options.clone())
                .expect("ordinary Cloud Files write through Main")
        } else {
            main.posix_write_file(DOCUMENT, b"old", options.clone())
                .expect("original native Main write")
        };
        let old = runtime
            .node_metadata(&local)
            .expect("actual node metadata inspection")
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(shard.shard_id.clone()),
                &old_stat.object_id,
            )
            .expect("actual old durable object read")
            .expect("actual old durable object");
        assert_eq!(old.version, old_stat.version);
        if with_private_backend {
            assert_eq!(
                service.metadata().object(&old.object_id),
                Some(&old),
                "ordinary Cloud write completion must earn its private projection"
            );
        } else {
            assert!(service.metadata_backend().is_none());
        }
        let current_stat = main
            .posix_write_file(DOCUMENT, b"current", options)
            .expect("current native Main write");
        let current = runtime
            .node_metadata(&local)
            .expect("actual node metadata inspection")
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(shard.shard_id.clone()),
                &current_stat.object_id,
            )
            .expect("actual current durable object read")
            .expect("actual current durable object");
        assert_ne!(old.version, current.version);
        assert_eq!(current.version, current_stat.version);
        assert_eq!(
            main.posix_read_file_distributed(DOCUMENT)
                .expect("native content readback"),
            b"current"
        );
        if with_private_backend {
            assert_eq!(
                service.metadata().object(&old.object_id),
                Some(&old),
                "a separate Main write must leave the earned private view stale"
            );
        }
        let baseline = main
            .poll_namespace_changes_for_subtree(
                shard.file_system_id.clone(),
                ROOT,
                &initial_cursors,
            )
            .expect("actual resident feed");
        assert_eq!(baseline.len(), feed_shards.len());
        assert!(baseline.iter().all(|batch| !batch.rescan_required()));
        let owning_batches = baseline
            .iter()
            .filter(|batch| batch.next_cursor.shard_id.0 == shard.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(owning_batches.len(), 1, "exact owning-shard feed batch");
        let durable_head = owning_batches[0].journal_head_sequence;
        let feed_cursors = baseline
            .iter()
            .map(|batch| batch.next_cursor.clone())
            .collect::<Vec<_>>();
        assert!(durable_head > 0);
        let reads = runtime
            .capture_namespace_reads(&local, 32)
            .expect("actual read-head observer");
        drop(main);
        PrimaryFixture {
            runtime,
            service,
            reads,
            local,
            shard,
            old,
            current,
            durable_head,
            feed_cursors,
        }
    }

    #[test]
    fn local_primary_stat_reads_current_daemon_generation_over_private_metadata() {
        let mut fixture = primary_service("cloud-files-local-stat", true);
        drop(fixture.reads);
        fixture.reads = fixture
            .runtime
            .capture_namespace_requests(&fixture.local, 32)
            .expect("actual metadata query observer");
        assert_eq!(
            fixture
                .service
                .metadata()
                .object(&fixture.old.object_id)
                .unwrap()
                .version,
            fixture.old.version
        );
        let stat = fixture
            .service
            .posix_lstat_distributed(DOCUMENT)
            .expect("authoritative stat");
        assert_eq!(stat.version, fixture.current.version);
        assert_eq!(stat.content_hash, fixture.current.content_hash);
        assert_eq!(stat.size_bytes, Some(7));
        let requests = fixture.requests();
        assert!(!requests.is_empty());
        let is_requested_stat = |request: &dfs::DfsNamespaceCommand| {
            matches!(
                &request.operation,
                dfs::DfsNamespaceOperation::MetadataQuery { query }
                    if query.file_system_id == fixture.shard.file_system_id
                        && query.shard_id.0 == fixture.shard.shard_id
                        && query.epoch == fixture.shard.epoch
                        && matches!(&query.selection, dfs::DfsMetadataSelection::StatAtPathV1 { request }
                            if request.path == DOCUMENT)
            )
        };
        assert!(requests.iter().any(is_requested_stat), "{requests:?}");
        assert!(
            requests.iter().all(|request| {
                matches!(
                    &request.operation,
                    dfs::DfsNamespaceOperation::LookupPath { .. }
                ) || is_requested_stat(request)
            }),
            "{requests:?}"
        );
        fixture.close();
    }

    #[test]
    fn local_metadata_query_uses_native_main_and_rejects_wrong_identity_or_effects() {
        let PrimaryFixture {
            runtime,
            service,
            reads,
            local,
            shard,
            current,
            ..
        } = primary_service("cloud-files-local-query-contract", false);
        drop(reads);
        let capture = runtime
            .capture_namespace_requests(&local, 32)
            .expect("actual native ingress observer");
        let mut transport = service.into_transport();
        let command = dfs::DfsNamespaceCommand {
            request_id: "cloud-files-local-query-contract".into(),
            file_system_id: shard.file_system_id.clone(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: vec![shard.clone()],
            operation: dfs::DfsNamespaceOperation::MetadataQuery {
                query: dfs::DfsMetadataShardQuery {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: dfs::DfsMetadataShardId::new(shard.shard_id.clone()),
                    epoch: shard.epoch,
                    selection: dfs::DfsMetadataSelection::CurrentObject {
                        object_id: current.object_id.clone(),
                    },
                },
            },
        };
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 70001,
            message_id: 70001,
            message: dfs::DfsNetworkMessage::NamespaceRequest(command.clone()),
        };
        let foreign = NodeId::from_legacy_named("cloud-files-foreign-query").unwrap();
        for changed in [
            "source",
            "target",
            "requester",
            "journal",
            "cursor",
            "mutation",
        ] {
            let mut rejected = message.clone();
            let dfs::DfsNetworkMessage::NamespaceRequest(request) = &mut rejected.message else {
                unreachable!();
            };
            match changed {
                "source" => rejected.source_node_id = foreign.clone(),
                "target" => rejected.target_node_id = foreign.clone(),
                "requester" => request.requesting_node_id = foreign.clone(),
                "journal" => request.include_metadata_journal_batches = true,
                "cursor" => {
                    request
                        .metadata_journal_after_sequences
                        .push(dfs::DfsNamespaceShardCursor {
                            shard_id: dfs::DfsMetadataShardId::new(shard.shard_id.clone()),
                            after_sequence: 0,
                        })
                }
                "mutation" => {
                    request.operation = dfs::DfsNamespaceOperation::Mkdir {
                        path: format!("{ROOT}/must-not-exist"),
                    }
                }
                _ => unreachable!(),
            }
            assert!(
                transport
                    .send_local_metadata_query_to_main(rejected)
                    .is_err(),
                "{changed}"
            );
        }
        let mut disconnected = DfsCloudFilesEndpointTransport::new(local.clone(), Vec::new(), None);
        assert!(
            disconnected
                .send_local_metadata_query_to_main(message.clone())
                .is_err()
        );
        assert!(disconnected.incoming_messages.is_empty());
        transport.set_operation_deadline(Some(Instant::now()));
        assert!(
            transport
                .send_local_metadata_query_to_main(message.clone())
                .is_err()
        );
        assert!(
            capture.records().is_empty(),
            "rejections never dispatch to Main"
        );
        assert!(transport.incoming_messages.is_empty());
        transport.set_operation_deadline(None);

        transport
            .send_local_metadata_query_to_main(message.clone())
            .expect("actual native Main query");
        assert_eq!(capture.records(), vec![message]);
        let response = transport
            .take_namespace_response(&command.request_id)
            .unwrap()
            .expect("native response");
        assert_eq!(response.source_node_id, local);
        assert_eq!(response.target_node_id, local);
        let dfs::DfsNetworkMessage::NamespaceResponse(response) = response.message else {
            panic!("native query returned a non-response");
        };
        assert_eq!(response.request_id, command.request_id);
        assert_eq!(response.result, dfs::DfsNamespaceResult::Applied);
        let Some(dfs::DfsPosixCoordinationResult::MetadataQuery(result)) =
            response.posix_coordination_result
        else {
            panic!("native query omitted its typed response");
        };
        assert_eq!(result.file_system_id, shard.file_system_id);
        assert_eq!(result.shard_id.0, shard.shard_id);
        assert_eq!(result.epoch, shard.epoch);
        let dfs::DfsMetadataQueryResult::CurrentObject {
            object: Some(object),
        } = result.result
        else {
            panic!("native query omitted its current object");
        };
        assert_eq!(object.object_id, current.object_id);
        assert_eq!(object.version, current.version);
        assert_eq!(object.content_hash, current.content_hash);
        assert!(transport.incoming_messages.is_empty());
        drop(capture);
        drop(transport);
        runtime.close().expect("normal native shutdown");
    }

    #[test]
    fn local_primary_feed_reads_resident_daemon_head_and_queues_only_response() {
        let mut fixture = primary_service("cloud-files-local-feed", false);
        let batches = fixture
            .service
            .poll_namespace_changes_for_subtree(
                fixture.shard.file_system_id.clone(),
                ROOT,
                &fixture.feed_cursors,
            )
            .expect("actual daemon feed");
        assert_eq!(batches.len(), fixture.feed_cursors.len());
        assert!(batches.iter().all(|batch| !batch.rescan_required()));
        for cursor in &fixture.feed_cursors {
            let returned = batches
                .iter()
                .filter(|batch| batch.next_cursor.shard_id == cursor.shard_id)
                .collect::<Vec<_>>();
            assert_eq!(returned.len(), 1, "retain every actual subtree response");
            assert_eq!(
                returned[0].next_cursor.file_system_id,
                cursor.file_system_id
            );
            assert_eq!(
                returned[0].next_cursor.authority_epoch,
                cursor.authority_epoch
            );
            assert!(returned[0].journal_head_sequence >= cursor.after_sequence);
            assert_eq!(
                returned[0].next_cursor.after_sequence,
                returned[0].journal_head_sequence
            );
        }
        let owning_batches = batches
            .iter()
            .filter(|batch| batch.next_cursor.shard_id.0 == fixture.shard.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(owning_batches.len(), 1);
        let owning_head = owning_batches[0].journal_head_sequence;
        assert!(owning_head >= fixture.durable_head);
        let requests = fixture.requests();
        assert_eq!(requests.len(), fixture.feed_cursors.len());
        assert!(requests.iter().all(|request| matches!(
            &request.operation,
            dfs::DfsNamespaceOperation::PollNamespaceChanges { .. }
        )));
        let PrimaryFixture {
            runtime,
            service,
            reads,
            local,
            shard,
            ..
        } = fixture;
        let transport = service.into_transport();
        assert!(
            transport.incoming_messages.is_empty(),
            "normal routed poll consumes its sole response"
        );
        let metadata_root = runtime
            .data_dir(&local)
            .dfs_dir()
            .join("metadata")
            .join(&shard.shard_id);
        let anchor = dfs::read_metadata_candidate(&metadata_root)
            .expect("actual current snapshot anchor")
            .prefix_proof;
        let physical = dfs::inspect_metadata_prefix_descendant(
            &metadata_root,
            &dfs::DfsMetadataShardId::new(shard.shard_id),
            &anchor,
        )
        .expect("pinned physical prefix inspection")
        .expect("verified actual snapshot descendant");
        assert!(physical.journal_sequence >= owning_head);
        drop(reads);
        drop(transport);
        runtime.close().expect("normal native shutdown");
    }

    #[test]
    fn delegated_same_node_namespace_call_does_not_request_replica_replay() {
        let mut fixture = primary_service("cloud-files-local-no-replay", false);
        fixture
            .service
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                fixture.shard.file_system_id.clone(),
                fixture.local.clone(),
                dfs::DfsNamespaceOperation::LookupPath {
                    path: DOCUMENT.into(),
                    inline_range: None,
                },
                Duration::from_secs(1),
                true,
                true,
            )
            .expect("resident-primary result without client replica ingest");
        let requests = fixture.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            matches!(&requests[0].operation, dfs::DfsNamespaceOperation::LookupPath { path, .. }
            if path == DOCUMENT)
        );
        assert!(!requests[0].include_metadata_journal_batches);
        assert!(requests[0].metadata_journal_after_sequences.is_empty());
        assert!(fixture.service.metadata_backend().is_none());
        fixture.close();
    }
    #[test]
    fn same_node_endpoint_exclusive_write_uses_main_and_preserves_exact_retry() {
        let mut fixture = primary_service("cloud-files-native-exclusive", true);
        let path = format!("{ROOT}/exclusive.txt");
        let operation = "cloud-files-exclusive-operation";
        let options = dfs::DfsWriteOptions::replicated_eventual(vec![fixture.local.clone()]);
        let before_rpc = fixture.runtime.rpc_observations().len();
        let created = fixture
            .service
            .posix_create_file_with_operation_id(&path, b"old", options.clone(), operation)
            .expect("ordinary exclusive create must reach the actual Main");
        let mut main = fixture
            .runtime
            .client(&fixture.local)
            .expect("actual Main client");
        let durable = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(fixture.shard.shard_id.clone()),
                &created.object_id,
            )
            .unwrap()
            .expect("Main's actual durable exclusive object");
        assert_eq!(durable.version, created.version);
        assert_eq!(durable.content_hash, fixture.old.content_hash);
        assert_eq!(durable.content_size_bytes, Some(3));
        assert_eq!(main.posix_read_file_distributed(&path).unwrap(), b"old");
        let hash = durable.content_hash.as_deref().unwrap();
        assert_eq!(main.content().read_blob(hash).unwrap(), b"old");

        let replay = fixture
            .service
            .posix_create_file_with_operation_id(&path, b"old", options.clone(), operation)
            .expect("same original operation replays its actual Main completion");
        assert_eq!(replay.object_id, created.object_id);
        assert_eq!(replay.version, created.version);
        assert_eq!(replay.content_hash, created.content_hash);
        let conflict = fixture
            .service
            .posix_create_file_with_operation_id(
                &path,
                b"old",
                options,
                "another-exclusive-operation",
            )
            .expect_err("another operation cannot replace an existing path");
        assert!(matches!(conflict, dfs::DfsPosixError::AlreadyExists { .. }));
        let after = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(fixture.shard.shard_id.clone()),
                &created.object_id,
            )
            .unwrap()
            .unwrap();
        assert_eq!(after, durable);

        let history = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .object_write_intent_history(&created.object_id)
            .unwrap();
        let intent_id = history
            .intents
            .iter()
            .find(|record| record.operation_id.as_deref() == Some(operation))
            .map(|record| record.intent_id.clone())
            .or_else(|| {
                history
                    .receipts
                    .iter()
                    .find(|record| record.operation_id.as_deref() == Some(operation))
                    .map(|record| record.intent_id.clone())
            })
            .expect("actual Main retains this original operation");
        let intent = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_intent(&created.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert!(intent.publication.is_current());
        assert_eq!(intent.status, dfs::DfsWriteIntentStatus::Committed);
        assert_eq!(intent.file_system_id, fixture.shard.file_system_id);
        assert_eq!(intent.coordinator_node_id, fixture.local);
        assert_eq!(intent.owner_epoch, fixture.shard.epoch);
        assert_eq!(intent.target_version, created.version);
        assert_eq!(intent.content_hash, durable.content_hash);
        let observations = fixture.runtime.rpc_observations();
        let completed = observations
            .iter()
            .skip(before_rpc)
            .find(|event| {
                event.source == fixture.local
                    && event.target == fixture.local
                    && event.route == dfs::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    && event.origin == dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                    && event.phase == dfs::test_support::DfsTestRpcPhase::Completed
            })
            .expect("real same-node Main RPC completed; no local private execution");
        assert!(
            observations
                .iter()
                .any(|event| event.call_id == completed.call_id
                    && event.source == completed.source
                    && event.target == completed.target
                    && event.route == completed.route
                    && event.phase == dfs::test_support::DfsTestRpcPhase::BeforeDispatch)
        );
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        drop(main);
        fixture.close();
    }

    #[test]
    fn same_node_endpoint_retained_write_uses_main_without_replacing_current_head() {
        let mut fixture = primary_service("cloud-files-native-retained", true);
        let options = dfs::DfsWriteOptions::replicated_eventual(vec![fixture.local.clone()]);
        let request = dfs::DfsPreservedContentRequest {
            path: DOCUMENT.into(),
            file_system_id: fixture.current.file_system_id.clone(),
            object_id: fixture.current.object_id.clone(),
            publication: dfs::DfsRetainedContentPublication {
                namespace: "org.sinomo.endpoint.regression".into(),
                reference_id: "retained-endpoint-body".into(),
                metadata: None,
            },
            operation_id: "cloud-files-retained-operation".into(),
        };
        let before_rpc = fixture.runtime.rpc_observations().len();
        let dfs::DfsPreservedContentOutcome::Retained { identity } = fixture
            .service
            .preserve_content_revision(request.clone(), &b"old"[..], options.clone())
            .expect("ordinary retained reader must reach actual Main")
        else {
            panic!("actual W1 must publish the retained version");
        };
        let mut main = fixture
            .runtime
            .client(&fixture.local)
            .expect("actual Main client");
        let version = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .expect("actual Main's retained immutable version");
        assert_ne!(identity.version, fixture.current.version);
        assert_eq!(version.file_system_id, fixture.current.file_system_id);
        assert_eq!(version.object_id, fixture.current.object_id);
        assert_eq!(version.content_hash, fixture.old.content_hash);
        assert_eq!(version.size_bytes, Some(3));
        let grant = version
            .retention_grant(&request.publication.namespace)
            .expect("actual retained publication owns its namespace grant");
        assert_eq!(grant.object.file_system_id, identity.file_system_id);
        assert_eq!(grant.object.object_id, identity.object_id);
        assert_eq!(grant.object.version, identity.version);
        assert_eq!(grant.object.content_hash, version.content_hash);
        assert_eq!(grant.object.content_size_bytes, version.size_bytes);
        let hash = version.content_hash.as_deref().unwrap();
        assert_eq!(main.content().read_blob(hash).unwrap(), b"old");
        let canonical = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(fixture.shard.shard_id.clone()),
                &identity.object_id,
            )
            .unwrap()
            .unwrap();
        assert_eq!(canonical, fixture.current);
        assert_eq!(
            main.posix_read_file_distributed(DOCUMENT).unwrap(),
            b"current"
        );
        let replay = fixture
            .service
            .preserve_content_revision(request.clone(), &b"old"[..], options)
            .expect("same original retained operation replays the actual receipt");
        assert_eq!(
            replay,
            dfs::DfsPreservedContentOutcome::Retained {
                identity: identity.clone()
            }
        );
        let after = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_object_on_shard(
                &dfs::DfsMetadataShardId::new(fixture.shard.shard_id.clone()),
                &identity.object_id,
            )
            .unwrap()
            .unwrap();
        assert_eq!(after, canonical);
        let history = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        let intent_id = history
            .intents
            .iter()
            .find(|record| record.operation_id.as_deref() == Some(request.operation_id.as_str()))
            .map(|record| record.intent_id.clone())
            .or_else(|| {
                history
                    .receipts
                    .iter()
                    .find(|record| {
                        record.operation_id.as_deref() == Some(request.operation_id.as_str())
                    })
                    .map(|record| record.intent_id.clone())
            })
            .expect("actual Main retains original retained command identity");
        let intent = fixture
            .runtime
            .node_metadata(&fixture.local)
            .unwrap()
            .get_intent(&identity.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert!(!intent.publication.is_current());
        assert_eq!(intent.status, dfs::DfsWriteIntentStatus::Committed);
        assert_eq!(intent.file_system_id, identity.file_system_id);
        assert_eq!(intent.coordinator_node_id, fixture.local);
        assert_eq!(intent.owner_epoch, fixture.shard.epoch);
        assert_eq!(intent.target_version, identity.version);
        assert_eq!(intent.content_hash, version.content_hash);
        let observations = fixture.runtime.rpc_observations();
        let completed = observations
            .iter()
            .skip(before_rpc)
            .find(|event| {
                event.source == fixture.local
                    && event.target == fixture.local
                    && event.route == dfs::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    && event.origin == dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                    && event.phase == dfs::test_support::DfsTestRpcPhase::Completed
            })
            .expect("real same-node retained Main RPC completed");
        assert!(
            observations
                .iter()
                .any(|event| event.call_id == completed.call_id
                    && event.source == completed.source
                    && event.target == completed.target
                    && event.route == completed.route
                    && event.phase == dfs::test_support::DfsTestRpcPhase::BeforeDispatch)
        );
        let _observed_read_heads = fixture.requests();
        drop(main);
        fixture.close();
    }
}
