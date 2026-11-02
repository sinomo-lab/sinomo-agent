use super::*;
use dfs::test_support::DfsTestRuntime;

#[test]
fn cloud_files_root_stat_queries_its_resident_main() {
    let (runtime, nodes) =
        DfsTestRuntime::start_cluster("cloud-files-root-main-query", 1).expect("native Main");
    let local = nodes[0].clone();
    let root = "/services/cloud-files-root-main-query";
    let mut main = runtime.client(&local).expect("registered Main client");
    main.posix_mkdir_all(root).expect("actual root publication");
    let expected = main.posix_lstat_distributed(root).expect("Main root stat");
    let capture = runtime
        .capture_namespace_requests(&local, 32)
        .expect("native ingress observer");
    let (peer, _) = runtime.peer_transports(&local).expect("authenticated peer");
    let mut service = dfs::NetworkedDfsService::new(
        DfsCloudFilesEndpointTransport::new(local.clone(), [], Some(peer)),
        dfs::LocalContentStore::new(runtime.data_dir(&local).dfs_dir().join("content"))
            .expect("actual content store"),
    )
    .with_boxed_control_plane(
        runtime
            .control_plane_client()
            .expect("native control plane"),
    );

    // This is the first authoritative read in the Cloud Files full scan. The
    // mounted adapter must not treat its private projection as resident Main.
    let actual = service
        .posix_lstat_distributed(root)
        .expect("Cloud Files authoritative root stat through Main");
    assert_eq!(actual.object_id, expected.object_id);
    assert_eq!(actual.kind, expected.kind);
    let reads = capture.records();
    assert!(
        reads.iter().any(|message| matches!(
            &message.message,
            dfs::DfsNetworkMessage::NamespaceRequest(command)
                if matches!(command.operation, dfs::DfsNamespaceOperation::MetadataQuery { .. })
        )),
        "the native Main received the metadata query"
    );
    for message in &reads {
        assert_eq!(message.source_node_id, local);
        assert_eq!(message.target_node_id, local);
        let dfs::DfsNetworkMessage::NamespaceRequest(command) = &message.message else {
            panic!("read capture returned another message family");
        };
        assert_eq!(command.requesting_node_id, local);
        assert!(!command.include_metadata_journal_batches);
        assert!(command.metadata_journal_after_sequences.is_empty());
    }
    assert_eq!(runtime.observation_dropped_count(), 0);
    let mut transport = service.into_transport();
    assert!(transport.incoming_messages.is_empty());
    let query = reads
        .into_iter()
        .find(|message| {
            matches!(&message.message,
            dfs::DfsNetworkMessage::NamespaceRequest(command)
                if matches!(command.operation, dfs::DfsNamespaceOperation::MetadataQuery { .. }))
        })
        .expect("actual successful query, with native authority stamps");
    for (index, fault) in ["stale-epoch", "foreign-filesystem"]
        .into_iter()
        .enumerate()
    {
        let mut message = query.clone();
        message.frame_id += 100 + index as u64;
        message.message_id += 100 + index as u64;
        let dfs::DfsNetworkMessage::NamespaceRequest(command) = &mut message.message else {
            unreachable!();
        };
        command.request_id = format!("cloud-files-main-query-{fault}");
        let request_id = command.request_id.clone();
        let dfs::DfsNamespaceOperation::MetadataQuery { query } = &mut command.operation else {
            unreachable!();
        };
        if fault == "stale-epoch" {
            query.epoch += 1;
            for shard in &mut command.authority_shards {
                shard.epoch += 1;
            }
        } else {
            let foreign = dfs::DfsFileSystemId::from_legacy_named("cloud-files-foreign-fs")
                .expect("foreign ID");
            query.file_system_id = foreign.clone();
            command.file_system_id = foreign.clone();
            for shard in &mut command.authority_shards {
                shard.file_system_id = foreign.clone();
            }
        }
        transport
            .send_local_metadata_query_to_main(message)
            .expect("Main returns the normal authority rejection");
        let response = transport
            .take_namespace_response(&request_id)
            .expect("response custody")
            .expect("exact negative response");
        assert_eq!(response.source_node_id, local);
        assert_eq!(response.target_node_id, local);
        let dfs::DfsNetworkMessage::NamespaceResponse(response) = response.message else {
            panic!("unexpected response family");
        };
        assert!(
            matches!(response.result, dfs::DfsNamespaceResult::Rejected { .. }),
            "{fault} must not become a successful local read"
        );
        assert!(transport.incoming_messages.is_empty());
    }
    assert_eq!(
        main.posix_lstat_distributed(root).unwrap().object_id,
        expected.object_id
    );
    drop(transport);
    drop(capture);
    drop(main);
    runtime.close().expect("normal native shutdown");
}

#[derive(Default)]
struct RejectingPeer {
    calls: std::sync::Mutex<Vec<(NodeId, DfsObjectRpcSendOptions)>>,
}

impl DfsPeerTransport for RejectingPeer {
    fn call_object_rpc(
        &self,
        target: &NodeId,
        _call: network::ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<network::ObjectRpcResult, dfs::DfsPeerTransportError> {
        self.calls.lock().unwrap().push((target.clone(), options));
        Err(dfs::DfsPeerTransportError::new("sentinel peer refusal"))
    }
}

fn guard_query(local: &NodeId) -> DfsRoutedMessage {
    // These synthetic IDs exercise only rejection before dispatch. Native
    // positive and authority controls above use Main's real recorded query.
    DfsRoutedMessage {
        source_node_id: local.clone(),
        target_node_id: local.clone(),
        frame_id: 7,
        message_id: 7,
        message: dfs::DfsNetworkMessage::NamespaceRequest(dfs::DfsNamespaceCommand {
            request_id: "guard-query".into(),
            file_system_id: dfs::sifs_file_system_id(),
            requesting_node_id: local.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: dfs::DfsNamespaceOperation::MetadataQuery {
                query: dfs::DfsMetadataShardQuery {
                    file_system_id: dfs::sifs_file_system_id(),
                    shard_id: dfs::DfsMetadataShardId::new("guard-shard"),
                    epoch: 1,
                    selection: dfs::DfsMetadataSelection::DirectoryPage {
                        path: "/".into(),
                        after: None,
                        limit: 1,
                    },
                },
            },
        }),
    }
}

#[test]
fn local_main_query_rejects_other_callers_targets_writes_and_replay() {
    let local = NodeId::from_legacy_named("cloud-files-guard-local").unwrap();
    let foreign = NodeId::from_legacy_named("cloud-files-guard-foreign").unwrap();
    let peer = Arc::new(RejectingPeer::default());
    let mut transport =
        DfsCloudFilesEndpointTransport::new(local.clone(), [foreign.clone()], Some(peer.clone()));
    for fault in [
        "source",
        "target",
        "requester",
        "write",
        "journal",
        "cursor",
    ] {
        let mut message = guard_query(&local);
        let dfs::DfsNetworkMessage::NamespaceRequest(command) = &mut message.message else {
            unreachable!();
        };
        match fault {
            "source" => message.source_node_id = foreign.clone(),
            "target" => message.target_node_id = foreign.clone(),
            "requester" => command.requesting_node_id = foreign.clone(),
            "write" => {
                command.operation = dfs::DfsNamespaceOperation::Mkdir {
                    path: "/forbidden".into(),
                }
            }
            "journal" => command.include_metadata_journal_batches = true,
            "cursor" => {
                command
                    .metadata_journal_after_sequences
                    .push(dfs::DfsNamespaceShardCursor {
                        shard_id: dfs::DfsMetadataShardId::new("guard-shard"),
                        after_sequence: 1,
                    })
            }
            _ => unreachable!(),
        }
        let error = transport
            .send_local_metadata_query_to_main(message)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("only an exact local read-only metadata query"),
            "{fault}: {error}"
        );
        assert!(peer.calls.lock().unwrap().is_empty());
        assert!(transport.incoming_messages.is_empty());
    }
}

#[test]
fn local_main_query_fails_closed_without_peer_or_after_deadline() {
    let local = NodeId::from_legacy_named("cloud-files-guard-unavailable").unwrap();
    let mut no_peer = DfsCloudFilesEndpointTransport::new(local.clone(), [], None);
    let error = no_peer
        .send_local_metadata_query_to_main(guard_query(&local))
        .unwrap_err();
    assert!(error.to_string().contains("no authenticated sender"));
    assert!(
        no_peer.incoming_messages.is_empty(),
        "never enqueue into private authority"
    );

    let peer = Arc::new(RejectingPeer::default());
    let mut expired = DfsCloudFilesEndpointTransport::new(local.clone(), [], Some(peer.clone()));
    expired.set_operation_deadline(Some(Instant::now() - Duration::from_secs(1)));
    let error = expired
        .send_local_metadata_query_to_main(guard_query(&local))
        .unwrap_err();
    assert!(error.to_string().contains("deadline expired"));
    assert!(peer.calls.lock().unwrap().is_empty());
    assert!(expired.incoming_messages.is_empty());
}

#[test]
fn local_main_query_preserves_deadline_and_does_not_retry_peer_failure() {
    let local = NodeId::from_legacy_named("cloud-files-guard-delivery").unwrap();
    let peer = Arc::new(RejectingPeer::default());
    let mut transport = DfsCloudFilesEndpointTransport::new(local.clone(), [], Some(peer.clone()));
    let deadline = Instant::now() + Duration::from_secs(2);
    transport.set_operation_deadline(Some(deadline));
    let error = transport
        .send_local_metadata_query_to_main(guard_query(&local))
        .unwrap_err();
    assert!(error.to_string().contains("sentinel peer refusal"));
    let calls = peer.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "the adapter cannot replay an uncertain caller operation"
    );
    assert_eq!(calls[0].0, local);
    assert!(
        calls[0]
            .1
            .overall_timeout
            .is_some_and(|budget| budget <= Duration::from_secs(2))
    );
    assert_eq!(transport.operation_deadline(), Some(deadline));
    assert!(transport.incoming_messages.is_empty());
}
