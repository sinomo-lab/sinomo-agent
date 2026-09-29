//! Protocol regression tests.

use super::*;

#[test]
fn detached_message_worker_finishes_every_lane_local_parent_touch() {
    let root = temp_root("detached-worker-drains-parent-touches");
    let node = NodeId::from_legacy_named("node_detached_parent_touches").expect("node id");
    let sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected send to {target_node_id} for {}",
            command.request_id
        )))
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
        node,
        Vec::new(),
        sender,
        Duration::from_millis(1),
    );
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut service = NetworkedDfsService::new(transport, content)
        .with_metadata_backend(metadata)
        .expect("attach metadata backend");
    service
        .bootstrap_distributed_sifs()
        .expect("bootstrap distributed SIFS");
    let mut parent_versions = Vec::new();
    for index in 0..(DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT + 3) {
        let parent = format!("/services/deferred-parent-{index}");
        service.posix_mkdir_all(&parent).expect("create parent");
        service
            .posix_set_times(&parent, Some(12_345), Some(67_890))
            .expect("set old parent times");
        let before = service.posix_stat(&parent).expect("parent before touch");
        let child = format!("{parent}/created.txt");
        service
            .writes
            .deferred_routed_create_parent_touches
            .insert(child);
        parent_versions.push((parent, before.version));
    }

    let worker = service.detach_message_deferred_work();

    assert!(
        service
            .writes
            .deferred_routed_create_parent_touches
            .is_empty()
    );
    assert!(!worker.has_pending().expect("detached outbox state"));
    for (parent, version_before) in parent_versions {
        let after = service.posix_stat(&parent).expect("parent after touch");
        assert_ne!(
            after.version, version_before,
            "detaching must apply the queued metadata touch for {parent}"
        );
        assert!(
            after.modified_at_unix_nanos > 67_890,
            "detaching must refresh the queued parent mtime for {parent}"
        );
    }
}

#[test]
fn exclusive_create_message_fails_closed_for_legacy_peers() {
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum LegacyDfsNetworkMessage {
        DistributedWriteRequest(DfsDistributedWriteCommand),
    }

    let requester = NodeId::from_legacy_named("node_exclusive_create_wire").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("object_exclusive_create_wire").expect("object id");
    let mut command = test_routed_write_command("exclusive_create_wire", &requester, &object_id);
    command.exclusive_create = true;

    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let bytes = codec
        .encode(&DfsNetworkMessage::ExclusiveCreateRequest(command.clone()))
        .expect("encode exclusive create message");
    assert!(
        codec.decode::<LegacyDfsNetworkMessage>(&bytes).is_err(),
        "a peer that only understands overwrite-capable distributed writes must reject exclusive create"
    );
    assert_eq!(
        codec
            .decode::<DfsNetworkMessage>(&bytes)
            .expect("decode exclusive create message"),
        DfsNetworkMessage::ExclusiveCreateRequest(command)
    );
}

#[test]
fn file_history_retained_write_requires_its_distinct_wire_variant() {
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum LegacyDfsNetworkMessage {
        DistributedWriteRequest(DfsDistributedWriteCommand),
    }
    let owner = NodeId::from_legacy_named("retained-wire-owner").unwrap();
    let object = DfsObjectId::from_legacy_named("retained-wire-object").unwrap();
    let mut command = test_routed_write_command("retained-wire-action", &owner, &object);
    command.touch_parent_on_create = false;
    command.publication = DfsWritePublication::Retained {
        capture: Box::new(DfsRetainedWritePublication {
            pin_id: "explicit-local-pin".into(),
            source: DfsFileVersionSource::LocalOperation {
                operation_id: "local-operation".into(),
                base_version: None,
            },
        }),
    };
    assert!(command.require_publication_mode(false).is_err());
    command.require_publication_mode(true).unwrap();
    let message = command.into_network_message();
    assert!(matches!(
        message,
        DfsNetworkMessage::RetainedWriteRequest(_)
    ));
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let bytes = codec.encode(&message).unwrap();
    assert!(codec.decode::<LegacyDfsNetworkMessage>(&bytes).is_err());
    assert_eq!(codec.decode::<DfsNetworkMessage>(&bytes).unwrap(), message);
}

#[test]
fn retained_networked_dfs_service_events_drop_payload_bytes() {
    let object_id = DfsObjectId::from_legacy_named("obj_retained_event_payload").expect("object");
    let version = DfsVersion::from_legacy_named("version_retained_event_payload").expect("version");
    let source_node_id =
        NodeId::from_legacy_named("node_retained_event_payload_source").expect("source");
    let requester_node_id =
        NodeId::from_legacy_named("node_retained_event_payload_requester").expect("requester");
    let events = vec![
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: "read-1".to_string(),
            object_id: object_id.clone(),
            source_node_id: source_node_id.clone(),
            result: DfsReadResult::Found(DfsReadData {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some("hash-read".to_string()),
                bytes: vec![1; 1024],
            }),
        },
        NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: "served-1".to_string(),
            object_id: object_id.clone(),
            requester_node_id,
            result: DfsReadResult::RangeFound(DfsReadRangeData {
                object_id: object_id.clone(),
                version,
                content_hash: Some("hash-served".to_string()),
                offset: 256,
                total_size_bytes: 16384,
                bytes: vec![3; 4096],
            }),
        },
    ];

    let retained = retained_networked_dfs_service_events(&events);

    match &events[0] {
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            result: DfsReadResult::Found(data),
            ..
        } => assert_eq!(data.bytes.len(), 1024),
        other => panic!("unexpected event: {other:?}"),
    }
    match &retained[0] {
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            result: DfsReadResult::Found(data),
            ..
        } => {
            assert!(data.bytes.is_empty());
            assert_eq!(data.content_hash.as_deref(), Some("hash-read"));
        }
        other => panic!("unexpected retained event: {other:?}"),
    }
    match &retained[1] {
        NetworkedDfsServiceEvent::RemoteReadServed {
            result: DfsReadResult::RangeFound(data),
            ..
        } => {
            assert!(data.bytes.is_empty());
            assert_eq!(data.total_size_bytes, 16384);
        }
        other => panic!("unexpected retained event: {other:?}"),
    }
}

#[test]
fn dfs_protocol_binary_payloads_use_native_bytes_codec() {
    let object_id = DfsObjectId::from_legacy_named("object").expect("object id");
    let version = DfsVersion::from_legacy_named("version").expect("version id");
    let node_id = NodeId::from_legacy_named("node").expect("node id");
    let file_system_id = DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("fs id");
    let bytes = protocol_test_bytes();

    assert_native_bytes_field(
        &DfsReadData {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some("hash".to_string()),
            bytes: bytes.clone(),
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsReadRangeData {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some("hash".to_string()),
            offset: 0,
            total_size_bytes: bytes.len() as u64,
            bytes: bytes.clone(),
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsWriteCommand {
            request_id: "write".to_string(),
            request: protocol_test_write_request(),
            bytes: bytes.clone(),
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsDistributedWriteCommand {
            publication: Default::default(),
            request_id: "distributed-write".to_string(),
            request: protocol_test_write_request(),
            expected_base_version: None,
            metadata_journal_after_sequence: None,
            bytes: bytes.clone(),
            content_hash: Some("hash".to_string()),
            content_size_bytes: Some(bytes.len() as u64),
            requester_blob_pre_staged: true,
            desired_replica_nodes: vec![node_id.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            object_metadata: None,
            authority_shard: None,
            touch_parent_on_create: true,
            foreground_replica_repair: true,
            exclusive_create: false,
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsStageBlobCommand {
            request_id: "stage".to_string(),
            intent_id: "intent".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: None,
            version: version.clone(),
            content_hash: "hash".to_string(),
            bytes: bytes.clone(),
            object_metadata: None,
            authority_shard: None,
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsStagePatchCommand {
            request_id: "stage-patch".to_string(),
            intent_id: "intent".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: version.clone(),
            version: version.clone(),
            content_hash: "hash".to_string(),
            offset: 0,
            patch_hash: "patch".to_string(),
            bytes: bytes.clone(),
            object_metadata: None,
            authority_shard: None,
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsRepairReplicaCommand {
            request_id: "repair".to_string(),
            intent_id: "intent".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: None,
            version: version.clone(),
            content_hash: "hash".to_string(),
            bytes: bytes.clone(),
            object_metadata: None,
            authority_shard: None,
        },
        "bytes",
    );

    let found = sinomo_core::object::serde_ext::to_value(&DfsFetchBlobResult::Found {
        content_hash: "hash".to_string(),
        bytes,
    })
    .expect("serialize fetch result");
    assert!(
        matches!(
            object_field(object_field(&found, "found"), "bytes"),
            Value::Bytes(_)
        ),
        "expected DfsFetchBlobResult::Found bytes to serialize as native bytes"
    );
}

#[test]
fn async_dfs_message_transport_in_process_sends_waits_and_drains() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let network = AsyncInProcessNetwork::new();
        let node_a = NodeId::from_legacy_named("node_dfs_async_frame_a").expect("node a");
        let node_b = NodeId::from_legacy_named("node_dfs_async_frame_b").expect("node b");
        let mut transport_a = network.add_node(node_a.clone());
        let mut transport_b = network.add_node(node_b.clone());
        let message =
            test_dfs_lookup_routed_message(&node_a, &node_b, 7, "/async/message-transport");

        AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport_a,
            &node_b,
            DfsRoutedCall::value(message),
        )
        .await
        .expect("send");
        assert!(
            AsyncDfsMessageTransport::wait_for_incoming_async(&mut transport_b, Duration::ZERO)
                .await
                .expect("wait")
        );
        let inbound = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut transport_b)
            .await
            .expect("drain");

        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].target_node_id, node_b);
        assert_eq!(inbound[0].source_node_id, node_a);
        assert_eq!(inbound[0].frame_id, 7);
    });
}

#[test]
fn served_message_transport_sends_nested_return_node_requests_synchronously() {
    let primary = NodeId::from_legacy_named("node_served_nested_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_served_nested_requester").expect("requester");
    let sent_requests = Arc::new(Mutex::new(Vec::new()));
    let sender_requests = Arc::clone(&sent_requests);
    let sender_primary = primary.clone();
    let sender_requester = requester.clone();
    let sender: DfsMessageSender = Arc::new(move |target, command, _timeout| {
        assert_eq!(target, &sender_requester);
        sender_requests
            .lock()
            .expect("sent requests")
            .push(command.request_id.clone());
        let DfsNetworkMessage::NamespaceRequest(nested) = &command.routed_message.message else {
            panic!(
                "expected nested namespace request, got {:?}",
                command.routed_message.message
            );
        };
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: vec![build_dfs_routed_message(
                &sender_requester,
                &sender_primary,
                command.routed_message.frame_id + 100,
                command.routed_message.message_id + 100,
                DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                    request_id: nested.request_id.clone(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: None,
                    posix_coordination_result: None,
                }),
            )],
        })
    });
    let mut transport = DfsNodeServedMessageTransport::new_with_message_sender(
        primary.clone(),
        vec![requester.clone()],
        sender,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let served = test_dfs_lookup_routed_message(&requester, &primary, 1, "/services/served");
    transport
        .begin_message(&served)
        .expect("begin served message");

    transport
        .send_routed_call(
            &requester,
            DfsRoutedCall::value(build_dfs_routed_message(
                &primary,
                &requester,
                2,
                2,
                DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                    request_id: "dfs_net_req_nested_return_node".to_string(),
                    file_system_id: sifs_file_system_id(),
                    requesting_node_id: primary.clone(),
                    include_metadata_journal_batches: false,
                    metadata_journal_after_sequences: Vec::new(),
                    authority_shards: Vec::new(),
                    operation: DfsNamespaceOperation::TouchDirectoryModified {
                        path: "/services".to_string(),
                    },
                }),
            )),
        )
        .expect("nested return-node request should be sent synchronously");

    assert_eq!(
        sent_requests.lock().expect("sent requests").as_slice(),
        ["dfs_message_2"],
        "nested return-node requests must be sent as calls, not returned as the original call response"
    );
    let incoming = transport
        .drain_incoming_messages()
        .expect("returned nested response");
    assert_eq!(incoming.len(), 1);
    assert!(matches!(
        incoming[0].message,
        DfsNetworkMessage::NamespaceResponse(_)
    ));
    assert!(
        transport.finish_message().is_empty(),
        "nested request responses are processed as incoming work, not returned as original RPC outbound messages"
    );
}

#[test]
fn served_message_transport_returns_response_frames_to_caller() {
    let primary = NodeId::from_legacy_named("node_served_return_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_served_return_requester").expect("requester");
    let sent_requests = Arc::new(AtomicUsize::new(0));
    let sender_count = Arc::clone(&sent_requests);
    let sender: DfsMessageSender = Arc::new(move |_target, _command, _timeout| {
        sender_count.fetch_add(1, Ordering::SeqCst);
        Ok(DfsMessageResponse {
            request_id: "unexpected".to_string(),
            outbound_messages: Vec::new(),
        })
    });
    let mut transport = DfsNodeServedMessageTransport::new_with_message_sender(
        primary.clone(),
        vec![requester.clone()],
        sender,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let served = test_dfs_lookup_routed_message(&requester, &primary, 3, "/services/served");
    transport
        .begin_message(&served)
        .expect("begin served message");

    transport
        .send_routed_call(
            &requester,
            DfsRoutedCall::value(build_dfs_routed_message(
                &primary,
                &requester,
                4,
                4,
                DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                    request_id: "dfs_net_req_returned_response".to_string(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: None,
                    posix_coordination_result: None,
                }),
            )),
        )
        .expect("response frame should return to the caller");

    assert_eq!(sent_requests.load(Ordering::SeqCst), 0);
    let outbound = transport.finish_message();
    assert_eq!(outbound.len(), 1);
    assert!(matches!(
        outbound[0].message,
        DfsNetworkMessage::NamespaceResponse(_)
    ));
}

#[test]
fn dfs_service_advertisements_use_node_identity_only() {
    let node = NodeId::from_legacy_named("node_dfs_service_advertisement_node_only").expect("node");

    let metadata = DfsNodeAdvertisement::metadata(node.clone());
    let content = DfsNodeAdvertisement::content(node.clone());

    assert_eq!(metadata.node_id, node);
    assert!(metadata.accepts_metadata);
    assert!(!metadata.accepts_content);
    assert_eq!(content.node_id, metadata.node_id);
    assert!(!content.accepts_metadata);
    assert!(content.accepts_content);
}

#[test]
fn dfs_peer_transport_adapters_delegate_all_routes() {
    let peer = Arc::new(RecordingPeerTransport::default());
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let message_sender = dfs_message_sender_from_peer_transport(Arc::clone(&peer_transport));
    let journal_sender =
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport));
    let snapshot_sender = dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport);
    let local = NodeId::from_legacy_named("node_dfs_peer_transport_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_peer_transport_remote").expect("remote");
    let routed_message = build_dfs_routed_message(
        &local,
        &remote,
        42,
        42,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-1".to_string(),
            object_id: DfsObjectId::generate(),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );

    message_sender(
        &remote,
        &DfsMessageCommand {
            request_id: "protocol-1".to_string(),
            routed_message,
        },
        DfsObjectRpcSendOptions::new(Duration::from_secs(1)),
    )
    .expect("message send");

    let shard_id = DfsMetadataShardId::new("root");
    journal_sender(
        &remote,
        &DfsMetadataJournalBatchCommand {
            request_id: "journal-1".to_string(),
            source_node_id: Some(local.clone()),
            file_system_id: None,
            authority_epoch: None,
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        },
        Duration::from_secs(1),
    )
    .expect("journal send");

    let snapshot_root = temp_root("dfs_peer_transport_snapshot");
    let snapshot = LocalSobjMetadataBackend::new(shard_id.clone(), &snapshot_root)
        .expect("metadata backend")
        .export_table_snapshot();
    snapshot_sender(
        &remote,
        &DfsMetadataSnapshotInstallCommand {
            request_id: "snapshot-1".to_string(),
            source_node_id: local,
            file_system_id: None,
            authority_epoch: None,
            primary_handoff_id: None,
            shard_id,
            next_after_sequence: 0,
            snapshot,
            reason: "test".to_string(),
        },
        Duration::from_secs(1),
    )
    .expect("snapshot send");

    assert_eq!(
        peer.calls.lock().expect("calls").as_slice(),
        &[
            format!("rpc:/dfs/data/read:{remote}:protocol-1"),
            format!("rpc:/dfs/metadata/journal-batch:{remote}:journal-1"),
            format!("rpc:/dfs/metadata/snapshot-install:{remote}:snapshot-1"),
        ]
    );
    std::fs::remove_dir_all(snapshot_root).ok();
}

#[test]
fn node_transport_sends_rpc_and_queues_returned_messages() {
    let local = NodeId::from_legacy_named("node_dfs_node_route_send_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_node_route_send_remote").expect("remote");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_sender = Arc::clone(&calls);
    let local_for_sender = local.clone();
    let remote_for_sender = remote.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        calls_for_sender
            .lock()
            .expect("calls lock")
            .push((target_node_id.clone(), command.routed_message.clone()));
        Ok(test_dfs_message_response_from_routed_messages(
            command.request_id.clone(),
            vec![test_dfs_lookup_routed_message(
                &remote_for_sender,
                &local_for_sender,
                100,
                "/node_route/send-return",
            )],
        ))
    });
    let mut transport = DfsNodeTransport::with_connect_timeout(
        local.clone(),
        vec![remote.clone()],
        Duration::from_millis(1),
        sender,
    );
    let message = test_dfs_lookup_routed_message(&local, &remote, 7, "/node_route/send");

    transport
        .send_routed_call(&remote, DfsRoutedCall::value(message.clone()))
        .expect("send message");
    let recorded = calls.lock().expect("calls lock");
    assert_eq!(recorded.as_slice(), &[(remote.clone(), message)]);
    drop(recorded);
    let incoming = transport
        .drain_incoming_messages()
        .expect("incoming messages");
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].target_node_id, local);
    assert_eq!(incoming[0].source_node_id, remote);
}

#[test]
fn node_transport_async_send_requires_async_sender() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let local =
            NodeId::from_legacy_named("node_dfs_node_route_async_send_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_node_route_async_send_remote").expect("remote");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let local_for_sender = local.clone();
        let remote_for_sender = remote.clone();
        let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
            calls_for_sender
                .lock()
                .expect("calls lock")
                .push((target_node_id.clone(), command.routed_message.clone()));
            Ok(test_dfs_message_response_from_routed_messages(
                command.request_id.clone(),
                vec![test_dfs_lookup_routed_message(
                    &remote_for_sender,
                    &local_for_sender,
                    100,
                    "/node_route/async-missing-return",
                )],
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_millis(1),
            sender,
        );
        let message =
            test_dfs_lookup_routed_message(&local, &remote, 7, "/node_route/async-missing");

        let error = AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport,
            &remote,
            DfsRoutedCall::value(message),
        )
        .await
        .expect_err("async send should require async sender");
        assert!(error.to_string().contains("without an async sender"));
        assert!(calls.lock().expect("calls lock").is_empty());
        assert_eq!(
            AsyncDfsMessageTransport::pending_incoming_count_async(&mut transport)
                .await
                .expect("pending messages"),
            Some(0)
        );
    });
}

#[test]
fn node_transport_async_sender_does_not_require_sync_sender() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let local =
            NodeId::from_legacy_named("node_dfs_node_route_async_only_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_node_route_async_only_remote").expect("remote");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_sender = Arc::clone(&calls);
        let local_for_sender = local.clone();
        let remote_for_sender = remote.clone();
        let sender: AsyncDfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
            let calls_for_sender = Arc::clone(&calls_for_sender);
            let target_node_id_for_sender = target_node_id.clone();
            let request_id = command.request_id.clone();
            let routed_message = command.routed_message.clone();
            let local_for_sender = local_for_sender.clone();
            let remote_for_sender = remote_for_sender.clone();
            Box::pin(async move {
                calls_for_sender
                    .lock()
                    .expect("calls lock")
                    .push((target_node_id_for_sender, routed_message));
                Ok(test_dfs_message_response_from_routed_messages(
                    request_id,
                    vec![test_dfs_lookup_routed_message(
                        &remote_for_sender,
                        &local_for_sender,
                        101,
                        "/node_route/async-only-return",
                    )],
                ))
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout_async(
            local.clone(),
            vec![remote.clone()],
            Duration::from_millis(1),
            sender,
        );
        let message = test_dfs_lookup_routed_message(&local, &remote, 7, "/node_route/async-only");

        assert!(
            transport
                .send_routed_call(&remote, DfsRoutedCall::value(message.clone()))
                .expect_err("sync send should be unavailable")
                .to_string()
                .contains("without a sync sender")
        );
        AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport,
            &remote,
            DfsRoutedCall::value(message.clone()),
        )
        .await
        .expect("async send message");
        {
            let recorded = calls.lock().expect("calls lock");
            assert_eq!(recorded.as_slice(), &[(remote.clone(), message)]);
        }
        let incoming = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut transport)
            .await
            .expect("incoming messages");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].target_node_id, local);
        assert_eq!(incoming[0].source_node_id, remote);
    });
}

#[test]
fn node_transport_dual_senders_support_sync_and_async_sends() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let local = NodeId::from_legacy_named("node_dfs_node_route_dual_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_node_route_dual_remote").expect("remote");
        let sync_calls = Arc::new(Mutex::new(Vec::new()));
        let async_calls = Arc::new(Mutex::new(Vec::new()));

        let sync_calls_for_sender = Arc::clone(&sync_calls);
        let local_for_sync = local.clone();
        let remote_for_sync = remote.clone();
        let sync_sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
            sync_calls_for_sender
                .lock()
                .expect("sync calls lock")
                .push((target_node_id.clone(), command.routed_message.frame_id));
            Ok(test_dfs_message_response_from_routed_messages(
                command.request_id.clone(),
                vec![test_dfs_lookup_routed_message(
                    &remote_for_sync,
                    &local_for_sync,
                    201,
                    "/node_route/dual-sync-return",
                )],
            ))
        });

        let async_calls_for_sender = Arc::clone(&async_calls);
        let local_for_async = local.clone();
        let remote_for_async = remote.clone();
        let async_sender: AsyncDfsMessageSender =
            Arc::new(move |target_node_id, command, _timeout| {
                let async_calls_for_sender = Arc::clone(&async_calls_for_sender);
                let target_node_id_for_sender = target_node_id.clone();
                let request_id = command.request_id.clone();
                let frame_id = command.routed_message.frame_id;
                let local_for_async = local_for_async.clone();
                let remote_for_async = remote_for_async.clone();
                Box::pin(async move {
                    async_calls_for_sender
                        .lock()
                        .expect("async calls lock")
                        .push((target_node_id_for_sender, frame_id));
                    Ok(test_dfs_message_response_from_routed_messages(
                        request_id,
                        vec![test_dfs_lookup_routed_message(
                            &remote_for_async,
                            &local_for_async,
                            301,
                            "/node_route/dual-async-return",
                        )],
                    ))
                })
            });

        let mut transport = DfsNodeTransport::with_connect_timeout_and_senders(
            local.clone(),
            vec![remote.clone()],
            Duration::from_millis(1),
            sync_sender,
            async_sender,
        );
        let sync_message =
            test_dfs_lookup_routed_message(&local, &remote, 11, "/node_route/dual-sync");
        transport
            .send_routed_call(&remote, DfsRoutedCall::value(sync_message))
            .expect("sync send message");
        assert_eq!(
            sync_calls.lock().expect("sync calls lock").as_slice(),
            &[(remote.clone(), 11)]
        );
        assert!(async_calls.lock().expect("async calls lock").is_empty());
        let incoming = transport
            .drain_incoming_messages()
            .expect("sync incoming messages");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].target_node_id, local);
        assert_eq!(incoming[0].source_node_id, remote);

        let async_message =
            test_dfs_lookup_routed_message(&local, &remote, 12, "/node_route/dual-async");
        AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport,
            &remote,
            DfsRoutedCall::value(async_message),
        )
        .await
        .expect("async send message");
        assert_eq!(
            async_calls.lock().expect("async calls lock").as_slice(),
            &[(remote.clone(), 12)]
        );
        let incoming = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut transport)
            .await
            .expect("async incoming messages");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].target_node_id, local);
        assert_eq!(incoming[0].source_node_id, remote);
    });
}

#[test]
fn node_transport_relays_remote_returned_messages_preserving_source() {
    let local = NodeId::from_legacy_named("node_dfs_node_route_relay_local").expect("local");
    let primary = NodeId::from_legacy_named("node_dfs_node_route_relay_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_dfs_node_route_relay_replica").expect("replica");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_sender = Arc::clone(&calls);
    let local_for_sender = local.clone();
    let primary_for_sender = primary.clone();
    let replica_for_sender = replica.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        calls_for_sender.lock().expect("calls lock").push((
            target_node_id.clone(),
            command.routed_message.source_node_id.clone(),
            command.routed_message.target_node_id.clone(),
        ));
        if command.routed_message.target_node_id == primary_for_sender {
            return Ok(test_dfs_message_response_from_routed_messages(
                command.request_id.clone(),
                vec![test_dfs_lookup_routed_message(
                    &primary_for_sender,
                    &replica_for_sender,
                    101,
                    "/node_route/relay-primary",
                )],
            ));
        }
        if command.routed_message.target_node_id == replica_for_sender {
            return Ok(test_dfs_message_response_from_routed_messages(
                command.request_id.clone(),
                vec![test_dfs_lookup_routed_message(
                    &replica_for_sender,
                    &local_for_sender,
                    102,
                    "/node_route/relay-replica",
                )],
            ));
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let mut transport = DfsNodeTransport::with_connect_timeout(
        local.clone(),
        vec![primary.clone(), replica.clone()],
        Duration::from_millis(1),
        sender,
    );
    let message = test_dfs_lookup_routed_message(&local, &primary, 9, "/node_route/relay");

    transport
        .send_routed_call(&primary, DfsRoutedCall::value(message))
        .expect("send through relay");

    assert_eq!(
        calls.lock().expect("calls lock").as_slice(),
        &[
            (primary.clone(), local.clone(), primary.clone()),
            (replica.clone(), primary.clone(), replica.clone()),
        ]
    );
    let incoming = transport
        .drain_incoming_messages()
        .expect("incoming messages");
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].target_node_id, local);
    assert_eq!(incoming[0].source_node_id, replica);
}

#[test]
fn served_message_drains_nested_stage_response_before_returning() {
    let root = temp_root("served-object-message-drains-nested-stage-response");
    let requester = NodeId::from_legacy_named("node_served_frame_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_served_frame_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_served_frame_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_served_frame_routed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_served_frame_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_served_frame_2").expect("version");
    let stage_calls = Arc::new(Mutex::new(Vec::new()));
    let promotion_calls = Arc::new(Mutex::new(Vec::new()));
    let promotion_gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let stage_calls_for_sender = Arc::clone(&stage_calls);
    let promotion_calls_for_sender = Arc::clone(&promotion_calls);
    let promotion_gate_for_sender = Arc::clone(&promotion_gate);
    let replica_for_sender = replica.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        assert_eq!(target_node_id, &replica_for_sender);
        let message = command.routed_message.message.clone();
        let stage = match message {
            DfsNetworkMessage::StageBlob(stage) => stage,
            DfsNetworkMessage::CommitVersion(commit) => {
                promotion_calls_for_sender
                    .lock()
                    .expect("promotion calls lock")
                    .push(commit.intent_id.clone());
                let (state, changed) = promotion_gate_for_sender.as_ref();
                let mut state = state.lock().expect("promotion gate");
                state.0 = true;
                changed.notify_all();
                while !state.1 {
                    state = changed.wait(state).expect("promotion release");
                }
                drop(state);
                return Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: vec![DfsRoutedMessage {
                        source_node_id: command.routed_message.target_node_id.clone(),
                        target_node_id: command.routed_message.source_node_id.clone(),
                        frame_id: command.routed_message.frame_id,
                        message_id: command.routed_message.message_id,
                        message: DfsNetworkMessage::CommitVersionResponse(
                            DfsCommitVersionResponse {
                                request_id: commit.request_id,
                                intent_id: commit.intent_id,
                                file_system_id: commit.file_system_id,
                                object_id: commit.object_id,
                                version: commit.version,
                                replica_node_id: command.routed_message.target_node_id.clone(),
                                result: DfsBlobTransferResult::Committed {
                                    content_hash: commit.content_hash,
                                },
                            },
                        ),
                    }],
                });
            }
            _ => {
                return Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: Vec::new(),
                });
            }
        };
        stage_calls_for_sender
            .lock()
            .expect("stage calls lock")
            .push(stage.request_id.clone());
        let response = DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: stage.request_id,
            intent_id: stage.intent_id,
            file_system_id: stage.file_system_id,
            object_id: stage.object_id,
            version: stage.version,
            replica_node_id: command.routed_message.target_node_id.clone(),
            result: DfsBlobTransferResult::Durable {
                content_hash: stage.content_hash,
            },
        });
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: vec![DfsRoutedMessage {
                source_node_id: command.routed_message.target_node_id.clone(),
                target_node_id: command.routed_message.source_node_id.clone(),
                frame_id: 100,
                message_id: 100,
                message: response,
            }],
        })
    });
    let transport = served_transport_with_canonical_message_peer(
        primary.clone(),
        vec![replica.clone()],
        sender,
        Duration::from_millis(1),
    );
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(transport, content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/served-frame/routed.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let request_id = "dfs_net_req_served_frame".to_string();
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: request_id.clone(),
        request: DfsWriteRequest {
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            target_version: version_2.clone(),
            requesting_node_id: requester.clone(),
        },
        expected_base_version: Some(version_1),
        metadata_journal_after_sequence: None,
        bytes: b"served routed bytes".to_vec(),
        content_hash: None,
        content_size_bytes: None,
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![primary.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        object_metadata: None,
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: true,
        exclusive_create: false,
    };
    let routed_message = build_dfs_routed_message(
        &requester,
        &primary,
        10,
        11,
        DfsNetworkMessage::DistributedWriteRequest(command),
    );

    let result = dfs
        .process_message_command(test_dfs_message_command(
            "dfs_served_message_rpc",
            routed_message,
        ))
        .expect("process DFS message");

    assert_eq!(stage_calls.lock().expect("stage calls lock").len(), 1);
    assert!(
        promotion_calls
            .lock()
            .expect("promotion calls lock")
            .is_empty(),
        "served write response must be complete before replica promotion dispatch"
    );
    assert!(
        dfs.writes.pending_writes.is_empty(),
        "W=N served write must have no pending write or repair tail before promotion"
    );
    assert!(
        dfs.coordinator.repair_jobs().is_empty(),
        "W=N served write must have no coordinator repair jobs before promotion"
    );
    let responses = result
        .response
        .outbound_messages
        .iter()
        .filter_map(|routed| {
            if routed.source_node_id != primary || routed.target_node_id != requester {
                return None;
            }
            Some(routed.message.clone())
        })
        .collect::<Vec<_>>();
    assert!(
        responses.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: response_request_id,
                    object_id: response_object_id,
                    result:
                        DfsDistributedWriteResult::Outcome {
                            outcome:
                                NetworkedDfsWriteOutcome::Pending {
                                    durable_receipts: 1,
                                    required_receipts: 2,
                                    ..
                                },
                        },
                    ..
                }) if response_request_id.as_str() == request_id.as_str()
                    && response_object_id == &object_id
            )
        }),
        "served message should return the initial pending response, got {responses:?}"
    );
    assert!(
        responses.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: response_request_id,
                    object_id: response_object_id,
                    result:
                        DfsDistributedWriteResult::Outcome {
                            outcome:
                                NetworkedDfsWriteOutcome::Committed {
                                    version,
                                    ..
                                },
                        },
                    ..
                }) if response_request_id.as_str() == request_id.as_str()
                    && response_object_id == &object_id
                    && version == &version_2
            )
        }),
        "served message must drain nested replica response and return the committed response, got {responses:?}"
    );

    let mut promotion_worker = dfs.prepare_message_deferred_work();
    let promotion_thread = std::thread::spawn(move || promotion_worker.drain());
    let (gate, changed) = promotion_gate.as_ref();
    let entered = gate.lock().expect("promotion gate");
    let (mut entered, wait) = changed
        .wait_timeout_while(entered, Duration::from_secs(1), |state| !state.0)
        .expect("promotion dispatch wait");
    let timed_out = wait.timed_out();
    let deferred_work_pending = dfs
        .message_deferred_work_pending()
        .expect("inflight promotion outbox");
    let primary_version_while_blocked = dfs
        .current_read_object_metadata(&object_id)
        .expect("primary remains usable while promotion waits")
        .version;
    entered.1 = true;
    changed.notify_all();
    drop(entered);
    promotion_thread
        .join()
        .expect("promotion worker thread")
        .expect("drain post-response replica promotion");
    assert!(!timed_out, "promotion RPC should enter its sender");
    assert!(
        !deferred_work_pending,
        "the detached worker must claim the promotion without retaining the service lane"
    );
    assert_eq!(
        primary_version_while_blocked, version_2,
        "a blocked promotion RPC must not prevent the primary service from serving later work"
    );
    assert_eq!(
        promotion_calls.lock().expect("promotion calls lock").len(),
        1,
        "the durable staged replica should be promoted exactly once after response"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_dfs_process_incoming_skips_drain_when_transport_reports_empty() {
    let node = NodeId::from_legacy_named("node_local_owned_empty_hint").expect("node");
    let drain_calls = Arc::new(Mutex::new(0));
    let transport = CountingEmptyTransport::new(node, drain_calls.clone());
    let mut dfs = LocalOwnedDfsService::new(transport);

    assert!(
        dfs.process_incoming()
            .expect("process empty incoming")
            .is_empty()
    );
    assert_eq!(
        *drain_calls.lock().expect("drain calls"),
        0,
        "exact empty inbound hints should avoid LocalOwned DFS mailbox drains"
    );
}

#[test]
fn networked_dfs_process_incoming_skips_drain_when_transport_reports_empty() {
    let root = temp_root("empty-inbound-hint");
    let node = NodeId::from_legacy_named("node_networked_empty_hint").expect("node");
    let drain_calls = Arc::new(Mutex::new(0));
    let transport = CountingEmptyTransport::new(node, drain_calls.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(transport, content);

    assert!(
        dfs.process_incoming()
            .expect("process empty incoming")
            .is_empty()
    );
    assert_eq!(
        *drain_calls.lock().expect("drain calls"),
        0,
        "exact empty inbound hints should avoid Networked DFS mailbox drains"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_store_path_index_matches_scan_semantics() {
    let owner = NodeId::from_legacy_named("node_owner").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let mut store = DfsMetadataStore::new();
    store.add_file_system(file_system(owner.clone()));

    let mut file = file_metadata(owner.clone());
    file.object_id = DfsObjectId::from_legacy_named("obj_live").expect("object");
    file.kind = DfsObjectKind::File;
    file.path = "/users/example/./hello.txt".to_string();
    store.add_object(file.clone()).expect("add file");

    // Lookup uses the normalized key (".." and "." resolved), and a path that
    // does not normalize to the stored key does not match.
    let found = store
        .object_by_normalized_posix_path(&file_system_id, "/users/example/hello.txt")
        .expect("indexed object");
    assert_eq!(found.object_id, file.object_id);
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/missing.txt")
            .is_none()
    );

    // Updating the same object id to a new path moves the index entry.
    let mut moved = file.clone();
    moved.path = "/users/example/renamed.txt".to_string();
    store.add_object(moved.clone()).expect("move file");
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/hello.txt")
            .is_none(),
        "old path must no longer resolve after a path move"
    );
    assert_eq!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .expect("moved object")
            .object_id,
        file.object_id
    );

    // A tombstone object sharing the path is skipped in favor of a live object,
    // regardless of object-id ordering.
    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id = DfsObjectId::from_legacy_named("obj_aaaa_tombstone").expect("object");
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.path = "/users/example/renamed.txt".to_string();
    store.add_object(tombstone.clone()).expect("add tombstone");
    assert_eq!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .expect("live object preferred over tombstone")
            .object_id,
        file.object_id
    );

    // Removing the live object leaves only the tombstone, so the path no longer
    // resolves to a visible object.
    store.remove_object(&file.object_id);
    assert!(
        store
            .object_by_normalized_posix_path(&file_system_id, "/users/example/renamed.txt")
            .is_none(),
        "only a tombstone remains, which must not resolve"
    );

    // Removing the tombstone empties the bucket entirely.
    store.remove_object(&tombstone.object_id);
    assert!(store.objects_by_path.is_empty());
}

#[test]
fn networked_dfs_async_send_uses_typed_object_message_path() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let root = temp_root("networked-dfs-async-send-typed-object-message");
        let network = AsyncInProcessNetwork::new();
        let local = NodeId::from_legacy_named("node_async_frame_local").expect("local");
        let remote = NodeId::from_legacy_named("node_async_frame_remote").expect("remote");
        let object_id = DfsObjectId::from_legacy_named("obj_async_frame").expect("object");
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let mut dfs = NetworkedDfsService::new(network.add_node(local.clone()), content);
        let mut remote_transport = network.add_node(remote.clone());

        let sent = dfs
            .send_dfs_message_if_routable_async(
                &remote,
                DfsNetworkMessage::ReadRequest(DfsReadRequest {
                    request_id: "async-read-1".to_string(),
                    object_id: object_id.clone(),
                    version: None,
                    content_hash: None,
                    content_size_bytes: None,
                    range_offset: None,
                    range_max_bytes: None,
                }),
            )
            .await
            .expect("async send");

        assert!(sent);
        let inbound =
            AsyncDfsMessageTransport::drain_incoming_messages_async(&mut remote_transport)
                .await
                .expect("drain remote");
        assert_eq!(inbound.len(), 1);
        let message = &inbound[0];
        assert_eq!(message.frame_id, 1);
        assert_eq!(message.source_node_id, local);
        assert_eq!(message.target_node_id, remote);
        assert_eq!(message.message_id, 1);
        match &message.message {
            DfsNetworkMessage::ReadRequest(request) => {
                assert_eq!(request.request_id, "async-read-1");
                assert_eq!(request.object_id, object_id);
            }
            other => panic!("unexpected DFS payload: {other:?}"),
        }

        std::fs::remove_dir_all(root).ok();
    });
}

#[test]
fn dfs_message_command_processor_rejects_wrong_target_node() {
    let root = temp_root("message_command_wrong_target");
    let local = NodeId::from_legacy_named("node_message_command_local").expect("local");
    let remote = NodeId::from_legacy_named("node_message_command_remote").expect("remote");
    let command = DfsMessageCommand {
        request_id: "message-command-wrong-target".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: remote.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "wrong-target-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");

    let error = process_dfs_message_command_for_test(
        local.clone(),
        command,
        content,
        Box::new(metadata),
        None,
    )
    .expect_err("wrong target rejected");

    let error = error.to_string();
    assert!(error.contains(local.as_str()), "{error}");
    assert!(error.contains(remote.as_str()), "{error}");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn installed_catalog_reconciliation_only_releases_positively_covered_reservations() {
    let root = temp_root("installed-catalog-positive-reservation-reconciliation");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let covered = content
        .put_object_version(
            "catalog-covered-object",
            "version_1",
            b"catalog-covered bytes",
        )
        .expect("put catalog-covered version");
    let uncovered = content
        .put_object_version(
            "catalog-uncovered-object",
            "version_1",
            b"catalog-uncovered bytes",
        )
        .expect("put catalog-uncovered version");
    let node_id = NodeId::from_legacy_named("node_installed_catalog_reconciliation").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content);
    let catalog_versions =
        BTreeSet::from([("catalog-covered-object".to_owned(), "version_1".to_owned())]);

    let report = dfs
        .reconcile_content_reservations_from_installed_catalog(
            &catalog_versions,
            "installed-catalog-generation",
            u64::MAX,
        )
        .expect("reconcile reservations from installed catalog");
    let reconciliation = report
        .content_reservation_reconciliation
        .expect("reservation reconciliation report");
    assert_eq!(reconciliation.reservation_count, 2);
    assert_eq!(reconciliation.examined_reservation_count, 2);
    assert_eq!(reconciliation.released_catalog_covered_count, 1);
    assert_eq!(reconciliation.released_rejected_count, 0);
    assert_eq!(reconciliation.retained_catalog_too_old_count, 1);

    let reservations = dfs
        .content
        .content_reservation_roots()
        .expect("remaining reservations");
    assert_eq!(reservations.reservation_count, 1);
    assert_eq!(
        reservations.reservations[0].identity.object_id,
        "catalog-uncovered-object"
    );
    dfs.content
        .read_manifest("catalog-covered-object", "version_1")
        .expect("positive reconciliation must not collect covered manifests");
    dfs.content
        .read_manifest("catalog-uncovered-object", "version_1")
        .expect("uncovered reservations must remain protected");
    assert!(
        dfs.content
            .has_blob(&covered.content_hash)
            .expect("covered blob remains")
    );
    assert!(
        dfs.content
            .has_blob(&uncovered.content_hash)
            .expect("uncovered blob remains")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dfs_smoke_routable_nodes_follow_peer_node_order() {
    let local = NodeId::from_legacy_named("node_dfs_smoke_routable_local").expect("local node");
    let bridge = NodeId::from_legacy_named("node_dfs_smoke_routable_bridge").expect("bridge node");
    let target = NodeId::from_legacy_named("node_dfs_smoke_routable_target").expect("target node");

    let routable = dfs_smoke_routable_node_ids(
        &local,
        &[
            local.clone(),
            target.clone(),
            bridge.clone(),
            target.clone(),
        ],
    );

    assert_eq!(
        routable,
        vec![target, bridge],
        "DFS smoke should keep peer-node selector order and de-duplicate node targets"
    );
}

#[test]
fn smoke_driver_can_use_local_only_foreground_profile() {
    let root = temp_root("smoke-driver-local-only-foreground");
    let local =
        NodeId::from_legacy_named("node_smoke_driver_local_foreground_local").expect("local");
    let replica =
        NodeId::from_legacy_named("node_smoke_driver_local_foreground_replica").expect("replica");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let send_count = Arc::new(AtomicUsize::new(0));
    let mut dfs = NetworkedDfsService::new(
        CountingFailingSendTransport {
            local_node_id: local.clone(),
            send_count: send_count.clone(),
        },
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    let mut smoke_host = FixedSmokeHost {
        pending_repair_count: 1,
    };
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let file_system = distributed_sifs(local.clone());

    let report = run_dfs_smoke_with_host(
        &mut dfs,
        &local,
        vec![local.clone(), replica.clone()],
        vec![replica.clone()],
        &mut smoke_host,
        file_system.file_system_id,
        Vec::new(),
        &mut publisher_host,
        &mut transport,
        DfsSmokeOptions {
            nonce: Some(112233),
            stable_path: true,
            remote_replica_candidates: false,
            foreground_replica_repair: false,
            inspect_pending_repairs: false,
            publish_metadata: false,
            ..DfsSmokeOptions::default()
        },
    )
    .expect("smoke");

    assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.committed_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.pending_repair_count, 0);
    assert_eq!(send_count.load(Ordering::Relaxed), 0);
    assert!(report.path.starts_with("/services/dfs-smoke/current-"));
    assert!(report.attempts[0].committed);
    assert!(publisher_host.opened_shards.is_empty());
    assert_eq!(transport.batches, 0);
    assert!(report.metadata_replication.outcomes.is_empty());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn add_object_metadata_batch_persists_visible_versions_in_one_transaction() {
    let root = temp_root("add-object-metadata-batch-visible-versions");
    let owner = NodeId::from_legacy_named("node_batch_visible_owner").expect("node");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");

    let objects = (0..3)
        .map(|index| {
            let mut object = file_metadata(owner.clone());
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_batch_visible_{index}"))
                .expect("object");
            object.path = format!("/services/batch-visible/file-{index}.txt");
            object.content_hash = Some(format!("blake3:{index:064x}"));
            object.content_size_bytes = Some(128 + index);
            object.version =
                DfsVersion::from_legacy_named(format!("version_batch_visible_{index}"))
                    .expect("version");
            object
        })
        .collect::<Vec<_>>();

    dfs.add_object_metadata_batch(objects.clone())
        .expect("metadata batch");

    let backend = dfs.metadata_backend().expect("metadata backend");
    let entries = backend.journal_since(0).expect("journal");
    assert_eq!(
        entries.len(),
        2,
        "filesystem plus one namespace transaction"
    );
    let transaction = entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => Some(transaction),
            _ => None,
        })
        .expect("namespace transaction");
    assert_eq!(transaction.objects.len(), objects.len());
    assert_eq!(transaction.versions.len(), objects.len());

    for object in &objects {
        assert_eq!(
            backend.get_object(&object.object_id).expect("object"),
            Some(object.clone())
        );
        let version = backend
            .get_version(&object.object_id, &object.version)
            .expect("version")
            .expect("visible version");
        assert_eq!(version.content_hash, object.content_hash);
        assert_eq!(version.size_bytes, object.content_size_bytes);
        assert!(version.visible);
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dfs_metadata_publish_gate_tracks_mutating_events() {
    let node = NodeId::from_legacy_named("node_dfs_publish_gate").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_publish_gate").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_publish_gate").expect("version");
    let content_hash =
        "blake3:0000000000000000000000000000000000000000000000000000000000000000".to_string();

    assert!(!dfs_events_should_publish_metadata(&[]));
    assert!(!dfs_events_should_publish_metadata(&[
        NetworkedDfsServiceEvent::BlobFetched {
            intent_id: "intent_read_only".to_string(),
            object_id: object_id.clone(),
            version: version.clone(),
            source_node_id: node.clone(),
            content_hash: content_hash.clone(),
        },
        NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
            request_id: "metadata_batch".to_string(),
            source_node_id: node.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            applied_entries: 1,
            next_after_sequence: 1,
            verified_prefix_only: false,
        },
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "remote_write_rejected".to_string(),
            object_id: object_id.clone(),
            requester_node_id: node.clone(),
            result: DfsDistributedWriteResult::Rejected {
                reason: "rejected".to_string(),
                current_primary_node_id: None,
            },
        },
        NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: "remote_write_requester_completed".to_string(),
            object_id: object_id.clone(),
            primary_node_id: node.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "remote_write_requester_intent".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
        },
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id: "namespace_lookup".to_string(),
            requester_node_id: node.clone(),
            result: DfsNamespaceResult::Applied,
            mutates_metadata: false,
        },
    ]));
    assert!(dfs_events_should_publish_metadata(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "remote_write_primary_served".to_string(),
            object_id: object_id.clone(),
            requester_node_id: node.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "remote_write_primary_intent".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
        },
    ]));
    assert!(!dfs_events_should_publish_metadata(&[
        NetworkedDfsServiceEvent::BlobStaged {
            intent_id: "intent_staged".to_string(),
            object_id: object_id.clone(),
            version: version.clone(),
            replica_node_id: node.clone(),
            content_hash: content_hash.clone(),
        },
    ]));
    assert!(!dfs_events_should_publish_metadata(&[
        NetworkedDfsServiceEvent::VersionCommitted {
            intent_id: "intent_committed".to_string(),
            object_id,
            version,
            content_hash,
        },
    ]));
    assert!(dfs_events_should_publish_metadata(&[
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id: "namespace_mutation".to_string(),
            requester_node_id: node,
            result: DfsNamespaceResult::Applied,
            mutates_metadata: true,
        },
    ]));
}

#[test]
fn dfs_metadata_publish_after_events_runs_only_for_mutating_events() {
    let node = NodeId::from_legacy_named("node_dfs_publish_after_events").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_publish_after_events").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_dfs_publish_after_events").expect("version");
    let content_hash =
        "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string();
    let mut publishes = 0usize;
    let mut reports = Vec::new();
    let mut errors = Vec::new();

    let read_only = [NetworkedDfsServiceEvent::BlobFetched {
        intent_id: "intent_read_only_after_events".to_string(),
        object_id: object_id.clone(),
        version: version.clone(),
        source_node_id: node.clone(),
        content_hash: content_hash.clone(),
    }];
    assert!(!run_dfs_metadata_publish_after_events(
        &read_only,
        || {
            publishes += 1;
            Ok(DfsMetadataJournalReplicationReport {
                outcomes: Vec::new(),
            })
        },
        |report| reports.push(report),
        |error| errors.push(error),
    ));
    assert_eq!(publishes, 0);
    assert!(reports.is_empty());
    assert!(errors.is_empty());

    let mutating = [NetworkedDfsServiceEvent::RemoteNamespaceServed {
        request_id: "namespace_committed_after_events".to_string(),
        requester_node_id: node,
        result: DfsNamespaceResult::Applied,
        mutates_metadata: true,
    }];
    assert!(run_dfs_metadata_publish_after_events(
        &mutating,
        || {
            publishes += 1;
            Ok(DfsMetadataJournalReplicationReport {
                outcomes: Vec::new(),
            })
        },
        |report| reports.push(report),
        |error| errors.push(error),
    ));
    assert_eq!(publishes, 1);
    assert_eq!(reports.len(), 1);
    assert!(errors.is_empty());
}

#[test]
fn metadata_publish_scheduler_on_runtime_retries_backlog_reports() {
    let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
    let node = NodeId::from_legacy_named("node_dfs_publish_scheduler_runtime").expect("node");
    let shard_id = DfsMetadataShardId::new("root");
    let events = [NetworkedDfsServiceEvent::RemoteNamespaceServed {
        request_id: "namespace_publish_scheduler_runtime".to_string(),
        requester_node_id: node.clone(),
        result: DfsNamespaceResult::Applied,
        mutates_metadata: true,
    }];
    let attempts = Arc::new(Mutex::new(0usize));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let attempts_for_publish = Arc::clone(&attempts);
    let node_for_publish = node.clone();
    let shard_for_publish = shard_id.clone();
    let reports_for_callback = Arc::clone(&reports);
    let errors_for_callback = Arc::clone(&errors);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");

    let start = schedule_dfs_metadata_publish_after_events_async_on_runtime(
        scheduler,
        runtime.handle().clone(),
        &events,
        DfsMetadataPublishScheduleOptions {
            max_retry_rounds: 1,
            retry_delay: Duration::from_millis(1),
            max_retry_delay: Duration::from_millis(10),
        },
        move || {
            let mut attempts = attempts_for_publish.lock().expect("attempt lock");
            *attempts += 1;
            let outcomes = if *attempts == 1 {
                vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id: node_for_publish.clone(),
                    shard_id: shard_for_publish.clone(),
                    after_sequence: 1,
                    max_batches: 1,
                }]
            } else {
                vec![DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: node_for_publish.clone(),
                    shard_id: shard_for_publish.clone(),
                    after_sequence: 1,
                }]
            };
            async move { Ok(DfsMetadataJournalReplicationReport { outcomes }) }
        },
        move |report| {
            reports_for_callback
                .lock()
                .expect("reports lock")
                .push(report);
        },
        move |error| {
            errors_for_callback.lock().expect("errors lock").push(error);
        },
    );

    assert_eq!(start, Some(DfsMetadataPublishScheduleStart::Started));
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if reports.lock().expect("reports lock").len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    assert_eq!(*attempts.lock().expect("attempt lock"), 2);
    assert!(errors.lock().expect("errors lock").is_empty());
    let reports = reports.lock().expect("reports lock");
    assert!(matches!(
        reports[0].outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::BacklogRemaining { .. }]
    ));
    assert!(matches!(
        reports[1].outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::NoChanges { .. }]
    ));
}

#[test]
fn metadata_execution_exclusions_park_offline_before_planning_healthy_target() {
    let root = temp_root("metadata-execution-exclusions");
    let local = NodeId::from_legacy_named("node_meta_exclusion_local").expect("local");
    let offline = NodeId::from_legacy_named("node_meta_exclusion_offline").expect("offline");
    let healthy = NodeId::from_legacy_named("node_meta_exclusion_healthy").expect("healthy");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(local.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("root"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let mut object = file_metadata(local.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_meta_exclusion").expect("object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/exclusion.txt".to_string();
    backend.put_object(&object).expect("object metadata");
    drop(backend);
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![offline.clone(), healthy.clone()],
        epoch: 3,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut host = RecordingMetadataJournalPublisherHost::new(root.path().to_path_buf());

    let (outcomes, targets) =
        prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
            file_system.file_system_id,
            &local,
            vec![shard],
            &mut host,
            &[],
            &DfsMaintenanceExecutionExclusions::new([offline.clone()]),
            DfsMetadataJournalReplicationOptions::default(),
        )
        .expect("prepare metadata publish with execution exclusion");

    assert!(matches!(
        outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id: outcome_shard_id,
            after_sequence: 0,
        }] if target_node_id == &offline && outcome_shard_id == &shard_id
    ));
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].target_node_id, healthy);
    assert_eq!(targets[0].after_sequence, 0);
    assert!(!targets[0].commands.is_empty());
    assert_eq!(
        host.cursor_requests,
        vec![(offline, shard_id.clone()), (healthy, shard_id)]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_node_transport_sends_one_target_attempt() {
    let target = NodeId::from_legacy_named("node_meta_node_route_fallback_target").expect("target");
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let attempted_for_sender = Arc::clone(&attempted);
    let sender: DfsMetadataJournalBatchSender =
        Arc::new(move |target_node_id, _command, _timeout| {
            attempted_for_sender
                .lock()
                .expect("attempted lock")
                .push(target_node_id.to_string());
            Err(DfsMetadataPeerTransportError::new("simulated failure"))
        });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(move |_target_node_id, command, _timeout| {
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id.clone(),
                shard_id: command.shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Installed {
                    archived_existing: false,
                },
            })
        });
    let mut transport =
        DfsMetadataNodeTransport::new(vec![target.clone()], sender, snapshot_sender);
    let command = DfsMetadataJournalBatchCommand {
        request_id: "meta-node_route-fallback".to_string(),
        source_node_id: None,
        file_system_id: None,
        authority_epoch: None,
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: Vec::new(),
            next_after_sequence: 0,
        },
    };

    let result = transport.send_metadata_journal_batch(&target, &command, Duration::from_secs(5));

    assert!(matches!(
        result,
        Err(DfsMetadataJournalReplicationTransportError::Failed { .. })
    ));
    assert_eq!(
        attempted.lock().expect("attempted lock").as_slice(),
        &[target.to_string()],
        "metadata transport should make one target-level attempt"
    );
}

#[test]
fn async_metadata_publish_after_events_obeys_metadata_event_gate() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let reports_for_publish = Arc::clone(&reports);
        let errors_for_publish = Arc::clone(&errors);
        let skipped = run_dfs_metadata_publish_after_events_async(
            &[],
            || async {
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            },
            move |report| reports_for_publish.lock().expect("reports").push(report),
            move |error| errors_for_publish.lock().expect("errors").push(error),
        )
        .await;
        assert!(!skipped);
        assert!(reports.lock().expect("reports").is_empty());
        assert!(errors.lock().expect("errors").is_empty());

        let events = vec![NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id: "namespace_async_publish_gate".to_string(),
            requester_node_id: NodeId::from_legacy_named("node_async_publish_gate").unwrap(),
            result: DfsNamespaceResult::Applied,
            mutates_metadata: true,
        }];
        let reports_for_publish = Arc::clone(&reports);
        let errors_for_publish = Arc::clone(&errors);
        let published = run_dfs_metadata_publish_after_events_async(
            &events,
            || async {
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: vec![DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id: NodeId::from_legacy_named("node_async_publish_gate_target")
                            .expect("target"),
                        shard_id: DfsMetadataShardId::new("root"),
                        after_sequence: 0,
                    }],
                })
            },
            move |report| reports_for_publish.lock().expect("reports").push(report),
            move |error| errors_for_publish.lock().expect("errors").push(error),
        )
        .await;

        assert!(published);
        assert_eq!(reports.lock().expect("reports").len(), 1);
        assert!(errors.lock().expect("errors").is_empty());
    });
}

#[test]
fn canonical_prefix_probe_persists_marker_before_promoted_lane_rebind() {
    exercise_promoted_lane_rebind(false);
}

#[test]
fn cached_promoted_lane_rebind_does_not_depend_on_its_old_replica_role() {
    exercise_promoted_lane_rebind(true);
}

fn exercise_promoted_lane_rebind(explicit_rebind: bool) {
    let root = temp_root("service-primary-handoff-marker");
    let retiring_primary =
        NodeId::from_legacy_named("node_primary_handoff_retiring").expect("retiring primary");
    let promoted =
        NodeId::from_legacy_named("node_primary_handoff_promoted").expect("promoted replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(retiring_primary.clone());
    let retiring = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: retiring_primary.clone(),
        replica_node_ids: vec![promoted.clone()],
        epoch: 17,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired = DfsMetadataShardConfig {
        primary_node_id: promoted.clone(),
        replica_node_ids: vec![retiring_primary.clone()],
        epoch: retiring.epoch + 1,
        ..retiring.clone()
    };

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let mut source_object = file_metadata(retiring_primary.clone());
    source_object.policy = DfsObjectPolicy::ReplicatedEventual;
    source.put_object(&source_object).expect("source object");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source batch")
        .expect("source entries");
    let journal_sequence = batch.next_after_sequence;
    let state_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        &source,
        &shard_id,
        journal_sequence,
    )
    .expect("source exact head");

    let receiver_data_dir = SinomoDataDir::new(root.join("receiver-data"));
    receiver_data_dir
        .ensure_root_layout()
        .expect("receiver data layout");
    let receiver = if explicit_rebind {
        LocalShardedMetadataBackend::new_with_replica_repair(
            default_dfs_metadata_shards_root(&receiver_data_dir),
            vec![retiring.clone()],
            &promoted,
        )
    } else {
        LocalShardedMetadataBackend::new(
            default_dfs_metadata_shards_root(&receiver_data_dir),
            vec![retiring.clone()],
        )
    }
    .expect("cold routed receiver backend");
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let handoff = control_plane.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_metadata_shard(&retiring)
            .expect("retiring shard config");
        let handoff = store
            .begin_metadata_shard_membership_handoff(&retiring, &desired)
            .expect("begin primary handoff")
            .expect("exact retiring config");
        store
            .acknowledge_metadata_shard_membership_handoff_fence(
                &handoff,
                &DfsMetadataShardMembershipHandoffFence {
                    handoff_id: handoff.handoff_id.clone(),
                    primary_node_id: retiring_primary.clone(),
                    journal_sequence,
                    state_fingerprint: state_fingerprint.clone(),
                },
            )
            .expect("acknowledge retiring fence")
            .expect("exact pending handoff")
    });

    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: promoted.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(receiver)
    .expect("promoted service");
    let routed_prefix_before = service
        .metadata_backend
        .as_deref()
        .expect("routed receiver backend")
        .metadata_prefix_proof(&shard_id)
        .expect("cold routed prefix")
        .expect("cold routed exact prefix");
    assert!(routed_prefix_before.journal_sequence < journal_sequence);

    let mut canonical_writer =
        open_local_metadata_shard_backend(&receiver_data_dir, shard_id.clone())
            .expect("canonical direct backend");
    canonical_writer
        .apply_journal_batch(&batch)
        .expect("canonical receiver exact catch-up");
    let durable_prefix = canonical_writer
        .metadata_prefix_proof()
        .expect("canonical durable prefix");
    assert_eq!(durable_prefix.journal_sequence, journal_sequence);
    assert_eq!(durable_prefix.digest, state_fingerprint);
    drop(canonical_writer);

    let response =
        crate::metadata_replication::process_cluster_dfs_metadata_journal_batch_with_context(
            &receiver_data_dir,
            &retiring_primary,
            DfsMetadataJournalBatchCommand {
                request_id: "primary-handoff-prefix-probe".to_string(),
                source_node_id: Some(retiring_primary.clone()),
                file_system_id: Some(retiring.file_system_id.clone()),
                authority_epoch: Some(retiring.epoch),
                verify_prefix_only: true,
                prefix_probe: Some(DfsMetadataPrefixProbe {
                    barrier_id: "primary-handoff-prefix-barrier".to_string(),
                    file_system_id: Some(file_system.file_system_id.clone()),
                    authority_epoch: Some(retiring.epoch),
                    shard_id: shard_id.clone(),
                    journal_sequence,
                    state_fingerprint: state_fingerprint.clone(),
                    primary_handoff_id: Some(handoff.handoff_id.clone()),
                }),
                authority_probe: None,
                durable_replication: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    entries: Vec::new(),
                    next_after_sequence: journal_sequence,
                },
            },
            Some(DfsMetadataReplicaShardContext {
                local_node_id: promoted.clone(),
                shard: retiring.clone(),
                pending_membership_handoff: Some(handoff.clone()),
            }),
        )
        .expect("process canonical promoted-prefix probe");
    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    assert_eq!(
        response.verified_authority_probe_id,
        Some(handoff.handoff_id.clone())
    );
    assert_eq!(
        service
            .metadata_backend
            .as_deref()
            .expect("still-cached routed backend")
            .metadata_prefix_proof(&shard_id)
            .expect("still-cached routed prefix"),
        Some(routed_prefix_before),
        "the direct route must not depend on mutating the resident routed lane"
    );
    let marker = read_local_dfs_metadata_shard_write_freeze(&receiver_data_dir, &shard_id)
        .expect("read promoted marker")
        .expect("durable marker before ACK");
    assert_eq!(marker.handoff_id, handoff.handoff_id);
    assert_eq!(marker.retiring_epoch, retiring.epoch);
    assert_eq!(marker.activated_epoch, None);
    assert_eq!(marker.journal_sequence, journal_sequence);
    assert_eq!(marker.state_fingerprint, state_fingerprint);

    control_plane.with_client(|kv| {
        assert!(
            DfsControlPlaneStore::new(kv)
                .complete_metadata_shard_membership_handoff(
                    &retiring,
                    &desired,
                    &DfsMetadataShardMembershipHandoffProof {
                        handoff_id: handoff.handoff_id.clone(),
                        retiring_shard: retiring.clone(),
                        desired_shard: desired.clone(),
                        journal_sequence,
                        state_fingerprint: state_fingerprint.clone(),
                        acknowledged_node_ids: vec![retiring_primary.clone(), promoted.clone(),],
                    },
                )
                .expect("complete primary handoff")
        );
    });
    let target_version = crate::namespace::next_version(&source_object.version);
    if explicit_rebind {
        assert!(
            service
                .rebind_completed_local_metadata_shard_membership_handoff_if_frozen(&desired)
                .expect("current-owner mutation can rebind the cached replica instance")
        );
    }
    let event = service
        .handle_distributed_write_request(
            retiring_primary.clone(),
            DfsDistributedWriteCommand {
                publication: Default::default(),
                request_id: "promoted-authority-first-routed-write".to_string(),
                request: DfsWriteRequest {
                    file_system_id: file_system.file_system_id.clone(),
                    object_id: source_object.object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: retiring_primary,
                },
                expected_base_version: Some(source_object.version.clone()),
                metadata_journal_after_sequence: None,
                bytes: b"first write through the cached promoted lane".to_vec(),
                content_hash: None,
                content_size_bytes: None,
                requester_blob_pre_staged: true,
                desired_replica_nodes: vec![promoted.clone()],
                policy: DfsReplicationPolicy::new(1, 1).expect("local write policy"),
                object_metadata: Some(source_object.clone()),
                authority_shard: Some(desired.clone()),
                touch_parent_on_create: false,
                foreground_replica_repair: false,
                exclusive_create: false,
            },
        )
        .expect("stamped routed write auto-rebinds promoted generation");
    let diagnostic = format!("{event:?}");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed { version, .. },
                },
                ..
            } if version == target_version
        ),
        "{diagnostic}"
    );
    assert_eq!(
        service
            .metadata_backend
            .as_deref()
            .expect("promoted backend")
            .metadata_shard_write_freeze(&shard_id)
            .expect("activated marker")
            .expect("durable activated marker")
            .activated_epoch,
        Some(desired.epoch)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_prefix_probe_rejects_same_head_with_different_state_and_is_legacy_safe() {
    let root = temp_root("metadata-prefix-probe-state-mismatch");
    let primary = NodeId::from_legacy_named("node_meta_probe_mismatch_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_probe_mismatch_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source backend");
    let mut receiver = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("receiver"))
        .expect("receiver backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    receiver
        .put_file_system(&file_system)
        .expect("receiver filesystem");
    let source_object = file_metadata(primary.clone());
    source.put_object(&source_object).expect("source object");
    let mut divergent_object = source_object;
    divergent_object.path = "src/divergent.rs".to_string();
    receiver
        .put_object(&divergent_object)
        .expect("divergent object");
    let journal_sequence = source.journal_last_sequence();
    assert_eq!(receiver.journal_last_sequence(), journal_sequence);
    let state_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        &source,
        &shard_id,
        journal_sequence,
    )
    .expect("source checkpoint fingerprint");
    let probe_command = DfsMetadataJournalBatchCommand {
        request_id: "request-mismatched-prefix-probe".to_string(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(file_system.file_system_id.clone()),
        authority_epoch: Some(1),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id: "barrier-mismatched-checkpoint".to_string(),
            file_system_id: Some(file_system.file_system_id.clone()),
            authority_epoch: Some(1),
            shard_id: shard_id.clone(),
            journal_sequence,
            state_fingerprint,
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            entries: Vec::new(),
            next_after_sequence: journal_sequence,
        },
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: primary,
            replica_node_ids: vec![replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    };

    let response =
        process_dfs_metadata_journal_batch(&mut receiver, Some(&context), probe_command.clone());
    assert!(matches!(
        response.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("state mismatch")
    ));
    assert!(!response.verified_prefix_only);

    let mut legacy_receiver = LocalSobjMetadataBackend::new(shard_id, root.join("legacy-receiver"))
        .expect("legacy receiver backend");
    legacy_receiver
        .put_file_system(&file_system)
        .expect("legacy receiver filesystem");
    let legacy_head = legacy_receiver.journal_last_sequence();
    assert!(legacy_head < journal_sequence);
    assert_eq!(
        legacy_receiver
            .apply_journal_batch(&probe_command.batch)
            .expect("legacy peer applies unknown probe as an empty batch"),
        0
    );
    assert_eq!(legacy_receiver.journal_last_sequence(), legacy_head);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn background_quorum_stage_refills_failed_target_without_retry_tick() {
    let root = temp_root("background-quorum-immediate-refill");
    let owner = NodeId::from_legacy_named("node_background_refill_owner").expect("owner node");
    let missing =
        NodeId::from_legacy_named("node_background_refill_missing").expect("missing node");
    let reachable =
        NodeId::from_legacy_named("node_background_refill_reachable").expect("reachable node");
    let object_id = DfsObjectId::from_legacy_named("obj_background_refill").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_background_refill_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_background_refill_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut reachable_mailbox = network.add_node(reachable.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let bytes = vec![0xa5; 2 * 1024 * 1024 + 4096];
    let outcome = owner_dfs
        .write_distributed_reader_with_base(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            Some(version_1),
            std::io::Cursor::new(bytes),
            vec![owner, missing.clone(), reachable.clone()],
            DfsReplicationPolicy::new(2, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("background W2 write with failed first target");
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("expected pending W2 write after remote stage send");
    };
    let pending = owner_dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    assert!(pending.stage_retries.contains_key(&missing));
    assert_eq!(
        pending
            .stage_inflight_request_ids
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![reachable.clone()],
        "the freed W slot should move to the next reachable candidate immediately"
    );
    assert!(
        drain_dfs_messages(&mut reachable_mailbox).iter().any(
            |message| matches!(message, DfsNetworkMessage::StageBlob(command)
                if command.intent_id == intent_id)
        ),
        "reachable fallback must receive the stage without a maintenance tick"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn malformed_dfs_object_rpc_message_does_not_block_following_valid_message() {
    let root = temp_root("malformed-dfs-object-message-does-not-block-valid-message");
    let requester = NodeId::from_legacy_named("node_bad_frame_requester").expect("node");
    let server = NodeId::from_legacy_named("node_bad_frame_server").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_bad_frame_read").expect("object");
    let bytes = b"valid message after bad message";
    let network = InProcessNetwork::new();
    let mut requester_transport = network.add_node(requester.clone());
    let mut dfs = NetworkedDfsService::new(
        network.add_node(server.clone()),
        LocalContentStore::new(root.join("server-content")).expect("server content"),
    );
    dfs.add_file_system(file_system(server.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(server.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![server.clone()];
    dfs.put_local_object_version(metadata, bytes)
        .expect("seed object");

    requester_transport
        .send_object_rpc_request(
            &server,
            ObjectRpcRequest::from_encoded_body(
                DFS_DATA_READ_ROUTE,
                DFS_RPC_CATEGORY,
                b"not a sinomo object payload".to_vec(),
            ),
        )
        .expect("send malformed object RPC message");
    let valid_message = build_dfs_routed_message(
        &requester,
        &server,
        2,
        2,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "dfs_net_req_after_bad_message".to_string(),
            object_id: object_id.clone(),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    DfsMessageTransport::send_routed_call(
        &mut requester_transport,
        &server,
        DfsRoutedCall::value(valid_message),
    )
    .expect("send valid DFS call after malformed object RPC message");

    let events = dfs
        .process_incoming()
        .expect("malformed object RPC message should be isolated");

    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::RemoteReadServed {
                request_id,
                object_id: served_object_id,
                result: DfsReadResult::Found(data),
                ..
            } if request_id == "dfs_net_req_after_bad_message"
                && served_object_id == &object_id
                && data.bytes.is_empty()
        )
    }));
    let responses = drain_dfs_messages(&mut requester_transport);
    assert!(matches!(
        responses.as_slice(),
        [DfsNetworkMessage::ReadResponse(DfsReadResponse {
            request_id,
            object_id: response_object_id,
            result: DfsReadResult::Found(data),
        })] if request_id == "dfs_net_req_after_bad_message"
            && response_object_id == &object_id
            && data.bytes.as_slice() == bytes
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn object_metadata_normalizes_duplicate_current_locations() {
    let root = temp_root("metadata-normalizes-duplicate-locations");
    let owner = NodeId::from_legacy_named("node_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_location_replica").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_duplicate_locations").expect("object");
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

    dfs.add_object_metadata(object.clone())
        .expect("object metadata");

    assert_eq!(
        dfs.metadata()
            .object(&object.object_id)
            .expect("memory object")
            .current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object.object_id)
        .expect("backend read")
        .expect("backend object");
    assert_eq!(
        backend_object.current_locations,
        vec![owner.clone(), replica.clone()]
    );
    let backend_version = dfs
        .metadata_backend()
        .expect("backend")
        .get_version(&object.object_id, &object.version)
        .expect("backend version read")
        .expect("backend version");
    assert_eq!(backend_version.locations, vec![owner, replica]);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn object_version_record_normalizes_duplicate_locations() {
    let owner = NodeId::from_legacy_named("node_version_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_version_location_replica").expect("node");
    let mut object = file_metadata(owner.clone());
    object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];

    let record = object_version_record(&object, None);

    assert_eq!(record.locations, vec![owner, replica]);
}

#[test]
fn remote_range_rejects_wrong_version() {
    assert_malformed_remote_range_rejected(
        "range_wrong_version",
        |data| {
            data.version =
                DfsVersion::from_legacy_named("range_wrong_version_other").expect("version")
        },
        "expected version",
    );
}

#[test]
fn remote_range_rejects_wrong_offset() {
    assert_malformed_remote_range_rejected(
        "range_wrong_offset",
        |data| data.offset += 1,
        "offset mismatch",
    );
}

#[test]
fn remote_range_rejects_short_length() {
    assert_malformed_remote_range_rejected(
        "range_short_length",
        |data| {
            data.bytes.pop();
        },
        "length mismatch",
    );
}

#[test]
fn remote_range_rejects_wrong_total_size() {
    assert_malformed_remote_range_rejected(
        "range_wrong_total",
        |data| data.total_size_bytes += 1,
        "does not match object metadata",
    );
}

#[test]
fn remote_range_does_not_rehash_bytes_with_matching_identity() {
    assert_malformed_remote_range_rejected(
        "range_wrong_hash",
        |data| data.content_hash = Some("blake3:wrong-range-hash".to_string()),
        "expected content hash",
    );
}

#[test]
fn stage_command_accepts_metadata_shard_primary_for_transient_object() {
    let root = temp_root("stage-command-accepts-shard-primary-transient");
    let owner = NodeId::from_legacy_named("node_stage_transient_owner").expect("owner");
    let primary = NodeId::from_legacy_named("node_stage_transient_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_stage_transient_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_transient_primary").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_transient_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_transient_2").expect("target version");
    let file_system = file_system(owner.clone());
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let network = InProcessNetwork::new();
    let _primary_mailbox = network.add_node(primary.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(replica.clone()), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut object = file_metadata(owner);
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/transient/stage.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    let bytes = b"metadata shard primary can stage a transient object";
    let content_hash = content_hash(bytes);

    let event = dfs
        .handle_stage_blob(
            primary.clone(),
            DfsStageBlobCommand {
                request_id: "dfs_stage_transient_1".to_string(),
                intent_id: "dfs_intent_stage_transient_1".to_string(),
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: content_hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(object),
                authority_shard: None,
            },
        )
        .expect("stage command");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            replica_node_id,
            ..
        } if replica_node_id == replica
    ));
    let manifest = dfs
        .content()
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("staged manifest");
    assert_eq!(manifest.content_hash, content_hash);
    assert_eq!(
        dfs.content()
            .read_blob(&manifest.content_hash)
            .expect("staged blob"),
        bytes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn visible_metadata_publication_is_idempotent() {
    let root = temp_root("visible-metadata-publication-idempotent");
    let node_id = NodeId::from_legacy_named("node_visible_metadata_idempotent").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_meta/work/site.py";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_meta/work")
        .expect("mkdir");
    let stat = dfs
        .posix_write_file(
            file_path,
            b"print('metadata')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
    let object = dfs
        .current_read_object_metadata(&stat.object_id)
        .expect("current object");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object");
    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object again");
    dfs.put_object_version_size_in_metadata_backend(&object, stat.size_bytes)
        .expect("repeat visible version size");

    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "re-publishing unchanged object/version metadata should not emit journal entries"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn explicit_dfs_roots_are_preserved() {
    let root = PathBuf::from("/tmp/sinomo-dfs-explicit-root");

    assert_eq!(
        resolve_dfs_metadata_root(Some(root.join("metadata"))).expect("metadata root"),
        root.join("metadata")
    );
    assert_eq!(
        resolve_dfs_pin_root(Some(root.join("pins"))).expect("pin root"),
        root.join("pins")
    );
}

#[test]
fn canonical_catalog_visible_without_local_payload() {
    let mut cluster = sifs_posix_cluster("sifs-canonical-catalog-remote-payload");
    let catalog_dir = "/services/catalog";
    let catalog_path = "/services/catalog/catalog.sobj";
    let catalog = br#"{"skills":["remote-only"],"revision":7}"#;
    cluster
        .primary
        .posix_mkdir_all(catalog_dir)
        .expect("catalog directory");
    sifs_cluster_primary_posix_write(&mut cluster, catalog_path, catalog);
    let expected_hash = content_hash(catalog);

    sifs_cluster_publish_metadata_until(&mut cluster, "catalog metadata catch-up", |cluster| {
        cluster
            .observer
            .posix_stat(catalog_path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
    assert!(
        !cluster
            .observer
            .content()
            .has_blob(&expected_hash)
            .expect("inspect observer content mirror"),
        "the fixture must keep catalog bytes exclusively on another online node"
    );

    let visible =
        sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, catalog_path);
    assert_eq!(visible, catalog);
    assert!(
        cluster
            .observer
            .content()
            .has_blob(&expected_hash)
            .expect("inspect fetched catalog"),
        "a canonical SIFS read should fetch the payload instead of treating the local mirror as authoritative"
    );
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn local_eventual_batch_shard_selection_lists_control_plane_once_for_bulk_paths() {
    let root = temp_root("local-eventual-batch-shard-selection-once");
    let primary = NodeId::from_legacy_named("node_apply_txn_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_apply_txn_requester").expect("requester");
    let shards = sifs_two_primary_shard_configs(
        &primary,
        &primary,
        1,
        vec![requester.clone()],
        vec![requester.clone()],
    );
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }

    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    let network = InProcessNetwork::new();
    let _requester_transport = network.add_node(requester.clone());
    let mut dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(CountingDfsControlPlaneKvClient::new(
        kv,
        Arc::clone(&range_prefix_calls),
    ))
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");

    let mut objects = Vec::new();
    let mut versions = Vec::new();
    for index in 0..64 {
        let path = format!("/services/bulk/file-{index:03}.txt");
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(format!("obj_apply_txn_{index:03}"))
                .expect("object"),
            file_system_id: sifs_file_system_id(),
            path,
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: requester.clone(),
            current_locations: vec![requester.clone()],
            content_hash: Some(format!("hash-{index:03}")),
            content_size_bytes: Some(4096),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named(format!("version_apply_txn_{index:03}"))
                .expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: Some(EventId::generate()),
        };
        versions.push(object_version_record(&object, object.content_size_bytes));
        objects.push(object);
    }
    let writes = objects
        .iter()
        .map(|object| DfsLocalEventualBatchWriteProof {
            expected_base_version: None,
            expected_parent_object_id: None,
            expected_parent_version: None,
            receipt: DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    committed_version: object.version.clone(),
                    owner_node_id: primary.clone(),
                    event_id: object
                        .last_modified_by_event_id
                        .clone()
                        .expect("write event"),
                },
                replica_node_id: requester.clone(),
                content_hash: object.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
        })
        .collect::<Vec<_>>();
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: namespace_transaction_id_for_batch(
            "visible-batch",
            &sifs_file_system_id(),
            &objects,
            &versions,
        ),
        file_system_id: sifs_file_system_id(),
        objects,
        versions,
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let committed_object_id = transaction.objects[0].object_id.clone();
    let committed_version = transaction.objects[0].version.clone();

    range_prefix_calls.store(0, Ordering::SeqCst);
    let command = DfsNamespaceCommand {
        request_id: "dfs_net_req_local_eventual_bulk".to_string(),
        file_system_id: sifs_file_system_id(),
        requesting_node_id: requester.clone(),
        include_metadata_journal_batches: false,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction,
            writes,
        },
    };
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    crate::service_metadata::reset_namespace_prepare_expectation_capture_calls();
    let event = dfs
        .handle_namespace_request(requester.clone(), command.clone())
        .expect("commit local-eventual batch namespace request");
    let table_snapshot_calls = crate::metadata_backend::metadata_current_tables_snapshot_calls();
    let expectation_capture_calls =
        crate::service_metadata::namespace_prepare_expectation_capture_calls();

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            ..
        }
    ));
    assert!(
        table_snapshot_calls <= 10,
        "64-file local-eventual validation opened {table_snapshot_calls} metadata table \
         snapshots; the atomically fused terminal receipt must not be rescanned across every shard"
    );
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "bulk local-eventual shard selection must classify paths from one shard snapshot"
    );
    assert_eq!(
        expectation_capture_calls, 1,
        "bulk local-eventual validation and prepare must share one exact expectation capture"
    );
    assert!(
        dfs.backend_receipt_for_replica(&committed_object_id, &committed_version, &requester,)
            .expect("durable batch receipt lookup")
            .is_some(),
        "visible batch metadata must have its caller receipt"
    );

    // The durable request ledger and committed transaction/receipts remain the
    // retry anchor; revalidating against the changed base would reject it.
    let replay = dfs
        .handle_namespace_request(requester, command)
        .expect("durable local-eventual batch replay");
    assert!(matches!(
        replay,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            mutates_metadata: false,
            ..
        }
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn bulk_local_eventual_splits_distinct_shards_owned_by_the_same_primary() {
    let root = temp_root("bulk-local-eventual-same-primary-distinct-shards");
    let primary = NodeId::from_legacy_named("node_bulk_same_primary").expect("primary");
    let shards = sifs_two_primary_shard_configs(&primary, &primary, 1, Vec::new(), Vec::new());
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }

    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(primary),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");
    dfs.posix_mkdir_all("/users/bulk-same-primary")
        .expect("users parent");
    dfs.posix_mkdir_all("/services/bulk-same-primary")
        .expect("services parent");

    let files = [
        (
            "/users/bulk-same-primary/users.txt",
            b"users shard bytes".as_slice(),
        ),
        (
            "/services/bulk-same-primary/services.txt",
            b"services shard bytes".as_slice(),
        ),
    ];
    let stats = dfs
        .posix_create_files_local_eventual_with_known_parents(files)
        .expect("same-primary multi-shard bulk create");

    assert_eq!(stats.len(), 2);
    for (path, bytes) in files {
        assert_eq!(
            dfs.posix_read_file(path).expect("read committed file"),
            bytes
        );
    }
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_eventual_batch_rejects_multiple_shards_with_the_same_local_primary() {
    let root = temp_root("local-eventual-batch-rejects-multiple-local-shards");
    let primary = NodeId::from_legacy_named("node_apply_txn_multi_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_apply_txn_multi_requester").expect("requester");
    let shards = sifs_two_primary_shard_configs(
        &primary,
        &primary,
        1,
        vec![requester.clone()],
        vec![requester.clone()],
    );
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }

    let network = InProcessNetwork::new();
    let _requester_transport = network.add_node(requester.clone());
    let mut dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata"),
    )
    .expect("metadata backend");

    let mut services_object = file_metadata(primary.clone());
    services_object.file_system_id = sifs_file_system_id();
    services_object.object_id =
        DfsObjectId::from_legacy_named("obj_apply_txn_multi_services").expect("services object");
    services_object.path = "/services/bulk/services.txt".to_string();
    services_object.version = DfsVersion::from_legacy_named("version_apply_txn_multi_services")
        .expect("services version");
    let mut users_object = file_metadata(primary);
    users_object.file_system_id = sifs_file_system_id();
    users_object.object_id =
        DfsObjectId::from_legacy_named("obj_apply_txn_multi_users").expect("users object");
    users_object.path = "/users/bulk/users.txt".to_string();
    users_object.version =
        DfsVersion::from_legacy_named("version_apply_txn_multi_users").expect("users version");
    let versions = vec![
        object_version_record(&services_object, services_object.content_size_bytes),
        object_version_record(&users_object, users_object.content_size_bytes),
    ];
    let transaction = DfsNamespaceTransaction {
        file_version_pins: None,
        transaction_id: "namespace-transaction-two-local-shards".to_string(),
        file_system_id: sifs_file_system_id(),
        objects: vec![services_object, users_object],
        versions,
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    let event = dfs
        .handle_namespace_request(
            requester.clone(),
            DfsNamespaceCommand {
                request_id: "dfs_net_req_apply_txn_multi_local".to_string(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: requester,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::CommitLocalEventualBatch {
                    transaction,
                    writes: Vec::new(),
                },
            },
        )
        .expect("multi-shard namespace request produces a typed rejection");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected { ref reason, .. },
            ..
        } if reason.contains("must resolve to exactly one metadata shard")
    ));
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object_by_path(&sifs_file_system_id(), "/services/bulk/services.txt")
            .expect("services lookup")
            .is_none(),
        "a rejected same-primary cross-shard transaction must not publish either half"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object_by_path(&sifs_file_system_id(), "/users/bulk/users.txt")
            .expect("users lookup")
            .is_none(),
        "a rejected same-primary cross-shard transaction must not publish either half"
    );

    std::fs::remove_dir_all(root).ok();
}
