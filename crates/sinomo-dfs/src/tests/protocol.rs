//! Protocol regression tests.

use super::*;

#[test]
fn repair_background_hint_is_required_and_explicit_priorities_round_trip() {
    let mut command = DfsRepairReplicaCommand {
        background_repair: false,
        request_id: "repair-priority-wire".into(),
        intent_id: "repair-priority-intent".into(),
        file_system_id: sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("repair-priority-object").unwrap(),
        expected_base_version: None,
        version: DfsVersion::from_legacy_named("repair-priority-version").unwrap(),
        content_hash: "blake3:repair-priority".into(),
        bytes: vec![1, 2, 3],
        object_metadata: None,
        authority_shard: None,
        receipt_generation: 0,
    };
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let foreground = serde_json::to_value(&command).unwrap();
    assert_eq!(
        foreground.get("background_repair"),
        Some(&serde_json::Value::Bool(false))
    );
    let encoded = codec.encode(&command).unwrap();
    assert_eq!(
        codec.decode::<DfsRepairReplicaCommand>(&encoded).unwrap(),
        command
    );

    command.background_repair = true;
    let mut missing_hint = serde_json::to_value(&command).unwrap();
    assert_eq!(
        missing_hint.get("background_repair"),
        Some(&serde_json::Value::Bool(true))
    );
    let encoded = codec.encode(&command).unwrap();
    assert_eq!(
        codec.decode::<DfsRepairReplicaCommand>(&encoded).unwrap(),
        command
    );

    missing_hint
        .as_object_mut()
        .unwrap()
        .remove("background_repair");
    assert!(serde_json::from_value::<DfsRepairReplicaCommand>(missing_hint).is_err());
}

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
    let transport = DfsNodeTransport::new_with_message_sender(
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
fn exclusive_create_current_wire_round_trips_without_changing_publication_mode() {
    let requester = NodeId::from_legacy_named("node_exclusive_create_wire").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("object_exclusive_create_wire").expect("object id");
    let mut command = test_routed_write_command("exclusive_create_wire", &requester, &object_id);
    command.exclusive_create = true;

    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let bytes = codec
        .encode(&DfsNetworkMessage::ExclusiveCreateRequest(command.clone()))
        .expect("encode exclusive create message");

    assert_eq!(
        codec
            .decode::<DfsNetworkMessage>(&bytes)
            .expect("decode exclusive create message"),
        DfsNetworkMessage::ExclusiveCreateRequest(command)
    );
}

#[test]
fn file_history_retained_write_requires_its_distinct_wire_variant() {
    let owner = NodeId::from_legacy_named("retained-wire-owner").unwrap();
    let object = DfsObjectId::from_legacy_named("retained-wire-object").unwrap();
    let mut command = test_routed_write_command("retained-wire-action", &owner, &object);
    command.touch_parent_on_create = false;
    command.publication = DfsWritePublication::Retained {
        capture: Box::new(DfsRetainedWritePublication {
            namespace: sinomo_api::dfs::file_history::DFS_HISTORY_METADATA_NAMESPACE.into(),
            reference_id: "explicit-local-pin".into(),
            metadata: Some(DfsModuleMetadataValue {
                schema_version: 1,
                payload: serde_json::to_value(DfsFileVersionSource::LocalOperation {
                    operation_id: "local-operation".into(),
                    base_version: None,
                })
                .unwrap(),
            }),
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
            patch: None,
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
            receipt_generation: 0,
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
            receipt_generation: 0,
        },
        "bytes",
    );
    assert_native_bytes_field(
        &DfsRepairReplicaCommand {
            background_repair: false,
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
            receipt_generation: 0,
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
fn async_dfs_message_transport_sends_waits_and_drains_native_reply() {
    let name = "native-async-routed-message-transport";
    let (runtime, mut primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    primary
        .posix_mkdir("/services/async-message-transport")
        .unwrap();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let requester = nodes.into_iter().find(|node| *node != owner).unwrap();
    let mut transport = runtime.client(&requester).unwrap().into_transport();
    let message =
        test_dfs_lookup_routed_message(&requester, &owner, 7, "/services/async-message-transport");
    runtime.runtime_handle().block_on(async {
        AsyncDfsMessageTransport::send_routed_call_async(
            &mut transport,
            &owner,
            DfsRoutedCall::value(message),
        )
        .await
        .unwrap();
        assert!(
            AsyncDfsMessageTransport::wait_for_incoming_async(&mut transport, Duration::ZERO)
                .await
                .unwrap()
        );
        let inbound = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut transport)
            .await
            .unwrap();
        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].target_node_id, requester);
        assert_eq!(inbound[0].source_node_id, owner);
        assert!(matches!(
            inbound[0].message,
            DfsNetworkMessage::NamespaceResponse(_)
        ));
    });
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == requester
                && event.target == owner
                && event.route == crate::DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    drop(transport);
    drop(primary);
    runtime.close().unwrap();
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
                    committed_work: Default::default(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: false,
                    posix_coordination_result: None,
                }),
            )],
        })
    });
    let mut transport = DfsNodeTransport::new_with_message_sender(
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
    let mut transport = DfsNodeTransport::new_with_message_sender(
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
                    committed_work: Default::default(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: false,
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
    let name = "native-served-nested-stage-response";
    let (runtime, browser, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(browser);
    let primary = shard.primary_node_id.clone();
    let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let replica = peers[0].clone();
    let requester = peers[1].clone();
    let path = "/services/served-frame-routed.txt";
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap();
    placement.required_node_ids = vec![primary.clone(), replica.clone()];
    let accepted = runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.scanned_object_count, 0);
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal empty Apply setup deadline");
        runtime
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    let (_, _, policy) = dfs
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    let base = dfs
        .posix_write_file(
            path,
            b"actual sealed predecessor",
            DfsWriteOptions {
                policy: policy.clone(),
                replica_candidates: vec![primary.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    dfs.flush_detached_transport_sends().unwrap();
    dfs.process_incoming().unwrap();
    let original = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&base.object_id)
        .unwrap()
        .unwrap();
    let object_id = base.object_id;
    let version_1 = base.version;
    let version_2 = crate::namespace::next_version(&version_1);
    let requester_service = runtime.client(&requester).unwrap();
    requester_service
        .content()
        .put_object_version(
            object_id.as_str(),
            version_2.as_str(),
            b"served routed bytes",
        )
        .unwrap();
    drop(requester_service);
    let (peer, async_peer) = runtime.peer_transports(&primary).unwrap();
    dfs.transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
        primary.clone(),
        peers,
        peer,
        async_peer,
        Duration::from_millis(1),
    );
    let promotion_gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(&promotion_gate)),
    });
    let observed_before = runtime.rpc_observations().len();
    let request_id = "dfs_net_req_served_frame".to_string();
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        patch: None,
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
        policy,
        object_metadata: Some(original),
        authority_shard: Some(shard),
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

    let calls = runtime.rpc_observations()[observed_before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.source == primary
                && call.target == replica
                && call.route == crate::DFS_DATA_STAGE_BLOB_ROUTE
                && call.phase == crate::test_runtime::RpcPhase::Completed)
            .count(),
        1,
        "one actual nested Stage completes: {:?}",
        result.events
    );
    assert!(
        !calls.iter().any(|call| call.source == primary
            && call.target == replica
            && call.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch),
        "served response completes before promotion dispatch"
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
    let promotion_deadline = Instant::now() + Duration::from_secs(1);
    let entered = loop {
        if runtime.rpc_observations()[observed_before..]
            .iter()
            .any(|call| {
                call.source == primary
                    && call.target == replica
                    && call.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                    && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            })
        {
            break true;
        }
        if Instant::now() >= promotion_deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let deferred_work_pending = dfs
        .message_deferred_work_pending()
        .expect("inflight promotion outbox");
    let primary_version_while_blocked = dfs
        .current_read_object_metadata(&object_id)
        .expect("primary remains usable while promotion waits")
        .version;
    runtime.clear_faults();
    promotion_gate.add_permits(1);
    promotion_thread
        .join()
        .expect("promotion worker thread")
        .expect("drain post-response replica promotion");
    assert!(entered, "promotion RPC should enter its actual sender");
    assert!(
        !deferred_work_pending,
        "the detached worker must claim the promotion without retaining the service lane"
    );
    assert_eq!(
        primary_version_while_blocked, version_2,
        "a blocked promotion RPC must not prevent the primary service from serving later work"
    );
    assert_eq!(
        runtime.rpc_observations()[observed_before..]
            .iter()
            .filter(|call| call.source == primary
                && call.target == replica
                && call.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && call.phase == crate::test_runtime::RpcPhase::Completed)
            .count(),
        1,
        "the durable staged replica should be promoted exactly once after response"
    );

    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn networked_dfs_process_incoming_skips_drain_when_transport_reports_empty() {
    let root = temp_root("empty-inbound-hint");
    let node = NodeId::from_legacy_named("node_networked_empty_hint").expect("node");
    let drain_calls = Arc::new(Mutex::new(0));
    let transport = crate::test_runtime::DfsTestRuntime::local_transport_with_drain_counter(
        node,
        drain_calls.clone(),
    );
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
    let name = "native-async-typed-object-message";
    let (runtime, mut primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    let stat = primary
        .posix_write_file(
            "/services/async-typed-read",
            b"actual source bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let requester = nodes.into_iter().find(|node| *node != owner).unwrap();
    let mut client = runtime.client(&requester).unwrap();
    runtime.runtime_handle().block_on(async {
        assert!(client.send_dfs_message_if_routable_async(&owner, DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "native-async-read-1".into(), object_id: stat.object_id.clone(),
            version: Some(stat.version.clone()), content_hash: stat.content_hash.clone(),
            content_size_bytes: stat.size_bytes, range_offset: None, range_max_bytes: None,
        })).await.unwrap());
        let inbound = AsyncDfsMessageTransport::drain_incoming_messages_async(&mut client.transport).await.unwrap();
        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].source_node_id, owner);
        assert_eq!(inbound[0].target_node_id, requester);
        let DfsNetworkMessage::ReadResponse(response) = &inbound[0].message else { panic!("actual typed current read response"); };
        assert_eq!(response.request_id, "native-async-read-1");
        assert_eq!(response.object_id, stat.object_id);
        assert!(matches!(&response.result, DfsReadResult::Found(data) if data.bytes == b"actual source bytes"));
    });
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == requester
                && event.target == owner
                && event.route == crate::DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    drop(client);
    drop(primary);
    runtime.close().unwrap();
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
    let (runtime, mut dfs, config) = crate::test_support::current_native_metadata_service(
        "native-installed-catalog-positive-coverage",
        1,
        "/services",
    )
    .unwrap();
    let covered = dfs
        .posix_write_file(
            "/services/catalog-covered",
            b"catalog-covered bytes",
            DfsWriteOptions::replicated_eventual(vec![config.primary_node_id.clone()]),
        )
        .unwrap();
    let covered_record = runtime
        .node_metadata(&config.primary_node_id)
        .unwrap()
        .get_version(&covered.object_id, &covered.version)
        .unwrap()
        .unwrap();
    assert!(covered_record.visible);
    let covered_hash = covered_record.content_hash.unwrap();
    dfs.content
        .ensure_content_reservation(
            covered.object_id.as_str(),
            covered.version.as_str(),
            &covered_hash,
        )
        .unwrap();
    let uncovered_object = DfsObjectId::generate();
    let uncovered_version = DfsVersion::generate();
    let uncovered = dfs
        .content
        .put_object_version(
            uncovered_object.as_str(),
            uncovered_version.as_str(),
            b"catalog-uncovered bytes",
        )
        .unwrap();
    let catalog_versions =
        BTreeSet::from([(covered.object_id.to_string(), covered.version.to_string())]);
    // The positive tuple comes from an actually published current version. A
    // coverage-only reconciliation must retain an unrelated local reservation.
    let report = dfs
        .reconcile_content_reservations_from_installed_catalog(
            &catalog_versions,
            "native-installed-catalog-positive-coverage",
            u64::MAX,
        )
        .unwrap();
    let reconciliation = report.content_reservation_reconciliation.unwrap();
    assert_eq!(reconciliation.reservation_count, 2);
    assert_eq!(reconciliation.examined_reservation_count, 2);
    assert_eq!(reconciliation.released_catalog_covered_count, 1);
    assert_eq!(reconciliation.released_rejected_count, 0);
    assert_eq!(reconciliation.retained_catalog_too_old_count, 1);
    let reservations = dfs.content.content_reservation_roots().unwrap();
    assert_eq!(reservations.reservation_count, 1);
    assert_eq!(
        reservations.reservations[0].identity.object_id,
        uncovered_object.as_str()
    );
    dfs.content
        .read_manifest(covered.object_id.as_str(), covered.version.as_str())
        .unwrap();
    dfs.content
        .read_manifest(uncovered_object.as_str(), uncovered_version.as_str())
        .unwrap();
    assert!(dfs.content.has_blob(&covered_hash).unwrap());
    assert!(dfs.content.has_blob(&uncovered.content_hash).unwrap());
    drop(dfs);
    runtime.close().unwrap();
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
fn add_object_metadata_batch_rejects_unsealed_file_bytes() {
    let node =
        crate::test_support::DfsTestRuntime::ordered_node_ids("native-unsealed-metadata-batch", 1)
            .remove(0);
    let mut runtime =
        crate::test_support::DfsTestRuntime::new("native-unsealed-metadata-batch", node.clone())
            .unwrap();
    runtime.start_nodes().unwrap();
    // Metadata-only publication is Main-side store work: use the root shard's
    // store-backed opener, not a caller delegating to its Main.
    let mut service = runtime.metadata_primary_service("/").unwrap();
    let fs_id = runtime.file_system().file_system_id;
    let shard = DfsMetadataShardId::new("root");
    let before = service
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    let template = service
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&fs_id, "/")
        .unwrap()
        .unwrap();
    let objects = (0..3)
        .map(|index| {
            let mut object = template.clone();
            object.path = format!("/unsealed-batch-{index}.bin");
            object.object_id = crate::object_id_for_path(&object.path);
            object.version = DfsVersion::generate();
            object.kind = DfsObjectKind::File;
            object.content_hash = Some(format!(
                "blake3:{}",
                blake3::hash(b"unverified bytes").to_hex()
            ));
            object.content_size_bytes = Some(16);
            object
        })
        .collect::<Vec<_>>();
    assert!(
        service.add_object_metadata_batch(objects.clone()).is_err(),
        "metadata-only updates cannot authorize new file bytes without a sealed content operation"
    );
    let backend = service.metadata_backend().unwrap();
    assert_eq!(
        backend
            .export_shard_table_snapshot(&shard)
            .unwrap()
            .unwrap(),
        before
    );
    for object in objects {
        assert!(backend.get_object(&object.object_id).unwrap().is_none());
        assert!(
            backend
                .get_version(&object.object_id, &object.version)
                .unwrap()
                .is_none()
        );
    }
    drop(service);
    runtime.close().unwrap();
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
    let snapshot_sender: DfsMetadataSnapshotInstallSender = Arc::new(|_, _, _| {
        Err(DfsMetadataPeerTransportError::new(
            "unexpected snapshot route",
        ))
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
        batch: DfsMetadataJournalFrameBatch {
            shard_id: DfsMetadataShardId::new("root"),
            journal_frames: Vec::new(),
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
fn background_quorum_stage_refills_failed_target_without_retry_tick() {
    let name = "native-quorum-stage-immediate-refill";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id;
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let path = "/services/native-quorum-refill.bin";
    drop(dfs);
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap();
    placement.required_node_ids = nodes.clone();
    let accepted = runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert_eq!(job.scanned_object_count, 0);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual empty Apply remains within its network budget"
        );
        runtime
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let background = runtime
        .deny_background_replica_before_claim(&primary)
        .unwrap();
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    let (_, _, policy) = dfs
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 3)
        .unwrap();
    assert_eq!(
        (policy.min_commit_replicas, policy.desired_replicas),
        (2, 3)
    );
    assert_eq!(
        policy
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect()
    );
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(primary.clone()),
        target: None,
        route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::BudgetExhaustedForFirstCall,
    });
    let before = runtime.rpc_observations().len();
    let retry_tick = dfs.repair_retry_tick;
    let bytes = (0..2 * 1024 * 1024 + 4096)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    // Measure this same Core producer's stage and retry state while it owns
    // the actual shard admission needed by a fresh namespace write.
    let execution = dfs
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: config.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&config.shard_id),
            owner_epoch: config.epoch,
            operation_id: name.into(),
        })
        .unwrap()
        .expect("the current native Core must acquire actual shard admission");
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: nodes,
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(execution);
    assert_eq!(
        dfs.repair_retry_tick, retry_tick,
        "crossing W refills immediately without a maintenance clock advance"
    );
    let calls = runtime.rpc_observations()[before..]
        .iter()
        .filter(|event| event.source == primary && event.route == DFS_DATA_STAGE_BLOB_ROUTE)
        .cloned()
        .collect::<Vec<_>>();
    let failed = calls
        .iter()
        .find(|event| event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse)
        .unwrap();
    let accepted = calls
        .iter()
        .find(|event| event.phase == crate::test_support::DfsTestRpcPhase::Completed)
        .unwrap();
    assert_ne!(failed.target, accepted.target);
    assert_ne!(failed.call_id, accepted.call_id);
    assert_eq!(
        calls
            .iter()
            .filter(
                |event| event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse
            )
            .count(),
        1
    );
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), accepted.target.clone()])
    );
    assert_eq!(dfs.coordinator().repair_jobs().len(), 1);
    assert_eq!(
        dfs.coordinator().repair_jobs()[0].missing_replica_node_id,
        failed.target
    );
    let peer = runtime.client(&accepted.target).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(peer);
    drop(dfs);
    runtime.clear_faults();
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn malformed_dfs_object_rpc_message_does_not_block_following_valid_message() {
    let name = "native-malformed-object-rpc-isolated";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let server = config.primary_node_id;
    let requester = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != server)
        .unwrap();
    let bytes = b"valid native message after bad message";
    let stat = dfs
        .posix_write_file(
            "/services/native-malformed-isolation",
            bytes,
            DfsWriteOptions::replicated_eventual(vec![server.clone()]),
        )
        .unwrap();
    let before = runtime
        .node_metadata(&server)
        .unwrap()
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .map(|status| (status.shard_id, status.journal_last_sequence))
        .collect::<BTreeMap<_, _>>();
    runtime
        .rpc_request(
            &requester,
            &server,
            ObjectRpcRequest::from_encoded_body(
                DFS_DATA_READ_ROUTE,
                DFS_RPC_CATEGORY,
                b"not a sinomo object payload".to_vec(),
            ),
            Duration::from_secs(5),
        )
        .expect_err("real endpoint rejects malformed SOBJ");
    assert_eq!(
        runtime
            .node_metadata(&server)
            .unwrap()
            .metadata_shard_statuses()
            .unwrap()
            .into_iter()
            .map(|status| (status.shard_id, status.journal_last_sequence))
            .collect::<BTreeMap<_, _>>(),
        before
    );
    let command = test_dfs_message_command_from_message(
        &requester,
        &server,
        2,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "native-read-after-malformed".into(),
            object_id: stat.object_id.clone(),
            version: Some(stat.version.clone()),
            content_hash: stat.content_hash.clone(),
            content_size_bytes: stat.size_bytes,
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let reply = runtime
        .rpc_result(
            &requester,
            &server,
            sinomo_core::network::ObjectRpcCall::value(dfs_message_rpc_request(&command).unwrap()),
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(
        !reply.body.is_empty(),
        "actual data-read reply owns its attachment"
    );
    let head =
        crate::body_rpc::decode_dfs_attachment_message_rpc_response(&reply.response).unwrap();
    assert_eq!(head.attachment.total_size_bytes, bytes.len() as u64);
    let response = runtime.runtime_handle().block_on(async {
        tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            crate::node_transport::dfs_message_response_from_rpc_result(reply),
        )
        .await
        .unwrap()
        .unwrap()
    });
    assert!(matches!(response.outbound_messages.as_slice(),
        [DfsRoutedMessage { message: DfsNetworkMessage::ReadResponse(DfsReadResponse {
            request_id, object_id, result: DfsReadResult::Found(data),
        }), .. }] if request_id == "native-read-after-malformed"
            && object_id == &stat.object_id && data.version == stat.version && data.bytes.as_slice() == bytes
    ));
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn object_metadata_normalizes_duplicate_current_locations() {
    let root = temp_root("metadata-normalizes-duplicate-locations");
    let owner = NodeId::from_legacy_named("node_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_location_replica").expect("node");
    let (_runtime, client) = native_sifs_service(
        "object_metadata_normalizes_duplicate_current_locations",
        owner.clone(),
    );
    drop(client);
    // Metadata publication is Main-side store work on the store-backed opener.
    let mut dfs = _runtime.metadata_primary_service("/").unwrap();
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
    let name = "native-stage-primary-transient-create";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id;
    drop(service);
    let others = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let owner = others[0].clone();
    let replica = others[1].clone();
    let path = "/services/native-transient-stage.bin";
    assert!(
        runtime
            .node_metadata(&replica)
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), path)
            .unwrap()
            .is_none()
    );
    let mut policy = DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedConfirmed, 2, 2);
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    let mut requested =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    requested.required_node_ids = policy.required_node_ids.clone();
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, requested)
        .unwrap();
    let actual_config = runtime.shared_control_plane().unwrap().with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap()
    });
    assert!(
        actual_config.subtree_policies.contains(&policy),
        "stage coverage requires its actual committed required-node rule: {actual_config:?}"
    );
    let mut writer = runtime.client(&owner).unwrap();
    let (_, _, write_policy) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedConfirmed, 2)
        .unwrap();
    assert_eq!(write_policy.min_commit_replicas, 2);
    assert_eq!(write_policy.desired_replicas, 2);
    assert_eq!(write_policy.required_node_ids, policy.required_node_ids);
    let before = runtime.rpc_observations().len();
    let bytes = b"actual metadata primary stages a requester's new object";
    let stat = writer
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: write_policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(stat.policy, DfsObjectPolicy::ReplicatedConfirmed);
    let stage_observations = runtime.rpc_observations();
    assert!(
        stage_observations[before..]
            .iter()
            .any(|event| event.source == primary
                && event.target == replica
                && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "actual completed write={stat:?}; first actual RPC cut={:?}; subsequent actual RPC cut={:?}",
        &stage_observations[before..],
        &runtime.rpc_observations()[before..]
    );
    let publisher = runtime.node_metadata(&primary).unwrap();
    let metadata = publisher.get_object(&stat.object_id).unwrap().unwrap();
    assert_eq!(metadata.owner_node_id, owner);
    let receipts = publisher
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, replica.clone()])
    );
    let receiver = runtime.client(&replica).unwrap();
    let manifest = receiver
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(Some(manifest.content_hash), stat.content_hash);
    assert_eq!(
        receiver
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(receiver);
    drop(publisher);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn visible_metadata_publication_is_idempotent() {
    let node_id = NodeId::from_legacy_named("node_visible_metadata_idempotent").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_meta/work/site.py";

    let (runtime, mut client) = native_sifs_service(
        "visible_metadata_publication_is_idempotent",
        node_id.clone(),
    );
    client
        .posix_mkdir_all("/services/sandbox/instances/sandbox_meta/work")
        .expect("mkdir");
    let stat = client
        .posix_write_file(
            file_path,
            b"print('metadata')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
    drop(client);
    // Republication is Main-side store work on the store-backed opener.
    let mut dfs = runtime.metadata_primary_service(file_path).unwrap();
    let object = dfs
        .current_read_object_metadata(&stat.object_id)
        .expect("current object");
    let journal_before = dfs
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();

    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object");
    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("repeat visible object again");
    dfs.put_object_version_size_in_metadata_backend(&object, stat.size_bytes)
        .expect("repeat visible version size");

    let journal_after = dfs
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();
    assert_eq!(
        journal_after, journal_before,
        "re-publishing unchanged object/version metadata should not emit journal entries"
    );

    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object
    );
    assert_eq!(
        dfs.posix_read_file(file_path).unwrap(),
        b"print('metadata')\n"
    );
    drop(dfs);
    runtime.close().unwrap();
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
    let mut cluster = sifs_posix_cluster("sifs-canonical-catalog-remote-payload", "/services");
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
        super::cross_shard::node_store_object_at(
            &cluster._runtime,
            &cluster.observer_node,
            catalog_path,
        )
        .is_some_and(|object| object.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
    assert!(
        !cluster
            .observer
            .content()
            .has_blob(&expected_hash)
            .expect("inspect observer content mirror"),
        "the fixture must keep catalog bytes exclusively on another online node"
    );

    let before = cluster._runtime.rpc_observations().len();
    let visible = cluster
        .observer
        .posix_read_file_distributed(catalog_path)
        .expect("observer catalog read");
    assert_eq!(visible, catalog);
    // A caller delegating to its Main returns the payload without populating a
    // local mirror; the actual remote object read is the fetch evidence.
    assert!(
        cluster._runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == cluster.observer_node
                && event.target != cluster.observer_node
                && event.route == crate::DFS_DIRECT_READ_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed),
        "a canonical SIFS read should fetch the payload instead of treating the local mirror as authoritative"
    );
    drop(cluster);
}

#[test]
fn local_eventual_batch_shard_selection_uses_one_current_snapshot_for_bulk_paths() {
    let name = "native-local-eventual-bulk-capture";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 1, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    drop(service);
    // These counters and the explicit publication replay belong to the actual
    // owning Core lane, rather than a browser facade's sharded read projection.
    let (mut dfs, probe) = runtime
        .metadata_primary_service_with_control_plane_probe("/services")
        .unwrap();
    dfs.posix_mkdir_all("/services/native-bulk").unwrap();
    let parent_before = dfs.posix_stat("/services/native-bulk").unwrap();
    let shard_id = DfsMetadataShardId::new(&config.shard_id);
    let before = dfs
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .map(|record| record.digest.operation_id)
        .collect::<BTreeSet<_>>();
    let paths = (0..64)
        .map(|index| format!("/services/native-bulk/file-{index:03}"))
        .collect::<Vec<_>>();
    let files = paths
        .iter()
        .map(|path| (path.as_str(), b"genuine local bytes".as_slice()))
        .collect::<Vec<_>>();
    let prefix_reads_before = probe.prefix_read_calls();
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    crate::service_metadata::reset_namespace_prepare_expectation_capture_calls();
    let stats = dfs
        .posix_create_files_local_eventual_with_known_parents(files.iter().copied())
        .unwrap();
    let snapshot_calls = crate::metadata_backend::metadata_current_tables_snapshot_calls();
    eprintln!(
        "64-file bulk: snapshots={snapshot_calls}, prepare_read_sets={}",
        crate::service_metadata::namespace_prepare_expectation_capture_calls()
    );
    assert_eq!(stats.len(), 64);
    assert!(
        snapshot_calls <= 10,
        "64-file bulk validation opened {snapshot_calls} table snapshots"
    );
    assert_eq!(
        probe.prefix_read_calls(),
        prefix_reads_before,
        "native bulk routing must consume its already-current assignment projection without an implicit KV scan"
    );
    assert_eq!(
        crate::service_metadata::namespace_prepare_expectation_capture_calls(),
        1,
        "same-shard bulk validation and prepare share one real captured read set"
    );
    let authored = dfs
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .filter(|record| !before.contains(&record.digest.operation_id))
        .collect::<Vec<_>>();
    assert_eq!(authored.len(), 1);
    assert_eq!(authored[0].digest.shard_id.0, config.shard_id);
    for (stat, (path, bytes)) in stats.iter().zip(files.iter()) {
        assert_eq!(dfs.posix_read_file(path).unwrap(), *bytes);
        assert!(
            dfs.backend_receipt_for_replica(&stat.object_id, &stat.version, &owner)
                .unwrap()
                .is_some()
        );
    }
    // Reuse only the actual committed immutable body. Changed current heads
    // cannot turn its retry into a fresh visibility write or another receipt.
    let replay_body = dfs
        .metadata_backend()
        .unwrap()
        .prepared_operation_payload(&authored[0].digest.operation_id)
        .unwrap()
        .unwrap();
    let before_replay = dfs
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .map(|status| (status.shard_id, status.journal_last_sequence))
        .collect::<BTreeMap<_, _>>();
    assert!(
        !dfs.publish_prepared_operation(replay_body.as_ref().clone())
            .unwrap()
            .published
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap()
            .into_iter()
            .map(|status| (status.shard_id, status.journal_last_sequence))
            .collect::<BTreeMap<_, _>>(),
        before_replay
    );
    // Exercise the batch entry too, with genuine original receipts and parent
    // observations. An old exact batch must not roll back a later file head.
    let proofs = replay_body
        .content_receipts
        .iter()
        .map(|receipt| DfsLocalEventualBatchWriteProof {
            expected_base_version: None,
            expected_parent_object_id: Some(parent_before.object_id.clone()),
            expected_parent_version: Some(parent_before.version.clone()),
            receipt: receipt.clone(),
        })
        .collect::<Vec<_>>();
    let newer = dfs
        .posix_write_file(
            &paths[0],
            b"newer bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let head_before_replay = dfs
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();
    assert!(
        !dfs.apply_local_eventual_batch_commit(
            &owner,
            replay_body.transaction.clone(),
            proofs.clone(),
            Some(config.clone())
        )
        .unwrap()
    );
    assert_eq!(
        dfs.metadata.object(&newer.object_id).unwrap().version,
        newer.version
    );
    assert_eq!(dfs.posix_read_file(&paths[0]).unwrap(), b"newer bytes");
    let mut changed_parent = proofs.clone();
    changed_parent[0].expected_parent_version = Some(newer.version.clone());
    assert!(
        dfs.apply_local_eventual_batch_commit(
            &owner,
            replay_body.transaction.clone(),
            changed_parent,
            Some(config.clone())
        )
        .is_err()
    );
    let mut changed_body = replay_body.transaction.clone();
    changed_body.created_at += time::Duration::seconds(1);
    assert!(
        dfs.apply_local_eventual_batch_commit(
            &owner,
            changed_body,
            proofs.clone(),
            Some(config.clone())
        )
        .is_err()
    );
    let mut changed_receipt = proofs.clone();
    changed_receipt[0].receipt.content_hash = Some("blake3:changed-replay-proof".into());
    assert!(
        dfs.apply_local_eventual_batch_commit(
            &owner,
            replay_body.transaction.clone(),
            changed_receipt,
            Some(config.clone())
        )
        .is_err()
    );
    let mut duplicate_proof = proofs.clone();
    duplicate_proof[1] = duplicate_proof[0].clone();
    assert!(
        dfs.apply_local_eventual_batch_commit(
            &owner,
            replay_body.transaction.clone(),
            duplicate_proof,
            Some(config)
        )
        .is_err()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap(),
        head_before_replay
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .prepared_operation_record(&authored[0].digest.operation_id)
            .unwrap()
            .unwrap(),
        authored[0]
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn bulk_local_eventual_splits_distinct_shards_owned_by_the_same_primary() {
    let (runtime, mut dfs, _) = crate::test_support::current_native_metadata_service(
        "native-bulk-same-owner-distinct-shards",
        1,
        "/services",
    )
    .unwrap();
    dfs.posix_mkdir_all("/users/native-bulk").unwrap();
    dfs.posix_mkdir_all("/services/native-bulk").unwrap();
    let committed = || {
        ["users", "services"]
            .into_iter()
            .flat_map(|shard| {
                // Snapshot the actual Core lane after the served operation has
                // drained; a browser facade may retain its earlier projection.
                let current = runtime
                    .metadata_primary_service(&format!("/{shard}"))
                    .unwrap();
                current
                    .metadata_backend()
                    .unwrap()
                    .export_shard_table_snapshot(&DfsMetadataShardId::new(shard))
                    .unwrap()
                    .unwrap()
                    .tables
                    .prepared_operations_v7
            })
            .filter(|record| record.status == crate::DfsPreparedOperationStatus::Committed)
            .collect::<Vec<_>>()
    };
    let before = committed()
        .into_iter()
        .map(|record| record.digest.operation_id)
        .collect::<BTreeSet<_>>();
    let files = [
        (
            "/users/native-bulk/users.txt",
            b"users shard bytes".as_slice(),
        ),
        (
            "/services/native-bulk/services.txt",
            b"services shard bytes".as_slice(),
        ),
    ];
    let stats = dfs
        .posix_create_files_local_eventual_with_known_parents(files)
        .unwrap();
    assert_eq!(stats.len(), 2);
    let authored = committed()
        .into_iter()
        .filter(|record| !before.contains(&record.digest.operation_id))
        .collect::<Vec<_>>();
    assert_eq!(authored.len(), 2, "one genuine committed batch per shard");
    let scopes = authored
        .into_iter()
        .map(|record| record.digest.shard_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        scopes,
        BTreeSet::from([
            DfsMetadataShardId::new("users"),
            DfsMetadataShardId::new("services")
        ])
    );
    for (path, bytes) in files {
        assert_eq!(dfs.posix_read_file_distributed(path).unwrap(), bytes);
    }
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn local_eventual_batch_rejects_multiple_shards_with_the_same_local_primary() {
    let (runtime, mut dfs, config) = crate::test_support::current_native_metadata_service(
        "native-raw-batch-rejects-actual-multiple-scopes",
        1,
        "/services",
    )
    .unwrap();
    let owner = config.primary_node_id;
    let config_owner = owner.clone();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let a = dfs
        .posix_write_file(
            "/services/native-cross-batch-source",
            b"services source bytes",
            options.clone(),
        )
        .unwrap();
    let b = dfs
        .posix_write_file(
            "/users/native-cross-batch-source",
            b"users source bytes",
            options,
        )
        .unwrap();
    let backend = runtime.node_metadata(&owner).unwrap();
    let objects = [&a, &b].map(|stat| backend.get_object(&stat.object_id).unwrap().unwrap());
    let versions = [&a, &b].map(|stat| {
        backend
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
    });
    let actual_snapshots = || {
        [
            ("root", "/"),
            ("services", "/services"),
            ("users", "/users"),
        ]
        .into_iter()
        .map(|(shard, path)| {
            let current = runtime.metadata_primary_service(path).unwrap();
            let id = DfsMetadataShardId::new(shard);
            (
                id.clone(),
                current
                    .metadata_backend()
                    .unwrap()
                    .export_shard_table_snapshot(&id)
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>()
    };
    let before = actual_snapshots();
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        extension_expectations: Vec::new(),
        contribution_digest: None,
        file_version_pins: None,
        transaction_id: "native-cross-scope-negative-batch".into(),
        file_system_id: a.file_system_id.clone(),
        objects: objects.to_vec(),
        versions: versions.to_vec(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::now_utc(),
    };
    let writes = [&a, &b]
        .into_iter()
        .map(|stat| DfsLocalEventualBatchWriteProof {
            expected_base_version: Some(stat.version.clone()),
            expected_parent_object_id: None,
            expected_parent_version: None,
            receipt: backend
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap()
                .into_iter()
                .find(|receipt| receipt.replica_node_id == owner)
                .unwrap(),
        })
        .collect();
    let operation = DfsNamespaceOperation::CommitLocalEventualBatch {
        transaction,
        writes,
    };
    drop(backend);
    // Serving a namespace request is Main-side store work: use the store-backed
    // opener rather than a caller delegating to its Main.
    let mut author = runtime.metadata_primary_service("/services").unwrap();
    let authority_shards = author
        .affected_namespace_shards(&a.file_system_id, &operation)
        .unwrap();
    assert_eq!(authority_shards.len(), 2);
    assert!(
        authority_shards
            .iter()
            .all(|shard| shard.primary_node_id == owner)
    );
    let event = author
        .handle_namespace_request(
            owner.clone(),
            DfsNamespaceCommand {
                request_id: "native-cross-scope-negative-request".into(),
                file_system_id: a.file_system_id.clone(),
                requesting_node_id: owner,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards,
                operation,
            },
        )
        .unwrap();
    assert!(
        matches!(event, NetworkedDfsServiceEvent::RemoteNamespaceServed {
        result: DfsNamespaceResult::Rejected { ref reason, .. }, ..
    } if reason.contains("must resolve to exactly one metadata shard")),
        "{event:?}"
    );
    drop(author);
    assert_eq!(actual_snapshots(), before);
    let backend = runtime.node_metadata(&config_owner).unwrap();
    assert_eq!(
        backend.get_object(&a.object_id).unwrap(),
        Some(objects[0].clone())
    );
    assert_eq!(
        backend.get_object(&b.object_id).unwrap(),
        Some(objects[1].clone())
    );
    drop(dfs);
    runtime.close().unwrap();
}
