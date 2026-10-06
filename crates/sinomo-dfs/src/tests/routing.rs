//! Routing regression tests.

use super::*;

#[test]
fn routed_namespace_complete_single_response_batch_avoids_follow_up_sync() {
    let writer = NodeId::from_legacy_named("node_routed_sync_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_routed_sync_primary").expect("primary");
    let file_system_id = sifs_file_system_id();
    let root = temp_root("routed_namespace_sync_synced_path_backlog");
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![writer.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let primary_sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected primary-side send to {target_node_id} for {}",
            command.request_id
        )))
    });
    let primary_transport = DfsNodeServedMessageTransport::new_with_message_sender(
        primary.clone(),
        vec![writer.clone()],
        primary_sender,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let primary_backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("services"),
        root.join("primary-metadata"),
    )
    .expect("primary metadata backend");
    let mut primary_service = NetworkedDfsService::new(
        primary_transport,
        LocalContentStore::new(root.join("primary-content")).expect("primary content"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(primary_backend)
    .expect("primary metadata");
    primary_service
        .bootstrap_distributed_sifs()
        .expect("primary bootstrap");
    let services_shard_id = DfsMetadataShardId::new("services");
    let bootstrap_sequence = primary_service
        .metadata_backend()
        .expect("primary metadata backend")
        .journal_last_sequence()
        .expect("primary bootstrap journal head");
    let bootstrap_snapshot = primary_service
        .metadata_backend()
        .expect("primary metadata backend")
        .export_shard_table_snapshot(&services_shard_id)
        .expect("primary bootstrap snapshot")
        .expect("services bootstrap snapshot");
    for index in 0..(DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 8) {
        let mut object = file_metadata(primary.clone());
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_routed_sync_backlog_{index}"))
                .expect("object");
        object.path = format!("/services/backlog-seed-{index}.txt");
        object.version =
            DfsVersion::from_legacy_named(format!("version_routed_sync_{index}")).expect("version");
        object.content_hash = Some(format!("blake3:routed-sync-{index}"));
        object.content_size_bytes = Some(index as u64);
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        primary_service
            .add_object_metadata(object)
            .expect("primary backlog object");
    }
    let primary_service = Arc::new(Mutex::new(primary_service));
    let namespace_calls = Arc::new(Mutex::new(Vec::new()));
    let mkdir_all_response_batches = Arc::new(Mutex::new(Vec::new()));
    let mkdir_all_response_has_more = Arc::new(Mutex::new(Vec::new()));
    let primary_for_sender = primary.clone();
    let primary_service_for_sender = Arc::clone(&primary_service);
    let namespace_calls_for_sender = Arc::clone(&namespace_calls);
    let mkdir_all_response_batches_for_sender = Arc::clone(&mkdir_all_response_batches);
    let mkdir_all_response_has_more_for_sender = Arc::clone(&mkdir_all_response_has_more);
    let writer_sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        if target_node_id != &primary_for_sender {
            return Err(DfsPeerTransportError::new(format!(
                "unexpected target {target_node_id}"
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
        let capture_mkdir_all_response = match &command.routed_message.message {
            DfsNetworkMessage::NamespaceRequest(namespace) => {
                matches!(&namespace.operation, DfsNamespaceOperation::MkdirAll { .. })
            }
            _ => false,
        };
        if let DfsNetworkMessage::NamespaceRequest(namespace) = &command.routed_message.message {
            let label = match &namespace.operation {
                DfsNamespaceOperation::MkdirAll { .. } => "mkdir-all",
                DfsNamespaceOperation::SyncPath { .. } => "sync-path",
                _ => "other-namespace",
            };
            namespace_calls_for_sender
                .lock()
                .expect("namespace calls")
                .push(label.to_string());
        }
        let result = primary_service_for_sender
            .lock()
            .expect("primary service")
            .process_message_command(command.clone())
            .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
        if capture_mkdir_all_response {
            let mut captured_batches = mkdir_all_response_batches_for_sender
                .lock()
                .expect("mkdir-all response batches");
            for outbound in &result.response.outbound_messages {
                let DfsNetworkMessage::NamespaceResponse(response) = &outbound.message else {
                    continue;
                };
                mkdir_all_response_has_more_for_sender
                    .lock()
                    .expect("mkdir-all response continuation")
                    .push(response.metadata_journal_has_more);
                for batch in &response.metadata_journal_batches {
                    let encoded_bytes =
                        crate::metadata_journal::metadata_journal_batch_encoded_len(batch)
                            .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
                    captured_batches.push((batch.entries.len(), encoded_bytes));
                }
            }
        }
        Ok(result.response)
    });
    let writer_transport = DfsNodeTransport::with_message_sender_for_nodes(
        writer.clone(),
        vec![primary.clone()],
        Duration::from_millis(20),
        writer_sender,
    );
    let mut writer_backend =
        LocalSobjMetadataBackend::new(services_shard_id.clone(), root.join("writer-metadata"))
            .expect("writer metadata backend");
    writer_backend
        .replace_with_replicated_snapshot(services_shard_id, bootstrap_sequence, bootstrap_snapshot)
        .expect("seed writer from replicated bootstrap prefix");
    let mut writer_service = NetworkedDfsService::new(
        writer_transport,
        LocalContentStore::new(root.join("writer-content")).expect("writer content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(writer_backend)
    .expect("attach writer metadata replica");
    writer_service
        .posix_mkdir_all("/services/routed-sync-backlog/new-dir")
        .expect("routed mkdir should drain bounded response backlog");

    assert_eq!(
        DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT, 2048,
        "namespace catch-up should request the larger entry cap"
    );
    assert_eq!(
        DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        "namespace catch-up must retain the ordinary 4 MiB encoded-byte bound"
    );
    let response_batches = mkdir_all_response_batches
        .lock()
        .expect("mkdir-all response batches")
        .clone();
    assert_eq!(
        response_batches.len(),
        DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT,
        "the namespace response should still carry only one journal batch"
    );
    let (entry_count, encoded_bytes) = response_batches[0];
    assert!(
        entry_count > DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
        "the namespace response should pack past the ordinary 512-entry cap"
    );
    assert!(
        entry_count <= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT,
        "the namespace response exceeded its 2048-entry cap"
    );
    assert!(
        encoded_bytes <= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
        "the namespace response exceeded its 4 MiB encoded-byte cap"
    );
    assert_eq!(
        *mkdir_all_response_has_more
            .lock()
            .expect("mkdir-all response continuation"),
        vec![Some(false)],
        "the authority should explicitly report that the single batch covered its journal tail"
    );

    let calls = namespace_calls.lock().expect("namespace calls").clone();
    assert_eq!(
        calls.first().map(String::as_str),
        Some("mkdir-all"),
        "the mutating operation should be sent first"
    );
    assert!(
        !calls.iter().any(|call| call == "sync-path"),
        "a complete one-batch response must not trigger a redundant follow-up sync"
    );
    assert!(
        writer_service
            .posix_stat(&format!(
                "/services/backlog-seed-{}.txt",
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 7
            ))
            .is_ok(),
        "latest backlog entry should be visible after routed mkdir returns"
    );
}

#[test]
fn dfs_peer_rpc_helpers_preserve_route_contracts() {
    let shard_id = DfsMetadataShardId::new("rpc-helper-shard");
    let local = NodeId::from_legacy_named("node_dfs_rpc_helper_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_rpc_helper_remote").expect("remote");

    let status_request = dfs_status_rpc_request().expect("status request");
    assert_eq!(status_request.route, DFS_STATUS_ROUTE);
    assert_eq!(status_request.category, DFS_RPC_CATEGORY);
    status_request
        .decode_body::<()>()
        .expect("status empty body");
    let status = DfsOperatorStatus {
        storage_migrations: Vec::new(),
        incomplete_reasons: Vec::new(),
        bounded_omissions: Vec::new(),
        file_system_count: 1,
        object_count: 2,
        version_count: 3,
        receipt_count: 4,
        intent_count: 5,
        pending_write_count: 6,
        open_conflict_count: 7,
        pending_repair_count: 8,
        repair_attempt_count: 0,
        pending_namespace_job_count: 9,
        failed_namespace_job_count: 10,
        cross_shard_namespace_lock_count: Some(11),
        metadata_shards: Vec::new(),
        repair_attempts: Vec::new(),
    };
    let status_rpc_response =
        sinomo_core::network::ObjectRpcResponse::encode_body(DFS_STATUS_ROUTE, &status)
            .expect("status response");
    assert_eq!(
        decode_dfs_status_rpc_response(&status_rpc_response).expect("status response body"),
        status
    );

    let inspection_request = DaemonDfsInspectionRequest::Object {
        object_id: DfsObjectId::from_legacy_named("object_rpc_helper").expect("object"),
    };
    let inspection_rpc_request =
        dfs_inspection_rpc_request(&inspection_request).expect("inspection request");
    assert_eq!(inspection_rpc_request.route, DFS_INSPECTION_ROUTE);
    assert_eq!(inspection_rpc_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        inspection_rpc_request
            .decode_body::<DaemonDfsInspectionRequest>()
            .expect("inspection body"),
        inspection_request
    );
    let inspection_response = DaemonDfsInspectionResponse::Object { object: None };
    let inspection_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_INSPECTION_ROUTE,
        &inspection_response,
    )
    .expect("inspection response");
    assert_eq!(
        decode_dfs_inspection_rpc_response(&inspection_rpc_response)
            .expect("inspection response body"),
        inspection_response
    );

    let smoke_request = dfs_smoke_rpc_request().expect("smoke request");
    assert_eq!(smoke_request.route, DFS_SMOKE_ROUTE);
    assert_eq!(smoke_request.category, DFS_RPC_CATEGORY);
    smoke_request.decode_body::<()>().expect("smoke empty body");
    let smoke_response = DfsSmokeReport {
        path: "/smoke.txt".to_string(),
        file_system_id: sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("object_smoke_rpc_helper").expect("object"),
        version: DfsVersion::from_legacy_named("version_smoke_rpc_helper").expect("version"),
        content_hash: Some("hash".to_string()),
        size_bytes: Some(4),
        requested_replica_node_ids: vec![local.clone()],
        committed_replica_node_ids: vec![local.clone()],
        pending_repair_count: 0,
        metadata_replication: DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        },
        attempts: vec![DfsSmokeAttempt {
            replica_node_ids: vec![local.clone()],
            path: "/smoke.txt".to_string(),
            committed: true,
            detail: "ok".to_string(),
        }],
    };
    let smoke_rpc_response =
        sinomo_core::network::ObjectRpcResponse::encode_body(DFS_SMOKE_ROUTE, &smoke_response)
            .expect("smoke response");
    assert_eq!(
        decode_dfs_smoke_rpc_response(&smoke_rpc_response).expect("smoke response body"),
        smoke_response
    );

    let journal_command = DfsMetadataJournalBatchCommand {
        request_id: "journal-request".to_string(),
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
            next_after_sequence: 7,
        },
    };
    let journal_request =
        dfs_metadata_journal_batch_rpc_request(&journal_command).expect("journal request");
    assert_eq!(journal_request.route, DFS_METADATA_JOURNAL_BATCH_ROUTE);
    assert_eq!(journal_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        journal_request
            .decode_body::<DfsMetadataJournalBatchCommand>()
            .expect("journal body"),
        journal_command
    );
    let journal_response = DfsMetadataJournalBatchResponse {
        request_id: "journal-request".to_string(),
        shard_id: shard_id.clone(),
        next_after_sequence: 7,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: DfsMetadataJournalBatchResult::Applied,
    };
    let journal_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_METADATA_JOURNAL_BATCH_ROUTE,
        &journal_response,
    )
    .expect("journal response");
    assert_eq!(
        decode_dfs_metadata_journal_batch_rpc_response(&journal_rpc_response)
            .expect("journal response body"),
        journal_response
    );

    let snapshot_root = temp_root("rpc_helper_snapshot");
    let snapshot = LocalSobjMetadataBackend::new(shard_id.clone(), &snapshot_root)
        .expect("metadata backend")
        .export_table_snapshot();
    let snapshot_command = DfsMetadataSnapshotInstallCommand {
        request_id: "snapshot-request".to_string(),
        source_node_id: local.clone(),
        file_system_id: None,
        authority_epoch: None,
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: 9,
        snapshot,
        reason: "test".to_string(),
    };
    let snapshot_request =
        dfs_metadata_snapshot_install_rpc_request(&snapshot_command).expect("snapshot request");
    assert_eq!(snapshot_request.route, DFS_METADATA_SNAPSHOT_INSTALL_ROUTE);
    assert_eq!(snapshot_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        snapshot_request
            .decode_body::<DfsMetadataSnapshotInstallCommand>()
            .expect("snapshot body"),
        snapshot_command
    );
    let snapshot_response = DfsMetadataSnapshotInstallResponse {
        request_id: "snapshot-request".to_string(),
        shard_id: shard_id.clone(),
        next_after_sequence: 9,
        result: DfsMetadataSnapshotInstallResult::Installed {
            archived_existing: false,
        },
    };
    let snapshot_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        &snapshot_response,
    )
    .expect("snapshot response");
    assert_eq!(
        decode_dfs_metadata_snapshot_install_rpc_response(&snapshot_rpc_response)
            .expect("snapshot response body"),
        snapshot_response
    );

    let message_command = DfsMessageCommand {
        request_id: "message-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 2,
            message_id: 3,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "message-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };
    let message_request = dfs_message_rpc_request(&message_command).expect("message request");
    assert_eq!(message_request.route, DFS_DATA_READ_ROUTE);
    assert_eq!(message_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        message_request
            .decode_body::<DfsMessageCommand>()
            .expect("message body"),
        message_command
    );
    assert_eq!(
        decode_dfs_message_rpc_request(&message_request).expect("validated message request"),
        message_command
    );
    let correlated_traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
    let correlated_request = crate::with_dfs_traceparent(Some(correlated_traceparent), || {
        dfs_message_rpc_request(&message_command).expect("correlated message request")
    });
    assert_eq!(
        correlated_request.traceparent(),
        Some(correlated_traceparent)
    );
    let namespace_command = DfsMessageCommand {
        request_id: "namespace-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 6,
            message_id: 7,
            message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: "namespace-mkdir".to_string(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: local.clone(),
                include_metadata_journal_batches: true,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::Mkdir {
                    path: "/rpc-helper".to_string(),
                },
            }),
        },
    };
    let namespace_request =
        dfs_message_rpc_request(&namespace_command).expect("namespace message request");
    assert_eq!(namespace_request.route, DFS_DATA_NAMESPACE_ROUTE);
    assert!(
        namespace_request.is_response_retry_safe(),
        "durably admitted namespace mutations must reconcile an exact retry after a lost response"
    );
    let distributed_command = DfsMessageCommand {
        request_id: "distributed-write-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 8,
            message_id: 9,
            message: DfsNetworkMessage::DistributedWriteRequest(test_routed_write_command(
                "distributed-write-request",
                &local,
                &DfsObjectId::generate(),
            )),
        },
    };
    let distributed_request =
        dfs_message_rpc_request(&distributed_command).expect("distributed write message request");
    assert!(
        distributed_request.is_response_retry_safe(),
        "distributed write request IDs make a lost response safe to redispatch"
    );
    let distributed_response = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "distributed-write-response".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 9,
            message_id: 10,
            message: DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: "distributed-write-response".to_string(),
                object_id: DfsObjectId::generate(),
                result: DfsDistributedWriteResult::Rejected {
                    reason: "test completed response".to_string(),
                    current_primary_node_id: None,
                },
                metadata_journal_batches: Vec::new(),
            }),
        },
    })
    .expect("distributed write response message request");
    assert!(
        distributed_response.is_response_retry_safe(),
        "a completed response is requester-idempotent and must survive an ambiguous detached send"
    );
    let mut exclusive_create =
        test_routed_write_command("exclusive-create-request", &local, &DfsObjectId::generate());
    exclusive_create.exclusive_create = true;
    let exclusive_request = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "exclusive-create-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 10,
            message_id: 11,
            message: DfsNetworkMessage::ExclusiveCreateRequest(exclusive_create),
        },
    })
    .expect("exclusive create message request");
    assert!(
        !exclusive_request.is_response_retry_safe(),
        "exclusive create cannot be redispatched until its success response is durably replayable"
    );
    let repair_object_id = DfsObjectId::generate();
    let repair_version = DfsVersion::generate();
    let repair_request = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "repair-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 10,
            message_id: 11,
            message: DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "repair-request".to_string(),
                intent_id: "repair-intent".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: repair_object_id.clone(),
                expected_base_version: None,
                version: repair_version.clone(),
                content_hash: "blake3:repair".to_string(),
                bytes: b"repair".to_vec(),
                object_metadata: None,
                authority_shard: None,
            }),
        },
    })
    .expect("repair message request");
    assert!(
        repair_request.is_response_retry_safe(),
        "content-addressed repair commands are safe to redispatch after a lost response"
    );
    let mismatched_route_request = sinomo_core::network::ObjectRpcRequest::encode_body(
        DFS_DATA_WRITE_ROUTE,
        DFS_RPC_CATEGORY,
        &message_command,
    )
    .expect("mismatched route request");
    let mismatched = decode_dfs_message_rpc_request(&mismatched_route_request)
        .expect_err("typed DFS route should match message body");
    assert!(
        mismatched
            .to_string()
            .contains("does not match message route"),
        "unexpected mismatch error: {mismatched}"
    );
    let message_response = DfsMessageResponse {
        request_id: "message-request".to_string(),
        outbound_messages: vec![DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 4,
            message_id: 5,
            message: DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: "message-read".to_string(),
                object_id: DfsObjectId::generate(),
                result: DfsReadResult::NotFound,
            }),
        }],
    };
    let message_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_DATA_READ_RESPONSE_ROUTE,
        &message_response,
    )
    .expect("message response");
    assert_eq!(
        decode_dfs_message_rpc_response(&message_rpc_response).expect("message response body"),
        message_response
    );
    assert_eq!(
        test_dfs_message_response_from_routed_messages(
            "message-request",
            message_response.outbound_messages.clone()
        ),
        message_response
    );
    std::fs::remove_dir_all(snapshot_root).ok();
}

#[test]
fn networked_dfs_builds_routed_messages_without_transport_send() {
    let root = temp_root("networked-dfs-builds-routed-messages");
    let local = NodeId::from_legacy_named("node_build_frame_local").expect("local");
    let remote = NodeId::from_legacy_named("node_build_frame_remote").expect("remote");
    let object_id = DfsObjectId::from_legacy_named("obj_build_frame").expect("object");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(ReceiveOnlyTransport::new(local.clone()), content);

    let first_routed_message = dfs.next_dfs_routed_message(
        &remote,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-1".to_string(),
            object_id: object_id.clone(),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );

    assert_eq!(first_routed_message.frame_id, 1);
    assert_eq!(first_routed_message.source_node_id, local);
    assert_eq!(first_routed_message.target_node_id, remote);
    assert_eq!(first_routed_message.message_id, 1);
    match &first_routed_message.message {
        DfsNetworkMessage::ReadRequest(request) => {
            assert_eq!(request.request_id, "read-1");
            assert_eq!(request.object_id, object_id);
        }
        other => panic!("unexpected DFS payload: {other:?}"),
    }

    let second_routed_message = dfs.next_dfs_routed_message(
        &first_routed_message.target_node_id.clone(),
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-2".to_string(),
            object_id: DfsObjectId::from_legacy_named("obj_build_frame_second").expect("object"),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    assert_eq!(second_routed_message.frame_id, 2);
    assert_eq!(second_routed_message.message_id, 2);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_tracks_remote_intent_alias() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_alias_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_alias_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_alias").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_write_alias").expect("version");
    let root = temp_root("routed-write-alias");
    let authority_shard = test_routed_authority_shard(&primary, 1);
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(test_routed_authority_control_plane(&authority_shard));
    dfs.writes.pending_routed_writes.insert(
        "dfs_net_req_1".to_string(),
        test_pending_routed_write(
            "dfs_net_req_1",
            &requester,
            &object_id,
            &primary,
            authority_shard,
        ),
    );

    dfs.handle_distributed_write_response(
        primary.clone(),
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_1".to_string(),
            object_id: object_id.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "intent_remote_alias".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("pending response");

    assert!(matches!(
        dfs.write_outcome("dfs_net_req_1").expect("request outcome"),
        NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts: 1,
            required_receipts: 2,
        } if intent_id == "intent_remote_alias"
    ));
    assert!(
        dfs.writes
            .pending_routed_writes
            .contains_key("intent_remote_alias")
    );
    assert!(
        dfs.writes.pending_routed_writes["dfs_net_req_1"]
            .command
            .is_some(),
        "the canonical request retains its retry command while unresolved"
    );
    assert!(
        dfs.writes.pending_routed_writes["intent_remote_alias"]
            .command
            .is_none(),
        "a remote intent alias must not clone the full routed command"
    );

    let event = dfs
        .handle_distributed_write_response(
            primary.clone(),
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_1".to_string(),
                object_id: object_id.clone(),
                result: DfsDistributedWriteResult::Rejected {
                    reason: "exclusive retry observed the pending create".to_string(),
                    current_primary_node_id: None,
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .expect("stale retry rejection");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert!(matches!(
        dfs.write_outcome("intent_remote_alias")
            .expect("intent remains pending"),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));

    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "intent_remote_alias".to_string(),
            object_id,
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed {
                    intent_id: "intent_remote_alias".to_string(),
                    version: version.clone(),
                    content_hash: "blake3:routed-write-alias".to_string(),
                    repair_jobs: Vec::new(),
                },
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("committed response");

    assert!(
        dfs.writes
            .pending_routed_writes
            .values()
            .all(|pending| pending.command.is_none()),
        "terminal routed-write state must release every command body"
    );
    assert!(matches!(
        dfs.take_write_outcome("intent_remote_alias")
            .expect("alias outcome"),
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            version: committed_version,
            ..
        } if intent_id == "intent_remote_alias" && committed_version == version
    ));
    assert!(
        dfs.writes.pending_routed_writes.is_empty(),
        "consuming the terminal outcome must remove the request and its aliases"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_stale_pending_response_does_not_downgrade_completion() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_stale_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_stale_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_stale").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_write_stale").expect("version");
    let root = temp_root("routed-write-stale-pending");
    let authority_shard = test_routed_authority_shard(&primary, 1);
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(test_routed_authority_control_plane(&authority_shard));
    dfs.writes.pending_routed_writes.insert(
        "dfs_net_req_1".to_string(),
        test_pending_routed_write(
            "dfs_net_req_1",
            &requester,
            &object_id,
            &primary,
            authority_shard,
        ),
    );

    dfs.handle_distributed_write_response(
        primary.clone(),
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_1".to_string(),
            object_id: object_id.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed {
                    intent_id: "intent_remote_alias".to_string(),
                    version: version.clone(),
                    content_hash: "blake3:routed-write-stale".to_string(),
                    repair_jobs: Vec::new(),
                },
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("committed response");

    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_1".to_string(),
            object_id,
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Pending {
                    intent_id: "intent_remote_alias".to_string(),
                    durable_receipts: 1,
                    required_receipts: 2,
                },
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("stale pending response");

    assert!(matches!(
        dfs.write_outcome("dfs_net_req_1")
            .expect("request outcome"),
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            version: committed_version,
            ..
        } if intent_id == "intent_remote_alias" && committed_version == version
    ));
    assert!(matches!(
        dfs.write_outcome("intent_remote_alias")
            .expect("alias outcome"),
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            version: committed_version,
            ..
        } if intent_id == "intent_remote_alias" && committed_version == version
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_write_outcome_wins_over_stale_routed_alias_shadow() {
    let local = NodeId::from_legacy_named("node_local_shadow_writer").expect("local");
    let replica = NodeId::from_legacy_named("node_local_shadow_replica").expect("replica");
    let primary = NodeId::from_legacy_named("node_local_shadow_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_local_shadow").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_local_shadow_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_local_shadow_2").expect("version");
    let root = temp_root("local-write-shadowed-by-routed-alias");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: local.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let file_system_id = sifs_file_system_id();
    dfs.add_file_system(DfsFileSystem {
        metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: file_system_id.clone(),
        display_name: "sifs".to_string(),
        owner_node_id: local.clone(),
        subtree_policies: Vec::new(),
    })
    .expect("filesystem");
    dfs.add_object_metadata(DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/local-shadow.txt".to_string(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: local.clone(),
        current_locations: vec![local.clone()],
        content_hash: Some(content_hash(b"old local shadow bytes")),
        content_size_bytes: Some(b"old local shadow bytes".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })
    .expect("object metadata");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: local.clone(),
            },
            b"new local shadow bytes".to_vec(),
            vec![local.clone(), replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("local write");
    let NetworkedDfsWriteOutcome::Committed { intent_id, .. } = write else {
        panic!("expected committed local write, got {write:?}");
    };
    dfs.writes.pending_routed_writes.insert(
        intent_id.clone(),
        PendingRoutedWrite {
            canonical_request_id: "dfs_net_req_shadow".to_string(),
            object_id: object_id.clone(),
            primary_node_id: primary.clone(),
            authority_shard: test_routed_authority_shard(&primary, 1),
            command: Some(test_routed_write_command(
                "dfs_net_req_shadow",
                &local,
                &object_id,
            )),
            attachment_bytes: None,
            authority_redirects_remaining: DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
            remote_intent_id: Some(intent_id.clone()),
            durable_receipts: 1,
            required_receipts: 2,
            result: None,
            compact_retained_at: None,
        },
    );

    assert!(matches!(
        dfs.write_outcome(&intent_id).expect("shadowed outcome"),
        NetworkedDfsWriteOutcome::Committed {
            intent_id: committed_intent_id,
            version,
            ..
        } if committed_intent_id == intent_id && version == version_2
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn empty_routed_write_uses_an_ordinary_value_call() {
    let requester =
        NodeId::from_legacy_named("node_empty_routed_requester").expect("requester node");
    let primary = NodeId::from_legacy_named("node_empty_routed_primary").expect("primary node");
    let root = temp_root("empty-routed-value-call");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut service = NetworkedDfsService::new(
        RecordingRoutedCallTransport {
            local_node_id: requester.clone(),
            calls: Arc::clone(&calls),
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    );
    let path = "/users/empty-routed-value";
    let object =
        crate::posix_object::new_posix_metadata_object(&requester, path, DfsObjectKind::File);
    let target_version = crate::namespace::next_version(&object.version);
    let shard = DfsMetadataShardConfig {
        file_system_id: object.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![requester.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    let outcome = service
        .route_distributed_bytes(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: requester.clone(),
            },
            Some(object.version.clone()),
            Vec::new(),
            vec![requester],
            DfsReplicationPolicy::new(1, 1).expect("replication policy"),
            object,
            shard,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                creation: DfsWriteCreation::Exclusive,
                repair: DfsWriteRepair::Foreground,
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: false,
                operation_id: None,
            },
        )
        .expect("route empty write");
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));

    let calls = calls.lock().expect("recorded routed calls");
    assert_eq!(calls.len(), 1);
    let (target_node_id, call) = &calls[0];
    assert_eq!(target_node_id, &primary);
    assert!(
        call.attachment.is_none(),
        "zero-byte content must not produce an attachment-shaped RPC head"
    );
    let DfsNetworkMessage::ExclusiveCreateRequest(command) = &call.message.message else {
        panic!(
            "expected routed exclusive create, got {:?}",
            call.message.message
        );
    };
    assert!(command.bytes.is_empty());
    assert_eq!(command.content_hash, None);
    assert_eq!(command.content_size_bytes, None);
    assert!(!command.requester_blob_pre_staged);
    assert!(command.exclusive_create);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_routed_strict_write_prefers_metadata_primary_replica() {
    let root = temp_root("posix-routed-strict-prefers-primary");
    let writer =
        NodeId::from_legacy_named("node_posix_routed_primary_pref_writer").expect("writer");
    let primary =
        NodeId::from_legacy_named("node_posix_routed_primary_pref_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_posix_routed_primary_pref_replica").expect("replica");
    let file_system = file_system(primary.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![writer.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let captured_desired = Arc::new(Mutex::new(None::<Vec<NodeId>>));
    let captured_desired_for_sender = Arc::clone(&captured_desired);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(command) = &command.routed_message.message
        {
            *captured_desired_for_sender.lock().expect("capture") =
                Some(command.desired_replica_nodes.clone());
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/routed-primary-preferred.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata = seeded_durable_metadata_replica(
        root.join("writer-metadata"),
        vec![shard],
        &file_system,
        &object,
    );
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach writer metadata replica");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    dfs.posix_write_file(
        "/routed-primary-preferred.txt",
        b"strict routed POSIX write prefers primary",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            replica_candidates: vec![writer.clone(), replica.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    )
    .expect("pending routed write returns stat for existing object");

    assert_eq!(
        captured_desired.lock().expect("capture").clone(),
        Some(vec![writer, primary]),
        "routed strict POSIX write should choose the metadata primary as the second replica"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_routed_write_sends_attachment_without_local_persistence() {
    let root = temp_root("posix-routed-value");
    let writer = NodeId::from_legacy_named("node_posix_routed_inline_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_posix_routed_inline_primary").expect("primary");
    let file_system = file_system(primary.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![writer.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let captured_command = Arc::new(Mutex::new(None::<DfsDistributedWriteCommand>));
    let captured_command_for_sender = Arc::clone(&captured_command);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/routed-value.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata = seeded_durable_metadata_replica(
        root.join("writer-metadata"),
        vec![shard],
        &file_system,
        &object,
    );
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach writer metadata replica");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let bytes = b"materialized routed POSIX bytes".to_vec();
    let expected_hash = content_hash(&bytes);
    dfs.posix_write_file(
        "/routed-value.txt",
        &bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            replica_candidates: vec![writer.clone(), primary.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    )
    .expect("pending routed write returns stat for existing object");

    let command = captured_command
        .lock()
        .expect("capture")
        .clone()
        .expect("captured routed write command");
    assert_eq!(command.bytes, bytes);
    assert_eq!(command.content_hash, None);
    assert_eq!(command.content_size_bytes, None);
    assert_eq!(command.desired_replica_nodes, vec![writer.clone(), primary]);
    assert!(
        !dfs.content
            .has_blob(&expected_hash)
            .expect("writer blob probe"),
        "the remote-primary sender must not persist the payload before streaming it"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn staged_write_routes_keep_their_durable_requester_in_the_replica_plan() {
    for mode in ["reader", "conditional", "bytes"] {
        let conditional = mode == "conditional";
        let root = temp_root("posix-staged-reader-value");
        let writer = NodeId::from_legacy_named("node_posix_staged_reader_writer").expect("writer");
        let primary =
            NodeId::from_legacy_named("node_posix_staged_reader_primary").expect("primary");
        let file_system = file_system(primary.clone());
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![writer.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control_plane.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
                .expect("filesystem config");
            control.put_metadata_shard(&shard).expect("metadata shard");
        });

        let captured_command = Arc::new(Mutex::new(None::<DfsDistributedWriteCommand>));
        let captured_command_for_sender = Arc::clone(&captured_command);
        let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
            if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
                &command.routed_message.message
            {
                *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
            }
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let transport =
            node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
        let mut object = file_metadata(primary.clone());
        object.file_system_id = file_system.file_system_id.clone();
        object.path = "/routed-value.txt".to_string();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![writer.clone()];
        let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
        if mode == "bytes" {
            let previous = writer_content
                .put_object_version(object.object_id.as_str(), object.version.as_str(), b"base")
                .expect("seed existing file content");
            object.content_hash = Some(previous.content_hash);
            object.content_size_bytes = Some(previous.size_bytes);
        }
        let metadata = seeded_durable_metadata_replica(
            root.join("writer-metadata"),
            vec![shard],
            &file_system,
            &object,
        );
        let mut dfs = NetworkedDfsService::new(transport, writer_content)
            .with_control_plane(control_plane)
            .with_metadata_backend(metadata)
            .expect("attach writer metadata replica");
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

        let bytes = b"durable staged CAS bytes".to_vec();
        let expected_hash = content_hash(&bytes);
        let mut options = DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            replica_candidates: vec![writer.clone(), primary.clone()],
            wait_for_commit: false,
            foreground_replica_repair: false,
        };
        if mode == "bytes" {
            options.policy.required_node_ids.push(writer.clone());
            dfs.posix_write_file("/routed-value.txt", &bytes, options)
        } else if conditional {
            dfs.posix_replace_file_if_version_from_reader_with_operation_id(
                "/routed-value.txt",
                &object.object_id,
                &object.version,
                std::io::Cursor::new(bytes.clone()),
                options,
                "stable-staged-cas",
            )
        } else {
            dfs.posix_write_file_from_reader(
                "/routed-value.txt",
                std::io::Cursor::new(bytes.clone()),
                options,
            )
        }
        .expect("pending routed write returns stat for existing object");

        let command = captured_command
            .lock()
            .expect("capture")
            .clone()
            .expect("captured routed write command");
        assert!(command.requester_blob_pre_staged);
        assert!(
            command.metadata_journal_after_sequence.is_some(),
            "public reader writes retain namespace replay"
        );
        assert!(command.bytes.is_empty());
        assert_eq!(command.content_hash.as_ref(), Some(&expected_hash));
        if conditional {
            assert_eq!(command.desired_replica_nodes, vec![writer]);
        } else {
            // Ordinary writes also apply the filesystem's managed replica
            // policy, which can require additional copies.
            assert!(command.desired_replica_nodes.contains(&writer));
        }
        assert!(
            dfs.content.has_blob(&expected_hash).unwrap(),
            "the declared replica actually owns the durable bytes"
        );

        std::fs::remove_dir_all(root).ok();
    }
}

#[test]
fn internal_cas_requires_commit_without_requesting_namespace_replay() {
    let root = temp_root("internal-cas-no-replay");
    let writer = NodeId::from_legacy_named("node_internal_cas_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_internal_cas_primary").expect("primary");
    let file_system = file_system(primary.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![writer.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/routed-value.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let lookup_object = object.clone();
    let captured_command = Arc::new(Mutex::new(None::<DfsDistributedWriteCommand>));
    let captured_command_for_sender = Arc::clone(&captured_command);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
            return Err(DfsPeerTransportError::with_delivery_state(
                "simulated unavailable authority",
                Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent),
            ));
        }
        if let DfsNetworkMessage::NamespaceRequest(request) = &command.routed_message.message {
            if let DfsNamespaceOperation::LookupPaths { paths } = &request.operation {
                return Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: vec![DfsRoutedMessage {
                        source_node_id: command.routed_message.target_node_id.clone(),
                        target_node_id: command.routed_message.source_node_id.clone(),
                        frame_id: command.routed_message.frame_id,
                        message_id: command.routed_message.message_id,
                        message: DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                            request_id: request.request_id.clone(),
                            result: DfsNamespaceResult::Applied,
                            synced_path_object: None,
                            synced_path_objects: if paths.contains(&lookup_object.path) {
                                vec![lookup_object.clone()]
                            } else {
                                vec![]
                            },
                            metadata_journal_batches: vec![],
                            metadata_journal_has_more: Some(false),
                            posix_coordination_result: None,
                        }),
                    }],
                });
            }
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata = seeded_durable_metadata_replica(
        root.join("writer-metadata"),
        vec![shard],
        &file_system,
        &object,
    );
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach writer metadata replica");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let bytes = b"durable staged CAS bytes".to_vec();
    let expected_hash = content_hash(&bytes);
    dfs.posix_write_internal_file_from_reader(
        "/routed-value.txt",
        std::io::Cursor::new(bytes.clone()),
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![writer.clone(), primary.clone()],
            wait_for_commit: false,
            foreground_replica_repair: false,
        },
        Some((&object.object_id, &object.version)),
        "internal-cas-no-replay",
    )
    .expect_err("an internal write must not claim success without the authority's commit receipt");

    let command = captured_command
        .lock()
        .expect("capture")
        .clone()
        .expect("captured routed write command");
    assert!(command.requester_blob_pre_staged);
    assert_eq!(command.metadata_journal_after_sequence, None);
    assert!(command.bytes.is_empty());
    assert_eq!(command.content_hash.as_ref(), Some(&expected_hash));
    assert_eq!(command.desired_replica_nodes, vec![writer]);
    assert!(
        dfs.content.has_blob(&expected_hash).unwrap(),
        "the declared replica actually owns the durable bytes"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_routed_multimegabyte_write_uses_same_attachment_path() {
    let root = temp_root("posix-routed-multimegabyte-value");
    let writer = NodeId::from_legacy_named("node_posix_routed_manifest_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_posix_routed_manifest_primary").expect("primary");
    let file_system = file_system(primary.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![writer.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let captured_command = Arc::new(Mutex::new(None::<DfsDistributedWriteCommand>));
    let captured_command_for_sender = Arc::clone(&captured_command);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/routed-multimegabyte-value.bin".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata = seeded_durable_metadata_replica(
        root.join("writer-metadata"),
        vec![shard],
        &file_system,
        &object,
    );
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach writer metadata replica");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let bytes = vec![0x57; 2 * 1024 * 1024 + 8192];
    let expected_hash = content_hash(&bytes);
    dfs.posix_write_file(
        "/routed-multimegabyte-value.bin",
        &bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            replica_candidates: vec![writer.clone(), primary.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    )
    .expect("pending routed write returns stat for existing object");

    let command = captured_command
        .lock()
        .expect("capture")
        .clone()
        .expect("captured routed write command");
    assert_eq!(command.bytes, bytes);
    assert_eq!(command.content_hash, None);
    assert_eq!(command.content_size_bytes, None);
    assert_eq!(command.desired_replica_nodes, vec![writer, primary]);
    assert!(
        !dfs.content
            .has_blob(&expected_hash)
            .expect("writer blob probe"),
        "large remote-primary writes must stream without a sender-side blob copy"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_remote_primary_deferred_write_carries_streamed_payload() {
    let root = temp_root("posix-remote-primary-deferred-materialized");
    let writer = NodeId::from_legacy_named("node_posix_remote_deferred_materialized_writer")
        .expect("writer");
    let primary = NodeId::from_legacy_named("node_posix_remote_deferred_materialized_primary")
        .expect("primary");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![writer.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control.put_metadata_shard(&shard).expect("metadata shard");
    });

    let captured_command = Arc::new(Mutex::new(None::<DfsDistributedWriteCommand>));
    let captured_command_for_sender = Arc::clone(&captured_command);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let path = "/routed-deferred-materialized.bin";
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = path.to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata =
        seeded_durable_metadata_replica(root.join("metadata"), vec![shard], &file_system, &object);
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let bytes = b"deferred remote primary write uses the materialized value path".to_vec();
    let expected_hash = content_hash(&bytes);
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![writer.clone(), primary.clone()],
                wait_for_commit: false,
                foreground_replica_repair: false,
            },
        )
        .expect("pending routed write returns stat for existing object");

    assert_eq!(
        stat.content_hash, None,
        "the sender cannot report a content identity before the primary's one-pass ingest completes"
    );
    let command = captured_command
        .lock()
        .expect("capture")
        .clone()
        .expect("captured routed write command");
    assert_eq!(command.bytes, bytes);
    assert_eq!(
        command.content_hash, None,
        "materialized remote-primary POSIX writes should not require a follow-up blob fetch"
    );
    assert_eq!(command.content_size_bytes, None);
    assert_eq!(command.desired_replica_nodes, vec![writer, primary]);
    assert!(
        command.metadata_journal_after_sequence.is_some(),
        "deferred POSIX writes need bounded response journal replay when callers publish metadata"
    );
    assert!(
        dfs.flush_detached_transport_sends().is_ok(),
        "remote-primary POSIX writes must not leave detached sends behind"
    );
    assert!(
        !command.foreground_replica_repair,
        "deferred publish must preserve nonforeground repair mode"
    );
    assert!(
        !dfs.content
            .has_blob(&expected_hash)
            .expect("writer blob probe"),
        "deferred remote-primary writes must not first persist on the sender"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_read_is_routed_to_owner_node() {
    let root = temp_root("local-owned-read-routed-to-owner");
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut remote_dfs = NetworkedDfsService::new(
        network.add_node(remote.clone()),
        LocalContentStore::new(root.join("remote-content")).expect("remote content"),
    );
    let metadata = file_metadata(owner.clone());
    let object_id = metadata.object_id.clone();

    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner filesystem");
    remote_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("remote filesystem");
    owner_dfs
        .put_local_object_version(metadata, b"owned bytes")
        .expect("seed owner object");
    remote_dfs
        .add_object_metadata(
            owner_dfs
                .metadata()
                .object(&object_id)
                .expect("owner metadata")
                .clone(),
        )
        .expect("replicate metadata");

    assert_eq!(
        remote_dfs
            .request_remote_object_read_without_cache(&object_id)
            .expect("read routes"),
        vec![owner.clone()],
    );
    let request_id = remote_dfs
        .reads
        .pending_reads
        .keys()
        .next()
        .expect("pending owner read")
        .clone();

    let owner_events = owner_dfs.process_incoming().expect("owner handles read");
    assert!(matches!(
        &owner_events[..],
        [NetworkedDfsServiceEvent::RemoteReadServed {
            requester_node_id,
            result: DfsReadResult::Found(_),
            ..
        }] if requester_node_id == &remote
    ));

    let remote_events = remote_dfs
        .process_incoming()
        .expect("remote receives read response");
    assert!(matches!(
        &remote_events[..],
        [NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: received_request_id,
            source_node_id,
            result: DfsReadResult::Found(DfsReadData { bytes, .. }),
            ..
        }] if received_request_id == &request_id
            && source_node_id == &owner
            && bytes == b"owned bytes"
    ));
}

#[test]
fn replicated_eventual_write_is_routed_to_metadata_primary_and_committed() {
    let mut cluster = sifs_posix_cluster("routed-primary-write-commit");
    let path = "/users/routed-primary-write.txt";
    sifs_cluster_primary_posix_write(&mut cluster, path, b"old bytes");
    let original_hash = content_hash(b"old bytes");
    sifs_cluster_publish_metadata_until(&mut cluster, "seed writer metadata", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(original_hash.as_str()))
    });
    let original = cluster
        .writer
        .posix_stat(path)
        .expect("original writer stat");
    let object_id = original.object_id.clone();
    let target_version = crate::namespace::next_version(&original.version);
    let primary_node = cluster.primary_node.clone();
    let request = DfsWriteRequest {
        file_system_id: sifs_file_system_id(),
        object_id: object_id.clone(),
        target_version: target_version.clone(),
        requesting_node_id: cluster.writer_node.clone(),
    };
    assert_ne!(cluster.writer_node, primary_node);
    let outcome = sifs_cluster_writer_call_with_pump(&mut cluster, "routed overwrite", |writer| {
        writer.write_distributed_eventual(
            request,
            b"updated bytes".to_vec(),
            vec![primary_node],
            DfsReplicationPolicy::new(1, 1).expect("single-owner content policy"),
        )
    })
    .expect("write routes to metadata primary");
    if let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome {
        sifs_posix_cluster_pump_until(&mut cluster, "routed overwrite response", |cluster| {
            matches!(
                cluster.writer.write_outcome(&intent_id),
                Ok(NetworkedDfsWriteOutcome::Committed { .. })
            )
        });
    } else {
        assert!(matches!(
            outcome,
            NetworkedDfsWriteOutcome::Committed { .. }
        ));
    }
    assert_eq!(
        cluster
            .primary
            .read_local_object(&object_id)
            .expect("primary read"),
        Some(DfsReadData {
            object_id: object_id.clone(),
            version: target_version.clone(),
            content_hash: Some(content_hash(b"updated bytes")),
            bytes: b"updated bytes".to_vec(),
        }),
    );
    let writer_stat = cluster
        .writer
        .posix_stat(path)
        .expect("writer metadata updated");
    assert_eq!(writer_stat.object_id, object_id);
    assert_eq!(writer_stat.version, target_version);
    assert_eq!(
        writer_stat.content_hash.as_deref(),
        Some(content_hash(b"updated bytes").as_str()),
    );
}

#[test]
fn sharded_lazy_reader_refreshes_after_remote_primary_overwrite() {
    let root = temp_root("sifs-sharded-lazy-reader-remote-primary");
    let primary_node = NodeId::from_legacy_named("node_sifs_lazy_remote_primary").expect("primary");
    let writer_node = NodeId::from_legacy_named("node_sifs_lazy_remote_writer").expect("writer");
    let shards = sifs_two_primary_shard_configs(
        &primary_node,
        &primary_node,
        1,
        vec![writer_node.clone()],
        vec![writer_node.clone()],
    );
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary_node.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });
    let network = InProcessNetwork::new();
    let primary_root = root.join("primary");
    let writer_root = root.join("writer");
    let mut primary = NetworkedDfsService::new(
        network.add_node(primary_node.clone()),
        LocalContentStore::new(primary_root.join("content")).expect("primary content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend_lazy(
        open_local_sharded_metadata_backend(primary_root.join("metadata"), shards.clone())
            .expect("primary metadata"),
    )
    .expect("primary metadata backend");
    let mut writer = NetworkedDfsService::new(
        network.add_node(writer_node.clone()),
        LocalContentStore::new(writer_root.join("content")).expect("writer content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend_lazy(
        open_local_sharded_metadata_backend(writer_root.join("metadata"), shards.clone())
            .expect("writer metadata"),
    )
    .expect("writer metadata backend");
    primary
        .bootstrap_distributed_sifs()
        .expect("primary bootstrap");

    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("primary publish");
        primary.process_incoming().expect("primary pump");
        writer.process_incoming().expect("writer pump");
        if writer.posix_stat(SIFS_SERVICES_PATH).is_ok() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("writer did not learn SIFS services namespace");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    writer
        .bootstrap_distributed_sifs()
        .expect("writer bootstrap");

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let path = "/services/current-read-remote-primary.txt";
    let old = b"old remote primary reader payload".to_vec();
    let new = b"new remote primary reader payload".to_vec();
    writer
        .posix_write_file(
            path,
            &old,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("initial remote-primary write");

    let stale_metadata = writer.metadata().clone();
    let mut reader = NetworkedDfsService::new(
        network.add_node(writer_node.clone()),
        LocalContentStore::new(writer_root.join("content")).expect("reader content"),
    );
    for file_system in stale_metadata.filesystems.values().cloned() {
        reader
            .add_file_system(file_system)
            .expect("seed reader filesystem");
    }
    for object in stale_metadata.objects.values().cloned() {
        reader
            .add_object_metadata(object)
            .expect("seed reader metadata");
    }
    let mut reader = reader.with_control_plane(shared_kv);
    assert_eq!(
        reader
            .posix_read_file_distributed(path)
            .expect("reader old payload"),
        old
    );

    let replacement = writer
        .posix_write_file(
            path,
            &new,
            DfsWriteOptions::replicated_eventual(vec![writer_node]),
        )
        .expect("remote-primary overwrite");

    let mut stale_stream = reader
        .posix_open_file_stream(path)
        .expect("reader retained local stream");
    let mut stale = Vec::new();
    stale_stream
        .reader
        .read_to_end(&mut stale)
        .expect("read retained local stream");
    assert_eq!(stale, old, "the fixture must retain the pre-overwrite view");

    let mut current_stream = reader
        .posix_open_current_file_stream(path)
        .expect("open current local stream after authoritative lookup");
    assert_eq!(current_stream.object_id, replacement.object_id);
    assert_eq!(current_stream.version, replacement.version);
    let mut current = Vec::new();
    current_stream
        .reader
        .read_to_end(&mut current)
        .expect("read current local stream");
    assert_eq!(
        current, new,
        "current-metadata stream must keep using local exact-version content"
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_routed_create_is_read_your_write_on_writer() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-ryw");
    let path = "/users/read-your-write.txt";
    let bytes = b"hello from the writer node";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // Read-your-write through the strictly local read path: after the routed
    // write commits, the writer must see its own bytes without any further
    // network round trip.
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        bytes.to_vec()
    );
    let stat = cluster.writer.posix_stat(path).expect("writer stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );

    // The new entry must be listed in the parent directory on the writer.
    let entries = cluster
        .writer
        .posix_list_dir("/users")
        .expect("writer list /users");
    assert!(
        entries
            .iter()
            .any(|entry| entry.path == path && entry.kind == DfsObjectKind::File),
        "writer directory listing must contain the routed write: {entries:?}"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_routed_write_is_visible_on_primary() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-primary-vis");
    let path = "/users/primary-visibility.txt";
    let bytes = b"routed bytes visible on the primary";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // The shard primary serialized the write, so its metadata must already be
    // authoritative: correct kind, size, and content hash.
    let stat = cluster.primary.posix_stat(path).expect("primary stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );

    let entries = cluster
        .primary
        .posix_list_dir("/users")
        .expect("primary list /users");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "primary directory listing must contain the routed write: {entries:?}"
    );

    // The content was staged on the writer; the primary must be able to make
    // it locally readable through the remote-read path.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(read, bytes.to_vec());

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_routed_overwrite_advances_version_monotonically() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-overwrite");
    let path = "/users/overwrite.txt";
    let first = b"first version".to_vec();
    let second = b"the second, longer version of the content".to_vec();
    let third = b"3rd".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &first);
    let first_stat = cluster.writer.posix_stat(path).expect("first stat");

    sifs_cluster_routed_posix_write(&mut cluster, path, &second);
    let second_stat = cluster.writer.posix_stat(path).expect("second stat");
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("second read"),
        second
    );
    assert_eq!(second_stat.size_bytes, Some(second.len() as u64));
    assert!(
        second_stat.version > first_stat.version,
        "overwrite must advance the visible version: {:?} -> {:?}",
        first_stat.version,
        second_stat.version
    );
    assert!(
        second_stat.modified_at_unix_nanos >= first_stat.modified_at_unix_nanos,
        "overwrite must not move mtime backwards"
    );

    // Shrinking overwrite: size must follow the latest committed content.
    sifs_cluster_routed_posix_write(&mut cluster, path, &third);
    let third_stat = cluster.writer.posix_stat(path).expect("third stat");
    assert_eq!(third_stat.size_bytes, Some(third.len() as u64));
    assert!(third_stat.version > second_stat.version);
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("third read"),
        third
    );

    // The primary observes the same latest version.
    let primary_stat = cluster.primary.posix_stat(path).expect("primary stat");
    assert_eq!(
        primary_stat.content_hash.as_deref(),
        Some(content_hash(&third).as_str())
    );
    assert_eq!(primary_stat.size_bytes, Some(third.len() as u64));

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_routed_create_updates_parent_directory_mtime_cluster_wide() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-parent-mtime");
    let parent = "/users";
    let path = "/users/parent-mtime.txt";
    let bytes = b"parent mtime must advance everywhere";

    let writer_parent_before = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat before");

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // POSIX: creating an entry updates the parent directory mtime. The shard
    // primary journals that update, so the writer must observe it through the
    // routed-write response replay without appending to its own journal.
    let primary_parent = cluster
        .primary
        .posix_stat(parent)
        .expect("primary parent stat");
    sifs_posix_cluster_pump_until(&mut cluster, "writer parent mtime replay", |cluster| {
        cluster
            .writer
            .posix_stat(parent)
            .is_ok_and(|stat| stat.modified_at_unix_nanos == primary_parent.modified_at_unix_nanos)
    });
    let writer_parent = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat after");
    assert!(
        writer_parent.modified_at_unix_nanos > writer_parent_before.modified_at_unix_nanos,
        "routed create must advance the parent directory mtime on the writer"
    );
    assert_eq!(
        writer_parent.version, primary_parent.version,
        "writer must replay the primary's parent directory record, not invent its own"
    );

    // The observer converges to the identical parent record after publication.
    sifs_cluster_publish_metadata_until(&mut cluster, "observer parent mtime", |cluster| {
        cluster.observer.posix_stat(parent).is_ok_and(|stat| {
            stat.modified_at_unix_nanos == primary_parent.modified_at_unix_nanos
                && stat.version == primary_parent.version
        })
    });

    assert!(
        cluster.rejections.is_empty(),
        "parent mtime propagation must not fork any replica journal: {:?}",
        cluster.rejections
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_retried_pending_routed_create_preserves_parent_mtime_touch() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-retried-create-parent-mtime");
    let parent = "/users";
    let path = "/users/retried-parent-mtime.txt";
    let bytes = b"retried pending create must retain parent touch";
    let writer_parent_before = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat before");
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(3, 3).expect("three-receipt policy"),
        replica_candidates: vec![
            cluster.writer_node.clone(),
            cluster.observer_node.clone(),
            cluster.primary_node.clone(),
        ],
        wait_for_commit: false,
        foreground_replica_repair: true,
    };
    match cluster.writer.posix_write_file(path, bytes, options) {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed POSIX create failed: {error}"),
    }
    let command = cluster
        .writer
        .writes
        .pending_routed_writes
        .values()
        .next()
        .expect("pending routed create")
        .command
        .clone()
        .expect("pending routed create command");

    cluster
        .primary
        .process_incoming()
        .expect("primary handles original create");
    let original_pending = cluster
        .primary
        .writes
        .pending_remote_write_responses
        .values()
        .next()
        .expect("primary pending response after original");
    assert_eq!(original_pending.created_object_path.as_deref(), Some(path));

    assert!(
        cluster
            .writer
            .send_dfs_message_if_routable(
                &cluster.primary_node,
                DfsNetworkMessage::DistributedWriteRequest(command),
            )
            .expect("redispatch identical routed create")
    );
    cluster
        .primary
        .process_incoming()
        .expect("primary handles retried create");
    let retried_pending = cluster
        .primary
        .writes
        .pending_remote_write_responses
        .values()
        .next()
        .expect("primary pending response after retry");
    assert_eq!(
        retried_pending.created_object_path.as_deref(),
        Some(path),
        "a response retry must preserve the original create's parent-touch obligation"
    );

    let expected_hash = content_hash(bytes);
    sifs_posix_cluster_pump_until(&mut cluster, "retried routed create", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });
    let primary_parent = cluster
        .primary
        .posix_stat(parent)
        .expect("primary parent stat after retry");
    sifs_posix_cluster_pump_until(&mut cluster, "retried create parent replay", |cluster| {
        cluster
            .writer
            .posix_stat(parent)
            .is_ok_and(|stat| stat.modified_at_unix_nanos == primary_parent.modified_at_unix_nanos)
    });
    let writer_parent = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat after retry");
    assert!(
        writer_parent.modified_at_unix_nanos > writer_parent_before.modified_at_unix_nanos,
        "retried routed create must advance the parent directory mtime"
    );
    assert_eq!(writer_parent.version, primary_parent.version);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_routed_namespace_ops_have_read_your_write_semantics() {
    // Namespace mutations from a non-primary node route to the metadata shard
    // primary and replay the primary's journal before returning, so every
    // blocking POSIX call observes its own effect locally.
    let cluster = sifs_posix_cluster("sifs-cluster-ns-ryw");
    let SifsPosixCluster {
        root,
        primary_node: _,
        writer_node,
        observer_node,
        primary,
        mut writer,
        mut observer,
        rejections: _,
    } = cluster;

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    // mkdir routes and is immediately visible locally.
    let dir_stat = writer
        .posix_mkdir("/users/routed-dir")
        .expect("routed mkdir");
    assert_eq!(dir_stat.kind, DfsObjectKind::Directory);
    assert!(
        writer
            .posix_list_dir("/users")
            .expect("writer list /users")
            .iter()
            .any(|entry| entry.path == "/users/routed-dir"),
        "routed mkdir must be locally listed after it returns"
    );

    let attrs_dir = writer
        .posix_mkdir_with_create_attrs(
            "/users/routed-attrs",
            DfsPosixCreateAttrs::new(0o2750, 4101, 4201),
        )
        .expect("routed mkdir with attrs");
    assert_eq!(
        (attrs_dir.mode, attrs_dir.uid, attrs_dir.gid),
        (0o2750, 4101, 4201)
    );
    // Give the second creator a parent snapshot, then leave it deliberately
    // stale while the first creator publishes the children. A distributed
    // stat returns the authoritative value but intentionally does not mutate
    // the requester's metadata cache, so seed the already-committed parent as
    // a replica journal replay would.
    let observer_parent = writer
        .lookup_sifs_path("/users/routed-attrs")
        .expect("lookup routed attrs parent")
        .expect("routed attrs parent metadata");
    let observer_parent_id = observer_parent.object_id.clone();
    observer
        .add_transient_object_metadata(observer_parent)
        .expect("seed stale creator parent snapshot");
    observer
        .transient_metadata_object_ids
        .remove(&observer_parent_id);
    let attrs_child_dir = writer
        .posix_mkdir_with_create_attrs(
            "/users/routed-attrs/child",
            DfsPosixCreateAttrs::new(0o750, 4104, 4204),
        )
        .expect("routed child mkdir with attrs");
    assert_eq!(
        (
            attrs_child_dir.mode,
            attrs_child_dir.uid,
            attrs_child_dir.gid
        ),
        (0o2750, 4104, 4201)
    );
    let attrs_file = writer
        .posix_write_file_with_create_attrs(
            "/users/routed-attrs/file",
            b"attrs",
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            DfsPosixCreateAttrs::new(0o640, 4102, 4202),
        )
        .expect("routed file with attrs");
    assert_eq!(
        (attrs_file.mode, attrs_file.uid, attrs_file.gid),
        (0o640, 4102, 4201)
    );
    let raced_file_create = observer.posix_create_file_with_attrs(
        "/users/routed-attrs/file",
        DfsWriteOptions::replicated_eventual(vec![observer_node.clone()]),
        DfsPosixCreateAttrs::new(0o600, 5102, 5202),
    );
    assert!(
        matches!(raced_file_create, Err(DfsPosixError::AlreadyExists { .. })),
        "stale routed exclusive create must lose at the metadata primary: {raced_file_create:?}"
    );
    let parent_before_symlink = writer
        .posix_set_times("/users/routed-attrs", None, Some(123_000_000))
        .expect("set routed parent mtime before symlink")
        .modified_at_unix_nanos;
    let attrs_link = writer
        .posix_symlink_with_create_attrs(
            "file",
            "/users/routed-attrs/link",
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            DfsPosixCreateAttrs::new(0o777, 4103, 4203),
        )
        .expect("routed symlink with attrs");
    assert_eq!(
        (attrs_link.mode, attrs_link.uid, attrs_link.gid),
        (0o777, 4103, 4201)
    );
    assert!(matches!(
        observer.posix_symlink_with_create_attrs(
            "other",
            "/users/routed-attrs/link",
            DfsWriteOptions::replicated_eventual(vec![observer_node]),
            DfsPosixCreateAttrs::new(0o777, 5103, 5203),
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    let parent_after_symlink = writer
        .posix_stat("/users/routed-attrs")
        .expect("routed parent after symlink")
        .modified_at_unix_nanos;
    assert!(
        parent_after_symlink > parent_before_symlink,
        "routed symlink create must journal and replay its parent mtime update"
    );

    // mkdir_all creates the whole chain through the primary.
    writer
        .posix_mkdir_all("/users/routed-dir/a/b")
        .expect("routed mkdir_all");
    assert_eq!(
        writer
            .posix_stat("/users/routed-dir/a/b")
            .expect("nested dir stat")
            .kind,
        DfsObjectKind::Directory
    );

    // Content write into the routed directory (already-routed write path).
    let bytes = b"namespace routed file content".to_vec();
    let file_stat = writer
        .posix_write_file(
            "/users/routed-dir/file.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("routed content write");
    assert_eq!(
        file_stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert!(
        writer
            .posix_list_dir("/users/routed-dir")
            .expect("writer list routed dir after write")
            .iter()
            .any(|entry| entry.path == "/users/routed-dir/file.txt"),
        "routed content write must be locally listed after it returns"
    );

    // chmod / utimens route and read back their own effect.
    let mode_stat = writer
        .posix_set_mode("/users/routed-dir/file.txt", 0o600)
        .expect("routed set_mode");
    assert_eq!(mode_stat.mode, 0o600);
    assert_eq!(
        writer
            .posix_stat("/users/routed-dir/file.txt")
            .expect("stat after chmod")
            .mode,
        0o600
    );
    let times_stat = writer
        .posix_set_times(
            "/users/routed-dir/file.txt",
            Some(123_000_000_000),
            Some(456_000_000_000),
        )
        .expect("routed set_times");
    assert_eq!(times_stat.accessed_at_unix_nanos, 123_000_000_000);
    assert_eq!(times_stat.modified_at_unix_nanos, 456_000_000_000);

    // rename routes; the destination is readable locally right away because
    // the content was staged here and the metadata replayed.
    let renamed_stat = writer
        .posix_rename(
            "/users/routed-dir/file.txt",
            "/users/routed-dir/renamed.txt",
        )
        .expect("routed rename");
    assert_eq!(
        renamed_stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(
        writer
            .posix_read_file("/users/routed-dir/renamed.txt")
            .expect("read renamed"),
        bytes
    );
    assert!(matches!(
        writer.posix_stat("/users/routed-dir/file.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    // A canonical renamed file can be unlinked immediately after the caller
    // observes the rename result. This is the same shape as the cluster
    // read-your-write probe: write -> rename -> read -> unlink.
    let delete_bytes = b"rename then delete canonical file".to_vec();
    writer
        .posix_write_file(
            "/users/routed-dir/delete-source.txt",
            &delete_bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("routed delete-source write");
    let delete_renamed = "/users/routed-dir/delete-renamed.txt";
    writer
        .posix_rename("/users/routed-dir/delete-source.txt", delete_renamed)
        .expect("routed rename before canonical unlink");
    assert_eq!(
        writer
            .posix_read_file(delete_renamed)
            .expect("read renamed before canonical unlink"),
        delete_bytes
    );
    writer
        .posix_unlink(delete_renamed)
        .expect("unlink canonical renamed file");
    assert!(matches!(
        writer.posix_stat(delete_renamed),
        Err(DfsPosixError::NotFound { .. })
    ));

    // hardlink routes and updates the link count everywhere it is observed.
    let link_stat = writer
        .posix_hardlink("/users/routed-dir/renamed.txt", "/users/routed-dir/link")
        .expect("routed hardlink");
    assert_eq!(link_stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(
        writer
            .posix_stat("/users/routed-dir/renamed.txt")
            .expect("stat canonical after hardlink")
            .link_count,
        2
    );

    // unlink routes; the alias disappears and the link count drops.
    let unlinked = writer
        .posix_unlink("/users/routed-dir/link")
        .expect("routed unlink");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        writer.posix_stat("/users/routed-dir/link"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        writer
            .posix_stat("/users/routed-dir/renamed.txt")
            .expect("stat after alias unlink")
            .link_count,
        1
    );

    // POSIX error mapping survives the wire round trip.
    assert!(matches!(
        writer.posix_mkdir("/users/routed-dir"),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert!(matches!(
        writer.posix_rmdir("/users/routed-dir"),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert!(matches!(
        writer.posix_unlink("/users/routed-dir"),
        Err(DfsPosixError::IsDirectory { .. })
    ));
    assert!(matches!(
        writer.posix_unlink("/users/routed-missing.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        writer.posix_rename("/users/routed-dir", "/users/routed-dir/inside"),
        Err(DfsPosixError::InvalidPath { .. })
    ));

    // Recursive clear routes, then the emptied directory can be removed.
    writer
        .posix_clear_dir_recursive("/users/routed-dir")
        .expect("routed recursive clear");
    assert!(
        writer
            .posix_list_dir("/users/routed-dir")
            .expect("list cleared dir")
            .is_empty(),
        "recursive clear must empty the directory on the requester"
    );
    writer
        .posix_rmdir("/users/routed-dir")
        .expect("routed rmdir after clear");
    assert!(matches!(
        writer.posix_stat("/users/routed-dir"),
        Err(DfsPosixError::NotFound { .. })
    ));

    assert!(
        writer.pending_routed_namespace_ops.is_empty(),
        "settled routed namespace ops must not leak pending state"
    );

    // The primary holds the same authoritative view.
    stop_primary.store(true, Ordering::SeqCst);
    let mut primary = primary_thread.join().expect("primary thread");
    assert!(matches!(
        primary.posix_stat("/users/routed-dir"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        primary
            .posix_stat("/users/routed-attrs")
            .expect("primary routed symlink parent")
            .modified_at_unix_nanos,
        parent_after_symlink,
        "requester and metadata primary must agree on the routed symlink parent generation"
    );

    // Journals stayed aligned: publication to both replicas applies cleanly.
    let mut rejections = Vec::new();
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("publish after routed namespace ops");
        pump_dfs_services_collect_rejections(
            &mut [
                ("primary", &mut primary),
                ("writer", &mut writer),
                ("observer", &mut observer),
            ],
            &mut rejections,
        );
        if matches!(
            observer.posix_stat("/users/routed-dir"),
            Err(DfsPosixError::NotFound { .. })
        ) && observer
            .posix_stat("/users/routed-dir/renamed.txt")
            .is_err()
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "observer did not converge after routed namespace ops; rejections: {rejections:?}"
            );
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    assert!(
        rejections.is_empty(),
        "routed namespace ops must keep replica journals aligned: {rejections:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_mkdir_all_remote_primary_falls_back_when_local_chain_exists() {
    let root = temp_root("sifs-mkdir-all-local-chain");
    let local_node =
        NodeId::from_legacy_named("node_sifs_mkdir_all_local_chain_local").expect("local");
    let remote_primary =
        NodeId::from_legacy_named("node_sifs_mkdir_all_local_chain_remote").expect("remote");
    let shards = sifs_two_primary_shard_configs(
        &local_node,
        &remote_primary,
        1,
        vec![remote_primary.clone()],
        vec![local_node.clone()],
    );
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                local_node.clone(),
            )))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("shard config");
        }
    });

    let network = InProcessNetwork::new();
    let mut local = NetworkedDfsService::new(
        network.add_node(local_node.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), shards)
            .expect("sharded metadata"),
    )
    .expect("metadata backend");
    local
        .bootstrap_distributed_sifs()
        .expect("bootstrap local mirror");
    for stale_path in [SIFS_SERVICES_PATH, "/services/dfs-smoke"] {
        let stale = crate::posix_object::new_posix_metadata_object(
            &local_node,
            stale_path,
            DfsObjectKind::Directory,
        );
        let stale_id = stale.object_id.clone();
        local
            .add_transient_object_metadata(stale)
            .expect("seed stale local replicated chain");
        local.transient_metadata_object_ids.remove(&stale_id);
    }

    local
        .posix_mkdir_all("/services/dfs-smoke")
        .expect("idempotent mkdir_all should not require remote primary");
    local
        .posix_mkdir_all_without_local_replay("/services/dfs-smoke")
        .expect("idempotent mkdir_all without replay should not require remote primary");

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_mkdir_all_allows_immediate_child_mkdir() {
    let cluster = sifs_two_primary_cluster("sifs-routed-mkdir-all-child");
    let SifsTwoPrimaryCluster {
        root,
        users_primary,
        services_primary,
        mut replica,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let base = "/services/cluster-tests";
    let child = "/services/cluster-tests/dir-race-local";
    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all base");
    replica
        .posix_mkdir(child)
        .expect("immediate child mkdir sees routed mkdir_all parent");
    assert!(replica.posix_stat(base).is_ok());
    assert!(replica.posix_stat(child).is_ok());

    stop.store(true, Ordering::SeqCst);
    let _users_primary = users_thread.join().expect("users thread");
    let _services_primary = services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_lookup_under_split_prefix_uses_exact_path_primary() {
    let cluster = sifs_two_primary_cluster("sifs-routed-lookup-split-prefix");
    let SifsTwoPrimaryCluster {
        root,
        mut services_primary,
        mut replica,
        services_primary_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let dir = "/services/cluster-tests/latency-regression";
    let path = "/services/cluster-tests/latency-regression/tiny.txt";
    let bytes = b"split-prefix routed lookup".to_vec();
    services_primary
        .posix_mkdir_all(dir)
        .expect("services primary mkdir_all dir");
    services_primary
        .posix_write_file(
            path,
            &bytes,
            sifs_cluster_write_options(&services_primary_node),
        )
        .expect("services primary write file");

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    crate::posix::reset_posix_metadata_raw_lookup_count();
    let stat = replica
        .posix_stat_distributed(path)
        .expect("distributed stat follows exact path primary");
    let component_count = path.trim_matches('/').split('/').count();
    let stat_lookup_count = crate::posix::posix_metadata_raw_lookup_count();
    let stat_lookup_bound = component_count + 4;
    assert!(
        stat_lookup_count <= stat_lookup_bound,
        "distributed stat performed {stat_lookup_count} raw metadata lookups for {component_count} path components; expected no more than {stat_lookup_bound} including root and fixed authority checks"
    );
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    crate::posix::reset_posix_metadata_raw_lookup_count();
    crate::service_read::reset_remote_metadata_range_read_request_count();
    let prefix = replica
        .posix_read_file_prefix_distributed(path, bytes.len())
        .expect("distributed prefix read after exact-primary stat");
    assert_eq!(prefix.bytes, bytes);
    assert!(!prefix.truncated);
    assert_eq!(
        crate::service_read::remote_metadata_range_read_request_count(),
        0,
        "the authoritative lookup should return a small local range without a second content RPC"
    );
    let read_lookup_count = crate::posix::posix_metadata_raw_lookup_count();
    let read_lookup_bound = component_count + 6;
    assert!(
        read_lookup_count <= read_lookup_bound,
        "distributed read performed {read_lookup_count} raw metadata lookups for {component_count} path components; expected no more than {read_lookup_bound} including root and fixed authority/content checks"
    );

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    assert_eq!(
        services_primary
            .posix_stat(path)
            .expect("services primary stat")
            .size_bytes,
        Some(bytes.len() as u64)
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_child_create_rejects_parent_removed_on_primary() {
    exercise_routed_child_create_with_removed_parent(true);
}

#[test]
fn prestaged_byte_create_rejects_parent_removed_on_primary() {
    exercise_routed_child_create_with_removed_parent(false);
}

fn exercise_routed_child_create_with_removed_parent(foreground_replica_repair: bool) {
    let mut cluster = sifs_two_primary_cluster(&format!(
        "sifs-routed-create-removed-parent-{foreground_replica_repair}"
    ));
    let dir = "/services/cluster-tests/stale-parent";
    let child = "/services/cluster-tests/stale-parent/child.txt";
    cluster
        .services_primary
        .posix_mkdir_all(dir)
        .expect("services primary creates parent");
    sifs_two_primary_publish_until(&mut cluster, "stale writer sees parent", |cluster| {
        cluster.replica.posix_stat(dir).is_ok()
    });
    cluster
        .services_primary
        .posix_rmdir(dir)
        .expect("services primary removes parent");
    assert!(matches!(
        cluster.services_primary.posix_stat(dir),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        cluster.replica.posix_stat(dir).is_ok(),
        "replica must still have stale parent metadata before routed write"
    );
    let queued_before = cluster
        .services_primary
        .transport
        .pending_incoming_count()
        .expect("primary queue before browser parent check");
    crate::browser_service::prepare_sifs_browser_write_parent(&mut cluster.replica, child)
        .expect("cached directory skips recursive creation only");
    assert_eq!(
        cluster
            .services_primary
            .transport
            .pending_incoming_count()
            .expect("primary queue after browser parent check"),
        queued_before,
        "the browser must not send a redundant MkdirAll for a cached parent"
    );

    let mut options = sifs_cluster_write_options(&cluster.replica_node.clone());
    options.foreground_replica_repair = foreground_replica_repair;
    let create = if foreground_replica_repair {
        cluster.replica.posix_write_file(child, b"child", options)
    } else {
        options.wait_for_commit = true;
        let replica = &mut cluster.replica;
        let primary = &mut cluster.services_primary;
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel();
            scope.spawn(move || {
                tx.send(replica.posix_write_file(child, b"child", options))
                    .expect("return rejected prestaged create");
            });
            let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
            let mut observed_prestage = false;
            loop {
                let messages = primary.transport.drain_incoming_messages().unwrap();
                for message in &messages {
                    if let DfsNetworkMessage::DistributedWriteRequest(command) = &message.message {
                        assert!(command.requester_blob_pre_staged);
                        observed_prestage = true;
                    }
                }
                primary
                    .process_routed_messages_with_options(messages, false)
                    .unwrap();
                if let Ok(result) = rx.try_recv() {
                    assert!(
                        observed_prestage,
                        "test must reach primary with a staged manifest"
                    );
                    break result;
                }
                assert!(
                    Instant::now() < deadline,
                    "prestaged parent rejection timed out"
                );
                std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
            }
        })
    };
    assert!(
        matches!(
            create,
            Err(DfsPosixError::PendingWrite { .. })
                | Err(DfsPosixError::MissingParent { .. })
                | Err(DfsPosixError::NotFound { .. })
                | Err(DfsPosixError::Dfs(_))
        ),
        "stale routed child create must not commit: {create:?}"
    );

    let request_id = cluster
        .replica
        .writes
        .pending_routed_writes
        .keys()
        .next()
        .cloned();
    if !foreground_replica_repair {
        assert!(
            cluster
                .replica
                .content
                .has_blob(&content_hash(b"child"))
                .unwrap()
        );
    }
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    let routed_error = loop {
        let Some(request_id) = request_id.as_deref() else {
            break None;
        };
        match cluster.replica.write_outcome(request_id) {
            Err(error) => break Some(error),
            Ok(NetworkedDfsWriteOutcome::Pending { .. }) => {}
            Ok(outcome) => panic!("stale routed child create unexpectedly settled: {outcome:?}"),
        }
        let _ = cluster
            .services_primary
            .process_incoming()
            .expect("services primary handles routed write");
        let _ = cluster
            .replica
            .process_incoming()
            .expect("replica handles routed write response");
        if Instant::now() >= deadline {
            panic!(
                "stale routed child create did not settle; pending={:?}",
                cluster.replica.writes.pending_routed_writes
            );
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    };
    if let Some(error) = routed_error {
        assert!(
            error
                .to_string()
                .contains("parent directory does not exist"),
            "unexpected stale routed child create rejection: {error}"
        );
    }
    assert!(matches!(
        cluster.services_primary.posix_stat(child),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        cluster.replica.posix_stat(child),
        Err(DfsPosixError::NotFound { .. })
    ));
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_bulk_local_eventual_remote_primary_large_batch_is_fully_visible() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-remote-primary-large");
    let remote_dir = "/services/bulk-remote-large";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "remote bulk parent", |cluster| {
        cluster.users_primary.posix_stat(remote_dir).is_ok()
            && cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        root,
        mut users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let batch = (0..64)
        .map(|index| {
            (
                format!("{remote_dir}/bulk-{index:06}.bin"),
                format!("bulk remote primary payload {index:06} ")
                    .repeat(128)
                    .into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let stats = replica
        .posix_write_files_local_eventual_with_known_parents(
            batch
                .iter()
                .map(|(path, bytes)| (path.as_str(), bytes.as_slice())),
        )
        .expect("remote-primary bulk write");
    assert_eq!(stats.len(), batch.len());

    let replica_stop = Arc::new(AtomicBool::new(false));
    let replica_thread_stop = Arc::clone(&replica_stop);
    let replica_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(replica, replica_thread_stop));

    let read_paths = batch
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let file_system_id = sifs_file_system_id();
    for (path, _) in &batch {
        assert!(
            users_primary
                .metadata()
                .object_by_normalized_posix_path(&file_system_id, path)
                .is_none(),
            "reader unexpectedly had remote-primary metadata before bulk read for {path}"
        );
    }
    let read_results = users_primary.posix_read_files_distributed(&read_paths);
    replica_stop.store(true, Ordering::SeqCst);
    let _replica = replica_thread.join().expect("replica thread");
    for ((path, bytes), result) in batch.iter().zip(read_results) {
        assert_eq!(
            result.expect("bulk remote read"),
            *bytes,
            "remote primary bulk read returned wrong bytes for {path}"
        );
    }
    for (path, _) in &batch {
        assert!(
            users_primary
                .metadata()
                .object_by_normalized_posix_path(&file_system_id, path)
                .is_none(),
            "bulk read should not materialize remote-primary metadata on the reader for {path}"
        );
    }

    for (path, bytes) in &batch {
        assert_eq!(
            users_primary
                .posix_stat_distributed(path)
                .expect("remote-primary stat through primary")
                .content_hash
                .as_deref(),
            Some(content_hash(bytes).as_str()),
            "missing remote-primary bulk path {path}"
        );
    }

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    for (path, bytes) in &batch {
        assert_eq!(
            services_primary
                .posix_stat(path)
                .expect("remote primary local stat")
                .content_hash
                .as_deref(),
            Some(content_hash(bytes).as_str()),
            "remote primary did not materialize {path}"
        );
    }

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_remote_primary_deferred_publish_write_persists_for_fresh_replica_reader() {
    let cluster = sifs_two_primary_cluster("sifs-remote-primary-deferred-fresh");
    let SifsTwoPrimaryCluster {
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let path = "/services/fresh-replica-writer.bin";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
        replica_candidates: vec![replica_node.clone(), services_primary_node.clone()],
        wait_for_commit: false,
        foreground_replica_repair: false,
    };
    let seed = vec![b'A'; 4096];
    let writer_a = vec![b'B'; 4096];
    let writer_b = vec![b'C'; 4096];

    replica
        .posix_write_file(path, &seed, options.clone())
        .expect("seed remote-primary file");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("seed response replay");
    replica
        .posix_write_file(path, &writer_a, options.clone())
        .expect("first remote-primary overwrite");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("first overwrite response replay");
    replica
        .posix_write_file(path, &writer_b, options)
        .expect("second remote-primary overwrite");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("second overwrite response replay");

    stop.store(true, Ordering::SeqCst);
    let _services_primary = services_thread.join().expect("services thread");
    drop(replica);

    let reopened = open_local_sharded_metadata_backend(
        root.join("replica").join("metadata"),
        sifs_two_primary_shard_configs(
            &users_primary_node,
            &services_primary_node,
            2,
            vec![services_primary_node.clone(), replica_node.clone()],
            vec![users_primary_node.clone(), replica_node],
        ),
    )
    .expect("reopen replica metadata");
    let stored = reopened
        .get_object_by_path(&sifs_file_system_id(), path)
        .expect("fresh metadata lookup")
        .expect("fresh metadata object");
    assert_eq!(
        stored.content_hash.as_deref(),
        Some(content_hash(&writer_b).as_str()),
        "fresh replica metadata must persist the committed remote-primary write, not the seed"
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn local_eventual_prepare_race_preserves_typed_conflict_in_routed_response() {
    let root = temp_root("local-eventual-prepare-race-conflict");
    let primary = NodeId::from_legacy_named("node_prepare_race_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_prepare_race_requester").expect("requester");
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
        open_local_sharded_metadata_backend(root.join("metadata"), shards)
            .expect("metadata backend"),
    )
    .expect("metadata backend attach");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let path = "/services/prepare-race.bin";
    let object_id =
        DfsObjectId::from_legacy_named("obj_prepare_race").expect("prepare-race object");
    let base_version =
        DfsVersion::from_legacy_named("version_prepare_race_base").expect("base version");
    let target_version =
        DfsVersion::from_legacy_named("version_prepare_race_target").expect("target version");
    let raced_version =
        DfsVersion::from_legacy_named("version_prepare_race_winner").expect("winner version");
    let mut current = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: sifs_file_system_id(),
        path: path.to_string(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: requester.clone(),
        current_locations: vec![requester.clone()],
        content_hash: Some(content_hash(b"prepare-race base")),
        content_size_bytes: Some(b"prepare-race base".len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 1,
        modified_at_unix_nanos: 1,
        created_at_unix_nanos: 1,
        changed_at_unix_nanos: 1,
        version: base_version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: Some(EventId::generate()),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_object(&current)
        .expect("seed current object");
    dfs.metadata
        .add_object(current.clone())
        .expect("seed current service object");

    let target_event_id = EventId::generate();
    let mut target = current.clone();
    target.version = target_version.clone();
    target.content_hash = Some(content_hash(b"prepare-race target"));
    target.content_size_bytes = Some(b"prepare-race target".len() as u64);
    target.modified_at_unix_nanos = 2;
    target.changed_at_unix_nanos = 2;
    target.last_modified_by_event_id = Some(target_event_id.clone());
    let versions = vec![object_version_record(&target, target.content_size_bytes)];
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        extension_expectations: Vec::new(),
        contribution_digest: None,
        file_version_pins: None,
        transaction_id: namespace_transaction_id_for_batch(
            "visible-batch",
            &sifs_file_system_id(),
            std::slice::from_ref(&target),
            &versions,
        ),
        file_system_id: sifs_file_system_id(),
        objects: vec![target.clone()],
        versions,
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let writes = vec![DfsLocalEventualBatchWriteProof {
        expected_base_version: Some(base_version.clone()),
        expected_parent_object_id: None,
        expected_parent_version: None,
        receipt: DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: target_version.clone(),
                owner_node_id: primary.clone(),
                event_id: target_event_id,
            },
            replica_node_id: requester.clone(),
            content_hash: target.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        },
    }];

    current.version = raced_version.clone();
    current.content_hash = Some(content_hash(b"prepare-race winner"));
    current.content_size_bytes = Some(b"prepare-race winner".len() as u64);
    current.modified_at_unix_nanos = 3;
    current.changed_at_unix_nanos = 3;
    current.last_modified_by_event_id = Some(EventId::generate());
    let raced = current;
    let _hook = crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
        move |backend| {
            backend
                .put_object(&raced)
                .expect("install concurrent winner after expectation capture");
        },
    );

    let command = DfsNamespaceCommand {
        request_id: "dfs_net_req_prepare_race_conflict".to_string(),
        file_system_id: sifs_file_system_id(),
        requesting_node_id: requester.clone(),
        include_metadata_journal_batches: false,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction: transaction.clone(),
            writes,
        },
    };
    let event = dfs
        .handle_namespace_request(requester, command)
        .expect("serve raced local-eventual batch");
    let (reason, posix_error) = match event {
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result:
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    ..
                },
            ..
        } => (reason, posix_error),
        other => panic!("expected typed prepare conflict, got {other:?}"),
    };
    assert!(matches!(
        &posix_error,
        Some(DfsNamespacePosixError::Conflict {
            path: conflict_path,
            reason,
        }) if conflict_path == path
            && reason.contains(base_version.as_str())
            && reason.contains(raced_version.as_str())
    ));
    assert!(matches!(
        crate::posix_model::posix_error_for_namespace_rejection(reason, posix_error),
        DfsPosixError::Conflict {
            path: conflict_path,
            ..
        } if conflict_path == path
    ));
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_object(&object_id)
            .expect("current object lookup")
            .expect("current object")
            .version,
        raced_version,
        "the rejected target must not overwrite the concurrent winner"
    );
    assert!(
        dfs.metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_namespace_transaction(&transaction.transaction_id)
            .expect("transaction lookup")
            .is_none(),
        "a precondition conflict must reject before journaling the transaction"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_local_eventual_batch_preserves_stale_child_as_typed_conflict() {
    let root = temp_root("local-eventual-batch-stale-child-conflict");
    let primary = NodeId::from_legacy_named("node_stale_child_primary").expect("primary");
    let requester = NodeId::from_legacy_named("node_stale_child_requester").expect("requester");
    let file_system = file_system(primary.clone());
    let mut current = file_metadata(requester.clone());
    current.file_system_id = file_system.file_system_id.clone();
    current.object_id = DfsObjectId::from_legacy_named("object_stale_child").expect("object id");
    current.path = "/services/stale-child.txt".to_string();
    current.policy = DfsObjectPolicy::ReplicatedEventual;
    current.content_hash = Some(content_hash(b"authority-current"));
    current.content_size_bytes = Some(17);
    current.version =
        DfsVersion::from_legacy_named("version_stale_child_current").expect("version");

    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current child");

    let mut proposed = current.clone();
    proposed.version =
        DfsVersion::from_legacy_named("version_stale_child_proposed").expect("version");
    proposed.content_hash = Some(content_hash(b"stale-proposal"));
    proposed.content_size_bytes = Some(14);
    proposed.modified_at_unix_nanos = 2;
    proposed.changed_at_unix_nanos = 2;
    proposed.last_modified_by_event_id = Some(EventId::generate());
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        extension_expectations: Vec::new(),
        contribution_digest: None,
        file_version_pins: None,
        transaction_id: "transaction-stale-child".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![proposed.clone()],
        versions: vec![object_version_record(
            &proposed,
            proposed.content_size_bytes,
        )],
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let writes = vec![DfsLocalEventualBatchWriteProof {
        expected_base_version: Some(
            DfsVersion::from_legacy_named("version_stale_child_observed").expect("version"),
        ),
        expected_parent_object_id: None,
        expected_parent_version: None,
        receipt: DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system.file_system_id.clone(),
                object_id: proposed.object_id.clone(),
                committed_version: proposed.version.clone(),
                owner_node_id: primary.clone(),
                event_id: proposed
                    .last_modified_by_event_id
                    .clone()
                    .expect("write event"),
            },
            replica_node_id: requester.clone(),
            content_hash: proposed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        },
    }];

    let network = InProcessNetwork::new();
    let _requester_transport = network.add_node(requester.clone());
    let mut service = NetworkedDfsService::new(
        network.add_node(primary),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata-backed service");
    let event = service
        .handle_namespace_request(
            requester.clone(),
            DfsNamespaceCommand {
                request_id: "request-stale-child".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                requesting_node_id: requester,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::CommitLocalEventualBatch {
                    transaction,
                    writes,
                },
            },
        )
        .expect("stale child response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected {
                posix_error: Some(DfsNamespacePosixError::Conflict { ref path, .. }),
                ..
            },
            ..
        } if path == &current.path
    ));
    assert_eq!(
        service
            .metadata_backend()
            .expect("metadata backend")
            .get_object(&current.object_id)
            .expect("current child"),
        Some(current),
        "stale validation must not mutate authoritative metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_services_routed_rename_then_unlink_is_read_your_write() {
    let cluster = sifs_two_primary_cluster("sifs-services-rename-unlink-ryw");
    let SifsTwoPrimaryCluster {
        root,
        users_primary,
        services_primary,
        mut replica,
        replica_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let base = "/services/cluster-tests";
    let path = "/services/cluster-tests/ryw.txt";
    let renamed = "/services/cluster-tests/ryw-renamed.txt";
    let bytes = b"services routed rename unlink read-your-write".to_vec();

    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all base");
    replica
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("routed services write");
    assert_eq!(
        replica.posix_read_file(path).expect("read before rename"),
        bytes
    );

    replica
        .posix_rename(path, renamed)
        .expect("routed services rename");
    assert_eq!(
        replica
            .posix_read_file(renamed)
            .expect("read renamed before unlink"),
        bytes
    );
    assert!(matches!(
        replica.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));

    let unlinked = replica
        .posix_unlink(renamed)
        .expect("unlink routed renamed services file");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        replica.posix_stat(renamed),
        Err(DfsPosixError::NotFound { .. })
    ));

    stop.store(true, Ordering::SeqCst);
    let users_primary = users_thread.join().expect("users thread");
    let services_primary = services_thread.join().expect("services thread");
    assert!(matches!(
        services_primary.posix_stat(renamed),
        Err(DfsPosixError::NotFound { .. })
    ));
    std::fs::remove_dir_all(&root).ok();
    drop(users_primary);
}

#[test]
fn late_routed_responses_are_discarded_without_failing_the_event_loop() {
    let root = temp_root("late-routed-responses-discarded");
    let local = NodeId::from_legacy_named("node_late_response_local").expect("local");
    let stranger = NodeId::from_legacy_named("node_late_response_stranger").expect("stranger");
    let object_id = DfsObjectId::from_legacy_named("obj_late_response").expect("object");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: local.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    // A write response for a request this node no longer tracks.
    let event = dfs
        .handle_distributed_write_response(
            stranger.clone(),
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_untracked".to_string(),
                object_id: object_id.clone(),
                result: DfsDistributedWriteResult::Rejected {
                    reason: "too late".to_string(),
                    current_primary_node_id: None,
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .expect("late write response must not error");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));

    // A namespace response for an untracked request.
    let event = dfs
        .handle_namespace_response(
            stranger.clone(),
            DfsNamespaceResponse {
                request_id: "dfs_net_req_untracked_ns".to_string(),
                result: DfsNamespaceResult::Applied,
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_batches: Vec::new(),
                metadata_journal_has_more: None,
                posix_coordination_result: None,
            },
        )
        .expect("late namespace response must not error");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));

    // A response from the wrong primary for a tracked request.
    let primary = NodeId::from_legacy_named("node_late_response_primary").expect("primary");
    dfs.writes.pending_routed_writes.insert(
        "dfs_net_req_tracked".to_string(),
        test_pending_routed_write(
            "dfs_net_req_tracked",
            &local,
            &object_id,
            &primary,
            test_routed_authority_shard(&primary, 1),
        ),
    );
    let event = dfs
        .handle_distributed_write_response(
            stranger,
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_tracked".to_string(),
                object_id,
                result: DfsDistributedWriteResult::Rejected {
                    reason: "spoofed".to_string(),
                    current_primary_node_id: None,
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .expect("mismatched response must not error");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    // The tracked request stays pending for the real primary.
    assert!(
        dfs.writes
            .pending_routed_writes
            .contains_key("dfs_net_req_tracked")
    );

    // A routed-write rejection caused by an in-flight cross-shard move keeps
    // its retriable Busy class across the string-typed wire result.
    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_tracked".to_string(),
            object_id: DfsObjectId::from_legacy_named("obj_late_response").expect("object"),
            result: DfsDistributedWriteResult::Rejected {
                reason:
                    "path /users/moving.txt is moving between metadata shards; retry the operation"
                        .to_string(),
                current_primary_node_id: None,
            },
            metadata_journal_batches: Vec::new(),
        },
    )
    .expect("busy rejection response");
    assert!(matches!(
        dfs.write_outcome("dfs_net_req_tracked"),
        Err(DfsError::Busy { path }) if path == "/users/moving.txt"
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_non_replica_routed_create_is_listed_after_blocking_write() {
    let cluster = sifs_outsider_cluster("sifs-outsider-create-list-ryw");
    let SifsPosixCluster {
        root,
        primary,
        observer_node,
        mut observer,
        ..
    } = cluster;

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let dir = "/users/outsider-list";
    let path = "/users/outsider-list/tiny.txt";
    let bytes = b"tiny routed outsider create".to_vec();

    observer
        .posix_mkdir_all(dir)
        .expect("routed mkdir from non-replica requester");
    let mut options = DfsWriteOptions::replicated_eventual(vec![observer_node.clone()]);
    options.foreground_replica_repair = false;
    observer
        .posix_write_file(path, &bytes, options)
        .expect("committed routed create from non-replica requester");

    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("outsider reads its own routed create from the primary"),
        bytes
    );
    let entries = observer
        .posix_list_dir_distributed(dir)
        .expect("outsider lists directory after routed create");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "committed routed create must be visible to an immediate distributed list: {entries:?}"
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_routed_write_refreshes_stale_receiver_control_plane() {
    let root = temp_root("sifs-write-refresh-stale-receiver");
    let old_primary_node =
        NodeId::from_legacy_named("node_sifs_write_refresh_old_primary").expect("old primary");
    let current_primary_node = NodeId::from_legacy_named("node_sifs_write_refresh_current_primary")
        .expect("current primary");
    let requester_node =
        NodeId::from_legacy_named("node_sifs_write_refresh_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![old_primary_node.clone(), current_primary_node.clone()],
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let put_control_plane = |kv_client: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
                             primary: &NodeId,
                             epoch: u64| {
        kv_client.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                    current_primary_node.clone(),
                )))
                .expect("filesystem config");
            control
                .put_metadata_shard(&shard_config(primary, epoch))
                .expect("shard config");
        });
    };

    let kv_current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_current, &current_primary_node, 2);
    let kv_old_primary = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_old_primary, &current_primary_node, 2);
    let kv_requester = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_requester, &old_primary_node, 1);
    let stale_requester_snapshot = kv_requester
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    put_control_plane(&kv_requester, &current_primary_node, 2);

    let refreshes = Arc::new(AtomicUsize::new(0));

    let mut old_primary = sifs_posix_cluster_node(
        &network,
        &kv_old_primary,
        &root,
        "old-primary",
        old_primary_node.clone(),
    );
    let mut current_primary = sifs_posix_cluster_node(
        &network,
        &kv_old_primary,
        &root,
        "current-primary",
        current_primary_node.clone(),
    );
    let mut requester = sifs_posix_cluster_node(
        &network,
        &kv_requester,
        &root,
        "requester",
        requester_node.clone(),
    );
    requester =
        requester.with_boxed_control_plane(Box::new(RefreshingDfsControlPlaneKvClient::new(
            InProcessKvStore::restore(stale_requester_snapshot)
                .expect("restore stale requester control plane"),
            kv_requester.clone(),
            Arc::new(AtomicUsize::new(0)),
        )));
    current_primary
        .bootstrap_distributed_sifs()
        .expect("current primary SIFS bootstrap");

    let parent_stop = Arc::new(AtomicBool::new(false));
    let old_parent_stop = Arc::clone(&parent_stop);
    let old_parent_thread = std::thread::spawn(move || {
        let mut node = old_primary;
        while !old_parent_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary parent pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        node
    });
    let current_parent_stop = Arc::clone(&parent_stop);
    let current_parent_thread = std::thread::spawn(move || {
        let mut node = current_primary;
        while !current_parent_stop.load(Ordering::SeqCst) {
            node.process_incoming()
                .expect("current primary parent pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        node
    });
    requester
        .posix_mkdir_all("/users")
        .expect("requester creates parent through current primary");
    parent_stop.store(true, Ordering::SeqCst);
    old_primary = old_parent_thread.join().expect("old primary parent thread");
    current_primary = current_parent_thread
        .join()
        .expect("current primary parent thread");

    let mut stale_current_store = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut stale_current_store);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                current_primary_node.clone(),
            )))
            .expect("stale receiver filesystem config");
        control
            .put_metadata_shard(&shard_config(&old_primary_node, 1))
            .expect("stale receiver shard config");
    }
    let refreshing_current_control = RefreshingDfsControlPlaneKvClient::new(
        stale_current_store,
        kv_current,
        Arc::clone(&refreshes),
    );
    current_primary =
        current_primary.with_boxed_control_plane(Box::new(refreshing_current_control));

    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread = std::thread::spawn(move || {
        let mut node = old_primary;
        while !old_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let current_stop = Arc::clone(&stop);
    let current_thread = std::thread::spawn(move || {
        let mut node = current_primary;
        while !current_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("current primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let path = "/users/write-refreshes-stale-receiver.txt";
    let bytes = b"stale receiver refreshes control plane".to_vec();
    requester
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![requester_node, current_primary_node]),
        )
        .expect("routed write refreshes stale receiver control plane");
    assert!(
        refreshes.load(Ordering::SeqCst) > 0,
        "current primary should refresh its stale control-plane view"
    );

    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary thread");
    current_thread.join().expect("current primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_routed_lookup_rejects_uncorroborated_redirect_chain() {
    let root = temp_root("sifs-lookup-redirect-refreshes-primary");
    let old_primary =
        NodeId::from_legacy_named("node_sifs_lookup_refresh_old_primary").expect("old primary");
    let intermediate_primary =
        NodeId::from_legacy_named("node_sifs_lookup_refresh_mid_primary").expect("mid primary");
    let current_primary =
        NodeId::from_legacy_named("node_sifs_lookup_refresh_current_primary").expect("primary");
    let requester_node =
        NodeId::from_legacy_named("node_sifs_lookup_refresh_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![
            old_primary.clone(),
            intermediate_primary.clone(),
            current_primary.clone(),
        ],
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let put_control_plane = |kv_client: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
                             primary: &NodeId,
                             epoch: u64| {
        kv_client.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                    current_primary.clone(),
                )))
                .expect("filesystem config");
            control
                .put_metadata_shard(&shard_config(primary, epoch))
                .expect("shard config");
        });
    };

    let kv_old = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_old, &intermediate_primary, 2);
    let kv_intermediate = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_intermediate, &old_primary, 2);
    let kv_current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_current, &current_primary, 3);
    let kv_requester = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_requester, &old_primary, 1);

    let old_node =
        sifs_posix_cluster_node(&network, &kv_old, &root, "old-primary", old_primary.clone());
    let intermediate_node = sifs_posix_cluster_node(
        &network,
        &kv_intermediate,
        &root,
        "intermediate-primary",
        intermediate_primary.clone(),
    );
    let mut current_node = sifs_posix_cluster_node(
        &network,
        &kv_current,
        &root,
        "current-primary",
        current_primary.clone(),
    );
    let mut requester =
        sifs_posix_cluster_node(&network, &kv_requester, &root, "requester", requester_node);
    let stale_requester_snapshot = kv_requester
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    put_control_plane(&kv_requester, &current_primary, 3);
    requester =
        requester.with_boxed_control_plane(Box::new(RefreshingDfsControlPlaneKvClient::new(
            InProcessKvStore::restore(stale_requester_snapshot)
                .expect("restore stale requester control plane"),
            kv_requester.clone(),
            Arc::new(AtomicUsize::new(0)),
        )));

    current_node
        .bootstrap_distributed_sifs()
        .expect("current primary SIFS bootstrap");
    let path = "/users/lookup-refreshes-primary.txt";
    let bytes = b"routed lookup reselects refreshed primary".to_vec();
    current_node
        .posix_mkdir_all("/users")
        .expect("current primary creates parent");
    current_node
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![current_primary.clone()]),
        )
        .expect("current primary writes file");

    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread = std::thread::spawn(move || {
        let mut node = old_node;
        while !old_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let intermediate_stop = Arc::clone(&stop);
    let intermediate_thread = std::thread::spawn(move || {
        let mut node = intermediate_node;
        while !intermediate_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("intermediate primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let current_stop = Arc::clone(&stop);
    let current_thread = std::thread::spawn(move || {
        let mut node = current_node;
        while !current_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("current primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let error = requester
        .posix_stat_distributed(path)
        .expect_err("lookup must reject an intermediate primary not corroborated by refresh");
    assert!(
        error.to_string().contains("uncorroborated")
            || error.to_string().contains("current affected assignment"),
        "unexpected redirect rejection: {error}"
    );

    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary thread");
    intermediate_thread
        .join()
        .expect("intermediate primary thread");
    current_thread.join().expect("current primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cross_primary_dot_paths_route_to_the_resolved_namespace() {
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = sifs_two_primary_cluster("sifs-dot-component-routing");
    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let services_stop = Arc::clone(&stop);
    let users_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(users_primary, users_stop));
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let result = (|| -> Result<_, DfsPosixError> {
        let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
        replica.posix_mkdir("/services/sub")?;
        replica.posix_symlink("/services/sub", "/users/link", options.clone())?;
        replica.posix_write_file("/users/victim", b"must survive", options.clone())?;
        replica.posix_write_file("/services/victim", b"resolved target", options)?;
        let read = replica.posix_read_file_distributed("/users/link/../victim")?;
        replica.posix_rename("/users/link/../victim", "/users/link/../renamed")?;
        replica.posix_hardlink("/users/link/../renamed", "/users/link/../alias")?;
        replica.posix_unlink("/users/link/../alias")?;
        replica.posix_unlink("/users/link/../renamed")?;
        let survivor = replica.posix_read_file_distributed("/users/victim")?;
        Ok((read, survivor))
    })();
    stop.store(true, Ordering::SeqCst);
    users_thread.join().expect("users primary pump");
    services_thread.join().expect("services primary pump");
    let (read, survivor) = result.expect("dot paths resolve before shard admission");
    assert_eq!(read, b"resolved target");
    assert_eq!(survivor, b"must survive");
    std::fs::remove_dir_all(root).ok();
}
