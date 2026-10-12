//! Routing regression tests.

use super::*;

fn native_pending_routed_write(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    DfsVersion,
    Vec<u8>,
    Vec<NodeId>,
    String,
    DfsDistributedWriteResponse,
) {
    let (runtime, primary, _, requester, base, _) =
        native_read_fixture(name, 1, false, b"actual routed predecessor".to_vec());
    let nodes = vec![primary.clone(), requester.clone()];
    let path = base.path.clone();
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    deny_native_read_repair(&runtime, &primary, &requester);
    let mut client = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x74; 2 * 1024 * 1024 + 73];
    let operation = sinomo_core::Id::generate().to_string();
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = client
        .write_distributed_bytes_with_base(
            request.clone(),
            Some(base.version.clone()),
            bytes.clone(),
            nodes.clone(),
            policy,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.clone()),
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(client.writes.pending_routed_writes.contains_key(&operation));
    let queued = client.transport.drain_incoming_messages().unwrap();
    let response = queued
        .into_iter()
        .find_map(|routed| {
            if routed.source_node_id != primary || routed.target_node_id != requester {
                return None;
            }
            let DfsNetworkMessage::DistributedWriteResponse(response) = routed.message else {
                return None;
            };
            (response.request_id == operation && response.object_id == request.object_id)
                .then_some(response)
        })
        .expect("normal routed request produced its own native response");
    assert!(matches!(
        &response.result,
        DfsDistributedWriteResult::Outcome {
            outcome: NetworkedDfsWriteOutcome::Pending { .. }
        }
    ));
    (
        runtime,
        client,
        request,
        base.version,
        bytes,
        nodes,
        operation,
        response,
    )
}

fn complete_native_routed_write(
    runtime: &crate::test_runtime::DfsTestRuntime,
    client: &mut NetworkedDfsService<DfsNodeTransport>,
    request: &DfsWriteRequest,
    base: &DfsVersion,
    bytes: &[u8],
    nodes: &[NodeId],
    operation: &str,
) -> (NodeId, DfsDistributedWriteResponse) {
    let primary = client.writes.pending_routed_writes[operation]
        .primary_node_id
        .clone();
    let config = client.writes.pending_routed_writes[operation]
        .authority_shard
        .clone();
    runtime.clear_faults();
    let mut owner = runtime.client(&primary).unwrap();
    owner.hydrate_from_metadata_backend().unwrap();
    owner.retry_pending_repairs().unwrap();
    owner.process_incoming().unwrap();
    owner.flush_detached_transport_sends().unwrap();
    let publication = runtime
        .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
    assert_eq!(publication.omitted_kv_event_count, 0);
    owner.process_incoming().unwrap();
    let actual = owner
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(actual.file_system_id, config.file_system_id);
    assert_eq!(
        actual.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );
    drop(owner);
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.to_vec();
    // Replay the SAME real operation through ordinary dispatch. Only its
    // actual committed receipt may generate the terminal response.
    client
        .write_distributed_bytes_with_base(
            request.clone(),
            Some(base.clone()),
            bytes.to_vec(),
            nodes.to_vec(),
            policy,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.into()),
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    let response = client
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find_map(|routed| {
            if routed.source_node_id != primary || routed.target_node_id != *client.node_id() {
                return None;
            }
            let DfsNetworkMessage::DistributedWriteResponse(response) = routed.message else {
                return None;
            };
            (response.request_id == operation
                && response.object_id == request.object_id
                && matches!(
                    &response.result,
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. }
                    }
                ))
            .then_some(response)
        })
        .expect("actual native stable replay returns its exact committed result");
    (primary, response)
}

#[test]
fn routed_write_response_tracks_remote_intent_alias() {
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-routed-alias");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Pending { intent_id, .. },
    } = &pending_response.result
    else {
        unreachable!()
    };
    let alias = intent_id.clone();
    client
        .handle_distributed_write_response(primary.clone(), pending_response.clone())
        .unwrap();
    assert!(
        matches!(client.write_outcome(&operation).unwrap(), NetworkedDfsWriteOutcome::Pending { intent_id, .. } if intent_id == alias)
    );
    assert!(
        client.writes.pending_routed_writes[&operation]
            .command
            .is_some()
    );
    assert!(
        client.writes.pending_routed_writes[&alias]
            .command
            .is_none()
    );
    let mut rejected = pending_response;
    rejected.result = DfsDistributedWriteResult::Rejected {
        reason: "exclusive retry observed the pending create".into(),
        current_primary_node_id: None,
    };
    assert!(matches!(
        client
            .handle_distributed_write_response(primary.clone(), rejected)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert!(matches!(
        client.write_outcome(&alias).unwrap(),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let (actual_primary, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    assert_eq!(actual_primary, primary);
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash: hash,
                ..
            },
    } = &terminal.result
    else {
        unreachable!()
    };
    assert_eq!(intent_id, &alias);
    assert_eq!(hash, &content_hash(&bytes));
    let actual_version = version.clone();
    client
        .handle_distributed_write_response(primary, terminal)
        .unwrap();
    assert!(
        client
            .writes
            .pending_routed_writes
            .values()
            .all(|pending| pending.command.is_none())
    );
    assert!(
        matches!(client.take_write_outcome(&alias).unwrap(), NetworkedDfsWriteOutcome::Committed { version, .. } if version == actual_version)
    );
    assert!(client.writes.pending_routed_writes.is_empty());
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn routed_write_stale_pending_response_does_not_downgrade_completion() {
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-routed-late-pending");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    client
        .handle_distributed_write_response(primary.clone(), pending_response.clone())
        .unwrap();
    let (actual_primary, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    assert_eq!(actual_primary, primary);
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id, version, ..
        },
    } = &terminal.result
    else {
        unreachable!()
    };
    let alias = intent_id.clone();
    let actual_version = version.clone();
    client
        .handle_distributed_write_response(primary.clone(), terminal)
        .unwrap();
    assert!(matches!(
        client
            .handle_distributed_write_response(primary, pending_response)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    for id in [&operation, &alias] {
        assert!(
            matches!(client.write_outcome(id).unwrap(), NetworkedDfsWriteOutcome::Committed { intent_id, version, .. } if intent_id == alias && version == actual_version)
        );
    }
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn local_write_outcome_wins_over_stale_routed_alias_shadow() {
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-local-committed-before-routed-reply");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    client
        .handle_distributed_write_response(primary, pending_response)
        .unwrap();
    let (_, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash: hash,
                ..
            },
    } = &terminal.result
    else {
        unreachable!()
    };
    let pending = &client.writes.pending_routed_writes[intent_id];
    assert!(
        pending.result.is_none(),
        "the actual terminal response has not been consumed"
    );
    assert_eq!(pending.remote_intent_id.as_ref(), Some(intent_id));
    assert!(
        matches!(client.write_outcome(intent_id).unwrap(), NetworkedDfsWriteOutcome::Committed { intent_id: actual_intent, version: actual_version, content_hash: actual_hash, .. }
        if actual_intent == *intent_id && actual_version == *version && actual_hash == *hash)
    );
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_routed_namespace_replays_a_bounded_actual_backlog() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-routing-backlog", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-routing-backlog", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = shard.primary_node_id.clone();
    let mut source = runtime.client(&primary).unwrap();
    for index in 0..(DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT / 2 + 8) {
        source
            .posix_write_file(
                &format!("/services/native-backlog-{index}.bin"),
                &(index as u64).to_le_bytes(),
                DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
            )
            .unwrap();
    }
    drop(source);
    let current_shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let requester = nodes
        .iter()
        .find(|node| **node != current_shard.primary_node_id)
        .unwrap()
        .clone();
    let global_observations_before = runtime.rpc_observations().len();
    let global_dropped_before = runtime.observation_dropped_count();
    let capture = runtime
        .capture_namespace_requests(&current_shard.primary_node_id, 32)
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    client
        .posix_mkdir_all("/services/native-after-backlog/deep")
        .unwrap();
    let rows = client.posix_list_dir_distributed("/services").unwrap();
    assert!(rows.iter().any(|row| row.name == "native-after-backlog"));
    let actual = client
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT,
            DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert!(actual.entries.len() > DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
    assert!(actual.entries.len() <= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT);
    for entry in actual.entries {
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PrepareOperationV7 { .. }
                | DfsMetadataMutation::CommitPreparedOperationV7 { .. }
        ));
    }
    // This window observes only real post-backlog Main dispatches. Its records()
    // checks zero omissions independently of the long-lived transport vector.
    let requests = capture.records();
    let exact_owner_request = |message: &crate::DfsRoutedMessage| {
        if message.source_node_id != requester
            || message.target_node_id != current_shard.primary_node_id
        {
            return false;
        }
        let DfsNetworkMessage::NamespaceRequest(request) = &message.message else {
            return false;
        };
        request.requesting_node_id == requester
            && request.file_system_id == current_shard.file_system_id
            && request.authority_shards == vec![current_shard.clone()]
    };
    assert!(
        requests
            .iter()
            .filter(|message| exact_owner_request(message))
            .any(|message| {
                matches!(&message.message, DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path }
                if path == "/services/native-after-backlog/deep"))
            })
    );
    assert!(requests.iter().filter(|message| exact_owner_request(message)).any(|message| {
        matches!(&message.message, DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MetadataQuery { query }
                if query.file_system_id == current_shard.file_system_id
                    && query.shard_id.0 == current_shard.shard_id
                    && query.epoch == current_shard.epoch
                    && matches!(&query.selection, crate::DfsMetadataSelection::DirectoryPage { path, .. }
                        if path == "/services")))
    }));
    let global_dropped_after = runtime.observation_dropped_count();
    drop(client);
    let coverage = runtime.close_with_namespace_capture(capture).unwrap();
    assert_eq!(coverage.namespace_omitted, Some(0));
    println!(
        "backlog observation coverage: before_records={global_observations_before} before_dropped={global_dropped_before} after_dropped={global_dropped_after} final={}",
        serde_json::to_string(&coverage).unwrap()
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
        batch: DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            journal_frames: Vec::new(),
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
        .export_snapshot_image()
        .expect("native snapshot image");
    let snapshot_command = DfsMetadataSnapshotInstallCommand {
        request_id: "snapshot-request".to_string(),
        source_node_id: local.clone(),
        file_system_id: None,
        authority_epoch: None,
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: 9,
        snapshot: snapshot.descriptor().clone(),
        image: Some(snapshot),
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
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(local.clone()),
        content,
    );

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
fn local_owned_read_is_routed_to_owner_node() {
    let name = "native-local-owned-routed-read";
    let (runtime, mut owner_dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    let remote = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != owner)
        .unwrap();
    let path = "/services/native-local-owned-read";
    let mut fs = runtime.file_system();
    fs.subtree_policies.push(DfsSubtreePolicy::new(
        path,
        DfsObjectPolicy::LocalOwned,
        1,
        1,
    ));
    owner_dfs.add_file_system(fs).unwrap();
    let stat = owner_dfs
        .posix_write_file(
            path,
            b"owned bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let mut reader = runtime.client(&remote).unwrap();
    assert_eq!(
        reader
            .request_remote_object_read_without_cache(&stat.object_id)
            .unwrap(),
        vec![owner.clone()]
    );
    let request_id = reader
        .reads
        .pending_reads
        .keys()
        .next()
        .expect("actual pending read")
        .clone();
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: actual, source_node_id,
            result: DfsReadResult::Found(DfsReadData { bytes, .. }), ..
        } if actual == &request_id && source_node_id == &owner && bytes == b"owned bytes")));
    assert!(
        !runtime
            .rpc_observations()
            .iter()
            .any(|event| event.target == remote && event.route == DFS_DATA_STAGE_BLOB_ROUTE)
    );
    drop(reader);
    drop(owner_dfs);
    runtime.close().unwrap();
}

#[test]
fn replicated_eventual_write_is_routed_to_metadata_primary_and_committed() {
    let mut cluster = sifs_posix_cluster("routed-primary-write-commit", "/users");
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
    let name = "native-lazy-reader-remote-primary";
    let (runtime, primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary_node = config.primary_node_id;
    let writer_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let mut writer = runtime.client(&writer_node).unwrap();
    let path = "/services/current-read-remote-primary.txt";
    let old = b"old remote primary reader payload".to_vec();
    let new = b"new remote primary reader payload".to_vec();
    writer
        .posix_write_file(
            path,
            &old,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .unwrap();
    let mut reader = runtime.client(&writer_node).unwrap();
    let mut stale_stream = reader.posix_open_file_stream(path).unwrap();
    let replacement = writer
        .posix_write_file(
            path,
            &new,
            DfsWriteOptions::replicated_eventual(vec![writer_node]),
        )
        .unwrap();
    let mut stale = Vec::new();
    stale_stream.reader.read_to_end(&mut stale).unwrap();
    assert_eq!(
        stale, old,
        "opened content retains its actual pre-overwrite generation"
    );
    let mut current_stream = reader.posix_open_current_file_stream(path).unwrap();
    assert_eq!(current_stream.object_id, replacement.object_id);
    assert_eq!(current_stream.version, replacement.version);
    let mut current = Vec::new();
    current_stream.reader.read_to_end(&mut current).unwrap();
    assert_eq!(
        current, new,
        "fresh authority selects the current exact local body"
    );
    drop(current_stream);
    drop(stale_stream);
    drop(reader);
    drop(writer);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_routed_create_is_read_your_write_on_writer() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-ryw", "/users");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_write_is_visible_on_primary() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-primary-vis", "/users");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_overwrite_advances_version_monotonically() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-overwrite", "/users");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_create_updates_parent_directory_mtime_cluster_wide() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-parent-mtime", "/users");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_retried_pending_routed_create_preserves_parent_mtime_touch() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-retried-create-parent-mtime", "/users");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_namespace_ops_have_read_your_write_semantics() {
    // Namespace mutations from a non-primary node route to the metadata shard
    // primary and replay the primary's journal before returning, so every
    // blocking POSIX call observes its own effect locally.
    let cluster = sifs_posix_cluster("sifs-cluster-ns-ryw", "/users");
    let SifsPosixCluster {
        root,
        primary_node,
        writer_node,
        observer_node,
        primary,
        mut writer,
        mut observer,
        rejections: _,
        _runtime,
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
        let publication = _runtime
            .maintenance_tick(
                &primary_node,
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .expect("ordinary Main publication after routed namespace ops");
        assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
        assert_eq!(publication.omitted_kv_event_count, 0);
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

    drop(_runtime);
}

#[test]
fn sifs_cluster_posix_mkdir_all_remote_primary_falls_back_when_local_chain_exists() {
    let name = "native-mkdir-all-local-chain";
    let (runtime, mut primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary_node = config.primary_node_id;
    let observer = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let path = "/services/native-existing-chain";
    primary.posix_mkdir_all(path).unwrap();
    primary.posix_sync_directory(path).unwrap();
    let mut local = runtime.client(&observer).unwrap();
    assert_eq!(
        local.posix_stat(path).unwrap().kind,
        DfsObjectKind::Directory
    );
    let shard = DfsMetadataShardId::new(config.shard_id);
    let before = primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(observer.clone()),
        target: Some(primary_node),
        route: None,
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    local
        .posix_mkdir_all(path)
        .expect("the actual locally mirrored existing chain is idempotent");
    local
        .posix_mkdir_all_without_local_replay(path)
        .expect("the actual existing chain needs no replay or remote mutation");
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        before
    );
    drop(local);
    drop(primary);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_routed_mkdir_all_allows_immediate_child_mkdir() {
    let cluster = sifs_two_primary_cluster("sifs-routed-mkdir-all-child");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
        shared_kv,
        users_primary_node,
        _runtime,
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
    cluster.close();
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
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
        shared_kv,
        _runtime,
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
fn sifs_cluster_posix_services_routed_rename_then_unlink_is_read_your_write() {
    let cluster = sifs_two_primary_cluster("sifs-services-rename-unlink-ryw");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
    let (runtime, mut client, request, _, _, _, operation, actual) =
        native_pending_routed_write("native-late-routed-response");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    let mut untracked = actual.clone();
    untracked.request_id = sinomo_core::Id::generate().to_string();
    assert!(matches!(
        client
            .handle_distributed_write_response(primary.clone(), untracked)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert!(matches!(
        client
            .handle_namespace_response(
                primary.clone(),
                DfsNamespaceResponse {
                    request_id: sinomo_core::Id::generate().to_string(),
                    committed_work: Default::default(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: false,
                    posix_coordination_result: None,
                }
            )
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    let pending = client.writes.pending_routed_writes[&operation].clone();
    let wrong_source = client.node_id().clone();
    assert_ne!(wrong_source, primary);
    assert!(matches!(
        client
            .handle_distributed_write_response(wrong_source, actual.clone())
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert_eq!(client.writes.pending_routed_writes[&operation], pending);
    let mut rejected = actual;
    let path = pending
        .command
        .as_ref()
        .unwrap()
        .object_metadata
        .as_ref()
        .unwrap()
        .path
        .clone();
    rejected.result = DfsDistributedWriteResult::Rejected {
        reason: format!("path {path} is moving between metadata shards; retry the operation"),
        current_primary_node_id: None,
    };
    assert_eq!(rejected.object_id, request.object_id);
    client
        .handle_distributed_write_response(primary, rejected)
        .unwrap();
    assert!(
        matches!(client.write_outcome(&operation), Err(DfsError::Busy { path: busy }) if busy == path)
    );
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn sifs_non_replica_routed_create_is_listed_after_blocking_write() {
    let cluster = sifs_outsider_cluster("sifs-outsider-create-list-ryw");
    let SifsPosixCluster {
        root,
        primary,
        observer_node,
        mut observer,
        _runtime,
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
    drop(_runtime);
}

#[test]
fn sifs_cross_primary_dot_paths_route_to_the_resolved_namespace() {
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn native_routed_stale_unlink_preserves_typed_conflict_and_actual_winner() {
    let mut cluster = sifs_two_primary_cluster("native-routed-stale-unlink");
    let path = "/services/stale-caller";
    let first = cluster
        .replica
        .posix_write_file(
            path,
            b"first generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let winner = cluster
        .services_primary
        .posix_write_file(
            path,
            b"actual concurrent winner",
            DfsWriteOptions::replicated_eventual(vec![cluster.services_primary_node.clone()]),
        )
        .unwrap();
    let error = cluster
        .replica
        .posix_unlink_if_generation(
            path,
            sinomo_core::Id::generate().to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: first.object_id,
                version: first.version,
            },
        )
        .unwrap_err();
    assert!(
        matches!(error, DfsPosixError::Conflict { ref path, .. } if path == "/services/stale-caller")
    );
    assert_eq!(
        cluster.replica.posix_stat(path).unwrap().version,
        winner.version
    );
    assert_eq!(
        cluster.replica.posix_read_file(path).unwrap(),
        b"actual concurrent winner"
    );
    cluster.close();
}

#[test]
fn native_routed_stale_rename_cannot_replace_a_current_generation() {
    let mut cluster = sifs_two_primary_cluster("native-routed-stale-rename");
    let from = "/services/stale-rename-source";
    let to = "/services/stale-rename-target";
    let first = cluster
        .replica
        .posix_write_file(
            from,
            b"first generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let winner = cluster
        .services_primary
        .posix_write_file(
            from,
            b"winning generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.services_primary_node.clone()]),
        )
        .unwrap();
    let error = cluster
        .replica
        .posix_rename_if_generation(
            from,
            to,
            sinomo_core::Id::generate().to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: first.object_id,
                version: first.version,
            },
            DfsNamespaceDestinationPrecondition::Absent,
        )
        .unwrap_err();
    assert!(
        matches!(error, DfsPosixError::Conflict { ref path, .. } if path == "/services/stale-rename-source")
    );
    assert_eq!(
        cluster.replica.posix_stat(from).unwrap().version,
        winner.version
    );
    assert!(matches!(
        cluster.replica.posix_stat(to),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster.replica.posix_read_file(from).unwrap(),
        b"winning generation"
    );
    cluster.close();
}
