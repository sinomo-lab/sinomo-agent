//! Repair regression tests.

use super::*;

#[test]
fn dfs_message_repair_progress_is_only_inline_for_explicit_foreground_repair_writes() {
    let source = NodeId::from_legacy_named("node_protocol_repair_source").expect("source");
    let target = NodeId::from_legacy_named("node_protocol_repair_target").expect("target");
    let file_system_id = sifs_file_system_id();
    let namespace_command = |request_id: &str, operation| {
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: request_id.to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: source.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation,
        })
    };
    let lookup = test_dfs_message_command_from_message(
        &source,
        &target,
        1,
        namespace_command(
            "dfs_lookup_no_inline_repair",
            DfsNamespaceOperation::LookupPath {
                path: "/services/no-inline-repair".to_string(),
                inline_range: None,
            },
        ),
    );
    let mkdir = test_dfs_message_command_from_message(
        &source,
        &target,
        2,
        namespace_command(
            "dfs_mkdir_no_inline_repair",
            DfsNamespaceOperation::MkdirAll {
                path: "/services/no-inline-repair".to_string(),
            },
        ),
    );
    let sync_path = test_dfs_message_command_from_message(
        &source,
        &target,
        3,
        namespace_command(
            "dfs_sync_path_no_inline_repair",
            DfsNamespaceOperation::SyncPath {
                path: "/services/no-inline-repair".to_string(),
            },
        ),
    );
    let read = test_dfs_message_command_from_message(
        &source,
        &target,
        4,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "dfs_read_no_inline_repair".to_string(),
            object_id: DfsObjectId::from_legacy_named("obj_no_inline_repair").expect("object"),
            version: None,
            content_hash: Some("blake3:noinline".to_string()),
            content_size_bytes: Some(0),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let object_id = DfsObjectId::from_legacy_named("obj_foreground_repair_write").expect("object");
    let mut background_write =
        test_routed_write_command("dfs_background_write_no_inline_repair", &source, &object_id);
    background_write.foreground_replica_repair = false;
    let background_write = test_dfs_message_command_from_message(
        &source,
        &target,
        5,
        DfsNetworkMessage::DistributedWriteRequest(background_write),
    );
    let foreground_repair_write = test_dfs_message_command_from_message(
        &source,
        &target,
        6,
        DfsNetworkMessage::DistributedWriteRequest(test_routed_write_command(
            "dfs_foreground_repair_write",
            &source,
            &object_id,
        )),
    );

    assert!(!dfs_message_allows_write_repair_progress(&lookup));
    assert!(!dfs_message_allows_write_repair_progress(&mkdir));
    assert!(!dfs_message_allows_write_repair_progress(&sync_path));
    assert!(!dfs_message_allows_write_repair_progress(&read));
    assert!(!dfs_message_allows_write_repair_progress(&background_write));
    assert!(dfs_message_allows_write_repair_progress(
        &foreground_repair_write
    ));
}

#[test]
fn posix_wait_for_commit_reports_repair_retry_failure_without_failing_write() {
    let root = temp_root("posix-wait-repair-retry-best-effort");
    let local = NodeId::from_legacy_named("node_posix_wait_repair_local").expect("local");
    let remote = NodeId::from_legacy_named("node_posix_wait_repair_remote").expect("remote");
    let stage_calls = Arc::new(Mutex::new(Vec::new()));
    let stage_calls_for_sender = Arc::clone(&stage_calls);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        let DfsNetworkMessage::StageBlob(stage) = &command.routed_message.message else {
            return Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            });
        };
        stage_calls_for_sender
            .lock()
            .expect("stage calls lock")
            .push(stage.request_id.clone());
        let response = DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
            request_id: stage.request_id.clone(),
            intent_id: stage.intent_id.clone(),
            file_system_id: stage.file_system_id.clone(),
            object_id: stage.object_id.clone(),
            version: stage.version.clone(),
            replica_node_id: command.routed_message.target_node_id.clone(),
            result: DfsBlobTransferResult::Durable {
                content_hash: stage.content_hash.clone(),
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
    let transport = node_transport_with_canonical_message_peer(
        local.clone(),
        vec![remote.clone()],
        Duration::from_millis(1),
        sender,
    );
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(transport, content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/repair").expect("repair dir");

    let mut initial = vec![0_u8; 256 * 1024 + 31];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let broken_path = "/services/repair/broken-patch.bin";
    dfs.posix_write_file(
        broken_path,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![local.clone()]),
    )
    .expect("initial self write");

    let mut patch = vec![0_u8; 80 * 1024 + 7];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    dfs.posix_write_file_at(
        broken_path,
        96 * 1024 + 5,
        &patch,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
            replica_candidates: vec![local.clone(), remote.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    )
    .expect("pending patch write");
    let pending_patch_hash = dfs
        .writes
        .pending_writes
        .values()
        .find_map(|pending| pending.patch.as_ref().map(|patch| patch.patch_hash.clone()))
        .expect("pending patch hash");
    dfs.content()
        .debug_remove_loose_blob(&pending_patch_hash)
        .expect("remove patch blob");
    *stage_calls.lock().expect("stage calls lock") = Vec::new();
    dfs.repair_retry_tick = 0;
    for pending in dfs.writes.pending_writes.values_mut() {
        for state in pending.stage_retries.values_mut() {
            state.next_retry_tick = 0;
            state.inflight = false;
        }
        for state in pending.repair_retries.values_mut() {
            state.next_retry_tick = 0;
            state.inflight = false;
        }
    }

    let stat = dfs
        .posix_write_file(
            "/services/repair/foreground.txt",
            b"foreground bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("foreground policy"),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("foreground write should not fail on repair retry error");

    assert_eq!(stat.current_locations, vec![local, remote]);
    assert_eq!(stat.size_bytes, Some(b"foreground bytes".len() as u64));
    assert_eq!(stage_calls.lock().expect("stage calls lock").len(), 1);
    assert!(
        dfs.last_events().iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::RepairRetryFailed { reason }
                if reason.contains("failed to open DFS patch blob")
        )),
        "repair retry failure should be reported as an event: {:?}",
        dfs.last_events()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn served_receipt_returns_before_unrelated_completed_write_response_rpc() {
    let root = temp_root("served-receipt-detaches-unrelated-write-response");
    let requester =
        NodeId::from_legacy_named("node_served_unrelated_response_requester").expect("requester");
    let primary =
        NodeId::from_legacy_named("node_served_unrelated_response_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_served_unrelated_response_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_served_unrelated_response").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_served_unrelated_response_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_served_unrelated_response_2").expect("version");
    let staged_command = Arc::new(Mutex::new(None::<DfsStageBlobCommand>));
    let completed_response_calls = Arc::new(AtomicUsize::new(0));
    let completed_response_gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let staged_command_for_sender = Arc::clone(&staged_command);
    let completed_response_calls_for_sender = Arc::clone(&completed_response_calls);
    let completed_response_gate_for_sender = Arc::clone(&completed_response_gate);
    let requester_for_sender = requester.clone();
    let replica_for_sender = replica.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        match &command.routed_message.message {
            DfsNetworkMessage::StageBlob(stage) => {
                assert_eq!(target_node_id, &replica_for_sender);
                *staged_command_for_sender.lock().expect("staged command") = Some(stage.clone());
            }
            DfsNetworkMessage::CommitVersion(commit) => {
                assert_eq!(target_node_id, &replica_for_sender);
                return Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: vec![DfsRoutedMessage {
                        source_node_id: command.routed_message.target_node_id.clone(),
                        target_node_id: command.routed_message.source_node_id.clone(),
                        frame_id: command.routed_message.frame_id.saturating_add(100),
                        message_id: command.routed_message.message_id.saturating_add(100),
                        message: DfsNetworkMessage::CommitVersionResponse(
                            DfsCommitVersionResponse {
                                request_id: commit.request_id.clone(),
                                intent_id: commit.intent_id.clone(),
                                file_system_id: commit.file_system_id.clone(),
                                object_id: commit.object_id.clone(),
                                version: commit.version.clone(),
                                replica_node_id: replica_for_sender.clone(),
                                result: DfsBlobTransferResult::Committed {
                                    content_hash: commit.content_hash.clone(),
                                },
                            },
                        ),
                    }],
                });
            }
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    },
                ..
            }) => {
                assert_eq!(target_node_id, &requester_for_sender);
                completed_response_calls_for_sender.fetch_add(1, Ordering::SeqCst);
                let (state, changed) = completed_response_gate_for_sender.as_ref();
                let mut state = state.lock().expect("completed response gate");
                state.0 = true;
                changed.notify_all();
                while !state.1 {
                    state = changed.wait(state).expect("completed response release");
                }
            }
            _ => {}
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = served_transport_with_canonical_message_peer(
        primary.clone(),
        vec![requester.clone(), replica.clone()],
        sender,
        Duration::from_secs(1),
    );
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(transport, content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/served-frame/unrelated-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let request_id = "dfs_net_req_served_unrelated_response".to_string();
    let first_result = dfs
        .process_message_command(test_dfs_message_command(
            "dfs_served_unrelated_response_write_rpc",
            build_dfs_routed_message(
                &requester,
                &primary,
                20,
                21,
                DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
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
                    bytes: b"completed response belongs to an earlier caller".to_vec(),
                    content_hash: None,
                    content_size_bytes: None,
                    requester_blob_pre_staged: true,
                    desired_replica_nodes: vec![primary.clone(), replica.clone()],
                    policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                    object_metadata: None,
                    authority_shard: None,
                    touch_parent_on_create: false,
                    foreground_replica_repair: false,
                    exclusive_create: false,
                }),
            ),
        ))
        .expect("start pending routed write");
    assert!(
        first_result
            .response
            .outbound_messages
            .iter()
            .any(|message| {
                matches!(
                    &message.message,
                    DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                        request_id: response_request_id,
                        result:
                            DfsDistributedWriteResult::Outcome {
                                outcome: NetworkedDfsWriteOutcome::Pending { .. },
                            },
                        ..
                    }) if response_request_id == &request_id
                )
            })
    );
    let stage = staged_command
        .lock()
        .expect("staged command")
        .clone()
        .expect("replica stage command");
    let receipt_message = test_dfs_message_command(
        "dfs_served_unrelated_response_receipt_rpc",
        build_dfs_routed_message(
            &replica,
            &primary,
            22,
            23,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: stage.request_id,
                intent_id: stage.intent_id,
                file_system_id: stage.file_system_id,
                object_id: stage.object_id,
                version: stage.version,
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: stage.content_hash,
                },
            }),
        ),
    );

    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let process_thread = std::thread::spawn(move || {
        let result = dfs.process_message_command(receipt_message);
        let _ = completed_tx.send(());
        (dfs, result)
    });
    if completed_rx.recv_timeout(Duration::from_secs(2)).is_err() {
        let (state, changed) = completed_response_gate.as_ref();
        let mut state = state.lock().expect("release blocked in-call response");
        state.1 = true;
        changed.notify_all();
        drop(state);
        let _ = process_thread.join();
        panic!("receipt RPC waited for an unrelated completed-response RPC");
    }
    let (mut dfs, receipt_result) = process_thread.join().expect("receipt service thread");
    let receipt_result = receipt_result.expect("serve replica receipt");
    assert!(
        !receipt_result
            .response
            .outbound_messages
            .iter()
            .any(|message| {
                matches!(
                    &message.message,
                    DfsNetworkMessage::DistributedWriteResponse(_)
                )
            }),
        "requester A's completion must not be returned on replica B's RPC"
    );
    assert_eq!(completed_response_calls.load(Ordering::SeqCst), 0);
    assert!(
        dfs.writes.pending_remote_write_responses.is_empty(),
        "the detached outbox owns the completed response after enqueue"
    );
    assert!(
        dfs.message_deferred_work_pending()
            .expect("completed response outbox"),
        "the unrelated completed response must remain visible to the daemon worker"
    );

    let mut worker = dfs.prepare_message_deferred_work();
    let worker_thread = std::thread::spawn(move || worker.drain());
    let (state, changed) = completed_response_gate.as_ref();
    let state = state.lock().expect("completed response gate");
    let (mut state, wait) = changed
        .wait_timeout_while(state, Duration::from_secs(2), |state| !state.0)
        .expect("completed response dispatch wait");
    let timed_out = wait.timed_out();
    let released_before_assertion = state.1;
    state.1 = true;
    changed.notify_all();
    drop(state);
    worker_thread
        .join()
        .expect("completed response worker thread")
        .expect("drain completed response outbox");
    assert!(
        !timed_out,
        "detached worker did not dispatch the completed response"
    );
    assert!(
        !released_before_assertion,
        "the current receipt command must have returned before response release"
    );
    assert_eq!(completed_response_calls.load(Ordering::SeqCst), 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn served_non_foreground_write_does_not_retry_unrelated_repairs() {
    let root = temp_root("served-non-foreground-write-skips-unrelated-repairs");
    let requester = NodeId::from_legacy_named("node_served_deferred_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_served_deferred_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_served_deferred_replica").expect("replica");
    let backlog_object =
        DfsObjectId::from_legacy_named("obj_served_deferred_backlog").expect("object");
    let routed_object =
        DfsObjectId::from_legacy_named("obj_served_deferred_routed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_served_deferred_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_served_deferred_2").expect("version");
    let repair_calls = Arc::new(Mutex::new(Vec::new()));
    let repair_calls_for_sender = Arc::clone(&repair_calls);
    let replica_for_sender = replica.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        assert_eq!(target_node_id, &replica_for_sender);
        if let message @ (DfsNetworkMessage::RepairReplica(_) | DfsNetworkMessage::StageBlob(_)) =
            &command.routed_message.message
        {
            repair_calls_for_sender
                .lock()
                .expect("repair calls lock")
                .push(dfs_message_trace_label(message).to_string());
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
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

    let mut backlog_metadata = file_metadata(primary.clone());
    backlog_metadata.object_id = backlog_object.clone();
    backlog_metadata.path = "/services/served-frame/backlog.bin".to_string();
    backlog_metadata.version = version_1.clone();
    backlog_metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    backlog_metadata.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(backlog_metadata)
        .expect("backlog object metadata");

    let backlog_write = dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: backlog_object,
                target_version: version_2.clone(),
                requesting_node_id: primary.clone(),
            },
            std::io::Cursor::new(b"repair backlog bytes".to_vec()),
            vec![primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("backlog write");
    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = backlog_write else {
        panic!("expected committed backlog write, got {backlog_write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    repair_calls.lock().expect("repair calls lock").clear();

    let mut routed_metadata = file_metadata(primary.clone());
    routed_metadata.object_id = routed_object.clone();
    routed_metadata.path = "/services/served-frame/routed-deferred.bin".to_string();
    routed_metadata.version = version_1.clone();
    routed_metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    routed_metadata.current_locations = vec![requester.clone()];
    dfs.add_object_metadata(routed_metadata)
        .expect("routed object metadata");

    let request_id = "dfs_net_req_served_deferred".to_string();
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: request_id.clone(),
        request: DfsWriteRequest {
            file_system_id: sifs_file_system_id(),
            object_id: routed_object.clone(),
            target_version: version_2.clone(),
            requesting_node_id: requester.clone(),
        },
        expected_base_version: Some(version_1),
        metadata_journal_after_sequence: None,
        bytes: b"served routed deferred bytes".to_vec(),
        content_hash: None,
        content_size_bytes: None,
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![requester.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
        object_metadata: None,
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: false,
        exclusive_create: false,
    };
    let routed_message = build_dfs_routed_message(
        &requester,
        &primary,
        12,
        13,
        DfsNetworkMessage::DistributedWriteRequest(command),
    );

    let result = dfs
        .process_message_command(test_dfs_message_command(
            "dfs_served_deferred_message_rpc",
            routed_message,
        ))
        .expect("process DFS message");

    assert!(
        repair_calls.lock().expect("repair calls lock").is_empty(),
        "non-foreground routed write must not retry unrelated repair backlog"
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
                                NetworkedDfsWriteOutcome::Committed {
                                    version,
                                    ..
                                },
                        },
                    ..
                }) if response_request_id.as_str() == request_id.as_str()
                    && response_object_id == &routed_object
                    && version == &version_2
            )
        }),
        "served message should return committed deferred write response, got {responses:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn served_foreground_write_returns_committed_response_before_replica_repair_completes() {
    let root = temp_root("served-foreground-write-commits-before-repair");
    let requester = NodeId::from_legacy_named("node_served_scoped_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_served_scoped_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_served_scoped_replica").expect("replica");
    let backlog_object =
        DfsObjectId::from_legacy_named("obj_served_scoped_backlog").expect("object");
    let routed_object = DfsObjectId::from_legacy_named("obj_served_scoped_routed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_served_scoped_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_served_scoped_2").expect("version");
    let transfer_objects = Arc::new(Mutex::new(Vec::<DfsObjectId>::new()));
    let transfer_objects_for_sender = Arc::clone(&transfer_objects);
    let replica_for_sender = replica.clone();
    let sender: DfsMessageSender = Arc::new(move |target_node_id, command, _timeout| {
        assert_eq!(target_node_id, &replica_for_sender);
        match &command.routed_message.message {
            DfsNetworkMessage::RepairReplica(command) => transfer_objects_for_sender
                .lock()
                .expect("transfer objects")
                .push(command.object_id.clone()),
            DfsNetworkMessage::StageBlob(command) => transfer_objects_for_sender
                .lock()
                .expect("transfer objects")
                .push(command.object_id.clone()),
            _ => {}
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
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

    let mut backlog_metadata = file_metadata(primary.clone());
    backlog_metadata.object_id = backlog_object.clone();
    backlog_metadata.path = "/services/served-frame/scoped-backlog.bin".to_string();
    backlog_metadata.version = version_1.clone();
    backlog_metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    backlog_metadata.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(backlog_metadata)
        .expect("backlog object metadata");
    let backlog_write = dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: backlog_object.clone(),
                target_version: version_2.clone(),
                requesting_node_id: primary.clone(),
            },
            std::io::Cursor::new(b"unrelated repair backlog bytes".to_vec()),
            vec![primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("backlog write");
    assert!(matches!(
        backlog_write,
        NetworkedDfsWriteOutcome::Committed { repair_jobs, .. }
            if repair_jobs.len() == 1 && repair_jobs[0].missing_replica_node_id == replica
    ));
    transfer_objects.lock().expect("transfer objects").clear();

    let mut routed_metadata = file_metadata(primary.clone());
    routed_metadata.object_id = routed_object.clone();
    routed_metadata.path = "/services/served-frame/scoped-routed.bin".to_string();
    routed_metadata.version = version_1.clone();
    routed_metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    routed_metadata.current_locations = vec![requester.clone()];
    dfs.add_object_metadata(routed_metadata)
        .expect("routed object metadata");

    let request_id = "dfs_net_req_served_scoped".to_string();
    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: request_id.clone(),
        request: DfsWriteRequest {
            file_system_id: sifs_file_system_id(),
            object_id: routed_object.clone(),
            target_version: version_2,
            requesting_node_id: requester.clone(),
        },
        expected_base_version: Some(version_1),
        metadata_journal_after_sequence: None,
        bytes: b"served scoped foreground bytes".to_vec(),
        content_hash: None,
        content_size_bytes: None,
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![requester.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
        object_metadata: None,
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: true,
        exclusive_create: false,
    };
    let routed_message = build_dfs_routed_message(
        &requester,
        &primary,
        14,
        15,
        DfsNetworkMessage::DistributedWriteRequest(command),
    );

    let result = dfs
        .process_message_command(test_dfs_message_command(
            "dfs_served_scoped_message_rpc",
            routed_message,
        ))
        .expect("process DFS message");

    let transfer_objects = transfer_objects.lock().expect("transfer objects");
    assert!(
        transfer_objects.is_empty(),
        "foreground served message must return before inline repair retries: {transfer_objects:?}"
    );
    assert!(
        result
            .response
            .outbound_messages
            .iter()
            .any(|message| matches!(
                &message.message,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: response_request_id,
                    result:
                        DfsDistributedWriteResult::Outcome {
                            outcome:
                                NetworkedDfsWriteOutcome::Committed {
                                    intent_id,
                                    repair_jobs,
                                    ..
                                },
                        },
                    ..
                }) if response_request_id == &request_id
                    && repair_jobs.len() == 1
                    && repair_jobs[0].intent_id == *intent_id
                    && repair_jobs[0].object_id == routed_object
                    && repair_jobs[0].missing_replica_node_id == replica
            )),
        "foreground served message should return the committed outcome with pending repair jobs"
    );
    assert!(
        !result
            .response
            .outbound_messages
            .iter()
            .any(|message| matches!(
                &message.message,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: response_request_id,
                    result:
                        DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Pending { .. },
                        },
                    ..
                }) if response_request_id == &request_id
            )),
        "a served transport that may return a durable commit before replica repair must not emit a pending response first"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn served_sync_path_does_not_retry_repairs_inline() {
    let root = temp_root("served-sync-path-skips-inline-repair");
    let requester =
        NodeId::from_legacy_named("node_served_frame_repair_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_served_frame_repair_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_served_frame_repair_replica").expect("replica");
    let path = "/services/served-frame/repair-retry-patch.bin";
    let send_attempts = Arc::new(Mutex::new(0usize));
    let send_attempts_for_sender = Arc::clone(&send_attempts);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, _command, _timeout| {
        *send_attempts_for_sender.lock().expect("send attempts") += 1;
        Ok(DfsMessageResponse {
            request_id: "dropped-stage".to_string(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
        primary.clone(),
        vec![replica.clone()],
        sender,
        Duration::from_millis(1),
    );
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(transport, content)
        .with_metadata_backend(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("served-repair"),
                root.join("metadata"),
            )
            .expect("metadata backend"),
        )
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    let mut initial = vec![0_u8; 256 * 1024 + 31];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    dfs.posix_mkdir_all("/services/served-frame")
        .expect("served-frame directory");
    dfs.posix_write_file(
        path,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
    )
    .expect("initial self write");
    let base_object = dfs.posix_stat(path).expect("stat base object");
    dfs.content()
        .put_object_version(
            base_object.object_id.as_str(),
            base_object.version.as_str(),
            &initial,
        )
        .expect("seed base object version manifest");

    let mut patch = vec![0_u8; 80 * 1024 + 7];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    dfs.posix_write_file_at(
        path,
        96 * 1024 + 5,
        &patch,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
            replica_candidates: vec![primary.clone(), replica.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    )
    .expect("patch write");
    let pending_patch_hash = dfs
        .writes
        .pending_writes
        .values()
        .find_map(|pending| pending.patch.as_ref().map(|patch| patch.patch_hash.clone()))
        .expect("pending patch hash");
    dfs.content()
        .debug_remove_loose_blob(&pending_patch_hash)
        .expect("remove patch blob");
    *send_attempts.lock().expect("send attempts") = 0;
    dfs.repair_retry_tick = 0;
    for pending in dfs.writes.pending_writes.values_mut() {
        for state in pending.stage_retries.values_mut() {
            state.next_retry_tick = 0;
            state.inflight = false;
        }
        for state in pending.repair_retries.values_mut() {
            state.next_retry_tick = 0;
            state.inflight = false;
        }
    }

    let routed_message = build_dfs_routed_message(
        &requester,
        &primary,
        77,
        77,
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "served_frame_sync_after_repair_failure".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::SyncPath {
                path: "/services".to_string(),
            },
        }),
    );

    let result = dfs
        .process_message_command(test_dfs_message_command(
            "dfs_served_repair_retry_message_rpc",
            routed_message,
        ))
        .expect("sync path should not fail on queued repair backlog");

    assert_eq!(*send_attempts.lock().expect("send attempts"), 0);
    assert!(
        !result
            .events
            .iter()
            .any(|event| matches!(event, NetworkedDfsServiceEvent::RepairRetryFailed { .. })),
        "sync path should not run or report inline repair retry work: {:?}",
        result.events
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_content_anti_entropy_pages_and_repairs_stale_locations() {
    let root = temp_root("local-content-anti-entropy");
    let node_id = NodeId::from_legacy_named("node_anti_entropy").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("fs_anti_entropy").expect("fs");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let file_system = DfsFileSystem {
        metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: file_system_id.clone(),
        display_name: "anti entropy".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_file_system(&file_system)
        .expect("filesystem");

    let mut stale_a = file_metadata(node_id.clone());
    stale_a.file_system_id = file_system_id.clone();
    stale_a.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_stale_a").expect("object");
    stale_a.version = DfsVersion::from_legacy_named("version_anti_entropy_stale_a").expect("ver");
    stale_a.path = "/anti/000-stale-a.bin".to_string();
    stale_a.current_locations = vec![node_id.clone()];
    stale_a.content_hash = Some(content_hash(b"missing local bytes a"));
    stale_a.content_size_bytes = Some(21);

    let mut stale_b = file_metadata(node_id.clone());
    stale_b.file_system_id = file_system_id.clone();
    stale_b.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_stale_b").expect("object");
    stale_b.version = DfsVersion::from_legacy_named("version_anti_entropy_stale_b").expect("ver");
    stale_b.path = "/anti/001-stale-b.bin".to_string();
    stale_b.current_locations = vec![node_id.clone()];
    stale_b.content_hash = Some(content_hash(b"missing local bytes b"));
    stale_b.content_size_bytes = Some(21);

    let mut healthy = file_metadata(node_id.clone());
    healthy.file_system_id = file_system_id.clone();
    healthy.object_id = DfsObjectId::from_legacy_named("obj_anti_entropy_healthy").expect("object");
    healthy.version = DfsVersion::from_legacy_named("version_anti_entropy_healthy").expect("ver");
    healthy.path = "/anti/002-healthy.bin".to_string();
    let healthy_manifest = dfs
        .content
        .put_object_version(
            healthy.object_id.as_str(),
            healthy.version.as_str(),
            b"healthy local bytes",
        )
        .expect("healthy content");
    healthy.current_locations = vec![node_id.clone()];
    healthy.content_hash = Some(healthy_manifest.content_hash.clone());
    healthy.content_size_bytes = Some(healthy_manifest.size_bytes);

    {
        let backend = dfs.metadata_backend.as_mut().expect("metadata backend");
        backend.put_object(&stale_a).expect("stale a metadata");
        backend.put_object(&stale_b).expect("stale b metadata");
        backend.put_object(&healthy).expect("healthy metadata");
    }

    let first = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: file_system_id.clone(),
            after: None,
            max_objects: 1,
            max_issues: 1,
            repair_metadata: true,
        })
        .expect("first anti-entropy page");
    assert_eq!(first.scanned_object_count, 1);
    assert_eq!(first.checked_local_location_count, 1);
    assert_eq!(first.stale_local_location_count, 1);
    assert_eq!(first.repaired_location_count, 1);
    assert_eq!(first.issue_count, 1);
    assert_eq!(first.issues.len(), 1);
    assert!(first.issues[0].repaired);
    assert!(first.next_cursor.is_some());

    let backend = dfs.metadata_backend().expect("metadata backend");
    let repaired_a = backend
        .get_object(&stale_a.object_id)
        .expect("stale a object")
        .expect("stale a after repair");
    assert!(!repaired_a.current_locations.contains(&node_id));
    let repaired_a_version = backend
        .get_version(&stale_a.object_id, &stale_a.version)
        .expect("stale a version")
        .expect("stale a version after repair");
    assert!(!repaired_a_version.locations.contains(&node_id));

    let second = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: file_system_id.clone(),
            after: first.next_cursor,
            max_objects: 8,
            max_issues: 8,
            repair_metadata: true,
        })
        .expect("second anti-entropy page");
    assert_eq!(second.scanned_object_count, 2);
    assert_eq!(second.checked_local_location_count, 2);
    assert_eq!(second.stale_local_location_count, 1);
    assert_eq!(second.repaired_location_count, 1);
    assert!(second.next_cursor.is_none());

    let backend = dfs.metadata_backend().expect("metadata backend");
    let repaired_b = backend
        .get_object(&stale_b.object_id)
        .expect("stale b object")
        .expect("stale b after repair");
    assert!(!repaired_b.current_locations.contains(&node_id));
    let healthy_object = backend
        .get_object(&healthy.object_id)
        .expect("healthy object")
        .expect("healthy remains");
    assert!(healthy_object.current_locations.contains(&node_id));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_content_anti_entropy_repairs_size_from_verified_hash_addressed_blob() {
    let root = temp_root("local-content-anti-entropy-repairs-size");
    let node_id = NodeId::from_legacy_named("node_anti_entropy_size").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("fs_anti_entropy_size").expect("fs");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let file_system = DfsFileSystem {
        metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: file_system_id.clone(),
        display_name: "anti entropy size".to_string(),
        owner_node_id: node_id.clone(),
        subtree_policies: Vec::new(),
    };
    dfs.add_file_system(file_system).expect("filesystem");

    let bytes = b"verified hash-addressed bytes with incorrect metadata size";
    let mut object = file_metadata(node_id.clone());
    object.file_system_id = file_system_id.clone();
    object.object_id =
        DfsObjectId::from_legacy_named("obj_anti_entropy_wrong_size").expect("object");
    object.version = DfsVersion::from_legacy_named("version_wrong_size").expect("ver");
    object.path = "/anti/wrong-size.bin".to_string();
    let manifest = dfs
        .content
        .put_object_version_blob_only(object.object_id.as_str(), object.version.as_str(), bytes)
        .expect("hash-addressed blob");
    object.current_locations = vec![node_id.clone()];
    object.content_hash = Some(manifest.content_hash.clone());
    object.content_size_bytes = Some(0);
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_object_and_version(&object, &object_version_record(&object, Some(0)))
        .expect("incorrect-size metadata");

    let report = dfs
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id,
            after: None,
            max_objects: 8,
            max_issues: 8,
            repair_metadata: true,
        })
        .expect("anti entropy");

    assert_eq!(report.stale_local_location_count, 1);
    assert_eq!(report.repaired_location_count, 1);
    assert_eq!(report.issue_count, 1);
    assert!(report.issues[0].repaired);
    let backend = dfs.metadata_backend().expect("metadata backend");
    let repaired = backend
        .get_object(&object.object_id)
        .expect("object")
        .expect("repaired object");
    assert_eq!(repaired.content_size_bytes, Some(bytes.len() as u64));
    let repaired_version = backend
        .get_version(&object.object_id, &object.version)
        .expect("version")
        .expect("repaired version");
    assert_eq!(repaired_version.size_bytes, Some(bytes.len() as u64));
    let repaired_manifest = dfs
        .content
        .read_manifest(object.object_id.as_str(), object.version.as_str())
        .expect("repaired manifest");
    assert_eq!(repaired_manifest.size_bytes, bytes.len() as u64);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_receipt_completion_sends_untracked_routed_write_response() {
    let root = temp_root("fetch-receipt-sends-untracked-routed-response");
    let primary =
        NodeId::from_legacy_named("node_fetch_receipt_response_primary").expect("primary");
    let requester =
        NodeId::from_legacy_named("node_fetch_receipt_response_requester").expect("requester");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_receipt_response").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_fetch_receipt_response_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_fetch_receipt_response_2").expect("version");
    let bytes = b"fetch receipt completion must notify stateless routed writer";
    let network = InProcessNetwork::new();
    let mut requester_mailbox = network.add_node(requester.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let manifest = content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local fetched content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/stateless-fetch-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![requester.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: requester.clone(),
            },
            Some(version_1),
            manifest.content_hash.clone(),
            manifest.size_bytes,
            vec![requester.clone(), primary.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("pending remote-manifest write");
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("expected pending write, got {outcome:?}");
    };
    assert_eq!(durable_receipts, 1);
    assert_eq!(required_receipts, 2);
    let _ = drain_dfs_messages(&mut requester_mailbox);

    dfs.record_local_pending_write_receipt_if_desired(
        &intent_id,
        &object_id,
        &version_2,
        &manifest.content_hash,
    )
    .expect("record local fetched receipt");

    let messages = drain_dfs_messages(&mut requester_mailbox);
    assert!(
        messages.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id,
                    object_id: response_object_id,
                    result:
                        DfsDistributedWriteResult::Outcome {
                            outcome:
                                NetworkedDfsWriteOutcome::Committed {
                                    intent_id: response_intent_id,
                                    version,
                                    content_hash,
                                    ..
                                },
                        },
                    ..
                }) if request_id == &intent_id
                    && response_intent_id == &intent_id
                    && response_object_id == &object_id
                    && version == &version_2
                    && content_hash == &manifest.content_hash
            )
        }),
        "primary must notify the routed requester after local fetch receipt commits, got {messages:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_inline_blob_records_requester_receipt_without_restaging() {
    let root = temp_root("routed-inline-records-requester-receipt");
    let primary = NodeId::from_legacy_named("node_routed_inline_receipt_primary").expect("primary");
    let writer = NodeId::from_legacy_named("node_routed_inline_receipt_writer").expect("writer");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_inline_receipt").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_routed_inline_receipt_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_routed_inline_receipt_2").expect("version");
    let network = InProcessNetwork::new();
    let mut writer_mailbox = network.add_node(writer.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/routed-inline-receipt.bin".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            Some(version_1),
            b"complete inline routed bytes".to_vec(),
            vec![writer.clone(), primary.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("inline routed write");

    assert!(
        matches!(
            &outcome,
            NetworkedDfsWriteOutcome::Committed {
                version,
                repair_jobs,
                ..
            } if version == &version_2 && repair_jobs.is_empty()
        ),
        "primary should commit from requester+primary receipts without repair: {outcome:?}"
    );
    let writer_messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        writer_messages.iter().all(|message| {
            !matches!(
                message,
                DfsNetworkMessage::StageBlob(_)
                    | DfsNetworkMessage::FetchBlob(_)
                    | DfsNetworkMessage::FetchBlobResponse(_)
            )
        }),
        "primary must not transfer inline blob bytes back to the requester: {writer_messages:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stateless_node_route_reader_write_commits_after_primary_fetch_receipt() {
    let root = temp_root("stateless-node_route-reader-write-primary-fetch");
    let writer =
        NodeId::from_legacy_named("node_stateless_node_route_writer").expect("writer node");
    let primary =
        NodeId::from_legacy_named("node_stateless_node_route_primary").expect("primary node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_stateless_node_route_fetch").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stateless_node_route_fetch_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stateless_node_route_fetch_2").expect("version");
    let file_system = file_system(primary.clone());
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
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
    let primary_content_root = root.join("primary-content");
    let primary_metadata_root = root.join("primary-metadata");
    let trace = Arc::new(Mutex::new(Vec::new()));
    {
        let mut primary_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("services"),
            &primary_metadata_root,
        )
        .expect("primary metadata");
        primary_backend
            .put_file_system(&file_system)
            .expect("primary filesystem");
        for object in distributed_sifs_bootstrap_objects(file_system.owner_node_id.clone()) {
            primary_backend
                .put_object(&object)
                .expect("primary SIFS bootstrap object");
        }
    }
    let peer = Arc::new(StatelessPrimaryPeerTransport {
        primary_node_id: primary.clone(),
        content_root: primary_content_root,
        metadata_root: primary_metadata_root,
        control_plane: control_plane.clone(),
        trace: Arc::clone(&trace),
    });
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer;
    let transport = DfsNodeTransport::with_peer_transports_for_nodes(
        writer.clone(),
        vec![primary.clone()],
        Duration::from_millis(1),
        peer_transport,
        async_peer_transport,
    );
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let mut dfs =
        NetworkedDfsService::new(transport, writer_content).with_control_plane(control_plane);
    dfs.add_file_system(file_system.clone())
        .expect("writer filesystem");
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/stateless-fetch.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_transient_object_metadata(object)
        .expect("stateless writer object view");

    let outcome = dfs
        .write_distributed_eventual_from_reader(
            DfsWriteRequest {
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            std::io::Cursor::new(b"stateless routed reader bytes".to_vec()),
            vec![writer.clone(), primary.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("routed reader write");
    let NetworkedDfsWriteOutcome::Pending { mut intent_id, .. } = outcome else {
        panic!("expected pending routed write, got {outcome:?}");
    };
    let mut committed = None;
    for _ in 0..20 {
        let _ = dfs.process_incoming().expect("process routed frames");
        let next = dfs.write_outcome(&intent_id).expect("write outcome");
        match next {
            NetworkedDfsWriteOutcome::Pending {
                intent_id: next_intent_id,
                ..
            } => {
                intent_id = next_intent_id;
                std::thread::sleep(Duration::from_millis(10));
            }
            outcome => {
                committed = Some(outcome);
                break;
            }
        }
    }
    let committed = committed.unwrap_or_else(|| {
        panic!(
            "routed write should commit; trace={:?}",
            trace.lock().expect("trace")
        )
    });

    assert!(
        matches!(
            &committed,
            NetworkedDfsWriteOutcome::Committed { version, .. } if version == &version_2
        ),
        "unexpected served routed write outcome: {committed:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_streamed_write_preserves_nonforeground_repair_mode() {
    let root = temp_root("routed-inline-nonforeground-repair");
    let writer = NodeId::from_legacy_named("node_routed_inline_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_routed_inline_primary").expect("primary");
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

    let captured_commands = Arc::new(Mutex::new(Vec::<DfsDistributedWriteCommand>::new()));
    let captured_commands_for_sender = Arc::clone(&captured_commands);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            captured_commands_for_sender
                .lock()
                .expect("capture")
                .push(write_command.clone());
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
    object.path = "/routed-inline-nonforeground.bin".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    let writer_content = LocalContentStore::new(root.join("writer-content")).expect("content");
    let metadata = seeded_durable_metadata_replica(
        root.join("metadata"),
        vec![shard.clone()],
        &file_system,
        &object,
    );
    let mut dfs = NetworkedDfsService::new(transport, writer_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach metadata replica");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.replication
        .local_metadata_journal_cursors
        .insert(DfsMetadataShardId::new("root"), 43);

    dfs.route_distributed_bytes(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_inline_nonforeground")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        b"routed-inline-nonforeground".to_vec(),
        vec![writer.clone(), primary.clone()],
        DfsReplicationPolicy::new(1, 2).expect("policy"),
        object.clone(),
        shard.clone(),
        DfsDistributedWriteOptions {
            publication: Default::default(),
            touch_parent_on_create: true,
            creation: DfsWriteCreation::Upsert,
            repair: DfsWriteRepair::Background,
            dispatch: DfsWriteDispatch::AwaitResponse,
            replay_response_journal: true,
            operation_id: None,
        },
    )
    .expect("route inline nonforeground write");
    dfs.route_distributed_bytes(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_inline_no_replay")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        b"routed-inline-no-replay".to_vec(),
        vec![writer.clone(), primary.clone()],
        DfsReplicationPolicy::new(1, 2).expect("policy"),
        object.clone(),
        shard.clone(),
        DfsDistributedWriteOptions {
            publication: Default::default(),
            touch_parent_on_create: true,
            creation: DfsWriteCreation::Upsert,
            repair: DfsWriteRepair::Background,
            dispatch: DfsWriteDispatch::AwaitResponse,
            replay_response_journal: false,
            operation_id: None,
        },
    )
    .expect("route inline write without response journal");
    dfs.route_distributed_bytes(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_inline_detached")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        b"routed-inline-detached".to_vec(),
        vec![writer, primary],
        DfsReplicationPolicy::new(1, 2).expect("policy"),
        object,
        shard,
        DfsDistributedWriteOptions {
            publication: Default::default(),
            touch_parent_on_create: true,
            creation: DfsWriteCreation::Upsert,
            repair: DfsWriteRepair::Background,
            dispatch: DfsWriteDispatch::Detached,
            replay_response_journal: true,
            operation_id: None,
        },
    )
    .expect("route detached inline write");

    assert_eq!(
        captured_commands.lock().expect("capture").len(),
        3,
        "owned payloads are streamed before return even when metadata completion is detached"
    );
    dfs.flush_detached_transport_sends()
        .expect("streamed write leaves no payload send queued");

    let commands = captured_commands.lock().expect("capture").clone();
    assert_eq!(commands.len(), 3);
    assert!(
        commands
            .iter()
            .all(|command| !command.requester_blob_pre_staged),
        "the primary must not credit the streaming requester with a local blob"
    );
    assert!(
        !commands[0].foreground_replica_repair,
        "inline routed writes must preserve nonforeground repair mode"
    );
    assert_eq!(
        commands[0].metadata_journal_after_sequence,
        Some(43),
        "synchronous inline routed writes should request response journal replay from the cached applied cursor"
    );
    assert!(
        commands[1].metadata_journal_after_sequence.is_none(),
        "synchronous inline routed writes may skip response journal replay"
    );
    assert!(
        commands[2].metadata_journal_after_sequence.is_none(),
        "detached inline routed writes should skip response journal replay"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_remote_manifest_repair_fetches_from_durable_requester() {
    let root = temp_root("remote-manifest-commit-fetches-repair-source");
    let writer = NodeId::from_legacy_named("node_remote_manifest_writer").expect("writer node");
    let primary = NodeId::from_legacy_named("node_remote_manifest_primary").expect("primary node");
    let replica = NodeId::from_legacy_named("node_remote_manifest_replica").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_remote_manifest_fetch").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_remote_manifest_fetch_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_remote_manifest_fetch_2").expect("version");
    let bytes = b"reader backed bytes stay on the writer until the primary fetches them";
    let content_hash = content_hash(bytes);
    let network = InProcessNetwork::new();
    let mut writer_mailbox = network.add_node(writer.clone());
    let content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let metadata =
        open_root_local_metadata_backend(root.join("primary-metadata")).expect("primary metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/remote-manifest-fetch.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            Some(version_1),
            content_hash.clone(),
            bytes.len() as u64,
            vec![writer.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("remote manifest write");
    let intent_id = match outcome {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert!(
                repair_jobs
                    .iter()
                    .any(|job| job.missing_replica_node_id == replica)
            );
            intent_id
        }
        other => panic!("expected threshold commit with repair job, got {other:?}"),
    };

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending repair");
    assert!(
        pending.fetch_inflight_from.contains(&writer),
        "primary must fetch committed repair content from the durable writer"
    );
    let messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        messages.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::FetchBlob(command)
                    if command.intent_id == intent_id
                        && command.object_id == object_id
                        && command.version == version_2
                        && command.content_hash == content_hash
            )
        }),
        "expected FetchBlob to writer after threshold commit, got {messages:?}"
    );

    dfs.writes.pending_writes.clear();
    let scheduled = dfs
        .retry_pending_repairs()
        .expect("restore persisted repair and request fetch");
    assert!(
        scheduled.is_empty(),
        "primary should fetch missing repair content before scheduling repair"
    );
    let restored_pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("metadata primary restores pending repair write");
    assert!(
        restored_pending.fetch_inflight_from.contains(&writer),
        "restored pending repair should fetch from durable writer"
    );
    let retry_messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        retry_messages.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::FetchBlob(command)
                    if command.intent_id == intent_id
                        && command.object_id == object_id
                        && command.version == version_2
                        && command.content_hash == content_hash
            )
        }),
        "expected retry FetchBlob after restoring pending repair, got {retry_messages:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_remote_manifest_self_repair_fetches_from_durable_requester() {
    let root = temp_root("remote-manifest-self-repair-fetches-source");
    let writer =
        NodeId::from_legacy_named("node_remote_manifest_self_writer").expect("writer node");
    let primary =
        NodeId::from_legacy_named("node_remote_manifest_self_primary").expect("primary node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_remote_manifest_self_fetch").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_remote_manifest_self_fetch_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_remote_manifest_self_fetch_2").expect("version");
    let bytes = b"reader backed bytes must be fetched when the primary itself is missing";
    let content_hash = content_hash(bytes);
    let network = InProcessNetwork::new();
    let mut writer_mailbox = network.add_node(writer.clone());
    let content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let metadata =
        open_root_local_metadata_backend(root.join("primary-metadata")).expect("primary metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/remote-manifest-self-fetch.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            Some(version_1),
            content_hash.clone(),
            bytes.len() as u64,
            vec![writer.clone(), primary.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("remote manifest write");
    let intent_id = match outcome {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert!(
                repair_jobs
                    .iter()
                    .any(|job| job.missing_replica_node_id == primary)
            );
            intent_id
        }
        other => panic!("expected threshold commit with self repair job, got {other:?}"),
    };

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending repair");
    assert!(
        pending.fetch_inflight_from.contains(&writer),
        "primary must fetch committed self-repair content from durable writer"
    );
    let messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        messages.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::FetchBlob(command)
                    if command.intent_id == intent_id
                        && command.object_id == object_id
                        && command.version == version_2
                        && command.content_hash == content_hash
            )
        }),
        "expected self-repair FetchBlob to writer after threshold commit, got {messages:?}"
    );

    dfs.writes.pending_writes.clear();
    let scheduled = dfs
        .retry_pending_repairs()
        .expect("restore persisted self repair and request fetch");
    assert!(
        scheduled.is_empty(),
        "self repair should fetch missing local content before any remote repair send"
    );
    let restored_pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("metadata primary restores self repair write");
    assert!(
        restored_pending.fetch_inflight_from.contains(&writer),
        "restored self repair should fetch from durable writer"
    );
    let retry_messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        retry_messages.iter().any(|message| {
            matches!(
                message,
                DfsNetworkMessage::FetchBlob(command)
                    if command.intent_id == intent_id
                        && command.object_id == object_id
                        && command.version == version_2
                        && command.content_hash == content_hash
            )
        }),
        "expected retry self-repair FetchBlob after restoring pending repair, got {retry_messages:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn nonforeground_remote_manifest_self_repair_defers_fetch_from_durable_requester() {
    let root = temp_root("remote-manifest-self-repair-defers-fetch");
    let writer =
        NodeId::from_legacy_named("node_remote_manifest_defer_writer").expect("writer node");
    let primary =
        NodeId::from_legacy_named("node_remote_manifest_defer_primary").expect("primary node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_remote_manifest_defer_fetch").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_remote_manifest_defer_fetch_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_remote_manifest_defer_fetch_2").expect("version");
    let bytes = b"reader backed bytes should not be fetched by nonforeground write";
    let content_hash = content_hash(bytes);
    let network = InProcessNetwork::new();
    let mut writer_mailbox = network.add_node(writer.clone());
    let content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let metadata =
        open_root_local_metadata_backend(root.join("primary-metadata")).expect("primary metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/remote-manifest-defer-fetch.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    // Exercise the routed handler, including work queued after its response.
    // Calling write_remote_distributed_manifest directly misses that boundary.
    let event = dfs
        .handle_distributed_write_request(
            writer.clone(),
            DfsDistributedWriteCommand {
                publication: Default::default(),
                request_id: "background-routed-manifest".into(),
                request: DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: writer.clone(),
                },
                expected_base_version: Some(version_1),
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(content_hash.clone()),
                content_size_bytes: Some(bytes.len() as u64),
                requester_blob_pre_staged: true,
                desired_replica_nodes: vec![writer.clone(), primary.clone()],
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                object_metadata: None,
                authority_shard: None,
                touch_parent_on_create: false,
                foreground_replica_repair: false,
                exclusive_create: false,
            },
        )
        .expect("nonforeground routed manifest write");
    let NetworkedDfsServiceEvent::RemoteWriteServed {
        result: DfsDistributedWriteResult::Outcome { outcome },
        ..
    } = event
    else {
        panic!("expected committed routed write, got {event:?}");
    };
    let intent_id = match outcome {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert!(
                repair_jobs
                    .iter()
                    .any(|job| job.missing_replica_node_id == primary)
            );
            intent_id
        }
        other => panic!("expected threshold commit with deferred self repair, got {other:?}"),
    };

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending repair");
    assert!(
        !pending.fetch_inflight_from.contains(&writer),
        "nonforeground writes must not fetch committed self-repair content before returning"
    );
    let messages = drain_dfs_messages(&mut writer_mailbox);
    assert!(
        messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::FetchBlob(_))),
        "nonforeground remote manifest write should not synchronously fetch missing local copy, got {messages:?}"
    );

    // The foreground shortcut must not lose repair ownership. Rebuild pending
    // work from durable metadata and let the maintenance path fetch the bytes.
    dfs.writes.pending_writes.clear();
    dfs.retry_pending_repairs().expect("resume durable repair");
    assert!(
        drain_dfs_messages(&mut writer_mailbox)
            .iter()
            .any(|message| {
                matches!(message, DfsNetworkMessage::FetchBlob(command)
            if command.intent_id == intent_id && command.version == version_2)
            })
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn eventual_write_with_local_receipt_tolerates_unreachable_desired_replica() {
    let root = temp_root("eventual-write-tolerates-unreachable-replica");
    let owner = NodeId::from_legacy_named("node_eventual_tolerates_owner").expect("owner node");
    let replica =
        NodeId::from_legacy_named("node_eventual_tolerates_replica").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_eventual_tolerates").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(owner.clone()),
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"eventual bytes".to_vec(),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write should commit locally");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_manifest_write_deferred_foreground_repair_skips_post_commit_fetch() {
    let root = temp_root("remote-manifest-deferred-foreground-fetch");
    let primary =
        NodeId::from_legacy_named("node_remote_manifest_deferred_primary").expect("primary node");
    let requester = NodeId::from_legacy_named("node_remote_manifest_deferred_requester")
        .expect("requester node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_remote_manifest_deferred_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(primary.clone()),
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut metadata = file_metadata(primary.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: requester.clone(),
            },
            Some(version_1),
            content_hash(b"remote manifest deferred bytes"),
            30,
            vec![primary.clone(), requester],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Background,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("deferred foreground repair must not synchronously fetch a missing local copy");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write with deferred repair, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, primary);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reader_write_can_defer_post_commit_replica_repair() {
    let root = temp_root("reader-write-defer-repair");
    let owner = NodeId::from_legacy_named("node_defer_repair_owner").expect("owner node");
    let replica = NodeId::from_legacy_named("node_defer_repair_replica").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_defer_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_defer_repair_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_defer_repair_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut replica_transport = network.add_node(replica.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let write = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(b"deferred foreground repair".to_vec()),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("write should commit");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    let foreground_messages = drain_dfs_messages(&mut replica_transport);
    assert!(
        foreground_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_))),
        "deferred foreground repair must not stage a post-commit replica inline"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn background_w2_stages_only_quorum_deficit_and_defers_n3_repair() {
    let root = temp_root("background-w2-quorum-deficit");
    let owner = NodeId::from_legacy_named("node_background_w2_owner").expect("owner node");
    let replica_a =
        NodeId::from_legacy_named("node_background_w2_replica_a").expect("replica node");
    let replica_b =
        NodeId::from_legacy_named("node_background_w2_replica_b").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_background_w2").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_background_w2_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_background_w2_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut replica_a_dfs = NetworkedDfsService::new(
        network.add_node(replica_a.clone()),
        LocalContentStore::new(root.join("replica-a-content")).expect("replica content"),
    );
    let mut replica_b_mailbox = network.add_node(replica_b.clone());
    let file_system = file_system(owner.clone());
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_a_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_a_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let bytes = vec![0x5a; 2 * 1024 * 1024 + 4096];
    let outcome = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(bytes),
            vec![owner, replica_a.clone(), replica_b.clone()],
            DfsReplicationPolicy::new(2, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("background W2 write");
    let intent_id = match outcome {
        NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts: 1,
            required_receipts: 2,
        } => intent_id,
        other => panic!("expected one-receipt pending W2 write, got {other:?}"),
    };
    assert!(
        !owner_dfs
            .writes
            .pending_writes
            .get(&intent_id)
            .expect("pending write")
            .foreground_replica_repair
    );
    assert!(
        drain_dfs_messages(&mut replica_b_mailbox).is_empty(),
        "background W2 must reserve only one remote stage slot"
    );

    let replica_events = replica_a_dfs.process_incoming().expect("replica A stage");
    assert!(replica_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            replica_node_id,
            ..
        } if replica_node_id == &replica_a
    )));
    let owner_events = owner_dfs.process_incoming().expect("owner stage receipt");
    assert!(owner_events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            replica_node_id,
            ..
        } if replica_node_id == &replica_a
    )));
    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = owner_dfs
        .write_outcome(&intent_id)
        .expect("committed W2 outcome")
    else {
        panic!("stage receipt should commit the background W2 write");
    };
    assert!(
        repair_jobs
            .iter()
            .any(|job| job.missing_replica_node_id == replica_b)
    );
    assert!(
        drain_dfs_messages(&mut replica_b_mailbox).is_empty(),
        "crossing W in background mode must not synchronously start N-W repair"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn foreground_w2_does_not_repair_a_sibling_stage_already_inflight() {
    let root = temp_root("foreground-w2-stage-repair-disjoint");
    let owner = NodeId::from_legacy_named("node_foreground_w2_owner").expect("owner node");
    let replica_a =
        NodeId::from_legacy_named("node_foreground_w2_replica_a").expect("replica node");
    let replica_b =
        NodeId::from_legacy_named("node_foreground_w2_replica_b").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_foreground_w2").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_foreground_w2_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_foreground_w2_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut replica_a_dfs = NetworkedDfsService::new(
        network.add_node(replica_a.clone()),
        LocalContentStore::new(root.join("replica-a-content")).expect("replica content"),
    );
    let mut replica_b_mailbox = network.add_node(replica_b.clone());
    let file_system = file_system(owner.clone());
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner filesystem");
    replica_a_dfs
        .add_file_system(file_system)
        .expect("replica filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_a_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let bytes = vec![0x6b; 2 * 1024 * 1024 + 4096];
    let outcome = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(bytes),
            vec![owner, replica_a.clone(), replica_b.clone()],
            DfsReplicationPolicy::new(2, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Foreground,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("foreground W2 write");
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("expected pre-quorum foreground write, got {outcome:?}");
    };
    let pending = owner_dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending foreground write");
    assert_eq!(
        pending
            .stage_inflight_request_ids
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([replica_a.clone(), replica_b.clone()])
    );

    let replica_b_stage = drain_dfs_messages(&mut replica_b_mailbox)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::StageBlob(command) if command.intent_id == intent_id => {
                Some(command)
            }
            _ => None,
        })
        .expect("replica B stage command");
    replica_a_dfs.process_incoming().expect("replica A stage");
    owner_dfs.process_incoming().expect("replica A receipt");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = owner_dfs
        .write_outcome(&intent_id)
        .expect("committed foreground W2 outcome")
    else {
        panic!("first remote receipt should publish W");
    };
    assert!(
        repair_jobs
            .iter()
            .any(|job| job.missing_replica_node_id == replica_b)
    );
    assert!(
        drain_dfs_messages(&mut replica_b_mailbox).is_empty(),
        "an inflight StageBlob must suppress a duplicate RepairReplica"
    );

    let late_event = owner_dfs
        .handle_stage_blob_response(
            replica_b.clone(),
            DfsStageBlobResponse {
                request_id: replica_b_stage.request_id,
                intent_id: intent_id.clone(),
                file_system_id: replica_b_stage.file_system_id,
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica_b.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: replica_b_stage.content_hash,
                },
            },
        )
        .expect("late sibling stage response")
        .expect("late sibling stage event");
    assert!(matches!(
        late_event,
        NetworkedDfsServiceEvent::BlobStaged { replica_node_id, .. }
            if replica_node_id == replica_b
    ));
    let post_stage_messages = drain_dfs_messages(&mut replica_b_mailbox);
    assert!(post_stage_messages.iter().any(|message| matches!(
        message,
        DfsNetworkMessage::CommitVersion(command) if command.intent_id == intent_id
    )));
    assert!(
        post_stage_messages
            .iter()
            .all(|message| !matches!(message, DfsNetworkMessage::RepairReplica(_)))
    );
    assert!(matches!(
        owner_dfs.write_outcome(&intent_id).expect("fully replicated outcome"),
        NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.is_empty()
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn foreground_w1_postcommit_targets_use_repair_not_stage() {
    let root = temp_root("foreground-w1-postcommit-repair-only");
    let owner = NodeId::from_legacy_named("node_foreground_w1_owner").expect("owner node");
    let replica_a =
        NodeId::from_legacy_named("node_foreground_w1_replica_a").expect("replica node");
    let replica_b =
        NodeId::from_legacy_named("node_foreground_w1_replica_b").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_foreground_w1").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_foreground_w1_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_foreground_w1_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut replica_a_mailbox = network.add_node(replica_a.clone());
    let mut replica_b_mailbox = network.add_node(replica_b.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let outcome = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(vec![0x3c; 2 * 1024 * 1024 + 4096]),
            vec![owner, replica_a, replica_b],
            DfsReplicationPolicy::new(1, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Foreground,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("foreground W1 write");
    assert!(matches!(
        outcome,
        NetworkedDfsWriteOutcome::Committed { .. }
    ));
    for messages in [
        drain_dfs_messages(&mut replica_a_mailbox),
        drain_dfs_messages(&mut replica_b_mailbox),
    ] {
        assert!(
            messages
                .iter()
                .any(|message| matches!(message, DfsNetworkMessage::RepairReplica(_)))
        );
        assert!(
            messages
                .iter()
                .all(|message| !matches!(message, DfsNetworkMessage::StageBlob(_)))
        );
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn background_local_fetch_crossing_w_preserves_deferred_repair_mode() {
    let root = temp_root("background-local-fetch-preserves-repair-mode");
    let writer = NodeId::from_legacy_named("node_background_fetch_writer").expect("writer node");
    let primary = NodeId::from_legacy_named("node_background_fetch_primary").expect("primary node");
    let replica = NodeId::from_legacy_named("node_background_fetch_replica").expect("replica node");
    let object_id = DfsObjectId::from_legacy_named("obj_background_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_background_fetch_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_background_fetch_2").expect("version");
    let bytes = b"background fetch receipt crosses W without synchronous N repair";
    let expected_hash = content_hash(bytes);
    let network = InProcessNetwork::new();
    let mut writer_mailbox = network.add_node(writer.clone());
    let mut replica_mailbox = network.add_node(replica.clone());
    let content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content);
    dfs.add_file_system(file_system(primary.clone()))
        .expect("filesystem");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![writer.clone()];
    dfs.add_object_metadata(object).expect("object");

    let outcome = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: writer.clone(),
            },
            Some(version_1),
            expected_hash.clone(),
            bytes.len() as u64,
            vec![writer.clone(), primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Background,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("background remote-manifest write");
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
        panic!("expected local-fetch pending write, got {outcome:?}");
    };
    assert!(drain_dfs_messages(&mut writer_mailbox).iter().any(
        |message| matches!(message, DfsNetworkMessage::FetchBlob(command)
            if command.intent_id == intent_id)
    ));
    assert!(drain_dfs_messages(&mut replica_mailbox).is_empty());

    dfs.content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("fetched local blob");
    dfs.record_local_pending_write_receipt_if_desired(
        &intent_id,
        &object_id,
        &version_2,
        &expected_hash,
    )
    .expect("record fetched local receipt");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } =
        dfs.write_outcome(&intent_id).expect("committed outcome")
    else {
        panic!("local fetch receipt should cross W");
    };
    assert!(
        repair_jobs
            .iter()
            .any(|job| job.missing_replica_node_id == replica)
    );
    assert!(
        drain_dfs_messages(&mut replica_mailbox).is_empty(),
        "background fetch completion must not synchronously send N-W repair"
    );

    std::fs::remove_dir_all(root).ok();
}

fn add_unadmitted_repair_tail<T: DfsMessageTransport>(dfs: &mut NetworkedDfsService<T>) {
    // A trailing committed job deliberately lacks object metadata. A whole-
    // queue preparation sweep would prevent earlier, budgeted work from running.
    let intent_id = dfs.writes.pending_writes.keys().next().unwrap();
    let mut unadmitted = dfs.coordinator.intent(intent_id).unwrap().clone();
    unadmitted.intent_id = "zz-unadmitted-repair-tail".into();
    unadmitted.object_id = DfsObjectId::from_legacy_named("unadmitted-repair-object").unwrap();
    unadmitted.state = DfsWriteIntentState::Committed;
    dfs.coordinator.restore_intent(unadmitted).unwrap();
}

#[test]
fn retry_pending_repairs_bounded_counts_failed_stage_attempts() {
    let root = temp_root("retry-repairs-bounds-failed-stages");
    let (mut dfs, send_count) =
        counting_failing_pending_stage_service(&root, "bounded_sync_stage", 4);
    add_unadmitted_repair_tail(&mut dfs);

    let scheduled = dfs
        .retry_pending_repairs_bounded(1)
        .expect("bounded failed stage retry");

    assert!(scheduled.is_empty());
    assert_eq!(
        send_count.load(Ordering::Relaxed),
        1,
        "a failed stage send must consume the shared maintenance budget"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn retry_pending_repairs_async_bounded_counts_failed_stage_attempts() {
    let root = temp_root("retry-repairs-async-bounds-failed-stages");
    let (mut dfs, send_count) =
        counting_failing_pending_stage_service(&root, "bounded_async_stage", 4);
    add_unadmitted_repair_tail(&mut dfs);

    let scheduled = dfs
        .retry_pending_repairs_async_bounded_with_attempts(1, &[])
        .await
        .expect("bounded async failed stage retry");

    assert!(scheduled.is_empty());
    assert_eq!(
        send_count.load(Ordering::Relaxed),
        1,
        "a failed async stage send must consume the shared maintenance budget"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_bounded_limits_scheduled_transfers() {
    let root = temp_root("retry-repairs-bounded");
    let owner = NodeId::from_legacy_named("node_bounded_repair_owner").expect("owner node");
    let replica_a = NodeId::from_legacy_named("node_bounded_repair_replica_a").expect("replica");
    let replica_b = NodeId::from_legacy_named("node_bounded_repair_replica_b").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_bounded_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_bounded_repair_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_bounded_repair_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    let mut replica_a_transport = network.add_node(replica_a.clone());
    let mut replica_b_transport = network.add_node(replica_b.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let write = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(b"bounded repair body".to_vec()),
            vec![owner, replica_a, replica_b],
            DfsReplicationPolicy::new(1, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("write should commit");
    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 2);
    let _ = drain_dfs_messages(&mut replica_a_transport);
    let _ = drain_dfs_messages(&mut replica_b_transport);

    add_unadmitted_repair_tail(&mut owner_dfs);

    let scheduled = owner_dfs
        .retry_pending_repairs_bounded(1)
        .expect("bounded retry");
    assert_eq!(scheduled.len(), 1);

    let replica_messages = drain_dfs_messages(&mut replica_a_transport)
        .into_iter()
        .chain(drain_dfs_messages(&mut replica_b_transport))
        .filter(|message| matches!(message, DfsNetworkMessage::RepairReplica(_)))
        .count();
    assert_eq!(replica_messages, 1);

    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn repair_execution_exclusions_park_offline_target_and_run_healthy_target() {
    let root = temp_root("repair-execution-exclusions");
    let owner = NodeId::from_legacy_named("node_repair_exclusion_owner").expect("owner");
    let offline = NodeId::from_legacy_named("node_repair_exclusion_offline").expect("offline");
    let healthy = NodeId::from_legacy_named("node_repair_exclusion_healthy").expect("healthy");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_exclusion").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_repair_exclusion_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_repair_exclusion_2").expect("version");
    let targets = Arc::new(Mutex::new(Vec::new()));
    let mut owner_dfs = NetworkedDfsService::new(
        RecordingFailingSendTransport {
            local_node_id: owner.clone(),
            targets: Arc::clone(&targets),
        },
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    );
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let write = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(b"repair exclusion body".to_vec()),
            vec![owner, offline.clone(), healthy.clone()],
            DfsReplicationPolicy::new(1, 3).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("write should commit");
    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 2);
    targets.lock().expect("recorded targets").clear();

    let scheduled = owner_dfs
        .retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
            1,
            &[],
            &DfsMaintenanceExecutionExclusions::new([offline.clone()]),
        )
        .await
        .expect("repair retry with execution exclusion");

    assert!(
        scheduled.is_empty(),
        "the recording transport rejects sends"
    );
    assert_eq!(
        *targets.lock().expect("recorded targets"),
        vec![healthy],
        "the excluded target must not execute or consume the one-target budget"
    );
    assert!(
        owner_dfs
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.missing_replica_node_id == offline)
    );
    assert!(
        owner_dfs
            .active_repair_attempt_records()
            .iter()
            .all(|record| record.missing_replica_node_id != offline),
        "execution exclusion must not mutate the parked target's attempt state"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn parked_repair_attempt_merge_preserves_active_inputs_byte_for_byte() {
    let offline = NodeId::from_legacy_named("node_repair_attempt_parked").expect("offline");
    let healthy = NodeId::from_legacy_named("node_repair_attempt_healthy").expect("healthy");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_attempt_parked").expect("object");
    let version = DfsVersion::from_legacy_named("version_repair_attempt_parked").expect("version");
    let content_hash = format!("blake3:{}", "2".repeat(64));
    let record =
        |intent_id: &str, target: NodeId, next_retry_unix_nanos: u64| DfsRepairAttemptRecord {
            intent_id: intent_id.to_string(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            missing_replica_node_id: target,
            attempts: 7,
            in_flight: true,
            last_attempt_unix_nanos: 11,
            next_retry_unix_nanos,
            transfer_request_id: Some(format!("request-{intent_id}")),
            last_error: Some("transient route failure".to_string()),
        };
    let parked_future = record("intent-parked-future", offline.clone(), 99);
    let parked_due = record("intent-parked-due", offline.clone(), 1);
    let parked_completed = record("intent-parked-completed", offline.clone(), 55);
    let mut regenerated_excluded = parked_future.clone();
    regenerated_excluded.attempts = 8;
    regenerated_excluded.in_flight = false;
    regenerated_excluded.last_attempt_unix_nanos = 88;
    regenerated_excluded.next_retry_unix_nanos = 100;
    regenerated_excluded.transfer_request_id = None;
    let healthy_active = record("intent-healthy", healthy.clone(), 42);
    let active_repair_keys = BTreeSet::from([
        (parked_future.intent_id.clone(), offline.clone()),
        (parked_due.intent_id.clone(), offline.clone()),
        (healthy_active.intent_id.clone(), healthy),
    ]);

    let merged = crate::maintenance::merge_parked_dfs_repair_attempt_records(
        vec![regenerated_excluded, healthy_active.clone()],
        vec![parked_future.clone(), parked_due.clone(), parked_completed],
        &active_repair_keys,
        &DfsMaintenanceExecutionExclusions::new([offline]),
    );

    assert_eq!(merged.len(), 3);
    assert!(merged.contains(&parked_future));
    assert!(merged.contains(&parked_due));
    assert!(merged.contains(&healthy_active));
}

#[test]
fn transient_route_repair_attempts_retry_promptly_after_restore() {
    let root = temp_root("transient-route-repair-attempts-retry-after-restore");
    let owner = NodeId::from_legacy_named("node_route_restore_owner").expect("owner node");
    let replica = NodeId::from_legacy_named("node_route_restore_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_route_restore").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_route_restore_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_route_restore_2").expect("version");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("owner content"),
    )
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("metadata backend");
    let mut replica_transport = network.add_node(replica.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let write = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(b"transient route repair retry body".to_vec()),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("write should commit");
    let NetworkedDfsWriteOutcome::Committed {
        intent_id,
        content_hash,
        repair_jobs,
        ..
    } = write
    else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    let repair_job = repair_jobs[0].clone();
    let now = dfs_now_unix_nanos();
    let repair_attempts = vec![DfsRepairAttemptRecord {
        intent_id: intent_id.clone(),
        object_id: repair_job.object_id.clone(),
        version: repair_job.version.clone(),
        content_hash,
        missing_replica_node_id: repair_job.missing_replica_node_id.clone(),
        attempts: 50,
        in_flight: false,
        last_attempt_unix_nanos: now,
        next_retry_unix_nanos: now.saturating_add(300_000_000_000),
        transfer_request_id: None,
        last_error: Some(
            "network error at cluster target rpc: timed out routing to node_route_restore_replica after 15000ms"
                .to_string(),
        ),
    }];
    let _ = drain_dfs_messages(&mut replica_transport);
    let pending = owner_dfs
        .writes
        .pending_writes
        .get_mut(&intent_id)
        .expect("pending write");
    pending.repair_retries.clear();
    pending.repair_inflight_request_ids.clear();
    owner_dfs.repair_attempt_records.clear();

    let scheduled = owner_dfs
        .retry_pending_repairs_bounded_with_attempts(1, &repair_attempts)
        .expect("retry after route restore");
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].missing_replica_node_id, replica);
    let replica_messages = drain_dfs_messages(&mut replica_transport)
        .into_iter()
        .filter(|message| matches!(message, DfsNetworkMessage::RepairReplica(_)))
        .count();
    assert_eq!(replica_messages, 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_suppresses_stale_content_target_after_object_advances() {
    let root = temp_root("retry-repair-suppresses-stale-content");
    let owner = NodeId::from_legacy_named("node_repair_stale_content_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_repair_stale_content_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_stale_content").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_stale_content_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_stale_content_2").expect("version");
    let version_3 =
        DfsVersion::from_legacy_named("version_repair_stale_content_3").expect("version");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(object).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"stale repair target bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write should commit locally");
    let NetworkedDfsWriteOutcome::Committed {
        intent_id,
        repair_jobs,
        ..
    } = write
    else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);

    let mut replica_mailbox = network.add_node(replica.clone());
    let replacement_bytes = b"newer visible content should retire stale repair";
    let mut advanced = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    advanced.version = version_3;
    advanced.content_hash = Some(content_hash(replacement_bytes));
    advanced.content_size_bytes = Some(replacement_bytes.len() as u64);
    advanced.current_locations = vec![owner];
    dfs.add_object_metadata(advanced).expect("advanced object");

    dfs.repair_retry_tick = 32;
    let scheduled = dfs.retry_pending_repairs().expect("retry repairs");

    assert!(
        scheduled.is_empty(),
        "stale content repair must not schedule a remote frame"
    );
    assert!(
        drain_dfs_messages(&mut replica_mailbox).is_empty(),
        "stale content repair must not send repair or fetch frames"
    );
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "committed stale repair state should be dropped once the object content advances"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_keeps_same_content_after_metadata_only_advance() {
    let root = temp_root("retry-repair-keeps-same-content");
    let owner = NodeId::from_legacy_named("node_repair_same_content_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_repair_same_content_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_same_content").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_same_content_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_same_content_2").expect("version");
    let version_3 =
        DfsVersion::from_legacy_named("version_repair_same_content_3").expect("version");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(object).expect("object");

    let bytes = b"metadata-only version keeps this content";
    let expected_hash = content_hash(bytes);
    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write should commit locally");
    let NetworkedDfsWriteOutcome::Committed {
        intent_id,
        repair_jobs,
        ..
    } = write
    else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);

    let mut replica_mailbox = network.add_node(replica.clone());
    let mut advanced = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    advanced.version = version_3;
    advanced.content_hash = Some(expected_hash.clone());
    advanced.content_size_bytes = Some(bytes.len() as u64);
    advanced.current_locations = vec![owner];
    dfs.add_object_metadata(advanced).expect("advanced object");

    dfs.repair_retry_tick = 32;
    let scheduled = dfs.retry_pending_repairs().expect("retry repairs");

    assert_eq!(scheduled.len(), 1);
    assert!(
        dfs.writes.pending_writes.contains_key(&intent_id),
        "same-content repair should remain pending until the replica receives it"
    );
    let messages = drain_dfs_messages(&mut replica_mailbox);
    assert!(matches!(
        messages.as_slice(),
        [DfsNetworkMessage::RepairReplica(command)]
            if command.intent_id == intent_id
                && command.object_id == object_id
                && command.version == version_2
                && command.content_hash == expected_hash
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn receipt_updates_visible_current_locations_for_current_version() {
    let root = temp_root("receipt-updates-current-locations");
    let owner = NodeId::from_legacy_named("node_receipt_location_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_receipt_location_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_location").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    metadata.content_hash = Some("blake3:receipt-location".to_string());
    dfs.add_object_metadata(metadata).expect("metadata");

    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        "blake3:receipt-location",
    )
    .expect("receipt");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    assert_eq!(object.current_locations, vec![owner, replica]);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stat_derives_current_locations_from_matching_receipts_without_writing() {
    let root = temp_root("stat-derives-receipt-locations");
    let owner = NodeId::from_legacy_named("node_stat_receipt_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_stat_receipt_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stat_receipt").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = "blake3:stat-receipt-location";
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/stat-receipt-location.bin".to_string();
    metadata.version = version.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    metadata.content_hash = Some(content_hash.to_string());
    metadata.content_size_bytes = Some(17);
    dfs.put_visible_object_in_metadata_backend(&metadata)
        .expect("visible object");
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash.to_string()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("receipt");

    let stat = dfs
        .posix_stat_object(&object_id)
        .expect("stat")
        .expect("object");

    assert_eq!(
        stat.current_locations.iter().collect::<BTreeSet<_>>(),
        [&owner, &replica].into_iter().collect::<BTreeSet<_>>()
    );
    drop(dfs);

    let backend =
        open_root_local_metadata_backend(root.join("metadata")).expect("reopen metadata backend");
    assert_eq!(
        backend
            .get_object(&object_id)
            .expect("object")
            .expect("object")
            .current_locations,
        vec![owner.clone()],
        "stat must not persist read-side location reconciliation"
    );
    assert_eq!(
        backend
            .get_version(&object_id, &version)
            .expect("version")
            .expect("version")
            .locations,
        vec![owner],
        "stat must not rewrite the visible version record"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_repairs_visible_target_pending_intent() {
    let root = temp_root("hydrate-repairs-visible-target-pending");
    let owner = NodeId::from_legacy_named("node_hydrate_visible_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_visible_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_visible").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"visible target already committed";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(&content_root).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let manifest = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content manifest");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_2.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(manifest.size_bytes);
    metadata.current_locations = vec![owner.clone(), replica.clone()];
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("visible object");
    let intent_id = format!("dfs_intent_{}_42", owner.as_str());
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            base_version: Some(version_1),
            target_version: version_2.clone(),
            coordinator_node_id: owner.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(manifest.content_hash.clone()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc(),
            updated_at: Some(OffsetDateTime::now_utc()),
        })
        .expect("stale pending intent");
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: owner.clone(),
            content_hash: Some(manifest.content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("local receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("visible target stale pending intent should hydrate");

    let recovered_intent = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent")
        .expect("intent");
    assert_eq!(recovered_intent.status, DfsWriteIntentStatus::Committed);
    assert!(
        recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "outstanding repair work should retain pending write state after hydration"
    );
    assert_eq!(
        recovered_owner.coordinator().repair_jobs().len(),
        1,
        "unverified replica locations should remain repair work after hydration"
    );
    let recovered_receipts = recovered_owner
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert!(
        recovered_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == owner),
        "hydration should restore verified backend receipts"
    );
    assert!(
        !recovered_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica),
        "hydration must not synthesize receipts for unverified remote replicas"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydration_binds_only_matching_local_content_before_restoring_receipt() {
    for scenario in ["missing-manifest", "conflicting-manifest", "wrong-size"] {
        let root = temp_root(&format!("hydrate-local-blob-{scenario}"));
        let fixture = seed_durable_commit_promotion_fixture(&root, scenario);
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
                .expect("backend");
        backend
            .remove_receipt(
                &fixture.object_id,
                &fixture.target_version,
                &fixture.replica,
            )
            .expect("remove receipt without removing advertised location");
        if scenario == "wrong-size" {
            let mut version = backend
                .get_version(&fixture.object_id, &fixture.target_version)
                .expect("version")
                .expect("version");
            version.size_bytes = version.size_bytes.map(|size| size + 1);
            backend
                .put_version(&version)
                .expect("mismatched size metadata");
        }
        let source = LocalContentStore::new(&fixture.content_root).expect("source");
        let (_, bytes) = source
            .read_object_version(fixture.object_id.as_str(), fixture.target_version.as_str())
            .expect("source bytes");
        let content = LocalContentStore::new(root.join("target-content")).expect("target");
        content
            .put_object_version_blob_only(fixture.object_id.as_str(), "older-version", &bytes)
            .expect("unbound current content");
        if scenario == "conflicting-manifest" {
            let conflicting = content
                .put_object_version_blob_only(
                    fixture.object_id.as_str(),
                    fixture.target_version.as_str(),
                    b"different content",
                )
                .expect("conflicting blob");
            content
                .put_blob_only_manifest(
                    fixture.object_id.as_str(),
                    fixture.target_version.as_str(),
                    &conflicting.content_hash,
                    conflicting.size_bytes,
                )
                .expect("persist conflicting manifest");
            assert_ne!(
                content
                    .read_manifest(fixture.object_id.as_str(), fixture.target_version.as_str())
                    .expect("conflicting manifest exists")
                    .content_hash,
                fixture.content_hash
            );
        }
        assert!(
            !backend
                .receipts_for_version(&fixture.object_id, &fixture.target_version)
                .expect("receipts before hydration")
                .iter()
                .any(|receipt| receipt.replica_node_id == fixture.replica)
        );
        let target = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(fixture.replica.clone()),
            content,
        )
        .with_metadata_backend(backend)
        .expect("hydrate");
        let restored = target
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica);
        assert_eq!(
            restored,
            scenario == "missing-manifest",
            "{scenario}: {:?}",
            target
                .metadata_backend()
                .expect("backend")
                .receipts_for_version(&fixture.object_id, &fixture.target_version)
        );
        if restored {
            let manifest = target
                .content()
                .read_manifest(fixture.object_id.as_str(), fixture.target_version.as_str())
                .expect("receipt has durable manifest");
            assert_eq!(manifest.content_hash, fixture.content_hash);
        }
        std::fs::remove_dir_all(root).ok();
    }
}

#[test]
fn local_blob_repair_persists_version_manifest_before_receipt_and_restart() {
    let root = temp_root("local-blob-repair-manifest");
    let fixture = seed_durable_commit_promotion_fixture(&root, "local-blob-manifest");
    let network = InProcessNetwork::new();
    let target_content_root = root.join("target-content");
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("target content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target backend"),
    )
    .expect("target hydration");
    assert!(
        target
            .transition_missing_local_commit_content_to_repair(
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
            .expect("prune missing local version")
    );
    let source = LocalContentStore::new(&fixture.content_root).expect("source content");
    let (_, bytes) = source
        .read_object_version(fixture.object_id.as_str(), fixture.target_version.as_str())
        .expect("source bytes");
    target
        .content()
        .put_object_version_blob_only(
            fixture.object_id.as_str(),
            "earlier-version-with-same-bytes",
            &bytes,
        )
        .expect("existing content under another version");
    target
        .hydrate_from_metadata_backend()
        .expect("recover repair queue");
    assert_eq!(target.coordinator().repair_jobs().len(), 1);
    target
        .retry_pending_repairs_bounded(1)
        .expect("repair from local blob");
    assert!(target.coordinator().repair_jobs().is_empty());
    let manifest = target
        .content()
        .read_manifest(fixture.object_id.as_str(), fixture.target_version.as_str())
        .expect("a durability receipt must have an exact version manifest");
    assert_eq!(manifest.content_hash, fixture.content_hash);
    drop(target);

    let mut reopened = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("reopened content"),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened metadata"),
    )
    .expect("reopened service");
    let object = reopened
        .metadata()
        .object(&fixture.object_id)
        .cloned()
        .expect("object");
    let event = reopened
        .handle_commit_version(
            fixture.owner.clone(),
            DfsCommitVersionCommand {
                request_id: "replayed-after-local-blob-repair".to_string(),
                intent_id: fixture.intent_id.clone(),
                file_system_id: object.file_system_id.clone(),
                object_id: fixture.object_id.clone(),
                expected_base_version: None,
                version: fixture.target_version.clone(),
                content_hash: fixture.content_hash.clone(),
                object_metadata: Some(object),
                authority_shard: None,
            },
        )
        .expect("replayed commit notification");
    assert!(
        matches!(event, NetworkedDfsServiceEvent::VersionCommitted { .. }),
        "{event:?}"
    );
    assert!(reopened.coordinator().repair_jobs().is_empty());
    assert!(
        reopened
            .metadata_backend()
            .expect("backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_transitions_stale_receipt_into_fetch_repair() {
    let root = temp_root("missing-commit-content-fetch-repair");
    let fixture = seed_durable_commit_promotion_fixture(&root, "missing-content-fetch");
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(fixture.owner.clone());
    let target_content_root = root.join("target-content");
    let target_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target metadata backend");
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("empty target content"),
    )
    .with_metadata_backend(target_backend)
    .expect("target metadata hydration");
    assert!(target.coordinator().repair_jobs().is_empty());
    assert!(
        target
            .content()
            .read_manifest(fixture.object_id.as_str(), fixture.target_version.as_str())
            .is_err(),
        "the stale target receipt must begin without local content"
    );
    let object_metadata = target
        .metadata()
        .object(&fixture.object_id)
        .cloned()
        .expect("committed object metadata");

    target.begin_message_committed_work();
    let event = target
        .handle_commit_version(
            fixture.owner.clone(),
            DfsCommitVersionCommand {
                request_id: "commit_missing_content_fetch_repair".to_string(),
                intent_id: fixture.intent_id.clone(),
                file_system_id: object_metadata.file_system_id.clone(),
                object_id: fixture.object_id.clone(),
                expected_base_version: None,
                version: fixture.target_version.clone(),
                content_hash: fixture.content_hash.clone(),
                object_metadata: Some(object_metadata),
                authority_shard: None,
            },
        )
        .expect("missing commit content response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected { ref reason, .. }
            if dfs_rejection_is_commit_content_repair_pending(
                reason,
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
    ));
    let response = drain_dfs_messages(&mut owner_transport)
        .into_iter()
        .find_map(|message| match message {
            DfsNetworkMessage::CommitVersionResponse(response) => Some(response),
            _ => None,
        })
        .expect("commit response");
    assert!(matches!(
        response.result,
        DfsBlobTransferResult::Rejected { ref reason }
            if dfs_rejection_is_commit_content_repair_pending(
                reason,
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
    ));

    let work = target.take_message_committed_work();
    assert!(
        work.publish_metadata,
        "receipt withdrawal must publish metadata"
    );
    assert!(work.repair_maintenance, "missing content must wake repair");
    let queued = target.take_message_committed_repair_queue_records();
    assert!(
        queued.iter().any(|record| {
            record.intent_id == fixture.intent_id
                && record.missing_replica_node_id == fixture.replica
                && record.version == fixture.target_version
                && record.content_hash == fixture.content_hash
        }),
        "missing content must enter the durable queue before the request lane exits"
    );

    let backend = target.metadata_backend().expect("target backend");
    assert_eq!(
        backend
            .get_intent(&fixture.object_id, &fixture.intent_id)
            .expect("committed intent")
            .expect("committed intent")
            .status,
        DfsWriteIntentStatus::Committed,
        "repair transition must preserve the committed intent"
    );
    assert!(
        !backend
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("receipts after transition")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "the stale target receipt must be durably removed"
    );
    assert!(
        !backend
            .get_object(&fixture.object_id)
            .expect("object after transition")
            .expect("object after transition")
            .current_locations
            .contains(&fixture.replica),
        "the stale target location must be durably removed"
    );
    drop(target);

    let target_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened target metadata backend");
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("reopened target content"),
    )
    .with_metadata_backend(target_backend)
    .expect("reopened target hydration");
    assert_eq!(target.coordinator().repair_jobs().len(), 1);
    assert_eq!(
        target.coordinator().repair_jobs()[0].missing_replica_node_id,
        fixture.replica
    );
    let mut owner = NetworkedDfsService::new(
        owner_transport,
        LocalContentStore::new(&fixture.content_root).expect("owner source content"),
    );

    let scheduled = target
        .retry_pending_repairs_bounded(1)
        .expect("request exact content from durable source");
    assert!(
        scheduled.is_empty(),
        "self-repair waits for the exact source fetch before committing"
    );
    owner.process_incoming().expect("serve exact source fetch");
    target
        .process_incoming()
        .expect("accept fetched exact content");

    assert!(target.coordinator().repair_jobs().is_empty());
    assert!(
        target
            .metadata_backend()
            .expect("repaired backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("repaired receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "fetch completion must restore the target durability receipt"
    );
    let manifest = target
        .content()
        .read_manifest(fixture.object_id.as_str(), fixture.target_version.as_str())
        .expect("repaired target manifest");
    assert_eq!(manifest.content_hash, fixture.content_hash);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_prunes_receipt_when_locations_are_already_absent() {
    let root = temp_root("missing-commit-content-stale-receipt-only");
    let fixture = seed_durable_commit_promotion_fixture(&root, "stale-receipt-only");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("metadata backend");
    let mut object = backend
        .get_object(&fixture.object_id)
        .expect("object")
        .expect("object");
    object
        .current_locations
        .retain(|node_id| node_id != &fixture.replica);
    let mut version = backend
        .get_version(&fixture.object_id, &fixture.target_version)
        .expect("version")
        .expect("version");
    version
        .locations
        .retain(|node_id| node_id != &fixture.replica);
    backend
        .put_object_and_version(&object, &version)
        .expect("pre-pruned locations");
    assert!(
        backend
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("stale receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "the fixture must retain only the stale durable receipt"
    );

    let network = InProcessNetwork::new();
    let _owner_transport = network.add_node(fixture.owner.clone());
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(root.join("target-content")).expect("empty target content"),
    )
    .with_metadata_backend(backend)
    .expect("target metadata hydration");
    let object_metadata = target
        .metadata()
        .object(&fixture.object_id)
        .cloned()
        .expect("committed object metadata");

    let event = target
        .handle_commit_version(
            fixture.owner,
            DfsCommitVersionCommand {
                request_id: "commit_missing_content_stale_receipt_only".to_string(),
                intent_id: fixture.intent_id.clone(),
                file_system_id: object_metadata.file_system_id.clone(),
                object_id: fixture.object_id.clone(),
                expected_base_version: None,
                version: fixture.target_version.clone(),
                content_hash: fixture.content_hash.clone(),
                object_metadata: Some(object_metadata),
                authority_shard: None,
            },
        )
        .expect("repair-pending response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected { ref reason, .. }
            if dfs_rejection_is_commit_content_repair_pending(
                reason,
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
    ));
    assert!(
        !target
            .metadata_backend()
            .expect("target backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("receipts after transition")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "terminal repair-pending requires the stale receipt to be durably absent"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_scans_receipts_once_globally() {
    let root = temp_root("hydrate-global-receipt-scan");
    let owner = NodeId::from_legacy_named("node_hydrate_global_receipts").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_global_replica").expect("node");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");

    let mut intent_ids = Vec::new();
    for index in 0..3 {
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_hydrate_global_receipt_{index}"))
                .expect("object");
        let base_version =
            DfsVersion::from_legacy_named(format!("version_hydrate_global_base_{index}"))
                .expect("base version");
        let target_version =
            DfsVersion::from_legacy_named(format!("version_hydrate_global_target_{index}"))
                .expect("target version");
        let mut metadata = file_metadata(owner.clone());
        metadata.object_id = object_id.clone();
        metadata.path = format!("/users/hydrate/global-receipt-{index}.txt");
        metadata.version = base_version;
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        owner_dfs
            .add_object_metadata(metadata)
            .expect("owner object");

        let outcome = owner_dfs
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id,
                    target_version,
                    requesting_node_id: owner.clone(),
                },
                format!("globally scanned receipt {index}").into_bytes(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).expect("policy"),
            )
            .expect("committed write");
        match outcome {
            NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_ids.push(intent_id),
            other => panic!("expected committed write, got {other:?}"),
        }
    }
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    recovered_backend.reset_test_receipt_scan_counts();
    let scan_probe = recovered_backend.clone();
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert_eq!(
        scan_probe.test_receipt_scan_counts(),
        (1, 0),
        "hydration must use one global receipt page rather than one object/version scan per intent"
    );
    for intent_id in intent_ids {
        assert_eq!(
            recovered_owner
                .coordinator()
                .receipts_for_intent(&intent_id)
                .len(),
            1,
            "global receipt indexing must preserve coordinator receipt recovery"
        );
        assert!(
            recovered_owner
                .writes
                .pending_writes
                .contains_key(&intent_id),
            "global receipt indexing must retain pending sender state for missing repairs"
        );
    }
    assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 3);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn queued_repair_restore_rebuilds_once_and_preserves_inactive_history() {
    let root = sinomo_testdir::TestDir::new("dfs-queued-repair-bulk-restore");
    let owner = NodeId::from_legacy_named("bulk-restore-owner").unwrap();
    let replica = NodeId::from_legacy_named("bulk-restore-replica").unwrap();
    let network = InProcessNetwork::new();
    let mut service = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    service.add_file_system(file_system(owner.clone())).unwrap();
    for index in 0..32 {
        let mut object = file_metadata(owner.clone());
        object.object_id = DfsObjectId::from_legacy_named(format!("bulk-object-{index}")).unwrap();
        object.path = format!("/bulk/{index}.bin");
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        service.add_object_metadata(object.clone()).unwrap();
        let result = service
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object.object_id,
                    target_version: DfsVersion::from_legacy_named("bulk-new-version").unwrap(),
                    requesting_node_id: owner.clone(),
                },
                b"bulk repair".to_vec(),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).unwrap(),
            )
            .unwrap();
        assert!(matches!(result, NetworkedDfsWriteOutcome::Committed { .. }));
    }
    let records = service.durable_repair_queue_records();
    assert_eq!(records.len(), 32);
    let inactive = service
        .coordinator
        .intent(&records[0].intent_id)
        .unwrap()
        .clone();
    service.coordinator = DistributedEventualCoordinator::new();
    service
        .coordinator
        .restore_intent(inactive.clone())
        .unwrap();
    service
        .coordinator
        .set_committed_repair_active(&inactive.intent_id, false);
    service.writes.pending_writes.clear();
    let rebuilds = service.coordinator.repair_job_rebuild_count;
    service
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    assert_eq!(
        service.coordinator.repair_job_rebuild_count,
        rebuilds + 1,
        "restore the entire queue and its receipts with one repair-list rebuild"
    );
    assert_eq!(service.coordinator.repair_jobs().len(), 31);
    assert!(
        service
            .coordinator
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != inactive.intent_id)
    );
    let jobs = service.coordinator.repair_jobs().to_vec();
    service
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    assert_eq!(service.coordinator.repair_job_rebuild_count, rebuilds + 1);
    assert_eq!(service.coordinator.repair_jobs(), jobs);
}

#[test]
fn hydrate_from_metadata_backend_restores_committed_pending_write_for_missing_repair() {
    let root = temp_root("hydrate-restores-committed-repair");
    let owner = NodeId::from_legacy_named("node_hydrate_repair_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_repair_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"repair after restart bytes".to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert!(
        recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "committed intents with missing replicas must keep pending state for repair"
    );
    assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydration_prunes_over_bound_superseded_repair_history_and_keeps_actionable_work() {
    fn put_visible_object(
        backend: &mut impl DfsMetadataBackend,
        owner: &NodeId,
        object_id: DfsObjectId,
        label: &str,
        kind: DfsObjectKind,
        version: DfsVersion,
        content_hash: Option<String>,
        size_bytes: Option<u64>,
    ) -> DfsObjectMetadata {
        let mut object = file_metadata(owner.clone());
        object.object_id = object_id;
        object.path = format!("/services/hydration-repair-{label}.bin");
        object.kind = kind;
        object.version = version;
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![owner.clone()];
        object.content_hash = content_hash;
        object.content_size_bytes = size_bytes;
        backend
            .put_object_and_version(&object, &object_version_record(&object, size_bytes))
            .expect("visible object and version");
        object
    }

    fn put_committed_missing_replica_intent(
        backend: &mut impl DfsMetadataBackend,
        owner: &NodeId,
        replica: &NodeId,
        object: &DfsObjectMetadata,
        intent_id: &str,
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        content_hash: Option<String>,
    ) {
        let now = OffsetDateTime::UNIX_EPOCH;
        backend
            .put_intent(&DfsWriteIntentRecord {
                publication: Default::default(),
                intent_id: intent_id.to_string(),
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                base_version,
                target_version: target_version.clone(),
                coordinator_node_id: owner.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 0,
                desired_replica_node_ids: vec![owner.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(1, 2).expect("replication policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: content_hash.clone(),
                object_metadata: None,
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: now,
                updated_at: Some(now),
            })
            .expect("committed intent");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    committed_version: target_version,
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: owner.clone(),
                content_hash,
                received_at: now,
            })
            .expect("owner receipt");
    }

    let root = temp_root("hydrate-prunes-superseded-repair-history");
    let owner = NodeId::from_legacy_named("node_hydrate_history_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_hydrate_history_replica").expect("replica");
    let metadata_root = root.join("metadata");
    let content_root = root.join("content");
    let content = LocalContentStore::new(&content_root).expect("content store");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("metadata backend");
    backend
        .put_file_system(&file_system(owner.clone()))
        .expect("filesystem");

    let stale_intent_count = MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS + 1;
    let mut stale_intent_ids = BTreeSet::new();
    for index in 0..stale_intent_count {
        let object_id = DfsObjectId::from_legacy_named(format!("obj_hydration_stale_{index}"))
            .expect("stale object");
        let visible_version =
            DfsVersion::from_legacy_named(format!("version_hydration_visible_{index}"))
                .expect("visible version");
        let target_version =
            DfsVersion::from_legacy_named(format!("version_hydration_stale_target_{index}"))
                .expect("stale target version");
        let base_version =
            DfsVersion::from_legacy_named(format!("version_hydration_stale_base_{index}"))
                .expect("stale base version");
        let visible_bytes = format!("current visible content {index}");
        let stale_bytes = format!("superseded repair content {index}");
        let visible_is_tombstone = index % 2 == 0;
        let object = put_visible_object(
            &mut backend,
            &owner,
            object_id,
            &format!("stale-{index}"),
            if visible_is_tombstone {
                DfsObjectKind::Tombstone
            } else {
                DfsObjectKind::File
            },
            visible_version,
            (!visible_is_tombstone).then(|| content_hash(visible_bytes.as_bytes())),
            (!visible_is_tombstone).then_some(visible_bytes.len() as u64),
        );
        let intent_id = format!("dfs_intent_hydration_stale_{index:03}");
        put_committed_missing_replica_intent(
            &mut backend,
            &owner,
            &replica,
            &object,
            &intent_id,
            Some(base_version),
            target_version,
            Some(content_hash(stale_bytes.as_bytes())),
        );
        stale_intent_ids.insert(intent_id);
    }

    let exact_object_id =
        DfsObjectId::from_legacy_named("obj_hydration_actionable_exact").expect("exact object");
    let exact_version =
        DfsVersion::from_legacy_named("version_hydration_actionable_exact").expect("exact version");
    let exact_manifest = content
        .put_object_version(
            exact_object_id.as_str(),
            exact_version.as_str(),
            b"exact current repair content",
        )
        .expect("exact content");
    let exact_object = put_visible_object(
        &mut backend,
        &owner,
        exact_object_id,
        "actionable-exact",
        DfsObjectKind::File,
        exact_version.clone(),
        Some(exact_manifest.content_hash.clone()),
        Some(exact_manifest.size_bytes),
    );
    let exact_intent_id = "dfs_intent_hydration_actionable_exact";
    put_committed_missing_replica_intent(
        &mut backend,
        &owner,
        &replica,
        &exact_object,
        exact_intent_id,
        Some(
            DfsVersion::from_legacy_named("version_hydration_actionable_exact_base")
                .expect("exact base"),
        ),
        exact_version,
        Some(exact_manifest.content_hash),
    );

    let same_content_object_id =
        DfsObjectId::from_legacy_named("obj_hydration_actionable_same_content")
            .expect("same-content object");
    let same_content_target =
        DfsVersion::from_legacy_named("version_hydration_actionable_same_content_target")
            .expect("same-content target");
    let same_content_visible =
        DfsVersion::from_legacy_named("version_hydration_actionable_same_content_visible")
            .expect("same-content visible version");
    let same_content_manifest = content
        .put_object_version(
            same_content_object_id.as_str(),
            same_content_target.as_str(),
            b"metadata-only advance keeps these bytes",
        )
        .expect("same-content bytes");
    let same_content_object = put_visible_object(
        &mut backend,
        &owner,
        same_content_object_id,
        "actionable-same-content",
        DfsObjectKind::File,
        same_content_visible,
        Some(same_content_manifest.content_hash.clone()),
        Some(same_content_manifest.size_bytes),
    );
    let same_content_intent_id = "dfs_intent_hydration_actionable_same_content";
    put_committed_missing_replica_intent(
        &mut backend,
        &owner,
        &replica,
        &same_content_object,
        same_content_intent_id,
        Some(
            DfsVersion::from_legacy_named("version_hydration_actionable_same_content_base")
                .expect("same-content base"),
        ),
        same_content_target,
        Some(same_content_manifest.content_hash),
    );

    let base_visible_object_id =
        DfsObjectId::from_legacy_named("obj_hydration_actionable_base_visible")
            .expect("base-visible object");
    let base_visible_version =
        DfsVersion::from_legacy_named("version_hydration_actionable_base_visible")
            .expect("base-visible version");
    let base_visible_target =
        DfsVersion::from_legacy_named("version_hydration_actionable_base_target")
            .expect("base-visible target");
    let base_visible_bytes = b"currently visible base bytes";
    let base_target_manifest = content
        .put_object_version(
            base_visible_object_id.as_str(),
            base_visible_target.as_str(),
            b"committed target waiting for publication",
        )
        .expect("base recovery target bytes");
    let base_visible_object = put_visible_object(
        &mut backend,
        &owner,
        base_visible_object_id,
        "actionable-base-visible",
        DfsObjectKind::File,
        base_visible_version.clone(),
        Some(content_hash(base_visible_bytes)),
        Some(base_visible_bytes.len() as u64),
    );
    let base_visible_intent_id = "dfs_intent_hydration_actionable_base_visible";
    put_committed_missing_replica_intent(
        &mut backend,
        &owner,
        &replica,
        &base_visible_object,
        base_visible_intent_id,
        Some(base_visible_version),
        base_visible_target,
        Some(base_target_manifest.content_hash),
    );

    let ambiguous_object_id = DfsObjectId::from_legacy_named("obj_hydration_actionable_ambiguous")
        .expect("ambiguous object");
    let ambiguous_target =
        DfsVersion::from_legacy_named("version_hydration_actionable_ambiguous_target")
            .expect("ambiguous target");
    let ambiguous_manifest = content
        .put_object_version(
            ambiguous_object_id.as_str(),
            ambiguous_target.as_str(),
            b"repair identity retained when visible hash is absent",
        )
        .expect("ambiguous target bytes");
    let ambiguous_object = put_visible_object(
        &mut backend,
        &owner,
        ambiguous_object_id,
        "actionable-ambiguous",
        DfsObjectKind::File,
        DfsVersion::from_legacy_named("version_hydration_actionable_ambiguous_visible")
            .expect("ambiguous visible version"),
        None,
        None,
    );
    let ambiguous_intent_id = "dfs_intent_hydration_actionable_ambiguous";
    put_committed_missing_replica_intent(
        &mut backend,
        &owner,
        &replica,
        &ambiguous_object,
        ambiguous_intent_id,
        Some(
            DfsVersion::from_legacy_named("version_hydration_actionable_ambiguous_base")
                .expect("ambiguous base"),
        ),
        ambiguous_target,
        Some(ambiguous_manifest.content_hash),
    );

    drop(backend);
    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let mut recovered = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(owner.clone()),
        LocalContentStore::new(&content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("hydrate repair state");

    assert_eq!(
        recovered.coordinator().intent_count(),
        stale_intent_count + 4,
        "hydration must retain terminal intent history"
    );
    assert!(stale_intent_ids.iter().all(|intent_id| {
        recovered
            .coordinator()
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
    }));

    let expected_actionable_intent_ids = BTreeSet::from([
        exact_intent_id.to_string(),
        same_content_intent_id.to_string(),
        base_visible_intent_id.to_string(),
        ambiguous_intent_id.to_string(),
    ]);
    let repair_intent_ids = recovered
        .coordinator()
        .repair_jobs()
        .iter()
        .map(|job| job.intent_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(repair_intent_ids, expected_actionable_intent_ids);
    assert_eq!(recovered.coordinator().repair_jobs().len(), 4);
    assert!(
        recovered
            .coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.missing_replica_node_id == replica)
    );
    assert_eq!(
        recovered
            .writes
            .pending_writes
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_actionable_intent_ids,
        "only actionable committed repairs may retain pending write state"
    );
    let durable_records = recovered.durable_repair_queue_records();
    assert_eq!(durable_records.len(), 4);
    assert_eq!(
        durable_records
            .iter()
            .map(|record| record.intent_id.clone())
            .collect::<BTreeSet<_>>(),
        expected_actionable_intent_ids,
        "mounted handoff inventory must retain every valid repair identity"
    );
    assert!(durable_records.iter().all(|record| {
        record.missing_replica_node_id == replica && record.attempts == 0 && !record.in_flight
    }));
    assert!(
        durable_records.len() <= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS,
        "superseded history must not overflow the exact mounted handoff bound"
    );
    assert!(stale_intent_ids.is_disjoint(&expected_actionable_intent_ids));

    let replayed_actionable_intent = recovered
        .coordinator
        .intent(exact_intent_id)
        .cloned()
        .expect("exact-current intent retained for replay");
    recovered
        .coordinator
        .restore_intent(replayed_actionable_intent)
        .expect("later generic intent restore");
    assert_eq!(
        recovered
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect::<BTreeSet<_>>(),
        expected_actionable_intent_ids,
        "later generic rebuild must not resurrect suppressed hydration history"
    );

    let live_intent = recovered
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            DfsObjectId::from_legacy_named("obj_hydration_new_live_repair")
                .expect("new live repair object"),
            None,
            DfsVersion::from_legacy_named("version_hydration_new_live_repair")
                .expect("new live repair version"),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("live repair policy"),
        )
        .expect("new live intent");
    let live_intent_id = live_intent.intent_id.clone();
    recovered
        .coordinator
        .record_durable_receipt(&live_intent_id, owner.clone())
        .expect("new live owner receipt");
    recovered
        .coordinator
        .commit_intent(&live_intent_id)
        .expect("new live commit");

    recovered
        .coordinator
        .restore_durable_receipt(exact_intent_id, owner)
        .expect("later generic receipt restore");
    let mut expected_after_live_commit = expected_actionable_intent_ids.clone();
    expected_after_live_commit.insert(live_intent_id.clone());
    let repair_intent_ids_after_live_commit = recovered
        .coordinator
        .repair_jobs()
        .iter()
        .map(|job| job.intent_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        repair_intent_ids_after_live_commit,
        expected_after_live_commit
    );
    assert!(
        stale_intent_ids.is_disjoint(&repair_intent_ids_after_live_commit),
        "process-generation suppression must survive every generic rebuild"
    );
    assert!(
        recovered.coordinator.repair_jobs().iter().any(|job| {
            job.intent_id == live_intent_id && job.missing_replica_node_id == replica
        }),
        "hydration suppression must not hide repair work from a new live commit"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_skips_non_coordinator_repair_sender_state() {
    let root = temp_root("hydrate-skips-non-coordinator-repair");
    let owner = NodeId::from_legacy_named("node_hydrate_noncoord_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_noncoord_replica").expect("node");
    let observer = NodeId::from_legacy_named("node_hydrate_noncoord_observer").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_noncoord").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"repair should stay with coordinator".to_vec(),
            vec![owner, replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    drop(owner_dfs);

    let observer_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("observer backend");
    let mut observer_dfs = NetworkedDfsService::new(
        network.add_node(observer),
        LocalContentStore::new(root.join("observer-content")).expect("observer content"),
    )
    .with_metadata_backend(observer_backend)
    .expect("observer metadata");

    assert_eq!(
        observer_dfs.coordinator().repair_jobs().len(),
        1,
        "non-coordinator still observes committed repair metadata"
    );
    assert!(
        !observer_dfs.writes.pending_writes.contains_key(&intent_id),
        "non-coordinator must not hydrate local repair sender state"
    );
    assert!(
        observer_dfs
            .retry_pending_repairs()
            .expect("non-coordinator repair retry")
            .is_empty(),
        "non-coordinator must not schedule repair frames for another coordinator's intent"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn operational_metrics_report_repair_backlog_and_content_footprint() {
    let root = temp_root("operational-metrics-repair-backlog");
    let owner = NodeId::from_legacy_named("node_metrics_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_metrics_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_metrics_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"metrics should expose repair backlog and content footprint";
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), root.join("metadata"))
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write with missing repair");
    assert!(matches!(
        write,
        NetworkedDfsWriteOutcome::Committed {
            repair_jobs,
            ..
        } if repair_jobs.len() == 1
    ));

    let metrics = owner_dfs.operational_metrics().expect("metrics");
    assert_eq!(metrics.node_id, owner);
    assert!(metrics.metadata_backend_configured);
    assert_eq!(metrics.file_system_count, 1);
    assert_eq!(metrics.object_count, 1);
    assert_eq!(metrics.coordinator_intent_count, 1);
    assert_eq!(metrics.coordinator_receipt_count, 1);
    assert_eq!(metrics.coordinator_committed_version_count, 1);
    assert_eq!(metrics.repair_job_count, 1);
    assert_eq!(metrics.pending_write_count, 1);
    assert_eq!(metrics.pending_read_count, 0);
    assert_eq!(metrics.stage_retry_count, 0);
    assert_eq!(metrics.repair_retry_count, 1);
    assert_eq!(metrics.stage_inflight_count, 0);
    assert_eq!(metrics.repair_inflight_count, 0);
    assert_eq!(metrics.fetch_inflight_count, 0);
    assert_eq!(metrics.fetch_failed_source_count, 0);
    assert_eq!(metrics.content.blob_count, 1);
    assert_eq!(metrics.content.manifest_count, 1);
    assert_eq!(metrics.content.logical_object_bytes, bytes.len() as u64);
    assert_eq!(metrics.content.staging_file_count, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_ignores_receipts_with_wrong_content_hash() {
    let root = temp_root("hydrate-ignores-wrong-hash-receipt");
    let owner = NodeId::from_legacy_named("node_hydrate_hash_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_hash_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_hash_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"receipt hash must match intent bytes";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("committed write with missing repair");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    let wrong_hash = content_hash(b"receipt points at different bytes");
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2,
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(wrong_hash),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("stale wrong-hash receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert!(
        recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "wrong-hash receipts must not suppress repair state after hydration"
    );
    assert_eq!(recovered_owner.coordinator().repair_jobs().len(), 1);
    assert_eq!(
        recovered_owner.coordinator().repair_jobs()[0].missing_replica_node_id,
        replica
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_ignores_unexpected_replica_receipts() {
    let root = temp_root("hydrate-ignores-unexpected-receipt");
    let owner = NodeId::from_legacy_named("node_hydrate_unexpected_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_unexpected_replica").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_unexpected_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"unexpected receipt should not break recovery";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("self-only committed write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
        other => panic!("expected committed write, got {other:?}"),
    };
    let content_hash = content_hash(bytes);
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2,
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("unexpected stale receipt");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("unexpected replica receipt should not break hydration");

    assert!(
        !recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "fully repaired committed write should not be restored as pending"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    assert!(
        recovered_owner
            .coordinator()
            .receipts_for_intent(&intent_id)
            .into_iter()
            .all(|receipt| receipt.replica_node_id != replica),
        "unexpected replica receipt must not be restored into coordinator state"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_applies_commit_ready_receipts_pending_intent() {
    let root = temp_root("hydrate-applies-commit-ready-receipts-pending");
    let owner = NodeId::from_legacy_named("node_hydrate_ready_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_ready_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_ready").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit ready after restart";
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("owner fs");
    owner_dfs
        .add_object_metadata(metadata)
        .expect("owner object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            bytes.to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let content_hash = content_hash(bytes);
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version_2.clone(),
                owner_node_id: owner.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("persist replica receipt without visible commit");
    assert_eq!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object")
            .expect("object")
            .version,
        version_1,
        "test setup should leave visible metadata uncommitted before restart"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("commit-ready receipts-pending intent should hydrate");

    let object = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("object")
        .expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(object.content_hash.as_deref(), Some(content_hash.as_str()));
    assert!(
        !recovered_owner
            .transient_metadata_object_ids
            .contains(&object_id),
        "a recovered visible commit must stop shadowing durable namespace metadata"
    );
    assert_eq!(
        object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, replica])
    );
    assert!(
        !recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id),
        "fully receipted commit-ready intent should be cleaned after visible commit recovery"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    assert_eq!(
        recovered_owner
            .metadata_backend()
            .expect("backend")
            .get_intent(&object_id, &intent_id)
            .expect("intent")
            .expect("intent")
            .status,
        DfsWriteIntentStatus::Committed
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn apply_committed_intent_tolerates_missing_local_repair_blob() {
    let root = temp_root("apply-committed-missing-repair-blob");
    let owner = NodeId::from_legacy_named("node_apply_missing_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_apply_missing_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_apply_missing_blob").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = vec![b'm'; 17 * 1024];
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata).expect("object");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), &bytes)
        .expect("manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("commit with missing repair");
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("commit applies even when eager repair bytes are missing");
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);

    let object = dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(object.current_locations, vec![owner]);
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    let pending = dfs
        .writes
        .pending_writes
        .get(&intent.intent_id)
        .expect("pending write");
    assert!(pending.repair_retries.contains_key(&replica));
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_blob_sources_use_backend_current_locations_over_stale_memory() {
    let root = temp_root("repair-sources-backend-current");
    let owner = NodeId::from_legacy_named("node_repair_source_owner").expect("node");
    let source = NodeId::from_legacy_named("node_repair_source_backend").expect("node");
    let missing = NodeId::from_legacy_named("node_repair_source_missing").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_source_backend").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair source bytes";
    let hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut backend_current = file_metadata(owner.clone());
    backend_current.object_id = object_id.clone();
    backend_current.version = version.clone();
    backend_current.policy = DfsObjectPolicy::ReplicatedEventual;
    backend_current.current_locations = vec![source.clone()];
    backend_current.content_hash = Some(hash.clone());
    backend_current.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(backend_current.clone())
        .expect("backend object");

    let mut stale_memory = backend_current;
    stale_memory.current_locations = vec![owner.clone()];
    dfs.add_transient_object_metadata(stale_memory)
        .expect("stale memory object");
    let pending = PendingNetworkedWrite {
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: hash,
        size_bytes: bytes.len() as u64,
        foreground_replica_repair: true,
        patch: None,
        stage_retries: BTreeMap::new(),
        repair_retries: BTreeMap::new(),
        stage_inflight_request_ids: BTreeMap::new(),
        repair_inflight_request_ids: BTreeMap::new(),
        fetch_inflight_from: BTreeSet::new(),
        fetch_inflight_request_ids: BTreeMap::new(),
        fetch_inflight_started_ticks: BTreeMap::new(),
        fetch_failed_from: BTreeSet::new(),
    };
    let job = DistributedRepairJob {
        intent_id: "dfs_intent_repair_source_backend".to_string(),
        file_system_id: sifs_file_system_id(),
        object_id,
        version,
        missing_replica_node_id: missing,
    };

    assert_eq!(
        dfs.repair_blob_sources(&job, &pending)
            .expect("repair sources"),
        vec![source]
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_is_idle_noop_without_distributed_work() {
    let root = temp_root("retry-pending-repairs-idle-noop");
    let owner = NodeId::from_legacy_named("node_retry_idle").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.repair_retry_tick = 41;

    let scheduled = dfs.retry_pending_repairs().expect("idle retry");

    assert!(scheduled.is_empty());
    assert_eq!(dfs.repair_retry_tick, 41);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_fetch_request_expiry_wakes_stage_and_repair_retries() {
    let root = temp_root("stale-fetch-request-expiry");
    let owner = NodeId::from_legacy_named("node_fetch_timeout_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_timeout_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_timeout_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_timeout").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_timeout".to_string();
    let request_id = "dfs_net_req_fetch_timeout".to_string();
    let content_hash = content_hash(b"fetch timeout bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.repair_retry_tick = fetch_inflight_timeout_ticks();
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id,
            version,
            content_hash,
            size_bytes: 0,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 1,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 1,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.expire_stale_fetch_requests();

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert_eq!(
        pending.fetch_inflight_request_ids.get(&source),
        Some(&request_id),
        "expired fetches keep their request id so a late completed response remains correlatable"
    );
    assert!(!pending.fetch_inflight_started_ticks.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_rejection_wakes_stage_and_repair_retries() {
    let root = temp_root("fetch-rejection-wakes-retries");
    let owner = NodeId::from_legacy_named("node_fetch_reject_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_reject_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_reject_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_reject").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_reject".to_string();
    let request_id = "dfs_net_req_fetch_reject".to_string();
    let content_hash = content_hash(b"fetch reject bytes");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: b"fetch reject bytes".len() as u64,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id,
                version,
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::NotFound,
            },
        )
        .expect("fetch miss handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetchRejected {
            source_node_id,
            ..
        } if source_node_id == source
    ));
    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_store_failure_wakes_stage_and_repair_retries() {
    let root = temp_root("fetch-store-failure-wakes-retries");
    let owner = NodeId::from_legacy_named("node_fetch_store_fail_owner").expect("node");
    let source = NodeId::from_legacy_named("node_fetch_store_fail_source").expect("node");
    let target = NodeId::from_legacy_named("node_fetch_store_fail_target").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_store_fail").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let intent_id = "dfs_intent_fetch_store_fail".to_string();
    let request_id = "dfs_net_req_fetch_store_fail".to_string();
    let expected_bytes = b"expected fetch bytes";
    let corrupt_bytes = b"corrupt fetch bytes";
    let content_hash = content_hash(expected_bytes);
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            size_bytes: expected_bytes.len() as u64,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            repair_retries: BTreeMap::from([(
                target.clone(),
                RepairRetryState {
                    attempts: 2,
                    next_retry_tick: 100,
                    inflight: true,
                },
            )]),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::from([source.clone()]),
            fetch_inflight_request_ids: BTreeMap::from([(source.clone(), request_id.clone())]),
            fetch_inflight_started_ticks: BTreeMap::from([(source.clone(), 0)]),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    let event = dfs
        .handle_fetch_blob_response(
            source.clone(),
            DfsFetchBlobResponse {
                request_id,
                intent_id: intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                source_node_id: source.clone(),
                result: DfsFetchBlobResult::Found {
                    content_hash: content_hash.clone(),
                    bytes: corrupt_bytes.to_vec(),
                },
            },
        )
        .expect("hash mismatch is isolated to the fetch source");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetchRejected {
            source_node_id,
            reason,
            ..
        } if source_node_id == source && reason.contains("content hash mismatch")
    ));
    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(pending.fetch_failed_from.contains(&source));
    assert_eq!(
        pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .stage_retries
            .get(&target)
            .expect("stage retry")
            .inflight
    );
    assert_eq!(
        pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .next_retry_tick,
        0
    );
    assert!(
        !pending
            .repair_retries
            .get(&target)
            .expect("repair retry")
            .inflight
    );
    let manifest_error = dfs
        .content
        .read_manifest(object_id.as_str(), version.as_str())
        .expect_err("rejected fetch must not leave a wrong object-version manifest");
    assert!(matches!(
        manifest_error,
        ContentStoreError::Io { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_retry_schedule_does_not_double_count_completed_inflight_attempt() {
    let root = temp_root("repair-retry-no-double-count");
    let owner = NodeId::from_legacy_named("node_retry_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_retry_replica").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let intent_id = "dfs_intent_retry".to_string();
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id: DfsObjectId::from_legacy_named("obj_retry").expect("object"),
            version: DfsVersion::from_legacy_named("version_2").expect("version"),
            content_hash: "blake3:retry".to_string(),
            size_bytes: 5,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.repair_retry_tick = 10;
    dfs.record_repair_inflight(&intent_id, &replica, "dfs_net_req_repair".to_string());
    dfs.record_repair_retry_scheduled(&intent_id, &replica);

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    let state = pending
        .repair_retries
        .get(&replica)
        .expect("repair retry state");
    assert_eq!(state.attempts, 1);
    assert_eq!(state.next_retry_tick, 11);
    assert!(!state.inflight);
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    assert!(!dfs.repair_retry_is_due(&intent_id, &replica));
    dfs.repair_retry_tick = 11;
    assert!(dfs.repair_retry_is_due(&intent_id, &replica));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_retry_schedule_preserves_network_error_diagnostics() {
    let root = temp_root("repair-retry-preserves-network-error");
    let owner = NodeId::from_legacy_named("node_retry_error_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_retry_error_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_retry_error").expect("object");
    let version = DfsVersion::from_legacy_named("version_retry_error").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let intent_id = "dfs_intent_retry_error";
    dfs.writes.pending_writes.insert(
        intent_id.to_string(),
        PendingNetworkedWrite::new(
            object_id.clone(),
            version.clone(),
            "blake3:retry-error".to_string(),
            64,
        ),
    );

    dfs.repair_retry_tick = 12;
    dfs.record_repair_retry_scheduled_with_error(
        intent_id,
        &replica,
        Some("route to target node failed: no usable route".to_string()),
    );

    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent_id, intent_id);
    assert_eq!(records[0].object_id, object_id);
    assert_eq!(records[0].version, version);
    assert_eq!(records[0].missing_replica_node_id, replica);
    assert_eq!(records[0].attempts, 1);
    assert!(!records[0].in_flight);
    assert!(
        records[0]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("no usable route"))
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_retry_schedule_caps_transient_route_backoff() {
    let root = temp_root("repair-retry-caps-transient-route-backoff");
    let owner = NodeId::from_legacy_named("node_retry_cap_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_retry_cap_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_retry_cap").expect("object");
    let version = DfsVersion::from_legacy_named("version_retry_cap").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let intent_id = "dfs_intent_retry_cap";
    dfs.writes.pending_writes.insert(
        intent_id.to_string(),
        PendingNetworkedWrite::new(object_id, version, "blake3:retry-cap".to_string(), 64),
    );

    dfs.repair_retry_tick = 12;
    for _ in 0..8 {
        dfs.record_repair_retry_scheduled_with_error(
            intent_id,
            &replica,
            Some("network I/O error during send DFS message: timed out routing".to_string()),
        );
    }

    let pending = dfs
        .writes
        .pending_writes
        .get(intent_id)
        .expect("pending write");
    let state = pending
        .repair_retries
        .get(&replica)
        .expect("repair retry state");
    assert_eq!(state.attempts, 8);
    assert_eq!(state.next_retry_tick, 18);
    assert!(!state.inflight);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_repair_base_rejection_is_retryable() {
    let reason = "stale DFS repair base for obj_stale: expected version_2, current version_1, command version_3";
    assert!(
        !repair_rejection_indicates_lost_authority(reason),
        "stale repair-base rejections are replica sequencing failures, not permanent authority loss"
    );
    assert!(
        repair_rejection_indicates_stale_replica_base(reason),
        "stale repair-base rejections should stay separately recognizable for diagnostics"
    );
}

#[test]
fn repair_attempt_restore_retries_stale_base_suppression_records() {
    let root = temp_root("repair-attempt-restore-stale-base");
    let owner = NodeId::from_legacy_named("node_repair_restore_stale_base_owner").expect("node");
    let replica =
        NodeId::from_legacy_named("node_repair_restore_stale_base_replica").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_repair_restore_stale_base").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_restore_stale_base_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_restore_stale_base_2").expect("version");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut replica_mailbox = network.add_node(replica.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version_1;
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(object).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"retry stale repair base after restore".to_vec(),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write commits locally with pending repair");
    let (intent_id, content_hash) = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            content_hash,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
            (intent_id, content_hash)
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    let _ = drain_dfs_messages(&mut replica_mailbox);

    let records = vec![DfsRepairAttemptRecord {
        intent_id: intent_id.clone(),
        object_id,
        version: version_2,
        content_hash,
        missing_replica_node_id: replica.clone(),
        attempts: 2,
        in_flight: false,
        last_attempt_unix_nanos: dfs_now_unix_nanos(),
        next_retry_unix_nanos: u64::MAX,
        transfer_request_id: None,
        last_error: Some(
            "stale DFS repair base for obj_repair_restore_stale_base: expected version_1, current version_0, command version_2"
                .to_string(),
        ),
    }];
    let pending = dfs
        .writes
        .pending_writes
        .get_mut(&intent_id)
        .expect("pending write");
    pending.repair_retries.clear();
    pending.repair_inflight_request_ids.clear();
    dfs.repair_attempt_records.clear();

    let scheduled = dfs
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .expect("retry stale-base repair");
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].missing_replica_node_id, replica);
    let repair_messages = drain_dfs_messages(&mut replica_mailbox)
        .into_iter()
        .filter(|message| matches!(message, DfsNetworkMessage::RepairReplica(_)))
        .count();
    assert_eq!(repair_messages, 1);
    assert_ne!(
        dfs.active_repair_attempt_records()[0].next_retry_unix_nanos,
        u64::MAX,
        "restored stale-base attempts must not remain durably suppressed"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn active_repair_attempt_does_not_resurrect_cleared_resume_identity() {
    let root = temp_root("active-repair-does-not-resurrect-resume");
    let owner = NodeId::from_legacy_named("node_resume_clear_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_resume_clear_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_resume_clear").expect("object");
    let version = DfsVersion::from_legacy_named("version_resume_clear").expect("version");
    let content_hash = content_hash(b"resume clear");
    let intent_id = "dfs_intent_resume_clear";
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let mut pending = PendingNetworkedWrite::new(
        object_id.clone(),
        version.clone(),
        content_hash.clone(),
        2 * 1024 * 1024 + 1,
    );
    let retry = pending.repair_retries.entry(replica.clone()).or_default();
    retry.attempts = 1;
    retry.inflight = false;
    retry.next_retry_tick = dfs.repair_retry_tick.saturating_add(10);
    dfs.writes
        .pending_writes
        .insert(intent_id.to_string(), pending);
    let stale_next_retry_unix_nanos = dfs_now_unix_nanos().saturating_add(60_000_000_000);
    dfs.repair_attempt_records.insert(
        (intent_id.to_string(), replica.clone()),
        DfsRepairAttemptRecord {
            intent_id: intent_id.to_string(),
            object_id,
            version,
            content_hash,
            missing_replica_node_id: replica,
            attempts: 1,
            in_flight: true,
            last_attempt_unix_nanos: dfs_now_unix_nanos(),
            next_retry_unix_nanos: stale_next_retry_unix_nanos,
            transfer_request_id: Some("dfs_stale_resume_identity".to_string()),
            last_error: Some("rejected transfer".to_string()),
        },
    );

    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert!(!records[0].in_flight);
    assert_eq!(records[0].transfer_request_id, None);
    assert_ne!(
        records[0].next_retry_unix_nanos, stale_next_retry_unix_nanos,
        "clearing a rejected resume must also refresh its durable retry deadline"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_response_conflicting_receipt_preserves_retry_state() {
    let root = temp_root("repair-conflicting-receipt-keeps-retry");
    let owner = NodeId::from_legacy_named("node_repair_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair response with a durable conflicting receipt";
    let content_hash = content_hash(bytes);
    let request_id = "dfs_net_req_repair_conflicting_receipt".to_string();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("metadata");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![replica.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: content_hash.clone(),
            size_bytes: bytes.len() as u64,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.record_repair_inflight(&intent.intent_id, &replica, request_id.clone());
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-repair-receipt",
    )
    .expect("seed conflicting receipt");

    let error = dfs
        .handle_repair_replica_response(
            replica.clone(),
            DfsRepairReplicaResponse {
                request_id: request_id.clone(),
                intent_id: intent.intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id,
                version: version_2,
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable { content_hash },
            },
        )
        .expect_err("conflicting durable receipt must reject repair response");
    assert!(
        error
            .to_string()
            .contains("conflicting DFS replica receipt"),
        "unexpected error: {error}"
    );

    let pending = dfs
        .writes
        .pending_writes
        .get(&intent.intent_id)
        .expect("pending write");
    assert!(
        pending.repair_retries.contains_key(&replica),
        "rejected repair response must keep retry state"
    );
    assert_eq!(
        pending.repair_inflight_request_ids.get(&replica),
        Some(&request_id),
        "rejected repair response must not clear the in-flight request before durable acceptance"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stateless_repair_response_restores_pending_write_without_inflight_request() {
    let root = temp_root("stateless-repair-response-restores-pending");
    let owner = NodeId::from_legacy_named("node_stateless_repair_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stateless_repair_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stateless_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stateless repair response bytes";
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite::new(
            object_id.clone(),
            version_2.clone(),
            manifest.content_hash.clone(),
            manifest.size_bytes,
        ),
    );
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("owner receipt");
    dfs.apply_committed_intent(&intent.intent_id)
        .expect("commit with missing replica");
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    let pending = dfs
        .writes
        .pending_writes
        .get_mut(&intent.intent_id)
        .expect("committed pending repair");
    pending.repair_retries.clear();
    pending.repair_inflight_request_ids.clear();
    dfs.repair_attempt_records.clear();
    assert!(
        dfs.active_repair_attempt_records().is_empty(),
        "an unattempted committed repair has no operational attempt record"
    );
    let durable_repair_queue = dfs.durable_repair_queue_records();
    assert_eq!(durable_repair_queue.len(), 1);
    assert_eq!(durable_repair_queue[0].intent_id, intent.intent_id);
    assert_eq!(durable_repair_queue[0].attempts, 0);
    assert_eq!(
        durable_repair_queue[0].missing_replica_node_id, replica,
        "restart durability must include the exact unattempted repair target"
    );
    drop(dfs);

    let mut recovered = networked_sifs_with_backend(&root, owner.clone());
    let pending = recovered
        .writes
        .pending_writes
        .get(&intent.intent_id)
        .expect("hydrated pending repair");
    assert!(
        pending.repair_inflight_request_ids.is_empty(),
        "fresh service instance should not have volatile repair inflight state"
    );

    let event = recovered
        .handle_repair_replica_response(
            replica.clone(),
            DfsRepairReplicaResponse {
                request_id: "dfs_net_req_stateless_repair_response".to_string(),
                intent_id: intent.intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            },
        )
        .expect("stateless repair response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::ReplicaRepaired {
            intent_id: ref repaired_intent,
            object_id: ref repaired_object,
            replica_node_id: ref repaired_replica,
            ..
        } if repaired_intent == &intent.intent_id
            && repaired_object == &object_id
            && repaired_replica == &replica
    ));
    assert!(recovered.coordinator.repair_jobs().is_empty());
    assert!(
        !recovered
            .writes
            .pending_writes
            .contains_key(&intent.intent_id),
        "fully settled repair should release its pending content and retry state immediately"
    );
    assert!(recovered.active_repair_attempt_records().is_empty());
    let object = recovered.metadata().object(&object_id).expect("object");
    assert!(object.current_locations.contains(&owner));
    assert!(object.current_locations.contains(&replica));
    drop(recovered);

    let mut reopened = networked_sifs_with_backend(&root, owner);
    let scheduled = reopened
        .retry_pending_repairs_bounded_with_attempts(1, &durable_repair_queue)
        .expect("classify stale durable repair queue record");
    assert!(
        scheduled.is_empty(),
        "a receipt-settled repair must not be rescheduled from a stale durable queue record"
    );
    assert!(
        reopened.durable_repair_queue_records().is_empty(),
        "a receipt-settled durable queue record must be pruned during reactive restore"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_conflicting_receipt_does_not_commit_visible_metadata() {
    let root = temp_root("repair-command-conflicting-receipt-no-visible-commit");
    let owner = NodeId::from_legacy_named("node_repair_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair command should not commit after receipt conflict";
    let content_hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-repair-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_repair_cmd_conflict".to_string(),
                intent_id: "dfs_intent_repair_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
                authority_shard: None,
            },
        )
        .expect("repair command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "repair command with conflicting receipt must be rejected, got {event:?}"
    );

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected repair command must not advance visible metadata"
    );
    assert!(
        !object.current_locations.contains(&replica),
        "rejected repair command must not add the local replica to visible locations"
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("backend object")
        .expect("backend object exists");
    assert_eq!(
        backend_object.version, version_1,
        "rejected repair command must not publish visible metadata to the backend"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "receipt preflight should reject the repair before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_accepts_current_replica_after_metadata_primary_moves() {
    let root = temp_root("repair-command-current-replica-after-primary-move");
    let source = NodeId::from_legacy_named("node_repair_moved_current_replica").expect("source");
    let target = NodeId::from_legacy_named("node_repair_moved_primary").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_moved_primary").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_repair_moved_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_moved_2").expect("target version");
    let file_system = file_system(source.clone());
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
                primary_node_id: target.clone(),
                replica_node_ids: vec![source.clone()],
                epoch: 2,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let network = InProcessNetwork::new();
    let _source_mailbox = network.add_node(source.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("services"), root.join("metadata"))
            .expect("services metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(target.clone()), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut object = file_metadata(source.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/repair-after-primary-move.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    dfs.add_object_metadata(object.clone()).expect("metadata");
    let bytes = b"repair bytes from current replica after metadata primary moved";
    let content_hash = content_hash(bytes);
    let command = DfsRepairReplicaCommand {
        background_repair: false,
        request_id: "dfs_net_req_repair_moved_primary".to_string(),
        intent_id: "dfs_intent_repair_moved_primary".to_string(),
        file_system_id: file_system.file_system_id,
        object_id: object_id.clone(),
        expected_base_version: Some(version_1),
        version: version_2.clone(),
        content_hash: content_hash.clone(),
        bytes: bytes.to_vec(),
        object_metadata: Some(object),
        authority_shard: None,
    };

    let event = dfs
        .handle_repair_replica(source.clone(), command.clone())
        .expect("repair command");

    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::ReplicaRepaired {
                object_id: ref repaired_object_id,
                ref replica_node_id,
                ..
            } if repaired_object_id == &object_id && replica_node_id == &target
        ),
        "unexpected repair event: {event:?}"
    );
    let replayed = dfs
        .handle_repair_replica(source, command)
        .expect("replayed repair command");
    assert!(
        matches!(replayed, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
        "a lost repair response must be safe to recover by redispatching the stable command"
    );
    let manifest = dfs
        .content()
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("repaired manifest");
    assert_eq!(manifest.content_hash, content_hash);
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("repaired object metadata");
    assert_eq!(object.version, version_2);
    assert!(object.current_locations.contains(&target));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_accepts_metadata_primary_for_stored_intent() {
    let root = temp_root("repair-command-metadata-primary-stored-intent");
    let coordinator =
        NodeId::from_legacy_named("node_repair_stored_intent_coordinator").expect("coordinator");
    let primary = NodeId::from_legacy_named("node_repair_stored_intent_primary").expect("primary");
    let target = NodeId::from_legacy_named("node_repair_stored_intent_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_stored_intent").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_repair_stored_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_stored_2").expect("target version");
    let intent_id = "dfs_intent_repair_stored_primary";
    let file_system = file_system(coordinator.clone());
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
                replica_node_ids: vec![target.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut object = file_metadata(coordinator.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/repair-stored-intent.bin".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![coordinator.clone()];
    let bytes = b"stored intent repair bytes from the metadata primary";
    let content_hash = content_hash(bytes);
    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: version_2.clone(),
            coordinator_node_id: coordinator.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 3,
            desired_replica_node_ids: vec![coordinator.clone(), target.clone()],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: Some(content_hash.clone()),
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("intent");

    let network = InProcessNetwork::new();
    let _primary_mailbox = network.add_node(primary.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(target.clone()), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(backend)
        .expect("metadata backend");

    let event = dfs
        .handle_repair_replica(
            primary.clone(),
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_repair_stored_primary".to_string(),
                intent_id: intent_id.to_string(),
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
        .expect("repair command");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::ReplicaRepaired {
            object_id: repaired_object_id,
            replica_node_id,
            ..
        } if repaired_object_id == object_id && replica_node_id == target
    ));
    let manifest = dfs
        .content()
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("repaired manifest");
    assert_eq!(manifest.content_hash, content_hash);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_rejects_unknown_object_from_non_primary_metadata_source() {
    assert_repair_hint_cannot_authorize_unknown_foreign_object(false);
}

#[test]
fn background_repair_hint_does_not_authorize_unknown_foreign_object() {
    assert_repair_hint_cannot_authorize_unknown_foreign_object(true);
}

fn assert_repair_hint_cannot_authorize_unknown_foreign_object(background_repair: bool) {
    let root = temp_root("repair-command-unknown-object-non-primary-source");
    let source = NodeId::from_legacy_named("node_repair_unknown_source").expect("source");
    let primary = NodeId::from_legacy_named("node_repair_unknown_primary").expect("primary");
    let target = NodeId::from_legacy_named("node_repair_unknown_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_unknown_primary").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_unknown_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_unknown_2").expect("target version");
    let file_system = file_system(source.clone());
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
                replica_node_ids: vec![target.clone()],
                epoch: 2,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let network = InProcessNetwork::new();
    let _source_mailbox = network.add_node(source.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(target), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut object = file_metadata(source.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/unknown-repair.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    let bytes = b"unknown object metadata should require shard primary";
    let content_hash = content_hash(bytes);

    let mut command = DfsRepairReplicaCommand {
        background_repair: false,
        request_id: "dfs_net_req_repair_unknown_primary".to_string(),
        intent_id: "dfs_intent_repair_unknown_primary".to_string(),
        file_system_id: file_system.file_system_id,
        object_id: object_id.clone(),
        expected_base_version: Some(version_1),
        version: version_2.clone(),
        content_hash,
        bytes: bytes.to_vec(),
        object_metadata: Some(object),
        authority_shard: None,
    };
    command.background_repair = background_repair;
    let event = dfs
        .handle_repair_replica(source, command)
        .expect("repair command");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected {
            object_id: rejected_object_id,
            reason,
            ..
        } if rejected_object_id == object_id
            && reason.contains("source is not current metadata shard primary")
            && reason.contains(primary.as_str())
    ));
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "a background scheduling hint must not permit unauthorized content storage"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_command_conflicting_receipt_does_not_store_orphan_content() {
    let root = temp_root("stage-command-conflicting-receipt-no-orphan-content");
    let owner = NodeId::from_legacy_named("node_stage_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage command should reject before storing orphan content";
    let content_hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-stage-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_net_req_stage_cmd_conflict".to_string(),
                intent_id: "dfs_intent_stage_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
                authority_shard: None,
            },
        )
        .expect("stage command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "stage command with conflicting receipt must be rejected, got {event:?}"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "stage receipt preflight should reject before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_commits_when_response_target_is_unroutable() {
    let root = temp_root("repair-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_repair_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_notify").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair committed despite lost requester";
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_repair_unroutable".to_string(),
                intent_id: "dfs_intent_repair_unroutable".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("unroutable response target should not fail repair commit");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::ReplicaRepaired {
            object_id: repaired_object_id,
            version: repaired_version,
            replica_node_id,
            ..
        } if repaired_object_id == object_id
            && repaired_version == version
            && replica_node_id == replica
    ));
    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Current
        } if data.version == version && data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_accepts_stale_base_when_content_hash_matches_current() {
    // Regression: a metadata-only version bump (chmod/chown/utimes/hardlink/
    // rename) advances an object past the version a queued content repair
    // targets, while leaving content unchanged. The repair's base no longer
    // matches current, but its content hash equals what the current version
    // still references. Content is addressed by hash, so the repair must be
    // accepted -- otherwise the only copy of the content never reaches the
    // replica and reads fail with NotFound despite metadata being present.
    let root = temp_root("repair-stale-base-matching-content");
    let owner = NodeId::from_legacy_named("node_stale_base_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_base_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_base").expect("object");
    let base_version = DfsVersion::from_legacy_named("version_0").expect("version");
    let content_version = DfsVersion::from_legacy_named("version_1").expect("version");
    let current_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"content unchanged across a metadata-only version bump";
    let hash = content_hash(bytes);

    // The replica's view of the object has already advanced to the
    // metadata-only bump (version_2), still referencing the same content. The
    // queued content repair, however, still targets the content write
    // (version_1) whose intent based off version_0.
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = current_version.clone();
    metadata.content_hash = Some(hash.clone());
    metadata.current_locations = vec![owner.clone()];

    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_stale_base".to_string(),
                intent_id: "dfs_intent_stale_base".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(base_version),
                version: content_version.clone(),
                content_hash: hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("repair with matching content hash should be accepted");

    assert!(
        matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
        "stale-base repair whose content matches current must commit"
    );

    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(
        matches!(read, NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == bytes),
        "replica must serve the repaired content-addressed blob"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reconnect_refreshes_unmanaged_local_manifest_receipt() {
    let root = temp_root("reconnect-refreshes-unmanaged-receipt");
    let local = NodeId::from_legacy_named("node_reconnect_unmanaged_local").expect("local");
    let remote = NodeId::from_legacy_named("node_reconnect_unmanaged_remote").expect("remote");
    let object_id =
        DfsObjectId::from_legacy_named("obj_reconnect_unmanaged_receipt").expect("object");
    let version = DfsVersion::from_legacy_named("version_reconnect_unmanaged").expect("version");
    let bytes = b"ordinary reconnect bytes";
    let mut dfs = networked_sifs_with_backend(&root, local.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed local manifest and blob");
    let mut metadata = file_metadata(local.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/reconnect-unmanaged.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![remote];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile unmanaged manifest");

    assert_eq!(reconciliation.refreshed_receipts, 1);
    assert!(!reconciliation.missing_current);
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == local),
        "ordinary reconnect keeps its legacy receipt refresh behavior"
    );
    assert!(
        dfs.metadata()
            .object(&object_id)
            .expect("object")
            .current_locations
            .contains(&local),
        "ordinary reconnect keeps its legacy location refresh behavior"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_conflicting_receipt_does_not_advance_visible_metadata() {
    let root = temp_root("commit-conflicting-receipt-no-visible-advance");
    let owner = NodeId::from_legacy_named("node_commit_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit must not advance over conflicting durable receipt";
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-commit-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_conflict".to_string(),
                intent_id: "dfs_intent_commit_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash: manifest.content_hash,
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("commit conflict response");

    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "commit with conflicting receipt must be rejected, got {event:?}"
    );
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected commit must not advance visible metadata"
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some("blake3:stale-conflicting-commit-receipt")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_repairs_missing_local_receipt_when_backend_is_authoritative() {
    let root = temp_root("commit-repairs-missing-local-receipt");
    let owner = NodeId::from_legacy_named("node_commit_receipt_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_receipt_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit should publish missing local receipt";
    let mut dfs = networked_sifs_with_backend(&root, replica.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");

    dfs.begin_message_committed_work();
    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_receipt".to_string(),
                intent_id: "dfs_intent_commit_receipt".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("commit response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted {
            object_id: committed,
            ..
        } if committed == object_id
    ));
    assert!(
        dfs.take_message_committed_work().publish_metadata,
        "the actual authoritative receipt mutation must schedule its publication"
    );
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(object.version, version_2);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, replica);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_intent_repairs_already_current_stale_locations() {
    let root = temp_root("committed-intent-repairs-stale-locations");
    let owner = NodeId::from_legacy_named("node_stale_location_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_location_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_location_commit").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"already current with stale locations";

    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata.clone())
        .expect("base visible object");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("manifest");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("intent");
    let mut stale_visible = metadata;
    stale_visible.version = version_2.clone();
    stale_visible.current_locations = vec![owner.clone()];
    stale_visible.content_hash = Some(manifest.content_hash.clone());
    stale_visible.content_size_bytes = Some(manifest.size_bytes);
    dfs.add_object_metadata(stale_visible)
        .expect("stale visible object");
    for node_id in [&owner, &replica] {
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version_2,
            node_id,
            &manifest.content_hash,
        )
        .expect("backend receipt");
        dfs.coordinator
            .record_durable_receipt(&intent.intent_id, node_id.clone())
            .expect("durable receipt");
    }
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2,
            content_hash: manifest.content_hash.clone(),
            size_bytes: manifest.size_bytes,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("repair stale visible locations");

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    let current_locations = object
        .current_locations
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected_locations = [owner.clone(), replica.clone()].into_iter().collect();
    assert_eq!(
        current_locations, expected_locations,
        "already-current commits must still repair stale durable replica locations"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_metadata_receipt_publication_is_idempotent() {
    let root = temp_root("metadata-receipt-publication-idempotent");
    let owner = NodeId::from_legacy_named("node_receipt_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_receipt_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_idempotent").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = "blake3:receipt-idempotent";

    let mut dfs = networked_sifs_with_backend(&root, owner);
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        content_hash,
    )
    .expect("first receipt");
    let journal_sequence_before = dfs
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata shard status")
        .into_iter()
        .map(|status| status.journal_last_sequence)
        .max()
        .unwrap_or_default();

    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version,
        &replica,
        content_hash,
    )
    .expect("duplicate receipt");

    let journal_sequence_after = dfs
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata shard status")
        .into_iter()
        .map(|status| status.journal_last_sequence)
        .max()
        .unwrap_or_default();
    assert_eq!(
        journal_sequence_after, journal_sequence_before,
        "duplicate replica receipts for the same content hash should not emit journal entries"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn conflicting_metadata_receipt_from_same_replica_is_rejected() {
    let root = temp_root("metadata-receipt-conflicting-hash-rejected");
    let owner = NodeId::from_legacy_named("node_receipt_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_receipt_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_conflict").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let content_hash = "blake3:receipt-original";
    let conflicting_hash = "blake3:receipt-conflicting";

    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert!(
        dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("first receipt")
    );
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    let error = dfs
        .put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            conflicting_hash,
        )
        .expect_err("conflicting receipt must be rejected");

    assert!(
        error
            .to_string()
            .contains("conflicting DFS replica receipt"),
        "unexpected error: {error}"
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].content_hash.as_deref(), Some(content_hash));
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "rejected conflicting receipt must not overwrite receipt metadata or emit journals"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_routes_bounded_directory_rename_and_resume_receipts() {
    let cluster = sifs_posix_cluster("sifs-cluster-bounded-rename");
    let SifsPosixCluster {
        root,
        primary,
        writer_node,
        mut writer,
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

    let from = "/users/bounded-old";
    let to = "/users/bounded-new";
    writer
        .posix_mkdir_all(&format!("{from}/nested"))
        .expect("routed source tree");
    for index in 0..4 {
        writer
            .posix_write_file(
                &format!("{from}/nested/{index}.txt"),
                index.to_string().as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            )
            .expect("routed source file");
    }
    let mut job = writer
        .posix_rename_directory_bounded_routed(from, to, 1)
        .expect("first routed bounded batch");
    while job.status == DfsNamespaceJobStatus::Running {
        job = writer
            .posix_resume_namespace_job_bounded_routed(&job.job_id, from, 1)
            .expect("next routed bounded batch");
    }
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(matches!(
        writer.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    for index in 0..4 {
        assert_eq!(
            writer
                .posix_read_file(&format!("{to}/nested/{index}.txt"))
                .expect("renamed file"),
            index.to_string().as_bytes()
        );
    }

    stop_primary.store(true, Ordering::SeqCst);
    let _primary = primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_mkdir_all_repairs_primary_when_local_chain_exists() {
    let cluster = sifs_two_primary_cluster("sifs-routed-mkdir-all-primary-repair");
    let SifsTwoPrimaryCluster {
        root,
        services_primary,
        services_primary_node,
        mut replica,
        replica_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let base = "/services/cluster-tests";
    let path = "/services/cluster-tests/primary-repair.txt";
    let stale = crate::posix_object::new_posix_metadata_object(
        &replica_node,
        base,
        DfsObjectKind::Directory,
    );
    let stale_id = stale.object_id.clone();
    replica
        .add_transient_object_metadata(stale)
        .expect("seed stale local chain");
    replica.transient_metadata_object_ids.remove(&stale_id);
    assert!(
        replica.posix_stat(base).is_ok(),
        "replica precondition should have local parent"
    );
    assert!(
        matches!(
            services_primary.posix_stat(base),
            Err(DfsPosixError::NotFound { .. })
        ),
        "services primary precondition should lack the parent"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all should repair primary despite local chain");
    let unused_content_candidate =
        NodeId::from_legacy_named("node_sifs_routed_mkdir_unused_content").expect("unused node");
    let mut write_options = DfsWriteOptions::replicated_eventual(vec![
        replica_node,
        services_primary_node,
        unused_content_candidate,
    ]);
    write_options.foreground_replica_repair = false;
    replica
        .posix_write_file(path, b"child", write_options)
        .expect("child write after authoritative mkdir_all");

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    assert!(services_primary.posix_stat(base).is_ok());
    assert!(services_primary.posix_stat(path).is_ok());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn committed_repair_recovery_follows_current_metadata_primary() {
    let root = temp_root("committed-repair-current-primary");
    let old = NodeId::from_legacy_named("repair-recovery-old-coordinator").unwrap();
    let primary = NodeId::from_legacy_named("repair-recovery-new-primary").unwrap();
    let missing = NodeId::from_legacy_named("repair-recovery-missing-replica").unwrap();
    let fs = file_system(old.clone());
    let object_id = DfsObjectId::from_legacy_named("repair-recovery-owned-object").unwrap();
    let version = DfsVersion::from_legacy_named("repair-recovery-committed-version").unwrap();
    let bytes = b"committed data survives the metadata-primary move";
    let hash = content_hash(bytes);
    let intent_id = "repair-recovery-original-intent";
    let control = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_file_system_config(&DfsFileSystemConfig::from(&fs))
            .unwrap();
        store
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: fs.file_system_id.clone(),
                shard_id: "services".into(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![old.clone(), missing.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".into(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .unwrap();
    });
    for (label, local, should_own) in [
        ("primary", primary.clone(), true),
        ("retired", old.clone(), false),
    ] {
        let mut object = file_metadata(old.clone());
        object.file_system_id = fs.file_system_id.clone();
        object.object_id = object_id.clone();
        object.path = "/services/repair-after-handoff.bin".into();
        object.version = version.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![old.clone()];
        object.content_hash = Some(hash.clone());
        object.content_size_bytes = Some(bytes.len() as u64);
        let mut backend =
            open_root_local_metadata_backend(root.join(label).join("metadata")).unwrap();
        backend.put_file_system(&fs).unwrap();
        backend.put_object(&object).unwrap();
        backend
            .put_version(&DfsObjectVersionRecord {
                metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
                content_durability: None,
                file_system_id: fs.file_system_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(hash.clone()),
                size_bytes: Some(bytes.len() as u64),
                locations: vec![old.clone()],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
                retention: Default::default(),
                module_metadata: Default::default(),
            })
            .unwrap();
        backend
            .put_intent(&DfsWriteIntentRecord {
                publication: DfsWritePublication::Current,
                intent_id: intent_id.into(),
                file_system_id: fs.file_system_id.clone(),
                object_id: object_id.clone(),
                base_version: None,
                target_version: version.clone(),
                coordinator_node_id: old.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 1,
                desired_replica_node_ids: vec![old.clone(), missing.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some(hash.clone()),
                object_metadata: Some(object),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .unwrap();
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: fs.file_system_id.clone(),
                    object_id: object_id.clone(),
                    committed_version: version.clone(),
                    owner_node_id: old.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: old.clone(),
                content_hash: Some(hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .unwrap();
        let network = InProcessNetwork::new();
        let dfs = NetworkedDfsService::new(
            network.add_node(local),
            LocalContentStore::new(root.join(label).join("content")).unwrap(),
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(backend)
        .unwrap();
        let queued = dfs.durable_repair_queue_records();
        assert_eq!(
            queued
                .iter()
                .any(|job| job.intent_id == intent_id && job.missing_replica_node_id == missing),
            should_own,
            "committed repair ownership must follow the current metadata primary, not the historical coordinator ({label})"
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_intent(&object_id, intent_id)
                .unwrap()
                .unwrap()
                .coordinator_node_id,
            old,
            "recovery must preserve the durable write identity"
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_object(&object_id)
                .unwrap()
                .unwrap()
                .version,
            version
        );
    }
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_receipt_defers_capture_location_and_settles_through_scheduler_after_commit() {
    for recreate_before_retry in [false, true] {
        let root = temp_root(&format!(
            "repair-capture-reservation-{recreate_before_retry}"
        ));
        let owner = NodeId::from_legacy_named("repair-capture-owner").unwrap();
        let replica = NodeId::from_legacy_named("repair-capture-replica").unwrap();
        let object_id = DfsObjectId::from_legacy_named("repair-capture-source").unwrap();
        let initial_version = DfsVersion::from_legacy_named("repair-capture-initial").unwrap();
        let version = DfsVersion::from_legacy_named("repair-capture-written").unwrap();
        let network = InProcessNetwork::new();
        let content_root = root.join("owner-content");
        let metadata_root = root.join("owner-metadata");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root).unwrap();
        backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
        let mut dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).unwrap(),
        )
        .with_metadata_backend(backend)
        .unwrap();
        dfs.bootstrap_distributed_sifs().unwrap();
        let mut target = NetworkedDfsService::new(
            network.add_node(replica.clone()),
            LocalContentStore::new(root.join("replica-content")).unwrap(),
        );
        target.add_file_system(file_system(owner.clone())).unwrap();
        let mut initial = file_metadata(owner.clone());
        initial.object_id = object_id.clone();
        initial.version = initial_version;
        initial.path = "/services/repair-capture-source.txt".into();
        initial.policy = DfsObjectPolicy::ReplicatedEventual;
        initial.current_locations = vec![owner.clone()];
        dfs.add_object_metadata(initial.clone()).unwrap();
        target.add_object_metadata(initial).unwrap();
        let bytes = b"receipt survives a namespace capture reservation";
        let outcome = dfs
            .write_distributed_reader_with_candidates(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version.clone(),
                    requesting_node_id: owner.clone(),
                },
                std::io::Cursor::new(bytes.to_vec()),
                vec![owner.clone(), replica.clone()],
                DfsReplicationPolicy::new(1, 2).unwrap(),
                DfsDistributedWriteOptions {
                    touch_parent_on_create: false,
                    repair: DfsWriteRepair::Background,
                    ..DfsDistributedWriteOptions::default()
                },
            )
            .unwrap();
        let NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } = outcome
        else {
            panic!("the real W1 write must commit before its missing-N repair");
        };
        assert_eq!(repair_jobs.len(), 1);
        assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
        let mut before = dfs
            .metadata_backend
            .as_ref()
            .unwrap()
            .get_version(&object_id, &version)
            .unwrap()
            .unwrap();
        let foreign = DfsModuleMetadataValue {
            schema_version: 1,
            payload: serde_json::json!({"foreign": "preserve"}),
        };
        before
            .module_metadata
            .insert("vendor.foreign".into(), foreign.clone());
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .put_version(&before)
            .unwrap();
        let mut snapshot = dfs.current_read_object_metadata(&object_id).unwrap();
        snapshot.path = "/services/repair-capture-source.txt".into();
        let created_at = OffsetDateTime::now_utc();
        let identity = DfsRevisionReference {
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            version: version.clone(),
        };
        let request = DfsContentReferenceRequest {
            wire_format: DfsContentReferenceFormat::NamespacedV6,
            namespace: "vendor.repair.capture".into(),
            path: snapshot.path.clone(),
            identity: identity.clone(),
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "held-capture".into(),
                metadata: None,
            },
            operation_id: format!("repair-capture:{recreate_before_retry}"),
        };
        let mut captured = before.clone();
        captured.retention.insert(
            request.namespace.clone(),
            DfsRevisionRetentionState {
                order: 0,
                grant: Some(Box::new(DfsRevisionRetentionGrant {
                    object: Box::new(snapshot.clone()),
                    origin: DfsRetentionGrantOrigin::ExplicitOperation,
                    granted_at_unix_nanos: created_at.unix_timestamp_nanos().try_into().unwrap(),
                    minimum: None,
                })),
            },
        );
        let mut capture = DfsNamespacePrepareRecord {
            operation_id: format!("namespace:repair-capture:{recreate_before_retry}"),
            file_system_id: sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch: 1,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: object_id.clone(),
                expected_version: Some(version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: snapshot.path.clone(),
                expected_object_id: Some(object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                content_reference_identity: Some(identity),
                contribution_digest: Some(
                    blake3::hash(&serde_json::to_vec(&request).unwrap())
                        .to_hex()
                        .to_string(),
                ),
                transaction_id: format!("file-history:{}", request.operation_id),
                file_system_id: sifs_file_system_id(),
                objects: Vec::new(),
                versions: vec![captured.clone()],
                advisory_lock_states: Vec::new(),
                created_at,
                extension_expectations: vec![DfsRevisionMetadataExpectation {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    expected: Some(Box::new(before.clone())),
                }],
                file_version_pins: Some(DfsFileVersionPinMutation {
                    add: vec![DfsMetadataVersionPinRecord {
                        namespace: Some(request.namespace.clone()),
                        file_system_id: sifs_file_system_id(),
                        object_id: object_id.clone(),
                        version: version.clone(),
                        pin_id: crate::content_reference_storage_id(
                            &request.namespace,
                            "held-capture",
                        ),
                        created_at,
                    }],
                    remove: Vec::new(),
                }),
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        crate::metadata_records::normalize_namespace_prepare_in_place(&mut capture);
        crate::metadata_prepare_codec::namespace_prepare_for_codec_authoring(
            &mut capture,
            DfsMetadataNamespacePrepareCodec::PackedV6,
        );
        capture.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&capture).unwrap();
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .prepare_namespace_transaction(&capture)
            .unwrap();
        assert!(capture.transaction.objects.is_empty());
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_version(&object_id, &version)
                .unwrap(),
            Some(before.clone())
        );
        let scheduled = dfs.retry_pending_repairs_bounded(1).unwrap();
        assert_eq!(
            scheduled.len(),
            1,
            "ordinary scheduler sends the missing replica repair"
        );
        let pending = dfs.writes.pending_writes.get(&intent_id).unwrap();
        let request_id = pending.repair_inflight_request_ids[&replica].clone();
        let attempts = pending.repair_retries[&replica].attempts;
        let due_tick = pending.repair_retries[&replica].next_retry_tick;
        assert!(pending.repair_retries[&replica].inflight);
        let target_events = target.process_incoming().unwrap();
        assert!(target_events.iter().any(|event| matches!(event,
            NetworkedDfsServiceEvent::ReplicaRepaired { object_id: repaired, .. } if repaired == &object_id)));
        let committed = DfsCommittedWorkTracker::new();
        dfs.set_committed_work_tracker(committed.clone());
        dfs.begin_message_committed_work();
        let error = dfs
            .process_incoming()
            .expect_err("receipt location must defer while capture owns the indexed source");
        assert!(error.to_string().contains("namespace prepare"), "{error}");
        assert!(
            committed.take().publish_metadata,
            "the durable receipt keeps its owner work despite the location deferral/error"
        );
        assert!(dfs.take_message_committed_work().publish_metadata);
        let receipt = dfs
            .backend_receipt_for_replica(&object_id, &version, &replica)
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt.content_hash.as_deref(),
            Some(content_hash(bytes).as_str())
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_version(&object_id, &version)
                .unwrap(),
            Some(before)
        );
        let pending = dfs
            .writes
            .pending_writes
            .get(&intent_id)
            .expect("deferral preserves pending write");
        assert_eq!(
            pending.repair_inflight_request_ids.get(&replica),
            Some(&request_id)
        );
        assert_eq!(pending.repair_retries[&replica].attempts, attempts);
        assert_eq!(pending.repair_retries[&replica].next_retry_tick, due_tick);
        assert!(pending.repair_retries[&replica].inflight);
        assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
        assert_eq!(
            dfs.coordinator.repair_jobs()[0].missing_replica_node_id,
            replica
        );
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_intent(&object_id, &intent_id)
                .unwrap()
                .unwrap()
                .coordinator_node_id,
            owner,
            "the original durable write owner survives receipt deferral"
        );
        let durable_queue = dfs.durable_repair_queue_records();
        assert_eq!(durable_queue.len(), 1);
        assert_eq!(durable_queue[0].intent_id, intent_id);
        assert_eq!(durable_queue[0].missing_replica_node_id, replica);
        let saved = dfs
            .metadata_backend
            .as_ref()
            .unwrap()
            .get_namespace_prepare(&capture.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            saved, capture,
            "repair receipt cannot alter the admitted capture readset/digest"
        );
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .commit_prepared_namespace_transaction(
                &capture.operation_id,
                &capture.operation_digest,
                capture.owner_epoch,
            )
            .unwrap();
        assert_eq!(
            dfs.metadata_backend
                .as_ref()
                .unwrap()
                .get_version(&object_id, &version)
                .unwrap(),
            Some(captured.clone())
        );
        if recreate_before_retry {
            drop(dfs);
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                    .unwrap();
            backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
            dfs = NetworkedDfsService::new(
                network.add_node(owner.clone()),
                LocalContentStore::new(&content_root).unwrap(),
            )
            .with_metadata_backend(backend)
            .expect("real service recreation must preserve or settle deferred location work");
            assert_eq!(
                dfs.coordinator.durable_receipt_count(&intent_id),
                2,
                "recreation must retain both physical receipts while location work is unsettled"
            );
        }
        let location_published = dfs
            .metadata_backend
            .as_ref()
            .unwrap()
            .get_version(&object_id, &version)
            .unwrap()
            .unwrap()
            .locations
            .contains(&replica);
        if !location_published {
            assert!(
                dfs.writes.pending_writes.contains_key(&intent_id),
                "receipt-only hydration must not discard unfinished location publication"
            );
            assert!(dfs.coordinator.repair_jobs().iter().any(|job| job.intent_id == intent_id && job.missing_replica_node_id == replica),
                "the durable receipt cannot erase missing-location repair ownership on recreation");
            if recreate_before_retry {
                let original_deadline = durable_queue[0].next_retry_unix_nanos;
                assert_ne!(
                    original_deadline,
                    u64::MAX,
                    "the first real repair attempt is retryable"
                );
                let now: u64 = OffsetDateTime::now_utc()
                    .unix_timestamp_nanos()
                    .try_into()
                    .unwrap();
                let remaining = original_deadline.saturating_sub(now);
                assert!(
                    remaining <= 30_000_000_000,
                    "the original first-attempt deadline must remain bounded"
                );
                // Honor the persisted real deadline exactly; do not rewrite it or shorten production cadence.
                std::thread::sleep(std::time::Duration::from_nanos(remaining));
            }
            let mut scheduled_again = Vec::new();
            // Advance only ordinary scheduler passes; use its recorded due tick, never shorten or rewrite retry state.
            for _ in 0..=due_tick {
                let scheduled = if recreate_before_retry {
                    dfs.retry_pending_repairs_bounded_with_attempts(1, &durable_queue)
                        .unwrap()
                } else {
                    dfs.retry_pending_repairs_bounded(1).unwrap()
                };
                if !scheduled.is_empty() {
                    scheduled_again = scheduled;
                    break;
                }
            }
            assert_eq!(
                scheduled_again.len(),
                1,
                "ordinary retry must dispatch the existing receipt's unfinished location"
            );
            if !recreate_before_retry {
                assert_eq!(
                    dfs.writes.pending_writes[&intent_id]
                        .repair_inflight_request_ids
                        .get(&replica),
                    Some(&request_id)
                );
            }
            target.process_incoming().unwrap();
            dfs.begin_message_committed_work();
            let events = dfs.process_incoming().unwrap();
            assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { object_id: repaired, .. } if repaired == &object_id)));
            assert!(
                dfs.take_message_committed_work().publish_metadata,
                "existing receipt retry owns the new location journal edge"
            );
        }
        let final_row = dfs
            .metadata_backend
            .as_ref()
            .unwrap()
            .get_version(&object_id, &version)
            .unwrap()
            .unwrap();
        assert!(final_row.locations.contains(&replica));
        assert_eq!(final_row.retention, captured.retention);
        assert_eq!(
            final_row.module_metadata.get("vendor.foreign"),
            Some(&foreign)
        );
        assert_eq!(
            dfs.backend_receipt_for_replica(&object_id, &version, &replica)
                .unwrap(),
            Some(receipt)
        );
        assert!(dfs.coordinator.repair_jobs().is_empty());
        assert!(!dfs.writes.pending_writes.contains_key(&intent_id));
        assert!(dfs.durable_repair_queue_records().is_empty());
        drop(dfs);
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root).unwrap();
        assert_eq!(
            reopened.get_version(&object_id, &version).unwrap(),
            Some(final_row)
        );
    }
}
