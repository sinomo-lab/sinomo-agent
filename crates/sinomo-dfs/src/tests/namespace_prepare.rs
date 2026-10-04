//! Namespace prepare regression tests.

use super::*;

#[test]
fn clustered_w2_foreground_create_commits_the_exact_authorized_namespace_prepare() {
    let root = temp_root("clustered-w2-foreground-exact-create-prepare");
    let local = NodeId::from_legacy_named("node_clustered_w2_create_local").expect("local");
    let remote = NodeId::from_legacy_named("node_clustered_w2_create_remote").expect("remote");
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
            .expect("stage calls")
            .push(stage.request_id.clone());
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: vec![DfsRoutedMessage {
                source_node_id: command.routed_message.target_node_id.clone(),
                target_node_id: command.routed_message.source_node_id.clone(),
                frame_id: 101,
                message_id: 101,
                message: DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                    request_id: stage.request_id.clone(),
                    intent_id: stage.intent_id.clone(),
                    file_system_id: stage.file_system_id.clone(),
                    object_id: stage.object_id.clone(),
                    version: stage.version.clone(),
                    replica_node_id: command.routed_message.target_node_id.clone(),
                    result: DfsBlobTransferResult::Durable {
                        content_hash: stage.content_hash.clone(),
                    },
                }),
            }],
        })
    });
    let transport = node_transport_with_canonical_message_peer(
        local.clone(),
        vec![remote.clone()],
        Duration::from_millis(1),
        sender,
    );
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(local.clone())))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    let metadata_root = root.join("metadata");
    let observed_prepare = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_from_hook = Arc::clone(&observed_prepare);
    let first_durable_created_at = Arc::new(Mutex::new(None));
    let creation_from_hook = Arc::clone(&first_durable_created_at);
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |prepare| {
            assert_eq!(prepare.transaction.objects.len(), 2);
            assert_eq!(prepare.transaction.versions.len(), 2);
            let child = prepare
                .transaction
                .objects
                .iter()
                .find(|object| object.kind == DfsObjectKind::File)
                .unwrap();
            let intent_id = prepare
                .operation_id
                .strip_prefix("namespace:create:")
                .unwrap();
            let backend = open_root_local_metadata_backend(metadata_root).unwrap();
            let pending = backend
                .get_intent(&child.object_id, intent_id)
                .unwrap()
                .unwrap();
            assert_eq!(pending.status, DfsWriteIntentStatus::ReceiptsPending);
            *creation_from_hook.lock().unwrap() = Some(pending.created_at);
            assert!(
                pending.base_version.is_some(),
                "public create retains its transient version token"
            );
            assert_eq!(
                pending.base_version.as_ref(),
                pending
                    .object_metadata
                    .as_ref()
                    .map(|object| &object.version)
            );
            assert!(backend.get_object(&child.object_id).unwrap().is_none());
            assert_eq!(pending.content_hash, child.content_hash);
            assert!(
                backend
                    .get_file_system(&pending.file_system_id)
                    .unwrap()
                    .unwrap()
                    .file_history
                    .is_empty()
            );
            drop(backend);
            // Count only canonical initial preparation and final C/T barriers;
            // earlier content receipt collection stays outside this assertion.
            crate::metadata_journal::reset_journal_append_sync_calls_for_test();
            observed_from_hook.store(true, std::sync::atomic::Ordering::Release);
        }));
    });

    let stat = dfs
        .posix_write_file(
            "/services/clustered-w2-create.txt",
            &[0x57; 4096],
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("W2 policy"),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("clustered W2 create");

    assert!(observed_prepare.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        crate::metadata_journal::journal_append_sync_calls_for_test(),
        2,
        "public4KiB create must share one I/R/P sync and one C/T sync"
    );

    assert_eq!(stage_calls.lock().expect("stage calls").len(), 1);
    let backend = dfs.metadata_backend.as_ref().expect("metadata backend");
    let intent = backend
        .all_intents()
        .expect("write intents")
        .into_iter()
        .find(|intent| intent.object_id == stat.object_id)
        .expect("committed create intent");
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(
        Some(intent.created_at),
        *first_durable_created_at.lock().unwrap(),
        "service terminalization must adopt the first durable intent creation identity returned by preparation"
    );
    let operation_id = format!("namespace:create:{}", intent.intent_id);
    assert!(
        backend
            .get_namespace_prepare(&operation_id)
            .expect("read create prepare")
            .is_none(),
        "the committed create prepare must retain only compact retry evidence"
    );
    let journal = backend.journal_since(0).expect("create journal");
    let prepare_entry = journal
        .iter()
        .find(|entry| {
            matches!(
                &entry.mutation,
                DfsMetadataMutation::PutNamespacePrepare {
                    operation_id: candidate,
                    status: DfsNamespacePrepareStatus::Prepared,
                    ..
                } if candidate == &operation_id
            )
        })
        .expect("authoritative create prepare entry");
    let prepare = prepare_entry
        .mutation
        .namespace_prepare_payload()
        .expect("decode create prepare")
        .expect("create prepare payload")
        .into_owned();
    assert_eq!(
        prepare.operation_digest,
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest")
    );
    let visible_object = backend
        .get_object(&stat.object_id)
        .expect("read visible object")
        .expect("visible object");
    assert!(
        prepare
            .transaction
            .objects
            .iter()
            .any(|object| object == &visible_object),
        "the visible child must be byte-for-byte the authorized prepared child"
    );
    assert!(
        prepare
            .transaction
            .objects
            .iter()
            .any(|object| object.path == SIFS_SERVICES_PATH),
        "the exact create prepare must include its parent touch"
    );

    let mut child_only = file_metadata(local.clone());
    child_only.object_id =
        DfsObjectId::from_legacy_named("obj_clustered_w2_child_only").expect("object");
    child_only.path = "/services/clustered-w2-child-only.txt".to_string();
    child_only.file_system_id = sifs_file_system_id();
    child_only.owner_node_id = local.clone();
    child_only.current_locations = vec![local.clone()];
    child_only.policy = DfsObjectPolicy::ReplicatedEventual;
    let child_only_base = child_only.version.clone();
    let child_only_target = crate::namespace::next_version(&child_only_base);
    let child_only_id = child_only.object_id.clone();
    dfs.add_transient_object_metadata(child_only)
        .expect("child-only transient metadata");
    let mut child_only_outcome = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: child_only_id.clone(),
                target_version: child_only_target,
                requesting_node_id: local.clone(),
            },
            Some(child_only_base),
            b"child-only first publication".to_vec(),
            vec![local, remote],
            DfsReplicationPolicy::new(2, 2).expect("child-only W2 policy"),
        )
        .expect("clustered child-only W2 create");
    if let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = &child_only_outcome {
        let intent_id = intent_id.clone();
        for _ in 0..4 {
            dfs.process_incoming()
                .expect("process child-only replica receipt");
            child_only_outcome = dfs
                .write_outcome(&intent_id)
                .expect("child-only write outcome");
            if !matches!(child_only_outcome, NetworkedDfsWriteOutcome::Pending { .. }) {
                break;
            }
        }
    }
    assert!(
        matches!(
            &child_only_outcome,
            NetworkedDfsWriteOutcome::Committed { .. }
        ),
        "child-only first publication must commit: {child_only_outcome:?}"
    );
    assert_eq!(stage_calls.lock().expect("stage calls").len(), 2);
    let backend = dfs.metadata_backend.as_ref().expect("metadata backend");
    let child_only_intent = backend
        .all_intents()
        .expect("write intents")
        .into_iter()
        .find(|intent| intent.object_id == child_only_id)
        .expect("child-only create intent");
    let child_only_operation_id = format!("namespace:create:{}", child_only_intent.intent_id);
    assert!(
        backend
            .get_namespace_prepare(&child_only_operation_id)
            .expect("read child-only prepare")
            .is_none()
    );
    let child_only_journal = backend.journal_since(0).expect("child-only journal");
    let child_only_prepare = child_only_journal
        .iter()
        .find(|entry| {
            matches!(
                &entry.mutation,
                DfsMetadataMutation::PutNamespacePrepare {
                    operation_id: candidate,
                    status: DfsNamespacePrepareStatus::Prepared,
                    ..
                } if candidate == &child_only_operation_id
            )
        })
        .expect("authoritative child-only prepare entry")
        .mutation
        .namespace_prepare_payload()
        .expect("decode child-only prepare")
        .expect("child-only prepare payload")
        .into_owned();
    let child_only_visible = backend
        .get_object(&child_only_id)
        .expect("read child-only object")
        .expect("visible child-only object");
    assert_eq!(
        child_only_prepare.status,
        DfsNamespacePrepareStatus::Prepared
    );
    assert_eq!(
        child_only_prepare.transaction.objects,
        vec![child_only_visible]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn concurrent_create_prepare_reuses_the_durable_winning_payload() {
    for mismatched_permissions in [false, true] {
        let root = temp_root("concurrent-exact-create-prepare");
        let local = NodeId::from_legacy_named("node_clustered_w2_create_local").expect("local");
        let remote = NodeId::from_legacy_named("node_clustered_w2_create_remote").expect("remote");
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
                .expect("stage calls")
                .push(stage.request_id.clone());
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![DfsRoutedMessage {
                    source_node_id: command.routed_message.target_node_id.clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: 101,
                    message_id: 101,
                    message: DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                        request_id: stage.request_id.clone(),
                        intent_id: stage.intent_id.clone(),
                        file_system_id: stage.file_system_id.clone(),
                        object_id: stage.object_id.clone(),
                        version: stage.version.clone(),
                        replica_node_id: command.routed_message.target_node_id.clone(),
                        result: DfsBlobTransferResult::Durable {
                            content_hash: stage.content_hash.clone(),
                        },
                    }),
                }],
            })
        });
        let transport = node_transport_with_canonical_message_peer(
            local.clone(),
            vec![remote.clone()],
            Duration::from_millis(1),
            sender,
        );
        let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        shared_kv.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                    local.clone(),
                )))
                .expect("filesystem config");
            control
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: sifs_file_system_id(),
                    shard_id: "root".to_string(),
                    primary_node_id: local.clone(),
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                })
                .expect("metadata shard");
        });
        let mut dfs = NetworkedDfsService::new(
            transport,
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(shared_kv)
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
        )
        .expect("metadata backend");
        dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

        let metadata_root = root.join("metadata");
        let winner = Arc::new(Mutex::new(None));
        let captured = winner.clone();
        crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |prepare| {
                let mut raced = prepare.clone();
                raced.transaction.created_at += time::Duration::nanoseconds(1);
                if mismatched_permissions {
                    raced.transaction.objects[0].mode ^= 0o100;
                }
                raced.operation_digest =
                    crate::service_replica::metadata_namespace_prepare_payload_digest(&raced)
                        .unwrap();
                open_root_local_metadata_backend(metadata_root)
                    .unwrap()
                    .prepare_namespace_transaction(&raced)
                    .unwrap();
                *captured.lock().unwrap() = Some(raced);
            }));
        });
        let result = dfs.posix_write_file(
            "/services/raced-create.txt",
            b"one stable create",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![local, remote],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        );
        let winner = winner.lock().unwrap().clone().unwrap();
        if mismatched_permissions {
            let error = result.expect_err("same intent id must not adopt different permissions");
            assert!(
                error
                    .to_string()
                    .contains("target payload does not match recovered intent"),
                "{error}"
            );
            assert!(
                dfs.metadata_backend()
                    .unwrap()
                    .get_object(&winner.transaction.objects[0].object_id)
                    .unwrap()
                    .is_none()
            );
            continue;
        }
        let stat = result.expect(
            "foreground must resume the exact prepare persisted by a racing recovery worker",
        );
        let durable = dfs
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            &durable,
            winner
                .transaction
                .objects
                .iter()
                .find(|o| o.object_id == stat.object_id)
                .unwrap()
        );
        let prepares = dfs
            .metadata_backend()
            .unwrap()
            .journal_since(0)
            .unwrap()
            .into_iter()
            .filter_map(|entry| {
                entry
                    .mutation
                    .namespace_prepare_payload()
                    .ok()
                    .flatten()
                    .map(|p| p.into_owned())
            })
            .filter(|prepare| prepare.operation_id == winner.operation_id)
            .collect::<Vec<_>>();
        assert!(!prepares.is_empty());
        assert!(
            prepares
                .iter()
                .all(|p| p.operation_digest == winner.operation_digest)
        );
    }
}
