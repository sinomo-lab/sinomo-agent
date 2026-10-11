//! Namespace prepare regression tests.

use super::*;

#[test]
fn clustered_w2_foreground_create_commits_the_exact_authorized_namespace_prepare() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("exact-create", 2);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "clustered-w2-foreground-exact-create-prepare",
        nodes[0].clone(),
    )
    .expect("normal runtime fixture");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("native replica");
    }
    runtime.start_nodes().expect("normal native cluster");
    let control = runtime
        .shared_control_plane()
        .expect("native control plane");
    let configs = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&sifs_file_system_id())
        })
        .expect("actual assignments");
    let services = configs
        .iter()
        .find(|shard| shard.shard_id == "services")
        .expect("actual services assignment");
    let local = services.primary_node_id.clone();
    let remote = nodes.iter().find(|node| **node != local).unwrap().clone();
    let mut dfs = runtime.client(&local).expect("normal primary client");
    let metadata_root = default_dfs_metadata_shards_root(runtime.data_dir(&local));
    let hook_configs = configs.clone();
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
            let intent_id = prepare.operation_id.strip_prefix("write:").unwrap();
            let backend = open_local_sharded_metadata_backend(metadata_root, hook_configs).unwrap();
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
                    .retention_policies
                    .get("module.dfs.history")
                    .is_none_or(|policy| policy.rules.is_empty())
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
        "public 4 KiB create must append one immutable prepare and one digest commit"
    );

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
    let operation_id = format!("write:{}", intent.intent_id);
    let record = backend
        .prepared_operation_record(&operation_id)
        .expect("read retained create preparation")
        .expect("sealed create body");
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    assert!(
        record.committed_entry.is_some(),
        "exact retry retains its original result"
    );
    assert_eq!(
        record.digest.payload_digest,
        record.payload.canonical_digest().unwrap()
    );
    let journal = backend.journal_since(0).expect("create journal");
    let authored = journal
        .iter()
        .filter(|entry| match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 {
                operation_id: candidate,
                ..
            }
            | DfsMetadataMutation::CommitPreparedOperationV7 {
                operation_id: candidate,
                ..
            } => candidate == &operation_id,
        })
        .collect::<Vec<_>>();
    assert_eq!(authored.len(), 2, "one prepare and one digest-only commit");
    let DfsMetadataMutation::PrepareOperationV7 {
        record: prepared, ..
    } = &authored[0].mutation
    else {
        panic!("first durable operation must prepare its full body");
    };
    assert_eq!(prepared.payload, record.payload);
    let DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } = &authored[1].mutation
    else {
        panic!("second durable operation must commit only the digest");
    };
    assert_eq!(digest, &record.digest);
    let prepare = &record.payload;
    let remote_receipt = prepare
        .content_receipts
        .iter()
        .find(|receipt| receipt.replica_node_id == remote)
        .expect("actual remote durable receipt");
    assert_eq!(remote_receipt.receipt.object_id, stat.object_id);
    let remote_client = runtime.client(&remote).expect("normal remote client");
    assert_eq!(
        remote_client
            .content
            .read_blob(
                remote_receipt
                    .content_hash
                    .as_deref()
                    .expect("receipt content hash")
            )
            .unwrap(),
        vec![0x57; 4096]
    );
    drop(remote_client);
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
    let backend = dfs.metadata_backend.as_ref().expect("metadata backend");
    let child_only_intent = backend
        .all_intents()
        .expect("write intents")
        .into_iter()
        .find(|intent| intent.object_id == child_only_id)
        .expect("child-only create intent");
    let child_only_operation_id = format!("write:{}", child_only_intent.intent_id);
    let child_only_record = backend
        .prepared_operation_record(&child_only_operation_id)
        .expect("read child-only preparation")
        .expect("sealed child-only body");
    assert_eq!(
        child_only_record.status,
        DfsPreparedOperationStatus::Committed
    );
    let child_only_visible = backend
        .get_object(&child_only_id)
        .expect("read child-only object")
        .expect("visible child-only object");
    assert_eq!(
        child_only_record.payload.transaction.objects,
        vec![child_only_visible]
    );
    assert!(
        child_only_record
            .payload
            .content_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id != dfs.node_id)
    );
    drop(dfs);
    drop(control);
    runtime.close().expect("native fixture shutdown");
}

#[test]
fn concurrent_create_rejects_every_different_immutable_body_without_publication() {
    for changed_permissions in [false, true] {
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("raced-create", 2);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("raced-immutable-create", nodes[0].clone())
                .unwrap();
        runtime.define_node(nodes[1].clone()).unwrap();
        runtime.start_nodes().unwrap();
        let configs = runtime.metadata_shards().unwrap();
        let services = configs
            .iter()
            .find(|shard| shard.shard_id == "services")
            .unwrap();
        let local = services.primary_node_id.clone();
        let mut dfs = runtime.client(&local).unwrap();
        let metadata_root = default_dfs_metadata_shards_root(runtime.data_dir(&local));
        let captured = Arc::new(Mutex::new(None));
        let captured_from_hook = captured.clone();
        let recovery_fence = Arc::new(Mutex::new(None));
        let fence_from_hook = recovery_fence.clone();
        crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |payload| {
                let mut raced = payload.clone();
                raced.transaction.created_at += time::Duration::nanoseconds(1);
                if changed_permissions {
                    let child = raced
                        .transaction
                        .objects
                        .iter_mut()
                        .find(|object| object.kind == DfsObjectKind::File)
                        .unwrap();
                    child.mode ^= 0o100;
                    raced.terminal_intent.as_mut().unwrap().object_metadata = Some(child.clone());
                }
                let mut backend =
                    open_local_sharded_metadata_backend(metadata_root, configs).unwrap();
                let scope = backend
                    .publication_owner_scope(&raced.file_system_id, &raced.shard_id)
                    .unwrap();
                *fence_from_hook.lock().unwrap() =
                    Some(crate::publication_guard::shard_fence_guard(&scope));
                // This actual writer authors only a hidden proposal. The
                // retained physical fence prevents recovery publication during
                // the foreground's immutable-body rejection.
                backend.prepare_operation(raced.clone(), None).unwrap();
                *captured_from_hook.lock().unwrap() = Some(raced);
            }));
        });
        let error = dfs
            .posix_write_file(
                "/services/raced-create.txt",
                b"one stable create",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .expect_err("a different timestamp or permissions cannot adopt another immutable body");
        assert!(
            matches!(error, DfsPosixError::Dfs(DfsError::Metadata { .. })),
            "{error}"
        );
        let raced = captured.lock().unwrap().clone().unwrap();
        let child = raced
            .transaction
            .objects
            .iter()
            .find(|object| object.kind == DfsObjectKind::File)
            .unwrap();
        let backend = dfs.metadata_backend().unwrap();
        assert!(backend.get_object(&child.object_id).unwrap().is_none());
        let record = backend
            .prepared_operation_record(&raced.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(record.payload.as_ref(), &raced);
        assert_eq!(record.status, DfsPreparedOperationStatus::Prepared);
        let authored = backend
            .journal_since(0)
            .unwrap()
            .into_iter()
            .filter(|entry| match &entry.mutation {
                DfsMetadataMutation::PrepareOperationV7 { record, .. } => {
                    record.digest.operation_id == raced.operation_id
                }
                DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                    digest.operation_id == raced.operation_id
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            authored.len(),
            1,
            "only the hidden racing prepare may exist"
        );
        assert!(matches!(
            authored[0].mutation,
            DfsMetadataMutation::PrepareOperationV7 { .. }
        ));
        drop(recovery_fence.lock().unwrap().take());
        drop(dfs);
        runtime.close().unwrap();
    }
}
