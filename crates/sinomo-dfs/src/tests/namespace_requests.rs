//! Namespace requests regression tests.

use super::*;

#[test]
fn mkdir_all_commits_a_missing_chain_in_one_namespace_transaction() {
    let root = temp_root("mkdir-all-single-transaction");
    let node = NodeId::from_legacy_named("node_mkdir_all_single_transaction").expect("node id");
    let mut dfs =
        open_shared_metadata_posix_service(&root, &root.join("metadata"), node, "content");
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");

    crate::metadata_backend::reset_metadata_journal_write_count();
    dfs.posix_mkdir_all("/services/batched/one/two")
        .expect("create directory chain");
    assert_eq!(
        crate::metadata_backend::metadata_journal_write_count(),
        1,
        "one mkdir-all operation should publish its complete missing chain atomically"
    );
    for path in [
        "/services/batched",
        "/services/batched/one",
        "/services/batched/one/two",
    ] {
        assert_eq!(
            dfs.posix_stat(path).expect("created directory").kind,
            DfsObjectKind::Directory
        );
    }

    crate::metadata_backend::reset_metadata_journal_write_count();
    dfs.posix_mkdir_all("/services/batched/one/two")
        .expect("repeat existing directory chain");
    assert_eq!(
        crate::metadata_backend::metadata_journal_write_count(),
        0,
        "an already-complete chain must remain a no-op"
    );
}

#[test]
fn maintenance_tick_rebinds_activated_handoff_before_namespace_job_mutation() {
    let root = temp_root("maintenance-tick-rebinds-activated-handoff-first");
    let local = NodeId::from_legacy_named("node_maintenance_handoff_rebind").expect("local");
    let mut dfs = networked_sifs_with_backend(&root, local.clone());
    let now = OffsetDateTime::now_utc();
    let invalid_job = DfsNamespaceJobRecord {
        job_id: "invalid-job-behind-activated-handoff".to_string(),
        file_system_id: sifs_file_system_id(),
        origin: None,
        kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
        status: DfsNamespaceJobStatus::Running,
        root_path: "/services/sandbox/instances/handoff-rebind/work".to_string(),
        target_path: None,
        pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryStart {
            source_object_id: DfsObjectId::generate(),
        }],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    };
    dfs.metadata_backend
        .as_deref_mut()
        .expect("metadata backend")
        .put_namespace_job(&invalid_job)
        .expect("install invalid namespace job");

    // Simulate the activation transaction committing before the retiring
    // process advances its local marker. The real transaction records the
    // exact completion proof atomically with the active assignment, so build
    // that lifecycle instead of publishing an impossible proof-less state.
    let shard_id = DfsMetadataShardId::new("root");
    let removed_replica =
        NodeId::from_legacy_named("node_maintenance_removed_replica").expect("removed replica");
    let retiring = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![removed_replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let active = DfsMetadataShardConfig {
        replica_node_ids: Vec::new(),
        epoch: 2,
        ..retiring.clone()
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_metadata_shard(&retiring)
            .expect("publish retiring assignment");
        let handoff = store
            .begin_metadata_shard_membership_handoff(&retiring, &active)
            .expect("begin removal handoff")
            .expect("new pending handoff");
        let freeze = dfs
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &handoff.handoff_id,
                retiring.epoch,
            )
            .expect("install retiring freeze")
            .expect("local shard");
        let fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: local.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint.clone(),
        };
        let handoff = store
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &fence)
            .expect("acknowledge retiring fence")
            .expect("exact pending handoff");
        assert!(
            store
                .complete_metadata_shard_membership_handoff(
                    &retiring,
                    &active,
                    &DfsMetadataShardMembershipHandoffProof {
                        handoff_id: handoff.handoff_id,
                        retiring_shard: retiring.clone(),
                        desired_shard: active.clone(),
                        journal_sequence: freeze.journal_sequence,
                        state_fingerprint: freeze.state_fingerprint,
                        acknowledged_node_ids: vec![local.clone(), removed_replica.clone()],
                    },
                )
                .expect("commit activated assignment")
        );
    });
    let mut dfs = dfs.with_control_plane(control_plane);

    let mut maintenance_host = RecordingMaintenanceHost::default();
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("publisher"));
    let unused_target =
        NodeId::from_legacy_named("node_maintenance_unused_replica").expect("unused target");
    let mut transport = ApplyingMetadataJournalTransport::new(unused_target);
    let report = run_dfs_maintenance_tick_with_host(
        &mut dfs,
        &mut maintenance_host,
        active.file_system_id.clone(),
        &local,
        vec![active.clone()],
        &mut publisher_host,
        &mut transport,
        DfsMaintenanceTickOptions {
            namespace_job_max_jobs: 4,
            namespace_job_max_steps_per_job: 4,
            conflict_max_records: 0,
            write_intent_max_records: 0,
            repair_max_jobs: 0,
            lifecycle: DfsMaintenanceRunOptions {
                anti_entropy_max_objects: 0,
                anti_entropy_max_issues: 0,
                repair_metadata: false,
                cleanup_stale_staging: false,
                ..DfsMaintenanceRunOptions::default()
            },
            ..DfsMaintenanceTickOptions::default()
        },
    )
    .expect("maintenance must reconcile authority before frozen metadata work");

    assert_eq!(report.namespace_jobs.failed_job_count, 1);
    let failed = dfs
        .metadata_backend
        .as_deref()
        .expect("metadata backend")
        .get_namespace_job(&invalid_job.job_id)
        .expect("read namespace job")
        .expect("namespace job remains recorded");
    assert_eq!(failed.status, DfsNamespaceJobStatus::Failed);
    let marker = dfs
        .metadata_backend
        .as_deref()
        .expect("metadata backend")
        .metadata_shard_write_freeze(&shard_id)
        .expect("read durable active marker")
        .expect("active generation marker remains durable");
    assert_eq!(marker.retiring_epoch, retiring.epoch);
    assert_eq!(marker.activated_epoch, Some(active.epoch));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_missing_namespace_transaction_payload_uses_keyed_replay() {
    let root = temp_root("metadata-journal-namespace-transaction-keyed-replay");
    let owner = NodeId::from_legacy_named("node_namespace_tx_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut unrelated = file_metadata(owner.clone());
    unrelated.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_tx_unrelated").expect("object");
    unrelated.path = "/services/namespace-tx/unrelated.txt".to_string();
    backend.put_object(&unrelated).expect("unrelated object");

    let mut touched = file_metadata(owner.clone());
    touched.object_id = DfsObjectId::from_legacy_named("obj_namespace_tx_touched").expect("object");
    touched.path = "/services/namespace-tx/touched.txt".to_string();
    touched.version =
        DfsVersion::from_legacy_named("version_namespace_tx_touched").expect("touched version");
    let transaction = DfsNamespaceTransaction {
        transaction_id: "namespace_tx_keyed_replay".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![touched.clone()],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut entry = backend
        .apply_namespace_transaction(&transaction)
        .expect("transaction");
    if let DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } = &mut entry.mutation
    {
        *transaction = None;
    } else {
        panic!("expected namespace transaction journal entry");
    }

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content);
    dfs.metadata.add_file_system(file_system);
    dfs.set_metadata_backend_unhydrated(backend);

    dfs.apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
        shard_id: entry.shard_id.clone(),
        next_after_sequence: entry.sequence,
        entries: vec![entry],
    })
    .expect("incremental namespace transaction replay");

    assert!(
        dfs.metadata.object(&touched.object_id).is_some(),
        "missing-payload namespace transaction replay should restore touched objects"
    );
    assert!(
        dfs.metadata.object(&unrelated.object_id).is_none(),
        "missing-payload namespace transaction replay should not scan and import the whole file system"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_missing_namespace_transaction_record_fails_without_full_scan() {
    let root = temp_root("metadata-journal-namespace-transaction-missing-record");
    let owner = NodeId::from_legacy_named("node_namespace_tx_missing_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut unrelated = file_metadata(owner.clone());
    unrelated.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_tx_missing_unrelated").expect("object");
    unrelated.path = "/services/namespace-tx-missing/unrelated.txt".to_string();
    backend.put_object(&unrelated).expect("unrelated object");

    let entry = DfsMetadataJournalEntry {
        shard_id: DfsMetadataShardId::new("root"),
        sequence: 99,
        created_at: OffsetDateTime::UNIX_EPOCH,
        mutation: DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: file_system.file_system_id.clone(),
            transaction_id: "namespace_tx_missing_record".to_string(),
            object_count: 1,
            operation_id: None,
            transaction: None,
        },
    };

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content);
    dfs.metadata.add_file_system(file_system);
    dfs.set_metadata_backend_unhydrated(backend);

    let error = dfs
        .apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
            shard_id: entry.shard_id.clone(),
            next_after_sequence: entry.sequence,
            entries: vec![entry],
        })
        .expect_err("missing namespace transaction record should fail replay");
    assert!(
        error
            .to_string()
            .contains("cannot apply namespace transaction namespace_tx_missing_record without its transaction payload")
    );
    assert!(
        dfs.metadata.object(&unrelated.object_id).is_none(),
        "unrecoverable sparse namespace transaction replay must not import the whole file system"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reader_backed_stable_create_replays_after_reopen_and_rejects_a_new_digest() {
    let root = temp_root("reader-stable-create-reopen");
    let node_id = NodeId::from_legacy_named("node_reader_stable_create").expect("node");
    let path = "/documents/reader-create.txt";
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);

    let created = {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/documents").expect("mkdir");
        dfs.posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"captured once".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("stable reader create")
    };

    let mut restarted = networked_sifs_with_backend(&root, node_id.clone());
    assert_eq!(
        created.version,
        crate::intent::stable_write_target_version(&node_id, &created.object_id, &operation_id,),
        "stable create must publish its deterministic target version",
    );
    let reopened = restarted.posix_stat(path).expect("reopen created file");
    assert_eq!(reopened.object_id, created.object_id);
    assert_eq!(reopened.version, created.version);
    let durable_object = restarted
        .metadata_backend()
        .expect("backend")
        .get_object(&created.object_id)
        .expect("get durable object")
        .expect("durable object");
    let expected_event = EventId::derived(
        "sinomo-dfs.routed-exclusive-create.v1",
        format!(
            "{}\0{}\0{}\0{:?}\0{}\0{}\0{}",
            operation_id,
            durable_object.object_id,
            durable_object.path,
            durable_object.kind,
            durable_object.mode,
            durable_object.uid,
            durable_object.gid,
        )
        .as_bytes(),
    );
    assert_eq!(
        durable_object.last_modified_by_event_id.as_ref(),
        Some(&expected_event),
        "stable exclusive create must retain deterministic replay provenance",
    );
    let replayed = restarted
        .posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"captured once".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("reconcile dropped create response after restart");
    assert_eq!(replayed.object_id, created.object_id);
    assert_eq!(replayed.version, created.version);
    assert_eq!(
        restarted.posix_read_file(path).expect("replayed bytes"),
        b"captured once"
    );

    assert!(matches!(
        restarted.posix_create_file_from_reader_with_operation_id(
            path,
            std::io::Cursor::new(b"different content".to_vec()),
            options,
            &operation_id,
        ),
        Err(DfsPosixError::Conflict { .. }) | Err(DfsPosixError::Dfs(_))
    ));
    assert_eq!(
        restarted
            .posix_read_file(path)
            .expect("original bytes remain"),
        b"captured once"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stable_create_reconciliation_routes_from_a_restarted_non_replica_after_authority_moves() {
    let root = temp_root("stable-create-reconcile-authority-move");
    let old_primary_node =
        NodeId::from_legacy_named("node_stable_reconcile_old_primary").expect("old primary");
    let new_primary_node =
        NodeId::from_legacy_named("node_stable_reconcile_new_primary").expect("new primary");
    let requester_node =
        NodeId::from_legacy_named("node_stable_reconcile_requester").expect("requester");
    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = |primary: &NodeId, replicas: Vec<NodeId>, epoch| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: replicas,
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                old_primary_node.clone(),
            )))
            .expect("filesystem config");
        control
            .put_metadata_shard(&shard(&old_primary_node, vec![new_primary_node.clone()], 1))
            .expect("initial shard");
    });
    let mut old_primary = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "old-primary",
        old_primary_node.clone(),
    );
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "new-primary",
        new_primary_node.clone(),
    );
    let mut requester = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "requester",
        requester_node.clone(),
    );

    // Bootstrap is a replicated namespace mutation, so keep the future
    // authority online while the original primary publishes the root shard.
    new_primary = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let pump_stop = Arc::clone(&stop);
        let pump = scope.spawn(move || pump_sifs_service_until_stop(new_primary, pump_stop));
        old_primary
            .bootstrap_distributed_sifs()
            .expect("initial primary bootstrap");
        stop.store(true, Ordering::SeqCst);
        pump.join().expect("bootstrap replica pump")
    });

    let path = "/users/stable-reconcile-after-move.txt";
    let bytes = b"durable stable reconciliation".to_vec();
    let expected_hash = content_hash(&bytes);
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let options = DfsWriteOptions::replicated_eventual(vec![requester_node.clone()]);
    let (old_after_create, new_after_create, created) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let old_stop = Arc::clone(&stop);
        let new_stop = Arc::clone(&stop);
        let old_pump = scope.spawn(move || pump_sifs_service_until_stop(old_primary, old_stop));
        let new_pump = scope.spawn(move || pump_sifs_service_until_stop(new_primary, new_stop));
        let parent = requester.posix_mkdir_all("/users");
        let created = parent.and_then(|_| {
            requester.posix_create_file_from_reader_with_operation_id(
                path,
                std::io::Cursor::new(bytes),
                options,
                &operation_id,
            )
        });
        stop.store(true, Ordering::SeqCst);
        (
            old_pump.join().expect("old primary pump"),
            new_pump.join().expect("new primary pump"),
            created,
        )
    });
    let created = created.expect("routed stable create");
    old_primary = old_after_create;
    new_primary = new_after_create;

    let stable_intent_id =
        crate::intent::stable_write_intent_id(&requester_node, &created.object_id, &operation_id);
    let replication_deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        old_primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("publish terminal stable receipt");
        new_primary
            .process_incoming()
            .expect("future authority receives terminal receipt");
        old_primary
            .process_incoming()
            .expect("original authority receives replica acknowledgement");
        let replicated_status = new_primary
            .metadata_backend()
            .expect("replica metadata")
            .get_intent(&created.object_id, &stable_intent_id)
            .expect("replicated intent lookup")
            .map(|record| record.status);
        if matches!(replicated_status, Some(DfsWriteIntentStatus::Committed)) {
            break;
        }
        assert!(
            Instant::now() < replication_deadline,
            "future authority did not receive the terminal stable receipt: {replicated_status:?}",
        );
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    assert!(
        new_primary
            .metadata_backend()
            .expect("replica metadata")
            .get_intent(&created.object_id, &stable_intent_id)
            .expect("replicated intent lookup")
            .is_some(),
        "the future authority must durably replicate the exact operation receipt",
    );

    // Promote the replica, then restart both it and the non-replica caller.
    // The caller deliberately reopens with an empty metadata root, proving the
    // answer is routed to the new authority instead of inferred locally.
    shared_kv.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard(&new_primary_node, vec![old_primary_node.clone()], 2))
            .expect("promote replicated authority");
    });
    drop(old_primary);
    drop(new_primary);
    drop(requester);
    let restarted_primary = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "new-primary",
        new_primary_node.clone(),
    );
    let mut restarted_requester = sifs_posix_cluster_node(
        &network,
        &shared_kv,
        &root,
        "requester-without-local-history",
        requester_node,
    );
    assert!(
        restarted_requester
            .metadata_backend()
            .expect("requester metadata")
            .get_intent(&created.object_id, &stable_intent_id)
            .expect("local receipt lookup")
            .is_none(),
        "the restarted non-replica fixture must not have local receipt evidence",
    );

    let (reconciliation, restarted_primary) = std::thread::scope(|scope| {
        let stop = Arc::new(AtomicBool::new(false));
        let pump_stop = Arc::clone(&stop);
        let pump = scope.spawn(move || pump_sifs_service_until_stop(restarted_primary, pump_stop));
        let reconciliation = restarted_requester.reconcile_stable_create_file_operation(
            path,
            &operation_id,
            &expected_hash,
            created.size_bytes.expect("created size"),
        );
        stop.store(true, Ordering::SeqCst);
        (
            reconciliation,
            pump.join().expect("promoted authority pump"),
        )
    });
    let reconciliation = reconciliation.expect("routed reconciliation");
    let DfsStableWriteOperationReconciliation::Applied { receipt } = reconciliation else {
        panic!("remote durable receipt must prove Applied: {reconciliation:?}");
    };
    assert_eq!(receipt.operation_id, operation_id);
    assert_eq!(receipt.object_id, created.object_id);
    assert_eq!(
        receipt.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(receipt.content_size_bytes, created.size_bytes);

    drop(restarted_primary);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reader_backed_stable_cas_replays_but_rejects_a_recreated_path() {
    let root = temp_root("reader-stable-cas-recreated");
    let node_id = NodeId::from_legacy_named("node_reader_stable_cas").expect("node");
    let path = "/documents/reader-cas.txt";
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let (opened, saved) = {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/documents").expect("mkdir");
        let opened = dfs
            .posix_write_file(path, b"opened", options.clone())
            .expect("seed file");
        let saved = dfs
            .posix_replace_file_if_version_from_reader_with_operation_id(
                path,
                &opened.object_id,
                &opened.version,
                std::io::Cursor::new(b"saved by reader".to_vec()),
                options.clone(),
                &operation_id,
            )
            .expect("reader CAS");
        (opened, saved)
    };

    let mut restarted = networked_sifs_with_backend(&root, node_id.clone());
    let replayed = restarted
        .posix_replace_file_if_version_from_reader_with_operation_id(
            path,
            &opened.object_id,
            &opened.version,
            std::io::Cursor::new(b"saved by reader".to_vec()),
            options.clone(),
            &operation_id,
        )
        .expect("reconcile dropped CAS response after restart");
    assert_eq!(replayed.version, saved.version);

    restarted.posix_unlink(path).expect("unlink old identity");
    let recreated = restarted
        .posix_write_file(path, b"replacement identity", options.clone())
        .expect("recreate path");
    assert_ne!(recreated.object_id, opened.object_id);
    assert!(matches!(
        restarted.posix_replace_file_if_version_from_reader_with_operation_id(
            path,
            &opened.object_id,
            &opened.version,
            std::io::Cursor::new(b"must not overwrite replacement".to_vec()),
            options,
            &operation_id,
        ),
        Err(DfsPosixError::Conflict { .. })
    ));
    assert_eq!(
        restarted
            .posix_read_file(path)
            .expect("replacement remains"),
        b"replacement identity"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_namespace_job_maintenance_marks_invalid_job_failed() {
    let root = temp_root("sifs-namespace-job-maintenance-failed");
    let node_id = NodeId::from_legacy_named("node_sifs_namespace_job_maintenance").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id);
    let now = time::OffsetDateTime::now_utc();
    let job = DfsNamespaceJobRecord {
        job_id: "invalid-clear-job".to_string(),
        file_system_id: sifs_file_system_id(),
        origin: None,
        kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
        status: DfsNamespaceJobStatus::Running,
        root_path: "/services/sandbox/instances/invalid-clear/work".to_string(),
        target_path: None,
        pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryStart {
            source_object_id: DfsObjectId::generate(),
        }],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    };
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_namespace_job(&job)
        .expect("put invalid job");

    let report = dfs
        .posix_resume_namespace_jobs_bounded(4, 4)
        .expect("namespace job maintenance");

    assert_eq!(report.scanned_job_count, 1);
    assert_eq!(report.resumed_job_count, 1);
    assert_eq!(report.failed_job_count, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].job_id, job.job_id);
    let failed = dfs
        .metadata_backend
        .as_ref()
        .expect("backend")
        .get_namespace_job(&job.job_id)
        .expect("get failed job")
        .expect("failed job");
    assert_eq!(failed.status, DfsNamespaceJobStatus::Failed);
    assert!(failed.failure_reason.as_deref().is_some_and(|reason| {
        reason.contains("invalid DFS path") || reason.contains("invalid-clear-job")
    }));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_recreate_directory_uses_a_new_namespace_transaction_generation() {
    let root = temp_root("sifs-posix-recreate-dir-transaction-generation");
    let node_id = NodeId::from_legacy_named("node_sifs_recreate_dir_transaction").expect("node");
    let parent_path = "/services/sandbox/instances/sandbox_recreate_dir_transaction/work";
    let dir_path = "/services/sandbox/instances/sandbox_recreate_dir_transaction/work/recreated";

    let mut dfs = networked_sifs_with_backend(&root, node_id);
    dfs.posix_mkdir_all(parent_path).expect("mkdir parent");
    let original = dfs.posix_mkdir(dir_path).expect("initial mkdir");
    dfs.posix_rmdir(dir_path).expect("rmdir");
    let recreated = dfs.posix_mkdir(dir_path).expect("recreate dir");

    assert_eq!(recreated.object_id, original.object_id);
    assert_ne!(recreated.version, original.version);
    assert_eq!(dfs.posix_stat(dir_path).expect("recreated stat"), recreated);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn durable_pending_namespace_request_recovers_automatically_after_restart() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-pending-restart");
    let SifsTwoPrimaryCluster {
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        mut services_primary,
        replica,
        ..
    } = cluster;
    let source = "/services/pending-source.txt";
    let destination = "/services/pending-destination.txt";
    let primary_node = services_primary.node_id().clone();
    services_primary
        .posix_write_file(
            source,
            b"pending request payload",
            DfsWriteOptions::replicated_eventual(vec![primary_node]),
        )
        .expect("seed pending source");

    let command = DfsNamespaceCommand {
        request_id: "dfs_net_req_pending_restart".to_string(),
        file_system_id: sifs_file_system_id(),
        requesting_node_id: replica_node.clone(),
        include_metadata_journal_batches: true,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::Rename {
            from: source.to_string(),
            to: destination.to_string(),
        },
    };
    let encoded = serde_json::to_vec(&(
        &command.file_system_id,
        &command.requesting_node_id,
        &command.operation,
    ))
    .expect("encode command digest");
    let now = OffsetDateTime::now_utc();
    let pending = DfsNamespaceRequestRecord {
        file_system_id: command.file_system_id.clone(),
        requester_node_id: replica_node.clone(),
        request_id: command.request_id.clone(),
        operation_id: "req-pending-restart".to_string(),
        authority_shard_id: DfsMetadataShardId::new("services"),
        operation: command.operation.clone(),
        command_digest: format!("blake3:{}", blake3::hash(&encoded).to_hex()),
        status: DfsNamespaceRequestStatus::Pending,
        result: None,
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: now,
        updated_at: now,
    };
    services_primary
        .metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_namespace_request_on_shard(&DfsMetadataShardId::new("services"), &pending)
        .expect("persist pending request");

    let restarted_transport = services_primary.transport.clone();
    drop(services_primary);
    let restarted_shards = sifs_two_primary_shard_configs(
        &users_primary_node,
        &services_primary_node,
        2,
        vec![services_primary_node.clone(), replica_node.clone()],
        vec![users_primary_node.clone(), replica_node.clone()],
    );
    let mut services_primary = NetworkedDfsService::new(
        restarted_transport,
        LocalContentStore::new(root.join("services-primary").join("content"))
            .expect("restarted services content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(
            root.join("services-primary").join("metadata"),
            restarted_shards,
        )
        .expect("reopen services metadata"),
    )
    .expect("attach restarted services metadata");

    assert!(matches!(
        services_primary.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(services_primary.posix_stat(destination).is_ok());
    let recovered = services_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_request(&replica_node, &command.request_id)
        .expect("recovered request")
        .expect("durable request");
    assert_eq!(recovered.status, DfsNamespaceRequestStatus::Completed);
    assert_eq!(recovered.result, Some(DfsNamespaceResult::Applied));

    let event = services_primary
        .handle_namespace_request(replica_node, command)
        .expect("replay automatically recovered request");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            mutates_metadata: false,
            ..
        }
    ));

    drop(replica);
    std::fs::remove_dir_all(root).ok();
}
