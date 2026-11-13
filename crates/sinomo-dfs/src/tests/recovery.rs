//! Recovery regression tests.

use super::*;

#[test]
fn dfs_request_ids_remain_unique_across_service_restarts() {
    let node = NodeId::from_legacy_named("node_request_id_restart").expect("node");
    let first_root = temp_root("request-id-first");
    let restarted_root = temp_root("request-id-restarted");
    let mut first = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(&first_root).expect("first content"),
    );
    let mut restarted = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
        LocalContentStore::new(&restarted_root).expect("restarted content"),
    );

    let first_id = first.next_request_id();
    let restarted_id = restarted.next_request_id();
    assert!(first_id.starts_with("dfs_net_req_1_"), "{first_id}");
    assert!(restarted_id.starts_with("dfs_net_req_1_"), "{restarted_id}");
    assert_ne!(first_id, restarted_id);
    std::fs::remove_dir_all(first_root).ok();
    std::fs::remove_dir_all(restarted_root).ok();
}

#[test]
fn lifecycle_maintenance_host_applies_bounded_tombstone_compaction_options() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-bounded-tombstone-maintenance",
        1,
    );
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "native-bounded-tombstone-maintenance",
        nodes[0].clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    // Tombstone compaction is Main-side store maintenance: run it on the
    // store-backed opener, not a caller delegating to its Main.
    let mut dfs = runtime.metadata_primary_service("/").unwrap();
    let mut host = RecordingMaintenanceHost::default();
    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions {
            compact_tombstones: true,
            tombstone_compaction_min_age: Duration::from_nanos(17),
            tombstone_compaction_max_records: 23,
            ..Default::default()
        },
    )
    .unwrap();
    let report = reports
        .iter()
        .find_map(|report| report.tombstone_compaction.as_ref())
        .expect("native bounded collection report");
    assert_eq!(report.min_tombstone_age_nanos, 17);
    assert_eq!(report.max_compactions, Some(23));
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn metadata_prefix_probe_accepts_only_the_exact_materialized_checkpoint() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-exact-prefix-probe", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-exact-prefix-probe", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let source = runtime.root_shard().unwrap();
    let target = source
        .replica_node_ids
        .iter()
        .find(|node| **node != source.primary_node_id)
        .unwrap()
        .clone();
    let shard_id = DfsMetadataShardId::new(source.shard_id.clone());
    let cut = runtime
        .node_metadata(&target)
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    let mut command = DfsMetadataJournalBatchCommand {
        request_id: "native-exact-prefix".into(),
        source_node_id: Some(source.primary_node_id.clone()),
        file_system_id: Some(source.file_system_id.clone()),
        authority_epoch: Some(source.epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id: "native-prefix".into(),
            file_system_id: Some(source.file_system_id.clone()),
            authority_epoch: Some(source.epoch),
            shard_id: shard_id.clone(),
            journal_sequence: cut.prefix_proof.journal_sequence,
            state_fingerprint: cut.prefix_proof.digest.clone(),
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            journal_frames: Vec::new(),
            next_after_sequence: cut.prefix_proof.journal_sequence,
        },
    };
    let request = dfs_metadata_journal_batch_rpc_request(&command).unwrap();
    let reply = runtime
        .rpc_request(
            &source.primary_node_id,
            &target,
            request,
            Duration::from_secs(5),
        )
        .unwrap();
    let response = decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    command.prefix_probe.as_mut().unwrap().state_fingerprint="dfs-metadata-prefix-v2:blake3:0000000000000000000000000000000000000000000000000000000000000000".into();
    let request = dfs_metadata_journal_batch_rpc_request(&command).unwrap();
    let reply = runtime
        .rpc_request(
            &source.primary_node_id,
            &target,
            request,
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(matches!(
        decode_dfs_metadata_journal_batch_rpc_response(&reply)
            .unwrap()
            .result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    let after = runtime
        .node_metadata(&target)
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, cut.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        cut.semantic_state_fingerprint
    );
    runtime.close().unwrap();
}

#[test]
fn recovery_hydration_can_cancel_between_synchronous_backend_phases() {
    let (runtime, client, _) = crate::test_support::current_native_metadata_service(
        "native-recovery-cancellation-between-phases",
        1,
        "/services",
    )
    .unwrap();
    drop(client);
    // Recovery hydration is Main-side store work on the store-backed opener.
    let mut service = runtime.metadata_primary_service("/services").unwrap();
    let cancellation_checks = AtomicUsize::new(0);
    let before = service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let completed = service
        .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(&|| {
            cancellation_checks.fetch_add(1, Ordering::SeqCst) >= 1
        })
        .unwrap();
    assert!(!completed);
    assert!(
        cancellation_checks.load(Ordering::SeqCst) >= 2,
        "the normal recovery driver must recheck cancellation after synchronous native backend work"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn recovery_hydration_skips_fully_repaired_committed_pending_write() {
    let name = "native-fully-receipted-commit-recovery";
    let (mut runtime, mut owner_dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::LoseReplyAfterDispatch,
    });
    let path = "/services/fully-receipted-recovery";
    let result = owner_dfs.posix_write_file(
        path,
        b"fully repaired bytes",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).unwrap(),
            replica_candidates: vec![owner.clone(), replica.clone()],
            wait_for_commit: true,
            foreground_replica_repair: true,
        },
    );
    // Receiver execution is real. An uncertain sender response cannot undo the
    // already committed metadata/content that normal reconciliation observes.
    let stat = owner_dfs.posix_stat_distributed(path).unwrap();
    let receipts = runtime
        .node_metadata(&owner)
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.content_hash.as_deref() == stat.content_hash.as_deref())
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        2,
        "W=2 must come from the actual two receivers, result={result:?}"
    );
    assert!(owner_dfs.writes.pending_writes.is_empty());
    assert!(owner_dfs.coordinator().repair_jobs().is_empty());
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse),
        "the lost response must follow actual native receiver execution"
    );
    let peer = runtime.client(&replica).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        b"fully repaired bytes"
    );
    drop(peer);
    drop(owner_dfs);
    runtime.clear_faults();
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    // Commit-promotion recovery is Main-side store work on the store-backed
    // opener; a caller delegating to its Main has nothing to hydrate.
    let mut recovered = runtime.metadata_primary_service(path).unwrap();
    let before = runtime.rpc_observations().len();
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    assert!(recovered.writes.pending_writes.is_empty());
    assert!(recovered.coordinator().repair_jobs().is_empty());
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let after_replay = runtime.rpc_observations().len();
    let replay_count = runtime.rpc_observations()[before..after_replay]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
        })
        .count();
    assert!(
        replay_count <= 1,
        "one exact current commit may be replayed, never a fabricated ACK"
    );
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    assert!(
        !runtime.rpc_observations()[after_replay..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "settled exact native commit must not be requeued by a later hydration"
    );
    assert_eq!(
        recovered.posix_read_file(path).unwrap(),
        b"fully repaired bytes"
    );
    drop(recovered);
    runtime.close().unwrap();
}

#[test]
fn hydrate_recovers_zero_byte_create_after_missing_native_prepare_ack_once() {
    let name = "native-zero-create-floor-return-failure";
    let (mut runtime, mut original, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    let parent_path = "/services/native-floor-recovery";
    let child_path = "/services/native-floor-recovery/empty.bin";
    drop(original);
    let mut original = runtime.metadata_primary_service(child_path).unwrap();
    original.posix_mkdir_all(parent_path).unwrap();
    let parent_before = original.posix_stat(parent_path).unwrap();
    let parent_versions_before = original
        .metadata_backend()
        .unwrap()
        .versions_for_object(&parent_before.object_id)
        .unwrap()
        .len();
    let missing_ack = config.replica_node_ids[0].clone();
    let missing_ack_fault = crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(missing_ack),
        route: None,
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    };
    let fault = runtime.metadata_operation_floor_fault(&owner).unwrap();
    let trigger = fault.operation_floor_denial_trigger(
        crate::control_plane::metadata_shard_operation_floor_key(
            &config.file_system_id,
            &config.shard_id,
        ),
    );
    let runtime = std::rc::Rc::new(std::cell::RefCell::new(runtime));
    let runtime_in_hook = std::rc::Rc::clone(&runtime);
    let reached_prepare = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_prepare = Arc::clone(&reached_prepare);
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_| {
            observed_prepare.store(true, Ordering::SeqCst);
            trigger();
            runtime_in_hook.borrow().install_fault(missing_ack_fault);
        }));
    });
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let result = original.posix_create_file_with_contents_and_attrs_and_operation_id(
        child_path,
        &[],
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        DfsPosixCreateAttrs::new(0o640, 6101, 6102),
        &operation_id,
    );
    // Clear an unconsumed negative hook on every outcome before reclaiming the
    // fixture; an earlier failure must not retain its native owner accidentally.
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        hook.borrow_mut().take();
    });
    let mut runtime = std::rc::Rc::try_unwrap(runtime)
        .ok()
        .expect("prepare hook released fixture")
        .into_inner();
    assert!(reached_prepare.load(Ordering::SeqCst));
    let error = result.unwrap_err();
    assert!(
        matches!(
            &error,
            DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
                cause: crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
                    required: 3,
                    acknowledged: 2,
                    ..
                },
                ..
            })
        ),
        "the actual missing native participant must prevent publication: {error:?}"
    );
    assert_eq!(
        fault.denied_operation_floor_reads(),
        0,
        "ordinary full-participant preparation must not fall back to a KV operation floor"
    );
    let backend = original.metadata_backend().unwrap();
    let prepared = backend
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record.status == DfsPreparedOperationStatus::Prepared
                && record
                    .transaction
                    .objects
                    .iter()
                    .any(|object| object.path == child_path)
        })
        .unwrap();
    let pending = prepared.prepared_intent.as_ref().unwrap();
    assert!(pending.touch_parent_on_create);
    assert_eq!(
        pending.content_hash.as_deref(),
        Some(content_hash(&[]).as_str())
    );
    assert_eq!(
        prepared.content_receipts.len(),
        1,
        "the actual empty bytes already meet W"
    );
    assert_eq!(prepared.content_receipts[0].replica_node_id, owner);
    assert!(
        backend
            .get_object_by_path(&sifs_file_system_id(), child_path)
            .unwrap()
            .is_none()
    );
    assert_eq!(original.posix_stat(parent_path).unwrap(), parent_before);
    let expected_child = prepared
        .transaction
        .objects
        .iter()
        .find(|object| object.path == child_path)
        .unwrap()
        .clone();
    drop(original);
    drop(fault);
    runtime.clear_faults();
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let mut recovered = runtime.metadata_primary_service(child_path).unwrap();
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let child = recovered.posix_stat(child_path).unwrap();
    assert_eq!(child.object_id, expected_child.object_id);
    assert_eq!(child.version, expected_child.version);
    assert_eq!(child.size_bytes, Some(0));
    assert_eq!((child.mode, child.uid, child.gid), (0o640, 6101, 6102));
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_write_count,
        0
    );
    assert!(recovered.writes.pending_writes.is_empty());
    let parent_after = recovered.posix_stat(parent_path).unwrap();
    assert_ne!(parent_after.version, parent_before.version);
    assert!(parent_after.modified_at_unix_nanos > parent_before.modified_at_unix_nanos);
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .versions_for_object(&parent_before.object_id)
            .unwrap()
            .len(),
        parent_versions_before + 1
    );
    drop(recovered);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reopened = runtime.metadata_primary_service(child_path).unwrap();
    assert_eq!(reopened.posix_stat(child_path).unwrap(), child);
    assert_eq!(reopened.posix_stat(parent_path).unwrap(), parent_after);
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_write_count,
        0
    );
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_survives_metadata_backend_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_recursive_clear_survives_metadata_backend_reopen",
        node_id.clone(),
    );
    drop(setup);
    let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
    let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

    {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all(clear_root).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"tmp",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write");
        assert_eq!(
            dfs.posix_read_file_distributed(file_path).expect("read"),
            b"tmp"
        );
        dfs.posix_clear_dir_recursive(clear_root)
            .expect("clear recursive");
        assert!(matches!(
            dfs.posix_read_file_distributed(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        assert!(matches!(
            dfs.posix_read_file_distributed(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all(clear_root)
            .expect("recreate cleared root");
        dfs.posix_write_file(
            file_path,
            b"fresh",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("rewrite after clear");
        assert_eq!(
            dfs.posix_read_file_distributed(file_path)
                .expect("fresh read"),
            b"fresh"
        );
    }

    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_bounded_job_resumes_after_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear_job").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_recursive_clear_bounded_job_resumes_after_reopen",
        node_id.clone(),
    );
    drop(setup);
    let clear_root = "/services/sandbox/instances/sandbox_clear_job/tmp/0";
    let nested_dir = format!("{clear_root}/nested/deeper");
    let files = [
        format!("{clear_root}/root-cache.txt"),
        format!("{clear_root}/nested/cache-a.txt"),
        format!("{nested_dir}/cache-b.txt"),
        format!("{nested_dir}/cache-c.txt"),
    ];
    let job_id = {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all(&nested_dir).expect("mkdir nested");
        for path in &files {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write file");
        }
        drop(dfs);
        // Starting a bounded clear job is Main-side store work (callers use
        // the routed unbounded clear); run it on the store-backed opener.
        let mut dfs = runtime.metadata_primary_service(clear_root).unwrap();

        let job = dfs
            .posix_clear_dir_recursive_bounded(clear_root, 1)
            .expect("first bounded clear batch");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(
            !job.pending_frames.is_empty(),
            "a one-step batch should persist remaining directory work"
        );
        assert!(
            dfs.metadata_backend
                .as_ref()
                .expect("backend")
                .get_namespace_job(&job.job_id)
                .expect("get persisted job")
                .is_some(),
            "bounded recursive clear progress should be durable"
        );
        job.job_id
    };

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        let mut job = dfs
            .posix_resume_namespace_job_bounded(&job_id, 2)
            .expect("resume bounded clear after reopen");
        while job.status != DfsNamespaceJobStatus::Completed {
            job = dfs
                .posix_resume_namespace_job_bounded(&job_id, 2)
                .expect("resume next batch");
        }
        assert!(job.processed_object_count >= files.len() as u64);
        for path in &files {
            assert!(matches!(
                dfs.posix_read_file_distributed(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        assert!(
            dfs.posix_list_dir_distributed(clear_root)
                .expect("cleared root list")
                .is_empty()
        );
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        let job = runtime
            .node_metadata(&node_id)
            .expect("node metadata store")
            .get_namespace_job(&job_id)
            .expect("get completed job")
            .expect("job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        for path in &files {
            assert!(matches!(
                dfs.posix_read_file_distributed(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
    }

    runtime.close().unwrap();
}

#[test]
fn sifs_posix_rename_directory_bounded_job_resumes_after_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_job").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_posix_rename_directory_bounded_job_resumes_after_reopen",
        node_id.clone(),
    );
    drop(setup);
    let old_dir = "/services/sandbox/instances/sandbox_rename_job/work/old";
    let new_dir = "/services/sandbox/instances/sandbox_rename_job/work/new";
    let old_nested = format!("{old_dir}/nested/deeper");
    let old_files = [
        format!("{old_dir}/root.txt"),
        format!("{old_dir}/nested/a.txt"),
        format!("{old_nested}/b.txt"),
        format!("{old_nested}/c.txt"),
    ];
    let new_files = [
        format!("{new_dir}/root.txt"),
        format!("{new_dir}/nested/a.txt"),
        format!("{new_dir}/nested/deeper/b.txt"),
        format!("{new_dir}/nested/deeper/c.txt"),
    ];
    let job_id = {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all(&old_nested).expect("mkdir old tree");
        for path in &old_files {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write old file");
        }

        let job = dfs
            .posix_rename_directory_bounded_routed(old_dir, new_dir, 1)
            .expect("first bounded rename batch");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(
            !job.pending_frames.is_empty(),
            "a one-step directory rename should persist remaining subtree work"
        );
        assert!(matches!(
            dfs.posix_stat_distributed(old_dir),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_stat_distributed(new_dir)
                .expect("new root after first batch")
                .kind,
            DfsObjectKind::Directory
        );
        job.job_id
    };

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        let mut job = dfs
            .posix_resume_namespace_job_bounded_routed(&job_id, old_dir, 2)
            .expect("resume bounded rename after reopen");
        while job.status != DfsNamespaceJobStatus::Completed {
            job = dfs
                .posix_resume_namespace_job_bounded_routed(&job_id, old_dir, 2)
                .expect("resume next rename batch");
        }
        assert!(job.processed_object_count >= old_files.len() as u64);
        for path in &old_files {
            assert!(matches!(
                dfs.posix_read_file_distributed(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        for (old_path, new_path) in old_files.iter().zip(new_files.iter()) {
            assert_eq!(
                dfs.posix_read_file_distributed(new_path)
                    .expect("new file content"),
                old_path.as_bytes()
            );
        }
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.metadata_primary_service(new_dir).unwrap();
        let job = dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_namespace_job(&job_id)
            .expect("get completed rename job")
            .expect("job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        for new_path in &new_files {
            assert_eq!(
                dfs.posix_stat_distributed(new_path)
                    .expect("reloaded moved file")
                    .kind,
                DfsObjectKind::File
            );
        }
    }

    runtime.close().unwrap();
}

#[test]
fn sifs_posix_unlink_tombstone_survives_fresh_shared_metadata_recovery() {
    let root = temp_root("sifs-unlink-shared-metadata-recovery");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_unlink_shared_metadata").expect("node");
    let path = "/services/shared-unlink-recovery.txt";

    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "sifs_posix_unlink_tombstone_survives_fresh_shared_metadata_recovery",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut seed = runtime.client(&node).unwrap();
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_write_file(
        path,
        b"delete me",
        DfsWriteOptions::replicated_eventual(vec![node.clone()]),
    )
    .expect("create file");

    let mut remover = runtime.client(&node).unwrap();
    let live_changed_at = remover
        .posix_lstat_distributed(path)
        .expect("live file stat")
        .changed_at_unix_nanos;
    let removed = remover.posix_unlink(path).expect("unlink file");
    assert!(
        removed.changed_at_unix_nanos > live_changed_at,
        "unlink tombstone ctime must fence historical namespace transactions"
    );

    let mut verifier = runtime.client(&node).unwrap();
    assert!(matches!(
        verifier.posix_stat_distributed(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        runtime
            .node_metadata(&node)
            .expect("node metadata store")
            .get_object_by_path(&sifs_file_system_id(), path)
            .expect("durable path lookup")
            .expect("durable tombstone")
            .kind,
        DfsObjectKind::Tombstone
    );
}

#[test]
fn sifs_cluster_writer_recovers_from_local_blob_loss_after_async_repair() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-writer-blob-loss", "/services");
    let path = "/services/async-repaired-writer-loss.bin";
    let bytes = (0..64 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_hash = content_hash(&bytes);
    let required = vec![cluster.writer_node.clone(), cluster.primary_node.clone()];
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = required.clone();
    let accepted = cluster
        ._runtime
        .apply_placement_policy(
            &cluster.primary_node,
            path,
            DfsPlacementScope::SelfOnly,
            placement,
        )
        .unwrap();
    let control = cluster._runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal empty Apply setup budget");
        cluster
            ._runtime
            .maintenance_tick(&cluster.primary_node, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    cluster
        ._runtime
        .wait_published_configuration_current(&cluster.writer_node)
        .unwrap();
    let (_, _, policy) = cluster.writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!((policy.min_commit_replicas, policy.desired_replicas), (1, 2));
    assert_eq!(
        policy.required_node_ids.iter().cloned().collect::<BTreeSet<_>>(),
        required.into_iter().collect::<BTreeSet<_>>()
    );

    match cluster.writer.posix_write_file(
        path,
        &bytes,
        DfsWriteOptions {
            policy,
            replica_candidates: vec![cluster.writer_node.clone(), cluster.primary_node.clone()],
            wait_for_commit: false,
            foreground_replica_repair: false,
        },
    ) {
        Ok(stat) => assert_eq!(
            stat.content_hash, None,
            "a pending remote-primary write has no content identity until primary ingest completes"
        ),
        Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("async routed write failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "writer observes async write", |cluster| {
        cluster
            .writer
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    sifs_posix_cluster_pump_until(&mut cluster, "primary async repair", |cluster| {
        cluster._runtime.maintenance_tick(
            &cluster.primary_node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        ).unwrap().outcome.unwrap();
        cluster
            .primary
            .content()
            .read_blob(&expected_hash)
            .is_ok_and(|read| read == bytes)
    });
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "writer learns repaired primary location",
        |cluster| {
            cluster.writer.posix_stat_distributed(path).is_ok_and(|stat| {
                stat.current_locations.contains(&cluster.writer_node)
                    && stat.current_locations.contains(&cluster.primary_node)
            })
        },
    );

    cluster
        .writer
        .content
        .debug_remove_loose_blob(&expected_hash)
        .expect("remove writer loose blob");
    match cluster.writer.content().read_blob(&expected_hash).unwrap_err() {
        crate::ContentStoreError::MissingLooseBlob { hash } => assert_eq!(hash, expected_hash),
        crate::ContentStoreError::Io { source, .. } => {
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        error => panic!("fault injection must remove the writer's local readable content: {error}"),
    }

    let recovered =
        sifs_cluster_fetch_for_local_read(&mut cluster, |cluster| &mut cluster.writer, path);
    assert_eq!(recovered, bytes);
    assert!(
        cluster.rejections.is_empty(),
        "journal replication should remain clean: {:?}",
        cluster.rejections
    );

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_write_survives_writer_restart() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-routed-writer-restart", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-routed-writer-restart", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/users/native-routed-restart.txt";
    let mut writer = runtime.client(&nodes[1]).unwrap();
    let stat = writer
        .posix_write_file(
            path,
            b"durable routed bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    writer
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(writer);
    runtime.stop_node(&nodes[1]).unwrap();
    runtime.reopen_node(nodes[1].clone()).unwrap();
    let mut recovered = runtime.client(&nodes[1]).unwrap();
    assert_eq!(
        recovered.posix_stat_distributed(path).unwrap().version,
        stat.version
    );
    assert_eq!(
        recovered.posix_read_file_distributed(path).unwrap(),
        b"durable routed bytes"
    );
    drop(recovered);
    runtime.close().unwrap();
}

#[test]
fn recovered_stage_response_committed_work_tracks_remaining_repairs_only() {
    for (case, include_missing_replica, expect_repair_maintenance) in
        [("remaining", true, true), ("complete", false, false)]
    {
        let name = format!("native-recovered-stage-work-{case}");
        let path = format!("/services/recovered-work-{case}.bin");
        let desired = if include_missing_replica { 3 } else { 2 };
        let (runtime, primary, nodes, mut original, policy) =
            super::repair::native_owned_repair_fixture(&name, &path, 2, desired, false);
        let peers = nodes
            .iter()
            .filter(|node| **node != primary)
            .cloned()
            .collect::<Vec<_>>();
        let replica = peers[0].clone();
        let background = runtime
            .deny_background_replica_before_claim(&primary)
            .unwrap();
        let base = original
            .posix_write_file(
                &path,
                b"actual sealed recovery predecessor",
                DfsWriteOptions {
                    policy: policy.clone(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        while original
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&base.object_id, &base.version)
            .unwrap()
            .len()
            != desired
        {
            assert!(
                Instant::now() < setup_deadline,
                "native predecessor desired copies must settle"
            );
            drive_repair_executor(&runtime, &mut original);
        }
        original.flush_detached_transport_sends().unwrap();
        if let Some(missing) = peers.get(1) {
            for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
                runtime.install_fault(crate::test_runtime::FaultRule {
                    source: Some(primary.clone()),
                    target: Some(missing.clone()),
                    route: Some(route.into()),
                    call_id: None,
                    origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
                    remaining_hits: 32,
                    action: crate::test_runtime::FaultAction::BudgetExhausted,
                });
            }
        }
        let bytes = format!("recovered stage committed work {case}").into_bytes();
        let hash = content_hash(&bytes);
        let request = DfsWriteRequest {
            file_system_id: base.file_system_id,
            object_id: base.object_id,
            target_version: crate::namespace::next_version(&base.version),
            requesting_node_id: primary.clone(),
        };
        let outcome = original
            .write_distributed_reader_with_candidates(
                request.clone(),
                std::io::Cursor::new(&bytes),
                nodes.clone(),
                policy,
                DfsDistributedWriteOptions::default(),
            )
            .unwrap();
        let NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts: 1,
            required_receipts: 2,
        } = outcome
        else {
            panic!("actual pre-recovery W2 pending write required: {outcome:?}");
        };
        let old = original
            .transport
            .drain_incoming_messages()
            .unwrap()
            .into_iter()
            .find(|routed| {
                matches!(&routed.message, DfsNetworkMessage::StageBlobResponse(reply)
                if reply.intent_id == intent_id && reply.replica_node_id == replica)
            })
            .unwrap();
        let DfsNetworkMessage::StageBlobResponse(old_ack) = &old.message else {
            unreachable!()
        };
        let old_request_id = old_ack.request_id.clone();
        original
            .ensure_stage_response_matches_pending(old_ack, &hash)
            .unwrap();
        drop(original);
        let mut recovered = runtime.metadata_primary_service(&path).unwrap();
        recovered.hydrate_from_metadata_backend().unwrap();
        assert!(
            recovered
                .writes
                .pending_writes
                .get(&intent_id)
                .is_some_and(|pending| pending.stage_inflight_request_ids.is_empty()),
            "recovery cannot inherit ephemeral Stage tracking"
        );
        assert!(
            recovered
                .ensure_stage_response_matches_pending(old_ack, &hash)
                .is_err()
        );
        let unchanged = recovered
            .metadata_backend()
            .unwrap()
            .export_table_snapshot()
            .unwrap();
        let rejected = recovered
            .process_routed_messages_with_options(vec![old], false)
            .unwrap();
        assert!(
            rejected
                .iter()
                .any(|event| matches!(event, NetworkedDfsServiceEvent::BlobStageRejected { .. }))
        );
        assert_eq!(
            recovered
                .metadata_backend()
                .unwrap()
                .export_table_snapshot()
                .unwrap(),
            unchanged
        );
        recovered.retry_pending_stages(8).unwrap();
        let fresh_request_id = recovered
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .stage_inflight_request_ids
            .get(&replica)
            .unwrap()
            .clone();
        assert_ne!(fresh_request_id, old_request_id);
        let fresh = recovered.transport.drain_incoming_messages().unwrap().into_iter().find(|routed|
            matches!(&routed.message, DfsNetworkMessage::StageBlobResponse(reply)
                if reply.request_id == fresh_request_id && reply.intent_id == intent_id
                && reply.file_system_id == request.file_system_id && reply.object_id == request.object_id
                && reply.version == request.target_version && reply.replica_node_id == replica
                && matches!(&reply.result, DfsBlobTransferResult::Durable { content_hash } if content_hash == &hash))).unwrap();
        let (peer, async_peer) = runtime.peer_transports(&primary).unwrap();
        recovered.transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            primary.clone(),
            peers,
            peer,
            async_peer,
            Duration::from_millis(50),
        );
        let report = recovered.process_message_command_report(test_dfs_message_command(
            format!("dfs_message_recovered_work_{case}"),
            fresh,
        ));
        let result = report.outcome.expect("serve recovered stage response");
        assert!(result.events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent_id, .. }
                if event_intent_id == &intent_id
        )));
        assert!(report.committed_work.publish_metadata);
        let committed = recovered
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .expect("the recovered commit is visible");
        let remaining = super::repair::indexed_object_repair_needs(
            &runtime,
            &recovered,
            &committed.file_system_id,
            &committed.path,
            &committed.object_id,
            &committed.version,
        );
        assert_eq!(
            remaining.len(),
            usize::from(expect_repair_maintenance),
            "the recovered commit leaves exactly its missing desired replica as repair work for {case}: {remaining:?}"
        );
        assert!(matches!(
            recovered.write_outcome(&intent_id).expect("committed outcome"),
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. }
                if repair_jobs.is_empty() != expect_repair_maintenance
        ));

        recovered.flush_detached_transport_sends().unwrap();
        drop(recovered);
        drop(background);
        runtime.clear_faults();
        runtime.close().unwrap();
    }
}
