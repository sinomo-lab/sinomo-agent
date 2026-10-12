//! Maintenance regression tests.

use super::*;

#[test]
fn lifecycle_maintenance_content_only_node_preserves_manifests_and_blobs_without_metadata_roots() {
    let root = temp_root("lifecycle-maintenance-content-only-no-roots");
    let node_id = NodeId::from_legacy_named("node_lifecycle_content_only").expect("node");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node_id),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let version_1 = dfs
        .content
        .put_object_version(
            "content-only-object",
            "version_1",
            b"content-only version one",
        )
        .expect("put first content-only version");
    let version_2 = dfs
        .content
        .put_object_version(
            "content-only-object",
            "version_2",
            b"content-only version two",
        )
        .expect("put second content-only version");

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            scrub_content: true,
            stale_staging_gc: Some(ContentStoreStagingGcOptions::delete_eligible(
                std::time::Duration::ZERO,
            )),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                std::time::Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(
                std::time::Duration::ZERO,
            )),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("content-only lifecycle maintenance");

    assert!(!report.metadata_backend_configured);
    assert!(report.content_gc_metadata_protection_unavailable);
    assert!(
        dfs_lifecycle_maintenance_notice_lines(std::slice::from_ref(&report))
            .iter()
            .any(|line| line.contains("content GC catalog unavailable")),
        "operators must see why destructive content GC was skipped"
    );
    assert!(
        report
            .content_scrub
            .as_ref()
            .expect("content scrub remains available")
            .is_healthy()
    );
    assert!(
        report.stale_staging_gc.is_some(),
        "staging cleanup does not require metadata reachability roots"
    );
    assert!(
        report.version_gc.is_none(),
        "destructive manifest GC must fail closed without metadata roots"
    );
    assert!(
        report.orphan_gc.is_none(),
        "destructive blob GC must fail closed without metadata roots"
    );
    assert_eq!(
        dfs.content
            .read_object_version("content-only-object", "version_1")
            .expect("first manifest remains readable")
            .1,
        b"content-only version one"
    );
    assert_eq!(
        dfs.content
            .read_object_version("content-only-object", "version_2")
            .expect("second manifest remains readable")
            .1,
        b"content-only version two"
    );
    assert!(
        dfs.content
            .has_blob(&version_1.content_hash)
            .expect("first blob remains")
    );
    assert!(
        dfs.content
            .has_blob(&version_2.content_hash)
            .expect("second blob remains")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_content_only_dry_run_marks_projection_unprotected() {
    let root = temp_root("lifecycle-maintenance-content-only-dry-run");
    let node_id = NodeId::from_legacy_named("node_lifecycle_content_only_dry_run").expect("node");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node_id),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let version_1 = dfs
        .content
        .put_object_version("content-only-dry-run", "version_1", b"dry-run version one")
        .expect("put first dry-run version");
    let version_2 = dfs
        .content
        .put_object_version("content-only-dry-run", "version_2", b"dry-run version two")
        .expect("put second dry-run version");

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            version_gc: Some(ContentStoreVersionGcOptions::dry_run(
                std::time::Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::dry_run(std::time::Duration::ZERO)),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("content-only dry-run lifecycle maintenance");

    assert!(!report.metadata_backend_configured);
    assert!(report.content_gc_metadata_protection_unavailable);
    let version_gc = report.version_gc.expect("unprotected version projection");
    assert!(version_gc.dry_run);
    assert_eq!(version_gc.scanned_version_count, 2);
    assert_eq!(version_gc.eligible_version_count, 0);
    assert_eq!(version_gc.retained_protected_version_count, 2);
    assert!(
        report
            .orphan_gc
            .expect("unprotected orphan projection")
            .dry_run
    );
    assert_eq!(
        dfs.content
            .read_object_version("content-only-dry-run", "version_1")
            .expect("first dry-run manifest remains")
            .1,
        b"dry-run version one"
    );
    assert_eq!(
        dfs.content
            .read_object_version("content-only-dry-run", "version_2")
            .expect("second dry-run manifest remains")
            .1,
        b"dry-run version two"
    );
    assert!(
        dfs.content
            .has_blob(&version_1.content_hash)
            .expect("first dry-run blob remains")
    );
    assert!(
        dfs.content
            .has_blob(&version_2.content_hash)
            .expect("second dry-run blob remains")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_host_clears_completed_anti_entropy_cursor() {
    let root = temp_root("lifecycle-maintenance-host-clears-cursor");
    let node_id = NodeId::from_legacy_named("node_lifecycle_host_clear").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "lifecycle_maintenance_host_clears_completed_anti_entropy_cursor",
        node_id,
    );
    let mut host = RecordingMaintenanceHost::default();

    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions::default(),
    )
    .expect("lifecycle maintenance");

    assert_eq!(reports.len(), 1);
    assert!(reports[0].anti_entropy.is_some());
    assert!(host.stored.is_empty());
    assert_eq!(host.cleared, vec![sifs_file_system_id()]);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_host_rejects_mismatched_cursor() {
    let root = temp_root("lifecycle-maintenance-host-mismatched-cursor");
    let node_id = NodeId::from_legacy_named("node_lifecycle_host_mismatch").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "lifecycle_maintenance_host_rejects_mismatched_cursor",
        node_id,
    );
    let mut host = RecordingMaintenanceHost::default();
    host.cursors.push((
        sifs_file_system_id(),
        DfsAntiEntropyCursorRecord {
            file_system_id: DfsFileSystemId::from_legacy_named("wrong_cursor_fs").expect("fs"),
            next_cursor: None,
        },
    ));

    let error = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions::default(),
    )
    .expect_err("mismatched cursor rejected");

    match error {
        DfsError::Metadata { message } => {
            assert!(message.contains("does not match file system"));
        }
        other => panic!("unexpected error: {other}"),
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn async_maintenance_yields_between_completed_synchronous_phases() {
    let local = NodeId::generate();
    let (runtime, mut dfs) = native_sifs_service("async-maintenance-native-yield", local.clone());
    let data_dir = runtime.data_dir(&local).clone();
    let root = data_dir.root();
    let mut maintenance_host = RecordingMaintenanceHost::default();
    let repair_queue_load_count = Arc::clone(&maintenance_host.load_repair_attempt_records_count);
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let (_, peer) = runtime.peer_transports(&local).unwrap();
    let mut transport = AsyncDfsMetadataNodeTransport::new(
        Vec::<NodeId>::new(),
        async_dfs_metadata_journal_batch_sender_from_peer_transport(&data_dir, Arc::clone(&peer)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer),
    );
    let execution_exclusions = DfsMaintenanceExecutionExclusions::default();
    let mut maintenance = Box::pin(
        run_dfs_maintenance_tick_with_async_host_and_execution_exclusions(
            &mut dfs,
            &mut maintenance_host,
            &data_dir,
            sifs_file_system_id(),
            &local,
            Vec::new(),
            &mut publisher_host,
            &mut transport,
            &execution_exclusions,
            None,
            DfsMaintenanceTickOptions {
                publish_metadata: false,
                ..DfsMaintenanceTickOptions::default()
            },
        ),
    );
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    // The durable queue snapshot precedes placement so scheduling and execution
    // use the same eligibility input; each preceding phase still owns a poll.
    for phase in 0..5 {
        assert!(
            matches!(
                std::future::Future::poll(maintenance.as_mut(), &mut context),
                std::task::Poll::Pending
            ),
            "bounded synchronous maintenance phase {phase} must yield"
        );
        assert_eq!(
            repair_queue_load_count.load(Ordering::Relaxed),
            0,
            "repair queue load must not run in the same poll as an earlier bounded phase"
        );
    }

    assert!(matches!(
        std::future::Future::poll(maintenance.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
    assert_eq!(repair_queue_load_count.load(Ordering::Relaxed), 1);
    for phase in ["placement", "lifecycle"] {
        assert!(
            matches!(
                std::future::Future::poll(maintenance.as_mut(), &mut context),
                std::task::Poll::Pending
            ),
            "bounded {phase} must yield after the shared repair queue snapshot"
        );
        assert_eq!(
            repair_queue_load_count.load(Ordering::Relaxed),
            1,
            "placement and lifecycle must reuse the same owner snapshot"
        );
    }
    drop(maintenance);

    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn dfs_maintenance_tick_observations_summarize_actionable_report() {
    let target = NodeId::from_legacy_named("node_dfs_observation_target").expect("node");
    let file_system_id =
        DfsFileSystemId::from_legacy_named("fs_dfs_observation").expect("file system");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_observation").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_observation").expect("version");
    let shard_id = DfsMetadataShardId::new("root");
    let report = DfsMaintenanceTickReport {
        namespace_jobs: DfsNamespaceJobMaintenanceReport {
            scanned_job_count: 2,
            resumed_job_count: 1,
            completed_job_count: 1,
            failed_job_count: 1,
            remaining_running_job_count: 0,
            failures: vec![DfsNamespaceJobFailure {
                job_id: "job-observation".to_string(),
                kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
                reason: "permission denied".to_string(),
            }],
        },
        write_intents: DfsWriteIntentMaintenanceReport {
            visited_intent_record_count: 3,
            visited_intent_shard_count: 1,
            scan_has_more: false,
            scanned_active_intent_count: 3,
            committed_visible_intent_count: 1,
            aborted_missing_object_count: 1,
            aborted_superseded_intent_count: 0,
            skipped_fresh_stale_candidate_count: 0,
        },
        placement_reconciliation: None,
        lifecycle_reports: vec![DfsLifecycleMaintenanceReport {
            anti_entropy: Some(DfsAntiEntropyReport {
                file_system_id: file_system_id.clone(),
                after: None,
                next_cursor: Some(DfsObjectPageCursor {
                    path: "/services/dfs-observation.txt".to_string(),
                    object_id: object_id.clone(),
                    shard_id: Some(shard_id.clone()),
                }),
                metadata_backend_configured: true,
                scanned_object_count: 4,
                checked_local_location_count: 3,
                stale_local_location_count: 1,
                repaired_location_count: 1,
                issue_count: 1,
                issues: vec![DfsAntiEntropyIssue {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    path: "/services/dfs-observation.txt".to_string(),
                    content_hash: "blake3:observation".to_string(),
                    reason: "missing local content".to_string(),
                    repaired: true,
                }],
                max_issues_reached: true,
            }),
            metadata_backend_configured: true,
            ..DfsLifecycleMaintenanceReport::default()
        }],
        scheduled_repairs: vec![DistributedRepairJob {
            intent_id: "intent-observation-repair".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            missing_replica_node_id: target.clone(),
        }],
        followup_events: vec![NetworkedDfsServiceEvent::RepairRejected {
            intent_id: "intent-observation-repair".to_string(),
            object_id: object_id.clone(),
            replica_node_id: target.clone(),
            reason: "network authorization denied".to_string(),
        }],
        repair_attempts_before: Vec::new(),
        repair_attempts_after: Vec::new(),
        repair_budget_exhausted: false,
        repair_page_checkpoint: Default::default(),
        metadata_replication: DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: target.clone(),
                    shard_id: shard_id.clone(),
                    entries: 3,
                    next_after_sequence: 9,
                },
                DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                    target_node_id: target.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence: 12,
                    archived_existing: true,
                },
                DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: target.clone(),
                    shard_id: shard_id.clone(),
                    reason: "timeout".to_string(),
                },
                DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: target.clone(),
                    shard_id,
                    after_sequence: 12,
                },
            ],
        },
    };

    let observations = dfs_maintenance_tick_observations(&report);

    assert_eq!(observations.metadata_replications_sent, 2);
    assert_eq!(
        observations.notice_lines,
        vec![
            "DFS namespace jobs: scanned=2, resumed=1, completed=1, failed=1, remaining=0"
                .to_string(),
            "DFS namespace job job-observation (ClearDirectoryRecursive) failed: permission denied"
                .to_string(),
            "DFS write intents: scanned_active=3, committed_visible=1, aborted_missing_object=1, aborted_superseded=0, skipped_fresh=0"
                .to_string(),
            format!(
                "DFS anti-entropy {file_system_id}: scanned=4, checked_local=3, stale_local=1, repaired=1, issues=1, next_cursor=true"
            ),
            format!(
                "DFS anti-entropy issue {object_id} {version} /services/dfs-observation.txt: missing local content (repaired=true)"
            ),
            format!("DFS anti-entropy {file_system_id} stopped after reaching the issue limit"),
            "DFS repairs: scheduled=1, followups=1".to_string(),
            format!(
                "DFS repair scheduled intent-observation-repair {object_id} {version} -> {target}"
            ),
            format!(
                "DFS repair rejected intent-observation-repair {object_id} by {target}: network authorization denied"
            ),
            format!("replicated 3 DFS metadata journal entries to {target}"),
            format!("repaired DFS metadata replica {target} with snapshot through 12"),
            format!("DFS metadata replication to {target} failed: timeout"),
        ]
    );
}

#[test]
fn native_sifs_bootstrap_keeps_one_complete_current_image_after_restart() {
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-bootstrap-current-restart",
        1,
    )
    .remove(0);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-bootstrap-current-restart", node.clone())
            .unwrap();
    runtime.start_nodes().unwrap();
    let mut service = runtime.client(&node).unwrap();
    let file = service
        .posix_write_file(
            "/services/native-bootstrap-restart.bin",
            b"retained current bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    for path in ["/", SIFS_SERVICES_PATH, SIFS_USERS_PATH] {
        assert_eq!(
            service.posix_stat(path).unwrap().kind,
            DfsObjectKind::Directory
        );
    }
    let configs = runtime.metadata_shards().unwrap();
    let mut initial = BTreeMap::new();
    for shard in &configs {
        let id = DfsMetadataShardId::new(shard.shard_id.clone());
        initial.insert(
            id.clone(),
            service
                .metadata_backend()
                .unwrap()
                .export_shard_table_snapshot(&id)
                .unwrap()
                .unwrap(),
        );
    }
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut service = runtime.client(&node).unwrap();
    assert_eq!(
        service
            .posix_read_file("/services/native-bootstrap-restart.bin")
            .unwrap(),
        b"retained current bytes"
    );
    assert_eq!(
        service
            .posix_stat("/services/native-bootstrap-restart.bin")
            .unwrap()
            .object_id,
        file.object_id
    );
    for (id, before) in initial {
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .export_shard_table_snapshot(&id)
                .unwrap()
                .unwrap(),
            before,
            "normal startup does not re-seed retained current data"
        );
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_async_commit_fetches_missing_local_desired_copy_without_maintenance() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-async-local-fetch", "/services");
    let path = "/services/async-local-fetch.bin";
    let bytes = (0..64 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();

    match cluster.writer.posix_write_file(
        path,
        &bytes,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
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

    sifs_posix_cluster_pump_until(
        &mut cluster,
        "primary fetches missing local desired copy after async commit",
        |cluster| {
            cluster
                .primary
                .posix_read_file(path)
                .is_ok_and(|read| read == bytes)
        },
    );

    assert!(
        cluster.rejections.is_empty(),
        "journal replication should remain clean: {:?}",
        cluster.rejections
    );

    drop(cluster);
}
