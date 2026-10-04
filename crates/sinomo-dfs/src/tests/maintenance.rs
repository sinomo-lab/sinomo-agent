//! Maintenance regression tests.

use super::*;

#[test]
fn sifs_bootstrap_repairs_untraversable_legacy_system_directories() {
    let root = temp_root("sifs-bootstrap-legacy-zero-mode");
    let node = NodeId::from_legacy_named("node_sifs_bootstrap_legacy_mode").expect("node");
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(ReceiveOnlyTransport::new(node), content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap SIFS");
    for path in ["/", "/sinomo"] {
        let mut legacy = crate::namespace::directory_object(path, dfs.node_id().clone());
        legacy.mode = 0;
        dfs.add_object_metadata(legacy)
            .expect("seed legacy bootstrap directory");
    }
    assert_eq!(dfs.posix_stat("/").expect("zero root").mode, 0);
    assert_eq!(dfs.posix_stat("/sinomo").expect("zero sinomo").mode, 0);

    dfs.bootstrap_distributed_sifs()
        .expect("repair SIFS bootstrap modes");

    assert_eq!(dfs.posix_stat("/").expect("root").mode, 0o755);
    assert_eq!(dfs.posix_stat("/sinomo").expect("sinomo").mode, 0o755);

    dfs.posix_set_mode("/sinomo", 0)
        .expect("intentional chmod after migration");
    dfs.bootstrap_distributed_sifs()
        .expect("re-bootstrap after intentional chmod");
    assert_eq!(
        dfs.posix_stat("/sinomo").expect("intentional mode").mode,
        0,
        "bootstrap must not mistake a real chmod(000) for legacy metadata"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_content_only_node_preserves_manifests_and_blobs_without_metadata_roots() {
    let root = temp_root("lifecycle-maintenance-content-only-no-roots");
    let node_id = NodeId::from_legacy_named("node_lifecycle_content_only").expect("node");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
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
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
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
    let mut dfs = networked_sifs_with_backend(&root, node_id);
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
fn local_maintenance_host_migrates_legacy_anti_entropy_cursor() {
    let root = temp_root("local-maintenance-host-cursor-migration");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("root layout");
    let file_system_id =
        DfsFileSystemId::from_legacy_named("fs_anti_entropy_cursor_migration").expect("fs");
    let object_id =
        DfsObjectId::from_legacy_named("obj_anti_entropy_cursor_migration").expect("object");
    let cursor = DfsAntiEntropyCursorRecord {
        file_system_id: file_system_id.clone(),
        next_cursor: Some(DfsObjectPageCursor {
            path: "/services/cursor-migration".to_string(),
            object_id,
            shard_id: None,
        }),
    };
    let path = default_dfs_anti_entropy_cursor_path(&data_dir, &file_system_id);
    let legacy_path = legacy_dfs_anti_entropy_cursor_path(&path);
    std::fs::create_dir_all(legacy_path.parent().expect("legacy cursor parent"))
        .expect("create legacy cursor parent");
    std::fs::write(
        &legacy_path,
        serde_json::to_vec_pretty(&cursor).expect("legacy cursor json"),
    )
    .expect("write legacy cursor");

    let loaded = read_dfs_anti_entropy_cursor(&path)
        .expect("read legacy cursor")
        .expect("legacy cursor");
    assert_eq!(loaded, cursor);
    assert!(!path.exists());
    assert!(legacy_path.exists());

    write_dfs_anti_entropy_cursor(&path, &loaded).expect("write sobj cursor");
    assert!(path.exists());
    assert!(!legacy_path.exists());
    assert_eq!(
        read_dfs_anti_entropy_cursor(&path).expect("read sobj cursor"),
        Some(cursor)
    );

    clear_dfs_anti_entropy_cursor(&path).expect("clear cursor");
    assert!(!path.exists());
    assert!(!legacy_path.exists());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_maintenance_host_rejects_mismatched_cursor() {
    let root = temp_root("lifecycle-maintenance-host-mismatched-cursor");
    let node_id = NodeId::from_legacy_named("node_lifecycle_host_mismatch").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id);
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

#[tokio::test]
async fn async_maintenance_yields_between_completed_synchronous_phases() {
    let root = temp_root("async-maintenance-yields-between-completed-phases");
    let local = NodeId::from_legacy_named("node_async_maintenance_yield").expect("local");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        RecordingFailingSendTransport {
            local_node_id: local.clone(),
            targets: Arc::new(Mutex::new(Vec::new())),
        },
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut maintenance_host = RecordingMaintenanceHost::default();
    let repair_queue_load_count = Arc::clone(&maintenance_host.load_repair_attempt_records_count);
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let mut transport = UnavailableAsyncMetadataJournalTransport;
    let data_dir = SinomoDataDir::new(&root);
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

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn maintenance_tick_drains_inflight_repair_response_without_new_schedule() {
    let root = temp_root("maintenance-tick-drains-inflight-repair-response");
    let owner = NodeId::from_legacy_named("node_maintenance_drain_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_maintenance_drain_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_maintenance_drain").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_maintenance_drain_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_maintenance_drain_2").expect("version");
    let bytes = b"maintenance drains already queued repair responses";
    let content_hash = content_hash(bytes);
    let network = InProcessNetwork::new();
    let mut replica_transport = network.add_node(replica.clone());
    let content = LocalContentStore::new(root.join("owner-content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = "/services/maintenance/drain.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
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
        .expect("threshold write");
    let intent_id = match outcome {
        NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
        other => panic!("expected committed write with repair, got {other:?}"),
    };
    let request_id = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .and_then(|pending| pending.repair_inflight_request_ids.get(&replica))
        .cloned()
        .expect("inflight repair request");
    assert!(
        !dfs.repair_retry_is_due(&intent_id, &replica),
        "repair response should be queued while retry is still inflight"
    );
    dfs.writes
        .pending_writes
        .get_mut(&intent_id)
        .and_then(|pending| pending.repair_retries.get_mut(&replica))
        .expect("repair retry state")
        .next_retry_tick = 100;

    let response_message = build_dfs_routed_message(
        &replica,
        &owner,
        900,
        901,
        DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
            request_id,
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            version: version_2.clone(),
            replica_node_id: replica.clone(),
            result: DfsBlobTransferResult::Committed {
                content_hash: content_hash.clone(),
            },
        }),
    );
    DfsMessageTransport::send_routed_call(
        &mut replica_transport,
        &owner,
        DfsRoutedCall::value(response_message),
    )
    .expect("queue repair response to owner");

    let committed_work_tracker = DfsCommittedWorkTracker::new();
    dfs.set_committed_work_tracker(committed_work_tracker.clone());
    let mut maintenance_host = RecordingMaintenanceHost::default();
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let mut metadata_transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let report = run_dfs_maintenance_tick_with_host(
        &mut dfs,
        &mut maintenance_host,
        sifs_file_system_id(),
        &owner,
        Vec::new(),
        &mut publisher_host,
        &mut metadata_transport,
        DfsMaintenanceTickOptions::default(),
    )
    .expect("maintenance tick");

    assert!(
        report.scheduled_repairs.is_empty(),
        "inflight repair should not be rescheduled in this tick"
    );
    assert!(
        report.followup_events.iter().any(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: event_intent_id,
                    replica_node_id,
                    ..
                } if event_intent_id == &intent_id && replica_node_id == &replica
            )
        }),
        "maintenance must drain the queued repair response"
    );
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "the final durable receipt must release the pending blob immediately"
    );
    assert!(
        dfs.coordinator
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != intent_id),
        "the final durable receipt must clear the repair job"
    );
    assert!(
        maintenance_host.repair_attempt_records.is_empty(),
        "maintenance must durably clear the completed repair attempt"
    );
    assert!(
        maintenance_host
            .stored_repair_attempt_records
            .last()
            .is_some_and(Vec::is_empty),
        "maintenance must persist the empty post-repair attempt set"
    );
    assert_eq!(
        committed_work_tracker.take(),
        DfsCommittedWork {
            publish_metadata: true,
            repair_maintenance: false,
            namespace_maintenance: false,
            metadata_replica_progress: false,
            metadata_materialization: false,
            attachment_cleanup: false,
        },
        "a maintenance-consumed durable receipt must retain its publication edge outside the service generation"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_maintenance_tick_opens_service_and_publishes_metadata() {
    let root = temp_root("cluster-maintenance-tick");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_maintenance_tick")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_maintenance_local").expect("local");
    let replica = NodeId::from_legacy_named("node_dfs_maintenance_replica").expect("replica");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs maintenance tick",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let replica_member = sinomo_core::cluster::local_cluster_state_authority_member(
        cluster_id,
        replica.clone(),
        "replica",
        "pk-replica",
        now,
    );
    state.members.push(replica_member);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("cluster kv state");

    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_kv_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("shard op"),
        ),
    )
    .expect("write shard config");

    let mut backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    let mut object = file_metadata(local.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_dfs_maintenance_tick").expect("object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/maintenance-tick.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&object).expect("object");
    drop(backend);

    let message_sender: DfsMessageSender = Arc::new(|_target_node_id, _command, _timeout| {
        Err(DfsPeerTransportError::new(
            "message sender should not be needed",
        ))
    });
    let sender: DfsMetadataJournalBatchSender = Arc::new(|_target_node_id, command, _timeout| {
        assert!(
            command
                .durable_replication
                .as_ref()
                .is_some_and(|replication| !replication.journal_frames.is_empty()),
            "cluster maintenance must send exact durable frames: {command:?}"
        );
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            applied_entries: applied_metadata_journal_command_entry_count(command),
            verified_prefix_only: command.verify_prefix_only,
            verified_authority_probe_id: applied_metadata_journal_command_confirmation(command),
            result: DfsMetadataJournalBatchResult::Applied,
        })
    });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|_target_node_id, _command, _timeout| {
            Err(DfsMetadataPeerTransportError::new(
                "snapshot install should not be needed",
            ))
        });
    let mut control_plane = sinomo_core::kv::InProcessKvStore::new();
    control_plane
        .put(
            metadata_shard_key(&file_system.file_system_id, "root"),
            sinomo_core::kv::encode_kv_value(&shard).expect("encode authority shard"),
            None,
        )
        .expect("seed authoritative maintenance topology");

    let report = run_cluster_dfs_maintenance_tick(
        &data_dir,
        &cluster_paths,
        message_sender,
        sender,
        snapshot_sender,
        Some(Box::new(control_plane)),
        DfsMaintenanceTickOptions::default(),
    )
    .expect("maintenance tick")
    .expect("clustered tick report");

    assert_eq!(report.lifecycle_reports.len(), 1);
    assert!(report.scheduled_repairs.is_empty());
    assert!(
        matches!(
            report.metadata_replication.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id,
                shard_id,
                entries,
                next_after_sequence,
            }] if target_node_id == &replica
                && shard_id == &DfsMetadataShardId::new("root")
                && *entries == 2
                && *next_after_sequence == 2
        ),
        "unexpected cluster metadata replication outcomes: {:?}",
        report.metadata_replication.outcomes
    );

    std::fs::remove_dir_all(root).ok();
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
        conflicts: DfsConflictMaintenanceReport::default(),
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
fn sifs_bootstrap_repairs_partial_sharded_filesystem_records() {
    let root = temp_root("sifs-bootstrap-partial-sharded-filesystem");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_partial_sharded_fs").expect("node");
    let file_system = distributed_sifs(node.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: node.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: node.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: SIFS_SERVICES_PATH.to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    {
        let mut partial =
            open_local_sharded_metadata_backend(&metadata_root, vec![services_shard.clone()])
                .expect("partial sharded backend");
        partial.put_file_system(&file_system).expect("filesystem");
    }

    let metadata = open_local_sharded_metadata_backend(
        &metadata_root,
        vec![root_shard.clone(), services_shard.clone()],
    )
    .expect("full sharded backend");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(node.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");

    dfs.bootstrap_distributed_sifs()
        .expect("bootstrap should repair the missing root-shard filesystem record");
    assert!(dfs.posix_stat("/").is_ok());
    assert!(dfs.posix_stat(SIFS_SERVICES_PATH).is_ok());

    let repaired = open_local_sharded_metadata_backend(&metadata_root, vec![root_shard])
        .expect("root shard backend");
    assert_eq!(
        repaired
            .get_file_system(&file_system.file_system_id)
            .expect("root shard filesystem"),
        Some(file_system)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_conflict_maintenance_marks_resolved_and_superseded_records() {
    let root = temp_root("stale-conflict-maintenance");
    let node = NodeId::from_legacy_named("node_stale_conflict_maintenance").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let base_version = DfsVersion::from_legacy_named("version_conflict_base").expect("version");
    let remote_version = DfsVersion::from_legacy_named("version_conflict_remote").expect("version");
    let local_version = DfsVersion::from_legacy_named("version_conflict_local").expect("version");
    let newer_version = DfsVersion::from_legacy_named("version_conflict_newer").expect("version");
    let active_object_id = DfsObjectId::from_legacy_named("obj_conflict_active").expect("object");
    let resolved_object_id =
        DfsObjectId::from_legacy_named("obj_conflict_resolved").expect("object");
    let superseded_object_id =
        DfsObjectId::from_legacy_named("obj_conflict_superseded").expect("object");

    for (object_id, version) in [
        (&resolved_object_id, &remote_version),
        (&active_object_id, &base_version),
        (&superseded_object_id, &newer_version),
    ] {
        let mut object = file_metadata(node.clone());
        object.object_id = object_id.clone();
        object.version = version.clone();
        object.path = format!("/services/{}.json", object_id.as_str());
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        dfs.add_object_metadata(object).expect("object");
    }

    for object_id in [
        resolved_object_id.clone(),
        active_object_id.clone(),
        superseded_object_id.clone(),
    ] {
        dfs.put_conflict_record(&DfsConflictRecord {
            conflict_id: format!("conflict_{}", object_id.as_str()),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            path: format!("/services/{}.json", object_id.as_str()),
            base_version: Some(base_version.clone()),
            local_version: local_version.clone(),
            remote_version: remote_version.clone(),
            detected_by_node_id: node.clone(),
            reason: "test stale conflict".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("conflict");
    }

    let report = dfs
        .reconcile_stale_conflicts_bounded(10)
        .expect("reconcile conflicts");

    assert_eq!(report.scanned_open_conflict_count, 3);
    assert_eq!(report.resolved_conflict_count, 1);
    assert_eq!(report.superseded_conflict_count, 1);
    assert_eq!(
        dfs.conflicts_for_object(&resolved_object_id)
            .expect("resolved conflict")[0]
            .status,
        DfsConflictStatus::Resolved
    );
    assert_eq!(
        dfs.conflicts_for_object(&active_object_id)
            .expect("active conflict")[0]
            .status,
        DfsConflictStatus::Open
    );
    assert_eq!(
        dfs.conflicts_for_object(&superseded_object_id)
            .expect("superseded conflict")[0]
            .status,
        DfsConflictStatus::Superseded
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stale_write_intent_maintenance_commits_visible_and_aborts_orphans() {
    let root = temp_root("stale-write-intent-maintenance");
    let node = NodeId::from_legacy_named("node_stale_write_intent_maintenance").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_write_intent_replica").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let base_version = DfsVersion::from_legacy_named("version_write_intent_base").expect("version");
    let target_version =
        DfsVersion::from_legacy_named("version_write_intent_target").expect("version");
    let visible_object_id =
        DfsObjectId::from_legacy_named("obj_write_intent_visible").expect("object");
    let missing_old_object_id =
        DfsObjectId::from_legacy_named("obj_write_intent_missing_old").expect("object");
    let missing_fresh_object_id =
        DfsObjectId::from_legacy_named("obj_write_intent_missing_fresh").expect("object");
    let content_hash = "blake3:stale-write-intent-visible";
    let stale_created_at = OffsetDateTime::now_utc() - time::Duration::days(2);
    let fresh_created_at = OffsetDateTime::now_utc();

    let mut visible_object = file_metadata(node.clone());
    visible_object.object_id = visible_object_id.clone();
    visible_object.path = "/services/stale-write-intent-visible.txt".to_string();
    visible_object.version = target_version.clone();
    visible_object.policy = DfsObjectPolicy::ReplicatedEventual;
    visible_object.content_hash = Some(content_hash.to_string());
    visible_object.current_locations = vec![node.clone()];
    dfs.add_object_metadata(visible_object.clone())
        .expect("visible object");

    for (intent_id, object_id, created_at) in [
        (
            "dfs_intent_visible_stale",
            visible_object_id.clone(),
            stale_created_at,
        ),
        (
            "dfs_intent_missing_old",
            missing_old_object_id.clone(),
            stale_created_at,
        ),
        (
            "dfs_intent_missing_fresh",
            missing_fresh_object_id.clone(),
            fresh_created_at,
        ),
    ] {
        dfs.metadata_backend
            .as_mut()
            .expect("metadata backend")
            .put_intent(&DfsWriteIntentRecord {
                publication: Default::default(),
                intent_id: intent_id.to_string(),
                file_system_id: sifs_file_system_id(),
                object_id,
                base_version: Some(base_version.clone()),
                target_version: target_version.clone(),
                coordinator_node_id: node.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 0,
                desired_replica_node_ids: vec![node.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(content_hash.to_string()),
                object_metadata: None,
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at,
                updated_at: Some(created_at),
            })
            .expect("intent");
    }

    let report = dfs
        .reconcile_stale_write_intents_bounded(10, Duration::from_secs(24 * 60 * 60))
        .expect("reconcile stale write intents");

    assert_eq!(report.scanned_active_intent_count, 3);
    assert_eq!(report.committed_visible_intent_count, 1);
    assert_eq!(report.aborted_missing_object_count, 1);
    assert_eq!(report.aborted_superseded_intent_count, 0);
    assert_eq!(report.skipped_fresh_stale_candidate_count, 1);

    let backend = dfs.metadata_backend().expect("backend");
    assert_eq!(
        backend
            .get_intent(&visible_object_id, "dfs_intent_visible_stale")
            .expect("visible intent")
            .expect("visible intent")
            .status,
        DfsWriteIntentStatus::Committed
    );
    assert_eq!(
        backend
            .get_intent(&missing_old_object_id, "dfs_intent_missing_old")
            .expect("missing old intent")
            .expect("missing old intent")
            .status,
        DfsWriteIntentStatus::Aborted
    );
    assert_eq!(
        backend
            .get_intent(&missing_fresh_object_id, "dfs_intent_missing_fresh")
            .expect("missing fresh intent")
            .expect("missing fresh intent")
            .status,
        DfsWriteIntentStatus::ReceiptsPending
    );
    assert_eq!(
        DfsOperatorInspector::new(backend)
            .status()
            .expect("operator status")
            .pending_write_count,
        1,
        "only the fresh missing-object intent should remain active"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_async_commit_fetches_missing_local_desired_copy_without_maintenance() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-async-local-fetch");
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

    std::fs::remove_dir_all(&cluster.root).ok();
}
