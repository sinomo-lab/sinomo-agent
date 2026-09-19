//! Replication regression tests.

use super::*;

#[test]
fn maintenance_tick_runs_lifecycle_and_metadata_journal_publish() {
    let root = temp_root("maintenance-tick-lifecycle-metadata-publish");
    let local = NodeId::from_legacy_named("node_maintenance_tick_local").expect("local");
    let replica = NodeId::from_legacy_named("node_maintenance_tick_replica").expect("replica");
    let mut dfs = networked_sifs_with_backend(&root, local.clone());
    let file_system = file_system(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let mut publish_backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("journal").join("root"))
            .expect("publish metadata backend");
    publish_backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let mut object = file_metadata(local.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_maintenance_tick_publish")
        .expect("maintenance tick object");
    object.path = "/services/maintenance/tick.txt".to_string();
    object.file_system_id = file_system.file_system_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    publish_backend
        .put_object(&object)
        .expect("object metadata");
    drop(publish_backend);

    let mut maintenance_host = RecordingMaintenanceHost::default();
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let report = run_dfs_maintenance_tick_with_host(
        &mut dfs,
        &mut maintenance_host,
        file_system.file_system_id.clone(),
        &local,
        vec![DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }],
        &mut publisher_host,
        &mut transport,
        DfsMaintenanceTickOptions {
            namespace_job_max_jobs: 4,
            namespace_job_max_steps_per_job: 16,
            ..DfsMaintenanceTickOptions::default()
        },
    )
    .expect("maintenance tick");

    assert_eq!(report.lifecycle_reports.len(), 1);
    assert!(report.scheduled_repairs.is_empty());
    assert!(report.followup_events.is_empty());
    assert_eq!(publisher_host.opened_shards, vec![shard_id.clone()]);
    assert_eq!(
        publisher_host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())]
    );
    assert_eq!(transport.batches, 1);
    assert!(
        matches!(
            report.metadata_replication.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id,
                shard_id: outcome_shard_id,
                entries,
                next_after_sequence,
            }] if target_node_id == &replica
                && outcome_shard_id == &shard_id
                && *entries == 2
                && *next_after_sequence == 2
        ),
        "unexpected metadata replication outcomes: {:?}",
        report.metadata_replication.outcomes
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_replication_cursor_advances_only_after_replica_ack() {
    let root = temp_root("metadata-replication-cursor-after-ack");
    let primary = NodeId::from_legacy_named("node_meta_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_replica").expect("replica");
    let file_system = file_system(primary.clone());
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: primary.clone(),
                replica_node_ids: vec![primary.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });

    let network = InProcessNetwork::new();
    let mut drop_replica_mailbox = network.add_node(replica.clone());
    let primary_content = LocalContentStore::new(root.join("primary-content")).expect("content");
    let mut primary_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("primary-meta"))
            .expect("primary backend");
    primary_backend
        .put_file_system(&file_system)
        .expect("primary filesystem");
    let mut first = file_metadata(primary.clone());
    first.object_id = DfsObjectId::from_legacy_named("obj_meta_first").expect("object");
    first.path = "/services/metadata/first.txt".to_string();
    first.file_system_id = file_system.file_system_id.clone();
    first.policy = DfsObjectPolicy::ReplicatedEventual;
    primary_backend.put_object(&first).expect("first object");
    let mut primary_dfs =
        NetworkedDfsService::new(network.add_node(primary.clone()), primary_content)
            .with_control_plane(shared_kv.clone())
            .with_metadata_backend(primary_backend)
            .expect("primary dfs");

    let outcomes = primary_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
        .expect("first replication send");
    assert!(
        matches!(
            outcomes.as_slice(),
            [DfsMetadataReplicationOutcome::Sent { .. }]
        ),
        "unexpected first replication outcomes: {outcomes:?}"
    );

    assert_eq!(
        DfsMessageTransport::drain_incoming_messages(&mut drop_replica_mailbox)
            .expect("drop first batch")
            .len(),
        1,
        "the first metadata batch is intentionally lost before the replica applies it"
    );

    let mut second = file_metadata(primary.clone());
    second.object_id = DfsObjectId::from_legacy_named("obj_meta_second").expect("object");
    second.path = "/services/metadata/second.txt".to_string();
    second.file_system_id = file_system.file_system_id.clone();
    second.policy = DfsObjectPolicy::ReplicatedEventual;
    primary_dfs
        .add_object_metadata(second.clone())
        .expect("second object");

    let outcomes = primary_dfs
        .replicate_metadata_shard_to_configured_replicas(&file_system.file_system_id, "root")
        .expect("retry replication before ack");
    assert!(matches!(
        outcomes.as_slice(),
        [DfsMetadataReplicationOutcome::Sent { entries, .. }] if *entries >= 3
    ));

    let replica_content = LocalContentStore::new(root.join("replica-content")).expect("content");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("replica-meta"))
            .expect("replica backend");
    let mut replica_dfs =
        NetworkedDfsService::new(network.add_node(replica.clone()), replica_content)
            .with_control_plane(shared_kv.clone())
            .with_metadata_backend(replica_backend)
            .expect("replica dfs");
    let events = replica_dfs
        .process_incoming()
        .expect("replica applies retry");
    assert!(
        !events.is_empty()
            && events.iter().all(|event| matches!(
                event,
                NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
            ))
            && events.iter().any(|event| matches!(
                event,
                NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    applied_entries,
                    ..
                } if *applied_entries >= 3
            )),
        "unexpected replica events: {events:?}"
    );

    let replica_backend = replica_dfs
        .metadata_backend()
        .expect("replica metadata backend");
    assert_eq!(
        replica_backend
            .get_object(&first.object_id)
            .expect("first replicated")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(first.path.as_str())
    );
    assert_eq!(
        replica_backend
            .get_object(&second.object_id)
            .expect("second replicated")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(second.path.as_str())
    );

    let owner_events = primary_dfs.process_incoming().expect("owner handles ack");
    assert!(matches!(
        owner_events.as_slice(),
        [NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
            source_node_id,
            next_after_sequence,
            ..
        }] if source_node_id == &replica && *next_after_sequence >= 3
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_replication_cursor_migrates_legacy_json_to_backend() {
    let root = temp_root("metadata-replication-cursor-legacy-migration");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let target = NodeId::from_legacy_named("node_metadata_cursor_legacy_target").expect("target");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata")).expect("metadata");
    let legacy_path = legacy_dfs_metadata_replication_cursor_path(&data_dir, &target, &shard_id);
    write_legacy_dfs_metadata_replication_cursor(
        &legacy_path,
        &LegacyDfsMetadataReplicationCursor {
            target_node_id: target.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence: 7,
        },
    )
    .expect("legacy cursor");

    let migrated =
        migrate_legacy_dfs_metadata_replication_cursor(&mut backend, &data_dir, &target, &shard_id)
            .expect("migrate legacy cursor");

    assert_eq!(migrated, 7);
    assert_eq!(
        backend
            .metadata_replication_cursor(&target, &shard_id)
            .expect("backend cursor"),
        7
    );
    assert!(!legacy_path.exists());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dfs_metadata_replication_retry_gate_tracks_incomplete_outcomes() {
    let node = NodeId::from_legacy_named("node_dfs_retry_gate").expect("node");
    let shard_id = DfsMetadataShardId::new("services");

    assert!(!dfs_metadata_replication_report_needs_retry(
        &DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: node.clone(),
                    shard_id: shard_id.clone(),
                    entries: 1,
                    next_after_sequence: 8,
                },
                DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: node.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence: 8,
                },
            ],
        }
    ));
    assert!(
        dfs_metadata_replication_report_needs_retry(&DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: node.clone(),
                shard_id: shard_id.clone(),
                reason: "target timed out after applying the batch".to_string(),
            }],
        }),
        "an ambiguous post-send timeout must retain the exact lane under failure backoff"
    );
    assert!(dfs_metadata_replication_report_needs_retry(
        &DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: node,
                shard_id,
                after_sequence: 64,
                max_batches: 64,
            }],
        }
    ));
}

#[test]
fn metadata_journal_replication_helper_advances_backend_cursor_after_apply() {
    let root = temp_root("metadata-journal-helper-advances-cursor");
    let primary = NodeId::from_legacy_named("node_meta_helper_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_helper_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let mut object = file_metadata(primary.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_meta_helper").expect("object");
    object.path = "/services/metadata/helper.txt".to_string();
    object.file_system_id = file_system.file_system_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&object).expect("object metadata");

    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let outcomes = replicate_dfs_metadata_shard_to_target(
        &mut backend,
        &primary,
        &replica,
        &file_system.file_system_id,
        1,
        &shard_id,
        0,
        &mut transport,
        DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 1,
            ..DfsMetadataJournalReplicationOptions::default()
        },
    )
    .expect("replicate metadata journal");

    let next_after_sequence = match outcomes.as_slice() {
        [
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id,
                shard_id: outcome_shard_id,
                entries,
                next_after_sequence,
            },
        ] if target_node_id == &replica && outcome_shard_id == &shard_id && *entries >= 2 => {
            *next_after_sequence
        }
        other => panic!("unexpected replication outcomes: {other:?}"),
    };
    assert_eq!(transport.batches, 1);
    assert_eq!(transport.snapshot_installs, 0);
    assert_eq!(
        backend
            .metadata_replication_cursor(&replica, &shard_id)
            .expect("replication cursor"),
        next_after_sequence
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_replication_repairs_sequence_conflict_with_snapshot() {
    let root = temp_root("metadata-journal-helper-snapshot-repair");
    let primary = NodeId::from_legacy_named("node_meta_snapshot_repair_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_snapshot_repair_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");

    let mut transport = SequenceConflictMetadataJournalTransport::new(replica.clone());
    let outcomes = replicate_dfs_metadata_shard_to_target(
        &mut backend,
        &primary,
        &replica,
        &file_system.file_system_id,
        1,
        &shard_id,
        0,
        &mut transport,
        DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 1,
            ..DfsMetadataJournalReplicationOptions::default()
        },
    )
    .expect("replicate metadata journal with snapshot repair");

    let next_after_sequence = backend.journal_last_sequence();
    assert_eq!(transport.batches, 1);
    assert_eq!(transport.snapshot_installs, 1);
    assert_eq!(
        outcomes,
        vec![DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
            target_node_id: replica.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence,
            archived_existing: true,
        }]
    );
    assert_eq!(
        backend
            .metadata_replication_cursor(&replica, &shard_id)
            .expect("replication cursor"),
        next_after_sequence
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_replication_repairs_corrupt_local_journal_with_snapshot() {
    let root = temp_root("metadata-journal-helper-corrupt-journal-repair");
    let primary =
        NodeId::from_legacy_named("node_meta_corrupt_journal_repair_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_meta_corrupt_journal_repair_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");

    let mut transport = SequenceConflictMetadataJournalTransport::new_with_reason(
        replica.clone(),
        "metadata serialization error: failed to decode segmented journal entry at /home/ix/.sinomo/dfs/metadata/services/journal/segments/00000000000000000001.slog: unexpected end of input",
    );
    let outcomes = replicate_dfs_metadata_shard_to_target(
        &mut backend,
        &primary,
        &replica,
        &file_system.file_system_id,
        1,
        &shard_id,
        0,
        &mut transport,
        DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 1,
            ..DfsMetadataJournalReplicationOptions::default()
        },
    )
    .expect("replicate metadata journal with corrupt-journal snapshot repair");

    let next_after_sequence = backend.journal_last_sequence();
    assert_eq!(transport.batches, 1);
    assert_eq!(transport.snapshot_installs, 1);
    assert_eq!(
        outcomes,
        vec![DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
            target_node_id: replica.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence,
            archived_existing: true,
        }]
    );
    assert_eq!(
        backend
            .metadata_replication_cursor(&replica, &shard_id)
            .expect("replication cursor"),
        next_after_sequence
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_publisher_routes_primary_shards_and_reports_non_primary() {
    let root = temp_root("metadata-journal-publisher-routes-primary");
    let local = NodeId::from_legacy_named("node_meta_publisher_local").expect("local");
    let replica = NodeId::from_legacy_named("node_meta_publisher_replica").expect("replica");
    let remote_primary =
        NodeId::from_legacy_named("node_meta_publisher_remote_primary").expect("remote primary");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(local.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("root")).expect("metadata");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let mut object = file_metadata(local.clone());
    object.object_id = DfsObjectId::from_legacy_named("obj_meta_publisher").expect("object");
    object.path = "/services/metadata/publisher.txt".to_string();
    object.file_system_id = file_system.file_system_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&object).expect("object metadata");
    drop(backend);

    let shards = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: remote_primary.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![local.clone(), replica.clone()],
            epoch: 3,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let mut host = RecordingMetadataJournalPublisherHost::new(root.to_path_buf());
    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());

    let report = publish_dfs_metadata_journal_with_host(
        file_system.file_system_id.clone(),
        &local,
        shards,
        &mut host,
        &mut transport,
        DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 1,
            ..DfsMetadataJournalReplicationOptions::default()
        },
    )
    .expect("publish metadata journal");

    assert_eq!(host.opened_shards, vec![shard_id.clone()]);
    assert_eq!(
        host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())]
    );
    assert_eq!(transport.batches, 1);
    assert_eq!(
        report.outcomes,
        vec![
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: replica.clone(),
                shard_id: shard_id.clone(),
                entries: 2,
                next_after_sequence: 2,
            },
            DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: file_system.file_system_id,
                shard_id: DfsMetadataShardId::new("services"),
                local_node_id: local,
                primary_node_id: remote_primary,
                epoch: 7,
            },
        ]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_publish_retries_legacy_transient_replica_suppression() {
    let root = temp_root("metadata-journal-publish-retries-transient-suppression");
    let local = NodeId::from_legacy_named("node_meta_retry_local").expect("local");
    let replica = NodeId::from_legacy_named("node_meta_retry_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let suppression = DfsMetadataReplicationSuppressionRecord {
        code: None,
        target_node_id: replica.clone(),
        shard_id: shard_id.clone(),
        shard_epoch: 7,
        reason: "metadata journal batch has no local shard config".to_string(),
        updated_at_unix_nanos: dfs_now_unix_nanos(),
    };
    let mut host = RecordingMetadataJournalPublisherHost::new(root.join("transient"));

    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id,
        &local,
        vec![shard],
        &mut host,
        &[suppression],
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare transiently rejected replica");

    assert!(targets.is_empty());
    assert_eq!(
        host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())],
        "a stale suppression from a receiver startup race must be ignored"
    );
    assert_eq!(
        outcomes,
        vec![DfsMetadataJournalReplicationOutcome::NoChanges {
            target_node_id: replica,
            shard_id,
            after_sequence: 0,
        }]
    );
    assert!(!metadata_replication_rejection_is_topology_mismatch(
        "local node replica is not configured as a replica for metadata shard root"
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_publish_reuses_one_journal_payload_for_equal_replica_cursors() {
    let root = temp_root("metadata-publish-shared-cursor-payload");
    let local = NodeId::from_legacy_named("node_meta_shared_cursor_local").expect("local");
    let replica_a =
        NodeId::from_legacy_named("node_meta_shared_cursor_replica_a").expect("replica a");
    let replica_b =
        NodeId::from_legacy_named("node_meta_shared_cursor_replica_b").expect("replica b");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(local.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("root"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    drop(backend);
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![replica_a, replica_b],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut host = RecordingMetadataJournalPublisherHost::new(root.path().to_path_buf());

    crate::metadata_journal::reset_metadata_journal_segment_scan_calls();
    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id,
        &local,
        vec![shard],
        &mut host,
        &[],
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare shared-cursor publish");

    assert!(outcomes.is_empty());
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].commands.len(), 1);
    assert_eq!(targets[1].commands.len(), 1);
    assert_eq!(targets[0].commands[0].batch, targets[1].commands[0].batch);
    assert_eq!(
        crate::metadata_journal::metadata_journal_segment_scan_calls(),
        2,
        "backend recovery scans once and equal replica cursors share one suffix scan"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_replication_primary_mismatch_is_topology_mismatch() {
    assert!(metadata_replication_rejection_is_topology_mismatch(
        "metadata journal source node_b is not shard services primary node_a"
    ));
    assert!(metadata_replication_rejection_is_topology_mismatch(
        "metadata snapshot source node_b is not shard users primary node_a"
    ));
}

#[test]
fn cluster_metadata_journal_publish_reads_cluster_state_and_shards() {
    let root = temp_root("cluster-metadata-journal-publish");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_helper")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_publish_helper_local").expect("local");
    let replica = NodeId::from_legacy_named("node_dfs_publish_helper_replica").expect("replica");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs metadata publish helper",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0] = sinomo_core::cluster::local_cluster_state_authority_member(
        cluster_id.clone(),
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let replica_member = sinomo_core::cluster::local_cluster_state_authority_member(
        cluster_id.clone(),
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
    object.object_id = DfsObjectId::from_legacy_named("obj_dfs_publish_helper").expect("object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/publish-helper.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&object).expect("object");
    drop(backend);

    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_for_sender = Arc::clone(&sent);
    let sender: DfsMetadataJournalBatchSender =
        Arc::new(move |target_node_id, command, _timeout| {
            sent_for_sender.lock().expect("sent lock").push((
                target_node_id.to_string(),
                applied_metadata_journal_command_entry_count(command),
            ));
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
        Arc::new(move |_target_node_id, _command, _timeout| {
            Err(DfsMetadataPeerTransportError::new(
                "snapshot install should not be needed",
            ))
        });

    let report = publish_cluster_dfs_metadata_journal(
        &data_dir,
        &cluster_paths,
        sender,
        snapshot_sender,
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("publish cluster metadata journal");

    assert_eq!(
        sent.lock().expect("sent lock").as_slice(),
        &[(replica.to_string(), 2)]
    );
    assert_eq!(
        report.outcomes,
        vec![DfsMetadataJournalReplicationOutcome::Applied {
            target_node_id: replica,
            shard_id: DfsMetadataShardId::new("root"),
            entries: 2,
            next_after_sequence: 2,
        }]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn async_cluster_metadata_publish_repairs_sequence_conflict_with_snapshot() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let root = temp_root("async-cluster-metadata-publish-snapshot-repair");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_async_snapshot_repair")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_async_snapshot_local").expect("local");
        let replica =
            NodeId::from_legacy_named("node_dfs_async_snapshot_replica").expect("replica");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "async DFS metadata snapshot repair",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0] = sinomo_core::cluster::local_cluster_state_authority_member(
            cluster_id.clone(),
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let replica_member = sinomo_core::cluster::local_cluster_state_authority_member(
            cluster_id.clone(),
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
        drop(backend);

        let journal_attempts = Arc::new(Mutex::new(0usize));
        let journal_attempts_for_sender = Arc::clone(&journal_attempts);
        let sender: AsyncDfsMetadataJournalBatchSender =
            Arc::new(move |_target_node_id, command, _timeout| {
                let journal_attempts = Arc::clone(&journal_attempts_for_sender);
                Box::pin(async move {
                    *journal_attempts.lock().expect("journal attempts") += 1;
                    Ok(DfsMetadataJournalBatchResponse {
                        request_id: command.request_id.clone(),
                        shard_id: command.batch.shard_id.clone(),
                        next_after_sequence: command.batch.next_after_sequence,
                        applied_entries: 0,
                        verified_prefix_only: false,
                        verified_authority_probe_id: None,
                        result: DfsMetadataJournalBatchResult::Rejected {
                            reason:
                                "metadata journal sequence 1 already exists with a different entry"
                                    .to_string(),
                            code: None,
                        },
                    })
                })
            });
        let snapshot_installs = Arc::new(Mutex::new(Vec::new()));
        let snapshot_installs_for_sender = Arc::clone(&snapshot_installs);
        let snapshot_sender: AsyncDfsMetadataSnapshotInstallSender =
            Arc::new(move |target_node_id, command, _timeout| {
                let snapshot_installs = Arc::clone(&snapshot_installs_for_sender);
                Box::pin(async move {
                    snapshot_installs
                        .lock()
                        .expect("snapshot installs")
                        .push((target_node_id.to_string(), command.next_after_sequence));
                    Ok(DfsMetadataSnapshotInstallResponse {
                        request_id: command.request_id.clone(),
                        shard_id: command.shard_id.clone(),
                        next_after_sequence: command.next_after_sequence,
                        result: DfsMetadataSnapshotInstallResult::Installed {
                            archived_existing: true,
                        },
                    })
                })
            });

        let report = publish_cluster_dfs_metadata_journal_async(
            &data_dir,
            &cluster_paths,
            sender,
            snapshot_sender,
            DfsMetadataJournalReplicationOptions::default(),
        )
        .await
        .expect("publish metadata");

        assert_eq!(*journal_attempts.lock().expect("journal attempts"), 1);
        assert_eq!(
            snapshot_installs
                .lock()
                .expect("snapshot installs")
                .as_slice(),
            &[(replica.to_string(), 1)]
        );
        assert_eq!(
            report.outcomes,
            vec![DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: replica.clone(),
                shard_id: DfsMetadataShardId::new("root"),
                next_after_sequence: 1,
                archived_existing: true,
            }]
        );
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        assert_eq!(
            backend
                .metadata_replication_cursor(&replica, &DfsMetadataShardId::new("root"))
                .expect("replication cursor"),
            1
        );

        std::fs::remove_dir_all(root).ok();
    });
}

#[test]
fn metadata_node_transport_rejects_mismatched_journal_response() {
    let target = NodeId::from_legacy_named("node_meta_node_route_mismatch_target").expect("target");
    let attempts = Arc::new(Mutex::new(0usize));
    let attempts_for_sender = Arc::clone(&attempts);
    let sender: DfsMetadataJournalBatchSender =
        Arc::new(move |_target_node_id, command, _timeout| {
            let mut attempts = attempts_for_sender.lock().expect("attempts lock");
            *attempts += 1;
            Ok(DfsMetadataJournalBatchResponse {
                request_id: command.request_id.clone(),
                shard_id: command.batch.shard_id.clone(),
                next_after_sequence: if *attempts == 1 {
                    command.batch.next_after_sequence + 1
                } else {
                    command.batch.next_after_sequence
                },
                applied_entries: command.batch.entries.len(),
                verified_prefix_only: command.verify_prefix_only,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Applied,
            })
        });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(move |_target_node_id, command, _timeout| {
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id.clone(),
                shard_id: command.shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Installed {
                    archived_existing: false,
                },
            })
        });
    let mut transport =
        DfsMetadataNodeTransport::new(vec![target.clone()], sender, snapshot_sender);
    let command = DfsMetadataJournalBatchCommand {
        request_id: "meta-node_route-mismatch".to_string(),
        source_node_id: None,
        file_system_id: None,
        authority_epoch: None,
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: Vec::new(),
            next_after_sequence: 5,
        },
    };

    let result = transport.send_metadata_journal_batch(&target, &command, Duration::from_secs(5));

    assert!(matches!(
        result,
        Err(DfsMetadataJournalReplicationTransportError::Failed { .. })
    ));
    assert_eq!(*attempts.lock().expect("attempts lock"), 1);
}

#[test]
fn async_metadata_node_transport_rejects_mismatched_journal_response() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let target = NodeId::from_legacy_named("node_async_meta_node_route_mismatch_target")
            .expect("target");
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_for_sender = Arc::clone(&attempts);
        let sender: AsyncDfsMetadataJournalBatchSender =
            Arc::new(move |_target_node_id, command, _timeout| {
                let attempts = Arc::clone(&attempts_for_sender);
                Box::pin(async move {
                    let mut attempts = attempts.lock().expect("attempts lock");
                    *attempts += 1;
                    Ok(DfsMetadataJournalBatchResponse {
                        request_id: command.request_id.clone(),
                        shard_id: command.batch.shard_id.clone(),
                        next_after_sequence: command.batch.next_after_sequence + 1,
                        applied_entries: command.batch.entries.len(),
                        verified_prefix_only: command.verify_prefix_only,
                        verified_authority_probe_id: None,
                        result: DfsMetadataJournalBatchResult::Applied,
                    })
                })
            });
        let snapshot_sender: AsyncDfsMetadataSnapshotInstallSender =
            Arc::new(move |_target_node_id, command, _timeout| {
                Box::pin(async move {
                    Ok(DfsMetadataSnapshotInstallResponse {
                        request_id: command.request_id.clone(),
                        shard_id: command.shard_id.clone(),
                        next_after_sequence: command.next_after_sequence,
                        result: DfsMetadataSnapshotInstallResult::Installed {
                            archived_existing: false,
                        },
                    })
                })
            });
        let transport =
            AsyncDfsMetadataNodeTransport::new(vec![target.clone()], sender, snapshot_sender);
        let command = DfsMetadataJournalBatchCommand {
            request_id: "async-meta-node_route-mismatch".to_string(),
            source_node_id: None,
            file_system_id: None,
            authority_epoch: None,
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("root"),
                entries: Vec::new(),
                next_after_sequence: 7,
            },
        };

        let result = transport
            .send_metadata_journal_batch_async(&target, &command, Duration::from_secs(5))
            .await;

        assert!(matches!(
            result,
            Err(DfsMetadataJournalReplicationTransportError::Failed { .. })
        ));
        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
    });
}

#[test]
fn metadata_journal_batch_processor_rejects_non_primary_source() {
    let root = temp_root("metadata-journal-processor-rejects-source");
    let primary = NodeId::from_legacy_named("node_meta_processor_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_processor_replica").expect("replica");
    let wrong_source =
        NodeId::from_legacy_named("node_meta_processor_wrong_source").expect("wrong source");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![primary.clone(), replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    };

    let response = process_dfs_metadata_journal_batch(
        &mut backend,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-reject-source".to_string(),
            source_node_id: Some(wrong_source.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id,
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        },
    );

    assert!(matches!(
        response.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains(&format!("source {wrong_source}"))
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn replica_journal_ingest_keeps_visible_namespace_in_backend_not_service_mirror() {
    const OBJECT_COUNT: usize = 4_096;

    let root = temp_root("replica-journal-does-not-mirror-visible-namespace");
    let primary =
        NodeId::from_legacy_named("node_replica_no_mirror_primary").expect("primary node id");
    let replica =
        NodeId::from_legacy_named("node_replica_no_mirror_receiver").expect("replica node id");
    let file_system = file_system(primary.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .expect("metadata shard")
    });

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source-metadata"))
        .expect("source metadata");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let mut objects = Vec::with_capacity(OBJECT_COUNT);
    for index in 0..OBJECT_COUNT {
        let mut object = file_metadata(primary.clone());
        object.file_system_id = file_system.file_system_id.clone();
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_replica_no_mirror_{index:04}"))
                .expect("object id");
        object.path = format!("/services/replica-no-mirror/{index:04}.txt");
        objects.push(object);
    }
    let probe_object = objects[OBJECT_COUNT / 2].clone();
    source
        .apply_namespace_transaction(&DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "namespace_tx_replica_no_mirror".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("bulk namespace transaction");
    let guarded_path = "/services/replica-no-mirror/guarded";
    source
        .put_namespace_job(&DfsNamespaceJobRecord {
            job_id: "job_replica_no_mirror_guard".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: guarded_path.to_string(),
            target_path: Some("/users/replica-no-mirror/guarded".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        })
        .expect("running namespace job");
    let batch = source
        .journal_batch_since_limited(0, 8, 64 * 1024 * 1024)
        .expect("source journal")
        .expect("source journal batch");

    let receiver = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("receiver-metadata"))
        .expect("receiver metadata");
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(receiver)
    .expect("receiver service");

    let event = service
        .handle_metadata_journal_batch(
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "request-replica-no-mirror".to_string(),
                source_node_id: Some(primary),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: batch.clone(),
            },
        )
        .expect("replica journal ingest");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
    ));
    assert!(
        service.metadata().objects.is_empty(),
        "replica ingest must not retain a second visible-object namespace"
    );
    assert_eq!(
        service
            .metadata_backend()
            .expect("receiver backend")
            .get_object_by_path(&file_system.file_system_id, &probe_object.path)
            .expect("durable path lookup"),
        Some(probe_object.clone()),
        "the durable replica tables remain the exact read source"
    );
    assert_eq!(
        service
            .current_read_object_metadata(&probe_object.object_id)
            .expect("service targeted backend read"),
        probe_object,
        "targeted service reads must not depend on the removed replica mirror"
    );
    assert!(matches!(
        service.ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            &format!("{guarded_path}/child.txt"),
        ),
        Err(DfsError::Busy { .. })
    ));
    assert_eq!(
        service
            .replication
            .local_metadata_journal_cursors
            .get(&shard_id),
        Some(&batch.next_after_sequence),
        "replica observation must still advance the service replay cursor"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn exact_resident_snapshot_install_keeps_namespace_out_of_service_mirror() {
    let root = temp_root("exact-resident-snapshot-does-not-mirror-namespace");
    let primary =
        NodeId::from_legacy_named("node_exact_snapshot_primary").expect("primary node id");
    let replica =
        NodeId::from_legacy_named("node_exact_snapshot_replica").expect("replica node id");
    let file_system = file_system(primary.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .expect("metadata shard")
    });

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source-metadata"))
        .expect("source metadata");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id =
        DfsObjectId::from_legacy_named("obj_exact_snapshot_probe").expect("object id");
    object.path = "/services/exact-snapshot/probe.txt".to_string();
    source.put_object(&object).expect("source object");
    let next_after_sequence = source.journal_last_sequence();
    let snapshot = source.export_table_snapshot();

    let resident =
        LocalShardedMetadataBackend::new(root.join("receiver-metadata"), vec![shard.clone()])
            .expect("receiver sharded metadata")
            .into_resident();
    let exact_backend = resident
        .scope_shard(&file_system.file_system_id, &shard_id)
        .expect("exact receiver shard")
        .checkout()
        .expect("exact receiver checkout");
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_resident_metadata_shard_backend_lazy(exact_backend)
    .expect("lazy exact replica service");

    crate::service_hydration::reset_full_metadata_hydration_calls();
    let event = service
        .handle_metadata_snapshot_install(
            primary,
            DfsMetadataSnapshotInstallCommand {
                request_id: "request-exact-resident-snapshot".to_string(),
                source_node_id: shard.primary_node_id.clone(),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                primary_handoff_id: None,
                shard_id: shard_id.clone(),
                next_after_sequence,
                snapshot,
                reason: "repair exact resident replica".to_string(),
            },
        )
        .expect("snapshot install");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. }
    ));
    assert_eq!(
        crate::service_hydration::full_metadata_hydration_calls(),
        0,
        "exact replica snapshot install must not scan and retain the full namespace"
    );
    assert!(
        service.metadata().objects.is_empty(),
        "the resident shard tables, not the request shell, own replica objects"
    );
    assert_eq!(
        service
            .metadata_backend()
            .expect("resident receiver backend")
            .get_object(&object.object_id)
            .expect("durable object lookup"),
        Some(object.clone())
    );
    assert_eq!(
        service
            .current_read_object_metadata(&object.object_id)
            .expect("targeted backend read"),
        object
    );
    assert_eq!(
        service
            .replication
            .local_metadata_journal_cursors
            .get(&shard_id),
        Some(&next_after_sequence)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn networked_metadata_replica_ingest_without_control_plane_fails_closed_without_mutation() {
    let root = temp_root("networked-metadata-ingest-no-control-plane");
    let primary =
        NodeId::from_legacy_named("node_no_control_plane_primary").expect("primary node id");
    let replica =
        NodeId::from_legacy_named("node_no_control_plane_replica").expect("replica node id");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source-metadata"))
        .expect("source metadata backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let mut source_object = file_metadata(primary.clone());
    source_object.object_id =
        DfsObjectId::from_legacy_named("obj_no_control_plane_ingest").expect("source object");
    source_object.file_system_id = file_system.file_system_id.clone();
    source_object.path = "/services/no-control-plane-ingest.txt".to_string();
    source
        .put_object(&source_object)
        .expect("source object metadata");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    let source_sequence = source.journal_last_sequence();
    let source_snapshot = source.export_table_snapshot();

    let receiver = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("receiver-metadata"))
        .expect("receiver metadata backend");
    let receiver_before = receiver.export_table_snapshot();
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_metadata_backend(receiver)
    .expect("receiver service");

    let journal_event = service
        .handle_metadata_journal_batch(
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "request-no-control-plane-journal".to_string(),
                source_node_id: Some(primary.clone()),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(41),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch,
            },
        )
        .expect("journal ingest rejection");
    assert!(matches!(
        journal_event,
        NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
            if reason.contains("requires a committed control-plane shard view")
    ));

    let snapshot_event = service
        .handle_metadata_snapshot_install(
            primary.clone(),
            DfsMetadataSnapshotInstallCommand {
                request_id: "request-no-control-plane-snapshot".to_string(),
                source_node_id: primary,
                file_system_id: Some(file_system.file_system_id),
                authority_epoch: Some(41),
                primary_handoff_id: None,
                shard_id,
                next_after_sequence: source_sequence,
                snapshot: source_snapshot,
                reason: "repair exact-looking replica state".to_string(),
            },
        )
        .expect("snapshot ingest rejection");
    assert!(matches!(
        snapshot_event,
        NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
            if reason.contains("requires a committed control-plane shard view")
    ));

    let receiver_after = service
        .metadata_backend
        .as_ref()
        .expect("receiver backend")
        .export_table_snapshot()
        .expect("receiver snapshot after rejected ingest");
    assert_eq!(receiver_after, receiver_before);
    assert!(
        service
            .metadata_backend
            .as_ref()
            .expect("receiver backend")
            .get_object(&source_object.object_id)
            .expect("receiver object lookup")
            .is_none(),
        "journal and snapshot ingest must leave the backend unchanged without a committed shard view"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn service_prefix_ack_retains_progress_before_failed_delivery_and_conflicting_snapshot() {
    let root = temp_root("service-prefix-ack-floor");
    let primary = NodeId::from_legacy_named("node_service_probe_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_service_probe_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 11,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source backend");
    source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let source_object = file_metadata(primary.clone());
    source.put_object(&source_object).expect("source object");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source batch")
        .expect("source entries");
    let journal_sequence = batch.next_after_sequence;
    let state_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        &source,
        &shard_id,
        journal_sequence,
    )
    .expect("source prefix proof");

    let receiver_root = root.join("receiver");
    let receiver =
        LocalSobjMetadataBackend::new(shard_id.clone(), &receiver_root).expect("receiver backend");
    let mut receiver_writer = LocalSobjMetadataBackend::new(shard_id.clone(), receiver_root)
        .expect("receiver writer backend");
    receiver_writer
        .apply_journal_batch(&batch)
        .expect("receiver exact catch-up");
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .expect("shard config")
    });
    let fail_sends = Arc::new(AtomicBool::new(true));
    let mut service = NetworkedDfsService::new(
        ToggleSendTransport {
            local_node_id: replica.clone(),
            fail_sends: Arc::clone(&fail_sends),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(receiver)
    .expect("receiver service");

    service.begin_message_committed_work();
    let error = service
        .handle_metadata_journal_batch(
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "service-prefix-probe".to_string(),
                source_node_id: Some(primary.clone()),
                file_system_id: Some(shard.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: true,
                prefix_probe: Some(DfsMetadataPrefixProbe {
                    barrier_id: "service-prefix-barrier".to_string(),
                    file_system_id: Some(file_system.file_system_id.clone()),
                    authority_epoch: Some(shard.epoch),
                    shard_id: shard_id.clone(),
                    journal_sequence,
                    state_fingerprint: state_fingerprint.clone(),
                    primary_handoff_id: None,
                }),
                authority_probe: None,
                durable_replication: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    entries: Vec::new(),
                    next_after_sequence: journal_sequence,
                },
            },
        )
        .expect_err("prefix ACK delivery must fail after durable proof");
    assert!(matches!(error, DfsError::Network { .. }));
    let committed_work = service.take_message_committed_work();
    assert!(
        committed_work.metadata_replica_progress,
        "the durable prefix floor must retain its local progress edge when ACK delivery fails"
    );
    let durable_prefix_proof = service
        .metadata_backend
        .as_ref()
        .expect("service backend")
        .metadata_prefix_proof(&shard_id)
        .expect("durable prefix proof")
        .expect("exact durable prefix proof");
    assert_eq!(
        service
            .metadata_backend
            .as_ref()
            .expect("service backend")
            .metadata_prefix_ack_floor(&shard_id)
            .expect("prefix floor"),
        Some(DfsMetadataPrefixAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence,
            state_fingerprint,
            durable_prefix_proof: Some(durable_prefix_proof),
        })
    );
    fail_sends.store(false, Ordering::SeqCst);

    let mut divergent = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("divergent"))
        .expect("divergent backend");
    divergent
        .put_file_system(&file_system)
        .expect("divergent filesystem");
    let mut divergent_object = source_object.clone();
    divergent_object.path = "/same-sequence-divergence".to_string();
    divergent
        .put_object(&divergent_object)
        .expect("divergent object");
    assert_eq!(divergent.journal_last_sequence(), journal_sequence);
    let rejection = service
        .handle_metadata_snapshot_install(
            primary.clone(),
            DfsMetadataSnapshotInstallCommand {
                request_id: "service-conflicting-snapshot".to_string(),
                source_node_id: primary.clone(),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                primary_handoff_id: None,
                shard_id: shard_id.clone(),
                next_after_sequence: journal_sequence,
                snapshot: divergent.export_table_snapshot(),
                reason: "same-sequence conflicting repair".to_string(),
            },
        )
        .expect("service snapshot rejection");
    assert!(matches!(
        rejection,
        NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
            if reason.contains("locally acknowledged checkpoint")
    ));

    let mut prepared_object = source_object.clone();
    prepared_object.version =
        DfsVersion::from_legacy_named("service-authority-prepared-v2").expect("prepared version");
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "service-authority-prepare".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
        owner_epoch: shard.epoch,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: source_object.object_id.clone(),
            expected_version: Some(source_object.version.clone()),
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: source_object.path.clone(),
            expected_object_id: Some(source_object.object_id.clone()),
        }],
        content_receipts: Vec::new(),
        transaction: DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "service-authority-transaction".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects: vec![prepared_object],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        },
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    let prepare_entry = source
        .prepare_namespace_transaction(&prepare)
        .expect("source prepare");
    let authority_batch = source
        .journal_batch_since_limited(
            journal_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("authority batch")
        .expect("authority entry");
    let prepared_prefix = source
        .metadata_prefix_proof()
        .expect("prepared source prefix");
    let authority_operation =
        crate::service_replica::metadata_authority_operation_for_namespace_prepare(
            &prepare,
            prepare_entry.sequence,
        )
        .expect("authority operation");
    assert!(matches!(
        authority_batch.entries.as_slice(),
        [DfsMetadataJournalEntry {
            mutation: DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: None,
                packed_prepare_v2: Some(_),
                ..
            },
            ..
        }]
    ));
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let authority_event = service
        .handle_metadata_journal_batch(
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "service-authority-probe".to_string(),
                source_node_id: Some(primary),
                file_system_id: Some(shard.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: Some(DfsMetadataAuthorityProbe {
                    probe_id: "service-authority-probe-id".to_string(),
                    expected_shard: shard,
                    operation: authority_operation,
                    durable_journal_ack: false,
                    journal_frames: Vec::new(),
                    prepared_prefix: Some(prepared_prefix),
                }),
                durable_replication: None,
                batch: authority_batch,
            },
        )
        .expect("service authority probe");
    assert!(matches!(
        authority_event,
        NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
    ));
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "in-service PackedV2 apply and authority ACK must share one canonical digest"
    );
    assert_eq!(
        service
            .metadata_backend
            .as_ref()
            .expect("service backend")
            .metadata_authority_ack_floor(&DfsMetadataShardId::new("root"))
            .expect("authority floor"),
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: DfsMetadataShardId::new("root"),
            journal_sequence: prepare_entry.sequence,
            durable_prefix_proof: None,
        }),
        "in-service authority ACK must persist its rollback floor before responding"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_snapshot_install_processor_replaces_backend_snapshot() {
    let root = temp_root("metadata-snapshot-install-processor");
    let primary = NodeId::from_legacy_named("node_meta_snapshot_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_snapshot_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut source_backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source-meta"))
            .expect("source metadata backend");
    source_backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    let mut object = file_metadata(primary.clone());
    object.object_id =
        DfsObjectId::from_legacy_named("obj_meta_snapshot_install").expect("snapshot object");
    object.path = "/services/metadata/snapshot-install.txt".to_string();
    object.file_system_id = file_system.file_system_id.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    source_backend.put_object(&object).expect("object metadata");

    let snapshot = source_backend.export_table_snapshot();
    let next_after_sequence = source_backend.journal_last_sequence();
    let snapshot_prefix = snapshot
        .prefix_proof
        .clone()
        .expect("snapshot exact prefix");
    let mut replica_backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica-meta"))
            .expect("replica metadata backend");
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id,
            shard_id: shard_id.0.clone(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![primary.clone(), replica],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    };
    replica_backend
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: next_after_sequence,
            durable_prefix_proof: Some(snapshot_prefix.clone()),
        })
        .expect("matching exact authority floor");

    let response = process_dfs_metadata_snapshot_install(
        &mut replica_backend,
        Some(&context),
        DfsMetadataSnapshotInstallCommand {
            request_id: "request-install-snapshot".to_string(),
            source_node_id: primary.clone(),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            primary_handoff_id: None,
            shard_id: shard_id.clone(),
            next_after_sequence,
            snapshot: snapshot.clone(),
            reason: "repair sequence conflict".to_string(),
        },
    );

    assert!(matches!(
        response.result,
        DfsMetadataSnapshotInstallResult::Installed { .. }
    ));
    assert_eq!(
        replica_backend
            .get_object(&object.object_id)
            .expect("replicated object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some(object.path.as_str())
    );

    let mut divergent_prefix = snapshot_prefix;
    let final_digit = divergent_prefix.digest.pop().expect("digest digit");
    divergent_prefix
        .digest
        .push(if final_digit == '0' { '1' } else { '0' });
    let mut divergent_replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("divergent-replica-meta"))
            .expect("divergent replica metadata backend");
    divergent_replica
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: next_after_sequence,
            durable_prefix_proof: Some(divergent_prefix),
        })
        .expect("divergent exact authority floor");
    let divergent_response = process_dfs_metadata_snapshot_install(
        &mut divergent_replica,
        Some(&context),
        DfsMetadataSnapshotInstallCommand {
            request_id: "request-install-divergent-snapshot".to_string(),
            source_node_id: primary,
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            primary_handoff_id: None,
            shard_id,
            next_after_sequence,
            snapshot,
            reason: "same-sequence divergent repair".to_string(),
        },
    );
    assert!(matches!(
        divergent_response.result,
        DfsMetadataSnapshotInstallResult::Rejected { ref reason , ..}
            if reason.contains("prefix conflicts with locally acknowledged authority operation")
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_intent_conflict_rejects_incrementally_without_full_hydrate() {
    let root = temp_root("metadata-journal-intent-reject-incremental");
    let owner = NodeId::from_legacy_named("node_meta_conflict_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_meta_conflict_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_meta_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let file_system = file_system(owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/metadata/conflict.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];

    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: "dfs_intent_meta_conflict_first".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: version_2.clone(),
            coordinator_node_id: owner.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: None,
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("first intent");

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_metadata_backend(backend)
        .expect("dfs");
    dfs.reads.pending_reads.insert(
        "sentinel-read".to_string(),
        PendingNetworkedRead {
            object_id: object_id.clone(),
            version: version_1.clone(),
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
            target_node_id: replica.clone(),
            next_retry_at: std::time::Instant::now(),
            cache_response: true,
        },
    );

    let second_intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "dfs_intent_meta_conflict_second".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        object_id: object_id.clone(),
        base_version: Some(version_1.clone()),
        target_version: version_3.clone(),
        coordinator_node_id: replica.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: vec![owner.clone(), replica.clone()],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: None,
        object_metadata: Some(object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
        updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
    };
    let entry = dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .put_intent(&second_intent)
        .expect("second intent");
    let batch = DfsMetadataJournalBatch {
        shard_id: entry.shard_id.clone(),
        next_after_sequence: entry.sequence,
        entries: vec![entry],
    };

    dfs.apply_metadata_journal_batch_to_service_state(&batch)
        .expect("incremental apply");

    assert!(
        dfs.reads.pending_reads.contains_key("sentinel-read"),
        "metadata journal intent rejection must not clear active read state with a full hydrate"
    );
    let stored_intent = dfs
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &second_intent.intent_id)
        .expect("stored intent")
        .expect("second intent remains");
    assert_eq!(stored_intent.status, DfsWriteIntentStatus::Rejected);
    assert!(
        dfs.conflicts_for_object(&object_id)
            .expect("conflicts")
            .is_empty(),
        "normal metadata replay must not create DFS conflicts"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_journal_compact_namespace_commit_refreshes_affected_service_objects() {
    let root = temp_root("metadata-journal-compact-namespace-commit-service-refresh");
    let owner = NodeId::from_legacy_named("node_compact_commit_service_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut current = file_metadata(owner.clone());
    current.file_system_id = file_system.file_system_id.clone();
    current.object_id =
        DfsObjectId::from_legacy_named("obj_compact_commit_service").expect("object");
    current.path = "/services/compact-commit-service.txt".to_string();
    current.version =
        DfsVersion::from_legacy_named("version_compact_commit_service_1").expect("version");
    backend.put_object(&current).expect("current object");
    let seed_entries = backend.journal_since(0).expect("seed journal");

    let mut committed = current.clone();
    committed.version =
        DfsVersion::from_legacy_named("version_compact_commit_service_2").expect("version");
    committed.mode = 0o600;
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "operation-compact-commit-service".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: 1,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: current.object_id.clone(),
            expected_version: Some(current.version.clone()),
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: current.path.clone(),
            expected_object_id: Some(current.object_id.clone()),
        }],
        content_receipts: Vec::new(),
        transaction: DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "transaction-compact-commit-service".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects: vec![committed.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        },
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    let prepare_entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepared transaction");
    let commit_entry = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("compact commit");
    assert_eq!(
        commit_entry.mutation.affected_object_ids(),
        BTreeSet::from([current.object_id.clone()])
    );

    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content);
    dfs.apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: seed_entries.last().expect("seed entry").sequence,
        entries: seed_entries,
    })
    .expect("seed stale service state");
    assert_eq!(dfs.metadata.object(&current.object_id), Some(&current));

    dfs.set_metadata_backend_unhydrated(backend);
    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: prepare_entry.sequence,
        entries: vec![prepare_entry],
    })
    .expect("hidden prepare service replay");
    assert_eq!(
        crate::metadata_read_snapshot::metadata_durable_table_probes(),
        0,
        "a hidden prepare must not reload every expected/proposed object from durable metadata"
    );
    assert_eq!(
        dfs.metadata.object(&current.object_id),
        Some(&current),
        "hidden prepare replay must not change visible service state"
    );
    dfs.apply_metadata_journal_batch_to_service_state(&DfsMetadataJournalBatch {
        shard_id,
        next_after_sequence: commit_entry.sequence,
        entries: vec![commit_entry],
    })
    .expect("refresh compact commit");
    assert_eq!(
        dfs.metadata.object(&current.object_id),
        Some(&committed),
        "compact commit IDs must refresh the service cache from authoritative metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_replay_uses_existing_committed_intent_after_service_restart() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_replay_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_replay_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_replay").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_routed_replay_base").expect("base version");
    let target_version =
        DfsVersion::from_legacy_named("version_routed_replay_target").expect("target version");
    let root = temp_root("routed-write-replay-existing-intent");
    let metadata_root = root.join("metadata");
    let content_hash = content_hash(b"remote routed duplicate payload");
    let file_system = file_system(primary.clone());

    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/replayed-routed-write.txt".to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;

    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: "dfs_req_replayed_routed_write".to_string(),
        request: DfsWriteRequest {
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            target_version: target_version.clone(),
            requesting_node_id: requester.clone(),
        },
        expected_base_version: Some(base_version),
        metadata_journal_after_sequence: None,
        bytes: Vec::new(),
        content_hash: Some(content_hash.clone()),
        content_size_bytes: Some(31),
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![requester.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        object_metadata: Some(object.clone()),
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: true,
        exclusive_create: false,
    };

    let mut primary_service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: primary.clone(),
        },
        LocalContentStore::new(root.join("content-primary")).expect("primary content"),
    )
    .with_metadata_backend(open_root_local_metadata_backend(&metadata_root).expect("metadata"))
    .expect("metadata backend");
    primary_service
        .add_file_system(file_system.clone())
        .expect("filesystem");
    primary_service.add_object_metadata(object).expect("object");
    let first_outcome = primary_service
        .write_remote_distributed_manifest(
            command.request.clone(),
            command.expected_base_version.clone(),
            content_hash.clone(),
            31,
            command.desired_replica_nodes.clone(),
            command.policy.clone(),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("first routed write");
    let NetworkedDfsWriteOutcome::Committed { intent_id, .. } = first_outcome else {
        panic!("expected committed routed write, got {first_outcome:?}");
    };

    let in_memory_replay = primary_service
        .replayed_routed_write_outcome_if_known(&requester, &command)
        .expect("in-memory replay lookup")
        .expect("exact in-memory replay");
    assert!(matches!(
        in_memory_replay,
        NetworkedDfsWriteOutcome::Committed {
            intent_id: replayed_intent_id,
            content_hash: replayed_content_hash,
            ..
        } if replayed_intent_id == intent_id && replayed_content_hash == content_hash
    ));

    let mut different_content = command.clone();
    different_content.content_hash = Some(crate::content_hash(b"different routed write payload"));
    assert!(
        primary_service
            .replayed_routed_write_outcome_if_known(&requester, &different_content)
            .expect("different-content replay lookup")
            .is_none(),
        "a distinct payload must not borrow an in-memory committed response"
    );

    let mut different_size = command.clone();
    different_size.content_size_bytes = Some(32);
    assert!(
        primary_service
            .replayed_routed_write_outcome_if_known(&requester, &different_size)
            .expect("different-size replay lookup")
            .is_none(),
        "a manifest command with a different size must not borrow a committed response"
    );

    let extra_replica =
        NodeId::from_legacy_named("node_routed_write_replay_extra_replica").expect("replica");
    let mut stronger_placement = command.clone();
    stronger_placement.desired_replica_nodes.push(extra_replica);
    stronger_placement.policy = DfsReplicationPolicy::new(2, 2).expect("stronger policy");
    assert!(
        primary_service
            .replayed_routed_write_outcome_if_known(&requester, &stronger_placement)
            .expect("stronger-placement replay lookup")
            .is_none(),
        "a stronger durability request must not borrow a weaker committed response"
    );

    let mut restarted_primary = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: primary,
        },
        LocalContentStore::new(root.join("content-restarted")).expect("restarted content"),
    )
    .with_metadata_backend(open_root_local_metadata_backend(&metadata_root).expect("metadata"))
    .expect("metadata backend");
    let replayed = restarted_primary
        .replayed_routed_write_outcome_if_known(&requester, &command)
        .expect("replay lookup")
        .expect("existing committed intent");

    assert!(matches!(
        replayed,
        NetworkedDfsWriteOutcome::Committed {
            intent_id: replayed_intent_id,
            version,
            content_hash: replayed_content_hash,
            ..
        } if replayed_intent_id == intent_id
            && version == target_version
            && replayed_content_hash == content_hash
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_exclusive_create_replays_only_the_exact_request_and_rolls_back_rejection() {
    let requester =
        NodeId::from_legacy_named("node_routed_exclusive_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_exclusive_primary").expect("primary");
    let root = temp_root("routed-exclusive-create-identity");
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![requester.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .expect("metadata shard");
    });
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: primary.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), vec![shard])
            .expect("metadata backend"),
    )
    .expect("attach metadata backend");
    service
        .bootstrap_distributed_sifs()
        .expect("bootstrap SIFS");

    let command_for = |request_id: &str, path: &str| {
        let mut object =
            crate::posix_object::new_posix_metadata_object(&primary, path, DfsObjectKind::File);
        crate::posix_object::apply_posix_create_attrs(
            &mut object,
            DfsPosixCreateAttrs::new(0o640, 8101, 8102),
        );
        DfsDistributedWriteCommand {
            publication: Default::default(),
            request_id: request_id.to_string(),
            request: DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: crate::namespace::next_version(&object.version),
                requesting_node_id: requester.clone(),
            },
            expected_base_version: Some(object.version.clone()),
            metadata_journal_after_sequence: None,
            bytes: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            requester_blob_pre_staged: true,
            desired_replica_nodes: vec![primary.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            object_metadata: Some(object),
            authority_shard: None,
            touch_parent_on_create: true,
            foreground_replica_repair: true,
            exclusive_create: true,
        }
    };

    let fresh_replay_probe = command_for(
        "exclusive-create-fresh-replay-probe",
        "/users/exclusive-routed-probe",
    );
    crate::metadata_sharded_backend::reset_sharded_metadata_object_id_lookup_count();
    assert!(
        service
            .replayed_routed_write_outcome_if_known(&requester, &fresh_replay_probe)
            .expect("fresh exclusive-create replay lookup")
            .is_none()
    );
    assert_eq!(
        crate::metadata_sharded_backend::sharded_metadata_object_id_lookup_count(),
        0,
        "fresh exclusive-create replay detection must stay on the path-selected shard"
    );

    let command = command_for("exclusive-create-original", "/users/exclusive-routed");
    let first = service
        .handle_distributed_write_request(requester.clone(), command.clone())
        .expect("serve first exclusive create");
    assert!(
        matches!(
            &first,
            NetworkedDfsServiceEvent::RemoteWriteServed {
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed { .. }
                },
                ..
            }
        ),
        "first exclusive create: {first:?}"
    );
    let duplicate = service
        .handle_distributed_write_request(requester.clone(), command.clone())
        .expect("serve exact duplicate");
    assert!(matches!(
        duplicate,
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed { .. }
            },
            ..
        }
    ));

    let mut altered_attrs = command.clone();
    altered_attrs
        .object_metadata
        .as_mut()
        .expect("create metadata")
        .mode = 0o600;
    let altered_attrs = service
        .handle_distributed_write_request(requester.clone(), altered_attrs)
        .expect("serve same request id with altered attrs");
    assert!(matches!(
        altered_attrs,
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Rejected { ref reason, .. },
            ..
        } if reason.contains("already exists")
    ));

    let mut fresh = command;
    fresh.request_id = "exclusive-create-fresh".to_string();
    let fresh = service
        .handle_distributed_write_request(requester.clone(), fresh)
        .expect("serve fresh exclusive create");
    assert!(matches!(
        fresh,
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Rejected { ref reason, .. },
            ..
        } if reason.contains("already exists")
    ));

    let failed_path = "/users/exclusive-routed-rejected";
    let mut invalid = command_for("exclusive-create-invalid", failed_path);
    invalid.desired_replica_nodes.clear();
    let rejected = service
        .handle_distributed_write_request(requester.clone(), invalid)
        .expect("serve invalid exclusive create");
    assert!(matches!(
        rejected,
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Rejected { .. },
            ..
        }
    ));
    assert!(matches!(
        service.posix_lstat(failed_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let retry = service
        .handle_distributed_write_request(
            requester.clone(),
            command_for("exclusive-create-valid-retry", failed_path),
        )
        .expect("serve valid retry");
    assert!(matches!(
        retry,
        NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed { .. }
            },
            ..
        }
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_replay_pending_intent_requires_matching_content_identity() {
    let requester =
        NodeId::from_legacy_named("node_routed_pending_replay_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_pending_replay_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_routed_pending_replay_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_pending_replay").expect("object");
    let base_version =
        DfsVersion::from_legacy_named("version_routed_pending_base").expect("base version");
    let target_version =
        DfsVersion::from_legacy_named("version_routed_pending_target").expect("target version");
    let root = temp_root("routed-write-replay-pending-content-identity");
    let file_system = file_system(primary.clone());
    let payload = b"pending routed replay payload";
    let payload_hash = content_hash(payload);

    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/pending-routed-replay.txt".to_string();
    object.version = base_version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;

    let command = DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: "dfs_req_pending_routed_replay".to_string(),
        request: DfsWriteRequest {
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            target_version: target_version.clone(),
            requesting_node_id: requester.clone(),
        },
        expected_base_version: Some(base_version),
        metadata_journal_after_sequence: None,
        bytes: Vec::new(),
        content_hash: Some(payload_hash.clone()),
        content_size_bytes: Some(payload.len() as u64),
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![requester.clone(), replica],
        policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
        object_metadata: Some(object.clone()),
        authority_shard: None,
        touch_parent_on_create: false,
        foreground_replica_repair: true,
        exclusive_create: false,
    };

    let mut primary_service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: primary,
        },
        LocalContentStore::new(root.join("content-primary")).expect("primary content"),
    );
    primary_service
        .add_file_system(file_system)
        .expect("filesystem");
    primary_service.add_object_metadata(object).expect("object");
    let first_outcome = primary_service
        .write_remote_distributed_manifest(
            command.request.clone(),
            command.expected_base_version.clone(),
            payload_hash.clone(),
            payload.len() as u64,
            command.desired_replica_nodes.clone(),
            command.policy.clone(),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("first pending routed write");
    let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = first_outcome else {
        panic!("expected pending routed write, got {first_outcome:?}");
    };

    assert!(matches!(
        primary_service
            .replayed_routed_write_outcome_if_known(&requester, &command)
            .expect("exact pending replay lookup"),
        Some(NetworkedDfsWriteOutcome::Pending {
            intent_id: replayed_intent_id,
            ..
        }) if replayed_intent_id == intent_id
    ));

    let mut different_content = command;
    different_content.content_hash = Some(content_hash(b"different pending replay payload"));
    assert!(
        primary_service
            .replayed_routed_write_outcome_if_known(&requester, &different_content)
            .expect("different pending replay lookup")
            .is_none(),
        "a distinct payload must not borrow an in-memory pending intent"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_replays_primary_metadata_journal() {
    let requester =
        NodeId::from_legacy_named("node_routed_write_journal_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_write_journal_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_write_journal").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_write_journal").expect("version");
    let root = temp_root("routed-write-journal-replay");
    let file_system = file_system(primary.clone());
    let authority_shard = test_routed_authority_shard(&primary, 1);
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&authority_shard)
            .expect("routed response authority shard")
    });
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/services/read-your-write.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.version = version.clone();
    object.content_hash = Some("blake3:routed-write-journal".to_string());
    object.content_size_bytes = Some(23);
    object.current_locations = vec![requester.clone(), primary.clone()];

    let mut primary_backend =
        open_root_local_metadata_backend(root.join("primary-metadata")).expect("primary metadata");
    primary_backend
        .put_file_system(&file_system)
        .expect("primary filesystem");
    primary_backend
        .put_object_and_version(
            &object,
            &object_version_record(&object, object.content_size_bytes),
        )
        .expect("primary object");
    let batch = primary_backend
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("primary journal")
        .expect("primary journal batch");

    let requester_metadata_root = root.join("requester-metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(
        open_root_local_metadata_backend(&requester_metadata_root).expect("requester metadata"),
    )
    .expect("requester dfs");
    dfs.writes.pending_routed_writes.insert(
        "dfs_net_req_journal".to_string(),
        test_pending_routed_write(
            "dfs_net_req_journal",
            &requester,
            &object_id,
            &primary,
            authority_shard,
        ),
    );

    dfs.handle_distributed_write_response(
        primary,
        DfsDistributedWriteResponse {
            request_id: "dfs_net_req_journal".to_string(),
            object_id: object_id.clone(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed {
                    intent_id: "dfs_net_req_journal".to_string(),
                    version: version.clone(),
                    content_hash: "blake3:routed-write-journal".to_string(),
                    repair_jobs: Vec::new(),
                },
            },
            metadata_journal_batches: vec![batch],
        },
    )
    .expect("committed response");
    drop(dfs);

    let reopened =
        open_root_local_metadata_backend(&requester_metadata_root).expect("reopened metadata");
    let stored = reopened
        .get_object_by_path(&file_system.file_system_id, "/services/read-your-write.txt")
        .expect("lookup persisted object")
        .expect("persisted object");
    assert_eq!(stored.object_id, object_id);
    assert_eq!(stored.version, version);
    assert_eq!(
        stored.content_hash.as_deref(),
        Some("blake3:routed-write-journal")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_rejects_late_retired_primary_journal_without_mutation() {
    let root = temp_root("routed-write-retired-primary-journal");
    let requester = NodeId::from_legacy_named("node_routed_retired_requester").expect("requester");
    let retired_primary =
        NodeId::from_legacy_named("node_routed_retired_primary").expect("retired primary");
    let current_primary =
        NodeId::from_legacy_named("node_routed_current_primary").expect("current primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_retired_primary").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_retired_primary").expect("version");
    let path = "/services/retired-primary-response.txt";
    let batch = test_routed_response_journal_batch(
        &root.join("source-metadata"),
        &retired_primary,
        &requester,
        &object_id,
        &version,
        path,
    );
    let dispatch_authority = test_routed_authority_shard(&retired_primary, 1);
    let current_authority = test_routed_authority_shard(&current_primary, 2);
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&current_authority)
            .expect("current authority shard")
    });
    let receiver_metadata_root = root.join("receiver-metadata");
    let mut dfs = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(
        open_root_local_metadata_backend(&receiver_metadata_root).expect("receiver metadata"),
    )
    .expect("receiver service");
    dfs.writes.pending_routed_writes.insert(
        "dfs_net_req_retired_primary".to_string(),
        test_pending_routed_write(
            "dfs_net_req_retired_primary",
            &requester,
            &object_id,
            &retired_primary,
            dispatch_authority,
        ),
    );

    let event = dfs
        .handle_distributed_write_response(
            retired_primary,
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_retired_primary".to_string(),
                object_id: object_id.clone(),
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed {
                        intent_id: "dfs_net_req_retired_primary".to_string(),
                        version,
                        content_hash: "blake3:routed-response-authority".to_string(),
                        repair_jobs: Vec::new(),
                    },
                },
                metadata_journal_batches: vec![batch],
            },
        )
        .expect("late routed response is handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { ref reason, .. }
            if reason.contains("no longer current")
    ));
    drop(dfs);
    assert!(
        open_root_local_metadata_backend(&receiver_metadata_root)
            .expect("reopen receiver metadata")
            .get_object(&object_id)
            .expect("read rejected object")
            .is_none(),
        "a late retired-primary response must not mutate requester metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_rejects_retired_primary_outcome_without_journal_batches() {
    let root = temp_root("routed-write-retired-primary-outcome-without-journal");
    let requester =
        NodeId::from_legacy_named("node_routed_retired_no_journal_requester").expect("requester");
    let retired_primary =
        NodeId::from_legacy_named("node_routed_retired_no_journal_primary").expect("primary");
    let object_id =
        DfsObjectId::from_legacy_named("obj_routed_retired_no_journal").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_routed_retired_no_journal").expect("version");
    let dispatch_authority = test_routed_authority_shard(&retired_primary, 1);
    let current_authority = test_routed_authority_shard(&requester, 2);
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&current_authority)
            .expect("current authority shard")
    });
    let mut dfs = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: requester.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane);
    let request_id = "dfs_net_req_retired_primary_without_journal";
    dfs.writes.pending_routed_writes.insert(
        request_id.to_string(),
        test_pending_routed_write(
            request_id,
            &requester,
            &object_id,
            &retired_primary,
            dispatch_authority,
        ),
    );

    let event = dfs
        .handle_distributed_write_response(
            retired_primary,
            DfsDistributedWriteResponse {
                request_id: request_id.to_string(),
                object_id,
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed {
                        intent_id: request_id.to_string(),
                        version,
                        content_hash: "blake3:routed-response-without-journal".to_string(),
                        repair_jobs: Vec::new(),
                    },
                },
                metadata_journal_batches: Vec::new(),
            },
        )
        .expect("late routed response is handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { ref reason, .. }
            if reason.contains("epoch 1 is no longer current")
    ));
    assert!(matches!(
        &dfs.writes.pending_routed_writes
            .get(request_id)
            .expect("tracked routed write")
            .result,
        Some(Err(DfsError::Distributed { message }))
            if message.contains("epoch 1 is no longer current")
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_remote_primary_nonforeground_wait_write_requests_bounded_response_journal_replay() {
    let root = temp_root("posix-remote-primary-nonforeground-wait-bounded-replay");
    let writer =
        NodeId::from_legacy_named("node_posix_remote_wait_bounded_replay_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_posix_remote_wait_bounded_replay_primary")
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
        let mut outbound_messages = Vec::new();
        if let DfsNetworkMessage::DistributedWriteRequest(write_command) =
            &command.routed_message.message
        {
            *captured_command_for_sender.lock().expect("capture") = Some(write_command.clone());
            let content_hash = content_hash(&write_command.bytes);
            outbound_messages.push(build_dfs_routed_message(
                &command.routed_message.target_node_id,
                &command.routed_message.source_node_id,
                command.routed_message.frame_id,
                command.routed_message.message_id + 1,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: write_command.request_id.clone(),
                    object_id: write_command.request.object_id.clone(),
                    result: DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed {
                            intent_id: "remote-primary-committed".to_string(),
                            version: write_command.request.target_version.clone(),
                            content_hash,
                            repair_jobs: Vec::new(),
                        },
                    },
                    metadata_journal_batches: Vec::new(),
                }),
            ));
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages,
        })
    });
    let transport =
        node_transport_with_attachment_sender(writer.clone(), vec![primary.clone()], sender);
    let path = "/routed-materialized-wait-nonforeground.bin";
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

    let bytes = b"materialized wait remote primary write should not replay journal".to_vec();
    let expected_hash = content_hash(&bytes);
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![writer.clone(), primary.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("committed routed write returns stat for existing object");

    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let command = captured_command
        .lock()
        .expect("capture")
        .clone()
        .expect("captured routed write command");
    assert_eq!(command.bytes, bytes);
    assert_eq!(command.desired_replica_nodes, vec![writer, primary]);
    assert!(
        !command.foreground_replica_repair,
        "test should exercise the nonforeground SIFS write mode"
    );
    assert!(
        command.metadata_journal_after_sequence.is_some(),
        "nonforeground wait-for-commit POSIX writes need bounded response journal replay for fresh readers"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_manifest_write_controls_response_journal_replay() {
    let root = temp_root("routed-manifest-sync-nonforeground-journal");
    let writer = NodeId::from_legacy_named("node_routed_manifest_sync_writer").expect("writer");
    let primary = NodeId::from_legacy_named("node_routed_manifest_sync_primary").expect("primary");
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
    object.path = "/routed-manifest-sync.bin".to_string();
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
        .insert(DfsMetadataShardId::new("root"), 42);

    dfs.route_distributed_manifest(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_manifest_sync")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        content_hash(b"routed-manifest-sync"),
        b"routed-manifest-sync".len() as u64,
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
    .expect("route synchronous nonforeground manifest write");
    dfs.route_distributed_manifest(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_manifest_sync_no_replay")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        content_hash(b"routed-manifest-sync-no-replay"),
        b"routed-manifest-sync-no-replay".len() as u64,
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
    .expect("route synchronous nonforeground manifest write without response journal");
    dfs.route_distributed_manifest(
        DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: DfsVersion::from_legacy_named("version_routed_manifest_detached")
                .expect("version"),
            requesting_node_id: writer.clone(),
        },
        Some(object.version.clone()),
        content_hash(b"routed-manifest-detached"),
        b"routed-manifest-detached".len() as u64,
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
    .expect("route detached nonforeground manifest write");
    dfs.flush_detached_transport_sends()
        .expect("flush detached routed write");

    let commands = captured_commands.lock().expect("capture").clone();
    assert_eq!(commands.len(), 3);
    assert_eq!(
        commands[0].metadata_journal_after_sequence,
        Some(42),
        "synchronous routed manifest writes must request response journal replay from the cached applied cursor"
    );
    assert!(
        commands[1].metadata_journal_after_sequence.is_none(),
        "synchronous routed manifest writes may wait for commit without response journal replay"
    );
    assert!(
        commands[2].metadata_journal_after_sequence.is_none(),
        "detached routed manifest writes should skip response journal replay"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_nonforeground_response_replay_is_one_batch() {
    let root = temp_root("routed-write-nonforeground-one-batch");
    let requester =
        NodeId::from_legacy_named("node_routed_write_one_batch_requester").expect("requester");
    let primary =
        NodeId::from_legacy_named("node_routed_write_one_batch_primary").expect("primary");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![requester.clone()],
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

    let responses = Arc::new(Mutex::new(Vec::<DfsDistributedWriteResponse>::new()));
    let responses_for_sender = Arc::clone(&responses);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        match &command.routed_message.message {
            DfsNetworkMessage::DistributedWriteResponse(response) => {
                responses_for_sender
                    .lock()
                    .expect("responses")
                    .push(response.clone());
            }
            DfsNetworkMessage::MetadataJournalBatch(batch) => {
                return Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: vec![DfsRoutedMessage {
                        source_node_id: command.routed_message.target_node_id.clone(),
                        target_node_id: command.routed_message.source_node_id.clone(),
                        frame_id: command.routed_message.frame_id.saturating_add(100),
                        message_id: command.routed_message.message_id.saturating_add(100),
                        message: DfsNetworkMessage::MetadataJournalBatchResponse(
                            DfsMetadataJournalBatchResponse {
                                request_id: batch.request_id.clone(),
                                shard_id: batch.batch.shard_id.clone(),
                                next_after_sequence: batch.batch.next_after_sequence,
                                applied_entries: applied_metadata_journal_command_entry_count(
                                    batch,
                                ),
                                verified_prefix_only: batch.verify_prefix_only,
                                verified_authority_probe_id:
                                    applied_metadata_journal_command_confirmation(batch),
                                result: DfsMetadataJournalBatchResult::Applied,
                            },
                        ),
                    }],
                });
            }
            _ => {}
        }
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeTransport::with_connect_timeout(
        primary.clone(),
        vec![requester.clone()],
        Duration::from_millis(1),
        sender,
    );
    let mut dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(
        open_local_sharded_metadata_backend(root.join("metadata"), vec![shard])
            .expect("metadata backend"),
    )
    .expect("attach metadata backend");
    dfs.add_file_system(file_system.clone())
        .expect("primary filesystem");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");

    for index in 0..(DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 8) {
        let mut filler = file_metadata(primary.clone());
        filler.object_id = DfsObjectId::from_legacy_named(format!("obj_one_batch_filler_{index}"))
            .expect("filler object");
        filler.file_system_id = file_system.file_system_id.clone();
        filler.path = format!("/filler-{index:04}.txt");
        filler.policy = DfsObjectPolicy::ReplicatedEventual;
        filler.current_locations = vec![primary.clone()];
        dfs.add_object_metadata(filler).expect("filler metadata");
    }

    let mut object = file_metadata(primary.clone());
    object.object_id =
        DfsObjectId::from_legacy_named("obj_routed_write_one_batch").expect("target object");
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/one-batch-target.txt".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![requester.clone()];
    dfs.add_object_metadata(object.clone())
        .expect("target metadata");

    dfs.handle_distributed_write_request(
        requester.clone(),
        DfsDistributedWriteCommand {
            publication: Default::default(),
            request_id: "dfs_net_req_one_batch".to_string(),
            request: DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: crate::namespace::next_version(&object.version),
                requesting_node_id: requester.clone(),
            },
            expected_base_version: Some(object.version.clone()),
            metadata_journal_after_sequence: Some(0),
            bytes: b"bounded nonforeground response replay".to_vec(),
            content_hash: None,
            content_size_bytes: None,
            requester_blob_pre_staged: true,
            desired_replica_nodes: vec![requester, primary],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            object_metadata: Some(object),
            authority_shard: None,
            touch_parent_on_create: false,
            foreground_replica_repair: false,
            exclusive_create: false,
        },
    )
    .expect("serve routed write");

    let responses = responses.lock().expect("responses").clone();
    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0].metadata_journal_batches.len(),
        DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT,
        "nonforeground routed write responses must not ship a deep catch-up run"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_current_manifest_blob_returns_pending_replication() {
    let root = temp_root("read-missing-current-manifest-blob");
    let node_id = NodeId::from_legacy_named("node_missing_blob").expect("node");
    let replica = NodeId::from_legacy_named("node_missing_blob_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_missing_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = vec![b'm'; 17 * 1024];
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), &bytes)
        .expect("seed manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("content").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");
    let mut metadata = file_metadata(node_id);
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![replica.clone()];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let outcome = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("missing blob is treated as cache miss");

    assert!(matches!(
        outcome,
        NetworkedDfsReadOutcome::PendingReplication {
            object_id: pending_object_id,
            version: pending_version,
            content_hash,
            current_locations,
        } if pending_object_id == object_id
            && pending_version == version
            && content_hash.as_deref() == Some(manifest.content_hash.as_str())
            && current_locations == vec![replica]
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_conditional_replace_rejects_a_stale_editor_snapshot() {
    let root = temp_root("posix-conditional-replace-stale-editor");
    let node_id = NodeId::from_legacy_named("node_conditional_replace").expect("node");
    let path = "/documents/notes.txt";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/documents").expect("mkdir");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let opened = dfs
        .posix_write_file(path, b"opened", options.clone())
        .expect("create file");

    dfs.posix_write_file(path, b"newer", options.clone())
        .expect("concurrent write");
    let error = dfs
        .posix_replace_file_if_version(
            path,
            &opened.object_id,
            &opened.version,
            b"stale editor contents",
            options.clone(),
        )
        .expect_err("stale editor save must fail");
    assert!(matches!(error, DfsPosixError::Conflict { .. }));
    assert_eq!(
        dfs.posix_read_file(path).expect("read newer version"),
        b"newer"
    );

    let current = dfs.posix_stat(path).expect("current stat");
    let saved = dfs
        .posix_replace_file_if_version(
            path,
            &current.object_id,
            &current.version,
            b"saved safely",
            options,
        )
        .expect("matching conditional save");
    assert_ne!(saved.version, current.version);
    assert_eq!(
        dfs.posix_read_file(path).expect("read saved version"),
        b"saved safely"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn receipt_replay_publishes_a_missing_location_then_becomes_exactly_idempotent() {
    let root = temp_root("metadata-receipt-partial-location-replay");
    let owner = NodeId::from_legacy_named("node_receipt_partial_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_receipt_partial_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_receipt_partial").expect("object");
    let version = DfsVersion::from_legacy_named("version_receipt_partial").expect("version");
    let content_hash = "blake3:receipt-partial";

    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = "/receipt-partial.bin".to_string();
    object.version = version.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    object.content_hash = Some(content_hash.to_string());
    object.content_size_bytes = Some(1);
    dfs.add_object_metadata(object.clone())
        .expect("visible object");
    dfs.put_visible_object_in_metadata_backend(&object)
        .expect("durable visible object");

    // Model a crash after the receipt commit but before its separately
    // journaled visible-location update.
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                committed_version: version.clone(),
                owner_node_id: owner,
                event_id: EventId::generate(),
            },
            replica_node_id: replica.clone(),
            content_hash: Some(content_hash.to_string()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("partial durable receipt");
    let journal_sequence_before_location = dfs
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata shard status")[0]
        .journal_last_sequence;

    let committed_work = DfsCommittedWorkTracker::new();
    dfs.set_committed_work_tracker(committed_work.clone());
    assert!(
        !dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("repair partial receipt"),
        "the receipt itself was already durable"
    );
    let journal_sequence_after_location = dfs
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata shard status")[0]
        .journal_last_sequence;
    assert_eq!(
        journal_sequence_after_location,
        journal_sequence_before_location + 1,
        "repairing the missing location must append exactly one journal entry"
    );
    assert!(
        committed_work.take().publish_metadata,
        "the location-only journal append must retain its publication edge"
    );

    assert!(
        !dfs.put_metadata_receipt(
            &sifs_file_system_id(),
            &object_id,
            &version,
            &replica,
            content_hash,
        )
        .expect("exact receipt replay")
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .expect("metadata backend")
            .metadata_shard_statuses()
            .expect("metadata shard status")[0]
            .journal_last_sequence,
        journal_sequence_after_location,
        "an exact replay must not append another location transaction"
    );
    assert_eq!(committed_work.take(), DfsCommittedWork::default());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sharded_point_lookup_snapshot_refreshes_only_the_routed_path_shard() {
    let root = temp_root("sifs-sharded-point-lookup-snapshot");
    let node = NodeId::from_legacy_named("node_sifs_sharded_point_snapshot").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let mut backend =
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("metadata");
    let file_system_id = sifs_file_system_id();

    crate::metadata_sharded_backend::reset_sharded_consistent_read_snapshot_shards();
    backend
        .begin_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent")
        .expect("path-scoped snapshot");
    backend.end_consistent_read_snapshot_for_path(&file_system_id, "/services/point-lookup-parent");

    assert_eq!(
        crate::metadata_sharded_backend::sharded_consistent_read_snapshot_shards(),
        vec![
            DfsMetadataShardId::new("services"),
            DfsMetadataShardId::new("root"),
        ],
        "one-path response hydration must retain the routing fallback without refreshing the unrelated users shard"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_replica_catches_up_after_journal_publish() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-catchup");
    let path = "/users/replicated.txt";
    let bytes = b"observer should converge to this content";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // The observer never participated in the write; it learns the namespace
    // through the primary's metadata journal publication.
    assert!(
        matches!(
            cluster.observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "observer must not see the path before metadata publication"
    );

    let expected_hash = content_hash(bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, path, |cluster| {
        cluster
            .observer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    let stat = cluster.observer.posix_stat(path).expect("observer stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list /users");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "observer directory listing must contain the replicated path: {entries:?}"
    );

    // Content follows metadata on demand.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.observer, path);
    assert_eq!(read, bytes.to_vec());

    // Publication is idempotent: republishing must not error or regress state.
    cluster
        .primary
        .replicate_configured_metadata_shards(&sifs_file_system_id())
        .expect("idempotent metadata journal publish");
    sifs_posix_cluster_pump_round(&mut cluster);
    let stat = cluster
        .observer
        .posix_stat(path)
        .expect("observer stat after republish");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(expected_hash.as_str()),
        "republishing the journal must not regress replicated metadata"
    );

    // The writer (also a configured replica) already replayed these journal
    // entries from the routed-write response; the publish stream must remain
    // consistent for it as well.
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        bytes.to_vec()
    );

    assert!(
        cluster.rejections.is_empty(),
        "no journal batch may be rejected during routed writes and catch-up: {:?}",
        cluster.rejections
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn routed_namespace_without_local_replay_omits_response_journal_batches() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-no-replay-batches");
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        mut services_primary,
        replica,
        ..
    } = cluster;
    let request_id = "dfs_net_req_namespace_no_replay".to_string();

    let event = services_primary
        .handle_namespace_request(
            replica_node.clone(),
            DfsNamespaceCommand {
                request_id: request_id.clone(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: replica_node,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::MkdirAll {
                    path: "/services/no-replay-batches".to_string(),
                },
            },
        )
        .expect("serve no-replay namespace request");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            ..
        }
    ));

    let mut replica_transport = replica.into_transport();
    let responses = drain_dfs_messages(&mut replica_transport)
        .into_iter()
        .filter_map(|message| match message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(response),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].request_id, request_id);
    assert!(matches!(responses[0].result, DfsNamespaceResult::Applied));
    assert!(
        responses[0].metadata_journal_batches.is_empty(),
        "no-local-replay namespace response must not carry journal batches"
    );
    assert!(
        responses[0].synced_path_object.is_none() && responses[0].synced_path_objects.is_empty(),
        "no-local-replay namespace response must not refresh and return path objects"
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn durable_namespace_exact_retry_replays_completed_rename_and_rejects_id_reuse() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-rename-replay");
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
    let source = "/services/replay-source.txt";
    let destination = "/services/replay-destination.txt";
    let second_destination = "/services/replay-second-destination.txt";
    let bytes = b"namespace replay payload";
    let primary_node = services_primary.node_id().clone();
    services_primary
        .posix_write_file(
            source,
            bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_node]),
        )
        .expect("seed source on services primary");

    let request_id = "dfs_net_req_retried_rename".to_string();
    let command = DfsNamespaceCommand {
        request_id: request_id.clone(),
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
    let first = services_primary
        .handle_namespace_request(replica_node.clone(), command.clone())
        .expect("serve first rename");
    assert!(matches!(
        first,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            mutates_metadata: true,
            ..
        }
    ));
    let destination_version = services_primary
        .posix_stat(destination)
        .expect("destination after first rename")
        .version;
    let durable_request = services_primary
        .metadata_backend
        .as_ref()
        .expect("metadata backend")
        .get_namespace_request(&replica_node, &request_id)
        .expect("durable namespace request lookup")
        .expect("completed namespace request record");
    assert_eq!(durable_request.status, DfsNamespaceRequestStatus::Completed);
    assert_eq!(durable_request.result, Some(DfsNamespaceResult::Applied));

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

    let mut replay_command = command;
    replay_command.include_metadata_journal_batches = false;
    replay_command.metadata_journal_after_sequences = vec![DfsNamespaceShardCursor {
        shard_id: DfsMetadataShardId::new("services"),
        after_sequence: u64::MAX,
    }];
    let replay = services_primary
        .handle_namespace_request(replica_node.clone(), replay_command)
        .expect("replay identical rename");
    assert!(matches!(
        replay,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Applied,
            mutates_metadata: false,
            ..
        }
    ));
    assert!(matches!(
        services_primary.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let replayed_destination = services_primary
        .posix_stat(destination)
        .expect("destination remains after replay");
    assert_eq!(replayed_destination.version, destination_version);
    assert_eq!(
        services_primary
            .posix_read_file(destination)
            .expect("read replayed destination"),
        bytes
    );

    let distinct_same_id = DfsNamespaceCommand {
        request_id,
        file_system_id: sifs_file_system_id(),
        requesting_node_id: replica_node.clone(),
        include_metadata_journal_batches: true,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::Rename {
            from: destination.to_string(),
            to: second_destination.to_string(),
        },
    };
    let distinct = services_primary
        .handle_namespace_request(replica_node, distinct_same_id)
        .expect("same request id with a distinct command must be rejected");
    assert!(matches!(
        distinct,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected { ref reason, .. },
            mutates_metadata: false,
            ..
        } if reason.contains("already used for a different command")
    ));
    assert!(matches!(
        services_primary.posix_stat(second_destination),
        Err(DfsPosixError::NotFound { .. })
    ));

    drop(replica);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn routed_namespace_cursor_snapshot_uses_cached_state_not_backend_status() {
    let mut cluster = sifs_two_primary_cluster("sifs-namespace-cached-cursors");
    let services_shard = DfsMetadataShardId::new("services");
    cluster
        .replica
        .replication
        .local_metadata_journal_cursors
        .clear();
    assert!(
        cluster
            .replica
            .local_metadata_shard_cursors()
            .expect("cached cursors")
            .is_empty(),
        "routed namespace sends must not scan backend shard status when the cache is empty"
    );

    cluster
        .replica
        .replication
        .local_metadata_journal_cursors
        .insert(services_shard.clone(), 42);
    let cursors = cluster
        .replica
        .local_metadata_shard_cursors()
        .expect("cached cursor snapshot");
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].shard_id, services_shard);
    assert_eq!(cursors[0].after_sequence, 42);

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn namespace_rename_shard_selection_uses_one_config_snapshot_for_both_paths() {
    let root = temp_root("namespace-rename-shard-selection-one-snapshot");
    let users_primary =
        NodeId::from_legacy_named("node_namespace_snapshot_users").expect("users primary");
    let services_primary =
        NodeId::from_legacy_named("node_namespace_snapshot_services").expect("services primary");
    let requester =
        NodeId::from_legacy_named("node_namespace_snapshot_requester").expect("requester");
    let shards = sifs_two_primary_shard_configs(
        &users_primary,
        &services_primary,
        1,
        vec![requester.clone()],
        vec![requester.clone()],
    );
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }

    let range_prefix_calls = Arc::new(AtomicUsize::new(0));
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(requester),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(CountingDfsControlPlaneKvClient::new(
        kv,
        Arc::clone(&range_prefix_calls),
    ));

    let affected = dfs
        .affected_namespace_shards(
            &sifs_file_system_id(),
            &DfsNamespaceOperation::Rename {
                from: "/users/source.txt".to_string(),
                to: "/services/destination.txt".to_string(),
            },
        )
        .expect("rename shard selection");
    let affected_ids = affected
        .iter()
        .map(|shard| shard.shard_id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(affected_ids, BTreeSet::from(["services", "users"]));
    assert_eq!(
        range_prefix_calls.load(Ordering::SeqCst),
        1,
        "all rename paths and parents must be selected from one immutable config snapshot"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sync_path_response_object_updates_outsider_read_view_without_journal_batches() {
    let root = temp_root("sync-path-response-object");
    let local = NodeId::from_legacy_named("node_sync_path_response_local").expect("local");
    let primary = NodeId::from_legacy_named("node_sync_path_response_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_sync_path_response").expect("object");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: local.clone(),
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    dfs.add_file_system(distributed_sifs(primary.clone()))
        .expect("filesystem");
    dfs.pending_routed_namespace_ops.insert(
        "dfs_net_req_sync_path".to_string(),
        PendingRoutedNamespaceOp {
            primary_node_id: primary.clone(),
            dispatch_authority_shard: None,
            authority_shards: Vec::new(),
            replay_journal_batches: true,
            apply_synced_path_object: true,
            result: None,
        },
    );

    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: sifs_file_system_id(),
        path: "/users/late-visible.txt".to_string(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: primary.clone(),
        current_locations: vec![primary.clone()],
        content_hash: Some("blake3:sync-path-response".to_string()),
        content_size_bytes: Some(32),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_sync_path_response").expect("version"),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };

    let event = dfs
        .handle_namespace_response(
            primary,
            DfsNamespaceResponse {
                request_id: "dfs_net_req_sync_path".to_string(),
                result: DfsNamespaceResult::Applied,
                synced_path_object: Some(object.clone()),
                synced_path_objects: Vec::new(),
                metadata_journal_batches: Vec::new(),
                metadata_journal_has_more: None,
                posix_coordination_result: None,
            },
        )
        .expect("sync response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceCompleted { .. }
    ));
    assert_eq!(
        dfs.metadata()
            .object_by_normalized_posix_path(&sifs_file_system_id(), "/users/late-visible.txt")
            .map(|stored| (&stored.object_id, stored.content_hash.as_deref())),
        Some((&object_id, Some("blake3:sync-path-response")))
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_routed_op_replays_deep_catch_up_across_batches() {
    // A requester far behind the primary's journal (more entries than one
    // batch carries) must still read its own routed mutation immediately:
    // responses carry a bounded run of batches and the wrapper pulls more.
    let cluster = sifs_two_primary_cluster("sifs-xshard-deepsync");
    let SifsTwoPrimaryCluster {
        root,
        replica_node: _,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let mut users_primary = users_primary;

    // Build a deep journal on the users primary locally (no publication).
    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![users_primary.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    for index in 0..700 {
        users_primary
            .posix_write_file(
                &format!("/users/bulk-{index:03}.txt"),
                format!("bulk payload {index}").as_bytes(),
                local_options.clone(),
            )
            .expect("bulk create");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    // One routed namespace op must catch the replica up through the entire
    // backlog, not just the first bounded batch.
    replica
        .posix_mkdir("/users/deep-sync-marker")
        .expect("routed mkdir with deep catch-up");
    assert!(
        replica.posix_stat("/users/bulk-000.txt").is_ok(),
        "earliest backlog entry must be visible after the routed op"
    );
    assert!(
        replica.posix_stat("/users/bulk-699.txt").is_ok(),
        "latest backlog entry must be visible after the routed op"
    );
    assert!(replica.posix_stat("/users/deep-sync-marker").is_ok());

    stop.store(true, Ordering::SeqCst);
    users_thread.join().expect("users thread");
    services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_mkdir_all_replays_deep_catch_up_across_batches() {
    // `mkdir_all` must provide the same immediate local visibility as `mkdir`.
    // The routed primary can only include a bounded journal run in the first
    // response, so the wrapper has to pull the rest before returning.
    let cluster = sifs_two_primary_cluster("sifs-xshard-mkdir-all-deepsync");
    let SifsTwoPrimaryCluster {
        root,
        replica_node: _,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let mut users_primary = users_primary;

    let local_options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        replica_candidates: vec![users_primary.node_id().clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    for index in 0..700 {
        users_primary
            .posix_write_file(
                &format!("/users/mkdir-all-backlog-{index:03}.txt"),
                format!("bulk payload {index}").as_bytes(),
                local_options.clone(),
            )
            .expect("bulk create");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let users_stop = Arc::clone(&stop);
    let users_thread = std::thread::spawn(move || {
        let mut users_primary = users_primary;
        while !users_stop.load(Ordering::SeqCst) {
            users_primary
                .process_incoming()
                .expect("users primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        users_primary
    });
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_mkdir_all("/users/deep-mkdir-all/leaf")
        .expect("routed mkdir_all with deep catch-up");
    assert!(
        replica
            .posix_stat("/users/mkdir-all-backlog-000.txt")
            .is_ok(),
        "earliest backlog entry must be visible after routed mkdir_all"
    );
    assert!(
        replica
            .posix_stat("/users/mkdir-all-backlog-699.txt")
            .is_ok(),
        "latest backlog entry must be visible after routed mkdir_all"
    );
    assert!(replica.posix_stat("/users/deep-mkdir-all").is_ok());
    assert!(replica.posix_stat("/users/deep-mkdir-all/leaf").is_ok());

    stop.store(true, Ordering::SeqCst);
    users_thread.join().expect("users thread");
    services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_mkdir_all_under_services_replays_created_chain() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-mkdir-all-services-chain");
    let SifsTwoPrimaryCluster {
        root,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_mkdir_all("/services/cluster-tests/latency-regression")
        .expect("routed services mkdir_all");
    assert!(
        replica.posix_stat("/services/cluster-tests").is_ok(),
        "mkdir_all must sync the intermediate directory"
    );
    assert!(
        replica
            .posix_stat("/services/cluster-tests/latency-regression")
            .is_ok(),
        "mkdir_all must sync the created leaf directory"
    );

    stop.store(true, Ordering::SeqCst);
    services_thread.join().expect("services thread");
    std::fs::remove_dir_all(&root).ok();
}
