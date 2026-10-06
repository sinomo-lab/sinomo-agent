//! Bootstrap regression tests.

use super::*;

#[test]
fn clustered_service_rejects_public_local_metadata_publication_apis() {
    let root = temp_root("clustered-local-publication-rejected");
    let local = NodeId::from_legacy_named("node_clustered_local_publication").expect("local");
    let object = file_metadata(local.clone());
    let object_id = object.object_id.clone();
    let version = object.version.clone();
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(local),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(SharedDfsControlPlaneKvClient::new(InProcessKvStore::new()));

    for error in [
        dfs.put_local_object_version(object.clone(), b"local bytes")
            .expect_err("clustered byte publication must use canonical write"),
        dfs.put_local_object_version_from_reader(
            object,
            std::io::Cursor::new(b"reader bytes".to_vec()),
        )
        .expect_err("clustered reader publication must use canonical write"),
    ] {
        assert!(
            matches!(error, DfsError::Distributed { ref message } if message.contains("standalone-only")),
            "unexpected clustered local-publication error: {error}"
        );
    }
    assert!(
        dfs.content()
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "rejected clustered publication must not stage content before authorization"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_message_command_processor_returns_none_without_cluster_state() {
    let root = temp_root("cluster_dfs_message_command_orphan");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_cluster_message_command_orphan_local").expect("local");
    let command = DfsMessageCommand {
        request_id: "dfs_message_command_orphan".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "orphan-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };

    let result = process_cluster_dfs_message_command(&data_dir, &cluster_paths, command, None)
        .expect("cluster DFS message command");

    assert!(result.is_none());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_message_handler_returns_none_without_cluster_state() {
    let root = temp_root("cluster_dfs_message_handler_orphan");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_cluster_message_handler_orphan_local").expect("local");
    let command = DfsMessageCommand {
        request_id: "dfs_message_handler_orphan".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "orphan-handler-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };
    let peer = Arc::new(RecordingPeerTransport::default());
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let mut reports = Vec::new();
    let mut errors = Vec::new();

    let result = run_cluster_dfs_message_command_with_metadata_publish(
        &data_dir,
        &cluster_paths,
        command.clone(),
        peer_transport,
        None,
        DfsMetadataJournalReplicationOptions::default(),
        |report| reports.push(report),
        |error| errors.push(error),
    )
    .expect("run handler");

    assert!(result.is_none());
    assert!(reports.is_empty());
    assert!(errors.is_empty());
    assert!(peer.calls.lock().expect("calls").is_empty());

    assert!(peer.calls.lock().expect("calls").is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_message_command_processor_uses_cluster_local_node_id() {
    let root = temp_root("cluster_message_command_wrong_target");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_message_command")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_cluster_message_local").expect("local");
    let remote = NodeId::from_legacy_named("node_cluster_message_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "dfs message command",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0] = sinomo_core::cluster::local_cluster_state_authority_member(
        state.cluster_id.clone(),
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("cluster kv state");
    let command = DfsMessageCommand {
        request_id: "message-command-cluster-wrong-target".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: remote.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "cluster-wrong-target-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };

    let error = process_cluster_dfs_message_command(&data_dir, &cluster_paths, command, None)
        .expect_err("wrong target rejected");

    let error = error.to_string();
    assert!(
        error.contains(local.as_str()),
        "local id missing from: {error}"
    );
    assert!(
        error.contains(remote.as_str()),
        "remote id missing from: {error}"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_smoke_opens_service_and_publishes_metadata() {
    let root = temp_root("cluster-smoke-helper");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_smoke_helper")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_smoke_helper_local").expect("local");
    let replica = NodeId::from_legacy_named("node_dfs_smoke_helper_replica").expect("replica");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs smoke helper",
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

    let message_sender: DfsMessageSender = Arc::new(|_target_node_id, _command, _timeout| {
        Err(DfsPeerTransportError::new(
            "message sender should not be needed",
        ))
    });
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
        Arc::new(|_target_node_id, _command, _timeout| {
            Err(DfsMetadataPeerTransportError::new(
                "snapshot install should not be needed",
            ))
        });

    let report = run_cluster_dfs_smoke(
        &data_dir,
        &cluster_paths,
        message_sender,
        sender,
        snapshot_sender,
        None,
        DfsSmokeOptions {
            nonce: Some(24680),
            ..DfsSmokeOptions::default()
        },
    )
    .expect("cluster smoke")
    .expect("cluster smoke report");

    assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.committed_replica_node_ids, vec![local]);
    assert_eq!(report.pending_repair_count, 0);
    assert_eq!(report.attempts.len(), 1);
    assert!(report.attempts[0].committed);
    assert!(report.path.starts_with("/dfs-smoke/24680-"));
    let sent = sent.lock().expect("sent lock");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, replica.to_string());
    assert!(
        sent[0].1 > 0,
        "cluster smoke should publish metadata created by the smoke write"
    );
    assert!(matches!(
        report.metadata_replication.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::Applied {
            target_node_id,
            shard_id,
            entries,
            ..
        }] if target_node_id == &replica
            && shard_id == &DfsMetadataShardId::new("root")
            && *entries == sent[0].1
            && *entries > 0
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_cluster_smoke_bootstraps_dfs_before_running_smoke() {
    let root = temp_root("default-cluster-smoke-bootstrap");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_default_smoke")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_default_smoke_local").expect("local");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "dfs default smoke",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("cluster kv state");

    let message_sender: DfsMessageSender = Arc::new(|_target_node_id, _command, _timeout| {
        Err(DfsPeerTransportError::new(
            "single-node default smoke should not send DFS messages",
        ))
    });
    let sender: DfsMetadataJournalBatchSender = Arc::new(|_target_node_id, _command, _timeout| {
        Err(DfsMetadataPeerTransportError::new(
            "single-node default smoke should not replicate metadata",
        ))
    });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|_target_node_id, _command, _timeout| {
            Err(DfsMetadataPeerTransportError::new(
                "single-node default smoke should not install metadata snapshots",
            ))
        });

    let report = run_default_cluster_dfs_smoke(
        &data_dir,
        &cluster_paths,
        message_sender,
        sender,
        snapshot_sender,
        Box::new(ClusterPathDfsControlPlaneKvClient::new(&cluster_paths)),
        Some(Box::new(ClusterPathDfsControlPlaneKvClient::new(
            &cluster_paths,
        ))),
        DfsDefaultClusterSmokeOptions {
            bootstrap: DfsDefaultClusterBootstrapOptions {
                publish_cluster_control: true,
                materialize_local_metadata: true,
                local_zone: None,
                local_content_advertisement: None,
            },
            smoke: DfsSmokeOptions {
                nonce: Some(13579),
                ..DfsSmokeOptions::default()
            },
        },
    )
    .expect("default cluster smoke")
    .expect("default cluster smoke report");

    assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.committed_replica_node_ids, vec![local.clone()]);
    assert!(report.path.starts_with("/dfs-smoke/13579-"));
    assert!(
        !sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            content_services_prefix(),
        )
        .expect("content service advertisements")
        .is_empty(),
        "default smoke should bootstrap DFS service advertisements before running"
    );
    assert!(
        !sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &metadata_shards_prefix(&sifs_file_system_id()),
        )
        .expect("metadata shard config")
        .is_empty(),
        "default smoke should bootstrap DFS metadata shard config before running"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cluster_dfs_metadata_publish_after_events_uses_default_cluster_publish() {
    let root = temp_root("cluster-publish-after-events");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let events = [NetworkedDfsServiceEvent::RemoteNamespaceServed {
        request_id: "namespace_cluster_publish_after_events".to_string(),
        requester_node_id: NodeId::from_legacy_named("node_cluster_publish_after_events").unwrap(),
        result: DfsNamespaceResult::Applied,
        mutates_metadata: true,
    }];
    let metadata_sender: DfsMetadataJournalBatchSender =
        Arc::new(|_, _, _| panic!("orphan cluster publish should not send metadata batches"));
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|_, _, _| panic!("orphan cluster publish should not install metadata snapshots"));
    let mut reports = Vec::new();
    let mut errors = Vec::new();

    assert!(run_cluster_dfs_metadata_publish_after_events(
        &data_dir,
        &cluster_paths,
        metadata_sender,
        snapshot_sender,
        &events,
        DfsMetadataJournalReplicationOptions::default(),
        |report| reports.push(report),
        |error| errors.push(error),
    ));

    assert_eq!(
        reports,
        vec![DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        }]
    );
    assert!(errors.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cold_sifs_scale_fixture_stays_preemptible() {
    let root = temp_root("cold-sifs-scale-preemptible");
    let node = NodeId::from_legacy_named("node_cold_sifs_scale").expect("node");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    backend
        .put_file_system(&DfsFileSystem::sifs(node.clone()))
        .expect("filesystem");
    for index in 0..2_048 {
        backend
            .put_object(&directory_object(
                &format!("/services/scale-fixture-{index:04}"),
                node.clone(),
            ))
            .expect("scale fixture object");
    }

    let mut service = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(node),
        LocalContentStore::new(root.join("content")).expect("content store"),
    );
    service.set_metadata_backend_unhydrated(backend);
    let cancellation_checks = AtomicUsize::new(0);
    let completed = service
        .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(&|| {
            cancellation_checks.fetch_add(1, Ordering::SeqCst) >= 1
        })
        .expect("cancellable scale hydration");

    assert!(!completed);
    assert!(
        cancellation_checks.load(Ordering::SeqCst) >= 2,
        "a cold hydration must yield to foreground cancellation between durable phases even at scale"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_opened_object_ranges_allow_one_atime_update_for_many_chunks() {
    let root = temp_root("sifs-opened-range-atime");
    let node_id = NodeId::from_legacy_named("node_sifs_opened_range_atime").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_range_atime/work/file.txt";
    let bytes = b"content split across several FUSE reads";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_range_atime/work")
        .expect("mkdir");
    let written = dfs
        .posix_write_file(
            file_path,
            bytes,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old times");
    let before = dfs.posix_stat(file_path).expect("before stat");

    assert_eq!(
        dfs.posix_read_file_range_for_object(&written.object_id, 0, 7)
            .expect("first no-atime range"),
        &bytes[..7]
    );
    let after_no_atime = dfs.posix_stat(file_path).expect("no-atime stat");
    assert_eq!(after_no_atime.version, before.version);
    assert_eq!(after_no_atime.accessed_at_unix_nanos, 12_345);

    assert_eq!(
        dfs.posix_read_file_range_for_object_update_access(&written.object_id, 7, 9)
            .expect("first handle range updates atime"),
        &bytes[7..16]
    );
    let after_access = dfs.posix_stat(file_path).expect("updated atime stat");
    assert_ne!(after_access.version, before.version);
    assert!(after_access.accessed_at_unix_nanos > 12_345);
    assert_eq!(after_access.modified_at_unix_nanos, 67_890);

    assert_eq!(
        dfs.posix_read_file_range_for_object(&written.object_id, 16, 8)
            .expect("subsequent no-atime range"),
        &bytes[16..24]
    );
    let after_next_chunk = dfs.posix_stat(file_path).expect("next chunk stat");
    assert_eq!(
        after_next_chunk.version, after_access.version,
        "subsequent chunks must not append authoritative atime metadata"
    );

    let renamed_path = "/services/sandbox/instances/sandbox_range_atime/work/file-renamed.txt";
    dfs.posix_rename(file_path, renamed_path)
        .expect("rename opened file");
    assert_eq!(
        dfs.posix_read_file_range_for_opened_object(renamed_path, &written.object_id, 5, 6, false,)
            .expect("renamed opened-object range"),
        &bytes[5..11]
    );

    let local_node_id = dfs.node_id().clone();
    dfs.posix_write_file(
        file_path,
        b"replacement",
        DfsWriteOptions::replicated_eventual(vec![local_node_id]),
    )
    .expect("pathname replacement");
    assert!(matches!(
        dfs.posix_read_file_range_for_opened_object(file_path, &written.object_id, 0, 4, false,),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file_range_for_opened_object(renamed_path, &written.object_id, 0, 4, false,)
            .expect("old handle keeps renamed identity"),
        &bytes[..4]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_bulk_local_eventual_splits_mixed_primary_batch() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-mixed-primary");
    let local_dir = "/users/bulk-mixed";
    let remote_dir = "/services/bulk-mixed";
    let local_path = "/users/bulk-mixed/local.txt";
    let remote_path = "/services/bulk-mixed/remote.txt";
    let local_bytes = b"bulk bytes owned by the local users shard".as_slice();
    let remote_bytes = b"bulk bytes owned by the remote services shard".as_slice();

    cluster
        .users_primary
        .posix_mkdir_all(local_dir)
        .expect("local bulk parent");
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "mixed bulk parents", |cluster| {
        cluster.users_primary.posix_stat(remote_dir).is_ok()
            && cluster.services_primary.posix_stat(local_dir).is_ok()
            && cluster.replica.posix_stat(local_dir).is_ok()
            && cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        mut users_primary,
        services_primary,
        replica,
        rejections,
    } = cluster;

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    let stats = users_primary
        .posix_write_files_local_eventual_with_known_parents(vec![
            (local_path, local_bytes),
            (remote_path, remote_bytes),
        ])
        .expect("mixed-primary bulk write");
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].path, local_path);
    assert_eq!(
        stats[0].content_hash.as_deref(),
        Some(content_hash(local_bytes).as_str())
    );
    assert_eq!(stats[1].path, remote_path);
    assert_eq!(
        stats[1].content_hash.as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );
    assert_eq!(
        users_primary
            .posix_read_file(local_path)
            .expect("read local bulk file"),
        local_bytes
    );
    assert_eq!(
        users_primary
            .posix_stat_distributed(remote_path)
            .expect("remote stat through primary")
            .content_hash
            .as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    assert_eq!(
        services_primary
            .posix_stat(remote_path)
            .expect("remote primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(remote_bytes).as_str())
    );
    assert_eq!(
        users_primary
            .posix_stat(local_path)
            .expect("local primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(local_bytes).as_str())
    );

    let mut cluster = SifsTwoPrimaryCluster {
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections,
    };
    sifs_two_primary_publish_until(&mut cluster, "mixed bulk convergence", |cluster| {
        cluster.replica.posix_stat(local_path).is_ok_and(|stat| {
            stat.content_hash.as_deref() == Some(content_hash(local_bytes).as_str())
        }) && cluster.replica.posix_stat(remote_path).is_ok_and(|stat| {
            stat.content_hash.as_deref() == Some(content_hash(remote_bytes).as_str())
        })
    });
    assert!(
        cluster.rejections.is_empty(),
        "mixed-primary bulk write must not fork metadata journals: {:?}",
        cluster.rejections
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn clustered_bootstrap_ensures_filesystem_only_on_authoritative_shard_and_probe_prefix() {
    let root = temp_root("filesystem-ensure-exact-authoritative-shard");
    let local = NodeId::from_legacy_named("node_filesystem_ensure_local").expect("local");
    let root_primary =
        NodeId::from_legacy_named("node_filesystem_ensure_root_primary").expect("root primary");
    let services_replica = NodeId::from_legacy_named("node_filesystem_ensure_services_replica")
        .expect("services replica");
    let file_system = distributed_sifs(local.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: root_primary,
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![services_replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: SIFS_SERVICES_PATH.to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let shards = vec![root_shard, services_shard];
    let mut kv = InProcessKvStore::new();
    {
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    }
    let metadata_root = root.join("metadata");
    let metadata = open_local_sharded_metadata_backend(metadata_root, shards).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(local),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(kv)
    .with_metadata_backend(metadata)
    .expect("metadata backend");

    dfs.bootstrap_distributed_sifs()
        .expect("bootstrap locally authoritative services shard");

    let root_shard_id = DfsMetadataShardId::new("root");
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .journal_batch_for_shard_since_limited(
                &root_shard_id,
                0,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .expect("root journal")
            .is_none(),
        "the locally opened non-primary root shard must remain at genesis"
    );

    let services_shard_id = DfsMetadataShardId::new("services");
    let services_batch = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_batch_for_shard_since_limited(
            &services_shard_id,
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("services journal")
        .expect("services bootstrap entries");
    let filesystem_sequence = services_batch
        .entries
        .iter()
        .find(|entry| matches!(&entry.mutation, DfsMetadataMutation::PutFileSystem { .. }))
        .map(|entry| entry.sequence)
        .expect("services filesystem entry");
    let prepare_entry = services_batch
        .entries
        .iter()
        .find(|entry| {
            matches!(
                &entry.mutation,
                DfsMetadataMutation::PutNamespacePrepare {
                    status: DfsNamespacePrepareStatus::Prepared,
                    ..
                }
            )
        })
        .cloned()
        .expect("services namespace prepare");
    assert!(filesystem_sequence < prepare_entry.sequence);

    let prepared_batch = DfsMetadataJournalBatch {
        shard_id: services_shard_id,
        entries: vec![prepare_entry.clone()],
        next_after_sequence: prepare_entry.sequence,
    };
    let authority_batch = dfs
        .metadata_authority_batch_for_target(&services_replica, &prepared_batch)
        .expect("authority batch")
        .expect("missing receiver prefix through prepare");
    assert!(
        authority_batch.entries.iter().any(|entry| {
            entry.sequence == filesystem_sequence
                && matches!(&entry.mutation, DfsMetadataMutation::PutFileSystem { .. })
        }),
        "the first authority batch must carry the missing filesystem prefix"
    );
    assert!(
        authority_batch
            .entries
            .iter()
            .any(|entry| entry.sequence == prepare_entry.sequence)
    );

    std::fs::remove_dir_all(root).ok();
}
