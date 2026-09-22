//! Writes regression tests.

use super::*;

#[test]
fn node_transport_completes_synchronous_replica_write() {
    let root = temp_root("node_route-transport-replica-write");
    let local = NodeId::from_legacy_named("node_dfs_node_route_write_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_node_route_write_remote").expect("remote");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_sender = Arc::clone(&calls);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        let DfsNetworkMessage::StageBlob(stage) = &command.routed_message.message else {
            return Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            });
        };
        calls_for_sender
            .lock()
            .expect("calls lock")
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
    dfs.posix_mkdir_all("/services").expect("services dir");

    let stat = dfs
        .posix_write_file(
            "/services/node_route-transport.txt",
            b"replicated bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("replicated write");

    assert_eq!(calls.lock().expect("calls lock").len(), 1);
    assert_eq!(stat.current_locations, vec![local, remote]);
    assert_eq!(stat.size_bytes, Some(b"replicated bytes".len() as u64));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_wait_for_commit_uses_transport_operation_deadline() {
    let root = temp_root("posix-wait-operation-deadline");
    let local = NodeId::from_legacy_named("node_posix_wait_deadline_local").expect("local");
    let remote = NodeId::from_legacy_named("node_posix_wait_deadline_remote").expect("remote");
    let operation_deadline = Instant::now() + Duration::from_millis(30);
    let observed_deadline_sets = Arc::new(Mutex::new(Vec::new()));
    let transport = DeadlineDroppingSendTransport {
        local_node_id: local.clone(),
        operation_deadline: Some(operation_deadline),
        observed_deadline_sets: Arc::clone(&observed_deadline_sets),
    };
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(transport, content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services").expect("services dir");

    let started_at = Instant::now();
    let error = dfs
        .posix_write_file(
            "/services/deadline.txt",
            b"pending bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                replica_candidates: vec![local, remote],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect_err("missing replica response should remain pending at the deadline");

    assert!(
        matches!(error, DfsPosixError::PendingWrite { .. }),
        "unexpected bounded settle error: {error:?}"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "the caller deadline must bound the 45-second POSIX settle loop"
    );
    assert_eq!(
        dfs.transport.operation_deadline,
        Some(operation_deadline),
        "the caller's earlier deadline must be restored after the write"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_write_installs_and_restores_its_commit_deadline() {
    let root = temp_root("posix-write-installs-deadline");
    let local = NodeId::from_legacy_named("node_posix_write_deadline_local").expect("local");
    let observed_deadline_sets = Arc::new(Mutex::new(Vec::new()));
    let transport = DeadlineDroppingSendTransport {
        local_node_id: local.clone(),
        operation_deadline: None,
        observed_deadline_sets: Arc::clone(&observed_deadline_sets),
    };
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(transport, content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services").expect("services dir");
    observed_deadline_sets
        .lock()
        .expect("deadline sets")
        .clear();

    let started_at = Instant::now();
    dfs.posix_write_file(
        "/services/deadline.txt",
        b"bounded bytes",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            replica_candidates: vec![local],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    )
    .expect("local bounded write");

    let observed = observed_deadline_sets.lock().expect("deadline sets");
    assert_eq!(observed.len(), 2, "install then restore the write deadline");
    let installed = observed[0].expect("installed write deadline");
    assert!(installed > started_at + Duration::from_secs(40));
    assert!(installed <= started_at + Duration::from_secs(46));
    assert_eq!(observed[1], None, "restore the unbounded caller state");
    assert_eq!(dfs.transport.operation_deadline, None);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_sandbox_create_namespace_mutations_install_and_restore_commit_deadlines() {
    let root = temp_root("posix-sandbox-create-mutation-deadlines");
    let local = NodeId::from_legacy_named("node_posix_sandbox_deadline_local").expect("local");
    let observed_deadline_sets = Arc::new(Mutex::new(Vec::new()));
    let transport = DeadlineDroppingSendTransport {
        local_node_id: local.clone(),
        operation_deadline: None,
        observed_deadline_sets: Arc::clone(&observed_deadline_sets),
    };
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(transport, content);
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    observed_deadline_sets
        .lock()
        .expect("deadline sets")
        .clear();

    let started_at = Instant::now();
    dfs.posix_mkdir_all("/services/sandbox/instances")
        .expect("sandbox directory chain");
    dfs.posix_symlink(
        "/services/sandbox/generation",
        "/services/sandbox/instances/swap",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            replica_candidates: vec![local],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    )
    .expect("sandbox generation link");
    dfs.posix_rename_single_namespace_atomic(
        "/services/sandbox/instances/swap",
        "/services/sandbox/instances/stable",
    )
    .expect("publish stable generation link");

    let observed = observed_deadline_sets.lock().expect("deadline sets");
    assert!(
        observed.len() >= 6,
        "the three mutations must each install then restore a deadline: {observed:?}"
    );
    assert_eq!(
        observed
            .iter()
            .filter(|deadline| deadline.is_none())
            .count(),
        3,
        "each top-level mutation restores the unbounded caller state"
    );
    assert!(
        observed
            .iter()
            .flatten()
            .filter(|deadline| **deadline > started_at + Duration::from_secs(40))
            .count()
            >= 3,
        "each top-level mutation installs the 45-second commit budget"
    );
    assert_eq!(dfs.transport.operation_deadline, None);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_cluster_dfs_helper_writes_default_records_to_cluster_kv() {
    let root = temp_root("default-cluster-dfs-helper");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_default_helper")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_default_helper_local").expect("local");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "dfs default helper",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("cluster kv state");

    let report = ensure_default_cluster_dfs(
        &data_dir,
        &cluster_paths,
        Box::new(ClusterPathDfsControlPlaneKvClient::new(&cluster_paths)),
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control: true,
            materialize_local_metadata: true,
            local_zone: None,
            local_content_advertisement: None,
        },
    )
    .expect("default cluster dfs")
    .expect("default cluster dfs report");

    assert!(report.local_has_primary_metadata_shards);
    let config_entry = sinomo_core::kv::read_cluster_kv_entry(
        &cluster_paths.kv_paths(),
        &file_system_config_key(&report.file_system.file_system_id),
    )
    .expect("filesystem config")
    .expect("filesystem config present");
    let config = config_entry
        .decode::<DfsFileSystemConfig>()
        .expect("filesystem config decode");
    assert_eq!(config.owner_node_id, local);
    assert_eq!(
        sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &metadata_shards_prefix(&report.file_system.file_system_id),
        )
        .expect("metadata shard config")
        .len(),
        3
    );
    assert!(
        !sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            content_services_prefix(),
        )
        .expect("content service advertisements")
        .is_empty()
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn control_plane_default_bootstrap_host_writes_default_dfs_records() {
    let root = temp_root("control-plane-default-bootstrap-host");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_bootstrap_host")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_bootstrap_host_local").expect("local");
    let remote = (0..128)
        .map(|index| {
            NodeId::from_legacy_named(format!("node_dfs_bootstrap_host_remote_{index}"))
                .expect("remote")
        })
        .find(|candidate| candidate > &local)
        .expect("remote node id ordered after local node id");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs bootstrap host",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state
        .members
        .push(sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        ));
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let mut stale_writer = control_plane.clone();
    DfsControlPlaneKvClient::put(
        &mut stale_writer,
        metadata_service_key(&local),
        b"legacy metadata advertisement".to_vec(),
    )
    .expect("write stale metadata advertisement");
    DfsControlPlaneKvClient::put(
        &mut stale_writer,
        content_service_key(&local),
        b"legacy content advertisement".to_vec(),
    )
    .expect("write stale content advertisement");
    let mut remote_content = DfsNodeAdvertisement::content(remote.clone());
    remote_content.zone = Some("zone-b".to_string());
    DfsControlPlaneKvClient::put(
        &mut stale_writer,
        content_service_key(&remote),
        sinomo_core::kv::encode_kv_value(&remote_content).expect("encode remote advertisement"),
    )
    .expect("write remote content advertisement");
    let managed_rule = normalize_placement_rule(
        "/managed",
        DfsPlacementScope::Subtree,
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
            .expect("managed placement policy"),
    )
    .expect("managed placement rule");
    let mut current_config = DfsFileSystemConfig::from(&default_cluster_sifs(&state));
    upsert_placement_rule(&mut current_config.subtree_policies, managed_rule.clone());
    DfsControlPlaneKvClient::put(
        &mut stale_writer,
        file_system_config_key(&current_config.file_system_id),
        sinomo_core::kv::encode_kv_value(&current_config).expect("encode filesystem config"),
    )
    .expect("write managed filesystem config");
    let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
        data_dir.clone(),
        cluster_paths,
        local.clone(),
        Box::new(control_plane.clone()),
    );

    let report = ensure_default_distributed_dfs_with_host(
        &state,
        &mut host,
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control: true,
            materialize_local_metadata: true,
            local_zone: Some("zone-a".to_string()),
            local_content_advertisement: None,
        },
    )
    .expect("default dfs bootstrap");

    assert!(report.local_has_primary_metadata_shards);
    let mut reader = control_plane.clone();
    let store = DfsControlPlaneStore::new(&mut reader);
    let config = store
        .get_file_system_config(&report.file_system.file_system_id)
        .expect("filesystem config")
        .expect("filesystem config present");
    assert_eq!(config.owner_node_id, local);
    assert!(config.subtree_policies.contains(&managed_rule));
    let shards = store
        .list_metadata_shards(&report.file_system.file_system_id)
        .expect("metadata shards");
    assert_eq!(shards.len(), 3);
    assert!(
        shards
            .iter()
            .any(|shard| matches!(shard.assignment, DfsMetadataShardAssignment::Root))
    );
    assert!(
        store
            .kv
            .range_prefix(content_services_prefix())
            .expect("content advertisements")
            .into_iter()
            .any(|entry| entry.decode::<DfsNodeAdvertisement>().is_ok())
    );
    let metadata_advertisement = store
        .metadata_service(&local)
        .expect("metadata advertisement")
        .expect("metadata advertisement present");
    assert_eq!(metadata_advertisement.node_id, local.clone());
    assert!(metadata_advertisement.accepts_metadata);
    assert_eq!(metadata_advertisement.zone.as_deref(), Some("zone-a"));
    let content_advertisement = store
        .content_service(&local)
        .expect("content advertisement")
        .expect("content advertisement present");
    assert_eq!(content_advertisement.node_id, local);
    assert!(content_advertisement.accepts_content);
    assert_eq!(content_advertisement.zone.as_deref(), Some("zone-a"));
    let remote_content_advertisement = store
        .content_service(&remote)
        .expect("remote content advertisement")
        .expect("remote content advertisement present");
    assert_eq!(remote_content_advertisement.zone.as_deref(), Some("zone-b"));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn smoke_driver_commits_local_candidate_and_publishes_metadata() {
    let root = temp_root("smoke-driver-local-candidate");
    let local = NodeId::from_legacy_named("node_smoke_driver_local").expect("local");
    let replica = NodeId::from_legacy_named("node_smoke_driver_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let metadata_root = root.join("metadata");
    let metadata = LocalSobjMetadataBackend::new(shard_id.clone(), metadata_root.join("root"))
        .expect("metadata backend");
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(network.add_node(local.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut smoke_host = FixedSmokeHost {
        pending_repair_count: 0,
    };
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(metadata_root);
    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let file_system = distributed_sifs(local.clone());

    let report = run_dfs_smoke_with_host(
        &mut dfs,
        &local,
        vec![local.clone()],
        Vec::new(),
        &mut smoke_host,
        file_system.file_system_id.clone(),
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
        DfsSmokeOptions {
            nonce: Some(12345),
            ..DfsSmokeOptions::default()
        },
    )
    .expect("smoke");

    assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.committed_replica_node_ids, vec![local]);
    assert_eq!(report.pending_repair_count, 0);
    assert_eq!(report.attempts.len(), 1);
    assert!(report.attempts[0].committed);
    assert!(report.path.starts_with("/dfs-smoke/12345-"));
    assert_eq!(publisher_host.opened_shards, vec![shard_id.clone()]);
    assert_eq!(
        publisher_host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())]
    );
    assert_eq!(transport.batches, 1);
    assert!(matches!(
        report.metadata_replication.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::Applied {
            target_node_id,
            shard_id: outcome_shard_id,
            ..
        }] if target_node_id == &replica && outcome_shard_id == &shard_id
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn smoke_driver_reports_local_commit_when_remote_replica_is_pending() {
    let root = temp_root("smoke-driver-remote-pending");
    let local = NodeId::from_legacy_named("node_smoke_driver_pending_local").expect("local");
    let replica = NodeId::from_legacy_named("node_smoke_driver_pending_replica").expect("replica");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: local.clone(),
        },
        content,
    )
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    let mut smoke_host = FixedSmokeHost {
        pending_repair_count: 1,
    };
    let mut publisher_host = RecordingMetadataJournalPublisherHost::new(root.join("journal"));
    let mut transport = ApplyingMetadataJournalTransport::new(replica.clone());
    let file_system = distributed_sifs(local.clone());

    let report = run_dfs_smoke_with_host(
        &mut dfs,
        &local,
        vec![local.clone(), replica.clone()],
        vec![replica.clone()],
        &mut smoke_host,
        file_system.file_system_id,
        Vec::new(),
        &mut publisher_host,
        &mut transport,
        DfsSmokeOptions {
            nonce: Some(67890),
            ..DfsSmokeOptions::default()
        },
    )
    .expect("smoke");

    assert_eq!(
        report.requested_replica_node_ids,
        vec![local.clone(), replica]
    );
    assert_eq!(report.committed_replica_node_ids, vec![local]);
    assert_eq!(report.pending_repair_count, 1);
    assert_eq!(report.attempts.len(), 1);
    assert!(report.attempts[0].committed);
    assert!(report.path.starts_with("/services/dfs-smoke/67890-"));
    assert!(matches!(
        report.metadata_replication.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::MissingShard { shard_id, .. }]
            if shard_id == &DfsMetadataShardId::new("root")
    ));
    assert!(publisher_host.opened_shards.is_empty());
    assert_eq!(transport.batches, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_sifs_metadata_materialization_writes_node_directories_and_visible_versions() {
    let root = temp_root("default-sifs-materialization");
    let primary = NodeId::from_legacy_named("node_sifs_materialize_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_sifs_materialize_replica").expect("replica");
    let file_system = distributed_sifs(primary.clone());
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");

    materialize_default_sifs_metadata(
        &mut backend,
        &file_system,
        vec![primary.clone(), replica.clone()],
        primary.clone(),
        OffsetDateTime::UNIX_EPOCH,
    )
    .expect("materialize default sifs metadata");

    let replica_files = backend
        .get_object_by_path(
            &file_system.file_system_id,
            sifs_node_files_path(&replica).trim_matches('/'),
        )
        .expect("replica files lookup")
        .expect("replica files object");
    let version = backend
        .get_version(&replica_files.object_id, &replica_files.version)
        .expect("replica files version")
        .expect("visible version record");
    assert!(version.visible);
    assert_eq!(version.created_at, OffsetDateTime::UNIX_EPOCH);
    assert_eq!(version.locations, replica_files.current_locations);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sharded_backend_updates_active_intent_on_original_shard_after_object_moves() {
    let root = temp_root("sharded-active-intent-original-shard-update");
    let owner = NodeId::from_legacy_named("node_sharded_active_intent_owner").expect("owner");
    let file_system = file_system(owner.clone());
    let object_id =
        DfsObjectId::from_legacy_named("obj_sharded_active_intent_move").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_sharded_active_intent_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_sharded_active_intent_2").expect("version");
    let shard_configs = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "users".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let mut backend =
        open_local_sharded_metadata_backend(root.join("metadata"), shard_configs).expect("backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let mut original_object = file_metadata(owner.clone());
    original_object.object_id = object_id.clone();
    original_object.file_system_id = file_system.file_system_id.clone();
    original_object.path = "/services/original.txt".to_string();
    original_object.version = version_1.clone();
    original_object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend
        .put_object(&original_object)
        .expect("original object");

    let intent_id = "dfs_intent_sharded_active_intent_move".to_string();
    let mut intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: intent_id.clone(),
        file_system_id: file_system.file_system_id.clone(),
        object_id: object_id.clone(),
        base_version: Some(version_1.clone()),
        target_version: version_2.clone(),
        coordinator_node_id: owner.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![owner.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:sharded-active-intent".to_string()),
        object_metadata: Some(original_object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    backend
        .put_intent_for_object(Some(&original_object), &intent)
        .expect("active intent on original shard");

    let mut moved_object = original_object.clone();
    moved_object.path = "/users/relocated.txt".to_string();
    backend.put_object(&moved_object).expect("moved object");

    let active_page = backend
        .write_intents_page(None, 8)
        .expect("active intent scan");
    assert!(
        active_page
            .intents
            .iter()
            .any(|candidate| candidate.intent_id == intent_id
                && candidate.status == DfsWriteIntentStatus::ReceiptsPending),
        "active intents on stale shards must remain visible to maintenance scans"
    );
    assert_eq!(
        backend
            .metadata_summary()
            .expect("summary before terminal update")
            .pending_write_count,
        1
    );

    intent.status = DfsWriteIntentStatus::Committed;
    intent.object_metadata = Some(moved_object.clone());
    backend
        .put_intent_for_object(Some(&moved_object), &intent)
        .expect("terminal update stays on original shard");
    assert_eq!(
        backend
            .metadata_summary()
            .expect("summary after terminal update")
            .pending_write_count,
        0
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_response_commits_restored_pending_intent_without_inflight_request_id() {
    let root = temp_root("stage-response-commits-restored-pending-intent");
    let primary =
        NodeId::from_legacy_named("node_stage_response_restore_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_stage_response_restore_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_response_restore").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_response_restore_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_response_restore_2").expect("version");
    let bytes = b"stage response must survive stateless daemon handling";
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/stateless-stage-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    dfs.add_object_metadata(object).expect("object metadata");

    let outcome = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: primary.clone(),
            },
            bytes.to_vec(),
            vec![primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending distributed write");
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
    let content_hash = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("intent read")
        .expect("intent")
        .content_hash
        .expect("intent content hash");
    drop(dfs);

    let restarted_network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut restarted =
        NetworkedDfsService::new(restarted_network.add_node(primary.clone()), content)
            .with_metadata_backend(metadata)
            .expect("restored metadata backend");
    assert!(
        restarted
            .writes
            .pending_writes
            .get(&intent_id)
            .is_some_and(|pending| pending.stage_inflight_request_ids.is_empty()),
        "restored daemon state should not rely on ephemeral inflight request ids"
    );

    let event = restarted
        .handle_stage_blob_response(
            replica.clone(),
            DfsStageBlobResponse {
                request_id: "lost-daemon-inflight-request-id".to_string(),
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: content_hash.clone(),
                },
            },
        )
        .expect("stage response")
        .expect("stage event");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            intent_id: ref event_intent_id,
            ref replica_node_id,
            ..
        } if event_intent_id == &intent_id && replica_node_id == &replica
    ));
    assert!(matches!(
        restarted.write_outcome(&intent_id).expect("write outcome"),
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash: ref committed_hash,
            ..
        } if version == version_2 && committed_hash == &content_hash
    ));
    let committed = restarted
        .current_read_object_metadata(&object_id)
        .expect("committed object");
    assert_eq!(committed.version, version_2);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(content_hash.as_str())
    );
    assert!(committed.current_locations.contains(&primary));
    assert!(committed.current_locations.contains(&replica));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_non_foreground_write_reports_only_durable_locations() {
    let root = temp_root("posix-non-foreground-current-locations");
    let local = NodeId::from_legacy_named("node_posix_async_repair_local").expect("local node");
    let remote = NodeId::from_legacy_named("node_posix_async_repair_remote").expect("remote node");
    let mut dfs = networked_sifs_with_backend(&root, local.clone());
    dfs.posix_mkdir_all("/services").expect("services dir");

    let stat = dfs
        .posix_write_file(
            "/services/async-repair.txt",
            b"commit locally and repair the remote replica later",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![local.clone(), remote.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("non-foreground write");

    assert_eq!(
        stat.current_locations,
        vec![local],
        "the returned POSIX stat must not claim an async repair target is durable"
    );
    assert_eq!(
        dfs.operational_metrics().expect("metrics").repair_job_count,
        1,
        "the remote desired replica should remain queued for repair"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_local_primary_detached_manifest_write_persists_visible_metadata_without_self_rpc() {
    let root = temp_root("posix-local-primary-detached-manifest");
    let local =
        NodeId::from_legacy_named("node_posix_local_manifest_primary").expect("local primary");
    let peer = NodeId::from_legacy_named("node_posix_local_manifest_peer").expect("peer");
    let file_system = file_system(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![peer.clone()],
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

    let send_count = Arc::new(AtomicUsize::new(0));
    let send_count_for_sender = Arc::clone(&send_count);
    let sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        send_count_for_sender.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeTransport::with_connect_timeout(
        local.clone(),
        vec![local.clone()],
        Duration::from_millis(1),
        sender,
    );
    let content_root = root.join("content");
    let metadata_root = root.join("metadata");
    let content = LocalContentStore::new(&content_root).expect("content");
    let mut dfs = NetworkedDfsService::new(transport, content)
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            open_local_sharded_metadata_backend(&metadata_root, vec![shard.clone()])
                .expect("metadata backend"),
        )
        .expect("attach metadata backend");
    dfs.add_file_system(file_system.clone())
        .expect("filesystem");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services").expect("parent");

    let path = "/services/local-primary-large.bin";
    let bytes = vec![0x63; 2 * 1024 * 1024 + 8192];
    let expected_hash = content_hash(&bytes);
    let stat = dfs
        .posix_write_file_with_known_parent_deferred_local_replay(
            path,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![local.clone(), peer.clone()],
                wait_for_commit: false,
                foreground_replica_repair: false,
            },
        )
        .expect("detached local-primary write");

    assert_eq!(send_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(dfs.posix_read_file(path).expect("read local"), bytes);

    let reload_sender: DfsMessageSender = Arc::new(move |_target_node_id, command, _timeout| {
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let reload_transport = DfsNodeTransport::with_connect_timeout(
        local.clone(),
        vec![local.clone()],
        Duration::from_millis(1),
        reload_sender,
    );
    let reload_content = LocalContentStore::new(&content_root).expect("reload content");
    let mut reloaded = NetworkedDfsService::new(reload_transport, reload_content)
        .with_control_plane(control_plane)
        .with_metadata_backend(
            open_local_sharded_metadata_backend(&metadata_root, vec![shard])
                .expect("reload metadata backend"),
        )
        .expect("attach reload metadata backend");
    reloaded
        .add_file_system(file_system)
        .expect("reload filesystem");
    reloaded.bootstrap_distributed_sifs().expect("reload sifs");

    assert_eq!(
        reloaded
            .posix_stat(path)
            .expect("reloaded stat")
            .content_hash
            .as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(reloaded.posix_read_file(path).expect("reload read"), bytes);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stage_response_completes_hydrated_transient_intent_without_visible_object() {
    let root = temp_root("stage-response-restores-transient-intent-record");
    let primary =
        NodeId::from_legacy_named("node_stage_response_transient_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_stage_response_transient_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_response_transient").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_stage_response_transient_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_stage_response_transient_2").expect("version");
    let bytes = b"transient intent metadata restores stateless primary commit";
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let manifest = content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/stateless-transient-stage-response.txt".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    let intent_id = "dfs_intent_stage_response_transient".to_string();
    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend
        .put_file_system(&file_system(primary.clone()))
        .expect("filesystem");
    backend
        .put_intent_for_object(
            Some(&object),
            &DfsWriteIntentRecord {
                publication: Default::default(),
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                base_version: Some(version_1),
                target_version: version_2.clone(),
                coordinator_node_id: primary.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 0,
                desired_replica_node_ids: vec![primary.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(manifest.content_hash.clone()),
                object_metadata: Some(object.clone()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            },
        )
        .expect("intent");
    backend
        .put_receipt_for_object(
            None,
            &DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: version_2.clone(),
                    owner_node_id: primary.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: primary.clone(),
                content_hash: Some(manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
        )
        .expect("primary receipt");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(network.add_node(primary.clone()), content)
        .with_metadata_backend(backend)
        .expect("metadata backend");
    assert!(
        dfs.coordinator.intent(&intent_id).is_some(),
        "active transient intent should hydrate even though its object is not visible yet"
    );
    assert!(dfs.current_read_object_metadata(&object_id).is_err());

    let event = dfs
        .handle_stage_blob_response(
            replica.clone(),
            DfsStageBlobResponse {
                request_id: "lost-daemon-inflight-request-id".to_string(),
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2.clone(),
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            },
        )
        .expect("stage response")
        .expect("stage event");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged {
            intent_id: ref event_intent_id,
            ..
        } if event_intent_id == &intent_id
    ));
    assert!(matches!(
        dfs.write_outcome(&intent_id).expect("write outcome"),
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash,
            ..
        } if version == version_2 && content_hash == manifest.content_hash
    ));
    let committed = dfs
        .current_read_object_metadata(&object_id)
        .expect("committed object");
    assert_eq!(committed.version, version_2);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn posix_transient_create_routes_precommit_records_to_path_shard() {
    let root = temp_root("posix-transient-create-path-shard");
    let owner = NodeId::from_legacy_named("node_transient_path_shard_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_transient_path_shard_replica").expect("replica");
    let file_system = file_system(owner.clone());
    let shard_configs = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        for shard in &shard_configs {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });

    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_local_sharded_metadata_backend(root.join("metadata"), shard_configs)
        .expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        content,
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(metadata)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    dfs.posix_mkdir_all("/services/dfs-smoke")
        .expect("smoke directory");

    let stat = dfs
        .posix_write_file(
            "/services/dfs-smoke/transient-create.txt",
            b"path shard transient create",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                replica_candidates: vec![owner.clone(), replica],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("transient create write");

    let backend = dfs.metadata_backend().expect("metadata backend");
    assert_eq!(
        backend
            .get_object(&stat.object_id)
            .expect("committed object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some("/services/dfs-smoke/transient-create.txt")
    );
    let services_batch = backend
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("services journal")
        .expect("services entries");
    assert!(
        services_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::PutIntent { .. })),
        "transient create intent must be journaled on the selected services shard"
    );
    assert!(
        services_batch
            .entries
            .iter()
            .any(|entry| matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. })),
        "transient create receipt must be journaled on the selected services shard"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn remote_manifest_write_defers_failed_post_commit_fetch() {
    let root = temp_root("remote-manifest-defers-fetch-error");
    let primary =
        NodeId::from_legacy_named("node_remote_manifest_fetch_primary").expect("primary node");
    let requester =
        NodeId::from_legacy_named("node_remote_manifest_fetch_requester").expect("requester node");
    let object_id = DfsObjectId::from_legacy_named("obj_remote_manifest_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: primary.clone(),
        },
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

    let remote_content_hash = content_hash(b"remote manifest bytes");
    let write = dfs
        .write_remote_distributed_manifest(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: requester.clone(),
            },
            Some(version_1),
            remote_content_hash,
            21,
            vec![primary.clone(), requester],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("post-commit fetch failure should be deferred");

    let NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } = write else {
        panic!("expected committed write with deferred repair, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, primary);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_does_not_resurrect_aborted_pending_write() {
    let root = temp_root("hydrate-skips-aborted-pending-write");
    let owner = NodeId::from_legacy_named("node_hydrate_abort_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_abort_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_abort").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
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
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    dfs.abort_pending_write(&intent_id).expect("abort");
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "abort should remove in-memory pending write"
    );

    dfs.hydrate_from_metadata_backend().expect("rehydrate");

    let restored_aborted_intent = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("aborted intent lookup")
        .expect("aborted intent remains durable");
    assert_eq!(
        restored_aborted_intent.status,
        DfsWriteIntentStatus::Aborted
    );
    assert!(
        dfs.coordinator().intent(&intent_id).is_none(),
        "closed aborted intents should advance restored ID state without occupying coordinator memory"
    );
    assert_eq!(
        dfs.metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1
    );
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "rehydration must not resurrect pending state for aborted writes"
    );
    let next_write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner,
            },
            b"aborted pending bytes".to_vec(),
            vec![dfs.node_id().clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("retry pending write");
    let next_intent_id = match next_write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected retry pending write, got {other:?}"),
    };
    assert_ne!(
        next_intent_id, intent_id,
        "hydration must observe closed intent ids so new writes do not reuse them"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_superseded_pending_intent_preserves_replica_records_and_live_work() {
    for frozen in [false, true] {
        for status in [
            DfsWriteIntentStatus::Staging,
            DfsWriteIntentStatus::ReceiptsPending,
        ] {
            let root = temp_root("hydrate-superseded-pending-intent");
            let owner = NodeId::from_legacy_named("node_hydrate_superseded_owner").expect("owner");
            let shard_id = DfsMetadataShardId::new("root");
            let file_system = file_system(owner.clone());
            let mut current = file_metadata(owner.clone());
            current.file_system_id = file_system.file_system_id.clone();
            current.path = "/services/chat/snapshot.json".to_string();
            current.version =
                DfsVersion::from_legacy_named("version_hydrate_current").expect("current version");
            let mut stale_base = current.clone();
            stale_base.version =
                DfsVersion::from_legacy_named("version_hydrate_old_base").expect("old version");
            let stale = DfsWriteIntentRecord {
                publication: Default::default(),
                intent_id: "hydrate-superseded-pending".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                object_id: current.object_id.clone(),
                base_version: Some(stale_base.version.clone()),
                target_version: DfsVersion::from_legacy_named("version_hydrate_stale_target")
                    .expect("target"),
                coordinator_node_id: owner.clone(),
                operation_id: None,
                operation_order_token: None,
                owner_epoch: 1,
                desired_replica_node_ids: vec![owner.clone()],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status,
                content_hash: None,
                object_metadata: Some(stale_base),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: None,
            };
            let content = LocalContentStore::new(root.join("content")).expect("content");
            let mut live_object = current.clone();
            live_object.object_id =
                DfsObjectId::from_legacy_named("obj_hydrate_live_pending").expect("live object");
            live_object.path = "/services/chat/other-snapshot.json".to_string();
            let live_target =
                DfsVersion::from_legacy_named("version_hydrate_live_target").expect("live target");
            let hash = content
                .put_object_version(
                    live_object.object_id.as_str(),
                    live_target.as_str(),
                    b"pending live write",
                )
                .expect("live content")
                .content_hash;
            let live = DfsWriteIntentRecord {
                intent_id: "hydrate-valid-pending".to_string(),
                object_id: live_object.object_id.clone(),
                base_version: Some(live_object.version.clone()),
                target_version: live_target,
                content_hash: Some(hash),
                object_metadata: Some(live_object.clone()),
                ..stale.clone()
            };
            let mut backend =
                open_root_local_metadata_backend(root.join("metadata")).expect("backend");
            backend.put_file_system(&file_system).expect("filesystem");
            backend.put_object(&current).expect("current object");
            backend.put_object(&live_object).expect("live object");
            backend
                .put_intent(&stale)
                .expect("superseded pending record");
            backend.put_intent(&live).expect("valid pending record");
            if frozen {
                backend
                    .freeze_metadata_shard_for_membership_handoff(
                        &shard_id,
                        "hydrate-retired-primary",
                        1,
                    )
                    .expect("freeze");
                backend
                    .mark_metadata_shard_membership_handoff_activated(
                        &shard_id,
                        "hydrate-retired-primary",
                        2,
                    )
                    .expect("successor activation");
            }
            let sequence = backend.journal_last_sequence();
            let network = InProcessNetwork::new();
            let service = NetworkedDfsService::new(network.add_node(owner), content)
                .with_metadata_backend(backend)
                .expect("superseded pending work must not prevent restart hydration");
            assert_eq!(
                service
                    .current_read_object_metadata(&current.object_id)
                    .expect("current snapshot")
                    .version,
                current.version
            );
            assert!(service.coordinator.intent(&stale.intent_id).is_none());
            assert!(
                service.coordinator.intent(&live.intent_id).is_some(),
                "valid pending work must still be recovered"
            );
            assert!(service.writes.pending_writes.contains_key(&live.intent_id));
            assert_eq!(
                service
                    .metadata_backend()
                    .expect("backend")
                    .get_intent(&current.object_id, &stale.intent_id)
                    .expect("durable pending record"),
                Some(stale.clone())
            );
            drop(service);
            let mut reopened =
                open_root_local_metadata_backend(root.join("metadata")).expect("reopen");
            assert_eq!(
                reopened.journal_last_sequence(),
                sequence,
                "hydration must not author a rejection on the replica"
            );
            if frozen {
                assert!(matches!(
                    reopened.put_intent(&stale),
                    Err(MetadataBackendError::MembershipHandoffFrozen { .. })
                ));
            }
            drop(reopened);
            std::fs::remove_dir_all(root).ok();
        }
    }
}

#[test]
fn hydrate_from_metadata_backend_rejects_competing_pending_intent_without_conflict() {
    let root = temp_root("hydrate-competing-pending-intent-reject");
    let owner = NodeId::from_legacy_named("node_hydrate_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let version_3 = DfsVersion::from_legacy_named("version_3").expect("version");
    let second_bytes = b"recovered competing pending write";
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

    let first = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"first pending write".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("first pending write");
    let first_intent_id = match first {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write, got {other:?}"),
    };
    let second_hash = owner_dfs
        .content()
        .put_object_version(object_id.as_str(), version_3.as_str(), second_bytes)
        .expect("second content")
        .content_hash;
    owner_dfs
        .metadata_backend
        .as_mut()
        .expect("backend")
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("dfs_intent_{}_99", owner.as_str()),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: version_3.clone(),
            coordinator_node_id: owner.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(second_hash.clone()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::now_utc() + time::Duration::seconds(1),
            updated_at: Some(OffsetDateTime::now_utc() + time::Duration::seconds(1)),
        })
        .expect("competing persisted intent");
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("competing pending intent should hydrate as rejected");

    assert!(
        recovered_owner
            .writes
            .pending_writes
            .contains_key(&first_intent_id),
        "the earliest pending intent should remain repairable"
    );
    assert_eq!(
        recovered_owner
            .metadata()
            .object(&object_id)
            .expect("visible object")
            .version,
        version_1,
        "rejected recovered intent must not publish a visible version"
    );
    let conflicts = recovered_owner
        .conflicts_for_object(&object_id)
        .expect("conflicts");
    assert!(
        conflicts.is_empty(),
        "normal hydrate rejection must not create DFS conflicts"
    );
    let recovered_intent = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_intent(&object_id, &format!("dfs_intent_{}_99", owner.as_str()))
        .expect("intent")
        .expect("intent");
    assert_eq!(recovered_intent.status, DfsWriteIntentStatus::Rejected);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_without_source_remains_pending_with_backoff() {
    let root = temp_root("missing-commit-content-no-source");
    let fixture = seed_durable_commit_promotion_fixture(&root, "missing-content-no-source");
    let target_content_root = root.join("target-content");
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target metadata backend");
    let mut target = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("empty target content"),
    )
    .with_metadata_backend(backend)
    .expect("target metadata hydration");
    let object_metadata = target
        .metadata()
        .object(&fixture.object_id)
        .cloned()
        .expect("committed object metadata");

    let _ = target.handle_commit_version(
        fixture.owner,
        DfsCommitVersionCommand {
            request_id: "commit_missing_content_no_source".to_string(),
            intent_id: fixture.intent_id.clone(),
            file_system_id: object_metadata.file_system_id.clone(),
            object_id: fixture.object_id.clone(),
            expected_base_version: None,
            version: fixture.target_version.clone(),
            content_hash: fixture.content_hash.clone(),
            object_metadata: Some(object_metadata),
            authority_shard: None,
        },
    );
    drop(target);

    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened target metadata backend");
    let mut target = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(fixture.replica.clone()),
        LocalContentStore::new(&target_content_root).expect("reopened target content"),
    )
    .with_metadata_backend(backend)
    .expect("reopened target hydration");
    assert_eq!(target.coordinator().repair_jobs().len(), 1);

    assert!(
        target
            .retry_pending_repairs_bounded(1)
            .expect("bounded missing-source repair")
            .is_empty()
    );
    assert_eq!(target.coordinator().repair_jobs().len(), 1);
    assert!(
        target
            .writes
            .pending_writes
            .contains_key(&fixture.intent_id)
    );
    let attempts = target.active_repair_attempt_records();
    assert_eq!(attempts.len(), 1);
    assert!(!attempts[0].in_flight);
    assert!(attempts[0].next_retry_unix_nanos > attempts[0].last_attempt_unix_nanos);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_reselects_guard_when_object_moves_shards_before_certification() {
    let root = temp_root("missing-commit-content-shard-reselection");
    let fixture = seed_durable_commit_promotion_fixture(&root, "shard-reselection");
    let initial_shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "services".to_string(),
        primary_node_id: fixture.replica.clone(),
        replica_node_ids: vec![fixture.owner.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let (range_started_tx, range_started_rx) = std::sync::mpsc::sync_channel(1);
    let shared_kv = SharedDfsControlPlaneKvClient::new(RangeSignalingControlPlaneKvClient::new(
        range_started_tx,
    ));
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_history: Default::default(),
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: fixture.owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&initial_shard)
            .expect("initial services shard");
    });

    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target metadata backend");
    let mut target = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(fixture.replica.clone()),
        LocalContentStore::new(root.join("target-content")).expect("empty target content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(backend)
    .expect("target metadata hydration");
    let object_path = target
        .metadata()
        .object(&fixture.object_id)
        .expect("committed object metadata")
        .path
        .clone();
    let moved_shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "services-recovery-object".to_string(),
        primary_node_id: fixture.replica.clone(),
        replica_node_ids: vec![fixture.owner.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: object_path,
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    let initial_guard = crate::service_replica::metadata_shard_publication_guard(
        &initial_shard.file_system_id,
        &initial_shard.shard_id,
    );
    shared_kv.with_client(|kv| kv.arm());
    let transition_object_id = fixture.object_id.clone();
    let transition_version = fixture.target_version.clone();
    let transition_content_hash = fixture.content_hash.clone();
    let transition = std::thread::spawn(move || {
        let transitioned = target
            .transition_missing_local_commit_content_to_repair(
                &transition_object_id,
                &transition_version,
                &transition_content_hash,
            )
            .expect("missing-content transition");
        (target, transitioned)
    });
    range_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("transition selected the initial shard");
    shared_kv.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&moved_shard)
            .expect("publish more-specific object shard");
    });
    drop(initial_guard);

    let (target, transitioned) = transition.join().expect("join guarded transition");
    assert!(
        transitioned,
        "stable current authority should complete after reacquiring the moved shard guard"
    );
    assert!(
        shared_kv.with_client(|kv| kv.metadata_shard_range_count()) >= 4,
        "a changed certified shard key must force guarded selection and certification to repeat"
    );
    assert!(
        !target
            .metadata_backend()
            .expect("target backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("receipts after transition")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "the receipt may be pruned only after the moved shard is guarded and certified"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_raw_new_object_write_does_not_touch_parent() {
    let (before, after, after_second_restart) = recovered_new_object_parent_metadata(false);
    assert_eq!(
        after, before,
        "raw writes must not imply POSIX create effects"
    );
    assert_eq!(after_second_restart, before);
}

#[test]
fn hydrate_from_metadata_backend_keeps_incomplete_new_object_intent_invisible() {
    let root = temp_root("hydrate-keeps-incomplete-new-object-invisible");
    let primary = NodeId::from_legacy_named("node_hydrate_incomplete_primary").expect("node");
    let requester = NodeId::from_legacy_named("node_hydrate_incomplete_requester").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_incomplete_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_incomplete_new").expect("object");
    let target_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("primary-content");
    let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("primary"), &metadata_root)
        .expect("primary backend");
    let mut primary_dfs = NetworkedDfsService::new(
        network.add_node(primary.clone()),
        LocalContentStore::new(&content_root).expect("primary content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");
    let mut metadata = file_metadata(primary.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/hydrate-incomplete-new.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    primary_dfs
        .add_file_system(file_system(primary.clone()))
        .expect("primary fs");
    primary_dfs
        .add_transient_object_metadata(metadata)
        .expect("transient new object");

    let write = primary_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version,
                requesting_node_id: requester,
            },
            b"incomplete new object bytes".to_vec(),
            vec![primary.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(
        primary_dfs
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object lookup")
            .is_none()
    );
    drop(primary_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("primary"), &metadata_root)
            .expect("recovered backend");
    let recovered = NetworkedDfsService::new(
        network.add_node(primary),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("incomplete intent should hydrate without becoming visible");
    assert!(
        recovered.metadata().object(&object_id).is_none(),
        "receipt-incomplete new object must remain absent from the in-memory namespace"
    );
    assert!(
        recovered
            .metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("object lookup")
            .is_none(),
        "receipt-incomplete new object must remain absent from durable visible metadata"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_does_not_mix_duplicate_intent_ids() {
    let root = temp_root("hydrate-duplicate-intent-ids");
    let owner = NodeId::from_legacy_named("node_hydrate_dup_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_dup_replica").expect("node");
    let other_replica = NodeId::from_legacy_named("node_hydrate_dup_other_replica").expect("node");
    let pending_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_dup_pending").expect("object");
    let committed_object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_dup_committed").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let pending_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let committed_version = DfsVersion::from_legacy_named("version_3").expect("version");
    let intent_id = format!("dfs_intent_{}_20", owner.as_str());
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let content_root = root.join("owner-content");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("backend");
    let file_system = file_system(owner.clone());
    backend.put_file_system(&file_system).expect("filesystem");

    let mut pending_object = file_metadata(owner.clone());
    pending_object.object_id = pending_object_id.clone();
    pending_object.path = "/services/dfs-smoke/pending-duplicate-intent.txt".to_string();
    pending_object.version = version_1.clone();
    pending_object.policy = DfsObjectPolicy::ReplicatedEventual;
    backend.put_object(&pending_object).expect("pending object");

    let content = LocalContentStore::new(&content_root).expect("content");
    let pending_manifest = content
        .put_object_version(
            pending_object_id.as_str(),
            pending_version.as_str(),
            b"pending duplicate intent bytes",
        )
        .expect("pending manifest");
    backend
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: pending_object_id.clone(),
            base_version: Some(version_1.clone()),
            target_version: pending_version.clone(),
            coordinator_node_id: owner.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some(pending_manifest.content_hash.clone()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("pending intent");
    for node_id in [&owner, &replica] {
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: pending_object_id.clone(),
                    committed_version: pending_version.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: node_id.clone(),
                content_hash: Some(pending_manifest.content_hash.clone()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("pending receipt");
    }

    let mut committed_object = file_metadata(owner.clone());
    committed_object.object_id = committed_object_id.clone();
    committed_object.path = "/services/dfs-smoke/committed-duplicate-intent.txt".to_string();
    committed_object.version = committed_version.clone();
    committed_object.policy = DfsObjectPolicy::ReplicatedEventual;
    committed_object.current_locations = vec![owner.clone(), other_replica.clone()];
    committed_object.content_hash = Some("blake3:committed-duplicate".to_string());
    committed_object.content_size_bytes = Some(3);
    backend
        .put_object(&committed_object)
        .expect("committed object");
    backend
        .put_version(&object_version_record(&committed_object, Some(3)))
        .expect("committed version");
    backend
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.clone(),
            file_system_id: sifs_file_system_id(),
            object_id: committed_object_id.clone(),
            base_version: Some(version_1),
            target_version: committed_version,
            coordinator_node_id: owner.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![owner.clone(), other_replica.clone()],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: committed_object.content_hash.clone(),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
        })
        .expect("committed duplicate intent");
    for node_id in [&owner, &other_replica] {
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: sifs_file_system_id(),
                    object_id: committed_object_id.clone(),
                    committed_version: committed_object.version.clone(),
                    owner_node_id: owner.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: node_id.clone(),
                content_hash: committed_object.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("committed receipt");
    }
    drop(backend);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("duplicate intent ids should not poison hydration");

    let pending_object = recovered_owner
        .metadata_backend()
        .expect("backend")
        .get_object(&pending_object_id)
        .expect("pending object")
        .expect("pending object");
    assert_eq!(pending_object.version, pending_version);
    assert_eq!(
        pending_object
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner, replica])
    );
    assert!(
        !recovered_owner
            .writes
            .pending_writes
            .contains_key(&intent_id)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sharded_hydration_omits_ambiguous_historical_intent_provenance() {
    let root = temp_root("hydrate-ambiguous-historical-intent-provenance");
    let owner = NodeId::from_legacy_named("node_hydrate_ambiguous_owner").expect("node");
    let object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_ambiguous_history").expect("object");
    let intent_id = "dfs_intent_hydrate_ambiguous_history".to_string();
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
    let shards = vec![
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: root_shard_id.0.clone(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: services_shard_id.0.clone(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 2,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    ];
    let mut backend =
        open_local_sharded_metadata_backend(root.join("metadata"), shards).expect("backend");
    backend
        .put_file_system(&distributed_sifs(owner.clone()))
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.path = "/services/ambiguous-history.txt".to_string();
    object.version = DfsVersion::from_legacy_named("version_2").expect("version");
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.content_hash = Some("blake3:ambiguous-history".to_string());
    object.content_size_bytes = Some(1);
    backend.put_object(&object).expect("object");
    backend
        .put_version(&object_version_record(&object, Some(1)))
        .expect("version");
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: intent_id.clone(),
        file_system_id: sifs_file_system_id(),
        object_id: object_id.clone(),
        base_version: Some(DfsVersion::from_legacy_named("version_1").expect("base version")),
        target_version: object.version.clone(),
        coordinator_node_id: owner.clone(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![owner.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::Committed,
        content_hash: object.content_hash.clone(),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    };
    for shard_id in [&root_shard_id, &services_shard_id] {
        backend
            .shard_backend_mut(shard_id)
            .expect("shard backend")
            .put_intent(&intent)
            .expect("historical intent copy");
    }

    let recovered = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(owner),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend(backend)
    .expect("duplicate historical intent must not block hydration");
    let key = (object_id.clone(), intent_id.clone());
    assert!(
        !recovered
            .durable_write_intent_shards()
            .expect("intent provenance")
            .contains_key(&key),
        "an intent present on multiple shards must not be assigned arbitrary recovery provenance"
    );
    assert!(
        recovered
            .coordinator
            .intent(&intent_id)
            .is_some_and(|restored| {
                restored.object_id == object_id
                    && matches!(restored.state, DfsWriteIntentState::Committed)
            }),
        "the committed historical intent should still hydrate"
    );
    for shard_id in [&root_shard_id, &services_shard_id] {
        let snapshot = recovered
            .metadata_backend()
            .expect("metadata backend")
            .export_shard_table_snapshot(shard_id)
            .expect("shard snapshot")
            .expect("local shard snapshot");
        let tables = crate::metadata_table_store::metadata_tables_from_table_snapshot(
            snapshot,
            "test shard snapshot",
        )
        .expect("metadata tables");
        assert!(
            tables.intents.contains_key(&key),
            "hydration must not delete the historical copy on shard {}",
            shard_id.0
        );
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_from_metadata_backend_restores_staging_pending_write_size_from_local_manifest() {
    let root = temp_root("hydrate-staging-size-from-manifest");
    let owner = NodeId::from_legacy_named("node_hydrate_staging_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_staging_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_staging_size").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"staging bytes survive restart";
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
            vec![owner.clone(), replica],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("staging write");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => intent_id,
        other => panic!("expected pending write before replica receipt, got {other:?}"),
    };
    assert!(
        owner_dfs
            .metadata_backend()
            .expect("backend")
            .get_version(&object_id, &version_2)
            .expect("version lookup")
            .is_none(),
        "staging writes should not need a visible version record before commit"
    );
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_owner = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(content_root).expect("recovered content"),
    )
    .with_metadata_backend(recovered_backend)
    .expect("recovered metadata");

    assert_eq!(
        recovered_owner
            .writes
            .pending_writes
            .get(&intent_id)
            .expect("restored pending write")
            .size_bytes,
        bytes.len() as u64
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_write_size_hint_ignores_backend_version_with_wrong_hash() {
    let root = temp_root("pending-size-ignores-wrong-hash-version");
    let owner = NodeId::from_legacy_named("node_size_hint_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_size_hint_wrong_hash").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"correct pending content size";
    let correct_hash = content_hash(bytes);
    let wrong_hash = content_hash(b"wrong content");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut object = file_metadata(owner);
    object.object_id = object_id.clone();
    object.version = version.clone();
    object.content_hash = Some(wrong_hash.clone());
    object.content_size_bytes = Some(999);
    dfs.metadata_backend
        .as_mut()
        .expect("backend")
        .put_version(&object_version_record(&object, Some(999)))
        .expect("wrong hash version");
    dfs.content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("correct local manifest");

    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &correct_hash)
            .expect("size hint"),
        Some(bytes.len() as u64)
    );
    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &wrong_hash)
            .expect("wrong-hash size hint"),
        Some(999)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_write_size_hint_uses_verified_hash_addressed_blob_without_manifest() {
    let root = temp_root("pending-size-uses-hash-addressed-blob");
    let owner = NodeId::from_legacy_named("node_size_hint_blob_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_size_hint_hash_blob").expect("object");
    let version = DfsVersion::from_legacy_named("version_size_hint_hash_blob").expect("version");
    let bytes = b"pending content retained only as a hash-addressed blob";
    let dfs = networked_sifs_with_backend(&root, owner);
    let manifest = dfs
        .content
        .put_object_version_blob_only(object_id.as_str(), version.as_str(), bytes)
        .expect("hash-addressed blob");

    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "the regression requires a collected per-version manifest"
    );
    assert_eq!(
        dfs.pending_write_size_hint(&object_id, &version, &manifest.content_hash)
            .expect("size hint"),
        Some(bytes.len() as u64)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_pending_write_cleanup_drops_stale_stage_inflight_state() {
    let root = temp_root("cleanup-drops-stale-stage-inflight");
    let owner = NodeId::from_legacy_named("node_cleanup_owner").expect("node");
    let stale_replica = NodeId::from_legacy_named("node_cleanup_stale_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_cleanup_stale_stage").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    let base_version = metadata.version.clone();
    dfs.add_object_metadata(metadata).expect("object");

    let content_hash = content_hash(b"cleanup bytes");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(base_version),
            version_2.clone(),
            dfs.node_id().clone(),
            vec![dfs.node_id().clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    let intent_id = intent.intent_id;
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite {
            object_id,
            version: version_2,
            content_hash,
            size_bytes: b"cleanup bytes".len() as u64,
            foreground_replica_repair: true,
            patch: None,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::from([(
                stale_replica,
                "dfs_net_req_stale_stage".to_string(),
            )]),
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        },
    );
    dfs.coordinator
        .record_durable_receipt(&intent_id, dfs.node_id().clone())
        .expect("record receipt");
    dfs.apply_committed_intent(&intent_id)
        .expect("commit intent");
    assert!(dfs.coordinator.repair_jobs().is_empty());

    dfs.cleanup_committed_pending_write_if_repaired(&intent_id);

    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "fully repaired committed writes must not retain stale stage in-flight state"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn write_outcome_uses_backend_current_metadata_after_pending_state_is_removed() {
    let root = temp_root("write-outcome-backend-current");
    let owner = NodeId::from_legacy_named("node_write_outcome_owner").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_write_outcome_backend").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"write outcome backend bytes";
    let hash = content_hash(bytes);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata.clone()).expect("object");
    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner.clone(),
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        PendingNetworkedWrite {
            object_id: object_id.clone(),
            version: version_2.clone(),
            content_hash: hash.clone(),
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
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("record receipt");
    dfs.apply_committed_intent(&intent.intent_id)
        .expect("commit intent");
    let mut backend_current = metadata;
    backend_current.version = version_2.clone();
    backend_current.content_hash = Some(hash.clone());
    backend_current.content_size_bytes = Some(bytes.len() as u64);
    backend_current.current_locations = vec![owner];
    dfs.add_object_metadata(backend_current)
        .expect("backend current object");
    dfs.remove_transient_object_metadata(&object_id);
    assert!(
        dfs.metadata().object(&object_id).is_none(),
        "test requires backend-only committed metadata"
    );

    let outcome = dfs
        .write_outcome(&intent.intent_id)
        .expect("write outcome from backend metadata");
    assert!(matches!(
        outcome,
        NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash,
            ..
        } if version == version_2 && content_hash == hash
    ));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_applies_when_response_target_is_unroutable() {
    let root = temp_root("commit-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_commit_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_response").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system_id = sifs_file_system_id();
    let bytes = b"commit response target is unavailable";
    let content_hash = content_hash(bytes);
    let content = LocalContentStore::new(root.join("content")).expect("content");
    content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("staged replica bytes");
    let mut transport = ReceiveOnlyTransport::new(replica.clone());
    let object_metadata = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "commit-response.bin".to_string(),
        kind: DfsObjectKind::Artifact,
        tombstone_provenance: None,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version_1,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    transport.push_message(
        owner.clone(),
        replica.clone(),
        DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
            request_id: "dfs_net_req_commit_unroutable_response".to_string(),
            intent_id: "intent_commit_unroutable_response".to_string(),
            file_system_id,
            object_id: object_id.clone(),
            expected_base_version: Some(object_metadata.version.clone()),
            version: version_2.clone(),
            content_hash: content_hash.clone(),
            object_metadata: Some(object_metadata),
            authority_shard: None,
        }),
    );
    let mut dfs = NetworkedDfsService::new(transport, content);

    let events = dfs
        .process_incoming()
        .expect("commit should apply even if response cannot be routed");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::VersionCommitted {
                object_id: committed,
                version,
                content_hash: committed_hash,
                ..
            } if committed == &object_id && version == &version_2 && committed_hash == &content_hash
        )
    }));
    assert_eq!(
        dfs.read_local_object(&object_id)
            .expect("read committed object")
            .expect("committed data")
            .bytes,
        bytes
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_blob_accepts_metadata_primary_for_stored_intent() {
    let root = temp_root("fetch-blob-metadata-primary-stored-intent");
    let coordinator =
        NodeId::from_legacy_named("node_fetch_stored_intent_coordinator").expect("coordinator");
    let primary = NodeId::from_legacy_named("node_fetch_stored_intent_primary").expect("primary");
    let source = NodeId::from_legacy_named("node_fetch_stored_intent_source").expect("source");
    let target = NodeId::from_legacy_named("node_fetch_stored_intent_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_stored_intent").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_fetch_stored_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_fetch_stored_2").expect("target version");
    let intent_id = "dfs_intent_fetch_stored_primary";
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
                replica_node_ids: vec![source.clone(), target.clone()],
                epoch: 4,
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
    object.path = "/services/fetch-stored-intent.bin".to_string();
    object.version = version_2.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    let bytes = b"stored intent fetch bytes for metadata-primary repair";
    let content_hash = content_hash(bytes);
    object.content_hash = Some(content_hash.clone());
    object.content_size_bytes = Some(bytes.len() as u64);
    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_intent(&DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(version_1),
            target_version: version_2.clone(),
            coordinator_node_id: coordinator.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 4,
            desired_replica_node_ids: vec![source.clone(), target.clone()],
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
    let mut dfs = NetworkedDfsService::new(network.add_node(source.clone()), content)
        .with_control_plane(shared_kv)
        .with_metadata_backend(backend)
        .expect("metadata backend");
    dfs.content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");

    let event = dfs
        .handle_fetch_blob(
            primary,
            DfsFetchBlobCommand {
                request_id: "dfs_net_req_fetch_stored_primary".to_string(),
                intent_id: intent_id.to_string(),
                object_id: object_id.clone(),
                version: version_2,
                content_hash: content_hash.clone(),
                authority_shard: None,
            },
        )
        .expect("fetch blob");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::BlobFetched {
            object_id: fetched_object_id,
            source_node_id,
            content_hash: fetched_hash,
            ..
        } if fetched_object_id == object_id
            && source_node_id == source
            && fetched_hash == content_hash
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_intent_applies_when_commit_notification_target_is_unroutable() {
    let root = temp_root("commit-notify-target-unroutable");
    let owner = NodeId::from_legacy_named("node_commit_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_notify").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(owner.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let bytes = b"committed bytes despite unroutable notify";
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");
    let intent = dfs
        .coordinator
        .create_intent(
            metadata.file_system_id.clone(),
            object_id.clone(),
            Some(version_1),
            version_2.clone(),
            owner,
            vec![replica.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect("intent");
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
    let committed = dfs
        .coordinator
        .record_durable_receipt(&intent.intent_id, replica.clone())
        .expect("durable receipt");
    assert!(matches!(
        committed.state,
        DfsWriteIntentState::AwaitingReceipts
    ));

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("post-commit replica notification is best effort");

    let object = dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_2);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    assert_eq!(object.current_locations, vec![replica]);
    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read state");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Current
        } if data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn replica_stage_and_commit_do_not_append_authoritative_metadata() {
    let root = temp_root("replica-stage-commit-no-authoritative-metadata");
    let owner = NodeId::from_legacy_named("node_replica_cache_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_replica_cache_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_replica_cache").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"replica content cache without authoritative metadata append";
    let mut object = file_metadata(owner.clone());
    object.object_id = object_id.clone();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_history: Default::default(),
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let mut dfs = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("metadata backend");
    let before_journal = dfs
        .metadata_backend()
        .expect("metadata backend")
        .metadata_shard_statuses()
        .expect("metadata status")
        .pop()
        .expect("root shard status")
        .journal_last_sequence;

    let stage_event = dfs
        .handle_stage_blob(
            owner.clone(),
            DfsStageBlobCommand {
                request_id: "dfs_net_req_replica_stage_cache".to_string(),
                intent_id: "dfs_intent_replica_cache".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(object.clone()),
                authority_shard: None,
            },
        )
        .expect("replica stage");
    assert!(matches!(
        stage_event,
        NetworkedDfsServiceEvent::BlobStaged { .. }
    ));

    let commit_event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_replica_commit_cache".to_string(),
                intent_id: "dfs_intent_replica_cache".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: content_hash(bytes),
                object_metadata: Some({
                    object.version = version_2.clone();
                    object.content_hash = Some(content_hash(bytes));
                    object.current_locations = vec![replica.clone()];
                    object
                }),
                authority_shard: None,
            },
        )
        .expect("replica commit");
    assert!(matches!(
        commit_event,
        NetworkedDfsServiceEvent::VersionCommitted { .. }
    ));
    assert_eq!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("root shard status")
            .journal_last_sequence,
        before_journal,
        "replica data-plane commands must wait for primary journal replay before mutating the durable metadata shard"
    );
    assert!(
        dfs.metadata_backend()
            .expect("metadata backend")
            .get_object(&object_id)
            .expect("backend object")
            .is_none(),
        "replica cache metadata must not be published to the durable shard backend"
    );
    assert_eq!(
        dfs.current_read_object_metadata(&object_id)
            .expect("transient local cache")
            .version,
        version_2
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_rejects_manifest_without_content_bytes() {
    let root = temp_root("commit-rejects-manifest-without-blob");
    let owner = NodeId::from_legacy_named("node_commit_missing_blob_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_missing_blob_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_missing_blob").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = vec![b'c'; 17 * 1024];
    let network = InProcessNetwork::new();
    let _owner_transport = network.add_node(owner.clone());
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica")).expect("replica content"),
    );
    replica_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    metadata.current_locations = vec![owner.clone()];
    replica_dfs
        .add_object_metadata(metadata.clone())
        .expect("metadata");
    let manifest = replica_dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), &bytes)
        .expect("stage manifest");
    let hex = manifest
        .content_hash
        .strip_prefix("blake3:")
        .expect("blake3 hash");
    std::fs::remove_file(root.join("replica").join("blobs").join(&hex[..2]).join(hex))
        .expect("remove blob");

    let event = replica_dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_missing_blob".to_string(),
                intent_id: "dfs_intent_commit_missing_blob".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("commit response");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected {
            object_id: rejected_object_id,
            reason,
            ..
        } if rejected_object_id == object_id && reason.contains("committed content bytes are missing")
    ));
    assert!(matches!(
        replica_dfs.read_local_object_with_state(&object_id, false),
        Ok(NetworkedDfsReadOutcome::NotFound)
    ));
    let object = replica_dfs.metadata().object(&object_id).expect("object");
    assert_eq!(object.version, version_1);
    assert_eq!(object.content_hash, None);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_write_routes_to_owner() {
    let owner = NodeId::from_legacy_named("node_desktop").expect("node");
    let remote = NodeId::from_legacy_named("node_mobile").expect("node");
    let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
    let object_id = DfsObjectId::from_legacy_named("obj_file").expect("object");
    let mut store = DfsMetadataStore::new();
    store.add_file_system(file_system(owner.clone()));
    store
        .add_object(DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id,
            path: "src/lib.rs".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        })
        .expect("add object");

    let route = store
        .route_write(
            DfsWriteRequest {
                file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                object_id,
                target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
                requesting_node_id: remote.clone(),
            },
            &remote,
        )
        .expect("route write");

    assert_eq!(
        route.action,
        WriteRouteAction::RouteToOwner {
            owner_node_id: owner
        }
    );
}

#[test]
fn sifs_self_only_eventual_write_uses_local_fast_path() {
    let root = temp_root("sifs-self-only-eventual-fast-path");
    let node_id = NodeId::from_legacy_named("node_sifs_fast_path").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_fast/work/site.py";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_fast/work")
        .expect("mkdir");
    let stat = dfs
        .posix_write_file(
            file_path,
            b"print('fast path')\n",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write");

    assert_eq!(stat.size_bytes, Some(19));
    assert_eq!(
        dfs.posix_read_file(file_path).expect("read"),
        b"print('fast path')\n"
    );
    assert!(
        dfs.writes.pending_writes.is_empty(),
        "self-only eventual writes should not leave distributed intents"
    );
    assert!(dfs.coordinator.repair_jobs().is_empty());

    let journal_count = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert!(
        journal_count < 64,
        "self-only write should not emit per-file distributed intent/receipt/commit journals"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_commit_metadata_application_is_idempotent() {
    let root = temp_root("duplicate-commit-metadata-idempotent");
    let node_id = NodeId::from_legacy_named("node_duplicate_commit_meta").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_duplicate_commit_meta").expect("object");
    let bytes = b"duplicate commit bytes";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    let mut metadata = file_metadata(node_id.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.last_modified_by_event_id =
        Some(EventId::from_legacy_named("evt_duplicate_commit_base").expect("event"));
    let version = metadata.version.clone();
    let hash = dfs
        .put_local_object_version(metadata, bytes)
        .expect("local version");
    let before = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    let committed = dfs
        .commit_local_metadata(&object_id, &version, &hash, dfs_now_unix_nanos())
        .expect("duplicate commit metadata");
    dfs.put_visible_object_in_metadata_backend(&committed)
        .expect("duplicate visible publish");

    let after = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after duplicate commit");
    assert_eq!(
        after.last_modified_by_event_id,
        before.last_modified_by_event_id
    );
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(after.changed_at_unix_nanos, before.changed_at_unix_nanos);
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "duplicate committed metadata application must not emit visible metadata churn"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn duplicate_committed_intent_application_does_not_recommit_visible_version() {
    let root = temp_root("duplicate-committed-intent-idempotent");
    let owner = NodeId::from_legacy_named("node_duplicate_intent_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_duplicate_intent_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_duplicate_intent_commit").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"duplicate committed intent bytes";

    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version_1.clone();
    dfs.add_object_metadata(metadata).expect("object");
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
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &owner,
        &manifest.content_hash,
    )
    .expect("backend receipt");
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .expect("commit with repair");
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
        .expect("first commit apply");
    let before = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after first apply");
    let journal_count_before = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();

    dfs.apply_committed_intent(&intent.intent_id)
        .expect("duplicate commit apply");

    let after = dfs
        .current_read_object_metadata(&object_id)
        .expect("current object after duplicate apply");
    assert_eq!(after, before);
    let journal_count_after = std::fs::read_dir(root.join("metadata/journal"))
        .expect("journal dir")
        .count();
    assert_eq!(
        journal_count_after, journal_count_before,
        "reapplying an already-visible committed intent must not append a commit journal entry"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_exclusive_create_never_overwrites_an_existing_file() {
    let root = temp_root("sifs-posix-exclusive-create");
    let node_id = NodeId::from_legacy_named("node_sifs_exclusive_create").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/exclusive-create")
        .expect("mkdir parent");
    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let path = "/services/exclusive-create/file";
    dfs.posix_write_file_with_create_attrs(
        path,
        b"keep me",
        options.clone(),
        DfsPosixCreateAttrs::new(0o640, 1001, 1002),
    )
    .expect("seed existing file");

    assert!(matches!(
        dfs.posix_create_file_with_attrs(
            path,
            options.clone(),
            DfsPosixCreateAttrs::new(0o600, 2001, 2002),
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(path).expect("existing bytes"),
        b"keep me"
    );
    let existing = dfs.posix_stat(path).expect("existing attrs");
    assert_eq!(
        (existing.mode, existing.uid, existing.gid),
        (0o640, 1001, 1002)
    );

    dfs.posix_unlink(path).expect("unlink existing file");
    let recreated = dfs
        .posix_create_file_with_attrs(path, options, DfsPosixCreateAttrs::new(0o600, 2001, 2002))
        .expect("exclusive create over tombstone");
    assert_eq!(recreated.size_bytes, Some(0));
    assert_eq!(
        (recreated.mode, recreated.uid, recreated.gid),
        (0o600, 2001, 2002)
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_updates_mtime_without_changing_atime() {
    let root = temp_root("sifs-posix-write-mtime");
    let node_id = NodeId::from_legacy_named("node_sifs_write_mtime").expect("node");
    let original = "/services/sandbox/instances/sandbox_write_mtime/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_write_mtime/work/alias.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_write_mtime/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");

    let stat = dfs
        .posix_write_file(
            link,
            b"updated",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write through hardlink");

    assert_eq!(stat.accessed_at_unix_nanos, 12_345);
    assert!(stat.modified_at_unix_nanos > 67_890);
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(
        original_stat.modified_at_unix_nanos,
        link_stat.modified_at_unix_nanos
    );
    assert!(original_stat.modified_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        original_stat.modified_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_local_eventual_writes_visible_files() {
    let root = temp_root("sifs-posix-bulk-local-eventual");
    let node_id = NodeId::from_legacy_named("node_sifs_bulk_local").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_local/work";
    let path_a = "/services/sandbox/instances/sandbox_bulk_local/work/a.txt";
    let path_b = "/services/sandbox/instances/sandbox_bulk_local/work/b.txt";

    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    let stats = dfs
        .posix_write_files_local_eventual_with_known_parents(vec![
            (path_a, b"bulk alpha".as_slice()),
            (path_b, b"bulk beta".as_slice()),
        ])
        .expect("bulk write");

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].path, path_a);
    assert_eq!(stats[0].current_locations, vec![node_id.clone()]);
    assert_eq!(stats[0].size_bytes, Some("bulk alpha".len() as u64));
    assert_eq!(stats[1].path, path_b);
    assert_eq!(stats[1].current_locations, vec![node_id]);
    assert_eq!(stats[1].size_bytes, Some("bulk beta".len() as u64));
    assert_eq!(dfs.posix_read_file(path_a).expect("read a"), b"bulk alpha");
    assert_eq!(dfs.posix_read_file(path_b).expect("read b"), b"bulk beta");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_overwrite_pins_metadata_generation_during_preparation() {
    let root = temp_root("sifs-posix-bulk-overwrite-pinned-metadata");
    let node_id =
        NodeId::from_legacy_named("node_sifs_bulk_overwrite_pinned_metadata").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_pinned_metadata/work";
    let mut dfs = networked_sifs_with_backend(&root, node_id);
    dfs.posix_mkdir_all(work).expect("mkdir");
    let owned_files = (0..64)
        .map(|index| {
            (
                format!("{work}/file-{index:03}.txt"),
                format!("initial-{index:03}").into_bytes(),
                format!("overwritten-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    dfs.posix_create_files_local_eventual_with_known_parents(
        owned_files
            .iter()
            .map(|(path, initial, _)| (path.as_str(), initial.as_slice())),
    )
    .expect("initial bulk create");
    let versions_before = owned_files
        .iter()
        .map(|(path, _, _)| dfs.posix_stat(path).expect("stat initial").version)
        .collect::<Vec<_>>();

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    crate::posix::reset_posix_metadata_raw_lookup_count();
    let stats = dfs
        .posix_write_files_local_eventual_with_known_parents(
            owned_files
                .iter()
                .map(|(path, _, overwritten)| (path.as_str(), overwritten.as_slice())),
        )
        .expect("bulk overwrite");

    assert_eq!(stats.len(), owned_files.len());
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "bulk overwrite child discovery must reuse the pinned metadata generation \
         instead of polling checkpoint, snapshot, and table-log files per object"
    );
    let parent_component_count = work.trim_matches('/').split('/').count();
    assert!(
        crate::posix::posix_bulk_prep_table_snapshot_calls()
            <= owned_files.len() + parent_component_count + 1,
        "bulk overwrite performed {} metadata table reads for {} child objects; \
         expected one pinned ancestor/parent revalidation plus one read per \
         child, without hydrating receipt-derived locations that the publisher \
         replaces",
        crate::posix::posix_bulk_prep_table_snapshot_calls(),
        owned_files.len(),
    );
    let raw_lookup_bound = owned_files.len() + parent_component_count * 2 + 3;
    assert!(
        crate::posix::posix_metadata_raw_lookup_count() <= raw_lookup_bound,
        "bulk overwrite performed {} raw path lookups for {} children below one \
         already-resolved parent; expected no more than {raw_lookup_bound}",
        crate::posix::posix_metadata_raw_lookup_count(),
        owned_files.len(),
    );
    for (((path, _, overwritten), version_before), stat) in
        owned_files.iter().zip(versions_before).zip(stats)
    {
        assert_eq!(stat.path, *path);
        assert_ne!(stat.version, version_before);
        assert_eq!(
            dfs.posix_read_file(path).expect("read overwritten file"),
            *overwritten
        );
    }

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.posix_stat(&owned_files[0].0)
        .expect("stat after bulk overwrite");
    assert!(
        crate::metadata_read_snapshot::metadata_durable_table_probes() > 0,
        "bulk overwrite must release its consistent-read scope before returning"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_distributed_write_rejects_stale_backend_base_version() {
    let root = temp_root("sifs-stale-write-base");
    let node_id = NodeId::from_legacy_named("node_sifs_stale_write_base").expect("node");
    let work = "/services/sandbox/instances/sandbox_stale_write/work";
    let file_path = "/services/sandbox/instances/sandbox_stale_write/work/file.txt";

    let mut writer = networked_sifs_with_backend(&root, node_id.clone());
    writer.posix_mkdir_all(work).expect("mkdir");
    writer
        .posix_write_file(
            file_path,
            b"abcdef",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("initial write");

    let stale = networked_sifs_with_backend(&root, node_id.clone());
    let stale_stat = stale.posix_stat(file_path).expect("stale stat");

    writer
        .posix_write_file_at(
            file_path,
            0,
            b"XY",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("fresh offset write");
    let fresh_stat = writer.posix_stat(file_path).expect("fresh stat");
    let journal_count_before_conflict = writer
        .metadata_backend()
        .expect("writer metadata backend")
        .journal_since(0)
        .expect("journal before conflict")
        .len();

    let mut stale_writer = stale;
    let request = DfsWriteRequest {
        file_system_id: stale_stat.file_system_id.clone(),
        object_id: stale_stat.object_id.clone(),
        target_version: crate::namespace::next_version(&stale_stat.version),
        requesting_node_id: node_id.clone(),
    };
    let error = stale_writer
        .write_distributed_eventual_with_base(
            request,
            Some(stale_stat.version.clone()),
            b"stale overwrite".to_vec(),
            vec![node_id.clone()],
            DfsReplicationPolicy::new(1, 1).expect("policy"),
        )
        .expect_err("stale write must reject");
    assert!(
        error.to_string().contains("stale DFS write base"),
        "unexpected stale write error: {error}"
    );

    let journal_after_rejection = stale_writer
        .metadata_backend()
        .expect("stale writer metadata backend")
        .journal_since(0)
        .expect("journal after rejection");
    assert_eq!(
        journal_after_rejection.len(),
        journal_count_before_conflict,
        "stale writer rejection must not append conflict metadata or overwrite fresh writer entries"
    );

    let reloaded = networked_sifs_with_backend(&root, node_id);
    assert_eq!(
        reloaded
            .posix_stat(file_path)
            .expect("stat after stale rejection")
            .content_hash,
        fresh_stat.content_hash,
        "stale rejection must not replace the committed content hash"
    );
    assert_eq!(
        reloaded.posix_read_file(file_path).expect("read winner"),
        b"XYcdef"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_distributed_write_validates_replica_plan_before_storing_content() {
    let root = temp_root("sifs-invalid-replica-plan-no-content");
    let node_id = NodeId::from_legacy_named("node_sifs_invalid_plan").expect("node");
    let work = "/services/sandbox/instances/sandbox_invalid_plan/work";
    let file_path = "/services/sandbox/instances/sandbox_invalid_plan/work/file.txt";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file_path,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("initial write");
    let stat = dfs.posix_stat(file_path).expect("stat");
    let rejected_bytes = b"replica plan is invalid";
    let rejected_hash = content_hash(rejected_bytes);
    let target_version = crate::namespace::next_version(&stat.version);

    let error = dfs
        .write_distributed_eventual_with_base(
            DfsWriteRequest {
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: node_id.clone(),
            },
            Some(stat.version),
            rejected_bytes.to_vec(),
            vec![node_id],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect_err("invalid replica plan should fail before content write");

    assert!(
        matches!(error, DfsError::Distributed { .. }),
        "invalid replica plan should be reported as distributed validation failure: {error:?}"
    );
    assert!(
        !dfs.content
            .has_blob(&rejected_hash)
            .expect("rejected blob check"),
        "rejected write bytes must not be persisted before replica validation"
    );
    assert!(
        dfs.content
            .read_manifest(stat.object_id.as_str(), target_version.as_str())
            .is_err(),
        "rejected write must not publish a content manifest"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_primary_detached_large_write_is_readable_without_follow_up_flush() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-primary-detached-large");
    let path = "/services/primary-detached-large.bin";
    let bytes = (0..(2 * 1024 * 1024 + 8192))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_hash = content_hash(&bytes);
    let mut options = sifs_cluster_write_options(&cluster.primary_node.clone());
    options.foreground_replica_repair = false;

    let stat = cluster
        .primary
        .posix_write_file(path, &bytes, options)
        .expect("primary detached large write");

    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(
        cluster
            .primary
            .content
            .read_blob(&expected_hash)
            .expect("durable blob is readable when metadata becomes visible"),
        bytes
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_concurrent_whole_file_overwrite_many_readers() {
    let mut cluster = sifs_posix_cluster("sifs-contract-overwrite-readers");
    let scenario = "concurrent-whole-file-overwrite-many-readers";
    let path = "/users/contract-overwrite.txt";
    let base = vec![b'A'; 4096];
    let writer_bytes = vec![b'B'; 4096];
    let observer_bytes = vec![b'C'; 4096];
    let valid_hashes = [
        read_content_hash(&base),
        read_content_hash(&writer_bytes),
        read_content_hash(&observer_bytes),
    ];
    let mut observations = Vec::new();

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "overwrite observer seed catch-up",
        |cluster| {
            cluster
                .observer
                .posix_stat(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(valid_hashes[0].as_str()))
        },
    );
    let base_stat = cluster.writer.posix_stat(path).expect("base stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&base_stat),
    );

    let writer_request = match cluster.writer.posix_write_file(
        path,
        &writer_bytes,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    ) {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "writer-overwrite",
                &cluster.writer_node,
                path,
                Instant::now(),
                "committed-immediately",
                Some(&stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id),
        Err(error) => panic!("writer overwrite failed unexpectedly: {error}"),
    };
    let observer_request = match cluster.observer.posix_write_file(
        path,
        &observer_bytes,
        sifs_cluster_write_options(&cluster.observer_node.clone()),
    ) {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "observer-overwrite",
                &cluster.observer_node,
                path,
                Instant::now(),
                "committed-immediately",
                Some(&stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id),
        Err(DfsPosixError::Dfs(DfsError::Busy { .. })) => None,
        Err(DfsPosixError::Conflict { .. }) => {
            panic!("observer overwrite created a DFS conflict during normal operation")
        }
        Err(error) => panic!("observer overwrite failed unexpectedly: {error}"),
    };

    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        for (op, node, read) in [
            (
                "reader-writer",
                cluster.writer_node.clone(),
                cluster.writer.posix_stat(path),
            ),
            (
                "reader-observer",
                cluster.observer_node.clone(),
                cluster.observer.posix_stat(path),
            ),
            (
                "reader-primary",
                cluster.primary_node.clone(),
                cluster.primary.posix_stat(path),
            ),
        ] {
            if let Ok(stat) = read {
                assert!(
                    stat.content_hash
                        .as_ref()
                        .is_none_or(|hash| valid_hashes.contains(hash)),
                    "{op} observed a non-serialized content hash: {stat:?}"
                );
                record_posix_observation(
                    &mut observations,
                    scenario,
                    op,
                    &node,
                    path,
                    Instant::now(),
                    "stat",
                    Some(&stat),
                );
            }
        }
        let writer_settled = writer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        let observer_settled = observer_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.observer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        });
        if writer_settled && observer_settled {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "concurrent overwrite did not settle; observations: {observations:#?}"
        );
        sifs_posix_cluster_pump_round(&mut cluster);
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }

    let final_stat = cluster
        .primary
        .posix_stat(path)
        .expect("primary final stat");
    assert!(
        final_stat
            .content_hash
            .as_ref()
            .is_some_and(|hash| valid_hashes.contains(hash)),
        "final visible version must be one complete serialized overwrite: {final_stat:?}"
    );
    let final_bytes = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert!(
        [&base, &writer_bytes, &observer_bytes]
            .iter()
            .any(|candidate| candidate.as_slice() == final_bytes.as_slice()),
        "final read must be one complete candidate, not mixed bytes"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_large_streamed_write_visibility() {
    let mut cluster = sifs_posix_cluster("sifs-contract-large-visibility");
    let scenario = "large-streamed-write-visibility";
    let path = "/services/contract-large-visible.bin";
    let old = vec![0x11; 128 * 1024];
    let new = (0..(4 * 1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let old_hash = content_hash(&old);
    let new_hash = content_hash(&new);
    let mut observations = Vec::new();

    sifs_cluster_routed_posix_write(&mut cluster, path, &old);
    let old_stat = cluster.writer.posix_stat(path).expect("old stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&old_stat),
    );
    let write = cluster.writer.posix_write_file(
        path,
        &new,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
            replica_candidates: vec![cluster.writer_node.clone(), cluster.observer_node.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    );
    assert!(
        matches!(write, Ok(_) | Err(DfsPosixError::PendingWrite { .. })),
        "large staged write must commit or report pending, got {write:?}"
    );

    let pre_commit = cluster
        .primary
        .posix_stat(path)
        .expect("primary pre-commit stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "reader-before-commit",
        &cluster.primary_node,
        path,
        Instant::now(),
        "stat",
        Some(&pre_commit),
    );
    assert_ne!(
        pre_commit.content_hash.as_deref(),
        Some(new_hash.as_str()),
        "large chunked bytes must not become visible before commit"
    );

    sifs_posix_cluster_pump_until(&mut cluster, "large chunked write commit", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(new_hash.as_str()))
    });
    let committed = cluster.writer.posix_stat(path).expect("committed stat");
    assert_eq!(committed.content_hash.as_deref(), Some(new_hash.as_str()));
    assert_ne!(old_hash, new_hash);
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_rename_over_existing_with_concurrent_write() {
    let mut cluster = sifs_posix_cluster("sifs-contract-rename-over-write");
    let scenario = "rename-over-existing-with-concurrent-write";
    let source = "/users/contract-rename-over-src.txt";
    let destination = "/users/contract-rename-over-dst.txt";
    let source_bytes = b"source wins if rename serializes last".to_vec();
    let destination_bytes = b"destination original".to_vec();
    let writer_bytes = b"concurrent destination writer".to_vec();
    let valid_hashes = [
        content_hash(&source_bytes),
        content_hash(&destination_bytes),
        content_hash(&writer_bytes),
    ];
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, source, &source_bytes);
    sifs_cluster_primary_posix_write(&mut cluster, destination, &destination_bytes);
    let write_result = cluster.writer.posix_write_file(
        destination,
        &writer_bytes,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let write_request = match &write_result {
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        _ => None,
    };
    record_posix_observation(
        &mut observations,
        scenario,
        "write-destination",
        &cluster.writer_node,
        destination,
        Instant::now(),
        format!("{write_result:?}"),
        write_result.as_ref().ok(),
    );
    let rename_result = cluster.primary.posix_rename(source, destination);
    record_posix_observation(
        &mut observations,
        scenario,
        "rename-over",
        &cluster.primary_node,
        destination,
        Instant::now(),
        format!("{rename_result:?}"),
        rename_result.as_ref().ok(),
    );
    sifs_posix_cluster_pump_until(&mut cluster, "rename/write race settle", |cluster| {
        write_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        }) && cluster.primary.posix_stat(destination).is_ok()
    });

    assert!(matches!(
        cluster.primary.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let final_stat = cluster
        .primary
        .posix_stat(destination)
        .expect("destination stat");
    assert!(
        final_stat
            .content_hash
            .as_ref()
            .is_some_and(|hash| valid_hashes.contains(hash)),
        "destination must reference one serialized participant: {final_stat:?}"
    );
    let source_entries = cluster
        .primary
        .posix_list_dir("/users")
        .expect("list users");
    assert_eq!(
        source_entries
            .iter()
            .filter(|entry| entry.path == source || entry.path == destination)
            .count(),
        1,
        "rename-over race must leave exactly one live destination entry: {source_entries:?}"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_contract_metadata_primary_redirect_during_write() {
    let root = temp_root("sifs-contract-write-redirect");
    let scenario = "metadata-primary-redirect-during-write";
    let old_primary_node =
        NodeId::from_legacy_named("node_contract_redirect_old").expect("old primary");
    let new_primary_node =
        NodeId::from_legacy_named("node_contract_redirect_new").expect("new primary");
    let requester_node =
        NodeId::from_legacy_named("node_contract_redirect_requester").expect("requester");
    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![old_primary_node.clone(), new_primary_node.clone()],
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let kv_current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_current.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                new_primary_node.clone(),
            )))
            .expect("current filesystem");
        control
            .put_metadata_shard(&shard_config(&new_primary_node, 2))
            .expect("current shard");
    });
    let kv_stale = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_stale.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                new_primary_node.clone(),
            )))
            .expect("stale filesystem");
        control
            .put_metadata_shard(&shard_config(&old_primary_node, 1))
            .expect("stale shard");
    });
    let stale_requester_snapshot = kv_stale
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    kv_stale.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard_config(&new_primary_node, 2))
            .expect("refreshed requester shard")
    });
    let old_primary = sifs_posix_cluster_node(
        &network,
        &kv_current,
        &root,
        "old-primary",
        old_primary_node,
    );
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &kv_current,
        &root,
        "new-primary",
        new_primary_node.clone(),
    );
    let mut requester = sifs_posix_cluster_node(
        &network,
        &kv_stale,
        &root,
        "requester",
        requester_node.clone(),
    );
    requester =
        requester.with_boxed_control_plane(Box::new(RefreshingDfsControlPlaneKvClient::new(
            InProcessKvStore::restore(stale_requester_snapshot)
                .expect("restore stale requester control plane"),
            kv_stale.clone(),
            Arc::new(AtomicUsize::new(0)),
        )));
    let mut observations = Vec::new();
    new_primary
        .bootstrap_distributed_sifs()
        .expect("new primary bootstrap");
    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(old_primary, old_stop));
    let new_stop = Arc::clone(&stop);
    let new_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(new_primary, new_stop));
    let path = "/users/contract-redirected-write.txt";
    let bytes = b"redirected write".to_vec();
    requester
        .posix_mkdir_all("/users")
        .expect("parent mkdir follows redirect");
    let stat = requester
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![requester_node.clone(), new_primary_node]),
        )
        .expect("write follows redirect");
    record_posix_observation(
        &mut observations,
        scenario,
        "redirected-write",
        &requester_node,
        path,
        Instant::now(),
        "committed",
        Some(&stat),
    );
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary");
    new_thread.join().expect("new primary");
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sharded_lazy_reader_sees_sibling_visible_overwrite() {
    let root = temp_root("sifs-sharded-lazy-reader-current");
    let node = NodeId::from_legacy_named("node_sifs_sharded_lazy_reader").expect("node");
    let shards = sifs_two_primary_shard_configs(&node, &node, 1, Vec::new(), Vec::new());
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(node.clone())))
            .expect("sifs filesystem config");
        for shard in &shards {
            control.put_metadata_shard(shard).expect("metadata shard");
        }
    });
    let network = InProcessNetwork::new();
    let content_root = root.join("content");
    let metadata_root = root.join("metadata");
    let mut writer = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(&content_root).expect("writer content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend_lazy(
        open_local_sharded_metadata_backend(&metadata_root, shards.clone())
            .expect("writer metadata"),
    )
    .expect("writer metadata backend");
    writer
        .bootstrap_distributed_sifs()
        .expect("writer bootstrap");

    let path = "/services/current-read-refresh.txt";
    let old = b"old sharded lazy reader payload".to_vec();
    let new = b"new sharded lazy reader payload".to_vec();
    writer
        .posix_write_file(
            path,
            &old,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .expect("initial write");

    let mut reader = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(&content_root).expect("reader content"),
    )
    .with_control_plane(shared_kv)
    .with_metadata_backend_lazy(
        open_local_sharded_metadata_backend(&metadata_root, shards).expect("reader metadata"),
    )
    .expect("reader metadata backend");
    reader
        .bootstrap_distributed_sifs()
        .expect("reader bootstrap");
    assert_eq!(
        reader
            .posix_read_file_distributed(path)
            .expect("reader old payload"),
        old
    );

    writer
        .posix_write_file(
            path,
            &new,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .expect("overwrite");

    assert_eq!(
        reader
            .posix_read_file_distributed(path)
            .expect("reader current payload"),
        new
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_blocking_write_and_read_with_live_peers() {
    // End-to-end through the blocking POSIX surface that the FUSE adapter and
    // CLI use: `wait_for_commit: true` writes on a non-primary node and
    // blocking distributed reads, with peer nodes pumping their own event
    // loops on threads like live daemons.
    let cluster = sifs_posix_cluster("sifs-cluster-blocking");
    let path = "/users/blocking.txt";
    let bytes = b"blocking write through live peers".to_vec();
    let SifsPosixCluster {
        root,
        primary_node: _,
        writer_node,
        observer_node: _,
        primary,
        mut writer,
        mut observer,
        rejections: _,
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

    // The production write path: blocks until the routed write commits.
    let stat = writer
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("blocking routed write");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));

    // Read-your-write immediately after the blocking write returns.
    assert_eq!(
        writer.posix_read_file(path).expect("read your write"),
        bytes
    );

    // Stop the primary thread and publish its journal so the observer learns
    // the namespace.
    stop_primary.store(true, Ordering::SeqCst);
    let mut primary = primary_thread.join().expect("primary thread");
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("primary journal publish");
        primary.process_incoming().expect("primary pump");
        writer.process_incoming().expect("writer pump");
        observer.process_incoming().expect("observer pump");
        if observer.posix_stat(path).is_ok() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("observer did not learn the namespace from journal publication");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }

    // The blocking distributed read consults the metadata primary and fetches
    // content from the writer replica while both peers keep serving frames.
    let read = sifs_cluster_blocking_distributed_read_with_pump(
        &mut observer,
        path,
        "blocking distributed read",
        || {
            primary.process_incoming().expect("primary pump");
            writer.process_incoming().expect("writer pump");
        },
    );
    assert_eq!(read, bytes);

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_prefetches_authoritative_children() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-overwrite");
    let remote_dir = "/services/bulk-metadata-less-overwrite";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less overwrite parent", |cluster| {
        cluster.replica.posix_stat(remote_dir).is_ok()
    });

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

    let files = (0..16)
        .map(|index| {
            (
                format!("{remote_dir}/file-{index:03}.bin"),
                format!("initial-{index:03}").into_bytes(),
                format!("replacement-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let created = replica
        .posix_create_files_local_eventual_with_known_parents(
            files
                .iter()
                .map(|(path, initial, _)| (path.as_str(), initial.as_slice())),
        )
        .expect("initial remote-primary bulk create");
    let created_versions = created
        .iter()
        .map(|stat| (stat.object_id.clone(), stat.version.clone()))
        .collect::<BTreeMap<_, _>>();
    let parent_object_id = replica
        .posix_stat(remote_dir)
        .expect("metadata-less overwrite parent before eviction")
        .object_id;

    // Match a newly opened one-shot CLI requester: retain the content store,
    // but remove the parent and every child from the memory overlay and drop
    // the durable metadata backend. The overwrite must discover the exact
    // parent and child object/version identities from the metadata authority
    // in one bounded LookupPaths request.
    replica.metadata.remove_object(&parent_object_id);
    for stat in &created {
        replica.metadata.remove_object(&stat.object_id);
    }
    replica.metadata_backend = None;
    assert!(
        replica
            .metadata
            .object_by_normalized_posix_path(&sifs_file_system_id(), remote_dir)
            .is_none(),
        "test requester unexpectedly retained parent metadata"
    );
    for (path, _, _) in &files {
        assert!(
            replica
                .metadata
                .object_by_normalized_posix_path(&sifs_file_system_id(), path)
                .is_none(),
            "test requester unexpectedly retained child metadata for {path}"
        );
    }

    let overwritten = replica
        .posix_write_files_local_eventual_with_known_parents(
            files
                .iter()
                .map(|(path, _, replacement)| (path.as_str(), replacement.as_slice())),
        )
        .expect("metadata-less remote-primary bulk overwrite");
    assert_eq!(overwritten.len(), files.len());
    for ((path, _, replacement), stat) in files.iter().zip(&overwritten) {
        assert_eq!(stat.path, *path);
        assert_eq!(
            stat.object_id,
            created
                .iter()
                .find(|created| created.path == *path)
                .expect("created object")
                .object_id
        );
        assert_ne!(
            stat.version,
            *created_versions
                .get(&stat.object_id)
                .expect("created object version")
        );
        assert_eq!(
            stat.content_hash.as_deref(),
            Some(content_hash(replacement).as_str()),
            "metadata-less overwrite returned the wrong content for {path}"
        );
    }

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services primary thread");
    for (path, _, replacement) in &files {
        assert_eq!(
            services_primary
                .posix_stat(path)
                .expect("stat authoritative overwrite")
                .content_hash
                .as_deref(),
            Some(content_hash(replacement).as_str()),
            "metadata authority retained the wrong content generation for {path}"
        );
    }

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_follows_authoritative_symlink() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-symlink");
    let remote_dir = "/services/bulk-metadata-less-symlink";
    let target = "/services/bulk-metadata-less-symlink/target.txt";
    let link = "/services/bulk-metadata-less-symlink/link.txt";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote symlink parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less symlink parent", |cluster| {
        cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        root,
        services_primary,
        replica_node,
        mut replica,
        ..
    } = cluster;
    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));
    let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
    replica
        .posix_write_file(target, b"before", options.clone())
        .expect("write symlink target");
    replica
        .posix_symlink("target.txt", link, options)
        .expect("create symlink");
    let target_before = replica.posix_stat(target).expect("stat symlink target");
    let link_before = replica
        .posix_lstat_distributed(link)
        .expect("lstat symlink before metadata eviction");
    assert_eq!(link_before.kind, DfsObjectKind::Symlink);
    let parent_id = replica
        .posix_stat(remote_dir)
        .expect("stat symlink parent")
        .object_id;

    for object_id in [&parent_id, &target_before.object_id, &link_before.object_id] {
        replica.metadata.remove_object(object_id);
    }
    replica.metadata_backend = None;

    let stat = replica
        .posix_write_files_local_eventual_with_known_parents([(
            link,
            b"through authoritative symlink".as_slice(),
        )])
        .expect("metadata-less write through authoritative symlink")
        .pop()
        .expect("symlink write stat");
    assert_eq!(stat.path, link);
    assert_eq!(stat.object_id, target_before.object_id);
    assert_eq!(
        replica
            .posix_read_file_distributed(target)
            .expect("read symlink target"),
        b"through authoritative symlink"
    );
    let link_after = replica
        .posix_lstat_distributed(link)
        .expect("lstat symlink after overwrite");
    assert_eq!(link_after.kind, DfsObjectKind::Symlink);
    assert_eq!(link_after.object_id, link_before.object_id);

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services primary thread");
    assert_eq!(
        services_primary
            .posix_stat(target)
            .expect("authoritative symlink target stat")
            .content_hash
            .as_deref(),
        Some(content_hash(b"through authoritative symlink").as_str())
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_non_replica_requester_routes_namespace_op_with_read_your_write() {
    let cluster = sifs_outsider_cluster("sifs-outsider-routed-namespace");
    let SifsPosixCluster {
        root,
        primary,
        mut observer,
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

    // The outsider is not in the shard's replica set; the routed mkdir's
    // response batches must still replay into its local mirror.
    observer
        .posix_mkdir_all("/users/outsider/nested")
        .expect("routed mkdir from non-replica requester");

    // Read-your-write without any further pumping: the mirror was updated by
    // the response replay alone.
    assert_eq!(
        observer
            .posix_stat("/users/outsider/nested")
            .expect("outsider reads its own routed namespace write")
            .kind,
        DfsObjectKind::Directory
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    std::fs::remove_dir_all(&root).ok();
}
