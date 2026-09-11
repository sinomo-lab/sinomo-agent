//! Authority regression tests.

use super::*;

#[test]
fn direct_authority_probe_uses_canonical_route_and_old_peer_fails_closed() {
    let local = NodeId::from_legacy_named("node_direct_authority_local").expect("local");
    let remote = NodeId::from_legacy_named("node_direct_authority_remote").expect("remote");
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let source_root = temp_root("direct-authority-route-source");
    let mut source = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new(shard.shard_id.clone()),
        &source_root,
    )
    .expect("source metadata backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    let command = DfsMetadataJournalBatchCommand {
        request_id: "direct-authority-request".to_string(),
        source_node_id: Some(local.clone()),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(DfsMetadataAuthorityProbe {
            probe_id: "direct-authority-probe".to_string(),
            expected_shard: shard.clone(),
            operation: DfsMetadataAuthorityOperation::NamespaceMutation {
                operation_id: "direct-authority-operation".to_string(),
                file_system_id: shard.file_system_id.clone(),
                prepare_sequence: batch.next_after_sequence,
                payload_digest: "blake3:direct-authority-operation".to_string(),
            },
            durable_journal_ack: false,
            journal_frames: Vec::new(),
            prepared_prefix: None,
        }),
        durable_replication: None,
        batch,
    };

    let peer = Arc::new(RecordingPeerTransport::default());
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let async_peer_transport = recording_async_peer_transport(Arc::clone(&peer));
    let mut transport = DfsNodeTransport::with_peer_transports_for_nodes(
        local.clone(),
        vec![remote.clone()],
        Duration::from_secs(1),
        peer_transport,
        async_peer_transport,
    );
    let response = transport
        .send_metadata_journal_batch_direct(&remote, &command)
        .expect("direct metadata route")
        .expect("production node transport supports direct metadata route");
    assert_eq!(response.verified_authority_probe_id, None);
    assert_eq!(
        peer.calls.lock().expect("calls").as_slice(),
        &[format!(
            "rpc:/dfs/metadata/journal-batch:{remote}:direct-authority-request"
        )]
    );

    let served_peer = Arc::new(RecordingPeerTransport::default());
    let served_peer_transport: Arc<dyn DfsPeerTransport> = served_peer.clone();
    let mut served = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
        local,
        vec![remote.clone()],
        served_peer_transport,
        Duration::from_secs(1),
    );
    let response = served
        .send_metadata_journal_batch_direct(&remote, &command)
        .expect("served direct metadata route")
        .expect("production served transport retains direct peer transport");
    assert_eq!(response.verified_authority_probe_id, None);
    assert_eq!(
        served_peer.calls.lock().expect("calls").as_slice(),
        &[format!(
            "rpc:/dfs/metadata/journal-batch:{remote}:direct-authority-request"
        )]
    );
    std::fs::remove_dir_all(source_root).ok();
}

#[test]
fn direct_metadata_authority_fans_out_to_replicas_concurrently() {
    let local = NodeId::from_legacy_named("node_direct_fanout_local").expect("local");
    let remotes = (0..3)
        .map(|index| {
            NodeId::from_legacy_named(format!("node_direct_fanout_remote_{index}")).expect("remote")
        })
        .collect::<Vec<_>>();
    let peer = Arc::new(ConcurrentMetadataPeerTransport::default());
    let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
    let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
        local.clone(),
        remotes.clone(),
        peer_transport,
        Duration::from_secs(1),
    );
    let shard_id = DfsMetadataShardId::new("root");
    let requests = remotes
        .iter()
        .enumerate()
        .map(|(index, remote)| {
            (
                remote.clone(),
                DfsMetadataJournalBatchCommand {
                    request_id: format!("direct-fanout-{index}"),
                    source_node_id: Some(local.clone()),
                    file_system_id: None,
                    authority_epoch: None,
                    verify_prefix_only: false,
                    prefix_probe: None,
                    authority_probe: None,
                    durable_replication: None,
                    batch: DfsMetadataJournalBatch {
                        shard_id: shard_id.clone(),
                        entries: Vec::new(),
                        next_after_sequence: 9,
                    },
                },
            )
        })
        .collect::<Vec<_>>();

    let responses = transport
        .send_metadata_journal_batches_direct(&requests)
        .expect("direct fan-out")
        .expect("peer transport retained");

    assert_eq!(responses.len(), remotes.len());
    assert!(
        peer.max_active_calls.load(Ordering::SeqCst) >= 2,
        "metadata authority RPCs must overlap instead of adding replica RTTs"
    );
}

#[test]
fn posix_create_cleans_transient_metadata_when_placement_authority_is_not_ready() {
    let root = temp_root("posix-create-authority-not-ready-cleanup");
    let node_id = NodeId::from_legacy_named("node_posix_create_authority_not_ready").expect("node");
    let parent = "/authority-not-ready-cleanup";
    let path = "/authority-not-ready-cleanup/record.json";
    let payload = b"immutable chat metadata";
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.posix_mkdir_all(parent).expect("fixture parent");

    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let file_system = distributed_sifs(node_id.clone());
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id,
                shard_id: "root".to_string(),
                primary_node_id: node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("root metadata shard");
    });
    let fail_reads = Arc::new(AtomicBool::new(true));
    dfs = dfs.with_control_plane(FailingFileSystemConfigReadKvClient {
        inner: shared_kv,
        fail_reads: Arc::clone(&fail_reads),
    });

    let options = DfsWriteOptions::replicated_eventual(vec![node_id]);
    let error = dfs
        .posix_write_file(path, payload, options.clone())
        .expect_err("unready placement authority must reject the create");
    assert!(
        error
            .to_string()
            .contains("daemon DFS retained metadata authority generation 1 is not ready"),
        "unexpected authority error: {error}"
    );
    assert!(
        dfs.lookup_sifs_path(path)
            .expect("lookup after rejected create")
            .is_none(),
        "a rejected create must not remain visible as contentless transient metadata"
    );
    assert!(
        dfs.transient_metadata_object_ids.is_empty(),
        "a rejected create must release its transient namespace overlay"
    );

    fail_reads.store(false, Ordering::SeqCst);
    let committed = dfs
        .posix_write_file(path, payload, options)
        .expect("retry after authority recovery");
    assert_eq!(committed.path, path);
    assert_eq!(
        dfs.posix_read_file(path).expect("read recovered write"),
        payload
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn non_authority_default_bootstrap_publishes_only_its_local_zone() {
    let root = temp_root("non-authority-default-bootstrap-zone");
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_non_authority_zone")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_non_authority_zone_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_non_authority_zone_remote").expect("remote node");
    let mut state = sinomo_core::ClusterBootstrapState::pending_join(
        cluster_id.clone(),
        "dfs non-authority zone",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].state = sinomo_core::ClusterMemberState::Learner;
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
    let mut host = DfsControlPlaneDefaultClusterBootstrapHost::new(
        data_dir,
        cluster_paths,
        local.clone(),
        Box::new(control_plane.clone()),
    );

    ensure_default_distributed_dfs_with_host(
        &state,
        &mut host,
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control: false,
            materialize_local_metadata: false,
            local_zone: Some("zone-local".to_string()),
            local_content_advertisement: None,
        },
    )
    .expect("local service advertisement");

    let mut reader = control_plane;
    let store = DfsControlPlaneStore::new(&mut reader);
    let local_content = store
        .content_service(&local)
        .expect("local content advertisement")
        .expect("local content advertisement present");
    assert_eq!(local_content.zone.as_deref(), Some("zone-local"));
    assert!(
        store
            .content_service(&remote)
            .expect("remote content advertisement")
            .is_none(),
        "a non-authority node must not synthesize another node's zone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dfs_repair_maintenance_gate_tracks_authority_owned_write_work() {
    let authority = NodeId::from_legacy_named("node_dfs_repair_gate_authority").expect("authority");
    let requester = NodeId::from_legacy_named("node_dfs_repair_gate_requester").expect("requester");
    let replica = NodeId::from_legacy_named("node_dfs_repair_gate_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_repair_gate").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_repair_gate").expect("version");
    let file_system_id = sifs_file_system_id();
    let repair_job = DistributedRepairJob {
        intent_id: "intent_dfs_repair_gate".to_string(),
        file_system_id,
        object_id: object_id.clone(),
        version: version.clone(),
        missing_replica_node_id: replica,
    };
    let pending = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Pending {
            intent_id: "intent_dfs_repair_gate_pending".to_string(),
            durable_receipts: 1,
            required_receipts: 2,
        },
    };
    let committed_with_repair = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id: repair_job.intent_id.clone(),
            version: version.clone(),
            content_hash: "blake3:repair-gate".to_string(),
            repair_jobs: vec![repair_job],
        },
    };
    let committed_without_repair = DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id: "intent_dfs_repair_gate_complete".to_string(),
            version,
            content_hash: "blake3:repair-gate-complete".to_string(),
            repair_jobs: Vec::new(),
        },
    };

    assert!(dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_pending".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: pending,
        },
    ]));
    assert!(dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_committed".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: committed_with_repair.clone(),
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_complete".to_string(),
            object_id: object_id.clone(),
            requester_node_id: requester.clone(),
            result: committed_without_repair,
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: "repair_gate_requester_completion".to_string(),
            object_id: object_id.clone(),
            primary_node_id: authority,
            result: committed_with_repair,
        },
    ]));
    assert!(!dfs_events_should_wake_repair_maintenance(&[
        NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id: "repair_gate_rejected".to_string(),
            object_id,
            requester_node_id: requester,
            result: DfsDistributedWriteResult::Rejected {
                reason: "not authoritative".to_string(),
                current_primary_node_id: None,
            },
        },
    ]));
}

#[test]
fn metadata_journal_publish_suppresses_proven_primary_mismatch_for_epoch() {
    let root = temp_root("metadata-journal-publish-suppresses-topology-mismatch");
    let local = NodeId::from_legacy_named("node_meta_suppress_local").expect("local");
    let replica = NodeId::from_legacy_named("node_meta_suppress_replica").expect("replica");
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
        reason: format!(
            "metadata journal source {local} is not shard root primary {}",
            NodeId::from_legacy_named("node_meta_suppress_other_primary").expect("other primary")
        ),
        updated_at_unix_nanos: dfs_now_unix_nanos(),
    };
    let mut host = RecordingMetadataJournalPublisherHost::new(root.join("suppressed"));

    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id.clone(),
        &local,
        vec![shard.clone()],
        &mut host,
        std::slice::from_ref(&suppression),
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare suppressed publish");

    assert!(targets.is_empty());
    assert_eq!(host.opened_shards, vec![shard_id.clone()]);
    assert!(
        host.cursor_requests.is_empty(),
        "suppressed target must not read/update replication cursor"
    );
    assert_eq!(
        outcomes,
        vec![
            DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed {
                target_node_id: replica.clone(),
                shard_id: shard_id.clone(),
                epoch: 7,
                reason: suppression.reason.clone(),
            }
        ]
    );

    let mut host = RecordingMetadataJournalPublisherHost::new(root.join("epoch-changed"));
    let mut changed_epoch_shard = shard;
    changed_epoch_shard.epoch = 8;
    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id,
        &local,
        vec![changed_epoch_shard],
        &mut host,
        &[suppression],
        DfsMetadataJournalReplicationOptions::default(),
    )
    .expect("prepare changed-epoch publish");

    assert!(targets.is_empty());
    assert_eq!(
        host.cursor_requests,
        vec![(replica.clone(), shard_id.clone())],
        "shard epoch changes must invalidate topology suppression"
    );
    assert_eq!(
        outcomes,
        vec![DfsMetadataJournalReplicationOutcome::NoChanges {
            target_node_id: replica,
            shard_id,
            after_sequence: 0,
        }]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn clustered_metadata_journal_batch_requires_exact_authority_identity() {
    let primary = NodeId::from_legacy_named("node_meta_identity_primary").expect("primary node id");
    let replica = NodeId::from_legacy_named("node_meta_identity_replica").expect("replica node id");
    let file_system = file_system(primary.clone());
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![replica],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
    };
    let exact = DfsMetadataJournalBatchCommand {
        request_id: "request-exact-journal-authority".to_string(),
        source_node_id: Some(primary),
        file_system_id: Some(file_system.file_system_id),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: Vec::new(),
            next_after_sequence: 0,
        },
    };

    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &exact),
        None
    );

    let mut missing = exact.clone();
    missing.file_system_id = None;
    missing.authority_epoch = None;
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &missing),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("authority identity")
    ));

    let mut stale = exact;
    stale.authority_epoch = Some(context.shard.epoch - 1);
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &stale),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("authority identity")
    ));
}

#[test]
fn networked_metadata_journal_batch_requires_exact_authority_identity() {
    let root = temp_root("networked-metadata-journal-authority-identity");
    let primary =
        NodeId::from_legacy_named("node_service_identity_primary").expect("primary node id");
    let replica =
        NodeId::from_legacy_named("node_service_identity_replica").expect("replica node id");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 12,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard)
            .expect("shard config")
    });
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(backend)
    .expect("receiver service");
    let command = DfsMetadataJournalBatchCommand {
        request_id: "request-service-journal-authority".to_string(),
        source_node_id: Some(primary.clone()),
        file_system_id: None,
        authority_epoch: None,
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: Vec::new(),
            next_after_sequence: 0,
        },
    };

    let missing = service
        .handle_metadata_journal_batch(primary.clone(), command.clone())
        .expect("missing identity rejection");
    assert!(matches!(
        missing,
        NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
            if reason.contains("missing exact filesystem and authority epoch identity")
    ));

    let mut stale = command;
    stale.file_system_id = Some(shard.file_system_id.clone());
    stale.authority_epoch = Some(shard.epoch - 1);
    let stale = service
        .handle_metadata_journal_batch(primary, stale)
        .expect("stale identity rejection");
    assert!(matches!(
        stale,
        NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
            if reason.contains("authority identity")
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn networked_metadata_journal_batch_refreshes_stale_receiver_authority() {
    let root = temp_root("networked-metadata-journal-refreshes-authority");
    let primary = NodeId::from_legacy_named("node_meta_refresh_primary").expect("primary node id");
    let replica = NodeId::from_legacy_named("node_meta_refresh_replica").expect("replica node id");
    let file_system = file_system(primary.clone());
    let shard = |epoch| DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    current.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard(12))
            .expect("current shard config")
    });
    let mut stale = InProcessKvStore::new();
    DfsControlPlaneStore::new(&mut stale)
        .put_metadata_shard(&shard(11))
        .expect("stale shard config");
    let refreshes = Arc::new(AtomicUsize::new(0));
    let control_plane =
        RefreshingDfsControlPlaneKvClient::new(stale, current, Arc::clone(&refreshes));
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(backend)
    .expect("receiver service");

    let event = service
        .handle_metadata_journal_batch(
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "request-refresh-journal-authority".to_string(),
                source_node_id: Some(primary),
                file_system_id: Some(file_system.file_system_id),
                authority_epoch: Some(12),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: DfsMetadataShardId::new("root"),
                    entries: Vec::new(),
                    next_after_sequence: 0,
                },
            },
        )
        .expect("current journal authority is accepted after refresh");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
    ));
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn replica_command_refreshes_stale_receiver_authority_before_cache_decisions() {
    let root = temp_root("replica-command-refreshes-stale-authority");
    let primary =
        NodeId::from_legacy_named("node_replica_refresh_primary").expect("primary node id");
    let replica =
        NodeId::from_legacy_named("node_replica_refresh_replica").expect("replica node id");
    let mut object = file_metadata(primary.clone());
    object.object_id = DfsObjectId::from_legacy_named("object_replica_refresh").expect("object id");
    object.path = "/services/replica-refresh.bin".to_string();
    let shard = |primary_node_id: NodeId, epoch| DfsMetadataShardConfig {
        file_system_id: object.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id,
        replica_node_ids: vec![replica.clone(), primary.clone()],
        epoch,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let stale_shard = shard(replica.clone(), 16);
    let current_shard = shard(primary.clone(), 17);
    let current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    current.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&current_shard)
            .expect("current shard config")
    });
    let mut stale = InProcessKvStore::new();
    DfsControlPlaneStore::new(&mut stale)
        .put_metadata_shard(&stale_shard)
        .expect("stale shard config");
    let refreshes = Arc::new(AtomicUsize::new(0));
    let control_plane =
        RefreshingDfsControlPlaneKvClient::new(stale, current, Arc::clone(&refreshes));
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("services"), root.join("metadata"))
            .expect("metadata backend");
    let mut service = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: replica,
        },
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(backend)
    .expect("receiver service");

    service
        .ensure_replica_command_authorized_with_metadata(
            &primary,
            "intent_replica_refresh",
            &object.object_id,
            Some(&object),
            Some(&current_shard),
        )
        .expect("authenticated current primary is accepted after refresh");

    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        service
            .command_authority_shard_for_object(Some(&object))
            .expect("refreshed authority lookup"),
        None,
        "the content replica must not retain the retiring-primary authority view"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_authority_probe_requires_exact_cluster_context() {
    let root = temp_root("metadata-authority-probe-exact-context");
    let primary = NodeId::from_legacy_named("node_meta_authority_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_authority_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 3,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source metadata backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    let command = DfsMetadataJournalBatchCommand {
        request_id: "request-authority-exact-context".to_string(),
        source_node_id: Some(primary),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(DfsMetadataAuthorityProbe {
            probe_id: "probe-authority-exact-context".to_string(),
            expected_shard: shard.clone(),
            operation: DfsMetadataAuthorityOperation::NamespaceMutation {
                operation_id: "namespace-authority-exact-context".to_string(),
                file_system_id: file_system.file_system_id,
                prepare_sequence: batch.next_after_sequence,
                payload_digest: "blake3:authority-exact-context".to_string(),
            },
            durable_journal_ack: false,
            journal_frames: Vec::new(),
            prepared_prefix: None,
        }),
        durable_replication: None,
        batch,
    };
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");

    let missing_context = process_dfs_metadata_journal_batch(&mut backend, None, command.clone());
    assert!(matches!(
        missing_context.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("exact committed cluster shard context")
    ));
    assert_eq!(missing_context.verified_authority_probe_id, None);

    let exact_context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: shard.clone(),
    };
    let mut empty_command = command.clone();
    empty_command.batch.entries.clear();
    let empty =
        process_dfs_metadata_journal_batch(&mut backend, Some(&exact_context), empty_command);
    assert!(matches!(
        empty.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("exact committed assignment")
    ));
    assert_eq!(empty.verified_authority_probe_id, None);

    let mut stale_context_shard = shard;
    stale_context_shard.epoch += 1;
    let stale_context = DfsMetadataReplicaShardContext {
        local_node_id: replica,
        pending_membership_handoff: None,
        shard: stale_context_shard,
    };
    let stale = process_dfs_metadata_journal_batch(&mut backend, Some(&stale_context), command);
    assert!(matches!(
        stale.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("exact committed assignment")
    ));
    assert_eq!(stale.verified_authority_probe_id, None);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_authority_probe_applies_batch_before_echoing_nonce() {
    let root = temp_root("metadata-authority-probe-applies-first");
    let primary = NodeId::from_legacy_named("node_meta_authority_apply_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_authority_apply_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 4,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV2,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica,
        pending_membership_handoff: None,
        shard: shard.clone(),
    };
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source metadata backend");
    source.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV2);
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let rollback_sequence = source.journal_last_sequence();
    let rollback_snapshot = source.export_table_snapshot();
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/authority-applies-first".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    let transaction = DfsNamespaceTransaction {
        transaction_id: "transaction-authority-applies-first".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![object.clone()],
        versions: vec![object_version_record(&object, object.content_size_bytes)],
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::now_utc(),
    };
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "namespace-authority-applies-first".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: shard.epoch,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: None,
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: object.path.clone(),
            expected_object_id: None,
        }],
        content_receipts: Vec::new(),
        transaction,
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    source
        .prepare_namespace_transaction(&prepare)
        .expect("source namespace prepare");
    let batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    assert!(!batch.entries.is_empty());
    assert!(batch.entries.iter().any(|entry| matches!(
        entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: None,
            packed_prepare_v1: None,
            packed_prepare_v2: Some(_),
            ..
        }
    )));
    let authority_sequence = batch.next_after_sequence;
    let prepared_prefix = source
        .metadata_prefix_proof()
        .expect("prepared source prefix");
    let mut receiver = LocalSobjMetadataBackend::new(shard_id, root.join("receiver"))
        .expect("receiver metadata backend");
    let probe_id = "probe-authority-applies-first".to_string();
    let operation = crate::service_replica::metadata_authority_operation_for_namespace_prepare(
        &prepare,
        batch.next_after_sequence,
    )
    .expect("namespace authority operation");
    let authority_probe = DfsMetadataAuthorityProbe {
        probe_id,
        expected_shard: shard,
        operation,
        durable_journal_ack: false,
        journal_frames: Vec::new(),
        prepared_prefix: Some(prepared_prefix),
    };
    let confirmation_id =
        crate::service_replica::metadata_authority_probe_confirmation_id(&authority_probe)
            .expect("prefix-bound authority confirmation");
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let response = process_dfs_metadata_journal_batch(
        &mut receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-applies-first".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(authority_probe.clone()),
            durable_replication: None,
            batch: batch.clone(),
        },
    );

    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert!(response.applied_entries > 0);
    assert_eq!(response.verified_authority_probe_id, Some(confirmation_id));
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "standalone PackedV2 apply and authority ACK must share one canonical digest"
    );
    assert_eq!(
        receiver
            .metadata_authority_ack_floor(&DfsMetadataShardId::new("root"))
            .expect("authority acknowledgement floor"),
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: DfsMetadataShardId::new("root"),
            journal_sequence: authority_sequence,
            durable_prefix_proof: None,
        })
    );

    let exact_frames = DfsMetadataBackend::journal_frames_for_batch(&source, &batch)
        .expect("source exact journal frames");
    let mut durable_probe = authority_probe.clone();
    durable_probe.durable_journal_ack = true;
    durable_probe.journal_frames = exact_frames.clone();
    let durable_prefix = durable_probe
        .prepared_prefix
        .clone()
        .expect("durable prepared prefix");
    let durable_confirmation_id =
        crate::service_replica::metadata_authority_probe_confirmation_id(&durable_probe)
            .expect("durable prefix-bound authority confirmation");
    let durable_wire_batch = DfsMetadataJournalBatch {
        shard_id: DfsMetadataShardId::new("root"),
        entries: Vec::new(),
        next_after_sequence: authority_sequence,
    };
    let mut durable_receiver = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("durable-receiver"),
    )
    .expect("durable receiver metadata backend");
    crate::metadata_journal::reset_metadata_journal_tail_recovery_calls();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let durable_response = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-frame".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert_eq!(
        durable_response.result,
        DfsMetadataJournalBatchResult::Applied
    );
    assert!(durable_response.applied_entries > 0);
    assert_eq!(
        durable_response.verified_authority_probe_id,
        Some(durable_confirmation_id.clone())
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        0,
        "durable PackedV2 ingest must validate its exact retained identity without canonical expansion"
    );
    assert_eq!(
        crate::metadata_journal::metadata_journal_tail_recovery_calls(),
        0,
        "a resident durable ACK must trust its maintained head instead of rerunning crash recovery"
    );
    assert_eq!(durable_receiver.journal_last_sequence(), authority_sequence);
    assert!(
        durable_receiver
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable receiver prepare before materialization")
            .is_none(),
        "durable ACK must not wait for rebuildable table expansion"
    );
    assert_eq!(
        durable_receiver
            .metadata_authority_ack_floor(&DfsMetadataShardId::new("root"))
            .expect("durable authority acknowledgement floor"),
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: DfsMetadataShardId::new("root"),
            journal_sequence: authority_sequence,
            durable_prefix_proof: Some(durable_prefix.clone()),
        })
    );

    let durable_retry = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-frame-retry".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert_eq!(durable_retry.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(durable_retry.applied_entries, 0);
    assert_eq!(
        durable_retry.verified_authority_probe_id,
        Some(durable_confirmation_id.clone())
    );
    assert!(
        durable_receiver
            .materialize_pending_replica_journal()
            .expect("materialize durable journal tail")
            > 0
    );
    let assert_canonical_prepare = |actual: Option<DfsNamespacePrepareRecord>| {
        let actual = actual.expect("canonical namespace prepare");
        assert_eq!(actual.operation_id, prepare.operation_id);
        assert_eq!(actual.operation_digest, prepare.operation_digest);
        assert_eq!(actual.owner_epoch, prepare.owner_epoch);
        assert_eq!(actual.status, DfsNamespacePrepareStatus::Prepared);
    };
    assert_canonical_prepare(
        durable_receiver
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable receiver prepare after materialization"),
    );
    assert_eq!(
        durable_receiver
            .metadata_prefix_proof()
            .expect("durable receiver materialized prefix"),
        durable_prefix
    );

    let materialized_retry = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-materialized-retry".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert_eq!(
        materialized_retry.result,
        DfsMetadataJournalBatchResult::Applied
    );
    assert_eq!(materialized_retry.applied_entries, 0);
    assert_eq!(
        materialized_retry.verified_authority_probe_id,
        Some(durable_confirmation_id.clone()),
        "an exact retry must retain its acknowledged target after async materialization"
    );

    let mut advanced_file_system = file_system.clone();
    advanced_file_system.display_name = "advanced past durable retry".to_string();
    durable_receiver
        .put_file_system(&advanced_file_system)
        .expect("advance materialized journal past acknowledged target");
    assert!(durable_receiver.journal_last_sequence() > authority_sequence);
    let advanced_materialized_retry = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-advanced-materialized-retry".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert_eq!(
        advanced_materialized_retry.result,
        DfsMetadataJournalBatchResult::Applied
    );
    assert_eq!(advanced_materialized_retry.applied_entries, 0);
    assert_eq!(
        advanced_materialized_retry.verified_authority_probe_id,
        Some(durable_confirmation_id.clone()),
        "an exact floor must preserve the acknowledged proof after later materialization"
    );

    let mut divergent_retry_probe = durable_probe.clone();
    let mut divergent_retry_entry = crate::metadata_journal::decode_metadata_journal_frame_record(
        divergent_retry_probe
            .journal_frames
            .first()
            .expect("durable retry frame"),
    )
    .expect("decode durable retry frame");
    divergent_retry_entry.created_at += time::Duration::nanoseconds(1);
    divergent_retry_probe.journal_frames[0] =
        crate::metadata_journal::encode_metadata_journal_frame_record(&divergent_retry_entry)
            .expect("encode divergent retry frame");
    let divergent_materialized_retry = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-divergent-materialized-retry".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(divergent_retry_probe),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert!(matches!(
        divergent_materialized_retry.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("different exact bytes")
    ));
    let advanced_sequence = durable_receiver.journal_last_sequence();
    let advanced_prefix = durable_receiver
        .metadata_prefix_proof()
        .expect("advanced materialized prefix");
    durable_receiver
        .put_metadata_authority_ack_floor(&DfsMetadataAuthorityAckFloor {
            shard_id: DfsMetadataShardId::new("root"),
            journal_sequence: advanced_sequence,
            durable_prefix_proof: Some(advanced_prefix),
        })
        .expect("advance exact authority floor");
    let retry_behind_advanced_floor = process_dfs_metadata_journal_batch(
        &mut durable_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-behind-advanced-floor".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert!(matches!(
        retry_behind_advanced_floor.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("behind authority floor")
    ));

    let mut crash_receiver = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("durable-crash-receiver"),
    )
    .expect("durable crash receiver metadata backend");
    let crash_response = process_dfs_metadata_journal_batch(
        &mut crash_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-durable-crash".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(durable_probe.clone()),
            durable_replication: None,
            batch: durable_wire_batch.clone(),
        },
    );
    assert_eq!(
        crash_response.result,
        DfsMetadataJournalBatchResult::Applied
    );
    assert!(
        crash_receiver
            .get_namespace_prepare(&prepare.operation_id)
            .expect("crash receiver prepare before reopen")
            .is_none()
    );
    drop(crash_receiver);
    let reopened_crash_receiver = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("durable-crash-receiver"),
    )
    .expect("reopen durable crash receiver");
    assert_canonical_prepare(
        reopened_crash_receiver
            .get_namespace_prepare(&prepare.operation_id)
            .expect("crash receiver prepare after reopen"),
    );
    assert_eq!(
        reopened_crash_receiver
            .metadata_prefix_proof()
            .expect("crash receiver prefix after reopen"),
        durable_probe
            .prepared_prefix
            .clone()
            .expect("crash receiver expected prefix")
    );

    let mut malformed_probe = durable_probe;
    let mut malformed_bytes = malformed_probe.journal_frames[0].exact_bytes.to_vec();
    malformed_bytes[0] ^= 0xff;
    malformed_probe.journal_frames[0].exact_bytes = bytes::Bytes::from(malformed_bytes);
    let mut malformed_receiver = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("malformed-frame-receiver"),
    )
    .expect("malformed frame receiver");
    let malformed = process_dfs_metadata_journal_batch(
        &mut malformed_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-malformed-frame".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(malformed_probe),
            durable_replication: None,
            batch: durable_wire_batch,
        },
    );
    assert!(matches!(
        malformed.result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    assert_eq!(malformed_receiver.journal_last_sequence(), 0);
    assert_eq!(
        malformed_receiver
            .metadata_authority_ack_floor(&DfsMetadataShardId::new("root"))
            .expect("malformed authority floor"),
        None
    );

    let mut divergent_probe = authority_probe;
    divergent_probe
        .prepared_prefix
        .as_mut()
        .expect("prepared prefix")
        .digest = format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64));
    let mut divergent_receiver = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("divergent-receiver"),
    )
    .expect("divergent receiver metadata backend");
    let rejected = process_dfs_metadata_journal_batch(
        &mut divergent_receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-divergent-prefix".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(divergent_probe),
            durable_replication: None,
            batch,
        },
    );
    assert!(matches!(
        rejected.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("prefix does not match")
    ));
    assert_eq!(
        divergent_receiver
            .metadata_authority_ack_floor(&DfsMetadataShardId::new("root"))
            .expect("divergent authority floor"),
        None,
        "a divergent prefix must remain snapshot-repairable"
    );
    assert_eq!(
        receiver
            .get_file_system(&file_system.file_system_id)
            .expect("receiver filesystem"),
        Some(file_system)
    );
    let rollback = process_dfs_metadata_snapshot_install(
        &mut receiver,
        Some(&context),
        DfsMetadataSnapshotInstallCommand {
            request_id: "request-authority-rollback-snapshot".to_string(),
            source_node_id: primary,
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("root"),
            next_after_sequence: rollback_sequence,
            snapshot: rollback_snapshot,
            reason: "in-flight snapshot predating authority acknowledgement".to_string(),
        },
    );
    assert!(matches!(
        rollback.result,
        DfsMetadataSnapshotInstallResult::Rejected { ref reason , ..}
            if reason.contains("locally acknowledged authority operation")
    ));
    let retained_prepare = receiver
        .get_namespace_prepare(&prepare.operation_id)
        .expect("prepare after rejected snapshot")
        .expect("acknowledged prepare retained");
    assert_eq!(retained_prepare.operation_id, prepare.operation_id);
    assert_eq!(retained_prepare.operation_digest, prepare.operation_digest);
    assert_eq!(retained_prepare.owner_epoch, prepare.owner_epoch);
    assert_eq!(retained_prepare.status, DfsNamespacePrepareStatus::Prepared);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_authority_probe_rejects_a_gap_before_the_hidden_prepare() {
    let root = temp_root("metadata-authority-probe-rejects-gap");
    let primary = NodeId::from_legacy_named("node_meta_authority_gap_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_authority_gap_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 5,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica,
        pending_membership_handoff: None,
        shard: shard.clone(),
    };
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source metadata backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    source
        .put_file_system(&file_system)
        .expect("source gap entry");
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/authority-gap".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "namespace-authority-gap".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: shard.epoch,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: None,
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: object.path.clone(),
            expected_object_id: None,
        }],
        content_receipts: Vec::new(),
        transaction: DfsNamespaceTransaction {
            transaction_id: "transaction-authority-gap".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: vec![object_version_record(&object, object.content_size_bytes)],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        },
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    source
        .prepare_namespace_transaction(&prepare)
        .expect("source namespace prepare");
    let source_batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    assert_eq!(source_batch.entries.len(), 3);
    let prepared_prefix = source
        .metadata_prefix_proof()
        .expect("prepared source prefix");
    let prepare_entry = source_batch.entries.last().expect("prepare entry").clone();
    let prepare_sequence = prepare_entry.sequence;
    let mut receiver = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("receiver"))
        .expect("receiver metadata backend");
    receiver
        .apply_journal_batch(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            entries: vec![source_batch.entries[0].clone()],
            next_after_sequence: source_batch.entries[0].sequence,
        })
        .expect("apply first prefix entry");
    assert_eq!(
        receiver
            .metadata_replay_cursor(&shard_id)
            .expect("receiver replay cursor"),
        1
    );

    let response = process_dfs_metadata_journal_batch(
        &mut receiver,
        Some(&context),
        DfsMetadataJournalBatchCommand {
            request_id: "request-authority-gap".to_string(),
            source_node_id: Some(primary),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(DfsMetadataAuthorityProbe {
                probe_id: "probe-authority-gap".to_string(),
                expected_shard: shard,
                operation:
                    crate::service_replica::metadata_authority_operation_for_namespace_prepare(
                        &prepare,
                        prepare_sequence,
                    )
                    .expect("authority operation"),
                durable_journal_ack: false,
                journal_frames: Vec::new(),
                prepared_prefix: Some(prepared_prefix),
            }),
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries: vec![prepare_entry],
                next_after_sequence: prepare_sequence,
            },
        },
    );

    assert!(matches!(
        response.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("durable replay requires sequence 2")
    ));
    assert_eq!(response.verified_authority_probe_id, None);
    assert_eq!(
        receiver
            .metadata_replay_cursor(&shard_id)
            .expect("unchanged receiver replay cursor"),
        1
    );
    assert_eq!(
        receiver
            .get_namespace_prepare(&prepare.operation_id)
            .expect("missing skipped prepare"),
        None
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_authority_sender_fills_each_targets_missing_journal_prefix() {
    let root = temp_root("metadata-authority-sender-fills-prefix");
    let primary = NodeId::from_legacy_named("node_meta_authority_prefix_primary").expect("primary");
    let target = NodeId::from_legacy_named("node_meta_authority_prefix_target").expect("target");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("metadata"))
        .expect("metadata backend");
    for _ in 0..3 {
        backend
            .put_file_system(&file_system)
            .expect("source journal entry");
    }
    let complete = backend
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source batch");
    assert_eq!(
        complete
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    backend
        .put_metadata_replication_cursor(&target, &shard_id, 1)
        .expect("target cursor");
    let prepared_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        entries: vec![complete.entries[2].clone()],
        next_after_sequence: 3,
    };
    let mut dfs = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: primary,
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend(backend)
    .expect("metadata backend");

    let catch_up = dfs
        .metadata_authority_batch_for_target(&target, &prepared_batch)
        .expect("authority catch-up batch")
        .expect("bounded prefix reaches prepare");
    assert_eq!(
        catch_up
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(catch_up.next_after_sequence, 3);

    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_metadata_replication_cursor(&target, &shard_id, 3)
        .expect("advance target cursor");
    let retry = dfs
        .metadata_authority_batch_for_target(&target, &prepared_batch)
        .expect("authority retry batch")
        .expect("exact retry batch");
    assert_eq!(
        retry
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![3]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn pending_membership_handoff_authorizes_exact_suffix_and_prefix_for_new_replica() {
    let primary = NodeId::from_legacy_named("node_meta_handoff_primary").expect("primary node id");
    let retiring_replica = NodeId::from_legacy_named("node_meta_handoff_retiring_replica")
        .expect("retiring replica node id");
    let desired_replica = NodeId::from_legacy_named("node_meta_handoff_desired_replica")
        .expect("desired replica node id");
    let file_system_id =
        DfsFileSystemId::from_legacy_named("metadata_handoff_receiver").expect("filesystem id");
    let shard_id = DfsMetadataShardId::new("root");
    let retiring_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![retiring_replica],
        epoch: 8,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![desired_replica.clone()],
        epoch: 9,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: desired_replica,
        shard: retiring_shard.clone(),
        pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
            handoff_id: "membership-handoff-receiver".to_string(),
            retiring_shard,
            desired_shard,
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        }),
    };
    let exact_prefix_probe = DfsMetadataJournalBatchCommand {
        request_id: "request-membership-handoff-prefix".to_string(),
        source_node_id: Some(primary),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id: "barrier-membership-handoff-prefix".to_string(),
            file_system_id: Some(file_system_id),
            authority_epoch: Some(context.shard.epoch),
            shard_id: shard_id.clone(),
            journal_sequence: 4,
            state_fingerprint: "blake3:membership-handoff-state".to_string(),
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id,
            entries: Vec::new(),
            next_after_sequence: 4,
        },
    };

    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &exact_prefix_probe),
        None,
        "the pending desired replica must accept the exact retiring-primary prefix proof"
    );

    let mut ordinary_batch = exact_prefix_probe.clone();
    ordinary_batch.verify_prefix_only = false;
    ordinary_batch.prefix_probe = None;
    assert_eq!(
        validate_dfs_metadata_journal_batch_source(Some(&context), &ordinary_batch),
        None,
        "the exact pending handoff must permit the retiring-primary suffix before the prefix proof"
    );

    let mut unfenced_context = context;
    unfenced_context.pending_membership_handoff = None;
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&unfenced_context), &exact_prefix_probe),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("neither a configured replica nor an exact pending membership-handoff catch-up target")
    ));
    assert!(matches!(
        validate_dfs_metadata_journal_batch_source(Some(&unfenced_context), &ordinary_batch),
        Some(DfsMetadataJournalBatchResponse {
            result: DfsMetadataJournalBatchResult::Rejected { ref reason , ..},
            ..
        }) if reason.contains("neither a configured replica nor an exact pending membership-handoff catch-up target")
    ));
}

#[test]
fn ordinary_durable_catchup_does_not_mask_delayed_authority_operation() {
    let root = temp_root("metadata-handoff-durable-catchup");
    let primary =
        NodeId::from_legacy_named("node_meta_handoff_durable_primary").expect("primary node id");
    let retiring_replica =
        NodeId::from_legacy_named("node_meta_handoff_durable_retiring").expect("retiring replica");
    let desired_replica =
        NodeId::from_legacy_named("node_meta_handoff_durable_desired").expect("desired replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let retiring_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![retiring_replica, desired_replica.clone()],
        epoch: 8,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired_shard = DfsMetadataShardConfig {
        replica_node_ids: vec![desired_replica.clone()],
        epoch: retiring_shard.epoch + 1,
        ..retiring_shard.clone()
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: desired_replica.clone(),
        shard: retiring_shard.clone(),
        pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
            handoff_id: "membership-handoff-durable-catchup".to_string(),
            retiring_shard: retiring_shard.clone(),
            desired_shard,
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        }),
    };

    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source"))
        .expect("source metadata backend");
    source
        .put_file_system(&file_system)
        .expect("source filesystem");
    let mut object = file_metadata(primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.path = "/delayed-authority-after-catchup".to_string();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    let transaction = DfsNamespaceTransaction {
        transaction_id: "transaction-delayed-authority-after-catchup".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        objects: vec![object.clone()],
        versions: vec![object_version_record(&object, object.content_size_bytes)],
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::now_utc(),
    };
    let mut prepare = DfsNamespacePrepareRecord {
        operation_id: "namespace-delayed-authority-after-catchup".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.clone(),
        owner_epoch: retiring_shard.epoch,
        operation_digest: String::new(),
        expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
            object_id: object.object_id.clone(),
            expected_version: None,
        }],
        expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
            path: object.path.clone(),
            expected_object_id: None,
        }],
        content_receipts: Vec::new(),
        transaction,
        completed_request: None,
        completed_request_completion: None,
        status: DfsNamespacePrepareStatus::Prepared,
    };
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    let prepare_entry = source
        .prepare_namespace_transaction(&prepare)
        .expect("source namespace prepare");
    let authority_sequence = prepare_entry.sequence;
    let authority_prefix = source
        .metadata_prefix_proof()
        .expect("authority operation prefix");
    let authority_batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("authority source journal")
        .expect("authority source batch");
    assert_eq!(authority_batch.next_after_sequence, authority_sequence);
    let authority_frames = DfsMetadataBackend::journal_frames_for_batch(&source, &authority_batch)
        .expect("authority exact frames");
    let authority_probe = DfsMetadataAuthorityProbe {
        probe_id: "probe-delayed-authority-after-catchup".to_string(),
        expected_shard: retiring_shard.clone(),
        operation: crate::service_replica::metadata_authority_operation_for_namespace_prepare(
            &prepare,
            authority_sequence,
        )
        .expect("authority operation"),
        durable_journal_ack: true,
        journal_frames: authority_frames,
        prepared_prefix: Some(authority_prefix.clone()),
    };
    let authority_confirmation =
        crate::service_replica::metadata_authority_probe_confirmation_id(&authority_probe)
            .expect("authority confirmation");

    let mut later_file_system = file_system.clone();
    later_file_system.display_name = "ordinary catch-up past delayed authority".to_string();
    source
        .put_file_system(&later_file_system)
        .expect("advance ordinary source journal");
    let source_batch = source
        .journal_batch_since_limited(
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("source journal")
        .expect("source journal batch");
    let target_sequence = source_batch.next_after_sequence;
    let expected_prefix = source.metadata_prefix_proof().expect("source prefix");
    let prepared = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
        &source,
        &source_batch,
    )
    .expect("prepare exact catch-up frames");
    let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &desired_replica,
        &file_system.file_system_id,
        retiring_shard.epoch,
        prepared,
    );
    let expected_confirmation = command
        .durable_replication
        .as_ref()
        .expect("durable replication carrier")
        .confirmation_id
        .clone();
    assert!(command.authority_probe.is_none());
    assert!(command.batch.entries.is_empty());

    let mut replica = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica"))
        .expect("replica metadata backend");
    let response =
        process_dfs_metadata_journal_batch(&mut replica, Some(&context), command.clone());

    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(response.next_after_sequence, target_sequence);
    assert_eq!(
        response.verified_authority_probe_id,
        Some(expected_confirmation)
    );
    assert_eq!(replica.journal_last_sequence(), target_sequence);
    assert!(target_sequence > authority_sequence);
    assert!(
        replica
            .get_file_system(&file_system.file_system_id)
            .expect("filesystem before materialization")
            .is_none(),
        "ordinary handoff catch-up ACK must not wait for rebuildable table expansion"
    );
    assert_eq!(
        replica
            .metadata_authority_ack_floor(&shard_id)
            .expect("operation-specific authority floor"),
        None,
        "ordinary catch-up must not alias the operation-specific authority floor"
    );
    assert_eq!(
        replica
            .metadata_prefix_ack_floor(&shard_id)
            .expect("ordinary durable prefix floor"),
        Some(DfsMetadataPrefixAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: target_sequence,
            state_fingerprint: String::new(),
            durable_prefix_proof: Some(expected_prefix.clone()),
        })
    );

    let delayed_command = || DfsMetadataJournalBatchCommand {
        request_id: "request-delayed-authority-after-catchup".to_string(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(context.shard.file_system_id.clone()),
        authority_epoch: Some(context.shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(authority_probe.clone()),
        durable_replication: None,
        batch: DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            entries: Vec::new(),
            next_after_sequence: authority_sequence,
        },
    };

    let mut materialized_first_replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("materialized-first-replica"))
            .expect("materialized-first replica metadata backend");
    let materialized_first_catchup = process_dfs_metadata_journal_batch(
        &mut materialized_first_replica,
        Some(&context),
        command,
    );
    assert_eq!(
        materialized_first_catchup.result,
        DfsMetadataJournalBatchResult::Applied
    );
    assert!(
        materialized_first_replica
            .materialize_pending_replica_journal()
            .expect("materialize catch-up before first authority operation")
            > 0
    );
    assert_eq!(
        materialized_first_replica
            .metadata_prefix_proof()
            .expect("materialized-first prefix"),
        expected_prefix
    );
    assert_eq!(
        materialized_first_replica
            .metadata_authority_ack_floor(&shard_id)
            .expect("materialized-first authority floor before operation"),
        None
    );
    let materialized_first_delayed = process_dfs_metadata_journal_batch(
        &mut materialized_first_replica,
        Some(&context),
        delayed_command(),
    );
    assert_eq!(
        materialized_first_delayed.result,
        DfsMetadataJournalBatchResult::Applied,
        "the first delayed authority delivery at M must remain provable after tables materialize through N"
    );
    assert_eq!(materialized_first_delayed.applied_entries, 0);
    assert_eq!(
        materialized_first_delayed.verified_authority_probe_id,
        Some(authority_confirmation.clone())
    );
    assert_eq!(
        materialized_first_replica
            .metadata_authority_ack_floor(&shard_id)
            .expect("materialized-first delayed operation authority floor"),
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: authority_sequence,
            durable_prefix_proof: Some(authority_prefix.clone()),
        })
    );

    let delayed =
        process_dfs_metadata_journal_batch(&mut replica, Some(&context), delayed_command());
    assert_eq!(delayed.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(delayed.applied_entries, 0);
    assert_eq!(
        delayed.verified_authority_probe_id,
        Some(authority_confirmation.clone())
    );
    assert_eq!(
        replica
            .metadata_authority_ack_floor(&shard_id)
            .expect("delayed operation authority floor"),
        Some(DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: authority_sequence,
            durable_prefix_proof: Some(authority_prefix.clone()),
        }),
        "a monotonic ordinary prefix floor at N must not mask exact operation M"
    );
    assert!(
        replica
            .materialize_pending_replica_journal()
            .expect("materialize deferred catch-up")
            > 0
    );
    let retry = process_dfs_metadata_journal_batch(&mut replica, Some(&context), delayed_command());
    assert_eq!(retry.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(retry.applied_entries, 0);
    assert_eq!(
        retry.verified_authority_probe_id,
        Some(authority_confirmation),
        "the exact M receipt must remain provable after deferred tables materialize through N"
    );
    assert_eq!(
        replica
            .metadata_prefix_ack_floor(&shard_id)
            .expect("ordinary prefix floor after delayed retry"),
        Some(DfsMetadataPrefixAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: target_sequence,
            state_fingerprint: String::new(),
            durable_prefix_proof: Some(expected_prefix),
        }),
        "operation retries must not move the ordinary prefix floor backward"
    );
    assert_eq!(
        replica
            .get_file_system(&file_system.file_system_id)
            .expect("filesystem after materialization"),
        Some(later_file_system)
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn distributed_write_uses_path_selected_metadata_shard_epoch() {
    let root = temp_root("distributed-write-path-shard-epoch");
    let owner = NodeId::from_legacy_named("node_path_shard_owner").expect("owner");
    let root_primary =
        NodeId::from_legacy_named("node_path_shard_root_primary").expect("root primary");
    let replica = NodeId::from_legacy_named("node_path_shard_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_path_shard_epoch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let file_system = file_system(owner.clone());
    let shard_configs = vec![
        DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: root_primary.clone(),
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
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.file_system_id = file_system.file_system_id.clone();
    metadata.path = "/services/path-shard.txt".to_string();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"path shard bytes".to_vec(),
            vec![owner, replica],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("path shard write");

    let NetworkedDfsWriteOutcome::Committed { intent_id, .. } = write else {
        panic!("expected committed write, got {write:?}");
    };
    let intent = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_intent(&object_id, &intent_id)
        .expect("read intent")
        .expect("intent");
    assert_eq!(intent.owner_epoch, 7);
    let services_batch = dfs
        .metadata_backend()
        .expect("metadata backend")
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("services journal")
        .expect("services entries");
    assert!(
        services_batch.entries.iter().any(|entry| matches!(
            entry.mutation,
            DfsMetadataMutation::PutIntent { .. } | DfsMetadataMutation::PutVersion { .. }
        )),
        "distributed write metadata must be journaled on the selected services shard"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_write_response_rejects_same_primary_stale_dispatch_epoch_without_mutation() {
    let root = temp_root("routed-write-stale-dispatch-epoch");
    let requester = NodeId::from_legacy_named("node_routed_epoch_requester").expect("requester");
    let primary = NodeId::from_legacy_named("node_routed_epoch_primary").expect("primary");
    let object_id = DfsObjectId::from_legacy_named("obj_routed_stale_epoch").expect("object");
    let version = DfsVersion::from_legacy_named("version_routed_stale_epoch").expect("version");
    let path = "/services/stale-dispatch-epoch.txt";
    let batch = test_routed_response_journal_batch(
        &root.join("source-metadata"),
        &primary,
        &requester,
        &object_id,
        &version,
        path,
    );
    let dispatch_authority = test_routed_authority_shard(&primary, 7);
    let current_authority = test_routed_authority_shard(&primary, 8);
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
        "dfs_net_req_stale_epoch".to_string(),
        test_pending_routed_write(
            "dfs_net_req_stale_epoch",
            &requester,
            &object_id,
            &primary,
            dispatch_authority,
        ),
    );

    let event = dfs
        .handle_distributed_write_response(
            primary,
            DfsDistributedWriteResponse {
                request_id: "dfs_net_req_stale_epoch".to_string(),
                object_id: object_id.clone(),
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed {
                        intent_id: "dfs_net_req_stale_epoch".to_string(),
                        version,
                        content_hash: "blake3:routed-response-authority".to_string(),
                        repair_jobs: Vec::new(),
                    },
                },
                metadata_journal_batches: vec![batch],
            },
        )
        .expect("stale-epoch routed response is handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { ref reason, .. }
            if reason.contains("epoch 7 is no longer current")
    ));
    drop(dfs);
    assert!(
        open_root_local_metadata_backend(&receiver_metadata_root)
            .expect("reopen receiver metadata")
            .get_object(&object_id)
            .expect("read rejected object")
            .is_none(),
        "a same-primary response from a stale dispatch epoch must not mutate requester metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_namespace_response_rejects_retired_dispatch_authority_without_mutation() {
    let root = temp_root("routed-namespace-retired-dispatch-authority");
    let requester =
        NodeId::from_legacy_named("node_namespace_retired_requester").expect("requester");
    let retired_primary =
        NodeId::from_legacy_named("node_namespace_retired_primary").expect("retired primary");
    let current_primary =
        NodeId::from_legacy_named("node_namespace_current_primary").expect("current primary");
    let object_id =
        DfsObjectId::from_legacy_named("obj_namespace_retired_primary").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_namespace_retired_primary").expect("version");
    let batch = test_routed_response_journal_batch(
        &root.join("source-metadata"),
        &retired_primary,
        &requester,
        &object_id,
        &version,
        "/services/retired-namespace-response.txt",
    );
    let dispatch_authority = test_routed_authority_shard(&retired_primary, 3);
    let current_authority = test_routed_authority_shard(&current_primary, 4);
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
    dfs.pending_routed_namespace_ops.insert(
        "dfs_net_req_retired_namespace".to_string(),
        PendingRoutedNamespaceOp {
            primary_node_id: retired_primary.clone(),
            dispatch_authority_shard: Some(dispatch_authority.clone()),
            authority_shards: vec![dispatch_authority],
            replay_journal_batches: true,
            apply_synced_path_object: false,
            result: None,
        },
    );

    let event = dfs
        .handle_namespace_response(
            retired_primary,
            DfsNamespaceResponse {
                request_id: "dfs_net_req_retired_namespace".to_string(),
                result: DfsNamespaceResult::Applied,
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_batches: vec![batch],
                metadata_journal_has_more: None,
                posix_coordination_result: None,
            },
        )
        .expect("late namespace response is handled");
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
            .expect("read rejected namespace object")
            .is_none(),
        "a namespace response from a retired dispatch authority must not mutate requester metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn routed_namespace_zero_batch_response_rejects_retired_authority_without_path_mutation() {
    let root = temp_root("routed-namespace-zero-batch-retired-authority");
    let requester =
        NodeId::from_legacy_named("node_namespace_zero_batch_requester").expect("requester");
    let retired_primary =
        NodeId::from_legacy_named("node_namespace_zero_batch_retired").expect("retired primary");
    let current_primary =
        NodeId::from_legacy_named("node_namespace_zero_batch_current").expect("current primary");
    let object_id =
        DfsObjectId::from_legacy_named("obj_namespace_zero_batch_retired").expect("object");
    let dispatch_authority = test_routed_authority_shard(&retired_primary, 11);
    let current_authority = test_routed_authority_shard(&current_primary, 12);
    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&current_authority)
            .expect("current authority shard")
    });
    let receiver_metadata_root = root.join("receiver-metadata");
    let mut dfs = NetworkedDfsService::new(
        DroppingSendTransport {
            local_node_id: requester,
        },
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(
        open_root_local_metadata_backend(&receiver_metadata_root).expect("receiver metadata"),
    )
    .expect("receiver service");
    dfs.pending_routed_namespace_ops.insert(
        "dfs_net_req_zero_batch_retired_namespace".to_string(),
        PendingRoutedNamespaceOp {
            primary_node_id: retired_primary.clone(),
            dispatch_authority_shard: Some(dispatch_authority.clone()),
            authority_shards: vec![dispatch_authority],
            replay_journal_batches: false,
            apply_synced_path_object: true,
            result: None,
        },
    );
    let mut object = file_metadata(retired_primary.clone());
    object.object_id = object_id.clone();
    object.file_system_id = sifs_file_system_id();
    object.path = "/services/zero-batch-retired.txt".to_string();

    let event = dfs
        .handle_namespace_response(
            retired_primary,
            DfsNamespaceResponse {
                request_id: "dfs_net_req_zero_batch_retired_namespace".to_string(),
                result: DfsNamespaceResult::Applied,
                synced_path_object: Some(object),
                synced_path_objects: Vec::new(),
                metadata_journal_batches: Vec::new(),
                metadata_journal_has_more: None,
                posix_coordination_result: None,
            },
        )
        .expect("late zero-batch namespace response is handled");
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
        "a zero-batch response from a retired authority must not install its synced path object"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn retry_pending_repairs_skips_sender_that_lost_metadata_primary_authority() {
    let root = temp_root("retry-repair-skips-stale-primary");
    let owner = NodeId::from_legacy_named("node_repair_stale_primary_owner").expect("owner");
    let new_primary = NodeId::from_legacy_named("node_repair_stale_primary_new").expect("primary");
    let replica = NodeId::from_legacy_named("node_repair_stale_primary_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_stale_primary").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_stale_primary_1").expect("version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_stale_primary_2").expect("version");
    let file_system = file_system(owner.clone());
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
                primary_node_id: owner.clone(),
                replica_node_ids: vec![new_primary.clone(), replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata shard");
    });
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("services"), root.join("metadata"))
            .expect("services metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(owner.clone()), content)
        .with_control_plane(shared_kv.clone())
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    dfs.add_file_system(file_system.clone())
        .expect("filesystem");
    let mut object = file_metadata(owner.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/stale-primary-repair.bin".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(object).expect("object");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: file_system.file_system_id.clone(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"repair should not be sent from stale primary".to_vec(),
            vec![owner.clone(), replica.clone()],
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
            assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
            intent_id
        }
        other => panic!("expected committed write with pending repair, got {other:?}"),
    };

    let mut replica_mailbox = network.add_node(replica.clone());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: new_primary,
                replica_node_ids: vec![owner.clone(), replica.clone()],
                epoch: 2,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("metadata primary move");
    });
    dfs.repair_retry_tick = 16;

    let scheduled = dfs.retry_pending_repairs().expect("retry repairs");

    assert!(
        scheduled.is_empty(),
        "stale metadata primary must not schedule remote repair frames"
    );
    assert!(
        drain_dfs_messages(&mut replica_mailbox).is_empty(),
        "stale metadata primary must not send repair frames after authority moves"
    );
    assert!(
        !dfs.writes.pending_writes.contains_key(&intent_id),
        "stale committed repair state should be dropped after authority moves"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_on_non_authority_keeps_receipt_retryable() {
    let root = temp_root("missing-commit-content-non-authority");
    let fixture = seed_durable_commit_promotion_fixture(&root, "missing-content-non-authority");
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: fixture.owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "services".to_string(),
                primary_node_id: fixture.owner.clone(),
                replica_node_ids: vec![fixture.replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("services shard");
    });
    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(fixture.owner.clone());
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target metadata backend");
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(root.join("target-content")).expect("empty target content"),
    )
    .with_control_plane(shared_kv)
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
                request_id: "commit_missing_content_non_authority".to_string(),
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
        .expect("retryable missing-content response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected { ref reason, .. }
            if !dfs_rejection_is_commit_content_repair_pending(
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
            if !dfs_rejection_is_commit_content_repair_pending(
                reason,
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
    ));
    assert!(
        target
            .metadata_backend()
            .expect("target backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("unchanged receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "a non-authority cannot retire or prune the durable receipt"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn missing_local_commit_content_on_retiring_primary_with_pending_handoff_stays_retryable() {
    let root = temp_root("missing-commit-content-retiring-primary-handoff");
    let fixture = seed_durable_commit_promotion_fixture(&root, "retiring-primary-handoff");
    let retiring = DfsMetadataShardConfig {
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
    let desired = DfsMetadataShardConfig {
        primary_node_id: fixture.owner.clone(),
        replica_node_ids: vec![fixture.replica.clone()],
        epoch: retiring.epoch + 1,
        ..retiring.clone()
    };
    let (range_started_tx, range_started_rx) = std::sync::mpsc::sync_channel(1);
    let shared_kv = SharedDfsControlPlaneKvClient::new(RangeSignalingControlPlaneKvClient::new(
        range_started_tx,
    ));
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig {
                file_system_id: sifs_file_system_id(),
                display_name: "SIFS".to_string(),
                owner_node_id: fixture.owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        control
            .put_metadata_shard(&retiring)
            .expect("retiring services shard");
    });

    let network = InProcessNetwork::new();
    let mut owner_transport = network.add_node(fixture.owner.clone());
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("target metadata backend");
    let mut target = NetworkedDfsService::new(
        network.add_node(fixture.replica.clone()),
        LocalContentStore::new(root.join("target-content")).expect("empty target content"),
    )
    .with_control_plane(shared_kv.clone())
    .with_metadata_backend(backend)
    .expect("target metadata hydration");
    let object_metadata = target
        .metadata()
        .object(&fixture.object_id)
        .cloned()
        .expect("committed object metadata");

    // Hold the shard publication guard so the transition can complete its
    // initial shard selection but cannot certify authority yet. Beginning the
    // handoff at that exact point proves that certification runs under the
    // guard and observes the newly durable marker before any receipt prune.
    let publication_guard = crate::service_replica::metadata_shard_publication_guard(
        &retiring.file_system_id,
        &retiring.shard_id,
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
        .expect("transition reached guarded shard selection");
    shared_kv.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .begin_metadata_shard_membership_handoff(&retiring, &desired)
            .expect("begin primary handoff while transition is fenced")
            .expect("pending primary handoff");
    });
    drop(publication_guard);
    let (mut target, transitioned) = transition.join().expect("join guarded transition");
    assert!(
        !transitioned,
        "a handoff that starts before certification must keep the transition retryable"
    );

    let event = target
        .handle_commit_version(
            fixture.owner.clone(),
            DfsCommitVersionCommand {
                request_id: "commit_missing_content_retiring_primary_handoff".to_string(),
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
        .expect("retryable pending-handoff response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected { ref reason, .. }
            if !dfs_rejection_is_commit_content_repair_pending(
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
            if !dfs_rejection_is_commit_content_repair_pending(
                reason,
                &fixture.object_id,
                &fixture.target_version,
                &fixture.content_hash,
            )
    ));
    assert!(
        target
            .metadata_backend()
            .expect("target backend")
            .receipts_for_version(&fixture.object_id, &fixture.target_version)
            .expect("unchanged receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == fixture.replica),
        "a pending handoff must fence the stale primary before it prunes the durable receipt"
    );
    assert!(
        target
            .metadata_backend()
            .expect("target backend")
            .get_object(&fixture.object_id)
            .expect("object")
            .expect("object")
            .current_locations
            .contains(&fixture.replica),
        "a pending handoff must preserve the stale primary location until current authority repairs it"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn default_node_transport_hydration_does_not_enable_early_response_or_queue_promotions() {
    let root = temp_root("default-node-hydration-no-promotion-recovery");
    let fixture = seed_durable_commit_promotion_fixture(&root, "default");
    let detached_send_state = DfsNodeDetachedSendState::new();
    let transport = recovery_hydration_node_transport(
        fixture.owner.clone(),
        fixture.replica,
        detached_send_state.clone(),
    );
    assert!(
        !transport.can_return_committed_foreground_response_before_replica_repair(),
        "sharing an outbox must not opt an ordinary node transport into served-message semantics"
    );
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened metadata backend");
    let _service = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(fixture.content_root).expect("reopened content"),
    )
    .with_metadata_backend(backend)
    .expect("ordinary hydration");

    assert_eq!(
        detached_send_state.queued_send_count(),
        0,
        "ordinary node hydration must retain its foreground semantics"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn ordinary_served_lane_hydration_does_not_queue_background_promotions() {
    let root = temp_root("ordinary-served-hydration-no-promotion-recovery");
    let fixture = seed_durable_commit_promotion_fixture(&root, "served-default");
    let detached_send_state = DfsNodeDetachedSendState::new();
    let sender: DfsMessageSender = Arc::new(|_target, command, _options| {
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: Vec::new(),
        })
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
        fixture.owner,
        vec![fixture.replica],
        sender,
        Duration::from_secs(1),
        detached_send_state.clone(),
    );
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened metadata backend");
    let service = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(fixture.content_root).expect("reopened content"),
    )
    .with_metadata_backend(backend)
    .expect("ordinary served-lane hydration");

    assert!(
        !service
            .message_deferred_work_pending()
            .expect("ordinary served-lane detached state"),
        "request-lane hydration must not reconstruct background promotion work"
    );
    assert_eq!(detached_send_state.queued_send_count(), 0);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_authority_rejection_suppresses_stale_retry_loop() {
    let root = temp_root("repair-authority-rejection-suppresses-retry");
    let owner = NodeId::from_legacy_named("node_repair_auth_reject_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_auth_reject_replica").expect("node");
    let new_primary = NodeId::from_legacy_named("node_repair_auth_reject_primary").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_auth_reject").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("metadata");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"repair authority rejection bytes".to_vec(),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write commits locally with pending repair");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);

    let request_id = "dfs_net_req_repair_auth_reject".to_string();
    dfs.record_repair_inflight(&intent_id, &replica, request_id.clone());
    let event = dfs
        .handle_repair_replica_response(
            replica.clone(),
            DfsRepairReplicaResponse {
                request_id,
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2,
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: format!(
                        "source is not current metadata shard primary for sifs; primary is {new_primary} at epoch 2"
                    ),
                },
            },
        )
        .expect("authority rejection is handled");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RepairRejected {
            replica_node_id,
            reason,
            ..
        } if replica_node_id == replica
            && reason.contains("source is not current metadata shard primary")
    ));

    dfs.repair_retry_tick = 100;
    assert!(
        !dfs.repair_retry_is_due(&intent_id, &replica),
        "lost-authority repair rejection must not retry"
    );
    dfs.record_fetch_failed_source_and_wake_retries(&intent_id, &replica);
    let pending = dfs
        .writes
        .pending_writes
        .get(&intent_id)
        .expect("pending write");
    let retry = pending
        .repair_retries
        .get(&replica)
        .expect("suppressed repair state");
    assert_eq!(retry.next_retry_tick, DFS_REPAIR_RETRY_SUPPRESSED_TICK);
    assert!(!retry.inflight);
    assert!(
        !pending.repair_inflight_request_ids.contains_key(&replica),
        "suppression must clear the rejected in-flight repair request"
    );
    assert!(
        !dfs.repair_retry_is_due(&intent_id, &replica),
        "fetch wakeups must not re-enable suppressed stale-authority repairs"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_attempt_restore_preserves_lost_authority_suppression() {
    let root = temp_root("repair-attempt-restore-lost-authority");
    let owner = NodeId::from_legacy_named("node_repair_restore_reject_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_restore_reject_replica").expect("node");
    let new_primary =
        NodeId::from_legacy_named("node_repair_restore_reject_primary").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_restore_reject").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    dfs.add_object_metadata(metadata).expect("metadata");

    let write = dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2.clone(),
                requesting_node_id: owner.clone(),
            },
            b"repair restore lost authority bytes".to_vec(),
            vec![owner, replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("write commits locally with pending repair");
    let intent_id = match write {
        NetworkedDfsWriteOutcome::Committed {
            intent_id,
            repair_jobs,
            ..
        } => {
            assert_eq!(repair_jobs.len(), 1);
            assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
            intent_id
        }
        other => panic!("expected committed write with repair job, got {other:?}"),
    };

    let request_id = "dfs_net_req_repair_restore_auth_reject".to_string();
    dfs.record_repair_inflight(&intent_id, &replica, request_id.clone());
    let event = dfs
        .handle_repair_replica_response(
            replica.clone(),
            DfsRepairReplicaResponse {
                request_id,
                intent_id: intent_id.clone(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                version: version_2,
                replica_node_id: replica.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: format!(
                        "source is not current metadata shard primary for sifs; primary is {new_primary} at epoch 2"
                    ),
                },
            },
        )
        .expect("authority rejection is handled");
    dfs.observe_repair_maintenance_events(&[event]);

    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent_id, intent_id);
    assert_eq!(records[0].missing_replica_node_id, replica);
    assert!(
        records[0]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("source is not current metadata shard primary"))
    );

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
        .expect("restore attempts");
    assert!(
        scheduled.is_empty(),
        "restored lost-authority attempt must not schedule another stale repair"
    );
    assert!(
        !dfs.repair_retry_is_due(&intent_id, &replica),
        "restored lost-authority attempt must stay suppressed"
    );
    assert_eq!(
        dfs.active_repair_attempt_records().len(),
        1,
        "suppression record must remain durable across the next tick"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_accepts_newer_authority_shard_when_receiver_view_is_stale() {
    let root = temp_root("repair-command-newer-authority-stale-receiver");
    let stale_primary =
        NodeId::from_legacy_named("node_repair_stale_receiver_old_primary").expect("old primary");
    let current_primary =
        NodeId::from_legacy_named("node_repair_stale_receiver_new_primary").expect("new primary");
    let target = NodeId::from_legacy_named("node_repair_stale_receiver_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_stale_receiver").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_repair_stale_receiver_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_repair_stale_receiver_2").expect("target version");
    let file_system = file_system(current_primary.clone());
    let mut stale_kv = InProcessKvStore::new();
    {
        let kv = &mut stale_kv;
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: stale_primary.clone(),
                replica_node_ids: vec![target.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("stale metadata shard");
    }
    let authority_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: current_primary.clone(),
        replica_node_ids: vec![target.clone()],
        epoch: 6,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let current_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    current_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("current filesystem config");
        control
            .put_metadata_shard(&authority_shard)
            .expect("current metadata shard");
    });
    let refreshes = Arc::new(AtomicUsize::new(0));
    let refreshing_kv =
        RefreshingDfsControlPlaneKvClient::new(stale_kv, current_kv, Arc::clone(&refreshes));
    let network = InProcessNetwork::new();
    let _primary_mailbox = network.add_node(current_primary.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(network.add_node(target.clone()), content)
        .with_control_plane(refreshing_kv)
        .with_metadata_backend(metadata)
        .expect("metadata backend");
    let mut object = file_metadata(current_primary.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/repair-stale-receiver.bin".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![current_primary.clone()];
    let bytes = b"repair bytes from newer metadata authority";
    let content_hash = content_hash(bytes);

    let event = dfs
        .handle_repair_replica(
            current_primary,
            DfsRepairReplicaCommand {
                request_id: "dfs_net_req_repair_stale_receiver".to_string(),
                intent_id: "dfs_intent_repair_stale_receiver".to_string(),
                file_system_id: file_system.file_system_id,
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: content_hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(object),
                authority_shard: Some(authority_shard),
            },
        )
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
    let manifest = dfs
        .content()
        .read_manifest(object_id.as_str(), version_2.as_str())
        .expect("repaired manifest");
    assert_eq!(manifest.content_hash, content_hash);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .get_object(&object_id)
            .expect("backend object")
            .is_none(),
        "stale receiver must cache the replica without publishing authoritative metadata"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn fetch_blob_accepts_newer_authority_shard_when_receiver_view_is_stale() {
    let root = temp_root("fetch-blob-newer-authority-stale-receiver");
    let coordinator =
        NodeId::from_legacy_named("node_fetch_stale_receiver_coordinator").expect("coordinator");
    let stale_primary =
        NodeId::from_legacy_named("node_fetch_stale_receiver_old_primary").expect("old primary");
    let current_primary =
        NodeId::from_legacy_named("node_fetch_stale_receiver_new_primary").expect("new primary");
    let source = NodeId::from_legacy_named("node_fetch_stale_receiver_source").expect("source");
    let target = NodeId::from_legacy_named("node_fetch_stale_receiver_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_fetch_stale_receiver").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_fetch_stale_receiver_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_fetch_stale_receiver_2").expect("target version");
    let intent_id = "dfs_intent_fetch_stale_receiver";
    let file_system = file_system(coordinator.clone());
    let stale_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    stale_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: stale_primary,
                replica_node_ids: vec![source.clone(), target.clone()],
                epoch: 3,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("stale metadata shard");
    });
    let authority_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: current_primary.clone(),
        replica_node_ids: vec![source.clone(), target.clone()],
        epoch: 6,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let mut object = file_metadata(coordinator.clone());
    object.file_system_id = file_system.file_system_id.clone();
    object.object_id = object_id.clone();
    object.path = "/services/fetch-stale-receiver.bin".to_string();
    object.version = version_2.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![source.clone()];
    let bytes = b"fetch bytes for newer metadata authority";
    let content_hash = content_hash(bytes);
    object.content_hash = Some(content_hash.clone());
    object.content_size_bytes = Some(bytes.len() as u64);
    let mut backend = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_intent(&DfsWriteIntentRecord {
            intent_id: intent_id.to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(version_1),
            target_version: version_2.clone(),
            coordinator_node_id: coordinator,
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 6,
            desired_replica_node_ids: vec![source.clone(), target],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: Some(content_hash.clone()),
            object_metadata: Some(object),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        })
        .expect("intent");

    let network = InProcessNetwork::new();
    let _primary_mailbox = network.add_node(current_primary.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(network.add_node(source.clone()), content)
        .with_control_plane(stale_kv)
        .with_metadata_backend(backend)
        .expect("metadata backend");
    dfs.content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("content");

    let event = dfs
        .handle_fetch_blob(
            current_primary,
            DfsFetchBlobCommand {
                request_id: "dfs_net_req_fetch_stale_receiver".to_string(),
                intent_id: intent_id.to_string(),
                object_id: object_id.clone(),
                version: version_2,
                content_hash: content_hash.clone(),
                authority_shard: Some(authority_shard),
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
fn atomic_namespace_rename_rejects_shard_epoch_change_before_commit() {
    let root = temp_root("atomic-namespace-rename-shard-change");
    let node_id = NodeId::from_legacy_named("node_atomic_namespace_primary").expect("primary node");
    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    let shard = |epoch| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                node_id.clone(),
            )))
            .expect("filesystem config");
        control
            .put_metadata_shard(&shard(1))
            .expect("initial shard config");
    });
    let mut service =
        sifs_posix_cluster_node(&network, &shared_kv, &root, "primary", node_id.clone());
    service
        .bootstrap_distributed_sifs()
        .expect("bootstrap SIFS");
    service
        .posix_mkdir_all("/services/sandbox/instances")
        .expect("instance namespace");
    service
        .posix_symlink(
            "/services/sandbox/generations/one",
            "/services/sandbox/instances/.swap",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("staged link");

    shared_kv.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard(2))
            .expect("advance shard epoch");
    });

    let error = service
        .posix_rename_single_namespace_atomic_local(
            "/services/sandbox/instances/.swap",
            "/services/sandbox/instances/one",
            Some(("root", 1)),
        )
        .expect_err("stale atomic authority must fail closed");
    assert!(matches!(error, DfsPosixError::InvalidPath { .. }));
    assert!(
        service
            .posix_lstat_distributed("/services/sandbox/instances/.swap")
            .is_ok()
    );
    assert!(matches!(
        service.posix_lstat_distributed("/services/sandbox/instances/one"),
        Err(DfsPosixError::NotFound { .. })
    ));
}

#[test]
fn sifs_cluster_posix_contract_partitioned_concurrent_writers_are_fenced() {
    let root = temp_root("sifs-contract-partitioned-writers");
    let scenario = "partitioned-concurrent-writers";
    let old_primary_node =
        NodeId::from_legacy_named("node_contract_partition_old").expect("old primary");
    let new_primary_node =
        NodeId::from_legacy_named("node_contract_partition_new").expect("new primary");
    let network = InProcessNetwork::new();
    let current_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    current_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                new_primary_node.clone(),
            )))
            .expect("current fs");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: new_primary_node.clone(),
                replica_node_ids: vec![old_primary_node.clone()],
                epoch: 2,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("current shard");
    });
    // A node with an entirely separate stale KV universe has no authoritative
    // signal to distinguish itself from a fork. The production fence is the
    // current control-plane view: stale local metadata must not let the old
    // primary commit when KV says a newer primary owns the shard.
    let mut old_primary = sifs_posix_cluster_node(
        &network,
        &current_kv,
        &root,
        "old-primary",
        old_primary_node.clone(),
    );
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &current_kv,
        &root,
        "new-primary",
        new_primary_node.clone(),
    );
    new_primary
        .bootstrap_distributed_sifs()
        .expect("new primary bootstrap");
    // Give the stale-side process real replicated namespace state. Merely
    // opening a current follower and calling bootstrap is intentionally a
    // no-op under authority-local bootstrap, so the old fixture's
    // MissingParent result never exercised the ownership fence at all.
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        new_primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("replicate current namespace to stale-side process");
        old_primary
            .process_incoming()
            .expect("stale-side metadata catch-up");
        new_primary
            .process_incoming()
            .expect("current-primary catch-up acknowledgement");
        if old_primary.posix_stat("/users").is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "stale-side process did not receive the current namespace"
        );
    }
    let mut observations = Vec::new();
    let path = "/users/contract-partitioned.txt";
    let stop_old_primary = Arc::new(AtomicBool::new(false));
    let old_primary_stop = Arc::clone(&stop_old_primary);
    let old_primary_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(old_primary, old_primary_stop));
    let current = new_primary.posix_write_file(
        path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![new_primary.node_id().clone()]),
    );
    stop_old_primary.store(true, Ordering::SeqCst);
    let mut old_primary = old_primary_thread.join().expect("old primary pump");
    let stale = old_primary.posix_write_file(
        path,
        b"stale",
        DfsWriteOptions::replicated_eventual(vec![old_primary.node_id().clone()]),
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "current-primary-write",
        new_primary.node_id(),
        path,
        Instant::now(),
        format!("{current:?}"),
        current.as_ref().ok(),
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "stale-primary-write",
        old_primary.node_id(),
        path,
        Instant::now(),
        format!("{stale:?}"),
        stale.as_ref().ok(),
    );
    assert!(
        current.is_ok(),
        "current primary should be able to commit while fenced stale primary must not win"
    );
    assert!(
        matches!(
            stale,
            Err(DfsPosixError::PendingWrite { .. }) | Err(DfsPosixError::Dfs(_))
        ),
        "stale partitioned primary must fail or route instead of silently committing: {stale:?}"
    );
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_bulk_create_rebases_stale_parent_on_authority() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-stale-parent-rebase");
    let parent_path = "/services/bulk-stale-parent";
    let child_path = "/services/bulk-stale-parent/created.txt";
    cluster
        .services_primary
        .posix_mkdir_all(parent_path)
        .expect("authoritative bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "initial bulk parent", |cluster| {
        cluster.replica.posix_stat(parent_path).is_ok()
    });
    let stale_parent_version =
        DfsVersion::from_legacy_named("version_1").expect("stale parent version");
    let authority_parent_version =
        DfsVersion::from_legacy_named("version_3").expect("authority parent version");
    for (service, version) in [
        (&mut cluster.replica, stale_parent_version.clone()),
        (
            &mut cluster.services_primary,
            authority_parent_version.clone(),
        ),
    ] {
        let mut parent = service
            .metadata_backend
            .as_ref()
            .expect("bulk parent metadata backend")
            .get_object_by_path(&sifs_file_system_id(), parent_path)
            .expect("bulk parent lookup")
            .expect("bulk parent object");
        parent.version = version;
        service
            .metadata_backend
            .as_mut()
            .expect("bulk parent metadata backend")
            .put_object(&parent)
            .expect("force test parent generation");
        service
            .metadata
            .add_object(parent)
            .expect("refresh test parent in memory");
        service.clear_metadata_lookup_cache();
    }
    let stale_parent_version = cluster
        .replica
        .posix_stat(parent_path)
        .expect("replica bulk parent")
        .version;

    let authority_parent_before = cluster
        .services_primary
        .posix_stat(parent_path)
        .expect("advanced authoritative bulk parent");
    assert_ne!(
        authority_parent_before.version, stale_parent_version,
        "test must retain a stale requester parent"
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat(parent_path)
            .expect("still-stale replica bulk parent")
            .version,
        stale_parent_version,
        "requester must not process the queued parent advances before preparing the batch"
    );
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

    let stats = replica
        .posix_create_files_local_eventual_with_known_parents([(
            child_path,
            b"stale parent bulk payload".as_slice(),
        )])
        .expect("bulk create from stale-parent requester");
    assert_eq!(stats.len(), 1);

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services primary thread");
    let authority_parent_after = services_primary
        .posix_stat(parent_path)
        .expect("authoritative parent after bulk create");
    assert_ne!(
        authority_parent_after.version, authority_parent_before.version,
        "the successful retry must advance the authoritative parent"
    );
    assert_eq!(
        services_primary
            .posix_stat(child_path)
            .expect("authoritative bulk child")
            .content_hash
            .as_deref(),
        Some(content_hash(b"stale parent bulk payload").as_str())
    );
    let bulk_requests = services_primary
        .metadata_backend
        .as_ref()
        .expect("services metadata backend")
        .list_namespace_requests()
        .expect("bulk namespace request ledger")
        .into_iter()
        .filter(|request| {
            matches!(
                request.operation,
                DfsNamespaceOperation::CommitLocalEventualBatch { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(
        bulk_requests.iter().any(|request| {
            matches!(
                request.result,
                Some(DfsNamespaceResult::Rejected {
                    posix_error: Some(DfsNamespacePosixError::Conflict { .. }),
                    ..
                })
            )
        }),
        "the authority must reject the requester's stale parent generation before retry"
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn routed_namespace_rejects_stale_authority_stamp_before_mutation() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-stale-authority-stamp");
    let SifsTwoPrimaryCluster {
        root,
        replica_node,
        mut services_primary,
        replica,
        ..
    } = cluster;
    let file_system_id = sifs_file_system_id();
    let path = "/services/stale-authority-stamp";
    let current_primary = services_primary.node_id().clone();
    let operation = DfsNamespaceOperation::MkdirAll {
        path: path.to_string(),
    };
    let mut authority_shards = services_primary
        .affected_namespace_shards(&file_system_id, &operation)
        .expect("current namespace authority shards");
    assert_eq!(authority_shards.len(), 1, "test path must be single-shard");
    authority_shards[0].epoch = authority_shards[0].epoch.saturating_sub(1);

    let event = services_primary
        .handle_namespace_request(
            replica_node.clone(),
            DfsNamespaceCommand {
                request_id: "dfs_net_req_stale_namespace_stamp".to_string(),
                file_system_id,
                requesting_node_id: replica_node,
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards,
                operation,
            },
        )
        .expect("serve stale-stamped namespace request");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result: DfsNamespaceResult::Rejected {
                ref reason,
                current_primary_node_id: Some(ref primary),
                ..
            },
            ..
        } if reason.contains("authority shard stamp") && primary == &current_primary
    ));
    assert!(
        matches!(
            services_primary.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "a stale admission stamp must be rejected before namespace mutation"
    );

    drop(replica);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_authority_local_follower_bootstrap_stays_on_incremental_stream() {
    // Bootstrap objects are authority-local. A follower may open/bootstrap its
    // service independently, but it must not manufacture a divergent namespace
    // journal that forces an avoidable full snapshot before ordinary catch-up.
    let root = temp_root("sifs-authority-local-bootstrap-incremental");
    let primary_node = NodeId::from_legacy_named("node_inservice_snap_primary").expect("primary");
    let follower_node =
        NodeId::from_legacy_named("node_inservice_snap_follower").expect("follower");
    let network = InProcessNetwork::new();
    let shared_kv = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_kv.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                primary_node.clone(),
            )))
            .expect("fs config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: primary_node.clone(),
                replica_node_ids: vec![follower_node.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("shard config");
    });
    let node = |node_id: &NodeId, dir: &str| {
        let node_root = root.join(dir);
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id.clone()),
            LocalContentStore::new(node_root.join("content")).expect("content"),
        )
        .with_control_plane(shared_kv.clone())
        .with_metadata_backend(
            open_root_local_metadata_backend(node_root.join("metadata")).expect("metadata"),
        )
        .expect("backend attach");
        // Both services open independently. Only the configured primary may
        // create the fixed namespace anchors.
        dfs.bootstrap_distributed_sifs().expect("bootstrap");
        dfs
    };
    let mut primary = node(&primary_node, "primary");
    let mut follower = node(&follower_node, "follower");
    assert_eq!(
        follower
            .metadata_backend()
            .expect("follower metadata backend")
            .journal_last_sequence()
            .expect("follower bootstrap journal head"),
        0,
        "a non-primary bootstrap must leave its durable metadata journal at genesis"
    );

    let bytes = b"post-repair payload".to_vec();
    primary
        .posix_write_file(
            "/users/after-repair.txt",
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("primary create");

    let mut saw_snapshot_install = false;
    let expected_hash = content_hash(&bytes);
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("publish");
        for service in [&mut primary, &mut follower] {
            for event in service.process_incoming().expect("pump") {
                if matches!(
                    event,
                    NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. }
                ) {
                    saw_snapshot_install = true;
                }
            }
        }
        if follower
            .posix_stat("/users/after-repair.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!("authority-local follower did not converge through incremental publication");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    assert!(
        !saw_snapshot_install,
        "authority-local follower bootstrap must not force a full metadata snapshot"
    );

    // Post-repair, the follower applies regular journal batches cleanly.
    let more = b"incremental after the snapshot".to_vec();
    primary
        .posix_write_file(
            "/users/incremental.txt",
            &more,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                replica_candidates: vec![primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("primary second incremental create");
    let expected_more = content_hash(&more);
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        primary
            .replicate_configured_metadata_shards(&sifs_file_system_id())
            .expect("publish incremental");
        primary.process_incoming().expect("primary pump");
        follower.process_incoming().expect("follower pump");
        if follower
            .posix_stat("/users/incremental.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_more.as_str()))
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!("follower did not remain on the incremental metadata stream");
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_routed_namespace_op_follows_authority_redirect() {
    let root = temp_root("sifs-authority-redirect");
    let primary_node =
        NodeId::from_legacy_named("node_sifs_redirect_old_primary").expect("old primary");
    let writer_node =
        NodeId::from_legacy_named("node_sifs_redirect_new_primary").expect("new primary");
    let observer_node =
        NodeId::from_legacy_named("node_sifs_redirect_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![primary_node.clone(), writer_node.clone()],
        epoch,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };

    // Current control-plane view: the shard moved to `writer_node` at epoch 2.
    let kv_current = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_current.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                writer_node.clone(),
            )))
            .expect("current filesystem config");
        control
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("current shard config");
    });
    // The requester's stale view still names the old primary at epoch 1.
    let kv_stale = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_stale.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                writer_node.clone(),
            )))
            .expect("stale filesystem config");
        control
            .put_metadata_shard(&shard_config(&primary_node, 1))
            .expect("stale shard config");
    });
    let stale_requester_snapshot = kv_stale
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    kv_stale.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("refreshed requester shard config")
    });

    let old_primary =
        sifs_posix_cluster_node(&network, &kv_current, &root, "old-primary", primary_node);
    let mut new_primary =
        sifs_posix_cluster_node(&network, &kv_current, &root, "new-primary", writer_node);
    let mut requester =
        sifs_posix_cluster_node(&network, &kv_stale, &root, "requester", observer_node);
    requester =
        requester.with_boxed_control_plane(Box::new(RefreshingDfsControlPlaneKvClient::new(
            InProcessKvStore::restore(stale_requester_snapshot)
                .expect("restore stale requester control plane"),
            kv_stale.clone(),
            Arc::new(AtomicUsize::new(0)),
        )));
    new_primary
        .bootstrap_distributed_sifs()
        .expect("new primary SIFS bootstrap");

    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread = std::thread::spawn(move || {
        let mut node = old_primary;
        while !old_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let new_stop = Arc::clone(&stop);
    let new_thread = std::thread::spawn(move || {
        let mut node = new_primary;
        while !new_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("new primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // The requester routes on its stale view to the old primary, which
    // rejects with the current authority; the operation must follow the
    // redirect and succeed at the new primary.
    requester
        .posix_mkdir_all("/users/redirected")
        .expect("routed mkdir follows authority redirect");
    assert_eq!(
        requester
            .posix_stat("/users/redirected")
            .expect("requester reads its own redirected write")
            .kind,
        DfsObjectKind::Directory
    );

    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary thread");
    new_thread.join().expect("new primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_routed_write_follows_authority_redirect() {
    let root = temp_root("sifs-write-authority-redirect");
    let primary_node =
        NodeId::from_legacy_named("node_sifs_write_redirect_old_primary").expect("old primary");
    let writer_node =
        NodeId::from_legacy_named("node_sifs_write_redirect_new_primary").expect("new primary");
    let requester_node =
        NodeId::from_legacy_named("node_sifs_write_redirect_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![primary_node.clone(), writer_node.clone()],
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
                writer_node.clone(),
            )))
            .expect("current filesystem config");
        control
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("current shard config");
    });
    let kv_stale = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_stale.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                writer_node.clone(),
            )))
            .expect("stale filesystem config");
        control
            .put_metadata_shard(&shard_config(&primary_node, 1))
            .expect("stale shard config");
    });
    let stale_requester_snapshot = kv_stale
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    kv_stale.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("refreshed requester shard config")
    });

    let old_primary =
        sifs_posix_cluster_node(&network, &kv_current, &root, "old-primary", primary_node);
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &kv_current,
        &root,
        "new-primary",
        writer_node.clone(),
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
    new_primary
        .bootstrap_distributed_sifs()
        .expect("new primary SIFS bootstrap");

    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread = std::thread::spawn(move || {
        let mut node = old_primary;
        while !old_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let new_stop = Arc::clone(&stop);
    let new_thread = std::thread::spawn(move || {
        let mut node = new_primary;
        while !new_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("new primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    requester
        .posix_mkdir_all("/users")
        .expect("parent mkdir follows authority redirect");
    let path = "/users/write-redirected.txt";
    let bytes = b"routed write follows current primary".to_vec();
    let stat = requester
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![requester_node, writer_node]),
        )
        .expect("routed write follows authority redirect");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        requester
            .posix_read_file_distributed(path)
            .expect("requester reads redirected write"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary thread");
    new_thread.join().expect("new primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_routed_lookup_follows_authority_redirect() {
    let root = temp_root("sifs-lookup-authority-redirect");
    let primary_node =
        NodeId::from_legacy_named("node_sifs_lookup_redirect_old_primary").expect("old primary");
    let writer_node =
        NodeId::from_legacy_named("node_sifs_lookup_redirect_new_primary").expect("new primary");
    let requester_node =
        NodeId::from_legacy_named("node_sifs_lookup_redirect_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![primary_node.clone(), writer_node.clone()],
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
                writer_node.clone(),
            )))
            .expect("current filesystem config");
        control
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("current shard config");
    });
    let kv_stale = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    kv_stale.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                writer_node.clone(),
            )))
            .expect("stale filesystem config");
        control
            .put_metadata_shard(&shard_config(&primary_node, 1))
            .expect("stale shard config");
    });

    let stale_requester_snapshot = kv_stale
        .with_client(|kv| kv.try_snapshot())
        .expect("stale requester control-plane snapshot");
    kv_stale.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&shard_config(&writer_node, 2))
            .expect("refreshed requester shard config")
    });
    let old_primary =
        sifs_posix_cluster_node(&network, &kv_current, &root, "old-primary", primary_node);
    let mut new_primary = sifs_posix_cluster_node(
        &network,
        &kv_current,
        &root,
        "new-primary",
        writer_node.clone(),
    );
    let mut requester =
        sifs_posix_cluster_node(&network, &kv_stale, &root, "requester", requester_node);
    requester =
        requester.with_boxed_control_plane(Box::new(RefreshingDfsControlPlaneKvClient::new(
            InProcessKvStore::restore(stale_requester_snapshot)
                .expect("restore stale requester control plane"),
            kv_stale.clone(),
            Arc::new(AtomicUsize::new(0)),
        )));
    new_primary
        .bootstrap_distributed_sifs()
        .expect("new primary SIFS bootstrap");
    let path = "/users/lookup-redirected.txt";
    let bytes = b"routed lookup follows current primary".to_vec();
    new_primary
        .posix_mkdir_all("/users")
        .expect("new primary creates parent");
    new_primary
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node]),
        )
        .expect("new primary writes file");

    let stop = Arc::new(AtomicBool::new(false));
    let old_stop = Arc::clone(&stop);
    let old_thread = std::thread::spawn(move || {
        let mut node = old_primary;
        while !old_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("old primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let new_stop = Arc::clone(&stop);
    let new_thread = std::thread::spawn(move || {
        let mut node = new_primary;
        while !new_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("new primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let stat = requester
        .posix_stat_distributed(path)
        .expect("routed lookup follows authority redirect");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        requester
            .posix_read_file_distributed(path)
            .expect("redirected lookup enables read"),
        bytes
    );

    stop.store(true, Ordering::SeqCst);
    old_thread.join().expect("old primary thread");
    new_thread.join().expect("new primary thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_routed_lookup_uses_local_replica_after_authority_redirect_cycle() {
    let root = temp_root("sifs-lookup-redirect-cycle-local-replica");
    let primary_a =
        NodeId::from_legacy_named("node_sifs_lookup_cycle_primary_a").expect("primary a");
    let primary_b =
        NodeId::from_legacy_named("node_sifs_lookup_cycle_primary_b").expect("primary b");
    let requester_node =
        NodeId::from_legacy_named("node_sifs_lookup_cycle_requester").expect("requester");

    let network = InProcessNetwork::new();
    let shard_config = |primary: &NodeId, epoch: u64| {
        let mut replica_node_ids =
            vec![primary_a.clone(), primary_b.clone(), requester_node.clone()];
        replica_node_ids.retain(|node_id| node_id != primary);
        DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "services".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids,
            epoch,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    };
    let put_control_plane = |kv_client: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
                             primary: &NodeId,
                             epoch: u64| {
        kv_client.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                    requester_node.clone(),
                )))
                .expect("filesystem config");
            control
                .put_metadata_shard(&shard_config(primary, epoch))
                .expect("services shard config");
        });
    };

    let kv_a = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_a, &primary_b, 3);
    let kv_b = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_b, &primary_a, 4);
    let kv_requester = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    put_control_plane(&kv_requester, &requester_node, 1);

    let node_a = sifs_posix_cluster_node(&network, &kv_a, &root, "primary-a", primary_a.clone());
    let node_b = sifs_posix_cluster_node(&network, &kv_b, &root, "primary-b", primary_b.clone());
    let requester_root = root.join("requester");
    let mut requester = NetworkedDfsService::new(
        network.add_node(requester_node.clone()),
        LocalContentStore::new(requester_root.join("content")).expect("requester content store"),
    )
    .with_control_plane(kv_requester.clone())
    .with_metadata_backend(
        open_local_sharded_metadata_backend(
            requester_root.join("metadata"),
            vec![shard_config(&requester_node, 1)],
        )
        .expect("requester services metadata shard"),
    )
    .expect("requester metadata backend attach");

    requester
        .bootstrap_distributed_sifs()
        .expect("requester SIFS bootstrap");
    let path = "/services/lookup-cycle-local-replica.txt";
    let bytes = b"local replica survives redirect cycle".to_vec();
    requester
        .posix_mkdir_all("/services")
        .expect("requester creates services");
    requester
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![primary_a.clone(), primary_b.clone()]),
        )
        .expect("requester writes local replica");

    put_control_plane(&kv_requester, &primary_a, 4);

    let stop = Arc::new(AtomicBool::new(false));
    let a_stop = Arc::clone(&stop);
    let a_thread = std::thread::spawn(move || {
        let mut node = node_a;
        while !a_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("primary a pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let b_stop = Arc::clone(&stop);
    let b_thread = std::thread::spawn(move || {
        let mut node = node_b;
        while !b_stop.load(Ordering::SeqCst) {
            node.process_incoming().expect("primary b pump");
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let stat = requester.posix_stat_distributed(path);
    assert_eq!(
        stat.expect("local replica stat survives authority redirect cycle")
            .size_bytes,
        Some(bytes.len() as u64)
    );

    let missing_error = requester
        .posix_stat_distributed("/services/lookup-cycle-missing.txt")
        .expect_err("missing replica state must not launder an authority error into NotFound");
    assert!(
        missing_error.to_string().contains("uncorroborated"),
        "unexpected missing-state error: {missing_error}"
    );

    let mut removed_replica = shard_config(&primary_a, 5);
    removed_replica
        .replica_node_ids
        .retain(|node_id| node_id != &requester_node);
    kv_requester.with_client(|kv| {
        DfsControlPlaneStore::new(kv)
            .put_metadata_shard(&removed_replica)
            .expect("remove requester from current replica assignment")
    });
    let removed_membership_error = requester
        .posix_stat_distributed(path)
        .expect_err("removed replica must not use its retained local hit");
    assert!(
        removed_membership_error
            .to_string()
            .contains("uncorroborated"),
        "unexpected removed-membership error: {removed_membership_error}"
    );

    stop.store(true, Ordering::SeqCst);
    a_thread.join().expect("primary a thread");
    b_thread.join().expect("primary b thread");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn served_routed_write_committed_work_tracks_only_authority_owned_repairs() {
    for (case, include_missing_replica, expect_repair_maintenance) in
        [("w1_n2", true, true), ("w1_n1", false, false)]
    {
        let root = temp_root(&format!("served-routed-write-committed-work-{case}"));
        let primary = NodeId::from_legacy_named(format!("node_committed_work_primary_{case}"))
            .expect("primary");
        let requester = NodeId::from_legacy_named(format!("node_committed_work_requester_{case}"))
            .expect("requester");
        let replica = NodeId::from_legacy_named(format!("node_committed_work_replica_{case}"))
            .expect("replica");
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_committed_work_{case}")).expect("object");
        let version_1 = DfsVersion::from_legacy_named(format!("version_committed_work_{case}_1"))
            .expect("base version");
        let version_2 = DfsVersion::from_legacy_named(format!("version_committed_work_{case}_2"))
            .expect("target version");
        let sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
            Err(DfsPeerTransportError::new(format!(
                "unexpected committed-work test send to {target_node_id} for {}",
                command.request_id
            )))
        });
        let transport = DfsNodeServedMessageTransport::new_with_message_sender(
            primary.clone(),
            vec![requester.clone(), replica.clone()],
            sender,
            Duration::from_millis(50),
        );
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let metadata =
            open_root_local_metadata_backend(root.join("metadata")).expect("metadata backend");
        let mut dfs = NetworkedDfsService::new(transport, content)
            .with_metadata_backend(metadata)
            .expect("attach metadata backend");
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
        let mut object = file_metadata(requester.clone());
        object.object_id = object_id.clone();
        object.path = format!("/services/committed-work-{case}.bin");
        object.version = version_1.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![requester.clone()];
        dfs.add_object_metadata(object).expect("object metadata");

        let mut desired_replica_nodes = vec![requester.clone()];
        if include_missing_replica {
            desired_replica_nodes.push(replica);
        }
        let desired_replicas = desired_replica_nodes.len();
        let request_id = format!("dfs_committed_work_{case}");
        let report = dfs.process_message_command_report(test_dfs_message_command(
            format!("dfs_message_committed_work_{case}"),
            build_dfs_routed_message(
                &requester,
                &primary,
                40,
                41,
                DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                    request_id,
                    request: DfsWriteRequest {
                        file_system_id: sifs_file_system_id(),
                        object_id,
                        target_version: version_2,
                        requesting_node_id: requester.clone(),
                    },
                    expected_base_version: Some(version_1),
                    metadata_journal_after_sequence: None,
                    bytes: Vec::new(),
                    content_hash: Some(content_hash(b"")),
                    content_size_bytes: Some(0),
                    requester_blob_pre_staged: true,
                    desired_replica_nodes,
                    policy: DfsReplicationPolicy::new(1, desired_replicas).expect("policy"),
                    object_metadata: None,
                    authority_shard: None,
                    touch_parent_on_create: false,
                    foreground_replica_repair: false,
                    exclusive_create: false,
                }),
            ),
        ));

        report.outcome.expect("serve routed write");
        assert!(
            report.committed_work.publish_metadata,
            "the metadata authority must retain the write publication edge for {case}"
        );
        assert_eq!(
            report.committed_work.repair_maintenance, expect_repair_maintenance,
            "only an authority-owned missing replica may retain repair work for {case}"
        );
        assert_eq!(
            report.committed_repair_records.len(),
            usize::from(expect_repair_maintenance),
            "the durable queue handoff must contain only repairs created by this message for {case}"
        );
        if let Some(record) = report.committed_repair_records.first() {
            assert_eq!(record.attempts, 0);
            assert!(!record.in_flight);
        }
        assert!(!report.committed_work.metadata_replica_progress);

        std::fs::remove_dir_all(root).ok();
    }
}

#[test]
fn served_replica_stage_does_not_claim_authority_repair_work() {
    let root = temp_root("served-replica-stage-committed-work");
    let primary = NodeId::from_legacy_named("node_committed_work_stage_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_committed_work_stage_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_committed_work_stage").expect("object");
    let version_1 =
        DfsVersion::from_legacy_named("version_committed_work_stage_1").expect("base version");
    let version_2 =
        DfsVersion::from_legacy_named("version_committed_work_stage_2").expect("target version");
    let bytes = b"replica stage does not own authority repair work";
    let sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected replica-stage test send to {target_node_id} for {}",
            command.request_id
        )))
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
        replica.clone(),
        vec![primary.clone()],
        sender,
        Duration::from_millis(50),
    );
    let content = LocalContentStore::new(root.join("content")).expect("content store");
    let mut dfs = NetworkedDfsService::new(transport, content);
    let mut object = file_metadata(primary.clone());
    object.object_id = object_id.clone();
    object.path = "/services/committed-work-stage.bin".to_string();
    object.version = version_1.clone();
    object.policy = DfsObjectPolicy::ReplicatedEventual;
    object.current_locations = vec![primary.clone()];
    let report = dfs.process_message_command_report(test_dfs_message_command(
        "dfs_message_committed_work_stage",
        build_dfs_routed_message(
            &primary,
            &replica,
            42,
            43,
            DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id: "dfs_committed_work_stage".to_string(),
                intent_id: "dfs_intent_committed_work_stage".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id,
                expected_base_version: Some(version_1),
                version: version_2,
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(object),
                authority_shard: None,
            }),
        ),
    ));

    let result = report.outcome.expect("serve replica stage");
    assert!(result.events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::BlobStaged { replica_node_id, .. }
            if replica_node_id == &replica
    )));
    assert!(
        !report.committed_work.repair_maintenance,
        "a content replica must not wake metadata-authority repair maintenance"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn served_requester_completion_does_not_claim_authority_repair_work() {
    let root = temp_root("served-requester-completion-committed-work");
    let requester =
        NodeId::from_legacy_named("node_committed_work_completion_requester").expect("requester");
    let primary =
        NodeId::from_legacy_named("node_committed_work_completion_primary").expect("primary");
    let replica =
        NodeId::from_legacy_named("node_committed_work_completion_replica").expect("replica");
    let object_id =
        DfsObjectId::from_legacy_named("obj_committed_work_completion").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_committed_work_completion").expect("version");
    let authority_shard = test_routed_authority_shard(&primary, 7);
    let sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected requester-completion test send to {target_node_id} for {}",
            command.request_id
        )))
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
        requester.clone(),
        vec![primary.clone()],
        sender,
        Duration::from_millis(50),
    );
    let mut dfs = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_control_plane(test_routed_authority_control_plane(&authority_shard));
    let request_id = "dfs_committed_work_completion";
    dfs.writes.pending_routed_writes.insert(
        request_id.to_string(),
        test_pending_routed_write(
            request_id,
            &requester,
            &object_id,
            &primary,
            authority_shard,
        ),
    );
    let report = dfs.process_message_command_report(test_dfs_message_command(
        "dfs_message_committed_work_completion",
        build_dfs_routed_message(
            &primary,
            &requester,
            44,
            45,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: request_id.to_string(),
                object_id: object_id.clone(),
                result: DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed {
                        intent_id: request_id.to_string(),
                        version: version.clone(),
                        content_hash: "blake3:committed-work-requester".to_string(),
                        repair_jobs: vec![DistributedRepairJob {
                            intent_id: request_id.to_string(),
                            file_system_id: sifs_file_system_id(),
                            object_id,
                            version,
                            missing_replica_node_id: replica,
                        }],
                    },
                },
                metadata_journal_batches: Vec::new(),
            }),
        ),
    ));

    let result = report.outcome.expect("serve requester completion");
    assert!(result.events.iter().any(|event| matches!(
        event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id: event_request_id, .. }
            if event_request_id == request_id
    )));
    assert!(
        !report.committed_work.repair_maintenance,
        "the requester observes repair backlog but does not own it"
    );

    std::fs::remove_dir_all(root).ok();
}
