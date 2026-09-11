//! Recovery regression tests.

use super::*;

#[test]
fn dfs_request_ids_remain_unique_across_service_restarts() {
    let node = NodeId::from_legacy_named("node_request_id_restart").expect("node");
    let first_root = temp_root("request-id-first");
    let restarted_root = temp_root("request-id-restarted");
    let mut first = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(node.clone()),
        LocalContentStore::new(&first_root).expect("first content"),
    );
    let mut restarted = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(node),
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
    let root = temp_root("lifecycle-maintenance-host-tombstone-compaction");
    let node_id = NodeId::from_legacy_named("node_lifecycle_tombstone_host").expect("node");
    let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
    dfs.metadata_backend
        .as_mut()
        .expect("metadata backend")
        .put_file_system(&DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named(
                "fs_lifecycle_tombstone_host_second",
            )
            .expect("second filesystem"),
            display_name: "second".to_string(),
            owner_node_id: node_id,
            subtree_policies: Vec::new(),
        })
        .expect("second filesystem metadata");
    let mut host = RecordingMaintenanceHost::default();

    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions {
            compact_tombstones: true,
            tombstone_compaction_min_age: std::time::Duration::from_nanos(17),
            tombstone_compaction_max_records: 23,
            ..DfsMaintenanceRunOptions::default()
        },
    )
    .expect("lifecycle maintenance");

    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports
            .iter()
            .filter(|report| report.tombstone_compaction_requested)
            .count(),
        1
    );
    let report = reports
        .iter()
        .find_map(|report| report.tombstone_compaction.as_ref())
        .expect("single tombstone compaction report");
    assert_eq!(report.min_tombstone_age_nanos, 17);
    assert_eq!(report.max_compactions, Some(23));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn metadata_prefix_probe_accepts_only_the_exact_materialized_checkpoint() {
    let root = temp_root("metadata-prefix-probe-exact-checkpoint");
    let primary = NodeId::from_legacy_named("node_meta_probe_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_meta_probe_replica").expect("replica");
    let shard_id = DfsMetadataShardId::new("root");
    let file_system = file_system(primary.clone());
    let metadata_root = root.join("metadata");
    let mut backend =
        LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).expect("metadata backend");
    let mut durable_writer = LocalSobjMetadataBackend::new(shard_id.clone(), metadata_root)
        .expect("durable writer backend");
    durable_writer
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    durable_writer
        .put_object(&file_metadata(primary.clone()))
        .expect("object metadata");
    let journal_sequence = durable_writer.journal_last_sequence();
    let state_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        &durable_writer,
        &shard_id,
        journal_sequence,
    )
    .expect("checkpoint fingerprint");
    let context = DfsMetadataReplicaShardContext {
        local_node_id: replica.clone(),
        pending_membership_handoff: None,
        shard: DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![replica],
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
            request_id: "request-exact-prefix-probe".to_string(),
            source_node_id: Some(primary),
            file_system_id: Some(context.shard.file_system_id.clone()),
            authority_epoch: Some(context.shard.epoch),
            verify_prefix_only: true,
            prefix_probe: Some(DfsMetadataPrefixProbe {
                barrier_id: "barrier-exact-checkpoint".to_string(),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(1),
                shard_id: shard_id.clone(),
                journal_sequence,
                state_fingerprint,
                primary_handoff_id: None,
            }),
            authority_probe: None,
            durable_replication: None,
            batch: DfsMetadataJournalBatch {
                shard_id,
                entries: Vec::new(),
                next_after_sequence: journal_sequence,
            },
        },
    );

    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    assert_eq!(response.next_after_sequence, journal_sequence);
    assert_eq!(backend.journal_last_sequence(), journal_sequence);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn persisted_repair_attempts_prevent_duplicate_retry_after_reopen() {
    let root = temp_root("persisted-repair-attempts-prevent-duplicate-retry");
    let owner = NodeId::from_legacy_named("node_persisted_repair_owner").expect("owner node");
    let replica = NodeId::from_legacy_named("node_persisted_repair_replica").expect("replica");
    let object_id = DfsObjectId::from_legacy_named("obj_persisted_repair").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_persisted_repair_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_persisted_repair_2").expect("version");
    let content_root = root.join("owner-content");
    let metadata_root = root.join("owner-metadata");
    let network = InProcessNetwork::new();
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), metadata_root.clone())
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(content_root.clone()).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut replica_transport = network.add_node(replica.clone());
    owner_dfs
        .add_file_system(file_system(owner.clone()))
        .expect("filesystem");
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1;
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs.add_object_metadata(metadata).expect("object");

    let write = owner_dfs
        .write_distributed_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id,
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            std::io::Cursor::new(b"persisted repair retry body".to_vec()),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
            DfsDistributedWriteOptions {
                touch_parent_on_create: true,
                repair: DfsWriteRepair::Background,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .expect("write should commit");
    let NetworkedDfsWriteOutcome::Committed {
        intent_id,
        content_hash,
        repair_jobs,
        ..
    } = write
    else {
        panic!("expected committed write, got {write:?}");
    };
    assert_eq!(repair_jobs.len(), 1);
    let repair_job = repair_jobs[0].clone();
    let now = dfs_now_unix_nanos();
    let repair_attempts = vec![DfsRepairAttemptRecord {
        intent_id,
        object_id: repair_job.object_id.clone(),
        version: repair_job.version.clone(),
        content_hash,
        missing_replica_node_id: repair_job.missing_replica_node_id.clone(),
        attempts: 1,
        in_flight: true,
        last_attempt_unix_nanos: now,
        next_retry_unix_nanos: now.saturating_add(60_000_000_000),
        transfer_request_id: None,
        last_error: None,
    }];
    let _ = drain_dfs_messages(&mut replica_transport);

    drop(owner_dfs);
    let reopened_network = InProcessNetwork::new();
    let reopened_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), metadata_root.clone())
            .expect("reopened owner backend");
    let mut reopened_dfs = NetworkedDfsService::new(
        reopened_network.add_node(owner),
        LocalContentStore::new(content_root).expect("reopened owner content"),
    )
    .with_metadata_backend(reopened_backend)
    .expect("reopened metadata backend");
    let mut reopened_replica_transport = reopened_network.add_node(replica);

    let duplicate = reopened_dfs
        .retry_pending_repairs_bounded_with_attempts(1, &repair_attempts)
        .expect("retry after reopen");
    assert!(
        duplicate.is_empty(),
        "reopened maintenance must honor persisted retry backoff"
    );
    assert_eq!(
        reopened_dfs
            .operational_metrics()
            .expect("reopened metrics")
            .repair_attempt_count,
        1
    );
    let replica_messages = drain_dfs_messages(&mut reopened_replica_transport)
        .into_iter()
        .filter(|message| matches!(message, DfsNetworkMessage::RepairReplica(_)))
        .count();
    assert_eq!(replica_messages, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn persisted_repair_attempts_prevent_duplicate_fetch_after_reopen() {
    let root = temp_root("persisted-repair-attempts-prevent-duplicate-fetch");
    let primary = NodeId::from_legacy_named("node_persisted_fetch_primary").expect("primary");
    let source = NodeId::from_legacy_named("node_persisted_fetch_source").expect("source");
    let target = NodeId::from_legacy_named("node_persisted_fetch_target").expect("target");
    let object_id = DfsObjectId::from_legacy_named("obj_persisted_fetch").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_persisted_fetch_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_persisted_fetch_2").expect("version");
    let content_hash = content_hash(b"persisted repair fetch body");
    let size_bytes = 2 * 1024 * 1024 + 17;
    let network = InProcessNetwork::new();
    let primary_transport = network.add_node(primary.clone());
    let mut source_transport = network.add_node(source.clone());
    let _target_transport = network.add_node(target.clone());
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let metadata_backend =
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
    let mut dfs = NetworkedDfsService::new(primary_transport, content)
        .with_metadata_backend(metadata_backend)
        .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
    let mut metadata = file_metadata(primary.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/persisted-fetch.bin".to_string();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = Vec::new();
    metadata.content_hash = None;
    metadata.content_size_bytes = None;
    dfs.add_object_metadata(metadata).expect("object metadata");

    let intent = dfs
        .coordinator
        .create_intent(
            sifs_file_system_id(),
            object_id.clone(),
            Some(version_1.clone()),
            version_2.clone(),
            primary.clone(),
            vec![source.clone(), target.clone()],
            DfsReplicationPolicy::new(1, 2).expect("policy"),
        )
        .expect("intent");
    let intent_id = intent.intent_id.clone();
    dfs.coordinator
        .record_durable_receipt(&intent_id, source.clone())
        .expect("source receipt");
    dfs.coordinator
        .commit_intent(&intent_id)
        .expect("committed intent");
    let mut committed_metadata = file_metadata(primary.clone());
    committed_metadata.object_id = object_id.clone();
    committed_metadata.path = "/services/persisted-fetch.bin".to_string();
    committed_metadata.version = version_2.clone();
    committed_metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    committed_metadata.current_locations = vec![source.clone()];
    committed_metadata.content_hash = Some(content_hash.clone());
    committed_metadata.content_size_bytes = Some(size_bytes);
    dfs.add_object_metadata(committed_metadata)
        .expect("committed object metadata");
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    assert_eq!(
        dfs.coordinator.repair_jobs()[0].missing_replica_node_id,
        target
    );
    dfs.writes.pending_writes.insert(
        intent_id.clone(),
        PendingNetworkedWrite::new(
            object_id.clone(),
            version_2,
            content_hash.clone(),
            size_bytes,
        ),
    );

    let scheduled = dfs
        .retry_pending_repairs_bounded(1)
        .expect("fetch-backed repair tick");
    assert!(
        scheduled.is_empty(),
        "fetch-backed repairs wait for the alternate source before scheduling target repair"
    );
    let fetches = drain_dfs_messages(&mut source_transport)
        .into_iter()
        .filter(|message| matches!(message, DfsNetworkMessage::FetchBlob(_)))
        .count();
    assert_eq!(fetches, 1, "first repair tick must request the blob once");
    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent_id, intent_id);
    assert_eq!(records[0].missing_replica_node_id, target);
    assert!(records[0].in_flight);

    let pending = dfs
        .writes
        .pending_writes
        .get_mut(&intent_id)
        .expect("pending write");
    pending.fetch_inflight_from.clear();
    pending.fetch_inflight_request_ids.clear();
    pending.fetch_inflight_started_ticks.clear();
    pending.repair_retries.clear();
    pending.repair_inflight_request_ids.clear();
    dfs.repair_attempt_records.clear();

    let duplicate = dfs
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .expect("restored fetch-backed repair tick");
    assert!(
        duplicate.is_empty(),
        "restored in-flight fetch attempt must honor durable backoff"
    );
    let duplicate_fetches = drain_dfs_messages(&mut source_transport)
        .into_iter()
        .filter(|message| matches!(message, DfsNetworkMessage::FetchBlob(_)))
        .count();
    assert_eq!(
        duplicate_fetches, 0,
        "restored maintenance must not duplicate the fetch request"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn explicit_recovery_hydration_queues_once_and_deduplicates_through_shared_state() {
    let root = temp_root("explicit-node-promotion-recovery-hydration");
    let fixture = seed_durable_commit_promotion_fixture(&root, "explicit");
    let detached_send_state = DfsNodeDetachedSendState::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened metadata backend");
    backend.reset_test_receipt_scan_counts();
    let scan_probe = backend.clone();
    let mut recovery = NetworkedDfsService::new(
        recovery_hydration_node_transport(
            fixture.owner.clone(),
            fixture.replica.clone(),
            detached_send_state.clone(),
        ),
        LocalContentStore::new(&fixture.content_root).expect("reopened recovery content"),
    );
    recovery.set_metadata_backend_unhydrated(backend);
    recovery
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .expect("explicit recovery hydration");

    assert_eq!(scan_probe.test_receipt_scan_counts(), (1, 0));
    assert_eq!(
        detached_send_state.queued_send_count(),
        1,
        "the durable committed replica receipt must reconstruct one promotion"
    );

    let reopened_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("second reopened metadata backend");
    let mut rebuilt_lane = NetworkedDfsService::new(
        recovery_hydration_node_transport(
            fixture.owner,
            fixture.replica,
            detached_send_state.clone(),
        ),
        LocalContentStore::new(fixture.content_root).expect("second reopened content"),
    );
    rebuilt_lane.set_metadata_backend_unhydrated(reopened_backend);
    rebuilt_lane
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .expect("recovery hydration after service rebuild");
    assert_eq!(
        detached_send_state.queued_send_count(),
        1,
        "shared identity state must deduplicate the exact recovered promotion across service rebuilds"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn recovery_hydration_does_not_promote_from_non_authoritative_object_owner() {
    let root = temp_root("object-owner-no-promotion-authority");
    let fixture = seed_durable_commit_promotion_fixture(&root, "non-authoritative-owner");
    let target = NodeId::from_legacy_named("node_recovery_hydrate_target_non_authoritative")
        .expect("target");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &fixture.metadata_root)
            .expect("reopened metadata backend");
    let mut intent = backend
        .get_intent(&fixture.object_id, &fixture.intent_id)
        .expect("read committed intent")
        .expect("committed intent");
    intent.coordinator_node_id = fixture.replica.clone();
    intent.desired_replica_node_ids.push(target.clone());
    intent.policy = DfsReplicationPolicy::new(2, 3).expect("three-node repair policy");
    backend
        .put_intent(&intent)
        .expect("rebind test coordinator");
    backend
        .put_receipt(&DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: intent.file_system_id.clone(),
                object_id: fixture.object_id.clone(),
                committed_version: fixture.target_version.clone(),
                owner_node_id: fixture.replica.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: target.clone(),
            content_hash: Some(fixture.content_hash.clone()),
            received_at: OffsetDateTime::now_utc(),
        })
        .expect("target receipt");
    drop(backend);

    let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    control_plane.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        store
            .put_file_system_config(&DfsFileSystemConfig {
                file_system_id: intent.file_system_id.clone(),
                display_name: "SIFS".to_string(),
                owner_node_id: fixture.owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem config");
        store
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: intent.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: fixture.replica.clone(),
                replica_node_ids: vec![fixture.owner.clone(), target.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("services shard");
    });

    let detached_send_state = DfsNodeDetachedSendState::new();
    let transport = recovery_hydration_node_transport(
        fixture.owner.clone(),
        fixture.replica,
        detached_send_state.clone(),
    );
    let mut recovery = NetworkedDfsService::new(
        transport,
        LocalContentStore::new(fixture.content_root).expect("reopened owner content"),
    )
    .with_control_plane(control_plane);
    recovery.set_metadata_backend_unhydrated(
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), fixture.metadata_root)
            .expect("reopen modified metadata"),
    );
    recovery
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .expect("explicit recovery hydration");

    assert_eq!(
        detached_send_state.queued_send_count(),
        0,
        "object ownership alone must not authorize recovered replica promotions"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn recovery_hydration_can_cancel_between_synchronous_backend_phases() {
    let root = temp_root("recovery-hydration-cancels-between-phases");
    let node = NodeId::from_legacy_named("node_recovery_hydration_cancel").expect("node");
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
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
        .expect("cancellable recovery hydration");

    assert!(!completed);
    assert!(
        cancellation_checks.load(Ordering::SeqCst) >= 2,
        "hydration must recheck foreground cancellation after synchronous backend work"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn recovery_hydration_skips_fully_repaired_committed_pending_write() {
    let root = temp_root("hydrate-skips-fully-repaired-committed");
    let owner = NodeId::from_legacy_named("node_hydrate_full_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_hydrate_full_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_hydrate_full").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("owner backend");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("metadata backend");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content")).expect("replica content"),
    );
    let file_system = file_system(owner.clone());
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    owner_dfs
        .add_file_system(file_system.clone())
        .expect("owner fs");
    replica_dfs
        .add_file_system(file_system)
        .expect("replica fs");
    owner_dfs
        .add_object_metadata(metadata.clone())
        .expect("owner object");
    replica_dfs
        .add_object_metadata(metadata)
        .expect("replica object");

    let write = owner_dfs
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                target_version: version_2,
                requesting_node_id: owner.clone(),
            },
            b"fully repaired bytes".to_vec(),
            vec![owner.clone(), replica.clone()],
            DfsReplicationPolicy::new(2, 2).expect("policy"),
        )
        .expect("pending write");
    assert!(matches!(write, NetworkedDfsWriteOutcome::Pending { .. }));
    replica_dfs.process_incoming().expect("replica stage");
    owner_dfs.process_incoming().expect("owner ack");
    replica_dfs.process_incoming().expect("replica commit");
    assert!(
        owner_dfs.writes.pending_writes.is_empty(),
        "fully repaired committed writes should leave no local pending repair state"
    );
    assert!(owner_dfs.coordinator().repair_jobs().is_empty());
    drop(owner_dfs);

    let recovered_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("owner"), &metadata_root)
            .expect("recovered backend");
    let recovered_promotion_calls = Arc::new(AtomicUsize::new(0));
    let sender_promotion_calls = Arc::clone(&recovered_promotion_calls);
    let recovered_sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
        let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
            return Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            });
        };
        sender_promotion_calls.fetch_add(1, Ordering::SeqCst);
        Ok(DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: vec![DfsRoutedMessage {
                source_node_id: command.routed_message.target_node_id.clone(),
                target_node_id: command.routed_message.source_node_id.clone(),
                frame_id: command.routed_message.frame_id,
                message_id: command.routed_message.message_id,
                message: DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                    request_id: commit.request_id.clone(),
                    intent_id: commit.intent_id.clone(),
                    file_system_id: commit.file_system_id.clone(),
                    object_id: commit.object_id.clone(),
                    version: commit.version.clone(),
                    replica_node_id: command.routed_message.target_node_id.clone(),
                    result: DfsBlobTransferResult::Committed {
                        content_hash: commit.content_hash.clone(),
                    },
                }),
            }],
        })
    });
    let recovered_transport = DfsNodeServedMessageTransport::new_with_message_sender(
        owner.clone(),
        vec![replica],
        recovered_sender,
        Duration::from_secs(1),
    );
    let mut recovered_owner = NetworkedDfsService::new(
        recovered_transport,
        LocalContentStore::new(root.join("owner-content")).expect("recovered content"),
    );
    recovered_owner.set_metadata_backend_unhydrated(recovered_backend);
    recovered_owner
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .expect("recovered metadata and post-response work");

    assert!(
        recovered_owner.writes.pending_writes.is_empty(),
        "fully receipted committed intents should not become pending writes after hydration"
    );
    assert!(recovered_owner.coordinator().repair_jobs().is_empty());
    assert!(
        recovered_owner
            .message_deferred_work_pending()
            .expect("recovered promotion state"),
        "hydration must reconstruct CommitVersion work from the durable committed intent and receipts"
    );
    recovered_owner
        .drain_message_deferred_work()
        .expect("replay recovered promotion");
    assert_eq!(recovered_promotion_calls.load(Ordering::SeqCst), 1);
    assert!(
        !recovered_owner
            .message_deferred_work_pending()
            .expect("settled recovered promotion state")
    );
    recovered_owner
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .expect("repeat hydration after promotion acknowledgement");
    assert!(
        !recovered_owner
            .message_deferred_work_pending()
            .expect("deduplicated recovered promotion state"),
        "a settled exact promotion must not be requeued by periodic hydration"
    );
    assert_eq!(recovered_promotion_calls.load(Ordering::SeqCst), 1);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrate_recovers_zero_byte_create_after_authoritative_view_refresh_failure_once() {
    let root = temp_root("hydrate-zero-byte-creates-after-authority-refresh-failure");
    let owner = NodeId::from_legacy_named("node_hydrate_zero_create_refresh_owner").expect("owner");
    let metadata_replica =
        NodeId::from_legacy_named("node_hydrate_zero_create_refresh_replica").expect("replica");
    let file_system = file_system(owner.clone());
    let metadata_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: owner.clone(),
        replica_node_ids: vec![metadata_replica],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let shared_control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared_control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
            .expect("filesystem config");
        control
            .put_metadata_shard(&metadata_shard)
            .expect("metadata shard");
    });
    let control_plane =
        FailingMetadataOperationFloorReadKvClient::new(shared_control_plane.clone());
    let metadata_root = root.join("metadata");
    let content_root = root.join("content");
    let parent_path = "/services/authority-refresh-recovery";
    let child_path = "/services/authority-refresh-recovery/page-01.txt";
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let empty_content_hash = content_hash(&[]);
    let metadata = open_root_local_metadata_backend(&metadata_root).expect("metadata backend");
    let mut original = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        LocalContentStore::new(&content_root).expect("content store"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(metadata)
    .expect("attach metadata backend");
    original
        .bootstrap_distributed_sifs()
        .expect("bootstrap SIFS");
    original
        .posix_mkdir_all(parent_path)
        .expect("create parent");
    let parent_before = original.posix_stat(parent_path).expect("parent before");
    let parent_version_count_before = original
        .metadata_backend()
        .expect("metadata backend")
        .versions_for_object(&parent_before.object_id)
        .expect("parent versions before")
        .len();

    control_plane.set_fail_operation_floor_reads(true);
    let error = original
        .posix_create_file_with_contents_and_attrs_and_operation_id(
            child_path,
            &[],
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            DfsPosixCreateAttrs::new(0o640, 6101, 6102),
            &operation_id,
        )
        .expect_err("post-prepare authoritative view failure must reach the caller");
    assert!(
        error.to_string().contains("retry the read"),
        "unexpected injected create error: {error}"
    );
    control_plane.set_fail_operation_floor_reads(false);
    assert!(
        control_plane
            .failed_operation_floor_reads
            .load(Ordering::SeqCst)
            > 0,
        "the create must reach the post-prepare operation-floor read"
    );
    assert!(
        control_plane.refresh_calls.load(Ordering::SeqCst) > 0,
        "bounded retry must refresh the authoritative view before surfacing exhaustion"
    );
    let pending_page = original
        .metadata_backend()
        .expect("metadata backend")
        .write_intents_page(None, 16)
        .expect("pending intents");
    let pending = pending_page
        .intents
        .iter()
        .filter(|intent| intent.status == DfsWriteIntentStatus::ReceiptsPending)
        .collect::<Vec<_>>();
    assert_eq!(pending.len(), 1);
    let intent = pending[0];
    assert!(intent.touch_parent_on_create);
    assert_eq!(
        intent.content_hash.as_deref(),
        Some(empty_content_hash.as_str()),
        "pending intent must retain the canonical zero-byte content identity"
    );
    assert_eq!(
        original
            .metadata_backend()
            .expect("metadata backend")
            .receipts_for_version(&intent.object_id, &intent.target_version)
            .expect("durable local receipt")
            .len(),
        1,
        "the failed zero-byte create must remain commit-ready"
    );
    assert_eq!(
        original
            .metadata_backend()
            .expect("metadata backend")
            .metadata_summary()
            .expect("metadata summary")
            .pending_write_count,
        1
    );
    assert!(
        original
            .metadata_backend()
            .expect("metadata backend")
            .get_object_by_path(&sifs_file_system_id(), child_path)
            .expect("hidden path lookup")
            .is_none(),
        "failed create must stay hidden until recovery"
    );
    drop(original);

    let recovered_metadata =
        open_root_local_metadata_backend(&metadata_root).expect("recovered metadata backend");
    let recovered = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner.clone(),
        },
        LocalContentStore::new(&content_root).expect("recovered content store"),
    )
    .with_control_plane(control_plane.clone())
    .with_metadata_backend(recovered_metadata)
    .expect("healthy reopen must recover commit-ready creates");
    let recovered_child = recovered
        .posix_stat(child_path)
        .expect("recovered zero-byte child");
    assert_eq!(recovered_child.size_bytes, Some(0));
    assert_eq!(
        recovered
            .metadata_backend()
            .expect("metadata backend")
            .metadata_summary()
            .expect("metadata summary")
            .pending_write_count,
        0
    );
    assert!(recovered.writes.pending_writes.is_empty());
    let parent_after_recovery = recovered.posix_stat(parent_path).expect("recovered parent");
    assert_ne!(parent_after_recovery.version, parent_before.version);
    assert!(parent_after_recovery.modified_at_unix_nanos > parent_before.modified_at_unix_nanos);
    assert_eq!(
        recovered
            .metadata_backend()
            .expect("metadata backend")
            .versions_for_object(&parent_before.object_id)
            .expect("parent versions after recovery")
            .len(),
        parent_version_count_before + 1,
        "the recovered create must touch the parent exactly once"
    );
    drop(recovered);

    let reopened_metadata =
        open_root_local_metadata_backend(&metadata_root).expect("reopened metadata backend");
    let reopened = NetworkedDfsService::new(
        FailingSendTransport {
            local_node_id: owner,
        },
        LocalContentStore::new(&content_root).expect("reopened content store"),
    )
    .with_control_plane(control_plane)
    .with_metadata_backend(reopened_metadata)
    .expect("second reopen must be idempotent");
    let reopened_child = reopened.posix_stat(child_path).expect("reopened child");
    assert_eq!(reopened_child.object_id, recovered_child.object_id);
    assert_eq!(reopened_child.version, recovered_child.version);
    assert_eq!(reopened_child.size_bytes, Some(0));
    assert_eq!(
        reopened.posix_stat(parent_path).expect("reopened parent"),
        parent_after_recovery,
        "second hydration must not repeat recovered parent effects"
    );
    assert_eq!(
        reopened
            .metadata_backend()
            .expect("metadata backend")
            .metadata_summary()
            .expect("metadata summary")
            .pending_write_count,
        0
    );
    assert!(reopened.writes.pending_writes.is_empty());
    drop(reopened);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_survives_metadata_backend_reopen() {
    let root = temp_root("sifs-recursive-clear-reopen");
    let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
    let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
    let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(clear_root).expect("mkdir");
        dfs.posix_write_file(
            file_path,
            b"tmp",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write");
        assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"tmp");
        dfs.posix_clear_dir_recursive(clear_root)
            .expect("clear recursive");
        assert!(matches!(
            dfs.posix_read_file(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id.clone());
        assert!(matches!(
            dfs.posix_read_file(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(clear_root)
            .expect("recreate cleared root");
        dfs.posix_write_file(
            file_path,
            b"fresh",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("rewrite after clear");
        assert_eq!(
            dfs.posix_read_file(file_path).expect("fresh read"),
            b"fresh"
        );
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_recursive_clear_bounded_job_resumes_after_reopen() {
    let root = temp_root("sifs-recursive-clear-bounded-job");
    let node_id = NodeId::from_legacy_named("node_sifs_clear_job").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_job/tmp/0";
    let nested_dir = format!("{clear_root}/nested/deeper");
    let files = [
        format!("{clear_root}/root-cache.txt"),
        format!("{clear_root}/nested/cache-a.txt"),
        format!("{nested_dir}/cache-b.txt"),
        format!("{nested_dir}/cache-c.txt"),
    ];
    let job_id = {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all(&nested_dir).expect("mkdir nested");
        for path in &files {
            dfs.posix_write_file(
                path,
                path.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write file");
        }

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

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        assert!(
            dfs.posix_list_dir(clear_root)
                .expect("cleared root list")
                .is_empty()
        );
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id);
        let job = dfs
            .metadata_backend
            .as_ref()
            .expect("backend")
            .get_namespace_job(&job_id)
            .expect("get completed job")
            .expect("job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        for path in &files {
            assert!(matches!(
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_directory_bounded_job_resumes_after_reopen() {
    let root = temp_root("sifs-posix-rename-dir-bounded-job");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_job").expect("node");
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
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
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
            .posix_rename_directory_bounded(old_dir, new_dir, 1)
            .expect("first bounded rename batch");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(
            !job.pending_frames.is_empty(),
            "a one-step directory rename should persist remaining subtree work"
        );
        assert!(matches!(
            dfs.posix_stat(old_dir),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            dfs.posix_stat(new_dir)
                .expect("new root after first batch")
                .kind,
            DfsObjectKind::Directory
        );
        job.job_id
    };

    {
        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        let mut job = dfs
            .posix_resume_namespace_job_bounded(&job_id, 2)
            .expect("resume bounded rename after reopen");
        while job.status != DfsNamespaceJobStatus::Completed {
            job = dfs
                .posix_resume_namespace_job_bounded(&job_id, 2)
                .expect("resume next rename batch");
        }
        assert!(job.processed_object_count >= old_files.len() as u64);
        for path in &old_files {
            assert!(matches!(
                dfs.posix_read_file(path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }
        for (old_path, new_path) in old_files.iter().zip(new_files.iter()) {
            assert_eq!(
                dfs.posix_read_file(new_path).expect("new file content"),
                old_path.as_bytes()
            );
        }
    }

    {
        let dfs = networked_sifs_with_backend(&root, node_id);
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
                dfs.posix_stat(new_path).expect("reloaded moved file").kind,
                DfsObjectKind::File
            );
        }
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_unlink_tombstone_survives_fresh_shared_metadata_recovery() {
    let root = temp_root("sifs-unlink-shared-metadata-recovery");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_unlink_shared_metadata").expect("node");
    let path = "/services/shared-unlink-recovery.txt";

    let mut seed =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-seed");
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_write_file(
        path,
        b"delete me",
        DfsWriteOptions::replicated_eventual(vec![node.clone()]),
    )
    .expect("create file");

    let mut remover =
        open_shared_metadata_posix_service(&root, &metadata_root, node.clone(), "content-unlink");
    let live_changed_at = remover
        .posix_lstat(path)
        .expect("live file stat")
        .changed_at_unix_nanos;
    let removed = remover.posix_unlink(path).expect("unlink file");
    assert!(
        removed.changed_at_unix_nanos > live_changed_at,
        "unlink tombstone ctime must fence historical namespace transactions"
    );

    let verifier =
        open_shared_metadata_posix_service(&root, &metadata_root, node, "content-verify");
    assert!(matches!(
        verifier.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        verifier
            .metadata_backend()
            .expect("verifier backend")
            .get_object_by_path(&sifs_file_system_id(), path)
            .expect("durable path lookup")
            .expect("durable tombstone")
            .kind,
        DfsObjectKind::Tombstone
    );
}

#[test]
fn sifs_cluster_posix_contract_writer_restart_during_pending_write() {
    let mut cluster = sifs_posix_cluster("sifs-contract-pending-restart");
    let scenario = "writer-restart-during-pending-write";
    let path = "/users/contract-pending-restart.txt";
    let old = b"old committed bytes".to_vec();
    let new = b"pending bytes should not be partially visible".to_vec();
    let mut observations = Vec::new();
    sifs_cluster_routed_posix_write(&mut cluster, path, &old);

    let pending = cluster.writer.posix_write_file(
        path,
        &new,
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
            replica_candidates: vec![cluster.writer_node.clone(), cluster.observer_node.clone()],
            wait_for_commit: false,
            foreground_replica_repair: true,
        },
    );
    record_posix_observation(
        &mut observations,
        scenario,
        "pending-write",
        &cluster.writer_node,
        path,
        Instant::now(),
        format!("{pending:?}"),
        pending.as_ref().ok(),
    );
    let primary_stat = cluster.primary.posix_stat(path).expect("primary stat");
    assert_eq!(
        primary_stat.content_hash.as_deref(),
        Some(content_hash(&old).as_str()),
        "pending write must not be visible before restart/recovery"
    );

    let writer_node = cluster.writer_node.clone();
    let writer_root = cluster.root.join("writer");
    let placeholder = NetworkedDfsService::new(
        sinomo_core::network::InProcessNetwork::new().add_node(
            NodeId::from_legacy_named("node_contract_pending_placeholder").expect("node"),
        ),
        LocalContentStore::new(cluster.root.join("unused-pending-content")).expect("content"),
    );
    let writer = std::mem::replace(&mut cluster.writer, placeholder);
    drop(writer);
    let mut restarted_writer = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(writer_node.clone()),
        LocalContentStore::new(writer_root.join("content")).expect("reopened content"),
    )
    .with_metadata_backend(
        open_root_local_metadata_backend(writer_root.join("metadata")).expect("reopened metadata"),
    )
    .expect("restarted writer");
    restarted_writer
        .bootstrap_distributed_sifs()
        .expect("restarted bootstrap");
    let restarted = restarted_writer.posix_read_file(path);
    assert!(
        restarted.as_ref().is_ok_and(|bytes| bytes == &old)
            || matches!(restarted, Err(DfsPosixError::NotFound { .. })),
        "restart must not expose partial pending bytes: {restarted:?}"
    );
    let metrics = restarted_writer
        .operational_metrics()
        .expect("restarted metrics");
    assert_eq!(
        metrics.pending_write_count, 0,
        "restarted writer should not keep an unobservable pending write"
    );
    finish_posix_observations(&observations);
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_writer_recovers_from_local_blob_loss_after_async_repair() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-writer-blob-loss");
    let path = "/services/async-repaired-writer-loss.bin";
    let bytes = (0..64 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_hash = content_hash(&bytes);
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
    sifs_posix_cluster_pump_until(&mut cluster, "writer observes async write", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    sifs_posix_cluster_pump_until(&mut cluster, "primary async repair", |cluster| {
        cluster
            .primary
            .retry_pending_repairs()
            .expect("primary repair tick");
        cluster
            .primary
            .posix_read_file(path)
            .is_ok_and(|read| read == bytes)
    });
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "writer learns repaired primary location",
        |cluster| {
            cluster.writer.posix_stat(path).is_ok_and(|stat| {
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
    assert!(
        matches!(
            cluster.writer.posix_read_file(path),
            Err(DfsPosixError::NotFound { .. })
        ),
        "fault injection must remove the writer's local readable content"
    );

    let recovered =
        sifs_cluster_fetch_for_local_read(&mut cluster, |cluster| &mut cluster.writer, path);
    assert_eq!(recovered, bytes);
    assert!(
        cluster.rejections.is_empty(),
        "journal replication should remain clean: {:?}",
        cluster.rejections
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn restart_catchup_accepts_foreground_attachment() {
    let mut cluster = sifs_posix_cluster("sifs-restart-catchup-foreground-attachment");
    let missed_path = "/users/missed-while-offline.txt";
    let foreground_path = "/users/foreground-after-restart.txt";
    let missed_bytes = b"metadata committed while the replica was offline";
    let foreground_bytes = b"foreground attachment submitted during catch-up";

    let writer_node = cluster.writer_node.clone();
    let writer_transport = cluster.writer.transport.clone();
    let writer_root = cluster.root.join("writer");
    let placeholder = NetworkedDfsService::new(
        InProcessNetwork::new()
            .add_node(NodeId::from_legacy_named("node_restart_catchup_placeholder").expect("node")),
        LocalContentStore::new(cluster.root.join("unused-restart-catchup-content"))
            .expect("placeholder content"),
    );
    drop(std::mem::replace(&mut cluster.writer, placeholder));

    sifs_cluster_primary_posix_write(&mut cluster, missed_path, missed_bytes);

    let catchup_control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    catchup_control_plane.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                cluster.primary_node.clone(),
            )))
            .expect("filesystem config");
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: cluster.primary_node.clone(),
                replica_node_ids: vec![writer_node.clone(), cluster.observer_node.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .expect("root shard config");
    });
    cluster.writer = NetworkedDfsService::new(
        writer_transport,
        LocalContentStore::new(writer_root.join("content")).expect("reopened content"),
    )
    .with_control_plane(catchup_control_plane)
    .with_metadata_backend(
        open_root_local_metadata_backend(writer_root.join("metadata")).expect("reopened metadata"),
    )
    .expect("restarted writer");

    sifs_cluster_routed_posix_write(&mut cluster, foreground_path, foreground_bytes);
    assert_eq!(
        cluster
            .writer
            .posix_read_file(foreground_path)
            .expect("foreground attachment after catch-up"),
        foreground_bytes
    );
    sifs_cluster_publish_metadata_until(&mut cluster, "offline metadata catch-up", |cluster| {
        cluster.writer.posix_stat(missed_path).is_ok()
    });
    assert_eq!(
        sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.writer, missed_path),
        missed_bytes
    );
    assert!(
        cluster.rejections.is_empty(),
        "restart catch-up and foreground attachment must share one consistent journal: {:?}",
        cluster.rejections
    );
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_posix_routed_write_survives_writer_restart() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-restart");
    let path = "/users/restart-durable.txt";
    let bytes = b"these bytes must survive a writer restart";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);
    assert_eq!(
        cluster
            .writer
            .posix_read_file(path)
            .expect("pre-restart read"),
        bytes.to_vec()
    );

    // Simulate a writer crash/restart: drop the service and rebuild it from
    // the same durable content store and metadata backend.
    let writer_node = cluster.writer_node.clone();
    let writer_root = cluster.root.join("writer");
    let placeholder = NetworkedDfsService::new(
        sinomo_core::network::InProcessNetwork::new().add_node(writer_node.clone()),
        LocalContentStore::new(cluster.root.join("unused-content"))
            .expect("placeholder content store"),
    );
    let writer = std::mem::replace(&mut cluster.writer, placeholder);
    drop(writer);

    let mut restarted = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(writer_node),
        LocalContentStore::new(writer_root.join("content")).expect("reopened content store"),
    )
    .with_metadata_backend(
        open_root_local_metadata_backend(writer_root.join("metadata"))
            .expect("reopened metadata backend"),
    )
    .expect("restarted writer service");
    restarted
        .bootstrap_distributed_sifs()
        .expect("restarted SIFS bootstrap");

    // Committed state must be durable: same bytes, same stat identity.
    assert_eq!(
        restarted.posix_read_file(path).expect("post-restart read"),
        bytes.to_vec()
    );
    let stat = restarted.posix_stat(path).expect("post-restart stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );
    let entries = restarted
        .posix_list_dir("/users")
        .expect("post-restart list");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "restarted writer must still list the committed path: {entries:?}"
    );

    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn sifs_cluster_empty_create_routes_retries_replicates_and_survives_restart() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-empty-create");
    let create_path = "/users/empty-create";
    let exclusive_path = "/users/empty-exclusive-create";
    let expected_hash = content_hash(&[]);
    let attrs = DfsPosixCreateAttrs::new(0o640, 6101, 6102);

    // O_CREAT without O_EXCL still routes a zero-byte creation to the remote
    // metadata primary and returns the primary's canonical empty identity.
    let writer_node = cluster.writer_node.clone();
    let create_stat = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "blocking routed empty O_CREAT",
        move |writer| {
            writer.posix_write_file_with_create_attrs(
                create_path,
                &[],
                DfsWriteOptions::replicated_eventual(vec![writer_node]),
                attrs,
            )
        },
    )
    .expect("routed empty O_CREAT");
    assert_eq!(create_stat.kind, DfsObjectKind::File);
    assert_eq!(create_stat.size_bytes, Some(0));
    assert_eq!(
        create_stat.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        cluster
            .primary
            .posix_stat(create_path)
            .expect("primary sees routed empty O_CREAT")
            .content_hash
            .as_deref(),
        Some(expected_hash.as_str())
    );

    // Use all three replicas so the O_EXCL create normally remains pending
    // while its exact routed command is captured. Whether it is still pending
    // or has just committed, the primary must replay it instead of performing
    // a second namespace create.
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(3, 3).expect("three-replica policy"),
        replica_candidates: vec![
            cluster.writer_node.clone(),
            cluster.observer_node.clone(),
            cluster.primary_node.clone(),
        ],
        wait_for_commit: false,
        foreground_replica_repair: true,
    };
    let first_create = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "initial routed empty O_EXCL",
        move |writer| writer.posix_create_file_with_attrs(exclusive_path, options, attrs),
    );
    assert!(
        matches!(
            first_create,
            Ok(_) | Err(DfsPosixError::PendingWrite { .. })
        ),
        "initial routed empty O_EXCL create failed: {first_create:?}"
    );
    let command = cluster
        .writer
        .writes
        .pending_routed_writes
        .values()
        .find(|pending| {
            pending.command.as_ref().is_some_and(|command| {
                command.exclusive_create
                    && command
                        .object_metadata
                        .as_ref()
                        .is_some_and(|object| object.path == exclusive_path)
            })
        })
        .expect("pending routed empty create")
        .command
        .clone()
        .expect("pending routed empty create command");
    assert!(command.exclusive_create);
    assert!(command.bytes.is_empty());
    assert_eq!(command.content_hash, None);
    assert_eq!(command.content_size_bytes, None);
    assert!(!command.requester_blob_pre_staged);

    assert!(
        cluster
            .writer
            .send_dfs_message_if_routable(
                &cluster.primary_node,
                DfsNetworkMessage::ExclusiveCreateRequest(command),
            )
            .expect("resend exact empty create")
    );
    cluster
        .primary
        .process_incoming()
        .expect("primary handles exact empty-create retry");

    sifs_posix_cluster_pump_until(&mut cluster, "empty O_EXCL create settles", |cluster| {
        sifs_routed_writes_settled(&cluster.writer)
            && cluster.writer.posix_stat(exclusive_path).is_ok_and(|stat| {
                stat.size_bytes == Some(0)
                    && stat.content_hash.as_deref() == Some(expected_hash.as_str())
            })
    });
    sifs_cluster_publish_metadata_until(
        &mut cluster,
        "empty O_EXCL create reaches metadata replica",
        |cluster| {
            cluster
                .observer
                .posix_stat(exclusive_path)
                .is_ok_and(|stat| {
                    stat.size_bytes == Some(0)
                        && stat.content_hash.as_deref() == Some(expected_hash.as_str())
                })
        },
    );

    for (label, service) in [
        ("primary", &mut cluster.primary),
        ("writer", &mut cluster.writer),
        ("observer", &mut cluster.observer),
    ] {
        let stat = service
            .posix_stat(exclusive_path)
            .unwrap_or_else(|error| panic!("{label} stat failed: {error}"));
        assert_eq!(stat.kind, DfsObjectKind::File, "{label} kind");
        assert_eq!(stat.size_bytes, Some(0), "{label} size");
        assert_eq!(
            stat.content_hash.as_deref(),
            Some(expected_hash.as_str()),
            "{label} canonical empty hash"
        );
        assert_eq!(
            service
                .posix_read_file(exclusive_path)
                .unwrap_or_else(|error| panic!("{label} empty read failed: {error}")),
            Vec::<u8>::new(),
            "{label} empty content"
        );
    }

    let writer_node = cluster.writer_node.clone();
    let duplicate_create = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "duplicate routed empty O_EXCL",
        move |writer| {
            writer.posix_create_file_with_attrs(
                exclusive_path,
                DfsWriteOptions::replicated_eventual(vec![writer_node]),
                attrs,
            )
        },
    );
    assert!(matches!(
        duplicate_create,
        Err(DfsPosixError::AlreadyExists { .. })
    ));

    // Reopen the non-primary writer from only its durable metadata and content
    // roots. The zero-byte manifest must remain readable after restart.
    let writer_node = cluster.writer_node.clone();
    let writer_root = cluster.root.join("writer");
    let placeholder = NetworkedDfsService::new(
        InProcessNetwork::new().add_node(writer_node.clone()),
        LocalContentStore::new(cluster.root.join("unused-empty-content"))
            .expect("placeholder content store"),
    );
    let writer = std::mem::replace(&mut cluster.writer, placeholder);
    drop(writer);
    let mut restarted = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(writer_node),
        LocalContentStore::new(writer_root.join("content")).expect("reopened content store"),
    )
    .with_metadata_backend(
        open_root_local_metadata_backend(writer_root.join("metadata"))
            .expect("reopened metadata backend"),
    )
    .expect("restarted writer metadata");
    restarted
        .bootstrap_distributed_sifs()
        .expect("restarted writer bootstrap");
    let restarted_stat = restarted
        .posix_stat(exclusive_path)
        .expect("restarted empty stat");
    assert_eq!(restarted_stat.size_bytes, Some(0));
    assert_eq!(
        restarted_stat.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        restarted
            .posix_read_file(exclusive_path)
            .expect("restarted empty read"),
        Vec::<u8>::new()
    );
    assert!(
        restarted
            .posix_list_dir("/users")
            .expect("restarted user listing")
            .iter()
            .any(|entry| entry.path == exclusive_path)
    );

    assert!(
        cluster.rejections.is_empty(),
        "empty routed create must not fork replica metadata: {:?}",
        cluster.rejections
    );
    std::fs::remove_dir_all(&cluster.root).ok();
}

#[test]
fn recovered_stage_response_committed_work_tracks_remaining_repairs_only() {
    for (case, include_missing_replica, expect_repair_maintenance) in
        [("remaining", true, true), ("complete", false, false)]
    {
        let root = temp_root(&format!("recovered-stage-committed-work-{case}"));
        let primary = NodeId::from_legacy_named(format!("node_recovered_work_primary_{case}"))
            .expect("primary");
        let replica = NodeId::from_legacy_named(format!("node_recovered_work_replica_{case}"))
            .expect("replica");
        let missing = NodeId::from_legacy_named(format!("node_recovered_work_missing_{case}"))
            .expect("missing replica");
        let object_id =
            DfsObjectId::from_legacy_named(format!("obj_recovered_work_{case}")).expect("object");
        let version_1 = DfsVersion::from_legacy_named(format!("version_recovered_work_{case}_1"))
            .expect("base version");
        let version_2 = DfsVersion::from_legacy_named(format!("version_recovered_work_{case}_2"))
            .expect("target version");
        let bytes = format!("recovered stage committed work {case}").into_bytes();
        let content_root = root.join("content");
        let metadata_root = root.join("metadata");
        let content = LocalContentStore::new(&content_root).expect("content store");
        let metadata = open_root_local_metadata_backend(&metadata_root).expect("metadata backend");
        let mut original =
            NetworkedDfsService::new(ReceiveOnlyTransport::new(primary.clone()), content)
                .with_metadata_backend(metadata)
                .expect("attach metadata backend");
        original
            .bootstrap_distributed_sifs()
            .expect("bootstrap sifs");
        let mut object = file_metadata(primary.clone());
        object.object_id = object_id.clone();
        object.path = format!("/services/recovered-work-{case}.bin");
        object.version = version_1.clone();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        object.current_locations = vec![primary.clone()];
        original
            .add_object_metadata(object)
            .expect("object metadata");
        let mut desired_replica_nodes = vec![primary.clone(), replica.clone()];
        if include_missing_replica {
            desired_replica_nodes.push(missing.clone());
        }
        let desired_replicas = desired_replica_nodes.len();
        let outcome = original
            .write_distributed_eventual(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    target_version: version_2.clone(),
                    requesting_node_id: primary.clone(),
                },
                bytes,
                desired_replica_nodes,
                DfsReplicationPolicy::new(2, desired_replicas).expect("policy"),
            )
            .expect("create pending write");
        let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome else {
            panic!("expected pre-recovery pending write, got {outcome:?}");
        };
        let content_hash = original
            .metadata_backend()
            .expect("metadata backend")
            .get_intent(&object_id, &intent_id)
            .expect("read intent")
            .expect("intent")
            .content_hash
            .expect("intent content hash");
        drop(original);

        let sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
            Err(DfsPeerTransportError::new(format!(
                "unexpected recovered-stage test send to {target_node_id} for {}",
                command.request_id
            )))
        });
        let transport = DfsNodeServedMessageTransport::new_with_message_sender(
            primary.clone(),
            vec![replica.clone(), missing.clone()],
            sender,
            Duration::from_millis(50),
        );
        let content = LocalContentStore::new(&content_root).expect("reopen content store");
        let metadata =
            open_root_local_metadata_backend(&metadata_root).expect("reopen metadata backend");
        let mut recovered = NetworkedDfsService::new(transport, content)
            .with_metadata_backend(metadata)
            .expect("restore metadata backend");
        assert!(
            recovered
                .writes
                .pending_writes
                .get(&intent_id)
                .is_some_and(|pending| pending.stage_inflight_request_ids.is_empty()),
            "recovery must not depend on ephemeral stage request tracking"
        );
        let report = recovered.process_message_command_report(test_dfs_message_command(
            format!("dfs_message_recovered_work_{case}"),
            build_dfs_routed_message(
                &replica,
                &primary,
                46,
                47,
                DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                    request_id: format!("lost_recovered_work_request_{case}"),
                    intent_id: intent_id.clone(),
                    file_system_id: sifs_file_system_id(),
                    object_id: object_id.clone(),
                    version: version_2,
                    replica_node_id: replica.clone(),
                    result: DfsBlobTransferResult::Durable { content_hash },
                }),
            ),
        ));

        let result = report.outcome.expect("serve recovered stage response");
        assert!(result.events.iter().any(|event| matches!(
            event,
            NetworkedDfsServiceEvent::BlobStaged { intent_id: event_intent_id, .. }
                if event_intent_id == &intent_id
        )));
        assert!(report.committed_work.publish_metadata);
        assert_eq!(
            report.committed_work.repair_maintenance, expect_repair_maintenance,
            "a recovered receipt must retain repair work exactly while desired replicas remain for {case}"
        );
        assert_eq!(
            report.committed_repair_records.len(),
            usize::from(expect_repair_maintenance),
            "the recovered commit must hand off only its newly created missing target for {case}"
        );
        assert!(matches!(
            recovered.write_outcome(&intent_id).expect("committed outcome"),
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. }
                if repair_jobs.is_empty() != expect_repair_maintenance
        ));

        std::fs::remove_dir_all(root).ok();
    }
}
