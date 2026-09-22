use super::*;
use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use sinomo_core::network::{
    ClusterConnectivityRecord, ClusterConnectivityState, ClusterEndpointId, cluster_route_next_hop,
};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
    DfsObjectPolicy, DfsWriteRequest,
};
use sinomo_dfs::{
    DfsBlobTransferResult, DfsDistributedWriteCommand, DfsFileSystemId,
    DfsMetadataJournalReplicationOutcome,
    DfsMetadataPublishPlanningAttempt as DaemonDfsMetadataPublishPlanningAttempt,
    DfsMetadataReplicaLagStatus, DfsNamespaceCommand, DfsNamespaceJobStatus, DfsNamespaceOperation,
    DfsNetworkMessage, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy,
    DfsRoutedMessage, DfsStageBlobCommand, DfsVersion, DfsWriteOptions, DistributedRepairJob,
    run_metadata_publish_blocking_execution as run_daemon_dfs_metadata_publish_blocking_execution,
    run_metadata_publish_lane_planning_with_admission as run_daemon_dfs_metadata_publish_lane_planning_with_admission,
};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Mutex};

fn applied_test_metadata_journal_response(
    command: &DfsMetadataJournalBatchCommand,
) -> DfsMetadataJournalBatchResponse {
    DfsMetadataJournalBatchResponse {
        request_id: command.request_id.clone(),
        shard_id: command.batch.shard_id.clone(),
        next_after_sequence: command.batch.next_after_sequence,
        applied_entries: command
            .durable_replication
            .as_ref()
            .map_or(command.batch.entries.len(), |replication| {
                replication.journal_frames.len()
            }),
        verified_prefix_only: command.verify_prefix_only,
        verified_authority_probe_id: command
            .durable_replication
            .as_ref()
            .map(|replication| replication.confirmation_id.clone()),
        result: DfsMetadataJournalBatchResult::Applied,
    }
}

#[test]
fn daemon_dfs_peer_transport_traits_share_one_adapter() {
    let root = temp_root("daemon-dfs-shared-peer-transport");
    let data_dir = SinomoDataDir::new(root.path());
    let runtime = daemon_test_runtime("daemon-dfs-shared-peer-transport");

    let (peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(&data_dir, runtime.handle());

    assert_eq!(
        Arc::as_ptr(&peer_transport) as *const (),
        Arc::as_ptr(&async_peer_transport) as *const (),
        "blocking and async DFS calls must reuse one peer transport adapter"
    );
}

#[test]
fn daemon_dfs_peer_snapshot_includes_members_committed_after_bootstrap_checkpoint() {
    let root = temp_root("dfs-peer-snapshot-kv-membership");
    let data_dir = SinomoDataDir::new(root.path());
    data_dir.ensure_root_layout().unwrap();
    let paths = ClusterPaths::new(&data_dir);
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("dfs-peer-snapshot-kv-membership").unwrap(),
        "peer membership",
        NodeId::from_legacy_named("dfs_peer_local").unwrap(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&paths, &state).unwrap();
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state).unwrap();
    let initial = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    assert_eq!(initial.peer_node_ids, vec![state.local_node_id.clone()]);
    let mut peer = state.members[0].clone();
    peer.node_id = NodeId::from_legacy_named("dfs_peer_joined_later").unwrap();
    peer.node_public_key = "pk-remote".into();
    let remote = peer.node_id.clone();
    state.members.push(peer);
    // Joining members are committed in KV; the local identity checkpoint can
    // legitimately remain older. Discovery must not permanently exclude them.
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state).unwrap();
    assert_eq!(
        sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .unwrap()
            .members
            .len(),
        1
    );
    sinomo_dfs::test_expire_dfs_runtime_peer_discovery_cache(
        &paths,
        DAEMON_DFS_PEER_DISCOVERY_VALIDATION_LEASE,
    );
    let checkpoint = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    assert!(
        sinomo_dfs::discovered_cluster_dfs_peer_node_ids(&paths, &checkpoint)
            .unwrap()
            .contains(&remote),
        "callers with an old identity checkpoint must discover KV members"
    );
    let peers = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    assert!(
        peers.peer_node_ids.contains(&remote),
        "committed member missing from DFS peers: {peers:?}"
    );
}

#[test]
fn daemon_dfs_peer_snapshot_retains_the_leased_state_during_replacement() {
    let root = temp_root("daemon-dfs-peer-snapshot-replacement");
    let data_dir = SinomoDataDir::new(root.path());
    data_dir.ensure_root_layout().expect("data directory");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_daemon_dfs_peer_snapshot_replacement")
            .expect("cluster id"),
        "daemon DFS peer snapshot replacement",
        NodeId::from_legacy_named("node_daemon_dfs_peer_snapshot_replacement").expect("node id"),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let first = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("read initial peer snapshot")
        .expect("initial peer snapshot");
    fs::remove_file(&cluster_paths.state).expect("simulate replacement namespace cut");
    let leased = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("read leased peer snapshot")
        .expect("leased peer snapshot must survive the replacement cut");

    assert_eq!(leased.cluster_id, first.cluster_id);
    assert_eq!(leased.local_node_id, first.local_node_id);
    assert_eq!(leased.peer_node_ids, first.peer_node_ids);
}

#[test]
fn daemon_dfs_peer_snapshot_leases_provider_discovery_without_leasing_identity() {
    let root = temp_root("daemon-dfs-peer-snapshot-provider-lease");
    let data_dir = SinomoDataDir::new(root.path());
    data_dir.ensure_root_layout().expect("data directory");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_daemon_dfs_peer_snapshot_provider_lease")
            .expect("cluster id"),
        "daemon DFS peer snapshot provider lease",
        NodeId::from_legacy_named("node_daemon_dfs_peer_snapshot_provider_lease").expect("node id"),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let first = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("read initial peer snapshot")
        .expect("initial peer snapshot");
    let kv_snapshot = cluster_paths.kv_paths().snapshot;
    fs::write(&kv_snapshot, b"invalid cluster KV snapshot")
        .expect("corrupt provider discovery source");

    let leased = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("read leased peer snapshot")
        .expect("leased peer snapshot");
    assert_eq!(leased.cluster_id, first.cluster_id);
    assert_eq!(leased.local_node_id, first.local_node_id);
    assert_eq!(leased.peer_node_ids, first.peer_node_ids);

    sinomo_dfs::test_expire_dfs_runtime_peer_discovery_cache(
        &cluster_paths,
        DAEMON_DFS_PEER_DISCOVERY_VALIDATION_LEASE,
    );
    assert!(
        read_daemon_dfs_message_peer_snapshot(&cluster_paths).is_err(),
        "an expired routing lease must validate the current provider catalog"
    );
    sinomo_dfs::test_clear_dfs_runtime_peer_discovery_cache(&cluster_paths);
}

fn mounted_mutation_test_repair_record(label: &str) -> DfsRepairAttemptRecord {
    DfsRepairAttemptRecord {
        intent_id: format!("mounted-mutation-{label}"),
        object_id: sinomo_core::Id::derived(
            "test.daemon.mounted-mutation.object",
            label.as_bytes(),
        ),
        version: sinomo_core::Id::derived("test.daemon.mounted-mutation.version", label.as_bytes()),
        content_hash: "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_string(),
        missing_replica_node_id: NodeId::from_legacy_named(format!(
            "node_mounted_mutation_{label}"
        ))
        .expect("mounted-mutation repair node"),
        attempts: 0,
        in_flight: false,
        last_attempt_unix_nanos: 0,
        next_retry_unix_nanos: 0,
        transfer_request_id: None,
        last_error: None,
    }
}

#[test]
fn mounted_mutation_handoff_is_exact_idempotent_and_queue_first() {
    let root = temp_root("mounted-mutation-handoff-exact");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-handoff-exact");
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let session = store
        .open_host_mount_session(sinomo_dfs::MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
        .expect("mount session");
    let mut mutation = store
        .prepare(&session, Some("create-1".to_string()), "create")
        .expect("prepare mutation");
    let marker_id = mutation.marker().marker_id.clone();
    let marker_nonce = mutation.marker().marker_nonce.clone();
    let arm_request_id = sinomo_core::Id::generate();
    let arm_request = crate::DaemonDfsMountedMutationHandoffRequest::Arm {
        marker_id: marker_id.clone(),
        marker_nonce: marker_nonce.clone(),
        arm_request_id: arm_request_id.clone(),
    };
    let first_arm = handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        arm_request.clone(),
    )
    .expect("first Arm");
    assert_eq!(
        first_arm.phase,
        crate::DaemonDfsMountedMutationHandoffPhase::Armed
    );
    assert_eq!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            arm_request,
        )
        .expect("same Arm retry"),
        first_arm
    );
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Arm {
                marker_id: marker_id.clone(),
                marker_nonce: marker_nonce.clone(),
                arm_request_id: sinomo_core::Id::generate(),
            },
        )
        .is_err(),
        "a different Arm request identity must conflict"
    );

    let repair = mounted_mutation_test_repair_record("queue-first");
    let captured_work = sinomo_dfs::MountedSifsMutationCapturedWork::new(
        sinomo_dfs::DfsCommittedWork {
            repair_maintenance: true,
            ..sinomo_dfs::DfsCommittedWork::default()
        },
        vec![repair.clone()],
    )
    .expect("captured work");
    mutation
        .record_captured_success(&arm_request_id, captured_work.clone())
        .expect("persist adapter capture");
    let commit_request = crate::DaemonDfsMountedMutationHandoffRequest::CommitApplied {
        marker_id: marker_id.clone(),
        marker_nonce: marker_nonce.clone(),
        arm_request_id: arm_request_id.clone(),
        captured_work: captured_work.clone(),
    };
    fail_next_daemon_dfs_mounted_mutation_after_repair_persist(&data_dir);
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            commit_request.clone(),
        )
        .is_err(),
        "injected crash cut must happen before DaemonAccepted"
    );
    assert!(matches!(
        store
            .marker(&marker_id)
            .expect("marker after crash cut")
            .expect("marker")
            .phase,
        sinomo_dfs::MountedSifsMutationOutboxPhase::Captured { .. }
    ));
    assert_eq!(
        LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data_dir))
            .view()
            .expect("repair queue after crash cut")
            .records,
        vec![repair],
        "the exact repair delta must be fsynced before daemon acceptance"
    );
    assert_eq!(
        store
            .daemon_committed_work_latch()
            .expect("latch after queue-only crash cut")
            .generation,
        0,
        "the injected cut is before the separately fsynced committed-work latch"
    );

    let commit = handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        commit_request.clone(),
    )
    .expect("retry commit");
    assert_eq!(
        commit.phase,
        crate::DaemonDfsMountedMutationHandoffPhase::DaemonAccepted
    );
    assert_eq!(commit.marker_id.as_ref(), Some(&marker_id));
    assert_eq!(commit.arm_request_id.as_ref(), Some(&arm_request_id));
    assert_eq!(commit.appended_repair_record_count, 0);
    let accepted_latch = store
        .daemon_committed_work_latch()
        .expect("accepted committed-work latch");
    assert_eq!(accepted_latch.generation, 1);
    assert!(accepted_latch.committed_work.repair_maintenance);
    assert_eq!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            commit_request,
        )
        .expect("idempotent commit retry")
        .appended_repair_record_count,
        0
    );
    assert_eq!(
        store
            .daemon_committed_work_latch()
            .expect("idempotent retry latch")
            .generation,
        accepted_latch.generation,
        "an exact accepted retry must not create a second latch generation"
    );
    let conflicting = sinomo_dfs::MountedSifsMutationCapturedWork::new(
        sinomo_dfs::DfsCommittedWork {
            namespace_maintenance: true,
            ..captured_work.committed_work
        },
        captured_work.committed_repair_records.clone(),
    )
    .expect("conflicting capture");
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::CommitApplied {
                marker_id,
                marker_nonce,
                arm_request_id,
                captured_work: conflicting,
            },
        )
        .is_err(),
        "accepted capture must not admit a different retry payload"
    );
}

#[test]
fn mounted_mutation_handoff_requires_adapter_proof_and_exact_unknown_error() {
    let root = temp_root("mounted-mutation-handoff-proof");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-handoff-proof");
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let session = store
        .open_host_mount_session(
            sinomo_dfs::MountedSifsMutationAdapter::ExternalProvider,
            "sync-root",
        )
        .expect("mount session");

    let mut no_write = store
        .prepare(&session, Some("unlink-1".to_string()), "unlink")
        .expect("prepare no-write mutation");
    let no_write_id = no_write.marker().marker_id.clone();
    let no_write_nonce = no_write.marker().marker_nonce.clone();
    let no_write_arm = sinomo_core::Id::generate();
    handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        crate::DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id: no_write_id.clone(),
            marker_nonce: no_write_nonce.clone(),
            arm_request_id: no_write_arm.clone(),
        },
    )
    .expect("arm no-write mutation");
    let prove_request = crate::DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
        marker_id: no_write_id.clone(),
        marker_nonce: no_write_nonce,
        arm_request_id: no_write_arm.clone(),
        proof_code: "backend-proved-not-entered".to_string(),
    };
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            prove_request.clone(),
        )
        .is_err(),
        "daemon must not author adapter no-write evidence"
    );
    no_write
        .record_proven_no_write(&no_write_arm, "backend-proved-not-entered")
        .expect("persist adapter no-write proof");
    let proof = handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        prove_request,
    )
    .expect("attest exact no-write proof");
    assert_eq!(proof.marker_id.as_ref(), Some(&no_write_id));
    assert_eq!(
        proof.phase,
        crate::DaemonDfsMountedMutationHandoffPhase::ProvenNoWrite
    );

    let mut unknown = store
        .prepare(&session, Some("rename-1".to_string()), "rename")
        .expect("prepare unknown mutation");
    let unknown_id = unknown.marker().marker_id.clone();
    let unknown_nonce = unknown.marker().marker_nonce.clone();
    let unknown_arm = sinomo_core::Id::generate();
    handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        crate::DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id: unknown_id.clone(),
            marker_nonce: unknown_nonce.clone(),
            arm_request_id: unknown_arm.clone(),
        },
    )
    .expect("arm unknown mutation");
    let observation = sinomo_dfs::MountedSifsMutationErrorObservation::new(
        sinomo_dfs::MountedSifsMutationCapturedWork::default(),
        "possibly-applied",
    )
    .expect("unknown observation");
    unknown
        .record_unknown_after_error(&unknown_arm, observation.clone())
        .expect("persist unknown observation");
    handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &paths,
        runtime.handle(),
        None,
        crate::DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id: unknown_id.clone(),
            marker_nonce: unknown_nonce.clone(),
            arm_request_id: unknown_arm.clone(),
            observation: observation.clone(),
        },
    )
    .expect("accept exact unknown observation");
    let conflicting_observation = sinomo_dfs::MountedSifsMutationErrorObservation::new(
        observation.captured_work,
        "different-error",
    )
    .expect("conflicting observation");
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
                marker_id: unknown_id,
                marker_nonce: unknown_nonce,
                arm_request_id: unknown_arm,
                observation: conflicting_observation,
            },
        )
        .is_err(),
        "unknown-error retries must preserve the exact fsynced observation"
    );
}

#[test]
fn mounted_mutation_classifier_fences_live_work_and_recovery_retains_bounded_passes() {
    let root = temp_root("mounted-mutation-classifier-recovery");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-classifier-recovery");
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let session = store
        .open_host_mount_session(sinomo_dfs::MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
        .expect("mount session");
    let mut mutation = store
        .prepare(&session, Some("mkdir-1".to_string()), "mkdir")
        .expect("prepare mutation");
    let marker_id = mutation.marker().marker_id.clone();
    let arm_request_id = sinomo_core::Id::generate();

    let live = daemon_dfs_mounted_mutation_pending_work(&data_dir, 100)
        .expect("live mounted-mutation projection");
    assert_eq!(live.mounted_mutation_live_count, 1);
    assert_eq!(live.mounted_mutation_recoverable_exact_count, 0);
    assert_eq!(live.mounted_mutation_recoverable_full_count, 0);
    assert_eq!(live.deferred_work_count, 1);
    assert!(live.next_mounted_mutation_reprobe_unix_nanos.is_some());
    assert_eq!(
        daemon_dfs_pending_reactive_maintenance_work_on_runtime(&paths, runtime.handle())
            .expect("production startup classifier")
            .mounted_mutation_live_count,
        1,
        "the production startup probe must see outbox work before cluster state exists"
    );

    store
        .arm_live(&marker_id, &mutation.marker().marker_nonce, &arm_request_id)
        .expect("arm exact recovery marker");
    mutation
        .record_captured_success(
            &arm_request_id,
            sinomo_dfs::MountedSifsMutationCapturedWork::new(
                sinomo_dfs::DfsCommittedWork {
                    publish_metadata: true,
                    ..sinomo_dfs::DfsCommittedWork::default()
                },
                Vec::new(),
            )
            .expect("captured exact work"),
        )
        .expect("capture exact work");
    drop(mutation);
    let exact = daemon_dfs_mounted_mutation_pending_work(&data_dir, 200)
        .expect("exact mounted-mutation projection");
    assert_eq!(exact.mounted_mutation_live_count, 0);
    assert_eq!(exact.mounted_mutation_recoverable_exact_count, 1);
    assert!(exact.should_trigger_background_maintenance());

    let claim = store
        .try_claim_recovery(&marker_id)
        .expect("claim exact marker")
        .expect("exact recovery claim");
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(vec![claim], Vec::new(), false),
        DaemonDfsMaintenanceTickOutcome::Deferred,
    )
    .expect("retain deferred marker");
    assert!(matches!(
        store
            .marker(&marker_id)
            .expect("marker")
            .expect("marker")
            .phase,
        sinomo_dfs::MountedSifsMutationOutboxPhase::Captured { .. }
    ));

    let claim = store
        .try_claim_recovery(&marker_id)
        .expect("reclaim exact marker")
        .expect("exact recovery claim");
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(vec![claim], Vec::new(), false),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: true,
        },
    )
    .expect("retain bounded marker");
    assert!(matches!(
        store
            .marker(&marker_id)
            .expect("marker")
            .expect("marker")
            .phase,
        sinomo_dfs::MountedSifsMutationOutboxPhase::Captured { .. }
    ));

    let claim = store
        .try_claim_recovery(&marker_id)
        .expect("reclaim exact marker after limit")
        .expect("exact recovery claim");
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(vec![claim], Vec::new(), false),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        },
    )
    .expect("terminalize completed marker");
    assert_eq!(
        store
            .status()
            .expect("terminal status")
            .pending_marker_count,
        0
    );
    assert_eq!(
        store
            .daemon_committed_work_latch()
            .expect("exact recovery latch")
            .generation,
        1,
        "terminalization must retain a durable daemon ownership generation"
    );
}

#[test]
fn mounted_mutation_classifier_reports_pending_overflow_without_unbounded_scan_results() {
    let root = temp_root("mounted-mutation-classifier-overflow");
    let data_dir = SinomoDataDir::new(root.path());
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let session = store
        .open_host_mount_session(
            sinomo_dfs::MountedSifsMutationAdapter::ExternalProvider,
            "sync-root",
        )
        .expect("mount session");
    let mut live_mutations = Vec::new();
    for index in 0..sinomo_dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
        live_mutations.push(
            store
                .prepare(&session, Some(format!("write-{index}")), "write")
                .expect("prepare bounded marker"),
        );
    }
    let corrupt_id = sinomo_core::Id::generate();
    let corrupt_dir = store.root().join("operations").join(corrupt_id.as_str());
    fs::create_dir(&corrupt_dir).expect("create overflow operation");
    fs::write(corrupt_dir.join("state.sobj"), b"not-a-sobj").expect("write corrupt overflow state");
    fs::write(corrupt_dir.join("lease.lock"), b"").expect("write overflow lease");

    let pending = daemon_dfs_mounted_mutation_pending_work(&data_dir, 300)
        .expect("bounded overflow projection");
    assert_eq!(pending.mounted_mutation_overflow_count, 1);
    assert!(pending.mounted_mutation_live_count <= 64);
    assert!(pending.deferred_work_count > 0);
    assert!(pending.next_mounted_mutation_reprobe_unix_nanos.is_some());
    drop(live_mutations);
}

#[test]
fn mounted_mutation_main_startup_replays_the_durable_latch_once() {
    let root = temp_root("mounted-mutation-startup-latch-replay");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_mounted_mutation_startup_replay").expect("local node");
    write_test_cluster_state_with_members(root.path(), &local, Vec::new());
    let cluster_paths = ClusterPaths::new(&data_dir);
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let retained = store
        .retain_daemon_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            repair_maintenance: true,
            namespace_maintenance: true,
            metadata_replica_progress: true,
            metadata_materialization: true,
        })
        .expect("retain startup latch");
    let reconciliation_rx =
        sinomo_dfs::subscribe_default_dfs_reconciliation_replica_progress(&cluster_paths)
            .expect("default DFS reconciliation wake");
    let initial_reconciliation_generation = *reconciliation_rx.borrow();
    let initial_repair_edges = daemon_dfs_repair_maintenance_edge_request_count(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-startup-latch-replay");

    replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(&paths, runtime.handle())
        .expect("replay startup latch");

    assert_eq!(
        store
            .daemon_committed_work_latch()
            .expect("replayed latch")
            .generation,
        retained.generation,
        "startup replay must not destructively clear or rewrite the latch"
    );
    assert!(
        daemon_dfs_repair_maintenance_edge_request_count(&data_dir) > initial_repair_edges,
        "startup replay must restore the maintenance owner"
    );
    assert_ne!(
        *reconciliation_rx.borrow(),
        initial_reconciliation_generation,
        "startup replay must restore default-reconciliation ownership"
    );
    assert!(
        daemon_dfs_metadata_publish_discovery_is_active(&data_dir.root()),
        "startup replay must retain a metadata publisher discovery lane"
    );

    let replayed_repair_edges = daemon_dfs_repair_maintenance_edge_request_count(&data_dir);
    replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(
        &shadow_daemon_paths_from_data_dir(&data_dir),
        runtime.handle(),
    )
    .expect("shadow replay is a no-op");
    assert_eq!(
        daemon_dfs_repair_maintenance_edge_request_count(&data_dir),
        replayed_repair_edges,
        "a shadow daemon must never replay main-owned mounted work"
    );
}

#[test]
fn mounted_mutation_full_recovery_never_claims_a_live_mount_session() {
    let root = temp_root("mounted-mutation-full-recovery-fence");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-full-recovery-fence");
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    assert_eq!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .expect("empty outbox is ready")
        .phase,
        crate::DaemonDfsMountedMutationHandoffPhase::Ready
    );
    let session = store
        .open_host_mount_session(sinomo_dfs::MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
        .expect("mount session");
    let marker_id = {
        let mutation = store
            .prepare(&session, Some("unlink-unknown".to_string()), "unlink")
            .expect("prepare ambiguous marker");
        mutation.marker().marker_id.clone()
    };
    assert!(
        store
            .try_claim_recovery(&marker_id)
            .expect("live-session claim probe")
            .is_none(),
        "an orphan operation lock must not override the live external mount session"
    );
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .is_err(),
        "a new mount must not start while an ambiguity marker is pending"
    );
    let live =
        daemon_dfs_mounted_mutation_pending_work(&data_dir, 400).expect("live-session classifier");
    assert_eq!(live.mounted_mutation_recoverable_full_count, 0);
    assert_eq!(live.mounted_mutation_live_count, 1);

    drop(session);
    let dead =
        daemon_dfs_mounted_mutation_pending_work(&data_dir, 500).expect("dead-session classifier");
    assert_eq!(dead.mounted_mutation_recoverable_full_count, 1);
    assert!(dead.should_trigger_background_maintenance());
    let wake_count = daemon_dfs_maintenance_wake_count();
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .is_err(),
        "a recoverable marker still gates the replacement mount"
    );
    assert!(
        daemon_dfs_maintenance_wake_count() > wake_count,
        "the readiness probe must wake reactive recovery after the mount locks disappear"
    );
    let claim = store
        .try_claim_recovery(&marker_id)
        .expect("dead-session claim")
        .expect("full recovery claim");
    assert_eq!(
        claim.recovery_kind(),
        sinomo_dfs::MountedSifsMutationRecoveryKind::Full
    );
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(vec![claim], Vec::new(), true),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        },
    )
    .expect("terminalize full recovery marker");
    assert_eq!(
        store
            .status()
            .expect("terminal status")
            .pending_marker_count,
        0
    );
    let latch = store
        .daemon_committed_work_latch()
        .expect("full recovery latch");
    assert!(latch.committed_work.publish_metadata);
    assert!(latch.committed_work.repair_maintenance);
    assert!(latch.committed_work.namespace_maintenance);
    assert!(latch.committed_work.metadata_replica_progress);
    assert!(latch.committed_work.metadata_materialization);
    assert_eq!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .expect("completed recovery restores readiness")
        .phase,
        crate::DaemonDfsMountedMutationHandoffPhase::Ready
    );
}

#[test]
fn mounted_mutation_corrupt_recovery_requires_a_completed_full_pass() {
    let root = temp_root("mounted-mutation-corrupt-full-recovery");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let runtime = daemon_test_runtime("mounted-mutation-corrupt-full-recovery");
    let store = sinomo_dfs::MountedSifsMutationOutboxStore::new(&data_dir)
        .expect("mounted-mutation outbox");
    let marker_id = sinomo_core::Id::generate();
    let directory = store.root().join("operations").join(marker_id.as_str());
    fs::create_dir(&directory).expect("create corrupt operation");
    fs::write(directory.join("state.sobj"), b"not-a-sobj").expect("write corrupt state");
    fs::write(directory.join("lease.lock"), b"").expect("write orphan lease");
    assert!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .is_err(),
        "Probe must fail closed on corrupt work"
    );

    let claim = store
        .try_claim_corrupt_recovery(&marker_id)
        .expect("claim corrupt marker")
        .expect("corrupt recovery claim");
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(Vec::new(), vec![claim], true),
        DaemonDfsMaintenanceTickOutcome::Deferred,
    )
    .expect("retain corrupt marker after deferred pass");
    assert!(directory.is_dir());

    let claim = store
        .try_claim_corrupt_recovery(&marker_id)
        .expect("reclaim corrupt marker")
        .expect("corrupt recovery claim");
    complete_daemon_dfs_mounted_mutation_recovery(
        DaemonDfsMountedMutationRecoveryBatch::from_claims_for_test(Vec::new(), vec![claim], true),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        },
    )
    .expect("terminalize corrupt marker after full pass");
    assert_eq!(
        store
            .status()
            .expect("recovered status")
            .pending_marker_count,
        0
    );
    let latch = store
        .daemon_committed_work_latch()
        .expect("corrupt recovery latch");
    assert!(latch.committed_work.publish_metadata);
    assert!(latch.committed_work.repair_maintenance);
    assert!(latch.committed_work.namespace_maintenance);
    assert!(latch.committed_work.metadata_replica_progress);
    assert!(latch.committed_work.metadata_materialization);
    assert_eq!(
        handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
            &paths,
            runtime.handle(),
            None,
            crate::DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .expect("full recovery restores readiness")
        .phase,
        crate::DaemonDfsMountedMutationHandoffPhase::Ready
    );
}

#[test]
fn daemon_metadata_authority_retries_preserve_one_absolute_deadline() {
    fn shard_entry(shard: &DfsMetadataShardConfig) -> KvEntry {
        KvEntry {
            key: metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            value: sinomo_core::kv::encode_kv_value(shard).expect("encode shard"),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        }
    }

    fn scripted_client(
        name: &str,
    ) -> (
        DaemonDfsControlPlaneKvClient,
        tokio::runtime::Runtime,
        DfsMetadataShardConfig,
    ) {
        let root = temp_root(name);
        let data_dir = SinomoDataDir::new(root.path());
        let runtime = daemon_test_runtime(name);
        let shard = DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: NodeId::from_legacy_named(format!("node_{}", name.replace('-', "_")))
                .expect("node"),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        (
            DaemonDfsControlPlaneKvClient::new_on_runtime(
                &ClusterPaths::new(&data_dir),
                runtime.handle(),
            ),
            runtime,
            shard,
        )
    }

    let (mut sync_client, _sync_runtime, sync_shard) =
        scripted_client("daemon-authority-sync-absolute-deadline");
    sync_client
        .authoritative_exact_set_outcomes
        .lock()
        .expect("script sync authority outcomes")
        .extend([
            DaemonDfsTestAuthoritativeExactSetOutcome::Error(KvError::AuthorityUnavailable {
                detail: "temporary authority loss".to_string(),
            }),
            DaemonDfsTestAuthoritativeExactSetOutcome::Error(KvError::Io {
                path: "test authority generation".to_string(),
                detail: "resident Raft view changed after ReadIndex quorum; retry the read"
                    .to_string(),
            }),
            DaemonDfsTestAuthoritativeExactSetOutcome::Entries(vec![
                Some(shard_entry(&sync_shard)),
                None,
            ]),
        ]);
    let sync_deadline = Instant::now() + Duration::from_secs(1);
    assert!(
        sinomo_dfs::DfsControlPlaneStore::new(&mut sync_client)
            .metadata_shard_authority_is_current_before(&sync_shard, Some(sync_deadline),)
            .expect("sync authority retry before caller deadline")
    );
    assert_eq!(
        sync_client
            .authoritative_exact_set_deadlines
            .lock()
            .expect("sync authority deadlines")
            .as_slice(),
        &[sync_deadline; 3],
        "sync retries must not restart the daemon's 30-second read budget"
    );

    let (mut async_client, async_runtime, async_shard) =
        scripted_client("daemon-authority-async-absolute-deadline");
    async_client
        .authoritative_exact_set_outcomes
        .lock()
        .expect("script async authority outcomes")
        .extend([
            DaemonDfsTestAuthoritativeExactSetOutcome::Error(KvError::AuthorityUnavailable {
                detail: "temporary async authority loss".to_string(),
            }),
            DaemonDfsTestAuthoritativeExactSetOutcome::Entries(vec![
                Some(shard_entry(&async_shard)),
                None,
            ]),
        ]);
    let async_deadline = Instant::now() + Duration::from_secs(1);
    assert!(
            async_runtime
                .block_on(
                    sinomo_dfs::DfsControlPlaneAsyncStore::new(&mut async_client)
                        .metadata_shard_authority_is_current_before(
                            &async_shard,
                            Some(async_deadline),
                        ),
                )
                .expect("async authority retry before caller deadline")
        );
    assert_eq!(
        async_client
            .authoritative_exact_set_deadlines
            .lock()
            .expect("async authority deadlines")
            .as_slice(),
        &[async_deadline; 2],
        "async retries must not restart the daemon's 30-second read budget"
    );

    let (mut expired_client, _expired_runtime, expired_shard) =
        scripted_client("daemon-authority-expired-deadline");
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .expect("expired deadline");
    let keys = vec![
        metadata_shard_key(&expired_shard.file_system_id, &expired_shard.shard_id),
        sinomo_dfs::metadata_shard_membership_handoff_key(
            &expired_shard.file_system_id,
            &expired_shard.shard_id,
        ),
    ];
    assert!(matches!(
        sinomo_dfs::DfsControlPlaneKvClient::get_authoritative_consistent_before(
            &mut expired_client,
            &keys,
            expired,
        ),
        Err(KvError::AuthorityUnavailable { detail })
            if detail.contains("deadline elapsed")
    ));
    assert!(
        expired_client
            .authoritative_exact_set_deadlines
            .lock()
            .expect("expired authority deadlines")
            .is_empty(),
        "an expired caller budget must not start an exact authority read"
    );
}

// Restart recovery deliberately keeps the permit record private. Tests
// encode the durable shape directly to model residue from a dead process;
// `inspect_dfs_placement_authority_permit` below proves it remains
// compatible with the production decoder before recovery runs.
#[derive(serde::Serialize)]
struct TestPlacementAuthorityPermitRecord {
    job_id: String,
    holder_id: String,
    holder_cluster_id: Option<String>,
    holder_node_id: Option<NodeId>,
    holder_session_id: Option<String>,
}

#[test]
fn daemon_dfs_blocking_owner_clone_retains_exclusive_data_lock() {
    let root = temp_root("blocking-owner-lock");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control loop data-owner lock"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner).expect("register data-owner lock");
    let blocking_owner = daemon_dfs_data_owner_lock(&paths)
        .expect("read registered lock")
        .expect("blocking task owner clone");

    drop(control_owner);
    assert!(
        DaemonDataOwnerLock::acquire(&paths).is_err(),
        "a replacement daemon must remain excluded while abandoned blocking DFS work runs"
    );

    drop(blocking_owner);
    DaemonDataOwnerLock::acquire(&paths)
        .expect("data-owner lock is released after blocking work stops");
}

#[test]
fn daemon_dfs_placement_identity_requires_live_main_data_owner_lock() {
    let root = temp_root("placement-identity-owner-lock");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let local_node_id = NodeId::generate();
    write_test_cluster_state(root.path(), &local_node_id);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("placement-identity-owner-lock");
    let unowned_client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());

    assert!(
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&unowned_client)
            .is_none(),
        "a shadow or utility process without the main data lock must not acquire placement authority"
    );

    let owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
    register_daemon_dfs_data_owner_lock(&paths, &owner).expect("register main lock");
    let owned_client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
    let owned_identity =
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&owned_client)
            .expect("owned placement identity");
    assert!(
        sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &owned_client,
            &local_node_id,
            &owned_identity,
        ),
        "the main daemon may acquire placement authority while its exclusive lock is live"
    );
    assert!(
        !sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &owned_client,
            &NodeId::generate(),
            &owned_identity,
        ),
        "host authorization is bound to the local cluster node"
    );
    let forged_identity = sinomo_dfs::DfsPlacementAuthorityProcessIdentity {
        cluster_id: owned_identity.cluster_id.clone(),
        process_session_id: "forged-placement-session".to_string(),
    };
    assert!(
        !sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &owned_client,
            &local_node_id,
            &forged_identity,
        ),
        "host authorization is bound to the current process session"
    );
    drop(owner);
    assert!(
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&owned_client)
            .is_none(),
        "an ordinary cached client must not pin the data-owner lock after its task guard exits"
    );
    let replacement =
        DaemonDataOwnerLock::acquire(&paths).expect("weak cached client released data-owner lock");
    drop(replacement);
    drop(owned_client);

    let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("replacement main owner"));
    register_daemon_dfs_data_owner_lock(&paths, &owner).expect("register replacement lock");
    let retained_client =
        DaemonDfsControlPlaneKvClient::new_owned_on_runtime(&cluster_paths, runtime.handle());
    drop(owner);
    assert!(
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&retained_client)
            .is_some(),
        "an explicitly owned sandbox service retains authority for its full lifetime"
    );
    assert!(DaemonDataOwnerLock::acquire(&paths).is_err());
    drop(retained_client);
    DaemonDataOwnerLock::acquire(&paths).expect("owned sandbox service released lock");
}

#[test]
fn daemon_metadata_authority_uses_one_exact_read_without_snapshot_refresh() {
    let root = temp_root("daemon-metadata-authority-exact-read");
    let data_dir = SinomoDataDir::new(root.path());
    data_dir.ensure_root_layout().expect("data directory");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local_node =
        NodeId::from_legacy_named("node_daemon_metadata_authority_exact_read").expect("local node");
    let shard = DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: local_node.clone(),
        replica_node_ids: Vec::new(),
        epoch: 11,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
        "cluster_daemon_metadata_authority_exact_read",
    )
    .expect("cluster id");
    let now = OffsetDateTime::now_utc();
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon metadata authority exact read",
        local_node.clone(),
        "local",
        "pk-local",
        now,
    );
    state.term = 7;
    let capabilities = sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
    state.members[0].capabilities = capabilities.clone();
    state.members[0].allowed_capabilities = capabilities;
    let config = sinomo_core::kv::ClusterKvRaftConfigRecord {
        cluster_id,
        term: 7,
        kv_revision: 1,
        voters: vec![local_node.clone()],
        learners: Vec::new(),
        non_voting: Vec::new(),
        updated_at: now,
    };
    let mut node = cluster_paths
        .kv_paths()
        .raft_store()
        .open_node(&local_node)
        .expect("open local Raft node");
    node.become_leader(7).expect("establish local leader");
    node.append_local_command(
        7,
        sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
            key: sinomo_core::kv::cluster_kv_raft_config_key(),
            value: sinomo_core::kv::encode_kv_value(&config).expect("encode Raft config"),
            lease_id: None,
        }),
    )
    .expect("append Raft config");
    node.append_local_command(
        7,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                sinomo_dfs::file_system_config_key(&shard.file_system_id),
                &test_file_system_config_for_shard(&shard),
                None,
            )
            .expect("filesystem config op"),
        ),
    )
    .expect("append filesystem config");
    let last = node
        .append_local_command(
            7,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                    &shard,
                    None,
                )
                .expect("metadata shard op"),
            ),
        )
        .expect("append metadata shard config");
    node.commit_to(last.index)
        .expect("commit exact-read fixture");
    state.kv_revision = node.store().revision().0;
    drop(node);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    assert!(
        !cluster_paths.kv_paths().snapshot.exists(),
        "the regression requires authority construction without a materialized checkpoint"
    );
    let runtime = daemon_test_runtime("daemon-metadata-authority-exact-read");
    let metadata_authority_preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &state.cluster_id,
            &state.local_node_id,
            runtime.handle(),
        ))
        .expect("async preflight must establish the retained prefix before sync clients");
    assert!(
        metadata_authority_preflight
            .authority
            .retained_complete_prefix_for_generation(metadata_authority_preflight.generation)
            .is_some()
    );
    let attached =
        daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
            .expect("cluster state must attach authority without a materialized checkpoint");
    let file_system_key = sinomo_dfs::file_system_config_key(&shard.file_system_id);
    assert_eq!(
        attached
            .get(&file_system_key)
            .expect("bounded exact authority through the state-gated factory")
            .expect("resident filesystem config")
            .decode::<DfsFileSystemConfig>()
            .expect("decode resident filesystem config")
            .file_system_id,
        shard.file_system_id,
    );
    let mut client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());

    let started = Instant::now();
    assert!(
        sinomo_dfs::DfsControlPlaneStore::new(&mut client)
            .metadata_shard_authority_is_current(&shard)
            .expect("operation-bound exact shard authority")
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "a local exact-key-set authority fence must not approach the former five-second lock budget"
    );
    assert_eq!(
        client.authoritative_exact_set_calls.load(Ordering::SeqCst),
        1,
        "one validation must issue one exact-key-set ReadIndex"
    );
    assert_eq!(
        client.full_snapshot_refresh_calls.load(Ordering::SeqCst),
        0,
        "exact authority validation must not fetch a full authoritative snapshot first"
    );

    let mut async_client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
    let async_started = Instant::now();
    assert!(
        runtime
            .block_on(
                sinomo_dfs::DfsControlPlaneAsyncStore::new(&mut async_client)
                    .metadata_shard_authority_is_current(&shard),
            )
            .expect("async operation-bound exact shard authority")
    );
    assert!(
        async_started.elapsed() < Duration::from_millis(500),
        "an async local exact-key-set authority fence must not approach the former five-second lock budget"
    );
    assert_eq!(
        async_client
            .authoritative_exact_set_calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        async_client
            .full_snapshot_refresh_calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn daemon_dfs_mutation_scheduler_is_reused_per_data_dir() {
    let first_root = temp_root("mutation-scheduler-first");
    let second_root = temp_root("mutation-scheduler-second");
    let first_data = SinomoDataDir::new(first_root.path());
    let second_data = SinomoDataDir::new(second_root.path());
    let first = daemon_dfs_mutation_scheduler(&first_data).expect("first scheduler");
    let same = daemon_dfs_mutation_scheduler(&first_data).expect("same scheduler");
    let other = daemon_dfs_mutation_scheduler(&second_data).expect("other scheduler");

    assert!(Arc::ptr_eq(&first, &same));
    assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn replica_materialization_scheduler_coalesces_only_the_exact_shard() {
    let root = temp_root("replica-materialization-scheduler");
    let data_dir = SinomoDataDir::new(root.path());
    let file_system_id =
        DfsFileSystemId::from_legacy_named("replica_materialization_scheduler_filesystem")
            .expect("filesystem id");
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let first =
        daemon_dfs_metadata_materialization_lane_scheduler(&data_dir, &file_system_id, &root_shard)
            .expect("first materialization scheduler");
    let same =
        daemon_dfs_metadata_materialization_lane_scheduler(&data_dir, &file_system_id, &root_shard)
            .expect("same materialization scheduler");
    let other = daemon_dfs_metadata_materialization_lane_scheduler(
        &data_dir,
        &file_system_id,
        &services_shard,
    )
    .expect("other materialization scheduler");

    assert!(Arc::ptr_eq(&first, &same));
    assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn allocator_cleanup_yields_to_foreground_mutation_admission() {
    let root = temp_root("allocator-cleanup-foreground-preemption");
    let data_dir = SinomoDataDir::new(root.path());
    let runtime = daemon_test_runtime("allocator-cleanup-foreground-preemption");
    let mutation_scheduler = daemon_dfs_mutation_scheduler(&data_dir).expect("mutation scheduler");
    let trims_before = daemon_dfs_allocator_trim_completion_count(&data_dir);

    schedule_daemon_dfs_allocator_cleanup_on_runtime(
        data_dir.clone(),
        runtime.handle(),
        Arc::clone(&mutation_scheduler),
    );
    runtime.block_on(async {
        let interest_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while mutation_scheduler.maintenance_interest_count() == 0 {
            assert!(
                tokio::time::Instant::now() < interest_deadline,
                "allocator cleanup did not publish foreground interest"
            );
            tokio::task::yield_now().await;
        }

        let foreground = admit_daemon_dfs_foreground_message(
            &mutation_scheduler,
            &DfsMessageExecutionScope::ExactShard {
                file_system_id: DfsFileSystemId::from_legacy_named("allocator_cleanup_filesystem")
                    .expect("filesystem id"),
                shard_id: DfsMetadataShardId::new("root"),
            },
        )
        .await
        .expect("foreground admission");
        tokio::time::sleep(DAEMON_DFS_ALLOCATOR_CLEANUP_QUIET_PERIOD * 3).await;
        assert_eq!(
            daemon_dfs_allocator_trim_completion_count(&data_dir),
            trims_before,
            "allocator cleanup must not trim while foreground admission is held"
        );
        drop(foreground);

        let trim_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while daemon_dfs_allocator_trim_completion_count(&data_dir) == trims_before {
            assert!(
                tokio::time::Instant::now() < trim_deadline,
                "allocator cleanup did not resume after foreground admission"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

#[test]
fn lightweight_dfs_reads_bypass_global_admission() {
    let root = temp_root("lightweight-read-bypasses-mutation-gate");
    let scheduler = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(root.path()))
        .expect("mutation scheduler");
    let runtime = daemon_test_runtime("lightweight-read-bypasses-mutation-gate");
    runtime.block_on(async {
        let global = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("global admission");
        let read_scope = DfsMessageExecutionScope::ReadOnly;

        let read_admission = tokio::time::timeout(
            Duration::from_millis(100),
            admit_daemon_dfs_foreground_message(&scheduler, &read_scope),
        )
        .await
        .expect("lightweight read must not wait for mutation execution")
        .expect("lightweight read admission");
        assert!(matches!(
            read_admission,
            DaemonDfsMutationAdmission::ReadOnly { .. }
        ));

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                admit_daemon_dfs_foreground_message(
                    &scheduler,
                    &DfsMessageExecutionScope::GlobalExclusive,
                ),
            )
            .await
            .is_err(),
            "another global operation must wait for global admission"
        );

        drop(global);
        let hydrating_read = tokio::time::timeout(
            Duration::from_secs(1),
            admit_daemon_dfs_foreground_message(
                &scheduler,
                &DfsMessageExecutionScope::GlobalExclusive,
            ),
        )
        .await
        .expect("global admission after release")
        .expect("global admission");
        assert!(matches!(
            hydrating_read,
            DaemonDfsMutationAdmission::Global { .. }
        ));
    });
}

#[test]
fn daemon_dfs_status_admission_bypasses_global_mutation_fence() {
    let root = temp_root("dfs-status-bypasses-global-mutation-fence");
    let scheduler = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(root.path()))
        .expect("mutation scheduler");
    let runtime = daemon_test_runtime("dfs-status-bypasses-global-mutation-fence");
    runtime.block_on(async {
        let global = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("hold global mutation fence");
        let status = tokio::time::timeout(
            Duration::from_millis(100),
            admit_daemon_dfs_foreground_message(&scheduler, &DfsMessageExecutionScope::ReadOnly),
        )
        .await
        .expect("status admission must not wait for global mutation")
        .expect("status read-only admission");
        assert!(matches!(
            status,
            DaemonDfsMutationAdmission::ReadOnly { .. }
        ));
        drop(global);
    });
}

#[test]
fn foreground_dfs_cancels_a_queued_maintenance_writer() {
    let root = temp_root("foreground-cancels-queued-maintenance-writer");
    let data_dir = SinomoDataDir::new(root.path());
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).expect("mutation scheduler");
    let runtime = daemon_test_runtime("foreground-cancels-queued-maintenance-writer");
    runtime.block_on(async {
        // A replica materializer retains the shared generation fence while
        // it waits for a quiet shard-local replay window.
        let retained_replica = scheduler
            .admit(&daemon_dfs_test_execution_scope(
                "queued-maintenance-fs",
                "root",
            ))
            .await
            .expect("retained replica generation");
        let foreground_preemption = scheduler.begin_maintenance_interest();
        let maintenance_data_dir = data_dir.clone();
        let maintenance = tokio::spawn(async move {
            acquire_daemon_dfs_maintenance_admission_until_foreground_waits(
                &maintenance_data_dir,
                &foreground_preemption,
            )
            .await
            .expect("maintenance admission result")
        });

        // Let maintenance acquire its lifecycle gate and queue the global
        // writer behind the retained replica read fence.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let foreground = tokio::time::timeout(
            Duration::from_secs(1),
            admit_daemon_dfs_foreground_message(
                &scheduler,
                &daemon_dfs_test_execution_scope("queued-maintenance-fs", "services"),
            ),
        )
        .await
        .expect("foreground read must cancel the queued maintenance writer")
        .expect("foreground admission");
        assert!(
            maintenance
                .await
                .expect("maintenance admission task")
                .is_none(),
            "pre-admission maintenance must defer after foreground interest"
        );
        drop(foreground);
        drop(retained_replica);
    });
}

#[test]
fn foreground_dfs_preempts_pending_maintenance_before_admission_release() {
    #[derive(Debug)]
    struct MaintenanceGenerationDrop(Arc<AtomicBool>);

    impl Drop for MaintenanceGenerationDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let runtime = daemon_test_runtime("foreground-preempts-pending-maintenance");
    runtime.block_on(async {
        let maintenance_interest = scheduler.begin_maintenance_interest();
        let maintenance_admission = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("maintenance admission");
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
        let generation_dropped = Arc::new(AtomicBool::new(false));
        let maintenance_scheduler = Arc::clone(&scheduler);
        let pending_maintenance_scheduler = Arc::clone(&scheduler);
        let maintenance_generation_dropped = Arc::clone(&generation_dropped);
        let observed_generation_dropped = Arc::clone(&generation_dropped);
        let maintenance_task = tokio::spawn(async move {
            let outcome = run_daemon_dfs_maintenance_until_foreground_waits(
                &maintenance_interest,
                async move {
                    let _generation = MaintenanceGenerationDrop(maintenance_generation_dropped);
                    assert!(
                        pending_maintenance_scheduler.global_is_exclusively_held(),
                        "maintenance local work must retain exclusive mutation admission"
                    );
                    pending_tx
                        .send(())
                        .expect("report outbound maintenance wait");
                    std::future::pending::<()>().await;
                },
            )
            .await;
            assert!(matches!(
                outcome,
                DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
            ));
            assert!(
                observed_generation_dropped.load(Ordering::Acquire),
                "the maintenance service generation must be destroyed before admission returns"
            );
            assert!(
                maintenance_scheduler.global_is_exclusively_held(),
                "preemption must not release admission before dropping maintenance state"
            );
            drop(maintenance_admission);
        });

        pending_rx.await.expect("maintenance reached outbound wait");
        let foreground_scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
        let foreground_admission = tokio::time::timeout(
            Duration::from_secs(1),
            admit_daemon_dfs_foreground_message(&scheduler, &foreground_scope),
        )
        .await
        .expect("foreground admission must preempt pending maintenance")
        .expect("foreground admission");
        maintenance_task.await.expect("maintenance preemption task");
        assert!(generation_dropped.load(Ordering::Acquire));
        drop(foreground_admission);
    });
}

#[test]
fn caught_up_metadata_prefix_probe_waits_without_preempting_maintenance() {
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let runtime = daemon_test_runtime("metadata-prefix-probe-does-not-preempt-maintenance");
    runtime.block_on(async {
        let maintenance_interest = scheduler.begin_maintenance_interest();
        let maintenance_admission = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("maintenance admission");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("prefix-probe-fs").expect("filesystem id");
        let shard_id = DfsMetadataShardId::new("root");
        let primary_node_id =
            NodeId::from_legacy_named("prefix-probe-primary").expect("primary node id");
        let local_node_id =
            NodeId::from_legacy_named("prefix-probe-replica").expect("replica node id");
        let barrier_id = "prefix-probe-barrier";
        let probe = DfsMetadataJournalBatchCommand {
            request_id: format!(
                "dfs_meta_probe_{}_{}_{}",
                primary_node_id, local_node_id, barrier_id
            ),
            source_node_id: Some(primary_node_id.clone()),
            file_system_id: Some(file_system_id.clone()),
            authority_epoch: Some(7),
            verify_prefix_only: true,
            prefix_probe: Some(sinomo_dfs::DfsMetadataPrefixProbe {
                barrier_id: barrier_id.to_string(),
                file_system_id: Some(file_system_id.clone()),
                authority_epoch: Some(7),
                shard_id: shard_id.clone(),
                journal_sequence: 0,
                state_fingerprint: "blake3:prefix-probe-state".to_string(),
                primary_handoff_id: None,
            }),
            authority_probe: None,
            durable_replication: None,
            batch: sinomo_dfs::DfsMetadataJournalBatch {
                shard_id,
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        };
        assert!(daemon_dfs_metadata_prefix_probe_is_verification_only(
            &probe,
            &primary_node_id,
            &local_node_id,
        ));
        let scope = daemon_dfs_test_execution_scope("prefix-probe-fs", "root");
        {
            let background_admission = admit_daemon_dfs_message(
                &scheduler,
                &scope,
                !daemon_dfs_metadata_prefix_probe_is_verification_only(
                    &probe,
                    &primary_node_id,
                    &local_node_id,
                ),
            );
            tokio::pin!(background_admission);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut background_admission)
                    .await
                    .is_err(),
                "verification must queue behind the admitted maintenance pass"
            );
            assert!(
                !maintenance_interest.is_requested(),
                "verification-only metadata probes must not cancel repair maintenance"
            );
        }

        let mut sync_probe = probe.clone();
        sync_probe.request_id = format!("dfs_sync_probe_{barrier_id}");
        assert!(!daemon_dfs_metadata_prefix_probe_is_verification_only(
            &sync_probe,
            &primary_node_id,
            &local_node_id,
        ));
        let mut malformed_probe = probe.clone();
        malformed_probe
            .prefix_probe
            .as_mut()
            .expect("prefix probe")
            .journal_sequence += 1;
        assert!(!daemon_dfs_metadata_prefix_probe_is_verification_only(
            &malformed_probe,
            &primary_node_id,
            &local_node_id,
        ));
        let mut handoff_probe = probe;
        handoff_probe
            .prefix_probe
            .as_mut()
            .expect("prefix probe")
            .primary_handoff_id = Some("handoff-id".to_string());
        assert!(!daemon_dfs_metadata_prefix_probe_is_verification_only(
            &handoff_probe,
            &primary_node_id,
            &local_node_id,
        ));
        {
            let foreground_admission = admit_daemon_dfs_message(
                &scheduler,
                &scope,
                !daemon_dfs_metadata_prefix_probe_is_verification_only(
                    &handoff_probe,
                    &primary_node_id,
                    &local_node_id,
                ),
            );
            tokio::pin!(foreground_admission);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut foreground_admission)
                    .await
                    .is_err(),
                "mutation must still wait for maintenance admission to release"
            );
            assert!(
                maintenance_interest.is_requested(),
                "handoff prefix probes must retain foreground preemption"
            );
        }
        drop(maintenance_admission);
    });
}

#[test]
fn simultaneous_two_node_maintenance_rpc_waits_make_foreground_progress() {
    let first_root = temp_root("simultaneous-maintenance-first-node");
    let second_root = temp_root("simultaneous-maintenance-second-node");
    let first_scheduler = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(first_root.path()))
        .expect("first scheduler");
    let second_scheduler = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(second_root.path()))
        .expect("second scheduler");
    let runtime = daemon_test_runtime("simultaneous-two-node-maintenance-rpc-waits");
    runtime.block_on(async {
        let first_interest = first_scheduler.begin_maintenance_interest();
        let second_interest = second_scheduler.begin_maintenance_interest();
        let first_admission = first_scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("first maintenance admission");
        let second_admission = second_scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("second maintenance admission");
        let (first_waiting_tx, first_waiting_rx) = tokio::sync::oneshot::channel();
        let (second_waiting_tx, second_waiting_rx) = tokio::sync::oneshot::channel();

        let first_maintenance = tokio::spawn(async move {
            let outcome =
                run_daemon_dfs_maintenance_until_foreground_waits(&first_interest, async move {
                    first_waiting_tx.send(()).expect("first waiting signal");
                    std::future::pending::<()>().await;
                })
                .await;
            drop(first_admission);
            outcome
        });
        let second_maintenance = tokio::spawn(async move {
            let outcome =
                run_daemon_dfs_maintenance_until_foreground_waits(&second_interest, async move {
                    second_waiting_tx.send(()).expect("second waiting signal");
                    std::future::pending::<()>().await;
                })
                .await;
            drop(second_admission);
            outcome
        });

        // Both nodes now hold local maintenance admission while awaiting
        // an outbound peer response. Simultaneous reciprocal inbound RPCs
        // must request foreground admission and cancel both replay-safe
        // maintenance futures before either network timeout is needed.
        first_waiting_rx.await.expect("first outbound wait");
        second_waiting_rx.await.expect("second outbound wait");
        let first_scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
        let second_scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
        let (first_foreground, second_foreground) =
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(
                    admit_daemon_dfs_foreground_message(&first_scheduler, &first_scope),
                    admit_daemon_dfs_foreground_message(&second_scheduler, &second_scope,),
                )
            })
            .await
            .expect("reciprocal inbound RPCs must make progress without timeout");
        let first_foreground = first_foreground.expect("first foreground admission");
        let second_foreground = second_foreground.expect("second foreground admission");
        assert!(matches!(
            first_maintenance.await.expect("first maintenance task"),
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        assert!(matches!(
            second_maintenance.await.expect("second maintenance task"),
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        drop(first_foreground);
        drop(second_foreground);
    });
}

#[test]
fn mandatory_default_dfs_lifecycle_is_single_flight_per_data_dir() {
    let first_root = temp_root("lifecycle-single-flight-first");
    let second_root = temp_root("lifecycle-single-flight-second");
    let first_gate = daemon_default_dfs_lifecycle_gate(first_root.path()).expect("first gate");
    let same_gate = daemon_default_dfs_lifecycle_gate(first_root.path()).expect("same gate");
    let other_gate = daemon_default_dfs_lifecycle_gate(second_root.path()).expect("other gate");

    assert!(Arc::ptr_eq(&first_gate, &same_gate));
    assert!(!Arc::ptr_eq(&first_gate, &other_gate));

    let first = first_gate.clone().try_lock_owned().expect("first pass");
    assert!(
        same_gate.try_lock_owned().is_err(),
        "concurrent reconciliation must coalesce while a pass owns the gate"
    );
    assert!(
        other_gate.try_lock_owned().is_ok(),
        "independent data directories must not block each other"
    );
    drop(first);
    assert!(first_gate.try_lock_owned().is_ok());
}

#[test]
fn metadata_publish_schedulers_are_independent_per_shard() {
    let root = temp_root("metadata-publish-shard-lanes");
    let data_dir = SinomoDataDir::new(root.path());
    let file_system_id =
        DfsFileSystemId::from_legacy_named("metadata-publish-lane-fs").expect("filesystem id");
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let first = daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &root_shard)
        .expect("root publish lane");
    let same = daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &root_shard)
        .expect("same root publish lane");
    let other =
        daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &services_shard)
            .expect("services publish lane");

    assert!(Arc::ptr_eq(&first, &same));
    assert!(
        !Arc::ptr_eq(&first, &other),
        "a blocked replica for one shard must not occupy another shard's reactor lane"
    );
}

#[test]
fn metadata_publish_planning_does_not_hold_admission_while_shard_is_borrowed() {
    let root = temp_root("metadata-publish-admission-resident-lease-order");
    let local =
        NodeId::from_legacy_named("metadata-publish-lease-order-local").expect("local node");
    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard_config = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local,
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let resident = sinomo_dfs::LocalShardedMetadataBackend::new(
        root.path().join("metadata"),
        vec![shard_config],
    )
    .expect("resident metadata backend")
    .into_resident();
    let shard = resident
        .scope_shard(&file_system.file_system_id, &shard_id)
        .expect("resident root shard");
    let held_lease = shard.checkout().expect("hold resident root shard");

    let runtime = daemon_test_runtime("metadata-publish-admission-resident-lease-order");
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id,
        shard_id,
    };
    runtime.block_on(async {
        let planning_shard = shard.clone();
        let planning_scheduler = Arc::clone(&scheduler);
        let attempts = Arc::new(AtomicUsize::new(0));
        let planning_attempts = Arc::clone(&attempts);
        let (planning_started_tx, planning_started_rx) = tokio::sync::oneshot::channel();
        let mut planning_started_tx = Some(planning_started_tx);
        let publisher = tokio::spawn(async move {
            run_daemon_dfs_metadata_publish_lane_planning_with_admission(
                planning_scheduler,
                lane,
                move || {
                    planning_attempts.fetch_add(1, Ordering::SeqCst);
                    if let Some(started) = planning_started_tx.take() {
                        let _ = started.send(());
                    }
                    match planning_shard
                        .try_checkout()
                        .map_err(|error| error.to_string())?
                    {
                        Some(_lease) => Ok(DaemonDfsMetadataPublishPlanningAttempt::Complete(())),
                        None => Ok(
                            DaemonDfsMetadataPublishPlanningAttempt::WaitForResidentShard(
                                planning_shard.clone(),
                            ),
                        ),
                    }
                },
            )
            .await
        });

        // This signal is sent from inside the blocking planner while its
        // exact-shard admission guard is alive. The global waiter owns the
        // conflicting resident lease, reproducing the former cycle:
        // exact admission -> lease, lease -> global admission.
        planning_started_rx
            .await
            .expect("publisher entered exact-shard planning");
        let global_scheduler = Arc::clone(&scheduler);
        let global = tokio::spawn(async move {
            let admission = global_scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await?;
            drop(held_lease);
            drop(admission);
            Ok::<_, DaemonError>(())
        });

        let (global, publisher) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(global, publisher)
        })
        .await
        .expect("resident lease and mutation admission must not form a cycle");
        global
            .expect("global waiter task")
            .expect("global waiter admission");
        publisher
            .expect("publisher task")
            .expect("publisher planning");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "publisher must retry once after the resident owner releases its lease"
        );

        drop(
            shard
                .try_checkout()
                .expect("post-cycle checkout")
                .expect("publisher must not leak the resident shard lease"),
        );
        let global = tokio::time::timeout(
            Duration::from_secs(1),
            scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .expect("publisher must not leak exact-shard admission")
        .expect("post-cycle global admission");
        drop(global);
    });
}

#[test]
fn metadata_publish_admission_fences_planning_but_not_blocked_transport() {
    struct ExecutionGuardDrop(Arc<AtomicBool>);

    impl Drop for ExecutionGuardDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let runtime = daemon_test_runtime("metadata-publish-foreground-admission");
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let file_system_id =
        DfsFileSystemId::from_legacy_named("metadata-publish-admission-fs").expect("filesystem id");
    let root_lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let services_lane = DaemonDfsMutationLaneKey {
        file_system_id,
        shard_id: DfsMetadataShardId::new("services"),
    };

    runtime.block_on(async {
            let foreground_scope = DfsMessageExecutionScope::ExactShard {
                file_system_id: root_lane.file_system_id.clone(),
                shard_id: root_lane.shard_id.clone(),
            };
            let same_scheduler = Arc::clone(&scheduler);
            let same_lane = root_lane.clone();
            let execution_guard_dropped = Arc::new(AtomicBool::new(false));
            let publisher_guard = ExecutionGuardDrop(Arc::clone(&execution_guard_dropped));
            let (refresh_started_tx, refresh_started_rx) = tokio::sync::oneshot::channel();
            let (release_refresh_tx, release_refresh_rx) = tokio::sync::oneshot::channel();
            let (planned_tx, mut planned_rx) = tokio::sync::oneshot::channel();
            let mut planned_tx = Some(planned_tx);
            let (transport_started_tx, transport_started_rx) = tokio::sync::oneshot::channel();
            let (release_transport_tx, release_transport_rx) = tokio::sync::oneshot::channel();
            let publisher = tokio::spawn(async move {
                let _ = refresh_started_tx.send(());
                release_refresh_rx
                    .await
                    .map_err(|_| "blocked KV refresh release was dropped".to_string())?;
                run_daemon_dfs_metadata_publish_lane_planning_with_admission(
                    same_scheduler,
                    same_lane,
                    move || {
                        if let Some(planned) = planned_tx.take() {
                            let _ = planned.send(());
                        }
                        Ok(DaemonDfsMetadataPublishPlanningAttempt::Complete(()))
                    },
                )
                .await?;
                run_daemon_dfs_metadata_publish_blocking_execution(
                    tokio::runtime::Handle::current(),
                    publisher_guard,
                    async move {
                        let _ = transport_started_tx.send(());
                        release_transport_rx
                            .await
                            .map_err(|_| "blocked transport release was dropped".to_string())
                    },
                )
                .await
            });
            refresh_started_rx
                .await
                .expect("publisher must enter its KV refresh phase");
            let foreground = tokio::time::timeout(
                Duration::from_secs(2),
                scheduler.admit(&foreground_scope),
            )
            .await
            .expect("blocked KV refresh must not retain same-shard admission")
            .expect("foreground root admission");
            release_refresh_tx.send(()).expect("release blocked KV refresh");
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut planned_rx)
                    .await
                    .is_err(),
                "same-shard publisher planning must wait until the foreground transaction releases admission"
            );

            tokio::time::timeout(
                Duration::from_secs(2),
                run_daemon_dfs_metadata_publish_lane_planning_with_admission(
                    Arc::clone(&scheduler),
                    services_lane,
                    || Ok(DaemonDfsMetadataPublishPlanningAttempt::Complete(())),
                ),
            )
            .await
            .expect("disjoint-shard publisher must remain concurrent")
            .expect("disjoint-shard publish");

            drop(foreground);
            tokio::time::timeout(Duration::from_secs(2), &mut planned_rx)
                .await
                .expect("same-shard publisher must resume after foreground completion")
                .expect("same-shard planner signal");
            transport_started_rx
                .await
                .expect("publisher must enter its transport phase");

            let later_foreground = tokio::time::timeout(
                Duration::from_secs(2),
                scheduler.admit(&foreground_scope),
            )
            .await
            .expect("blocked transport must not retain same-shard admission")
            .expect("later foreground root admission");
            drop(later_foreground);

            publisher.abort();
            assert!(
                publisher.await.expect_err("publisher task should be cancelled").is_cancelled(),
                "test must cancel the scheduler future while blocking execution continues"
            );
            assert!(
                !execution_guard_dropped.load(Ordering::SeqCst),
                "scheduler cancellation must not drop the execution capability while transport is active"
            );
            release_transport_tx
                .send(())
                .expect("release blocked transport");
            tokio::time::timeout(Duration::from_secs(2), async {
                while !execution_guard_dropped.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("execution capability must release after transport completion");
        });
}

#[test]
fn metadata_publish_discovery_uses_applied_topology_ahead_of_materialized_snapshot() {
    let root = temp_root("metadata-publish-latest-observed-topology");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_metadata_publish_promoted_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_metadata_publish_retiring_remote").expect("remote node");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "10.79.0.1:21353");
    let mut authority_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("read authority fixture state");
    let local_member = authority_state
        .members
        .iter_mut()
        .find(|member| member.node_id == local)
        .expect("local authority member");
    local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
    local_member.allowed_capabilities = local_member.capabilities.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &authority_state)
        .expect("write authority-capable fixture state");
    let stale_materialized = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
        .expect("pre-promotion snapshot");
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote],
        epoch: 2,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "services"),
                &shard,
                None,
            )
            .expect("promoted shard op"),
        ),
    )
    .expect("commit promoted shard");

    // Model the short production window where the local Raft state has
    // applied the promotion but the derived materialized snapshot still
    // describes the retiring topology.
    sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &stale_materialized)
        .expect("restore stale materialized snapshot");
    assert!(
        sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            "/dfs/filesystems/",
        )
        .expect("stale materialized topology")
        .is_empty(),
        "the regression requires the materialized projection to trail Raft"
    );

    let runtime = daemon_test_runtime_with_thread_stack_size(
        "metadata-publish-prefix-authority",
        2 * 1024 * 1024,
    );
    let _ = take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths);
    let cold_cluster_paths = cluster_paths.clone();
    let cold_runtime = runtime.handle().clone();
    let cold_cluster_id = authority_state.cluster_id.clone();
    let cold_local_node_id = local.clone();
    let cold_preflight = runtime
        .block_on(runtime.spawn(async move {
            warm_daemon_dfs_metadata_authority_for_identity_async(
                &cold_cluster_paths,
                &cold_cluster_id,
                &cold_local_node_id,
                &cold_runtime,
            )
            .await
        }))
        .expect("2 MiB cold authority worker")
        .expect("cold bounded metadata-authority prefix proof");
    assert!(
        cold_preflight
            .authority
            .retained_complete_prefix_revision_for_generation(cold_preflight.generation)
            .is_some(),
        "cold authority warmup must install a complete retained prefix proof"
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        1,
        "cold authority warmup must use one bounded prefix ReadIndex on a 2 MiB worker"
    );

    let client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
    let prefix = format!(
        "{}/",
        sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
    );
    let authoritative = sinomo_dfs::DfsControlPlaneKvClient::range_prefix(&client, &prefix)
        .expect("bounded resident prefix authority");
    assert!(
        authoritative.iter().any(|entry| {
            entry
                .decode::<DfsMetadataShardConfig>()
                .is_ok_and(|observed| observed == shard)
        }),
        "the daemon authority projection must expose topology newer than the materialized checkpoint"
    );
    assert_eq!(
        sinomo_dfs::DfsControlPlaneKvClient::range_prefix(&client, &prefix)
            .expect("repeat cached authoritative prefix"),
        authoritative,
        "a validated prefix must remain a compact per-client projection"
    );
    assert_eq!(
        client.full_snapshot_refresh_calls.load(Ordering::SeqCst),
        0,
        "bounded prefix authority must not fetch a complete snapshot"
    );

    let async_client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
    let async_authoritative = runtime
        .block_on(
            sinomo_dfs::AsyncDfsControlPlaneKvClient::range_prefix_async(&async_client, &prefix),
        )
        .expect("async bounded resident prefix authority");
    assert_eq!(async_authoritative, authoritative);
    assert_eq!(
        async_client
            .full_snapshot_refresh_calls
            .load(Ordering::SeqCst),
        0,
    );
    let _ = take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths);

    let discovery_cluster_paths = cluster_paths.clone();
    let discovery_runtime = runtime.handle().clone();
    let discovery = runtime.spawn(async move {
        daemon_dfs_metadata_publish_discovery_lanes_boxed_async(
            &data_dir,
            &discovery_cluster_paths,
            &discovery_runtime,
        )
        .await
    });
    assert_eq!(
        runtime
            .block_on(discovery)
            .expect("2 MiB metadata discovery worker")
            .expect("discover promoted primary lane"),
        vec![DaemonDfsMutationLaneKey {
            file_system_id: file_system.file_system_id,
            shard_id: DfsMetadataShardId::new("services"),
        }],
        "promotion discovery must use applied authority rather than the stale materialized projection"
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
        "the retained cold proof must prevent a second discovery ReadIndex"
    );
}

#[test]
fn committed_work_emitter_is_quiet_for_noop_and_retains_publish_edge() {
    let root = temp_root("committed-work-publish-edge");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("committed-work-publish-edge");
    let initial = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    let initial_storage_generation = daemon_dfs_storage_environment_dirty_generation(&data_dir);

    emit_daemon_dfs_committed_edges(
        &data_dir,
        &cluster_paths,
        &DfsMessageExecutionScope::GlobalExclusive,
        runtime.handle(),
        None,
        DfsMessageCommittedWork::default(),
        &[],
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()),
        initial,
        "an empty committed-work receipt must not wake discovery"
    );
    assert_eq!(
        daemon_dfs_storage_environment_dirty_generation(&data_dir),
        initial_storage_generation,
        "an empty receipt must not claim that storage changed"
    );

    emit_daemon_dfs_committed_edges(
        &data_dir,
        &cluster_paths,
        &DfsMessageExecutionScope::GlobalExclusive,
        runtime.handle(),
        None,
        DfsMessageCommittedWork {
            publish_metadata: true,
            ..DfsMessageCommittedWork::default()
        },
        &[],
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 == initial.0
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 > initial.0,
        "a committed metadata edge must remain owned even when discovery cannot yet read cluster topology"
    );
    assert!(
        daemon_dfs_storage_environment_dirty_generation(&data_dir) > initial_storage_generation,
        "a committed mutation must retain one capacity-resample edge"
    );
}

#[test]
fn metadata_publish_discovery_retains_global_edge_across_planning_failure() {
    let root = temp_root("metadata-publish-discovery-retry");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let paths = DaemonPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");
    let runtime = daemon_test_runtime("metadata-publish-discovery-retry");

    assert_eq!(
        schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
            data_dir.clone(),
            cluster_paths.clone(),
            runtime.handle(),
            None,
            None,
        ),
        DfsMetadataPublishScheduleStart::Started,
        "the discovery lane must be retained before cluster planning succeeds"
    );
    let first_deadline = Instant::now() + Duration::from_secs(2);
    while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 == 0
        && Instant::now() < first_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let (failed_attempts, successes) =
        daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    assert!(failed_attempts > 0);
    assert_eq!(successes, 0, "cluster state is intentionally absent");

    let local = NodeId::from_legacy_named("node_dfs_discovery_retry").expect("node");
    write_test_cluster_state(&data_root, &local);
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("metadata shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("install recovered topology");

    let recovery_deadline = Instant::now() + Duration::from_secs(5);
    while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).1 == 0
        && Instant::now() < recovery_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut settled = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    assert!(settled.0 >= 2, "the retained edge must retry planning");
    assert!(
        settled.1 >= 1,
        "topology recovery alone must drain the retained discovery edge"
    );

    // Installing topology can produce another legitimate discovery edge:
    // the exact lane opened by the retained attempt attaches its resident
    // authority observer, which then publishes the newly observed semantic
    // authority generation. Do not require an exact success count. Require
    // the coalesced lane to become quiet after those bounded wakes instead,
    // proving recovery does not turn the observer edge into a retry loop.
    let quiet_window = Duration::from_millis(1_250);
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    let mut unchanged_since = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(10));
        let current = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
        if current != settled {
            settled = current;
            unchanged_since = Instant::now();
        }
        if unchanged_since.elapsed() >= quiet_window {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "metadata discovery did not drain after topology recovery; last counts: {settled:?}"
        );
    }
}

#[test]
fn metadata_publish_missing_route_wakes_on_semantic_route_generation() {
    let root = temp_root("metadata-publish-route-generation");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");
    let runtime = daemon_test_runtime("metadata-publish-route-generation");
    let file_system_id =
        DfsFileSystemId::from_legacy_named("metadata-route-generation-fs").expect("filesystem id");
    let shard_id = DfsMetadataShardId::new("services");
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let scheduler =
        daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &shard_id)
            .expect("publish lane");
    let attempts = Arc::new(AtomicUsize::new(0));
    let route_available = Arc::new(AtomicBool::new(false));
    let publish_attempts = Arc::clone(&attempts);
    let publish_route_available = Arc::clone(&route_available);
    let target =
        NodeId::from_legacy_named("node_metadata_route_generation_target").expect("target node");
    let publish_shard = shard_id.clone();
    assert_eq!(
        sinomo_dfs::schedule_dfs_metadata_publish_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 1,
                retry_delay: Duration::from_secs(60 * 60),
                max_retry_delay: Duration::from_secs(60 * 60),
            },
            move || {
                let attempts = Arc::clone(&publish_attempts);
                let route_available = Arc::clone(&publish_route_available);
                let target = target.clone();
                let shard_id = publish_shard.clone();
                async move {
                    attempts.fetch_add(1, Ordering::AcqRel);
                    let outcomes = if route_available.load(Ordering::Acquire) {
                        Vec::new()
                    } else {
                        vec![DfsMetadataJournalReplicationOutcome::MissingRoute {
                            target_node_id: target,
                            shard_id,
                            after_sequence: 0,
                        }]
                    };
                    Ok::<_, String>(DfsMetadataJournalReplicationReport { outcomes })
                }
            },
            |_| {},
            |error| panic!("unexpected publish error: {error}"),
        ),
        DfsMetadataPublishScheduleStart::Started
    );

    let first_deadline = Instant::now() + Duration::from_secs(2);
    while attempts.load(Ordering::Acquire) == 0 && Instant::now() < first_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        attempts.load(Ordering::Acquire),
        1,
        "the safety reprobe must remain sparse while the route is unavailable"
    );

    route_available.store(true, Ordering::Release);
    assert_eq!(
        notify_daemon_dfs_metadata_publish_route_generation_on_runtime(&paths, runtime.handle(),)
            .expect("route generation wake"),
        1,
        "only the retained exact lane should be re-dirtied"
    );
    let recovery_deadline = Instant::now() + Duration::from_secs(2);
    while attempts.load(Ordering::Acquire) < 2 && Instant::now() < recovery_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        attempts.load(Ordering::Acquire),
        2,
        "route recovery must wake immediately without another DFS mutation"
    );

    // Keep the key in scope to make the test's exact-lane identity clear.
    assert_eq!(lane.shard_id, shard_id);
}

#[test]
fn daemon_dfs_maintenance_admission_is_node_local_and_per_data_root() {
    let first_root = temp_root("daemon-dfs-maintenance-admission-first");
    let second_root = temp_root("daemon-dfs-maintenance-admission-second");
    let first_data_dir = SinomoDataDir::new(first_root.path());
    let second_data_dir = SinomoDataDir::new(second_root.path());
    let runtime = daemon_test_runtime("daemon-dfs-maintenance-admission-node-local");
    runtime.block_on(async {
        let first_admission = acquire_daemon_dfs_maintenance_admission(&first_data_dir)
            .await
            .expect("first data root admission");

        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                acquire_daemon_dfs_maintenance_admission(&first_data_dir),
            )
            .await
            .is_err(),
            "one data root must serialize its lifecycle and metadata mutation generation"
        );

        let second_admission = tokio::time::timeout(
            Duration::from_secs(1),
            acquire_daemon_dfs_maintenance_admission(&second_data_dir),
        )
        .await
        .expect("a different node-local data root must not wait for unrelated maintenance")
        .expect("second data root admission");

        drop(second_admission);
        drop(first_admission);
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire_daemon_dfs_maintenance_admission(&first_data_dir),
        )
        .await
        .expect("released data-root admission must become available")
        .expect("reacquire first data root");
    });
}
#[test]
fn mandatory_default_dfs_lifecycle_guard_survives_dropped_blocking_join() {
    let root = temp_root("lifecycle-cancel-safety");
    let gate = daemon_default_dfs_lifecycle_gate(root.path()).expect("lifecycle gate");
    let guard = gate.clone().try_lock_owned().expect("lifecycle pass");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let task = runtime.spawn_blocking(move || {
        let lifecycle_guard = guard;
        started_tx.send(()).expect("signal lifecycle start");
        release_rx.recv().expect("release lifecycle task");
        drop(lifecycle_guard);
        completed_tx.send(()).expect("signal lifecycle completion");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocking lifecycle task should start");
    drop(task);
    assert!(
        gate.clone().try_lock_owned().is_err(),
        "dropping a blocking JoinHandle must not release its owned lifecycle guard"
    );

    release_tx.send(()).expect("release lifecycle task");
    completed_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocking lifecycle task should complete");
    assert!(gate.try_lock_owned().is_ok());
    runtime.shutdown_timeout(Duration::from_secs(1));
}

fn test_dfs_lookup_routed_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
    path: &str,
) -> DfsRoutedMessage {
    let message = DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
        request_id: format!("daemon_dfs_lookup_{frame_id}"),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        requesting_node_id: source_node_id.clone(),
        include_metadata_journal_batches: false,
        metadata_journal_after_sequences: Vec::new(),
        authority_shards: Vec::new(),
        operation: DfsNamespaceOperation::LookupPath {
            path: path.to_string(),
            inline_range: None,
        },
    });
    DfsRoutedMessage {
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        frame_id,
        message_id: frame_id,
        message,
    }
}

fn test_dfs_exact_read_routed_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
) -> DfsRoutedMessage {
    DfsRoutedMessage {
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        frame_id,
        message_id: frame_id,
        message: DfsNetworkMessage::ReadRequest(sinomo_dfs::DfsReadRequest {
            request_id: format!("daemon_dfs_exact_read_{frame_id}"),
            object_id: DfsObjectId::from_legacy_named(format!(
                "obj_daemon_dfs_exact_read_{frame_id}"
            ))
            .expect("object id"),
            version: Some(
                DfsVersion::from_legacy_named(format!("version_daemon_dfs_exact_read_{frame_id}"))
                    .expect("version"),
            ),
            content_hash: Some(format!("blake3:missing-exact-read-{frame_id}")),
            content_size_bytes: Some(12),
            range_offset: None,
            range_max_bytes: None,
        }),
    }
}

fn test_dfs_write_routed_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
    path: &str,
) -> DfsRoutedMessage {
    let object_id = DfsObjectId::from_legacy_named(format!("obj_daemon_dfs_write_{frame_id}"))
        .expect("object id");
    let base_version =
        DfsVersion::from_legacy_named(format!("version_daemon_dfs_write_base_{frame_id}"))
            .expect("base version");
    let target_version =
        DfsVersion::from_legacy_named(format!("version_daemon_dfs_write_target_{frame_id}"))
            .expect("target version");
    let bytes = format!("daemon DFS write message {frame_id}").into_bytes();
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        path: path.to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: target_node_id.clone(),
        current_locations: vec![source_node_id.clone()],
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
        version: base_version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
        tombstone_provenance: None,
    };
    let message = DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
        publication: Default::default(),
        request_id: format!("daemon_dfs_write_{frame_id}"),
        request: DfsWriteRequest {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            object_id,
            target_version,
            requesting_node_id: source_node_id.clone(),
        },
        expected_base_version: Some(base_version),
        metadata_journal_after_sequence: None,
        bytes,
        content_hash: None,
        content_size_bytes: None,
        requester_blob_pre_staged: true,
        desired_replica_nodes: vec![source_node_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
        object_metadata: Some(object),
        authority_shard: None,
        touch_parent_on_create: false,
        exclusive_create: false,
        foreground_replica_repair: false,
    });
    DfsRoutedMessage {
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        frame_id,
        message_id: frame_id,
        message,
    }
}

fn test_dfs_message_command(
    request_id: impl Into<String>,
    routed_message: DfsRoutedMessage,
) -> DfsMessageCommand {
    DfsMessageCommand {
        request_id: request_id.into(),
        routed_message,
    }
}

fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: sinomo_core::kv::KvCommand,
) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::network::NetworkError> {
    sinomo_core::cluster::append_cluster_raft_command(paths, node_id, command)
}

fn install_test_metadata_replica_shard(
    cluster_paths: &ClusterPaths,
    cluster_name: &str,
    local_node_id: &NodeId,
    shard: &DfsMetadataShardConfig,
) {
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        sinomo_core::node::ClusterId::from_legacy_named(cluster_name).expect("cluster"),
        cluster_name,
        local_node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(cluster_paths, &state)
        .expect("seed canonical cluster authority");
    append_cluster_raft_command(
        cluster_paths,
        local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                sinomo_dfs::file_system_config_key(&shard.file_system_id),
                &test_file_system_config_for_shard(shard),
                None,
            )
            .expect("filesystem config op"),
        ),
    )
    .expect("write filesystem config");
    append_cluster_raft_command(
        cluster_paths,
        local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                shard,
                None,
            )
            .expect("metadata shard op"),
        ),
    )
    .expect("write metadata shard config");
}

fn test_file_system_config_for_shard(shard: &DfsMetadataShardConfig) -> DfsFileSystemConfig {
    DfsFileSystemConfig {
        file_history: Default::default(),
        file_system_id: shard.file_system_id.clone(),
        display_name: format!("{} test filesystem", shard.file_system_id),
        owner_node_id: shard.primary_node_id.clone(),
        subtree_policies: Vec::new(),
    }
}

fn daemon_test_runtime(name: &str) -> tokio::runtime::Runtime {
    build_daemon_async_runtime(name).expect("test DFS runtime")
}

fn daemon_test_runtime_with_thread_stack_size(
    name: &str,
    thread_stack_size: usize,
) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(format!("sinomo-{name}-async"))
        .thread_stack_size(thread_stack_size)
        .enable_all()
        .build()
        .expect("test daemon runtime with explicit thread stack size")
}

#[test]
fn daemon_streams_owned_dfs_attachment_into_unbound_quarantine() {
    let root = temp_root("streamed-primary-content-ingest");
    let data_dir = SinomoDataDir::new(root.path());
    let source = NodeId::from_legacy_named("node_streamed_ingest_source").expect("source");
    let target = NodeId::from_legacy_named("node_streamed_ingest_target").expect("target");
    let mut routed =
        test_dfs_write_routed_message(&source, &target, 71_001, "/services/streamed.bin");
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
        panic!("distributed write request");
    };
    let bytes = std::mem::take(&mut write.bytes);
    let object_id = write.request.object_id.clone();
    let version = write.request.target_version.clone();
    write.requester_blob_pre_staged = false;
    let command = test_dfs_message_command("streamed-primary-content-ingest", routed);
    let (body_sender, body) =
        sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(bytes.len() as u64), 1);
    let attachment = DfsInboundAttachment {
        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: None,
        },
        body,
    };
    let runtime = daemon_test_runtime("daemon-dfs-streamed-primary-content-ingest");
    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("shared content store");
    let streamed_bytes = bytes.clone();
    let producer = runtime.spawn(async move {
        for chunk in streamed_bytes.chunks(8 * 1024) {
            body_sender
                .send(chunk.to_vec())
                .await
                .expect("stream attachment chunk");
        }
    });

    let (command, preingested_blob) = runtime
        .block_on(ingest_dfs_message_attachment(
            Some(content.clone()),
            command,
            Some(attachment),
        ))
        .expect("stream attachment into content store");
    runtime
        .block_on(producer)
        .expect("join attachment producer");

    let preingested_blob = preingested_blob.expect("quarantined attachment");
    let DfsNetworkMessage::DistributedWriteRequest(write) = command.routed_message.message else {
        panic!("distributed write request");
    };
    assert!(write.bytes.is_empty());
    assert_eq!(write.content_size_bytes, Some(bytes.len() as u64));
    let content_hash = write.content_hash.expect("primary finalized hash");
    assert!(
        content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_err(),
        "attachment ingest must not reserve its requested object version before admission"
    );
    assert_eq!(preingested_blob.content_hash, content_hash);
    content
        .bind_quarantined_blob(object_id.as_str(), version.as_str(), preingested_blob)
        .expect("bind admitted test blob");
    let (manifest, stored) = content
        .read_object_version(object_id.as_str(), version.as_str())
        .expect("read streamed object");
    assert_eq!(stored, bytes);
    assert_eq!(content_hash, manifest.content_hash);
}

fn run_daemon_dfs_maintenance_body_for_test(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Runtime,
) -> Result<(), DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let metadata_authority_preflight =
        runtime.block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            runtime.handle(),
        ))?;
    let foreground_preemption =
        daemon_dfs_mutation_scheduler(&data_dir)?.begin_maintenance_interest();
    let mut deferred_work = None;
    let result = runtime.block_on(run_daemon_dfs_maintenance_tick_with_admission(
        data_dir,
        cluster_paths.clone(),
        runtime.handle(),
        &metadata_authority_preflight,
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            content_gc_requested: true,
            content_reservation_reconciliation_requested: true,
            commit_promotion_recovery_requested: true,
        },
        sinomo_device::NodeResourceSnapshot::default(),
        DfsCommittedWorkTracker::new(),
        &mut deferred_work,
        &foreground_preemption,
    ));
    if let Some((detached_runtime, worker)) = deferred_work {
        schedule_daemon_dfs_message_deferred_work(
            runtime.handle(),
            &cluster_paths,
            detached_runtime,
            worker,
        );
    }
    result.map(|_| ())
}

fn daemon_dfs_test_execution_scope(
    file_system_id: &str,
    shard_id: &str,
) -> DfsMessageExecutionScope {
    DfsMessageExecutionScope::ExactShard {
        file_system_id: DfsFileSystemId::from_legacy_named(file_system_id)
            .expect("test filesystem id"),
        shard_id: DfsMetadataShardId::new(shard_id),
    }
}

#[test]
fn daemon_dfs_blocking_task_retains_admission_after_rpc_cancellation() {
    let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-admission");
    runtime.block_on(async {
        let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking_scheduler = Arc::clone(&scheduler);
        let handler = tokio::spawn(async move {
            let admission = blocking_scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await
                .expect("global admission");
            tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                release_rx.recv().expect("release blocking DFS task");
                drop(admission);
            })
            .await
            .expect("blocking DFS task")
        });
        started_rx.await.expect("blocking task started");
        handler.abort();
        assert!(
            handler
                .await
                .expect_err("handler cancellation")
                .is_cancelled()
        );

        let exact_scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
        let exact_scheduler = Arc::clone(&scheduler);
        let mut exact_waiter = tokio::spawn(async move {
            exact_scheduler
                .admit(&exact_scope)
                .await
                .expect("exact admission")
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut exact_waiter)
                .await
                .is_err(),
            "cancelling the RPC future must not release a running blocking task's admission"
        );

        release_tx.send(()).expect("release task");
        let exact = tokio::time::timeout(Duration::from_secs(1), &mut exact_waiter)
            .await
            .expect("exact waiter should advance after blocking work stops")
            .expect("exact waiter task");
        drop(exact);
    });
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn daemon_dfs_explicit_recovery_uses_bounds_and_routine_work_skips_inventory() {
    let explicit_recovery = daemon_dfs_explicit_disaster_recovery_tick_options(true, true);
    assert_eq!(
        explicit_recovery.metadata_replication,
        daemon_dfs_metadata_replication_options()
    );
    assert_eq!(
        daemon_dfs_metadata_replication_options().max_batches_per_target,
        DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET
    );
    assert_eq!(
        explicit_recovery.repair_max_jobs,
        DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK
    );
    assert!(explicit_recovery.lifecycle.compact_tombstones);
    assert_eq!(
        explicit_recovery.lifecycle.tombstone_compaction_min_age,
        DAEMON_DFS_TOMBSTONE_COMPACTION_MIN_AGE
    );
    assert_eq!(
        explicit_recovery.lifecycle.tombstone_compaction_max_records,
        DAEMON_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS
    );
    let lifecycle = explicit_recovery.lifecycle;
    assert!(lifecycle.collect_metadata_history);
    assert_eq!(
        lifecycle.metadata_history_gc_min_age,
        DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE
    );
    assert_eq!(
        lifecycle.metadata_history_gc_keep_latest_versions,
        DAEMON_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS
    );
    assert_eq!(
        lifecycle.metadata_history_gc_max_records,
        DAEMON_DFS_METADATA_HISTORY_GC_MAX_RECORDS
    );
    assert!(DAEMON_DFS_METADATA_HISTORY_GC_MAX_AGE >= DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE);
    assert!(DAEMON_DFS_METADATA_HISTORY_GC_RECORD_TRIGGER > 0);
    assert!(DAEMON_DFS_METADATA_HISTORY_GC_BYTES_TRIGGER > 0);
    assert!(lifecycle.collect_content_versions);
    assert!(lifecycle.collect_orphan_content);
    assert!(lifecycle.reconcile_content_reservations);
    assert_eq!(
        lifecycle.content_reservation_reconciliation_max_records,
        DAEMON_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS
    );
    let restricted =
        daemon_dfs_routine_maintenance_tick_options(sinomo_device::NodeResourceSnapshot::observed(
            sinomo_device::NodeResourceMode::Restricted,
            sinomo_device::NodeNetworkMetering::Unknown,
        ));
    assert_eq!(restricted.repair_max_jobs, 0);
    assert_eq!(restricted.placement_reconciliation_max_objects, 0);
    assert_eq!(restricted.metadata_replication.max_batches_per_target, 0);
    let constrained =
        daemon_dfs_routine_maintenance_tick_options(sinomo_device::NodeResourceSnapshot::observed(
            sinomo_device::NodeResourceMode::Constrained,
            sinomo_device::NodeNetworkMetering::Unmetered,
        ));
    assert_eq!(constrained.repair_max_jobs, 1);
    assert_eq!(constrained.placement_reconciliation_max_objects, 16);
    assert_eq!(constrained.metadata_replication.max_batches_per_target, 1);
    let metered =
        daemon_dfs_routine_maintenance_tick_options(sinomo_device::NodeResourceSnapshot::observed(
            sinomo_device::NodeResourceMode::Constrained,
            sinomo_device::NodeNetworkMetering::Metered,
        ));
    assert_eq!(metered.repair_max_jobs, 0);
    assert_eq!(metered.placement_reconciliation_max_objects, 0);
    assert_eq!(metered.metadata_replication.max_batches_per_target, 0);
    let unrelated_work =
        daemon_dfs_routine_maintenance_tick_options(sinomo_device::NodeResourceSnapshot::default())
            .lifecycle;
    assert!(!unrelated_work.repair_metadata);
    assert!(!unrelated_work.cleanup_stale_staging);
    assert!(!unrelated_work.compact_tombstones);
    assert!(!unrelated_work.collect_metadata_history);
    assert!(!unrelated_work.collect_content_versions);
    assert!(!unrelated_work.collect_orphan_content);
    assert!(!unrelated_work.reconcile_content_reservations);
    let reservation_only =
        daemon_dfs_explicit_disaster_recovery_tick_options(false, true).lifecycle;
    assert!(reservation_only.repair_metadata);
    assert!(reservation_only.cleanup_stale_staging);
    assert!(reservation_only.compact_tombstones);
    assert!(reservation_only.collect_metadata_history);
    assert!(!reservation_only.collect_content_versions);
    assert!(!reservation_only.collect_orphan_content);
    assert!(reservation_only.reconcile_content_reservations);
    let destructive_only =
        daemon_dfs_explicit_disaster_recovery_tick_options(true, false).lifecycle;
    assert!(destructive_only.collect_content_versions);
    assert!(destructive_only.collect_orphan_content);
    assert!(destructive_only.reconcile_content_reservations);
    assert!(
        !explicit_recovery.publish_metadata,
        "broad maintenance must not race retained exact-shard publishers"
    );
    assert_eq!(
        daemon_dfs_metadata_replication_options().attempt_timeout,
        DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT
    );
    assert_eq!(
        daemon_dfs_metadata_replication_options().target_timeout,
        DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT
    );
    assert!(daemon_dfs_metadata_replication_options().probe_caught_up_replicas);
    assert_eq!(
        daemon_dfs_metadata_publish_schedule_options().max_retry_rounds,
        DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS
    );
    assert_eq!(
        daemon_dfs_metadata_publish_schedule_options().retry_delay,
        DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
    );
    assert_eq!(
        daemon_dfs_metadata_publish_schedule_options().max_retry_delay,
        DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY
    );
}

#[test]
fn daemon_dfs_repair_queue_startup_reconciles_each_activation_without_generation_reuse() {
    let root = temp_root("repair-queue-reconciliation-marker");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);

    // An older successful migration says nothing about a later crash window.
    let old_marker = data_dir
        .dfs_dir()
        .join("maintenance/repair-queue-reconciled-v1");
    std::fs::create_dir_all(old_marker.parent().unwrap()).unwrap();
    std::fs::write(&old_marker, b"sinomo-dfs-repair-queue-reconciled-v1\n").unwrap();
    let initial_generation = schedule_daemon_dfs_startup_repair_reconciliation(&paths)
        .expect("schedule startup even with a legacy completion marker");
    complete_daemon_dfs_maintenance_reconciliation(&paths, initial_generation).unwrap();
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        None
    );
    let restarted_generation = schedule_daemon_dfs_startup_repair_reconciliation(&paths).unwrap();
    assert!(restarted_generation > initial_generation);
    complete_daemon_dfs_maintenance_reconciliation(&paths, initial_generation).unwrap();
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(restarted_generation),
        "a late old completion cannot consume a new activation's recovery"
    );

    let first_manual = request_daemon_dfs_maintenance_reconciliation(&paths)
        .expect("first manual maintenance reconciliation");
    let second_manual = request_daemon_dfs_maintenance_reconciliation(&paths)
        .expect("second manual maintenance reconciliation");
    assert!(second_manual > first_manual);
    complete_daemon_dfs_maintenance_reconciliation(&paths, first_manual)
        .expect("older pass completion");
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(second_manual),
        "an older pass must not clear a newer manual maintenance request"
    );
    complete_daemon_dfs_maintenance_reconciliation(&paths, second_manual)
        .expect("newest pass completion");
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        None
    );
}

#[test]
fn daemon_dfs_pending_probe_does_not_schedule_unprotected_content_gc() {
    let root = temp_root("pending-unprotected-content-gc");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = NodeId::from_legacy_named("node_pending_unprotected_content").expect("node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_pending_unprotected_content").expect("cluster"),
        "pending unprotected content",
        node_id,
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    drop(
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("initialize metadata backend"),
    );
    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
    let orphan = content
        .put_object_version_blob_only(
            "content-only-object",
            "content-only-version",
            b"content without an authoritative metadata root",
        )
        .expect("store content-only blob");
    assert!(
        content
            .has_blob(&orphan.content_hash)
            .expect("content-only blob exists")
    );

    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.due_time_based_maintenance_count, 0);
    assert!(
        !pending.should_trigger_background_maintenance(),
        "unprotected content candidates must not create a daemon maintenance loop"
    );
    assert!(
        content
            .has_blob(&orphan.content_hash)
            .expect("content-only blob remains"),
        "the scheduling probe must remain read-only"
    );
}

#[test]
fn daemon_dfs_content_gc_refresh_classifier_parks_only_structural_or_handoff_gaps() {
    for reason in [
        DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured,
        DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured,
        DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
            handoff_id: Some("handoff".to_string()),
        },
        DfsContentRootCatalogIncompleteReason::PageBudgetExceeded { max_pages: 1 },
    ] {
        assert!(
            !daemon_dfs_content_gc_blocked_reason_requires_refresh(&reason),
            "{reason:?} must remain parked on its state change and bounded deadline"
        );
    }
    for reason in [
        DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
        DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
        DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled,
        DfsContentRootCatalogIncompleteReason::CatalogTimestampInFuture {
            identity: None,
            observed_unix_nanos: u64::MAX,
            now_unix_nanos: 1,
        },
    ] {
        assert!(
            daemon_dfs_content_gc_blocked_reason_requires_refresh(&reason),
            "{reason:?} must request a fresh authoritative catalog immediately"
        );
    }
    let ordinary_advance = DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark {
        identity: sinomo_dfs::DfsContentRootCatalogShardIdentity {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            shard_id: DfsMetadataShardId::new("root"),
            primary_node_id: NodeId::from_legacy_named("node_catalog").expect("primary"),
            epoch: 1,
        },
        catalog_journal_sequence: 1,
        watermark_journal_sequence: 2,
    };
    assert!(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(&ordinary_advance));
    assert!(
        !daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(
            &DfsContentRootCatalogIncompleteReason::ShardIncomplete {
                identity: sinomo_dfs::DfsContentRootCatalogShardIdentity {
                    file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                    shard_id: DfsMetadataShardId::new("root"),
                    primary_node_id: NodeId::from_legacy_named("node_catalog").expect("primary"),
                    epoch: 1,
                },
                reasons: vec![
                    ordinary_advance,
                    DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
                ],
            }
        ),
        "a mixed head-advance and fingerprint failure must remain immediately repairable"
    );
}

#[test]
fn daemon_dfs_content_gc_stale_catalog_observations_request_a_new_generation() {
    let version_report = sinomo_dfs::ContentStoreVersionGcReport {
        retained_same_or_stale_catalog_version_count: 2,
        ..sinomo_dfs::ContentStoreVersionGcReport::default()
    };
    assert_eq!(daemon_dfs_version_gc_candidate_count(&version_report), 2);

    let orphan_report = sinomo_dfs::ContentStoreGcReport {
        retained_same_or_stale_catalog_orphan_count: 3,
        ..sinomo_dfs::ContentStoreGcReport::default()
    };
    assert_eq!(daemon_dfs_orphan_gc_candidate_count(&orphan_report), 3);
}

#[test]
fn daemon_dfs_content_gc_projection_requires_complete_catalog_coverage() {
    let root = temp_root("pending-content-gc-catalog-coverage");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_pending_content_catalog").expect("local node");
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: Vec::new(),
        epoch: 3,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    install_test_metadata_replica_shard(
        &cluster_paths,
        "cluster_pending_content_catalog",
        &local,
        &shard,
    );
    let state_fingerprint = format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64));
    let watermark = sinomo_dfs::DfsMetadataShardHeadWatermark {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard.shard_id.clone(),
        primary_node_id: local.clone(),
        epoch: shard.epoch,
        journal_sequence: 7,
        state_fingerprint: state_fingerprint.clone(),
        acknowledged_node_ids: vec![local],
    };
    append_cluster_raft_command(
        &cluster_paths,
        &shard.primary_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                sinomo_dfs::metadata_shard_head_watermark_key(
                    &shard.file_system_id,
                    &shard.shard_id,
                ),
                &watermark,
                None,
            )
            .expect("head watermark op"),
        ),
    )
    .expect("write head watermark");

    let content_root = data_dir.dfs_dir().join("content");
    let content = LocalContentStore::new(&content_root).expect("content store");
    content
        .put_object_version(
            "pending-content-object",
            "pending-content-version-1",
            b"old manifest",
        )
        .expect("old manifest");
    content
        .put_object_version(
            "pending-content-object",
            "pending-content-version-2",
            b"latest manifest",
        )
        .expect("latest manifest");
    content
        .put_object_version_blob_only(
            "pending-orphan-object",
            "pending-orphan-version",
            b"orphan blob",
        )
        .expect("orphan blob");
    for reservation in content
        .content_reservation_roots()
        .expect("reservation roots")
        .reservations
    {
        content
            .release_content_reservation_for_test(
                &reservation.identity.object_id,
                &reservation.identity.version,
                &reservation.identity.content_hash,
            )
            .expect("release fixture reservation");
    }
    let reservations = content
        .content_reservation_roots()
        .expect("empty reservation roots");
    assert_eq!(reservations.reservation_count, 0);

    let generated_at_unix_nanos = daemon_dfs_now_unix_nanos().saturating_sub(1);
    let file_system_config = test_file_system_config_for_shard(&shard);
    let topology_digest = sinomo_dfs::dfs_content_root_catalog_topology_digest_with_file_systems(
        std::slice::from_ref(&file_system_config),
        std::slice::from_ref(&shard),
    )
    .expect("topology digest");
    let identity = sinomo_dfs::DfsContentRootCatalogShardIdentity::from_config(&shard);
    let complete_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
        topology_digest.clone(),
        vec![identity.clone()],
        generated_at_unix_nanos,
        vec![sinomo_dfs::DfsContentRootCatalogFragment {
            identity: identity.clone(),
            topology_digest: topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: watermark.journal_sequence,
                state_fingerprint,
            },
            entries: Vec::new(),
        }],
    )
    .expect("complete catalog");
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(complete_catalog.clone(), generated_at_unix_nanos)
        .expect("install complete catalog");
    let lifecycle = DfsMaintenanceRunOptions {
        content_version_gc_min_age: Duration::ZERO,
        content_version_gc_keep_latest_versions: 1,
        content_orphan_gc_min_age: Duration::ZERO,
        content_root_catalog_max_age: Duration::from_secs(60),
        ..DfsMaintenanceRunOptions::default()
    };
    let complete = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect("complete projection");
    assert_eq!(complete.covered_shard_count, 1);
    assert_eq!(complete.expected_shard_count, 1);
    assert!(complete.blocked_reasons.is_empty());
    assert_eq!(complete.scrub_blocked_issue_count, 0);
    assert_eq!(complete.version_candidate_count, 1);
    assert_eq!(complete.orphan_candidate_count, 1);
    assert!(complete.due_maintenance_count >= 2);
    assert_eq!(complete.reservation_due_maintenance_count, 0);
    assert!(complete.next_deadline_unix_nanos.is_some());

    let covered_object_id =
        DfsObjectId::from_legacy_named("covered-reservation-object").expect("object id");
    let covered_version =
        DfsVersion::from_legacy_named("covered-reservation-version").expect("version");
    let covered_content_hash = format!("blake3:{}", "2".repeat(64));
    let covered_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
        topology_digest.clone(),
        vec![identity.clone()],
        generated_at_unix_nanos,
        vec![sinomo_dfs::DfsContentRootCatalogFragment {
            identity: identity.clone(),
            topology_digest: topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: watermark.journal_sequence,
                state_fingerprint: watermark.state_fingerprint.clone(),
            },
            entries: vec![sinomo_dfs::DfsContentRootCatalogEntry::Version(
                sinomo_dfs::DfsMetadataVersionIdentity::new(
                    covered_object_id.clone(),
                    covered_version.clone(),
                ),
            )],
        }],
    )
    .expect("covered-reservation catalog");
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(covered_catalog, generated_at_unix_nanos)
        .expect("install covered-reservation catalog");
    let covered_reservations = sinomo_dfs::ContentStoreReservationRoots {
        reservation_count: 1,
        reservations: vec![sinomo_dfs::ContentStoreReservationRecord {
            identity: sinomo_dfs::ContentStoreReservationIdentity {
                object_id: covered_object_id.as_str().to_string(),
                version: covered_version.as_str().to_string(),
                content_hash: covered_content_hash.clone(),
            },
            reserved_at_unix_nanos: generated_at_unix_nanos,
        }],
        versions: BTreeSet::from([(
            covered_object_id.as_str().to_string(),
            covered_version.as_str().to_string(),
        )]),
        content_hashes: BTreeSet::from([covered_content_hash]),
    };
    let covered_reservation = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &covered_reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect("covered-reservation projection");
    assert_eq!(
        covered_reservation.due_maintenance_count, complete.due_maintenance_count,
        "catalog-covered reservations must not request destructive GC or another catalog refresh"
    );
    assert_eq!(
        covered_reservation.reservation_due_maintenance_count, 1,
        "catalog-covered reservations must request only bounded local reconciliation"
    );
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(complete_catalog.clone(), generated_at_unix_nanos)
        .expect("restore catalog without rejected reservation");
    let rejected_object_id = "rejected-reservation-object".to_string();
    let rejected_version = "rejected-reservation-version".to_string();
    let rejection_grace_nanos =
        daemon_dfs_duration_nanos(lifecycle.content_reservation_rejection_grace);
    let rejected_reservations = sinomo_dfs::ContentStoreReservationRoots {
        reservation_count: 1,
        reservations: vec![sinomo_dfs::ContentStoreReservationRecord {
            identity: sinomo_dfs::ContentStoreReservationIdentity {
                object_id: rejected_object_id.clone(),
                version: rejected_version.clone(),
                content_hash: format!("blake3:{}", "3".repeat(64)),
            },
            reserved_at_unix_nanos: generated_at_unix_nanos
                .saturating_sub(rejection_grace_nanos.saturating_add(1)),
        }],
        versions: BTreeSet::from([(rejected_object_id, rejected_version)]),
        content_hashes: BTreeSet::new(),
    };
    let rejected_reservation = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &rejected_reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect("rejection-due reservation projection");
    assert_eq!(
        rejected_reservation.due_maintenance_count,
        complete.due_maintenance_count.saturating_add(1),
        "an uncovered reservation with durable rejection evidence must request a fresh destructive pass"
    );
    assert_eq!(
        rejected_reservation.reservation_due_maintenance_count, 0,
        "rejection requires refreshed catalog admission rather than reuse-only reconciliation"
    );

    let behind_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
        topology_digest.clone(),
        vec![identity.clone()],
        generated_at_unix_nanos,
        vec![sinomo_dfs::DfsContentRootCatalogFragment {
            identity: identity.clone(),
            topology_digest: topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: watermark.journal_sequence.saturating_sub(1),
                state_fingerprint: format!("dfs-metadata-prefix-v2:blake3:{}", "1".repeat(64)),
            },
            entries: vec![sinomo_dfs::DfsContentRootCatalogEntry::Version(
                sinomo_dfs::DfsMetadataVersionIdentity::new(covered_object_id, covered_version),
            )],
        }],
    )
    .expect("behind catalog");
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(behind_catalog, generated_at_unix_nanos)
        .expect("install behind catalog");
    let coalesced_now = generated_at_unix_nanos.saturating_add(1);
    let behind = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &covered_reservations,
        lifecycle,
        coalesced_now,
    )
    .expect("behind-head projection");
    assert_eq!(behind.catalog_journal_lag, 1);
    assert_eq!(
        behind.due_maintenance_count, 0,
        "ordinary head churn must not immediately fan out another full catalog refresh"
    );
    assert_eq!(
        behind.reservation_due_maintenance_count, 1,
        "historical positive coverage must keep draining reservations while destructive refresh is coalesced"
    );
    assert_eq!(
        behind.next_deadline_unix_nanos,
        Some(
            generated_at_unix_nanos.saturating_add(daemon_dfs_duration_nanos(
                lifecycle.content_root_catalog_max_age,
            ))
        )
    );
    let repeated = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &covered_reservations,
        lifecycle,
        coalesced_now.saturating_add(1),
    )
    .expect("repeated behind-head projection");
    assert_eq!(
        repeated.next_deadline_unix_nanos, behind.next_deadline_unix_nanos,
        "head-lag coalescing must retain one absolute deadline"
    );
    let refresh_deadline = behind
        .next_deadline_unix_nanos
        .expect("coalesced refresh deadline");
    let expired = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &covered_reservations,
        lifecycle,
        refresh_deadline,
    )
    .expect("expired behind-head projection");
    assert!(
        expired.due_maintenance_count > 0,
        "the bounded coalescing deadline must make refresh due"
    );

    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(complete_catalog, generated_at_unix_nanos)
        .expect("restore complete catalog");
    std::fs::write(
        content_root
            .join("objects")
            .join("pending-content-object")
            .join("pending-content-version-2")
            .join("manifest.sobj"),
        b"torn manifest",
    )
    .expect("corrupt fixture manifest");
    let scrub_blocked = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect("scrub-blocked projection");
    assert!(
        scrub_blocked.scrub_blocked_issue_count > 0,
        "local scrub failures must remain visible when catalog coverage is complete"
    );

    let incomplete_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
        topology_digest.clone(),
        vec![identity.clone()],
        generated_at_unix_nanos,
        vec![sinomo_dfs::DfsContentRootCatalogFragment {
            identity: identity.clone(),
            topology_digest: topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Incomplete {
                reasons: vec![DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable],
            },
            entries: Vec::new(),
        }],
    )
    .expect("incomplete catalog");
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(incomplete_catalog, generated_at_unix_nanos)
        .expect("install incomplete catalog");
    let incomplete = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect("incomplete projection");
    assert_eq!(incomplete.covered_shard_count, 0);
    assert_eq!(incomplete.expected_shard_count, 1);
    assert_eq!(incomplete.version_candidate_count, 0);
    assert_eq!(incomplete.orphan_candidate_count, 0);
    assert!(incomplete.due_maintenance_count > 0);
    assert_eq!(incomplete.next_deadline_unix_nanos, None);
    assert!(matches!(
        incomplete.blocked_reasons.as_slice(),
        [DfsContentRootCatalogIncompleteReason::ShardIncomplete { .. }]
    ));

    let parked_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
        topology_digest.clone(),
        vec![identity.clone()],
        generated_at_unix_nanos,
        vec![sinomo_dfs::DfsContentRootCatalogFragment {
            identity,
            topology_digest,
            generated_at_unix_nanos,
            coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Incomplete {
                reasons: vec![
                    DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                        handoff_id: Some("pending-catalog-handoff".to_string()),
                    },
                ],
            },
            entries: Vec::new(),
        }],
    )
    .expect("parked handoff catalog");
    default_dfs_content_root_catalog_store(&content_root)
        .install_test_catalog_at(parked_catalog, generated_at_unix_nanos)
        .expect("install parked handoff catalog");
    let parked = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &reservations,
        DfsMaintenanceRunOptions::default(),
        daemon_dfs_now_unix_nanos(),
    )
    .expect("parked handoff projection");
    assert_eq!(parked.due_maintenance_count, 0);
    assert_eq!(
        parked.next_deadline_unix_nanos,
        Some(
            generated_at_unix_nanos.saturating_add(daemon_dfs_duration_nanos(
                DfsMaintenanceRunOptions::default().content_root_catalog_max_age,
            ))
        )
    );

    let paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(
        pending.due_time_based_maintenance_count, 0,
        "routine scheduling must not import a content-inventory deadline"
    );
    assert!(
        !pending.should_trigger_background_maintenance(),
        "an explicit disaster-recovery projection must not leak into routine scheduling"
    );
}

#[test]
fn daemon_dfs_metadata_history_gc_schedule_combines_pressure_and_safe_age() {
    fn projection(
        eligible_record_count: u64,
        oldest_eligible_record_unix_nanos: Option<u64>,
        next_eligible_at_unix_nanos: Option<u64>,
        total_record_count: u64,
        estimated_serialized_bytes: u64,
        snapshot_size_bytes: u64,
        journal_size_bytes: u64,
    ) -> sinomo_dfs::DfsMetadataHistoryGcReport {
        sinomo_dfs::DfsMetadataHistoryGcReport {
            min_history_age_nanos: 100,
            eligible_version_count: eligible_record_count,
            oldest_eligible_record_unix_nanos,
            next_eligible_at_unix_nanos,
            shards: vec![sinomo_dfs::DfsMetadataHistoryShardStats {
                shard_id: DfsMetadataShardId::new("root"),
                collection_authorized: true,
                snapshot_size_bytes: Some(snapshot_size_bytes),
                journal_size_bytes,
                journal_segment_count: 1,
                journal_head_sequence: 1,
                total_record_count,
                estimated_serialized_bytes,
                tables: Vec::new(),
            }],
            ..Default::default()
        }
    }

    let young_pressure = projection(0, None, Some(1_100), 10, 10, 10, 10);
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &young_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        ),
        DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: 0,
            next_deadline_unix_nanos: Some(1_100),
        },
        "young-only high-water pressure must not launch a no-op GC pass"
    );

    let below_pressure = projection(1, Some(850), None, 9, 9, 9, 9);
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &below_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        ),
        DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: 0,
            next_deadline_unix_nanos: Some(1_050),
        },
        "low-volume eligible history should park until its maximum-age deadline"
    );

    let record_pressure = projection(1, Some(850), None, 10, 9, 9, 9);
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &record_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        )
        .due_record_count,
        1
    );

    let mut replica_pressure = projection(1, Some(850), None, 9, 9, 9, 9);
    replica_pressure
        .shards
        .push(sinomo_dfs::DfsMetadataHistoryShardStats {
            shard_id: DfsMetadataShardId::new("replica"),
            collection_authorized: false,
            snapshot_size_bytes: Some(100),
            journal_size_bytes: 100,
            journal_segment_count: 1,
            journal_head_sequence: 1,
            total_record_count: 100,
            estimated_serialized_bytes: 100,
            tables: Vec::new(),
        });
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &replica_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        ),
        DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: 0,
            next_deadline_unix_nanos: Some(1_050),
        },
        "replica and frozen-shard pressure must not schedule authoritative GC"
    );

    let byte_pressure = projection(1, Some(850), None, 9, 9, 9, 10);
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &byte_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        )
        .due_record_count,
        1
    );

    let age_pressure = projection(1, Some(800), None, 9, 9, 9, 9);
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &age_pressure,
            1_000,
            10,
            10,
            Duration::from_nanos(200),
        )
        .due_record_count,
        1
    );

    let configured_minimum_wins = projection(1, Some(850), None, 9, 9, 9, 9);
    let mut configured_minimum_wins = configured_minimum_wins;
    configured_minimum_wins.min_history_age_nanos = 200;
    assert_eq!(
        daemon_dfs_metadata_history_gc_schedule(
            &configured_minimum_wins,
            1_000,
            10,
            10,
            Duration::from_nanos(50),
        ),
        DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: 0,
            next_deadline_unix_nanos: Some(1_050),
        },
        "the age trigger must never shorten the configured recovery window"
    );
}

#[test]
fn daemon_dfs_maintenance_continuation_requires_explicit_progress_and_remainder() {
    fn empty_report() -> DfsMaintenanceTickReport {
        DfsMaintenanceTickReport {
            namespace_jobs: Default::default(),
            conflicts: Default::default(),
            write_intents: Default::default(),
            placement_reconciliation: None,
            lifecycle_reports: Vec::new(),
            scheduled_repairs: Vec::new(),
            followup_events: Vec::new(),
            repair_attempts_before: Vec::new(),
            repair_attempts_after: Vec::new(),
            repair_budget_exhausted: false,
            metadata_replication: DfsMetadataJournalReplicationReport {
                outcomes: Vec::new(),
            },
        }
    }

    let mut no_progress_namespace = empty_report();
    no_progress_namespace.namespace_jobs.resumed_job_count = 1;
    no_progress_namespace
        .namespace_jobs
        .remaining_running_job_count = 1;
    assert!(
        !daemon_dfs_maintenance_report_reached_limit(&no_progress_namespace),
        "a still-running job is not evidence that its successful pass advanced"
    );

    let mut bounded_namespace = empty_report();
    bounded_namespace.namespace_jobs.resumed_job_count = 1;
    bounded_namespace.namespace_jobs.completed_job_count = 1;
    bounded_namespace.namespace_jobs.remaining_running_job_count = 1;
    assert!(
        daemon_dfs_maintenance_report_reached_limit(&bounded_namespace),
        "a completed bounded job plus an explicitly skipped running job owns continuation"
    );

    let mut no_progress_repairs = empty_report();
    no_progress_repairs.repair_budget_exhausted = true;
    assert!(
        !daemon_dfs_maintenance_report_reached_limit(&no_progress_repairs),
        "a bare repair remainder without a scheduled repair must keep ordinary backoff"
    );

    let mut bounded_repairs = no_progress_repairs;
    bounded_repairs
        .scheduled_repairs
        .push(DistributedRepairJob {
            intent_id: "intent-bounded-repair".to_string(),
            file_system_id: DfsFileSystemId::generate(),
            object_id: DfsObjectId::generate(),
            version: DfsVersion::generate(),
            missing_replica_node_id: NodeId::generate(),
        });
    assert!(
        daemon_dfs_maintenance_report_reached_limit(&bounded_repairs),
        "a scheduled repair plus an immediately runnable remainder owns continuation"
    );

    let mut no_progress_compaction = empty_report();
    no_progress_compaction
        .lifecycle_reports
        .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
            tombstone_compaction: Some(sinomo_dfs::DfsTombstoneCompactionReport {
                max_compactions_reached: true,
                ..Default::default()
            }),
            ..Default::default()
        });
    assert!(
        !daemon_dfs_maintenance_report_reached_limit(&no_progress_compaction),
        "a zero-progress retained-limit report must keep ordinary backoff"
    );
    no_progress_compaction.lifecycle_reports[0]
        .tombstone_compaction
        .as_mut()
        .expect("compaction report")
        .compacted_tombstone_count = 1;
    assert!(daemon_dfs_maintenance_report_reached_limit(
        &no_progress_compaction
    ));

    let mut no_progress_history_gc = empty_report();
    no_progress_history_gc
        .lifecycle_reports
        .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
            metadata_history_gc: Some(sinomo_dfs::DfsMetadataHistoryGcReport {
                max_removals_reached: true,
                ..Default::default()
            }),
            ..Default::default()
        });
    assert!(
        !daemon_dfs_maintenance_report_reached_limit(&no_progress_history_gc),
        "a zero-progress metadata-history limit must keep ordinary backoff"
    );
    no_progress_history_gc.lifecycle_reports[0]
        .metadata_history_gc
        .as_mut()
        .expect("metadata history GC report")
        .removed_version_count = 1;
    assert!(daemon_dfs_maintenance_report_reached_limit(
        &no_progress_history_gc
    ));

    let mut reservation_batch = empty_report();
    reservation_batch
        .lifecycle_reports
        .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
            content_reservation_reconciliation: Some(
                sinomo_dfs::ContentStoreReservationReconciliationReport {
                    has_more: true,
                    ..Default::default()
                },
            ),
            ..Default::default()
        });
    assert!(
        !daemon_dfs_maintenance_report_reached_limit(&reservation_batch),
        "an empty reservation batch must not spin an immediate continuation"
    );
    reservation_batch.lifecycle_reports[0]
        .content_reservation_reconciliation
        .as_mut()
        .expect("reservation reconciliation report")
        .examined_reservation_count = 1;
    assert!(
        daemon_dfs_maintenance_report_reached_limit(&reservation_batch),
        "a bounded reservation batch with remaining identities must drain immediately"
    );
}

fn test_default_cluster_bootstrap_host(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: NodeId,
    runtime: &tokio::runtime::Handle,
) -> sinomo_dfs::DfsControlPlaneDefaultClusterBootstrapHost {
    sinomo_dfs::DfsControlPlaneDefaultClusterBootstrapHost::new(
        data_dir.clone(),
        cluster_paths.clone(),
        local_node_id,
        Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
            cluster_paths,
            runtime,
        )),
    )
}

fn process_test_dfs_metadata_journal_batch(
    paths: &DaemonPaths,
    context: sinomo_dfs::DfsMetadataReplicaShardContext,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let authenticated_source_node_id =
        command
            .source_node_id
            .clone()
            .ok_or_else(|| DaemonError::Network {
                address: "DFS metadata journal test source".to_string(),
                detail: "metadata journal test command has no claimed source".to_string(),
            })?;
    sinomo_dfs::process_cluster_dfs_metadata_journal_batch_with_replica_context_for_test(
        &data_dir,
        &authenticated_source_node_id,
        command,
        context,
    )
    .map_err(Into::into)
}

#[test]
fn daemon_data_plane_replica_ingress_reuses_one_resident_shard() {
    let root = temp_root("daemon_data_plane_resident_replica");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = Arc::new(SinomoDataDir::new(&data_root));
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(data_dir.as_ref());
    let primary =
        NodeId::from_legacy_named("node_daemon_data_plane_replica_primary").expect("primary");
    let local = NodeId::from_legacy_named("node_daemon_data_plane_replica_local").expect("local");
    let file_system = distributed_sifs(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![local.clone()],
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    install_test_metadata_replica_shard(
        &cluster_paths,
        "cluster_daemon_data_plane_resident_replica",
        &local,
        &shard,
    );
    let context = sinomo_dfs::DfsMetadataReplicaShardContext {
        local_node_id: local,
        shard: shard.clone(),
        pending_membership_handoff: None,
    };
    let mut source = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("source-metadata"),
    )
    .expect("source metadata");
    let runtime = daemon_test_runtime("dfs-resident-replica-ingress");
    let host = DaemonDfsDataPlaneHost::new_on_runtime(paths.clone(), runtime.handle());

    let missing_context = runtime.block_on(host.apply_metadata_journal_batch(
        Arc::clone(&data_dir),
        primary.clone(),
        DfsMetadataJournalBatchCommand {
            request_id: "resident-replica-missing-context".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(file_system.file_system_id.clone()),
            authority_epoch: Some(shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch: sinomo_dfs::DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("root"),
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        },
        None,
    ));
    assert!(
        missing_context
            .expect_err("replica ingress without main data owner")
            .to_string()
            .contains("data-owner capability")
    );
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0
    );

    let snapshot_command = sinomo_dfs::DfsMetadataSnapshotInstallCommand {
        request_id: "resident-replica-snapshot".to_string(),
        source_node_id: primary.clone(),
        file_system_id: Some(file_system.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        primary_handoff_id: None,
        shard_id: DfsMetadataShardId::new("root"),
        next_after_sequence: 0,
        snapshot: source.export_table_snapshot(),
        reason: "resident replica test".to_string(),
    };
    let missing_owner = runtime.block_on(host.install_metadata_snapshot(
        Arc::clone(&data_dir),
        primary.clone(),
        snapshot_command.clone(),
        Some(context.clone()),
    ));
    assert!(
        missing_owner
            .expect_err("replica ingress without main data owner")
            .to_string()
            .contains("data-owner capability")
    );
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0
    );

    let data_owner_lock = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main data owner"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
        .expect("register main data owner");
    let missing_context = runtime
        .block_on(host.apply_metadata_journal_batch(
            Arc::clone(&data_dir),
            primary.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "resident-replica-missing-context-with-owner".to_string(),
                source_node_id: Some(primary.clone()),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: sinomo_dfs::DfsMetadataJournalBatch {
                    shard_id: DfsMetadataShardId::new("root"),
                    entries: Vec::new(),
                    next_after_sequence: 0,
                },
            },
            None,
        ))
        .expect("typed missing-context response with owner");
    assert!(matches!(
        missing_context.result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    let snapshot = runtime
        .block_on(host.install_metadata_snapshot(
            Arc::clone(&data_dir),
            primary.clone(),
            snapshot_command,
            Some(context.clone()),
        ))
        .expect("resident snapshot response");
    assert!(matches!(
        snapshot.result,
        DfsMetadataSnapshotInstallResult::Installed { .. }
    ));
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0
    );

    source
        .put_file_system(&file_system)
        .expect("source filesystem journal entry");
    let mut source_subscription = source.subscribe(0);
    let source_batch = source
        .poll_subscription(&mut source_subscription)
        .expect("source journal batch");
    let source_frames = DfsMetadataBackend::journal_frames_for_batch(&source, &source_batch)
        .expect("source canonical journal frames");
    let source_next_after_sequence = source_batch.next_after_sequence;
    let cleanup_requests_before_materialization =
        daemon_dfs_allocator_cleanup_request_count(data_dir.as_ref());
    let allocator_trims_before_materialization =
        daemon_dfs_allocator_trim_completion_count(data_dir.as_ref());
    let journal_request_id = "resident-replica-journal";
    let journal_command = DfsMetadataJournalBatchCommand {
        request_id: journal_request_id.to_string(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(file_system.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: Some(sinomo_dfs::DfsMetadataDurableReplication {
            confirmation_id: "resident-replica-durable-confirmation".to_string(),
            journal_frames: source_frames,
        }),
        batch: sinomo_dfs::DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: Vec::new(),
            next_after_sequence: source_next_after_sequence,
        },
    };
    let (journal_persisted, release_journal) =
        install_daemon_dfs_message_committed_edge_test_hook(journal_request_id);
    let journal_host = host.clone();
    let journal_data_dir = Arc::clone(&data_dir);
    let journal_primary = primary.clone();
    let journal_context = context.clone();
    let journal_task = runtime.spawn(async move {
        journal_host
            .apply_metadata_journal_batch(
                journal_data_dir,
                journal_primary,
                journal_command,
                Some(journal_context),
            )
            .await
    });
    journal_persisted
        .recv_timeout(Duration::from_secs(2))
        .expect("durable journal append reached cancellation boundary");
    journal_task.abort();
    release_journal
        .send(())
        .expect("release cancelled durable journal task");
    runtime.block_on(async {
        let aborted = journal_task.await.expect_err("host future was cancelled");
        assert!(aborted.is_cancelled());
    });
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0
    );
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let materialized = daemon_dfs_cached_resident_metadata_shard_handle(
                data_dir.as_ref(),
                &read_daemon_dfs_message_peer_snapshot(&cluster_paths)
                    .expect("materialization peer snapshot read")
                    .expect("materialization peer snapshot"),
                &file_system.file_system_id,
                &DfsMetadataShardId::new("root"),
                &cluster_paths,
                runtime.handle(),
            )
            .and_then(|shard| {
                shard
                    .with_backend(|backend| backend.get_file_system(&file_system.file_system_id))
                    .map_err(Into::into)
            })
            .expect("resident materialization read");
            if materialized.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "durably acknowledged resident frame did not materialize"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while daemon_dfs_allocator_cleanup_request_count(data_dir.as_ref())
            <= cleanup_requests_before_materialization
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "materializing a durable replica suffix must retain allocator cleanup work"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while daemon_dfs_allocator_trim_completion_count(data_dir.as_ref())
            <= allocator_trims_before_materialization
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "scoped allocator cleanup did not trim after materialization"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let mut next_shard = shard.clone();
    next_shard.epoch = next_shard.epoch.saturating_add(1);
    append_cluster_raft_command(
        &cluster_paths,
        &context.local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&next_shard.file_system_id, &next_shard.shard_id),
                &next_shard,
                None,
            )
            .expect("next metadata shard op"),
        ),
    )
    .expect("advance metadata shard authority");
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("peer snapshot read")
        .expect("peer snapshot");
    let _authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime.handle()),
    )
    .expect("authority generation");
    let stale = runtime
        .block_on(host.apply_metadata_journal_batch(
            Arc::clone(&data_dir),
            context.shard.primary_node_id.clone(),
            DfsMetadataJournalBatchCommand {
                request_id: "resident-replica-stale-context".to_string(),
                source_node_id: Some(context.shard.primary_node_id.clone()),
                file_system_id: Some(context.shard.file_system_id.clone()),
                authority_epoch: Some(context.shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: sinomo_dfs::DfsMetadataJournalBatch {
                    shard_id: DfsMetadataShardId::new(context.shard.shard_id.clone()),
                    entries: Vec::new(),
                    next_after_sequence: 0,
                },
            },
            Some(context),
        ))
        .expect("typed stale-context response");
    assert!(matches!(
        stale.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains("authority context is stale")
    ));
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0
    );

    evict_daemon_dfs_message_service_cache(data_dir.as_ref()).expect("evict resident cache");
    drop(data_owner_lock);
}

#[test]
fn daemon_dfs_metadata_journal_batch_applies_to_local_backend() {
    let root = temp_root("daemon_dfs_metadata_journal_batch");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let source_root = root.join("source-meta");
    let source_node_id =
        NodeId::from_legacy_named("node_daemon_dfs_metadata_source").expect("source node");
    let local_node_id =
        NodeId::from_legacy_named("node_daemon_dfs_metadata_local").expect("local node");
    let file_system = distributed_sifs(source_node_id.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: source_node_id.clone(),
        replica_node_ids: vec![local_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    install_test_metadata_replica_shard(
        &cluster_paths,
        "cluster_daemon_dfs_metadata_apply",
        &local_node_id,
        &shard,
    );
    let object = distributed_sifs_bootstrap_objects(source_node_id.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    let mut source = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
        .expect("source backend");
    source.put_file_system(&file_system).expect("filesystem");
    source.put_object(&object).expect("object");
    source
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        })
        .expect("version");
    let mut subscription = source.subscribe(0);
    let batch = source
        .poll_subscription(&mut subscription)
        .expect("source batch");

    let response = process_test_dfs_metadata_journal_batch(
        &paths,
        sinomo_dfs::DfsMetadataReplicaShardContext {
            local_node_id: local_node_id.clone(),
            shard: shard.clone(),
            pending_membership_handoff: None,
        },
        DfsMetadataJournalBatchCommand {
            request_id: "dfs_meta_req_1".to_string(),
            source_node_id: Some(source_node_id),
            file_system_id: Some(file_system.file_system_id.clone()),
            authority_epoch: Some(shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch,
        },
    )
    .expect("apply batch");
    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(response.applied_entries, 3);

    let target = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("data").join("dfs").join("metadata").join("root"),
    )
    .expect("target backend");
    assert_eq!(
        target.get_object(&object.object_id).expect("target object"),
        Some(object)
    );
}

#[test]
fn daemon_dfs_metadata_journal_batch_uses_path_safe_shard_directory() {
    let root = temp_root("daemon_dfs_metadata_journal_batch_safe_shard");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let source_root = root.join("source-meta");
    let source_node_id =
        NodeId::from_legacy_named("node_daemon_dfs_metadata_safe_source").expect("source node");
    let local_node_id =
        NodeId::from_legacy_named("node_daemon_dfs_metadata_safe_local").expect("local node");
    let file_system = distributed_sifs(source_node_id.clone());
    let object = distributed_sifs_bootstrap_objects(source_node_id.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    let shard_id = DfsMetadataShardId::new("root/frag");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: source_node_id.clone(),
        replica_node_ids: vec![local_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    install_test_metadata_replica_shard(
        &cluster_paths,
        "cluster_daemon_dfs_metadata_safe",
        &local_node_id,
        &shard,
    );
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), source_root).expect("source backend");
    source.put_file_system(&file_system).expect("filesystem");
    source.put_object(&object).expect("object");
    let mut subscription = source.subscribe(0);
    let batch = source
        .poll_subscription(&mut subscription)
        .expect("source batch");

    let response = process_test_dfs_metadata_journal_batch(
        &paths,
        sinomo_dfs::DfsMetadataReplicaShardContext {
            local_node_id: local_node_id.clone(),
            shard: shard.clone(),
            pending_membership_handoff: None,
        },
        DfsMetadataJournalBatchCommand {
            request_id: "dfs_meta_req_safe".to_string(),
            source_node_id: Some(source_node_id),
            file_system_id: Some(file_system.file_system_id.clone()),
            authority_epoch: Some(shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch,
        },
    )
    .expect("apply batch");
    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);

    let target = LocalSobjMetadataBackend::new(
        shard_id,
        root.join("data")
            .join("dfs")
            .join("metadata")
            .join("root_frag"),
    )
    .expect("target backend");
    assert_eq!(
        target.get_object(&object.object_id).expect("target object"),
        Some(object)
    );
    assert!(
        !root
            .join("data")
            .join("dfs")
            .join("metadata")
            .join("root")
            .join("frag")
            .exists()
    );
}

#[test]
fn daemon_dfs_metadata_journal_batch_rejects_non_primary_source() {
    let root = temp_root("daemon_dfs_metadata_journal_batch_rejects_stale_source");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_daemon_dfs_meta_local").expect("local");
    let primary = NodeId::from_legacy_named("node_daemon_dfs_meta_primary").expect("primary");
    let stale = NodeId::from_legacy_named("node_daemon_dfs_meta_stale").expect("stale");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_meta_reject")
            .expect("cluster"),
        "daemon dfs metadata reject",
        local.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    let file_system = distributed_sifs(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("metadata shard op"),
        ),
    )
    .expect("write shard config");
    let source_root = root.join("source-meta");
    let mut source = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
        .expect("source backend");
    source.put_file_system(&file_system).expect("filesystem");
    let object = distributed_sifs_bootstrap_objects(primary.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    source.put_object(&object).expect("object");
    let mut subscription = source.subscribe(0);
    let batch = source
        .poll_subscription(&mut subscription)
        .expect("source batch");

    let response = process_test_dfs_metadata_journal_batch(
        &paths,
        sinomo_dfs::DfsMetadataReplicaShardContext {
            local_node_id: local.clone(),
            shard: shard.clone(),
            pending_membership_handoff: None,
        },
        DfsMetadataJournalBatchCommand {
            request_id: "dfs_meta_req_stale_source".to_string(),
            source_node_id: Some(stale.clone()),
            file_system_id: Some(file_system.file_system_id.clone()),
            authority_epoch: Some(shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: None,
            batch,
        },
    )
    .expect("reject batch");

    assert!(matches!(
        response.result,
        DfsMetadataJournalBatchResult::Rejected { ref reason , ..}
            if reason.contains(&format!("source {stale} is not shard root primary {primary}"))
    ));
    let target = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("target backend");
    assert!(
        target
            .get_object(&object.object_id)
            .expect("target object")
            .is_none(),
        "rejected non-primary journal batch must not mutate the local replica"
    );
}

#[test]
fn daemon_dfs_status_reads_default_metadata_backend() {
    let root = temp_root("daemon_dfs_status");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = NodeId::from_legacy_named("node_daemon_dfs_status").expect("node");
    let file_system = distributed_sifs(node_id.clone());
    let object = distributed_sifs_bootstrap_objects(node_id)
        .into_iter()
        .next()
        .expect("bootstrap object");
    let mut backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        })
        .expect("version");

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("dfs status");

    assert_eq!(status.file_system_count, 1);
    assert_eq!(status.object_count, 1);
    assert_eq!(status.version_count, 1);
}

#[test]
fn daemon_dfs_maintenance_tick_resumes_namespace_jobs() {
    let root = temp_root("daemon_dfs_maintenance_namespace_jobs");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_namespace_jobs")
        .expect("cluster");
    let node_id = NodeId::from_legacy_named("node_daemon_dfs_namespace_jobs").expect("node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "daemon dfs namespace jobs",
        node_id.clone(),
        "local",
        "pk-local",
        now,
    );
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let old_dir = "/services/sandbox/instances/daemon_namespace_job/work/old";
    let new_dir = "/services/sandbox/instances/daemon_namespace_job/work/new";
    let old_file = format!("{old_dir}/nested/file.txt");
    let new_file = format!("{new_dir}/nested/file.txt");
    let job_id = {
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let metadata =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            daemon_dfs_node_transport(node_id.clone(), Vec::new()),
            content,
        )
        .with_boxed_metadata_backend(metadata)
        .expect("service backend");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap sifs");
        service
            .posix_mkdir_all(&format!("{old_dir}/nested"))
            .expect("mkdir old tree");
        service
            .posix_write_file(
                &old_file,
                b"daemon namespace job",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write file");
        let job = service
            .posix_rename_directory_bounded(old_dir, new_dir, 1)
            .expect("start bounded rename");
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        job.job_id
    };

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("status before maintenance");
    assert_eq!(status.pending_namespace_job_count, 1);
    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.pending_namespace_job_count, 1);
    assert_eq!(pending.pending_repair_count, 0);
    assert_eq!(pending.repair_attempt_count, 0);
    assert_eq!(pending.due_repair_attempt_count, 0);
    assert_eq!(pending.unattempted_repair_count, 0);
    assert_eq!(pending.next_repair_retry_unix_nanos, None);
    assert_eq!(pending.metadata_replica_lag_count, 0);
    assert_eq!(pending.pending_local_membership_handoff_count, 0);
    assert!(pending.has_work());
    assert!(pending.should_trigger_background_maintenance());

    let runtime = daemon_test_runtime("daemon-dfs-maintenance-namespace-jobs");
    run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect("maintenance tick before shard bootstrap");

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("status after deferred maintenance");
    assert_eq!(status.pending_namespace_job_count, 1);
    assert_eq!(status.failed_namespace_job_count, 0);
    assert!(
        daemon_dfs_pending_maintenance_work(&paths)
            .expect("pending maintenance after deferred tick")
            .has_work()
    );
    let backend =
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
    let deferred_job = backend
        .get_namespace_job(&job_id)
        .expect("get deferred namespace job")
        .expect("deferred namespace job");
    assert_eq!(deferred_job.status, DfsNamespaceJobStatus::Running);
    assert!(deferred_job.completed_at.is_none());
    drop(backend);

    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id,
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&root_shard.file_system_id, &root_shard.shard_id),
                &root_shard,
                None,
            )
            .expect("root metadata shard op"),
        ),
    )
    .expect("commit root metadata shard");

    run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect("maintenance tick after shard bootstrap");

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("status after configured maintenance");
    assert_eq!(status.pending_namespace_job_count, 0);
    assert_eq!(status.failed_namespace_job_count, 0);
    let pending =
        daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance after tick");
    assert!(!pending.has_work(), "unexpected pending work: {pending:#?}");
    let backend =
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
    let job = backend
        .get_namespace_job(&job_id)
        .expect("get namespace job")
        .expect("namespace job");
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(job.completed_at.is_some());

    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
    let service = NetworkedDfsService::new(daemon_dfs_node_transport(node_id, Vec::new()), content)
        .with_boxed_metadata_backend(backend)
        .expect("service backend");
    assert_eq!(
        service.posix_read_file(&new_file).expect("read moved file"),
        b"daemon namespace job"
    );
    assert!(service.posix_read_file(&old_file).is_err());
}

#[test]
fn daemon_dfs_conflict_probe_parks_an_open_base_conflict() {
    let root = temp_root("pending-conflict-classifier");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = NodeId::from_legacy_named("node_pending_conflict").expect("node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_pending_conflict").expect("cluster"),
        "pending conflict classifier",
        node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let object_id = DfsObjectId::from_legacy_named("object_pending_conflict").expect("object id");
    let base_version =
        DfsVersion::from_legacy_named("version_pending_conflict_base").expect("base version");
    let local_version =
        DfsVersion::from_legacy_named("version_pending_conflict_local").expect("local version");
    let remote_version =
        DfsVersion::from_legacy_named("version_pending_conflict_remote").expect("remote version");
    let mut object = pending_object_metadata_for_test(
        object_id.clone(),
        file_system.file_system_id.clone(),
        base_version.clone(),
        node_id.clone(),
        "/services/pending-conflict.txt",
    );
    let conflict = DfsConflictRecord {
        conflict_id: "conflict_pending_base".to_string(),
        file_system_id: file_system.file_system_id.clone(),
        object_id: object_id.clone(),
        path: object.path.clone(),
        base_version: Some(base_version),
        local_version: local_version.clone(),
        remote_version,
        detected_by_node_id: node_id,
        reason: "base remains visible".to_string(),
        local_content_hash: Some("blake3:local-pending-conflict".to_string()),
        remote_content_hash: Some("blake3:remote-pending-conflict".to_string()),
        created_by_intent_id: None,
        strategy: Some("branch".to_string()),
        status: DfsConflictStatus::Open,
        detected_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut metadata =
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata");
    metadata.put_file_system(&file_system).expect("filesystem");
    metadata.put_object(&object).expect("base object");
    metadata.put_conflict(&conflict).expect("open conflict");
    drop(metadata);

    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.actionable_conflict_count, 0);
    assert!(
        !pending.should_trigger_background_maintenance(),
        "an intentional open conflict at its base version must stay parked"
    );

    object.version = local_version;
    let mut metadata =
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata");
    metadata.put_object(&object).expect("resolved object");
    drop(metadata);
    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("resolved pending work");
    assert_eq!(pending.actionable_conflict_count, 1);
    assert!(pending.should_trigger_background_maintenance());
}

fn placement_reconciliation_job_for_test(
    state: DfsPlacementReconciliationState,
) -> DfsPlacementReconciliationJob {
    DfsPlacementReconciliationJob {
        schema_version: sinomo_dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
        job_id: "placement-probe-job".to_string(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        path: "/services".to_string(),
        scope: sinomo_dfs::DfsPlacementScope::Subtree,
        unavailable_node_ids: BTreeSet::new(),
        candidate_snapshot: Vec::new(),
        placement_snapshot: None,
        adaptive: false,
        adaptive_budget: None,
        processed_content_bytes: 0,
        accepted_config: None,
        state,
        cursor: None,
        total_object_count: 0,
        scanned_object_count: 0,
        matched_object_count: 0,
        updated_object_count: 0,
        scheduled_repair_count: 0,
        completed_repair_count: 0,
        pending_repair_count: 0,
        revision: 0,
        created_at_unix_nanos: 1,
        updated_at_unix_nanos: 1,
        last_error: None,
    }
}

#[test]
fn daemon_dfs_placement_probe_triggers_new_work_but_parks_offline_repairs() {
    let pending = placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Pending);
    assert_eq!(
        daemon_dfs_actionable_placement_reconciliation_count(&[pending], 0),
        1
    );
    let scanning = placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Scanning);
    assert_eq!(
        daemon_dfs_actionable_placement_reconciliation_count(&[scanning], 0),
        1
    );
    let mut repairing =
        placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Repairing);
    assert_eq!(
        daemon_dfs_actionable_placement_reconciliation_count(std::slice::from_ref(&repairing), 3,),
        1,
        "unrelated repair backlog must not stall a fresh Repairing scan"
    );
    repairing.pending_repair_count = 1;
    assert_eq!(
        daemon_dfs_actionable_placement_reconciliation_count(std::slice::from_ref(&repairing), 3,),
        0,
        "a completed Repairing scan with offline work must stay parked"
    );
    assert_eq!(
        daemon_dfs_actionable_placement_reconciliation_count(&[repairing], 0),
        1,
        "a repair-free job needs one final pass to CAS Completed"
    );
}

#[test]
fn daemon_dfs_reactive_probe_never_opens_the_metadata_or_content_inventory() {
    let root = temp_root("reactive-probe-skips-inventory");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = NodeId::from_legacy_named("node_reactive_probe").expect("node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_reactive_probe").expect("cluster"),
        "reactive probe",
        node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let corrupt_metadata = data_dir
        .dfs_dir()
        .join("metadata")
        .join("root")
        .join("metadata-tables.snapshot.sobj");
    fs::create_dir_all(corrupt_metadata.parent().expect("metadata parent"))
        .expect("create corrupt metadata parent");
    fs::write(&corrupt_metadata, b"must not be opened by reactive probe")
        .expect("write corrupt metadata");
    let corrupt_manifest = data_dir
        .dfs_dir()
        .join("content")
        .join("objects")
        .join("ignored")
        .join("ignored")
        .join("manifest.sobj");
    fs::create_dir_all(corrupt_manifest.parent().expect("manifest parent"))
        .expect("create corrupt manifest parent");
    fs::write(&corrupt_manifest, b"must not be opened by reactive probe")
        .expect("write corrupt manifest");

    let pending = daemon_dfs_pending_reactive_maintenance_work(&paths).expect("reactive probe");
    assert_eq!(pending, DaemonDfsPendingMaintenanceWork::default());
    assert!(
        !daemon_dfs_repair_queue_work_is_known(&paths),
        "an empty startup queue must not make connectivity refresh launch DFS maintenance"
    );

    let retry_target =
        NodeId::from_legacy_named("node_reactive_retry_target").expect("retry target");
    let new_target = NodeId::from_legacy_named("node_reactive_new_target").expect("new target");
    let now = daemon_dfs_now_unix_nanos();
    let preserved_retry = DfsRepairAttemptRecord {
        intent_id: "intent-reactive-retry".to_string(),
        object_id: DfsObjectId::from_legacy_named("object_reactive_retry").expect("retry object"),
        version: DfsVersion::from_legacy_named("version_reactive_retry").expect("retry version"),
        content_hash: "blake3:reactive-retry".to_string(),
        missing_replica_node_id: retry_target,
        attempts: 3,
        in_flight: false,
        last_attempt_unix_nanos: now.saturating_sub(1),
        next_retry_unix_nanos: now.saturating_add(1_000_000_000),
        transfer_request_id: None,
        last_error: Some("retry later".to_string()),
    };
    write_dfs_repair_attempt_snapshot(
        &default_dfs_repair_attempt_snapshot_path(&data_dir),
        &DfsRepairAttemptSnapshot {
            records: vec![preserved_retry.clone()],
        },
    )
    .expect("initial repair queue");
    let new_repair = DfsRepairAttemptRecord {
        intent_id: "intent-reactive-new".to_string(),
        object_id: DfsObjectId::from_legacy_named("object_reactive_new").expect("new object"),
        version: DfsVersion::from_legacy_named("version_reactive_new").expect("new version"),
        content_hash: "blake3:reactive-new".to_string(),
        missing_replica_node_id: new_target,
        attempts: 0,
        in_flight: false,
        last_attempt_unix_nanos: 0,
        next_retry_unix_nanos: 0,
        transfer_request_id: None,
        last_error: None,
    };
    let mut stale_retry = preserved_retry.clone();
    stale_retry.attempts = 0;
    stale_retry.next_retry_unix_nanos = 0;
    persist_daemon_dfs_repair_queue_records(&data_dir, vec![stale_retry, new_repair])
        .expect("merge committed repair queue");

    let snapshot =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))
            .expect("read merged repair queue")
            .expect("merged repair queue");
    assert_eq!(snapshot.records.len(), 2);
    assert!(
        snapshot
            .records
            .iter()
            .any(|record| record == &preserved_retry),
        "committed queue persistence must not regress existing retry/backoff state"
    );
    let pending =
        daemon_dfs_pending_reactive_maintenance_work(&paths).expect("queued reactive probe");
    assert_eq!(pending.pending_repair_count, 2);
    assert_eq!(pending.due_repair_attempt_count, 1);
    assert!(pending.should_trigger_background_maintenance());
    assert!(
        daemon_dfs_repair_queue_work_is_known(&paths),
        "the bounded queue sample must retain blocker-clear wake eligibility"
    );
}

#[test]
fn daemon_dfs_pending_probe_reads_placement_jobs_from_cluster_kv() {
    let root = temp_root("pending-placement-classifier");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let node_id = NodeId::from_legacy_named("node_pending_placement").expect("node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_pending_placement").expect("cluster"),
        "pending placement classifier",
        node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    let job = placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Pending);
    append_cluster_raft_command(
        &cluster_paths,
        &node_id,
        sinomo_core::kv::KvCommand::Op(
            KvOp::put_serde(
                sinomo_dfs::placement_reconciliation_key(&job.file_system_id, &job.job_id),
                &job,
                None,
            )
            .expect("placement job op"),
        ),
    )
    .expect("placement job");

    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.actionable_placement_reconciliation_count, 1);
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_pending_repair_backlog_in_backoff_does_not_force_triggered_loop() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_write_count: 0,
        pending_repair_count: 8,
        repair_attempt_count: 8,
        due_repair_attempt_count: 0,
        unattempted_repair_count: 0,
        next_repair_retry_unix_nanos: Some(42),
        pending_namespace_job_count: 0,
        actionable_conflict_count: 0,
        actionable_placement_reconciliation_count: 0,
        metadata_replica_lag_count: 0,
        pending_local_membership_handoff_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(!pending.should_trigger_background_maintenance());
}

fn pending_work_connectivity_record(
    cluster_id: &ClusterId,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &str,
    state: ClusterConnectivityState,
) -> ClusterConnectivityRecord {
    let now = OffsetDateTime::now_utc();
    ClusterConnectivityRecord {
        cluster_id: cluster_id.clone(),
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        endpoint_id: ClusterEndpointId::from_legacy_named(endpoint_id).expect("endpoint id"),
        transport: ClusterTransportKind::LanTcp,
        address: format!("{}.example.test:21353", target_node_id.as_str()),
        latency_ms: (state == ClusterConnectivityState::Healthy).then_some(1),
        consecutive_failures: if state == ClusterConnectivityState::Unreachable {
            1
        } else {
            0
        },
        last_success_at: None,
        last_failure_at: None,
        failure_reason: None,
        state,
        observed_at: now,
        expires_at: now + time::Duration::minutes(5),
    }
}

#[test]
fn daemon_dfs_offline_node_with_only_degraded_route_is_parked() {
    let cluster_id = ClusterId::from_legacy_named("cluster_pending_degraded").expect("cluster id");
    let local_node_id =
        NodeId::from_legacy_named("node_pending_degraded_local").expect("local node");
    let target_node_id =
        NodeId::from_legacy_named("node_pending_degraded_target").expect("target node");
    let connectivity = ClusterConnectivityGraph::from_records(
        local_node_id.clone(),
        [pending_work_connectivity_record(
            &cluster_id,
            &local_node_id,
            &target_node_id,
            "endpoint_pending_degraded_direct",
            ClusterConnectivityState::Degraded,
        )],
    );

    assert!(
        cluster_route_next_hop(&connectivity, &local_node_id, &target_node_id).is_some(),
        "the fixture must retain the stale degraded route"
    );
    assert!(daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Offline,
        &connectivity,
        &local_node_id,
        &target_node_id,
    ));
    assert!(!daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Online,
        &connectivity,
        &local_node_id,
        &target_node_id,
    ));
}

#[test]
fn daemon_dfs_relay_routable_node_is_not_treated_as_offline() {
    let cluster_id = ClusterId::from_legacy_named("cluster_pending_routed").expect("cluster id");
    let local_node_id = NodeId::from_legacy_named("node_pending_routed_local").expect("local node");
    let bridge_node_id =
        NodeId::from_legacy_named("node_pending_routed_bridge").expect("bridge node");
    let target_node_id =
        NodeId::from_legacy_named("node_pending_routed_target").expect("target node");
    let connectivity = ClusterConnectivityGraph::from_records(
        local_node_id.clone(),
        [
            pending_work_connectivity_record(
                &cluster_id,
                &local_node_id,
                &target_node_id,
                "endpoint_pending_routed_direct",
                ClusterConnectivityState::Unreachable,
            ),
            pending_work_connectivity_record(
                &cluster_id,
                &local_node_id,
                &bridge_node_id,
                "endpoint_pending_routed_bridge",
                ClusterConnectivityState::Healthy,
            ),
            pending_work_connectivity_record(
                &cluster_id,
                &bridge_node_id,
                &target_node_id,
                "endpoint_pending_routed_target",
                ClusterConnectivityState::Healthy,
            ),
        ],
    );

    assert!(!daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Offline,
        &connectivity,
        &local_node_id,
        &target_node_id,
    ));

    let unroutable = ClusterConnectivityGraph::from_records(
        local_node_id.clone(),
        [pending_work_connectivity_record(
            &cluster_id,
            &local_node_id,
            &target_node_id,
            "endpoint_pending_unroutable_direct",
            ClusterConnectivityState::Unreachable,
        )],
    );
    assert!(daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Offline,
        &unroutable,
        &local_node_id,
        &target_node_id,
    ));
    assert!(!daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Offline,
        &unroutable,
        &local_node_id,
        &local_node_id,
    ));
    assert!(daemon_dfs_node_is_offline_for_maintenance(
        &ClusterNodeAvailability::Unknown,
        &unroutable,
        &local_node_id,
        &target_node_id,
    ));
}

fn pending_write_intent_for_test(
    intent_id: &str,
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
    target_version: DfsVersion,
    coordinator_node_id: NodeId,
    desired_replica_node_ids: Vec<NodeId>,
    min_commit_replicas: usize,
) -> DfsWriteIntentRecord {
    DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: intent_id.to_string(),
        file_system_id,
        object_id,
        base_version: None,
        target_version,
        coordinator_node_id,
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        policy: DfsReplicationPolicy::new(min_commit_replicas, desired_replica_node_ids.len())
            .expect("replication policy"),
        desired_replica_node_ids,
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:pending-write".to_string()),
        object_metadata: None,
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    }
}

fn pending_write_receipt_for_test(
    intent: &DfsWriteIntentRecord,
    replica_node_id: NodeId,
    event_id: &str,
) -> DfsReplicaReceiptRecord {
    DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            committed_version: intent.target_version.clone(),
            owner_node_id: intent.coordinator_node_id.clone(),
            event_id: EventId::from_legacy_named(event_id).expect("event id"),
        },
        replica_node_id,
        content_hash: intent.content_hash.clone(),
        received_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn pending_object_metadata_for_test(
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    version: DfsVersion,
    owner_node_id: NodeId,
    path: &str,
) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id,
        file_system_id,
        path: path.to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner_node_id.clone(),
        current_locations: vec![owner_node_id],
        content_hash: Some("blake3:pending-base".to_string()),
        content_size_bytes: Some(1),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
        tombstone_provenance: None,
    }
}

#[test]
fn daemon_dfs_explicitly_offline_replica_backlog_stays_durable_without_triggering() {
    let local_node_id =
        NodeId::from_legacy_named("node_pending_offline_local").expect("local node");
    let offline_node_id =
        NodeId::from_legacy_named("node_pending_offline_target").expect("offline node");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let object_id = DfsObjectId::from_legacy_named("object_pending_offline").expect("object id");
    let version = DfsVersion::from_legacy_named("version_pending_offline").expect("version");
    let intent = pending_write_intent_for_test(
        "intent-active-pending-offline",
        file_system_id.clone(),
        object_id.clone(),
        version.clone(),
        local_node_id.clone(),
        vec![local_node_id.clone(), offline_node_id.clone()],
        2,
    );
    let receipt = pending_write_receipt_for_test(
        &intent,
        local_node_id.clone(),
        "event_pending_offline_local_receipt",
    );
    let repair = DfsOperatorRepairView {
        intent_id: "intent-pending-offline".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: Some("blake3:pending-offline".to_string()),
        missing_replica_node_id: offline_node_id.clone(),
    };
    let attempt = DfsRepairAttemptRecord {
        intent_id: repair.intent_id.clone(),
        object_id: object_id.clone(),
        version: version.clone(),
        content_hash: "blake3:pending-offline".to_string(),
        missing_replica_node_id: offline_node_id.clone(),
        attempts: 3,
        in_flight: false,
        last_attempt_unix_nanos: 1,
        next_retry_unix_nanos: 2,
        transfer_request_id: None,
        last_error: Some("offline".to_string()),
    };
    let shard_status = DfsMetadataShardStatus {
        shard_id: DfsMetadataShardId::new("root"),
        file_system_id: Some(file_system_id),
        primary_node_id: Some(local_node_id.clone()),
        replica_node_ids: vec![offline_node_id.clone()],
        epoch: Some(1),
        journal_next_sequence: 10,
        journal_last_sequence: 9,
        replica_lag: vec![DfsMetadataReplicaLagStatus {
            target_node_id: offline_node_id.clone(),
            replicated_through_sequence: 1,
            lag_entries: 8,
        }],
        replay_cursors: Vec::new(),
    };

    let pending = daemon_dfs_pending_work_from_records(
        1,
        1,
        0,
        0,
        0,
        &[intent],
        &[receipt],
        &[shard_status],
        &[repair],
        &[attempt],
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::from([offline_node_id]),
        &local_node_id,
        0,
        3,
    );

    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.pending_repair_count, 1);
    assert_eq!(pending.repair_attempt_count, 1);
    assert_eq!(pending.due_repair_attempt_count, 0);
    assert_eq!(pending.unattempted_repair_count, 0);
    assert_eq!(pending.next_repair_retry_unix_nanos, None);
    assert_eq!(pending.metadata_replica_lag_count, 0);
    assert!(
        pending.has_work(),
        "the durable repair backlog remains visible"
    );
    assert!(!pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_non_offline_replica_backlog_still_triggers() {
    let local_node_id = NodeId::from_legacy_named("node_pending_online_local").expect("local node");
    let target_node_id =
        NodeId::from_legacy_named("node_pending_online_target").expect("target node");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let object_id = DfsObjectId::from_legacy_named("object_pending_online").expect("object id");
    let version = DfsVersion::from_legacy_named("version_pending_online").expect("version");
    let intent = pending_write_intent_for_test(
        "intent-active-pending-online",
        file_system_id.clone(),
        object_id.clone(),
        version.clone(),
        local_node_id.clone(),
        vec![local_node_id.clone(), target_node_id.clone()],
        2,
    );
    let receipt = pending_write_receipt_for_test(
        &intent,
        local_node_id.clone(),
        "event_pending_online_local_receipt",
    );
    let repair = DfsOperatorRepairView {
        intent_id: "intent-pending-online".to_string(),
        file_system_id: file_system_id.clone(),
        object_id,
        version,
        content_hash: Some("blake3:pending-online".to_string()),
        missing_replica_node_id: target_node_id.clone(),
    };
    let shard_status = DfsMetadataShardStatus {
        shard_id: DfsMetadataShardId::new("root"),
        file_system_id: Some(file_system_id),
        primary_node_id: Some(local_node_id.clone()),
        replica_node_ids: vec![target_node_id.clone()],
        epoch: Some(1),
        journal_next_sequence: 10,
        journal_last_sequence: 9,
        replica_lag: vec![DfsMetadataReplicaLagStatus {
            target_node_id,
            replicated_through_sequence: 1,
            lag_entries: 8,
        }],
        replay_cursors: Vec::new(),
    };

    let pending = daemon_dfs_pending_work_from_records(
        1,
        1,
        0,
        0,
        0,
        &[intent],
        &[receipt],
        &[shard_status],
        &[repair],
        &[],
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &local_node_id,
        0,
        3,
    );

    assert_eq!(pending.pending_write_count, 1);
    assert_eq!(pending.unattempted_repair_count, 1);
    assert_eq!(pending.metadata_replica_lag_count, 1);
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_inflight_repair_projects_retry_deadline() {
    let local_node_id =
        NodeId::from_legacy_named("node_inflight_repair_local").expect("local node");
    let target_node_id =
        NodeId::from_legacy_named("node_inflight_repair_target").expect("target node");
    let repair = DfsOperatorRepairView {
        intent_id: "intent-inflight-repair".to_string(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("object_inflight_repair").expect("object id"),
        version: DfsVersion::from_legacy_named("version_inflight_repair").expect("version"),
        content_hash: Some("blake3:inflight-repair".to_string()),
        missing_replica_node_id: target_node_id.clone(),
    };
    let attempt = DfsRepairAttemptRecord {
        intent_id: repair.intent_id.clone(),
        object_id: repair.object_id.clone(),
        version: repair.version.clone(),
        content_hash: repair.content_hash.clone().expect("content hash"),
        missing_replica_node_id: target_node_id,
        attempts: 1,
        in_flight: true,
        last_attempt_unix_nanos: 1,
        next_retry_unix_nanos: 100,
        transfer_request_id: Some("repair-inflight-request".to_string()),
        last_error: None,
    };
    let project = |now_unix_nanos| {
        daemon_dfs_pending_work_from_records(
            0,
            1,
            0,
            0,
            0,
            &[],
            &[],
            &[],
            std::slice::from_ref(&repair),
            std::slice::from_ref(&attempt),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            now_unix_nanos,
        )
    };

    let waiting = project(99);
    assert_eq!(waiting.due_repair_attempt_count, 0);
    assert_eq!(waiting.unattempted_repair_count, 0);
    assert_eq!(waiting.next_repair_retry_unix_nanos, Some(100));
    assert!(!waiting.should_trigger_background_maintenance());
    assert!(
        !waiting.has_deferred_work(),
        "an in-flight transfer timeout is scheduled work, not parked work"
    );

    let due = project(100);
    assert_eq!(due.due_repair_attempt_count, 1);
    assert_eq!(due.next_repair_retry_unix_nanos, None);
    assert!(due.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_missing_orphan_does_not_hide_valid_repair_trigger() {
    let root = temp_root("pending-missing-orphan-valid-repair");
    let data_dir = SinomoDataDir::new(root.path());
    let metadata = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.path().join("metadata"),
    )
    .expect("metadata backend");
    let local_node_id = NodeId::from_legacy_named("node_pending_orphan_local").expect("local node");
    let target_node_id =
        NodeId::from_legacy_named("node_pending_orphan_healthy").expect("target node");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let orphan_object_id =
        DfsObjectId::from_legacy_named("object_pending_orphan").expect("object id");
    let orphan_version = DfsVersion::from_legacy_named("version_pending_orphan").expect("version");
    let mut orphan = pending_write_intent_for_test(
        "intent-pending-orphan",
        file_system_id.clone(),
        orphan_object_id,
        orphan_version,
        local_node_id.clone(),
        vec![local_node_id.clone(), target_node_id.clone()],
        2,
    );
    orphan.content_hash = Some(format!("blake3:{}", "0".repeat(64)));
    let repair = DfsOperatorRepairView {
        intent_id: "intent-pending-valid-repair".to_string(),
        file_system_id,
        object_id: DfsObjectId::from_legacy_named("object_pending_valid_repair")
            .expect("repair object"),
        version: DfsVersion::from_legacy_named("version_pending_valid_repair")
            .expect("repair version"),
        content_hash: Some("blake3:pending-valid-repair".to_string()),
        missing_replica_node_id: target_node_id,
    };
    let intents_without_recovery_source = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&orphan),
        &[],
        &BTreeSet::new(),
        &local_node_id,
    );
    assert_eq!(
        intents_without_recovery_source,
        BTreeSet::from([orphan.intent_id.clone()])
    );

    let pending = daemon_dfs_pending_work_from_records(
        1,
        1,
        0,
        0,
        0,
        &[orphan],
        &[],
        &[],
        &[repair],
        &[],
        &intents_without_recovery_source,
        &BTreeSet::new(),
        &BTreeSet::new(),
        &local_node_id,
        0,
        3,
    );

    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.unattempted_repair_count, 1);
    assert!(
        pending.should_trigger_background_maintenance(),
        "the executable healthy repair must retain the short cadence"
    );

    let orphan_only = daemon_dfs_pending_work_from_records(
        1,
        0,
        0,
        0,
        0,
        &[pending_write_intent_for_test(
            "intent-pending-orphan",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_orphan_only").expect("object"),
            DfsVersion::from_legacy_named("version_pending_orphan_only").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone()],
            1,
        )],
        &[],
        &[],
        &[],
        &[],
        &intents_without_recovery_source,
        &BTreeSet::new(),
        &BTreeSet::new(),
        &local_node_id,
        0,
        3,
    );
    assert!(!orphan_only.should_trigger_background_maintenance());
}

#[test]
fn source_less_write_intent_projects_exact_stale_deadline() {
    let local_node_id = NodeId::from_legacy_named("node_pending_stale_local").expect("local node");
    let intent = pending_write_intent_for_test(
        "intent-pending-stale",
        sinomo_dfs::sifs_file_system_id(),
        DfsObjectId::from_legacy_named("object_pending_stale").expect("object"),
        DfsVersion::from_legacy_named("version_pending_stale").expect("version"),
        local_node_id.clone(),
        vec![local_node_id.clone()],
        1,
    );
    let source_less = BTreeSet::from([intent.intent_id.clone()]);
    let stale_at = daemon_dfs_duration_nanos(sinomo_dfs::DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE);
    let project = |now_unix_nanos| {
        daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            0,
            0,
            std::slice::from_ref(&intent),
            &[],
            &[],
            &[],
            &[],
            &source_less,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            now_unix_nanos,
        )
    };

    let future = project(stale_at.saturating_sub(1));
    assert_eq!(future.pending_write_count, 0);
    assert_eq!(
        future.next_time_based_maintenance_unix_nanos,
        Some(stale_at)
    );
    assert!(!future.should_trigger_background_maintenance());
    assert!(
        !future.has_deferred_work(),
        "an exact stale deadline is scheduled work, not parked work"
    );

    let due = project(stale_at);
    assert_eq!(due.due_time_based_maintenance_count, 1);
    assert!(due.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_offline_only_receipt_below_quorum_is_not_executable_work() {
    let root = temp_root("pending-offline-only-source");
    let data_dir = SinomoDataDir::new(root.path());
    let mut metadata = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.path().join("metadata"),
    )
    .expect("metadata backend");
    let local_node_id = NodeId::from_legacy_named("node_pending_source_local").expect("local node");
    let offline_node_id =
        NodeId::from_legacy_named("node_pending_source_offline").expect("offline node");
    let healthy_target_node_id =
        NodeId::from_legacy_named("node_pending_source_healthy").expect("healthy node");
    let mut intent = pending_write_intent_for_test(
        "intent-pending-offline-source",
        sinomo_dfs::sifs_file_system_id(),
        DfsObjectId::from_legacy_named("object_pending_offline_source").expect("object"),
        DfsVersion::from_legacy_named("version_pending_offline_source").expect("version"),
        local_node_id.clone(),
        vec![
            local_node_id.clone(),
            offline_node_id.clone(),
            healthy_target_node_id.clone(),
        ],
        2,
    );
    intent.content_hash = Some(format!("blake3:{}", "1".repeat(64)));
    let offline_receipt = pending_write_receipt_for_test(
        &intent,
        offline_node_id.clone(),
        "event_pending_offline_source",
    );
    let offline_node_ids = BTreeSet::from([offline_node_id.clone()]);

    let source_less = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&offline_receipt),
        &offline_node_ids,
        &local_node_id,
    );
    assert_eq!(source_less, BTreeSet::from([intent.intent_id.clone()]));

    let reachable_source = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&offline_receipt),
        &BTreeSet::new(),
        &local_node_id,
    );
    assert_eq!(
        reachable_source,
        BTreeSet::from([intent.intent_id.clone()]),
        "a reachable receipt cannot hydrate an invisible create before a pending write exists"
    );

    let second_offline_receipt = pending_write_receipt_for_test(
        &intent,
        healthy_target_node_id.clone(),
        "event_pending_second_offline_source",
    );
    let quorum_offline_sources = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        &[offline_receipt.clone(), second_offline_receipt],
        &BTreeSet::from([offline_node_id.clone(), healthy_target_node_id.clone()]),
        &local_node_id,
    );
    assert_eq!(
        quorum_offline_sources,
        BTreeSet::from([intent.intent_id.clone()]),
        "quorum proof does not replace an accessible content or size source"
    );

    let repair = DfsOperatorRepairView {
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        version: intent.target_version.clone(),
        content_hash: intent.content_hash.clone(),
        missing_replica_node_id: healthy_target_node_id,
    };
    assert!(
        daemon_dfs_repairs_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&repair),
            std::slice::from_ref(&offline_receipt),
            &offline_node_ids,
            &local_node_id,
        )
        .is_empty(),
        "a concurrently removed repair object remains actionable for stale-record cleanup"
    );
    metadata
        .put_file_system(&distributed_sifs(local_node_id.clone()))
        .expect("filesystem");
    metadata
        .put_object(&DfsObjectMetadata {
            object_id: repair.object_id.clone(),
            file_system_id: repair.file_system_id.clone(),
            path: "/pending/offline-source".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: local_node_id.clone(),
            current_locations: vec![offline_node_id.clone()],
            content_hash: repair.content_hash.clone(),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: repair.version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
            tombstone_provenance: None,
        })
        .expect("repair object");
    let repairs_without_recovery_source = daemon_dfs_repairs_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&repair),
        std::slice::from_ref(&offline_receipt),
        &offline_node_ids,
        &local_node_id,
    );
    assert_eq!(
        repairs_without_recovery_source,
        BTreeSet::from([(
            repair.intent_id.clone(),
            repair.missing_replica_node_id.clone(),
        )])
    );
    assert!(
        daemon_dfs_repairs_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&repair),
            std::slice::from_ref(&offline_receipt),
            &BTreeSet::new(),
            &local_node_id,
        )
        .is_empty(),
        "a reachable exact-version receipt is an executable repair source"
    );

    let pending = daemon_dfs_pending_work_from_records(
        1,
        1,
        0,
        0,
        0,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&offline_receipt),
        &[],
        &[repair],
        &[],
        &source_less,
        &repairs_without_recovery_source,
        &offline_node_ids,
        &local_node_id,
        0,
        3,
    );
    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.unattempted_repair_count, 0);
    assert!(!pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_stale_local_quorum_receipt_without_content_is_parked() {
    let root = temp_root("pending-stale-local-quorum");
    let data_dir = SinomoDataDir::new(root.path());
    let mut metadata = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.path().join("metadata"),
    )
    .expect("metadata backend");
    let local_node_id = NodeId::from_legacy_named("node_pending_stale_local").expect("local node");
    let mut intent = pending_write_intent_for_test(
        "intent-pending-stale-local-quorum",
        sinomo_dfs::sifs_file_system_id(),
        DfsObjectId::from_legacy_named("object_pending_stale_local").expect("object"),
        DfsVersion::from_legacy_named("version_pending_stale_local").expect("version"),
        local_node_id.clone(),
        vec![local_node_id.clone()],
        1,
    );
    let base_version =
        DfsVersion::from_legacy_named("version_pending_stale_local_base").expect("base version");
    intent.base_version = Some(base_version.clone());
    intent.object_metadata = Some(pending_object_metadata_for_test(
        intent.object_id.clone(),
        intent.file_system_id.clone(),
        base_version,
        local_node_id.clone(),
        "/services/pending-stale-local.txt",
    ));
    intent.content_hash = Some(format!("blake3:{}", "2".repeat(64)));
    let local_receipt =
        pending_write_receipt_for_test(&intent, local_node_id.clone(), "event_pending_stale_local");

    let source_less = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&local_receipt),
        &BTreeSet::new(),
        &local_node_id,
    );
    assert_eq!(source_less, BTreeSet::from([intent.intent_id.clone()]));

    let pending = daemon_dfs_pending_work_from_records(
        1,
        0,
        0,
        0,
        0,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&local_receipt),
        &[],
        &[],
        &[],
        &source_less,
        &BTreeSet::new(),
        &BTreeSet::new(),
        &local_node_id,
        0,
        3,
    );
    assert_eq!(pending.pending_write_count, 0);
    assert!(!pending.should_trigger_background_maintenance());

    metadata
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            version: intent.target_version.clone(),
            content_hash: intent.content_hash.clone(),
            size_bytes: Some(1),
            locations: vec![local_node_id.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: false,
        })
        .expect("durable target size hint");
    let create_metadata = intent.object_metadata.take().expect("create metadata");
    let no_create_metadata = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&local_receipt),
        &BTreeSet::new(),
        &local_node_id,
    );
    assert_eq!(
        no_create_metadata,
        BTreeSet::from([intent.intent_id.clone()]),
        "receipt quorum and a size hint cannot reconstruct missing create metadata"
    );
    intent.object_metadata = Some(create_metadata);
    assert!(
        daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&local_receipt),
            &BTreeSet::new(),
            &local_node_id,
        )
        .is_empty(),
        "valid create metadata plus quorum and a durable size hint is promotable"
    );

    intent.content_hash = None;
    let no_hash_source_less = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        &[],
        &BTreeSet::new(),
        &local_node_id,
    );
    assert_eq!(
        no_hash_source_less,
        BTreeSet::from([intent.intent_id]),
        "a non-visible no-hash intent has no hydration recovery path"
    );
}

#[test]
fn daemon_dfs_extent_backed_pending_write_is_an_executable_local_source() {
    let root = temp_root("pending-extent-source");
    let data_dir = SinomoDataDir::new(root.path());
    let mut metadata = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.path().join("metadata"),
    )
    .expect("metadata backend");
    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
    let local_node_id = NodeId::from_legacy_named("node_pending_extent_local").expect("local node");
    let target_node_id =
        NodeId::from_legacy_named("node_pending_extent_target").expect("target node");
    let object_id = DfsObjectId::from_legacy_named("object_pending_extent").expect("object id");
    let base_version =
        DfsVersion::from_legacy_named("version_pending_extent_base").expect("base version");
    let target_version =
        DfsVersion::from_legacy_named("version_pending_extent_target").expect("target version");
    metadata
        .put_file_system(&distributed_sifs(local_node_id.clone()))
        .expect("filesystem");
    metadata
        .put_object(&pending_object_metadata_for_test(
            object_id.clone(),
            sinomo_dfs::sifs_file_system_id(),
            base_version.clone(),
            local_node_id.clone(),
            "/pending/extent-update.txt",
        ))
        .expect("base object");
    content
        .put_object_version(object_id.as_str(), base_version.as_str(), b"abcdefgh")
        .expect("base object version");
    let patched = content
        .put_object_version_patch(
            object_id.as_str(),
            base_version.as_str(),
            target_version.as_str(),
            2,
            b"ZZ",
        )
        .expect("extent-backed target version");
    assert!(!patched.extents.is_empty());
    let mut intent = pending_write_intent_for_test(
        "intent-pending-extent-source",
        sinomo_dfs::sifs_file_system_id(),
        object_id,
        target_version,
        local_node_id.clone(),
        vec![local_node_id.clone(), target_node_id.clone()],
        2,
    );
    intent.content_hash = Some(patched.content_hash);

    assert!(
        daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            &[],
            &BTreeSet::new(),
            &local_node_id,
        )
        .is_empty()
    );
    assert!(
        daemon_dfs_repairs_without_recovery_source(
            &data_dir,
            &metadata,
            &[DfsOperatorRepairView {
                intent_id: intent.intent_id,
                file_system_id: intent.file_system_id,
                object_id: intent.object_id,
                version: intent.target_version,
                content_hash: intent.content_hash,
                missing_replica_node_id: target_node_id,
            }],
            &[],
            &BTreeSet::new(),
            &local_node_id,
        )
        .is_empty(),
        "the extent-backed local version is also an executable repair source"
    );
}

#[test]
fn daemon_dfs_quorum_ready_write_still_triggers_when_remaining_replica_is_offline() {
    let root = temp_root("pending-quorum-ready-update");
    let data_dir = SinomoDataDir::new(root.path());
    let mut metadata = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.path().join("metadata"),
    )
    .expect("metadata backend");
    let local_node_id = NodeId::from_legacy_named("node_pending_quorum_local").expect("local node");
    let offline_node_id =
        NodeId::from_legacy_named("node_pending_quorum_offline").expect("offline node");
    let object_id =
        DfsObjectId::from_legacy_named("object_pending_quorum_ready").expect("object id");
    let base_version =
        DfsVersion::from_legacy_named("version_pending_quorum_base").expect("base version");
    metadata
        .put_file_system(&distributed_sifs(local_node_id.clone()))
        .expect("filesystem");
    metadata
        .put_object(&pending_object_metadata_for_test(
            object_id.clone(),
            sinomo_dfs::sifs_file_system_id(),
            base_version.clone(),
            local_node_id.clone(),
            "/pending/quorum-update.txt",
        ))
        .expect("base object");
    let mut intent = pending_write_intent_for_test(
        "intent-pending-quorum-ready",
        sinomo_dfs::sifs_file_system_id(),
        object_id,
        DfsVersion::from_legacy_named("version_pending_quorum_ready").expect("version"),
        local_node_id.clone(),
        vec![local_node_id.clone(), offline_node_id.clone()],
        1,
    );
    intent.base_version = Some(base_version);
    let receipt = pending_write_receipt_for_test(
        &intent,
        local_node_id.clone(),
        "event_pending_quorum_ready_receipt",
    );
    let source_less = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &metadata,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&receipt),
        &BTreeSet::from([offline_node_id.clone()]),
        &local_node_id,
    );
    assert!(
        source_less.is_empty(),
        "a visible base lets receipt quorum promote an update without a target size hint"
    );

    let pending = daemon_dfs_pending_work_from_records(
        1,
        0,
        0,
        0,
        0,
        std::slice::from_ref(&intent),
        std::slice::from_ref(&receipt),
        &[],
        &[],
        &[],
        &source_less,
        &BTreeSet::new(),
        &BTreeSet::from([offline_node_id]),
        &local_node_id,
        0,
        3,
    );

    assert_eq!(pending.pending_write_count, 1);
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_wrong_hash_receipt_does_not_hide_actionable_pending_write() {
    let local_node_id = NodeId::from_legacy_named("node_pending_hash_local").expect("local node");
    let offline_node_id =
        NodeId::from_legacy_named("node_pending_hash_offline").expect("offline node");
    let intent = pending_write_intent_for_test(
        "intent-pending-wrong-hash",
        sinomo_dfs::sifs_file_system_id(),
        DfsObjectId::from_legacy_named("object_pending_wrong_hash").expect("object id"),
        DfsVersion::from_legacy_named("version_pending_wrong_hash").expect("version"),
        local_node_id.clone(),
        vec![local_node_id.clone(), offline_node_id.clone()],
        2,
    );
    let mut wrong_hash_receipt = pending_write_receipt_for_test(
        &intent,
        local_node_id.clone(),
        "event_pending_wrong_hash_receipt",
    );
    wrong_hash_receipt.content_hash = Some("blake3:different-content".to_string());

    let pending = daemon_dfs_pending_work_from_records(
        1,
        0,
        0,
        0,
        0,
        &[intent],
        &[wrong_hash_receipt],
        &[],
        &[],
        &[],
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::from([offline_node_id]),
        &local_node_id,
        0,
        3,
    );

    assert_eq!(pending.pending_write_count, 1);
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_due_repair_backlog_forces_triggered_loop() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_write_count: 0,
        pending_repair_count: 8,
        repair_attempt_count: 8,
        due_repair_attempt_count: 1,
        unattempted_repair_count: 0,
        next_repair_retry_unix_nanos: None,
        pending_namespace_job_count: 0,
        actionable_conflict_count: 0,
        actionable_placement_reconciliation_count: 0,
        metadata_replica_lag_count: 0,
        pending_local_membership_handoff_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_unattempted_repair_backlog_forces_triggered_loop() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_write_count: 0,
        pending_repair_count: 8,
        repair_attempt_count: 4,
        due_repair_attempt_count: 0,
        unattempted_repair_count: 4,
        next_repair_retry_unix_nanos: Some(42),
        pending_namespace_job_count: 0,
        actionable_conflict_count: 0,
        actionable_placement_reconciliation_count: 0,
        metadata_replica_lag_count: 0,
        pending_local_membership_handoff_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_pending_writes_force_triggered_loop() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_write_count: 8,
        pending_repair_count: 0,
        repair_attempt_count: 0,
        due_repair_attempt_count: 0,
        unattempted_repair_count: 0,
        next_repair_retry_unix_nanos: None,
        pending_namespace_job_count: 0,
        actionable_conflict_count: 0,
        actionable_placement_reconciliation_count: 0,
        metadata_replica_lag_count: 0,
        pending_local_membership_handoff_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_metadata_replica_lag_stays_owned_by_exact_lane() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_write_count: 0,
        pending_repair_count: 0,
        repair_attempt_count: 0,
        due_repair_attempt_count: 0,
        unattempted_repair_count: 0,
        next_repair_retry_unix_nanos: None,
        pending_namespace_job_count: 0,
        actionable_conflict_count: 0,
        actionable_placement_reconciliation_count: 0,
        metadata_replica_lag_count: 1,
        pending_local_membership_handoff_count: 0,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(!pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_local_membership_handoff_forces_triggered_loop() {
    let pending = DaemonDfsPendingMaintenanceWork {
        pending_local_membership_handoff_count: 1,
        ..DaemonDfsPendingMaintenanceWork::default()
    };

    assert!(pending.has_work());
    assert!(pending.should_trigger_background_maintenance());
}

#[test]
fn daemon_dfs_reactive_probe_retains_local_membership_handoff_wake() {
    let root = temp_root("reactive-probe-membership-handoff");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local_node_id =
        NodeId::from_legacy_named("node_reactive_handoff_local").expect("local node");
    let remote_node_id =
        NodeId::from_legacy_named("node_reactive_handoff_remote").expect("remote node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_reactive_handoff").expect("cluster"),
        "reactive handoff probe",
        local_node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let retiring_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local_node_id.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        handoff_id: "reactive-handoff-root-2".to_string(),
        desired_shard: DfsMetadataShardConfig {
            replica_node_ids: vec![remote_node_id],
            epoch: 2,
            ..retiring_shard.clone()
        },
        retiring_shard,
        required_prepared_sequence: 0,
        required_head_watermark: None,
        retiring_primary_fence: None,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local_node_id,
        sinomo_core::kv::KvCommand::Op(
            KvOp::put_serde(
                sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "root"),
                &handoff,
                None,
            )
            .expect("encode handoff"),
        ),
    )
    .expect("append handoff");

    let pending = daemon_dfs_pending_reactive_maintenance_work(&paths).expect("reactive probe");
    assert_eq!(pending.pending_local_membership_handoff_count, 1);
    assert!(pending.should_trigger_background_maintenance());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_dfs_reactive_probe_retains_completed_promoted_primary_rebind_until_maintenance() {
    let root = temp_root("reactive-probe-completed-primary-rebind");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local_node_id =
        NodeId::from_legacy_named("node_completed_rebind_promoted").expect("local node");
    let retiring_node_id =
        NodeId::from_legacy_named("node_completed_rebind_retiring").expect("retiring node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        ClusterId::from_legacy_named("cluster_completed_primary_rebind").expect("cluster"),
        "completed primary rebind probe",
        local_node_id.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    state.members[0].capabilities =
        sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
    state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let file_system = distributed_sifs(local_node_id.clone());
    let file_system_id = file_system.file_system_id.clone();
    let shard_id = DfsMetadataShardId::new("root");
    let retiring = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: retiring_node_id.clone(),
        replica_node_ids: vec![local_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let active = DfsMetadataShardConfig {
        primary_node_id: local_node_id.clone(),
        replica_node_ids: vec![retiring_node_id.clone()],
        epoch: retiring.epoch + 1,
        ..retiring.clone()
    };
    let handoff_id = "completed-primary-rebind-root";
    let freeze = {
        let mut backend =
            sinomo_dfs::open_local_metadata_shard_backend(&data_dir, shard_id.clone())
                .expect("local promoted replica backend");
        backend
            .put_file_system(&file_system)
            .expect("local filesystem metadata");
        backend
            .freeze_metadata_shard_for_membership_handoff(&shard_id, handoff_id, retiring.epoch)
            .expect("install promoted-replica freeze")
            .expect("matching local shard")
    };
    let proof = DfsMetadataShardMembershipHandoffProof {
        handoff_id: handoff_id.to_string(),
        retiring_shard: retiring,
        desired_shard: active.clone(),
        journal_sequence: freeze.journal_sequence,
        state_fingerprint: freeze.state_fingerprint,
        acknowledged_node_ids: vec![retiring_node_id, local_node_id.clone()],
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local_node_id,
        sinomo_core::kv::KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(
                    file_system_config_key(&file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                KvOp::put_serde(
                    metadata_shard_key(&file_system_id, &shard_id.0),
                    &active,
                    None,
                )
                .expect("active shard op"),
                KvOp::put_serde(
                    metadata_shard_membership_handoff_proof_key(&file_system_id, &shard_id.0),
                    &proof,
                    None,
                )
                .expect("completion proof op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("commit completed handoff image");

    let pending =
        daemon_dfs_pending_reactive_maintenance_work(&paths).expect("completed rebind projection");
    assert_eq!(pending.pending_local_membership_handoff_count, 0);
    assert_eq!(pending.completed_local_primary_handoff_rebind_count, 1);
    assert!(
        pending.should_trigger_background_maintenance(),
        "the durable unactivated marker must survive a lost or coalesced completion wake"
    );

    let runtime = daemon_test_runtime("daemon-dfs-completed-primary-rebind");
    run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect("maintenance consumes promoted-primary rebind");
    let activated = read_local_dfs_metadata_shard_write_freeze(&data_dir, &shard_id)
        .expect("read activated marker")
        .expect("durable marker remains for stale handles");
    assert_eq!(activated.activated_epoch, Some(active.epoch));
    let cleared =
        daemon_dfs_pending_reactive_maintenance_work(&paths).expect("post-rebind projection");
    assert_eq!(cleared.completed_local_primary_handoff_rebind_count, 0);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn daemon_dfs_handoff_probe_only_tracks_locally_primary_shards() {
    let local_node_id = NodeId::from_legacy_named("node_handoff_probe_local").expect("node");
    let remote_node_id = NodeId::from_legacy_named("node_handoff_probe_remote").expect("node");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let status =
        |shard_id: &str, primary_node_id: NodeId, configured: bool| DfsMetadataShardStatus {
            shard_id: DfsMetadataShardId::new(shard_id),
            file_system_id: configured.then(|| file_system_id.clone()),
            primary_node_id: Some(primary_node_id),
            replica_node_ids: Vec::new(),
            epoch: Some(1),
            journal_next_sequence: 1,
            journal_last_sequence: 0,
            replica_lag: Vec::new(),
            replay_cursors: Vec::new(),
        };
    let statuses = vec![
        status("root", local_node_id.clone(), true),
        status("services", remote_node_id, true),
        status("unconfigured", local_node_id.clone(), false),
    ];

    assert_eq!(
        daemon_dfs_local_membership_handoff_keys(&statuses, &local_node_id),
        vec![sinomo_dfs::metadata_shard_membership_handoff_key(
            &file_system_id,
            "root"
        )]
    );
}

#[test]
fn daemon_dfs_control_plane_txn_notifies_only_local_handoff_puts() {
    let local_node_id = NodeId::from_legacy_named("node_handoff_notify_local").expect("node");
    let remote_node_id = NodeId::from_legacy_named("node_handoff_notify_remote").expect("node");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let retiring = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: local_node_id.clone(),
        replica_node_ids: vec![remote_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        handoff_id: "handoff_notify_local".to_string(),
        retiring_shard: retiring.clone(),
        desired_shard: DfsMetadataShardConfig {
            epoch: 2,
            replica_node_ids: vec![remote_node_id.clone()],
            ..retiring
        },
        required_prepared_sequence: 0,
        required_head_watermark: None,
        retiring_primary_fence: None,
    };
    let handoff_key =
        sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "services");
    assert!(daemon_dfs_membership_handoff_key_is_exact(&handoff_key));
    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::put_serde(handoff_key, &handoff, None).expect("handoff put")],
        failure: Vec::new(),
    };

    assert!(daemon_dfs_txn_puts_membership_handoff_for_node(
        &txn,
        &local_node_id
    ));
    assert!(!daemon_dfs_txn_puts_membership_handoff_for_node(
        &txn,
        &remote_node_id
    ));

    let proof_key =
        sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "services");
    assert!(!daemon_dfs_membership_handoff_key_is_exact(&proof_key));
    let proof_key_txn = KvTxn {
        compares: Vec::new(),
        success: vec![KvOp::put_serde(proof_key, &handoff, None).expect("proof-key put")],
        failure: Vec::new(),
    };
    assert!(!daemon_dfs_txn_puts_membership_handoff_for_node(
        &proof_key_txn,
        &local_node_id
    ));
}

#[test]
fn daemon_dfs_maintenance_tick_runs_anti_entropy() {
    let root = temp_root("daemon_dfs_maintenance_anti_entropy");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_anti_entropy_tick")
            .expect("cluster");
    let node_id = NodeId::from_legacy_named("node_daemon_dfs_anti_entropy_tick").expect("node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "daemon dfs anti entropy",
        node_id.clone(),
        "local",
        "pk-local",
        now,
    );
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let object_id =
        DfsObjectId::from_legacy_named("obj_daemon_dfs_anti_entropy_tick").expect("object");
    let version =
        DfsVersion::from_legacy_named("version_daemon_dfs_anti_entropy_tick").expect("version");
    let missing_hash = format!(
        "blake3:{}",
        blake3::hash(b"missing daemon anti entropy bytes").to_hex()
    );
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system.file_system_id.clone(),
        path: "/services/daemon-anti-entropy/missing.bin".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: node_id.clone(),
        current_locations: vec![node_id.clone()],
        content_hash: Some(missing_hash),
        content_size_bytes: Some(33),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
        tombstone_provenance: None,
    };
    {
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let metadata =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            daemon_dfs_node_transport(node_id.clone(), Vec::new()),
            content,
        )
        .with_boxed_metadata_backend(metadata)
        .expect("service backend");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap sifs");
        service
            .add_object_metadata(object.clone())
            .expect("missing object metadata");
    }

    let runtime = daemon_test_runtime("daemon-dfs-maintenance-anti-entropy");
    run_daemon_dfs_maintenance_body_for_test(&paths, &runtime).expect("maintenance tick");

    let backend =
        open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
    let repaired_object = backend
        .get_object(&object_id)
        .expect("get repaired object")
        .expect("repaired object");
    assert!(
        !repaired_object.current_locations.contains(&node_id),
        "anti-entropy must remove stale local object locations"
    );
    let repaired_version = backend
        .get_version(&object_id, &version)
        .expect("get repaired version")
        .expect("repaired version");
    assert!(
        !repaired_version.locations.contains(&node_id),
        "anti-entropy must remove stale local version locations"
    );
    assert!(
        !sinomo_dfs::default_dfs_anti_entropy_cursor_path(&data_dir, &file_system.file_system_id)
            .exists(),
        "completed scans should clear their cursor"
    );
}

#[test]
fn daemon_dfs_status_reads_configured_sharded_metadata_backend() {
    let root = temp_root("daemon_dfs_status_sharded");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_status_sharded")
        .expect("cluster");
    let node_id = NodeId::from_legacy_named("node_daemon_dfs_status_sharded").expect("node");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "daemon dfs status sharded",
        node_id.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    let remote_node_id =
        NodeId::from_legacy_named("node_daemon_dfs_status_sharded_remote").expect("remote");
    let file_system = distributed_sifs(node_id.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: vec![remote_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: node_id.clone(),
        replica_node_ids: vec![remote_node_id.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &node_id,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &root_shard,
                    None,
                )
                .expect("root shard op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &services_shard,
                    None,
                )
                .expect("services shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write shard configs");
    let mut object = distributed_sifs_bootstrap_objects(node_id)
        .into_iter()
        .next()
        .expect("bootstrap object");
    object.path = "/services/status-sharded.txt".to_string();
    let mut backend =
        sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("open sharded backend")
            .expect("configured backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        })
        .expect("version");
    backend
        .put_metadata_replication_cursor(&remote_node_id, &DfsMetadataShardId::new("services"), 1)
        .expect("metadata cursor");

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("dfs status");

    assert_eq!(status.file_system_count, 1);
    assert_eq!(status.object_count, 0);
    assert_eq!(status.version_count, 0);
    assert!(status.incomplete_reasons.iter().any(|reason| {
        reason.contains("historical routing topology")
            && reason.contains("metadata counters requiring authoritative visibility were omitted")
    }));
    assert!(status.bounded_omissions.iter().any(|omission| matches!(
        omission,
        sinomo_dfs::DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
    )));
    assert!(
        status.blocking_incomplete_reasons().is_empty(),
        "typed bounded omissions must not become invariant failures: {:?}",
        status.incomplete_reasons
    );
    assert_eq!(status.metadata_shards.len(), 2);
    let services_status = status
        .metadata_shards
        .iter()
        .find(|shard| shard.shard_id == DfsMetadataShardId::new("services"))
        .expect("services shard status");
    assert_eq!(services_status.journal_last_sequence, 3);
    assert!(services_status.replay_cursors.is_empty());
    assert_eq!(services_status.replica_lag.len(), 1);
    assert_eq!(
        services_status.replica_lag[0],
        sinomo_dfs::DfsMetadataReplicaLagStatus {
            target_node_id: remote_node_id.clone(),
            replicated_through_sequence: 1,
            lag_entries: 2,
        }
    );
    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.pending_repair_count, 0);
    assert_eq!(pending.pending_namespace_job_count, 0);
    assert_eq!(pending.metadata_replica_lag_count, 2);
    assert_eq!(pending.pending_local_membership_handoff_count, 0);
    assert_eq!(pending.due_time_based_maintenance_count, 0);
    assert!(
        !pending.should_trigger_background_maintenance(),
        "metadata lag alone is observable but owned by retained exact-shard publishers: {pending:#?}"
    );
    let DaemonDfsInspectionResponse::FileSystem {
        file_system: Some(file_system_view),
    } = sinomo_dfs::inspect_cluster_aware_dfs(
        &data_dir,
        &cluster_paths,
        DaemonDfsInspectionRequest::FileSystem {
            file_system_id: file_system.file_system_id.clone(),
        },
    )
    .expect("filesystem inspection")
    else {
        panic!("expected filesystem inspection response")
    };
    assert_eq!(file_system_view.object_count, 1);
    assert_eq!(file_system_view.open_conflict_count, 0);
    assert_eq!(file_system_view.pending_repair_count, 0);
}

#[test]
fn daemon_dfs_status_reports_replica_lag_only_from_primary() {
    let root = temp_root("daemon_dfs_status_replica_lag_primary_only");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_status_replica_lag")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_status_replica_local").expect("local");
    let primary = NodeId::from_legacy_named("node_dfs_status_replica_primary").expect("primary");
    let state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "daemon dfs status replica lag",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary,
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("root shard op"),
        ),
    )
    .expect("write shard config");
    let mut backend =
        sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
            .expect("open sharded backend")
            .expect("configured backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("dfs status");

    assert_eq!(status.metadata_shards.len(), 1);
    assert!(status.metadata_shards[0].replica_lag.is_empty());
}

#[test]
fn daemon_dfs_metadata_journal_publish_migrates_legacy_cursor_to_backend() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_cursor_migration");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_cursor")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_metadata_publish_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_remote").expect("remote node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.21:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");

    let object = distributed_sifs_bootstrap_objects(local.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("object");
    backend
        .put_version(&DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        })
        .expect("version");
    let legacy_path =
        sinomo_dfs::legacy_dfs_metadata_replication_cursor_path(&data_dir, &remote, &shard_id);
    if let Some(parent) = legacy_path.parent() {
        fs::create_dir_all(parent).expect("legacy cursor parent");
    }
    let legacy_cursor = sinomo_dfs::LegacyDfsMetadataReplicationCursor {
        target_node_id: remote.clone(),
        shard_id: shard_id.clone(),
        next_after_sequence: 1,
    };
    fs::write(
        &legacy_path,
        serde_json::to_vec_pretty(&legacy_cursor).expect("legacy cursor json"),
    )
    .expect("legacy cursor");

    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_for_sender = Arc::clone(&sent);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        sent_for_sender
            .lock()
            .expect("sent lock")
            .push(command.clone());
        Ok(applied_test_metadata_journal_response(command))
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    assert_eq!(
        report.outcomes,
        vec![DfsMetadataJournalReplicationOutcome::Applied {
            target_node_id: remote.clone(),
            shard_id: shard_id.clone(),
            entries: 2,
            next_after_sequence: 3,
        }]
    );
    let sent = sent.lock().expect("sent lock");
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0]
            .durable_replication
            .as_ref()
            .expect("ordinary replication carries immutable journal frames")
            .journal_frames
            .iter()
            .map(|frame| frame.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    drop(sent);
    let backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend reload");
    assert_eq!(
        backend
            .metadata_replication_cursor(&remote, &shard_id)
            .expect("backend cursor"),
        3
    );
    assert!(!legacy_path.exists());
}

#[test]
fn daemon_dfs_metadata_journal_publish_replicates_all_local_primary_shards() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_all_shards");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_all")
            .expect("cluster");
    let local =
        NodeId::from_legacy_named("node_dfs_metadata_publish_all_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_all_remote").expect("remote node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish all",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.22:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    for shard_name in ["root", "services"] {
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_name.to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: if shard_name == "root" {
                DfsMetadataShardAssignment::Root
            } else {
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                }
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, shard_name),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_metadata_shard_{shard_name}"))
                .expect("object");
        object.path = format!("/{shard_name}/metadata.txt");
        let mut backend = sinomo_dfs::open_local_metadata_shard_backend(
            &data_dir,
            DfsMetadataShardId::new(shard_name),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
    }

    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_for_sender = Arc::clone(&sent);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        sent_for_sender
            .lock()
            .expect("sent lock")
            .push(command.clone());
        Ok(applied_test_metadata_journal_response(command))
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    assert_eq!(
        report.outcomes,
        vec![
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: remote.clone(),
                shard_id: DfsMetadataShardId::new("root"),
                entries: 2,
                next_after_sequence: 2,
            },
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: remote.clone(),
                shard_id: DfsMetadataShardId::new("services"),
                entries: 2,
                next_after_sequence: 2,
            },
        ]
    );
    let sent = sent.lock().expect("sent lock");
    assert_eq!(
        sent.iter()
            .map(|command| command.batch.shard_id.clone())
            .collect::<Vec<_>>(),
        vec![
            DfsMetadataShardId::new("root"),
            DfsMetadataShardId::new("services")
        ]
    );
}

#[test]
fn daemon_dfs_metadata_journal_publish_drains_bounded_batches() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_drains_batches");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_drain")
            .expect("cluster");
    let local =
        NodeId::from_legacy_named("node_dfs_metadata_publish_drain_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_drain_remote").expect("remote node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish drain",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.23:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");

    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    let base_object = distributed_sifs_bootstrap_objects(local.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    let object_count = sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 7;
    for index in 0..object_count {
        let mut object = base_object.clone();
        object.object_id =
            DfsObjectId::from_legacy_named(format!("obj_daemon_metadata_drain_{index}"))
                .expect("object");
        object.path = format!("/services/daemon-metadata-drain-{index}.txt");
        object.version =
            DfsVersion::from_legacy_named(format!("version_daemon_metadata_drain_{index}"))
                .expect("version");
        object.content_hash = Some(format!("blake3:daemon-metadata-drain-{index}"));
        object.content_size_bytes = Some(index as u64);
        backend.put_object(&object).expect("object");
    }
    drop(backend);

    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_for_sender = Arc::clone(&sent);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        sent_for_sender
            .lock()
            .expect("sent lock")
            .push(command.clone());
        Ok(applied_test_metadata_journal_response(command))
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    let expected_entries = 1 + object_count;
    assert_eq!(
        report.outcomes,
        vec![
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                entries: sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                next_after_sequence: sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64,
            },
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                entries: expected_entries - sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                next_after_sequence: expected_entries as u64,
            },
        ]
    );
    let sent = sent.lock().expect("sent lock");
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent.iter()
            .map(|command| {
                command
                    .durable_replication
                    .as_ref()
                    .expect("ordinary replication carries immutable journal frames")
                    .journal_frames
                    .len()
            })
            .collect::<Vec<_>>(),
        vec![
            sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            expected_entries - sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
        ]
    );
    drop(sent);
    let backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend reload");
    assert_eq!(
        backend
            .metadata_replication_cursor(&remote, &shard_id)
            .expect("backend cursor"),
        expected_entries as u64
    );
}

#[test]
fn daemon_dfs_metadata_journal_publish_rejects_mismatched_ack_cursor() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_mismatched_ack");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bad_ack")
            .expect("cluster");
    let local =
        NodeId::from_legacy_named("node_dfs_metadata_publish_bad_ack_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_bad_ack_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish bad ack",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.24:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");

    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    drop(backend);

    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence + 1,
            applied_entries: command.batch.entries.len(),
            verified_prefix_only: command.verify_prefix_only,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Applied,
        })
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    assert!(matches!(
        report.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::Failed { reason, .. }]
            if reason.contains("mismatched metadata journal response")
    ));
    let backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend reload");
    assert_eq!(
        backend
            .metadata_replication_cursor(&remote, &shard_id)
            .expect("backend cursor"),
        0
    );
}

#[test]
fn daemon_dfs_metadata_journal_publish_stops_after_replica_rejection() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_rejected_batch");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_rejected")
            .expect("cluster");
    let local =
        NodeId::from_legacy_named("node_dfs_metadata_publish_rejected_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_rejected_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish rejected",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.25:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");

    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    drop(backend);

    let attempts = Arc::new(Mutex::new(0usize));
    let attempts_for_sender = Arc::clone(&attempts);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        let mut attempts = attempts_for_sender.lock().expect("attempts lock");
        *attempts += 1;
        assert_eq!(
            *attempts, 1,
            "replica rejection must be terminal for this publish target"
        );
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: "replica already has a conflicting journal entry".to_string(),
                code: None,
            },
        })
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    assert_eq!(*attempts.lock().expect("attempts lock"), 1);
    assert!(matches!(
        report.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::Rejected { reason, .. }]
            if reason.contains("conflicting journal entry")
    ));
    let backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend reload");
    assert_eq!(
        backend
            .metadata_replication_cursor(&remote, &shard_id)
            .expect("backend cursor"),
        0
    );
}

#[test]
fn daemon_dfs_metadata_journal_publish_repairs_sequence_conflict_with_snapshot() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_snapshot_repair");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_snapshot_repair")
            .expect("cluster");
    let local =
        NodeId::from_legacy_named("node_dfs_metadata_snapshot_repair_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_snapshot_repair_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata snapshot repair",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.26:21353")];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");

    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    drop(backend);

    let sender: DfsMetadataJournalBatchSender = Arc::new(move |_endpoint, command, _timeout| {
        Ok(DfsMetadataJournalBatchResponse {
            request_id: command.request_id.clone(),
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: format!(
                    "metadata journal sequence {} already exists with a different entry while replaying shard root",
                    command.batch.next_after_sequence
                ),
                code: None,
            },
        })
    });
    let snapshot_installs = Arc::new(Mutex::new(Vec::new()));
    let snapshot_installs_for_sender = Arc::clone(&snapshot_installs);
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(move |_endpoint, command, _timeout| {
            snapshot_installs_for_sender
                .lock()
                .expect("snapshot installs lock")
                .push(command.clone());
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id.clone(),
                shard_id: command.shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Installed {
                    archived_existing: true,
                },
            })
        });

    let report = publish_default_dfs_metadata_journal_with_senders(&paths, sender, snapshot_sender)
        .expect("publish metadata journal");

    assert_eq!(
        report.outcomes,
        vec![DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
            target_node_id: remote.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence: 1,
            archived_existing: true,
        }]
    );
    assert_eq!(snapshot_installs.lock().expect("lock").len(), 1);
    let backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend reload");
    assert_eq!(
        backend
            .metadata_replication_cursor(&remote, &shard_id)
            .expect("backend cursor"),
        1
    );
}

#[test]
fn daemon_dfs_metadata_journal_publish_bounds_endpoint_fallbacks_per_target() {
    let root = temp_root("daemon_dfs_metadata_journal_publish_bounds_endpoints");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bounds")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_metadata_publish_bounds_local").expect("local");
    let remote =
        NodeId::from_legacy_named("node_dfs_metadata_publish_bounds_remote").expect("remote");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs metadata publish bounds",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        remote.clone(),
        "remote",
        "pk-remote",
        now,
    );
    let lan_primary = test_endpoint(&remote, "10.0.0.10:21353");
    let lan_secondary = test_endpoint(&remote, "10.0.0.11:21353");
    let mut public = test_endpoint(&remote, "203.0.113.10:21353");
    public.privacy = ClusterEndpointPrivacy::InternetDirect;
    let iroh = ClusterEndpointRecord {
        endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
            "sinomo.test.endpoint.iroh",
            remote.as_str(),
        ),
        node_id: remote.clone(),
        transport: ClusterTransportKind::Iroh,
        address: "iroh://iroh_public_key_dfs_metadata_bounds?relay=https://relay.example./"
            .to_string(),
        source: ClusterEndpointSource::Rendezvous,
        privacy: ClusterEndpointPrivacy::Relayed,
        priority: 0,
        last_observed_latency_ms: Some(40),
    };
    remote_member.endpoints = vec![
        lan_secondary,
        iroh.clone(),
        public.clone(),
        lan_primary.clone(),
    ];
    state.members.push(remote_member);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

    let file_system = distributed_sifs(local.clone());
    let shard_id = DfsMetadataShardId::new("root");
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write shard config");
    let mut backend = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    drop(backend);

    let attempted = Arc::new(Mutex::new(Vec::new()));
    let attempted_for_sender = Arc::clone(&attempted);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |endpoint, _command, _timeout| {
        attempted_for_sender
            .lock()
            .expect("attempted lock")
            .push(endpoint.to_string());
        Err(DfsMetadataPeerTransportError::new(format!(
            "{} simulated failure",
            endpoint
        )))
    });

    let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
        .expect("publish metadata journal");

    assert!(matches!(
        report.outcomes.as_slice(),
        [DfsMetadataJournalReplicationOutcome::Failed { .. }]
    ));
    assert_eq!(
        attempted.lock().expect("attempted lock").as_slice(),
        &[remote.to_string()],
        "metadata replication should make one target-level attempt"
    );
}

#[test]
fn cancelled_daemon_dfs_rpc_retains_committed_work_edges() {
    let root = temp_root("daemon-dfs-cancelled-rpc-retains-publish-edge");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");
    let local = NodeId::from_legacy_named("node_dfs_cancelled_publish_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_cancelled_publish_remote").expect("remote node");
    let missing_replica =
        NodeId::from_legacy_named("node_dfs_cancelled_publish_missing").expect("missing node");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let mut cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("read test cluster state");
    let local_member = cluster_state
        .members
        .iter_mut()
        .find(|member| member.node_id == local)
        .expect("local test member");
    local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
    local_member.allowed_capabilities = local_member.capabilities.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
        .expect("write voter-capable test cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &cluster_state)
        .expect("sync test voter membership to KV");

    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("metadata shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write DFS control plane");
    let mut backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        default_dfs_metadata_root(&data_dir),
    )
    .expect("metadata backend");
    backend
        .put_file_system(&file_system)
        .expect("filesystem metadata");
    for object in distributed_sifs_bootstrap_objects(local.clone()) {
        backend.put_object(&object).expect("bootstrap object");
    }
    drop(backend);

    let request_id = "dfs_message_cancelled_publish_edge";
    let frame_id = 90_101;
    let object_id = DfsObjectId::from_legacy_named(format!("obj_daemon_dfs_write_{frame_id}"))
        .expect("object id");
    let mut routed_message = test_dfs_write_routed_message(
        &remote,
        &local,
        frame_id,
        "/services/cancelled-publish-edge.txt",
    );
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed_message.message else {
        panic!("distributed write request");
    };
    write.desired_replica_nodes = vec![local.clone(), missing_replica];
    write.policy = DfsReplicationPolicy::new(1, 2).expect("W=1, N=2 policy");
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    let command = test_dfs_message_command(request_id, routed_message);
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);
    let initial_repair_edges = daemon_dfs_repair_maintenance_edge_request_count(&data_dir);
    let (committed_rx, release_tx) =
        install_daemon_dfs_message_committed_edge_test_hook(request_id);
    let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-publish-edge");
    let task_paths = paths.clone();
    let task_runtime = runtime.handle().clone();
    let rpc = runtime.handle().spawn(async move {
        handle_daemon_dfs_message_async(&task_paths, command, None, None, &task_runtime).await
    });

    committed_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("durable message mutation reached committed-edge boundary");
    assert!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("reopen metadata backend")
        .get_object(&object_id)
        .expect("read committed object")
        .is_some(),
        "the mutation must be durable before the cancellable RPC boundary"
    );
    assert_eq!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("reopen metadata backend for repair summary")
        .metadata_summary()
        .expect("read repair summary")
        .pending_repair_count,
        1,
        "the authority must durably own one repair before edge delivery"
    );
    rpc.abort();
    let cancelled = runtime
        .block_on(rpc)
        .expect_err("RPC task must be cancelled");
    assert!(cancelled.is_cancelled());
    release_tx
        .send(())
        .expect("release non-cancellable committed-edge delivery");

    let deadline = Instant::now() + Duration::from_secs(3);
    while (daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) == initial_requests
        || daemon_dfs_repair_maintenance_edge_request_count(&data_dir) == initial_repair_edges)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) > initial_requests,
        "a cancelled RPC must still dirty the exact retained publisher lane"
    );
    assert!(
        daemon_dfs_repair_maintenance_edge_request_count(&data_dir) > initial_repair_edges,
        "a cancelled RPC must still wake the authority's repair classifier"
    );
}

#[test]
fn cancelled_daemon_dfs_rpc_retains_subscribed_replica_progress_edge() {
    let root = temp_root("daemon-dfs-cancelled-rpc-retains-replica-progress");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");
    let primary = NodeId::from_legacy_named("node_dfs_cancelled_replica_primary").expect("primary");
    let replica = NodeId::from_legacy_named("node_dfs_cancelled_replica_local").expect("replica");
    let file_system = distributed_sifs(primary.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    install_test_metadata_replica_shard(
        &cluster_paths,
        "cluster_daemon_dfs_cancelled_replica_progress",
        &replica,
        &shard,
    );
    let object = distributed_sifs_bootstrap_objects(primary.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    let mut source = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("source-metadata"),
    )
    .expect("source backend");
    source.put_file_system(&file_system).expect("filesystem");
    source.put_object(&object).expect("object");
    let mut subscription = source.subscribe(0);
    let batch = source
        .poll_subscription(&mut subscription)
        .expect("source journal batch");

    sinomo_dfs::test_establish_default_cluster_dfs_replica_progress_subscription(&data_dir);
    assert!(
        sinomo_dfs::take_default_cluster_dfs_materialization_immediate_retry(&data_dir),
        "test subscription establishes and consumes its verification generation"
    );
    let mut reconciliation_rx =
        sinomo_dfs::subscribe_default_dfs_reconciliation_replica_progress(&cluster_paths)
            .expect("default DFS reconciliation wake");
    let _ = reconciliation_rx.borrow_and_update();

    let request_id = "dfs_message_cancelled_replica_progress";
    let command = test_dfs_message_command(
        request_id,
        DfsRoutedMessage {
            source_node_id: primary.clone(),
            target_node_id: replica,
            frame_id: 90_102,
            message_id: 90_102,
            message: DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                request_id: "dfs_metadata_cancelled_replica_progress".to_string(),
                source_node_id: Some(primary),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch,
            }),
        },
    );
    let (committed_rx, release_tx) =
        install_daemon_dfs_message_committed_edge_test_hook(request_id);
    let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-replica-progress");
    let task_paths = paths.clone();
    let task_runtime = runtime.handle().clone();
    let rpc = runtime.handle().spawn(async move {
        handle_daemon_dfs_message_async(&task_paths, command, None, None, &task_runtime).await
    });

    committed_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("replica mutation reached committed-edge boundary");
    assert_eq!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("reopen replica backend")
        .get_object(&object.object_id)
        .expect("read replicated object"),
        Some(object.clone()),
        "the replica journal batch must be durable before RPC cancellation"
    );
    rpc.abort();
    let cancelled = runtime
        .block_on(rpc)
        .expect_err("RPC task must be cancelled");
    assert!(cancelled.is_cancelled());
    release_tx
        .send(())
        .expect("release non-cancellable replica-progress delivery");

    let deadline = Instant::now() + Duration::from_secs(3);
    while !reconciliation_rx
        .has_changed()
        .expect("reconciliation wake channel remains open")
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        reconciliation_rx
            .has_changed()
            .expect("reconciliation wake channel remains open"),
        "a cancelled RPC must still emit the subscribed replica-progress generation"
    );
    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("message service cache lock");
    let entry = cache
        .get(&data_dir.root())
        .expect("cold message cache entry");
    assert_eq!(
        entry.exact_shard_services.len(),
        1,
        "the first exact replica command must initialize its shard lane inside global admission"
    );
    let exact_lane = entry
        .exact_shard_services
        .values()
        .next()
        .expect("exact replica lane")
        .lock()
        .expect("exact replica lane lock");
    assert!(
        exact_lane
            .service
            .metadata()
            .object(&object.object_id)
            .is_none(),
        "the cold exact lane must retain replica objects only in its resident backend"
    );
    assert!(
        entry.primary_service.is_none(),
        "cold exact replica admission must not construct an unused global shell"
    );
}

#[test]
fn daemon_dfs_message_processes_stage_blob_and_returns_response() {
    let root = temp_root("daemon_dfs_message_stage_blob");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let owner = NodeId::from_legacy_named("node_dfs_protocol_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_dfs_protocol_replica").expect("replica");
    write_test_cluster_state(&data_root, &replica);
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let file_system = distributed_sifs(owner.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: owner.clone(),
        replica_node_ids: vec![owner.clone(), replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &replica,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("metadata shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write dfs control plane");
    let object_id = DfsObjectId::from_legacy_named("obj_dfs_data_plane_stage").expect("object");
    let version = DfsVersion::from_legacy_named("version_dfs_data_plane_stage").expect("version");
    let bytes = b"daemon dfs data-plane message bytes".to_vec();
    let content_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "services/daemon-dfs-data-plane.bin".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(content_hash.clone()),
        content_size_bytes: Some(bytes.len() as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version.clone(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
        tombstone_provenance: None,
    };
    let message = DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
        request_id: "dfs_stage_rpc_1".to_string(),
        intent_id: "dfs_intent_protocol_stage".to_string(),
        file_system_id: file_system_id.clone(),
        object_id: object_id.clone(),
        expected_base_version: None,
        version: version.clone(),
        content_hash: content_hash.clone(),
        bytes: bytes.clone(),
        object_metadata: Some(object),
        authority_shard: None,
    });
    let routed_message = DfsRoutedMessage {
        source_node_id: owner.clone(),
        target_node_id: replica.clone(),
        frame_id: 1,
        message_id: 1,
        message,
    };

    let runtime = daemon_test_runtime("daemon-dfs-message-replica-stage");
    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command("dfs_message_rpc_1", routed_message),
        runtime.handle(),
    )
    .expect("DFS message");

    assert_eq!(response.request_id, "dfs_message_rpc_1");
    assert_eq!(response.outbound_messages.len(), 1);
    let response_message = &response.outbound_messages[0].message;
    assert!(matches!(
        response_message,
        DfsNetworkMessage::StageBlobResponse(response)
            if response.request_id == "dfs_stage_rpc_1"
                && response.object_id == object_id
                && response.version == version
                && response.replica_node_id == replica
                && response.result == DfsBlobTransferResult::Durable { content_hash }
    ));

    let store = LocalContentStore::new(root.join("data").join("dfs").join("content"))
        .expect("content store");
    let manifest = store
        .read_manifest(object_id.as_str(), version.as_str())
        .expect("read staged manifest");
    let read = store.read_blob(&manifest.content_hash).expect("read blob");
    assert_eq!(read, bytes);
    let backend = sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
        .expect("open sharded backend")
        .expect("configured backend");
    assert!(
        backend
            .get_object(&object_id)
            .expect("backend object")
            .is_none(),
        "non-primary content replicas must not append authoritative metadata while handling DFS messages"
    );
}

#[test]
fn daemon_dfs_message_rejects_wrong_target_node() {
    let root = temp_root("daemon_dfs_message_wrong_target");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let local = NodeId::from_legacy_named("node_dfs_protocol_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_protocol_remote").expect("remote");
    write_test_cluster_state(&data_root, &local);
    let routed_message =
        test_dfs_lookup_routed_message(&remote, &remote, 1, "/services/daemon-wrong-target.txt");

    let runtime = daemon_test_runtime("daemon-dfs-message-wrong-target");
    let error = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command("dfs_message_rpc_wrong_target", routed_message),
        runtime.handle(),
    )
    .expect_err("wrong target rejected");
    assert!(error.to_string().contains("does not match local node"));
}

#[test]
fn daemon_dfs_distributed_write_skips_cold_full_metadata_hydrate() {
    let root = temp_root("daemon_dfs_write_skips_cold_hydrate");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main data owner"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner).expect("register main data owner");
    let local = NodeId::from_legacy_named("node_dfs_protocol_write_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_protocol_write_remote").expect("remote");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let runtime = daemon_test_runtime("daemon-dfs-write-skips-cold-hydrate");

    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("read test cluster state");
    let local_member = cluster_state
        .members
        .iter_mut()
        .find(|member| member.node_id == local)
        .expect("local test member");
    local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
    local_member.allowed_capabilities = local_member.capabilities.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
        .expect("write voter-capable test cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &cluster_state)
        .expect("sync test voter membership to KV");
    let file_system = distributed_sifs(local.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &root_shard,
                    None,
                )
                .expect("root shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write DFS control plane");
    let mut unrelated = distributed_sifs_bootstrap_objects(local.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    unrelated.object_id =
        sinomo_dfs::DfsObjectId::from_legacy_named("obj_daemon_unrelated_cold_metadata")
            .expect("object id");
    unrelated.path = "/users/unrelated-cold-metadata.txt".to_string();
    unrelated.version =
        sinomo_dfs::DfsVersion::from_legacy_named("version_daemon_unrelated_cold_metadata")
            .expect("version");
    let unrelated_object_id = unrelated.object_id.clone();
    let mut backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        default_dfs_metadata_root(&data_dir),
    )
    .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    for object in distributed_sifs_bootstrap_objects(local.clone()) {
        backend.put_object(&object).expect("bootstrap object");
    }
    backend.put_object(&unrelated).expect("unrelated object");
    drop(backend);

    let cache_key = data_dir.root();
    let mut routed_message = test_dfs_write_routed_message(
        &remote,
        &local,
        1,
        "/services/daemon-write-skips-cold-hydrate.txt",
    );
    let sinomo_dfs::DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed_message.message
    else {
        panic!("write routed message");
    };
    write.desired_replica_nodes = vec![local.clone()];
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command("dfs_message_write_skips_cold_hydrate", routed_message),
        runtime.handle(),
    )
    .expect("distributed write DFS message");
    assert_eq!(response.request_id, "dfs_message_write_skips_cold_hydrate");
    let written_object_id = sinomo_dfs::DfsObjectId::from_legacy_named("obj_daemon_dfs_write_1")
        .expect("written object id");
    let persisted = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        default_dfs_metadata_root(&data_dir),
    )
    .expect("reopen metadata backend")
    .get_object(&written_object_id)
    .expect("read written object")
    .unwrap_or_else(|| {
        panic!(
            "cold routed write must persist its object: {:?}",
            response.outbound_messages
        )
    });
    assert_eq!(
        persisted.path,
        "/services/daemon-write-skips-cold-hydrate.txt"
    );
    let primary_service = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        Arc::clone(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("mutation service lane"),
        )
    };
    let lane = primary_service.lock().expect("primary service lock");
    assert!(
        lane.service
            .metadata()
            .object(&unrelated_object_id)
            .is_none(),
        "cold message-lane construction must not eagerly load unrelated metadata"
    );
    drop(lane);
    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("cache lock");
    assert!(Arc::ptr_eq(
        &primary_service,
        cache
            .get(&cache_key)
            .expect("cache entry")
            .primary_service
            .as_ref()
            .expect("mutation service lane")
    ));
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        1,
        "a cold routed mutation must reuse the one backend-bearing cache lane"
    );
    // Status may borrow only an already-materialized queue view. Model the
    // daemon's reactive-maintenance initialization boundary explicitly;
    // the handler itself must not replay this durable queue.
    LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data_dir))
        .view()
        .expect("publish empty resident repair queue generation");
    let allocator_trims_before_status = daemon_dfs_allocator_trim_completion_count(&data_dir);
    for _ in 0..2 {
        let status_deadline = Instant::now() + Duration::from_secs(1);
        let status = loop {
            let status = runtime
                .block_on(handle_daemon_dfs_status_async(&paths, runtime.handle()))
                .expect("status from resident mutation lane");
            let blocking = status.blocking_incomplete_reasons();
            if blocking.is_empty() {
                break status;
            }
            assert!(
                    blocking.iter().all(|reason| reason.contains(
                        "bounded DFS status unavailable while the resident generation cache is being updated"
                    )),
                    "resident status reported a non-transient incomplete reason: {:?}",
                    status.incomplete_reasons,
                );
            assert!(
                Instant::now() < status_deadline,
                "resident status did not become available after a parallel cache update",
            );
            std::thread::yield_now();
        };
        assert!(
            status.metadata_shards.iter().any(|shard| {
                shard.shard_id == DfsMetadataShardId::new("root")
                    && shard.file_system_id.as_ref() == Some(&file_system.file_system_id)
                    && shard.primary_node_id.as_ref() == Some(&local)
            }),
            "bounded status must expose the already-resident root shard topology",
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        1,
        "repeated daemon status must borrow the resident metadata backend"
    );
    assert_eq!(
        daemon_dfs_allocator_trim_completion_count(&data_dir),
        allocator_trims_before_status,
        "bounded status must not create discarded table generations"
    );

    // Removing the cached service shell proves status borrows the resident
    // projection directly rather than silently initializing an inspection
    // or mutation service to reach the same backend.
    {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let detached = cache
            .get_mut(&cache_key)
            .expect("cache entry")
            .primary_service
            .take()
            .expect("detach mutation service shell");
        assert!(Arc::ptr_eq(&detached, &primary_service));
    }
    let lane_opens_before_detached_status =
        daemon_dfs_test_message_service_lane_open_count(&data_dir);
    runtime
        .block_on(handle_daemon_dfs_status_async(&paths, runtime.handle()))
        .expect("status without a cached service shell");
    {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get_mut(&cache_key).expect("cache entry");
        assert!(
            entry.primary_service.is_none(),
            "bounded status must not initialize a service shell"
        );
        entry.primary_service = Some(Arc::clone(&primary_service));
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_opens_before_detached_status
    );

    let resident = daemon_dfs_message_service_cache()
        .lock()
        .expect("cache lock")
        .get(&cache_key)
        .expect("cache entry")
        .resident_metadata_backend
        .clone()
        .expect("resident metadata generation");
    let root_handle = resident
        .scope_shard(
            &file_system.file_system_id,
            &DfsMetadataShardId::new("root"),
        )
        .expect("scope resident root shard");
    let root_lease = root_handle.checkout().expect("hold resident root shard");
    runtime
        .block_on(async {
            let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
            let global = scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await?;
            let status = tokio::time::timeout(
                Duration::from_millis(500),
                handle_daemon_dfs_status_async(&paths, runtime.handle()),
            )
            .await
            .map_err(|_| DaemonError::Network {
                address: "bounded status test".to_string(),
                detail: "status waited behind the global mutation fence".to_string(),
            })??;
            drop(global);
            Ok::<_, DaemonError>(status)
        })
        .expect("status serves the last completed shard generation through a global fence");
    drop(root_lease);

    let cache_guard = daemon_dfs_message_service_cache()
        .lock()
        .expect("hold cache mutation ownership");
    let cache_busy_status = runtime
        .block_on(async {
            tokio::time::timeout(
                Duration::from_millis(500),
                handle_daemon_dfs_status_async(&paths, runtime.handle()),
            )
            .await
        })
        .expect("cache-owned status must return promptly")
        .expect("cache-owned status must degrade to a partial response");
    assert!(
        cache_busy_status
            .incomplete_reasons
            .iter()
            .any(|reason| reason.contains("cache is being updated"))
    );
    assert!(
        !cache_busy_status.metadata_shards.is_empty(),
        "partial status must retain configured shard topology"
    );
    drop(cache_guard);

    let resident_before_cold_status = {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get_mut(&cache_key).expect("cache entry");
        entry.resident_metadata_initialized = false;
        entry.resident_metadata_backend.take()
    };
    let lane_opens_before_cold_status = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let cold_status = runtime
        .block_on(handle_daemon_dfs_status_async(&paths, runtime.handle()))
        .expect("unhydrated cache must return partial status");
    assert!(
        cold_status.incomplete_reasons.iter().any(|reason| {
            reason.contains("not hydrated") || reason.contains("cache is being updated")
        }),
        "a cold or concurrently-owned resident cache must degrade explicitly: {:?}",
        cold_status.incomplete_reasons
    );
    assert!(!cold_status.metadata_shards.is_empty());
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_opens_before_cold_status,
        "cold status must not initialize a metadata service lane"
    );
    {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get_mut(&cache_key).expect("cache entry");
        entry.resident_metadata_backend = resident_before_cold_status;
        entry.resident_metadata_initialized = true;
    }

    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_read_survives_maintenance",
            test_dfs_lookup_routed_message(
                &remote,
                &local,
                2,
                "/services/daemon-write-skips-cold-hydrate.txt",
            ),
        ),
        runtime.handle(),
    )
    .expect("lightweight lookup DFS message");
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        assert!(Arc::ptr_eq(
            &primary_service,
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        ));
    }
    let lane_open_count_before_maintenance =
        daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let weak_primary = Arc::downgrade(&primary_service);
    drop(primary_service);
    run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect("successful maintenance tick");
    assert!(
        weak_primary.upgrade().is_none(),
        "maintenance transfers ownership out of the old cache handle"
    );
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let lane = cache
            .get(&cache_key)
            .expect("cache entry")
            .primary_service
            .as_ref()
            .expect("restored mutation service")
            .lock()
            .expect("restored mutation service lock");
        assert!(
            lane.service
                .metadata()
                .object(&unrelated_object_id)
                .is_none(),
            "restored maintenance lane must not retain accumulated object metadata"
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_open_count_before_maintenance,
        "successful maintenance must not open another backend-bearing lane"
    );

    let mut second_write = test_dfs_write_routed_message(
        &remote,
        &local,
        3,
        "/services/daemon-write-after-maintenance.txt",
    );
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut second_write.message else {
        panic!("distributed write request");
    };
    write.desired_replica_nodes = vec![local.clone()];
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command("dfs_message_write_after_maintenance", second_write),
        runtime.handle(),
    )
    .expect("distributed write after maintenance");
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_open_count_before_maintenance,
        "the first post-maintenance mutation must not reopen metadata"
    );

    let repair_snapshot = sinomo_dfs::default_dfs_repair_attempt_snapshot_path(&data_dir);
    fs::create_dir_all(
        repair_snapshot
            .parent()
            .expect("repair snapshot parent directory"),
    )
    .expect("create repair snapshot directory");
    fs::write(&repair_snapshot, b"invalid repair snapshot").expect("corrupt repair snapshot");
    let error = run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect_err("corrupt repair state must fail maintenance");
    assert!(
        error.to_string().contains("repair"),
        "unexpected maintenance error: {error}"
    );
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        assert!(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .is_none(),
            "failed maintenance must drop its backend-bearing mutation lane"
        );
    }
    fs::remove_file(&repair_snapshot).expect("remove corrupt repair snapshot");

    let mut third_write = test_dfs_write_routed_message(
        &remote,
        &local,
        4,
        "/services/daemon-write-after-failed-maintenance.txt",
    );
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut third_write.message else {
        panic!("distributed write request");
    };
    write.desired_replica_nodes = vec![local.clone()];
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command("dfs_message_write_after_failed_maintenance", third_write),
        runtime.handle(),
    )
    .expect("distributed write after failed maintenance");
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_open_count_before_maintenance.saturating_add(1),
        "the first mutation after failed maintenance must open a new lane"
    );

    let current_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("cluster state before peer-only maintenance refresh");
    let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &current_state,
        runtime.handle(),
        None,
    )
    .expect("take mutation lane before peer-only refresh");
    let added_peer = NodeId::from_legacy_named("node_dfs_protocol_added_peer").expect("added peer");
    let mut changed_state = current_state;
    changed_state
        .members
        .push(test_remote_cluster_member_with_endpoint(
            &changed_state.cluster_id,
            &added_peer,
            "127.0.0.2:9",
        ));
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &changed_state)
        .expect("write peer-only cluster-state change");
    assert!(
        restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data_dir,
            &cluster_paths,
            runtime.handle(),
            retained,
        )
        .expect("restore mutation lane after peer-only refresh"),
        "peer membership changes must refresh, not discard, the retained mutation lane"
    );
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get(&cache_key).expect("cache entry");
        let lane = entry
            .primary_service
            .as_ref()
            .expect("restored mutation lane")
            .lock()
            .expect("mutation lane lock");
        assert!(
            lane.peer_node_ids.contains(&added_peer),
            "the retained mutation lane must install the current peer targets"
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_open_count_before_maintenance.saturating_add(1),
        "peer-only maintenance refresh must retain the open metadata backend"
    );

    let current_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("current cluster state");
    let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &current_state,
        runtime.handle(),
        None,
    )
    .expect("take mutation lane for authority-change validation");
    let changed_root_shard = DfsMetadataShardConfig {
        epoch: root_shard.epoch.saturating_add(1),
        ..root_shard
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(
                    &changed_root_shard.file_system_id,
                    &changed_root_shard.shard_id,
                ),
                &changed_root_shard,
                None,
            )
            .expect("changed root shard op"),
        ),
    )
    .expect("commit changed root shard");
    assert!(
        !restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data_dir,
            &cluster_paths,
            runtime.handle(),
            retained,
        )
        .expect("validate changed maintenance signature"),
        "authority changes must reject a maintenance lane captured from the old generation"
    );
    assert!(
        daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock")
            .get(&cache_key)
            .expect("cache entry")
            .primary_service
            .is_none(),
        "a rejected authority generation must leave the mutation lane empty"
    );
    assert!(
        evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
        "shutdown must release the complete message cache"
    );
}

#[test]
fn daemon_dfs_lookup_message_reuses_resident_service_without_full_hydrate() {
    let root = temp_root("daemon_dfs_message_warm_cache");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let local = NodeId::from_legacy_named("node_dfs_protocol_warm_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_protocol_warm_remote").expect("remote");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "10.77.0.1:21353");
    let runtime = daemon_test_runtime("daemon-dfs-message-warm-cache");

    let cache_key = SinomoDataDir::new(&data_root).root();
    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_warm_cache_1",
            test_dfs_lookup_routed_message(&remote, &local, 1, "/services/warm-cache-1.txt"),
        ),
        runtime.handle(),
    )
    .expect("first DFS message");
    assert_eq!(response.request_id, "dfs_message_warm_cache_1");
    let read_service = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get(&cache_key).expect("cache entry");
        Arc::clone(
            entry
                .primary_service
                .as_ref()
                .expect("lookup opens the resident metadata lane"),
        )
    };
    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_warm_cache_2",
            test_dfs_lookup_routed_message(&remote, &local, 2, "/services/warm-cache-2.txt"),
        ),
        runtime.handle(),
    )
    .expect("second DFS message");
    assert_eq!(response.request_id, "dfs_message_warm_cache_2");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "10.77.0.2:21353");
    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_warm_cache_3",
            test_dfs_lookup_routed_message(&remote, &local, 3, "/services/warm-cache-3.txt"),
        ),
        runtime.handle(),
    )
    .expect("third DFS message");
    assert_eq!(response.request_id, "dfs_message_warm_cache_3");
    let second_read_service = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get(&cache_key).expect("cache entry");
        Arc::clone(
            entry
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        )
    };
    assert!(
        Arc::ptr_eq(&read_service, &second_read_service),
        "endpoint-list refreshes should reuse the resident metadata lane"
    );
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&SinomoDataDir::new(&data_root)),
        1,
        "metadata reads should reuse one resident backend generation"
    );
}

#[test]
fn daemon_dfs_peer_membership_refresh_reuses_resident_lane() {
    let root = temp_root("daemon_dfs_message_peer_refresh");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let local = NodeId::from_legacy_named("node_dfs_peer_refresh_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_peer_refresh_remote").expect("remote");
    let added = NodeId::from_legacy_named("node_dfs_peer_refresh_added").expect("added");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let runtime = daemon_test_runtime("daemon-dfs-message-peer-refresh");

    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_peer_refresh_1",
            test_dfs_lookup_routed_message(&remote, &local, 1, "/services/peer-refresh-1.txt"),
        ),
        runtime.handle(),
    )
    .expect("initial lookup");
    let cache_key = data_dir.root();
    let read_service = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        Arc::clone(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        )
    };
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
        .expect("cluster");
    write_test_cluster_state_with_members(
        &data_root,
        &local,
        vec![
            test_remote_cluster_member_with_endpoint(&cluster_id, &remote, "127.0.0.1:9"),
            test_remote_cluster_member_with_endpoint(&cluster_id, &added, "127.0.0.2:9"),
        ],
    );
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_peer_refresh_2",
            test_dfs_lookup_routed_message(&remote, &local, 2, "/services/peer-refresh-2.txt"),
        ),
        runtime.handle(),
    )
    .expect("lookup after peer addition");
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get(&cache_key).expect("cache entry");
        let current = entry
            .primary_service
            .as_ref()
            .expect("resident metadata lane");
        assert!(
            Arc::ptr_eq(&read_service, current),
            "peer addition must not reopen the resident metadata backend"
        );
        let lane = current.lock().expect("resident lane lock");
        assert!(lane.peer_node_ids.contains(&remote));
        assert!(lane.peer_node_ids.contains(&added));
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        1,
        "peer membership changes must keep the one open metadata backend"
    );
    assert!(
        evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
        "test cleanup must release the cached lane"
    );
}

#[test]
fn daemon_dfs_resident_mutation_lanes_are_scoped_by_exact_shard() {
    let root = temp_root("daemon_dfs_resident_exact_shard_lanes");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_resident_lanes_local").expect("local node");
    let remote = NodeId::from_legacy_named("node_dfs_resident_lanes_remote").expect("remote node");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");

    let file_system = distributed_sifs(local.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &root_shard,
                    None,
                )
                .expect("root shard config"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &services_shard,
                    None,
                )
                .expect("services shard config"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write resident lane control plane");

    let runtime = daemon_test_runtime("daemon-dfs-resident-exact-shard-lanes");
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("read peer snapshot")
        .expect("initialized cluster state");
    let metadata_authority_preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &cluster_paths,
            &peer_snapshot,
            runtime.handle(),
        ))
        .expect("warm resident lane authority");
    let root_key = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let services_key = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id,
        shard_id: DfsMetadataShardId::new("services"),
    };
    let select = |kind| {
        let allow_generation_rollover = matches!(&kind, DaemonDfsMessageLaneKind::MutationGlobal);
        daemon_dfs_message_lane(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            runtime.handle(),
            kind,
            allow_generation_rollover,
            Some(&metadata_authority_preflight),
        )
        .map(|(lane, _)| lane)
        .expect("select resident mutation lane")
    };

    let root_lane = select(DaemonDfsMessageLaneKind::MutationExact(root_key.clone()));
    let same_root_lane = select(DaemonDfsMessageLaneKind::MutationExact(root_key.clone()));
    let services_lane = select(DaemonDfsMessageLaneKind::MutationExact(services_key));
    {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message service cache lock");
        let entry = cache.get(&data_dir.root()).expect("message cache entry");
        assert!(
            entry.primary_service.is_none(),
            "exact-shard selection must not construct the all-shard service"
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        2,
        "two exact shards must construct only their two request shells"
    );
    let global_lane = select(DaemonDfsMessageLaneKind::MutationGlobal);

    assert!(
        Arc::ptr_eq(&root_lane, &same_root_lane),
        "repeated exact-shard selection must reuse one request-state mutex"
    );
    assert!(
        !Arc::ptr_eq(&root_lane, &services_lane),
        "disjoint exact shards must not share a request-state mutex"
    );
    assert!(
        !Arc::ptr_eq(&root_lane, &global_lane) && !Arc::ptr_eq(&services_lane, &global_lane),
        "global compatibility work must retain its own request-state mutex"
    );

    let root_guard = root_lane.lock().expect("root lane lock");
    assert!(
        same_root_lane.try_lock().is_err(),
        "the reused same-shard lane must serialize concurrent request state"
    );
    let services_guard = services_lane
        .try_lock()
        .expect("a disjoint shard lane must remain independently lockable");
    let global_guard = global_lane
        .try_lock()
        .expect("the global lane mutex must be distinct from exact-shard lanes");
    assert!(matches!(
        root_guard.resident_metadata_scope,
        Some(DaemonDfsResidentMetadataScope::ExactShard(_))
    ));
    assert!(matches!(
        services_guard.resident_metadata_scope,
        Some(DaemonDfsResidentMetadataScope::ExactShard(_))
    ));
    assert!(matches!(
        global_guard.resident_metadata_scope,
        Some(DaemonDfsResidentMetadataScope::Global(_))
    ));
    drop((root_guard, services_guard, global_guard));

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("message service cache lock");
    let entry = cache.get(&data_dir.root()).expect("message cache entry");
    assert_eq!(entry.exact_shard_services.len(), 2);
    assert!(Arc::ptr_eq(
        entry.primary_service.as_ref().expect("global lane"),
        &global_lane,
    ));
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        3,
        "two exact-shard lanes and one global lane must share one resident backend generation"
    );
    assert!(
        daemon_dfs_message_cache_exact_admission_is_current(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &root_key,
            runtime.handle(),
        )
        .expect("validate current exact admission"),
        "a resident exact lane must be admitted only against its observed topology generation"
    );

    let changed_root_shard = DfsMetadataShardConfig {
        epoch: root_shard.epoch.saturating_add(1),
        ..root_shard
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(
                    &changed_root_shard.file_system_id,
                    &changed_root_shard.shard_id,
                ),
                &changed_root_shard,
                None,
            )
            .expect("updated root shard config"),
        ),
    )
    .expect("commit updated root shard authority");
    assert!(
        !daemon_dfs_message_cache_exact_admission_is_current(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &root_key,
            runtime.handle(),
        )
        .expect("reject stale exact admission"),
        "an authority edge must fail closed until global admission replaces the resident generation"
    );
    assert!(
        evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
        "test cleanup must release resident lanes"
    );
}

#[test]
fn daemon_dfs_exact_service_lane_requires_complete_single_shard_state() {
    let source = NodeId::from_legacy_named("node_dfs_exact_scope_source").expect("source");
    let target = NodeId::from_legacy_named("node_dfs_exact_scope_target").expect("target");
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let shard = |shard_id: &str, assignment| DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.to_string(),
        primary_node_id: target.clone(),
        replica_node_ids: vec![source.clone()],
        epoch: 1,
        assignment,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let services = shard(
        "services",
        DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
    );
    let root = shard("root", DfsMetadataShardAssignment::Root);
    let namespace_command = |operation, authority_shards| {
        test_dfs_message_command(
            "dfs-exact-scope",
            DfsRoutedMessage {
                source_node_id: source.clone(),
                target_node_id: target.clone(),
                frame_id: 1,
                message_id: 1,
                message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                    request_id: "dfs-exact-scope-namespace".to_string(),
                    file_system_id: file_system_id.clone(),
                    requesting_node_id: source.clone(),
                    include_metadata_journal_batches: false,
                    metadata_journal_after_sequences: Vec::new(),
                    authority_shards,
                    operation,
                }),
            },
        )
    };

    let single = namespace_command(
        DfsNamespaceOperation::Mkdir {
            path: "/services/exact".to_string(),
        },
        vec![services.clone()],
    );
    assert!(daemon_dfs_message_supports_exact_service_lane(&single));
    assert!(matches!(
        dfs_message_execution_scope(&single),
        DfsMessageExecutionScope::ExactShard { .. }
    ));

    let read_only = namespace_command(
        DfsNamespaceOperation::LookupPath {
            path: "/services/exact".to_string(),
            inline_range: None,
        },
        vec![services.clone()],
    );
    assert!(
        daemon_dfs_message_supports_exact_service_lane(&read_only),
        "a single stamped lookup is fully owned by its resident shard lane"
    );
    assert!(dfs_message_is_read_only_request(&read_only));
    assert!(matches!(
        dfs_message_execution_scope(&read_only),
        DfsMessageExecutionScope::ExactShard { .. }
    ));
    let namespace_poll = namespace_command(
        DfsNamespaceOperation::PollNamespaceChanges {
            request: sinomo_dfs::DfsNamespaceChangePollRequest {
                root_path: "/".to_string(),
                cursor: sinomo_dfs::DfsNamespaceChangeCursor::from_shard_config(&services, 0),
            },
        },
        vec![services.clone()],
    );
    assert!(daemon_dfs_message_supports_exact_service_lane(
        &namespace_poll
    ));
    assert!(dfs_message_is_read_only_request(&namespace_poll));
    assert_eq!(
        dfs_message_execution_scope(&namespace_poll),
        DfsMessageExecutionScope::ExactShard {
            file_system_id: file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("services"),
        },
        "namespace polling must dispatch by cursor shard, not subtree root"
    );

    let cross_shard = namespace_command(
        DfsNamespaceOperation::Rename {
            from: "/services/exact".to_string(),
            to: "/users/exact".to_string(),
        },
        vec![services, root],
    );
    assert!(
        !daemon_dfs_message_supports_exact_service_lane(&cross_shard),
        "multi-shard namespace state must stay globally owned"
    );

    let mut distributed_write =
        test_dfs_write_routed_message(&source, &target, 2, "/services/exact-write.txt");
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut distributed_write.message else {
        unreachable!("test helper builds a distributed write")
    };
    write.authority_shard = Some(shard(
        "services",
        DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        },
    ));
    let distributed_write = test_dfs_message_command("dfs-exact-write", distributed_write);
    assert!(matches!(
        dfs_message_execution_scope(&distributed_write),
        DfsMessageExecutionScope::ExactShard { .. }
    ));
    assert!(
        !daemon_dfs_message_supports_exact_service_lane(&distributed_write),
        "distributed-write coordinator and repair state is not shard-owned yet"
    );
}

#[test]
fn daemon_dfs_stamped_lookup_is_cold_global_warm_exact_and_read_only() {
    let root = temp_root("daemon_dfs_stamped_lookup_lane_safety");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main data owner"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner).expect("register main data owner");
    let local = NodeId::from_legacy_named("node_dfs_stamped_lookup_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_stamped_lookup_remote").expect("remote");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let mut cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("read cluster state");
    let local_member = cluster_state
        .members
        .iter_mut()
        .find(|member| member.node_id == local)
        .expect("local member");
    local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
    local_member.allowed_capabilities = local_member.capabilities.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
        .expect("write voter-capable cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &cluster_state)
        .expect("sync voter membership");

    let file_system = distributed_sifs(local.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 9,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let users_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "users".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 9,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: "/users".to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, &root_shard.shard_id),
                    &root_shard,
                    None,
                )
                .expect("root shard config"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("write DFS control plane");

    let metadata_root = default_dfs_metadata_root(&data_dir);
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
            .expect("metadata backend");
    backend.put_file_system(&file_system).expect("filesystem");
    for object in distributed_sifs_bootstrap_objects(local.clone()) {
        backend.put_object(&object).expect("bootstrap object");
    }
    let journal_sequence_before = backend.journal_last_sequence();
    drop(backend);

    let stamped_lookup = |frame_id: u64, stamp: DfsMetadataShardConfig| {
        let mut routed = test_dfs_lookup_routed_message(&remote, &local, frame_id, "/services");
        let DfsNetworkMessage::NamespaceRequest(namespace) = &mut routed.message else {
            unreachable!("lookup helper returns namespace request")
        };
        namespace.authority_shards = vec![stamp];
        routed
    };
    let stamped_poll = |frame_id: u64, stamp: DfsMetadataShardConfig| {
        let mut routed = test_dfs_lookup_routed_message(&remote, &local, frame_id, "/");
        let DfsNetworkMessage::NamespaceRequest(namespace) = &mut routed.message else {
            unreachable!("lookup helper returns namespace request")
        };
        namespace.authority_shards = vec![stamp.clone()];
        namespace.operation = DfsNamespaceOperation::PollNamespaceChanges {
            request: sinomo_dfs::DfsNamespaceChangePollRequest {
                root_path: "/".to_string(),
                cursor: sinomo_dfs::DfsNamespaceChangeCursor::from_shard_config(&stamp, 0),
            },
        };
        routed
    };
    let runtime = daemon_test_runtime("daemon-dfs-stamped-lookup-lane-safety");
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
    );

    let first = test_dfs_message_command(
        "dfs_stamped_lookup_cold",
        stamped_lookup(1, root_shard.clone()),
    );
    runtime
        .block_on(handle_daemon_dfs_message_async(
            &paths,
            first,
            None,
            None,
            runtime.handle(),
        ))
        .expect("cold stamped lookup");
    let (
        authority_materializations_after_cold,
        authority_refreshes_after_cold,
        cold_primary_service_is_some,
        cold_exact_shard_services_are_empty,
    ) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            entry
                .metadata_authority_generation
                .projection_materialization_count(),
            entry
                .metadata_authority_generation
                .authority_refresh_count(),
            entry.primary_service.is_some(),
            entry.exact_shard_services.is_empty(),
        )
    };
    assert!(cold_primary_service_is_some);
    assert_eq!(
        authority_refreshes_after_cold, 1,
        "cold admission must establish one shared retained prefix proof",
    );
    assert!(
        cold_exact_shard_services_are_empty,
        "a cold stamped lookup must stay on the global service"
    );
    assert!(
        authority_materializations_after_cold > 0,
        "cold resident initialization must exercise the projection materialization counter",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        1,
        "cold namespace admission must cross one bounded prefix ReadIndex",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
        "cold namespace admission must not add an exact-key quorum read",
    );

    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("peer snapshot")
        .expect("clustered peer snapshot");
    let current_probe = test_dfs_message_command(
        "dfs_stamped_lookup_current_probe",
        stamped_lookup(2, root_shard.clone()),
    );
    let DfsNetworkMessage::NamespaceRequest(current_namespace) =
        &current_probe.routed_message.message
    else {
        unreachable!("current probe is a namespace request")
    };
    assert!(
        daemon_dfs_message_cache_exact_namespace_read_admission_is_current(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            current_namespace,
            runtime.handle(),
        )
        .expect("validate warm current stamp"),
        "the global cold read should leave the exact resident shard warm"
    );
    let stale_shard = DfsMetadataShardConfig {
        epoch: root_shard.epoch - 1,
        ..root_shard.clone()
    };
    let stale_probe = test_dfs_message_command(
        "dfs_stamped_lookup_stale_probe",
        stamped_lookup(3, stale_shard),
    );
    let DfsNetworkMessage::NamespaceRequest(stale_namespace) = &stale_probe.routed_message.message
    else {
        unreachable!("stale probe is a namespace request")
    };
    assert!(
        !daemon_dfs_message_cache_exact_namespace_read_admission_is_current(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            stale_namespace,
            runtime.handle(),
        )
        .expect("reject stale same-id stamp"),
        "a warm shard id must not admit an obsolete full authority stamp"
    );

    runtime
        .block_on(handle_daemon_dfs_message_async(
            &paths,
            current_probe,
            None,
            None,
            runtime.handle(),
        ))
        .expect("warm stamped lookup");
    let (
        authority_materializations_after_warm_lane,
        authority_refreshes_before_repeated_warm,
        exact_lane_before_repeated_warm,
    ) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            entry
                .metadata_authority_generation
                .projection_materialization_count(),
            entry
                .metadata_authority_generation
                .authority_refresh_count(),
            Arc::as_ptr(
                entry
                    .exact_shard_services
                    .values()
                    .next()
                    .expect("warm exact lane"),
            ) as usize,
        )
    };
    assert!(
        authority_materializations_after_warm_lane >= authority_materializations_after_cold,
        "the materialization counter must remain monotonic across one-time lane initialization",
    );
    for frame_id in 4..=11 {
        runtime
            .block_on(handle_daemon_dfs_message_async(
                &paths,
                test_dfs_message_command(
                    "dfs_stamped_lookup_repeated_warm",
                    stamped_lookup(frame_id, root_shard.clone()),
                ),
                None,
                None,
                runtime.handle(),
            ))
            .expect("repeated warm stamped lookup");
    }
    let (
        authority_materializations_after_repeated_warm,
        authority_refreshes_after_repeated_warm,
        exact_lane_after_repeated_warm,
    ) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            entry
                .metadata_authority_generation
                .projection_materialization_count(),
            entry
                .metadata_authority_generation
                .authority_refresh_count(),
            Arc::as_ptr(
                entry
                    .exact_shard_services
                    .values()
                    .next()
                    .expect("retained warm exact lane"),
            ) as usize,
        )
    };
    assert!(
        authority_materializations_after_repeated_warm
            >= authority_materializations_after_warm_lane,
        "retained projection materialization diagnostics must remain monotonic",
    );
    assert_eq!(
        authority_refreshes_after_repeated_warm, authority_refreshes_before_repeated_warm,
        "repeated warm authority preflights must not issue another bounded authority refresh",
    );
    assert_eq!(
        exact_lane_after_repeated_warm, exact_lane_before_repeated_warm,
        "repeated warm lookups must reuse the exact resident lane",
    );
    let (exact_lane_before_checkpoint_change, refreshes_before_checkpoint_change) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            Arc::as_ptr(
                entry
                    .exact_shard_services
                    .values()
                    .next()
                    .expect("warm exact lane"),
            ) as usize,
            entry
                .metadata_authority_generation
                .authority_refresh_count(),
        )
    };
    let materialized = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
        .expect("canonical snapshot used to recreate materialized checkpoint");
    std::fs::remove_file(&cluster_paths.kv_paths().snapshot)
        .expect("unlink materialized checkpoint");
    runtime
        .block_on(handle_daemon_dfs_message_async(
            &paths,
            test_dfs_message_command(
                "dfs_stamped_lookup_without_materialized_checkpoint",
                stamped_lookup(12, root_shard.clone()),
            ),
            None,
            None,
            runtime.handle(),
        ))
        .expect("warm lookup without materialized checkpoint");
    sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &materialized)
        .expect("recreate materialized checkpoint");
    runtime
        .block_on(handle_daemon_dfs_message_async(
            &paths,
            test_dfs_message_command(
                "dfs_stamped_lookup_after_materialized_checkpoint_recreate",
                stamped_lookup(13, root_shard.clone()),
            ),
            None,
            None,
            runtime.handle(),
        ))
        .expect("warm lookup after materialized checkpoint recreate");
    let (
        exact_lane_count_after_checkpoint_change,
        exact_lane_after_checkpoint_change,
        refreshes_after_checkpoint_change,
    ) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            entry.exact_shard_services.len(),
            Arc::as_ptr(
                entry
                    .exact_shard_services
                    .values()
                    .next()
                    .expect("retained exact lane"),
            ) as usize,
            entry
                .metadata_authority_generation
                .authority_refresh_count(),
        )
    };
    assert_eq!(exact_lane_count_after_checkpoint_change, 1);
    assert_eq!(
        exact_lane_after_checkpoint_change, exact_lane_before_checkpoint_change,
        "unlinking or recreating the observational checkpoint must not reopen the DFS lane",
    );
    assert_eq!(
        refreshes_after_checkpoint_change, refreshes_before_checkpoint_change,
        "materialized checkpoint changes must not refresh retained Raft authority",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
        "cold and repeated warm namespace reads must use the retained authority projection, not enter Raft ReadIndex validation",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
        "repeated warm namespace reads must reuse the seeded retained prefix without another Raft ReadIndex",
    );

    let (lane_opens_before_cold_poll, exact_lanes_before_cold_poll) = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("message cache");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        (
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            entry.exact_shard_services.len(),
        )
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, &users_shard.shard_id),
                &users_shard,
                None,
            )
            .expect("new users shard config"),
        ),
    )
    .expect("commit new cold users shard");
    let cold_poll_result = runtime.block_on(handle_daemon_dfs_message_async(
        &paths,
        test_dfs_message_command(
            "dfs_namespace_poll_cold_nonresident",
            stamped_poll(50, users_shard),
        ),
        None,
        None,
        runtime.handle(),
    ));
    let cold_poll_rejected = match cold_poll_result {
        Ok(_) => false,
        Err(error) => {
            assert!(
                error.to_string().contains("refusing to cold-open"),
                "unexpected cold poll error: {error}"
            );
            true
        }
    };
    let lane_opens_after_cold_poll = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("message cache");
    let entry = cache.get(&data_dir.root()).expect("cache entry");
    let exact_lanes_after_cold_poll = entry.exact_shard_services.len();
    drop(cache);
    if cold_poll_rejected {
        assert_eq!(
            lane_opens_after_cold_poll, lane_opens_before_cold_poll,
            "a cold namespace poll must fail before opening any service/backend lane"
        );
        assert_eq!(
            exact_lanes_after_cold_poll, exact_lanes_before_cold_poll,
            "a cold namespace poll must not construct an exact shard service"
        );
    } else {
        // The authority reactor may install the committed shard between the
        // append above and admission. In that case the request is no longer a
        // cold open: serving it through one newly resident exact lane is the
        // correct converged outcome.
        assert_eq!(
            lane_opens_after_cold_poll,
            lane_opens_before_cold_poll.saturating_add(1),
            "a reactively resident namespace poll must open exactly one service lane"
        );
        assert_eq!(
            exact_lanes_after_cold_poll,
            exact_lanes_before_cold_poll.saturating_add(1),
            "a reactively resident namespace poll must retain exactly one new shard lane"
        );
    }

    let repair_queue =
        LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data_dir))
            .view()
            .expect("repair queue view");
    assert_eq!(repair_queue.head_sequence, 0);
    assert!(repair_queue.records.is_empty());

    assert!(evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"));
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
        .expect("reopen metadata backend");
    assert_eq!(
        reopened.journal_last_sequence(),
        journal_sequence_before,
        "cold and warm stamped lookups must append no metadata journal records"
    );
}

#[test]
fn daemon_dfs_content_store_lookup_does_not_open_service_lane() {
    let root = temp_root("daemon_dfs_content_store_lookup");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let local = NodeId::from_legacy_named("node_dfs_content_store_lookup").expect("local");
    write_test_cluster_state(&data_root, &local);
    let runtime = daemon_test_runtime("daemon-dfs-content-store-lookup");
    let data_owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main data owner"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner).expect("register main data owner");
    let host = DaemonDfsDataPlaneHost::new_on_runtime(paths, runtime.handle());

    assert!(
        host.shared_content_store(&data_dir)
            .expect("shared content-store lookup")
            .is_some()
    );

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("cache lock");
    let entry = cache.get(&data_dir.root()).expect("cache entry");
    assert!(entry.content_read_service.is_none());
    assert!(entry.primary_service.is_none());
    assert!(entry.exact_shard_services.is_empty());
    assert!(entry.resident_metadata_backend.is_none());
    assert!(!entry.resident_metadata_initialized);
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0,
        "content-store lookup must not instantiate a DFS service lane"
    );
}

#[test]
fn daemon_dfs_exact_read_lane_does_not_open_metadata_backend() {
    let root = temp_root("daemon_dfs_exact_read_content_lane");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let local = NodeId::from_legacy_named("node_dfs_exact_read_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_exact_read_remote").expect("remote");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
    let runtime = daemon_test_runtime("daemon-dfs-exact-read-content-lane");

    let response = handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_exact_content_read",
            test_dfs_exact_read_routed_message(&remote, &local, 1),
        ),
        runtime.handle(),
    )
    .expect("exact content read");
    assert_eq!(response.request_id, "dfs_message_exact_content_read");

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("cache lock");
    let entry = cache.get(&data_dir.root()).expect("cache entry");
    assert!(entry.content_read_service.is_some());
    assert!(entry.primary_service.is_none());
    assert!(entry.exact_shard_services.is_empty());
    assert!(entry.resident_metadata_backend.is_none());
    assert!(!entry.resident_metadata_initialized);
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        0,
        "exact immutable reads must not open a metadata-bearing service lane"
    );
}

#[test]
fn daemon_owned_dfs_clients_reuse_and_track_shared_metadata_authority_generation() {
    let root = temp_root("daemon-owned-dfs-client-shared-authority");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_daemon_owned_dfs_shared_authority").expect("local node");
    write_test_cluster_state(&data_root, &local);
    let state =
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).expect("cluster state");
    let runtime = daemon_test_runtime("daemon-owned-dfs-client-shared-authority");
    // Install the retained generation before attaching a runtime so this
    // regression controls every possible authoritative network call.
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &local,
        None,
    )
    .expect("shared retained authority");
    let file_system = distributed_sifs(local.clone());
    let file_system_key = file_system_config_key(&file_system.file_system_id);
    let shard_key = metadata_shard_key(&file_system.file_system_id, "root");
    let shard_v1 = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let initial = default_dfs_reactor_test_snapshot(
        1,
        vec![
            (
                file_system_key,
                sinomo_core::kv::encode_kv_value(&DfsFileSystemConfig::from(&file_system))
                    .expect("filesystem config"),
            ),
            (
                shard_key.clone(),
                sinomo_core::kv::encode_kv_value(&shard_v1).expect("shard v1"),
            ),
        ],
    );
    assert!(
        authority
            .install_authoritative_projection(test_dfs_authoritative_prefix_projection(&initial))
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
    );

    let read_epoch = |client: &dyn sinomo_dfs::DfsControlPlaneKvClient| {
        client
            .get(&shard_key)
            .expect("read retained shard authority")
            .expect("retained shard entry")
            .decode::<DfsMetadataShardConfig>()
            .expect("decode retained shard")
            .epoch
    };
    for _ in 0..8 {
        let daemon = DaemonDfsControlPlaneKvClient::new_seeded_on_runtime(
            &cluster_paths,
            runtime.handle(),
            None,
        );
        assert_eq!(read_epoch(&daemon), 1);
        let browser =
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
                .expect("fresh daemon browser client");
        assert_eq!(read_epoch(browser.as_ref()), 1);
        let sandbox =
            DaemonDfsControlPlaneKvClient::new_owned_on_runtime(&cluster_paths, runtime.handle());
        assert_eq!(read_epoch(&sandbox), 1);
    }
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
        "repeated short-lived daemon clients must not issue exact ReadIndex requests",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
        "repeated short-lived daemon clients must seed from the shared retained prefix",
    );

    let cached = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
        .expect("cached browser client");
    assert_eq!(read_epoch(cached.as_ref()), 1);
    let mut shard_v2 = shard_v1.clone();
    shard_v2.epoch = 2;
    let encoded_v2 = sinomo_core::kv::encode_kv_value(&shard_v2).expect("shard v2");
    let previous_generation = authority.generation();
    assert!(
        authority.observe_delta(&sinomo_core::kv::ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: vec![sinomo_core::kv::KvEvent {
                revision: KvRevision(2),
                key: shard_key.clone(),
                kind: sinomo_core::kv::KvEventKind::Put {
                    value: encoded_v2.clone(),
                },
            }],
            final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
                key: shard_key.clone(),
                entry: Some(KvEntry {
                    key: shard_key.clone(),
                    value: encoded_v2,
                    create_revision: KvRevision(1),
                    modify_revision: KvRevision(2),
                    version: KvVersion(2),
                    lease_id: None,
                }),
            }],
        })
    );
    assert_eq!(authority.generation(), previous_generation + 1);
    assert_eq!(
        read_epoch(cached.as_ref()),
        2,
        "a cached browser client must invalidate and reseed after shared authority advances",
    );
    let observational_key = format!(
        "/dfs/filesystems/{}/client-observation",
        file_system.file_system_id
    );
    let topology_generation = authority.generation();
    let projection_generation = authority.projection_generation();
    assert!(
        authority.observe_delta(&sinomo_core::kv::ClusterKvCommittedDelta {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            events: vec![sinomo_core::kv::KvEvent {
                revision: KvRevision(3),
                key: observational_key.clone(),
                kind: sinomo_core::kv::KvEventKind::Put {
                    value: b"visible".to_vec(),
                },
            }],
            final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
                key: observational_key.clone(),
                entry: Some(KvEntry {
                    key: observational_key.clone(),
                    value: b"visible".to_vec(),
                    create_revision: KvRevision(3),
                    modify_revision: KvRevision(3),
                    version: KvVersion(1),
                    lease_id: None,
                }),
            }],
        })
    );
    assert_eq!(
        authority.generation(),
        topology_generation,
        "non-topology prefix state must not reopen resident metadata lanes",
    );
    assert_eq!(authority.projection_generation(), projection_generation + 1,);
    assert_eq!(
        cached
            .get(&observational_key)
            .expect("read same-topology projection update")
            .expect("same-topology projection entry")
            .value,
        b"visible",
        "cached clients must reseed when retained prefix content changes without a topology generation change",
    );
    let reopened =
        daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime.handle()))
            .expect("reopened browser client");
    assert_eq!(read_epoch(reopened.as_ref()), 2);
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        0,
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
        "generation reseed must consume the committed delta projection without a new ReadIndex",
    );
}

#[test]
fn daemon_dfs_async_client_preserves_newer_exact_proof_while_retained_authority_catches_up() {
    let root = temp_root("daemon-dfs-client-newer-than-retained-authority");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("daemon-dfs-client-newer-than-retained-authority");
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let key = "/dfs/filesystems/test/metadata-shards/root".to_string();
    let stable_value = b"stable-authority".to_vec();
    assert!(
        authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(1, vec![(key.clone(), stable_value.clone())],)
        ),)
    );
    let topology_generation = authority.generation();

    let client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
    let mut client = client;
    client.retained_metadata_authority = Some(Arc::clone(&authority));
    let newer_entry = KvEntry {
        key: key.clone(),
        value: stable_value.clone(),
        create_revision: KvRevision(1),
        modify_revision: KvRevision(2),
        version: KvVersion(2),
        lease_id: None,
    };
    {
        let mut projection = client.projection.lock().expect("client projection");
        projection.revision = KvRevision(2);
        projection
            .exact_entries
            .insert(key.clone(), Some(newer_entry.clone()));
    }

    let observed = runtime
        .block_on(sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(
            &client, &key,
        ))
        .expect("newer exact proof must remain usable")
        .expect("newer exact entry");
    assert_eq!(observed, newer_entry);
    assert_eq!(
        client.authoritative_exact_set_calls.load(Ordering::Acquire),
        0,
        "a covered newer exact proof must not issue another ReadIndex",
    );
    let retained = authority.debug_snapshot();
    assert_eq!(retained.revision, None);
    assert_eq!(
        retained.required_revision,
        KvRevision(2),
        "the lagging shared projection must still fail closed and request catch-up",
    );
    assert!(retained.topology_comparison_baseline);
    assert_eq!(
        authority.generation(),
        topology_generation,
        "revision catch-up must not manufacture a historical topology generation",
    );
    assert!(
        authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(2, vec![(key.clone(), stable_value)],)
        ),)
    );
    assert_eq!(
        authority.generation(),
        topology_generation,
        "an equal replacement topology must restore readiness without reopening resident lanes",
    );

    let changed_value = b"changed-authority".to_vec();
    {
        let mut projection = client.projection.lock().expect("client projection");
        projection.revision = KvRevision(3);
        projection.exact_entries.insert(
            key.clone(),
            Some(KvEntry {
                key: key.clone(),
                value: changed_value.clone(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(3),
                version: KvVersion(3),
                lease_id: None,
            }),
        );
    }
    runtime
        .block_on(sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(
            &client, &key,
        ))
        .expect("changed newer exact proof must remain usable");
    assert!(
        authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(3, vec![(key, changed_value)],)
        ),)
    );
    assert_eq!(
        authority.generation(),
        topology_generation + 1,
        "a changed replacement topology must advance the resident generation after comparison",
    );
}

struct DeferredDfsAuthorityHost {
    runtime: tokio::runtime::Handle,
    response: Mutex<
        Option<tokio::sync::oneshot::Receiver<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>>>,
    >,
    started: tokio::sync::Notify,
    reads: AtomicUsize,
}

impl sinomo_dfs::DfsMetadataAuthorityHost for DeferredDfsAuthorityHost {
    fn read_authoritative_prefix(&self) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'_> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        let response = self.response.lock().unwrap().take();
        self.started.notify_one();
        Box::pin(async move {
            match response {
                Some(response) => response.await.map_err(|_| sinomo_dfs::DfsError::Network {
                    message: "test authority proof cancelled".into(),
                }),
                None => Err(sinomo_dfs::DfsError::Network {
                    message: "test authority unavailable".into(),
                }),
            }
        })
    }

    fn spawn(&self, task: sinomo_dfs::DfsMetadataAuthorityTask) {
        self.runtime.spawn(task);
    }

    fn authority_changed(&self) {}
}

#[test]
fn daemon_dfs_global_preflight_accepts_only_a_ready_successor_generation() {
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let key = "/dfs/filesystems/test/metadata-shards/root".to_string();
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(1, vec![(key.clone(), b"first".to_vec())]),
    ));
    let preflight = DaemonDfsMetadataAuthorityPreflight {
        authority: Arc::clone(&authority),
        generation: authority.generation(),
    };
    assert!(preflight.admits_retained_generation(&authority, authority.generation(), false));
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(2, vec![(key, b"successor".to_vec())]),
    ));
    let successor = authority.generation();
    assert_ne!(successor, preflight.generation);
    assert!(
        preflight.admits_retained_generation(&authority, successor, true),
        "global admission may use the already-complete successor proof"
    );
    assert!(
        !preflight.admits_retained_generation(&authority, successor, false),
        "exact admission must not roll over its generation"
    );
    assert!(
        !preflight.admits_retained_generation(&authority, preflight.generation, true),
        "an old cache entry is never current authority"
    );
    let foreign = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    assert!(!preflight.admits_retained_generation(&foreign, successor, true));
    authority.require_authoritative_revision(KvRevision(3));
    assert!(
        !preflight.admits_retained_generation(&authority, successor, true),
        "a missing successor proof must still fail closed"
    );
}

#[test]
fn daemon_dfs_clients_wait_for_inflight_retained_authority_refresh() {
    for asynchronous in [false, true] {
        let root = temp_root("daemon-dfs-client-authority-refresh");
        let runtime = daemon_test_runtime("daemon-dfs-client-authority-refresh");
        let paths = ClusterPaths::new(&SinomoDataDir::new(root.join("data")));
        let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
        let key = "/dfs/filesystems/test/metadata-shards/root".to_string();
        assert!(authority.install_authoritative_projection(
            test_dfs_authoritative_prefix_projection(&default_dfs_reactor_test_snapshot(
                1,
                vec![(key.clone(), b"old".to_vec())]
            ),)
        ));
        let old_generation = authority.generation();
        let (send, receive) = tokio::sync::oneshot::channel();
        let host = Arc::new(DeferredDfsAuthorityHost {
            runtime: runtime.handle().clone(),
            response: Mutex::new(Some(receive)),
            started: tokio::sync::Notify::new(),
            reads: AtomicUsize::new(0),
        });
        authority.attach_host(host.clone());
        let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
        client.retained_metadata_authority = Some(Arc::clone(&authority));
        // A browser lane retains the old view before a newer operation-bound
        // proof forces shared catch-up. No stale scope may escape the gap.
        assert_eq!(
            sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key)
                .unwrap()
                .unwrap()
                .value,
            b"old"
        );
        authority.require_authoritative_revision(KvRevision(2));
        runtime.block_on(host.started.notified());
        let read_client = client.clone();
        let read_key = key.clone();
        let mut request = if asynchronous {
            runtime.spawn(async move {
                sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(&read_client, &read_key).await
            })
        } else {
            runtime.spawn_blocking(move || {
                sinomo_dfs::DfsControlPlaneKvClient::get(&read_client, &read_key)
            })
        };
        assert!(
            runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_millis(25), &mut request).await
                })
                .is_err(),
            "a lookup must wait for the already-running authoritative refresh"
        );
        let (entries, revision, _) = test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(2, vec![(key.clone(), b"new".to_vec())]),
        )
        .into_parts();
        send.send(sinomo_core::kv::KvAuthoritativeRead {
            value: entries,
            authority: sinomo_core::kv::KvAuthorityProof {
                term: 1,
                commit_index: 2,
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 1,
                    required_quorum: 1,
                },
            },
        })
        .unwrap();
        let result = runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(2), request).await })
            .expect("bounded lookup")
            .unwrap()
            .expect("refreshed authority")
            .unwrap();
        assert_eq!(result.value, b"new");
        assert_eq!(authority.generation(), old_generation + 1);
        assert_eq!(
            host.reads.load(Ordering::Acquire),
            1,
            "join the shared refresh without duplicate ReadIndex"
        );
        assert_eq!(
            sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key)
                .unwrap()
                .unwrap()
                .value,
            b"new"
        );
        assert_eq!(
            host.reads.load(Ordering::Acquire),
            1,
            "warm reads stay local"
        );
    }
}

#[test]
fn daemon_dfs_clients_do_not_serve_stale_scopes_without_authority_refresh() {
    let root = temp_root("daemon-dfs-client-authority-unavailable");
    let runtime = daemon_test_runtime("daemon-dfs-client-authority-unavailable");
    let paths = ClusterPaths::new(&SinomoDataDir::new(root.join("data")));
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let key = "/dfs/filesystems/test/metadata-shards/root".to_string();
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(1, vec![(key.clone(), b"old".to_vec())]),
    ));
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
    client.retained_metadata_authority = Some(Arc::clone(&authority));
    assert!(
        sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key)
            .unwrap()
            .is_some()
    );
    authority.require_authoritative_revision(KvRevision(2));
    assert!(matches!(
        sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key),
        Err(KvError::AuthorityUnavailable { .. })
    ));
    assert!(matches!(
        runtime.block_on(sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(
            &client, &key
        )),
        Err(KvError::AuthorityUnavailable { .. })
    ));
    assert_eq!(authority.debug_snapshot().revision, None);
}

#[test]
fn daemon_dfs_clients_bound_a_stalled_authority_refresh() {
    let root = temp_root("daemon-dfs-client-authority-deadline");
    let runtime = daemon_test_runtime("daemon-dfs-client-authority-deadline");
    let paths = ClusterPaths::new(&SinomoDataDir::new(root.join("data")));
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let (_send, receive) = tokio::sync::oneshot::channel();
    let host = Arc::new(DeferredDfsAuthorityHost {
        runtime: runtime.handle().clone(),
        response: Mutex::new(Some(receive)),
        started: tokio::sync::Notify::new(),
        reads: AtomicUsize::new(0),
    });
    authority.attach_host(host.clone());
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
    client.retained_metadata_authority = Some(Arc::clone(&authority));
    let error = runtime
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(10),
                sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(
                    &client,
                    "/dfs/filesystems/test",
                ),
            )
            .await
        })
        .expect("client bounds the stalled shared read")
        .expect_err("no authoritative proof");
    assert!(matches!(error, KvError::AuthorityUnavailable { .. }));
    // Deployed clients recognize this compatibility diagnostic when deciding
    // to reconcile the same operation identity after an uncertain write.
    let diagnostic = error.to_string();
    assert!(diagnostic.contains("retained metadata authority generation"));
    assert!(diagnostic.contains("is not ready"));
    assert!(diagnostic.contains("deadline elapsed"));
    assert_eq!(host.reads.load(Ordering::Acquire), 1);
    assert_eq!(authority.debug_snapshot().revision, None);
}

#[test]
fn daemon_dfs_metadata_authority_refresh_handoff_does_not_refetch() {
    let root = temp_root("daemon-dfs-authority-refresh-handoff");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = daemon_test_runtime("daemon-dfs-authority-refresh-handoff");
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let host = daemon_dfs_metadata_authority_host(&cluster_paths, Some(runtime.handle()))
        .expect("metadata authority host");
    assert!(authority.attach_host(host));

    let reached_before_claim = Arc::new(tokio::sync::Notify::new());
    let release_claim = Arc::new(tokio::sync::Notify::new());
    authority.install_refresh_before_claim_hook(Some((
        Arc::clone(&reached_before_claim),
        Arc::clone(&release_claim),
    )));
    let expected_generation = authority.generation();
    let waiter_authority = Arc::clone(&authority);
    let waiter = runtime.spawn(async move {
        waiter_authority
            .ensure_complete_prefix_for_generation_async(expected_generation)
            .await
    });
    runtime.block_on(reached_before_claim.notified());

    // Model a refresh completing in the narrow interval between the
    // waiter's retained read and CAS. The waiter must observe this proof
    // before issuing another ReadIndex.
    assert!(
        authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(1, Vec::new(),)
        ),)
    );
    release_claim.notify_one();

    assert!(
        runtime
            .block_on(waiter)
            .expect("join authority waiter")
            .is_some()
    );
    assert_eq!(authority.authority_refresh_count(), 0);
    assert_eq!(
        authority.projection_materialization_count(),
        0,
        "singleflight handoff readiness must not clone the retained prefix",
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths),
        0,
        "a completed singleflight handoff must not issue a second bounded ReadIndex",
    );
}

#[test]
fn daemon_dfs_metadata_authority_registry_prunes_dead_unique_roots() {
    let root = temp_root("daemon-dfs-authority-registry-prune");
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
        "cluster_daemon_dfs_authority_registry_prune",
    )
    .expect("cluster id");
    let local =
        NodeId::from_legacy_named("node_daemon_dfs_authority_registry_prune").expect("local node");

    for index in 0..32 {
        let data_dir = SinomoDataDir::new(root.path().join(format!("expired-{index}")));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let authority = daemon_dfs_metadata_authority_generation_for_identity(
            &cluster_paths,
            &cluster_id,
            &local,
            None,
        )
        .expect("ephemeral authority generation");
        drop(authority);
    }

    let retained_data_dir = SinomoDataDir::new(root.path().join("retained"));
    let retained_paths = ClusterPaths::new(&retained_data_dir);
    let retained = daemon_dfs_metadata_authority_generation_for_identity(
        &retained_paths,
        &cluster_id,
        &local,
        None,
    )
    .expect("retained authority generation");
    let matching_entries = daemon_dfs_metadata_authority_generations()
        .live_keys()
        .expect("authority registry")
        .into_iter()
        .filter(|key| key.cluster_root.starts_with(root.path()))
        .count();
    assert_eq!(matching_entries, 1);
    assert!(Arc::strong_count(&retained) >= 1);
}

#[test]
fn daemon_dfs_metadata_authority_generation_tracks_only_exact_projection_changes() {
    let root = temp_root("daemon_dfs_metadata_authority_generation");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_authority_generation").expect("local");
    write_test_cluster_state(&data_root, &local);
    let runtime = daemon_test_runtime("daemon-dfs-metadata-authority-generation");
    let generation = daemon_dfs_metadata_authority_generation(&cluster_paths, runtime.handle())
        .expect("generation");
    wait_for_daemon_dfs_metadata_authority_projection(&generation);
    let initial = generation.generation();
    let initial_rebuilds = generation.projection_rebuild_count();
    assert!(
        initial_rebuilds > 0,
        "the resident snapshot must initialize once"
    );

    let mut observed_revision = generation
        .debug_snapshot()
        .revision
        .expect("authoritative base revision");
    let mut append =
        |generation: &Arc<DaemonDfsMetadataAuthorityGeneration>, key: &str, value: &[u8]| {
            let snapshot = append_cluster_raft_command(
                &cluster_paths,
                &local,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: key.to_string(),
                    value: value.to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append KV value");
            if snapshot.revision > observed_revision {
                let events = snapshot
                    .events
                    .iter()
                    .filter(|event| event.revision > observed_revision)
                    .cloned()
                    .collect::<Vec<_>>();
                let final_key_states = events
                    .iter()
                    .map(|event| event.key.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|key| sinomo_core::kv::ClusterKvCommittedKeyState {
                        entry: snapshot.get(&key).cloned(),
                        key,
                    })
                    .collect();
                assert!(
                    generation.observe_delta(&sinomo_core::kv::ClusterKvCommittedDelta {
                        revision: snapshot.revision,
                        compacted_revision: snapshot.compacted_revision,
                        events,
                        final_key_states,
                    },)
                );
                observed_revision = snapshot.revision;
            }
        };
    append(&generation, "/cluster/unrelated", b"one");
    append(&generation, "/cluster/unrelated", b"two");
    assert_eq!(generation.generation(), initial);
    assert_eq!(
        generation.projection_rebuild_count(),
        initial_rebuilds,
        "unrelated commits must advance the cursor without rebuilding the authority projection"
    );

    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let shard_key = metadata_shard_key(&file_system_id, "root");
    append(&generation, &shard_key, b"shard-v1");
    let after_shard = generation.generation();
    assert_eq!(after_shard, initial + 1);
    let after_shard_rebuilds = generation.projection_rebuild_count();
    assert_eq!(after_shard_rebuilds, initial_rebuilds);
    append(&generation, &shard_key, b"shard-v1");
    let after_identical_shard_rebuilds = generation.projection_rebuild_count();
    assert_eq!(
        after_identical_shard_rebuilds, after_shard_rebuilds,
        "an idempotent put produces no committed authority generation"
    );
    append(
        &generation,
        "/dfs/filesystems/unrelated/non-authority",
        b"config-churn",
    );
    assert_eq!(generation.generation(), after_shard);
    assert_eq!(
        generation.projection_rebuild_count(),
        after_identical_shard_rebuilds,
        "nearby but non-authority DFS keys must not rebuild the projection"
    );

    append(
        &generation,
        &sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "root"),
        b"handoff-v1",
    );
    let after_handoff = generation.generation();
    assert_eq!(after_handoff, after_shard + 1);
    append(
        &generation,
        &sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "root"),
        b"proof-v1",
    );
    assert_eq!(generation.generation(), after_handoff + 1);

    let cluster_state =
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).expect("cluster state");
    let detached_send_runtime =
        daemon_dfs_detached_send_runtime(&data_dir, &cluster_paths).expect("detached send runtime");
    let retained = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        &cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        None,
    )
    .expect("retained authority observer");
    assert!(Arc::ptr_eq(&generation, &retained));
    let retained_pointer = Arc::as_ptr(&retained);
    let before_no_cache_change = retained.generation();
    drop(generation);
    drop(retained);

    let retained_after_change = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        &cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        None,
    )
    .expect("authority observer retained without a message cache lane");
    assert_eq!(Arc::as_ptr(&retained_after_change), retained_pointer);
    append(&retained_after_change, &shard_key, b"shard-v2");
    assert_eq!(
        retained_after_change.generation(),
        before_no_cache_change + 1,
        "a retained no-cache observer must apply committed topology deltas"
    );
}

#[test]
fn daemon_dfs_revision_coverage_accepts_multi_key_transaction_revision() {
    let events = vec![
        sinomo_core::kv::KvEvent {
            revision: KvRevision(8),
            key: "/dfs/filesystems/sifs/config".to_string(),
            kind: sinomo_core::kv::KvEventKind::Delete,
        },
        sinomo_core::kv::KvEvent {
            revision: KvRevision(8),
            key: "/dfs/filesystems/sifs/metadata-shards/root".to_string(),
            kind: sinomo_core::kv::KvEventKind::Delete,
        },
    ];

    assert!(daemon_kv_events_cover_revision_range(
        KvRevision(7),
        KvRevision(8),
        &events,
    ));
}

#[test]
fn daemon_dfs_metadata_authority_delta_rejects_revision_gaps() {
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let shard_key = metadata_shard_key(&file_system_id, "root");
    let generation = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    assert!(
        generation.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(1, vec![(shard_key.clone(), b"shard-v1".to_vec())],)
        ),)
    );
    let initial_generation = generation.generation();
    let initial_rebuilds = generation.projection_rebuild_count();

    let complete = sinomo_core::kv::ClusterKvCommittedDelta {
        revision: KvRevision(2),
        compacted_revision: KvRevision::ZERO,
        events: vec![sinomo_core::kv::KvEvent {
            revision: KvRevision(2),
            key: shard_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put {
                value: b"shard-v2".to_vec(),
            },
        }],
        final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
            key: shard_key.clone(),
            entry: Some(KvEntry {
                key: shard_key.clone(),
                value: b"shard-v2".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(2),
                version: KvVersion(2),
                lease_id: None,
            }),
        }],
    };
    assert!(generation.observe_delta(&complete));
    assert_eq!(generation.generation(), initial_generation + 1);
    let complete_generation = generation.generation();
    assert_eq!(generation.projection_rebuild_count(), initial_rebuilds);
    assert_eq!(generation.debug_snapshot().revision, Some(KvRevision(2)));
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(complete_generation),
        Some(KvRevision(2)),
        "a contiguous retained projection must expose a generation-scoped readiness certificate",
    );

    let gapped = sinomo_core::kv::ClusterKvCommittedDelta {
        revision: KvRevision(4),
        compacted_revision: KvRevision::ZERO,
        events: vec![sinomo_core::kv::KvEvent {
            revision: KvRevision(4),
            key: shard_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put {
                value: b"shard-v2".to_vec(),
            },
        }],
        final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
            key: shard_key.clone(),
            entry: Some(KvEntry {
                key: shard_key.clone(),
                value: b"shard-v2".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(4),
                version: KvVersion(3),
                lease_id: None,
            }),
        }],
    };
    assert!(!generation.observe_delta(&gapped));
    assert_eq!(
        generation.generation(),
        complete_generation,
        "a revision gap must not manufacture a topology transition before the replacement proof is compared",
    );
    let gapped_generation = generation.generation();
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(complete_generation),
        None,
        "the current generation must remain fail-closed while its retained projection has a gap",
    );
    {
        let projection = generation.debug_snapshot();
        assert_eq!(projection.revision, None);
        assert_eq!(projection.required_revision, KvRevision(4));
        assert_eq!(
            projection.entry_count, 1,
            "the last exact topology remains comparison-only while readiness is withdrawn",
        );
        assert!(projection.topology_comparison_baseline);
    }
    assert!(
        !generation
            .shard_projection_state(&file_system_id, &DfsMetadataShardId::new("root"))
            .1,
        "a missed delta must withdraw exact-lane authority immediately"
    );

    assert!(!generation.install_authoritative_projection(
        test_dfs_authoritative_prefix_projection(&default_dfs_reactor_test_snapshot(
            2,
            vec![(shard_key.clone(), b"stale-checkpoint".to_vec())],
        )),
    ));
    assert_eq!(generation.generation(), gapped_generation);
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(gapped_generation),
        None,
        "a stale checkpoint below the required revision must not restore readiness",
    );
    assert!(
        generation.debug_snapshot().entry_count > 0,
        "a stale checkpoint below the missing revision must not erase the comparison baseline"
    );

    assert!(
        generation.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(4, vec![(shard_key.clone(), b"shard-v2".to_vec())],)
        ),)
    );
    assert_eq!(
        generation.generation(),
        gapped_generation,
        "closing a revision gap with equal topology must not reopen resident metadata"
    );
    assert_eq!(generation.debug_snapshot().revision, Some(KvRevision(4)),);
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(gapped_generation),
        Some(KvRevision(4)),
        "only a checkpoint closing the revision gap may restore readiness",
    );

    assert!(
        generation.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(5, vec![(shard_key, b"shard-v5".to_vec())],)
        ),)
    );
    assert_eq!(
        generation.generation(),
        gapped_generation + 1,
        "a changed authoritative replacement must still advance the resident generation",
    );
}

#[test]
fn daemon_dfs_metadata_authority_delta_invalidates_prefix_overflow() {
    let generation = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let entries = (0..sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map(|index| {
            (
                format!("{DAEMON_DFS_METADATA_AUTHORITY_PREFIX}overflow/records/{index:04}"),
                vec![b'x'],
            )
        })
        .collect();
    assert!(
        generation.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(1, entries,)
        ),)
    );
    let overflow_key = format!("{DAEMON_DFS_METADATA_AUTHORITY_PREFIX}overflow/records/extra");
    let overflow_entry = KvEntry {
        key: overflow_key.clone(),
        value: vec![b'y'],
        create_revision: KvRevision(2),
        modify_revision: KvRevision(2),
        version: KvVersion(1),
        lease_id: None,
    };
    assert!(
        !generation.observe_delta(&sinomo_core::kv::ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: vec![sinomo_core::kv::KvEvent {
                revision: KvRevision(2),
                key: overflow_key.clone(),
                kind: sinomo_core::kv::KvEventKind::Put {
                    value: overflow_entry.value.clone(),
                },
            }],
            final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
                key: overflow_key,
                entry: Some(overflow_entry),
            }],
        },)
    );
    let projection = generation.debug_snapshot();
    assert_eq!(projection.revision, None);
    assert_eq!(projection.required_revision, KvRevision(2));
    assert_eq!(projection.entry_count, 0);
    assert_eq!(projection.metadata_file_system_count, 0);
}

#[test]
fn metadata_authority_change_schedules_inherited_local_primary_backlog() {
    let root = temp_root("daemon-dfs-authority-inherited-backlog");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_inherited_backlog_local").expect("node");
    let remote = NodeId::from_legacy_named("node_dfs_inherited_backlog_remote").expect("node");
    write_test_cluster_state(&data_root, &local);
    let state =
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).expect("cluster state");
    let file_system = distributed_sifs(local.clone());
    let mut shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: remote.clone(),
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("remote-primary shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("initial remote authority");
    let mut inherited =
        sinomo_dfs::open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("local replica backend");
    inherited
        .put_file_system(&file_system)
        .expect("replica filesystem");
    let inherited_object = distributed_sifs_bootstrap_objects(local.clone())
        .into_iter()
        .next()
        .expect("inherited object");
    inherited
        .put_object(&inherited_object)
        .expect("inherited journal entry");
    drop(inherited);

    let runtime = daemon_test_runtime("daemon-dfs-authority-inherited-backlog");
    let paths = DaemonPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");
    let generation = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &state.cluster_id,
            &local,
            runtime.handle(),
        ))
        .expect("authority observer preflight")
        .authority;
    let initial_generation = generation.generation();
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);

    shard.primary_node_id = local.clone();
    shard.replica_node_ids = vec![remote];
    shard.epoch = 2;
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("local-primary shard op"),
        ),
    )
    .expect("transfer authority to local node");

    let deadline = Instant::now() + Duration::from_secs(3);
    while daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) == initial_requests
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(generation.generation(), initial_generation + 1);
    assert!(
        daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) > initial_requests,
        "the authority edge must publish an inherited journal without another DFS mutation"
    );
}

#[test]
fn metadata_authority_discovery_rolls_resident_generation_without_publish_lanes() {
    let root = temp_root("daemon-dfs-authority-idle-resident-rollover");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_idle_rollover_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_idle_rollover_remote").expect("remote");
    write_test_cluster_state(&data_root, &local);
    let file_system = distributed_sifs(local.clone());
    let mut shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: remote,
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("remote-primary shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("initial remote-only authority");

    let runtime = daemon_test_runtime("daemon-dfs-authority-idle-resident-rollover");
    let paths = DaemonPaths::new(&data_dir);
    let control_owner =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("data-owner capability"));
    register_daemon_dfs_data_owner_lock(&paths, &control_owner)
        .expect("register data-owner capability");

    assert!(
        runtime
            .block_on(daemon_dfs_metadata_publish_discovery_lanes_boxed_async(
                &data_dir,
                &cluster_paths,
                runtime.handle(),
            ))
            .expect("initial remote-only discovery")
            .is_empty(),
        "a node with no primary assignment must have no publish lane"
    );
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .expect("peer snapshot")
        .expect("clustered peer snapshot");
    // The bounded observation deliberately uses try_lock on the process-wide
    // resident cache. Other parallel tests may be publishing an unrelated
    // entry; wait only for that documented transient, not authority failures.
    let bounded_resident = || {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match daemon_dfs_bounded_status_resident(&data_dir, &peer_snapshot) {
                Err(sinomo_dfs::DfsError::Network { ref message })
                    if message
                        == "bounded DFS status unavailable while the resident generation cache is being updated"
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                result => break result,
            }
        }
    };
    bounded_resident().expect("initial idle resident generation");
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &peer_snapshot.cluster_id,
        &local,
        Some(runtime.handle()),
    )
    .expect("metadata authority");
    let initial_generation = authority.generation();

    shard.epoch = shard.epoch.saturating_add(1);
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("advanced remote-primary shard op"),
        ),
    )
    .expect("advance remote-only authority");
    let deadline = Instant::now() + Duration::from_secs(3);
    while authority.generation() == initial_generation && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(authority.generation() > initial_generation);

    assert!(
        runtime
            .block_on(daemon_dfs_metadata_publish_discovery_lanes_boxed_async(
                &data_dir,
                &cluster_paths,
                runtime.handle(),
            ))
            .expect("advanced remote-only discovery")
            .is_empty()
    );
    let resident = bounded_resident().expect("idle resident generation rolled forward");
    assert!(resident.bounded_status_snapshot().is_ok());
    let cache = daemon_dfs_message_service_cache()
        .lock()
        .expect("message cache");
    let entry = cache.get(&data_dir.root()).expect("resident cache entry");
    assert_eq!(
        entry.signature.metadata_authority_generation,
        authority.generation()
    );
    assert!(entry.signature.metadata_authority_generation > initial_generation);
    drop(cache);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"));
}

#[test]
fn materialized_snapshot_replacement_cannot_advance_metadata_authority() {
    let root = temp_root("daemon-dfs-materialized-authority-is-observational");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local =
        NodeId::from_legacy_named("node_dfs_materialized_authority_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_materialized_authority_remote").expect("remote node");
    write_test_cluster_state(&data_root, &local);
    let file_system = distributed_sifs(local.clone());
    let mut shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: remote.clone(),
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config op"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("remote-primary shard op"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("initial remote authority");
    let runtime = daemon_test_runtime("daemon-dfs-materialized-authority-observer");
    let generation = daemon_dfs_metadata_authority_generation(&cluster_paths, runtime.handle())
        .expect("canonical authority observer");
    wait_for_daemon_dfs_metadata_authority_projection(&generation);
    let initial_generation = generation.generation();

    shard.primary_node_id = local.clone();
    shard.replica_node_ids = vec![remote];
    shard.epoch = 2;
    let current = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
        .expect("current snapshot before external materialization");
    let mut external_store =
        sinomo_core::kv::InProcessKvStore::restore(current).expect("restore external store");
    external_store
        .put_serde(
            metadata_shard_key(&file_system.file_system_id, "root"),
            &shard,
            None,
        )
        .expect("apply externally committed authority");
    let external_snapshot = external_store.snapshot().expect("external snapshot");
    // Write through the R0 materialized-snapshot API, deliberately bypassing
    // sinomo-cluster's in-process committed-snapshot observer. This models
    // a separate CLI process advancing the shared Docker data directory.
    sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &external_snapshot)
        .expect("materialize external authority snapshot");
    assert_eq!(
        generation.generation(),
        initial_generation,
        "replacing the observational checkpoint must not change committed DFS authority"
    );

    let observed_revision = generation
        .current_complete_prefix_revision()
        .expect("retained authority revision");
    let committed = append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("canonical shard op"),
        ),
    )
    .expect("commit canonical authority change");
    let events = committed
        .events
        .iter()
        .filter(|event| event.revision > observed_revision)
        .cloned()
        .collect::<Vec<_>>();
    let final_key_states = events
        .iter()
        .map(|event| event.key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|key| sinomo_core::kv::ClusterKvCommittedKeyState {
            entry: committed.get(&key).cloned(),
            key,
        })
        .collect();
    assert!(
        generation.observe_delta(&sinomo_core::kv::ClusterKvCommittedDelta {
            revision: committed.revision,
            compacted_revision: committed.compacted_revision,
            events,
            final_key_states,
        },)
    );
    assert_eq!(generation.generation(), initial_generation + 1);
}

#[test]
fn daemon_dfs_detached_send_runtime_is_scoped_to_cluster_and_local_identity() {
    let root = temp_root("daemon_dfs_detached_send_runtime_identity");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_runtime_identity").expect("local");
    write_test_cluster_state(&data_root, &local);
    let cluster_state =
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).expect("cluster state");

    let original = daemon_dfs_detached_send_runtime_for_identity(
        &data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
    .expect("original identity runtime");
    let same = daemon_dfs_detached_send_runtime_for_identity(
        &data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
    .expect("same identity runtime");
    assert!(Arc::ptr_eq(&original, &same));

    let other_cluster =
        sinomo_core::Id::from_legacy_named("cluster_dfs_runtime_other").expect("other cluster");
    let other_local = NodeId::from_legacy_named("node_dfs_runtime_other").expect("other node");
    let changed_cluster = daemon_dfs_detached_send_runtime_for_identity(
        &data_dir,
        &other_cluster,
        &cluster_state.local_node_id,
    )
    .expect("changed cluster runtime");
    let changed_local = daemon_dfs_detached_send_runtime_for_identity(
        &data_dir,
        &cluster_state.cluster_id,
        &other_local,
    )
    .expect("changed local runtime");
    assert!(!Arc::ptr_eq(&original, &changed_cluster));
    assert!(!Arc::ptr_eq(&original, &changed_local));
}

fn default_dfs_reactor_test_snapshot(revision: u64, entries: Vec<(String, Vec<u8>)>) -> KvSnapshot {
    let mut entries = entries
        .into_iter()
        .map(|(key, value)| KvEntry {
            key,
            value,
            create_revision: KvRevision(revision),
            modify_revision: KvRevision(revision),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    KvSnapshot {
        revision: KvRevision(revision),
        compacted_revision: KvRevision::ZERO,
        entries,
        leases: Vec::new(),
        events: Vec::new(),
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
    }
}

fn test_dfs_authoritative_prefix_projection(
    snapshot: &KvSnapshot,
) -> sinomo_dfs::DfsAuthoritativePrefixProjection {
    sinomo_dfs::DfsAuthoritativePrefixProjection::complete(
        DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
        snapshot
            .prefix_entries(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
            .cloned()
            .collect(),
        snapshot.revision,
    )
    .expect("valid test authoritative prefix projection")
}

fn wait_for_daemon_dfs_metadata_authority_projection(
    generation: &DaemonDfsMetadataAuthorityGeneration,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while generation.debug_snapshot().revision.is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        generation.debug_snapshot().revision.is_some(),
        "bounded authoritative prefix refresh did not publish a projection"
    );
}

#[test]
fn default_dfs_projection_skips_unrelated_commits_incrementally() {
    let config_key = sinomo_dfs::file_system_config_key(&sinomo_dfs::sifs_file_system_id());
    let (projection_tx, mut projection_rx) =
        tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
    let mut observer = DaemonDefaultDfsCommittedProjectionObserver::default();

    let initial = default_dfs_reactor_test_snapshot(1, vec![(config_key.clone(), vec![1])]);
    observer.observe_snapshot(&projection_tx, &initial);
    assert_eq!(observer.rebuild_count(), 1);
    let _ = projection_rx.borrow_and_update();

    let mut unrelated = default_dfs_reactor_test_snapshot(
        2,
        vec![
            (config_key.clone(), vec![1]),
            ("/unrelated/application/state".to_string(), vec![9]),
        ],
    );
    unrelated.events.push(sinomo_core::kv::KvEvent {
        revision: KvRevision(2),
        key: "/unrelated/application/state".to_string(),
        kind: sinomo_core::kv::KvEventKind::Put { value: vec![9] },
    });
    observer.observe_snapshot(&projection_tx, &unrelated);
    assert_eq!(
        observer.rebuild_count(),
        1,
        "an unrelated commit must not reconstruct the default DFS projection"
    );
    assert!(
        !projection_rx
            .has_changed()
            .expect("projection channel open"),
        "unrelated commits must not wake the reactor"
    );
    assert_eq!(projection_tx.borrow().revision, Some(KvRevision(2)));

    let mut relevant = default_dfs_reactor_test_snapshot(3, vec![(config_key.clone(), vec![2])]);
    relevant.events.push(sinomo_core::kv::KvEvent {
        revision: KvRevision(3),
        key: config_key,
        kind: sinomo_core::kv::KvEventKind::Put { value: vec![2] },
    });
    observer.observe_snapshot(&projection_tx, &relevant);
    assert_eq!(observer.rebuild_count(), 2);
    assert!(
        projection_rx
            .has_changed()
            .expect("projection channel open"),
        "relevant commits must wake the reactor"
    );
    let _ = projection_rx.borrow_and_update();

    let mut compacted = default_dfs_reactor_test_snapshot(
        4,
        vec![("/unrelated/application/state".to_string(), vec![10])],
    );
    compacted.compacted_revision = KvRevision(4);
    observer.observe_snapshot(&projection_tx, &compacted);
    assert_eq!(
        observer.rebuild_count(),
        3,
        "compaction must rebuild from the resident snapshot"
    );
}

#[test]
fn default_dfs_projection_applies_complete_delta_and_rebuilds_gaps() {
    let config_key = sinomo_dfs::file_system_config_key(&sinomo_dfs::sifs_file_system_id());
    let (projection_tx, mut projection_rx) =
        tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
    let mut observer = DaemonDefaultDfsCommittedProjectionObserver::default();

    observer.observe_snapshot(
        &projection_tx,
        &default_dfs_reactor_test_snapshot(1, vec![(config_key.clone(), vec![1])]),
    );
    assert_eq!(observer.rebuild_count(), 1);
    let _ = projection_rx.borrow_and_update();

    let complete = sinomo_core::kv::ClusterKvCommittedDelta {
        revision: KvRevision(2),
        compacted_revision: KvRevision::ZERO,
        events: vec![sinomo_core::kv::KvEvent {
            revision: KvRevision(2),
            key: config_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![2] },
        }],
        final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
            key: config_key.clone(),
            entry: Some(KvEntry {
                key: config_key.clone(),
                value: vec![2],
                create_revision: KvRevision(1),
                modify_revision: KvRevision(2),
                version: KvVersion(2),
                lease_id: None,
            }),
        }],
    };
    assert!(observer.observe_delta(&projection_tx, &complete));
    assert_eq!(
        observer.rebuild_count(),
        1,
        "a complete bounded delta must not rebuild the full snapshot projection"
    );
    assert!(
        projection_rx
            .has_changed()
            .expect("projection channel open"),
        "a relevant final-key-state change must wake default DFS reconciliation"
    );
    let changed = projection_rx.borrow_and_update().clone();
    assert_eq!(changed.revision, Some(KvRevision(2)));
    assert_eq!(changed.inputs, vec![(config_key.clone(), vec![2])]);

    assert!(observer.observe_delta(&projection_tx, &complete));
    assert!(
        !projection_rx
            .has_changed()
            .expect("projection channel open"),
        "an equal delta must not retrigger reconciliation"
    );

    let gapped = sinomo_core::kv::ClusterKvCommittedDelta {
        revision: KvRevision(4),
        compacted_revision: KvRevision::ZERO,
        events: vec![sinomo_core::kv::KvEvent {
            revision: KvRevision(4),
            key: config_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![4] },
        }],
        final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
            key: config_key.clone(),
            entry: Some(KvEntry {
                key: config_key,
                value: vec![4],
                create_revision: KvRevision(1),
                modify_revision: KvRevision(4),
                version: KvVersion(3),
                lease_id: None,
            }),
        }],
    };
    assert!(
        !observer.observe_delta(&projection_tx, &gapped),
        "a missed revision must request a conservative resident-snapshot rebuild"
    );
    assert_eq!(observer.revision(), Some(KvRevision(2)));
    assert_eq!(projection_tx.borrow().revision, Some(KvRevision(2)));
}

#[test]
fn default_dfs_reactor_starts_dirty_and_coalesces_snapshot_inputs() {
    let root = temp_root("default-dfs-reactor-coalesces-inputs");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_coalesce").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");

    let startup = reactor.pending_work().expect("dirty startup work");
    assert!(startup.reconcile_defaults);
    assert!(startup.recover_prior_process_permit);
    assert!(startup.refresh_before_recovery);
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);
    assert_eq!(reactor.pending_work(), None);

    let config_key = sinomo_dfs::file_system_config_key(&sinomo_dfs::sifs_file_system_id());
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(1, vec![(config_key.clone(), vec![1])]),
    );
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(2, vec![(config_key, vec![2])]),
    );

    let changed = reactor.pending_work().expect("coalesced input change");
    assert!(changed.reconcile_defaults);
    assert!(!changed.recover_prior_process_permit);
    assert!(!changed.refresh_before_recovery);
    assert!(changed.inputs.iter().any(|(_, value)| value == &vec![2]));
    reactor.complete_default_reconciliation(&changed);
    assert_eq!(
        reactor.pending_work(),
        None,
        "the latest committed input generation must reconcile exactly once"
    );
}

#[test]
fn default_dfs_reactor_consumes_local_replica_progress_once() {
    let root = temp_root("default-dfs-reactor-local-replica-progress");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_local_progress").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);
    assert_eq!(reactor.pending_work(), None);

    notify_daemon_default_dfs_reconciliation_replica_progress(&cluster_paths);
    let progress = reactor.pending_work().expect("local replica progress");
    assert!(progress.reconcile_defaults);
    assert!(!progress.recover_prior_process_permit);
    reactor.complete_default_reconciliation(&progress);
    assert_eq!(
        reactor.pending_work(),
        None,
        "one local replica generation must reconcile exactly once"
    );
}

#[test]
fn default_dfs_parked_handoff_ignores_unowned_inputs_and_completes_once() {
    let root = temp_root("default-dfs-reactor-parked-handoff");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_parked_promoted").expect("local node");
    let retiring_primary =
        NodeId::from_legacy_named("node_default_dfs_parked_retiring").expect("retiring node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);

    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let retiring = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: retiring_primary.clone(),
        replica_node_ids: vec![local.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired = DfsMetadataShardConfig {
        primary_node_id: local.clone(),
        replica_node_ids: vec![retiring_primary.clone()],
        epoch: 2,
        ..retiring.clone()
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        handoff_id: "handoff-default-dfs-parked".to_string(),
        retiring_shard: retiring.clone(),
        desired_shard: desired.clone(),
        required_prepared_sequence: 0,
        required_head_watermark: None,
        retiring_primary_fence: None,
    };
    let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "root");
    let handoff_key = sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "root");
    let proof_key =
        sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "root");
    let unrelated_key =
        format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}{local}/{retiring_primary}/test-endpoint");
    let parked_entries = vec![
        (
            shard_key.clone(),
            sinomo_core::kv::encode_kv_value(&retiring).expect("retiring shard"),
        ),
        (
            handoff_key.clone(),
            sinomo_core::kv::encode_kv_value(&handoff).expect("handoff"),
        ),
        (unrelated_key.clone(), vec![1]),
    ];
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(1, parked_entries.clone()),
    );
    let work = reactor.pending_work().expect("handoff input generation");
    let parked = DaemonDefaultDfsParkedHandoff::new(
        file_system_id.clone(),
        "root".to_string(),
        handoff.handoff_id.clone(),
        retiring.epoch,
        desired.epoch,
    );
    assert_eq!(
        daemon_default_dfs_locally_owned_pending_handoff(&retiring_primary, &work.inputs),
        Some(parked.clone()),
        "a non-voter retiring primary must inherit the committed handoff maintenance edge"
    );
    assert_eq!(
        daemon_default_dfs_locally_owned_pending_handoff(&local, &work.inputs),
        None,
        "the promoted node must not consume retiring-primary maintenance work"
    );
    assert!(parked.is_owned_by_retiring_primary(&retiring_primary, &work.inputs));
    assert!(
        !parked.is_owned_by_retiring_primary(&local, &work.inputs),
        "only the exact retiring primary should receive the maintenance wake"
    );
    reactor.park_default_reconciliation(&work, parked);
    assert_eq!(
        reactor.pending_work(),
        None,
        "recording the durable wait must consume the attempted generation without a failure loop"
    );

    let mut unrelated_entries = parked_entries;
    unrelated_entries
        .iter_mut()
        .find(|(key, _)| key == &unrelated_key)
        .expect("unrelated input")
        .1 = vec![2];
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(2, unrelated_entries),
    );
    assert_eq!(
        reactor.pending_work(),
        None,
        "connectivity/source churn cannot own a parked handoff retry"
    );

    let proof = sinomo_dfs::DfsMetadataShardMembershipHandoffProof {
        handoff_id: handoff.handoff_id,
        retiring_shard: retiring,
        desired_shard: desired.clone(),
        journal_sequence: 7,
        state_fingerprint: "blake3:default-dfs-parked".to_string(),
        acknowledged_node_ids: vec![retiring_primary.clone(), local.clone()],
    };
    let completed_entries = vec![
        (
            shard_key.clone(),
            sinomo_core::kv::encode_kv_value(&desired).expect("active shard"),
        ),
        (
            proof_key.clone(),
            sinomo_core::kv::encode_kv_value(&proof).expect("completion proof"),
        ),
        (unrelated_key, vec![2]),
    ];
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(3, completed_entries.clone()),
    );
    let completion = reactor
        .pending_work()
        .expect("proof and authority completion generation");
    let parked = reactor
        .runtime
        .parked_handoff()
        .expect("exact parked generation");
    assert!(
        parked.is_completed_by_desired_primary(&local, &completion.inputs),
        "the reactor's actual local desired primary must inherit a proactive rebind wake"
    );
    assert!(
        !parked.is_completed_by_desired_primary(&retiring_primary, &completion.inputs),
        "the retiring primary must not consume the promoted-primary rebind edge"
    );
    let same_primary_desired = DfsMetadataShardConfig {
        primary_node_id: retiring_primary.clone(),
        replica_node_ids: vec![local.clone()],
        ..desired.clone()
    };
    let same_primary_proof = DfsMetadataShardMembershipHandoffProof {
        desired_shard: same_primary_desired.clone(),
        ..proof.clone()
    };
    let same_primary_inputs = vec![
        (
            shard_key,
            sinomo_core::kv::encode_kv_value(&same_primary_desired)
                .expect("same-primary active shard"),
        ),
        (
            proof_key,
            sinomo_core::kv::encode_kv_value(&same_primary_proof).expect("same-primary proof"),
        ),
    ];
    assert!(
        !parked.is_completed_by_desired_primary(&retiring_primary, &same_primary_inputs,),
        "same-primary membership changes complete in the retiring lane and must not schedule promoted-primary rebind"
    );
    assert!(
        reactor
            .complete_default_reconciliation_after_control_plane_observation(&completion, &local,),
        "completion must transfer the exact promoted generation to maintenance"
    );
    let follow_up = reactor
        .pending_work()
        .expect("completed handoff must force one lifecycle follow-up");
    assert!(
        follow_up.reconcile_defaults,
        "the follow-up must re-evaluate the completed coherent projection"
    );
    assert_eq!(follow_up.inputs, completion.inputs);
    reactor.complete_default_reconciliation(&follow_up);
    assert_eq!(
        reactor.pending_work(),
        None,
        "a converged follow-up must settle without a reconciliation loop"
    );
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(4, completed_entries),
    );
    assert_eq!(
        reactor.pending_work(),
        None,
        "the same completed handoff generation must not activate or clear twice"
    );
}

#[test]
fn parked_default_dfs_projection_rebuilds_only_for_exact_wake_owners() {
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let parked = DaemonDefaultDfsParkedHandoff::new(
        file_system_id.clone(),
        "services".to_string(),
        "handoff-projection-parked".to_string(),
        4,
        5,
    );
    let parked_subscription = Arc::new(Mutex::new(Some(parked)));
    let mut observer = DaemonDefaultDfsCommittedProjectionObserver::new_with_parked_subscription(
        parked_subscription,
    );
    let (projection_tx, mut projection_rx) =
        tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
    let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "services");
    let proof_key =
        sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "services");
    let connectivity_key =
        format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}source/target/endpoint");

    let initial = default_dfs_reactor_test_snapshot(
        1,
        vec![
            (shard_key.clone(), vec![1]),
            (connectivity_key.clone(), vec![1]),
        ],
    );
    observer.observe_snapshot(&projection_tx, &initial);
    assert_eq!(observer.rebuild_count(), 1);
    let _ = projection_rx.borrow_and_update();

    let mut unrelated = default_dfs_reactor_test_snapshot(
        2,
        vec![
            (shard_key.clone(), vec![1]),
            (connectivity_key.clone(), vec![2]),
        ],
    );
    unrelated.events.push(sinomo_core::kv::KvEvent {
        revision: KvRevision(2),
        key: connectivity_key.clone(),
        kind: sinomo_core::kv::KvEventKind::Put { value: vec![2] },
    });
    observer.observe_snapshot(&projection_tx, &unrelated);
    assert_eq!(
        observer.rebuild_count(),
        1,
        "a parked projection consumes deltas without reopening/rebuilding resident state for unrelated connectivity"
    );
    assert!(!projection_rx.has_changed().expect("projection open"));
    assert!(
        projection_tx
            .borrow()
            .inputs
            .iter()
            .any(|(key, value)| key == &connectivity_key && value == &vec![2]),
        "the ignored wake must still update the resident projection for the generation after handoff completion"
    );

    let mut proof = default_dfs_reactor_test_snapshot(
        3,
        vec![(shard_key, vec![1]), (proof_key.clone(), vec![9])],
    );
    proof.events.push(sinomo_core::kv::KvEvent {
        revision: KvRevision(3),
        key: proof_key,
        kind: sinomo_core::kv::KvEventKind::Put { value: vec![9] },
    });
    observer.observe_snapshot(&projection_tx, &proof);
    assert_eq!(observer.rebuild_count(), 2);
    assert!(
        projection_rx.has_changed().expect("projection open"),
        "the exact completion proof must wake the parked owner"
    );
}

#[test]
fn default_dfs_failure_retry_generation_excludes_unrelated_projection_wakes() {
    let root = temp_root("default-dfs-failure-retry-generation");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local =
        NodeId::from_legacy_named("node_default_dfs_failure_generation").expect("local node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "users");
    let connectivity_key = format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}failure/source/target");
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            1,
            vec![
                (shard_key.clone(), vec![1]),
                (connectivity_key.clone(), vec![1]),
            ],
        ),
    );
    let failed_generation = reactor.exact_retry_generation();

    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            2,
            vec![(shard_key.clone(), vec![1]), (connectivity_key, vec![2])],
        ),
    );
    assert_eq!(
        reactor.exact_retry_generation(),
        failed_generation,
        "an unrelated source wake must preserve the existing failure backoff deadline"
    );

    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(3, vec![(shard_key, vec![2])]),
    );
    assert_ne!(
        reactor.exact_retry_generation(),
        failed_generation,
        "a committed shard-authority generation may wake before the failure deadline"
    );
}

#[test]
fn default_dfs_reactor_endpoint_topology_ignores_observed_latency() {
    let root = temp_root("default-dfs-reactor-endpoint-topology");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_endpoint").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);

    let mut endpoint = ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_default_dfs_topology")
            .expect("endpoint"),
        node_id: local.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: "10.0.0.1:21353".to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 10,
        last_observed_latency_ms: Some(1),
    };
    let endpoint_key = format!(
        "{DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX}{}/{}",
        local, endpoint.endpoint_id
    );
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            1,
            vec![(
                endpoint_key.clone(),
                sinomo_core::kv::encode_kv_value(&endpoint).expect("endpoint value"),
            )],
        ),
    );
    let published = reactor.pending_work().expect("endpoint generation");
    reactor.complete_default_reconciliation(&published);

    endpoint.last_observed_latency_ms = Some(250);
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            2,
            vec![(
                endpoint_key.clone(),
                sinomo_core::kv::encode_kv_value(&endpoint).expect("refreshed endpoint"),
            )],
        ),
    );
    assert_eq!(
        reactor.pending_work(),
        None,
        "latency observations are not DFS placement topology"
    );

    endpoint.address = "10.0.0.2:21353".to_string();
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            3,
            vec![(
                endpoint_key,
                sinomo_core::kv::encode_kv_value(&endpoint).expect("moved endpoint"),
            )],
        ),
    );
    assert!(
        reactor
            .pending_work()
            .is_some_and(|work| work.reconcile_defaults),
        "endpoint address changes must rebuild failover placement"
    );
}

#[test]
fn default_dfs_reactor_tracks_service_provider_semantics_without_refresh_loop() {
    let root = temp_root("default-dfs-reactor-service-provider");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_service_provider").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);

    let key = sinomo_core::service::cluster_service_instance_key("dfs", &local);
    let mut instance = sinomo_core::service::ServiceInstanceRecord::new("dfs", local.clone());
    instance.routes = vec!["dfs.metadata".to_string()];
    instance.provider_capabilities = vec!["dfs.metadata.replicate".to_string()];
    instance.updated_at = time::OffsetDateTime::now_utc();
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            1,
            vec![(
                key.clone(),
                sinomo_core::kv::encode_kv_value(&instance).expect("service instance"),
            )],
        ),
    );
    let discovered = reactor.pending_work().expect("provider discovery");
    assert!(discovered.reconcile_defaults);
    reactor.complete_default_reconciliation(&discovered);

    instance.registry_version = 2;
    instance.updated_at += time::Duration::seconds(30);
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            2,
            vec![(
                key.clone(),
                sinomo_core::kv::encode_kv_value(&instance).expect("refreshed service instance"),
            )],
        ),
    );
    assert_eq!(
        reactor.pending_work(),
        None,
        "service registry refreshes are not placement topology"
    );

    instance.provider_capabilities.clear();
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            3,
            vec![(
                key,
                sinomo_core::kv::encode_kv_value(&instance).expect("changed service capabilities"),
            )],
        ),
    );
    assert!(
        reactor
            .pending_work()
            .is_some_and(|work| work.reconcile_defaults),
        "metadata-provider capability changes must rebuild placement"
    );
}

#[test]
fn default_dfs_reactor_repairs_semantic_advertisement_drift_without_refresh_loop() {
    let root = temp_root("default-dfs-reactor-semantic-advertisement");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_output").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);

    let metadata = sinomo_dfs::DfsNodeAdvertisement::metadata(local.clone());
    let content = sinomo_dfs::DfsNodeAdvertisement::content(local.clone());
    let snapshot = default_dfs_reactor_test_snapshot(
        1,
        vec![
            (
                sinomo_dfs::metadata_service_key(&local),
                sinomo_core::kv::encode_kv_value(&metadata).expect("metadata ad"),
            ),
            (
                sinomo_dfs::content_service_key(&local),
                sinomo_core::kv::encode_kv_value(&content).expect("content ad"),
            ),
        ],
    );
    observe_daemon_default_dfs_reconciliation(&reactor._projection_tx, &local, &snapshot);
    let published = reactor
        .pending_work()
        .expect("semantic advertisement generation");
    assert!(published.reconcile_defaults);
    reactor.complete_default_reconciliation(&published);

    let mut refreshed_metadata = metadata.clone();
    refreshed_metadata.updated_at += time::Duration::seconds(30);
    let mut refreshed_content = content.clone();
    refreshed_content.updated_at += time::Duration::seconds(30);
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            2,
            vec![
                (
                    sinomo_dfs::metadata_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&refreshed_metadata)
                        .expect("refreshed metadata ad"),
                ),
                (
                    sinomo_dfs::content_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&refreshed_content)
                        .expect("refreshed content ad"),
                ),
            ],
        ),
    );
    assert_eq!(
        reactor.pending_work(),
        None,
        "updated_at-only advertisement refreshes must remain quiet"
    );

    refreshed_metadata.accepts_metadata = false;
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(
            3,
            vec![
                (
                    sinomo_dfs::metadata_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&refreshed_metadata)
                        .expect("invalid metadata ad"),
                ),
                (
                    sinomo_dfs::content_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&refreshed_content).expect("content ad"),
                ),
            ],
        ),
    );
    let corrupted = reactor
        .pending_work()
        .expect("semantic advertisement corruption");
    assert!(
        corrupted.reconcile_defaults,
        "semantic advertisement corruption must request repair"
    );
    reactor.complete_default_reconciliation(&corrupted);
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(4, Vec::new()),
    );
    assert!(
        reactor
            .pending_work()
            .is_some_and(|work| work.reconcile_defaults),
        "advertisement deletion must request self-healing publication"
    );
}

#[test]
fn default_dfs_reactor_permit_change_requests_only_exact_recovery() {
    let root = temp_root("default-dfs-reactor-permit-change");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    cluster_paths.ensure().expect("cluster paths");
    let local = NodeId::from_legacy_named("node_default_dfs_permit").expect("node");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let startup = reactor.pending_work().expect("startup work");
    reactor.complete_default_reconciliation(&startup);
    reactor.complete_prior_process_recovery(&startup);

    let permit_key = sinomo_dfs::placement_authority_permit_key(&sinomo_dfs::sifs_file_system_id());
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(1, vec![(permit_key.clone(), vec![9])]),
    );
    let changed = reactor.pending_work().expect("permit change work");
    assert!(!changed.reconcile_defaults);
    assert!(changed.recover_prior_process_permit);
    assert!(
        !changed.refresh_before_recovery,
        "a committed exact-key observation is already the refresh edge"
    );
    reactor.complete_prior_process_recovery(&changed);

    // A watch channel may coalesce these two committed snapshots before
    // the reactor runs. Value-only tracking would mistake the final permit
    // for the already-recovered generation and skip exact recovery.
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(2, Vec::new()),
    );
    observe_daemon_default_dfs_reconciliation(
        &reactor._projection_tx,
        &local,
        &default_dfs_reactor_test_snapshot(3, vec![(permit_key, vec![9])]),
    );
    let reinserted = reactor
        .pending_work()
        .expect("identical reinsert is a new exact generation");
    assert!(!reinserted.reconcile_defaults);
    assert!(reinserted.recover_prior_process_permit);
    assert_eq!(
        reinserted
            .placement_authority_permit
            .as_ref()
            .map(|permit| permit.modify_revision),
        Some(KvRevision(3))
    );
}

#[test]
fn daemon_dfs_metadata_authority_observer_is_scoped_to_cluster_incarnation() {
    let root = temp_root("daemon_dfs_authority_observer_cluster_incarnation");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("root layout");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_authority_incarnation").expect("local");
    let old_cluster =
        sinomo_core::Id::from_legacy_named("cluster_dfs_authority_old").expect("old cluster");
    let new_cluster =
        sinomo_core::Id::from_legacy_named("cluster_dfs_authority_new").expect("new cluster");
    let shard_key = metadata_shard_key(&sinomo_dfs::sifs_file_system_id(), "root");
    let snapshot = |revision: u64, value: &[u8]| KvSnapshot {
        revision: KvRevision(revision),
        compacted_revision: KvRevision::ZERO,
        entries: vec![KvEntry {
            key: shard_key.clone(),
            value: value.to_vec(),
            create_revision: KvRevision(revision),
            modify_revision: KvRevision(revision),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        }],
        leases: Vec::new(),
        events: vec![sinomo_core::kv::KvEvent {
            revision: KvRevision(revision),
            key: shard_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put {
                value: value.to_vec(),
            },
        }],
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
    };

    let old_runtime =
        daemon_dfs_detached_send_runtime_for_identity(&data_dir, &old_cluster, &local)
            .expect("old runtime");
    let old_observer = daemon_dfs_retained_metadata_authority_generation(
        old_runtime.as_ref(),
        &cluster_paths,
        &old_cluster,
        &local,
        None,
    )
    .expect("old observer");
    assert!(old_observer.install_authoritative_projection(
        test_dfs_authoritative_prefix_projection(&snapshot(100, b"old topology")),
    ));
    let old_generation = old_observer.generation();
    assert!(!old_observer.install_authoritative_projection(
        test_dfs_authoritative_prefix_projection(&snapshot(1, b"new topology")),
    ));
    assert_eq!(
        old_observer.generation(),
        old_generation,
        "one observer must reject a stale revision"
    );

    let new_runtime =
        daemon_dfs_detached_send_runtime_for_identity(&data_dir, &new_cluster, &local)
            .expect("new runtime");
    let new_observer = daemon_dfs_retained_metadata_authority_generation(
        new_runtime.as_ref(),
        &cluster_paths,
        &new_cluster,
        &local,
        None,
    )
    .expect("new observer");
    assert!(
        !Arc::ptr_eq(&old_observer, &new_observer),
        "a replacement cluster on the same data root needs a fresh revision domain"
    );
    let new_generation = new_observer.generation();
    assert!(new_observer.install_authoritative_projection(
        test_dfs_authoritative_prefix_projection(&snapshot(1, b"new topology")),
    ));
    assert_eq!(
        new_observer.generation(),
        new_generation,
        "the replacement cluster must accept its lower starting revision without rolling an authority generation that was never valid"
    );
}

#[test]
fn daemon_dfs_message_cache_rebuilds_only_for_metadata_authority_changes() {
    let root = temp_root("daemon_dfs_message_authority_cache");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_authority_cache_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_authority_cache_remote").expect("remote");
    write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "10.78.0.1:21353");
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
            key: "/cluster/authority-cache-unrelated".to_string(),
            value: b"initial".to_vec(),
            lease_id: None,
        }),
    )
    .expect("initial unrelated KV");
    let runtime = daemon_test_runtime("daemon-dfs-message-authority-cache");
    let cache_key = data_dir.root();

    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_authority_cache_1",
            test_dfs_lookup_routed_message(&remote, &local, 1, "/services/cache-1.txt"),
        ),
        runtime.handle(),
    )
    .expect("first DFS message");
    let first_lane = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        Arc::clone(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        )
    };
    let first_lane_weak = Arc::downgrade(&first_lane);

    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
            key: "/cluster/authority-cache-unrelated".to_string(),
            value: b"changed".to_vec(),
            lease_id: None,
        }),
    )
    .expect("unrelated KV churn");
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_authority_cache_2",
            test_dfs_lookup_routed_message(&remote, &local, 2, "/services/cache-2.txt"),
        ),
        runtime.handle(),
    )
    .expect("second DFS message");
    let unchanged_lane = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        Arc::clone(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        )
    };
    assert!(Arc::ptr_eq(&first_lane, &unchanged_lane));

    let file_system = distributed_sifs(local.clone());
    let root_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: local.clone(),
        replica_node_ids: vec![remote.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .expect("filesystem config"),
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &root_shard,
                    None,
                )
                .expect("root shard"),
            ],
            failure: Vec::new(),
        }),
    )
    .expect("metadata authority KV");
    handle_daemon_dfs_message_on_runtime(
        &paths,
        test_dfs_message_command(
            "dfs_message_authority_cache_3",
            test_dfs_lookup_routed_message(&remote, &local, 3, "/services/cache-3.txt"),
        ),
        runtime.handle(),
    )
    .expect("third DFS message");
    let rebuilt_lane = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        Arc::clone(
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        )
    };
    assert!(!Arc::ptr_eq(&first_lane, &rebuilt_lane));
    drop(unchanged_lane);
    drop(first_lane);
    assert!(
        first_lane_weak.upgrade().is_none(),
        "a completed superseded service generation must be collectible"
    );
}

#[test]
fn daemon_dfs_prepared_deferred_worker_does_not_retain_service_lane() {
    let root = temp_root("daemon_dfs_detached_worker_releases_lane");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_detached_worker_local").expect("local");
    write_test_cluster_state(&data_root, &local);
    let runtime = daemon_test_runtime("daemon-dfs-detached-worker-releases-lane");
    let lane = open_daemon_dfs_message_service_lane(
        &data_dir,
        &cluster_paths,
        &local,
        Vec::new(),
        runtime.handle(),
    )
    .expect("open message service lane");
    let weak_lane = Arc::downgrade(&lane);
    let detached_runtime = Arc::clone(lane.detached_send_runtime());
    let worker = lane
        .lock()
        .expect("service lane")
        .service
        .detach_message_deferred_work();
    drop(lane);

    assert!(
        weak_lane.upgrade().is_none(),
        "a lightweight retry worker must not retain the metadata-heavy service lane"
    );
    assert!(!worker.has_pending().expect("inspect detached worker"));
    runtime.block_on(async {
        schedule_daemon_dfs_message_deferred_work(
            runtime.handle(),
            &cluster_paths,
            detached_runtime.clone(),
            worker,
        );
        tokio::task::yield_now().await;
        assert!(
            detached_runtime.worker_is_active(),
            "the lightweight shared-outbox watcher remains parked for restored work"
        );
    });
}

#[test]
fn daemon_dfs_deferred_worker_notifies_only_for_target_policy_changes() {
    let root = temp_root("daemon_dfs_detached_worker_policy_notifications");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_detached_policy_local").expect("local");
    let offline = NodeId::from_legacy_named("node_dfs_detached_policy_offline").expect("offline");
    write_test_cluster_state(&data_root, &local);
    let runtime = daemon_test_runtime("daemon-dfs-detached-worker-policy-notifications");
    let lane = open_daemon_dfs_message_service_lane(
        &data_dir,
        &cluster_paths,
        &local,
        Vec::new(),
        runtime.handle(),
    )
    .expect("open message service lane");
    let detached_runtime = Arc::clone(lane.detached_send_runtime());
    let worker = lane
        .lock()
        .expect("service lane")
        .service
        .detach_message_deferred_work();
    let inspection_worker = worker.clone();

    runtime.block_on(async {
        let baseline_generation = inspection_worker
            .scheduling_snapshot()
            .expect("baseline detached schedule")
            .2;
        let offline_policy = BTreeSet::from([offline]);
        schedule_daemon_dfs_message_deferred_work_with_policy(
            runtime.handle(),
            Arc::clone(&detached_runtime),
            worker.clone(),
            offline_policy.clone(),
        );
        tokio::task::yield_now().await;
        assert_eq!(
            inspection_worker
                .scheduling_snapshot()
                .expect("initial detached policy schedule")
                .2,
            baseline_generation,
            "starting the worker does not need a synthetic policy wake"
        );

        schedule_daemon_dfs_message_deferred_work_with_policy(
            runtime.handle(),
            Arc::clone(&detached_runtime),
            worker.clone(),
            offline_policy,
        );
        tokio::task::yield_now().await;
        assert_eq!(
            inspection_worker
                .scheduling_snapshot()
                .expect("unchanged detached policy schedule")
                .2,
            baseline_generation,
            "republishing an unchanged target policy must not wake the parked worker"
        );

        schedule_daemon_dfs_message_deferred_work_with_policy(
            runtime.handle(),
            Arc::clone(&detached_runtime),
            worker,
            BTreeSet::new(),
        );
        assert_eq!(
            inspection_worker
                .scheduling_snapshot()
                .expect("recovered detached policy schedule")
                .2,
            baseline_generation.wrapping_add(1),
            "an offline-to-online target policy change must wake the parked worker"
        );
    });
}

#[test]
fn daemon_dfs_deferred_runtime_is_shared_across_service_lanes() {
    let root = temp_root("daemon_dfs_deferred_runtime_coalesces_lanes");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named("node_dfs_deferred_coalesce_local").expect("local");
    write_test_cluster_state(&data_root, &local);
    let runtime = daemon_test_runtime("daemon-dfs-deferred-runtime-coalesces-lanes");
    let first_lane = open_daemon_dfs_message_service_lane(
        &data_dir,
        &cluster_paths,
        &local,
        Vec::new(),
        runtime.handle(),
    )
    .expect("open first lane");
    let second_lane = open_daemon_dfs_message_service_lane(
        &data_dir,
        &cluster_paths,
        &local,
        Vec::new(),
        runtime.handle(),
    )
    .expect("open second lane");
    assert!(Arc::ptr_eq(
        first_lane.detached_send_runtime(),
        second_lane.detached_send_runtime()
    ));
    let detached_runtime = Arc::clone(first_lane.detached_send_runtime());
    let first_worker = first_lane
        .lock()
        .expect("first lane")
        .service
        .detach_message_deferred_work();
    let second_worker = second_lane
        .lock()
        .expect("second lane")
        .service
        .detach_message_deferred_work();
    let first_weak = Arc::downgrade(&first_lane);
    let second_weak = Arc::downgrade(&second_lane);

    runtime.block_on(async {
        schedule_daemon_dfs_message_deferred_work(
            runtime.handle(),
            &cluster_paths,
            Arc::clone(&detached_runtime),
            first_worker,
        );
        schedule_daemon_dfs_message_deferred_work(
            runtime.handle(),
            &cluster_paths,
            Arc::clone(&detached_runtime),
            second_worker,
        );
        drop(first_lane);
        drop(second_lane);
        assert!(first_weak.upgrade().is_none());
        assert!(second_weak.upgrade().is_none());
        assert_eq!(
            detached_runtime.worker_schedule_generation(),
            2,
            "both service lanes must publish their wake to the shared runtime"
        );
        assert!(
            detached_runtime.worker_is_active(),
            "one lightweight watcher must own the shared outbox"
        );
    });
}

#[test]
fn dfs_service_members_include_authorized_voters_and_learners_only() {
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_service_members")
        .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_service_local").expect("local");
    let voter = NodeId::from_legacy_named("node_dfs_service_voter").expect("voter");
    let learner = NodeId::from_legacy_named("node_dfs_service_learner").expect("learner");
    let non_voting = NodeId::from_legacy_named("node_dfs_service_non_voting").expect("non voting");
    let removed = NodeId::from_legacy_named("node_dfs_service_removed").expect("removed");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs service members",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state
        .members
        .push(sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter.clone(),
            "voter",
            "pk-voter",
            now,
        ));
    let mut learner_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        learner.clone(),
        "learner",
        "pk-learner",
        now,
    );
    learner_member.role = sinomo_core::ClusterMemberRole::Learner;
    learner_member.state = sinomo_core::ClusterMemberState::Learner;
    state.members.push(learner_member);
    state
        .members
        .push(sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            non_voting.clone(),
            "shadow",
            "pk-shadow",
            now,
        ));
    let mut removed_member = sinomo_core::ClusterMemberRecord::local_voter(
        cluster_id.clone(),
        removed.clone(),
        "removed",
        "pk-removed",
        now,
    );
    removed_member.state = sinomo_core::ClusterMemberState::Removed;
    state.members.push(removed_member);

    let service_nodes = sinomo_dfs::dfs_service_members(&state)
        .into_iter()
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();

    assert_eq!(service_nodes, vec![local, voter, learner]);
}

#[test]
fn default_cluster_sifs_covers_every_dfs_service_node() {
    let state = test_cluster_state_with_members(
        "cluster_dfs_default_sifs",
        "node_dfs_default_local",
        &["node_dfs_default_remote_a", "node_dfs_default_remote_b"],
    );
    let file_system = sinomo_dfs::default_cluster_sifs(&state);

    let expected_node_ids = sinomo_dfs::stable_dfs_service_node_ids(&state);
    assert_eq!(file_system.owner_node_id, expected_node_ids[0]);
    for node_id in expected_node_ids {
        assert!(file_system.subtree_policies.iter().any(|policy| {
            policy.path_prefix == sifs_node_files_path(&node_id)
                && policy.policy == DfsObjectPolicy::LocalOwned
        }));
    }
    assert!(file_system.subtree_policies.iter().any(|policy| {
        policy.path_prefix == sinomo_dfs::SIFS_USERS_PATH
            && policy.policy == DfsObjectPolicy::ReplicatedEventual
    }));
    assert!(file_system.subtree_policies.iter().any(|policy| {
        policy.path_prefix == sinomo_dfs::SIFS_SERVICES_PATH
            && policy.policy == DfsObjectPolicy::ReplicatedEventual
    }));
}

#[test]
fn default_root_metadata_shard_is_deterministic_and_epoch_stable() {
    let root = temp_root("default_root_metadata_shard_is_deterministic");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = test_cluster_state_with_members(
        "cluster_dfs_default_root_shard",
        "node_dfs_default_root_local",
        &[
            "node_dfs_default_root_remote_a",
            "node_dfs_default_root_remote_b",
        ],
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let expected_primary = sinomo_dfs::stable_dfs_service_node_ids(&state)[0].clone();
    let old_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".to_string(),
        primary_node_id: expected_primary.clone(),
        replica_node_ids: Vec::new(),
        epoch: 9,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &state.local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &old_shard,
                None,
            )
            .expect("put shard op"),
        ),
    )
    .expect("write old shard");

    let runtime = daemon_test_runtime("default-dfs-bootstrap-migrates-shard-primary");
    let mut host = test_default_cluster_bootstrap_host(
        &data_dir,
        &cluster_paths,
        state.local_node_id.clone(),
        runtime.handle(),
    );
    let migrated = sinomo_dfs::default_cluster_metadata_shard_with_host(
        &mut host,
        &state,
        &file_system,
        "root",
        DfsMetadataShardAssignment::Root,
        0,
    )
    .expect("default shard");
    assert_eq!(migrated.primary_node_id, expected_primary);
    assert_eq!(migrated.epoch, 10);

    append_cluster_raft_command(
        &cluster_paths,
        &state.local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &migrated,
                None,
            )
            .expect("put migrated op"),
        ),
    )
    .expect("write migrated shard");
    let stable = sinomo_dfs::default_cluster_metadata_shard_with_host(
        &mut host,
        &state,
        &file_system,
        "root",
        DfsMetadataShardAssignment::Root,
        0,
    )
    .expect("stable shard");
    assert_eq!(stable, migrated);
}

#[test]
fn default_cluster_metadata_shards_cover_root_users_and_services() {
    let root = temp_root("default_cluster_metadata_shards_cover_sifs_paths");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = test_cluster_state_with_members(
        "cluster_dfs_default_path_shards",
        "node_dfs_default_path_shards_local",
        &[
            "node_dfs_default_path_shards_remote_a",
            "node_dfs_default_path_shards_remote_b",
        ],
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let runtime = daemon_test_runtime("default-dfs-bootstrap-default-path-shards");
    let mut host = test_default_cluster_bootstrap_host(
        &data_dir,
        &cluster_paths,
        state.local_node_id.clone(),
        runtime.handle(),
    );
    let shards =
        sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
            .expect("metadata shards");
    let by_id = shards
        .into_iter()
        .map(|shard| (shard.shard_id.clone(), shard))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(by_id.len(), 3);
    assert!(matches!(
        by_id.get("root").expect("root shard").assignment,
        DfsMetadataShardAssignment::Root
    ));
    assert!(matches!(
        &by_id.get("services").expect("services shard").assignment,
        DfsMetadataShardAssignment::PathPrefix { prefix }
            if prefix == sinomo_dfs::SIFS_SERVICES_PATH
    ));
    assert!(matches!(
        &by_id.get("users").expect("users shard").assignment,
        DfsMetadataShardAssignment::PathPrefix { prefix }
            if prefix == sinomo_dfs::SIFS_USERS_PATH
    ));
    assert_eq!(
        by_id
            .get("services")
            .expect("services shard")
            .primary_node_id,
        by_id.get("root").expect("root shard").primary_node_id,
        "default path-prefix shards start on the root primary until explicit primary transfer is used"
    );
}

#[test]
fn default_cluster_metadata_shards_replicate_to_all_service_nodes() {
    let root = temp_root("default_cluster_metadata_shards_replicate_to_all_services");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = test_cluster_state_with_members(
        "cluster_dfs_default_all_service_replicas",
        "node_dfs_default_all_replicas_local",
        &[
            "node_dfs_default_all_replicas_remote_a",
            "node_dfs_default_all_replicas_remote_b",
            "node_dfs_default_all_replicas_remote_c",
            "node_dfs_default_all_replicas_remote_d",
            "node_dfs_default_all_replicas_remote_e",
        ],
    );
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
    assert_eq!(service_nodes.len(), 6);
    let runtime = daemon_test_runtime("default-dfs-bootstrap-all-service-replicas");
    let mut host = test_default_cluster_bootstrap_host(
        &data_dir,
        &cluster_paths,
        state.local_node_id.clone(),
        runtime.handle(),
    );

    let shards =
        sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
            .expect("metadata shards");

    for shard in shards {
        let expected_replicas = service_nodes
            .iter()
            .filter(|node_id| *node_id != &shard.primary_node_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            shard.replica_node_ids, expected_replicas,
            "default shard {} should push metadata to every non-primary DFS service node",
            shard.shard_id
        );
    }
}

#[test]
fn default_cluster_metadata_shard_expands_replicas_before_primary_transfer() {
    let root = temp_root("default_cluster_metadata_shard_expands_before_transfer");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut state = test_cluster_state_with_members(
        "cluster_dfs_default_expand_replicas",
        "node_dfs_default_expand_local",
        &[
            "node_dfs_default_expand_remote_a",
            "node_dfs_default_expand_remote_b",
            "node_dfs_default_expand_remote_c",
            "node_dfs_default_expand_remote_d",
            "node_dfs_default_expand_remote_e",
        ],
    );
    let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
    assert_eq!(service_nodes.len(), 6);
    let bootstrap_primary = service_nodes[0].clone();
    let target_primary = service_nodes[1].clone();
    state.local_node_id = bootstrap_primary.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let old_services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: bootstrap_primary.clone(),
        replica_node_ids: service_nodes[2..4].to_vec(),
        epoch: 4,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
        },
        namespace_prepare_codec: Default::default(),
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &state.local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "services"),
                &old_services_shard,
                None,
            )
            .expect("put old services shard op"),
        ),
    )
    .expect("write old services shard");

    let runtime = daemon_test_runtime("default-dfs-bootstrap-expands-before-transfer");
    let mut host = test_default_cluster_bootstrap_host(
        &data_dir,
        &cluster_paths,
        state.local_node_id.clone(),
        runtime.handle(),
    );
    let expanded =
        sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
            .expect("expanded shards")
            .into_iter()
            .find(|shard| shard.shard_id == "services")
            .expect("services shard");
    let expected_replicas = service_nodes
        .iter()
        .filter(|node_id| *node_id != &bootstrap_primary)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        expanded.primary_node_id, bootstrap_primary,
        "primary should not move until the candidate has replayed the current journal"
    );
    assert_eq!(expanded.replica_node_ids, expected_replicas);
    assert_eq!(expanded.epoch, old_services_shard.epoch + 1);
    assert!(
        expanded
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == &target_primary),
        "the future primary must become a replica before primary transfer can become ready"
    );
}

#[test]
fn default_cluster_metadata_shard_transfers_after_candidate_replays_current_journal() {
    let root = temp_root("default_cluster_metadata_shard_transfer_after_replay");
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut state = test_cluster_state_with_members(
        "cluster_dfs_default_path_shard_transfer",
        "node_dfs_default_transfer_local",
        &[
            "node_dfs_default_transfer_remote_a",
            "node_dfs_default_transfer_remote_b",
        ],
    );
    let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
    let bootstrap_primary = service_nodes[0].clone();
    let target_primary = service_nodes[1].clone();
    state.local_node_id = bootstrap_primary.clone();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let services_shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "services".to_string(),
        primary_node_id: bootstrap_primary.clone(),
        replica_node_ids: service_nodes
            .iter()
            .filter(|node_id| *node_id != &bootstrap_primary)
            .cloned()
            .collect(),
        epoch: 3,
        assignment: DfsMetadataShardAssignment::PathPrefix {
            prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
        },
        // Isolate primary transfer from a separate codec-upgrade epoch.
        namespace_prepare_codec: sinomo_dfs::DfsMetadataNamespacePrepareCodec::PackedV3,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    append_cluster_raft_command(
        &cluster_paths,
        &state.local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&file_system.file_system_id, "services"),
                &services_shard,
                None,
            )
            .expect("put services shard op"),
        ),
    )
    .expect("write services shard");
    let shard_id = DfsMetadataShardId::new("services");
    let mut backend = sinomo_dfs::open_local_metadata_shard_backend(&data_dir, shard_id.clone())
        .expect("services backend");
    backend.put_file_system(&file_system).expect("filesystem");

    let runtime = daemon_test_runtime("default-dfs-bootstrap-waits-for-shard-transfer");
    let mut host = test_default_cluster_bootstrap_host(
        &data_dir,
        &cluster_paths,
        state.local_node_id.clone(),
        runtime.handle(),
    );
    let waiting =
        sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
            .expect("waiting shards")
            .into_iter()
            .find(|shard| shard.shard_id == "services")
            .expect("services shard");
    assert_eq!(
        waiting.primary_node_id, bootstrap_primary,
        "primary transfer should wait until the candidate has replayed the current journal"
    );
    assert_eq!(waiting.epoch, services_shard.epoch);

    backend
        .put_metadata_replication_cursor(&target_primary, &shard_id, 1)
        .expect("candidate replay cursor");

    let transferred =
        sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
            .expect("transferred shards")
            .into_iter()
            .find(|shard| shard.shard_id == "services")
            .expect("services shard");
    assert_eq!(transferred.primary_node_id, target_primary);
    assert_eq!(transferred.epoch, services_shard.epoch + 1);
    assert!(
        transferred
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == &state.local_node_id)
    );
}

#[test]
fn default_dfs_reactor_propagates_mandatory_recovery_failure() {
    let root = temp_root("default-dfs-bootstrap-recovery-required");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let cluster_id =
        ClusterId::from_legacy_named("cluster_dfs_recovery_required").expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_recovery_required").expect("node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs recovery required",
        local.clone(),
        "local",
        "pk-local",
        OffsetDateTime::now_utc(),
    );
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync cluster state");
    let runtime = daemon_test_runtime("default-dfs-bootstrap-recovery-required");
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");

    let error = runtime
        .block_on(reactor.reconcile_startup_until_idle_async(
            &paths,
            &DaemonRunOptions::default(),
            &cluster_id,
            &local,
            runtime.handle(),
        ))
        .expect_err("authoritative startup must propagate recovery failure");

    assert!(
        matches!(&error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
        "unexpected recovery error: {error}"
    );
}

#[test]
fn prior_process_recovery_uses_one_authority_read_on_two_mib_worker_stack() {
    let root = temp_root("default-dfs-bootstrap-recovers-prior-process-permit");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
        "cluster_dfs_bootstrap_recovers_prior_process_permit",
    )
    .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_bootstrap_recovery_local").expect("local node");
    let remote =
        NodeId::from_legacy_named("node_dfs_bootstrap_recovery_remote").expect("remote node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs bootstrap recovers prior process permit",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].capabilities =
        sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
    state
        .members
        .push(sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            remote,
            "remote",
            "pk-remote",
            now,
        ));
    assert!(!sinomo_core::network::cluster_is_settled_solo(&state));
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");

    let permit_key = seed_test_prior_process_placement_permit(
        &cluster_paths,
        &local,
        &cluster_id,
        "recoverable",
    );
    let file_system_id = sinomo_dfs::sifs_file_system_id();

    let data_owner_lock =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
        .expect("register main daemon data-owner lock");
    let runtime = daemon_test_runtime_with_thread_stack_size(
        "default-dfs-bootstrap-recovers-prior-process-permit",
        2 * 1024 * 1024,
    );
    assert!(
        inspect_dfs_placement_authority_permit(&paths, runtime.handle(), &file_system_id,)
            .expect("inspect seeded permit")
            .is_some(),
        "test must seed a decodable prior-process permit"
    );
    let _ = take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths);

    assert!(
        runtime
            .block_on(
                recover_prior_process_default_dfs_placement_authority_async_on_runtime(
                    &paths,
                    runtime.handle(),
                    true,
                ),
            )
            .expect("recover prior-process placement authority permit"),
        "the startup recovery must clear an exact prior-process permit"
    );

    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths),
        1,
        "startup recovery must issue one exact-key authority read without recursive validation"
    );

    assert!(
        sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&cluster_paths, &permit_key,)
            .expect("read recovered permit")
            .is_none(),
        "startup recovery must exact-CAS clear its local prior-process permit"
    );
}

#[test]
fn default_distributed_dfs_bootstrap_migrates_legacy_services_metadata_before_publish() {
    let root = temp_root("default_distributed_dfs_bootstrap_migrates_services");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_owner_lock =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
        .expect("register main daemon data-owner lock");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().expect("data dir");
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_bootstrap_migrate_services")
            .expect("cluster");
    let local = NodeId::from_legacy_named("node_dfs_bootstrap_migrate_services").expect("node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs bootstrap migrate services",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].capabilities =
        sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    let file_system = sinomo_dfs::default_cluster_sifs(&state);
    let mut legacy_object = distributed_sifs_bootstrap_objects(local.clone())
        .into_iter()
        .next()
        .expect("bootstrap object");
    legacy_object.object_id =
        DfsObjectId::from_legacy_named("obj_legacy_services_metadata").expect("object");
    legacy_object.path = "/services/legacy.txt".to_string();
    {
        let mut legacy_root = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("legacy root backend");
        legacy_root
            .put_file_system(&file_system)
            .expect("filesystem");
        legacy_root
            .put_object(&legacy_object)
            .expect("legacy services object");
    }

    let runtime = daemon_test_runtime("default-dfs-bootstrap-migrates-legacy");
    runtime
        .block_on(ensure_default_distributed_dfs_async_on_runtime(
            &paths,
            &DaemonRunOptions::default(),
            true,
            runtime.handle().clone(),
        ))
        .expect("bootstrap dfs");

    let shards = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
        .expect("current canonical shard authority")
        .range_prefix_limited(
            &format!(
                "{}/",
                sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
            ),
            sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )
        .expect("shard kv")
        .into_iter()
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()
        .expect("decode shards");
    assert_eq!(shards.len(), 3);

    let backend = sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
        .expect("open sharded backend")
        .expect("configured backend");
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &legacy_object.path)
            .expect("migrated services path"),
        Some(legacy_object.clone())
    );
    assert_eq!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("root shard")
        .get_object(&legacy_object.object_id)
        .expect("root object")
        .as_ref()
        .map(|object| object.kind.clone()),
        Some(DfsObjectKind::Tombstone)
    );
}

#[test]
fn default_dfs_reactor_defers_recovery_failure_on_non_voter() {
    let root = temp_root("default_distributed_dfs_bootstrap_non_primary");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_bootstrap_non_primary")
            .expect("cluster");
    let mut node_ids = [
        NodeId::from_legacy_named("node_dfs_bootstrap_non_primary_a").expect("node a"),
        NodeId::from_legacy_named("node_dfs_bootstrap_non_primary_b").expect("node b"),
    ];
    node_ids.sort();
    let primary = node_ids[0].clone();
    let local = node_ids[1].clone();
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "dfs bootstrap non primary",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].role = sinomo_core::ClusterMemberRole::Learner;
    state.members[0].state = sinomo_core::ClusterMemberState::Learner;
    state.members[0].capabilities =
        sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Learner);
    state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
    state
        .members
        .push(sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            primary,
            "primary",
            "pk-primary",
            now,
        ));
    let voter_capabilities =
        sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
    state.members[1].capabilities = voter_capabilities.clone();
    state.members[1].allowed_capabilities = voter_capabilities;
    assert_ne!(sinomo_dfs::default_cluster_sifs_owner(&state), local);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync cluster state");
    let permit_key =
        seed_test_prior_process_placement_permit(&cluster_paths, &local, &cluster_id, "deferred");

    let runtime = daemon_test_runtime_with_thread_stack_size(
        "default-dfs-bootstrap-non-primary",
        2 * 1024 * 1024,
    );
    let recovery_error =
        recover_prior_process_default_dfs_placement_authority(&paths, runtime.handle())
            .expect_err("test intentionally omits the main daemon data-owner lock");
    assert!(
        matches!(&recovery_error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
        "unexpected recovery error: {recovery_error}"
    );
    let mut reactor =
        DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
    let reactor_error = runtime
        .block_on(reactor.reconcile_pending_async(
            &paths,
            &DaemonRunOptions::default(),
            &cluster_id,
            &local,
            runtime.handle(),
            false,
        ))
        .expect_err("non-voter recovery failure remains pending for retry");
    assert!(
        matches!(&reactor_error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
        "unexpected reactor error: {reactor_error}"
    );
    assert_eq!(
        daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()),
        (0, 0),
        "a non-voter must not enter authoritative metadata-publish discovery"
    );
    assert!(
        !daemon_dfs_metadata_publish_discovery_is_active(&data_dir.root()),
        "a non-voter must not retain a metadata-publish discovery task"
    );
    assert!(
        sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&cluster_paths, &permit_key,)
            .expect("read deferred permit")
            .is_some(),
        "failed best-effort recovery must leave placement fenced"
    );

    let backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        data_dir.dfs_dir().join("metadata").join("root"),
    )
    .expect("metadata backend");
    assert!(
        backend
            .journal_since_limited(0, 1)
            .expect("metadata journal")
            .is_empty(),
        "non-primary bootstrap must not create root-shard journal entries"
    );
}

#[test]
fn daemon_dfs_smoke_commits_single_node_cluster_locally() {
    let root = temp_root("daemon_dfs_smoke_single_node");
    let data_root = root.join("data");
    let paths = DaemonPaths::with_root(data_root.join("daemon"));
    let data_owner_lock =
        Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
    register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
        .expect("register main daemon data-owner lock");
    let data_dir = SinomoDataDir::new(&data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let local = NodeId::from_legacy_named("node_dfs_smoke_single").expect("local node");
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_smoke_single")
        .expect("cluster");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "dfs smoke single",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    state.members[0].capabilities =
        sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("sync raft");
    let runtime = daemon_test_runtime("daemon-dfs-smoke-single-node");
    runtime
        .block_on(ensure_default_distributed_dfs_async_on_runtime(
            &paths,
            &DaemonRunOptions::default(),
            true,
            runtime.handle().clone(),
        ))
        .expect("bootstrap dfs");

    let report = runtime
        .block_on(run_daemon_dfs_smoke_async(&paths, runtime.handle()))
        .expect("dfs smoke");

    assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
    assert_eq!(report.committed_replica_node_ids, vec![local]);
    assert_eq!(report.attempts.len(), 1);
    assert!(report.attempts[0].committed);
    assert_eq!(
        report.attempts[0].replica_node_ids,
        report.requested_replica_node_ids
    );
    assert!(report.path.starts_with("/dfs-smoke/"));
    assert!(
        !report
            .metadata_replication
            .outcomes
            .iter()
            .any(|outcome| matches!(
                outcome,
                DfsMetadataJournalReplicationOutcome::MissingShard { .. }
            ))
    );
}

#[test]
fn dfs_service_advertisement_uses_node_id_for_remote_members() {
    let remote = NodeId::from_legacy_named("node_dfs_service_node_route_remote").expect("remote");
    let advertisement = sinomo_dfs::DfsNodeAdvertisement::content(remote.clone());

    assert_eq!(advertisement.node_id, remote);
    assert!(advertisement.accepts_content);
}

fn write_test_cluster_state(data_root: &std::path::Path, node_id: &NodeId) {
    write_test_cluster_state_with_members(data_root, node_id, Vec::new());
}

fn seed_test_prior_process_placement_permit(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    cluster_id: &ClusterId,
    case: &str,
) -> String {
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    let permit_key = format!("/dfs/filesystems/{file_system_id}/placement-authority-permit-v1");
    let permit = TestPlacementAuthorityPermitRecord {
        job_id: format!("prior-process-{case}-job"),
        holder_id: format!("prior-process-{case}-holder"),
        holder_cluster_id: Some(cluster_id.to_string()),
        holder_node_id: Some(local_node_id.clone()),
        holder_session_id: Some(format!("prior-process-{case}-session")),
    };
    append_cluster_raft_command(
        cluster_paths,
        local_node_id,
        sinomo_core::kv::KvCommand::Op(
            sinomo_core::kv::KvOp::put_serde(permit_key.clone(), &permit, None)
                .expect("encode prior-process permit"),
        ),
    )
    .expect("seed prior-process permit");
    permit_key
}

fn write_test_cluster_state_with_remote_endpoint(
    data_root: &std::path::Path,
    local_node_id: &NodeId,
    remote_node_id: &NodeId,
    address: &str,
) {
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
        .expect("cluster");
    let remote = test_remote_cluster_member_with_endpoint(&cluster_id, remote_node_id, address);
    write_test_cluster_state_with_members(data_root, local_node_id, vec![remote]);
}

fn test_remote_cluster_member_with_endpoint(
    cluster_id: &sinomo_core::node::ClusterId,
    remote_node_id: &NodeId,
    address: &str,
) -> sinomo_core::ClusterMemberRecord {
    let now = OffsetDateTime::now_utc();
    let mut remote = sinomo_core::ClusterMemberRecord::local_non_voting(
        cluster_id.clone(),
        remote_node_id.clone(),
        "daemon dfs protocol remote",
        format!("pk-{remote_node_id}"),
        now,
    );
    remote.capabilities = sinomo_core::node::main_daemon_capabilities(&remote.role);
    remote.allowed_capabilities = remote.capabilities.clone();
    remote
        .endpoints
        .push(sinomo_core::network::daemon_lan_endpoint(
            remote_node_id,
            address.to_string(),
            5,
        ));
    remote
}

fn write_test_cluster_state_with_members(
    data_root: &std::path::Path,
    node_id: &NodeId,
    members: Vec<sinomo_core::ClusterMemberRecord>,
) {
    let data_dir = SinomoDataDir::new(data_root);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now = OffsetDateTime::now_utc();
    let cluster_id = sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
        .expect("cluster");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id,
        "daemon dfs protocol",
        node_id.clone(),
        "daemon dfs protocol node",
        format!("pk-{node_id}"),
        now,
    );
    state.members.extend(members);
    authorize_test_main_daemon_members(&mut state);
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
    sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
        .expect("seed canonical cluster authority");
}

fn authorize_test_main_daemon_members(state: &mut sinomo_core::ClusterBootstrapState) {
    for member in &mut state.members {
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
    }
}

fn test_cluster_state_with_members(
    cluster_name: &str,
    local_name: &str,
    remote_names: &[&str],
) -> sinomo_core::ClusterBootstrapState {
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        sinomo_core::node::ClusterId::from_legacy_named(cluster_name).expect("cluster id");
    let local = NodeId::from_legacy_named(local_name).expect("local node");
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "daemon dfs test cluster",
        local,
        "local",
        "pk-local",
        now,
    );
    authorize_test_main_daemon_members(&mut state);
    for remote_name in remote_names {
        let node_id = NodeId::from_legacy_named(*remote_name).expect("remote node");
        let mut member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node_id,
            *remote_name,
            format!("pk-{remote_name}"),
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut member,
            sinomo_core::ClusterMemberRole::Learner,
            now,
        );
        state.members.push(member);
    }
    state
}

fn test_endpoint(node_id: &NodeId, address: &str) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
            "sinomo.test.endpoint",
            format!("{}\0{address}", node_id.as_str()),
        ),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: address.to_string(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 10,
        last_observed_latency_ms: None,
    }
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-daemon-dfs-{name}"))
}
