use super::*;
use crate::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsControlPlaneStore, DfsMessageTransport, DfsPosixSyncKind,
};
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
    DfsMetadataRejectionCode, DfsMetadataReplicaLagStatus, DfsNamespaceCommand,
    DfsNamespaceJobStatus, DfsNamespaceOperation, DfsNetworkMessage, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsReplicationPolicy, DfsRoutedMessage, DfsStageBlobCommand, DfsVersion,
    DfsWriteOptions, DistributedRepairJob,
    run_metadata_publish_blocking_execution as run_daemon_dfs_metadata_publish_blocking_execution,
    run_metadata_publish_lane_planning_with_admission as run_daemon_dfs_metadata_publish_lane_planning_with_admission,
};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Mutex};

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

fn mounted_mutation_test_repair_record(label: &str) -> DfsRepairAttemptRecord {
    DfsRepairAttemptRecord {
        transfer_delivery_state: None,
        completed_transfer_request_id: None,
        transfer_kind: None,
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
            semantic_changes: true,
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
    holder_cluster_id: String,
    holder_node_id: NodeId,
    holder_session_id: String,
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
    let local = NodeId::generate();
    let mut native =
        crate::test_support::DfsTestRuntime::new("native-placement-owner-lifetime", local.clone())
            .unwrap();
    let data_dir = native.data_dir(&local).clone();
    let paths = DaemonPaths::new(&data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = native.runtime_handle();
    let unowned = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, &runtime);
    assert!(
        crate::DfsControlPlaneKvClient::placement_authority_process_identity(&unowned).is_none()
    );
    native.start_nodes().unwrap();
    let client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, &runtime);
    let identity =
        crate::DfsControlPlaneKvClient::placement_authority_process_identity(&client).unwrap();
    assert!(
        crate::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &client, &local, &identity
        )
    );
    assert!(
        !crate::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &client,
            &NodeId::generate(),
            &identity
        )
    );
    let forged = crate::DfsPlacementAuthorityProcessIdentity {
        cluster_id: identity.cluster_id.clone(),
        process_session_id: "forged-current-process".into(),
    };
    assert!(
        !crate::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
            &client, &local, &forged
        )
    );
    native.stop_node(&local).unwrap();
    assert!(
        crate::DfsControlPlaneKvClient::placement_authority_process_identity(&client).is_none(),
        "a weak client cannot pin a stopped native Main owner"
    );
    drop(
        DaemonDataOwnerLock::acquire(&paths)
            .expect("normal component shutdown released the actual owner"),
    );
    native.reopen_node(local.clone()).unwrap();
    let owned_service = native.client(&local).unwrap();
    native.stop_node(&local).unwrap();
    assert!(
        DaemonDataOwnerLock::acquire(&paths).is_err(),
        "the ordinary service provider retains its actual data custody"
    );
    drop(owned_service);
    drop(
        DaemonDataOwnerLock::acquire(&paths)
            .expect("dropping the actual service releases its passive owner custody"),
    );
    native.close().unwrap();
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

struct NativeMaterializationFixture {
    native: crate::test_support::DfsTestRuntime,
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    data_owner: Arc<DaemonDataOwnerLock>,
    shard: ResidentLocalMetadataShardHandle,
    scheduler: Arc<DaemonDfsMutationScheduler>,
    object_id: crate::DfsObjectId,
    config: DfsMetadataShardConfig,
    pause: DfsTestMaterializationPause,
}
impl NativeMaterializationFixture {
    fn wait_materialized(&self) {
        self.runtime.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if self
                    .shard
                    .with_backend(|backend| backend.get_object(&self.object_id))
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "actual durable suffix did not materialize"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }
    fn close(mut self) {
        self.pause.release();
        self.wait_materialized();
        drop(self.pause);
        drop(self.shard);
        drop(self.data_owner);
        self.native.close().unwrap();
    }
}
fn replica_materialization_fixture(name: &str) -> NativeMaterializationFixture {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let config = native.root_shard().unwrap();
    let replica = config.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let pause = native
        .pause_replica_materialization(&replica, &shard_id)
        .unwrap();
    let mut source = native.client(&config.primary_node_id).unwrap();
    let object_id = source
        .posix_mkdir("/native-materialization-marker")
        .unwrap()
        .object_id;
    drop(source);
    let capture = native
        .wait_paused_materialization(&pause, Duration::from_secs(2))
        .unwrap();
    assert!(
        capture.unavailable.is_none(),
        "actual pause must retain a factual native cut: {capture:?}"
    );
    assert!(
        capture.durable_sequence.unwrap() > capture.materialized_sequence.unwrap(),
        "real durable ingress must precede replay"
    );
    let runtime = native.runtime_handle();
    let data_dir = native.data_dir(&replica).clone();
    let paths = DaemonPaths::new(&data_dir);
    let data_owner = require_daemon_dfs_data_owner_lock(&paths).unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .unwrap()
        .unwrap();
    let shard = daemon_dfs_message_cache_exact_admission_handle(
        &data_dir,
        &cluster_paths,
        &peer_snapshot,
        &DaemonDfsMutationLaneKey {
            file_system_id: config.file_system_id.clone(),
            shard_id,
        },
        &runtime,
    )
    .unwrap()
    .unwrap();
    assert!(
        shard
            .with_backend(|backend| backend.get_object(&object_id))
            .unwrap()
            .is_none()
    );
    NativeMaterializationFixture {
        native,
        runtime,
        data_dir,
        data_owner,
        shard,
        scheduler,
        object_id,
        config,
        pause,
    }
}

#[test]
fn replica_materialization_wait_releases_generation_before_foreground_admission() {
    let fixture = replica_materialization_fixture("native-materialization-foreground-queue");
    fixture.runtime.block_on(async {
        let foreground = admit_daemon_dfs_foreground_message(
            &fixture.scheduler,
            &DfsMessageExecutionScope::GlobalExclusive,
        )
        .await
        .unwrap();
        assert!(
            fixture.shard.try_checkout().unwrap().is_some(),
            "parked native replay retains no resident lease"
        );
        fixture.pause.release();
        let mut cancelled = Box::pin(admit_daemon_dfs_foreground_message(
            &fixture.scheduler,
            &DfsMessageExecutionScope::GlobalExclusive,
        ));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(
                cancelled.as_mut(),
                cx
            )))
            .await
            .is_pending()
        );
        drop(cancelled);
        assert!(
            fixture
                .shard
                .with_backend(|backend| backend.get_object(&fixture.object_id))
                .unwrap()
                .is_none()
        );
        drop(foreground);
        drop(
            tokio::time::timeout(
                Duration::from_secs(1),
                admit_daemon_dfs_foreground_message(
                    &fixture.scheduler,
                    &DfsMessageExecutionScope::GlobalExclusive,
                ),
            )
            .await
            .expect("cancelled waiter must leave the actual queue")
            .unwrap(),
        );
    });
    fixture.close();
}

#[test]
fn replica_materialization_resumes_after_foreground_without_another_append() {
    let fixture = replica_materialization_fixture("native-materialization-foreground-resume");
    fixture.runtime.block_on(async {
        let foreground = admit_daemon_dfs_foreground_message(
            &fixture.scheduler,
            &DfsMessageExecutionScope::GlobalExclusive,
        )
        .await
        .unwrap();
        fixture.pause.release();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            fixture
                .shard
                .with_backend(|backend| backend.get_object(&fixture.object_id))
                .unwrap()
                .is_none(),
            "actual accepted suffix must stay queued behind foreground admission"
        );
        assert!(fixture.shard.try_checkout().unwrap().is_some());
        drop(foreground);
    });
    fixture.wait_materialized();
    assert!(
        fixture
            .shard
            .with_backend(|backend| backend.get_object(&fixture.object_id))
            .unwrap()
            .is_some()
    );
    fixture.close();
}

#[test]
#[allow(clippy::async_yields_async)] // Keep the task handle to exercise cancellation/generation races.
fn replica_materialization_cancellation_retains_blocking_replay_capabilities() {
    let fixture = replica_materialization_fixture("native-materialization-cancel-blocking");
    let (started, release) = install_daemon_dfs_message_committed_edge_test_hook(&format!(
        "replica-materialization:{}",
        fixture.data_dir.root().display()
    ));
    fixture.pause.release();
    started
        .recv_timeout(Duration::from_secs(2))
        .expect("real replay holds its blocking capabilities");
    fixture.runtime.block_on(async {
        fixture
            .pause
            .abort_worker(Duration::from_secs(2))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                fixture
                    .scheduler
                    .admit(&DfsMessageExecutionScope::GlobalExclusive)
            )
            .await
            .is_err(),
            "cancelling the real scheduler future cannot release an active blocking replay"
        );
        assert!(fixture.shard.try_checkout().unwrap().is_none());
        assert!(
            fixture
                .pause
                .blocking_replay_holds_owner(&fixture.data_owner),
            "the actual blocking replay retains the Main owner"
        );
        release.send(()).unwrap();
        drop(
            tokio::time::timeout(
                Duration::from_secs(2),
                fixture
                    .scheduler
                    .admit(&DfsMessageExecutionScope::GlobalExclusive),
            )
            .await
            .unwrap()
            .unwrap(),
        );
        assert!(
            !fixture
                .pause
                .blocking_replay_holds_owner(&fixture.data_owner)
        );
    });
    fixture.close();
}

#[test]
#[allow(clippy::async_yields_async)] // Inspect the worker across a generation change before awaiting it.
fn replica_materialization_reacquires_generation_changed_while_queued() {
    let mut fixture = replica_materialization_fixture("native-materialization-generation-rollover");
    let old_scope = fixture.shard.clone();
    let mut desired = fixture.config.clone();
    desired.primary_node_id = fixture.config.replica_node_ids[0].clone();
    desired.replica_node_ids = vec![fixture.config.primary_node_id.clone()];
    desired.epoch += 1;
    let activated = fixture
        .native
        .reassign_metadata_shard(
            &fixture.config.file_system_id,
            &fixture.config.shard_id,
            desired.primary_node_id,
            desired.replica_node_ids,
        )
        .expect("real joint activation while the worker is parked");
    assert!(activated.epoch > fixture.config.epoch);
    fixture.pause.release();
    fixture.wait_materialized();
    assert!(
        old_scope.try_checkout().unwrap().is_some(),
        "replay must release its previous canonical generation"
    );
    let paths = ClusterPaths::new(&fixture.data_dir);
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let current = daemon_dfs_message_cache_exact_admission_handle(
        &fixture.data_dir,
        &paths,
        &peer,
        &DaemonDfsMutationLaneKey {
            file_system_id: activated.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(activated.shard_id),
        },
        &fixture.runtime,
    )
    .unwrap()
    .unwrap();
    assert!(
        current
            .with_backend(|backend| backend.get_object(&fixture.object_id))
            .unwrap()
            .is_some()
    );
    fixture.shard = current;
    fixture.close();
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
fn background_metadata_publication_waits_but_durability_catchup_preempts() {
    let nodes =
        crate::test_support::DfsTestRuntime::ordered_node_ids("native-publication-priority", 2);
    let mut native =
        crate::test_support::DfsTestRuntime::new("native-publication-priority", nodes[0].clone())
            .unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let shard = native.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let file_system = crate::distributed_sifs(primary.clone());
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let source = native.client(&primary).unwrap();
    let backend = source.metadata_backend().unwrap();
    let mut subscription = backend.subscribe(0);
    let batch = backend.poll_subscription(&mut subscription).unwrap();
    let frames =
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
            .unwrap();
    let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &replica,
        &file_system.file_system_id,
        shard.epoch,
        frames,
        true,
    );
    assert!(dfs_metadata_replication_can_wait_for_maintenance(
        &command, &primary, &replica
    ));

    let mut missing_current_field = serde_json::to_value(&command).unwrap();
    missing_current_field["durable_replication"]
        .as_object_mut()
        .unwrap()
        .remove("background_publication");
    assert!(
        serde_json::from_value::<DfsMetadataJournalBatchCommand>(missing_current_field).is_err(),
        "current requests require an explicit scheduling field"
    );
    let mut foreground_command = command.clone();
    foreground_command
        .durable_replication
        .as_mut()
        .unwrap()
        .background_publication = false;
    assert!(!dfs_metadata_replication_can_wait_for_maintenance(
        &foreground_command,
        &primary,
        &replica
    ));
    let mut sync = command.clone();
    sync.request_id = "dfs_sync_catchup_barrier_request".into();
    assert!(!dfs_metadata_replication_can_wait_for_maintenance(
        &sync, &primary, &replica
    ));
    let mut mixed = command.clone();
    mixed.verify_prefix_only = true;
    assert!(!dfs_metadata_replication_can_wait_for_maintenance(
        &mixed, &primary, &replica
    ));
    assert!(!dfs_metadata_replication_can_wait_for_maintenance(
        &command, &replica, &primary
    ));

    let scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&replica)).unwrap();
    let runtime = native.runtime_handle();
    runtime.block_on(async {
        let interest = scheduler.begin_maintenance_interest();
        let maintenance = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let scope = DfsMessageExecutionScope::ExactShard {
            file_system_id: file_system.file_system_id.clone(),
            shard_id,
        };
        {
            let background = admit_daemon_dfs_message(
                &scheduler,
                &scope,
                !dfs_metadata_replication_can_wait_for_maintenance(&command, &primary, &replica),
            );
            tokio::pin!(background);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut background)
                    .await
                    .is_err()
            );
            assert!(
                !interest.is_requested(),
                "background data cannot cancel its maintenance author"
            );
            drop(maintenance);
            let admitted = background.await.unwrap();
            assert!(
                admitted.is_exact(),
                "background still takes the normal exact-shard gate"
            );
        }
        let maintenance = scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let foreground = admit_daemon_dfs_message(
            &scheduler,
            &scope,
            !dfs_metadata_replication_can_wait_for_maintenance(
                &foreground_command,
                &primary,
                &replica,
            ),
        );
        tokio::pin!(foreground);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut foreground)
                .await
                .is_err()
        );
        assert!(
            interest.is_requested(),
            "foreground catch-up still breaks admission dependencies"
        );
        drop(maintenance);
        assert!(foreground.await.unwrap().is_exact());
    });
    drop(source);
    native.close().unwrap();
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
            batch: sinomo_dfs::DfsMetadataJournalFrameBatch {
                shard_id,
                journal_frames: Vec::new(),
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
fn placement_and_promotions_wait_for_repairs_but_user_requests_still_preempt() {
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let runtime = daemon_test_runtime("placement-background-priority");
    let node = NodeId::from_legacy_named("placement_priority_node").unwrap();
    let mut message =
        sinomo_dfs::DfsNetworkMessage::NamespaceRequest(sinomo_dfs::DfsNamespaceCommand {
            request_id: "placement-poll".into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            requesting_node_id: node.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: sinomo_dfs::DfsNamespaceOperation::ReconcilePlacement {
                job_id: "job".into(),
                path: "/file".into(),
                object_id: sinomo_core::Id::generate(),
                expected_version: sinomo_core::Id::generate(),
                object_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                replication_policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                desired_replica_node_ids: vec![node],
            },
        });
    let promotion =
        sinomo_dfs::DfsNetworkMessage::CommitVersion(sinomo_dfs::DfsCommitVersionCommand {
            request_id: "detached-promotion".into(),
            intent_id: "committed-write".into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            object_id: sinomo_core::Id::generate(),
            expected_base_version: None,
            version: sinomo_core::Id::generate(),
            content_hash: "blake3:committed".into(),
            object_metadata: None,
            authority_shard: None,
        });
    runtime.block_on(async {
        let interest = scheduler.begin_maintenance_interest();
        let scope = DfsMessageExecutionScope::GlobalExclusive;
        for background_message in [&message, &promotion] {
            let held = scheduler.admit(&scope).await.unwrap();
            let priority =
                super::message_host::daemon_dfs_message_preempts_maintenance(background_message);
            assert!(!priority);
            let mut background = Box::pin(admit_daemon_dfs_message(&scheduler, &scope, priority));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), background.as_mut())
                    .await
                    .is_err()
            );
            assert!(
                !interest.is_requested(),
                "post-commit work must not cancel the repair holding admission"
            );
            drop(held);
            drop(
                tokio::time::timeout(Duration::from_secs(1), background)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let sinomo_dfs::DfsNetworkMessage::NamespaceRequest(command) = &mut message else {
            unreachable!()
        };
        command.operation = sinomo_dfs::DfsNamespaceOperation::MkdirAll {
            path: "/foreground".into(),
        };
        let priority = super::message_host::daemon_dfs_message_preempts_maintenance(&message);
        assert!(priority);
        let held = scheduler.admit(&scope).await.unwrap();
        let mut foreground = Box::pin(admit_daemon_dfs_message(&scheduler, &scope, priority));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), foreground.as_mut())
                .await
                .is_err()
        );
        assert!(
            interest.is_requested(),
            "user work must retain foreground preemption"
        );
        drop(held);
        drop(
            tokio::time::timeout(Duration::from_secs(1), foreground)
                .await
                .unwrap()
                .unwrap(),
        );
    });
}

#[test]
fn background_repair_admission_waits_while_explicit_foreground_repairs_preempt() {
    use std::future::Future;

    let foreground = sinomo_dfs::DfsRepairReplicaCommand {
        background_repair: false,
        request_id: "repair-admission-priority".into(),
        intent_id: "repair-admission-intent".into(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        object_id: sinomo_core::Id::from_legacy_named("repair-admission-object").unwrap(),
        expected_base_version: None,
        version: sinomo_core::Id::from_legacy_named("repair-admission-version").unwrap(),
        content_hash: "blake3:repair-admission".into(),
        bytes: Vec::new(),
        object_metadata: None,
        authority_shard: None,
    };
    let mut background = foreground.clone();
    background.background_repair = true;
    let mut missing_hint = serde_json::to_value(&background).unwrap();
    missing_hint
        .as_object_mut()
        .unwrap()
        .remove("background_repair");
    assert!(serde_json::from_value::<sinomo_dfs::DfsRepairReplicaCommand>(missing_hint).is_err());
    let source = NodeId::from_legacy_named("repair-admission-source").unwrap();
    let target = NodeId::from_legacy_named("repair-admission-target").unwrap();
    let runtime = daemon_test_runtime("repair-admission-priority");
    runtime.block_on(async {
        for (label, repair, preempts) in [
            ("background", background, false),
            ("foreground", foreground, true),
        ] {
            let command = sinomo_dfs::DfsMessageCommand {
                request_id: format!("repair-admission-{label}"),
                routed_message: DfsRoutedMessage {
                    source_node_id: source.clone(),
                    target_node_id: target.clone(),
                    frame_id: 1,
                    message_id: 1,
                    message: DfsNetworkMessage::RepairReplica(repair),
                },
            };
            let scope = sinomo_dfs::dfs_message_execution_scope(&command);
            assert_eq!(
                scope,
                DfsMessageExecutionScope::GlobalExclusive,
                "a scheduling hint must not narrow repair mutation admission"
            );
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let interest = scheduler.begin_maintenance_interest();
            let held = scheduler.admit(&scope).await.unwrap();
            let priority = super::message_host::daemon_dfs_message_preempts_maintenance(
                &command.routed_message.message,
            );
            assert_eq!(priority, preempts, "incorrect priority for {label} repair");
            let mut admission = Box::pin(admit_daemon_dfs_message(&scheduler, &scope, priority));
            let queued = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(admission.as_mut().poll(cx).is_pending())
            })
            .await;
            assert!(queued, "{label} repair bypassed global mutation admission");
            assert_eq!(
                interest.is_requested(),
                preempts,
                "incorrect maintenance preemption for {label} repair"
            );
            drop(held);
            drop(
                tokio::time::timeout(Duration::from_secs(1), admission)
                    .await
                    .expect("repair admission advances after maintenance releases ownership")
                    .unwrap(),
            );
        }
    });
}

struct OwnedBackgroundRepairTestReader {
    bytes: std::io::Cursor<Vec<u8>>,
    reads: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl std::io::Read for OwnedBackgroundRepairTestReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        std::io::Read::read(&mut self.bytes, buffer)
    }
}

impl Drop for OwnedBackgroundRepairTestReader {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

struct OwnedBackgroundRepairTestPeer<F>(F);

impl<F> sinomo_dfs::DfsPeerTransport for OwnedBackgroundRepairTestPeer<F>
where
    F: Fn(
            &NodeId,
            sinomo_core::network::ObjectRpcCall,
            sinomo_dfs::DfsObjectRpcSendOptions,
        )
            -> Result<sinomo_core::network::ObjectRpcResult, sinomo_dfs::DfsPeerTransportError>
        + Send
        + Sync,
{
    fn call_object_rpc(
        &self,
        target: &NodeId,
        call: sinomo_core::network::ObjectRpcCall,
        options: sinomo_dfs::DfsObjectRpcSendOptions,
    ) -> Result<sinomo_core::network::ObjectRpcResult, sinomo_dfs::DfsPeerTransportError> {
        (self.0)(target, call, options)
    }
}

/// These are payload-owner tests. Their positive command is observed from an
/// ordinary native W1/N publication and checkpointed background repair, then
/// replayed through the same registered native ingress. Normal route policy
/// may park the original owner; observing it supplies no reachability fact.
struct NativeOwnedBackgroundRepairFixture {
    native: crate::test_runtime::DfsTestRuntime,
    source: NodeId,
    targets: Vec<NodeId>,
    peer: Arc<dyn sinomo_dfs::DfsPeerTransport>,
    commands: BTreeMap<NodeId, DfsRoutedMessage>,
    descriptors: BTreeMap<NodeId, sinomo_dfs::DfsRpcAttachmentDescriptor>,
    payload_bytes: Vec<u8>,
}

impl NativeOwnedBackgroundRepairFixture {
    fn new(name: &str, target_count: usize) -> Self {
        use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule};
        let (native, nodes) = DfsTestRuntime::start_cluster(name, target_count + 1).unwrap();
        let source = native.root_shard().unwrap().primary_node_id;
        let targets = nodes
            .iter()
            .filter(|node| **node != source)
            .cloned()
            .collect::<Vec<_>>();
        let path = "/owned-background-repair.bin";
        let placement = native
            .apply_placement_policy(
                &source,
                path,
                crate::DfsPlacementScope::SelfOnly,
                crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, nodes.len())
                    .unwrap(),
            )
            .unwrap();
        let control = native.shared_control_plane().unwrap();
        let mut remaining_maintenance_turns = 8;
        loop {
            let current = control
                .with_client(|kv| {
                    DfsControlPlaneStore::new(kv.as_mut())
                        .placement_reconciliation(&placement.file_system_id, &placement.job_id)
                })
                .unwrap()
                .expect("actual placement Apply remains readable");
            if current.state == crate::DfsPlacementReconciliationState::Completed {
                assert_eq!(current.scanned_object_count, 0);
                break;
            }
            assert!(
                remaining_maintenance_turns > 0,
                "ordinary empty placement Apply must settle within the original setup budget"
            );
            remaining_maintenance_turns -= 1;
            let tick = match native
                .maintenance_tick(&source, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            {
                Ok(tick) => tick,
                Err(error)
                    if error
                        == "metadata backend error: bounded DFS repair queue status unavailable while the resident queue is being updated" =>
                {
                    // This read-only diagnostic can race the real resident
                    // update. Consume this original setup turn and retain all
                    // actual captured producer facts; never infer empty/Ready.
                    eprintln!(
                        "native repair queue observation unavailable during its actual update"
                    );
                    continue;
                }
                Err(error) => panic!("ordinary native maintenance failed: {error}"),
            };
            assert!(
                tick.outcome.is_ok(),
                "ordinary empty placement owner: {:?}",
                tick.outcome
            );
            assert_eq!(tick.omitted_kv_event_count, 0);
        }
        let captured = native
            .capture_checkpointed_background_repairs(&source, 32)
            .unwrap();
        for target in &targets {
            // The real W1 producer must retain a missing N replica. Only Stage
            // delivery is denied; ordinary native background repair is allowed.
            native.install_fault(FaultRule {
                source: Some(source.clone()),
                target: Some(target.clone()),
                route: Some(crate::routes::DFS_DATA_STAGE_BLOB_ROUTE.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::TransientRouteFailureBeforeDispatch,
            });
        }
        let mut writer = native.client(&source).unwrap();
        let stat = writer
            .posix_write_file(
                path,
                b"owned background repair payload",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, nodes.len()).unwrap(),
                    replica_candidates: nodes,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let payload_bytes = writer.posix_read_file(path).unwrap();
        let current = writer.posix_stat(path).unwrap();
        assert_eq!(current.object_id, stat.object_id);
        assert_eq!(current.version, stat.version);
        drop(writer);
        native.clear_faults();
        for _ in 0..remaining_maintenance_turns {
            let observed_targets = captured
                .records()
                .into_iter()
                .filter_map(|record| {
                    let DfsNetworkMessage::RepairReplica(repair) = &record.message.message else {
                        return None;
                    };
                    (repair.background_repair
                        && repair
                            .object_metadata
                            .as_ref()
                            .is_some_and(|object| object.path == path))
                    .then_some(record.message.target_node_id)
                })
                .collect::<BTreeSet<_>>();
            if observed_targets.len() == targets.len() {
                break;
            }
            let tick = match native
                .maintenance_tick(&source, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            {
                Ok(tick) => tick,
                Err(error)
                    if error
                        == "metadata backend error: bounded DFS repair queue status unavailable while the resident queue is being updated" =>
                {
                    // This read-only diagnostic can race the real resident
                    // update. Consume this original setup turn and retain all
                    // actual captured producer facts; never infer empty/Ready.
                    eprintln!(
                        "native repair queue observation unavailable during its actual update"
                    );
                    continue;
                }
                Err(error) => panic!("ordinary native maintenance failed: {error}"),
            };
            assert!(
                tick.outcome.is_ok(),
                "ordinary native repair owner: {:?}",
                tick.outcome
            );
            assert_eq!(tick.omitted_kv_event_count, 0);
        }
        assert_eq!(captured.omitted_count(), 0);
        let mut commands = BTreeMap::new();
        let mut descriptors = BTreeMap::new();
        for record in captured.records() {
            let DfsNetworkMessage::RepairReplica(repair) = &record.message.message else {
                unreachable!();
            };
            if !repair
                .object_metadata
                .as_ref()
                .is_some_and(|object| object.path == path)
            {
                continue;
            }
            assert!(repair.background_repair);
            assert_eq!(record.message.source_node_id, source);
            assert!(targets.contains(&record.message.target_node_id));
            descriptors
                .entry(record.message.target_node_id.clone())
                .or_insert(record.descriptor);
            commands
                .entry(record.message.target_node_id.clone())
                .or_insert(record.message);
        }
        drop(captured);
        assert_eq!(
            commands.len(),
            targets.len(),
            "capture each actual background repair command"
        );
        for (target, message) in &commands {
            let DfsNetworkMessage::RepairReplica(command) = &message.message else {
                unreachable!();
            };
            assert_eq!(command.object_id, stat.object_id);
            assert_eq!(command.version, stat.version);
            assert!(command.authority_shard.is_some());
            assert!(
                command.bytes.is_empty(),
                "actual native attachment header stays unchanged"
            );
            assert_eq!(
                sinomo_dfs::content_hash(&payload_bytes),
                command.content_hash
            );
            assert_eq!(
                descriptors[target].total_size_bytes,
                payload_bytes.len() as u64
            );
            assert_eq!(
                descriptors[target].content_hash.as_deref(),
                Some(command.content_hash.as_str())
            );
        }
        let peer = native.peer_transports(&source).unwrap().0;
        Self {
            native,
            source,
            targets,
            peer,
            commands,
            descriptors,
            payload_bytes,
        }
    }

    fn request_id(&self, target: &NodeId) -> String {
        let DfsNetworkMessage::RepairReplica(command) = &self.commands[target].message else {
            unreachable!();
        };
        command.request_id.clone()
    }

    fn intent_id(&self, target: &NodeId) -> String {
        let DfsNetworkMessage::RepairReplica(command) = &self.commands[target].message else {
            unreachable!();
        };
        command.intent_id.clone()
    }
}

fn owned_background_repair_test_call(
    native: &NativeOwnedBackgroundRepairFixture,
    target: &NodeId,
    reads: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
) -> sinomo_dfs::DfsRoutedSend {
    let message = native.commands[target].clone();
    let DfsNetworkMessage::RepairReplica(command) = &message.message else {
        unreachable!();
    };
    assert!(command.bytes.is_empty());
    let bytes = native.payload_bytes.clone();
    assert_eq!(sinomo_dfs::content_hash(&bytes), command.content_hash);
    let descriptor = native.descriptors[target].clone();
    let total_size_bytes = descriptor.total_size_bytes;
    assert_eq!(total_size_bytes, bytes.len() as u64);
    sinomo_dfs::DfsRoutedSend::new(
        target.clone(),
        sinomo_dfs::DfsRoutedCall::with_attachment(
            message,
            sinomo_dfs::DfsOutboundAttachment {
                descriptor,
                source: sinomo_core::network::ObjectRpcBodySource::from_reader(
                    OwnedBackgroundRepairTestReader {
                        bytes: std::io::Cursor::new(bytes),
                        reads,
                        dropped,
                    },
                    Some(total_size_bytes),
                ),
            },
        ),
    )
}

#[test]
fn background_repair_dispatch_releases_source_for_reciprocal_routine_admission() {
    use sinomo_dfs::DfsMessageTransport;
    use std::future::Future;

    let root = temp_root("background-repair-reciprocal-admission");
    let native = NativeOwnedBackgroundRepairFixture::new("background-repair-reciprocal-native", 1);
    let source = native.source.clone();
    let target = native.targets[0].clone();
    let expected_request_id = native.request_id(&target);
    let source_scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let target_scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let source_interest = source_scheduler.begin_maintenance_interest();
    let target_interest = target_scheduler.begin_maintenance_interest();
    let runtime = daemon_test_runtime("background-repair-reciprocal-admission");
    let scope = DfsMessageExecutionScope::GlobalExclusive;
    let source_owner = runtime.block_on(source_scheduler.admit(&scope)).unwrap();
    let target_owner = runtime.block_on(target_scheduler.admit(&scope)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let replies = Arc::new(AtomicUsize::new(0));
    let native_peer = Arc::clone(&native.peer);
    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    let queued_tx = Mutex::new(Some(queued_tx));
    let peer_source_scheduler = Arc::clone(&source_scheduler);
    let peer_target_scheduler = Arc::clone(&target_scheduler);
    let peer_calls = Arc::clone(&calls);
    let peer_runtime = runtime.handle().clone();
    let peer_target = target.clone();
    let peer: Arc<dyn sinomo_dfs::DfsPeerTransport> = Arc::new(OwnedBackgroundRepairTestPeer(
        move |node: &NodeId,
              call: sinomo_core::network::ObjectRpcCall,
              options: sinomo_dfs::DfsObjectRpcSendOptions| {
            assert_eq!(node, &peer_target);
            peer_calls.fetch_add(1, Ordering::SeqCst);
            drop(
                peer_source_scheduler
                    .try_admit_global()
                    .expect("background peer I/O must not retain the source mutation owner"),
            );
            let admitted = peer_runtime.block_on(async {
                let mut pending = Box::pin(admit_daemon_dfs_message(
                    &peer_target_scheduler,
                    &DfsMessageExecutionScope::GlobalExclusive,
                    false,
                ));
                assert!(
                    std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
                    })
                    .await
                );
                queued_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), pending)
                    .await
                    .expect("background repair must advance after target Routine completes")
                    .unwrap()
            });
            let result = native_peer.call_object_rpc(node, call, options);
            drop(admitted);
            result
        },
    ));
    let transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
        source.clone(),
        [target.clone()],
        peer,
        Duration::from_secs(1),
    );
    let mut service = NetworkedDfsService::new(
        transport,
        sinomo_dfs::LocalContentStore::new(root.join("source-content")).unwrap(),
    );
    service
        .transport
        .prepare_background_replica_dispatch(
            vec![owned_background_repair_test_call(
                &native,
                &target,
                Arc::clone(&reads),
                Arc::clone(&dropped),
            )],
            &[None],
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    service
        .transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    let mut worker = service.detach_message_deferred_work();
    drop(service);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the worker must own the reader after service drop"
    );
    let sink_scheduler = Arc::clone(&source_scheduler);
    let sink_runtime = runtime.handle().clone();
    let sink_replies = Arc::clone(&replies);
    let sink_original = native.commands[&target].clone();
    let sink_shards = native.native.metadata_shards().unwrap();
    let sink_local = source.clone();
    let other_shard = sink_shards
        .iter()
        .find(|shard| shard.shard_id == "services")
        .expect("the genuine native setup has the independent Services shard");
    let reciprocal_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: other_shard.file_system_id.clone(),
        shard_id: sinomo_dfs::DfsMetadataShardId::new(&other_shard.shard_id),
    };
    worker.set_background_response_handler(move |message, original| {
        let original =
            original.expect("checkpointed reply retains its original locally owned request");
        assert_eq!(original, &sink_original);
        let scope = super::message_host::owned_repair_completion_scope(
            original,
            &message,
            &sink_local,
            &sink_shards,
        )
        .expect("the exact native request and reply select their current owning shard");
        assert!(
            matches!(&scope, DfsMessageExecutionScope::ExactShard { shard_id, .. }
            if shard_id.0 == "root")
        );
        // Mutate only DATA clones of the actual native pair. None may earn even
        // a scheduling scope, and none is dispatched or applied to a backend.
        let mut foreign = original.clone();
        foreign.source_node_id = original.target_node_id.clone();
        assert!(
            super::message_host::owned_repair_completion_scope(
                &foreign,
                &message,
                &sink_local,
                &sink_shards,
            )
            .is_err()
        );
        let mut stale = original.clone();
        let DfsNetworkMessage::RepairReplica(request) = &mut stale.message else {
            unreachable!()
        };
        request.authority_shard.as_mut().unwrap().epoch += 1;
        assert!(
            super::message_host::owned_repair_completion_scope(
                &stale,
                &message,
                &sink_local,
                &sink_shards,
            )
            .is_err()
        );
        let mut wrong_path = original.clone();
        let DfsNetworkMessage::RepairReplica(request) = &mut wrong_path.message else {
            unreachable!()
        };
        request.object_metadata.as_mut().unwrap().path = "/services/foreign-repair.bin".into();
        assert!(
            super::message_host::owned_repair_completion_scope(
                &wrong_path,
                &message,
                &sink_local,
                &sink_shards,
            )
            .is_err()
        );
        let mut wrong_version = message.clone();
        let DfsNetworkMessage::RepairReplicaResponse(response) = &mut wrong_version.message else {
            unreachable!()
        };
        response.version =
            sinomo_core::Id::derived("test.owned-repair.foreign-version", b"negative");
        assert!(
            super::message_host::owned_repair_completion_scope(
                original,
                &wrong_version,
                &sink_local,
                &sink_shards,
            )
            .is_err()
        );
        let mut wrong_hash = message.clone();
        let DfsNetworkMessage::RepairReplicaResponse(response) = &mut wrong_hash.message else {
            unreachable!()
        };
        let DfsBlobTransferResult::Committed { content_hash } = &mut response.result else {
            panic!("the actual native peer committed this repair body")
        };
        content_hash.push('0');
        assert!(
            super::message_host::owned_repair_completion_scope(
                original,
                &wrong_hash,
                &sink_local,
                &sink_shards,
            )
            .is_err()
        );
        assert!(
            super::message_host::daemon_dfs_message_preempts_maintenance(&message.message),
            "ordinary unsolicited repair replies retain foreground classification"
        );
        let admission = sink_runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                admit_daemon_dfs_message(&sink_scheduler, &scope, false),
            )
            .await
            .expect("matched reply reacquires fresh source admission")
            .unwrap()
        });
        assert!(
            matches!(message.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if response.request_id == expected_request_id
                && matches!(response.result, DfsBlobTransferResult::Committed { .. }))
        );
        sink_replies.fetch_add(1, Ordering::SeqCst);
        drop(admission);
        Ok(())
    });
    drop(source_owner);
    runtime.block_on(async {
        let dispatch = tokio::task::spawn_blocking(move || {
            worker.drain_exclusive()?;
            assert!(!worker.has_pending()?);
            worker.drain_exclusive()
        });
        tokio::time::timeout(Duration::from_secs(1), queued_rx)
            .await
            .expect("repair reached target admission")
            .unwrap();
        let reciprocal = tokio::time::timeout(
            Duration::from_secs(1),
            admit_daemon_dfs_message(&source_scheduler, &reciprocal_scope, false),
        )
        .await
        .expect("independent Services work can enter while the Root repair waits")
        .unwrap();
        assert!(!source_interest.is_requested());
        assert!(!target_interest.is_requested());
        // Keep the independent shard lease until the real repair return has
        // completed its earned Root admission. A global restamp would stall.
        drop(target_owner);
        tokio::time::timeout(Duration::from_secs(1), dispatch)
            .await
            .expect("reciprocal work resolves without preempting either Routine")
            .unwrap()
            .unwrap();
        drop(reciprocal);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(replies.load(Ordering::SeqCst), 1);
    assert!(reads.load(Ordering::SeqCst) > 0);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!source_interest.is_requested());
    assert!(!target_interest.is_requested());
    native.native.close().unwrap();
}

#[test]
fn background_repair_worker_replacement_retains_parked_owned_payload() {
    use sinomo_dfs::DfsMessageTransport;

    let root = temp_root("background-repair-worker-replacement");
    let native = NativeOwnedBackgroundRepairFixture::new("background-repair-replacement-native", 1);
    let source = native.source.clone();
    let target = native.targets[0].clone();
    let expected_request_id = native.request_id(&target);
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let replies = Arc::new(AtomicUsize::new(0));
    let native_peer = Arc::clone(&native.peer);
    let peer_calls = Arc::clone(&calls);
    let peer_target = target.clone();
    let peer: Arc<dyn sinomo_dfs::DfsPeerTransport> = Arc::new(OwnedBackgroundRepairTestPeer(
        move |node: &NodeId,
              call: sinomo_core::network::ObjectRpcCall,
              options: sinomo_dfs::DfsObjectRpcSendOptions| {
            assert_eq!(node, &peer_target);
            peer_calls.fetch_add(1, Ordering::SeqCst);
            native_peer.call_object_rpc(node, call, options)
        },
    ));
    let new_service = || {
        NetworkedDfsService::new(
            DfsNodeTransport::new_with_peer_transport_for_nodes(
                source.clone(),
                [target.clone()],
                Arc::clone(&peer),
                Duration::from_secs(1),
            ),
            sinomo_dfs::LocalContentStore::new(root.join("replacement-source-content")).unwrap(),
        )
    };
    let mut service = new_service();
    service
        .transport
        .prepare_background_replica_dispatch(
            vec![owned_background_repair_test_call(
                &native,
                &target,
                Arc::clone(&reads),
                Arc::clone(&dropped),
            )],
            &[None],
        )
        .unwrap();
    service
        .transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    let mut previous = service.detach_message_deferred_work();
    drop(service);
    previous.set_background_response_handler(|_, _| panic!("an offline target must stay parked"));
    previous.set_deferred_target_node_ids([target.clone()].into_iter().collect());
    assert!(previous.has_pending().unwrap());
    assert!(!previous.scheduling_snapshot().unwrap().0);
    previous.drain_exclusive().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(!dropped.load(Ordering::SeqCst));

    let mut replacement_service = new_service();
    let mut replacement = replacement_service.detach_message_deferred_work();
    drop(replacement_service);
    replacement.retain_prepared_calls_from(&previous);
    assert!(!previous.has_pending().unwrap());
    drop(previous);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "replacement must retain the previous reader"
    );
    let sink_replies = Arc::clone(&replies);
    replacement.set_background_response_handler(move |message, original| {
        assert!(
            original.is_some(),
            "checkpointed reply retains its original locally owned request"
        );
        assert!(
            matches!(message.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if response.request_id == expected_request_id
                && matches!(response.result, DfsBlobTransferResult::Committed { .. }))
        );
        sink_replies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    replacement.set_deferred_target_node_ids([target].into_iter().collect());
    assert!(!replacement.scheduling_snapshot().unwrap().0);
    replacement.drain_exclusive().unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    replacement.set_deferred_target_node_ids(Default::default());
    assert!(replacement.scheduling_snapshot().unwrap().0);
    replacement.drain_exclusive().unwrap();
    replacement.drain_exclusive().unwrap();
    assert!(!replacement.has_pending().unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(replies.load(Ordering::SeqCst), 1);
    assert!(reads.load(Ordering::SeqCst) > 0);
    assert!(dropped.load(Ordering::SeqCst));
    native.native.close().unwrap();
}

#[test]
fn background_repair_same_policy_replacement_wakes_parked_empty_scheduler() {
    assert_background_repair_same_policy_replacement_wakes(false);
}

#[test]
fn background_repair_same_policy_publication_before_wait_registration_is_observed() {
    assert_background_repair_same_policy_replacement_wakes(true);
}

fn assert_background_repair_same_policy_replacement_wakes(register_after_publication: bool) {
    use sinomo_dfs::DfsMessageTransport;
    use std::task::Poll;

    let root = temp_root("background-repair-same-policy-wake");
    let native = NativeOwnedBackgroundRepairFixture::new("background-same-policy-native", 1);
    let source = native.source.clone();
    let target = native.targets[0].clone();
    let expected_request_id = native.request_id(&target);
    let expected_intent_id = native.intent_id(&target);
    let runtime = daemon_test_runtime("background-repair-same-policy-wake");
    let detached_runtime = Arc::new(DaemonDfsDetachedSendRuntime::new());
    let state = detached_runtime.detached_state();
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let replies = Arc::new(AtomicUsize::new(0));
    let native_peer = Arc::clone(&native.peer);
    let peer_calls = Arc::clone(&calls);
    let peer_target = target.clone();
    let peer: Arc<dyn sinomo_dfs::DfsPeerTransport> = Arc::new(OwnedBackgroundRepairTestPeer(
        move |node: &NodeId,
              call: sinomo_core::network::ObjectRpcCall,
              options: sinomo_dfs::DfsObjectRpcSendOptions| {
            assert_eq!(node, &peer_target);
            assert_eq!(call.request.route, "/dfs/data/repair-replica");
            peer_calls.fetch_add(1, Ordering::SeqCst);
            native_peer.call_object_rpc(node, call, options)
        },
    ));
    let new_service = || {
        NetworkedDfsService::new(
            DfsNodeTransport::new_with_peer_transport_for_nodes_and_detached_state(
                source.clone(),
                [target.clone()],
                Arc::clone(&peer),
                Duration::from_secs(1),
                state.clone(),
            ),
            sinomo_dfs::LocalContentStore::new(root.join("same-policy-source-content")).unwrap(),
        )
    };
    let mut empty_service = new_service();
    let empty_worker = empty_service.detach_message_deferred_work();
    drop(empty_service);
    let inspection_worker = empty_worker.clone();
    let mut task = None;
    detached_runtime.schedule_deferred_work(
        empty_worker,
        BTreeSet::new(),
        DAEMON_DFS_PROMOTION_RETRY_DELAY,
        |spawned| task = Some(spawned),
    );
    let mut task = task.expect("first publication starts the watcher");

    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                assert!(task.as_mut().poll(cx).is_pending());
                if detached_runtime.worker_wait_count() == 0 {
                    Poll::Pending
                } else {
                    // This poll reached the wait and returned Pending, so its
                    // empty-outbox notification has actually been registered.
                    Poll::Ready(())
                }
            }),
        )
        .await
        .expect("empty watcher registers its parked wait");
        assert_eq!(detached_runtime.worker_wait_count(), 1);
        let (ready, retry_at, observed_generation) =
            inspection_worker.scheduling_snapshot().unwrap();
        assert!(!ready);
        assert_eq!(retry_at, None);
        assert_eq!(state.queued_send_count(), 0);

        let mut replacement_service = new_service();
        replacement_service
            .transport
            .prepare_background_replica_dispatch(
                vec![owned_background_repair_test_call(
                    &native,
                    &target,
                    Arc::clone(&reads),
                    Arc::clone(&dropped),
                )],
                &[None],
            )
            .unwrap();
        replacement_service
            .transport
            .checkpoint_background_replica_dispatch()
            .unwrap();
        let mut replacement = replacement_service.detach_message_deferred_work();
        drop(replacement_service);
        let sink_replies = Arc::clone(&replies);
        replacement.set_background_response_handler(move |message, original| {
            assert!(
                original.is_some(),
                "checkpointed reply retains its original locally owned request"
            );
            assert!(
                matches!(message.message, DfsNetworkMessage::RepairReplicaResponse(response)
                if response.request_id == expected_request_id
                    && response.intent_id == expected_intent_id
                    && matches!(response.result, DfsBlobTransferResult::Committed { .. }))
            );
            sink_replies.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(!dropped.load(Ordering::SeqCst));
        assert_eq!(state.queued_send_count(), 0);
        assert_eq!(
            inspection_worker.scheduling_snapshot().unwrap().2,
            observed_generation,
            "checkpointed background calls do not enqueue ordinary outbox traffic"
        );

        // Constructing this future does not register it. The publication below
        // uses notify_waiters, which retains no permit for a future registrant.
        let before_registration = inspection_worker.wait_for_change(observed_generation);
        detached_runtime.schedule_deferred_work(
            replacement,
            BTreeSet::new(),
            DAEMON_DFS_PROMOTION_RETRY_DELAY,
            |_| panic!("an existing watcher must be reused"),
        );
        assert_eq!(detached_runtime.worker_schedule_generation(), 2);
        assert_eq!(
            inspection_worker.scheduling_snapshot().unwrap().2,
            observed_generation.wrapping_add(1)
        );
        if register_after_publication {
            tokio::pin!(before_registration);
            std::future::poll_fn(|cx| {
                match before_registration.as_mut().poll(cx) {
                    Poll::Ready(result) => result.unwrap(),
                    Poll::Pending => {
                        panic!("generation recheck must observe the earlier publication")
                    }
                }
                Poll::Ready(())
            })
            .await;
        }
        tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                assert!(task.as_mut().poll(cx).is_pending());
                if detached_runtime.worker_wait_count() < 2 {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }),
        )
        .await
        .expect("same-policy publication dispatches and parks again");
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(replies.load(Ordering::SeqCst), 1);
    assert!(reads.load(Ordering::SeqCst) > 0);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(state.queued_send_count(), 0);
    assert!(!inspection_worker.has_pending().unwrap());
    assert!(detached_runtime.worker_is_active());
    native.native.close().unwrap();
}

#[test]
fn background_repair_checkpoint_survives_cancellation_without_publishing_unpersisted_calls() {
    use sinomo_dfs::{DfsMaintenanceHost, DfsMessageTransport};
    use std::future::Future;

    let kinds = [
        DaemonDfsMaintenanceKind::Routine,
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            content_gc_requested: false,
            content_reservation_reconciliation_requested: false,
            commit_promotion_recovery_requested: false,
        },
    ];
    for (maintenance_kind, phase) in kinds.into_iter().flat_map(|kind| {
        ["persisted", "failed-persistence", "before-persistence"]
            .into_iter()
            .map(move |phase| (kind, phase))
    }) {
        let root = temp_root("background-repair-cancellation-checkpoint");
        let native = NativeOwnedBackgroundRepairFixture::new("background-cancel-native", 1);
        let source = native.source.clone();
        let target = native.targets[0].clone();
        let expected_request_id = native.request_id(&target);
        let runtime = daemon_test_runtime("background-repair-cancellation-checkpoint");
        let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
        let interest = scheduler.begin_maintenance_interest();
        let owner = runtime
            .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let replies = Arc::new(AtomicUsize::new(0));
        let native_peer = Arc::clone(&native.peer);
        let peer_calls = Arc::clone(&calls);
        let peer_target = target.clone();
        let peer: Arc<dyn sinomo_dfs::DfsPeerTransport> = Arc::new(OwnedBackgroundRepairTestPeer(
            move |node: &NodeId,
                  call: sinomo_core::network::ObjectRpcCall,
                  options: sinomo_dfs::DfsObjectRpcSendOptions| {
                assert_eq!(node, &peer_target);
                peer_calls.fetch_add(1, Ordering::SeqCst);
                native_peer.call_object_rpc(node, call, options)
            },
        ));
        let mut service = NetworkedDfsService::new(
            DfsNodeTransport::new_with_peer_transport_for_nodes(
                source.clone(),
                [target.clone()],
                peer,
                Duration::from_secs(1),
            ),
            sinomo_dfs::LocalContentStore::new(root.join("checkpoint-source-content")).unwrap(),
        );
        let mut older_worker = service.prepare_background_replica_worker();
        older_worker
            .set_background_response_handler(|_, _| panic!("an older holder must stay empty"));
        service.set_background_replica_dispatch(maintenance_kind.replica_dispatch_is_deferred());
        // Capture the new holder before the cancellable generation starts,
        // as both regular maintenance kinds do. Private preparations stay invisible.
        let mut worker = service.prepare_background_replica_worker();
        let call = owned_background_repair_test_call(
            &native,
            &target,
            Arc::clone(&reads),
            Arc::clone(&dropped),
        );
        let DfsNetworkMessage::RepairReplica(command) = &call.call.message.message else {
            unreachable!()
        };
        let mut attempt = mounted_mutation_test_repair_record("background-checkpoint");
        attempt.intent_id = command.intent_id.clone();
        attempt.object_id = command.object_id.clone();
        attempt.version = command.version.clone();
        attempt.content_hash = command.content_hash.clone();
        attempt.missing_replica_node_id = target;
        attempt.attempts = 1;
        attempt.in_flight = true;
        attempt.transfer_request_id = Some(command.request_id.clone());
        let data_dir = SinomoDataDir::new(root.join("data"));
        fs::create_dir_all(data_dir.root()).unwrap();
        if phase == "failed-persistence" {
            let queue_path = sinomo_dfs::default_dfs_repair_attempt_snapshot_path(&data_dir);
            let queue_dir = queue_path.parent().unwrap();
            fs::create_dir_all(queue_dir.parent().unwrap()).unwrap();
            fs::write(queue_dir, b"block the actual repair queue directory").unwrap();
        }
        let next_holder = Arc::new(Mutex::new(None));
        let captured_next_holder = Arc::clone(&next_holder);
        let outcome = runtime.block_on(async {
            let maintenance = async move {
                service
                    .transport
                    .prepare_background_replica_dispatch(vec![call], &[None])
                    .unwrap();
                if phase != "before-persistence" {
                    let mut host = sinomo_dfs::LocalDfsMaintenanceHost::new(data_dir);
                    let stored = host.store_repair_attempt_records(std::slice::from_ref(&attempt));
                    if phase == "persisted" {
                        stored.expect("persist the exact repair reservation");
                        assert_eq!(host.load_repair_attempt_records().unwrap(), vec![attempt]);
                        service
                            .transport
                            .checkpoint_background_replica_dispatch()
                            .unwrap();
                    } else {
                        assert!(stored.is_err(), "the real queue persistence must fail");
                        service.set_background_replica_dispatch(false);
                        service.set_background_replica_dispatch(true);
                        let next = service.prepare_background_replica_worker();
                        service
                            .transport
                            .checkpoint_background_replica_dispatch()
                            .unwrap();
                        *captured_next_holder.lock().unwrap() = Some(next);
                    }
                }
                // No pass success is returned. Foreground cancellation must
                // destroy this service while the outside checkpoint owner lives.
                std::future::pending::<()>().await;
            };
            let mut cancellation = Box::pin(run_daemon_dfs_maintenance_until_foreground_waits(
                &interest,
                maintenance,
            ));
            assert!(
                std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(cancellation.as_mut().poll(cx).is_pending())
                })
                .await
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            assert!(!older_worker.has_pending().unwrap());
            older_worker.drain_exclusive().unwrap();
            scheduler.request_foreground_admission();
            tokio::time::timeout(Duration::from_secs(1), cancellation)
                .await
                .expect("foreground cancels the pending generation")
        });
        assert!(matches!(
            outcome,
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        assert!(scheduler.global_is_exclusively_held());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(worker.has_pending().unwrap(), phase == "persisted");
        assert_eq!(dropped.load(Ordering::SeqCst), phase != "persisted");
        drop(owner);
        drop(interest);
        let sink_interest = scheduler.begin_maintenance_interest();
        let sink_scheduler = Arc::clone(&scheduler);
        let sink_runtime = runtime.handle().clone();
        let sink_replies = Arc::clone(&replies);
        worker.set_background_response_handler(move |message, original| {
            assert!(
                original.is_some(),
                "checkpointed reply retains its original locally owned request"
            );
            let admission = sink_runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    admit_daemon_dfs_message(
                        &sink_scheduler,
                        &DfsMessageExecutionScope::GlobalExclusive,
                        false,
                    ),
                )
                .await
                .expect("receipt reacquires released source admission")
                .unwrap()
            });
            assert!(
                matches!(message.message, DfsNetworkMessage::RepairReplicaResponse(response)
                if response.request_id == expected_request_id
                    && matches!(response.result, DfsBlobTransferResult::Committed { .. }))
            );
            sink_replies.fetch_add(1, Ordering::SeqCst);
            drop(admission);
            Ok(())
        });
        worker.drain_exclusive().unwrap();
        worker.drain_exclusive().unwrap();
        if let Some(mut next) = next_holder.lock().unwrap().take() {
            assert!(
                !next.has_pending().unwrap(),
                "failed preparations must not leak into the next Routine"
            );
            next.drain_exclusive().unwrap();
        }
        older_worker.drain_exclusive().unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(phase == "persisted")
        );
        assert_eq!(
            replies.load(Ordering::SeqCst),
            usize::from(phase == "persisted")
        );
        assert!(!worker.has_pending().unwrap());
        assert!(!sink_interest.is_requested());
        assert!(dropped.load(Ordering::SeqCst));
        native.native.close().unwrap();
    }
}

#[test]
fn background_repair_batch_delivers_healthy_reply_after_unmatched_peer() {
    assert_background_repair_batch_continues_after_error(true);
}

#[test]
fn background_repair_batch_delivers_healthy_reply_after_handler_error() {
    assert_background_repair_batch_continues_after_error(false);
}

fn assert_background_repair_batch_continues_after_error(unmatched_peer: bool) {
    use sinomo_dfs::DfsMessageTransport;

    let root = temp_root("background-repair-partial-reply");
    let native = NativeOwnedBackgroundRepairFixture::new("background-partial-reply-native", 2);
    let source = native.source.clone();
    let faulty = native.targets[0].clone();
    let healthy = native.targets[1].clone();
    let native_peer = Arc::clone(&native.peer);
    let repair_calls = Arc::new(AtomicUsize::new(0));
    let ordinary_calls = Arc::new(AtomicUsize::new(0));
    let healthy_replies = Arc::new(AtomicUsize::new(0));
    let peer_repairs = Arc::clone(&repair_calls);
    let peer_ordinary = Arc::clone(&ordinary_calls);
    let peer_faulty = faulty.clone();
    let peer_healthy = healthy.clone();
    let peer: Arc<dyn sinomo_dfs::DfsPeerTransport> = Arc::new(OwnedBackgroundRepairTestPeer(
        move |target: &NodeId,
              call: sinomo_core::network::ObjectRpcCall,
              options: sinomo_dfs::DfsObjectRpcSendOptions| {
            let is_repair = call.request.route == "/dfs/data/repair-replica";
            if is_repair {
                peer_repairs.fetch_add(1, Ordering::SeqCst);
            } else {
                assert_eq!(call.request.route, "/dfs/data/commit-version");
                peer_ordinary.fetch_add(1, Ordering::SeqCst);
            }
            assert!(target == &peer_faulty || target == &peer_healthy);
            let result = native_peer.call_object_rpc(target, call, options)?;
            if unmatched_peer && is_repair && target == &peer_faulty {
                // The actual native carrier returns its ordinary typed value
                // as a stream. Preserve the no-unexpected-attachment assertion,
                // then use the SAME normal decoder before one negative ID edit.
                assert!(result.has_streamed_value() || result.body.is_empty());
                let response_route = result.response.route.clone();
                let mut returned = sinomo_dfs::dfs_message_response_from_rpc_result_sync(result)?;
                let DfsNetworkMessage::RepairReplicaResponse(reply) =
                    &mut returned.outbound_messages[0].message
                else {
                    unreachable!()
                };
                reply.request_id = "unmatched-repair-response".into();
                sinomo_core::network::ObjectRpcResponse::encode_body(&response_route, &returned)
                    .map(sinomo_core::network::ObjectRpcResult::value)
                    .map_err(|error| sinomo_dfs::DfsPeerTransportError::new(error.to_string()))
            } else {
                Ok(result)
            }
        },
    ));
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::new_with_peer_transport_for_nodes(
            source.clone(),
            [faulty.clone(), healthy.clone()],
            peer,
            Duration::from_secs(1),
        ),
        sinomo_dfs::LocalContentStore::new(root.join("partial-source-content")).unwrap(),
    );
    service.set_background_replica_dispatch(true);
    let first = owned_background_repair_test_call(
        &native,
        &faulty,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let second = owned_background_repair_test_call(
        &native,
        &healthy,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let DfsNetworkMessage::RepairReplica(template) = &first.call.message.message else {
        unreachable!()
    };
    let promotion = sinomo_dfs::DfsCommitVersionCommand {
        request_id: "ordinary-background-promotion".into(),
        intent_id: template.intent_id.clone(),
        file_system_id: template.file_system_id.clone(),
        object_id: template.object_id.clone(),
        expected_base_version: None,
        version: template.version.clone(),
        content_hash: template.content_hash.clone(),
        object_metadata: template.object_metadata.clone(),
        authority_shard: template.authority_shard.clone(),
    };
    service
        .transport
        .prepare_background_replica_dispatch(vec![first, second], &[None, None])
        .unwrap();
    service
        .transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    service
        .transport
        .send_detached_routed_message(
            &faulty,
            DfsRoutedMessage {
                source_node_id: source,
                target_node_id: faulty.clone(),
                frame_id: 3,
                message_id: 3,
                message: DfsNetworkMessage::CommitVersion(promotion),
            },
        )
        .unwrap();
    assert_eq!(repair_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ordinary_calls.load(Ordering::SeqCst), 0);
    let mut worker = service.detach_message_deferred_work();
    drop(service);
    let observed_healthy = Arc::clone(&healthy_replies);
    worker.set_background_response_handler(move |message, original| {
        assert!(
            original.is_some(),
            "checkpointed reply retains its original locally owned request"
        );
        assert!(
            matches!(&message.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if matches!(response.result, DfsBlobTransferResult::Committed { .. }))
        );
        if message.source_node_id == faulty {
            return Err(sinomo_dfs::DfsError::Network {
                message: "injected repair response handler failure".into(),
            });
        }
        assert_eq!(message.source_node_id, healthy);
        observed_healthy.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let error = worker
        .drain_exclusive()
        .expect_err("one background return must fail");
    assert!(
        error.to_string().contains(if unmatched_peer {
            "unmatched response identity"
        } else {
            "injected repair response handler failure"
        }),
        "unexpected background failure: {error}"
    );
    assert_eq!(repair_calls.load(Ordering::SeqCst), 2);
    assert_eq!(healthy_replies.load(Ordering::SeqCst), 1);
    assert_eq!(
        ordinary_calls.load(Ordering::SeqCst),
        1,
        "ordinary detached work must flush despite a background reply failure"
    );
    assert!(!worker.has_pending().unwrap());
    worker.drain_exclusive().unwrap();
    assert_eq!(
        repair_calls.load(Ordering::SeqCst),
        2,
        "unknown background calls must not be resent"
    );
    assert_eq!(ordinary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(healthy_replies.load(Ordering::SeqCst), 1);
    native.native.close().unwrap();
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
    let (native, nodes) =
        crate::test_runtime::DfsTestRuntime::start_cluster("metadata-publish-lease-order-owner", 1)
            .expect("ordinary native owner startup");
    let owner_data_dir = native.data_dir(&nodes[0]).clone();
    let task_owner = registered_dfs_runtime_host(&owner_data_dir)
        .unwrap()
        .expect("actual registered native host");
    let shard_config = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let file_system_id = shard_config.file_system_id.clone();
    let shard_id = DfsMetadataShardId::new(shard_config.shard_id.clone());
    // This empty, unpublished pool models only checkout/admission ordering;
    // no rows, writer authority or healthy replica proof are installed in it.
    let resident = sinomo_dfs::LocalShardedMetadataBackend::new(
        root.path().join("metadata"),
        vec![shard_config],
    )
    .expect("resident metadata backend")
    .into_resident();
    let shard = resident
        .scope_shard(&file_system_id, &shard_id)
        .expect("resident root shard");
    let held_lease = shard.checkout().expect("hold resident root shard");

    let runtime = crate::test_runtime::executor();
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let lane = DaemonDfsMutationLaneKey {
        file_system_id,
        shard_id,
    };
    runtime.block_on(async {
        let planning_shard = shard.clone();
        let planning_scheduler = Arc::clone(&scheduler);
        let attempts = Arc::new(AtomicUsize::new(0));
        let planning_attempts = Arc::clone(&attempts);
        let (planning_started_tx, planning_started_rx) = tokio::sync::oneshot::channel();
        let mut planning_started_tx = Some(planning_started_tx);
        let publisher_owner = Arc::clone(&task_owner);
        let publisher = tokio::spawn(async move {
            run_daemon_dfs_metadata_publish_lane_planning_with_admission(
                planning_scheduler,
                lane,
                publisher_owner.as_ref(),
                move |_| {
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
    drop(task_owner);
    native.close().expect("ordinary native owner close");
}

#[test]
fn metadata_publish_admission_fences_planning_but_not_blocked_transport() {
    struct ExecutionGuardDrop(Arc<AtomicBool>);

    impl Drop for ExecutionGuardDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let (native, nodes) =
        crate::test_runtime::DfsTestRuntime::start_cluster("metadata-publish-foreground-owner", 1)
            .expect("ordinary native owner startup");
    let owner_data_dir = native.data_dir(&nodes[0]).clone();
    let task_owner = registered_dfs_runtime_host(&owner_data_dir)
        .unwrap()
        .expect("actual registered native host");
    let runtime = crate::test_runtime::executor();
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let file_system_id = native.file_system().file_system_id;
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
            let publisher_owner = Arc::clone(&task_owner);
            let publisher = tokio::spawn(async move {
                let _ = refresh_started_tx.send(());
                release_refresh_rx
                    .await
                    .map_err(|_| "blocked KV refresh release was dropped".to_string())?;
                run_daemon_dfs_metadata_publish_lane_planning_with_admission(
                    same_scheduler,
                    same_lane,
                    publisher_owner.as_ref(),
                    move |_| {
                        if let Some(planned) = planned_tx.take() {
                            let _ = planned.send(());
                        }
                        Ok(DaemonDfsMetadataPublishPlanningAttempt::Complete(()))
                    },
                )
                .await?;
                run_daemon_dfs_metadata_publish_blocking_execution(
                    tokio::runtime::Handle::current(),
                    publisher_owner.as_ref(),
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
                    task_owner.as_ref(),
                    |_| Ok(DaemonDfsMetadataPublishPlanningAttempt::Complete(())),
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
    drop(task_owner);
    native.close().expect("ordinary native owner close");
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
        3,
        "cold authority warmup must prove all three bounded configuration scopes on a 2 MiB worker"
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
    let (native, nodes) =
        crate::test_runtime::DfsTestRuntime::start_cluster("native-metadata-discovery-recovery", 1)
            .unwrap();
    let data_dir = native.data_dir(&nodes[0]).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = native.runtime_handle();
    // Drain only this fixture's startup edge before injecting missing topology.
    // All subsequent work stays in its actual registered component/task owner.
    let startup_deadline = Instant::now() + Duration::from_secs(5);
    while daemon_dfs_metadata_publish_discovery_is_active(&data_dir.root()) {
        assert!(
            Instant::now() < startup_deadline,
            "normal discovery startup must drain"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let initial = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    let saved_state = cluster_paths
        .state
        .with_extension("discovery-recovery-saved");
    std::fs::rename(&cluster_paths.state, &saved_state).unwrap();
    assert!(sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).is_err());
    let start = schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
        data_dir.clone(),
        cluster_paths.clone(),
        &runtime,
        None,
        None,
    );
    let first_deadline = Instant::now() + Duration::from_secs(2);
    while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 == initial.0
        && Instant::now() < first_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let failed = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    // Restore the exact same durable identity before assertions/cleanup; no
    // new configuration, artificial authority or second schedule wakes it.
    std::fs::rename(&saved_state, &cluster_paths.state).unwrap();
    assert_eq!(start, DfsMetadataPublishScheduleStart::Started);
    assert!(failed.0 > initial.0);
    assert_eq!(
        failed.1, initial.1,
        "absent topology cannot complete discovery"
    );
    let recovery_deadline = Instant::now() + Duration::from_secs(5);
    while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).1 == initial.1
        && Instant::now() < recovery_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut settled = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
    assert!(
        settled.0 >= initial.0 + 2,
        "the retained edge must retry planning"
    );
    assert!(
        settled.1 > initial.1,
        "topology recovery alone must drain the retained edge"
    );
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
    native.close().expect("drain the actual embedded owner");
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
        patch: None,
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
        retention_policies: Default::default(),
        module_metadata: Default::default(),
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
    // Enter the real owner/admission wrapper; calling the inner body cannot
    // supply clustered publication custody.
    runtime
        .block_on(run_daemon_dfs_maintenance_tick_kind_on_runtime(
            paths,
            runtime.handle(),
            DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
                content_gc_requested: true,
                content_reservation_reconciliation_requested: true,
                commit_promotion_recovery_requested: true,
            },
            sinomo_device::NodeResourceSnapshot::default(),
        ))
        .map(|_| ())
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
        DfsMetadataJournalReplicationOptions {
            background_publication: false,
            ..daemon_dfs_metadata_replication_options()
        }
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
    assert!(daemon_dfs_metadata_replication_options().background_publication);
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
    let name = "native-pending-unprotected-content";
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = crate::test_runtime::DfsTestRuntime::new(name, node.clone()).unwrap();
    native.start_nodes().unwrap();
    let client = native.client(&node).unwrap();
    let orphan = client
        .content()
        .put_object_version_blob_only(
            "content-only-object",
            "content-only-version",
            b"content without authoritative metadata",
        )
        .unwrap();
    let paths = DaemonPaths::new(native.data_dir(&node));
    let before = daemon_dfs_pending_reactive_maintenance_work_on_runtime(
        &paths,
        crate::test_runtime::executor().handle(),
    )
    .unwrap();
    assert_eq!(before.due_time_based_maintenance_count, 0);
    assert!(client.content().has_blob(&orphan.content_hash).unwrap());
    let after = daemon_dfs_pending_reactive_maintenance_work_on_runtime(
        &paths,
        crate::test_runtime::executor().handle(),
    )
    .unwrap();
    assert_eq!(after.due_time_based_maintenance_count, 0);
    assert_eq!(
        after.should_trigger_background_maintenance(),
        before.should_trigger_background_maintenance(),
        "physical unprotected bytes add no scheduling authority"
    );
    assert!(
        client.content().has_blob(&orphan.content_hash).unwrap(),
        "pending probe remains read-only"
    );
    drop(client);
    native.close().unwrap();
}

#[test]
fn repair_reconciliation_retains_bounded_pass_and_exact_namespace_compaction_scope() {
    let root = temp_root("bounded-reconciliation-request-retention");
    let paths = DaemonPaths::new(&SinomoDataDir::new(root.path()));
    let jobs = vec![
        "old-failed-job-a".to_string(),
        "old-failed-job-b".to_string(),
    ];
    let generation =
        request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs(&paths, jobs.clone())
            .unwrap();
    for outcome in [
        DaemonDfsMaintenanceTickOutcome::Deferred,
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: true,
        },
    ] {
        complete_daemon_dfs_maintenance_reconciliation_after_tick(
            &paths,
            Some(generation),
            outcome,
        )
        .unwrap();
        assert_eq!(
            daemon_dfs_maintenance_reconciliation_generation(&paths),
            Some(generation)
        );
        assert_eq!(
            daemon_dfs_requested_namespace_job_compaction(&paths),
            Some((generation, jobs.clone()))
        );
    }
    let newer = request_daemon_dfs_maintenance_reconciliation(&paths).unwrap();
    complete_daemon_dfs_namespace_job_compaction(&paths, generation).unwrap();
    complete_daemon_dfs_maintenance_reconciliation_after_tick(
        &paths,
        Some(generation),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        },
    )
    .unwrap();
    assert_eq!(
        daemon_dfs_requested_namespace_job_compaction(&paths),
        Some((newer, jobs))
    );
    complete_daemon_dfs_namespace_job_compaction(&paths, newer).unwrap();
    assert_eq!(daemon_dfs_requested_namespace_job_compaction(&paths), None);
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(newer)
    );
    complete_daemon_dfs_maintenance_reconciliation_after_tick(
        &paths,
        Some(newer),
        DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        },
    )
    .unwrap();
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        None
    );
    assert!(
        request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs(
            &paths,
            vec![String::new()]
        )
        .is_err()
    );
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        None
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
    content
        .ensure_content_reservation(
            covered_object_id.as_str(),
            covered_version.as_str(),
            &covered_content_hash,
        )
        .unwrap();
    let covered_stamp = content
        .content_reservation_roots()
        .unwrap()
        .reservations
        .into_iter()
        .find(|record| {
            record.identity.object_id == covered_object_id.as_str()
                && record.identity.version == covered_version.as_str()
        })
        .unwrap()
        .causal_stamp;
    let covered_reservations = sinomo_dfs::ContentStoreReservationRoots {
        reservation_count: 1,
        reservations: vec![sinomo_dfs::ContentStoreReservationRecord {
            identity: sinomo_dfs::ContentStoreReservationIdentity {
                object_id: covered_object_id.as_str().to_string(),
                version: covered_version.as_str().to_string(),
                content_hash: covered_content_hash.clone(),
            },
            reserved_at_unix_nanos: generated_at_unix_nanos,
            causal_stamp: covered_stamp,
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
    content
        .ensure_content_reservation(
            &rejected_object_id,
            &rejected_version,
            &format!("blake3:{}", "3".repeat(64)),
        )
        .unwrap();
    let rejected_stamp = content
        .content_reservation_roots()
        .unwrap()
        .reservations
        .into_iter()
        .find(|record| {
            record.identity.object_id == rejected_object_id
                && record.identity.version == rejected_version
        })
        .unwrap()
        .causal_stamp;
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
            causal_stamp: rejected_stamp,
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
    let manifest = content
        .read_manifest("pending-content-object", "pending-content-version-2")
        .expect("actual current live manifest");
    assert_eq!(manifest.object_id, "pending-content-object");
    assert_eq!(manifest.version, "pending-content-version-2");
    assert_eq!(
        manifest.content_hash,
        crate::content_hash(b"latest manifest")
    );
    assert_eq!(manifest.size_bytes, b"latest manifest".len() as u64);
    let catalog_before = serde_json::to_value(
        default_dfs_content_root_catalog_store(&content_root)
            .load()
            .expect("actual complete catalog before corruption"),
    )
    .unwrap();
    content
        .corrupt_current_manifest_for_test(&manifest)
        .expect("corrupt only the actual current manifest record");
    assert!(matches!(
        content.read_manifest(&manifest.object_id, &manifest.version),
        Err(crate::ContentStoreError::InvalidContentLayout { .. })
    ));
    let scrub_blocked = daemon_dfs_content_gc_projection(
        &data_dir,
        &cluster_paths,
        &content,
        &reservations,
        lifecycle,
        daemon_dfs_now_unix_nanos(),
    )
    .expect_err("invalid current manifest blocks a complete-catalog GC projection");
    assert!(matches!(scrub_blocked, DaemonError::Parse { .. }));
    assert_eq!(
        serde_json::to_value(
            default_dfs_content_root_catalog_store(&content_root)
                .load()
                .expect("complete catalog remains unchanged after refused GC"),
        )
        .unwrap(),
        catalog_before,
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
            write_intents: Default::default(),
            placement_reconciliation: None,
            lifecycle_reports: Vec::new(),
            scheduled_repairs: Vec::new(),
            followup_events: Vec::new(),
            repair_attempts_before: Vec::new(),
            repair_attempts_after: Vec::new(),
            repair_budget_exhausted: false,
            repair_page_checkpoint: Default::default(),
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
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids("native-resident-ingress", 2);
    let mut native =
        crate::test_support::DfsTestRuntime::new("native-resident-ingress", nodes[0].clone())
            .unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let config = native.root_shard().unwrap();
    let primary = config.primary_node_id.clone();
    let replica = config.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut source = native.client(&primary).unwrap();
    let before = source
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .prefix_proof
        .journal_sequence;
    let marker = source
        .posix_mkdir("/native-resident-ingress-marker")
        .unwrap();
    drop(source);
    let mut source = native.metadata_primary_service("/").unwrap();
    let completed = source.posix_sync_directory("/").unwrap();
    assert_eq!(
        completed
            .metadata_certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [primary.clone(), replica.clone()].into_iter().collect(),
        "duplicate-ingress setup must be durably complete on both real participants"
    );
    let backend = source.metadata_backend().unwrap();
    let mut subscription = backend.subscribe(before);
    let batch = backend.poll_subscription(&mut subscription).unwrap();
    let frames =
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
            .unwrap();
    let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &replica,
        &config.file_system_id,
        config.epoch,
        frames,
        false,
    );
    let data_dir = native.data_dir(&replica);
    let paths = ClusterPaths::new(data_dir);
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let key = DaemonDfsMutationLaneKey {
        file_system_id: config.file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let actual_handle = daemon_dfs_message_cache_exact_admission_handle(
        data_dir,
        &paths,
        &peer,
        &key,
        &native.runtime_handle(),
    )
    .unwrap()
    .unwrap();
    for _ in 0..2 {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_RPC_CATEGORY,
            &command,
        )
        .unwrap();
        let response = native
            .rpc_request(
                &primary,
                &replica,
                request,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        let response: DfsMetadataJournalBatchResponse = response.decode_body().unwrap();
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(
            response.applied_entries, 0,
            "real ingress replay cannot append the same native frame twice"
        );
        assert_eq!(
            response.next_after_sequence,
            command.batch.next_after_sequence
        );
        let again = daemon_dfs_message_cache_exact_admission_handle(
            data_dir,
            &paths,
            &peer,
            &key,
            &native.runtime_handle(),
        )
        .unwrap()
        .unwrap();
        assert!(
            again
                .with_backend(|backend| backend.get_object(&marker.object_id))
                .unwrap()
                .is_some()
        );
        assert!(actual_handle.is_open() && again.is_open());
    }
    let snapshot = backend
        .export_shard_snapshot_image(&shard_id)
        .unwrap()
        .unwrap();
    let snapshot_command = crate::DfsMetadataSnapshotInstallCommand {
        request_id: "native-resident-current-snapshot".into(),
        source_node_id: primary.clone(),
        file_system_id: Some(config.file_system_id.clone()),
        authority_epoch: Some(config.epoch),
        primary_handoff_id: None,
        shard_id,
        next_after_sequence: snapshot.prefix_proof.journal_sequence,
        snapshot: snapshot.descriptor().clone(),
        image: Some(snapshot),
        reason: "native exact-prefix replica snapshot regression".into(),
    };
    let (peer, _) = native.peer_transports(&primary).unwrap();
    let sender =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let response = sender(
        &replica,
        &snapshot_command,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )
    .unwrap();
    assert!(
        matches!(
            response.result,
            crate::DfsMetadataSnapshotInstallResult::Installed { .. }
        ),
        "actual native snapshot response: {response:?}"
    );
    assert_eq!(native.observation_dropped_count(), 0);
    native.assert_fault_budgets().unwrap();
    drop(actual_handle);
    drop(source);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_batch_applies_to_local_backend() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-first-replica-ingest", 2);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-first-replica-ingest", nodes[0].clone())
            .unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let mut writer = runtime.client(&primary).unwrap();
    let initial = runtime.metadata_primary_service("/").unwrap();
    let before = initial
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .prefix_proof
        .journal_sequence;
    drop(initial);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::PauseBeforeDispatch(gate.clone()),
    });
    let baseline = runtime.rpc_observations().len();
    std::thread::scope(|scope| {
        let publication = scope.spawn(move || {
            writer.posix_write_file(
                "/native-receiver-apply.bin",
                b"actual current family",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![primary.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
        });
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut observed = false;
        while Instant::now() < deadline {
            observed = runtime
                .rpc_observations()
                .into_iter()
                .skip(baseline)
                .any(|event| {
                    event.target == replica
                        && event.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                        && event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                });
            if observed {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let actual_ingest = (|| -> Result<_, String> {
            if !observed {
                return Err("native prepare dispatch was not observed".into());
            }
            let source = LocalSobjMetadataBackend::open_for_inspection(
                shard_id.clone(),
                crate::local_dfs_metadata_shard_path(
                    runtime.data_dir(&shard.primary_node_id),
                    &shard_id,
                ),
            )
            .map_err(|error| error.to_string())?;
            let batch = source
                .journal_batch_for_shard_since_limited(&shard_id, before, 1, usize::MAX)
                .map_err(|error| error.to_string())?
                .ok_or("actual retained prepare frame missing")?;
            assert!(matches!(
                &batch.entries[0].mutation,
                crate::DfsMetadataMutation::PrepareOperationV7 { .. }
            ));
            let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
                &source, &batch,
            )
            .map_err(|error| error.to_string())?;
            let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
                &shard.primary_node_id,
                &replica,
                &shard.file_system_id,
                shard.epoch,
                frames,
                false,
            );
            assert!(
                command.durable_replication.is_some(),
                "the actual frame carries its normal exact-prefix purpose"
            );
            let request = sinomo_core::network::ObjectRpcRequest::encode_body(
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_RPC_CATEGORY,
                &command,
            )
            .map_err(|error| error.to_string())?;
            let response = runtime.rpc_request(
                &shard.primary_node_id,
                &replica,
                request,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )?;
            let response: crate::DfsMetadataJournalBatchResponse =
                response.decode_body().map_err(|error| error.to_string())?;
            Ok(response)
        })();
        runtime.clear_faults();
        gate.add_permits(64);
        let published = publication.join().expect("native writer joins");
        let response = actual_ingest.expect("authenticated actual first ingest");
        assert_eq!(
            response.result,
            crate::DfsMetadataJournalBatchResult::Applied
        );
        assert_eq!(
            response.applied_entries, 1,
            "one real complete Prepare frame first ingested"
        );
        let stat = published.expect("normal original RPC exact replay and commit complete");
        let mut target = runtime.client(&replica).unwrap();
        let object = target
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(object.version, stat.version);
        assert_eq!(
            target
                .posix_read_file_distributed("/native-receiver-apply.bin")
                .unwrap(),
            b"actual current family"
        );
        drop(target);
    });
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.assert_fault_budgets().unwrap();
    runtime.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_batch_uses_path_safe_shard_directory() {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids("native-path-safe-shard", 2);
    let mut native =
        crate::test_support::DfsTestRuntime::new("native-path-safe-shard", nodes[0].clone())
            .unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let config = native.root_shard().unwrap();
    let primary = config.primary_node_id.clone();
    let replica = config.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut source = native.client(&primary).unwrap();
    source.posix_mkdir("/native-path-safe-marker").unwrap();
    drop(source);
    let mut source = native.metadata_primary_service("/").unwrap();
    let setup = source.posix_sync_directory("/").unwrap();
    assert_eq!(
        setup
            .metadata_certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [primary.clone(), replica.clone()].into_iter().collect(),
        "finish the legitimate fixture publication before observing hostile input"
    );
    let backend = source.metadata_backend().unwrap();
    let mut subscription = backend.subscribe(0);
    let batch = backend.poll_subscription(&mut subscription).unwrap();
    let frames =
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
            .unwrap();
    let mut command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &replica,
        &config.file_system_id,
        config.epoch,
        frames,
        false,
    );
    let data_dir = native.data_dir(&replica);
    let actual_root = crate::default_dfs_metadata_shards_root(data_dir);
    let inspect = || {
        crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            actual_root.join(crate::metadata_keys::key_segment(&shard_id.0)),
        )
        .unwrap()
        .export_table_snapshot()
    };
    let before = inspect();
    for hostile_name in ["root/frag", "../escaped-native-shard"] {
        let encoded = crate::metadata_keys::key_segment(&hostile_name);
        assert_eq!(
            std::path::Path::new(&encoded).components().count(),
            1,
            "the actual path encoder uses one physical component"
        );
        command.batch.shard_id = DfsMetadataShardId::new(hostile_name);
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_RPC_CATEGORY,
            &command,
        )
        .unwrap();
        if let Ok(response) = native.rpc_request(
            &primary,
            &replica,
            request,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        ) {
            let response: DfsMetadataJournalBatchResponse = response.decode_body().unwrap();
            assert!(matches!(
                response.result,
                DfsMetadataJournalBatchResult::Rejected { .. }
            ));
        }
        assert_eq!(
            inspect(),
            before,
            "an unconfigured or incoherent scope cannot change the real replica prefix/tables"
        );
        assert!(!actual_root.join(hostile_name).exists());
    }
    assert_eq!(native.observation_dropped_count(), 0);
    native.assert_fault_budgets().unwrap();
    drop(source);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_batch_rejects_non_primary_source() {
    let (runtime, source, shard) = crate::test_support::current_native_metadata_service(
        "native-reject-nonprimary-source",
        3,
        "/",
    )
    .unwrap();
    let target = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let backend = source.metadata_backend().unwrap();
    let batch = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    let frames =
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(backend, &batch)
            .unwrap();
    assert!(!frames.journal_frames.is_empty());
    let mut command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &shard.primary_node_id,
        &target,
        &shard.file_system_id,
        shard.epoch,
        frames,
        false,
    );
    command.source_node_id = Some(target.clone());
    let before = runtime
        .client(&target)
        .unwrap()
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    let mut transport = source.into_transport();
    let response =
        DfsMessageTransport::send_metadata_journal_batch_direct(&mut transport, &target, &command)
            .unwrap()
            .unwrap();
    assert!(matches!(
        response.result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    let target_client = runtime.client(&target).unwrap();
    let after = target_client
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        before.semantic_state_fingerprint
    );
    drop(target_client);
    drop(transport);
    runtime.close().unwrap();
}

#[test]
fn daemon_dfs_status_reads_default_metadata_backend() {
    let (runtime, mut service, shard) = crate::test_support::current_native_metadata_service(
        "native-daemon-dfs-status",
        1,
        "/services",
    )
    .unwrap();
    let node = shard.primary_node_id.clone();
    let stat = service
        .posix_write_file(
            "/services/native-status.txt",
            b"normal status bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    service
        .posix_sync_file("/services/native-status.txt", DfsPosixSyncKind::File)
        .unwrap();
    let expected = service
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap();
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .is_some()
    );
    drop(service);
    let data = runtime.data_dir(&node);
    let status =
        sinomo_dfs::inspect_cluster_aware_dfs_status(data, &ClusterPaths::new(data)).unwrap();
    assert_eq!(status.file_system_count, expected.file_system_count);
    assert_eq!(status.object_count, expected.object_count);
    assert_eq!(status.version_count, expected.version_count);
    assert!(status.object_count > 0 && status.version_count > 0);
    runtime.close().unwrap();
}

#[test]
fn daemon_dfs_maintenance_tick_resumes_namespace_jobs() {
    let name = "native-served-namespace-job-resume";
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = crate::test_runtime::DfsTestRuntime::new(name, node.clone()).unwrap();
    native.start_nodes().unwrap();
    let mut client = native.client(&node).unwrap();
    let old = "/services/native-job/old";
    let new = "/services/native-job/new";
    let source_file = format!("{old}/nested/file.txt");
    let target_file = format!("{new}/nested/file.txt");
    client.posix_mkdir_all(&format!("{old}/nested")).unwrap();
    client
        .posix_write_file(
            &source_file,
            b"actual native namespace job",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let job = client
        .posix_rename_directory_bounded_routed(old, new, 1)
        .unwrap();
    assert_eq!(job.status, DfsNamespaceJobStatus::Running);
    let paths = DaemonPaths::new(native.data_dir(&node));
    let observed_at = OffsetDateTime::now_utc();
    let pending = daemon_dfs_pending_maintenance_work_on_runtime(
        &paths,
        crate::test_runtime::executor().handle(),
    )
    .unwrap();
    let inspector = native.metadata_primary_service(old).unwrap();
    let actual_job = inspector
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job.job_id)
        .unwrap();
    drop(inspector);
    assert_eq!(
        pending.pending_namespace_job_count, 1,
        "actual job at probe starting {observed_at}: {actual_job:?}; captured reply={job:?}"
    );
    assert!(pending.should_trigger_background_maintenance());
    for _ in 0..8 {
        let tick = native
            .maintenance_tick(&node, Duration::from_secs(5))
            .unwrap();
        assert!(
            tick.outcome.is_ok(),
            "ordinary served job owner: {:?}",
            tick.outcome
        );
        let inspector = native.metadata_primary_service(old).unwrap();
        let stored = inspector
            .metadata_backend()
            .unwrap()
            .get_namespace_job(&job.job_id)
            .unwrap()
            .unwrap();
        drop(inspector);
        if stored.status == DfsNamespaceJobStatus::Completed {
            break;
        }
    }
    let inspector = native.metadata_primary_service(old).unwrap();
    let stored = inspector
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job.job_id)
        .unwrap()
        .unwrap();
    drop(inspector);
    assert_eq!(stored.status, DfsNamespaceJobStatus::Completed);
    assert!(stored.completed_at.is_some());
    assert_eq!(
        daemon_dfs_pending_maintenance_work_on_runtime(
            &paths,
            crate::test_runtime::executor().handle()
        )
        .unwrap()
        .pending_namespace_job_count,
        0
    );
    assert_eq!(
        client.posix_read_file(&target_file).unwrap(),
        b"actual native namespace job"
    );
    assert!(client.posix_read_file(&source_file).is_err());
    drop(client);
    native.close().unwrap();
}

fn placement_reconciliation_job_for_test(
    state: DfsPlacementReconciliationState,
) -> DfsPlacementReconciliationJob {
    let node = NodeId::generate();
    let mut native =
        crate::test_runtime::DfsTestRuntime::new("current-placement-probe-data", node.clone())
            .unwrap();
    native.start_nodes().unwrap();
    let control = native.shared_control_plane().unwrap();
    let accepted_config = control.with_client(|kv| {
        crate::DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&native.file_system().file_system_id)
            .unwrap()
            .unwrap()
    });
    let mut client = native.client(&node).unwrap();
    let candidate_snapshot = client.placement_content_candidates().unwrap();
    drop(client);
    let job = DfsPlacementReconciliationJob {
        schema_version: sinomo_dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
        job_id: "placement-probe-job".to_string(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        path: "/services".to_string(),
        scope: sinomo_dfs::DfsPlacementScope::Subtree,
        unavailable_node_ids: BTreeSet::new(),
        candidate_snapshot,
        placement_snapshot: None,
        adaptive: false,
        adaptive_budget: None,
        processed_content_bytes: 0,
        accepted_config,
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
    };
    native.close().unwrap();
    job
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
        transfer_delivery_state: Some(
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
        ),
        completed_transfer_request_id: None,
        transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
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
        transfer_delivery_state: Some(
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
        ),
        completed_transfer_request_id: None,
        transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
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
    // These are negative scheduling DATA, with the same past/future deadlines.
    // They cannot establish transport custody, an ACK, or a writable owner.
    let mut unidentified = snapshot.records.clone();
    for record in &mut unidentified {
        record.transfer_kind = None;
        record.transfer_delivery_state = None;
    }
    let untouched = unidentified.clone();
    let unavailable = crate::dfs_pending_reactive_maintenance_work_from_authority_entries(
        &data_dir,
        &node_id,
        &[],
        &unidentified,
        now,
    )
    .expect("same production classifier retains unavailable repair DATA");
    assert_eq!(unavailable.pending_repair_count, unidentified.len());
    assert_eq!(unavailable.repair_attempt_count, unidentified.len());
    assert_eq!(unavailable.due_repair_attempt_count, 0);
    assert_eq!(unavailable.next_repair_retry_unix_nanos, None);
    assert!(!unavailable.should_trigger_background_maintenance());
    assert_eq!(
        unidentified, untouched,
        "classification never changes queue deadlines or custody"
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
fn reactive_placement_probe_wakes_only_the_current_root_primary() {
    let root = temp_root("local-placement-classifier");
    let data_dir = SinomoDataDir::new(root.path());
    let owner = NodeId::from_legacy_named("placement_owner").unwrap();
    let replica = NodeId::from_legacy_named("placement_replica").unwrap();
    let job = placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Pending);
    let mut shard = DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: "root".into(),
        primary_node_id: owner.clone(),
        replica_node_ids: vec![replica.clone()],
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let entries = |shard: &DfsMetadataShardConfig| {
        default_dfs_reactor_test_snapshot(
            1,
            vec![
                (
                    metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                    sinomo_core::kv::encode_kv_value(shard).unwrap(),
                ),
                (
                    sinomo_dfs::placement_reconciliation_key(&job.file_system_id, &job.job_id),
                    sinomo_core::kv::encode_kv_value(&job).unwrap(),
                ),
            ],
        )
        .entries
    };
    let probe = |node, shard: &DfsMetadataShardConfig| {
        crate::dfs_pending_reactive_maintenance_work_from_authority_entries(
            &data_dir,
            node,
            &entries(shard),
            &[],
            daemon_dfs_now_unix_nanos(),
        )
        .unwrap()
    };
    assert_eq!(
        probe(&owner, &shard).actionable_placement_reconciliation_count,
        1
    );
    assert_eq!(
        probe(&replica, &shard).actionable_placement_reconciliation_count,
        0
    );
    assert!(!probe(&replica, &shard).should_trigger_background_maintenance());
    shard.primary_node_id = replica.clone();
    shard.epoch += 1;
    assert_eq!(
        probe(&replica, &shard).actionable_placement_reconciliation_count,
        1
    );
    assert!(!probe(&owner, &shard).should_trigger_background_maintenance());
}

#[test]
fn deferred_reconciliation_retries_without_unrelated_placement_work() {
    let root = temp_root("deferred-reconciliation-retry");
    let paths = DaemonPaths::new(&SinomoDataDir::new(root.path()));
    let generation = schedule_daemon_dfs_startup_repair_reconciliation(&paths).unwrap();
    let now = Instant::now();
    let idle = crate::dfs_maintenance_completion_decision(
        DaemonDfsMaintenanceTickOutcome::Deferred,
        now,
        daemon_dfs_now_unix_nanos(),
        Some(Default::default()),
        false,
    );
    assert!(!idle.trigger_maintenance);
    let retry = super::component_runtime::retain_requested_reconciliation(&paths, idle);
    assert!(retry.trigger_maintenance);
    assert!(
        !retry.immediate_continuation,
        "preemption keeps the existing retry delay"
    );
    assert_eq!(
        crate::dfs_maintenance_due_in(
            now,
            now,
            now - (crate::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
                - crate::DFS_MAINTENANCE_LEASE_RETRY_DELAY),
            retry.trigger_maintenance,
            retry.immediate_continuation,
        ),
        Some(crate::DFS_MAINTENANCE_LEASE_RETRY_DELAY)
    );
    complete_daemon_dfs_maintenance_reconciliation(&paths, generation).unwrap();
    assert!(
        !super::component_runtime::retain_requested_reconciliation(&paths, idle)
            .trigger_maintenance
    );
}

#[test]
fn recovery_handoff_is_durable_preserves_backoff_and_fences_completion() {
    let root = temp_root("recovery-handoff-checkpoint");
    let data_dir = SinomoDataDir::new(root.path());
    let paths = DaemonPaths::new(&data_dir);
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(1, Vec::new()),
    ));
    let authority_generation = authority.generation();
    let first = request_daemon_dfs_maintenance_reconciliation(&paths).unwrap();
    let mut backed_off = mounted_mutation_test_repair_record("handoff_retry");
    backed_off.attempts = 4;
    backed_off.next_retry_unix_nanos = u64::MAX;
    persist_daemon_dfs_repair_queue_records(&data_dir, vec![backed_off.clone()]).unwrap();
    let mut rediscovered = backed_off.clone();
    rediscovered.attempts = 0;
    rediscovered.next_retry_unix_nanos = 0;
    let new_record = mounted_mutation_test_repair_record("handoff_new");
    let newer = request_daemon_dfs_maintenance_reconciliation(&paths).unwrap();
    assert!(
        super::maintenance_host::checkpoint_daemon_dfs_reconciliation(
            &data_dir,
            &authority,
            authority_generation,
            Some(first),
            vec![rediscovered, new_record.clone()],
        )
        .unwrap()
    );
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(newer)
    );
    let durable =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))
            .unwrap()
            .unwrap();
    assert!(durable.records.contains(&backed_off));
    assert!(durable.records.contains(&new_record));
    assert!(
        super::maintenance_host::checkpoint_daemon_dfs_reconciliation(
            &data_dir,
            &authority,
            authority_generation,
            Some(newer),
            Vec::new(),
        )
        .unwrap()
    );
    // Cancellation of a later network phase retains both the completed scan
    // and the runnable request. Authority changes cannot reuse that checkpoint.
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(newer)
    );
    assert!(
        super::maintenance_host::daemon_dfs_reconciliation_checkpoint_matches(
            &paths,
            Some(newer),
            authority_generation,
        )
    );
    assert!(
        !super::maintenance_host::daemon_dfs_reconciliation_checkpoint_matches(
            &paths,
            Some(newer),
            authority_generation + 1,
        )
    );
    let idle = crate::dfs_maintenance_probe_decision(
        Instant::now(),
        daemon_dfs_now_unix_nanos(),
        Some(Default::default()),
    );
    assert!(
        super::component_runtime::retain_requested_reconciliation(&paths, idle).trigger_maintenance
    );
    complete_daemon_dfs_maintenance_reconciliation(&paths, newer).unwrap();
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        None
    );
    let pending = request_daemon_dfs_maintenance_reconciliation(&paths).unwrap();
    authority.require_authoritative_revision(KvRevision(2));
    assert!(
        !super::maintenance_host::checkpoint_daemon_dfs_reconciliation(
            &data_dir,
            &authority,
            authority_generation,
            Some(pending),
            Vec::new(),
        )
        .unwrap()
    );
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&paths),
        Some(pending)
    );
    complete_daemon_dfs_maintenance_reconciliation(&paths, pending).unwrap();

    let failed_root = temp_root("recovery-handoff-storage-failure");
    let failed_dir = SinomoDataDir::new(failed_root.path());
    let failed_paths = DaemonPaths::new(&failed_dir);
    fs::create_dir_all(failed_dir.dfs_dir().parent().unwrap()).unwrap();
    fs::write(failed_dir.dfs_dir(), b"not a directory").unwrap();
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(2, Vec::new()),
    ));
    let pending = request_daemon_dfs_maintenance_reconciliation(&failed_paths).unwrap();
    assert!(
        super::maintenance_host::checkpoint_daemon_dfs_reconciliation(
            &failed_dir,
            &authority,
            authority.generation(),
            Some(pending),
            vec![new_record],
        )
        .is_err()
    );
    assert_eq!(
        daemon_dfs_maintenance_reconciliation_generation(&failed_paths),
        Some(pending)
    );
    complete_daemon_dfs_maintenance_reconciliation(&failed_paths, pending).unwrap();
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
        transfer_delivery_state: None,
        completed_transfer_request_id: None,
        transfer_kind: None,
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
        transfer_delivery_state: None,
        completed_transfer_request_id: None,
        transfer_kind: None,
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
    let mut fixture = native_pending_source_fixture("native-offline-only-below-w", false);
    let replica = fixture.native.root_shard().unwrap().replica_node_ids[0].clone();
    let reader = fixture.native.client(&replica).unwrap();
    let intent = fixture.intent.clone();
    let receipt = fixture.receipt.clone();
    drop(fixture.source.take());
    fixture.native.stop_node(&fixture.primary).unwrap();
    let offline = BTreeSet::from([fixture.primary.clone()]);
    let source_less = daemon_dfs_intents_without_recovery_source(
        fixture.native.data_dir(&replica),
        reader.metadata_backend().unwrap(),
        std::slice::from_ref(&intent),
        std::slice::from_ref(&receipt),
        &offline,
        &replica,
    );
    assert_eq!(
        source_less,
        BTreeSet::from([intent.intent_id.clone()]),
        "the only genuine receipt holder is stopped and W2 still lacks its second receipt"
    );
    let pending = daemon_dfs_pending_work_from_records(
        1,
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
        &offline,
        &replica,
        0,
        3,
    );
    assert_eq!(pending.pending_write_count, 0);
    assert!(!pending.should_trigger_background_maintenance());
    drop(reader);
    fixture.native.reopen_node(fixture.primary.clone()).unwrap();
    let mut reopened = fixture.native.metadata_primary_service("/").unwrap();
    reopened.hydrate_from_metadata_backend().unwrap();
    fixture.source = Some(reopened);
    fixture.close();
}

#[test]
fn daemon_dfs_stale_local_quorum_receipt_without_content_is_parked() {
    with_native_create_proposal(
        "native-stale-local-quorum",
        false,
        false,
        |native, primary, payload| {
            let intent = payload.prepared_intent.as_ref().unwrap();
            assert_eq!(intent.policy.min_commit_replicas, 1);
            assert_eq!(payload.content_receipts.len(), 1);
            assert_eq!(payload.content_receipts[0].replica_node_id, *primary);
            let data_dir = native.data_dir(primary);
            let content =
                crate::LocalContentStore::new(data_dir.dfs_dir().join("content")).unwrap();
            content
                .debug_remove_loose_blob(intent.content_hash.as_ref().unwrap())
                .unwrap();
            let metadata = crate::LocalSobjMetadataBackend::open_for_inspection(
                payload.shard_id.clone(),
                crate::default_dfs_metadata_shards_root(data_dir)
                    .join(crate::metadata_keys::key_segment(&payload.shard_id.0)),
            )
            .unwrap();
            assert!(metadata.get_object(&intent.object_id).unwrap().is_none());
            let source_less = daemon_dfs_intents_without_recovery_source(
                data_dir,
                &metadata,
                std::slice::from_ref(intent),
                &payload.content_receipts,
                &BTreeSet::new(),
                primary,
            );
            assert_eq!(
                source_less,
                BTreeSet::from([intent.intent_id.clone()]),
                "the actual prior durable receipt cannot replace missing bytes and an unpublished create size"
            );
            let pending = daemon_dfs_pending_work_from_records(
                1,
                0,
                0,
                0,
                std::slice::from_ref(intent),
                &payload.content_receipts,
                &[],
                &[],
                &[],
                &source_less,
                &BTreeSet::new(),
                &BTreeSet::new(),
                primary,
                0,
                3,
            );
            assert_eq!(pending.pending_write_count, 0);
            assert!(!pending.should_trigger_background_maintenance());
        },
    );
}

#[test]
fn daemon_dfs_extent_backed_pending_write_is_an_executable_local_source() {
    let fixture = native_pending_source_fixture("native-extent-pending-source", true);
    let source = fixture.source.as_ref().unwrap();
    let data_dir = fixture.native.data_dir(&fixture.primary);
    let content = crate::LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"));
    let manifest = content
        .read_manifest(
            fixture.intent.object_id.as_str(),
            fixture.intent.target_version.as_str(),
        )
        .unwrap();
    assert!(
        !manifest.extents.is_empty(),
        "normal patch staging must retain its actual extent manifest"
    );
    let source_less = daemon_dfs_intents_without_recovery_source(
        data_dir,
        source.metadata_backend().unwrap(),
        std::slice::from_ref(&fixture.intent),
        std::slice::from_ref(&fixture.receipt),
        &BTreeSet::new(),
        &fixture.primary,
    );
    assert!(
        source_less.is_empty(),
        "real readable extents remain a local source below the frozen W2 threshold"
    );
    assert_eq!(fixture.intent.policy.min_commit_replicas, 2);
    assert_eq!(fixture.intent.desired_replica_node_ids.len(), 2);
    fixture.close();
}

#[test]
fn daemon_dfs_quorum_ready_write_still_triggers_when_remaining_replica_is_offline() {
    with_native_create_proposal(
        "native-ready-offline-desired",
        true,
        true,
        |native, primary, payload| {
            let intent = payload.prepared_intent.as_ref().unwrap();
            assert_eq!(intent.policy.min_commit_replicas, 1);
            assert_eq!(intent.policy.desired_replicas, 2);
            assert_eq!(payload.content_receipts.len(), 1);
            let remaining = intent
                .desired_replica_node_ids
                .iter()
                .find(|node| *node != primary)
                .unwrap()
                .clone();
            native.stop_node(&remaining).unwrap();
            let data_dir = native.data_dir(primary);
            let metadata = crate::LocalSobjMetadataBackend::open_for_inspection(
                payload.shard_id.clone(),
                crate::default_dfs_metadata_shards_root(data_dir)
                    .join(crate::metadata_keys::key_segment(&payload.shard_id.0)),
            )
            .unwrap();
            let offline = BTreeSet::from([remaining.clone()]);
            let source_less = daemon_dfs_intents_without_recovery_source(
                data_dir,
                &metadata,
                std::slice::from_ref(intent),
                &payload.content_receipts,
                &offline,
                primary,
            );
            assert!(
                source_less.is_empty(),
                "real W1 receipt plus the real staged reader manifest retains executable hydration"
            );
            let pending = daemon_dfs_pending_work_from_records(
                1,
                0,
                0,
                0,
                std::slice::from_ref(intent),
                &payload.content_receipts,
                &[],
                &[],
                &[],
                &source_less,
                &BTreeSet::new(),
                &offline,
                primary,
                0,
                3,
            );
            assert_eq!(pending.pending_write_count, 1);
            assert!(pending.should_trigger_background_maintenance());
            drop(metadata);
            native.clear_faults();
            native.reopen_node(remaining).unwrap();
        },
    );
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

        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        cause: crate::DfsMetadataShardHandoffCause::Membership,
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
    let name = "native-completed-primary-rebind-wake";
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let retiring = native.root_shard().unwrap();
    let control = native.shared_control_plane().unwrap();
    let mut source = native.client(&retiring.primary_node_id).unwrap();
    source.posix_mkdir("/native-rebind-source-cut").unwrap();
    let promoted_data = native.data_dir(&nodes[1]).clone();
    // Hold only local activation. Global admission also blocks the fresh
    // replica proof required to complete the actual handoff itself.
    let rebind_hold = install_daemon_dfs_pre_dispatch_test_hook(
        &promoted_data,
        Arc::new(|point, _, _| {
            if matches!(point, DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind { shard_id }
                if shard_id.0 == "root")
            {
                return Err(crate::DfsError::Cancelled {
                    operation: "negative local completed-primary activation interruption",
                });
            }
            Ok(())
        }),
    );
    let desired = DfsMetadataShardConfig {
        primary_node_id: nodes[1].clone(),
        replica_node_ids: vec![nodes[0].clone()],
        epoch: retiring.epoch + 1,
        ..retiring.clone()
    };
    drop(source);
    let installed = native
        .reassign_metadata_shard(
            &retiring.file_system_id,
            "root",
            desired.primary_node_id.clone(),
            desired.replica_node_ids.clone(),
        )
        .unwrap();
    assert_eq!(installed, desired);
    let actual = control.with_client(|kv| {
        crate::DfsControlPlaneStore::new(kv.as_mut())
            .get_metadata_shard(&desired.file_system_id, "root")
            .unwrap()
            .unwrap()
    });
    assert_eq!(actual, desired);
    let paths = DaemonPaths::new(&promoted_data);
    let promoted_cluster = ClusterPaths::new(&promoted_data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&promoted_cluster).unwrap();
    let runtime = native.runtime_handle();
    let preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &promoted_cluster,
            &state.cluster_id,
            &state.local_node_id,
            &runtime,
        ))
        .unwrap();
    let projection = preflight.authority.current_complete_prefix().unwrap();
    let projected_shard = projection
        .entries()
        .iter()
        .find(|entry| entry.key == crate::metadata_shard_key(&desired.file_system_id, "root"))
        .unwrap()
        .decode::<DfsMetadataShardConfig>()
        .unwrap();
    let projected_proof = projection
        .entries()
        .iter()
        .find(|entry| {
            entry.key
                == crate::metadata_shard_membership_handoff_proof_key(
                    &desired.file_system_id,
                    "root",
                )
        })
        .map(|entry| {
            entry
                .decode::<DfsMetadataShardMembershipHandoffProof>()
                .unwrap()
        });
    eprintln!(
        "promoted={} expected={desired:?} retained={projected_shard:?} proof={projected_proof:?} sorted={}",
        nodes[1],
        projection
            .entries()
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
    );
    let pending =
        daemon_dfs_pending_reactive_maintenance_work_on_runtime(&paths, &runtime).unwrap();
    assert_eq!(pending.pending_local_membership_handoff_count, 0);
    assert_eq!(
        pending.completed_local_primary_handoff_rebind_count,
        1,
        "actual promoted marker: {:?}",
        read_local_dfs_metadata_shard_write_freeze(
            &promoted_data,
            &DfsMetadataShardId::new("root"),
        )
        .unwrap()
    );
    assert!(
        pending.should_trigger_background_maintenance(),
        "the actual unactivated receiver marker retains its completion wake"
    );
    drop(rebind_hold);
    let observed = native
        .maintenance_tick(&nodes[1], Duration::from_secs(5))
        .unwrap();
    assert!(
        observed.outcome.is_ok(),
        "actual promoted owner: {:?}",
        observed.outcome
    );
    let marker = read_local_dfs_metadata_shard_write_freeze(
        &promoted_data,
        &DfsMetadataShardId::new("root"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(marker.activated_epoch, Some(desired.epoch));
    assert_eq!(
        daemon_dfs_pending_reactive_maintenance_work_on_runtime(&paths, &runtime)
            .unwrap()
            .completed_local_primary_handoff_rebind_count,
        0
    );
    drop(preflight);
    let mut promoted = native.client(&nodes[1]).unwrap();
    assert!(promoted.posix_stat("/native-rebind-source-cut").is_ok());
    promoted.posix_mkdir("/native-rebind-new-author").unwrap();
    drop(promoted);
    drop(control);
    native.close().unwrap();
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

        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        cause: crate::DfsMetadataShardHandoffCause::Membership,
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
    let name = "native-served-anti-entropy";
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = crate::test_runtime::DfsTestRuntime::new(name, node.clone()).unwrap();
    native.start_nodes().unwrap();
    let mut client = native.client(&node).unwrap();
    let path = "/services/native-anti-entropy/missing.bin";
    client
        .posix_mkdir_all("/services/native-anti-entropy")
        .unwrap();
    let stat = client
        .posix_write_file(
            path,
            &vec![0x5a; 2 * 1024 * 1024],
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let object = client
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert!(object.current_locations.contains(&node));
    client
        .content()
        .debug_remove_loose_blob(object.content_hash.as_deref().unwrap())
        .unwrap();
    let paths = DaemonPaths::new(native.data_dir(&node));
    for _ in 0..8 {
        // Inventory reconciliation is explicit. Routine resident ticks are
        // deliberately inventory-free and must remain quiet without this ask.
        let started = Instant::now();
        let executor = crate::test_runtime::executor();
        let outcome = executor.block_on(run_daemon_dfs_maintenance_tick_kind_on_runtime(
            &paths,
            executor.handle(),
            DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
                content_gc_requested: false,
                content_reservation_reconciliation_requested: true,
                commit_promotion_recovery_requested: false,
            },
            sinomo_device::NodeResourceSnapshot::default(),
        ));
        // Join the original native invocation before checking the same limit;
        // never abandon a running data owner on a timeout.
        assert!(started.elapsed() <= Duration::from_secs(5));
        assert!(
            outcome.is_ok(),
            "explicit actual anti-entropy owner: {outcome:?}"
        );
        let inspector = native.metadata_primary_service(path).unwrap();
        if !inspector
            .current_read_object_metadata(&object.object_id)
            .unwrap()
            .current_locations
            .contains(&node)
        {
            break;
        }
    }
    let inspector = native.metadata_primary_service(path).unwrap();
    let current = inspector
        .current_read_object_metadata(&object.object_id)
        .unwrap();
    assert!(!current.current_locations.contains(&node));
    let version = inspector
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    assert!(!version.locations.contains(&node));
    assert!(
        !sinomo_dfs::default_dfs_anti_entropy_cursor_path(
            native.data_dir(&node),
            &object.file_system_id
        )
        .exists(),
        "completed actual scan clears its cursor"
    );
    drop(inspector);
    drop(client);
    native.close().unwrap();
}

// Establish real lag without allowing a publication with an incomplete ACK set.
// The stopped receiver has durably accepted the exact hidden prepare; it has not
// received the later commit marker when the publisher resumes.
fn create_directory_with_replica_stopped_before_commit(
    runtime: &mut crate::test_runtime::DfsTestRuntime,
    source: &mut crate::NetworkedDfsService<crate::DfsNodeTransport>,
    primary: &NodeId,
    replica: &NodeId,
    path: &str,
) -> crate::DfsPreparedOperationDigest {
    let (captured_tx, captured_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let pause = Mutex::new(Some((captured_tx, release_rx)));
    let expected_path = path.to_string();
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, command, _| {
        if !matches!(point, DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }) {
            return Ok(());
        }
        let Some(command) = command else {
            return Ok(());
        };
        let crate::DfsNetworkMessage::NamespaceRequest(request) = &command.routed_message.message
        else {
            return Ok(());
        };
        if !matches!(&request.operation, crate::DfsNamespaceOperation::MkdirAll { path } if path == &expected_path)
        {
            return Ok(());
        }
        let (captured, release) = pause
            .lock()
            .unwrap()
            .take()
            .expect("one native directory producer");
        let path = expected_path.clone();
        crate::publication_guard::AFTER_PREPARED_AUTHORIZATION.with(|hook| {
            assert!(hook.borrow().is_none());
            *hook.borrow_mut() = Some(Box::new(move |digest, payload| {
                assert!(
                    payload
                        .transaction
                        .objects
                        .iter()
                        .any(|object| object.path == path)
                );
                assert_eq!(digest.payload_digest, payload.canonical_digest().unwrap());
                captured.send(digest.clone()).unwrap();
                release
                    .recv()
                    .expect("test releases the real admitted writer");
            }));
        });
        Ok(())
    });
    let hook = install_daemon_dfs_pre_dispatch_test_hook(runtime.data_dir(primary), hook);
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let digest = std::thread::scope(|scope| {
        let release = Release(Some(release_tx));
        let publisher = scope.spawn(|| source.posix_mkdir_all(path));
        let digest = captured_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("actual full-participant author guard");
        runtime
            .stop_node(replica)
            .expect("stop acknowledged disposable replica before commit");
        drop(release);
        publisher
            .join()
            .unwrap()
            .expect("publication uses its genuine complete prepare acknowledgements");
        digest
    });
    drop(hook);
    let backend = crate::LocalSobjMetadataBackend::open_for_inspection(
        digest.shard_id.clone(),
        crate::default_dfs_metadata_shards_root(runtime.data_dir(replica))
            .join(crate::metadata_keys::key_segment(&digest.shard_id.0)),
    )
    .unwrap();
    let original = backend
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.digest, digest);
    assert_eq!(original.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(
        backend
            .get_object_by_path(&digest.file_system_id, path)
            .unwrap()
            .is_none()
    );
    digest
}

#[test]
fn daemon_dfs_status_reads_configured_sharded_metadata_backend() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("native-sharded-status", 3);
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("native-sharded-status", nodes[0].clone())
            .expect("normal native runtime");
    for node in &nodes[1..] {
        runtime
            .define_node(node.clone())
            .expect("actual metadata member");
    }
    runtime.start_nodes().expect("native component startup");
    let control = runtime
        .shared_control_plane()
        .expect("native control plane");
    let configs = control
        .with_client(|kv| {
            sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut())
                .list_metadata_shards(&sinomo_dfs::sifs_file_system_id())
        })
        .expect("actual assignments");
    let services = configs
        .iter()
        .find(|config| config.shard_id == "services")
        .unwrap()
        .clone();
    let source_node = services.primary_node_id.clone();
    let offline_node = services
        .replica_node_ids
        .iter()
        .find(|node| **node != nodes[0])
        .expect("non-voter metadata replica")
        .clone();
    let data_dir = runtime.data_dir(&source_node).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
    let mut source = runtime
        .client(&source_node)
        .expect("actual primary service");
    let before = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths).unwrap();
    let services_id = DfsMetadataShardId::new("services");
    let before_sequence = source
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&services_id)
        .unwrap()
        .unwrap()
        .materialized_through_sequence;
    let admitted = create_directory_with_replica_stopped_before_commit(
        &mut runtime,
        &mut source,
        &source_node,
        &offline_node,
        "/services/status-sharded",
    );
    assert_eq!(admitted.prepare_sequence, before_sequence + 1);
    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
        .expect("current sharded status");
    assert_eq!(status.file_system_count, 1);
    assert_eq!(status.object_count, before.object_count + 1);
    assert!(status.version_count > before.version_count);
    assert!(
        status.blocking_incomplete_reasons().is_empty(),
        "{:?}",
        status.incomplete_reasons
    );
    assert!(!status.bounded_omissions.iter().any(|omission| matches!(
        omission,
        sinomo_dfs::DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
    )));
    assert_eq!(status.metadata_shards.len(), configs.len());
    let services_status = status
        .metadata_shards
        .iter()
        .find(|shard| shard.shard_id == services_id)
        .expect("actual services status");
    assert_eq!(
        services_status.journal_last_sequence,
        before_sequence + 2,
        "the guarded directory mutation contributes one prepare and one commit"
    );
    assert!(services_status.replay_cursors.is_empty());
    let stopped_lag = services_status
        .replica_lag
        .iter()
        .find(|lag| lag.target_node_id == offline_node)
        .expect("stopped configured participant lag");
    assert_eq!(
        services_status.journal_last_sequence - admitted.prepare_sequence,
        1,
        "full-participant publication leaves only its commit marker behind the acknowledged preparation"
    );
    assert_eq!(
        stopped_lag.lag_entries,
        services_status.journal_last_sequence - admitted.prepare_sequence,
        "reported lag equals the proven native prefix gap, not an assumed degraded-write frame count"
    );
    let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
    assert_eq!(pending.pending_write_count, 0);
    assert_eq!(pending.pending_repair_count, 0);
    assert_eq!(pending.pending_namespace_job_count, 0);
    assert_eq!(
        pending.metadata_replica_lag_count, 0,
        "offline lag is not immediately executable work"
    );
    assert_eq!(
        pending.deferred_work_count, 1,
        "the stopped participant's exact commit-marker gap remains accounted for"
    );
    assert!(
        !pending.has_work(),
        "only the explicitly counted deferred publisher remains"
    );
    assert_eq!(pending.pending_local_membership_handoff_count, 0);
    assert_eq!(pending.due_time_based_maintenance_count, 0);
    assert!(
        !pending.should_trigger_background_maintenance(),
        "metadata lag is owned by retained exact-shard publishers: {pending:#?}"
    );
    let DaemonDfsInspectionResponse::FileSystem {
        file_system: Some(view),
    } = sinomo_dfs::inspect_cluster_aware_dfs(
        &data_dir,
        &cluster_paths,
        DaemonDfsInspectionRequest::FileSystem {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
        },
    )
    .expect("filesystem inspection")
    else {
        panic!("expected filesystem view")
    };
    assert_eq!(view.object_count, status.object_count);
    assert_eq!(view.pending_repair_count, 0);
    drop(source);
    drop(control);
    runtime.close().expect("native fixture shutdown");
}

#[test]
fn daemon_dfs_status_reports_replica_lag_only_from_primary() {
    let nodes =
        crate::test_support::DfsTestRuntime::ordered_node_ids("native-replica-lag-status", 2);
    let primary = nodes[0].clone();
    let replica = nodes[1].clone();
    let mut fixture =
        crate::test_support::DfsTestRuntime::new("native-replica-lag-status", primary.clone())
            .expect("normal runtime");
    fixture
        .define_node(replica.clone())
        .expect("actual replica");
    fixture
        .start_nodes()
        .expect("normal native component startup");
    let fs = fixture.file_system().file_system_id;
    let control = fixture
        .shared_control_plane()
        .expect("actual native control plane");
    let configs = control
        .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&fs))
        .expect("actual assignments");
    for config in &configs {
        if config.primary_node_id != primary {
            let replicas =
                crate::control_plane::topology_preserving_metadata_shard_replicas(config, &primary)
                    .expect("same actual member set");
            fixture
                .reassign_metadata_shard(&fs, &config.shard_id, primary.clone(), replicas)
                .expect("native primary placement");
        }
    }
    let data_dir = fixture.data_dir(&replica);
    let status =
        sinomo_dfs::inspect_cluster_aware_dfs_status(data_dir, &ClusterPaths::new(data_dir))
            .expect("actual replica status");
    assert_eq!(status.metadata_shards.len(), configs.len());
    for shard in &status.metadata_shards {
        assert_eq!(shard.primary_node_id, Some(primary.clone()));
        assert!(
            shard.replica_lag.is_empty(),
            "a replica must not claim source-side replication lag: {shard:?}"
        );
    }
    drop(control);
    fixture.close().expect("normal owned runtime shutdown");
}

fn late_native_replication_fixture(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    crate::NetworkedDfsService<crate::DfsNodeTransport>,
    DfsMetadataShardConfig,
) {
    let (native, requester, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/")
            .expect("normal owned native metadata fixture");
    drop(requester);
    let source = native
        .metadata_primary_service("/")
        .expect("actual owning Root Core producer");
    (native, source, shard)
}

fn late_native_hidden_suffix(
    service: &mut crate::NetworkedDfsService<crate::DfsNodeTransport>,
    shard: &DfsMetadataShardConfig,
    count: usize,
) -> u64 {
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let current = service
        .metadata_backend()
        .unwrap()
        .get_file_system_on_shard(&shard_id, &shard.file_system_id)
        .expect("actual owning filesystem row read")
        .expect("normal native filesystem row exists");
    for index in 0..count {
        let mut next = current.clone();
        next.display_name.push_str(&format!("-hidden-{index}"));
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard_id.clone(),
                owner_epoch: shard.epoch,
                operation_id: format!("late-native-hidden-{}-{index}", sinomo_core::Id::generate()),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system: next }],
            OffsetDateTime::now_utc(),
        )
        .expect("complete current hidden body");
        service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(payload, None)
            .expect("sole normal current prepare kernel");
    }
    service
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .find(|status| status.shard_id.0 == shard.shard_id)
        .unwrap()
        .journal_last_sequence
}

fn late_native_publish_options() -> crate::DfsMetadataJournalReplicationOptions {
    crate::DfsMetadataJournalReplicationOptions {
        probe_caught_up_replicas: true,
        attempt_timeout: Duration::from_secs(1),
        target_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}

#[test]
fn daemon_dfs_metadata_journal_publish_replicates_all_local_primary_shards() {
    let (native, service, root) = late_native_replication_fixture("native-publish-all-shards");
    let local = root.primary_node_id.clone();
    let fs = root.file_system_id.clone();
    let configs = native.metadata_shards().unwrap();
    drop(service);
    let mut expected = Vec::new();
    for config in configs
        .iter()
        .filter(|config| matches!(config.shard_id.as_str(), "root" | "services"))
    {
        let mut members = config.replica_node_ids.clone();
        members.push(config.primary_node_id.clone());
        members.sort();
        members.dedup();
        members.retain(|node| node != &local);
        let installed = native
            .reassign_metadata_shard(&fs, &config.shard_id, local.clone(), members)
            .unwrap();
        let mut source = native
            .metadata_primary_service(if installed.shard_id == "root" {
                "/"
            } else {
                "/services"
            })
            .unwrap();
        late_native_hidden_suffix(&mut source, &installed, 1);
        expected.push(DfsMetadataShardId::new(installed.shard_id));
    }
    let (peer, _) = native.peer_transports(&local).unwrap();
    let actual = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        native.data_dir(&local),
        Arc::clone(&peer),
    );
    let snapshot =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let sent = Arc::new(Mutex::new(Vec::new()));
    let copied = Arc::clone(&sent);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
        copied.lock().unwrap().push(command.batch.shard_id.clone());
        actual(node, command, budget)
    });
    let data = native.data_dir(&local);
    let report = crate::publish_cluster_dfs_metadata_journal(
        data,
        &ClusterPaths::new(data),
        sender,
        snapshot,
        late_native_publish_options(),
    )
    .unwrap();
    for shard in expected {
        assert!(
            sent.lock().unwrap().contains(&shard),
            "normal publisher must visit each local-primary shard"
        );
        assert!(
            report.outcomes.iter().any(|outcome| matches!(outcome,
            DfsMetadataJournalReplicationOutcome::Applied { shard_id, .. } if shard_id == &shard)),
            "the visited shard must receive an actual receiver acknowledgement: {report:?}"
        );
    }
    assert_eq!(native.observation_dropped_count(), 0);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_publish_drains_bounded_batches() {
    let (native, mut service, shard) =
        late_native_replication_fixture("native-publish-bounded-suffix");
    let local = shard.primary_node_id.clone();
    let target = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let count = crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 7;
    let head = late_native_hidden_suffix(&mut service, &shard, count);
    drop(service);
    let (peer, _) = native.peer_transports(&local).unwrap();
    let actual = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        native.data_dir(&local),
        Arc::clone(&peer),
    );
    let snapshot =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let sent = Arc::new(Mutex::new(Vec::new()));
    let copied = Arc::clone(&sent);
    let selected = target.clone();
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
        if node == &selected && command.batch.shard_id == shard_id {
            copied.lock().unwrap().push(command.clone());
        }
        actual(node, command, budget)
    });
    let data = native.data_dir(&local);
    let report = crate::publish_cluster_dfs_metadata_journal(
        data,
        &ClusterPaths::new(data),
        sender,
        snapshot,
        late_native_publish_options(),
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    let frames = sent
        .iter()
        .filter(|command| !command.batch.journal_frames.is_empty())
        .collect::<Vec<_>>();
    assert!(
        frames.len() >= 2,
        "actual suffix exceeds one bounded batch: {report:?}"
    );
    for command in &frames {
        assert!(
            command.batch.journal_frames.len() <= crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
        );
        assert!(
            command.durable_replication.is_some(),
            "actual framed suffix uses durable replication"
        );
        for frame in &command.batch.journal_frames {
            assert!(
                !frame.exact_bytes.is_empty(),
                "each selected journal frame retains exact native bytes"
            );
        }
    }
    assert_eq!(frames.last().unwrap().batch.next_after_sequence, head);
    drop(sent);
    let source = native.client(&local).unwrap();
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_replication_cursor(&target, &DfsMetadataShardId::new(shard.shard_id))
            .unwrap(),
        head
    );
    drop(source);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_publish_rejects_mismatched_ack_cursor() {
    let (native, mut service, shard) =
        late_native_replication_fixture("native-publish-mismatched-ack");
    let local = shard.primary_node_id.clone();
    let target = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let before = service
        .metadata_backend()
        .unwrap()
        .metadata_replication_cursor(&target, &shard_id)
        .unwrap();
    let head = late_native_hidden_suffix(&mut service, &shard, 1);
    drop(service);
    let (peer, _) = native.peer_transports(&local).unwrap();
    let actual = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        native.data_dir(&local),
        Arc::clone(&peer),
    );
    let snapshot =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let selected = target.clone();
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
        let mut response = actual(node, command, budget)?;
        if node == &selected && command.batch.shard_id == shard_id {
            assert!(matches!(
                response.result,
                DfsMetadataJournalBatchResult::Applied
            ));
            response.next_after_sequence = response.next_after_sequence.saturating_add(1);
        }
        Ok(response)
    });
    let data = native.data_dir(&local);
    let report = crate::publish_cluster_dfs_metadata_journal(
        data,
        &ClusterPaths::new(data),
        sender,
        snapshot,
        late_native_publish_options(),
    )
    .unwrap();
    assert!(report.outcomes.iter().any(|outcome| matches!(outcome,
        DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. } if target_node_id == &target)));
    let source = native.client(&local).unwrap();
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_replication_cursor(&target, &DfsMetadataShardId::new(shard.shard_id))
            .unwrap(),
        before,
        "a corrupted actual response must not advance the scheduling cursor"
    );
    let receiver = native.client(&target).unwrap();
    assert!(
        receiver
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap()
            .iter()
            .any(|status| status.shard_id.0 == "root" && status.journal_last_sequence >= head),
        "negative response corruption happens only after real durable receiver ingestion"
    );
    drop(receiver);
    drop(source);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_publish_stops_after_replica_rejection() {
    let (native, mut service, shard) =
        late_native_replication_fixture("native-publish-real-rejection");
    let local = shard.primary_node_id.clone();
    let target = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let before = service
        .metadata_backend()
        .unwrap()
        .metadata_replication_cursor(&target, &shard_id)
        .unwrap();
    late_native_hidden_suffix(&mut service, &shard, 1);
    drop(service);
    let (peer, _) = native.peer_transports(&local).unwrap();
    let actual = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        native.data_dir(&local),
        Arc::clone(&peer),
    );
    let snapshot =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let selected = target.clone();
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
        if node != &selected || command.batch.shard_id != shard_id {
            return actual(node, command, budget);
        }
        observed.fetch_add(1, Ordering::SeqCst);
        let mut invalid = command.clone();
        invalid.authority_epoch = Some(shard.epoch.saturating_add(1));
        actual(node, &invalid, budget)
    });
    let data = native.data_dir(&local);
    let report = crate::publish_cluster_dfs_metadata_journal(
        data,
        &ClusterPaths::new(data),
        sender,
        snapshot,
        late_native_publish_options(),
    )
    .unwrap();
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "actual stale-epoch receiver rejection terminates this target attempt"
    );
    assert!(report.outcomes.iter().any(|outcome| matches!(outcome,
        DfsMetadataJournalReplicationOutcome::Rejected { target_node_id, .. } if target_node_id == &target)));
    let source = native.client(&local).unwrap();
    assert_eq!(
        source
            .metadata_backend()
            .unwrap()
            .metadata_replication_cursor(&target, &DfsMetadataShardId::new("root"))
            .unwrap(),
        before
    );
    drop(source);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_metadata_journal_publish_repairs_sequence_conflict_with_snapshot() {
    use std::io::{Seek, Write};
    let (mut native, mut service, shard) =
        late_native_replication_fixture("native-host-snapshot-repair");
    let local = shard.primary_node_id.clone();
    let target = shard.replica_node_ids[0].clone();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let object = service
        .posix_write_file(
            "/native-host-snapshot-repair",
            b"actual retained source bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![local.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let snapshot = service
        .metadata_backend()
        .unwrap()
        .export_shard_snapshot_image(&shard_id)
        .unwrap()
        .unwrap();
    let head = snapshot.materialized_through_sequence;
    let command = crate::DfsMetadataSnapshotInstallCommand {
        request_id: "native-host-real-snapshot-repair".into(),
        source_node_id: local.clone(),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: head,
        snapshot: snapshot.descriptor().clone(),
        image: Some(snapshot),
        reason: "actual current snapshot corruption recovery".into(),
    };
    drop(service);
    let (peer, _) = native.peer_transports(&local).unwrap();
    let sender =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let installed = sender(&target, &command, Duration::from_secs(5)).unwrap();
    assert!(matches!(
        installed.result,
        DfsMetadataSnapshotInstallResult::Installed { .. }
    ));
    let root = crate::default_dfs_metadata_shards_root(native.data_dir(&target))
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    native.stop_node(&target).unwrap();
    let snapshot_path = crate::metadata_table_store::metadata_table_snapshot_path(&root);
    let reader =
        sinomo_core::storage::migration::stream::MigrationStreamReader::open(&snapshot_path)
            .unwrap();
    let offset = reader.cursor().offset + 8;
    drop(reader);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&snapshot_path)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    file.write_all(b"BAD").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &root).is_err());
    native.reopen_node(target.clone()).unwrap();
    let repaired = sender(&target, &command, Duration::from_secs(5)).unwrap();
    assert!(matches!(
        repaired.result,
        DfsMetadataSnapshotInstallResult::Installed {
            archived_existing: true
        }
    ));
    assert_eq!(repaired.next_after_sequence, head);
    assert!(root.join("replica-repair-archives").is_dir());
    let receiver = native.client(&target).unwrap();
    let retained = receiver
        .metadata_backend()
        .unwrap()
        .get_object(&object.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.version, object.version);
    assert_eq!(retained.content_hash, object.content_hash);
    drop(receiver);
    drop(sender);
    native.stop_node(&target).unwrap();
    native.reopen_node(target.clone()).unwrap();
    let receiver = native.client(&target).unwrap();
    assert_eq!(
        receiver
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap()
            .unwrap(),
        retained
    );
    drop(receiver);
    native.close().unwrap();
}
#[test]
fn daemon_dfs_metadata_journal_publish_bounds_endpoint_fallbacks_per_target() {
    let (native, mut service, shard) =
        late_native_replication_fixture("native-publish-target-attempt");
    let local = shard.primary_node_id.clone();
    let target = shard.replica_node_ids[0].clone();
    late_native_hidden_suffix(&mut service, &shard, 1);
    drop(service);
    native.install_fault(crate::test_runtime::FaultRule {
        source: Some(local.clone()),
        target: Some(target.clone()),
        route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    let (peer, _) = native.peer_transports(&local).unwrap();
    let actual = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(
        native.data_dir(&local),
        Arc::clone(&peer),
    );
    let snapshot =
        crate::metadata_replication::dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&attempted);
    let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
        observed
            .lock()
            .unwrap()
            .push((node.clone(), command.batch.shard_id.clone()));
        actual(node, command, budget)
    });
    let data = native.data_dir(&local);
    let report = crate::publish_cluster_dfs_metadata_journal(
        data,
        &ClusterPaths::new(data),
        sender,
        snapshot,
        late_native_publish_options(),
    )
    .unwrap();
    assert!(report.outcomes.iter().any(|outcome| matches!(outcome,
        DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. } if target_node_id == &target)));
    assert_eq!(
        attempted
            .lock()
            .unwrap()
            .iter()
            .filter(|(node, shard)| *node == target && shard.0 == "root")
            .count(),
        1,
        "publisher makes one target-level attempt; the normal carrier owns route fallback"
    );
    native.clear_faults();
    native.close().unwrap();
}

#[test]
fn cancelled_daemon_dfs_rpc_retains_committed_work_edges() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-cancel-committed-edge", 3);
    let mut native =
        crate::test_runtime::DfsTestRuntime::new("native-cancel-committed-edge", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    let trace = crate::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-cancel-committed-edge",
        nodes
            .iter()
            .map(|node| (node.clone(), native.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .expect("actual cancellation capture before normal mounts");
    native.start_nodes().unwrap();
    let shard = native.root_shard().unwrap();
    let local = shard.primary_node_id.clone();
    let remote = shard.replica_node_ids[0].clone();
    let missing_replica = shard.replica_node_ids[1].clone();
    let data_dir = native.data_dir(&local).clone();
    let paths = DaemonPaths::new(&data_dir);
    let file_system = native.file_system();
    let path = "/cancelled-publish-edge.txt";
    let mut placement =
        crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = vec![local.clone(), missing_replica.clone()];
    let accepted = native
        .apply_placement_policy(&local, path, crate::DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let control = native.shared_control_plane().unwrap();
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                crate::DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert_eq!(job.scanned_object_count, 0);
        assert!(job.last_error.is_none());
        if job.state == crate::DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero());
        native
            .maintenance_tick(&local, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let mut source = native.metadata_primary_service(path).unwrap();
    let (_, _, policy) = source
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    let prior = source
        .posix_write_file(
            path,
            b"real previous head",
            DfsWriteOptions {
                policy: policy.clone(),
                replica_candidates: vec![local.clone(), missing_replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let base_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while source
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&prior.object_id, &prior.version)
        .unwrap()
        .len()
        != 2
    {
        assert!(
            Instant::now() < base_deadline,
            "the real predecessor copies must settle before the cancellation fault"
        );
        source.retry_pending_repairs_bounded(1).unwrap();
        source.process_incoming().unwrap();
    }
    let original = source
        .metadata_backend()
        .unwrap()
        .get_object(&prior.object_id)
        .unwrap()
        .unwrap();
    drop(source);
    native.install_fault(crate::test_runtime::FaultRule {
        source: Some(local.clone()),
        target: Some(missing_replica.clone()),
        route: Some(crate::DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    let request_id = "dfs_message_cancelled_publish_edge";
    let frame_id = 90_101;
    let object_id = original.object_id.clone();
    let mut routed_message = test_dfs_write_routed_message(&remote, &local, frame_id, path);
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed_message.message else {
        panic!("distributed write request");
    };
    write.request.object_id = original.object_id.clone();
    write.expected_base_version = Some(original.version.clone());
    write.object_metadata = Some(original);
    write.authority_shard = Some(shard.clone());
    write.requester_blob_pre_staged = false;
    write.desired_replica_nodes = vec![local.clone(), missing_replica];
    write.policy = policy;
    let committed_version = write.request.target_version.clone();
    let committed_hash = crate::content_hash(&write.bytes);
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    let command = test_dfs_message_command(request_id, routed_message);
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
    };
    let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);
    let initial_repair_edges = daemon_dfs_repair_maintenance_edge_request_count(&data_dir);
    let (committed_rx, release_tx) =
        install_daemon_dfs_message_committed_edge_test_hook(request_id);
    let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-publish-edge");
    let invocation = native.rpc_result_future(
        &remote,
        &local,
        crate::dfs_message_rpc_call(&command).unwrap(),
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let rpc = runtime.handle().spawn(invocation);

    committed_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("durable message mutation reached committed-edge boundary");
    assert!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            crate::default_dfs_metadata_shards_root(&data_dir)
                .join(crate::metadata_keys::key_segment(&shard.shard_id)),
        )
        .expect("reopen metadata backend")
        .get_object(&object_id)
        .expect("read committed object")
        .is_some_and(|object| object.version == committed_version
            && object.content_hash.as_deref() == Some(committed_hash.as_str())),
        "the NEW mutation must be durable before the cancellable RPC boundary"
    );
    assert_eq!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            crate::default_dfs_metadata_shards_root(&data_dir)
                .join(crate::metadata_keys::key_segment(&shard.shard_id)),
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
    native.clear_faults();
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    native.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}

#[test]
fn cancelled_daemon_dfs_rpc_retains_subscribed_replica_progress_edge() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-cancel-replica-progress", 3);
    let mut native = crate::test_runtime::DfsTestRuntime::new(
        "native-cancel-replica-progress",
        nodes[0].clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    let trace = crate::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-cancel-replica-progress",
        nodes
            .iter()
            .map(|node| (node.clone(), native.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .expect("actual cancellation capture before normal mounts");
    native.start_nodes().unwrap();
    let shard = native.root_shard().unwrap();
    let mut source = native
        .metadata_primary_service("/native-cancel-progress-marker")
        .unwrap();
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    let data_dir = native.data_dir(&replica).clone();
    let paths = DaemonPaths::new(&data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let file_system = native.file_system();
    let stat = source
        .posix_write_file(
            "/native-cancel-progress-marker",
            b"actual replicated body",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![primary.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let object = source
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let target = source.capture_metadata_quorum_sync_target(&object).unwrap();
    let certificate = source
        .sync_metadata_shard_to_quorum(&target, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert_eq!(certificate.acknowledged_node_ids.len(), 3);
    native.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: None,
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    let before = source
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .find(|status| status.shard_id.0 == shard.shard_id)
        .unwrap()
        .journal_last_sequence;
    let after_sequence = late_native_hidden_suffix(&mut source, &shard, 1);
    assert_eq!(after_sequence, before + 1);
    let expected_prefix = source
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new(&shard.shard_id))
        .unwrap()
        .unwrap();
    let batch = source
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new(shard.shard_id.clone()),
            before,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    let framed = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
        source.metadata_backend().unwrap(),
        &batch,
    )
    .unwrap();
    drop(source);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
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
            target_node_id: replica.clone(),
            frame_id: 90_102,
            message_id: 90_102,
            message: DfsNetworkMessage::MetadataJournalBatch(
                crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
                    &primary,
                    &replica,
                    &file_system.file_system_id,
                    shard.epoch,
                    framed,
                    false,
                ),
            ),
        },
    );
    let (committed_rx, release_tx) =
        install_daemon_dfs_message_committed_edge_test_hook(request_id);
    let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-replica-progress");
    let invocation = native.rpc_result_future(
        &primary,
        &replica,
        crate::dfs_message_rpc_call(&command).unwrap(),
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let rpc = runtime.handle().spawn(invocation);

    committed_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("replica mutation reached committed-edge boundary");
    assert_eq!(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            crate::default_dfs_metadata_shards_root(&data_dir)
                .join(crate::metadata_keys::key_segment(&shard.shard_id)),
        )
        .expect("reopen replica backend")
        .get_object(&object.object_id)
        .expect("read replicated object"),
        Some(object.clone()),
        "the replica journal batch must be durable before RPC cancellation"
    );
    let durable = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&shard.shard_id),
        crate::default_dfs_metadata_shards_root(&data_dir)
            .join(crate::metadata_keys::key_segment(&shard.shard_id)),
    )
    .unwrap();
    assert_eq!(
        durable.journal_last_sequence(),
        after_sequence,
        "the actual new native frame is durable before caller cancellation"
    );
    assert_eq!(
        durable.journal_since_limited(before, 1).unwrap(),
        batch.entries,
        "the receiver persisted the exact original frame, not just an old visible object"
    );
    drop(durable);
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
    loop {
        let materialized = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(&shard.shard_id),
            crate::default_dfs_metadata_shards_root(&data_dir)
                .join(crate::metadata_keys::key_segment(&shard.shard_id)),
        )
        .unwrap()
        .metadata_prefix_proof()
        .unwrap();
        if materialized == expected_prefix {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the owned replay must materialize the exact prefix within the existing edge budget"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let cache_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .expect("actual root cache slot");
    let cache = cache_slot.lock().expect("message service cache lock");
    let entry = cache.as_ref().expect("cold message cache entry");
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
    drop(exact_lane);
    drop(cache);
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    native.clear_faults();
    native.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}

#[test]
fn daemon_dfs_message_processes_stage_blob_and_returns_response() {
    let (native, initial, shard) = crate::test_support::current_native_metadata_service(
        "native-message-stage-blob",
        2,
        "/services",
    )
    .expect("actual mounted DFS owners");
    let owner = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    drop(initial);
    let candidates = vec![owner.clone(), replica.clone()];
    let path = "/services/daemon-dfs-data-plane.bin";
    let mut placement =
        crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = candidates.clone();
    native
        .apply_placement_policy(&owner, path, crate::DfsPlacementScope::SelfOnly, placement)
        .expect("normal native W2/N2 placement Apply");
    let captured = Arc::new(Mutex::new(
        crate::test_runtime::StageExchangeCapture::default(),
    ));
    native.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.to_string()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::CaptureStageExchangeAndLoseAfterDispatch(
            Arc::clone(&captured),
        ),
    });
    let bytes = b"daemon dfs data-plane message bytes".to_vec();
    let content_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let mut writer = native.client(&owner).expect("normal Main writer");
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = candidates.clone();
    let pending = writer
        .posix_write_file_from_reader(
            path,
            std::io::Cursor::new(&bytes),
            DfsWriteOptions {
                policy,
                replica_candidates: candidates,
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect_err("nonwaiting create returns its routed pending identity");
    let crate::DfsPosixError::PendingWrite {
        intent_id: caller_request_id,
        ..
    } = pending
    else {
        panic!("normal W2 create must remain genuinely Pending: {pending}");
    };
    // The dispatch API returns caller-side Pending before draining its queued
    // reply. The synchronous native RPC has completed; process that SAME Main
    // response through normal validation before inspecting its receipt count.
    let served = writer
        .process_incoming()
        .expect("settle actual queued Main response");
    let outcome = writer
        .write_outcome(&caller_request_id)
        .expect("actual validated Main write outcome");
    let crate::NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("lost genuine Stage delivery must keep Main below W2: {outcome:?}");
    };
    assert!(
        served.iter().any(|event| matches!(event,
            crate::NetworkedDfsServiceEvent::RemoteWriteCompleted {
                request_id,
                primary_node_id,
                result: crate::DfsDistributedWriteResult::Outcome {
                    outcome: crate::NetworkedDfsWriteOutcome::Pending {
                        intent_id: served_intent,
                        durable_receipts: 1,
                        required_receipts: 2,
                    },
                },
                ..
            } if request_id == &caller_request_id
                && primary_node_id == &owner
                && served_intent == &intent_id
        )),
        "receipt counts must come from the actual completed Main response"
    );
    assert_eq!((durable_receipts, required_receipts), (1, 2));
    let exchange = {
        let captured = captured.lock().expect("actual Stage exchange capture");
        assert_eq!(captured.dropped_count, 0);
        assert!(
            !captured.exchanges.is_empty(),
            "normal Main dispatched its actual Stage"
        );
        captured.exchanges[0].clone()
    };
    let DfsNetworkMessage::StageBlob(stage) = &exchange.command.routed_message.message else {
        panic!("current normal writer must produce the current Stage command");
    };
    let object_id = stage.object_id.clone();
    let version = stage.version.clone();
    assert_eq!(stage.intent_id, intent_id);
    assert_eq!(stage.file_system_id, shard.file_system_id);
    assert_eq!(stage.content_hash, content_hash);
    assert_eq!(stage.authority_shard.as_ref(), Some(&shard));
    let metadata = stage
        .object_metadata
        .as_ref()
        .expect("normal create supplies its real metadata");
    assert_eq!(metadata.object_id, object_id);
    assert_eq!(
        metadata.path.trim_start_matches('/'),
        path.trim_start_matches('/')
    );
    assert_eq!(metadata.owner_node_id, owner);
    assert_eq!(exchange.source_node_id, owner);
    assert_eq!(exchange.target_node_id, replica);
    assert_eq!(
        exchange.signer.cluster_identity().unwrap().0,
        owner.as_str()
    );
    assert_eq!(exchange.request.route, crate::DFS_DATA_STAGE_BLOB_ROUTE);
    assert_eq!(exchange.body_bytes, Some(bytes.len() as u64));
    assert_eq!(
        exchange
            .attachment
            .as_ref()
            .expect("actual streamed Stage descriptor")
            .total_size_bytes,
        bytes.len() as u64
    );
    assert_eq!(exchange.response_request_id, exchange.command.request_id);
    assert_eq!(exchange.response_outbound_message_count, 1);
    assert_eq!(exchange.response.source_node_id, replica);
    assert_eq!(exchange.response.target_node_id, owner);
    assert!(matches!(
        &exchange.response.message,
        DfsNetworkMessage::StageBlobResponse(response)
            if response.request_id == stage.request_id
                && response.intent_id == intent_id
                && response.object_id == object_id
                && response.version == version
                && response.replica_node_id == replica
                && response.result == DfsBlobTransferResult::Durable { content_hash: content_hash.clone() }
    ));
    assert!(matches!(
        writer
            .write_outcome(&intent_id)
            .expect("actual source intent outcome"),
        crate::NetworkedDfsWriteOutcome::Pending {
            durable_receipts: 1,
            required_receipts: 2,
            ..
        }
    ));
    let data_dir = native.data_dir(&replica);
    let cluster_paths = ClusterPaths::new(data_dir);
    let store = LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
    let manifest = store
        .read_manifest(object_id.as_str(), version.as_str())
        .expect("read staged manifest");
    assert_eq!(manifest.content_hash, content_hash);
    assert_eq!(
        store.read_blob(&manifest.content_hash).expect("read blob"),
        bytes
    );
    let backend = sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(data_dir, &cluster_paths)
        .expect("open sharded backend")
        .expect("configured backend");
    assert!(
        backend
            .get_object(&object_id)
            .expect("backend object")
            .is_none(),
        "non-primary content replicas must not append authoritative metadata while handling DFS messages"
    );
    assert!(
        writer
            .metadata_backend()
            .unwrap()
            .get_object(&object_id)
            .unwrap()
            .is_none(),
        "physical Stage durability alone cannot publish the file below W2"
    );
    drop(backend);
    drop(store);
    drop(writer);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_message_rejects_wrong_target_node() {
    let (native, nodes) =
        crate::test_support::DfsTestRuntime::start_cluster("native-message-wrong-target", 1)
            .unwrap();
    let local = &nodes[0];
    let data_dir = native.data_dir(local);
    let context = native.component_context(local).unwrap();
    let remote = NodeId::from_legacy_named("node_dfs_protocol_wrong_target").unwrap();
    let error = handle_daemon_dfs_message_on_runtime(
        &DaemonPaths::new(&data_dir),
        test_dfs_message_command(
            "dfs_message_rpc_wrong_target",
            test_dfs_lookup_routed_message(local, &remote, 1, "/services/daemon-wrong-target.txt"),
        ),
        context.runtime().unwrap(),
    )
    .expect_err("wrong target rejected by actual registered Main");
    assert!(error.to_string().contains("does not match local node"));
    drop(context);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_distributed_write_skips_cold_full_metadata_hydrate() {
    let (native, mut source, root_shard) = crate::test_support::current_native_metadata_service(
        "native-cold-write-lane",
        3,
        "/services",
    )
    .unwrap();
    let local = root_shard.primary_node_id.clone();
    let remote = root_shard.replica_node_ids[0].clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let paths = DaemonPaths::new(&data_dir);
    let file_system = native.file_system();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![local.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let unrelated_object_id = source
        .posix_write_file(
            "/unrelated-cold-metadata",
            b"genuine unrelated bytes",
            options.clone(),
        )
        .unwrap()
        .object_id;
    source
        .posix_write_file(
            "/services/daemon-write-skips-cold-hydrate.txt",
            b"prior current head",
            options.clone(),
        )
        .unwrap();
    source
        .posix_write_file(
            "/services/daemon-write-after-maintenance.txt",
            b"prior second head",
            options.clone(),
        )
        .unwrap();
    source
        .posix_write_file(
            "/services/daemon-write-after-failed-maintenance.txt",
            b"prior third head",
            options,
        )
        .unwrap();
    let planned_object = |path: &str| {
        source
            .metadata_backend()
            .unwrap()
            .get_object_by_path_on_shard(
                &DfsMetadataShardId::new(root_shard.shard_id.clone()),
                &file_system.file_system_id,
                path,
            )
            .unwrap()
            .unwrap()
    };
    let first_object = planned_object("/services/daemon-write-skips-cold-hydrate.txt");
    let second_object = planned_object("/services/daemon-write-after-maintenance.txt");
    let third_object = planned_object("/services/daemon-write-after-failed-maintenance.txt");
    drop(source);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
    let lane_opens_before_write = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let runtime = daemon_test_runtime("native-cold-write-lane-observer");
    let cache_key = data_dir.root();
    let invoke_native = |command: crate::DfsMessageCommand| {
        let response = native.rpc_call(
            &remote,
            &local,
            crate::dfs_message_rpc_call(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )?;
        crate::decode_dfs_message_rpc_response(&response).map_err(|error| error.to_string())
    };
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
    write.requester_blob_pre_staged = false;
    write.authority_shard = Some(root_shard.clone());
    write.request.object_id = first_object.object_id.clone();
    write.expected_base_version = Some(first_object.version.clone());
    write.object_metadata = Some(first_object.clone());
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    let response = invoke_native(test_dfs_message_command(
        "dfs_message_write_skips_cold_hydrate",
        routed_message,
    ))
    .expect("distributed write DFS message");
    assert_eq!(response.request_id, "dfs_message_write_skips_cold_hydrate");
    let written_object_id = first_object.object_id.clone();
    let persisted = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new(root_shard.shard_id.clone()),
        crate::default_dfs_metadata_shards_root(&data_dir)
            .join(crate::metadata_keys::key_segment(&root_shard.shard_id)),
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        Arc::clone(
            cache
                .as_ref()
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
    let cache_slot = daemon_dfs_message_service_cache()
        .root(&cache_key)
        .expect("actual root cache slot");
    let cache = cache_slot.lock().expect("cache lock");
    assert!(Arc::ptr_eq(
        &primary_service,
        cache
            .as_ref()
            .expect("cache entry")
            .primary_service
            .as_ref()
            .expect("mutation service lane")
    ));
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_opens_before_write + 1,
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
                shard.shard_id == DfsMetadataShardId::new(root_shard.shard_id.clone())
                    && shard.file_system_id.as_ref() == Some(&file_system.file_system_id)
                    && shard.primary_node_id.as_ref() == Some(&local)
            }),
            "bounded status must expose the already-resident root shard topology",
        );
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_opens_before_write + 1,
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let mut cache = cache_slot.lock().expect("cache lock");
        let detached = cache
            .as_mut()
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let mut cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_mut().expect("cache entry");
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
        .root(&cache_key)
        .expect("actual root cache slot")
        .lock()
        .expect("cache lock")
        .as_ref()
        .expect("cache entry")
        .resident_metadata_backend
        .clone()
        .expect("resident metadata generation");
    let root_handle = resident
        .scope_shard(
            &file_system.file_system_id,
            &DfsMetadataShardId::new(root_shard.shard_id.clone()),
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

    let cache_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .expect("actual root cache slot");
    let cache_guard = cache_slot.lock().expect("hold cache mutation ownership");
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let mut cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_mut().expect("cache entry");
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let mut cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_mut().expect("cache entry");
        entry.resident_metadata_backend = resident_before_cold_status;
        entry.resident_metadata_initialized = true;
    }

    invoke_native(test_dfs_message_command(
        "dfs_message_read_survives_maintenance",
        test_dfs_lookup_routed_message(
            &remote,
            &local,
            2,
            "/services/daemon-write-skips-cold-hydrate.txt",
        ),
    ))
    .expect("lightweight lookup DFS message");
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        assert!(Arc::ptr_eq(
            &primary_service,
            cache
                .as_ref()
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let lane = cache
            .as_ref()
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
    write.requester_blob_pre_staged = false;
    write.authority_shard = Some(root_shard.clone());
    write.request.object_id = second_object.object_id.clone();
    write.expected_base_version = Some(second_object.version.clone());
    write.object_metadata = Some(second_object.clone());
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    invoke_native(test_dfs_message_command(
        "dfs_message_write_after_maintenance",
        second_write,
    ))
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
    // Corrupt the real queue after outer scheduling has observed it, at the
    // existing admitted maintenance entry. This exercises failure while the
    // body owns its lane, rather than an earlier preflight rejection.
    let corrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let triggered = Arc::clone(&corrupted);
    let corrupt_path = repair_snapshot.clone();
    let failure = install_daemon_dfs_pre_dispatch_test_hook(
        &data_dir,
        Arc::new(move |point, command, _| {
            if matches!(
                point,
                DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            ) && command.is_none()
                && !triggered.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                fs::write(&corrupt_path, b"invalid repair snapshot")
                    .expect("corrupt actual repair snapshot");
            }
            Ok(())
        }),
    );
    let error = run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
        .expect_err("corrupt repair state must fail maintenance");
    drop(failure);
    assert!(corrupted.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        error.to_string().contains("repair"),
        "unexpected maintenance error: {error}"
    );
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        assert!(
            cache
                .as_ref()
                .expect("cache entry")
                .primary_service
                .is_some(),
            "failed maintenance must return its unchanged owner/storage lane without discarding live custody"
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
    write.requester_blob_pre_staged = false;
    write.authority_shard = Some(root_shard.clone());
    write.request.object_id = third_object.object_id.clone();
    write.expected_base_version = Some(third_object.version.clone());
    write.object_metadata = Some(third_object.clone());
    write
        .object_metadata
        .as_mut()
        .expect("write object metadata")
        .current_locations = vec![local.clone()];
    invoke_native(test_dfs_message_command(
        "dfs_message_write_after_failed_maintenance",
        third_write,
    ))
    .expect("distributed write after failed maintenance");
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        lane_open_count_before_maintenance,
        "the first mutation after failed maintenance must reuse the checked returned lane"
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
    let added_peer = remote.clone();
    let mut changed_state = current_state;
    let member = changed_state
        .members
        .iter_mut()
        .find(|member| member.node_id == added_peer)
        .unwrap();
    let before_endpoints = member.endpoints.clone();
    if let Some(endpoint) = member.endpoints.first_mut() {
        endpoint.priority = endpoint.priority.saturating_add(1);
    } else {
        // The native component carrier has no socket endpoint. Change only
        // routing DATA to a deliberately unusable address, never live health.
        member
            .endpoints
            .push(sinomo_core::network::daemon_lan_endpoint(
                &added_peer,
                "127.0.0.1:9".to_string(),
                5,
            ));
    }
    assert_ne!(member.endpoints, before_endpoints);
    let control = native.shared_control_plane().unwrap();
    control
        .with_client(|kv| {
            kv.put(
                format!("/cluster/members/{added_peer}"),
                sinomo_core::kv::encode_kv_value(member).unwrap(),
            )
        })
        .unwrap();
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &changed_state).unwrap();
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_ref().expect("cache entry");
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
        lane_open_count_before_maintenance,
        "peer-only maintenance refresh must retain the open metadata backend"
    );

    let current_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("current cluster state");
    let mut retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &current_state,
        runtime.handle(),
        None,
    )
    .expect("take mutation lane for authority-change validation");
    assert!(
        retained
            .lane
            .service
            .can_release_hydrated_metadata_projection()
    );
    // Retain the old service/signature for the handback check, but release our
    // own resident checkout before the actual handoff needs the same store.
    retained
        .lane
        .service
        .release_resident_metadata_backend()
        .unwrap();

    let changed_root_shard = native
        .reassign_metadata_shard(
            &root_shard.file_system_id,
            &root_shard.shard_id,
            remote.clone(),
            std::iter::once(local.clone())
                .chain(
                    root_shard
                        .replica_node_ids
                        .iter()
                        .filter(|node| *node != &remote)
                        .cloned(),
                )
                .collect(),
        )
        .unwrap();
    assert_eq!(changed_root_shard.epoch, root_shard.epoch + 1);
    // The real handoff serves work and installs its current lane. A delayed
    // old handback must refuse reuse without removing or replacing that lane.
    let (current_signature, current_lane) = {
        let slot = daemon_dfs_message_service_cache().root(&cache_key).unwrap();
        let cache = slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert_ne!(
            entry.signature.metadata_storage_generation,
            retained.signature.metadata_storage_generation
        );
        (
            entry.signature.clone(),
            Arc::clone(
                entry
                    .primary_service
                    .as_ref()
                    .expect("actual handoff installed current lane"),
            ),
        )
    };
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
    {
        let slot = daemon_dfs_message_service_cache().root(&cache_key).unwrap();
        let cache = slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert_eq!(entry.signature, current_signature);
        assert!(
            Arc::ptr_eq(entry.primary_service.as_ref().unwrap(), &current_lane),
            "rejected old authority cannot replace the actual handoff's current lane"
        );
    }
    drop(current_lane);
    assert!(
        evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
        "shutdown must release the complete message cache"
    );
    drop(control);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_lookup_message_reuses_resident_service_without_full_hydrate() {
    let name = "native-message-warm-cache";
    let (native, mut source, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services")
            .expect("actual mounted DFS owners");
    let local = shard.primary_node_id.clone();
    let remote = shard.replica_node_ids[0].clone();
    let data_dir = native.data_dir(&local).clone();
    let unrelated = source
        .posix_write_file(
            "/services/warm-cache-unrelated.txt",
            b"actual metadata that the lookup lane must not fully hydrate",
            DfsWriteOptions::replicated_eventual(vec![local.clone()]),
        )
        .expect("genuine unrelated metadata row");
    drop(source);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
    let opens_before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let lookup = |request_id: &str, frame_id, path: &str| {
        let command = test_dfs_message_command(
            request_id,
            test_dfs_lookup_routed_message(&remote, &local, frame_id, path),
        );
        let response = native
            .rpc_request(
                &remote,
                &local,
                crate::dfs_message_rpc_request(&command).unwrap(),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .expect("actual authenticated Main lookup");
        crate::decode_dfs_message_rpc_response(&response).unwrap()
    };
    let response = lookup("dfs_message_warm_cache_1", 1, "/services/warm-cache-1.txt");
    assert_eq!(response.request_id, "dfs_message_warm_cache_1");
    let cache_key = data_dir.root();
    let read_service = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_ref().expect("cache entry");
        Arc::clone(
            entry
                .primary_service
                .as_ref()
                .expect("lookup opens the resident metadata lane"),
        )
    };
    let response = lookup("dfs_message_warm_cache_2", 2, "/services/warm-cache-2.txt");
    assert_eq!(response.request_id, "dfs_message_warm_cache_2");

    // Change routing DATA for an actual committed member. Identity, role and
    // capabilities remain its normal native values, and no health is seeded.
    let control = native.shared_control_plane().unwrap();
    let member_key = sinomo_core::node::cluster_member_key(&remote);
    let current_member = control
        .with_client(|kv| kv.get(&member_key))
        .unwrap()
        .expect("actual committed member")
        .decode::<sinomo_core::ClusterMemberRecord>()
        .unwrap();
    let mut changed_member = current_member.clone();
    if let Some(endpoint) = changed_member.endpoints.first_mut() {
        endpoint.priority = endpoint.priority.saturating_add(1);
        endpoint.last_observed_latency_ms = None;
    } else {
        changed_member
            .endpoints
            .push(sinomo_core::network::daemon_lan_endpoint(
                &remote,
                "127.0.0.1:9".to_string(),
                5,
            ));
    }
    assert_ne!(changed_member.endpoints, current_member.endpoints);
    let expected_value = sinomo_core::kv::encode_kv_value(&changed_member).unwrap();
    let observed_endpoint = Arc::new(Mutex::new(None::<sinomo_core::kv::KvEntry>));
    let captured_endpoint = Arc::clone(&observed_endpoint);
    let expected_key = member_key.clone();
    let expected_endpoint_value = expected_value.clone();
    // Observe the actual native producer's ordered applied commit. The source
    // member publisher may subsequently refresh its real empty endpoint list;
    // that later valid commit cannot overwrite this exact earlier observation.
    let producer = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2).remove(0);
    let producer_paths = ClusterPaths::new(native.data_dir(&producer));
    let endpoint_subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &producer_paths,
        move |observation| {
            if observation.applied_index.is_none() {
                return;
            }
            let sinomo_core::kv::KvCommittedObservationValue::Delta(delta) = &observation.value
            else {
                return;
            };
            if let Some(entry) = delta
                .final_key_states
                .iter()
                .filter(|state| state.key == expected_key)
                .filter_map(|state| state.entry.as_ref())
                .find(|entry| entry.value == expected_endpoint_value)
            {
                captured_endpoint
                    .lock()
                    .unwrap()
                    .get_or_insert_with(|| entry.clone());
            }
        },
    );
    control
        .with_client(|kv| kv.put(member_key.clone(), expected_value))
        .expect("normal native endpoint configuration commit");
    let response = lookup("dfs_message_warm_cache_3", 3, "/services/warm-cache-3.txt");
    assert_eq!(response.request_id, "dfs_message_warm_cache_3");
    let second_read_service = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_ref().expect("cache entry");
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
    assert!(
        read_service
            .lock()
            .unwrap()
            .service
            .metadata()
            .object(&unrelated.object_id)
            .is_none(),
        "lookups must retain the thin lane rather than fully hydrate unrelated metadata"
    );
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir) - opens_before,
        1,
        "metadata reads should reuse one resident backend generation"
    );
    drop(endpoint_subscription);
    assert_eq!(
        observed_endpoint
            .lock()
            .unwrap()
            .take()
            .expect("actual endpoint configuration applied before source republishing")
            .decode::<sinomo_core::ClusterMemberRecord>()
            .unwrap(),
        changed_member,
        "the endpoint refresh must actually commit its complete member DATA"
    );
    drop(control);
    drop(read_service);
    drop(second_read_service);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_peer_membership_refresh_reuses_resident_lane() {
    let name = "native-message-peer-refresh";
    let (mut native, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/services")
            .expect("actual mounted DFS owners");
    let local = shard.primary_node_id.clone();
    let remote = shard.replica_node_ids[0].clone();
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let added = nodes
        .iter()
        .find(|node| **node != local && **node != remote)
        .unwrap()
        .clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let member_key = sinomo_core::node::cluster_member_key(&added);
    let wal_paths = cluster_paths.kv_paths().raft_wal_paths(&local);
    let actual_member = || {
        let read = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
            &wal_paths,
            &local,
            Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |projection| {
                projection.read_index_prefix_limited_by_payload_bytes(
                    "/cluster/members/",
                    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                )
            },
        )
        .expect("bounded actual committed membership projection")
        .expect("actual resident membership read");
        read.value
            .unwrap()
            .into_iter()
            .find(|entry| entry.key == member_key)
            .expect("actual precommitted member")
    };
    let runtime = native.runtime_handle();
    let actual_peers = || {
        runtime
            .block_on(read_daemon_dfs_message_peer_snapshot_before(
                &cluster_paths,
                &runtime,
                tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            ))
            .unwrap()
            .unwrap()
    };
    let original_member = actual_member();
    let initial_peers = actual_peers();
    assert!(initial_peers.peer_node_ids.contains(&added));
    drop(initial);
    native
        .set_node_dfs_component_enabled(&added, false)
        .expect("normal component withdrawal and catalog removal");
    let withdrawn = actual_peers();
    assert!(withdrawn.peer_node_ids.contains(&local));
    assert!(withdrawn.peer_node_ids.contains(&remote));
    assert!(!withdrawn.peer_node_ids.contains(&added));
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
    let opens_before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let lookup =
        |native: &crate::test_runtime::DfsTestRuntime, request_id: &str, frame_id, path: &str| {
            let command = test_dfs_message_command(
                request_id,
                test_dfs_lookup_routed_message(&remote, &local, frame_id, path),
            );
            let response = native
                .rpc_request(
                    &remote,
                    &local,
                    crate::dfs_message_rpc_request(&command).unwrap(),
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                )
                .expect("actual authenticated Main lookup");
            let response = crate::decode_dfs_message_rpc_response(&response).unwrap();
            assert_eq!(response.request_id, request_id);
        };
    lookup(
        &native,
        "dfs_message_peer_refresh_1",
        1,
        "/services/peer-refresh-1.txt",
    );
    let cache_key = data_dir.root();
    let read_service = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let service = Arc::clone(
            cache
                .as_ref()
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("resident metadata lane"),
        );
        let lane = service.lock().unwrap();
        assert!(lane.peer_node_ids.contains(&remote));
        assert!(!lane.peer_node_ids.contains(&added));
        drop(lane);
        service
    };
    // The member already belongs to the immutable committed roster. Only the
    // genuine normal component lifecycle adds its current DFS provider offer.
    native
        .set_node_dfs_component_enabled(&added, true)
        .expect("normal DFS remount and actual catalog provider addition");
    lookup(
        &native,
        "dfs_message_peer_refresh_2",
        2,
        "/services/peer-refresh-2.txt",
    );
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("cache lock");
        let entry = cache.as_ref().expect("cache entry");
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
    let current_member = actual_member();
    assert_eq!(
        current_member.value, original_member.value,
        "catalog provider addition must preserve committed member identity, role and capabilities"
    );
    let current_peers = actual_peers();
    assert!(current_peers.peer_node_ids.contains(&added));
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir) - opens_before,
        1,
        "peer membership changes must keep the one open metadata backend"
    );
    assert!(
        evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
        "test cleanup must release the cached lane"
    );
    drop(read_service);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_resident_mutation_lanes_are_scoped_by_exact_shard() {
    let local = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-resident-exact-shard-lanes",
        1,
    )
    .remove(0);
    let (native, client) = crate::test_runtime::DfsTestRuntime::start_single_node(
        "native-resident-exact-shard-lanes",
        local.clone(),
    )
    .unwrap();
    drop(client);
    let context = native.component_context(&local).unwrap();
    let runtime = context
        .runtime()
        .expect("actual mounted Main runtime")
        .clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let root_shard = native.root_shard().unwrap();
    let services_shard = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    assert_eq!(root_shard.primary_node_id, local);
    assert_eq!(services_shard.primary_node_id, local);
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .unwrap()
        .expect("actual native peer projection");
    let warm = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &cluster_paths,
            &peer_snapshot,
            &runtime,
        ))
        .unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .expect("normal global generation admission");
    let preflight = warm.for_admission(&admission);
    let root_key = DaemonDfsMutationLaneKey {
        file_system_id: root_shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(root_shard.shard_id.clone()),
    };
    let services_key = DaemonDfsMutationLaneKey {
        file_system_id: services_shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(services_shard.shard_id.clone()),
    };
    let (had_root, had_services, prior_global, prior_exact_count) = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        let entry = cache.as_ref().expect("normal Main resident generation");
        (
            entry.exact_shard_services.contains_key(&root_key),
            entry.exact_shard_services.contains_key(&services_key),
            entry.primary_service.clone(),
            entry.exact_shard_services.len(),
        )
    };
    let opens_before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let select = |kind| {
        let rollover = matches!(&kind, DaemonDfsMessageLaneKind::MutationGlobal);
        daemon_dfs_message_lane(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &runtime,
            kind,
            rollover,
            Some(&preflight),
        )
        .map(|(lane, _)| lane)
        .expect("normal admitted resident lane selection")
    };
    let root_lane = select(DaemonDfsMessageLaneKind::MutationExact(root_key.clone()));
    let same_root_lane = select(DaemonDfsMessageLaneKind::MutationExact(root_key.clone()));
    let services_lane = select(DaemonDfsMessageLaneKind::MutationExact(services_key));
    let added_exact = usize::from(!had_root) + usize::from(!had_services);
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert_eq!(
            entry.exact_shard_services.len(),
            prior_exact_count + added_exact
        );
        match (&prior_global, &entry.primary_service) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("exact selection changed the all-shard service"),
        }
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        opens_before + added_exact,
        "exact selection opens only absent request shells"
    );
    let global_lane = select(DaemonDfsMessageLaneKind::MutationGlobal);
    assert!(Arc::ptr_eq(&root_lane, &same_root_lane));
    assert!(!Arc::ptr_eq(&root_lane, &services_lane));
    assert!(!Arc::ptr_eq(&root_lane, &global_lane) && !Arc::ptr_eq(&services_lane, &global_lane));
    let root_guard = root_lane.lock().unwrap();
    assert!(
        same_root_lane.try_lock().is_err(),
        "same-shard request state must serialize"
    );
    let services_guard = services_lane
        .try_lock()
        .expect("disjoint shard remains independently lockable");
    let global_guard = global_lane
        .try_lock()
        .expect("global request mutex remains distinct");
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
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        opens_before + added_exact + usize::from(prior_global.is_none())
    );
    assert!(
        daemon_dfs_message_cache_exact_admission_is_current(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &root_key,
            &runtime,
            #[cfg(test)]
            None,
        )
        .unwrap()
    );
    drop(admission);
    native
        .bootstrap_empty_metadata_prefix("exact-lane-extra", "/exact-lane-extra")
        .expect("actual normal native genesis and assignment commit");
    let exact_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: root_key.file_system_id.clone(),
        shard_id: root_key.shard_id.clone(),
    };
    let exact_admission = runtime.block_on(scheduler.admit(&exact_scope)).unwrap();
    let old_preflight = preflight.for_admission(&exact_admission);
    assert_eq!(old_preflight.generation, preflight.generation);
    assert!(
        matches!(
            try_daemon_dfs_resident_metadata_backend_after_preflight(
                &data_dir,
                &cluster_paths,
                &peer_snapshot,
                &old_preflight,
                &runtime,
            ),
            Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(_))
        ),
        "actual topology advancement must reject the earlier exact preflight before backend selection"
    );
    drop(exact_admission);
    drop((
        root_lane,
        same_root_lane,
        services_lane,
        global_lane,
        prior_global,
    ));
    drop(context);
    native.close().unwrap();
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
    let (native, source, root_shard) =
        late_native_replication_fixture("native-stamped-lookup-lanes");
    let local = root_shard.primary_node_id.clone();
    let remote = root_shard.replica_node_ids[0].clone();
    let file_system = native.file_system();
    let users_shard = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "users")
        .unwrap();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let paths = DaemonPaths::new(&data_dir);
    let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir)
        .join(crate::metadata_keys::key_segment(&root_shard.shard_id));
    let journal_sequence_before = source
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .find(|status| status.shard_id.0 == root_shard.shard_id)
        .unwrap()
        .journal_last_sequence;
    let refreshes_before_cold = {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        cache
            .as_ref()
            .unwrap()
            .metadata_authority_generation
            .authority_refresh_count()
    };
    drop(source);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
    let stamped_lookup = |frame_id: u64, stamp: DfsMetadataShardConfig| {
        let mut routed = test_dfs_lookup_routed_message(&remote, &local, frame_id, "/");
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
                root_path: "/users".to_string(),
                cursor: sinomo_dfs::DfsNamespaceChangeCursor::from_shard_config(&stamp, 0),
            },
        };
        routed
    };
    let runtime = daemon_test_runtime("daemon-dfs-stamped-lookup-lane-safety");
    // Startup has already established a genuine complete authority projection.
    // Clear only observation counters; a cold message lane must reuse it.
    take_daemon_dfs_test_authoritative_exact_set_call_count(&cluster_paths);
    take_daemon_dfs_test_authoritative_prefix_call_count(&cluster_paths);
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("message cache");
        let entry = cache.as_ref().expect("cache entry");
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
    assert!(
        cold_primary_service_is_some,
        "cold lookup retained no global primary service: exact lanes empty={cold_exact_shard_services_are_empty}, authority refreshes={authority_refreshes_after_cold}, projection materializations={authority_materializations_after_cold}",
    );
    assert_eq!(
        authority_refreshes_after_cold, refreshes_before_cold,
        "cold message admission must reuse the actual mount authority proof",
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
        0,
        "the normal mount already proved the complete scopes; cold read adds no observer KV work",
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
            #[cfg(test)]
            None,
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
            #[cfg(test)]
            None,
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("message cache");
        let entry = cache.as_ref().expect("cache entry");
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("message cache");
        let entry = cache.as_ref().expect("cache entry");
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("message cache");
        let entry = cache.as_ref().expect("cache entry");
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
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().expect("message cache");
        let entry = cache.as_ref().expect("cache entry");
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

    let new_users_primary = if users_shard.primary_node_id == remote {
        local.clone()
    } else {
        remote.clone()
    };
    let members = std::iter::once(users_shard.primary_node_id.clone())
        .chain(users_shard.replica_node_ids.clone())
        .filter(|node| node != &new_users_primary)
        .collect();
    let users_shard = native
        .reassign_metadata_shard(
            &file_system.file_system_id,
            &users_shard.shard_id,
            new_users_primary.clone(),
            members,
        )
        .unwrap();
    let cold_poll_paths = DaemonPaths::new(native.data_dir(&new_users_primary));
    assert!(evict_daemon_dfs_message_service_cache(native.data_dir(&new_users_primary)).unwrap());
    let data_dir = native.data_dir(&new_users_primary).clone();
    let lane_opens_before_cold_poll = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let exact_lanes_before_cold_poll = 0usize;
    let cold_poll_result = runtime.block_on(handle_daemon_dfs_message_async(
        &cold_poll_paths,
        test_dfs_message_command("dfs_namespace_poll_cold_nonresident", {
            let mut message = stamped_poll(50, users_shard);
            message.target_node_id = new_users_primary.clone();
            message
        }),
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
    let cache_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .expect("actual root cache slot");
    let cache = cache_slot.lock().expect("message cache");
    // A correctly rejected cold poll may leave the cache entry absent.
    // That is an observed zero-lane state, not unavailable lane-count evidence.
    let exact_lanes_after_cold_poll = cache
        .as_ref()
        .map_or(0, |entry| entry.exact_shard_services.len());
    let has_users_lane = cache.as_ref().is_some_and(|entry| {
        entry.exact_shard_services.keys().any(|key| {
            key.file_system_id == file_system.file_system_id && key.shard_id.0 == "users"
        })
    });
    drop(cache);
    if cold_poll_rejected {
        assert_eq!(
            lane_opens_after_cold_poll, lane_opens_before_cold_poll,
            "a cold namespace poll must fail before opening any service/backend lane"
        );
        // The authority reactor can invalidate an older cached generation
        // after the append above. Evicting its lanes is valid; constructing a
        // new lane for this rejected cold request is not. The monotonic open
        // counter checked above also detects replacement by a new lane.
        assert!(
            exact_lanes_after_cold_poll <= exact_lanes_before_cold_poll,
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
        assert!(
            has_users_lane
                && exact_lanes_after_cold_poll <= exact_lanes_before_cold_poll.saturating_add(1),
            "the new users lane must exist; prior-generation lanes may be evicted"
        );
    }

    let data_dir = native.data_dir(&local).clone();
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
    drop(reopened);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_content_store_lookup_does_not_open_service_lane() {
    let (native, nodes) =
        crate::test_support::DfsTestRuntime::start_cluster("native-content-store-lookup", 1)
            .unwrap();
    let data_dir = native.data_dir(&nodes[0]);
    let context = native.component_context(&nodes[0]).unwrap();
    let host = DaemonDfsDataPlaneHost::new_on_runtime(
        DaemonPaths::new(&data_dir),
        context.runtime().unwrap(),
    );
    // Normal Main startup already owns metadata lanes. A content-only lookup
    // must reuse that owner without adding another metadata-bearing lane.
    let before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    assert!(host.shared_content_store(&data_dir).unwrap().is_some());
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        before,
        "content-store lookup must not instantiate another DFS service lane",
    );
    drop(host);
    drop(context);
    native.close().unwrap();
}

#[test]
fn daemon_dfs_exact_read_lane_does_not_open_metadata_backend() {
    let (native, nodes) =
        crate::test_support::DfsTestRuntime::start_cluster("native-exact-content-read-lane", 1)
            .unwrap();
    let local = &nodes[0];
    let data_dir = native.data_dir(local);
    let context = native.component_context(local).unwrap();
    let before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    let response = handle_daemon_dfs_message_on_runtime(
        &DaemonPaths::new(&data_dir),
        test_dfs_message_command(
            "dfs_message_exact_content_read",
            test_dfs_exact_read_routed_message(local, local, 1),
        ),
        context.runtime().unwrap(),
    )
    .expect("actual Main exact immutable read of absent content");
    assert_eq!(response.request_id, "dfs_message_exact_content_read");
    let cache_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .expect("actual root cache slot");
    let cache = cache_slot.lock().unwrap();
    assert!(cache.as_ref().unwrap().content_read_service.is_some());
    drop(cache);
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        before,
        "exact immutable reads must not add a metadata-bearing service lane",
    );
    drop(context);
    native.close().unwrap();
}

#[test]
fn daemon_owned_dfs_clients_reuse_and_track_shared_metadata_authority_generation() {
    let nodes =
        crate::test_support::DfsTestRuntime::ordered_node_ids("native-shared-client-generation", 2);
    let mut native = crate::test_support::DfsTestRuntime::new(
        "native-shared-client-generation",
        nodes[0].clone(),
    )
    .unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let initial = native.root_shard().unwrap();
    let local = initial.primary_node_id.clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).unwrap();
    let runtime = native.runtime_handle();
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &local,
        Some(&runtime),
    )
    .unwrap();
    // Retain and warm the actual shared generation before measuring warm
    // clients. Merely obtaining its weak-registry handle is not a ready cut.
    let ready = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &state.cluster_id,
            &local,
            &runtime,
        ))
        .unwrap();
    assert!(authority.current_complete_prefix().is_some());
    let key = metadata_shard_key(&initial.file_system_id, &initial.shard_id);
    let read = |client: &dyn crate::DfsControlPlaneKvClient| {
        client
            .get(&key)
            .unwrap()
            .unwrap()
            .decode::<DfsMetadataShardConfig>()
            .unwrap()
    };
    // An explicit authority client still performs its real exact-set read;
    // the daemon-client constructor is the retained-observation contract below.
    let explicit = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, &runtime);
    assert_eq!(read(&explicit), initial);
    assert_eq!(
        explicit
            .authoritative_exact_set_calls
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        explicit.authoritative_prefix_calls.load(Ordering::Relaxed),
        0
    );
    drop(explicit);
    for _ in 0..8 {
        let client =
            DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(&cluster_paths, &runtime);
        assert_eq!(read(&client), initial);
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::Relaxed),
            0
        );
        assert_eq!(client.authoritative_prefix_calls.load(Ordering::Relaxed), 0);
        let browser =
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime)).unwrap();
        assert_eq!(read(browser.as_ref()), initial);
        let shell = native.client(&local).unwrap();
        assert_eq!(read(shell.control_plane.as_deref().unwrap()), initial);
        drop(shell);
    }
    let cached =
        DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(&cluster_paths, &runtime);
    assert_eq!(read(&cached), initial);
    let next_primary = initial.replica_node_ids[0].clone();
    let activated = native
        .reassign_metadata_shard(
            &initial.file_system_id,
            &initial.shard_id,
            next_primary,
            vec![initial.primary_node_id.clone()],
        )
        .unwrap();
    assert!(activated.epoch > initial.epoch);
    assert_eq!(
        read(&cached),
        activated,
        "the same normal client tracks actual joint activation"
    );
    let topology = authority.generation();
    let projection = authority.projection_generation();
    let observation_key = format!(
        "/dfs/filesystems/{}/client-observation",
        initial.file_system_id
    );
    native
        .shared_control_plane()
        .unwrap()
        .with_client(|kv| {
            kv.txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![sinomo_core::kv::KvOp::Put {
                    key: observation_key.clone(),
                    value: b"visible".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
        })
        .unwrap();
    assert_eq!(
        crate::DfsControlPlaneKvClient::get(&cached, &observation_key)
            .unwrap()
            .unwrap()
            .value,
        b"visible"
    );
    assert_eq!(
        authority.generation(),
        topology,
        "an actual observation row changes no physical topology"
    );
    assert!(authority.projection_generation() > projection);
    assert_eq!(
        cached.authoritative_exact_set_calls.load(Ordering::Relaxed),
        0
    );
    assert_eq!(cached.authoritative_prefix_calls.load(Ordering::Relaxed), 0);
    drop(cached);
    drop(ready);
    drop(authority);
    native.close().unwrap();
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

fn daemon_dfs_unified_observation_withdraws_admission(
    snapshot_replacement: bool,
    future_revision: bool,
    native_gap: bool,
) {
    let name = if native_gap {
        "daemon-dfs-unified-native-gap"
    } else {
        match (snapshot_replacement, future_revision) {
            (true, false) => "daemon-dfs-unified-equal-revision-replacement",
            (false, false) => "daemon-dfs-unified-unknown-gap",
            (true, true) => "daemon-dfs-unified-future-replacement",
            (false, true) => "daemon-dfs-unified-future-prestate-gap",
        }
    };
    let root = temp_root(name);
    let data_root = root.join("data");
    let data_dir = SinomoDataDir::new(&data_root);
    data_dir.ensure_root_layout().unwrap();
    let paths = ClusterPaths::new(&data_dir);
    let local = NodeId::from_legacy_named(format!("{name}-local")).unwrap();
    write_test_cluster_state(&data_root, &local);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let file_system = distributed_sifs(local.clone());
    let shard = DfsMetadataShardConfig {
        file_system_id: file_system.file_system_id.clone(),
        shard_id: "root".into(),
        primary_node_id: local.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let key = metadata_shard_key(&file_system.file_system_id, "root");
    let initial = append_cluster_raft_command(
        &paths,
        &local,
        sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![
                sinomo_core::kv::KvOp::put_serde(
                    file_system_config_key(&file_system.file_system_id),
                    &DfsFileSystemConfig::from(&file_system),
                    None,
                )
                .unwrap(),
                sinomo_core::kv::KvOp::put_serde(key.clone(), &shard, None).unwrap(),
            ],
            failure: Vec::new(),
        }),
    )
    .unwrap();
    let mut materialized = initial.clone();
    if future_revision && !snapshot_replacement {
        materialized.revision = KvRevision(99);
    }
    sinomo_core::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &materialized).unwrap();
    let runtime = daemon_test_runtime(name);
    // Register the production weak subscription before attaching a controlled
    // metadata-only read host. The signal must travel through the real ordered
    // source and daemon adapter, rather than calling observe_discontinuity.
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &paths,
        &state.cluster_id,
        &local,
        None,
    )
    .unwrap();
    assert!(
        authority
            .install_authoritative_projection(test_dfs_authoritative_prefix_projection(&initial))
    );
    let generation = authority.generation();
    let storage_generation = authority.storage_generation();
    let (send, receive) = tokio::sync::oneshot::channel();
    let host = Arc::new(DeferredDfsAuthorityHost {
        runtime: runtime.handle().clone(),
        response: Mutex::new(Some(receive)),
        started: tokio::sync::Notify::new(),
        reads: AtomicUsize::new(0),
    });
    authority.attach_host(host.clone());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    let _subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &paths,
        move |observation| {
            observed.lock().unwrap().push((
                observation.revision,
                observation.applied_index,
                observation.kind,
                matches!(
                    observation.value,
                    sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_)
                ),
            ));
        },
    );
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
    client.retained_metadata_authority = Some(Arc::clone(&authority));
    assert_eq!(
        sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key)
            .unwrap()
            .unwrap()
            .value,
        initial.get(&key).unwrap().value
    );
    let mut terminal_applied_index = paths
        .kv_paths()
        .raft_store()
        .open_node(&local)
        .unwrap()
        .applied_index();
    if native_gap {
        let committed = paths
            .kv_paths()
            .raft_store()
            .append_command_with_response(
                &local,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Delete {
                    key: "/native/missing".into(),
                }),
            )
            .unwrap();
        terminal_applied_index = paths
            .kv_paths()
            .raft_store()
            .open_node(&local)
            .unwrap()
            .applied_index();
        let receipt = committed.observation_receipt.as_ref().unwrap();
        receipt.abandon_publication().unwrap();
        receipt.wait_delivered().unwrap();
    } else if snapshot_replacement {
        let mut replacement = initial.clone();
        if future_revision {
            replacement.revision = KvRevision(99);
        }
        let mut changed_shard = shard.clone();
        changed_shard.epoch = 99;
        replacement
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .unwrap()
            .value = sinomo_core::kv::encode_kv_value(&changed_shard).unwrap();
        sinomo_core::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &replacement).unwrap();
    } else {
        // An abandoned derived writer supplies only its materialized prestate,
        // when known, and no native applied index. Release precedes delivery.
        let source = sinomo_core::kv::try_admit_committed_observation(
            &paths.kv_paths(),
            sinomo_core::kv::KvObservationProposalOwnership::Absent,
        )
        .unwrap();
        let owner = source.storage_owner().unwrap();
        let receipt = if future_revision {
            // The actual derived prestate above is ahead of native authority.
            // Abandon its ordered receipt after the storage fence releases.
            source
                .commit(
                    materialized.revision,
                    materialized.revision,
                    None,
                    sinomo_core::kv::KvCommittedObservationKind::Replacement,
                )
                .unwrap()
        } else {
            source.commit_unknown_projection_prestate().unwrap()
        };
        receipt.abandon_publication().unwrap();
        drop(source);
        drop(owner);
        receipt.wait_delivered().unwrap();
    }
    assert_eq!(
        &*seen.lock().unwrap(),
        &[(
            if native_gap {
                Some(initial.revision)
            } else if future_revision {
                Some(KvRevision(99))
            } else {
                snapshot_replacement.then_some(initial.revision)
            },
            native_gap.then_some(terminal_applied_index),
            if native_gap {
                sinomo_core::kv::KvCommittedObservationKind::Contiguous
            } else {
                sinomo_core::kv::KvCommittedObservationKind::Replacement
            },
            !snapshot_replacement,
        )]
    );
    if !native_gap {
        assert_eq!(authority.debug_snapshot().revision, Some(initial.revision));
        assert_eq!(
            authority.debug_snapshot().required_revision,
            initial.revision
        );
        assert!(
            authority
                .retained_complete_prefix_for_generation(generation)
                .is_some()
        );
        assert_eq!(
            host.reads.load(Ordering::Acquire),
            0,
            "derived-only hints cannot create native authority work"
        );
        assert_eq!(authority.generation(), generation);
        assert_eq!(authority.storage_generation(), storage_generation);
        assert_eq!(
            sinomo_dfs::DfsControlPlaneKvClient::get(&client, &key)
                .unwrap()
                .unwrap()
                .value,
            initial.get(&key).unwrap().value
        );
        return;
    }
    assert_eq!(authority.debug_snapshot().revision, None);
    assert_eq!(
        authority.debug_snapshot().required_revision,
        initial.revision,
        "derived unknown, equal or future revisions retain the native authority floor"
    );
    assert!(
        authority
            .retained_complete_prefix_for_generation(generation)
            .is_none()
    );
    assert_eq!(authority.storage_generation(), storage_generation);
    runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(2), host.started.notified()).await
        })
        .expect("the subscription starts one existing authoritative refresh owner");
    let read_key = key.clone();
    let mut read = runtime.spawn(async move {
        sinomo_dfs::AsyncDfsControlPlaneKvClient::get_async(&client, &read_key).await
    });
    assert!(
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_millis(25), &mut read).await })
            .is_err(),
        "the adapter cannot serve the old scope or observational replacement before a fresh proof"
    );
    let (entries, revision, _) = test_dfs_authoritative_prefix_projection(&initial).into_parts();
    send.send(sinomo_core::kv::KvAuthoritativeRead {
        value: entries,
        authority: sinomo_core::kv::KvAuthorityProof {
            term: 1,
            commit_index: terminal_applied_index,
            revision,
            basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1,
            },
        },
    })
    .unwrap();
    let entry = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(2), read).await })
        .expect("the existing owner restores exact admission at the same revision")
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(entry.value, initial.get(&key).unwrap().value);
    assert_eq!(authority.debug_snapshot().revision, Some(initial.revision));
    assert_eq!(host.reads.load(Ordering::Acquire), 1);
    assert_eq!(authority.generation(), generation);
    assert_eq!(authority.storage_generation(), storage_generation);
}

#[test]
fn daemon_dfs_unified_native_gap_withdraws_until_fresh_proof() {
    daemon_dfs_unified_observation_withdraws_admission(false, false, true);
}

#[test]
fn daemon_dfs_unified_unknown_gap_withdraws_admission_until_fresh_proof() {
    daemon_dfs_unified_observation_withdraws_admission(false, false, false);
}

#[test]
fn daemon_dfs_unified_equal_revision_replacement_withdraws_admission_until_fresh_proof() {
    daemon_dfs_unified_observation_withdraws_admission(true, false, false);
}

#[test]
fn daemon_dfs_unified_future_snapshot_withdraws_without_raising_native_floor() {
    daemon_dfs_unified_observation_withdraws_admission(true, true, false);
}

#[test]
fn daemon_dfs_unified_future_prestate_gap_withdraws_without_raising_native_floor() {
    daemon_dfs_unified_observation_withdraws_admission(false, true, false);
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
fn daemon_dfs_decoded_shards_follow_proofs_without_repeated_materialization() {
    let root = temp_root("daemon-dfs-decoded-shards");
    let runtime = daemon_test_runtime("daemon-dfs-decoded-shards");
    let paths = ClusterPaths::new(&SinomoDataDir::new(root.join("data")));
    let authority = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    let mut shard = DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: "root".to_string(),
        primary_node_id: NodeId::from_legacy_named("decoded_shards_primary").unwrap(),
        replica_node_ids: Vec::new(),
        epoch: 7,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    let key = metadata_shard_key(&shard.file_system_id, &shard.shard_id);
    let install = |revision, shard: &DfsMetadataShardConfig| {
        authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            &default_dfs_reactor_test_snapshot(
                revision,
                vec![(
                    key.clone(),
                    sinomo_core::kv::encode_kv_value(shard).unwrap(),
                )],
            ),
        ));
    };
    install(1, &shard);
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
    client.retained_metadata_authority = Some(Arc::clone(&authority));
    for _ in 0..128 {
        assert_eq!(
            sinomo_dfs::DfsControlPlaneStore::new(&mut client)
                .list_metadata_shards(&shard.file_system_id)
                .unwrap(),
            vec![shard.clone()],
        );
    }
    assert_eq!(authority.projection_materialization_count(), 1);
    assert_eq!(client.authoritative_prefix_calls.load(Ordering::Acquire), 0);
    let generation = authority.generation();
    // An unrelated committed revision must not cancel an admitted recovery.
    install(2, &shard);
    assert!(
        authority
            .retained_complete_prefix_revision_for_generation(generation)
            .is_some()
    );
    authority.require_authoritative_revision(KvRevision(3));
    assert!(
        authority
            .retained_complete_prefix_revision_for_generation(generation)
            .is_none()
    );
    assert!(
        sinomo_dfs::DfsControlPlaneStore::new(&mut client)
            .list_metadata_shards(&shard.file_system_id)
            .is_err()
    );
    shard.epoch += 1;
    install(3, &shard);
    assert!(
        authority
            .retained_complete_prefix_revision_for_generation(generation)
            .is_none()
    );
    assert_eq!(
        sinomo_dfs::DfsControlPlaneStore::new(&mut client)
            .list_metadata_shards(&shard.file_system_id)
            .unwrap(),
        vec![shard.clone()]
    );
    authority.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
        &default_dfs_reactor_test_snapshot(4, vec![(key, b"invalid shard".to_vec())]),
    ));
    assert!(
        sinomo_dfs::DfsControlPlaneStore::new(&mut client)
            .list_metadata_shards(&shard.file_system_id)
            .is_err()
    );
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
            .ensure_complete_prefix_for_generation_async(
                expected_generation,
                #[cfg(test)]
                None,
            )
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
                let final_key_states: Vec<_> = events
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
                        events: events.into(),
                        final_key_states: final_key_states.into(),
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
fn daemon_dfs_snapshot_import_invalidates_changed_retained_authority() {
    let root = temp_root("dfs_snapshot_import_authority");
    let data_dir = SinomoDataDir::new(&root);
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().unwrap();
    let cluster_id = sinomo_core::Id::generate();
    let local = NodeId::generate();
    let generation =
        daemon_dfs_metadata_authority_generation_for_identity(&paths, &cluster_id, &local, None)
            .unwrap();
    let key = metadata_shard_key(&sinomo_dfs::sifs_file_system_id(), "root");
    let store = paths.kv_paths().raft_store();
    let initial = store
        .append_command_with_response_and_publish_report(
            &local,
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                key: key.clone(),
                value: b"old-shard".to_vec(),
                lease_id: None,
            }),
            |_| false,
        )
        .unwrap()
        .0
        .snapshot
        .as_ref()
        .clone();

    assert!(
        generation
            .install_authoritative_projection(test_dfs_authoritative_prefix_projection(&initial))
    );
    let old_generation = generation.generation();
    let mut unrelated = initial.clone();
    unrelated.revision = KvRevision(2);
    unrelated.entries.extend(
        default_dfs_reactor_test_snapshot(
            2,
            vec![(
                "/cluster/services/observe/test-node".into(),
                b"unrelated-service-refresh".to_vec(),
            )],
        )
        .entries,
    );
    unrelated
        .entries
        .sort_by(|left, right| left.key.cmp(&right.key));
    sinomo_core::cluster::write_imported_cluster_kv_snapshot_if_changed(&paths, &unrelated)
        .unwrap();
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(old_generation),
        Some(KvRevision(1)),
        "unrelated snapshot churn must preserve the existing DFS proof"
    );
    let replacement = default_dfs_reactor_test_snapshot(3, vec![(key, b"new-shard".to_vec())]);
    sinomo_core::cluster::write_imported_cluster_kv_snapshot_if_changed(&paths, &replacement)
        .unwrap();
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(old_generation),
        Some(initial.revision),
        "even a changed derived image cannot retire the native proof"
    );
    let committed = store
        .append_command_with_response(
            &local,
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                key: metadata_shard_key(&sinomo_dfs::sifs_file_system_id(), "root"),
                value: b"new-shard".to_vec(),
                lease_id: None,
            }),
        )
        .unwrap();
    let receipt = committed.observation_receipt.as_ref().unwrap();
    receipt.abandon_publication().unwrap();
    receipt.wait_delivered().unwrap();
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(old_generation),
        None,
        "a genuine unpublished native effect withdraws stale admission"
    );
    assert_eq!(
        generation.generation(),
        old_generation,
        "a gap cannot install replacement authority"
    );
    let replacement = committed.snapshot.as_ref();
    assert!(
        generation.install_authoritative_projection(test_dfs_authoritative_prefix_projection(
            replacement
        ))
    );
    assert_eq!(generation.generation(), old_generation + 1);
    generation.observe_snapshot(&replacement);
    generation.observe_snapshot(&initial);
    assert_eq!(
        generation.retained_complete_prefix_revision_for_generation(old_generation + 1),
        Some(replacement.revision),
        "duplicate or older snapshot delivery must not schedule more recovery"
    );
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
        }]
        .into(),
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
        }]
        .into(),
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
        }]
        .into(),
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
        }]
        .into(),
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
    let entries = (0..sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES)
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
            }]
            .into(),
            final_key_states: vec![sinomo_core::kv::ClusterKvCommittedKeyState {
                key: overflow_key,
                entry: Some(overflow_entry),
            }]
            .into(),
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
    let name = "native-authority-inherited-publish-backlog";
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let retiring = native.root_shard().unwrap();
    let mut source = native.client(&retiring.primary_node_id).unwrap();
    source.posix_mkdir("/native-inherited-prefix").unwrap();
    let inherited = source.posix_stat("/native-inherited-prefix").unwrap();
    let promoted_data = native.data_dir(&nodes[1]).clone();
    let paths = ClusterPaths::new(&promoted_data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let runtime = crate::test_runtime::executor();
    let generation = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &paths,
            &state.cluster_id,
            &state.local_node_id,
            runtime.handle(),
        ))
        .unwrap()
        .authority;
    let before_generation = generation.generation();
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: retiring.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new("root"),
    };
    let before_requests = daemon_dfs_metadata_publish_lane_request_count(&promoted_data, &lane);
    let active = native
        .reassign_metadata_shard(
            &retiring.file_system_id,
            "root",
            nodes[1].clone(),
            vec![nodes[0].clone()],
        )
        .unwrap();
    assert_eq!(active.primary_node_id, nodes[1]);
    assert!(
        generation.generation() > before_generation,
        "actual native authority activation advances its retained projection"
    );
    assert!(
        daemon_dfs_metadata_publish_lane_request_count(&promoted_data, &lane) > before_requests,
        "the real authority edge requests publication of inherited journal data without another mutation"
    );
    let observed = native
        .maintenance_tick(&nodes[1], Duration::from_secs(5))
        .unwrap();
    assert!(
        observed.outcome.is_ok(),
        "ordinary inherited owner: {:?}",
        observed.outcome
    );
    assert_eq!(observed.omitted_kv_event_count, 0);
    let mut promoted = native.client(&nodes[1]).unwrap();
    assert_eq!(
        promoted
            .posix_stat("/native-inherited-prefix")
            .unwrap()
            .object_id,
        inherited.object_id
    );
    promoted
        .posix_mkdir("/native-inherited-owner-live")
        .unwrap();
    drop(promoted);
    drop(source);
    native.close().unwrap();
}

#[test]
fn metadata_authority_discovery_rolls_resident_generation_without_publish_lanes() {
    let (native, nodes) = crate::test_support::DfsTestRuntime::start_cluster(
        "daemon-dfs-authority-idle-resident-rollover",
        4,
    )
    .expect("normal mounted native authorities");
    let configs = native
        .metadata_shards()
        .expect("actual committed assignments");
    let local = nodes
        .iter()
        .find(|node| {
            configs
                .iter()
                .all(|config| &config.primary_node_id != *node)
        })
        .expect("actual non-primary observer")
        .clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = native.runtime_handle();
    let root_shard = configs
        .iter()
        .find(|config| config.shard_id == "root")
        .expect("normal committed root assignment")
        .clone();
    assert!(configs.iter().all(|config| config.primary_node_id != local));
    assert!(
        runtime
            .block_on(daemon_dfs_metadata_publish_discovery_lanes_boxed_async(
                &data_dir,
                &cluster_paths,
                &runtime,
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
        Some(&runtime),
    )
    .expect("metadata authority");
    let initial_generation = authority.generation();

    let promoted = nodes
        .iter()
        .find(|node| **node != local && **node != root_shard.primary_node_id)
        .expect("another actual remote owner")
        .clone();
    let replicas = std::iter::once(root_shard.primary_node_id.clone())
        .chain(root_shard.replica_node_ids.iter().cloned())
        .filter(|node| node != &promoted)
        .collect::<Vec<_>>();
    let advanced = native
        .reassign_metadata_shard(
            &root_shard.file_system_id,
            &root_shard.shard_id,
            promoted,
            replicas,
        )
        .expect("actual freeze/ACK/joint-activation handoff between remote owners");
    assert!(advanced.epoch > root_shard.epoch);
    assert_ne!(advanced.primary_node_id, local);
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
                &runtime,
            ))
            .expect("advanced remote-only discovery")
            .is_empty()
    );
    let resident = bounded_resident().expect("idle resident generation rolled forward");
    assert!(resident.bounded_status_snapshot().is_ok());
    let cache_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .expect("actual root cache slot");
    let cache = cache_slot.lock().expect("message cache");
    let entry = cache.as_ref().expect("resident cache entry");
    assert_eq!(
        entry.signature.metadata_authority_generation,
        authority.generation()
    );
    assert!(entry.signature.metadata_authority_generation > initial_generation);
    drop(cache);
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"));
    drop(resident);
    drop(authority);
    native.close().expect("normal native owner shutdown");
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
    let native_projection = generation
        .current_complete_prefix()
        .expect("initial native authority proof");
    let initial_storage_generation = generation.storage_generation();
    let refreshes_before = generation.authority_refresh_count();
    let resident = cluster_paths
        .kv_paths()
        .raft_store()
        .open_node(&local)
        .expect("hold the native owner across derived replacement");
    // Write through the R0 materialized-snapshot API, deliberately bypassing
    // sinomo-cluster's in-process committed-snapshot observer. This models
    // a separate CLI process advancing the shared Docker data directory.
    sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &external_snapshot)
        .expect("materialize external authority snapshot");
    assert_eq!(
        generation.current_complete_prefix_revision(),
        Some(native_projection.revision())
    );
    assert_eq!(
        generation.generation(),
        initial_generation,
        "replacing the observational checkpoint must not change committed DFS authority"
    );
    assert_eq!(
        generation.current_complete_prefix(),
        Some(native_projection),
        "derived future promotion neither grants nor retires native authority"
    );
    assert_eq!(generation.storage_generation(), initial_storage_generation);
    assert_eq!(
        generation.authority_refresh_count(),
        refreshes_before,
        "derived-file changes cannot introduce new native authority reads"
    );
    drop(resident);

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
    let final_key_states: Vec<_> = events
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
            events: events.into(),
            final_key_states: final_key_states.into(),
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
        }]
        .into(),
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
        }]
        .into(),
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
        }]
        .into(),
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
        }]
        .into(),
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

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired = DfsMetadataShardConfig {
        primary_node_id: local.clone(),
        replica_node_ids: vec![retiring_primary.clone()],
        epoch: 2,
        ..retiring.clone()
    };
    let handoff = DfsMetadataShardMembershipHandoff {
        cause: crate::DfsMetadataShardHandoffCause::Membership,
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
        cause: crate::DfsMetadataShardHandoffCause::Membership,
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
    let local =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-message-authority-cache", 1)
            .remove(0);
    let (native, client) = crate::test_runtime::DfsTestRuntime::start_single_node(
        "native-message-authority-cache",
        local.clone(),
    )
    .unwrap();
    drop(client);
    let context = native.component_context(&local).unwrap();
    let runtime = context
        .runtime()
        .expect("actual mounted Main runtime")
        .clone();
    let data_dir = native.data_dir(&local).clone();
    let paths = DaemonPaths::new(&data_dir);
    let cache_key = data_dir.root();
    let cluster = context
        .cluster_access()
        .expect("actual native committed configuration owner");
    let commit_unrelated = |value: &[u8]| {
        let request = sinomo_core::kv::KvTxnRequest::new(
            sinomo_core::kv::KvRequestId::generate(),
            sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![sinomo_core::kv::KvOp::Put {
                    key: "/cluster/authority-cache-unrelated".into(),
                    value: value.to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        );
        let committed = runtime
            .block_on(cluster.kv_commit_compact_async(request))
            .expect("normal current-term native configuration commit");
        assert!(
            committed
                .txn_response
                .is_some_and(|response| response.succeeded)
        );
    };
    let lookup = |id: &str, frame, path: &str| {
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                id,
                test_dfs_lookup_routed_message(&local, &local, frame, path),
            ),
            &runtime,
        )
        .expect("actual mounted Main lookup");
        assert_eq!(response.request_id, id);
    };
    let lane = || {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&cache_key)
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        Arc::clone(
            cache
                .as_ref()
                .unwrap()
                .primary_service
                .as_ref()
                .expect("actual resident metadata lane"),
        )
    };
    commit_unrelated(b"initial");
    lookup("dfs_message_authority_cache_1", 1, "/services/cache-1.txt");
    let first_lane = lane();
    let first_lane_weak = Arc::downgrade(&first_lane);
    let opens_before = daemon_dfs_test_message_service_lane_open_count(&data_dir);
    commit_unrelated(b"changed");
    lookup("dfs_message_authority_cache_2", 2, "/services/cache-2.txt");
    let unchanged_lane = lane();
    assert!(
        Arc::ptr_eq(&first_lane, &unchanged_lane),
        "unrelated native KV commits must preserve the resident lane"
    );
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&data_dir),
        opens_before
    );
    let scope = native
        .bootstrap_empty_metadata_prefix("authority-cache-extra", "/authority-cache-extra")
        .expect("normal native genesis, assignment CAS and guarded publication");
    assert_eq!(scope.primary_node_id, local);
    lookup(
        "dfs_message_authority_cache_3",
        3,
        "/authority-cache-extra/cache-3.txt",
    );
    let rebuilt_lane = lane();
    assert!(
        !Arc::ptr_eq(&first_lane, &rebuilt_lane),
        "actual physical topology change must replace the old resident lane"
    );
    drop((unchanged_lane, first_lane));
    assert!(
        first_lane_weak.upgrade().is_none(),
        "superseded completed resident service must be collectible"
    );
    drop((rebuilt_lane, cluster, context));
    native.close().unwrap();
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
fn daemon_dfs_deferred_worker_notifies_after_each_replacement_publication() {
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
            baseline_generation.wrapping_add(1),
            "republishing the same policy must wake the watcher for replacement-owned work"
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
            baseline_generation.wrapping_add(2),
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
    let config = DfsFileSystemConfig::from(&file_system);
    for path in [sinomo_dfs::SIFS_USERS_PATH, sinomo_dfs::SIFS_SERVICES_PATH] {
        for candidate_count in [1, 2, 6] {
            let resolved = sinomo_dfs::resolve_placement_policy(
                &config,
                path,
                DfsObjectPolicy::ReplicatedEventual,
                candidate_count,
            )
            .expect("normal shared default policy");
            assert_eq!(resolved.source_path, None);
            assert_eq!(
                resolved.policy.consistency,
                DfsObjectPolicy::ReplicatedEventual
            );
            assert_eq!(
                (
                    resolved.policy.min_commit_replicas,
                    resolved.policy.desired_replicas
                ),
                (1, candidate_count.min(2))
            );
        }
    }
}

#[test]
fn default_root_metadata_shard_is_deterministic_and_epoch_stable() {
    let node =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-stable-root-config", 1)
            .remove(0);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-stable-root-config", node.clone())
            .unwrap();
    runtime.start_nodes().unwrap();
    let first = runtime.root_shard().unwrap();
    let repeated = runtime.root_shard().unwrap();
    assert_eq!(first.primary_node_id, node);
    assert_eq!(
        first.epoch, 1,
        "complete native singleton genesis retains its original epoch"
    );
    assert_eq!(
        repeated, first,
        "unchanged actual assignment is deterministic and epoch stable"
    );
    runtime.close().unwrap();
}

#[test]
fn default_cluster_metadata_shards_cover_root_users_and_services() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-default-path-shards", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-default-path-shards", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    // Discovery preserves committed owners. Establish the requested spread
    // through the same explicit guarded handoffs used by an operator.
    let initial = runtime.metadata_shards().unwrap();
    assert!(
        initial
            .iter()
            .all(|shard| shard.primary_node_id == nodes[0])
    );
    for (shard, target) in [("services", &nodes[1]), ("users", &nodes[2])] {
        let fs = initial
            .iter()
            .find(|config| config.shard_id == shard)
            .unwrap()
            .file_system_id
            .clone();
        runtime
            .reassign_metadata_shard(
                &fs,
                shard,
                target.clone(),
                nodes
                    .iter()
                    .filter(|node| *node != target)
                    .cloned()
                    .collect(),
            )
            .unwrap();
    }
    let by_id = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .map(|shard| (shard.shard_id.clone(), shard))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_id.len(), 3);
    assert!(matches!(
        by_id["root"].assignment,
        DfsMetadataShardAssignment::Root
    ));
    assert!(matches!(&by_id["services"].assignment,
        DfsMetadataShardAssignment::PathPrefix { prefix } if prefix == crate::SIFS_SERVICES_PATH));
    assert!(matches!(&by_id["users"].assignment,
        DfsMetadataShardAssignment::PathPrefix { prefix } if prefix == crate::SIFS_USERS_PATH));
    // The explicit native handoffs above now own these exact roles.
    assert_eq!(by_id["root"].primary_node_id, nodes[0]);
    assert_eq!(by_id["services"].primary_node_id, nodes[1]);
    assert_eq!(by_id["users"].primary_node_id, nodes[2]);
    runtime.close().unwrap();
}

#[test]
fn default_cluster_metadata_shards_replicate_to_all_service_nodes() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-all-six-metadata-replicas",
        6,
    );
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "native-all-six-metadata-replicas",
        nodes[0].clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    assert_eq!(nodes.len(), 6);
    for shard in runtime.metadata_shards().unwrap() {
        let expected = nodes
            .iter()
            .filter(|node| **node != shard.primary_node_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            shard.replica_node_ids, expected,
            "actual default shard {} includes every other advertised native metadata node",
            shard.shard_id
        );
    }
    runtime.close().unwrap();
}

#[test]
fn default_cluster_metadata_shard_expands_replicas_before_primary_transfer() {
    let name = "native-expand-before-primary-transfer";
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 6);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let expanded = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "services")
        .unwrap();
    assert_eq!(expanded.primary_node_id, nodes[0]);
    assert_eq!(
        std::iter::once(&expanded.primary_node_id)
            .chain(&expanded.replica_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect()
    );
    runtime
        .reassign_metadata_shard(
            &expanded.file_system_id,
            "services",
            nodes[1].clone(),
            nodes
                .iter()
                .filter(|node| **node != nodes[1])
                .cloned()
                .collect(),
        )
        .unwrap();
    let actual = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap();
    assert_eq!(actual.primary_node_id, nodes[1]);
    let control = runtime.shared_control_plane().unwrap();
    let proof: crate::DfsMetadataShardMembershipHandoffProof = control.with_client(|client| {
        client
            .get(&crate::metadata_shard_membership_handoff_proof_key(
                &actual.file_system_id,
                &actual.shard_id,
            ))
            .unwrap()
            .unwrap()
            .decode()
            .unwrap()
    });
    assert_eq!(proof.desired_shard, actual);
    assert_ne!(
        proof.retiring_shard.primary_node_id,
        proof.desired_shard.primary_node_id
    );
    let retiring_nodes = std::iter::once(&proof.retiring_shard.primary_node_id)
        .chain(&proof.retiring_shard.replica_node_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        retiring_nodes,
        nodes.iter().cloned().collect(),
        "the actual retiring assignment expanded to every native metadata node BEFORE primary rotation"
    );
    assert_eq!(
        proof
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        retiring_nodes
    );
    runtime.close().unwrap();
}

#[test]
fn default_cluster_metadata_shard_transfers_after_candidate_replays_current_journal() {
    let nodes =
        crate::test_support::DfsTestRuntime::ordered_node_ids("native-default-shard-transfer", 3);
    let primary = nodes[0].clone();
    let candidate = nodes[1].clone();
    let mut fixture =
        crate::test_support::DfsTestRuntime::new("native-default-shard-transfer", primary.clone())
            .expect("normal runtime");
    fixture.define_node(candidate.clone()).expect("candidate");
    fixture
        .define_node(nodes[2].clone())
        .expect("real recovery-majority peer");
    fixture.start_nodes().expect("normal component startup");
    let file_system_id = fixture.file_system().file_system_id;
    let control = fixture
        .shared_control_plane()
        .expect("real native control plane");
    let configs = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&file_system_id)
        })
        .expect("actual assignments");
    let initial = configs
        .iter()
        .find(|config| config.shard_id == "services")
        .expect("services assignment")
        .clone();
    if initial.primary_node_id != primary {
        let replicas =
            crate::control_plane::topology_preserving_metadata_shard_replicas(&initial, &primary)
                .expect("same native member ring");
        fixture
            .reassign_metadata_shard(&file_system_id, "services", primary.clone(), replicas)
            .expect("native primary placement");
    }
    let actual = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard(&file_system_id, "services")
        })
        .expect("actual assignment")
        .expect("services");
    let mut source = fixture.client(&primary).expect("real source client");
    let admitted = create_directory_with_replica_stopped_before_commit(
        &mut fixture,
        &mut source,
        &primary,
        &candidate,
        "/services/transfer-replay-witness",
    );
    let current = fixture.metadata_primary_service("/services").unwrap();
    let snapshot = current
        .metadata_backend()
        .expect("actual source backend")
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .expect("native source snapshot")
        .expect("services snapshot");
    assert!(snapshot.materialized_through_sequence > admitted.prepare_sequence);
    drop(current);
    let mut desired = actual.clone();
    desired.primary_node_id = candidate.clone();
    desired.replica_node_ids =
        crate::control_plane::topology_preserving_metadata_shard_replicas(&actual, &candidate)
            .expect("unchanged native member ring");
    desired.epoch += 1;
    control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .begin_metadata_shard_membership_handoff(&actual, &desired)
        })
        .expect("real native handoff stage")
        .expect("source assignment exact");
    let configs = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&file_system_id)
        })
        .expect("current native assignments");
    let _blocked = source
        .reconcile_pending_metadata_shard_membership_handoffs(&configs, Duration::from_millis(100));
    assert_eq!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&file_system_id, "services"))
            .unwrap(),
        Some(actual.clone()),
        "primary transfer must wait for actual candidate replay and acknowledgement"
    );
    fixture
        .reopen_node(candidate.clone())
        .expect("normal candidate restart/replay");
    source
        .reconcile_pending_metadata_shard_membership_handoffs(&configs, Duration::from_secs(5))
        .expect("real freeze/replay/joint activation");
    let transferred = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard(&file_system_id, "services")
        })
        .expect("actual transferred assignment")
        .expect("services");
    assert_eq!(transferred, desired);
    assert!(transferred.replica_node_ids.contains(&primary));
    let mut reopened = fixture.client(&candidate).expect("actual promoted owner");
    let target = reopened
        .metadata_backend()
        .expect("actual promoted backend")
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .expect("actual promoted snapshot")
        .expect("services snapshot");
    assert!(target.materialized_through_sequence >= snapshot.materialized_through_sequence);
    assert!(
        reopened
            .posix_stat("/services/transfer-replay-witness")
            .is_ok(),
        "late source publication must survive real candidate replay"
    );
    drop(reopened);
    drop(source);
    drop(control);
    fixture.close().expect("normal owned runtime shutdown");
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
    let recovery_error =
        recover_prior_process_default_dfs_placement_authority(&paths, runtime.handle())
            .expect_err("direct recovery requires actual data-owner custody");
    assert!(
        matches!(&recovery_error, DaemonError::Parse { detail }
        if detail.contains("without a live exclusive main-daemon data-owner lock")),
        "unexpected direct recovery error: {recovery_error}"
    );
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
        matches!(&error, DaemonError::Network { address, detail }
            if address == "DFS startup mounted-work replay"
                && detail == "startup replay has no registered runtime host"),
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
        matches!(&reactor_error, DaemonError::Network { address, detail }
            if address == "DFS startup mounted-work replay"
                && detail == "startup replay has no registered runtime host"),
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
    let local = NodeId::from_legacy_named("node_dfs_smoke_single").expect("local node");
    let mut fixture =
        crate::test_runtime::DfsTestRuntime::new("daemon-dfs-smoke-single-node", local.clone())
            .expect("normal native smoke fixture");
    fixture
        .start_nodes()
        .expect("bootstrap actual mounted DFS owner");
    let paths = DaemonPaths::new(fixture.data_dir(&local));
    let runtime = fixture.runtime_handle();
    let report = runtime
        .block_on(run_daemon_dfs_smoke_async(&paths, &runtime))
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
    fixture.close().expect("drain normal native smoke fixture");
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
        holder_cluster_id: cluster_id.to_string(),
        holder_node_id: local_node_id.clone(),
        holder_session_id: format!("prior-process-{case}-session"),
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

#[test]
fn daemon_dfs_transactions_do_not_materialize_post_commit_snapshots() {
    let root = temp_root("dfs-compact-control-commit");
    let data_dir = SinomoDataDir::new(root.path());
    let node = NodeId::generate();
    write_test_cluster_state(root.path(), &node);
    let paths = ClusterPaths::new(&data_dir);
    let cluster = Arc::new(TestDfsClusterAccess {
        local_data_dir: data_dir.clone(),
        authority_data_dir: data_dir.clone(),
        peers: None,
        object_rpc: None,
        full_commit_calls: AtomicUsize::new(0),
        compact_commit_calls: AtomicUsize::new(0),
    });
    let host = Arc::new(DfsRuntimeHost::new(
        cluster.clone(),
        Arc::new(TestDfsObjectRpcTransport),
        None,
    ));
    register_dfs_runtime_host(&data_dir, &host).unwrap();
    let runtime = daemon_test_runtime("dfs-compact-control-commit");
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime.handle());
    let txn = |key: &str| KvTxn {
        compares: vec![],
        success: vec![KvOp::Put {
            key: key.into(),
            value: b"committed".to_vec(),
            lease_id: None,
        }],
        failure: vec![],
    };
    let first =
        sinomo_dfs::DfsControlPlaneKvClient::txn(&mut client, txn("/dfs/test/compact-sync"))
            .unwrap();
    let second = runtime
        .block_on(sinomo_dfs::AsyncDfsControlPlaneKvClient::txn_async(
            &mut client,
            txn("/dfs/test/compact-async"),
        ))
        .unwrap();
    let (third, native) = sinomo_dfs::DfsControlPlaneKvClient::txn_with_native_authority(
        &mut client,
        txn("/dfs/test/compact-native-proof"),
    )
    .expect("existing native commit receipt");
    let native = native.expect("normal R0 host supplies actual commit evidence");
    assert!(third.succeeded);
    assert_eq!(native.result.request_id, native.request.request_id);
    assert_eq!(native.result.txn_response.as_ref(), Some(&third));
    assert_eq!(native.result.authority.revision, third.revision);
    assert_eq!(
        native
            .result
            .submitted_command_digest
            .unwrap_or(native.result.command_digest),
        native
            .request
            .clone()
            .into_command_request()
            .digest()
            .unwrap(),
    );
    assert!(matches!(native.result.authority.basis,
        sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum { voter_acks, required_quorum }
        if required_quorum > 0 && voter_acks >= required_quorum));
    assert!(first.succeeded && second.succeeded);
    assert!(second.revision > first.revision);
    assert_eq!(
        cluster.full_commit_calls.load(Ordering::SeqCst),
        0,
        "DFS must not request a full post-commit snapshot that can fail after the commit"
    );
    assert_eq!(cluster.compact_commit_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        sinomo_dfs::DfsControlPlaneKvClient::get(&client, "/dfs/test/compact-async")
            .unwrap()
            .unwrap()
            .value,
        b"committed"
    );
    unregister_dfs_runtime_host(&data_dir);
}

mod predispatch_authority_retry;

#[test]
fn daemon_intent_scan_cursor_survives_dropped_maintenance_shell_and_resets_with_cache_owner() {
    let (native, nodes) = crate::test_support::DfsTestRuntime::start_cluster(
        "daemon-intent-scan-owner-continuation",
        1,
    )
    .expect("normal mounted native owner");
    let local = nodes[0].clone();
    let data_dir = native.data_dir(&local).clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = native.runtime_handle();
    let mut caller = native.client(&local).expect("normal registered client");
    caller
        .posix_lstat_distributed("/services")
        .expect("ordinary Main lookup warms its actual resident cache");
    drop(caller);
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .expect("current bootstrap state");
    // Retain native authority before admission across cache replacement. No
    // background scan may run between eviction and observing the fresh owner.
    let preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            &runtime,
        ))
        .expect("retain exact authority across cache-owner replacement");
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).expect("scheduler");
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .expect("exclusive maintenance ownership");
    let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        &runtime,
        None,
    )
    .expect("take actual cached lane");
    let owner = Arc::clone(&retained.lane.service.write_intent_maintenance_scan);
    let key = |second| crate::DfsWriteIntentScanCursor {
        created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(second),
        intent_id: format!("cache-cursor-{second}"),
        object_id: crate::DfsObjectId::from_legacy_named(format!("cache-cursor-object-{second}"))
            .unwrap(),
    };
    let expected = Some(crate::DfsWriteIntentMaintenanceCursor {
        shard_id: DfsMetadataShardId::new("root"),
        after: Some(key(2)),
        through: Some(key(9)),
    });
    let initial_cursor = owner
        .cursor()
        .expect("actual admitted startup scan position");
    owner
        .advance(&initial_cursor, expected.clone())
        .expect("complete raw page cursor");
    assert_eq!(owner.cursor().unwrap(), expected);
    owner.advance(&initial_cursor, None).unwrap();
    assert_eq!(
        owner.cursor().unwrap(),
        expected,
        "a stale compare cannot rewind the scan"
    );
    // Cancellation/error destroys the taken service without returning it. The
    // cache's independent Arc must retain both after-key and frozen upper cut.
    drop(retained);
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&data_dir.root())
            .expect("actual root cache slot");
        let cache = cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert!(entry.primary_service.is_none());
        assert!(
            entry.resident_metadata_initialized && entry.resident_metadata_backend.is_some(),
            "fixture owns a real resident backend"
        );
        assert!(Arc::ptr_eq(&owner, &entry.write_intent_maintenance_scan));
        assert_eq!(
            entry.write_intent_maintenance_scan.cursor().unwrap(),
            expected
        );
    }
    let rebuilt = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        &runtime,
        None,
    )
    .expect("rebuild a real maintenance service after cancellation");
    assert!(Arc::ptr_eq(
        &owner,
        &rebuilt.lane.service.write_intent_maintenance_scan
    ));
    assert_eq!(
        rebuilt
            .lane
            .service
            .write_intent_maintenance_scan
            .cursor()
            .unwrap(),
        expected
    );
    drop(rebuilt);

    // Eviction is a real cache-owner replacement, unlike merely constructing
    // two services with a manually shared cursor. A fresh owner safely rewinds.
    assert!(evict_daemon_dfs_message_service_cache(&data_dir).unwrap());
    let replaced = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        &runtime,
        Some(&preflight),
    )
    .expect("new cache owner");
    assert!(!Arc::ptr_eq(
        &owner,
        &replaced.lane.service.write_intent_maintenance_scan
    ));
    assert_eq!(
        replaced
            .lane
            .service
            .write_intent_maintenance_scan
            .cursor()
            .unwrap(),
        None
    );
    assert_eq!(
        owner.cursor().unwrap(),
        expected,
        "old observer cannot rewrite the new owner"
    );
    drop(replaced);
    evict_daemon_dfs_message_service_cache(&data_dir).unwrap();
    drop(admission);
    drop(preflight);
    drop(owner);
    native.close().expect("normal native owner shutdown");
}

mod metadata_store_lifecycle;

#[test]
fn cancelled_embedded_runtime_join_restores_unfinished_task_ownership() {
    let local =
        crate::test_support::DfsTestRuntime::ordered_node_ids("dfs-runtime-join-cancellation", 1)
            .remove(0);
    let mut native =
        crate::test_support::DfsTestRuntime::new("dfs-runtime-join-cancellation", local.clone())
            .unwrap();
    native.start_nodes().unwrap();
    let data_dir = native.data_dir(&local).clone();
    let runtime = native.runtime_handle();
    let host = registered_dfs_runtime_host(&data_dir)
        .unwrap()
        .expect("actual mounted component host");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_completed = Arc::clone(&completed);
    let task = runtime.spawn_blocking(move || {
        started_tx.send(()).unwrap();
        if release_rx.recv().is_ok() {
            worker_completed.store(true, Ordering::Release);
        }
    });
    let task_id = task.id();
    host.own_task(task);
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    runtime.block_on(async {
        let join_data_dir = data_dir.clone();
        let joining = tokio::spawn(async move {
            join_registered_runtime_for_test(&join_data_dir, Duration::from_secs(5)).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if host
                    .tasks
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|task| task.id() != task_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("join owns the running blocking task");
        joining.abort();
        assert!(joining.await.unwrap_err().is_cancelled());
        assert!(
            host.tasks
                .lock()
                .unwrap()
                .iter()
                .any(|task| task.id() == task_id && !task.is_finished()),
            "cancellation restores the exact unfinished task to its existing native owner"
        );
        assert!(!completed.load(Ordering::Acquire));
        release_tx.send(()).unwrap();
        join_registered_runtime_for_test(&data_dir, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(host.tasks.lock().unwrap().is_empty());
        assert!(completed.load(Ordering::Acquire));
    });
    drop(host);
    native.close().unwrap();
}

#[test]
fn stopped_runtime_joins_a_started_blocking_child_registered_during_drain() {
    let local =
        crate::test_support::DfsTestRuntime::ordered_node_ids("dfs-runtime-late-blocking-owner", 1)
            .remove(0);
    let mut native =
        crate::test_support::DfsTestRuntime::new("dfs-runtime-late-blocking-owner", local.clone())
            .unwrap();
    native.start_nodes().unwrap();
    let data_dir = native.data_dir(&local).clone();
    let runtime = native.runtime_handle();
    let host = registered_dfs_runtime_host(&data_dir)
        .unwrap()
        .expect("actual mounted component host");
    let paths = DfsRuntimePaths::new(&data_dir);
    let owner = require_daemon_dfs_data_owner_lock(&paths).unwrap();
    let owner_file_identity = owner
        .storage_owner_file_identity()
        .expect("actual original owner file identity");
    let owner_weak = Arc::downgrade(&owner);
    let (parent_started_tx, parent_started_rx) = std::sync::mpsc::channel();
    let (create_child_tx, create_child_rx) = std::sync::mpsc::channel();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let (child_registered_tx, child_registered_rx) = tokio::sync::oneshot::channel();
    let child_host = Arc::clone(&host);
    let child_runtime = runtime.clone();
    host.own_task(runtime.spawn_blocking(move || {
        parent_started_tx.send(()).unwrap();
        create_child_rx.recv().unwrap();
        let child = child_runtime.spawn_blocking(move || {
            let _actual_owner = owner;
            child_started_tx.send(()).unwrap();
            let _ = release_child_rx.recv();
        });
        // Wait for actual execution before registering: abort cannot stop this
        // child, and the stopped host must retain it in its existing registry.
        child_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        child_host.own_task(child);
        let _ = child_registered_tx.send(());
    }));
    parent_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    runtime.block_on(async {
        let join_data_dir = data_dir.clone();
        let joining = tokio::spawn(async move {
            join_registered_runtime_for_test(&join_data_dir, Duration::from_secs(5)).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !host.shutting_down.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        create_child_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), child_registered_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(owner_weak.upgrade().is_some());
        assert!(!joining.is_finished(), "started child still owns storage");
        release_child_tx.send(()).unwrap();
        joining.await.unwrap().unwrap();
        assert!(owner_weak.upgrade().is_some(),
            "the actual mounted component still retains the same storage owner after this task drain");
        assert!(host.tasks.lock().unwrap().is_empty());

    });
    drop(host);
    native.stop_node(&local).unwrap();
    assert!(
        owner_weak.upgrade().is_none(),
        "actual component scope drain and started blocking-child join release the original owner"
    );
    native.reopen_node(local).unwrap();
    let reopened = require_daemon_dfs_data_owner_lock(&paths).unwrap();
    assert_eq!(
        reopened
            .storage_owner_file_identity()
            .expect("actual remounted owner file identity"),
        owner_file_identity,
        "normal remount acquires the actual same storage-owner file"
    );
    assert!(
        !std::sync::Weak::ptr_eq(&owner_weak, &Arc::downgrade(&reopened)),
        "normal remount owns a new capability, rather than reusing the drained owner"
    );
    drop(reopened);
    native.close().unwrap();
}

#[test]
fn maintenance_peer_discovery_uses_owned_projection_without_authoritative_reads() {
    let (native, mut requester, shard) = crate::test_support::current_native_metadata_service(
        "native-maintenance-no-catalog-refetch",
        3,
        "/",
    )
    .unwrap();
    requester
        .posix_write_file(
            "/peer-discovery-ready",
            b"native readiness",
            crate::DfsWriteOptions::replicated_eventual(vec![shard.primary_node_id.clone()]),
        )
        .unwrap();
    drop(requester);
    let data_dir = native.data_dir(&shard.primary_node_id).clone();
    let paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let runtime = native.runtime_handle();
    let expected = std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    take_daemon_dfs_test_authoritative_prefix_call_count(&paths);
    for _ in 0..8 {
        let mut retained = take_daemon_dfs_message_mutation_service_for_maintenance(
            &data_dir, &paths, &state, &runtime, None,
        )
        .unwrap();
        assert_eq!(
            retained
                .peer_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            retained
                .lane
                .service
                .placement_content_candidates()
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.node_id)
                .collect::<BTreeSet<_>>(),
            expected,
            "content planning consumes the same coherent advertisements without metadata discovery"
        );
        assert!(
            restore_daemon_dfs_message_mutation_service_after_maintenance(
                &data_dir, &paths, &runtime, retained,
            )
            .unwrap()
        );
    }
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&paths),
        0,
        "maintenance lane handoff must not refetch the already-owned provider catalog"
    );
    drop(admission);
    native.close().unwrap();
}

#[test]
fn native_peer_projection_uses_owned_members_and_catalog_without_optional_checkpoint() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-owned-peer-projection", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-owned-peer-projection", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    for node in &nodes {
        let paths = ClusterPaths::new(runtime.data_dir(node));
        let snapshot = read_daemon_dfs_message_peer_snapshot(&paths)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.local_node_id, *node);
        assert_eq!(
            snapshot.peer_node_ids, nodes,
            "committed current native providers are not hidden by an optional KV checkpoint"
        );
    }
    let host = registered_dfs_runtime_host(runtime.data_dir(&nodes[0]))
        .unwrap()
        .unwrap();
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(
        runtime.data_dir(&nodes[0]),
    ))
    .unwrap();
    let members = host
        .cluster
        .kv_range_prefix_local("/cluster/members/")
        .unwrap()
        .value
        .into_iter()
        .map(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>().unwrap())
        .collect::<Vec<_>>();
    let mut entries = host
        .cluster
        .kv_range_prefix_local(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
        .unwrap()
        .value;
    let mut changed = false;
    for entry in &mut entries {
        let mut instance = entry
            .decode::<sinomo_core::service::ServiceInstanceRecord>()
            .unwrap();
        if instance.node_id == nodes[2]
            && instance
                .provider_capabilities
                .iter()
                .any(|capability| capability == "dfs.object.transfer")
        {
            assert_eq!(instance.interface_version, crate::DFS_PROTOCOL);
            instance.interface_version = "sinomo.dfs.v1".into();
            entry.value = sinomo_core::kv::encode_kv_value(&instance).unwrap();
            changed = true;
        }
    }
    assert!(
        changed,
        "the negative input is derived from a genuine mounted provider advertisement"
    );
    let current = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
        &state, &members, &entries,
    )
    .unwrap();
    assert!(
        !current.contains(&nodes[2]),
        "an incompatible provider must upgrade rather than receive current operations"
    );
    assert!(current.contains(&nodes[0]) && current.contains(&nodes[1]));
    drop(host);
    runtime.close().unwrap();
}

// Captures the actual ordinary producer body after genuine local/remote byte
// work, before hidden preparation. The body and receipts are copied unchanged.
fn with_native_create_proposal(
    name: &str,
    reader: bool,
    missing_replica: bool,
    inspect: impl FnOnce(
        &mut crate::test_support::DfsTestRuntime,
        &NodeId,
        &crate::DfsPreparedOperationPayload,
    ),
) {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(
        name,
        if missing_replica { 2 } else { 1 },
    );
    let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    native.start_nodes().unwrap();
    let config = native.root_shard().unwrap();
    let primary = config.primary_node_id.clone();
    if missing_replica {
        native.install_fault(crate::test_runtime::FaultRule {
            source: Some(primary.clone()),
            target: Some(nodes[1].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    let mut source = native.metadata_primary_service("/").unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, nodes.len()).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let (captured_tx, captured_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let release = Release(Some(release_tx));
        let publisher = scope.spawn(move || {
            crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |payload| {
                    captured_tx.send(payload.clone()).unwrap();
                    let _ = release_rx.recv();
                }));
            });
            let bytes = vec![91u8; 256 * 1024];
            if reader {
                source.posix_write_file_from_reader(
                    "/native-source-proposal.bin",
                    std::io::Cursor::new(bytes),
                    options,
                )
            } else {
                source.posix_write_file("/native-source-proposal.bin", &bytes, options)
            }
        });
        let payload = captured_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("actual native before-prepare body");
        inspect(&mut native, &primary, &payload);
        drop(release);
        let _actual_outcome = publisher
            .join()
            .expect("actual native producer joins after source inspection");
    });
    native.clear_faults();
    assert_eq!(native.observation_dropped_count(), 0);
    native.close().unwrap();
}

struct NativePendingSourceFixture {
    native: crate::test_support::DfsTestRuntime,
    source: Option<crate::NetworkedDfsService<crate::DfsNodeTransport>>,
    primary: NodeId,
    intent: crate::DfsWriteIntentRecord,
    receipt: crate::DfsReplicaReceiptRecord,
}
impl NativePendingSourceFixture {
    fn close(mut self) {
        self.source
            .as_mut()
            .unwrap()
            .abort_pending_write(&self.intent.intent_id)
            .unwrap();
        drop(self.source.take());
        self.native.clear_faults();
        assert_eq!(self.native.observation_dropped_count(), 0);
        self.native.close().unwrap();
    }
}
fn native_pending_source_fixture(name: &str, patch: bool) -> NativePendingSourceFixture {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let mut native = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    native.start_nodes().unwrap();
    let config = native.root_shard().unwrap();
    let primary = config.primary_node_id.clone();
    let remote = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut source = native.metadata_primary_service("/").unwrap();
    let path = "/native-pending-source.bin";
    let original = source
        .posix_write_file(
            path,
            &vec![37u8; 256 * 1024],
            DfsWriteOptions::replicated_eventual(vec![primary.clone(), remote.clone()]),
        )
        .unwrap();
    let mut file_system = native.file_system();
    let mut policy = crate::DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedConfirmed, 2, 2);
    policy.required_node_ids = vec![primary.clone(), remote.clone()];
    file_system.subtree_policies.push(policy);
    source.add_file_system(file_system).unwrap();
    native.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(remote.clone()),
        route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    let request = DfsWriteRequest {
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        target_version: DfsVersion::generate(),
        requesting_node_id: primary.clone(),
    };
    let outcome = if patch {
        source
            .write_distributed_eventual_patch_with_base(
                request,
                original.version,
                64 * 1024,
                b"actual patch bytes",
                vec![primary.clone(), remote],
                DfsReplicationPolicy::new(2, 2).unwrap(),
                false,
            )
            .unwrap()
            .expect("normal patch staging")
    } else {
        source
            .write_distributed_eventual(
                request,
                vec![93u8; 256 * 1024],
                vec![primary.clone(), remote],
                DfsReplicationPolicy::new(2, 2).unwrap(),
            )
            .unwrap()
    };
    let crate::NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = outcome
    else {
        panic!("real content-stage fault must retain W2 pending");
    };
    assert_eq!((durable_receipts, required_receipts), (1, 2));
    let backend = source.metadata_backend().unwrap();
    let intent = backend
        .get_intent(&original.object_id, &intent_id)
        .unwrap()
        .unwrap();
    let receipt = backend
        .get_receipt_on_shard(
            &DfsMetadataShardId::new(config.shard_id),
            &intent.object_id,
            &intent.target_version,
            &primary,
        )
        .unwrap()
        .unwrap();
    assert_eq!(receipt.replica_node_id, primary);
    assert_eq!(receipt.receipt.committed_version, intent.target_version);
    NativePendingSourceFixture {
        native,
        source: Some(source),
        primary,
        intent,
        receipt,
    }
}

#[test]
fn native_cold_root_cache_does_not_block_another_ready_root() {
    use crate::test_runtime::DfsTestRuntime;
    use std::sync::Condvar;
    let (first, first_nodes) =
        DfsTestRuntime::start_cluster("native-cold-cache-first-root", 1).unwrap();
    let (second, second_nodes) =
        DfsTestRuntime::start_cluster("native-cold-cache-second-root", 1).unwrap();
    let first_node = first_nodes[0].clone();
    let second_node = second_nodes[0].clone();
    let mut ready = second.client(&second_node).unwrap();
    ready
        .posix_write_file(
            "/ready-cache-before",
            b"before",
            DfsWriteOptions::replicated_eventual(vec![second_node.clone()]),
        )
        .unwrap();
    let second_paths = ClusterPaths::new(second.data_dir(&second_node));
    let second_peer = read_daemon_dfs_message_peer_snapshot(&second_paths)
        .unwrap()
        .unwrap();
    drop(daemon_dfs_bounded_status_resident(second.data_dir(&second_node), &second_peer).unwrap());

    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    let release = ReleaseOnDrop(Arc::clone(&gate));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let entered_tx = Mutex::new(Some(entered_tx));
    let blocked_gate = Arc::clone(&gate);
    let hook = super::message_host::install_daemon_dfs_pre_dispatch_test_hook(
        first.data_dir(&first_node),
        Arc::new(move |point, _, _| {
            if point
                != super::message_host::DaemonDfsPreDispatchTestPoint::BeforeResidentBackendOpen
            {
                return Ok(());
            }
            if let Some(entered) = entered_tx.lock().unwrap().take() {
                entered.send(()).unwrap();
                let mut released = blocked_gate.0.lock().unwrap();
                while !*released {
                    released = blocked_gate.1.wait(released).unwrap();
                }
            }
            Ok(())
        }),
    );
    // Clear the actual first root cache while its existing native owner holds
    // Global admission; the next ordinary Main request must cold-open it.
    let scheduler = daemon_dfs_mutation_scheduler(first.data_dir(&first_node)).unwrap();
    let admission = first
        .runtime_handle()
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    assert!(evict_daemon_dfs_message_service_cache(first.data_dir(&first_node)).unwrap());
    drop(admission);
    let mut cold = first.client(&first_node).unwrap();
    let first_actor = first_node.clone();
    let writer = std::thread::spawn(move || {
        let written = cold.posix_write_file(
            "/cold-cache-after",
            b"cold root",
            DfsWriteOptions::replicated_eventual(vec![first_actor]),
        );
        let read = written
            .as_ref()
            .ok()
            .map(|_| cold.posix_read_file("/cold-cache-after"));
        drop(cold);
        (written, read)
    });
    let entered = entered_rx.recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    let written = ready.posix_write_file(
        "/ready-cache-during",
        b"independent ready root",
        DfsWriteOptions::replicated_eventual(vec![second_node.clone()]),
    );
    let read = written
        .as_ref()
        .ok()
        .map(|_| ready.posix_read_file("/ready-cache-during"));
    let resident = daemon_dfs_bounded_status_resident(second.data_dir(&second_node), &second_peer);
    // Resume before assertions or close so a failed independent-root check
    // cannot strand a genuine started child behind this negative test gate.
    drop(release);
    let first_result = writer.join().unwrap();
    drop(hook);
    entered.expect("the real first root cold-open boundary was reached");
    written.expect("another ready root executes while the first root cache is held");
    assert_eq!(read.unwrap().unwrap(), b"independent ready root");
    drop(resident.expect("another ready root remains passively observable"));
    first_result.0.unwrap();
    assert_eq!(first_result.1.unwrap().unwrap(), b"cold root");
    drop(ready);
    first.close().unwrap();
    second.close().unwrap();
}

#[cfg(unix)]
#[test]
fn native_cache_alias_and_physical_root_share_one_owner_slot() {
    use crate::test_runtime::DfsTestRuntime;
    let node = DfsTestRuntime::ordered_node_ids("native-cache-physical-alias", 1).remove(0);
    let mut native =
        DfsTestRuntime::new_with_data_dir_alias("native-cache-physical-alias", node.clone())
            .unwrap();
    native.start_nodes().unwrap();
    let lexical = native.data_dir(&node).root();
    let physical = lexical.canonicalize().unwrap();
    assert_ne!(lexical, physical);
    let logical_slot = daemon_dfs_message_service_cache().root(&lexical).unwrap();
    let physical_slot = daemon_dfs_message_service_cache().root(&physical).unwrap();
    assert!(Arc::ptr_eq(&logical_slot, &physical_slot));
    drop(logical_slot);
    drop(physical_slot);
    native.close().unwrap();
}

#[test]
fn native_storage_rollover_retains_only_unchanged_shard_instances() {
    use crate::test_runtime::DfsTestRuntime;
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-resident-slot-rollover", 2).unwrap();
    let root_config = native.root_shard().unwrap();
    let local = root_config.primary_node_id.clone();
    let mut writer = native.client(&local).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![local.clone()]);
    writer
        .posix_write_file("/resident-slot-before", b"root before", options.clone())
        .unwrap();
    let services_before = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "services")
        .unwrap();
    let data_dir = native.data_dir(&local);
    let root_slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .unwrap();
    let before = {
        let cache = root_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert!(entry.resident_metadata_initialized);
        entry.resident_metadata_backend.clone().unwrap()
    };
    let root_id = DfsMetadataShardId::new("root");
    let services_id = DfsMetadataShardId::new("services");
    let old_root = before
        .scope_shard(&root_config.file_system_id, &root_id)
        .unwrap();
    let old_services = before
        .scope_shard(&services_before.file_system_id, &services_id)
        .unwrap();
    let identity = |handle: &crate::ResidentLocalMetadataShardHandle| {
        handle
            .with_backend(|backend| Ok(backend as *mut crate::LocalSobjMetadataBackend as usize))
            .unwrap()
    };
    let root_instance = identity(&old_root);
    let services_instance = identity(&old_services);
    let promoted = services_before.replica_node_ids[0].clone();
    let desired = native
        .reassign_metadata_shard(
            &services_before.file_system_id,
            "services",
            promoted,
            vec![services_before.primary_node_id.clone()],
        )
        .unwrap();
    assert_ne!(desired, services_before);
    // This ordinary Main-owned write performs the generation rollover, rather
    // than directly installing any cache, config, slot or writer capability.
    writer
        .posix_write_file("/resident-slot-after", b"root after", options)
        .unwrap();
    let after = {
        let cache = root_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert!(entry.resident_metadata_initialized);
        entry.resident_metadata_backend.clone().unwrap()
    };
    assert_eq!(after.shard_config(&root_id).unwrap(), root_config);
    assert_eq!(after.shard_config(&services_id).unwrap(), desired);
    assert_eq!(
        identity(
            &after
                .scope_shard(&root_config.file_system_id, &root_id)
                .unwrap()
        ),
        root_instance
    );
    assert_ne!(
        identity(
            &after
                .scope_shard(&desired.file_system_id, &services_id)
                .unwrap()
        ),
        services_instance
    );
    assert_eq!(
        writer.posix_read_file("/resident-slot-before").unwrap(),
        b"root before"
    );
    assert_eq!(
        writer.posix_read_file("/resident-slot-after").unwrap(),
        b"root after"
    );
    // Every retained DATA handle is dropped before the normal mounted owner
    // close; it cannot prolong shutdown or carry authority into a reopen.
    drop(old_services);
    drop(old_root);
    drop(before);
    drop(after);
    drop(writer);
    assert_eq!(nodes.len(), 2);
    native.close().unwrap();
}

#[test]
fn native_retained_shard_slot_keeps_the_actual_queued_checkout() {
    use crate::test_runtime::DfsTestRuntime;
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-retained-slot-checkout", 1).unwrap();
    let local = nodes[0].clone();
    let mut writer = native.client(&local).unwrap();
    writer
        .posix_write_file(
            "/retained-slot-custody",
            b"durable",
            DfsWriteOptions::replicated_eventual(vec![local.clone()]),
        )
        .unwrap();
    let data_dir = native.data_dir(&local);
    let slot = daemon_dfs_message_service_cache()
        .root(&data_dir.root())
        .unwrap();
    let before = {
        let cache = slot.lock().unwrap();
        cache
            .as_ref()
            .unwrap()
            .resident_metadata_backend
            .clone()
            .unwrap()
    };
    let configs = native.metadata_shards().unwrap();
    let root = native.root_shard().unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(data_dir).unwrap();
    let admission = native
        .runtime_handle()
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let root_id = DfsMetadataShardId::new("root");
    let old = before.scope_shard(&root.file_system_id, &root_id).unwrap();
    let held = old.checkout().unwrap();
    let current = before
        .into_reconfigured_for_replica_repair(
            crate::default_dfs_metadata_shards_root(data_dir),
            configs,
            &local,
        )
        .unwrap();
    let next = current.scope_shard(&root.file_system_id, &root_id).unwrap();
    assert!(
        next.try_checkout().unwrap().is_none(),
        "retention cannot create a second available backend beside the held original"
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let queued = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let lease = next.checkout().unwrap();
        let sequence = lease.metadata_prefix_proof().unwrap().journal_sequence;
        drop(lease);
        finished_tx.send(sequence).unwrap();
    });
    started_rx
        .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(finished_rx.try_recv().is_err());
    let expected = held.metadata_prefix_proof().unwrap().journal_sequence;
    drop(held);
    assert_eq!(
        finished_rx
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap(),
        expected
    );
    queued.join().unwrap();
    drop(old);
    drop(current);
    drop(admission);
    drop(writer);
    native.close().unwrap();
}

#[test]
fn native_object_rpc_route_capability_tracks_both_endpoint_lifetimes() {
    use sinomo_core::network::RoutedAvailability;
    let label = "native-object-rpc-route-capability";
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(label, 2);
    let mut native = crate::test_runtime::DfsTestRuntime::new(label, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native.start_nodes().unwrap();
    let transport = native
        .component_context(&nodes[0])
        .unwrap()
        .object_rpc_transport()
        .unwrap();
    assert_eq!(
        transport.availability(&nodes[1]),
        RoutedAvailability::Candidate
    );
    let paths = ClusterPaths::new(native.data_dir(&nodes[0]));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == nodes[1])
        .unwrap();
    assert!(
        sinomo_dfs::maintenance::dfs_transport_candidate_can_relax_unknown_route(
            &ClusterNodeAvailability::Unknown,
            member,
            transport.availability(&nodes[1]),
        )
    );
    assert!(
        sinomo_dfs::dfs_offline_node_ids(&paths, &state).contains(&nodes[1]),
        "default Unknown keeps the existing routing projection"
    );
    assert!(
        !daemon_dfs_offline_node_ids(&paths, &state).contains(&nodes[1]),
        "actual callable embedded route resolves only the routing exclusion"
    );
    assert_eq!(
        sinomo_module::ComponentObjectRpcTransport::availability(
            &TestDfsObjectRpcTransport,
            &nodes[1]
        ),
        RoutedAvailability::Unknown
    );
    for availability in [
        ClusterNodeAvailability::Offline,
        ClusterNodeAvailability::Online,
    ] {
        assert!(
            !sinomo_dfs::maintenance::dfs_transport_candidate_can_relax_unknown_route(
                &availability,
                member,
                RoutedAvailability::Candidate,
            )
        );
    }
    for route in [RoutedAvailability::NoPath, RoutedAvailability::Unknown] {
        assert!(
            !sinomo_dfs::maintenance::dfs_transport_candidate_can_relax_unknown_route(
                &ClusterNodeAvailability::Unknown,
                member,
                route,
            )
        );
    }
    let mut removed = member.clone();
    removed.state = sinomo_core::ClusterMemberState::Removed;
    assert!(
        !sinomo_dfs::maintenance::dfs_transport_candidate_can_relax_unknown_route(
            &ClusterNodeAvailability::Unknown,
            &removed,
            RoutedAvailability::Candidate,
        )
    );
    native.stop_node(&nodes[1]).unwrap();
    assert_eq!(
        transport.availability(&nodes[1]),
        RoutedAvailability::NoPath
    );
    assert!(daemon_dfs_offline_node_ids(&paths, &state).contains(&nodes[1]));
    native.reopen_node(nodes[1].clone()).unwrap();
    assert_eq!(
        transport.availability(&nodes[1]),
        RoutedAvailability::Candidate
    );
    assert!(!daemon_dfs_offline_node_ids(&paths, &state).contains(&nodes[1]));
    native.stop_node(&nodes[0]).unwrap();
    assert_eq!(
        transport.availability(&nodes[1]),
        RoutedAvailability::NoPath,
        "target registration alone cannot make a withdrawn source callable"
    );
    native.reopen_node(nodes[0].clone()).unwrap();
    let current = native
        .component_context(&nodes[0])
        .unwrap()
        .object_rpc_transport()
        .unwrap();
    assert_eq!(
        current.availability(&nodes[1]),
        RoutedAvailability::Candidate
    );
    drop(current);
    drop(transport);
    native.close().unwrap();
}
