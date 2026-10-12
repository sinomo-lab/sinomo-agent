//! Authority regression tests.

use super::*;

#[test]
fn posix_create_cleans_transient_metadata_when_publication_authorization_is_unavailable() {
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
    let nodes = DfsTestRuntime::ordered_node_ids("native-create-publication-unavailable", 3);
    let node_id = nodes[0].clone();
    let parent = "/authority-not-ready-cleanup";
    let path = "/authority-not-ready-cleanup/record.json";
    let payload = b"immutable chat metadata";
    let mut runtime = DfsTestRuntime::new(
        "posix_create_cleans_transient_metadata_when_publication_authorization_is_unavailable",
        node_id.clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    assert_eq!(shard.primary_node_id, node_id);
    assert_eq!(
        shard
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        nodes[1..].iter().cloned().collect()
    );
    // The ordinary exact CoreEffect opener reaches the guard's typed error
    // without a same-node Main RPC erasing it into delivery uncertainty.
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    dfs.posix_mkdir_all(parent).expect("fixture parent");
    let observations_before = runtime.rpc_observations().len();
    for replica in &shard.replica_node_ids {
        for route in [
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ] {
            runtime.install_fault(FaultRule {
                source: Some(node_id.clone()),
                target: Some(replica.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        }
    }
    // The real prepared-publication guard needs operation-bound metadata acknowledgements,
    // including its recovery quorum. A browser planning getter is unrelated to
    // an explicit-options writer and supplies no publication authorization.
    let options = DfsWriteOptions::replicated_eventual(vec![node_id.clone()]);
    let operation_id = sinomo_core::Id::generate().to_string();
    let error = dfs
        .posix_write_file_with_operation_id(path, payload, options.clone(), &operation_id)
        .expect_err("unavailable operation-bound metadata quorum must reject the create");
    assert!(
        matches!(&error, DfsPosixError::Dfs(DfsError::MetadataRecoveryQuorumUnavailable {
        file_system_id, shard_id, required: 2, verified: 1,
    }) if file_system_id == &shard.file_system_id && shard_id == &shard.shard_id),
        "unexpected authorization error: {error}"
    );
    for replica in &shard.replica_node_ids {
        assert!(
            runtime.rpc_observations()[observations_before..]
                .iter()
                .any(|event| {
                    event.source == node_id
                        && &event.target == replica
                        && matches!(
                            event.route.as_str(),
                            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                                | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                        )
                        && event.phase == RpcPhase::EndedWithoutResponse
                }),
            "no actual failed metadata carrier observation for {replica}"
        );
    }
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
    // Overlay cleanup does not revoke the actual retained preparation. This
    // foreground Core write first prepares its exact pending intent/receipt.
    let snapshot = dfs
        .metadata_backend()
        .unwrap()
        .export_table_snapshot()
        .unwrap();
    let retained: Vec<_> = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.payload.metadata_effects.iter().any(|effect| {
                matches!(effect,
            crate::DfsPreparedMetadataEffect::PutIntent { intent }
            if intent.operation_id.as_deref() == Some(operation_id.as_str())
                && intent.object_metadata.as_ref().is_some_and(|object| object.path == path))
            })
        })
        .cloned()
        .collect();
    assert_eq!(
        retained.len(),
        1,
        "the actual write owns one retained preparation"
    );
    let retained = &retained[0];
    assert_eq!(retained.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(retained.committed_entry.is_none());
    let intent = retained
        .payload
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            crate::DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
            _ => None,
        })
        .unwrap();
    assert_eq!(intent.file_system_id, shard.file_system_id);
    assert_eq!(intent.owner_epoch, shard.epoch);
    assert_eq!(intent.coordinator_node_id, node_id);
    assert_eq!(
        (
            intent.policy.min_commit_replicas,
            intent.policy.desired_replicas
        ),
        (1, 1)
    );
    let expected_hash = format!("blake3:{}", blake3::hash(payload).to_hex());
    assert_eq!(intent.content_hash.as_deref(), Some(expected_hash.as_str()));
    runtime.clear_faults();
    dfs.refresh_control_plane_latest()
        .expect("ordinary configuration recovery");
    // Normal hydration recovers the SAME captured body with fresh real
    // operation-bound authority; no test-created guard or abort is supplied.
    dfs.hydrate_from_metadata_backend()
        .expect("ordinary retained prepare recovery");
    let recovered = dfs
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&retained.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.digest, retained.digest);
    assert_eq!(recovered.payload, retained.payload);
    assert_eq!(
        recovered.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert!(recovered.committed_entry.is_some());
    let committed = dfs
        .posix_write_file_with_operation_id(path, payload, options, &operation_id)
        .expect("retry after publication authority recovery");
    assert_eq!(committed.path, path);
    assert_eq!(
        dfs.posix_read_file(path).expect("read recovered write"),
        payload
    );
    drop(dfs);
    runtime.close().expect("ordinary native close");
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

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let suppression = DfsMetadataReplicationSuppressionRecord {
        code: Some(DfsMetadataRejectionCode::PrimaryMismatch),
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
        batch: DfsMetadataJournalFrameBatch {
            shard_id: DfsMetadataShardId::new("root"),
            journal_frames: Vec::new(),
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
fn metadata_authority_probe_rejects_a_gap_before_the_hidden_prepare() {
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    let nodes = DfsTestRuntime::ordered_node_ids("native-authority-gap", 3);
    let primary = nodes[0].clone();
    let replica = nodes[1].clone();
    let mut runtime = DfsTestRuntime::new("native-authority-gap", primary.clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let file_system = runtime.file_system();
    let mut source = runtime.client(&primary).unwrap();
    let dispatch = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: None,
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(dispatch.clone()),
    });
    let receiver = LocalSobjMetadataBackend::new(
        shard_id.clone(),
        default_dfs_metadata_shards_root(runtime.data_dir(&replica)).join(&shard_id.0),
    )
    .unwrap();
    let call = |command: &DfsMetadataJournalBatchCommand| {
        let request = dfs_metadata_journal_batch_rpc_request(command).unwrap();
        decode_dfs_metadata_journal_batch_rpc_response(
            &runtime
                .rpc_request(&primary, &replica, request, Duration::from_secs(5))
                .unwrap(),
        )
        .unwrap()
    };
    // Synchronize the existing real prefix before introducing two new hidden
    // operations. Automatic traffic remains paused; explicit calls still use
    // the authenticated native registry and persisted receiver.
    let mut base_head = 0;
    loop {
        let backend = source.metadata_backend.as_ref().unwrap();
        let Some(batch) = backend
            .journal_batch_for_shard_since_limited(
                &shard_id,
                base_head,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
        else {
            break;
        };
        let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
            backend.as_ref(),
            &batch,
        )
        .unwrap();
        let command = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
            &primary,
            &replica,
            &shard.file_system_id,
            shard.epoch,
            frames,
            false,
        );
        let response = call(&command);
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(
            response.verified_authority_probe_id,
            Some(
                command
                    .durable_replication
                    .as_ref()
                    .unwrap()
                    .confirmation_id
                    .clone()
            )
        );
        base_head = batch.next_after_sequence;
    }
    let settle = Instant::now();
    while receiver.metadata_replay_cursor(&shard_id).unwrap() < base_head {
        assert!(
            settle.elapsed() < Duration::from_secs(5),
            "native baseline did not materialize"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let prepare = |service: &mut NetworkedDfsService<DfsNodeTransport>, operation_id: &str| {
        service
            .metadata_backend
            .as_mut()
            .unwrap()
            .prepare_operation(
                DfsPreparedOperationPayload::for_metadata_effects(
                    DfsPreparedOperationScope {
                        file_system_id: shard.file_system_id.clone(),
                        shard_id: shard_id.clone(),
                        owner_epoch: shard.epoch,
                        operation_id: operation_id.into(),
                    },
                    Vec::new(),
                    Vec::new(),
                    vec![DfsPreparedMetadataEffect::PutFileSystem {
                        file_system: file_system.clone(),
                    }],
                    OffsetDateTime::now_utc(),
                )
                .unwrap(),
                None,
            )
            .unwrap()
    };
    let first = prepare(&mut source, "authority-gap-missing-prefix");
    let second = prepare(&mut source, "authority-gap-hidden-prepare");
    assert_eq!(first.prepare_sequence, base_head + 1);
    assert_eq!(second.prepare_sequence, first.prepare_sequence + 1);
    let backend = source.metadata_backend.as_ref().unwrap();
    let suffix = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            first.prepare_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(suffix.entries.len(), 1);
    let frames = crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
        backend.as_ref(),
        &suffix,
    )
    .unwrap();
    let prepared_prefix = backend.metadata_prefix_proof(&shard_id).unwrap().unwrap();
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "native-authority-gap-probe".into(),
        expected_shard: shard.clone(),
        operation: DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: second.operation_id.clone(),
            file_system_id: second.file_system_id.clone(),
            shard_id: second.shard_id.clone(),
            owner_epoch: second.owner_epoch,
            prepare_sequence: second.prepare_sequence,
            payload_digest: second.payload_digest.clone(),
        },
        prepared_prefix: prepared_prefix.clone(),
    };
    let command = DfsMetadataJournalBatchCommand {
        request_id: "native-authority-gap-request".into(),
        source_node_id: Some(primary.clone()),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: Some(probe.clone()),
        durable_replication: None,
        batch: frames,
    };
    let rejected = call(&command);
    assert!(matches!(
        rejected.result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    assert_eq!(rejected.verified_authority_probe_id, None);
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    assert_eq!(
        receiver
            .prepared_operation_record(&second.operation_id)
            .unwrap(),
        None
    );
    let pause = runtime
        .pause_replica_materialization(&replica, &shard_id)
        .unwrap();
    let missing = backend
        .journal_batch_for_shard_since_limited(
            &shard_id,
            base_head,
            1,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert_eq!(missing.entries[0].sequence, first.prepare_sequence);
    let catchup = crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &primary,
        &replica,
        &shard.file_system_id,
        shard.epoch,
        crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
            backend.as_ref(),
            &missing,
        )
        .unwrap(),
        false,
    );
    let ack = call(&catchup);
    assert_eq!(ack.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        ack.verified_authority_probe_id,
        Some(
            catchup
                .durable_replication
                .as_ref()
                .unwrap()
                .confirmation_id
                .clone()
        )
    );
    let capture = runtime
        .wait_paused_materialization(&pause, Duration::from_secs(5))
        .unwrap();
    assert_eq!(capture.unavailable, None);
    assert_eq!(capture.durable_sequence, Some(first.prepare_sequence));
    assert_eq!(capture.materialized_sequence, Some(base_head));
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    assert_eq!(
        receiver
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap()
            .journal_sequence,
        first.prepare_sequence
    );
    // The real authority receiver accepts the contiguous durable suffix even
    // while the rebuildable projection remains paused at the older prefix.
    let accepted = call(&command);
    assert_eq!(accepted.result, DfsMetadataJournalBatchResult::Applied);
    assert_eq!(
        accepted.verified_authority_probe_id,
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
    );
    assert_eq!(
        receiver
            .metadata_authority_ack_floor(&shard_id)
            .unwrap()
            .unwrap()
            .journal_sequence,
        second.prepare_sequence
    );
    assert_eq!(
        receiver.metadata_replay_cursor(&shard_id).unwrap(),
        base_head
    );
    pause.release();
    drop(pause);
    dispatch.close();
    runtime.clear_faults();
    drop(source);
    drop(receiver);
    drop(call);
    runtime
        .close()
        .expect("join actual native ownership and cleanup");
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

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let desired_shard = DfsMetadataShardConfig {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.0.clone(),
        primary_node_id: primary.clone(),
        replica_node_ids: vec![desired_replica.clone()],
        epoch: 9,
        assignment: DfsMetadataShardAssignment::Root,

        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let context = DfsMetadataReplicaShardContext {
        local_node_id: desired_replica,
        shard: retiring_shard.clone(),
        pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
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
        batch: DfsMetadataJournalFrameBatch {
            shard_id,
            journal_frames: Vec::new(),
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
fn distributed_write_uses_path_selected_metadata_shard_epoch() {
    let cluster = sifs_two_primary_cluster("native-path-shard-write-epoch");
    let SifsTwoPrimaryCluster {
        _runtime: runtime,
        services_primary_node,
        mut services_primary,
        replica_node,
        ..
    } = cluster;
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap();
    let written = services_primary
        .posix_write_file(
            "/services/path-shard.txt",
            b"path shard bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![services_primary_node, replica_node],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let backend = services_primary.metadata_backend().unwrap();
    let intent = backend
        .intents_for_object(&written.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| intent.target_version == written.version)
        .unwrap();
    assert_eq!(intent.owner_epoch, config.epoch);
    let record = backend
        .prepared_operation_record(&format!("write:{}", intent.intent_id))
        .unwrap()
        .unwrap();
    assert_eq!(record.digest.shard_id, DfsMetadataShardId::new("services"));
    assert_eq!(record.digest.owner_epoch, config.epoch);
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    drop(services_primary);
    runtime.close().unwrap();
}

#[test]
fn retry_pending_repairs_skips_sender_that_lost_metadata_primary_authority() {
    let name = "native-repair-lost-primary-sender";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let new_primary = peers[0].clone();
    let replica = peers[1].clone();
    let path = "/services/native-lost-primary-repair.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let mut old = runtime.client(&owner).unwrap();
    let stat = old
        .posix_write_file(
            path,
            b"actual old-primary repair bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let job = old
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == stat.object_id
                && job.version == stat.version
                && job.missing_replica_node_id == replica
        })
        .unwrap()
        .clone();
    assert!(old.writes.pending_writes.contains_key(&job.intent_id));
    let activated = runtime
        .reassign_metadata_shard(
            &stat.file_system_id,
            "services",
            new_primary.clone(),
            nodes
                .iter()
                .filter(|node| **node != new_primary)
                .cloned()
                .collect(),
        )
        .unwrap();
    assert_eq!(activated.primary_node_id, new_primary);
    let before = runtime.rpc_observations().len();
    assert!(old.retry_pending_repairs_bounded(1).unwrap().is_empty());
    assert!(!old.writes.pending_writes.contains_key(&job.intent_id));
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && [
                    crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
                    crate::DFS_DATA_FETCH_BLOB_ROUTE
                ]
                .contains(&event.route.as_str())
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "an actual retired metadata primary never dispatches repair after the normal handoff"
    );
    drop(old);
    runtime.clear_faults();
    runtime.close().unwrap();
}

fn native_negative_lost_authority_reply(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
) {
    let (runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        super::repair::native_committed_repair_backlog(name, false);
    runtime.clear_faults();
    assert_eq!(service.retry_pending_repairs_bounded(1).unwrap().len(), 1);
    let mut original = service
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find(|routed| {
            matches!(&routed.message,
            DfsNetworkMessage::RepairReplicaResponse(response) if response.intent_id == intent_id)
        })
        .unwrap();
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &mut original.message else {
        unreachable!()
    };
    assert_eq!(response.object_id, stat.object_id);
    assert_eq!(response.version, stat.version);
    assert_eq!(response.replica_node_id, replica);
    assert_eq!(
        service
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .repair_inflight_request_ids
            .get(&replica),
        Some(&response.request_id)
    );
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
        | DfsBlobTransferResult::Durable { content_hash: hash }
        if Some(hash.as_str()) == stat.content_hash.as_deref())
    );
    // Negative reply fault only. This does not claim that a handoff happened
    // or construct a different healthy authority tuple, receipt or permission.
    response.result = DfsBlobTransferResult::Rejected {
        reason: "source is not current metadata shard primary".into(),
    };
    let events = service
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RepairRejected { replica_node_id, reason, .. }
            if replica_node_id == &replica && reason.contains("source is not current metadata shard primary"))));
    service.observe_repair_maintenance_events(&events);
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    (runtime, owner, replica, service, stat, intent_id)
}

#[test]
fn repair_authority_rejection_suppresses_stale_retry_loop() {
    let (runtime, owner, replica, mut service, _stat, intent_id) =
        native_negative_lost_authority_reply("native-negative-authority-suppression");
    let before = runtime.rpc_observations().len();
    for _ in 0..100 {
        assert!(service.retry_pending_repairs_bounded(1).unwrap().is_empty());
    }
    let pending = service.writes.pending_writes.get(&intent_id).unwrap();
    let retry = pending.repair_retries.get(&replica).unwrap();
    assert_eq!(retry.next_retry_tick, DFS_REPAIR_RETRY_SUPPRESSED_TICK);
    assert!(!retry.inflight);
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica));
    assert!(!service.repair_retry_is_due(&intent_id, &replica));
    // Pure wake classification on a detached copy of the actual suppressed
    // state exercises the same helper used by Fetch rejection. It cannot mint
    // a fetch request or change any live runtime completion/authority state.
    let mut wake_input = retry.clone();
    crate::transfer::wake_repair_retry_state(&mut wake_input);
    assert_eq!(wake_input.next_retry_tick, DFS_REPAIR_RETRY_SUPPRESSED_TICK);
    assert!(!wake_input.inflight);
    assert!(runtime.rpc_observations()[before..].iter().all(|event| {
        !(event.source == owner
            && event.target == replica
            && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                .contains(&event.route.as_str())
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    }));
    drop(service);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_attempt_restore_preserves_lost_authority_suppression() {
    let (mut runtime, owner, replica, service, stat, intent_id) =
        native_negative_lost_authority_reply("native-negative-authority-restore");
    let records = service.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent_id, intent_id);
    assert_eq!(records[0].missing_replica_node_id, replica);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(
        records[0].next_retry_unix_nanos,
        u64::MAX,
        "the actual current negative-reply consumer produced lost-authority suppression"
    );
    assert!(
        records[0]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("source is not current metadata shard primary"))
    );
    drop(service);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut restored = runtime.client(&owner).unwrap();
    let before = runtime.rpc_observations().len();
    let scheduled = restored
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .unwrap();
    assert!(scheduled.is_empty());
    assert!(!restored.repair_retry_is_due(&intent_id, &replica));
    let retained = restored.active_repair_attempt_records();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        retained[0].next_retry_unix_nanos,
        records[0].next_retry_unix_nanos
    );
    assert_eq!(retained[0].last_error, records[0].last_error);
    assert!(runtime.rpc_observations()[before..].iter().all(|event| {
        !(event.source == owner
            && event.target == replica
            && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                .contains(&event.route.as_str())
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
    }));
    drop(restored);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_command_accepts_newer_authority_shard_when_receiver_view_is_stale() {
    let name = "native-repair-new-authority-projection-gap";
    let (runtime, initial, old_config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let old_primary = old_config.primary_node_id.clone();
    drop(initial);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != old_primary)
        .cloned()
        .collect::<Vec<_>>();
    let current_primary = peers[0].clone();
    let target = peers[1].clone();
    let path = "/services/native-stale-configuration-repair.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![old_primary.clone(), target.clone()];
    runtime
        .apply_placement_policy(&old_primary, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    deny_native_read_repair(&runtime, &old_primary, &target);
    let mut source = runtime.client(&old_primary).unwrap();
    let bytes = vec![0x79; 2 * 1024 * 1024 + 4096];
    let stat = source
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![old_primary.clone(), target.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let job = source
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == stat.object_id
                && job.version == stat.version
                && job.missing_replica_node_id == target
        })
        .unwrap()
        .clone();
    let intent = source
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &job.intent_id)
        .unwrap()
        .unwrap();
    let mut next = runtime.client(&current_primary).unwrap();
    assert_eq!(
        next.request_remote_object_read(&stat.object_id).unwrap(),
        vec![old_primary.clone()]
    );
    next.process_incoming().unwrap();
    assert_eq!(
        next.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes,
        "the future primary obtains only ordinary verified cache bytes before authority changes"
    );
    let target_before = runtime.client(&target).unwrap();
    let target_content = target_before.content().clone();
    let target_head = target_before
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let configs = runtime.metadata_shards().unwrap();
    let retiring = configs
        .iter()
        .find(|config| config.shard_id == "services")
        .unwrap()
        .clone();
    assert_eq!(retiring.primary_node_id, old_primary);
    let key = crate::control_plane::metadata_shard_membership_handoff_key(
        &stat.file_system_id,
        "services",
    );
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(&target, key.clone())
        .unwrap();
    let handoff = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&retiring, &current_primary)
        })
        .unwrap()
        .unwrap();
    // The third receiver owns no fabricated replacement view. Normal joint
    // handoff can activate through the two actual assignment majorities and
    // both real primaries while its configuration rebuild remains withheld.
    source
        .reconcile_pending_metadata_shard_membership_handoffs(
            &configs,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        gap.dropped_delta()
            .unwrap()
            .events
            .iter()
            .any(|event| event.key == key)
    );
    let activated = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&stat.file_system_id, "services")
        })
        .unwrap()
        .unwrap();
    assert_eq!(activated, handoff.desired_shard);
    assert_eq!(activated.primary_node_id, current_primary);
    assert!(activated.epoch > retiring.epoch);
    assert!(
        target_content
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err()
    );
    next.rebind_completed_sifs_metadata_shard_membership_handoffs()
        .unwrap();
    let current = next.current_read_object_metadata(&stat.object_id).unwrap();
    let request = next.next_request_id();
    let targets = vec![(target.clone(), request.clone())];
    let producer_intent = intent.clone();
    let producer_stat = stat.clone();
    let producer_bytes = bytes.len() as u64;
    let observations = runtime.rpc_observation_reader();
    let replies_before = runtime.rpc_observations().len();
    let repair = std::thread::spawn(move || {
        let sent = next.send_repair_replica_attachments_if_routable(
            &targets,
            &producer_intent.intent_id,
            &producer_stat.file_system_id,
            &producer_stat.object_id,
            producer_intent.base_version,
            &producer_stat.version,
            producer_stat.content_hash.as_deref().unwrap(),
            producer_bytes,
            Some(current),
        );
        (next, sent)
    });
    let reached = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            loop {
                if observations()[replies_before..].iter().any(|event| {
                    event.source == current_primary
                        && event.target == target
                        && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
    });
    let missing_before_refresh = target_content
        .read_object_version(stat.object_id.as_str(), stat.version.as_str())
        .is_err();
    // Only the real paused rebuild is released. The already-issued normal
    // current Repair must wait for native readiness/ReadIndex before ingest.
    gap.release();
    let (mut next, sent) = repair.join().unwrap();
    reached.unwrap();
    assert!(
        missing_before_refresh,
        "new authority hints cannot bypass the receiver's withheld current read"
    );
    assert!(sent.unwrap()[0].sent);
    let returned = next.transport.drain_incoming_messages().unwrap();
    assert!(returned.iter().any(|routed| routed.source_node_id == target
        && matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if response.request_id == request && response.object_id == stat.object_id
                && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash } if Some(content_hash) == stat.content_hash.as_ref()))));
    let refreshed_target = runtime.client(&target).unwrap();
    assert!(
        runtime.rpc_observations()[replies_before..]
            .iter()
            .any(|event| event.source == current_primary
                && event.target == target
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    assert_eq!(
        target_content
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        refreshed_target
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .version,
        target_head.version,
        "receiver repair cannot invent another authoritative visible generation"
    );
    runtime.clear_faults();
    drop(refreshed_target);
    drop(target_before);
    drop(next);
    drop(source);
    drop(control);
    drop(target_content);
    drop(gap);
    runtime.close().unwrap();
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
    let replica_delivery = Arc::new(tokio::sync::Semaphore::new(0));
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.services_primary_node.clone()),
            target: Some(cluster.replica_node.clone()),
            route: Some("/dfs/metadata/journal-batch".to_string()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(
                &replica_delivery,
            )),
        });
    // Advance the actual primary through the current kernel while delivery to
    // this requester is paused. The third live metadata member supplies the
    // genuine recovery-majority evidence; no replica table is rewritten.
    cluster
        .services_primary
        .posix_mkdir_all(&format!("{parent_path}/authority-advance"))
        .expect("real authoritative parent advance");
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
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
    _runtime.clear_faults();
    replica_delivery.add_permits(64);
    drop(services_primary);
    drop(replica);
    drop(shared_kv);
    _runtime.close().expect("normal fixture shutdown");
}

#[test]
fn routed_namespace_rejects_stale_authority_stamp_before_mutation() {
    let cluster = sifs_two_primary_cluster("sifs-namespace-stale-authority-stamp");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
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
    let name = "native-authority-local-follower-incremental";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/users").unwrap();
    let primary_node = config.primary_node_id;
    drop(initial);
    let follower_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let path = "/users/native-bootstrap-incremental";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    policy.required_node_ids = vec![primary_node.clone()];
    runtime
        .apply_placement_policy(&primary_node, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut primary = runtime.client(&primary_node).unwrap();
    let mut follower = runtime.client(&follower_node).unwrap();
    let before_bootstrap = follower
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();
    follower.bootstrap_distributed_sifs().unwrap();
    assert_eq!(
        follower
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap(),
        before_bootstrap,
        "normal follower bootstrap must retain actual inherited prefixes without authoring a divergent namespace"
    );
    primary.posix_mkdir(path).unwrap();
    let before = runtime.rpc_observations().len();
    for (index, bytes) in [
        (0, b"first actual incremental payload".as_slice()),
        (1, b"second actual incremental payload".as_slice()),
    ] {
        let file = format!("{path}/record-{index}");
        let stat = primary
            .posix_write_file(
                &file,
                bytes,
                DfsWriteOptions::replicated_eventual(vec![primary_node.clone()]),
            )
            .unwrap();
        let publication = runtime
            .maintenance_tick(&primary_node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
        assert_eq!(publication.omitted_kv_event_count, 0);
        assert_eq!(follower.posix_stat(&file).unwrap().version, stat.version);
        assert_eq!(follower.posix_read_file_distributed(&file).unwrap(), bytes);
    }
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert!(calls.iter().any(|event| {
        event.source == primary_node
            && event.target == follower_node
            && [
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            ]
            .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::Completed
    }));
    assert!(
        !calls.iter().any(|event| event.source == primary_node
            && event.target == follower_node
            && [
                crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE
            ]
            .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "actual authority-local bootstrap keeps normal follow-up writes on incremental publication"
    );
    drop(follower);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn served_routed_write_committed_work_tracks_only_authority_owned_repairs() {
    for desired in [1, 2] {
        let name = format!("native-authority-served-work-n{desired}");
        let (runtime, primary, replica, requester, base, _) =
            native_read_fixture(&name, 1, false, b"actual routed predecessor".to_vec());
        let nodes = if desired == 1 {
            vec![requester.clone()]
        } else {
            vec![requester.clone(), replica.clone()]
        };
        let mut placement =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, desired).unwrap();
        placement.required_node_ids = nodes.clone();
        runtime
            .apply_placement_policy(&primary, &base.path, DfsPlacementScope::Subtree, placement)
            .unwrap();
        deny_native_read_repair(&runtime, &primary, &replica);
        let capture = runtime.capture_served_work(&primary, 32).unwrap();
        let mut client = runtime.client(&requester).unwrap();
        let request = DfsWriteRequest {
            file_system_id: base.file_system_id,
            object_id: base.object_id,
            target_version: crate::namespace::next_version(&base.version),
            requesting_node_id: requester.clone(),
        };
        let bytes = vec![0x77; 2 * 1024 * 1024 + 19];
        let operation = sinomo_core::Id::generate().to_string();
        let mut policy = DfsReplicationPolicy::new(1, desired).unwrap();
        policy.required_node_ids = nodes.clone();
        client
            .write_distributed_bytes_with_base(
                request.clone(),
                Some(base.version),
                bytes.clone(),
                nodes,
                policy,
                DfsDistributedWriteOptions {
                    operation_id: Some(operation.clone()),
                    repair: DfsWriteRepair::Background,
                    ..DfsDistributedWriteOptions::default()
                },
            )
            .unwrap();
        let observed = capture.records().into_iter().find(|row| matches!(
            &row.command.routed_message.message,
            DfsNetworkMessage::DistributedWriteRequest(command)
                if command.request_id == operation && command.request.object_id == request.object_id))
            .expect("same native routed operation's actual Main report");
        assert_eq!(observed.command.routed_message.source_node_id, requester);
        assert_eq!(observed.command.routed_message.target_node_id, primary);
        assert!(observed.succeeded);
        assert!(observed.committed_work.publish_metadata);
        assert_eq!(observed.committed_work.repair_maintenance, desired == 2);
        assert_eq!(observed.repair_record_count, usize::from(desired == 2));
        assert!(observed.new_repair_records_unattempted);
        assert!(!observed.committed_work.metadata_replica_progress);
        let terminal = client
            .transport
            .drain_incoming_messages()
            .unwrap()
            .into_iter()
            .find_map(|routed| match routed.message {
                DfsNetworkMessage::DistributedWriteResponse(response)
                    if response.request_id == operation =>
                {
                    Some(response)
                }
                _ => None,
            })
            .unwrap();
        let DfsDistributedWriteResult::Outcome {
            outcome:
                NetworkedDfsWriteOutcome::Committed {
                    intent_id,
                    repair_jobs,
                    version,
                    content_hash: hash,
                },
        } = &terminal.result
        else {
            panic!("actual W1 native publication required")
        };
        assert_eq!(version, &request.target_version);
        assert_eq!(hash, &content_hash(&bytes));
        assert_eq!(repair_jobs.len(), usize::from(desired == 2));
        let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
            runtime.data_dir(&primary),
        ))
        .view()
        .unwrap();
        let owned = queue
            .records
            .iter()
            .filter(|record| {
                record.intent_id == *intent_id
                    && record.object_id == request.object_id
                    && record.version == request.target_version
            })
            .collect::<Vec<_>>();
        assert_eq!(owned.len(), usize::from(desired == 2));
        if let Some(record) = owned.first() {
            assert_eq!(record.missing_replica_node_id, replica);
        }
        drop(capture);
        drop(client);
        runtime.clear_faults();
        runtime.close().unwrap();
    }
}

#[test]
fn served_replica_stage_does_not_claim_authority_repair_work() {
    let name = "native-replica-stage-served-work";
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = shard.primary_node_id;
    let replica = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary)
        .unwrap();
    drop(initial);
    let path = "/services/native-replica-work.bin";
    let mut first_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    first_policy.required_node_ids = vec![primary.clone()];
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, first_policy)
        .unwrap();
    let mut first = runtime.client(&primary).unwrap();
    let base = first
        .posix_write_file(
            path,
            b"actual stage predecessor",
            DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(first);
    let nodes = vec![primary.clone(), replica.clone()];
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    target_policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, target_policy)
        .unwrap();
    let capture = runtime.capture_served_work(&replica, 32).unwrap();
    let mut owner = runtime.client(&primary).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: primary.clone(),
    };
    let bytes = vec![0x65; 2 * 1024 * 1024 + 29];
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    assert!(matches!(
        owner
            .write_distributed_reader_with_candidates(
                request.clone(),
                std::io::Cursor::new(bytes.clone()),
                nodes,
                policy,
                DfsDistributedWriteOptions::default()
            )
            .unwrap(),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let original = owner
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find_map(|routed| match routed.message {
            DfsNetworkMessage::StageBlobResponse(response)
                if response.object_id == request.object_id =>
            {
                Some(response)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(original.replica_node_id, replica);
    assert_eq!(original.version, request.target_version);
    assert!(
        matches!(&original.result, DfsBlobTransferResult::Durable { content_hash: hash }
        if hash == &content_hash(&bytes))
    );
    let observed = capture
        .records()
        .into_iter()
        .find(|row| {
            matches!(
        &row.command.routed_message.message,
        DfsNetworkMessage::StageBlob(command) if command.request_id == original.request_id
            && command.intent_id == original.intent_id && command.object_id == request.object_id)
        })
        .expect("same actual Stage RPC's Main report");
    assert_eq!(observed.command.routed_message.source_node_id, primary);
    assert_eq!(observed.command.routed_message.target_node_id, replica);
    assert!(observed.succeeded);
    assert!(!observed.committed_work.repair_maintenance);
    assert_eq!(observed.repair_record_count, 0);
    let received = runtime.client(&replica).unwrap();
    assert_eq!(
        received
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(received);
    drop(capture);
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn served_requester_completion_does_not_claim_authority_repair_work() {
    let (runtime, primary, requester, replica, mut client, request, _bytes, original) =
        super::repair::native_routed_committed_backlog("native-requester-work-completion", false);
    let DfsNetworkMessage::DistributedWriteResponse(response) = &original.message else {
        unreachable!()
    };
    let request_id = response.request_id.clone();
    assert_eq!(original.source_node_id, primary);
    assert_eq!(original.target_node_id, requester);
    assert_eq!(response.object_id, request.object_id);
    assert!(
        client
            .writes
            .pending_routed_writes
            .contains_key(&request_id)
    );
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed { repair_jobs, .. },
    } = &response.result
    else {
        panic!("actual current primary committed response required")
    };
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    // The response and pending owner both came from the same normal native
    // request. Use the same work capture and normal consumer as the served lane.
    client.begin_message_committed_work();
    let DfsNetworkMessage::DistributedWriteResponse(response) = original.message else {
        unreachable!()
    };
    let event = client
        .handle_distributed_write_response(primary.clone(), response)
        .unwrap();
    let committed_work = client.take_message_committed_work();
    assert!(matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id: id, .. }
            if id == request_id));
    assert!(
        !committed_work.repair_maintenance,
        "observing another node's actual repair backlog does not grant requester ownership"
    );
    assert!(client.coordinator().repair_jobs().is_empty());
    let mut authority = runtime.client(&primary).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    assert!(authority.coordinator().repair_jobs().iter().any(|job|
        job.object_id == request.object_id && job.missing_replica_node_id == replica));
    drop(authority);
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}
