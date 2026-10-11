//! Recovery regression tests.

use super::*;

#[test]
fn dfs_request_ids_remain_unique_across_service_restarts() {
    let node = NodeId::from_legacy_named("node_request_id_restart").expect("node");
    let first_root = temp_root("request-id-first");
    let restarted_root = temp_root("request-id-restarted");
    let mut first = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(&first_root).expect("first content"),
    );
    let mut restarted = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node),
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
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
        "native-bounded-tombstone-maintenance",
        1,
    );
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "native-bounded-tombstone-maintenance",
        nodes[0].clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&nodes[0]).unwrap();
    let mut host = RecordingMaintenanceHost::default();
    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions {
            compact_tombstones: true,
            tombstone_compaction_min_age: Duration::from_nanos(17),
            tombstone_compaction_max_records: 23,
            ..Default::default()
        },
    )
    .unwrap();
    let report = reports
        .iter()
        .find_map(|report| report.tombstone_compaction.as_ref())
        .expect("native bounded collection report");
    assert_eq!(report.min_tombstone_age_nanos, 17);
    assert_eq!(report.max_compactions, Some(23));
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn metadata_prefix_probe_accepts_only_the_exact_materialized_checkpoint() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-exact-prefix-probe", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-exact-prefix-probe", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let source = runtime.root_shard().unwrap();
    let target = source
        .replica_node_ids
        .iter()
        .find(|node| **node != source.primary_node_id)
        .unwrap()
        .clone();
    let shard_id = DfsMetadataShardId::new(source.shard_id.clone());
    let receiver = runtime.client(&target).unwrap();
    let cut = receiver
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    drop(receiver);
    let mut command = DfsMetadataJournalBatchCommand {
        request_id: "native-exact-prefix".into(),
        source_node_id: Some(source.primary_node_id.clone()),
        file_system_id: Some(source.file_system_id.clone()),
        authority_epoch: Some(source.epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id: "native-prefix".into(),
            file_system_id: Some(source.file_system_id.clone()),
            authority_epoch: Some(source.epoch),
            shard_id: shard_id.clone(),
            journal_sequence: cut.prefix_proof.journal_sequence,
            state_fingerprint: cut.prefix_proof.digest.clone(),
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            journal_frames: Vec::new(),
            next_after_sequence: cut.prefix_proof.journal_sequence,
        },
    };
    let request = dfs_metadata_journal_batch_rpc_request(&command).unwrap();
    let reply = runtime
        .rpc_request(
            &source.primary_node_id,
            &target,
            request,
            Duration::from_secs(5),
        )
        .unwrap();
    let response = decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
    assert!(response.verified_prefix_only);
    command.prefix_probe.as_mut().unwrap().state_fingerprint="dfs-metadata-prefix-v2:blake3:0000000000000000000000000000000000000000000000000000000000000000".into();
    let request = dfs_metadata_journal_batch_rpc_request(&command).unwrap();
    let reply = runtime
        .rpc_request(
            &source.primary_node_id,
            &target,
            request,
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(matches!(
        decode_dfs_metadata_journal_batch_rpc_response(&reply)
            .unwrap()
            .result,
        DfsMetadataJournalBatchResult::Rejected { .. }
    ));
    let after = runtime
        .client(&target)
        .unwrap()
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, cut.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        cut.semantic_state_fingerprint
    );
    runtime.close().unwrap();
}

#[test]
fn persisted_repair_attempts_prevent_duplicate_retry_after_reopen() {
    let name = "native-captured-repair-attempt-service-reopen";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    let replica = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != owner)
        .unwrap();
    let stat = dfs
        .posix_write_file(
            "/services/native-recorded-backoff",
            b"actual repair attempt bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(dfs.coordinator().repair_jobs().len(), 1);
    let scheduled = dfs.retry_pending_repairs_bounded(1).unwrap();
    assert_eq!(scheduled.len(), 1);
    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(records[0].version, stat.version);
    assert_eq!(records[0].content_hash, stat.content_hash.clone().unwrap());
    assert_eq!(records[0].missing_replica_node_id, replica);
    assert!(records[0].in_flight);
    assert!(records[0].transfer_request_id.is_some());
    assert!(records[0].next_retry_unix_nanos > dfs_now_unix_nanos());
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "the captured retry must follow a real hosted receiver"
    );
    // Drop this caller before its queued response is consumed. The actual
    // returned in-flight record is restart bookkeeping, never a receipt or
    // permission to run repair without current native authority.
    drop(dfs);
    let mut reopened = runtime.client(&owner).unwrap();
    let before = runtime.rpc_observations().len();
    assert!(records[0].next_retry_unix_nanos > dfs_now_unix_nanos());
    assert!(
        reopened
            .retry_pending_repairs_bounded_with_attempts(1, &records)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.operational_metrics().unwrap().repair_attempt_count,
        1
    );
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch)
    );
    let peer = runtime.client(&replica).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        b"actual repair attempt bytes"
    );
    drop(peer);
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn persisted_repair_attempts_prevent_duplicate_fetch_after_reopen() {
    let name = "native-captured-fetch-attempt-service-reopen";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id;
    let other = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let source = other[0].clone();
    let target = other[1].clone();
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(target.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let path = "/services/native-fetch-backoff.bin";
    let bytes = (0..2 * 1024 * 1024 + 17)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let stat = dfs
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 3).unwrap(),
                replica_candidates: vec![primary.clone(), source.clone(), target.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), source.clone()])
    );
    assert_eq!(dfs.coordinator().repair_jobs().len(), 1);
    let hash = stat.content_hash.clone().unwrap();
    dfs.content().debug_remove_loose_blob(&hash).unwrap();
    assert!(
        dfs.content().open_blob_stream(&hash).is_err(),
        "fault removes the actual local bytes"
    );
    let before_fetch = runtime.rpc_observations().len();
    assert!(dfs.retry_pending_repairs_bounded(1).unwrap().is_empty());
    assert!(
        runtime.rpc_observations()[before_fetch..]
            .iter()
            .any(|event| event.source == primary
                && event.target == source
                && event.route == crate::DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(records[0].version, stat.version);
    assert_eq!(records[0].content_hash, hash);
    assert_eq!(records[0].missing_replica_node_id, target);
    assert!(records[0].in_flight);
    assert!(records[0].transfer_request_id.is_some());
    assert!(records[0].next_retry_unix_nanos > dfs_now_unix_nanos());
    drop(dfs);
    let mut reopened = runtime.client(&primary).unwrap();
    let before_retry = runtime.rpc_observations().len();
    assert!(records[0].next_retry_unix_nanos > dfs_now_unix_nanos());
    assert!(
        reopened
            .retry_pending_repairs_bounded_with_attempts(1, &records)
            .unwrap()
            .is_empty()
    );
    assert!(
        !runtime.rpc_observations()[before_retry..]
            .iter()
            .any(|event| event.source == primary
                && event.target == source
                && event.route == crate::DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch)
    );
    let actual_source = runtime.client(&source).unwrap();
    assert_eq!(
        actual_source
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(actual_source);
    drop(reopened);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn recovery_hydration_can_cancel_between_synchronous_backend_phases() {
    let (runtime, mut service, _) = crate::test_support::current_native_metadata_service(
        "native-recovery-cancellation-between-phases",
        1,
        "/services",
    )
    .unwrap();
    let cancellation_checks = AtomicUsize::new(0);
    let before = service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let completed = service
        .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(&|| {
            cancellation_checks.fetch_add(1, Ordering::SeqCst) >= 1
        })
        .unwrap();
    assert!(!completed);
    assert!(
        cancellation_checks.load(Ordering::SeqCst) >= 2,
        "the normal recovery driver must recheck cancellation after synchronous native backend work"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn recovery_hydration_skips_fully_repaired_committed_pending_write() {
    let name = "native-fully-receipted-commit-recovery";
    let (mut runtime, mut owner_dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_COMMIT_VERSION_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::LoseReplyAfterDispatch,
    });
    let path = "/services/fully-receipted-recovery";
    let result = owner_dfs.posix_write_file(
        path,
        b"fully repaired bytes",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(2, 2).unwrap(),
            replica_candidates: vec![owner.clone(), replica.clone()],
            wait_for_commit: true,
            foreground_replica_repair: true,
        },
    );
    // Receiver execution is real. An uncertain sender response cannot undo the
    // already committed metadata/content that normal reconciliation observes.
    let stat = owner_dfs.posix_stat_distributed(path).unwrap();
    let receipts = owner_dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.content_hash.as_deref() == stat.content_hash.as_deref())
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        2,
        "W=2 must come from the actual two receivers, result={result:?}"
    );
    assert!(owner_dfs.writes.pending_writes.is_empty());
    assert!(owner_dfs.coordinator().repair_jobs().is_empty());
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse),
        "the lost response must follow actual native receiver execution"
    );
    let peer = runtime.client(&replica).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        b"fully repaired bytes"
    );
    drop(peer);
    drop(owner_dfs);
    runtime.clear_faults();
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut recovered = runtime.client(&owner).unwrap();
    let before = runtime.rpc_observations().len();
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    assert!(recovered.writes.pending_writes.is_empty());
    assert!(recovered.coordinator().repair_jobs().is_empty());
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let after_replay = runtime.rpc_observations().len();
    let replay_count = runtime.rpc_observations()[before..after_replay]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
        })
        .count();
    assert!(
        replay_count <= 1,
        "one exact current commit may be replayed, never a fabricated ACK"
    );
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    assert!(
        !runtime.rpc_observations()[after_replay..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "settled exact native commit must not be requeued by a later hydration"
    );
    assert_eq!(
        recovered.posix_read_file(path).unwrap(),
        b"fully repaired bytes"
    );
    drop(recovered);
    runtime.close().unwrap();
}

#[test]
fn hydrate_recovers_zero_byte_create_after_authoritative_view_refresh_failure_once() {
    let name = "native-zero-create-floor-return-failure";
    let (mut runtime, mut original, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id.clone();
    let parent_path = "/services/native-floor-recovery";
    let child_path = "/services/native-floor-recovery/empty.bin";
    original.posix_mkdir_all(parent_path).unwrap();
    let parent_before = original.posix_stat(parent_path).unwrap();
    let parent_versions_before = original
        .metadata_backend()
        .unwrap()
        .versions_for_object(&parent_before.object_id)
        .unwrap()
        .len();
    let missing_ack = config.replica_node_ids[0].clone();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(missing_ack),
        route: Some(DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let fault = runtime.metadata_operation_floor_fault(&owner).unwrap();
    let trigger = fault.operation_floor_denial_trigger(
        crate::control_plane::metadata_shard_operation_floor_key(
            &config.file_system_id,
            &config.shard_id,
        ),
    );
    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_| trigger()));
    });
    let operation_id = format!("cf_{}", sinomo_core::Id::generate());
    let error = original
        .posix_create_file_with_contents_and_attrs_and_operation_id(
            child_path,
            &[],
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            DfsPosixCreateAttrs::new(0o640, 6101, 6102),
            &operation_id,
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("retry the read"),
        "unexpected real failure: {error}"
    );
    assert!(
        fault.denied_operation_floor_reads() > 1,
        "the normal bounded authority retry must reach the actual floor boundary"
    );
    let backend = original.metadata_backend().unwrap();
    let prepared = backend
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record.status == DfsPreparedOperationStatus::Prepared
                && record
                    .transaction
                    .objects
                    .iter()
                    .any(|object| object.path == child_path)
        })
        .unwrap();
    let pending = prepared.prepared_intent.as_ref().unwrap();
    assert!(pending.touch_parent_on_create);
    assert_eq!(
        pending.content_hash.as_deref(),
        Some(content_hash(&[]).as_str())
    );
    assert_eq!(
        prepared.content_receipts.len(),
        1,
        "the actual empty bytes already meet W"
    );
    assert_eq!(prepared.content_receipts[0].replica_node_id, owner);
    assert!(
        backend
            .get_object_by_path(&sifs_file_system_id(), child_path)
            .unwrap()
            .is_none()
    );
    assert_eq!(original.posix_stat(parent_path).unwrap(), parent_before);
    let expected_child = prepared
        .transaction
        .objects
        .iter()
        .find(|object| object.path == child_path)
        .unwrap()
        .clone();
    drop(original);
    drop(fault);
    runtime.clear_faults();
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let mut recovered = runtime.client(&owner).unwrap();
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let child = recovered.posix_stat(child_path).unwrap();
    assert_eq!(child.object_id, expected_child.object_id);
    assert_eq!(child.version, expected_child.version);
    assert_eq!(child.size_bytes, Some(0));
    assert_eq!((child.mode, child.uid, child.gid), (0o640, 6101, 6102));
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_write_count,
        0
    );
    assert!(recovered.writes.pending_writes.is_empty());
    let parent_after = recovered.posix_stat(parent_path).unwrap();
    assert_ne!(parent_after.version, parent_before.version);
    assert!(parent_after.modified_at_unix_nanos > parent_before.modified_at_unix_nanos);
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .versions_for_object(&parent_before.object_id)
            .unwrap()
            .len(),
        parent_versions_before + 1
    );
    drop(recovered);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reopened = runtime.client(&owner).unwrap();
    assert_eq!(reopened.posix_stat(child_path).unwrap(), child);
    assert_eq!(reopened.posix_stat(parent_path).unwrap(), parent_after);
    assert_eq!(
        reopened
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_write_count,
        0
    );
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_survives_metadata_backend_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_recursive_clear_survives_metadata_backend_reopen",
        node_id.clone(),
    );
    drop(setup);
    let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
    let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

    {
        let mut dfs = runtime.client(&node_id).unwrap();
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

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let dfs = runtime.client(&node_id).unwrap();
        assert!(matches!(
            dfs.posix_read_file(file_path),
            Err(DfsPosixError::NotFound { .. })
        ));
    }

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
        dfs.posix_mkdir_all(clear_root)
            .expect("recreate cleared root");
        dfs.posix_write_file(
            file_path,
            b"fresh",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("rewrite after clear");
        assert_eq!(
            dfs.posix_read_file(file_path).expect("fresh read"),
            b"fresh"
        );
    }

    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_bounded_job_resumes_after_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear_job").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_recursive_clear_bounded_job_resumes_after_reopen",
        node_id.clone(),
    );
    drop(setup);
    let clear_root = "/services/sandbox/instances/sandbox_clear_job/tmp/0";
    let nested_dir = format!("{clear_root}/nested/deeper");
    let files = [
        format!("{clear_root}/root-cache.txt"),
        format!("{clear_root}/nested/cache-a.txt"),
        format!("{nested_dir}/cache-b.txt"),
        format!("{nested_dir}/cache-c.txt"),
    ];
    let job_id = {
        let mut dfs = runtime.client(&node_id).unwrap();
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

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
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

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let dfs = runtime.client(&node_id).unwrap();
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

    runtime.close().unwrap();
}

#[test]
fn sifs_posix_rename_directory_bounded_job_resumes_after_reopen() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_dir_job").expect("node");
    let (mut runtime, setup) = native_sifs_service(
        "sifs_posix_rename_directory_bounded_job_resumes_after_reopen",
        node_id.clone(),
    );
    drop(setup);
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
        let mut dfs = runtime.client(&node_id).unwrap();
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

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let mut dfs = runtime.client(&node_id).unwrap();
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

    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    {
        let dfs = runtime.client(&node_id).unwrap();
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

    runtime.close().unwrap();
}

#[test]
fn sifs_posix_unlink_tombstone_survives_fresh_shared_metadata_recovery() {
    let root = temp_root("sifs-unlink-shared-metadata-recovery");
    let metadata_root = root.join("metadata");
    let node = NodeId::from_legacy_named("node_sifs_unlink_shared_metadata").expect("node");
    let path = "/services/shared-unlink-recovery.txt";

    let mut runtime = crate::test_runtime::DfsTestRuntime::new(
        "sifs_posix_unlink_tombstone_survives_fresh_shared_metadata_recovery",
        node.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut seed = runtime.client(&node).unwrap();
    seed.bootstrap_distributed_sifs().expect("bootstrap sifs");
    seed.posix_write_file(
        path,
        b"delete me",
        DfsWriteOptions::replicated_eventual(vec![node.clone()]),
    )
    .expect("create file");

    let mut remover = runtime.client(&node).unwrap();
    let live_changed_at = remover
        .posix_lstat(path)
        .expect("live file stat")
        .changed_at_unix_nanos;
    let removed = remover.posix_unlink(path).expect("unlink file");
    assert!(
        removed.changed_at_unix_nanos > live_changed_at,
        "unlink tombstone ctime must fence historical namespace transactions"
    );

    let verifier = runtime.client(&node).unwrap();
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
fn sifs_cluster_writer_recovers_from_local_blob_loss_after_async_repair() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-writer-blob-loss", "/services");
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

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_write_survives_writer_restart() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-routed-writer-restart", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-routed-writer-restart", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/users/native-routed-restart.txt";
    let mut writer = runtime.client(&nodes[1]).unwrap();
    let stat = writer
        .posix_write_file(
            path,
            b"durable routed bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    writer
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    drop(writer);
    runtime.stop_node(&nodes[1]).unwrap();
    runtime.reopen_node(nodes[1].clone()).unwrap();
    let mut recovered = runtime.client(&nodes[1]).unwrap();
    assert_eq!(recovered.posix_stat(path).unwrap().version, stat.version);
    assert_eq!(
        recovered.posix_read_file_distributed(path).unwrap(),
        b"durable routed bytes"
    );
    drop(recovered);
    runtime.close().unwrap();
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
        let mut original = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(primary.clone()),
            content,
        )
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
        let transport = DfsNodeTransport::new_with_message_sender(
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
