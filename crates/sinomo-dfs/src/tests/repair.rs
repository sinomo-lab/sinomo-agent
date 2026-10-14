//! Repair regression tests.

use super::*;

// Real configuration precedes each fixture producer. Candidate hints do not
// create a managed W/N contract, and a browser owns no Core coordinator state.
fn native_owned_repair_fixture(
    name: &str,
    path: &str,
    min_commit: usize,
    desired: usize,
    deny_missing_content: bool,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    Vec<NodeId>,
    NetworkedDfsService<DfsNodeTransport>,
    DfsReplicationPolicy,
) {
    let (runtime, browser, config) =
        crate::test_support::current_native_metadata_service(name, desired, "/services").unwrap();
    drop(browser);
    let owner = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, desired);
    let expected_nodes = nodes.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        std::iter::once(owner.clone())
            .chain(config.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>(),
        expected_nodes
    );
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, min_commit, desired).unwrap();
    placement.required_node_ids = nodes.clone();
    let accepted = runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let observed = || {
        control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .expect("the exact accepted empty fixture policy remains observable")
    };
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    for _ in 0..2 {
        let current = observed();
        assert_eq!(current.job_id, accepted.job_id);
        assert_eq!(current.file_system_id, accepted.file_system_id);
        assert_eq!(current.path, path);
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.candidate_snapshot, accepted.candidate_snapshot);
        assert_eq!(current.placement_snapshot, accepted.placement_snapshot);
        assert_eq!(current.scanned_object_count, 0);
        assert_eq!(current.matched_object_count, 0);
        assert_eq!(current.scheduled_repair_count, 0);
        assert!(
            current.last_error.is_none(),
            "actual empty fixture Apply: {current:?}"
        );
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(matches!(
            current.state,
            DfsPlacementReconciliationState::Pending
                | DfsPlacementReconciliationState::Scanning
                | DfsPlacementReconciliationState::Repairing
        ));
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal empty fixture Apply budget");
        let tick = runtime.maintenance_tick(&owner, remaining).unwrap();
        assert!(
            tick.outcome.is_ok(),
            "normal empty fixture Apply: {:?}",
            tick.outcome
        );
        assert_eq!(tick.omitted_kv_event_count, 0);
    }
    let completed = observed();
    assert_eq!(completed.job_id, accepted.job_id);
    assert_eq!(completed.accepted_config, accepted.accepted_config);
    assert_eq!(completed.candidate_snapshot, accepted.candidate_snapshot);
    assert_eq!(completed.placement_snapshot, accepted.placement_snapshot);
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.total_object_count, 0);
    assert_eq!(completed.scanned_object_count, 0);
    assert_eq!(completed.matched_object_count, 0);
    assert_eq!(completed.scheduled_repair_count, 0);
    assert_eq!(completed.pending_repair_count, 0);
    assert!(completed.last_error.is_none());
    assert!(Instant::now() < deadline);
    drop(control);
    if deny_missing_content {
        for peer in nodes.iter().filter(|peer| **peer != owner) {
            for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
                runtime.install_fault(crate::test_support::DfsTestFaultRule {
                    source: Some(owner.clone()),
                    target: Some(peer.clone()),
                    route: Some(route.into()),
                    call_id: None,
                    origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
                    remaining_hits: 32,
                    action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
                });
            }
        }
    }
    let mut core = runtime.metadata_primary_service(path).unwrap();
    let (resolved, _, policy) = core
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, desired)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedEventual
    );
    assert_eq!(policy.min_commit_replicas, min_commit);
    assert_eq!(policy.desired_replicas, desired);
    assert_eq!(
        policy
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_nodes
    );
    (runtime, owner, nodes, core, policy)
}

pub(super) fn native_routed_committed_backlog(
    name: &str,
    foreground: bool,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    Vec<u8>,
    DfsRoutedMessage,
) {
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = shard.primary_node_id;
    drop(initial);
    let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let replica = peers[0].clone();
    let requester = peers[1].clone();
    let path = format!("/services/{name}.bin");
    let initial_holder = requester.clone();
    let nodes = vec![requester.clone(), replica.clone()];
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    // The routed W1/N2 regression starts with a genuinely Eventual object.
    // A later managed-policy update cannot rewrite a Confirmed predecessor.
    let mut seed = runtime.client(&requester).unwrap();
    let mut seed_policy = DfsReplicationPolicy::new(1, 2).unwrap();
    seed_policy.required_node_ids = nodes.clone();
    let base = seed
        .posix_write_file(
            &path,
            b"actual routed predecessor",
            DfsWriteOptions {
                replica_candidates: nodes.clone(),
                policy: seed_policy,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    assert_eq!(
        seed.current_read_object_metadata(&base.object_id)
            .unwrap()
            .policy,
        DfsObjectPolicy::ReplicatedEventual
    );
    drop(seed);
    deny_native_read_repair(&runtime, &primary, &replica);
    let unrelated_path = format!("/services/{name}-unrelated.bin");
    let mut unrelated_placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    unrelated_placement.required_node_ids = vec![primary.clone(), replica.clone()];
    runtime
        .apply_placement_policy(
            &primary,
            &unrelated_path,
            DfsPlacementScope::Subtree,
            unrelated_placement,
        )
        .unwrap();
    // The unrelated repair belongs to the genuine current Core producer,
    // while the requester below remains a browser client.
    let mut owner = runtime.metadata_primary_service(&unrelated_path).unwrap();
    let mut unrelated_policy = DfsReplicationPolicy::new(1, 2).unwrap();
    unrelated_policy.required_node_ids = vec![primary.clone(), replica.clone()];
    let unrelated = owner
        .posix_write_file(
            &unrelated_path,
            b"actual unrelated repair backlog",
            DfsWriteOptions {
                policy: unrelated_policy,
                replica_candidates: vec![primary.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert!(
        owner
            .coordinator()
            .repair_jobs()
            .iter()
            .any(|job| job.object_id == unrelated.object_id
                && job.missing_replica_node_id == replica)
    );
    drop(owner);
    let mut client = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x73; 2 * 1024 * 1024 + 17];
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = nodes;
    let operation = sinomo_core::Id::generate().to_string();
    let options = DfsDistributedWriteOptions {
        operation_id: Some(operation.clone()),
        repair: if foreground {
            DfsWriteRepair::Foreground
        } else {
            DfsWriteRepair::Background
        },
        ..DfsDistributedWriteOptions::default()
    };
    let outcome = if foreground {
        // The reader path durably captures on the selected requester before
        // advertising its manifest. A primary-owned byte stream has no such
        // requester receipt and cannot demonstrate an already-satisfied W1.
        client.write_distributed_reader_with_base(
            request.clone(),
            Some(base.version),
            std::io::Cursor::new(bytes.clone()),
            policy.required_node_ids.clone(),
            policy,
            options,
        )
    } else {
        client.write_distributed_bytes_with_base(
            request.clone(),
            Some(base.version),
            bytes.clone(),
            policy.required_node_ids.clone(),
            policy,
            options,
        )
    }
    .unwrap();
    assert_eq!(
        client
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(client.writes.pending_routed_writes.contains_key(&operation));
    let original = client
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find(|routed| {
            routed.source_node_id == primary
                && routed.target_node_id == requester
                && matches!(&routed.message,
                DfsNetworkMessage::DistributedWriteResponse(response)
                    if response.request_id == operation && response.object_id == request.object_id)
        })
        .expect("actual native current primary's owned completion frame");
    let DfsNetworkMessage::DistributedWriteResponse(response) = &original.message else {
        unreachable!()
    };
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash: hash,
                repair_jobs,
                ..
            },
    } = &response.result
    else {
        panic!("normal W1 receiver must publish before N2 repair: {response:?}")
    };
    assert_eq!(version, &request.target_version);
    assert_eq!(hash, &content_hash(&bytes));
    let author = runtime.metadata_primary_service(&path).unwrap();
    let receipts = author
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    assert_eq!(repair_jobs.len(), 1, "{response:?}");
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, initial_holder);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    drop(author);
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
    (
        runtime, primary, requester, replica, client, request, bytes, original,
    )
}

fn native_routed_manifest_write(
    name: &str,
    min_commit: usize,
    desired: usize,
    primary_is_desired: bool,
    foreground: bool,
    deny_third_target: bool,
    deny_primary_fetch: bool,
) -> (
    crate::test_support::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    ContentManifest,
    NetworkedDfsWriteOutcome,
) {
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id;
    drop(service);
    let other = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let requester = other[0].clone();
    let replica = other[1].clone();
    if deny_third_target {
        for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
            runtime.install_fault(crate::test_support::DfsTestFaultRule {
                source: Some(primary.clone()),
                target: Some(replica.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
                remaining_hits: 32,
                action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
            });
        }
    }
    let mut writer = runtime.client(&requester).unwrap();
    let path = format!("/services/{name}.bin");
    let base = writer
        .posix_write_file(
            &path,
            b"actual original head",
            DfsWriteOptions::replicated_eventual(vec![requester.clone()]),
        )
        .unwrap();
    if deny_primary_fetch {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(requester.clone()),
            route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = (0..2 * 1024 * 1024 + 17)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    // This is the same real requester capture and current manifest producer
    // used by current internal CAS and retained-content callers. It installs
    // no metadata, coordinator state or authority witness.
    let manifest = writer
        .content()
        .put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            &bytes,
        )
        .unwrap();
    let candidates = match (desired, primary_is_desired) {
        (2, true) => vec![requester.clone(), primary.clone()],
        (2, false) => vec![requester.clone(), replica.clone()],
        (3, _) => vec![requester.clone(), primary.clone(), replica.clone()],
        _ => unreachable!("bounded native fixture replication plan"),
    };
    let mut policy = DfsReplicationPolicy::new(min_commit, desired).unwrap();
    policy.required_node_ids = candidates.clone();
    let outcome = writer
        .write_distributed_manifest_with_candidates(
            request.clone(),
            Some(base.version),
            manifest.content_hash.clone(),
            manifest.size_bytes,
            candidates,
            policy,
            DfsDistributedWriteOptions {
                publication: DfsWritePublication::Current,
                repair: if foreground {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    (
        runtime, primary, requester, replica, writer, request, manifest, outcome,
    )
}

fn assert_native_manifest_repair_pending(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    request: &DfsWriteRequest,
    missing: &NodeId,
) -> DfsWriteIntentRecord {
    service.hydrate_from_metadata_backend().unwrap();
    let job = service
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == request.object_id
                && job.version == request.target_version
                && &job.missing_replica_node_id == missing
        })
        .expect("genuine unfinished repair obligation");
    let backend = service.metadata_backend().unwrap();
    let record = backend
        .get_intent(&request.object_id, &job.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    assert!(record.desired_replica_node_ids.contains(missing));
    assert_eq!(record.target_version, request.target_version);
    assert!(service.writes.pending_writes.contains_key(&job.intent_id));
    assert!(
        !backend
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .any(|receipt| &receipt.replica_node_id == missing),
        "an already completed copy is not restart recovery coverage"
    );
    record
}

#[test]
fn dfs_message_repair_progress_is_only_inline_for_explicit_foreground_repair_writes() {
    let source = NodeId::from_legacy_named("node_protocol_repair_source").expect("source");
    let target = NodeId::from_legacy_named("node_protocol_repair_target").expect("target");
    let file_system_id = sifs_file_system_id();
    let namespace_command = |request_id: &str, operation| {
        DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: request_id.to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: source.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation,
        })
    };
    let lookup = test_dfs_message_command_from_message(
        &source,
        &target,
        1,
        namespace_command(
            "dfs_lookup_no_inline_repair",
            DfsNamespaceOperation::LookupPath {
                path: "/services/no-inline-repair".to_string(),
                inline_range: None,
            },
        ),
    );
    let mkdir = test_dfs_message_command_from_message(
        &source,
        &target,
        2,
        namespace_command(
            "dfs_mkdir_no_inline_repair",
            DfsNamespaceOperation::MkdirAll {
                path: "/services/no-inline-repair".to_string(),
            },
        ),
    );
    let sync_path = test_dfs_message_command_from_message(
        &source,
        &target,
        3,
        namespace_command(
            "dfs_sync_path_no_inline_repair",
            DfsNamespaceOperation::SyncPath {
                path: "/services/no-inline-repair".to_string(),
            },
        ),
    );
    let read = test_dfs_message_command_from_message(
        &source,
        &target,
        4,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "dfs_read_no_inline_repair".to_string(),
            object_id: DfsObjectId::from_legacy_named("obj_no_inline_repair").expect("object"),
            version: None,
            content_hash: Some("blake3:noinline".to_string()),
            content_size_bytes: Some(0),
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    let object_id = DfsObjectId::from_legacy_named("obj_foreground_repair_write").expect("object");
    let mut background_write =
        test_routed_write_command("dfs_background_write_no_inline_repair", &source, &object_id);
    background_write.foreground_replica_repair = false;
    let background_write = test_dfs_message_command_from_message(
        &source,
        &target,
        5,
        DfsNetworkMessage::DistributedWriteRequest(background_write),
    );
    let foreground_repair_write = test_dfs_message_command_from_message(
        &source,
        &target,
        6,
        DfsNetworkMessage::DistributedWriteRequest(test_routed_write_command(
            "dfs_foreground_repair_write",
            &source,
            &object_id,
        )),
    );

    assert!(!dfs_message_allows_write_repair_progress(&lookup));
    assert!(!dfs_message_allows_write_repair_progress(&mkdir));
    assert!(!dfs_message_allows_write_repair_progress(&sync_path));
    assert!(!dfs_message_allows_write_repair_progress(&read));
    assert!(!dfs_message_allows_write_repair_progress(&background_write));
    assert!(dfs_message_allows_write_repair_progress(
        &foreground_repair_write
    ));
}

fn native_broken_patch_wait(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    String,
) {
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = shard.primary_node_id;
    let replica = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != owner)
        .unwrap();
    drop(initial);
    let path = format!("/services/{name}-broken.bin");
    let foreground_path = format!("/services/{name}-foreground.bin");
    let mut base_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    base_policy.required_node_ids = vec![owner.clone()];
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, base_policy)
        .unwrap();
    let mut first = runtime.client(&owner).unwrap();
    let initial = (0..256 * 1024 + 31)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let base = first
        .posix_write_file(
            &path,
            &initial,
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(first);
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    target_policy.required_node_ids = vec![owner.clone(), replica.clone()];
    for target in [&path, &foreground_path] {
        runtime
            .apply_placement_policy(
                &owner,
                target,
                DfsPlacementScope::Subtree,
                target_policy.clone(),
            )
            .unwrap();
    }
    for route in [
        DFS_DATA_STAGE_BLOB_ROUTE,
        DFS_DATA_STAGE_PATCH_ROUTE,
        DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
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
    let mut service = runtime.metadata_primary_service(&path).unwrap();
    let patch = (0..80 * 1024 + 7)
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    service
        .posix_write_file_at(
            &path,
            96 * 1024 + 5,
            &patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let (intent_id, patch_hash) = service
        .writes
        .pending_writes
        .iter()
        .find_map(|(intent_id, pending)| {
            pending
                .patch
                .as_ref()
                .filter(|_| pending.object_id == base.object_id)
                .map(|patch| (intent_id.clone(), patch.patch_hash.clone()))
        })
        .expect("the actual delta producer retains its unfinished patch write");
    let record = service
        .metadata_backend()
        .unwrap()
        .get_intent(&base.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_ne!(record.status, DfsWriteIntentStatus::Committed);
    assert_eq!(record.policy.min_commit_replicas, 2);
    assert!(
        !service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&base.object_id, &record.target_version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    service
        .content()
        .debug_remove_loose_blob(&patch_hash)
        .unwrap();
    // Move only through ordinary cooling passes to one pass before the actual
    // failed stage becomes eligible. No clock, retry or inflight row is edited.
    for _ in 0..8 {
        let pending = service.writes.pending_writes.get(&intent_id).unwrap();
        let next = pending.stage_retries.get(&replica).unwrap().next_retry_tick;
        if next <= service.repair_retry_tick + 1 {
            break;
        }
        service.retry_pending_repairs_bounded(1).unwrap();
    }
    runtime.clear_faults();
    (
        runtime,
        owner,
        replica,
        service,
        base,
        intent_id,
        foreground_path,
    )
}

#[test]
fn posix_wait_for_commit_reports_repair_retry_failure_without_failing_write() {
    let (runtime, owner, replica, mut service, _base, _broken_intent, path) =
        native_broken_patch_wait("native-posix-wait-retry-best-effort");
    let before = runtime.rpc_observations().len();
    let bytes = vec![0x79; 2 * 1024 * 1024 + 71];
    let stat = service
        .posix_write_file(
            &path,
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    assert_eq!(
        stat.current_locations.into_iter().collect::<BTreeSet<_>>(),
        [owner.clone(), replica.clone()].into_iter().collect()
    );
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert!(
        service.last_events().iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RepairRetryFailed { reason }
            if reason.contains("failed to open DFS patch blob"))),
        "ordinary write settle reports a real unrelated missing-patch retry failure"
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
            .count(),
        1
    );
    assert_eq!(service.posix_read_file(&path).unwrap(), bytes);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn served_receipt_returns_before_unrelated_completed_write_response_rpc() {
    let name = "native-served-receipt-detached-completion";
    let (mut runtime, primary, replica, requester, base, _) =
        native_read_fixture(name, 1, false, b"actual routed predecessor".to_vec());
    let nodes = vec![requester.clone(), replica.clone()];
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, &base.path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    let captured = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: None,
            captured: Arc::clone(&captured),
        },
    });
    let mut client = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x6d; 2 * 1024 * 1024 + 31];
    let operation = sinomo_core::Id::generate().to_string();
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let pending = client
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
    assert!(matches!(pending, NetworkedDfsWriteOutcome::Pending { .. }));
    client.process_incoming().unwrap();
    let original = captured
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("actual receiver generated the lost Stage acknowledgement");
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, primary);
    let DfsNetworkMessage::StageBlobResponse(ack) = &original.message else {
        unreachable!()
    };
    assert_eq!(ack.file_system_id, request.file_system_id);
    assert_eq!(ack.object_id, request.object_id);
    assert_eq!(ack.version, request.target_version);
    assert_eq!(ack.replica_node_id, replica);
    assert!(
        matches!(&ack.result, DfsBlobTransferResult::Durable { content_hash: hash }
        if hash == &content_hash(&bytes))
    );
    let intent_id = ack.intent_id.clone();
    let receipt_rpc_id = ack.request_id.clone();
    assert!(client.writes.pending_routed_writes.contains_key(&operation));
    let mut authority = runtime.client(&primary).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    assert_ne!(
        authority
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    drop(authority);
    runtime.clear_faults();
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(primary.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
    // The original lost send has genuinely expired its volatile reservation.
    // Reopen Main with the real durable unfinished intent before replaying its
    // owned reply, just as ordinary late-receipt recovery does.
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.client(&primary).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let retained = restored.writes.pending_writes.get(&intent_id).unwrap();
    assert!(retained.stage_retries.is_empty());
    assert!(retained.stage_inflight_request_ids.is_empty());
    assert_ne!(
        restored
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    drop(restored);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(requester.clone()),
        route: Some(DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    let before = runtime.rpc_observations().len();
    // Replay the exact owned acknowledgement through the real authenticated
    // replica -> Main endpoint; the caller supplies no receipt or completion.
    let started = Instant::now();
    let receipt = runtime.rpc_request(
        &replica,
        &primary,
        dfs_message_rpc_request(&DfsMessageCommand {
            request_id: receipt_rpc_id,
            routed_message: original,
        })
        .unwrap(),
        Duration::from_secs(1),
    );
    let receipt_elapsed = started.elapsed();
    let wait_started = Instant::now();
    while wait_started.elapsed() < Duration::from_secs(1) {
        if runtime.rpc_observations()[before..].iter().any(|event| {
            event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        }) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let paused = runtime.rpc_observations();
    let completion_started = paused[before..].iter().any(|event| {
        event.source == primary
            && event.target == requester
            && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
    });
    let completion_finished = paused[before..].iter().any(|event| {
        event.source == primary
            && event.target == requester
            && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::Completed
    });
    // Always release the actual worker before assertions or fixture shutdown.
    gate.add_permits(1);
    let receipt = receipt.expect("native receipt RPC returns while unrelated completion is gated");
    decode_dfs_message_rpc_response(&receipt).unwrap();
    assert!(
        receipt_elapsed < Duration::from_secs(1),
        "unchanged receipt response budget"
    );
    assert!(
        completion_started,
        "the normal native detached worker owns the real completion"
    );
    assert!(
        !completion_finished,
        "receipt construction does not wait for completion delivery"
    );
    let settled = Instant::now();
    let mut outcome = client.write_outcome(&operation).unwrap();
    while matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. })
        && settled.elapsed() < Duration::from_secs(1)
    {
        client.process_incoming().unwrap();
        outcome = client.write_outcome(&operation).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        matches!(outcome, NetworkedDfsWriteOutcome::Committed { version, content_hash: hash, .. }
        if version == request.target_version && hash == content_hash(&bytes))
    );
    let authority = runtime.client(&primary).unwrap();
    assert_eq!(
        authority
            .metadata_backend()
            .unwrap()
            .get_object(&request.object_id)
            .unwrap()
            .unwrap()
            .version,
        request.target_version
    );
    let receipts = authority
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica
                && receipt.content_hash.as_deref() == Some(content_hash(&bytes).as_str()))
    );
    drop(authority);
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}

fn assert_native_served_write_defers_repair(name: &str, foreground: bool) {
    let (runtime, primary, requester, replica, mut client, request, bytes, original) =
        native_routed_committed_backlog(name, foreground);
    let observed = runtime.rpc_observations();
    let start = observed
        .iter()
        .rposition(|event| {
            event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Constructed
        })
        .unwrap();
    let call_id = observed[start].call_id;
    let end = observed
        .iter()
        .enumerate()
        .skip(start)
        .find(|(_, event)| {
            event.source == requester
                && event.target == primary
                && event.call_id == call_id
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed
        })
        .unwrap()
        .0;
    assert!(
        observed[start..=end]
            .iter()
            .all(|event| !(event.source == primary
                && event.target == replica
                && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                    .contains(&event.route.as_str())
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)),
        "the native served reply does not wait for its own or unrelated replica repairs"
    );
    let DfsNetworkMessage::DistributedWriteResponse(response) = original.message else {
        unreachable!()
    };
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                repair_jobs,
                version,
                content_hash: hash,
                ..
            },
    } = &response.result
    else {
        panic!("the first real served response must be committed")
    };
    assert_eq!(version, &request.target_version);
    assert_eq!(hash, &content_hash(&bytes));
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].object_id, request.object_id);
    assert_eq!(repair_jobs[0].missing_replica_node_id, replica);
    client
        .handle_distributed_write_response(primary.clone(), response)
        .unwrap();
    let mut authority = runtime.client(&primary).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    assert!(authority.coordinator().repair_jobs().iter().any(|job|
        job.object_id == request.object_id && job.missing_replica_node_id == replica));
    assert!(authority.coordinator().repair_jobs().iter().any(|job|
        job.object_id != request.object_id && job.missing_replica_node_id == replica));
    assert!(
        !authority
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    drop(authority);
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn served_non_foreground_write_does_not_retry_unrelated_repairs() {
    assert_native_served_write_defers_repair("native-served-background-unrelated", false);
}

#[test]
fn served_foreground_write_returns_committed_response_before_replica_repair_completes() {
    assert_native_served_write_defers_repair("native-served-foreground-before-repair", true);
}

#[test]
fn served_sync_path_does_not_retry_repairs_inline() {
    let (runtime, primary, requester, service, base, broken_intent, _path) =
        native_broken_patch_wait("native-served-sync-no-inline-repair");
    let record = service
        .metadata_backend()
        .unwrap()
        .get_intent(&base.object_id, &broken_intent)
        .unwrap()
        .unwrap();
    let before_status = record.status;
    assert_ne!(before_status, DfsWriteIntentStatus::Committed);
    drop(service);
    let mut client = runtime.client(&requester).unwrap();
    let before = runtime.rpc_observations().len();
    let result = client
        .route_namespace_operation_to_metadata_primary_without_local_replay(
            sifs_file_system_id(),
            primary.clone(),
            DfsNamespaceOperation::SyncPath {
                path: "/services".into(),
            },
        )
        .unwrap();
    assert!(matches!(result, DfsNamespaceResult::Applied));
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .all(|event| !(event.source == primary
                && event.target == requester
                && [
                    DFS_DATA_STAGE_BLOB_ROUTE,
                    DFS_DATA_STAGE_PATCH_ROUTE,
                    DFS_DATA_REPAIR_REPLICA_ROUTE
                ]
                .contains(&event.route.as_str())
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)),
        "the actual namespace endpoint must not perform inline repair retries"
    );
    let mut authority = runtime.client(&primary).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    let retained = authority
        .metadata_backend()
        .unwrap()
        .get_intent(&base.object_id, &broken_intent)
        .unwrap()
        .unwrap();
    assert_eq!(retained.status, before_status);
    assert!(authority.writes.pending_writes.contains_key(&broken_intent));
    assert!(
        !client
            .last_events()
            .iter()
            .any(|event| matches!(event, NetworkedDfsServiceEvent::RepairRetryFailed { .. }))
    );
    drop(authority);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn local_content_anti_entropy_pages_and_repairs_stale_locations() {
    let node = NodeId::generate();
    let (runtime, mut service) = native_sifs_service("native-anti-entropy-pages", node.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let mut objects = Vec::new();
    for index in 0..3 {
        let bytes = vec![index as u8 + 1; 512 * 1024];
        objects.push(
            service
                .posix_write_file(
                    &format!("/services/anti-entropy-{index}"),
                    &bytes,
                    options.clone(),
                )
                .unwrap(),
        );
    }
    for object in &objects[..2] {
        service
            .content
            .debug_remove_loose_blob(object.content_hash.as_deref().unwrap())
            .unwrap();
    }
    let mut cursor = None;
    let mut issues = 0;
    let mut repaired = 0;
    for _ in 0..128 {
        let report = service
            .run_local_content_anti_entropy(DfsAntiEntropyOptions {
                file_system_id: sifs_file_system_id(),
                after: cursor.clone(),
                max_objects: 1,
                max_issues: 1,
                repair_metadata: true,
            })
            .unwrap();
        assert!(report.scanned_object_count <= 1);
        assert!(report.issues.len() <= 1);
        issues += report.issue_count;
        repaired += report.repaired_location_count;
        cursor = report.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        cursor.is_none(),
        "bounded ordinary metadata pages must finish"
    );
    assert_eq!(issues, 2);
    assert_eq!(repaired, 2);
    for object in &objects[..2] {
        let current = service
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap()
            .unwrap();
        assert!(!current.current_locations.contains(&node));
    }
    let healthy = &objects[2];
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&healthy.object_id)
            .unwrap()
            .unwrap()
            .current_locations
            .contains(&node)
    );
    assert_eq!(
        service.posix_read_file(&healthy.path).unwrap(),
        vec![3; 512 * 1024]
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn current_anti_entropy_preserves_verified_content_generation_and_size() {
    let node = NodeId::generate();
    let (runtime, mut service) =
        native_sifs_service("native-anti-entropy-verified-size", node.clone());
    let bytes = vec![0x73; 512 * 1024];
    let object = service
        .posix_write_file(
            "/services/verified-size",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![node]),
        )
        .unwrap();
    let before = service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    let report = service
        .run_local_content_anti_entropy(DfsAntiEntropyOptions {
            file_system_id: object.file_system_id.clone(),
            after: None,
            max_objects: 128,
            max_issues: 8,
            repair_metadata: true,
        })
        .unwrap();
    assert_eq!(report.issue_count, 0);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        service.posix_stat(&object.path).unwrap().size_bytes,
        Some(bytes.len() as u64)
    );
    assert_eq!(service.posix_read_file(&object.path).unwrap(), bytes);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn fetch_receipt_completion_sends_untracked_routed_write_response() {
    let (mut runtime, primary, requester, _, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-fetch-receipt-late-response",
            2,
            2,
            true,
            true,
            false,
            true,
        );
    let NetworkedDfsWriteOutcome::Pending { ref intent_id, .. } = outcome else {
        panic!("actual primary Fetch denial must leave W2 pending: {outcome:?}");
    };
    let mut inspect = runtime.client(&primary).unwrap();
    let native_intent = inspect
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, intent_id)
        .unwrap()
        .unwrap();
    assert_ne!(native_intent.status, DfsWriteIntentStatus::Committed);
    assert!(
        inspect
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    assert_eq!(
        inspect
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([requester.clone()])
    );
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.client(&primary).unwrap();
    assert_eq!(
        restored
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, intent_id)
            .unwrap()
            .unwrap(),
        native_intent
    );
    restored.hydrate_from_metadata_backend().unwrap();
    assert!(restored.writes.pending_writes.contains_key(intent_id));
    drop(restored);
    runtime.clear_faults();
    runtime
        .maintenance_tick(&primary, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let events = writer
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(5))
        .unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { object_id, primary_node_id,
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed { version, content_hash, .. }
            }, .. } if object_id == &request.object_id && primary_node_id == &primary
                && version == &request.target_version && content_hash == &manifest.content_hash)));
    let published = runtime.client(&primary).unwrap();
    let receipts = published
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), requester.clone()])
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "actual recovered W receipt sends its later routed completion"
    );
    assert_eq!(
        published
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1
    );
    drop(published);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn routed_inline_blob_records_requester_receipt_without_restaging() {
    let name = "native-routed-inline-requester-receipt";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = config.primary_node_id;
    drop(service);
    let requester = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary)
        .unwrap();
    let path = "/services/native-inline-requester.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = vec![primary.clone(), requester.clone()];
    runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut writer = runtime.client(&requester).unwrap();
    let before = runtime.rpc_observations().len();
    let bytes = b"complete inline routed bytes genuinely held by their requester";
    let stat = writer
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                replica_candidates: vec![primary.clone(), requester.clone()],
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let calls = runtime.rpc_observations()[before..].to_vec();
    assert!(calls.iter().any(|event| event.source == requester
        && event.target == primary
        && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
    assert!(
        !calls.iter().any(|event| event.source == primary
            && event.target == requester
            && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_FETCH_BLOB_ROUTE]
                .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "normal primary never retransfers inline bytes to the actual durable requester"
    );
    let published = runtime.client(&primary).unwrap();
    let receipts = published
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), requester.clone()])
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.content_hash == stat.content_hash
                && receipt.receipt.file_system_id == stat.file_system_id
                && receipt.receipt.object_id == stat.object_id
                && receipt.receipt.committed_version == stat.version)
    );
    assert_eq!(
        published
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        writer
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(published);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn committed_remote_manifest_repair_fetches_from_durable_requester() {
    let (mut runtime, primary, requester, replica, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-routed-manifest-remote-repair",
            1,
            2,
            false,
            true,
            true,
            false,
        );
    writer.settle_ready_write_outcome(outcome, true).unwrap();
    let mut inspect = runtime.client(&primary).unwrap();
    let native_intent = assert_native_manifest_repair_pending(&mut inspect, &request, &replica);
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.client(&primary).unwrap();
    assert_eq!(
        assert_native_manifest_repair_pending(&mut restored, &request, &replica),
        native_intent
    );
    drop(restored);
    runtime.clear_faults();
    runtime
        .maintenance_tick(&primary, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let mut final_primary = runtime.client(&primary).unwrap();
    let current = final_primary
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    final_primary
        .posix_sync_file(&current.path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let calls = runtime.rpc_observations();
    let fetch = calls
        .iter()
        .position(|event| {
            event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
        .unwrap();
    let repair = calls
        .iter()
        .position(|event| {
            event.source == primary
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
        .unwrap();
    assert!(
        repair > fetch,
        "actual missing primary bytes precede target repair"
    );
    let target = runtime.client(&replica).unwrap();
    let stored = target
        .content()
        .read_manifest(request.object_id.as_str(), request.target_version.as_str())
        .unwrap();
    assert_eq!(stored.content_hash, manifest.content_hash);
    assert_eq!(stored.size_bytes, manifest.size_bytes);
    assert_eq!(
        target
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1
    );
    drop(target);
    drop(final_primary);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn committed_remote_manifest_self_repair_fetches_from_durable_requester() {
    let (mut runtime, primary, requester, _, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-routed-manifest-self-repair",
            1,
            2,
            true,
            true,
            false,
            true,
        );
    writer.settle_ready_write_outcome(outcome, true).unwrap();
    let mut inspect = runtime.client(&primary).unwrap();
    let native_intent = assert_native_manifest_repair_pending(&mut inspect, &request, &primary);
    assert!(
        inspect
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.client(&primary).unwrap();
    assert_eq!(
        assert_native_manifest_repair_pending(&mut restored, &request, &primary),
        native_intent
    );
    assert!(
        restored
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    drop(restored);
    let attempts_before = crate::read_dfs_repair_attempt_snapshot(
        &crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&primary)),
    )
    .unwrap();
    runtime.clear_faults();
    let retry_at = attempts_before
        .as_ref()
        .unwrap()
        .records
        .iter()
        .find(|row| {
            row.intent_id == native_intent.intent_id && row.missing_replica_node_id == primary
        })
        .expect("actual durable self-repair continuation")
        .next_retry_unix_nanos;
    let remaining_backoff = Duration::from_nanos(
        retry_at.saturating_sub(crate::metadata_records::dfs_now_unix_nanos()),
    );
    assert!(
        remaining_backoff <= sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        "fixture must retain the normal recorded retry bound"
    );
    // Clearing a negative transport fault does not expire durable backoff.
    // Wait on the real clock; do not rewrite queue facts or extend the call budget.
    std::thread::sleep(remaining_backoff);
    runtime
        .maintenance_tick(&primary, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "actual repair queue before maintenance={attempts_before:#?}; after={:#?}",
        crate::read_dfs_repair_attempt_snapshot(&crate::default_dfs_repair_attempt_snapshot_path(
            runtime.data_dir(&primary)
        ))
        .unwrap()
    );
    let mut recovered = runtime.client(&primary).unwrap();
    let current = recovered
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    recovered
        .posix_sync_file(&current.path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let stored = recovered
        .content()
        .read_manifest(request.object_id.as_str(), request.target_version.as_str())
        .unwrap();
    assert_eq!(stored.content_hash, manifest.content_hash);
    assert_eq!(stored.size_bytes, manifest.size_bytes);
    let current = recovered
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, request.target_version);
    assert!(current.current_locations.contains(&primary));
    assert_eq!(
        recovered
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1
    );
    drop(recovered);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn nonforeground_remote_manifest_self_repair_defers_fetch_from_durable_requester() {
    let (mut runtime, primary, requester, _, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-routed-background-self-repair",
            1,
            2,
            true,
            false,
            false,
            true,
        );
    writer.settle_ready_write_outcome(outcome, false).unwrap();
    let calls = runtime.rpc_observations();
    let response = calls
        .iter()
        .position(|event| {
            event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
        .unwrap();
    assert!(
        calls
            .iter()
            .enumerate()
            .filter(|(_, event)| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch)
            .all(|(index, _)| index > response),
        "normal nonforeground requester reply completes before fetch dispatch"
    );
    let mut inspect = runtime.client(&primary).unwrap();
    let native_intent = assert_native_manifest_repair_pending(&mut inspect, &request, &primary);
    assert!(
        inspect
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.client(&primary).unwrap();
    assert_eq!(
        assert_native_manifest_repair_pending(&mut restored, &request, &primary),
        native_intent
    );
    assert!(
        restored
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    drop(restored);
    let attempts_before = crate::read_dfs_repair_attempt_snapshot(
        &crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&primary)),
    )
    .unwrap();
    runtime.clear_faults();
    let retry_at = attempts_before
        .as_ref()
        .unwrap()
        .records
        .iter()
        .find(|row| {
            row.intent_id == native_intent.intent_id && row.missing_replica_node_id == primary
        })
        .expect("actual durable self-repair continuation")
        .next_retry_unix_nanos;
    let remaining_backoff = Duration::from_nanos(
        retry_at.saturating_sub(crate::metadata_records::dfs_now_unix_nanos()),
    );
    assert!(
        remaining_backoff <= sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        "fixture must retain the normal recorded retry bound"
    );
    // Clearing a negative transport fault does not expire durable backoff.
    // Wait on the real clock; do not rewrite queue facts or extend the call budget.
    std::thread::sleep(remaining_backoff);
    runtime
        .maintenance_tick(&primary, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "actual repair queue before maintenance={attempts_before:#?}; after={:#?}",
        crate::read_dfs_repair_attempt_snapshot(&crate::default_dfs_repair_attempt_snapshot_path(
            runtime.data_dir(&primary)
        ))
        .unwrap()
    );
    let mut recovered = runtime.client(&primary).unwrap();
    let current = recovered
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    recovered
        .posix_sync_file(&current.path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let stored = recovered
        .content()
        .read_manifest(request.object_id.as_str(), request.target_version.as_str())
        .unwrap();
    assert_eq!(stored.content_hash, manifest.content_hash);
    assert_eq!(stored.size_bytes, manifest.size_bytes);
    let current = recovered
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, request.target_version);
    assert!(current.current_locations.contains(&primary));
    assert_eq!(
        recovered
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1
    );
    drop(recovered);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn eventual_write_with_local_receipt_tolerates_unreachable_desired_replica() {
    let (runtime, owner, replica, service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-eventual-unreachable-desired", false);
    let jobs = service
        .coordinator()
        .repair_jobs()
        .iter()
        .filter(|job| job.intent_id == intent_id)
        .collect::<Vec<_>>();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].missing_replica_node_id, replica);
    let current = service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, stat.version);
    assert_eq!(current.current_locations, vec![owner.clone()]);
    let failed_attempt_observed = || {
        runtime.rpc_observations().iter().any(|event| {
            event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse
        })
    };
    // W1 acknowledgement precedes background N2 delivery. Drive the actual
    // owner until the injected unreachable-target attempt has really ended.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while !failed_attempt_observed() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "no actual failed repair attempt within the normal caller budget"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    assert!(failed_attempt_observed());
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap(),
        Some(current)
    );
    drop(service);
    runtime.clear_faults();
    runtime.close().unwrap();
}

pub(super) fn assert_native_remote_manifest_fetch_deferral(name: &str, foreground: bool) {
    let (runtime, initial, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = shard.primary_node_id.clone();
    let requester = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary)
        .unwrap();
    drop(initial);
    let path = format!("/services/{name}.bin");
    let mut first_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    first_policy.required_node_ids = vec![requester.clone(), primary.clone()];
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, first_policy)
        .unwrap();
    let mut first = runtime.client(&requester).unwrap();
    let predecessor = first
        .posix_write_file(
            &path,
            b"actual requester predecessor",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![requester.clone(), primary.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(first);
    let nodes = vec![requester.clone(), primary.clone()];
    let mut target_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    target_policy.required_node_ids = nodes.clone();
    let accepted = runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, target_policy)
        .unwrap();
    // The healthy predecessor already has both real holders. Finish this
    // exact policy job before constructing the new immutable request, so an
    // unrelated placement intent cannot replace its missing-byte obligation.
    let control = runtime.shared_control_plane().unwrap();
    let read_apply = || {
        control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap()
    };
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let current = read_apply();
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.path, path);
        assert_eq!(current.scheduled_repair_count, 0);
        assert_eq!(current.pending_repair_count, 0);
        assert!(current.last_error.is_none());
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(matches!(
            current.state,
            DfsPlacementReconciliationState::Pending
                | DfsPlacementReconciliationState::Scanning
                | DfsPlacementReconciliationState::Repairing
        ));
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero());
        runtime
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    assert_eq!(
        read_apply().state,
        DfsPlacementReconciliationState::Completed
    );
    assert!(Instant::now() < setup_deadline);
    drop(control);
    let mut current = runtime.client(&requester).unwrap();
    let base = current.posix_stat_distributed(&path).unwrap();
    assert_eq!(base.object_id, predecessor.object_id);
    assert_eq!(base.file_system_id, predecessor.file_system_id);
    assert_eq!(base.content_hash, predecessor.content_hash);
    drop(current);
    let captured_fetch = Arc::new(Mutex::new(
        Vec::<crate::test_runtime::CapturedFetchReply>::new(),
    ));
    let served_fetch = runtime.capture_served_work(&requester, 32).unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(primary.clone()),
        target: Some(requester.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::AlterFetchReplyAfterDispatch {
            change: crate::test_runtime::FetchReplyFault::NotFound,
            captured: Arc::clone(&captured_fetch),
        },
    });
    let mut writer = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x62; 2 * 1024 * 1024 + 23];
    let manifest = writer
        .content()
        .put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            &bytes,
        )
        .unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let before = runtime.rpc_observations().len();
    writer
        .write_distributed_manifest_with_candidates(
            request.clone(),
            Some(base.version),
            manifest.content_hash.clone(),
            manifest.size_bytes,
            nodes,
            policy,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.clone()),
                repair: if foreground {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    writer.process_incoming().unwrap();
    let outcome = writer.write_outcome(&operation).unwrap();
    let NetworkedDfsWriteOutcome::Committed {
        repair_jobs,
        version,
        content_hash: hash,
        ..
    } = outcome
    else {
        panic!("normal native manifest W1 publication must remain committed")
    };
    assert_eq!(version, request.target_version);
    assert_eq!(hash, manifest.content_hash);
    assert_eq!(repair_jobs.len(), 1);
    assert_eq!(repair_jobs[0].missing_replica_node_id, primary);
    let mut authority = runtime.metadata_primary_service(&path).unwrap();
    authority.hydrate_from_metadata_backend().unwrap();
    let actual = authority
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(actual.version, request.target_version);
    assert_eq!(
        actual.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    assert!(
        !authority
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == primary)
    );
    let observed = runtime.rpc_observations();
    let start = observed
        .iter()
        .enumerate()
        .skip(before)
        .find(|(_, event)| {
            event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Constructed
        })
        .unwrap()
        .0;
    let call_id = observed[start].call_id;
    let end = observed
        .iter()
        .enumerate()
        .skip(start)
        .find(|(_, event)| {
            event.source == requester
                && event.target == primary
                && event.call_id == call_id
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed
        })
        .unwrap()
        .0;
    assert!(
        observed[start..=end]
            .iter()
            .all(|event| !(event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)),
        "the native served completion defers its missing local body transfer"
    );
    if foreground {
        let intent = repair_jobs[0].intent_id.clone();
        assert!(
            authority.coordinator().repair_jobs().iter().any(|job| {
                job.intent_id == intent
                    && job.object_id == request.object_id
                    && job.version == request.target_version
                    && job.missing_replica_node_id == primary
            }),
            "the actual owning Core restores this genuine missing-byte obligation"
        );
        drop(authority);
        // Main owns the durable queue and its actual producer admission. A
        // public browser's private retry maps do not drive that queue.
        runtime.request_repair_reconciliation(&primary).unwrap();
        runtime
            .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap()
            .outcome
            .unwrap();
        let replies = captured_fetch.lock().unwrap().clone();
        let original = replies
            .iter()
            .find(|captured| {
                matches!(&captured.routed.message,
            DfsNetworkMessage::FetchBlobResponse(response)
            if response.intent_id == intent && response.object_id == request.object_id
                && response.version == request.target_version)
            })
            .expect("actual Main's selected post-commit Fetch reply");
        assert_eq!(original.routed.source_node_id, requester);
        assert_eq!(original.routed.target_node_id, primary);
        let DfsNetworkMessage::FetchBlobResponse(response) = &original.routed.message else {
            unreachable!()
        };
        assert_eq!(response.source_node_id, requester);
        assert!(
            matches!(&response.result, DfsFetchBlobResult::Found { content_hash: hash, bytes: actual_bytes }
            if hash == &manifest.content_hash && actual_bytes == &bytes),
            "the fault changes only a genuine current-byte response"
        );
        let matching = served_fetch.records().into_iter().filter(|record| {
            record.command.request_id == original.rpc_request_id
                && record.command.routed_message.source_node_id == primary
                && record.command.routed_message.target_node_id == requester
                && matches!(&record.command.routed_message.message, DfsNetworkMessage::FetchBlob(command)
                    if command.request_id == response.request_id && command.intent_id == intent
                        && command.object_id == request.object_id && command.version == request.target_version
                        && command.content_hash == manifest.content_hash && command.authority_shard.as_ref() == Some(&shard))
        }).collect::<Vec<_>>();
        assert_eq!(
            matching.len(),
            1,
            "the original full Fetch command and reply belong to the SAME actual native call"
        );
        assert!(matching[0].succeeded);
        authority = runtime.metadata_primary_service(&path).unwrap();
        authority.hydrate_from_metadata_backend().unwrap();
        assert!(authority.coordinator().repair_jobs().iter().any(|job| {
            job.intent_id == intent
                && job.object_id == request.object_id
                && job.version == request.target_version
                && job.missing_replica_node_id == primary
        }));
        assert_eq!(
            authority
                .metadata_backend()
                .unwrap()
                .get_object(&request.object_id)
                .unwrap(),
            Some(actual.clone()),
            "the exact post-commit negative Fetch cannot rewind the visible publication"
        );
        assert!(
            !authority
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&request.object_id, &request.target_version)
                .unwrap()
                .iter()
                .any(|receipt| receipt.replica_node_id == primary)
        );
    }

    assert_eq!(
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(authority);
    drop(served_fetch);
    drop(writer);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn remote_manifest_write_deferred_foreground_repair_skips_post_commit_fetch() {
    assert_native_remote_manifest_fetch_deferral(
        "native-background-manifest-no-inline-fetch",
        false,
    );
}

#[test]
fn reader_write_can_defer_post_commit_replica_repair() {
    let name = "native-reader-background-repair-mode";
    let path = "/services/native-reader-deferred.bin";
    let (runtime, owner, nodes, mut dfs, policy) =
        native_owned_repair_fixture(name, path, 1, 2, false);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let before = runtime.rpc_observations().len();
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/native-reader-deferred.bin",
            std::io::Cursor::new(b"actual deferred reader bytes".to_vec()),
            DfsWriteOptions {
                policy,
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let jobs = dfs.coordinator().repair_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].object_id, stat.object_id);
    assert_eq!(jobs[0].version, stat.version);
    assert_eq!(jobs[0].missing_replica_node_id, replica);
    assert!(
        !dfs.writes
            .pending_writes
            .get(&jobs[0].intent_id)
            .unwrap()
            .foreground_replica_repair
    );
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "post-W replicas are never staged by the reader path"
    );
    assert_eq!(
        dfs.posix_read_file(&stat.path).unwrap(),
        b"actual deferred reader bytes"
    );
    drop(dfs);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn background_w2_stages_only_quorum_deficit_and_defers_n3_repair() {
    let name = "native-background-w2-quorum-deficit";
    let path = "/services/native-background-w2.bin";
    let (runtime, owner, nodes, mut dfs, policy) =
        native_owned_repair_fixture(name, path, 2, 3, false);
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let before = runtime.rpc_observations().len();
    let bytes = vec![0x5a; 2 * 1024 * 1024 + 4096];
    let stat = dfs
        .posix_write_file_from_reader(
            path,
            std::io::Cursor::new(bytes.clone()),
            DfsWriteOptions {
                policy,
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let calls = runtime.rpc_observations()[before..]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        calls.len(),
        1,
        "background W2 reserves only its one actual remote deficit"
    );
    let jobs = dfs.coordinator().repair_jobs();
    assert_eq!(jobs.len(), 1);
    assert_ne!(jobs[0].missing_replica_node_id, calls[0].target);
    assert!(
        !dfs.writes
            .pending_writes
            .get(&jobs[0].intent_id)
            .unwrap()
            .foreground_replica_repair
    );
    let prepared =
        dfs.metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
            .unwrap()
            .unwrap()
            .tables
            .prepared_operations_v7
            .into_iter()
            .filter(|record| record.status == DfsPreparedOperationStatus::Committed)
            .map(|record| record.payload)
            .find(|record| {
                record.transaction.objects.iter().any(|object| {
                    object.object_id == stat.object_id && object.version == stat.version
                }) && record.terminal_intent.is_some()
            })
            .unwrap();
    assert_eq!(prepared.content_receipts.len(), 2);
    assert_eq!(
        prepared
            .content_receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), calls[0].target.clone()])
    );
    let receiver = runtime.client(&calls[0].target).unwrap();
    assert_eq!(
        receiver
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(receiver);
    drop(dfs);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn foreground_w2_does_not_repair_a_sibling_stage_already_inflight() {
    let name = "native-foreground-w2-held-sibling";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let replica_a = peers[0].clone();
    let replica_b = peers[1].clone();
    let path = format!("/services/{name}.bin");
    let base = dfs
        .posix_write_file(
            &path,
            b"actual stage predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x6b; 2 * 1024 * 1024 + 4096];
    let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica_b.clone()),
        route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    let before = runtime.rpc_observations().len();
    let writer_request = request.clone();
    let writer_bytes = bytes.clone();
    let writer = std::thread::spawn(move || {
        let outcome = dfs.write_distributed_reader_with_candidates(
            writer_request,
            std::io::Cursor::new(writer_bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        );
        (dfs, outcome)
    });
    let observed = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            loop {
                let events = runtime.rpc_observations()[before..].to_vec();
                if events.iter().any(|event| {
                    event.source == owner
                        && event.target == replica_a
                        && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                        && event.phase == crate::test_support::DfsTestRpcPhase::Completed
                }) && events.iter().any(|event| {
                    event.source == owner
                        && event.target == replica_b
                        && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
                }) {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
    });
    // Normal fanout joins all sends; the held interval proves duplicate suppression,
    // and publication/completion assertions follow the actual sibling receiver.
    runtime.clear_faults();
    gate.add_permits(1);
    let (mut dfs, outcome) = writer.join().unwrap();
    let calls_while_held = observed.unwrap();
    assert!(!calls_while_held.iter().any(|event| event.source == owner
        && event.target == replica_b
        && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch));
    let outcome = outcome.unwrap();
    let intent = match &outcome {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. }
        | NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id.clone(),
    };
    let events = dfs.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobStaged { replica_node_id, .. } if replica_node_id == &replica_b)),
        "the held native receiver supplies its own Durable receipt");
    dfs.settle_ready_write_outcome(dfs.write_outcome(&intent).unwrap(), true)
        .unwrap();
    assert!(
        matches!(dfs.write_outcome(&intent).unwrap(), NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } if repair_jobs.is_empty())
    );
    dfs.posix_sync_file(&path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let calls = runtime.rpc_observations();
    assert!(
        !calls.iter().any(|event| event.source == owner
            && event.target == replica_b
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "a genuine late sibling Stage must never own a duplicate Repair"
    );
    assert!(calls.iter().any(|event| event.source == owner
        && event.target == replica_b
        && event.route == crate::DFS_DATA_COMMIT_VERSION_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
    for peer in [replica_a, replica_b] {
        let replica = runtime.client(&peer).unwrap();
        assert_eq!(
            replica
                .content()
                .read_object_version(request.object_id.as_str(), request.target_version.as_str())
                .unwrap()
                .1,
            bytes
        );
    }
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap();
    assert_eq!(receipts.len(), 3);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn foreground_w1_postcommit_targets_use_repair_not_stage() {
    let name = "native-foreground-w1-repair-not-stage";
    let (runtime, dfs, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let path = "/services/native-foreground-w1.bin";
    drop(dfs);
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let before = runtime.rpc_observations().len();
    let bytes = vec![0x3c; 2 * 1024 * 1024 + 4096];
    let stat = dfs
        .posix_write_file_from_reader(
            path,
            std::io::Cursor::new(bytes.clone()),
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let calls = runtime.rpc_observations()[before..].to_vec();
    for replica in nodes.iter().filter(|node| **node != owner) {
        assert!(calls.iter().any(|event| event.source == owner
            && &event.target == replica
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
        assert!(!calls.iter().any(|event| event.source == owner
            && &event.target == replica
            && event.route == DFS_DATA_STAGE_BLOB_ROUTE
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch));
        let receiver = runtime.client(replica).unwrap();
        assert_eq!(
            receiver
                .content()
                .read_object_version(stat.object_id.as_str(), stat.version.as_str())
                .unwrap()
                .1,
            bytes
        );
    }
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn background_local_fetch_crossing_w_preserves_deferred_repair_mode() {
    let (runtime, primary, requester, replica, mut writer, request, _, outcome) =
        native_routed_manifest_write(
            "native-background-fetch-crosses-w",
            2,
            3,
            true,
            false,
            true,
            false,
        );
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    let events = writer
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(5))
        .unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteWriteCompleted {
        result: DfsDistributedWriteResult::Outcome { outcome: NetworkedDfsWriteOutcome::Committed { version, repair_jobs, .. } }, ..
    } if version == &request.target_version && repair_jobs.iter().any(|job| job.missing_replica_node_id == replica))));
    let calls = runtime.rpc_observations();
    assert!(calls.iter().any(|event| event.source == primary
        && event.target == requester
        && event.route == DFS_DATA_FETCH_BLOB_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
    let completion = calls
        .iter()
        .position(|event| {
            event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
        .unwrap();
    assert!(
        !calls
            .iter()
            .take(completion + 1)
            .any(|event| event.source == primary
                && event.target == replica
                && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                    .contains(&event.route.as_str())
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "the real fetched W receipt cannot start background N-W work before the requester completion"
    );
    let mut restored = runtime.client(&primary).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let job = restored
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == request.object_id
                && job.version == request.target_version
                && job.missing_replica_node_id == replica
        })
        .unwrap();
    assert!(
        !restored
            .writes
            .pending_writes
            .get(&job.intent_id)
            .unwrap()
            .foreground_replica_repair
    );
    let body = restored
        .metadata_backend()
        .unwrap()
        .list_prepared_operations()
        .unwrap()
        .into_iter()
        .find(|record| {
            record.terminal_intent.as_ref().is_some_and(|intent| {
                intent.object_id == request.object_id
                    && intent.target_version == request.target_version
            })
        })
        .unwrap();
    assert_eq!(
        body.content_receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([requester, primary.clone()])
    );
    drop(restored);
    runtime.clear_faults();
    runtime
        .maintenance_tick(&primary, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    let mut final_primary = runtime.client(&primary).unwrap();
    let object = final_primary
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    final_primary
        .posix_sync_file(&object.path, crate::DfsPosixSyncKind::File)
        .expect("normal file barrier must settle the healed actual N-W obligation");
    let peer = runtime.client(&replica).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        writer
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1
    );
    drop(peer);
    drop(final_primary);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn parked_repair_attempt_merge_preserves_active_inputs_byte_for_byte() {
    let offline = NodeId::from_legacy_named("node_repair_attempt_parked").expect("offline");
    let healthy = NodeId::from_legacy_named("node_repair_attempt_healthy").expect("healthy");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_attempt_parked").expect("object");
    let version = DfsVersion::from_legacy_named("version_repair_attempt_parked").expect("version");
    let content_hash = format!("blake3:{}", "2".repeat(64));
    let record =
        |intent_id: &str, target: NodeId, next_retry_unix_nanos: u64| DfsRepairAttemptRecord {
            transfer_delivery_state: None,
            completed_transfer_request_id: None,
            transfer_kind: None,
            intent_id: intent_id.to_string(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: content_hash.clone(),
            missing_replica_node_id: target,
            attempts: 7,
            in_flight: true,
            last_attempt_unix_nanos: 11,
            next_retry_unix_nanos,
            transfer_request_id: Some(format!("request-{intent_id}")),
            last_error: Some("transient route failure".to_string()),
        };
    let parked_future = record("intent-parked-future", offline.clone(), 99);
    let parked_due = record("intent-parked-due", offline.clone(), 1);
    let parked_completed = record("intent-parked-completed", offline.clone(), 55);
    let mut regenerated_excluded = parked_future.clone();
    regenerated_excluded.attempts = 8;
    regenerated_excluded.in_flight = false;
    regenerated_excluded.last_attempt_unix_nanos = 88;
    regenerated_excluded.next_retry_unix_nanos = 100;
    regenerated_excluded.transfer_request_id = None;
    let healthy_active = record("intent-healthy", healthy.clone(), 42);
    let active_repair_keys = BTreeSet::from([
        (parked_future.intent_id.clone(), offline.clone()),
        (parked_due.intent_id.clone(), offline.clone()),
        (healthy_active.intent_id.clone(), healthy),
    ]);

    let merged = crate::maintenance::merge_parked_dfs_repair_attempt_records(
        vec![regenerated_excluded, healthy_active.clone()],
        vec![parked_future.clone(), parked_due.clone(), parked_completed],
        &active_repair_keys,
        &DfsMaintenanceExecutionExclusions::new([offline]),
    );

    assert_eq!(merged.len(), 3);
    assert!(merged.contains(&parked_future));
    assert!(merged.contains(&parked_due));
    assert!(merged.contains(&healthy_active));
}

#[test]
fn transient_route_repair_attempts_retry_promptly_after_restore() {
    let name = "native-transient-route-repair-restore";
    let path = "/services/native-transient-retry";
    let (runtime, owner, nodes, core, policy) = native_owned_repair_fixture(name, path, 1, 2, true);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    drop(core);
    // Main owns the normal write and durable missing-target enrollment. Keep
    // its background claim parked while this exact Core retry is selected.
    let mut requester = runtime.client(&owner).unwrap();
    let stat = requester
        .posix_write_file(
            "/services/native-transient-retry",
            b"actual route failure bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    dfs.hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let intent = dfs.coordinator().repair_jobs()[0].intent_id.clone();
    // A real before-dispatch failure supplies the attempt's identity and time.
    runtime.clear_faults();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    dfs.retry_pending_repairs_bounded(1).unwrap();
    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(records[0].intent_id, intent);
    assert!(records[0].last_error.is_some());
    assert!(!records[0].in_flight);
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse)
    );
    // Preserve the exact normal retry record generated by the negative carrier return.
    assert!(crate::repair::repair_attempt_record_indicates_transient_route_failure(&records[0]));
    drop(dfs);
    runtime.clear_faults();
    let mut reopened = runtime.metadata_primary_service(path).unwrap();
    let before = runtime.rpc_observations().len();
    let scheduled = reopened
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .unwrap();
    assert_eq!(scheduled.len(), 1);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    reopened.process_incoming().unwrap();
    assert_eq!(
        reopened.posix_read_file(&stat.path).unwrap(),
        b"actual route failure bytes"
    );
    drop(reopened);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn retry_pending_repairs_suppresses_stale_content_target_after_object_advances() {
    let (runtime, owner, peer, mut dfs) =
        native_missing_content_repair("native-stale-content-repair-retirement");
    let path = "/services/native-stale-content";
    let first = dfs
        .posix_write_file(
            path,
            b"old missing replica bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), peer.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(dfs.coordinator().repair_jobs().len(), 1);
    let old_intent = dfs.coordinator().repair_jobs()[0].intent_id.clone();
    let replacement = dfs
        .posix_write_file(
            path,
            b"new visible content",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    assert_ne!(replacement.version, first.version);
    assert_ne!(replacement.content_hash, first.content_hash);
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    dfs.repair_retry_tick = 32;
    assert!(dfs.retry_pending_repairs().unwrap().is_empty());
    assert!(!dfs.writes.pending_writes.contains_key(&old_intent));
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == peer
                && [
                    crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
                    crate::DFS_DATA_FETCH_BLOB_ROUTE
                ]
                .contains(&event.route.as_str())
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch),
        "retired old-byte work must not dispatch another actual repair/fetch"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"new visible content");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn retry_pending_repairs_keeps_same_content_after_metadata_only_advance() {
    let (runtime, owner, peer, mut dfs) =
        native_missing_content_repair("native-metadata-successor-repair-plan");
    let path = "/services/native-same-content";
    let first = dfs
        .posix_write_file(
            path,
            b"same immutable bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), peer.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let old_intent = dfs.coordinator().repair_jobs()[0].intent_id.clone();
    let advanced = dfs.posix_set_mode(path, 0o640).unwrap();
    assert_ne!(advanced.version, first.version);
    assert_eq!(advanced.content_hash, first.content_hash);
    let row = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&advanced.object_id, &advanced.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        row.content_durability.unwrap().source_version,
        first.version
    );
    assert!(dfs.writes.pending_writes.contains_key(&old_intent));
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    dfs.repair_retry_tick = 32;
    dfs.retry_pending_repairs().unwrap();
    runtime
        .maintenance_tick(&owner, Duration::from_secs(5))
        .unwrap()
        .outcome
        .unwrap();
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == peer
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed),
        "the recorded same-byte obligation must progress through an actual receiver"
    );
    let actual_peer = runtime.client(&peer).unwrap();
    assert_eq!(
        actual_peer
            .content()
            .read_object_version(first.object_id.as_str(), first.version.as_str())
            .unwrap()
            .1,
        b"same immutable bytes"
    );
    assert_eq!(dfs.posix_stat(path).unwrap().version, advanced.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"same immutable bytes");
    drop(actual_peer);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn receipt_updates_visible_current_locations_for_current_version() {
    let (runtime, mut dfs, stat, receipt) = native_withdrawn_local_replica_fixture(
        "native-receipt-restores-current-location",
        b"real replica location bytes",
    );
    let before = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    assert_eq!(before.current_locations.len(), 1);
    assert!(!before.current_locations.contains(&receipt.replica_node_id));
    assert!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            receipt.content_hash.as_deref().unwrap(),
        )
        .unwrap()
    );
    let object = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let mut expected = before;
    expected.current_locations.push(receipt.replica_node_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .as_ref(),
        Some(&expected)
    );
    assert_eq!(
        object, expected,
        "only the genuine replica location changes"
    );
    assert_eq!(object.version, stat.version);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn stat_derives_current_locations_from_matching_receipts_without_writing() {
    let (runtime, mut service, config) = crate::test_support::current_native_metadata_service(
        "native-stat-durable-receipts",
        2,
        "/services",
    )
    .unwrap();
    let replicas = std::iter::once(config.primary_node_id.clone())
        .chain(config.replica_node_ids.clone())
        .collect::<Vec<_>>();
    let object = service
        .posix_write_file(
            "/services/receipt-locations",
            b"actual receipt bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: replicas.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let shard = DfsMetadataShardId::new(config.shard_id);
    let before = service
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    for _ in 0..4 {
        let stat = service
            .posix_stat_object(&object.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            stat.current_locations.into_iter().collect::<BTreeSet<_>>(),
            replicas.iter().cloned().collect()
        );
    }
    let after = service
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    assert_eq!(
        after, before,
        "ordinary stat derives its answer without authoring metadata or receipt rows"
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn hydrate_from_metadata_backend_scans_receipts_once_globally() {
    let path = "/services/native-global-hydration";
    let (runtime, owner, nodes, mut dfs, policy) =
        native_owned_repair_fixture("native-global-receipt-hydration", path, 1, 2, true);
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    dfs.posix_mkdir_all("/services/native-global-hydration")
        .unwrap();
    let mut intents = Vec::new();
    for index in 0..3 {
        let stat = dfs
            .posix_write_file(
                &format!("/services/native-global-hydration/file-{index}"),
                format!("actual globally scanned receipt {index}").as_bytes(),
                DfsWriteOptions {
                    policy: policy.clone(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let job = dfs
            .coordinator()
            .repair_jobs()
            .iter()
            .find(|job| job.object_id == stat.object_id)
            .unwrap();
        intents.push(job.intent_id.clone());
    }
    drop(dfs);
    let mut recovered = runtime.metadata_primary_service(path).unwrap();
    recovered
        .metadata_backend()
        .unwrap()
        .reset_test_receipt_scan_counts_by_shard();
    // Sender activation belongs to the existing owned recovery boundary;
    // ordinary read hydration must not silently acquire repair execution.
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let counts = recovered
        .metadata_backend()
        .unwrap()
        .test_receipt_scan_counts_by_shard();
    assert!(!counts.is_empty());
    assert!(
        counts.values().all(|count| *count == (1, 0)),
        "one bounded global pass per actual physical shard, no per-intent receipt rescan: {counts:?}"
    );
    for intent in intents {
        assert_eq!(
            recovered.coordinator().receipts_for_intent(&intent).len(),
            1
        );
        assert!(recovered.writes.pending_writes.contains_key(&intent));
    }
    assert_eq!(recovered.coordinator().repair_jobs().len(), 3);
    drop(recovered);
    drop(background);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn queued_repair_restore_rebuilds_once_and_preserves_inactive_history() {
    let name = "native-queued-repair-bulk-restore";
    let (runtime, owner, peer, mut service) = native_missing_content_repair(name);
    service
        .posix_mkdir_all("/services/native-bulk-restoration")
        .unwrap();
    let mut first_path = String::new();
    for index in 0..32 {
        let path = format!("/services/native-bulk-restoration/{index}.bin");
        if index == 0 {
            first_path = path.clone();
        }
        service
            .posix_write_file(
                &path,
                b"actual bulk repair bytes",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                    replica_candidates: vec![owner.clone(), peer.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
    }
    let records = service.durable_repair_queue_records();
    assert_eq!(records.len(), 32);
    let first_object = service.posix_stat(&first_path).unwrap().object_id;
    let inactive = records
        .iter()
        .find(|record| record.object_id == first_object)
        .unwrap()
        .clone();
    // A genuine newer current generation retires the old byte obligation.
    service
        .posix_write_file(
            &first_path,
            b"new current bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(service);
    let mut restored = runtime.client(&owner).unwrap();
    assert!(
        restored.writes.pending_writes.is_empty(),
        "ordinary client begins lazy, with no installed fake coordinator state"
    );
    assert!(
        restored
            .retry_pending_repairs_bounded_with_attempts(1, std::slice::from_ref(&inactive))
            .unwrap()
            .is_empty()
    );
    assert!(
        restored
            .coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != inactive.intent_id)
    );
    let rebuilds = restored.coordinator.repair_job_rebuild_count;
    restored
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    assert_eq!(restored.coordinator.repair_job_rebuild_count, rebuilds + 1);
    assert_eq!(restored.coordinator().repair_jobs().len(), 31);
    assert!(
        restored
            .coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != inactive.intent_id)
    );
    let jobs = restored.coordinator().repair_jobs().to_vec();
    restored
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    assert_eq!(restored.coordinator.repair_job_rebuild_count, rebuilds + 1);
    assert_eq!(restored.coordinator().repair_jobs(), jobs);
    assert_eq!(
        restored.posix_read_file(&first_path).unwrap(),
        b"new current bytes"
    );
    drop(restored);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn hydrate_from_metadata_backend_restores_committed_pending_write_for_missing_repair() {
    let path = "/services/native-missing-repair";
    let (mut runtime, owner, nodes, mut dfs, policy) = native_owned_repair_fixture(
        "native-hydration-committed-missing-repair",
        path,
        1,
        2,
        true,
    );
    let peer = nodes.into_iter().find(|node| *node != owner).unwrap();
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let stat = dfs
        .posix_write_file(
            "/services/native-missing-repair",
            b"repair after restart bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), peer],
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(dfs.coordinator().repair_jobs().len(), 1);
    let intent_id = dfs.coordinator().repair_jobs()[0].intent_id.clone();
    let native_intent = dfs
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(native_intent.status, DfsWriteIntentStatus::Committed);
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut recovered = runtime.metadata_primary_service(path).unwrap();
    recovered
        .hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    assert!(
        recovered.writes.pending_writes.contains_key(&intent_id),
        "real committed missing-N receipts must restore pending repair state"
    );
    assert_eq!(recovered.coordinator().repair_jobs().len(), 1);
    assert_eq!(
        recovered
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &intent_id)
            .unwrap()
            .unwrap(),
        native_intent
    );
    assert_eq!(
        recovered.posix_read_file(&stat.path).unwrap(),
        b"repair after restart bytes"
    );
    drop(recovered);
    drop(background);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn hydrate_from_metadata_backend_skips_non_coordinator_repair_sender_state() {
    let name = "native-observer-repair-hydration";
    let (runtime, initial, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    let others = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != owner)
        .collect::<Vec<_>>();
    let replica = others[0].clone();
    let observer = others[1].clone();
    drop(initial);
    let mut primary = runtime.metadata_primary_service("/services").unwrap();
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    policy.forbidden_node_ids = vec![observer.clone()];
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
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
    let stat = primary
        .posix_write_file(
            "/services/native-observer-hydration",
            b"repair stays with actual metadata primary",
            DfsWriteOptions {
                policy,
                replica_candidates: vec![owner.clone(), replica],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let intent_id = primary.coordinator().repair_jobs()[0].intent_id.clone();
    let object = primary
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let target = primary
        .capture_metadata_quorum_sync_target(&object)
        .unwrap();
    primary
        .sync_metadata_shard_to_quorum_intersecting(
            &target,
            std::slice::from_ref(&observer),
            Duration::from_secs(5),
        )
        .unwrap();
    drop(primary);
    let mut observed = runtime.client(&observer).unwrap();
    observed.hydrate_from_metadata_backend().unwrap();
    assert!(
        observed
            .coordinator()
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id)
    );
    assert!(
        !observed.writes.pending_writes.contains_key(&intent_id),
        "metadata observation grants no repair sender role"
    );
    let before = runtime.rpc_observations().len();
    assert!(observed.retry_pending_repairs().unwrap().is_empty());
    assert!(!runtime.rpc_observations()[before..].iter().any(|event| {
        event.source == observer
            && [
                DFS_DATA_STAGE_BLOB_ROUTE,
                DFS_DATA_REPAIR_REPLICA_ROUTE,
                DFS_DATA_FETCH_BLOB_ROUTE,
            ]
            .contains(&event.route.as_str())
            && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
    }));
    drop(observed);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn operational_metrics_report_repair_backlog_and_content_footprint() {
    let path = "/services/native-metrics";
    let (runtime, owner, nodes, mut dfs, policy) =
        native_owned_repair_fixture("native-repair-backlog-metrics", path, 1, 2, true);
    dfs.hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let peer = nodes.into_iter().find(|node| *node != owner).unwrap();
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let before = dfs.operational_metrics().unwrap();
    let bytes = b"metrics should expose real repair backlog and content footprint";
    drop(dfs);
    // The normal requester/Main response completes its staged local binding.
    // A Core effect opener observes and recovers the owning repair obligation;
    // it does not stand in for this public requester completion.
    let mut requester = runtime.client(&owner).unwrap();
    let stat = requester
        .posix_write_file(
            "/services/native-metrics",
            bytes,
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), peer.clone()],
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    dfs.hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    let metrics = dfs.operational_metrics().unwrap();
    // Copy the same service's actual sender DATA beside the counter capture.
    // No getter or another run's sender lifetime classifies this local view.
    let local_senders = dfs
        .writes
        .pending_writes
        .iter()
        .map(|(intent, pending)| {
            (
                intent.clone(),
                pending.object_id.clone(),
                pending.version.clone(),
                pending.content_hash.clone(),
                pending.size_bytes,
                pending.foreground_replica_repair,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(metrics.pending_write_count, local_senders.len());
    assert_eq!(metrics.node_id, owner);
    assert!(metrics.metadata_backend_configured);
    assert_eq!(metrics.file_system_count, before.file_system_count);
    assert!(dfs.metadata().object(&stat.object_id).is_some());
    assert!(metrics.object_count >= before.object_count + 1);
    assert_eq!(
        metrics.coordinator_intent_count,
        before.coordinator_intent_count + 1
    );
    assert_eq!(
        metrics.coordinator_receipt_count,
        before.coordinator_receipt_count + 1
    );
    assert_eq!(
        metrics.coordinator_committed_version_count,
        before.coordinator_committed_version_count + 1
    );
    assert_eq!(
        metrics.repair_job_count,
        1,
        "actual jobs: {:?}",
        dfs.coordinator().repair_jobs()
    );
    let job = &dfs.coordinator().repair_jobs()[0];
    assert_eq!(job.object_id, stat.object_id);
    assert_eq!(job.version, stat.version);
    assert_eq!(job.missing_replica_node_id, peer);
    let stored_version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&stat.object_id, &stat.version)
        .unwrap()
        .unwrap();
    assert!(stored_version.visible);
    assert_eq!(stored_version.content_hash, stat.content_hash);
    assert_eq!(stored_version.size_bytes, Some(bytes.len() as u64));
    // Missing-N work is proved by its durable committed job. A local sender,
    // when retained, must be this SAME byte obligation and background mode;
    // a closed local sender cannot stand in for absence of the repair job.
    for (intent, object, version, hash, size, foreground) in &local_senders {
        assert_eq!(intent, &job.intent_id);
        assert_eq!(object, &stat.object_id);
        assert_eq!(version, &stat.version);
        assert_eq!(Some(hash.as_str()), stat.content_hash.as_deref());
        assert_eq!(*size, bytes.len() as u64);
        assert!(!foreground);
    }
    assert_eq!(metrics.pending_read_count, 0);
    assert_eq!(metrics.stage_inflight_count, 0);
    assert_eq!(metrics.repair_inflight_count, 0);
    assert_eq!(metrics.fetch_inflight_count, 0);
    assert_eq!(metrics.content.blob_count, before.content.blob_count + 1);
    assert_eq!(
        metrics.content.manifest_count,
        before.content.manifest_count + 1
    );
    assert_eq!(
        metrics.content.logical_object_bytes,
        before.content.logical_object_bytes + bytes.len() as u64
    );
    assert_eq!(metrics.content.staging_file_count, 0);
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(manifest.object_id, stat.object_id.as_str());
    assert_eq!(manifest.version, stat.version.as_str());
    assert_eq!(
        Some(manifest.content_hash.as_str()),
        stat.content_hash.as_deref()
    );
    assert_eq!(manifest.content_hash, content_hash(bytes));
    assert_eq!(manifest.size_bytes, bytes.len() as u64);
    let (read_manifest, read_bytes) = dfs
        .content()
        .read_object_version(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(read_manifest, manifest);
    assert_eq!(read_bytes.as_slice(), bytes);
    drop(dfs);
    drop(background);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn apply_committed_intent_tolerates_missing_local_repair_blob() {
    let name = "native-apply-committed-missing-repair-blob";
    let (runtime, mut dfs, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let replica_a = peers[0].clone();
    let replica_b = peers[1].clone();
    let path = format!("/services/{name}.bin");
    let base = dfs
        .posix_write_file(
            &path,
            b"actual stage predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    // The following apply/repair assertions belong to the actual producer,
    // not the browser client's routed pending-request identity.
    drop(dfs);
    let mut dfs = runtime.metadata_primary_service(&path).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x6b; 2 * 1024 * 1024 + 4096];
    let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let source_content = dfs.content().clone();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica_a.clone()),
        route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(owner.clone()),
            target: Some(replica_b.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica_a.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let before = runtime.rpc_observations().len();
    let writer_request = request.clone();
    let writer_bytes = bytes.clone();
    let writer = std::thread::spawn(move || {
        let outcome = dfs.write_distributed_reader_with_candidates(
            writer_request,
            std::io::Cursor::new(writer_bytes),
            nodes,
            policy,
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        );
        (dfs, outcome)
    });
    let reached = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            loop {
                if runtime.rpc_observations()[before..].iter().any(|event| {
                    event.source == owner
                        && event.target == replica_a
                        && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                        && event.phase == crate::test_support::DfsTestRpcPhase::AfterDispatch
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
    });
    let removed = source_content.debug_remove_loose_blob(&content_hash(&bytes));
    gate.add_permits(1);
    let (mut dfs, outcome) = writer.join().unwrap();
    reached.unwrap();
    removed.unwrap();
    let outcome = outcome.unwrap();
    let intent = match outcome {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. }
        | NetworkedDfsWriteOutcome::Committed { intent_id, .. } => intent_id,
    };
    // The real peer's held Stage reply crosses W after actual local disk loss.
    dfs.process_incoming().unwrap();
    dfs.apply_committed_intent(&intent)
        .expect("actual W publication tolerates unavailable eager repair bytes");
    let object = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(object.version, request.target_version);
    assert_eq!(
        object
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica_a.clone()])
    );
    assert!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    let job = dfs
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| job.intent_id == intent)
        .unwrap();
    assert_eq!(job.missing_replica_node_id, replica_b);
    let pending = dfs.writes.pending_writes.get(&intent).unwrap();
    assert!(pending.repair_retries.contains_key(&replica_b));
    assert!(!pending.repair_inflight_request_ids.contains_key(&replica_b));
    let peer = runtime.client(&replica_a).unwrap();
    assert_eq!(
        peer.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(peer);
    runtime.clear_faults();
    dfs.posix_sync_file(&path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let repaired = runtime.client(&replica_b).unwrap();
    assert_eq!(
        repaired
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(repaired);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn repair_blob_sources_use_backend_current_locations_over_stale_memory() {
    let name = "native-repair-sources-current-backend";
    let (runtime, seed, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(seed);
    let primary = config.primary_node_id;
    let peers = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let missing = peers[0].clone();
    let requester = peers[1].clone();
    let path = format!("/services/{name}.bin");
    // This test performs an Eventual overwrite. The shared read fixture creates
    // Confirmed objects, whose immutable policy cannot admit a W1/N2 write.
    let mut initial = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    initial.required_node_ids = vec![primary.clone()];
    initial.forbidden_node_ids = peers;
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, initial)
        .unwrap();
    let mut seed = runtime.client(&primary).unwrap();
    let (resolved, _, initial_replication) = seed
        .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedEventual
    );
    let base = seed
        .posix_write_file(
            &path,
            b"actual old source bytes",
            DfsWriteOptions {
                replica_candidates: vec![primary.clone()],
                policy: initial_replication,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(
        seed.current_read_object_metadata(&base.object_id)
            .unwrap()
            .policy,
        DfsObjectPolicy::ReplicatedEventual
    );
    drop(seed);
    let mut stale = runtime.client(&primary).unwrap();
    stale.hydrate_from_metadata_backend().unwrap();
    let old_memory = stale.metadata().object(&base.object_id).unwrap().clone();
    assert_eq!(old_memory.version, base.version);
    assert_eq!(old_memory.current_locations, vec![primary.clone()]);
    let nodes = vec![requester.clone(), missing.clone()];
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    placement.forbidden_node_ids = vec![primary.clone()];
    runtime
        .apply_placement_policy(&primary, &base.path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    deny_native_read_repair(&runtime, &primary, &missing);
    let mut writer = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x51; 2 * 1024 * 1024 + 11];
    let mut policy = DfsReplicationPolicy::new(1, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    policy.forbidden_node_ids = vec![primary.clone()];
    writer
        .write_distributed_bytes_with_base(
            request.clone(),
            Some(base.version),
            bytes.clone(),
            nodes,
            policy,
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Background,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    writer.process_incoming().unwrap();
    let current = stale
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, request.target_version);
    assert_eq!(
        current.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(current.current_locations, vec![requester.clone()]);
    assert_eq!(
        stale.metadata().object(&request.object_id).unwrap().version,
        old_memory.version,
        "this is an actual retained old client projection across a normal new publication"
    );
    let mut restored = runtime.metadata_primary_service(&path).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let job = restored
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| job.object_id == request.object_id && job.missing_replica_node_id == missing)
        .unwrap()
        .clone();
    let pending = restored.writes.pending_writes.get(&job.intent_id).unwrap();
    assert_eq!(pending.object_id, request.object_id);
    assert_eq!(pending.version, request.target_version);
    assert_eq!(
        stale.repair_blob_sources(&job, pending).unwrap(),
        vec![requester]
    );
    drop(restored);
    drop(writer);
    drop(stale);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn retry_pending_repairs_is_idle_noop_without_distributed_work() {
    let (runtime, mut dfs, _) = crate::test_support::current_native_metadata_service(
        "native-idle-repair-noop",
        1,
        "/services",
    )
    .unwrap();
    dfs.repair_retry_tick = 41;
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert!(dfs.retry_pending_repairs().unwrap().is_empty());
    assert_eq!(dfs.repair_retry_tick, 41);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    drop(dfs);
    runtime.close().unwrap();
}

fn native_fetch_retry_fixture(
    name: &str,
    change: crate::test_runtime::FetchReplyFault,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    Vec<u8>,
    String,
    crate::test_runtime::CapturedFetchReply,
) {
    let (runtime, client, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(client);
    // The mounted client delegates mutation to Main. This fixture observes
    // its actual owning Core's coordinator and transfer state instead.
    let mut dfs = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(dfs.node_id(), &owner);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let source = peers[0].clone();
    let target = peers[1].clone();
    let path = format!("/services/{name}.bin");
    let base = dfs
        .posix_write_file(
            &path,
            b"actual fetch retry predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(owner.clone()),
            target: Some(target.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x63; 2 * 1024 * 1024 + 4096];
    let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = dfs
        .write_distributed_reader_with_candidates(
            request.clone(),
            std::io::Cursor::new(bytes.clone()),
            nodes,
            policy,
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Foreground,
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    dfs.settle_ready_write_outcome(outcome, true).unwrap();
    let intent_id = dfs
        .coordinator()
        .repair_jobs()
        .iter()
        .find(|job| {
            job.object_id == request.object_id
                && job.version == request.target_version
                && job.missing_replica_node_id == target
        })
        .unwrap()
        .intent_id
        .clone();
    for _ in 0..32 {
        if dfs
            .writes
            .pending_writes
            .get(&intent_id)
            .unwrap()
            .repair_retries
            .contains_key(&target)
        {
            break;
        }
        dfs.retry_pending_repairs_bounded(1).unwrap();
    }
    let pending = dfs.writes.pending_writes.get(&intent_id).unwrap();
    assert!(
        pending.stage_retries.contains_key(&target),
        "real denied initial Stage created retry state"
    );
    assert!(
        pending.repair_retries.contains_key(&target),
        "real denied post-W Repair created retry state"
    );
    dfs.content()
        .debug_remove_loose_blob(&content_hash(&bytes))
        .unwrap();
    assert!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    let captured = Arc::new(Mutex::new(Vec::new()));
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(source.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::AlterFetchReplyAfterDispatch {
            change,
            captured: Arc::clone(&captured),
        },
    });
    for _ in 0..32 {
        dfs.retry_pending_repairs_bounded(1).unwrap();
        if !captured.lock().unwrap().is_empty() {
            break;
        }
    }
    let records = captured.lock().unwrap();
    assert_eq!(
        records.len(),
        1,
        "one actual normal Fetch response was captured"
    );
    let captured = records[0].clone();
    assert_eq!(captured.routed.source_node_id, source);
    assert_eq!(captured.routed.target_node_id, owner);
    let DfsNetworkMessage::FetchBlobResponse(response) = &captured.routed.message else {
        unreachable!()
    };
    assert_eq!(response.intent_id, intent_id);
    assert_eq!(response.object_id, request.object_id);
    assert_eq!(response.version, request.target_version);
    assert!(
        matches!(&response.result, DfsFetchBlobResult::Found { content_hash: hash, bytes: payload }
        if hash == &content_hash(&bytes) && payload == &bytes)
    );
    let pending = dfs.writes.pending_writes.get(&intent_id).unwrap();
    assert_eq!(
        pending.fetch_inflight_request_ids.get(&source),
        Some(&response.request_id)
    );
    assert!(pending.fetch_inflight_from.contains(&source));
    (
        runtime, owner, source, target, dfs, request, bytes, intent_id, captured,
    )
}

fn assert_real_fetch_retry_wake(
    dfs: &NetworkedDfsService<DfsNodeTransport>,
    intent: &str,
    source: &NodeId,
    target: &NodeId,
) {
    let pending = dfs.writes.pending_writes.get(intent).unwrap();
    assert!(!pending.fetch_inflight_from.contains(source));
    assert!(!pending.fetch_inflight_started_ticks.contains_key(source));
    assert!(pending.fetch_failed_from.contains(source));
    let stage = pending.stage_retries.get(target).unwrap();
    assert_eq!(stage.next_retry_tick, 0);
    assert!(!stage.inflight);
    let repair = pending.repair_retries.get(target).unwrap();
    assert_eq!(repair.next_retry_tick, 0);
    assert!(!repair.inflight);
}

fn settle_real_fetch_retry(
    runtime: &crate::test_runtime::DfsTestRuntime,
    owner: &NodeId,
    target: &NodeId,
    dfs: &mut NetworkedDfsService<DfsNodeTransport>,
    request: &DfsWriteRequest,
    bytes: &[u8],
) {
    runtime.clear_faults();
    let current = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    dfs.posix_sync_file(&current.path, crate::DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    let target_dfs = runtime.client(target).unwrap();
    assert_eq!(
        target_dfs
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| &event.source == owner
                && &event.target == target
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
}

#[test]
fn stale_fetch_request_expiry_wakes_stage_and_repair_retries() {
    let (runtime, owner, source, target, mut dfs, request, bytes, intent, captured) =
        native_fetch_retry_fixture(
            "native-stale-fetch-expiry",
            crate::test_runtime::FetchReplyFault::Missing,
        );
    let original_request_id = match &captured.routed.message {
        DfsNetworkMessage::FetchBlobResponse(response) => response.request_id.clone(),
        _ => unreachable!(),
    };
    // Use the ordinary retry-page clock with the real missing target excluded.
    // Expiry is observed before this target can schedule a replacement fetch.
    let exclusions = DfsMaintenanceExecutionExclusions::new([target.clone()]);
    for _ in 0..=fetch_inflight_timeout_ticks() {
        crate::test_runtime::executor()
            .block_on(dfs.retry_pending_repair_page(1, &[], &exclusions, None, None, true))
            .unwrap();
        if !dfs
            .writes
            .pending_writes
            .get(&intent)
            .unwrap()
            .fetch_inflight_from
            .contains(&source)
        {
            break;
        }
    }
    assert_real_fetch_retry_wake(&dfs, &intent, &source, &target);
    assert_eq!(
        dfs.writes
            .pending_writes
            .get(&intent)
            .unwrap()
            .fetch_inflight_request_ids
            .get(&source),
        Some(&original_request_id),
        "expiry preserves the real late response identity"
    );
    assert!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err(),
        "the expired original Fetch has not supplied the missing native body"
    );
    // A wrong request identity remains negative even after scheduling expiry.
    // Only that field changes in this copy of the actual captured native reply.
    let mut wrong_reply = captured.routed.clone();
    let DfsNetworkMessage::FetchBlobResponse(wrong) = &mut wrong_reply.message else {
        unreachable!()
    };
    wrong.request_id.push_str(":wrong-late-request");
    dfs.transport.test_enqueue_protocol_message(wrong_reply);
    let rejected = dfs.process_incoming().unwrap();
    assert!(rejected.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobFetchRejected { intent_id, object_id, source_node_id, reason }
        if intent_id == &intent && object_id == &request.object_id && source_node_id == &source
            && reason == "unexpected DFS fetch response request id")));
    assert_eq!(
        dfs.writes
            .pending_writes
            .get(&intent)
            .unwrap()
            .fetch_inflight_request_ids
            .get(&source),
        Some(&original_request_id)
    );
    assert!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    let before_late_reply = runtime.rpc_observations().len();
    // Replay the unchanged Found DTO captured from the authenticated native
    // source into the SAME original transport ingress. This adds no second
    // carrier hop or authority; normal owned processing validates the body.
    dfs.transport.test_enqueue_protocol_message(captured.routed);
    let events = dfs.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobFetched { intent_id, object_id, version, source_node_id, content_hash: hash }
        if intent_id == &intent && object_id == &request.object_id && version == &request.target_version
            && source_node_id == &source && hash == &content_hash(&bytes))));
    assert!(events.iter().all(|event| !matches!(event,
        NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, .. }
            if source_node_id == &source)));
    assert_eq!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .unwrap()
            .1,
        bytes,
        "the exact late original reply supplies the actual missing native body"
    );
    let pending = dfs.writes.pending_writes.get(&intent).unwrap();
    assert!(!pending.fetch_inflight_request_ids.contains_key(&source));
    assert!(!pending.fetch_inflight_from.contains(&source));
    assert!(
        runtime.rpc_observations()[before_late_reply..]
            .iter()
            .all(|event| event.route != DFS_DATA_FETCH_BLOB_ROUTE),
        "accepting the exact late body requires no replacement Fetch"
    );
    settle_real_fetch_retry(&runtime, &owner, &target, &mut dfs, &request, &bytes);
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn fetch_rejection_wakes_stage_and_repair_retries() {
    let (runtime, owner, source, target, mut dfs, request, bytes, intent, _) =
        native_fetch_retry_fixture(
            "native-fetch-negative-rejection",
            crate::test_runtime::FetchReplyFault::NotFound,
        );
    let events = dfs.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, reason, .. }
            if source_node_id == &source && reason == "blob not found")));
    assert_real_fetch_retry_wake(&dfs, &intent, &source, &target);
    assert!(
        !dfs.writes
            .pending_writes
            .get(&intent)
            .unwrap()
            .fetch_inflight_request_ids
            .contains_key(&source)
    );
    settle_real_fetch_retry(&runtime, &owner, &target, &mut dfs, &request, &bytes);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn fetch_store_failure_wakes_stage_and_repair_retries() {
    let (runtime, owner, source, target, mut dfs, request, bytes, intent, _) =
        native_fetch_retry_fixture(
            "native-fetch-negative-store-failure",
            crate::test_runtime::FetchReplyFault::CorruptPayload,
        );
    let events = dfs.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::BlobFetchRejected { source_node_id, reason, .. }
            if source_node_id == &source && reason.contains("content hash mismatch"))));
    assert_real_fetch_retry_wake(&dfs, &intent, &source, &target);
    assert!(
        !dfs.writes
            .pending_writes
            .get(&intent)
            .unwrap()
            .fetch_inflight_request_ids
            .contains_key(&source)
    );
    let binding = dfs
        .content()
        .read_manifest(request.object_id.as_str(), request.target_version.as_str())
        .unwrap();
    assert_eq!(
        binding.content_hash,
        content_hash(&bytes),
        "a rejected response preserves the original valid binding"
    );
    assert!(
        dfs.content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err(),
        "corrupt captured bytes cannot replace the missing valid blob"
    );
    settle_real_fetch_retry(&runtime, &owner, &target, &mut dfs, &request, &bytes);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn repair_retry_schedule_does_not_double_count_completed_inflight_attempt() {
    let name = "native-repair-attempt-completion-count";
    let path = "/services/native-attempt-count";
    let (runtime, owner, nodes, core, policy) = native_owned_repair_fixture(name, path, 1, 2, true);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    let background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    drop(core);
    // Main owns the normal write and durable missing-target enrollment. Keep
    // its background claim parked while this exact Core retry is selected.
    let mut requester = runtime.client(&owner).unwrap();
    let stat = requester
        .posix_write_file(
            "/services/native-attempt-count",
            b"actual pending attempt bytes",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(requester);
    let mut dfs = runtime.metadata_primary_service(path).unwrap();
    dfs.hydrate_from_metadata_backend_for_commit_promotion_recovery()
        .unwrap();
    runtime.clear_faults();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::LoseReplyAfterDispatch,
    });
    let before = runtime.rpc_observations().len();
    assert!(dfs.retry_pending_repairs_bounded(1).unwrap().is_empty());
    let records = dfs.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].object_id, stat.object_id);
    assert_eq!(
        records[0].attempts, 1,
        "one real lost-response send is one failed attempt"
    );
    assert!(!records[0].in_flight);
    assert!(
        records[0]
            .last_error
            .as_deref()
            .is_some_and(|reason| reason.contains("lost response"))
    );
    let intent = records[0].intent_id.clone();
    let state = dfs
        .writes
        .pending_writes
        .get(&intent)
        .unwrap()
        .repair_retries
        .get(&replica)
        .unwrap();
    assert_eq!(state.attempts, 1);
    assert!(!state.inflight);
    assert!(!dfs.repair_retry_is_due(&intent, &replica));
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::EndedWithoutResponse)
    );
    let received = runtime.client(&replica).unwrap();
    assert_eq!(
        received
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        b"actual pending attempt bytes",
        "the real receiver completed before only its reply was lost"
    );
    drop(received);
    runtime.clear_faults();
    dfs.posix_sync_file(&stat.path, DfsPosixSyncKind::File)
        .unwrap();
    let sends = runtime.rpc_observations()[before..]
        .iter()
        .filter(|event| {
            event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
        })
        .count();
    assert_eq!(
        sends, 2,
        "one failed reply plus one actual exact retry, no double-counted send"
    );
    assert!(
        dfs.coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != intent)
    );
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    drop(dfs);
    drop(background);
    runtime.close().unwrap();
}

pub(super) fn native_committed_repair_backlog(
    name: &str,
    intercept_background: bool,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    Option<impl Send + use<>>,
) {
    let (runtime, owner, replica, service) = native_missing_content_repair(name);
    drop(service);
    let background = intercept_background.then(|| {
        runtime
            .deny_background_replica_before_claim(&owner)
            .unwrap()
    });
    let path = format!("/services/{name}.bin");
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone(), replica.clone()];
    let accepted = runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    // Finish this exact policy job while its new path is still empty. Otherwise
    // its normal scan can create a different placement repair for the later
    // seed, so a fault bound to that seed's original intent would miss it.
    let control = runtime.shared_control_plane().unwrap();
    let read_accepted = || {
        control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .expect("the exact accepted native policy job remains observable")
    };
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // The empty normal job has only its Scanning and Repairing passes. This is
    // one bounded setup window, not a retry of failed work or a phase override.
    for _ in 0..2 {
        let current = read_accepted();
        assert_eq!(current.job_id, accepted.job_id);
        assert_eq!(current.file_system_id, accepted.file_system_id);
        assert_eq!(current.path, accepted.path);
        assert_eq!(current.scope, accepted.scope);
        assert_eq!(current.accepted_config, accepted.accepted_config);
        assert_eq!(current.candidate_snapshot, accepted.candidate_snapshot);
        assert_eq!(current.placement_snapshot, accepted.placement_snapshot);
        assert_eq!(current.scanned_object_count, 0);
        assert_eq!(current.matched_object_count, 0);
        assert_eq!(current.scheduled_repair_count, 0);
        assert!(
            current.last_error.is_none(),
            "exact empty Apply job: {current:?}"
        );
        if current.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        assert!(matches!(
            current.state,
            DfsPlacementReconciliationState::Pending
                | DfsPlacementReconciliationState::Scanning
                | DfsPlacementReconciliationState::Repairing
        ));
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "the empty policy job exhausted its normal setup window"
        );
        let tick = runtime.maintenance_tick(&owner, remaining).unwrap();
        assert!(
            tick.outcome.is_ok(),
            "normal empty Apply progress: {:?}",
            tick.outcome
        );
        assert_eq!(tick.omitted_kv_event_count, 0);
    }
    let completed = read_accepted();
    assert_eq!(completed.job_id, accepted.job_id);
    assert_eq!(completed.file_system_id, accepted.file_system_id);
    assert_eq!(completed.path, path);
    assert_eq!(completed.scope, accepted.scope);
    assert_eq!(completed.accepted_config, accepted.accepted_config);
    assert_eq!(completed.candidate_snapshot, accepted.candidate_snapshot);
    assert_eq!(completed.placement_snapshot, accepted.placement_snapshot);
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.total_object_count, 0);
    assert_eq!(completed.scanned_object_count, 0);
    assert_eq!(completed.matched_object_count, 0);
    assert_eq!(completed.scheduled_repair_count, 0);
    assert_eq!(completed.pending_repair_count, 0);
    assert!(completed.last_error.is_none());
    assert!(
        Instant::now() < setup_deadline,
        "the empty policy job completed outside its normal setup window"
    );
    drop(control);
    // Open Main only after the actual Apply has completed, so its retained
    // path policy includes the same full required-node constraints.
    let mut service = runtime.client(&owner).unwrap();
    let (resolved, _, replication) = service
        .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedEventual, 2)
        .unwrap();
    assert_eq!(
        resolved.policy.consistency,
        DfsObjectPolicy::ReplicatedEventual
    );
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 2);
    assert_eq!(
        replication
            .required_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [owner.clone(), replica.clone()].into_iter().collect()
    );
    let expected_replication = replication.clone();
    let bytes = (0..256 * 1024 + 31)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let stat = service
        .posix_write_file(
            &path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: replication,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(service);
    // Main performed the write. Borrow its real current CoreEffect owner and
    // recover only actual stored full intent/receipt and durable queue DATA.
    let mut service = runtime.metadata_primary_service(&path).unwrap();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let captured = queue.view().unwrap().records;
    let actual = captured
        .iter()
        .find(|row| {
            row.object_id == stat.object_id
                && row.version == stat.version
                && Some(row.content_hash.as_str()) == stat.content_hash.as_deref()
                && row.missing_replica_node_id == replica
        })
        .expect("actual Main durably enrolled its canonical missing target");
    let intent_id = actual.intent_id.clone();
    let history = service
        .metadata_backend()
        .unwrap()
        .object_write_intent_history(&stat.object_id)
        .unwrap();
    let record = history
        .intents
        .into_iter()
        .find(|record| record.intent_id == intent_id)
        .or_else(|| {
            history
                .receipts
                .into_iter()
                .find(|receipt| {
                    receipt.intent_id == intent_id
                        && receipt.publication.is_current()
                        && receipt.status == DfsWriteIntentStatus::Committed
                        && receipt.file_system_id == stat.file_system_id
                        && receipt.object_id == stat.object_id
                        && receipt.target_version == stat.version
                        && receipt.content_hash == stat.content_hash
                })
                .map(|receipt| receipt.to_intent_record())
        })
        .expect(
            "actual full owner or exact canonical Current receipt supports normal W-to-N recovery",
        );
    assert!(record.publication.is_current());
    assert_eq!(record.file_system_id, stat.file_system_id);
    assert_eq!(record.object_id, stat.object_id);
    assert_eq!(record.coordinator_node_id, owner);
    assert_eq!(record.target_version, stat.version);
    assert_eq!(record.content_hash, stat.content_hash);
    assert_eq!(record.policy.min_commit_replicas, 1);
    assert_eq!(record.policy.desired_replicas, 2);
    assert_eq!(record.policy, expected_replication);
    assert_eq!(record.desired_replica_node_ids.len(), 2);
    assert!(record.desired_replica_node_ids.contains(&owner));
    assert!(record.desired_replica_node_ids.contains(&replica));
    service
        .retry_pending_repairs_bounded_with_attempts(0, &captured)
        .unwrap();
    assert!(
        service
            .coordinator()
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id
                && job.object_id == stat.object_id
                && job.missing_replica_node_id == replica),
        "normal CoreEffect recovery derives the actual owner's missing target"
    );
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    let receipts = service
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == owner)
    );
    assert!(
        !receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    (
        runtime, owner, replica, service, stat, intent_id, background,
    )
}

#[test]
fn repair_retry_schedule_preserves_network_error_diagnostics() {
    let (runtime, owner, replica, mut service, stat, intent_id, background_interception) =
        native_committed_repair_backlog("native-retry-network-diagnostics", true);
    assert!(
        background_interception.is_some(),
        "select the genuine foreground owner before clearing transport faults"
    );
    runtime.clear_faults();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    let before = runtime.rpc_observations().len();
    service.retry_pending_repairs_bounded(1).unwrap();
    let records = service.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.intent_id, intent_id);
    assert_eq!(record.object_id, stat.object_id);
    assert_eq!(record.version, stat.version);
    assert_eq!(record.missing_replica_node_id, replica);
    assert_eq!(record.attempts, 1);
    assert!(!record.in_flight);
    assert!(
        record
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("no usable route"))
    );
    assert!(crate::repair::repair_attempt_record_indicates_transient_route_failure(record));
    let observations = runtime.rpc_observations();
    assert!(
        observations[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse)
    );
    assert!(
        !observations[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
    drop(service);
    drop(background_interception);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_retry_schedule_caps_transient_route_backoff() {
    let (runtime, owner, replica, mut service, _stat, intent_id, background_interception) =
        native_committed_repair_backlog("native-retry-transient-backoff-cap", true);
    assert!(
        background_interception.is_some(),
        "select the genuine foreground owner before clearing transport faults"
    );
    runtime.clear_faults();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 8,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    let before = runtime.rpc_observations().len();
    // Advance only the normal maintenance entry point. Cooling passes perform
    // no send; each negative native send creates its own retry record.
    for _ in 0..48 {
        service.retry_pending_repairs_bounded(1).unwrap();
        let attempts = service
            .writes
            .pending_writes
            .get(&intent_id)
            .and_then(|pending| pending.repair_retries.get(&replica))
            .map_or(0, |state| state.attempts);
        if attempts == 8 {
            break;
        }
    }
    let state = service
        .writes
        .pending_writes
        .get(&intent_id)
        .unwrap()
        .repair_retries
        .get(&replica)
        .unwrap();
    assert_eq!(state.attempts, 8);
    assert_eq!(state.next_retry_tick, service.repair_retry_tick + 6);
    assert!(!state.inflight);
    assert!(!service.repair_retry_is_due(&intent_id, &replica));
    let records = service.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].attempts, 8);
    assert!(crate::repair::repair_attempt_record_indicates_transient_route_failure(&records[0]));
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse)
            .count(),
        8
    );
    drop(service);
    drop(background_interception);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn stale_repair_base_rejection_is_retryable() {
    let reason = "stale DFS repair base for obj_stale: expected version_2, current version_1, command version_3";
    assert!(
        !repair_rejection_indicates_lost_authority(reason),
        "stale repair-base rejections are replica sequencing failures, not permanent authority loss"
    );
    assert!(
        repair_rejection_indicates_stale_replica_base(reason),
        "stale repair-base rejections should stay separately recognizable for diagnostics"
    );
}

#[test]
fn active_repair_attempt_does_not_resurrect_cleared_resume_identity() {
    let (runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-repair-clears-real-resume", true);
    assert!(
        _background_interception.is_some(),
        "this case inspects the foreground producer's first native transfer"
    );
    runtime.clear_faults();
    let prior_attempts = service.active_repair_attempt_records();
    let scheduled = service.retry_pending_repairs_bounded(1).unwrap();
    assert_eq!(
        scheduled.len(),
        1,
        "actual prior transfer custody: {prior_attempts:#?}"
    );
    let records = service.active_repair_attempt_records();
    assert_eq!(records.len(), 1);
    assert!(records[0].in_flight);
    let request_id = records[0].transfer_request_id.clone().unwrap();
    let original_deadline = records[0].next_retry_unix_nanos;
    // The ordinary restart cursor is the exact current send's own exported
    // record. No pending, transfer identity or durable timestamp is invented.
    service
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    let mut original = service
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find(|routed| {
            matches!(&routed.message,
            DfsNetworkMessage::RepairReplicaResponse(response)
                if response.intent_id == intent_id && response.request_id == request_id)
        })
        .unwrap();
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(response) = &mut original.message else {
        unreachable!()
    };
    assert_eq!(response.object_id, stat.object_id);
    assert_eq!(response.version, stat.version);
    assert!(
        matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: hash }
        | DfsBlobTransferResult::Durable { content_hash: hash }
        if Some(hash.as_str()) == stat.content_hash.as_deref())
    );
    response.result = DfsBlobTransferResult::Rejected {
        reason: "negative captured repair reply".into(),
    };
    let events = service
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RepairRejected { intent_id: rejected, .. } if rejected == &intent_id)));
    let current = service.active_repair_attempt_records();
    assert_eq!(current.len(), 1);
    assert!(!current[0].in_flight);
    assert_eq!(current[0].transfer_request_id, None);
    assert_ne!(
        current[0].next_retry_unix_nanos, original_deadline,
        "normal rejection clears the actual resume identity and refreshes its retry deadline"
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn repair_response_conflicting_receipt_preserves_retry_state() {
    let (runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-repair-negative-reply-keeps-retry", true);
    assert!(
        _background_interception.is_some(),
        "this case inspects the foreground producer's first native transfer"
    );
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    let prior_attempts = service.active_repair_attempt_records();
    let scheduled = service.retry_pending_repairs_bounded(1).unwrap();
    assert_eq!(
        scheduled.len(),
        1,
        "actual prior transfer custody: {prior_attempts:#?}"
    );
    let original = service
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find(|message| {
            matches!(&message.message,
            DfsNetworkMessage::RepairReplicaResponse(response)
                if response.intent_id == intent_id)
        })
        .expect("actual native receiver's queued repair reply");
    assert_eq!(original.source_node_id, replica);
    assert_eq!(original.target_node_id, owner);
    let DfsNetworkMessage::RepairReplicaResponse(mut response) = original.message else {
        unreachable!()
    };
    assert_eq!(response.file_system_id, stat.file_system_id);
    assert_eq!(response.object_id, stat.object_id);
    assert_eq!(response.version, stat.version);
    assert_eq!(response.replica_node_id, replica);
    let request_id = response.request_id.clone();
    let original_hash = match &response.result {
        DfsBlobTransferResult::Committed { content_hash }
        | DfsBlobTransferResult::Durable { content_hash } => content_hash.clone(),
        other => panic!("native repair must actually accept its bytes: {other:?}"),
    };
    assert_eq!(Some(original_hash.as_str()), stat.content_hash.as_deref());
    let pending = service.writes.pending_writes.get(&intent_id).unwrap();
    assert_eq!(
        pending.repair_inflight_request_ids.get(&replica),
        Some(&request_id)
    );
    assert!(pending.repair_retries.get(&replica).unwrap().inflight);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
    // Only the hash of this owned, decoded real reply is corrupted. No receipt
    // or contradictory healthy object/version evidence is planted in metadata.
    let mut corrupt_hash = original_hash;
    let last = corrupt_hash.pop().unwrap();
    corrupt_hash.push(if last == '0' { '1' } else { '0' });
    match &mut response.result {
        DfsBlobTransferResult::Committed { content_hash }
        | DfsBlobTransferResult::Durable { content_hash } => *content_hash = corrupt_hash,
        _ => unreachable!(),
    }
    let event = service
        .handle_repair_replica_response(replica.clone(), response)
        .unwrap();
    assert!(
        matches!(event, NetworkedDfsServiceEvent::RepairRejected { reason, .. }
        if reason.contains("content hash")),
        "negative captured hash must be rejected"
    );
    let pending = service.writes.pending_writes.get(&intent_id).unwrap();
    assert!(pending.repair_retries.contains_key(&replica));
    assert_eq!(
        pending.repair_inflight_request_ids.get(&replica),
        Some(&request_id),
        "rejected reply cannot clear a real inflight reservation before acceptance"
    );
    assert!(
        !service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    // The normal timeout/retry producer obtains the unchanged genuine receipt.
    for _ in 0..8 {
        service.retry_pending_repairs_bounded(1).unwrap();
        service.process_incoming().unwrap();
        if service
            .coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != intent_id)
        {
            break;
        }
    }
    assert!(
        service
            .coordinator()
            .repair_jobs()
            .iter()
            .all(|job| job.intent_id != intent_id)
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica
                && receipt.content_hash == stat.content_hash)
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn repair_command_conflicting_receipt_does_not_commit_visible_metadata() {
    let root = temp_root("repair-command-conflicting-receipt-no-visible-commit");
    let owner = NodeId::from_legacy_named("node_repair_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair command should not commit after receipt conflict";
    let content_hash = content_hash(bytes);
    let (_runtime, mut dfs) = native_sifs_service(
        "repair_command_conflicting_receipt_does_not_commit_visible_metadata",
        replica.clone(),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-repair-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_repair_cmd_conflict".to_string(),
                intent_id: "dfs_intent_repair_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
                authority_shard: None,
            },
        )
        .expect("repair command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "repair command with conflicting receipt must be rejected, got {event:?}"
    );

    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected repair command must not advance visible metadata"
    );
    assert!(
        !object.current_locations.contains(&replica),
        "rejected repair command must not add the local replica to visible locations"
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("backend")
        .get_object(&object_id)
        .expect("backend object")
        .expect("backend object exists");
    assert_eq!(
        backend_object.version, version_1,
        "rejected repair command must not publish visible metadata to the backend"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "receipt preflight should reject the repair before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_command_accepts_current_replica_after_metadata_primary_moves() {
    let (runtime, source, target, mut writer) =
        native_missing_content_repair("native-repair-current-replica-after-primary-move");
    let bytes = vec![0x72; 2 * 1024 * 1024 + 4096];
    let stat = writer
        .posix_write_file(
            "/services/native-repair-primary-move.bin",
            &bytes,
            DfsWriteOptions {
                replica_candidates: vec![source.clone(), target.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let content_hash = stat
        .content_hash
        .clone()
        .expect("actual native current content hash");
    let job = writer
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
    let intent = writer
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &job.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(
        writer
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(writer);
    // The future primary must still be missing content after its real joint
    // metadata handoff; only negative transport denial holds its local recovery.
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(target.clone()),
        target: Some(source.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let activated = runtime
        .reassign_metadata_shard(
            &stat.file_system_id,
            "services",
            target.clone(),
            vec![source.clone()],
        )
        .unwrap();
    assert_eq!(activated.primary_node_id, target);
    assert!(activated.replica_node_ids.contains(&source));
    let inspect = runtime.client(&target).unwrap();
    assert!(
        inspect
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err()
    );
    let current = inspect
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, stat.version);
    assert!(current.current_locations.contains(&source));
    drop(inspect);
    let mut replica = runtime.client(&source).unwrap();
    runtime.clear_faults();
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(source.clone()),
        target: Some(target.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_support::DfsTestFaultAction::LoseReplyAfterDispatch,
    });
    // This normal Repair producer opens the source's real manifest and binds
    // the actual current assignment. It grants no metadata author permission.
    let request_id = replica.next_request_id();
    let targets = vec![(target.clone(), request_id.clone())];
    let first = replica
        .send_repair_replica_attachments_if_routable(
            &targets,
            &intent.intent_id,
            &stat.file_system_id,
            &stat.object_id,
            intent.base_version.clone(),
            &stat.version,
            &content_hash,
            bytes.len() as u64,
            Some(current.clone()),
        )
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(
        !first[0].sent,
        "actual receiver completed before its reply was lost"
    );
    let repaired = runtime.client(&target).unwrap();
    assert_eq!(
        repaired
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(repaired);
    runtime.clear_faults();
    let replay = replica
        .send_repair_replica_attachments_if_routable(
            &targets,
            &intent.intent_id,
            &stat.file_system_id,
            &stat.object_id,
            intent.base_version,
            &stat.version,
            &content_hash,
            bytes.len() as u64,
            Some(current),
        )
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert!(replay[0].sent);
    let returned = replica.transport.drain_incoming_messages().unwrap();
    assert!(returned.iter().any(|routed| routed.source_node_id == target && routed.target_node_id == source
        && matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(response)
            if response.request_id == request_id && response.intent_id == intent.intent_id
                && response.object_id == stat.object_id && response.version == stat.version
                && response.replica_node_id == target
                && matches!(&response.result, DfsBlobTransferResult::Committed { content_hash: actual_hash } if actual_hash == &content_hash))),
        "stable replay receives the native current-primary receiver's actual Committed result");
    let repaired = runtime.client(&target).unwrap();
    let manifest = repaired
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(manifest.content_hash, content_hash);
    let current = repaired
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, stat.version);
    assert!(current.current_locations.contains(&target));
    assert!(
        repaired
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == target
                && receipt.content_hash.as_deref() == Some(content_hash.as_str()))
    );
    drop(repaired);
    drop(replica);
    runtime.close().unwrap();
}

#[test]
fn repair_command_rejects_unknown_object_from_non_primary_metadata_source() {
    assert_repair_hint_cannot_authorize_unknown_foreign_object(false);
}

#[test]
fn background_repair_hint_does_not_authorize_unknown_foreign_object() {
    assert_repair_hint_cannot_authorize_unknown_foreign_object(true);
}

fn assert_repair_hint_cannot_authorize_unknown_foreign_object(background_repair: bool) {
    let name = if background_repair {
        "native-background-foreign-repair-hint"
    } else {
        "native-foreign-repair-hint"
    };
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    drop(service);
    let other = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != primary)
        .collect::<Vec<_>>();
    let source = other[0].clone();
    let target = other[1].clone();
    let bytes = b"an actual current object is not permission for an unknown hinted object";
    let mut source_service = runtime.client(&source).unwrap();
    let current = source_service
        .posix_write_file(
            "/services/actual-repair-hint-source",
            bytes,
            DfsWriteOptions::replicated_eventual(vec![source.clone()]),
        )
        .unwrap();
    drop(source_service);
    let publisher = runtime.client(&primary).unwrap();
    let mut untrusted_hint = publisher
        .metadata_backend()
        .unwrap()
        .get_object(&current.object_id)
        .unwrap()
        .unwrap();
    let unknown = DfsObjectId::generate();
    let version = DfsVersion::generate();
    untrusted_hint.object_id = unknown.clone();
    untrusted_hint.path = "/services/unknown-hinted-repair".into();
    untrusted_hint.version = version.clone();
    drop(publisher);
    let receiver = runtime.client(&target).unwrap();
    let before = receiver
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new(config.shard_id.clone()))
        .unwrap()
        .unwrap();
    assert!(
        receiver
            .metadata_backend()
            .unwrap()
            .get_object(&unknown)
            .unwrap()
            .is_none()
    );
    let before_blob = receiver
        .content()
        .has_blob(&current.content_hash.clone().unwrap())
        .unwrap();
    drop(receiver);
    // This is deliberate untrusted DATA, sent by the actual non-primary member.
    // No hint, scheduling bool or genuine neighboring object grants publication.
    let command = test_dfs_message_command_from_message(
        &source,
        &target,
        1,
        DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
            background_repair,
            request_id: "native-foreign-hint-negative".into(),
            intent_id: "native-foreign-hint-negative-intent".into(),
            file_system_id: config.file_system_id.clone(),
            object_id: unknown.clone(),
            expected_base_version: None,
            version: version.clone(),
            content_hash: current.content_hash.clone().unwrap(),
            bytes: bytes.to_vec(),
            object_metadata: Some(untrusted_hint),
            authority_shard: Some(config.clone()),
        }),
    );
    let result = runtime.rpc_request(
        &source,
        &target,
        dfs_message_rpc_request(&command).unwrap(),
        Duration::from_secs(5),
    );
    match result {
        Err(reason) => assert!(
            reason.contains("primary") || reason.contains("authority"),
            "unexpected rejection: {reason}"
        ),
        Ok(reply) => {
            let decoded = decode_dfs_message_rpc_response(&reply).unwrap();
            assert!(decoded.outbound_messages.iter().any(|message|
                matches!(&message.message, DfsNetworkMessage::RepairReplicaResponse(response)
                    if matches!(&response.result, DfsBlobTransferResult::Rejected { reason }
                        if reason.contains("primary") || reason.contains("authority")))));
        }
    }
    let receiver = runtime.client(&target).unwrap();
    let after = receiver
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new(config.shard_id))
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        before.semantic_state_fingerprint
    );
    assert!(
        receiver
            .metadata_backend()
            .unwrap()
            .get_object(&unknown)
            .unwrap()
            .is_none()
    );
    assert!(
        receiver
            .content()
            .read_manifest(unknown.as_str(), version.as_str())
            .is_err()
    );
    assert_eq!(
        receiver
            .content()
            .has_blob(&current.content_hash.unwrap())
            .unwrap(),
        before_blob
    );
    drop(receiver);
    runtime.close().unwrap();
}

#[test]
fn stage_command_conflicting_receipt_does_not_store_orphan_content() {
    let root = temp_root("stage-command-conflicting-receipt-no-orphan-content");
    let owner = NodeId::from_legacy_named("node_stage_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stage_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stage_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"stage command should reject before storing orphan content";
    let content_hash = content_hash(bytes);
    let (_runtime, mut dfs) = native_sifs_service(
        "stage_command_conflicting_receipt_does_not_store_orphan_content",
        replica.clone(),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata).expect("metadata");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-stage-command-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_stage_blob(
            owner,
            DfsStageBlobCommand {
                request_id: "dfs_net_req_stage_cmd_conflict".to_string(),
                intent_id: "dfs_intent_stage_cmd_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash,
                bytes: bytes.to_vec(),
                object_metadata: None,
                authority_shard: None,
            },
        )
        .expect("stage command response event");
    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::BlobStageRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "stage command with conflicting receipt must be rejected, got {event:?}"
    );
    assert!(
        dfs.content
            .read_manifest(object_id.as_str(), version_2.as_str())
            .is_err(),
        "stage receipt preflight should reject before storing orphaned content"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_commits_when_response_target_is_unroutable() {
    let root = temp_root("repair-response-target-unroutable");
    let owner = NodeId::from_legacy_named("node_repair_notify_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_notify_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_notify").expect("object");
    let version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair committed despite lost requester";
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_repair_unroutable".to_string(),
                intent_id: "dfs_intent_repair_unroutable".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(metadata.version.clone()),
                version: version.clone(),
                content_hash: content_hash(bytes),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("unroutable response target should not fail repair commit");

    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::ReplicaRepaired {
            object_id: repaired_object_id,
            version: repaired_version,
            replica_node_id,
            ..
        } if repaired_object_id == object_id
            && repaired_version == version
            && replica_node_id == replica
    ));
    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(matches!(
        read,
        NetworkedDfsReadOutcome::Found {
            data,
            freshness: NetworkedDfsReadFreshness::Current
        } if data.version == version && data.bytes == bytes
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn repair_replica_accepts_stale_base_when_content_hash_matches_current() {
    // Regression: a metadata-only version bump (chmod/chown/utimes/hardlink/
    // rename) advances an object past the version a queued content repair
    // targets, while leaving content unchanged. The repair's base no longer
    // matches current, but its content hash equals what the current version
    // still references. Content is addressed by hash, so the repair must be
    // accepted -- otherwise the only copy of the content never reaches the
    // replica and reads fail with NotFound despite metadata being present.
    let root = temp_root("repair-stale-base-matching-content");
    let owner = NodeId::from_legacy_named("node_stale_base_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_stale_base_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_stale_base").expect("object");
    let base_version = DfsVersion::from_legacy_named("version_0").expect("version");
    let content_version = DfsVersion::from_legacy_named("version_1").expect("version");
    let current_version = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"content unchanged across a metadata-only version bump";
    let hash = content_hash(bytes);

    // The replica's view of the object has already advanced to the
    // metadata-only bump (version_2), still referencing the same content. The
    // queued content repair, however, still targets the content write
    // (version_1) whose intent based off version_0.
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = current_version.clone();
    metadata.content_hash = Some(hash.clone());
    metadata.current_locations = vec![owner.clone()];

    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(replica.clone()),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let event = dfs
        .handle_repair_replica(
            owner,
            DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "dfs_net_req_stale_base".to_string(),
                intent_id: "dfs_intent_stale_base".to_string(),
                file_system_id: metadata.file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: Some(base_version),
                version: content_version.clone(),
                content_hash: hash.clone(),
                bytes: bytes.to_vec(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("repair with matching content hash should be accepted");

    assert!(
        matches!(event, NetworkedDfsServiceEvent::ReplicaRepaired { .. }),
        "stale-base repair whose content matches current must commit"
    );

    let read = dfs
        .read_local_object_with_state(&object_id, false)
        .expect("read repaired object");
    assert!(
        matches!(read, NetworkedDfsReadOutcome::Found { data, .. } if data.bytes == bytes),
        "replica must serve the repaired content-addressed blob"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn reconnect_refreshes_unmanaged_local_manifest_receipt() {
    let root = temp_root("reconnect-refreshes-unmanaged-receipt");
    let local = NodeId::from_legacy_named("node_reconnect_unmanaged_local").expect("local");
    let remote = NodeId::from_legacy_named("node_reconnect_unmanaged_remote").expect("remote");
    let object_id =
        DfsObjectId::from_legacy_named("obj_reconnect_unmanaged_receipt").expect("object");
    let version = DfsVersion::from_legacy_named("version_reconnect_unmanaged").expect("version");
    let bytes = b"ordinary reconnect bytes";
    let (_runtime, mut dfs) = native_sifs_service(
        "reconnect_refreshes_unmanaged_local_manifest_receipt",
        local.clone(),
    );
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version.as_str(), bytes)
        .expect("seed local manifest and blob");
    let mut metadata = file_metadata(local.clone());
    metadata.object_id = object_id.clone();
    metadata.path = "/services/reconnect-unmanaged.txt".to_string();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.version = version.clone();
    metadata.current_locations = vec![remote];
    metadata.content_hash = Some(manifest.content_hash.clone());
    metadata.content_size_bytes = Some(bytes.len() as u64);
    dfs.add_object_metadata(metadata).expect("metadata");

    let reconciliation = dfs
        .reconcile_local_manifests(&object_id)
        .expect("reconcile unmanaged manifest");

    assert_eq!(reconciliation.refreshed_receipts, 1);
    assert!(!reconciliation.missing_current);
    assert!(
        dfs.metadata_backend()
            .expect("backend")
            .receipts_for_version(&object_id, &version)
            .expect("receipts")
            .iter()
            .any(|receipt| receipt.replica_node_id == local),
        "ordinary reconnect keeps its legacy receipt refresh behavior"
    );
    assert!(
        dfs.metadata()
            .object(&object_id)
            .expect("object")
            .current_locations
            .contains(&local),
        "ordinary reconnect keeps its legacy location refresh behavior"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_conflicting_receipt_does_not_advance_visible_metadata() {
    let root = temp_root("commit-conflicting-receipt-no-visible-advance");
    let owner = NodeId::from_legacy_named("node_commit_conflict_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_conflict_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit must not advance over conflicting durable receipt";
    let (_runtime, mut dfs) = native_sifs_service(
        "commit_version_conflicting_receipt_does_not_advance_visible_metadata",
        replica.clone(),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");
    dfs.put_metadata_receipt(
        &sifs_file_system_id(),
        &object_id,
        &version_2,
        &replica,
        "blake3:stale-conflicting-commit-receipt",
    )
    .expect("seed conflicting receipt");

    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_conflict".to_string(),
                intent_id: "dfs_intent_commit_conflict".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1.clone()),
                version: version_2.clone(),
                content_hash: manifest.content_hash,
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("commit conflict response");

    assert!(
        matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected {
                ref reason,
                ..
            } if reason.contains("conflicting DFS replica receipt")
        ),
        "commit with conflicting receipt must be rejected, got {event:?}"
    );
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(
        object.version, version_1,
        "rejected commit must not advance visible metadata"
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some("blake3:stale-conflicting-commit-receipt")
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn commit_version_repairs_missing_local_receipt_when_backend_is_authoritative() {
    let root = temp_root("commit-repairs-missing-local-receipt");
    let owner = NodeId::from_legacy_named("node_commit_receipt_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_commit_receipt_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_commit_receipt").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"commit should publish missing local receipt";
    let (_runtime, mut dfs) = native_sifs_service(
        "commit_version_repairs_missing_local_receipt_when_backend_is_authoritative",
        replica.clone(),
    );
    let mut metadata = file_metadata(owner.clone());
    metadata.object_id = object_id.clone();
    metadata.version = version_1.clone();
    metadata.policy = DfsObjectPolicy::ReplicatedEventual;
    metadata.current_locations = vec![owner.clone()];
    dfs.add_object_metadata(metadata.clone()).expect("metadata");
    let manifest = dfs
        .content
        .put_object_version(object_id.as_str(), version_2.as_str(), bytes)
        .expect("local staged content");

    dfs.begin_message_committed_work();
    let event = dfs
        .handle_commit_version(
            owner,
            DfsCommitVersionCommand {
                request_id: "dfs_net_req_commit_receipt".to_string(),
                intent_id: "dfs_intent_commit_receipt".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: object_id.clone(),
                expected_base_version: Some(version_1),
                version: version_2.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
                authority_shard: None,
            },
        )
        .expect("commit response");
    assert!(matches!(
        event,
        NetworkedDfsServiceEvent::VersionCommitted {
            object_id: committed,
            ..
        } if committed == object_id
    ));
    assert!(
        dfs.take_message_committed_work().publish_metadata,
        "the actual authoritative receipt mutation must schedule its publication"
    );
    let object = dfs
        .current_read_object_metadata(&object_id)
        .expect("visible metadata");
    assert_eq!(object.version, version_2);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    let receipts = dfs
        .metadata_backend()
        .expect("backend")
        .receipts_for_version(&object_id, &version_2)
        .expect("receipts");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, replica);
    assert_eq!(
        receipts[0].content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn committed_intent_repairs_already_current_stale_locations() {
    let (runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-already-current-late-repair-locations", true);
    let before = service
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(before.version, stat.version);
    assert_eq!(before.current_locations, vec![owner.clone()]);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent(&stat.object_id, &intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    runtime.clear_faults();
    let observed_before = runtime.rpc_observations().len();
    assert_eq!(service.retry_pending_repairs_bounded(1).unwrap().len(), 1);
    // Native ingest has finished, but its owned reply has not yet contributed a
    // receipt to the published current version's location inventory.
    let received = runtime.client(&replica).unwrap();
    assert_eq!(
        received
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        service
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1
    );
    drop(received);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap()
            .version,
        stat.version
    );
    assert!(
        !service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    let events = service.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: repaired, replica_node_id, .. }
            if repaired == &intent_id && replica_node_id == &replica)));
    let current = service
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(
        current.version, stat.version,
        "late normal repair repairs locations without publishing another version"
    );
    assert_eq!(current.content_hash, stat.content_hash);
    let locations = current
        .current_locations
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        locations,
        [owner.clone(), replica.clone()].into_iter().collect()
    );
    assert!(
        runtime.rpc_observations()[observed_before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
    service.apply_committed_intent(&intent_id).unwrap();
    let replay = service
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    assert_eq!(replay.version, stat.version);
    assert_eq!(
        replay
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        locations
    );
    drop(service);
    runtime.close().unwrap();
}

pub(super) fn native_withdrawn_local_replica_fixture(
    name: &str,
    bytes: &[u8],
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    DfsReplicaReceiptRecord,
) {
    let path = "/services/receipt-republication";
    let (runtime, owner, nodes, mut author, policy) =
        native_owned_repair_fixture(name, path, 1, 2, false);
    let stat = author
        .posix_write_file(
            path,
            bytes,
            DfsWriteOptions {
                policy,
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    // W1 visibility can precede the ordinary N2 continuation. Drain that real
    // owner before treating the second node as a durable fixture witness.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let current = runtime.metadata_primary_service(path).unwrap();
        let receipts = current
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        drop(current);
        if receipts.len() == 2 {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual N2 receipt continuation did not drain"
        );
        author.retry_pending_repairs_bounded(1).unwrap();
        author.process_incoming().unwrap();
    }
    drop(author);
    let mut author = runtime.metadata_primary_service(path).unwrap();
    let actual_receipts = author
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        actual_receipts.len(),
        2,
        "both actual replicas must be durable before withdrawing one receipt"
    );
    for node in &nodes {
        let holder = runtime.client(node).unwrap();
        assert_eq!(
            holder
                .content()
                .read_blob(stat.content_hash.as_deref().unwrap())
                .unwrap(),
            bytes
        );
    }
    let receipt = author
        .backend_receipt_for_replica(&stat.object_id, &stat.version, &owner)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.content_hash, stat.content_hash);
    // Simulate withdrawal of the actual location through its normal guarded
    // path, including the exact object/version/prefix predicates. Its bytes
    // remain durable and are checked before publishing the receipt again.
    author
        .mark_remote_read_location_missing(
            &stat.object_id,
            &stat.version,
            &stat.content_hash,
            &receipt.replica_node_id,
        )
        .unwrap();
    assert!(
        author
            .backend_receipt_for_replica(&stat.object_id, &stat.version, &receipt.replica_node_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        author
            .content()
            .read_blob(stat.content_hash.as_deref().unwrap())
            .unwrap(),
        bytes
    );
    // Start the receipt caller without the withdrawing reader's transient
    // health suppression. The real withdrawn metadata and surviving W remain.
    drop(author);
    let author = runtime.metadata_primary_service(path).unwrap();
    (runtime, author, stat, receipt)
}

#[test]
fn duplicate_metadata_receipt_publication_is_idempotent() {
    let (runtime, mut dfs, stat, receipt) = native_withdrawn_local_replica_fixture(
        "native-receipt-idempotence",
        b"real native receipt publication bytes",
    );
    let hash = receipt.content_hash.as_deref().unwrap();
    assert!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            hash
        )
        .unwrap()
    );
    let before = dfs
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    assert!(
        !dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            hash
        )
        .unwrap()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
            .unwrap()
            .unwrap(),
        before,
        "duplicate actual replica receipt changes neither tables nor journal prefix"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn conflicting_metadata_receipt_from_same_replica_is_rejected() {
    let (runtime, mut dfs, stat, receipt) = native_withdrawn_local_replica_fixture(
        "native-receipt-conflict",
        b"real native receipt publication bytes",
    );
    let hash = receipt.content_hash.as_deref().unwrap();
    assert!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            hash
        )
        .unwrap()
    );
    let before = dfs
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    let error = dfs
        .put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            "blake3:receipt-conflicting",
        )
        .expect_err("conflicting receipt must be rejected");
    assert!(
        error
            .to_string()
            .contains("conflicting DFS replica receipt"),
        "unexpected error: {error}"
    );
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(receipts.len(), 2);
    let target_receipts = receipts
        .iter()
        .filter(|row| row.replica_node_id == receipt.replica_node_id)
        .collect::<Vec<_>>();
    assert_eq!(target_receipts.len(), 1);
    assert_eq!(target_receipts[0].content_hash.as_deref(), Some(hash));
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
            .unwrap()
            .unwrap(),
        before,
        "rejected conflicting receipt changes neither tables nor journal prefix"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_routes_bounded_directory_rename_and_resume_receipts() {
    let cluster = sifs_posix_cluster("sifs-cluster-bounded-rename", "/users");
    let SifsPosixCluster {
        root,
        primary,
        writer_node,
        mut writer,
        _runtime,
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

    let from = "/users/bounded-old";
    let to = "/users/bounded-new";
    writer
        .posix_mkdir_all(&format!("{from}/nested"))
        .expect("routed source tree");
    for index in 0..4 {
        writer
            .posix_write_file(
                &format!("{from}/nested/{index}.txt"),
                index.to_string().as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            )
            .expect("routed source file");
    }
    let mut job = writer
        .posix_rename_directory_bounded_routed(from, to, 1)
        .expect("first routed bounded batch");
    while job.status == DfsNamespaceJobStatus::Running {
        job = writer
            .posix_resume_namespace_job_bounded_routed(&job.job_id, from, 1)
            .expect("next routed bounded batch");
    }
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(matches!(
        writer.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    for index in 0..4 {
        assert_eq!(
            writer
                .posix_read_file(&format!("{to}/nested/{index}.txt"))
                .expect("renamed file"),
            index.to_string().as_bytes()
        );
    }

    stop_primary.store(true, Ordering::SeqCst);
    let _primary = primary_thread.join().expect("primary thread");
    drop(_runtime);
}

#[test]
fn sifs_cluster_routed_mkdir_all_repairs_primary_when_local_chain_exists() {
    let cluster = sifs_two_primary_cluster("sifs-routed-mkdir-all-primary-repair");
    let SifsTwoPrimaryCluster {
        shared_kv,
        users_primary_node,
        _runtime,
        root,
        services_primary,
        services_primary_node,
        mut replica,
        replica_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let base = "/services/cluster-tests";
    let path = "/services/cluster-tests/primary-repair.txt";
    let stale = crate::posix_object::new_posix_metadata_object(
        &replica_node,
        base,
        DfsObjectKind::Directory,
    );
    let stale_id = stale.object_id.clone();
    replica
        .add_transient_object_metadata(stale)
        .expect("seed stale local chain");
    replica.transient_metadata_object_ids.remove(&stale_id);
    assert!(
        replica.posix_stat(base).is_ok(),
        "replica precondition should have local parent"
    );
    assert!(
        matches!(
            services_primary.posix_stat(base),
            Err(DfsPosixError::NotFound { .. })
        ),
        "services primary precondition should lack the parent"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let services_stop = Arc::clone(&stop);
    let services_thread =
        std::thread::spawn(move || pump_sifs_service_until_stop(services_primary, services_stop));

    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all should repair primary despite local chain");
    let unused_content_candidate =
        NodeId::from_legacy_named("node_sifs_routed_mkdir_unused_content").expect("unused node");
    let mut write_options = DfsWriteOptions::replicated_eventual(vec![
        replica_node,
        services_primary_node,
        unused_content_candidate,
    ]);
    write_options.foreground_replica_repair = false;
    replica
        .posix_write_file(path, b"child", write_options)
        .expect("child write after authoritative mkdir_all");

    stop.store(true, Ordering::SeqCst);
    let services_primary = services_thread.join().expect("services thread");
    assert!(services_primary.posix_stat(base).is_ok());
    assert!(services_primary.posix_stat(path).is_ok());
    std::fs::remove_dir_all(&root).ok();
}

fn native_missing_content_repair(
    name: &str,
) -> (
    crate::test_support::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
) {
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    drop(service);
    let peer = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != owner)
        .unwrap();
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(owner.clone()),
            target: Some(peer.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(crate::test_support::DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let service = runtime.metadata_primary_service("/services").unwrap();
    (runtime, owner, peer, service)
}

#[test]
fn native_repair_transfer_custody_survives_cancel_restart_and_correlates_rejected_reply() {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let (
        mut runtime,
        owner,
        replica,
        mut service,
        stat,
        intent_id,
        exchange,
        _background_interception,
    ) = native_committed_stage_backlog("native-repair-transfer-custody");
    let mut stage = exchange.response.clone();
    let DfsNetworkMessage::StageBlobResponse(reply) = &mut stage.message else {
        unreachable!()
    };
    reply.result = DfsBlobTransferResult::Rejected {
        reason: "negative actual Stage completion control".into(),
    };
    service
        .process_routed_messages_with_options(vec![stage.clone()], true)
        .unwrap();
    // CoreEffect owns these actual terminal facts; normal Main checkpoints
    // them before a later operation can claim the target.
    let actual_stage_terminal = service.durable_repair_queue_records();
    let completed_stage = actual_stage_terminal
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    let DfsNetworkMessage::StageBlobResponse(actual_stage_reply) = &stage.message else {
        unreachable!()
    };
    assert_eq!(
        completed_stage.transfer_kind,
        Some(DfsRepairTransferKind::Stage)
    );
    assert!(completed_stage.transfer_request_id.is_none());
    assert_eq!(
        completed_stage.completed_transfer_request_id.as_deref(),
        Some(actual_stage_reply.request_id.as_str())
    );
    let queue = checkpoint_actual_native_custody(&runtime, &owner, &actual_stage_terminal);
    assert_eq!(queue.view().unwrap().records, actual_stage_terminal);
    runtime.clear_faults();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    // The actual normal producer chooses its first Repair request after the
    // genuine precommit Stage terminal; no test-created packet schedules work.
    service.retry_pending_repairs_bounded(1).unwrap();
    let first_record = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(
        first_record.transfer_kind,
        Some(DfsRepairTransferKind::Repair)
    );
    assert_eq!(
        first_record.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(first_record.transfer_request_id.is_none());
    assert!(first_record.completed_transfer_request_id.is_none());
    runtime.clear_faults();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while service.transport.pending_incoming_count().unwrap() == Some(0) {
        assert!(
            Instant::now() < deadline,
            "normal foreground producer reaches its actual repair reply"
        );
        service.retry_pending_repairs_bounded(1).unwrap();
    }
    let completed_id = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap()
        .transfer_request_id
        .expect("actual producer owns the captured native reply");
    let mut original = service.transport.drain_incoming_messages().unwrap().into_iter().find(|routed| {
        matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(reply) if reply.request_id == completed_id)
    }).unwrap();
    // Persist the SAME producer's actual named Repair before processing its
    // captured return. No request or delivery state is constructed here.
    let actual_repair_claim = service.durable_repair_queue_records();
    let named_repair = actual_repair_claim
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(
        named_repair.transfer_kind,
        Some(DfsRepairTransferKind::Repair)
    );
    assert_eq!(
        named_repair.transfer_request_id.as_deref(),
        Some(completed_id.as_str())
    );
    assert!(named_repair.completed_transfer_request_id.is_none());
    let queue = checkpoint_actual_native_custody(&runtime, &owner, &actual_repair_claim);
    assert_eq!(queue.view().unwrap().records, actual_repair_claim);
    let DfsNetworkMessage::RepairReplicaResponse(reply) = &mut original.message else {
        unreachable!()
    };
    reply.result = DfsBlobTransferResult::Rejected {
        reason: "negative actual receiver reply for custody control".into(),
    };
    service
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    let completed = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|record| record.intent_id == intent_id)
        .unwrap();
    assert_eq!(
        completed.completed_transfer_request_id.as_deref(),
        Some(completed_id.as_str())
    );
    assert!(completed.transfer_request_id.is_none());

    // A known negative terminal has its ordinary durable retry cooldown. Wait
    // for that actual eligibility before starting the unchanged carrier budget.
    let physical_retry = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(physical_retry.object_id, stat.object_id);
    assert_eq!(physical_retry.version, stat.version);
    assert_eq!(
        Some(&physical_retry.content_hash),
        stat.content_hash.as_ref()
    );
    assert_eq!(
        physical_retry.transfer_kind,
        Some(DfsRepairTransferKind::Repair)
    );
    assert_eq!(
        physical_retry.completed_transfer_request_id.as_deref(),
        Some(completed_id.as_str())
    );
    assert!(physical_retry.transfer_request_id.is_none());
    assert!(!physical_retry.in_flight);
    assert_eq!(physical_retry.attempts, completed.attempts);
    assert!(physical_retry.next_retry_unix_nanos > named_repair.next_retry_unix_nanos);
    // Re-open the existing normal Core effect boundary and restore only this
    // actual queue row; zero scheduling cannot manufacture a transfer or receipt.
    let mut cold = runtime.metadata_primary_service(&stat.path).unwrap();
    cold.retry_pending_repairs_bounded_with_attempts(0, std::slice::from_ref(&physical_retry))
        .unwrap();
    let restored_terminal = cold
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(restored_terminal, physical_retry);
    assert!(!cold.repair_retry_is_due(&intent_id, &replica));
    drop(cold);
    let retry_due = physical_retry
        .next_retry_unix_nanos
        .max(completed.next_retry_unix_nanos);
    assert_ne!(
        retry_due,
        u64::MAX,
        "the actual terminal retry must have finite eligibility"
    );
    let eligibility_started = Instant::now();
    crate::test_runtime::executor().block_on(async {
        let remaining =
            Duration::from_nanos(retry_due.saturating_sub(crate::test_support::now_unix_nanos()));
        tokio::time::sleep_until(tokio::time::Instant::now() + remaining).await;
    });
    assert!(crate::test_support::now_unix_nanos() >= retry_due);
    let eligible = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(eligible, physical_retry);
    assert!(eligible.next_retry_unix_nanos <= crate::test_support::now_unix_nanos());
    println!(
        "actual native physical retry attempts={} cooldown_nanos={} due_unix_nanos={}",
        physical_retry.attempts,
        physical_retry
            .next_retry_unix_nanos
            .saturating_sub(physical_retry.last_attempt_unix_nanos),
        physical_retry.next_retry_unix_nanos
    );
    println!(
        "actual native repair retry eligibility due_unix_nanos={retry_due} prerequisite_wait_seconds={:.6}; included in total test wall time",
        eligibility_started.elapsed().as_secs_f64()
    );

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    let before = runtime.rpc_observations().len();
    crate::test_runtime::executor().block_on(async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let exclusions = crate::DfsMaintenanceExecutionExclusions::default();
            let send = service.retry_pending_repair_page(1, &[], &exclusions, None, None, true);
            tokio::pin!(send);
            let observed = async {
                loop {
                    if runtime.rpc_observations()[before..].iter().any(|event| event.source == owner && event.target == replica
                        && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch) { break; }
                    tokio::task::yield_now().await;
                }
            };
            tokio::pin!(observed);
            tokio::select! {
                result = &mut send => { let progress = result.unwrap(); assert!(progress.error.is_none(), "normal repair-page error: {:?}", progress.error); },
                result = tokio::time::timeout_at(deadline, &mut observed) => { result.unwrap(); break; },
            }
            assert!(tokio::time::Instant::now() < deadline, "actual normal maintenance enters its native carrier");
        }
        // This drops the awaited native send future itself, not an outer
        // blocking caller whose actual Main transfer may continue running.
    });
    let unresolved_id = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap()
        .transfer_request_id
        .expect("actual cancelled producer retains its exact native ID");
    let records = service.durable_repair_queue_records();
    let unresolved = records
        .iter()
        .find(|record| record.intent_id == intent_id)
        .unwrap();
    assert_eq!(
        unresolved.transfer_request_id.as_deref(),
        Some(unresolved_id.as_str())
    );
    assert_eq!(
        unresolved.transfer_kind,
        Some(DfsRepairTransferKind::Repair)
    );
    assert!(unresolved.completed_transfer_request_id.is_none());
    assert!(unresolved.transfer_delivery_state.is_none());
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
    );
    let (_, observed_queue) = queue.view_for_maintenance().unwrap();
    assert!(matches!(
        queue.reconcile_observed(&observed_queue, &records).unwrap(),
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_)
    ));
    drop(service);
    gate.add_permits(1);
    runtime.clear_faults();
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut service = runtime.metadata_primary_service(&stat.path).unwrap();
    service
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    let restored = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|record| record.intent_id == intent_id)
        .unwrap();
    assert_eq!(
        restored.transfer_request_id.as_deref(),
        Some(unresolved_id.as_str())
    );
    assert!(restored.transfer_delivery_state.is_none());
    assert!(restored.completed_transfer_request_id.is_none());
    let replacement_id = service.next_request_id();
    let DfsNetworkMessage::StageBlob(actual_stage) = &exchange.command.routed_message.message
    else {
        unreachable!()
    };
    assert!(
        service
            .send_repair_replica_attachments_if_routable(
                &[(replica.clone(), replacement_id)],
                &actual_stage.intent_id,
                &actual_stage.file_system_id,
                &actual_stage.object_id,
                actual_stage.expected_base_version.clone(),
                &actual_stage.version,
                &actual_stage.content_hash,
                exchange.body_bytes.unwrap(),
                actual_stage.object_metadata.clone(),
            )
            .is_err()
    );
    // A diagnostic containing route words has no typed delivery evidence.
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    let before_opaque = runtime.rpc_observations().len();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let previous_deadline = service.transport.set_operation_deadline(Some(deadline));
    let attempted = service.send_repair_replica_attachments_if_routable(
        &[(replica.clone(), unresolved_id.clone())],
        &actual_stage.intent_id,
        &actual_stage.file_system_id,
        &actual_stage.object_id,
        actual_stage.expected_base_version.clone(),
        &actual_stage.version,
        &actual_stage.content_hash,
        exchange.body_bytes.unwrap(),
        actual_stage.object_metadata.clone(),
    );
    service.transport.set_operation_deadline(previous_deadline);
    attempted.unwrap();
    assert!(
        Instant::now() < deadline,
        "normal exact retry respects its original budget"
    );
    assert!(
        runtime.rpc_observations()[before_opaque..]
            .iter()
            .any(|event| {
                event.source == owner
                    && event.target == replica
                    && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                    && event.dfs_intent_id.as_deref() == Some(intent_id.as_str())
                    && event.dfs_file_system_id.as_ref() == Some(&stat.file_system_id)
                    && event.dfs_object_id.as_ref() == Some(&stat.object_id)
                    && event.dfs_version.as_ref() == Some(&stat.version)
            }),
        "the exact claimed tuple reaches its real opaque carrier fault"
    );
    let opaque_record = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|record| record.intent_id == intent_id)
        .unwrap();
    assert_eq!(
        opaque_record.transfer_request_id.as_deref(),
        Some(unresolved_id.as_str())
    );
    assert!(opaque_record.transfer_delivery_state.is_none());
    runtime.clear_faults();
    let terminal_replies = Arc::new(Mutex::new(Vec::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: None,
            captured: Arc::clone(&terminal_replies),
        },
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let previous_deadline = service.transport.set_operation_deadline(Some(deadline));
    let attempted = service.send_repair_replica_attachments_if_routable(
        &[(replica.clone(), unresolved_id.clone())],
        &actual_stage.intent_id,
        &actual_stage.file_system_id,
        &actual_stage.object_id,
        actual_stage.expected_base_version.clone(),
        &actual_stage.version,
        &actual_stage.content_hash,
        exchange.body_bytes.unwrap(),
        actual_stage.object_metadata.clone(),
    );
    service.transport.set_operation_deadline(previous_deadline);
    attempted.unwrap();
    assert!(
        Instant::now() < deadline,
        "normal exact terminal retry respects its original budget"
    );
    let mut original = terminal_replies
        .lock()
        .unwrap()
        .iter()
        .find(|routed| {
            matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(reply)
            if reply.request_id == unresolved_id && reply.intent_id == intent_id
                && reply.file_system_id == stat.file_system_id
                && reply.object_id == stat.object_id && reply.version == stat.version
                && reply.replica_node_id == replica
                && routed.source_node_id == replica && routed.target_node_id == owner)
        })
        .cloned()
        .expect("the actual native terminal reply retains its exact owner/tuple/claim");
    let DfsNetworkMessage::RepairReplicaResponse(reply) = &mut original.message else {
        unreachable!()
    };
    reply.result = DfsBlobTransferResult::Rejected {
        reason: "negative captured native terminal reply".into(),
    };
    runtime.clear_faults();
    let mut negative_replies = vec![(stage, false)];
    for wrong_id in [false, true] {
        let mut wrong = original.clone();
        let DfsNetworkMessage::RepairReplicaResponse(reply) = &mut wrong.message else {
            unreachable!()
        };
        if wrong_id {
            reply.request_id.push_str("-wrong");
        } else {
            reply.object_id = DfsObjectId::generate();
        }
        negative_replies.push((wrong, !wrong_id));
    }
    for (negative, wrong_object) in negative_replies {
        // Persist the SAME actual producer custody, release the CoreEffect
        // observer, then let ordinary Main ingress restore and validate it.
        let expected_records = service.durable_repair_queue_records();
        checkpoint_actual_native_custody(&runtime, &owner, &expected_records);
        let physical_before = queue.view().unwrap().records;
        let expected = physical_before
            .iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap()
            .clone();
        let before_receipts = service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        drop(service);
        let command = DfsMessageCommand {
            request_id: sinomo_core::Id::generate().to_string(),
            routed_message: negative,
        };
        let returned = runtime.rpc_request(
            &replica,
            &owner,
            dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        );
        if wrong_object {
            assert_eq!(
                returned.unwrap_err(),
                format!(
                    "network I/O error during shared DFS native service dispatch: invalid DFS runtime operation: distributed DFS error: unknown pending write intent: {intent_id}"
                ),
                "the actual Main handler rejects the foreign object before it can settle custody"
            );
        } else {
            returned.unwrap();
        }
        // All three authenticated Main ingress cuts must preserve the named
        // Repair custody; the late Stage cannot settle another transfer kind.
        let persisted = queue.view().unwrap().records;
        let physical_after = persisted
            .iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap();
        assert_eq!(
            physical_after, &expected,
            "a negative actual Main reply preserves the entire durable custody row"
        );
        service = runtime.metadata_primary_service(&stat.path).unwrap();
        service
            .retry_pending_repairs_bounded_with_attempts(0, &persisted)
            .unwrap();
        let current = service
            .durable_repair_queue_records()
            .into_iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap();
        // Zero-attempt normal restoration can project fresh scheduling times.
        // Its immutable identity and every custody/attempt fact still match the
        // unchanged physical record observed on either side of Main ingress.
        assert_eq!(
            (
                &current.intent_id,
                &current.object_id,
                &current.version,
                &current.content_hash,
                &current.missing_replica_node_id,
            ),
            (
                &expected.intent_id,
                &expected.object_id,
                &expected.version,
                &expected.content_hash,
                &expected.missing_replica_node_id,
            )
        );
        assert_eq!(
            (
                current.attempts,
                current.in_flight,
                &current.transfer_request_id,
                &current.last_error,
                current.transfer_delivery_state,
                &current.completed_transfer_request_id,
                current.transfer_kind,
            ),
            (
                expected.attempts,
                expected.in_flight,
                &expected.transfer_request_id,
                &expected.last_error,
                expected.transfer_delivery_state,
                &expected.completed_transfer_request_id,
                expected.transfer_kind,
            )
        );
        assert_eq!(
            current.transfer_request_id.as_deref(),
            Some(unresolved_id.as_str())
        );
        assert!(current.completed_transfer_request_id.is_none());
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap(),
            before_receipts
        );
    }
    service
        .process_routed_messages_with_options(vec![original], true)
        .unwrap();
    let released = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|record| record.intent_id == intent_id)
        .unwrap();
    assert!(released.transfer_request_id.is_none());
    assert_eq!(
        released.completed_transfer_request_id.as_deref(),
        Some(unresolved_id.as_str())
    );
    assert!(!released.in_flight);
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.assert_fault_budgets().unwrap();
    drop(service);
    runtime.close().unwrap();
}

fn checkpoint_actual_native_custody(
    runtime: &crate::test_runtime::DfsTestRuntime,
    owner: &NodeId,
    records: &[DfsRepairAttemptRecord],
) -> crate::LocalDfsRepairQueueStore {
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(owner)),
    );
    let (_, observed) = queue.view_for_maintenance().unwrap();
    assert!(matches!(
        queue.reconcile_observed(&observed, records).unwrap(),
        crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_)
    ));
    queue
}

#[test]
fn native_browser_fresh_repair_custody_reaches_n2_after_healing() {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let name = "native-browser-fresh-head-ready-tail";
    let (runtime, client, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(client);
    let owner = config.primary_node_id.clone();
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let mut others = nodes.iter().filter(|node| **node != owner);
    let replica = others.next().unwrap().clone();
    let requester = others.next().unwrap().clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![owner.clone()];
    policy.forbidden_node_ids = vec![requester.clone()];
    runtime
        .apply_placement_policy(&owner, "/services", DfsPlacementScope::Subtree, policy)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    control.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        let before = store
            .get_file_system_config(&config.file_system_id)
            .unwrap()
            .unwrap();
        let mut desired = before.clone();
        desired.retention_policies.insert(
            "module.dfs.history".into(),
            DfsRetentionPolicy {
                rules: vec![DfsRetentionRule {
                    path: "/services".into(),
                    minimum: DfsRetentionMinimum {
                        keep_last: 1,
                        keep_for_seconds: 0,
                    },
                }],
            },
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&before), &desired)
                .unwrap()
        );
    });
    drop(control);
    let held = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
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
    // Use the normal caller's configured policy resolver, as the CLI does;
    // the POSIX write API preserves the replication policy in its options.
    let mut writer = runtime.client(&requester).unwrap();
    let (resolved, advertisements, managed_policy) = writer
        .managed_replication_policy_for_path(
            "/services/unknown-head.bin",
            DfsObjectPolicy::ReplicatedEventual,
            nodes.len(),
        )
        .unwrap();
    assert_eq!(resolved.source_path.as_deref(), Some("/services"));
    assert_eq!(resolved.source_scope, Some(DfsPlacementScope::Subtree));
    assert_eq!(managed_policy.min_commit_replicas, 1);
    assert_eq!(managed_policy.desired_replicas, 2);
    assert_eq!(managed_policy.required_node_ids, vec![owner.clone()]);
    assert_eq!(managed_policy.forbidden_node_ids, vec![requester.clone()]);
    assert!(nodes.iter().all(|node| {
        advertisements
            .iter()
            .any(|advertisement| advertisement.node_id == *node && advertisement.accepts_content)
    }));
    let options = DfsWriteOptions {
        replica_candidates: nodes,
        policy: managed_policy,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    // Use the already registered resident Browser byte producer used by the
    // daemon-facing CLI, with the same managed policy and real requester.
    // The unit write response carries no stat, so each subsequent observation
    // uses the ordinary distributed read. UNKNOWN is proved only below.
    let first_response = runtime
        .rpc_request(
            &requester,
            &requester,
            ObjectRpcRequest::encode_body(
                crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
                sinomo_api::DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserWriteFileRequest {
                    path: "/services/unknown-head.bin".into(),
                    bytes: b"rule-a".to_vec(),
                },
            )
            .unwrap(),
            DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        )
        .unwrap();
    assert_eq!(
        first_response.route,
        crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
    );
    let _: crate::DaemonDfsBrowserWriteFileResponse = first_response.decode_body().unwrap();
    let first = writer
        .posix_stat_distributed("/services/unknown-head.bin")
        .unwrap();
    assert_eq!(first.file_system_id, config.file_system_id);
    assert_eq!(first.path, "/services/unknown-head.bin");
    assert_eq!(first.content_hash, Some(content_hash(b"rule-a")));
    assert_eq!(first.size_bytes, Some(b"rule-a".len() as u64));
    let middle_response = runtime
        .rpc_request(
            &requester,
            &requester,
            ObjectRpcRequest::encode_body(
                crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
                sinomo_api::DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserWriteFileRequest {
                    path: first.path.clone(),
                    bytes: b"rule-b".to_vec(),
                },
            )
            .unwrap(),
            DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        )
        .unwrap();
    assert_eq!(
        middle_response.route,
        crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
    );
    let _: crate::DaemonDfsBrowserWriteFileResponse = middle_response.decode_body().unwrap();
    let middle = writer.posix_stat_distributed(&first.path).unwrap();
    assert_eq!(middle.file_system_id, config.file_system_id);
    assert_eq!(middle.path, "/services/unknown-head.bin");
    assert_eq!(middle.content_hash, Some(content_hash(b"rule-b")));
    assert_eq!(middle.size_bytes, Some(b"rule-b".len() as u64));
    let head_response = runtime
        .rpc_request(
            &requester,
            &requester,
            ObjectRpcRequest::encode_body(
                crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
                sinomo_api::DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserWriteFileRequest {
                    path: middle.path.clone(),
                    bytes: b"rule-c".to_vec(),
                },
            )
            .unwrap(),
            DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        )
        .unwrap();
    assert_eq!(
        head_response.route,
        crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
    );
    let _: crate::DaemonDfsBrowserWriteFileResponse = head_response.decode_body().unwrap();
    let head = writer.posix_stat_distributed(&middle.path).unwrap();
    assert_eq!(head.file_system_id, config.file_system_id);
    assert_eq!(head.path, "/services/unknown-head.bin");
    assert_eq!(head.content_hash, Some(content_hash(b"rule-c")));
    assert_eq!(head.size_bytes, Some(b"rule-c".len() as u64));
    assert_eq!(first.file_system_id, middle.file_system_id);
    assert_eq!(middle.file_system_id, head.file_system_id);
    assert_eq!(first.object_id, middle.object_id);
    assert_eq!(middle.object_id, head.object_id);
    assert_ne!(first.version, middle.version);
    assert_ne!(middle.version, head.version);
    writer.posix_unlink(&head.path).unwrap();
    assert!(matches!(
        writer.posix_stat_distributed(&head.path),
        Err(crate::DfsPosixError::NotFound { .. })
    ));
    drop(writer);
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    let before = queue.view().unwrap();
    {
        let inspect = runtime.metadata_primary_service(&head.path).unwrap();
        let backend = inspect.metadata_backend().unwrap();
        let version = backend
            .get_version(&head.object_id, &head.version)
            .unwrap()
            .unwrap();
        assert!(version.visible);
        assert_eq!(version.file_system_id, head.file_system_id);
        assert_eq!(version.object_id, head.object_id);
        assert_eq!(version.version, head.version);
        assert_eq!(version.content_hash, head.content_hash);
        assert_eq!(version.size_bytes, Some(b"rule-c".len() as u64));
        let receipts = backend
            .receipts_for_version(&head.object_id, &head.version)
            .unwrap();
        assert!(
            receipts.iter().any(|receipt| {
                crate::content_durability::receipt_matches_target_version(receipt, &version, &owner)
            }),
            "the actual preferred W1 owner has its exact durable target receipt"
        );
        assert!(
            receipts.iter().all(|receipt| {
                !crate::content_durability::receipt_matches_target_version(
                    receipt, &version, &replica,
                ) && !crate::content_durability::receipt_matches_target_version(
                    receipt, &version, &requester,
                )
            }),
            "the missing replica and outside-plan requester supply no borrowed durable receipt"
        );
    }
    let fresh = before
        .records
        .iter()
        .find(|row| {
            row.object_id == head.object_id
                && row.version == head.version
                && Some(row.content_hash.as_str()) == head.content_hash.as_deref()
                && row.missing_replica_node_id == replica
                && row.transfer_delivery_state == Some(NetworkDeliveryState::DefinitelyNotSent)
                && row.transfer_request_id.is_none()
                && row.completed_transfer_request_id.is_none()
        })
        .expect("the actual Browser producer retains its observed no-send repair custody");
    assert_eq!(fresh.transfer_kind, Some(DfsRepairTransferKind::Repair));
    let mut writer = runtime.client(&owner).unwrap();
    let (tail_resolved, _, tail_policy) = writer
        .managed_replication_policy_for_path(
            "/services/ready-tail.bin",
            DfsObjectPolicy::ReplicatedEventual,
            options.replica_candidates.len(),
        )
        .unwrap();
    assert_eq!(tail_resolved.source_path.as_deref(), Some("/services"));
    assert_eq!(tail_resolved.source_scope, Some(DfsPlacementScope::Subtree));
    assert_eq!(tail_policy, options.policy);
    let tail_options = DfsWriteOptions {
        policy: tail_policy,
        ..options
    };
    let bytes = b"actual independent ready tail";
    let tail = writer
        .posix_write_file("/services/ready-tail.bin", bytes, tail_options)
        .unwrap();
    drop(writer);
    let queued = queue.view().unwrap();
    let ready = queued
        .records
        .iter()
        .find(|row| {
            row.object_id == tail.object_id
                && row.version == tail.version
                && Some(row.content_hash.as_str()) == tail.content_hash.as_deref()
                && row.missing_replica_node_id == replica
        })
        .unwrap();
    assert_eq!(ready.transfer_kind, Some(DfsRepairTransferKind::Repair));
    assert_eq!(
        ready.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(ready.transfer_request_id.is_none() && !ready.in_flight);
    let ready_intent = ready.intent_id.clone();
    let inspect = runtime.metadata_primary_service(&tail.path).unwrap();
    for (stat, intent_id, actual_requester) in [
        (&head, fresh.intent_id.as_str(), &requester),
        (&tail, ready_intent.as_str(), &owner),
    ] {
        let history = inspect
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        let intent = history
            .intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .unwrap();
        assert!(intent.publication.is_current());
        assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
        assert_eq!(intent.file_system_id, stat.file_system_id);
        assert_eq!(intent.object_id, stat.object_id);
        assert_eq!(intent.target_version, stat.version);
        assert_eq!(intent.content_hash, stat.content_hash);
        assert_eq!(&intent.coordinator_node_id, actual_requester);
        assert!(
            intent
                .operation_id
                .as_ref()
                .is_some_and(|operation| !operation.is_empty())
        );
        assert_eq!(intent.policy.min_commit_replicas, 1);
        assert_eq!(intent.policy.desired_replicas, 2);
        assert_eq!(
            intent
                .policy
                .required_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner.clone()])
        );
        assert_eq!(
            intent
                .policy
                .forbidden_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([requester.clone()])
        );
        assert_eq!(
            intent
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([owner.clone(), replica.clone()])
        );
        assert_eq!(intent.desired_replica_node_ids.first(), Some(&owner));
        let version = inspect
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        assert!(version.visible);
        assert_eq!(version.file_system_id, stat.file_system_id);
        assert_eq!(version.object_id, stat.object_id);
        assert_eq!(version.version, stat.version);
        assert_eq!(version.content_hash, stat.content_hash);
        let recorded = version.content_durability.as_ref().unwrap();
        assert_eq!(recorded.source_version, stat.version);
        assert_eq!(&recorded.coordinator_node_id, actual_requester);
        assert_eq!(recorded.owner_epoch, intent.owner_epoch);
        assert_eq!(recorded.policy, intent.policy);
        assert_eq!(
            recorded.desired_replica_node_ids,
            intent.desired_replica_node_ids
        );
    }
    drop(inspect);

    let original_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let first_observation = runtime.rpc_observations().len();
    runtime.clear_faults();
    drop(held);
    let remaining = original_deadline
        .checked_duration_since(Instant::now())
        .unwrap();
    let observation = runtime.maintenance_tick(&owner, remaining).unwrap();
    assert!(
        observation.outcome.is_ok(),
        "same bounded ordinary Main owner: {:?}",
        observation.outcome
    );
    assert_eq!(observation.omitted_kv_event_count, 0);
    // The ordinary tick checkpoints custody before its detached carrier runs.
    // Its exact terminal record can remain until ordinary queue retirement.
    let terminal_row = |row: &DfsRepairAttemptRecord| {
        row.object_id == tail.object_id
            && row.version == tail.version
            && Some(row.content_hash.as_str()) == tail.content_hash.as_deref()
            && row.missing_replica_node_id == replica
            && row.transfer_kind == Some(DfsRepairTransferKind::Repair)
            && !crate::service_repair::repair_transfer_custody_unresolved(row)
            && row.next_retry_unix_nanos == 0
            && row.last_error.is_none()
            && row
                .completed_transfer_request_id
                .as_deref()
                .is_some_and(|request| {
                    runtime.rpc_observations()[first_observation..]
                        .iter()
                        .any(|event| {
                            event.source == owner
                                && event.target == replica
                                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                                && event.dfs_intent_id.as_deref() == Some(ready_intent.as_str())
                                && event.dfs_request_id.as_deref() == Some(request)
                        })
                })
    };
    // Queue DATA alone never certifies the effect: require its native receipt.
    let (receipts, after) = loop {
        assert!(
            Instant::now() < original_deadline,
            "the current repair completes within its original five-second window"
        );
        let inspect = runtime.metadata_primary_service(&tail.path).unwrap();
        let receipts = inspect
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&tail.object_id, &tail.version)
            .unwrap();
        drop(inspect);
        let after = queue.view().unwrap();
        let received = receipts.iter().any(|receipt| {
            receipt.receipt.file_system_id == tail.file_system_id
                && receipt.receipt.object_id == tail.object_id
                && receipt.receipt.committed_version == tail.version
                && receipt.content_hash == tail.content_hash
                && receipt.replica_node_id == replica
                && receipt.receipt.owner_node_id == owner
        });
        let settled = after
            .records
            .iter()
            .filter(|row| row.intent_id == ready_intent)
            .all(terminal_row);
        if received && settled {
            assert!(
                Instant::now() < original_deadline,
                "a receipt observed after the original deadline is not completion"
            );
            break (receipts, after);
        }
        std::thread::yield_now();
    };
    assert!(
        runtime.rpc_observations()[first_observation..]
            .iter()
            .any(|event| {
                event.source == owner
                    && event.target == replica
                    && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            }),
        "the current DNS repair reaches its genuine carrier"
    );
    assert!(
        receipts.iter().any(|receipt| {
            receipt.receipt.file_system_id == tail.file_system_id
                && receipt.receipt.object_id == tail.object_id
                && receipt.receipt.committed_version == tail.version
                && receipt.content_hash == tail.content_hash
                && receipt.replica_node_id == replica
                && receipt.receipt.owner_node_id == owner
        }),
        "the current N2 target has its actual full-tuple receipt"
    );
    assert!(
        after
            .records
            .iter()
            .filter(|row| row.intent_id == ready_intent)
            .all(terminal_row),
        "the completed ready obligation has no unresolved carrier custody"
    );
    let mut inspect = runtime.metadata_primary_service(&tail.path).unwrap();
    assert_eq!(
        inspect.posix_read_file_distributed(&tail.path).unwrap(),
        bytes
    );
    drop(inspect);
    assert!(
        Instant::now() < original_deadline,
        "the original five-second completion window is unchanged"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.close().unwrap();
    assert!(
        Instant::now() < original_deadline,
        "the same completion window includes close"
    );
}

#[test]
fn native_fresh_transfer_enrollment_restores_first_dispatch_and_rejects_untracked_authenticated_reply()
 {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let (mut runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-fresh-transfer-enrollment", true);
    // Observe enrollment written by actual Main's new-intent producer. The
    // returned caller may be restored and cannot create initial custody facts.
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
    );
    let enrolled = queue.view().unwrap().records;
    let fresh = enrolled
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(fresh.attempts, 0);
    assert_eq!(fresh.transfer_kind, Some(DfsRepairTransferKind::Repair));
    assert_eq!(
        fresh.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(fresh.transfer_request_id.is_none());
    drop(service);
    runtime.clear_faults();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let records = queue.view().unwrap().records;
    let mut restored = runtime.metadata_primary_service(&stat.path).unwrap();
    restored
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    let before = runtime.rpc_observations().len();
    restored
        .retry_pending_repairs_bounded_with_attempts(1, &records)
        .unwrap();
    assert!(
        runtime.rpc_observations()[before..].iter().any(|event| {
            event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        }),
        "the actual fresh-enrollment queue must reach its first normal maintenance carrier"
    );
    let unsent = restored.durable_repair_queue_records();
    let row = unsent
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert!(row.transfer_request_id.is_none());
    assert_eq!(
        row.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    checkpoint_actual_native_custody(&runtime, &owner, &unsent);
    runtime.clear_faults();
    // The existing normal one-target producer chooses its own actual request;
    // no separate test packet or request scheduler is constructed.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while restored.transport.pending_incoming_count().unwrap() == Some(0) {
        assert!(
            Instant::now() < deadline,
            "actual normal foreground repair reaches its native reply"
        );
        restored.retry_pending_repairs_bounded(1).unwrap();
    }
    let request_id = restored
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap()
        .transfer_request_id
        .expect("actual foreground producer owns its native reply");
    let original = restored.transport.drain_incoming_messages().unwrap().into_iter().find(|routed| {
        matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(reply) if reply.request_id == request_id
            && matches!(&reply.result, DfsBlobTransferResult::Durable { .. } | DfsBlobTransferResult::Committed { .. }))
    }).unwrap();
    // The reply is the actual native receiver's positive result. Main's durable
    // queue still has no expected request ID: the client owns this outstanding
    // reply, and replaying it to another owner cannot manufacture correlation.
    let durable_before = queue.view().unwrap().records;
    assert!(
        durable_before
            .iter()
            .find(|row| row.intent_id == intent_id)
            .unwrap()
            .transfer_request_id
            .is_none()
    );
    let before_receipts = restored
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert!(
        !before_receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    );
    let command = DfsMessageCommand {
        request_id: sinomo_core::Id::generate().to_string(),
        routed_message: original,
    };
    // Release the cached CoreEffect observer before Main handles this replay.
    drop(restored);
    runtime
        .rpc_request(
            &replica,
            &owner,
            dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    // Reopen the normal actual owner so this reads Main's newly stored state.
    let observed = runtime.metadata_primary_service(&stat.path).unwrap();
    let after_receipts = observed
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        after_receipts, before_receipts,
        "an authenticated positive reply without exact current queue/map custody cannot persist a receipt"
    );
    let durable_after = queue.view().unwrap().records;
    let protected = durable_after
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap();
    assert_eq!(protected.transfer_request_id, None);
    assert_eq!(
        protected.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(protected.completed_transfer_request_id.is_none());
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.assert_fault_budgets().unwrap();
    drop(observed);
    runtime.close().unwrap();
}

/// Shared normal Main W2/N3 producer. B's genuine precommit Stage completes
/// at its actual receiver, but its response is lost before Main can consume it.
pub(super) fn native_committed_stage_backlog(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    crate::test_runtime::CapturedStageExchange,
    crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
) {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let (runtime, mut writer, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    let owner = config.primary_node_id;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let replica_a = peers[0].clone();
    let replica_b = peers[1].clone();
    let path = format!("/services/{name}.bin");
    writer
        .posix_write_file(
            &path,
            b"actual native Stage predecessor",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    drop(writer);
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).unwrap();
    placement.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let target_digest = *blake3::hash(replica_b.as_str().as_bytes()).as_bytes();
    let background_guard = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&owner),
        Arc::new(move |point, _, _| {
            if matches!(point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                    kind: DfsRepairTransferKind::Repair,
                    background_repair: true,
                    target_id_digest,
                    ..
                } if target_id_digest == target_digest)
            {
                // A negative scheduling fault at the normal claim boundary.
                // The same CoreEffect foreground producer remains unmodified.
                return Err(DfsError::Cancelled {
                    operation: "negative native background transfer preclaim interruption",
                });
            }
            Ok(())
        }),
    );
    let captured = Arc::new(Mutex::new(
        crate::test_runtime::StageExchangeCapture::default(),
    ));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica_b.clone()),
        route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::CaptureStageExchangeAndLoseAfterDispatch(
            Arc::clone(&captured),
        ),
    });
    let bytes = (0..256 * 1024 + 31)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut policy = DfsReplicationPolicy::new(2, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    // The real CoreEffect foreground writer keeps its normal N-target Stage
    // budget. Served Main intentionally returns at W and repairs the rest.
    let mut writer = runtime.metadata_primary_service(&path).unwrap();
    let stat = writer
        .posix_write_file(
            &path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: nodes,
                policy,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let exchange = {
        let observed = captured.lock().unwrap();
        assert_eq!(observed.dropped_count, 0);
        observed
            .exchanges
            .iter()
            .find(|exchange| {
                matches!(&exchange.command.routed_message.message,
                DfsNetworkMessage::StageBlob(stage)
                    if stage.object_id == stat.object_id && stage.version == stat.version)
            })
            .expect("actual W2/N3 Main fanout must supply B's genuine named Stage")
            .clone()
    };
    let DfsNetworkMessage::StageBlob(stage) = &exchange.command.routed_message.message else {
        unreachable!()
    };
    let DfsNetworkMessage::StageBlobResponse(reply) = &exchange.response.message else {
        unreachable!()
    };
    assert_eq!(exchange.source_node_id, owner);
    assert_eq!(exchange.target_node_id, replica_b);
    assert_eq!(exchange.command.routed_message.source_node_id, owner);
    assert_eq!(exchange.command.routed_message.target_node_id, replica_b);
    assert_eq!(exchange.response_request_id, exchange.command.request_id);
    assert_eq!(exchange.response_outbound_message_count, 1);
    assert_eq!(exchange.response.source_node_id, replica_b);
    assert_eq!(exchange.response.target_node_id, owner);
    assert_eq!(reply.request_id, stage.request_id);
    assert_eq!(reply.intent_id, stage.intent_id);
    assert_eq!(reply.file_system_id, stage.file_system_id);
    assert_eq!(reply.object_id, stat.object_id);
    assert_eq!(reply.version, stat.version);
    assert_eq!(reply.replica_node_id, replica_b);
    assert!(matches!(&reply.result,
        DfsBlobTransferResult::Durable { content_hash } |
        DfsBlobTransferResult::Committed { content_hash }
            if Some(content_hash.as_str()) == stat.content_hash.as_deref()));
    assert_eq!(exchange.body_bytes, Some(bytes.len() as u64));
    assert_eq!(stage.content_hash, stat.content_hash.clone().unwrap());
    assert_eq!(stage.file_system_id, stat.file_system_id);
    assert!(stage.authority_shard.is_some());
    let intent_id = stage.intent_id.clone();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(&owner),
    ));
    // Checkpoint only the exact custody produced by this real foreground
    // writer. Preserve all other physical rows and reject conflicting claims.
    let produced = writer
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica_b)
        .expect("normal CoreEffect writer retains its genuine missing-N Stage");
    assert_eq!(produced.transfer_kind, Some(DfsRepairTransferKind::Stage));
    assert_eq!(
        produced.transfer_request_id.as_deref(),
        Some(stage.request_id.as_str())
    );
    assert_eq!(
        produced.transfer_delivery_state,
        Some(NetworkDeliveryState::PossiblyApplied)
    );
    let (current, observed) = queue.view_for_maintenance().unwrap();
    let mut checkpoint = current.records;
    if let Some(existing) = checkpoint
        .iter_mut()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica_b)
    {
        if crate::service_repair::repair_transfer_custody_unresolved(existing)
            || existing.completed_transfer_request_id.is_some()
            || existing.in_flight
            || existing.transfer_delivery_state != Some(NetworkDeliveryState::DefinitelyNotSent)
        {
            assert_eq!(
                *existing, produced,
                "never replace current named, UNKNOWN, in-flight or completed custody"
            );
        } else {
            assert!(existing.transfer_kind.is_some());
            assert!(existing.transfer_request_id.is_none());
            assert_eq!(existing.object_id, produced.object_id);
            assert_eq!(existing.version, produced.version);
            assert_eq!(existing.content_hash, produced.content_hash);
        }
        *existing = produced;
    } else {
        checkpoint.push(produced);
    }
    assert!(
        matches!(
            queue.reconcile_observed(&observed, &checkpoint).unwrap(),
            crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(_)
        ),
        "current queue CAS must reject generation drift"
    );
    drop(writer);
    let records = queue.view().unwrap().records;
    let named = records
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica_b)
        .expect("Main must retain its genuine missing-N Stage custody");
    assert_eq!(named.object_id, stat.object_id);
    assert_eq!(named.version, stat.version);
    assert_eq!(named.content_hash, stage.content_hash);
    assert_eq!(named.transfer_kind, Some(DfsRepairTransferKind::Stage));
    assert_eq!(
        named.transfer_request_id.as_deref(),
        Some(stage.request_id.as_str())
    );
    assert_eq!(
        named.transfer_delivery_state,
        Some(NetworkDeliveryState::PossiblyApplied)
    );
    assert!(named.completed_transfer_request_id.is_none());
    assert!(
        !named.in_flight,
        "the lost native carrier has ended, while its identity remains unresolved"
    );
    let mut service = runtime.metadata_primary_service(&path).unwrap();
    let backend = service.metadata_backend().unwrap();
    let history = backend
        .object_write_intent_history(&stat.object_id)
        .unwrap();
    let record = history
        .intents
        .iter()
        .find(|row| row.intent_id == intent_id)
        .cloned()
        .or_else(|| {
            history
                .receipts
                .iter()
                .find(|receipt| {
                    receipt.intent_id == intent_id
                        && receipt.publication.is_current()
                        && receipt.file_system_id == stat.file_system_id
                        && receipt.object_id == stat.object_id
                        && receipt.target_version == stat.version
                        && receipt.content_hash == stat.content_hash
                })
                .map(|receipt| receipt.to_intent_record())
        })
        .expect("actual stored current owner/receipt must support normal repair restoration");
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    assert!(record.publication.is_current());
    assert_eq!(record.file_system_id, stat.file_system_id);
    assert_eq!(record.object_id, stat.object_id);
    assert_eq!(record.target_version, stat.version);
    assert_eq!(record.content_hash, stat.content_hash);
    assert_eq!(record.base_version, stage.expected_base_version);
    assert_eq!(record.coordinator_node_id, owner);
    assert_eq!(
        record
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica_a.clone(), replica_b.clone()]),
    );
    assert_eq!(
        (
            record.policy.min_commit_replicas,
            record.policy.desired_replicas
        ),
        (2, 3)
    );
    let receipts = backend
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    let actual_holders = receipts
        .iter()
        .filter(|receipt| receipt.content_hash.as_deref() == stat.content_hash.as_deref())
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_holders, BTreeSet::from([owner.clone(), replica_a]));
    assert_eq!(
        service
            .content()
            .read_blob(stat.content_hash.as_deref().unwrap())
            .unwrap(),
        bytes
    );
    service
        .retry_pending_repairs_bounded_with_attempts(0, &records)
        .unwrap();
    let restored = service.durable_repair_queue_records();
    let restored = restored
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica_b)
        .unwrap();
    assert_eq!(restored.transfer_kind, Some(DfsRepairTransferKind::Stage));
    assert_eq!(
        restored.transfer_request_id.as_deref(),
        Some(stage.request_id.as_str())
    );
    assert_eq!(
        restored.transfer_delivery_state,
        Some(NetworkDeliveryState::PossiblyApplied)
    );
    assert!(restored.completed_transfer_request_id.is_none());
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    (
        runtime,
        owner,
        replica_b,
        service,
        stat,
        intent_id,
        exchange,
        background_guard,
    )
}

#[test]
fn native_stage_queue_settles_its_exact_terminal_or_not_sent_before_repair_transition() {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    for not_sent in [false, true] {
        let (runtime, owner, replica, mut service, stat, intent_id, exchange, background_guard) =
            native_committed_stage_backlog(if not_sent {
                "native-stage-queue-not-sent"
            } else {
                "native-stage-queue-terminal"
            });
        let DfsNetworkMessage::StageBlob(stage) = &exchange.command.routed_message.message else {
            unreachable!()
        };
        let stage_id = stage.request_id.clone();
        runtime.clear_faults();
        if not_sent {
            runtime.install_fault(crate::test_runtime::FaultRule {
                source: Some(owner.clone()),
                target: Some(replica.clone()),
                route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
                call_id: None,
                origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
                remaining_hits: 1,
                action: crate::test_runtime::FaultAction::BudgetExhausted,
            });
            // Retry ONLY the genuine precommit sibling's unresolved ID and
            // immutable command DATA. A failed later send cannot erase the
            // already possibly-applied original receiver.
            let outcomes = service
                .send_stage_blob_attachments_if_routable(
                    &[(replica.clone(), stage_id.clone())],
                    &stage.intent_id,
                    &stage.file_system_id,
                    &stage.object_id,
                    stage.expected_base_version.clone(),
                    &stage.version,
                    &stage.content_hash,
                    exchange.body_bytes.unwrap(),
                    stage.object_metadata.clone(),
                )
                .unwrap();
            assert_eq!(outcomes.len(), 1);
            assert!(!outcomes[0].sent);
            assert_eq!(
                outcomes[0].delivery_state,
                Some(NetworkDeliveryState::DefinitelyNotSent)
            );
            let current = service.durable_repair_queue_records();
            let protected = current
                .iter()
                .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
                .unwrap();
            assert_eq!(protected.transfer_kind, Some(DfsRepairTransferKind::Stage));
            assert_eq!(
                protected.transfer_request_id.as_deref(),
                Some(stage_id.as_str())
            );
            assert_eq!(
                protected.transfer_delivery_state,
                Some(NetworkDeliveryState::PossiblyApplied)
            );
            assert!(protected.completed_transfer_request_id.is_none());
            assert!(!protected.in_flight);
            runtime.clear_faults();
        }
        let mut original = exchange.response.clone();
        let DfsNetworkMessage::StageBlobResponse(reply) = &mut original.message else {
            unreachable!()
        };
        reply.result = DfsBlobTransferResult::Rejected {
            reason: "negative exact captured native Stage terminal control".into(),
        };
        let mut wrong = original.clone();
        let DfsNetworkMessage::StageBlobResponse(reply) = &mut wrong.message else {
            unreachable!()
        };
        reply.request_id.push_str("-wrong");
        service
            .process_routed_messages_with_options(vec![wrong], true)
            .unwrap();
        let unchanged = service.durable_repair_queue_records();
        let unchanged = unchanged
            .iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap();
        assert_eq!(unchanged.transfer_kind, Some(DfsRepairTransferKind::Stage));
        assert_eq!(
            unchanged.transfer_request_id.as_deref(),
            Some(stage_id.as_str())
        );
        assert_eq!(
            unchanged.transfer_delivery_state,
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        assert!(unchanged.completed_transfer_request_id.is_none());
        service
            .process_routed_messages_with_options(vec![original], true)
            .unwrap();
        let mut stage_settled = service.durable_repair_queue_records();
        let settled = stage_settled
            .iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap();
        assert_eq!(settled.transfer_kind, Some(DfsRepairTransferKind::Stage));
        assert!(settled.transfer_request_id.is_none());
        assert_eq!(
            settled.completed_transfer_request_id.as_deref(),
            Some(stage_id.as_str())
        );
        assert_eq!(
            settled.transfer_delivery_state,
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        assert!(!settled.in_flight);
        if not_sent {
            // After the exact old Stage has settled, the shared normal Stage
            // producer owns a new request for this same immutable DATA.
            let new_stage_id = service.next_request_id();
            assert_ne!(new_stage_id, stage_id);
            runtime.install_fault(crate::test_runtime::FaultRule {
                source: Some(owner.clone()),
                target: Some(replica.clone()),
                route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
                call_id: None,
                origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
                remaining_hits: 1,
                action: crate::test_runtime::FaultAction::BudgetExhausted,
            });
            let before_new_stage = runtime.rpc_observations().len();
            let outcomes = service
                .send_stage_blob_attachments_if_routable(
                    &[(replica.clone(), new_stage_id)],
                    &stage.intent_id,
                    &stage.file_system_id,
                    &stage.object_id,
                    stage.expected_base_version.clone(),
                    &stage.version,
                    &stage.content_hash,
                    exchange.body_bytes.unwrap(),
                    stage.object_metadata.clone(),
                )
                .unwrap();
            assert_eq!(outcomes.len(), 1);
            assert!(!outcomes[0].sent);
            assert_eq!(
                outcomes[0].delivery_state,
                Some(NetworkDeliveryState::DefinitelyNotSent)
            );
            assert!(runtime.rpc_observations()[before_new_stage..].iter().any(
                |event| event.source == owner
                    && event.target == replica
                    && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            ));
            stage_settled = service.durable_repair_queue_records();
            let unsent_stage = stage_settled
                .iter()
                .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
                .unwrap();
            assert_eq!(
                unsent_stage.transfer_kind,
                Some(DfsRepairTransferKind::Stage)
            );
            assert_eq!(
                unsent_stage.transfer_delivery_state,
                Some(NetworkDeliveryState::DefinitelyNotSent)
            );
            assert!(unsent_stage.transfer_request_id.is_none());
            assert!(unsent_stage.completed_transfer_request_id.is_none());
            assert!(!unsent_stage.in_flight);
            runtime.clear_faults();
        }
        checkpoint_actual_native_custody(&runtime, &owner, &stage_settled);
        drop(service);
        let mut restored = runtime.metadata_primary_service(&stat.path).unwrap();
        restored
            .retry_pending_repairs_bounded_with_attempts(0, &stage_settled)
            .unwrap();
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(owner.clone()),
            target: Some(replica.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
        let before = runtime.rpc_observations().len();
        // The existing normal maintenance producer chooses the Repair ID and
        // command after the exact Stage terminal boundary has settled.
        restored
            .retry_pending_repairs_bounded_with_attempts(1, &stage_settled)
            .unwrap();
        assert!(
            runtime.rpc_observations()[before..]
                .iter()
                .any(|event| event.source == owner
                    && event.target == replica
                    && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch)
        );
        let transitioned = restored.durable_repair_queue_records();
        let row = transitioned
            .iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
            .unwrap();
        assert_eq!(row.transfer_kind, Some(DfsRepairTransferKind::Repair));
        assert_eq!(
            row.transfer_delivery_state,
            Some(NetworkDeliveryState::DefinitelyNotSent)
        );
        assert!(
            row.completed_transfer_request_id.is_none(),
            "Stage terminal evidence never becomes Repair completion"
        );
        assert!(row.transfer_request_id.is_none());
        assert!(!row.in_flight);
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime.assert_fault_budgets().unwrap();
        drop(restored);
        runtime.clear_faults();
        drop(background_guard);
        runtime.close().unwrap();
    }
}

#[test]
fn native_retained_owner_prepublication_interruption_and_queue_cas_recover_without_freshness_loss()
{
    use crate::runtime_host::DaemonDfsPreDispatchTestPoint;
    use sinomo_core::network::NetworkDeliveryState;
    for interrupted in [true, false] {
        let (mut runtime, owner, remote, core, stat, _canonical_id, _background_interception) =
            native_committed_repair_backlog(
                if interrupted {
                    "native-retained-prepublication-cut"
                } else {
                    "native-retained-prepublication-cas"
                },
                false,
            );
        drop(core);
        let namespace = "vendor.prepublication-control";
        let reference_id = "actual-retained-root";
        // Preserve different bytes through the ordinary Main/core producer.
        // Its Retained publication leaves the canonical head unchanged.
        let mut writer = runtime.client(&owner).unwrap();
        let crate::DfsPreservedContentOutcome::Retained { identity } = writer
            .preserve_content_revision(
                crate::DfsPreservedContentRequest {
                    path: stat.path.clone(),
                    file_system_id: stat.file_system_id.clone(),
                    object_id: stat.object_id.clone(),
                    operation_id: sinomo_core::Id::generate().to_string(),
                    publication: crate::DfsRetainedContentPublication {
                        namespace: namespace.into(),
                        reference_id: reference_id.into(),
                        metadata: None,
                    },
                },
                &b"retained bytes independent of the canonical head"[..],
                DfsWriteOptions {
                    replica_candidates: vec![owner.clone(), remote.clone()],
                    policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap()
        else {
            panic!("actual retained content reaches W");
        };
        drop(writer);
        let mut core = runtime.metadata_primary_service(&stat.path).unwrap();
        let initial = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        let original_id = initial
            .intents
            .iter()
            .find(|record| {
                record.target_version == identity.version
                    && matches!(
                        record.publication,
                        crate::DfsWritePublication::Retained { .. }
                    )
            })
            .expect("ordinary preserve owns an actual FULL retained-content intent")
            .intent_id
            .clone();
        core.retain_content_revision(DfsContentReferenceRequest {
            namespace: namespace.into(),
            path: stat.path.clone(),
            identity: identity.clone(),
            operation: DfsContentReferenceOperation::Release {
                reference_id: reference_id.into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
        drop(core);
        let mut core = runtime.metadata_primary_service(&stat.path).unwrap();
        let backend = core.metadata_backend().unwrap();
        let version = backend
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .unwrap();
        assert!(
            version.has_retention_grant(),
            "Release withdraws the reference, retaining immutable grant provenance"
        );
        assert!(
            backend
                .version_pins_for_object(&identity.object_id)
                .unwrap()
                .iter()
                .all(|pin| pin.version != identity.version),
            "ordinary Release removes the actual pin"
        );
        let new_id = crate::file_history::retained_repair_intent_id(&version);
        let history = backend
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        assert!(
            history
                .intents
                .iter()
                .any(|record| record.intent_id == original_id
                    && record.status == DfsWriteIntentStatus::Committed),
            "the first GC still has the actual FULL original retained-content owner"
        );
        assert!(
            !history
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == original_id),
            "the original has not been compacted before its real first producer cut"
        );
        assert!(
            !history
                .intents
                .iter()
                .any(|record| record.intent_id == new_id)
        );
        assert!(
            !history
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == new_id)
        );
        let reference = DfsContentReferenceRequest {
            namespace: namespace.into(),
            path: stat.path.clone(),
            identity: identity.clone(),
            operation: DfsContentReferenceOperation::Retain {
                reference_id: reference_id.into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        };
        let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
            runtime.data_dir(&owner),
        ));
        let before = queue.view().unwrap();
        let actual_existing = before
            .records
            .iter()
            .find(|record| record.intent_id == original_id)
            .or_else(|| before.records.first())
            .expect("actual remaining Main queue custody for an identity-only generation fault")
            .clone();
        let expected = *blake3::hash(new_id.as_bytes()).as_bytes();
        let reached = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&reached);
        let generation_store = queue.clone();
        let cas_basis = Arc::new(std::sync::Mutex::new(None));
        let captured_cas_basis = Arc::clone(&cas_basis);
        let hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            runtime.data_dir(&owner),
            Arc::new(move |point, command, context| {
                let matching = if interrupted {
                    matches!(point, DaemonDfsPreDispatchTestPoint::AfterRetainedRepairQueueEnrollment { owner_id_digest } if owner_id_digest == expected)
                } else {
                    matches!(point, DaemonDfsPreDispatchTestPoint::BeforeRetainedRepairQueueEnrollment { owner_id_digest } if owner_id_digest == expected)
                };
                if !matching {
                    return Ok(());
                }
                observed.fetch_add(1, Ordering::SeqCst);
                assert!(command.is_none() && context.is_none());
                if interrupted {
                    return Err(DfsError::Cancelled {
                        operation: "retained repair prepublication",
                    });
                }
                // A real existing value changes only the owned log generation.
                let current = generation_store.view()?;
                *captured_cas_basis.lock().unwrap() = Some(current.records.clone());
                let actual = current
                    .records
                    .iter()
                    .find(|row| {
                        row.intent_id == actual_existing.intent_id
                            && row.missing_replica_node_id
                                == actual_existing.missing_replica_node_id
                    })
                    .or_else(|| current.records.first())
                    .ok_or_else(|| DfsError::Metadata {
                        message: "generation fault requires actual current queue data".into(),
                    })?
                    .clone();
                generation_store
                    .append_operations(vec![DfsRepairQueueOperation::Set { record: actual }])?;
                Ok(())
            }),
        );
        // The real first owner is created by this ordinary GC compaction,
        // before a later Retain can reactivate it. Inject the cut here.
        let gc_options = DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
            .with_now_unix_nanos(crate::test_support::now_unix_nanos());
        assert!(core.gc_metadata_history(gc_options.clone()).is_err());
        assert!(
            reached.load(Ordering::SeqCst) >= 1,
            "the actual GC commit reaches its new-owner queue checkpoint"
        );
        let stored = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        assert!(
            stored
                .intents
                .iter()
                .any(|record| record.intent_id == original_id
                    && record.status == DfsWriteIntentStatus::Committed)
        );
        assert!(
            !stored
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == original_id),
            "failed GC rolls back original FULL-to-compact publication"
        );
        assert_eq!(
            core.metadata_backend()
                .unwrap()
                .get_version(&identity.object_id, &identity.version)
                .unwrap()
                .unwrap(),
            version,
            "failed GC preserves actual immutable grant provenance"
        );
        assert!(
            core.metadata_backend()
                .unwrap()
                .version_pins_for_object(&identity.object_id)
                .unwrap()
                .iter()
                .all(|pin| pin.version != identity.version)
        );
        assert!(
            !stored
                .intents
                .iter()
                .any(|record| record.intent_id == new_id)
        );
        assert!(
            !stored
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == new_id)
        );
        let cut = queue.view().unwrap();
        if interrupted {
            let dormant = cut
                .records
                .iter()
                .filter(|row| row.intent_id == new_id)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(dormant.len(), 1);
            assert_eq!(dormant[0].missing_replica_node_id, remote);
            assert_eq!(
                dormant[0].transfer_delivery_state,
                Some(NetworkDeliveryState::DefinitelyNotSent)
            );
            assert!(dormant[0].transfer_request_id.is_none());
            assert!(
                core.retry_pending_repairs_bounded_with_attempts(1, &dormant)
                    .is_err(),
                "dormant queue facts cannot create an executable metadata owner"
            );
            assert!(core.coordinator().intent(&new_id).is_none());
            assert!(!core.writes.pending_writes.contains_key(&new_id));
        } else {
            assert_eq!(
                cut.records,
                cas_basis.lock().unwrap().clone().unwrap(),
                "the generation-only fault preserves actual facts read at its cut"
            );
            assert!(!cut.records.iter().any(|row| row.intent_id == new_id));
        }
        drop(hook);
        // Observe true born facts at the actual queue checkpoint BEFORE owner
        // exposure; ordinary background work may claim them after publication.
        let born = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_birth = Arc::clone(&born);
        let birth_store = queue.clone();
        let birth_target = remote.clone();
        let birth_id = new_id.clone();
        let birth_hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            runtime.data_dir(&owner),
            Arc::new(move |point, command, context| {
                if matches!(point, DaemonDfsPreDispatchTestPoint::AfterRetainedRepairQueueEnrollment { owner_id_digest } if owner_id_digest == expected)
                {
                    assert!(command.is_none() && context.is_none());
                    *captured_birth.lock().unwrap() = birth_store
                        .records_for_targets(&birth_id, std::slice::from_ref(&birth_target))?;
                }
                Ok(())
            }),
        );
        let before_dispatch = runtime.rpc_observations().len();
        drop(core);
        if interrupted {
            runtime.stop_node(&owner).unwrap();
            runtime.reopen_node(owner.clone()).unwrap();
        }
        let background = runtime
            .capture_checkpointed_background_repairs(&owner, 32)
            .unwrap();
        let core = runtime.metadata_primary_service(&stat.path).unwrap();
        let resumed = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        let already_published = resumed.intents.iter().any(|row| {
            row.intent_id == new_id
                && row.publication.is_retained_repair()
                && row.status == DfsWriteIntentStatus::Committed
        });
        drop(core);
        if !already_published {
            runtime
                .gc_metadata_history_on_current_primaries(gc_options)
                .unwrap();
        }
        let core = runtime.metadata_primary_service(&stat.path).unwrap();
        let actual = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        assert!(
            !actual
                .intents
                .iter()
                .any(|record| record.intent_id == original_id)
        );
        assert!(
            actual
                .intents
                .iter()
                .any(|record| record.intent_id == new_id
                    && record.publication.is_retained_repair()
                    && record.status == DfsWriteIntentStatus::Committed)
        );
        let own = queue
            .view()
            .unwrap()
            .records
            .into_iter()
            .filter(|row| row.intent_id == new_id)
            .collect::<Vec<_>>();
        assert_eq!(own.len(), 1);
        let initial_birth = born.lock().unwrap();
        assert_eq!(initial_birth.len(), 1);
        assert_eq!(initial_birth[0].object_id, identity.object_id);
        assert_eq!(initial_birth[0].version, identity.version);
        assert_eq!(initial_birth[0].missing_replica_node_id, remote);
        assert_eq!(
            initial_birth[0].transfer_kind,
            Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
        );
        assert_eq!(
            initial_birth[0].transfer_delivery_state,
            Some(NetworkDeliveryState::DefinitelyNotSent)
        );
        assert!(initial_birth[0].transfer_request_id.is_none());
        assert!(initial_birth[0].completed_transfer_request_id.is_none());
        drop(initial_birth);
        drop(birth_hook);
        drop(core);
        // GC created the inactive owner. Only this ordinary live Retain permits
        // its normal missing-N executor to dispatch; no grant is fabricated.
        let mut client = runtime.client(&owner).unwrap();
        client.retain_content_revision(reference).unwrap();
        drop(client);
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let prepared = background.records();
            let dispatched = runtime.rpc_observations()[before_dispatch..]
                .iter()
                .any(|event| {
                    if event.source != owner
                        || event.target != remote
                        || event.phase != crate::test_runtime::RpcPhase::BeforeDispatch
                        || event.route != DFS_DATA_REPAIR_REPLICA_ROUTE
                        || event.dfs_intent_id.as_deref() != Some(new_id.as_str())
                        || event.dfs_file_system_id.as_ref() != Some(&identity.file_system_id)
                        || event.dfs_object_id.as_ref() != Some(&identity.object_id)
                        || event.dfs_version.as_ref() != Some(&identity.version)
                    {
                        return false;
                    }
                    // This exact original inline head proves the native attempt
                    // even when normal recovery dispatched before observer install.
                    event.dfs_frame_id.is_some()
                        && (prepared.iter().all(|record| match &record.message.message {
                            DfsNetworkMessage::RepairReplica(command) => {
                                command.intent_id != new_id
                            }
                            DfsNetworkMessage::RepairRetainedBlob(command) => {
                                command.intent_id != new_id
                            }
                            _ => true,
                        }) || prepared.iter().any(|record| {
                            if record.message.frame_id != event.dfs_frame_id.unwrap()
                                || record.message.target_node_id != remote
                            {
                                return false;
                            }
                            match &record.message.message {
                                DfsNetworkMessage::RepairReplica(command) => {
                                    command.intent_id == new_id
                                        && command.file_system_id == identity.file_system_id
                                        && command.object_id == identity.object_id
                                        && command.version == identity.version
                                }
                                DfsNetworkMessage::RepairRetainedBlob(command) => {
                                    command.intent_id == new_id
                                        && command.file_system_id == identity.file_system_id
                                        && command.object_id == identity.object_id
                                        && command.version == identity.version
                                }
                                _ => false,
                            }
                        }))
                });
            if dispatched {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "actual normal Main must prepare and dispatch this retained owner within its existing network budget"
            );
            let pass = runtime.maintenance_tick(&owner, remaining).unwrap();
            pass.outcome
                .expect("actual normal maintenance owner outcome");
        }
        assert_eq!(background.omitted_count(), 0);
        drop(background);
        runtime.assert_fault_budgets().unwrap();
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime.clear_faults();
        runtime.close().unwrap();
    }
}

#[test]
fn native_connectivity_park_returns_through_actual_main_repair() {
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let (mut runtime, owner, replica, mut core, stat, intent_id, background) =
        native_committed_repair_backlog("native-connectivity-park-return", true);
    let before = runtime.rpc_observations().len();
    core.retry_pending_repairs_bounded(1).unwrap();
    let actual = core.durable_repair_queue_records();
    let failed = actual
        .iter()
        .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .unwrap()
        .clone();
    assert!(failed.attempts > 0);
    assert_eq!(failed.transfer_kind, Some(DfsRepairTransferKind::Repair));
    assert_eq!(
        failed.transfer_delivery_state,
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(failed.transfer_request_id.is_none());
    assert!(failed.completed_transfer_request_id.is_none());
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch),
        "the settled retry is produced by an actual native content attempt"
    );
    let queue = checkpoint_actual_native_custody(&runtime, &owner, &actual);
    drop(core);
    runtime.clear_faults();
    runtime.stop_node(&replica).unwrap();
    drop(background);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let parked = loop {
        if let Some(row) = queue.view().unwrap().records.into_iter().find(|row| {
            row.intent_id == intent_id
                && row.missing_replica_node_id == replica
                && row.next_retry_unix_nanos == u64::MAX
        }) {
            break row;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal Main must checkpoint the actual withdrawn-route blocker within its existing budget"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    };
    let mut expected = failed;
    expected.next_retry_unix_nanos = u64::MAX;
    expected.last_error =
        Some(crate::maintenance::DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY.into());
    assert_eq!(
        parked, expected,
        "parking changes only actual scheduling and the routing diagnostic"
    );
    // Lose only the canonical attempt's genuine native return. A placement
    // owner for the same bytes remains free to satisfy the actual missing N.
    let lost = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id: Some(intent_id.clone()),
            captured: Arc::clone(&lost),
        },
    });
    let before_return = runtime.rpc_observations().len();
    runtime.reopen_node(replica.clone()).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let claimed = loop {
        let core = runtime.metadata_primary_service(&stat.path).unwrap();
        let receipt = core
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .into_iter()
            .find(|receipt| {
                receipt.replica_node_id == replica
                    && receipt.receipt.file_system_id == stat.file_system_id
                    && receipt.receipt.object_id == stat.object_id
                    && receipt.receipt.committed_version == stat.version
                    && receipt.content_hash == stat.content_hash
            });
        drop(core);
        let current = queue
            .view()
            .unwrap()
            .records
            .into_iter()
            .find(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica);
        let replies = lost.lock().unwrap();
        for routed in replies.iter() {
            let DfsNetworkMessage::RepairReplicaResponse(reply) = &routed.message else {
                panic!("the canonical response-loss cut captures only actual Repair replies");
            };
            assert_eq!(routed.source_node_id, replica);
            assert_eq!(routed.target_node_id, owner);
            assert_eq!(reply.intent_id, intent_id);
            assert_eq!(reply.file_system_id, stat.file_system_id);
            assert_eq!(reply.object_id, stat.object_id);
            assert_eq!(reply.version, stat.version);
            assert_eq!(reply.replica_node_id, replica);
            assert!(
                matches!(&reply.result, DfsBlobTransferResult::Committed { content_hash }
                if Some(content_hash.as_str()) == stat.content_hash.as_deref())
            );
            assert!(
                runtime.rpc_observations()[before_return..]
                    .iter()
                    .any(|event| {
                        event.source == owner
                            && event.target == replica
                            && event.phase == crate::test_runtime::RpcPhase::AfterDispatch
                            && event.dfs_intent_id.as_deref() == Some(intent_id.as_str())
                            && event.dfs_request_id.as_deref() == Some(reply.request_id.as_str())
                            && event.dfs_file_system_id.as_ref() == Some(&stat.file_system_id)
                            && event.dfs_object_id.as_ref() == Some(&stat.object_id)
                            && event.dfs_version.as_ref() == Some(&stat.version)
                    })
            );
        }
        if receipt.is_some() {
            if let Some(current) = current {
                if replies.iter().any(|routed| {
                    matches!(&routed.message, DfsNetworkMessage::RepairReplicaResponse(reply)
                        if current.transfer_request_id.as_deref() == Some(reply.request_id.as_str()))
                }) {
                    assert_eq!(current.object_id, stat.object_id);
                    assert_eq!(current.version, stat.version);
                    assert_eq!(Some(current.content_hash.as_str()), stat.content_hash.as_deref());
                    assert_eq!(current.transfer_kind, Some(DfsRepairTransferKind::Repair));
                    assert_eq!(current.transfer_delivery_state, Some(NetworkDeliveryState::PossiblyApplied));
                    assert!(current.completed_transfer_request_id.is_none());
                    assert!(crate::service_repair::repair_transfer_custody_unresolved(&current));
                    break current;
                }
            }
        }
        drop(replies);
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual Main repair after endpoint return must settle within its existing network budget"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    };
    assert!(
        runtime.rpc_observations()[before_return..]
            .iter()
            .any(|event| event.source == owner
                && event.target == replica
                && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                && event.dfs_intent_id.as_deref() == Some(intent_id.as_str())
                && event.dfs_object_id.as_ref() == Some(&stat.object_id)
                && event.dfs_version.as_ref() == Some(&stat.version))
    );
    let mut reader = runtime.client(&replica).unwrap();
    assert_eq!(
        reader.posix_read_file(&stat.path).unwrap(),
        (0..256 * 1024 + 31)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>()
    );
    drop(reader);
    let core = runtime.metadata_primary_service(&stat.path).unwrap();
    let missing = crate::DfsOperatorInspector::new(core.metadata_backend().unwrap())
        .repairs(Some(&stat.file_system_id), Some(&stat.object_id))
        .unwrap();
    assert!(
        !missing.iter().any(|repair| repair.intent_id == intent_id
            && repair.version == stat.version
            && repair.missing_replica_node_id == replica),
        "actual durable receipt must settle this exact missing-N obligation"
    );
    drop(core);
    let remaining = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .filter(|row| row.intent_id == intent_id && row.missing_replica_node_id == replica)
        .collect::<Vec<_>>();
    assert_eq!(
        remaining,
        vec![claimed],
        "the sibling's receipt preserves the whole actual canonical claim whose native return was lost"
    );
    runtime.clear_faults();
    runtime.close().unwrap();
}
