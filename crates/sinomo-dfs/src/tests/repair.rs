//! Repair regression tests.

use super::*;

mod executor;
mod receipt_generation;
mod source_fetch_admission;
mod rename_pending;

// Direct Core fixtures still need the mounted owner's real scheduler admission.
// Resolve its current native shard; the normal provider validates the physical
// owner/generation and rechecks that assignment after acquiring ExactShard.
pub(super) fn native_fixture_write_admission(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &NetworkedDfsService<DfsNodeTransport>,
    path: &str,
) -> crate::publication_guard::DfsPublicationContextRestore {
    let control = runtime.shared_control_plane().unwrap();
    let shard = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), path)
            .unwrap()
            .expect("the fixture path has an actual native assignment")
    });
    assert_eq!(&shard.primary_node_id, service.node_id());
    service
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: shard.file_system_id,
            shard_id: DfsMetadataShardId::new(shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: format!("native-fixture-write-admission:{path}"),
        })
        .unwrap()
        .expect("the actual Core provider acquired fresh native admission")
}

// A saved Original keeps background_repair=true even when this admitted Core
// continues it synchronously. Exclude competing Main maintenance by its actual
// preflight boundary, not by changing or rejecting that immutable command bit.
pub(super) fn native_private_repair_owner<H>(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &NetworkedDfsService<DfsNodeTransport>,
    path: &str,
    previous_hold: H,
) -> (
    crate::publication_guard::DfsPublicationContextRestore,
    crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
) {
    let execution = native_fixture_write_admission(runtime, service, path);
    // The current native admission closes the gap while replacing the hold.
    drop(previous_hold);
    let hold = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(service.node_id()),
        Arc::new(|point, command, _context| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            ) && command.is_none()
            {
                return Err(DfsError::Cancelled {
                    operation: "private admitted repair excludes routine Main maintenance",
                });
            }
            Ok(())
        }),
    );
    (execution, hold)
}

// Real configuration precedes each fixture producer. Candidate hints do not
// create a managed W/N contract, and a browser owns no Core coordinator state.
pub(super) fn native_owned_repair_fixture(
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
    // Maintenance invocations can finish a retained turn without running placement.
    // Observe this exact job until its existing native completion boundary.
    let placement_owner = runtime.root_shard().unwrap().primary_node_id;
    loop {
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
        let tick = runtime
            .maintenance_tick(&placement_owner, remaining)
            .unwrap();
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
    assert_eq!(
        completed.state,
        DfsPlacementReconciliationState::Completed,
        "actual empty policy: {completed:?}"
    );
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
    // Placement completion on the root owner does not publish another Main's view.
    runtime.wait_published_configuration_current(&owner).unwrap();
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
        seed.current_read_object_metadata_distributed(&base.object_id)
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
    let execution = native_fixture_write_admission(&runtime, &owner, &unrelated_path);
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
    drop(execution);
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
    // The primary's executor runs concurrently with the served reply; hold it
    // across the write so the reply's own RPC window shows only what the
    // reply path itself sends.
    let executor_hold = runtime.hold_repair_executor(&primary).unwrap();
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
    drop(executor_hold);
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
    native_routed_manifest_write_with_response_pause(
        name,
        min_commit,
        desired,
        primary_is_desired,
        foreground,
        deny_third_target,
        deny_primary_fetch,
        None,
    )
}

#[cfg(feature = "storage-conversion")]
#[test]
fn native_pending_fetch_reply_keeps_correlation_through_content_preparation() {
    let (runtime, primary, source, _, client, request, _, outcome) =
        native_routed_manifest_write("native-fetch-import-wait", 2, 2, true, false, false, true);
    let NetworkedDfsWriteOutcome::Pending { .. } = outcome else {
        panic!("the denied primary fetch must leave the original write pending");
    };
    drop(client);
    let executor_hold = runtime.hold_repair_executor(&primary).unwrap();
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    let (execution, routine_hold) = native_private_repair_owner(&runtime, &owner, "/services", ());
    runtime.clear_faults();
    owner.hydrate_from_metadata_backend().unwrap();
    let intent_id = owner
        .metadata_backend()
        .unwrap()
        .intents_for_object(&request.object_id)
        .unwrap()
        .into_iter()
        .find(|row| {
            row.target_version == request.target_version
                && row.status == DfsWriteIntentStatus::ReceiptsPending
        })
        .expect("the actual native write retains its uncommitted target")
        .intent_id;
    owner
        .restore_pending_write_for_intent_if_missing(&intent_id, &request.object_id)
        .unwrap();
    assert!(
        owner.metadata.object(&request.object_id).is_some(),
        "the live owner retains its real object anchor"
    );
    let intent = owner.coordinator.intent(&intent_id).unwrap().clone();
    let pending = owner.writes.pending_writes.get(&intent_id).unwrap().clone();
    assert!(
        owner
            .request_stage_blob_from_alternate(&intent, &pending)
            .unwrap()
    );
    let replies = owner.transport.drain_incoming_messages().unwrap();
    let response = replies
        .into_iter()
        .find_map(|reply| match reply.message {
            DfsNetworkMessage::FetchBlobResponse(response) => Some(response),
            _ => None,
        })
        .expect("the native source returned its actual stored bytes");
    assert!(
        matches!(&response.result, crate::DfsFetchBlobResult::Found { bytes, .. } if !bytes.is_empty())
    );
    let original_request = response.request_id.clone();
    let content = owner.content().clone();
    // The real staging API marks an empty inventory incomplete. With no
    // converter registered its missing-version check must stay Pending.
    content
        .begin_inventory_import("native-empty-fetch-import")
        .unwrap();
    let first = owner.handle_fetch_blob_response(source.clone(), response.clone());
    let retained = owner
        .writes
        .pending_writes
        .get(&intent_id)
        .unwrap()
        .fetch_inflight_request_ids
        .get(&source)
        .cloned();
    // Wrong request and authenticated source must not consume the retained reply.
    let mut wrong = response.clone();
    wrong.request_id.push_str("-foreign");
    let wrong_request = owner.handle_fetch_blob_response(source.clone(), wrong);
    let wrong_source = owner.handle_fetch_blob_response(primary.clone(), response.clone());
    content
        .complete_inventory_import("native-empty-fetch-import")
        .unwrap();
    let resumed = owner.handle_fetch_blob_response(source, response);
    let record = owner
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    let stored = content
        .read_object_version(request.object_id.as_str(), request.target_version.as_str())
        .unwrap();
    drop(execution);
    drop(owner);
    drop(routine_hold);
    drop(executor_hold);
    runtime.close().unwrap();
    assert!(
        matches!(first, Err(DfsError::StoragePreparationPending { .. })),
        "local preparation must retain delivery: {first:?}"
    );
    assert_eq!(retained.as_deref(), Some(original_request.as_str()));
    assert!(matches!(
        wrong_request,
        Ok(NetworkedDfsServiceEvent::BlobFetchRejected { .. })
    ));
    assert!(matches!(
        wrong_source,
        Ok(NetworkedDfsServiceEvent::BlobFetchRejected { .. })
    ));
    assert!(
        matches!(resumed, Ok(NetworkedDfsServiceEvent::BlobFetched { .. })),
        "{resumed:?}"
    );
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    assert_eq!(record.target_version, request.target_version);
    assert_eq!(record.content_hash, Some(stored.0.content_hash));
}

#[test]
fn native_pending_write_fetches_an_excluded_source_for_an_admitted_target() {
    let (runtime, primary, source, _, client, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-pending-excluded-source",
            2,
            2,
            true,
            false,
            false,
            true,
        );
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    drop(client);
    let executor_hold = runtime.hold_repair_executor(&primary).unwrap();
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    let (execution, routine_hold) = native_private_repair_owner(&runtime, &owner, "/services", ());
    owner.hydrate_from_metadata_backend().unwrap();
    let original = owner
        .metadata_backend()
        .unwrap()
        .intents_for_object(&request.object_id)
        .unwrap()
        .into_iter()
        .find(|row| {
            row.target_version == request.target_version
                && row.status == DfsWriteIntentStatus::ReceiptsPending
        })
        .unwrap();
    owner
        .restore_pending_write_for_intent_if_missing(&original.intent_id, &request.object_id)
        .unwrap();
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    let fetch_count = |runtime: &crate::test_runtime::DfsTestRuntime| {
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|call| {
                call.source == primary
                    && call.target == source
                    && call.route == DFS_DATA_FETCH_BLOB_ROUTE
                    && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            })
            .count()
    };
    let all_excluded =
        crate::DfsMaintenanceExecutionExclusions::new([primary.clone(), source.clone()]);
    crate::test_runtime::executor()
        .block_on(owner.retry_pending_stages_async(1, &all_excluded))
        .unwrap();
    assert_eq!(
        fetch_count(&runtime),
        0,
        "without an admitted target, no fetch is scheduled"
    );
    let source_excluded = crate::DfsMaintenanceExecutionExclusions::new([source.clone()]);
    crate::test_runtime::executor()
        .block_on(owner.retry_pending_stages_async(1, &source_excluded))
        .unwrap();
    let sent = fetch_count(&runtime);
    owner.process_incoming_until_quiescent().unwrap();
    let current = owner
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &original.intent_id)
        .unwrap()
        .unwrap();
    let object = owner
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    drop(execution);
    drop(owner);
    drop(routine_hold);
    drop(executor_hold);
    runtime.close().unwrap();
    assert_eq!(
        sent, 1,
        "required source reads must not inherit destination exclusions"
    );
    assert_eq!(current.status, DfsWriteIntentStatus::Committed);
    assert_eq!(current.operation_id, original.operation_id);
    assert_eq!(object.version, request.target_version);
    assert_eq!(
        object.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
}

fn native_routed_manifest_write_with_response_pause(
    name: &str,
    min_commit: usize,
    desired: usize,
    primary_is_desired: bool,
    foreground: bool,
    deny_third_target: bool,
    deny_primary_fetch: bool,
    response_pause: Option<std::sync::Arc<tokio::sync::Semaphore>>,
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
    if let Some(gate) = response_pause {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(requester.clone()),
            route: Some(DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 1,
            action: crate::test_support::DfsTestFaultAction::PauseAfterDispatch(gate),
        });
    }
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
    // Repair hydration is Main-side work on the primary's store-backed
    // owner; the delegating client keeps no store.
    let mut authority = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(authority.node_id(), &primary);
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
fn served_lookup_path_does_not_retry_repairs_inline() {
    let (runtime, primary, requester, service, base, broken_intent, _path) =
        native_broken_patch_wait("native-served-lookup-no-inline-repair");
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
        .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
            sifs_file_system_id(),
            primary.clone(),
            DfsNamespaceOperation::LookupPath {
                path: "/services".into(),
                inline_range: None,
            },
            crate::DFS_NO_CALLER_BUDGET,
            false,
            false,
        )
        .unwrap()
        .result;
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
    // Repair hydration is Main-side work on the primary's store-backed
    // owner; the delegating client keeps no store.
    let mut authority = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(authority.node_id(), &primary);
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
    // Local content anti-entropy is Main-side maintenance over the node's
    // store-backed metadata owner; the delegating client keeps no store.
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(owner.node_id(), &node);
    let mut cursor = None;
    let mut issues = 0;
    let mut repaired = 0;
    for _ in 0..128 {
        let report = owner
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
    drop(owner);
    let store = runtime.node_metadata(&node).unwrap();
    for object in &objects[..2] {
        let current = store.get_object(&object.object_id).unwrap().unwrap();
        assert!(!current.current_locations.contains(&node));
    }
    let healthy = &objects[2];
    assert!(
        store
            .get_object(&healthy.object_id)
            .unwrap()
            .unwrap()
            .current_locations
            .contains(&node)
    );
    assert_eq!(
        service.posix_read_file_distributed(&healthy.path).unwrap(),
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
    // Local content anti-entropy is Main-side maintenance over the node's
    // store-backed metadata owner; the delegating client keeps no store.
    let mut owner = runtime.metadata_primary_service(&object.path).unwrap();
    let before = owner
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    let report = owner
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
        owner
            .metadata_backend()
            .unwrap()
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap(),
        before
    );
    drop(owner);
    assert_eq!(
        service
            .posix_stat_distributed(&object.path)
            .unwrap()
            .size_bytes,
        Some(bytes.len() as u64)
    );
    assert_eq!(service.posix_read_file_distributed(&object.path).unwrap(), bytes);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn routed_inline_blob_records_requester_receipt_without_restaging() {
    let name = "native-routed-inline-requester-receipt";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = config.primary_node_id.clone();
    drop(service);
    let requester = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary)
        .unwrap();
    let path = "/services/native-inline-requester.bin";
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = vec![primary.clone(), requester.clone()];
    let accepted = runtime
        .apply_placement_policy(&primary, path, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.accepted_config, accepted.accepted_config);
        assert_eq!(job.scanned_object_count, 0);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal empty Apply setup budget");
        runtime
            .maintenance_tick(&primary, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let mut writer = runtime.client(&requester).unwrap();
    let (_, _, replication) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedConfirmed, 2)
        .unwrap();
    let base = writer
        .posix_write_file(
            path,
            b"actual inline predecessor",
            DfsWriteOptions {
                replica_candidates: vec![primary.clone(), requester.clone()],
                policy: replication.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let version = crate::namespace::next_version(&base.version);
    let bytes = b"complete inline routed bytes genuinely held by their requester";
    let manifest = writer
        .content()
        .put_object_version(base.object_id.as_str(), version.as_str(), bytes)
        .unwrap();
    assert_eq!(manifest.content_hash, crate::content_hash(bytes));
    let source = runtime.metadata_primary_service(path).unwrap();
    let object = source
        .metadata_backend()
        .unwrap()
        .get_object(&base.object_id)
        .unwrap()
        .unwrap();
    drop(source);
    let request_id = format!("native-inline-prestaged-{}", sinomo_core::Id::generate());
    let command = DfsMessageCommand {
        request_id: request_id.clone(),
        routed_message: build_dfs_routed_message(
            &requester,
            &primary,
            910,
            911,
            DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                storage_revision: None,
                publication: Default::default(),
                patch: None,
                request_id,
                request: DfsWriteRequest {
                    file_system_id: base.file_system_id,
                    object_id: base.object_id,
                    target_version: version.clone(),
                    requesting_node_id: requester.clone(),
                },
                expected_base_version: Some(base.version),
                metadata_journal_after_sequence: None,
                bytes: bytes.to_vec(),
                content_hash: None,
                content_size_bytes: None,
                requester_blob_pre_staged: true,
                desired_replica_nodes: vec![primary.clone(), requester.clone()],
                policy: replication,
                object_metadata: Some(object),
                authority_shard: Some(config),
                touch_parent_on_create: false,
                foreground_replica_repair: true,
                wait_for_commit: false,
                exclusive_create: false,
            }),
        ),
    };
    let before = runtime.rpc_observations().len();
    let result = runtime
        .rpc_call(
            &requester,
            &primary,
            crate::dfs_message_rpc_call(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let response = crate::decode_dfs_message_rpc_response(&result).unwrap();
    assert!(response.outbound_messages.iter().any(|message| matches!(&message.message,
        DfsNetworkMessage::DistributedWriteResponse(reply) if matches!(&reply.result,
            DfsDistributedWriteResult::Outcome { outcome: NetworkedDfsWriteOutcome::Committed { version: committed, .. } }
                if committed == &version))), "actual prestaged inline completion: {response:?}");
    let stat = writer.posix_stat_distributed(path).unwrap();
    assert_eq!(stat.version, version);
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
    // The primary's durable store; the delegating client keeps no store.
    let receipts = runtime
        .node_metadata(&primary)
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
fn synchronous_recovery_fetches_the_only_missing_local_receipt_within_budget() {
    let (mut runtime, primary, requester, _, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-sync-only-local-receipt",
            2,
            2,
            true,
            false,
            false,
            true,
        );
    let NetworkedDfsWriteOutcome::Pending {
        intent_id: route_id,
        ..
    } = outcome
    else {
        panic!("actual W2 pending caller");
    };
    writer.process_incoming().unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts: 1,
        required_receipts: 2,
    } = writer.write_outcome(&route_id).unwrap()
    else {
        panic!("actual pending native receipt");
    };
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    // After normal startup completes with the original fetch denial active,
    // select this synchronous Core recovery as the producer under test.
    // A restarted Main can independently recover the same missing receipt;
    // its autonomous work is outside this call's zero/one scheduling budget.
    // Deny only maintenance before admission; keep normal RPC endpoints live.
    let background = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&primary),
        Arc::new(|point, command, _| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            ) && command.is_none()
            {
                return Err(DfsError::Cancelled {
                    operation: "negative autonomous maintenance during synchronous recovery budget",
                });
            }
            Ok(())
        }),
    );
    let mut core = runtime.metadata_primary_service("/services").unwrap();
    core.hydrate_from_metadata_backend().unwrap();
    assert!(core.writes.pending_writes.contains_key(&intent_id));
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    core.retry_pending_stages(0).unwrap();
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE)
    );
    assert_ne!(
        core.metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, &intent_id)
            .unwrap()
            .unwrap()
            .status,
        DfsWriteIntentStatus::Committed
    );
    core.retry_pending_stages(1).unwrap();
    core.process_incoming().unwrap();
    let committed = core
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.status, DfsWriteIntentStatus::Committed);
    assert_eq!(committed.target_version, request.target_version);
    assert_eq!(
        committed.content_hash.as_deref(),
        Some(manifest.content_hash.as_str())
    );
    assert_eq!(
        core.metadata_backend()
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), requester.clone()])
    );
    assert_eq!(
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch)
            .count(),
        1
    );
    assert_eq!(
        core.content().read_blob(&manifest.content_hash).unwrap(),
        writer.content().read_blob(&manifest.content_hash).unwrap()
    );
    drop(core);
    drop(writer);
    drop(background);
    runtime.close().unwrap();
}

#[test]
fn eventual_write_with_local_receipt_tolerates_unreachable_desired_replica() {
    let (runtime, owner, replica, service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-eventual-unreachable-desired", false);
    assert_eq!(
        indexed_repair_needs(&runtime, &service, &stat),
        vec![(replica.clone(), Some(intent_id))]
    );
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

// Observe the intended before-dispatch transport fault and its real durable
// retry checkpoint BEFORE withdrawing the source endpoint. Otherwise stop can
// substitute an unrelated "unknown source node" failure; restart correctly
// wakes that connectivity error and creates a second ten-second retry.

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
        let completion_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let maintenance = runtime
            .maintenance_tick(
                &primary,
                completion_deadline.saturating_duration_since(Instant::now()),
            )
            .unwrap();
        maintenance.outcome.as_ref().unwrap();
        let matches_original = |captured: &crate::test_runtime::CapturedFetchReply| {
            matches!(&captured.routed.message,
                DfsNetworkMessage::FetchBlobResponse(response)
                if response.intent_id == intent && response.object_id == request.object_id
                    && response.version == request.target_version)
        };
        let after_tick_reply_count = captured_fetch.lock().unwrap().len();
        let after_tick_matched = captured_fetch.lock().unwrap().iter().any(&matches_original);
        // Main hands off the original call after maintenance admission releases.
        // One returned tick is not transport completion. Observe that SAME call
        // within the existing budget, without issuing another maintenance turn.
        let matched_within_budget = loop {
            let matched = captured_fetch.lock().unwrap().iter().any(&matches_original);
            let remaining = completion_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break false;
            }
            if matched {
                break true;
            }
            std::thread::sleep(remaining.min(Duration::from_millis(1)));
        };
        let replies = captured_fetch.lock().unwrap().clone();
        {
            // Retain both cuts, including any reply still absent at the deadline.
            // Diagnostic I/O cannot replace or mask the exact assertion below.
            let diagnostic = runtime
                .data_dir(&primary)
                .root()
                .join("boundary-diagnostic-post-commit-fetch.json");
            let captured = serde_json::json!({
                "operation_id": operation, "intent_id": intent, "request": request,
                "primary": primary, "requester": requester,
                "content_hash": manifest.content_hash, "bytes": manifest.size_bytes,
                "maintenance": format!("{maintenance:#?}"),
                "after_tick_reply_count": after_tick_reply_count,
                "after_tick_matched_original": after_tick_matched,
                "matched_within_original_budget": matched_within_budget,
                "matched_original_at_final_observation": replies.iter().any(&matches_original),
                "remaining_original_budget_ns": completion_deadline.saturating_duration_since(Instant::now()).as_nanos().to_string(),
                "captured_reply_count": replies.len(),
                "reply_heads": replies.iter().take(8).map(|captured| {
                    let mut head = serde_json::json!({
                        "rpc_request_id": captured.rpc_request_id,
                        "source": captured.routed.source_node_id,
                        "target": captured.routed.target_node_id,
                    });
                    if let DfsNetworkMessage::FetchBlobResponse(response) = &captured.routed.message {
                        head["request_id"] = serde_json::json!(response.request_id);
                        head["intent_id"] = serde_json::json!(response.intent_id);
                        head["object_id"] = serde_json::json!(response.object_id);
                        head["version"] = serde_json::json!(response.version);
                        if let DfsFetchBlobResult::Found { content_hash, bytes } = &response.result {
                            head["found"] = serde_json::json!({"hash":content_hash,"byte_count":bytes.len()});
                        }
                    }
                    head
                }).collect::<Vec<_>>(),
                "reply_heads_omitted": replies.len().saturating_sub(8),
                "rpc_omitted": runtime.observation_dropped_count(),
                "rpc_at_final_observation": format!("{:#?}", runtime.rpc_observations()),
                "qualification": "Actual returned tick and same original detached reply; one unchanged network budget starts before tick. No recovery lookup, additional tick, new write identity, enlarged deadline or supplied reply."
            });
            eprintln!(
                "post-commit fetch diagnostic {:?}: {:?}",
                diagnostic,
                std::fs::write(&diagnostic, serde_json::to_vec_pretty(&captured).unwrap())
            );
        }
        let original = replies
            .iter()
            .find(|captured| {
                matches!(&captured.routed.message,
            DfsNetworkMessage::FetchBlobResponse(response)
            if response.intent_id == intent && response.object_id == request.object_id
                && response.version == request.target_version)
            })
            .expect("actual Main's selected post-commit Fetch reply");
        assert!(
            matched_within_budget,
            "the original detached Fetch must finish within the unchanged network budget"
        );
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
    // Main may return W1 before its normal N3 continuation. Observe the real
    // delivery before judging the command kind or receiver bytes.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let calls = runtime.rpc_observations();
        if nodes.iter().filter(|node| **node != owner).all(|replica| {
            calls[before..].iter().any(|event| {
                event.source == owner
                    && &event.target == replica
                    && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == crate::test_support::DfsTestRpcPhase::Completed
            })
        }) {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal N3 repair continuation did not complete"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
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

fn native_completed_write_observations(
    runtime: &crate::test_support::DfsTestRuntime,
    primary: &NodeId,
    requester: &NodeId,
    write: &DfsWriteRequest,
    request_id: &str,
    deadline: Instant,
    mut on_pending: impl FnMut(),
) -> Vec<crate::test_runtime::RpcObservation> {
    loop {
        assert!(
            Instant::now() < deadline,
            "actual response RPC completion exhausted the original write-response deadline for {request_id}"
        );
        assert_eq!(
            runtime.observation_dropped_count(),
            0,
            "missing observations cannot prove response completion"
        );
        let calls = runtime.rpc_observations();
        let decoded = calls.iter().find(|row| {
            row.source == *primary
                && row.target == *requester
                && row.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && row.phase == crate::test_runtime::RpcPhase::Decoded
                && row.dfs_request_id.as_deref() == Some(request_id)
                && row.dfs_object_id.as_ref() == Some(&write.object_id)
                && row.dfs_version.as_ref() == Some(&write.target_version)
        });
        if let Some(decoded) = decoded {
            assert!(
                !calls.iter().any(|row| row.call_id == decoded.call_id
                    && row.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse),
                "actual response RPC ended without completion for {request_id}: {calls:?}"
            );
            if calls.iter().any(|row| {
                row.call_id == decoded.call_id
                    && row.phase == crate::test_runtime::RpcPhase::Completed
            }) {
                assert!(
                    Instant::now() < deadline,
                    "response observation crossed the original absolute deadline"
                );
                return calls;
            }
        }
        on_pending();
        std::thread::sleep(
            Duration::from_millis(1).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

#[test]
fn native_write_completion_observation_waits_for_actual_response_rpc() {
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let (runtime, primary, requester, replica, mut writer, request, _, outcome) =
        native_routed_manifest_write_with_response_pause(
            "native-response-completion-boundary",
            2,
            3,
            true,
            false,
            true,
            false,
            Some(gate.clone()),
        );
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    let deadline = Instant::now() + Duration::from_secs(5);
    let events = writer
        .process_incoming_until_pending_routed_writes_ready(
            deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap();
    let response_id = events
        .iter()
        .find_map(|event| match event {
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                request_id,
                object_id,
                primary_node_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome:
                            NetworkedDfsWriteOutcome::Committed {
                                version,
                                repair_jobs,
                                ..
                            },
                    },
            } if object_id == &request.object_id
                && primary_node_id == &primary
                && version == &request.target_version
                && repair_jobs
                    .iter()
                    .any(|job| job.missing_replica_node_id == replica) =>
            {
                Some(request_id.clone())
            }
            _ => None,
        })
        .expect("actual matching deferred committed response");
    let before = runtime.rpc_observations();
    let decoded = before
        .iter()
        .find(|row| {
            row.phase == crate::test_runtime::RpcPhase::Decoded
                && row.source == primary
                && row.target == requester
                && row.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && row.dfs_request_id.as_deref() == Some(&response_id)
                && row.dfs_object_id.as_ref() == Some(&request.object_id)
                && row.dfs_version.as_ref() == Some(&request.target_version)
        })
        .unwrap();
    let call_id = decoded.call_id;
    assert!(
        !before
            .iter()
            .any(|row| row.call_id == call_id
                && row.phase == crate::test_runtime::RpcPhase::Completed)
    );
    let mut released = false;
    let observed = native_completed_write_observations(
        &runtime,
        &primary,
        &requester,
        &request,
        &response_id,
        deadline,
        || {
            if !released {
                released = true;
                gate.add_permits(1);
            }
        },
    );
    eprintln!(
        "actual response-boundary request={request:?} response={response_id} call={call_id} events={events:?} before={:?} observed={:?}",
        before
            .iter()
            .filter(|row| row.call_id == call_id)
            .collect::<Vec<_>>(),
        observed
            .iter()
            .filter(|row| row.call_id == call_id)
            .collect::<Vec<_>>()
    );
    // Drain the controlled old failure before asserting, without replacing the
    // actual response or acknowledging anything on its behalf.
    if !released {
        gate.add_permits(1);
    }
    let complete = observed.iter().position(|row| {
        row.call_id == call_id && row.phase == crate::test_runtime::RpcPhase::Completed
    });
    let no_early_repair = complete.is_some_and(|end| {
        !observed.iter().take(end + 1).any(|row| {
            row.source == primary
                && row.target == replica
                && row.phase == crate::test_runtime::RpcPhase::BeforeDispatch
                && [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE]
                    .contains(&row.route.as_str())
        })
    });
    drop(writer);
    runtime.close().unwrap();
    assert!(
        complete.is_some(),
        "the real RPC completion must be observed within the original deadline"
    );
    assert!(
        no_early_repair,
        "deferred repair cannot precede actual response completion"
    );
}

#[test]
fn native_completed_response_progresses_while_advisory_maintenance_is_parked() {
    let (runtime, primary, requester, _, mut writer, request, manifest, outcome) =
        native_routed_manifest_write(
            "native-required-completion-policy",
            2,
            2,
            true,
            false,
            false,
            false,
        );
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    let deadline = Instant::now() + Duration::from_secs(5);
    // Retain the actual authenticated response before its original requester
    // consumes it. No success, receipt, or metadata body is supplied by this test.
    let response = 'capture: loop {
        assert!(
            Instant::now() < deadline,
            "native completion was not produced"
        );
        let incoming = writer.transport.drain_incoming_messages().unwrap();
        for message in incoming {
            if matches!(&message.message,
                DfsNetworkMessage::DistributedWriteResponse(response)
                if response.object_id == request.object_id
                && matches!(&response.result, DfsDistributedWriteResult::Outcome {
                    outcome: NetworkedDfsWriteOutcome::Committed { version, content_hash, .. }
                } if version == &request.target_version && content_hash == &manifest.content_hash))
            {
                break 'capture message;
            }
            writer
                .process_routed_messages_with_options(vec![message], false)
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(response.source_node_id, primary);
    assert_eq!(response.target_node_id, requester);
    let response_id = match &response.message {
        DfsNetworkMessage::DistributedWriteResponse(body) => body.request_id.clone(),
        _ => unreachable!(),
    };
    native_completed_write_observations(
        &runtime,
        &primary,
        &requester,
        &request,
        &response_id,
        deadline,
        || {},
    );
    // Exercise an exact replay of that real response through the ordinary
    // detached outbox, under the same negative advisory hint seen in Docker.
    // The original operation, body, authority and requester remain unchanged.
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    owner
        .transport
        .send_detached_routed_message(&requester, response)
        .unwrap();
    let mut worker = owner.prepare_message_deferred_work();
    worker.set_deferred_target_node_ids(BTreeSet::from([requester.clone()]));
    let ready = worker.scheduling_snapshot().unwrap().0;
    let before = runtime.rpc_observations().len();
    worker.drain_exclusive().unwrap();
    let while_excluded = runtime.rpc_observations()[before..].to_vec();
    // Drain even the old failing control normally before asserting its result.
    worker.set_deferred_target_node_ids(BTreeSet::new());
    worker.drain_exclusive().unwrap();
    let events = writer
        .process_incoming_until_pending_routed_writes_ready(
            deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap();
    let bytes = writer
        .content()
        .read_object_version(request.object_id.as_str(), request.target_version.as_str())
        .unwrap()
        .1;
    assert_eq!(crate::content_hash(&bytes), manifest.content_hash);
    assert_eq!(bytes.len() as u64, manifest.size_bytes);
    let omitted = runtime.observation_dropped_count();
    drop(worker);
    drop(owner);
    drop(writer);
    runtime.close().unwrap();
    assert_eq!(omitted, 0);
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { request_id, object_id, result:
            DfsDistributedWriteResult::Outcome { outcome: NetworkedDfsWriteOutcome::Committed { version, content_hash, .. } }, .. }
            if request_id == &response_id && object_id == &request.object_id
            && version == &request.target_version && content_hash == &manifest.content_hash)));
    assert!(
        ready,
        "required completion cannot be parked by advisory maintenance availability"
    );
    let decoded = while_excluded
        .iter()
        .find(|event| {
            event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Decoded
                && event.dfs_request_id.as_deref() == Some(&response_id)
                && event.dfs_object_id.as_ref() == Some(&request.object_id)
                && event.dfs_version.as_ref() == Some(&request.target_version)
        })
        .expect("the unchanged completion reaches the actual registered requester while excluded");
    assert!(
        while_excluded
            .iter()
            .any(|event| event.call_id == decoded.call_id
                && event.phase == crate::test_runtime::RpcPhase::Completed)
    );
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
    let completion_deadline = Instant::now() + Duration::from_secs(5);
    let events = writer
        .process_incoming_until_pending_routed_writes_ready(
            completion_deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap();
    assert!(events.iter().any(|event| matches!(event, NetworkedDfsServiceEvent::RemoteWriteCompleted {
        result: DfsDistributedWriteResult::Outcome { outcome: NetworkedDfsWriteOutcome::Committed { version, repair_jobs, .. } }, ..
    } if version == &request.target_version && repair_jobs.iter().any(|job| job.missing_replica_node_id == replica))));
    let completed_request = events
        .iter()
        .find_map(|event| match event {
            NetworkedDfsServiceEvent::RemoteWriteCompleted {
                request_id,
                object_id,
                result:
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { version, .. },
                    },
                ..
            } if object_id == &request.object_id && version == &request.target_version => {
                Some(request_id.clone())
            }
            _ => None,
        })
        .unwrap();
    let calls = native_completed_write_observations(
        &runtime,
        &primary,
        &requester,
        &request,
        &completed_request,
        completion_deadline,
        || {},
    );

    assert!(calls.iter().any(|event| event.source == primary
        && event.target == requester
        && event.route == DFS_DATA_FETCH_BLOB_ROUTE
        && event.phase == crate::test_support::DfsTestRpcPhase::Completed));
    let response_call_id = calls
        .iter()
        .find(|event| {
            event.phase == crate::test_runtime::RpcPhase::Decoded
                && event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.dfs_request_id.as_deref() == Some(&completed_request)
                && event.dfs_object_id.as_ref() == Some(&request.object_id)
                && event.dfs_version.as_ref() == Some(&request.target_version)
        })
        .unwrap()
        .call_id;
    let completion = calls
        .iter()
        .position(|event| {
            event.call_id == response_call_id
                && event.source == primary
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
    let mut restored = runtime.metadata_primary_service("/services").unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let job = restored
        .coordinator()
        .repair_jobs()
        .into_iter()
        .find(|job| {
            job.object_id == request.object_id
                && job.version == request.target_version
                && job.missing_replica_node_id == replica
        })
        .unwrap();
    restored
        .restore_pending_write_for_intent_if_missing(&job.intent_id, &job.object_id)
        .unwrap();
    assert!(
        !restored
            .writes
            .pending_writes
            .get(&job.intent_id)
            .unwrap()
            .foreground_replica_repair
    );
    let owned_shard = restored.local_prepared_operation_shard().unwrap();
    let body = restored
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&owned_shard)
        .unwrap()
        .unwrap()
        .tables
        .prepared_operations_v7
        .into_iter()
        .find(|record| {
            record.status == crate::DfsPreparedOperationStatus::Committed
                && record.terminal_intent.as_ref().is_some_and(|intent| {
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
    let object = runtime
        .node_metadata(&primary)
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
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
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
    // The primary's durable store; the delegating client keeps no store.
    let primary = config.primary_node_id.clone();
    let shard = DfsMetadataShardId::new(config.shard_id);
    let before = runtime
        .node_metadata(&primary)
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
    let after = runtime
        .node_metadata(&primary)
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
    let execution = native_fixture_write_admission(&runtime, &dfs, path);
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
            .into_iter()
            .find(|job| job.object_id == stat.object_id)
            .unwrap();
        intents.push(job.intent_id.clone());
    }
    drop(execution);
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
    }
    assert_eq!(recovered.coordinator().repair_jobs().len(), 3);
    drop(recovered);
    drop(background);
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
    assert!(metrics.object_count > before.object_count);
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
fn repair_command_conflicting_receipt_does_not_commit_visible_metadata() {
    let root = temp_root("repair-command-conflicting-receipt-no-visible-commit");
    let owner = NodeId::from_legacy_named("node_repair_cmd_owner").expect("node");
    let replica = NodeId::from_legacy_named("node_repair_cmd_replica").expect("node");
    let object_id = DfsObjectId::from_legacy_named("obj_repair_cmd_conflict").expect("object");
    let version_1 = DfsVersion::from_legacy_named("version_1").expect("version");
    let version_2 = DfsVersion::from_legacy_named("version_2").expect("version");
    let bytes = b"repair command should not commit after receipt conflict";
    let content_hash = content_hash(bytes);
    let (runtime, client) = native_sifs_service(
        "repair_command_conflicting_receipt_does_not_commit_visible_metadata",
        replica.clone(),
    );
    drop(client);
    // Metadata seeding and the receiver handler are Main-side work on the
    // node's store-backed owner; the delegating client keeps no store.
    let mut dfs = runtime.metadata_primary_service("/").unwrap();
    assert_eq!(dfs.node_id(), &replica);
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
        dfs.backend_receipt_slot_generation(&object_id, &version_2, &replica).unwrap(),
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
                receipt_generation: 0,
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
    // Durable rows are read from the nodes' stores; clients keep none.
    let mut untrusted_hint = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_object(&current.object_id)
        .unwrap()
        .unwrap();
    let unknown = DfsObjectId::generate();
    let version = DfsVersion::generate();
    untrusted_hint.object_id = unknown.clone();
    untrusted_hint.path = "/services/unknown-hinted-repair".into();
    untrusted_hint.version = version.clone();
    let receiver = runtime.client(&target).unwrap();
    let receiver_store = runtime.node_metadata(&target).unwrap();
    let before = receiver_store
        .export_shard_table_snapshot(&DfsMetadataShardId::new(config.shard_id.clone()))
        .unwrap()
        .unwrap();
    assert!(receiver_store.get_object(&unknown).unwrap().is_none());
    drop(receiver_store);
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
            receipt_generation: 0,
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
    let receiver_store = runtime.node_metadata(&target).unwrap();
    let after = receiver_store
        .export_shard_table_snapshot(&DfsMetadataShardId::new(config.shard_id))
        .unwrap()
        .unwrap();
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.semantic_state_fingerprint,
        before.semantic_state_fingerprint
    );
    assert!(receiver_store.get_object(&unknown).unwrap().is_none());
    drop(receiver_store);
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
    let (runtime, client) = native_sifs_service(
        "stage_command_conflicting_receipt_does_not_store_orphan_content",
        replica.clone(),
    );
    drop(client);
    // Metadata seeding and the receiver handler are Main-side work on the
    // node's store-backed owner; the delegating client keeps no store.
    let mut dfs = runtime.metadata_primary_service("/").unwrap();
    assert_eq!(dfs.node_id(), &replica);
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
        dfs.backend_receipt_slot_generation(&object_id, &version_2, &replica).unwrap(),
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
                receipt_generation: 0,
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

// Produce real sealed content and its missing replica before testing receiver
// semantics. The standalone predecessor authored arbitrary object rows without
// a sealed durability record, so it never reached replica ingest.
fn assert_native_repair_receiver(name: &str, bytes: &[u8], advance_metadata: bool) {
    let path = format!("/services/{name}");
    let (runtime, owner, nodes, mut source, policy) =
        native_owned_repair_fixture(name, &path, 1, 2, true);
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    let _background = runtime
        .deny_background_replica_before_claim(&owner)
        .unwrap();
    let execution = native_fixture_write_admission(&runtime, &source, &path);
    let stat = source
        .posix_write_file(
            &path,
            bytes,
            DfsWriteOptions {
                policy,
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(execution);
    let job = source
        .coordinator()
        .repair_jobs()
        .into_iter()
        .find(|job| job.object_id == stat.object_id && job.missing_replica_node_id == replica)
        .unwrap()
        .clone();
    let intent = source
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &job.intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    if advance_metadata {
        let changed = source.posix_set_mode(&path, 0o640).unwrap();
        assert_ne!(changed.version, stat.version);
        assert_ne!(Some(&changed.version), intent.base_version.as_ref());
        assert_eq!(changed.content_hash, stat.content_hash);
    }
    let metadata = source
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let visible_version = metadata.version.clone();
    let shard = source
        .command_authority_shard_for_object(Some(&metadata))
        .unwrap()
        .unwrap();
    let sync = source
        .capture_metadata_quorum_sync_target(&metadata)
        .unwrap();
    let certificate = source
        .sync_metadata_shard_to_quorum(&sync, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert_eq!(certificate.acknowledged_node_ids.len(), 2);
    let mut reader = runtime.client(&replica).unwrap();
    let content_hash = stat.content_hash.clone().unwrap();
    assert!(!reader.content().has_blob(&content_hash).unwrap());
    // The replica's Main receives the command over the normal DFS transport,
    // as in production, but has no return route to the owner. The accepted
    // bytes must survive the missing notification.
    runtime.install_fault(crate::test_support::DfsTestFaultRule {
        source: Some(replica.clone()),
        target: Some(owner.clone()),
        route: None,
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
    });
    let request_id = format!("native-receiver-{}", sinomo_core::Id::generate());
    let command = test_dfs_message_command_from_message(
        &owner,
        &replica,
        1,
        DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
            background_repair: false,
            request_id: request_id.clone(),
            intent_id: intent.intent_id.clone(),
            file_system_id: stat.file_system_id.clone(),
            object_id: stat.object_id.clone(),
            expected_base_version: intent.base_version,
            version: stat.version.clone(),
            content_hash: content_hash.clone(),
            bytes: bytes.to_vec(),
            object_metadata: Some(metadata),
            authority_shard: Some(shard),
            receipt_generation: 0,
        }),
    );
    let before = runtime.rpc_observations().len();
    let reply = runtime
        .rpc_request(
            &owner,
            &replica,
            dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let decoded = decode_dfs_message_rpc_response(&reply).unwrap();
    assert!(
        decoded.outbound_messages.iter().any(|message| matches!(&message.message,
            DfsNetworkMessage::RepairReplicaResponse(response)
                if response.request_id == request_id
                    && response.object_id == stat.object_id
                    && response.version == stat.version
                    && response.replica_node_id == replica
                    && matches!(&response.result,
                        DfsBlobTransferResult::Committed { content_hash: actual }
                        | DfsBlobTransferResult::Durable { content_hash: actual }
                            if actual == &content_hash))),
        "actual receiver repair: {decoded:?}"
    );
    assert!(
        !runtime.rpc_observations()[before..]
            .iter()
            .any(|call| call.source == replica
                && call.target == owner
                && matches!(
                    call.phase,
                    crate::test_runtime::RpcPhase::AfterDispatch
                        | crate::test_runtime::RpcPhase::Decoded
                        | crate::test_runtime::RpcPhase::Completed
                )),
        "the receiver cannot deliver its notification over the absent return route"
    );
    // The replica's durable state: its current metadata row is the visible
    // version, whose content its own store now holds.
    let current = runtime
        .node_metadata(&replica)
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.version, visible_version);
    assert_eq!(current.content_hash.as_deref(), Some(content_hash.as_str()));
    assert_eq!(reader.content().read_blob(&content_hash).unwrap(), bytes);
    assert_eq!(
        reader
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str(),)
            .unwrap()
            .1,
        bytes
    );
    // Callers on the replica read the repaired bytes at the current metadata
    // version once its route to the metadata primary returns.
    runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&replica)
            && rule.target.as_ref() == Some(&owner)
            && rule.route.is_none()
    });
    assert_eq!(
        reader.posix_stat_distributed(&path).unwrap().version,
        visible_version,
        "the actual current metadata version must read the repaired bytes"
    );
    assert_eq!(reader.posix_read_file_distributed(&path).unwrap(), bytes);
    drop(reader);
    drop(source);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_replica_commits_when_response_target_is_unroutable() {
    assert_native_repair_receiver(
        "native-repair-unroutable-response",
        b"repair committed despite lost requester",
        false,
    );
}

#[test]
fn repair_replica_accepts_stale_base_when_content_hash_matches_current() {
    assert_native_repair_receiver(
        "native-repair-stale-base-same-content",
        b"content unchanged across a metadata-only version bump",
        true,
    );
}

#[test]
fn unmanaged_manifest_reconnect_requires_native_receipt_publication() {
    let bytes = b"ordinary reconnect bytes";
    // The explicit publisher below must own the first publication after withdrawal.
    let (runtime, mut dfs, stat, removed, executor_hold) =
        native_withdrawn_local_replica_fixture_with_hold(
            "native-unmanaged-manifest-reconnect", bytes, false, true,
        );
    assert!(dfs.control_plane.is_some());
    // A reconnect starts with a real local manifest for these already verified
    // bytes. Recording local content is not a clustered receipt publication.
    let manifest = dfs
        .content()
        .put_object_version(stat.object_id.as_str(), stat.version.as_str(), bytes)
        .unwrap();
    assert_eq!(Some(manifest.content_hash), stat.content_hash);

    assert!(
        dfs.backend_receipt_for_replica(&stat.object_id, &stat.version, &removed.replica_node_id)
            .unwrap()
            .is_none()
    );
    let before = dfs
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    let reconciliation = dfs.reconcile_local_manifests(&stat.object_id).unwrap();
    assert_eq!(
        reconciliation.refreshed_receipts, 0,
        "local manifest enumeration cannot grant clustered receipt authority without a current publication"
    );
    assert!(!reconciliation.missing_current);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        dfs.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );

    // The actual current owner can republish its verified local copy while the
    // other real replica still supplies the recorded W1 durability witness.
    assert!(
        dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &removed.replica_node_id,
            stat.content_hash.as_deref().unwrap(),
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &removed.replica_node_id,
            )
            .unwrap(),
        )
        .unwrap()
    );
    let receipt = dfs
        .backend_receipt_for_replica(&stat.object_id, &stat.version, &removed.replica_node_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            &receipt.receipt.file_system_id,
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
            &receipt.receipt.owner_node_id,
            &receipt.replica_node_id,
            &receipt.content_hash
        ),
        (
            &removed.receipt.file_system_id,
            &removed.receipt.object_id,
            &removed.receipt.committed_version,
            &removed.receipt.owner_node_id,
            &removed.replica_node_id,
            &removed.content_hash
        )
    );
    assert_ne!(
        receipt.receipt.event_id, removed.receipt.event_id,
        "a new native publication carries its own actual event"
    );
    assert!(receipt.received_at >= removed.received_at);
    let current = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    assert_eq!(current.version, stat.version);
    assert_eq!(current.content_hash, stat.content_hash);
    assert!(current.current_locations.contains(&receipt.replica_node_id));
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap()
            .len(),
        2
    );
    assert!(
        !dfs.put_metadata_receipt(
            &stat.file_system_id,
            &stat.object_id,
            &stat.version,
            &receipt.replica_node_id,
            stat.content_hash.as_deref().unwrap(),
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
        )
        .unwrap(),
        "the same actual receipt is published once"
    );
    assert_eq!(
        dfs.backend_receipt_for_replica(&stat.object_id, &stat.version, &receipt.replica_node_id)
            .unwrap()
            .as_ref(),
        Some(&receipt),
        "an exact duplicate does not mint another event or timestamp"
    );
    drop(dfs);
    drop(executor_hold);
    runtime.close().unwrap();
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
    let (runtime, client) = native_sifs_service(
        "commit_version_conflicting_receipt_does_not_advance_visible_metadata",
        replica.clone(),
    );
    drop(client);
    // Metadata seeding and the receiver handler are Main-side work on the
    // node's store-backed owner; the delegating client keeps no store.
    let mut dfs = runtime.metadata_primary_service("/").unwrap();
    assert_eq!(dfs.node_id(), &replica);
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
        dfs.backend_receipt_slot_generation(&object_id, &version_2, &replica).unwrap(),
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
    let bytes = b"commit should publish missing local receipt";
    // The shard primary's executor would restore the withdrawn receipt from
    // its still-present bytes once the withdrawal is published; hold it so
    // this commit is the one that restores it.
    let (runtime, mut dfs, stat, removed, executor_hold) = native_withdrawn_local_replica_fixture_held(
        "native-commit-repairs-missing-receipt",
        bytes,
    );
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    assert_eq!(removed.replica_node_id, shard.primary_node_id);
    let before = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(before.len(), 1);
    assert_ne!(before[0].replica_node_id, removed.replica_node_id);
    let intent = dfs
        .metadata_backend()
        .unwrap()
        .intents_for_object(&stat.object_id)
        .unwrap()
        .into_iter()
        .find(|intent| {
            intent.target_version == stat.version
                && intent.status == DfsWriteIntentStatus::Committed
                && intent.publication.is_current()
        })
        .unwrap();
    assert_eq!(intent.coordinator_node_id, shard.primary_node_id);
    let metadata = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let manifest = dfs
        .content()
        .put_object_version(stat.object_id.as_str(), stat.version.as_str(), bytes)
        .unwrap();
    assert_eq!(Some(manifest.content_hash.clone()), stat.content_hash);
    dfs.begin_message_committed_work();
    let event = dfs
        .handle_commit_version(
            intent.coordinator_node_id.clone(),
            DfsCommitVersionCommand {
                request_id: format!("native-commit-replay-{}", sinomo_core::Id::generate()),
                intent_id: intent.intent_id.clone(),
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                expected_base_version: intent.base_version,
                version: stat.version.clone(),
                content_hash: manifest.content_hash.clone(),
                object_metadata: Some(metadata),
                authority_shard: Some(shard),
            },
        )
        .unwrap();
    assert!(
        matches!(&event, NetworkedDfsServiceEvent::VersionCommitted {
        object_id, version, content_hash, ..
    } if object_id == &stat.object_id && version == &stat.version
        && content_hash == &manifest.content_hash),
        "actual commit event: {event:?}"
    );
    assert!(
        dfs.take_message_committed_work().publish_metadata,
        "the actual authoritative receipt mutation must schedule its publication"
    );
    let current = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    assert_eq!(current.version, stat.version);
    assert_eq!(current.content_hash, stat.content_hash);
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap();
    assert_eq!(
        receipts.len(),
        before.len() + 1,
        "exactly one missing local receipt is restored"
    );
    assert!(
        receipts.contains(&before[0]),
        "the real surviving W witness is unchanged"
    );
    let local = receipts
        .iter()
        .find(|receipt| receipt.replica_node_id == removed.replica_node_id)
        .unwrap();
    assert_eq!(local.content_hash, stat.content_hash);
    assert_eq!(
        dfs.content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .unwrap()
            .1,
        bytes
    );
    drop(dfs);
    drop(executor_hold);
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
    let (runtime, author, stat, receipt, _) =
        native_withdrawn_local_replica_fixture_with_hold(name, bytes, true, false);
    (runtime, author, stat, receipt)
}

/// The same fixture with the shard primary's repair executor held from before
/// the withdrawal: the withdrawn location stays withdrawn until the returned
/// hold drops, instead of racing the executor that restores its receipt.
pub(super) fn native_withdrawn_local_replica_fixture_held(
    name: &str,
    bytes: &[u8],
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    DfsReplicaReceiptRecord,
    crate::repair_executor::DfsRepairExecutorTestHold,
) {
    let (runtime, author, stat, receipt, hold) =
        native_withdrawn_local_replica_fixture_with_hold(name, bytes, true, true);
    (runtime, author, stat, receipt, hold.expect("requested executor hold"))
}

fn native_withdrawn_local_replica_fixture_with_hold(
    name: &str,
    bytes: &[u8],
    managed: bool,
    hold_executor: bool,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    DfsReplicaReceiptRecord,
    Option<crate::repair_executor::DfsRepairExecutorTestHold>,
) {
    let path = "/services/receipt-republication";
    let (runtime, owner, nodes, mut author, policy) = if managed {
        native_owned_repair_fixture(name, path, 1, 2, false)
    } else {
        let (runtime, browser, config) =
            crate::test_support::current_native_metadata_service(name, 2, path).unwrap();
        drop(browser);
        let owner = config.primary_node_id;
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let mut author = runtime.metadata_primary_service(path).unwrap();
        let (resolved, _, policy) = author
            .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
            .unwrap();
        assert!(
            resolved.source_path.is_none(),
            "unmanaged native path uses its actual default policy"
        );
        assert_eq!(
            (policy.min_commit_replicas, policy.desired_replicas),
            (1, 2)
        );
        (runtime, owner, nodes, author, policy)
    };
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
    // W1 visibility can precede the ordinary N2 continuation. The owner's
    // repair executor drains it; explicit maintenance ticks drive that pass
    // before the second node is treated as a durable fixture witness.
    drop(author);
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
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
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
    let hold = hold_executor.then(|| {
        crate::repair_executor::hold_repair_executor_for_test(
            &runtime.data_dir(author.node_id()).root(),
        )
    });
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
    (runtime, author, stat, receipt, hold)
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
            hash,
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
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
            hash,
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
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
            hash,
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
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
            dfs.backend_receipt_slot_generation(
                &stat.object_id,
                &stat.version,
                &receipt.replica_node_id,
            )
            .unwrap(),
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
    std::thread::scope(|scope| {
        let primary_stop = Arc::clone(&stop_primary);
        let primary_thread = scope.spawn(move || {
            let mut primary = primary;
            while !primary_stop.load(Ordering::SeqCst) {
                primary.process_incoming().map_err(|error| error.to_string())?;
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok::<_, String>(primary)
        });
        // Drop before scope joins on a caller panic; preserve its original
        // error while stopping the existing pump, rather than detaching it.
        struct StopPrimary(Arc<AtomicBool>);
        impl Drop for StopPrimary {
            fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
        }
        let _stop_on_exit = StopPrimary(Arc::clone(&stop_primary));

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
            let started = Instant::now();
            let resumed = writer.posix_resume_namespace_job_bounded_routed(&job.job_id, from, 1);
            let original_completed_after = started.elapsed();
            if resumed.is_err() {
                // Error-only readout AFTER the original call; no retry, mutation,
                // manufactured row or change to the original receipt/error oracle.
                let raw = _runtime.rpc_observations();
                let observed = (|| -> Result<serde_json::Value, String> {
                    let shards = _runtime.metadata_shards()?;
                    let shard = crate::select_metadata_shard_config_for_path(
                        &_runtime.file_system().file_system_id, &shards, to,
                    ).map_err(|error| error.to_string())?
                        .ok_or("actual rename target has no current metadata assignment")?;
                    let current = _runtime.metadata_primary_service(to)?;
                    let backend = current.metadata_backend()
                        .ok_or("actual current Core has no native metadata backend")?;
                    let snapshot = backend.export_shard_table_snapshot(
                        &DfsMetadataShardId::new(&shard.shard_id),
                    ).map_err(|error| error.to_string())?;
                    serde_json::to_value(serde_json::json!({
                        "actual_current_assignment":shard,
                        "actual_current_core_node":current.node_id(),
                        "actual_native_table_snapshot":snapshot,
                    })).map_err(|error| error.to_string())
                })();
                let evidence = serde_json::json!({
                    "qualification":"error-only actual native after-cut; same routed max_steps1 call and unchanged failure oracle; snapshot does not itself prove the earlier interleaving",
                    "from":from,"to":to,"requested_job_before_call":job,
                    "original_result_debug":format!("{resumed:?}"),
                    "original_completion_elapsed_nanos":original_completed_after.as_nanos(),
                    "actual_native_after_cut":observed,
                    "actual_rpc_window_debug":format!("{raw:#?}"),
                    "actual_rpc_omitted":_runtime.observation_dropped_count(),
                });
                let output = root.join("bounded-directory-rename-error.json");
                let exported = serde_json::to_vec_pretty(&evidence)
                    .map_err(|error| error.to_string())
                    .and_then(|bytes| std::fs::write(&output, bytes).map_err(|error| error.to_string()));
                if let Err(error) = exported {
                    eprintln!("rename diagnostic export {} failed: {error}", output.display());
                }
            }
            job = resumed.expect("next routed bounded batch");
        }
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(matches!(
            writer.posix_stat_distributed(from),
            Err(DfsPosixError::NotFound { .. })
        ));
        for index in 0..4 {
            assert_eq!(
                writer
                    .posix_read_file_distributed(&format!("{to}/nested/{index}.txt"))
                    .expect("renamed file"),
                index.to_string().as_bytes()
            );
        }

        stop_primary.store(true, Ordering::SeqCst);
        let _primary = primary_thread.join().expect("primary thread").expect("primary pump");
    });
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
    // A delegating replica keeps no store; its stale chain is the in-memory
    // namespace projection that routed mkdir_all must not trust.
    assert!(
        replica
            .metadata()
            .object_by_normalized_posix_path(&sifs_file_system_id(), base)
            .is_some_and(|object| object.object_id == stale_id
                && object.kind == DfsObjectKind::Directory),
        "replica precondition should have local parent"
    );
    let mut services_primary = services_primary;
    assert!(
        matches!(
            services_primary.posix_stat_distributed(base),
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
    let mut services_primary = services_thread.join().expect("services thread");
    assert!(services_primary.posix_stat_distributed(base).is_ok());
    assert!(services_primary.posix_stat_distributed(path).is_ok());
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
    native_missing_content_repair_with_nodes(name, 2)
}

fn native_missing_content_repair_with_nodes(
    name: &str,
    node_count: usize,
) -> (
    crate::test_support::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
) {
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, node_count, "/services")
            .unwrap();
    let owner = config.primary_node_id;
    drop(service);
    let peer = crate::test_support::DfsTestRuntime::ordered_node_ids(name, node_count)
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
    native_committed_repair_backlog_with_nodes(name, intercept_background, 2)
}

pub(super) fn native_committed_repair_backlog_with_nodes(
    name: &str,
    intercept_background: bool,
    node_count: usize,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<DfsNodeTransport>,
    DfsStat,
    String,
    Option<impl Send + use<>>,
) {
    let (runtime, owner, replica, service) =
        native_missing_content_repair_with_nodes(name, node_count);
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
    // Drive the existing root coordinator until this exact accepted job completes;
    // a bounded maintenance invocation need not run a placement page.
    let placement_owner = runtime.root_shard().unwrap().primary_node_id;
    loop {
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
        let tick = runtime
            .maintenance_tick(&placement_owner, remaining)
            .unwrap();
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
    // Main performed the write below N. Its shard tables' repair index holds
    // the missing target, derived from the committed owner intent.
    let service = runtime.metadata_primary_service(&path).unwrap();
    let needs = indexed_repair_needs(&runtime, &service, &stat);
    let intent_id = needs
        .iter()
        .find(|(target, _)| *target == replica)
        .and_then(|(_, owner)| owner.clone())
        .expect("the index derives the canonical missing target from its owner intent");
    assert_eq!(needs.len(), 1, "only the missing target is a need: {needs:?}");
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
        .expect("the index's owner intent is the actual committed W-to-N record");
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

/// The missing targets the shard tables' repair index holds for this exact
/// committed version, each with the owner intent it was derived from.
pub(super) fn indexed_repair_needs(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &NetworkedDfsService<DfsNodeTransport>,
    stat: &DfsStat,
) -> Vec<(NodeId, Option<String>)> {
    indexed_object_repair_needs(
        runtime,
        service,
        &stat.file_system_id,
        &stat.path,
        &stat.object_id,
        &stat.version,
    )
}

/// The same for one object version named by its row.
pub(super) fn indexed_object_repair_needs(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &NetworkedDfsService<DfsNodeTransport>,
    file_system_id: &DfsFileSystemId,
    path: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Vec<(NodeId, Option<String>)> {
    let shards = runtime.metadata_shards().unwrap();
    let shard = crate::select_metadata_shard_config_for_path(file_system_id, &shards, path)
        .unwrap()
        .expect("the version's authority shard");
    service
        .metadata_backend()
        .unwrap()
        .repair_index_objects(
            &DfsMetadataShardId::new(shard.shard_id),
            &[object_id.clone()].into_iter().collect(),
        )
        .unwrap()
        .needs
        .into_iter()
        .filter(|((_, need_version, _), _)| need_version == version)
        .map(|((_, _, target), need)| (target, need.owner_intent_id))
        .collect()
}

/// Shared normal Main W2/N3 producer. B's genuine precommit Stage completes
/// at its actual receiver, but its response is lost before Main can consume
/// it; the write commits at W2 and leaves B as repair work.
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
    crate::DfsRepairExecutorTestHold,
) {
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
    // The node's repair executor sends nothing to B meanwhile.
    let background_guard = runtime.hold_repair_executor(&owner).unwrap();
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
    drop(writer);
    let service = runtime.metadata_primary_service(&path).unwrap();
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
    // The shard tables' repair index holds B, derived from the owner intent.
    assert_eq!(
        indexed_repair_needs(&runtime, &service, &stat),
        vec![(replica_b.clone(), Some(intent_id.clone()))]
    );
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
fn repair_executor_is_idle_noop_without_repair_needs() {
    let (runtime, client, _) = crate::test_support::current_native_metadata_service(
        "native-idle-repair-noop",
        1,
        "/services",
    )
    .unwrap();
    drop(client);
    // The node's repair executor runs on the store-backed owner.
    let mut dfs = runtime.metadata_primary_service("/services").unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let data_root = runtime.data_dir(dfs.node_id()).root();
    let pass = dfs
        .run_repair_executor_pass(
            &data_root,
            &runtime.metadata_shards().unwrap(),
            &crate::DfsMaintenanceExecutionExclusions::default(),
            8,
        )
        .unwrap();
    assert_eq!((pass.scheduled.len(), pass.tracked, pass.has_more), (0, 0, false));
    let work = crate::repair_executor::repair_executor_work(&data_root);
    assert_eq!(
        (work.need_count, work.next_due_unix_nanos),
        (0, None),
        "an idle executor holds no timer"
    );
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

#[test]
fn repair_keeps_same_content_need_after_metadata_only_advance() {
    let (runtime, owner, peer, mut dfs) =
        native_missing_content_repair("native-metadata-successor-repair-plan");
    // No attempt meets the denied peer while the fixture's faults stand.
    let hold = runtime.hold_repair_executor(&owner).unwrap();
    let path = "/services/native-same-content";
    let execution = native_fixture_write_admission(&runtime, &dfs, path);
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
    drop(execution);
    assert_eq!(indexed_repair_needs(&runtime, &dfs, &first).len(), 1);
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
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    drop(hold);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let repaired = || {
        runtime.rpc_observations()[before..].iter().any(|event| {
            event.source == owner
                && event.target == peer
                && event.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed
        })
    };
    while !repaired() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "the recorded same-byte obligation must progress through an actual receiver"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
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
fn repair_suppresses_stale_content_target_after_object_advances() {
    let (runtime, owner, peer, mut dfs) =
        native_missing_content_repair("native-stale-content-repair-retirement");
    let hold = runtime.hold_repair_executor(&owner).unwrap();
    let path = "/services/native-stale-content";
    let execution = native_fixture_write_admission(&runtime, &dfs, path);
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
    drop(execution);
    assert_eq!(indexed_repair_needs(&runtime, &dfs, &first).len(), 1);
    let replacement = dfs
        .posix_write_file(
            path,
            b"new visible content",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    assert_ne!(replacement.version, first.version);
    assert_ne!(replacement.content_hash, first.content_hash);
    assert!(
        indexed_repair_needs(&runtime, &dfs, &first).is_empty(),
        "the replaced version's need leaves the index"
    );
    runtime.clear_faults();
    let before = runtime.rpc_observations().len();
    drop(hold);
    runtime
        .maintenance_tick(&owner, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap()
        .outcome
        .unwrap();
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
        seed.current_read_object_metadata_distributed(&base.object_id)
            .unwrap()
            .policy,
        DfsObjectPolicy::ReplicatedEventual
    );
    drop(seed);
    // Repair source selection is Main-side work on the store-backed owner;
    // a delegating client keeps neither a store nor a hydrated projection.
    let mut stale = runtime.metadata_primary_service(&path).unwrap();
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
    // The test drives the primary's executor on the stale lane; routine
    // maintenance stays out of the way.
    let (execution, hold) = native_private_repair_owner(&runtime, &stale, &path, ());
    // The routed write below needs the primary's own admission.
    drop(execution);
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
        "this is an actual retained old owner projection across a normal new publication"
    );
    assert!(
        indexed_object_repair_needs(
            &runtime,
            &stale,
            &request.file_system_id,
            &path,
            &request.object_id,
            &request.target_version,
        )
        .iter()
        .any(|(target, owner)| *target == missing && owner.is_some()),
        "the shard tables hold the exact current missing-target repair"
    );
    // The primary holds no copy: the executor first fetches one, from the
    // tables' current holders, never from the lane's stale projection.
    drop(writer);
    let before = runtime.rpc_observations().len();
    let pass = stale
        .run_repair_executor_pass(
            &runtime.data_dir(&primary).root(),
            &runtime.metadata_shards().unwrap(),
            &crate::DfsMaintenanceExecutionExclusions::default(),
            1,
        )
        .unwrap();
    assert_eq!(pass.scheduled.len(), 1);
    let fetch_targets = runtime.rpc_observations()[before..]
        .iter()
        .filter(|event| {
            event.source == primary
                && event.route == crate::DFS_DATA_FETCH_BLOB_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::BeforeDispatch
        })
        .map(|event| event.target.clone())
        .collect::<Vec<_>>();
    assert_eq!(fetch_targets, vec![requester]);
    drop(stale);
    drop(hold);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn repair_command_accepts_current_replica_after_metadata_primary_moves() {
    let (runtime, source, target, mut writer) =
        native_missing_content_repair("native-repair-current-replica-after-primary-move");
    let bytes = vec![0x72; 2 * 1024 * 1024 + 4096];
    let execution = native_fixture_write_admission(
        &runtime,
        &writer,
        "/services/native-repair-primary-move.bin",
    );
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
    drop(execution);
    let content_hash = stat
        .content_hash
        .clone()
        .expect("actual native current content hash");
    let (_, intent_id) = indexed_repair_needs(&runtime, &writer, &stat)
        .into_iter()
        .find(|(missing, _)| *missing == target)
        .expect("the committed write leaves the target as repair work");
    let intent = writer
        .metadata_backend()
        .unwrap()
        .get_intent(&stat.object_id, &intent_id.unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    // The attachment producer reads a live manifest. Materialize that exact
    // already-committed local identity from the source's real stored blob.
    let actual_bytes = writer.content().read_blob(&content_hash).unwrap();
    assert_eq!(actual_bytes, bytes);
    let manifest = writer
        .content()
        .put_object_version(
            stat.object_id.as_str(),
            stat.version.as_str(),
            &actual_bytes,
        )
        .unwrap();
    assert_eq!(manifest.content_hash, content_hash);

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
    // The new primary's durable store; the delegating client keeps no store.
    let current = runtime
        .node_metadata(&target)
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
    let first = send_repair_replica_for_test(
        &mut replica,
        &targets,
        &intent,
        &stat,
        &content_hash,
        bytes.len() as u64,
        current.clone(),
    );
    assert!(
        !first,
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
    let replay = send_repair_replica_for_test(
        &mut replica,
        &targets,
        &intent,
        &stat,
        &content_hash,
        bytes.len() as u64,
        current,
    );
    assert!(replay);
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
    let store = runtime.node_metadata(&target).unwrap();
    let current = store.get_object(&stat.object_id).unwrap().unwrap();
    assert_eq!(current.version, stat.version);
    assert!(current.current_locations.contains(&target));
    assert!(
        store
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

/// Send one repair replica from `service` exactly as the repair executor
/// does, outside any executor, and report whether the call completed.
pub(super) fn send_repair_replica_for_test(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    targets: &[(NodeId, String)],
    intent: &crate::DfsWriteIntentRecord,
    stat: &DfsStat,
    content_hash: &str,
    size_bytes: u64,
    current: crate::DfsObjectMetadata,
) -> bool {
    let requests = service
        .prepare_repair_replica_sends(
            targets,
            &intent.intent_id,
            &stat.file_system_id,
            &stat.object_id,
            intent.base_version.clone(),
            &stat.version,
            content_hash,
            size_bytes,
            current,
            false,
        )
        .unwrap()
        .into_requests();
    service
        .transport
        .send_routed_calls_bounded(requests, crate::service_outbound::DFS_REPLICA_BODY_FANOUT_CAP)
        .unwrap()
        .iter()
        .all(|outcome| outcome.result.is_ok())
}

#[test]
fn committed_intent_repairs_already_current_stale_locations() {
    let (runtime, owner, replica, mut service, stat, intent_id, _background_interception) =
        native_committed_repair_backlog("native-already-current-late-repair-locations", true);
    let (execution, background_interception) =
        native_private_repair_owner(&runtime, &service, &stat.path, _background_interception);
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
    let pass = service
        .run_repair_executor_pass(
            &runtime.data_dir(&owner).root(),
            &runtime.metadata_shards().unwrap(),
            &crate::DfsMaintenanceExecutionExclusions::default(),
            1,
        )
        .unwrap();
    assert_eq!(pass.scheduled.len(), 1);
    service
        .transport
        .checkpoint_background_replica_dispatch()
        .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while !runtime.rpc_observations()[observed_before..].iter().any(|event| {
        event.source == owner
            && event.target == replica
            && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
            && event.phase == crate::test_runtime::RpcPhase::Completed
    }) {
        assert!(Instant::now() < deadline, "the receiver's actual ingest");
        std::thread::sleep(Duration::from_millis(5));
    }
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
    let mut events = Vec::new();
    while !events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::ReplicaRepaired { intent_id: repaired, replica_node_id, .. }
            if repaired == &intent_id && replica_node_id == &replica))
    {
        assert!(Instant::now() < deadline, "the receiver's reply: {events:?}");
        events.extend(service.process_incoming().unwrap());
        std::thread::sleep(Duration::from_millis(5));
    }
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
    // A lane that holds the write's state replays its exact application.
    service.hydrate_from_metadata_backend().unwrap();
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
    drop(execution);
    drop(service);
    drop(background_interception);
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
    // No repair attempt meets the denied replicas while the faults stand.
    let hold = runtime.hold_repair_executor(&owner).unwrap();
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
    assert_eq!(
        indexed_object_repair_needs(
            &runtime,
            &dfs,
            &request.file_system_id,
            &path,
            &request.object_id,
            &request.target_version,
        ),
        vec![(replica_b.clone(), Some(intent.clone()))],
        "the missing desired replica remains repair work in the shard tables"
    );
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
    drop(hold);
    // The executor first refills the owner's lost copy, then sends it.
    let deadline = Instant::now() + 2 * sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while !dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&request.object_id, &request.target_version)
        .unwrap()
        .iter()
        .any(|receipt| receipt.replica_node_id == replica_b)
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "the missing replica is repaired");
        runtime
            .maintenance_tick(
                &owner,
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .unwrap()
            .outcome
            .unwrap();
    }
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
fn foreground_w2_does_not_repair_a_sibling_stage_already_inflight() {
    let name = "native-foreground-w2-held-sibling";
    let path = format!("/services/{name}.bin");
    let (runtime, owner, nodes, mut dfs, policy) =
        native_owned_repair_fixture(name, &path, 2, 3, false);
    let peers = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let replica_a = peers[0].clone();
    let replica_b = peers[1].clone();
    let base = dfs
        .posix_write_file(
            &path,
            b"actual stage predecessor",
            DfsWriteOptions {
                policy: policy.clone(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let base_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&base.object_id, &base.version)
        .unwrap()
        .len()
        != 3
    {
        assert!(
            Instant::now() < base_deadline,
            "actual predecessor N3 must settle before selected Stage pause"
        );
        drive_repair_executor(&runtime, &mut dfs);
    }
    dfs.flush_detached_transport_sends().unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: owner.clone(),
    };
    let bytes = vec![0x6b; 2 * 1024 * 1024 + 4096];
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
    // The executor's own pass finds no need left to send.
    drive_repair_executor(&runtime, &mut dfs);
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

fn assert_native_manifest_repair_pending(
    runtime: &crate::test_runtime::DfsTestRuntime,
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    request: &DfsWriteRequest,
    missing: &NodeId,
) -> DfsWriteIntentRecord {
    let path = service
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .expect("the committed object")
        .path;
    let intent_id = indexed_object_repair_needs(
        runtime,
        service,
        &request.file_system_id,
        &path,
        &request.object_id,
        &request.target_version,
    )
    .into_iter()
    .find(|(target, _)| target == missing)
    .and_then(|(_, owner)| owner)
    .expect("genuine unfinished repair obligation in the shard tables");
    let backend = service.metadata_backend().unwrap();
    let record = backend
        .get_intent(&request.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    assert!(record.desired_replica_node_ids.contains(missing));
    assert_eq!(record.target_version, request.target_version);
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
    // Repair hydration is Main-side work on the primary's store-backed
    // owner; the delegating client keeps no store.
    let mut inspect = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(inspect.node_id(), &primary);
    let native_intent =
        assert_native_manifest_repair_pending(&runtime, &mut inspect, &request, &replica);
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(
        assert_native_manifest_repair_pending(&runtime, &mut restored, &request, &replica),
        native_intent
    );
    drop(restored);
    runtime.clear_faults();
    // The restarted primary's executor reads its index afresh: it first
    // fetches its own missing copy from the durable requester, then repairs
    // the target from it.
    let deadline = Instant::now() + 2 * sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let repaired = || {
        runtime
            .node_metadata(&primary)
            .unwrap()
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .any(|receipt| receipt.replica_node_id == replica)
    };
    while !repaired() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "the target is repaired");
        runtime
            .maintenance_tick(
                &primary,
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .unwrap()
            .outcome
            .unwrap();
    }
    let final_primary = runtime.client(&primary).unwrap();
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
        service.retry_pending_stages(1).unwrap();
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
fn posix_wait_for_commit_drives_only_its_own_write() {
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
    // The lane's unrelated broken write is not retried by this wait; its
    // own stages retry in maintenance. The wait drives only its own write.
    assert!(
        !service.last_events().iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RepairRetryFailed { .. })),
        "write settle drives no other write's retries"
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
    let captured_before_claim = Arc::clone(&captured);
    let retry_hold = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&primary),
        Arc::new(move |point, _, _| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim { .. }
            ) && !captured_before_claim.lock().unwrap().is_empty()
            {
                return Err(DfsError::Cancelled {
                    operation: "negative retry claim after original lost Stage",
                });
            }
            Ok(())
        }),
    );
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
    // Hydration of pending writes is Main-side work on the primary's
    // store-backed owner; the delegating client keeps no store.
    let mut authority = runtime.metadata_primary_service(&base.path).unwrap();
    assert_eq!(authority.node_id(), &primary);
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
    let mut restored = runtime.metadata_primary_service(&base.path).unwrap();
    restored.hydrate_from_metadata_backend().unwrap();
    let retained = restored.writes.pending_writes.get(&intent_id).unwrap();
    assert!(retained.stage_retries.is_empty());
    assert!(retained.stage_inflight_request_ids.is_empty());
    assert!(
        restored
            .ensure_stage_response_matches_pending(ack, &content_hash(&bytes))
            .is_err(),
        "the old ACK has no current same-kind request after restart"
    );
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
    let fresh_receiver = runtime.capture_served_work(&replica, 32).unwrap();
    let before = runtime.rpc_observations().len();
    assert_eq!(
        runtime.remove_faults_matching(|rule| rule.source.as_ref() == Some(&primary)
            && rule.target.as_ref() == Some(&replica)
            && rule.route.as_deref().is_some_and(|route| [
                DFS_DATA_STAGE_BLOB_ROUTE,
                DFS_DATA_REPAIR_REPLICA_ROUTE
            ]
            .contains(&route))),
        2
    );
    // Recovery must produce a new real Stage; the expired ACK is never used
    // as publication authority. Establish the blocked native completion before
    // measuring the independent receipt RPC's original one-second budget.
    let recovery_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    drop(retry_hold);
    runtime.request_repair_reconciliation(&primary).unwrap();
    let completion_ready = loop {
        if runtime.rpc_observations()[before..].iter().any(|event| {
            event.source == primary
                && event.target == requester
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
        }) {
            break true;
        }
        if Instant::now() >= recovery_deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    if !completion_ready {
        gate.add_permits(1);
    }
    assert!(
        completion_ready,
        "normal fresh Stage recovery must reach its real completion; calls={:?}; receiver={:?}",
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == primary
                && [
                    DFS_DATA_STAGE_BLOB_ROUTE,
                    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                ]
                .contains(&event.route.as_str()))
            .collect::<Vec<_>>(),
        fresh_receiver.records()
    );
    let fresh_stages = fresh_receiver
        .records()
        .into_iter()
        .filter_map(|record| {
            if let DfsNetworkMessage::StageBlob(stage) = record.command.routed_message.message {
                Some((
                    stage.request_id,
                    stage.intent_id,
                    stage.object_id,
                    stage.version,
                    stage.content_hash,
                    record.command.routed_message.source_node_id,
                    record.command.routed_message.target_node_id,
                    record.succeeded,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert!(
        fresh_stages.iter().any(
            |(request_id, stage_intent, object, version, hash, source, target, succeeded)| {
                request_id != &receipt_rpc_id
                    && stage_intent == &intent_id
                    && object == &request.object_id
                    && version == &request.target_version
                    && hash == &content_hash(&bytes)
                    && source == &primary
                    && target == &replica
                    && *succeeded
            }
        ),
        "actual fresh Stage receiver reports: {fresh_stages:?}; carrier observations: {:?}",
        runtime.rpc_observations()[before..]
            .iter()
            .filter(|event| event.source == primary && event.target == replica)
            .collect::<Vec<_>>()
    );
    // This real expired receipt remains a negative control, while its handler
    // must still be responsive during unrelated completion-delivery I/O.
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
    let authority = runtime.node_metadata(&primary).unwrap();
    assert_eq!(
        authority
            .get_object(&request.object_id)
            .unwrap()
            .unwrap()
            .version,
        request.target_version
    );
    let receipts = authority
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
    let NetworkedDfsWriteOutcome::Pending {
        intent_id: ref route_id,
        ..
    } = outcome
    else {
        panic!("actual primary Fetch denial must leave W2 pending: {outcome:?}");
    };
    // Consume the real primary's Pending response before inspecting its
    // canonical intent; the caller's route identity is not a metadata key.
    writer.process_incoming().unwrap();
    let pending = writer.write_outcome(route_id).unwrap();
    let NetworkedDfsWriteOutcome::Pending {
        ref intent_id,
        durable_receipts: 1,
        required_receipts: 2,
    } = pending
    else {
        panic!("actual canonical W2 progress: {pending:?}");
    };
    let inspect = runtime.client(&primary).unwrap();
    // The primary's durable store; the delegating client keeps no store.
    let store = runtime.node_metadata(&primary).unwrap();
    let native_intent = store
        .get_intent(&request.object_id, intent_id)
        .unwrap()
        .unwrap();
    assert_ne!(native_intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(
        native_intent.operation_id.as_deref(),
        Some(route_id.as_str())
    );
    assert!(
        inspect
            .content()
            .read_object_version(request.object_id.as_str(), request.target_version.as_str())
            .is_err()
    );
    assert_eq!(
        store
            .receipts_for_version(&request.object_id, &request.target_version)
            .unwrap()
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([requester.clone()])
    );
    drop(store);
    drop(inspect);
    runtime.stop_node(&primary).unwrap();
    runtime.reopen_node(primary.clone()).unwrap();
    let mut restored = runtime.metadata_primary_service("/services").unwrap();
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
    runtime.request_repair_reconciliation(&primary).unwrap();
    let recovery_start = runtime.rpc_observations().len();
    let recovery_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = recovery_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal bounded recovery did not settle the actual pending intent"
        );
        let tick = runtime.maintenance_tick(&primary, remaining).unwrap();
        tick.outcome.unwrap();
        if runtime.rpc_observations()[recovery_start..]
            .iter()
            .any(|event| {
                event.source == primary
                    && event.target == requester
                    && event.route == DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                    && event.phase == crate::test_support::DfsTestRpcPhase::Completed
            })
        {
            break;
        }
    }
    let events = writer.process_incoming_until_pending_routed_writes_ready(Duration::from_secs(5));
    assert!(
        events.is_ok(),
        "late response result={events:?}; actual canonical intent={:?}; matching RPCs={:?}",
        runtime
            .metadata_primary_service("/services")
            .map(|core| core
                .metadata_backend()
                .unwrap()
                .get_intent(&request.object_id, intent_id)),
        runtime
            .rpc_observations()
            .into_iter()
            .filter(|event| event.source == primary
                && event.target == requester
                && [
                    DFS_DATA_FETCH_BLOB_ROUTE,
                    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE
                ]
                .contains(&event.route.as_str()))
            .collect::<Vec<_>>()
    );
    let events = events.unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteWriteCompleted { object_id, primary_node_id,
            result: DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed { version, content_hash, .. }
            }, .. } if object_id == &request.object_id && primary_node_id == &primary
                && version == &request.target_version && content_hash == &manifest.content_hash)));
    let published = runtime.client(&primary).unwrap();
    let receipts = runtime
        .node_metadata(&primary)
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
