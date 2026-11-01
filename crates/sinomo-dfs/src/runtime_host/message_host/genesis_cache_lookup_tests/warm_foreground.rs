use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
use std::collections::BTreeSet;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

// Test-owned negative gates must be released on every failed setup/unwind.
// Removing selectors never resets the fabric's sticky exhaustion counter.
struct WarmFaultCleanup<'a> {
    native: &'a crate::test_runtime::DfsTestRuntime,
    primary: sinomo_core::node::NodeId,
    ack_peer: sinomo_core::node::NodeId,
    requester: sinomo_core::node::NodeId,
    foreground: Arc<Semaphore>,
    routine: Arc<Semaphore>,
}
impl WarmFaultCleanup<'_> {
    fn owns_pause(&self, rule: &FaultRule) -> bool {
        let FaultAction::PauseBeforeDispatch(gate) = &rule.action else {
            return false;
        };
        rule.origin == Some(RpcOrigin::RegisteredHost)
            && ((rule.source.as_ref() == Some(&self.requester)
                && rule.target.as_ref() == Some(&self.primary)
                && rule.route.as_deref() == Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE)
                && Arc::ptr_eq(gate, &self.foreground))
                || (rule.source.as_ref() == Some(&self.primary)
                    && rule.target.as_ref() == Some(&self.ack_peer)
                    && rule.route.as_deref() == Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE)
                    && Arc::ptr_eq(gate, &self.routine)))
    }
    fn owns_setup(&self, rule: &FaultRule) -> bool {
        matches!(rule.action, FaultAction::BudgetExhausted)
            && rule.source.as_ref() == Some(&self.primary)
            && rule.target.as_ref() == Some(&self.ack_peer)
            && rule.call_id.is_none()
            && rule.origin.is_none()
            && matches!(
                rule.route.as_deref(),
                Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE)
                    | Some(crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE)
            )
    }
    fn retire_consumed(&self) -> usize {
        self.native
            .remove_faults_matching(|rule| rule.remaining_hits == 0 && self.owns_pause(rule))
    }
}
impl Drop for WarmFaultCleanup<'_> {
    fn drop(&mut self) {
        self.foreground.close();
        self.routine.close();
        self.native
            .remove_faults_matching(|rule| self.owns_pause(rule) || self.owns_setup(rule));
    }
}
#[test]
fn warm_genesis_foreground_releases_actual_routine_checkout_before_admission() {
    const PATH: &str = "/warm-foreground/file.bin";
    const PENDING: &str = "native-warm-genesis-hidden-mkdir";
    const PENDING_PATH: &str = "/warm-maint/unconfirmed";
    const OPERATION: &str = "native-warm-genesis-overwrite";
    let (native, initial, shard) = crate::test_support::current_native_metadata_service(
        "native-warm-genesis-foreground",
        3,
        PATH,
    )
    .unwrap();
    drop(initial);
    let primary = shard.primary_node_id.clone();
    let ack_peer = shard.replica_node_ids[0].clone();
    let requester = shard.replica_node_ids[1].clone();
    let mut writer = native.client(&requester).unwrap();
    writer.posix_mkdir_all("/warm-foreground").unwrap();
    writer.posix_mkdir_all("/warm-maint").unwrap();
    let options = crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: vec![primary.clone(), ack_peer.clone(), requester.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let base = writer
        .posix_write_file(PATH, b"before", options.clone())
        .unwrap();
    writer
        .posix_sync_file(PATH, crate::DfsPosixSyncKind::File)
        .unwrap();
    let data = native.data_dir(&primary).clone();
    let scheduler = daemon_dfs_mutation_scheduler(&data).unwrap();
    let slot = daemon_dfs_message_service_cache()
        .existing_root(&data.root())
        .unwrap()
        .unwrap();
    let resident = slot
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .resident_metadata_backend
        .clone()
        .unwrap();
    let handle = resident
        .scope_shard(
            &shard.file_system_id,
            &crate::DfsMetadataShardId::new(&shard.shard_id),
        )
        .unwrap();
    let expected_slot = handle.native_replica_observation_identity();
    let armed = Arc::new(AtomicBool::new(false));
    let before_execute = Arc::new(AtomicBool::new(false));
    let hook_armed = Arc::clone(&armed);
    let hook_seen = Arc::clone(&before_execute);
    let hook_data = data.clone();
    let hook_fs = shard.file_system_id.clone();
    let hook_shard = crate::DfsMetadataShardId::new(&shard.shard_id);
    let hook_slot = expected_slot.clone();
    let hook = install_daemon_dfs_pre_dispatch_test_hook(
        &data,
        Arc::new(move |point, command, _| {
            if point == DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
                && command.is_none()
                && !hook_armed.load(Ordering::Acquire)
            {
                return Err(crate::DfsError::Cancelled {
                    operation: "hold normal Routine until real hidden prepare is observed",
                });
            }
            if matches!(point, DaemonDfsPreDispatchTestPoint::BeforeExecute { admission_held: true })
            && command.is_some_and(|command| matches!(&command.routed_message.message,
                crate::DfsNetworkMessage::DistributedWriteRequest(write) if write.request_id == OPERATION)) {
            let root = daemon_dfs_message_service_cache().existing_root(&hook_data.root()).unwrap().unwrap();
            let cache = root.lock().unwrap();
            let entry = cache.as_ref().unwrap();
            assert!(entry.primary_service.is_some(), "Routine returned the actual cached Main lane before execution");
            let actual = entry.resident_metadata_backend.as_ref().unwrap()
                .scope_shard(&hook_fs, &hook_shard).unwrap().native_replica_observation_identity();
            assert_eq!(actual, hook_slot, "foreground uses the same restored physical owner");
            hook_seen.store(true, Ordering::Release);
        }
            Ok(())
        }),
    );
    let foreground_gate = Arc::new(Semaphore::new(0));
    let routine_gate = Arc::new(Semaphore::new(0));
    let fault_cleanup = WarmFaultCleanup {
        native: &native,
        primary: primary.clone(),
        ack_peer: ack_peer.clone(),
        requester: requester.clone(),
        foreground: Arc::clone(&foreground_gate),
        routine: Arc::clone(&routine_gate),
    };
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        native.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: Some(ack_peer.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::BudgetExhausted,
        });
    }
    let original = native
        .client(&requester)
        .unwrap()
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            PENDING.into(),
            shard.file_system_id.clone(),
            primary.clone(),
            crate::DfsNamespaceOperation::MkdirAll {
                path: PENDING_PATH.into(),
            },
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            false,
            false,
        );
    native.remove_faults_matching(|rule| fault_cleanup.owns_setup(rule));
    native.assert_fault_budgets().unwrap();
    assert!(
        matches!(&original, Ok(outcome) if matches!(&outcome.result,
        crate::DfsNamespaceResult::Rejected { .. })),
        "{original:?}"
    );
    let owner = native.metadata_primary_service(PENDING_PATH).unwrap();
    let saved = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_for_request(&requester, PENDING)
        .unwrap()
        .expect("genuine rejected mkdir has a real hidden prepare");
    assert_eq!(saved.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&shard.file_system_id, PENDING_PATH)
            .unwrap()
            .is_none()
    );
    drop(owner);
    let trace = crate::native_shard_trace::capture_native_shard_trace(
        &data.root(),
        crate::DfsMetadataShardId::new(&shard.shard_id),
    )
    .unwrap();
    native.install_fault(FaultRule {
        source: Some(requester.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&foreground_gate)),
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    writer.set_transport_operation_deadline(Some(deadline));
    let (sent, received) = mpsc::sync_channel(1);
    let start = native.rpc_observations().len();
    let foreground = std::thread::spawn(move || {
        sent.send(writer.posix_write_file_with_operation_id(PATH, b"after", options, OPERATION))
            .unwrap();
    });
    let encoded = loop {
        let found = native.rpc_observations().iter().skip(start).any(|call| {
            call.source == requester
                && call.target == primary
                && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.phase == RpcPhase::BeforeDispatch
        });
        if found || Instant::now() >= deadline {
            break found;
        }
        std::thread::yield_now();
    };
    native.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(ack_peer.clone()),
        route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&routine_gate)),
    });
    let paths = DaemonPaths::new(&data);
    let runtime = native.runtime_handle();
    let resources = native
        .component_context(&primary)
        .unwrap()
        .node_resources()
        .unwrap()
        .latest();
    armed.store(true, Ordering::Release);
    let maintenance = std::thread::spawn(move || {
        runtime.block_on(
        crate::runtime_host::maintenance_runtime::run_daemon_dfs_maintenance_tick_kind_on_runtime(
            &paths, &runtime, crate::runtime_host::maintenance_runtime::DaemonDfsMaintenanceKind::Routine, resources))
    });
    // Observe the live physical checkout before doing a full trace export:
    // the existing direct authority RPC has its own shorter bounded deadline.
    let mut retired_pause_selectors = 0;
    let (held_owner, busy, held_call_id, held_rpc_frontier, held_trace_frontier) = loop {
        retired_pause_selectors += fault_cleanup.retire_consumed();
        let calls = native.rpc_observations();
        let active = calls.iter().skip(start).find(|call| {
            call.source == primary
                && call.target == ack_peer
                && call.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                && call.phase == RpcPhase::BeforeDispatch
                && call.origin == RpcOrigin::RegisteredHost
                && !calls.iter().any(|later| {
                    later.call_id == call.call_id
                        && matches!(
                            later.phase,
                            RpcPhase::AfterDispatch
                                | RpcPhase::Completed
                                | RpcPhase::EndedWithoutResponse
                        )
                })
        });
        let held_owner = active.is_some() && scheduler.global_is_exclusively_held();
        let busy = held_owner && handle.try_checkout().unwrap().is_none();
        let frontier = trace.observation_frontier();
        if (busy && frontier.is_some()) || Instant::now() >= deadline {
            break (
                held_owner,
                busy,
                active.map(|call| call.call_id),
                calls.len(),
                frontier,
            );
        }
        std::thread::yield_now();
    };
    while retired_pause_selectors < 2 && Instant::now() < deadline {
        retired_pause_selectors += fault_cleanup.retire_consumed();
        if retired_pause_selectors < 2 {
            std::thread::yield_now();
        }
    }
    let fault_budgets_clean_before_release = native.assert_fault_budgets().is_ok();
    let held_carrier_ids = native
        .rpc_observations()
        .iter()
        .skip(start)
        .take(held_rpc_frontier.saturating_sub(start))
        .filter(|call| {
            call.source == primary
                && call.target == ack_peer
                && call.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                && call.origin == RpcOrigin::RegisteredHost
                && call.phase == RpcPhase::BeforeDispatch
        })
        .map(|call| call.call_id)
        .collect::<BTreeSet<_>>();
    let live_at_foreground_release = held_call_id.is_some_and(|call_id| {
        held_carrier_ids == BTreeSet::from([call_id])
            && !native.rpc_observations().iter().any(|call| {
                call.call_id == call_id
                    && matches!(
                        call.phase,
                        RpcPhase::AfterDispatch
                            | RpcPhase::Completed
                            | RpcPhase::EndedWithoutResponse
                    )
            })
    }) && scheduler.global_is_exclusively_held()
        && handle.try_checkout().unwrap().is_none();
    let held = held_owner && live_at_foreground_release;
    foreground_gate.add_permits(1);
    let early = received.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    let completed_before_gate_release = early.is_ok();
    // Always release both genuine original carriers before assertion/shutdown.
    routine_gate.add_permits(1);
    foreground_gate.add_permits(1);
    let outcome = match early {
        Ok(value) => value,
        Err(_) => received
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap(),
    };
    foreground.join().unwrap();
    let maintenance_outcome = maintenance.join().unwrap();
    native.remove_faults_matching(|rule| fault_cleanup.owns_pause(rule));
    drop(hook);
    let mut raw_trace = Vec::new();
    trace.export_raw(&mut raw_trace).unwrap();
    fs::write(
        data.root().join("warm-genesis-after-foreground-trace.json"),
        &raw_trace,
    )
    .unwrap();
    let captured: serde_json::Value = serde_json::from_slice(&raw_trace).unwrap();
    let packets = captured["boundary_packets"].as_array().unwrap();
    let ordinal = |packet: &serde_json::Value| {
        packet["owner_ordinal"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok())
    };
    let expected_operation = serde_json::json!({
        "kind":"prepared-operation", "operation_id":saved.digest.operation_id,
        "file_system_id":saved.digest.file_system_id,"shard_id":saved.digest.shard_id,
        "owner_epoch":saved.digest.owner_epoch,"prepare_sequence":saved.digest.prepare_sequence,
        "payload_digest":saved.digest.payload_digest
    });
    // Started is deliberately buffered until its real RPC owner returns. The
    // already allocated ordinal joins that exact body to the earlier live cut.
    let held_native_attempts = packets
        .iter()
        .filter(|packet| {
            let facts = &packet["facts"];
            facts["phase"] == "direct-journal-rpc-attempt-started"
                && facts["target_node_id"] == serde_json::json!(&ack_peer)
                && facts["route"] == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                && facts["command"]["source_node_id"] == serde_json::json!(&primary)
                && facts["command"]["authority_probe"]["operation"] == expected_operation
                && ordinal(packet)
                    .zip(held_trace_frontier)
                    .is_some_and(|(started, frontier)| started <= frontier)
                && packets.iter().any(|terminal| {
                    let ended = &terminal["facts"];
                    matches!(
                        ended["phase"].as_str(),
                        Some(
                            "direct-journal-rpc-attempt-returned"
                                | "direct-journal-rpc-attempt-abandoned"
                        )
                    ) && ended["attempt_id"] == facts["attempt_id"]
                        && ordinal(terminal)
                            .zip(held_trace_frontier)
                            .is_some_and(|(returned, frontier)| returned > frontier)
                })
        })
        .collect::<Vec<_>>();
    let held_original_attempt = (held_native_attempts.len() == 1)
        .then(|| held_native_attempts[0]["facts"]["attempt_id"].as_str())
        .flatten();
    let exact_saved_probe_at_live_cut = held_original_attempt.is_some()
        && captured["dropped"].as_str() == Some("0")
        && captured["incomplete"].as_str() == Some("0")
        && captured["closed"].as_bool() == Some(false)
        && captured["actual_owner_stopped"].as_bool() == Some(false);
    fs::write(data.root().join("warm-genesis-foreground-setup.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "actual_foreground_encoded_before_routine":encoded,"actual_routine_wait":held,"same_warm_checkout_busy":busy,
        "active_native_call_id_at_checkout":held_call_id,"active_original_attempt_id":held_original_attempt,
        "saved_rpc_frontier":held_rpc_frontier,"saved_trace_frontier":held_trace_frontier,
        "selected_held_carrier_ids":held_carrier_ids,"native_attempts_spanning_live_frontier":held_native_attempts,
        "exact_saved_probe_joined_to_live_frontier":exact_saved_probe_at_live_cut,"same_active_checkout_at_foreground_release":live_at_foreground_release,
        "foreground_completed_before_ack_gate_release":completed_before_gate_release,"before_execute_same_owner":before_execute.load(Ordering::Acquire),
        "retired_consumed_pause_selectors":retired_pause_selectors,"fault_budgets_clean_before_release":fault_budgets_clean_before_release,
        "foreground_result":format!("{outcome:?}"),"maintenance_result":format!("{maintenance_outcome:?}"),"saved_digest":saved.digest,
        "actual_rpc":format!("{:#?}",native.rpc_observations()),
        "qualification":"live carrier/Global/same-slot Busy cut joined after return to unique exact native Started <= frontier < terminal; before optional sealed-operation lookup; failed setup remains failed"
    })).unwrap()).unwrap();
    assert!(
        encoded && held && busy && exact_saved_probe_at_live_cut,
        "actual encoded caller and held normal warm Routine are required"
    );
    assert!(
        completed_before_gate_release,
        "foreground must finish while the same actual authority gate remains closed"
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(retired_pause_selectors, 2);
    assert!(fault_budgets_clean_before_release);
    let continuation = native
        .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    let owner = native.metadata_primary_service(PATH).unwrap();
    let original_record = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&saved.digest.operation_id)
        .unwrap()
        .unwrap();
    let operation = format!(
        "write:{}",
        crate::intent::stable_write_intent_id(&requester, &base.object_id, OPERATION)
    );
    let committed = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&operation)
        .unwrap()
        .unwrap();
    committed.validate().unwrap();
    let terminal = committed.payload.terminal_intent.as_ref().unwrap();
    let qualifying = committed
        .payload
        .content_receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.object_id == terminal.object_id
                && receipt.receipt.committed_version == terminal.target_version
                && receipt.content_hash == terminal.content_hash
                && terminal
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
        })
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<BTreeSet<_>>();
    fs::write(data.root().join("warm-genesis-foreground-result.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "actual_foreground_encoded_before_routine":encoded,"actual_routine_wait":held,"same_warm_checkout_busy":busy,
        "active_native_call_id_at_checkout":held_call_id,"active_original_attempt_id":held_original_attempt,
        "saved_rpc_frontier":held_rpc_frontier,"saved_trace_frontier":held_trace_frontier,
        "selected_held_carrier_ids":held_carrier_ids,"native_attempts_spanning_live_frontier":held_native_attempts,
        "exact_saved_probe_joined_to_live_frontier":exact_saved_probe_at_live_cut,
        "same_active_checkout_at_foreground_release":live_at_foreground_release,
        "foreground_completed_before_ack_gate_release":completed_before_gate_release,"before_execute_same_owner":before_execute.load(Ordering::Acquire),
        "retired_consumed_pause_selectors":retired_pause_selectors,"fault_budgets_clean_before_release":fault_budgets_clean_before_release,
        "foreground_result":format!("{outcome:?}"),"maintenance_result":format!("{maintenance_outcome:?}"),
        "continuation":format!("{continuation:?}"),"saved_digest":saved.digest,"saved_body":saved.payload,
        "original_recovered":original_record,"foreground_terminal":committed,
        "qualifying_receipt_nodes":qualifying,"actual_rpc":format!("{:#?}",native.rpc_observations())
    })).unwrap()).unwrap();
    trace.close();
    trace
        .export_raw(fs::File::create(data.root().join("warm-genesis-native-trace.json")).unwrap())
        .unwrap();
    let bytes = native
        .client(&requester)
        .unwrap()
        .posix_read_file(PATH)
        .unwrap();
    drop(owner);
    drop(trace);
    drop(handle);
    drop(resident);
    drop(slot);
    drop(scheduler);
    drop(fault_cleanup);
    native.close().unwrap();
    assert!(
        encoded && held && busy && exact_saved_probe_at_live_cut,
        "actual encoded caller and held normal warm Routine are required"
    );
    assert_eq!(
        retired_pause_selectors, 2,
        "only consumed test-owned selectors are retired; the actual calls retain both closed gates"
    );
    assert!(
        fault_budgets_clean_before_release,
        "no exhausted fault budget is ignored or reset"
    );
    assert!(
        completed_before_gate_release && outcome.is_ok(),
        "foreground waits for no test-owned ACK gate: {outcome:?}"
    );
    assert!(
        before_execute.load(Ordering::Acquire),
        "same restored native owner before foreground effects"
    );
    assert!(maintenance_outcome.is_ok() && continuation.outcome.is_ok());
    assert_eq!(original_record.digest, saved.digest);
    assert_eq!(original_record.payload, saved.payload);
    assert_eq!(
        original_record.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert_eq!(
        committed.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert_eq!(
        (
            terminal.policy.min_commit_replicas,
            terminal.policy.desired_replicas
        ),
        (2, 3)
    );
    assert!(qualifying.len() >= 2);
    assert_eq!(bytes, b"after");
}

fn warm_negative(name: &str, kind: u8) {
    let (native, nodes) = crate::test_support::DfsTestRuntime::start_cluster(name, 2).unwrap();
    let local = native.root_shard().unwrap().primary_node_id;
    let data = native.data_dir(&local).clone();
    let mut client = native.client(&local).unwrap();
    let object = client.posix_mkdir("/genesis-cache-ready").unwrap();
    drop(client);
    let job = if kind == 0 {
        Some(
            native
                .apply_placement_policy(
                    &local,
                    "/genesis-cache-ready",
                    crate::DfsPlacementScope::Subtree,
                    crate::DfsPlacementPolicy::new(
                        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                        1,
                        1,
                    )
                    .unwrap(),
                )
                .unwrap(),
        )
    } else {
        None
    };
    let paths = ClusterPaths::new(&data);
    let runtime = native.runtime_handle();
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let mut preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &paths, &peer, &runtime,
        ))
        .unwrap();
    let mut command = mutation_for_genesis_check(&native, &local);
    if let Some(job) = job {
        let crate::DfsNetworkMessage::NamespaceRequest(request) =
            &mut command.routed_message.message
        else {
            unreachable!()
        };
        request.operation = crate::DfsNamespaceOperation::ReconcilePlacement {
            job_id: job.job_id,
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy: object.policy.clone(),
            replication_policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            desired_replica_node_ids: vec![local.clone()],
        };
        assert!(!daemon_dfs_message_preempts_maintenance(
            &command.routed_message.message
        ));
    }
    let owner = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(&data)).unwrap();
    runtime
        .block_on(prepare_daemon_dfs_genesis_ingress(
            &data,
            &paths,
            &peer,
            &command,
            Some(&preflight),
            &runtime,
            &owner,
        ))
        .unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data).unwrap();
    let interest = scheduler.begin_maintenance_interest();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let slot = daemon_dfs_message_service_cache()
        .existing_root(&data.root())
        .unwrap()
        .unwrap();
    let resident = slot
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .resident_metadata_backend
        .clone()
        .unwrap();
    let shard = native.root_shard().unwrap();
    let handle = resident
        .scope_shard(
            &shard.file_system_id,
            &crate::DfsMetadataShardId::new(&shard.shard_id),
        )
        .unwrap();
    let lease = handle.checkout().unwrap();
    let result;
    if kind == 0 {
        let worker_data = data.clone();
        let worker_paths = paths.clone();
        let worker_peer = peer.clone();
        let worker_preflight = preflight.clone();
        let worker_runtime = runtime.clone();
        let worker_owner = Arc::clone(&owner);
        let (send, receive) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            send.send(
                worker_runtime
                    .block_on(prepare_daemon_dfs_genesis_ingress(
                        &worker_data,
                        &worker_paths,
                        &worker_peer,
                        &command,
                        Some(&worker_preflight),
                        &worker_runtime,
                        &worker_owner,
                    ))
                    .map_err(|error| error.into_dfs_error().to_string()),
            )
            .unwrap()
        });
        let early = receive.recv_timeout(Duration::from_millis(100));
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = match early {
            Ok(value) => value,
            Err(_) => receive
                .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap(),
        };
        worker.join().unwrap();
        assert!(
            !requested,
            "a real placement child must not cancel the owner it needs"
        );
        assert!(result.is_ok());
    } else if kind == 1 {
        preflight.generation = preflight.generation.checked_sub(1).unwrap();
        let stale = runtime.block_on(prepare_daemon_dfs_genesis_ingress(
            &data,
            &paths,
            &peer,
            &command,
            Some(&preflight),
            &runtime,
            &owner,
        ));
        let typed = matches!(
            stale,
            Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(_))
        );
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = Ok(());
        assert!(
            typed && !requested,
            "stale original authority cannot gain scheduling or publication readiness"
        );
    } else {
        command.routed_message.target_node_id =
            nodes.iter().find(|node| **node != local).unwrap().clone();
        let invalid = native.rpc_request(
            &local,
            &local,
            crate::dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        );
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = Ok(());
        assert!(
            invalid.is_err() && !requested,
            "wrong target must fail before warm owner wait"
        );
    }
    fs::write(data.root().join("warm-genesis-negative.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "kind":kind,"preempted":interest.is_requested(),"result":result,"unchanged_object":format!("{object:?}")
    })).unwrap()).unwrap();
    drop(interest);
    drop(scheduler);
    drop(handle);
    drop(resident);
    drop(slot);
    drop(owner);
    drop(preflight);
    let mut check = native.client(&local).unwrap();
    assert_eq!(
        check
            .posix_stat_distributed("/genesis-cache-ready")
            .unwrap()
            .object_id,
        object.object_id
    );
    assert!(matches!(
        check.posix_stat_distributed("/genesis-cache-renamed"),
        Err(crate::DfsPosixError::NotFound { .. })
    ));
    drop(check);
    native.close().unwrap();
}

#[test]
fn warm_genesis_background_placement_wait_does_not_preempt() {
    warm_negative("native-warm-genesis-background", 0);
}
#[test]
fn warm_genesis_stale_authority_does_not_bypass_owner() {
    warm_negative("native-warm-genesis-stale", 1);
}
#[test]
fn warm_genesis_wrong_target_does_not_touch_owner() {
    warm_negative("native-warm-genesis-wrong-target", 2);
}
