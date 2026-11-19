use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

fn native_trace_json(
    trace: &crate::native_shard_trace::NativeShardTraceCapture,
) -> serde_json::Value {
    let mut bytes = Vec::new();
    trace.export_raw(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn maintenance_cancellation_is_owned_by_its_root_and_foreground_generation() {
    let first_root = temp_root("maintenance-cancel-generation-first");
    let other_root = temp_root("maintenance-cancel-generation-other");
    let first = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(first_root.path())).unwrap();
    let other = daemon_dfs_mutation_scheduler(&SinomoDataDir::new(other_root.path())).unwrap();
    let turn = first.begin_maintenance_interest();
    let cancellation = turn.cancellation_flag();
    let other_turn = other.begin_maintenance_interest();
    other.request_foreground_admission();
    assert!(other_turn.cancellation_flag().is_cancelled());
    assert!(
        !cancellation.is_cancelled(),
        "another DATA owner is isolated"
    );
    first.request_foreground_admission();
    assert!(cancellation.is_cancelled());
    drop(turn);
    let next_turn = first.begin_maintenance_interest();
    let next_cancellation = next_turn.cancellation_flag();
    assert!(
        !next_cancellation.is_cancelled(),
        "an old generation cannot cancel a new turn"
    );
    let caller = crate::DfsCancellationFlag::new();
    let linked = crate::DfsCancellationFlag::linked([caller.clone(), next_cancellation]);
    assert!(!linked.is_cancelled());
    caller.cancel();
    assert!(
        linked.is_cancelled(),
        "the caller's existing cancellation remains effective"
    );
}

// This reproduces the actual two-owner wait graph. A normal encoded root write
// holds A's root lane while local exact work holds A's users lane, and
// Routine(B) recovers its saved users prepare and waits for A's authority ACK
// on that users lane. The root write then needs B's authority ACK, which only
// B's yield lets through. Neither assignment nor its all-participant
// publication rule is changed by the test.
#[test]
fn native_routine_authority_wait_yields_to_reciprocal_foreground_authority() {
    use crate::test_runtime::{FaultAction, FaultRule, RpcPhase};
    use std::sync::Condvar;

    const PATH: &str = "/cluster-tests/native-reciprocal-maintenance.bin";
    const PENDING: &str = "native-reciprocal-users-mkdir";
    const USER_PATH: &str = "/users/native-reciprocal-maintenance";
    let (native, initial, root) = crate::test_support::current_native_metadata_service(
        "native-reciprocal-maintenance",
        3,
        PATH,
    )
    .unwrap();
    drop(initial);
    let primary = root.primary_node_id.clone();
    let maintenance_node = root.replica_node_ids[0].clone();
    let requester = root.replica_node_ids[1].clone();
    let users = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "users")
        .unwrap();
    let users = if users.primary_node_id == maintenance_node {
        users
    } else {
        native
            .reassign_metadata_shard(
                &users.file_system_id,
                &users.shard_id,
                maintenance_node.clone(),
                vec![primary.clone(), requester.clone()],
            )
            .unwrap()
    };
    let nodes = vec![primary.clone(), maintenance_node.clone(), requester.clone()];
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let mut writer = native.client(&requester).unwrap();
    writer.posix_mkdir_all("/cluster-tests").unwrap();
    let initial_object = writer
        .posix_write_file(PATH, b"before", options.clone())
        .unwrap();
    writer
        .posix_sync_file(PATH, DfsPosixSyncKind::File)
        .unwrap();
    let root_operation = format!(
        "write:{}",
        crate::intent::stable_write_intent_id(
            &requester,
            &initial_object.object_id,
            "native-reciprocal-root-overwrite",
        )
    );

    // A real encoded stable mkdir reaches durable preparation, then loses its
    // authority confirmation. Its pending request makes the SAME immutable
    // body eligible for ordinary Routine namespace recovery.
    let mut namespace = native.client(&requester).unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let allow_maintenance = Arc::clone(&armed);
    let background_hold = install_daemon_dfs_pre_dispatch_test_hook(
        native.data_dir(&maintenance_node),
        Arc::new(move |point, command, _| {
            if point == DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
                && command.is_none()
                && !allow_maintenance.load(Ordering::Acquire)
            {
                return Err(crate::DfsError::Cancelled {
                    operation: "hold native recovery before admission",
                });
            }
            Ok(())
        }),
    );
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        native.install_fault(FaultRule {
            source: Some(maintenance_node.clone()),
            target: Some(primary.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::BudgetExhausted,
        });
    }
    let original_namespace_outcome = namespace
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            PENDING.into(),
            users.file_system_id.clone(),
            maintenance_node.clone(),
            DfsNamespaceOperation::MkdirAll {
                path: USER_PATH.into(),
            },
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            false,
            false,
        );
    native.clear_faults();
    drop(namespace);
    let users_owner = native.metadata_primary_service("/users").unwrap();
    let pending = users_owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_for_request(&requester, PENDING)
        .unwrap()
        .expect("the actual failed mkdir must retain its pending request and hidden prepare");
    let saved = pending.digest.clone();
    let payload = pending.payload.clone();
    assert!(
        matches!(&original_namespace_outcome,
            Ok(outcome) if matches!(&outcome.result, crate::DfsNamespaceResult::Rejected { .. })
        ),
        "the original mkdir must return its typed rejection: {original_namespace_outcome:?}"
    );
    assert_eq!(saved.owner_epoch, users.epoch);
    assert_eq!(pending.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(
        users_owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&users.file_system_id, USER_PATH)
            .unwrap()
            .is_none(),
        "the failed original preparation must remain hidden"
    );
    drop(users_owner);

    let users_trace = crate::native_shard_trace::capture_native_shard_trace(
        &native.data_dir(&maintenance_node).root(),
        DfsMetadataShardId::new("users"),
    )
    .unwrap();
    let root_trace = crate::native_shard_trace::capture_native_shard_trace(
        &native.data_dir(&primary).root(),
        DfsMetadataShardId::new("root"),
    )
    .unwrap();

    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let held = Arc::clone(&gate);
    let foreground_hold = install_daemon_dfs_pre_dispatch_test_hook(
        native.data_dir(&primary),
        Arc::new(move |point, command, _| {
            let Some(command) = command else {
                return Ok(());
            };
            let DfsNetworkMessage::DistributedWriteRequest(write) = &command.routed_message.message
            else {
                return Ok(());
            };
            if !matches!(
                point,
                DaemonDfsPreDispatchTestPoint::BeforeExecute {
                    admission_held: true
                }
            ) || !write
                .object_metadata
                .as_ref()
                .is_some_and(|object| object.path == PATH)
            {
                return Ok(());
            }
            let (lock, changed) = &*held;
            let mut state = lock.lock().unwrap();
            state.0 = true;
            changed.notify_all();
            while !state.1 {
                state = changed.wait(state).unwrap();
            }
            Ok(())
        }),
    );
    let source_scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&primary)).unwrap();
    let target_scheduler =
        daemon_dfs_mutation_scheduler(native.data_dir(&maintenance_node)).unwrap();
    let paths = DaemonPaths::new(native.data_dir(&maintenance_node));
    let runtime = native.runtime_handle();
    let resources = native
        .component_context(&maintenance_node)
        .unwrap()
        .node_resources()
        .unwrap()
        .latest();
    let root_lane = DaemonDfsMutationLaneKey {
        file_system_id: root.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(root.shard_id.clone()),
    };
    let foreground = std::thread::spawn(move || {
        writer.posix_write_file_with_operation_id(
            PATH,
            b"after",
            options,
            "native-reciprocal-root-overwrite",
        )
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let foreground_entered = {
        let (lock, changed) = &*gate;
        let state = lock.lock().unwrap();
        let (state, _) = changed
            .wait_timeout_while(
                state,
                deadline.saturating_duration_since(Instant::now()),
                |state| !state.0,
            )
            .unwrap();
        state.0
    };
    let source_held =
        source_scheduler.lane_held(&root_lane) && !source_scheduler.global_writer_holds();
    // Local exact work holds A's users lane, so A cannot answer Routine(B)'s
    // users probe until it ends; the routed root write holds only its own lane.
    let users_lane_hold = native
        .hold_main_exact_admission(&primary, &users.file_system_id, &users.shard_id)
        .unwrap();
    native.checkpoint_rpc_observation_window().unwrap();
    armed.store(true, Ordering::Release);
    let maintenance = std::thread::spawn(move || {
        runtime.block_on(
            super::super::maintenance_runtime::run_daemon_dfs_maintenance_tick_kind_on_runtime(
                &paths,
                &runtime,
                super::super::maintenance_runtime::DaemonDfsMaintenanceKind::Routine,
                resources,
            ),
        )
    });
    // Routine(B) holds its global admission while its direct authority call
    // to held A is still in flight. A trace packet publishes only when its call
    // ends, so the in-flight call is found here from RPC observations and is
    // bound to the saved operation from the trace after the cycle.
    let mut reciprocal_queued = false;
    while Instant::now() < deadline {
        reciprocal_queued = target_scheduler.global_is_exclusively_held() && {
            let calls = native.rpc_observations();
            calls.iter().any(|call| {
                call.source == maintenance_node
                    && call.target == primary
                    && call.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && call.phase == RpcPhase::BeforeDispatch
                    && !calls.iter().any(|ended| {
                        ended.call_id == call.call_id
                            && matches!(
                                ended.phase,
                                RpcPhase::Completed | RpcPhase::EndedWithoutResponse
                            )
                    })
            })
        };
        if reciprocal_queued || foreground.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    // Always release the original held request before assertions or shutdown.
    gate.0.lock().unwrap().1 = true;
    gate.1.notify_all();
    let released_at = Instant::now();
    let write_result = foreground.join().unwrap();
    let elapsed = released_at.elapsed();
    let maintenance_result = maintenance.join().unwrap();
    drop(users_lane_hold);
    drop(foreground_hold);
    drop(background_hold);
    let calls = native.rpc_observations();
    let acknowledged = calls.iter().any(|call| {
        call.source == primary
            && call.target == maintenance_node
            && matches!(
                call.route.as_str(),
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
            )
            && call.phase == RpcPhase::Completed
            && call.outcome_error.is_none()
    });
    let routine_waited_for_held_authority = native_trace_json(&users_trace)["boundary_packets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|packet| {
            let fact = &packet["facts"];
            fact["phase"] == "direct-journal-rpc-attempt-started"
                && fact["target_node_id"] == primary.as_str()
                && fact["command"]["authority_probe"]["operation"]["operation_id"]
                    == saved.operation_id
        });
    let root_facts = native_trace_json(&root_trace);
    let diagnostics = std::env::temp_dir().join(format!(
        "native-reciprocal-maintenance-evidence-{}",
        sinomo_core::Id::generate(),
    ));
    fs::create_dir_all(&diagnostics).unwrap();
    root_trace
        .export_raw(fs::File::create(diagnostics.join("root-cycle.json")).unwrap())
        .unwrap();
    users_trace
        .export_raw(fs::File::create(diagnostics.join("users-cycle.json")).unwrap())
        .unwrap();
    eprintln!(
        "native reciprocal maintenance diagnostics={}",
        diagnostics.display()
    );
    let root_starts = root_facts["boundary_packets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|packet| &packet["facts"])
        .filter(|fact| {
            fact["phase"] == "direct-journal-rpc-attempt-started"
                && fact["target_node_id"] == maintenance_node.as_str()
                && fact["command"]["authority_probe"]["operation"]["operation_id"] == root_operation
        })
        .map(|fact| {
            let command: crate::DfsMetadataJournalBatchCommand =
                serde_json::from_value(fact["command"].clone()).unwrap();
            assert_eq!(command.source_node_id.as_ref(), Some(&primary));
            let probe = command.authority_probe.as_ref().unwrap();
            let confirmation =
                crate::service_replica::metadata_authority_probe_confirmation_id(probe)
                    .expect("actual probe has a canonical prefix confirmation");
            (command.request_id, confirmation)
        })
        .collect::<Vec<_>>();
    let accepted_authority = root_facts["boundary_packets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|packet| {
            let fact = &packet["facts"];
            fact["phase"] == "source-journal-response-accepted"
                && fact["authenticated_source_node_id"] == maintenance_node.as_str()
                && fact["expected_target_node_id"] == maintenance_node.as_str()
                && root_starts.iter().any(|(request, confirmation)| {
                    fact["response"]["request_id"] == *request
                        && fact["expected_confirmation_id"] == *confirmation
                        && fact["response"]["verified_authority_probe_id"] == *confirmation
                })
        });

    fs::write(
        diagnostics.join("relevant-rpc-observations.txt"),
        calls
            .iter()
            .filter(|call| {
                matches!(
                    call.route.as_str(),
                    crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                ) && ((call.source == primary && call.target == maintenance_node)
                    || (call.source == maintenance_node && call.target == primary))
            })
            .map(|call| format!("{call:?}\n"))
            .collect::<String>(),
    )
    .unwrap();
    fs::write(diagnostics.join("cycle-outcomes.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "foreground_entered":foreground_entered, "source_held":source_held,
        "reciprocal_queued":reciprocal_queued, "original_write_result":format!("{write_result:?}"),
        "write_after_release_elapsed_us":elapsed.as_micros().to_string(),
        "maintenance_result":format!("{maintenance_result:?}"),
        "root_prepared_operation":root_operation, "users_prepared_digest":saved,
        "exact_root_authority_commands":root_starts, "accepted_authority":accepted_authority,
    })).unwrap()).unwrap();

    // Continue the durable pending operation rather than replaying the root
    // write or reconstructing its body. The next turn must have a fresh flag.
    let recovery = native
        .maintenance_tick(
            &maintenance_node,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let owner = native.metadata_primary_service("/users").unwrap();
    let record = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&saved.operation_id)
        .unwrap()
        .unwrap();
    let root_owner = native.metadata_primary_service(PATH).unwrap();
    let root_record = root_owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&root_operation)
        .unwrap()
        .unwrap();
    root_record.validate().unwrap();
    drop(root_owner);
    let bytes = native
        .client(&requester)
        .unwrap()
        .posix_read_file_distributed(PATH);
    drop(owner);
    let omitted = native.observation_dropped_count();
    users_trace.close();
    root_trace.close();
    root_trace
        .export_raw(fs::File::create(diagnostics.join("root-final.json")).unwrap())
        .unwrap();
    users_trace
        .export_raw(fs::File::create(diagnostics.join("users-final.json")).unwrap())
        .unwrap();
    let users_facts = native_trace_json(&users_trace);
    drop(users_trace);
    drop(root_trace);
    native.close().unwrap();
    assert!(
        foreground_entered && source_held,
        "actual encoded root request must hold its root lane on A, not A's global writer"
    );
    assert!(
        reciprocal_queued && routine_waited_for_held_authority,
        "Routine(B) must wait for its users authority ACK from held A"
    );
    assert!(
        write_result.is_ok(),
        "original root write: {write_result:?}; maintenance: {maintenance_result:?}"
    );
    assert!(
        elapsed < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        "original authority deadline was exhausted: {elapsed:?}"
    );
    assert!(
        acknowledged && accepted_authority,
        "the actual native authority response from B must complete"
    );
    assert!(
        maintenance_result.is_ok(),
        "foreground preemption must retain an ordinary continuation: {maintenance_result:?}"
    );
    assert!(
        recovery.outcome.is_ok(),
        "next maintenance turn: {recovery:?}"
    );
    assert_eq!(record.digest, saved);
    assert_eq!(record.payload, payload);
    assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
    assert_eq!(
        root_record.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    let terminal = root_record.payload.terminal_intent.as_ref().unwrap();
    assert_eq!(terminal.policy.min_commit_replicas, 2);
    assert_eq!(terminal.policy.desired_replicas, 3);
    assert_eq!(
        terminal
            .desired_replica_node_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    let sealed = root_record
        .payload
        .content_receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.file_system_id == terminal.file_system_id
                && receipt.receipt.object_id == terminal.object_id
                && receipt.receipt.committed_version == terminal.target_version
                && receipt.content_hash == terminal.content_hash
                && terminal.content_hash.is_some()
                && terminal
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
        })
        .map(|receipt| &receipt.replica_node_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        sealed.len() >= 2,
        "actual W2 preparation must seal two distinct qualifying receipts: {root_record:?}"
    );
    assert_eq!(bytes.unwrap(), b"after");
    assert_eq!(omitted, 0);
    assert_eq!(users_facts["dropped"], "0");
    assert_eq!(root_facts["dropped"], "0");
}
