//! The node's repair executor schedules from the derived repair index of the
//! shards it is primary for. Each case drives passes explicitly; the fixture
//! holds the runtime's own routine maintenance out of the way.

use super::*;
use crate::DfsMaintenanceExecutionExclusions;
use crate::repair_executor::{
    register_repair_executor_wake, repair_executor_work, reset_repair_executor_for_test,
};

fn services_shard(runtime: &crate::test_support::DfsTestRuntime) -> DfsMetadataShardConfig {
    runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .expect("the fixture has a services shard")
}

fn write_below_n(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    path: &str,
    nodes: Vec<NodeId>,
    min_commit_replicas: usize,
) -> DfsStat {
    let desired = nodes.len();
    let bytes = (0..48 * 1024 + 5)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    service
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions {
                replica_candidates: nodes,
                policy: DfsReplicationPolicy::new(min_commit_replicas, desired).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
}

fn index_keys(
    backend: &dyn crate::DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
) -> Vec<(DfsObjectId, DfsVersion, NodeId)> {
    backend
        .repair_index_page(&DfsMetadataShardId::new(shard.shard_id.clone()), None, 64)
        .unwrap()
        .needs
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

fn has_receipt(backend: &dyn crate::DfsMetadataBackend, stat: &DfsStat, replica: &NodeId) -> bool {
    backend
        .receipts_for_version(&stat.object_id, &stat.version)
        .unwrap()
        .iter()
        .any(|receipt| &receipt.replica_node_id == replica)
}

/// One executor pass over `shards`, then the replies it drew.
fn executor_pass(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    data_root: &Path,
    shards: &[DfsMetadataShardConfig],
    exclusions: &DfsMaintenanceExecutionExclusions,
) -> usize {
    let pass = service
        .run_repair_executor_pass(data_root, shards, exclusions, 8)
        .unwrap();
    service.process_incoming_until_quiescent().unwrap();
    pass.scheduled.len()
}

#[test]
fn executor_schedules_only_primary_shards_and_a_successor_restarts_timing() {
    let name = "native-repair-executor-primary";
    let (runtime, owner, replica, mut service) = native_missing_content_repair(name);
    let path = format!("/services/{name}.bin");
    let (_execution, _hold) = native_private_repair_owner(&runtime, &service, &path, ());
    let data_root = runtime.data_dir(&owner).root();
    reset_repair_executor_for_test(&data_root);
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let _wake = register_repair_executor_wake(&data_root, None, move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let shard = services_shard(&runtime);
    let none = DfsMaintenanceExecutionExclusions::default();
    // The first pass observes this node's primary assignments; from then on
    // a primary shard's index change reaches the executor.
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &none
        ),
        0
    );
    wakes.store(0, Ordering::SeqCst);
    let stat = write_below_n(&mut service, &path, vec![owner.clone(), replica.clone()], 1);
    assert!(
        wakes.load(Ordering::SeqCst) > 0,
        "the shard writer's new need wakes the executor"
    );
    let key = (
        stat.object_id.clone(),
        stat.version.clone(),
        replica.clone(),
    );
    assert_eq!(
        index_keys(service.metadata_backend().unwrap(), &shard),
        vec![key.clone()]
    );

    let mut elsewhere = shard.clone();
    elsewhere.primary_node_id = replica.clone();
    assert_eq!(
        executor_pass(&mut service, &data_root, &[elsewhere], &none),
        0,
        "a node schedules no shard it is not primary for"
    );

    // The fixture denies the target's data routes: the attempt fails.
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &none
        ),
        1
    );
    let work = repair_executor_work(&data_root);
    assert_eq!(work.tracked_need_count, 1);
    assert!(
        work.next_due_unix_nanos
            .is_some_and(|due| due > crate::dfs_now_unix_nanos()),
        "a failed attempt backs off on the executor's single timer: {work:?}"
    );
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &none
        ),
        0,
        "in-memory backoff holds the need"
    );

    // A successor assignment of the same shard starts with empty backoff.
    let mut successor = shard.clone();
    successor.epoch += 1;
    assert_eq!(
        executor_pass(&mut service, &data_root, &[successor], &none),
        1,
        "the successor reads its index and attempts {key:?} at once"
    );
    runtime.clear_faults();
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn executor_parks_an_unreachable_target_and_repairs_it_on_the_reachability_wake() {
    let name = "native-repair-executor-reachability";
    let (runtime, owner, replica, mut service) = native_missing_content_repair(name);
    let path = format!("/services/{name}.bin");
    let (_execution, _hold) = native_private_repair_owner(&runtime, &service, &path, ());
    let data_root = runtime.data_dir(&owner).root();
    reset_repair_executor_for_test(&data_root);
    let stat = write_below_n(&mut service, &path, vec![owner.clone(), replica.clone()], 1);
    let shard = services_shard(&runtime);
    let unreachable = DfsMaintenanceExecutionExclusions::new([replica.clone()]);

    // A cached projection never defers the first attempt; its failure parks
    // the need for the reachability edge instead of a re-send timer.
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &unreachable
        ),
        1,
        "a cached projection never defers the first attempt"
    );
    // The failed attempt backs off; once due, its unreachable target parks it.
    let due = repair_executor_work(&data_root)
        .next_due_unix_nanos
        .expect("the failed attempt backs off");
    while crate::dfs_now_unix_nanos() < due {
        std::thread::sleep(Duration::from_millis(50));
    }
    crate::repair_executor::repair_executor_timer_fired(&data_root);
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &unreachable
        ),
        0
    );
    let work = repair_executor_work(&data_root);
    assert!(work.parked, "{work:?}");
    assert_eq!(
        work.next_due_unix_nanos, None,
        "nothing polls an unreachable target"
    );
    assert!(!has_receipt(
        service.metadata_backend().unwrap(),
        &stat,
        &replica
    ));

    // The target becomes reachable: the route-generation edge wakes the
    // executor because a need is parked, and one pass repairs it.
    runtime.clear_faults();
    let none = DfsMaintenanceExecutionExclusions::default();
    crate::repair_executor::repair_executor_reachability_changed(&data_root);
    assert_eq!(
        executor_pass(
            &mut service,
            &data_root,
            std::slice::from_ref(&shard),
            &none
        ),
        1
    );
    assert!(
        has_receipt(service.metadata_backend().unwrap(), &stat, &replica),
        "the reachable target holds a receipt after the woken pass"
    );
    assert!(index_keys(service.metadata_backend().unwrap(), &shard).is_empty());
    executor_pass(&mut service, &data_root, &[shard], &none);
    let work = repair_executor_work(&data_root);
    assert_eq!(
        (
            work.tracked_need_count,
            work.next_due_unix_nanos,
            work.parked
        ),
        (0, None, false),
        "a settled need leaves no timing behind: {work:?}"
    );
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn attempt_outcomes_end_only_the_current_attempt_and_wake_the_executor() {
    let root = sinomo_testdir::TestDir::new("repair-executor-attempt-outcome");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let _wake = register_repair_executor_wake(&data_root, None, move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let node = NodeId::from_legacy_named("node_repair_executor_outcome").unwrap();
    crate::repair_executor::bind_repair_executor_node_for_test(&node, &data_root);
    let key = (
        DfsObjectId::from_legacy_named("obj_repair_executor_outcome").unwrap(),
        DfsVersion::from_legacy_named("version_repair_executor_outcome").unwrap(),
        NodeId::from_legacy_named("node_repair_executor_outcome_target").unwrap(),
    );
    crate::repair_executor::repair_executor_attempt_started(&node, key.clone(), "request-1", 0);
    crate::repair_executor::repair_executor_attempt_ended(
        &node,
        &key,
        "request-stale",
        crate::repair_executor::DfsRepairAttemptOutcome::Failed,
    );
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "a stale reply ends nothing"
    );
    for outcome in [
        crate::repair_executor::DfsRepairAttemptOutcome::Settled,
        crate::repair_executor::DfsRepairAttemptOutcome::Failed,
    ] {
        crate::repair_executor::repair_executor_attempt_ended(&node, &key, "request-1", outcome);
    }
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the current attempt's outcome wakes once; the attempt is then over"
    );
    assert!(repair_executor_work(&data_root).due);
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn successor_primary_converges_outstanding_repair_from_its_own_index() {
    let name = "native-repair-executor-successor";
    let (mut runtime, service, shard) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(service);
    let retiring = shard.primary_node_id.clone();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let successor = nodes
        .iter()
        .find(|node| **node != retiring)
        .unwrap()
        .clone();
    let missing = nodes
        .iter()
        .find(|node| **node != retiring && **node != successor)
        .unwrap()
        .clone();
    for route in [
        crate::DFS_DATA_STAGE_BLOB_ROUTE,
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(crate::test_support::DfsTestFaultRule {
            source: None,
            target: Some(missing.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    }
    let path = format!("/services/{name}.bin");
    let mut writer = runtime.metadata_primary_service("/services").unwrap();
    let execution = native_fixture_write_admission(&runtime, &writer, &path);
    let stat = write_below_n(
        &mut writer,
        &path,
        vec![retiring.clone(), successor.clone(), missing.clone()],
        2,
    );
    drop(execution);
    drop(writer);
    let key = (
        stat.object_id.clone(),
        stat.version.clone(),
        missing.clone(),
    );

    // Offline takeover has no supported path: the handoff needs the retiring
    // primary's fence. It hands off while up, without any hold: with no
    // custody it carries no repair state over. Its own executor's attempt
    // failed against the denied target and awaits its re-check, so the need
    // is still outstanding when the successor takes the shard and the
    // retiring primary goes offline.
    runtime.clear_faults();
    let activated = runtime
        .reassign_metadata_shard(
            &stat.file_system_id,
            "services",
            successor.clone(),
            vec![missing.clone()],
        )
        .unwrap();
    assert_eq!(activated.primary_node_id, successor);
    runtime.stop_node(&retiring).unwrap();
    // The successor's replicated tables carry the need. Its own executor may
    // already be serving it from the assignment wake; nothing it schedules
    // came from the retiring owner.
    let successor_metadata = runtime.node_metadata(&successor).unwrap();
    assert!(
        has_receipt(&successor_metadata, &stat, &missing)
            || successor_metadata
                .repair_index_page(&DfsMetadataShardId::new("services"), None, 64)
                .unwrap()
                .needs
                .iter()
                .any(|(candidate, _)| *candidate == key),
        "the successor's index holds the outstanding need"
    );
    drop(successor_metadata);

    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let metadata = runtime.node_metadata(&successor).unwrap();
        if has_receipt(&metadata, &stat, &missing) {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "the successor's executor must converge the desired placement"
        );
        // A tick that yields to concurrent work is retried; the successor's
        // own wakes drive the same executor between ticks.
        let _ = runtime
            .maintenance_tick(&successor, remaining)
            .unwrap()
            .outcome;
    }
    let metadata = runtime.node_metadata(&successor).unwrap();
    assert!(
        metadata
            .repair_index_page(&DfsMetadataShardId::new("services"), None, 64)
            .unwrap()
            .needs
            .iter()
            .all(|(candidate, _)| *candidate != key),
        "the desired placement converged"
    );
    drop(metadata);
    runtime.close().unwrap();
}

/// A wake that concerns only the executor runs only the executor's pass: no
/// placement, handoff or journal-publish phase. A full tick that is due still
/// serves the executor's work in its own executor phase.
#[test]
fn executor_wakes_run_only_the_executor_pass_and_full_ticks_still_serve_it() {
    use crate::maintenance::phase_runs;
    let name = "native-executor-only-turn";
    let (runtime, service, shard) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    drop(service);
    let node = shard.primary_node_id.clone();
    let root = runtime.data_dir(&node).root();
    let phases = || {
        [
            phase_runs::HANDOFF,
            phase_runs::PLACEMENT,
            phase_runs::JOURNAL_PUBLISH,
            phase_runs::FULL_TICK_EXECUTOR_PASS,
        ]
        .map(|phase| phase_runs::count(&root, phase))
    };
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&node)).unwrap();
    let idle = || !repair_executor_work(&root).due && !scheduler.global_is_exclusively_held();
    // Startup's own turns finish first: the counts hold still while idle.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let before = phases();
        std::thread::sleep(Duration::from_millis(300));
        if idle() && phases() == before {
            break;
        }
        assert!(Instant::now() < deadline, "the fixture never became idle");
    }
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let shard_root =
        crate::default_dfs_metadata_shards_root(runtime.data_dir(&node)).join(&shard.shard_id);
    let object = DfsObjectId::from_legacy_named("obj_executor_only_turn").unwrap();
    let key = (
        object.clone(),
        DfsVersion::from_legacy_named("version_executor_only_turn").unwrap(),
        node.clone(),
    );
    let wakes: [(&str, Box<dyn Fn()>); 4] = [
        (
            "repair index change",
            Box::new(|| {
                crate::repair_executor::repair_index_changed(
                    &shard_root,
                    &shard_id,
                    crate::repair_executor::DfsRepairIndexChange::Objects(
                        [object.clone()].into_iter().collect(),
                    ),
                )
            }),
        ),
        (
            "attempt outcome",
            Box::new(|| {
                crate::repair_executor::repair_executor_attempt_started(
                    &node,
                    key.clone(),
                    "executor-only-turn-attempt",
                    0,
                );
                crate::repair_executor::repair_executor_attempt_ended(
                    &node,
                    &key,
                    "executor-only-turn-attempt",
                    crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                )
            }),
        ),
        (
            "reachability change",
            Box::new(|| {
                crate::repair_executor::mark_repair_executor_parked_for_test(&root);
                crate::repair_executor::repair_executor_reachability_changed(&root);
            }),
        ),
        (
            "primary assignment",
            Box::new(|| crate::repair_executor::wake_repair_executor(&root, "primary_assignment")),
        ),
    ];
    for (wake, raise) in &wakes {
        let before = phases();
        let turns = phase_runs::count(&root, phase_runs::EXECUTOR_TURN);
        raise();
        let deadline = Instant::now() + Duration::from_secs(10);
        while phase_runs::count(&root, phase_runs::EXECUTOR_TURN) == turns || !idle() {
            assert!(
                Instant::now() < deadline,
                "the {wake} wake ran no executor turn"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // Anything the turn set off would start right after it.
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            phases(),
            before,
            "the {wake} wake ran a placement, handoff, journal-publish or full-tick phase"
        );
    }
    // Executor work that no executor turn served is served by a due full tick.
    let turns = phase_runs::count(&root, phase_runs::EXECUTOR_TURN);
    let full_passes = phase_runs::count(&root, phase_runs::FULL_TICK_EXECUTOR_PASS);
    crate::repair_executor::repair_executor_timer_fired(&root);
    assert!(repair_executor_work(&root).due);
    // A bounded turn resumes where the last one stopped; the turn that reaches
    // its repair phase runs the executor's pass.
    for _ in 0..16 {
        runtime
            .maintenance_tick(&node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap()
            .outcome
            .unwrap();
        if phase_runs::count(&root, phase_runs::FULL_TICK_EXECUTOR_PASS) > full_passes {
            break;
        }
    }
    assert!(
        phase_runs::count(&root, phase_runs::FULL_TICK_EXECUTOR_PASS) > full_passes,
        "the full tick runs its own executor phase"
    );
    assert!(
        !repair_executor_work(&root).due,
        "the full tick's executor phase serves the pending work"
    );
    assert_eq!(phase_runs::count(&root, phase_runs::EXECUTOR_TURN), turns);
    runtime.close().unwrap();
}

#[test]
fn a_repair_reply_that_cannot_publish_still_ends_its_attempt_as_failed() {
    let root = sinomo_testdir::TestDir::new("repair-executor-reply-error");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let node = NodeId::from_legacy_named("node_repair_executor_reply_error").unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let _wake = register_repair_executor_wake(&data_root, Some(&node), move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let target = NodeId::from_legacy_named("node_repair_executor_reply_error_target").unwrap();
    let object_id = DfsObjectId::from_legacy_named("obj_repair_executor_reply_error").unwrap();
    let version = DfsVersion::from_legacy_named("version_repair_executor_reply_error").unwrap();
    let key = (object_id.clone(), version.clone(), target.clone());
    crate::repair_executor::repair_executor_attempt_started(&node, key, "repair-request", 0);
    // A node without its shard tables cannot publish the reply's receipt.
    let mut service = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node.clone()),
        LocalContentStore::new(root.path().join("content")).unwrap(),
    );
    let reply = service.handle_repair_replica_response(
        target.clone(),
        crate::DfsRepairReplicaResponse {
            request_id: "repair-request".into(),
            intent_id: "intent-repair-reply-error".into(),
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").unwrap(),
            object_id,
            version,
            replica_node_id: target,
            result: crate::DfsBlobTransferResult::Committed {
                content_hash: "blake3:reply-error".into(),
            },
            receipt_generation: 0,
        },
    );
    assert!(reply.is_err(), "publication fails without the shard tables");
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the failed publication ends the attempt and wakes the executor"
    );
    assert!(repair_executor_work(&data_root).due);
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn a_file_sync_rerun_keeps_its_wait_deadline_and_no_later_command_inherits_it() {
    let name = "native-file-sync-carried-deadline";
    let (runtime, owner, replica, mut service) = native_missing_content_repair(name);
    let path = format!("/services/{name}.bin");
    // The private owner keeps the node's maintenance, and so its repair
    // executor, from serving the missing replica: a file sync waits for it
    // until the sync's deadline.
    let (execution, hold) = native_private_repair_owner(&runtime, &service, &path, ());
    service
        .posix_write_file(
            &path,
            b"carried deadline",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), replica.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let target = service
        .posix_capture_file_sync_target(&path, crate::DfsPosixSyncKind::File)
        .unwrap();
    let completed_wait = || crate::source_adoption::CompletedRemoteNamespacePhase::RepairProgress {
        object_id: target.object_id.clone(),
        version: target.version.clone(),
        deadline: Some(Instant::now()),
    };
    // The re-run after the repair wait ends at the original deadline, not
    // at a fresh one.
    service
        .complete_remote_namespace_phase(completed_wait())
        .unwrap();
    let started = Instant::now();
    let rerun = service.posix_sync_file_local(&target, Duration::from_secs(30));
    assert!(
        matches!(rerun, Err(crate::DfsPosixError::PendingWrite { .. })),
        "the re-run ends at its deadline with the replica still missing: {rerun:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the re-run kept the original deadline"
    );
    // A re-run that does not reach the file sync leaves no deadline for a
    // later command on the same lane.
    service
        .complete_remote_namespace_phase(completed_wait())
        .unwrap();
    let lookup = crate::DfsMessageCommand {
        request_id: "file-sync-carried-deadline-lookup".into(),
        routed_message: crate::DfsRoutedMessage {
            source_node_id: replica.clone(),
            target_node_id: owner.clone(),
            frame_id: 1,
            message_id: 1,
            message: DfsNetworkMessage::NamespaceRequest(crate::DfsNamespaceCommand {
                request_id: "file-sync-carried-deadline-lookup".into(),
                file_system_id: crate::sifs_file_system_id(),
                requesting_node_id: replica.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: crate::DfsNamespaceOperation::LookupPath {
                    path: "/services".into(),
                    inline_range: None,
                },
            }),
        },
    };
    drop(service.process_message_command_report_with_native_return(lookup, None));
    assert!(service.file_sync_wait_deadline.is_none());
    drop(hold);
    drop(execution);
    drop(service);
    runtime.close().unwrap();
}
