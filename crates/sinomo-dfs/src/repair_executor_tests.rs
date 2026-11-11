use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn objects(name: &str) -> DfsRepairIndexChange {
    DfsRepairIndexChange::Objects(BTreeSet::from([crate::DfsObjectId::from_legacy_named(
        name,
    )
    .unwrap()]))
}

#[test]
fn index_changes_reach_the_registered_runtime_only_for_primary_shards() {
    let root = sinomo_testdir::TestDir::new("repair-executor-wakes");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let registration = register_repair_executor_wake(&data_root, None, move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        !repair_executor_work(&data_root).due,
        "registration alone is no work: the first assignment observation is"
    );
    let services = DfsMetadataShardId::new("services");
    let users = DfsMetadataShardId::new("users");
    let shard_root = |shard: &str| data_root.join("dfs").join("metadata").join(shard);
    repair_index_changed(&shard_root("services"), &services, objects("obj_a"));
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "before its first pass the executor reads every primary index anyway"
    );
    *lock(&executor(&data_root).primary_shards) = Some(BTreeSet::from(["services".into()]));
    repair_index_changed(&shard_root("users"), &users, objects("obj_a"));
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "a replica shard wakes nobody"
    );
    repair_index_changed(
        &root.path().with_file_name("another-node").join("dfs"),
        &services,
        objects("obj_a"),
    );
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "another node's shard does not"
    );
    repair_index_changed(&shard_root("services"), &services, objects("obj_a"));
    repair_index_changed(&shard_root("services"), &services, objects("obj_b"));
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    let events = std::mem::take(&mut *lock(&executor(&data_root).events));
    assert_eq!(
        events.changed.get("services").map(BTreeSet::len),
        Some(2),
        "the wake carries the changed objects, not a traversal"
    );

    drop(registration);
    repair_index_changed(&shard_root("services"), &services, objects("obj_c"));
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        2,
        "a stopped runtime receives no wake"
    );
    // Its tracked needs and timing end with it; a wake it did not serve stays
    // pending for the runtime that registers next, whose first pass reads the
    // index afresh.
    let work = repair_executor_work(&data_root);
    assert!(
        work.due && work.tracked_need_count == 0 && work.next_due_unix_nanos.is_none(),
        "a stopped node keeps no executor state but its pending wake: {work:?}"
    );
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn the_first_assignment_observation_and_every_gain_wake_the_executor() {
    let mut observed = None;
    let assigned = |epoch| BTreeMap::from([("services".to_string(), epoch)]);
    assert!(
        repair_primary_assignments_changed(&mut observed, assigned(1)),
        "the first observation after process start reads the index"
    );
    assert!(!repair_primary_assignments_changed(
        &mut observed,
        assigned(1)
    ));
    assert!(
        repair_primary_assignments_changed(&mut observed, assigned(2)),
        "a successor assignment wakes the executor"
    );
    assert!(
        !repair_primary_assignments_changed(&mut observed, BTreeMap::new()),
        "a lost assignment brings no work"
    );
    assert!(repair_primary_assignments_changed(
        &mut observed,
        assigned(2)
    ));
    let mut none_at_start = None;
    assert!(
        !repair_primary_assignments_changed(&mut none_at_start, BTreeMap::new()),
        "a node primary for nothing has no first pass to run"
    );
}

#[test]
fn reachability_and_authority_edges_are_recorded_and_wake_only_waiting_executors() {
    let root = sinomo_testdir::TestDir::new("repair-executor-edges");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let _registration = register_repair_executor_wake(&data_root, None, move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    repair_executor_reachability_changed(&data_root);
    repair_executor_authority_changed(&data_root);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "nothing waits for either edge and no pass runs"
    );
    let events = std::mem::take(&mut *lock(&executor(&data_root).events));
    assert!(
        events.reachability_changed && events.authority_changed,
        "both edges are recorded for the next pass"
    );
    {
        let executor = executor(&data_root);
        let mut work = lock(&executor.work);
        work.parked = true;
        work.awaiting_authority = true;
    }
    repair_executor_reachability_changed(&data_root);
    repair_executor_authority_changed(&data_root);
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    let events = std::mem::take(&mut *lock(&executor(&data_root).events));
    assert!(events.reachability_changed && events.authority_changed);
    reset_repair_executor_for_test(&data_root);
}

/// An index whose reads fail, for the executor's transactional pass.
struct FailingIndex;

impl DfsRepairNeedSource for FailingIndex {
    fn needs_for_objects(
        &self,
        _shard: &str,
        _objects: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Err(DfsError::Metadata {
            message: "injected index read failure".into(),
        })
    }
    fn needs_for_keys(
        &self,
        _shard: &str,
        _keys: &[DfsRepairNeedKey],
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(Vec::new())
    }
    fn page(
        &self,
        _shard: &str,
        _after: Option<&DfsRepairNeedKey>,
        _limit: usize,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(Vec::new())
    }
    fn need_count(&self, _shard: &str) -> Result<usize, DfsError> {
        Ok(0)
    }
}

#[test]
fn a_pass_whose_index_read_fails_keeps_its_events_and_stays_pending() {
    let executor = DfsRepairExecutor::default();
    let object = crate::DfsObjectId::from_legacy_named("obj_repair_executor_read_failure").unwrap();
    {
        let mut events = lock(&executor.events);
        events
            .changed
            .entry("services".into())
            .or_default()
            .insert(object.clone());
        events.reachability_changed = true;
    }
    let mut scheduler = lock(&executor.scheduler);
    let none = DfsMaintenanceExecutionExclusions::default();
    let result = executor.pass_events(
        &mut scheduler,
        &FailingIndex,
        1,
        BTreeMap::from([("services".to_owned(), 1)]),
        &none,
        8,
    );
    let result = result.map(|(selection, _)| selection);
    assert!(result.is_err());
    assert!(
        executor.pending.load(Ordering::SeqCst),
        "the retry runs the pass again"
    );
    // An event that arrived during the failed pass is kept with the restored.
    let events = std::mem::take(&mut *lock(&executor.events));
    assert_eq!(
        events.changed.get("services"),
        Some(&BTreeSet::from([object])),
        "the failed pass's events return for the retry"
    );
    assert!(events.reachability_changed);
}

/// One shard's index holding a single need, for executor-level passes.
struct OneNeedIndex(DfsRepairNeedKey, DfsRepairNeed);

impl DfsRepairNeedSource for OneNeedIndex {
    fn needs_for_objects(
        &self,
        _shard: &str,
        objects: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(objects
            .contains(&self.0.0)
            .then(|| (self.0.clone(), self.1.clone()))
            .into_iter()
            .collect())
    }
    fn needs_for_keys(
        &self,
        _shard: &str,
        keys: &[DfsRepairNeedKey],
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(keys
            .contains(&self.0)
            .then(|| (self.0.clone(), self.1.clone()))
            .into_iter()
            .collect())
    }
    fn page(
        &self,
        _shard: &str,
        after: Option<&DfsRepairNeedKey>,
        _limit: usize,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(after
            .is_none_or(|after| *after < self.0)
            .then(|| (self.0.clone(), self.1.clone()))
            .into_iter()
            .collect())
    }
    fn need_count(&self, _shard: &str) -> Result<usize, DfsError> {
        Ok(1)
    }
}

/// Test support: park one need on `target` in `data_root`'s executor under
/// `exclusions`, and publish the work a pass would: a failed attempt, then a
/// pass at its backoff's end that finds the target excluded.
pub(crate) fn park_one_repair_need_for_test(
    data_root: &Path,
    target: &NodeId,
    exclusions: &DfsMaintenanceExecutionExclusions,
) {
    let executor = executor(data_root);
    let key = (
        crate::DfsObjectId::from_legacy_named("obj_repair_executor_parked_need").unwrap(),
        crate::DfsVersion::from_legacy_named("version_repair_executor_parked_need").unwrap(),
        target.clone(),
    );
    let index = OneNeedIndex(
        key,
        DfsRepairNeed {
            file_system_id: crate::DfsFileSystemId::from_legacy_named("fs_parked_need").unwrap(),
            content_hash: "blake3:parked-need".into(),
            owner_intent_id: Some("intent_parked_need".into()),
            root: crate::metadata_summary::RepairRoot::Permanent,
            owner_root: Some(crate::metadata_summary::RepairRoot::Permanent),
            receipt_generation: 0,
            enrollment_blocked_by: None,
        },
    );
    let primaries = || BTreeMap::from([("services".to_owned(), 1)]);
    let mut scheduler = lock(&executor.scheduler);
    let now = crate::metadata_records::dfs_now_unix_nanos();
    let none = DfsMaintenanceExecutionExclusions::default();
    let (selection, _) = executor
        .pass_events(&mut scheduler, &index, now, primaries(), &none, 8)
        .unwrap();
    scheduler.record_dispatch(
        &selection.candidates[0].key,
        DfsRepairDispatchResult::SendFailed {
            error: "unreachable".into(),
        },
        now,
    );
    let backoff_end = scheduler.next_due().unwrap();
    executor
        .pass_events(
            &mut scheduler,
            &index,
            backoff_end,
            primaries(),
            exclusions,
            8,
        )
        .unwrap();
    assert!(scheduler.has_parked_needs());
    *lock(&executor.work) = DfsRepairExecutorWork {
        due: false,
        need_count: scheduler.need_count(),
        tracked_need_count: scheduler.tracked_count(),
        in_flight_count: scheduler.in_flight_count(),
        backoff_count: scheduler.backoff_count(),
        parked: true,
        awaiting_authority: false,
        next_due_unix_nanos: scheduler.next_due(),
    };
    executor.pending.store(false, Ordering::SeqCst);
}

#[test]
fn a_reachability_event_newer_than_the_view_a_need_parked_on_is_served_again() {
    let executor = DfsRepairExecutor::default();
    let target = NodeId::from_legacy_named("node_repair_executor_stale_view").unwrap();
    let key = (
        crate::DfsObjectId::from_legacy_named("obj_repair_executor_stale_view").unwrap(),
        crate::DfsVersion::from_legacy_named("version_repair_executor_stale_view").unwrap(),
        target.clone(),
    );
    let index = OneNeedIndex(
        key.clone(),
        DfsRepairNeed {
            file_system_id: crate::DfsFileSystemId::from_legacy_named("fs_stale_view").unwrap(),
            content_hash: "blake3:stale-view".into(),
            owner_intent_id: Some("intent_stale_view".into()),
            root: crate::metadata_summary::RepairRoot::Permanent,
            owner_root: Some(crate::metadata_summary::RepairRoot::Permanent),
            receipt_generation: 0,
            enrollment_blocked_by: None,
        },
    );
    let primaries = || BTreeMap::from([("services".to_owned(), 1)]);
    let mut scheduler = lock(&executor.scheduler);
    let none = DfsMaintenanceExecutionExclusions::default();
    let (selection, _) = executor
        .pass_events(&mut scheduler, &index, 1, primaries(), &none, 8)
        .unwrap();
    scheduler.record_dispatch(
        &selection.candidates[0].key,
        DfsRepairDispatchResult::SendFailed {
            error: "unreachable".into(),
        },
        1,
    );
    let backoff_end = scheduler.next_due().unwrap();
    // The reachability view was read before the latest event; the pass
    // consumes that event and parks the need on the older view.
    lock(&executor.events).reachability_changed = true;
    executor.reachability_epoch.store(5, Ordering::SeqCst);
    let older_view =
        DfsMaintenanceExecutionExclusions::new([target.clone()]).with_repair_reachability_epoch(4);
    let (_, edges) = executor
        .pass_events(
            &mut scheduler,
            &index,
            backoff_end,
            primaries(),
            &older_view,
            8,
        )
        .unwrap();
    assert!(scheduler.has_parked_needs());
    assert!(executor.rewake_for_edges(&scheduler, &edges));
    assert!(lock(&executor.events).reachability_changed);
    assert!(executor.pending.load(Ordering::SeqCst));
    // A pass on a view read after every event consumes it for good.
    executor.pending.store(false, Ordering::SeqCst);
    let current_view =
        DfsMaintenanceExecutionExclusions::new([target]).with_repair_reachability_epoch(5);
    let (_, edges) = executor
        .pass_events(
            &mut scheduler,
            &index,
            backoff_end,
            primaries(),
            &current_view,
            8,
        )
        .unwrap();
    assert!(!executor.rewake_for_edges(&scheduler, &edges));
    assert!(!lock(&executor.events).reachability_changed);
    // An event raised while a pass runs, after it took its events, wakes the
    // pass after it once the parked work is published.
    let (_, edges) = executor
        .pass_events(
            &mut scheduler,
            &index,
            backoff_end,
            primaries(),
            &current_view,
            8,
        )
        .unwrap();
    executor.reachability_epoch.fetch_add(1, Ordering::SeqCst);
    assert!(executor.rewake_for_edges(&scheduler, &edges));
}

#[test]
fn an_authority_event_newer_than_the_view_a_turn_dispatched_with_is_served_again() {
    let executor = DfsRepairExecutor::default();
    let target = NodeId::from_legacy_named("node_repair_executor_stale_authority").unwrap();
    let key = (
        crate::DfsObjectId::from_legacy_named("obj_repair_executor_stale_authority").unwrap(),
        crate::DfsVersion::from_legacy_named("version_repair_executor_stale_authority").unwrap(),
        target,
    );
    let index = OneNeedIndex(
        key.clone(),
        DfsRepairNeed {
            file_system_id: crate::DfsFileSystemId::from_legacy_named("fs_stale_authority")
                .unwrap(),
            content_hash: "blake3:stale-authority".into(),
            owner_intent_id: Some("intent_stale_authority".into()),
            root: crate::metadata_summary::RepairRoot::Permanent,
            owner_root: Some(crate::metadata_summary::RepairRoot::Permanent),
            receipt_generation: 0,
            enrollment_blocked_by: None,
        },
    );
    let primaries = || BTreeMap::from([("services".to_owned(), 1)]);
    let mut scheduler = lock(&executor.scheduler);
    let lagging = DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::AwaitingAuthority);
    let none = DfsMaintenanceExecutionExclusions::default();
    let (selection, _) = executor
        .pass_events(&mut scheduler, &index, 1, primaries(), &none, 8)
        .unwrap();
    scheduler.record_dispatch(&selection.candidates[0].key, lagging.clone(), 1);
    assert!(scheduler.has_awaiting_authority());
    // The turn captured its authority view before the latest event; the pass
    // consumes that event, and dispatch on the older view refuses again.
    repair_executor_authority_changed_on(&executor);
    let older_view = DfsMaintenanceExecutionExclusions::default().with_repair_authority_epoch(0);
    let (selection, edges) = executor
        .pass_events(&mut scheduler, &index, 1, primaries(), &older_view, 8)
        .unwrap();
    scheduler.record_dispatch(&selection.candidates[0].key, lagging.clone(), 1);
    assert!(executor.rewake_for_edges(&scheduler, &edges));
    assert!(
        lock(&executor.events).authority_changed,
        "the re-wake carries the authority event"
    );
    // A turn whose view was captured after every event consumes it for good.
    executor.pending.store(false, Ordering::SeqCst);
    let current_view = DfsMaintenanceExecutionExclusions::default()
        .with_repair_authority_epoch(executor.authority_epoch.load(Ordering::SeqCst));
    let (selection, edges) = executor
        .pass_events(&mut scheduler, &index, 1, primaries(), &current_view, 8)
        .unwrap();
    scheduler.record_dispatch(&selection.candidates[0].key, lagging, 1);
    assert!(!executor.rewake_for_edges(&scheduler, &edges));
    assert!(!lock(&executor.events).authority_changed);
}

/// Record and count one authority event on `executor`, as the published
/// configuration's edge does.
fn repair_executor_authority_changed_on(executor: &DfsRepairExecutor) {
    let mut events = lock(&executor.events);
    events.authority_changed = true;
    executor.authority_epoch.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn a_durability_wait_sees_a_node_that_only_its_first_pass_binds() {
    let root = sinomo_testdir::TestDir::new("repair-executor-pass-binding");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let node = NodeId::from_legacy_named("node_repair_executor_pass_binding").unwrap();
    // The runtime registered without knowing its node, as when the cluster
    // state could not be read at maintenance start.
    let registration = register_repair_executor_wake(&data_root, None, || {});
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let waiter = {
        let node = node.clone();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let progressed = repair_executor_wait_for_progress(&node, 0, deadline);
            (progressed, started.elapsed())
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(100));
    let (executor, _) = bind_pass_executor(&node, &data_root);
    std::thread::sleep(std::time::Duration::from_millis(100));
    advance_progress(&executor);
    let (progressed, waited) = waiter.join().unwrap();
    assert!(progressed);
    assert!(
        waited < std::time::Duration::from_secs(10),
        "the wait followed the pass's binding instead of sleeping to its deadline"
    );
    drop(registration);
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn registration_binds_the_node_and_delivers_a_wake_raised_before_it() {
    let root = sinomo_testdir::TestDir::new("repair-executor-early-wake");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let node = NodeId::from_legacy_named("node_repair_executor_early_wake").unwrap();
    // The assignment watcher's first wake can precede the maintenance task.
    wake_repair_executor(&data_root, "primary_assignment");
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let registration = register_repair_executor_wake(&data_root, Some(&node), move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the early wake is delivered"
    );
    assert_eq!(
        repair_executor_progress(&node),
        Some(0),
        "progress and demand reach the executor before its first pass"
    );
    // A wake raised on the stopping runtime's executor and not served stays
    // pending for the runtime that registers next.
    wake_repair_executor(&data_root, "primary_assignment");
    drop(registration);
    let restarted = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&restarted);
    let _registration = register_repair_executor_wake(&data_root, Some(&node), move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(restarted.load(Ordering::SeqCst), 1);
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn a_durability_wait_ends_when_its_runtime_stops() {
    let root = sinomo_testdir::TestDir::new("repair-executor-wait-shutdown");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let node = NodeId::from_legacy_named("node_repair_executor_wait_shutdown").unwrap();
    let registration = register_repair_executor_wake(&data_root, Some(&node), || {});
    let seen = repair_executor_progress(&node).unwrap();
    let waiter_node = node.clone();
    let started = std::time::Instant::now();
    let waiter = std::thread::spawn(move || {
        repair_executor_wait_for_progress(
            &waiter_node,
            seen,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        )
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    drop(registration);
    assert!(!waiter.join().unwrap(), "no progress was made");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the wait returned at shutdown, not at its deadline"
    );
    reset_repair_executor_for_test(&data_root);
}

#[test]
fn a_second_producer_does_not_replace_an_attempt_in_flight() {
    let root = sinomo_testdir::TestDir::new("repair-executor-two-producers");
    let data_root = root.path().to_path_buf();
    reset_repair_executor_for_test(&data_root);
    let node = NodeId::from_legacy_named("node_repair_executor_two_producers").unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    let _registration = register_repair_executor_wake(&data_root, Some(&node), move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let key = (
        crate::DfsObjectId::from_legacy_named("obj_repair_executor_two_producers").unwrap(),
        crate::DfsVersion::from_legacy_named("version_repair_executor_two_producers").unwrap(),
        NodeId::from_legacy_named("node_repair_executor_two_producers_target").unwrap(),
    );
    // The executor's own attempt is in flight when a write sends the same
    // replica beyond its commit quorum.
    repair_executor_attempt_started(&node, key.clone(), "executor-request", 3);
    repair_executor_attempt_started(&node, key.clone(), "write-request", 3);
    repair_executor_attempt_ended(
        &node,
        &key,
        "executor-request",
        DfsRepairAttemptOutcome::Failed,
    );
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the executor's reply still ends its attempt"
    );
    reset_repair_executor_for_test(&data_root);
}
