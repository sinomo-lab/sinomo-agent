use super::*;
use crate::metadata_summary::RepairRoot;
use crate::{DfsFileSystemId, DfsVersion};
use std::cell::RefCell;

const SHARD: &str = "services";

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn key(object: usize, target: &NodeId) -> DfsRepairNeedKey {
    (
        DfsObjectId::from_legacy_named(format!("obj_scheduler_{object:05}")).unwrap(),
        DfsVersion::from_legacy_named("version_scheduler").unwrap(),
        target.clone(),
    )
}

fn need(generation: u64) -> DfsRepairNeed {
    DfsRepairNeed {
        file_system_id: DfsFileSystemId::from_legacy_named("fs_scheduler").unwrap(),
        content_hash: "blake3:scheduler".into(),
        owner_intent_id: Some("intent_scheduler".into()),
        root: RepairRoot::Permanent,
        owner_root: Some(RepairRoot::Permanent),
        receipt_generation: generation,
        enrollment_blocked_by: None,
    }
}

/// An in-memory index for one shard that counts the entries it returns.
#[derive(Default)]
struct FakeIndex {
    needs: RefCell<BTreeMap<DfsRepairNeedKey, DfsRepairNeed>>,
    returned: RefCell<usize>,
    /// Key reads left before one fails, when set.
    key_reads_before_failure: RefCell<Option<usize>>,
}

impl FakeIndex {
    fn returned(&self, entries: &[(DfsRepairNeedKey, DfsRepairNeed)]) {
        *self.returned.borrow_mut() += entries.len();
    }
}

impl DfsRepairNeedSource for FakeIndex {
    fn needs_for_objects(
        &self,
        _shard: &str,
        objects: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        let found = self
            .needs
            .borrow()
            .iter()
            .filter(|(key, _)| objects.contains(&key.0))
            .map(|(key, need)| (key.clone(), need.clone()))
            .collect::<Vec<_>>();
        self.returned(&found);
        Ok(found)
    }

    fn needs_for_keys(
        &self,
        _shard: &str,
        keys: &[DfsRepairNeedKey],
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        if let Some(left) = self.key_reads_before_failure.borrow_mut().as_mut() {
            if *left == 0 {
                return Err(DfsError::Metadata {
                    message: "injected index read failure".into(),
                });
            }
            *left -= 1;
        }
        let needs = self.needs.borrow();
        let found = keys
            .iter()
            .filter_map(|key| needs.get(key).map(|need| (key.clone(), need.clone())))
            .collect::<Vec<_>>();
        self.returned(&found);
        Ok(found)
    }

    fn page(
        &self,
        _shard: &str,
        after: Option<&DfsRepairNeedKey>,
        limit: usize,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        let needs = self.needs.borrow();
        let found = needs
            .iter()
            .filter(|(key, _)| after.is_none_or(|after| *key > after))
            .take(limit)
            .map(|(key, need)| (key.clone(), need.clone()))
            .collect::<Vec<_>>();
        self.returned(&found);
        Ok(found)
    }

    fn need_count(&self, _shard: &str) -> Result<usize, DfsError> {
        Ok(self.needs.borrow().len())
    }
}

fn input<'a>(
    now: u64,
    exclusions: &'a DfsMaintenanceExecutionExclusions,
    changed: &[DfsRepairNeedKey],
    outcomes: Vec<(DfsRepairNeedKey, Option<String>, DfsRepairAttemptOutcome)>,
) -> DfsRepairPassInput<'a> {
    let mut changed_objects = BTreeMap::<String, BTreeSet<DfsObjectId>>::new();
    for key in changed {
        changed_objects
            .entry(SHARD.to_owned())
            .or_default()
            .insert(key.0.clone());
    }
    DfsRepairPassInput {
        now,
        primaries: BTreeMap::from([(SHARD.to_owned(), 1)]),
        exclusions,
        changed: changed_objects,
        replaced: BTreeSet::new(),
        outcomes: outcomes
            .into_iter()
            .map(|(key, request_id, outcome)| (key, request_id, outcome, None))
            .collect(),
        authority_changed: false,
        demanded: BTreeSet::new(),
        limit: 8,
    }
}

fn dispatch_all(scheduler: &mut DfsRepairScheduler, selection: &DfsRepairExecutorSelection) {
    for candidate in &selection.candidates {
        scheduler.record_dispatch(
            &candidate.key,
            DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::Failed),
            1,
        );
    }
}

#[test]
fn a_gained_assignment_reads_the_whole_index_once_and_admits_up_to_capacity() {
    let target = node("node_scheduler_target");
    let index = FakeIndex::default();
    for object in 0..40 {
        index
            .needs
            .borrow_mut()
            .insert(key(object, &target), need(0));
    }
    let mut scheduler = DfsRepairScheduler::default();
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut selected = 0;
    let mut passes = 0;
    loop {
        let selection = scheduler
            .pass(&index, input(1, &none, &[], Vec::new()))
            .unwrap();
        selected += selection.candidates.len();
        passes += 1;
        if !scheduler.has_ready_work() {
            break;
        }
    }
    assert_eq!(
        selected, DFS_REPAIR_EXECUTOR_MAX_IN_FLIGHT,
        "capacity bounds outstanding attempts"
    );
    assert!(
        passes <= 5,
        "the startup traversal continues within its wake"
    );
    assert_eq!(scheduler.tracked_count(), 40);
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert!(
        selection.candidates.is_empty() && !scheduler.has_ready_work(),
        "no capacity: an outcome, not the pass, admits the next need"
    );
}

#[test]
fn index_changes_cost_proportional_to_the_change_not_to_parked_needs() {
    const K: usize = 200;
    const M: usize = 10;
    let offline = node("node_scheduler_offline");
    let online = node("node_scheduler_online");
    let index = FakeIndex::default();
    for object in 0..K {
        index
            .needs
            .borrow_mut()
            .insert(key(object, &offline), need(0));
    }
    let unreachable = DfsMaintenanceExecutionExclusions::new([offline.clone()]);
    let mut scheduler = DfsRepairScheduler::default();
    // Startup: every parked need is attempted once and fails against the
    // unreachable target, then parks.
    loop {
        let selection = scheduler
            .pass(&index, input(1, &unreachable, &[], Vec::new()))
            .unwrap();
        dispatch_all(&mut scheduler, &selection);
        if selection.candidates.is_empty() && !scheduler.has_ready_work() {
            break;
        }
    }
    // Backoff ends: each failed need parks on its unreachable target.
    let late = u64::MAX / 2;
    loop {
        let selection = scheduler
            .pass(&index, input(late, &unreachable, &[], Vec::new()))
            .unwrap();
        dispatch_all(&mut scheduler, &selection);
        if selection.candidates.is_empty() && !scheduler.has_ready_work() {
            break;
        }
    }
    assert!(scheduler.has_parked_needs());
    assert_eq!(
        scheduler.next_due(),
        None,
        "nothing polls an unreachable target"
    );

    let before = *index.returned.borrow();
    let examined_before = scheduler.examined_total;
    for change in 0..M {
        let new_key = key(K + change, &online);
        index.needs.borrow_mut().insert(new_key.clone(), need(0));
        let selection = scheduler
            .pass(&index, input(late, &unreachable, &[new_key], Vec::new()))
            .unwrap();
        assert_eq!(selection.candidates.len(), 1);
        scheduler.record_dispatch(
            &selection.candidates[0].key,
            DfsRepairDispatchResult::Sent {
                request_id: format!("request-{change}"),
            },
            late,
        );
    }
    let read = *index.returned.borrow() - before;
    let examined = scheduler.examined_total - examined_before;
    assert!(
        read <= 2 * M && examined <= 2 * M as u64,
        "{M} index changes read {read} and examined {examined} entries with {K} parked needs"
    );

    // The reachability edge releases exactly the parked needs of the target
    // that became reachable.
    let reachable = DfsMaintenanceExecutionExclusions::default();
    let mut edge = input(late, &reachable, &[], Vec::new());
    edge.limit = K;
    let selection = scheduler.pass(&index, edge).unwrap();
    assert_eq!(
        selection.candidates.len(),
        DFS_REPAIR_EXECUTOR_MAX_IN_FLIGHT - M,
        "released needs are admitted up to the concurrency bound"
    );
    assert!(
        !scheduler.has_ready_work(),
        "the other released needs wait for capacity, which an outcome frees"
    );
    assert!(!scheduler.has_parked_needs());
    assert_eq!(scheduler.tracked_count(), K + M);
}

#[test]
fn outcomes_reachability_and_authority_release_only_their_needs() {
    let target = node("node_scheduler_flaky");
    let other = node("node_scheduler_other");
    let index = FakeIndex::default();
    let failed = key(1, &target);
    let refused = key(2, &other);
    let superseded = key(3, &other);
    for key in [&failed, &refused, &superseded] {
        index.needs.borrow_mut().insert(key.clone(), need(0));
    }
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert_eq!(selection.candidates.len(), 3);
    for candidate in &selection.candidates {
        let result = if candidate.key == refused {
            DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::AwaitingAuthority)
        } else {
            DfsRepairDispatchResult::Sent {
                request_id: format!("request-{}", candidate.key.0),
            }
        };
        scheduler.record_dispatch(&candidate.key, result, 1);
    }
    // A stale reply to a replaced request changes nothing.
    let outcomes = vec![
        (
            failed.clone(),
            Some(format!("request-{}", failed.0)),
            DfsRepairAttemptOutcome::Failed,
        ),
        (
            superseded.clone(),
            Some("request-stale".into()),
            DfsRepairAttemptOutcome::Superseded,
        ),
    ];
    let selection = scheduler
        .pass(&index, input(2, &none, &[], outcomes))
        .unwrap();
    assert!(selection.candidates.is_empty());
    let due = scheduler.next_due().expect("the failed attempt backs off");
    assert!(due > 2);

    // The superseded reply re-plans at once, with no backoff.
    let selection = scheduler
        .pass(
            &index,
            input(
                2,
                &none,
                &[],
                vec![(
                    superseded.clone(),
                    Some(format!("request-{}", superseded.0)),
                    DfsRepairAttemptOutcome::Superseded,
                )],
            ),
        )
        .unwrap();
    assert_eq!(
        selection
            .candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect::<Vec<_>>(),
        vec![superseded.clone()]
    );

    // Superseded again at the same slot generation: the re-plan changed
    // nothing, so it backs off instead of re-sending in a loop.
    scheduler.record_dispatch(
        &superseded,
        DfsRepairDispatchResult::Sent {
            request_id: "request-again".into(),
        },
        2,
    );
    let selection = scheduler
        .pass(
            &index,
            input(
                2,
                &none,
                &[],
                vec![(
                    superseded.clone(),
                    Some("request-again".into()),
                    DfsRepairAttemptOutcome::Superseded,
                )],
            ),
        )
        .unwrap();
    assert!(
        selection.candidates.is_empty(),
        "a repeated supersession backs off"
    );

    // A refused dispatch waits for the authority edge and does not back off.
    let mut authority = input(3, &none, &[], Vec::new());
    authority.authority_changed = true;
    let selection = scheduler.pass(&index, authority).unwrap();
    assert_eq!(
        selection
            .candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect::<Vec<_>>(),
        vec![refused]
    );
}

#[test]
fn a_primary_change_restarts_timing_and_rereads_the_gained_shard() {
    let target = node("node_scheduler_successor");
    let index = FakeIndex::default();
    let need_key = key(1, &target);
    index.needs.borrow_mut().insert(need_key.clone(), need(0));
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    dispatch_all(&mut scheduler, &selection);
    assert!(scheduler.next_due().is_some());

    let mut lost = input(2, &none, &[], Vec::new());
    lost.primaries = BTreeMap::new();
    scheduler.pass(&index, lost).unwrap();
    assert_eq!(
        scheduler.tracked_count(),
        0,
        "a lost assignment drops its timing"
    );

    let mut gained = input(2, &none, &[], Vec::new());
    gained.primaries = BTreeMap::from([(SHARD.to_owned(), 2)]);
    let selection = scheduler.pass(&index, gained).unwrap();
    assert_eq!(
        selection.candidates.len(),
        1,
        "the successor reads its index and attempts at once"
    );
}

#[test]
fn a_durability_demand_goes_ahead_but_never_cuts_short_an_earned_backoff() {
    let target = node("node_scheduler_demand");
    let index = FakeIndex::default();
    let failed = key(1, &target);
    index.needs.borrow_mut().insert(failed.clone(), need(0));
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert_eq!(selection.candidates.len(), 1);
    scheduler.record_dispatch(
        &failed,
        DfsRepairDispatchResult::SendFailed {
            error: "rejected".into(),
        },
        1,
    );
    let backoff_end = scheduler.next_due().unwrap();
    assert!(backoff_end > 2);
    // A sync re-run demands the object again and again; none of those
    // demands sends the content before the backoff the failure earned ends.
    for _ in 0..5 {
        let mut demand = input(2, &none, &[], Vec::new());
        demand.demanded = BTreeSet::from([failed.0.clone()]);
        let selection = scheduler.pass(&index, demand).unwrap();
        assert!(
            selection.candidates.is_empty(),
            "a demand must not end an earned backoff"
        );
    }
    assert_eq!(scheduler.next_due(), Some(backoff_end));

    // A demanded need never attempted is admitted and goes ahead of needs
    // that were ready before it.
    let waiting = (2..6)
        .map(|object| key(object, &target))
        .collect::<Vec<_>>();
    for entry in &waiting {
        index.needs.borrow_mut().insert(entry.clone(), need(0));
    }
    let mut changed = input(2, &none, &waiting, Vec::new());
    changed.limit = 0;
    scheduler.pass(&index, changed).unwrap();
    let untracked = key(9, &target);
    index.needs.borrow_mut().insert(untracked.clone(), need(0));
    let mut demand = input(2, &none, &[], Vec::new());
    demand.demanded = BTreeSet::from([untracked.0.clone()]);
    demand.limit = 1;
    let selection = scheduler.pass(&index, demand).unwrap();
    assert_eq!(
        selection
            .candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect::<Vec<_>>(),
        vec![untracked],
        "the demanded need is read and attempted first"
    );
}

#[test]
fn ready_needs_are_served_in_arrival_order_under_sustained_arrivals() {
    let target = node("node_scheduler_fifo");
    let index = FakeIndex::default();
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    // The first need to become ready has the highest key.
    let first = key(999, &target);
    index.needs.borrow_mut().insert(first.clone(), need(0));
    let mut arrive = input(1, &none, std::slice::from_ref(&first), Vec::new());
    arrive.limit = 0;
    scheduler.pass(&index, arrive).unwrap();
    // Lower keys keep arriving, more than each pass serves.
    let mut served = Vec::new();
    for round in 0..3 {
        let arrivals = (round * 2..round * 2 + 2)
            .map(|object| key(object, &target))
            .collect::<Vec<_>>();
        for entry in &arrivals {
            index.needs.borrow_mut().insert(entry.clone(), need(0));
        }
        let mut pass = input(1, &none, &arrivals, Vec::new());
        pass.limit = 1;
        let selection = scheduler.pass(&index, pass).unwrap();
        for candidate in &selection.candidates {
            served.push(candidate.key.clone());
            scheduler.record_dispatch(
                &candidate.key,
                DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::Settled),
                1,
            );
        }
    }
    assert_eq!(
        served.first(),
        Some(&first),
        "needs are served in the order they became ready, whatever their keys"
    );
    assert_eq!(served.len(), 3);
}

#[test]
fn a_failed_index_read_leaves_no_need_holding_a_dispatch_slot() {
    let target = node("node_scheduler_read_failure");
    let index = FakeIndex::default();
    let present = key(1, &target);
    let gone = key(2, &target);
    let later = key(3, &target);
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    // `present` and `gone` become ready before `later`.
    for (arrivals, limit) in [
        (vec![present.clone(), gone.clone()], 0),
        (vec![later.clone()], 0),
    ] {
        for entry in &arrivals {
            index.needs.borrow_mut().insert(entry.clone(), need(0));
        }
        let mut changed = input(1, &none, &arrivals, Vec::new());
        changed.limit = limit;
        scheduler.pass(&index, changed).unwrap();
    }
    assert_eq!(scheduler.tracked_count(), 3);
    // The first selection batch admits `present` and finds `gone` removed;
    // the second batch's read fails.
    index.needs.borrow_mut().remove(&gone);
    *index.key_reads_before_failure.borrow_mut() = Some(1);
    let mut failing = input(1, &none, &[], Vec::new());
    failing.limit = 2;
    assert!(scheduler.pass(&index, failing).is_err());
    assert_eq!(
        scheduler.in_flight_count(),
        0,
        "a failed pass leaves no need dispatching"
    );
    *index.key_reads_before_failure.borrow_mut() = None;
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    let mut selected = selection
        .candidates
        .iter()
        .map(|candidate| candidate.key.clone())
        .collect::<Vec<_>>();
    selected.sort();
    let mut expected = vec![present, later];
    expected.sort();
    assert_eq!(selected, expected, "the retry admits every need again");
}

#[test]
fn ownerless_live_demand_stays_tracked_until_an_owner_or_authority_releases_it() {
    let target = node("node_scheduler_ownerless");
    let index = FakeIndex::default();
    let ownerless = key(1, &target);
    let mut demand = need(0);
    demand.owner_intent_id = None;
    demand.owner_root = None;
    index
        .needs
        .borrow_mut()
        .insert(ownerless.clone(), demand.clone());
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert!(selection.candidates.is_empty());
    assert_eq!(
        selection.ownerless,
        BTreeSet::from([(ownerless.0.clone(), ownerless.1.clone())])
    );
    for enrollment in [
        DfsRepairOwnerEnrollment::Declined,
        DfsRepairOwnerEnrollment::Enrolled,
    ] {
        scheduler.record_owner_enrollment(&ownerless.0, &ownerless.1, enrollment, 1);
        assert_eq!(scheduler.tracked_count(), 1, "the need stays tracked");
        let selection = scheduler
            .pass(&index, input(1, &none, &[], Vec::new()))
            .unwrap();
        assert!(
            selection.candidates.is_empty() && selection.ownerless.is_empty(),
            "only the change that enrolls an owner releases it"
        );
    }
    // A decline from a lagging authority view waits for the authority edge.
    scheduler.record_owner_enrollment(
        &ownerless.0,
        &ownerless.1,
        DfsRepairOwnerEnrollment::AuthorityLag,
        1,
    );
    assert!(scheduler.has_awaiting_authority());
    let mut caught_up = input(1, &none, &[], Vec::new());
    caught_up.authority_changed = true;
    let selection = scheduler.pass(&index, caught_up).unwrap();
    assert_eq!(selection.ownerless.len(), 1, "enrollment is asked again");
    // The index change that enrolled the owner admits the need.
    index.needs.borrow_mut().insert(ownerless.clone(), need(0));
    let selection = scheduler
        .pass(
            &index,
            input(1, &none, std::slice::from_ref(&ownerless), Vec::new()),
        )
        .unwrap();
    assert_eq!(selection.candidates.len(), 1);
    assert_eq!(selection.candidates[0].key, ownerless);
}

#[test]
fn a_parked_need_is_reevaluated_when_its_exclusion_evidence_expires() {
    let target = node("node_scheduler_expiring_exclusion");
    let index = FakeIndex::default();
    let parked = key(1, &target);
    index.needs.borrow_mut().insert(parked.clone(), need(0));
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    scheduler.record_dispatch(
        &selection.candidates[0].key,
        DfsRepairDispatchResult::SendFailed {
            error: "unreachable".into(),
        },
        1,
    );
    let backoff_end = scheduler.next_due().unwrap();
    let expires_at = backoff_end + 1_000;
    let excluded = DfsMaintenanceExecutionExclusions::new([target.clone()])
        .with_reevaluate_at_by_target(BTreeMap::from([(target.clone(), expires_at)]));
    scheduler
        .pass(&index, input(backoff_end, &excluded, &[], Vec::new()))
        .unwrap();
    assert!(scheduler.has_parked_needs());
    assert_eq!(
        scheduler.next_due(),
        Some(expires_at),
        "the evidence's expiry is the parked need's timer"
    );
    // Before expiry nothing is re-read; at expiry the target is no longer
    // excluded, and no reachability event was raised.
    let selection = scheduler
        .pass(&index, input(expires_at - 1, &excluded, &[], Vec::new()))
        .unwrap();
    assert!(selection.candidates.is_empty() && scheduler.has_parked_needs());
    let selection = scheduler
        .pass(&index, input(expires_at, &none, &[], Vec::new()))
        .unwrap();
    assert_eq!(selection.candidates.len(), 1);
    assert!(!scheduler.has_parked_needs());
}

#[test]
fn an_unrelated_targets_expiring_evidence_is_no_timer_for_a_parked_need() {
    let parked_on = node("node_scheduler_parked_target");
    let unrelated = node("node_scheduler_unrelated_target");
    let index = FakeIndex::default();
    let parked = key(1, &parked_on);
    index.needs.borrow_mut().insert(parked.clone(), need(0));
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    scheduler.record_dispatch(
        &selection.candidates[0].key,
        DfsRepairDispatchResult::SendFailed {
            error: "unreachable".into(),
        },
        1,
    );
    let backoff_end = scheduler.next_due().unwrap();
    // Both nodes are excluded; only the unrelated node's evidence expires.
    let unrelated_expiry = backoff_end + 1_000;
    let excluded = DfsMaintenanceExecutionExclusions::new([parked_on.clone(), unrelated.clone()])
        .with_reevaluate_at_by_target(BTreeMap::from([(unrelated.clone(), unrelated_expiry)]));
    scheduler
        .pass(&index, input(backoff_end, &excluded, &[], Vec::new()))
        .unwrap();
    assert!(scheduler.has_parked_needs());
    assert_eq!(
        scheduler.next_due(),
        None,
        "evidence about another node cannot release this need: no timer"
    );
    let examined = scheduler.examined_total;
    let selection = scheduler
        .pass(&index, input(unrelated_expiry, &excluded, &[], Vec::new()))
        .unwrap();
    assert!(selection.candidates.is_empty() && scheduler.has_parked_needs());
    assert_eq!(
        scheduler.examined_total, examined,
        "a pass at the unrelated expiry re-reads no parked need"
    );
}

#[test]
fn only_this_nodes_unreachable_records_about_an_excluded_target_can_expire_its_exclusion() {
    use sinomo_core::network::{ClusterConnectivityRecord, ClusterConnectivityState};
    let local = node("node_scheduler_expiry_local");
    let excluded = node("node_scheduler_expiry_excluded");
    let other = node("node_scheduler_expiry_other");
    let now = time::OffsetDateTime::now_utc();
    let record = |source: &NodeId, target: &NodeId, endpoint: &str, state, minutes| {
        ClusterConnectivityRecord {
            cluster_id: sinomo_core::node::ClusterId::from_legacy_named("cluster_scheduler_expiry")
                .unwrap(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: sinomo_core::Id::from_legacy_named(endpoint).unwrap(),
            transport: sinomo_core::ClusterTransportKind::LanTcp,
            address: format!("10.0.0.1:{}", 7000 + minutes),
            state,
            latency_ms: None,
            consecutive_failures: 3,
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::minutes(minutes),
        }
    };
    let graph = sinomo_core::network::ClusterConnectivityGraph::from_records(
        local.clone(),
        [
            // This node's own observation of the excluded target.
            record(
                &local,
                &excluded,
                "endpoint_excluded_a",
                ClusterConnectivityState::Unreachable,
                30,
            ),
            record(
                &local,
                &excluded,
                "endpoint_excluded_b",
                ClusterConnectivityState::Unreachable,
                20,
            ),
            // Evidence that expires sooner but cannot lift the exclusion.
            record(
                &local,
                &other,
                "endpoint_other",
                ClusterConnectivityState::Unreachable,
                1,
            ),
            record(
                &other,
                &excluded,
                "endpoint_other_excluded",
                ClusterConnectivityState::Unreachable,
                2,
            ),
            record(
                &excluded,
                &local,
                "endpoint_excluded_local",
                ClusterConnectivityState::Unreachable,
                3,
            ),
            record(
                &other,
                &local,
                "endpoint_other_local",
                ClusterConnectivityState::Healthy,
                4,
            ),
            record(
                &local,
                &excluded,
                "endpoint_excluded_c",
                ClusterConnectivityState::Degraded,
                5,
            ),
        ],
    );
    let expiries = crate::maintenance::dfs_exclusion_evidence_expiry(
        &graph,
        &local,
        &BTreeSet::from([excluded.clone()]),
    );
    let expected =
        u64::try_from((now + time::Duration::minutes(20)).unix_timestamp_nanos()).unwrap();
    assert_eq!(expiries, BTreeMap::from([(excluded, expected)]));
}

fn ownerless_need() -> DfsRepairNeed {
    let mut demand = need(0);
    demand.owner_intent_id = None;
    demand.owner_root = None;
    demand
}

#[test]
fn an_unfinished_write_that_blocks_enrollment_releases_the_need_when_it_ends() {
    let target = node("node_scheduler_blocked_enrollment");
    let index = FakeIndex::default();
    let waiting = key(1, &target);
    let mut blocked = ownerless_need();
    blocked.enrollment_blocked_by = Some("intent_unfinished".into());
    index
        .needs
        .borrow_mut()
        .insert(waiting.clone(), blocked.clone());
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert!(
        selection.candidates.is_empty() && selection.ownerless.is_empty(),
        "enrollment is not asked while the unfinished write owns the version"
    );
    assert_eq!(scheduler.tracked_count(), 1);
    // The write aborts: the need still has no owner, but the change of the
    // need reaches the executor and enrollment is asked.
    index
        .needs
        .borrow_mut()
        .insert(waiting.clone(), ownerless_need());
    let selection = scheduler
        .pass(
            &index,
            input(1, &none, std::slice::from_ref(&waiting), Vec::new()),
        )
        .unwrap();
    assert_eq!(
        selection.ownerless,
        BTreeSet::from([(waiting.0.clone(), waiting.1.clone())])
    );
    // Declined with nothing changed: it waits, and asks again only after the
    // next change of the need.
    scheduler.record_owner_enrollment(
        &waiting.0,
        &waiting.1,
        DfsRepairOwnerEnrollment::Declined,
        1,
    );
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert!(selection.ownerless.is_empty());
    let selection = scheduler
        .pass(
            &index,
            input(1, &none, std::slice::from_ref(&waiting), Vec::new()),
        )
        .unwrap();
    assert_eq!(selection.ownerless.len(), 1);
}

#[test]
fn a_failed_pass_keeps_the_enrollment_requests_of_its_earlier_batches() {
    let target = node("node_scheduler_failed_enrollment_batch");
    let index = FakeIndex::default();
    let ownerless = key(1, &target);
    let later = key(2, &target);
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    // `ownerless` becomes ready before `later`.
    for (arrival, demand) in [
        (ownerless.clone(), ownerless_need()),
        (later.clone(), need(0)),
    ] {
        index.needs.borrow_mut().insert(arrival.clone(), demand);
        let mut changed = input(1, &none, std::slice::from_ref(&arrival), Vec::new());
        changed.limit = 0;
        scheduler.pass(&index, changed).unwrap();
    }
    // The first batch moves `ownerless` to wait for enrollment; the second
    // batch's read fails.
    *index.key_reads_before_failure.borrow_mut() = Some(1);
    let mut failing = input(1, &none, &[], Vec::new());
    failing.limit = 1;
    assert!(scheduler.pass(&index, failing).is_err());
    *index.key_reads_before_failure.borrow_mut() = None;
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    assert_eq!(
        selection.ownerless,
        BTreeSet::from([(ownerless.0.clone(), ownerless.1.clone())]),
        "the retry asks for the enrollment the failed pass did not"
    );
}

#[test]
fn a_target_that_leaves_the_exclusions_without_an_edge_dispatches_on_the_next_pass() {
    let target = node("node_scheduler_silent_recovery");
    let index = FakeIndex::default();
    let parked = key(1, &target);
    index.needs.borrow_mut().insert(parked.clone(), need(0));
    let none = DfsMaintenanceExecutionExclusions::default();
    let mut scheduler = DfsRepairScheduler::default();
    let selection = scheduler
        .pass(&index, input(1, &none, &[], Vec::new()))
        .unwrap();
    scheduler.record_dispatch(
        &selection.candidates[0].key,
        DfsRepairDispatchResult::SendFailed {
            error: "unreachable".into(),
        },
        1,
    );
    let backoff_end = scheduler.next_due().unwrap();
    // Excluded with no evidence that can expire, such as a stored Offline
    // status.
    let excluded = DfsMaintenanceExecutionExclusions::new([target.clone()]);
    scheduler
        .pass(&index, input(backoff_end, &excluded, &[], Vec::new()))
        .unwrap();
    assert!(scheduler.has_parked_needs());
    assert_eq!(scheduler.next_due(), None);
    // The status leaves Offline: no reachability edge and no expiry. The
    // next pass, run for an unrelated change, sees fresh exclusions without
    // the target and dispatches its need.
    let unrelated = key(2, &node("node_scheduler_other_target"));
    index.needs.borrow_mut().insert(unrelated.clone(), need(0));
    let selection = scheduler
        .pass(
            &index,
            input(
                backoff_end,
                &none,
                std::slice::from_ref(&unrelated),
                Vec::new(),
            ),
        )
        .unwrap();
    assert!(
        selection
            .candidates
            .iter()
            .any(|candidate| candidate.key == parked),
        "the released need dispatches"
    );
    assert!(!scheduler.has_parked_needs());
}
