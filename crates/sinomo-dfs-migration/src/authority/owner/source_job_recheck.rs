//! The re-send fallback of a conversion that waits on other shards' jobs.
//!
//! A shard's contribution rounds wait while a source names a job touching
//! the shard that is not parked on it (`shard_conversion`). The job's
//! coordinator announces its park and its end, and those events wake the
//! waiting unit (`source_job_wakes`), but their delivery is best effort: a
//! lost end would leave the shard waiting until some unrelated wake.
//!
//! While a unit's last step parked on such a job, the owner therefore
//! re-checks it. The scheduler holds one re-check time per such unit in its
//! existing retry timers: [`FIRST_RECHECK`] after the park, and each
//! re-check that parks again doubles the delay up to [`MAX_RECHECK`], the
//! network liveness interval; re-checking more often than the network
//! judges a path adds nothing. A re-check only runs the unit's step again,
//! which reads the sources and decides, so it never fails or abandons
//! anything and has no attempt cap. A real wake of the unit (a job event, a
//! watched KV key, a reachability or verdict change) resets the delay. The
//! re-checks end once a step of the unit no longer parks on a source job:
//! its rounds agreed, the shard activated, the attempt was superseded, or
//! the conversion stopped.
//!
//! Waits on KV keys, reachability and verdicts take no timer. This node's
//! KV committed-observation stream reports every committed change, or a
//! discontinuity that wakes every metadata unit, and its reachability
//! verdicts are computed locally and delivered as changes, so neither loses
//! a wake for a re-check to recover.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The first re-check after a unit parks on a source job.
pub(super) const FIRST_RECHECK: Duration = Duration::from_millis(250);
/// The longest re-check delay: the network layer's liveness interval.
pub(super) const MAX_RECHECK: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;

/// The re-check state the scheduler keeps beside its retry timers.
#[derive(Debug, Default)]
pub(super) struct SourceJobRechecks {
    /// Units whose running step parked on a source job.
    parked: BTreeSet<String>,
    /// The next re-check delay of each unit whose last step parked on a
    /// source job.
    delays: BTreeMap<String, Duration>,
}

impl SourceJobRechecks {
    /// A step of `id` starts; only what it records counts at its finish.
    pub fn started(&mut self, id: &str) {
        self.parked.remove(id);
    }

    /// The running step of `id` parks on a source job.
    pub fn parked(&mut self, id: String) {
        self.parked.insert(id);
    }

    /// The step of `id` finished, leaving the unit `waiting` for a wake.
    /// Returns the delay of its re-check when that step parked on a source
    /// job; otherwise the unit's re-checks end.
    pub fn finished(&mut self, id: &str, waiting: bool) -> Option<Duration> {
        if !(self.parked.remove(id) && waiting) {
            self.delays.remove(id);
            return None;
        }
        let delay = self.delays.get(id).copied().unwrap_or(FIRST_RECHECK);
        self.delays
            .insert(id.to_owned(), delay.saturating_mul(2).min(MAX_RECHECK));
        Some(delay)
    }

    /// A real event woke `id`: a later park starts from the first delay.
    pub fn woken(&mut self, id: &str) {
        self.delays.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::inventory::{ObservedState, UnitKey, UnitRole};
    use super::super::scheduler::Scheduler;
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    fn unit(shard: &str) -> UnitKey {
        UnitKey {
            relative_root: Path::new("dfs/metadata").join(shard),
            role: UnitRole::Metadata {
                shard: sinomo_dfs::DfsMetadataShardId::new(shard),
            },
        }
    }

    fn waiting(evidence: &str) -> ObservedState {
        ObservedState::Waiting {
            evidence: evidence.into(),
        }
    }

    /// Run the next queued unit, which must be `key`, and finish it in
    /// `state`; `on_jobs` makes its step park on a source job. Returns the
    /// instants around the finish.
    fn run(
        scheduler: &mut Scheduler,
        key: &UnitKey,
        on_jobs: bool,
        state: ObservedState,
    ) -> (Instant, Instant) {
        let (id, mut reference) = scheduler.next().expect("the unit is queued");
        assert_eq!(reference.unit, *key);
        if on_jobs {
            scheduler.recheck_source_jobs(key).unwrap();
        }
        reference.state = state;
        let before = Instant::now();
        scheduler.finish(&id, reference);
        (before, Instant::now())
    }

    fn park_on_jobs(scheduler: &mut Scheduler, key: &UnitKey) -> (Instant, Instant) {
        run(
            scheduler,
            key,
            true,
            waiting("jobs of shards services touch this shard"),
        )
    }

    /// The unit's re-check is armed `delay` after its finish.
    fn assert_armed(scheduler: &Scheduler, (before, after): (Instant, Instant), delay: Duration) {
        let at = scheduler.next_retry().expect("a re-check is armed");
        assert!(
            at >= before + delay && at <= after + delay,
            "re-check {:?} after the finish, expected {delay:?}",
            at.saturating_duration_since(before)
        );
    }

    /// Wait for the armed re-check; it requeues the unit and nothing else
    /// does before it is due.
    fn await_recheck(scheduler: &mut Scheduler) {
        let at = scheduler.next_retry().expect("a re-check is armed");
        assert!(
            scheduler.next().is_none(),
            "the unit waits for its re-check"
        );
        std::thread::sleep(at.saturating_duration_since(Instant::now()));
    }

    #[test]
    fn the_recheck_delay_doubles_up_to_the_liveness_interval_with_no_cap() {
        let mut rechecks = SourceJobRechecks::default();
        let delays = (0..64)
            .map(|_| {
                rechecks.started("users");
                // Parks recorded twice in one step arm one re-check.
                rechecks.parked("users".into());
                rechecks.parked("users".into());
                rechecks
                    .finished("users", true)
                    .expect("every park on a source job is re-checked")
            })
            .collect::<Vec<_>>();
        let millis = |ms| Duration::from_millis(ms);
        assert_eq!(
            delays[..6],
            [
                millis(250),
                millis(500),
                millis(1000),
                millis(2000),
                millis(4000),
                millis(5000)
            ]
        );
        assert!(
            delays[6..]
                .iter()
                .all(|delay| *delay == sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );

        // The scheduler arms each re-check in its retry timers, and the
        // timer alone requeues the parked unit.
        let mut scheduler = Scheduler::new();
        let users = unit("users");
        scheduler.enqueue(users.clone(), false).unwrap();
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK);
        await_recheck(&mut scheduler);
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK * 2);
    }

    #[test]
    fn a_real_wake_resets_the_recheck_delay() {
        use sinomo_dfs::DfsNamespaceJobEventKind::Parked;
        let mut scheduler = Scheduler::new();
        let users = unit("users");
        scheduler.enqueue(users.clone(), false).unwrap();
        park_on_jobs(&mut scheduler, &users);
        await_recheck(&mut scheduler);
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK * 2);
        // A job event requeues the unit at once; its next park starts over.
        let event = sinomo_dfs::DfsNamespaceJobEvent {
            kind: Parked,
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            coordinator_shard_id: sinomo_dfs::DfsMetadataShardId::new("services"),
            job_id: "services-directory-move".into(),
            status: sinomo_dfs::DfsNamespaceJobStatus::Running,
            touched_shard_ids: vec![sinomo_dfs::DfsMetadataShardId::new("users")],
        };
        assert!(super::super::source_job_wakes::source_job_changed(
            &mut scheduler,
            &event
        ));
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK);
        // So does any other wake of the unit, here a watched KV key.
        await_recheck(&mut scheduler);
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK * 2);
        assert!(scheduler.dependencies_changed(|key| *key == users));
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK);
        // A wake during the step reruns it instead of arming a re-check.
        assert!(scheduler.dependencies_changed(|key| *key == users));
        let (id, mut reference) = scheduler.next().unwrap();
        scheduler.recheck_source_jobs(&users).unwrap();
        assert!(scheduler.dependencies_changed(|key| *key == users));
        reference.state = waiting("jobs of shards services touch this shard");
        scheduler.finish(&id, reference);
        assert!(
            scheduler.next_retry().is_none(),
            "the rerun is queued, not timed"
        );
        let finished = park_on_jobs(&mut scheduler, &users);
        assert_armed(&scheduler, finished, FIRST_RECHECK);
    }

    /// The re-checks end with the source-job wait: the shard activated, a
    /// step parked on something else (the rounds agreed and the step waits
    /// on its install members, or a serving change superseded the
    /// attempt), or the conversion stopped. A later park starts over.
    #[test]
    fn the_rechecks_end_once_the_unit_no_longer_waits_on_source_jobs() {
        for end in [
            ObservedState::VerifiedCurrent,
            waiting("only the shard's primary converts it"),
            ObservedState::Blocked {
                evidence: "metadata shard conversion stopped: dangling alias".into(),
            },
        ] {
            let mut scheduler = Scheduler::new();
            let users = unit("users");
            scheduler.enqueue(users.clone(), false).unwrap();
            park_on_jobs(&mut scheduler, &users);
            await_recheck(&mut scheduler);
            let blocked = matches!(end, ObservedState::Blocked { .. });
            let settled = !matches!(end, ObservedState::Waiting { .. });
            run(&mut scheduler, &users, false, end);
            assert!(scheduler.next_retry().is_none(), "no re-check is armed");
            assert!(scheduler.next().is_none());
            if blocked {
                continue;
            }
            if settled {
                // Demand reopens an activated unit's observation.
                scheduler.enqueue(users.clone(), true).unwrap();
            } else {
                assert!(scheduler.dependencies_changed(|key| *key == users));
            }
            let finished = park_on_jobs(&mut scheduler, &users);
            assert_armed(&scheduler, finished, FIRST_RECHECK);
        }
    }

    /// No unit waiting on a source job, no timer: an idle owner holds none,
    /// and units parked on KV keys, reachability or verdicts hold none.
    #[test]
    fn no_timer_without_a_unit_waiting_on_source_jobs() {
        let mut scheduler = Scheduler::new();
        assert!(scheduler.next_retry().is_none());
        let node = |name: &str| sinomo_core::node::NodeId::from_legacy_named(name).unwrap();
        for (shard, evidence) in [
            ("users", "the shard has no serving record"),
            (
                "services",
                "a contributing shard has no member that can answer",
            ),
            (
                "root",
                "only the shard's primary converts it; this member does not judge it lost",
            ),
        ] {
            let key = unit(shard);
            scheduler.enqueue(key.clone(), false).unwrap();
            let (id, mut reference) = scheduler.next().unwrap();
            match shard {
                "users" => scheduler
                    .watch_native_keys(&key, ["/dfs/key".to_owned()].into())
                    .unwrap(),
                "services" => scheduler
                    .watch_reachable_nodes(&key, [node("member")].into())
                    .unwrap(),
                _ => scheduler
                    .watch_node_verdicts(&key, [node("primary")].into())
                    .unwrap(),
            }
            reference.state = waiting(evidence);
            scheduler.finish(&id, reference);
        }
        assert!(scheduler.next().is_none());
        assert!(
            scheduler.next_retry().is_none(),
            "no parked unit owns a timer"
        );
    }
}
