//! The owner's re-check of a conversion parked on another shard's job, run
//! with the owner's scheduler and no job event delivered.
use super::super::source_job_recheck::FIRST_RECHECK;
use super::*;
use std::time::{Duration, Instant};

/// `Env` stepped as the owner steps a unit: the step's park on source jobs
/// reaches the scheduler, as `OwnerConversionEnv` delivers it.
struct Owned<'a> {
    env: &'a Env,
    scheduler: &'a RefCell<Scheduler>,
    unit: &'a UnitKey,
}

impl ShardConversionEnv for Owned<'_> {
    type Source = ScriptedSource;
    fn local_node(&self) -> Result<NodeId> {
        self.env.local_node()
    }
    fn data_root(&self) -> &Path {
        self.env.data_root()
    }
    fn read_exact(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>> {
        self.env.read_exact(keys)
    }
    fn catalog(
        &self,
        file_system_id: &sinomo_dfs::DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>> {
        self.env.catalog(file_system_id)
    }
    fn submit(&self, request: &KvTxnRequest) -> Result<Submitted> {
        self.env.submit(request)
    }
    fn capture(&self, shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        self.env.capture(shard)
    }
    fn placement_evidence(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Option<Vec<crate::metadata::RetainedPlacementReservation>>> {
        self.env.placement_evidence(shard)
    }
    fn contribution_source(&self) -> &ScriptedSource {
        self.env.contribution_source()
    }
    fn block_on<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        self.env.block_on(future)
    }
    fn install(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardConfig,
        attempt: &sinomo_dfs::DfsMetadataShardConversion,
        image: &Path,
        source: &crate::authority::staged_service::conversion_install::ConversionSourceCut,
    ) -> Result<InstallReply> {
        self.env.install(node, shard, attempt, image, source)
    }
    fn activate_local(
        &self,
        shard: &DfsMetadataShardConfig,
        record: &DfsMetadataShardStorage,
        image: &Path,
    ) -> Result<()> {
        self.env.activate_local(shard, record, image)
    }
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()> {
        self.env.watch_keys(keys)?;
        let watched = self.env.watched_keys.borrow().clone();
        self.scheduler
            .borrow_mut()
            .watch_native_keys(self.unit, watched)
    }
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.env.watch_nodes(nodes)?;
        let watched = self.env.watched_nodes.borrow().clone();
        self.scheduler
            .borrow_mut()
            .watch_reachable_nodes(self.unit, watched)
    }
    fn recheck_source_jobs(&self) -> Result<()> {
        self.scheduler.borrow_mut().recheck_source_jobs(self.unit)
    }
}

fn users_unit() -> UnitKey {
    UnitKey {
        relative_root: "dfs/metadata/users".into(),
        role: UnitRole::Metadata {
            shard: DfsMetadataShardId::new("users"),
        },
    }
}

/// Take the unit's queued turns as the owner does, recording each step's
/// state, until none is queued. Returns the unit's state.
fn run_queued(owned: &Owned) -> ObservedState {
    loop {
        let work = owned.scheduler.borrow_mut().next();
        let Some((id, mut reference)) = work else {
            return owned
                .scheduler
                .borrow()
                .state(owned.unit)
                .unwrap()
                .cloned()
                .unwrap();
        };
        reference.state =
            observed_state(&advance_shard_conversion(owned, &fs_id(), "users").unwrap());
        let again = reference.state == ObservedState::Runnable;
        owned.scheduler.borrow_mut().finish(&id, reference);
        if again {
            owned
                .scheduler
                .borrow_mut()
                .enqueue(owned.unit.clone(), false)
                .unwrap();
        }
    }
}

/// The owner's loop with no event delivered: run queued turns, and sleep
/// until the scheduler's next timer when none is queued. Returns the unit's
/// state once it is current, or once nothing would wake it again.
fn run_owner(owned: &Owned) -> ObservedState {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let state = run_queued(owned);
        let next = owned.scheduler.borrow().next_retry();
        let Some(at) = next.filter(|_| state != ObservedState::VerifiedCurrent) else {
            return state;
        };
        assert!(at < deadline, "the unit keeps re-checking: {state:?}");
        std::thread::sleep(at.saturating_duration_since(Instant::now()));
    }
}

fn parked_on_jobs(state: &ObservedState) -> bool {
    matches!(state, ObservedState::Waiting { evidence } if evidence == "jobs of shards services touch this shard")
}

/// The services job touching users ends, and its end is never delivered:
/// the owner's re-check reads the sources again and the shard converges.
/// Without the re-check nothing wakes the unit and it stays parked.
#[test]
fn a_lost_job_end_converges_through_the_owners_recheck() {
    let cluster = cluster("recheck-lost-end", &["primary-p"], None);
    let env = env("shard-conversion-recheck-lost-end", "primary-p", &cluster);
    let (scheduler, unit) = (RefCell::new(Scheduler::new()), users_unit());
    let owned = Owned {
        env: &env,
        scheduler: &scheduler,
        unit: &unit,
    };
    env.source.set(
        "services",
        with_jobs(answer("services", 1, "basis-1"), [directory_move(false)]),
    );
    scheduler.borrow_mut().enqueue(unit.clone(), false).unwrap();
    let state = run_queued(&owned);
    assert!(parked_on_jobs(&state), "{state:?}");
    assert!(
        cluster
            .borrow()
            .storage("users")
            .unwrap()
            .converting_attempt()
            .unwrap()
            .sealed()
            .is_none()
    );
    // A re-check while the job runs finds it again and parks once more.
    let at = scheduler
        .borrow()
        .next_retry()
        .expect("the parked unit is re-checked");
    std::thread::sleep(at.saturating_duration_since(Instant::now()));
    let rechecked = Instant::now();
    let state = run_queued(&owned);
    assert!(parked_on_jobs(&state), "{state:?}");
    let at = scheduler
        .borrow()
        .next_retry()
        .expect("the unit is re-checked again");
    assert!(at >= rechecked + FIRST_RECHECK * 2, "the delay doubles");
    // The job ends; its `dfs.namespace.job.completed` event is lost.
    env.source.set("services", answer("services", 1, "basis-2"));
    assert_eq!(run_owner(&owned), ObservedState::VerifiedCurrent);
    assert!(cluster.borrow().storage("users").unwrap().is_active());
    assert_eq!(file_count(&env), Some(3));
    assert!(
        scheduler.borrow().next_retry().is_none(),
        "activation ends the re-checks"
    );
}

/// A wait on a source that cannot answer is woken by its keys and nodes and
/// asks no re-check, nor does a serving change that supersedes the attempt;
/// the re-check that finds the supersede ends.
#[test]
fn only_a_wait_on_source_jobs_is_rechecked() {
    let cluster = cluster("recheck-other-waits", &["primary-p", "replica-a"], None);
    let env = env(
        "shard-conversion-recheck-other-waits",
        "primary-p",
        &cluster,
    );
    let (scheduler, unit) = (RefCell::new(Scheduler::new()), users_unit());
    let owned = Owned {
        env: &env,
        scheduler: &scheduler,
        unit: &unit,
    };
    env.source.set(
        "services",
        SourceContributionRead::Waiting {
            nodes: [node("services-member")].into(),
            keys: BTreeSet::new(),
        },
    );
    scheduler.borrow_mut().enqueue(unit.clone(), false).unwrap();
    let state = run_queued(&owned);
    assert!(
        matches!(&state, ObservedState::Waiting { evidence } if evidence.contains("no member that can answer")),
        "{state:?}"
    );
    assert!(
        scheduler.borrow().next_retry().is_none(),
        "reachability wakes this wait"
    );
    // The source answers and names a job: the rounds park on it.
    env.source.set(
        "services",
        with_jobs(answer("services", 1, "basis-1"), [directory_move(false)]),
    );
    assert!(
        scheduler
            .borrow_mut()
            .nodes_became_reachable(&[node("services-member")].into())
    );
    assert!(parked_on_jobs(&run_queued(&owned)));
    assert!(scheduler.borrow().next_retry().is_some());
    // A takeover moves the primary to replica-a. The re-check finds the
    // attempt superseded, and the wait is no longer on source jobs.
    let successor = shard_config("users", "replica-a", &["primary-p"], 4);
    cluster.borrow_mut().put(
        sinomo_dfs::metadata_shard_key(&fs_id(), "users"),
        &successor,
    );
    let state = run_owner(&owned);
    assert!(
        matches!(&state, ObservedState::Waiting { evidence } if evidence == "only the shard's primary converts it"),
        "{state:?}"
    );
    assert!(
        scheduler.borrow().next_retry().is_none(),
        "the supersede ends the re-checks"
    );
}
