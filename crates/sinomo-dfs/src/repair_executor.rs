//! The node's single repair executor.
//!
//! Every replica derives the same repair index from its shard tables. The
//! executor of the node that is currently primary for a shard is the only one
//! that schedules that shard's repairs; it runs as the repair phase of the
//! node's DFS maintenance runtime, so one pass runs at a time per node.
//!
//! All scheduling state is in memory and keyed by the node's data root: the
//! scheduler's per-need phase and the attempt registry. Both start empty at
//! process start, and a shard's state is dropped when its primary assignment
//! changes, so a successor restarts timing instead of inheriting the retiring
//! owner's.
//!
//! The executor acts only on events, and each pass does work proportional to
//! what they name: the objects whose index entries changed, the attempts whose
//! outcome arrived, the targets whose reachability changed, an authority view
//! that caught up, a gained primary assignment and its single timer. The whole
//! index of a shard is read only when this node gains its primary assignment,
//! which includes the first observation after process start.

mod dispatch;
mod scheduler;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use sinomo_core::node::NodeId;

use crate::maintenance::DfsMaintenanceExecutionExclusions;
use crate::metadata_tables::{DfsRepairNeed, DfsRepairNeedKey};
use crate::{
    DfsError, DfsMessageTransport, DfsMetadataShardConfig, DfsMetadataShardId, DfsObjectId,
    DfsVersion, NetworkedDfsService, dfs_now_unix_nanos,
};

pub(crate) use scheduler::{
    DfsRepairAttemptOutcome, DfsRepairDispatchResult, DfsRepairExecutorCandidate,
    DfsRepairOwnerEnrollment,
};
use scheduler::{DfsRepairNeedSource, DfsRepairPassInput, DfsRepairScheduler};

type DfsRepairExecutorWake = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct DfsRepairExecutorRegistry {
    executors: BTreeMap<PathBuf, Arc<DfsRepairExecutor>>,
    wakes: BTreeMap<PathBuf, DfsRepairExecutorWake>,
    /// The data root each local node's executor serves, so a reply or send
    /// failure observed by any lane of that node reaches its executor.
    node_roots: BTreeMap<NodeId, PathBuf>,
    /// Test-support holds per data root. They outlive the executor, so a
    /// hold taken before a node restart still holds its fresh executor.
    #[cfg(any(test, feature = "test-support"))]
    holds: BTreeMap<PathBuf, usize>,
}

static REGISTRY: OnceLock<Mutex<DfsRepairExecutorRegistry>> = OnceLock::new();

/// Counts wake registrations, so a durability wait on a node whose executor
/// is not registered yet waits for that registration instead of polling.
static REGISTRATIONS: OnceLock<(Mutex<u64>, std::sync::Condvar)> = OnceLock::new();

fn registrations() -> &'static (Mutex<u64>, std::sync::Condvar) {
    REGISTRATIONS.get_or_init(Default::default)
}

/// A node gained an executor binding: durability waits that found none
/// look again.
fn notify_registration() {
    let (count, registered) = registrations();
    *lock(count) += 1;
    registered.notify_all();
}

fn registry() -> MutexGuard<'static, DfsRepairExecutorRegistry> {
    REGISTRY
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// How a shard's index changed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRepairIndexChange {
    /// An applied batch changed the needs of these objects.
    Objects(BTreeSet<DfsObjectId>),
    /// An installed snapshot replaced the index.
    Replaced,
}

/// Events delivered since the last pass.
#[derive(Debug, Default)]
struct DfsRepairExecutorEvents {
    changed: BTreeMap<String, BTreeSet<DfsObjectId>>,
    replaced: BTreeSet<String>,
    outcomes: Vec<scheduler::DfsRepairOutcomeEvent>,
    authority_changed: bool,
    reachability_changed: bool,
    demanded: BTreeSet<DfsObjectId>,
}

impl DfsRepairExecutorEvents {
    /// Return events a pass took but did not serve, ahead of any that
    /// arrived meanwhile.
    fn restore(&mut self, mut taken: DfsRepairExecutorEvents) {
        for (shard, objects) in std::mem::take(&mut self.changed) {
            taken.changed.entry(shard).or_default().extend(objects);
        }
        taken.replaced.append(&mut self.replaced);
        taken.outcomes.append(&mut self.outcomes);
        taken.authority_changed |= self.authority_changed;
        taken.reachability_changed |= self.reachability_changed;
        taken.demanded.append(&mut self.demanded);
        *self = taken;
    }
}

/// One attempt that left and awaits its outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsRepairAttempt {
    pub(crate) request_id: String,
    /// Receipt-slot generation the attempt planned against.
    pub(crate) generation: u64,
    pub(crate) sent_unix_nanos: u64,
}

/// What status, placement and the maintenance loop read about the executor,
/// published by each pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsRepairExecutorWork {
    /// A wake or the continuation of a gained assignment waits for a pass.
    pub(crate) due: bool,
    /// Owned needs in this node's primary shards, from the index's
    /// maintained counts.
    pub(crate) need_count: usize,
    /// Needs the scheduler tracks that it can attempt: ready, in flight,
    /// backing off, parked or waiting for authority. Demand waiting for an
    /// owner is not attempted work until a change of its need enrolls one.
    pub(crate) tracked_need_count: usize,
    pub(crate) in_flight_count: usize,
    pub(crate) backoff_count: usize,
    pub(crate) parked: bool,
    pub(crate) awaiting_authority: bool,
    /// The executor's single timer: the earliest backoff end or re-check.
    pub(crate) next_due_unix_nanos: Option<u64>,
}

/// One node's executor. Wakes and outcomes arrive from shard writers,
/// appliers and reply lanes that may hold metadata locks, so those paths
/// touch only the leaf locks (`pending`, `primary_shards`, `events`,
/// `attempts`, `work`) and never `scheduler`, which a pass holds while it
/// reads the index and dispatches.
#[derive(Debug, Default)]
pub(crate) struct DfsRepairExecutor {
    pending: AtomicBool,
    /// Counts reachability events, so a pass can tell whether an event it
    /// consumes is newer than the reachability view it was given, and
    /// whether one arrived while it ran.
    reachability_epoch: AtomicU64,
    /// Counts authority events, so a pass can tell whether an event it
    /// consumes is newer than the authority view its turn captured, and
    /// whether one arrived while it ran.
    authority_epoch: AtomicU64,
    /// Counts the passes that published their work, so the maintenance loop
    /// can tell whether a turn ran a pass that replaced the timer.
    passes: AtomicU64,
    /// The runtime that served this executor stopped: durability waits on
    /// it return.
    closed: AtomicBool,
    /// A turn yielded because the published configuration was not ready:
    /// the next publication wakes the executor.
    awaiting_publication: AtomicBool,
    /// Shards this node was primary for at its last pass; `None` before it,
    /// when the first pass reads every primary shard's whole index anyway.
    primary_shards: Mutex<Option<BTreeSet<String>>>,
    /// Primary shards the last pass left out because this node's copy was
    /// closed for storage. Their activation wakes the executor, whose next
    /// pass gains them and reads their whole index.
    storage_closed_primaries: Mutex<BTreeSet<String>>,
    events: Mutex<DfsRepairExecutorEvents>,
    /// The attempt in flight for each need, whoever sent it: the executor's
    /// own dispatch or a write's send beyond its commit quorum.
    attempts: Mutex<BTreeMap<DfsRepairNeedKey, DfsRepairAttempt>>,
    work: Mutex<DfsRepairExecutorWork>,
    /// The last status sample, served while a pass holds the scheduler.
    status: Mutex<Vec<crate::DfsRepairAttemptRecord>>,
    scheduler: Mutex<DfsRepairScheduler>,
    /// Repair progress on this node: each index change of a primary shard
    /// and each ended attempt advance it. A durability wait blocks on it.
    progress: Mutex<u64>,
    progress_changed: std::sync::Condvar,
}

impl DfsRepairExecutor {
    /// Take the delivered events and serve them in one scheduler pass. A
    /// pass that fails serves nothing: its events return ahead of any that
    /// arrived meanwhile and the executor stays pending, so the retry reads
    /// the index again. Returns the edges the pass consumed, which
    /// `rewake_for_edges` checks once the pass has published its work.
    fn pass_events(
        &self,
        scheduler: &mut DfsRepairScheduler,
        source: &dyn DfsRepairNeedSource,
        now: u64,
        primaries: BTreeMap<String, u64>,
        exclusions: &DfsMaintenanceExecutionExclusions,
        limit: usize,
    ) -> Result<(scheduler::DfsRepairExecutorSelection, DfsRepairPassEdges), DfsError> {
        // An edge is counted and recorded under the events lock, so the
        // counts read here cover exactly the events taken.
        let (events, reachability_at_take, authority_at_take) = {
            let mut events = lock(&self.events);
            (
                std::mem::take(&mut *events),
                self.reachability_epoch.load(Ordering::SeqCst),
                self.authority_epoch.load(Ordering::SeqCst),
            )
        };
        let stale_reachability = events.reachability_changed
            && exclusions
                .repair_reachability_epoch()
                .is_some_and(|seen| reachability_at_take > seen);
        let stale_authority = events.authority_changed
            && exclusions
                .repair_authority_epoch()
                .is_some_and(|seen| authority_at_take > seen);
        let input = DfsRepairPassInput {
            now,
            primaries,
            exclusions,
            changed: events.changed.clone(),
            replaced: events.replaced.clone(),
            outcomes: events.outcomes.clone(),
            authority_changed: events.authority_changed,
            demanded: events.demanded.clone(),
            limit,
        };
        match scheduler.pass(source, input) {
            Ok(selection) => Ok((
                selection,
                DfsRepairPassEdges {
                    reachability_at_take,
                    authority_at_take,
                    stale_reachability,
                    stale_authority,
                },
            )),
            Err(error) => {
                lock(&self.events).restore(events);
                self.pending.store(true, Ordering::SeqCst);
                Err(error)
            }
        }
    }

    /// After a pass published its work: whether the executor must wake
    /// again for an edge its waiting needs wait for. A reachability event
    /// newer than the view the pass parked needs on is recorded again, and an
    /// edge raised while the pass ran, already recorded, needs a pass to
    /// serve it. An edge raised after this check finds the published work.
    fn rewake_for_edges(&self, scheduler: &DfsRepairScheduler, edges: &DfsRepairPassEdges) -> bool {
        let parked = scheduler.has_parked_needs();
        let awaiting_authority = scheduler.has_awaiting_authority();
        if edges.stale_reachability && parked {
            lock(&self.events).reachability_changed = true;
        }
        if edges.stale_authority && awaiting_authority {
            lock(&self.events).authority_changed = true;
        }
        let rewake = (parked
            && (edges.stale_reachability
                || self.reachability_epoch.load(Ordering::SeqCst) > edges.reachability_at_take))
            || (awaiting_authority
                && (edges.stale_authority
                    || self.authority_epoch.load(Ordering::SeqCst) > edges.authority_at_take));
        if rewake {
            self.pending.store(true, Ordering::SeqCst);
        }
        rewake
    }
}

/// The edge counts a pass read when it took its events.
#[derive(Clone, Copy, Debug)]
struct DfsRepairPassEdges {
    reachability_at_take: u64,
    authority_at_take: u64,
    /// The pass consumed a reachability event newer than its view.
    stale_reachability: bool,
    /// The pass consumed an authority event newer than the authority view
    /// its turn dispatched with.
    stale_authority: bool,
}

fn advance_progress(executor: &DfsRepairExecutor) {
    *lock(&executor.progress) += 1;
    executor.progress_changed.notify_all();
}

/// The repair progress of the executor serving `node_id`, if one is
/// registered: the mark a durability wait blocks past.
pub(crate) fn repair_executor_progress(node_id: &NodeId) -> Option<u64> {
    let (_, executor) = executor_for_node(node_id)?;
    Some(*lock(&executor.progress))
}

/// Block until repair progress on `node_id` passes `seen`, or `deadline`.
/// The wait is on the index-change and attempt-outcome edge, never a poll;
/// it returns whether progress advanced.
pub(crate) fn repair_executor_wait_for_progress(
    node_id: &NodeId,
    seen: u64,
    deadline: impl Into<Option<std::time::Instant>>,
) -> bool {
    // Without a deadline the wait ends on the executor's own edge: its
    // registration, a progress change, or its close.
    let deadline: Option<std::time::Instant> = deadline.into();
    let remaining = || match deadline {
        Some(deadline) => deadline
            .checked_duration_since(std::time::Instant::now())
            .map(Some),
        None => Some(None),
    };
    let executor = loop {
        let (count, registered) = registrations();
        let observed = *lock(count);
        if let Some((_, executor)) = executor_for_node(node_id) {
            break executor;
        }
        // No runtime serves this node yet: its registration is the edge.
        let Some(remaining) = remaining() else {
            return false;
        };
        let guard = lock(count);
        if *guard == observed {
            match remaining {
                Some(remaining) => drop(
                    registered
                        .wait_timeout(guard, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                ),
                None => drop(
                    registered
                        .wait(guard)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                ),
            }
        }
    };
    let mut progress = lock(&executor.progress);
    loop {
        if *progress != seen || executor.closed.load(Ordering::SeqCst) {
            return *progress != seen;
        }
        let Some(remaining) = remaining() else {
            return false;
        };
        progress = match remaining {
            Some(remaining) => {
                executor
                    .progress_changed
                    .wait_timeout(progress, remaining)
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .0
            }
            None => executor
                .progress_changed
                .wait(progress)
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        };
    }
}

/// The executor a pass on `data_root` serves, and whether a test holds it.
/// The pass binds `node_id` to it; a first binding is a registration edge
/// for durability waits that found no executor for the node.
fn bind_pass_executor(node_id: &NodeId, data_root: &Path) -> (Arc<DfsRepairExecutor>, bool) {
    let (executor, held, bound) = {
        let mut registry = registry();
        let bound = registry
            .node_roots
            .insert(node_id.clone(), data_root.to_path_buf())
            .is_none_or(|previous| previous != data_root);
        #[cfg(any(test, feature = "test-support"))]
        let held = registry
            .holds
            .get(data_root)
            .is_some_and(|count| *count > 0);
        #[cfg(not(any(test, feature = "test-support")))]
        let held = false;
        let executor = Arc::clone(
            registry
                .executors
                .entry(data_root.to_path_buf())
                .or_default(),
        );
        (executor, held, bound)
    };
    if bound {
        // A durability wait that found no executor for this node waits
        // for a registration: a first binding here is one.
        notify_registration();
    }
    (executor, held)
}

fn executor(data_root: &Path) -> Arc<DfsRepairExecutor> {
    Arc::clone(
        registry()
            .executors
            .entry(data_root.to_path_buf())
            .or_default(),
    )
}

fn executor_for_node(node_id: &NodeId) -> Option<(PathBuf, Arc<DfsRepairExecutor>)> {
    let registry = registry();
    let data_root = registry.node_roots.get(node_id)?.clone();
    let executor = registry.executors.get(&data_root).cloned()?;
    Some((data_root, executor))
}

/// The work of the executor serving `node_id`, if one is registered.
pub(crate) fn repair_executor_work_for_node(node_id: &NodeId) -> Option<DfsRepairExecutorWork> {
    let (data_root, _) = executor_for_node(node_id)?;
    Some(repair_executor_work(&data_root))
}

pub(crate) fn repair_executor_work(data_root: &Path) -> DfsRepairExecutorWork {
    let executor = executor(data_root);
    let mut work = *lock(&executor.work);
    work.due |= executor.pending.load(Ordering::SeqCst);
    work
}

/// Operator status rows for up to `limit` of the needs the executor tracks.
/// A status read never waits for a pass: while one holds the scheduler, the
/// last sample is served.
pub(crate) fn repair_executor_status_sample(
    data_root: &Path,
    limit: usize,
) -> Vec<crate::DfsRepairAttemptRecord> {
    let executor = executor(data_root);
    let sample = match executor.scheduler.try_lock() {
        Ok(scheduler) => scheduler.status_sample(limit),
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            poisoned.into_inner().status_sample(limit)
        }
        Err(std::sync::TryLockError::WouldBlock) => return lock(&executor.status).clone(),
    };
    lock(&executor.status).clone_from(&sample);
    sample
}

/// Unregisters the runtime's wake when its maintenance task stops.
#[must_use]
pub(crate) struct DfsRepairExecutorWakeRegistration {
    data_root: PathBuf,
    wake: DfsRepairExecutorWake,
}

impl Drop for DfsRepairExecutorWakeRegistration {
    /// A stopped node schedules nothing; its in-memory attempts and timing
    /// end with it, durability waits on it return, and a restart's first pass
    /// reads the index afresh. A registration already replaced by a newer
    /// runtime on the same root leaves that runtime's wake and executor in
    /// place.
    fn drop(&mut self) {
        let removed = {
            let mut registry = registry();
            if registry
                .wakes
                .get(&self.data_root)
                .is_some_and(|current| Arc::ptr_eq(current, &self.wake))
            {
                registry.wakes.remove(&self.data_root);
                let removed = registry.executors.remove(&self.data_root);
                // A wake raised on the stopping executor and not served yet,
                // such as a restarted runtime's first assignment observation,
                // stays pending for the runtime that registers next.
                if removed
                    .as_ref()
                    .is_some_and(|executor| executor.pending.load(Ordering::SeqCst))
                {
                    registry.executors.insert(
                        self.data_root.clone(),
                        Arc::new(DfsRepairExecutor {
                            pending: AtomicBool::new(true),
                            ..Default::default()
                        }),
                    );
                }
                removed
            } else {
                None
            }
        };
        if let Some(executor) = removed {
            executor.closed.store(true, Ordering::SeqCst);
            let _progress = lock(&executor.progress);
            executor.progress_changed.notify_all();
        }
    }
}

/// The maintenance runtime registers how a wake reaches it, for the node it
/// serves. From then on progress, demand and outcomes observed by any lane of
/// that node reach this executor. A wake raised before the registration, such
/// as the first observation of the node's primary assignments, is pending on
/// the executor and is delivered here.
pub(crate) fn register_repair_executor_wake(
    data_root: &Path,
    node_id: Option<&NodeId>,
    wake: impl Fn() + Send + Sync + 'static,
) -> DfsRepairExecutorWakeRegistration {
    let wake: DfsRepairExecutorWake = Arc::new(wake);
    let executor = {
        let mut registry = registry();
        registry
            .wakes
            .insert(data_root.to_path_buf(), Arc::clone(&wake));
        if let Some(node_id) = node_id {
            registry
                .node_roots
                .insert(node_id.clone(), data_root.to_path_buf());
        }
        Arc::clone(
            registry
                .executors
                .entry(data_root.to_path_buf())
                .or_default(),
        )
    };
    notify_registration();
    if executor.pending.load(Ordering::SeqCst) {
        wake();
    }
    DfsRepairExecutorWakeRegistration {
        data_root: data_root.to_path_buf(),
        wake,
    }
}

/// Mark the root's current executor pending and deliver the wake. Both are
/// read under the registry lock that unregistration holds while it removes
/// an executor and carries its pending flag, so a wake racing a stopping
/// runtime is either carried to the next one or lands on it.
fn raise_wake(data_root: &Path, source: &'static str) {
    tracing::trace!(target: "sinomo.dfs.perf", stage = "dfs.repair.executor.wake",
        data_root = %data_root.display(), source, "repair executor wake");
    let wake = {
        let mut registry = registry();
        registry
            .executors
            .entry(data_root.to_path_buf())
            .or_default()
            .pending
            .store(true, Ordering::SeqCst);
        registry.wakes.get(data_root).cloned()
    };
    if let Some(wake) = wake {
        wake();
    }
}

/// Raise one wake for the node's executor. `source` names the wake edge.
pub(crate) fn wake_repair_executor(data_root: &Path, source: &'static str) {
    raise_wake(data_root, source);
}

/// Shards whose storage turned Active on this node's configuration. When the
/// executor's last pass left one of them out as a closed primary copy, wake
/// it: its next pass gains the shard and reads its whole index. An executor
/// with no such shard has no work from this and is not woken.
pub(crate) fn repair_executor_shard_storage_activated(
    data_root: &Path,
    activated: &BTreeSet<crate::DfsMetadataShardStorageKey>,
) -> bool {
    let waiting = {
        let executor = executor(data_root);
        let closed = lock(&executor.storage_closed_primaries);
        activated.iter().any(|(_, shard)| closed.contains(&shard.0))
    };
    if waiting {
        raise_wake(data_root, "storage_activated");
    }
    waiting
}

/// The executor's timer fired: the next pass serves it. The caller already
/// runs that pass, so no further wake is raised.
pub(crate) fn repair_executor_timer_fired(data_root: &Path) {
    executor(data_root).pending.store(true, Ordering::SeqCst);
}

/// A shard writer or replica applier changed its shard's repair index.
/// Shard backends live under their node's data root. Only a primary shard's
/// change reaches the executor: a replica schedules nothing, and a new
/// primary reads the whole index when it gains the assignment.
pub(crate) fn repair_index_changed(
    shard_root: &Path,
    shard_id: &DfsMetadataShardId,
    change: DfsRepairIndexChange,
) {
    let found = {
        let registry = registry();
        registry
            .wakes
            .keys()
            .chain(registry.executors.keys())
            .find(|data_root| shard_root.starts_with(data_root))
            .cloned()
            .map(|data_root| {
                let executor = registry.executors.get(&data_root).cloned();
                (data_root, executor)
            })
    };
    let Some((data_root, Some(executor))) = found else {
        return;
    };
    let primary = lock(&executor.primary_shards)
        .as_ref()
        .is_some_and(|primaries| primaries.contains(&shard_id.0));
    if !primary {
        return;
    }
    {
        let mut events = lock(&executor.events);
        match change {
            DfsRepairIndexChange::Objects(objects) => events
                .changed
                .entry(shard_id.0.clone())
                .or_default()
                .extend(objects),
            DfsRepairIndexChange::Replaced => {
                events.replaced.insert(shard_id.0.clone());
            }
        }
    }
    advance_progress(&executor);
    raise_wake(&data_root, "repair_index");
}

/// A write's send to a replica beyond its commit quorum is about to leave:
/// it is that need's attempt until its outcome arrives, so the executor does
/// not send the same replica again meanwhile.
pub(crate) fn repair_executor_attempt_started(
    node_id: &NodeId,
    key: DfsRepairNeedKey,
    request_id: &str,
    generation: u64,
) {
    if let Some((_, executor)) = executor_for_node(node_id) {
        // An attempt already in flight for the need, the executor's own or
        // another send's, keeps its correlation: its reply still ends it,
        // and a reply to this send changes the index like any receipt.
        lock(&executor.attempts)
            .entry(key)
            .or_insert_with(|| DfsRepairAttempt {
                request_id: request_id.to_owned(),
                generation,
                sent_unix_nanos: dfs_now_unix_nanos(),
            });
    }
}

/// The attempt `request_id` for `key` ended, observed by any lane of
/// `node_id`: a reply, a rejected reply or a send failure. It ends the
/// attempt and wakes the executor. An outcome for a request that is no longer
/// the need's attempt changes nothing.
pub(crate) fn repair_executor_attempt_ended(
    node_id: &NodeId,
    key: &DfsRepairNeedKey,
    request_id: &str,
    outcome: DfsRepairAttemptOutcome,
) {
    repair_executor_attempt_ended_with_error(node_id, key, request_id, outcome, None);
}

/// The same, with the error a failed or rejected attempt reported; the need
/// keeps it for operator status until it settles.
pub(crate) fn repair_executor_attempt_ended_with_error(
    node_id: &NodeId,
    key: &DfsRepairNeedKey,
    request_id: &str,
    outcome: DfsRepairAttemptOutcome,
    error: Option<String>,
) {
    let Some((data_root, executor)) = executor_for_node(node_id) else {
        return;
    };
    {
        let mut attempts = lock(&executor.attempts);
        if attempts.get(key).map(|attempt| attempt.request_id.as_str()) != Some(request_id) {
            return;
        }
        attempts.remove(key);
    }
    lock(&executor.events).outcomes.push((
        key.clone(),
        Some(request_id.to_owned()),
        outcome,
        error,
    ));
    advance_progress(&executor);
    raise_wake(&data_root, "attempt_outcome");
}

/// The rejection reason an attempt's processed reply carries, if any.
pub(crate) fn repair_event_error(event: &crate::NetworkedDfsServiceEvent) -> Option<String> {
    match event {
        crate::NetworkedDfsServiceEvent::RepairRejected { reason, .. }
        | crate::NetworkedDfsServiceEvent::BlobFetchRejected { reason, .. }
        | crate::NetworkedDfsServiceEvent::BlobStageRejected { reason, .. } => Some(reason.clone()),
        _ => None,
    }
}

/// The need whose attempt `request_id` is, among the in-flight attempts of
/// one object version: a fetch names its source, not the replica it repairs.
pub(crate) fn repair_executor_request_need(
    node_id: &NodeId,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    request_id: &str,
) -> Option<DfsRepairNeedKey> {
    let (_, executor) = executor_for_node(node_id)?;
    lock(&executor.attempts)
        .iter()
        .find(|((object, attempt_version, _), attempt)| {
            object == object_id && attempt_version == version && attempt.request_id == request_id
        })
        .map(|(key, _)| key.clone())
}

/// A foreground durability wait (a file sync) is blocked on `object_id`'s
/// missing replicas: its ready needs go ahead of others and its needs not
/// tracked yet are read now. A backoff or park a failed attempt earned is
/// left to its own time or edge. The event touches that object's needs only.
pub(crate) fn repair_executor_object_demanded(node_id: &NodeId, object_id: &DfsObjectId) {
    let Some((data_root, executor)) = executor_for_node(node_id) else {
        return;
    };
    lock(&executor.events).demanded.insert(object_id.clone());
    raise_wake(&data_root, "durability_demand");
}

/// The count of reachability events raised on the node's executor so far.
pub(crate) fn repair_executor_reachability_epoch(data_root: &Path) -> u64 {
    executor(data_root)
        .reachability_epoch
        .load(Ordering::SeqCst)
}

/// The count of authority events raised on the node's executor so far. A
/// turn reads it before it captures its authority view.
pub(crate) fn repair_executor_authority_epoch(data_root: &Path) -> u64 {
    executor(data_root).authority_epoch.load(Ordering::SeqCst)
}

/// The count of passes the node's executor published.
pub(crate) fn repair_executor_pass_count(data_root: &Path) -> u64 {
    executor(data_root).passes.load(Ordering::SeqCst)
}

/// Reachability changed: release needs parked on targets that became
/// reachable. The event is always recorded and counted. It wakes the
/// executor when a need is parked; a pass running now that parks one, or
/// that parked one on an older view, wakes the next pass itself.
pub(crate) fn repair_executor_reachability_changed(data_root: &Path) {
    let executor = executor(data_root);
    {
        let mut events = lock(&executor.events);
        events.reachability_changed = true;
        executor.reachability_epoch.fetch_add(1, Ordering::SeqCst);
    }
    if lock(&executor.work).parked {
        raise_wake(data_root, "reachability");
    }
}

/// The local authority view changed: needs whose dispatch was refused while
/// it lagged the published assignment are due again, and a turn that yielded
/// to an unready configuration runs again. The event is always recorded and
/// counted; it wakes the executor when something waits for it, and a pass
/// running now that leaves a need waiting for it wakes the next pass.
pub(crate) fn repair_executor_authority_changed(data_root: &Path) {
    let executor = executor(data_root);
    let awaiting_publication = executor.awaiting_publication.swap(false, Ordering::SeqCst);
    {
        let mut events = lock(&executor.events);
        events.authority_changed = true;
        executor.authority_epoch.fetch_add(1, Ordering::SeqCst);
    }
    if lock(&executor.work).awaiting_authority || awaiting_publication {
        raise_wake(data_root, "authority");
    }
}

/// An executor turn yielded because the published configuration was not
/// ready; its pending work stays pending until the next publication.
pub(crate) fn repair_executor_await_publication(data_root: &Path) {
    executor(data_root)
        .awaiting_publication
        .store(true, Ordering::SeqCst);
}

/// Shards this node is primary for, with their assignment epochs.
pub(crate) fn local_primary_shards(
    shards: &[DfsMetadataShardConfig],
    local_node_id: &NodeId,
) -> BTreeMap<String, u64> {
    shards
        .iter()
        .filter(|shard| &shard.primary_node_id == local_node_id)
        .map(|shard| (shard.shard_id.clone(), shard.epoch))
        .collect()
}

/// Whether a published configuration gave this node a primary assignment it
/// did not hold before, the edge that wakes the executor: a gained assignment
/// brings the whole index of its shard. The first observation after process
/// start gains every assignment it shows. A lost assignment needs no pass;
/// the next pass drops its timing.
pub(crate) fn repair_primary_assignments_changed(
    observed: &mut Option<BTreeMap<String, u64>>,
    primaries: BTreeMap<String, u64>,
) -> bool {
    let gained = primaries.iter().any(|(shard, epoch)| {
        observed
            .as_ref()
            .is_none_or(|previous| previous.get(shard) != Some(epoch))
    });
    *observed = Some(primaries);
    gained
}

/// The index of the node's primary shards, read through its metadata backend.
struct DfsBackendRepairNeedSource<'a> {
    backend: &'a dyn crate::DfsMetadataBackend,
}

impl DfsRepairNeedSource for DfsBackendRepairNeedSource<'_> {
    fn needs_for_objects(
        &self,
        shard: &str,
        objects: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(self
            .backend
            .repair_index_objects(&DfsMetadataShardId::new(shard), objects)?
            .needs)
    }

    fn needs_for_keys(
        &self,
        shard: &str,
        keys: &[DfsRepairNeedKey],
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(self
            .backend
            .repair_index_needs(&DfsMetadataShardId::new(shard), keys)?
            .needs)
    }

    fn page(
        &self,
        shard: &str,
        after: Option<&DfsRepairNeedKey>,
        limit: usize,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError> {
        Ok(self
            .backend
            .repair_index_page(&DfsMetadataShardId::new(shard), after, limit)?
            .needs)
    }

    fn need_count(&self, shard: &str) -> Result<usize, DfsError> {
        Ok(self
            .backend
            .repair_index_owned_count(&DfsMetadataShardId::new(shard))?)
    }
}

/// What one executor pass did.
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsRepairExecutorPassReport {
    /// The repairs this pass attempted.
    pub(crate) scheduled: Vec<crate::DistributedRepairJob>,
    /// Needs the executor tracks after this pass.
    pub(crate) tracked: usize,
    /// Index entries and tracked needs this pass touched.
    pub(crate) examined: usize,
    /// A gained assignment's traversal or ready needs remain within capacity:
    /// the same wake continues.
    pub(crate) has_more: bool,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Serve every event delivered since the last pass and dispatch the needs
    /// it admits, for the shards this node is primary for in
    /// `metadata_shards`.
    pub(crate) fn run_repair_executor_pass(
        &mut self,
        data_root: &Path,
        metadata_shards: &[DfsMetadataShardConfig],
        exclusions: &DfsMaintenanceExecutionExclusions,
        limit: usize,
    ) -> Result<DfsRepairExecutorPassReport, DfsError> {
        let mut primaries = local_primary_shards(metadata_shards, &self.node_id);
        // A primary whose local copy is closed for storage is left out, as on
        // a node that is not its primary: its needs are not read or timed
        // while it is closed, and its whole index is read once it opens.
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut closed = BTreeSet::new();
            for shard in primaries.keys() {
                if backend.metadata_shard_storage_is_closed(&DfsMetadataShardId::new(shard.as_str()))? {
                    closed.insert(shard.clone());
                }
            }
            primaries.retain(|shard, _| !closed.contains(shard));
            *lock(&executor(data_root).storage_closed_primaries) = closed;
        }
        self.run_repair_executor_pass_with(data_root, Some(primaries), exclusions, limit)
    }

    fn run_repair_executor_pass_with(
        &mut self,
        data_root: &Path,
        primaries: Option<BTreeMap<String, u64>>,
        exclusions: &DfsMaintenanceExecutionExclusions,
        limit: usize,
    ) -> Result<DfsRepairExecutorPassReport, DfsError> {
        let (executor, held) = bind_pass_executor(&self.node_id, data_root);
        if self.metadata_backend.is_none() {
            executor.pending.store(false, Ordering::SeqCst);
            return Ok(DfsRepairExecutorPassReport::default());
        }
        // A held executor leaves its events for the pass after the release,
        // which wakes it.
        if held {
            executor.pending.store(false, Ordering::SeqCst);
            return Ok(DfsRepairExecutorPassReport::default());
        }
        let mut scheduler = lock(&executor.scheduler);
        executor.pending.store(false, Ordering::SeqCst);
        let primaries = primaries.unwrap_or_else(|| scheduler.primaries().clone());
        *lock(&executor.primary_shards) = Some(primaries.keys().cloned().collect());
        let now = dfs_now_unix_nanos();
        let selection = {
            let backend = self.metadata_backend.as_ref().expect("checked above");
            let source = DfsBackendRepairNeedSource {
                backend: backend.as_ref(),
            };
            executor.pass_events(&mut scheduler, &source, now, primaries, exclusions, limit)?
        };
        let (selection, edges) = selection;
        // A need some other producer is already sending is that attempt's
        // until its outcome or re-check; the executor does not send the
        // replica again meanwhile. An attempt past its re-check is replaced.
        let foreign = {
            let mut attempts = lock(&executor.attempts);
            let mut foreign = BTreeMap::new();
            for candidate in &selection.candidates {
                let Some(attempt) = attempts.get(&candidate.key) else {
                    continue;
                };
                if attempt
                    .sent_unix_nanos
                    .saturating_add(scheduler::in_flight_resend_nanos())
                    > now
                {
                    foreign.insert(candidate.key.clone(), attempt.clone());
                } else {
                    attempts.remove(&candidate.key);
                }
            }
            foreign
        };
        let mut dispatched = 0usize;
        let mut scheduled = Vec::new();
        for candidate in &selection.candidates {
            if let Some(attempt) = foreign.get(&candidate.key) {
                scheduler.record_dispatch(
                    &candidate.key,
                    DfsRepairDispatchResult::Sent {
                        request_id: attempt.request_id.clone(),
                    },
                    attempt.sent_unix_nanos,
                );
                continue;
            }
            let result = match self.dispatch_repair_need(&executor, candidate) {
                Ok(result) => result,
                Err(error) => {
                    // A failed dispatch is that attempt's outcome: it backs
                    // off; other needs of the pass still dispatch.
                    tracing::debug!(target: "sinomo.dfs.repair", %error,
                        object_id = %candidate.key.0, version = %candidate.key.1,
                        target = %candidate.key.2, "repair dispatch failed");
                    DfsRepairDispatchResult::Failed {
                        error: error.to_string(),
                    }
                }
            };
            // An attempt that left counts, whether its send failed at once
            // or its reply is outstanding.
            if matches!(
                result,
                DfsRepairDispatchResult::Sent { .. } | DfsRepairDispatchResult::SendFailed { .. }
            ) {
                dispatched += 1;
                scheduled.push(crate::DistributedRepairJob {
                    intent_id: candidate.need.owner_intent_id.clone().unwrap_or_default(),
                    file_system_id: candidate.need.file_system_id.clone(),
                    object_id: candidate.key.0.clone(),
                    version: candidate.key.1.clone(),
                    missing_replica_node_id: candidate.key.2.clone(),
                });
            }
            scheduler.record_dispatch(&candidate.key, result, dfs_now_unix_nanos());
        }
        for (object_id, version) in &selection.ownerless {
            // Real demand without an owner intent is enrolled through the
            // existing retained-owner path; the owner's publication changes
            // the index, which releases the need with its owner. Until then
            // the need stays tracked. Enrollment can report an active owner
            // without publishing anything for this need, so only the change
            // its publication delivered to this executor counts as enrolled.
            let enrollment = match self.reconcile_file_history_repair_root(object_id, version) {
                Ok(_)
                    if lock(&executor.events)
                        .changed
                        .values()
                        .any(|objects| objects.contains(object_id)) =>
                {
                    DfsRepairOwnerEnrollment::Enrolled
                }
                Ok(_) => match self.current_read_object_metadata(object_id) {
                    Ok(object)
                        if !self
                            .is_local_metadata_primary_for_object(&object)
                            .unwrap_or(true) =>
                    {
                        DfsRepairOwnerEnrollment::AuthorityLag
                    }
                    _ => DfsRepairOwnerEnrollment::Declined,
                },
                Err(error) => {
                    tracing::debug!(target: "sinomo.dfs.repair", %error, %object_id, %version,
                        "ownerless repair demand could not be enrolled");
                    DfsRepairOwnerEnrollment::Failed {
                        error: error.to_string(),
                    }
                }
            };
            scheduler.record_owner_enrollment(object_id, version, enrollment, dfs_now_unix_nanos());
        }
        let has_more = scheduler.has_ready_work();
        let tracked = scheduler.tracked_count();
        *lock(&executor.work) = DfsRepairExecutorWork {
            due: has_more,
            need_count: scheduler.need_count(),
            tracked_need_count: scheduler.attempted_count(),
            in_flight_count: scheduler.in_flight_count(),
            backoff_count: scheduler.backoff_count(),
            parked: scheduler.has_parked_needs(),
            awaiting_authority: scheduler.has_awaiting_authority(),
            next_due_unix_nanos: scheduler.next_due(),
        };
        executor.passes.fetch_add(1, Ordering::SeqCst);
        if executor.rewake_for_edges(&scheduler, &edges) {
            raise_wake(data_root, "edge_during_pass");
        }
        tracing::trace!(target: "sinomo.dfs.perf", stage = "dfs.repair.executor.pass",
            node_id = %self.node_id, primary_shards = scheduler.primaries().len(),
            examined = selection.examined, selected = selection.candidates.len(), dispatched,
            tracked = scheduler.tracked_count(), has_more, "repair executor pass");
        drop(scheduler);
        // The pass's detached sends move to the worker the host schedules
        // when its admission ends, in every maintenance kind.
        self.transport
            .checkpoint_background_replica_dispatch()
            .map_err(DfsError::from_network)?;
        Ok(DfsRepairExecutorPassReport {
            scheduled,
            tracked,
            examined: selection.examined,
            has_more,
        })
    }
}

/// Test support: holds the node's executor so it dispatches no repair.
/// Index changes, outcomes and edges still accumulate; dropping the hold
/// wakes the executor to serve them. Fixture startup is unaffected because
/// nothing waits for the executor before the hold is taken.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub struct DfsRepairExecutorTestHold {
    data_root: PathBuf,
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn hold_repair_executor_for_test(data_root: &Path) -> DfsRepairExecutorTestHold {
    *registry().holds.entry(data_root.to_path_buf()).or_default() += 1;
    DfsRepairExecutorTestHold {
        data_root: data_root.to_path_buf(),
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for DfsRepairExecutorTestHold {
    fn drop(&mut self) {
        let released = {
            let mut registry = registry();
            let count = registry.holds.entry(self.data_root.clone()).or_default();
            *count = count.saturating_sub(1);
            let released = *count == 0;
            if released {
                registry.holds.remove(&self.data_root);
            }
            released
        };
        if released {
            raise_wake(&self.data_root, "test_hold_released");
        }
    }
}

/// Test support: record that the last pass parked a need, the precondition
/// of a reachability wake.
#[cfg(test)]
pub(crate) fn mark_repair_executor_parked_for_test(data_root: &Path) {
    lock(&executor(data_root).work).parked = true;
}

/// Test support: every backoff of the node's executor ends now, as its timer
/// would at the backoff's end.
#[cfg(test)]
pub(crate) fn elapse_repair_executor_backoffs_for_test(data_root: &Path) {
    lock(&executor(data_root).scheduler).elapse_backoffs_for_test();
}

/// Test support: set the executor's published timer, as a pass would.
#[cfg(test)]
pub(crate) fn set_repair_executor_timer_for_test(data_root: &Path, due_unix_nanos: Option<u64>) {
    lock(&executor(data_root).work).next_due_unix_nanos = due_unix_nanos;
}

#[cfg(test)]
pub(crate) fn reset_repair_executor_for_test(data_root: &Path) {
    let mut registry = registry();
    registry.executors.remove(data_root);
    registry.wakes.remove(data_root);
}

#[cfg(test)]
pub(crate) fn bind_repair_executor_node_for_test(node_id: &NodeId, data_root: &Path) {
    let mut registry = registry();
    registry
        .node_roots
        .insert(node_id.clone(), data_root.to_path_buf());
    registry
        .executors
        .entry(data_root.to_path_buf())
        .or_default();
}

#[cfg(test)]
#[path = "repair_executor_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::park_one_repair_need_for_test;
