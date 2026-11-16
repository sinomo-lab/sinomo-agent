use crate::{
    DfsError, DfsFileSystemId, DfsMessageExecutionScope, DfsMetadataPublishScheduler,
    DfsMetadataShardId,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

/// Default bound for shard-specific mutation gates retained by one DFS runtime.
///
/// Shard identities may originate on the wire, so the admission layer must not
/// retain an unbounded gate for every identity it observes. Once this bound is
/// reached, new shard identities conservatively use global admission.
pub(crate) const DFS_MUTATION_SHARD_GATE_LIMIT: usize = 256;

#[derive(Debug)]
pub(crate) enum DfsMaintenancePreemptionOutcome<T> {
    Finished(T),
    ForegroundWaiting,
}

pub(crate) async fn acquire_dfs_maintenance_admission_until_foreground_waits<F, T, E>(
    foreground_preemption: &DfsMaintenanceForegroundPreemption,
    admission: F,
) -> Result<Option<T>, E>
where
    F: Future<Output = Result<T, E>>,
{
    let mut admission = Box::pin(admission);
    let outcome = tokio::select! {
        biased;
        _ = foreground_preemption.requested() => None,
        result = &mut admission => Some(result?),
    };
    if outcome.is_none() {
        let drop_started = Instant::now();
        drop(admission);
        tracing::trace!(
            target: "sinomo.dfs.perf",
            admission_future_drop_us = drop_started.elapsed().as_micros() as u64,
            "DFS maintenance admission cancelled for foreground traffic"
        );
    }
    Ok(outcome)
}

pub(crate) async fn run_dfs_maintenance_until_foreground_waits<F>(
    foreground_preemption: &DfsMaintenanceForegroundPreemption,
    maintenance: F,
) -> DfsMaintenancePreemptionOutcome<F::Output>
where
    F: Future,
{
    let mut maintenance = Box::pin(maintenance);
    let outcome = tokio::select! {
        biased;
        _ = foreground_preemption.requested() => {
            DfsMaintenancePreemptionOutcome::ForegroundWaiting
        }
        result = std::future::poll_fn(|cx| {
            let started = crate::dfs_perf_trace_enabled().then(Instant::now);
            let result = maintenance.as_mut().poll(cx);
            if let Some(started) = started {
                tracing::trace!(
                    target: "sinomo.dfs.perf",
                    phase = "poll",
                    ready = result.is_ready(),
                    foreground_requested = foreground_preemption.is_requested(),
                    elapsed_us = started.elapsed().as_micros() as u64,
                    "dfs-maintenance-poll"
                );
            }
            result
        }) => DfsMaintenancePreemptionOutcome::Finished(result),
    };
    let maintenance_preempted =
        matches!(&outcome, DfsMaintenancePreemptionOutcome::ForegroundWaiting);
    let maintenance_drop_started = Instant::now();
    drop(maintenance);
    tracing::trace!(
        target: "sinomo.dfs.perf",
        maintenance_preempted,
        maintenance_future_drop_us = maintenance_drop_started.elapsed().as_micros() as u64,
        "DFS maintenance future destroyed before admission release"
    );
    outcome
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DfsMutationLaneKey {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DfsMetadataLaneKey {
    pub data_root: PathBuf,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
}

impl DfsMetadataLaneKey {
    pub fn new(
        data_root: impl Into<PathBuf>,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    ) -> Self {
        Self {
            data_root: data_root.into(),
            file_system_id,
            shard_id,
        }
    }
}

impl DfsMutationLaneKey {
    pub fn new(file_system_id: DfsFileSystemId, shard_id: DfsMetadataShardId) -> Self {
        Self {
            file_system_id,
            shard_id,
        }
    }

    pub fn from_scope(scope: &DfsMessageExecutionScope) -> Option<Self> {
        match scope {
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id,
            } => Some(Self::new(file_system_id.clone(), shard_id.clone())),
            _ => None,
        }
    }
}

/// Owner-crate admission state for concurrent DFS request and maintenance work.
///
/// Exact-shard mutations may run concurrently across distinct shards. Global
/// topology changes exclude all exact work, while immutable content reads do
/// not borrow mutation admission at all. Maintenance publishes an interest
/// generation so foreground work can preempt a pass before it monopolizes the
/// global generation.
///
/// A node's answers to a peer's in-flight operation (journal batches, write-set
/// probes and snapshot installs) are admitted on their exact shard lane past a
/// global writer that is only waiting, and never while one holds the node. The
/// peer's operation may be what that writer waits for, through the peer
/// waiting on this answer; ordinary exact work still queues behind the writer.
#[derive(Debug)]
pub(crate) struct DfsMutationScheduler {
    global: Arc<tokio::sync::RwLock<()>>,
    answers: Arc<DfsAnswerGate>,
    shard_gates: Mutex<BTreeMap<DfsMutationLaneKey, Arc<tokio::sync::Semaphore>>>,
    shard_gate_limit: usize,
    maintenance_interest_count: AtomicUsize,
    foreground_generation: Arc<crate::service_state::DfsCancellationGeneration>,
    foreground_notify: tokio::sync::Notify,
}

impl Default for DfsMutationScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsMutationScheduler {
    pub fn new() -> Self {
        Self::with_shard_gate_limit(DFS_MUTATION_SHARD_GATE_LIMIT)
    }

    pub fn with_shard_gate_limit(shard_gate_limit: usize) -> Self {
        Self {
            global: Arc::new(tokio::sync::RwLock::new(())),
            answers: Arc::default(),
            shard_gates: Mutex::new(BTreeMap::new()),
            shard_gate_limit,
            maintenance_interest_count: AtomicUsize::new(0),
            foreground_generation: Arc::default(),
            foreground_notify: tokio::sync::Notify::new(),
        }
    }

    pub fn begin_maintenance_interest(self: &Arc<Self>) -> DfsMaintenanceForegroundPreemption {
        // Capture the baseline before publishing maintenance interest. Any
        // foreground waiter that can observe the interest therefore advances
        // a generation this guard will observe, including while another host
        // capability is still being acquired.
        let foreground_generation = self.foreground_generation.current();
        self.maintenance_interest_count
            .fetch_add(1, Ordering::AcqRel);
        DfsMaintenanceForegroundPreemption {
            scheduler: Arc::clone(self),
            foreground_generation,
        }
    }

    pub fn request_foreground_admission(&self) {
        if self.maintenance_interest_count.load(Ordering::Acquire) == 0 {
            return;
        }
        // Also wakes a synchronous maintenance wait whose async caller cannot
        // be polled until that wait returns.
        self.foreground_generation.advance();
        self.foreground_notify.notify_waiters();
    }

    #[cfg(test)]
    pub fn maintenance_interest_count(&self) -> usize {
        self.maintenance_interest_count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn global_is_exclusively_held(&self) -> bool {
        self.global.try_read().is_err()
    }

    pub fn try_admit_global(self: &Arc<Self>) -> Option<DfsMutationAdmission> {
        let global = Arc::clone(&self.global).try_write_owned().ok()?;
        let answers = self.answers.try_hold()?;
        Some(DfsMutationAdmission::Global {
            _scheduler: Arc::clone(self),
            _answers: answers,
            _global: global,
        })
    }

    async fn admit_global(self: &Arc<Self>) -> DfsMutationAdmission {
        let global = Arc::clone(&self.global).write_owned().await;
        // Answers admitted while this writer waited finish before it holds the
        // node; no new answer starts until it releases.
        let answers = self.answers.hold().await;
        DfsMutationAdmission::Global {
            _scheduler: Arc::clone(self),
            _answers: answers,
            _global: global,
        }
    }

    fn shard_gate(
        &self,
        key: &DfsMutationLaneKey,
    ) -> Result<Option<Arc<tokio::sync::Semaphore>>, DfsError> {
        let mut gates = self.shard_gates.lock().map_err(|_| DfsError::Network {
            message: "DFS mutation scheduler shard-gate map is poisoned".to_string(),
        })?;
        if let Some(gate) = gates.get(key) {
            return Ok(Some(Arc::clone(gate)));
        }
        if gates.len() >= self.shard_gate_limit {
            return Ok(None);
        }
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        gates.insert(key.clone(), Arc::clone(&gate));
        Ok(Some(gate))
    }

    pub async fn admit(
        self: &Arc<Self>,
        scope: &DfsMessageExecutionScope,
    ) -> Result<DfsMutationAdmission, DfsError> {
        let Some(key) = DfsMutationLaneKey::from_scope(scope) else {
            if matches!(scope, DfsMessageExecutionScope::ReadOnly) {
                return Ok(DfsMutationAdmission::ReadOnly {
                    _scheduler: Arc::clone(self),
                });
            }
            let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
            let admission = self.admit_global().await;
            trace_dfs_admission_gate(self, scope, "global-write", gate_started);
            return Ok(admission);
        };

        // Reserve the exact shard before joining the global read side. A
        // same-shard backlog therefore waits without holding read guards, so
        // Tokio's write-preferring RwLock can admit a queued global mutation
        // after only the currently active exact owners finish.
        let Some(shard_gate) = self.shard_gate(&key)? else {
            let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
            let admission = self.admit_global().await;
            trace_dfs_admission_gate(self, scope, "global-write", gate_started);
            return Ok(admission);
        };
        let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let shard = shard_gate
            .acquire_owned()
            .await
            .map_err(|_| DfsError::Network {
                message: format!(
                    "DFS mutation scheduler closed the {}:{} shard gate",
                    key.file_system_id, key.shard_id.0
                ),
            })?;
        trace_dfs_admission_gate(self, scope, "shard-semaphore", gate_started);
        let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let global = Arc::clone(&self.global).read_owned().await;
        trace_dfs_admission_gate(self, scope, "global-read", gate_started);
        Ok(DfsMutationAdmission::ExactShard {
            _scheduler: Arc::clone(self),
            _global: DfsExactGlobalAdmission::Read(global),
            _shard: shard,
        })
    }

    /// Admit this node's answer to a peer's in-flight operation on `scope`'s
    /// exact shard lane. It queues behind local work on the same shard, as any
    /// exact admission does, and never runs while a global writer holds the
    /// node, but it passes a global writer that is only waiting: the peer's
    /// operation may be what that writer waits for. A scope without an exact
    /// lane is admitted as `admit` admits it.
    pub async fn admit_answer(
        self: &Arc<Self>,
        scope: &DfsMessageExecutionScope,
    ) -> Result<DfsMutationAdmission, DfsError> {
        let Some(key) = DfsMutationLaneKey::from_scope(scope) else {
            return self.admit(scope).await;
        };
        let Some(shard_gate) = self.shard_gate(&key)? else {
            return self.admit(scope).await;
        };
        let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let shard = shard_gate
            .acquire_owned()
            .await
            .map_err(|_| DfsError::Network {
                message: format!(
                    "DFS mutation scheduler closed the {}:{} shard gate",
                    key.file_system_id, key.shard_id.0
                ),
            })?;
        trace_dfs_admission_gate(self, scope, "shard-semaphore", gate_started);
        let gate_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let answer = self.answers.enter().await;
        trace_dfs_admission_gate(self, scope, "peer-answer", gate_started);
        Ok(DfsMutationAdmission::ExactShard {
            _scheduler: Arc::clone(self),
            _global: DfsExactGlobalAdmission::Answer(answer),
            _shard: shard,
        })
    }
}

/// Answers to peers in flight on a node, and whether a global writer holds it.
#[derive(Debug, Default)]
struct DfsAnswerGate {
    state: Mutex<DfsAnswerGateState>,
    changed: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct DfsAnswerGateState {
    writer_holds: bool,
    answers: usize,
}

impl DfsAnswerGate {
    fn lock(&self) -> std::sync::MutexGuard<'_, DfsAnswerGateState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Enter one answer once no global writer holds the node.
    async fn enter(self: &Arc<Self>) -> DfsAnswerAdmission {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock();
                if !state.writer_holds {
                    state.answers += 1;
                    return DfsAnswerAdmission {
                        gate: Arc::clone(self),
                    };
                }
            }
            changed.await;
        }
    }

    /// Close the gate to new answers for a writer that already holds the
    /// global lock, then wait for the answers already admitted to finish.
    async fn hold(self: &Arc<Self>) -> DfsAnswerWriterHold {
        let hold = {
            let mut state = self.lock();
            state.writer_holds = true;
            DfsAnswerWriterHold {
                gate: Arc::clone(self),
            }
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.lock().answers == 0 {
                return hold;
            }
            changed.await;
        }
    }

    fn try_hold(self: &Arc<Self>) -> Option<DfsAnswerWriterHold> {
        let mut state = self.lock();
        if state.answers > 0 {
            return None;
        }
        state.writer_holds = true;
        Some(DfsAnswerWriterHold {
            gate: Arc::clone(self),
        })
    }
}

/// One admitted answer; dropping it may let a holding writer proceed.
#[derive(Debug)]
pub(crate) struct DfsAnswerAdmission {
    gate: Arc<DfsAnswerGate>,
}

impl Drop for DfsAnswerAdmission {
    fn drop(&mut self) {
        let last = {
            let mut state = self.gate.lock();
            state.answers = state.answers.saturating_sub(1);
            state.answers == 0
        };
        if last {
            self.gate.changed.notify_waiters();
        }
    }
}

/// A global writer's exclusion of answers, from its drain until it releases.
#[derive(Debug)]
pub(crate) struct DfsAnswerWriterHold {
    gate: Arc<DfsAnswerGate>,
}

impl Drop for DfsAnswerWriterHold {
    fn drop(&mut self) {
        self.gate.lock().writer_holds = false;
        self.gate.changed.notify_waiters();
    }
}

/// The global side an exact admission holds: the read side of the global
/// lock, or, for an answer to a peer, the answer gate.
#[derive(Debug)]
#[allow(dead_code)] // Each variant owns its guard solely for Drop.
pub(crate) enum DfsExactGlobalAdmission {
    Read(tokio::sync::OwnedRwLockReadGuard<()>),
    Answer(DfsAnswerAdmission),
}

fn trace_dfs_admission_gate(
    scheduler: &DfsMutationScheduler,
    scope: &DfsMessageExecutionScope,
    phase: &str,
    started: Option<Instant>,
) {
    let Some(started) = started else {
        return;
    };
    let (requested_scope, fs, shard) = match scope {
        DfsMessageExecutionScope::ExactShard {
            file_system_id,
            shard_id,
        } => ("exact", file_system_id.as_str(), shard_id.0.as_str()),
        DfsMessageExecutionScope::ReadOnly => ("read-only", "-", "-"),
        _ => ("global", "-", "-"),
    };
    tracing::trace!(
        target: "sinomo.dfs.perf",
        scheduler = %format_args!("{scheduler:p}"),
        requested_scope, fs, shard, phase,
        elapsed_us = started.elapsed().as_micros() as u64,
        "dfs-admission-gate"
    );
}

#[derive(Debug)]
pub(crate) enum DfsMutationAdmission {
    ReadOnly {
        _scheduler: Arc<DfsMutationScheduler>,
    },
    ExactShard {
        _scheduler: Arc<DfsMutationScheduler>,
        _global: DfsExactGlobalAdmission,
        _shard: tokio::sync::OwnedSemaphorePermit,
    },
    Global {
        _scheduler: Arc<DfsMutationScheduler>,
        // Released in reverse of acquisition: the answer gate, then the lock.
        _answers: DfsAnswerWriterHold,
        _global: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

impl DfsMutationAdmission {
    pub fn is_global(&self) -> bool {
        matches!(self, Self::Global { .. })
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, Self::ExactShard { .. })
    }

    pub(crate) fn scheduler(&self) -> Arc<DfsMutationScheduler> {
        match self {
            Self::ReadOnly { _scheduler }
            | Self::ExactShard { _scheduler, .. }
            | Self::Global { _scheduler, .. } => Arc::clone(_scheduler),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DfsMaintenanceForegroundPreemption {
    scheduler: Arc<DfsMutationScheduler>,
    foreground_generation: u64,
}

impl DfsMaintenanceForegroundPreemption {
    pub(crate) fn cancellation_flag(&self) -> crate::DfsCancellationFlag {
        crate::DfsCancellationFlag::after_generation_change(
            Arc::clone(&self.scheduler.foreground_generation),
            self.foreground_generation,
        )
    }

    pub fn is_requested(&self) -> bool {
        self.scheduler.foreground_generation.current() != self.foreground_generation
    }

    pub async fn requested(&self) {
        loop {
            let notified = self.scheduler.foreground_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for DfsMaintenanceForegroundPreemption {
    fn drop(&mut self) {
        let previous = self
            .scheduler
            .maintenance_interest_count
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "DFS maintenance interest underflow");
    }
}

/// Weak, bounded process registry for independently-owned DFS runtime
/// schedulers. The registry is an index only; active callers own the scheduler.
#[derive(Debug, Default)]
pub(crate) struct DfsMutationSchedulerRegistry {
    schedulers: Mutex<BTreeMap<PathBuf, Weak<DfsMutationScheduler>>>,
}

/// Weak owner-crate registry for coalescing metadata workers by host-supplied
/// identity. Hosts choose keys; DFS owns scheduler lifetime and pruning.
#[derive(Debug)]
pub(crate) struct DfsMetadataSchedulerRegistry<K> {
    label: &'static str,
    schedulers: Mutex<BTreeMap<K, Weak<DfsMetadataPublishScheduler>>>,
}

impl<K> DfsMetadataSchedulerRegistry<K>
where
    K: Clone + Ord,
{
    pub fn new(label: &'static str) -> Self {
        Self {
            label,
            schedulers: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn scheduler(&self, key: K) -> Result<Arc<DfsMetadataPublishScheduler>, DfsError> {
        let mut schedulers = self.schedulers.lock().map_err(|_| DfsError::Network {
            message: format!("{} registry is poisoned", self.label),
        })?;
        if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
            return Ok(scheduler);
        }
        schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        schedulers.insert(key, Arc::downgrade(&scheduler));
        Ok(scheduler)
    }

    #[cfg(test)]
    pub fn contains_live(&self, key: &K) -> bool {
        self.schedulers
            .lock()
            .ok()
            .and_then(|schedulers| schedulers.get(key).and_then(Weak::upgrade))
            .is_some()
    }

    pub fn live_keys(&self) -> Result<Vec<K>, DfsError> {
        let mut schedulers = self.schedulers.lock().map_err(|_| DfsError::Network {
            message: format!("{} registry is poisoned", self.label),
        })?;
        schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
        Ok(schedulers.keys().cloned().collect())
    }
}

impl DfsMutationSchedulerRegistry {
    pub fn scheduler(
        &self,
        data_root: impl Into<PathBuf>,
    ) -> Result<Arc<DfsMutationScheduler>, DfsError> {
        let key = data_root.into();
        let mut schedulers = self.schedulers.lock().map_err(|_| DfsError::Network {
            message: "DFS mutation scheduler registry is poisoned".to_string(),
        })?;
        if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
            return Ok(scheduler);
        }
        schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
        let scheduler = Arc::new(DfsMutationScheduler::new());
        schedulers.insert(key, Arc::downgrade(&scheduler));
        Ok(scheduler)
    }
}

/// Test support for answer admission, keyed by a watched request id:
/// whether a replica answer reached its admission, a gate that holds it there,
/// and how often its record view was checked while it waited.
#[cfg(test)]
pub(crate) mod answer_admission_test_hooks {
    use std::collections::BTreeMap;
    use std::sync::{Arc, LazyLock, Mutex};

    #[derive(Default)]
    struct Watched {
        gate: Option<Arc<tokio::sync::Semaphore>>,
        arrived: bool,
        view_checks: usize,
    }

    static WATCHED: LazyLock<Mutex<BTreeMap<String, Watched>>> = LazyLock::new(Default::default);

    fn watched() -> std::sync::MutexGuard<'static, BTreeMap<String, Watched>> {
        WATCHED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Observe `request_id` without holding it.
    pub(crate) fn watch(request_id: &str) {
        watched().entry(request_id.to_owned()).or_default();
    }

    /// Hold `request_id` just before its admission until the returned gate
    /// gets a permit.
    pub(crate) fn hold_before_admission(request_id: &str) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        watched().entry(request_id.to_owned()).or_default().gate = Some(Arc::clone(&gate));
        gate
    }

    /// Whether the watched request reached its admission.
    pub(crate) fn arrived(request_id: &str) -> bool {
        watched()
            .get(request_id)
            .is_some_and(|watched| watched.arrived)
    }

    /// How often the watched request's record view was checked while it
    /// waited for admission.
    pub(crate) fn view_checks(request_id: &str) -> usize {
        watched()
            .get(request_id)
            .map_or(0, |watched| watched.view_checks)
    }

    pub(crate) async fn before_admission(request_id: &str) {
        let gate = {
            let mut watched = watched();
            let Some(entry) = watched.get_mut(request_id) else {
                return;
            };
            entry.arrived = true;
            entry.gate.clone()
        };
        if let Some(gate) = gate {
            let _ = gate.acquire().await;
        }
    }

    pub(crate) fn record_view_check(request_id: &str) {
        if let Some(entry) = watched().get_mut(request_id) {
            entry.view_checks += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn scope(file_system: &str, shard: &str) -> DfsMessageExecutionScope {
        DfsMessageExecutionScope::ExactShard {
            file_system_id: DfsFileSystemId::from_legacy_named(file_system)
                .expect("test filesystem id"),
            shard_id: DfsMetadataShardId::new(shard),
        }
    }

    /// Two nodes each hold one shard's lane while a keeper there waits on
    /// the other node, and each node has a takeover activation queued for its
    /// global writer. Each node's answer to the other's in-flight request
    /// needs only that request's free shard lane. As an ordinary exact
    /// admission it would queue behind the waiting local writer, which waits
    /// for the local keeper, which waits for the other node's answer: a
    /// two-node cycle. Answers pass the waiting writers and break it.
    #[tokio::test(flavor = "multi_thread")]
    async fn answers_to_peers_pass_waiting_global_writers_in_a_two_node_cycle() {
        let x = Arc::new(DfsMutationScheduler::new());
        let y = Arc::new(DfsMutationScheduler::new());
        let on_x = scope("answer-cycle-fs", "x-primary");
        let on_y = scope("answer-cycle-fs", "y-primary");
        let keeper_x = x.admit(&on_x).await.expect("keeper on x");
        let keeper_y = y.admit(&on_y).await.expect("keeper on y");
        let writer_x = tokio::spawn({
            let x = Arc::clone(&x);
            async move { x.admit(&DfsMessageExecutionScope::GlobalExclusive).await }
        });
        let writer_y = tokio::spawn({
            let y = Arc::clone(&y);
            async move { y.admit(&DfsMessageExecutionScope::GlobalExclusive).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Ordinary exact work still queues behind a waiting writer.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), x.admit(&on_y))
                .await
                .is_err()
        );
        let (on_y_answer, on_x_answer) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(y.admit_answer(&on_x), x.admit_answer(&on_y))
        })
        .await
        .expect("each answer passes its node's waiting writer");
        drop(on_y_answer.expect("answer on y"));
        drop(on_x_answer.expect("answer on x"));
        // The keepers got their answers and release; the writers then hold.
        drop(keeper_x);
        drop(keeper_y);
        for writer in [writer_x, writer_y] {
            let held = tokio::time::timeout(Duration::from_secs(2), writer)
                .await
                .expect("the writer is admitted once the keeper releases")
                .expect("join writer")
                .expect("writer admission");
            assert!(held.is_global());
        }
    }

    /// An answer never runs while a global writer holds the node, and a
    /// writer waits for answers admitted before it holds, which bounds its
    /// wait by the answers already in flight.
    #[tokio::test(flavor = "multi_thread")]
    async fn answers_never_overlap_a_holding_global_writer() {
        let node = Arc::new(DfsMutationScheduler::new());
        let lane = scope("answer-exclusion-fs", "lane");
        let writer = node
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await
            .expect("writer holds");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), node.admit_answer(&lane))
                .await
                .is_err(),
            "no answer while a writer holds"
        );
        assert!(node.try_admit_global().is_none());
        drop(writer);

        let answer = node.admit_answer(&lane).await.expect("answer admitted");
        let mut queued_writer = Box::pin(node.admit(&DfsMessageExecutionScope::GlobalExclusive));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), queued_writer.as_mut())
                .await
                .is_err(),
            "the writer waits for the answer in flight"
        );
        assert!(node.try_admit_global().is_none());
        drop(answer);
        let writer = tokio::time::timeout(Duration::from_secs(1), queued_writer)
            .await
            .expect("the writer holds once the answer finished")
            .expect("writer admission");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), node.admit_answer(&lane))
                .await
                .is_err(),
            "a new answer waits for the holding writer"
        );
        drop(writer);
        node.admit_answer(&lane)
            .await
            .expect("answers resume after the writer releases");
    }

    #[test]
    fn scheduler_bounds_wire_derived_shard_gates() {
        let scheduler = DfsMutationScheduler::with_shard_gate_limit(2);
        let file_system_id =
            DfsFileSystemId::from_legacy_named("scheduler-gate-bound-fs").expect("filesystem");
        for shard in ["root", "services"] {
            assert!(
                scheduler
                    .shard_gate(&DfsMutationLaneKey::new(
                        file_system_id.clone(),
                        DfsMetadataShardId::new(shard),
                    ))
                    .expect("bounded shard gate")
                    .is_some()
            );
        }
        assert!(
            scheduler
                .shard_gate(&DfsMutationLaneKey::new(
                    file_system_id,
                    DfsMetadataShardId::new("overflow"),
                ))
                .expect("overflow shard gate")
                .is_none()
        );
    }

    #[tokio::test]
    async fn disjoint_shards_are_admitted_concurrently() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let first = scheduler
            .admit(&scope("scheduler-fs", "root"))
            .await
            .unwrap();
        let second = tokio::time::timeout(
            Duration::from_secs(1),
            scheduler.admit(&scope("scheduler-fs", "services")),
        )
        .await
        .expect("disjoint shard must not wait")
        .unwrap();
        drop((first, second));
    }

    #[tokio::test]
    async fn same_shard_is_serialized() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let exact_scope = scope("scheduler-fs", "root");
        let first = scheduler.admit(&exact_scope).await.unwrap();
        let waiter_scheduler = Arc::clone(&scheduler);
        let waiter_scope = exact_scope.clone();
        let mut waiter = tokio::spawn(async move { waiter_scheduler.admit(&waiter_scope).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiter)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("same-shard waiter advances")
            .expect("waiter task")
            .expect("second admission");
        drop(second);
    }

    #[tokio::test]
    async fn cancelled_same_shard_waiter_does_not_leak_admission() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let exact_scope = scope("scheduler-fs", "root");
        let first = scheduler.admit(&exact_scope).await.unwrap();
        let waiter_scheduler = Arc::clone(&scheduler);
        let waiter_scope = exact_scope.clone();
        let mut waiter = tokio::spawn(async move { waiter_scheduler.admit(&waiter_scope).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiter)
                .await
                .is_err()
        );
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        drop(first);
        let next = tokio::time::timeout(Duration::from_secs(1), scheduler.admit(&exact_scope))
            .await
            .expect("admission advances after cancellation")
            .expect("next admission");
        drop(next);
    }

    #[tokio::test]
    async fn global_admission_excludes_exact_shards() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let exact_scope = scope("scheduler-fs", "root");
        let exact = scheduler.admit(&exact_scope).await.unwrap();
        let global_scheduler = Arc::clone(&scheduler);
        let mut global = tokio::spawn(async move {
            global_scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut global)
                .await
                .is_err()
        );
        drop(exact);
        let global = tokio::time::timeout(Duration::from_secs(1), global)
            .await
            .expect("global advances")
            .expect("global task")
            .expect("global admission");
        drop(global);
    }

    #[tokio::test]
    async fn same_shard_backlog_does_not_delay_global_admission() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let exact_scope = scope("scheduler-fs", "root");
        let first = scheduler.admit(&exact_scope).await.unwrap();

        let queued_scheduler = Arc::clone(&scheduler);
        let queued_scope = exact_scope.clone();
        let mut queued_exact =
            tokio::spawn(async move { queued_scheduler.admit(&queued_scope).await });
        tokio::task::yield_now().await;

        let global_scheduler = Arc::clone(&scheduler);
        let global = tokio::spawn(async move {
            global_scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await
        });
        drop(first);
        let global = tokio::time::timeout(Duration::from_secs(1), global)
            .await
            .expect("global bypasses same-shard backlog")
            .expect("global task")
            .expect("global admission");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut queued_exact)
                .await
                .is_err()
        );
        drop(global);
        let exact = tokio::time::timeout(Duration::from_secs(1), queued_exact)
            .await
            .expect("exact waiter advances")
            .expect("exact task")
            .expect("exact admission");
        drop(exact);
    }

    #[tokio::test]
    async fn foreground_request_preempts_maintenance_interest() {
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let interest = scheduler.begin_maintenance_interest();
        assert!(!interest.is_requested());
        scheduler.request_foreground_admission();
        tokio::time::timeout(Duration::from_secs(1), interest.requested())
            .await
            .expect("maintenance observes foreground request");
    }

    #[test]
    fn registry_reuses_only_live_scheduler_for_same_root() {
        let registry = DfsMutationSchedulerRegistry::default();
        let first = registry.scheduler("first").unwrap();
        let same = registry.scheduler("first").unwrap();
        let other = registry.scheduler("second").unwrap();
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &other));
    }
}
