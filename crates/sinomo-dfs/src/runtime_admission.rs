use crate::{
    DfsError, DfsFileSystemId, DfsMessageExecutionScope, DfsMetadataPublishScheduler,
    DfsMetadataShardId,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Instant;

/// Default bound for shard-specific mutation gates retained by one DFS runtime.
///
/// Shard identities may originate on the wire, so the admission layer must not
/// retain an unbounded gate for every identity it observes. Once this bound is
/// reached, new shard identities conservatively use global admission.
pub const DFS_MUTATION_SHARD_GATE_LIMIT: usize = 256;

#[derive(Debug)]
pub enum DfsMaintenancePreemptionOutcome<T> {
    Finished(T),
    ForegroundWaiting,
}

pub async fn acquire_dfs_maintenance_admission_until_foreground_waits<F, T, E>(
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

pub async fn run_dfs_maintenance_until_foreground_waits<F>(
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
        result = &mut maintenance => DfsMaintenancePreemptionOutcome::Finished(result),
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
pub struct DfsMutationLaneKey {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DfsMetadataLaneKey {
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
#[derive(Debug)]
pub struct DfsMutationScheduler {
    global: Arc<tokio::sync::RwLock<()>>,
    shard_gates: Mutex<BTreeMap<DfsMutationLaneKey, Arc<tokio::sync::Semaphore>>>,
    shard_gate_limit: usize,
    maintenance_interest_count: AtomicUsize,
    foreground_generation: AtomicU64,
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
            shard_gates: Mutex::new(BTreeMap::new()),
            shard_gate_limit,
            maintenance_interest_count: AtomicUsize::new(0),
            foreground_generation: AtomicU64::new(0),
            foreground_notify: tokio::sync::Notify::new(),
        }
    }

    pub fn begin_maintenance_interest(self: &Arc<Self>) -> DfsMaintenanceForegroundPreemption {
        // Capture the baseline before publishing maintenance interest. Any
        // foreground waiter that can observe the interest therefore advances
        // a generation this guard will observe, including while another host
        // capability is still being acquired.
        let foreground_generation = self.foreground_generation.load(Ordering::Acquire);
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
        self.foreground_generation.fetch_add(1, Ordering::AcqRel);
        self.foreground_notify.notify_waiters();
    }

    pub fn maintenance_interest_count(&self) -> usize {
        self.maintenance_interest_count.load(Ordering::Acquire)
    }

    pub fn global_is_exclusively_held(&self) -> bool {
        self.global.try_read().is_err()
    }

    pub fn try_admit_global(self: &Arc<Self>) -> Option<DfsMutationAdmission> {
        Arc::clone(&self.global)
            .try_write_owned()
            .ok()
            .map(|global| DfsMutationAdmission::Global {
                _scheduler: Arc::clone(self),
                _global: global,
            })
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
            let global = Arc::clone(&self.global).write_owned().await;
            return Ok(DfsMutationAdmission::Global {
                _scheduler: Arc::clone(self),
                _global: global,
            });
        };

        // Reserve the exact shard before joining the global read side. A
        // same-shard backlog therefore waits without holding read guards, so
        // Tokio's write-preferring RwLock can admit a queued global mutation
        // after only the currently active exact owners finish.
        let Some(shard_gate) = self.shard_gate(&key)? else {
            let global = Arc::clone(&self.global).write_owned().await;
            return Ok(DfsMutationAdmission::Global {
                _scheduler: Arc::clone(self),
                _global: global,
            });
        };
        let shard = shard_gate
            .acquire_owned()
            .await
            .map_err(|_| DfsError::Network {
                message: format!(
                    "DFS mutation scheduler closed the {}:{} shard gate",
                    key.file_system_id, key.shard_id.0
                ),
            })?;
        let global = Arc::clone(&self.global).read_owned().await;
        Ok(DfsMutationAdmission::ExactShard {
            _scheduler: Arc::clone(self),
            _global: global,
            _shard: shard,
        })
    }
}

#[derive(Debug)]
pub enum DfsMutationAdmission {
    ReadOnly {
        _scheduler: Arc<DfsMutationScheduler>,
    },
    ExactShard {
        _scheduler: Arc<DfsMutationScheduler>,
        _global: tokio::sync::OwnedRwLockReadGuard<()>,
        _shard: tokio::sync::OwnedSemaphorePermit,
    },
    Global {
        _scheduler: Arc<DfsMutationScheduler>,
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

    pub fn into_generation_fence(self) -> Option<DfsMutationGenerationFence> {
        match self {
            Self::ReadOnly { .. } => None,
            Self::ExactShard {
                _scheduler,
                _global,
                _shard,
            } => {
                drop(_shard);
                Some(DfsMutationGenerationFence::Shared {
                    _scheduler,
                    _global,
                })
            }
            Self::Global {
                _scheduler,
                _global,
            } => Some(DfsMutationGenerationFence::Exclusive {
                _scheduler,
                _global,
            }),
        }
    }
}

/// The resident metadata generation fence retained after an exact mutation has
/// durably appended replica frames.
#[derive(Debug)]
pub enum DfsMutationGenerationFence {
    Shared {
        _scheduler: Arc<DfsMutationScheduler>,
        _global: tokio::sync::OwnedRwLockReadGuard<()>,
    },
    Exclusive {
        _scheduler: Arc<DfsMutationScheduler>,
        _global: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

impl DfsMutationGenerationFence {
    pub fn begin_foreground_preemption(&self) -> DfsMaintenanceForegroundPreemption {
        self.scheduler().begin_maintenance_interest()
    }

    pub fn scheduler(&self) -> Arc<DfsMutationScheduler> {
        match self {
            Self::Shared { _scheduler, .. } | Self::Exclusive { _scheduler, .. } => {
                Arc::clone(_scheduler)
            }
        }
    }
}

#[derive(Debug)]
pub struct DfsMaintenanceForegroundPreemption {
    scheduler: Arc<DfsMutationScheduler>,
    foreground_generation: u64,
}

impl DfsMaintenanceForegroundPreemption {
    pub fn is_requested(&self) -> bool {
        self.scheduler.foreground_generation.load(Ordering::Acquire) != self.foreground_generation
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
pub struct DfsMutationSchedulerRegistry {
    schedulers: Mutex<BTreeMap<PathBuf, Weak<DfsMutationScheduler>>>,
}

/// Weak owner-crate registry for coalescing metadata workers by host-supplied
/// identity. Hosts choose keys; DFS owns scheduler lifetime and pruning.
#[derive(Debug)]
pub struct DfsMetadataSchedulerRegistry<K> {
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
