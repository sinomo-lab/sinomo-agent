//! Event-triggered recovery of clustered sandbox lifecycle transactions.
//!
//! A pending authoritative fence is finished when something happens to it:
//! a lease-holding request opens the sandbox, a read-only request observes
//! the fence, the component host becomes ready (this node's own wake
//! markers), or committed membership drops the node named by the fence's
//! cluster-KV index entry. The worker never lists sandbox state and has no
//! cadence; a retry exists only as a bounded follow-up to lease contention.

use std::collections::BTreeSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sinomo_core::local::SinomoDataDir;
use sinomo_core::service::ClusterAccess;

use super::ClusteredSandboxBackend;
use crate::{
    ClusteredSandboxRecoveryAttempt, SandboxBackendError, SandboxId,
    committed_delta_removes_cluster_member,
};

/// Follow-up attempts after the lifecycle lease was found held. A live
/// holder settles its own fence; the DFS advisory grant of a crashed holder
/// lapses within its 120-second lease, which this schedule spans once.
const CONTENTION_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(90),
];

/// Upper bound on waiting for a warming lease authority's stated retry time.
const WARMING_RETRY_MAX_DELAY: Duration = Duration::from_secs(130);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryTrigger {
    /// This node's own wake marker, read once at host readiness.
    Marked,
    /// An indexed fence whose publishing node left the membership.
    Orphaned,
    /// A fence reported to a read-only request.
    Observed,
}

impl RecoveryTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::Marked => "node-local wake marker",
            Self::Orphaned => "membership drop",
            Self::Observed => "observed fence",
        }
    }
}

enum RecoveryRequest {
    Marked,
    Orphaned,
    Observed(SandboxId),
}

/// Requests accepted by the worker and not yet finished, including the
/// recoveries a listing produces. Lets tests wait for work to complete.
#[derive(Default)]
struct Outstanding {
    count: Mutex<usize>,
    idle: Condvar,
}

impl Outstanding {
    fn add(&self) {
        *self.count.lock().unwrap_or_else(|error| error.into_inner()) += 1;
    }

    fn done(&self) {
        let mut count = self.count.lock().unwrap_or_else(|error| error.into_inner());
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.idle.notify_all();
        }
    }

    #[cfg(test)]
    fn wait_idle(&self, timeout: Duration) -> bool {
        let count = self.count.lock().unwrap_or_else(|error| error.into_inner());
        let (count, _) = self
            .idle
            .wait_timeout_while(count, timeout, |count| *count > 0)
            .unwrap_or_else(|error| error.into_inner());
        *count == 0
    }
}

#[cfg(test)]
static TEST_RECOVERY_SCHEDULERS: std::sync::OnceLock<
    Mutex<std::collections::BTreeMap<std::path::PathBuf, SandboxLifecycleRecoveryScheduler>>,
> = std::sync::OnceLock::new();

/// Expose a mounted component's scheduler so tests can wait for its work.
#[cfg(test)]
pub(crate) fn register_test_recovery_scheduler(
    root: std::path::PathBuf,
    scheduler: &SandboxLifecycleRecoveryScheduler,
) {
    TEST_RECOVERY_SCHEDULERS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(root, scheduler.clone());
}

#[cfg(test)]
pub(crate) fn test_recovery_scheduler(
    root: &std::path::Path,
) -> Option<SandboxLifecycleRecoveryScheduler> {
    TEST_RECOVERY_SCHEDULERS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(root)
        .cloned()
}

/// Cloneable handle that hands recovery triggers to the component's worker.
#[derive(Clone)]
pub struct SandboxLifecycleRecoveryScheduler {
    requests: tokio::sync::mpsc::UnboundedSender<RecoveryRequest>,
    outstanding: Arc<Outstanding>,
}

impl SandboxLifecycleRecoveryScheduler {
    /// Recover this node's own marked transactions once.
    pub fn recover_marked(&self) {
        self.send(RecoveryRequest::Marked);
    }

    /// Re-evaluate indexed fences after committed membership dropped a node.
    pub fn recover_orphaned(&self) {
        self.send(RecoveryRequest::Orphaned);
    }

    /// Finish a fence that a read-only request reported as pending.
    pub fn recover_observed(&self, sandbox_id: SandboxId) {
        self.send(RecoveryRequest::Observed(sandbox_id));
    }

    fn send(&self, request: RecoveryRequest) {
        self.outstanding.add();
        if self.requests.send(request).is_err() {
            self.outstanding.done();
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        self.outstanding.wait_idle(timeout)
    }
}

/// Component-owned worker executing the scheduled recoveries.
pub struct SandboxLifecycleRecoveryWorker {
    backend: ClusteredSandboxBackend,
    requests: tokio::sync::mpsc::UnboundedReceiver<RecoveryRequest>,
    outstanding: Arc<Outstanding>,
    contention_retry_delays: Vec<Duration>,
}

pub fn sandbox_lifecycle_recovery(
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn ClusterAccess>,
    runtime: tokio::runtime::Handle,
) -> (
    SandboxLifecycleRecoveryScheduler,
    SandboxLifecycleRecoveryWorker,
) {
    let (sender, requests) = tokio::sync::mpsc::unbounded_channel();
    let outstanding = Arc::new(Outstanding::default());
    (
        SandboxLifecycleRecoveryScheduler {
            requests: sender,
            outstanding: Arc::clone(&outstanding),
        },
        SandboxLifecycleRecoveryWorker {
            backend: ClusteredSandboxBackend::new(data_dir, cluster, Some(runtime)),
            requests,
            outstanding,
            contention_retry_delays: CONTENTION_RETRY_DELAYS.to_vec(),
        },
    )
}

/// sinomo:kv-consistency: Observe committed membership removal on this node's applied KV stream so indexed fences of a departed node are finished.
pub fn subscribe_sandbox_membership_recovery(
    data_dir: &SinomoDataDir,
    scheduler: SandboxLifecycleRecoveryScheduler,
) -> sinomo_core::kv::KvCommittedObservationSubscription {
    sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &sinomo_core::network::ClusterPaths::new(data_dir),
        move |observation| {
            if observation.applied_index.is_none() {
                return;
            }
            let membership_may_have_dropped = match &observation.value {
                sinomo_core::kv::KvCommittedObservationValue::Delta(delta) => {
                    committed_delta_removes_cluster_member(delta)
                }
                // Replacement and lost continuity hide individual deletes.
                sinomo_core::kv::KvCommittedObservationValue::Snapshot(_) => {
                    observation.kind == sinomo_core::kv::KvCommittedObservationKind::Replacement
                }
                sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_) => true,
            };
            if membership_may_have_dropped {
                scheduler.recover_orphaned();
            }
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Listing {
    Marked,
    Orphaned,
}

impl Listing {
    fn trigger(self) -> RecoveryTrigger {
        match self {
            Self::Marked => RecoveryTrigger::Marked,
            Self::Orphaned => RecoveryTrigger::Orphaned,
        }
    }
}

enum JobOutput {
    Listed {
        listing: Listing,
        sandbox_ids: Vec<SandboxId>,
    },
    Recovered {
        sandbox_id: SandboxId,
    },
}

#[derive(Default)]
struct ListingState {
    running: bool,
    rerun: bool,
}

impl SandboxLifecycleRecoveryWorker {
    #[cfg(test)]
    pub(crate) fn with_contention_retry_delays(mut self, delays: Vec<Duration>) -> Self {
        self.contention_retry_delays = delays;
        self
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut jobs = tokio::task::JoinSet::new();
        let mut in_flight = BTreeSet::new();
        let mut marked = ListingState::default();
        let mut orphaned = ListingState::default();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                request = self.requests.recv() => {
                    let Some(request) = request else {
                        return;
                    };
                    match request {
                        RecoveryRequest::Marked => {
                            self.request_listing(&mut jobs, &mut marked, Listing::Marked)
                        }
                        RecoveryRequest::Orphaned => {
                            self.request_listing(&mut jobs, &mut orphaned, Listing::Orphaned)
                        }
                        RecoveryRequest::Observed(sandbox_id) => self.start_recovery(
                            &mut jobs,
                            &mut in_flight,
                            sandbox_id,
                            RecoveryTrigger::Observed,
                            &shutdown,
                        ),
                    }
                }
                Some(joined) = jobs.join_next(), if !jobs.is_empty() => {
                    match joined {
                        Ok(JobOutput::Listed { listing, sandbox_ids }) => {
                            for sandbox_id in sandbox_ids {
                                self.outstanding.add();
                                self.start_recovery(
                                    &mut jobs,
                                    &mut in_flight,
                                    sandbox_id,
                                    listing.trigger(),
                                    &shutdown,
                                );
                            }
                            let state = match listing {
                                Listing::Marked => &mut marked,
                                Listing::Orphaned => &mut orphaned,
                            };
                            state.running = false;
                            self.outstanding.done();
                            if std::mem::take(&mut state.rerun) {
                                state.running = true;
                                jobs.spawn(self.listing_job(listing));
                            }
                        }
                        Ok(JobOutput::Recovered { sandbox_id }) => {
                            in_flight.remove(&sandbox_id);
                            self.outstanding.done();
                        }
                        // Blocking work runs in its own tasks, so a job only
                        // fails here if its own bookkeeping panicked.
                        Err(error) => {
                            tracing::warn!(%error, "Sandbox lifecycle recovery job failed")
                        }
                    }
                }
            }
        }
    }

    fn request_listing(
        &self,
        jobs: &mut tokio::task::JoinSet<JobOutput>,
        state: &mut ListingState,
        listing: Listing,
    ) {
        if !state.running {
            state.running = true;
            jobs.spawn(self.listing_job(listing));
        } else if !state.rerun {
            // The rerun carries this request's outstanding count.
            state.rerun = true;
        } else {
            self.outstanding.done();
        }
    }

    fn listing_job(
        &self,
        listing: Listing,
    ) -> impl std::future::Future<Output = JobOutput> + Send + 'static {
        let backend = self.backend.clone();
        async move {
            let listed = tokio::task::spawn_blocking(move || match listing {
                Listing::Marked => backend
                    .marked_lifecycle_operations()
                    .map(|marked| marked.into_iter().collect()),
                Listing::Orphaned => backend.orphaned_lifecycle_operations(),
            })
            .await;
            let sandbox_ids = match listed {
                Ok(Ok(sandbox_ids)) => sandbox_ids,
                Ok(Err(error)) => {
                    tracing::warn!(
                        trigger = listing.trigger().label(),
                        %error,
                        "Sandbox lifecycle recovery could not read its obligations"
                    );
                    Vec::new()
                }
                Err(error) => {
                    tracing::warn!(
                        trigger = listing.trigger().label(),
                        %error,
                        "Sandbox lifecycle obligation read task failed"
                    );
                    Vec::new()
                }
            };
            JobOutput::Listed {
                listing,
                sandbox_ids,
            }
        }
    }

    fn start_recovery(
        &self,
        jobs: &mut tokio::task::JoinSet<JobOutput>,
        in_flight: &mut BTreeSet<SandboxId>,
        sandbox_id: SandboxId,
        trigger: RecoveryTrigger,
        shutdown: &tokio::sync::watch::Receiver<bool>,
    ) {
        if !in_flight.insert(sandbox_id.clone()) {
            self.outstanding.done();
            return;
        }
        let backend = self.backend.clone();
        let delays = self.contention_retry_delays.clone();
        let shutdown = shutdown.clone();
        jobs.spawn(async move {
            recover_with_bounded_follow_up(backend, &sandbox_id, trigger, delays, shutdown).await;
            JobOutput::Recovered { sandbox_id }
        });
    }
}

async fn recover_with_bounded_follow_up(
    backend: ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
    trigger: RecoveryTrigger,
    delays: Vec<Duration>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut delays = delays.into_iter();
    loop {
        let attempt_backend = backend.clone();
        let attempt_id = sandbox_id.clone();
        let attempt = tokio::task::spawn_blocking(move || {
            attempt_backend.recover_lifecycle_operation(&attempt_id)
        })
        .await;
        let retry_after_unix_nanos = match attempt {
            Ok(Ok(ClusteredSandboxRecoveryAttempt::Settled)) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    trigger = trigger.label(),
                    "Sandbox lifecycle recovery settled"
                );
                return;
            }
            Ok(Ok(ClusteredSandboxRecoveryAttempt::Deferred {
                retry_after_unix_nanos,
            })) => retry_after_unix_nanos,
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    trigger = trigger.label(),
                    %error,
                    "authoritative sandbox transaction remains fenced"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "Sandbox lifecycle recovery task failed"
                );
                return;
            }
        };
        let Some(contention_delay) = delays.next() else {
            tracing::info!(
                sandbox_id = %sandbox_id,
                trigger = trigger.label(),
                "sandbox lifecycle lease stayed held; the fence recovers on its next access, restart or membership change"
            );
            return;
        };
        let delay = retry_after_unix_nanos
            .map(|retry_after| until_unix_nanos(retry_after).min(WARMING_RETRY_MAX_DELAY))
            .unwrap_or(contention_delay);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
        let check_backend = backend.clone();
        let check_id = sandbox_id.clone();
        match tokio::task::spawn_blocking(move || {
            still_obligated(&check_backend, &check_id, trigger)
        })
        .await
        {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return,
            Ok(Err(error)) => {
                tracing::warn!(sandbox_id = %sandbox_id, %error, "Sandbox lifecycle recovery recheck failed");
                return;
            }
            Err(error) => {
                tracing::warn!(sandbox_id = %sandbox_id, %error, "Sandbox lifecycle recovery recheck task failed");
                return;
            }
        }
    }
}

/// Cheap local check before a follow-up attempt: the holder that blocked
/// the previous attempt may have settled or taken over the obligation.
fn still_obligated(
    backend: &ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
    trigger: RecoveryTrigger,
) -> Result<bool, SandboxBackendError> {
    match trigger {
        RecoveryTrigger::Marked => Ok(backend
            .recovery_state()
            .is_authoritative_recovery_pending(sandbox_id)),
        RecoveryTrigger::Orphaned => Ok(backend
            .orphaned_lifecycle_operations()?
            .contains(sandbox_id)),
        RecoveryTrigger::Observed => Ok(true),
    }
}

fn until_unix_nanos(target_unix_nanos: u64) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    Duration::from_nanos(target_unix_nanos.saturating_sub(now))
}
