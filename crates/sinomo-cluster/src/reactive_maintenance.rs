use crate::control_plane_driver::ClusterControlPlaneDriver;
use crate::kv_local_store::{
    ClusterKvPathsExt, kv_err, locked_cache_cluster_kv_snapshot_if_newer_than_raft,
    locked_kv_state_authority_id_async, read_cluster_kv_snapshot,
    write_imported_cluster_kv_snapshot_if_changed,
};
use crate::kv_txn_quorum::{
    ClusterKvTxnQuorumAsyncHost, read_cluster_kv_authoritative_snapshot_async_with_host,
};
use crate::local_state_store::project_committed_cluster_raft_config;
use crate::prelude::*;
use crate::raft_election::elect_cluster_raft_leader_async_with_host;
use crate::raft_fanout::{
    ClusterKvCommittedFanoutAsyncHost, ClusterRaftCommittedFanoutReport,
    fan_out_committed_cluster_raft_command_async_with_host,
};
use sinomo_kv::{KvRevision, KvSnapshot};
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector, ClusterMembershipRpcFuture,
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterPaths, ClusterSignedObjectRpcAsyncHost,
    NetworkError, ObjectRpcRequest, ObjectRpcSessionCall, cluster_node_blocks_leader_preference,
};
use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

/// Maximum number of wake requests retained while one opportunistic maintenance
/// worker is active. Requests beyond this limit are counted, but do not allocate
/// queue entries. Distinct wake sources are retained separately so reports still
/// explain why the coalesced pass ran.
pub const DEFAULT_CLUSTER_KV_MAINTENANCE_MAX_COALESCED_WAKES: usize = 64;
pub const DEFAULT_CLUSTER_KV_MAINTENANCE_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(250);
pub const DEFAULT_CLUSTER_KV_MAINTENANCE_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// An external event that makes bounded cluster-KV maintenance useful.
///
/// These are scheduling hints only. They never prove a Raft timeout, leadership,
/// quorum, or freshness condition.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ClusterKvMaintenanceWakeSource {
    DaemonStartup,
    NetworkChange,
    AppConnection,
    CliConnection,
    ClusterStatus,
    ClusterHealth,
    RolloutActivity,
    MigrationOperation,
    PeerRpc,
    LeaderObservation,
    MembershipChange,
    RaftProgress,
    PeriodicOpportunity,
    ExplicitOperatorRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvMaintenanceWakeDisposition {
    /// No worker was active; the caller owns starting one.
    StartWorker,
    /// An active worker will observe this wake in its next coalesced pass.
    Coalesced,
    /// The bounded pending counter was full. The source was retained and the
    /// dropped count will be visible in the next batch.
    Saturated,
    /// Shutdown has started, so no new maintenance work was accepted.
    RejectedShutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceWakeBatch {
    pub sources: Vec<ClusterKvMaintenanceWakeSource>,
    pub coalesced_wake_count: usize,
    pub saturated_wake_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvMaintenanceWorkerDisposition {
    /// More work arrived during the pass; the current worker should take a new
    /// batch after yielding to the runtime.
    ContinueAfterYield,
    /// No work is pending and ownership of the worker slot was released.
    Idle,
    /// Shutdown is active and ownership of the worker slot was released.
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvMaintenancePassOutcome {
    Completed,
    RetryableFailure,
}

#[derive(Debug)]
struct ClusterKvMaintenanceWakeState {
    worker_active: bool,
    shutting_down: bool,
    pending_sources: BTreeSet<ClusterKvMaintenanceWakeSource>,
    pending_wake_count: usize,
    saturated_wake_count: usize,
    consecutive_retryable_failures: u32,
    retry_not_before: Option<tokio::time::Instant>,
}

/// Path-local wake coalescer for passive cluster maintenance.
///
/// The coordinator deliberately does not own a timer or a background task. A
/// host requests a wake, starts a worker only for `StartWorker`, and lets that
/// worker drain bounded batches while the daemon is already active.
#[derive(Debug)]
pub struct ClusterKvMaintenanceCoordinator {
    max_coalesced_wakes: usize,
    retry_backoff_base: Duration,
    retry_backoff_max: Duration,
    state: Mutex<ClusterKvMaintenanceWakeState>,
}

impl Default for ClusterKvMaintenanceCoordinator {
    fn default() -> Self {
        Self::new(DEFAULT_CLUSTER_KV_MAINTENANCE_MAX_COALESCED_WAKES)
    }
}

impl ClusterKvMaintenanceCoordinator {
    pub fn new(max_coalesced_wakes: usize) -> Self {
        Self::with_retry_backoff(
            max_coalesced_wakes,
            DEFAULT_CLUSTER_KV_MAINTENANCE_RETRY_BACKOFF_BASE,
            DEFAULT_CLUSTER_KV_MAINTENANCE_RETRY_BACKOFF_MAX,
        )
    }

    pub fn with_retry_backoff(
        max_coalesced_wakes: usize,
        retry_backoff_base: Duration,
        retry_backoff_max: Duration,
    ) -> Self {
        let retry_backoff_base = retry_backoff_base.max(Duration::from_millis(1));
        Self {
            max_coalesced_wakes: max_coalesced_wakes.max(1),
            retry_backoff_base,
            retry_backoff_max: retry_backoff_max.max(retry_backoff_base),
            state: Mutex::new(ClusterKvMaintenanceWakeState {
                worker_active: false,
                shutting_down: false,
                pending_sources: BTreeSet::new(),
                pending_wake_count: 0,
                saturated_wake_count: 0,
                consecutive_retryable_failures: 0,
                retry_not_before: None,
            }),
        }
    }

    /// Opens a fresh daemon-owned maintenance generation.
    ///
    /// The daemon keeps this coordinator process-global, so embedded runtimes
    /// and tests can stop and later start a daemon in the same process. The
    /// caller must hold exclusive daemon-lifecycle ownership; stale scheduling
    /// hints from the prior generation are deliberately discarded.
    pub fn start_generation(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.worker_active = false;
        state.shutting_down = false;
        state.pending_sources.clear();
        state.pending_wake_count = 0;
        state.saturated_wake_count = 0;
        state.consecutive_retryable_failures = 0;
        state.retry_not_before = None;
    }

    pub fn request_wake(
        &self,
        source: ClusterKvMaintenanceWakeSource,
    ) -> ClusterKvMaintenanceWakeDisposition {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.shutting_down {
            return ClusterKvMaintenanceWakeDisposition::RejectedShutdown;
        }
        state.pending_sources.insert(source);
        let saturated = state.pending_wake_count >= self.max_coalesced_wakes;
        if saturated {
            state.saturated_wake_count = state.saturated_wake_count.saturating_add(1);
        } else {
            state.pending_wake_count += 1;
        }
        if !state.worker_active {
            state.worker_active = true;
            ClusterKvMaintenanceWakeDisposition::StartWorker
        } else if saturated {
            ClusterKvMaintenanceWakeDisposition::Saturated
        } else {
            ClusterKvMaintenanceWakeDisposition::Coalesced
        }
    }

    /// Takes the currently coalesced work. Only the active worker should call
    /// this method. An empty result means no pass is currently needed.
    pub fn take_pending_batch_if_due(&self) -> Option<ClusterKvMaintenanceWakeBatch> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.worker_active || state.pending_wake_count == 0 {
            return None;
        }
        if let Some(retry_not_before) = state.retry_not_before {
            if retry_not_before > tokio::time::Instant::now() {
                return None;
            }
            state.retry_not_before = None;
        }
        let sources = std::mem::take(&mut state.pending_sources)
            .into_iter()
            .collect();
        let coalesced_wake_count = std::mem::take(&mut state.pending_wake_count);
        let saturated_wake_count = std::mem::take(&mut state.saturated_wake_count);
        Some(ClusterKvMaintenanceWakeBatch {
            sources,
            coalesced_wake_count,
            saturated_wake_count,
        })
    }

    /// Completes one pass without losing a wake racing with completion.
    pub fn finish_pass(
        &self,
        outcome: ClusterKvMaintenancePassOutcome,
    ) -> ClusterKvMaintenanceWorkerDisposition {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.shutting_down {
            state.worker_active = false;
            state.pending_sources.clear();
            state.pending_wake_count = 0;
            state.saturated_wake_count = 0;
            state.consecutive_retryable_failures = 0;
            state.retry_not_before = None;
            return ClusterKvMaintenanceWorkerDisposition::Shutdown;
        }
        match outcome {
            ClusterKvMaintenancePassOutcome::Completed => {
                state.consecutive_retryable_failures = 0;
                state.retry_not_before = None;
            }
            ClusterKvMaintenancePassOutcome::RetryableFailure => {
                state.consecutive_retryable_failures =
                    state.consecutive_retryable_failures.saturating_add(1);
                let exponent = state
                    .consecutive_retryable_failures
                    .saturating_sub(1)
                    .min(31);
                let multiplier = 1_u32 << exponent;
                let delay = self
                    .retry_backoff_base
                    .saturating_mul(multiplier)
                    .min(self.retry_backoff_max);
                state.retry_not_before = Some(tokio::time::Instant::now() + delay);
            }
        }
        if state.pending_wake_count > 0 {
            ClusterKvMaintenanceWorkerDisposition::ContinueAfterYield
        } else {
            state.worker_active = false;
            ClusterKvMaintenanceWorkerDisposition::Idle
        }
    }

    /// Remaining retry cooldown for a worker whose queued wake is not yet due.
    /// Hosts may use this duration to arrange a delayed notification without
    /// introducing an always-on maintenance timer.
    pub fn retry_delay(&self) -> Option<Duration> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.retry_not_before.map(|retry_not_before| {
            retry_not_before
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or_default()
        })
    }

    /// Stops accepting new wakes. An active pass may finish, but queued work is
    /// discarded because wake evidence is never authoritative.
    pub fn shutdown(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let worker_was_active = state.worker_active;
        state.shutting_down = true;
        state.worker_active = false;
        state.pending_sources.clear();
        state.pending_wake_count = 0;
        state.saturated_wake_count = 0;
        state.consecutive_retryable_failures = 0;
        state.retry_not_before = None;
        worker_was_active
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ClusterKvMaintenanceStep {
    LeaderAndTermRefresh,
    HeartbeatAndCatchUp,
    SnapshotRefresh,
    WatchDrainAndFanout,
    PeerProgressHintPersist,
    ConfigChangeReconcile,
    RaftCompaction,
    NodeStatusPublish,
    EndpointObservationPublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvMaintenanceStepDisposition {
    Completed,
    Skipped,
    Failed,
    TimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceActionResult {
    pub disposition: ClusterKvMaintenanceStepDisposition,
    pub code: String,
    pub detail: String,
}

impl ClusterKvMaintenanceActionResult {
    pub fn completed(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            disposition: ClusterKvMaintenanceStepDisposition::Completed,
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub fn skipped(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            disposition: ClusterKvMaintenanceStepDisposition::Skipped,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceStepEvidence {
    pub step: ClusterKvMaintenanceStep,
    pub disposition: ClusterKvMaintenanceStepDisposition,
    pub code: String,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceOptions {
    /// Total wall-clock budget for a single coalesced pass.
    pub total_budget: Duration,
    /// Per-step ceiling. Remaining time is divided fairly among unfinished
    /// steps so an early stalled transport cannot starve compaction/status work.
    pub max_step_budget: Duration,
}

impl Default for ClusterKvMaintenanceOptions {
    fn default() -> Self {
        Self {
            total_budget: Duration::from_secs(30),
            max_step_budget: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceReport {
    pub wake: ClusterKvMaintenanceWakeBatch,
    pub evidence: Vec<ClusterKvMaintenanceStepEvidence>,
}

impl ClusterKvMaintenanceReport {
    pub fn pass_outcome(&self) -> ClusterKvMaintenancePassOutcome {
        let retryable = self.evidence.iter().any(|item| {
            matches!(
                item.disposition,
                ClusterKvMaintenanceStepDisposition::Failed
                    | ClusterKvMaintenanceStepDisposition::TimedOut
            ) || matches!(
                item.code.as_str(),
                "leader_election_inconclusive"
                    | "no_reachable_snapshot_source"
                    | "maintenance_budget_exhausted"
            )
        });
        if retryable {
            ClusterKvMaintenancePassOutcome::RetryableFailure
        } else {
            ClusterKvMaintenancePassOutcome::Completed
        }
    }
}

pub trait ClusterKvMaintenanceHost: Send {
    fn refresh_leader_and_term_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "leader_term_hook_unavailable",
            "host does not provide leader/term refresh",
        )
    }

    fn heartbeat_and_catch_up_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "replication_hook_unavailable",
            "host does not provide heartbeat/catch-up",
        )
    }

    fn refresh_snapshot_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "snapshot_hook_unavailable",
            "host does not provide snapshot refresh",
        )
    }

    fn drain_and_fan_out_watches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "watch_hook_unavailable",
            "host does not provide watch maintenance",
        )
    }

    fn persist_peer_progress_hints_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "peer_progress_hook_unavailable",
            "host does not persist soft peer progress hints",
        )
    }

    fn reconcile_config_changes_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "config_change_recovery_hook_unavailable",
            "host does not provide Raft config lifecycle recovery",
        )
    }

    fn compact_raft_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "compaction_hook_unavailable",
            "host does not provide Raft compaction",
        )
    }

    fn publish_node_status_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "node_status_hook_unavailable",
            "node status publication requires host process evidence",
        )
    }

    fn publish_endpoint_observations_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        maintenance_skip(
            "endpoint_observation_hook_unavailable",
            "endpoint publication requires host network evidence",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvMaintenanceRaftObservation {
    pub known_leader_id: Option<NodeId>,
    pub current_term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub local_revision: KvRevision,
    pub local_is_voter: bool,
    pub local_is_leader: bool,
}

/// Concrete maintenance adapter over the existing transport-coupled control
/// plane. It reuses Raft election, append replication, snapshot RPC, watch
/// fanout, and compaction mechanisms; it does not implement consensus itself.
pub struct ClusterControlPlaneMaintenanceDriver<'a, T> {
    runtime: tokio::runtime::Handle,
    driver: &'a mut ClusterControlPlaneDriver<T>,
    last_observation: Option<ClusterKvMaintenanceRaftObservation>,
    last_fanout: Option<ClusterRaftCommittedFanoutReport>,
}

impl<'a, T> ClusterControlPlaneMaintenanceDriver<'a, T> {
    pub fn new(
        runtime: &tokio::runtime::Handle,
        driver: &'a mut ClusterControlPlaneDriver<T>,
    ) -> Self {
        Self {
            runtime: runtime.clone(),
            driver,
            last_observation: None,
            last_fanout: None,
        }
    }

    pub fn control_plane_driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        self.driver
    }

    pub fn last_observation(&self) -> Option<&ClusterKvMaintenanceRaftObservation> {
        self.last_observation.as_ref()
    }
}

impl<T> ClusterKvMaintenanceHost for ClusterControlPlaneMaintenanceDriver<'_, T>
where
    T: ClusterKvTxnQuorumAsyncHost + ClusterKvCommittedFanoutAsyncHost + Send,
{
    fn refresh_leader_and_term_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        Box::pin(async move {
            let observation =
                read_cluster_kv_maintenance_raft_observation_async(&self.runtime, paths, state)
                    .await?;
            if let Some(leader_id) = observation.known_leader_id.as_ref() {
                let leader_id = leader_id.clone();
                let proof = read_cluster_kv_authoritative_snapshot_async_with_host(
                    &self.runtime,
                    paths,
                    state,
                    self.driver.timeouts.kv_txn_quorum_options(true),
                    &mut self.driver.transport,
                )
                .await;
                if let Ok(read) = proof {
                    let observation = read_cluster_kv_maintenance_raft_observation_async(
                        &self.runtime,
                        paths,
                        state,
                    )
                    .await?;
                    let detail = format!(
                        "proved leader {leader_id} live with a current-term quorum at term {} and revision {}",
                        read.authority.term, read.authority.revision.0
                    );
                    let code = if observation.local_is_leader {
                        "local_leader_liveness_proved"
                    } else {
                        "remote_leader_liveness_proved"
                    };
                    self.last_observation = Some(observation);
                    return Ok(ClusterKvMaintenanceActionResult::completed(code, detail));
                }

                // A remembered leader is only a hint. Once its authoritative
                // liveness proof fails, clear it before campaigning; otherwise
                // election preference treats the stale ID as durable forever.
                let clear_paths = paths.clone();
                let local_node_id = state.local_node_id.clone();
                self.runtime
                    .spawn_blocking(move || {
                        clear_paths
                            .kv_paths()
                            .clear_raft_leader_and_write_snapshot_if_changed(&local_node_id)
                            .map_err(kv_err)?;
                        Ok::<_, NetworkError>(())
                    })
                    .await
                    .map_err(|source| NetworkError::Io {
                        operation: "clear stale cluster KV leader".to_string(),
                        detail: format!("stale leader clear task failed: {source}"),
                    })??;
            }
            if !observation.local_is_voter {
                self.last_observation = Some(observation);
                return Ok(ClusterKvMaintenanceActionResult::skipped(
                    "local_node_not_voter",
                    "the local node cannot campaign for Raft leadership",
                ));
            }

            let elected = elect_cluster_raft_leader_async_with_host(
                &self.runtime,
                paths,
                state,
                self.driver.timeouts.leader_election_options(),
                &mut self.driver.transport,
            )
            .await?;
            let observation =
                read_cluster_kv_maintenance_raft_observation_async(&self.runtime, paths, state)
                    .await?;
            let result = if elected && observation.local_is_leader {
                ClusterKvMaintenanceActionResult::completed(
                    "local_leader_elected",
                    format!(
                        "elected local leader at term {} and commit index {}",
                        observation.current_term, observation.commit_index
                    ),
                )
            } else {
                ClusterKvMaintenanceActionResult::skipped(
                    "leader_election_inconclusive",
                    format!(
                        "no leader was established at observed term {}; quorum may be offline",
                        observation.current_term
                    ),
                )
            };
            self.last_observation = Some(observation);
            Ok(result)
        })
    }

    fn heartbeat_and_catch_up_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        Box::pin(async move {
            let observation = match self.last_observation.clone() {
                Some(observation) => observation,
                None => {
                    read_cluster_kv_maintenance_raft_observation_async(&self.runtime, paths, state)
                        .await?
                }
            };
            if !observation.local_is_leader {
                return Ok(ClusterKvMaintenanceActionResult::skipped(
                    "remote_or_unknown_leader",
                    "only the current local leader sends append heartbeats and catch-up data",
                ));
            }
            let paths_for_snapshot = paths.clone();
            let snapshot = self
                .runtime
                .spawn_blocking(move || read_cluster_kv_snapshot(&paths_for_snapshot))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster KV reactive replication".to_string(),
                    detail: format!("local snapshot read task failed: {source}"),
                })??;
            let options = crate::control_plane_options::ClusterRaftCommittedFanoutOptions {
                raft_append_timeout: budget,
                watch_event_timeout: budget.min(self.driver.timeouts.rpc_timeout),
            };
            let report = fan_out_committed_cluster_raft_command_async_with_host(
                &self.runtime,
                paths,
                state,
                &snapshot.events,
                options,
                self.driver,
            )
            .await?;
            let detail = format!(
                "attempted {} peers, caught up {}, sent {} watch batches",
                report.attempted_nodes,
                report.replicated_nodes.len(),
                report.watch_event_batches_sent
            );
            self.last_fanout = Some(report);
            Ok(ClusterKvMaintenanceActionResult::completed(
                "leader_append_maintenance_completed",
                detail,
            ))
        })
    }

    fn refresh_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        Box::pin(async move {
            let observation = match self.last_observation.clone() {
                Some(observation) => observation,
                None => {
                    read_cluster_kv_maintenance_raft_observation_async(&self.runtime, paths, state)
                        .await?
                }
            };
            if observation.local_is_leader {
                return Ok(ClusterKvMaintenanceActionResult::skipped(
                    "local_leader_has_canonical_log",
                    "the local leader does not pull a peer materialized snapshot",
                ));
            }
            let outcome = refresh_cluster_kv_snapshot_async(
                &self.runtime,
                paths,
                state,
                budget,
                &mut self.driver.transport,
            )
            .await?;
            let Some((source, snapshot, cache_changed)) = outcome else {
                return Ok(ClusterKvMaintenanceActionResult::skipped(
                    "no_reachable_snapshot_source",
                    "no reachable voter returned a cluster KV snapshot within the fair-share budget",
                ));
            };
            Ok(ClusterKvMaintenanceActionResult::completed(
                "snapshot_refreshed",
                format!(
                    "observed revision {} from {source}; local cache changed: {cache_changed}",
                    snapshot.revision.0
                ),
            ))
        })
    }

    fn drain_and_fan_out_watches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        Box::pin(async move {
            let Some(report) = self.last_fanout.as_ref() else {
                return Ok(ClusterKvMaintenanceActionResult::skipped(
                    "watch_fanout_not_owned_locally",
                    "the local node did not run leader append fanout in this pass",
                ));
            };
            Ok(ClusterKvMaintenanceActionResult::completed(
                "watch_fanout_drained",
                format!(
                    "sent {} bounded watch-event batches after accepted replication",
                    report.watch_event_batches_sent
                ),
            ))
        })
    }

    fn persist_peer_progress_hints_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        let Some(report) = self.last_fanout.as_ref() else {
            return maintenance_skip(
                "peer_progress_not_observed",
                "the local node did not run leader replication in this pass",
            );
        };
        if report.peer_progress_hints.is_empty() {
            return maintenance_skip(
                "peer_progress_unchanged",
                "no peer accepted append replication in this pass",
            );
        }
        let runtime = self.runtime.clone();
        let kv_paths = paths.kv_paths();
        let hints = report.peer_progress_hints.clone();
        Box::pin(async move {
            let count = hints.len();
            runtime
                .spawn_blocking(move || {
                    for hint in hints {
                        sinomo_kv::record_cluster_kv_raft_peer_progress(&kv_paths, hint)
                            .map_err(kv_err)?;
                    }
                    Ok::<_, NetworkError>(())
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster KV peer progress persistence".to_string(),
                    detail: format!("peer progress persistence task failed: {source}"),
                })??;
            Ok(ClusterKvMaintenanceActionResult::completed(
                "peer_progress_persisted",
                format!("persisted {count} non-authoritative peer progress hints"),
            ))
        })
    }

    fn compact_raft_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        _budget: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
        let runtime = self.runtime.clone();
        let paths = paths.clone();
        let local_node_id = state.local_node_id.clone();
        Box::pin(async move {
            let (snapshot_index, applied_index) = runtime
                .spawn_blocking(move || {
                    let node = paths
                        .kv_paths()
                        .open_raft_node(&local_node_id)
                        .map_err(kv_err)?;
                    Ok::<_, NetworkError>((node.snapshot_index(), node.applied_index()))
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster KV Raft compaction".to_string(),
                    detail: format!("compaction task failed: {source}"),
                })??;
            Ok(ClusterKvMaintenanceActionResult::completed(
                "raft_compaction_checked",
                format!(
                    "checked compaction thresholds at snapshot index {snapshot_index} and applied index {applied_index}"
                ),
            ))
        })
    }
}

async fn read_cluster_kv_maintenance_raft_observation_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<ClusterKvMaintenanceRaftObservation, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || {
            let node = paths
                .kv_paths()
                .open_raft_node(&state.local_node_id)
                .map_err(kv_err)?;
            let hard_state = node.hard_state();
            let known_leader_id = hard_state
                .leader_id
                .clone()
                .filter(|leader_id| state.node_is_raft_voter(leader_id));
            let local_is_voter = sinomo_kv::cluster_kv_raft_local_is_voter(&state);
            let local_is_leader =
                local_is_voter && known_leader_id.as_ref() == Some(&state.local_node_id);
            Ok(ClusterKvMaintenanceRaftObservation {
                known_leader_id,
                current_term: hard_state.current_term,
                commit_index: hard_state.commit_index,
                applied_index: node.applied_index(),
                last_log_index: node.last_log_index(),
                snapshot_index: node.snapshot_index(),
                local_revision: node.store().revision(),
                local_is_voter,
                local_is_leader,
            })
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster KV maintenance observation".to_string(),
            detail: format!("local Raft observation task failed: {source}"),
        })?
}

async fn refresh_cluster_kv_snapshot_async<T>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    budget: Duration,
    host: &mut T,
) -> Result<Option<(NodeId, KvSnapshot, bool)>, NetworkError>
where
    T: ClusterSignedObjectRpcAsyncHost,
{
    let mut targets = ClusterEndpointSelector::select_internal_unique_node_ids_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    )
    .await?;
    targets.retain(|node_id| node_id != &state.local_node_id && state.node_is_raft_voter(node_id));
    if let Ok(Some(leader_id)) = locked_kv_state_authority_id_async(runtime, paths, state).await
        && !cluster_node_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets.iter().position(|node_id| node_id == &leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }
    let deadline = tokio::time::Instant::now() + budget;
    for (index, target_node_id) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let remaining_targets =
            u32::try_from(targets.len().saturating_sub(index).max(1)).unwrap_or(u32::MAX);
        let timeout = remaining
            .checked_div(remaining_targets)
            .unwrap_or(remaining)
            .max(Duration::from_millis(1));
        let payload = sinomo_kv::cluster_kv_local_snapshot_rpc_payload().map_err(kv_err)?;
        let request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
        let response = {
            let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
            client
                .call_target(
                    state,
                    target_node_id,
                    ObjectRpcSessionCall::value(request),
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                )
                .await
        };
        let Ok(response) = response else {
            continue;
        };
        if response.response.error_detail().is_err() {
            continue;
        }
        let Ok(local_read) =
            sinomo_kv::decode_cluster_kv_local_snapshot_rpc_body(&response.response.body)
        else {
            continue;
        };
        let snapshot = local_read.value;
        let paths_for_write = paths.clone();
        let local_node_id = state.local_node_id.clone();
        let snapshot_for_write = snapshot.clone();
        let cache_changed = runtime
            .spawn_blocking(move || {
                write_imported_cluster_kv_snapshot_if_changed(
                    &paths_for_write,
                    &snapshot_for_write,
                )?;
                locked_cache_cluster_kv_snapshot_if_newer_than_raft(
                    &paths_for_write,
                    &local_node_id,
                    snapshot_for_write,
                )
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster KV snapshot refresh".to_string(),
                detail: format!("snapshot cache task failed: {source}"),
            })??;
        return Ok(Some((target_node_id.clone(), snapshot, cache_changed)));
    }
    Ok(None)
}

fn maintenance_skip<'a>(
    code: &'static str,
    detail: &'static str,
) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
    Box::pin(async move { Ok(ClusterKvMaintenanceActionResult::skipped(code, detail)) })
}

const CLUSTER_KV_MAINTENANCE_STEPS: [ClusterKvMaintenanceStep; 9] = [
    ClusterKvMaintenanceStep::LeaderAndTermRefresh,
    ClusterKvMaintenanceStep::HeartbeatAndCatchUp,
    ClusterKvMaintenanceStep::SnapshotRefresh,
    ClusterKvMaintenanceStep::WatchDrainAndFanout,
    ClusterKvMaintenanceStep::PeerProgressHintPersist,
    ClusterKvMaintenanceStep::ConfigChangeReconcile,
    ClusterKvMaintenanceStep::RaftCompaction,
    ClusterKvMaintenanceStep::NodeStatusPublish,
    ClusterKvMaintenanceStep::EndpointObservationPublish,
];

/// Runs one bounded maintenance pass. Every planned step emits evidence,
/// including budget exhaustion, timeout, host failure, or an explicit skip.
pub async fn run_cluster_kv_maintenance_tick_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    wake: ClusterKvMaintenanceWakeBatch,
    options: ClusterKvMaintenanceOptions,
    host: &mut impl ClusterKvMaintenanceHost,
) -> ClusterKvMaintenanceReport {
    let started = tokio::time::Instant::now();
    let deadline = started + options.total_budget;
    let mut evidence = Vec::with_capacity(CLUSTER_KV_MAINTENANCE_STEPS.len());
    let projection_paths = paths.clone();
    let mut consensus_state = state.clone();
    let projection = tokio::task::spawn_blocking(move || {
        project_committed_cluster_raft_config(&projection_paths, &mut consensus_state)?;
        Ok::<_, NetworkError>(consensus_state)
    })
    .await;
    match projection {
        Ok(Ok(projected)) => *state = projected,
        Ok(Err(error)) => {
            evidence.push(maintenance_error_evidence(
                ClusterKvMaintenanceStep::LeaderAndTermRefresh,
                error,
            ));
            return ClusterKvMaintenanceReport { wake, evidence };
        }
        Err(source) => {
            evidence.push(ClusterKvMaintenanceStepEvidence {
                step: ClusterKvMaintenanceStep::LeaderAndTermRefresh,
                disposition: ClusterKvMaintenanceStepDisposition::Failed,
                code: "maintenance_config_projection_failed".to_string(),
                detail: format!("Raft config projection task failed: {source}"),
            });
            return ClusterKvMaintenanceReport { wake, evidence };
        }
    }
    for (index, step) in CLUSTER_KV_MAINTENANCE_STEPS.iter().copied().enumerate() {
        let now = tokio::time::Instant::now();
        let remaining = deadline.checked_duration_since(now).unwrap_or_default();
        if remaining.is_zero() {
            evidence.push(ClusterKvMaintenanceStepEvidence {
                step,
                disposition: ClusterKvMaintenanceStepDisposition::Skipped,
                code: "maintenance_budget_exhausted".to_string(),
                detail: "the coalesced maintenance pass exhausted its total budget".to_string(),
            });
            continue;
        }
        let unfinished = CLUSTER_KV_MAINTENANCE_STEPS
            .len()
            .saturating_sub(index)
            .max(1);
        let unfinished = u32::try_from(unfinished).unwrap_or(u32::MAX);
        let fair_share = remaining
            .checked_div(unfinished)
            .unwrap_or(remaining)
            .max(Duration::from_millis(1));
        let step_budget = fair_share.min(options.max_step_budget.max(Duration::from_millis(1)));
        let action = match step {
            ClusterKvMaintenanceStep::LeaderAndTermRefresh => {
                host.refresh_leader_and_term_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::HeartbeatAndCatchUp => {
                host.heartbeat_and_catch_up_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::SnapshotRefresh => {
                host.refresh_snapshot_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::WatchDrainAndFanout => {
                host.drain_and_fan_out_watches_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::PeerProgressHintPersist => {
                host.persist_peer_progress_hints_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::ConfigChangeReconcile => {
                host.reconcile_config_changes_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::RaftCompaction => {
                host.compact_raft_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::NodeStatusPublish => {
                host.publish_node_status_async(paths, state, step_budget)
            }
            ClusterKvMaintenanceStep::EndpointObservationPublish => {
                host.publish_endpoint_observations_async(paths, state, step_budget)
            }
        };
        let step_evidence = match tokio::time::timeout(step_budget, action).await {
            Ok(Ok(result)) => ClusterKvMaintenanceStepEvidence {
                step,
                disposition: result.disposition,
                code: result.code,
                detail: result.detail,
            },
            Ok(Err(error)) => maintenance_error_evidence(step, error),
            Err(_) => ClusterKvMaintenanceStepEvidence {
                step,
                disposition: ClusterKvMaintenanceStepDisposition::TimedOut,
                code: "maintenance_step_timed_out".to_string(),
                detail: format!("maintenance step exceeded its {step_budget:?} fair-share budget"),
            },
        };
        evidence.push(step_evidence);
    }
    ClusterKvMaintenanceReport { wake, evidence }
}

fn maintenance_error_evidence(
    step: ClusterKvMaintenanceStep,
    error: NetworkError,
) -> ClusterKvMaintenanceStepEvidence {
    ClusterKvMaintenanceStepEvidence {
        step,
        disposition: ClusterKvMaintenanceStepDisposition::Failed,
        code: "maintenance_step_failed".to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::ClusterBootstrapState;
    use std::collections::BTreeMap;

    #[test]
    fn wake_coalescer_is_bounded_and_preserves_distinct_sources() {
        let coordinator = ClusterKvMaintenanceCoordinator::new(2);
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::DaemonStartup),
            ClusterKvMaintenanceWakeDisposition::StartWorker
        );
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::PeerRpc),
            ClusterKvMaintenanceWakeDisposition::Coalesced
        );
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::ClusterHealth),
            ClusterKvMaintenanceWakeDisposition::Saturated
        );

        let batch = coordinator
            .take_pending_batch_if_due()
            .expect("pending batch");
        assert_eq!(batch.coalesced_wake_count, 2);
        assert_eq!(batch.saturated_wake_count, 1);
        assert_eq!(
            batch.sources,
            vec![
                ClusterKvMaintenanceWakeSource::DaemonStartup,
                ClusterKvMaintenanceWakeSource::ClusterHealth,
                ClusterKvMaintenanceWakeSource::PeerRpc,
            ]
        );
        assert_eq!(
            coordinator.finish_pass(ClusterKvMaintenancePassOutcome::Completed),
            ClusterKvMaintenanceWorkerDisposition::Idle
        );
    }

    #[test]
    fn wake_racing_with_pass_completion_is_not_lost() {
        let coordinator = ClusterKvMaintenanceCoordinator::default();
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::CliConnection),
            ClusterKvMaintenanceWakeDisposition::StartWorker
        );
        coordinator
            .take_pending_batch_if_due()
            .expect("first batch");
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::PeerRpc),
            ClusterKvMaintenanceWakeDisposition::Coalesced
        );
        assert_eq!(
            coordinator.finish_pass(ClusterKvMaintenancePassOutcome::Completed),
            ClusterKvMaintenanceWorkerDisposition::ContinueAfterYield
        );
        let next = coordinator
            .take_pending_batch_if_due()
            .expect("racing wake batch");
        assert_eq!(next.sources, vec![ClusterKvMaintenanceWakeSource::PeerRpc]);
    }

    #[test]
    fn shutdown_rejects_and_discards_non_authoritative_wakes() {
        let coordinator = ClusterKvMaintenanceCoordinator::default();
        coordinator.request_wake(ClusterKvMaintenanceWakeSource::PeriodicOpportunity);
        assert!(coordinator.shutdown());
        assert!(coordinator.take_pending_batch_if_due().is_none());
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::ExplicitOperatorRequest),
            ClusterKvMaintenanceWakeDisposition::RejectedShutdown
        );
        assert_eq!(
            coordinator.finish_pass(ClusterKvMaintenancePassOutcome::Completed),
            ClusterKvMaintenanceWorkerDisposition::Shutdown
        );
    }

    #[test]
    fn completed_shutdown_generation_can_be_reopened_without_stale_wakes() {
        let coordinator = ClusterKvMaintenanceCoordinator::new(2);
        coordinator.request_wake(ClusterKvMaintenanceWakeSource::NetworkChange);
        assert!(coordinator.shutdown());
        assert_eq!(
            coordinator.finish_pass(ClusterKvMaintenancePassOutcome::Completed),
            ClusterKvMaintenanceWorkerDisposition::Shutdown
        );

        coordinator.start_generation();
        assert!(coordinator.take_pending_batch_if_due().is_none());
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::DaemonStartup),
            ClusterKvMaintenanceWakeDisposition::StartWorker
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_failure_applies_bounded_exponential_cooldown() {
        let coordinator = ClusterKvMaintenanceCoordinator::with_retry_backoff(
            4,
            Duration::from_millis(100),
            Duration::from_millis(250),
        );
        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::DaemonStartup),
            ClusterKvMaintenanceWakeDisposition::StartWorker
        );
        coordinator
            .take_pending_batch_if_due()
            .expect("initial wake is due");
        assert_eq!(
            coordinator.finish_pass(ClusterKvMaintenancePassOutcome::RetryableFailure),
            ClusterKvMaintenanceWorkerDisposition::Idle
        );

        assert_eq!(
            coordinator.request_wake(ClusterKvMaintenanceWakeSource::PeerRpc),
            ClusterKvMaintenanceWakeDisposition::StartWorker
        );
        assert!(coordinator.take_pending_batch_if_due().is_none());
        assert_eq!(coordinator.retry_delay(), Some(Duration::from_millis(100)));
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(coordinator.take_pending_batch_if_due().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        coordinator
            .take_pending_batch_if_due()
            .expect("first cooldown elapsed");
        coordinator.finish_pass(ClusterKvMaintenancePassOutcome::RetryableFailure);

        coordinator.request_wake(ClusterKvMaintenanceWakeSource::ClusterHealth);
        assert_eq!(coordinator.retry_delay(), Some(Duration::from_millis(200)));
        tokio::time::advance(Duration::from_millis(200)).await;
        coordinator
            .take_pending_batch_if_due()
            .expect("second cooldown elapsed");
        coordinator.finish_pass(ClusterKvMaintenancePassOutcome::RetryableFailure);

        coordinator.request_wake(ClusterKvMaintenanceWakeSource::ClusterStatus);
        assert_eq!(coordinator.retry_delay(), Some(Duration::from_millis(250)));
        tokio::time::advance(Duration::from_millis(250)).await;
        coordinator
            .take_pending_batch_if_due()
            .expect("capped cooldown elapsed");
        coordinator.finish_pass(ClusterKvMaintenancePassOutcome::Completed);

        coordinator.request_wake(ClusterKvMaintenanceWakeSource::CliConnection);
        assert_eq!(coordinator.retry_delay(), None);
        assert!(coordinator.take_pending_batch_if_due().is_some());
    }

    struct RecordingMaintenanceHost {
        calls: Vec<ClusterKvMaintenanceStep>,
        delays: BTreeMap<ClusterKvMaintenanceStep, Duration>,
    }

    impl RecordingMaintenanceHost {
        fn action<'a>(
            &'a mut self,
            step: ClusterKvMaintenanceStep,
        ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
            self.calls.push(step);
            let delay = self.delays.get(&step).copied().unwrap_or_default();
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                Ok(ClusterKvMaintenanceActionResult::completed(
                    "test",
                    "completed",
                ))
            })
        }
    }

    macro_rules! recording_action {
        ($name:ident, $step:expr) => {
            fn $name<'a>(
                &'a mut self,
                _paths: &'a ClusterPaths,
                _state: &'a mut ClusterBootstrapState,
                _budget: Duration,
            ) -> ClusterMembershipRpcFuture<'a, ClusterKvMaintenanceActionResult> {
                self.action($step)
            }
        };
    }

    impl ClusterKvMaintenanceHost for RecordingMaintenanceHost {
        recording_action!(
            refresh_leader_and_term_async,
            ClusterKvMaintenanceStep::LeaderAndTermRefresh
        );
        recording_action!(
            heartbeat_and_catch_up_async,
            ClusterKvMaintenanceStep::HeartbeatAndCatchUp
        );
        recording_action!(
            refresh_snapshot_async,
            ClusterKvMaintenanceStep::SnapshotRefresh
        );
        recording_action!(
            drain_and_fan_out_watches_async,
            ClusterKvMaintenanceStep::WatchDrainAndFanout
        );
        recording_action!(
            persist_peer_progress_hints_async,
            ClusterKvMaintenanceStep::PeerProgressHintPersist
        );
        recording_action!(
            reconcile_config_changes_async,
            ClusterKvMaintenanceStep::ConfigChangeReconcile
        );
        recording_action!(compact_raft_async, ClusterKvMaintenanceStep::RaftCompaction);
        recording_action!(
            publish_node_status_async,
            ClusterKvMaintenanceStep::NodeStatusPublish
        );
        recording_action!(
            publish_endpoint_observations_async,
            ClusterKvMaintenanceStep::EndpointObservationPublish
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_step_does_not_starve_later_maintenance_actions() {
        let temp = sinomo_testdir::TestDir::new("cluster-reactive-maintenance");
        let data_dir = sinomo_local::SinomoDataDir::new(temp.path().to_path_buf());
        let paths = ClusterPaths::new(&data_dir);
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("reactive_maintenance_cluster").expect("cluster id"),
            "reactive maintenance cluster",
            NodeId::from_legacy_named("reactive_maintenance_node").expect("node id"),
            "reactive maintenance".to_string(),
            "reactive-maintenance-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let mut host = RecordingMaintenanceHost {
            calls: Vec::new(),
            delays: BTreeMap::from([(
                ClusterKvMaintenanceStep::LeaderAndTermRefresh,
                Duration::from_secs(60),
            )]),
        };
        let wake = ClusterKvMaintenanceWakeBatch {
            sources: vec![ClusterKvMaintenanceWakeSource::DaemonStartup],
            coalesced_wake_count: 1,
            saturated_wake_count: 0,
        };

        let report = run_cluster_kv_maintenance_tick_with_host(
            &paths,
            &mut state,
            wake,
            ClusterKvMaintenanceOptions {
                total_budget: Duration::from_millis(80),
                max_step_budget: Duration::from_millis(10),
            },
            &mut host,
        )
        .await;

        assert_eq!(
            host.calls.as_slice(),
            CLUSTER_KV_MAINTENANCE_STEPS.as_slice()
        );
        assert_eq!(
            report.evidence[0].disposition,
            ClusterKvMaintenanceStepDisposition::TimedOut
        );
        assert!(
            report.evidence[1..]
                .iter()
                .all(|item| { item.disposition == ClusterKvMaintenanceStepDisposition::Completed })
        );
    }
}
