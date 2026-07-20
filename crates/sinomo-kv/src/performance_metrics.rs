use crate::model::{KvCommand, KvOp};
use crate::paths::ClusterKvPaths;
use sinomo_node::NodeId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

const CLUSTER_KV_RAFT_LIVE_METRIC_STATES: usize = 64;
static CLUSTER_KV_RAFT_LIVE_METRIC_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_RAFT_LIVE_METRICS: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvRaftLiveMetricEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLiveMetrics {
    pub current_term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub retained_log_entries: usize,
}

#[derive(Clone)]
struct ClusterKvRaftLiveMetricEntry {
    metrics: ClusterKvRaftLiveMetrics,
    last_used: u64,
}

pub fn read_cluster_kv_raft_live_metrics(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Option<ClusterKvRaftLiveMetrics> {
    let root = paths.raft_wal_paths(node_id).root;
    let mut metrics = CLUSTER_KV_RAFT_LIVE_METRICS.lock().ok()?;
    let entry = metrics.get_mut(&root)?;
    entry.last_used = cluster_kv_raft_live_metric_tick();
    Some(entry.metrics.clone())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_cluster_kv_raft_live_metrics(
    root: &Path,
    current_term: u64,
    leader_id: Option<NodeId>,
    commit_index: u64,
    applied_index: u64,
    last_log_index: u64,
    snapshot_index: u64,
    retained_log_entries: usize,
) {
    let Ok(mut metrics) = CLUSTER_KV_RAFT_LIVE_METRICS.lock() else {
        return;
    };
    if !metrics.contains_key(root) && metrics.len() >= CLUSTER_KV_RAFT_LIVE_METRIC_STATES {
        let eviction = metrics
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(path, _)| path.clone());
        if let Some(eviction) = eviction {
            metrics.remove(&eviction);
        }
    }
    metrics.insert(
        root.to_path_buf(),
        ClusterKvRaftLiveMetricEntry {
            metrics: ClusterKvRaftLiveMetrics {
                current_term,
                leader_id,
                commit_index,
                applied_index,
                last_log_index,
                snapshot_index,
                retained_log_entries,
            },
            last_used: cluster_kv_raft_live_metric_tick(),
        },
    );
}

fn cluster_kv_raft_live_metric_tick() -> u64 {
    CLUSTER_KV_RAFT_LIVE_METRIC_CLOCK.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn duration_us(started: Instant) -> f64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64
}

pub(crate) fn record_kv_command_phase(phase: &'static str, started: Instant) {
    metrics::histogram!("sinomo.kv.command.phase.us", "phase" => phase)
        .record(duration_us(started));
}

pub(crate) struct KvCommandPhaseMetricGuard {
    phase: &'static str,
    started: Instant,
}

impl KvCommandPhaseMetricGuard {
    pub(crate) fn start(phase: &'static str) -> Self {
        Self {
            phase,
            started: Instant::now(),
        }
    }
}

impl Drop for KvCommandPhaseMetricGuard {
    fn drop(&mut self) {
        record_kv_command_phase(self.phase, self.started);
    }
}

pub(crate) fn record_kv_read(
    authority: &'static str,
    source: &'static str,
    outcome: &'static str,
    started: Instant,
) {
    metrics::counter!(
        "sinomo.kv.reads",
        "authority" => authority,
        "source" => source,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.kv.read.total.us",
        "authority" => authority,
        "source" => source,
        "outcome" => outcome
    )
    .record(duration_us(started));
}

pub(crate) struct KvCommandMetricGuard {
    command: &'static str,
    outcome: &'static str,
    started: Instant,
}

impl KvCommandMetricGuard {
    pub(crate) fn start(command: &KvCommand) -> Self {
        Self {
            command: kv_command_kind(command),
            outcome: "internal_error",
            started: Instant::now(),
        }
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for KvCommandMetricGuard {
    fn drop(&mut self) {
        metrics::counter!(
            "sinomo.kv.commands",
            "command" => self.command,
            "outcome" => self.outcome
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.kv.command.total.us",
            "command" => self.command,
            "outcome" => self.outcome
        )
        .record(duration_us(self.started));
    }
}

pub(crate) struct KvCommitConcurrencyMetricGuard {
    queued: bool,
    inflight: bool,
}

impl KvCommitConcurrencyMetricGuard {
    pub(crate) fn queued() -> Self {
        metrics::gauge!("sinomo.kv.commit.queue_depth").increment(1.0);
        Self {
            queued: true,
            inflight: false,
        }
    }

    pub(crate) fn acquired(&mut self) {
        if self.queued {
            metrics::gauge!("sinomo.kv.commit.queue_depth").decrement(1.0);
            self.queued = false;
        }
        if !self.inflight {
            metrics::gauge!("sinomo.kv.commit.inflight").increment(1.0);
            self.inflight = true;
        }
    }
}

impl Drop for KvCommitConcurrencyMetricGuard {
    fn drop(&mut self) {
        if self.queued {
            metrics::gauge!("sinomo.kv.commit.queue_depth").decrement(1.0);
        }
        if self.inflight {
            metrics::gauge!("sinomo.kv.commit.inflight").decrement(1.0);
        }
    }
}

pub(crate) struct KvStorageMetricGuard {
    operation: &'static str,
    histogram: &'static str,
    outcome: &'static str,
    started: Instant,
}

impl KvStorageMetricGuard {
    pub(crate) fn start(operation: &'static str, histogram: &'static str) -> Self {
        Self {
            operation,
            histogram,
            outcome: "error",
            started: Instant::now(),
        }
    }

    pub(crate) fn succeed(&mut self) {
        self.outcome = "ok";
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for KvStorageMetricGuard {
    fn drop(&mut self) {
        metrics::counter!(
            "sinomo.kv.storage.operations",
            "operation" => self.operation,
            "outcome" => self.outcome
        )
        .increment(1);
        metrics::histogram!(self.histogram, "outcome" => self.outcome)
            .record(duration_us(self.started));
    }
}

fn kv_command_kind(command: &KvCommand) -> &'static str {
    match command {
        KvCommand::Op(KvOp::Put { .. }) => "put",
        KvCommand::Op(KvOp::Delete { .. }) => "delete",
        KvCommand::Op(KvOp::DeletePrefix { .. }) => "delete_prefix",
        KvCommand::Op(KvOp::LeaseGrant { .. }) => "lease_grant",
        KvCommand::Op(KvOp::LeaseKeepAlive { .. }) => "lease_keep_alive",
        KvCommand::Op(KvOp::LeaseRevoke { .. }) => "lease_revoke",
        KvCommand::Txn(_) => "txn",
        KvCommand::Barrier => "barrier",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvCommand, KvOp};

    #[test]
    fn command_metric_labels_are_bounded_operation_classes() {
        assert_eq!(kv_command_kind(&KvCommand::Barrier), "barrier");
        assert_eq!(
            kv_command_kind(&KvCommand::Op(KvOp::Delete {
                key: "/arbitrary/user/key".to_string(),
            })),
            "delete"
        );
    }
}
