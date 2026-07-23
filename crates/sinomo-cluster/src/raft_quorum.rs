use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::control_plane_options::ClusterRaftAppendReplicationOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::LocalClusterRaftCommandStorage;
use crate::kv_local_store::kv_err;
use crate::local_state_store::project_committed_cluster_raft_config;
use crate::member_store::merge_cluster_state_from_peer;
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse,
};
use sinomo_kv::{KvCommandRequest, KvEvent, KvRequestId, KvSnapshot};
use sinomo_network::ClusterConnectivityState;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

type SharedLocalClusterRaftCommandStorage = Arc<Mutex<LocalClusterRaftCommandStorage>>;

pub type ClusterRaftQuorumBatch = Vec<(NodeId, ClusterRaftAppendRequest, Duration)>;
pub type ClusterRaftQuorumBatchOutcomes = Vec<(
    NodeId,
    ClusterRaftAppendRequest,
    Result<ClusterRaftAppendResponse, NetworkError>,
)>;

fn cluster_kv_replication_status_label(
    status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
) -> &'static str {
    match status {
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted => "accepted",
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => "rejected",
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => "failed",
    }
}

fn record_cluster_kv_replication_target_result(
    status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
    latency_ms: Option<u64>,
) {
    let outcome = cluster_kv_replication_status_label(status);
    metrics::counter!("sinomo.kv.replication.target_results", "outcome" => outcome).increment(1);
    if let Some(latency_ms) = latency_ms {
        metrics::histogram!("sinomo.kv.replication.target.us", "outcome" => outcome)
            .record(latency_ms.saturating_mul(1_000) as f64);
    }
}

fn record_cluster_kv_replication_send(request: &ClusterRaftAppendRequest) {
    let payload = if request.snapshot.is_some() {
        "snapshot"
    } else {
        "append"
    };
    metrics::counter!("sinomo.kv.replication.attempts", "payload" => payload).increment(1);
    metrics::histogram!("sinomo.kv.replication.entries", "payload" => payload)
        .record(request.append.entries.len() as f64);
}

fn record_cluster_kv_replication_catch_up(outcome: &'static str, attempts: usize) {
    metrics::histogram!("sinomo.kv.replication.catchup_attempts", "outcome" => outcome)
        .record(attempts as f64);
}

fn lock_local_cluster_raft_command_storage<'a>(
    storage: &'a Mutex<LocalClusterRaftCommandStorage>,
    operation: &str,
) -> Result<MutexGuard<'a, LocalClusterRaftCommandStorage>, NetworkError> {
    storage.lock().map_err(|error| NetworkError::Io {
        operation: operation.to_string(),
        detail: format!("local cluster Raft command session lock poisoned: {error}"),
    })
}

#[allow(clippy::too_many_arguments)]
fn cluster_kv_quorum_commit_phase_trace(
    phase: &'static str,
    request_id: &KvRequestId,
    local_node_id: &NodeId,
    duration: Duration,
    commit_elapsed: Duration,
    emission_delay: Duration,
    term: u64,
    log_index: u64,
    revision: u64,
    outcome: &'static str,
    detail: std::fmt::Arguments<'_>,
) {
    if !cluster_kv_quorum_commit_trace_enabled() {
        return;
    }
    let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    let commit_elapsed_us = u64::try_from(commit_elapsed.as_micros()).unwrap_or(u64::MAX);
    let emission_delay_us = u64::try_from(emission_delay.as_micros()).unwrap_or(u64::MAX);
    tracing::trace!(
        target: "sinomo.cluster.formation",
        phase,
        request_id = %request_id,
        local_node_id = %local_node_id,
        duration_us,
        commit_elapsed_us,
        emission_delay_us,
        term,
        log_index,
        revision,
        outcome,
        detail = %detail,
        "cluster KV quorum commit phase"
    );
    if cluster_kv_quorum_commit_stderr_trace_enabled() {
        eprintln!(
            "[sinomo cluster join] KV quorum phase={phase} request_id={request_id} local_node={local_node_id} duration_us={duration_us} commit_elapsed_us={commit_elapsed_us} emission_delay_us={emission_delay_us} term={term} log_index={log_index} revision={revision} outcome={outcome}: {detail}"
        );
    }
}

static CLUSTER_JOIN_STDERR_TRACE_ENABLED: OnceLock<bool> = OnceLock::new();

fn cluster_kv_quorum_commit_stderr_trace_enabled() -> bool {
    *CLUSTER_JOIN_STDERR_TRACE_ENABLED.get_or_init(|| {
        std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
    })
}

fn cluster_kv_quorum_commit_trace_enabled() -> bool {
    tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE)
        || cluster_kv_quorum_commit_stderr_trace_enabled()
}

const CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT: usize = 32;
const CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT: usize = 512;

struct ClusterKvQuorumCommitTraceRecord {
    phase: &'static str,
    duration: Duration,
    observed_at: Instant,
    commit_elapsed: Duration,
    term: u64,
    log_index: u64,
    revision: u64,
    outcome: &'static str,
    detail: String,
}

struct ClusterKvQuorumCommitTraceBuffer {
    request_id: KvRequestId,
    local_node_id: NodeId,
    started: Instant,
    initial_term: u64,
    initial_revision: u64,
    records: Vec<ClusterKvQuorumCommitTraceRecord>,
    dropped_records: u64,
}

#[derive(Clone, Default)]
struct ClusterKvQuorumCommitTrace {
    inner: Option<Arc<Mutex<ClusterKvQuorumCommitTraceBuffer>>>,
}

impl ClusterKvQuorumCommitTrace {
    fn new(request_id: KvRequestId, local_node_id: NodeId, term: u64, revision: u64) -> Self {
        if !cluster_kv_quorum_commit_trace_enabled() {
            return Self::default();
        }
        Self {
            inner: Some(Arc::new(Mutex::new(ClusterKvQuorumCommitTraceBuffer {
                request_id,
                local_node_id,
                started: Instant::now(),
                initial_term: term,
                initial_revision: revision,
                records: Vec::with_capacity(CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT),
                dropped_records: 0,
            }))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        phase: &'static str,
        duration: Duration,
        term: u64,
        log_index: u64,
        revision: u64,
        outcome: &'static str,
        detail: std::fmt::Arguments<'_>,
    ) {
        let Some(inner) = &self.inner else {
            return;
        };
        let Ok(mut buffer) = inner.lock() else {
            return;
        };
        if buffer.records.len() >= CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT {
            buffer.dropped_records = buffer.dropped_records.saturating_add(1);
            return;
        }
        let observed_at = Instant::now();
        let commit_elapsed = observed_at.saturating_duration_since(buffer.started);
        let mut detail = detail.to_string();
        if detail.len() > CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT {
            let mut truncate_at = CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT.saturating_sub(3);
            while !detail.is_char_boundary(truncate_at) {
                truncate_at = truncate_at.saturating_sub(1);
            }
            detail.truncate(truncate_at);
            detail.push_str("...");
        }
        buffer.records.push(ClusterKvQuorumCommitTraceRecord {
            phase,
            duration,
            observed_at,
            commit_elapsed,
            term,
            log_index,
            revision,
            outcome,
            detail,
        });
    }

    fn flush(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        let Ok(mut buffer) = inner.lock() else {
            return;
        };
        let request_id = buffer.request_id.clone();
        let local_node_id = buffer.local_node_id.clone();
        let initial_term = buffer.initial_term;
        let initial_revision = buffer.initial_revision;
        let started = buffer.started;
        let records = std::mem::take(&mut buffer.records);
        let dropped_records = std::mem::take(&mut buffer.dropped_records);
        drop(buffer);
        for record in records {
            cluster_kv_quorum_commit_phase_trace(
                record.phase,
                &request_id,
                &local_node_id,
                record.duration,
                record.commit_elapsed,
                record.observed_at.elapsed(),
                record.term,
                record.log_index,
                record.revision,
                record.outcome,
                format_args!("{}", record.detail),
            );
        }
        if dropped_records > 0 {
            cluster_kv_quorum_commit_phase_trace(
                "trace_buffer",
                &request_id,
                &local_node_id,
                Duration::default(),
                started.elapsed(),
                Duration::default(),
                initial_term,
                0,
                initial_revision,
                "truncated",
                format_args!(
                    "dropped {dropped_records} quorum phase records after reaching the bounded buffer limit"
                ),
            );
        }
    }
}

struct ClusterKvQuorumCommitSequenceTrace {
    trace: ClusterKvQuorumCommitTrace,
    acquired_at: Option<Instant>,
    term: u64,
    log_index: u64,
    revision: u64,
    completion_outcome: &'static str,
}

impl ClusterKvQuorumCommitSequenceTrace {
    fn new(trace: ClusterKvQuorumCommitTrace, term: u64, revision: u64) -> Self {
        Self {
            trace,
            acquired_at: None,
            term,
            log_index: 0,
            revision,
            completion_outcome: "released",
        }
    }

    fn record_acquired(&mut self) {
        self.acquired_at = Some(Instant::now());
    }

    fn record_result(&mut self, result: &Result<ClusterKvRaftQuorumCommitResult, NetworkError>) {
        match result {
            Ok(commit) => {
                self.term = commit.authority.term;
                self.log_index = commit.authority.commit_index;
                self.revision = commit.authority.revision.0;
                self.completion_outcome = if commit.replayed_committed_request {
                    "replayed"
                } else {
                    "completed"
                };
            }
            Err(_) => self.completion_outcome = "error",
        }
    }
}

impl Drop for ClusterKvQuorumCommitSequenceTrace {
    fn drop(&mut self) {
        let Some(acquired_at) = self.acquired_at else {
            return;
        };
        let outcome = if std::thread::panicking() {
            "unwound"
        } else {
            self.completion_outcome
        };
        self.trace.record(
            "commit_sequence_lock",
            acquired_at.elapsed(),
            self.term,
            self.log_index,
            self.revision,
            outcome,
            format_args!(
                "released the synchronous cluster KV commit sequence lock after quorum commit outcome {outcome}"
            ),
        );
    }
}

pub trait ClusterKvQuorumAsyncHost: Send {
    fn last_local_kv_replication_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64>;

    fn build_kv_replication_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest>;

    fn send_cluster_kv_replication_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn send_cluster_kv_replications_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        Box::pin(async move {
            let deadline = std::time::Instant::now()
                + attempts
                    .iter()
                    .map(|(_, _, timeout)| *timeout)
                    .max()
                    .unwrap_or_default();
            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            for (target, request, timeout) in attempts {
                if accepted >= required_acks {
                    break;
                }
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let result = self
                    .send_cluster_kv_replication_to_node_async(
                        paths,
                        state,
                        &target,
                        &request,
                        timeout.min(remaining),
                    )
                    .await;
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                outcomes.push((target, request, result));
            }
            Ok(outcomes)
        })
    }

    fn on_kv_replication_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_kv_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterRaftQuorumAsyncHost: Send {
    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn send_raft_appends_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        Box::pin(async move {
            let deadline = std::time::Instant::now()
                + attempts
                    .iter()
                    .map(|(_, _, timeout)| *timeout)
                    .max()
                    .unwrap_or_default();
            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            for (target, request, timeout) in attempts {
                if accepted >= required_acks {
                    break;
                }
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let result = self
                    .send_raft_append_to_node_async(
                        paths,
                        state,
                        &target,
                        &request,
                        timeout.min(remaining),
                    )
                    .await;
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                outcomes.push((target, request, result));
            }
            Ok(outcomes)
        })
    }

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl<T> ClusterRaftQuorumAsyncHost for T
where
    T: ClusterKvQuorumAsyncHost,
{
    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        self.send_cluster_kv_replication_to_node_async(
            paths,
            state,
            target_node_id,
            request,
            timeout,
        )
    }

    fn send_raft_appends_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        self.send_cluster_kv_replications_to_quorum_async(paths, state, attempts, required_acks)
    }

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        self.on_kv_replication_sent_async()
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        ClusterKvQuorumAsyncHost::record_node_command_connectivity(
            self,
            paths,
            state,
            target_node_id,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        self.fan_out_committed_cluster_kv_command_async(paths, state, events, targets)
    }
}

pub trait ClusterRaftQuorumTransportHost {
    fn local_has_verified_raft_leadership(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        paths
            .kv_paths()
            .locked_raft_local_is_leader(state)
            .map_err(kv_err)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) {
    }
}

pub trait ClusterRaftQuorumCommitHost:
    sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError> + ClusterRaftQuorumTransportHost
{
}

impl<T> ClusterRaftQuorumCommitHost for T where
    T: sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError>
        + ClusterRaftQuorumTransportHost
{
}

#[cfg(any(test, feature = "test-support"))]
pub struct LocalClusterRaftQuorumCommitHost<'a, S, R, F, O> {
    storage: LocalClusterRaftCommandStorage,
    _paths: std::marker::PhantomData<&'a ClusterPaths>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
}

#[cfg(any(test, feature = "test-support"))]
struct LocalResidentRaftAppendReplicationHost<'a, S, O> {
    paths: &'a ClusterPaths,
    storage: &'a mut LocalClusterRaftCommandStorage,
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: &'a mut S,
    on_raft_append_sent: &'a mut O,
}

#[cfg(any(test, feature = "test-support"))]
impl<S, O> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for LocalResidentRaftAppendReplicationHost<'_, S, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn kv_error(error: KvError) -> Self::Error {
        kv_err(error)
    }

    fn last_local_raft_log_index(
        &mut self,
        _state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        self.storage.last_log_index()
    }

    fn raft_peer_progress_next_index_hint(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        last_local_raft_log_index: u64,
    ) -> Option<u64> {
        sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint(
            &self.paths.kv_paths(),
            &target.node_id,
            state.term,
            last_local_raft_log_index,
        )
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        self.storage.build_raft_append_request(state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        (self.on_raft_append_sent)();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        (self.send_raft_append)(target, request, timeout)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S, R, F, O> sinomo_kv::ClusterKvRaftCommandStorage
    for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::replay_cluster_raft_request_if_committed(
            &mut self.storage,
            request,
        )
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_request_uncommitted(
            &mut self.storage,
            request,
        )
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_rejected_before_commit(
            &mut self.storage,
            request,
            failure,
        )
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_unknown(
            &mut self.storage,
            request,
            detail,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::observe_cluster_raft_term(
            &mut self.storage,
            state,
            term,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S, R, F, O> ClusterRaftQuorumTransportHost for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(&ClusterPaths, &ClusterBootstrapState, &NodeId, ClusterConnectivityState, Option<u64>),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<NodeId>),
    O: FnMut(),
{
    fn local_has_verified_raft_leadership(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.storage.local_is_leader(state)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let endpoint = representative_cluster_endpoint_for_node(state, target_node_id)?;
        let options = self
            .control_plane_timeouts
            .raft_append_replication_options(timeout);
        let mut host = LocalResidentRaftAppendReplicationHost {
            paths,
            storage: &mut self.storage,
            options,
            send_raft_append: &mut self.send_raft_append,
            on_raft_append_sent: &mut self.on_raft_append_sent,
        };
        sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
            state,
            &endpoint,
            &[],
            options.kv_options(),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        (self.record_endpoint_command_connectivity)(
            paths,
            state,
            target_node_id,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) {
        (self.fan_out_committed_cluster_raft_command)(paths, state, events, targets);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn commit_cluster_raft_command_with_local_storage_and_transport<S, R, F, O>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(&ClusterPaths, &ClusterBootstrapState, &NodeId, ClusterConnectivityState, Option<u64>),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<NodeId>),
    O: FnMut(),
{
    let mut host = LocalClusterRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        _paths: std::marker::PhantomData,
        control_plane_timeouts,
        send_raft_append,
        record_endpoint_command_connectivity,
        fan_out_committed_cluster_raft_command,
        on_raft_append_sent,
    };
    commit_cluster_raft_command_with_quorum_driver(paths, state, request, options, &mut host)
}

struct DriverNodeRaftAppendReplicationHost<'a, T> {
    paths: &'a ClusterPaths,
    storage: Option<&'a mut LocalClusterRaftCommandStorage>,
    options: ClusterRaftAppendReplicationOptions,
    transport: &'a mut T,
}

impl<T> sinomo_kv::ClusterKvRaftAppendReplicationHost for DriverNodeRaftAppendReplicationHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        if let Some(storage) = self.storage.as_deref_mut() {
            return storage.last_log_index();
        }
        self.paths
            .kv_paths()
            .locked_last_raft_log_index(&state.local_node_id)
            .map_err(kv_err)
    }

    fn raft_peer_progress_next_index_hint(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        last_local_raft_log_index: u64,
    ) -> Option<u64> {
        sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint(
            &self.paths.kv_paths(),
            target,
            state.term,
            last_local_raft_log_index,
        )
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        if let Some(storage) = self.storage.as_deref_mut() {
            return storage.build_raft_append_request(state, next_index);
        }
        self.paths
            .kv_paths()
            .locked_build_raft_append_request_from_next_index(state, next_index)
            .map_err(kv_err)
    }

    fn append_attempt_timeout(
        &mut self,
        _target: &Self::Target,
        remaining: Duration,
        _request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options.routed_node_append_attempt_timeout(remaining)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        self.transport.on_kv_replication_sent();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        self.transport
            .send_kv_replication_to_node(&request.state, target, request, timeout)
    }
}

struct DriverRaftQuorumCommitHost<'a, T> {
    storage: LocalClusterRaftCommandStorage,
    trace: ClusterKvQuorumCommitTrace,
    initial_term: u64,
    initial_revision: u64,
    driver: &'a mut ClusterControlPlaneDriver<T>,
}

impl<T> sinomo_kv::ClusterKvRaftCommandStorage for DriverRaftQuorumCommitHost<'_, T> {
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::replay_cluster_raft_request_if_committed(
            &mut self.storage,
            request,
        )
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        let observed = self.storage.append_uncommitted_observed(request);
        if observed.log_append_wal_duration.is_some() {
            self.trace.record(
                "compare",
                observed.compare_duration,
                self.initial_term,
                0,
                self.initial_revision,
                "passed",
                format_args!("request identity and transaction branch preflight passed"),
            );
        } else if let Err(error) = &observed.result {
            self.trace.record(
                "compare",
                observed.compare_duration,
                self.initial_term,
                0,
                self.initial_revision,
                "error",
                format_args!("{error}"),
            );
        }
        if let Some(duration) = observed.log_append_wal_duration {
            match &observed.result {
                Ok(entry) => self.trace.record(
                    "log_append_wal",
                    duration,
                    entry.term,
                    entry.index,
                    entry.revision.0,
                    "appended",
                    format_args!("request appended to the local durable Raft log"),
                ),
                Err(error) => self.trace.record(
                    "log_append_wal",
                    duration,
                    self.initial_term,
                    0,
                    self.initial_revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
        }
        observed.result.map_err(kv_err)
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_rejected_before_commit(
            &mut self.storage,
            request,
            failure,
        )
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_unknown(
            &mut self.storage,
            request,
            detail,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::observe_cluster_raft_term(
            &mut self.storage,
            state,
            term,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

impl<T> ClusterRaftQuorumTransportHost for DriverRaftQuorumCommitHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn local_has_verified_raft_leadership(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.storage.local_is_leader(state)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let options = self
            .driver
            .timeouts
            .raft_append_replication_options(timeout);
        let mut host = DriverNodeRaftAppendReplicationHost {
            paths,
            storage: Some(&mut self.storage),
            options,
            transport: &mut self.driver.transport,
        };
        sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
            state,
            target_node_id,
            &[],
            options.kv_options(),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        let Ok(endpoint) = representative_cluster_endpoint_for_node(state, target_node_id) else {
            return;
        };
        self.driver.transport.record_endpoint_command_connectivity(
            paths,
            state,
            &endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) {
        let endpoints = targets
            .iter()
            .filter_map(|node_id| representative_cluster_endpoint_for_node(&state, node_id).ok())
            .collect::<Vec<_>>();
        self.driver
            .transport
            .fan_out_committed_cluster_kv_command(paths, state, events, endpoints);
    }
}

pub(crate) fn commit_cluster_raft_command_with_control_plane_driver<T>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    driver: &mut ClusterControlPlaneDriver<T>,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    T: ClusterControlPlaneTransport,
{
    let request_id = request.request_id.clone();
    let local_node_id = state.local_node_id.clone();
    let initial_term = state.term;
    let initial_revision = state.kv_revision;
    let trace = ClusterKvQuorumCommitTrace::new(
        request_id,
        local_node_id.clone(),
        initial_term,
        initial_revision,
    );
    let mut host = DriverRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, local_node_id.clone()),
        trace: trace.clone(),
        initial_term,
        initial_revision,
        driver,
    };
    commit_cluster_raft_command_with_quorum_driver_and_trace(
        paths, state, request, options, trace, &mut host,
    )
}

pub(crate) fn catch_up_cluster_raft_member_with_control_plane_driver<T>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    driver: &mut ClusterControlPlaneDriver<T>,
) -> Result<ClusterRaftAppendResponse, NetworkError>
where
    T: ClusterControlPlaneTransport,
{
    let options = driver
        .timeouts
        .raft_append_replication_options(driver.timeouts.raft_append_timeout);
    let mut host = DriverNodeRaftAppendReplicationHost {
        paths,
        storage: None,
        options,
        transport: &mut driver.transport,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
        state,
        target_node_id,
        &[],
        options.kv_options(),
        &mut host,
    )
    .map(|outcome| outcome.response)
}

struct NetworkClusterKvRaftQuorumCommitHost<'a, H> {
    paths: &'a ClusterPaths,
    commit_sequence_trace: ClusterKvQuorumCommitSequenceTrace,
    host: &'a mut H,
}

fn remote_voter_node_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, NetworkError> {
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
    {
        return Ok(Vec::new());
    }
    let mut targets = ClusterEndpointSelector::new(paths, state)
        .try_select_internal_unique_node_ids(
            ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
        )?;
    targets.retain(|node_id| node_id != &state.local_node_id && state.node_is_raft_voter(node_id));
    Ok(targets)
}

fn representative_cluster_endpoint_for_node(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> Result<ClusterEndpointRecord, NetworkError> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *target_node_id)
        .and_then(|member| member.endpoints.first())
        .cloned()
        .ok_or_else(|| NetworkError::Io {
            operation: "cluster raft node endpoint representative".to_string(),
            detail: format!("node {target_node_id} has no known endpoint representative"),
        })
}

impl<H> sinomo_kv::ClusterKvRaftCommandStorage for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<std::fs::File>, NetworkError> {
        let started = Instant::now();
        let result = sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        );
        match &result {
            Ok(_) => {
                self.commit_sequence_trace.record_acquired();
                self.commit_sequence_trace.trace.record(
                    "leader_queue",
                    started.elapsed(),
                    self.commit_sequence_trace.term,
                    0,
                    self.commit_sequence_trace.revision,
                    "acquired",
                    format_args!("acquired the synchronous cluster KV commit sequence lock"),
                );
            }
            Err(error) => self.commit_sequence_trace.trace.record(
                "leader_queue",
                started.elapsed(),
                self.commit_sequence_trace.term,
                0,
                self.commit_sequence_trace.revision,
                "error",
                format_args!("{error}"),
            ),
        }
        result.map(Some).map_err(kv_err)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        self.host.replay_cluster_raft_request_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        self.host.append_cluster_raft_request_uncommitted(request)
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        self.host
            .record_cluster_raft_request_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        self.host
            .record_cluster_raft_request_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader()
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.host.observe_cluster_raft_term(state, term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        let started = Instant::now();
        let result = self.host.commit_cluster_raft_index_with_events(index);
        match &result {
            Ok(commit) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                commit.committed_request.term,
                index,
                commit.committed_request.revision.0,
                "applied",
                format_args!(
                    "committed the local Raft entry and published its materialized result"
                ),
            ),
            Err(error) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                self.commit_sequence_trace.term,
                index,
                self.commit_sequence_trace.revision,
                "error",
                format_args!("{error}"),
            ),
        }
        result
    }
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitHost for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Target = NodeId;

    fn local_has_verified_raft_leadership(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.host
            .local_has_verified_raft_leadership(self.paths, state)
    }

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, NetworkError> {
        remote_voter_node_targets(self.paths, state)
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        target
    }

    fn target_description(&self, target: &Self::Target) -> String {
        target.to_string()
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let started = Instant::now();
        let result = self
            .host
            .replicate_cluster_raft_to_node(self.paths, state, target, timeout);
        match &result {
            Ok(response) => self.commit_sequence_trace.trace.record(
                "replication",
                started.elapsed(),
                response.append.term,
                response.append.match_index,
                response.state.kv_revision,
                if response.accepted {
                    "accepted"
                } else {
                    "rejected"
                },
                format_args!("synchronous Raft append to voter {target}"),
            ),
            Err(error) => self.commit_sequence_trace.trace.record(
                "replication",
                started.elapsed(),
                state.term,
                0,
                state.kv_revision,
                "error",
                format_args!("synchronous Raft append to voter {target} failed: {error}"),
            ),
        }
        result
    }

    fn record_target_replication_status(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> Result<(), NetworkError> {
        record_cluster_kv_replication_target_result(status, latency_ms);
        let connectivity_state = match status {
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
            | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                ClusterConnectivityState::Healthy
            }
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                ClusterConnectivityState::Degraded
            }
        };
        self.host.record_node_command_connectivity(
            self.paths,
            state,
            target,
            connectivity_state,
            latency_ms,
        );
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        merge_cluster_state_from_peer(state, peer_state);
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> Result<(), NetworkError> {
        let started = Instant::now();
        let term = state.term;
        let revision = state.kv_revision;
        let event_count = events.len();
        let target_count = targets.len();
        self.host.fan_out_committed_cluster_raft_command(
            self.paths.clone(),
            state,
            events,
            targets,
        );
        self.commit_sequence_trace.trace.record(
            "response",
            started.elapsed(),
            term,
            0,
            revision,
            "completed",
            format_args!("fanned out {event_count} committed events to {target_count} targets"),
        );
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

#[cfg_attr(not(any(test, feature = "test-support")), allow(dead_code))]
pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let trace = ClusterKvQuorumCommitTrace::new(
        request.request_id.clone(),
        state.local_node_id.clone(),
        state.term,
        state.kv_revision,
    );
    commit_cluster_raft_command_with_quorum_driver_and_trace(
        paths, state, request, options, trace, host,
    )
}

fn commit_cluster_raft_command_with_quorum_driver_and_trace(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    trace: ClusterKvQuorumCommitTrace,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_config(paths, &mut consensus_state)?;
    let initial_term = consensus_state.term;
    let initial_revision = consensus_state.kv_revision;
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost {
        paths,
        commit_sequence_trace: ClusterKvQuorumCommitSequenceTrace::new(
            trace.clone(),
            initial_term,
            initial_revision,
        ),
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_raft_command_with_quorum(
        &mut consensus_state,
        request,
        options,
        &mut kv_host,
    );
    kv_host.commit_sequence_trace.record_result(&result);
    drop(kv_host);
    trace.flush();
    let result = result?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}

struct AsyncNodeRaftAppendReplicationHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    storage: SharedLocalClusterRaftCommandStorage,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for AsyncNodeRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "read resident local Raft log index",
                    )?
                    .last_log_index()
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "read resident local raft log index".to_string(),
                    detail: format!("local raft log-index task failed: {source}"),
                })?
        })
    }

    fn raft_peer_progress_next_index_hint_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        last_local_raft_log_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<u64>, Self::Error> {
        let paths = self.paths.kv_paths();
        let peer_node_id = target.clone();
        let current_term = state.term;
        Box::pin(async move {
            Ok(
                sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint_async(
                    paths,
                    peer_node_id,
                    current_term,
                    last_local_raft_log_index,
                )
                .await,
            )
        })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let state = state.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "build resident local Raft append request",
                    )?
                    .build_raft_append_request(&state, next_index)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "build resident local raft append request".to_string(),
                    detail: format!("local raft append-request task failed: {source}"),
                })?
        })
    }

    fn append_attempt_timeout(
        &mut self,
        _target: &Self::Target,
        remaining: Duration,
        _request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options.routed_node_append_attempt_timeout(remaining)
    }

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        self.host.on_raft_append_sent_async()
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append_to_node_async(
            self.paths,
            &request.state,
            target,
            request,
            timeout,
        )
    }
}

struct NetworkClusterKvRaftQuorumCommitAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    storage: SharedLocalClusterRaftCommandStorage,
    append_options: ClusterRaftAppendReplicationOptions,
    trace: ClusterKvQuorumCommitTrace,
    initial_term: u64,
    initial_revision: u64,
    required_replication_node_id: Option<NodeId>,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitAsyncHost
    for NetworkClusterKvRaftQuorumCommitAsyncHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }
    type Target = NodeId;

    fn local_has_verified_raft_leadership_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let state = state.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "verify local Raft leadership",
                    )?
                    .local_is_leader(&state)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "verify local raft leadership".to_string(),
                    detail: format!("local raft leadership task failed: {source}"),
                })?
        })
    }

    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<std::fs::File>, NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        let trace = self.trace.clone();
        let term = self.initial_term;
        let revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let result = runtime
                .spawn_blocking(move || {
                    sinomo_kv::lock_cluster_kv_commit_sequence(
                        &kv_paths,
                        sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
                    )
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft commit sequence lock".to_string(),
                    detail: format!("cluster raft commit sequence lock task failed: {source}"),
                })?;
            match &result {
                Ok(_) => trace.record(
                    "leader_queue",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "acquired",
                    format_args!("acquired the cluster KV commit sequence lock"),
                ),
                Err(error) => trace.record(
                    "leader_queue",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result.map(Some).map_err(kv_err)
        })
    }

    fn replay_cluster_raft_request_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>,
        NetworkError,
    > {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let request = request.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "replay committed cluster Raft request",
                    )?
                    .replay_if_committed(&request)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft replay committed request".to_string(),
                    detail: format!("cluster raft replay committed request task failed: {source}"),
                })?
        })
    }

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::KvLogEntry, NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let trace = self.trace.clone();
        let initial_term = self.initial_term;
        let initial_revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let observed = runtime
                .spawn_blocking(move || {
                    let mut storage = lock_local_cluster_raft_command_storage(
                        &storage,
                        "append uncommitted cluster Raft request",
                    )?;
                    Ok::<_, NetworkError>(storage.append_uncommitted_observed(request))
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft append uncommitted command".to_string(),
                    detail: format!(
                        "cluster raft append uncommitted command task failed: {source}"
                    ),
                })?;
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    trace.record(
                        "compare",
                        started.elapsed(),
                        initial_term,
                        0,
                        initial_revision,
                        "error",
                        format_args!("storage session failed before request comparison: {error}"),
                    );
                    return Err(error);
                }
            };

            if observed.log_append_wal_duration.is_some() {
                trace.record(
                    "compare",
                    observed.compare_duration,
                    initial_term,
                    0,
                    initial_revision,
                    "passed",
                    format_args!("request identity and transaction branch preflight passed"),
                );
            } else if let Err(error) = &observed.result {
                trace.record(
                    "compare",
                    observed.compare_duration,
                    initial_term,
                    0,
                    initial_revision,
                    "error",
                    format_args!("{error}"),
                );
            }

            if let Some(duration) = observed.log_append_wal_duration {
                match &observed.result {
                    Ok(entry) => trace.record(
                        "log_append_wal",
                        duration,
                        entry.term,
                        entry.index,
                        entry.revision.0,
                        "appended",
                        format_args!("request appended to the local durable Raft log"),
                    ),
                    Err(error) => trace.record(
                        "log_append_wal",
                        duration,
                        initial_term,
                        0,
                        initial_revision,
                        "error",
                        format_args!("{error}"),
                    ),
                }
            }
            observed.result.map_err(kv_err)
        })
    }

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a sinomo_kv::KvAuthorityFailure,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let request = request.clone();
        let failure = failure.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "record rejected cluster Raft request",
                    )?
                    .record_rejected_before_commit(&request, &failure)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "record rejected cluster raft request".to_string(),
                    detail: format!("record rejected cluster raft request task failed: {source}"),
                })?
        })
    }

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let request = request.clone();
        let detail = detail.to_string();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "record unknown cluster Raft request",
                    )?
                    .record_unknown(&request, &detail)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "record unknown cluster raft request".to_string(),
                    detail: format!("record unknown cluster raft request task failed: {source}"),
                })?
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(
                        &storage,
                        "discard uncommitted cluster Raft tail",
                    )?
                    .discard_uncommitted_tail_and_clear_leader()
                    .map(|_| ())
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft discard uncommitted tail".to_string(),
                    detail: format!("cluster raft discard uncommitted tail task failed: {source}"),
                })?
        })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::ClusterKvRaftCommitResult, NetworkError>
    {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let trace = self.trace.clone();
        let initial_term = self.initial_term;
        let initial_revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let result = match runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(&storage, "commit cluster Raft index")?
                        .commit_index_with_events(index)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft commit index".to_string(),
                    detail: format!("cluster raft commit index task failed: {source}"),
                }) {
                Ok(result) => result,
                Err(error) => Err(error),
            };
            match &result {
                Ok(commit) => trace.record(
                    "apply",
                    started.elapsed(),
                    commit.committed_request.term,
                    index,
                    commit.committed_request.revision.0,
                    "applied",
                    format_args!(
                        "committed the local Raft entry and published its materialized result"
                    ),
                ),
                Err(error) => trace.record(
                    "apply",
                    started.elapsed(),
                    initial_term,
                    index,
                    initial_revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result
        })
    }

    fn confirm_required_replication_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let Some(target) = self.required_replication_node_id.clone() else {
            return Box::pin(async { Ok(()) });
        };
        if target == state.local_node_id || !state.node_is_raft_voter(&target) {
            return Box::pin(async { Ok(()) });
        }
        let paths = self.paths;
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let append_options = self.append_options;
        let trace = self.trace.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            let started = Instant::now();
            let mut node_host = AsyncNodeRaftAppendReplicationHost {
                runtime,
                paths,
                storage,
                options: ClusterRaftAppendReplicationOptions {
                    timeout,
                    ..append_options
                },
                host,
            };
            let result = sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
                state,
                &target,
                &[],
                node_host.options.kv_options(),
                &mut node_host,
            )
            .await;
            match &result {
                Ok(outcome) if outcome.response.accepted => trace.record(
                    "required_replication_commit_confirm",
                    started.elapsed(),
                    state.term,
                    outcome.response.append.match_index,
                    state.kv_revision,
                    "accepted",
                    format_args!("confirmed committed index to forwarding voter {target}"),
                ),
                Ok(outcome) => trace.record(
                    "required_replication_commit_confirm",
                    started.elapsed(),
                    state.term,
                    outcome.response.append.match_index,
                    state.kv_revision,
                    "rejected",
                    format_args!(
                        "forwarding voter {target} rejected committed-index confirmation: {}",
                        outcome.response.detail
                    ),
                ),
                Err(error) => trace.record(
                    "required_replication_commit_confirm",
                    started.elapsed(),
                    state.term,
                    0,
                    state.kv_revision,
                    "error",
                    format_args!("could not confirm committed index to {target}: {error}"),
                ),
            }
            result.map(|_| ())
        })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move {
            if sinomo_kv::cluster_kv_raft_local_is_voter(state)
                && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
            {
                return Ok(Vec::new());
            }
            let mut targets = ClusterEndpointSelector::select_internal_unique_node_ids_async(
                runtime,
                paths,
                state,
                ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
            )
            .await?;
            targets.retain(|node_id| {
                node_id != &state.local_node_id && state.node_is_raft_voter(node_id)
            });
            Ok(targets)
        })
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        target
    }

    fn target_description(&self, target: &Self::Target) -> String {
        target.to_string()
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, NetworkError> {
        let paths = self.paths;
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let append_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            let mut node_host = AsyncNodeRaftAppendReplicationHost {
                runtime,
                paths,
                storage,
                options: ClusterRaftAppendReplicationOptions {
                    timeout,
                    ..append_options
                },
                host,
            };
            sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
                state,
                target,
                &[],
                node_host.options.kv_options(),
                &mut node_host,
            )
            .await
            .map(|outcome| outcome.response)
        })
    }

    fn replicate_cluster_raft_to_quorum_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<Self::Target>,
        required_remote_acks: usize,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Vec<sinomo_kv::ClusterKvRaftTargetReplicationAttempt<Self::Target, Self::Error>>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let storage = Arc::clone(&self.storage);
        let append_options = self.append_options;
        let required_replication_node_id = self.required_replication_node_id.clone();
        let trace = self.trace.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            let started = Instant::now();
            let target_count = targets.len();
            let mut required_replication_node_id = required_replication_node_id
                .filter(|required| targets.iter().any(|target| target == required));
            if required_remote_acks == 0 || targets.is_empty() {
                trace.record(
                    "replication",
                    started.elapsed(),
                    state.term,
                    0,
                    state.kv_revision,
                    "quorum",
                    format_args!(
                        "replicated to 0 of {required_remote_acks} required remote voters across {target_count} targets"
                    ),
                );
                return Ok(Vec::new());
            }

            struct PendingTarget {
                target: NodeId,
                next_index: u64,
                attempts: usize,
                started: std::time::Instant,
                last_response: Option<ClusterRaftAppendResponse>,
            }

            let last_log_index = {
                let storage = Arc::clone(&storage);
                runtime
                    .spawn_blocking(move || {
                        lock_local_cluster_raft_command_storage(
                            &storage,
                            "read resident cluster Raft log index",
                        )?
                        .last_log_index()
                    })
                    .await
                    .map_err(|source| NetworkError::Io {
                        operation: "read resident cluster Raft log index".to_string(),
                        detail: format!("resident Raft log index task failed: {source}"),
                    })??
            };
            let default_next_index = last_log_index.saturating_add(1);
            let mut pending = Vec::new();
            for target in targets {
                let next_index = sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint_async(
                    paths.kv_paths(),
                    target.clone(),
                    state.term,
                    last_log_index,
                )
                .await
                .filter(|next_index| (1..=default_next_index).contains(next_index))
                .unwrap_or(default_next_index);
                pending.push(PendingTarget {
                    target,
                    next_index,
                    attempts: 0,
                    started: std::time::Instant::now(),
                    last_response: None,
                });
            }

            let deadline = std::time::Instant::now() + timeout;
            let mut accepted = 0_usize;
            let mut completed = Vec::new();
            while accepted < required_remote_acks && !pending.is_empty() {
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }

                let mut sends = Vec::new();
                for target in &mut pending {
                    if required_replication_node_id
                        .as_ref()
                        .is_some_and(|required| required != &target.target)
                    {
                        continue;
                    }
                    if target.attempts >= append_options.max_attempts {
                        continue;
                    }
                    let request = {
                        let storage = Arc::clone(&storage);
                        let state = state.clone();
                        let next_index = target.next_index;
                        runtime
                            .spawn_blocking(move || {
                                lock_local_cluster_raft_command_storage(
                                    &storage,
                                    "build resident cluster Raft append request",
                                )?
                                .build_raft_append_request(&state, next_index)
                            })
                            .await
                            .map_err(|source| NetworkError::Io {
                                operation: "build resident cluster Raft append request".to_string(),
                                detail: format!("resident Raft append build task failed: {source}"),
                            })??
                    };
                    target.attempts = target.attempts.saturating_add(1);
                    record_cluster_kv_replication_send(&request);
                    host.on_raft_append_sent_async().await?;
                    sends.push((
                        target.target.clone(),
                        request,
                        append_options.routed_node_append_attempt_timeout(remaining),
                    ));
                }
                if sends.is_empty() {
                    break;
                }

                let outcomes = host
                    .send_raft_appends_to_quorum_async(
                        paths,
                        state,
                        sends,
                        required_remote_acks.saturating_sub(accepted),
                    )
                    .await?;
                if outcomes.is_empty() {
                    break;
                }
                let mut required_replication_failed = false;
                for (target_node_id, request, result) in outcomes {
                    let Some(position) = pending
                        .iter()
                        .position(|pending| pending.target == target_node_id)
                    else {
                        continue;
                    };
                    let mut target = pending.swap_remove(position);
                    match result {
                        Ok(response) if response.accepted => {
                            record_cluster_kv_replication_catch_up("accepted", target.attempts);
                            accepted = accepted.saturating_add(1);
                            if required_replication_node_id.as_ref() == Some(&target.target) {
                                required_replication_node_id = None;
                            }
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Ok(response) if response.append.term > request.append.term => {
                            if required_replication_node_id.as_ref() == Some(&target.target) {
                                required_replication_failed = true;
                            }
                            record_cluster_kv_replication_catch_up("higher_term", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Ok(response) if target.attempts < append_options.max_attempts => {
                            metrics::counter!("sinomo.kv.replication.backtracks").increment(1);
                            target.next_index =
                                sinomo_kv::cluster_kv_raft_next_index_after_rejection(
                                    target.next_index,
                                    &request,
                                    response.append.match_index,
                                );
                            target.last_response = Some(response);
                            pending.push(target);
                        }
                        Ok(response) => {
                            if required_replication_node_id.as_ref() == Some(&target.target) {
                                required_replication_failed = true;
                            }
                            record_cluster_kv_replication_catch_up("rejected", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Err(error) => {
                            if required_replication_node_id.as_ref() == Some(&target.target) {
                                required_replication_failed = true;
                            }
                            record_cluster_kv_replication_catch_up("failed", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Err(error),
                            });
                        }
                    }
                }
                if required_replication_failed {
                    break;
                }
            }

            if accepted < required_remote_acks {
                for target in pending {
                    record_cluster_kv_replication_catch_up("timeout", target.attempts);
                    let response =
                        target
                            .last_response
                            .unwrap_or_else(|| ClusterRaftAppendResponse {
                                accepted: false,
                                detail: format!(
                                    "raft append did not complete before {}ms catch-up budget",
                                    timeout.as_millis()
                                ),
                                state: state.clone(),
                                append: sinomo_kv::RaftAppendEntriesResponse {
                                    term: 0,
                                    success: false,
                                    match_index: 0,
                                },
                            });
                    completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                        target: target.target,
                        latency_ms: Some(
                            target
                                .started
                                .elapsed()
                                .as_millis()
                                .min(u128::from(u64::MAX)) as u64,
                        ),
                        result: Ok(response),
                    });
                }
            }
            trace.record(
                "replication",
                started.elapsed(),
                state.term,
                0,
                state.kv_revision,
                if accepted >= required_remote_acks {
                    "quorum"
                } else {
                    "incomplete"
                },
                format_args!(
                    "replicated to {accepted} of {required_remote_acks} required remote voters across {target_count} targets"
                ),
            );
            Ok(completed)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths;
        let host = &mut *self.host;
        Box::pin(async move {
            record_cluster_kv_replication_target_result(status, latency_ms);
            let connectivity_state = match status {
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
                | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                    ClusterConnectivityState::Healthy
                }
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                    ClusterConnectivityState::Degraded
                }
            };
            host.record_node_command_connectivity(
                paths,
                state,
                target,
                connectivity_state,
                latency_ms,
            );
            Ok(())
        })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        Box::pin(async move {
            merge_cluster_state_from_peer(state, peer_state);
            Ok(())
        })
    }

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let storage = Arc::clone(&self.storage);
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    lock_local_cluster_raft_command_storage(&storage, "observe cluster Raft term")?
                        .observe_term(term)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft term observation".to_string(),
                    detail: format!("term observation task failed: {source}"),
                })?
        })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let trace = self.trace.clone();
        let term = state.term;
        let revision = state.kv_revision;
        let event_count = events.len();
        let target_count = targets.len();
        let host = &mut *self.host;
        Box::pin(async move {
            let started = Instant::now();
            let result = host
                .fan_out_committed_cluster_raft_command_async(paths, state, events, targets)
                .await;
            match &result {
                Ok(()) => trace.record(
                    "response",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "completed",
                    format_args!(
                        "fanned out {event_count} committed events to {target_count} targets"
                    ),
                ),
                Err(error) => trace.record(
                    "response",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result
        })
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

pub async fn commit_cluster_raft_command_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    required_replication_node_id: Option<NodeId>,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
{
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    consensus_state = runtime
        .spawn_blocking(move || {
            project_committed_cluster_raft_config(&project_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "project committed cluster Raft config".to_string(),
            detail: format!("Raft config projection task failed: {source}"),
        })??;
    let request_id = request.request_id.clone();
    let local_node_id = consensus_state.local_node_id.clone();
    let initial_term = consensus_state.term;
    let initial_revision = consensus_state.kv_revision;
    let trace = ClusterKvQuorumCommitTrace::new(
        request_id,
        local_node_id.clone(),
        initial_term,
        initial_revision,
    );
    let storage = Arc::new(Mutex::new(LocalClusterRaftCommandStorage::new(
        paths,
        local_node_id.clone(),
    )));
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        storage,
        append_options,
        trace: trace.clone(),
        initial_term,
        initial_revision,
        required_replication_node_id,
        host,
    };
    let commit_started = Instant::now();
    let result = sinomo_kv::commit_cluster_kv_raft_command_with_quorum_async(
        &mut consensus_state,
        request,
        options,
        &mut kv_host,
    )
    .await;
    match &result {
        Ok(commit) => trace.record(
            "commit_complete",
            commit_started.elapsed(),
            commit.authority.term,
            commit.authority.commit_index,
            commit.authority.revision.0,
            if commit.replayed_committed_request {
                "replayed"
            } else {
                "completed"
            },
            format_args!(
                "asynchronous quorum commit future returned after releasing its commit sequence lock"
            ),
        ),
        Err(error) => trace.record(
            "commit_complete",
            commit_started.elapsed(),
            initial_term,
            0,
            initial_revision,
            "error",
            format_args!(
                "asynchronous quorum commit future returned after releasing its commit sequence lock: {error}"
            ),
        ),
    }
    drop(kv_host);
    trace.flush();
    let result = result?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}
