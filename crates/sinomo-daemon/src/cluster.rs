use crate::client::materialize_daemon_client_value_call_with_options;
#[cfg(test)]
use crate::status::ensure_daemon_cluster_members;
#[cfg(test)]
use crate::{AuthenticatedDaemonCommand, DaemonClusterRegistration};
use crate::{
    DaemonClusterStatus, DaemonError, DaemonObjectRpcAsyncHost, DaemonPaths, DaemonProbeStatus,
    DaemonRunOptions, IdleStatsAtomics, cluster_node_status_from_daemon_status,
    cluster_paths_from_env, current_daemon_cluster_options,
    current_shadow_daemon_options_on_runtime, daemon_cluster_options_from_status,
    daemon_cluster_process_from_paths, daemon_cluster_status, daemon_cluster_status_matches,
    daemon_connect_address, daemon_options_from_status, daemon_paths_are_shadow,
    fetch_cluster_kv_local_snapshot_from_endpoint_on_runtime,
    fetch_daemon_status_from_endpoint_on_runtime, read_daemon_status,
    send_cluster_member_publish_async, send_cluster_raft_append_on_runtime,
    send_cluster_raft_vote_on_runtime, write_daemon_status,
};
#[cfg(test)]
use sinomo_core::cluster::ClusterJoinRegistrationResponse;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::cluster::ClusterLocalDaemonMembershipAsyncObserver;
use sinomo_core::cluster::{
    ClusterControlInitRequest, ClusterControlInitResponse, ClusterControlInviteRequest,
    ClusterControlInviteResponse, ClusterControlJoinRequest, ClusterControlJoinResponse,
    ClusterControlLeaveRequest, ClusterControlLeaveResponse, ClusterControlRemoveRequest,
    ClusterControlRemoveResponse, ClusterControlRemovedMember, ClusterControlRenameRequest,
    ClusterControlRenameResponse, ClusterControlRoleRequest, ClusterControlRoleResponse,
    PeerConnectivityFetchScope, RemoteClusterControlAuthorityFence,
    accepted_join_connectivity_record_groups, filter_peer_connectivity_response_records,
    merge_connectivity_probe_records_with_live_unprobed,
    newly_healthy_connectivity_target_node_ids, peer_connectivity_import_endpoint_for_record,
    peer_connectivity_import_records_for_reachable_peers,
};
use sinomo_core::cluster::{
    ClusterHealthRow, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse, ClusterStatusAsyncProbe,
};
#[cfg(test)]
use sinomo_core::kv::InProcessKvStore;
use sinomo_core::kv::{
    ClusterKvRaftAppendReplicationOutcome, ClusterKvReplicationRequest,
    ClusterKvReplicationResponse, ClusterKvStateAuthorityVoteRequest,
    ClusterKvStateAuthorityVoteResponse, ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse,
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse,
};
#[cfg(test)]
use sinomo_core::kv::{KvCommand, KvOp};
use sinomo_core::kv::{KvEvent, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::__connectivity_store::LocalClusterConnectivityStoreSnapshot;
use sinomo_core::network::{
    AsyncMdnsDiscoverySocket, ClusterConnectivityRecord, ClusterConnectivityState,
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector, ClusterNetworkInfoRecord,
    ClusterPaths, MdnsDiscoveryCache, ObjectRpcRequest, ObjectRpcResponse,
    daemon_loopback_endpoint,
};
#[cfg(test)]
use sinomo_core::network::{ClusterConnectivityProbeState, MdnsDiscoveryAdvertisement};
use sinomo_core::node::ClusterId;
#[cfg(test)]
use sinomo_core::node::{ClusterInviteId, cluster_member_key};
use sinomo_core::node::{
    DAEMON_MAIN_RUN_CAPABILITY, NodeId, cluster_member_expected_daemon_process,
    cluster_member_is_local_process, shadow_daemon_node_id,
};
use sinomo_core::node::{load_or_create_main_node_identity, load_or_create_shadow_node_identity};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeAvailability,
    ClusterNodeRole, ClusterNodeStatusRecord, ClusterTransportKind, Id,
};
#[cfg(test)]
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointSource, ClusterJoinInvite, ClusterMemberRole,
    ClusterMemberState, ClusterNodeRolloutStatus, TrustDomain,
};
#[cfg(test)]
use sinomo_rollout::{
    DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS, desired_rollout_failure_is_hard,
    desired_rollout_stale_handoff_failure_status, write_rollout_status,
};
#[cfg(test)]
use sinomo_rollout::{
    DesiredRolloutReconcileDecision, SelfRolloutPaths, desired_rollout_reconcile_decision,
    read_rollout_status, record_desired_rollout_stale_handoff_failure_if_needed,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT: Duration = Duration::from_millis(1500);
pub const DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const CLUSTER_CONNECTIVITY_IMPORT_ENDPOINT_TIMEOUT: Duration = CLUSTER_HEALTH_PROBE_TIMEOUT;
/// A fresh join retires the daemon that performed it, so the accepted
/// introducer's one-hop graph should cross that handoff synchronously when it
/// is immediately available. This is a best-effort optimization, not the join
/// deadline: failure only defers discovery to replacement-daemon maintenance.
const DAEMON_JOIN_CONNECTIVITY_HANDOFF_TIMEOUT: Duration = Duration::from_millis(250);
const CLUSTER_HEALTH_ROUTED_PING_TIMEOUT: Duration = Duration::from_millis(750);
const CLUSTER_HEALTH_DIRECT_STATUS_TIMEOUT: Duration = Duration::from_millis(750);
const CLUSTER_HEALTH_DIRECT_STATUS_MAX_ENDPOINTS: usize = 4;
pub const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(test)]
const DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL: Duration =
    sinomo_core::network::CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL;
pub const DAEMON_CLUSTER_PUBLISH_INTERVAL: Duration =
    sinomo_core::network::CLUSTER_NETWORK_PUBLISH_INTERVAL;
#[cfg(test)]
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF: Duration =
    sinomo_core::cluster::CLUSTER_NETWORK_MAINTENANCE_FAILURE_BACKOFF;
/// First-failure publish backoff; doubles per consecutive failure up to
/// `DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF`. Cold cluster formation depends on
/// quick retries: deep-segment nodes race the bridge's first connectivity
/// publication, and a flat long backoff freezes the loser past the formation
/// window (relay routes need connectivity records, which need relayed
/// publishes — only fast retry breaks the cycle once the bridge publishes).
#[cfg(test)]
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF: Duration =
    sinomo_core::cluster::CLUSTER_NETWORK_MAINTENANCE_FAILURE_INITIAL_BACKOFF;
#[cfg(test)]
pub const DAEMON_CLUSTER_DISCOVERY_INTERVAL: Duration =
    sinomo_core::network::CLUSTER_NETWORK_DISCOVERY_INTERVAL;
#[cfg(test)]
const DAEMON_CLUSTER_STARTUP_SETTLE_DURATION: Duration =
    sinomo_core::cluster::CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION;
#[cfg(test)]
pub const DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL: Duration =
    sinomo_core::network::CLUSTER_NETWORK_PROBE_INTERVAL;
// Network-info records describe stable endpoint/environment data, not liveness.
// Active health and connectivity probes handle reachability, so unchanged records
// can refresh sparsely to avoid idle WAL/snapshot churn on low-write storage.
pub const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 =
    sinomo_core::network::CLUSTER_NETWORK_INFO_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 =
    sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 =
    sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 =
    sinomo_core::network::CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS;
const CLUSTER_HEALTH_PEER_ROWS_TIMEOUT: Duration = CLUSTER_HEALTH_PROBE_TIMEOUT;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 =
    sinomo_core::network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD;
const CLUSTER_CONNECTIVITY_IMPORT_CONCURRENCY: usize = 16;
const CLUSTER_NETWORK_PENDING_CONNECTIVITY_TIMEOUT: Duration = Duration::from_secs(8);
const CLUSTER_NETWORK_PENDING_CONNECTIVITY_PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
const CLUSTER_NETWORK_PENDING_CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(10);
const CLUSTER_NETWORK_PENDING_MEMBER_PUBLISH_TIMEOUT: Duration =
    CLUSTER_NETWORK_PENDING_CONTROL_PLANE_TIMEOUT;
const CLUSTER_NETWORK_PENDING_NODE_STATUS_TIMEOUT: Duration =
    CLUSTER_NETWORK_PENDING_CONTROL_PLANE_TIMEOUT;
// A freshly activated daemon first commits the liveness record that formation
// and operators consume. Slow-changing network metadata follows on a separate
// maintenance generation so six simultaneous replacements do not double the
// quorum-commit load on the join critical path.
const CLUSTER_NETWORK_INITIAL_PRESENCE_METADATA_FOLLOWUP_DELAY: Duration = Duration::from_secs(5);
// A stale node-status publication can itself depend on the route that this
// maintenance generation must repair. Bound that publication early enough to
// leave one complete direct probe plus local projection overhead; otherwise a
// restarted public node can remain unreachable forever while every tick spends
// its whole budget trying to publish through the missing route.
const CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE: Duration =
    Duration::from_millis(2_500);

#[cfg(test)]
fn cluster_network_info_key(node_id: &NodeId) -> String {
    format!("/cluster/network/info/{node_id}")
}

#[cfg(test)]
fn cluster_connectivity_key(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &sinomo_core::network::ClusterEndpointId,
) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/{target_node_id}/{endpoint_id}")
}

#[cfg(test)]
fn cluster_endpoint_key(node_id: &NodeId, endpoint: &ClusterEndpointRecord) -> String {
    format!("/cluster/endpoints/{node_id}/{}", endpoint.endpoint_id)
}

#[cfg(test)]
fn cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> String {
    format!(
        "{}:{}:{}",
        source_node_id, endpoint.node_id, endpoint.endpoint_id
    )
}

#[cfg(test)]
fn daemon_cluster_kv_state_authority_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, sinomo_core::network::NetworkError> {
    paths
        .kv_paths()
        .known_raft_voter_leader_id(state)
        .map_err(sinomo_core::network::NetworkError::directory)
}

fn daemon_locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<(), sinomo_core::network::NetworkError> {
    paths
        .kv_paths()
        .locked_set_raft_leader_compact(node_id, term)
        .map_err(sinomo_core::network::NetworkError::directory)
}

#[cfg(test)]
fn daemon_clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
    paths
        .kv_paths()
        .clear_raft_leader_and_write_snapshot_if_changed(node_id)
        .map_err(sinomo_core::network::NetworkError::directory)
}
const CLUSTER_CONNECTIVITY_PROBE_CONCURRENCY: usize = 16;

struct ClusterConnectivityProbePlan {
    active_keys: Vec<String>,
    entries: Vec<sinomo_core::network::ClusterConnectivityProbeBatchEntry>,
    remaining_due: usize,
    convergence_active: bool,
    force_probe: bool,
    timestamp: String,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
}

struct ClusterConnectivityProbeExecution {
    records: Vec<ClusterConnectivityRecord>,
    probed_degraded: bool,
    newly_healthy_target_node_ids: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterConnectivityProbePendingOutcome {
    /// Endpoint observations, local persistence, and publication completed.
    Completed,
    /// Endpoint observations were durably persisted; only their quorum
    /// publication remains owned by the connectivity-publication retry.
    PublicationDeferred,
    /// A newer topology or source-store generation won before local commit.
    /// Its owner is rearmed without charging endpoint-probe failure backoff.
    Superseded,
    /// The probe generation itself could not be durably persisted.
    ProbeFailed,
}

pub(crate) struct ClusterConnectivityPendingProbe {
    paths: ClusterPaths,
    source_node_id: NodeId,
    state: ClusterBootstrapState,
    plan: ClusterConnectivityProbePlan,
    input_generation: u64,
    local_connectivity_snapshot: LocalClusterConnectivityStoreSnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterConnectivityPendingPublish {
    paths: ClusterPaths,
    source_node_id: NodeId,
    state: ClusterBootstrapState,
    /// Exact source-owned set read back after its durable local commit.
    ///
    /// `None` is used only for ordinary publication planning, where the set
    /// is captured from the durable store before the first remote await.
    records: Option<Arc<Vec<ClusterConnectivityRecord>>>,
    input_generation: u64,
    local_connectivity_snapshot: LocalClusterConnectivityStoreSnapshot,
}

#[derive(Default)]
pub(crate) struct ClusterNetworkMaintenancePendingWork {
    publish_claimed: bool,
    probe_claimed: bool,
    connectivity_publish_claimed: bool,
    connectivity_publish: Option<ClusterConnectivityPendingPublish>,
    connectivity_probe: Option<ClusterConnectivityPendingProbe>,
    member_publishes: Vec<ClusterMemberPublishPendingWork>,
    node_status_refreshes: Vec<ClusterNodeStatusPendingRefresh>,
    low_priority_node_status_refreshes: Vec<ClusterNodeStatusPendingRefresh>,
}

impl ClusterNetworkMaintenancePendingWork {
    fn is_empty(&self) -> bool {
        !self.publish_claimed
            && !self.probe_claimed
            && !self.connectivity_publish_claimed
            && self.connectivity_publish.is_none()
            && self.connectivity_probe.is_none()
            && self.member_publishes.is_empty()
            && self.node_status_refreshes.is_empty()
            && self.low_priority_node_status_refreshes.is_empty()
    }
}

struct ClusterMemberPublishPendingWork {
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    force_kv_snapshot_sync: bool,
}

#[derive(Clone)]
struct ClusterNodeStatusPendingRefresh {
    paths: DaemonPaths,
    label: &'static str,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ClusterNodeStatusPendingOutcome {
    succeeded: bool,
    metadata_followup_needed: bool,
}

/// Owns a separately scheduled daemon operation and aborts it when its caller
/// is cancelled. This keeps task boundaries from detaching uncertain cluster
/// mutations when an enclosing deadline expires.
#[derive(Debug)]
struct OwnedDaemonTask<T> {
    task: Option<tokio::task::JoinHandle<T>>,
}

impl<T> OwnedDaemonTask<T> {
    fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self { task: Some(task) }
    }

    async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .task
            .as_mut()
            .expect("owned daemon task is joined once")
            .await;
        self.task.take();
        result
    }

    #[cfg(test)]
    fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }
}

impl<T> Drop for OwnedDaemonTask<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

static CLUSTER_PUBLICATION_CACHE: LazyLock<sinomo_core::cluster::ClusterPublicationCache> =
    LazyLock::new(sinomo_core::cluster::ClusterPublicationCache::default);

thread_local! {
    static COMMAND_CONNECTIVITY_SYNC_IN_FLIGHT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[derive(Clone, Debug)]
struct DaemonClusterRpcMode {
    runtime: tokio::runtime::Handle,
}

impl DaemonClusterRpcMode {
    fn new(runtime: tokio::runtime::Handle) -> Self {
        Self { runtime }
    }

    fn from_handle(runtime: &tokio::runtime::Handle) -> Self {
        Self::new(runtime.clone())
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }
}

/// `Handle::block_on` only makes progress when something else drives the
/// runtime's IO and timer drivers. The daemon owner runtime is multi-threaded
/// (its workers drive them); a current-thread handle that nobody is driving
/// deadlocks silently — even `tokio::time::timeout` never fires. Reject it
/// loudly instead.
fn owner_runtime_handle_can_block(runtime: &tokio::runtime::Handle) -> bool {
    matches!(
        runtime.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread
    )
}

const OWNER_RUNTIME_NOT_BLOCKABLE_DETAIL: &str = "daemon cluster owner-runtime bridge requires a multi-threaded owner runtime; blocking on an undriven current-thread Tokio runtime handle would deadlock — use the async cluster API";

type ClusterNetworkOwnerRuntimeFuture<'a, T> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<T, sinomo_core::network::NetworkError>> + Send + 'a,
    >,
>;

fn block_on_cluster_network_future_on_owner_runtime<T>(
    runtime: &tokio::runtime::Handle,
    future: ClusterNetworkOwnerRuntimeFuture<'_, T>,
) -> Result<T, sinomo_core::network::NetworkError>
where
    T: Send,
{
    if !owner_runtime_handle_can_block(runtime) {
        return Err(sinomo_core::network::NetworkError::Io {
            operation: "daemon cluster runtime".to_string(),
            detail: OWNER_RUNTIME_NOT_BLOCKABLE_DETAIL.to_string(),
        });
    }
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        return if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(future))
        } else {
            Err(sinomo_core::network::NetworkError::Io {
                operation: "daemon cluster runtime".to_string(),
                detail: "daemon cluster owner-runtime bridge cannot run inside a current-thread Tokio runtime; use the async cluster API".to_string(),
            })
        };
    }
    runtime.block_on(future)
}

type DaemonClusterOwnerRuntimeFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, DaemonError>> + Send + 'a>>;

/// Cross the synchronous daemon/cluster boundary with the composed async
/// state machine resident on the heap.
///
/// Several authority operations compose Raft forwarding, routed transport,
/// and response decoding futures. Accepting their concrete future type here
/// made `Handle::block_on` retain that complete state machine in a Tokio
/// blocking worker's stack frame. Keep the trait-object boundary in the type
/// signature so new synchronous callers cannot accidentally restore that
/// stack amplification.
fn block_on_daemon_cluster_future_on_owner_runtime<T>(
    runtime: &tokio::runtime::Handle,
    future: DaemonClusterOwnerRuntimeFuture<'_, T>,
) -> Result<T, DaemonError>
where
    T: Send,
{
    if !owner_runtime_handle_can_block(runtime) {
        return Err(DaemonError::Network {
            address: "daemon cluster runtime".to_string(),
            detail: OWNER_RUNTIME_NOT_BLOCKABLE_DETAIL.to_string(),
        });
    }
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        return if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(future))
        } else {
            Err(DaemonError::Network {
                address: "daemon cluster runtime".to_string(),
                detail: "daemon cluster owner-runtime bridge cannot run inside a current-thread Tokio runtime; use the async cluster API".to_string(),
            })
        };
    }
    runtime.block_on(future)
}

#[cfg(test)]
fn daemon_cluster_test_runtime() -> tokio::runtime::Runtime {
    // Multi-threaded on purpose: tests hand this runtime's HANDLE to daemon
    // hosts whose sync bridges `Handle::block_on` from the test thread. A
    // current-thread runtime that nobody drives would deadlock there (and is
    // now rejected by the owner-runtime bridges).
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("daemon cluster test runtime")
}

fn daemon_cluster_runtime_required_error(
    address: impl Into<String>,
    operation: &'static str,
) -> DaemonError {
    DaemonError::Network {
        address: address.into(),
        detail: format!(
            "{operation} requires the daemon-owned Tokio runtime; daemon-owned cluster code must pass the owner runtime handle"
        ),
    }
}

fn current_shadow_daemon_options_for_cluster_runtime(
    data_dir: &std::path::Path,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonRunOptions, DaemonError> {
    Ok(current_shadow_daemon_options_on_runtime(
        data_dir, timeout, runtime,
    ))
}

async fn current_shadow_daemon_options_for_cluster_async(
    data_dir: &std::path::Path,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonRunOptions {
    match crate::probe_daemon_status_async(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
        runtime,
    )
    .await
    {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    }
}

fn cached_network_info_matches(paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) -> bool {
    CLUSTER_PUBLICATION_CACHE.network_info_matches(paths, info, OffsetDateTime::now_utc())
}

fn remember_network_info(paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) {
    CLUSTER_PUBLICATION_CACHE.remember_network_info(paths, info);
}

fn cached_node_status_matches(paths: &ClusterPaths, status: &ClusterNodeStatusRecord) -> bool {
    CLUSTER_PUBLICATION_CACHE.node_status_matches(paths, status)
}

fn remember_node_status(paths: &ClusterPaths, status: &ClusterNodeStatusRecord) {
    CLUSTER_PUBLICATION_CACHE.remember_node_status(paths, status);
}

fn cached_connectivity_matches(paths: &ClusterPaths, record: &ClusterConnectivityRecord) -> bool {
    CLUSTER_PUBLICATION_CACHE.connectivity_record_matches(paths, record, OffsetDateTime::now_utc())
}

fn cached_connectivity_record_set_matches(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> bool {
    CLUSTER_PUBLICATION_CACHE.connectivity_record_set_matches(
        paths,
        source_node_id,
        records,
        OffsetDateTime::now_utc(),
    )
}

fn remember_connectivity_record_set(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) {
    CLUSTER_PUBLICATION_CACHE.remember_connectivity_record_set(paths, source_node_id, records);
}

fn publish_command_connectivity_if_needed(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    record: &ClusterConnectivityRecord,
    runtime: Option<&tokio::runtime::Handle>,
) {
    if cached_connectivity_matches(paths, record) {
        return;
    }
    COMMAND_CONNECTIVITY_SYNC_IN_FLIGHT.with(|in_flight| {
        if in_flight.get() {
            return;
        }
        in_flight.set(true);
        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            paths,
            &state.local_node_id,
        )
        .unwrap_or_default();
        if let Some(runtime) = runtime {
            let paths = paths.clone();
            let state = state.clone();
            let records = records.clone();
            let runtime = runtime.clone();
            runtime.clone().spawn(async move {
                let _ = import_peer_connectivity_records_from_reachable_endpoints_async(
                    &paths, &state, &records, runtime,
                )
                .await;
            });
        }
        in_flight.set(false);
    });
}

fn remember_successful_join_endpoint_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) {
    // The accepted response path must become locally routable before the
    // registration returns. Quorum publication is deliberately not performed
    // here: JoinAccepted wakes the persistent maintenance reactor, which owns
    // generation-fenced connectivity publication. The former ad-hoc publisher
    // raced subsequent learner admission commits and duplicated that owner.
    let _ = sinomo_core::cluster::ClusterCommandConnectivityObservation {
        connectivity_state: sinomo_core::network::ClusterConnectivityState::Healthy,
        latency_ms: None,
        bypass_failure_grace: false,
        command_auth_denied: false,
    }
    .record_daemon_owned(paths, state, endpoint);
}

pub(crate) fn remember_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_core::network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    runtime: Option<&tokio::runtime::Handle>,
) {
    remember_cluster_endpoint_command_connectivity_with_failure_grace(
        paths,
        state,
        endpoint,
        connectivity_state,
        latency_ms,
        true,
        runtime,
    );
}

#[cfg(test)]
pub(crate) fn remember_cluster_endpoint_command_connectivity_without_failure_grace(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_core::network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    runtime: Option<&tokio::runtime::Handle>,
) {
    remember_cluster_endpoint_command_connectivity_with_failure_grace(
        paths,
        state,
        endpoint,
        connectivity_state,
        latency_ms,
        false,
        runtime,
    );
}

fn remember_cluster_endpoint_command_connectivity_with_failure_grace(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_core::network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
    runtime: Option<&tokio::runtime::Handle>,
) {
    let observation = sinomo_core::cluster::ClusterCommandConnectivityObservation {
        connectivity_state,
        latency_ms,
        bypass_failure_grace: !allow_failure_grace,
        command_auth_denied: false,
    };
    let Ok(record) = observation.record_daemon_owned(paths, state, endpoint) else {
        return;
    };
    publish_command_connectivity_if_needed(paths, state, &record, runtime);
}

pub(crate) fn cluster_paths_from_daemon_paths(
    paths: &DaemonPaths,
) -> Result<ClusterPaths, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    Ok(ClusterPaths::new(&SinomoDataDir::new(data_dir)))
}

/// Hands registered network modules their cluster context once the node has
/// a cluster identity, starting any enabled module whose listener is not yet
/// running (modules registered pre-cluster, before `cluster init`/`join`).
///
/// This replaces built-in transport-specific listener loops: non-built-in
/// transports are module-owned now. Modules advertise their own endpoint
/// records, and those records reach cluster KV through the regular
/// maintenance/status publishes and the repair-route refresh.
///
/// Returns `None` when there is nothing to wait for: no registry, no
/// registered modules, or every enabled module already started at load time
/// (the daemon booted with a cluster identity).
pub fn start_daemon_network_module_context_task(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    cluster: Option<&DaemonClusterStatus>,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    label: &str,
) -> Option<tokio::task::JoinHandle<()>> {
    let needs_context = crate::registry::daemon_network_modules().is_some_and(|modules| {
        modules
            .registry
            .snapshot()
            .iter()
            .any(|status| status.enabled && !status.started)
    });
    if !needs_context {
        return None;
    }
    let Some(data_dir) = paths.root.parent().map(|path| path.to_path_buf()) else {
        tracing::warn!(
            "sinomo {label} network module context task disabled: daemon paths have no data directory parent"
        );
        return None;
    };
    let paths = paths.clone();
    let options = options.clone();
    let mut cluster = cluster.cloned();
    let label = label.to_string();
    let task = runtime.clone().spawn(async move {
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        let daemon_address = daemon_connect_address(&options.host, options.port);
        loop {
            if daemon_cluster_shutdown_requested(&shutdown_rx) {
                return;
            }
            if cluster.is_none() {
                let status_paths = paths.clone();
                let state_path = cluster_paths.state.clone();
                let status_runtime = runtime.clone();
                let status = tokio::task::spawn_blocking(move || {
                    (
                        daemon_cluster_status_from_paths_on_runtime(
                            &status_paths,
                            Some(&status_runtime),
                        )
                        .ok()
                        .flatten(),
                        state_path.exists(),
                    )
                })
                .await;
                let Ok((status, state_exists)) = status else {
                    tracing::warn!("sinomo {label} network module cluster status task failed");
                    if daemon_cluster_sleep_or_shutdown(
                        &mut shutdown_rx,
                        DAEMON_CLUSTER_PUBLISH_INTERVAL,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                };
                cluster = status;
                if cluster.is_none() {
                    let retry_delay = if state_exists {
                        Duration::from_secs(5)
                    } else {
                        DAEMON_CLUSTER_PUBLISH_INTERVAL
                    };
                    if daemon_cluster_sleep_or_shutdown(&mut shutdown_rx, retry_delay).await {
                        return;
                    }
                    continue;
                }
            }
            let Some(current_cluster) = cluster.clone() else {
                continue;
            };
            // The node has a cluster identity: deliver the context once.
            // Start failures are logged inside and recoverable through
            // `/network/modules/set-enabled`, which refreshes the context.
            let module_context = sinomo_core::network::NetworkModuleContext {
                cluster_root: cluster_paths.root.clone(),
                node_id: daemon_network_module_node_id(&current_cluster),
                cluster_id: Some(current_cluster.cluster_id.clone()),
                daemon_forward_address: daemon_address.clone(),
            };
            let started = tokio::task::spawn_blocking(move || {
                crate::registry::start_enabled_network_modules_with_context(&module_context)
            })
            .await
            .unwrap_or(false);
            if started {
                tracing::info!("sinomo {label} network modules started with cluster context");
            }
            return;
        }
    });
    Some(task)
}

fn daemon_cluster_shutdown_requested(shutdown: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn daemon_cluster_sleep_or_shutdown(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    if daemon_cluster_shutdown_requested(shutdown) {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => {
            changed.is_err() || daemon_cluster_shutdown_requested(shutdown)
        }
    }
}

pub(crate) fn daemon_network_module_node_id(cluster: &DaemonClusterStatus) -> NodeId {
    if cluster.process == sinomo_core::node::DAEMON_SHADOW_CLUSTER_ROLE {
        shadow_daemon_node_id(&cluster.node_id)
    } else {
        cluster.node_id.clone()
    }
}

pub(crate) fn daemon_cluster_status_from_paths_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Option<DaemonClusterStatus>, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster_on_runtime(paths, &mut status, runtime)?;
    Ok(status.cluster)
}

/// Network-module context for the daemon's current state; `None` before the
/// node has a cluster identity. Used by `/network/modules/set-enabled` so a
/// module registered pre-cluster (stored context `None`) can still start once
/// the node has joined.
pub(crate) fn daemon_network_module_context_now(
    paths: &DaemonPaths,
) -> Result<Option<sinomo_core::network::NetworkModuleContext>, DaemonError> {
    let Some(cluster) = daemon_cluster_status_from_paths_on_runtime(paths, None)? else {
        return Ok(None);
    };
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let status = read_daemon_status(paths)?;
    Ok(Some(sinomo_core::network::NetworkModuleContext {
        cluster_root: cluster_paths.root.clone(),
        node_id: daemon_network_module_node_id(&cluster),
        cluster_id: Some(cluster.cluster_id.clone()),
        daemon_forward_address: status.address,
    }))
}

/// Admit the canonical resident owner without discarding pending Raft entries.
/// Local commit knowledge can lag a quorum across process replacement.
pub(crate) fn initialize_local_cluster_raft_resident(
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    if crate::daemon_paths_are_shadow(paths) {
        return Ok(false);
    }
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
        return Ok(false);
    };
    cluster_paths
        .kv_paths()
        .locked_initialize_raft_resident(&state.local_node_id)
        .map(|_| true)
        .map_err(Into::into)
}

/// Installs the process-wide storage backstop for the shadow role before any
/// routing, authentication, service, or maintenance component can touch KV.
pub fn deny_shadow_daemon_local_raft_access(paths: &DaemonPaths) -> Result<(), DaemonError> {
    if !crate::daemon_paths_are_shadow(paths) {
        return Ok(());
    }
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_core::kv::deny_process_local_raft_access_for_root(cluster_paths.kv_paths().raft)
        .map_err(Into::into)
}

pub(crate) async fn handle_daemon_cluster_kv_request_async_on_runtime_with_timeouts(
    paths: &DaemonPaths,
    request: sinomo_core::kv::KvTxnRequest,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, DaemonError> {
    handle_daemon_cluster_kv_request_async_with_forwarding_and_timeouts(
        paths, request, true, runtime, timeouts,
    )
    .await
}

pub(crate) async fn handle_daemon_cluster_kv_request_compact_async_on_runtime_with_timeouts(
    paths: &DaemonPaths,
    request: sinomo_core::kv::KvTxnRequest,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<sinomo_core::kv::ClusterKvTxnCompactCommitResult, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let task_runtime = runtime.clone();
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::from_handle(&task_runtime),
            timeouts,
        );
        sinomo_core::cluster::handle_cluster_kv_txn_request_compact_async_with_host(
            &task_runtime,
            &cluster_paths,
            request,
            timeouts.kv_txn_quorum_options(true),
            &mut host,
        )
        .await
        .map_err(DaemonError::from)
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "compact cluster KV quorum commit task".to_string(),
        detail: source.to_string(),
    })?
}

pub(crate) async fn handle_daemon_cluster_kv_authoritative_snapshot_request_async_on_runtime(
    paths: &DaemonPaths,
    body: &[u8],
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let body = body.to_vec();
    let timeouts = cluster_control_plane_timeouts();
    let task_runtime = runtime.clone();
    // A learner serves a local authoritative read by forwarding it over the
    // complete routed stream stack. Keep that outbound protocol stack in an
    // owned task instead of nesting it beneath the inbound service dispatcher.
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::from_handle(&task_runtime),
            timeouts,
        );
        sinomo_core::cluster::handle_cluster_kv_authoritative_snapshot_request_async_with_host(
            &task_runtime,
            &cluster_paths,
            &body,
            timeouts.kv_txn_quorum_options(allow_forwarding),
            &mut host,
        )
        .await
        .map_err(DaemonError::from)
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "cluster KV authoritative read task".to_string(),
        detail: source.to_string(),
    })?
}

pub(crate) async fn handle_daemon_cluster_kv_authoritative_range_prefix_request_async_on_runtime(
    paths: &DaemonPaths,
    prefix: String,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<sinomo_core::kv::KvEntry>>, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let task_runtime = runtime.clone();
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        read_cluster_kv_authoritative_range_prefix_with_cluster_paths_async_on_runtime(
            &cluster_paths,
            prefix,
            allow_forwarding,
            &task_runtime,
        )
        .await
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "cluster KV authoritative prefix read task".to_string(),
        detail: source.to_string(),
    })?
}

/// Read one bounded prefix through the daemon-owned Raft authority path
/// without materializing or publishing the complete cluster KV snapshot.
pub(crate) async fn read_cluster_kv_authoritative_range_prefix_with_cluster_paths_async_on_runtime(
    cluster_paths: &ClusterPaths,
    prefix: String,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<sinomo_core::kv::KvEntry>>, DaemonError> {
    let timeouts = cluster_control_plane_timeouts();
    let started = Instant::now();
    let deadline = started
        .checked_add(timeouts.raft_append_timeout)
        .unwrap_or(started);
    let mut state = read_daemon_cluster_state_for_authority_before_deadline(
        runtime,
        cluster_paths,
        deadline,
        timeouts.raft_append_timeout,
    )
    .await?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(daemon_cluster_kv_authority_timeout(
            timeouts.raft_append_timeout,
        ));
    }
    let mut remaining_timeouts = timeouts;
    remaining_timeouts.raft_append_timeout = remaining;
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::from_handle(runtime),
        remaining_timeouts,
    );
    let read = sinomo_core::cluster::read_cluster_kv_authoritative_range_prefix_async_with_host(
        runtime,
        cluster_paths,
        &mut state,
        prefix,
        remaining_timeouts.kv_txn_quorum_options(allow_forwarding),
        &mut host,
    );
    match tokio::time::timeout(remaining, read).await {
        Ok(result) => result.map_err(DaemonError::from),
        Err(_) => Err(daemon_cluster_kv_authority_timeout(
            timeouts.raft_append_timeout,
        )),
    }
}

pub(crate) async fn handle_daemon_cluster_kv_authoritative_entry_request_async_on_runtime(
    paths: &DaemonPaths,
    key: String,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Option<sinomo_core::kv::KvEntry>>, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let timeouts = cluster_control_plane_timeouts();
    let task_runtime = runtime.clone();
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        let mut state = sinomo_core::cluster::read_cluster_state_without_kv_async(
            &task_runtime,
            &cluster_paths,
        )
        .await?;
        let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::from_handle(&task_runtime),
            timeouts,
        );
        sinomo_core::cluster::read_cluster_kv_authoritative_entry_async_with_host(
            &task_runtime,
            &cluster_paths,
            &mut state,
            key,
            timeouts.kv_txn_quorum_options(allow_forwarding),
            &mut host,
        )
        .await
        .map_err(DaemonError::from)
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "cluster KV authoritative point read task".to_string(),
        detail: source.to_string(),
    })?
}

pub(crate) async fn handle_daemon_cluster_kv_authoritative_entries_request_async_on_runtime(
    paths: &DaemonPaths,
    keys: Vec<String>,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>, DaemonError>
{
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let task_runtime = runtime.clone();
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime(
            &cluster_paths,
            keys,
            allow_forwarding,
            &task_runtime,
        )
        .await
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "cluster KV authoritative exact-key-set read task".to_string(),
        detail: source.to_string(),
    })?
}

/// Read one exact key set through the daemon-owned Raft authority path without
/// materializing or publishing the complete cluster KV snapshot.
pub(crate) async fn read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime(
    cluster_paths: &ClusterPaths,
    keys: Vec<String>,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>, DaemonError>
{
    let timeouts = cluster_control_plane_timeouts();
    read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_with_timeouts(
        cluster_paths,
        keys,
        allow_forwarding,
        runtime,
        timeouts,
    )
    .await
}

/// Deadline-preserving exact-key authority read used by focused control-plane
/// tests. The inherited deadline is also capped by the daemon's normal
/// Raft authority budget, and is never restarted by a retrying caller.
#[cfg(test)]
pub(crate) async fn read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_before_deadline(
    cluster_paths: &ClusterPaths,
    keys: Vec<String>,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
    deadline: Instant,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>, DaemonError>
{
    let timeouts = cluster_control_plane_timeouts();
    let started = Instant::now();
    let normal_deadline = started
        .checked_add(timeouts.raft_append_timeout)
        .unwrap_or(started);
    let deadline = deadline.min(normal_deadline);
    let timeout = deadline.saturating_duration_since(started);
    read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_with_timeouts_before_deadline(
        cluster_paths,
        keys,
        allow_forwarding,
        runtime,
        timeouts,
        deadline,
        timeout,
    )
    .await
}

async fn read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_with_timeouts(
    cluster_paths: &ClusterPaths,
    keys: Vec<String>,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>, DaemonError>
{
    let started = Instant::now();
    let deadline = started
        .checked_add(timeouts.raft_append_timeout)
        .unwrap_or(started);
    read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_with_timeouts_before_deadline(
        cluster_paths,
        keys,
        allow_forwarding,
        runtime,
        timeouts,
        deadline,
        timeouts.raft_append_timeout,
    )
    .await
}

async fn read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_with_timeouts_before_deadline(
    cluster_paths: &ClusterPaths,
    keys: Vec<String>,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
    deadline: Instant,
    timeout: Duration,
) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>, DaemonError>
{
    let mut state = read_daemon_cluster_state_for_authority_before_deadline(
        runtime,
        cluster_paths,
        deadline,
        timeout,
    )
    .await?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(daemon_cluster_kv_authority_timeout(timeout));
    }
    let mut remaining_timeouts = timeouts;
    remaining_timeouts.raft_append_timeout = remaining;
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::from_handle(runtime),
        remaining_timeouts,
    );
    let read = sinomo_core::cluster::read_cluster_kv_authoritative_entries_async_with_host(
        runtime,
        cluster_paths,
        &mut state,
        keys,
        remaining_timeouts.kv_txn_quorum_options(allow_forwarding),
        &mut host,
    );
    match tokio::time::timeout(remaining, read).await {
        Ok(result) => result.map_err(DaemonError::from),
        Err(_) => Err(daemon_cluster_kv_authority_timeout(timeout)),
    }
}

fn daemon_cluster_kv_authority_timeout(timeout: Duration) -> DaemonError {
    sinomo_core::network::NetworkError::timeout(
        "load local cluster state for authoritative cluster KV read",
        sinomo_core::network::NetworkTimeoutPhase::Overall,
        timeout,
        sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
    )
    .into()
}

async fn read_daemon_cluster_state_for_authority_before_deadline(
    runtime: &tokio::runtime::Handle,
    cluster_paths: &ClusterPaths,
    deadline: Instant,
    timeout: Duration,
) -> Result<ClusterBootstrapState, DaemonError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(daemon_cluster_kv_authority_timeout(timeout));
    }
    let task_paths = cluster_paths.clone();
    let mut task = OwnedDaemonTask::new(runtime.spawn_blocking(move || {
        sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&task_paths)
            .map(|state| Arc::try_unwrap(state).unwrap_or_else(|state| (*state).clone()))
    }));
    match tokio::time::timeout(remaining, task.join()).await {
        Ok(result) => result
            .map_err(|source| DaemonError::Network {
                address: "local cluster state authority-read task".to_string(),
                detail: source.to_string(),
            })?
            .map_err(DaemonError::from),
        Err(_) => Err(daemon_cluster_kv_authority_timeout(timeout)),
    }
}

pub(crate) async fn handle_daemon_cluster_kv_request_outcome_request_async_on_runtime(
    paths: &DaemonPaths,
    body: &[u8],
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, DaemonError> {
    let request = sinomo_core::kv::decode_cluster_kv_request_outcome_rpc_request_body(body)?;
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let timeouts = cluster_control_plane_timeouts();
    let mut state =
        sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, &cluster_paths).await?;
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::from_handle(runtime),
        timeouts,
    );
    // The service adapter enables forwarding only for a process-local signer.
    // The same distinction selects the originating node's attempt ledger:
    // authenticated forwarded RPCs return retained consensus history only,
    // then the original node overlays its own durable attempt state.
    sinomo_core::cluster::read_cluster_kv_request_outcome_async_with_host(
        runtime,
        &cluster_paths,
        &mut state,
        request.request_id,
        allow_forwarding,
        timeouts.kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )
    .await
    .map_err(DaemonError::from)
}

pub(crate) async fn handle_daemon_cluster_kv_txn_rpc_request_compact_async_on_runtime(
    paths: &DaemonPaths,
    request: sinomo_core::kv::KvTxnRequest,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::kv::ClusterKvTxnRpcResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::from_handle(runtime),
        cluster_control_plane_timeouts(),
    );
    sinomo_core::cluster::handle_forwarded_cluster_kv_txn_rpc_request_compact_async_with_host(
        runtime,
        &cluster_paths,
        request,
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )
    .await
    .map_err(Into::into)
}

async fn handle_daemon_cluster_kv_request_async_with_forwarding_and_timeouts(
    paths: &DaemonPaths,
    request: sinomo_core::kv::KvTxnRequest,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let task_runtime = runtime.clone();
    // The same boundary is required for learner writes: a local service call
    // may forward through routed object RPC before returning to the inbound
    // dispatcher. Abort-on-drop preserves cancellation ownership for the
    // potentially mutating operation.
    let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
        handle_cluster_kv_request_with_cluster_paths_async_on_runtime_with_timeouts(
            &cluster_paths,
            request,
            allow_forwarding,
            &task_runtime,
            timeouts,
        )
        .await
    }));
    task.join().await.map_err(|source| DaemonError::Network {
        address: "cluster KV quorum commit task".to_string(),
        detail: source.to_string(),
    })?
}

async fn handle_cluster_kv_request_with_cluster_paths_async_on_runtime_with_timeouts(
    cluster_paths: &ClusterPaths,
    request: sinomo_core::kv::KvTxnRequest,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::from_handle(runtime),
        timeouts,
    );
    sinomo_core::cluster::handle_cluster_kv_txn_request_async_with_host(
        runtime,
        cluster_paths,
        request,
        timeouts.kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )
    .await
    .map_err(Into::into)
}

pub(crate) struct DaemonClusterJoinRegistrationHost<'a> {
    runtime: tokio::runtime::Handle,
    object_rpc_host: DaemonObjectRpcAsyncHost,
    driver: sinomo_core::cluster::ClusterJoinRegistrationDriver<
        DaemonClusterControlPlaneTransport,
        DaemonLocalDaemonMembershipObserver<'a>,
    >,
}

impl<'a> DaemonClusterJoinRegistrationHost<'a> {
    pub(crate) fn new_on_runtime(
        daemon_paths: &'a DaemonPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let runtime = runtime.clone();
        Self {
            runtime: runtime.clone(),
            object_rpc_host: DaemonObjectRpcAsyncHost::new(&runtime, true).with_paths(daemon_paths),
            driver: sinomo_core::cluster::ClusterJoinRegistrationDriver::new(
                daemon_cluster_control_plane_driver_with_mode(DaemonClusterRpcMode::new(
                    runtime.clone(),
                )),
                DaemonLocalDaemonMembershipObserver {
                    daemon_paths,
                    runtime: runtime.clone(),
                },
            ),
        }
    }
}

struct DaemonLocalDaemonMembershipObserver<'a> {
    daemon_paths: &'a DaemonPaths,
    runtime: tokio::runtime::Handle,
}

impl sinomo_core::cluster::ClusterLocalDaemonMembershipObserver
    for DaemonLocalDaemonMembershipObserver<'_>
{
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
        sinomo_core::network::NetworkError,
    > {
        local_daemon_membership_observation_from_daemon_paths_on_runtime(
            self.daemon_paths,
            state,
            &self.runtime,
        )
        .map(Some)
        .map_err(|error| daemon_host_network_error("cluster join local membership", error))
    }
}

impl sinomo_core::cluster::ClusterLocalDaemonMembershipAsyncObserver
    for DaemonLocalDaemonMembershipObserver<'_>
{
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
    > {
        let daemon_paths = self.daemon_paths.clone();
        let state = state.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            local_daemon_membership_observation_from_daemon_paths_async(
                &daemon_paths,
                &state,
                &runtime,
            )
            .await
            .map(Some)
            .map_err(|error| daemon_host_network_error("cluster join local membership", error))
        })
    }
}

impl sinomo_core::cluster::ClusterJoinRegistrationHost for DaemonClusterJoinRegistrationHost<'_> {
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
        sinomo_core::network::NetworkError,
    > {
        sinomo_core::cluster::ClusterJoinRegistrationHost::local_daemon_membership_observation(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &sinomo_core::network::ObjectRpcRequest,
    ) -> Result<sinomo_core::network::ObjectRpcResponse, sinomo_core::network::NetworkError> {
        sinomo_core::cluster::ClusterJoinRegistrationHost::call_node_object_rpc(
            &mut self.driver,
            state,
            target_node_id,
            request,
        )
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>, sinomo_core::network::NetworkError>
    {
        sinomo_core::cluster::ClusterJoinRegistrationHost::authoritative_cluster_kv_snapshot(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn commit_join_registration_txn(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, sinomo_core::network::NetworkError> {
        sinomo_core::cluster::ClusterJoinRegistrationHost::commit_join_registration_txn(
            &mut self.driver,
            paths,
            state,
            request,
        )
    }

    fn admit_cluster_raft_learner(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        now: OffsetDateTime,
    ) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
        sinomo_core::cluster::ClusterJoinRegistrationHost::admit_cluster_raft_learner(
            &mut self.driver,
            paths,
            state,
            node_id,
            now,
        )
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), sinomo_core::network::NetworkError> {
        // Joining through a bridge has a narrow ordering window: the voter can
        // accept the member before the rest of the cluster has a routable edge
        // to it. Record this accepted response-path observation immediately so
        // local route planning does not wait for the maintenance reactor.
        remember_successful_join_endpoint_connectivity(paths, state, endpoint);
        Ok(())
    }
}

impl sinomo_core::network::ClusterObjectRpcAsyncHost for DaemonClusterJoinRegistrationHost<'_> {
    fn observe_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_core::network::ClusterObjectRpcObservation,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<'a, ()> {
        self.object_rpc_host
            .observe_endpoint_rpc_async(endpoint, observation)
    }

    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        self.object_rpc_host
            .open_object_rpc_stream_async(endpoint, health)
    }
}

impl sinomo_core::cluster::ClusterJoinRegistrationAsyncHost
    for DaemonClusterJoinRegistrationHost<'_>
{
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
    > {
        self.driver
            .observer_mut()
            .local_daemon_membership_observation_async(paths, state)
    }

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>,
    > {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let mut host = DaemonClusterMemberMutationHost::new_on_runtime(&runtime);
            sinomo_core::cluster::ClusterMemberMutationAsyncHost::authoritative_cluster_kv_snapshot_async(
                &mut host,
                paths,
                state,
            )
            .await
        })
    }

    fn commit_join_registration_txn_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvTxnCommitResult,
    > {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let mut host = DaemonClusterMemberMutationHost::new_on_runtime(&runtime);
            sinomo_core::cluster::ClusterMemberMutationAsyncHost::commit_cluster_kv_txn_with_quorum_async(
                &mut host,
                paths,
                state,
                request,
            )
            .await
        })
    }

    fn admit_cluster_raft_learner_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        node_id: &'a NodeId,
        now: OffsetDateTime,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let mut host = DaemonClusterMemberMutationHost::new_on_runtime(&runtime);
            sinomo_core::cluster::add_cluster_raft_learner_async_with_host(
                paths, state, node_id, now, &mut host,
            )
            .await
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_successful_join_endpoint_connectivity(paths, state, endpoint);
            Ok(())
        })
    }
}

pub(crate) async fn forward_cluster_invite_challenge_to_cluster_state_authority_async_on_runtime(
    paths: &DaemonPaths,
    challenge: &ClusterInviteChallengeRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state_paths = cluster_paths.clone();
    let state = runtime
        .spawn_blocking(move || sinomo_core::cluster::read_cluster_state(&state_paths))
        .await
        .map_err(|source| {
            DaemonError::from(sinomo_core::network::NetworkError::Io {
                operation: "read cluster state".to_string(),
                detail: format!("cluster state read task failed: {source}"),
            })
        })??;
    let mut host = DaemonClusterJoinRegistrationHost::new_on_runtime(paths, runtime);
    let authority_targets =
        sinomo_core::network::ClusterEndpointSelector::select_unique_node_targets_async(
            runtime,
            &cluster_paths,
            &state,
            sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateAuthority,
        )
        .await?;
    crate::cluster_join_trace(format_args!(
        "challenge invite={} authority targets: {}",
        challenge.invite_id,
        authority_targets
            .iter()
            .map(|endpoint| format!(
                "{} via {} ({:?})",
                endpoint.node_id, endpoint.address, endpoint.transport
            ))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let mut failures = Vec::new();
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
    for target in &authority_targets {
        let started = Instant::now();
        match client
            .call_target(
                &state,
                &target.node_id,
                sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                    sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE,
                    sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
                    challenge,
                )?,
                sinomo_core::cluster::main_daemon_cluster_rpc_options(
                    sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(
                        sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
                    ),
                ),
            )
            .await
        {
            Ok(result) => {
                crate::cluster_join_trace(format_args!(
                    "challenge invite={} routed authority {} via {} ({:?}) ok in {:?}",
                    challenge.invite_id,
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                tracing::debug!(
                    "sinomo daemon join challenge forwarded through route to authority {} via {} ({:?}) in {:?}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                );
                return result.decode_body().await.map_err(DaemonError::from);
            }
            Err(error) => {
                crate::cluster_join_trace(format_args!(
                    "challenge invite={} routed authority {} via {} ({:?}) failed in {:?}: {error}",
                    challenge.invite_id,
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                failures.push(format!(
                    "routed node {} via {} ({:?}): {error}",
                    target.node_id, target.address, target.transport
                ));
            }
        }
    }
    {
        let mut direct_client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        for endpoint in &authority_targets {
            let started = Instant::now();
            match direct_client
                .call_endpoint(
                    &state,
                    endpoint,
                    sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                        sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE,
                        sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
                        challenge,
                    )?,
                    sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(
                        sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
                    ),
                )
                .await
            {
                Ok(result) => {
                    crate::cluster_join_trace(format_args!(
                        "challenge invite={} direct authority {} ({:?}) ok in {:?}",
                        challenge.invite_id,
                        endpoint.address,
                        endpoint.transport,
                        started.elapsed()
                    ));
                    tracing::debug!(
                        "sinomo daemon join challenge forwarded directly to authority {} ({:?}) in {:?}",
                        endpoint.address,
                        endpoint.transport,
                        started.elapsed()
                    );
                    return result.decode_body().await.map_err(DaemonError::from);
                }
                Err(error) => {
                    crate::cluster_join_trace(format_args!(
                        "challenge invite={} direct authority {} ({:?}) failed in {:?}: {error}",
                        challenge.invite_id,
                        endpoint.address,
                        endpoint.transport,
                        started.elapsed()
                    ));
                    tracing::debug!(
                        "sinomo daemon direct join challenge forward to authority {} ({:?}) failed after {:?}: {error}",
                        endpoint.address,
                        endpoint.transport,
                        started.elapsed()
                    );
                    failures.push(format!(
                        "direct node {} via {} ({:?}): {error}",
                        endpoint.node_id, endpoint.address, endpoint.transport
                    ));
                }
            }
        }
    }
    Err(DaemonError::from(
        sinomo_core::network::NetworkError::ClusterReadOnly {
            detail: format!(
                "join challenge introducer is not a channel/issuer and could not relay the challenge to any cluster state authority{}",
                if failures.is_empty() {
                    String::new()
                } else {
                    format!(": {}", failures.join("; "))
                }
            ),
        },
    ))
}

fn call_node_object_rpc_on_runtime(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    rpc_request: &sinomo_core::network::ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: target_node_id.to_string(),
        detail: "cluster paths are unavailable for node object RPC".to_string(),
    })?;
    call_node_object_rpc_with_cluster_paths_on_runtime(
        &paths,
        state,
        target_node_id,
        rpc_request,
        options,
        runtime,
    )
}

pub(crate) fn call_node_object_rpc_with_cluster_paths_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    rpc_request: &sinomo_core::network::ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    block_on_daemon_cluster_future_on_owner_runtime(
        runtime,
        Box::pin(materialize_daemon_client_value_call_with_options(
            "cluster node object RPC value call",
            options,
            async {
                call_node_object_rpc_with_cluster_paths_async_on_runtime(
                    paths,
                    state,
                    target_node_id,
                    sinomo_core::network::ObjectRpcCall::value(rpc_request.clone()),
                    options,
                    runtime,
                )
                .await
                .map_err(DaemonError::from)
            },
        )),
    )
}

pub(crate) async fn call_node_object_rpc_with_cluster_paths_async_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    call: sinomo_core::network::ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResult, sinomo_core::network::NetworkError> {
    if target_node_id == &state.local_node_id {
        let data_dir =
            paths
                .root
                .parent()
                .ok_or_else(|| sinomo_core::network::NetworkError::Io {
                    operation: "resolve local cluster node service path".to_string(),
                    detail: format!(
                        "cluster path {} has no data directory parent",
                        paths.root.display()
                    ),
                })?;
        let daemon_paths = DaemonPaths::new(&SinomoDataDir::new(data_dir));
        return crate::client::send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
            &daemon_paths,
            target_node_id,
            call,
            options,
            runtime,
            true,
        )
        .await;
    }

    let mut host = DaemonObjectRpcAsyncHost::new(runtime, true).with_cluster_paths(paths);
    // KV authority forwarding already carries the exact Raft membership fence
    // in `state`; remote dispatch must retain it rather than recursively
    // consulting the KV-backed dynamic route directory.
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host,
        paths,
        state,
        sinomo_core::network::cluster_directory_for(paths),
    )?;
    client
        .call_target(state, target_node_id, call, options)
        .await
}

#[cfg(test)]
pub(crate) fn publish_local_members_to_quorum_if_needed_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_core::cluster::publish_local_members_to_quorum_if_needed_with_host(
        paths,
        state,
        sinomo_core::cluster::ClusterLocalMemberPublishOptions {
            member_publish_timeout: DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
            force_kv_snapshot_sync: false,
        },
        &mut host,
    )
    .map_err(Into::into)
}

pub(crate) async fn publish_local_members_to_quorum_if_needed_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    force_kv_snapshot_sync: bool,
    runtime: tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::new(runtime.clone()),
        timeouts,
    );
    sinomo_core::cluster::publish_local_members_to_quorum_if_needed_async_with_host(
        &runtime,
        paths,
        state,
        sinomo_core::cluster::ClusterLocalMemberPublishOptions {
            member_publish_timeout: DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
            force_kv_snapshot_sync,
        },
        &mut host,
    )
    .await
    .map_err(Into::into)
}

pub(crate) fn ensure_cluster_directory_installed() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sinomo_core::cluster::install_kv_cluster_directory);
}

pub fn cluster_state_authority_watch_targets(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<ClusterEndpointRecord>, DaemonError> {
    // This reactor runs in the shadow process. Select authority endpoints from
    // the passive bootstrap projection and let the RPC path discover/forward
    // to the current leader; a local leader lookup would open and retain the
    // main voter's Raft store in the shadow.
    let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(cluster_paths)?;
    let mut targets = Vec::new();
    if state.node_is_raft_voter(&state.local_node_id) {
        let data_dir = cluster_paths.root.parent().ok_or_else(|| DaemonError::Io {
            path: cluster_paths.root.display().to_string(),
            detail: "cluster paths have no data directory parent".to_string(),
        })?;
        let main_options = current_daemon_cluster_options(data_dir)
            .map(|(main, _)| main)
            .unwrap_or_default();
        targets.push(daemon_loopback_endpoint(
            &state.local_node_id,
            daemon_connect_address(&main_options.host, main_options.port),
            0,
        ));
    }
    for endpoint in ClusterEndpointSelector::new(cluster_paths, &state)
        .try_select(ClusterEndpointSelectionPurpose::ClusterStateAuthority)?
    {
        if endpoint.node_id == state.local_node_id
            || targets.iter().any(|target| {
                target.node_id == endpoint.node_id && target.address == endpoint.address
            })
        {
            continue;
        }
        targets.push(endpoint);
    }
    if targets.is_empty() {
        return Err(DaemonError::Network {
            address: "cluster state authority watch".to_string(),
            detail: "no KV voter endpoint is currently known".to_string(),
        });
    }
    Ok(targets)
}

type DaemonClusterStorageMutationGuard = tokio::sync::OwnedMutexGuard<()>;
type LocalRaftResidentInitializationTaskOutput =
    (Result<bool, DaemonError>, DaemonClusterStorageMutationGuard);

#[derive(Debug)]
enum LocalRaftResidentInitialization {
    Pending,
    InFlight(OwnedDaemonTask<LocalRaftResidentInitializationTaskOutput>),
    Ready,
}

impl LocalRaftResidentInitialization {
    fn from_startup(initialized: bool) -> Self {
        if initialized {
            Self::Ready
        } else {
            Self::Pending
        }
    }

    fn is_in_flight(&self) -> bool {
        matches!(self, Self::InFlight(_))
    }

    #[cfg(test)]
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    #[cfg(test)]
    fn in_flight_task_is_finished(&self) -> bool {
        matches!(self, Self::InFlight(task) if task.is_finished())
    }
}

#[derive(Debug)]
pub(crate) struct ClusterNetworkMaintenance {
    cadence: sinomo_core::cluster::ClusterNetworkMaintenanceCadence,
    resource_mode: sinomo_device::NodeResourceMode,
    pub discovery: Option<AsyncMdnsDiscoverySocket>,
    pub discovery_cache: MdnsDiscoveryCache,
    pub connectivity:
        std::collections::BTreeMap<String, sinomo_core::network::ClusterConnectivityProbeState>,
    pub connectivity_cursor: usize,
    pub environment_id: Option<String>,
    /// Tracks the single cancellation-safe attempt that opens the local voter
    /// through the canonical resident slot. The in-flight task owns the
    /// storage-mutation guard, so a timed-out tick cannot detach a waiter or
    /// expose the live paths to leave/quarantine before it is rejoined.
    local_raft_resident_initialization: LocalRaftResidentInitialization,
    pub solo_cluster_idle: bool,
    pub tick: u64,
    /// The exact durable source generation whose idempotent quorum publish is
    /// still pending. Probe completion replaces this with a newer generation;
    /// cancellation never discards it.
    retained_connectivity_publish: Option<ClusterConnectivityPendingPublish>,
    pub idle_stats: Arc<IdleStatsAtomics>,
}

fn effective_network_maintenance_role(
    configured_role: &ClusterNodeRole,
    resource_mode: sinomo_device::NodeResourceMode,
) -> ClusterNodeRole {
    match resource_mode {
        sinomo_device::NodeResourceMode::Normal => configured_role.clone(),
        sinomo_device::NodeResourceMode::Constrained => match configured_role {
            ClusterNodeRole::Mobile | ClusterNodeRole::Sensor => configured_role.clone(),
            ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
                ClusterNodeRole::Mobile
            }
        },
        sinomo_device::NodeResourceMode::Restricted => ClusterNodeRole::Sensor,
    }
}

impl std::ops::Deref for ClusterNetworkMaintenance {
    type Target = sinomo_core::cluster::ClusterNetworkMaintenanceCadence;

    fn deref(&self) -> &Self::Target {
        &self.cadence
    }
}

impl std::ops::DerefMut for ClusterNetworkMaintenance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cadence
    }
}

#[cfg(test)]
fn daemon_cluster_publish_interval(paths: &DaemonPaths) -> Duration {
    daemon_cluster_publish_interval_on_runtime(paths, None)
}

#[cfg(test)]
fn daemon_cluster_publish_interval_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Duration {
    let Ok(Some(cluster)) = daemon_cluster_status_from_paths_on_runtime(paths, runtime) else {
        return DAEMON_CLUSTER_PUBLISH_INTERVAL;
    };
    let Ok(cluster_paths) = cluster_paths_from_daemon_paths(paths) else {
        return DAEMON_CLUSTER_PUBLISH_INTERVAL;
    };
    sinomo_core::cluster::cluster_network_publish_interval_for_node(
        &cluster_paths,
        &cluster.node_id,
    )
}

/// Serializes destructive cluster-storage swaps (`/cluster/control/leave`
/// quarantines and re-initializes the live KV/raft files) against the
/// background maintenance tick, whose publishes commit into those same files.
/// Without this, a leave racing a tick can quarantine files mid-commit or
/// have the tick re-create state the leave just moved aside.
static DAEMON_CLUSTER_STORAGE_MUTATION_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonNodeStatusPublishPhase {
    DaemonStatusRead,
    ClusterStatusEnsure,
    ClusterStateRead,
    PresenceMatch,
    NodeStatusCommit,
    NetworkInfoCommit,
}

fn daemon_node_status_publish_phase_name(phase: DaemonNodeStatusPublishPhase) -> &'static str {
    match phase {
        DaemonNodeStatusPublishPhase::DaemonStatusRead => "daemon_status_read",
        DaemonNodeStatusPublishPhase::ClusterStatusEnsure => "cluster_status_ensure",
        DaemonNodeStatusPublishPhase::ClusterStateRead => "cluster_state_read",
        DaemonNodeStatusPublishPhase::PresenceMatch => "presence_match",
        DaemonNodeStatusPublishPhase::NodeStatusCommit => "node_status_commit",
        DaemonNodeStatusPublishPhase::NetworkInfoCommit => "network_info_commit",
    }
}

/// Cancellation-safe timing for the sequential node-status publication path.
///
/// The maintenance runner cancels this future when its fair-share budget is
/// exhausted, so an ordinary event after an `.await` cannot identify the
/// operation that owned the timeout. The guard records `completed = false`
/// when that cancellation drops the active phase. Ordinary operation pays no
/// clock-read or formatting cost while the existing maintenance trace target
/// is disabled.
struct DaemonNodeStatusPublishPhaseObservation {
    phase: DaemonNodeStatusPublishPhase,
    started: Option<Instant>,
    operation_budget: Duration,
    completed: bool,
}

impl DaemonNodeStatusPublishPhaseObservation {
    fn start(phase: DaemonNodeStatusPublishPhase, operation_budget: Duration) -> Self {
        Self {
            phase,
            started: tracing::enabled!(
                target: "sinomo.daemon.node_status_publish",
                tracing::Level::TRACE
            )
            .then(Instant::now),
            operation_budget,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for DaemonNodeStatusPublishPhaseObservation {
    fn drop(&mut self) {
        let Some(started) = self.started else {
            return;
        };
        let duration_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let operation_budget_us =
            self.operation_budget.as_micros().min(u128::from(u64::MAX)) as u64;
        tracing::trace!(
            target: "sinomo.daemon.node_status_publish",
            step = "node_status_publish",
            phase = daemon_node_status_publish_phase_name(self.phase),
            completed = self.completed,
            duration_us,
            operation_budget_us,
            "cluster KV node status publish phase ended"
        );
    }
}

impl ClusterNetworkMaintenance {
    pub async fn with_idle_stats(
        idle_stats: Arc<IdleStatsAtomics>,
        startup_local_raft_resident_initialized: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            cadence: sinomo_core::cluster::ClusterNetworkMaintenanceCadence::immediate(
                now,
                ClusterNodeRole::default(),
            ),
            resource_mode: sinomo_device::NodeResourceMode::Normal,
            discovery: AsyncMdnsDiscoverySocket::bind_default().await.ok(),
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            environment_id: None,
            local_raft_resident_initialization: LocalRaftResidentInitialization::from_startup(
                startup_local_raft_resident_initialized,
            ),
            solo_cluster_idle: false,
            tick: 0,
            idle_stats,
            retained_connectivity_publish: None,
        }
    }

    async fn retry_main_local_raft_resident_initialization(
        &mut self,
        paths: &DaemonPaths,
    ) -> Result<DaemonClusterStorageMutationGuard, DaemonError> {
        if daemon_paths_are_shadow(paths)
            || matches!(
                &self.local_raft_resident_initialization,
                LocalRaftResidentInitialization::Ready
            )
        {
            return Ok(Arc::clone(&*DAEMON_CLUSTER_STORAGE_MUTATION_LOCK)
                .lock_owned()
                .await);
        }

        if matches!(
            &self.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ) {
            let storage_guard = Arc::clone(&*DAEMON_CLUSTER_STORAGE_MUTATION_LOCK)
                .lock_owned()
                .await;
            let retry_paths = paths.clone();
            let task = tokio::task::spawn_blocking(move || {
                let result = initialize_local_cluster_raft_resident(&retry_paths);
                crate::control::request_daemon_cluster_maintenance(
                    crate::control::DaemonClusterMaintenanceReason::LifecycleWake,
                );
                (result, storage_guard)
            });
            self.local_raft_resident_initialization =
                LocalRaftResidentInitialization::InFlight(OwnedDaemonTask::new(task));
        }

        let task_result = match &mut self.local_raft_resident_initialization {
            LocalRaftResidentInitialization::InFlight(task) => task.join().await,
            LocalRaftResidentInitialization::Pending | LocalRaftResidentInitialization::Ready => {
                unreachable!("local Raft resident initialization task must be in flight")
            }
        };
        // `join` removes the completed handle. Restore a retryable terminal
        // state before propagating either the join or initialization error.
        self.local_raft_resident_initialization = LocalRaftResidentInitialization::Pending;
        let (initialized, storage_guard) = task_result.map_err(|source| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: format!("local Raft resident initialization task failed: {source}"),
        })?;
        if initialized? {
            self.local_raft_resident_initialization = LocalRaftResidentInitialization::Ready;
        }
        Ok(storage_guard)
    }

    pub async fn tick_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        runtime: tokio::runtime::Handle,
    ) -> Result<Option<ClusterNetworkMaintenancePendingWork>, DaemonError> {
        ensure_cluster_directory_installed();
        self.tick = self.tick.saturating_add(1);
        self.idle_stats.incr_ticks_total();
        let now = Instant::now();
        // A prior caller that observed an error must normally close the active
        // claim explicitly. Keep the maintenance owner self-healing for any
        // alternate caller: an unfinished generation is a failed generation,
        // never permission to spin the same overdue timestamps again.
        if self.cadence.has_active_due_claims() {
            self.record_active_tick_plan_failure(now);
        }
        // Keep an expired startup-settle deadline as the one-shot marker. The
        // active checks below compare it with `now`, while retaining it stops
        // an unconverged degraded endpoint from re-arming another settle window
        // and being softened forever. An explicit environment refresh replaces
        // the marker with a new future deadline.
        let connectivity_publish_retry_waiting = self.connectivity_publish_retry_waiting(now);
        let connectivity_publish_retry_due = self
            .connectivity_publish_retry_due_duration(now)
            .is_some_and(|duration| duration.is_zero());
        let due = sinomo_core::network::cluster_network_maintenance_due(
            now,
            &self.schedule_with_connectivity_publish_backoff(now),
        );
        let publish_due = due.publish_due;
        let probe_due = (due.probe_due || self.probe_retry_due(now))
            && !connectivity_publish_retry_waiting
            && !self.probe_retry_waiting(now);
        let discovery_due = (due.discovery_due || self.discovery_retry_due(now))
            && !self.discovery_retry_waiting(now);
        let scheduled_work_due =
            publish_due || probe_due || discovery_due || connectivity_publish_retry_due;
        if !scheduled_work_due && !self.local_raft_resident_initialization.is_in_flight() {
            return Ok(None);
        }
        if scheduled_work_due {
            self.claim_due_classes(
                now,
                publish_due,
                probe_due,
                discovery_due,
                connectivity_publish_retry_due,
            );
        }
        let _storage_guard = self
            .retry_main_local_raft_resident_initialization(paths)
            .await?;
        // A completion wake only harvests the one retained initializer and
        // releases its mutation fence. It must not create unscheduled work.
        if !scheduled_work_due {
            return Ok(None);
        }
        let mut pending_work = ClusterNetworkMaintenancePendingWork::default();

        let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        let state_path = cluster_paths.state.clone();
        let state_exists = tokio::task::spawn_blocking(move || state_path.exists())
            .await
            .map_err(|source| DaemonError::Io {
                path: cluster_paths.state.display().to_string(),
                detail: format!("cluster state existence task failed: {source}"),
            })?;
        if !state_exists {
            self.solo_cluster_idle = false;
            self.settle_connectivity_without_publication();
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            self.complete_all_active_due_claims();
            return Ok(None);
        }

        // The shadow observes the bootstrap projection for passive topology
        // context only. It must return before discovery writes, KV-backed
        // reads, local voter repair, publication, or authority-bearing work.
        if daemon_paths_are_shadow(paths) {
            self.idle_stats.incr_disk_reads_cluster_state();
            let state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &cluster_paths)
                    .await
                    .map_err(DaemonError::from)?;
            self.local_node_role = sinomo_core::network::cluster_local_node_role(&state);
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            self.settle_connectivity_without_publication();
            self.solo_cluster_idle = sinomo_core::network::cluster_is_settled_solo(&state);
            self.complete_all_active_due_claims();
            return Ok(None);
        }

        let mut discovery_already_processed = false;
        if self.solo_cluster_idle
            && (publish_due || probe_due || discovery_due || connectivity_publish_retry_due)
        {
            self.idle_stats.incr_disk_reads_cluster_state();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &cluster_paths)
                    .await
                    .map_err(DaemonError::from)?;
            self.local_node_role = sinomo_core::network::cluster_local_node_role(&state);
            let mut discovered = false;
            if discovery_due {
                self.last_discovery = now;
                discovered = self.process_discovery(&mut state);
                if sinomo_core::network::cluster_network_stable_discovery_announcement_due(
                    self.tick, discovered,
                ) {
                    self.announce(&state).await;
                }
                discovery_already_processed = true;
                if discovered {
                    sinomo_core::cluster::write_cluster_state_async(
                        &runtime,
                        &cluster_paths,
                        &state,
                    )
                    .await
                    .map_err(DaemonError::from)?;
                }
                // The discovery generation is now fully observed and, when
                // changed, durably projected. Later publish/probe failures do
                // not own this claim and must not back it off.
                self.complete_discovery_claim();
            }
            if !discovered && sinomo_core::network::cluster_is_settled_solo(&state) {
                self.settle_connectivity_without_publication();
                if publish_due {
                    self.last_publish = now;
                }
                if probe_due {
                    self.last_probe = now;
                }
                self.complete_all_active_due_claims();
                return Ok(None);
            }
            self.solo_cluster_idle = false;
        }

        self.idle_stats.incr_ticks_with_work();
        self.idle_stats.incr_disk_reads_cluster_state();
        // Committed KV writers invalidate the merged state cache reactively.
        // Scheduled maintenance consumes that projection; rebuilding the full
        // materialized snapshot on every publish/probe deadline turns an idle
        // timer into an O(KV) filesystem scan. Concrete publications and probes
        // retain their typed quorum and freshness checks below.
        let mut state = if publish_due || probe_due || connectivity_publish_retry_due {
            sinomo_core::cluster::read_cluster_state_cached_async(&runtime, &cluster_paths).await
        } else {
            sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &cluster_paths)
                .await
        }
        .map_err(DaemonError::from)?;
        self.local_node_role = sinomo_core::network::cluster_local_node_role(&state);
        if sinomo_core::network::cluster_state_needs_join_registration(&state) {
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            self.settle_connectivity_without_publication();
            self.solo_cluster_idle = false;
            self.complete_all_active_due_claims();
            return Ok(None);
        }
        self.cadence.ensure_connectivity_settle_deadline(
            now,
            sinomo_core::network::cluster_is_settled_solo(&state),
        );
        let state_before_maintenance = state.clone();
        let mut discovered = false;

        if discovery_due && !discovery_already_processed {
            self.last_discovery = now;
            discovered = self.process_discovery(&mut state);
            if discovered {
                self.connectivity_converged = false;
                sinomo_core::cluster::write_cluster_state_async(&runtime, &cluster_paths, &state)
                    .await
                    .map_err(DaemonError::from)?;
            }
            if sinomo_core::network::cluster_network_stable_discovery_announcement_due(
                self.tick, discovered,
            ) {
                self.announce(&state).await;
            }
            // Complete discovery before entering any unrelated publication or
            // probe await. The original state baseline remains available below
            // so a same-tick publish can still project discovered topology.
            self.complete_discovery_claim();
        }
        // Avoid cloning the full bootstrap state on ordinary publish/probe
        // ticks. A separate post-discovery baseline is needed only when this
        // generation actually changed and was already written above.
        let state_after_discovery = discovered.then(|| state.clone());

        if publish_due || probe_due || connectivity_publish_retry_due {
            let shadow_options = current_shadow_daemon_options_for_cluster_async(
                data_dir,
                DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
                &runtime,
            )
            .await;
            self.idle_stats.incr_disk_reads_network_module_endpoints();
            let refreshed_at = OffsetDateTime::now_utc();
            let daemon_membership_changed = refresh_local_daemon_membership_async(
                &runtime,
                &cluster_paths,
                &mut state,
                options,
                &shadow_options,
                refreshed_at,
            )
            .await?
            .changed();
            let network_module_endpoints_changed =
                refresh_local_network_module_endpoints(&mut state, refreshed_at)?;
            let local_state_changed = daemon_membership_changed || network_module_endpoints_changed;
            if local_state_changed {
                self.connectivity_converged = false;
                sinomo_core::cluster::write_cluster_state_async(&runtime, &cluster_paths, &state)
                    .await
                    .map_err(DaemonError::from)?;
            }
        }

        let cluster_is_solo = sinomo_core::network::cluster_is_settled_solo(&state);
        if publish_due || probe_due || connectivity_publish_retry_due {
            sinomo_core::cluster::refresh_cluster_connectivity_environment_async(
                &runtime,
                &cluster_paths,
                &state,
                &mut self.environment_id,
                &mut self.connectivity,
                OffsetDateTime::now_utc(),
            )
            .await?;
        }

        if publish_due {
            self.last_publish = now;
            // Component-derived control-plane state remains component-owned.
            // A generic network publication tick must never poll or republish
            // module state.
            if !cluster_is_solo {
                self.idle_stats.incr_kv_publish_member();
                pending_work
                    .member_publishes
                    .push(ClusterMemberPublishPendingWork {
                        paths: cluster_paths.clone(),
                        state: state.clone(),
                        force_kv_snapshot_sync: false,
                    });
            }
            if !cluster_is_solo && !probe_due && !self.connectivity_publish_retry_waiting(now) {
                pending_work.connectivity_publish =
                    Some(self.plan_connectivity_publish(&cluster_paths, &state)?);
            }
            if !cluster_is_solo {
                pending_work
                    .node_status_refreshes
                    .push(ClusterNodeStatusPendingRefresh {
                        paths: paths.clone(),
                        label: "node status",
                    });
                let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
                let shadow_status = crate::probe_daemon_status_async(
                    &shadow_paths,
                    DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
                    &runtime,
                )
                .await;
                if matches!(shadow_status, DaemonProbeStatus::Running(_)) {
                    pending_work.low_priority_node_status_refreshes.push(
                        ClusterNodeStatusPendingRefresh {
                            paths: shadow_paths,
                            label: "shadow status",
                        },
                    );
                }
            }
        }

        if probe_due {
            self.last_probe = now;
            if !cluster_is_solo {
                let plan = self.select_connectivity_probe_plan(&cluster_paths, &state);
                match sinomo_core::cluster::cluster_connectivity_probe_plan_action(
                    plan.force_probe,
                    plan.entries.len(),
                    plan.remaining_due,
                    plan.convergence_active,
                    publish_due || connectivity_publish_retry_due,
                ) {
                    sinomo_core::cluster::ClusterConnectivityProbePlanAction::Probe => {
                        let local_connectivity_snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&cluster_paths)?;
                        pending_work.connectivity_probe = Some(ClusterConnectivityPendingProbe {
                            paths: cluster_paths.clone(),
                            source_node_id: state.local_node_id.clone(),
                            state: state.clone(),
                            plan,
                            input_generation: self.connectivity_input_generation,
                            local_connectivity_snapshot,
                        });
                    }
                    sinomo_core::cluster::ClusterConnectivityProbePlanAction::PublishRetained => {
                        // A previous quorum publication failed, but no endpoint
                        // observation is due. Retry the retained record set without
                        // running the probe merge/write path again.
                        pending_work.connectivity_publish =
                            Some(self.plan_connectivity_publish(&cluster_paths, &state)?);
                    }
                    sinomo_core::cluster::ClusterConnectivityProbePlanAction::None => {}
                }
            } else {
                self.finish_connectivity_convergence();
            }
        } else if connectivity_publish_retry_due && !cluster_is_solo {
            // Quorum publication has its own generation and deadline. It may
            // retry while an unrelated endpoint-probe generation is backed
            // off, without claiming or completing that probe generation.
            pending_work.connectivity_publish =
                Some(self.plan_connectivity_publish(&cluster_paths, &state)?);
        }

        let maintenance_changed = !sinomo_core::node::cluster_bootstrap_state_stable_eq(
            state_after_discovery
                .as_ref()
                .unwrap_or(&state_before_maintenance),
            &state,
        );

        if maintenance_changed {
            self.connectivity_converged = cluster_is_solo;
            // Discovery and local membership/module refresh each persist their
            // own mutation before completing above. Do not rewrite the same
            // bootstrap generation a second time at the shared tail.
        }
        if cluster_is_solo {
            self.settle_connectivity_without_publication();
        }
        self.solo_cluster_idle = cluster_is_solo && !discovered;
        if self.cadence.discovery_claimed() {
            self.complete_discovery_claim();
        }
        pending_work.publish_claimed = self.cadence.take_publish_claim();
        pending_work.probe_claimed = self.cadence.take_probe_claim();
        pending_work.connectivity_publish_claimed = self.cadence.take_connectivity_publish_claim();
        Ok((!pending_work.is_empty()).then_some(pending_work))
    }

    #[cfg(test)]
    fn schedule(&self) -> sinomo_core::network::ClusterNetworkMaintenanceSchedule {
        self.resource_adjusted_schedule(self.cadence.schedule(self.discovery.is_some()))
    }

    fn plan_connectivity_publish(
        &self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<ClusterConnectivityPendingPublish, DaemonError> {
        let local_connectivity_snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(paths)?;
        let retained_records = self
            .retained_connectivity_publish
            .as_ref()
            .filter(|retained| {
                retained.paths == *paths
                    && retained.source_node_id == state.local_node_id
                    && retained.input_generation == self.connectivity_input_generation
                    && retained.local_connectivity_snapshot == local_connectivity_snapshot
            })
            .and_then(|retained| {
                let records = retained.records.as_ref()?;
                let current =
                    sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                        paths,
                        &state.local_node_id,
                    )
                    .ok()?;
                (current.as_slice() == records.as_slice()).then(|| Arc::clone(records))
            });
        Ok(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: state.local_node_id.clone(),
            state: state.clone(),
            records: retained_records,
            input_generation: self.connectivity_input_generation,
            local_connectivity_snapshot,
        })
    }

    fn schedule_with_connectivity_publish_backoff(
        &self,
        now: Instant,
    ) -> sinomo_core::network::ClusterNetworkMaintenanceSchedule {
        self.resource_adjusted_schedule(
            self.cadence
                .schedule_with_connectivity_publish_backoff(now, self.discovery.is_some()),
        )
    }

    pub fn next_due_duration(&self, now: Instant) -> Duration {
        let mut cadence = self.cadence.clone();
        cadence.local_node_role = self.effective_network_maintenance_role();
        cadence.next_due_duration(now, self.discovery.is_some())
    }

    pub fn set_resource_mode(&mut self, resource_mode: sinomo_device::NodeResourceMode) {
        self.resource_mode = resource_mode;
    }

    fn effective_network_maintenance_role(&self) -> ClusterNodeRole {
        effective_network_maintenance_role(&self.local_node_role, self.resource_mode)
    }

    fn resource_adjusted_schedule(
        &self,
        mut schedule: sinomo_core::network::ClusterNetworkMaintenanceSchedule,
    ) -> sinomo_core::network::ClusterNetworkMaintenanceSchedule {
        schedule.local_node_role = self.effective_network_maintenance_role();
        schedule
    }

    pub fn request_publish_refresh(&mut self, now: Instant) {
        self.last_publish =
            sinomo_core::network::cluster_network_maintenance_last_timestamp_for_next_due(
                now,
                sinomo_core::network::cluster_network_publish_interval_for_role(
                    &self.effective_network_maintenance_role(),
                ),
                Duration::ZERO,
            );
    }

    pub fn request_connectivity_refresh(&mut self, now: Instant) {
        self.cadence.request_connectivity_refresh(now);
        self.last_probe =
            sinomo_core::network::cluster_network_maintenance_last_timestamp_for_next_due(
                now,
                sinomo_core::network::cluster_network_probe_interval_for_role(
                    &self.effective_network_maintenance_role(),
                ),
                Duration::ZERO,
            );
    }

    pub fn request_network_environment_refresh(&mut self, now: Instant) {
        self.request_connectivity_refresh(now);
        self.startup_settle_until = now
            .checked_add(sinomo_core::cluster::CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION);
    }

    pub fn request_discovery_refresh(&mut self, now: Instant) {
        self.cadence.request_discovery_refresh(now);
        self.last_discovery =
            sinomo_core::network::cluster_network_maintenance_last_timestamp_for_next_due(
                now,
                sinomo_core::network::cluster_network_discovery_interval_for_role(
                    &self.effective_network_maintenance_role(),
                ),
                Duration::ZERO,
            );
    }

    fn complete_all_active_due_claims(&mut self) {
        if self.cadence.take_publish_claim() {
            self.record_publish_success();
        }
        if self.cadence.take_probe_claim() {
            self.record_probe_success();
        }
        if self.cadence.take_discovery_claim() {
            self.record_discovery_success();
        }
        if self.cadence.take_connectivity_publish_claim() {
            self.record_connectivity_publish_success();
        }
    }
}

impl ClusterNetworkMaintenance {
    fn record_node_status_publish(&self, outcome: &NodeStatusPublishOutcome) {
        if outcome.network_info_published {
            self.idle_stats.incr_kv_publish_network_info();
        }
        if outcome.node_status_published {
            self.idle_stats.incr_kv_publish_node_status();
        }
    }

    fn process_discovery(&mut self, state: &mut ClusterBootstrapState) -> bool {
        let Some(discovery) = &self.discovery else {
            return false;
        };
        let Ok(advertisements) = discovery.drain() else {
            return false;
        };
        self.idle_stats
            .add_mdns_announces_recv(advertisements.len() as u64);
        let changed = sinomo_core::network::apply_lan_discovery_advertisements(
            state,
            &mut self.discovery_cache,
            advertisements,
        );
        self.discovery_cache.expire_after_tick(self.tick);
        changed
    }

    async fn announce(&self, state: &ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(advertisement) =
            sinomo_core::network::lan_discovery_advertisement_for_state(state, self.tick)
        else {
            return;
        };
        self.idle_stats.incr_mdns_announces_sent();
        let _ = discovery.announce(&advertisement).await;
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_work_async(
        &mut self,
        pending: ClusterNetworkMaintenancePendingWork,
        runtime: tokio::runtime::Handle,
    ) {
        self.run_pending_work_with_deadline_async(pending, runtime, None)
            .await;
    }

    pub(crate) async fn run_pending_work_until_async(
        &mut self,
        pending: ClusterNetworkMaintenancePendingWork,
        runtime: tokio::runtime::Handle,
        deadline: Instant,
    ) {
        self.run_pending_work_with_deadline_async(pending, runtime, Some(deadline))
            .await;
    }

    async fn run_pending_work_with_deadline_async(
        &mut self,
        pending: ClusterNetworkMaintenancePendingWork,
        runtime: tokio::runtime::Handle,
        deadline: Option<Instant>,
    ) {
        let ClusterNetworkMaintenancePendingWork {
            publish_claimed,
            probe_claimed,
            connectivity_publish_claimed,
            connectivity_publish,
            connectivity_probe,
            member_publishes,
            node_status_refreshes,
            low_priority_node_status_refreshes,
        } = pending;
        let mut publish_succeeded = true;
        let mut presence_metadata_followup_needed = false;
        let mut probe_succeeded = true;
        // A main daemon's presence is part of join realization, not optional
        // graph maintenance. Publish it before probing so unreachable shadow
        // endpoints cannot leave a successfully joined main absent from the
        // cluster health read model for an entire maintenance deadline.
        // Connectivity-derived member/service publishes remain behind the
        // fresh local observation that can repair their routes.
        let connectivity_probe_scheduled = connectivity_probe.is_some();
        let connectivity_publish_scheduled = connectivity_publish.is_some();
        let node_status_deadline = if connectivity_probe_scheduled {
            cluster_network_deadline_with_tail_reserve(
                deadline,
                CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE,
            )
        } else {
            deadline
        };
        let connectivity_probe_deadline = deadline;
        for refresh in node_status_refreshes {
            let outcome = cluster_network_pending_step_with_deadline(
                "node status refresh",
                CLUSTER_NETWORK_PENDING_NODE_STATUS_TIMEOUT,
                node_status_deadline,
                self.run_pending_node_status_refresh_async(refresh, runtime.clone()),
            )
            .await;
            match outcome {
                Some(outcome) => {
                    publish_succeeded &= outcome.succeeded;
                    presence_metadata_followup_needed |= outcome.metadata_followup_needed;
                }
                None => publish_succeeded = false,
            }
        }
        if let Some(probe) = connectivity_probe {
            let probe_result = self
                .run_pending_connectivity_probe_async(
                    probe,
                    runtime.clone(),
                    connectivity_probe_deadline,
                )
                .await;
            match probe_result {
                ClusterConnectivityProbePendingOutcome::Completed
                | ClusterConnectivityProbePendingOutcome::PublicationDeferred
                | ClusterConnectivityProbePendingOutcome::Superseded => {}
                ClusterConnectivityProbePendingOutcome::ProbeFailed => {
                    // A cancelled/local-persistence probe must rerun. Quorum
                    // publication failure is deliberately excluded: its exact
                    // retained-record retry owns that backoff.
                    self.connectivity_force_probe = true;
                    probe_succeeded = false;
                    if connectivity_publish_claimed {
                        self.record_connectivity_publish_failure();
                    }
                }
            }
        }
        // A fresh probe reads and publishes the current record set itself, so a
        // same-tick cached publish would only duplicate work and steal budget
        // from node/member status publication.
        if !connectivity_probe_scheduled && let Some(publish) = connectivity_publish {
            // Capture the durable source generation before entering the
            // cancelable remote publication phase.
            self.run_pending_connectivity_publish_async(publish, runtime.clone(), deadline)
                .await;
        }
        if connectivity_publish_claimed
            && !connectivity_probe_scheduled
            && !connectivity_publish_scheduled
        {
            // A claimed exact retry must never disappear silently if planning
            // produced no network operation.
            self.record_connectivity_publish_failure();
        }
        for publish in member_publishes {
            publish_succeeded &= cluster_network_pending_step_with_deadline(
                "member publish",
                CLUSTER_NETWORK_PENDING_MEMBER_PUBLISH_TIMEOUT,
                deadline,
                self.run_pending_member_publish_async(publish, runtime.clone()),
            )
            .await
                == Some(true);
        }
        for refresh in low_priority_node_status_refreshes {
            let outcome = cluster_network_pending_step_with_deadline(
                "low-priority node status refresh",
                CLUSTER_NETWORK_PENDING_NODE_STATUS_TIMEOUT,
                deadline,
                self.run_pending_node_status_refresh_async(refresh, runtime.clone()),
            )
            .await;
            match outcome {
                Some(outcome) => {
                    publish_succeeded &= outcome.succeeded;
                    presence_metadata_followup_needed |= outcome.metadata_followup_needed;
                }
                None => publish_succeeded = false,
            }
        }
        if publish_claimed {
            if publish_succeeded {
                self.record_publish_success();
                if presence_metadata_followup_needed {
                    self.defer_publish_retry(
                        Instant::now(),
                        CLUSTER_NETWORK_INITIAL_PRESENCE_METADATA_FOLLOWUP_DELAY,
                    );
                }
            } else {
                self.record_publish_failure();
            }
        }
        if probe_claimed {
            if probe_succeeded {
                self.record_probe_success();
            } else {
                self.record_probe_failure_at(Instant::now());
            }
        }
    }

    async fn run_pending_connectivity_publish_async(
        &mut self,
        pending: ClusterConnectivityPendingPublish,
        runtime: tokio::runtime::Handle,
        deadline: Option<Instant>,
    ) -> bool {
        let ClusterConnectivityPendingPublish {
            paths,
            source_node_id,
            state,
            records,
            input_generation,
            local_connectivity_snapshot,
        } = pending;
        let current_state = match sinomo_core::cluster::read_cluster_state_async(&runtime, &paths)
            .await
        {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon current topology read before connectivity publish failed: {error}"
                );
                self.record_connectivity_publish_failure();
                return false;
            }
        };
        if input_generation != self.connectivity_input_generation
            || !sinomo_core::cluster::cluster_connectivity_probe_topology_is_current(
                &paths,
                &state,
                &current_state,
            )
        {
            self.record_connectivity_topology_superseded();
            return true;
        }
        let read_paths = paths.clone();
        let read_source_node_id = source_node_id.clone();
        let current_records = match runtime
            .spawn_blocking(move || {
                sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                    &read_paths,
                    &read_source_node_id,
                )
            })
            .await
        {
            Ok(Ok(records)) => records,
            Ok(Err(error)) => {
                tracing::warn!(
                    "sinomo daemon cached connectivity read failed for {source_node_id}: {error}"
                );
                self.record_connectivity_publish_failure();
                return false;
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon cached connectivity read task failed for {source_node_id}: {error}"
                );
                self.record_connectivity_publish_failure();
                return false;
            }
        };
        let records = records
            .filter(|retained| retained.as_slice() == current_records.as_slice())
            .unwrap_or_else(|| Arc::new(current_records));
        let (records, committed_snapshot) = match sinomo_core::network::__connectivity_store::commit_local_cluster_connectivity_source_set_if_snapshot_async(
            &runtime,
            &paths,
            &current_state,
            &source_node_id,
            records.as_slice(),
            &local_connectivity_snapshot,
        )
        .await
        {
            Ok(sinomo_core::network::__connectivity_store::LocalClusterConnectivitySourceSetCommit::Committed {
                records,
                snapshot,
                content_changed: _,
            }) => (Arc::new(records), snapshot),
            Ok(sinomo_core::network::__connectivity_store::LocalClusterConnectivitySourceSetCommit::Superseded {
                records,
                snapshot,
            }) => {
                self.retain_superseding_connectivity_source_generation(
                    paths,
                    source_node_id,
                    current_state,
                    records,
                    snapshot,
                );
                return true;
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon generation-fenced connectivity publish commit failed: {error}"
                );
                self.record_connectivity_publish_failure();
                return false;
            }
        };
        // Retain before the first remote await. A timeout drops only this
        // attempt; the same durable generation remains the independent retry
        // owner and full-source reconciliation is idempotent. An empty set is
        // still a generation: it must delete stale source-owned KV records and
        // remain fenced against a concurrent topology input.
        self.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: source_node_id.clone(),
            state: current_state.clone(),
            records: Some(Arc::clone(&records)),
            input_generation,
            local_connectivity_snapshot: committed_snapshot,
        });
        let mut publish_host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::new(runtime.clone()),
            cluster_control_plane_timeouts(),
        );
        let published = cluster_network_pending_step_with_deadline(
            "connectivity publish",
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_PUBLISH_TIMEOUT,
            deadline,
            sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_async_with_state_and_host(
                &runtime,
                &paths,
                &source_node_id,
                records.as_ref().clone(),
                current_state,
                &mut publish_host,
            ),
        )
        .await;
        match published {
            Some(Ok(_)) => {
                self.finish_connectivity_convergence();
                true
            }
            Some(Err(error)) => {
                self.record_connectivity_publish_failure();
                tracing::warn!("sinomo daemon cached connectivity quorum publish failed: {error}");
                false
            }
            None => {
                self.record_connectivity_publish_failure();
                false
            }
        }
    }

    async fn run_pending_node_status_refresh_async(
        &mut self,
        pending: ClusterNodeStatusPendingRefresh,
        runtime: tokio::runtime::Handle,
    ) -> ClusterNodeStatusPendingOutcome {
        let label = pending.label;
        let task_runtime = runtime.clone();
        let mut task = OwnedDaemonTask::new(runtime.spawn(async move {
            refresh_daemon_node_status_in_kv_async_with_timeouts(
                &pending.paths,
                task_runtime,
                cluster_control_plane_timeouts(),
            )
            .await
        }));
        match task.join().await {
            Ok(Ok(published)) => {
                self.record_node_status_publish(&published);
                ClusterNodeStatusPendingOutcome {
                    succeeded: true,
                    metadata_followup_needed: published.network_info_deferred,
                }
            }
            Ok(Err(error)) => {
                tracing::warn!("sinomo daemon {label} publish failed: {error}");
                ClusterNodeStatusPendingOutcome::default()
            }
            Err(error) => {
                tracing::warn!("sinomo daemon {label} publish task failed: {error}");
                ClusterNodeStatusPendingOutcome::default()
            }
        }
    }

    async fn run_pending_member_publish_async(
        &mut self,
        pending: ClusterMemberPublishPendingWork,
        runtime: tokio::runtime::Handle,
    ) -> bool {
        let result = publish_local_members_to_quorum_if_needed_async(
            &pending.paths,
            &pending.state,
            pending.force_kv_snapshot_sync,
            runtime,
            cluster_control_plane_timeouts(),
        )
        .await
        .map(|_| ());
        match result {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!("sinomo daemon cluster member publish failed: {error}");
                false
            }
        }
    }

    fn record_publish_failure(&mut self) {
        self.cadence.record_publish_failure(Instant::now());
    }

    fn record_connectivity_publish_failure(&mut self) {
        self.cadence
            .record_connectivity_publish_failure(Instant::now());
    }

    fn record_connectivity_topology_superseded(&mut self) {
        self.retained_connectivity_publish = None;
        self.cadence.record_connectivity_topology_superseded();
    }

    fn retain_superseding_connectivity_source_generation(
        &mut self,
        paths: ClusterPaths,
        source_node_id: NodeId,
        state: ClusterBootstrapState,
        records: Vec<ClusterConnectivityRecord>,
        local_connectivity_snapshot: LocalClusterConnectivityStoreSnapshot,
    ) {
        self.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths,
            source_node_id,
            state,
            records: Some(Arc::new(records)),
            input_generation: self.connectivity_input_generation,
            local_connectivity_snapshot,
        });
        // This is newer durable output, not a failed observation. Give its
        // idempotent publication an immediate independent turn without probe
        // or failure backoff.
        self.cadence
            .retain_superseding_connectivity_source_generation(Instant::now());
    }

    fn record_connectivity_publish_success(&mut self) {
        let superseded = self.connectivity_publish_superseded();
        self.retained_connectivity_publish = None;
        self.cadence.record_connectivity_publish_success(superseded);
    }

    fn connectivity_publish_superseded(&self) -> bool {
        self
            .retained_connectivity_publish
            .as_ref()
            .map(|retained| {
                let output_superseded = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(
                    &retained.paths,
                )
                .map(|current_snapshot| {
                    current_snapshot != retained.local_connectivity_snapshot
                        || retained.records.as_ref().is_none_or(|records| {
                            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                                &retained.paths,
                                &retained.source_node_id,
                            )
                            .map(|current| current.as_slice() != records.as_slice())
                            .unwrap_or(true)
                        })
                })
                .unwrap_or(true);
                retained.input_generation != self.connectivity_input_generation
                    || output_superseded
                    || !cluster_connectivity_probe_inputs_still_current(retained)
            })
            .unwrap_or(false)
    }

    fn settle_connectivity_without_publication(&mut self) {
        self.finish_connectivity_convergence();
    }

    fn finish_connectivity_convergence(&mut self) {
        let superseded = self.connectivity_publish_superseded();
        self.retained_connectivity_publish = None;
        self.cadence.finish_connectivity_convergence(superseded);
    }

    async fn run_pending_connectivity_probe_async(
        &mut self,
        pending: ClusterConnectivityPendingProbe,
        runtime: tokio::runtime::Handle,
        deadline: Option<Instant>,
    ) -> ClusterConnectivityProbePendingOutcome {
        let ClusterConnectivityPendingProbe {
            paths,
            source_node_id,
            state,
            plan,
            input_generation,
            local_connectivity_snapshot,
        } = pending;
        let remaining_due = plan.remaining_due;
        let convergence_active = plan.convergence_active;
        // Only endpoint observation is cancelable as the probe phase. Once a
        // complete batch exists, its source-owned projection must cross the
        // local durable boundary before any quorum/import await can consume
        // the remaining maintenance budget.
        let Some(execution) = cluster_network_pending_step_with_deadline(
            "connectivity probe",
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_TIMEOUT,
            deadline,
            self.execute_connectivity_probe_plan_async(&state, plan),
        )
        .await
        else {
            return ClusterConnectivityProbePendingOutcome::ProbeFailed;
        };
        let newly_healthy_target_node_ids = execution.newly_healthy_target_node_ids.clone();
        let convergence_followup_needed =
            sinomo_core::cluster::cluster_connectivity_probe_result_needs_followup(
                remaining_due,
                convergence_active,
                execution.probed_degraded,
            );
        let current_state = match sinomo_core::cluster::read_cluster_state_async(&runtime, &paths)
            .await
        {
            Ok(state) => state,
            Err(error) => {
                self.connectivity_force_probe = true;
                tracing::warn!(
                    "sinomo daemon current topology read before connectivity commit failed: {error}"
                );
                return ClusterConnectivityProbePendingOutcome::ProbeFailed;
            }
        };
        if input_generation != self.connectivity_input_generation
            || !sinomo_core::cluster::cluster_connectivity_probe_topology_is_current(
                &paths,
                &state,
                &current_state,
            )
        {
            self.record_connectivity_topology_superseded();
            return ClusterConnectivityProbePendingOutcome::Superseded;
        }
        let records = merge_probe_records_with_unsuperseded_bootstrap_connectivity(
            &paths,
            &source_node_id,
            execution.records,
        );
        let (persisted_records, committed_snapshot, content_changed) = match sinomo_core::network::__connectivity_store::commit_local_cluster_connectivity_source_set_if_snapshot_async(
            &runtime,
            &paths,
            &current_state,
            &source_node_id,
            &records,
            &local_connectivity_snapshot,
        )
        .await
        {
            Ok(sinomo_core::network::__connectivity_store::LocalClusterConnectivitySourceSetCommit::Committed {
                records,
                snapshot,
                content_changed,
            }) => (Arc::new(records), snapshot, content_changed),
            Ok(sinomo_core::network::__connectivity_store::LocalClusterConnectivitySourceSetCommit::Superseded {
                ..
            }) => {
                // A concurrent source-owned observation won the durable fence.
                // Publishing that winner does not complete this discarded probe:
                // its speculative cache may say Healthy while the durable edge
                // is Unreachable. Reseed from durable observations and retry the
                // bounded probe before allowing convergence to finish.
                self.connectivity.clear();
                self.record_connectivity_topology_superseded();
                return ClusterConnectivityProbePendingOutcome::Superseded;
            }
            Err(error) => {
                self.connectivity_force_probe = true;
                tracing::warn!(
                    "sinomo daemon generation-fenced connectivity probe commit failed: {error}"
                );
                return ClusterConnectivityProbePendingOutcome::ProbeFailed;
            }
        };
        let retained_publish = ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: source_node_id.clone(),
            state: current_state.clone(),
            records: Some(Arc::clone(&persisted_records)),
            input_generation,
            local_connectivity_snapshot: committed_snapshot,
        };
        // Install the retry owner before the first cancelable remote await.
        self.retained_connectivity_publish = Some(retained_publish);
        self.connectivity_force_probe = false;
        if content_changed {
            self.idle_stats.incr_connectivity_record_sets_written();
        }
        spawn_newly_reachable_cluster_raft_member_catch_up(
            &runtime,
            paths.clone(),
            current_state.clone(),
            newly_healthy_target_node_ids,
        );

        let mut publish_host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::new(runtime.clone()),
            cluster_control_plane_timeouts(),
        );
        let published = cluster_network_pending_step_with_deadline(
            "connectivity publish",
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_PUBLISH_TIMEOUT,
            deadline,
            sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_async_with_state_and_host(
                &runtime,
                &paths,
                &source_node_id,
                persisted_records.as_ref().clone(),
                current_state.clone(),
                &mut publish_host,
            ),
        )
        .await;
        match published {
            Some(Ok(_)) => {
                if convergence_followup_needed {
                    self.connectivity_converged = false;
                    self.record_connectivity_publish_success();
                } else {
                    self.finish_connectivity_convergence();
                }
                // Peer import is a derived read-model optimization. It may use
                // only whatever phase budget remains; source publication has
                // already crossed both durable boundaries.
                let imported = cluster_network_pending_step_with_deadline(
                    "peer connectivity import",
                    CLUSTER_NETWORK_PENDING_CONNECTIVITY_TIMEOUT,
                    deadline,
                    import_peer_connectivity_records_from_reachable_endpoints_async(
                        &paths,
                        &current_state,
                        persisted_records.as_slice(),
                        runtime,
                    ),
                )
                .await
                .unwrap_or(0);
                if imported > 0 {
                    tracing::debug!(
                        "sinomo daemon imported {imported} peer connectivity record set(s)"
                    );
                }
                ClusterConnectivityProbePendingOutcome::Completed
            }
            Some(Err(error)) => {
                self.record_connectivity_publish_failure();
                tracing::warn!("sinomo daemon connectivity quorum publish failed: {error}");
                ClusterConnectivityProbePendingOutcome::PublicationDeferred
            }
            None => {
                self.record_connectivity_publish_failure();
                ClusterConnectivityProbePendingOutcome::PublicationDeferred
            }
        }
    }

    async fn execute_connectivity_probe_plan_async(
        &mut self,
        state: &ClusterBootstrapState,
        plan: ClusterConnectivityProbePlan,
    ) -> ClusterConnectivityProbeExecution {
        let probed = probe_connectivity_batch_async(
            state.clone(),
            plan.entries.clone(),
            plan.timestamp.clone(),
            plan.now,
            plan.startup_settle_active,
        )
        .await;
        let probed_degraded = probed
            .iter()
            .any(|(_, record)| record.state == ClusterConnectivityState::Degraded);
        let newly_healthy_target_node_ids =
            newly_healthy_connectivity_target_node_ids(&self.connectivity, &probed);
        let records = self.apply_connectivity_probe_plan(plan, probed);
        ClusterConnectivityProbeExecution {
            records,
            probed_degraded,
            newly_healthy_target_node_ids,
        }
    }

    fn select_connectivity_probe_plan(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> ClusterConnectivityProbePlan {
        let now = time::OffsetDateTime::now_utc();
        let timestamp = now
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string());
        let startup_settle_active = self
            .startup_settle_until
            .is_some_and(|deadline| Instant::now() < deadline);
        let convergence_active = !self.connectivity_converged;
        let force_probe = self.connectivity_force_probe;
        let local_node_role = self.effective_network_maintenance_role();
        let batch = sinomo_core::network::select_cluster_connectivity_probe_batch(
            paths,
            state,
            &mut self.connectivity,
            self.connectivity_cursor,
            &local_node_role,
            startup_settle_active,
            convergence_active,
            force_probe,
            now,
        );
        self.connectivity_cursor = batch.next_cursor;
        self.idle_stats
            .add_connectivity_probes_sent(batch.entries.len() as u64);

        ClusterConnectivityProbePlan {
            active_keys: batch.active_keys,
            entries: batch.entries,
            remaining_due: batch.remaining_due,
            convergence_active,
            force_probe,
            timestamp,
            now,
            startup_settle_active,
        }
    }

    fn apply_connectivity_probe_plan(
        &mut self,
        plan: ClusterConnectivityProbePlan,
        probed: Vec<(String, ClusterConnectivityRecord)>,
    ) -> Vec<ClusterConnectivityRecord> {
        sinomo_core::network::apply_cluster_connectivity_probe_results(
            &mut self.connectivity,
            &plan.active_keys,
            probed,
        )
    }
}

/// A reachability transition is the recovery event for a missed committed
/// fanout. It owns one target-keyed Raft catch-up attempt; there is no periodic
/// KV pull or retry sweep. Raft append rejection/backtracking remains the only
/// gap-recovery protocol and naturally falls back to an authoritative snapshot
/// when the leader has compacted the missing prefix.
fn spawn_newly_reachable_cluster_raft_member_catch_up(
    runtime: &tokio::runtime::Handle,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    target_node_ids: Vec<NodeId>,
) {
    if target_node_ids.is_empty() {
        return;
    }
    let worker_runtime = runtime.clone();
    runtime.spawn(async move {
        catch_up_newly_reachable_cluster_raft_members_async(
            paths,
            state,
            target_node_ids,
            worker_runtime,
        )
        .await;
    });
}

async fn catch_up_newly_reachable_cluster_raft_members_async(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    target_node_ids: Vec<NodeId>,
    runtime: tokio::runtime::Handle,
) {
    let target_node_ids = target_node_ids
        .into_iter()
        .filter(|target_node_id| {
            *target_node_id != state.local_node_id
                && state.members.iter().any(|member| {
                    member.node_id == *target_node_id
                        && member.state.is_authorized()
                        && member
                            .capabilities
                            .iter()
                            .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
                })
        })
        .collect::<BTreeSet<_>>();
    if target_node_ids.is_empty() {
        return;
    }

    let read_paths = paths.clone();
    let read_state = state.clone();
    let leader_inputs = runtime
        .spawn_blocking(move || {
            if !read_paths
                .kv_paths()
                .locked_raft_local_is_leader(&read_state)
                .map_err(sinomo_core::network::NetworkError::directory)?
            {
                return Ok(None);
            }
            sinomo_core::kv::read_cluster_kv_watch_events(&read_paths.kv_paths())
                .map(|read| Some(read.events))
                .map_err(sinomo_core::network::NetworkError::directory)
        })
        .await;
    let events = match leader_inputs {
        Ok(Ok(Some(events))) => Arc::new(events),
        Ok(Ok(None)) => return,
        Ok(Err(error)) => {
            tracing::debug!(
                target: "sinomo.cluster.raft_catch_up",
                %error,
                "reachable-peer Raft catch-up could not read leader state"
            );
            return;
        }
        Err(error) => {
            tracing::debug!(
                target: "sinomo.cluster.raft_catch_up",
                %error,
                "reachable-peer Raft catch-up leader-state task failed"
            );
            return;
        }
    };

    let timeouts = cluster_control_plane_timeouts();
    let fanout_options = timeouts.committed_fanout_options();
    let mut tasks = tokio::task::JoinSet::new();
    for target_node_id in target_node_ids {
        let paths = paths.clone();
        let state = state.clone();
        let events = events.clone();
        let runtime = runtime.clone();
        tasks.spawn(async move {
            let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
                DaemonClusterRpcMode::new(runtime),
                timeouts,
            );
            let outcome = sinomo_core::cluster::replicate_cluster_kv_to_node_async_with_host(
                &paths,
                &state,
                &target_node_id,
                events.as_slice(),
                timeouts,
                fanout_options.raft_append_timeout,
                &mut host,
            )
            .await?;
            if !outcome.response.accepted {
                return Ok::<_, sinomo_core::network::NetworkError>(None);
            }
            if outcome.watch_event_fanout_needed {
                for event_batch in
                    sinomo_core::kv::cluster_kv_watch_event_fanout_batches(events.as_slice())
                {
                    let request = ClusterKvWatchEventsRequest {
                        state: state.clone(),
                        leader_id: state.local_node_id.clone(),
                        events: event_batch.to_vec(),
                    };
                    let response = sinomo_core::cluster::ClusterKvCommittedFanoutAsyncHost::send_cluster_kv_watch_events_to_node_async(
                        &mut host,
                        &state,
                        &target_node_id,
                        &request,
                        fanout_options.watch_event_timeout,
                    )
                    .await;
                    if !response.is_ok_and(|response| response.accepted) {
                        break;
                    }
                }
            }
            let match_index = outcome.response.append.match_index;
            Ok(Some(sinomo_core::kv::ClusterKvRaftPeerProgress {
                peer_node_id: target_node_id,
                term: outcome.response.append.term,
                next_index: match_index.saturating_add(1),
                match_index,
                last_accepted_index: match_index,
                observed_at: OffsetDateTime::now_utc(),
            }))
        });
    }

    let mut progress = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(Some(hint))) => progress.push(hint),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => tracing::debug!(
                target: "sinomo.cluster.raft_catch_up",
                %error,
                "reachable-peer Raft catch-up did not complete"
            ),
            Err(error) => tracing::debug!(
                target: "sinomo.cluster.raft_catch_up",
                %error,
                "reachable-peer Raft catch-up task failed"
            ),
        }
    }
    if progress.is_empty() {
        return;
    }
    let kv_paths = paths.kv_paths();
    match runtime
        .spawn_blocking(move || {
            sinomo_core::kv::record_cluster_kv_raft_peer_progress_batch(&kv_paths, progress)
        })
        .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(
            target: "sinomo.cluster.raft_catch_up",
            %error,
            "reachable-peer Raft catch-up could not persist peer progress"
        ),
        Err(error) => tracing::warn!(
            target: "sinomo.cluster.raft_catch_up",
            %error,
            "reachable-peer Raft catch-up progress task failed"
        ),
    }
}

fn merge_probe_records_with_unsuperseded_bootstrap_connectivity(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    probe_records: Vec<ClusterConnectivityRecord>,
) -> Vec<ClusterConnectivityRecord> {
    let now = OffsetDateTime::now_utc();
    let existing_records =
        sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            paths,
            source_node_id,
        )
        .unwrap_or_default();
    merge_connectivity_probe_records_with_live_unprobed(
        source_node_id,
        probe_records,
        existing_records,
        now,
    )
}

fn cluster_connectivity_probe_inputs_still_current(
    retained: &ClusterConnectivityPendingPublish,
) -> bool {
    let Ok(current_state) = sinomo_core::cluster::read_cluster_state(&retained.paths) else {
        return false;
    };
    sinomo_core::cluster::cluster_connectivity_probe_topology_is_current(
        &retained.paths,
        &retained.state,
        &current_state,
    )
}

fn cluster_network_deadline_with_tail_reserve(
    deadline: Option<Instant>,
    reserve: Duration,
) -> Option<Instant> {
    let deadline = deadline?;
    deadline
        .checked_sub(reserve)
        .or_else(|| Instant::now().checked_sub(Duration::from_nanos(1)))
}

async fn cluster_network_pending_step_with_deadline<F, T>(
    label: &str,
    max_duration: Duration,
    deadline: Option<Instant>,
    future: F,
) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    let Some(deadline) = deadline else {
        return Some(future.await);
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        tracing::warn!("sinomo daemon cluster network {label} skipped: maintenance budget expired");
        return None;
    };
    if remaining == Duration::ZERO {
        tracing::warn!("sinomo daemon cluster network {label} skipped: maintenance budget expired");
        return None;
    }
    let timeout = remaining.min(max_duration);
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => Some(result),
        Err(_) => {
            tracing::warn!(
                "sinomo daemon cluster network {label} timed out after {}ms",
                timeout.as_millis()
            );
            None
        }
    }
}

async fn import_accepted_join_connectivity_from_future_async<F>(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    runtime: tokio::runtime::Handle,
    fetch: F,
) -> Result<usize, sinomo_core::network::NetworkError>
where
    F: std::future::Future<Output = Result<Vec<ClusterConnectivityRecord>, String>> + Send,
{
    tokio::time::timeout(DAEMON_JOIN_CONNECTIVITY_HANDOFF_TIMEOUT, async move {
        let peer_records =
            fetch
                .await
                .map_err(|detail| sinomo_core::network::NetworkError::Io {
                    operation: "cold-start join connectivity handoff".to_string(),
                    detail,
                })?;
        let groups = accepted_join_connectivity_record_groups(
            &state,
            peer_records,
            OffsetDateTime::now_utc(),
        )?;
        let imported = groups.values().map(Vec::len).sum();
        sinomo_core::network::write_local_cluster_connectivity_record_groups_async(
            &runtime, &paths, &groups,
        )
        .await?;
        Ok(imported)
    })
    .await
    .map_err(|_| sinomo_core::network::NetworkError::Io {
        operation: "cold-start join connectivity handoff".to_string(),
        detail: format!(
            "accepted introducer connectivity import timed out after {}ms",
            DAEMON_JOIN_CONNECTIVITY_HANDOFF_TIMEOUT.as_millis()
        ),
    })?
}

fn complete_accepted_join_connectivity_handoff_on_runtime_with_fetch<F>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    accepted_endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    fetch: F,
) -> usize
where
    F: std::future::Future<Output = Result<Vec<ClusterConnectivityRecord>, String>> + Send,
{
    let started = Instant::now();
    let accepted_source_node_id = accepted_endpoint.node_id.clone();
    let result = block_on_cluster_network_future_on_owner_runtime(
        runtime,
        Box::pin(import_accepted_join_connectivity_from_future_async(
            paths.clone(),
            state.clone(),
            runtime.clone(),
            fetch,
        )),
    );
    match result {
        Ok(imported) => {
            crate::cluster_join_trace(format_args!(
                "connectivity handoff source={accepted_source_node_id} imported={imported} in {:?}",
                started.elapsed()
            ));
            imported
        }
        Err(error) => {
            crate::cluster_join_trace(format_args!(
                "connectivity handoff source={accepted_source_node_id} deferred after {:?}: {error}",
                started.elapsed()
            ));
            tracing::debug!(
                introducer_node_id = %accepted_source_node_id,
                %error,
                "sinomo daemon deferred accepted introducer connectivity import to replacement maintenance"
            );
            0
        }
    }
}

fn complete_accepted_join_connectivity_handoff_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    accepted_endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
) -> usize {
    let fetch_paths = paths.clone();
    let fetch_state = state.clone();
    let fetch_endpoint = accepted_endpoint.clone();
    let fetch_runtime = runtime.clone();
    let fetch = async move {
        let (_, records) = fetch_peer_connectivity_records_from_endpoint_async(
            fetch_paths,
            fetch_state,
            fetch_endpoint,
            fetch_runtime,
            DAEMON_JOIN_CONNECTIVITY_HANDOFF_TIMEOUT,
            PeerConnectivityFetchScope::FullSnapshot,
        )
        .await;
        records
    };
    complete_accepted_join_connectivity_handoff_on_runtime_with_fetch(
        paths,
        state,
        accepted_endpoint,
        runtime,
        fetch,
    )
}

async fn import_peer_connectivity_records_from_reachable_endpoints_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    records: &[ClusterConnectivityRecord],
    runtime: tokio::runtime::Handle,
) -> usize {
    let timeouts = cluster_control_plane_timeouts();
    let import_records = peer_connectivity_import_records_for_reachable_peers(
        state,
        records,
        OffsetDateTime::now_utc(),
    );
    let mut visited = BTreeSet::new();
    let mut endpoints = Vec::with_capacity(import_records.len());
    for record in &import_records {
        let Some(endpoint) = peer_connectivity_import_endpoint_for_record(state, record) else {
            continue;
        };
        let key = endpoint.node_id.to_string();
        if !visited.insert(key) {
            continue;
        }
        endpoints.push(endpoint);
    }
    if endpoints.is_empty() {
        return 0;
    }

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        CLUSTER_CONNECTIVITY_IMPORT_CONCURRENCY,
    ));
    let mut handles = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            break;
        };
        let state = state.clone();
        let paths = paths.clone();
        let runtime = runtime.clone();
        let timeout = connectivity_import_timeout_for_endpoint(&endpoint, timeouts);
        handles.push(tokio::spawn(async move {
            let _permit = permit;

            fetch_peer_connectivity_records_from_endpoint_async(
                paths,
                state,
                endpoint,
                runtime,
                timeout,
                PeerConnectivityFetchScope::EndpointSource,
            )
            .await
        }));
    }

    let mut imported = 0_usize;
    for handle in handles {
        let (endpoint, peer_records) = match handle.await {
            Ok(result) => result,
            Err(error) => {
                tracing::debug!("sinomo daemon peer connectivity import task failed: {error}");
                continue;
            }
        };
        let peer_records = match peer_records {
            Ok(peer_records) => peer_records,
            Err(error) => {
                tracing::debug!(
                    "sinomo daemon peer connectivity import from {} failed: {error}",
                    endpoint.address
                );
                continue;
            }
        };
        // The peer RPC currently carries records but no source-set watermark.
        // Merge observations by key so a late older subset cannot delete keys
        // learned from a newer response. Exact source-set replacement remains
        // reserved for locally owned observations; imported omissions age out
        // through their existing TTL or topology pruning.
        match sinomo_core::network::write_local_cluster_connectivity_records_async(
            &runtime,
            paths,
            &endpoint.node_id,
            &peer_records,
        )
        .await
        {
            Ok(()) => imported = imported.saturating_add(1),
            Err(error) => tracing::debug!(
                "sinomo daemon peer connectivity import from {} write failed: {error}",
                endpoint.address
            ),
        }
    }
    imported
}

fn connectivity_import_timeout_for_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Duration {
    let timeout = match endpoint.transport {
        ClusterTransportKind::Iroh => timeouts.iroh_command_timeout,
        _ => timeouts.tcp_command_timeout,
    };
    timeout.min(CLUSTER_CONNECTIVITY_IMPORT_ENDPOINT_TIMEOUT)
}

async fn fetch_peer_connectivity_records_from_endpoint_async(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    endpoint: ClusterEndpointRecord,
    runtime: tokio::runtime::Handle,
    timeout: Duration,
    scope: PeerConnectivityFetchScope,
) -> (
    ClusterEndpointRecord,
    Result<Vec<ClusterConnectivityRecord>, String>,
) {
    let result = async {
        let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
            DaemonClusterRpcMode::new(runtime.clone()),
            cluster_control_plane_timeouts(),
        );
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &paths);
        let response = client
            .call_endpoint(
                &state,
                &endpoint,
                sinomo_core::network::ObjectRpcCall::encode_value(
                    sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                    sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY,
                    (),
                )
                .map_err(|error| error.to_string())?,
                sinomo_core::network::ClusterObjectRpcOptions::default()
                    .with_overall_timeout(timeout),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => return Err(error.to_string()),
        };
        let peer_records = match response.decode_body().await {
            Ok(records) => {
                filter_peer_connectivity_response_records(&state, &endpoint, records, scope)?
            }
            Err(error) => return Err(format!("decode failed: {error}")),
        };
        Ok(peer_records)
    }
    .await;
    (endpoint, result)
}

async fn probe_connectivity_batch_async(
    state: ClusterBootstrapState,
    entries: Vec<sinomo_core::network::ClusterConnectivityProbeBatchEntry>,
    timestamp: String,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
) -> Vec<(String, ClusterConnectivityRecord)> {
    let fetch_state = state.clone();
    probe_connectivity_batch_with_fetch_async(
        state,
        entries,
        timestamp,
        now,
        startup_settle_active,
        CLUSTER_HEALTH_PROBE_TIMEOUT,
        move |endpoint, timeout| {
            let state = fetch_state.clone();
            async move {
                fetch_daemon_status_from_endpoint_direct_async(&state, &endpoint, timeout).await
            }
        },
    )
    .await
}

async fn probe_connectivity_batch_with_fetch_async<F, Fut>(
    state: ClusterBootstrapState,
    entries: Vec<sinomo_core::network::ClusterConnectivityProbeBatchEntry>,
    timestamp: String,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
    endpoint_timeout: Duration,
    fetch_status: F,
) -> Vec<(String, ClusterConnectivityRecord)>
where
    F: Fn(ClusterEndpointRecord, Duration) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<crate::DaemonStatus, DaemonError>> + Send + 'static,
{
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        CLUSTER_CONNECTIVITY_PROBE_CONCURRENCY,
    ));
    let mut handles = Vec::with_capacity(entries.len());
    for entry in entries {
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            break;
        };
        let state = state.clone();
        let timestamp = timestamp.clone();
        let fetch_status = fetch_status.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let endpoint = entry.endpoint.clone();
            let address = endpoint.address.clone();
            let result = match tokio::time::timeout(
                endpoint_timeout,
                fetch_status(endpoint, endpoint_timeout),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(DaemonError::Network {
                    address,
                    detail: format!(
                        "connectivity probe timed out after {}ms",
                        endpoint_timeout.as_millis()
                    ),
                }),
            };
            let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            connectivity_record_for_daemon_status_probe(
                &state,
                entry,
                result,
                latency_ms,
                &timestamp,
                now,
                startup_settle_active,
            )
        }));
    }

    let mut probed = Vec::with_capacity(handles.len());
    for handle in handles {
        if let Ok(record) = handle.await {
            probed.push(record);
        }
    }
    probed
}

fn connectivity_record_for_daemon_status_probe(
    state: &ClusterBootstrapState,
    entry: sinomo_core::network::ClusterConnectivityProbeBatchEntry,
    result: Result<crate::DaemonStatus, DaemonError>,
    latency_ms: u64,
    timestamp: &str,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
) -> (String, ClusterConnectivityRecord) {
    let observation = match &result {
        Ok(status) if daemon_cluster_status_matches(state, &entry.endpoint, status) => {
            sinomo_core::network::ClusterConnectivityProbeObservation::Healthy
        }
        Ok(status) => {
            tracing::warn!(
                "sinomo daemon connectivity probe identity mismatch: endpoint {} expected node {}, responder reported cluster={:?} node={:?}",
                entry.endpoint.address,
                entry.endpoint.node_id,
                status.cluster.as_ref().map(|c| c.cluster_id.to_string()),
                status.cluster.as_ref().map(|c| c.node_id.to_string()),
            );
            if status.cluster.is_some() {
                sinomo_core::network::ClusterConnectivityProbeObservation::IdentityMismatch
            } else {
                sinomo_core::network::ClusterConnectivityProbeObservation::Failed
            }
        }
        Err(_) => sinomo_core::network::ClusterConnectivityProbeObservation::Failed,
    };
    let record = sinomo_core::network::cluster_connectivity_record_for_probe_observation(
        state,
        &entry,
        observation,
        latency_ms,
        timestamp,
        now,
        startup_settle_active,
    );
    (entry.key, record)
}

#[cfg(test)]
fn cluster_connectivity_probe_due(
    record: &sinomo_core::network::ClusterConnectivityRecord,
    now: time::OffsetDateTime,
    refresh_deadline: time::OffsetDateTime,
    local_node_role: &ClusterNodeRole,
) -> bool {
    sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
        record,
        now,
        refresh_deadline,
        local_node_role,
        false,
    )
}

async fn fetch_daemon_status_from_endpoint_direct_async(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<crate::DaemonStatus, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonObjectRpcAsyncHost::direct_only(&runtime);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
    let response = client
        .call_endpoint(
            state,
            endpoint,
            sinomo_core::network::ObjectRpcCall::encode_value(
                crate::DAEMON_STATUS_ROUTE,
                crate::DAEMON_RPC_CATEGORY,
                (),
            )?,
            // Status probing owns this semantic operation deadline; transport
            // phase timeouts remain separate inside sinomo-network.
            sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        )
        .await
        .map_err(DaemonError::from)?;
    response.decode_body().await.map_err(DaemonError::from)
}

/// Records which of the two heartbeat records `refresh_daemon_node_status_in_kv`
/// actually wrote to cluster KV, so callers can account real publishes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeStatusPublishOutcome {
    pub network_info_published: bool,
    pub node_status_published: bool,
    pub network_info_deferred: bool,
}

pub fn refresh_daemon_node_status_in_kv(
    paths: &DaemonPaths,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    refresh_daemon_node_status_in_kv_on_runtime(paths, None)
}

pub(crate) fn refresh_daemon_node_status_in_kv_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    crate::service_adapter::apply_component_status_contributors(paths, &mut status)
        .map_err(|detail| DaemonError::Parse { detail })?;
    ensure_daemon_status_cluster_on_runtime(paths, &mut status, runtime)?;
    let Some(cluster) = status.cluster.as_ref() else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let network_info = sinomo_core::network::build_cluster_network_info_record(
        &state,
        &cluster.node_id,
        time::OffsetDateTime::now_utc(),
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    );
    let record = cluster_node_status_from_daemon_status(
        &status,
        cluster,
        None,
        time::OffsetDateTime::now_utc(),
    );
    let Some(runtime) = runtime else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let mut publish_match_host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    let publish_matches = sinomo_core::cluster::cluster_node_presence_publish_matches_with_host(
        &cluster_paths,
        &network_info,
        &record,
        &mut publish_match_host,
    );
    if publish_matches.all_match() {
        return Ok(NodeStatusPublishOutcome::default());
    }
    // Publish each record independently so the slow-changing network-info record
    // is not rewritten on every node-status heartbeat. Liveness goes first: if a
    // bounded caller is interrupted after one quorum commit, the daemon is still
    // visible and the slower-changing network metadata can converge on retry.
    let mut outcome = NodeStatusPublishOutcome::default();
    if !publish_matches.node_status_matches {
        sync_cluster_node_status_to_kv_on_runtime(&cluster_paths, record, runtime)?;
        outcome.node_status_published = true;
    }
    if !publish_matches.network_info_matches {
        if outcome.node_status_published {
            outcome.network_info_deferred = true;
        } else if let Err(error) =
            sync_cluster_network_info_to_kv_on_runtime(&cluster_paths, network_info, runtime)
        {
            tracing::warn!("sinomo daemon network info publish failed: {error}");
        } else {
            outcome.network_info_published = true;
        }
    }
    Ok(outcome)
}

async fn refresh_daemon_node_status_in_kv_async_with_timeouts(
    paths: &DaemonPaths,
    runtime: tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    let operation_budget = timeouts.rpc_timeout;
    let read_paths = paths.clone();
    let read_path = read_paths.root.display().to_string();
    let status_result = {
        let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
            DaemonNodeStatusPublishPhase::DaemonStatusRead,
            operation_budget,
        );
        let result = tokio::task::spawn_blocking(move || read_daemon_status(&read_paths)).await;
        phase.complete();
        result
    };
    let mut status = status_result.map_err(|source| DaemonError::Io {
        path: read_path,
        detail: format!("daemon status read task failed: {source}"),
    })??;
    crate::service_adapter::apply_component_status_contributors(paths, &mut status)
        .map_err(|detail| DaemonError::Parse { detail })?;
    let ensure_result = {
        let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
            DaemonNodeStatusPublishPhase::ClusterStatusEnsure,
            operation_budget,
        );
        let result = ensure_daemon_status_cluster_async(paths, &mut status, &runtime).await;
        phase.complete();
        result
    };
    ensure_result?;
    let Some(cluster) = status.cluster.clone() else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state_paths = cluster_paths.clone();
    let state_path = state_paths.root.display().to_string();
    let state_result = {
        let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
            DaemonNodeStatusPublishPhase::ClusterStateRead,
            operation_budget,
        );
        let result = tokio::task::spawn_blocking(move || {
            sinomo_core::cluster::read_cluster_state_without_kv(&state_paths)
        })
        .await;
        phase.complete();
        result
    };
    let state = state_result
        .map_err(|source| DaemonError::Io {
            path: state_path,
            detail: format!("cluster state read task failed: {source}"),
        })?
        .map_err(DaemonError::from)?;
    let network_info = sinomo_core::network::build_cluster_network_info_record(
        &state,
        &cluster.node_id,
        time::OffsetDateTime::now_utc(),
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    );
    let record = cluster_node_status_from_daemon_status(
        &status,
        &cluster,
        None,
        time::OffsetDateTime::now_utc(),
    );
    let mut publish_match_host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::new(runtime.clone()),
        timeouts,
    );
    let publish_matches = {
        let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
            DaemonNodeStatusPublishPhase::PresenceMatch,
            operation_budget,
        );
        let result = sinomo_core::cluster::cluster_node_presence_publish_matches_async_with_host(
            &runtime,
            &cluster_paths,
            &network_info,
            &record,
            &mut publish_match_host,
        )
        .await;
        phase.complete();
        result
    };
    if publish_matches.all_match() {
        return Ok(NodeStatusPublishOutcome::default());
    }

    let mut outcome = NodeStatusPublishOutcome::default();
    if !publish_matches.node_status_matches {
        let publish_result = {
            let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
                DaemonNodeStatusPublishPhase::NodeStatusCommit,
                operation_budget,
            );
            let result = sync_cluster_node_status_to_kv_async_with_timeouts(
                &cluster_paths,
                record,
                runtime.clone(),
                timeouts,
            )
            .await;
            phase.complete();
            result
        };
        publish_result?;
        outcome.node_status_published = true;
    }
    if !publish_matches.network_info_matches {
        if outcome.node_status_published {
            outcome.network_info_deferred = true;
        } else {
            let publish_result = {
                let mut phase = DaemonNodeStatusPublishPhaseObservation::start(
                    DaemonNodeStatusPublishPhase::NetworkInfoCommit,
                    operation_budget,
                );
                let result = sync_cluster_network_info_to_kv_async_with_timeouts(
                    &cluster_paths,
                    network_info,
                    runtime,
                    timeouts,
                )
                .await;
                phase.complete();
                result
            };
            if let Err(error) = publish_result {
                tracing::warn!("sinomo daemon network info publish failed: {error}");
            } else {
                outcome.network_info_published = true;
            }
        }
    }
    Ok(outcome)
}

#[cfg(test)]
pub(crate) fn ensure_daemon_status_cluster(
    paths: &DaemonPaths,
    status: &mut crate::DaemonStatus,
) -> Result<(), DaemonError> {
    ensure_daemon_status_cluster_on_runtime(paths, status, None)
}

pub(crate) fn ensure_daemon_status_cluster_on_runtime(
    paths: &DaemonPaths,
    status: &mut crate::DaemonStatus,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<(), DaemonError> {
    if status.cluster.is_some() {
        return Ok(());
    }
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let mut state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    if sinomo_core::network::cluster_state_needs_join_registration(&state) {
        return Ok(());
    }
    let process = daemon_cluster_process_from_paths(data_dir, paths);
    let (main_options, shadow_options) =
        daemon_cluster_options_from_status(data_dir, paths, status, process);
    let _ = refresh_local_daemon_membership_on_runtime(
        &cluster_paths,
        &mut state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
        runtime,
    )?;
    sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)?;
    if let Some(cluster) = daemon_cluster_status(&state, process) {
        status.cluster = Some(cluster);
        write_daemon_status(paths, status)?;
    }
    Ok(())
}

pub(crate) async fn ensure_daemon_status_cluster_async(
    paths: &DaemonPaths,
    status: &mut crate::DaemonStatus,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    if status.cluster.is_some() {
        return Ok(());
    }
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let data_dir = data_dir.to_path_buf();
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(&data_dir));
    let state_path = cluster_paths.state.clone();
    let state_exists = tokio::task::spawn_blocking(move || state_path.exists())
        .await
        .map_err(|source| DaemonError::Io {
            path: cluster_paths.state.display().to_string(),
            detail: format!("cluster state existence task failed: {source}"),
        })?;
    if !state_exists {
        return Ok(());
    }

    let read_paths = cluster_paths.clone();
    let read_path = read_paths.root.display().to_string();
    let mut state = tokio::task::spawn_blocking(move || {
        sinomo_core::cluster::read_cluster_state_without_kv(&read_paths)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: read_path,
        detail: format!("cluster state read task failed: {source}"),
    })?
    .map_err(DaemonError::from)?;
    if sinomo_core::network::cluster_state_needs_join_registration(&state) {
        return Ok(());
    }

    let process = daemon_cluster_process_from_paths(&data_dir, paths);
    let (main_options, shadow_options) =
        daemon_cluster_options_from_status(&data_dir, paths, status, process);
    let _ = refresh_local_daemon_membership_async(
        runtime,
        &cluster_paths,
        &mut state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
    )
    .await?;

    let write_paths = cluster_paths.clone();
    let write_path = write_paths.root.display().to_string();
    let state_for_write = state.clone();
    tokio::task::spawn_blocking(move || {
        sinomo_core::cluster::write_cluster_state(&write_paths, &state_for_write)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: write_path,
        detail: format!("cluster state write task failed: {source}"),
    })?
    .map_err(DaemonError::from)?;
    if let Some(cluster) = daemon_cluster_status(&state, process) {
        status.cluster = Some(cluster);
        let status_paths = paths.clone();
        let status_for_write = status.clone();
        let status_path = status_paths.root.display().to_string();
        tokio::task::spawn_blocking(move || write_daemon_status(&status_paths, &status_for_write))
            .await
            .map_err(|source| DaemonError::Io {
                path: status_path,
                detail: format!("daemon status write task failed: {source}"),
            })??;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    sync_cluster_state_to_kv_on_runtime(paths, state, runtime.handle())
}

#[cfg(test)]
pub(crate) fn sync_cluster_state_to_kv_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    sinomo_core::cluster::sync_cluster_state_to_kv_with_host(
        paths,
        state,
        &mut DaemonClusterMemberMutationHost::new_on_runtime(runtime),
    )
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn read_local_cluster_state(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_core::cluster::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

#[cfg(test)]
fn read_local_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

async fn fetch_local_cluster_health_rows_from_member_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<Vec<ClusterHealthRow>>, DaemonError> {
    let request = sinomo_core::cluster::cluster_local_health_rpc_request()?;
    let Some(response) = call_member_object_rpc_target_async(
        paths,
        state,
        member,
        &request,
        sinomo_core::cluster::main_daemon_cluster_rpc_options(
            sinomo_core::network::ClusterObjectRpcOptions::default()
                .with_overall_timeout(CLUSTER_HEALTH_PEER_ROWS_TIMEOUT),
        ),
        runtime,
        "cluster health peer rows",
    )
    .await?
    else {
        return Ok(None);
    };
    match sinomo_core::cluster::decode_cluster_health_rpc_response(&response) {
        Ok(rows) => Ok(Some(rows)),
        Err(error) => {
            tracing::warn!(
                "sinomo daemon cluster health peer rows from {} failed to decode: {error}",
                member.node_id
            );
            Ok(None)
        }
    }
}

async fn fetch_cluster_health_rows_from_member_without_recursive_peers_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<Vec<ClusterHealthRow>>, DaemonError> {
    let request = sinomo_core::cluster::cluster_health_rpc_request_without_recursive_peer_views()?;
    let Some(response) = call_member_object_rpc_target_async(
        paths,
        state,
        member,
        &request,
        sinomo_core::cluster::main_daemon_cluster_rpc_options(
            sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        ),
        runtime,
        "cluster health peer view",
    )
    .await?
    else {
        return Ok(None);
    };
    match sinomo_core::cluster::decode_cluster_health_rpc_response(&response) {
        Ok(rows) => Ok(Some(rows)),
        Err(error) => {
            tracing::warn!(
                "sinomo daemon cluster health peer view from {} failed to decode: {error}",
                member.node_id
            );
            Ok(None)
        }
    }
}

pub(crate) struct DaemonClusterStatusProbe<'a> {
    paths: &'a ClusterPaths,
    runtime: tokio::runtime::Handle,
}

impl<'a> DaemonClusterStatusProbe<'a> {
    pub(crate) fn new(paths: &'a ClusterPaths, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            paths,
            runtime: runtime.clone(),
        }
    }
}

impl ClusterStatusAsyncProbe for DaemonClusterStatusProbe<'_> {
    fn probe_member_status_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        member: &'a ClusterMemberRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
        Box::pin(async move {
            probe_member_daemon_status_async(self.paths, state, member, &self.runtime)
                .await
                .map_err(|error| daemon_host_network_error("cluster health probe", error))
        })
    }

    fn probe_member_statuses_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
        statuses: &'a mut BTreeMap<NodeId, Option<ClusterNodeStatusRecord>>,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let state = state.clone();
        let members = members.to_vec();
        Box::pin(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for member in members {
                let paths = paths.clone();
                let runtime = runtime.clone();
                let state = state.clone();
                let node_id = member.node_id.clone();
                tasks.spawn(async move {
                    let status =
                        probe_member_daemon_status_async(&paths, &state, &member, &runtime)
                            .await
                            .map_err(|error| {
                                daemon_host_network_error("cluster health probe", error)
                            })?;
                    Ok::<_, sinomo_core::network::NetworkError>((node_id, status))
                });
            }

            while let Some(result) = tasks.join_next().await {
                let (node_id, status) =
                    result.map_err(|source| sinomo_core::network::NetworkError::Io {
                        operation: "cluster health probe task".to_string(),
                        detail: source.to_string(),
                    })??;
                statuses.insert(node_id, status);
            }
            Ok(())
        })
    }

    fn peer_health_rows_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        member: &'a ClusterMemberRecord,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
        Box::pin(async move {
            fetch_cluster_health_rows_from_member_without_recursive_peers_async(
                self.paths,
                state,
                member,
                timeout,
                &self.runtime,
            )
            .await
            .map_err(|error| daemon_host_network_error("cluster health peer view", error))
        })
    }
}

async fn probe_member_daemon_status_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ClusterNodeStatusRecord>, DaemonError> {
    if cluster_member_is_local_process(state, member)
        && let Some(status) =
            probe_local_member_daemon_status_async(paths, state, member, runtime).await?
    {
        return Ok(Some(status));
    }
    let direct_endpoints = member_daemon_status_direct_probe_endpoints(paths, state, member);
    let routed_first =
        member_daemon_status_should_probe_routed_first(paths, state, member, &direct_endpoints);
    if routed_first
        && let Some(status) =
            probe_member_routed_ping_status_async(paths, state, member, runtime).await?
    {
        return Ok(Some(status));
    }
    if let Some(status) =
        probe_member_direct_daemon_status_async(state, member, &direct_endpoints).await?
    {
        return Ok(Some(status));
    }
    if !routed_first
        && let Some(status) =
            probe_member_routed_ping_status_async(paths, state, member, runtime).await?
    {
        return Ok(Some(status));
    }
    Ok(None)
}

async fn probe_member_direct_daemon_status_async(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    endpoints: &[ClusterEndpointRecord],
) -> Result<Option<ClusterNodeStatusRecord>, DaemonError> {
    for endpoint in endpoints
        .iter()
        .take(CLUSTER_HEALTH_DIRECT_STATUS_MAX_ENDPOINTS)
    {
        let status = match fetch_daemon_status_from_endpoint_direct_async(
            state,
            endpoint,
            CLUSTER_HEALTH_DIRECT_STATUS_TIMEOUT,
        )
        .await
        {
            Ok(status) => status,
            Err(error) => {
                tracing::debug!(
                    "sinomo daemon cluster health direct status probe of {} via {} failed: {error}",
                    member.node_id,
                    endpoint.address
                );
                continue;
            }
        };
        let Some(cluster) = status.cluster.as_ref() else {
            continue;
        };
        if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
            tracing::warn!(
                "sinomo daemon cluster health direct status identity mismatch: endpoint {} expected node {}, responder reported cluster={:?} node={:?}",
                endpoint.address,
                member.node_id,
                status
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.cluster_id.to_string()),
                status
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.node_id.to_string()),
            );
            continue;
        }
        let record = cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        );
        if cluster_member_expected_daemon_process(member)
            .is_some_and(|process| record.process != process)
        {
            continue;
        }
        return Ok(Some(record));
    }
    Ok(None)
}

fn member_daemon_status_direct_probe_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Vec<ClusterEndpointRecord> {
    sinomo_core::network::ClusterEndpointSelector::new(paths, state)
        .select(sinomo_core::network::ClusterEndpointSelectionPurpose::MemberDaemonStatus)
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id == member.node_id
                && endpoint.privacy != sinomo_core::ClusterEndpointPrivacy::LocalOnly
                && sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        })
        .collect()
}

fn member_daemon_status_should_probe_routed_first(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    direct_endpoints: &[ClusterEndpointRecord],
) -> bool {
    let selected_endpoints = direct_endpoints
        .iter()
        .take(CLUSTER_HEALTH_DIRECT_STATUS_MAX_ENDPOINTS)
        .collect::<Vec<_>>();
    if selected_endpoints.is_empty() {
        return false;
    }
    let graph = sinomo_core::network::ClusterConnectivityGraph::read_for_local(paths, state);
    let Some(route) =
        sinomo_core::network::cluster_route_next_hop(&graph, &state.local_node_id, &member.node_id)
    else {
        return false;
    };
    // A known multi-hop path is stronger evidence than the absence of a local
    // observation for the target's advertised socket. Prefer that route
    // immediately instead of spending each endpoint timeout rediscovering the
    // network segmentation. A direct route still wins unless all of its
    // selected endpoints are explicitly unreachable.
    if route.next_hop != member.node_id {
        return true;
    }
    selected_endpoints.iter().all(|endpoint| {
        graph.state_for_endpoint(endpoint)
            == Some(&sinomo_core::network::ClusterConnectivityState::Unreachable)
    })
}

async fn probe_member_routed_ping_status_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ClusterNodeStatusRecord>, DaemonError> {
    let request = sinomo_core::cluster::cluster_network_ping_rpc_request(
        format!("cluster-health-{}", member.node_id),
        0,
    )?;
    let Some(response) = call_member_object_rpc_target_async(
        paths,
        state,
        member,
        &request,
        sinomo_core::network::ClusterObjectRpcOptions::default()
            .with_overall_timeout(CLUSTER_HEALTH_ROUTED_PING_TIMEOUT),
        runtime,
        "cluster health routed ping probe",
    )
    .await?
    else {
        return Ok(None);
    };
    let ping = match sinomo_core::cluster::decode_cluster_network_ping_rpc_response(&response) {
        Ok(ping) => ping,
        Err(error) => {
            tracing::warn!(
                "sinomo daemon cluster health routed ping from {} failed to decode: {error}",
                member.node_id
            );
            return Ok(None);
        }
    };
    if ping.responder_node_id != member.node_id {
        tracing::warn!(
            "sinomo daemon cluster health routed ping of {} got responder {}",
            member.node_id,
            ping.responder_node_id
        );
        return Ok(None);
    }
    let ping_observed_at = ping.observed_at;
    if let Some(rows) =
        fetch_local_cluster_health_rows_from_member_async(paths, state, member, runtime).await?
        && let Some(status) = member_status_from_peer_local_health_rows(member, rows)
    {
        return Ok(Some(status));
    }
    let expected_process = cluster_member_expected_daemon_process(member).map(str::to_string);
    let paths_for_statuses = paths.clone();
    let member_for_statuses = member.clone();
    let stored = tokio::task::spawn_blocking(move || {
        sinomo_core::cluster::read_cluster_node_statuses_from_kv(&paths_for_statuses)
            .map(|statuses| {
                statuses.into_iter().find(|status| {
                    status.node_id == member_for_statuses.node_id
                        && expected_process
                            .as_deref()
                            .map(|process| status.process == process)
                            .unwrap_or(true)
                })
            })
            .map_err(DaemonError::from)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: format!("cluster health status fallback task failed: {source}"),
    })??;
    Ok(Some(member_status_for_verified_routed_ping(
        state,
        member,
        stored,
        ping_observed_at,
    )))
}

fn member_status_for_verified_routed_ping(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    stored: Option<ClusterNodeStatusRecord>,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    let mut status = stored.unwrap_or_else(|| {
        sinomo_core::cluster::unknown_cluster_node_status(state, member, observed_at)
    });
    status.availability = ClusterNodeAvailability::Online;
    status.observed_at = observed_at;
    status
}

fn member_status_from_peer_local_health_rows(
    member: &ClusterMemberRecord,
    rows: Vec<ClusterHealthRow>,
) -> Option<ClusterNodeStatusRecord> {
    let expected_process = cluster_member_expected_daemon_process(member);
    rows.into_iter().find_map(|row| {
        let process_matches = expected_process
            .map(|process| row.status.process == process)
            .unwrap_or(true);
        if row.availability != ClusterNodeAvailability::Online
            || !row.live_probe_observed
            || row.status.node_id != member.node_id
            || !process_matches
        {
            return None;
        }
        let mut status = row.status;
        status.availability = ClusterNodeAvailability::Online;
        Some(status)
    })
}

async fn call_member_object_rpc_target_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    request: &sinomo_core::network::ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    operation: &str,
) -> Result<Option<sinomo_core::network::ObjectRpcResponse>, DaemonError> {
    let mut host = DaemonObjectRpcAsyncHost::new(runtime, true).with_cluster_paths(paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, paths);
    match client
        .call_target(
            state,
            &member.node_id,
            sinomo_core::network::ObjectRpcCall::value(request.clone()),
            options,
        )
        .await
    {
        Ok(result) => match result.into_materialized_value_response(usize::MAX).await {
            Ok(response) => Ok(Some(response)),
            Err(error) => {
                tracing::debug!(
                    "sinomo daemon {operation} of {} returned a non-value response: {error}",
                    member.node_id
                );
                Ok(None)
            }
        },
        Err(error) => {
            tracing::debug!(
                "sinomo daemon {operation} of {} failed: {error}",
                member.node_id
            );
            Ok(None)
        }
    }
}

async fn probe_local_member_daemon_status_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ClusterNodeStatusRecord>, DaemonError> {
    let Some(data_dir) = paths.root.parent() else {
        return Ok(None);
    };
    let target_paths = if member.node_id == state.local_node_id {
        DaemonPaths::with_root(data_dir.join("daemon"))
    } else if member.node_id == shadow_daemon_node_id(&state.local_node_id) {
        DaemonPaths::with_root(data_dir.join("shadow-daemon"))
    } else {
        return Ok(None);
    };
    let status = match crate::probe_daemon_status_async(
        &target_paths,
        CLUSTER_HEALTH_PROBE_TIMEOUT,
        runtime,
    )
    .await
    {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            return Ok(None);
        }
    };
    let Some(cluster) = status.cluster.as_ref() else {
        return Ok(None);
    };
    if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
        return Ok(None);
    }
    let status =
        cluster_node_status_from_daemon_status(&status, cluster, None, OffsetDateTime::now_utc());
    if cluster_member_expected_daemon_process(member)
        .is_some_and(|process| status.process != process)
    {
        return Ok(None);
    }
    Ok(Some(status))
}

pub(crate) fn prepare_local_cluster_state(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let (main_options, shadow_options) =
        current_daemon_cluster_options(data_dir).unwrap_or_else(|| {
            (
                DaemonRunOptions::default(),
                DaemonRunOptions::shadow_default(),
            )
        });
    let _ = refresh_local_daemon_membership(
        paths,
        state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
    )?;
    let _ = refresh_local_network_module_endpoints(state, OffsetDateTime::now_utc())?;
    Ok(())
}

fn fold_local_shadow_network_module_endpoint_node_id(
    state: &ClusterBootstrapState,
    endpoint: &mut ClusterEndpointRecord,
) {
    if endpoint.node_id == shadow_daemon_node_id(&state.local_node_id) {
        endpoint.node_id = state.local_node_id.clone();
    }
}

fn local_network_module_endpoints() -> Result<Vec<ClusterEndpointRecord>, DaemonError> {
    let Some(modules) = crate::registry::daemon_network_modules() else {
        return Ok(Vec::new());
    };
    modules
        .registry
        .local_endpoints()
        .map_err(DaemonError::from)
}

fn refresh_local_network_module_endpoints(
    state: &mut ClusterBootstrapState,
    timestamp: OffsetDateTime,
) -> Result<bool, DaemonError> {
    apply_network_module_endpoints_to_state(state, local_network_module_endpoints()?, timestamp)
}

fn apply_network_module_endpoints_to_state(
    state: &mut ClusterBootstrapState,
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> Result<bool, DaemonError> {
    let mut changed = false;
    for mut endpoint in endpoints {
        fold_local_shadow_network_module_endpoint_node_id(state, &mut endpoint);
        let Some(member) = state
            .members
            .iter_mut()
            .find(|member| member.node_id == endpoint.node_id)
        else {
            continue;
        };
        changed |= sinomo_core::network::upsert_member_endpoint(member, endpoint);
        changed |= sinomo_core::network::cleanup_member_endpoint_duplicates(member);
    }
    if changed {
        state.updated_at = timestamp;
    }
    Ok(changed)
}

#[cfg(test)]
fn initialize_local_cluster(
    paths: &ClusterPaths,
    cluster_id: Option<ClusterId>,
    node_id: Option<NodeId>,
    name: String,
    now: OffsetDateTime,
) -> Result<ClusterBootstrapState, DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    initialize_local_cluster_on_runtime(
        paths,
        cluster_id,
        node_id,
        name.clone(),
        name,
        ClusterNodeRole::default(),
        now,
        runtime.handle(),
    )
}

/// Initializes a fresh single-node cluster on this node: writes bootstrap
/// state, claims raft leadership for the local node, and seeds cluster KV.
/// Mirrors the CLI's `cluster init` orchestration with daemon-side hosts.
pub(crate) fn initialize_local_cluster_on_runtime(
    paths: &ClusterPaths,
    cluster_id: Option<ClusterId>,
    node_id: Option<NodeId>,
    cluster_name: String,
    node_display_name: String,
    node_role: ClusterNodeRole,
    now: OffsetDateTime,
    _runtime: &tokio::runtime::Handle,
) -> Result<ClusterBootstrapState, DaemonError> {
    paths.ensure()?;
    if paths.state.exists() {
        return Err(DaemonError::Parse {
            detail: format!(
                "cluster is already initialized at {}; use `sinomo cluster status`",
                paths.state.display()
            ),
        });
    }

    let local_node_id = node_id.unwrap_or_else(NodeId::generate);
    let cluster_id = cluster_id.unwrap_or_else(ClusterId::generate);
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        cluster_name,
        local_node_id,
        node_display_name,
        main_identity.public_key.clone(),
        now,
    );
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == state.local_node_id)
    {
        sinomo_core::cluster::apply_cluster_node_role(member, node_role, now);
    }
    let (main_options, shadow_options) =
        current_daemon_cluster_options(data_dir).unwrap_or_else(|| {
            (
                DaemonRunOptions::default(),
                DaemonRunOptions::shadow_default(),
            )
        });
    let _ =
        refresh_local_daemon_membership(paths, &mut state, &main_options, &shadow_options, now)?;
    sinomo_core::cluster::write_cluster_state(paths, &state)?;
    daemon_locked_set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(paths, &state)?;
    Ok(state)
}

// ===== Daemon-mediated cluster control (`/cluster/control/*`) =====
//
// These execute the cluster operations the CLI used to drive in-process, but
// inside the daemon with its own transports and hosts. Handlers are blocking
// (their drivers bridge onto the owner runtime), so services call them through
// `spawn_blocking`.

/// Default TTL for daemon-issued cluster invites.
const DAEMON_CLUSTER_CONTROL_INVITE_TTL_SECONDS: i64 = 24 * 60 * 60;
/// End-to-end per-introducer budget for a daemon-mediated join attempt. A join
/// can legitimately include authority forwarding and a quorum commit, so its
/// semantic deadline must leave room beyond a single transport no-progress
/// interval and the normal membership RPC budget.
const DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Iroh joins have the same semantic work and must not inherit a test-cluster
/// wall-clock target as their production deadline.
const DAEMON_CLUSTER_CONTROL_IROH_JOIN_TIMEOUT: Duration = DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT;
/// Network phases retain their independent transport/no-progress limit;
/// `timeout` is the semantic end-to-end budget for this attempt.
fn daemon_cluster_control_rpc_options(
    timeout: Duration,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    // Routed control calls must preserve time for endpoint fallback. The
    // default five-second connect health limit can otherwise consume an
    // entire member-publish budget on one stale private address before a
    // reachable WireGuard, public, or Iroh endpoint is tried. Once connected,
    // quorum work may legitimately be quiet while the target commits and
    // durably replicates, so keep the longer control-plane progress limit.
    let health = sinomo_core::network::NetworkHealthPolicy::new(
        sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
        sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT,
    );
    sinomo_core::cluster::main_daemon_cluster_rpc_options(
        sinomo_core::network::ClusterObjectRpcOptions::new(health.into())
            .with_overall_timeout(timeout),
    )
}

/// Hostname-derived default display name for the local node.
fn daemon_cluster_control_node_display_name(fallback: &str) -> String {
    fn clean(hostname: &str) -> Option<String> {
        let hostname = hostname.trim().trim_end_matches('.');
        if hostname.is_empty()
            || hostname.eq_ignore_ascii_case("localhost")
            || hostname.eq_ignore_ascii_case("localhost.localdomain")
            || hostname.eq_ignore_ascii_case("(none)")
            || hostname.chars().any(char::is_whitespace)
        {
            return None;
        }
        Some(hostname.to_string())
    }
    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key)
            && let Some(hostname) = clean(&value)
        {
            return hostname;
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|hostname| clean(&hostname))
        .unwrap_or_else(|| fallback.to_string())
}

/// Reads cluster state and refreshes the local daemon membership records,
/// mirroring the CLI's pre-mutation state preparation.
fn daemon_cluster_control_state(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_core::cluster::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

/// `/cluster/control/init`: initialize a fresh single-node cluster.
pub(crate) fn handle_daemon_cluster_control_init_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlInitRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterControlInitResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    let cluster_name = request.cluster_name.trim().to_string();
    if cluster_name.is_empty() {
        return Err(DaemonError::Parse {
            detail: "cluster name must not be empty".to_string(),
        });
    }
    let node_display_name = request
        .node_display_name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| cluster_name.clone());
    let state = initialize_local_cluster_on_runtime(
        &paths,
        None,
        None,
        cluster_name,
        node_display_name,
        request.node_role.unwrap_or_default(),
        OffsetDateTime::now_utc(),
        runtime,
    )?;
    // Wake identity-bound workers only so they can observe the mismatch and
    // retire. The replacement owns maintenance for the new state.
    crate::control::request_daemon_cluster_maintenance(
        crate::control::DaemonClusterMaintenanceReason::LifecycleWake,
    );
    Ok(ClusterControlInitResponse {
        cluster_id: state.cluster_id,
        node_id: state.local_node_id,
    })
}

/// `/cluster/control/join`: join an existing cluster from an invite.
///
/// Handles the common paths — a fresh node and a retry over stale pending
/// join state. `// TODO(stage-G2):` port the CLI's single-node migration path
/// (`cluster_join_single_node_migration_with_paths`: plan + KV migration
/// record + storage quarantine + config import) so a node that is already in
/// a settled cluster can join another one through the daemon; until then that
/// case returns an error directing the operator to the CLI.
pub(crate) fn handle_daemon_cluster_control_join_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlJoinRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterControlJoinResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    paths.ensure()?;
    let requested_node_display_name = request
        .node_display_name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let requested_node_role = request.node_role.unwrap_or_default();
    let invite = sinomo_core::cluster::decode_cluster_join_invite(
        sinomo_core::cluster::extract_invite_from_join_command(request.invite.trim()),
    )
    .map_err(DaemonError::from)?;
    let timestamp = OffsetDateTime::now_utc();
    if sinomo_core::cluster::invite_has_embedded_signature(&invite) {
        sinomo_core::cluster::verify_cluster_join_invite_signature(&invite)?;
    }
    if invite.expires_at != OffsetDateTime::UNIX_EPOCH && invite.expires_at <= timestamp {
        return Err(DaemonError::Parse {
            detail: format!("cluster invite expired at {}", invite.expires_at),
        });
    }

    let mut preserved_node_id = None;
    if paths.state.exists() {
        let existing = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
        if sinomo_core::network::cluster_state_needs_join_registration(&existing) {
            // Stale pending join: retry with a fresh one-shot attempt, keeping
            // the previously generated node id.
            preserved_node_id = Some(existing.local_node_id);
            match std::fs::remove_file(&paths.state) {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(DaemonError::Io {
                        path: paths.state.display().to_string(),
                        detail: format!(
                            "failed to remove stale pending cluster join state: {source}"
                        ),
                    });
                }
            }
        } else {
            // Settled in another cluster: single-node migration (stage G2) —
            // freeze and quarantine the source, join the target with the same
            // node identity, then import the node-local configs.
            return handle_daemon_cluster_control_join_single_node_migration_on_runtime(
                daemon_paths,
                &paths,
                &invite,
                existing,
                requested_node_display_name,
                requested_node_role,
                timestamp,
                runtime,
            );
        }
    }

    daemon_cluster_control_join_fresh_on_runtime(
        daemon_paths,
        &paths,
        &invite,
        preserved_node_id,
        requested_node_display_name,
        requested_node_role,
        timestamp,
        runtime,
    )
}

/// The fresh-join body shared by plain joins and the join step of a
/// single-node migration: prepares pending state, reconciles with the
/// invite's introducers, and persists the accepted state.
fn daemon_cluster_control_join_fresh_on_runtime(
    daemon_paths: &DaemonPaths,
    paths: &ClusterPaths,
    invite: &sinomo_core::ClusterJoinInvite,
    preserved_node_id: Option<NodeId>,
    requested_node_display_name: Option<String>,
    requested_node_role: ClusterNodeRole,
    timestamp: OffsetDateTime,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterControlJoinResponse, DaemonError> {
    let local_node_id = preserved_node_id.unwrap_or_else(NodeId::generate);
    let node_display_name = requested_node_display_name
        .unwrap_or_else(|| daemon_cluster_control_node_display_name("sinomo-local"));
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    let mut state = sinomo_core::cluster::prepare_pending_cluster_join_state(
        invite.cluster_id.clone(),
        invite
            .cluster_name
            .clone()
            .unwrap_or_else(|| node_display_name.clone()),
        local_node_id,
        node_display_name,
        main_identity.public_key,
        shadow_identity.public_key,
        timestamp,
    );
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == state.local_node_id)
    {
        sinomo_core::cluster::apply_cluster_node_role(member, requested_node_role, timestamp);
    }
    let (main_options, shadow_options) =
        current_daemon_cluster_options(data_dir).unwrap_or_else(|| {
            (
                DaemonRunOptions::default(),
                DaemonRunOptions::shadow_default(),
            )
        });
    let _ = refresh_local_daemon_membership(
        paths,
        &mut state,
        &main_options,
        &shadow_options,
        timestamp,
    )?;

    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    host.transport_mut()
        .set_join_rpc_context(daemon_paths.clone(), invite);
    let reconciliation = sinomo_core::cluster::reconcile_cluster_join_with_introducers_with_host(
        &mut state,
        invite,
        DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT,
        DAEMON_CLUSTER_CONTROL_IROH_JOIN_TIMEOUT,
        Some(paths),
        &mut host,
    )?;
    if let Some(snapshot) = reconciliation.kv_snapshot {
        sinomo_core::kv::write_cluster_kv_snapshot_if_changed(&paths.kv_paths(), &snapshot)
            .map_err(DaemonError::from)?;
    }
    if sinomo_core::network::cluster_state_needs_join_registration(&state) {
        return Err(DaemonError::ClusterReadOnly {
            detail:
                "cluster join was not accepted by any introducer; local cluster state was not changed"
                    .to_string(),
        });
    }
    sinomo_core::cluster::write_cluster_state(paths, &state)?;
    // Joining replaces the daemon's authenticated cluster principal. Do not
    // let the bounded connectivity handoff reuse a `node_local` or pending
    // identity cached while the state file was being replaced.
    sinomo_core::network::invalidate_cached_daemon_signing_identity(&SinomoDataDir::new(data_dir))?;
    if let Some(accepted_endpoint) = reconciliation.accepted_endpoint.as_ref() {
        // The command handler returns into an identity-changing restart. Import
        // the introducer's already-observed one-hop graph before that handoff;
        // otherwise a detached coalesced publisher can be retired with the old
        // daemon and the replacement starts with only its own introducer edge.
        // This fetch is independently bounded and intentionally non-fatal.
        complete_accepted_join_connectivity_handoff_on_runtime(
            paths,
            &state,
            accepted_endpoint,
            runtime,
        );
    }
    // Wake identity-bound workers so they retire against their captured
    // startup identity instead of adopting this newly installed state.
    crate::control::request_daemon_cluster_maintenance(
        crate::control::DaemonClusterMaintenanceReason::LifecycleWake,
    );
    Ok(ClusterControlJoinResponse {
        cluster_id: state.cluster_id,
        node_id: state.local_node_id,
        accepted_endpoint: reconciliation
            .accepted_endpoint
            .map(|endpoint| endpoint.address),
        migration: None,
    })
}

/// `/cluster/control/join` for a node already settled in a source cluster:
/// the daemon-side port of the CLI's single-node migration (stage G2).
///
/// Freezes the source (migration record into source KV, storage quarantined
/// under `<cluster>/migrations/<id>/source`), joins the target cluster with
/// the SAME node identity, then imports the node's `/config/nodes/<node>/…`
/// keys from the quarantined snapshot. A failed target join rolls the
/// quarantine back so the node stays in its source cluster.
fn handle_daemon_cluster_control_join_single_node_migration_on_runtime(
    daemon_paths: &DaemonPaths,
    paths: &ClusterPaths,
    invite: &sinomo_core::ClusterJoinInvite,
    mut source: ClusterBootstrapState,
    requested_node_display_name: Option<String>,
    requested_node_role: ClusterNodeRole,
    timestamp: OffsetDateTime,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterControlJoinResponse, DaemonError> {
    // Serialize against the maintenance tick for the whole freeze -> join ->
    // import window, like the leave handler: the tick must not commit into
    // storage being moved aside (this handler runs on a blocking service
    // thread, never on the async workers).
    let _storage_guard = Arc::clone(&*DAEMON_CLUSTER_STORAGE_MUTATION_LOCK).blocking_lock_owned();
    let target_node_display_name = requested_node_display_name.or_else(|| {
        let source_name = source.local_node_display_name.trim();
        (!source_name.is_empty()).then(|| source_name.to_string())
    });
    let mut source_host = DaemonClusterMemberMutationHost::new_on_runtime(runtime);
    let report = sinomo_core::cluster::plan_cluster_migration_with_host(
        paths,
        &mut source,
        invite,
        sinomo_core::ClusterMigrationMode::SingleNode,
        timestamp,
        &mut source_host,
    )?;
    if report.migration.source_cluster_id != source.cluster_id {
        return Err(DaemonError::Parse {
            detail: "source cluster changed while planning migration; run cluster join again"
                .to_string(),
        });
    }

    let mut migration = report.migration;
    sinomo_core::cluster::write_cluster_migration_record_to_source_kv_with_host(
        paths,
        &mut source,
        &migration,
        &mut source_host,
    )?;
    let component_quarantine = crate::service_adapter::component_migration_contributions(
        daemon_paths,
        &migration.source_cluster_id,
    )
    .map_err(|detail| DaemonError::Parse { detail })?;
    let quarantine = sinomo_core::cluster::quarantine_cluster_storage_for_migration(
        paths,
        &migration,
        &component_quarantine.moves,
    )?;
    migration.phase = sinomo_core::ClusterMigrationPhase::SourceFrozen;
    migration.updated_at = OffsetDateTime::now_utc();
    migration
        .evidence_refs
        .push(format!("source-quarantine:{}", quarantine.root.display()));
    migration
        .evidence_refs
        .extend(component_quarantine.evidence_refs);
    sinomo_core::cluster::write_local_cluster_migration_record(&quarantine.root, &migration)?;

    match daemon_cluster_control_join_fresh_on_runtime(
        daemon_paths,
        paths,
        invite,
        Some(migration.target_node_id.clone()),
        target_node_display_name,
        requested_node_role,
        timestamp,
        runtime,
    ) {
        Ok(mut response) => {
            migration.target_cluster_id = response.cluster_id.clone();
            migration.target_node_id = response.node_id.clone();
            let mut target_state = daemon_cluster_control_state(paths)?;
            migration.target_cluster_name = Some(target_state.cluster_name.clone());
            let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
            let kv_import =
                sinomo_core::cluster::import_node_local_configs_from_source_kv_with_host(
                    paths,
                    &mut target_state,
                    &quarantine.root,
                    &migration,
                    cluster_kv_txn_quorum_options(true),
                    &mut host,
                )?;
            migration.phase = sinomo_core::ClusterMigrationPhase::Finalized;
            migration.updated_at = OffsetDateTime::now_utc();
            migration
                .evidence_refs
                .push("source-kv-policy:target-authoritative".to_string());
            migration.evidence_refs.push(format!(
                "source-kv-import-report:{}",
                sinomo_core::cluster::cluster_migration_kv_import_report_path(&quarantine.root)
                    .display()
            ));
            migration.detail = Some(format!(
                "single-node migration finalized; target cluster KV is authoritative; source cluster storage is quarantined locally; imported {} node-local config keys, deferred {}, skipped {} source cluster config keys; DFS shared import is deferred",
                kv_import.imported_node_local_config_keys.len(),
                kv_import.deferred_node_local_config_keys.len(),
                kv_import.skipped_cluster_level_config_count,
            ));
            sinomo_core::cluster::write_local_cluster_migration_record(
                &quarantine.root,
                &migration,
            )?;
            crate::append_operational_event_best_effort(
                daemon_paths,
                crate::OperationalSeverity::Info,
                "daemon-cluster-control",
                "cluster.migration.finalized",
                format!(
                    "single-node migration {} finalized: {} -> {}",
                    migration.migration_id,
                    migration.source_cluster_id,
                    migration.target_cluster_id
                ),
                None,
                serde_json::json!({
                    "migration_id": migration.migration_id.to_string(),
                    "source_cluster_id": migration.source_cluster_id.to_string(),
                    "target_cluster_id": migration.target_cluster_id.to_string(),
                    "quarantine_root": quarantine.root.display().to_string(),
                }),
            );
            response.migration = Some(sinomo_core::cluster::ClusterControlJoinMigrationSummary {
                migration_id: migration.migration_id.clone(),
                source_cluster_id: migration.source_cluster_id.clone(),
                quarantine_root: quarantine.root.display().to_string(),
                imported_node_local_config_keys: kv_import.imported_node_local_config_keys.len(),
                deferred_node_local_config_keys: kv_import.deferred_node_local_config_keys.len(),
            });
            Ok(response)
        }
        Err(error) => {
            migration.phase = sinomo_core::ClusterMigrationPhase::RolledBack;
            migration.updated_at = OffsetDateTime::now_utc();
            migration.detail = Some(format!(
                "target join failed and source cluster storage was restored: {error}"
            ));
            let _ = sinomo_core::cluster::write_local_cluster_migration_record(
                &quarantine.root,
                &migration,
            );
            sinomo_core::cluster::rollback_cluster_storage_quarantine(&quarantine)?;
            Err(error)
        }
    }
}

/// Daemon-side default invite introducers: the daemon's own advertised
/// addresses, currently advertised network-module endpoints, and
/// remote-capable main-daemon member endpoints. Ported from the CLI's
/// `default_invite_introducers`.
fn daemon_default_invite_introducers(
    daemon_paths: &DaemonPaths,
    _paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut introducers = Vec::new();
    if let Ok(status) = read_daemon_status(daemon_paths) {
        for endpoint in crate::invite_introducers_from_daemon_status(&state.local_node_id, &status)
        {
            sinomo_core::cluster::push_unique_introducer(&mut introducers, endpoint);
        }
    }
    for mut endpoint in local_network_module_endpoints().unwrap_or_default() {
        fold_local_shadow_network_module_endpoint_node_id(state, &mut endpoint);
        sinomo_core::cluster::push_unique_introducer(&mut introducers, endpoint);
    }
    for endpoint in state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "daemon.main.run")
        })
        .flat_map(|member| member.endpoints.iter().cloned())
    {
        sinomo_core::cluster::push_unique_introducer(&mut introducers, endpoint);
    }
    sinomo_core::cluster::sort_default_invite_introducers(&mut introducers);
    sinomo_core::cluster::compact_default_invite_introducers(&mut introducers);
    introducers
}

/// Quorum-commit host for invite publication, backed by the daemon's member
/// mutation driver.
struct DaemonClusterInvitePublishHost {
    inner: DaemonClusterMemberMutationHost,
}

impl sinomo_core::cluster::ClusterInvitePublishHost for DaemonClusterInvitePublishHost {
    type Error = DaemonError;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>, Self::Error> {
        sinomo_core::cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut self.inner,
            paths,
            state,
        )
        .map_err(Into::into)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, Self::Error> {
        sinomo_core::cluster::ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.inner,
            paths,
            state,
            request,
        )
        .map_err(Into::into)
    }
}

/// `/cluster/control/invite`: create, sign, and publish a join invite.
pub(crate) fn handle_daemon_cluster_control_invite_with_authority_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlInviteRequest,
    runtime: &tokio::runtime::Handle,
    remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<ClusterControlInviteResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    paths.ensure()?;
    let state = daemon_cluster_control_state(&paths)?;
    if !sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Err(DaemonError::ClusterReadOnly {
            detail: "local node is not a voter; create the invite on a voter node or restore voter connectivity".to_string(),
        });
    }
    let ttl_seconds = request
        .ttl_seconds
        .unwrap_or(DAEMON_CLUSTER_CONTROL_INVITE_TTL_SECONDS);
    if ttl_seconds <= 0 {
        return Err(DaemonError::Parse {
            detail: "invite ttl must be greater than zero seconds".to_string(),
        });
    }
    let now = OffsetDateTime::now_utc();
    let introducers = if request.introducers.is_empty() {
        daemon_default_invite_introducers(daemon_paths, &paths, &state)
    } else {
        request
            .introducers
            .iter()
            .map(|address| {
                sinomo_core::cluster::invite_introducer_endpoint_from_state(&state, address.clone())
            })
            .collect()
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let issuer_identity = load_or_create_main_node_identity(data_dir)?;
    let invite = sinomo_core::cluster::build_cluster_join_invite(
        &state,
        &issuer_identity,
        introducers,
        ttl_seconds,
        now,
    )?;
    let uri = sinomo_core::cluster::encode_cluster_join_invite(&invite)?;
    let mut host = DaemonClusterInvitePublishHost {
        inner: DaemonClusterMemberMutationHost::new_on_runtime(runtime)
            .with_remote_control_authority(remote_authority),
    };
    sinomo_core::cluster::publish_cluster_invitation_to_quorum_with_host(
        &paths, &state, &invite, &mut host,
    )?;
    Ok(ClusterControlInviteResponse {
        invite: uri,
        cluster_id: Some(invite.cluster_id.clone()),
        expires_at: Some(invite.expires_at),
    })
}

/// `/cluster/control/rename`: rename a member (the local node by default).
pub(crate) fn handle_daemon_cluster_control_rename_with_authority_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlRenameRequest,
    runtime: &tokio::runtime::Handle,
    remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<ClusterControlRenameResponse, DaemonError> {
    // Every pre-refactor entry point (the CLI parser) required a non-empty
    // display name; keep that contract at the daemon boundary.
    if request.display_name.trim().is_empty() {
        return Err(DaemonError::Parse {
            detail: "cluster rename requires a non-empty display name".to_string(),
        });
    }
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    let state = daemon_cluster_control_state(&paths)?;
    let node_id = request
        .node_id
        .unwrap_or_else(|| state.local_node_id.clone());
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime)
        .with_remote_control_authority(remote_authority);
    let update = sinomo_core::cluster::update_cluster_node_name_with_host(
        &paths,
        state,
        node_id,
        request.display_name,
        OffsetDateTime::now_utc(),
        &mut host,
    )?;
    if update.changed {
        crate::control::request_daemon_cluster_maintenance(
            crate::control::DaemonClusterMaintenanceReason::Rename,
        );
    }
    Ok(ClusterControlRenameResponse {
        node_id: update.node_id,
        previous_name: update.previous_name,
        name: update.name,
        changed: update.changed,
    })
}

/// `/cluster/control/role`: change a member's node role.
pub(crate) fn handle_daemon_cluster_control_role_with_authority_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlRoleRequest,
    runtime: &tokio::runtime::Handle,
    remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<ClusterControlRoleResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    let state = daemon_cluster_control_state(&paths)?;
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime)
        .with_remote_control_authority(remote_authority);
    // Older single-node app clusters can have a valid local member in
    // state.sobj but no corresponding materialized KV row. Repair that
    // authority gap before applying the typed role update so an app upgrade
    // can opt the existing node into the mobile maintenance policy.
    let local_member_missing_from_kv = request.node_id == state.local_node_id
        && !sinomo_core::cluster::read_cluster_members_from_kv(&paths)?
            .iter()
            .any(|member| member.node_id == request.node_id);
    if local_member_missing_from_kv {
        sinomo_core::cluster::sync_cluster_state_to_kv_with_host(&paths, &state, &mut host)?;
    }
    let update = sinomo_core::cluster::update_cluster_node_role_with_host(
        &paths,
        state,
        request.node_id,
        request.role,
        OffsetDateTime::now_utc(),
        &mut host,
    )?;
    if update.changed {
        crate::control::request_daemon_cluster_maintenance(
            crate::control::DaemonClusterMaintenanceReason::RoleChanged,
        );
    }
    Ok(ClusterControlRoleResponse {
        node_id: update.node_id,
        previous_role: update.previous_role,
        role: update.role,
        changed: update.changed,
    })
}

/// `/cluster/control/state-authority`: change committed Raft membership.
pub(crate) fn handle_daemon_cluster_control_state_authority_on_runtime(
    daemon_paths: &DaemonPaths,
    request: sinomo_core::cluster::ClusterControlStateAuthorityRequest,
    runtime: &tokio::runtime::Handle,
    remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<sinomo_core::cluster::ClusterControlStateAuthorityResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    let state = daemon_cluster_control_state(&paths)?;
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime)
        .with_remote_control_authority(remote_authority);
    let update = sinomo_core::cluster::update_cluster_state_authority_with_host(
        &paths,
        state,
        request.node_id,
        request.role,
        OffsetDateTime::now_utc(),
        &mut host,
    )?;
    if update.changed {
        crate::control::request_daemon_cluster_maintenance(
            crate::control::DaemonClusterMaintenanceReason::RoleChanged,
        );
    }
    Ok(update)
}

/// `/cluster/control/remove`: remove members by node id.
pub(crate) fn handle_daemon_cluster_control_remove_with_authority_on_runtime(
    daemon_paths: &DaemonPaths,
    request: ClusterControlRemoveRequest,
    runtime: &tokio::runtime::Handle,
    remote_authority: Option<RemoteClusterControlAuthorityFence>,
) -> Result<ClusterControlRemoveResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    let state = daemon_cluster_control_state(&paths)?;
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime)
        .with_remote_control_authority(remote_authority);
    let removal = sinomo_core::cluster::remove_cluster_members_with_host(
        &paths,
        state,
        request.node_ids,
        &mut host,
    )?;
    Ok(ClusterControlRemoveResponse {
        removed: removal
            .removed
            .into_iter()
            .map(|member| ClusterControlRemovedMember {
                node_id: member.node_id,
                node_role: member.node_role,
                state: member.state,
            })
            .collect(),
    })
}

/// `/cluster/control/leave`: quarantine the current cluster storage and
/// re-initialize this node as a fresh single-node cluster.
pub(crate) fn handle_daemon_cluster_control_leave_on_runtime(
    daemon_paths: &DaemonPaths,
    _request: ClusterControlLeaveRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterControlLeaveResponse, DaemonError> {
    let paths = cluster_paths_from_daemon_paths(daemon_paths)?;
    paths.ensure()?;
    let existing = match sinomo_core::cluster::read_cluster_state_without_kv(&paths) {
        Ok(state) => Some(state),
        Err(sinomo_core::network::NetworkError::ClusterOrphan) => None,
        Err(error) => return Err(error.into()),
    };
    // Leaving an initialized cluster is a cluster-level decision: establish a
    // fresh current-term quorum barrier before moving any live state aside.
    // Orphan recovery (artifacts without state.sobj) remains a local cleanup
    // path because there is no cluster identity against which to prove quorum.
    if let Some(mut consensus_state) = existing.clone() {
        let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime);
        sinomo_core::cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut host,
            &paths,
            &mut consensus_state,
        )?;
    }
    let leave_id = Id::generate();
    let previous_cluster_id = existing.as_ref().map(|state| state.cluster_id.clone());
    let local_node_id = existing
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(NodeId::generate);
    let name = existing
        .as_ref()
        .map(|state| state.local_node_display_name.trim())
        .filter(|name| !name.is_empty())
        .or_else(|| {
            existing
                .as_ref()
                .map(|state| state.cluster_name.trim())
                .filter(|name| !name.is_empty())
        })
        .map(str::to_string)
        .unwrap_or_else(|| daemon_cluster_control_node_display_name("sinomo-local"));
    let node_role = existing
        .as_ref()
        .and_then(|state| {
            state
                .members
                .iter()
                .find(|member| member.node_id == state.local_node_id)
        })
        .map(|member| member.node_role.clone())
        .unwrap_or_default();

    // Hold the storage-mutation lock across quarantine + re-init so the
    // maintenance tick cannot commit into files being moved aside (this
    // handler runs on a blocking service thread, never on the async workers).
    let _storage_guard = Arc::clone(&*DAEMON_CLUSTER_STORAGE_MUTATION_LOCK).blocking_lock_owned();
    let quarantine =
        if existing.is_some() || sinomo_core::cluster::cluster_storage_has_live_artifacts(&paths) {
            Some(sinomo_core::cluster::quarantine_cluster_storage_for_leave(
                &paths, &leave_id,
            )?)
        } else {
            None
        };

    match initialize_local_cluster_on_runtime(
        &paths,
        Some(ClusterId::generate()),
        Some(local_node_id),
        name.clone(),
        name,
        node_role,
        OffsetDateTime::now_utc(),
        runtime,
    ) {
        Ok(state) => {
            // Leave installs a new process identity. Wake old-identity workers
            // so they retire; the response-triggered replacement owns the new
            // identity's maintenance.
            crate::control::request_daemon_cluster_maintenance(
                crate::control::DaemonClusterMaintenanceReason::LifecycleWake,
            );
            Ok(ClusterControlLeaveResponse {
                previous_cluster_id,
                new_cluster_id: state.cluster_id,
                node_id: state.local_node_id,
            })
        }
        Err(error) => {
            if let Some(quarantine) = quarantine {
                sinomo_core::cluster::rollback_cluster_storage_quarantine(&quarantine)?;
            }
            Err(error)
        }
    }
}

#[cfg(test)]
fn update_cluster_node_role(
    paths: &ClusterPaths,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<sinomo_core::cluster::ClusterNodeRoleUpdate, DaemonError> {
    let state = read_local_cluster_state(paths)?;
    let runtime = daemon_cluster_test_runtime();
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
    sinomo_core::cluster::update_cluster_node_role_with_host(
        paths, state, node_id, role, now, &mut host,
    )
    .map_err(Into::into)
}

pub(crate) struct DaemonClusterMemberMutationHost {
    runtime: tokio::runtime::Handle,
    remote_control_authority: Option<RemoteClusterControlAuthorityFence>,
    driver: sinomo_core::cluster::ClusterMemberMutationDriver<
        DaemonClusterControlPlaneTransport,
        DaemonClusterStateSyncMembershipObserver,
    >,
}

impl DaemonClusterMemberMutationHost {
    pub(crate) fn new_on_runtime(runtime: &tokio::runtime::Handle) -> Self {
        Self::new_on_runtime_with_timeouts(runtime, cluster_control_plane_timeouts())
    }

    fn new_on_runtime_with_timeouts(
        runtime: &tokio::runtime::Handle,
        timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
    ) -> Self {
        let runtime = runtime.clone();
        Self {
            runtime: runtime.clone(),
            remote_control_authority: None,
            driver: sinomo_core::cluster::ClusterMemberMutationDriver::new(
                daemon_cluster_control_plane_driver_with_mode_and_timeouts(
                    DaemonClusterRpcMode::new(runtime.clone()),
                    timeouts,
                ),
                DaemonClusterStateSyncMembershipObserver {
                    runtime: runtime.clone(),
                },
            ),
        }
    }

    pub(crate) fn with_remote_control_authority(
        mut self,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> Self {
        self.remote_control_authority = authority;
        self
    }
}

#[derive(Clone, Debug)]
struct DaemonClusterStateSyncMembershipObserver {
    runtime: tokio::runtime::Handle,
}

impl sinomo_core::cluster::ClusterLocalDaemonMembershipObserver
    for DaemonClusterStateSyncMembershipObserver
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
        sinomo_core::network::NetworkError,
    > {
        let data_dir =
            paths
                .root
                .parent()
                .ok_or_else(|| sinomo_core::network::NetworkError::Io {
                    operation: "cluster state membership sync".to_string(),
                    detail: format!(
                        "cluster paths have no data directory parent: {}",
                        paths.root.display()
                    ),
                })?;
        let Some((main_options, shadow_options)) = current_daemon_cluster_options(data_dir) else {
            return Ok(None);
        };
        let timestamp = OffsetDateTime::now_utc();
        let runtime_for_future = self.runtime.clone();
        let observation = block_on_daemon_cluster_future_on_owner_runtime(
            &self.runtime,
            Box::pin(local_daemon_membership_observation_async(
                data_dir,
                state,
                &main_options,
                &shadow_options,
                timestamp,
                &runtime_for_future,
            )),
        );
        observation
            .map(Some)
            .map_err(|error| daemon_host_network_error("cluster state membership sync", error))
    }
}

impl sinomo_core::cluster::ClusterLocalDaemonMembershipAsyncObserver
    for DaemonClusterStateSyncMembershipObserver
{
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation>,
    > {
        let data_dir = match paths.root.parent() {
            Some(data_dir) => data_dir.to_path_buf(),
            None => {
                let detail = format!(
                    "cluster paths have no data directory parent: {}",
                    paths.root.display()
                );
                return Box::pin(async move {
                    Err(sinomo_core::network::NetworkError::Io {
                        operation: "cluster state membership sync".to_string(),
                        detail,
                    })
                });
            }
        };
        let state = state.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let options_data_dir = data_dir.clone();
            let Some((main_options, shadow_options)) = runtime
                .spawn_blocking(move || current_daemon_cluster_options(&options_data_dir))
                .await
                .map_err(|source| sinomo_core::network::NetworkError::Io {
                    operation: "cluster state membership sync".to_string(),
                    detail: format!("daemon cluster options task failed: {source}"),
                })?
            else {
                return Ok(None);
            };
            local_daemon_membership_observation_async(
                &data_dir,
                &state,
                &main_options,
                &shadow_options,
                OffsetDateTime::now_utc(),
                &runtime,
            )
            .await
            .map(Some)
            .map_err(|error| daemon_host_network_error("cluster state membership sync", error))
        })
    }
}

impl sinomo_core::cluster::ClusterMemberMutationHost for DaemonClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), sinomo_core::network::NetworkError> {
        sinomo_core::cluster::ClusterMemberMutationHost::prepare_cluster_state_for_kv_sync(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>, sinomo_core::network::NetworkError>
    {
        sinomo_core::cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, sinomo_core::network::NetworkError> {
        let request = match self.remote_control_authority.as_ref() {
            Some(authority) => authority.fence_request(request)?,
            None => request,
        };
        let result =
            sinomo_core::cluster::ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
                &mut self.driver,
                paths,
                state,
                request,
            )?;
        if let Some(authority) = self.remote_control_authority.as_mut() {
            authority.confirm_commit_and_advance(&result)?;
        }
        Ok(result)
    }

    fn prove_cluster_raft_member_caught_up(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
        required_log_index: u64,
        now: OffsetDateTime,
    ) -> Result<sinomo_core::kv::ClusterKvRaftCatchUpProof, sinomo_core::network::NetworkError>
    {
        sinomo_core::cluster::ClusterMemberMutationHost::prove_cluster_raft_member_caught_up(
            &mut self.driver,
            paths,
            state,
            node_id,
            required_log_index,
            now,
        )
    }
}

async fn commit_cluster_kv_request_with_quorum_async_on_transport(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: sinomo_core::kv::KvTxnRequest,
    allow_forwarding: bool,
    transport: &mut DaemonClusterControlPlaneTransport,
) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, sinomo_core::network::NetworkError> {
    let runtime = transport.rpc_mode.runtime().clone();
    let options = transport.cluster_kv_txn_quorum_options(allow_forwarding);
    let previous_paths = transport.active_cluster_paths.replace(paths.clone());
    let result = sinomo_core::cluster::commit_cluster_kv_txn_with_quorum_response_async_with_host(
        &runtime, paths, state, request, options, transport,
    )
    .await;
    transport.active_cluster_paths = previous_paths;
    result
}

async fn commit_cluster_kv_txn_with_quorum_async_on_transport(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: sinomo_core::kv::KvTxnRequest,
    allow_forwarding: bool,
    transport: &mut DaemonClusterControlPlaneTransport,
) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
    commit_cluster_kv_request_with_quorum_async_on_transport(
        paths,
        state,
        request,
        allow_forwarding,
        transport,
    )
    .await
    .map(|result| result.snapshot)
}

#[cfg(not(test))]
fn daemon_control_transport_env_cluster_paths() -> Option<ClusterPaths> {
    cluster_paths_from_env()
}

#[cfg(test)]
fn daemon_control_transport_env_cluster_paths() -> Option<ClusterPaths> {
    None
}

#[cfg(test)]
fn cluster_kv_object_rpc_request(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
}

#[cfg(test)]
fn cluster_raft_append_object_rpc_request(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcRequest {
    // Retrying identical AppendEntries is safe under Raft.
    cluster_kv_object_rpc_request(payload).with_response_retry_safe()
}

#[cfg(test)]
fn cluster_raft_vote_object_rpc_request(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcRequest {
    // Retrying an identical RequestVote is safe under Raft, but voting stays
    // on the ordinary short route budget to preserve recovery-oriented route
    // ordering and bounded election latency.
    cluster_kv_object_rpc_request(payload).with_response_retry_safe()
}

/// Decode the response body inside the original Raft RPC budget. Routed calls
/// return as soon as ownership of a streamed body transfers; without this
/// outer boundary, a peer can send a prompt response head and then stall the
/// consensus task indefinitely during body decoding.
async fn decode_cluster_raft_rpc_before<T, F>(
    operation: &'static str,
    timeout: Duration,
    call: F,
) -> Result<T, sinomo_core::network::NetworkError>
where
    T: serde::de::DeserializeOwned + Send + 'static,
    F: std::future::Future<
            Output = Result<
                sinomo_core::network::ObjectRpcResult,
                sinomo_core::network::NetworkError,
            >,
        >,
{
    match tokio::time::timeout(timeout, async move {
        let response = call.await?;
        response.decode_body().await
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(sinomo_core::network::NetworkError::timeout(
            operation,
            sinomo_core::network::NetworkTimeoutPhase::Overall,
            timeout,
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        )),
    }
}

async fn send_cluster_raft_append_routed_async(
    paths: &ClusterPaths,
    target_node_id: &NodeId,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, sinomo_core::network::NetworkError> {
    let owned_paths = paths.clone();
    let target = target_node_id.clone();
    let request = Arc::new(request.clone());
    let sent = Arc::clone(&request);
    let runtime = runtime.clone();
    sinomo_core::cluster::send_shared_cluster_raft_append(
        paths,
        target_node_id,
        request,
        timeout,
        move |remaining| async move {
            send_cluster_raft_append_routed_unshared_async(
                &owned_paths,
                &target,
                &sent,
                remaining,
                &runtime,
            )
            .await
        },
    )
    .await
}

async fn send_cluster_raft_append_routed_unshared_async(
    paths: &ClusterPaths,
    target_node_id: &NodeId,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, sinomo_core::network::NetworkError> {
    let mut call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
        sinomo_core::kv::CLUSTER_RAFT_APPEND_ROUTE,
        sinomo_core::kv::CLUSTER_RAFT_RPC_CATEGORY,
        request,
    )?;
    call.request = call.request.with_response_retry_safe();
    let mut host = DaemonObjectRpcAsyncHost::new(runtime, true).with_cluster_paths(paths);
    // Every Raft message carries the committed/projected membership fence that
    // selected this target. Consensus traffic must not consult the dynamic KV
    // directory whose contents it is responsible for replicating.
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host,
        paths,
        &request.state,
        sinomo_core::network::cluster_directory_for(paths),
    )?;
    decode_cluster_raft_rpc_before(
        "cluster Raft append RPC",
        timeout,
        client.call_target(
            &request.state,
            target_node_id,
            call,
            daemon_main_routed_rpc_options(timeout),
        ),
    )
    .await
}

fn send_cluster_raft_append_routed_on_runtime(
    paths: &ClusterPaths,
    target_node_id: &NodeId,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, sinomo_core::network::NetworkError> {
    block_on_cluster_network_future_on_owner_runtime(
        runtime,
        Box::pin(send_cluster_raft_append_routed_async(
            paths,
            target_node_id,
            request,
            timeout,
            runtime,
        )),
    )
}

async fn send_cluster_raft_vote_routed_async(
    paths: &ClusterPaths,
    target_node_id: &NodeId,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftVoteResponse, sinomo_core::network::NetworkError> {
    let mut call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
        sinomo_core::kv::CLUSTER_RAFT_VOTE_ROUTE,
        sinomo_core::kv::CLUSTER_RAFT_RPC_CATEGORY,
        request,
    )?;
    call.request = call.request.with_response_retry_safe();
    let mut host = DaemonObjectRpcAsyncHost::new(runtime, true).with_cluster_paths(paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host,
        paths,
        &request.state,
        sinomo_core::network::cluster_directory_for(paths),
    )?;
    decode_cluster_raft_rpc_before(
        "cluster Raft vote RPC",
        timeout,
        client.call_target(
            &request.state,
            target_node_id,
            call,
            daemon_main_routed_rpc_options(timeout),
        ),
    )
    .await
}

fn send_cluster_raft_vote_routed_on_runtime(
    paths: &ClusterPaths,
    target_node_id: &NodeId,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftVoteResponse, sinomo_core::network::NetworkError> {
    block_on_cluster_network_future_on_owner_runtime(
        runtime,
        Box::pin(send_cluster_raft_vote_routed_async(
            paths,
            target_node_id,
            request,
            timeout,
            runtime,
        )),
    )
}

fn daemon_main_routed_rpc_options(
    timeout: Duration,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    sinomo_core::cluster::main_daemon_cluster_rpc_options(
        sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
    )
}

impl sinomo_core::cluster::ClusterMemberMutationAsyncHost for DaemonClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            let Some(observation) = self
                .driver
                .observer_mut()
                .local_daemon_membership_observation_async(paths, state)
                .await?
            else {
                return Ok(());
            };
            sinomo_core::cluster::refresh_local_daemon_membership_from_disk_async(
                &self.runtime,
                paths,
                state,
                observation,
            )
            .await?;
            Ok(())
        })
    }

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>,
    > {
        let runtime = self.runtime.clone();
        let transport = self.driver.driver_mut().transport_mut();
        let options = transport.cluster_kv_txn_quorum_options(true);
        Box::pin(async move {
            // An empty mutation may return ExactCommittedRequest when a later
            // pipelined proposal commits its prefix. Membership needs a fresh
            // snapshot proof, which belongs to ReadIndex rather than a write.
            sinomo_core::cluster::read_cluster_kv_authoritative_snapshot_async_with_host(
                &runtime, paths, state, options, transport,
            )
            .await
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvTxnCommitResult,
    > {
        let request = match self.remote_control_authority.as_ref() {
            Some(authority) => match authority.fence_request(request) {
                Ok(request) => request,
                Err(error) => return Box::pin(async move { Err(error) }),
            },
            None => request,
        };
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let mut transport = DaemonClusterControlPlaneTransport::new(runtime);
            let result = commit_cluster_kv_request_with_quorum_async_on_transport(
                paths,
                state,
                request,
                true,
                &mut transport,
            )
            .await?;
            if let Some(authority) = self.remote_control_authority.as_mut() {
                authority.confirm_commit_and_advance(&result)?;
            }
            Ok(result)
        })
    }
}

fn daemon_cluster_control_plane_driver_with_mode(
    rpc_mode: DaemonClusterRpcMode,
) -> sinomo_core::cluster::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    daemon_cluster_control_plane_driver_with_mode_and_timeouts(
        rpc_mode,
        cluster_control_plane_timeouts(),
    )
}

fn daemon_cluster_control_plane_driver_with_mode_and_timeouts(
    rpc_mode: DaemonClusterRpcMode,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> sinomo_core::cluster::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    sinomo_core::cluster::ClusterControlPlaneDriver::with_timeouts(
        DaemonClusterControlPlaneTransport::with_mode_and_timeouts(rpc_mode, timeouts),
        timeouts,
    )
}

pub(crate) fn daemon_cluster_control_plane_driver_on_runtime(
    runtime: &tokio::runtime::Handle,
) -> sinomo_core::cluster::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    daemon_cluster_control_plane_driver_with_mode(DaemonClusterRpcMode::from_handle(runtime))
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonClusterControlPlaneTransport {
    rpc_mode: DaemonClusterRpcMode,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
    active_cluster_paths: Option<ClusterPaths>,
    join_rpc_context: Option<DaemonClusterJoinRpcContext>,
}

#[derive(Clone, Debug)]
struct DaemonClusterJoinRpcContext {
    daemon_paths: DaemonPaths,
    invitation_endpoint_ids: BTreeSet<String>,
}

impl DaemonClusterControlPlaneTransport {
    fn new(runtime: tokio::runtime::Handle) -> Self {
        Self::with_mode_and_timeouts(
            DaemonClusterRpcMode::new(runtime),
            cluster_control_plane_timeouts(),
        )
    }

    fn with_mode_and_timeouts(
        rpc_mode: DaemonClusterRpcMode,
        timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
    ) -> Self {
        Self {
            rpc_mode,
            timeouts,
            active_cluster_paths: None,
            join_rpc_context: None,
        }
    }

    fn set_join_rpc_context(
        &mut self,
        daemon_paths: DaemonPaths,
        invite: &sinomo_core::ClusterJoinInvite,
    ) {
        let invitation_endpoint_ids =
            sinomo_core::cluster::invite_channels_for_join_registration(invite)
                .into_iter()
                .map(|channel| channel.endpoint.endpoint_id.to_string())
                .collect();
        self.join_rpc_context = Some(DaemonClusterJoinRpcContext {
            daemon_paths,
            invitation_endpoint_ids,
        });
    }

    fn cluster_paths_for_routed_control_rpc(&self) -> Option<ClusterPaths> {
        self.active_cluster_paths
            .clone()
            .or_else(daemon_control_transport_env_cluster_paths)
    }

    fn state_authority_reachability_options(
        &self,
    ) -> sinomo_core::cluster::ClusterStateAuthorityReachabilityOptions {
        self.timeouts.state_authority_reachability_options()
    }

    fn cluster_kv_txn_quorum_options(
        &self,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvTxnQuorumOptions {
        self.timeouts.kv_txn_quorum_options(allow_forwarding)
    }
}

impl sinomo_core::cluster::ClusterControlPlaneTransport for DaemonClusterControlPlaneTransport {
    fn replace_active_cluster_paths(
        &mut self,
        paths: Option<ClusterPaths>,
    ) -> Option<ClusterPaths> {
        std::mem::replace(&mut self.active_cluster_paths, paths)
    }

    fn send_kv_replication(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvReplicationRequest,
        timeout: Duration,
    ) -> Result<ClusterKvReplicationResponse, sinomo_core::network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        if let Some(paths) = self.cluster_paths_for_routed_control_rpc() {
            return send_cluster_raft_append_routed_on_runtime(
                &paths,
                &endpoint.node_id,
                request,
                timeout,
                runtime,
            );
        }
        send_cluster_raft_append_on_runtime(endpoint, request, timeout, runtime).map_err(|error| {
            sinomo_core::network::NetworkError::Io {
                operation: "cluster raft append".to_string(),
                detail: error.to_string(),
            }
        })
    }

    fn send_kv_read_index_probes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &ClusterKvReplicationRequest,
        timeout: Duration,
        required_acks: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<ClusterKvReplicationResponse, sinomo_core::network::NetworkError>,
        )>,
        sinomo_core::network::NetworkError,
    > {
        let paths = self.cluster_paths_for_routed_control_rpc().ok_or_else(|| {
            sinomo_core::network::NetworkError::Io {
                operation: "concurrent cluster ReadIndex probes".to_string(),
                detail: "cluster paths are unavailable for routed ReadIndex probes".to_string(),
            }
        })?;
        let owner_runtime = self.rpc_mode.runtime().clone();
        let runtime = owner_runtime.clone();
        let request = std::sync::Arc::new(request.clone());
        block_on_cluster_network_future_on_owner_runtime(
            &owner_runtime,
            Box::pin(async move {
                let mut tasks = tokio::task::JoinSet::new();
                for target in targets {
                    let paths = paths.clone();
                    let runtime = runtime.clone();
                    let request = std::sync::Arc::clone(&request);
                    tasks.spawn(async move {
                        let result = send_cluster_raft_append_routed_async(
                            &paths, &target, &request, timeout, &runtime,
                        )
                        .await;
                        (target, result)
                    });
                }

                let expected_term = request.append.term;
                let mut accepted = 0_usize;
                let mut outcomes = Vec::new();
                let collect = tokio::time::timeout(timeout, async {
                    while let Some(joined) = tasks.join_next().await {
                        let (target, result) =
                            joined.map_err(|source| sinomo_core::network::NetworkError::Io {
                                operation: "join concurrent cluster ReadIndex probe".to_string(),
                                detail: source.to_string(),
                            })?;
                        if result.as_ref().is_ok_and(|response| {
                            response.accepted && response.append.term == expected_term
                        }) {
                            accepted = accepted.saturating_add(1);
                        }
                        outcomes.push((target, result));
                        if accepted >= required_acks {
                            break;
                        }
                    }
                    Ok::<(), sinomo_core::network::NetworkError>(())
                })
                .await;
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                if let Ok(result) = collect {
                    result?;
                }
                Ok(outcomes)
            }),
        )
    }

    fn request_kv_state_authority_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterKvStateAuthorityVoteResponse, sinomo_core::network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        if let Some(paths) = self.cluster_paths_for_routed_control_rpc() {
            return send_cluster_raft_vote_routed_on_runtime(
                &paths,
                &endpoint.node_id,
                request,
                timeout,
                runtime,
            );
        }
        send_cluster_raft_vote_on_runtime(endpoint, request, timeout, runtime).map_err(|error| {
            sinomo_core::network::NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: error.to_string(),
            }
        })
    }

    fn request_kv_state_authority_votes_from_nodes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, sinomo_core::network::NetworkError>,
        )>,
        sinomo_core::network::NetworkError,
    > {
        let paths = self.cluster_paths_for_routed_control_rpc().ok_or_else(|| {
            sinomo_core::network::NetworkError::Io {
                operation: "concurrent cluster Raft votes".to_string(),
                detail: "cluster paths are unavailable for routed Raft votes".to_string(),
            }
        })?;
        let owner_runtime = self.rpc_mode.runtime().clone();
        let runtime = owner_runtime.clone();
        let state = std::sync::Arc::new(state.clone());
        let request = std::sync::Arc::new(request.clone());
        block_on_cluster_network_future_on_owner_runtime(
            &owner_runtime,
            Box::pin(async move {
                let mut tasks = tokio::task::JoinSet::new();
                for target in targets {
                    let paths = paths.clone();
                    let runtime = runtime.clone();
                    let request = std::sync::Arc::clone(&request);
                    tasks.spawn(async move {
                        let result = send_cluster_raft_vote_routed_async(
                            &paths, &target, &request, timeout, &runtime,
                        )
                        .await;
                        (target, result)
                    });
                }

                let mut granted = 0_usize;
                let mut outcomes = Vec::new();
                let collect = tokio::time::timeout(timeout, async {
                    while let Some(joined) = tasks.join_next().await {
                        let (target, result) =
                            joined.map_err(|source| sinomo_core::network::NetworkError::Io {
                                operation: "join concurrent cluster Raft vote".to_string(),
                                detail: source.to_string(),
                            })?;
                        if result.as_ref().is_ok_and(|response| {
                            response.accepted
                                && response.vote.vote_granted
                                && response.vote.term == request.vote.term
                        }) {
                            granted = granted.saturating_add(1);
                        }
                        outcomes.push((target, result));
                        if granted >= required_grants {
                            // Capture every response that completed concurrently
                            // with quorum, especially a mandatory higher term,
                            // before cancelling genuinely outstanding RPCs.
                            while let Some(joined) = tasks.try_join_next() {
                                let (target, result) = joined.map_err(|source| {
                                    sinomo_core::network::NetworkError::Io {
                                        operation: "join ready concurrent cluster Raft vote"
                                            .to_string(),
                                        detail: source.to_string(),
                                    }
                                })?;
                                outcomes.push((target, result));
                            }
                            break;
                        }
                    }
                    Ok::<(), sinomo_core::network::NetworkError>(())
                })
                .await;
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                if let Ok(result) = collect {
                    result?;
                }
                let _ = state;
                Ok(outcomes)
            }),
        )
    }

    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool {
        fetch_daemon_status_from_endpoint_on_runtime(endpoint, timeout, self.rpc_mode.runtime())
            .is_ok()
    }

    fn probe_cluster_state_authority_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> bool {
        let Ok(request) = ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        ) else {
            return false;
        };
        self.call_node_object_rpc(state, target_node_id, &request, timeout)
            .and_then(|response| {
                response.error_detail()?;
                Ok(response)
            })
            .is_ok()
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        fetch_cluster_kv_local_snapshot_from_endpoint_on_runtime(endpoint, timeout, runtime)
            .map(|local| local.value)
            .map_err(|error| daemon_host_network_error("cluster kv snapshot", error))
    }

    fn call_endpoint_object_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_core::network::NetworkError> {
        let result = if let Some(context) = self.join_rpc_context.as_ref() {
            if !context
                .invitation_endpoint_ids
                .contains(&endpoint.endpoint_id.to_string())
            {
                return Err(sinomo_core::network::NetworkError::Io {
                    operation: "cluster endpoint object rpc".to_string(),
                    detail: format!(
                        "join endpoint {} is absent from the invitation channels",
                        endpoint.address
                    ),
                });
            }
            crate::client::send_daemon_join_bootstrap_to_endpoint_on_runtime(
                &context.daemon_paths,
                endpoint,
                request,
                timeout,
                self.rpc_mode.runtime(),
            )
        } else {
            crate::client::send_daemon_rpc_to_endpoint_with_options_on_runtime(
                endpoint,
                request,
                daemon_cluster_control_rpc_options(timeout),
                self.rpc_mode.runtime(),
            )
        };
        result.map_err(|error| daemon_host_network_error("cluster endpoint object rpc", error))
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_core::network::NetworkError> {
        call_node_object_rpc_on_runtime(
            state,
            target_node_id,
            request,
            daemon_cluster_control_rpc_options(timeout),
            self.rpc_mode.runtime(),
        )
        .map_err(|error| daemon_host_network_error("cluster node rpc", error))
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: sinomo_core::network::ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        remember_cluster_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
            Some(self.rpc_mode.runtime()),
        );
    }

    fn published_network_info_matches(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) -> bool {
        cached_network_info_matches(paths, info)
    }

    fn remember_published_network_info(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) {
        remember_network_info(paths, info);
    }

    fn published_node_status_matches(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) -> bool {
        cached_node_status_matches(paths, status)
    }

    fn remember_published_node_status(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) {
        remember_node_status(paths, status);
    }

    fn published_connectivity_record_set_matches(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) -> bool {
        cached_connectivity_record_set_matches(paths, source_node_id, records)
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) {
        remember_connectivity_record_set(paths, source_node_id, records);
    }

    fn fan_out_committed_cluster_kv_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
        crate::cluster::fan_out_committed_cluster_raft_command_on_runtime(
            paths,
            state,
            events,
            Some(self.rpc_mode.runtime()),
        );
    }

    fn on_kv_replication_sent(&mut self) {
        if let Some(stats) = crate::process_idle_stats() {
            stats.incr_raft_appends_sent();
        }
    }
}

impl sinomo_core::cluster::ClusterStateAuthorityReachabilityAsyncHost
    for DaemonClusterControlPlaneTransport
{
    fn probe_cluster_state_authority_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
        let paths = self.cluster_paths_for_routed_control_rpc();
        Box::pin(async move {
            let Some(paths) = paths else {
                return Ok(false);
            };
            let result =
                sinomo_core::network::ClusterObjectRpcAsyncHost::call_node_object_rpc_async(
                    self,
                    &paths,
                    state,
                    target_node_id,
                    sinomo_core::network::ObjectRpcCall::encode_value(
                        crate::DAEMON_STATUS_ROUTE,
                        crate::DAEMON_RPC_CATEGORY,
                        (),
                    )?,
                    daemon_main_routed_rpc_options(timeout),
                )
                .await;
            Ok(match result {
                Ok(result) => result.decode_body::<crate::DaemonStatus>().await.is_ok(),
                Err(_) => false,
            })
        })
    }
}

impl sinomo_core::cluster::ClusterKvTxnQuorumAsyncHost for DaemonClusterControlPlaneTransport {
    fn request_kv_state_authority_vote_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse>
    {
        let runtime = self.rpc_mode.runtime().clone();
        let paths = self.cluster_paths_for_routed_control_rpc();
        Box::pin(async move {
            if let Some(paths) = paths {
                return send_cluster_raft_vote_routed_async(
                    &paths,
                    target_node_id,
                    request,
                    timeout,
                    &runtime,
                )
                .await;
            }
            Err(sinomo_core::network::NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: "cluster paths are unavailable for routed node vote".to_string(),
            })
        })
    }

    fn request_kv_state_authority_votes_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &'a ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, sinomo_core::network::NetworkError>,
        )>,
    > {
        let runtime = self.rpc_mode.runtime().clone();
        let paths = self.cluster_paths_for_routed_control_rpc();
        let request = std::sync::Arc::new(request.clone());
        Box::pin(async move {
            let paths = paths.ok_or_else(|| sinomo_core::network::NetworkError::Io {
                operation: "concurrent cluster Raft votes".to_string(),
                detail: "cluster paths are unavailable for routed Raft votes".to_string(),
            })?;
            let mut tasks = tokio::task::JoinSet::new();
            for target in targets {
                let paths = paths.clone();
                let runtime = runtime.clone();
                let request = std::sync::Arc::clone(&request);
                tasks.spawn(async move {
                    let result = send_cluster_raft_vote_routed_async(
                        &paths, &target, &request, timeout, &runtime,
                    )
                    .await;
                    (target, result)
                });
            }
            let mut granted = 0_usize;
            let mut outcomes = Vec::new();
            let collect = tokio::time::timeout(timeout, async {
                while let Some(joined) = tasks.join_next().await {
                    let (target, result) =
                        joined.map_err(|source| sinomo_core::network::NetworkError::Io {
                            operation: "join concurrent cluster Raft vote".to_string(),
                            detail: source.to_string(),
                        })?;
                    if result.as_ref().is_ok_and(|response| {
                        response.accepted
                            && response.vote.vote_granted
                            && response.vote.term == request.vote.term
                    }) {
                        granted = granted.saturating_add(1);
                    }
                    outcomes.push((target, result));
                    if granted >= required_grants {
                        while let Some(joined) = tasks.try_join_next() {
                            let (target, result) = joined.map_err(|source| {
                                sinomo_core::network::NetworkError::Io {
                                    operation: "join ready concurrent cluster Raft vote"
                                        .to_string(),
                                    detail: source.to_string(),
                                }
                            })?;
                            outcomes.push((target, result));
                        }
                        break;
                    }
                }
                Ok::<(), sinomo_core::network::NetworkError>(())
            })
            .await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            if let Ok(result) = collect {
                result?;
            }
            Ok(outcomes)
        })
    }
}

impl sinomo_core::network::ClusterObjectRpcAsyncHost for DaemonClusterControlPlaneTransport {
    fn observe_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_core::network::ClusterObjectRpcObservation,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<'a, ()> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        let cluster_paths = self.cluster_paths_for_routed_control_rpc();
        Box::pin(async move {
            let mut host = DaemonObjectRpcAsyncHost::new(&runtime, true);
            if let Some(paths) = cluster_paths.as_ref() {
                host = host.with_cluster_paths(paths);
            }
            host.observe_endpoint_rpc_async(&endpoint, observation)
                .await
        })
    }

    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        let runtime = self.rpc_mode.runtime().clone();
        let cluster_paths = self.cluster_paths_for_routed_control_rpc();
        Box::pin(async move {
            let mut host = DaemonObjectRpcAsyncHost::new(&runtime, true);
            if let Some(paths) = cluster_paths.as_ref() {
                host = host.with_cluster_paths(paths);
            }
            host.open_object_rpc_stream_async(endpoint, health).await
        })
    }

    fn call_node_object_rpc_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        call: sinomo_core::network::ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<'a, sinomo_core::network::ObjectRpcResult>
    {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            call_node_object_rpc_with_cluster_paths_async_on_runtime(
                paths,
                state,
                target_node_id,
                call,
                options,
                &runtime,
            )
            .await
        })
    }
}

impl sinomo_core::cluster::ClusterLocalMemberPublishAsyncHost
    for DaemonClusterControlPlaneTransport
{
    fn cluster_has_local_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move {
            sinomo_core::cluster::cluster_has_local_quorum_async_with_host(
                paths,
                state,
                self.state_authority_reachability_options(),
                self,
            )
            .await
        })
    }

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<KvSnapshot>,
    > {
        Box::pin(async move {
            let result = commit_cluster_kv_request_with_quorum_async_on_transport(
                paths,
                state,
                sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
                    compares: Vec::new(),
                    success: Vec::new(),
                    failure: Vec::new(),
                }),
                true,
                self,
            )
            .await?;
            Ok(sinomo_core::kv::KvAuthoritativeRead {
                value: result.snapshot,
                authority: result.authority,
            })
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvTxnCommitResult,
    > {
        Box::pin(async move {
            commit_cluster_kv_request_with_quorum_async_on_transport(
                paths, state, request, true, self,
            )
            .await
        })
    }

    fn send_cluster_member_publish_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterMemberPublishResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            send_cluster_member_publish_async(endpoint, request, timeout, &runtime)
                .await
                .map_err(|error| {
                    daemon_host_network_error("cluster local member publish rpc", error)
                })
        })
    }
}

impl sinomo_core::cluster::ClusterConnectivityPublishAsyncHost
    for DaemonClusterControlPlaneTransport
{
    type Error = sinomo_core::network::NetworkError;

    fn published_connectivity_record_set_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        source_node_id: &'a NodeId,
        records: &'a [ClusterConnectivityRecord],
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move {
            Ok(cached_connectivity_record_set_matches(
                paths,
                source_node_id,
                records,
            ))
        })
    }

    fn remember_published_connectivity_record_set_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        source_node_id: &'a NodeId,
        records: &'a [ClusterConnectivityRecord],
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_connectivity_record_set(paths, source_node_id, records);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, request, true, self)
                .await
        })
    }
}

impl sinomo_core::cluster::ClusterNetworkInfoPublishAsyncHost
    for DaemonClusterControlPlaneTransport
{
    type Error = sinomo_core::network::NetworkError;

    fn published_network_info_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        info: &'a ClusterNetworkInfoRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move { Ok(cached_network_info_matches(paths, info)) })
    }

    fn remember_published_network_info_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        info: &'a ClusterNetworkInfoRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_network_info(paths, info);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, request, true, self)
                .await
        })
    }
}

impl sinomo_core::cluster::ClusterNodeStatusPublishAsyncHost
    for DaemonClusterControlPlaneTransport
{
    type Error = sinomo_core::network::NetworkError;

    fn published_node_status_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        status: &'a ClusterNodeStatusRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move { Ok(cached_node_status_matches(paths, status)) })
    }

    fn remember_published_node_status_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        status: &'a ClusterNodeStatusRecord,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_node_status(paths, status);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, request, true, self)
                .await
        })
    }
}

impl sinomo_core::cluster::ClusterKvCommittedFanoutAsyncHost
    for DaemonClusterControlPlaneTransport
{
    fn replicate_cluster_kv_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome>
    {
        Box::pin(async move {
            sinomo_core::cluster::replicate_cluster_kv_to_node_async_with_host(
                paths,
                state,
                target_node_id,
                watch_events,
                self.timeouts,
                timeout,
                self,
            )
            .await
        })
    }

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        let paths = self.cluster_paths_for_routed_control_rpc();
        Box::pin(async move {
            if let Some(response) = sinomo_core::kv::cluster_kv_watch_events_noop_response(request)
            {
                return Ok(response);
            }
            let Some(paths) = paths else {
                return Err(sinomo_core::network::NetworkError::Io {
                    operation: "cluster kv watch-event fanout".to_string(),
                    detail: "cluster paths are unavailable for routed watch-event fanout"
                        .to_string(),
                });
            };
            let response =
                sinomo_core::network::ClusterObjectRpcAsyncHost::call_node_object_rpc_async(
                    self,
                    &paths,
                    state,
                    target_node_id,
                    sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                        sinomo_core::kv::CLUSTER_KV_WATCH_EVENTS_ROUTE,
                        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
                        request,
                    )?,
                    sinomo_core::cluster::main_daemon_cluster_rpc_options(
                        sinomo_core::network::ClusterObjectRpcOptions::default()
                            .with_overall_timeout(timeout),
                    ),
                )
                .await?;
            response.decode_body().await
        })
    }
}

impl sinomo_core::cluster::ClusterKvQuorumAsyncHost for DaemonClusterControlPlaneTransport {
    fn last_local_kv_replication_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, u64> {
        Box::pin(async move {
            paths
                .kv_paths()
                .locked_last_raft_log_index_async(&state.local_node_id)
                .await
                .map_err(sinomo_core::network::NetworkError::directory)
        })
    }

    fn build_kv_replication_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest> {
        Box::pin(async move {
            paths
                .kv_paths()
                .locked_build_raft_append_request_from_next_index_async(state, next_index)
                .await
                .map_err(sinomo_core::network::NetworkError::directory)
        })
    }

    fn send_cluster_kv_replication_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            send_cluster_raft_append_routed_async(paths, target_node_id, request, timeout, &runtime)
                .await
        })
    }

    fn send_cluster_kv_replications_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        attempts: sinomo_core::cluster::ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::cluster::ClusterRaftQuorumBatchOutcomes,
    > {
        let runtime = self.rpc_mode.runtime().clone();
        let paths = paths.clone();
        Box::pin(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for (target_node_id, request, timeout) in attempts {
                let runtime = runtime.clone();
                let paths = paths.clone();
                tasks.spawn(async move {
                    let result = send_cluster_raft_append_routed_async(
                        &paths,
                        &target_node_id,
                        &request,
                        timeout,
                        &runtime,
                    )
                    .await;
                    (target_node_id, request, result)
                });
            }

            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            while let Some(joined) = tasks.join_next().await {
                let (target_node_id, request, result) =
                    joined.map_err(|source| sinomo_core::network::NetworkError::Io {
                        operation: "join concurrent cluster Raft append".to_string(),
                        detail: source.to_string(),
                    })?;
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                outcomes.push((target_node_id, request, result));
                if accepted >= required_acks {
                    tasks.abort_all();
                    break;
                }
            }
            Ok(outcomes)
        })
    }

    fn on_kv_replication_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            if let Some(stats) = crate::process_idle_stats() {
                stats.incr_raft_appends_sent();
            }
            Ok(())
        })
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
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            let _ = targets;
            crate::cluster::fan_out_committed_cluster_raft_command_on_runtime(
                paths,
                state,
                events,
                Some(&runtime),
            );
            Ok(())
        })
    }
}

#[cfg(test)]
fn daemon_cluster_status_from_local_state(
    paths: &ClusterPaths,
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, DaemonError> {
    let mut state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
    let _ = refresh_local_daemon_membership(
        paths,
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    )?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| DaemonError::Parse {
        detail: "daemon cluster member could not be resolved from local state".to_string(),
    })
}

pub(crate) fn sync_cluster_node_status_to_kv_on_runtime(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_core::cluster::publish_cluster_node_status_to_quorum_with_host(paths, status, &mut host)
        .map_err(Into::into)
}

async fn sync_cluster_node_status_to_kv_async_with_timeouts(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    runtime: tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::new(runtime.clone()),
        timeouts,
    );
    sinomo_core::cluster::publish_cluster_node_status_to_quorum_async_with_host(
        &runtime, paths, status, &mut host,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    sync_cluster_network_info_to_kv_on_runtime(paths, info, runtime.handle())
}

pub(crate) fn sync_cluster_network_info_to_kv_on_runtime(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_core::cluster::publish_cluster_network_info_to_quorum_with_host(paths, info, &mut host)
        .map_err(Into::into)
}

async fn sync_cluster_network_info_to_kv_async_with_timeouts(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    runtime: tokio::runtime::Handle,
    timeouts: sinomo_core::cluster::ClusterControlPlaneTimeouts,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport::with_mode_and_timeouts(
        DaemonClusterRpcMode::new(runtime.clone()),
        timeouts,
    );
    sinomo_core::cluster::publish_cluster_network_info_to_quorum_async_with_host(
        &runtime, paths, info, &mut host,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    sync_cluster_connectivity_records_to_kv_on_runtime(
        paths,
        source_node_id,
        records,
        runtime.handle(),
    )
}

#[cfg(test)]
pub(crate) fn sync_cluster_connectivity_records_to_kv_on_runtime(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_with_host(
        paths,
        source_node_id,
        records,
        &mut host,
    )
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn assign_local_node_public_keys(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    sinomo_core::cluster::assign_local_daemon_member_public_keys(
        state,
        main_identity.public_key,
        shadow_identity.public_key,
    );
    Ok(())
}

fn local_daemon_membership_observation(
    data_dir: &std::path::Path,
    state: &ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let lan_ips = sinomo_core::network::discover_default_lan_ip_candidates();
    let route_ips =
        sinomo_core::network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state(
            state,
        );
    let endpoint_config = crate::read_daemon_endpoint_advertisement_config(data_dir)?;
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let main_endpoints = crate::daemon_advertised_endpoints_for_state_with_manual_config(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
        &endpoint_config.main_endpoints,
    );
    let shadow_endpoints = crate::daemon_advertised_endpoints_for_state_with_manual_config(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
        &endpoint_config.shadow_endpoints,
    );
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    Ok(
        sinomo_core::cluster::ClusterLocalDaemonMembershipObservation {
            main_endpoints,
            shadow_endpoints,
            main_public_key: main_identity.public_key,
            shadow_public_key: shadow_identity.public_key,
            timestamp,
        },
    )
}

async fn local_daemon_membership_observation_async(
    data_dir: &std::path::Path,
    state: &ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let lan_ips = sinomo_core::network::discover_default_lan_ip_candidates_async();
    let route_ips =
        sinomo_core::network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state_async(
            state,
        );
    let (lan_ips, route_ips) = tokio::join!(lan_ips, route_ips);
    let endpoint_config = crate::read_daemon_endpoint_advertisement_config(data_dir)?;
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let main_endpoints = crate::daemon_advertised_endpoints_for_state_with_manual_config_async(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
        &endpoint_config.main_endpoints,
    );
    let shadow_endpoints = crate::daemon_advertised_endpoints_for_state_with_manual_config_async(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
        &endpoint_config.shadow_endpoints,
    );
    let identity_root = data_dir.to_path_buf();
    let identity_path = identity_root.display().to_string();
    let identities = runtime.spawn_blocking(move || {
        let main_identity = load_or_create_main_node_identity(&identity_root)?;
        let shadow_identity = load_or_create_shadow_node_identity(&identity_root)?;
        Ok::<_, DaemonError>((main_identity, shadow_identity))
    });
    let (main_endpoints, shadow_endpoints, identities) =
        tokio::join!(main_endpoints, shadow_endpoints, identities);
    let (main_identity, shadow_identity) = identities.map_err(|source| DaemonError::Io {
        path: identity_path,
        detail: format!("node identity loader task failed: {source}"),
    })??;
    Ok(
        sinomo_core::cluster::ClusterLocalDaemonMembershipObservation {
            main_endpoints,
            shadow_endpoints,
            main_public_key: main_identity.public_key,
            shadow_public_key: shadow_identity.public_key,
            timestamp,
        },
    )
}

fn local_daemon_membership_observation_from_daemon_paths_on_runtime(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let now = OffsetDateTime::now_utc();
    let (main_options, shadow_options) = if let Ok(main_status) = read_daemon_status(paths) {
        let shadow_options = current_shadow_daemon_options_for_cluster_runtime(
            data_dir,
            DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
            runtime,
        )?;
        (daemon_options_from_status(&main_status), shadow_options)
    } else {
        (
            DaemonRunOptions::default(),
            DaemonRunOptions::shadow_default(),
        )
    };
    let runtime_for_future = runtime.clone();
    block_on_daemon_cluster_future_on_owner_runtime(
        runtime,
        Box::pin(local_daemon_membership_observation_async(
            data_dir,
            state,
            &main_options,
            &shadow_options,
            now,
            &runtime_for_future,
        )),
    )
}

async fn local_daemon_membership_observation_from_daemon_paths_async(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let options_paths = paths.clone();
    let options_path = options_paths.root.display().to_string();
    let main_status = runtime
        .spawn_blocking(move || read_daemon_status(&options_paths).ok())
        .await
        .map_err(|source| DaemonError::Io {
            path: options_path,
            detail: format!("daemon membership options task failed: {source}"),
        })?;
    let (main_options, shadow_options) = if let Some(main_status) = main_status {
        (
            daemon_options_from_status(&main_status),
            current_shadow_daemon_options_for_cluster_async(
                data_dir,
                DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
                runtime,
            )
            .await,
        )
    } else {
        (
            DaemonRunOptions::default(),
            DaemonRunOptions::shadow_default(),
        )
    };
    local_daemon_membership_observation_async(
        data_dir,
        state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
        runtime,
    )
    .await
}

fn refresh_local_daemon_membership_on_runtime(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipRefresh, DaemonError> {
    let Some(runtime) = runtime else {
        return refresh_local_daemon_membership(
            paths,
            state,
            main_options,
            shadow_options,
            timestamp,
        );
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let runtime_for_future = runtime.clone();
    let observation = block_on_daemon_cluster_future_on_owner_runtime(
        runtime,
        Box::pin(local_daemon_membership_observation_async(
            data_dir,
            state,
            main_options,
            shadow_options,
            timestamp,
            &runtime_for_future,
        )),
    )?;
    sinomo_core::cluster::refresh_local_daemon_membership_from_disk(paths, state, observation)
        .map_err(Into::into)
}

async fn refresh_local_daemon_membership_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipRefresh, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let observation = local_daemon_membership_observation_async(
        data_dir,
        state,
        main_options,
        shadow_options,
        timestamp,
        runtime,
    )
    .await?;
    let result = sinomo_core::cluster::refresh_local_daemon_membership_from_disk_async(
        runtime,
        paths,
        state,
        observation,
    )
    .await
    .map_err(DaemonError::from)?;
    Ok(result)
}

fn refresh_local_daemon_membership(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<sinomo_core::cluster::ClusterLocalDaemonMembershipRefresh, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let observation = local_daemon_membership_observation(
        data_dir,
        state,
        main_options,
        shadow_options,
        timestamp,
    )?;
    sinomo_core::cluster::refresh_local_daemon_membership_from_disk(paths, state, observation)
        .map_err(Into::into)
}

fn fan_out_committed_cluster_raft_command_on_runtime(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
    runtime: Option<&tokio::runtime::Handle>,
) {
    if let Some(runtime) = runtime.cloned() {
        let host_runtime = runtime.clone();
        sinomo_core::cluster::spawn_cluster_kv_committed_fanout_worker_on_runtime(
            runtime,
            paths,
            state,
            events,
            cluster_control_plane_timeouts(),
            move || daemon_cluster_control_plane_driver_on_runtime(&host_runtime),
        );
    } else {
        let _ = (paths, state, events);
        tracing::warn!(
            "sinomo daemon cluster raft fanout skipped: {}",
            daemon_cluster_runtime_required_error(
                "cluster raft fanout",
                "cluster raft committed fanout"
            )
        );
    }
}

pub fn commit_cluster_kv_txn_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<KvSnapshot, DaemonError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Io {
            path: "tokio runtime".to_string(),
            detail: source.to_string(),
        })?;
    commit_cluster_kv_txn_with_quorum_on_runtime(paths, state, txn, runtime.handle())
}

pub(crate) fn commit_cluster_kv_txn_with_quorum_on_runtime(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    commit_cluster_kv_txn_with_quorum_response_on_runtime(paths, state, txn, runtime)
        .map(|(snapshot, _)| snapshot)
}

#[cfg(test)]
fn commit_cluster_kv_txn_with_quorum_response(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    commit_cluster_kv_txn_with_quorum_response_on_runtime(paths, state, txn, runtime.handle())
}

fn commit_cluster_kv_txn_with_quorum_response_on_runtime(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    runtime: &tokio::runtime::Handle,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    commit_cluster_kv_txn_with_quorum_response_with_forwarding_on_runtime(
        paths, state, txn, true, runtime,
    )
}

#[cfg(test)]
fn commit_cluster_kv_txn_with_quorum_response_with_forwarding(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    allow_forwarding: bool,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    let runtime = daemon_cluster_test_runtime();
    commit_cluster_kv_txn_with_quorum_response_with_forwarding_on_runtime(
        paths,
        state,
        txn,
        allow_forwarding,
        runtime.handle(),
    )
}

fn commit_cluster_kv_txn_with_quorum_response_with_forwarding_on_runtime(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    let result = sinomo_core::cluster::commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        state,
        sinomo_core::kv::KvTxnRequest::generated(txn),
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )?;
    Ok((result.snapshot, result.txn_response))
}

fn daemon_host_network_error(
    operation: &str,
    error: DaemonError,
) -> sinomo_core::network::NetworkError {
    match error {
        DaemonError::AuthDenied => sinomo_core::network::NetworkError::AuthDenied,
        DaemonError::AuthExpired => sinomo_core::network::NetworkError::AuthExpired,
        DaemonError::ClusterReadOnly { detail } => {
            sinomo_core::network::NetworkError::ClusterReadOnly { detail }
        }
        DaemonError::Network { address, detail } => sinomo_core::network::NetworkError::Io {
            operation: format!("{operation} {address}"),
            detail,
        },
        other => sinomo_core::network::NetworkError::Io {
            operation: operation.to_string(),
            detail: other.to_string(),
        },
    }
}

fn cluster_control_plane_timeouts() -> sinomo_core::cluster::ClusterControlPlaneTimeouts {
    sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
}

#[cfg(test)]
fn cluster_state_authority_reachability_options()
-> sinomo_core::cluster::ClusterStateAuthorityReachabilityOptions {
    cluster_control_plane_timeouts().state_authority_reachability_options()
}

fn cluster_kv_txn_quorum_options(
    allow_forwarding: bool,
) -> sinomo_core::cluster::ClusterKvTxnQuorumOptions {
    cluster_control_plane_timeouts().kv_txn_quorum_options(allow_forwarding)
}

#[cfg(test)]
mod tests {
    use super::*;

    type ClusterNetworkMaintenanceDueClaims =
        sinomo_core::cluster::ClusterNetworkMaintenanceDueClaims;

    // Test fixtures intentionally spell out scheduler generations. Keep that
    // readability while the production host stores those fields in the
    // cluster-owned cadence state machine.
    macro_rules! cluster_network_maintenance {
        (@parse [$($cadence:tt)*] [$($host:tt)*]) => {
            ClusterNetworkMaintenance {
                cadence: sinomo_core::cluster::ClusterNetworkMaintenanceCadence {
                    $($cadence)*
                },
                resource_mode: sinomo_device::NodeResourceMode::Normal,
                $($host)*
            }
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            connectivity_converged: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* connectivity_converged: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            local_node_role: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* local_node_role: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            startup_settle_until: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* startup_settle_until: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            publish_failure_streak: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* publish_failure_streak: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            probe_failure_streak: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* probe_failure_streak: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            discovery_failure_streak: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* discovery_failure_streak: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            probe_retry_at: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* probe_retry_at: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            discovery_retry_at: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* discovery_retry_at: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            connectivity_publish_failure_streak: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* connectivity_publish_failure_streak: $value,]
                [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            connectivity_publish_retry_at: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* connectivity_publish_retry_at: $value,]
                [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            connectivity_input_generation: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* connectivity_input_generation: $value,]
                [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            connectivity_force_probe: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* connectivity_force_probe: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            last_publish: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* last_publish: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            last_discovery: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* last_discovery: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            last_probe: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* last_probe: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            active_due_claims: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)* active_due_claims: $value,] [$($host)*] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            $field:ident: $value:expr, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)*] [$($host)* $field: $value,] $($rest)*)
        };
        (@parse [$($cadence:tt)*] [$($host:tt)*]
            $field:ident, $($rest:tt)*) => {
            cluster_network_maintenance!(@parse
                [$($cadence)*] [$($host)* $field,] $($rest)*)
        };
        ($($fields:tt)*) => {
            cluster_network_maintenance!(@parse [] [] $($fields)*)
        };
    }

    async fn stalled_cluster_raft_response_body_error<T>(
        operation: &'static str,
        timeout: Duration,
        response_head_delay: Duration,
    ) -> (sinomo_core::network::NetworkError, Duration)
    where
        T: serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static,
    {
        let (body_sender, body) =
            sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(16), 1);
        let response = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            "/test/stalled-cluster-raft-body",
            Vec::new(),
        );
        let result =
            sinomo_core::network::ObjectRpcResult::from_forwarded_body(response, body, true)
                .expect("construct streamed Raft response");
        let started_at = tokio::time::Instant::now();
        let error = decode_cluster_raft_rpc_before::<T, _>(operation, timeout, async move {
            tokio::time::sleep(response_head_delay).await;
            Ok(result)
        })
        .await
        .expect_err("a stalled streamed Raft body must time out");
        // Keep the sending half alive through decode so the receiver genuinely
        // stalls instead of observing an immediately closed test channel.
        drop(body_sender);
        (error, tokio::time::Instant::now() - started_at)
    }

    #[tokio::test(start_paused = true)]
    async fn raft_append_deadline_covers_a_stalled_streamed_response_body() {
        let timeout = Duration::from_millis(80);
        let (error, elapsed) =
            stalled_cluster_raft_response_body_error::<ClusterRaftAppendResponse>(
                "cluster Raft append RPC",
                timeout,
                Duration::from_millis(55),
            )
            .await;
        assert_eq!(elapsed, timeout);
        assert!(matches!(
            error,
            sinomo_core::network::NetworkError::Timeout {
                ref operation,
                phase: sinomo_core::network::NetworkTimeoutPhase::Overall,
                timeout: actual_timeout,
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
            } if operation == "cluster Raft append RPC" && actual_timeout == timeout
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn raft_vote_deadline_covers_a_stalled_streamed_response_body() {
        let timeout = Duration::from_millis(80);
        let (error, elapsed) = stalled_cluster_raft_response_body_error::<ClusterRaftVoteResponse>(
            "cluster Raft vote RPC",
            timeout,
            Duration::from_millis(55),
        )
        .await;
        assert_eq!(elapsed, timeout);
        assert!(matches!(
            error,
            sinomo_core::network::NetworkError::Timeout {
                ref operation,
                phase: sinomo_core::network::NetworkTimeoutPhase::Overall,
                timeout: actual_timeout,
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
            } if operation == "cluster Raft vote RPC" && actual_timeout == timeout
        ));
    }

    #[test]
    fn exact_authority_bootstrap_load_is_bounded_while_blocking_pool_is_saturated() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("authority-read runtime");
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let blocker = runtime.spawn_blocking(move || {
            entered_tx.send(()).expect("announce blocking owner");
            release_rx.recv().expect("release blocking owner");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool owner started");

        let root = sinomo_testdir::TestDir::new("daemon-exact-authority-bootstrap-deadline");
        let data_dir = SinomoDataDir::new(root.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        let timeout = Duration::from_millis(80);
        let started = Instant::now();
        let deadline = started + timeout;
        let result = runtime.block_on(
            read_cluster_kv_authoritative_entries_with_cluster_paths_async_on_runtime_before_deadline(
                &cluster_paths,
                vec!["/sifs/authority".to_string()],
                false,
                runtime.handle(),
                deadline,
            ),
        );
        let elapsed = started.elapsed();

        release_tx.send(()).expect("release blocking owner");
        runtime.block_on(blocker).expect("join blocking owner");
        let error = result.expect_err("queued bootstrap load must respect authority deadline");
        assert!(
            error.to_string().contains("network timeout"),
            "unexpected deadline error: {error}"
        );
        assert!(
            elapsed >= Duration::from_millis(70) && elapsed < Duration::from_millis(500),
            "blocking-pool saturation must consume only the original authority budget, elapsed {elapsed:?}"
        );
    }

    #[test]
    fn node_status_publish_observation_phase_labels_are_bounded_and_exhaustive() {
        let phases = [
            DaemonNodeStatusPublishPhase::DaemonStatusRead,
            DaemonNodeStatusPublishPhase::ClusterStatusEnsure,
            DaemonNodeStatusPublishPhase::ClusterStateRead,
            DaemonNodeStatusPublishPhase::PresenceMatch,
            DaemonNodeStatusPublishPhase::NodeStatusCommit,
            DaemonNodeStatusPublishPhase::NetworkInfoCommit,
        ];
        assert_eq!(
            phases.map(daemon_node_status_publish_phase_name),
            [
                "daemon_status_read",
                "cluster_status_ensure",
                "cluster_state_read",
                "presence_match",
                "node_status_commit",
                "network_info_commit",
            ]
        );
        let labels = phases
            .map(daemon_node_status_publish_phase_name)
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(labels.len(), phases.len());
    }

    #[test]
    fn daemon_cluster_join_uses_a_production_semantic_deadline() {
        assert_eq!(
            DAEMON_CLUSTER_CONTROL_IROH_JOIN_TIMEOUT, DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT,
            "transport choice must not change the semantic join budget"
        );
        assert!(
            DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT
                > sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
            "join reconciliation can include authority forwarding and quorum admission after the initial membership RPC"
        );
        assert!(
            DAEMON_CLUSTER_CONTROL_JOIN_TIMEOUT > sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            "a transport no-progress interval must not be the end-to-end join deadline"
        );
    }

    #[test]
    fn request_outcome_fails_closed_without_quorum_despite_local_rejection() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-kv-outcome-no-quorum");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data dir");
        let daemon_paths = DaemonPaths::new(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_kv_outcome_no_quorum").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_kv_outcome_local").expect("local");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "request outcome no quorum",
            local,
            "local",
            "pk-local",
            now,
        );
        for (node_id, name) in [
            (
                NodeId::from_legacy_named("node_daemon_kv_outcome_second").expect("second"),
                "second",
            ),
            (
                NodeId::from_legacy_named("node_daemon_kv_outcome_third").expect("third"),
                "third",
            ),
        ] {
            state.members.push(ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id,
                name,
                format!("pk-{name}"),
                now,
            ));
        }
        for member in &mut state.members {
            let capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities;
        }
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
            .expect("seed cluster raft config");
        let request = sinomo_core::kv::KvCommandRequest::generated(KvCommand::Barrier);
        sinomo_core::kv::mark_cluster_kv_request_rejected_before_commit(
            &cluster_paths.kv_paths(),
            &request,
            "not leader",
        )
        .expect("local rejected outcome");
        let payload = sinomo_core::kv::cluster_kv_request_outcome_rpc_payload(
            &sinomo_core::kv::ClusterKvRequestOutcomeRequest {
                request_id: request.request_id,
            },
        )
        .expect("outcome payload");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let error = runtime
            .block_on(
                handle_daemon_cluster_kv_request_outcome_request_async_on_runtime(
                    &daemon_paths,
                    &payload.body,
                    true,
                    runtime.handle(),
                ),
            )
            .expect_err("local attempt state must not bypass authority");

        assert!(
            matches!(error, DaemonError::ClusterReadOnly { .. }),
            "unexpected request-outcome failure: {error:?}"
        );
    }

    #[test]
    fn routed_raft_requests_have_operation_specific_retry_markers() {
        let append = cluster_raft_append_object_rpc_request(sinomo_core::kv::ClusterKvRpcPayload {
            route: "/test/daemon/raft/append",
            category: "test.cluster.kv",
            body: Vec::new(),
        });
        let vote = cluster_raft_vote_object_rpc_request(sinomo_core::kv::ClusterKvRpcPayload {
            route: "/test/daemon/raft/vote",
            category: "test.cluster.kv",
            body: Vec::new(),
        });
        let general = cluster_kv_object_rpc_request(sinomo_core::kv::ClusterKvRpcPayload {
            route: "/test/daemon/kv",
            category: "test.cluster.kv",
            body: Vec::new(),
        });

        assert!(append.is_response_retry_safe());
        assert!(vote.is_response_retry_safe());
        assert!(!general.is_response_retry_safe());
    }

    fn test_cluster_node_status(
        cluster_id: &ClusterId,
        node_id: &NodeId,
        process: &str,
        commit: Option<&str>,
        observed_at: OffsetDateTime,
    ) -> ClusterNodeStatusRecord {
        ClusterNodeStatusRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            process: process.to_string(),
            availability: ClusterNodeAvailability::Online,
            daemon_version: "0.1.0-test".to_string(),
            commit: commit.map(str::to_string),
            target_os: Some("test-os".to_string()),
            target_arch: Some("test-arch".to_string()),
            hostname: None,
            pid: Some(42),
            address: Some("127.0.0.1:21353".to_string()),
            bind_address: Some("0.0.0.0:21353".to_string()),
            executable: Some("sinomo".to_string()),
            rollout: None,
            daemon_started_at: Some("2026-05-04T00:00:00Z".to_string()),
            observed_at,
        }
    }

    fn test_network_module_relay_endpoint(node_id: &NodeId, key: &str) -> ClusterEndpointRecord {
        let address = format!("iroh://{key}?relay=https://relay.example./");
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
                "sinomo.test.daemon.module.endpoint",
                format!("{}\0{address}", node_id.as_str()),
            ),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address,
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 20,
            last_observed_latency_ms: None,
        }
    }

    #[test]
    fn maintenance_network_module_endpoint_refresh_updates_publish_state() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_module_endpoint_refresh").expect("cluster");
        let local_node = NodeId::from_legacy_named("node_module_endpoint_refresh").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "module endpoint refresh",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == local_node)
            .expect("local member");
        member.endpoints.push(ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_stale_module_refresh",
            )
            .expect("stale endpoint"),
            node_id: local_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://stale_module_refresh_key?direct=203.0.113.9:21356".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        });
        let current = test_network_module_relay_endpoint(&local_node, "module_refresh_key");
        let refreshed_at = now + time::Duration::seconds(1);

        assert!(
            apply_network_module_endpoints_to_state(&mut state, [current.clone()], refreshed_at,)
                .expect("apply active module endpoint")
        );

        let member = state
            .members
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("refreshed local member");
        let module_endpoints = member
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.transport == ClusterTransportKind::Iroh)
            .collect::<Vec<_>>();
        assert_eq!(module_endpoints.len(), 2);
        assert!(module_endpoints.contains(&&current));
        assert_eq!(state.updated_at, refreshed_at);

        let published = sinomo_core::network::build_cluster_network_info_record(
            &state,
            &local_node,
            refreshed_at,
            CLUSTER_NETWORK_INFO_TTL_SECONDS,
        );
        assert!(published.endpoints.contains(&current));
    }

    fn append_cluster_raft_command(
        paths: &ClusterPaths,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
        paths
            .kv_paths()
            .append_kv_command_and_publish(
                node_id,
                command,
                sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .map_err(sinomo_core::network::NetworkError::directory)
    }

    fn seed_cluster_node_status_to_kv(
        paths: &ClusterPaths,
        status: ClusterNodeStatusRecord,
    ) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(paths)?;
        let snapshot = append_cluster_raft_command(
            paths,
            &state.local_node_id,
            KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(
                        sinomo_core::node::cluster_node_status_key_for_record(&status),
                        &status,
                        None,
                    )
                    .map_err(sinomo_core::network::NetworkError::directory)?,
                ],
                failure: Vec::new(),
            }),
        )?;
        sinomo_core::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &snapshot)
            .map_err(sinomo_core::network::NetworkError::directory)?;
        Ok(snapshot)
    }

    #[test]
    fn cluster_owner_runtime_bridge_rejects_current_thread_runtime() {
        let owner_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owner runtime");
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");

        let error = caller_runtime.block_on(async {
            block_on_cluster_network_future_on_owner_runtime(
                owner_runtime.handle(),
                Box::pin(async { Ok::<_, sinomo_core::network::NetworkError>(()) }),
            )
            .expect_err("cluster bridge must not spawn a helper thread")
        });

        match error {
            sinomo_core::network::NetworkError::Io { operation, detail } => {
                assert_eq!(operation, "daemon cluster runtime");
                assert!(detail.contains("owner-runtime bridge"));
                assert!(detail.contains("current-thread Tokio runtime"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn daemon_host_network_error_preserves_auth_classification() {
        assert!(matches!(
            daemon_host_network_error("cluster relay", DaemonError::AuthDenied),
            sinomo_core::network::NetworkError::AuthDenied
        ));
        assert!(matches!(
            daemon_host_network_error(
                "cluster relay",
                DaemonError::ClusterReadOnly {
                    detail: "quorum unavailable".to_string(),
                },
            ),
            sinomo_core::network::NetworkError::ClusterReadOnly { .. }
        ));

        match daemon_host_network_error(
            "cluster relay",
            DaemonError::Network {
                address: "node2:21353".to_string(),
                detail: "timed out".to_string(),
            },
        ) {
            sinomo_core::network::NetworkError::Io { operation, detail } => {
                assert_eq!(operation, "cluster relay node2:21353");
                assert_eq!(detail, "timed out");
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    fn unique_cluster_test_root(name: &str) -> sinomo_testdir::TestDir {
        ensure_cluster_directory_installed();
        sinomo_testdir::TestDir::new(name)
    }

    #[test]
    fn daemon_network_module_node_id_uses_process_specific_identity() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_network_module_node").expect("cluster");
        let node_id =
            NodeId::from_legacy_named("node_daemon_network_module_node").expect("node id");
        let main = DaemonClusterStatus {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            process: sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Voter,
        };
        let mut shadow = main.clone();
        shadow.process = sinomo_core::node::DAEMON_SHADOW_CLUSTER_ROLE.to_string();

        assert_eq!(daemon_network_module_node_id(&main), node_id.clone());
        assert_eq!(
            daemon_network_module_node_id(&shadow),
            shadow_daemon_node_id(&node_id)
        );
    }

    #[test]
    fn daemon_cluster_control_join_migration_rolls_back_source_when_target_join_fails() {
        let root = unique_cluster_test_root("sinomo-daemon-join-migration-rollback");
        let daemon_paths = DaemonPaths::with_root(root.path().join("daemon"));
        let paths = cluster_paths_from_daemon_paths(&daemon_paths).expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let state =
            initialize_local_cluster(&paths, None, None, "migration source".to_string(), now)
                .expect("settled source cluster");
        let source_cluster_id = state.cluster_id.clone();

        // Invite for a DIFFERENT cluster whose only introducer is unreachable:
        // the daemon must take the migration branch (settled source), freeze
        // and quarantine the source, fail the target join, then restore the
        // source storage.
        let target_cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_migration_target").expect("target");
        let invite = sinomo_core::ClusterJoinInvite {
            invite_id: sinomo_core::node::ClusterInviteId::from_legacy_named(
                "invite_daemon_migration_rollback",
            )
            .expect("invite id"),
            cluster_id: target_cluster_id,
            cluster_name: Some("migration target".to_string()),
            cluster_public_key: "pk-migration-target".to_string(),
            invitation_secret: "secret_daemon_migration_rollback".to_string(),
            communication_channels: Vec::new(),
            introducers: vec![sinomo_core::cluster::invite_introducer_endpoint_from_state(
                &state,
                "127.0.0.1:1".to_string(),
            )],
            token_id: String::new(),
            expires_at: OffsetDateTime::UNIX_EPOCH,
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: Vec::new(),
            issuer_node_id: NodeId::from_legacy_named("node_daemon_migration_issuer")
                .expect("issuer"),
            issuer_public_key: String::new(),
            signature: String::new(),
        };
        let uri = sinomo_core::cluster::encode_cluster_join_invite(&invite).expect("invite uri");

        let runtime = daemon_cluster_test_runtime();
        let error = handle_daemon_cluster_control_join_on_runtime(
            &daemon_paths,
            ClusterControlJoinRequest {
                invite: uri,
                node_display_name: None,
                node_role: None,
            },
            runtime.handle(),
        )
        .expect_err("target join through an unreachable introducer must fail");
        assert!(
            error.to_string().contains("introducer"),
            "unexpected migration failure: {error}"
        );

        // Source storage restored by the quarantine rollback.
        let restored = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .expect("restored source state");
        assert_eq!(
            restored.cluster_id, source_cluster_id,
            "rolled-back migration must leave the node in its source cluster"
        );
        // The migration record documents the rollback.
        let migrations_root = paths.root.join("migrations");
        let record_path = std::fs::read_dir(&migrations_root)
            .expect("migrations dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path().join("source").join("migration.json"))
            .find(|path| path.exists())
            .expect("rolled-back migration record");
        let record: sinomo_core::ClusterMigrationRecord =
            serde_json::from_slice(&std::fs::read(&record_path).expect("read migration record"))
                .expect("decode migration record");
        assert_eq!(record.phase, sinomo_core::ClusterMigrationPhase::RolledBack);
        assert_eq!(record.source_cluster_id, source_cluster_id);
    }

    fn run_cluster_network_maintenance_tick_for_test(
        maintenance: &mut ClusterNetworkMaintenance,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Option<ClusterNetworkMaintenancePendingWork>, DaemonError> {
        runtime.block_on(maintenance.tick_async(paths, options, runtime.handle().clone()))
    }

    fn cluster_network_scheduler_maintenance_for_test(now: Instant) -> ClusterNetworkMaintenance {
        cluster_network_maintenance! {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: now,
            last_discovery: now,
            last_probe: now,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
        }
    }

    #[test]
    fn main_raft_startup_and_retry_preserve_peer_committed_pending_entry() {
        use sinomo_core::kv::{
            DurableRaftKvNode, KvCommand, KvOp, KvWalPaths, RaftAppendEntriesRequest,
        };
        for retry in [false, true] {
            let (_root, paths, state) =
                seeded_single_node_cluster_for_publish_cache_test(if retry {
                    "startup_pending_retry"
                } else {
                    "startup_pending_direct"
                });
            let kv_paths = paths.kv_paths();
            let wal_paths = kv_paths.raft_wal_paths(&state.local_node_id);
            let daemon_paths = DaemonPaths::with_root(paths.root.parent().unwrap().join("daemon"));
            let store = kv_paths.raft_store();
            let (snapshot, pending, hard_before) = {
                let mut node = store.open_node(&state.local_node_id).unwrap();
                let term = node.hard_state().current_term;
                node.become_leader(term).unwrap();
                let snapshot = node.save_snapshot().unwrap();
                let pending = node
                    .append_local_command(
                        term,
                        KvCommand::Op(KvOp::Put {
                            key: "/restart/accepted-before-crash".into(),
                            value: b"preserve".to_vec(),
                            lease_id: None,
                        }),
                    )
                    .unwrap();
                (snapshot, pending, node.hard_state().clone())
            };
            let mut peers = Vec::new();
            for name in ["restart_peer_a", "restart_peer_b"] {
                let mut peer = DurableRaftKvNode::open(
                    NodeId::from_legacy_named(name).unwrap(),
                    KvWalPaths::new(paths.root.join(name)),
                )
                .unwrap();
                peer.restore_snapshot(snapshot.clone()).unwrap();
                assert!(
                    peer.append_entries(RaftAppendEntriesRequest {
                        term: pending.term,
                        leader_id: state.local_node_id.clone(),
                        prev_log_index: snapshot.last_included_index,
                        prev_log_term: snapshot.last_included_term,
                        entries: vec![pending.clone()],
                        leader_commit: pending.index,
                    })
                    .unwrap()
                    .success
                );
                peers.push(peer);
            }
            // Both other voters committed the entry, but the old process died
            // before learning that outcome. Its local commit index is behind.
            assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
                &wal_paths
            ));
            if retry {
                let runtime = daemon_cluster_test_runtime();
                let mut maintenance =
                    cluster_network_scheduler_maintenance_for_test(Instant::now());
                drop(
                    runtime
                        .block_on(
                            maintenance
                                .retry_main_local_raft_resident_initialization(&daemon_paths),
                        )
                        .unwrap(),
                );
                assert!(maintenance.local_raft_resident_initialization.is_ready());
            } else {
                assert!(initialize_local_cluster_raft_resident(&daemon_paths).unwrap());
            }
            let mut node = store.open_node(&state.local_node_id).unwrap();
            assert_eq!(
                node.hard_state(),
                &hard_before,
                "startup must preserve term, vote and commit position"
            );
            assert_eq!(
                node.log_entry_at(pending.index),
                Some(&pending),
                "startup must not erase an entry held by another quorum"
            );
            let next = node
                .append_local_command(
                    pending.term,
                    KvCommand::Op(KvOp::Put {
                        key: "/restart/after".into(),
                        value: b"next".to_vec(),
                        lease_id: None,
                    }),
                )
                .unwrap();
            assert_eq!(
                next.index,
                pending.index + 1,
                "never reuse the old term/index"
            );
            for peer in &mut peers {
                assert!(
                    peer.append_entries(RaftAppendEntriesRequest {
                        term: next.term,
                        leader_id: state.local_node_id.clone(),
                        prev_log_index: pending.index,
                        prev_log_term: pending.term,
                        entries: vec![next.clone()],
                        leader_commit: next.index,
                    })
                    .unwrap()
                    .success
                );
            }
            node.commit_to(next.index).unwrap();
            for peer in peers {
                assert_eq!(
                    node.store().try_snapshot().unwrap(),
                    peer.store().try_snapshot().unwrap()
                );
            }
        }
    }

    #[test]
    fn main_raft_resident_initialization_retries_after_predecessor_lock_handoff() {
        let (_root, cluster_paths, state) = seeded_single_node_cluster_for_publish_cache_test(
            "main_raft_resident_initialization_handoff",
        );
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let daemon_paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        let wal_paths = cluster_paths
            .kv_paths()
            .raft_wal_paths(&state.local_node_id);
        assert!(
            sinomo_core::kv::evict_resident_raft_node_for_test(&wal_paths),
            "fixture initialization must release its resident slot before the predecessor opens"
        );

        // Model the predecessor shadow that still owns node.lock while the
        // replacement main initializes its resident Raft owner.
        let predecessor = sinomo_core::kv::DurableRaftKvNode::open(
            state.local_node_id.clone(),
            wal_paths.clone(),
        )
        .expect("predecessor raft holder");
        let startup_error = initialize_local_cluster_raft_resident(&daemon_paths)
            .expect_err("startup initialization must observe the predecessor lock");
        assert!(
            matches!(
                startup_error,
                DaemonError::Parse { ref detail } if detail.contains("raft node is busy")
            ),
            "unexpected startup initialization error: {startup_error}"
        );

        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(Instant::now());
        assert!(matches!(
            &maintenance.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ));
        let retry_error = runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect_err("maintenance worker must observe the retained predecessor lock");
        assert!(
            matches!(
                retry_error,
                DaemonError::Parse { ref detail } if detail.contains("raft node is busy")
            ),
            "unexpected maintenance initialization error: {retry_error}"
        );
        assert!(matches!(
            &maintenance.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ));
        drop(predecessor);

        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("maintenance retry after predecessor handoff");
        assert!(maintenance.local_raft_resident_initialization.is_ready());
        sinomo_core::kv::read_cluster_kv_resident_local_entry(
            &cluster_paths.kv_paths(),
            &state.local_node_id,
            &sinomo_core::kv::cluster_kv_raft_config_key(),
        )
        .expect("readiness local entry uses the initialized resident");

        // Once initialized, later maintenance ticks must not repeat the cold
        // open. Evicting the test slot makes any accidental retry observable.
        assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
            &wal_paths
        ));
        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("completed initialization remains a no-op");
        assert!(
            !sinomo_core::kv::evict_resident_raft_node_for_test(&wal_paths),
            "completed initialization must not re-open the resident on a later tick"
        );
        assert!(matches!(
            sinomo_core::kv::read_cluster_kv_resident_local_entry(
                &cluster_paths.kv_paths(),
                &state.local_node_id,
                &sinomo_core::kv::cluster_kv_raft_config_key(),
            ),
            Err(sinomo_core::kv::KvError::Busy { .. })
        ));
    }

    #[test]
    fn cancelled_main_raft_resident_initialization_retains_one_waiter_and_storage_fence() {
        let (_root, cluster_paths, state) = seeded_single_node_cluster_for_publish_cache_test(
            "main_raft_resident_initialization_cancellation",
        );
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let daemon_paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        let kv_paths = cluster_paths.kv_paths();
        let wal_paths = kv_paths.raft_wal_paths(&state.local_node_id);
        assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
            &wal_paths
        ));

        let operation_guard = sinomo_core::kv::lock_cluster_kv_raft_operation_for_paths(&kv_paths)
            .expect("hold local Raft operation lock");
        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(Instant::now());

        for attempt in 1..=3 {
            runtime.block_on(async {
                let initializer =
                    maintenance.retry_main_local_raft_resident_initialization(&daemon_paths);
                tokio::pin!(initializer);
                let registration_deadline = tokio::time::sleep(Duration::from_secs(1));
                tokio::pin!(registration_deadline);
                loop {
                    if sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths)
                        == 1
                    {
                        break;
                    }
                    tokio::select! {
                        _ = &mut initializer => {
                            panic!("initializer attempt {attempt} completed before registering its retained operation waiter")
                        }
                        _ = &mut registration_deadline => {
                            panic!("initializer attempt {attempt} did not register its retained operation waiter")
                        }
                        _ = tokio::task::yield_now() => {}
                    }
                }
            });
            assert!(matches!(
                &maintenance.local_raft_resident_initialization,
                LocalRaftResidentInitialization::InFlight(_)
            ));
            assert_eq!(
                sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
                1,
                "cancelled retries must rejoin the retained worker instead of accumulating waiters"
            );
        }

        let storage_lock = Arc::clone(&*DAEMON_CLUSTER_STORAGE_MUTATION_LOCK);
        let (storage_acquired_tx, storage_acquired_rx) = std::sync::mpsc::sync_channel(0);
        let storage_contender = std::thread::spawn(move || {
            let guard = storage_lock.blocking_lock_owned();
            storage_acquired_tx
                .send(())
                .expect("report storage lock acquisition");
            drop(guard);
        });
        assert!(
            storage_acquired_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "initializer must retain the storage-mutation fence while it is blocked"
        );

        drop(operation_guard);
        let completion_deadline = Instant::now() + Duration::from_secs(1);
        while !maintenance
            .local_raft_resident_initialization
            .in_flight_task_is_finished()
        {
            assert!(
                Instant::now() < completion_deadline,
                "initializer did not finish after the operation guard was released"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
            0,
            "the retained worker must leave no stale operation waiter after completion"
        );
        assert!(
            storage_acquired_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "the completed task output must retain the storage fence until it is rejoined"
        );

        let storage_guard = runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("rejoin the retained initializer");
        assert!(maintenance.local_raft_resident_initialization.is_ready());
        assert!(
            storage_acquired_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "the resumed tick must receive the same storage fence without a gap"
        );
        drop(storage_guard);
        storage_acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("storage contender acquires only after the resumed tick releases its guard");
        storage_contender.join().expect("storage contender joined");

        sinomo_core::kv::read_cluster_kv_resident_local_entry(
            &kv_paths,
            &state.local_node_id,
            &sinomo_core::kv::cluster_kv_raft_config_key(),
        )
        .expect("resident local reads recover after the retained initializer completes");
    }

    #[test]
    fn timed_out_maintenance_ticks_rejoin_one_initializer_and_charge_one_failure() {
        let (_root, cluster_paths, state) = seeded_single_node_cluster_for_publish_cache_test(
            "main_raft_resident_initialization_tick_timeout",
        );
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let daemon_paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        let kv_paths = cluster_paths.kv_paths();
        let wal_paths = kv_paths.raft_wal_paths(&state.local_node_id);
        assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
            &wal_paths
        ));

        let operation_guard = sinomo_core::kv::lock_cluster_kv_raft_operation_for_paths(&kv_paths)
            .expect("hold local Raft operation lock");
        let runtime = daemon_cluster_test_runtime();
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.last_publish = now
            .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL + Duration::from_secs(1))
            .expect("due publish");
        let options = DaemonRunOptions::default();

        for attempt in 1..=3 {
            runtime.block_on(async {
                let tick =
                    maintenance.tick_async(&daemon_paths, &options, runtime.handle().clone());
                tokio::pin!(tick);
                let registration_deadline = tokio::time::sleep(Duration::from_secs(1));
                tokio::pin!(registration_deadline);
                loop {
                    if sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths)
                        == 1
                    {
                        break;
                    }
                    tokio::select! {
                        _ = &mut tick => {
                            panic!("production-shaped tick attempt {attempt} completed before registering its retained operation waiter")
                        }
                        _ = &mut registration_deadline => {
                            panic!("production-shaped tick attempt {attempt} did not register its retained operation waiter")
                        }
                        _ = tokio::task::yield_now() => {}
                    }
                }
            });
            maintenance.record_active_tick_plan_failure(Instant::now());
            assert_eq!(
                sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
                1,
                "each outer timeout must rejoin, never detach another operation waiter"
            );
            assert_eq!(
                maintenance.publish_failure_streak, 1,
                "only the originally claimed publish generation may be charged"
            );
            assert!(maintenance.active_due_claims.is_empty());
        }

        drop(operation_guard);
        let resumed = runtime
            .block_on(maintenance.tick_async(&daemon_paths, &options, runtime.handle().clone()))
            .expect("completion wake rejoins the retained initializer");
        assert!(resumed.is_none());
        assert!(maintenance.local_raft_resident_initialization.is_ready());
        assert_eq!(
            sinomo_core::kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
            0
        );
        assert_eq!(maintenance.publish_failure_streak, 1);
        assert!(maintenance.active_due_claims.is_empty());
        sinomo_core::kv::read_cluster_kv_resident_local_entry(
            &kv_paths,
            &state.local_node_id,
            &sinomo_core::kv::cluster_kv_raft_config_key(),
        )
        .expect("production-shaped timeout recovery restores resident reads");
    }

    #[test]
    fn successful_startup_raft_resident_initialization_is_propagated() {
        let (_root, cluster_paths, state) = seeded_single_node_cluster_for_publish_cache_test(
            "startup_raft_resident_initialization_propagated",
        );
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let daemon_paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        let wal_paths = cluster_paths
            .kv_paths()
            .raft_wal_paths(&state.local_node_id);

        let startup_initialized = initialize_local_cluster_raft_resident(&daemon_paths)
            .expect("startup initializes the resident voter");
        assert!(startup_initialized);
        assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
            &wal_paths
        ));

        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
            startup_initialized,
        ));
        assert!(maintenance.local_raft_resident_initialization.is_ready());
        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("startup-ready maintenance acquires only the storage fence");
        assert!(
            !sinomo_core::kv::evict_resident_raft_node_for_test(&wal_paths),
            "a propagated startup success must suppress the duplicate maintenance cold-open"
        );
    }

    #[test]
    fn main_raft_resident_initialization_stays_pending_without_readable_state() {
        let root = unique_cluster_test_root("main_raft_resident_initialization_pending_state");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(Instant::now());

        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("missing state keeps initialization pending");
        assert!(matches!(
            &maintenance.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ));

        std::fs::write(&cluster_paths.state, b"not a cluster state object")
            .expect("write unreadable cluster state");
        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&daemon_paths))
            .expect("unreadable state keeps initialization pending");
        assert!(matches!(
            &maintenance.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ));
    }

    #[test]
    fn shadow_raft_resident_initialization_is_a_zero_open_noop() {
        let (_root, cluster_paths, state) = seeded_single_node_cluster_for_publish_cache_test(
            "shadow_raft_resident_initialization_noop",
        );
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        shadow_paths.ensure().expect("shadow paths");
        let wal_paths = cluster_paths
            .kv_paths()
            .raft_wal_paths(&state.local_node_id);
        assert!(sinomo_core::kv::evict_resident_raft_node_for_test(
            &wal_paths
        ));
        deny_shadow_daemon_local_raft_access(&shadow_paths).expect("deny shadow raft ownership");

        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(Instant::now());
        runtime
            .block_on(maintenance.retry_main_local_raft_resident_initialization(&shadow_paths))
            .expect("shadow initialization is skipped");
        assert!(matches!(
            &maintenance.local_raft_resident_initialization,
            LocalRaftResidentInitialization::Pending
        ));
        assert!(
            !sinomo_core::kv::evict_resident_raft_node_for_test(&wal_paths),
            "shadow initialization must not register a resident slot"
        );
        assert!(sinomo_core::kv::process_local_raft_access_is_denied(
            &wal_paths.root
        ));
        assert!(
            sinomo_core::kv::clear_process_local_raft_access_denial_for_test(
                &cluster_paths.kv_paths().raft
            )
        );
    }

    fn pending_join_state_for_test(
        paths: &ClusterPaths,
        cluster_id: ClusterId,
        cluster_name: String,
        local_node_id: NodeId,
        local_node_display_name: String,
        now: OffsetDateTime,
    ) -> ClusterBootstrapState {
        paths.ensure().expect("cluster paths");
        let data_dir = paths.root.parent().expect("cluster data dir");
        let main_identity = load_or_create_main_node_identity(data_dir).expect("main identity");
        let shadow_identity =
            load_or_create_shadow_node_identity(data_dir).expect("shadow identity");
        let mut state = sinomo_core::cluster::prepare_pending_cluster_join_state(
            cluster_id,
            cluster_name,
            local_node_id,
            local_node_display_name,
            main_identity.public_key,
            shadow_identity.public_key,
            now,
        );
        ensure_daemon_cluster_members(
            &mut state,
            &DaemonRunOptions::default(),
            &DaemonRunOptions::shadow_default(),
            now,
        );
        state
    }

    struct CachedHitPublishHost {
        store: InProcessKvStore,
        commits: usize,
        remembered_statuses: usize,
        remembered_connectivity: usize,
    }

    impl CachedHitPublishHost {
        fn from_paths(paths: &ClusterPaths) -> Self {
            let snapshot = sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(paths)
                .expect("seed kv snapshot");
            Self {
                store: InProcessKvStore::restore(snapshot).expect("restore kv store"),
                commits: 0,
                remembered_statuses: 0,
                remembered_connectivity: 0,
            }
        }

        fn commit(&mut self, txn: KvTxn) -> Result<KvSnapshot, sinomo_core::network::NetworkError> {
            self.commits = self.commits.saturating_add(1);
            self.store.txn(txn).map_err(test_kv_network_error)?;
            self.store.try_snapshot().map_err(test_kv_network_error)
        }
    }

    impl sinomo_core::cluster::ClusterNodeStatusPublishHost for CachedHitPublishHost {
        type Error = sinomo_core::network::NetworkError;

        fn published_node_status_matches(
            &mut self,
            _paths: &ClusterPaths,
            _status: &ClusterNodeStatusRecord,
        ) -> bool {
            true
        }

        fn remember_published_node_status(
            &mut self,
            _paths: &ClusterPaths,
            _status: &ClusterNodeStatusRecord,
        ) {
            self.remembered_statuses = self.remembered_statuses.saturating_add(1);
        }

        fn commit_cluster_kv_txn_with_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
            request: sinomo_core::kv::KvTxnRequest,
        ) -> Result<KvSnapshot, Self::Error> {
            self.commit(request.txn)
        }
    }

    impl sinomo_core::cluster::ClusterConnectivityPublishHost for CachedHitPublishHost {
        type Error = sinomo_core::network::NetworkError;

        fn published_connectivity_record_set_matches(
            &mut self,
            _paths: &ClusterPaths,
            _source_node_id: &NodeId,
            _records: &[ClusterConnectivityRecord],
        ) -> bool {
            true
        }

        fn remember_published_connectivity_record_set(
            &mut self,
            _paths: &ClusterPaths,
            _source_node_id: &NodeId,
            _records: &[ClusterConnectivityRecord],
        ) {
            self.remembered_connectivity = self.remembered_connectivity.saturating_add(1);
        }

        fn commit_cluster_kv_txn_with_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
            request: sinomo_core::kv::KvTxnRequest,
        ) -> Result<KvSnapshot, Self::Error> {
            self.commit(request.txn)
        }
    }

    impl sinomo_core::cluster::ClusterConnectivityPublishAsyncHost for CachedHitPublishHost {
        type Error = sinomo_core::network::NetworkError;

        fn published_connectivity_record_set_matches_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _source_node_id: &'a NodeId,
            _records: &'a [ClusterConnectivityRecord],
        ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, bool> {
            Box::pin(async move { Ok(self.remembered_connectivity > 0) })
        }

        fn remember_published_connectivity_record_set_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _source_node_id: &'a NodeId,
            _records: &'a [ClusterConnectivityRecord],
        ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
            Box::pin(async move {
                self.remembered_connectivity = self.remembered_connectivity.saturating_add(1);
                Ok(())
            })
        }

        fn commit_cluster_kv_txn_with_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a mut ClusterBootstrapState,
            request: sinomo_core::kv::KvTxnRequest,
        ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
            Box::pin(async move { self.commit(request.txn) })
        }
    }

    fn test_kv_network_error(
        source: sinomo_core::kv::KvError,
    ) -> sinomo_core::network::NetworkError {
        sinomo_core::network::NetworkError::Io {
            operation: "test kv txn".to_string(),
            detail: source.to_string(),
        }
    }

    fn seeded_single_node_cluster_for_publish_cache_test(
        name: &str,
    ) -> (sinomo_testdir::TestDir, ClusterPaths, ClusterBootstrapState) {
        let root = unique_cluster_test_root(name);
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named(format!("cluster_{name}")).expect("cluster id");
        let node_id = NodeId::from_legacy_named(format!("node_{name}")).expect("node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id, name, node_id, "local", "pk-local", now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        (root, paths, state)
    }

    #[test]
    fn shadow_network_publish_ticks_do_not_append_rollout_barriers() {
        let (_root, cluster_paths, state) =
            seeded_single_node_cluster_for_publish_cache_test("shadow_network_tick_no_barrier");
        let data_dir =
            SinomoDataDir::new(cluster_paths.root.parent().expect("cluster data directory"));
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        shadow_paths.ensure().expect("shadow paths");
        let wal_path = cluster_paths
            .kv_paths()
            .raft_wal_paths(&state.local_node_id)
            .wal;
        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
            false,
        ));
        maintenance.discovery = None;

        for _ in 0..2 {
            maintenance.last_publish = Instant::now()
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .expect("due publish timestamp");
            let wal_len_before = std::fs::metadata(&wal_path).expect("raft WAL").len();

            run_cluster_network_maintenance_tick_for_test(
                &mut maintenance,
                &shadow_paths,
                &DaemonRunOptions::shadow_default(),
                &runtime,
            )
            .expect("shadow network tick");

            assert_eq!(
                std::fs::metadata(&wal_path)
                    .expect("raft WAL after shadow tick")
                    .len(),
                wal_len_before,
                "a generic shadow network publication tick must not append an authoritative rollout barrier"
            );
        }
    }

    #[test]
    fn join_connectivity_handoff_records_locally_without_inline_quorum_publish() {
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("join_connectivity_handoff_is_local");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &state.local_node_id,
            "127.0.0.1:32154".to_string(),
            10,
        );
        state.members[0].endpoints.push(endpoint.clone());
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write endpoint state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync endpoint state to raft");

        remember_successful_join_endpoint_connectivity(&paths, &state, &endpoint);
        let record = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &paths,
            &state.local_node_id,
        )
        .expect("read local join observation")
        .into_iter()
        .find(|record| record.endpoint_id == endpoint.endpoint_id)
        .expect("join observation");
        let key = sinomo_core::cluster::cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        let snapshot = sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(&paths)
            .expect("pre-maintenance connectivity snapshot");
        assert!(
            snapshot.get(&key).is_none(),
            "accepted response-path handoff must not launch a competing quorum commit; the maintenance reactor owns publication"
        );
    }

    #[test]
    fn node_status_publish_cache_hit_does_not_mask_missing_kv_row() {
        let (_root, paths, state) =
            seeded_single_node_cluster_for_publish_cache_test("publish_cache_status_keepup");
        let status = test_cluster_node_status(
            &state.cluster_id,
            &state.local_node_id,
            "daemon-main",
            Some("keepup"),
            OffsetDateTime::now_utc(),
        );
        let mut host = CachedHitPublishHost::from_paths(&paths);

        let snapshot = sinomo_core::cluster::publish_cluster_node_status_to_quorum_with_host(
            &paths,
            status.clone(),
            &mut host,
        )
        .expect("publish status")
        .expect("status commit");

        assert_eq!(host.commits, 1);
        assert_eq!(host.remembered_statuses, 1);
        let key = sinomo_core::node::cluster_node_status_key_for_record(&status);
        assert!(
            snapshot.get(&key).is_some(),
            "status row should be committed"
        );
    }

    #[test]
    fn node_status_publish_cache_hit_does_not_mask_stale_kv_row() {
        let (_root, paths, state) =
            seeded_single_node_cluster_for_publish_cache_test("publish_cache_status_stale");
        let old_status = test_cluster_node_status(
            &state.cluster_id,
            &state.local_node_id,
            "daemon-main",
            Some("old"),
            OffsetDateTime::now_utc(),
        );
        seed_cluster_node_status_to_kv(&paths, old_status).expect("seed old status");

        let new_status = test_cluster_node_status(
            &state.cluster_id,
            &state.local_node_id,
            "daemon-main",
            Some("new"),
            OffsetDateTime::now_utc(),
        );
        let mut host = CachedHitPublishHost::from_paths(&paths);

        let snapshot = sinomo_core::cluster::publish_cluster_node_status_to_quorum_with_host(
            &paths,
            new_status.clone(),
            &mut host,
        )
        .expect("publish status")
        .expect("stale status should be replaced");

        assert_eq!(host.commits, 1);
        let key = sinomo_core::node::cluster_node_status_key_for_record(&new_status);
        let stored = snapshot
            .get(&key)
            .expect("updated status row")
            .decode::<ClusterNodeStatusRecord>()
            .expect("decode status");
        assert_eq!(stored.commit.as_deref(), Some("new"));
    }

    #[test]
    fn connectivity_publish_cache_hit_does_not_mask_missing_kv_rows() {
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("publish_cache_connectivity_keepup");
        let now = OffsetDateTime::now_utc();
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
                "sinomo.test.connectivity.cache",
                state.local_node_id.as_str(),
            ),
            node_id: state.local_node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.1:32153".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        };
        state.members[0].endpoints.push(endpoint.clone());
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.format(&Rfc3339).expect("timestamp")),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut host = CachedHitPublishHost::from_paths(&paths);

        let snapshot = sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_with_host(
            &paths,
            &state.local_node_id,
            vec![record.clone()],
            &mut host,
        )
        .expect("publish connectivity")
        .expect("connectivity commit");

        assert_eq!(host.commits, 1);
        assert_eq!(host.remembered_connectivity, 1);
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        assert!(
            snapshot.get(&key).is_some(),
            "connectivity row should be committed"
        );
    }

    #[test]
    fn connectivity_async_noop_publish_is_read_only_and_warms_cache() {
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("publish_cache_connectivity_noop");
        let now = OffsetDateTime::now_utc();
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
                "sinomo.test.connectivity.noop-cache",
                state.local_node_id.as_str(),
            ),
            node_id: state.local_node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.1:32154".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        };
        state.members[0].endpoints.push(endpoint.clone());
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.format(&Rfc3339).expect("timestamp")),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &state.local_node_id,
            vec![record.clone()],
        )
        .expect("seed connectivity in kv");
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &state.local_node_id,
            std::slice::from_ref(&record),
        )
        .expect("seed local connectivity source generation");
        let seeded = sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(&paths)
            .expect("seeded connectivity snapshot");
        let content_generation =
            sinomo_core::network::__connectivity_store::local_cluster_connectivity_content_generation(
                &paths,
            );
        let source_node_id = state.local_node_id.clone();
        let mut first_refresh = record.clone();
        first_refresh.latency_ms = Some(777);
        first_refresh.observed_at += time::Duration::seconds(1);
        first_refresh.expires_at += time::Duration::seconds(1);
        first_refresh.last_success_at = Some(
            first_refresh
                .observed_at
                .format(&Rfc3339)
                .expect("first refresh timestamp"),
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = CachedHitPublishHost::from_paths(&paths);
        let first = runtime
            .block_on(
                sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_async_with_state_and_host(
                    runtime.handle(),
                    &paths,
                    &source_node_id,
                    vec![first_refresh.clone()],
                    state.clone(),
                    &mut host,
                ),
            )
            .expect("first no-op publish");

        assert!(
            first.is_none(),
            "an unchanged source-owned publication is terminal"
        );
        assert_eq!(
            host.commits, 0,
            "an unchanged publication must not manufacture a barrier transaction"
        );
        assert_eq!(
            host.store.try_snapshot().expect("host snapshot").revision,
            seeded.revision,
            "the read-only publication must leave the authoritative revision unchanged"
        );
        assert_eq!(
            host.remembered_connectivity, 1,
            "verified no-op must warm the process publish cache"
        );
        assert_eq!(
            sinomo_core::network::__connectivity_store::local_cluster_connectivity_content_generation(
                &paths,
            ),
            content_generation,
            "the first repeated healthy generation must not advance local content"
        );
        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &source_node_id,
            )
            .expect("read source after first no-op"),
            vec![record.clone()],
            "the local source projection must retain the published evidence"
        );

        let mut second_refresh = first_refresh;
        second_refresh.latency_ms = Some(1_500);
        second_refresh.observed_at += time::Duration::seconds(1);
        second_refresh.expires_at += time::Duration::seconds(1);
        second_refresh.last_success_at = Some(
            second_refresh
                .observed_at
                .format(&Rfc3339)
                .expect("second refresh timestamp"),
        );

        let second = runtime
            .block_on(
                sinomo_core::cluster::sync_cluster_connectivity_records_to_quorum_async_with_state_and_host(
                    runtime.handle(),
                    &paths,
                    &source_node_id,
                    vec![second_refresh],
                    state,
                    &mut host,
                ),
            )
            .expect("cached no-op publish");

        assert!(
            second.is_none(),
            "a cached unchanged publish remains terminal"
        );
        assert_eq!(
            host.commits, 0,
            "cache refresh must remain derived from source state, not a Raft write"
        );
        assert_eq!(
            host.remembered_connectivity, 2,
            "each verified no-op refreshes the process cache from committed state"
        );
        assert_eq!(
            sinomo_core::network::__connectivity_store::local_cluster_connectivity_content_generation(
                &paths,
            ),
            content_generation,
            "repeated timestamp/latency-only evidence must stay quiet"
        );
        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &source_node_id,
            )
            .expect("read source after repeated no-op"),
            vec![record]
        );
    }

    #[test]
    fn cluster_state_kv_sync_preparation_uses_daemon_status_options() {
        let root = unique_cluster_test_root("cluster-sync-status-options");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        main_paths.ensure().expect("main daemon paths");
        shadow_paths.ensure().expect("shadow daemon paths");

        let main_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_153,
            port_fallback: false,
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_154,
            port_fallback: false,
        };
        write_daemon_status(
            &main_paths,
            &crate::DaemonStatus::new(&main_options, &main_paths, None),
        )
        .expect("write main status");
        write_daemon_status(
            &shadow_paths,
            &crate::DaemonStatus::new(&shadow_options, &shadow_paths, None),
        )
        .expect("write shadow status");

        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_sync_status_options").expect("cluster"),
            "sync status options".to_string(),
            NodeId::from_legacy_named("node_sync_status_options").expect("node"),
            "sync status options".to_string(),
            "main-public-key".to_string(),
            now,
        );
        ensure_daemon_cluster_members(
            &mut state,
            &DaemonRunOptions::default(),
            &DaemonRunOptions::shadow_default(),
            now,
        );

        let runtime = daemon_cluster_test_runtime();
        let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
        sinomo_core::cluster::ClusterMemberMutationHost::prepare_cluster_state_for_kv_sync(
            &mut host,
            &cluster_paths,
            &mut state,
        )
        .expect("prepare membership");

        let main_member = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("main member");
        assert!(
            main_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32153"
                )
        );
        assert!(
            main_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21353")
        );

        assert!(
            main_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32154"
                )
        );
        assert!(
            main_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21354")
        );
    }

    #[test]
    fn local_cluster_state_preparation_uses_daemon_status_options() {
        let root = unique_cluster_test_root("cluster-prepare-status-options");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        main_paths.ensure().expect("main daemon paths");
        shadow_paths.ensure().expect("shadow daemon paths");

        let main_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_153,
            port_fallback: false,
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_154,
            port_fallback: false,
        };
        write_daemon_status(
            &main_paths,
            &crate::DaemonStatus::new(&main_options, &main_paths, None),
        )
        .expect("write main status");
        write_daemon_status(
            &shadow_paths,
            &crate::DaemonStatus::new(&shadow_options, &shadow_paths, None),
        )
        .expect("write shadow status");

        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prepare_status_options").expect("cluster"),
            "prepare status options".to_string(),
            NodeId::from_legacy_named("node_prepare_status_options").expect("node"),
            "prepare status options".to_string(),
            "main-public-key".to_string(),
            now,
        );
        ensure_daemon_cluster_members(
            &mut state,
            &DaemonRunOptions::default(),
            &DaemonRunOptions::shadow_default(),
            now,
        );

        prepare_local_cluster_state(&cluster_paths, &mut state).expect("prepare local state");

        let main_member = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("main member");
        assert!(
            main_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32153"
                )
        );
        assert!(
            main_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21353")
        );

        assert!(
            main_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32154"
                )
        );
        assert!(
            main_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21354")
        );
    }

    #[test]
    fn ensure_daemon_status_cluster_leaves_pending_join_read_only() {
        let root = unique_cluster_test_root("daemon-status-pending-join");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let now = OffsetDateTime::now_utc();
        let pending = pending_join_state_for_test(
            &cluster_paths,
            ClusterId::from_legacy_named("cluster_daemon_status_pending_join").expect("cluster"),
            "pending join".to_string(),
            NodeId::from_legacy_named("node_daemon_status_pending_join").expect("node"),
            "pending".to_string(),
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &pending).expect("write pending");
        let mut status =
            crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, None);
        write_daemon_status(&daemon_paths, &status).expect("write status");

        ensure_daemon_status_cluster(&daemon_paths, &mut status)
            .expect("ensure daemon status cluster");

        assert!(
            status.cluster.is_none(),
            "legacy pending join state must not be advertised as this daemon's live cluster"
        );
        let stored = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("read stored");
        assert_eq!(stored.cluster_id, pending.cluster_id);
        assert!(
            sinomo_core::network::cluster_state_needs_join_registration(&stored),
            "legacy pending join state should remain ignored until an explicit join or leave command"
        );
    }

    struct TestClusterJoinRegistrationHost;

    impl sinomo_core::cluster::ClusterJoinRegistrationHost for TestClusterJoinRegistrationHost {
        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &sinomo_core::network::ObjectRpcRequest,
        ) -> Result<sinomo_core::network::ObjectRpcResponse, sinomo_core::network::NetworkError>
        {
            panic!("forwarded acceptance recorder should not send membership node RPCs")
        }

        fn remember_successful_join_endpoint(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
            endpoint: &ClusterEndpointRecord,
        ) -> Result<(), sinomo_core::network::NetworkError> {
            remember_cluster_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                sinomo_core::network::ClusterConnectivityState::Healthy,
                None,
                None,
            );
            Ok(())
        }
    }

    #[test]
    fn forwarded_join_acceptance_merges_voter_state_without_losing_local_identity() {
        let root = unique_cluster_test_root("daemon-forwarded-join-acceptance");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_forwarded_join").expect("cluster");
        let voter = NodeId::from_legacy_named("node_forwarded_join_voter").expect("voter");
        let bridge = NodeId::from_legacy_named("node_forwarded_join_bridge").expect("bridge");
        let joined = NodeId::from_legacy_named("node_forwarded_join_joined").expect("joined");
        let mut bridge_state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "forwarded join",
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            bridge_state
                .members
                .iter_mut()
                .find(|member| member.node_id == bridge)
                .expect("bridge member"),
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &bridge_state)
            .expect("write bridge state");

        let mut accepted_state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "forwarded join",
            voter,
            "voter",
            "pk-voter",
            now,
        );
        let mut bridge_member = ClusterMemberRecord::local_voter(
            accepted_state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut bridge_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_core::node::upsert_member_record(&mut accepted_state, bridge_member);
        let mut joined_member = ClusterMemberRecord::local_voter(
            accepted_state.cluster_id.clone(),
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_core::node::upsert_member_record(&mut accepted_state, joined_member.clone());
        let response = ClusterJoinRegistrationResponse {
            accepted: true,
            detail: "join registered".to_string(),
            state: accepted_state,
            kv_snapshot: None,
            responder_node_id: None,
        };

        sinomo_core::cluster::record_forwarded_join_registration_acceptance_with_host(
            &cluster_paths,
            &bridge_state,
            &[joined_member],
            &response,
            &mut TestClusterJoinRegistrationHost,
        )
        .expect("record forwarded join");

        let stored = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("read stored");
        assert_eq!(stored.local_node_id, bridge);
        assert!(
            stored.members.iter().any(|member| member.node_id == joined),
            "bridge should learn the member it forwarded to the voter"
        );
    }

    #[test]
    fn identityless_invite_recovers_already_accepted_join_from_snapshot() {
        let now = OffsetDateTime::now_utc();
        let actual_cluster =
            ClusterId::from_legacy_named("cluster_daemon_identityless_join").expect("cluster");
        let pending_cluster =
            ClusterId::new(sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID.to_string())
                .expect("pending cluster");
        let voter =
            NodeId::from_legacy_named("node_daemon_identityless_join_voter").expect("voter");
        let joined =
            NodeId::from_legacy_named("node_daemon_identityless_join_main").expect("joined");
        let joined_shadow = shadow_daemon_node_id(&joined);
        let mut pending = ClusterBootstrapState::pending_join(
            pending_cluster,
            "pending invite",
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        pending
            .members
            .push(ClusterMemberRecord::pending_non_voting(
                pending.cluster_id.clone(),
                joined_shadow.clone(),
                "joined shadow",
                "pk-shadow",
                now,
            ));

        let mut accepted_state = ClusterBootstrapState::initialized_locally(
            actual_cluster.clone(),
            "actual cluster",
            voter.clone(),
            "voter",
            "pk-voter",
            now,
        );
        let mut joined_member = ClusterMemberRecord::local_voter(
            actual_cluster.clone(),
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_core::node::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_core::node::upsert_member_record(
            &mut accepted_state,
            ClusterMemberRecord::local_non_voting(
                actual_cluster.clone(),
                joined_shadow,
                "joined shadow",
                "pk-shadow",
                now,
            ),
        );
        accepted_state.kv_revision = 4;
        accepted_state.updated_at = now;

        let mut store = InProcessKvStore::new();
        store
            .put_serde(
                sinomo_core::node::cluster_identity_key(),
                &sinomo_core::node::ClusterIdentityRecord::from(&accepted_state),
                None,
            )
            .expect("identity");
        for member in &accepted_state.members {
            store
                .put_serde(cluster_member_key(&member.node_id), member, None)
                .expect("member");
        }
        let invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::from_legacy_named("invite_identityless_join")
                .expect("invite"),
            cluster_id: ClusterId::new(sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID.to_string())
                .expect("pending invite cluster"),
            cluster_name: Some("actual cluster".to_string()),
            cluster_public_key: "pk-voter".to_string(),
            invitation_secret: "secret".to_string(),
            communication_channels: Vec::new(),
            introducers: Vec::new(),
            token_id: "token".to_string(),
            expires_at: now + time::Duration::hours(1),
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: NodeId::from_legacy_named("node_identityless_placeholder_issuer")
                .expect("placeholder issuer"),
            issuer_public_key: String::new(),
            signature: String::new(),
        };

        let recovered = sinomo_core::cluster::accepted_cluster_join_state_from_kv_snapshot(
            &pending,
            &invite,
            &store.snapshot().expect("snapshot"),
        )
        .expect("read snapshot")
        .expect("accepted state");
        assert_eq!(recovered.cluster_id, actual_cluster);
        let mut reconciled = pending;
        sinomo_core::cluster::merge_cluster_join_registration(&mut reconciled, recovered);
        assert_eq!(reconciled.cluster_id, actual_cluster);
        assert!(
            !sinomo_core::network::cluster_state_needs_join_registration(&reconciled),
            "snapshot reconciliation should leave the local node accepted"
        );
    }

    #[test]
    fn identityless_invite_requires_snapshot_for_accepted_join_response() {
        let now = OffsetDateTime::now_utc();
        let actual_cluster =
            ClusterId::from_legacy_named("cluster_daemon_identityless_response_requires_snapshot")
                .expect("cluster");
        let pending_cluster =
            ClusterId::new(sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID.to_string())
                .expect("pending cluster");
        let voter =
            NodeId::from_legacy_named("node_daemon_identityless_response_voter").expect("voter");
        let joined =
            NodeId::from_legacy_named("node_daemon_identityless_response_main").expect("joined");
        let joined_shadow = shadow_daemon_node_id(&joined);
        let mut pending = ClusterBootstrapState::pending_join(
            pending_cluster.clone(),
            "pending invite",
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        pending
            .members
            .push(ClusterMemberRecord::pending_non_voting(
                pending_cluster,
                joined_shadow.clone(),
                "joined shadow",
                "pk-shadow",
                now,
            ));

        let mut accepted_state = ClusterBootstrapState::initialized_locally(
            actual_cluster.clone(),
            "actual cluster",
            voter.clone(),
            "voter",
            "pk-voter",
            now,
        );
        let mut joined_member = ClusterMemberRecord::local_voter(
            actual_cluster.clone(),
            joined,
            "joined",
            "pk-joined",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_core::node::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_core::node::upsert_member_record(
            &mut accepted_state,
            ClusterMemberRecord::local_non_voting(
                actual_cluster,
                joined_shadow,
                "joined shadow",
                "pk-shadow",
                now,
            ),
        );
        let invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::from_legacy_named(
                "invite_identityless_response_requires_snapshot",
            )
            .expect("invite"),
            cluster_id: ClusterId::new(sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID.to_string())
                .expect("pending invite cluster"),
            cluster_name: Some("actual cluster".to_string()),
            cluster_public_key: "pk-voter".to_string(),
            invitation_secret: "secret".to_string(),
            communication_channels: Vec::new(),
            introducers: Vec::new(),
            token_id: "token".to_string(),
            expires_at: now + time::Duration::hours(1),
            trust_domain: TrustDomain::new("trust.local").expect("trust"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: voter.clone(),
            issuer_public_key: "pk-voter".to_string(),
            signature: String::new(),
        };
        let response = ClusterJoinRegistrationResponse {
            accepted: true,
            detail: "join registered".to_string(),
            state: accepted_state,
            kv_snapshot: None,
            responder_node_id: None,
        };
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&voter, "node1:21353".to_string(), 0);
        let mut reconciled = pending.clone();

        let reconciliation = sinomo_core::cluster::reconcile_accepted_cluster_join_response(
            &mut reconciled,
            &invite,
            &endpoint,
            response,
        )
        .expect("reconcile response");

        assert!(
            reconciliation.is_none(),
            "compact identityless invites must require a KV snapshot proving membership"
        );
        assert_eq!(reconciled.cluster_id, pending.cluster_id);
        assert!(sinomo_core::network::cluster_state_needs_join_registration(
            &reconciled
        ));
    }

    #[test]
    fn publish_failure_backoff_grows_exponentially_and_resets() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
            false,
        ));
        assert_eq!(
            maintenance.publish_failure_backoff(),
            DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF
        );
        maintenance.publish_failure_streak = 1;
        assert_eq!(
            maintenance.publish_failure_backoff(),
            DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF
        );
        maintenance.publish_failure_streak = 2;
        assert_eq!(
            maintenance.publish_failure_backoff(),
            DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF * 2
        );
        maintenance.publish_failure_streak = 12;
        assert_eq!(
            maintenance.publish_failure_backoff(),
            DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF
        );
        maintenance.record_publish_success();
        assert_eq!(maintenance.publish_failure_streak, 0);
    }

    #[test]
    fn resource_constraints_reduce_only_passive_network_cadence() {
        assert_eq!(
            effective_network_maintenance_role(
                &ClusterNodeRole::Workstation,
                sinomo_device::NodeResourceMode::Constrained,
            ),
            ClusterNodeRole::Mobile
        );
        assert_eq!(
            effective_network_maintenance_role(
                &ClusterNodeRole::Mobile,
                sinomo_device::NodeResourceMode::Restricted,
            ),
            ClusterNodeRole::Sensor
        );

        let now = Instant::now();
        let five_minutes = Duration::from_secs(5 * 60);
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.local_node_role = ClusterNodeRole::Mobile;
        maintenance.last_publish = now.checked_sub(five_minutes).expect("publish timestamp");
        maintenance.last_probe = now.checked_sub(five_minutes).expect("probe timestamp");
        maintenance.set_resource_mode(sinomo_device::NodeResourceMode::Restricted);

        assert_eq!(
            maintenance.next_due_duration(now),
            Duration::from_secs(10 * 60),
            "restricted mobile passive work uses the sensor-grade 15 minute cadence"
        );

        maintenance.request_publish_refresh(now);
        assert_eq!(
            maintenance.next_due_duration(now),
            Duration::ZERO,
            "an explicit dirty edge remains immediate under resource restriction"
        );
    }

    #[test]
    fn due_class_claim_failure_advances_once_and_backs_off_monotonically() {
        let first_claimed_at = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(first_claimed_at);

        maintenance.claim_due_classes(first_claimed_at, true, true, true, false);
        assert_eq!(maintenance.last_publish, first_claimed_at);
        assert_eq!(maintenance.last_probe, first_claimed_at);
        assert_eq!(maintenance.last_discovery, first_claimed_at);
        maintenance.record_active_tick_plan_failure(first_claimed_at);

        assert_eq!(maintenance.publish_failure_streak, 1);
        assert_eq!(maintenance.probe_failure_streak, 1);
        assert_eq!(maintenance.discovery_failure_streak, 1);
        assert_eq!(
            maintenance.next_due_duration(first_claimed_at),
            DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF,
            "a failed claimed generation must not collapse into the outer loop's minimum sleep"
        );

        let second_claimed_at = first_claimed_at
            .checked_add(DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF)
            .expect("second claim");
        maintenance.claim_due_classes(second_claimed_at, true, true, true, false);
        maintenance.record_active_tick_plan_failure(second_claimed_at);

        assert_eq!(maintenance.publish_failure_streak, 2);
        assert_eq!(maintenance.probe_failure_streak, 2);
        assert_eq!(maintenance.discovery_failure_streak, 2);
        assert_eq!(
            maintenance.next_due_duration(second_claimed_at),
            DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF * 2,
            "consecutive failed generations must retain exponential backoff ownership"
        );
    }

    #[test]
    fn tick_decode_failure_leaves_claimed_classes_for_one_scoped_backoff() {
        let runtime = daemon_cluster_test_runtime();
        let root = unique_cluster_test_root("sinomo-daemon-claimed-tick-decode-failure");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        std::fs::write(&cluster_paths.state, b"not-a-cluster-state")
            .expect("write invalid cluster state");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.last_publish = now
            .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
            .expect("due publish");
        maintenance.last_probe = now
            .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
            .expect("due probe");

        let result = runtime.block_on(maintenance.tick_async(
            &daemon_paths,
            &DaemonRunOptions::default(),
            runtime.handle().clone(),
        ));
        assert!(
            result.is_err(),
            "invalid authoritative state must fail planning"
        );

        assert!(maintenance.active_due_claims.publish);
        assert!(maintenance.active_due_claims.probe);
        assert!(!maintenance.active_due_claims.discovery);
        let failure_observed_at = Instant::now();
        maintenance.record_active_tick_plan_failure(failure_observed_at);
        assert!(maintenance.active_due_claims.is_empty());
        assert_eq!(maintenance.publish_failure_streak, 1);
        assert_eq!(maintenance.probe_failure_streak, 1);
        assert!(
            maintenance.next_due_duration(failure_observed_at)
                >= DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF,
            "decode failure must not leave a due timestamp that spins the daemon loop"
        );
    }

    #[test]
    fn claimed_publish_failure_preserves_unrelated_due_class_state() {
        let now = Instant::now();
        let retry_at = now.checked_add(Duration::from_secs(30)).expect("retry");
        let startup_settle_until = now.checked_add(Duration::from_secs(45));
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.publish_failure_streak = 3;
        maintenance.probe_failure_streak = 4;
        maintenance.discovery_failure_streak = 5;
        maintenance.probe_retry_at = Some(retry_at);
        maintenance.discovery_retry_at = Some(retry_at);
        maintenance.connectivity_publish_failure_streak = 6;
        maintenance.connectivity_publish_retry_at = Some(retry_at);
        maintenance.connectivity_force_probe = true;
        maintenance.startup_settle_until = startup_settle_until;
        let last_probe = maintenance.last_probe;
        let last_discovery = maintenance.last_discovery;

        maintenance.claim_due_classes(now, true, false, false, false);
        maintenance.record_active_tick_plan_failure(now);

        assert_eq!(maintenance.publish_failure_streak, 4);
        assert_eq!(maintenance.probe_failure_streak, 4);
        assert_eq!(maintenance.discovery_failure_streak, 5);
        assert_eq!(maintenance.probe_retry_at, Some(retry_at));
        assert_eq!(maintenance.discovery_retry_at, Some(retry_at));
        assert_eq!(maintenance.connectivity_publish_failure_streak, 6);
        assert_eq!(maintenance.connectivity_publish_retry_at, Some(retry_at));
        assert!(maintenance.connectivity_force_probe);
        assert_eq!(maintenance.startup_settle_until, startup_settle_until);
        assert_eq!(maintenance.last_probe, last_probe);
        assert_eq!(maintenance.last_discovery, last_discovery);
    }

    #[test]
    fn completed_discovery_claim_is_not_backed_off_by_later_plan_failure() {
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.discovery_failure_streak = 3;
        maintenance.claim_due_classes(now, true, true, true, false);

        maintenance.complete_discovery_claim();
        maintenance.record_active_tick_plan_failure(now);

        assert_eq!(maintenance.publish_failure_streak, 1);
        assert_eq!(maintenance.probe_failure_streak, 1);
        assert_eq!(
            maintenance.discovery_failure_streak, 0,
            "completion terminalizes discovery before unrelated publish/probe awaits"
        );
        assert_eq!(maintenance.discovery_retry_at, None);
    }

    #[test]
    fn connectivity_publication_claim_failure_preserves_probe_backoff() {
        let now = Instant::now();
        let probe_retry_at = now
            .checked_add(Duration::from_secs(30))
            .expect("probe retry");
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.probe_failure_streak = 4;
        maintenance.probe_retry_at = Some(probe_retry_at);
        maintenance.connectivity_publish_failure_streak = 2;
        maintenance.connectivity_publish_retry_at = Some(now);

        maintenance.claim_due_classes(now, false, false, false, true);
        maintenance.record_active_tick_plan_failure(now);

        let connectivity_delay = DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF * 4;
        assert_eq!(maintenance.connectivity_publish_failure_streak, 3);
        assert_eq!(
            maintenance.connectivity_publish_retry_at,
            now.checked_add(connectivity_delay)
        );
        assert_eq!(maintenance.probe_failure_streak, 4);
        assert_eq!(maintenance.probe_retry_at, Some(probe_retry_at));
        assert_eq!(
            maintenance.next_due_duration(now),
            connectivity_delay,
            "the exact connectivity deadline must not wait for unrelated probe backoff"
        );
    }

    #[test]
    fn expired_pending_batch_counts_one_failure_for_its_publish_generation() {
        let runtime = daemon_cluster_test_runtime();
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.publish_failure_streak = 2;
        maintenance.probe_failure_streak = 4;
        maintenance.discovery_failure_streak = 5;
        let paths = DaemonPaths::with_root(
            unique_cluster_test_root("sinomo-daemon-expired-publish-batch").join("daemon"),
        );
        let pending = ClusterNetworkMaintenancePendingWork {
            publish_claimed: true,
            node_status_refreshes: vec![
                ClusterNodeStatusPendingRefresh {
                    paths: paths.clone(),
                    label: "first",
                },
                ClusterNodeStatusPendingRefresh {
                    paths: paths.clone(),
                    label: "second",
                },
                ClusterNodeStatusPendingRefresh {
                    paths,
                    label: "third",
                },
            ],
            ..Default::default()
        };
        let expired = now.checked_sub(Duration::from_millis(1)).unwrap_or(now);

        runtime.block_on(maintenance.run_pending_work_until_async(
            pending,
            runtime.handle().clone(),
            expired,
        ));

        assert_eq!(
            maintenance.publish_failure_streak, 3,
            "every queued item belongs to one claimed publish generation"
        );
        assert_eq!(maintenance.probe_failure_streak, 4);
        assert_eq!(maintenance.discovery_failure_streak, 5);
    }

    #[test]
    fn connectivity_publication_cancellation_retains_exact_warmed_relay_generation() {
        let runtime = daemon_cluster_test_runtime();
        let (_root, cluster_paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("connectivity_retry_owner");
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_connectivity_retry_owner_remote").expect("remote");
        let endpoint =
            test_network_module_relay_endpoint(&remote_node, "connectivity_retry_owner_remote_key");
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote_node.clone(),
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write two-node state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync two-node state");
        let mut record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(
                observed_now
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string()),
            ),
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        // Exercise a warmed relayed route close enough to expiry that
        // stable-record publication must not be skipped as a cache hit.
        record.expires_at = observed_now + time::Duration::seconds(30);
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &state.local_node_id,
            std::slice::from_ref(&record),
        )
        .expect("persist source generation");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.probe_failure_streak = 4;
        let probe_retry_at = now
            .checked_add(Duration::from_secs(30))
            .expect("probe retry");
        maintenance.probe_retry_at = Some(probe_retry_at);
        maintenance.connectivity_publish_failure_streak = 2;
        let pending = ClusterNetworkMaintenancePendingWork {
            connectivity_publish_claimed: true,
            connectivity_publish: Some(ClusterConnectivityPendingPublish {
                paths: cluster_paths.clone(),
                source_node_id: state.local_node_id.clone(),
                state: state.clone(),
                records: None,
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&cluster_paths)
                    .expect("capture local connectivity store"),
            }),
            ..Default::default()
        };
        let expired = now.checked_sub(Duration::from_millis(1)).unwrap_or(now);

        runtime.block_on(maintenance.run_pending_work_until_async(
            pending,
            runtime.handle().clone(),
            expired,
        ));

        assert_eq!(maintenance.connectivity_publish_failure_streak, 3);
        assert!(maintenance.connectivity_publish_retry_at.is_some());
        let retained = maintenance
            .retained_connectivity_publish
            .as_ref()
            .expect("cancelled publication retains exact generation");
        assert_eq!(
            retained.records.as_ref().map(|records| records.as_slice()),
            Some(std::slice::from_ref(&record)),
            "the cancelable remote phase must not lose or reconstruct the committed source set"
        );
        assert_eq!(
            maintenance.probe_failure_streak, 4,
            "retained-record publication has an exact retry owner and must not also charge probe backoff"
        );
        assert_eq!(maintenance.probe_retry_at, Some(probe_retry_at));

        let warmed_retry = maintenance
            .plan_connectivity_publish(&cluster_paths, &state)
            .expect("plan warmed connectivity retry");
        assert_eq!(
            warmed_retry
                .records
                .as_ref()
                .map(|records| records.as_slice()),
            Some(std::slice::from_ref(&record)),
            "an unchanged durable generation should retry without a cold re-probe or lossy rebuild"
        );
        runtime.block_on(maintenance.run_pending_work_async(
            ClusterNetworkMaintenancePendingWork {
                connectivity_publish_claimed: true,
                connectivity_publish: Some(warmed_retry),
                ..Default::default()
            },
            runtime.handle().clone(),
        ));
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert!(maintenance.retained_connectivity_publish.is_none());
        assert!(
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&cluster_paths)
                .expect("read warmed KV graph")
                .iter()
                .any(|published| {
                    published.source_node_id == state.local_node_id
                        && published.target_node_id == remote_node
                        && published.endpoint_id == endpoint.endpoint_id
                }),
            "the idempotent warmed retry must publish the still-live near-expiry record"
        );
    }

    #[test]
    fn connectivity_publication_success_cannot_retire_newer_reactive_or_durable_inputs() {
        let (_root, paths, state) =
            seeded_single_node_cluster_for_publish_cache_test("connectivity_input_fence");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.connectivity_converged = true;
        maintenance.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: state.local_node_id.clone(),
            state: state.clone(),
            records: Some(Arc::new(Vec::new())),
            input_generation: maintenance.connectivity_input_generation,
            local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                .expect("capture local connectivity store"),
        });

        maintenance.request_connectivity_refresh(now);
        maintenance.record_connectivity_publish_success();
        assert!(!maintenance.connectivity_converged);
        assert!(maintenance.connectivity_force_probe);

        // A Raft-applied membership generation may be observed without this
        // process being the join handler. The durable target snapshot is the
        // second fence, so correctness never depends on receiving that wake.
        maintenance.connectivity_force_probe = false;
        maintenance.connectivity_converged = true;
        let stale_input_generation = maintenance.connectivity_input_generation;
        maintenance.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: state.local_node_id.clone(),
            state: state.clone(),
            records: Some(Arc::new(Vec::new())),
            input_generation: stale_input_generation,
            local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                .expect("capture local connectivity store"),
        });
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_connectivity_input_fence_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21353".to_string(),
            10,
        );
        let mut current_state = state;
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            current_state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        current_state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &current_state)
            .expect("write advanced membership");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &current_state)
            .expect("sync advanced membership");

        maintenance.record_connectivity_publish_success();
        assert!(!maintenance.connectivity_converged);
        assert!(maintenance.connectivity_force_probe);
    }

    #[test]
    fn stable_command_observation_does_not_supersede_published_probe_generation() {
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("connectivity_stable_command_fence");
        let observed_at = OffsetDateTime::now_utc();
        let remote_node = NodeId::from_legacy_named("node_connectivity_stable_command_remote")
            .expect("remote node");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_at,
        );
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write topology");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync topology");

        let initial = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(observed_at.format(&Rfc3339).expect("timestamp")),
            None,
            observed_at,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &state.local_node_id,
            std::slice::from_ref(&initial),
        )
        .expect("write initial probe generation");
        let snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture initial probe generation");
        let content_generation =
            sinomo_core::network::__connectivity_store::local_cluster_connectivity_content_generation(
                &paths,
            );
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.connectivity_converged = true;
        maintenance.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: state.local_node_id.clone(),
            state: state.clone(),
            records: Some(Arc::new(vec![initial.clone()])),
            input_generation: maintenance.connectivity_input_generation,
            local_connectivity_snapshot: snapshot.clone(),
        });

        let command_observation =
            sinomo_core::network::__connectivity_store::record_daemon_owned_local_cluster_endpoint_command_connectivity(
                &paths,
                &state,
                &endpoint,
                ClusterConnectivityState::Healthy,
                Some(777),
                true,
            )
            .expect("record stable command observation");
        assert_ne!(
            command_observation.observed_at, initial.observed_at,
            "the command path should have produced a genuinely newer observation"
        );
        assert_eq!(
            sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                .expect("capture after stable command observation"),
            snapshot,
            "a timestamp/latency-only command observation must not supersede the durable probe generation"
        );
        assert_eq!(
            sinomo_core::network::__connectivity_store::local_cluster_connectivity_content_generation(
                &paths,
            ),
            content_generation,
            "stable command observations must not manufacture a content wake"
        );
        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &state.local_node_id,
            )
            .expect("read retained source generation"),
            vec![initial]
        );

        maintenance.record_connectivity_publish_success();
        assert!(maintenance.connectivity_converged);
        assert!(!maintenance.connectivity_force_probe);
        assert!(maintenance.retained_connectivity_publish.is_none());
    }

    #[test]
    fn connectivity_latency_only_refresh_does_not_supersede_probe_inputs() {
        let (_root, paths, mut expected_state) =
            seeded_single_node_cluster_for_publish_cache_test("connectivity_latency_fence");
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_connectivity_latency_fence_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            expected_state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        expected_state.members.push(remote_member);
        let mut current_state = expected_state.clone();
        current_state.members[1].endpoints[0].last_observed_latency_ms = Some(7);
        sinomo_core::cluster::write_cluster_state(&paths, &current_state)
            .expect("write latency-refreshed state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &current_state)
            .expect("sync latency-refreshed state");

        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.connectivity_converged = true;
        maintenance.retained_connectivity_publish = Some(ClusterConnectivityPendingPublish {
            paths: paths.clone(),
            source_node_id: expected_state.local_node_id.clone(),
            state: expected_state,
            records: Some(Arc::new(Vec::new())),
            input_generation: maintenance.connectivity_input_generation,
            local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                .expect("capture local connectivity store"),
        });

        maintenance.record_connectivity_publish_success();

        assert!(maintenance.connectivity_converged);
        assert!(!maintenance.connectivity_force_probe);
        assert!(maintenance.retained_connectivity_publish.is_none());
    }

    #[test]
    fn stale_probe_topology_cannot_prune_new_join_observation() {
        let runtime = daemon_cluster_test_runtime();
        let (_root, paths, stale_state) =
            seeded_single_node_cluster_for_publish_cache_test("stale_probe_join_edge");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        let expected_store_snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture local connectivity store");
        let plan = ClusterConnectivityProbePlan {
            active_keys: Vec::new(),
            entries: Vec::new(),
            remaining_due: 0,
            convergence_active: true,
            force_probe: true,
            timestamp: "stale probe".to_string(),
            now: OffsetDateTime::now_utc(),
            startup_settle_active: false,
        };
        let pending = ClusterNetworkMaintenancePendingWork {
            probe_claimed: true,
            connectivity_probe: Some(ClusterConnectivityPendingProbe {
                paths: paths.clone(),
                source_node_id: stale_state.local_node_id.clone(),
                state: stale_state.clone(),
                plan,
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: expected_store_snapshot,
            }),
            ..Default::default()
        };

        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_stale_probe_join_edge_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21355".to_string(),
            10,
        );
        let mut current_state = stale_state;
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            current_state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        current_state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &current_state)
            .expect("write advanced join topology");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &current_state)
            .expect("sync advanced join topology");
        let join_record = sinomo_core::network::cluster_connectivity_record(
            &current_state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            None,
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_records(
            &paths,
            &current_state.local_node_id,
            std::slice::from_ref(&join_record),
        )
        .expect("commit newer join observation");

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &current_state.local_node_id,
            )
            .expect("read preserved join observation"),
            vec![join_record]
        );
        assert!(maintenance.connectivity_force_probe);
        assert_eq!(maintenance.probe_failure_streak, 0);
    }

    #[test]
    fn superseded_probe_retries_observation_instead_of_settling_cached_health() {
        let runtime = daemon_cluster_test_runtime();
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("superseded_probe");
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_superseded_probe_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21356".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write topology");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync topology");
        let expected_store_snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture local connectivity store");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        let failure_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Unreachable,
            Some(1),
            3,
            None,
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            std::slice::from_ref(&failure_record),
        )
        .expect("commit concurrent source generation");

        let key = cluster_connectivity_probe_key(&state.local_node_id, &endpoint);
        let mut cached_healthy = failure_record.clone();
        cached_healthy.state = ClusterConnectivityState::Healthy;
        cached_healthy.consecutive_failures = 0;
        maintenance.connectivity.insert(
            key.clone(),
            ClusterConnectivityProbeState {
                record: cached_healthy,
            },
        );
        let pending = ClusterNetworkMaintenancePendingWork {
            probe_claimed: true,
            connectivity_probe: Some(ClusterConnectivityPendingProbe {
                paths: paths.clone(),
                source_node_id: state.local_node_id.clone(),
                state: state.clone(),
                plan: ClusterConnectivityProbePlan {
                    active_keys: vec![key],
                    entries: Vec::new(),
                    remaining_due: 0,
                    convergence_active: true,
                    force_probe: false,
                    timestamp: observed_now.format(&Rfc3339).unwrap(),
                    now: observed_now,
                    startup_settle_active: false,
                },
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: expected_store_snapshot,
            }),
            ..Default::default()
        };

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &state.local_node_id,
            )
            .expect("read preserved source generation"),
            vec![failure_record.clone()]
        );
        assert!(maintenance.connectivity_force_probe);
        assert!(!maintenance.connectivity_converged);
        assert!(maintenance.connectivity.is_empty());
        assert!(maintenance.retained_connectivity_publish.is_none());
        assert_eq!(maintenance.probe_failure_streak, 0);
        let retry = maintenance.select_connectivity_probe_plan(&paths, &state);
        assert!(retry.entries.iter().any(|entry| entry.endpoint == endpoint));
    }

    #[test]
    fn stale_empty_cached_publish_cannot_clear_concurrent_source_generation() {
        let runtime = daemon_cluster_test_runtime();
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("stale_empty_publish");
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_stale_empty_publish_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21356".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write topology");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync topology");
        let expected_store_snapshot = sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture local connectivity store");
        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        let pending = ClusterNetworkMaintenancePendingWork {
            connectivity_publish_claimed: true,
            connectivity_publish: Some(ClusterConnectivityPendingPublish {
                paths: paths.clone(),
                source_node_id: state.local_node_id.clone(),
                state: state.clone(),
                records: Some(Arc::new(Vec::new())),
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: expected_store_snapshot,
            }),
            ..Default::default()
        };
        let join_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            None,
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            std::slice::from_ref(&join_record),
        )
        .expect("commit concurrent source generation");

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &state.local_node_id,
            )
            .expect("read preserved source generation"),
            vec![join_record.clone()]
        );
        let retained = maintenance
            .retained_connectivity_publish
            .as_ref()
            .expect("newer source generation owns publication retry");
        assert_eq!(
            retained.records.as_deref().map(Vec::as_slice),
            Some(std::slice::from_ref(&join_record))
        );
        assert!(!maintenance.connectivity_force_probe);
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert!(maintenance.connectivity_publish_retry_at.is_some());
    }

    #[test]
    fn empty_connectivity_generation_clears_stale_quorum_records() {
        let runtime = daemon_cluster_test_runtime();
        let (_root, paths, mut state) =
            seeded_single_node_cluster_for_publish_cache_test("connectivity_empty_generation");
        let observed_now = OffsetDateTime::now_utc();
        let remote_node =
            NodeId::from_legacy_named("node_connectivity_empty_generation_remote").expect("remote");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:21354".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write two-node state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync two-node state");
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            None,
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &state.local_node_id,
            vec![record],
        )
        .expect("seed stale quorum connectivity");
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &state.local_node_id,
            &[],
        )
        .expect("commit empty durable source generation");

        let now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        runtime.block_on(maintenance.run_pending_work_async(
            ClusterNetworkMaintenancePendingWork {
                connectivity_publish_claimed: true,
                connectivity_publish: Some(ClusterConnectivityPendingPublish {
                    paths: paths.clone(),
                    source_node_id: state.local_node_id.clone(),
                    state: state.clone(),
                    records: None,
                    input_generation: maintenance.connectivity_input_generation,
                    local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                        .expect("capture local connectivity store"),
                }),
                ..Default::default()
            },
            runtime.handle().clone(),
        ));

        assert!(maintenance.retained_connectivity_publish.is_none());
        assert!(
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&paths)
                .expect("read cleared KV graph")
                .iter()
                .all(|published| published.source_node_id != state.local_node_id),
            "an empty exact source generation must delete stale quorum records"
        );
    }

    #[test]
    fn completed_probe_generation_is_durable_before_quorum_cancellation() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blackhole");
        listener
            .set_nonblocking(true)
            .expect("nonblocking blackhole");
        let blackhole_address = listener.local_addr().expect("blackhole address");
        let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blackhole = std::thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = accepted_tx.send(());
                        let _ = release_rx.recv_timeout(Duration::from_secs(5));
                        drop(stream);
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if release_rx.try_recv().is_ok() {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("blackhole accept failed: {error}"),
                }
            }
        });

        let runtime = daemon_cluster_test_runtime();
        let root = unique_cluster_test_root("sinomo-daemon-probe-durable-before-cancel");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let observed_now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_probe_durable_before_cancel").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_probe_durable_before_cancel_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_probe_durable_before_cancel_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "probe durable before cancel",
            local_node.clone(),
            "local",
            "pk-local",
            observed_now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            blackhole_address.to_string(),
            10,
        );
        let remote_public_key = sinomo_core::network::daemon_auth_encode_public_key(&[7_u8; 32]);
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            remote_public_key,
            observed_now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(
                observed_now
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string()),
            ),
            None,
            observed_now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let key = cluster_connectivity_probe_key(&local_node, &endpoint);
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(maintenance_now);
        maintenance.connectivity.insert(
            key.clone(),
            ClusterConnectivityProbeState {
                record: record.clone(),
            },
        );
        let plan = ClusterConnectivityProbePlan {
            active_keys: vec![key],
            entries: Vec::new(),
            remaining_due: 0,
            convergence_active: true,
            force_probe: true,
            timestamp: observed_now
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            now: observed_now,
            startup_settle_active: false,
        };
        let pending = ClusterNetworkMaintenancePendingWork {
            probe_claimed: true,
            connectivity_probe: Some(ClusterConnectivityPendingProbe {
                paths: paths.clone(),
                source_node_id: local_node.clone(),
                state,
                plan,
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&paths)
                    .expect("capture local connectivity store"),
            }),
            ..Default::default()
        };

        runtime.block_on(maintenance.run_pending_work_until_async(
            pending,
            runtime.handle().clone(),
            Instant::now() + Duration::from_millis(250),
        ));
        let publication_reached_remote = accepted_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let _ = release_tx.send(());
        blackhole.join().expect("blackhole thread");

        assert!(
            publication_reached_remote,
            "the test must cancel the quorum phase after local probe persistence"
        );
        assert_eq!(maintenance.probe_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_failure_streak, 1);
        assert!(maintenance.connectivity_publish_retry_at.is_some());
        assert_eq!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &local_node,
            )
            .expect("read durable source generation"),
            vec![record.clone()],
            "a completed observation must survive cancellation of its derived quorum projection"
        );
        assert_eq!(
            maintenance
                .retained_connectivity_publish
                .as_ref()
                .and_then(|retained| retained.records.as_ref())
                .map(|records| records.as_slice()),
            Some(std::slice::from_ref(&record)),
            "the exact durable generation must remain owned by the independent publication retry"
        );
    }

    #[test]
    fn successful_publish_generation_does_not_clear_other_class_failures() {
        let runtime = daemon_cluster_test_runtime();
        let now = Instant::now();
        let retry_at = now.checked_add(Duration::from_secs(30)).expect("retry");
        let mut maintenance = cluster_network_scheduler_maintenance_for_test(now);
        maintenance.publish_failure_streak = 3;
        maintenance.probe_failure_streak = 4;
        maintenance.discovery_failure_streak = 5;
        maintenance.probe_retry_at = Some(retry_at);
        maintenance.discovery_retry_at = Some(retry_at);

        runtime.block_on(maintenance.run_pending_work_async(
            ClusterNetworkMaintenancePendingWork {
                publish_claimed: true,
                ..Default::default()
            },
            runtime.handle().clone(),
        ));

        assert_eq!(maintenance.publish_failure_streak, 0);
        assert_eq!(maintenance.probe_failure_streak, 4);
        assert_eq!(maintenance.discovery_failure_streak, 5);
        assert_eq!(maintenance.probe_retry_at, Some(retry_at));
        assert_eq!(maintenance.discovery_retry_at, Some(retry_at));
    }

    fn desired_rollout_status_for_test(
        status: &str,
        phase: &str,
        updated_at: OffsetDateTime,
    ) -> ClusterNodeRolloutStatus {
        ClusterNodeRolloutStatus {
            status: status.to_string(),
            mode: Some("cluster-remote-build".to_string()),
            detail: None,
            phase: Some(phase.to_string()),
            progress_current: Some(2),
            progress_total: Some(5),
            progress_message: Some(phase.to_string()),
            source: Some("source".to_string()),
            requested_at: Some("2026-07-07T00:00:00Z".to_string()),
            staged_at: None,
            updated_at: Some(updated_at.format(&Rfc3339).expect("updated timestamp")),
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        }
    }

    #[test]
    fn desired_rollout_reconcile_retries_missing_or_stale_progress() {
        let now = OffsetDateTime::now_utc();
        assert_eq!(
            desired_rollout_reconcile_decision(
                None,
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Retry
        );

        let fresh = desired_rollout_status_for_test("in-progress", "building", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&fresh),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Wait
        );

        let stale = desired_rollout_status_for_test(
            "in-progress",
            "building",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&stale),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Retry
        );

        let mut fresh_main_staged =
            desired_rollout_status_for_test("main-staged", "main-staged", now);
        fresh_main_staged.updated_at = None;
        fresh_main_staged.staged_at = Some(now.format(&Rfc3339).expect("staged timestamp"));
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&fresh_main_staged),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Wait
        );

        let mut fresh_main_staged_with_stale_update =
            desired_rollout_status_for_test("main-staged", "main-staged", now);
        fresh_main_staged_with_stale_update.updated_at = Some(
            (now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1))
                .format(&Rfc3339)
                .expect("updated timestamp"),
        );
        fresh_main_staged_with_stale_update.staged_at =
            Some(now.format(&Rfc3339).expect("staged timestamp"));
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&fresh_main_staged_with_stale_update),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Wait
        );

        let mut stale_main_staged = desired_rollout_status_for_test(
            "main-staged",
            "main-staged",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        stale_main_staged.updated_at = None;
        stale_main_staged.staged_at = Some(
            (now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1))
                .format(&Rfc3339)
                .expect("staged timestamp"),
        );
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&stale_main_staged),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );
    }

    #[test]
    fn desired_rollout_reconcile_stops_on_complete_or_hard_failure() {
        let now = OffsetDateTime::now_utc();
        let accepted = desired_rollout_status_for_test("accepted", "shadow-finalized", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&accepted),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Complete
        );

        let rejected = desired_rollout_status_for_test("rejected", "build-failed", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&rejected),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );

        let source_rejected = desired_rollout_status_for_test("rejected", "source-rejected", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&source_rejected),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );

        let failed = desired_rollout_status_for_test("failed", "activation-failed", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&failed),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );

        let rolled_back = desired_rollout_status_for_test("rolled-back", "shadow-finalized", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&rolled_back),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );
    }

    #[test]
    fn desired_rollout_reconcile_hard_failure_does_not_block_new_request() {
        let now = OffsetDateTime::now_utc();
        let compile_failure = desired_rollout_status_for_test("rejected", "build-failed", now);

        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&compile_failure),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::TerminalFailure
        );
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&compile_failure),
                Some("cluster-remote-build"),
                Some("2026-07-08T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Retry
        );
    }

    #[test]
    fn desired_rollout_reconcile_retries_stale_recoverable_failure() {
        let now = OffsetDateTime::now_utc();
        let fresh = desired_rollout_status_for_test("failed", "transport-failed", now);
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&fresh),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Wait
        );

        let stale = desired_rollout_status_for_test(
            "failed",
            "transport-failed",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&stale),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Retry
        );

        let stale_unknown_phase = desired_rollout_status_for_test(
            "rejected",
            "control-request-failed",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        assert_eq!(
            desired_rollout_reconcile_decision(
                Some(&stale_unknown_phase),
                Some("cluster-remote-build"),
                Some("2026-07-07T00:00:00Z"),
                now,
            ),
            DesiredRolloutReconcileDecision::Retry
        );
    }

    #[test]
    fn desired_rollout_stale_handoff_status_is_hard_failure() {
        let now = OffsetDateTime::now_utc();
        let mut staged = desired_rollout_status_for_test(
            "main-staged",
            "main-staged",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        staged.updated_at = None;
        staged.staged_at = Some(
            (now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1))
                .format(&Rfc3339)
                .expect("staged timestamp"),
        );

        let failure = desired_rollout_stale_handoff_failure_status(&staged, now);

        assert_eq!(failure.status, "failed");
        assert_eq!(failure.phase.as_deref(), Some("shadow-handoff-failed"));
        assert!(failure.failed_at.is_some());
        assert_eq!(failure.staged_at, staged.staged_at);
        assert!(desired_rollout_failure_is_hard(&failure));
    }

    #[test]
    fn desired_rollout_stale_handoff_is_recorded_to_rollout_state() {
        let root = unique_cluster_test_root("desired-rollout-stale-handoff-record");
        let data_dir = root.join("data");
        let rollout_paths = SelfRolloutPaths::new(&data_dir);
        sinomo_rollout::ensure_rollout_layout(&rollout_paths).expect("rollout layout");
        let now = OffsetDateTime::now_utc();
        let mut staged = desired_rollout_status_for_test(
            "main-staged",
            "main-staged",
            now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1),
        );
        staged.updated_at = None;
        staged.staged_at = Some(
            (now - time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS + 1))
                .format(&Rfc3339)
                .expect("staged timestamp"),
        );
        write_rollout_status(&rollout_paths, &staged).expect("staged status");

        let recorded = record_desired_rollout_stale_handoff_failure_if_needed(
            &rollout_paths,
            &staged,
            Some("cluster-remote-build"),
            Some("2026-07-07T00:00:00Z"),
            now,
        )
        .expect("record handoff failure");
        let status = read_rollout_status(&rollout_paths)
            .expect("read rollout status")
            .expect("rollout status");

        assert!(recorded);
        assert_eq!(status.status, "failed");
        assert_eq!(status.phase.as_deref(), Some("shadow-handoff-failed"));
        assert_eq!(status.requested_at.as_deref(), Some("2026-07-07T00:00:00Z"));
    }

    #[test]
    fn maintenance_failure_backoff_retries_after_configured_delay() {
        let now = Instant::now();
        let last_publish =
            sinomo_core::network::cluster_network_maintenance_last_timestamp_for_next_due(
                now,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
                DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF,
            );
        let before_backoff = now
            .checked_add(
                DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF
                    .checked_sub(Duration::from_secs(1))
                    .expect("backoff duration"),
            )
            .expect("before backoff instant");
        let after_backoff = now
            .checked_add(DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF)
            .expect("after backoff instant");

        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                before_backoff,
                last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "failed publishes should not retry before the configured backoff"
        );
        assert!(
            sinomo_core::network::cluster_network_maintenance_interval_due(
                after_backoff,
                last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "failed publishes should retry at the configured backoff, not backoff plus the publish interval"
        );
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                now,
                after_backoff,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "future deferred timestamps should be safe and not immediately due"
        );
    }

    #[test]
    fn connectivity_publish_failure_uses_independent_backoff() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
            false,
        ));
        maintenance.local_node_role = ClusterNodeRole::Workstation;
        maintenance.last_publish = Instant::now();
        maintenance.last_probe = maintenance.last_publish;

        maintenance.record_connectivity_publish_failure();

        assert_eq!(maintenance.publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_failure_streak, 1);
        let first_retry = maintenance
            .connectivity_publish_retry_at
            .expect("connectivity retry deadline");
        assert!(first_retry > Instant::now());
        assert_eq!(
            maintenance.last_publish, maintenance.last_probe,
            "connectivity failure must not defer unrelated publication or probe timestamps"
        );

        maintenance.record_publish_success();
        assert_eq!(maintenance.connectivity_publish_failure_streak, 1);
        assert_eq!(maintenance.connectivity_publish_retry_at, Some(first_retry));

        maintenance.record_connectivity_publish_failure();
        assert_eq!(maintenance.connectivity_publish_failure_streak, 2);
        assert!(
            maintenance
                .connectivity_publish_retry_at
                .expect("second connectivity retry deadline")
                > first_retry,
            "repeated connectivity publication failures should extend their own backoff"
        );

        maintenance.record_connectivity_publish_success();
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_retry_at, None);
    }

    #[test]
    fn connectivity_publish_backoff_masks_probe_and_convergence_wakes() {
        let now = Instant::now();
        let retry_at = now
            .checked_add(DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF)
            .expect("retry deadline");
        let maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Sensor,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: Some(
                now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION)
                    .expect("settle deadline"),
            ),
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 10,
            connectivity_publish_retry_at: Some(retry_at),
            connectivity_force_probe: false,
            last_publish: now,
            last_discovery: now,
            last_probe: now
                .checked_sub(sinomo_core::network::CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL)
                .expect("overdue probe timestamp"),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let masked = sinomo_core::network::cluster_network_maintenance_due(
            now,
            &maintenance.schedule_with_connectivity_publish_backoff(now),
        );
        assert!(
            masked.probe_due,
            "the lower-level schedule still sees an overdue ordinary probe"
        );
        assert_eq!(
            maintenance.next_due_duration(now),
            DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF,
            "the daemon scheduler must ignore both the ordinary probe and 250ms convergence cadence until the independent retry deadline"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_skips_discovery_without_socket() {
        let now = Instant::now();
        let maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            tick: 7,
            last_publish: now
                .checked_sub(Duration::from_secs(10))
                .expect("publish instant"),
            last_discovery: now
                .checked_sub(Duration::from_secs(29))
                .expect("discovery instant"),
            last_probe: now
                .checked_sub(Duration::from_secs(20))
                .expect("probe instant"),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert_eq!(
            maintenance.next_due_duration(now),
            DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.saturating_sub(Duration::from_secs(20)),
            "without an mDNS socket the next wake should be driven by publish/probe, not discovery"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_uses_role_publish_cadence() {
        let now = Instant::now();
        let maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Mobile,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            tick: 7,
            last_publish: now
                .checked_sub(Duration::from_secs(10))
                .expect("publish instant"),
            last_discovery: now,
            last_probe: now
                .checked_sub(Duration::from_secs(20))
                .expect("probe instant"),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert_eq!(
            maintenance.next_due_duration(now),
            sinomo_core::network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL - Duration::from_secs(20),
            "mobile maintenance should not wake on the workstation publish cadence"
        );
    }

    #[test]
    fn cluster_network_maintenance_mobile_role_skips_work_at_workstation_cadence() {
        let root = unique_cluster_test_root("sinomo-daemon-mobile-maintenance-idle-cadence");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Mobile,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .expect("publish instant"),
            last_discovery: maintenance_now,
            last_probe: maintenance_now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .expect("probe instant"),
            idle_stats: idle_stats.clone(),
        };
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let runtime = daemon_cluster_test_runtime();

        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("mobile idle tick");

        let snapshot = idle_stats.snapshot();
        assert_eq!(snapshot.ticks_total, 1);
        assert_eq!(
            snapshot.ticks_with_work, 0,
            "mobile maintenance should not do cluster work at the workstation cadence"
        );
        assert_eq!(
            snapshot.disk_reads_cluster_state, 0,
            "mobile maintenance should not read cluster state before its role-aware interval"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_reports_overdue_immediately() {
        let now = Instant::now();
        let maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            tick: 7,
            last_publish: now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL + Duration::from_secs(1))
                .expect("publish instant"),
            last_discovery: now,
            last_probe: now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert_eq!(
            maintenance.next_due_duration(now),
            Duration::ZERO,
            "overdue maintenance should be eligible immediately"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_accelerates_until_connectivity_converges() {
        let now = Instant::now();
        let maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            tick: 7,
            last_publish: now,
            last_discovery: now,
            last_probe: now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert_eq!(
            maintenance.next_due_duration(now),
            DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL,
            "unconverged connectivity should keep startup settling below the one-second gate"
        );
    }

    #[test]
    fn successful_connectivity_convergence_terminalizes_startup_settle_schedule() {
        let now = Instant::now();
        let runtime = daemon_cluster_test_runtime();
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
            false,
        ));
        maintenance.discovery = None;
        maintenance.connectivity_converged = false;
        maintenance.startup_settle_until = now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION);
        maintenance.last_publish = now;
        maintenance.last_probe = now;
        maintenance.last_discovery = now;

        assert_eq!(
            maintenance.next_due_duration(now),
            DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL,
            "active startup convergence should retain its short scheduling cadence"
        );

        maintenance.finish_connectivity_convergence();

        assert!(maintenance.connectivity_converged);
        assert_eq!(
            maintenance.startup_settle_until, None,
            "successful convergence must consume the startup settle deadline"
        );
        assert!(
            maintenance.next_due_duration(now) > DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL,
            "a completed convergence must return to the role-aware steady-state schedule"
        );
    }

    #[test]
    fn cluster_network_maintenance_network_environment_refresh_forces_reactive_tick() {
        let now = Instant::now();
        let retry_at = now.checked_add(Duration::from_secs(30)).expect("retry");
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: Some("old-environment".to_string()),
            local_node_role: ClusterNodeRole::Mobile,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 4,
            probe_failure_streak: 2,
            discovery_failure_streak: 3,
            probe_retry_at: Some(retry_at),
            discovery_retry_at: Some(retry_at),
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 5,
            connectivity_publish_retry_at: Some(retry_at),
            connectivity_force_probe: false,
            tick: 7,
            last_publish: now,
            last_discovery: now,
            last_probe: now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert!(
            !sinomo_core::network::cluster_network_maintenance_due(now, &maintenance.schedule())
                .publish_due,
            "test setup should start from an idle mobile cadence"
        );

        maintenance.request_network_environment_refresh(now);

        let due =
            sinomo_core::network::cluster_network_maintenance_due(now, &maintenance.schedule());
        assert!(
            !due.publish_due,
            "an interface change does not invalidate member/status publication"
        );
        assert!(due.probe_due);
        assert!(!due.discovery_due, "no discovery socket means no mDNS wake");
        assert!(
            !maintenance.connectivity_converged,
            "connectivity should re-enter convergence after a local network change"
        );
        assert_eq!(
            maintenance.environment_id.as_deref(),
            Some("old-environment"),
            "the next maintenance pass needs the previous environment id to detect and clear stale local probes"
        );
        assert_eq!(
            maintenance.next_due_duration(now),
            Duration::ZERO,
            "the event-triggered maintenance tick should be due immediately"
        );
        assert_eq!(maintenance.publish_failure_streak, 4);
        assert_eq!(maintenance.probe_failure_streak, 2);
        assert_eq!(maintenance.discovery_failure_streak, 3);
        assert_eq!(maintenance.connectivity_publish_failure_streak, 5);
        assert_eq!(maintenance.probe_retry_at, Some(now));
        assert_eq!(maintenance.connectivity_publish_retry_at, Some(now));
        assert_eq!(
            maintenance.discovery_retry_at,
            Some(retry_at),
            "connectivity invalidation must not disturb discovery backoff"
        );
    }

    #[test]
    fn cluster_network_maintenance_pending_join_stays_idle() {
        let root = unique_cluster_test_root("sinomo-daemon-pending-join-idle");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let pending = pending_join_state_for_test(
            &cluster_paths,
            ClusterId::from_legacy_named("cluster_pending_join_convergence").expect("cluster"),
            "pending join idle".to_string(),
            NodeId::from_legacy_named("node_pending_join_convergence").expect("node"),
            "pending".to_string(),
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &pending)
            .expect("write pending state");

        let now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: Some(
                now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION)
                    .expect("startup settle deadline"),
            ),
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 3,
            connectivity_publish_retry_at: Some(
                now.checked_sub(Duration::from_millis(1))
                    .expect("expired retry deadline"),
            ),
            connectivity_force_probe: true,
            tick: 7,
            last_publish: now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .expect("publish instant"),
            last_discovery: now,
            last_probe: now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .expect("probe instant"),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };
        let runtime = daemon_cluster_test_runtime();

        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("pending join maintenance tick");

        assert!(
            maintenance.connectivity_converged,
            "legacy pending join state must not keep connectivity convergence active"
        );
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_retry_at, None);
        assert!(!maintenance.connectivity_force_probe);
        assert_eq!(maintenance.startup_settle_until, None);
        assert!(
            maintenance.next_due_duration(Instant::now())
                > DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL,
            "legacy pending join state should not wake on the convergence cadence"
        );
    }

    #[test]
    fn cluster_network_maintenance_policy_throttles_mobile_and_sensor_roles() {
        assert_eq!(
            sinomo_core::network::cluster_network_publish_interval_for_role(
                &ClusterNodeRole::Workstation
            ),
            DAEMON_CLUSTER_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_probe_interval_for_role(
                &ClusterNodeRole::Workstation
            ),
            DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_discovery_interval_for_role(
                &ClusterNodeRole::Workstation
            ),
            DAEMON_CLUSTER_DISCOVERY_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Workstation
            ),
            sinomo_core::network::CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK
        );
        assert_eq!(
            sinomo_core::network::cluster_network_publish_interval_for_role(
                &ClusterNodeRole::Mobile
            ),
            sinomo_core::network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Mobile),
            sinomo_core::network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_discovery_interval_for_role(
                &ClusterNodeRole::Mobile
            ),
            sinomo_core::network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Mobile
            ),
            sinomo_core::network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK
        );
        assert_eq!(
            sinomo_core::network::cluster_network_publish_interval_for_role(
                &ClusterNodeRole::Sensor
            ),
            sinomo_core::network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Sensor),
            sinomo_core::network::CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_network_discovery_interval_for_role(
                &ClusterNodeRole::Sensor
            ),
            sinomo_core::network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_core::network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Sensor
            ),
            sinomo_core::network::CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK
        );
    }

    #[test]
    fn cluster_node_role_update_persists_local_role_for_maintenance_cadence() {
        let root = unique_cluster_test_root("sinomo-daemon-local-role-update");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_local_role_update").expect("cluster");
        let local_node = NodeId::from_legacy_named("node_daemon_local_role_update").expect("node");
        initialize_local_cluster(
            &paths,
            Some(cluster_id),
            Some(local_node.clone()),
            "local role update".to_string(),
            now,
        )
        .expect("initialize cluster");

        let update = update_cluster_node_role(
            &paths,
            local_node.clone(),
            ClusterNodeRole::Mobile,
            now + time::Duration::seconds(1),
        )
        .expect("update node role");

        assert!(update.changed);
        let local_state =
            sinomo_core::cluster::read_cluster_state_without_kv(&paths).expect("read local state");
        let local_member = local_state
            .members
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("local member");
        assert_eq!(
            local_member.node_role,
            ClusterNodeRole::Mobile,
            "role updates must patch local state so maintenance adopts mobile cadence"
        );
    }

    #[test]
    fn cluster_control_role_repairs_missing_kv_member_before_update() {
        let root = unique_cluster_test_root("sinomo-daemon-role-repairs-kv-member");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let now = OffsetDateTime::now_utc();
        let state = initialize_local_cluster(
            &paths,
            Some(
                ClusterId::from_legacy_named("cluster_daemon_role_repairs_kv_member")
                    .expect("cluster"),
            ),
            Some(NodeId::from_legacy_named("node_daemon_role_repairs_kv_member").expect("node")),
            "role repair".to_string(),
            now,
        )
        .expect("initialize cluster");
        let local_node = state.local_node_id.clone();
        let mut commit_state = state.clone();
        commit_cluster_kv_txn_with_quorum(
            &paths,
            &mut commit_state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Delete {
                    key: cluster_member_key(&local_node),
                }],
                failure: Vec::new(),
            },
        )
        .expect("remove legacy-missing member row");

        let runtime = daemon_cluster_test_runtime();
        let update = handle_daemon_cluster_control_role_with_authority_on_runtime(
            &daemon_paths,
            ClusterControlRoleRequest {
                node_id: local_node.clone(),
                role: ClusterNodeRole::Mobile,
            },
            runtime.handle(),
            None,
        )
        .expect("repair member and update role");

        assert!(update.changed);
        assert_eq!(update.role, ClusterNodeRole::Mobile);
        let snapshot = sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            local_node.clone(),
        )
        .expect("read repaired snapshot");
        let member = snapshot
            .get(&cluster_member_key(&local_node))
            .expect("repaired member row")
            .decode::<ClusterMemberRecord>()
            .expect("decode repaired member");
        assert_eq!(member.node_role, ClusterNodeRole::Mobile);
    }

    #[test]
    fn daemon_cluster_publish_interval_tracks_local_node_role() {
        let root = unique_cluster_test_root("sinomo-daemon-role-publish-interval");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_role_publish_interval").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_role_publish_interval").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "role publish interval",
            local,
            "local",
            "pk-local",
            now,
        );
        state.members[0].node_role = ClusterNodeRole::Mobile;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write mobile state");
        let cluster = daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("cluster status");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        assert_eq!(
            daemon_cluster_publish_interval(&daemon_paths),
            sinomo_core::network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );

        state.members[0].node_role = ClusterNodeRole::Sensor;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write sensor state");
        assert_eq!(
            daemon_cluster_publish_interval(&daemon_paths),
            sinomo_core::network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
    }

    #[test]
    fn stable_discovery_announcements_back_off_but_state_changes_force_send() {
        assert!(
            sinomo_core::network::cluster_network_stable_discovery_announcement_due(1, false),
            "fresh daemons should announce immediately"
        );
        assert!(
            !sinomo_core::network::cluster_network_stable_discovery_announcement_due(3, false),
            "unchanged discovery ticks should skip multicast announcements between stable sends"
        );
        assert!(
            sinomo_core::network::cluster_network_stable_discovery_announcement_due(
                1 + sinomo_core::network::CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE,
                false,
            ),
            "stable announcements should repeat on the configured low-rate cadence"
        );
        assert!(
            sinomo_core::network::cluster_network_stable_discovery_announcement_due(3, true),
            "discovery or local endpoint changes must force an immediate announcement"
        );
    }

    #[test]
    fn cluster_network_maintenance_skips_discovery_tick_without_socket() {
        let root = unique_cluster_test_root("sinomo-daemon-discovery-none-idle");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_discovery_none_idle").expect("cluster");
        let local = NodeId::from_legacy_named("node_discovery_none_idle").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "discovery none idle",
            local,
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now
                .checked_sub(DAEMON_CLUSTER_DISCOVERY_INTERVAL)
                .unwrap_or(maintenance_now),
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };
        let runtime = daemon_cluster_test_runtime();

        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick");

        let snapshot = idle_stats.snapshot();
        assert_eq!(snapshot.ticks_total, 1);
        assert_eq!(
            snapshot.ticks_with_work, 0,
            "a missing discovery socket should not turn an idle tick into work"
        );
        assert_eq!(
            snapshot.disk_reads_cluster_state, 0,
            "discovery-only ticks without a socket should not read cluster state"
        );
    }

    #[test]
    fn cluster_network_maintenance_throttles_unclustered_ticks() {
        let root = unique_cluster_test_root("sinomo-daemon-unclustered-maintenance-idle");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: Some(
                maintenance_now
                    .checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION)
                    .expect("startup settle deadline"),
            ),
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 3,
            connectivity_publish_retry_at: Some(
                maintenance_now
                    .checked_sub(Duration::from_millis(1))
                    .expect("expired connectivity retry deadline"),
            ),
            connectivity_force_probe: true,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now
                .checked_sub(DAEMON_CLUSTER_DISCOVERY_INTERVAL)
                .unwrap_or(maintenance_now),
            last_probe: maintenance_now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .unwrap_or(maintenance_now),
            idle_stats: idle_stats.clone(),
        };
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let runtime = daemon_cluster_test_runtime();

        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("first unclustered tick");
        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("second unclustered tick");

        let snapshot = idle_stats.snapshot();
        assert_eq!(snapshot.ticks_total, 2);
        assert_eq!(
            snapshot.ticks_with_work, 0,
            "unclustered maintenance should not count timer checks as work"
        );
        assert_eq!(
            snapshot.disk_reads_cluster_state, 0,
            "unclustered maintenance should not try to read absent cluster state"
        );
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_retry_at, None);
        assert!(!maintenance.connectivity_force_probe);
        assert!(maintenance.connectivity_converged);
        let after_ticks = Instant::now();
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                after_ticks,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "absent cluster state should advance publish timer"
        );
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                after_ticks,
                maintenance.last_probe,
                DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL,
            ),
            "absent cluster state should advance probe timer"
        );
    }

    #[test]
    fn cluster_network_maintenance_returns_pending_async_connectivity_probe() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-pending-async-connectivity");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, None),
        )
        .expect("write daemon status");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_pending_async_connectivity").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_pending_async_connectivity_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_pending_async_connectivity_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "pending async connectivity",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote_node, "127.0.0.1:1".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        daemon_locked_set_cluster_raft_leader(&cluster_paths, &local_node, state.term)
            .expect("establish local raft leader");
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(
            &cluster_paths,
            &state,
        )
        .expect("bootstrap local raft state");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .unwrap_or(maintenance_now),
            idle_stats: idle_stats.clone(),
        };

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("runtime tick should return pending async probe");
        assert!(
            !maintenance.connectivity_converged,
            "probe convergence should wait for pending async execution"
        );
        assert_eq!(idle_stats.snapshot().connectivity_probes_sent, 1);

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert!(
            !maintenance.connectivity_converged,
            "an initial degraded observation needs a rapid convergence follow-up instead of waiting for the 60-second probe cadence"
        );
        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &cluster_paths,
            &local_node,
        )
        .expect("connectivity records");
        assert!(
            records
                .iter()
                .any(|record| record.source_node_id == local_node
                    && record.target_node_id == remote_node),
            "pending async probe should record the remote endpoint result"
        );
        let kv_records =
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&cluster_paths)
                .expect("kv connectivity records");
        assert!(
            kv_records
                .iter()
                .any(|record| record.source_node_id == local_node
                    && record.target_node_id == remote_node),
            "pending async probe should publish the local source connectivity set into cluster KV"
        );

        maintenance.startup_settle_until = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .map(Some)
            .expect("expired startup-settle marker");
        for _ in 0..CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            maintenance.last_probe = Instant::now()
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .expect("due probe timestamp");
            maintenance.probe_retry_at = None;
            maintenance.connectivity_force_probe = true;
            let Some(pending) = run_cluster_network_maintenance_tick_for_test(
                &mut maintenance,
                &daemon_paths,
                &DaemonRunOptions::default(),
                &runtime,
            )
            .expect("post-settle maintenance tick") else {
                continue;
            };
            assert!(
                maintenance
                    .startup_settle_until
                    .is_some_and(|deadline| deadline <= Instant::now()),
                "an expired startup-settle window must remain a one-shot marker"
            );
            runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));
            if maintenance.connectivity_converged {
                break;
            }
        }

        assert!(
            maintenance.connectivity_converged,
            "post-settle failures must reach a terminal state instead of re-arming startup softening"
        );
        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &cluster_paths,
            &local_node,
        )
        .expect("post-settle connectivity records");
        assert!(
            records.iter().any(|record| {
                record.target_node_id == remote_node
                    && record.state == ClusterConnectivityState::Unreachable
            }),
            "a persistently failed endpoint must transition out of Degraded after startup settle"
        );
    }

    #[test]
    fn initial_node_status_publish_precedes_stalled_connectivity_probe() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("blackhole listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking blackhole listener");
        let blackhole_address = listener.local_addr().expect("blackhole address");
        let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blackhole = std::thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = accepted_tx.send(());
                        let _ = release_rx.recv_timeout(Duration::from_secs(5));
                        drop(stream);
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if release_rx.try_recv().is_ok() {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("blackhole accept failed: {error}"),
                }
            }
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-probe-followup-tail-reserve");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_probe_followup_tail_reserve").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_probe_followup_tail_reserve_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_probe_followup_tail_reserve_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "probe followup tail reserve",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            blackhole_address.to_string(),
            10,
        );
        // Canonical object-RPC calls authenticate the expected responder
        // before opening the stream. Keep this fixture cryptographically
        // well-formed so the probe reaches the intentionally stalled peer.
        let remote_public_key = sinomo_core::network::daemon_auth_encode_public_key(&[7_u8; 32]);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node,
            "remote",
            remote_public_key,
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        state.members.push(remote_member);
        // This fixture models one live daemon across a deliberately stalled
        // 2.5-second network phase. Keep its lightweight resident coordination
        // slot owned for that lifetime so unrelated parallel test roots cannot
        // make the process-wide bounded cache treat this live daemon as stale.
        let _resident_raft_pin = sinomo_core::kv::pin_resident_raft_node_registry_for_test(
            &cluster_paths.kv_paths().raft_wal_paths(&local_node),
        )
        .expect("pin live daemon resident raft slot");
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        daemon_locked_set_cluster_raft_leader(&cluster_paths, &local_node, state.term)
            .expect("establish local raft leader");
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(
            &cluster_paths,
            &state,
        )
        .expect("bootstrap local raft state");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats,
        };
        let plan = maintenance.select_connectivity_probe_plan(&cluster_paths, &state);
        assert_eq!(plan.entries.len(), 1, "test needs one stalled probe");
        let pending = ClusterNetworkMaintenancePendingWork {
            publish_claimed: true,
            connectivity_probe: Some(ClusterConnectivityPendingProbe {
                paths: cluster_paths.clone(),
                source_node_id: local_node.clone(),
                state,
                plan,
                input_generation: maintenance.connectivity_input_generation,
                local_connectivity_snapshot: sinomo_core::network::__connectivity_store::capture_local_cluster_connectivity_store_snapshot(&cluster_paths)
                    .expect("capture local connectivity store"),
            }),
            node_status_refreshes: vec![ClusterNodeStatusPendingRefresh {
                paths: daemon_paths.clone(),
                label: "node status before stalled probe",
            }],
            ..ClusterNetworkMaintenancePendingWork::default()
        };

        let started = Instant::now();
        runtime.block_on(maintenance.run_pending_work_until_async(
            pending,
            runtime.handle().clone(),
            Instant::now()
                + CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE
                + Duration::from_millis(250),
        ));
        let probe_was_stalled = accepted_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let _ = release_tx.send(());
        blackhole.join().expect("blackhole thread");

        assert!(
            probe_was_stalled,
            "probe must connect and stall in the test peer"
        );
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the probe must yield at its bounded deadline after the presence publish"
        );
        let statuses = sinomo_core::cluster::read_cluster_node_statuses_from_kv(&cluster_paths)
            .expect("node statuses");
        assert!(
            statuses.iter().any(|status| status.node_id == local_node),
            "join-critical node-status publication must execute before the stalled probe; statuses={statuses:?} idle={:?} failures={}",
            maintenance.idle_stats.snapshot(),
            maintenance.publish_failure_streak,
        );
        let snapshot =
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(&cluster_paths)
                .expect("post-presence snapshot");
        assert!(
            snapshot
                .get(&cluster_network_info_key(&local_node))
                .is_none(),
            "slow-changing network metadata must not add a second quorum commit to the initial presence generation"
        );
        assert!(
            maintenance.next_due_duration(Instant::now())
                <= CLUSTER_NETWORK_INITIAL_PRESENCE_METADATA_FOLLOWUP_DELAY,
            "deferred network metadata must retain a bounded maintenance follow-up"
        );
        let metadata_followup = runtime
            .block_on(refresh_daemon_node_status_in_kv_async_with_timeouts(
                &daemon_paths,
                runtime.handle().clone(),
                cluster_control_plane_timeouts(),
            ))
            .expect("publish deferred network metadata");
        assert_eq!(
            metadata_followup,
            NodeStatusPublishOutcome {
                network_info_published: true,
                node_status_published: false,
                network_info_deferred: false,
            }
        );
        let network_info_entry = sinomo_core::kv::read_cluster_kv_local_entry(
            &cluster_paths.kv_paths(),
            &local_node,
            &cluster_network_info_key(&local_node),
        )
        .expect("post-metadata local entry");
        assert!(
            network_info_entry.value.is_some(),
            "the follow-up generation must publish the deferred network metadata; local read={network_info_entry:?}"
        );
    }

    #[test]
    fn cluster_network_maintenance_publishes_cached_connectivity_when_probe_is_not_due() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-cached-connectivity-publish");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cached_connectivity_publish").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_cached_connectivity_publish_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_cached_connectivity_publish_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "cached connectivity publish",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote_node, "127.0.0.1:1".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync state to local kv");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(
                now.format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string()),
            ),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &local_node,
            std::slice::from_ref(&record),
        )
        .expect("write local connectivity cache");
        assert!(
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&cluster_paths)
                .expect("initial kv connectivity")
                .is_empty(),
            "test starts with a local connectivity cache but no KV connectivity graph"
        );

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats,
        };

        let connectivity_retry_at = Instant::now()
            .checked_add(DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF)
            .expect("connectivity retry deadline");
        maintenance.connectivity_converged = false;
        maintenance.connectivity_publish_failure_streak = 10;
        maintenance.connectivity_publish_retry_at = Some(connectivity_retry_at);
        maintenance.last_probe = maintenance_now
            .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
            .expect("overdue probe timestamp");

        let unrelated_publish = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick during connectivity backoff")
        .expect("ordinary publication should remain scheduled");
        assert!(
            !unrelated_publish.member_publishes.is_empty(),
            "member publication must remain live during connectivity backoff"
        );
        assert!(
            unrelated_publish.connectivity_publish.is_none()
                && unrelated_publish.connectivity_probe.is_none(),
            "an unrelated wake must not bypass the connectivity retry deadline"
        );
        assert_eq!(
            maintenance.connectivity_publish_retry_at,
            Some(connectivity_retry_at),
            "ordinary publication scheduling must not shorten connectivity backoff"
        );

        maintenance.connectivity_publish_retry_at = Some(
            Instant::now()
                .checked_sub(Duration::from_millis(1))
                .expect("expired connectivity retry deadline"),
        );
        maintenance.last_publish = Instant::now();

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("connectivity retry tick")
        .expect("expired connectivity retry should return pending work");
        assert!(
            pending.connectivity_publish.is_some(),
            "an expired retry with no due endpoint should publish the retained local cache"
        );
        assert!(
            pending.connectivity_probe.is_none(),
            "a quorum-only failure must not force an unnecessary fresh probe"
        );

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_retry_at, None);

        let kv_records =
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&cluster_paths)
                .expect("kv connectivity records");
        assert!(
            kv_records.iter().any(|record| {
                record.source_node_id == local_node
                    && record.target_node_id == remote_node
                    && record.state == ClusterConnectivityState::Healthy
            }),
            "cached local connectivity should be published through the normal cluster KV graph"
        );
    }

    #[test]
    fn pending_connectivity_probe_preserves_unsuperseded_bootstrap_records() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-bootstrap-merge");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_bootstrap_merge").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_bootstrap_merge_local").expect("local");
        let introducer =
            NodeId::from_legacy_named("node_connectivity_bootstrap_merge_intro").expect("intro");
        let probed =
            NodeId::from_legacy_named("node_connectivity_bootstrap_merge_probed").expect("probed");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "connectivity bootstrap merge",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let introducer_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&introducer, "node3:21353".to_string(), 10);
        let probed_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&probed, "node1:21353".to_string(), 20);
        let introducer_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &introducer_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("join".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let probed_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &probed_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            sinomo_core::network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some("probe failed".to_string()),
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&introducer_record),
        )
        .expect("seed bootstrap connectivity");

        let merged = merge_probe_records_with_unsuperseded_bootstrap_connectivity(
            &paths,
            &local,
            vec![probed_record.clone()],
        );

        assert!(merged.contains(&probed_record));
        assert!(
            merged.contains(&introducer_record),
            "fresh probe replacement must retain a live join introducer edge for bootstrap routing"
        );
    }

    #[test]
    fn pending_connectivity_probe_preserves_live_unprobed_unreachable_module_records() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-unreachable-merge");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_connectivity_unreachable_merge")
            .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_unreachable_merge_local").expect("local");
        let offline = NodeId::from_legacy_named("node_connectivity_unreachable_merge_offline")
            .expect("offline");
        let probed = NodeId::from_legacy_named("node_connectivity_unreachable_merge_probed")
            .expect("probed");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "connectivity unreachable merge",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let offline_iroh =
            test_network_module_relay_endpoint(&offline, "module_public_key_unreachable_merge");
        let mut offline_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &offline_iroh,
            ClusterConnectivityState::Unreachable,
            None,
            sinomo_core::network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some("network authorization was denied".to_string()),
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        offline_record.failure_reason =
            Some(sinomo_core::network::ClusterConnectivityFailureReason::CommandAuthDenied);
        let probed_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&probed, "node1:21353".to_string(), 20);
        let probed_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &probed_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("ok".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&offline_record),
        )
        .expect("seed unreachable module connectivity");

        let merged = merge_probe_records_with_unsuperseded_bootstrap_connectivity(
            &paths,
            &local,
            vec![probed_record.clone()],
        );

        assert!(merged.contains(&probed_record));
        assert!(
            merged.contains(&offline_record),
            "an unrelated probe must preserve a live unreachable module record until its backoff TTL expires"
        );
    }

    #[test]
    fn unprobed_degraded_module_record_does_not_keep_convergence_accelerated() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-degraded-module-idle");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_connectivity_degraded_module_idle")
            .expect("cluster");
        let local = NodeId::from_legacy_named("node_connectivity_degraded_module_idle_local")
            .expect("local");
        let remote = NodeId::from_legacy_named("node_connectivity_degraded_module_idle_remote")
            .expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity degraded module idle",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let module_endpoint =
            test_network_module_relay_endpoint(&remote, "module_public_key_degraded_idle");
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote, "remote", "pk-remote", now);
        remote_member.endpoints = vec![module_endpoint.clone()];
        state.members.push(remote_member);

        let module_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &module_endpoint,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("transient module failure".to_string()),
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&module_record),
        )
        .expect("seed degraded module connectivity");

        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };
        let plan = ClusterConnectivityProbePlan {
            active_keys: Vec::new(),
            entries: Vec::new(),
            remaining_due: 0,
            convergence_active: true,
            force_probe: false,
            timestamp: "unused".to_string(),
            now,
            startup_settle_active: false,
        };

        let execution =
            runtime.block_on(maintenance.execute_connectivity_probe_plan_async(&state, plan));
        assert!(execution.records.is_empty());
        assert!(
            !execution.probed_degraded,
            "a degraded record outside the actual probe batch must not request a convergence follow-up"
        );
        let followup_needed =
            sinomo_core::cluster::cluster_connectivity_probe_result_needs_followup(
                0,
                true,
                execution.probed_degraded,
            );
        let merged = merge_probe_records_with_unsuperseded_bootstrap_connectivity(
            &paths,
            &local,
            execution.records,
        );
        assert!(
            merged.contains(&module_record),
            "the module-owned record must still be retained for storage and publication"
        );

        maintenance.connectivity_converged = !followup_needed;
        assert!(maintenance.connectivity_converged);
        assert_eq!(
            maintenance.next_due_duration(maintenance_now),
            DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL,
            "settled connectivity should return to the normal workstation role cadence"
        );
    }

    #[test]
    fn cluster_network_deadline_with_tail_reserve_preserves_connectivity_recovery_budget() {
        assert!(
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE > CLUSTER_HEALTH_PROBE_TIMEOUT,
            "connectivity recovery must retain a complete endpoint probe plus projection overhead"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let reserved = cluster_network_deadline_with_tail_reserve(
            Some(deadline),
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE,
        )
        .expect("reserved deadline");
        let tail = deadline.duration_since(reserved);
        assert!(
            tail >= CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE,
            "status publication must leave the connectivity recovery reserve intact"
        );

        let already_expired = cluster_network_deadline_with_tail_reserve(
            Some(Instant::now() + Duration::from_millis(500)),
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_RECOVERY_RESERVE,
        )
        .expect("expired reserved deadline");
        assert!(
            already_expired <= Instant::now(),
            "connectivity should be skipped when the remaining budget is already reserved"
        );
    }

    #[test]
    fn cluster_network_pending_publish_timeouts_respect_control_plane_attempt_budget() {
        assert!(
            CLUSTER_NETWORK_PENDING_CONNECTIVITY_PUBLISH_TIMEOUT
                >= sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            "connectivity publish must allow at least one public/manual endpoint attempt"
        );
        assert!(
            CLUSTER_NETWORK_PENDING_NODE_STATUS_TIMEOUT
                >= sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            "node-status publish needs enough budget for one public endpoint attempt"
        );
        assert!(
            CLUSTER_NETWORK_PENDING_MEMBER_PUBLISH_TIMEOUT
                >= sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            "member publish needs enough budget for one public endpoint attempt"
        );
    }

    #[test]
    fn cluster_network_maintenance_startup_settle_uses_shared_pending_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-startup-settle-normal-publish");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_startup_settle_publish").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_startup_settle_publish_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_startup_settle_publish_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "startup settle publish",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync state to local kv");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: Some(
                maintenance_now
                    .checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION)
                    .expect("startup settle deadline"),
            ),
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("startup settle tick should return pending work");

        assert_eq!(pending.member_publishes.len(), 1);
        assert_eq!(pending.node_status_refreshes.len(), 1);
        assert!(
            pending.connectivity_probe.is_none(),
            "startup settling with no probeable endpoint must not construct an empty probe"
        );
        assert!(
            pending.connectivity_publish.is_some(),
            "startup settling should retain cached connectivity publication"
        );
    }

    #[test]
    fn cluster_network_maintenance_records_node_status_timeout_for_retry() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-node-status-timeout-retry");
        let data_root = root.join("data");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };
        let pending = ClusterNetworkMaintenancePendingWork {
            publish_claimed: true,
            node_status_refreshes: vec![ClusterNodeStatusPendingRefresh {
                paths: daemon_paths,
                label: "node status",
            }],
            ..Default::default()
        };
        let expired_deadline = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap_or_else(Instant::now);

        runtime.block_on(maintenance.run_pending_work_until_async(
            pending,
            runtime.handle().clone(),
            expired_deadline,
        ));

        assert_eq!(
            maintenance.publish_failure_streak, 1,
            "timed-out node-status publish should count as a publish failure"
        );
        let before_backoff = Instant::now()
            .checked_add(DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF / 2)
            .expect("before backoff instant");
        let after_backoff = Instant::now()
            .checked_add(
                DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF + Duration::from_millis(100),
            )
            .expect("after backoff instant");
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                before_backoff,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "timeout should not cause a tight retry loop"
        );
        assert!(
            sinomo_core::network::cluster_network_maintenance_interval_due(
                after_backoff,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "timeout should keep publish eligible after the failure backoff"
        );
    }

    #[test]
    fn cluster_network_maintenance_returns_pending_async_node_status_refresh() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-pending-async-node-status");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_pending_async_node_status").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_pending_async_node_status_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_pending_async_node_status_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "pending async node status",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync state to local kv");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("runtime publish tick should return pending status refresh");
        assert_eq!(pending.node_status_refreshes.len(), 1);
        assert!(pending.connectivity_probe.is_none());

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        let snapshot = idle_stats.snapshot();
        assert_eq!(snapshot.kv_publish_node_status, 1);
        let statuses = sinomo_core::cluster::read_cluster_node_statuses_from_kv(&cluster_paths)
            .expect("statuses");
        assert!(
            statuses.iter().any(|status| status.node_id == local_node
                && status.availability == ClusterNodeAvailability::Online),
            "pending async status refresh should publish this daemon's node status"
        );

        let repeat = ClusterNetworkMaintenancePendingWork {
            node_status_refreshes: vec![ClusterNodeStatusPendingRefresh {
                paths: daemon_paths,
                label: "unchanged node status",
            }],
            ..Default::default()
        };
        runtime.block_on(maintenance.run_pending_work_async(repeat, runtime.handle().clone()));
        assert_eq!(
            idle_stats.snapshot().kv_publish_node_status,
            1,
            "an unchanged async status refresh must not force another quorum write"
        );
    }

    #[test]
    fn cluster_network_maintenance_plans_probe_and_source_publish_without_full_state_sync() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-pending-async-member-publish");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_pending_async_member_publish").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_pending_async_member_publish_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_pending_async_member_publish_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "pending async member publish",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:1".to_string(),
            10,
        )];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        daemon_locked_set_cluster_raft_leader(&cluster_paths, &local_node, state.term)
            .expect("establish local raft leader");
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(
            &cluster_paths,
            &state,
        )
        .expect("bootstrap local raft state");
        let remote_endpoints_before_tick =
            sinomo_core::cluster::read_cluster_members_from_kv(&cluster_paths)
                .expect("members before node-local observation")
                .into_iter()
                .find(|member| member.node_id == remote_node)
                .expect("remote member before node-local observation")
                .endpoints;
        let mut observed_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:notaport".to_string(),
            20,
        );
        observed_endpoint.source = ClusterEndpointSource::Observed;
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == remote_node)
            .expect("remote member for node-local observation")
            .endpoints
            .push(observed_endpoint.clone());
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write node-local remote observation");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .unwrap_or(maintenance_now),
            idle_stats: idle_stats.clone(),
        };

        let members_before_tick =
            sinomo_core::cluster::read_cluster_members_from_kv(&cluster_paths)
                .expect("members before maintenance tick");
        let local_before_tick = members_before_tick
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("local member before maintenance tick");
        assert!(
            local_before_tick.endpoints.is_empty(),
            "fixture must start before local daemon endpoint publication"
        );
        let kv_before_tick =
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(&cluster_paths)
                .expect("KV snapshot before maintenance tick");

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("runtime publish tick should return pending member publish");
        assert_eq!(pending.member_publishes.len(), 1);
        assert!(!pending.member_publishes[0].force_kv_snapshot_sync);
        assert_eq!(pending.node_status_refreshes.len(), 1);
        assert!(
            pending.connectivity_probe.is_some(),
            "a same-tick local membership change must not prevent the due direct probe from being returned"
        );

        let refreshed_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("refreshed local cluster state");
        let refreshed_local = refreshed_state
            .members
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("refreshed local member");
        assert!(
            !refreshed_local.endpoints.is_empty(),
            "tick planning must observe the running daemon endpoint"
        );
        let members_after_planning =
            sinomo_core::cluster::read_cluster_members_from_kv(&cluster_paths)
                .expect("members after maintenance planning");
        let local_after_planning = members_after_planning
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("local member after maintenance planning");
        assert!(
            local_after_planning.endpoints.is_empty(),
            "tick planning must not run a full-state authority transaction before returning connectivity work"
        );
        assert_eq!(
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(&cluster_paths)
                .expect("KV snapshot after maintenance planning"),
            kv_before_tick,
            "tick planning must not mutate or advance the local Raft/KV projection"
        );

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert_eq!(idle_stats.snapshot().kv_publish_member, 1);
        let members = sinomo_core::cluster::read_cluster_members_from_kv(&cluster_paths)
            .expect("members from kv");
        let published_remote = members
            .iter()
            .find(|member| member.node_id == remote_node)
            .expect("published remote member");
        assert_eq!(
            published_remote.endpoints, remote_endpoints_before_tick,
            "source-owned local member publication must not publish or overwrite a remote node's node-local observed endpoint"
        );
        assert!(
            published_remote
                .endpoints
                .iter()
                .all(|endpoint| endpoint.endpoint_id != observed_endpoint.endpoint_id),
            "the node-local remote observation must remain outside authoritative KV"
        );
    }

    #[test]
    fn cluster_network_maintenance_learner_uses_only_source_owned_publish_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-learner-source-publish");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_learner_source_publish").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_learner_source_publish_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_learner_source_publish_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "learner source publish",
            local_node,
            "local",
            "pk-local",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut state.members[0],
            ClusterMemberRole::NonVoting,
            now,
        );
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:1".to_string(),
            10,
        )];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync state to local kv");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats,
        };

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("regular cadence should produce source-owned publish work");

        assert_eq!(pending.member_publishes.len(), 1);
        assert!(!pending.member_publishes[0].force_kv_snapshot_sync);
        assert_eq!(pending.node_status_refreshes.len(), 1);
    }

    #[test]
    fn cluster_network_maintenance_voter_uses_only_source_owned_publish_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = unique_cluster_test_root("sinomo-daemon-voter-source-publish");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        daemon_paths.ensure().expect("daemon paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_voter_source_publish").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_voter_source_publish_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_voter_source_publish_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "voter source publish",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "127.0.0.1:1".to_string(),
            10,
        )];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync state to local kv");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats,
        };

        let pending = run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("maintenance tick")
        .expect("voter maintenance should still publish member and status records");

        assert_eq!(pending.member_publishes.len(), 1);
        assert!(!pending.member_publishes[0].force_kv_snapshot_sync);
        assert_eq!(pending.node_status_refreshes.len(), 1);
    }

    #[test]
    fn cluster_network_maintenance_keeps_settled_solo_cluster_idle() {
        let root = unique_cluster_test_root("sinomo-daemon-solo-cluster-maintenance-idle");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_solo_maintenance_idle").expect("cluster");
        let local = NodeId::from_legacy_named("node_solo_maintenance_idle").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "solo maintenance idle",
            local,
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: true,

            startup_settle_until: Some(
                maintenance_now
                    .checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION)
                    .expect("startup settle deadline"),
            ),
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 3,
            connectivity_publish_retry_at: Some(
                maintenance_now
                    .checked_sub(Duration::from_millis(1))
                    .expect("expired connectivity retry deadline"),
            ),
            connectivity_force_probe: true,
            last_publish: maintenance_now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(maintenance_now),
            last_discovery: maintenance_now,
            last_probe: maintenance_now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .unwrap_or(maintenance_now),
            idle_stats: idle_stats.clone(),
        };
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let runtime = daemon_cluster_test_runtime();

        run_cluster_network_maintenance_tick_for_test(
            &mut maintenance,
            &daemon_paths,
            &DaemonRunOptions::default(),
            &runtime,
        )
        .expect("settled solo maintenance tick");

        let snapshot = idle_stats.snapshot();
        assert_eq!(snapshot.ticks_total, 1);
        assert_eq!(
            snapshot.ticks_with_work, 0,
            "settled solo clusters should not do publish/probe preparation"
        );
        assert_eq!(
            snapshot.disk_reads_cluster_state, 1,
            "settled solo clusters should only validate local state cheaply"
        );
        assert!(
            !cluster_paths.kv_paths().snapshot.exists(),
            "settled solo maintenance must not create or rewrite the KV snapshot"
        );
        assert_eq!(maintenance.connectivity_publish_failure_streak, 0);
        assert_eq!(maintenance.connectivity_publish_retry_at, None);
        assert!(!maintenance.connectivity_force_probe);
        assert!(maintenance.connectivity_converged);
        let after_tick = Instant::now();
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                after_tick,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "settled solo tick should advance publish timer"
        );
        assert!(
            !sinomo_core::network::cluster_network_maintenance_interval_due(
                after_tick,
                maintenance.last_probe,
                DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL,
            ),
            "settled solo tick should advance probe timer"
        );
    }

    #[test]
    fn cluster_network_discovery_replay_does_not_mark_state_changed() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_discovery_replay_noop").expect("cluster");
        let local = NodeId::from_legacy_named("node_discovery_replay_local").expect("local");
        let remote = NodeId::from_legacy_named("node_discovery_replay_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "discovery replay noop",
            local,
            "local",
            "pk-local",
            now,
        );
        state.members.push(ClusterMemberRecord::pending_learner(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        ));
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_discovery_replay_remote",
            )
            .expect("endpoint"),
            node_id: remote.clone(),
            transport: ClusterTransportKind::LanQuic,
            address: "10.0.0.20:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Mdns,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        };
        let advertisement =
            MdnsDiscoveryAdvertisement::new(cluster_id, remote, vec![endpoint.clone()], 1);
        let mut discovery_cache = MdnsDiscoveryCache::new();

        assert!(
            sinomo_core::network::apply_lan_discovery_advertisements(
                &mut state,
                &mut discovery_cache,
                vec![advertisement.clone()]
            ),
            "first discovery should add the remote endpoint"
        );
        assert_eq!(state.members[1].endpoints, vec![endpoint]);
        assert!(
            !sinomo_core::network::apply_lan_discovery_advertisements(
                &mut state,
                &mut discovery_cache,
                vec![advertisement]
            ),
            "replayed identical discovery should not force a cluster-state write"
        );
    }

    #[test]
    fn member_publish_noop_response_omits_full_snapshot() {
        let root = unique_cluster_test_root("sinomo-daemon-member-publish-noop-no-snapshot");
        let data_dir = SinomoDataDir::new(&root);
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_member_publish_noop").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_daemon_member_publish_noop").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "member publish noop",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let synced =
            sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
                .expect("sync cluster state");

        let authenticated = AuthenticatedDaemonCommand {
            route: sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE.to_string(),
            signer: crate::DaemonSignerAuthority::Local,
            signer_node_id: local.to_string(),
            signer_public_key: state.members[0].node_public_key.clone(),
        };
        let runtime = daemon_cluster_test_runtime();
        let mut member_host = daemon_cluster_control_plane_driver_on_runtime(runtime.handle());
        let mut invite_host = daemon_cluster_control_plane_driver_on_runtime(runtime.handle());
        let mut join_host =
            DaemonClusterJoinRegistrationHost::new_on_runtime(&daemon_paths, runtime.handle());

        // A publisher already at the responder's revision: stable publish,
        // nothing to converge, no snapshot in the response.
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE,
            sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &ClusterMemberPublishRequest {
                members: vec![state.members[0].clone()],
                local_kv_revision: Some(synced.revision),
            },
        )
        .expect("member publish rpc request");
        let response = sinomo_core::cluster::handle_cluster_membership_rpc_request_with_hosts(
            &cluster_paths,
            request,
            &authenticated.signer_node_id,
            &authenticated.signer_public_key,
            now,
            &mut join_host,
            &mut member_host,
            &mut invite_host,
        )
        .expect("member publish response");
        let response: ClusterMemberPublishResponse =
            response.decode_body().expect("member publish body");

        assert!(response.accepted);
        assert_eq!(response.detail, "member endpoints already current");
        assert!(
            response.kv_snapshot.is_none(),
            "a publisher at the responder's revision should not receive a snapshot"
        );

        // A publisher with no readable local KV reports no revision; only a
        // full snapshot converges it, so the stable publish still ships one.
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE,
            sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &ClusterMemberPublishRequest {
                members: vec![state.members[0].clone()],
                local_kv_revision: None,
            },
        )
        .expect("member publish rpc request without revision");
        let response = sinomo_core::cluster::handle_cluster_membership_rpc_request_with_hosts(
            &cluster_paths,
            request,
            &authenticated.signer_node_id,
            &authenticated.signer_public_key,
            now,
            &mut join_host,
            &mut member_host,
            &mut invite_host,
        )
        .expect("member publish response without revision");
        let response: ClusterMemberPublishResponse = response
            .decode_body()
            .expect("member publish body without revision");

        assert!(response.accepted);
        assert_eq!(
            response.kv_snapshot.map(|snapshot| snapshot.revision),
            Some(synced.revision),
            "a publisher without a readable local KV snapshot converges via the full snapshot"
        );
    }

    #[test]
    fn cluster_kv_commit_waits_for_local_raft_operation_lock() {
        let root = unique_cluster_test_root("sinomo-daemon-raft-local-lock-wait");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_raft_local_lock_wait").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_raft_local_lock_wait").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "raft local lock wait",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");

        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let paths_for_holder = paths.clone();
        let local_for_holder = local.clone();
        let holder = std::thread::spawn(move || {
            let _raft_guard = sinomo_core::kv::lock_cluster_kv_raft_operation_for_paths(
                &paths_for_holder.kv_paths(),
            )
            .expect("raft operation lock");
            let _node = paths_for_holder
                .kv_paths()
                .open_raft_node(&local_for_holder)
                .expect("hold raft node");
            held_tx.send(()).expect("notify lock held");
            std::thread::sleep(Duration::from_millis(100));
        });
        held_rx.recv().expect("wait for held raft node");

        let (snapshot, _) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/test/local-lock-wait".to_string(),
                    value: b"ok".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        )
        .expect("commit should wait for the in-process raft operation");
        holder.join().expect("join raft holder");

        let store = InProcessKvStore::restore(snapshot).expect("restore");
        assert_eq!(
            store.get("/test/local-lock-wait").expect("committed").value,
            b"ok"
        );
    }

    #[test]
    fn no_effect_cluster_kv_txn_with_quorum_commits_without_advancing_revision() {
        let root = unique_cluster_test_root("sinomo-daemon-no-effect-kv-commit");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_no_effect_kv_no_append").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_no_effect_kv_no_append").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "no effect kv no append",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");

        let (committed, _) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/test/no-effect/present".to_string(),
                    value: b"present".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        )
        .expect("seed kv entry");
        let wal_path = paths.kv_paths().raft_wal_paths(&local).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before no-effect")
            .len();

        let (snapshot, response) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Delete {
                    key: "/test/no-effect/missing".to_string(),
                }],
                failure: Vec::new(),
            },
        )
        .expect("no-effect txn should commit through raft");

        assert_eq!(
            snapshot.revision, committed.revision,
            "no-effect daemon txn must not advance the cluster revision"
        );
        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: committed.revision,
                responses: vec![sinomo_core::kv::KvOpResponse::Delete {
                    key: "/test/no-effect/missing".to_string(),
                    deleted: false,
                }],
            })
        );
        assert!(
            std::fs::metadata(&wal_path)
                .expect("wal after no-effect")
                .len()
                > wal_len_before,
            "a fresh no-effect request must append durable quorum evidence to the raft WAL"
        );
    }

    #[test]
    fn async_member_mutation_commit_uses_local_leader_quorum_path() {
        let root = unique_cluster_test_root("sinomo-daemon-async-local-leader-commit");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_async_local_leader").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_async_local_leader").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "async local leader",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        daemon_locked_set_cluster_raft_leader(&paths, &local, 1).expect("set leader");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
        let snapshot = runtime
            .block_on(
                sinomo_core::cluster::ClusterMemberMutationAsyncHost::commit_cluster_kv_txn_with_quorum_async(
                    &mut host,
                    &paths,
                    &mut state,
                    sinomo_core::kv::KvTxnRequest::generated(KvTxn {
                        compares: Vec::new(),
                        success: vec![KvOp::Put {
                            key: "/test/async-local-leader".to_string(),
                            value: b"ok".to_vec(),
                            lease_id: None,
                        }],
                        failure: Vec::new(),
                    }),
                ),
            )
            .expect("async local leader commit");

        let store = InProcessKvStore::restore(snapshot.snapshot).expect("restore");
        assert_eq!(
            store
                .get("/test/async-local-leader")
                .expect("committed")
                .value,
            b"ok"
        );
    }

    #[test]
    fn async_membership_snapshot_reuses_read_index_without_appending_noop() {
        let root = unique_cluster_test_root("sinomo-daemon-async-local-leader-commit");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_async_local_leader").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_async_local_leader").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "async local leader",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        daemon_locked_set_cluster_raft_leader(&paths, &local, 1).expect("set leader");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
        let read = runtime.block_on(
            sinomo_core::cluster::ClusterMemberMutationAsyncHost::authoritative_cluster_kv_snapshot_async(
                &mut host, &paths, &mut state,
            ),
        ).expect("cold authoritative snapshot");
        assert!(matches!(
            read.authority.basis,
            sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        let index = paths
            .kv_paths()
            .raft_store()
            .locked_last_log_index(&local)
            .unwrap();
        let read = runtime.block_on(
            sinomo_core::cluster::ClusterMemberMutationAsyncHost::authoritative_cluster_kv_snapshot_async(
                &mut host, &paths, &mut state,
            ),
        ).expect("warm authoritative snapshot");
        assert!(matches!(
            read.authority.basis,
            sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(
            paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&local)
                .unwrap(),
            index,
            "membership observations must use ReadIndex instead of proposing an empty mutation"
        );
    }

    #[test]
    fn stable_connectivity_refresh_is_read_only_without_advancing_revision() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-refresh-no-append");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_connectivity_refresh_no_append")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_connectivity_refresh_no_append_local")
            .expect("local");
        let remote = NodeId::from_legacy_named("node_daemon_connectivity_refresh_no_append_remote")
            .expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity refresh no append",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        let endpoint =
            sinomo_core::network::daemon_lan_endpoint(&remote, "10.0.0.71:21353".to_string(), 10);
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote, "remote", "pk-remote", now);
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");

        let seeded_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("seeded".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let key = cluster_connectivity_key(
            &seeded_record.source_node_id,
            &seeded_record.target_node_id,
            &seeded_record.endpoint_id,
        );
        let (seeded_snapshot, _) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(&key, &seeded_record, None).expect("seed connectivity op"),
                ],
                failure: Vec::new(),
            },
        )
        .expect("seed connectivity record");
        let seeded_entry = seeded_snapshot
            .get(&key)
            .expect("seeded connectivity entry")
            .clone();
        let wal_path = paths.kv_paths().raft_wal_paths(&local).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before semantic no-effect")
            .len();

        let refreshed_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(11),
            0,
            Some("refreshed".to_string()),
            None,
            now + time::Duration::seconds(5),
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        assert!(
            sinomo_core::cluster::cluster_connectivity_publish_record_matches(
                &seeded_record,
                &refreshed_record,
                now
            ),
            "test record should differ only by non-durable connectivity observations"
        );
        let result = sync_cluster_connectivity_records_to_kv(
            &paths,
            &state.local_node_id,
            vec![refreshed_record],
        )
        .expect("stable connectivity refresh");

        assert!(
            result.is_none(),
            "a stable source-owned refresh must not produce a transaction result"
        );
        let current = sinomo_core::cluster::read_cluster_kv_current_snapshot(&paths)
            .expect("current connectivity snapshot");
        assert_eq!(current.revision, seeded_snapshot.revision);
        assert_eq!(current.get(&key), Some(&seeded_entry));
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after semantic no-effect")
                .len(),
            wal_len_before,
            "stable connectivity refreshes must not append an empty Raft transaction"
        );
    }

    #[test]
    fn source_only_member_publish_uses_read_index_without_advancing_revision_or_wal() {
        let root = unique_cluster_test_root("sinomo-daemon-source-only-member-no-append");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_source_only_no_append").expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_source_only_no_append").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "source only member no append",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        let member = &mut state.members[0];
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = vec![ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_source_only",
            )
            .expect("endpoint"),
            node_id: local.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "172.23.0.4:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Mdns,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 20,
            last_observed_latency_ms: None,
        }];
        state.members[0]
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");

        let snapshot = sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            local.clone(),
        )
        .expect("read snapshot");
        let member_key = cluster_member_key(&local);
        let member_entry = snapshot.get(&member_key).expect("member entry").clone();
        let mut observed_member = state.members[0].clone();
        let observed_endpoint = observed_member
            .endpoints
            .iter_mut()
            .find(|endpoint| {
                endpoint.transport == ClusterTransportKind::LanTcp
                    && endpoint.address == "172.23.0.4:21353"
            })
            .expect("observed LAN endpoint");
        observed_endpoint.source = sinomo_core::ClusterEndpointSource::Observed;
        let observed_endpoint = observed_endpoint.clone();
        let endpoint_key = cluster_endpoint_key(&local, &observed_endpoint);
        let endpoint_entry = snapshot.get(&endpoint_key).expect("endpoint entry").clone();
        let wal_path = paths.kv_paths().raft_wal_paths(&local).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before source no-effect")
            .len();

        let runtime = daemon_cluster_test_runtime();
        let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime.handle());
        let response = sinomo_core::cluster::handle_cluster_member_publish_request_with_host(
            &paths,
            ClusterMemberPublishRequest {
                members: vec![observed_member],
                local_kv_revision: Some(snapshot.revision),
            },
            local.as_str(),
            &mut host,
        )
        .expect("source-only member publish should establish ReadIndex authority");
        assert!(response.accepted);
        assert_eq!(response.detail, "member endpoints already current");
        assert!(
            response.kv_snapshot.is_none(),
            "a current publisher does not need the unchanged snapshot payload"
        );

        let snapshot_after =
            sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(&paths.kv_paths(), local)
                .expect("read snapshot after source-only publish");

        assert_eq!(
            snapshot_after.revision, snapshot.revision,
            "source-only member publishes must not advance the cluster revision"
        );
        assert_eq!(snapshot_after.get(&member_key), Some(&member_entry));
        assert_eq!(snapshot_after.get(&endpoint_key), Some(&endpoint_entry));
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after source no-effect")
                .len(),
            wal_len_before,
            "source-only member publishes must reuse ReadIndex authority without a Raft write"
        );
    }

    #[test]
    fn forwarded_cluster_kv_txn_never_starts_local_election() {
        let root = unique_cluster_test_root("sinomo-daemon-forwarded-kv-local-election");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_forwarded_kv_local_election")
            .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_daemon_forwarded_kv_local_election").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "forwarded kv local election",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft");
        daemon_clear_cluster_raft_leader(&paths, &state.local_node_id)
            .expect("clear known state authority");
        assert_eq!(
            daemon_cluster_kv_state_authority_id(&paths, &state).expect("known state authority"),
            None
        );

        let key = "/test/forwarded-local-election".to_string();
        let error = commit_cluster_kv_txn_with_quorum_response_with_forwarding(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: key.clone(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            false,
        )
        .expect_err("forwarded non-leader voter must not campaign");
        assert!(matches!(
            error,
            DaemonError::ClusterReadOnly { detail }
                if detail.contains("forwarded cluster KV transaction reached a non-leader voter")
        ));
        assert!(
            sinomo_core::cluster::read_cluster_kv_snapshot(&paths)
                .expect("read cluster snapshot")
                .get(&key)
                .is_none(),
            "forwarded request must not commit locally"
        );
        assert_eq!(
            daemon_cluster_kv_state_authority_id(&paths, &state).expect("known state authority"),
            None
        );
    }

    #[test]
    fn daemon_cluster_status_from_local_state_resolves_shadow_without_kv() {
        let root = unique_cluster_test_root("sinomo-daemon-shadow-local-cluster-status");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_shadow_local_status").expect("cluster"),
            "shadow local status".to_string(),
            NodeId::from_legacy_named("node_shadow_local_status").expect("node"),
            "shadow local status".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let main_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_353,
            port_fallback: false,
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_354,
            port_fallback: false,
        };
        ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, now);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write local state");

        let registration = DaemonClusterRegistration {
            process: crate::DaemonClusterProcess::Shadow,
            main_options,
            shadow_options,
        };
        let status = daemon_cluster_status_from_local_state(&paths, &registration)
            .expect("shadow cluster status");

        assert_eq!(status.cluster_id, state.cluster_id);
        assert_eq!(
            status.process,
            sinomo_core::node::DAEMON_SHADOW_CLUSTER_ROLE
        );
        assert_eq!(status.member_role, ClusterMemberRole::Voter);
        assert_eq!(status.member_state, ClusterMemberState::Voter);
    }

    #[test]
    fn read_local_cluster_state_without_kv_ignores_cached_kv_members() {
        let root = unique_cluster_test_root("sinomo-daemon-local-state-without-kv");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_without_kv").expect("cluster without kv");
        let local_node = NodeId::from_legacy_named("node_daemon_without_kv_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_without_kv_remote").expect("remote");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "local state without kv".to_string(),
            local_node,
            "local".to_string(),
            "pk-local".to_string(),
            now,
        );
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write local state");
        let remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&remote_node), &remote_member, None)
            .expect("seed kv-only member");
        sinomo_core::kv::write_cluster_kv_snapshot(
            &paths.kv_paths(),
            &store.snapshot().expect("snapshot"),
        )
        .expect("write kv snapshot");

        let with_kv = read_local_cluster_state(&paths).expect("read local state with kv");
        assert!(
            with_kv
                .members
                .iter()
                .any(|member| member.node_id == remote_node),
            "test setup should expose the kv-only member through the normal helper"
        );

        let without_kv =
            read_local_cluster_state_without_kv(&paths).expect("read local state without kv");
        assert!(
            without_kv
                .members
                .iter()
                .all(|member| member.node_id != remote_node),
            "the without-kv helper must not merge cached kv members"
        );
    }

    #[test]
    fn command_connectivity_observation_does_not_mark_quorum_publish_cache() {
        let root = unique_cluster_test_root("sinomo-daemon-command-connectivity-cache");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_command_connectivity_cache")
            .expect("cluster");
        let local = NodeId::from_legacy_named("node_daemon_command_cache_local").expect("local");
        let remote = NodeId::from_legacy_named("node_daemon_command_cache_remote").expect("remote");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "command connectivity cache",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote,
            "remote-command-cache.invalid:21353".to_string(),
            10,
        );
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("command".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&record),
        )
        .expect("write local connectivity");

        publish_command_connectivity_if_needed(&paths, &state, &record, None);

        assert!(
            !cached_connectivity_matches(&paths, &record),
            "local command observations must not masquerade as quorum-published connectivity"
        );
        assert!(
            !cached_connectivity_record_set_matches(&paths, &local, &[record]),
            "maintenance must still publish the local source record set into cluster KV"
        );
    }

    fn doctor_connectivity_record(
        cluster_id: &ClusterId,
        source: &NodeId,
        target: &NodeId,
        endpoint_name: &str,
        address: &str,
        state: sinomo_core::network::ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let healthy = state == sinomo_core::network::ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(endpoint_name)
                .expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: address.to_string(),
            state,
            latency_ms: if healthy { Some(4) } else { None },
            consecutive_failures: if healthy {
                0
            } else {
                CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
            },
            last_success_at: if healthy {
                Some("ok".to_string())
            } else {
                None
            },
            last_failure_at: if healthy {
                None
            } else {
                Some("failed".to_string())
            },
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(CLUSTER_CONNECTIVITY_TTL_SECONDS),
        }
    }

    #[test]
    fn raft_catch_up_targets_only_peer_level_healthy_transitions() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_reachable_catch_up").expect("cluster");
        let source =
            NodeId::from_legacy_named("node_daemon_reachable_catch_up_source").expect("source");
        let recovered = NodeId::from_legacy_named("node_daemon_reachable_catch_up_recovered")
            .expect("recovered");
        let already_healthy =
            NodeId::from_legacy_named("node_daemon_reachable_catch_up_healthy").expect("healthy");
        let newly_observed =
            NodeId::from_legacy_named("node_daemon_reachable_catch_up_new").expect("new");
        let still_degraded =
            NodeId::from_legacy_named("node_daemon_reachable_catch_up_degraded").expect("degraded");
        let previous = BTreeMap::from([
            (
                "recovered-old".to_string(),
                ClusterConnectivityProbeState {
                    record: doctor_connectivity_record(
                        &cluster_id,
                        &source,
                        &recovered,
                        "endpoint_daemon_reachable_catch_up_recovered_old",
                        "192.0.2.20:21353",
                        ClusterConnectivityState::Unreachable,
                    ),
                },
            ),
            (
                "healthy-old".to_string(),
                ClusterConnectivityProbeState {
                    record: doctor_connectivity_record(
                        &cluster_id,
                        &source,
                        &already_healthy,
                        "endpoint_daemon_reachable_catch_up_healthy_old",
                        "192.0.2.21:21353",
                        ClusterConnectivityState::Healthy,
                    ),
                },
            ),
        ]);
        let probed = vec![
            (
                "recovered-new".to_string(),
                doctor_connectivity_record(
                    &cluster_id,
                    &source,
                    &recovered,
                    "endpoint_daemon_reachable_catch_up_recovered_new",
                    "192.0.2.20:21353",
                    ClusterConnectivityState::Healthy,
                ),
            ),
            (
                "healthy-new".to_string(),
                doctor_connectivity_record(
                    &cluster_id,
                    &source,
                    &already_healthy,
                    "endpoint_daemon_reachable_catch_up_healthy_new",
                    "192.0.2.21:21353",
                    ClusterConnectivityState::Healthy,
                ),
            ),
            (
                "new".to_string(),
                doctor_connectivity_record(
                    &cluster_id,
                    &source,
                    &newly_observed,
                    "endpoint_daemon_reachable_catch_up_new",
                    "192.0.2.22:21353",
                    ClusterConnectivityState::Healthy,
                ),
            ),
            (
                "degraded".to_string(),
                doctor_connectivity_record(
                    &cluster_id,
                    &source,
                    &still_degraded,
                    "endpoint_daemon_reachable_catch_up_degraded",
                    "192.0.2.23:21353",
                    ClusterConnectivityState::Degraded,
                ),
            ),
        ];

        assert_eq!(
            newly_healthy_connectivity_target_node_ids(&previous, &probed),
            BTreeSet::from([recovered, newly_observed])
                .into_iter()
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cluster_doctor_suppresses_unreachable_endpoint_when_target_has_healthy_route() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_doctor_routed_endpoint").expect("cluster");
        let source =
            NodeId::from_legacy_named("node_daemon_doctor_routed_endpoint_source").expect("source");
        let bridge =
            NodeId::from_legacy_named("node_daemon_doctor_routed_endpoint_bridge").expect("bridge");
        let target =
            NodeId::from_legacy_named("node_daemon_doctor_routed_endpoint_target").expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "doctor routed endpoint",
            source.clone(),
            "source",
            "pk-source",
            now,
        );
        let records = vec![
            doctor_connectivity_record(
                &cluster_id,
                &source,
                &target,
                "endpoint_daemon_doctor_routed_unreachable",
                "172.25.0.3:21353",
                sinomo_core::network::ClusterConnectivityState::Unreachable,
            ),
            doctor_connectivity_record(
                &cluster_id,
                &source,
                &bridge,
                "endpoint_daemon_doctor_routed_source_bridge",
                "172.23.0.4:21353",
                sinomo_core::network::ClusterConnectivityState::Healthy,
            ),
            doctor_connectivity_record(
                &cluster_id,
                &bridge,
                &target,
                "endpoint_daemon_doctor_routed_bridge_target",
                "172.25.0.3:21353",
                sinomo_core::network::ClusterConnectivityState::Healthy,
            ),
        ];

        let findings = sinomo_core::cluster::cluster_doctor_findings(&state, &records, &[]);

        assert!(
            !findings
                .iter()
                .any(|finding| finding.code == "endpoint-unreachable"),
            "an unreachable direct endpoint should not warn when the target is healthy via a bridge: {findings:?}"
        );
    }

    #[test]
    fn cluster_doctor_suppresses_unreachable_endpoint_when_target_is_online() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_doctor_online_endpoint").expect("cluster");
        let source =
            NodeId::from_legacy_named("node_daemon_doctor_online_endpoint_source").expect("source");
        let target =
            NodeId::from_legacy_named("node_daemon_doctor_online_endpoint_target").expect("target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "doctor online endpoint",
            source.clone(),
            "source",
            "pk-source",
            now,
        );
        let target_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        state.members.push(target_member.clone());
        let records = vec![doctor_connectivity_record(
            &cluster_id,
            &source,
            &target,
            "endpoint_daemon_doctor_online_unreachable",
            "172.25.0.3:21353",
            sinomo_core::network::ClusterConnectivityState::Unreachable,
        )];
        let health = vec![ClusterHealthRow {
            member: target_member.clone(),
            status: sinomo_core::cluster::unknown_cluster_node_status(&state, &target_member, now),
            availability: ClusterNodeAvailability::Online,
            live_probe_observed: false,
        }];

        let findings = sinomo_core::cluster::cluster_doctor_findings(&state, &records, &health);

        assert!(
            !findings
                .iter()
                .any(|finding| finding.code == "endpoint-unreachable"),
            "direct endpoint failures should not warn while health proves the target is reachable"
        );
    }

    #[test]
    fn cluster_doctor_warns_unreachable_endpoint_without_healthy_route() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_doctor_unrouted_endpoint")
            .expect("cluster");
        let source = NodeId::from_legacy_named("node_daemon_doctor_unrouted_endpoint_source")
            .expect("source");
        let target = NodeId::from_legacy_named("node_daemon_doctor_unrouted_endpoint_target")
            .expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "doctor unrouted endpoint",
            source.clone(),
            "source",
            "pk-source",
            now,
        );
        let records = vec![doctor_connectivity_record(
            &cluster_id,
            &source,
            &target,
            "endpoint_daemon_doctor_unrouted_unreachable",
            "172.25.0.3:21353",
            sinomo_core::network::ClusterConnectivityState::Unreachable,
        )];

        let findings = sinomo_core::cluster::cluster_doctor_findings(&state, &records, &[]);

        assert!(
            findings
                .iter()
                .any(|finding| finding.code == "endpoint-unreachable"),
            "a truly unrouted unreachable endpoint should still warn"
        );
    }

    #[test]
    fn network_info_noop_publish_reads_raft_when_materialized_snapshot_is_missing() {
        let (_root, paths, state) =
            seeded_single_node_cluster_for_publish_cache_test("network_info_cache");
        let now = OffsetDateTime::now_utc();
        let info = sinomo_core::network::build_cluster_network_info_record(
            &state,
            &state.local_node_id,
            now,
            CLUSTER_NETWORK_INFO_TTL_SECONDS,
        );
        let seeded = sync_cluster_network_info_to_kv(&paths, info.clone())
            .expect("seed network info")
            .expect("network info commit");
        assert!(
            cached_network_info_matches(&paths, &info),
            "the committed source value should warm the derived process cache"
        );
        let wal_path = paths.kv_paths().raft_wal_paths(&state.local_node_id).wal;
        let wal_len_after_seed = std::fs::metadata(&wal_path)
            .expect("wal after network info seed")
            .len();
        let unchanged =
            sync_cluster_network_info_to_kv(&paths, info.clone()).expect("stored match");
        assert!(
            unchanged.is_none(),
            "an unchanged source-owned network-info publish is terminal"
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after unchanged network info")
                .len(),
            wal_len_after_seed,
            "an unchanged publish must not append a barrier transaction"
        );
        std::fs::remove_file(&paths.kv_paths().snapshot).expect("remove snapshot");
        let recovered = sync_cluster_network_info_to_kv(&paths, info.clone())
            .expect("Raft state remains authoritative without the materialized snapshot");
        assert!(
            recovered.is_none(),
            "a missing materialized cache does not turn unchanged source state into a write"
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after raft-backed no-op")
                .len(),
            wal_len_after_seed
        );
        assert!(cached_network_info_matches(&paths, &info));
        let authoritative = sinomo_core::cluster::read_cluster_kv_current_snapshot(&paths)
            .expect("reconstruct authoritative snapshot from Raft");
        assert_eq!(authoritative.revision, seeded.revision);
        assert_eq!(
            authoritative
                .get(&cluster_network_info_key(&state.local_node_id))
                .and_then(|entry| entry.decode::<ClusterNetworkInfoRecord>().ok()),
            Some(info),
            "authoritative Raft state must retain the source-owned record"
        );
    }

    #[test]
    fn stable_cluster_state_sync_returns_current_snapshot_without_rewriting_materialized_cache() {
        let root = unique_cluster_test_root("sinomo-daemon-state-sync-current-snapshot");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_state_sync_current").expect("cluster");
        let local_node = NodeId::from_legacy_named("node_daemon_state_sync_current").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "state sync current",
            local_node.clone(),
            "local",
            "placeholder-public-key",
            now,
        );
        assign_local_node_public_keys(&paths, &mut state).expect("assign keys");
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        let current =
            sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
                .expect("seed raft");
        let member_key = cluster_member_key(&local_node);
        assert!(
            current.get(&member_key).is_some(),
            "raft snapshot should contain the local member"
        );
        let wal_path = paths.kv_paths().raft_wal_paths(&local_node).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before stable sync")
            .len();

        sinomo_core::kv::write_cluster_kv_snapshot(
            &paths.kv_paths(),
            &InProcessKvStore::new().snapshot().expect("snapshot"),
        )
        .expect("install stale materialized cache");
        let stale_materialized =
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
                .expect("stale materialized");
        assert!(
            stale_materialized.get(&member_key).is_none(),
            "test setup must make the materialized cache stale"
        );

        let returned = sync_cluster_state_to_kv(&paths, &state).expect("stable state sync");

        assert_eq!(
            returned.revision, current.revision,
            "stable sync should return the current raft-backed snapshot"
        );
        assert!(
            returned.get(&member_key).is_some(),
            "stable sync must not return the stale materialized fallback"
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after stable sync")
                .len(),
            wal_len_before,
            "stable sync must use ReadIndex quorum evidence without appending a write"
        );
        let materialized = sinomo_core::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .expect("materialized after sync");
        assert!(
            materialized.get(&member_key).is_none(),
            "ReadIndex must not rewrite the complete materialized snapshot for a stable read"
        );
    }

    #[test]
    fn cluster_network_info_ttl_keeps_stable_records_sparse() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_network_info_sparse").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_daemon_network_info_sparse_local").expect("local");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "network info sparse",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let info = sinomo_core::network::build_cluster_network_info_record(
            &state,
            &local_node,
            now,
            CLUSTER_NETWORK_INFO_TTL_SECONDS,
        );

        assert_eq!(
            info.expires_at,
            now + time::Duration::seconds(3600),
            "stable network-info records should not force short idle refresh cycles"
        );
        assert!(
            sinomo_core::network::cluster_network_info_publish_record_matches(
                &info,
                &info,
                now + time::Duration::minutes(30)
            ),
            "stable network info should remain fresh well past ordinary maintenance intervals"
        );
    }

    #[test]
    fn connectivity_probe_state_is_source_local_even_for_non_voters() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-non-voter");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_non_voter")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_non_voter_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_connectivity_non_voter_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity non voter",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut state.members[0],
            ClusterMemberRole::NonVoting,
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                cluster_connectivity_probe_key(&local_node, &endpoint),
                ClusterConnectivityProbeState {
                    record: record.clone(),
                },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let plan = maintenance.select_connectivity_probe_plan(&paths, &state);
        let records = maintenance.apply_connectivity_probe_plan(plan, Vec::new());

        assert_eq!(records, vec![record]);
        assert_eq!(records[0].source_node_id, local_node);
        assert_eq!(records[0].target_node_id, remote_node);
    }

    #[test]
    fn connectivity_probe_bounds_each_endpoint_before_collecting_batch() {
        let runtime = daemon_cluster_test_runtime();
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_probe_endpoint_timeout")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_probe_endpoint_timeout_local")
            .expect("local node");
        let fast_node = NodeId::from_legacy_named("node_daemon_probe_endpoint_timeout_fast")
            .expect("fast node");
        let slow_node = NodeId::from_legacy_named("node_daemon_probe_endpoint_timeout_slow")
            .expect("slow node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity endpoint timeout",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let fast_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &fast_node,
            "203.0.113.81:21353".to_string(),
            10,
        );
        let slow_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &slow_node,
            "203.0.113.82:21353".to_string(),
            10,
        );
        for (node, name, endpoint) in [
            (fast_node.clone(), "fast", fast_endpoint.clone()),
            (slow_node.clone(), "slow", slow_endpoint.clone()),
        ] {
            let mut member =
                ClusterMemberRecord::local_voter(cluster_id.clone(), node, name, "pk", now);
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![endpoint];
            state.members.push(member);
        }
        let entries = vec![
            sinomo_core::network::ClusterConnectivityProbeBatchEntry {
                key: cluster_connectivity_probe_key(&local_node, &fast_endpoint),
                endpoint: fast_endpoint,
                previous: None,
            },
            sinomo_core::network::ClusterConnectivityProbeBatchEntry {
                key: cluster_connectivity_probe_key(&local_node, &slow_endpoint),
                endpoint: slow_endpoint,
                previous: None,
            },
        ];
        let slow_node_for_fetch = slow_node.clone();
        let started = Instant::now();
        let records = runtime.block_on(probe_connectivity_batch_with_fetch_async(
            state,
            entries,
            "test".to_string(),
            now,
            false,
            Duration::from_millis(25),
            move |endpoint, _timeout| {
                let slow_node = slow_node_for_fetch.clone();
                async move {
                    if endpoint.node_id == slow_node {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                    Err(DaemonError::Network {
                        address: endpoint.address,
                        detail: "test probe failure".to_string(),
                    })
                }
            },
        ));

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "one hanging endpoint must not hold the whole probe batch open"
        );
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .any(|(_, record)| record.target_node_id == fast_node)
        );
        assert!(
            records
                .iter()
                .any(|(_, record)| record.target_node_id == slow_node)
        );
    }

    #[test]
    fn connectivity_environment_change_clears_persisted_local_probe_graph() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-env-clear");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_env_clear")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_env_clear_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_connectivity_env_clear_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity env clear",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.42:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_core::network::__connectivity_store::record_daemon_owned_local_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            true,
        )
        .expect("persist local connectivity");
        assert!(
            paths.connectivity_local.exists(),
            "test setup should persist the local probe graph"
        );

        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                cluster_connectivity_probe_key(&local_node, &endpoint),
                ClusterConnectivityProbeState { record },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: Some("old-environment".to_string()),
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: Instant::now(),
            last_discovery: Instant::now(),
            last_probe: Instant::now(),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        sinomo_core::cluster::refresh_cluster_connectivity_environment(
            &paths,
            &state,
            &mut maintenance.environment_id,
            &mut maintenance.connectivity,
            OffsetDateTime::now_utc(),
        )
        .expect("refresh environment");

        assert!(
            maintenance.connectivity.is_empty(),
            "network changes should clear in-memory probe state"
        );
        assert!(
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &local_node
            )
            .expect("local connectivity after environment change")
            .is_empty(),
            "network changes should clear persisted local probe state"
        );
        assert!(
            !paths.connectivity_local.exists(),
            "clearing persisted local probe state should remove the object file"
        );
    }

    #[test]
    fn connectivity_probe_suppresses_fresh_unreachable_until_refresh_deadline() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-unreachable-sleep");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_connectivity_unreachable_sleep")
                .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_connectivity_unreachable_sleep_local")
                .expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_connectivity_unreachable_sleep_remote")
                .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity unreachable sleep",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.53:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        let key = cluster_connectivity_probe_key(&local_node, &endpoint);
        let maintenance_now = Instant::now();
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                key,
                ClusterConnectivityProbeState {
                    record: record.clone(),
                },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let plan = maintenance.select_connectivity_probe_plan(&paths, &state);
        let records = maintenance.apply_connectivity_probe_plan(plan, Vec::new());

        assert_eq!(records, vec![record]);
        assert_eq!(
            idle_stats.snapshot().connectivity_probes_sent,
            0,
            "fresh unreachable endpoints should not burn battery by probing every tick"
        );
    }

    #[test]
    fn connectivity_probe_retries_unreachable_after_role_interval() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_unreachable_reprobe").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_unreachable_reprobe_local").expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_unreachable_reprobe_remote")
            .expect("remote node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "unreachable reprobe",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.54:21353".to_string(),
            10,
        );
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );

        assert!(
            !cluster_connectivity_probe_due(
                &record,
                now,
                now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
            ),
            "fresh unreachable endpoints should not be retried immediately"
        );

        let workstation_retry =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        assert!(
            cluster_connectivity_probe_due(
                &record,
                workstation_retry,
                workstation_retry
                    + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
            ),
            "workstation nodes should re-probe unreachable endpoints on the normal probe cadence"
        );
        assert!(
            !cluster_connectivity_probe_due(
                &record,
                workstation_retry,
                workstation_retry
                    + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Mobile,
            ),
            "mobile nodes should keep the longer unreachable re-probe cadence"
        );

        let mobile_retry = now
            + time::Duration::seconds(
                sinomo_core::network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL.as_secs() as i64,
            );
        assert!(
            cluster_connectivity_probe_due(
                &record,
                mobile_retry,
                mobile_retry + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Mobile,
            ),
            "mobile unreachable endpoints should still recover without waiting for the long TTL"
        );
    }

    #[test]
    fn connectivity_probe_paces_degraded_retry_by_role_interval() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_degraded_reprobe").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_degraded_reprobe_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_degraded_reprobe_remote").expect("remote node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "degraded reprobe",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.55:21353".to_string(),
            10,
        );
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        assert!(
            !cluster_connectivity_probe_due(
                &record,
                now + time::Duration::milliseconds(250),
                now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
            ),
            "rapid convergence ticks must not escalate failures faster than the role probe cadence"
        );

        let retry_at =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        assert!(
            cluster_connectivity_probe_due(
                &record,
                retry_at,
                retry_at + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
            ),
            "degraded links should still retry at the normal workstation cadence"
        );
    }

    #[test]
    fn connectivity_probe_retries_degraded_during_startup_settle() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_startup_degraded_reprobe")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_startup_degraded_reprobe_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_startup_degraded_reprobe_remote")
            .expect("remote node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "startup degraded reprobe",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.56:21353".to_string(),
            10,
        );
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        assert!(
            sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                &record,
                now + time::Duration::milliseconds(250),
                now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
                true,
            ),
            "startup settle must re-probe degraded first-pass records without waiting for the normal workstation cadence"
        );
    }

    #[test]
    fn connectivity_probe_revalidates_old_healthy_during_startup_settle() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_startup_healthy_reprobe")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_startup_healthy_reprobe_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_startup_healthy_reprobe_remote")
            .expect("remote node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "startup healthy reprobe",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.57:21353".to_string(),
            10,
        );
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let retry_at =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        let refresh_deadline =
            retry_at + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);

        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                &record,
                now + time::Duration::milliseconds(250),
                now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
                true,
            ),
            "fresh healthy records should not churn every startup-settle tick"
        );
        assert!(
            sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                &record,
                retry_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                true,
            ),
            "startup settle should revalidate old healthy records from the previous daemon"
        );
        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_startup_settle(
                &record,
                retry_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                false,
            ),
            "steady state keeps healthy records until refresh margin"
        );
    }

    #[test]
    fn connectivity_probe_suppresses_rerouted_unhealthy_endpoint() {
        let now = OffsetDateTime::now_utc();
        let due_at =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        let refresh_deadline =
            due_at + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_rerouted_unreachable_probe")
            .expect("cluster id");
        let local = NodeId::from_legacy_named("node_daemon_rerouted_probe_local").expect("local");
        let bridge =
            NodeId::from_legacy_named("node_daemon_rerouted_probe_bridge").expect("bridge");
        let target =
            NodeId::from_legacy_named("node_daemon_rerouted_probe_target").expect("target");
        let target_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&target, "10.0.0.63:21353".to_string(), 10);
        let mut unreachable_direct = doctor_connectivity_record(
            &cluster_id,
            &local,
            &target,
            "endpoint_daemon_rerouted_direct",
            &target_endpoint.address,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
        );
        unreachable_direct.observed_at = now;
        unreachable_direct.expires_at = now + time::Duration::hours(1);
        let mut degraded_direct = unreachable_direct.clone();
        degraded_direct.state = sinomo_core::network::ClusterConnectivityState::Degraded;
        degraded_direct.consecutive_failures = 1;
        let local_to_bridge = doctor_connectivity_record(
            &cluster_id,
            &local,
            &bridge,
            "endpoint_daemon_rerouted_local_bridge",
            "10.0.0.64:21353",
            sinomo_core::network::ClusterConnectivityState::Healthy,
        );
        let bridge_to_target = doctor_connectivity_record(
            &cluster_id,
            &bridge,
            &target,
            "endpoint_daemon_rerouted_bridge_target",
            "10.0.0.65:21353",
            sinomo_core::network::ClusterConnectivityState::Healthy,
        );
        let graph = sinomo_core::network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![
                unreachable_direct.clone(),
                local_to_bridge,
                bridge_to_target,
            ],
        );

        // A healthy alternate route means the direct endpoint is no longer load-bearing.
        // Suppress normal-cadence re-probes and let near-expiry refresh handle recovery.
        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_graph(
                &unreachable_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a due unreachable direct endpoint should not be reprobed while a healthy alternate route exists"
        );
        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_graph(
                &degraded_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a due degraded direct endpoint should not be reprobed while a healthy alternate route exists"
        );
    }

    #[test]
    fn connectivity_probe_retries_identity_mismatch_even_with_alternate_route() {
        let now = OffsetDateTime::now_utc();
        let due_at =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        let refresh_deadline =
            due_at + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_rerouted_identity_probe")
            .expect("cluster id");
        let local = NodeId::from_legacy_named("node_daemon_identity_probe_local").expect("local");
        let bridge =
            NodeId::from_legacy_named("node_daemon_identity_probe_bridge").expect("bridge");
        let target =
            NodeId::from_legacy_named("node_daemon_identity_probe_target").expect("target");
        let target_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&target, "10.0.0.67:21353".to_string(), 10);
        let mut identity_mismatch = doctor_connectivity_record(
            &cluster_id,
            &local,
            &target,
            "endpoint_daemon_identity_mismatch_direct",
            &target_endpoint.address,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
        );
        identity_mismatch.observed_at = now;
        identity_mismatch.expires_at = now + time::Duration::hours(1);
        identity_mismatch.failure_reason =
            Some(sinomo_core::network::ClusterConnectivityFailureReason::IdentityMismatch);
        let local_to_bridge = doctor_connectivity_record(
            &cluster_id,
            &local,
            &bridge,
            "endpoint_daemon_identity_probe_local_bridge",
            "10.0.0.68:21353",
            sinomo_core::network::ClusterConnectivityState::Healthy,
        );
        let bridge_to_target = doctor_connectivity_record(
            &cluster_id,
            &bridge,
            &target,
            "endpoint_daemon_identity_probe_bridge_target",
            "10.0.0.69:21353",
            sinomo_core::network::ClusterConnectivityState::Healthy,
        );
        let graph = sinomo_core::network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![identity_mismatch.clone(), local_to_bridge, bridge_to_target],
        );

        assert!(
            !sinomo_core::network::cluster_connectivity_probe_due_with_graph(
                &identity_mismatch,
                &target_endpoint,
                now,
                now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS),
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "fresh identity mismatches should not churn immediately"
        );
        assert!(
            sinomo_core::network::cluster_connectivity_probe_due_with_graph(
                &identity_mismatch,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "identity mismatches are hard route exclusions, so alternate routes must not defer their recovery probe until expiry"
        );
    }

    #[test]
    fn connectivity_probe_retries_unreachable_endpoint_without_route() {
        let now = OffsetDateTime::now_utc();
        let due_at =
            now + time::Duration::seconds(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL.as_secs() as i64);
        let refresh_deadline =
            due_at + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_unrouted_unreachable_probe")
            .expect("cluster id");
        let local = NodeId::from_legacy_named("node_daemon_unrouted_probe_local").expect("local");
        let target =
            NodeId::from_legacy_named("node_daemon_unrouted_probe_target").expect("target");
        let target_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&target, "10.0.0.66:21353".to_string(), 10);
        let mut unreachable_direct = doctor_connectivity_record(
            &cluster_id,
            &local,
            &target,
            "endpoint_daemon_unrouted_direct",
            &target_endpoint.address,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
        );
        unreachable_direct.observed_at = now;
        unreachable_direct.expires_at = now + time::Duration::hours(1);
        let graph = sinomo_core::network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![unreachable_direct.clone()],
        );

        assert!(
            sinomo_core::network::cluster_connectivity_probe_due_with_graph(
                &unreachable_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a genuinely unrouted peer should still be retried on the role cadence"
        );
    }

    #[test]
    fn command_connectivity_failures_mark_endpoint_unreachable_at_threshold() {
        let root = unique_cluster_test_root("sinomo-daemon-command-connectivity-threshold");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_command_connectivity")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_command_connectivity_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_command_connectivity_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "command connectivity threshold",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let tcp = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.53:21353".to_string(),
            10,
        );
        let public_tcp = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_command_connectivity_public",
            )
            .expect("endpoint"),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.53:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![tcp.clone(), public_tcp.clone()];
        state.members.push(remote_member);

        let read_tcp_record = || {
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &local_node,
            )
            .expect("local connectivity records")
            .into_iter()
            .find(|record| {
                record.target_node_id == remote_node && record.endpoint_id == tcp.endpoint_id
            })
            .expect("tcp connectivity record")
        };

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(8),
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.latency_ms, Some(8));
        assert!(
            !cached_connectivity_matches(&paths, &record),
            "command-path observations must not masquerade as quorum-published connectivity"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            "first command-path failure after a healthy send should keep the direct edge usable"
        );
        assert_eq!(record.consecutive_failures, 1);
        assert!(
            record.last_success_at.is_some(),
            "transient command-path failures should retain the last success timestamp"
        );

        for expected_failures in 2..CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            remember_cluster_endpoint_command_connectivity(
                &paths,
                &state,
                &tcp,
                sinomo_core::network::ClusterConnectivityState::Degraded,
                None,
                None,
            );
            let record = read_tcp_record();
            assert_eq!(
                record.state,
                sinomo_core::network::ClusterConnectivityState::Degraded
            );
            assert_eq!(record.consecutive_failures, expected_failures);
            assert!(
                record.last_success_at.is_some(),
                "short command-path failures should retain the last success timestamp"
            );
        }

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Unreachable
        );
        assert_eq!(
            record.consecutive_failures,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
        );
        assert!(
            record.last_success_at.is_none(),
            "sustained command-path failures should stop advertising a stale success"
        );

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&public_tcp.endpoint_id),
            "unreachable private command endpoint should not outrank public fallback"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(12),
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.latency_ms, Some(12));
        assert!(
            record.last_failure_at.is_some(),
            "healthy recovery keeps the last failure timestamp as diagnostic history"
        );

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "recovered direct command endpoint should become the preferred path again"
        );

        remember_cluster_endpoint_command_connectivity_without_failure_grace(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Degraded,
            "auth-style command failures should bypass transient grace without proving identity mismatch"
        );
        assert_eq!(record.consecutive_failures, 1);
        assert!(
            record.last_success_at.is_some(),
            "non-identity hard failures should retain recovery context before the failure budget is exhausted"
        );
        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(13),
            None,
        );
        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "a later successful command send should restore a hard-degraded endpoint"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            "identity failures should retire the exact endpoint immediately"
        );
        assert_eq!(
            record.consecutive_failures,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
        );
        assert!(
            record.last_success_at.is_none(),
            "identity failures must not retain stale successful reachability"
        );
        assert_eq!(
            record.failure_reason,
            Some(sinomo_core::network::ClusterConnectivityFailureReason::IdentityMismatch)
        );
        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&public_tcp.endpoint_id),
            "identity-failed private command endpoint should not outrank public fallback"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(14),
            None,
        );
        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "a later successful command send should restore the direct endpoint"
        );
    }

    #[test]
    fn connectivity_probe_mobile_role_limits_due_endpoint_batch() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-mobile-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_mobile_budget")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_mobile_budget_local")
            .expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity mobile budget",
            local_node,
            "local",
            "pk-local",
            now,
        );
        state.members[0].node_role = ClusterNodeRole::Mobile;
        for index in 0..6 {
            let remote_node = NodeId::from_legacy_named(format!(
                "node_daemon_connectivity_mobile_budget_remote_{index}"
            ))
            .expect("remote node");
            let endpoint = sinomo_core::network::daemon_lan_endpoint(
                &remote_node,
                format!("127.0.0.1:{}", 23100 + index),
                10,
            );
            let mut remote_member = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                remote_node,
                format!("remote {index}"),
                format!("pk-remote-{index}"),
                now,
            );
            remote_member.capabilities =
                sinomo_core::node::main_daemon_capabilities(&remote_member.role);
            remote_member.allowed_capabilities = remote_member.capabilities.clone();
            remote_member.endpoints = vec![endpoint];
            state.members.push(remote_member);
        }

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let plan = maintenance.select_connectivity_probe_plan(&paths, &state);
        let expected_batch_len =
            sinomo_core::network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK;
        assert_eq!(
            plan.entries.len(),
            expected_batch_len,
            "mobile nodes should not probe every due remote endpoint in one idle tick"
        );
        let probed = plan
            .entries
            .iter()
            .cloned()
            .map(|entry| {
                connectivity_record_for_daemon_status_probe(
                    &state,
                    entry,
                    Err(DaemonError::Network {
                        address: "synthetic-connectivity-test".to_string(),
                        detail: "unreachable".to_string(),
                    }),
                    0,
                    &plan.timestamp,
                    plan.now,
                    false,
                )
            })
            .collect();
        let records = maintenance.apply_connectivity_probe_plan(plan, probed);

        assert_eq!(
            idle_stats.snapshot().connectivity_probes_sent,
            expected_batch_len as u64,
            "mobile nodes should not probe every due remote endpoint in one idle tick"
        );
        assert_eq!(
            records.len(),
            expected_batch_len,
            "only the probed mobile batch should become publishable records"
        );
    }

    #[test]
    fn connectivity_probe_drops_records_for_removed_endpoints() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-removed-endpoint");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_removed")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_removed_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_connectivity_removed_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity removed endpoint",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.52:21353".to_string(),
            10,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == remote_node)
            .expect("remote member")
            .endpoints
            .clear();

        let key = cluster_connectivity_probe_key(&local_node, &endpoint);
        let maintenance_now = Instant::now();
        let mut maintenance = cluster_network_maintenance! {
            retained_connectivity_publish: None,
            connectivity_input_generation: 0,
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                key.clone(),
                ClusterConnectivityProbeState { record },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            local_raft_resident_initialization: LocalRaftResidentInitialization::Pending,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_force_probe: false,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let plan = maintenance.select_connectivity_probe_plan(&paths, &state);
        let records = maintenance.apply_connectivity_probe_plan(plan, Vec::new());

        assert!(
            records.is_empty(),
            "removed endpoints should not keep stale connectivity records publishable"
        );
        assert!(
            !maintenance.connectivity.contains_key(&key),
            "removed endpoint connectivity should be dropped from the local cache"
        );
    }

    #[test]
    fn endpoint_rotation_removes_obsolete_connectivity_generation() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-cached-delete");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_cached_delete")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_cached_delete_local")
            .expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_connectivity_cached_delete_remote")
                .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity cached delete",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let current_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.61:21353".to_string(),
            10,
        );
        let stale_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.62:21353".to_string(),
            20,
        );
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![current_endpoint.clone(), stale_endpoint.clone()];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");

        let current_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &current_endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        let stale_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &stale_endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(9),
            0,
            Some("old".to_string()),
            None,
            now,
            600,
        );
        let current_key = cluster_connectivity_key(
            &current_record.source_node_id,
            &current_record.target_node_id,
            &current_record.endpoint_id,
        );
        let stale_key = cluster_connectivity_key(
            &stale_record.source_node_id,
            &stale_record.target_node_id,
            &stale_record.endpoint_id,
        );
        assert_ne!(
            current_key, stale_key,
            "test endpoints must use distinct connectivity keys"
        );
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![current_record.clone(), stale_record.clone()],
        )
        .expect("seed connectivity records");
        remember_connectivity_record_set(
            &paths,
            &local_node,
            &[current_record.clone(), stale_record.clone()],
        );
        let seeded = sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            local_node.clone(),
        )
        .expect("seeded snapshot");
        assert!(
            seeded.get(&current_key).is_some(),
            "current connectivity should be seeded"
        );
        assert!(
            seeded.get(&stale_key).is_some(),
            "stale connectivity should be seeded"
        );
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == remote_node)
            .expect("remote member")
            .endpoints = vec![current_endpoint.clone()];
        sinomo_core::cluster::write_cluster_state(&paths, &state)
            .expect("publish rotated endpoint topology");
        sync_cluster_connectivity_records_to_kv(&paths, &local_node, vec![current_record.clone()])
            .expect("sync should delete stale connectivity");

        let snapshot = sinomo_core::kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            local_node.clone(),
        )
        .expect("snapshot");
        assert!(
            snapshot.get(&current_key).is_some(),
            "current connectivity should remain"
        );
        assert!(
            snapshot.get(&stale_key).is_none(),
            "stale connectivity should be deleted even when current records are cached"
        );
    }

    #[test]
    fn connectivity_sync_batches_large_stale_kv_deletions() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-large-delete");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_large_delete")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_connectivity_large_delete_local")
            .expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "connectivity large delete",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");

        let mut stale_records = Vec::new();
        for index in 0..=sinomo_core::kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND {
            let target_node = NodeId::from_legacy_named(format!(
                "node_daemon_connectivity_large_delete_target_{index}"
            ))
            .expect("target node");
            let endpoint = sinomo_core::network::daemon_lan_endpoint(
                &target_node,
                format!("10.64.{}.{}:21353", index / 256, index % 256),
                10,
            );
            stale_records.push(sinomo_core::network::cluster_connectivity_record(
                &state,
                &endpoint,
                sinomo_core::network::ClusterConnectivityState::Healthy,
                Some(11),
                0,
                Some("stale".to_string()),
                None,
                now,
                600,
            ));
        }
        let mut stale_ops = Vec::new();
        for record in &stale_records {
            stale_ops.push(
                KvOp::put_serde(
                    cluster_connectivity_key(
                        &record.source_node_id,
                        &record.target_node_id,
                        &record.endpoint_id,
                    ),
                    record,
                    None,
                )
                .expect("stale connectivity put op"),
            );
            if stale_ops.len() >= sinomo_core::kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND {
                append_cluster_raft_command(
                    &paths,
                    &local_node,
                    KvCommand::Txn(KvTxn {
                        compares: Vec::new(),
                        success: std::mem::take(&mut stale_ops),
                        failure: Vec::new(),
                    }),
                )
                .expect("seed stale connectivity batch");
            }
        }
        if !stale_ops.is_empty() {
            append_cluster_raft_command(
                &paths,
                &local_node,
                KvCommand::Txn(KvTxn {
                    compares: Vec::new(),
                    success: stale_ops,
                    failure: Vec::new(),
                }),
            )
            .expect("seed final stale connectivity batch");
        }

        sync_cluster_connectivity_records_to_kv(&paths, &local_node, Vec::new())
            .expect("delete stale connectivity in batches");

        let records =
            sinomo_core::cluster::read_cluster_connectivity_graph_from_kv(&paths).expect("records");
        assert!(
            records
                .iter()
                .all(|record| record.source_node_id != local_node),
            "daemon connectivity sync should delete large stale prefixes in bounded batches"
        );
    }

    #[test]
    fn raft_command_targets_preserve_connectivity_rank_for_voter_paths() {
        let root = unique_cluster_test_root("sinomo-daemon-raft-connectivity-order");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_raft_connectivity").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_raft_connectivity_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_raft_connectivity_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "raft connectivity order",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let tcp = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let public_tcp = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_raft_connectivity_public",
            )
            .expect("endpoint"),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.41:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![tcp.clone(), public_tcp.clone()];
        state.members.push(remote_member);

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft kv");
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &tcp,
                    sinomo_core::network::ClusterConnectivityState::Unreachable,
                    None,
                    4,
                    None,
                    Some("timed out".to_string()),
                    now,
                    60,
                ),
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &public_tcp,
                    sinomo_core::network::ClusterConnectivityState::Healthy,
                    Some(40),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("connectivity records");

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);

        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&public_tcp.endpoint_id),
            "raft/KV forwarding must not spend the command budget on an unreachable private TCP endpoint before a healthy public TCP path"
        );
        assert!(
            targets
                .iter()
                .any(|endpoint| endpoint.endpoint_id == tcp.endpoint_id)
        );
    }

    #[test]
    fn member_publish_targets_prefer_reachable_bridge_before_stale_voter() {
        let root = unique_cluster_test_root("sinomo-daemon-member-publish-bridge-target");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_member_publish_bridge")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_member_publish_leaf").expect("local node");
        let voter_node =
            NodeId::from_legacy_named("node_daemon_member_publish_voter").expect("voter node");
        let bridge_node =
            NodeId::from_legacy_named("node_daemon_member_publish_bridge").expect("bridge node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "member publish bridge target",
            local_node.clone(),
            "local leaf",
            "pk-local",
            now,
        );

        let voter_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &voter_node,
            "10.0.0.10:21353".to_string(),
            10,
        );
        let bridge_member_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &bridge_node,
            "10.0.0.20:21353".to_string(),
            10,
        );
        let bridge_learned_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&bridge_node, "node4:21353".to_string(), 5);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node,
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_core::node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint.clone()];
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            bridge_node,
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_member_endpoint.clone()];
        state.members.push(voter_member);
        state.members.push(bridge_member);

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft kv");
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &voter_endpoint,
                    sinomo_core::network::ClusterConnectivityState::Unreachable,
                    None,
                    4,
                    None,
                    Some("stale voter address".to_string()),
                    now,
                    60,
                ),
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &bridge_learned_endpoint,
                    sinomo_core::network::ClusterConnectivityState::Healthy,
                    Some(3),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("connectivity records");

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::MemberPublish);

        assert_eq!(
            targets
                .first()
                .map(|endpoint| (&endpoint.node_id, endpoint.address.as_str())),
            Some((
                &bridge_learned_endpoint.node_id,
                bridge_learned_endpoint.address.as_str()
            )),
            "roaming learners should hand member publishes to the learned reachable bridge address before burning time on stale member endpoints"
        );
        assert!(
            targets
                .iter()
                .any(|endpoint| endpoint.node_id == voter_endpoint.node_id),
            "voter endpoints should remain fallback publish targets"
        );
    }

    #[test]
    fn join_channel_endpoint_falls_back_to_invite_issuer_when_host_is_seed_alias() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_join_seed_alias").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_join_seed_alias_local").expect("local node");
        let issuer_node =
            NodeId::from_legacy_named("node_daemon_join_seed_alias_issuer").expect("issuer node");
        let channel_node =
            NodeId::from_legacy_named("node_daemon_join_seed_alias_channel").expect("channel node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "join seed alias",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut issuer_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            issuer_node.clone(),
            "dev-cluster",
            "pk-issuer",
            now,
        );
        issuer_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&issuer_member.role);
        issuer_member.allowed_capabilities = issuer_member.capabilities.clone();
        issuer_member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &issuer_node,
            "172.24.0.2:21353".to_string(),
            20,
        )];
        state.members.push(issuer_member);

        let channel_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&channel_node, "node1:21353".to_string(), 0);
        let invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::from_legacy_named("invite_daemon_join_seed_alias")
                .expect("invite id"),
            cluster_id,
            cluster_name: Some("join seed alias".to_string()),
            cluster_public_key: "pk-issuer".to_string(),
            invitation_secret: "secret".to_string(),
            communication_channels: vec![sinomo_core::ClusterInvitationChannel {
                endpoint: channel_endpoint.clone(),
                node_public_key: "pk-issuer".to_string(),
            }],
            introducers: Vec::new(),
            token_id: String::new(),
            expires_at: now + time::Duration::hours(1),
            trust_domain: TrustDomain::new("trust.local").expect("trust domain"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: issuer_node.clone(),
            issuer_public_key: "pk-issuer".to_string(),
            signature: String::new(),
        };

        let retargeted = sinomo_core::cluster::join_channel_endpoint_for_accepted_state(
            &state,
            &invite,
            &channel_endpoint,
        );

        assert_eq!(retargeted.node_id, issuer_node);
        assert_eq!(retargeted.address, "node1:21353");
    }

    #[test]
    fn member_publish_targets_preserve_locally_learned_bridge_endpoint_after_kv_overlay() {
        let root = unique_cluster_test_root("sinomo-daemon-member-publish-local-bridge-target");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_member_publish_local_bridge")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_member_publish_leaf").expect("local node");
        let voter_node =
            NodeId::from_legacy_named("node_daemon_member_publish_voter").expect("voter node");
        let bridge_node =
            NodeId::from_legacy_named("node_daemon_member_publish_bridge").expect("bridge node");
        let mut kv_state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "member publish local bridge target",
            local_node,
            "local leaf",
            "pk-local",
            now,
        );

        let voter_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &voter_node,
            "10.0.0.10:21353".to_string(),
            10,
        );
        let bridge_stale_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &bridge_node,
            "10.0.0.20:21353".to_string(),
            10,
        );
        let bridge_learned_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&bridge_node, "node3:21353".to_string(), 5);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node,
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_core::node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint];
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            bridge_node,
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_stale_endpoint];
        kv_state.members.push(voter_member);
        kv_state.members.push(bridge_member);

        sinomo_core::cluster::write_cluster_state(&paths, &kv_state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &kv_state)
            .expect("sync state to raft kv");

        let mut local_state = kv_state.clone();
        let local_bridge = local_state
            .members
            .iter_mut()
            .find(|member| member.node_id == bridge_learned_endpoint.node_id)
            .expect("bridge member");
        local_bridge.endpoints.push(bridge_learned_endpoint.clone());
        sinomo_core::cluster::write_cluster_state(&paths, &local_state)
            .expect("write local learned route");

        let merged_state =
            sinomo_core::cluster::read_cluster_state(&paths).expect("read merged state");
        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &merged_state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::MemberPublish);

        assert_eq!(
            targets
                .first()
                .map(|endpoint| (&endpoint.node_id, endpoint.address.as_str())),
            Some((
                &bridge_learned_endpoint.node_id,
                bridge_learned_endpoint.address.as_str()
            )),
            "local join-channel bridge routes must survive KV overlays and outrank stale advertised IPs"
        );
    }

    #[test]
    fn non_voter_member_publish_skips_when_local_members_already_match_kv() {
        let root = unique_cluster_test_root("sinomo-daemon-member-publish-kv-match");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_member_publish_kv_match")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_member_publish_kv_match").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "member publish kv match",
            local_node,
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("seed local kv snapshot");

        let runtime = daemon_cluster_test_runtime();
        publish_local_members_to_quorum_if_needed_on_runtime(&paths, &state, runtime.handle())
            .expect("stable local members should not be republished");
    }

    #[test]
    fn raft_command_targets_skip_unclaimed_module_fallbacks() {
        let root = unique_cluster_test_root("sinomo-daemon-raft-unknown-tcp-order");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_raft_unknown_tcp").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_raft_unknown_tcp_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_raft_unknown_tcp_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "raft unknown tcp order",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let public_tcp = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "203.0.113.41:21353".to_string(),
            10,
        );
        let private_tcp = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            20,
        );
        let iroh = test_network_module_relay_endpoint(
            &remote_node,
            "module_public_key_daemon_raft_unknown_tcp",
        );
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![public_tcp.clone(), private_tcp.clone(), iroh.clone()];
        state.members.push(remote_member);

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft kv");

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);

        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&private_tcp.endpoint_id),
            "raft/KV forwarding should try bounded TCP first when neither path has current connectivity data"
        );
        assert!(
            targets
                .iter()
                .all(|endpoint| endpoint.endpoint_id != iroh.endpoint_id),
            "unclaimed module endpoints must not be command targets"
        );
        let public_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == public_tcp.endpoint_id)
            .expect("public tcp target");
        let private_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == private_tcp.endpoint_id)
            .expect("private tcp target");
        assert!(private_position < public_position);
        assert!(
            targets
                .iter()
                .any(|endpoint| endpoint.endpoint_id == public_tcp.endpoint_id)
        );
        assert!(
            targets
                .iter()
                .any(|endpoint| endpoint.endpoint_id == private_tcp.endpoint_id)
        );
    }

    #[test]
    fn direct_status_probe_prefers_healthy_selected_endpoint() {
        let root = unique_cluster_test_root("sinomo-daemon-direct-status-selected-endpoint");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_direct_status").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_direct_status_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_direct_status_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "direct status endpoint order",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let stale_private = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let healthy_public = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_direct_status_public",
            )
            .expect("endpoint"),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.41:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![stale_private.clone(), healthy_public.clone()];
        state.members.push(remote_member.clone());

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync cluster state");
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &stale_private,
                    sinomo_core::network::ClusterConnectivityState::Unreachable,
                    None,
                    4,
                    None,
                    Some("stale private address".to_string()),
                    now,
                    60,
                ),
                sinomo_core::network::cluster_connectivity_record(
                    &state,
                    &healthy_public,
                    sinomo_core::network::ClusterConnectivityState::Healthy,
                    Some(84),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("connectivity records");

        let endpoints = member_daemon_status_direct_probe_endpoints(&paths, &state, &remote_member);

        assert_eq!(
            endpoints.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&healthy_public.endpoint_id),
            "direct health status should try the selector-ranked healthy endpoint before stale private fallbacks"
        );
    }

    #[test]
    fn member_status_probe_prefers_known_multihop_route_over_unobserved_direct_endpoint() {
        let root = unique_cluster_test_root("sinomo-daemon-status-segmented-route-first");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_status_segmented").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_status_segmented_local").expect("local node");
        let bridge_node =
            NodeId::from_legacy_named("node_daemon_status_segmented_bridge").expect("bridge node");
        let remote_node =
            NodeId::from_legacy_named("node_daemon_status_segmented_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "segmented status route",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &bridge_node,
            "10.70.0.2:21353".to_string(),
            10,
        );
        let remote_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.71.0.2:21353".to_string(),
            10,
        );
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            bridge_node.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_endpoint.clone()];
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![remote_endpoint.clone()];
        state.members.push(bridge_member);
        state.members.push(remote_member.clone());

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync cluster state");
        let local_to_bridge = sinomo_core::network::cluster_connectivity_record(
            &state,
            &bridge_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut bridge_to_remote = sinomo_core::network::cluster_connectivity_record(
            &state,
            &remote_endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        bridge_to_remote.source_node_id = bridge_node.clone();
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![local_to_bridge],
        )
        .expect("local connectivity records");
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &bridge_node,
            vec![bridge_to_remote],
        )
        .expect("bridge connectivity records");

        let direct_endpoints =
            member_daemon_status_direct_probe_endpoints(&paths, &state, &remote_member);
        assert_eq!(
            direct_endpoints
                .first()
                .map(|endpoint| &endpoint.endpoint_id),
            Some(&remote_endpoint.endpoint_id)
        );
        assert!(
            member_daemon_status_should_probe_routed_first(
                &paths,
                &state,
                &remote_member,
                &direct_endpoints,
            ),
            "a healthy local-to-bridge-to-target path should be tried before an unobserved direct endpoint"
        );

        let graph = sinomo_core::network::ClusterConnectivityGraph::read_for_local(&paths, &state);
        assert_eq!(graph.state_for_endpoint(&remote_endpoint), None);
        let route = sinomo_core::network::cluster_route_next_hop(&graph, &local_node, &remote_node)
            .expect("segmented route");
        assert_eq!(route.next_hop, bridge_node);

        let mut stored =
            sinomo_core::cluster::unknown_cluster_node_status(&state, &remote_member, now);
        stored.availability = ClusterNodeAvailability::Offline;
        stored.daemon_version = "stored-version".to_string();
        let ping_observed_at = now + time::Duration::seconds(5);
        let refreshed = member_status_for_verified_routed_ping(
            &state,
            &remote_member,
            Some(stored),
            ping_observed_at,
        );
        assert_eq!(refreshed.availability, ClusterNodeAvailability::Online);
        assert_eq!(refreshed.observed_at, ping_observed_at);
        assert_eq!(refreshed.daemon_version, "stored-version");
    }

    #[test]
    fn peer_connectivity_import_uses_observed_reachable_main_endpoint() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_peer_connectivity_import").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_peer_connectivity_import_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_peer_connectivity_import_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "peer connectivity import",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let stale_static = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let observed_endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_peer_connectivity_import_observed",
            )
            .expect("endpoint id"),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.41:21353".to_string(),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: Some(42),
        };
        let mut remote_member =
            ClusterMemberRecord::local_voter(cluster_id, remote_node, "remote", "pk-remote", now);
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![stale_static];
        state.members.push(remote_member);

        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &observed_endpoint,
            ClusterConnectivityState::Degraded,
            Some(42),
            1,
            Some("ok".to_string()),
            None,
            now,
            60,
        );

        let endpoint =
            peer_connectivity_import_endpoint_for_record(&state, &record).expect("import endpoint");
        assert_eq!(endpoint.endpoint_id, observed_endpoint.endpoint_id);
        assert_eq!(endpoint.address, observed_endpoint.address);
        assert_eq!(endpoint.source, ClusterEndpointSource::Observed);
        assert_eq!(endpoint.privacy, ClusterEndpointPrivacy::Lan);
        assert_eq!(endpoint.last_observed_latency_ms, Some(42));
    }

    #[test]
    fn peer_connectivity_import_caps_endpoint_timeout() {
        let remote_node =
            NodeId::from_legacy_named("node_peer_import_timeout_remote").expect("remote node");
        let tcp = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let iroh = test_network_module_relay_endpoint(
            &remote_node,
            "module_public_key_peer_import_timeout",
        );

        let normal_timeouts = cluster_control_plane_timeouts();
        assert_eq!(
            connectivity_import_timeout_for_endpoint(&tcp, normal_timeouts),
            CLUSTER_CONNECTIVITY_IMPORT_ENDPOINT_TIMEOUT
        );
        assert_eq!(
            connectivity_import_timeout_for_endpoint(&iroh, normal_timeouts),
            CLUSTER_CONNECTIVITY_IMPORT_ENDPOINT_TIMEOUT
        );
    }

    #[test]
    fn peer_connectivity_fetch_scope_preserves_ordinary_source_filter_and_bounds_snapshot() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_peer_connectivity_fetch_scope").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_peer_connectivity_fetch_scope_local").expect("local");
        let responder_node =
            NodeId::from_legacy_named("node_peer_connectivity_fetch_scope_responder")
                .expect("responder");
        let upstream_node =
            NodeId::from_legacy_named("node_peer_connectivity_fetch_scope_upstream")
                .expect("upstream");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "peer connectivity fetch scope",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let responder_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &responder_node,
            "node4:21353".to_string(),
            10,
        );
        let mut responder_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &responder_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("responder".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        responder_record.source_node_id = responder_node.clone();
        let mut upstream_record = responder_record.clone();
        upstream_record.source_node_id = upstream_node;
        upstream_record.last_success_at = Some("upstream".to_string());

        let endpoint_only = filter_peer_connectivity_response_records(
            &state,
            &responder_endpoint,
            vec![responder_record.clone(), upstream_record.clone()],
            PeerConnectivityFetchScope::EndpointSource,
        )
        .expect("filter ordinary endpoint source");
        assert_eq!(endpoint_only, vec![responder_record.clone()]);

        let full = filter_peer_connectivity_response_records(
            &state,
            &responder_endpoint,
            vec![responder_record, upstream_record.clone()],
            PeerConnectivityFetchScope::FullSnapshot,
        )
        .expect("retain full handoff snapshot");
        assert_eq!(full.len(), 2);

        let oversized = vec![
            upstream_record;
            sinomo_core::network::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS + 1
        ];
        let error = filter_peer_connectivity_response_records(
            &state,
            &responder_endpoint,
            oversized,
            PeerConnectivityFetchScope::EndpointSource,
        )
        .expect_err("aggregate cap must apply before endpoint-source filtering");
        assert!(error.contains("too many cluster connectivity snapshot records"));
    }

    #[test]
    fn cold_start_join_handoff_import_preserves_local_source_ownership() {
        let root = unique_cluster_test_root("cold-start-join-connectivity-handoff");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cold_start_join_handoff").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_cold_start_join_handoff_local").expect("local");
        let introducer_node = NodeId::from_legacy_named("node_cold_start_join_handoff_introducer")
            .expect("introducer");
        let bridge_node =
            NodeId::from_legacy_named("node_cold_start_join_handoff_bridge").expect("bridge");
        let authority_node =
            NodeId::from_legacy_named("node_cold_start_join_handoff_authority").expect("authority");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "cold start join handoff",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let accepted_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &introducer_node,
            "node4:21353".to_string(),
            10,
        );
        let bridge_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&bridge_node, "node3:21353".to_string(), 10);
        let authority_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &authority_node,
            "node1:21353".to_string(),
            10,
        );
        for (node_id, name, endpoint) in [
            (
                introducer_node.clone(),
                "introducer",
                accepted_endpoint.clone(),
            ),
            (bridge_node.clone(), "bridge", bridge_endpoint.clone()),
            (
                authority_node.clone(),
                "authority",
                authority_endpoint.clone(),
            ),
        ] {
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id,
                name,
                format!("pk-{name}"),
                now,
            );
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![endpoint];
            state.members.push(member);
        }

        let local_to_introducer = sinomo_core::network::cluster_connectivity_record(
            &state,
            &accepted_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("accepted response path".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_records(
            &paths,
            &local_node,
            std::slice::from_ref(&local_to_introducer),
        )
        .expect("seed local accepted response path");
        let mut introducer_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &bridge_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("accepted introducer evidence".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        introducer_record.source_node_id = introducer_node.clone();
        let mut upstream_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &authority_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("upstream authority evidence".to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        upstream_record.source_node_id = bridge_node.clone();
        let mut local_source_echo = upstream_record.clone();
        local_source_echo.source_node_id = local_node.clone();
        local_source_echo.last_success_at = Some("peer echo of local source".to_string());

        let expected_introducer = introducer_record.clone();
        let expected_upstream = upstream_record.clone();
        let runtime = daemon_cluster_test_runtime();
        let imported = complete_accepted_join_connectivity_handoff_on_runtime_with_fetch(
            &paths,
            &state,
            &accepted_endpoint,
            runtime.handle(),
            async move { Ok(vec![introducer_record, upstream_record, local_source_echo]) },
        );

        assert_eq!(imported, 2);
        let stored_introducer =
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &introducer_node,
            )
            .expect("read imported introducer connectivity");
        assert_eq!(stored_introducer, vec![expected_introducer]);
        let stored_upstream =
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &bridge_node,
            )
            .expect("read imported upstream connectivity");
        assert_eq!(stored_upstream, vec![expected_upstream]);
        let stored_local =
            sinomo_core::network::read_local_cluster_connectivity_records_for_source(
                &paths,
                &local_node,
            )
            .expect("read locally owned connectivity");
        assert_eq!(
            stored_local,
            vec![local_to_introducer],
            "a peer snapshot must never replace or extend locally owned observations"
        );

        let graph =
            sinomo_core::network::ClusterConnectivityGraph::try_read_for_local(&paths, &state)
                .expect("read composed handoff graph");
        let route =
            sinomo_core::network::cluster_route_next_hop(&graph, &local_node, &authority_node)
                .expect("handoff should compose a route to the authority");
        assert_eq!(route.next_hop, introducer_node);
        assert_eq!(
            route.path,
            vec![local_node, route.next_hop, bridge_node, authority_node]
        );
    }

    #[test]
    fn cold_start_join_handoff_timeout_is_bounded_and_non_fatal() {
        let root = unique_cluster_test_root("cold-start-join-connectivity-handoff-timeout");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cold_start_handoff_timeout").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_cold_start_handoff_timeout_local").expect("local");
        let introducer_node =
            NodeId::from_legacy_named("node_cold_start_handoff_timeout_introducer")
                .expect("introducer");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "cold start handoff timeout",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let accepted_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &introducer_node,
            "node4:21353".to_string(),
            10,
        );
        let runtime = daemon_cluster_test_runtime();
        let started = Instant::now();
        let imported = complete_accepted_join_connectivity_handoff_on_runtime_with_fetch(
            &paths,
            &state,
            &accepted_endpoint,
            runtime.handle(),
            std::future::pending::<Result<Vec<ClusterConnectivityRecord>, String>>(),
        );

        assert_eq!(
            imported, 0,
            "handoff import failure must not fail an already accepted join"
        );
        assert!(
            started.elapsed() < DAEMON_JOIN_CONNECTIVITY_HANDOFF_TIMEOUT + Duration::from_secs(1),
            "a best-effort handoff must remain independently bounded"
        );
    }

    #[test]
    fn peer_connectivity_import_uses_reachable_records_from_full_active_set() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_peer_import_probe_batch").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_peer_import_probe_batch_local").expect("local node");
        let selected_node = NodeId::from_legacy_named("node_peer_import_probe_batch_selected")
            .expect("selected node");
        let skipped_node = NodeId::from_legacy_named("node_peer_import_probe_batch_skipped")
            .expect("skipped node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "peer import probe batch",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let selected_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &selected_node,
            "203.0.113.41:21353".to_string(),
            10,
        );
        let skipped_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &skipped_node,
            "203.0.113.42:21353".to_string(),
            10,
        );
        for (node, name, endpoint) in [
            (selected_node.clone(), "selected", selected_endpoint.clone()),
            (skipped_node.clone(), "skipped", skipped_endpoint.clone()),
        ] {
            let mut member =
                ClusterMemberRecord::local_voter(cluster_id.clone(), node, name, "pk", now);
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![endpoint];
            state.members.push(member);
        }

        let selected_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &selected_endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("ok".to_string()),
            None,
            now,
            60,
        );
        let skipped_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &skipped_endpoint,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("ok".to_string()),
            None,
            now,
            60,
        );
        let import_records = peer_connectivity_import_records_for_reachable_peers(
            &state,
            &[selected_record.clone(), skipped_record.clone()],
            now,
        );

        assert_eq!(
            import_records,
            vec![selected_record, skipped_record],
            "peer connectivity import should use the full active reachable set, not only this tick's probe batch"
        );
    }

    #[test]
    fn peer_connectivity_import_skips_shadow_and_unreachable_records() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_peer_connectivity_import_skip")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_peer_connectivity_import_skip_local")
            .expect("local node");
        let remote_node = NodeId::from_legacy_named("node_peer_connectivity_import_skip_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "peer connectivity import skip",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        state.members.push(remote_member);

        let shadow_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "203.0.113.41:21354".to_string(),
            10,
        );
        let shadow_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &shadow_endpoint,
            ClusterConnectivityState::Healthy,
            Some(12),
            0,
            Some("ok".to_string()),
            None,
            now,
            60,
        );
        assert!(
            peer_connectivity_import_endpoint_for_record(&state, &shadow_record).is_none(),
            "shadow daemon endpoints must not be used for main-daemon peer connectivity import"
        );

        let main_endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "203.0.113.41:21353".to_string(),
            10,
        );
        let unreachable_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &main_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("unreachable".to_string()),
            now,
            60,
        );
        assert!(
            peer_connectivity_import_endpoint_for_record(&state, &unreachable_record).is_none(),
            "unreachable records must not trigger peer connectivity import"
        );
    }

    #[test]
    fn join_cluster_state_authority_targets_do_not_filter_offline_status_during_bootstrap() {
        let root = unique_cluster_test_root("sinomo-daemon-join-targets-ignore-offline-status");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_join_targets_bootstrap")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_join_targets_bootstrap_local")
            .expect("local node");
        let voter_node = NodeId::from_legacy_named("node_daemon_join_targets_bootstrap_voter")
            .expect("voter node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "join targets bootstrap",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let voter_endpoint =
            sinomo_core::network::daemon_lan_endpoint(&voter_node, "node1:21353".to_string(), 10);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node.clone(),
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_core::node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint.clone()];
        state.members.push(voter_member);

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync cluster state");
        seed_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id,
                node_id: voter_node,
                process: sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                availability: ClusterNodeAvailability::Offline,
                daemon_version: "0.1.0-test".to_string(),
                commit: None,
                target_os: Some("test-os".to_string()),
                target_arch: Some("test-arch".to_string()),
                hostname: None,
                pid: Some(42),
                address: Some("node1:21353".to_string()),
                bind_address: Some("0.0.0.0:21353".to_string()),
                executable: Some("sinomo".to_string()),
                rollout: None,
                daemon_started_at: Some("2026-05-04T00:00:00Z".to_string()),
                observed_at: now,
            },
        )
        .expect("sync offline node status");

        assert!(
            sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
                .select(
                    sinomo_core::network::ClusterEndpointSelectionPurpose::AvailableClusterStateAuthority,
                )
                .is_empty(),
            "ordinary live-voter routing should honor explicit offline status"
        );
        assert_eq!(
            sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
                .select(
                    sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateAuthority
                )
                .first()
                .map(|endpoint| &endpoint.endpoint_id),
            Some(&voter_endpoint.endpoint_id),
            "join bootstrap must still try voter command targets even when local health is stale/offline"
        );
    }

    #[test]
    fn routed_ping_status_prefers_peer_local_health_row() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_routed_ping_health").expect("cluster id");
        let remote_node = NodeId::from_legacy_named("node_daemon_routed_ping_health_remote")
            .expect("remote node");
        let other_node =
            NodeId::from_legacy_named("node_daemon_routed_ping_health_other").expect("other node");
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();

        let fresh = test_cluster_node_status(
            &cluster_id,
            &remote_node,
            sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE,
            Some("fresh-commit"),
            now,
        );
        let selected = member_status_from_peer_local_health_rows(
            &member,
            vec![
                ClusterHealthRow {
                    member: ClusterMemberRecord::local_voter(
                        cluster_id.clone(),
                        other_node.clone(),
                        "other",
                        "pk-other",
                        now,
                    ),
                    status: test_cluster_node_status(
                        &cluster_id,
                        &other_node,
                        sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE,
                        Some("other-commit"),
                        now,
                    ),
                    availability: ClusterNodeAvailability::Online,
                    live_probe_observed: true,
                },
                ClusterHealthRow {
                    member: member.clone(),
                    status: fresh.clone(),
                    availability: ClusterNodeAvailability::Online,
                    live_probe_observed: true,
                },
            ],
        )
        .expect("fresh peer row");

        assert_eq!(selected.commit.as_deref(), Some("fresh-commit"));
        assert_eq!(selected.node_id, remote_node);
        assert_eq!(selected.availability, ClusterNodeAvailability::Online);
    }

    #[test]
    fn raft_command_targets_try_each_voter_before_same_voter_fallbacks() {
        let root = unique_cluster_test_root("sinomo-daemon-raft-interleaved-voters");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_raft_interleave").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_raft_interleave_local").expect("local node");
        let first_node =
            NodeId::from_legacy_named("node_daemon_raft_interleave_first").expect("first node");
        let second_node =
            NodeId::from_legacy_named("node_daemon_raft_interleave_second").expect("second node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "raft interleaved voters",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let first_tcp = sinomo_core::network::daemon_lan_endpoint(
            &first_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let first_public_tcp = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_raft_interleave_first_public",
            )
            .expect("endpoint"),
            node_id: first_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.41:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let second_tcp = sinomo_core::network::daemon_lan_endpoint(
            &second_node,
            "10.0.0.42:21353".to_string(),
            10,
        );
        for (node_id, name, public_key, endpoints) in [
            (
                first_node.clone(),
                "first",
                "pk-first",
                vec![first_tcp.clone(), first_public_tcp.clone()],
            ),
            (
                second_node.clone(),
                "second",
                "pk-second",
                vec![second_tcp.clone()],
            ),
        ] {
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id,
                name,
                public_key,
                now,
            );
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = endpoints;
            state.members.push(member);
        }

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft kv");

        let targets = sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_core::network::ClusterEndpointSelectionPurpose::ClusterStateCommand);
        let first_public_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == first_public_tcp.endpoint_id)
            .expect("first public tcp");
        let second_tcp_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == second_tcp.endpoint_id)
            .expect("second tcp");

        assert!(
            second_tcp_position < first_public_position,
            "raft/KV forwarding should try another voter's best endpoint before fallbacks for one voter"
        );
    }

    #[test]
    fn unreachable_member_blocks_leader_preference_without_hiding_voter() {
        let root = unique_cluster_test_root("sinomo-daemon-unreachable-leader-status");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_unreachable_leader").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_unreachable_leader_local").expect("local node");
        let remote_node = NodeId::from_legacy_named("node_daemon_unreachable_leader_remote")
            .expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "unreachable leader status",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = sinomo_core::network::daemon_lan_endpoint(
            &remote_node,
            "10.0.0.41:21353".to_string(),
            10,
        );
        let remote_endpoint_id = endpoint.endpoint_id.clone();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        state.members.push(remote_member);

        sinomo_core::cluster::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state to raft kv");
        // The remote still self-reports Online (it never published an explicit
        // Offline). Liveness now follows the active probes, not the self-reported
        // observed_at, so a fresh-looking status must not earn leader preference
        // once the probes can no longer reach the node.
        seed_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id: cluster_id.clone(),
                node_id: remote_node.clone(),
                process: sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                availability: ClusterNodeAvailability::Online,
                daemon_version: "0.1.0-test".to_string(),
                commit: None,
                target_os: Some("test-os".to_string()),
                target_arch: Some("test-arch".to_string()),
                hostname: None,
                pid: Some(42),
                address: Some("10.0.0.41:21353".to_string()),
                bind_address: Some("0.0.0.0:21353".to_string()),
                executable: Some("sinomo".to_string()),
                rollout: None,
                daemon_started_at: Some("2026-05-04T00:00:00Z".to_string()),
                observed_at: now,
            },
        )
        .expect("sync online node status");
        // The local node's active connectivity probes can no longer reach the
        // remote member's only endpoint.
        sinomo_core::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![ClusterConnectivityRecord {
                cluster_id,
                source_node_id: local_node.clone(),
                target_node_id: remote_node.clone(),
                endpoint_id: remote_endpoint_id,
                transport: ClusterTransportKind::LanTcp,
                address: "10.0.0.41:21353".to_string(),
                state: sinomo_core::network::ClusterConnectivityState::Unreachable,
                latency_ms: None,
                consecutive_failures: 5,
                last_success_at: None,
                last_failure_at: Some("2026-05-04T00:10:00Z".to_string()),
                failure_reason: None,
                observed_at: now,
                expires_at: now + time::Duration::seconds(300),
            }],
        )
        .expect("sync unreachable connectivity");

        assert!(
            sinomo_core::cluster::read_cluster_node_statuses_from_kv(&paths)
                .expect("node statuses")
                .into_iter()
                .find(|status| status.node_id == remote_node)
                .is_some_and(|status| status.availability != ClusterNodeAvailability::Offline),
            "an unreachable member that never self-declared Offline is not explicit offline"
        );
        assert_eq!(
            sinomo_core::network::ClusterEndpointSelector::new(&paths, &state)
                .select(
                    sinomo_core::network::ClusterEndpointSelectionPurpose::AvailableClusterStateAuthority,
                )
                .len(),
            1,
            "an unreachable member must remain a voter while connectivity is recovering"
        );
    }

    #[test]
    fn raft_command_timeout_caps_routed_node_attempt() {
        assert_eq!(
            cluster_state_authority_reachability_options().routed_node_command_timeout(
                sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT
        );
    }

    #[test]
    fn routed_cluster_control_rpc_preserves_endpoint_fallback_budget() {
        let timeout = Duration::from_secs(30);
        let health = sinomo_core::network::NetworkHealthPolicy::new(
            sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
            sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT,
        );

        assert_eq!(
            daemon_cluster_control_rpc_options(timeout),
            sinomo_core::cluster::main_daemon_cluster_rpc_options(
                sinomo_core::network::ClusterObjectRpcOptions::new(health.into())
                    .with_overall_timeout(timeout)
            )
        );
    }

    #[test]
    fn raft_append_routing_excludes_a_faster_folded_shadow_endpoint() {
        let node_id = NodeId::from_legacy_named("node_raft_affinity_target").expect("node id");
        let main = sinomo_core::network::daemon_lan_endpoint(
            &node_id,
            "198.51.100.90:21353".to_string(),
            0,
        );
        let shadow_node_id = sinomo_core::node::shadow_daemon_node_id(&node_id);
        let mut folded_shadow = sinomo_core::network::daemon_lan_endpoint(
            &shadow_node_id,
            "198.51.100.90:21354".to_string(),
            0,
        );
        folded_shadow.node_id = node_id;

        assert!(sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(&main));
        assert!(
            !sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(&folded_shadow),
            "a shadow endpoint folded under the voter's node identity must not terminate Raft append"
        );
        assert_eq!(
            daemon_main_routed_rpc_options(Duration::from_secs(5)).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon,
            "Raft append/vote must not let lower shadow latency choose a process without consensus routes"
        );
    }

    #[test]
    fn foreground_raft_attempts_preserve_budget_for_node_route_recovery() {
        let timeout_options = cluster_state_authority_reachability_options();
        assert_eq!(
            timeout_options.routed_node_attempt_timeout(
                sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            timeout_options.routed_node_attempt_timeout(Duration::from_millis(50)),
            Duration::from_millis(50)
        );
    }
}
