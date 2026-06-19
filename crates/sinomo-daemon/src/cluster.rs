use crate::dfs::ensure_default_distributed_dfs_async_on_runtime;
#[cfg(test)]
use crate::status::ensure_daemon_cluster_members;
#[cfg(test)]
use crate::{AuthenticatedDaemonCommand, DaemonClusterRegistration};
use crate::{
    DaemonClusterStatus, DaemonError, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    DaemonSignedEndpointRpcAsyncHost, IdleStatsAtomics, SINOMO_DAEMON_DEFAULT_HOST,
    SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    cluster_node_status_from_daemon_status, cluster_paths_from_env, current_daemon_cluster_options,
    current_shadow_daemon_options_on_runtime, daemon_cluster_options_from_status,
    daemon_cluster_process_from_paths, daemon_cluster_status, daemon_cluster_status_matches,
    daemon_connect_address, daemon_options_from_status, daemon_paths_are_shadow,
    fetch_cluster_kv_snapshot_from_endpoint_on_runtime, fetch_daemon_status_from_endpoint_async,
    fetch_daemon_status_from_endpoint_on_runtime, handle_daemon_rollout_build_request,
    read_daemon_status, send_cluster_kv_watch_events_async, send_cluster_member_publish_async,
    send_cluster_raft_append_async, send_cluster_raft_append_on_runtime,
    send_cluster_raft_vote_async, send_cluster_raft_vote_on_runtime,
    send_daemon_rpc_to_endpoint_async, send_daemon_rpc_to_endpoint_on_runtime, write_daemon_status,
};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeRole,
    ClusterNodeRolloutStatus, ClusterNodeStatusRecord, ClusterTransportKind,
};
#[cfg(test)]
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterJoinInvite, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, TrustDomain,
};
use sinomo_kv::{
    ClusterKvRaftAppendReplicationOutcome, ClusterKvWatchEventsRequest,
    ClusterKvWatchEventsResponse, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse,
};
use sinomo_kv::{InProcessKvStore, KvEvent, KvSnapshot, KvTxn, KvTxnResponse};
#[cfg(test)]
use sinomo_kv::{KvCommand, KvOp};
#[cfg(test)]
use sinomo_kv::{KvCompare, KvOpResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::ClusterLocalDaemonMembershipAsyncObserver;
use sinomo_network::{
    AsyncMdnsDiscoverySocket, ClusterConnectivityRecord, ClusterConnectivityState,
    ClusterHealthRow, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse, ClusterNetworkInfoRecord,
    ClusterPaths, ClusterStatusAsyncProbe, IrohEndpointRecord, MdnsDiscoveryCache,
    ObjectRpcRequest, ObjectRpcResponse,
};
#[cfg(test)]
use sinomo_network::{
    ClusterConnectivityProbeState, ClusterJoinRegistrationResponse, MdnsDiscoveryAdvertisement,
};
#[cfg(test)]
use sinomo_node::{ClusterId, ClusterInviteId, cluster_member_key};
use sinomo_node::{
    NodeId, cluster_member_expected_daemon_process, cluster_member_is_local_process,
    shadow_daemon_node_id,
};
use sinomo_node::{load_or_create_main_node_identity, load_or_create_shadow_node_identity};
use sinomo_rollout::{
    ClusterRolloutBuildRequest, SelfRolloutPaths, managed_source_status, read_rollout_status,
    source_heads_match,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT: Duration = Duration::from_millis(1500);
pub const DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
pub const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(test)]
const DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL;
pub const DAEMON_CLUSTER_PUBLISH_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_PUBLISH_INTERVAL;
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF: Duration = Duration::from_secs(600);
/// First-failure publish backoff; doubles per consecutive failure up to
/// `DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF`. Cold cluster formation depends on
/// quick retries: deep-segment nodes race the bridge's first connectivity
/// publication, and a flat long backoff freezes the loser past the formation
/// window (relay routes need connectivity records, which need relayed
/// publishes — only fast retry breaks the cycle once the bridge publishes).
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_secs(15);
/// Learner KV snapshot pull cadence. Convergence is event-driven — the voter
/// pushes committed raft appends to every member on commit — so this pull is
/// only a recovery net for lost pushes and restarts, deliberately slow to keep
/// an idle cluster quiet.
pub const DAEMON_CLUSTER_KV_SNAPSHOT_PULL_INTERVAL: Duration = Duration::from_secs(600);
pub const DAEMON_CLUSTER_DISCOVERY_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_DISCOVERY_INTERVAL;
const DAEMON_CLUSTER_STARTUP_SETTLE_DURATION: Duration = Duration::from_secs(8);
const DAEMON_SHADOW_STATUS_STARTUP_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const DAEMON_SHADOW_STATUS_STARTUP_RETRY_ATTEMPTS: u8 = 20;
pub const DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_PROBE_INTERVAL;
// Network-info records describe stable endpoint/environment data, not liveness.
// Active health and connectivity probes handle reachability, so unchanged records
// can refresh sparsely to avoid idle WAL/snapshot churn on low-write storage.
pub const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 = sinomo_network::CLUSTER_NETWORK_INFO_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = sinomo_network::CLUSTER_CONNECTIVITY_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 =
    sinomo_network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 =
    sinomo_network::CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS;
const CLUSTER_HEALTH_PEER_ROWS_TIMEOUT: Duration = Duration::from_secs(8);
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 =
    sinomo_network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD;
const PUBLISHED_NODE_CACHE_MAX_ENTRIES: usize = sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_SOURCES: usize =
    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS_PER_SOURCE: usize =
    sinomo_network::MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS: usize =
    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;

fn daemon_cluster_kv_raft_store(paths: &ClusterPaths) -> sinomo_kv::ClusterKvRaftStore {
    paths.kv_paths().raft_store()
}

#[cfg(test)]
fn daemon_cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, sinomo_network::NetworkError> {
    Ok(daemon_cluster_kv_raft_store(paths)
        .known_leader_id(&state.local_node_id)?
        .filter(|leader_id| sinomo_kv::cluster_kv_raft_node_is_voter(state, leader_id)))
}

#[cfg(test)]
fn daemon_locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, sinomo_network::NetworkError> {
    daemon_cluster_kv_raft_store(paths)
        .locked_set_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

#[cfg(test)]
fn daemon_clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, sinomo_network::NetworkError> {
    daemon_cluster_kv_raft_store(paths)
        .clear_leader_and_write_snapshot_if_changed(node_id)
        .map_err(Into::into)
}
const CLUSTER_CONNECTIVITY_PROBE_CONCURRENCY: usize = 16;

struct ClusterConnectivityProbePlan {
    active_keys: BTreeSet<String>,
    entries: Vec<sinomo_network::ClusterConnectivityProbeBatchEntry>,
    timestamp: String,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
}

pub(crate) struct ClusterConnectivityPendingProbe {
    paths: ClusterPaths,
    source_node_id: NodeId,
    state: ClusterBootstrapState,
    plan: ClusterConnectivityProbePlan,
}

#[derive(Default)]
pub(crate) struct ClusterNetworkMaintenancePendingWork {
    connectivity_probe: Option<ClusterConnectivityPendingProbe>,
    member_publishes: Vec<ClusterMemberPublishPendingWork>,
    node_status_refreshes: Vec<ClusterNodeStatusPendingRefresh>,
}

impl ClusterNetworkMaintenancePendingWork {
    fn is_empty(&self) -> bool {
        self.connectivity_probe.is_none()
            && self.member_publishes.is_empty()
            && self.node_status_refreshes.is_empty()
    }
}

struct ClusterMemberPublishPendingWork {
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    force_kv_snapshot_sync: bool,
}

struct ClusterNodeStatusPendingRefresh {
    paths: DaemonPaths,
    label: &'static str,
}

static PUBLISHED_NETWORK_INFO: OnceLock<Mutex<BTreeMap<String, ClusterNetworkInfoRecord>>> =
    OnceLock::new();
static PUBLISHED_NODE_STATUS: OnceLock<Mutex<BTreeMap<String, ClusterNodeStatusRecord>>> =
    OnceLock::new();
static PUBLISHED_CONNECTIVITY: OnceLock<Mutex<BTreeMap<String, PublishedConnectivitySourceCache>>> =
    OnceLock::new();

thread_local! {
    static COMMAND_CONNECTIVITY_SYNC_IN_FLIGHT: std::cell::Cell<bool> =
        std::cell::Cell::new(false);
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

fn block_on_cluster_network_future_on_owner_runtime<F, T>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> Result<T, sinomo_network::NetworkError>
where
    F: std::future::Future<Output = Result<T, sinomo_network::NetworkError>> + Send,
    T: Send,
{
    if !owner_runtime_handle_can_block(runtime) {
        return Err(sinomo_network::NetworkError::Io {
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
            Err(sinomo_network::NetworkError::Io {
                operation: "daemon cluster runtime".to_string(),
                detail: "daemon cluster owner-runtime bridge cannot run inside a current-thread Tokio runtime; use the async cluster API".to_string(),
            })
        };
    }
    runtime.block_on(future)
}

fn block_on_daemon_cluster_future_on_owner_runtime<F, T>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
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

#[derive(Clone, Debug, Default)]
struct PublishedConnectivitySourceCache {
    records: BTreeMap<String, ClusterConnectivityRecord>,
}

fn publish_cache_key(paths: &ClusterPaths, node_id: &NodeId) -> String {
    format!("{}:{node_id}", paths.root.display())
}

fn insert_bounded_published_cache<T>(
    cache: &mut BTreeMap<String, T>,
    key: String,
    value: T,
    max_entries: usize,
) {
    if !cache.contains_key(&key) {
        while cache.len() >= max_entries {
            let Some(oldest) = cache.keys().next().cloned() else {
                break;
            };
            cache.remove(&oldest);
        }
    }
    cache.insert(key, value);
}

fn published_connectivity_cache_record_count(
    cache: &BTreeMap<String, PublishedConnectivitySourceCache>,
) -> usize {
    cache.values().map(|source| source.records.len()).sum()
}

fn ensure_published_connectivity_source(
    cache: &mut BTreeMap<String, PublishedConnectivitySourceCache>,
    source_prefix: String,
) {
    if cache.contains_key(&source_prefix) {
        return;
    }
    while cache.len() >= PUBLISHED_CONNECTIVITY_CACHE_MAX_SOURCES {
        let Some(oldest) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&oldest);
    }
    cache.insert(source_prefix, PublishedConnectivitySourceCache::default());
}

fn trim_published_connectivity_cache_records(
    cache: &mut BTreeMap<String, PublishedConnectivitySourceCache>,
    protected_source_prefix: &str,
) {
    while published_connectivity_cache_record_count(cache)
        > PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS
    {
        let evict_source = cache
            .iter()
            .find(|(source_prefix, source)| {
                source_prefix.as_str() != protected_source_prefix && !source.records.is_empty()
            })
            .map(|(source_prefix, _)| source_prefix.clone())
            .or_else(|| {
                cache
                    .keys()
                    .find(|source_prefix| source_prefix.as_str() != protected_source_prefix)
                    .cloned()
            });
        if let Some(source_prefix) = evict_source {
            cache.remove(&source_prefix);
            continue;
        }

        let Some(source) = cache.get_mut(protected_source_prefix) else {
            break;
        };
        while source.records.len() > PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS {
            let Some(key) = source.records.keys().next().cloned() else {
                break;
            };
            source.records.remove(&key);
        }
        break;
    }
}

fn cached_network_info_matches(paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) -> bool {
    let key = publish_cache_key(paths, &info.node_id);
    let now = OffsetDateTime::now_utc();
    PUBLISHED_NETWORK_INFO
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|cache| {
            cache.get(&key).is_some_and(|known| {
                sinomo_network::cluster_network_info_publish_record_matches(known, info, now)
            })
        })
}

fn remember_network_info(paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) {
    let key = publish_cache_key(paths, &info.node_id);
    if let Ok(mut cache) = PUBLISHED_NETWORK_INFO
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        insert_bounded_published_cache(
            &mut cache,
            key,
            info.clone(),
            PUBLISHED_NODE_CACHE_MAX_ENTRIES,
        );
    }
}

fn cached_node_status_matches(paths: &ClusterPaths, status: &ClusterNodeStatusRecord) -> bool {
    let key = publish_cache_key(paths, &status.node_id);
    // Node status is published only when its stable content changes — it is NOT
    // periodically refreshed. Liveness/crash detection comes from the active
    // connectivity probes (in-memory, persisted only on state change), so the
    // observed_at timestamp no longer needs a heartbeat. This keeps an idle cluster
    // from rewriting the KV snapshot every ~60s purely to bump a timestamp.
    PUBLISHED_NODE_STATUS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|cache| {
            cache.get(&key).is_some_and(|known| {
                sinomo_network::cluster_node_status_publish_record_matches(known, status)
            })
        })
}

fn remember_node_status(paths: &ClusterPaths, status: &ClusterNodeStatusRecord) {
    let key = publish_cache_key(paths, &status.node_id);
    if let Ok(mut cache) = PUBLISHED_NODE_STATUS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        insert_bounded_published_cache(
            &mut cache,
            key,
            status.clone(),
            PUBLISHED_NODE_CACHE_MAX_ENTRIES,
        );
    }
}

fn connectivity_cache_key(paths: &ClusterPaths, record: &ClusterConnectivityRecord) -> String {
    format!(
        "{}:{}",
        paths.root.display(),
        sinomo_network::cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        )
    )
}

fn cached_connectivity_matches(paths: &ClusterPaths, record: &ClusterConnectivityRecord) -> bool {
    let key = connectivity_cache_key(paths, record);
    let source_prefix = connectivity_cache_source_prefix(paths, &record.source_node_id);
    let now = OffsetDateTime::now_utc();
    PUBLISHED_CONNECTIVITY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|cache| {
            cache
                .get(&source_prefix)
                .and_then(|source| source.records.get(&key))
                .is_some_and(|known| {
                    sinomo_network::cluster_connectivity_publish_record_matches(known, record, now)
                })
        })
}

fn connectivity_cache_source_prefix(paths: &ClusterPaths, source_node_id: &NodeId) -> String {
    format!(
        "{}:{}",
        paths.root.display(),
        sinomo_network::cluster_connectivity_source_prefix(source_node_id)
    )
}

fn cached_connectivity_record_set_matches(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> bool {
    let now = OffsetDateTime::now_utc();
    let incoming_keys = records
        .iter()
        .map(|record| connectivity_cache_key(paths, record))
        .collect::<BTreeSet<_>>();
    let source_prefix = connectivity_cache_source_prefix(paths, source_node_id);
    PUBLISHED_CONNECTIVITY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|cache| {
            let Some(source) = cache.get(&source_prefix) else {
                return false;
            };
            if source
                .records
                .keys()
                .any(|key| !incoming_keys.contains(key))
            {
                return false;
            }
            records.iter().all(|record| {
                let key = connectivity_cache_key(paths, record);
                source.records.get(&key).is_some_and(|known| {
                    sinomo_network::cluster_connectivity_publish_record_matches(known, record, now)
                })
            })
        })
}

fn rollout_status_matches_desired(
    status: Option<&ClusterNodeRolloutStatus>,
    mode: Option<&str>,
    requested_at: Option<&str>,
) -> bool {
    let Some(status) = status else {
        return false;
    };
    status.mode.as_deref() == mode
        && status.requested_at.as_deref() == requested_at
        && matches!(
            status.status.as_str(),
            "accepted" | "main-staged" | "healthy" | "recovered"
        )
}

fn remember_connectivity_record_set(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) {
    if let Ok(mut cache) = PUBLISHED_CONNECTIVITY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let source_prefix = connectivity_cache_source_prefix(paths, source_node_id);
        ensure_published_connectivity_source(&mut cache, source_prefix.clone());
        {
            let source = cache
                .get_mut(&source_prefix)
                .expect("published connectivity source was inserted");
            source.records.clear();
            if records.len() > PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS_PER_SOURCE {
                return;
            }
            for record in records {
                source
                    .records
                    .insert(connectivity_cache_key(paths, record), record.clone());
            }
        }
        trim_published_connectivity_cache_records(&mut cache, &source_prefix);
    }
}

fn publish_command_connectivity_if_needed(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    record: &ClusterConnectivityRecord,
    runtime: Option<&tokio::runtime::Handle>,
) {
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        || cached_connectivity_matches(paths, record)
    {
        return;
    }
    COMMAND_CONNECTIVITY_SYNC_IN_FLIGHT.with(|in_flight| {
        if in_flight.get() {
            return;
        }
        in_flight.set(true);
        let records = sinomo_network::read_local_cluster_connectivity_records(paths)
            .unwrap_or_default()
            .into_iter()
            .filter(|record| record.source_node_id == state.local_node_id)
            .collect::<Vec<_>>();
        match runtime {
            Some(runtime) => {
                let _ = sync_cluster_connectivity_records_to_kv_on_runtime(
                    paths,
                    &state.local_node_id,
                    records,
                    runtime,
                );
            }
            None => {
                #[cfg(test)]
                {
                    let _ = sync_cluster_connectivity_records_to_kv(
                        paths,
                        &state.local_node_id,
                        records,
                    );
                }
            }
        }
        in_flight.set(false);
    });
}

pub(crate) fn remember_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
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

pub(crate) fn remember_cluster_endpoint_command_connectivity_without_failure_grace(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
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
    connectivity_state: sinomo_network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
    runtime: Option<&tokio::runtime::Handle>,
) {
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let previous = sinomo_network::read_local_cluster_connectivity_records(paths)
        .unwrap_or_default()
        .into_iter()
        .find(|record| {
            record.source_node_id == state.local_node_id
                && record.target_node_id == endpoint.node_id
                && record.endpoint_id == endpoint.endpoint_id
        });
    let is_healthy = matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Healthy
    );
    let observed_latency_ms = latency_ms.unwrap_or_else(|| {
        previous
            .as_ref()
            .and_then(|record| record.latency_ms)
            .unwrap_or(0)
    });
    let record = if matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Unreachable
    ) {
        sinomo_network::cluster_connectivity_record_for_identity_mismatch(
            state, endpoint, &timestamp, now,
        )
    } else if !is_healthy && !allow_failure_grace {
        sinomo_network::cluster_connectivity_record_for_hard_failure(
            state,
            endpoint,
            previous.as_ref(),
            &timestamp,
            now,
        )
    } else {
        sinomo_network::cluster_connectivity_record_for_probe_outcome(
            state,
            endpoint,
            previous.as_ref(),
            is_healthy,
            observed_latency_ms,
            &timestamp,
            now,
        )
    };
    if sinomo_network::write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )
    .is_ok()
    {
        publish_command_connectivity_if_needed(paths, state, &record, runtime);
    }
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

pub(crate) fn start_daemon_iroh_proxy_if_clustered(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    cluster: Option<&DaemonClusterStatus>,
    runtime: tokio::runtime::Handle,
    label: &str,
) -> Result<(), DaemonError> {
    if sinomo_network::iroh_disabled_by_env() {
        println!("sinomo {label} iroh disabled (SINOMO_DISABLE_IROH)");
        return Ok(());
    }
    let paths = paths.clone();
    let options = options.clone();
    let mut cluster = cluster.cloned();
    let label = label.to_string();
    let runtime = runtime.clone();
    runtime.clone().spawn(async move {
        let Some(data_dir) = paths.root.parent().map(|path| path.to_path_buf()) else {
            eprintln!(
                "sinomo {label} iroh proxy disabled: daemon paths have no data directory parent"
            );
            return;
        };
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        let daemon_address = daemon_connect_address(&options.host, options.port);
        let mut proxy_handle: Option<sinomo_network::IrohDaemonProxyHandle> = None;
        let mut next_republish = Instant::now();
        let mut proxy_start_failures = 0_u32;
        loop {
            if proxy_handle
                .as_ref()
                .is_some_and(sinomo_network::IrohDaemonProxyHandle::is_finished)
            {
                eprintln!("sinomo {label} iroh proxy supervisor stopped, restarting");
                proxy_handle = None;
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
                    eprintln!("sinomo {label} iroh cluster status task failed");
                    tokio::time::sleep(DAEMON_CLUSTER_PUBLISH_INTERVAL).await;
                    continue;
                };
                cluster = status;
                if cluster.is_none() {
                    let retry_delay = if state_exists {
                        Duration::from_secs(5)
                    } else {
                        DAEMON_CLUSTER_PUBLISH_INTERVAL
                    };
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
            }
            let Some(current_cluster) = cluster.clone() else {
                tokio::time::sleep(DAEMON_CLUSTER_PUBLISH_INTERVAL).await;
                continue;
            };
            if proxy_handle.is_none() {
                match sinomo_network::start_iroh_daemon_proxy(
                    runtime.clone(),
                    cluster_paths.clone(),
                    current_cluster.cluster_id.clone(),
                    current_cluster.node_id.clone(),
                    daemon_address.clone(),
                )
                .await
                {
                    Ok(handle) => {
                        let record = handle.endpoint_record().clone();
                        let endpoint = record.endpoint();
                        let endpoint_address = endpoint.address.clone();
                        let advertise_paths = paths.clone();
                        let advertise_runtime = runtime.clone();
                        match tokio::task::spawn_blocking(move || {
                            advertise_daemon_iroh_endpoint_on_runtime(
                                &advertise_paths,
                                record,
                                &advertise_runtime,
                            )
                        })
                        .await
                        {
                            Ok(Ok(())) => {
                                println!("sinomo {label} iroh endpoint: {endpoint_address}");
                            }
                            Ok(Err(error)) => {
                                eprintln!(
                                    "sinomo {label} iroh endpoint advertisement failed: {error}"
                                );
                            }
                            Err(error) => {
                                eprintln!(
                                    "sinomo {label} iroh endpoint advertisement task failed: {error}"
                                );
                            }
                        }
                        proxy_handle = Some(handle);
                        proxy_start_failures = 0;
                        next_republish = Instant::now()
                            + sinomo_network::cluster_network_publish_interval_for_node(
                                &cluster_paths,
                                &current_cluster.node_id,
                            );
                    }
                    Err(error) if error.to_string().contains("without iroh-transport") => {
                        eprintln!("sinomo {label} iroh proxy disabled: {error}");
                        break;
                    }
                    Err(error) => {
                        eprintln!("sinomo {label} iroh proxy unavailable, retrying: {error}");
                        let retry_delay =
                            sinomo_network::cluster_iroh_proxy_start_retry_sleep_duration(
                                proxy_start_failures,
                                sinomo_network::cluster_network_publish_interval_for_node(
                                    &cluster_paths,
                                    &current_cluster.node_id,
                                ),
                            );
                        proxy_start_failures = proxy_start_failures.saturating_add(1);
                        tokio::time::sleep(retry_delay).await;
                    }
                }
                continue;
            }
            if Instant::now() >= next_republish {
                let repair_paths = paths.clone();
                let repair_runtime = runtime.clone();
                let repair = tokio::task::spawn_blocking(move || {
                    let refreshed =
                        refresh_daemon_iroh_repair_route_on_runtime(&repair_paths, &repair_runtime);
                    let status =
                        daemon_cluster_status_from_paths_on_runtime(&repair_paths, Some(&repair_runtime))
                            .ok()
                            .flatten();
                    (refreshed, status)
                })
                .await;
                match repair {
                    Ok((Ok(_), Some(updated))) => {
                        cluster = Some(updated);
                    }
                    Ok((Ok(_), None)) => {}
                    Ok((Err(error), _)) => {
                        eprintln!("sinomo {label} iroh repair route publish failed: {error}");
                    }
                    Err(error) => {
                        eprintln!("sinomo {label} iroh repair route task failed: {error}");
                    }
                }
                let publish_node = cluster
                    .as_ref()
                    .map(|cluster| &cluster.node_id)
                    .unwrap_or(&current_cluster.node_id);
                next_republish = Instant::now()
                    + sinomo_network::cluster_network_publish_interval_for_node(
                        &cluster_paths,
                        publish_node,
                    );
            }
            tokio::time::sleep(sinomo_network::cluster_iroh_proxy_republish_sleep_duration(
                Instant::now(),
                next_republish,
            ))
            .await;
        }
    });
    Ok(())
}

pub(crate) fn daemon_cluster_status_from_paths_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Option<DaemonClusterStatus>, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster_on_runtime(paths, &mut status, runtime)?;
    Ok(status.cluster)
}

pub(crate) fn refresh_daemon_iroh_repair_route_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    refresh_daemon_iroh_repair_route_with_runtime(paths, Some(runtime))
}

#[cfg(test)]
fn refresh_daemon_iroh_repair_route(paths: &DaemonPaths) -> Result<bool, DaemonError> {
    refresh_daemon_iroh_repair_route_with_runtime(paths, None)
}

fn refresh_daemon_iroh_repair_route_with_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<bool, DaemonError> {
    let Some(cluster) = daemon_cluster_status_from_paths_on_runtime(paths, runtime)? else {
        return Ok(false);
    };
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    match sinomo_network::read_iroh_endpoint_record(&cluster_paths, &cluster.node_id) {
        Ok(record) => {
            advertise_daemon_iroh_endpoint_with_runtime(paths, record, runtime)?;
            Ok(true)
        }
        Err(error) => {
            eprintln!(
                "sinomo {} iroh repair route unavailable: {error}",
                cluster.process
            );
            Ok(false)
        }
    }
}

pub(crate) fn start_shadow_daemon_status_publisher(
    paths: &DaemonPaths,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let status_paths = paths.clone();
    runtime.clone().spawn(async move {
        let iroh_disabled = sinomo_network::iroh_disabled_by_env();
        let mut startup_retries_remaining = DAEMON_SHADOW_STATUS_STARTUP_RETRY_ATTEMPTS;
        let mut publish_interval = DAEMON_SHADOW_STATUS_STARTUP_RETRY_INTERVAL;
        loop {
            tokio::time::sleep(publish_interval).await;
            let tick_paths = status_paths.clone();
            let tick_runtime = runtime.clone();
            let tick = tokio::task::spawn_blocking(move || {
                let settled_solo =
                    daemon_paths_have_settled_solo_cluster(&tick_paths).unwrap_or(false);
                if !settled_solo {
                    if !iroh_disabled {
                        if let Err(error) =
                            refresh_daemon_iroh_repair_route_on_runtime(&tick_paths, &tick_runtime)
                        {
                            eprintln!(
                                "sinomo shadow daemon iroh repair route tick failed: {error}"
                            );
                        }
                    }
                }
                let clustered =
                    daemon_cluster_status_from_paths_on_runtime(&tick_paths, Some(&tick_runtime))
                        .ok()
                        .flatten()
                        .is_some();
                let steady_publish_interval =
                    daemon_cluster_publish_interval_on_runtime(&tick_paths, Some(&tick_runtime));
                (settled_solo, clustered, steady_publish_interval)
            })
            .await;
            let Ok((settled_solo, clustered, steady_publish_interval)) = tick else {
                eprintln!("sinomo shadow daemon cluster status tick task failed");
                publish_interval = DAEMON_SHADOW_STATUS_STARTUP_RETRY_INTERVAL;
                continue;
            };
            let mut status_publish_outcome = None;
            if !settled_solo {
                match refresh_daemon_node_status_in_kv_async(&status_paths, runtime.clone()).await {
                    Ok(outcome) => status_publish_outcome = Some(outcome),
                    Err(error) => {
                        eprintln!("sinomo shadow daemon cluster status tick failed: {error}")
                    }
                }
            }
            if startup_retries_remaining > 0 {
                startup_retries_remaining = startup_retries_remaining.saturating_sub(1);
                let publish_noop = status_publish_outcome.is_some_and(|outcome| {
                    !outcome.network_info_published && !outcome.node_status_published
                });
                if clustered && publish_noop {
                    startup_retries_remaining = 0;
                }
            }
            publish_interval = if startup_retries_remaining > 0 {
                DAEMON_SHADOW_STATUS_STARTUP_RETRY_INTERVAL
            } else {
                steady_publish_interval
            };
        }
    });
    Ok(())
}

fn daemon_paths_have_settled_solo_cluster(paths: &DaemonPaths) -> Result<bool, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if !cluster_paths.state.exists() {
        return Ok(false);
    }
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    Ok(sinomo_network::cluster_is_settled_solo(&state))
}

pub(crate) fn advertise_daemon_iroh_endpoint_on_runtime(
    paths: &DaemonPaths,
    record: IrohEndpointRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    advertise_daemon_iroh_endpoint_with_runtime(paths, record, Some(runtime))
}

fn advertise_daemon_iroh_endpoint_with_runtime(
    paths: &DaemonPaths,
    record: IrohEndpointRecord,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let persisted_endpoints = state
        .members
        .iter()
        .find(|member| member.node_id == record.node_id)
        .map(|member| member.endpoints.clone());
    prepare_local_cluster_state(&cluster_paths, &mut state)?;
    // Persist/replicate only when this member's endpoint set actually differs
    // from disk. prepare_local_cluster_state already folds iroh endpoints in
    // from disk, so we compare against the persisted endpoints rather than the
    // upsert delta: otherwise a brand-new endpoint that prepare merged would
    // never be written, while an idle re-advertise still short-circuits here
    // without touching disk, the KV mirror, or the raft log.
    if sinomo_network::apply_cluster_iroh_endpoint_advertisement(
        &mut state,
        record,
        persisted_endpoints.as_deref(),
        OffsetDateTime::now_utc(),
    )
    .is_some()
    {
        sinomo_network::write_cluster_state(&cluster_paths, &state)?;
        if let Some(runtime) = runtime {
            if let Err(error) = sync_cluster_state_to_kv_on_runtime(&cluster_paths, &state, runtime)
            {
                eprintln!("sinomo iroh endpoint cluster state publication failed: {error}");
            }
            let publish_result = publish_local_members_to_quorum_if_needed_on_runtime(
                &cluster_paths,
                &state,
                runtime,
            );
            if let Err(error) = publish_result {
                eprintln!("sinomo iroh endpoint member publication failed: {error}");
            }
        }
    }
    Ok(())
}

pub(crate) fn handle_daemon_cluster_kv_snapshot(
    paths: &DaemonPaths,
) -> Result<KvSnapshot, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_for_node(
        &cluster_paths.kv_paths(),
        state.local_node_id,
    )
    .map_err(Into::into)
}

pub(crate) async fn handle_daemon_cluster_kv_snapshot_async(
    paths: &DaemonPaths,
) -> Result<KvSnapshot, DaemonError> {
    let paths = paths.clone();
    let path = paths.root.display().to_string();
    tokio::task::spawn_blocking(move || handle_daemon_cluster_kv_snapshot(&paths))
        .await
        .map_err(|source| DaemonError::Io {
            path,
            detail: format!("cluster kv snapshot task failed: {source}"),
        })?
}

pub(crate) fn prune_local_cluster_raft_uncommitted_tail(
    paths: &DaemonPaths,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
        return Ok(());
    };
    let _raft_guard =
        sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&cluster_paths.kv_paths())?;
    daemon_cluster_kv_raft_store(&cluster_paths)
        .discard_uncommitted_tail(&state.local_node_id)
        .map(|_| ())?;
    Ok(())
}

pub(crate) async fn handle_daemon_cluster_kv_txn_request_async_on_runtime(
    paths: &DaemonPaths,
    txn: KvTxn,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    handle_daemon_cluster_kv_txn_request_async_with_forwarding(paths, txn, true, runtime).await
}

pub(crate) async fn handle_forwarded_daemon_cluster_kv_txn_request_async_on_runtime(
    paths: &DaemonPaths,
    txn: KvTxn,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    handle_daemon_cluster_kv_txn_request_async_with_forwarding(paths, txn, false, runtime).await
}

async fn handle_daemon_cluster_kv_txn_request_async_with_forwarding(
    paths: &DaemonPaths,
    txn: KvTxn,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    handle_cluster_kv_txn_request_with_cluster_paths_async_on_runtime(
        &cluster_paths,
        txn,
        allow_forwarding,
        runtime,
    )
    .await
}

pub(crate) fn handle_cluster_kv_txn_request_with_cluster_paths_on_runtime(
    cluster_paths: &ClusterPaths,
    txn: KvTxn,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    block_on_daemon_cluster_future_on_owner_runtime(
        runtime,
        handle_cluster_kv_txn_request_with_cluster_paths_async_on_runtime(
            cluster_paths,
            txn,
            allow_forwarding,
            runtime,
        ),
    )
}

pub(crate) async fn handle_cluster_kv_txn_request_with_cluster_paths_async_on_runtime(
    cluster_paths: &ClusterPaths,
    txn: KvTxn,
    allow_forwarding: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::from_handle(runtime),
    };
    sinomo_network::handle_cluster_kv_txn_request_async_with_host(
        runtime,
        cluster_paths,
        txn,
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )
    .await
    .map_err(Into::into)
}

pub(crate) struct DaemonClusterJoinRegistrationHost<'a> {
    runtime: tokio::runtime::Handle,
    endpoint_rpc_host: DaemonSignedEndpointRpcAsyncHost,
    driver: sinomo_network::ClusterJoinRegistrationDriver<
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
            endpoint_rpc_host: DaemonSignedEndpointRpcAsyncHost::new(&runtime, true),
            driver: sinomo_network::ClusterJoinRegistrationDriver::new(
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

impl sinomo_network::ClusterLocalDaemonMembershipObserver
    for DaemonLocalDaemonMembershipObserver<'_>
{
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
        sinomo_network::NetworkError,
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

impl sinomo_network::ClusterLocalDaemonMembershipAsyncObserver
    for DaemonLocalDaemonMembershipObserver<'_>
{
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
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

impl sinomo_network::ClusterMembershipRpcForwardHost for DaemonClusterJoinRegistrationHost<'_> {
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &sinomo_network::ObjectRpcRequest,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        sinomo_network::ClusterMembershipRpcForwardHost::send_membership_rpc_to_node(
            &mut self.driver,
            state,
            target_node_id,
            request,
        )
    }
}

impl sinomo_network::ClusterJoinRegistrationHost for DaemonClusterJoinRegistrationHost<'_> {
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
        sinomo_network::NetworkError,
    > {
        sinomo_network::ClusterJoinRegistrationHost::local_daemon_membership_observation(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        sinomo_network::ClusterJoinRegistrationHost::commit_join_registration_state(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), sinomo_network::NetworkError> {
        // Joining through a bridge has a narrow ordering window: the voter has
        // accepted the member before the joiner has imported the accepted state.
        // Keep this response-path observation local and let the periodic
        // connectivity publisher advertise it after the joiner settles.
        remember_cluster_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            None,
            None,
        );
        Ok(())
    }
}

impl sinomo_network::ClusterSignedEndpointRpcAsyncHost for DaemonClusterJoinRegistrationHost<'_> {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a sinomo_network::ObjectRpcRequest,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        self.endpoint_rpc_host
            .sign_endpoint_rpc_request_async(request)
    }

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a sinomo_network::ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_network::ObjectRpcResponse>>
    {
        self.endpoint_rpc_host.try_gateway_signed_rpc_async(
            endpoint,
            request,
            request_auth,
            timeouts,
        )
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_node::ClusterId>> {
        self.endpoint_rpc_host
            .endpoint_rpc_cluster_id_async(endpoint)
    }

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_network::ClusterSignedEndpointRpcObservation,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, ()> {
        self.endpoint_rpc_host
            .observe_signed_endpoint_rpc_async(endpoint, observation)
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a sinomo_network::ObjectRpcRequest,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, sinomo_network::ObjectRpcResponse> {
        self.endpoint_rpc_host
            .send_signed_endpoint_rpc_async(endpoint, auth, request, timeouts)
    }

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a sinomo_network::ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_network::ObjectRpcResponse>>
    {
        self.endpoint_rpc_host.relay_signed_endpoint_rpc_async(
            endpoint,
            request,
            request_auth,
            timeouts,
        )
    }
}

impl sinomo_network::ClusterJoinRegistrationAsyncHost for DaemonClusterJoinRegistrationHost<'_> {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
    > {
        self.driver
            .observer_mut()
            .local_daemon_membership_observation_async(paths, state)
    }

    fn commit_join_registration_state_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            sync_cluster_state_to_kv_async_on_runtime(paths, state, &runtime)
                .await
                .map_err(|error| {
                    daemon_host_network_error("cluster join registration commit", error)
                })
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_cluster_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                sinomo_network::ClusterConnectivityState::Healthy,
                None,
                None,
            );
            Ok(())
        })
    }
}

pub(crate) async fn forward_cluster_invite_challenge_to_voter_async_on_runtime(
    paths: &DaemonPaths,
    challenge: &ClusterInviteChallengeRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let rpc_request = sinomo_network::cluster_join_challenge_rpc_request(challenge)?;
    let mut host = DaemonClusterJoinRegistrationHost::new_on_runtime(paths, runtime);
    let response = sinomo_network::forward_cluster_join_challenge_rpc_to_voter_async_with_host(
        runtime,
        &cluster_paths,
        &rpc_request,
        &mut host,
    )
    .await?;
    response.decode_body().map_err(DaemonError::from)
}

fn send_object_rpc_to_node(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    rpc_request: &sinomo_network::ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: target_node_id.to_string(),
        detail: "cluster paths are unavailable for node object RPC".to_string(),
    })?;
    let mut host = DaemonSignedEndpointRpcAsyncHost::new(runtime, true);
    let mut client = sinomo_network::ClusterObjectRpcClient::routed(
        &mut host,
        sinomo_network::ClusterObjectRpcRouteContext::new(runtime, &paths),
    );
    block_on_cluster_network_future_on_owner_runtime(
        runtime,
        client.call_node(
            state,
            target_node_id,
            rpc_request,
            sinomo_network::ClusterObjectRpcNodeOptions::new(
                sinomo_network::ClusterEndpointSelectionPurpose::VoterCommand,
                sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
        ),
    )
    .map_err(DaemonError::from)
}

pub(crate) fn publish_local_members_to_quorum_if_needed_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_network::publish_local_members_to_quorum_if_needed_with_host(
        paths,
        state,
        sinomo_network::ClusterLocalMemberPublishOptions {
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
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
    };
    sinomo_network::publish_local_members_to_quorum_if_needed_async_with_host(
        &runtime,
        paths,
        state,
        sinomo_network::ClusterLocalMemberPublishOptions {
            member_publish_timeout: DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
            force_kv_snapshot_sync,
        },
        &mut host,
    )
    .await
    .map_err(Into::into)
}

#[derive(Debug)]
pub(crate) struct ClusterNetworkMaintenance {
    pub discovery: Option<AsyncMdnsDiscoverySocket>,
    pub discovery_cache: MdnsDiscoveryCache,
    pub connectivity:
        std::collections::BTreeMap<String, sinomo_network::ClusterConnectivityProbeState>,
    pub connectivity_cursor: usize,
    pub connectivity_converged: bool,
    pub environment_id: Option<String>,
    pub local_node_role: ClusterNodeRole,
    pub last_desired_rollout_attempt: Option<String>,
    pub solo_cluster_idle: bool,
    pub startup_settle_until: Option<Instant>,
    pub tick: u64,
    pub publish_failure_streak: u32,
    pub last_kv_snapshot_pull: Instant,
    pub last_publish: Instant,
    pub last_discovery: Instant,
    pub last_probe: Instant,
    pub idle_stats: Arc<IdleStatsAtomics>,
}

#[cfg(test)]
fn daemon_cluster_publish_interval(paths: &DaemonPaths) -> Duration {
    daemon_cluster_publish_interval_on_runtime(paths, None)
}

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
    sinomo_network::cluster_network_publish_interval_for_node(&cluster_paths, &cluster.node_id)
}

impl ClusterNetworkMaintenance {
    pub async fn with_idle_stats(idle_stats: Arc<IdleStatsAtomics>) -> Self {
        let now = Instant::now();
        Self {
            discovery: AsyncMdnsDiscoverySocket::bind_default().await.ok(),
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::default(),
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now
                .checked_sub(DAEMON_CLUSTER_KV_SNAPSHOT_PULL_INTERVAL)
                .unwrap_or(now),
            // Fire the first maintenance cycle immediately instead of after a full
            // interval, so a freshly started/joined node publishes its records and
            // probes its peers right away. Combined with parallel probing this lets
            // the cluster converge within a tick or two of join instead of ~60s.
            last_publish: now
                .checked_sub(DAEMON_CLUSTER_PUBLISH_INTERVAL)
                .unwrap_or(now),
            last_discovery: now
                .checked_sub(DAEMON_CLUSTER_DISCOVERY_INTERVAL)
                .unwrap_or(now),
            last_probe: now
                .checked_sub(DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL)
                .unwrap_or(now),
            idle_stats,
        }
    }

    pub async fn tick_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        runtime: tokio::runtime::Handle,
    ) -> Result<Option<ClusterNetworkMaintenancePendingWork>, DaemonError> {
        self.tick = self.tick.saturating_add(1);
        self.idle_stats.incr_ticks_total();
        let now = Instant::now();
        if self
            .startup_settle_until
            .is_some_and(|deadline| now >= deadline)
        {
            self.startup_settle_until = None;
        }
        let due = sinomo_network::cluster_network_maintenance_due(now, &self.schedule());
        let publish_due = due.publish_due;
        let probe_due = due.probe_due;
        let discovery_due = due.discovery_due;
        if !(publish_due || probe_due || discovery_due) {
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
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            return Ok(None);
        }

        let mut discovery_already_processed = false;
        if self.solo_cluster_idle && (publish_due || probe_due || discovery_due) {
            self.idle_stats.incr_disk_reads_cluster_state();
            let mut state =
                sinomo_network::read_cluster_state_without_kv_async(&runtime, &cluster_paths)
                    .await
                    .map_err(DaemonError::from)?;
            self.local_node_role = sinomo_network::cluster_local_node_role(&state);
            let mut discovered = false;
            if discovery_due {
                self.last_discovery = now;
                discovered = self.process_discovery(&mut state);
                if sinomo_network::cluster_network_stable_discovery_announcement_due(
                    self.tick, discovered,
                ) {
                    self.announce(&state).await;
                }
                discovery_already_processed = true;
                if discovered {
                    sinomo_network::write_cluster_state_async(&runtime, &cluster_paths, &state)
                        .await
                        .map_err(DaemonError::from)?;
                }
            }
            if !discovered && sinomo_network::cluster_is_settled_solo(&state) {
                if publish_due {
                    self.last_publish = now;
                }
                if probe_due {
                    self.last_probe = now;
                }
                return Ok(None);
            }
            self.solo_cluster_idle = false;
        }

        self.idle_stats.incr_ticks_with_work();
        self.idle_stats.incr_disk_reads_cluster_state();
        let mut state = if publish_due || probe_due {
            sinomo_network::read_cluster_state_async(&runtime, &cluster_paths).await
        } else {
            sinomo_network::read_cluster_state_without_kv_async(&runtime, &cluster_paths).await
        }
        .map_err(DaemonError::from)?;
        self.local_node_role = sinomo_network::cluster_local_node_role(&state);
        if sinomo_network::cluster_state_needs_join_registration(&state) {
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            self.connectivity_converged = false;
            self.startup_settle_until = now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION);
            self.solo_cluster_idle = false;
            return Ok(None);
        }
        if !self.connectivity_converged
            && self.startup_settle_until.is_none()
            && !sinomo_network::cluster_is_settled_solo(&state)
        {
            self.startup_settle_until = now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION);
        }
        if daemon_paths_are_shadow(paths) {
            if publish_due {
                self.last_publish = now;
                if let Err(error) = self.reconcile_desired_rollout(paths, &cluster_paths, &state) {
                    eprintln!("sinomo daemon desired rollout reconcile failed: {error}");
                }
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            self.connectivity_converged = true;
            self.solo_cluster_idle = sinomo_network::cluster_is_settled_solo(&state);
            return Ok(None);
        }
        let state_before_maintenance = state.clone();
        let mut discovered = false;

        if discovery_due && !discovery_already_processed {
            self.last_discovery = now;
            discovered = self.process_discovery(&mut state);
            if discovered {
                self.connectivity_converged = false;
            }
        }

        if publish_due || probe_due {
            let shadow_options = current_shadow_daemon_options_for_cluster_async(
                data_dir,
                DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
                &runtime,
            )
            .await;
            self.idle_stats.incr_disk_reads_iroh_endpoints();
            let local_state_changed = refresh_local_daemon_membership_async(
                &runtime,
                &cluster_paths,
                &mut state,
                options,
                &shadow_options,
                OffsetDateTime::now_utc(),
            )
            .await?
            .changed();
            if local_state_changed {
                self.connectivity_converged = false;
                sinomo_network::write_cluster_state_async(&runtime, &cluster_paths, &state)
                    .await
                    .map_err(DaemonError::from)?;
            }
        }

        let cluster_is_solo = sinomo_network::cluster_is_settled_solo(&state);
        sinomo_network::refresh_cluster_connectivity_environment_async(
            &runtime,
            &cluster_paths,
            &state,
            &mut self.environment_id,
            &mut self.connectivity,
            OffsetDateTime::now_utc(),
        )
        .await?;

        if publish_due {
            self.last_publish = now;
            let mut publish_failed = false;
            if cluster_is_solo {
                // no-op: nothing to publish to absent peers
            } else if sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
                if crate::dfs::default_distributed_dfs_enabled()
                    && let Err(error) = ensure_default_distributed_dfs_async_on_runtime(
                        paths,
                        options,
                        true,
                        runtime.clone(),
                    )
                    .await
                {
                    eprintln!("sinomo daemon distributed DFS control publish failed: {error}");
                    publish_failed = true;
                }
                if !sinomo_node::cluster_bootstrap_state_stable_eq(
                    &state_before_maintenance,
                    &state,
                ) && let Err(error) =
                    sync_cluster_state_to_kv_async_on_runtime(&cluster_paths, &state, &runtime)
                        .await
                {
                    eprintln!("sinomo daemon cluster state publish failed: {error}");
                    publish_failed = true;
                }
                self.idle_stats.incr_kv_publish_member();
                pending_work
                    .member_publishes
                    .push(ClusterMemberPublishPendingWork {
                        paths: cluster_paths.clone(),
                        state: state.clone(),
                        force_kv_snapshot_sync: false,
                    });
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
                    pending_work
                        .node_status_refreshes
                        .push(ClusterNodeStatusPendingRefresh {
                            paths: shadow_paths,
                            label: "shadow status",
                        });
                }
                if daemon_paths_are_shadow(paths)
                    && let Err(error) =
                        self.reconcile_desired_rollout(paths, &cluster_paths, &state)
                {
                    eprintln!("sinomo daemon desired rollout reconcile failed: {error}");
                }
            } else if !sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
                self.idle_stats.incr_kv_publish_member();
                // The forced publish doubles as the KV snapshot pull; pushes
                // from the voter keep learners converged event-driven, so the
                // pull runs on its own slow recovery cadence.
                let kv_snapshot_pull_due = self.last_kv_snapshot_pull.elapsed()
                    >= DAEMON_CLUSTER_KV_SNAPSHOT_PULL_INTERVAL;
                if kv_snapshot_pull_due {
                    self.last_kv_snapshot_pull = Instant::now();
                }
                pending_work
                    .member_publishes
                    .push(ClusterMemberPublishPendingWork {
                        paths: cluster_paths.clone(),
                        state: state.clone(),
                        force_kv_snapshot_sync: kv_snapshot_pull_due,
                    });
            }
            if publish_failed {
                self.publish_failure_streak = self.publish_failure_streak.saturating_add(1);
                self.last_publish =
                    sinomo_network::cluster_network_maintenance_last_timestamp_for_next_due(
                        now,
                        sinomo_network::cluster_network_publish_interval_for_role(
                            &self.local_node_role,
                        ),
                        self.publish_failure_backoff(),
                    );
            } else {
                self.publish_failure_streak = 0;
            }
        }

        if probe_due {
            self.last_probe = now;
            if !cluster_is_solo {
                let plan = self.select_connectivity_probe_plan(&cluster_paths, &state);
                pending_work.connectivity_probe = Some(ClusterConnectivityPendingProbe {
                    paths: cluster_paths.clone(),
                    source_node_id: state.local_node_id.clone(),
                    state: state.clone(),
                    plan,
                });
            } else {
                self.connectivity_converged = true;
            }
        }

        let maintenance_changed = discovered
            || !sinomo_node::cluster_bootstrap_state_stable_eq(&state_before_maintenance, &state);
        if discovery_due
            && !discovery_already_processed
            && sinomo_network::cluster_network_stable_discovery_announcement_due(
                self.tick,
                maintenance_changed,
            )
        {
            self.announce(&state).await;
        }

        if maintenance_changed {
            self.connectivity_converged = false;
            sinomo_network::write_cluster_state_async(&runtime, &cluster_paths, &state)
                .await
                .map_err(DaemonError::from)?;
        }
        self.solo_cluster_idle = cluster_is_solo && !discovered;
        Ok((!pending_work.is_empty()).then_some(pending_work))
    }

    fn schedule(&self) -> sinomo_network::ClusterNetworkMaintenanceSchedule {
        sinomo_network::ClusterNetworkMaintenanceSchedule {
            local_node_role: self.local_node_role.clone(),
            has_discovery: self.discovery.is_some(),
            connectivity_converged: self.connectivity_converged,
            startup_settle_until: self.startup_settle_until,
            last_publish: self.last_publish,
            last_probe: self.last_probe,
            last_discovery: self.last_discovery,
        }
    }

    pub fn next_due_duration(&self, now: Instant) -> Duration {
        sinomo_network::cluster_network_maintenance_next_due_duration(now, &self.schedule())
    }

    fn reconcile_desired_rollout(
        &mut self,
        paths: &DaemonPaths,
        cluster_paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), DaemonError> {
        let Some(desired) = sinomo_rollout::read_cluster_rollout_desired_from_kv(cluster_paths)?
        else {
            return Ok(());
        };
        if desired.state != "active"
            || desired.mode.as_deref() != Some("cluster-remote-build")
            || (desired.source_bundle.is_none() && desired.remote_repo.is_none())
        {
            return Ok(());
        }
        let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let rollout_paths = SelfRolloutPaths::new(data_dir);
        if rollout_status_matches_desired(
            read_rollout_status(&rollout_paths)?.as_ref(),
            desired.mode.as_deref(),
            desired.requested_at.as_deref(),
        ) {
            return Ok(());
        }
        if let Some(source_head) = desired.source_head.as_deref()
            && managed_source_status(&rollout_paths)
                .ok()
                .and_then(|status| status.head)
                .as_deref()
                .is_some_and(|current_head| source_heads_match(Some(current_head), source_head))
        {
            return Ok(());
        }

        let attempt_key = format!(
            "{}:{}",
            desired.requested_at.as_deref().unwrap_or("unknown-request"),
            desired
                .source_head
                .as_deref()
                .or(desired.source.as_deref())
                .unwrap_or("unknown-source")
        );
        if self.last_desired_rollout_attempt.as_deref() == Some(attempt_key.as_str()) {
            return Ok(());
        }
        self.last_desired_rollout_attempt = Some(attempt_key);

        let requested_at = desired.requested_at.clone().unwrap_or_else(|| {
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string())
        });
        let response = handle_daemon_rollout_build_request(
            paths,
            ClusterRolloutBuildRequest {
                source: desired
                    .source
                    .unwrap_or_else(|| "cluster desired rollout".to_string()),
                remote_repo: desired.remote_repo,
                source_bundle: desired.source_bundle,
                source_head: desired.source_head,
                force_source_overwrite: desired.force_source_overwrite,
                host: desired
                    .host
                    .unwrap_or_else(|| SINOMO_DAEMON_DEFAULT_HOST.to_string()),
                port: desired.port.unwrap_or(SINOMO_DAEMON_DEFAULT_PORT),
                shadow_port: desired
                    .shadow_port
                    .unwrap_or(SINOMO_SHADOW_DAEMON_DEFAULT_PORT),
                requested_at,
            },
        )?;
        if response.status == "accepted" {
            eprintln!(
                "sinomo daemon desired rollout accepted: {}",
                response.detail
            );
        } else {
            eprintln!(
                "sinomo daemon desired rollout {}: {}",
                response.status, response.detail
            );
        }
        Ok(())
    }

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
        let changed = sinomo_network::apply_lan_discovery_advertisements(
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
            sinomo_network::lan_discovery_advertisement_for_state(state, self.tick)
        else {
            return;
        };
        self.idle_stats.incr_mdns_announces_sent();
        let _ = discovery.announce(&advertisement).await;
    }

    pub(crate) async fn run_pending_work_async(
        &mut self,
        pending: ClusterNetworkMaintenancePendingWork,
        runtime: tokio::runtime::Handle,
    ) {
        // Connectivity refreshes must run before cluster writes that may need those
        // freshly-probed relay routes to reach a voter from a segmented learner.
        if let Some(probe) = pending.connectivity_probe {
            self.run_pending_connectivity_probe_async(probe, runtime.clone())
                .await;
        }
        for publish in pending.member_publishes {
            self.run_pending_member_publish_async(publish, runtime.clone())
                .await;
        }
        for refresh in pending.node_status_refreshes {
            self.run_pending_node_status_refresh_async(refresh, runtime.clone())
                .await;
        }
    }

    async fn run_pending_node_status_refresh_async(
        &mut self,
        pending: ClusterNodeStatusPendingRefresh,
        runtime: tokio::runtime::Handle,
    ) {
        match refresh_daemon_node_status_in_kv_async(&pending.paths, runtime).await {
            Ok(published) => {
                self.record_node_status_publish(&published);
                self.record_publish_success();
            }
            Err(error) => {
                eprintln!("sinomo daemon {} publish failed: {error}", pending.label);
                self.record_publish_failure();
            }
        }
    }

    async fn run_pending_member_publish_async(
        &mut self,
        pending: ClusterMemberPublishPendingWork,
        runtime: tokio::runtime::Handle,
    ) {
        let result = publish_local_members_to_quorum_if_needed_async(
            &pending.paths,
            &pending.state,
            pending.force_kv_snapshot_sync,
            runtime,
        )
        .await
        .map(|_| ());
        match result {
            Ok(()) => self.record_publish_success(),
            Err(error) => {
                eprintln!("sinomo daemon cluster member publish failed: {error}");
                self.record_publish_failure();
            }
        }
    }

    fn record_publish_failure(&mut self) {
        self.publish_failure_streak = self.publish_failure_streak.saturating_add(1);
        let now = Instant::now();
        self.last_publish = sinomo_network::cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            sinomo_network::cluster_network_publish_interval_for_role(&self.local_node_role),
            self.publish_failure_backoff(),
        );
    }

    /// Exponential backoff over consecutive publish failures: quick retries
    /// while the cluster is still forming, the full backoff once a failure
    /// looks persistent.
    fn publish_failure_backoff(&self) -> Duration {
        let exponent = self.publish_failure_streak.saturating_sub(1).min(8);
        DAEMON_CLUSTER_PUBLISH_FAILURE_INITIAL_BACKOFF
            .saturating_mul(1_u32 << exponent)
            .min(DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF)
    }

    fn record_publish_success(&mut self) {
        self.publish_failure_streak = 0;
    }

    pub(crate) async fn run_pending_connectivity_probe_async(
        &mut self,
        pending: ClusterConnectivityPendingProbe,
        runtime: tokio::runtime::Handle,
    ) {
        let records = self
            .execute_connectivity_probe_plan_async(&pending.state, pending.plan)
            .await;
        let mut host = DaemonClusterControlPlaneTransport {
            rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
        };
        let published =
            sinomo_network::sync_cluster_connectivity_records_to_quorum_async_with_host(
                &runtime,
                &pending.paths,
                &pending.source_node_id,
                records,
                &mut host,
            )
            .await;
        match published {
            Ok(Some(_)) => {
                self.connectivity_converged = true;
                self.idle_stats.incr_kv_publish_connectivity();
            }
            Ok(None) => {
                self.connectivity_converged = true;
            }
            Err(error) => {
                self.connectivity_converged = true;
                eprintln!("sinomo daemon cluster connectivity publish failed: {error}");
            }
        }
    }

    async fn execute_connectivity_probe_plan_async(
        &mut self,
        state: &ClusterBootstrapState,
        plan: ClusterConnectivityProbePlan,
    ) -> Vec<ClusterConnectivityRecord> {
        let probed = probe_connectivity_batch_async(
            state.clone(),
            plan.entries.clone(),
            plan.timestamp.clone(),
            plan.now,
            plan.startup_settle_active,
        )
        .await;
        self.apply_connectivity_probe_plan(plan, probed)
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
        let batch = sinomo_network::select_cluster_connectivity_probe_batch(
            paths,
            state,
            &mut self.connectivity,
            self.connectivity_cursor,
            &self.local_node_role,
            startup_settle_active,
            now,
        );
        self.connectivity_cursor = batch.next_cursor;
        self.idle_stats
            .add_connectivity_probes_sent(batch.entries.len() as u64);

        ClusterConnectivityProbePlan {
            active_keys: batch.active_keys,
            entries: batch.entries,
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
        sinomo_network::apply_cluster_connectivity_probe_results(
            &mut self.connectivity,
            &plan.active_keys,
            probed,
        )
    }
}

async fn probe_connectivity_batch_async(
    state: ClusterBootstrapState,
    entries: Vec<sinomo_network::ClusterConnectivityProbeBatchEntry>,
    timestamp: String,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
) -> Vec<(String, ClusterConnectivityRecord)> {
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
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let result = fetch_daemon_status_from_endpoint_direct_async(
                &entry.endpoint,
                CLUSTER_HEALTH_PROBE_TIMEOUT,
            )
            .await;
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
    entry: sinomo_network::ClusterConnectivityProbeBatchEntry,
    result: Result<crate::DaemonStatus, DaemonError>,
    latency_ms: u64,
    timestamp: &str,
    now: time::OffsetDateTime,
    startup_settle_active: bool,
) -> (String, ClusterConnectivityRecord) {
    let record = match &result {
        Ok(status) if daemon_cluster_status_matches(state, &entry.endpoint, status) => {
            sinomo_network::cluster_connectivity_record_for_probe_outcome(
                state,
                &entry.endpoint,
                entry.previous.as_ref(),
                true,
                latency_ms,
                timestamp,
                now,
            )
        }
        Ok(status) => {
            eprintln!(
                "sinomo daemon connectivity probe identity mismatch: endpoint {} expected node {}, responder reported cluster={:?} node={:?}",
                entry.endpoint.address,
                entry.endpoint.node_id,
                status.cluster.as_ref().map(|c| c.cluster_id.to_string()),
                status.cluster.as_ref().map(|c| c.node_id.to_string()),
            );
            if status.cluster.is_some() {
                sinomo_network::cluster_connectivity_record_for_identity_mismatch(
                    state,
                    &entry.endpoint,
                    timestamp,
                    now,
                )
            } else if startup_settle_active {
                sinomo_network::cluster_connectivity_record_for_startup_probe_failure(
                    state,
                    &entry.endpoint,
                    entry.previous.as_ref(),
                    latency_ms,
                    timestamp,
                    now,
                )
            } else {
                sinomo_network::cluster_connectivity_record_for_probe_outcome(
                    state,
                    &entry.endpoint,
                    entry.previous.as_ref(),
                    false,
                    latency_ms,
                    timestamp,
                    now,
                )
            }
        }
        Err(_) if startup_settle_active => {
            sinomo_network::cluster_connectivity_record_for_startup_probe_failure(
                state,
                &entry.endpoint,
                entry.previous.as_ref(),
                latency_ms,
                timestamp,
                now,
            )
        }
        Err(_) => sinomo_network::cluster_connectivity_record_for_probe_outcome(
            state,
            &entry.endpoint,
            entry.previous.as_ref(),
            false,
            latency_ms,
            timestamp,
            now,
        ),
    };
    (entry.key, record)
}

#[cfg(test)]
fn cluster_connectivity_probe_due(
    record: &sinomo_network::ClusterConnectivityRecord,
    now: time::OffsetDateTime,
    refresh_deadline: time::OffsetDateTime,
    local_node_role: &ClusterNodeRole,
) -> bool {
    sinomo_network::cluster_connectivity_probe_due_with_startup_settle(
        record,
        now,
        refresh_deadline,
        local_node_role,
        false,
    )
}

async fn fetch_daemon_status_from_endpoint_direct_async(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<crate::DaemonStatus, DaemonError> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let timeouts = sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout);
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonSignedEndpointRpcAsyncHost::direct_only(&runtime);
    let mut client = sinomo_network::ClusterObjectRpcClient::endpoint_only(&mut host);
    let response = client
        .call_endpoint(
            endpoint,
            &request,
            sinomo_network::ClusterObjectRpcOptions::new(timeouts),
        )
        .await
        .map_err(DaemonError::from)?;
    response.decode_body().map_err(DaemonError::from)
}

/// Records which of the two heartbeat records `refresh_daemon_node_status_in_kv`
/// actually wrote to cluster KV, so callers can account real publishes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NodeStatusPublishOutcome {
    pub network_info_published: bool,
    pub node_status_published: bool,
}

pub(crate) fn refresh_daemon_node_status_in_kv(
    paths: &DaemonPaths,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    refresh_daemon_node_status_in_kv_on_runtime(paths, None)
}

pub(crate) fn refresh_daemon_node_status_in_kv_on_runtime(
    paths: &DaemonPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster_on_runtime(paths, &mut status, runtime)?;
    let Some(cluster) = status.cluster.as_ref() else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let network_info = sinomo_network::build_cluster_network_info_record(
        &state,
        &cluster.node_id,
        time::OffsetDateTime::now_utc(),
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    );
    let record = cluster_node_status_from_daemon_status(
        &status,
        cluster,
        read_rollout_status(&rollout_paths)?,
        time::OffsetDateTime::now_utc(),
    );
    let Some(runtime) = runtime else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let mut publish_match_host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    let network_info_matches = sinomo_network::cluster_network_info_publish_matches_with_host(
        &cluster_paths,
        &network_info,
        &mut publish_match_host,
    )
    .unwrap_or(false);
    let node_status_matches = sinomo_network::cluster_node_status_publish_matches_with_host(
        &cluster_paths,
        &record,
        &mut publish_match_host,
    )
    .unwrap_or(false);
    if network_info_matches && node_status_matches {
        return Ok(NodeStatusPublishOutcome::default());
    }
    if !cluster_has_local_quorum_on_runtime(&cluster_paths, &state, runtime) {
        return Ok(NodeStatusPublishOutcome::default());
    }
    // Publish each record independently so the slow-changing network-info record
    // is not rewritten on every node-status heartbeat. Node status refreshes on a
    // ~60s margin; network info on a ~9min margin. Bundling them rewrote the KV
    // snapshot far more often than necessary.
    let mut outcome = NodeStatusPublishOutcome::default();
    if !network_info_matches {
        if let Err(error) =
            sync_cluster_network_info_to_kv_on_runtime(&cluster_paths, network_info, runtime)
        {
            eprintln!("sinomo daemon network info publish failed: {error}");
        } else {
            outcome.network_info_published = true;
        }
    }
    if !node_status_matches {
        sync_cluster_node_status_to_kv_on_runtime(&cluster_paths, record, runtime)?;
        outcome.node_status_published = true;
    }
    Ok(outcome)
}

pub(crate) async fn refresh_daemon_node_status_in_kv_async(
    paths: &DaemonPaths,
    runtime: tokio::runtime::Handle,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    let read_paths = paths.clone();
    let read_path = read_paths.root.display().to_string();
    let mut status = tokio::task::spawn_blocking(move || read_daemon_status(&read_paths))
        .await
        .map_err(|source| DaemonError::Io {
            path: read_path,
            detail: format!("daemon status read task failed: {source}"),
        })??;
    ensure_daemon_status_cluster_async(paths, &mut status, &runtime).await?;
    let Some(cluster) = status.cluster.clone() else {
        return Ok(NodeStatusPublishOutcome::default());
    };
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let state_paths = cluster_paths.clone();
    let state_path = state_paths.root.display().to_string();
    let state = tokio::task::spawn_blocking(move || {
        sinomo_network::read_cluster_state_without_kv(&state_paths)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: state_path,
        detail: format!("cluster state read task failed: {source}"),
    })?
    .map_err(DaemonError::from)?;
    let network_info = sinomo_network::build_cluster_network_info_record(
        &state,
        &cluster.node_id,
        time::OffsetDateTime::now_utc(),
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    );
    let rollout_path = rollout_paths.root.display().to_string();
    let rollout_status = tokio::task::spawn_blocking(move || read_rollout_status(&rollout_paths))
        .await
        .map_err(|source| DaemonError::Io {
            path: rollout_path,
            detail: format!("rollout status read task failed: {source}"),
        })??;
    let record = cluster_node_status_from_daemon_status(
        &status,
        &cluster,
        rollout_status,
        time::OffsetDateTime::now_utc(),
    );
    let mut network_match_host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
    };
    let network_info_matches =
        sinomo_network::cluster_network_info_publish_matches_async_with_host(
            &runtime,
            &cluster_paths,
            &network_info,
            &mut network_match_host,
        )
        .await
        .unwrap_or(false);
    let mut node_match_host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
    };
    let node_status_matches = sinomo_network::cluster_node_status_publish_matches_async_with_host(
        &runtime,
        &cluster_paths,
        &record,
        &mut node_match_host,
    )
    .await
    .unwrap_or(false);
    if network_info_matches && node_status_matches {
        return Ok(NodeStatusPublishOutcome::default());
    }

    let has_quorum =
        cluster_has_local_quorum_async(&cluster_paths, &state, runtime.clone()).await?;
    if !has_quorum {
        return Ok(NodeStatusPublishOutcome::default());
    }

    let mut outcome = NodeStatusPublishOutcome::default();
    if !network_info_matches {
        sync_cluster_network_info_to_kv_async(&cluster_paths, network_info, runtime.clone())
            .await?;
        outcome.network_info_published = true;
    }
    if !node_status_matches {
        sync_cluster_node_status_to_kv_async(&cluster_paths, record, runtime).await?;
        outcome.node_status_published = true;
    }
    Ok(outcome)
}

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
    let mut state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    if sinomo_network::cluster_state_needs_join_registration(&state) {
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
    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
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
        sinomo_network::read_cluster_state_without_kv(&read_paths)
    })
    .await
    .map_err(|source| DaemonError::Io {
        path: read_path,
        detail: format!("cluster state read task failed: {source}"),
    })?
    .map_err(DaemonError::from)?;
    if sinomo_network::cluster_state_needs_join_registration(&state) {
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
        sinomo_network::write_cluster_state(&write_paths, &state_for_write)
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

pub(crate) fn sync_cluster_state_to_kv_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    sinomo_network::sync_cluster_state_to_kv_with_host(
        paths,
        state,
        &mut DaemonClusterMemberMutationHost::new_on_runtime(runtime),
    )
    .map_err(Into::into)
}

pub(crate) async fn sync_cluster_state_to_kv_async_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    sinomo_network::sync_cluster_state_to_kv_async_with_host(
        runtime,
        paths,
        state,
        &mut DaemonClusterMemberMutationHost::new_on_runtime(runtime),
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn read_local_cluster_state(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_network::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

#[cfg(test)]
fn read_local_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_network::read_cluster_state_without_kv(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

async fn fetch_local_cluster_health_rows_from_member_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<Vec<ClusterHealthRow>>, DaemonError> {
    let paths_for_targets = paths.clone();
    let state_for_targets = state.clone();
    let member_for_targets = member.clone();
    let member_node_id = member.node_id.as_str().to_string();
    let endpoints = runtime
        .spawn_blocking(move || {
            sinomo_network::ClusterEndpointSelector::new(&paths_for_targets, &state_for_targets)
                .select_member(
                    &member_for_targets,
                    sinomo_network::ClusterEndpointSelectionPurpose::MemberDaemonStatus,
                )
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: member_node_id,
            detail: format!("cluster health peer target selection task failed: {source}"),
        })?;
    let request = sinomo_network::cluster_local_health_rpc_request()?;
    for endpoint in endpoints {
        let Ok(response) = send_daemon_rpc_to_endpoint_async(
            &endpoint,
            &request,
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(CLUSTER_HEALTH_PEER_ROWS_TIMEOUT),
            runtime,
        )
        .await
        else {
            continue;
        };
        let Ok(rows) = sinomo_network::decode_cluster_health_rpc_response(&response) else {
            continue;
        };
        return Ok(Some(rows));
    }
    Ok(None)
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
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
        Box::pin(async move {
            probe_member_daemon_status_async(self.paths, state, member, &self.runtime)
                .await
                .map_err(|error| daemon_host_network_error("cluster health probe", error))
        })
    }

    fn peer_local_health_rows_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        member: &'a ClusterMemberRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
        Box::pin(async move {
            fetch_local_cluster_health_rows_from_member_async(
                self.paths,
                state,
                member,
                &self.runtime,
            )
            .await
            .map_err(|error| daemon_host_network_error("cluster health peer rows", error))
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
    let paths_for_targets = paths.clone();
    let state_for_targets = state.clone();
    let member_for_targets = member.clone();
    let member_node_id = member.node_id.as_str().to_string();
    let endpoints = tokio::task::spawn_blocking(move || {
        sinomo_network::ClusterEndpointSelector::new(&paths_for_targets, &state_for_targets)
            .select_member(
                &member_for_targets,
                sinomo_network::ClusterEndpointSelectionPurpose::MemberDaemonStatus,
            )
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: member_node_id,
        detail: format!("cluster health target selection task failed: {source}"),
    })?;
    let mut probes = tokio::task::JoinSet::new();
    for endpoint in endpoints {
        probes.spawn(async move {
            let result = fetch_daemon_status_from_endpoint_direct_async(
                &endpoint,
                CLUSTER_HEALTH_PROBE_TIMEOUT,
            )
            .await;
            (endpoint, result)
        });
    }
    while let Some(joined) = probes.join_next().await {
        let (endpoint, status) = match joined {
            Ok(probe) => probe,
            Err(error) => {
                eprintln!(
                    "sinomo daemon cluster health probe task for {} failed: {error}",
                    member.node_id
                );
                continue;
            }
        };
        let status = match status {
            Ok(status) => status,
            Err(error) => {
                eprintln!(
                    "sinomo daemon cluster health probe of {} via {} failed: {error}",
                    member.node_id, endpoint.address
                );
                continue;
            }
        };
        let Some(cluster) = status.cluster.as_ref() else {
            eprintln!(
                "sinomo daemon cluster health probe of {} via {}: status has no cluster section",
                member.node_id, endpoint.address
            );
            continue;
        };
        let cluster_id_matches =
            cluster.cluster_id == state.cluster_id || cluster.cluster_id == member.cluster_id;
        if !cluster_id_matches || cluster.node_id != member.node_id {
            eprintln!(
                "sinomo daemon cluster health probe of {} via {}: identity mismatch (expected cluster {} or member cluster {}, responder cluster {} node {})",
                member.node_id,
                endpoint.address,
                state.cluster_id,
                member.cluster_id,
                cluster.cluster_id,
                cluster.node_id
            );
            continue;
        }
        let status = cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        );
        if cluster_member_expected_daemon_process(member)
            .is_some_and(|process| status.process != process)
        {
            eprintln!(
                "sinomo daemon cluster health probe of {} via {}: process mismatch (expected {}, responder {})",
                member.node_id,
                endpoint.address,
                cluster_member_expected_daemon_process(member).unwrap_or("unknown"),
                status.process
            );
            continue;
        }
        return Ok(Some(status));
    }
    Ok(None)
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
    Ok(())
}

#[cfg(test)]
fn initialize_local_cluster(
    paths: &ClusterPaths,
    cluster_id: Option<ClusterId>,
    node_id: Option<NodeId>,
    name: String,
    now: OffsetDateTime,
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
        name.clone(),
        local_node_id,
        name,
        main_identity.public_key.clone(),
        now,
    );
    let _ = refresh_local_daemon_membership(
        paths,
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        now,
    )?;
    sinomo_network::write_cluster_state(paths, &state)?;
    daemon_locked_set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    sync_cluster_state_to_kv(paths, &state)?;
    Ok(state)
}

#[cfg(test)]
fn update_cluster_node_role(
    paths: &ClusterPaths,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<sinomo_network::ClusterNodeRoleUpdate, DaemonError> {
    let state = read_local_cluster_state(paths)?;
    let runtime = daemon_cluster_test_runtime();
    let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
    sinomo_network::update_cluster_node_role_with_host(paths, state, node_id, role, now, &mut host)
        .map_err(Into::into)
}

pub(crate) struct DaemonClusterMemberMutationHost {
    runtime: tokio::runtime::Handle,
    driver: sinomo_network::ClusterMemberMutationDriver<
        DaemonClusterControlPlaneTransport,
        DaemonClusterStateSyncMembershipObserver,
    >,
}

impl DaemonClusterMemberMutationHost {
    pub(crate) fn new_on_runtime(runtime: &tokio::runtime::Handle) -> Self {
        let runtime = runtime.clone();
        Self {
            runtime: runtime.clone(),
            driver: sinomo_network::ClusterMemberMutationDriver::new(
                daemon_cluster_control_plane_driver_with_mode(DaemonClusterRpcMode::new(
                    runtime.clone(),
                )),
                DaemonClusterStateSyncMembershipObserver {
                    runtime: runtime.clone(),
                },
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct DaemonClusterStateSyncMembershipObserver {
    runtime: tokio::runtime::Handle,
}

impl sinomo_network::ClusterLocalDaemonMembershipObserver
    for DaemonClusterStateSyncMembershipObserver
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
        sinomo_network::NetworkError,
    > {
        let data_dir = paths
            .root
            .parent()
            .ok_or_else(|| sinomo_network::NetworkError::Io {
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
            local_daemon_membership_observation_async(
                data_dir,
                state,
                &main_options,
                &shadow_options,
                timestamp,
                &runtime_for_future,
            ),
        );
        observation
            .map(Some)
            .map_err(|error| daemon_host_network_error("cluster state membership sync", error))
    }
}

impl sinomo_network::ClusterLocalDaemonMembershipAsyncObserver
    for DaemonClusterStateSyncMembershipObserver
{
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<
        'a,
        Option<sinomo_network::ClusterLocalDaemonMembershipObservation>,
    > {
        let data_dir = match paths.root.parent() {
            Some(data_dir) => data_dir.to_path_buf(),
            None => {
                let detail = format!(
                    "cluster paths have no data directory parent: {}",
                    paths.root.display()
                );
                return Box::pin(async move {
                    Err(sinomo_network::NetworkError::Io {
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
                .map_err(|source| sinomo_network::NetworkError::Io {
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

impl sinomo_network::ClusterMemberMutationHost for DaemonClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), sinomo_network::NetworkError> {
        sinomo_network::ClusterMemberMutationHost::prepare_cluster_state_for_kv_sync(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, sinomo_network::NetworkError> {
        sinomo_network::ClusterMemberMutationHost::current_cluster_kv_store(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        sinomo_network::ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.driver,
            paths,
            state,
            txn,
        )
    }
}

async fn commit_cluster_kv_txn_with_quorum_async_on_transport(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    allow_forwarding: bool,
    transport: &mut DaemonClusterControlPlaneTransport,
) -> Result<KvSnapshot, sinomo_network::NetworkError> {
    let runtime = transport.rpc_mode.runtime().clone();
    sinomo_network::commit_cluster_kv_txn_with_quorum_response_async_with_host(
        &runtime,
        paths,
        state,
        txn,
        cluster_kv_txn_quorum_options(allow_forwarding),
        transport,
    )
    .await
    .map(|result| result.snapshot)
}

impl sinomo_network::ClusterMemberMutationAsyncHost for DaemonClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            let Some(observation) = self
                .driver
                .observer_mut()
                .local_daemon_membership_observation_async(paths, state)
                .await?
            else {
                return Ok(());
            };
            sinomo_network::refresh_local_daemon_membership_from_disk_async(
                &self.runtime,
                paths,
                state,
                observation,
            )
            .await?;
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let mut transport = DaemonClusterControlPlaneTransport {
                rpc_mode: DaemonClusterRpcMode::new(runtime),
            };
            commit_cluster_kv_txn_with_quorum_async_on_transport(
                paths,
                state,
                txn,
                true,
                &mut transport,
            )
            .await
        })
    }
}

fn daemon_cluster_control_plane_driver_with_mode(
    rpc_mode: DaemonClusterRpcMode,
) -> sinomo_network::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    sinomo_network::ClusterControlPlaneDriver::with_timeouts(
        DaemonClusterControlPlaneTransport { rpc_mode },
        cluster_control_plane_timeouts(),
    )
}

pub(crate) fn daemon_cluster_control_plane_driver_on_runtime(
    runtime: &tokio::runtime::Handle,
) -> sinomo_network::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    daemon_cluster_control_plane_driver_with_mode(DaemonClusterRpcMode::from_handle(runtime))
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonClusterControlPlaneTransport {
    rpc_mode: DaemonClusterRpcMode,
}

impl sinomo_network::ClusterControlPlaneTransport for DaemonClusterControlPlaneTransport {
    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, sinomo_network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        send_cluster_raft_append_on_runtime(endpoint, request, timeout, runtime).map_err(|error| {
            sinomo_network::NetworkError::Io {
                operation: "cluster raft append".to_string(),
                detail: error.to_string(),
            }
        })
    }

    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, sinomo_network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        send_cluster_raft_vote_on_runtime(endpoint, request, timeout, runtime).map_err(|error| {
            sinomo_network::NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: error.to_string(),
            }
        })
    }

    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        fetch_daemon_status_from_endpoint_on_runtime(endpoint, timeout, self.rpc_mode.runtime())
            .is_ok()
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        let runtime = self.rpc_mode.runtime();
        fetch_cluster_kv_snapshot_from_endpoint_on_runtime(endpoint, timeout, runtime)
            .map_err(|error| daemon_host_network_error("cluster kv snapshot", error))
    }

    fn send_cluster_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_network::NetworkError> {
        let result = send_daemon_rpc_to_endpoint_on_runtime(
            endpoint,
            request,
            timeout,
            self.rpc_mode.runtime(),
        );
        result.map_err(|error| daemon_host_network_error("cluster membership rpc", error))
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_network::NetworkError> {
        send_object_rpc_to_node(
            state,
            target_node_id,
            request,
            timeout,
            self.rpc_mode.runtime(),
        )
        .map_err(|error| daemon_host_network_error("cluster node rpc", error))
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: sinomo_network::ClusterConnectivityState,
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

    fn fan_out_committed_cluster_raft_command(
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

    fn on_raft_append_sent(&mut self) {
        if let Some(stats) = crate::process_idle_stats() {
            stats.incr_raft_appends_sent();
        }
    }
}

impl sinomo_network::ClusterVoterReachabilityAsyncHost for DaemonClusterControlPlaneTransport {
    fn probe_voter_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, bool> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            Ok(
                fetch_daemon_status_from_endpoint_async(endpoint, timeout, &runtime)
                    .await
                    .is_ok(),
            )
        })
    }
}

impl sinomo_network::ClusterRaftVoteTransportAsyncHost for DaemonClusterControlPlaneTransport {
    fn send_raft_vote_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            send_cluster_raft_vote_async(endpoint, request, timeout, &runtime)
                .await
                .map_err(|error| sinomo_network::NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: error.to_string(),
                })
        })
    }
}

impl sinomo_network::ClusterSignedEndpointRpcAsyncHost for DaemonClusterControlPlaneTransport {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let runtime = self.rpc_mode.runtime().clone();
        let request = request.clone();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.sign_endpoint_rpc_request_async(&request).await
        })
    }

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        let request = request.clone();
        let request_auth = request_auth.to_vec();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.try_gateway_signed_rpc_async(&endpoint, &request, &request_auth, timeouts)
                .await
        })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_node::ClusterId>> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.endpoint_rpc_cluster_id_async(&endpoint).await
        })
    }

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_network::ClusterSignedEndpointRpcObservation,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, ()> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.observe_signed_endpoint_rpc_async(&endpoint, observation)
                .await
        })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        let auth = auth.to_vec();
        let request = request.clone();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.send_signed_endpoint_rpc_async(&endpoint, &auth, &request, timeouts)
                .await
        })
    }

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        let runtime = self.rpc_mode.runtime().clone();
        let endpoint = endpoint.clone();
        let request = request.clone();
        let request_auth = request_auth.to_vec();
        Box::pin(async move {
            let mut host = DaemonSignedEndpointRpcAsyncHost::new(&runtime, true);
            host.relay_signed_endpoint_rpc_async(&endpoint, &request, &request_auth, timeouts)
                .await
        })
    }
}

impl sinomo_network::ClusterLocalMemberPublishAsyncHost for DaemonClusterControlPlaneTransport {
    fn cluster_has_local_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move {
            sinomo_network::cluster_has_local_quorum_async_with_host(
                paths,
                state,
                cluster_voter_reachability_options(),
                self,
            )
            .await
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, txn, true, self)
                .await
        })
    }

    fn send_cluster_member_publish_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterMemberPublishResponse> {
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

impl sinomo_network::ClusterRaftAppendReplicationAsyncHost for DaemonClusterControlPlaneTransport {
    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, u64> {
        Box::pin(async move {
            paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index_async(&state.local_node_id)
                .await
                .map_err(Into::into)
        })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest> {
        Box::pin(async move {
            paths
                .kv_paths()
                .raft_store()
                .locked_build_cluster_raft_append_request_from_next_index_async(state, next_index)
                .await
                .map_err(Into::into)
        })
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            send_cluster_raft_append_async(endpoint, request, timeout, &runtime)
                .await
                .map_err(|error| sinomo_network::NetworkError::Io {
                    operation: "cluster raft append".to_string(),
                    detail: error.to_string(),
                })
        })
    }

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            if let Some(stats) = crate::process_idle_stats() {
                stats.incr_raft_appends_sent();
            }
            Ok(())
        })
    }
}

impl sinomo_network::ClusterConnectivityPublishAsyncHost for DaemonClusterControlPlaneTransport {
    type Error = sinomo_network::NetworkError;

    fn published_connectivity_record_set_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        source_node_id: &'a NodeId,
        records: &'a [ClusterConnectivityRecord],
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, bool> {
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
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_connectivity_record_set(paths, source_node_id, records);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, txn, true, self)
                .await
        })
    }
}

impl sinomo_network::ClusterNetworkInfoPublishAsyncHost for DaemonClusterControlPlaneTransport {
    type Error = sinomo_network::NetworkError;

    fn published_network_info_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        info: &'a ClusterNetworkInfoRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move { Ok(cached_network_info_matches(paths, info)) })
    }

    fn remember_published_network_info_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        info: &'a ClusterNetworkInfoRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_network_info(paths, info);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, txn, true, self)
                .await
        })
    }
}

impl sinomo_network::ClusterNodeStatusPublishAsyncHost for DaemonClusterControlPlaneTransport {
    type Error = sinomo_network::NetworkError;

    fn published_node_status_matches_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        status: &'a ClusterNodeStatusRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async move { Ok(cached_node_status_matches(paths, status)) })
    }

    fn remember_published_node_status_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        status: &'a ClusterNodeStatusRecord,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            remember_node_status(paths, status);
            Ok(())
        })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            commit_cluster_kv_txn_with_quorum_async_on_transport(paths, state, txn, true, self)
                .await
        })
    }
}

impl sinomo_network::ClusterRaftCommittedFanoutAsyncHost for DaemonClusterControlPlaneTransport {
    fn replicate_cluster_raft_to_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome> {
        Box::pin(async move {
            sinomo_network::replicate_cluster_raft_to_endpoint_async_with_host(
                paths,
                state,
                endpoint,
                watch_events,
                cluster_control_plane_timeouts().raft_append_replication_options(timeout),
                self,
            )
            .await
        })
    }

    fn send_cluster_kv_watch_events_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        let runtime = self.rpc_mode.runtime().clone();
        Box::pin(async move {
            send_cluster_kv_watch_events_async(endpoint, request, timeout, &runtime)
                .await
                .map_err(|error| daemon_host_network_error("cluster kv watch-event fanout", error))
        })
    }
}

impl sinomo_network::ClusterRaftQuorumAsyncHost for DaemonClusterControlPlaneTransport {
    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        sinomo_network::ClusterControlPlaneTransport::record_endpoint_command_connectivity(
            self,
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) -> sinomo_network::ClusterMembershipRpcFuture<'a, ()> {
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
    let mut state = sinomo_network::read_cluster_state_without_kv(paths)?;
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
    sinomo_network::publish_cluster_node_status_to_quorum_with_host(paths, status, &mut host)
        .map_err(Into::into)
}

pub(crate) async fn sync_cluster_node_status_to_kv_async(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    runtime: tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
    };
    sinomo_network::publish_cluster_node_status_to_quorum_async_with_host(
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
    sinomo_network::publish_cluster_network_info_to_quorum_with_host(paths, info, &mut host)
        .map_err(Into::into)
}

pub(crate) async fn sync_cluster_network_info_to_kv_async(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    runtime: tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime.clone()),
    };
    sinomo_network::publish_cluster_network_info_to_quorum_async_with_host(
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

pub(crate) fn sync_cluster_connectivity_records_to_kv_on_runtime(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_network::sync_cluster_connectivity_records_to_quorum_with_host(
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
    sinomo_network::assign_local_daemon_member_public_keys(
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
) -> Result<sinomo_network::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let lan_ips = sinomo_network::discover_default_lan_ip_candidates();
    let route_ips =
        sinomo_network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state(state);
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let main_endpoints = crate::daemon_advertised_endpoints_for_state(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
    );
    let shadow_endpoints = crate::daemon_advertised_endpoints_for_state(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
    );
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    Ok(sinomo_network::ClusterLocalDaemonMembershipObservation {
        main_endpoints,
        shadow_endpoints,
        main_public_key: main_identity.public_key,
        shadow_public_key: shadow_identity.public_key,
        timestamp,
    })
}

async fn local_daemon_membership_observation_async(
    data_dir: &std::path::Path,
    state: &ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let lan_ips = sinomo_network::discover_default_lan_ip_candidates_async();
    let route_ips =
        sinomo_network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state_async(
            state,
        );
    let (lan_ips, route_ips) = tokio::join!(lan_ips, route_ips);
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let main_endpoints = crate::daemon_advertised_endpoints_for_state_async(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
    );
    let shadow_endpoints = crate::daemon_advertised_endpoints_for_state_async(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
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
    Ok(sinomo_network::ClusterLocalDaemonMembershipObservation {
        main_endpoints,
        shadow_endpoints,
        main_public_key: main_identity.public_key,
        shadow_public_key: shadow_identity.public_key,
        timestamp,
    })
}

fn local_daemon_membership_observation_from_daemon_paths_on_runtime(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ClusterLocalDaemonMembershipObservation, DaemonError> {
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
        local_daemon_membership_observation_async(
            data_dir,
            state,
            &main_options,
            &shadow_options,
            now,
            &runtime_for_future,
        ),
    )
}

async fn local_daemon_membership_observation_from_daemon_paths_async(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ClusterLocalDaemonMembershipObservation, DaemonError> {
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
) -> Result<sinomo_network::ClusterLocalDaemonMembershipRefresh, DaemonError> {
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
        local_daemon_membership_observation_async(
            data_dir,
            state,
            main_options,
            shadow_options,
            timestamp,
            &runtime_for_future,
        ),
    )?;
    sinomo_network::refresh_local_daemon_membership_from_disk(paths, state, observation)
        .map_err(Into::into)
}

async fn refresh_local_daemon_membership_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<sinomo_network::ClusterLocalDaemonMembershipRefresh, DaemonError> {
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
    let result = sinomo_network::refresh_local_daemon_membership_from_disk_async(
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
) -> Result<sinomo_network::ClusterLocalDaemonMembershipRefresh, DaemonError> {
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
    sinomo_network::refresh_local_daemon_membership_from_disk(paths, state, observation)
        .map_err(Into::into)
}

pub(crate) fn cluster_has_local_quorum_on_runtime(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> bool {
    let mut host = daemon_cluster_control_plane_driver_on_runtime(runtime);
    sinomo_network::cluster_has_local_quorum_with_host(
        paths,
        state,
        cluster_voter_reachability_options(),
        &mut host,
    )
}

pub(crate) async fn cluster_has_local_quorum_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    runtime: tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    let mut host = DaemonClusterControlPlaneTransport {
        rpc_mode: DaemonClusterRpcMode::new(runtime),
    };
    sinomo_network::cluster_has_local_quorum_async_with_host(
        paths,
        state,
        cluster_voter_reachability_options(),
        &mut host,
    )
    .await
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
        sinomo_network::spawn_cluster_raft_committed_fanout_worker_on_runtime(
            runtime,
            paths,
            state,
            events,
            cluster_raft_committed_fanout_options(),
            move || daemon_cluster_control_plane_driver_on_runtime(&host_runtime),
        );
    } else {
        let _ = (paths, state, events);
        eprintln!(
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
    let result = sinomo_network::commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        state,
        txn,
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )?;
    Ok((result.snapshot, result.txn_response))
}

/// Per-hop timeout for a relayed cluster RPC. Healthy LAN/container hops answer quickly;
/// long no-progress waits let background relays occupy foreground route capacity.
const DAEMON_CLUSTER_RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Handle a `CLUSTER_RELAY_ROUTE` request. Runs the pure routing decision, then either
/// delivers the wrapped request locally (when this node is the target) by re-injecting it
/// through the daemon's own control port, or forwards it one hop closer to the target over
/// the connectivity graph. The inner request's response flows back along the relay chain.
pub(crate) async fn handle_daemon_cluster_relay_request_on_runtime(
    paths: &DaemonPaths,
    envelope: sinomo_network::RelayEnvelope,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    if relay_envelope_inner_expects_streamed_response(&envelope) {
        return Err(DaemonError::Network {
            address: sinomo_network::CLUSTER_RELAY_ROUTE.to_string(),
            detail: "streamed raw relay responses require socket streaming dispatch".to_string(),
        });
    }
    let runtime = runtime.ok_or_else(|| DaemonError::Network {
        address: sinomo_network::CLUSTER_RELAY_ROUTE.to_string(),
        detail: "cluster relay dispatch requires the daemon-owned Tokio runtime".to_string(),
    })?;
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state_paths = cluster_paths.clone();
    let state_path = state_paths.root.display().to_string();
    let state = runtime
        .spawn_blocking(move || sinomo_network::read_cluster_state_cached(&state_paths))
        .await
        .map_err(|source| DaemonError::Io {
            path: state_path,
            detail: format!("cluster relay state read task failed: {source}"),
        })??;
    let local_node_id = relay_local_node_id(paths, &state);
    let mut host = DaemonClusterRelayAsyncHost {
        paths,
        runtime: runtime.clone(),
    };
    sinomo_network::handle_cluster_relay_async_with_host(
        runtime,
        &cluster_paths,
        &state,
        &local_node_id,
        envelope,
        sinomo_network::ClusterRelayOptions::new(
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(
                DAEMON_CLUSTER_RELAY_CONNECT_TIMEOUT,
            ),
        ),
        &mut host,
    )
    .await
    .map_err(Into::into)
}

pub(crate) async fn handle_daemon_cluster_relay_stream_forward_on_runtime(
    paths: &DaemonPaths,
    envelope: sinomo_network::RelayEnvelope,
    runtime: &tokio::runtime::Handle,
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state =
        sinomo_network::read_cluster_state_without_kv(&cluster_paths).map_err(DaemonError::from)?;
    let local_node_id = state.local_node_id.clone();
    let mut host = DaemonClusterRelayStreamForwardHost {
        paths,
        runtime: runtime.clone(),
        stream,
        stream_address,
    };
    let outcome = sinomo_network::handle_cluster_relay_stream_forward_async_with_host(
        runtime,
        &cluster_paths,
        &state,
        &local_node_id,
        envelope,
        sinomo_network::ClusterRelayOptions::new(
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(
                DAEMON_CLUSTER_RELAY_CONNECT_TIMEOUT,
            ),
        ),
        &mut host,
    )
    .await
    .map_err(DaemonError::from)?;
    match outcome {
        sinomo_network::ClusterRelayStreamForwardOutcome::Forwarded => Ok(()),
        sinomo_network::ClusterRelayStreamForwardOutcome::ErrorResponse(response) => stream
            .write_rpc_response(
                stream_address,
                &response,
                sinomo_network::ObjectFrameIoOptions::none(),
            )
            .await
            .map_err(DaemonError::from),
        sinomo_network::ClusterRelayStreamForwardOutcome::StreamStartedThenFailed(error) => {
            let _ = stream.shutdown_write(stream_address).await;
            Err(DaemonError::from(error))
        }
    }
}

struct DaemonClusterRelayAsyncHost<'a> {
    paths: &'a DaemonPaths,
    runtime: tokio::runtime::Handle,
}

impl sinomo_network::ClusterRelayAsyncHost for DaemonClusterRelayAsyncHost<'_> {
    fn try_deliver_local_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        envelope: &'a sinomo_network::RelayEnvelope,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterRelayFuture<'a, Option<sinomo_network::ObjectRpcResponse>> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let state = state.clone();
        let target_node_id = envelope.target_node_id.clone();
        let envelope = envelope.clone();
        Box::pin(async move {
            if target_node_id == relay_local_node_id(&paths, &state) {
                return crate::control::dispatch_daemon_control_request_in_process_async(
                    paths,
                    runtime,
                    envelope.inner,
                )
                .await
                .map(Some)
                .map_err(|error| daemon_host_network_error("cluster relay local delivery", error));
            }
            let delivery = tokio::task::spawn_blocking(move || {
                relay_local_delivery_target(&paths, &state, &target_node_id)
            })
            .await
            .map_err(|source| sinomo_network::NetworkError::Io {
                operation: "cluster relay local delivery".to_string(),
                detail: format!("local delivery target task failed: {source}"),
            })?
            .map_err(|error| daemon_host_network_error("cluster relay local delivery", error))?;
            let Some(delivery) = delivery else {
                return Ok(None);
            };
            match delivery {
                RelayLocalDeliveryTarget::CurrentProcess(paths) => {
                    crate::control::dispatch_daemon_control_request_in_process_async(
                        paths,
                        runtime,
                        envelope.inner,
                    )
                    .await
                    .map(Some)
                    .map_err(|error| {
                        daemon_host_network_error("cluster relay local delivery", error)
                    })
                }
                RelayLocalDeliveryTarget::LoopbackAddress(local_address) => {
                    let inner = &envelope.inner;
                    sinomo_network::send_daemon_rpc_to_address_async(
                        &local_address,
                        inner.auth.as_deref(),
                        inner,
                        timeouts,
                    )
                    .await
                    .map(Some)
                }
            }
        })
    }

    fn send_relay_request_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a sinomo_network::ObjectRpcRequest,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterRelayFuture<'a, sinomo_network::ObjectRpcResponse> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let endpoint = endpoint.clone();
        let request = request.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result =
                send_cluster_relay_request_to_next_hop_async(&endpoint, &request, timeouts)
                    .await
                    .map_err(|error| daemon_host_network_error("cluster relay", error));
            match &result {
                Ok(_) => {
                    let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                    remember_relay_endpoint_command_connectivity_async(
                        paths,
                        runtime,
                        endpoint,
                        sinomo_network::ClusterConnectivityState::Healthy,
                        Some(latency_ms),
                    )
                    .await;
                }
                Err(sinomo_network::NetworkError::Io { .. }) => {
                    remember_relay_endpoint_command_connectivity_async(
                        paths,
                        runtime,
                        endpoint,
                        sinomo_network::ClusterConnectivityState::Degraded,
                        None,
                    )
                    .await;
                }
                Err(sinomo_network::NetworkError::AuthDenied) => {
                    remember_relay_endpoint_command_connectivity_without_failure_grace_async(
                        paths,
                        runtime,
                        endpoint,
                        sinomo_network::ClusterConnectivityState::Degraded,
                        None,
                    )
                    .await;
                }
                Err(_) => {}
            }
            result
        })
    }
}

struct DaemonClusterRelayStreamForwardHost<'a> {
    paths: &'a DaemonPaths,
    runtime: tokio::runtime::Handle,
    stream: &'a mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &'a str,
}

impl sinomo_network::ClusterRelayStreamForwardAsyncHost
    for DaemonClusterRelayStreamForwardHost<'_>
{
    fn send_relay_stream_forward_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a sinomo_network::ObjectRpcRequest,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterRelayFuture<'a, sinomo_network::ClusterRelayStreamSendOutcome> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let endpoint = endpoint.clone();
        let request = request.clone();
        let stream = &mut *self.stream;
        let stream_address = self.stream_address;
        Box::pin(async move {
            let started = Instant::now();
            let result = send_cluster_relay_request_to_next_hop_streaming_async(
                &endpoint,
                request,
                timeouts,
                stream,
                stream_address,
            )
            .await;
            match result {
                Ok(ClusterRelayStreamForwardResult::Forwarded) => {
                    let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                    remember_relay_endpoint_command_connectivity_async(
                        paths,
                        runtime,
                        endpoint,
                        sinomo_network::ClusterConnectivityState::Healthy,
                        Some(latency_ms),
                    )
                    .await;
                    Ok(sinomo_network::ClusterRelayStreamSendOutcome::Forwarded)
                }
                Ok(ClusterRelayStreamForwardResult::RoutingError(response)) => Ok(
                    sinomo_network::ClusterRelayStreamSendOutcome::RoutingError(response),
                ),
                Ok(ClusterRelayStreamForwardResult::StreamStartedThenFailed(error)) => Ok(
                    sinomo_network::ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                        daemon_host_network_error("cluster relay stream", error),
                    ),
                ),
                Err(error) => {
                    remember_relay_endpoint_command_connectivity_async(
                        paths,
                        runtime,
                        endpoint,
                        sinomo_network::ClusterConnectivityState::Degraded,
                        None,
                    )
                    .await;
                    Err(daemon_host_network_error("cluster relay stream", error))
                }
            }
        })
    }
}

async fn remember_relay_endpoint_command_connectivity_async(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    endpoint: ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
    latency_ms: Option<u64>,
) {
    let _ = runtime
        .spawn_blocking(move || {
            let Ok(cluster_paths) = cluster_paths_from_daemon_paths(&paths) else {
                return;
            };
            let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
                return;
            };
            remember_cluster_endpoint_command_connectivity(
                &cluster_paths,
                &state,
                &endpoint,
                connectivity_state,
                latency_ms,
                None,
            );
        })
        .await;
}

async fn remember_relay_endpoint_command_connectivity_without_failure_grace_async(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    endpoint: ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
    latency_ms: Option<u64>,
) {
    let _ = runtime
        .spawn_blocking(move || {
            let Ok(cluster_paths) = cluster_paths_from_daemon_paths(&paths) else {
                return;
            };
            let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
                return;
            };
            remember_cluster_endpoint_command_connectivity_without_failure_grace(
                &cluster_paths,
                &state,
                &endpoint,
                connectivity_state,
                latency_ms,
                None,
            );
        })
        .await;
}

async fn send_cluster_relay_request_to_next_hop_async(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_network::ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonSignedEndpointRpcAsyncHost::direct_only(&runtime);
    let mut client = sinomo_network::ClusterObjectRpcClient::endpoint_only(&mut host);
    client
        .call_endpoint(
            endpoint,
            request,
            sinomo_network::ClusterObjectRpcOptions::new(timeouts),
        )
        .await
        .map_err(DaemonError::from)
}

fn relay_envelope_inner_expects_streamed_response(
    envelope: &sinomo_network::RelayEnvelope,
) -> bool {
    sinomo_network::object_rpc_request_stream_mode(&envelope.inner).expects_raw_streamed_response()
}

enum ClusterRelayStreamForwardResult {
    Forwarded,
    RoutingError(sinomo_network::ObjectRpcResponse),
    StreamStartedThenFailed(DaemonError),
}

async fn send_cluster_relay_request_to_next_hop_streaming_async(
    endpoint: &ClusterEndpointRecord,
    request: sinomo_network::ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    stream: &mut sinomo_network::AsyncDaemonControlStream,
    stream_address: &str,
) -> Result<ClusterRelayStreamForwardResult, DaemonError> {
    if !matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) {
        return Err(DaemonError::Network {
            address: format!("cluster relay {}", endpoint.address),
            detail: format!(
                "streaming relay forwarding supports TCP daemon-control endpoints, got {:?}",
                endpoint.transport
            ),
        });
    }

    let (mut request, request_auth) = tokio::task::spawn_blocking(move || {
        let request_auth = crate::sign_daemon_rpc_request_for_local_node(&request)?;
        Ok::<_, DaemonError>((request, request_auth))
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: format!("cluster relay {}", endpoint.address),
        detail: format!("cluster relay signing task failed: {source}"),
    })??;
    request.auth = Some(request_auth);

    let frame_options =
        sinomo_network::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    let mut downstream =
        sinomo_network::AsyncDaemonControlStream::connect_with_options(&endpoint.address, timeouts)
            .await
            .map_err(DaemonError::from)?;
    downstream
        .write_request(&endpoint.address, &request, frame_options)
        .await
        .map_err(DaemonError::from)?;
    let response = downstream
        .read_rpc_response(
            &endpoint.address,
            crate::control::DAEMON_COMMAND_MAX_BYTES,
            frame_options,
        )
        .await
        .map_err(DaemonError::from)?;
    if sinomo_network::relay_response_is_routing_error(&response) {
        return Ok(ClusterRelayStreamForwardResult::RoutingError(response));
    }

    stream
        .write_rpc_response(stream_address, &response, frame_options)
        .await
        .map_err(DaemonError::from)?;
    if sinomo_network::object_rpc_response_stream_mode(&response)
        == sinomo_network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
    {
        if let Err(error) = downstream
            .forward_raw_byte_stream_chunks_to(
                &endpoint.address,
                stream,
                stream_address,
                sinomo_network::RawByteStreamChunkOptions {
                    chunk_bytes: sinomo_network::OBJECT_STREAM_MAX_CHUNK_BYTES,
                    max_total_bytes: u64::try_from(crate::control::DAEMON_COMMAND_MAX_BYTES).ok(),
                    idle_timeout: timeouts.idle_timeout,
                },
            )
            .await
        {
            return Ok(ClusterRelayStreamForwardResult::StreamStartedThenFailed(
                DaemonError::Network {
                    address: format!("cluster relay {}", endpoint.address),
                    detail: format!(
                        "stream chunk forward failed after response route={} metadata={:?}: {}",
                        response.route, response.metadata, error
                    ),
                },
            ));
        }
    }
    Ok(ClusterRelayStreamForwardResult::Forwarded)
}

fn relay_local_node_id(_paths: &DaemonPaths, state: &ClusterBootstrapState) -> NodeId {
    state.local_node_id.clone()
}

enum RelayLocalDeliveryTarget {
    CurrentProcess(DaemonPaths),
    LoopbackAddress(String),
}

fn relay_local_delivery_target(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> Result<Option<RelayLocalDeliveryTarget>, DaemonError> {
    if *target_node_id == relay_local_node_id(paths, state)
        || (daemon_paths_are_shadow(paths)
            && *target_node_id == sinomo_node::shadow_daemon_node_id(&state.local_node_id))
    {
        return Ok(Some(RelayLocalDeliveryTarget::CurrentProcess(
            paths.clone(),
        )));
    }

    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let target_paths = if *target_node_id == state.local_node_id {
        Some(DaemonPaths::with_root(data_dir.join("daemon")))
    } else if *target_node_id == sinomo_node::shadow_daemon_node_id(&state.local_node_id) {
        Some(DaemonPaths::with_root(data_dir.join("shadow-daemon")))
    } else {
        None
    };
    let Some(target_paths) = target_paths else {
        return Ok(None);
    };
    let status = read_daemon_status(&target_paths)?;
    let port = daemon_options_from_status(&status).port;
    Ok(Some(RelayLocalDeliveryTarget::LoopbackAddress(
        daemon_connect_address("127.0.0.1", port),
    )))
}

fn daemon_host_network_error(operation: &str, error: DaemonError) -> sinomo_network::NetworkError {
    match error {
        DaemonError::AuthDenied => sinomo_network::NetworkError::AuthDenied,
        DaemonError::AuthExpired => sinomo_network::NetworkError::AuthExpired,
        DaemonError::ClusterReadOnly { detail } => {
            sinomo_network::NetworkError::ClusterReadOnly { detail }
        }
        DaemonError::Network { address, detail } => sinomo_network::NetworkError::Io {
            operation: format!("{operation} {address}"),
            detail,
        },
        other => sinomo_network::NetworkError::Io {
            operation: operation.to_string(),
            detail: other.to_string(),
        },
    }
}

fn cluster_control_plane_timeouts() -> sinomo_network::ClusterControlPlaneTimeouts {
    sinomo_network::ClusterControlPlaneTimeouts::default_daemon_transport()
}

#[cfg(test)]
fn cluster_raft_append_replication_options(
    timeout: Duration,
) -> sinomo_network::ClusterRaftAppendReplicationOptions {
    cluster_control_plane_timeouts().raft_append_replication_options(timeout)
}

fn cluster_raft_committed_fanout_options() -> sinomo_network::ClusterRaftCommittedFanoutOptions {
    cluster_control_plane_timeouts().committed_fanout_options()
}

fn cluster_voter_reachability_options() -> sinomo_network::ClusterVoterReachabilityOptions {
    cluster_control_plane_timeouts().voter_reachability_options()
}

fn cluster_kv_txn_quorum_options(
    allow_forwarding: bool,
) -> sinomo_network::ClusterKvTxnQuorumOptions {
    cluster_control_plane_timeouts().kv_txn_quorum_options(allow_forwarding)
}

#[cfg(test)]
fn cluster_raft_append_attempt_timeout(
    endpoint: &ClusterEndpointRecord,
    remaining: Duration,
    request: &ClusterRaftAppendRequest,
) -> Duration {
    cluster_raft_append_replication_options(remaining)
        .append_attempt_timeout(endpoint, remaining, request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecordingRelayStreamForwardMode {
        Forward,
        RoutingError,
    }

    struct RecordingRelayStreamForwardHost {
        mode: RecordingRelayStreamForwardMode,
        sent: Vec<(
            ClusterEndpointRecord,
            sinomo_network::RelayEnvelope,
            sinomo_network::DaemonRpcTimeouts,
        )>,
    }

    impl RecordingRelayStreamForwardHost {
        fn forwarding() -> Self {
            Self {
                mode: RecordingRelayStreamForwardMode::Forward,
                sent: Vec::new(),
            }
        }

        fn routing_error() -> Self {
            Self {
                mode: RecordingRelayStreamForwardMode::RoutingError,
                sent: Vec::new(),
            }
        }
    }

    impl sinomo_network::ClusterRelayStreamForwardAsyncHost for RecordingRelayStreamForwardHost {
        fn send_relay_stream_forward_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a sinomo_network::ObjectRpcRequest,
            timeouts: sinomo_network::DaemonRpcTimeouts,
        ) -> sinomo_network::ClusterRelayFuture<'a, sinomo_network::ClusterRelayStreamSendOutcome>
        {
            let endpoint = endpoint.clone();
            let request = request.clone();
            let mode = self.mode;
            let sent = &mut self.sent;
            Box::pin(async move {
                let envelope = sinomo_network::RelayEnvelope::from_request(&request)?;
                sent.push((endpoint, envelope, timeouts));
                match mode {
                    RecordingRelayStreamForwardMode::Forward => {
                        Ok(sinomo_network::ClusterRelayStreamSendOutcome::Forwarded)
                    }
                    RecordingRelayStreamForwardMode::RoutingError => {
                        Ok(sinomo_network::ClusterRelayStreamSendOutcome::RoutingError(
                            sinomo_network::ObjectRpcResponse::encode_error(
                                sinomo_network::CLUSTER_RELAY_ROUTE,
                                "test routing miss",
                            )?,
                        ))
                    }
                }
            })
        }
    }

    fn append_cluster_raft_command(
        paths: &ClusterPaths,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        paths
            .kv_paths()
            .raft_store()
            .append_command_and_publish(
                node_id,
                command,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
            )
            .map_err(Into::into)
    }

    fn seed_cluster_node_status_to_kv(
        paths: &ClusterPaths,
        status: ClusterNodeStatusRecord,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        let state = sinomo_network::read_cluster_state_without_kv(paths)?;
        append_cluster_raft_command(
            paths,
            &state.local_node_id,
            KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::put_serde(
                    sinomo_node::cluster_node_status_key_for_record(&status),
                    &status,
                    None,
                )?],
                failure: Vec::new(),
            }),
        )
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
            block_on_cluster_network_future_on_owner_runtime(owner_runtime.handle(), async {
                Ok::<_, sinomo_network::NetworkError>(())
            })
            .expect_err("cluster bridge must not spawn a helper thread")
        });

        match error {
            sinomo_network::NetworkError::Io { operation, detail } => {
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
            sinomo_network::NetworkError::AuthDenied
        ));
        assert!(matches!(
            daemon_host_network_error(
                "cluster relay",
                DaemonError::ClusterReadOnly {
                    detail: "quorum unavailable".to_string(),
                },
            ),
            sinomo_network::NetworkError::ClusterReadOnly { .. }
        ));

        match daemon_host_network_error(
            "cluster relay",
            DaemonError::Network {
                address: "node2:21353".to_string(),
                detail: "timed out".to_string(),
            },
        ) {
            sinomo_network::NetworkError::Io { operation, detail } => {
                assert_eq!(operation, "cluster relay node2:21353");
                assert_eq!(detail, "timed out");
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    fn unique_cluster_test_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(name)
    }

    fn run_cluster_network_maintenance_tick_for_test(
        maintenance: &mut ClusterNetworkMaintenance,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Option<ClusterNetworkMaintenancePendingWork>, DaemonError> {
        runtime.block_on(maintenance.tick_async(paths, options, runtime.handle().clone()))
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
        let mut state = sinomo_network::prepare_pending_cluster_join_state(
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

    #[test]
    fn published_cache_insert_is_bounded() {
        let mut cache = BTreeMap::new();
        insert_bounded_published_cache(&mut cache, "a".to_string(), 1, 2);
        insert_bounded_published_cache(&mut cache, "b".to_string(), 2, 2);
        insert_bounded_published_cache(&mut cache, "c".to_string(), 3, 2);

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key("a"));
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
    }

    #[test]
    fn relay_local_delivery_address_routes_between_colocated_main_and_shadow() {
        let root = unique_cluster_test_root("relay-local-main-shadow");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_local_main_shadow").expect("cluster id");
        let main_node_id =
            NodeId::from_legacy_named("node_relay_local_main_shadow").expect("main node");
        let shadow_node_id = shadow_daemon_node_id(&main_node_id);
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay local main shadow",
            main_node_id.clone(),
            "local",
            "pk-main",
            now,
        );

        let main_paths = DaemonPaths::new(&data_dir);
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        main_paths.ensure().expect("main daemon paths");
        shadow_paths.ensure().expect("shadow daemon paths");
        let main_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32153,
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32154,
        };
        crate::write_daemon_status(
            &main_paths,
            &crate::DaemonStatus::new(&main_options, &main_paths, None),
        )
        .expect("main status");
        crate::write_daemon_status(
            &shadow_paths,
            &crate::DaemonStatus::new(&shadow_options, &shadow_paths, None),
        )
        .expect("shadow status");

        match relay_local_delivery_target(&main_paths, &state, &main_node_id).expect("main to main")
        {
            Some(RelayLocalDeliveryTarget::CurrentProcess(paths)) => {
                assert_eq!(paths.root, main_paths.root)
            }
            _ => panic!("main target should dispatch in-process"),
        }
        match relay_local_delivery_target(&main_paths, &state, &shadow_node_id)
            .expect("main to shadow")
        {
            Some(RelayLocalDeliveryTarget::LoopbackAddress(address)) => {
                assert_eq!(address, "127.0.0.1:32154")
            }
            _ => panic!("shadow target from main should use sibling loopback"),
        }
        match relay_local_delivery_target(&shadow_paths, &state, &shadow_node_id)
            .expect("shadow to shadow")
        {
            Some(RelayLocalDeliveryTarget::CurrentProcess(paths)) => {
                assert_eq!(paths.root, shadow_paths.root)
            }
            _ => panic!("shadow target should dispatch in-process"),
        }
        match relay_local_delivery_target(&shadow_paths, &state, &main_node_id)
            .expect("shadow to main")
        {
            Some(RelayLocalDeliveryTarget::CurrentProcess(paths)) => {
                assert_eq!(paths.root, shadow_paths.root)
            }
            _ => panic!("main target from shadow should dispatch in-process"),
        }
        assert!(
            relay_local_delivery_target(
                &main_paths,
                &state,
                &NodeId::from_legacy_named("node_relay_local_remote").expect("remote")
            )
            .expect("remote target")
            .is_none()
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
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_154,
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
        sinomo_network::ClusterMemberMutationHost::prepare_cluster_state_for_kv_sync(
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
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 32_154,
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
        sinomo_network::write_cluster_state(&cluster_paths, &pending).expect("write pending");
        let mut status =
            crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, None);
        write_daemon_status(&daemon_paths, &status).expect("write status");

        ensure_daemon_status_cluster(&daemon_paths, &mut status)
            .expect("ensure daemon status cluster");

        assert!(
            status.cluster.is_none(),
            "pending join state must not be advertised as this daemon's live cluster"
        );
        let stored =
            sinomo_network::read_cluster_state_without_kv(&cluster_paths).expect("read stored");
        assert_eq!(stored.cluster_id, pending.cluster_id);
        assert!(
            sinomo_network::cluster_state_needs_join_registration(&stored),
            "pending join state should remain pending and owned by the join command"
        );
    }

    struct TestClusterJoinRegistrationHost;

    impl sinomo_network::ClusterMembershipRpcForwardHost for TestClusterJoinRegistrationHost {
        fn send_membership_rpc_to_node(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &sinomo_network::ObjectRpcRequest,
        ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
            panic!("forwarded acceptance recorder should not send membership node RPCs")
        }
    }

    impl sinomo_network::ClusterJoinRegistrationHost for TestClusterJoinRegistrationHost {
        fn commit_join_registration_state(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
            panic!("forwarded acceptance recorder should not commit join state")
        }

        fn remember_successful_join_endpoint(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
            endpoint: &ClusterEndpointRecord,
        ) -> Result<(), sinomo_network::NetworkError> {
            remember_cluster_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                sinomo_network::ClusterConnectivityState::Healthy,
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
        sinomo_node::apply_cluster_raft_role(
            bridge_state
                .members
                .iter_mut()
                .find(|member| member.node_id == bridge)
                .expect("bridge member"),
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_network::write_cluster_state(&cluster_paths, &bridge_state)
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
        sinomo_node::apply_cluster_raft_role(&mut bridge_member, ClusterMemberRole::Learner, now);
        sinomo_node::upsert_member_record(&mut accepted_state, bridge_member);
        let mut joined_member = ClusterMemberRecord::local_voter(
            accepted_state.cluster_id.clone(),
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        sinomo_node::apply_cluster_raft_role(&mut joined_member, ClusterMemberRole::Learner, now);
        sinomo_node::upsert_member_record(&mut accepted_state, joined_member.clone());
        let response = ClusterJoinRegistrationResponse {
            accepted: true,
            detail: "join registered".to_string(),
            state: accepted_state,
            kv_snapshot: None,
        };

        sinomo_network::record_forwarded_join_registration_acceptance_with_host(
            &cluster_paths,
            &bridge_state,
            &[joined_member],
            &response,
            &mut TestClusterJoinRegistrationHost,
        )
        .expect("record forwarded join");

        let stored =
            sinomo_network::read_cluster_state_without_kv(&cluster_paths).expect("read stored");
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
        let pending_cluster = ClusterId::new(sinomo_network::PENDING_INVITE_CLUSTER_ID.to_string())
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
        sinomo_node::apply_cluster_raft_role(&mut joined_member, ClusterMemberRole::Learner, now);
        sinomo_node::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_node::upsert_member_record(
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
                sinomo_node::cluster_identity_key(),
                &sinomo_node::ClusterIdentityRecord::from(&accepted_state),
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
            cluster_id: ClusterId::new(sinomo_network::PENDING_INVITE_CLUSTER_ID.to_string())
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

        let recovered = sinomo_network::accepted_cluster_join_state_from_kv_snapshot(
            &pending,
            &invite,
            &store.snapshot(),
        )
        .expect("read snapshot")
        .expect("accepted state");
        assert_eq!(recovered.cluster_id, actual_cluster);
        let mut reconciled = pending;
        sinomo_network::merge_cluster_join_registration(&mut reconciled, recovered);
        assert_eq!(reconciled.cluster_id, actual_cluster);
        assert!(
            !sinomo_network::cluster_state_needs_join_registration(&reconciled),
            "snapshot reconciliation should leave the local node accepted"
        );
    }

    #[test]
    fn identityless_invite_requires_snapshot_for_accepted_join_response() {
        let now = OffsetDateTime::now_utc();
        let actual_cluster =
            ClusterId::from_legacy_named("cluster_daemon_identityless_response_requires_snapshot")
                .expect("cluster");
        let pending_cluster = ClusterId::new(sinomo_network::PENDING_INVITE_CLUSTER_ID.to_string())
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
        sinomo_node::apply_cluster_raft_role(&mut joined_member, ClusterMemberRole::Learner, now);
        sinomo_node::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_node::upsert_member_record(
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
            cluster_id: ClusterId::new(sinomo_network::PENDING_INVITE_CLUSTER_ID.to_string())
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
        };
        let endpoint = sinomo_network::daemon_lan_endpoint(&voter, "node1:21353".to_string(), 0);
        let mut reconciled = pending.clone();

        let reconciliation = sinomo_network::reconcile_accepted_cluster_join_response(
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
        assert!(sinomo_network::cluster_state_needs_join_registration(
            &reconciled
        ));
    }

    #[test]
    fn published_connectivity_cache_total_records_is_bounded() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_connectivity_total_cache")
            .expect("cluster");
        let record_a = ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: NodeId::from_legacy_named("node_daemon_total_cache_source_a")
                .expect("source a"),
            target_node_id: NodeId::from_legacy_named("node_daemon_total_cache_target_a")
                .expect("target a"),
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_total_cache_a",
            )
            .expect("endpoint a"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.10:21353".to_string(),
            state: sinomo_network::ClusterConnectivityState::Healthy,
            latency_ms: Some(4),
            consecutive_failures: 0,
            last_success_at: Some("ok".to_string()),
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(300),
        };
        let record_b = ClusterConnectivityRecord {
            cluster_id,
            source_node_id: NodeId::from_legacy_named("node_daemon_total_cache_source_b")
                .expect("source b"),
            target_node_id: NodeId::from_legacy_named("node_daemon_total_cache_target_b")
                .expect("target b"),
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_total_cache_b",
            )
            .expect("endpoint b"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.11:21353".to_string(),
            state: sinomo_network::ClusterConnectivityState::Healthy,
            latency_ms: Some(5),
            consecutive_failures: 0,
            last_success_at: Some("ok".to_string()),
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(300),
        };
        let source_a = "source-a".to_string();
        let source_b = "source-b".to_string();
        let mut source_a_records = BTreeMap::new();
        for index in 0..PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS {
            source_a_records.insert(format!("a-{index:04}"), record_a.clone());
        }
        let mut source_b_records = BTreeMap::new();
        source_b_records.insert("b".to_string(), record_b);

        let mut cache = BTreeMap::new();
        cache.insert(
            source_a.clone(),
            PublishedConnectivitySourceCache {
                records: source_a_records,
            },
        );
        cache.insert(
            source_b.clone(),
            PublishedConnectivitySourceCache {
                records: source_b_records,
            },
        );

        trim_published_connectivity_cache_records(&mut cache, &source_b);

        assert!(
            published_connectivity_cache_record_count(&cache)
                <= PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS,
            "published connectivity cache must enforce a process-wide record cap"
        );
        assert!(
            cache
                .get(&source_b)
                .and_then(|source| source.records.get("b"))
                .is_some(),
            "the source that was just published should be preserved when trimming"
        );
        assert!(
            !cache.contains_key(&source_a),
            "older non-protected sources should be evicted first"
        );
    }

    #[test]
    fn publish_failure_backoff_grows_exponentially_and_resets() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut maintenance = runtime.block_on(ClusterNetworkMaintenance::with_idle_stats(
            Arc::new(IdleStatsAtomics::new()),
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
    fn maintenance_failure_backoff_retries_after_configured_delay() {
        let now = Instant::now();
        let last_publish = sinomo_network::cluster_network_maintenance_last_timestamp_for_next_due(
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
            !sinomo_network::cluster_network_maintenance_interval_due(
                before_backoff,
                last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "failed publishes should not retry before the configured backoff"
        );
        assert!(
            sinomo_network::cluster_network_maintenance_interval_due(
                after_backoff,
                last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "failed publishes should retry at the configured backoff, not backoff plus the publish interval"
        );
        assert!(
            !sinomo_network::cluster_network_maintenance_interval_due(
                now,
                after_backoff,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "future deferred timestamps should be safe and not immediately due"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_skips_discovery_without_socket() {
        let now = Instant::now();
        let maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now,
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
            Duration::from_secs(40),
            "without an mDNS socket the next wake should be driven by publish/probe, not discovery"
        );
    }

    #[test]
    fn cluster_network_maintenance_next_due_uses_role_publish_cadence() {
        let now = Instant::now();
        let maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Mobile,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now,
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
            sinomo_network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL - Duration::from_secs(20),
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
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Mobile,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now,
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
        let maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now,
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
    fn cluster_network_maintenance_pending_join_keeps_convergence_poll_active() {
        let root = unique_cluster_test_root("sinomo-daemon-pending-join-convergence-poll");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let pending = pending_join_state_for_test(
            &cluster_paths,
            ClusterId::from_legacy_named("cluster_pending_join_convergence").expect("cluster"),
            "pending join convergence".to_string(),
            NodeId::from_legacy_named("node_pending_join_convergence").expect("node"),
            "pending".to_string(),
            OffsetDateTime::now_utc(),
        );
        sinomo_network::write_cluster_state(&cluster_paths, &pending).expect("write pending state");

        let now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: now,
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
            !maintenance.connectivity_converged,
            "pending joins must keep the fast poll active so accepted joins publish before the idle gate"
        );
        assert!(
            maintenance.next_due_duration(Instant::now())
                <= DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL,
            "pending join maintenance should wake on the convergence cadence, not the full publish interval"
        );
    }

    #[test]
    fn cluster_network_maintenance_policy_throttles_mobile_and_sensor_roles() {
        assert_eq!(
            sinomo_network::cluster_network_publish_interval_for_role(
                &ClusterNodeRole::Workstation
            ),
            DAEMON_CLUSTER_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Workstation),
            DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_discovery_interval_for_role(
                &ClusterNodeRole::Workstation
            ),
            DAEMON_CLUSTER_DISCOVERY_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Workstation
            ),
            sinomo_network::CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK
        );
        assert_eq!(
            sinomo_network::cluster_network_publish_interval_for_role(&ClusterNodeRole::Mobile),
            sinomo_network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Mobile),
            sinomo_network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_discovery_interval_for_role(&ClusterNodeRole::Mobile),
            sinomo_network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Mobile
            ),
            sinomo_network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK
        );
        assert_eq!(
            sinomo_network::cluster_network_publish_interval_for_role(&ClusterNodeRole::Sensor),
            sinomo_network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Sensor),
            sinomo_network::CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_network_discovery_interval_for_role(&ClusterNodeRole::Sensor),
            sinomo_network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
        assert_eq!(
            sinomo_network::cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                &ClusterNodeRole::Sensor
            ),
            sinomo_network::CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK
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
            sinomo_network::read_cluster_state_without_kv(&paths).expect("read local state");
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write mobile state");
        let cluster = daemon_cluster_status(&state, crate::DaemonClusterProcess::Main)
            .expect("cluster status");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        assert_eq!(
            daemon_cluster_publish_interval(&daemon_paths),
            sinomo_network::CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL
        );

        state.members[0].node_role = ClusterNodeRole::Sensor;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write sensor state");
        assert_eq!(
            daemon_cluster_publish_interval(&daemon_paths),
            sinomo_network::CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL
        );
    }

    #[test]
    fn stable_discovery_announcements_back_off_but_state_changes_force_send() {
        assert!(
            sinomo_network::cluster_network_stable_discovery_announcement_due(1, false),
            "fresh daemons should announce immediately"
        );
        assert!(
            !sinomo_network::cluster_network_stable_discovery_announcement_due(3, false),
            "unchanged discovery ticks should skip multicast announcements between stable sends"
        );
        assert!(
            sinomo_network::cluster_network_stable_discovery_announcement_due(
                1 + sinomo_network::CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE,
                false,
            ),
            "stable announcements should repeat on the configured low-rate cadence"
        );
        assert!(
            sinomo_network::cluster_network_stable_discovery_announcement_due(3, true),
            "discovery or local endpoint changes must force an immediate announcement"
        );
    }

    #[test]
    fn daemon_paths_have_settled_solo_cluster_tracks_remote_members() {
        let root = unique_cluster_test_root("sinomo-daemon-solo-cluster-detect");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));

        assert!(
            !daemon_paths_have_settled_solo_cluster(&daemon_paths).expect("absent cluster"),
            "absent cluster state is not a settled solo cluster"
        );

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_solo_detect").expect("cluster");
        let local = NodeId::from_legacy_named("node_solo_detect_main").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "solo detect",
            local,
            "local",
            "pk-local",
            now,
        );
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write solo state");
        assert!(
            daemon_paths_have_settled_solo_cluster(&daemon_paths).expect("solo cluster"),
            "local-only cluster state should be recognized as settled solo"
        );

        let remote = NodeId::from_legacy_named("node_solo_detect_remote").expect("remote");
        state.members.push(ClusterMemberRecord::local_voter(
            cluster_id,
            remote,
            "remote",
            "pk-remote",
            now,
        ));
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write remote state");
        assert!(
            !daemon_paths_have_settled_solo_cluster(&daemon_paths).expect("remote cluster"),
            "remote members should disable solo idle suppression"
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let after_ticks = Instant::now();
        assert!(
            !sinomo_network::cluster_network_maintenance_interval_due(
                after_ticks,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "absent cluster state should advance publish timer"
        );
        assert!(
            !sinomo_network::cluster_network_maintenance_interval_due(
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "127.0.0.1:1".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: false,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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

        assert!(maintenance.connectivity_converged);
        let records = sinomo_network::read_local_cluster_connectivity_records(&cluster_paths)
            .expect("connectivity records");
        assert!(
            records
                .iter()
                .any(|record| record.source_node_id == local_node
                    && record.target_node_id == remote_node),
            "pending async probe should record the remote endpoint result"
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
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
            .expect("sync state to local raft");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_network::cluster_local_node_role(&state),
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let statuses =
            sinomo_network::read_cluster_node_statuses_from_kv(&cluster_paths).expect("statuses");
        assert!(
            statuses.iter().any(|status| status.node_id == local_node
                && status.availability == ClusterNodeAvailability::Online),
            "pending async status refresh should publish this daemon's node status"
        );
    }

    #[test]
    fn cluster_network_maintenance_returns_pending_async_member_publish() {
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
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
            .expect("sync state to local raft");
        let cluster =
            daemon_cluster_status(&state, crate::DaemonClusterProcess::Main).expect("cluster");
        write_daemon_status(
            &daemon_paths,
            &crate::DaemonStatus::new(&DaemonRunOptions::default(), &daemon_paths, Some(cluster)),
        )
        .expect("write daemon status");

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_network::cluster_local_node_role(&state),
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,
            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        .expect("runtime publish tick should return pending member publish");
        assert_eq!(pending.member_publishes.len(), 1);
        assert!(!pending.member_publishes[0].force_kv_snapshot_sync);
        assert!(pending.connectivity_probe.is_none());

        runtime.block_on(maintenance.run_pending_work_async(pending, runtime.handle().clone()));

        assert_eq!(idle_stats.snapshot().kv_publish_member, 1);
        let members =
            sinomo_network::read_cluster_members_from_kv(&cluster_paths).expect("members from kv");
        assert!(
            members.iter().any(|member| member.node_id == local_node),
            "pending async member publish should publish the local member"
        );
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
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: true,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
            !cluster_paths.kv_snapshot.exists(),
            "settled solo maintenance must not create or rewrite the KV snapshot"
        );
        let after_tick = Instant::now();
        assert!(
            !sinomo_network::cluster_network_maintenance_interval_due(
                after_tick,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "settled solo tick should advance publish timer"
        );
        assert!(
            !sinomo_network::cluster_network_maintenance_interval_due(
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
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
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
            sinomo_network::apply_lan_discovery_advertisements(
                &mut state,
                &mut discovery_cache,
                vec![advertisement.clone()]
            ),
            "first discovery should add the remote endpoint"
        );
        assert_eq!(state.members[1].endpoints, vec![endpoint]);
        assert!(
            !sinomo_network::apply_lan_discovery_advertisements(
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
            sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let synced =
            sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
                .expect("sync cluster state");

        let authenticated = AuthenticatedDaemonCommand {
            route: sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE.to_string(),
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
            sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
            sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &ClusterMemberPublishRequest {
                members: vec![state.members[0].clone()],
                local_kv_revision: Some(synced.revision),
            },
        )
        .expect("member publish rpc request");
        let response = sinomo_network::handle_cluster_membership_rpc_request_with_hosts(
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
            sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
            sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &ClusterMemberPublishRequest {
                members: vec![state.members[0].clone()],
                local_kv_revision: None,
            },
        )
        .expect("member publish rpc request without revision");
        let response = sinomo_network::handle_cluster_membership_rpc_request_with_hosts(
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
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft");

        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let paths_for_holder = paths.clone();
        let local_for_holder = local.clone();
        let holder = std::thread::spawn(move || {
            let _raft_guard =
                sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths_for_holder.kv_paths())
                    .expect("raft operation lock");
            let _node = paths_for_holder
                .kv_paths()
                .raft_store()
                .open_node(&local_for_holder)
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

        let store = InProcessKvStore::restore(snapshot);
        assert_eq!(
            store.get("/test/local-lock-wait").expect("committed").value,
            b"ok"
        );
    }

    #[test]
    fn no_effect_cluster_kv_txn_with_quorum_does_not_append_raft_log_entry() {
        let root = unique_cluster_test_root("sinomo-daemon-no-effect-kv-no-append");
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
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
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
        .expect("no-effect txn should succeed without raft append");

        assert_eq!(
            snapshot.revision, committed.revision,
            "no-effect daemon txn must not advance the cluster revision"
        );
        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: committed.revision,
                responses: vec![sinomo_kv::KvOpResponse::Delete {
                    key: "/test/no-effect/missing".to_string(),
                    deleted: false,
                }],
            })
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after no-effect")
                .len(),
            wal_len_before,
            "no-effect daemon txn must not append to the raft WAL"
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
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft");
        daemon_locked_set_cluster_raft_leader(&paths, &local, 1).expect("set leader");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = DaemonClusterMemberMutationHost::new_on_runtime(runtime.handle());
        let snapshot = runtime
            .block_on(
                sinomo_network::ClusterMemberMutationAsyncHost::commit_cluster_kv_txn_with_quorum_async(
                    &mut host,
                    &paths,
                    &mut state,
                    KvTxn {
                        compares: Vec::new(),
                        success: vec![KvOp::Put {
                            key: "/test/async-local-leader".to_string(),
                            value: b"ok".to_vec(),
                            lease_id: None,
                        }],
                        failure: Vec::new(),
                    },
                ),
            )
            .expect("async local leader commit");

        let store = InProcessKvStore::restore(snapshot);
        assert_eq!(
            store
                .get("/test/async-local-leader")
                .expect("committed")
                .value,
            b"ok"
        );
    }

    #[test]
    fn stable_connectivity_refresh_txn_does_not_append_raft_log_entry() {
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
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote, "10.0.0.71:21353".to_string(), 10);
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote, "remote", "pk-remote", now);
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state");

        let seeded_record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("seeded".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let key = sinomo_network::cluster_connectivity_key(
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

        let refreshed_record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(11),
            0,
            Some("refreshed".to_string()),
            None,
            now + time::Duration::seconds(5),
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        assert!(
            sinomo_network::cluster_connectivity_publish_record_matches(
                &seeded_record,
                &refreshed_record,
                now
            ),
            "test record should differ only by non-durable connectivity observations"
        );
        let (snapshot, response) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(&key, &refreshed_record, None)
                        .expect("refresh connectivity op"),
                ],
                failure: Vec::new(),
            },
        )
        .expect("stable connectivity refresh should succeed without append");

        assert_eq!(
            snapshot.revision, seeded_snapshot.revision,
            "stable connectivity refreshes must not advance the cluster revision"
        );
        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: seeded_snapshot.revision,
                responses: vec![KvOpResponse::Put {
                    key,
                    revision: seeded_entry.modify_revision,
                }],
            })
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after semantic no-effect")
                .len(),
            wal_len_before,
            "stable connectivity refreshes must not append to the raft WAL"
        );
    }

    #[test]
    fn source_only_member_publish_txn_does_not_append_raft_log_entry() {
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
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        let member = &mut state.members[0];
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = vec![
            ClusterEndpointRecord {
                endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
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
            },
            ClusterEndpointRecord {
                endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                    "endpoint_daemon_source_static",
                )
                .expect("endpoint"),
                node_id: local.clone(),
                transport: ClusterTransportKind::Loopback,
                address: "127.0.0.1:21353".to_string(),
                source: sinomo_core::ClusterEndpointSource::Static,
                privacy: ClusterEndpointPrivacy::LocalOnly,
                priority: 0,
                last_observed_latency_ms: None,
            },
        ];
        state.members[0]
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state");

        let snapshot =
            sinomo_kv::read_cluster_kv_current_snapshot_for_node(&paths.kv_paths(), local.clone())
                .expect("read snapshot");
        let config_key = sinomo_kv::cluster_kv_raft_config_key();
        let config_entry = snapshot.get(&config_key).expect("config entry").clone();
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
        let endpoint_key = sinomo_network::cluster_endpoint_key(&local, &observed_endpoint);
        let endpoint_entry = snapshot.get(&endpoint_key).expect("endpoint entry").clone();
        let wal_path = paths.kv_paths().raft_wal_paths(&local).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before source no-effect")
            .len();

        let (snapshot_after, response) = commit_cluster_kv_txn_with_quorum_response(
            &paths,
            &mut state,
            KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: config_key,
                    value: config_entry.value,
                }],
                success: vec![
                    KvOp::put_serde(&member_key, &observed_member, None).expect("member put op"),
                    KvOp::put_serde(&endpoint_key, &observed_endpoint, None)
                        .expect("endpoint put op"),
                ],
                failure: Vec::new(),
            },
        )
        .expect("source-only member publish should succeed without append");

        assert_eq!(
            snapshot_after.revision, snapshot.revision,
            "source-only member publishes must not advance the cluster revision"
        );
        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: snapshot.revision,
                responses: vec![
                    KvOpResponse::Put {
                        key: member_key,
                        revision: member_entry.modify_revision,
                    },
                    KvOpResponse::Put {
                        key: endpoint_key,
                        revision: endpoint_entry.modify_revision,
                    },
                ],
            })
        );
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("wal after source no-effect")
                .len(),
            wal_len_before,
            "source-only member publishes must not append to the raft WAL"
        );
    }

    #[test]
    fn forwarded_cluster_kv_txn_does_not_start_local_election() {
        let root = unique_cluster_test_root("sinomo-daemon-forwarded-kv-no-election");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_forwarded_kv_no_election")
            .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_daemon_forwarded_kv_no_election").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "forwarded kv no election",
            local,
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft");
        daemon_clear_cluster_raft_leader(&paths, &state.local_node_id).expect("clear known leader");
        assert_eq!(
            daemon_cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
            None
        );

        let error = commit_cluster_kv_txn_with_quorum_response_with_forwarding(
            &paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/test/forwarded-no-election".to_string(),
                    value: b"blocked".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            false,
        )
        .expect_err("forwarded non-leader must reject instead of electing");
        assert!(error.to_string().contains("non-leader voter"));
        assert_eq!(
            daemon_cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
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
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_354,
        };
        ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, now);
        sinomo_network::write_cluster_state(&paths, &state).expect("write local state");

        let registration = DaemonClusterRegistration {
            process: crate::DaemonClusterProcess::Shadow,
            main_options,
            shadow_options,
        };
        let status = daemon_cluster_status_from_local_state(&paths, &registration)
            .expect("shadow cluster status");

        assert_eq!(status.cluster_id, state.cluster_id);
        assert_eq!(status.process, sinomo_node::DAEMON_SHADOW_CLUSTER_ROLE);
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
        sinomo_network::write_cluster_state(&paths, &state).expect("write local state");
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
        sinomo_kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
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
    fn shadow_iroh_repair_route_refreshes_local_state_from_disk() {
        let root = unique_cluster_test_root("sinomo-daemon-shadow-iroh-repair-route");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let shadow_paths = DaemonPaths::for_shadow(&data_dir);
        shadow_paths.ensure().expect("shadow paths");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_shadow_iroh_repair_route").expect("cluster"),
            "shadow iroh repair route".to_string(),
            NodeId::from_legacy_named("node_shadow_iroh_repair_route").expect("node"),
            "shadow iroh repair route".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let main_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_353,
        };
        let shadow_options = DaemonRunOptions {
            host: "0.0.0.0".to_string(),
            port: 21_354,
        };
        ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, now);
        let shadow_cluster = daemon_cluster_status(&state, crate::DaemonClusterProcess::Shadow)
            .expect("shadow cluster status");
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write local state");
        write_daemon_status(
            &shadow_paths,
            &crate::DaemonStatus::new(&shadow_options, &shadow_paths, Some(shadow_cluster)),
        )
        .expect("write shadow status");
        sinomo_network::write_iroh_endpoint_record(
            &cluster_paths,
            &IrohEndpointRecord {
                cluster_id: state.cluster_id.clone(),
                node_id: state.local_node_id.clone(),
                endpoint_public_key: "iroh_public_key_shadow_repair_route".to_string(),
                direct_addresses: Vec::new(),
                relay_url: Some("https://relay.example./".to_string()),
                relay_mode: sinomo_network::IrohRelayMode::Default,
                expires_after_tick: 0,
            },
        )
        .expect("write iroh endpoint record");

        assert!(
            refresh_daemon_iroh_repair_route(&shadow_paths).expect("refresh repair route"),
            "shadow repair route should be refreshed from the local iroh endpoint record"
        );
        let updated =
            sinomo_network::read_cluster_state_without_kv(&cluster_paths).expect("read state");
        let main_member = updated
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("main member");
        assert!(main_member.endpoints.iter().any(|endpoint| {
            endpoint.transport == ClusterTransportKind::Iroh
                && endpoint.address
                    == "iroh://iroh_public_key_shadow_repair_route?relay=https://relay.example./"
        }));
    }

    #[test]
    fn connectivity_publish_cache_is_scoped_and_expires_before_kv_ttl() {
        let root_a = unique_cluster_test_root("sinomo-daemon-connectivity-cache-a");
        let root_b = unique_cluster_test_root("sinomo-daemon-connectivity-cache-b");
        let paths_a = ClusterPaths::new(&SinomoDataDir::new(&root_a));
        let paths_b = ClusterPaths::new(&SinomoDataDir::new(&root_b));
        paths_a.ensure().expect("paths a");
        paths_b.ensure().expect("paths b");

        let now = OffsetDateTime::now_utc();
        let record = ClusterConnectivityRecord {
            cluster_id: ClusterId::from_legacy_named("cluster_daemon_connectivity_cache")
                .expect("cluster"),
            source_node_id: NodeId::from_legacy_named("node_daemon_cache_source").expect("source"),
            target_node_id: NodeId::from_legacy_named("node_daemon_cache_target").expect("target"),
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_cache",
            )
            .expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.44:21353".to_string(),
            state: sinomo_network::ClusterConnectivityState::Healthy,
            latency_ms: Some(9),
            consecutive_failures: 0,
            last_success_at: Some("ok".to_string()),
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(300),
        };

        remember_connectivity_record_set(
            &paths_a,
            &record.source_node_id,
            std::slice::from_ref(&record),
        );
        assert!(cached_connectivity_matches(&paths_a, &record));
        assert!(
            !cached_connectivity_matches(&paths_b, &record),
            "connectivity publish cache must not cross data directories"
        );

        let mut near_expiry = record;
        near_expiry.expires_at = now + time::Duration::seconds(30);
        remember_connectivity_record_set(
            &paths_b,
            &near_expiry.source_node_id,
            std::slice::from_ref(&near_expiry),
        );
        assert!(
            !cached_connectivity_matches(&paths_b, &near_expiry),
            "stable cached connectivity must be publishable before its KV record expires"
        );
    }

    #[test]
    fn connectivity_record_set_cache_distinguishes_unknown_empty_source() {
        let root = unique_cluster_test_root("sinomo-daemon-connectivity-empty-source-cache");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("paths");
        let source_node =
            NodeId::from_legacy_named("node_daemon_empty_source_cache").expect("source");

        assert!(
            !cached_connectivity_record_set_matches(&paths, &source_node, &[]),
            "an unknown source must not be treated as a known empty published set"
        );
        remember_connectivity_record_set(&paths, &source_node, &[]);
        assert!(
            cached_connectivity_record_set_matches(&paths, &source_node, &[]),
            "an explicitly synced empty source can be cached"
        );
    }

    fn doctor_connectivity_record(
        cluster_id: &ClusterId,
        source: &NodeId,
        target: &NodeId,
        endpoint_name: &str,
        address: &str,
        state: sinomo_network::ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let healthy = state == sinomo_network::ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(endpoint_name)
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
            observed_at: now,
            expires_at: now + time::Duration::seconds(CLUSTER_CONNECTIVITY_TTL_SECONDS),
        }
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
                sinomo_network::ClusterConnectivityState::Unreachable,
            ),
            doctor_connectivity_record(
                &cluster_id,
                &source,
                &bridge,
                "endpoint_daemon_doctor_routed_source_bridge",
                "172.23.0.4:21353",
                sinomo_network::ClusterConnectivityState::Healthy,
            ),
            doctor_connectivity_record(
                &cluster_id,
                &bridge,
                &target,
                "endpoint_daemon_doctor_routed_bridge_target",
                "172.25.0.3:21353",
                sinomo_network::ClusterConnectivityState::Healthy,
            ),
        ];

        let findings = sinomo_network::cluster_doctor_findings(&state, &records, &[]);

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
            sinomo_network::ClusterConnectivityState::Unreachable,
        )];
        let health = vec![ClusterHealthRow {
            member: target_member.clone(),
            status: sinomo_network::unknown_cluster_node_status(&state, &target_member, now),
            availability: ClusterNodeAvailability::Online,
        }];

        let findings = sinomo_network::cluster_doctor_findings(&state, &records, &health);

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
            sinomo_network::ClusterConnectivityState::Unreachable,
        )];

        let findings = sinomo_network::cluster_doctor_findings(&state, &records, &[]);

        assert!(
            findings
                .iter()
                .any(|finding| finding.code == "endpoint-unreachable"),
            "a truly unrouted unreachable endpoint should still warn"
        );
    }

    #[test]
    fn stored_network_info_match_populates_publish_cache() {
        let root = unique_cluster_test_root("sinomo-daemon-network-info-cache");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_network_info_cache").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_daemon_network_info_cache_local").expect("local");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "network info cache",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let info = sinomo_network::build_cluster_network_info_record(
            &state,
            &local_node,
            now,
            CLUSTER_NETWORK_INFO_TTL_SECONDS,
        );
        let mut store = InProcessKvStore::new();
        store
            .put_serde(
                sinomo_network::cluster_network_info_key(&local_node),
                &info,
                None,
            )
            .expect("seed network info");
        sinomo_kv::write_cluster_kv_snapshot(&paths.kv_paths(), &store.snapshot())
            .expect("write matching snapshot");

        assert!(
            sync_cluster_network_info_to_kv(&paths, info.clone())
                .expect("stored match")
                .is_none()
        );
        std::fs::remove_file(&paths.kv_snapshot).expect("remove snapshot");
        assert!(
            sync_cluster_network_info_to_kv(&paths, info)
                .expect("cached match without snapshot")
                .is_none(),
            "stored-match caching should avoid repeated stable KV snapshot reads"
        );
    }

    #[test]
    fn stable_cluster_state_sync_returns_current_snapshot_not_stale_materialized() {
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
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        let current = sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
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

        sinomo_kv::write_cluster_kv_snapshot(
            &paths.kv_paths(),
            &InProcessKvStore::new().snapshot(),
        )
        .expect("install stale materialized cache");
        let stale_materialized = sinomo_kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
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
            "stable sync should not append a raft log entry"
        );
        let materialized = sinomo_kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .expect("materialized after sync");
        assert!(
            materialized.get(&member_key).is_none(),
            "stable sync should not rewrite the intentionally stale materialized cache"
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
        let info = sinomo_network::build_cluster_network_info_record(
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
            sinomo_network::cluster_network_info_publish_record_matches(
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
        sinomo_node::apply_cluster_raft_role(
            &mut state.members[0],
            ClusterMemberRole::NonVoting,
            now,
        );
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.41:21353".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                sinomo_network::cluster_connectivity_probe_key(&local_node, &endpoint),
                ClusterConnectivityProbeState {
                    record: record.clone(),
                },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.42:21353".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        sinomo_network::write_local_cluster_connectivity_records(
            &paths,
            &local_node,
            std::slice::from_ref(&record),
        )
        .expect("persist local connectivity");
        assert!(
            paths.connectivity_local.exists(),
            "test setup should persist the local probe graph"
        );

        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::from([(
                sinomo_network::cluster_connectivity_probe_key(&local_node, &endpoint),
                ClusterConnectivityProbeState { record },
            )]),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: Some("old-environment".to_string()),
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: Instant::now(),
            last_publish: Instant::now(),
            last_discovery: Instant::now(),
            last_probe: Instant::now(),
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        sinomo_network::refresh_cluster_connectivity_environment(
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
            sinomo_network::read_local_cluster_connectivity_records(&paths)
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.53:21353".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Unreachable,
            None,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        let key = sinomo_network::cluster_connectivity_probe_key(&local_node, &endpoint);
        let maintenance_now = Instant::now();
        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let mut maintenance = ClusterNetworkMaintenance {
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
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.54:21353".to_string(), 10);
        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Unreachable,
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
                sinomo_network::CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL.as_secs() as i64,
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.55:21353".to_string(), 10);
        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Degraded,
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.56:21353".to_string(), 10);
        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("failed".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        assert!(
            sinomo_network::cluster_connectivity_probe_due_with_startup_settle(
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
    fn connectivity_probe_retries_rerouted_unhealthy_endpoint() {
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
            sinomo_network::daemon_lan_endpoint(&target, "10.0.0.63:21353".to_string(), 10);
        let mut unreachable_direct = doctor_connectivity_record(
            &cluster_id,
            &local,
            &target,
            "endpoint_daemon_rerouted_direct",
            &target_endpoint.address,
            sinomo_network::ClusterConnectivityState::Unreachable,
        );
        unreachable_direct.observed_at = now;
        unreachable_direct.expires_at = now + time::Duration::hours(1);
        let mut degraded_direct = unreachable_direct.clone();
        degraded_direct.state = sinomo_network::ClusterConnectivityState::Degraded;
        degraded_direct.consecutive_failures = 1;
        let local_to_bridge = doctor_connectivity_record(
            &cluster_id,
            &local,
            &bridge,
            "endpoint_daemon_rerouted_local_bridge",
            "10.0.0.64:21353",
            sinomo_network::ClusterConnectivityState::Healthy,
        );
        let bridge_to_target = doctor_connectivity_record(
            &cluster_id,
            &bridge,
            &target,
            "endpoint_daemon_rerouted_bridge_target",
            "10.0.0.65:21353",
            sinomo_network::ClusterConnectivityState::Healthy,
        );
        let graph = sinomo_network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![
                unreachable_direct.clone(),
                local_to_bridge,
                bridge_to_target,
            ],
        );

        // A healthy alternate route must NOT suppress re-probing a due
        // non-healthy direct endpoint: a healed direct dial always beats
        // relaying, and during cold formation the "route" can itself traverse
        // stale edges — suppression used to pin late joiners as degraded until
        // record expiry.
        assert!(
            sinomo_network::cluster_connectivity_probe_due_with_graph(
                &unreachable_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a due unreachable direct endpoint should be reprobed even while a healthy alternate route exists"
        );
        assert!(
            sinomo_network::cluster_connectivity_probe_due_with_graph(
                &degraded_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a due degraded direct endpoint should be reprobed even while a healthy alternate route exists"
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
            sinomo_network::daemon_lan_endpoint(&target, "10.0.0.66:21353".to_string(), 10);
        let mut unreachable_direct = doctor_connectivity_record(
            &cluster_id,
            &local,
            &target,
            "endpoint_daemon_unrouted_direct",
            &target_endpoint.address,
            sinomo_network::ClusterConnectivityState::Unreachable,
        );
        unreachable_direct.observed_at = now;
        unreachable_direct.expires_at = now + time::Duration::hours(1);
        let graph = sinomo_network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![unreachable_direct.clone()],
        );

        assert!(
            sinomo_network::cluster_connectivity_probe_due_with_graph(
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
        let tcp =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.53:21353".to_string(), 10);
        let iroh = IrohEndpointRecord::from_relay_url(
            cluster_id.clone(),
            remote_node.clone(),
            "iroh_public_key_daemon_command_connectivity",
            "https://relay.example./",
        )
        .endpoint();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![tcp.clone(), iroh.clone()];
        state.members.push(remote_member);

        let read_tcp_record = || {
            sinomo_network::read_local_cluster_connectivity_records(&paths)
                .expect("local connectivity records")
                .into_iter()
                .find(|record| {
                    record.source_node_id == local_node
                        && record.target_node_id == remote_node
                        && record.endpoint_id == tcp.endpoint_id
                })
                .expect("tcp connectivity record")
        };

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(8),
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.latency_ms, Some(8));
        assert!(
            !cached_connectivity_matches(&paths, &record),
            "command-path observations must not enter the published cache before a KV sync"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Healthy,
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
                sinomo_network::ClusterConnectivityState::Degraded,
                None,
                None,
            );
            let record = read_tcp_record();
            assert_eq!(
                record.state,
                sinomo_network::ClusterConnectivityState::Degraded
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
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Unreachable
        );
        assert_eq!(
            record.consecutive_failures,
            CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
        );
        assert!(
            record.last_success_at.is_none(),
            "sustained command-path failures should stop advertising a stale success"
        );

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&iroh.endpoint_id),
            "unreachable direct command endpoint should not outrank relay fallback"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(12),
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.latency_ms, Some(12));
        assert!(
            record.last_failure_at.is_some(),
            "healthy recovery keeps the last failure timestamp as diagnostic history"
        );

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "recovered direct command endpoint should become the preferred path again"
        );

        remember_cluster_endpoint_command_connectivity_without_failure_grace(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Degraded,
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
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(13),
            None,
        );
        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "a later successful command send should restore a hard-degraded endpoint"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Unreachable,
            None,
            None,
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Unreachable,
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
        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&iroh.endpoint_id),
            "identity-failed direct command endpoint should not outrank relay fallback"
        );

        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &tcp,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(14),
            None,
        );
        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
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
            let endpoint = sinomo_network::daemon_lan_endpoint(
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
            remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
            remote_member.allowed_capabilities = remote_member.capabilities.clone();
            remote_member.endpoints = vec![endpoint];
            state.members.push(remote_member);
        }

        let idle_stats = Arc::new(IdleStatsAtomics::new());
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            connectivity_converged: true,
            environment_id: None,
            local_node_role: sinomo_network::cluster_local_node_role(&state),
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let plan = maintenance.select_connectivity_probe_plan(&paths, &state);
        let expected_batch_len =
            sinomo_network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK;
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.52:21353".to_string(), 10);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let record = sinomo_network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
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

        let key = sinomo_network::cluster_connectivity_probe_key(&local_node, &endpoint);
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
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
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
            tick: 0,
            publish_failure_streak: 0,
            last_kv_snapshot_pull: maintenance_now,
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
    fn connectivity_sync_deletes_stale_kv_records_even_when_current_records_are_cached() {
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
            sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let current_endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.61:21353".to_string(), 10);
        let stale_endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.62:21353".to_string(), 20);
        let mut remote_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![current_endpoint.clone(), stale_endpoint.clone()];
        state.members.push(remote_member);
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state");

        let current_record = sinomo_network::cluster_connectivity_record(
            &state,
            &current_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("ok".to_string()),
            None,
            now,
            600,
        );
        let stale_record = sinomo_network::cluster_connectivity_record(
            &state,
            &stale_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(9),
            0,
            Some("old".to_string()),
            None,
            now,
            600,
        );
        let current_key = sinomo_network::cluster_connectivity_key(
            &current_record.source_node_id,
            &current_record.target_node_id,
            &current_record.endpoint_id,
        );
        let stale_key = sinomo_network::cluster_connectivity_key(
            &stale_record.source_node_id,
            &stale_record.target_node_id,
            &stale_record.endpoint_id,
        );
        assert_ne!(
            current_key, stale_key,
            "test endpoints must use distinct connectivity keys"
        );
        sinomo_network::sync_cluster_connectivity_records_to_kv(
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
        let seeded = sinomo_kv::read_cluster_kv_current_snapshot_for_node(
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
        sync_cluster_connectivity_records_to_kv(&paths, &local_node, vec![current_record.clone()])
            .expect("sync should delete stale connectivity");

        let snapshot = sinomo_kv::read_cluster_kv_current_snapshot_for_node(
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
            sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state");

        let mut stale_records = Vec::new();
        for index in 0..=sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND {
            let target_node = NodeId::from_legacy_named(format!(
                "node_daemon_connectivity_large_delete_target_{index}"
            ))
            .expect("target node");
            let endpoint = sinomo_network::daemon_lan_endpoint(
                &target_node,
                format!("10.64.{}.{}:21353", index / 256, index % 256),
                10,
            );
            stale_records.push(sinomo_network::cluster_connectivity_record(
                &state,
                &endpoint,
                sinomo_network::ClusterConnectivityState::Healthy,
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
                    sinomo_network::cluster_connectivity_key(
                        &record.source_node_id,
                        &record.target_node_id,
                        &record.endpoint_id,
                    ),
                    record,
                    None,
                )
                .expect("stale connectivity put op"),
            );
            if stale_ops.len() >= sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND {
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

        let snapshot = sinomo_kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            local_node.clone(),
        )
        .expect("snapshot");
        assert!(
            snapshot
                .prefix_entries(&sinomo_network::cluster_connectivity_source_prefix(
                    &local_node
                ))
                .next()
                .is_none(),
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
        let tcp =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.41:21353".to_string(), 10);
        let iroh = IrohEndpointRecord::from_relay_url(
            cluster_id.clone(),
            remote_node.clone(),
            "iroh_public_key_daemon_raft_connectivity",
            "https://relay.example./",
        )
        .endpoint();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![tcp.clone(), iroh.clone()];
        state.members.push(remote_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft kv");
        sinomo_network::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &tcp,
                    sinomo_network::ClusterConnectivityState::Unreachable,
                    None,
                    4,
                    None,
                    Some("timed out".to_string()),
                    now,
                    60,
                ),
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &iroh,
                    sinomo_network::ClusterConnectivityState::Healthy,
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

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);

        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&iroh.endpoint_id),
            "raft/KV forwarding must not spend the command budget on an unreachable TCP endpoint before a healthy Iroh path"
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

        let voter_endpoint =
            sinomo_network::daemon_lan_endpoint(&voter_node, "10.0.0.10:21353".to_string(), 10);
        let bridge_member_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "10.0.0.20:21353".to_string(), 10);
        let bridge_learned_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "node4:21353".to_string(), 5);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node,
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint.clone()];
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            bridge_node,
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = sinomo_node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_member_endpoint.clone()];
        state.members.push(voter_member);
        state.members.push(bridge_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft kv");
        sinomo_network::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &voter_endpoint,
                    sinomo_network::ClusterConnectivityState::Unreachable,
                    None,
                    4,
                    None,
                    Some("stale voter address".to_string()),
                    now,
                    60,
                ),
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &bridge_learned_endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
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

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::MemberPublish);

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
        issuer_member.capabilities = sinomo_node::main_daemon_capabilities(&issuer_member.role);
        issuer_member.allowed_capabilities = issuer_member.capabilities.clone();
        issuer_member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
            &issuer_node,
            "172.24.0.2:21353".to_string(),
            20,
        )];
        state.members.push(issuer_member);

        let channel_endpoint =
            sinomo_network::daemon_lan_endpoint(&channel_node, "node1:21353".to_string(), 0);
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

        let retargeted = sinomo_network::join_channel_endpoint_for_accepted_state(
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

        let voter_endpoint =
            sinomo_network::daemon_lan_endpoint(&voter_node, "10.0.0.10:21353".to_string(), 10);
        let bridge_stale_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "10.0.0.20:21353".to_string(), 10);
        let bridge_learned_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "node3:21353".to_string(), 5);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node,
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint];
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            bridge_node,
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = sinomo_node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_stale_endpoint];
        kv_state.members.push(voter_member);
        kv_state.members.push(bridge_member);

        sinomo_network::write_cluster_state(&paths, &kv_state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &kv_state)
            .expect("sync state to raft kv");

        let mut local_state = kv_state.clone();
        let local_bridge = local_state
            .members
            .iter_mut()
            .find(|member| member.node_id == bridge_learned_endpoint.node_id)
            .expect("bridge member");
        local_bridge.endpoints.push(bridge_learned_endpoint.clone());
        sinomo_network::write_cluster_state(&paths, &local_state)
            .expect("write local learned route");

        let merged_state = sinomo_network::read_cluster_state(&paths).expect("read merged state");
        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &merged_state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::MemberPublish);

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
        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed local kv snapshot");

        let runtime = daemon_cluster_test_runtime();
        publish_local_members_to_quorum_if_needed_on_runtime(&paths, &state, runtime.handle())
            .expect("stable local members should not be republished");
    }

    #[test]
    fn raft_command_targets_try_unknown_tcp_before_unknown_iroh_fallbacks() {
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
        let public_tcp =
            sinomo_network::daemon_lan_endpoint(&remote_node, "203.0.113.41:21353".to_string(), 10);
        let private_tcp =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.41:21353".to_string(), 20);
        let iroh = IrohEndpointRecord::from_relay_url(
            cluster_id.clone(),
            remote_node.clone(),
            "iroh_public_key_daemon_raft_unknown_tcp",
            "https://relay.example./",
        )
        .endpoint();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node,
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![public_tcp.clone(), private_tcp.clone(), iroh.clone()];
        state.members.push(remote_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft kv");

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);

        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&private_tcp.endpoint_id),
            "raft/KV forwarding should try bounded TCP before spending the command budget on relay fallback when neither path has current connectivity data"
        );
        let iroh_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == iroh.endpoint_id)
            .expect("iroh target");
        let public_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == public_tcp.endpoint_id)
            .expect("public tcp target");
        assert!(public_position < iroh_position);
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

    fn test_relay_envelope(
        state: &ClusterBootstrapState,
        target_node: &NodeId,
    ) -> sinomo_network::RelayEnvelope {
        sinomo_network::RelayEnvelope::new(
            state.cluster_id.clone(),
            target_node.clone(),
            sinomo_network::ObjectRpcRequest {
                route: "/test/relay".to_string(),
                category: "test".to_string(),
                metadata: BTreeMap::new(),
                auth: None,
                body: Vec::new(),
            },
        )
    }

    #[test]
    fn relay_next_hops_fall_back_to_direct_target_endpoint_before_graph_converges() {
        let root = unique_cluster_test_root("sinomo-daemon-relay-direct-target-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_relay_direct_fallback")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_relay_direct_fallback_local")
            .expect("local node");
        let target_node = NodeId::from_legacy_named("node_daemon_relay_direct_fallback_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay direct fallback",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let target_endpoint =
            sinomo_network::daemon_lan_endpoint(&target_node, "node1:21353".to_string(), 10);
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = sinomo_node::main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(target_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        let runtime = daemon_cluster_test_runtime();
        let envelope = test_relay_envelope(&state, &target_node);
        let initial_ttl = envelope.ttl;
        let mut host = RecordingRelayStreamForwardHost::forwarding();
        let outcome = runtime
            .block_on(
                sinomo_network::handle_cluster_relay_stream_forward_async_with_host(
                    runtime.handle(),
                    &paths,
                    &state,
                    &local_node,
                    envelope,
                    sinomo_network::ClusterRelayOptions::new(
                        sinomo_network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
                    ),
                    &mut host,
                ),
            )
            .expect("relay forward");

        assert!(
            matches!(
                outcome,
                sinomo_network::ClusterRelayStreamForwardOutcome::Forwarded
            ),
            "relay should deliver directly to a known command endpoint while connectivity publication is still empty"
        );
        assert_eq!(host.sent.len(), 1);
        let (endpoint, forwarded, _timeouts) = host.sent.first().expect("sent relay");
        assert_eq!(endpoint.node_id, target_node);
        assert_eq!(
            endpoint.endpoint_id, target_endpoint.endpoint_id,
            "relay should use the known target command endpoint"
        );
        assert_eq!(forwarded.target_node_id, target_node);
        assert_eq!(forwarded.ttl, initial_ttl.saturating_sub(1));
        assert_eq!(forwarded.visited, vec![local_node]);
    }

    #[test]
    fn relay_next_hop_endpoints_suppress_unknown_hostname_fallback_after_healthy_route() {
        let root = unique_cluster_test_root("sinomo-daemon-relay-local-hostname-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_relay_hostname_fallback")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_relay_hostname_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_daemon_relay_hostname_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay hostname fallback",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        // Give the founding local member real main-daemon capabilities: this
        // test is about relay next-hop selection, and a capability-less local
        // member is not a raft voter, which would make the connectivity
        // remember helper publish to quorum — dialing the (unroutable) target
        // endpoints below with real sockets.
        let local_member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == local_node)
            .expect("local member");
        local_member.capabilities = sinomo_node::main_daemon_capabilities(&local_member.role);
        local_member.allowed_capabilities = local_member.capabilities.clone();
        let stale_endpoint =
            sinomo_network::daemon_lan_endpoint(&target_node, "10.10.0.5:21353".to_string(), 10);
        let learned_endpoint =
            sinomo_network::daemon_lan_endpoint(&target_node, "node1:21353".to_string(), 5);
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = sinomo_node::main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![stale_endpoint.clone(), learned_endpoint.clone()];
        state.members.push(target_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        remember_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &stale_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(1),
            None,
        );
        let runtime = daemon_cluster_test_runtime();
        let envelope = test_relay_envelope(&state, &target_node);
        let mut host = RecordingRelayStreamForwardHost::routing_error();
        let outcome = runtime
            .block_on(
                sinomo_network::handle_cluster_relay_stream_forward_async_with_host(
                    runtime.handle(),
                    &paths,
                    &state,
                    &local_node,
                    envelope,
                    sinomo_network::ClusterRelayOptions::new(
                        sinomo_network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
                    ),
                    &mut host,
                ),
            )
            .expect("relay forward");

        assert!(matches!(
            outcome,
            sinomo_network::ClusterRelayStreamForwardOutcome::ErrorResponse(_)
        ));
        assert_eq!(host.sent.len(), 1);
        assert!(
            host.sent[0].0.address == stale_endpoint.address,
            "healthy next-hop routing should try the known healthy endpoint first"
        );
        assert!(
            host.sent.iter().all(
                |(endpoint, _envelope, _timeouts)| endpoint.address != learned_endpoint.address
            ),
            "healthy next-hop routing suppresses unknown hostname fallbacks"
        );
    }

    #[test]
    fn join_voter_targets_do_not_filter_offline_status_during_bootstrap() {
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
            sinomo_network::daemon_lan_endpoint(&voter_node, "node1:21353".to_string(), 10);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            voter_node.clone(),
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint.clone()];
        state.members.push(voter_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync cluster state");
        seed_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id,
                node_id: voter_node,
                process: sinomo_node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                availability: ClusterNodeAvailability::Offline,
                daemon_version: "0.1.0-test".to_string(),
                commit: None,
                target_os: Some("test-os".to_string()),
                target_arch: Some("test-arch".to_string()),
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
            sinomo_network::ClusterEndpointSelector::new(&paths, &state)
                .select(sinomo_network::ClusterEndpointSelectionPurpose::LiveVoterCommand)
                .is_empty(),
            "ordinary live-voter routing should honor explicit offline status"
        );
        assert_eq!(
            sinomo_network::ClusterEndpointSelector::new(&paths, &state)
                .select(sinomo_network::ClusterEndpointSelectionPurpose::VoterCommand)
                .first()
                .map(|endpoint| &endpoint.endpoint_id),
            Some(&voter_endpoint.endpoint_id),
            "join bootstrap must still try voter command targets even when local health is stale/offline"
        );
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
        let first_tcp =
            sinomo_network::daemon_lan_endpoint(&first_node, "10.0.0.41:21353".to_string(), 10);
        let first_iroh = IrohEndpointRecord::from_relay_url(
            cluster_id.clone(),
            first_node.clone(),
            "iroh_public_key_daemon_raft_interleave_first",
            "https://relay.example./",
        )
        .endpoint();
        let second_tcp =
            sinomo_network::daemon_lan_endpoint(&second_node, "10.0.0.42:21353".to_string(), 10);
        for (node_id, name, public_key, endpoints) in [
            (
                first_node.clone(),
                "first",
                "pk-first",
                vec![first_tcp.clone(), first_iroh.clone()],
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
            member.capabilities = sinomo_node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = endpoints;
            state.members.push(member);
        }

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state to raft kv");

        let targets = sinomo_network::ClusterEndpointSelector::new(&paths, &state)
            .select(sinomo_network::ClusterEndpointSelectionPurpose::RaftCommand);
        let first_iroh_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == first_iroh.endpoint_id)
            .expect("first iroh");
        let second_tcp_position = targets
            .iter()
            .position(|endpoint| endpoint.endpoint_id == second_tcp.endpoint_id)
            .expect("second tcp");

        assert!(
            second_tcp_position < first_iroh_position,
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
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote_node, "10.0.0.41:21353".to_string(), 10);
        let remote_endpoint_id = endpoint.endpoint_id.clone();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities = sinomo_node::main_daemon_capabilities(&remote_member.role);
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![endpoint];
        state.members.push(remote_member);

        sinomo_network::write_cluster_state(&paths, &state).expect("write cluster state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
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
                process: sinomo_node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                availability: ClusterNodeAvailability::Online,
                daemon_version: "0.1.0-test".to_string(),
                commit: None,
                target_os: Some("test-os".to_string()),
                target_arch: Some("test-arch".to_string()),
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
        sinomo_network::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node,
            vec![ClusterConnectivityRecord {
                cluster_id,
                source_node_id: local_node.clone(),
                target_node_id: remote_node.clone(),
                endpoint_id: remote_endpoint_id,
                transport: ClusterTransportKind::LanTcp,
                address: "10.0.0.41:21353".to_string(),
                state: sinomo_network::ClusterConnectivityState::Unreachable,
                latency_ms: None,
                consecutive_failures: 5,
                last_success_at: None,
                last_failure_at: Some("2026-05-04T00:10:00Z".to_string()),
                observed_at: now,
                expires_at: now + time::Duration::seconds(300),
            }],
        )
        .expect("sync unreachable connectivity");

        assert!(
            sinomo_network::read_cluster_node_statuses_from_kv(&paths)
                .expect("node statuses")
                .into_iter()
                .find(|status| status.node_id == remote_node)
                .is_some_and(|status| status.availability != ClusterNodeAvailability::Offline),
            "an unreachable member that never self-declared Offline is not explicit offline"
        );
        assert_eq!(
            sinomo_network::ClusterEndpointSelector::new(&paths, &state)
                .select(sinomo_network::ClusterEndpointSelectionPurpose::LiveVoterCommand)
                .len(),
            1,
            "an unreachable member must remain a voter while connectivity is recovering"
        );
    }

    #[test]
    fn raft_command_timeout_caps_single_iroh_attempt_to_preserve_fallback_budget() {
        let node_id = NodeId::from_legacy_named("node_daemon_raft_iroh_timeout").expect("node");
        let endpoint = IrohEndpointRecord::from_relay_url(
            ClusterId::from_legacy_named("cluster_daemon_raft_iroh_timeout").expect("cluster"),
            node_id,
            "iroh_public_key_daemon_raft_timeout",
            "https://relay.example./",
        )
        .endpoint();

        assert_eq!(
            cluster_voter_reachability_options().command_timeout(
                &endpoint,
                sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT
        );
    }

    #[test]
    fn foreground_raft_attempts_preserve_budget_for_fallback_routes() {
        let node_id =
            NodeId::from_legacy_named("node_daemon_raft_foreground_timeout").expect("node");
        let lan = sinomo_network::daemon_lan_endpoint(&node_id, "10.0.0.41:21353".to_string(), 10);
        let public = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_raft_foreground_public",
            )
            .expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.41:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let iroh = IrohEndpointRecord::from_relay_url(
            ClusterId::from_legacy_named("cluster_daemon_raft_foreground").expect("cluster"),
            node_id,
            "iroh_public_key_daemon_raft_foreground",
            "https://relay.example./",
        )
        .endpoint();

        let timeout_options = cluster_voter_reachability_options();
        assert_eq!(
            timeout_options.fair_command_timeout(
                &lan,
                sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            timeout_options.fair_command_timeout(
                &public,
                sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            timeout_options.fair_command_timeout(
                &iroh,
                sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
            ),
            sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            timeout_options.fair_command_timeout(&lan, Duration::from_millis(50)),
            Duration::from_millis(50)
        );

        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_daemon_raft_snapshot_timeout").expect("cluster"),
            "snapshot timeout",
            NodeId::from_legacy_named("node_daemon_raft_snapshot_timeout_local").expect("local"),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        let snapshot_request = ClusterRaftAppendRequest {
            state,
            snapshot: Some(sinomo_kv::RaftSnapshot {
                last_included_index: 1,
                last_included_term: 1,
                kv: InProcessKvStore::new().snapshot(),
            }),
            append: sinomo_kv::RaftAppendEntriesRequest {
                term: 1,
                leader_id: NodeId::from_legacy_named("node_daemon_raft_snapshot_timeout_leader")
                    .expect("leader"),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: Vec::new(),
                leader_commit: 1,
            },
        };
        assert_eq!(
            cluster_raft_append_attempt_timeout(
                &public,
                sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
                &snapshot_request
            ),
            sinomo_network::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT
        );
    }
}
