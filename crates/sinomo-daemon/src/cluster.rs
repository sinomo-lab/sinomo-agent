use crate::{
    AuthenticatedDaemonCommand, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    DaemonClusterRegistration, DaemonClusterStatus, DaemonError, DaemonPaths, DaemonProbeStatus,
    DaemonRunOptions, IdleStatsAtomics, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_DEFAULT_PORT, authenticated_signer_matches_cluster_node,
    cluster_node_status_from_daemon_status, current_shadow_daemon_options,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths, daemon_cluster_status,
    daemon_cluster_status_matches, daemon_connect_address, daemon_options_from_status,
    daemon_paths_are_shadow, default_main_daemon_status, ensure_daemon_cluster_members,
    ensure_default_distributed_dfs, fetch_cluster_kv_snapshot_from_endpoint,
    fetch_daemon_status_from_endpoint, handle_daemon_rollout_build_request,
    invite_introducers_from_daemon_status, probe_daemon_status, read_daemon_status,
    send_cluster_join_registration_to_channel, send_cluster_kv_txn, send_cluster_kv_watch_events,
    send_cluster_member_publish, send_cluster_raft_append, send_cluster_raft_vote,
    unknown_cluster_node_status, write_daemon_status,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterJoinInvite,
    ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability,
    ClusterNodeRole, ClusterNodeRolloutStatus, ClusterNodeStatusRecord, ClusterTransportKind,
    TrustDomain,
};
use sinomo_kv::{
    InProcessKvStore, KvCommand, KvCompare, KvEntry, KvEvent, KvOp, KvSnapshot, KvTxn,
    KvTxnResponse, RaftAppendEntriesResponse, RaftRequestVoteRequest, RaftRequestVoteResponse,
    cluster_member_key,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterConnectivityRecord, ClusterId, ClusterInviteId, ClusterInvitePublishRequest,
    ClusterInvitePublishResponse, ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse,
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse,
    ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration, ClusterMemberPublishRequest,
    ClusterMemberPublishResponse, ClusterNetworkInfoRecord, ClusterPaths, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse, IrohEndpointRecord,
    MdnsDiscoveryAdvertisement, MdnsDiscoveryCache, MdnsDiscoverySocket,
    invite_channels_for_join_registration,
};
use sinomo_node::NodeId;
use sinomo_node::shadow_daemon_node_id;
use sinomo_node::{load_or_create_main_node_identity, load_or_create_shadow_node_identity};
use sinomo_rollout::{
    ClusterRolloutBuildRequest, SelfRolloutPaths, managed_source_status, read_rollout_status,
    source_heads_match,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const DAEMON_CLUSTER_RPC_TIMEOUT: Duration = Duration::from_millis(750);
pub const DAEMON_CLUSTER_TCP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DAEMON_CLUSTER_IROH_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
pub const DAEMON_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
pub const DAEMON_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
pub const DAEMON_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(1500);
pub const DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT: Duration = Duration::from_millis(1500);
pub const DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
pub const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_secs(15);
pub const DAEMON_CLUSTER_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF: Duration = Duration::from_secs(600);
pub const DAEMON_CLUSTER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
pub const DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL: Duration = Duration::from_secs(60);
pub const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 = 600;
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = 900;
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 = 7200;
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 = 120;
pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 = 3;
// Endpoints probed per connectivity tick. Probes run in PARALLEL (scoped threads),
// so the tick's wall time is bounded by one probe timeout regardless of the count;
// this lets the connectivity graph converge in a single tick after a restart/join
// instead of one-endpoint-per-60s-tick (which took O(endpoints) minutes to settle).
// Bounded so a very large cluster doesn't spawn an unreasonable number of threads
// at once; excess endpoints rotate in over subsequent ticks via connectivity_cursor.
const CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 32;
const CLUSTER_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;
const CLUSTER_RAFT_ELECTION_TERM_BUMP: u64 = 64;
const PUBLISHED_NODE_CACHE_MAX_ENTRIES: usize = sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_SOURCES: usize =
    sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS_PER_SOURCE: usize =
    sinomo_network::MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS: usize =
    sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
static CLUSTER_RAFT_FANOUT_ACTIVE: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_FANOUT_PENDING: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_OPERATION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static PUBLISHED_NETWORK_INFO: OnceLock<Mutex<BTreeMap<String, ClusterNetworkInfoRecord>>> =
    OnceLock::new();
static PUBLISHED_NODE_STATUS: OnceLock<Mutex<BTreeMap<String, ClusterNodeStatusRecord>>> =
    OnceLock::new();
static PUBLISHED_CONNECTIVITY: OnceLock<Mutex<BTreeMap<String, PublishedConnectivitySourceCache>>> =
    OnceLock::new();

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

fn lock_cluster_raft_operation() -> Result<std::sync::MutexGuard<'static, ()>, DaemonError> {
    CLUSTER_RAFT_OPERATION_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|error| DaemonError::Parse {
            detail: format!("cluster raft operation lock poisoned: {error}"),
        })
}

fn locked_cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, DaemonError> {
    let _raft_guard = lock_cluster_raft_operation()?;
    sinomo_network::cluster_raft_known_leader_id(paths, state).map_err(Into::into)
}

fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, DaemonError> {
    let _raft_guard = lock_cluster_raft_operation()?;
    sinomo_network::cluster_raft_local_is_leader(paths, state).map_err(Into::into)
}

fn locked_restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, DaemonError> {
    let _raft_guard = lock_cluster_raft_operation()?;
    sinomo_network::restore_cluster_raft_from_snapshot_if_newer(paths, node_id, snapshot)
        .map_err(Into::into)
}

fn cached_network_info_matches(paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) -> bool {
    let key = publish_cache_key(paths, &info.node_id);
    let now = OffsetDateTime::now_utc();
    PUBLISHED_NETWORK_INFO
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|cache| {
            cache.get(&key).is_some_and(|known| {
                sinomo_network::cluster_network_info_stable_eq(known, info)
                    && !sinomo_network::cluster_network_record_should_refresh(known.expires_at, now)
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
            cache
                .get(&key)
                .is_some_and(|known| sinomo_network::cluster_node_status_stable_eq(known, status))
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

#[cfg(test)]
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
                    sinomo_network::cluster_connectivity_stable_eq(known, record)
                        && !sinomo_network::cluster_network_record_should_refresh(
                            known.expires_at,
                            now,
                        )
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
                    sinomo_network::cluster_connectivity_stable_eq(known, record)
                        && !sinomo_network::cluster_network_record_should_refresh(
                            known.expires_at,
                            now,
                        )
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

fn remember_connectivity_records(paths: &ClusterPaths, records: &[ClusterConnectivityRecord]) {
    if let Ok(mut cache) = PUBLISHED_CONNECTIVITY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        for record in records {
            let source_prefix = connectivity_cache_source_prefix(paths, &record.source_node_id);
            {
                ensure_published_connectivity_source(&mut cache, source_prefix.clone());
                let source = cache
                    .get_mut(&source_prefix)
                    .expect("published connectivity source was inserted");
                let key = connectivity_cache_key(paths, record);
                if !source.records.contains_key(&key)
                    && source.records.len() >= PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS_PER_SOURCE
                {
                    source.records.clear();
                }
                source.records.insert(key, record.clone());
            }
            trim_published_connectivity_cache_records(&mut cache, &source_prefix);
        }
    }
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

fn remember_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
    latency_ms: Option<u64>,
) {
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let previous = sinomo_network::read_cluster_connectivity_graph(paths)
        .unwrap_or_default()
        .into_iter()
        .find(|record| {
            record.source_node_id == state.local_node_id
                && record.target_node_id == endpoint.node_id
                && record.endpoint_id == endpoint.endpoint_id
        });
    let consecutive_failures = if matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Healthy
    ) {
        0
    } else {
        previous
            .as_ref()
            .map(|record| record.consecutive_failures.saturating_add(1))
            .unwrap_or(1)
            .min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD)
    };
    let last_success_at = if matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Healthy
    ) {
        Some(timestamp.clone())
    } else {
        previous.and_then(|record| record.last_success_at)
    };
    let last_failure_at = if matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Healthy
    ) {
        None
    } else {
        Some(timestamp)
    };
    let record = sinomo_network::cluster_connectivity_record(
        state,
        endpoint,
        connectivity_state,
        latency_ms,
        consecutive_failures,
        last_success_at,
        last_failure_at,
        now,
        CLUSTER_CONNECTIVITY_TTL_SECONDS,
    );
    if sinomo_network::write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )
    .is_ok()
    {
        remember_connectivity_records(paths, &[record]);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthRow {
    pub member: ClusterMemberRecord,
    pub status: ClusterNodeStatusRecord,
    pub availability: ClusterNodeAvailability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDoctorFinding {
    pub severity: String,
    pub code: String,
    pub detail: String,
}

impl ClusterDoctorFinding {
    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: "warning".to_string(),
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRaftRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterMemberRole,
    pub previous_state: ClusterMemberState,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMemberRemoval {
    pub removed: Vec<ClusterMemberRecord>,
}

pub fn cluster_paths_from_daemon_paths(paths: &DaemonPaths) -> Result<ClusterPaths, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    Ok(ClusterPaths::new(&SinomoDataDir::new(data_dir)))
}

pub fn local_daemon_cluster_options(
    probe_timeout: Duration,
) -> Result<(DaemonRunOptions, DaemonRunOptions), DaemonError> {
    let main = match default_main_daemon_status(probe_timeout)? {
        Some(status) => daemon_options_from_status(&status),
        None => DaemonRunOptions::default(),
    };
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let shadow_paths = DaemonPaths::for_shadow(&data_dir);
    let shadow = match probe_daemon_status(&shadow_paths, probe_timeout) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    };
    Ok((main, shadow))
}

pub fn start_daemon_iroh_proxy_if_clustered(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    cluster: Option<&DaemonClusterStatus>,
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
    let thread_name = format!("sinomo-{label}-iroh");
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let Some(data_dir) = paths.root.parent() else {
                eprintln!(
                    "sinomo {label} iroh proxy disabled: daemon paths have no data directory parent"
                );
                return;
            };
            let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
            let daemon_address = daemon_connect_address(&options.host, options.port);
            let mut proxy_started = false;
            let mut next_republish = Instant::now();
            loop {
                if cluster.is_none() {
                    cluster = daemon_cluster_status_from_paths(&paths).ok().flatten();
                    if cluster.is_none() {
                        let retry_delay = if cluster_paths.state.exists() {
                            Duration::from_secs(5)
                        } else {
                            DAEMON_CLUSTER_PUBLISH_INTERVAL
                        };
                        thread::sleep(retry_delay);
                        continue;
                    }
                }
                let Some(current_cluster) = cluster.clone() else {
                    thread::sleep(DAEMON_CLUSTER_PUBLISH_INTERVAL);
                    continue;
                };
                if !proxy_started {
                    match sinomo_network::start_iroh_daemon_proxy(
                        cluster_paths.clone(),
                        current_cluster.cluster_id.clone(),
                        current_cluster.node_id.clone(),
                        daemon_address.clone(),
                    ) {
                        Ok(record) => {
                            let endpoint = record.endpoint();
                            if let Err(error) = advertise_daemon_iroh_endpoint(&paths, record) {
                                eprintln!(
                                    "sinomo {label} iroh endpoint advertisement failed: {error}"
                                );
                            } else {
                                println!("sinomo {label} iroh endpoint: {}", endpoint.address);
                            }
                            proxy_started = true;
                            next_republish = Instant::now() + DAEMON_CLUSTER_PUBLISH_INTERVAL;
                        }
                        Err(error) if error.to_string().contains("without iroh-transport") => {
                            eprintln!("sinomo {label} iroh proxy disabled: {error}");
                            break;
                        }
                        Err(error) => {
                            eprintln!("sinomo {label} iroh proxy unavailable, retrying: {error}");
                            thread::sleep(Duration::from_secs(5));
                        }
                    }
                    continue;
                }
                if Instant::now() >= next_republish {
                    if let Err(error) = refresh_daemon_iroh_repair_route(&paths) {
                        eprintln!("sinomo {label} iroh repair route publish failed: {error}");
                    } else if let Some(updated) =
                        daemon_cluster_status_from_paths(&paths).ok().flatten()
                    {
                        cluster = Some(updated);
                    }
                    next_republish = Instant::now() + DAEMON_CLUSTER_PUBLISH_INTERVAL;
                }
                thread::sleep(Duration::from_secs(5));
            }
        })
        .map(|_| ())
        .map_err(|source| DaemonError::Io {
            path: "spawn iroh proxy thread".to_string(),
            detail: source.to_string(),
        })
}

pub fn daemon_cluster_status_from_paths(
    paths: &DaemonPaths,
) -> Result<Option<DaemonClusterStatus>, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
    Ok(status.cluster)
}

pub fn refresh_daemon_iroh_repair_route(paths: &DaemonPaths) -> Result<bool, DaemonError> {
    let Some(cluster) = daemon_cluster_status_from_paths(paths)? else {
        return Ok(false);
    };
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    match read_local_iroh_endpoint_record(&cluster_paths, &cluster.node_id) {
        Ok(record) => {
            advertise_daemon_iroh_endpoint(paths, record)?;
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

pub fn start_shadow_daemon_status_publisher(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let status_paths = paths.clone();
    thread::Builder::new()
        .name("sinomo-shadow-status".to_string())
        .spawn(move || {
            let iroh_disabled = sinomo_network::iroh_disabled_by_env();
            loop {
                thread::sleep(DAEMON_CLUSTER_PUBLISH_INTERVAL);
                if !iroh_disabled {
                    if let Err(error) = refresh_daemon_iroh_repair_route(&status_paths) {
                        eprintln!("sinomo shadow daemon iroh repair route tick failed: {error}");
                    }
                }
                if let Err(error) = refresh_daemon_node_status_in_kv(&status_paths) {
                    eprintln!("sinomo shadow daemon cluster status tick failed: {error}");
                }
            }
        })
        .map(|_| ())
        .map_err(|source| DaemonError::Io {
            path: "spawn shadow status thread".to_string(),
            detail: source.to_string(),
        })
}

pub fn advertise_daemon_iroh_endpoint(
    paths: &DaemonPaths,
    record: IrohEndpointRecord,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let endpoint = record.endpoint();
    let persisted_endpoints = state
        .members
        .iter()
        .find(|member| member.node_id == endpoint.node_id)
        .map(|member| member.endpoints.clone());
    prepare_local_cluster_state(&cluster_paths, &mut state)?;
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)
    {
        sinomo_network::upsert_member_endpoint(member, endpoint);
        sinomo_network::cleanup_member_iroh_endpoints(member);
        // Persist/replicate only when this member's endpoint set actually differs
        // from disk. prepare_local_cluster_state already folds iroh endpoints in
        // from disk, so we compare against the persisted endpoints rather than the
        // upsert delta: otherwise a brand-new endpoint that prepare merged would
        // never be written, while an idle re-advertise still short-circuits here
        // without touching disk, the KV mirror, or the raft log.
        if persisted_endpoints.as_deref() == Some(member.endpoints.as_slice()) {
            return Ok(());
        }
        let published_member = member.clone();
        state.updated_at = OffsetDateTime::now_utc();
        sinomo_network::write_cluster_state(&cluster_paths, &state)?;
        if let Err(error) = sync_cluster_state_to_kv(&cluster_paths, &state) {
            eprintln!("sinomo iroh endpoint cluster state publication failed: {error}");
        }
        let mut commit_state = state.clone();
        let publish_result = commit_cluster_kv_txn_with_quorum(
            &cluster_paths,
            &mut commit_state,
            sinomo_network::cluster_member_publish_txn_from_kv(
                &cluster_paths,
                &[published_member],
            )?,
        );
        if let Err(error) = publish_result {
            eprintln!("sinomo iroh endpoint local quorum publication failed: {error}");
            if let Err(error) = publish_local_members_via_voter(&cluster_paths, &state) {
                eprintln!("sinomo iroh endpoint voter publication failed: {error}");
            }
        }
    }
    Ok(())
}

/// True when the cluster has at least one member that is not this node's own
/// main/shadow process and has not been removed. Used to short-circuit periodic
/// publishes/probes on a solo node, where they would have no audience.
pub fn cluster_has_remote_peers(state: &ClusterBootstrapState) -> bool {
    state.members.iter().any(|member| {
        !matches!(member.state, ClusterMemberState::Removed)
            && !sinomo_network::cluster_member_is_local_process(state, member)
    })
}

pub fn refresh_local_iroh_endpoints_from_disk(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<bool, DaemonError> {
    let mut changed = false;
    let local_nodes = [
        state.local_node_id.clone(),
        shadow_daemon_node_id(&state.local_node_id),
    ];
    for node_id in local_nodes {
        let Ok(record) = read_local_iroh_endpoint_record(paths, &node_id) else {
            continue;
        };
        let endpoint = record.endpoint();
        let Some(member) = state
            .members
            .iter_mut()
            .find(|member| member.node_id == endpoint.node_id)
        else {
            continue;
        };
        changed |= sinomo_network::upsert_member_endpoint(member, endpoint);
        changed |= sinomo_network::cleanup_member_iroh_endpoints(member);
    }
    if changed {
        state.updated_at = OffsetDateTime::now_utc();
    }
    Ok(changed)
}

pub fn handle_daemon_cluster_kv_snapshot(paths: &DaemonPaths) -> Result<KvSnapshot, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::read_cluster_kv_store(&cluster_paths)
        .map(|store| store.snapshot())
        .map_err(Into::into)
}

pub fn prune_local_cluster_raft_uncommitted_tail(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
        return Ok(());
    };
    let _raft_guard = lock_cluster_raft_operation()?;
    sinomo_network::discard_cluster_raft_uncommitted_tail(&cluster_paths, &state.local_node_id)?;
    Ok(())
}

pub fn handle_daemon_cluster_kv_range_prefix(
    paths: &DaemonPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::read_cluster_kv_prefix_entries_limited(
        &cluster_paths,
        prefix,
        sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )
    .map_err(Into::into)
}

pub fn handle_daemon_cluster_kv_txn_request(
    paths: &DaemonPaths,
    txn: KvTxn,
) -> Result<KvTxnResponse, DaemonError> {
    handle_daemon_cluster_kv_txn_request_with_forwarding(paths, txn, true)
}

pub fn handle_forwarded_daemon_cluster_kv_txn_request(
    paths: &DaemonPaths,
    txn: KvTxn,
) -> Result<KvTxnResponse, DaemonError> {
    handle_daemon_cluster_kv_txn_request_with_forwarding(paths, txn, false)
}

fn handle_daemon_cluster_kv_txn_request_with_forwarding(
    paths: &DaemonPaths,
    txn: KvTxn,
    allow_forwarding: bool,
) -> Result<KvTxnResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state_without_kv(&cluster_paths).map_err(
        |error| match error {
            sinomo_network::NetworkError::ClusterOrphan => DaemonError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other.into(),
        },
    )?;
    let (snapshot, response) = commit_cluster_kv_txn_with_quorum_response_with_forwarding(
        &cluster_paths,
        &mut state,
        txn,
        allow_forwarding,
    )?;
    sinomo_network::write_cluster_kv_snapshot_if_changed(&cluster_paths, &snapshot)?;
    Ok(response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: snapshot.revision,
        responses: Vec::new(),
    }))
}

pub fn handle_daemon_cluster_join_request(
    paths: &DaemonPaths,
    request: ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state(&cluster_paths)?;
    ensure_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        time::OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(&cluster_paths, &mut state)?;
    let persisted_invite =
        sinomo_network::read_cluster_invitation_from_kv(&cluster_paths, &request.invite.invite_id)?;
    if request.invite.cluster_public_key != persisted_invite.cluster_public_key
        || request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "invitation rejected".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    if let Err(error) = sinomo_network::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if let Ok(main_status) = read_daemon_status(paths) {
        let main_options = daemon_options_from_status(&main_status);
        let shadow_options = match probe_daemon_status(
            &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
            DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT,
        ) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => daemon_options_from_status(&status),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                DaemonRunOptions::shadow_default()
            }
        };
        ensure_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            time::OffsetDateTime::now_utc(),
        );
        assign_local_node_public_keys(&cluster_paths, &mut state)?;
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                persisted_invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if persisted_invite.expires_at <= time::OffsetDateTime::now_utc() {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                persisted_invite
                    .expires_at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request
        .members
        .iter()
        .any(|member| !persisted_invite.allowed_member_roles.contains(&member.role))
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "join request includes a member role not allowed by the invite".to_string(),
            state,
            kv_snapshot: None,
        });
    }

    if !sinomo_network::cluster_raft_local_is_voter(&state) {
        // This introducer is a non-voter (e.g. a bridge learner) and may not register
        // members itself. Forward the validated join to a reachable voter -- relayed across
        // segments by the routing-aware send -- so the voter performs the authoritative
        // registration. This is what lets a node join through a reachable bridge.
        return forward_cluster_join_registration_to_voter(&cluster_paths, &state, &request);
    }

    let timestamp = time::OffsetDateTime::now_utc();
    let mut accepted_state = state.clone();
    for mut member in request.members {
        member.cluster_id = accepted_state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = timestamp;
        sinomo_network::upsert_member_record(&mut accepted_state, member);
    }
    accepted_state.kv_revision = accepted_state.kv_revision.saturating_add(1);
    accepted_state.updated_at = timestamp;
    let snapshot = sync_cluster_state_to_kv(&cluster_paths, &accepted_state)?;
    sinomo_network::write_cluster_state(&cluster_paths, &accepted_state)?;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: accepted_state,
        kv_snapshot: Some(snapshot),
    })
}

/// Forward a validated join registration to a reachable voter (the only node authorized to
/// register members) via the routing-aware send, which relays it across network segments
/// when no voter is directly reachable. Used when this introducer is not itself a voter.
fn forward_cluster_join_registration_to_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, DaemonError> {
    let rpc_request = sinomo_network::ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let mut failures = Vec::new();
    for voter in cluster_raft_live_voter_targets(paths, state) {
        match crate::send_daemon_rpc_to_endpoint(&voter, &rpc_request, DAEMON_CLUSTER_RELAY_TIMEOUT)
        {
            Ok(response) => return response.decode_body().map_err(DaemonError::from),
            Err(error) => failures.push(format!(
                "{} ({:?}): {error}",
                voter.address, voter.transport
            )),
        }
    }
    Err(DaemonError::ClusterReadOnly {
        detail: format!(
            "join introducer is not a voter and could not forward the join to any voter{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

/// A bridge introducer named in an invite minted before it joined is not itself a
/// proof-capable channel/issuer, so it cannot answer a join challenge. Relay the challenge to
/// a reachable voter (the invite issuer can produce an issuer-signed proof) and return that
/// proof. The routing-aware send relays the challenge across segments when no voter is
/// directly reachable, so e.g. node4 (reaching the voter only via node3) can still serve a
/// challenge for node5/node6.
pub fn forward_cluster_invite_challenge_to_voter(
    paths: &DaemonPaths,
    challenge: &ClusterInviteChallengeRequest,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let rpc_request = sinomo_network::ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_JOIN_CHALLENGE_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        challenge,
    )?;
    let mut failures = Vec::new();
    for voter in cluster_raft_live_voter_targets(&cluster_paths, &state) {
        match crate::send_daemon_rpc_to_endpoint(&voter, &rpc_request, DAEMON_CLUSTER_RELAY_TIMEOUT)
        {
            Ok(response) => return response.decode_body().map_err(DaemonError::from),
            Err(error) => failures.push(format!(
                "{} ({:?}): {error}",
                voter.address, voter.transport
            )),
        }
    }
    Err(DaemonError::ClusterReadOnly {
        detail: format!(
            "join challenge introducer is not a channel/issuer and could not relay the challenge to any voter{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn reconcile_cluster_join_with_introducers(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    tcp_timeout: Duration,
    iroh_timeout: Duration,
) -> Result<ClusterJoinReconciliation, DaemonError> {
    if invite.communication_channels.is_empty() && invite.introducers.is_empty() {
        return Ok(ClusterJoinReconciliation {
            accepted_endpoint: None,
            kv_snapshot: None,
        });
    }
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite: invite.clone(),
        members: state.members.clone(),
    };
    let deadline = Instant::now() + tcp_timeout.max(iroh_timeout);
    let remaining_for = |channel_timeout: Duration| {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_millis(1));
        remaining.min(channel_timeout)
    };
    let mut failures = Vec::new();
    for channel in invite_channels_for_join_registration(&request.invite) {
        let tcp_remaining = remaining_for(tcp_timeout);
        let iroh_remaining = remaining_for(iroh_timeout);
        match send_cluster_join_registration_to_channel(
            &channel.endpoint,
            &request,
            tcp_remaining,
            iroh_remaining,
        ) {
            Ok(response) => {
                if response.accepted {
                    let kv_snapshot = response.kv_snapshot;
                    sinomo_network::merge_cluster_join_registration(state, response.state);
                    return Ok(ClusterJoinReconciliation {
                        accepted_endpoint: Some(channel.endpoint),
                        kv_snapshot,
                    });
                }
                failures.push(format!(
                    "{} ({:?}) rejected join: {}",
                    channel.endpoint.address, channel.endpoint.transport, response.detail
                ));
            }
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                channel.endpoint.address, channel.endpoint.transport, error
            )),
        }
    }
    if !failures.is_empty() {
        return Err(DaemonError::Network {
            address: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {}. If the invite only contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`.",
                failures.join("; ")
            ),
        });
    }
    Ok(ClusterJoinReconciliation {
        accepted_endpoint: None,
        kv_snapshot: None,
    })
}

pub fn publish_local_members_via_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), DaemonError> {
    let members = local_process_members(state);
    if members.is_empty() {
        return Ok(());
    }
    let request = ClusterMemberPublishRequest { members };
    let mut failures = Vec::new();
    for endpoint in sinomo_network::cluster_voter_command_targets(paths, state) {
        match send_cluster_member_publish(
            &endpoint,
            &request,
            DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
        ) {
            Ok(response) if response.accepted => {
                if let Some(snapshot) = response.kv_snapshot {
                    sinomo_network::write_cluster_kv_snapshot(paths, &snapshot)?;
                }
                return Ok(());
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    Err(DaemonError::ClusterReadOnly {
        detail: format!(
            "local node is not a voter and no reachable voter accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn publish_local_members_to_quorum_if_needed(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let members = local_process_members(state);
    if members.is_empty() {
        return Ok(None);
    }
    if sinomo_network::cluster_member_publish_matches_kv(paths, &members).unwrap_or(false) {
        return Ok(None);
    }
    if sinomo_network::cluster_raft_local_is_voter(state) && cluster_has_local_quorum(paths, state)
    {
        let mut commit_state = state.clone();
        return commit_cluster_kv_txn_with_quorum(
            paths,
            &mut commit_state,
            sinomo_network::cluster_member_publish_txn_from_kv(paths, &members)?,
        )
        .map(Some);
    }
    publish_local_members_via_voter(paths, state)?;
    Ok(None)
}

fn local_process_members(state: &ClusterBootstrapState) -> Vec<ClusterMemberRecord> {
    state
        .members
        .iter()
        .filter(|member| sinomo_network::cluster_member_is_local_process(state, member))
        .cloned()
        .collect()
}

#[derive(Debug)]
pub struct ClusterNetworkMaintenance {
    pub discovery: Option<MdnsDiscoverySocket>,
    pub discovery_cache: MdnsDiscoveryCache,
    pub connectivity: std::collections::BTreeMap<String, ClusterConnectivityProbeState>,
    pub connectivity_cursor: usize,
    pub environment_id: Option<String>,
    pub last_desired_rollout_attempt: Option<String>,
    pub tick: u64,
    pub last_publish: Instant,
    pub last_discovery: Instant,
    pub last_probe: Instant,
    pub idle_stats: Arc<IdleStatsAtomics>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeState {
    pub record: ClusterConnectivityRecord,
}

fn maintenance_interval_due(now: Instant, last: Instant, interval: Duration) -> bool {
    now.saturating_duration_since(last) >= interval
}

fn maintenance_last_timestamp_for_next_due(
    now: Instant,
    interval: Duration,
    delay: Duration,
) -> Instant {
    now.checked_add(delay.saturating_sub(interval))
        .unwrap_or(now)
}

impl ClusterNetworkMaintenance {
    pub fn new() -> Self {
        Self::with_idle_stats(Arc::new(IdleStatsAtomics::new()))
    }

    pub fn with_idle_stats(idle_stats: Arc<IdleStatsAtomics>) -> Self {
        let now = Instant::now();
        Self {
            discovery: MdnsDiscoverySocket::bind_default().ok(),
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 0,
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

    pub fn tick(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
    ) -> Result<(), DaemonError> {
        self.tick = self.tick.saturating_add(1);
        self.idle_stats.incr_ticks_total();
        let now = Instant::now();
        let publish_due =
            maintenance_interval_due(now, self.last_publish, DAEMON_CLUSTER_PUBLISH_INTERVAL);
        let probe_due =
            maintenance_interval_due(now, self.last_probe, DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL);
        let discovery_due = self.discovery.is_some()
            && maintenance_interval_due(
                now,
                self.last_discovery,
                DAEMON_CLUSTER_DISCOVERY_INTERVAL,
            );
        if !(publish_due || probe_due || discovery_due) {
            return Ok(());
        }

        let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        if !cluster_paths.state.exists() {
            if publish_due {
                self.last_publish = now;
            }
            if probe_due {
                self.last_probe = now;
            }
            if discovery_due {
                self.last_discovery = now;
            }
            return Ok(());
        }

        self.idle_stats.incr_ticks_with_work();
        self.idle_stats.incr_disk_reads_cluster_state();
        let mut state = if publish_due || probe_due {
            sinomo_network::read_cluster_state(&cluster_paths)?
        } else {
            sinomo_network::read_cluster_state_without_kv(&cluster_paths)?
        };
        let state_before_maintenance = state.clone();
        let mut discovered = false;

        if discovery_due {
            self.last_discovery = now;
            discovered = self.process_discovery(&mut state);
            self.announce(&state);
        }

        if publish_due || probe_due {
            sinomo_network::cleanup_cluster_iroh_endpoints(&mut state);
            let shadow_options =
                current_shadow_daemon_options(data_dir, DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT);
            let mut local_state_changed = ensure_daemon_cluster_members(
                &mut state,
                options,
                &shadow_options,
                time::OffsetDateTime::now_utc(),
            );
            let state_before_keys = state.clone();
            assign_local_node_public_keys(&cluster_paths, &mut state)?;
            local_state_changed |=
                !sinomo_network::cluster_bootstrap_state_stable_eq(&state_before_keys, &state);
            self.idle_stats.incr_disk_reads_iroh_endpoints();
            local_state_changed |=
                refresh_local_iroh_endpoints_from_disk(&cluster_paths, &mut state)?;
            if local_state_changed {
                sinomo_network::write_cluster_state(&cluster_paths, &state)?;
            }
        }

        // When the cluster has no remote peers (every member is this node's own
        // main/shadow process) and we are not mid-join, the periodic publishes and
        // connectivity probes have no audience: they only exist so *other* nodes can
        // read our liveness/endpoints. Skip them — this removes the node-status
        // heartbeat that otherwise rewrites the whole KV snapshot every cycle. The
        // timers are still advanced so we re-evaluate (cheaply) once per interval,
        // and a peer joining via the join RPC adds a member, which flips this gate
        // and resumes publishing on the next tick.
        let cluster_is_solo = !cluster_has_remote_peers(&state)
            && !sinomo_network::cluster_state_needs_join_registration(&state);

        if publish_due {
            self.last_publish = now;
            let mut publish_failed = false;
            if cluster_is_solo {
                // no-op: nothing to publish to absent peers
            } else if sinomo_network::cluster_raft_local_is_voter(&state) {
                if let Err(error) = ensure_default_distributed_dfs(paths, options, true) {
                    eprintln!("sinomo daemon distributed DFS control publish failed: {error}");
                    publish_failed = true;
                }
                if !sinomo_network::cluster_bootstrap_state_stable_eq(
                    &state_before_maintenance,
                    &state,
                ) {
                    if let Err(error) = sync_cluster_state_to_kv(&cluster_paths, &state) {
                        eprintln!("sinomo daemon cluster state publish failed: {error}");
                        publish_failed = true;
                    }
                }
                self.idle_stats.incr_kv_publish_member();
                if let Err(error) =
                    publish_local_members_to_quorum_if_needed(&cluster_paths, &state)
                {
                    eprintln!("sinomo daemon cluster member publish failed: {error}");
                    publish_failed = true;
                }
                // Track environment changes for connectivity-cache invalidation.
                // The network-info record itself is published (with dedup) by
                // refresh_daemon_node_status_in_kv below, so we do not write it
                // here — a second unconditional write was pure churn that rewrote
                // the KV snapshot every publish cycle even when nothing changed.
                let local_environment_id = sinomo_network::build_cluster_network_info_record(
                    &state,
                    &state.local_node_id,
                    time::OffsetDateTime::now_utc(),
                    CLUSTER_NETWORK_INFO_TTL_SECONDS,
                )
                .environment_id;
                if self
                    .environment_id
                    .as_ref()
                    .is_some_and(|known| known != &local_environment_id)
                {
                    self.connectivity.clear();
                }
                self.environment_id = Some(local_environment_id);
                match refresh_daemon_node_status_in_kv(paths) {
                    Ok(published) => self.record_node_status_publish(&published),
                    Err(error) => {
                        eprintln!("sinomo daemon node status publish failed: {error}");
                        publish_failed = true;
                    }
                }
                let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
                if matches!(
                    probe_daemon_status(&shadow_paths, DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT),
                    DaemonProbeStatus::Running(_)
                ) {
                    match refresh_daemon_node_status_in_kv(&shadow_paths) {
                        Ok(published) => self.record_node_status_publish(&published),
                        Err(error) => {
                            eprintln!("sinomo daemon shadow status publish failed: {error}");
                            publish_failed = true;
                        }
                    }
                }
                if daemon_paths_are_shadow(paths)
                    && let Err(error) =
                        self.reconcile_desired_rollout(paths, &cluster_paths, &state)
                {
                    eprintln!("sinomo daemon desired rollout reconcile failed: {error}");
                }
            } else if !sinomo_network::cluster_raft_local_is_voter(&state) {
                self.idle_stats.incr_kv_publish_member();
                if let Err(error) = publish_local_members_via_voter(&cluster_paths, &state) {
                    eprintln!("sinomo daemon cluster member publish via voter failed: {error}");
                    publish_failed = true;
                }
            }
            if publish_failed {
                self.last_publish = maintenance_last_timestamp_for_next_due(
                    now,
                    DAEMON_CLUSTER_PUBLISH_INTERVAL,
                    DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF,
                );
            }
        }

        if probe_due {
            self.last_probe = now;
            if !cluster_is_solo {
                let records = self.probe_connectivity(&cluster_paths, &state);
                match sync_cluster_connectivity_records_to_kv(
                    &cluster_paths,
                    &state.local_node_id,
                    records,
                ) {
                    Ok(Some(_)) => self.idle_stats.incr_kv_publish_connectivity(),
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("sinomo daemon cluster connectivity publish failed: {error}")
                    }
                }
            }
        }

        if discovered
            || !sinomo_network::cluster_bootstrap_state_stable_eq(&state_before_maintenance, &state)
        {
            sinomo_network::write_cluster_state(&cluster_paths, &state)?;
        }
        Ok(())
    }

    fn reconcile_desired_rollout(
        &mut self,
        paths: &DaemonPaths,
        cluster_paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), DaemonError> {
        let Some(desired) = sinomo_network::read_cluster_rollout_desired_from_kv(cluster_paths)?
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
        let changed = self.apply_discovery_advertisements(state, advertisements);
        self.discovery_cache.expire_after_tick(self.tick);
        changed
    }

    fn apply_discovery_advertisements(
        &mut self,
        state: &mut ClusterBootstrapState,
        advertisements: Vec<MdnsDiscoveryAdvertisement>,
    ) -> bool {
        let mut changed = false;
        for advertisement in advertisements {
            if advertisement.cluster_id != state.cluster_id
                || advertisement.node_id == state.local_node_id
            {
                continue;
            }
            for record in advertisement.records() {
                self.discovery_cache.upsert(record.clone());
                if let Some(member) = state
                    .members
                    .iter_mut()
                    .find(|member| member.node_id == record.endpoint.node_id)
                {
                    changed |= sinomo_network::upsert_member_endpoint(member, record.endpoint);
                }
            }
        }
        changed
    }

    fn announce(&self, state: &ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(member) = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
        else {
            return;
        };
        let endpoints: Vec<_> = member
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.privacy == ClusterEndpointPrivacy::Lan
                    && matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                    )
            })
            .cloned()
            .collect();
        if endpoints.is_empty() {
            return;
        }
        let advertisement = MdnsDiscoveryAdvertisement::new(
            state.cluster_id.clone(),
            state.local_node_id.clone(),
            endpoints,
            self.tick,
        );
        self.idle_stats.incr_mdns_announces_sent();
        let _ = discovery.announce(&advertisement);
    }

    fn probe_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Vec<ClusterConnectivityRecord> {
        let now = time::OffsetDateTime::now_utc();
        let timestamp = now
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string());
        let targets = sinomo_network::cluster_network_probe_targets(state);
        let active_keys = targets
            .iter()
            .map(|endpoint| {
                sinomo_network::cluster_connectivity_probe_key(&state.local_node_id, endpoint)
            })
            .collect::<std::collections::BTreeSet<_>>();
        // Seed the in-memory probe cache from the merged graph (KV + the local probe store,
        // local-authoritative for our own edges) rather than KV alone: a learner cannot commit
        // its connectivity edges to KV, so seeding from KV would discard our own last-known
        // reachability and force a full re-convergence on every restart.
        if self.connectivity.is_empty()
            && let Ok(records) = sinomo_network::read_cluster_connectivity_graph(paths)
        {
            for record in records
                .into_iter()
                .filter(|record| record.source_node_id == state.local_node_id)
            {
                let key = sinomo_network::cluster_connectivity_key(
                    &record.source_node_id,
                    &record.target_node_id,
                    &record.endpoint_id,
                );
                if !active_keys.contains(&key) {
                    continue;
                }
                self.connectivity
                    .insert(key, ClusterConnectivityProbeState { record });
            }
        }
        let refresh_deadline =
            now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
        let probe_targets = targets
            .iter()
            .filter(|endpoint| {
                let key =
                    sinomo_network::cluster_connectivity_probe_key(&state.local_node_id, endpoint);
                self.connectivity.get(&key).is_none_or(|probe| {
                    probe.record.expires_at <= refresh_deadline
                        || (probe.record.state != sinomo_network::ClusterConnectivityState::Healthy
                            && probe.record.consecutive_failures
                                < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD)
                        // Re-probe promptly after ANY recent failure, including a link still
                        // reported Healthy under the single-failure grace below, so a real
                        // outage is confirmed (and drops out of routing) within a couple ticks.
                        || probe.record.consecutive_failures > 0
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let probe_count = probe_targets
            .len()
            .min(CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK);
        let probe_start = if probe_targets.is_empty() {
            0
        } else {
            self.connectivity_cursor % probe_targets.len()
        };
        self.connectivity_cursor = if probe_targets.is_empty() {
            0
        } else {
            (probe_start + probe_count) % probe_targets.len()
        };
        self.idle_stats
            .add_connectivity_probes_sent(probe_count as u64);
        // Snapshot the batch (each endpoint + its previous record), then probe every
        // due endpoint in PARALLEL. Probing one endpoint per 60s tick (the old serial
        // cap) made the connectivity graph take O(endpoints x 60s) to converge after a
        // restart/join, so the cluster churned for minutes before going idle. Scoped
        // parallel probes converge it in a single tick; the tick's wall time stays at
        // ~one probe timeout regardless of how many endpoints are due.
        let batch: Vec<(
            String,
            ClusterEndpointRecord,
            Option<ClusterConnectivityRecord>,
        )> = probe_targets
            .iter()
            .cycle()
            .skip(probe_start)
            .take(probe_count)
            .map(|endpoint| {
                let key =
                    sinomo_network::cluster_connectivity_probe_key(&state.local_node_id, endpoint);
                let previous = self
                    .connectivity
                    .get(&key)
                    .map(|probe| probe.record.clone());
                (key, endpoint.clone(), previous)
            })
            .collect();
        let timestamp = timestamp.as_str();
        let probed: Vec<(String, ClusterConnectivityRecord)> = std::thread::scope(|scope| {
            let handles: Vec<_> = batch
                .iter()
                .map(|(key, endpoint, previous)| {
                    scope.spawn(move || {
                        let started = Instant::now();
                        let result = fetch_daemon_status_from_endpoint_direct(
                            endpoint,
                            CLUSTER_HEALTH_PROBE_TIMEOUT,
                        );
                        let latency_ms =
                            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        let record = match result {
                            Ok(status)
                                if daemon_cluster_status_matches(state, endpoint, &status) =>
                            {
                                sinomo_network::cluster_connectivity_record(
                                    state,
                                    endpoint,
                                    sinomo_network::ClusterConnectivityState::Healthy,
                                    Some(latency_ms),
                                    0,
                                    Some(timestamp.to_string()),
                                    previous
                                        .as_ref()
                                        .and_then(|record| record.last_failure_at.clone()),
                                    now,
                                    CLUSTER_CONNECTIVITY_TTL_SECONDS,
                                )
                            }
                            _ => {
                                let observed_failures = previous
                                    .as_ref()
                                    .map(|record| record.consecutive_failures.saturating_add(1))
                                    .unwrap_or(1);
                                let retained_success = previous.as_ref().and_then(|record| {
                                    if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
                                        record.last_success_at.clone()
                                    } else {
                                        None
                                    }
                                });
                                let previously_healthy = previous.as_ref().is_some_and(|record| {
                                    record.state
                                        == sinomo_network::ClusterConnectivityState::Healthy
                                });
                                let state_label = if previously_healthy && observed_failures == 1 {
                                    // Single-failure grace: a confirmed-healthy link survives one
                                    // transient probe failure as Healthy (stays routable through a
                                    // blip). The probe-selection clause re-probes it next tick, so
                                    // a real outage still drops it to Degraded within two cycles. A
                                    // never-confirmed link gets no grace.
                                    sinomo_network::ClusterConnectivityState::Healthy
                                } else if observed_failures
                                    >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
                                {
                                    sinomo_network::ClusterConnectivityState::Unreachable
                                } else {
                                    sinomo_network::ClusterConnectivityState::Degraded
                                };
                                let failures =
                                    observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
                                let ttl_seconds = if observed_failures
                                    >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
                                {
                                    CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS
                                } else {
                                    CLUSTER_CONNECTIVITY_TTL_SECONDS
                                };
                                sinomo_network::cluster_connectivity_record(
                                    state,
                                    endpoint,
                                    state_label,
                                    previous.as_ref().and_then(|record| record.latency_ms),
                                    failures,
                                    retained_success,
                                    Some(timestamp.to_string()),
                                    now,
                                    ttl_seconds,
                                )
                            }
                        };
                        (key.clone(), record)
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().ok())
                .collect()
        });
        for (key, record) in probed {
            self.connectivity
                .insert(key, ClusterConnectivityProbeState { record });
        }

        self.connectivity
            .retain(|key, _state| active_keys.contains(key));
        self.connectivity
            .values()
            .map(|state| state.record.clone())
            .collect()
    }
}

fn fetch_daemon_status_from_endpoint_direct(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<crate::DaemonStatus, DaemonError> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let signed = crate::sign_daemon_rpc_request_for_local_node(&request)?;
    let command = crate::daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE);
    let response = sinomo_network::send_daemon_rpc_to_cluster_endpoint(
        endpoint,
        &command,
        Some(&signed),
        &request,
        timeout,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

/// Records which of the two heartbeat records `refresh_daemon_node_status_in_kv`
/// actually wrote to cluster KV, so callers can account real publishes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeStatusPublishOutcome {
    pub network_info_published: bool,
    pub node_status_published: bool,
}

pub fn refresh_daemon_node_status_in_kv(
    paths: &DaemonPaths,
) -> Result<NodeStatusPublishOutcome, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
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
    let network_info_matches =
        sinomo_network::stored_cluster_network_info_matches(&cluster_paths, &network_info)
            .unwrap_or(false);
    let node_status_matches =
        sinomo_network::stored_cluster_node_status_matches(&cluster_paths, &record)
            .unwrap_or(false);
    if network_info_matches && node_status_matches {
        return Ok(NodeStatusPublishOutcome::default());
    }
    if !cluster_has_local_quorum(&cluster_paths, &state) {
        return Ok(NodeStatusPublishOutcome::default());
    }
    // Publish each record independently so the slow-changing network-info record
    // is not rewritten on every node-status heartbeat. Node status refreshes on a
    // ~60s margin; network info on a ~9min margin. Bundling them rewrote the KV
    // snapshot far more often than necessary.
    let mut outcome = NodeStatusPublishOutcome::default();
    if !network_info_matches {
        if let Err(error) = sync_cluster_network_info_to_kv(&cluster_paths, network_info) {
            eprintln!("sinomo daemon network info publish failed: {error}");
        } else {
            outcome.network_info_published = true;
        }
    }
    if !node_status_matches {
        sync_cluster_node_status_to_kv(&cluster_paths, record)?;
        outcome.node_status_published = true;
    }
    Ok(outcome)
}

pub fn ensure_daemon_status_cluster(
    paths: &DaemonPaths,
    status: &mut crate::DaemonStatus,
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
    let process = daemon_cluster_process_from_paths(data_dir, paths);
    let (main_options, shadow_options) =
        daemon_cluster_options_from_status(data_dir, paths, status, process);
    ensure_daemon_cluster_members(
        &mut state,
        &main_options,
        &shadow_options,
        time::OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(&cluster_paths, &mut state)?;
    let _ = refresh_local_iroh_endpoints_from_disk(&cluster_paths, &mut state)?;
    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
    if let Some(cluster) = daemon_cluster_status(&state, process) {
        status.cluster = Some(cluster);
        write_daemon_status(paths, status)?;
    }
    Ok(())
}

pub fn handle_daemon_cluster_kv_watch_prefix_request(
    paths: &DaemonPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::create_cluster_kv_watch(&cluster_paths, request).map_err(Into::into)
}

pub fn handle_daemon_cluster_kv_watch_drain_request(
    paths: &DaemonPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::drain_cluster_kv_watch(&cluster_paths, request).map_err(Into::into)
}

pub fn handle_daemon_cluster_kv_watch_cancel_request(
    paths: &DaemonPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::cancel_cluster_kv_watch(&cluster_paths, request).map_err(Into::into)
}

pub fn handle_daemon_cluster_kv_watch_events_request(
    paths: &DaemonPaths,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;
    if request.state.cluster_id != state.cluster_id {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            stored_events: 0,
        });
    }
    if !sinomo_network::cluster_raft_node_is_voter(&state, &request.leader_id) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out rejected from unauthorized leader {}",
                request.leader_id
            ),
            stored_events: 0,
        });
    }
    if request.events.len() > sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out batch has {} events; max is {}",
                request.events.len(),
                sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS
            ),
            stored_events: 0,
        });
    }
    let payload_bytes = sinomo_network::cluster_kv_watch_events_payload_bytes(&request.events);
    if payload_bytes > sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out batch is {payload_bytes} payload bytes; max is {}",
                sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
            ),
            stored_events: 0,
        });
    }
    let stored_events =
        sinomo_network::append_cluster_kv_watch_events(&cluster_paths, &request.events)?;
    Ok(ClusterKvWatchEventsResponse {
        accepted: true,
        detail: "cluster kv watch events stored".to_string(),
        stored_events,
    })
}

pub fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, DaemonError> {
    let mut keyed_state = state.clone();
    assign_local_node_public_keys(paths, &mut keyed_state)?;
    refresh_local_iroh_endpoints_from_disk(paths, &mut keyed_state)?;
    sinomo_network::cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let txn = sinomo_network::cluster_state_sync_txn(paths, &keyed_state)?;
    if txn.success.is_empty() {
        // Cluster KV already reflects this state; skip the commit so we don't bump
        // the KV revision and rewrite the materialized snapshot for no reason.
        return Ok(sinomo_network::read_cluster_kv_snapshot(paths)?);
    }
    let mut commit_state = keyed_state.clone();
    commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

pub fn read_local_cluster_state(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_network::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

pub fn read_local_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DaemonError> {
    let mut state = sinomo_network::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

pub fn read_local_iroh_endpoint_record(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, DaemonError> {
    sinomo_network::read_iroh_endpoint_record(paths, node_id).map_err(Into::into)
}

pub fn read_cluster_connectivity_graph_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, DaemonError> {
    sinomo_network::read_cluster_connectivity_graph_from_kv(paths).map_err(Into::into)
}

pub fn cluster_health_rows(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterHealthRow>, DaemonError> {
    let stored_statuses = sinomo_network::read_cluster_node_statuses_from_kv(paths)?;
    let mut rows = Vec::new();
    for member in &state.members {
        if matches!(member.state, ClusterMemberState::Removed) {
            continue;
        }
        let live = probe_member_daemon_status(paths, state, member);
        let stored = stored_statuses
            .iter()
            .find(|status| status.node_id == member.node_id)
            .cloned();
        let live_online = live.is_some();
        let mut status = if let Some(live) = live {
            let mut live = live;
            if live.rollout.is_none() {
                live.rollout = stored.as_ref().and_then(|status| status.rollout.clone());
            }
            live
        } else if let Some(stored) = stored {
            stored
        } else {
            unknown_cluster_node_status(state, member, OffsetDateTime::now_utc())
        };
        // Liveness is driven by the active status probe above, not by the
        // self-reported node-status `observed_at` (which is no longer refreshed on
        // a timer). A reachable peer answers the probe; an addressable peer that
        // does not answer is Offline; one we cannot probe at all is Unknown.
        let availability = if live_online {
            ClusterNodeAvailability::Online
        } else if sinomo_network::live_cluster_member_probe_possible(state, member) {
            ClusterNodeAvailability::Offline
        } else {
            ClusterNodeAvailability::Unknown
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
        });
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    Ok(rows)
}

pub fn probe_member_daemon_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    for endpoint in
        sinomo_network::cluster_member_daemon_status_targets_with_connectivity(paths, state, member)
    {
        let Ok(status) = fetch_daemon_status_from_endpoint(&endpoint, CLUSTER_HEALTH_PROBE_TIMEOUT)
        else {
            continue;
        };
        let Some(cluster) = status.cluster.as_ref() else {
            continue;
        };
        if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
            continue;
        }
        return Some(cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        ));
    }
    None
}

pub fn cluster_doctor_findings(
    state: &ClusterBootstrapState,
    connectivity: &[ClusterConnectivityRecord],
    health: &[ClusterHealthRow],
) -> Vec<ClusterDoctorFinding> {
    let mut findings = Vec::new();
    for row in health {
        if row.availability == ClusterNodeAvailability::Offline {
            findings.push(ClusterDoctorFinding::warning(
                "node-offline",
                format!(
                    "{} is offline from {} using advertised endpoints",
                    row.member.node_id, state.local_node_id
                ),
            ));
        } else if row.availability == ClusterNodeAvailability::Unknown {
            findings.push(ClusterDoctorFinding::warning(
                "node-unknown",
                format!("{} has no recent reachable status", row.member.node_id),
            ));
        }
    }

    for record in connectivity {
        if record.state == sinomo_network::ClusterConnectivityState::Unreachable {
            findings.push(ClusterDoctorFinding::warning(
                "endpoint-unreachable",
                format!(
                    "{} cannot reach {} at {} via {:?} ({} consecutive failures)",
                    record.source_node_id,
                    record.target_node_id,
                    record.address,
                    record.transport,
                    record.consecutive_failures
                ),
            ));
        }
    }

    for member in &state.members {
        let mut by_public_key: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for endpoint in &member.endpoints {
            if let Some(public_key) = sinomo_network::iroh_endpoint_public_key(endpoint) {
                *by_public_key.entry(public_key).or_default() += 1;
            }
        }
        for (public_key, count) in by_public_key {
            if count > 1 {
                findings.push(ClusterDoctorFinding::warning(
                    "duplicate-iroh-endpoint",
                    format!(
                        "{} has {count} Iroh endpoint records for public key {public_key}",
                        member.node_id
                    ),
                ));
            }
        }
    }

    let route_targets = sinomo_network::advertised_peer_route_targets(state);
    if !route_targets.is_empty() {
        let route_ips = sinomo_network::discover_peer_route_ip_candidates(state);
        if route_ips.is_empty() {
            findings.push(ClusterDoctorFinding::warning(
                "peer-route-unadvertised",
                format!(
                    "{} peer route target(s) exist, but no routable local source address could be discovered",
                    route_targets.len()
                ),
            ));
        }
    }

    findings.sort_by(|left, right| (&left.code, &left.detail).cmp(&(&right.code, &right.detail)));
    findings
}

pub fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, DaemonError> {
    sinomo_network::read_cluster_kv_snapshot(paths).map_err(Into::into)
}

pub fn remove_local_cluster_state(paths: &ClusterPaths) -> Result<(), DaemonError> {
    sinomo_network::remove_local_cluster_state(paths).map_err(Into::into)
}

pub fn write_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, DaemonError> {
    sinomo_network::write_cluster_kv_snapshot_if_changed(paths, snapshot).map_err(Into::into)
}

pub fn write_local_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), DaemonError> {
    sinomo_network::write_cluster_state(paths, state).map_err(Into::into)
}

pub fn prepare_local_cluster_state(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), DaemonError> {
    ensure_daemon_cluster_members(
        state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        time::OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(paths, state)?;
    refresh_local_iroh_endpoints_from_disk(paths, state)?;
    Ok(())
}

pub fn initialize_local_cluster(
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
    ensure_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        now,
    );
    assign_local_node_public_keys(paths, &mut state)?;
    sinomo_network::write_cluster_state(paths, &state)?;
    {
        let _raft_guard = lock_cluster_raft_operation()?;
        sinomo_network::set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    }
    sync_cluster_state_to_kv(paths, &state)?;
    Ok(state)
}

pub fn prepare_pending_cluster_join_state(
    paths: &ClusterPaths,
    cluster_id: ClusterId,
    cluster_name: String,
    local_node_id: NodeId,
    local_node_display_name: String,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    now: OffsetDateTime,
) -> Result<ClusterBootstrapState, DaemonError> {
    paths.ensure()?;
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::pending_join(
        cluster_id,
        cluster_name,
        local_node_id,
        local_node_display_name,
        main_identity.public_key.clone(),
        now,
    );
    ensure_daemon_cluster_members(&mut state, main_options, shadow_options, now);
    assign_local_node_public_keys(paths, &mut state)?;
    Ok(state)
}

pub fn prepare_cluster_join_retry_state(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    now: OffsetDateTime,
) -> Result<ClusterBootstrapState, DaemonError> {
    ensure_daemon_cluster_members(&mut state, main_options, shadow_options, now);
    assign_local_node_public_keys(paths, &mut state)?;
    Ok(state)
}

pub fn update_cluster_node_role(
    paths: &ClusterPaths,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<ClusterNodeRoleUpdate, DaemonError> {
    let mut state = read_local_cluster_state(paths)?;
    let store = current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(DaemonError::Parse {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if changed {
        sinomo_network::apply_cluster_node_role(&mut member, role.clone(), now);
        commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn(&store, member_entry, &member)?,
        )?;
    }
    Ok(ClusterNodeRoleUpdate {
        node_id,
        previous_role,
        role,
        changed,
    })
}

pub fn update_cluster_raft_role(
    paths: &ClusterPaths,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
) -> Result<ClusterRaftRoleUpdate, DaemonError> {
    let mut state = read_local_cluster_state(paths)?;
    let store = current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(DaemonError::Parse {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = sinomo_network::cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_network::cluster_raft_config_key())
        .and_then(|entry| {
            entry
                .decode::<sinomo_network::ClusterRaftConfigRecord>()
                .ok()
        })
        .and_then(|config| cluster_raft_config_member_role(&config, &node_id));
    let changed =
        previous_role != role || previous_state != new_state || config_role.as_ref() != Some(&role);
    if changed {
        sinomo_network::apply_cluster_raft_role(&mut member, role.clone(), now);
        commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_raft_update_txn(&store, member_entry, &member, now)?,
        )?;
    }
    Ok(ClusterRaftRoleUpdate {
        node_id,
        previous_role,
        previous_state,
        role,
        state: new_state,
        changed,
    })
}

fn current_cluster_kv_store(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<InProcessKvStore, DaemonError> {
    let mut snapshots = Vec::new();
    if let Ok(snapshot) = sinomo_network::read_cluster_kv_snapshot(paths) {
        snapshots.push(snapshot);
    }
    let mut targets = cluster_raft_live_voter_targets(paths, state);
    if let Ok(Some(leader_id)) = locked_cluster_raft_known_leader_id(paths, state)
        && !cluster_node_status_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }
    let refresh_budget = if snapshots.is_empty() {
        DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT
    } else {
        DAEMON_CLUSTER_RPC_TIMEOUT
    };
    let deadline = Instant::now() + refresh_budget;
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let snapshot_timeout =
            cluster_endpoint_fair_command_timeout(endpoint, remaining, targets.len() - index)
                .min(DAEMON_CLUSTER_RPC_TIMEOUT);
        if let Ok(snapshot) = fetch_cluster_kv_snapshot_from_endpoint(&endpoint, snapshot_timeout) {
            snapshots.push(snapshot);
        }
    }
    let snapshot = snapshots
        .into_iter()
        .max_by_key(|snapshot| snapshot.revision)
        .ok_or_else(|| DaemonError::ClusterReadOnly {
            detail: "no local or reachable cluster KV snapshot is available".to_string(),
        })?;
    sinomo_network::write_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
    locked_restore_cluster_raft_from_snapshot_if_newer(
        paths,
        &state.local_node_id,
        snapshot.clone(),
    )?;
    Ok(InProcessKvStore::restore(snapshot))
}

fn cluster_member_update_txn(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
) -> Result<KvTxn, DaemonError> {
    Ok(KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: current_entry.key,
            value: current_entry.value,
        }],
        success: cluster_member_update_ops(store, member)?,
        failure: Vec::new(),
    })
}

fn cluster_raft_config_member_role(
    config: &sinomo_network::ClusterRaftConfigRecord,
    node_id: &NodeId,
) -> Option<ClusterMemberRole> {
    if config.voters.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Voter)
    } else if config.learners.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Learner)
    } else if config
        .non_voting
        .iter()
        .any(|candidate| candidate == node_id)
    {
        Some(ClusterMemberRole::NonVoting)
    } else {
        None
    }
}

fn cluster_member_raft_update_txn(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
    now: OffsetDateTime,
) -> Result<KvTxn, DaemonError> {
    let raft_config_key = sinomo_network::cluster_raft_config_key();
    let Some(current_config_entry) = store.get(&raft_config_key).cloned() else {
        return Err(DaemonError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        });
    };
    let current_config: sinomo_network::ClusterRaftConfigRecord = current_config_entry.decode()?;
    let next_config = cluster_raft_config_from_store(store, member, &current_config, now)?;
    sinomo_network::validate_raft_config_transition(&current_config, &next_config)?;
    let mut success = cluster_member_update_ops(store, member)?;
    success.push(KvOp::put_serde(
        raft_config_key.clone(),
        &next_config,
        None,
    )?);
    Ok(KvTxn {
        compares: vec![
            KvCompare::ValueEquals {
                key: current_entry.key,
                value: current_entry.value,
            },
            KvCompare::ValueEquals {
                key: raft_config_key,
                value: current_config_entry.value,
            },
        ],
        success,
        failure: Vec::new(),
    })
}

fn cluster_member_update_ops(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
) -> Result<Vec<KvOp>, DaemonError> {
    let mut member = member.clone();
    sinomo_network::cleanup_member_iroh_endpoints(&mut member);
    let mut ops = vec![
        KvOp::put_serde(cluster_member_key(&member.node_id), &member, None)?,
        KvOp::put_serde(
            sinomo_network::cluster_capability_policy_key(&member.node_id),
            &sinomo_network::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )?,
    ];
    push_cluster_member_endpoint_diff_ops(store, &member, &mut ops)?;
    Ok(ops)
}

fn push_cluster_member_endpoint_diff_ops(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), DaemonError> {
    let endpoint_prefix = sinomo_network::cluster_endpoint_prefix(&member.node_id);
    let mut desired_endpoints = BTreeMap::new();
    for endpoint in &member.endpoints {
        let key = sinomo_network::cluster_endpoint_key(&member.node_id, endpoint);
        let op = KvOp::put_serde(key, endpoint, None)?;
        let KvOp::Put { key, value, .. } = op else {
            unreachable!("put_serde must produce a put operation");
        };
        desired_endpoints.insert(key, value);
    }

    for entry in store.range_prefix_limited(
        &endpoint_prefix,
        sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        if !desired_endpoints.contains_key(&entry.key) {
            ops.push(KvOp::Delete { key: entry.key });
        }
    }

    for (key, value) in desired_endpoints {
        if store
            .get(&key)
            .is_none_or(|entry| entry.value != value || entry.lease_id.is_some())
        {
            ops.push(KvOp::Put {
                key,
                value,
                lease_id: None,
            });
        }
    }

    Ok(())
}

fn cluster_raft_config_from_store(
    store: &InProcessKvStore,
    updated_member: &ClusterMemberRecord,
    current_config: &sinomo_network::ClusterRaftConfigRecord,
    now: OffsetDateTime,
) -> Result<sinomo_network::ClusterRaftConfigRecord, DaemonError> {
    let mut voters = Vec::new();
    let mut learners = Vec::new();
    let mut non_voting = Vec::new();
    for entry in store.range_prefix_limited(
        "/cluster/members/",
        sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.node_id == updated_member.node_id {
            member = updated_member.clone();
        }
        if !member.state.is_authorized() {
            continue;
        }
        match member.state {
            ClusterMemberState::Voter => voters.push(member.node_id),
            ClusterMemberState::Learner => learners.push(member.node_id),
            ClusterMemberState::NonVoting => non_voting.push(member.node_id),
            ClusterMemberState::Active => match member.role {
                ClusterMemberRole::Voter => voters.push(member.node_id),
                ClusterMemberRole::Learner => learners.push(member.node_id),
                ClusterMemberRole::NonVoting => non_voting.push(member.node_id),
            },
            ClusterMemberState::Pending
            | ClusterMemberState::Suspect
            | ClusterMemberState::Offline
            | ClusterMemberState::Removed => {}
        }
    }
    voters.sort();
    learners.sort();
    non_voting.sort();
    Ok(sinomo_network::ClusterRaftConfigRecord {
        cluster_id: current_config.cluster_id.clone(),
        term: current_config.term,
        kv_revision: current_config.kv_revision,
        voters,
        learners,
        non_voting,
        updated_at: now,
    })
}

pub fn remove_cluster_members(
    paths: &ClusterPaths,
    node_ids: Vec<NodeId>,
    now: OffsetDateTime,
) -> Result<ClusterMemberRemoval, DaemonError> {
    let mut state = read_local_cluster_state(paths)?;
    let _ = now;
    let store = current_cluster_kv_store(paths, &state)?;
    let requested = node_ids
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    if requested.is_empty() {
        return Err(DaemonError::Parse {
            detail: "no cluster members were specified".to_string(),
        });
    }
    let local_shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    if requested.contains(&state.local_node_id) || requested.contains(&local_shadow_node_id) {
        return Err(DaemonError::Parse {
            detail: "use `sinomo cluster leave` to remove the local daemon members".to_string(),
        });
    }

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(DaemonError::Parse {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode()?;
        if cluster_kv_member_is_voter(&store, &member.node_id)? {
            return Err(DaemonError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a voter; change its raft role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for txn in cluster_member_removal_txns(&store, &removed)? {
        commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

fn cluster_kv_member_is_voter(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<bool, DaemonError> {
    let Some(config_entry) = store.get(&sinomo_network::cluster_raft_config_key()) else {
        return Ok(false);
    };
    let config: sinomo_network::ClusterRaftConfigRecord = config_entry.decode()?;
    Ok(config.voters.iter().any(|voter| voter == node_id))
}

fn cluster_member_removal_txns(
    store: &InProcessKvStore,
    members: &[ClusterMemberRecord],
) -> Result<Vec<KvTxn>, DaemonError> {
    let mut txns = Vec::new();
    let final_endpoint_prefix_delete_limit =
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(2);
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    for member in members {
        let endpoint_prefix = sinomo_network::cluster_endpoint_prefix(&member.node_id);
        let mut residual_endpoint_keys = VecDeque::new();
        let mut explicit_delete_ops = Vec::new();
        for entry in store.prefix_entries(&endpoint_prefix) {
            residual_endpoint_keys.push_back(entry.key.clone());
            if residual_endpoint_keys.len() <= final_endpoint_prefix_delete_limit {
                continue;
            }
            if let Some(key) = residual_endpoint_keys.pop_front() {
                explicit_delete_ops.push(KvOp::Delete { key });
            }
            if explicit_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut explicit_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !explicit_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: explicit_delete_ops,
                failure: Vec::new(),
            });
        }

        txns.push(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::Delete {
                    key: cluster_member_key(&member.node_id),
                },
                KvOp::Delete {
                    key: sinomo_network::cluster_capability_policy_key(&member.node_id),
                },
                KvOp::DeletePrefix {
                    prefix: endpoint_prefix,
                },
            ],
            failure: Vec::new(),
        });
    }
    Ok(txns)
}

pub fn cluster_peer_members(
    paths: &ClusterPaths,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    now: OffsetDateTime,
) -> Result<Vec<sinomo_core::ClusterMemberRecord>, DaemonError> {
    let mut state = read_local_cluster_state(paths)?;
    if sinomo_network::cluster_state_needs_join_registration(&state) {
        ensure_daemon_cluster_members(&mut state, main_options, shadow_options, now);
        sinomo_network::write_cluster_state(paths, &state)?;
    }
    Ok(state.members)
}

pub fn register_daemon_cluster_node(
    paths: &ClusterPaths,
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, DaemonError> {
    let mut state = read_local_cluster_state(paths)?;
    ensure_daemon_cluster_members(
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    );
    sinomo_network::write_cluster_state(paths, &state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| DaemonError::Parse {
        detail: "daemon cluster member could not be registered".to_string(),
    })
}

pub fn daemon_cluster_status_from_local_state(
    paths: &ClusterPaths,
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, DaemonError> {
    let mut state = sinomo_network::read_cluster_state_without_kv(paths)?;
    ensure_daemon_cluster_members(
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(paths, &mut state)?;
    let _ = refresh_local_iroh_endpoints_from_disk(paths, &mut state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| DaemonError::Parse {
        detail: "daemon cluster member could not be resolved from local state".to_string(),
    })
}

pub fn daemon_cluster_registration_if_initialized(
    paths: &ClusterPaths,
    registration: DaemonClusterRegistration,
) -> Option<DaemonClusterRegistration> {
    paths.state.exists().then_some(registration)
}

pub fn cluster_invite_publish_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut seen_nodes = std::collections::BTreeSet::new();
    let mut targets = Vec::new();
    for endpoint in sinomo_network::cluster_sync_targets(state) {
        if !sinomo_network::cluster_raft_node_is_voter(state, &endpoint.node_id) {
            continue;
        }
        if !seen_nodes.insert(endpoint.node_id.clone()) {
            continue;
        }
        targets.push(endpoint);
    }
    targets
}

pub fn default_cluster_invite_introducers(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    daemon_probe_timeout: Duration,
) -> Vec<ClusterEndpointRecord> {
    let mut introducers = Vec::new();
    if let Some(data_dir) = paths.root.parent() {
        let daemon_paths = DaemonPaths::with_root(data_dir.join("daemon"));
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&daemon_paths, daemon_probe_timeout)
        {
            for endpoint in invite_introducers_from_daemon_status(&state.local_node_id, &status) {
                sinomo_network::push_unique_introducer(&mut introducers, endpoint);
            }
            if let Ok(record) = read_local_iroh_endpoint_record(paths, &state.local_node_id) {
                sinomo_network::push_unique_introducer(&mut introducers, record.endpoint());
            }
        }
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
        sinomo_network::push_unique_introducer(&mut introducers, endpoint);
    }
    sinomo_network::sort_default_invite_introducers(&mut introducers);
    sinomo_network::compact_default_invite_introducers(&mut introducers);
    introducers
}

pub fn build_cluster_join_invite(
    state: &ClusterBootstrapState,
    data_dir: &std::path::Path,
    introducers: Vec<ClusterEndpointRecord>,
    ttl_seconds: i64,
    now: OffsetDateTime,
) -> Result<ClusterJoinInvite, DaemonError> {
    let invite_id = ClusterInviteId::generate();
    let issuer_identity = load_or_create_main_node_identity(data_dir)?;
    let invitation_secret = sinomo_network::random_url_secret(32)?;
    let communication_channels = sinomo_network::invite_communication_channels(state, &introducers);
    let mut invite = ClusterJoinInvite {
        invite_id: invite_id.clone(),
        cluster_id: state.cluster_id.clone(),
        cluster_name: Some(state.cluster_name.clone()),
        cluster_public_key: issuer_identity.public_key.clone(),
        invitation_secret,
        communication_channels,
        introducers,
        token_id: format!("token_{}", invite_id.as_str().trim_start_matches("invite_")),
        expires_at: now + time::Duration::seconds(ttl_seconds),
        trust_domain: TrustDomain::new("trust.local").expect("static trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: state.local_node_id.clone(),
        issuer_public_key: issuer_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sinomo_network::sign_cluster_join_invite(&invite, &issuer_identity)?;
    Ok(invite)
}

pub fn put_cluster_kv_control_record<T: Serialize>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<(), DaemonError> {
    let op = KvOp::put_serde(key.clone(), value, None)?;
    if sinomo_network::read_cluster_kv_entry(paths, &key)?.is_some_and(|entry| match &op {
        KvOp::Put {
            value, lease_id, ..
        } => entry.value == *value && entry.lease_id == *lease_id,
        _ => false,
    }) {
        return Ok(());
    }
    let mut state = sinomo_network::read_cluster_state(paths)?;
    if &state.local_node_id != node_id {
        return Err(DaemonError::ClusterReadOnly {
            detail: format!(
                "local cluster state belongs to {}, not {node_id}",
                state.local_node_id
            ),
        });
    }
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![op],
            failure: Vec::new(),
        },
    )?;
    Ok(())
}

pub fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<Option<KvSnapshot>, DaemonError> {
    if cached_node_status_matches(paths, &status) {
        return Ok(None);
    }
    if sinomo_network::stored_cluster_node_status_matches(paths, &status)? {
        remember_node_status(paths, &status);
        return Ok(None);
    }
    let mut state = sinomo_network::read_cluster_state(paths)?;
    let mut success = vec![KvOp::put_serde(
        sinomo_network::cluster_node_status_key(&status.node_id),
        &status,
        None,
    )?];
    if let Some(rollout) = status.rollout.clone() {
        success.push(KvOp::put_serde(
            sinomo_network::cluster_rollout_node_key(&status.node_id),
            &rollout,
            None,
        )?);
    } else {
        success.push(KvOp::Delete {
            key: sinomo_network::cluster_rollout_node_key(&status.node_id),
        });
    }
    let result = commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        },
    )
    .map(Some);
    if result.is_ok() {
        remember_node_status(paths, &status);
    }
    result
}

pub fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<Option<KvSnapshot>, DaemonError> {
    if cached_network_info_matches(paths, &info) {
        return Ok(None);
    }
    if sinomo_network::stored_cluster_network_info_matches(paths, &info)? {
        remember_network_info(paths, &info);
        return Ok(None);
    }
    let mut state = sinomo_network::read_cluster_state(paths)?;
    let result = commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(
                sinomo_network::cluster_network_info_key(&info.node_id),
                &info,
                None,
            )?],
            failure: Vec::new(),
        },
    )
    .map(Some);
    if result.is_ok() {
        remember_network_info(paths, &info);
    }
    result
}

pub fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<Option<KvSnapshot>, DaemonError> {
    sinomo_network::write_local_cluster_connectivity_record_set(paths, source_node_id, &records)?;
    if cached_connectivity_record_set_matches(paths, source_node_id, &records) {
        return Ok(None);
    }
    let mut state = None;
    let mut latest = None;
    loop {
        let success = sinomo_network::cluster_connectivity_full_sync_batch_ops(
            paths,
            source_node_id,
            &records,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        )?;
        if success.is_empty() {
            remember_connectivity_record_set(paths, source_node_id, &records);
            return Ok(latest);
        }
        if state.is_none() {
            state = Some(sinomo_network::read_cluster_state(paths)?);
        }
        latest = Some(commit_cluster_kv_txn_with_quorum(
            paths,
            state
                .as_mut()
                .expect("cluster state is initialized before connectivity append"),
            KvTxn {
                compares: Vec::new(),
                success,
                failure: Vec::new(),
            },
        )?);
    }
}

pub fn expire_cluster_network_records_in_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: time::OffsetDateTime,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let (compares, success) = sinomo_network::expire_cluster_network_record_ops(paths, state, now)?;
    if success.is_empty() {
        return Ok(None);
    }
    let mut state = sinomo_network::read_cluster_state_without_kv(paths)?;
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares,
            success,
            failure: Vec::new(),
        },
    )
    .map(Some)
}

pub fn assign_local_node_public_keys(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    for member in &mut state.members {
        if member.node_id == state.local_node_id {
            member.node_public_key = main_identity.public_key.clone();
        } else if member.node_id == shadow_node_id {
            member.node_public_key = shadow_identity.public_key.clone();
        }
    }
    Ok(())
}

pub fn cluster_has_local_quorum(paths: &ClusterPaths, state: &ClusterBootstrapState) -> bool {
    cluster_reachable_voter_count(paths, state) >= sinomo_network::cluster_raft_quorum_size(state)
}

pub fn cluster_reachable_voter_count(paths: &ClusterPaths, state: &ClusterBootstrapState) -> usize {
    let quorum_size = sinomo_network::cluster_raft_quorum_size(state);
    let mut voters = std::collections::BTreeSet::new();
    if sinomo_network::cluster_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return voters.len();
        }
    }
    let deadline = Instant::now() + DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT;
    let targets = cluster_raft_live_voter_targets(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if fetch_daemon_status_from_endpoint(
            &endpoint,
            cluster_endpoint_fair_command_timeout(endpoint, remaining, targets.len() - index),
        )
        .is_ok()
        {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
    }
    voters.len()
}

pub fn commit_cluster_raft_command_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    let entry = {
        let _raft_guard = lock_cluster_raft_operation()?;
        match sinomo_network::append_cluster_raft_command_uncommitted_or_no_effect(
            paths,
            &state.local_node_id,
            command,
        )? {
            sinomo_network::ClusterRaftUncommittedAppend::Appended(entry) => entry,
            sinomo_network::ClusterRaftUncommittedAppend::NoEffect {
                snapshot,
                txn_response,
            } => return Ok((snapshot, txn_response)),
        }
    };
    let mut voter_acks = std::collections::BTreeSet::new();
    if sinomo_network::cluster_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    let quorum_size = sinomo_network::cluster_raft_quorum_size(state);
    let mut failures = Vec::new();
    let deadline = Instant::now() + DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT;
    let targets = cluster_raft_live_voter_targets(paths, state);
    for endpoint in &targets {
        if voter_acks.len() >= quorum_size {
            break;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "raft command did not reach quorum before {}ms replication budget",
                DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT.as_millis()
            ));
            break;
        }
        let started = Instant::now();
        match replicate_cluster_raft_to_endpoint_with_timeout(paths, state, endpoint, remaining) {
            Ok(response) if response.accepted => {
                remember_cluster_endpoint_command_connectivity(
                    paths,
                    state,
                    endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                );
                voter_acks.insert(endpoint.node_id.clone());
                sinomo_network::merge_cluster_state_from_peer(state, response.state);
            }
            Ok(response) => {
                remember_cluster_endpoint_command_connectivity(
                    paths,
                    state,
                    endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                );
                failures.push(format!(
                    "{} ({:?}) rejected append: {} (term {}, match_index {})",
                    endpoint.address,
                    endpoint.transport,
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                remember_cluster_endpoint_command_connectivity(
                    paths,
                    state,
                    endpoint,
                    sinomo_network::ClusterConnectivityState::Degraded,
                    None,
                );
                failures.push(format!(
                    "{} ({:?}) failed: {}",
                    endpoint.address, endpoint.transport, error
                ));
            }
        }
    }
    if voter_acks.len() < quorum_size {
        let _raft_guard = lock_cluster_raft_operation()?;
        let _ = sinomo_network::discard_cluster_raft_uncommitted_tail(paths, &state.local_node_id);
        let _ = sinomo_network::clear_cluster_raft_leader(paths, &state.local_node_id);
        return Err(DaemonError::ClusterReadOnly {
            detail: format!(
                "raft command at index {} replicated to {} voters, {} required{}",
                entry.index,
                voter_acks.len(),
                quorum_size,
                if failures.is_empty() {
                    String::new()
                } else {
                    format!(": {}", failures.join("; "))
                }
            ),
        });
    }

    let commit = {
        let _raft_guard = lock_cluster_raft_operation()?;
        sinomo_network::commit_cluster_raft_index_with_events(
            paths,
            &state.local_node_id,
            entry.index,
        )?
    };
    if committed_cluster_raft_command_needs_fanout(&targets) {
        fan_out_committed_cluster_raft_command(paths.clone(), state.clone(), commit.events.clone());
    }
    Ok((commit.snapshot, commit.txn_response))
}

fn committed_cluster_raft_command_needs_fanout(targets: &[ClusterEndpointRecord]) -> bool {
    !targets.is_empty()
}

fn fan_out_committed_cluster_raft_command(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
) {
    CLUSTER_RAFT_FANOUT_PENDING.store(true, Ordering::Release);
    if CLUSTER_RAFT_FANOUT_ACTIVE.swap(true, Ordering::AcqRel) {
        return;
    }
    if thread::Builder::new()
        .name("sinomo-cluster-raft-fanout".to_string())
        .spawn(move || run_cluster_raft_fanout_worker(paths, state, events))
        .is_err()
    {
        CLUSTER_RAFT_FANOUT_PENDING.store(true, Ordering::Release);
        CLUSTER_RAFT_FANOUT_ACTIVE.store(false, Ordering::Release);
    }
}

fn run_cluster_raft_fanout_worker(
    paths: ClusterPaths,
    mut state: ClusterBootstrapState,
    mut events: Vec<KvEvent>,
) {
    loop {
        while CLUSTER_RAFT_FANOUT_PENDING.swap(false, Ordering::AcqRel) {
            fan_out_committed_cluster_raft_command_blocking(&paths, &state, events);
            if let Ok(latest_state) = sinomo_network::read_cluster_state(&paths) {
                state = latest_state;
                events = sinomo_network::read_cluster_kv_snapshot(&paths)
                    .map(|snapshot| snapshot.events)
                    .unwrap_or_default();
            } else {
                events = Vec::new();
            }
        }

        if !claim_next_cluster_raft_fanout_pass(
            &CLUSTER_RAFT_FANOUT_ACTIVE,
            &CLUSTER_RAFT_FANOUT_PENDING,
        ) {
            break;
        }
    }
}

fn claim_next_cluster_raft_fanout_pass(active: &AtomicBool, pending: &AtomicBool) -> bool {
    active.store(false, Ordering::Release);
    if !pending.load(Ordering::Acquire) {
        return false;
    }
    !active.swap(true, Ordering::AcqRel)
}

fn fan_out_committed_cluster_raft_command_blocking(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: Vec<KvEvent>,
) {
    let mut replicated_nodes = std::collections::BTreeSet::new();
    for endpoint in cluster_raft_live_voter_targets(paths, state) {
        if replicated_nodes.contains(&endpoint.node_id) {
            continue;
        }
        if let Ok(outcome) = replicate_cluster_raft_to_endpoint_with_timeout_result(
            paths,
            state,
            &endpoint,
            DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT,
            &events,
        ) {
            if outcome.response.accepted {
                replicated_nodes.insert(endpoint.node_id.clone());
                if outcome.watch_event_fanout_needed {
                    send_cluster_kv_watch_event_fanout_batches(state, &endpoint, &events);
                }
            }
        }
    }
}

fn send_cluster_kv_watch_event_fanout_batches(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    events: &[KvEvent],
) {
    for event_batch in cluster_kv_watch_event_fanout_batches(events) {
        let response = send_cluster_kv_watch_events(
            endpoint,
            &ClusterKvWatchEventsRequest {
                state: state.clone(),
                leader_id: state.local_node_id.clone(),
                events: event_batch.to_vec(),
            },
            DAEMON_CLUSTER_RPC_TIMEOUT,
        );
        if !response.is_ok_and(|response| response.accepted) {
            break;
        }
    }
}

fn cluster_kv_watch_event_fanout_batches(events: &[KvEvent]) -> impl Iterator<Item = &[KvEvent]> {
    let mut batches = Vec::new();
    let mut start = 0_usize;
    while start < events.len() {
        let mut end = start;
        let mut payload_bytes = 0_usize;
        while end < events.len() {
            if end - start >= sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS {
                break;
            }
            let event_bytes = sinomo_network::cluster_kv_watch_event_payload_bytes(&events[end]);
            let next_payload_bytes = payload_bytes.saturating_add(event_bytes);
            if end > start
                && next_payload_bytes
                    > sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
            {
                break;
            }
            payload_bytes = next_payload_bytes;
            end += 1;
        }
        if end == start {
            end += 1;
        }
        batches.push(&events[start..end]);
        start = end;
    }
    batches.into_iter()
}

pub fn commit_cluster_kv_txn_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<KvSnapshot, DaemonError> {
    commit_cluster_kv_txn_with_quorum_response(paths, state, txn).map(|(snapshot, _)| snapshot)
}

pub fn commit_cluster_kv_txn_with_quorum_response(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    commit_cluster_kv_txn_with_quorum_response_with_forwarding(paths, state, txn, true)
}

fn commit_cluster_kv_txn_with_quorum_response_with_forwarding(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    allow_forwarding: bool,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError> {
    let _ = current_cluster_kv_store(paths, state);
    if !sinomo_network::cluster_raft_local_is_voter(state) {
        if allow_forwarding
            && let Some(response) = forward_cluster_kv_txn_to_voter(paths, state, &txn)
        {
            return response;
        }
        return Err(DaemonError::ClusterReadOnly {
            detail: format!(
                "local daemon is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
                cluster_reachable_voter_count(paths, state),
                sinomo_network::cluster_raft_quorum_size(state)
            ),
        });
    }
    if !locked_cluster_raft_local_is_leader(paths, state)? {
        let forwarding_failure = if allow_forwarding {
            match forward_cluster_kv_txn_to_voter(paths, state, &txn) {
                Some(Ok(response)) => return Ok(response),
                Some(Err(error)) => Some(error),
                None => None,
            }
        } else {
            None
        };
        if !allow_forwarding {
            return Err(DaemonError::ClusterReadOnly {
                detail: "forwarded cluster KV transaction reached a non-leader voter".to_string(),
            });
        }
        if !elect_cluster_raft_leader(paths, state)? {
            let mut error = DaemonError::ClusterReadOnly {
                detail: "local daemon is not the Raft leader and could not win quorum".to_string(),
            };
            if let Some(forwarding_failure) = forwarding_failure {
                error = DaemonError::ClusterReadOnly {
                    detail: format!(
                        "local daemon is not the Raft leader and could not win quorum; forwarding also failed: {forwarding_failure}"
                    ),
                };
            }
            return Err(error);
        }
    }
    if !locked_cluster_raft_local_is_leader(paths, state)? {
        return Err(DaemonError::ClusterReadOnly {
            detail: "local daemon is not the Raft leader and could not win quorum".to_string(),
        });
    }
    commit_cluster_raft_command_with_quorum(paths, state, KvCommand::Txn(txn))
}

fn forward_cluster_kv_txn_to_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
) -> Option<Result<(KvSnapshot, Option<KvTxnResponse>), DaemonError>> {
    let mut targets = cluster_raft_live_voter_targets(paths, state);
    if targets.is_empty() {
        return None;
    }
    if let Ok(Some(leader_id)) = locked_cluster_raft_known_leader_id(paths, state)
        && !cluster_node_status_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT;
    for endpoint in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "forwarding did not complete before {}ms command budget",
                DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT.as_millis()
            ));
            break;
        }
        let txn_timeout = cluster_endpoint_command_timeout(endpoint, remaining);
        match send_cluster_kv_txn(&endpoint, txn, txn_timeout) {
            Ok(response) => {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                return Some(
                    fetch_cluster_kv_snapshot_from_endpoint(
                        &endpoint,
                        cluster_endpoint_command_timeout(endpoint, remaining),
                    )
                    .map(|snapshot| (snapshot, Some(response))),
                );
            }
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    Some(Err(DaemonError::ClusterReadOnly {
        detail: format!(
            "could not forward cluster KV transaction to any known voter{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    }))
}

/// Per-hop timeout for a relayed cluster RPC. The first hop waits for the whole downstream
/// chain, so this is generous; the relay TTL bounds the number of hops.
const DAEMON_CLUSTER_RELAY_TIMEOUT: Duration = Duration::from_secs(15);
const DAEMON_MAIN_RUN_CAPABILITY: &str = "daemon.main.run";

fn relay_response_is_routing_error(response: &sinomo_network::ObjectRpcResponse) -> bool {
    response.route == sinomo_network::CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
}

fn relay_next_hops(
    graph: &sinomo_network::ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    sinomo_network::cluster_route_next_hops_best_effort(
        graph,
        local_node_id,
        target_node_id,
        excluded,
    )
    .into_iter()
    .filter(|candidate| relay_candidate_can_transit(state, candidate, target_node_id))
    .collect()
}

fn relay_candidate_can_transit(
    state: &ClusterBootstrapState,
    candidate: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if candidate == target_node_id {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *candidate)
        .is_some_and(|member| {
            member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
}

/// Handle a `CLUSTER_RELAY_ROUTE` request. Runs the pure routing decision, then either
/// delivers the wrapped request locally (when this node is the target) by re-injecting it
/// through the daemon's own control port, or forwards it one hop closer to the target over
/// the connectivity graph. The inner request's response flows back along the relay chain.
pub fn handle_daemon_cluster_relay_request(
    paths: &DaemonPaths,
    envelope: sinomo_network::RelayEnvelope,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let local_node_id = relay_local_node_id(paths, &state);
    let graph = sinomo_network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    if envelope.target_node_id == local_node_id {
        // We are the target: re-inject the original signed request through our own
        // control port so it passes the normal authentication + dispatch path.
        let inner = &envelope.inner;
        let inner_rpc = inner.rpc.as_ref().ok_or_else(|| DaemonError::Network {
            address: "relay-local".to_string(),
            detail: "relayed request carried no rpc payload".to_string(),
        })?;
        let port = daemon_options_from_status(&read_daemon_status(paths)?).port;
        let local_address = daemon_connect_address("127.0.0.1", port);
        return sinomo_network::send_daemon_rpc_raw(
            &local_address,
            &inner.command,
            inner.auth.as_deref(),
            inner_rpc,
            DAEMON_CLUSTER_RELAY_TIMEOUT,
        )
        .map_err(DaemonError::from);
    }
    if envelope.visited.iter().any(|node| node == &local_node_id) {
        return sinomo_network::ObjectRpcResponse::encode_error(
            sinomo_network::CLUSTER_RELAY_ROUTE,
            format!("relay dropped: relay loop: {local_node_id} already on the path"),
        )
        .map_err(DaemonError::from);
    }
    if envelope.ttl == 0 {
        return sinomo_network::ObjectRpcResponse::encode_error(
            sinomo_network::CLUSTER_RELAY_ROUTE,
            "relay dropped: relay ttl exhausted",
        )
        .map_err(DaemonError::from);
    }

    let next_hops = relay_next_hops(
        &graph,
        &state,
        &local_node_id,
        &envelope.target_node_id,
        &envelope.visited,
    );
    if next_hops.is_empty() {
        return sinomo_network::ObjectRpcResponse::encode_error(
            sinomo_network::CLUSTER_RELAY_ROUTE,
            format!(
                "relay dropped: no reachable route from {local_node_id} to {}",
                envelope.target_node_id
            ),
        )
        .map_err(DaemonError::from);
    }

    let mut failures = Vec::new();
    for next_hop in next_hops {
        let Some(endpoint) =
            cluster_relay_next_hop_endpoint(&cluster_paths, &state, &local_node_id, &next_hop)
        else {
            failures.push(format!(
                "{next_hop}: relay next hop has no reachable endpoint"
            ));
            continue;
        };
        let mut forwarded = envelope.clone();
        forwarded.ttl = envelope.ttl.saturating_sub(1);
        forwarded.visited.push(local_node_id.clone());
        let outer = forwarded.into_request().map_err(DaemonError::from)?;
        match crate::send_daemon_rpc_to_endpoint(&endpoint, &outer, DAEMON_CLUSTER_RELAY_TIMEOUT) {
            Ok(response) if relay_response_is_routing_error(&response) => {
                failures.push(format!(
                    "{} ({:?}) returned routing error",
                    endpoint.address, endpoint.transport
                ));
            }
            Ok(response) => return Ok(response),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }

    sinomo_network::ObjectRpcResponse::encode_error(
        sinomo_network::CLUSTER_RELAY_ROUTE,
        format!(
            "relay dropped: no reachable route from {local_node_id} to {}{}",
            envelope.target_node_id,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    )
    .map_err(DaemonError::from)
}

fn relay_local_node_id(paths: &DaemonPaths, state: &ClusterBootstrapState) -> NodeId {
    if daemon_paths_are_shadow(paths) {
        sinomo_node::shadow_daemon_node_id(&state.local_node_id)
    } else {
        state.local_node_id.clone()
    }
}

/// The endpoint to relay a hop through `node_id`, dialed over the link we have actually
/// confirmed healthy (built straight from the connectivity record). This dials a multi-homed
/// bridge on the segment we can reach rather than its first-listed command-target address,
/// which may be on a segment we cannot reach. Falls back to the connectivity-ordered command
/// target only when no healthy record exists (defensive; routing only picks healthy hops).
fn cluster_relay_next_hop_endpoint(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    node_id: &NodeId,
) -> Option<ClusterEndpointRecord> {
    let graph = sinomo_network::ClusterConnectivityGraph::read_for_local(paths, state);
    graph
        .best_healthy_endpoint(source_node_id, node_id)
        .or_else(|| {
            sinomo_network::cluster_raft_command_targets(paths, state)
                .into_iter()
                .find(|endpoint| endpoint.node_id == *node_id)
        })
}

/// On a failed direct dial to `endpoint`, attempt to relay the already-signed `request`
/// one hop closer to `endpoint.node_id` through a reachable bridge, per the cluster
/// connectivity graph. Returns `None` when no route is known (the caller keeps the
/// original error); relay envelopes are never themselves re-relayed. The next hop's relay
/// handler delivers locally or forwards onward, and its response flows back here.
/// Whether the local node's connectivity graph already records `endpoint` as `Unreachable`.
/// Used to skip a doomed direct dial (which would burn the whole timeout) in favor of relaying.
/// Returns `false` on any read error or when the edge is unknown, so callers fall back to the
/// normal direct-then-relay path rather than mis-skipping a reachable endpoint.
/// Whether `send_daemon_rpc_to_endpoint` should skip the direct dial and relay instead:
/// true when we have no currently-healthy direct edge to the target node BUT do have a healthy
/// multi-hop route to it, so a direct dial could only burn the timeout. Returns false (i.e.
/// still try a direct dial) when we DO have a healthy direct edge, or when no route is known at
/// all (e.g. at startup before the first probe) -- so unprobed-but-reachable peers still work.
pub(crate) fn cluster_endpoint_prefers_relay(endpoint: &ClusterEndpointRecord) -> bool {
    let Ok(data_dir) = SinomoDataDir::from_env_or_home() else {
        return false;
    };
    let cluster_paths = ClusterPaths::new(&data_dir);
    let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
        return false;
    };
    if endpoint.node_id == state.local_node_id {
        return false;
    }
    let graph = sinomo_network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    if graph
        .healthy_targets(&state.local_node_id)
        .contains(&endpoint.node_id)
    {
        return false;
    }
    // No healthy direct edge. Relay iff the relay would actually make progress -- i.e. the same
    // best-effort first-hop the relay uses resolves (a full healthy path OR a healthy neighbor
    // to hand off to). A learner often lacks the bridge's onward edges in its frozen join
    // snapshot, so requiring a fully-known path here would wrongly keep dialing direct.
    !relay_next_hops(&graph, &state, &state.local_node_id, &endpoint.node_id, &[]).is_empty()
}

pub(crate) fn relay_request_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_network::ObjectRpcRequest,
    request_auth: &[u8],
    request_command: &str,
    timeout: Duration,
) -> Option<Result<sinomo_network::ObjectRpcResponse, DaemonError>> {
    if request.route == sinomo_network::CLUSTER_RELAY_ROUTE {
        return None;
    }
    let deadline = crate::client::DaemonRpcAttemptDeadline::new(timeout);
    let data_dir = SinomoDataDir::from_env_or_home().ok()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths).ok()?;
    let graph = sinomo_network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    let next_hops = relay_next_hops(&graph, &state, &state.local_node_id, &endpoint.node_id, &[]);
    if next_hops.is_empty() {
        return None;
    }
    let inner = sinomo_network::DaemonControlRequest::authenticated_rpc(
        request_command.to_string(),
        request.clone(),
        request_auth.to_vec(),
    );
    let mut failures = Vec::new();
    for next_hop_id in next_hops {
        let Some(next_hop) = cluster_relay_next_hop_endpoint(
            &cluster_paths,
            &state,
            &state.local_node_id,
            &next_hop_id,
        ) else {
            failures.push(format!(
                "{next_hop_id}: relay next hop has no reachable endpoint"
            ));
            continue;
        };
        let mut envelope = sinomo_network::RelayEnvelope::new(
            state.cluster_id.clone(),
            endpoint.node_id.clone(),
            inner.clone(),
        );
        // Put the origin on the visited path so a downstream best-effort hop never bounces back.
        envelope.visited.push(state.local_node_id.clone());
        let outer = match envelope.into_request() {
            Ok(outer) => outer,
            Err(error) => return Some(Err(DaemonError::from(error))),
        };
        let outer_auth = match crate::sign_daemon_rpc_request_for_local_node(&outer) {
            Ok(auth) => auth,
            Err(error) => return Some(Err(error)),
        };
        match sinomo_network::send_daemon_rpc_to_cluster_endpoint(
            &next_hop,
            &crate::daemon_rpc_command(sinomo_network::CLUSTER_RELAY_ROUTE),
            Some(&outer_auth),
            &outer,
            deadline.remaining(),
        ) {
            Ok(response) if relay_response_is_routing_error(&response) => failures.push(format!(
                "{} ({:?}) returned routing error",
                next_hop.address, next_hop.transport
            )),
            Ok(response) => return Some(Ok(response)),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                next_hop.address, next_hop.transport, error
            )),
        }
    }
    Some(Err(DaemonError::Network {
        address: endpoint.address.clone(),
        detail: format!(
            "relay route to {} failed{}",
            endpoint.node_id,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    }))
}

fn cluster_raft_live_voter_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    sinomo_network::cluster_raft_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_network::cluster_raft_node_is_voter(state, &endpoint.node_id))
        .filter(|endpoint| !cluster_node_status_is_offline(paths, &endpoint.node_id))
        .collect()
}

fn cluster_node_status_blocks_leader_preference(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> bool {
    // A node blocks leader preference if it has gracefully reported itself
    // offline, or if the active connectivity probes can no longer reach it. The
    // self-reported `observed_at` is not consulted: it is no longer refreshed on a
    // timer, so an idle-but-healthy node must not be deprioritized as a candidate.
    if cluster_node_status_is_offline(paths, node_id) {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .is_some_and(|member| {
            sinomo_network::cluster_member_unreachable_via_connectivity(paths, state, member)
        })
}

fn cluster_node_status_is_offline(paths: &ClusterPaths, node_id: &NodeId) -> bool {
    sinomo_network::read_cluster_node_statuses_from_kv(paths)
        .unwrap_or_default()
        .into_iter()
        .find(|status| status.node_id == *node_id)
        .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
}

pub fn elect_cluster_raft_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, DaemonError> {
    if !sinomo_network::cluster_raft_local_is_voter(state) {
        return Ok(false);
    }
    for _ in 0..3 {
        let (election_term, last_log_index, last_log_term) = {
            let _raft_guard = lock_cluster_raft_operation()?;
            let node = sinomo_network::open_cluster_raft_kv_node(paths, &state.local_node_id)?;
            (
                node.hard_state()
                    .current_term
                    .saturating_add(CLUSTER_RAFT_ELECTION_TERM_BUMP),
                node.last_log_index(),
                node.last_log_term(),
            )
        };
        let vote_request = ClusterRaftVoteRequest {
            state: state.clone(),
            vote: RaftRequestVoteRequest {
                term: election_term,
                candidate_id: state.local_node_id.clone(),
                last_log_index,
                last_log_term,
            },
        };
        let local_vote = {
            let _raft_guard = lock_cluster_raft_operation()?;
            sinomo_network::apply_cluster_raft_vote(
                paths,
                &state.local_node_id,
                vote_request.vote.clone(),
            )?
        };
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = std::collections::BTreeSet::new();
        if local_vote.vote_granted {
            granted.insert(state.local_node_id.clone());
        }
        let quorum_size = sinomo_network::cluster_raft_quorum_size(state);
        let deadline = Instant::now() + DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT;
        let targets = cluster_raft_live_voter_targets(paths, state);
        for (index, endpoint) in targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                break;
            }
            let Ok(response) = send_cluster_raft_vote(
                endpoint,
                &vote_request,
                cluster_endpoint_fair_command_timeout(endpoint, remaining, targets.len() - index),
            ) else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.vote.term);
            if response.vote.vote_granted {
                granted.insert(endpoint.node_id.clone());
            }
        }
        if highest_observed_term > election_term {
            let _raft_guard = lock_cluster_raft_operation()?;
            sinomo_network::observe_cluster_raft_term(
                paths,
                &state.local_node_id,
                highest_observed_term,
            )?;
            continue;
        }
        if granted.len() < quorum_size {
            return Ok(false);
        }
        {
            let _raft_guard = lock_cluster_raft_operation()?;
            sinomo_network::set_cluster_raft_leader(paths, &state.local_node_id, election_term)?;
        }
        return Ok(true);
    }
    Ok(false)
}

pub fn replicate_cluster_raft_to_endpoint(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    replicate_cluster_raft_to_endpoint_with_timeout(
        paths,
        state,
        endpoint,
        DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT,
    )
}

fn replicate_cluster_raft_to_endpoint_with_timeout(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    replicate_cluster_raft_to_endpoint_with_timeout_result(paths, state, endpoint, timeout, &[])
        .map(|outcome| outcome.response)
}

#[derive(Debug)]
struct ClusterRaftReplicationOutcome {
    response: ClusterRaftAppendResponse,
    watch_event_fanout_needed: bool,
}

fn replicate_cluster_raft_to_endpoint_with_timeout_result(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    watch_events: &[KvEvent],
) -> Result<ClusterRaftReplicationOutcome, DaemonError> {
    let deadline = Instant::now() + timeout;
    let mut next_index = {
        let _raft_guard = lock_cluster_raft_operation()?;
        sinomo_network::open_cluster_raft_kv_node(paths, &state.local_node_id)?
            .last_log_index()
            .saturating_add(1)
    };
    let mut last_response = None;
    for _ in 0..CLUSTER_RAFT_APPEND_MAX_ATTEMPTS {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = {
            let _raft_guard = lock_cluster_raft_operation()?;
            sinomo_network::build_cluster_raft_append_request_from_next_index(
                paths, state, next_index,
            )?
        };
        let attempt_timeout = cluster_raft_append_attempt_timeout(endpoint, remaining, &request);
        if let Some(stats) = crate::process_idle_stats() {
            stats.incr_raft_appends_sent();
        }
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = send_cluster_raft_append(endpoint, &request, attempt_timeout)?;
        if response.accepted {
            return Ok(ClusterRaftReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else if response.append.match_index >= next_index {
            next_index.saturating_div(2).max(1)
        } else {
            next_index.saturating_sub(1).max(1)
        };
        last_response = Some(response);
    }
    Ok(ClusterRaftReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                timeout.as_millis()
            ),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
            },
        }),
        watch_event_fanout_needed: false,
    })
}

fn cluster_raft_append_request_needs_watch_event_fanout(
    request: &ClusterRaftAppendRequest,
    events: &[KvEvent],
) -> bool {
    if events.is_empty() {
        return false;
    }
    let Some(snapshot) = request.snapshot.as_ref() else {
        return false;
    };
    !cluster_kv_events_contain_all(&snapshot.kv.events, events)
}

fn cluster_kv_events_contain_all(existing: &[KvEvent], incoming: &[KvEvent]) -> bool {
    incoming.iter().all(|event| {
        existing.iter().any(|candidate| {
            candidate.revision == event.revision
                && candidate.key == event.key
                && candidate.kind == event.kind
        })
    })
}

fn cluster_endpoint_command_timeout(
    endpoint: &ClusterEndpointRecord,
    remaining: Duration,
) -> Duration {
    match endpoint.transport {
        ClusterTransportKind::Iroh => remaining.min(DAEMON_CLUSTER_IROH_COMMAND_TIMEOUT),
        _ => remaining.min(DAEMON_CLUSTER_TCP_COMMAND_TIMEOUT),
    }
}

fn cluster_endpoint_fair_command_timeout(
    endpoint: &ClusterEndpointRecord,
    remaining: Duration,
    _attempts_left: usize,
) -> Duration {
    let attempt_cap = match endpoint.transport {
        ClusterTransportKind::Iroh => DAEMON_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
        _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
            DAEMON_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT
        }
        _ => DAEMON_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
    };
    cluster_endpoint_command_timeout(endpoint, remaining.min(attempt_cap))
}

fn cluster_raft_append_attempt_timeout(
    endpoint: &ClusterEndpointRecord,
    remaining: Duration,
    request: &ClusterRaftAppendRequest,
) -> Duration {
    if request.snapshot.is_some() || request.append.entries.len() > 64 {
        return cluster_endpoint_command_timeout(endpoint, remaining);
    }
    cluster_endpoint_fair_command_timeout(endpoint, remaining, 1)
}

pub fn handle_daemon_cluster_raft_append_request(
    paths: &DaemonPaths,
    request: ClusterRaftAppendRequest,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    if let Some(stats) = crate::process_idle_stats() {
        stats.incr_raft_appends_recv();
    }
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state(&cluster_paths)?;
    let _raft_guard = lock_cluster_raft_operation()?;
    if request.state.cluster_id != state.cluster_id {
        let node = sinomo_network::open_cluster_raft_kv_node(&cluster_paths, &state.local_node_id)?;
        return Ok(ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            state,
            append: RaftAppendEntriesResponse {
                term: node.hard_state().current_term,
                success: false,
                match_index: node.last_log_index(),
            },
        });
    }
    let leader_authorized_by_local_state =
        sinomo_network::cluster_raft_node_is_voter(&state, &request.append.leader_id)
            && authenticated_signer_matches_cluster_node(
                &state,
                authenticated,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join =
        sinomo_network::cluster_state_needs_join_registration(&state)
            && request.append.leader_id == request.state.local_node_id
            && sinomo_network::cluster_raft_node_is_voter(
                &request.state,
                &request.append.leader_id,
            )
            && authenticated_signer_matches_cluster_node(
                &request.state,
                authenticated,
                &request.append.leader_id,
            );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        let node = sinomo_network::open_cluster_raft_kv_node(&cluster_paths, &state.local_node_id)?;
        return Ok(ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
            state,
            append: RaftAppendEntriesResponse {
                term: node.hard_state().current_term,
                success: false,
                match_index: node.last_log_index(),
            },
        });
    }
    sinomo_network::merge_cluster_state_from_peer(&mut state, request.state);
    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
    let append = sinomo_network::apply_cluster_raft_append(
        &cluster_paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub fn handle_daemon_cluster_raft_vote_request(
    paths: &DaemonPaths,
    request: ClusterRaftVoteRequest,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let mut state = sinomo_network::read_cluster_state(&cluster_paths)?;
    let _raft_guard = lock_cluster_raft_operation()?;
    let local_node_id = state.local_node_id.clone();
    if request.state.cluster_id != state.cluster_id {
        let node = sinomo_network::open_cluster_raft_kv_node(&cluster_paths, &local_node_id)?;
        return Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            state,
            vote: RaftRequestVoteResponse {
                term: node.hard_state().current_term,
                vote_granted: false,
            },
        });
    }
    let candidate_is_authorized_voter = state.members.iter().any(|member| {
        member.node_id == request.vote.candidate_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && sinomo_network::cluster_member_has_raft_vote(member)
    });
    if !candidate_is_authorized_voter
        || !authenticated_signer_matches_cluster_node(
            &state,
            authenticated,
            &request.vote.candidate_id,
        )
    {
        let node = sinomo_network::open_cluster_raft_kv_node(&cluster_paths, &local_node_id)?;
        return Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail: format!(
                "raft vote rejected for unauthorized or mismatched candidate {}",
                request.vote.candidate_id
            ),
            state,
            vote: RaftRequestVoteResponse {
                term: node.hard_state().current_term,
                vote_granted: false,
            },
        });
    }
    sinomo_network::merge_cluster_state_from_peer(&mut state, request.state);
    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
    let vote =
        sinomo_network::apply_cluster_raft_vote(&cluster_paths, &local_node_id, request.vote)?;
    Ok(ClusterRaftVoteResponse {
        accepted: vote.vote_granted,
        detail: if vote.vote_granted {
            "raft vote granted".to_string()
        } else {
            "raft vote rejected by local hard state".to_string()
        },
        state,
        vote,
    })
}

pub fn handle_daemon_cluster_member_publish_request(
    paths: &DaemonPaths,
    request: ClusterMemberPublishRequest,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;
    if request.members.is_empty() {
        return Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: "member publish request was empty".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    for member in &request.members {
        if member.cluster_id != state.cluster_id {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member {} cluster `{}` does not match local cluster `{}`",
                    member.node_id, member.cluster_id, state.cluster_id
                ),
                state,
                kv_snapshot: None,
            });
        }
        if !member_publish_authorized_for_signer(&member.node_id, &authenticated.signer_node_id) {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member publisher {} cannot publish member {}",
                    authenticated.signer_node_id, member.node_id
                ),
                state,
                kv_snapshot: None,
            });
        }
        if !state.members.iter().any(|existing| {
            existing.node_id == member.node_id
                && existing.node_public_key == member.node_public_key
                && existing.state.is_authorized()
        }) {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member {} is not an authorized cluster member with the published public key",
                    member.node_id
                ),
                state,
                kv_snapshot: None,
            });
        }
    }
    if sinomo_network::cluster_member_publish_matches_kv(&cluster_paths, &request.members)? {
        return Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints already current".to_string(),
            state,
            kv_snapshot: Some(handle_daemon_cluster_kv_snapshot(paths)?),
        });
    }
    let mut commit_state = state.clone();
    match commit_cluster_kv_txn_with_quorum(
        &cluster_paths,
        &mut commit_state,
        sinomo_network::cluster_member_publish_txn_from_kv(&cluster_paths, &request.members)?,
    ) {
        Ok(snapshot) => Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints published".to_string(),
            state: sinomo_network::read_cluster_state(&cluster_paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
        }),
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

fn member_publish_authorized_for_signer(member_node_id: &NodeId, signer_node_id: &str) -> bool {
    if member_node_id.as_str() == signer_node_id {
        return true;
    }
    if member_node_id
        .as_str()
        .starts_with(&format!("{signer_node_id}-"))
    {
        return true;
    }
    NodeId::new(signer_node_id)
        .map(|signer| shadow_daemon_node_id(&signer) == *member_node_id)
        .unwrap_or(false)
}

pub fn publish_cluster_invitation_to_quorum(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
) -> Result<(), DaemonError> {
    let txn = if sinomo_network::cluster_raft_local_is_voter(state) {
        sinomo_network::cluster_invitation_publish_txn_from_kv(paths, invite)?
    } else {
        sinomo_network::cluster_invitation_publish_txn(invite)?
    };
    if txn.success.is_empty() {
        return Ok(());
    }
    let mut commit_state = state.clone();
    commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)?;
    Ok(())
}

pub fn handle_daemon_cluster_invite_publish_request(
    paths: &DaemonPaths,
    request: ClusterInvitePublishRequest,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterInvitePublishResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;
    if request.invite.cluster_id != state.cluster_id {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match local cluster `{}`",
                request.invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request.invite.expires_at <= time::OffsetDateTime::now_utc() {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                request
                    .invite
                    .expires_at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
            state,
            kv_snapshot: None,
        });
    }
    if let Err(error) = sinomo_network::verify_cluster_join_invite_signature(&request.invite) {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                request.invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if authenticated.signer_node_id != request.invite.issuer_node_id.as_str()
        || authenticated.signer_public_key != request.invite.issuer_public_key
    {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: "invite publisher does not match invite issuer".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    match publish_cluster_invitation_to_quorum(&cluster_paths, &state, &request.invite) {
        Ok(()) => Ok(ClusterInvitePublishResponse {
            accepted: true,
            detail: "invite published".to_string(),
            state: sinomo_network::read_cluster_state(&cluster_paths).unwrap_or(state),
            kv_snapshot: handle_daemon_cluster_kv_snapshot(paths).ok(),
        }),
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn unique_cluster_test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
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

    fn assert_daemon_range_limit(error: DaemonError, prefix: &str) {
        let detail = match error {
            DaemonError::Parse { detail } => detail,
            other => panic!("expected range limit for {prefix}, got {other:?}"),
        };
        assert!(
            detail.contains(&format!("range for prefix {prefix} exceeded max"))
                && detail
                    .contains(&sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES.to_string()),
            "expected range limit for {prefix}, got {detail}"
        );
    }

    #[test]
    fn cluster_member_update_ops_diff_endpoint_records() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_endpoint_diff").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_endpoint_diff").expect("node");
        let unchanged = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_diff_unchanged",
            )
            .expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.1:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let stale = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_diff_stale",
            )
            .expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.2:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 1,
            last_observed_latency_ms: None,
        };
        let added = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_diff_added",
            )
            .expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.3:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 2,
            last_observed_latency_ms: None,
        };

        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            node_id.clone(),
            "endpoint diff",
            "pk-endpoint-diff",
            now,
        );
        member.endpoints = vec![unchanged.clone(), added.clone()];

        let unchanged_key = sinomo_network::cluster_endpoint_key(&node_id, &unchanged);
        let stale_key = sinomo_network::cluster_endpoint_key(&node_id, &stale);
        let added_key = sinomo_network::cluster_endpoint_key(&node_id, &added);
        let endpoint_prefix = sinomo_network::cluster_endpoint_prefix(&node_id);
        let mut store = InProcessKvStore::new();
        store
            .put_serde(unchanged_key.clone(), &unchanged, None)
            .expect("unchanged endpoint");
        store
            .put_serde(stale_key.clone(), &stale, None)
            .expect("stale endpoint");

        let ops = cluster_member_update_ops(&store, &member).expect("member update ops");

        assert!(
            ops.iter().all(|op| !matches!(
                op,
                KvOp::DeletePrefix { prefix } if prefix == &endpoint_prefix
            )),
            "member updates should avoid broad endpoint prefix deletes"
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, KvOp::Delete { key } if key == &stale_key)),
            "stale endpoint should be deleted explicitly"
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, KvOp::Put { key, .. } if key == &added_key)),
            "new endpoint should be put"
        );
        assert!(
            ops.iter()
                .all(|op| !matches!(op, KvOp::Put { key, .. } if key == &unchanged_key)),
            "unchanged endpoint should not be re-put"
        );
    }

    #[test]
    fn cluster_member_update_ops_rejects_oversized_endpoint_prefix() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_endpoint_update_limit").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_endpoint_update_limit").expect("node");
        let member = ClusterMemberRecord::local_voter(
            cluster_id,
            node_id.clone(),
            "endpoint update limit",
            "pk-endpoint-update-limit",
            now,
        );
        let endpoint_prefix = sinomo_network::cluster_endpoint_prefix(&node_id);
        let mut store = InProcessKvStore::new();
        for index in 0..=sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
            store
                .put(
                    format!("{endpoint_prefix}endpoint_{index:05}"),
                    Vec::new(),
                    None,
                )
                .expect("endpoint");
        }

        let error = cluster_member_update_ops(&store, &member).expect_err("range limit");
        assert_daemon_range_limit(error, &endpoint_prefix);
    }

    #[test]
    fn cluster_raft_config_from_store_rejects_oversized_member_prefix() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_member_config_limit").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_member_config_limit").expect("node");
        let member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node_id,
            "member config limit",
            "pk-member-config-limit",
            now,
        );
        let current_config = sinomo_network::ClusterRaftConfigRecord {
            cluster_id,
            term: 1,
            kv_revision: 1,
            voters: Vec::new(),
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let mut store = InProcessKvStore::new();
        for index in 0..=sinomo_network::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
            store
                .put(
                    format!("/cluster/members/node_{index:05}"),
                    Vec::new(),
                    None,
                )
                .expect("member");
        }

        let error = cluster_raft_config_from_store(&store, &member, &current_config, now)
            .expect_err("range limit");
        assert_daemon_range_limit(error, "/cluster/members/");
    }

    #[test]
    fn cluster_member_removal_txns_bound_large_endpoint_prefix_cleanup() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_removal_endpoint_limit").expect("cluster");
        let node_id =
            NodeId::from_legacy_named("node_daemon_removal_endpoint_limit").expect("node");
        let mut member = ClusterMemberRecord::pending_learner(
            cluster_id,
            node_id.clone(),
            "removal endpoint limit",
            "pk-removal-endpoint-limit",
            now,
        );
        member.state = ClusterMemberState::NonVoting;
        member.role = ClusterMemberRole::NonVoting;
        member.capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::NonVoting);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = (0..=sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| ClusterEndpointRecord {
                endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(format!(
                    "endpoint_daemon_removal_limit_{index}"
                ))
                .expect("endpoint"),
                node_id: node_id.clone(),
                transport: ClusterTransportKind::LanTcp,
                address: format!("127.0.0.1:{}", 20_000 + index),
                source: sinomo_core::ClusterEndpointSource::Static,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 0,
                last_observed_latency_ms: None,
            })
            .collect();

        let member_key = cluster_member_key(&node_id);
        let policy_key = sinomo_network::cluster_capability_policy_key(&node_id);
        let endpoint_prefix = sinomo_network::cluster_endpoint_prefix(&node_id);
        let mut store = InProcessKvStore::new();
        store
            .put_serde(member_key.clone(), &member, None)
            .expect("member");
        store
            .put_serde(
                policy_key.clone(),
                &sinomo_network::ClusterCapabilityPolicyRecord::from(&member),
                None,
            )
            .expect("policy");
        for endpoint in &member.endpoints {
            store
                .put_serde(
                    sinomo_network::cluster_endpoint_key(&node_id, endpoint),
                    endpoint,
                    None,
                )
                .expect("endpoint");
        }

        let txns =
            cluster_member_removal_txns(&store, std::slice::from_ref(&member)).expect("txns");

        assert!(
            txns.len() > 1,
            "large endpoint prefixes should be split before final removal"
        );
        assert!(
            txns.iter()
                .all(|txn| txn.success.len() <= sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH),
            "all removal transactions must fit within the KV branch op limit"
        );
        for txn in &txns[..txns.len() - 1] {
            assert!(
                txn.success.iter().all(|op| !matches!(
                    op,
                    KvOp::DeletePrefix { prefix } if prefix == &endpoint_prefix
                )),
                "pre-final removal batches should use explicit deletes"
            );
        }
        assert!(
            txns.last().is_some_and(|txn| txn.success.iter().any(|op| {
                matches!(op, KvOp::DeletePrefix { prefix } if prefix == &endpoint_prefix)
            })),
            "final removal should keep a prefix cleanup for residual endpoint keys"
        );

        for txn in txns {
            store
                .apply_command(KvCommand::Txn(txn))
                .expect("apply removal txn");
        }

        assert!(store.get(&member_key).is_none(), "member should be removed");
        assert!(store.get(&policy_key).is_none(), "policy should be removed");
        assert!(
            store.range_prefix(&endpoint_prefix).is_empty(),
            "endpoint records should be removed"
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batches_are_bounded_and_ordered() {
        let events = (0..(sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS * 2 + 3))
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: index.to_string().into_bytes(),
                },
            })
            .collect::<Vec<_>>();

        let batches = cluster_kv_watch_event_fanout_batches(&events).collect::<Vec<_>>();
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| {
            !batch.is_empty()
                && batch.len() <= sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS
                && sinomo_network::cluster_kv_watch_events_payload_bytes(batch)
                    <= sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
        }));
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.iter().map(|event| event.revision))
                .collect::<Vec<_>>(),
            events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batches_split_on_payload_size() {
        let value_len = sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES / 2;
        let events = (0..3)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index + 1),
                key: format!("/watch/fanout/payload/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: vec![index as u8; value_len],
                },
            })
            .collect::<Vec<_>>();

        let batches = cluster_kv_watch_event_fanout_batches(&events).collect::<Vec<_>>();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert!(batches.iter().all(|batch| {
            sinomo_network::cluster_kv_watch_events_payload_bytes(batch)
                <= sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
        }));
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.iter().map(|event| event.revision))
                .collect::<Vec<_>>(),
            events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cluster_raft_append_request_only_fans_out_watch_events_missing_from_snapshot() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_needed").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_needed_leader").expect("leader");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout needed",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let first = KvEvent {
            revision: sinomo_kv::KvRevision(1),
            key: "/watch/fanout/first".to_string(),
            kind: sinomo_kv::KvEventKind::Put {
                value: b"first".to_vec(),
            },
        };
        let second = KvEvent {
            revision: sinomo_kv::KvRevision(2),
            key: "/watch/fanout/second".to_string(),
            kind: sinomo_kv::KvEventKind::Put {
                value: b"second".to_vec(),
            },
        };
        let append = sinomo_kv::RaftAppendEntriesRequest {
            term: 1,
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 1,
        };
        let mut request = ClusterRaftAppendRequest {
            state: state.clone(),
            append,
            snapshot: None,
        };

        assert!(
            !cluster_raft_append_request_needs_watch_event_fanout(
                &request,
                std::slice::from_ref(&first)
            ),
            "incremental appends already apply and wake the follower's local watch state"
        );

        let mut snapshot = InProcessKvStore::new().snapshot();
        snapshot.events = vec![first.clone()];
        request.snapshot = Some(sinomo_kv::RaftSnapshot {
            last_included_index: 1,
            last_included_term: 1,
            kv: snapshot,
        });
        assert!(
            !cluster_raft_append_request_needs_watch_event_fanout(
                &request,
                std::slice::from_ref(&first)
            ),
            "snapshot installs that still contain the committed events do not need duplicate fanout"
        );
        assert!(
            cluster_raft_append_request_needs_watch_event_fanout(&request, &[first, second]),
            "snapshot installs with a trimmed event tail still need fanout for omitted watch events"
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_rejects_oversized_batches_before_storage() {
        let root = unique_cluster_test_root("sinomo-daemon-watch-fanout-oversized");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_oversized").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_oversized_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout oversized",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let events = (0..=sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/oversized/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: index.to_string().into_bytes(),
                },
            })
            .collect::<Vec<_>>();

        let response = handle_daemon_cluster_kv_watch_events_request(
            &daemon_paths,
            ClusterKvWatchEventsRequest {
                state,
                leader_id,
                events,
            },
        )
        .expect("watch event fanout response");

        assert!(!response.accepted);
        assert_eq!(response.stored_events, 0);
        assert!(
            response.detail.contains("max is"),
            "unexpected detail: {}",
            response.detail
        );
        assert!(
            !cluster_paths.kv_watch_events.exists(),
            "oversized batch must not checkpoint watch events"
        );
        assert!(
            !cluster_paths.kv_watch_events_log.exists(),
            "oversized batch must not append watch events"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cluster_kv_watch_event_fanout_rejects_oversized_payload_before_storage() {
        let root = unique_cluster_test_root("sinomo-daemon-watch-fanout-oversized-payload");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_payload").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_payload_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout payload",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let value_len = sinomo_network::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES / 2;
        let events = (0..2)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/payload/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: vec![index as u8; value_len],
                },
            })
            .collect::<Vec<_>>();

        let response = handle_daemon_cluster_kv_watch_events_request(
            &daemon_paths,
            ClusterKvWatchEventsRequest {
                state,
                leader_id,
                events,
            },
        )
        .expect("watch event fanout response");

        assert!(!response.accepted);
        assert_eq!(response.stored_events, 0);
        assert!(
            response.detail.contains("payload bytes"),
            "unexpected detail: {}",
            response.detail
        );
        assert!(
            !cluster_paths.kv_watch_events.exists(),
            "oversized payload must not checkpoint watch events"
        );
        assert!(
            !cluster_paths.kv_watch_events_log.exists(),
            "oversized payload must not append watch events"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cluster_raft_fanout_handoff_claims_pending_pass_after_clearing_active() {
        let active = AtomicBool::new(true);
        let pending = AtomicBool::new(false);
        assert!(!claim_next_cluster_raft_fanout_pass(&active, &pending));
        assert!(!active.load(Ordering::Acquire));

        active.store(true, Ordering::Release);
        pending.store(true, Ordering::Release);
        assert!(claim_next_cluster_raft_fanout_pass(&active, &pending));
        assert!(active.load(Ordering::Acquire));
    }

    #[test]
    fn committed_cluster_raft_command_skips_fanout_without_remote_targets() {
        assert!(
            !committed_cluster_raft_command_needs_fanout(&[]),
            "a solo voter commit should not spawn a background replication pass"
        );

        let node_id = NodeId::from_legacy_named("node_daemon_fanout_remote_target").expect("node");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_network::ClusterEndpointId::from_legacy_named(
                "endpoint_daemon_fanout_remote_target",
            )
            .expect("endpoint"),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: "127.0.0.1:21353".to_string(),
            source: sinomo_core::ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        assert!(
            committed_cluster_raft_command_needs_fanout(&[endpoint]),
            "remote voter targets still need a post-commit pass to advance follower commit indexes"
        );
    }

    #[test]
    fn maintenance_failure_backoff_retries_after_configured_delay() {
        let now = Instant::now();
        let last_publish = maintenance_last_timestamp_for_next_due(
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
            !maintenance_interval_due(
                before_backoff,
                last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL
            ),
            "failed publishes should not retry before the configured backoff"
        );
        assert!(
            maintenance_interval_due(after_backoff, last_publish, DAEMON_CLUSTER_PUBLISH_INTERVAL),
            "failed publishes should retry at the configured backoff, not backoff plus the publish interval"
        );
        assert!(
            !maintenance_interval_due(now, after_backoff, DAEMON_CLUSTER_PUBLISH_INTERVAL),
            "future deferred timestamps should be safe and not immediately due"
        );
    }

    #[test]
    fn cluster_has_remote_peers_treats_main_and_shadow_as_solo() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_solo_check").expect("cluster");
        let local = NodeId::from_legacy_named("node_solo_check_main").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "solo check",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        // Only the local main member: solo.
        assert!(!cluster_has_remote_peers(&state));

        // Adding this node's own shadow process is still solo.
        let shadow = sinomo_node::shadow_daemon_node_id(&local);
        state.members.push(ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            shadow,
            "local shadow",
            "pk-shadow",
            now,
        ));
        assert!(
            !cluster_has_remote_peers(&state),
            "main + shadow on one node must count as solo"
        );

        // A genuine remote member flips the gate.
        let remote = NodeId::from_legacy_named("node_solo_check_remote").expect("remote");
        state.members.push(ClusterMemberRecord::local_voter(
            cluster_id,
            remote,
            "remote",
            "pk-remote",
            now,
        ));
        assert!(cluster_has_remote_peers(&state));
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
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 0,
            last_publish: maintenance_now,
            last_discovery: maintenance_now
                .checked_sub(DAEMON_CLUSTER_DISCOVERY_INTERVAL)
                .unwrap_or(maintenance_now),
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        maintenance
            .tick(
                &DaemonPaths::with_root(data_root.join("daemon")),
                &DaemonRunOptions::default(),
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
        let _ = std::fs::remove_dir_all(root);
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
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 0,
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

        maintenance
            .tick(&daemon_paths, &DaemonRunOptions::default())
            .expect("first unclustered tick");
        maintenance
            .tick(&daemon_paths, &DaemonRunOptions::default())
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
            !maintenance_interval_due(
                after_ticks,
                maintenance.last_publish,
                DAEMON_CLUSTER_PUBLISH_INTERVAL,
            ),
            "absent cluster state should advance publish timer"
        );
        assert!(
            !maintenance_interval_due(
                after_ticks,
                maintenance.last_probe,
                DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL,
            ),
            "absent cluster state should advance probe timer"
        );

        let _ = std::fs::remove_dir_all(root);
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
        let maintenance_now = Instant::now();
        let mut maintenance = ClusterNetworkMaintenance {
            discovery: None,
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            connectivity_cursor: 0,
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 1,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        assert!(
            maintenance.apply_discovery_advertisements(&mut state, vec![advertisement.clone()]),
            "first discovery should add the remote endpoint"
        );
        assert_eq!(state.members[1].endpoints, vec![endpoint]);
        assert!(
            !maintenance.apply_discovery_advertisements(&mut state, vec![advertisement]),
            "replayed identical discovery should not force a cluster-state write"
        );
    }

    #[test]
    fn member_publish_authorizes_signer_shadow_daemon() {
        let signer =
            NodeId::from_legacy_named("node_member_publish_shadow_signer").expect("signer");
        let shadow = shadow_daemon_node_id(&signer);
        let other = NodeId::from_legacy_named("node_member_publish_shadow_other").expect("other");

        assert!(member_publish_authorized_for_signer(
            &signer,
            signer.as_str()
        ));
        assert!(member_publish_authorized_for_signer(
            &shadow,
            signer.as_str()
        ));
        assert!(!member_publish_authorized_for_signer(
            &other,
            signer.as_str()
        ));
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
        let holder = thread::spawn(move || {
            let _raft_guard = lock_cluster_raft_operation().expect("raft operation lock");
            let _node =
                sinomo_network::open_cluster_raft_kv_node(&paths_for_holder, &local_for_holder)
                    .expect("hold raft node");
            held_tx.send(()).expect("notify lock held");
            thread::sleep(Duration::from_millis(100));
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
        let _ = std::fs::remove_dir_all(root);
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
        let wal_path = sinomo_network::cluster_raft_wal_paths(&paths, &local).wal;
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
        let _ = std::fs::remove_dir_all(root);
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
        sinomo_network::clear_cluster_raft_leader(&paths, &state.local_node_id)
            .expect("clear known leader");
        assert_eq!(
            sinomo_network::cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
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
            sinomo_network::cluster_raft_known_leader_id(&paths, &state).expect("known leader"),
            None
        );
        let _ = std::fs::remove_dir_all(root);
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
        assert_eq!(status.process, crate::DAEMON_SHADOW_CLUSTER_ROLE);
        assert_eq!(status.member_role, ClusterMemberRole::NonVoting);
        assert_eq!(status.member_state, ClusterMemberState::NonVoting);

        std::fs::remove_dir_all(root).ok();
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
        let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
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
                node_id: shadow_node_id.clone(),
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
        let shadow_member = updated
            .members
            .iter()
            .find(|member| member.node_id == shadow_node_id)
            .expect("shadow member");
        assert!(shadow_member.endpoints.iter().any(|endpoint| {
            endpoint.transport == ClusterTransportKind::Iroh
                && endpoint.address
                    == "iroh://iroh_public_key_shadow_repair_route?relay=https://relay.example./"
        }));

        std::fs::remove_dir_all(root).ok();
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

        remember_connectivity_records(&paths_a, std::slice::from_ref(&record));
        assert!(cached_connectivity_matches(&paths_a, &record));
        assert!(
            !cached_connectivity_matches(&paths_b, &record),
            "connectivity publish cache must not cross data directories"
        );

        let mut near_expiry = record;
        near_expiry.expires_at = now + time::Duration::seconds(30);
        remember_connectivity_records(&paths_b, std::slice::from_ref(&near_expiry));
        assert!(
            !cached_connectivity_matches(&paths_b, &near_expiry),
            "stable cached connectivity must be publishable before its KV record expires"
        );

        std::fs::remove_dir_all(root_a).ok();
        std::fs::remove_dir_all(root_b).ok();
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

        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::write_cluster_kv_snapshot(&paths, &store.snapshot())
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

        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::apply_cluster_raft_role(
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
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 0,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let records = maintenance.probe_connectivity(&paths, &state);

        assert_eq!(records, vec![record]);
        assert_eq!(records[0].source_node_id, local_node);
        assert_eq!(records[0].target_node_id, remote_node);
        std::fs::remove_dir_all(root).ok();
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
            environment_id: None,
            last_desired_rollout_attempt: None,
            tick: 0,
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: Arc::new(IdleStatsAtomics::new()),
        };

        let records = maintenance.probe_connectivity(&paths, &state);

        assert!(
            records.is_empty(),
            "removed endpoints should not keep stale connectivity records publishable"
        );
        assert!(
            !maintenance.connectivity.contains_key(&key),
            "removed endpoint connectivity should be dropped from the local cache"
        );
        std::fs::remove_dir_all(root).ok();
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
        let seeded =
            sinomo_network::read_cluster_kv_current_snapshot(&paths).expect("seeded snapshot");
        assert!(
            seeded.get(&current_key).is_some(),
            "current connectivity should be seeded"
        );
        assert!(
            seeded.get(&stale_key).is_some(),
            "stale connectivity should be seeded"
        );
        assert!(
            sinomo_network::cluster_connectivity_full_sync_ops(
                &paths,
                &local_node,
                std::slice::from_ref(&current_record),
            )
            .expect("planned connectivity sync")
            .iter()
            .any(|op| matches!(op, KvOp::Delete { key } if key == &stale_key)),
            "planner should delete stale connectivity"
        );

        sync_cluster_connectivity_records_to_kv(&paths, &local_node, vec![current_record.clone()])
            .expect("sync should delete stale connectivity");

        let snapshot = sinomo_network::read_cluster_kv_current_snapshot(&paths).expect("snapshot");
        assert!(
            snapshot.get(&current_key).is_some(),
            "current connectivity should remain"
        );
        assert!(
            snapshot.get(&stale_key).is_none(),
            "stale connectivity should be deleted even when current records are cached"
        );
        std::fs::remove_dir_all(root).ok();
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
                sinomo_network::append_cluster_raft_command(
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
            sinomo_network::append_cluster_raft_command(
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

        let snapshot = sinomo_network::read_cluster_kv_current_snapshot(&paths).expect("snapshot");
        assert!(
            snapshot
                .prefix_entries(&sinomo_network::cluster_connectivity_source_prefix(
                    &local_node
                ))
                .next()
                .is_none(),
            "daemon connectivity sync should delete large stale prefixes in bounded batches"
        );
        std::fs::remove_dir_all(root).ok();
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
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &remote_node,
                "iroh_public_key_daemon_raft_connectivity",
            ),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address:
                "iroh://iroh_public_key_daemon_raft_connectivity?relay=https://relay.example./"
                    .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };
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

        let targets = sinomo_network::cluster_raft_command_targets(&paths, &state);

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
        std::fs::remove_dir_all(root).ok();
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
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &remote_node,
                "iroh_public_key_daemon_raft_unknown_tcp",
            ),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_public_key_daemon_raft_unknown_tcp?relay=https://relay.example./"
                .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };
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

        let targets = sinomo_network::cluster_raft_command_targets(&paths, &state);

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
        std::fs::remove_dir_all(root).ok();
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
        let first_iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &first_node,
                "iroh_public_key_daemon_raft_interleave_first",
            ),
            node_id: first_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address:
                "iroh://iroh_public_key_daemon_raft_interleave_first?relay=https://relay.example./"
                    .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };
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

        let targets = sinomo_network::cluster_raft_command_targets(&paths, &state);
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
        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::sync_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id: cluster_id.clone(),
                node_id: remote_node.clone(),
                process: "daemon-main".to_string(),
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
            cluster_node_status_blocks_leader_preference(&paths, &state, &remote_node),
            "a remote member the active probes cannot reach must not be preferred as leader"
        );
        assert!(
            !cluster_node_status_is_offline(&paths, &remote_node),
            "an unreachable member that never self-declared Offline is not explicit offline"
        );
        assert_eq!(
            cluster_raft_live_voter_targets(&paths, &state).len(),
            1,
            "an unreachable member must remain a voter while connectivity is recovering"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn raft_command_timeout_caps_single_iroh_attempt_to_preserve_fallback_budget() {
        let node_id = NodeId::from_legacy_named("node_daemon_raft_iroh_timeout").expect("node");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &node_id,
                "iroh_public_key_daemon_raft_timeout",
            ),
            node_id,
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_public_key_daemon_raft_timeout?relay=https://relay.example./"
                .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };

        assert_eq!(
            cluster_endpoint_command_timeout(&endpoint, DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT),
            DAEMON_CLUSTER_IROH_COMMAND_TIMEOUT
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
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &node_id,
                "iroh_public_key_daemon_raft_foreground",
            ),
            node_id,
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_public_key_daemon_raft_foreground?relay=https://relay.example./"
                .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };

        assert_eq!(
            cluster_endpoint_fair_command_timeout(&lan, DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT, 8),
            DAEMON_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            cluster_endpoint_fair_command_timeout(&public, DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT, 8),
            DAEMON_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            cluster_endpoint_fair_command_timeout(&iroh, DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT, 8),
            DAEMON_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            cluster_endpoint_fair_command_timeout(&lan, Duration::from_millis(50), 8),
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
                DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT,
                &snapshot_request
            ),
            DAEMON_CLUSTER_RAFT_APPEND_TIMEOUT
        );
    }
}
