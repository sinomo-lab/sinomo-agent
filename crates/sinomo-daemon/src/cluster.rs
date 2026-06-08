use crate::client::fetch_cluster_kv_snapshot_from_endpoint;
use crate::dfs::ensure_default_distributed_dfs;
#[cfg(test)]
use crate::status::ensure_daemon_cluster_members;
#[cfg(test)]
use crate::{AuthenticatedDaemonCommand, DaemonClusterRegistration};
use crate::{
    DaemonClusterStatus, DaemonError, DaemonPaths, DaemonProbeStatus, DaemonRunOptions,
    IdleStatsAtomics, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_DEFAULT_PORT, cluster_node_status_from_daemon_status,
    current_daemon_cluster_options, current_shadow_daemon_options,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths, daemon_cluster_status,
    daemon_cluster_status_matches, daemon_connect_address, daemon_options_from_status,
    daemon_paths_are_shadow, fetch_daemon_status_from_endpoint,
    handle_daemon_rollout_build_request, probe_daemon_status, read_daemon_status,
    send_cluster_kv_txn, send_cluster_kv_watch_events, send_cluster_raft_append,
    send_cluster_raft_vote, write_daemon_status,
};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeRole,
    ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
};
#[cfg(test)]
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterJoinInvite, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, ClusterTransportKind, TrustDomain,
};
#[cfg(test)]
use sinomo_kv::KvOp;
use sinomo_kv::{ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse};
use sinomo_kv::{InProcessKvStore, KvEvent, KvSnapshot, KvTxn, KvTxnResponse};
#[cfg(test)]
use sinomo_kv::{KvCommand, KvCompare, KvOpResponse};
use sinomo_local::SinomoDataDir;
#[cfg(test)]
use sinomo_network::{
    ClusterConnectivityProbeState, ClusterId, ClusterInviteId, ClusterJoinRegistrationResponse,
    MdnsDiscoveryAdvertisement, cluster_member_key,
};
use sinomo_network::{
    ClusterConnectivityRecord, ClusterHealthRow, ClusterInviteChallengeProof,
    ClusterInviteChallengeRequest, ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    ClusterNetworkInfoRecord, ClusterPaths, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse, ClusterStatusProbe, IrohEndpointRecord,
    MdnsDiscoveryCache, MdnsDiscoverySocket, ObjectRpcRequest, ObjectRpcResponse,
};
use sinomo_node::NodeId;
use sinomo_node::shadow_daemon_node_id;
use sinomo_node::{load_or_create_main_node_identity, load_or_create_shadow_node_identity};
use sinomo_rollout::{
    ClusterRolloutBuildRequest, SelfRolloutPaths, managed_source_status, read_rollout_status,
    source_heads_match,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT: Duration = Duration::from_millis(1500);
pub const DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLUSTER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_millis(750);
pub const DAEMON_CLUSTER_TICK_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(test)]
const DAEMON_CLUSTER_CONVERGENCE_TICK_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL;
pub const DAEMON_CLUSTER_PUBLISH_INTERVAL: Duration =
    sinomo_network::CLUSTER_NETWORK_PUBLISH_INTERVAL;
pub const DAEMON_CLUSTER_PUBLISH_FAILURE_BACKOFF: Duration = Duration::from_secs(600);
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
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = sinomo_network::CLUSTER_CONNECTIVITY_TTL_SECONDS;
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 =
    sinomo_network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS;
#[cfg(test)]
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 =
    sinomo_network::CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS;
const CLUSTER_HEALTH_PEER_ROWS_TIMEOUT: Duration = Duration::from_secs(8);
pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 =
    sinomo_network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD;
const PUBLISHED_NODE_CACHE_MAX_ENTRIES: usize = sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_SOURCES: usize =
    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS_PER_SOURCE: usize =
    sinomo_network::MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS;
const PUBLISHED_CONNECTIVITY_CACHE_MAX_RECORDS: usize =
    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
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
) {
    if sinomo_network::cluster_raft_local_is_voter(state)
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
        let _ = sync_cluster_connectivity_records_to_kv(paths, &state.local_node_id, records);
        in_flight.set(false);
    });
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
    let observed_failures = if is_healthy {
        0
    } else {
        previous
            .as_ref()
            .map(|record| record.consecutive_failures.saturating_add(1))
            .unwrap_or(1)
    };
    let consecutive_failures = observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
    let observed_connectivity_state = if is_healthy {
        sinomo_network::ClusterConnectivityState::Healthy
    } else if matches!(
        connectivity_state,
        sinomo_network::ClusterConnectivityState::Unreachable
    ) || consecutive_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
    {
        sinomo_network::ClusterConnectivityState::Unreachable
    } else {
        connectivity_state.clone()
    };
    let last_success_at = if is_healthy {
        Some(timestamp.clone())
    } else if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
        previous
            .as_ref()
            .and_then(|record| record.last_success_at.clone())
    } else {
        None
    };
    let last_failure_at = if is_healthy {
        None
    } else {
        Some(timestamp.clone())
    };
    let ttl_seconds = if matches!(
        observed_connectivity_state,
        sinomo_network::ClusterConnectivityState::Unreachable
    ) {
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS
    } else {
        CLUSTER_CONNECTIVITY_TTL_SECONDS
    };
    let record = sinomo_network::cluster_connectivity_record(
        state,
        endpoint,
        observed_connectivity_state,
        latency_ms,
        consecutive_failures,
        last_success_at,
        last_failure_at,
        now,
        ttl_seconds,
    );
    if sinomo_network::write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )
    .is_ok()
    {
        publish_command_connectivity_if_needed(paths, state, &record);
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
            let mut proxy_start_failures = 0_u32;
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
                            thread::sleep(retry_delay);
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
                thread::sleep(sinomo_network::cluster_iroh_proxy_republish_sleep_duration(
                    Instant::now(),
                    next_republish,
                ));
            }
        })
        .map(|_| ())
        .map_err(|source| DaemonError::Io {
            path: "spawn iroh proxy thread".to_string(),
            detail: source.to_string(),
        })
}

pub(crate) fn daemon_cluster_status_from_paths(
    paths: &DaemonPaths,
) -> Result<Option<DaemonClusterStatus>, DaemonError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
    Ok(status.cluster)
}

pub(crate) fn refresh_daemon_iroh_repair_route(paths: &DaemonPaths) -> Result<bool, DaemonError> {
    let Some(cluster) = daemon_cluster_status_from_paths(paths)? else {
        return Ok(false);
    };
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    match sinomo_network::read_iroh_endpoint_record(&cluster_paths, &cluster.node_id) {
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

pub(crate) fn start_shadow_daemon_status_publisher(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let status_paths = paths.clone();
    thread::Builder::new()
        .name("sinomo-shadow-status".to_string())
        .spawn(move || {
            let iroh_disabled = sinomo_network::iroh_disabled_by_env();
            let mut startup_retries_remaining = DAEMON_SHADOW_STATUS_STARTUP_RETRY_ATTEMPTS;
            let mut publish_interval = DAEMON_SHADOW_STATUS_STARTUP_RETRY_INTERVAL;
            loop {
                thread::sleep(publish_interval);
                let settled_solo =
                    daemon_paths_have_settled_solo_cluster(&status_paths).unwrap_or(false);
                let mut status_publish_outcome = None;
                if !settled_solo {
                    if !iroh_disabled {
                        if let Err(error) = refresh_daemon_iroh_repair_route(&status_paths) {
                            eprintln!(
                                "sinomo shadow daemon iroh repair route tick failed: {error}"
                            );
                        }
                    }
                    match refresh_daemon_node_status_in_kv(&status_paths) {
                        Ok(outcome) => status_publish_outcome = Some(outcome),
                        Err(error) => {
                            eprintln!("sinomo shadow daemon cluster status tick failed: {error}")
                        }
                    }
                }
                if startup_retries_remaining > 0 {
                    startup_retries_remaining = startup_retries_remaining.saturating_sub(1);
                    let clustered = daemon_cluster_status_from_paths(&status_paths)
                        .ok()
                        .flatten()
                        .is_some();
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
                    daemon_cluster_publish_interval(&status_paths)
                };
            }
        })
        .map(|_| ())
        .map_err(|source| DaemonError::Io {
            path: "spawn shadow status thread".to_string(),
            detail: source.to_string(),
        })
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

pub(crate) fn advertise_daemon_iroh_endpoint(
    paths: &DaemonPaths,
    record: IrohEndpointRecord,
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
    if let Some(published_member) = sinomo_network::apply_cluster_iroh_endpoint_advertisement(
        &mut state,
        record,
        persisted_endpoints.as_deref(),
        OffsetDateTime::now_utc(),
    ) {
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

pub(crate) fn handle_daemon_cluster_kv_snapshot(
    paths: &DaemonPaths,
) -> Result<KvSnapshot, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    sinomo_network::read_cluster_kv_store(&cluster_paths)
        .map(|store| store.snapshot())
        .map_err(Into::into)
}

pub(crate) fn prune_local_cluster_raft_uncommitted_tail(
    paths: &DaemonPaths,
) -> Result<(), DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let Ok(state) = sinomo_network::read_cluster_state_without_kv(&cluster_paths) else {
        return Ok(());
    };
    sinomo_network::locked_discard_cluster_raft_uncommitted_tail(
        &cluster_paths,
        &state.local_node_id,
    )?;
    Ok(())
}

pub(crate) fn handle_daemon_cluster_kv_txn_request(
    paths: &DaemonPaths,
    txn: KvTxn,
) -> Result<KvTxnResponse, DaemonError> {
    handle_daemon_cluster_kv_txn_request_with_forwarding(paths, txn, true)
}

pub(crate) fn handle_forwarded_daemon_cluster_kv_txn_request(
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
    handle_cluster_kv_txn_request_with_cluster_paths(&cluster_paths, txn, allow_forwarding)
}

pub(crate) fn handle_cluster_kv_txn_request_with_cluster_paths(
    cluster_paths: &ClusterPaths,
    txn: KvTxn,
    allow_forwarding: bool,
) -> Result<KvTxnResponse, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::handle_cluster_kv_txn_request_with_host(
        cluster_paths,
        txn,
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )
    .map_err(Into::into)
}

pub(crate) struct DaemonClusterJoinRegistrationHost<'a> {
    driver: sinomo_network::ClusterJoinRegistrationDriver<
        DaemonClusterControlPlaneTransport,
        DaemonLocalDaemonMembershipObserver<'a>,
    >,
}

impl<'a> DaemonClusterJoinRegistrationHost<'a> {
    pub(crate) fn new(daemon_paths: &'a DaemonPaths) -> Self {
        Self {
            driver: sinomo_network::ClusterJoinRegistrationDriver::new(
                daemon_cluster_control_plane_driver(),
                DaemonLocalDaemonMembershipObserver { daemon_paths },
            ),
        }
    }
}

struct DaemonLocalDaemonMembershipObserver<'a> {
    daemon_paths: &'a DaemonPaths,
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
        local_daemon_membership_observation_from_daemon_paths(self.daemon_paths, state)
            .map(Some)
            .map_err(|error| daemon_host_network_error("cluster join local membership", error))
    }
}

impl sinomo_network::ClusterMembershipRpcForwardHost for DaemonClusterJoinRegistrationHost<'_> {
    fn send_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_network::ObjectRpcRequest,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        sinomo_network::ClusterMembershipRpcForwardHost::send_membership_rpc(
            &mut self.driver,
            endpoint,
            request,
        )
    }

    fn relay_membership_rpc_to_voter_via_peer(
        &mut self,
        state: &ClusterBootstrapState,
        voter: &ClusterEndpointRecord,
        peer: &ClusterEndpointRecord,
        request: &sinomo_network::ObjectRpcRequest,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        sinomo_network::ClusterMembershipRpcForwardHost::relay_membership_rpc_to_voter_via_peer(
            &mut self.driver,
            state,
            voter,
            peer,
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
        sinomo_network::ClusterJoinRegistrationHost::remember_successful_join_endpoint(
            &mut self.driver,
            paths,
            state,
            endpoint,
        )
    }
}

pub(crate) fn forward_cluster_invite_challenge_to_voter(
    paths: &DaemonPaths,
    challenge: &ClusterInviteChallengeRequest,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let rpc_request = sinomo_network::cluster_join_challenge_rpc_request(challenge)?;
    let mut host = daemon_cluster_control_plane_driver();
    let response = sinomo_network::forward_cluster_join_challenge_rpc_to_voter_with_host(
        &cluster_paths,
        &rpc_request,
        &mut host,
    )?;
    response.decode_body().map_err(DaemonError::from)
}

fn relay_rpc_to_node_via_peer(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    peer: &ClusterEndpointRecord,
    rpc_request: &sinomo_network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let mut host = DaemonSignedRelayRpcHost;
    sinomo_network::relay_rpc_to_node_via_peer_with_host(
        state,
        target_node_id,
        peer,
        rpc_request,
        crate::daemon_rpc_command(&rpc_request.route),
        &crate::daemon_rpc_command(sinomo_network::CLUSTER_RELAY_ROUTE),
        sinomo_network::ClusterSignedRelayRpcOptions { timeout },
        &mut host,
    )
    .map_err(DaemonError::from)
}

struct DaemonSignedRelayRpcHost;

impl sinomo_network::ClusterSignedRelayRpcHost for DaemonSignedRelayRpcHost {
    fn sign_relay_rpc_request(
        &mut self,
        request: &sinomo_network::ObjectRpcRequest,
    ) -> Result<Vec<u8>, sinomo_network::NetworkError> {
        crate::sign_daemon_rpc_request_for_local_node(request)
            .map_err(|error| daemon_host_network_error("cluster relay signing", error))
    }

    fn send_signed_relay_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        command: &str,
        auth: &[u8],
        request: &sinomo_network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        sinomo_network::send_daemon_rpc_to_cluster_endpoint(
            endpoint,
            command,
            Some(auth),
            request,
            timeout,
        )
    }
}

pub(crate) fn publish_local_members_via_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::publish_local_members_via_voter_with_host(
        paths,
        state,
        sinomo_network::ClusterLocalMemberPublishOptions {
            member_publish_timeout: DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
        },
        &mut host,
    )
    .map_err(Into::into)
}

pub(crate) fn publish_local_members_to_quorum_if_needed(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::publish_local_members_to_quorum_if_needed_with_host(
        paths,
        state,
        sinomo_network::ClusterLocalMemberPublishOptions {
            member_publish_timeout: DAEMON_CLUSTER_MEMBER_PUBLISH_TIMEOUT,
        },
        &mut host,
    )
    .map_err(Into::into)
}

#[derive(Debug)]
pub(crate) struct ClusterNetworkMaintenance {
    pub discovery: Option<MdnsDiscoverySocket>,
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
    pub last_publish: Instant,
    pub last_discovery: Instant,
    pub last_probe: Instant,
    pub idle_stats: Arc<IdleStatsAtomics>,
}

fn daemon_cluster_publish_interval(paths: &DaemonPaths) -> Duration {
    let Ok(Some(cluster)) = daemon_cluster_status_from_paths(paths) else {
        return DAEMON_CLUSTER_PUBLISH_INTERVAL;
    };
    let Ok(cluster_paths) = cluster_paths_from_daemon_paths(paths) else {
        return DAEMON_CLUSTER_PUBLISH_INTERVAL;
    };
    sinomo_network::cluster_network_publish_interval_for_node(&cluster_paths, &cluster.node_id)
}

impl ClusterNetworkMaintenance {
    pub fn with_idle_stats(idle_stats: Arc<IdleStatsAtomics>) -> Self {
        let now = Instant::now();
        Self {
            discovery: MdnsDiscoverySocket::bind_default().ok(),
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
            return Ok(());
        }

        let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        if !cluster_paths.state.exists() {
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
            return Ok(());
        }

        let mut discovery_already_processed = false;
        if self.solo_cluster_idle && (publish_due || probe_due || discovery_due) {
            self.idle_stats.incr_disk_reads_cluster_state();
            let mut state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
            self.local_node_role = sinomo_network::cluster_local_node_role(&state);
            let mut discovered = false;
            if discovery_due {
                self.last_discovery = now;
                discovered = self.process_discovery(&mut state);
                if sinomo_network::cluster_network_stable_discovery_announcement_due(
                    self.tick, discovered,
                ) {
                    self.announce(&state);
                }
                discovery_already_processed = true;
                if discovered {
                    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
                }
            }
            if !discovered && sinomo_network::cluster_is_settled_solo(&state) {
                if publish_due {
                    self.last_publish = now;
                }
                if probe_due {
                    self.last_probe = now;
                }
                return Ok(());
            }
            self.solo_cluster_idle = false;
        }

        self.idle_stats.incr_ticks_with_work();
        self.idle_stats.incr_disk_reads_cluster_state();
        let mut state = if publish_due || probe_due {
            sinomo_network::read_cluster_state(&cluster_paths)?
        } else {
            sinomo_network::read_cluster_state_without_kv(&cluster_paths)?
        };
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
            // The join command owns registration while this state is pending, so
            // maintenance must stay read-only. Keep the convergence poll active,
            // though: the join may be accepted immediately after this tick, and a
            // full publish-interval sleep would delay the first stable member
            // publish/import past the idle-write gate.
            self.connectivity_converged = false;
            self.startup_settle_until = now.checked_add(DAEMON_CLUSTER_STARTUP_SETTLE_DURATION);
            self.solo_cluster_idle = false;
            return Ok(());
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
            return Ok(());
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
            sinomo_network::cleanup_cluster_iroh_endpoints(&mut state);
            let shadow_options =
                current_shadow_daemon_options(data_dir, DAEMON_CLUSTER_JOIN_STATUS_TIMEOUT);
            self.idle_stats.incr_disk_reads_iroh_endpoints();
            let local_state_changed = refresh_local_daemon_membership(
                &cluster_paths,
                &mut state,
                options,
                &shadow_options,
                OffsetDateTime::now_utc(),
            )?
            .changed();
            if local_state_changed {
                self.connectivity_converged = false;
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
        let cluster_is_solo = sinomo_network::cluster_is_settled_solo(&state);
        sinomo_network::refresh_cluster_connectivity_environment(
            &cluster_paths,
            &state,
            &mut self.environment_id,
            &mut self.connectivity,
            OffsetDateTime::now_utc(),
        )?;

        if publish_due {
            self.last_publish = now;
            let mut publish_failed = false;
            if cluster_is_solo {
                // no-op: nothing to publish to absent peers
            } else if sinomo_network::cluster_raft_local_is_voter(&state) {
                if crate::dfs::default_distributed_dfs_enabled()
                    && let Err(error) = ensure_default_distributed_dfs(paths, options, true)
                {
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
                self.last_publish =
                    sinomo_network::cluster_network_maintenance_last_timestamp_for_next_due(
                        now,
                        sinomo_network::cluster_network_publish_interval_for_role(
                            &self.local_node_role,
                        ),
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
                    Ok(Some(_)) => {
                        self.connectivity_converged = true;
                        self.idle_stats.incr_kv_publish_connectivity();
                    }
                    Ok(None) => {
                        self.connectivity_converged = true;
                    }
                    Err(error) => {
                        self.connectivity_converged = true;
                        eprintln!("sinomo daemon cluster connectivity publish failed: {error}")
                    }
                }
            } else {
                self.connectivity_converged = true;
            }
        }

        let maintenance_changed = discovered
            || !sinomo_network::cluster_bootstrap_state_stable_eq(
                &state_before_maintenance,
                &state,
            );
        if discovery_due
            && !discovery_already_processed
            && sinomo_network::cluster_network_stable_discovery_announcement_due(
                self.tick,
                maintenance_changed,
            )
        {
            self.announce(&state);
        }

        if maintenance_changed {
            self.connectivity_converged = false;
            sinomo_network::write_cluster_state(&cluster_paths, &state)?;
        }
        self.solo_cluster_idle = cluster_is_solo && !discovered;
        Ok(())
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
        let changed = sinomo_network::apply_lan_discovery_advertisements(
            state,
            &mut self.discovery_cache,
            advertisements,
        );
        self.discovery_cache.expire_after_tick(self.tick);
        changed
    }

    fn announce(&self, state: &ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(advertisement) =
            sinomo_network::lan_discovery_advertisement_for_state(state, self.tick)
        else {
            return;
        };
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

        // Probe every due endpoint in parallel. The network crate owns target
        // selection and connectivity-state transitions; the daemon supplies the
        // process-specific status RPC used to decide whether an endpoint is healthy.
        let timestamp = timestamp.as_str();
        let probed: Vec<(String, ClusterConnectivityRecord)> = std::thread::scope(|scope| {
            let handles: Vec<_> = batch
                .entries
                .iter()
                .map(|entry| {
                    scope.spawn(move || {
                        let started = Instant::now();
                        let result = fetch_daemon_status_from_endpoint_direct(
                            &entry.endpoint,
                            CLUSTER_HEALTH_PROBE_TIMEOUT,
                        );
                        let latency_ms =
                            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        let healthy = result.is_ok_and(|status| {
                            daemon_cluster_status_matches(state, &entry.endpoint, &status)
                        });
                        let record = sinomo_network::cluster_connectivity_record_for_probe_outcome(
                            state,
                            &entry.endpoint,
                            entry.previous.as_ref(),
                            healthy,
                            latency_ms,
                            timestamp,
                            now,
                        );
                        (entry.key.clone(), record)
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().ok())
                .collect()
        });
        sinomo_network::apply_cluster_connectivity_probe_results(
            &mut self.connectivity,
            &batch.active_keys,
            probed,
        )
    }
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
pub(crate) struct NodeStatusPublishOutcome {
    pub network_info_published: bool,
    pub node_status_published: bool,
}

pub(crate) fn refresh_daemon_node_status_in_kv(
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

pub(crate) fn ensure_daemon_status_cluster(
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
    if sinomo_network::cluster_state_needs_join_registration(&state) {
        return Ok(());
    }
    let process = daemon_cluster_process_from_paths(data_dir, paths);
    let (main_options, shadow_options) =
        daemon_cluster_options_from_status(data_dir, paths, status, process);
    let _ = refresh_local_daemon_membership(
        &cluster_paths,
        &mut state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
    )?;
    sinomo_network::write_cluster_state(&cluster_paths, &state)?;
    if let Some(cluster) = daemon_cluster_status(&state, process) {
        status.cluster = Some(cluster);
        write_daemon_status(paths, status)?;
    }
    Ok(())
}

pub(crate) fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, DaemonError> {
    sinomo_network::sync_cluster_state_to_kv_with_host(
        paths,
        state,
        &mut DaemonClusterMemberMutationHost::new(),
    )
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

fn fetch_local_cluster_health_rows_from_member(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<Vec<ClusterHealthRow>> {
    for endpoint in
        sinomo_network::cluster_member_daemon_status_targets_with_connectivity(paths, state, member)
    {
        let Ok(request) = sinomo_network::cluster_local_health_rpc_request() else {
            return None;
        };
        let Ok(response) = crate::send_daemon_rpc_to_endpoint(
            &endpoint,
            &request,
            CLUSTER_HEALTH_PEER_ROWS_TIMEOUT,
        ) else {
            continue;
        };
        let Ok(rows) = sinomo_network::decode_cluster_health_rpc_response(&response) else {
            continue;
        };
        return Some(rows);
    }
    None
}

pub(crate) struct DaemonClusterStatusProbe<'a> {
    paths: &'a ClusterPaths,
}

impl<'a> DaemonClusterStatusProbe<'a> {
    pub(crate) fn new(paths: &'a ClusterPaths) -> Self {
        Self { paths }
    }
}

impl ClusterStatusProbe for DaemonClusterStatusProbe<'_> {
    fn probe_member_status(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<ClusterNodeStatusRecord> {
        probe_member_daemon_status(self.paths, state, member)
    }

    fn peer_local_health_rows(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<Vec<ClusterHealthRow>> {
        fetch_local_cluster_health_rows_from_member(self.paths, state, member)
    }
}

fn probe_member_daemon_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    if sinomo_network::cluster_member_is_local_process(state, member)
        && let Some(status) = probe_local_member_daemon_status(paths, state, member)
    {
        return Some(status);
    }
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

fn probe_local_member_daemon_status(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    let data_dir = paths.root.parent()?;
    let target_paths = if member.node_id == state.local_node_id {
        DaemonPaths::with_root(data_dir.join("daemon"))
    } else if member.node_id == shadow_daemon_node_id(&state.local_node_id) {
        DaemonPaths::with_root(data_dir.join("shadow-daemon"))
    } else {
        return None;
    };
    let status = match probe_daemon_status(&target_paths, CLUSTER_HEALTH_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => return None,
    };
    let cluster = status.cluster.as_ref()?;
    if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
        return None;
    }
    Some(cluster_node_status_from_daemon_status(
        &status,
        cluster,
        None,
        OffsetDateTime::now_utc(),
    ))
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
    sinomo_network::locked_set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
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
    let mut host = DaemonClusterMemberMutationHost::new();
    sinomo_network::update_cluster_node_role_with_host(paths, state, node_id, role, now, &mut host)
        .map_err(Into::into)
}

struct DaemonClusterMemberMutationHost {
    driver: sinomo_network::ClusterMemberMutationDriver<
        DaemonClusterControlPlaneTransport,
        DaemonClusterStateSyncMembershipObserver,
    >,
}

impl DaemonClusterMemberMutationHost {
    fn new() -> Self {
        Self {
            driver: sinomo_network::ClusterMemberMutationDriver::new(
                daemon_cluster_control_plane_driver(),
                DaemonClusterStateSyncMembershipObserver,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DaemonClusterStateSyncMembershipObserver;

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
        local_daemon_membership_observation(
            data_dir,
            state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        )
        .map(Some)
        .map_err(|error| daemon_host_network_error("cluster state membership sync", error))
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

pub(crate) fn daemon_cluster_control_plane_driver()
-> sinomo_network::ClusterControlPlaneDriver<DaemonClusterControlPlaneTransport> {
    sinomo_network::ClusterControlPlaneDriver::with_timeouts(
        DaemonClusterControlPlaneTransport,
        cluster_control_plane_timeouts(),
    )
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DaemonClusterControlPlaneTransport;

impl sinomo_network::ClusterControlPlaneTransport for DaemonClusterControlPlaneTransport {
    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, sinomo_network::NetworkError> {
        send_cluster_raft_append(endpoint, request, timeout).map_err(|error| {
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
        send_cluster_raft_vote(endpoint, request, timeout).map_err(|error| {
            sinomo_network::NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: error.to_string(),
            }
        })
    }

    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        fetch_daemon_status_from_endpoint(endpoint, timeout).is_ok()
    }

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, sinomo_network::NetworkError> {
        send_cluster_kv_txn(endpoint, txn, timeout)
            .map_err(|error| daemon_host_network_error("cluster kv txn", error))
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, sinomo_network::NetworkError> {
        fetch_cluster_kv_snapshot_from_endpoint(endpoint, timeout)
            .map_err(|error| daemon_host_network_error("cluster kv snapshot", error))
    }

    fn send_cluster_kv_watch_events(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> Result<ClusterKvWatchEventsResponse, sinomo_network::NetworkError> {
        send_cluster_kv_watch_events(endpoint, request, timeout)
            .map_err(|error| daemon_host_network_error("cluster kv watch-event fanout", error))
    }

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, sinomo_network::NetworkError> {
        crate::client::send_cluster_member_publish(endpoint, request, timeout)
            .map_err(|error| daemon_host_network_error("cluster local member publish rpc", error))
    }

    fn send_cluster_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_network::NetworkError> {
        crate::send_daemon_rpc_to_endpoint(endpoint, request, timeout)
            .map_err(|error| daemon_host_network_error("cluster membership rpc", error))
    }

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, sinomo_network::NetworkError> {
        relay_rpc_to_node_via_peer(state, target_node_id, peer, request, timeout)
            .map_err(|error| daemon_host_network_error("cluster relay", error))
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
        crate::cluster::fan_out_committed_cluster_raft_command(paths, state, events);
    }

    fn on_raft_append_sent(&mut self) {
        if let Some(stats) = crate::process_idle_stats() {
            stats.incr_raft_appends_sent();
        }
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

pub(crate) fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::publish_cluster_node_status_to_quorum_with_host(paths, status, &mut host)
        .map_err(Into::into)
}

pub(crate) fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::publish_cluster_network_info_to_quorum_with_host(paths, info, &mut host)
        .map_err(Into::into)
}

pub(crate) fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<Option<KvSnapshot>, DaemonError> {
    let mut host = daemon_cluster_control_plane_driver();
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
    let route_ips = sinomo_network::discover_peer_route_ip_candidates(state);
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

fn local_daemon_membership_observation_from_daemon_paths(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
) -> Result<sinomo_network::ClusterLocalDaemonMembershipObservation, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let now = OffsetDateTime::now_utc();
    let (main_options, shadow_options) = if let Ok(main_status) = read_daemon_status(paths) {
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
        (daemon_options_from_status(&main_status), shadow_options)
    } else {
        (
            DaemonRunOptions::default(),
            DaemonRunOptions::shadow_default(),
        )
    };
    local_daemon_membership_observation(data_dir, state, &main_options, &shadow_options, now)
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

pub(crate) fn cluster_has_local_quorum(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> bool {
    let mut host = daemon_cluster_control_plane_driver();
    sinomo_network::cluster_has_local_quorum_with_host(
        paths,
        state,
        cluster_voter_reachability_options(),
        &mut host,
    )
}

fn fan_out_committed_cluster_raft_command(
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
) {
    sinomo_network::spawn_cluster_raft_committed_fanout_worker_with_host(
        paths,
        state,
        events,
        cluster_raft_committed_fanout_options(),
        daemon_cluster_control_plane_driver,
    );
}

pub(crate) fn commit_cluster_kv_txn_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<KvSnapshot, DaemonError> {
    commit_cluster_kv_txn_with_quorum_response(paths, state, txn).map(|(snapshot, _)| snapshot)
}

fn commit_cluster_kv_txn_with_quorum_response(
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
    let mut host = daemon_cluster_control_plane_driver();
    let result = sinomo_network::commit_cluster_kv_txn_with_quorum_response_with_host(
        paths,
        state,
        txn,
        cluster_kv_txn_quorum_options(allow_forwarding),
        &mut host,
    )?;
    Ok((result.snapshot, result.txn_response))
}

/// Per-hop timeout for a relayed cluster RPC. The first hop waits for the whole downstream
/// chain, so this is generous; the relay TTL bounds the number of hops.
const DAEMON_CLUSTER_RELAY_TIMEOUT: Duration = Duration::from_secs(15);
/// Handle a `CLUSTER_RELAY_ROUTE` request. Runs the pure routing decision, then either
/// delivers the wrapped request locally (when this node is the target) by re-injecting it
/// through the daemon's own control port, or forwards it one hop closer to the target over
/// the connectivity graph. The inner request's response flows back along the relay chain.
pub(crate) fn handle_daemon_cluster_relay_request(
    paths: &DaemonPaths,
    envelope: sinomo_network::RelayEnvelope,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let cluster_paths = cluster_paths_from_daemon_paths(paths)?;
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;
    let local_node_id = relay_local_node_id(paths, &state);
    let mut host = DaemonClusterRelayHost { paths };
    sinomo_network::handle_cluster_relay_with_host(
        &cluster_paths,
        &state,
        &local_node_id,
        envelope,
        sinomo_network::ClusterRelayOptions {
            timeout: DAEMON_CLUSTER_RELAY_TIMEOUT,
        },
        &mut host,
    )
    .map_err(Into::into)
}

struct DaemonClusterRelayHost<'a> {
    paths: &'a DaemonPaths,
}

impl sinomo_network::ClusterRelayHost for DaemonClusterRelayHost<'_> {
    fn try_deliver_local(
        &mut self,
        state: &ClusterBootstrapState,
        envelope: &sinomo_network::RelayEnvelope,
        timeout: Duration,
    ) -> Result<Option<sinomo_network::ObjectRpcResponse>, sinomo_network::NetworkError> {
        let Some(local_address) =
            relay_local_delivery_address(self.paths, state, &envelope.target_node_id).map_err(
                |error| daemon_host_network_error("cluster relay local delivery", error),
            )?
        else {
            return Ok(None);
        };
        // We are the target, or the target is our colocated main/shadow process:
        // re-inject the original signed request through that process' control port
        // so it passes the normal authentication + dispatch path.
        let inner = &envelope.inner;
        let inner_rpc = inner
            .rpc
            .as_ref()
            .ok_or_else(|| sinomo_network::NetworkError::Io {
                operation: "cluster relay local delivery".to_string(),
                detail: "relayed request carried no rpc payload".to_string(),
            })?;
        sinomo_network::send_daemon_rpc_raw(
            &local_address,
            &inner.command,
            inner.auth.as_deref(),
            inner_rpc,
            timeout,
        )
        .map(Some)
    }

    fn send_relay_request(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &sinomo_network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
        send_cluster_relay_request_to_next_hop(endpoint, request, timeout)
            .map_err(|error| daemon_host_network_error("cluster relay", error))
    }
}

fn send_cluster_relay_request_to_next_hop(
    endpoint: &ClusterEndpointRecord,
    request: &sinomo_network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let request_auth = crate::sign_daemon_rpc_request_for_local_node(request)?;
    let command = crate::daemon_rpc_command(sinomo_network::CLUSTER_RELAY_ROUTE);
    sinomo_network::send_daemon_rpc_to_cluster_endpoint(
        endpoint,
        &command,
        Some(&request_auth),
        request,
        timeout,
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

fn relay_local_delivery_address(
    paths: &DaemonPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> Result<Option<String>, DaemonError> {
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
    Ok(Some(daemon_connect_address("127.0.0.1", port)))
}

fn daemon_host_network_error(operation: &str, error: DaemonError) -> sinomo_network::NetworkError {
    sinomo_network::NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
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

        assert_eq!(
            relay_local_delivery_address(&main_paths, &state, &shadow_node_id)
                .expect("main to shadow"),
            Some("127.0.0.1:32154".to_string())
        );
        assert_eq!(
            relay_local_delivery_address(&shadow_paths, &state, &main_node_id)
                .expect("shadow to main"),
            Some("127.0.0.1:32153".to_string())
        );
        assert_eq!(
            relay_local_delivery_address(
                &main_paths,
                &state,
                &NodeId::from_legacy_named("node_relay_local_remote").expect("remote")
            )
            .expect("remote target"),
            None
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

        let mut host = DaemonClusterMemberMutationHost::new();
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

        let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
        let shadow_member = state
            .members
            .iter()
            .find(|member| member.node_id == shadow_node_id)
            .expect("shadow member");
        assert!(
            shadow_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32154"
                )
        );
        assert!(
            shadow_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21354")
        );

        std::fs::remove_dir_all(root).ok();
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

        let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
        let shadow_member = state
            .members
            .iter()
            .find(|member| member.node_id == shadow_node_id)
            .expect("shadow member");
        assert!(
            shadow_member
                .endpoints
                .iter()
                .any(
                    |endpoint| endpoint.transport == ClusterTransportKind::Loopback
                        && endpoint.address == "127.0.0.1:32154"
                )
        );
        assert!(
            shadow_member
                .endpoints
                .iter()
                .all(|endpoint| endpoint.address != "127.0.0.1:21354")
        );

        std::fs::remove_dir_all(root).ok();
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
        std::fs::remove_dir_all(root).ok();
    }

    struct TestClusterJoinRegistrationHost;

    impl sinomo_network::ClusterMembershipRpcForwardHost for TestClusterJoinRegistrationHost {
        fn send_membership_rpc(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &sinomo_network::ObjectRpcRequest,
        ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
            panic!("forwarded acceptance recorder should not send direct membership RPCs")
        }

        fn relay_membership_rpc_to_voter_via_peer(
            &mut self,
            _state: &ClusterBootstrapState,
            _voter: &ClusterEndpointRecord,
            _peer: &ClusterEndpointRecord,
            _request: &sinomo_network::ObjectRpcRequest,
        ) -> Result<sinomo_network::ObjectRpcResponse, sinomo_network::NetworkError> {
            panic!("forwarded acceptance recorder should not relay membership RPCs")
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
        sinomo_network::apply_cluster_raft_role(
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
        sinomo_network::apply_cluster_raft_role(
            &mut bridge_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_network::upsert_member_record(&mut accepted_state, bridge_member);
        let mut joined_member = ClusterMemberRecord::local_voter(
            accepted_state.cluster_id.clone(),
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        sinomo_network::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_network::upsert_member_record(&mut accepted_state, joined_member.clone());
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
        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_network::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_network::upsert_member_record(
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
                sinomo_network::cluster_identity_key(),
                &sinomo_network::ClusterIdentityRecord::from(&accepted_state),
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
        sinomo_network::apply_cluster_raft_role(
            &mut joined_member,
            ClusterMemberRole::Learner,
            now,
        );
        sinomo_network::upsert_member_record(&mut accepted_state, joined_member);
        sinomo_network::upsert_member_record(
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
    fn cluster_kv_watch_event_fanout_batches_are_bounded_and_ordered() {
        let events = (0..(sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS * 2 + 3))
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: index.to_string().into_bytes(),
                },
            })
            .collect::<Vec<_>>();

        let batches = sinomo_kv::cluster_kv_watch_event_fanout_batches(&events);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| {
            !batch.is_empty()
                && batch.len() <= sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS
                && sinomo_kv::cluster_kv_watch_events_payload_bytes(batch)
                    <= sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
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
        let value_len = sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES / 2;
        let events = (0..3)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index + 1),
                key: format!("/watch/fanout/payload/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: vec![index as u8; value_len],
                },
            })
            .collect::<Vec<_>>();

        let batches = sinomo_kv::cluster_kv_watch_event_fanout_batches(&events);
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert!(batches.iter().all(|batch| {
            sinomo_kv::cluster_kv_watch_events_payload_bytes(batch)
                <= sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES
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
    fn cluster_kv_watch_event_fanout_skips_storage_without_active_watchers() {
        let root = unique_cluster_test_root("sinomo-daemon-watch-fanout-no-active-watchers");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        sinomo_network::write_cluster_kv_snapshot(
            &cluster_paths,
            &InProcessKvStore::new().snapshot(),
        )
        .expect("write empty snapshot");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_no_active").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_no_active_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout no active",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write cluster state");
        let event = KvEvent {
            revision: sinomo_kv::KvRevision(1),
            key: "/watch/fanout/no-active".to_string(),
            kind: sinomo_kv::KvEventKind::Put {
                value: b"value".to_vec(),
            },
        };

        let response = sinomo_network::handle_cluster_kv_watch_events(
            &cluster_paths,
            ClusterKvWatchEventsRequest {
                state,
                leader_id,
                events: vec![event],
            },
        )
        .expect("watch event fanout response");

        assert!(response.accepted);
        assert_eq!(
            response.stored_events, 0,
            "fanout without active local watchers should be accepted but not persisted"
        );
        assert!(
            !cluster_paths.kv_watch_events.exists(),
            "no active watcher should avoid checkpoint writes"
        );
        assert!(
            !cluster_paths.kv_watch_events_log.exists(),
            "no active watcher should avoid log appends"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cluster_kv_watch_event_fanout_rejects_oversized_batches_before_storage() {
        let root = unique_cluster_test_root("sinomo-daemon-watch-fanout-oversized");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_oversized").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_oversized_leader").expect("leader");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout oversized",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let events = (0..=sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/oversized/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: index.to_string().into_bytes(),
                },
            })
            .collect::<Vec<_>>();

        let response = sinomo_network::handle_cluster_kv_watch_events(
            &cluster_paths,
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
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_watch_fanout_payload").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_watch_fanout_payload_leader").expect("leader");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "watch fanout payload",
            leader_id.clone(),
            "leader",
            "pk-leader",
            now,
        );
        let value_len = sinomo_kv::CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES / 2;
        let events = (0..2)
            .map(|index| KvEvent {
                revision: sinomo_kv::KvRevision(index as u64 + 1),
                key: format!("/watch/fanout/payload/{index}"),
                kind: sinomo_kv::KvEventKind::Put {
                    value: vec![index as u8; value_len],
                },
            })
            .collect::<Vec<_>>();

        let response = sinomo_network::handle_cluster_kv_watch_events(
            &cluster_paths,
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

        maintenance
            .tick(&daemon_paths, &DaemonRunOptions::default())
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
        let _ = std::fs::remove_dir_all(root);
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

        maintenance
            .tick(&daemon_paths, &DaemonRunOptions::default())
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
        std::fs::remove_dir_all(root).ok();
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
        std::fs::remove_dir_all(root).ok();
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

        std::fs::remove_dir_all(root).ok();
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

        let _ = std::fs::remove_dir_all(root);
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
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
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

        let _ = std::fs::remove_dir_all(root);
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

        maintenance
            .tick(&daemon_paths, &DaemonRunOptions::default())
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
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&cluster_paths, &state)
            .expect("sync cluster state");

        let authenticated = AuthenticatedDaemonCommand {
            command: "cluster-member-publish".to_string(),
            signer: crate::DaemonSignerAuthority::Local,
            signer_node_id: local.to_string(),
            signer_public_key: state.members[0].node_public_key.clone(),
        };
        let request = ObjectRpcRequest::encode_body(
            sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
            sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &ClusterMemberPublishRequest {
                members: vec![state.members[0].clone()],
            },
        )
        .expect("member publish rpc request");
        let mut member_host = daemon_cluster_control_plane_driver();
        let mut invite_host = daemon_cluster_control_plane_driver();
        let response = sinomo_network::handle_cluster_membership_rpc_request_with_hosts(
            &cluster_paths,
            request,
            &authenticated.signer_node_id,
            &authenticated.signer_public_key,
            now,
            &mut DaemonClusterJoinRegistrationHost::new(&daemon_paths),
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
            "stable member publish responses should not send unrelated full KV snapshots"
        );
        let _ = std::fs::remove_dir_all(root);
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
            let _raft_guard =
                sinomo_network::lock_cluster_raft_operation().expect("raft operation lock");
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
        let wal_path = sinomo_network::cluster_raft_wal_paths(&paths, &local).wal;
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
            sinomo_network::cluster_connectivity_stable_eq(&seeded_record, &refreshed_record),
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
        let _ = std::fs::remove_dir_all(root);
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
            sinomo_network::read_cluster_kv_current_snapshot(&paths).expect("read snapshot");
        let config_key = sinomo_network::cluster_raft_config_key();
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
        let wal_path = sinomo_network::cluster_raft_wal_paths(&paths, &local).wal;
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
        sinomo_network::write_cluster_kv_snapshot(&paths, &store.snapshot())
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
        let wal_path = sinomo_network::cluster_raft_wal_paths(&paths, &local_node).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("wal before stable sync")
            .len();

        sinomo_network::write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
            .expect("install stale materialized cache");
        let stale_materialized =
            sinomo_network::read_cluster_kv_snapshot(&paths).expect("stale materialized");
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
        let materialized =
            sinomo_network::read_cluster_kv_snapshot(&paths).expect("materialized after sync");
        assert!(
            materialized.get(&member_key).is_none(),
            "stable sync should not rewrite the intentionally stale materialized cache"
        );
        std::fs::remove_dir_all(root).ok();
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
            !sinomo_network::cluster_network_record_should_refresh(
                info.expires_at,
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
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
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
        std::fs::remove_dir_all(root).ok();
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
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let records = maintenance.probe_connectivity(&paths, &state);

        assert_eq!(records, vec![record]);
        assert_eq!(
            idle_stats.snapshot().connectivity_probes_sent,
            0,
            "fresh unreachable endpoints should not burn battery by probing every tick"
        );
        std::fs::remove_dir_all(root).ok();
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

        assert!(
            !sinomo_network::cluster_connectivity_probe_due_with_graph(
                &unreachable_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "an unreachable direct endpoint should not be reprobed at workstation cadence while a healthy alternate route exists"
        );
        assert!(
            !sinomo_network::cluster_connectivity_probe_due_with_graph(
                &degraded_direct,
                &target_endpoint,
                due_at,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                &graph,
                &local,
                false,
            ),
            "a degraded direct endpoint should not be reprobed at workstation cadence while a healthy alternate route exists"
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
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_network::stable_iroh_endpoint_id(
                &remote_node,
                "iroh_public_key_daemon_command_connectivity",
            ),
            node_id: remote_node.clone(),
            transport: ClusterTransportKind::Iroh,
            address:
                "iroh://iroh_public_key_daemon_command_connectivity?relay=https://relay.example./"
                    .to_string(),
            source: sinomo_core::ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };
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

        for expected_failures in 1..CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            remember_cluster_endpoint_command_connectivity(
                &paths,
                &state,
                &tcp,
                sinomo_network::ClusterConnectivityState::Degraded,
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

        let targets = sinomo_network::cluster_raft_command_targets(&paths, &state);
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
        );
        let record = read_tcp_record();
        assert_eq!(
            record.state,
            sinomo_network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.latency_ms, Some(12));
        assert!(record.last_failure_at.is_none());

        let targets = sinomo_network::cluster_raft_command_targets(&paths, &state);
        assert_eq!(
            targets.first().map(|endpoint| &endpoint.endpoint_id),
            Some(&tcp.endpoint_id),
            "recovered direct command endpoint should become the preferred path again"
        );

        std::fs::remove_dir_all(root).ok();
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
            last_publish: maintenance_now,
            last_discovery: maintenance_now,
            last_probe: maintenance_now,
            idle_stats: idle_stats.clone(),
        };

        let records = maintenance.probe_connectivity(&paths, &state);

        assert_eq!(
            idle_stats.snapshot().connectivity_probes_sent,
            sinomo_network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK as u64,
            "mobile nodes should not probe every due remote endpoint in one idle tick"
        );
        assert_eq!(
            records.len(),
            sinomo_network::CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK,
            "only the probed mobile batch should become publishable records"
        );
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
            connectivity_converged: true,
            environment_id: None,
            local_node_role: ClusterNodeRole::Workstation,
            last_desired_rollout_attempt: None,
            solo_cluster_idle: false,

            startup_settle_until: None,
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

        let targets = sinomo_network::cluster_member_publish_targets(&paths, &state);

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
        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::upsert_member_endpoint(local_bridge, bridge_learned_endpoint.clone());
        sinomo_network::write_cluster_state(&paths, &local_state)
            .expect("write local learned route");

        let merged_state = sinomo_network::read_cluster_state(&paths).expect("read merged state");
        let targets = sinomo_network::cluster_member_publish_targets(&paths, &merged_state);

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

        std::fs::remove_dir_all(root).ok();
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

        publish_local_members_via_voter(&paths, &state)
            .expect("stable local members should not be republished");
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
    fn relay_preference_skips_degraded_direct_endpoint_when_route_is_healthy() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_daemon_relay_prefers_route").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_daemon_relay_prefers_route_local").expect("local node");
        let bridge_node = NodeId::from_legacy_named("node_daemon_relay_prefers_route_bridge")
            .expect("bridge node");
        let voter_node =
            NodeId::from_legacy_named("node_daemon_relay_prefers_route_voter").expect("voter node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay prefers route",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );

        let bridge_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "10.0.0.4:21353".to_string(), 10);
        let voter_endpoint =
            sinomo_network::daemon_lan_endpoint(&voter_node, "10.0.1.1:21353".to_string(), 10);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            bridge_node.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = sinomo_node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_endpoint.clone()];
        state.members.push(bridge_member);
        let mut voter_member = ClusterMemberRecord::local_voter(
            cluster_id,
            voter_node.clone(),
            "voter",
            "pk-voter",
            now,
        );
        voter_member.capabilities = sinomo_node::main_daemon_capabilities(&voter_member.role);
        voter_member.allowed_capabilities = voter_member.capabilities.clone();
        voter_member.endpoints = vec![voter_endpoint.clone()];
        state.members.push(voter_member);

        let mut bridge_state = state.clone();
        bridge_state.local_node_id = bridge_node;
        let graph = sinomo_network::ClusterConnectivityGraph::from_records(
            local_node,
            [
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &voter_endpoint,
                    sinomo_network::ClusterConnectivityState::Degraded,
                    None,
                    1,
                    None,
                    Some("timeout".to_string()),
                    now,
                    60,
                ),
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &bridge_endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(2),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                sinomo_network::cluster_connectivity_record(
                    &bridge_state,
                    &voter_endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(2),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        );

        assert!(
            sinomo_network::cluster_endpoint_prefers_relay_with_graph(
                &graph,
                &state,
                &voter_endpoint
            ),
            "a degraded direct endpoint should relay immediately when a healthy bridge route exists"
        );
    }

    #[test]
    fn relay_preference_skips_unknown_direct_endpoint_when_route_is_healthy() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_daemon_relay_prefers_known_route")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_daemon_relay_prefers_known_route_local")
            .expect("local node");
        let bridge_node = NodeId::from_legacy_named("node_daemon_relay_prefers_known_route_bridge")
            .expect("bridge node");
        let target_node = NodeId::from_legacy_named("node_daemon_relay_prefers_known_route_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay prefers known route",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );

        let bridge_endpoint =
            sinomo_network::daemon_lan_endpoint(&bridge_node, "10.0.0.4:21353".to_string(), 10);
        let target_endpoint =
            sinomo_network::daemon_lan_endpoint(&target_node, "10.0.1.1:21353".to_string(), 10);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            bridge_node.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = sinomo_node::main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_endpoint.clone()];
        state.members.push(bridge_member);
        let mut target_member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            target_node,
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = sinomo_node::main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(target_member);

        let mut bridge_state = state.clone();
        bridge_state.local_node_id = bridge_node;
        let graph = sinomo_network::ClusterConnectivityGraph::from_records(
            local_node,
            [
                sinomo_network::cluster_connectivity_record(
                    &state,
                    &bridge_endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(2),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                sinomo_network::cluster_connectivity_record(
                    &bridge_state,
                    &target_endpoint,
                    sinomo_network::ClusterConnectivityState::Healthy,
                    Some(2),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        );

        assert!(
            sinomo_network::cluster_endpoint_prefers_relay_with_graph(
                &graph,
                &state,
                &target_endpoint
            ),
            "an unknown direct endpoint should relay immediately when a healthy bridge route exists"
        );
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
        let graph = sinomo_network::ClusterConnectivityGraph::from_records(local_node.clone(), []);

        let next_hops = sinomo_network::cluster_relay_next_hops_with_direct_target_fallback(
            &paths,
            &graph,
            &state,
            &local_node,
            &target_node,
            &[],
        );

        assert_eq!(
            next_hops,
            vec![target_node],
            "relay should deliver directly to a known command endpoint while connectivity publication is still empty"
        );
        assert_eq!(
            sinomo_network::cluster_relay_next_hop_endpoints(
                &paths,
                &state,
                &local_node,
                &next_hops[0],
            )
            .into_iter()
            .next()
            .map(|endpoint| endpoint.endpoint_id),
            Some(target_endpoint.endpoint_id)
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn relay_next_hop_endpoints_keep_locally_learned_hostname_fallback() {
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
        );

        let endpoints = sinomo_network::cluster_relay_next_hop_endpoints(
            &paths,
            &state,
            &local_node,
            &target_node,
        );

        assert_eq!(
            endpoints.first().map(|endpoint| &endpoint.address),
            Some(&stale_endpoint.address)
        );
        assert!(
            endpoints
                .iter()
                .any(|endpoint| endpoint.address == learned_endpoint.address),
            "relay should retain the locally learned hostname as a fallback for segmented startup"
        );
        std::fs::remove_dir_all(root).ok();
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
        sinomo_network::sync_cluster_node_status_to_kv(
            &paths,
            ClusterNodeStatusRecord {
                cluster_id,
                node_id: voter_node,
                process: "daemon-main".to_string(),
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
            sinomo_network::cluster_raft_live_voter_targets(&paths, &state).is_empty(),
            "ordinary live-voter routing should honor explicit offline status"
        );
        assert_eq!(
            sinomo_network::cluster_join_voter_targets(&paths, &state)
                .first()
                .map(|endpoint| &endpoint.endpoint_id),
            Some(&voter_endpoint.endpoint_id),
            "join bootstrap must still try voter command targets even when local health is stale/offline"
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
            sinomo_network::cluster_node_blocks_leader_preference(&paths, &state, &remote_node),
            "a remote member the active probes cannot reach must not be preferred as leader"
        );
        assert!(
            sinomo_network::read_cluster_node_statuses_from_kv(&paths)
                .expect("node statuses")
                .into_iter()
                .find(|status| status.node_id == remote_node)
                .is_some_and(|status| status.availability != ClusterNodeAvailability::Offline),
            "an unreachable member that never self-declared Offline is not explicit offline"
        );
        assert_eq!(
            sinomo_network::cluster_raft_live_voter_targets(&paths, &state).len(),
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
