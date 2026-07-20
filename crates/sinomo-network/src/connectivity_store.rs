use crate::connectivity::{
    ClusterConnectivityRecord, ClusterConnectivityState, MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_connectivity_record_for_command_auth_denied,
    cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome, cluster_connectivity_stable_eq,
    cluster_network_record_live, validate_cluster_connectivity_records_for_source,
};
use crate::error::NetworkError;
use crate::object_file_store::{
    read_optional_object_file, remove_optional_file, write_object_file,
};
use crate::{ClusterEndpointId, ClusterPaths};
use fs2::FileExt;
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES: usize = 128;

static LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATION_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static LOCAL_CLUSTER_CONNECTIVITY_RECORDS: LazyLock<
    Mutex<HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct LocalClusterConnectivityRecordCacheEntry {
    records: Vec<ClusterConnectivityRecord>,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

pub fn local_cluster_connectivity_content_generation(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS
        .lock()
        .ok()
        .and_then(|generations| generations.get(&paths.root).copied())
        .unwrap_or(0)
}

fn bump_local_cluster_connectivity_content_generation(paths: &ClusterPaths) {
    let next = LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATION_CLOCK.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut generations) = LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS.lock() {
        generations.insert(paths.root.clone(), next);
    }
}

/// Generation of changes that can alter route reachability or topology.
///
/// The full content generation also advances for observation-only refreshes
/// such as latency and timestamps. Delivery routing tables do not need to be
/// rebuilt for those refreshes: their endpoint candidates are unchanged and
/// live endpoint quality is applied again immediately before each attempt.
pub(crate) fn local_cluster_connectivity_routing_generation(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS
        .lock()
        .ok()
        .and_then(|generations| generations.get(&paths.root).copied())
        .unwrap_or(0)
}

fn bump_local_cluster_connectivity_routing_generation(paths: &ClusterPaths) {
    let next = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CLOCK.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut generations) = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS.lock() {
        generations.insert(paths.root.clone(), next);
    }
}

pub fn read_local_cluster_connectivity_records(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let now = OffsetDateTime::now_utc();
    if let Some(records) = read_local_cluster_connectivity_records_from_cache(paths, now)? {
        return Ok(records);
    }

    let Some(mut records) =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    else {
        return Ok(Vec::new());
    };
    records.retain(|record| cluster_network_record_live(record.expires_at, now));
    sort_cluster_connectivity_records(&mut records);
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if let Some(entry) = cache.get_mut(&paths.root) {
        prune_expired_local_cluster_connectivity_cache_entry(&paths.root, entry, now);
        if entry.records.is_empty() {
            cache.remove(&paths.root);
        } else {
            entry.last_used = local_cluster_connectivity_cache_tick();
            let mut cached_records = entry.records.clone();
            sort_cluster_connectivity_records(&mut cached_records);
            return Ok(cached_records);
        }
    }
    store_local_cluster_connectivity_cache_entry(
        &mut cache,
        paths.root.clone(),
        records.clone(),
        now,
    );
    Ok(records)
}

/// Discards this process's local connectivity snapshot so the next read
/// reloads the shared on-disk state.
///
/// Daemon-control clients can update the local connectivity file from another
/// process. Advancing both generations ensures cached route graphs are rebuilt
/// from the reloaded records as well.
pub fn invalidate_local_cluster_connectivity_cache(
    paths: &ClusterPaths,
) -> Result<(), NetworkError> {
    LOCAL_CLUSTER_CONNECTIVITY_RECORDS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity cache".to_string(),
            detail: error.to_string(),
        })?
        .remove(&paths.root);
    bump_local_cluster_connectivity_content_generation(paths);
    bump_local_cluster_connectivity_routing_generation(paths);
    Ok(())
}

pub fn read_local_cluster_connectivity_records_for_source(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let records = read_local_cluster_connectivity_records(paths)?;
    let mut filtered = Vec::new();
    for record in records
        .into_iter()
        .filter(|record| record.source_node_id == *source_node_id)
    {
        crate::object::try_reserve_vec(
            &mut filtered,
            1,
            "collect local connectivity records by source",
        )?;
        filtered.push(record);
    }
    Ok(filtered)
}

pub fn record_local_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    let previous = read_local_cluster_connectivity_records_for_source(paths, &state.local_node_id)?
        .into_iter()
        .find(|record| {
            record.target_node_id == endpoint.node_id && record.endpoint_id == endpoint.endpoint_id
        });
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let is_healthy = matches!(&connectivity_state, ClusterConnectivityState::Healthy);
    let observed_latency_ms = latency_ms.unwrap_or_else(|| {
        previous
            .as_ref()
            .and_then(|record| record.latency_ms)
            .unwrap_or(0)
    });
    let record = if matches!(&connectivity_state, ClusterConnectivityState::Unreachable) {
        cluster_connectivity_record_for_identity_mismatch(state, endpoint, &timestamp, now)
    } else if !is_healthy && !allow_failure_grace {
        cluster_connectivity_record_for_hard_failure(
            state,
            endpoint,
            previous.as_ref(),
            &timestamp,
            now,
        )
    } else {
        cluster_connectivity_record_for_probe_outcome(
            state,
            endpoint,
            previous.as_ref(),
            is_healthy,
            observed_latency_ms,
            &timestamp,
            now,
        )
    };
    write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )?;
    Ok(record)
}

pub fn record_local_cluster_endpoint_command_auth_denied(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let record =
        cluster_connectivity_record_for_command_auth_denied(state, endpoint, &timestamp, now);
    write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )?;
    Ok(record)
}

pub fn write_local_cluster_connectivity_records(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    write_local_cluster_connectivity_records_inner(paths, source_node_id, records, false)
}

pub async fn write_local_cluster_connectivity_records_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let source_node_id = source_node_id.clone();
    let records = records.to_vec();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_records(&paths, &source_node_id, &records)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

pub fn write_local_cluster_connectivity_record_set(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    write_local_cluster_connectivity_records_inner(paths, source_node_id, records, true)
}

pub async fn write_local_cluster_connectivity_record_set_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let source_node_id = source_node_id.clone();
    let records = records.to_vec();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_set(&paths, &source_node_id, &records)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

/// Merge record subsets owned by several source nodes in one process-safe
/// local-store transaction.
///
/// This is intentionally merge-only: imported peer snapshots do not carry a
/// source-set generation watermark, so an omitted key cannot authorize a
/// deletion. Expiry and topology pruning remove stale imported observations.
pub fn write_local_cluster_connectivity_record_groups(
    paths: &ClusterPaths,
    record_groups: &BTreeMap<NodeId, Vec<ClusterConnectivityRecord>>,
) -> Result<(), NetworkError> {
    if record_groups.is_empty() {
        return Ok(());
    }
    let mut observed_records = 0_usize;
    for (source_node_id, records) in record_groups {
        observed_records = observed_records.checked_add(records.len()).ok_or(
            NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records: usize::MAX,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            },
        )?;
        if observed_records > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
            return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            });
        }
        validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    }

    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    let existing_records = read_persisted_live_local_cluster_connectivity_records(paths, now)?;
    let mut merged = existing_records
        .iter()
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for records in record_groups.values() {
        for record in records {
            if !cluster_network_record_live(record.expires_at, now) {
                continue;
            }
            let key = cluster_connectivity_record_key(record);
            if merged
                .get(&key)
                .is_some_and(|existing| existing.observed_at > record.observed_at)
            {
                continue;
            }
            merged.insert(key, record.clone());
        }
    }
    if merged.len() > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: merged.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        });
    }
    for source_node_id in record_groups.keys() {
        let retained_source_records = merged
            .values()
            .filter(|record| record.source_node_id == *source_node_id)
            .count();
        if retained_source_records > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
            return Err(NetworkError::ClusterConnectivityRecordLimit {
                source_node_id: source_node_id.clone(),
                observed_records: retained_source_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
            });
        }
    }

    let mut merged_records = Vec::new();
    crate::object::try_reserve_vec(
        &mut merged_records,
        merged.len(),
        "collect aggregate merged connectivity records",
    )?;
    merged_records.extend(merged.into_values());
    sort_cluster_connectivity_records(&mut merged_records);

    let records_changed = existing_records != merged_records;
    let route_records_stable =
        local_cluster_connectivity_route_records_stable(&existing_records, &merged_records);
    let persisted_records_stable = route_records_stable
        && local_cluster_connectivity_persisted_records_stable(&existing_records, &merged_records);
    if !persisted_records_stable {
        if merged_records.is_empty() {
            remove_optional_file(&paths.connectivity_local)?;
        } else {
            write_object_file(&paths.connectivity_local, &merged_records)?;
        }
    }
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if merged_records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(
            &mut cache,
            paths.root.clone(),
            merged_records,
            now,
        );
    }
    if records_changed {
        bump_local_cluster_connectivity_content_generation(paths);
    }
    if !route_records_stable {
        bump_local_cluster_connectivity_routing_generation(paths);
    }
    Ok(())
}

pub async fn write_local_cluster_connectivity_record_groups_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    record_groups: &BTreeMap<NodeId, Vec<ClusterConnectivityRecord>>,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let record_groups = record_groups.clone();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_groups(&paths, &record_groups)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write aggregate local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

fn local_cluster_connectivity_mutation_lock_path(paths: &ClusterPaths) -> PathBuf {
    let parent = paths
        .connectivity_local
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let file_name = paths
        .connectivity_local
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("connectivity-local.sobj");
    parent.join(format!("{file_name}.lock"))
}

/// Serializes the complete on-disk connectivity read/modify/write transaction.
///
/// The main daemon, shadow daemon, and command clients can share the cluster
/// directory. Atomic file replacement prevents torn reads, but without this
/// advisory lock two writers updating different source-node sets can both read
/// the same snapshot and let the last rename silently discard the other source.
fn acquire_local_cluster_connectivity_mutation_lock(
    paths: &ClusterPaths,
) -> Result<std::fs::File, NetworkError> {
    let lock_path = local_cluster_connectivity_mutation_lock_path(paths);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let lock = options
        .open(&lock_path)
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    lock.lock_exclusive()
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(lock)
}

fn read_persisted_live_local_cluster_connectivity_records(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let mut records =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
            .unwrap_or_default();
    records.retain(|record| cluster_network_record_live(record.expires_at, now));
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}

pub fn prune_local_cluster_connectivity_records_for_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<usize, NetworkError> {
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    let Some(mut existing_records) =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    else {
        if let Ok(mut cache) = LOCAL_CLUSTER_CONNECTIVITY_RECORDS.lock() {
            cache.remove(&paths.root);
        }
        return Ok(0);
    };
    sort_cluster_connectivity_records(&mut existing_records);

    let topology = LocalConnectivityTopology::from_state(state);
    let mut retained = existing_records
        .iter()
        .filter(|record| topology.record_is_usable(record, now))
        .cloned()
        .collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut retained);

    let removed = existing_records.len().saturating_sub(retained.len());
    if existing_records == retained {
        let cache_changed = replace_local_cluster_connectivity_cache_records(paths, retained, now)?;
        if cache_changed {
            bump_local_cluster_connectivity_content_generation(paths);
            // A prune pass repairs an in-process cache that disagrees with the
            // authoritative on-disk record set. Conservatively invalidate the
            // routing table too; this path is maintenance, not the per-RPC
            // observation hot path.
            bump_local_cluster_connectivity_routing_generation(paths);
        }
        return Ok(0);
    }

    if retained.is_empty() {
        remove_optional_file(&paths.connectivity_local)?;
    } else {
        write_object_file(&paths.connectivity_local, &retained)?;
    }

    let _cache_changed = replace_local_cluster_connectivity_cache_records(paths, retained, now)?;
    bump_local_cluster_connectivity_content_generation(paths);
    bump_local_cluster_connectivity_routing_generation(paths);
    Ok(removed)
}

pub async fn prune_local_cluster_connectivity_records_for_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<usize, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || prune_local_cluster_connectivity_records_for_state(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "prune local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity prune task failed: {source}"),
        })?
}

#[derive(Default)]
struct LocalConnectivityTopology {
    cluster_id: Option<sinomo_node::ClusterId>,
    authorized_nodes: BTreeSet<NodeId>,
    endpoints: BTreeMap<ClusterEndpointId, ClusterEndpointRecord>,
}

impl LocalConnectivityTopology {
    fn from_state(state: &ClusterBootstrapState) -> Self {
        let mut topology = Self {
            cluster_id: Some(state.cluster_id.clone()),
            authorized_nodes: BTreeSet::new(),
            endpoints: BTreeMap::new(),
        };
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            topology.authorized_nodes.insert(member.node_id.clone());
            for endpoint in &member.endpoints {
                topology
                    .endpoints
                    .entry(endpoint.endpoint_id.clone())
                    .or_insert_with(|| endpoint.clone());
            }
        }
        topology
    }

    fn record_is_usable(&self, record: &ClusterConnectivityRecord, now: OffsetDateTime) -> bool {
        if self
            .cluster_id
            .as_ref()
            .is_some_and(|cluster_id| record.cluster_id != *cluster_id)
            || !cluster_network_record_live(record.expires_at, now)
            || !self.authorized_nodes.contains(&record.source_node_id)
            || !self.authorized_nodes.contains(&record.target_node_id)
        {
            return false;
        }
        if local_connectivity_record_is_synthetic_tcp_tunnel(record) {
            return true;
        }
        self.endpoints
            .get(&record.endpoint_id)
            .is_some_and(|endpoint| {
                endpoint.node_id == record.target_node_id
                    && endpoint.transport == record.transport
                    && endpoint.address == record.address
            })
    }
}

fn local_connectivity_record_is_synthetic_tcp_tunnel(record: &ClusterConnectivityRecord) -> bool {
    record.transport == ClusterTransportKind::LanTcp
        && record
            .address
            .strip_prefix(crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
            == Some(record.target_node_id.as_str())
}

fn local_cluster_connectivity_cache_tick() -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn read_local_cluster_connectivity_records_from_cache(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<Option<Vec<ClusterConnectivityRecord>>, NetworkError> {
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    let Some(entry) = cache.get_mut(&paths.root) else {
        return Ok(None);
    };
    prune_expired_local_cluster_connectivity_cache_entry(&paths.root, entry, now);
    if entry.records.is_empty() {
        cache.remove(&paths.root);
        return Ok(Some(Vec::new()));
    }
    entry.last_used = local_cluster_connectivity_cache_tick();
    let mut records = entry.records.clone();
    sort_cluster_connectivity_records(&mut records);
    Ok(Some(records))
}

fn earliest_cluster_connectivity_expiry(
    records: &[ClusterConnectivityRecord],
) -> Option<OffsetDateTime> {
    records.iter().map(|record| record.expires_at).min()
}

#[cfg(any(test, feature = "test-support"))]
fn record_local_cluster_connectivity_prune_pass_for_test(path: &PathBuf) {
    if let Ok(mut passes) = LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST.lock() {
        *passes.entry(path.clone()).or_default() += 1;
    }
}

fn prune_expired_local_cluster_connectivity_cache_entry(
    _path: &PathBuf,
    entry: &mut LocalClusterConnectivityRecordCacheEntry,
    now: OffsetDateTime,
) {
    if !entry
        .earliest_expires_at
        .is_some_and(|expires_at| expires_at <= now)
    {
        return;
    }
    #[cfg(any(test, feature = "test-support"))]
    record_local_cluster_connectivity_prune_pass_for_test(_path);
    entry
        .records
        .retain(|record| cluster_network_record_live(record.expires_at, now));
    entry.earliest_expires_at = earliest_cluster_connectivity_expiry(&entry.records);
}

fn prune_expired_local_cluster_connectivity_cache_entries(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    now: OffsetDateTime,
) {
    cache.retain(|path, entry| {
        prune_expired_local_cluster_connectivity_cache_entry(path, entry, now);
        !entry.records.is_empty()
    });
}

fn evict_local_cluster_connectivity_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

fn store_local_cluster_connectivity_cache_entry(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    key: PathBuf,
    records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) {
    prune_expired_local_cluster_connectivity_cache_entries(cache, now);
    evict_local_cluster_connectivity_cache_entry_if_needed(cache, &key);
    cache.insert(
        key,
        LocalClusterConnectivityRecordCacheEntry {
            earliest_expires_at: earliest_cluster_connectivity_expiry(&records),
            records,
            last_used: local_cluster_connectivity_cache_tick(),
        },
    );
}

fn replace_local_cluster_connectivity_cache_records(
    paths: &ClusterPaths,
    mut records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) -> Result<bool, NetworkError> {
    sort_cluster_connectivity_records(&mut records);
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    let changed = match cache.get(&paths.root) {
        Some(entry) => {
            let mut cached_records = entry.records.clone();
            sort_cluster_connectivity_records(&mut cached_records);
            cached_records != records
        }
        None => !records.is_empty(),
    };
    if records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(&mut cache, paths.root.clone(), records, now);
    }
    Ok(changed)
}

fn local_cluster_connectivity_persisted_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(|(existing, merged)| cluster_connectivity_stable_eq(existing, merged))
}

fn local_cluster_connectivity_raw_persisted_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    replace_source_records: bool,
) -> bool {
    let mut candidate = existing_records
        .iter()
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    if replace_source_records {
        candidate.retain(|(_, candidate_source_node_id, _), _| {
            candidate_source_node_id != source_node_id
        });
    }
    for record in records {
        candidate.insert(cluster_connectivity_record_key(record), record.clone());
    }
    let mut candidate = candidate.into_values().collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut candidate);
    local_cluster_connectivity_persisted_records_stable(existing_records, &candidate)
}

fn local_cluster_connectivity_route_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(local_cluster_connectivity_routing_record_stable_eq)
}

fn local_cluster_connectivity_routing_record_stable_eq(
    existing: (&ClusterConnectivityRecord, &ClusterConnectivityRecord),
) -> bool {
    let (existing, merged) = existing;
    cluster_connectivity_stable_eq(existing, merged)
        && local_cluster_connectivity_routing_latency_class(existing)
            == local_cluster_connectivity_routing_latency_class(merged)
        && existing.consecutive_failures == merged.consecutive_failures
        && merged.expires_at >= existing.expires_at
}

/// Coarse latency classes keep normal probe jitter from rebuilding every
/// target's routing table while still invalidating routes after a material
/// link-quality change. Exact endpoint quality is re-read before dialing; this
/// class only governs the cached ordering of alternate relay paths.
fn local_cluster_connectivity_routing_latency_class(record: &ClusterConnectivityRecord) -> u8 {
    if !matches!(record.state, ClusterConnectivityState::Healthy) {
        return 0;
    }
    match record.latency_ms {
        None => 0,
        // Ordinary LAN/container jitter must remain in one class: a boundary
        // in the tens of milliseconds lets concurrent observations alternate
        // the generation and recreate the original cache-thrashing bug.
        Some(0..=128) => 1,
        Some(129..=512) => 2,
        Some(513..=2_000) => 3,
        Some(_) => 4,
    }
}

fn cluster_connectivity_record_key(
    record: &ClusterConnectivityRecord,
) -> (NodeId, NodeId, ClusterEndpointId) {
    (
        record.source_node_id.clone(),
        record.target_node_id.clone(),
        record.endpoint_id.clone(),
    )
}

fn write_local_cluster_connectivity_records_inner(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    replace_source_records: bool,
) -> Result<(), NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    // Always reload the shared file after acquiring the process-safe lock. The
    // in-process route cache is intentionally allowed to lag an external
    // daemon-control writer and therefore cannot be the base of a mutation.
    let existing_records = read_persisted_live_local_cluster_connectivity_records(paths, now)?;
    let replacement_keys = if replace_source_records {
        records
            .iter()
            .filter(|record| {
                record.source_node_id == *source_node_id
                    && cluster_network_record_live(record.expires_at, now)
            })
            .map(cluster_connectivity_record_key)
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let replacement_observed_at = replace_source_records
        .then(|| {
            records
                .iter()
                .filter(|record| {
                    record.source_node_id == *source_node_id
                        && cluster_network_record_live(record.expires_at, now)
                })
                .map(|record| record.observed_at)
                .max()
        })
        .flatten();
    let mut merged = existing_records
        .iter()
        .filter(|record| {
            cluster_network_record_live(record.expires_at, now)
                && (!replace_source_records
                    || record.source_node_id != *source_node_id
                    || replacement_keys.contains(&cluster_connectivity_record_key(record))
                    // A complete-set producer can finish after a newer producer
                    // whose snapshot contains an additional endpoint. Preserve
                    // omitted observations newer than the incoming set's
                    // generation; a genuinely newer set still removes older
                    // omitted keys. An explicit empty set remains an explicit
                    // clear because it has no generation.
                    || replacement_observed_at
                        .is_some_and(|observed_at| record.observed_at > observed_at))
        })
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for record in records {
        if record.source_node_id == *source_node_id
            && cluster_network_record_live(record.expires_at, now)
        {
            let key = cluster_connectivity_record_key(record);
            if merged
                .get(&key)
                .is_some_and(|existing| existing.observed_at > record.observed_at)
            {
                // Observation tasks and full-set maintenance writes can finish
                // out of order. Never let a late older result regress a newer
                // reachability decision and make cluster KV oscillate.
                continue;
            }
            merged.insert(key, record.clone());
        }
    }
    let retained_source_records = merged
        .values()
        .filter(|record| record.source_node_id == *source_node_id)
        .count();
    if retained_source_records > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
        return Err(NetworkError::ClusterConnectivityRecordLimit {
            source_node_id: source_node_id.clone(),
            observed_records: retained_source_records,
            max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
        });
    }
    let mut merged_records = Vec::new();
    crate::object::try_reserve_vec(
        &mut merged_records,
        merged.len(),
        "collect merged connectivity records",
    )?;
    merged_records.extend(merged.into_values());
    let mut merged = merged_records;
    sort_cluster_connectivity_records(&mut merged);
    let records_changed = existing_records != merged;
    let route_records_stable =
        local_cluster_connectivity_route_records_stable(&existing_records, &merged);
    let mut persisted_records_stable = route_records_stable
        && local_cluster_connectivity_persisted_records_stable(&existing_records, &merged);
    let explicit_source_clear = replace_source_records && records.is_empty();
    if !persisted_records_stable
        && !explicit_source_clear
        && let Some(mut raw_existing_records) =
            read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    {
        sort_cluster_connectivity_records(&mut raw_existing_records);
        persisted_records_stable = local_cluster_connectivity_raw_persisted_records_stable(
            &raw_existing_records,
            source_node_id,
            records,
            replace_source_records,
        );
    }
    if !persisted_records_stable {
        if merged.is_empty() {
            remove_optional_file(&paths.connectivity_local)?;
        } else {
            write_object_file(&paths.connectivity_local, &merged)?;
        }
    }
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if merged.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(&mut cache, paths.root.clone(), merged, now);
    }
    if records_changed {
        bump_local_cluster_connectivity_content_generation(paths);
    }
    if !route_records_stable {
        bump_local_cluster_connectivity_routing_generation(paths);
    }
    Ok(())
}

pub(crate) fn sort_cluster_connectivity_records(records: &mut [ClusterConnectivityRecord]) {
    records.sort_by(|left, right| {
        (
            left.source_node_id.as_str(),
            left.target_node_id.as_str(),
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.source_node_id.as_str(),
                right.target_node_id.as_str(),
                right.endpoint_id.as_str(),
            ))
    });
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_record_cache_contains_for_test(paths: &ClusterPaths) -> bool {
    LOCAL_CLUSTER_CONNECTIVITY_RECORDS
        .lock()
        .map(|cache| cache.contains_key(&paths.root))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) {
    if let Ok(mut passes) = LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST.lock() {
        passes.remove(&paths.root);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST
        .lock()
        .ok()
        .and_then(|passes| passes.get(&paths.root).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_record_cache_capacity_for_test() -> usize {
    MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_local_cluster_connectivity_record_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = LOCAL_CLUSTER_CONNECTIVITY_RECORDS.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::{CLUSTER_CONNECTIVITY_TTL_SECONDS, cluster_connectivity_record};
    use sinomo_base::{
        ClusterEndpointPrivacy, ClusterEndpointSource, ClusterMemberRecord, ClusterTransportKind,
    };
    use sinomo_local::SinomoDataDir;
    use sinomo_node::ClusterId;

    fn connectivity_generation_fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        ClusterBootstrapState,
        NodeId,
        ClusterEndpointRecord,
        OffsetDateTime,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named(format!("cluster_{name}"))
            .expect("generation fixture cluster");
        let local = NodeId::from_legacy_named(format!("node_{name}_local"))
            .expect("generation fixture local node");
        let remote = NodeId::from_legacy_named(format!("node_{name}_remote"))
            .expect("generation fixture remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity generation fixture",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote, "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        (root, paths, state, local, endpoint, now)
    }

    #[test]
    fn observational_refresh_preserves_connectivity_routing_generation() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_observation_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial connectivity observation");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let refreshed_at = now + time::Duration::seconds(1);
        let refreshed = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("refreshed success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[refreshed])
            .expect("refresh connectivity observation");

        assert_ne!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "the route graph must still see a newer local observation"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "latency and timestamp refreshes must not invalidate the delivery routing table"
        );
    }

    #[test]
    fn explicit_invalidation_reloads_cross_process_connectivity_write() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_cross_process_reload");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write and cache initial connectivity");

        let changed_at = now + time::Duration::seconds(1);
        let externally_written = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            2,
            None,
            Some("external failure".to_string()),
            changed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_object_file(&paths.connectivity_local, &vec![externally_written])
            .expect("simulate another process replacing connectivity file");
        assert_eq!(
            read_local_cluster_connectivity_records(&paths).expect("read cached connectivity")[0]
                .state,
            ClusterConnectivityState::Healthy,
            "the test must prewarm the stale in-process cache"
        );

        invalidate_local_cluster_connectivity_cache(&paths).expect("invalidate connectivity");

        assert_eq!(
            read_local_cluster_connectivity_records(&paths).expect("reload connectivity from disk")
                [0]
            .state,
            ClusterConnectivityState::Degraded
        );
    }

    #[test]
    fn serialized_cross_source_mutation_preserves_external_source_set() {
        let (_root, paths, state, source_a, endpoint, now) =
            connectivity_generation_fixture("connectivity_cross_source_serialization");
        let record_a = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("source a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &source_a,
            std::slice::from_ref(&record_a),
        )
        .expect("seed and cache source a");

        let source_b = NodeId::from_legacy_named("node_connectivity_cross_source_serialization_b")
            .expect("source b");
        let mut record_b = record_a.clone();
        record_b.source_node_id = source_b.clone();
        record_b.last_success_at = Some("source b".to_string());

        let source_c = NodeId::from_legacy_named("node_connectivity_cross_source_serialization_c")
            .expect("source c");
        let mut record_c = record_a.clone();
        record_c.source_node_id = source_c;
        record_c.last_success_at = Some("external source c".to_string());
        let mut expected = vec![record_a.clone(), record_b.clone(), record_c.clone()];
        sort_cluster_connectivity_records(&mut expected);

        // Model another process updating a different source while this
        // process still has only source A cached. A concurrent source-B
        // writer must wait for the transaction lock, then reload A+C from
        // disk instead of building its replacement from the stale cache.
        let mutation_lock =
            acquire_local_cluster_connectivity_mutation_lock(&paths).expect("mutation lock");
        write_object_file(
            &paths.connectivity_local,
            &vec![record_a.clone(), record_c.clone()],
        )
        .expect("external source c write");

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer_paths = paths.clone();
        let writer = std::thread::spawn(move || {
            started_tx.send(()).expect("announce writer");
            let result = write_local_cluster_connectivity_records(
                &writer_paths,
                &source_b,
                std::slice::from_ref(&record_b),
            );
            done_tx.send(result).expect("report writer");
        });
        started_rx.recv().expect("writer started");
        assert!(
            matches!(
                done_rx.recv_timeout(std::time::Duration::from_millis(50)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "a second connectivity mutation must wait for the process-safe transaction lock"
        );
        drop(mutation_lock);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer should finish after lock release")
            .expect("merge source b");
        writer.join().expect("writer thread");

        invalidate_local_cluster_connectivity_cache(&paths).expect("reload persisted records");
        let stored =
            read_local_cluster_connectivity_records(&paths).expect("read serialized source sets");
        assert_eq!(stored, expected);
    }

    #[test]
    fn aggregate_group_merge_rejects_merged_total_without_partial_persistence() {
        let (_root, paths, state, source_a, endpoint, now) =
            connectivity_generation_fixture("connectivity_aggregate_merged_limit");
        let template = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("source a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut existing = Vec::with_capacity(MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1);
        for index in 0..MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1 {
            let mut record = template.clone();
            record.endpoint_id = ClusterEndpointId::derived(
                "sinomo.test.connectivity.aggregate-limit.existing",
                &index.to_le_bytes(),
            );
            record.address = format!("target:{index}");
            existing.push(record);
        }
        write_local_cluster_connectivity_record_set(&paths, &source_a, &existing)
            .expect("seed aggregate limit minus one");

        let source_b = NodeId::from_legacy_named("node_connectivity_aggregate_merged_limit_b")
            .expect("source b");
        let mut incoming = Vec::new();
        for index in 0_u64..2 {
            let mut record = template.clone();
            record.source_node_id = source_b.clone();
            record.endpoint_id = ClusterEndpointId::derived(
                "sinomo.test.connectivity.aggregate-limit.incoming",
                &index.to_le_bytes(),
            );
            record.address = format!("incoming:{index}");
            incoming.push(record);
        }
        let groups = BTreeMap::from([(source_b.clone(), incoming)]);

        let error = write_local_cluster_connectivity_record_groups(&paths, &groups)
            .expect_err("merged aggregate must remain bounded");
        assert!(matches!(
            error,
            NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            } if observed_records == MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS + 1
        ));
        let stored = read_local_cluster_connectivity_records(&paths)
            .expect("read records after rejected aggregate merge");
        assert_eq!(stored.len(), MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1);
        assert!(
            stored
                .iter()
                .all(|record| record.source_node_id == source_a),
            "aggregate rejection must happen before persisting any incoming source"
        );
    }

    #[test]
    fn route_semantic_change_advances_connectivity_routing_generation() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_routing_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial healthy connectivity");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let failed_at = now + time::Duration::seconds(1);
        let degraded = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("connection failed".to_string()),
            failed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[degraded])
            .expect("write degraded connectivity");

        assert_ne!(
            local_cluster_connectivity_content_generation(&paths),
            content_before
        );
        assert_ne!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "reachability-state changes must invalidate the delivery routing table"
        );
    }

    #[test]
    fn material_latency_change_advances_connectivity_routing_generation() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_latency_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial connectivity observation");
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let refreshed_at = now + time::Duration::seconds(1);
        let materially_slower = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(750),
            0,
            Some("materially slower success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[materially_slower])
            .expect("write materially slower connectivity observation");

        assert_ne!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "material link-quality changes must re-rank cached relay paths"
        );
    }

    #[test]
    fn routing_stability_detects_failure_count_and_shorter_expiry() {
        let (_root, _paths, state, _local, endpoint, now) =
            connectivity_generation_fixture("connectivity_routing_stability");
        let degraded = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            Some(20),
            1,
            Some("earlier success".to_string()),
            Some("first failure".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        let mut failed_again = degraded.clone();
        failed_again.consecutive_failures = 2;
        assert!(
            !local_cluster_connectivity_routing_record_stable_eq((&degraded, &failed_again)),
            "failure-cost changes must invalidate cached route ordering"
        );

        let mut shorter_lived = degraded.clone();
        shorter_lived.expires_at = degraded.expires_at - time::Duration::seconds(1);
        assert!(
            !local_cluster_connectivity_routing_record_stable_eq((&degraded, &shorter_lived)),
            "an earlier expiry must not leave a route cached past its lifetime"
        );
    }

    #[test]
    fn older_connectivity_observation_cannot_regress_newer_state() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_observation_order");
        let newer = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, std::slice::from_ref(&newer))
            .expect("write newer connectivity observation");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let older_at = now - time::Duration::seconds(1);
        let older = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("older failure".to_string()),
            older_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(&paths, &local, &[older])
            .expect("late full-set write must preserve newer observation");

        let stored = read_local_cluster_connectivity_records(&paths)
            .expect("read connectivity after stale write");
        assert_eq!(stored, vec![newer]);
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "discarding a stale observation must not churn route-graph content"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "discarding a stale observation must not invalidate delivery routes"
        );
    }

    #[test]
    fn stale_full_set_omission_cannot_delete_newer_endpoint() {
        let (_root, paths, state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_full_set_omission_order");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.full-set.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        let newer_at = now + time::Duration::seconds(2);
        let newer_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer a".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let newer_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("newer b".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[newer_a.clone(), newer_b.clone()],
        )
        .expect("seed newer complete source set");

        let stale_at = now + time::Duration::seconds(1);
        let stale_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Degraded,
            None,
            2,
            None,
            Some("stale a".to_string()),
            stale_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(&paths, &local, &[stale_a])
            .expect("merge stale complete source set");

        let mut expected = vec![newer_a, newer_b];
        sort_cluster_connectivity_records(&mut expected);
        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read source after stale full-set write"),
            expected
        );
    }

    #[test]
    fn newer_full_set_still_removes_older_omitted_endpoint() {
        let (_root, paths, state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_full_set_removal");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.full-set-removal.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        let initial_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("initial a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let initial_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("initial b".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(&paths, &local, &[initial_a, initial_b])
            .expect("seed initial complete source set");

        let replacement_at = now + time::Duration::seconds(1);
        let replacement_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("replacement a".to_string()),
            None,
            replacement_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&replacement_a),
        )
        .expect("write newer complete source set");

        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read source after newer removal"),
            vec![replacement_a]
        );
    }

    #[test]
    fn merge_only_late_peer_subset_preserves_newer_source_set_without_generation_churn() {
        let (_root, paths, state, source, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_peer_subset_order");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.peer-subset.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();

        let newer_at = now + time::Duration::seconds(1);
        let newer_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer a".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let newer_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("newer b".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &source,
            &[newer_a.clone(), newer_b.clone()],
        )
        .expect("seed newer peer source set");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let stale_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Unreachable,
            None,
            2,
            None,
            Some("stale a".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &source, &[stale_a])
            .expect("merge late peer subset");

        let stored = read_local_cluster_connectivity_records_for_source(&paths, &source)
            .expect("read merged peer source");
        let mut expected = vec![newer_a, newer_b];
        sort_cluster_connectivity_records(&mut expected);
        assert_eq!(stored, expected);
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "discarding a stale subset must not churn route-graph content"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "discarding a stale subset must not invalidate delivery routes"
        );
    }

    #[test]
    fn topology_prune_removes_removed_members_and_unadvertised_endpoints() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-connectivity-topology-prune");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_topology_prune").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_topology_prune_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_connectivity_topology_prune_remote").expect("remote");
        let removed =
            NodeId::from_legacy_named("node_connectivity_topology_prune_removed").expect("removed");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity topology prune",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote.clone(), "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let current = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let dirty_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.connectivity.prune.dirty",
                b"dirty",
            ),
            node_id: remote.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "stale.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let dirty = cluster_connectivity_record(
            &state,
            &dirty_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("dirty".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let removed_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &removed,
            "removed:21353".to_string(),
            10,
        );
        let removed_record = cluster_connectivity_record(
            &state,
            &removed_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some("removed".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let tunnel = ClusterConnectivityRecord {
            cluster_id: state.cluster_id.clone(),
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.connectivity.prune.tunnel",
                b"tunnel",
            ),
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}{}",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX,
                remote.as_str()
            ),
            state: ClusterConnectivityState::Healthy,
            latency_ms: Some(0),
            consecutive_failures: 0,
            last_success_at: Some("tunnel".to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(CLUSTER_CONNECTIVITY_TTL_SECONDS),
        };

        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[current.clone(), dirty, removed_record, tunnel.clone()],
        )
        .expect("seed local connectivity");

        let removed_count =
            prune_local_cluster_connectivity_records_for_state(&paths, &state).expect("prune");

        assert_eq!(removed_count, 2);
        let records =
            read_local_cluster_connectivity_records(&paths).expect("read pruned connectivity");
        assert_eq!(records.len(), 2);
        assert!(records.contains(&current));
        assert!(records.contains(&tunnel));
    }

    #[test]
    fn topology_prune_refreshes_stale_in_process_cache_when_disk_is_clean() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-connectivity-prune-cache");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_prune_cache").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_prune_cache_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_connectivity_prune_cache_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity prune cache",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote.clone(), "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let current = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let dirty_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.connectivity.prune.cache-dirty",
                b"dirty",
            ),
            node_id: remote,
            transport: ClusterTransportKind::LanTcp,
            address: "stale.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let dirty = cluster_connectivity_record(
            &state,
            &dirty_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("dirty".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[current.clone(), dirty.clone()],
        )
        .expect("seed dirty cache");
        assert!(
            read_local_cluster_connectivity_records(&paths)
                .expect("read dirty cache")
                .contains(&dirty)
        );
        write_object_file(&paths.connectivity_local, &vec![current.clone()])
            .expect("make disk clean without touching cache");

        let removed_count =
            prune_local_cluster_connectivity_records_for_state(&paths, &state).expect("prune");

        assert_eq!(removed_count, 0);
        let records =
            read_local_cluster_connectivity_records(&paths).expect("read pruned connectivity");
        assert_eq!(records, vec![current]);
    }
}
