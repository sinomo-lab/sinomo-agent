use crate::connectivity::{
    ClusterConnectivityRecord, ClusterConnectivityState, MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
    cluster_connectivity_record_for_command_auth_denied,
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
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES: usize = 128;

static LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATION_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
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

pub fn prune_local_cluster_connectivity_records_for_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<usize, NetworkError> {
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
    local_cluster_connectivity_route_records_stable(existing_records, merged_records)
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
    local_cluster_connectivity_route_records_stable(existing_records, &candidate)
}

fn local_cluster_connectivity_route_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(|(existing, merged)| cluster_connectivity_stable_eq(existing, merged))
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
    let now = OffsetDateTime::now_utc();
    let existing_records = read_local_cluster_connectivity_records(paths)?;
    let mut merged = existing_records
        .iter()
        .filter(|record| {
            cluster_network_record_live(record.expires_at, now)
                && (!replace_source_records || record.source_node_id != *source_node_id)
        })
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for record in records {
        if record.source_node_id == *source_node_id
            && cluster_network_record_live(record.expires_at, now)
        {
            merged.insert(cluster_connectivity_record_key(record), record.clone());
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
