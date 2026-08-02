use crate::connectivity_publish::cluster_connectivity_publish_record_matches;
use crate::keyspace::cluster_connectivity_key;
use crate::kv_snapshot_observer::{
    notify_cluster_kv_committed_delta_observers, notify_cluster_kv_committed_snapshot_observers,
};
use crate::local_state_store::read_cluster_state_without_kv;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::current_io_observer as io_observer;
use sinomo_network::{ClusterConnectivityRecord, cluster_network_record_live};
use sinomo_network::{ClusterFileMark, cluster_file_mark};

use sinomo_base::ClusterBootstrapState;
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvWalPaths};
use sinomo_kv::{
    ClusterKvCommittedDelta, ClusterKvCommittedKeyState, ClusterKvRaftCommitResult,
    ClusterRaftAppendRequest, DurableRaftKvNode, InProcessKvStore, KvCommand, KvCommandRequest,
    KvCommittedRequestRecord, KvEntry, KvEvent, KvLocalCache, KvLogEntry, KvOp, KvRevision,
    KvSnapshot, KvTxnResponse, RaftAppendEntriesRequest, RaftAppendEntriesResponse,
    RaftRequestVoteRequest, RaftRequestVoteResponse, RaftSnapshot,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(any(test, feature = "test-support"))]
use std::time::SystemTime;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const FILE_CACHE_CAPACITY: usize = 4096;
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_READER_NODE_ID_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterKvReaderNodeIdCacheEntry {
    mark: ClusterFileMark,
    node_id: NodeId,
    last_used: u64,
}

fn cluster_kv_metadata_cache_tick() -> u64 {
    CLUSTER_KV_METADATA_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn evict_cluster_kv_reader_node_id_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < FILE_CACHE_CAPACITY {
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

fn cached_cluster_kv_reader_node_id(path: &PathBuf, mark: ClusterFileMark) -> Option<NodeId> {
    let mut cache = CLUSTER_KV_READER_NODE_ID_CACHE.lock().ok()?;
    let entry = cache.get_mut(path)?;
    if entry.mark == mark {
        entry.last_used = cluster_kv_metadata_cache_tick();
        Some(entry.node_id.clone())
    } else {
        None
    }
}

fn record_cluster_kv_reader_node_id_cache(path: &PathBuf, mark: ClusterFileMark, node_id: NodeId) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        evict_cluster_kv_reader_node_id_cache_entry_if_needed(&mut cache, path);
        cache.insert(
            path.clone(),
            ClusterKvReaderNodeIdCacheEntry {
                mark,
                node_id,
                last_used: cluster_kv_metadata_cache_tick(),
            },
        );
    }
}

pub(crate) fn record_current_cluster_kv_reader_node_id_cache(
    paths: &ClusterPaths,
    node_id: NodeId,
) {
    if let Some(mark) = cluster_file_mark(&paths.state)
        && mark.exists
    {
        record_cluster_kv_reader_node_id_cache(&paths.state, mark, node_id);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn invalidate_cluster_kv_reader_node_id_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
fn record_cluster_kv_cached_snapshot_read_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        *reads.entry(paths.kv_paths().snapshot.clone()).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        reads.remove(&paths.kv_paths().snapshot);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&paths.kv_paths().snapshot).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cached_cluster_kv_reader_node_id_for_test(paths: &ClusterPaths) -> Option<NodeId> {
    let mark = cluster_file_mark(&paths.state)?;
    cached_cluster_kv_reader_node_id(&paths.state, mark)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_metadata_cache_capacity_for_test() -> usize {
    FILE_CACHE_CAPACITY
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf, node_id: &NodeId) {
    record_cluster_kv_reader_node_id_cache(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
        node_id.clone(),
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn refresh_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf) -> bool {
    cached_cluster_kv_reader_node_id(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
    )
    .is_some()
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_reader_node_id_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_READER_NODE_ID_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test(root: &std::path::Path) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

pub fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_local_cache(paths)
        .cached_snapshot()
        .map_err(kv_err)
}

pub fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let wrote =
        sinomo_kv::write_cluster_kv_materialized_snapshot_if_changed(&paths.kv_paths(), snapshot)
            .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write(paths, wrote, Some(snapshot));
    Ok(())
}

fn imported_cluster_kv_snapshot_phase_trace(
    phase: &'static str,
    duration: Duration,
    snapshot_revision: u64,
    route_materialization_required: bool,
    outcome: &'static str,
    detail: std::fmt::Arguments<'_>,
) {
    let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    tracing::trace!(
        target: "sinomo.cluster.formation",
        phase,
        duration_us,
        snapshot_revision,
        route_materialization_required,
        outcome,
        detail = %detail,
        "cluster imported KV snapshot phase"
    );
}

pub fn write_imported_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let trace_enabled =
        tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE);
    let route_projection_started = trace_enabled.then(|| {
        imported_cluster_kv_snapshot_phase_trace(
            "route_projection_current_snapshot_read",
            Duration::ZERO,
            snapshot.revision.0,
            false,
            "started",
            format_args!(
                "reading the current materialized snapshot and comparing route projection"
            ),
        );
        Instant::now()
    });
    let route_materialization_result =
        imported_snapshot_requires_route_read_model_materialization(paths, snapshot);
    if let Some(started) = route_projection_started {
        match &route_materialization_result {
            Ok(route_materialization_required) => imported_cluster_kv_snapshot_phase_trace(
                "route_projection_current_snapshot_read",
                started.elapsed(),
                snapshot.revision.0,
                *route_materialization_required,
                "completed",
                format_args!("current snapshot read and route projection comparison completed"),
            ),
            Err(error) => imported_cluster_kv_snapshot_phase_trace(
                "route_projection_current_snapshot_read",
                started.elapsed(),
                snapshot.revision.0,
                false,
                "error",
                format_args!("{error}"),
            ),
        }
    }
    let route_materialization_required = route_materialization_result?;
    let materialized_write_started = trace_enabled.then(|| {
        imported_cluster_kv_snapshot_phase_trace(
            "imported_materialized_snapshot_write",
            Duration::ZERO,
            snapshot.revision.0,
            route_materialization_required,
            "started",
            format_args!(
                "writing imported snapshot with route-sensitive materialization={route_materialization_required}"
            ),
        );
        Instant::now()
    });
    let write_result = if route_materialization_required {
        sinomo_kv::write_cluster_kv_snapshot_if_changed(&paths.kv_paths(), snapshot).map_err(kv_err)
    } else {
        sinomo_kv::write_imported_cluster_kv_snapshot_if_changed(
            &paths.kv_paths(),
            snapshot,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)
    };
    if let Some(started) = materialized_write_started {
        match &write_result {
            Ok(wrote) => imported_cluster_kv_snapshot_phase_trace(
                "imported_materialized_snapshot_write",
                started.elapsed(),
                snapshot.revision.0,
                route_materialization_required,
                if *wrote { "written" } else { "unchanged" },
                format_args!("imported snapshot materialization completed"),
            ),
            Err(error) => imported_cluster_kv_snapshot_phase_trace(
                "imported_materialized_snapshot_write",
                started.elapsed(),
                snapshot.revision.0,
                route_materialization_required,
                "error",
                format_args!("{error}"),
            ),
        }
    }
    let wrote = write_result?;
    observe_cluster_kv_snapshot_write(paths, wrote, Some(snapshot));
    Ok(wrote)
}

pub(crate) fn observe_cluster_kv_snapshot_write(
    paths: &ClusterPaths,
    wrote: bool,
    snapshot: Option<&KvSnapshot>,
) {
    observe_cluster_kv_snapshot_write_with_invalidation(paths, wrote, snapshot, true);
}

fn observe_cluster_kv_snapshot_write_with_invalidation(
    paths: &ClusterPaths,
    wrote: bool,
    snapshot: Option<&KvSnapshot>,
    invalidate_on_write: bool,
) {
    if wrote && invalidate_on_write {
        invalidate_cluster_kv_observed_read_models(paths);
    }
    if let Some(observer) = io_observer() {
        if wrote {
            observer.on_kv_snapshot_actual_write();
            if let Ok(metadata) = fs::metadata(&paths.kv_paths().snapshot) {
                observer.on_disk_write(metadata.len());
            }
        } else {
            observer.on_kv_snapshot_skipped_write();
        }
    }
    if let Some(snapshot) = snapshot {
        notify_cluster_kv_committed_snapshot_observers(paths, snapshot);
    }
}

fn observe_cluster_kv_committed_delta(
    paths: &ClusterPaths,
    revision: KvRevision,
    compacted_revision: KvRevision,
    events: Vec<KvEvent>,
    final_key_states: Vec<ClusterKvCommittedKeyState>,
    invalidate_read_models: bool,
) {
    if invalidate_read_models {
        invalidate_cluster_kv_observed_read_models_for_events(paths, &events);
    }
    notify_cluster_kv_committed_delta_observers(
        paths,
        &ClusterKvCommittedDelta {
            revision,
            compacted_revision,
            events,
            final_key_states,
        },
    );
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ClusterKvObservedReadModelInvalidation {
    cluster_state: bool,
    route_graph: bool,
}

#[cfg(test)]
static CLUSTER_KV_OBSERVED_READ_MODEL_INVALIDATIONS_FOR_TEST: LazyLock<
    Mutex<HashMap<PathBuf, (u64, u64)>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn cluster_kv_observed_read_model_invalidation_for_events(
    events: &[KvEvent],
) -> ClusterKvObservedReadModelInvalidation {
    let mut invalidation = ClusterKvObservedReadModelInvalidation::default();
    for event in events {
        let key = event.key.as_str();
        if key.starts_with("/cluster/members/")
            || key.starts_with("/cluster/endpoints/")
            || key.starts_with("/cluster/policy/capabilities/")
        {
            invalidation.cluster_state = true;
            invalidation.route_graph = true;
        } else if key.starts_with("/cluster/network/connectivity/") {
            invalidation.route_graph = true;
        }
        if invalidation.cluster_state && invalidation.route_graph {
            break;
        }
    }
    invalidation
}

fn invalidate_cluster_kv_observed_read_models_for_events(paths: &ClusterPaths, events: &[KvEvent]) {
    let invalidation = cluster_kv_observed_read_model_invalidation_for_events(events);
    invalidate_cluster_kv_observed_read_models_selective(paths, invalidation);
}

fn invalidate_cluster_kv_observed_read_models_selective(
    paths: &ClusterPaths,
    invalidation: ClusterKvObservedReadModelInvalidation,
) {
    #[cfg(test)]
    if let Ok(mut counts) = CLUSTER_KV_OBSERVED_READ_MODEL_INVALIDATIONS_FOR_TEST.lock() {
        let counts = counts.entry(paths.root.clone()).or_default();
        counts.0 = counts
            .0
            .saturating_add(u64::from(invalidation.cluster_state));
        counts.1 = counts.1.saturating_add(u64::from(invalidation.route_graph));
    }
    if invalidation.cluster_state {
        crate::local_state_store::invalidate_cluster_state_cache(paths);
    }
    if invalidation.route_graph {
        sinomo_network::invalidate_cluster_route_graph_cache(paths);
    }
}

/// Invalidates process-local projections derived from the committed cluster KV.
///
/// Local commit observers call this directly. A daemon watching the shared
/// materialized snapshot also calls it when another process publishes a newer
/// critical projection, because process-local observer callbacks cannot cross
/// the main/shadow process boundary.
pub fn invalidate_cluster_kv_observed_read_models(paths: &ClusterPaths) {
    invalidate_cluster_kv_observed_read_models_selective(
        paths,
        ClusterKvObservedReadModelInvalidation {
            cluster_state: true,
            route_graph: true,
        },
    );
}

fn imported_snapshot_requires_route_read_model_materialization(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let Some(existing) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    route_connectivity_projection_changed(existing.as_ref(), snapshot, OffsetDateTime::now_utc())
}

fn route_connectivity_projection_changed(
    existing: &KvSnapshot,
    incoming: &KvSnapshot,
    now: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let existing_records = live_connectivity_records_by_key(existing, now)?;
    let incoming_records = live_connectivity_records_by_key(incoming, now)?;
    if existing_records.len() != incoming_records.len() {
        return Ok(true);
    }
    for (key, incoming_record) in incoming_records {
        let Some(existing_record) = existing_records.get(&key) else {
            return Ok(true);
        };
        if !cluster_connectivity_publish_record_matches(existing_record, &incoming_record, now) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn live_connectivity_records_by_key(
    snapshot: &KvSnapshot,
    now: OffsetDateTime,
) -> Result<BTreeMap<String, ClusterConnectivityRecord>, NetworkError> {
    let mut records = BTreeMap::new();
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        let record = entry
            .decode::<ClusterConnectivityRecord>()
            .map_err(kv_err)?;
        if !cluster_network_record_live(record.expires_at, now) {
            continue;
        }
        records.insert(
            cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            ),
            record,
        );
    }
    Ok(records)
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_cluster_kv_store(paths: &ClusterPaths) -> Result<InProcessKvStore, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_store_for_node(&paths.kv_paths(), node_id).map_err(kv_err)
}

pub fn read_cluster_kv_current_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    read_cluster_kv_current_snapshot_arc(paths).map(|snapshot| (*snapshot).clone())
}

pub(crate) fn read_cluster_kv_current_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&paths.kv_paths(), node_id)
        .map_err(kv_err)
}

pub(crate) fn read_cluster_kv_current_or_cached_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(NetworkError::ClusterOrphan) => read_cluster_kv_cached_snapshot_if_present(paths),
        Err(error) => Err(error),
    }
}

pub fn read_cluster_kv_latest_observed_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    let current = match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Some(snapshot),
        Err(NetworkError::ClusterOrphan) => None,
        Err(error) => return Err(error),
    };
    let cached = read_cluster_kv_cached_snapshot_if_present(paths)?;
    Ok(match (current, cached) {
        (Some(current), Some(cached)) if cached.revision > current.revision => Some(cached),
        (Some(current), _) => Some(current),
        (None, cached) => cached,
    })
}

pub fn read_cluster_kv_latest_observed_snapshot(
    paths: &ClusterPaths,
) -> Result<KvSnapshot, NetworkError> {
    read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)?
        .as_deref()
        .cloned()
        .ok_or(NetworkError::ClusterOrphan)
}

pub(crate) fn read_cluster_kv_current_or_empty_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    match read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? {
        Some(snapshot) => Ok(snapshot),
        None => Ok(Arc::new(
            InProcessKvStore::new().try_snapshot().map_err(kv_err)?,
        )),
    }
}

pub(crate) fn read_cluster_kv_entry(
    paths: &ClusterPaths,
    key: &str,
) -> Result<Option<KvEntry>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    Ok(snapshot.get(key).cloned())
}

pub fn read_cluster_kv_latest_observed_entry(
    paths: &ClusterPaths,
    key: &str,
) -> Result<Option<KvEntry>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(None);
    };
    Ok(snapshot.get(key).cloned())
}

pub fn read_cluster_kv_latest_observed_prefix_entries(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    snapshot
        .range_prefix_limited(prefix, sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_cluster_kv_prefix_entries(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_cluster_kv_prefix_entries_limited(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        paths,
        prefix,
        max_entries,
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    sinomo_kv::read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        &paths.kv_paths(),
        prefix,
        max_entries,
        max_payload_bytes,
    )
    .map_err(kv_err)
}

pub(crate) fn read_cluster_kv_cached_snapshot_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    let snapshot = cluster_kv_local_cache(paths)
        .cached_snapshot_arc_if_present()
        .map_err(kv_err)?;
    #[cfg(test)]
    if snapshot.is_some() {
        record_cluster_kv_cached_snapshot_read_for_test(paths);
    }
    Ok(snapshot)
}

fn cluster_kv_local_cache(paths: &ClusterPaths) -> KvLocalCache {
    paths.kv_paths().local_cache()
}

pub(crate) fn read_cluster_kv_reader_node_id(paths: &ClusterPaths) -> Result<NodeId, NetworkError> {
    if let Some(mark) = cluster_file_mark(&paths.state)
        && mark.exists
        && let Some(node_id) = cached_cluster_kv_reader_node_id(&paths.state, mark)
    {
        return Ok(node_id);
    }
    match read_cluster_state_without_kv(paths) {
        Ok(state) => {
            let node_id = state.local_node_id;
            record_current_cluster_kv_reader_node_id_cache(paths, node_id.clone());
            Ok(node_id)
        }
        Err(_) if !paths.kv_paths().raft.exists() => {
            Ok(NodeId::from_legacy_named("cluster_kv_cache_reader")
                .expect("static cache reader node id"))
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    paths.kv_paths().raft_wal_paths(node_id)
}

pub fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    paths.kv_paths().open_raft_node(node_id).map_err(kv_err)
}

pub fn read_cluster_kv_local_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<sinomo_kv::KvLocalRead<KvSnapshot>, NetworkError> {
    let mut local =
        sinomo_kv::read_cluster_kv_local_snapshot(&paths.kv_paths(), &state.local_node_id)
            .map_err(kv_err)?;
    local.known_leader = local
        .known_leader
        .filter(|leader_id| state.node_is_raft_voter(leader_id));
    Ok(local)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cache_cluster_kv_snapshot_if_newer_than_raft(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    let observed = snapshot.clone();
    let changed = paths
        .kv_paths()
        .cache_snapshot_if_newer_than_raft_and_notify(node_id, snapshot)
        .map_err(kv_err)?;
    if changed {
        notify_cluster_kv_committed_snapshot_observers(paths, &observed);
    }
    Ok(changed)
}

pub(crate) fn locked_cache_cluster_kv_snapshot_if_newer_than_raft(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    let observed = snapshot.clone();
    let changed = match paths
        .kv_paths()
        .locked_cache_snapshot_if_newer_than_raft_and_notify(node_id, snapshot)
    {
        Ok(changed) => changed,
        // Snapshot refresh materializes the imported view before invoking this
        // reconciliation hook. If a foreground quorum command currently owns
        // the Raft node, do not fail the read-only refresh: the holder will
        // publish its local view when it commits, and a later refresh can
        // reconcile again. Other storage errors still fail closed.
        Err(KvError::Busy { .. }) => return Ok(false),
        Err(error) => return Err(kv_err(error)),
    };
    if changed {
        notify_cluster_kv_committed_snapshot_observers(paths, &observed);
    }
    Ok(changed)
}

pub(crate) fn locked_kv_state_authority_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    paths
        .kv_paths()
        .locked_known_raft_voter_leader_id(state)
        .map_err(kv_err)
}

pub(crate) async fn locked_kv_state_authority_id_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_kv_state_authority_id(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv state authority".to_string(),
            detail: format!("known state authority task failed: {source}"),
        })?
}

#[cfg(any(test, feature = "test-support"))]
pub async fn open_cluster_raft_kv_node_async(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    paths
        .kv_paths()
        .open_raft_node_async(node_id)
        .await
        .map_err(kv_err)
}

pub fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command_with_response(paths, node_id, command).map(|(snapshot, _)| snapshot)
}

/// Revision of the latest-observed cluster KV snapshot — the max of the local
/// raft view and the imported snapshot cache, i.e. the SAME source the KV
/// read model serves directory reads from. `None` when neither exists.
pub(crate) fn latest_observed_cluster_kv_snapshot_revision(
    paths: &ClusterPaths,
) -> Result<Option<KvRevision>, NetworkError> {
    Ok(
        read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)?
            .map(|snapshot| snapshot.revision),
    )
}

/// The local node's current cluster KV snapshot revision, if one is readable;
/// `None` asks the voter to ship its full snapshot back.
pub(crate) fn local_cluster_kv_snapshot_revision(paths: &ClusterPaths) -> Option<KvRevision> {
    let node_id = read_cluster_kv_reader_node_id(paths).ok()?;
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&paths.kv_paths(), node_id)
        .map(|snapshot| snapshot.revision)
        .ok()
}

pub(crate) fn imported_snapshot_advances_local_kv(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> bool {
    let Ok(node_id) = read_cluster_kv_reader_node_id(paths) else {
        return true;
    };
    paths
        .kv_paths()
        .imported_snapshot_advances_current_for_node(&node_id, snapshot)
}

pub(crate) fn append_cluster_raft_ops_in_batches(
    paths: &ClusterPaths,
    node_id: &NodeId,
    ops: Vec<KvOp>,
) -> Result<KvSnapshot, NetworkError> {
    let (snapshot, wrote) = paths
        .kv_paths()
        .append_kv_ops_in_batches_and_publish(
            node_id,
            ops,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write(paths, wrote, Some(&snapshot));
    Ok(snapshot)
}

pub fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let (snapshot, txn_response, wrote) = paths
        .kv_paths()
        .append_kv_command_with_response_and_publish_report(
            node_id,
            command,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write(paths, wrote, Some(&snapshot));
    Ok((snapshot, txn_response))
}

#[cfg(any(test, feature = "test-support"))]
pub fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    paths
        .kv_paths()
        .append_raft_command_uncommitted(node_id, command)
        .map_err(kv_err)
}

pub fn apply_cluster_raft_append(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: Option<RaftSnapshot>,
    append: RaftAppendEntriesRequest,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let (outcome, wrote) = paths
        .kv_paths()
        .apply_raft_append_compact_with_publish_report(node_id, snapshot, append)
        .map_err(kv_err)?;
    let observed = outcome
        .restored_snapshot
        .as_ref()
        .or(outcome.checkpoint_snapshot.as_ref());
    let restored_snapshot = outcome.restored_snapshot.is_some();
    let snapshot_invalidates_read_models = wrote && (!outcome.commit_advanced || restored_snapshot);
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        observed,
        !outcome.commit_advanced || restored_snapshot,
    );
    if outcome.commit_advanced {
        observe_cluster_kv_committed_delta(
            paths,
            outcome.read_state.revision,
            outcome.compacted_revision,
            outcome.committed_events,
            outcome.final_key_states,
            !snapshot_invalidates_read_models,
        );
    }
    Ok(outcome.response)
}

pub(crate) fn locked_apply_cluster_raft_vote(
    paths: &ClusterPaths,
    node_id: &NodeId,
    vote: RaftRequestVoteRequest,
) -> Result<RaftRequestVoteResponse, NetworkError> {
    paths
        .kv_paths()
        .locked_apply_raft_vote(node_id, vote)
        .map_err(kv_err)
}

pub(crate) struct LocalClusterRaftPostCommitObservations {
    paths: ClusterPaths,
    snapshot_write: Option<(bool, KvSnapshot)>,
    delta: Option<ClusterKvCommittedDelta>,
    watcher_events: Option<Vec<KvEvent>>,
}

impl LocalClusterRaftPostCommitObservations {
    pub(crate) fn publish(self) {
        let Self {
            paths,
            snapshot_write,
            delta,
            watcher_events,
        } = self;
        let has_delta = delta.is_some();
        if let Some(delta) = delta {
            observe_cluster_kv_committed_delta(
                &paths,
                delta.revision,
                delta.compacted_revision,
                delta.events,
                delta.final_key_states,
                true,
            );
        }
        if let Some((wrote, snapshot)) = snapshot_write {
            // A normal compact commit invalidates only the projections touched
            // by its exact event keys. Snapshot publication is still observed,
            // but must not invalidate the same caches a second time. Exact
            // replay repair has no delta, so conservatively invalidate both.
            observe_cluster_kv_snapshot_write_with_invalidation(
                &paths,
                wrote,
                Some(&snapshot),
                !has_delta,
            );
        }
        if let Some(events) = watcher_events {
            if events.is_empty() {
                sinomo_kv::notify_cluster_kv_watchers(&paths.kv_paths());
            } else {
                sinomo_kv::notify_cluster_kv_watchers_for_events(&paths.kv_paths(), &events);
            }
        }
    }
}

pub struct LocalClusterRaftCommandStorage {
    paths: ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

impl LocalClusterRaftCommandStorage {
    pub fn new(paths: &ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            paths: paths.clone(),
            inner: paths.kv_paths().local_raft_command_storage(
                local_node_id,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
            ),
        }
    }

    fn observe_last_snapshot_publish(&mut self, snapshot: &KvSnapshot) {
        if let Some(wrote) = self.inner.take_last_snapshot_publish_wrote() {
            observe_cluster_kv_snapshot_write(&self.paths, wrote, Some(snapshot));
        }
    }

    pub(crate) fn local_is_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.inner.local_is_leader(state).map_err(kv_err)
    }

    pub(crate) fn last_log_index(&mut self) -> Result<u64, NetworkError> {
        self.inner.last_log_index().map_err(kv_err)
    }

    pub(crate) fn read_index_state(
        &mut self,
    ) -> Result<sinomo_kv::ClusterKvRaftReadIndexState, NetworkError> {
        self.inner.read_index_state().map_err(kv_err)
    }

    pub(crate) fn read_index_snapshot(&mut self) -> Result<KvSnapshot, NetworkError> {
        self.inner.read_index_snapshot().map_err(kv_err)
    }

    pub(crate) fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, NetworkError> {
        self.inner.read_index_entry(key).map_err(kv_err)
    }

    pub(crate) fn read_index_request_outcome(
        &mut self,
        request_id: &sinomo_kv::KvRequestId,
    ) -> Result<Option<sinomo_kv::KvRequestOutcome>, NetworkError> {
        self.inner
            .read_index_request_outcome(request_id)
            .map_err(kv_err)
    }

    pub(crate) fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, NetworkError> {
        self.inner
            .read_index_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
            .map_err(kv_err)
    }

    pub(crate) fn release_session_after_commit(&mut self) {
        self.inner.release_session_after_commit();
    }

    pub(crate) fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        self.inner
            .build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(kv_err)
    }

    pub(crate) fn raft_append_request_uses_snapshot(
        &mut self,
        next_index: u64,
    ) -> Result<bool, NetworkError> {
        self.inner
            .cluster_raft_append_request_uses_snapshot(next_index)
            .map_err(kv_err)
    }

    pub(crate) fn replay_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, NetworkError> {
        self.inner.replay_if_committed(request).map_err(kv_err)
    }

    pub(crate) fn replay_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, NetworkError> {
        self.inner
            .replay_record_if_committed(request)
            .map_err(kv_err)
    }

    pub(crate) fn append_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, NetworkError> {
        self.inner.append_uncommitted(request).map_err(kv_err)
    }

    pub(crate) fn append_uncommitted_observed(
        &mut self,
        request: KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftObservedAppend {
        self.inner.append_uncommitted_observed(request)
    }

    pub(crate) fn record_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        self.inner
            .record_rejected_before_commit(request, failure)
            .map_err(kv_err)
    }

    pub(crate) fn record_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        self.inner.record_unknown(request, detail).map_err(kv_err)
    }

    pub(crate) fn discard_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<KvSnapshot, NetworkError> {
        let snapshot = self
            .inner
            .discard_uncommitted_tail_and_clear_leader()
            .map_err(kv_err)?;
        self.observe_last_snapshot_publish(&snapshot);
        Ok(snapshot)
    }

    pub(crate) fn discard_uncommitted_tail_and_clear_leader_compact(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.inner
            .discard_uncommitted_tail_and_clear_leader_compact()
            .map_err(kv_err)
    }

    pub(crate) fn observe_term(&mut self, term: u64) -> Result<(), NetworkError> {
        self.inner.observe_term(term).map_err(kv_err)
    }

    pub(crate) fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        let result = self.inner.commit_index_with_events(index).map_err(kv_err)?;
        self.observe_last_snapshot_publish(&result.snapshot);
        Ok(result)
    }

    pub(crate) fn commit_index_compact(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        self.inner.commit_index_compact(index).map_err(kv_err)
    }

    pub(crate) fn take_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<LocalClusterRaftPostCommitObservations, NetworkError> {
        let snapshot_publish_wrote = self.inner.take_last_snapshot_publish_wrote();
        let snapshot_write = match (snapshot_publish_wrote, commit) {
            (Some(wrote), Some(commit)) => commit
                .checkpoint_snapshot
                .as_ref()
                .map(|snapshot| (wrote, snapshot.clone())),
            (Some(true), None) => Some((
                true,
                sinomo_kv::read_cluster_kv_cached_snapshot(&self.paths.kv_paths())
                    .map_err(kv_err)?,
            )),
            _ => None,
        };
        let delta = commit.map(|commit| ClusterKvCommittedDelta {
            revision: commit.committed_request.revision,
            compacted_revision: commit.compacted_revision,
            events: commit.events.clone(),
            final_key_states: commit.final_key_states.clone(),
        });
        let watcher_events = commit
            .map(|commit| commit.events.clone())
            .or_else(|| snapshot_write.as_ref().map(|_| Vec::new()));
        Ok(LocalClusterRaftPostCommitObservations {
            paths: self.paths.clone(),
            snapshot_write,
            delta,
            watcher_events,
        })
    }
}

impl sinomo_kv::ClusterKvRaftCommandStorage for LocalClusterRaftCommandStorage {
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, NetworkError> {
        self.replay_if_committed(request)
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, NetworkError> {
        self.replay_record_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, NetworkError> {
        self.append_uncommitted(request)
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        self.record_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        self.record_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.discard_uncommitted_tail_and_clear_leader_compact()
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.observe_term(term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        self.commit_index_with_events(index)
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        self.commit_index_compact(index)
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), NetworkError> {
        self.take_post_commit_observations(commit)?.publish();
        Ok(())
    }
}

/// Derives the KV file layout for a cluster root. Lives beside the KV-backed
/// store code (not on `ClusterPaths` itself) so the networking library stays
/// free of KV layout knowledge.
pub trait ClusterKvPathsExt {
    fn kv_paths(&self) -> sinomo_kv::ClusterKvPaths;
}

impl ClusterKvPathsExt for sinomo_network::ClusterPaths {
    fn kv_paths(&self) -> sinomo_kv::ClusterKvPaths {
        sinomo_kv::ClusterKvPaths::new(&self.root)
    }
}

/// Wraps a KV store error for the networking error type. Lives with the
/// KV-backed store code; moves to `sinomo-cluster` with it.
pub fn kv_err(error: sinomo_kv::KvError) -> sinomo_network::NetworkError {
    match error {
        sinomo_kv::KvError::AuthorityUnavailable { detail } => {
            sinomo_network::NetworkError::ClusterReadOnly { detail }
        }
        error @ sinomo_kv::KvError::InvalidRaftConfigTransition { .. } => {
            sinomo_network::NetworkError::InvalidClusterCommand {
                detail: error.to_string(),
            }
        }
        error => sinomo_network::NetworkError::Directory {
            detail: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(key: &str) -> KvEvent {
        KvEvent {
            revision: KvRevision(1),
            key: key.to_string(),
            kind: sinomo_kv::KvEventKind::Put {
                value: b"value".to_vec(),
            },
        }
    }

    fn reset_observed_read_model_invalidation_counts(paths: &ClusterPaths) {
        CLUSTER_KV_OBSERVED_READ_MODEL_INVALIDATIONS_FOR_TEST
            .lock()
            .expect("invalidation test counts")
            .remove(&paths.root);
    }

    fn observed_read_model_invalidation_counts(paths: &ClusterPaths) -> (u64, u64) {
        CLUSTER_KV_OBSERVED_READ_MODEL_INVALIDATIONS_FOR_TEST
            .lock()
            .expect("invalidation test counts")
            .get(&paths.root)
            .copied()
            .unwrap_or_default()
    }

    #[test]
    fn committed_delta_invalidates_only_affected_cluster_read_models() {
        let none = ClusterKvObservedReadModelInvalidation::default();
        assert_eq!(
            cluster_kv_observed_read_model_invalidation_for_events(&[]),
            none
        );
        for key in [
            "/config/ui/theme",
            "/dfs/repair/job",
            "/cluster/services/catalog",
            "/cluster/network/info/node-a",
            "/cluster/raft/config",
            "/cluster/raft/config-changes/change-a",
        ] {
            assert_eq!(
                cluster_kv_observed_read_model_invalidation_for_events(&[test_event(key)]),
                none,
                "unrelated key {key} must retain cluster read-model caches"
            );
        }

        let state_and_route = ClusterKvObservedReadModelInvalidation {
            cluster_state: true,
            route_graph: true,
        };
        for key in [
            "/cluster/members/node-a",
            "/cluster/endpoints/node-a/endpoint-a",
            "/cluster/policy/capabilities/node-a",
        ] {
            assert_eq!(
                cluster_kv_observed_read_model_invalidation_for_events(&[test_event(key)]),
                state_and_route,
                "routing directory key {key} must invalidate state and route projections"
            );
        }
        assert_eq!(
            cluster_kv_observed_read_model_invalidation_for_events(&[test_event(
                "/cluster/network/connectivity/node-a/node-b/endpoint-a",
            )]),
            ClusterKvObservedReadModelInvalidation {
                cluster_state: false,
                route_graph: true,
            }
        );
    }

    #[test]
    fn local_snapshot_and_delta_publication_invalidates_each_affected_cache_once() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-kv-observation-invalidation");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let snapshot = InProcessKvStore::new()
            .try_snapshot()
            .expect("empty snapshot");

        reset_observed_read_model_invalidation_counts(&paths);
        LocalClusterRaftPostCommitObservations {
            paths: paths.clone(),
            snapshot_write: Some((true, snapshot.clone())),
            delta: Some(ClusterKvCommittedDelta {
                revision: KvRevision(1),
                compacted_revision: KvRevision::ZERO,
                events: vec![test_event("/cluster/members/node-a")],
                final_key_states: Vec::new(),
            }),
            watcher_events: None,
        }
        .publish();
        assert_eq!(observed_read_model_invalidation_counts(&paths), (1, 1));

        reset_observed_read_model_invalidation_counts(&paths);
        LocalClusterRaftPostCommitObservations {
            paths: paths.clone(),
            snapshot_write: Some((true, snapshot)),
            delta: Some(ClusterKvCommittedDelta {
                revision: KvRevision(2),
                compacted_revision: KvRevision::ZERO,
                events: vec![test_event("/cluster/services/catalog")],
                final_key_states: Vec::new(),
            }),
            watcher_events: None,
        }
        .publish();
        assert_eq!(
            observed_read_model_invalidation_counts(&paths),
            (0, 0),
            "an unrelated compact delta must not invalidate caches merely because its critical snapshot was published"
        );
    }

    #[test]
    fn imported_snapshot_reconciliation_defers_while_local_raft_node_is_busy() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-kv-import-busy");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let node_id = NodeId::from_legacy_named("node_kv_import_busy").expect("node");
        let held = open_cluster_raft_kv_node(&paths, &node_id).expect("hold local raft node");
        let mut imported = InProcessKvStore::new();
        imported
            .put("/imported", b"value".to_vec(), None)
            .expect("imported value");
        let snapshot = imported.try_snapshot().expect("imported snapshot");

        assert!(
            !locked_cache_cluster_kv_snapshot_if_newer_than_raft(
                &paths,
                &node_id,
                snapshot.clone(),
            )
            .expect("busy reconciliation is deferred")
        );

        drop(held);
        assert!(
            locked_cache_cluster_kv_snapshot_if_newer_than_raft(&paths, &node_id, snapshot)
                .expect("reconcile after lock release")
        );
    }
}
