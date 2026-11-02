#[cfg(any(test, feature = "test-support"))]
use crate::connectivity_publish::cluster_connectivity_publish_record_matches;
#[cfg(any(test, feature = "test-support"))]
use crate::keyspace::cluster_connectivity_key;
use crate::local_state_store::read_cluster_state_without_kv;
#[cfg(test)]
use crate::raft_quorum::{
    ClusterRaftBlockingCancellationPolicy, ClusterRaftBlockingDeadlinePolicy,
    run_cluster_raft_supervised_blocking,
};
#[allow(unused_imports)]
use sinomo_kv::KvError;
#[cfg(any(test, feature = "test-support"))]
use sinomo_network::ClusterConnectivityRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::current_io_observer as io_observer;
use sinomo_network::{ClusterFileMark, cluster_file_mark};

use sinomo_base::ClusterBootstrapState;
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvWalPaths};
use sinomo_kv::{
    ClusterKvCommittedDelta, ClusterKvCommittedKeyState, ClusterKvRaftCommitResult,
    ClusterRaftAppendRequest, DurableRaftKvNode, InProcessKvStore, KvCommand, KvCommandRequest,
    KvCommittedRequestRecord, KvEntry, KvEvent, KvLocalCache, KvLogEntry, KvOp, KvRevision,
    KvSnapshot, KvTxnResponse, RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftSnapshot,
};
use sinomo_node::NodeId;
#[cfg(any(test, feature = "test-support"))]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(any(test, feature = "test-support"))]
use std::time::Duration;
use std::time::Instant;
#[cfg(any(test, feature = "test-support"))]
use std::time::SystemTime;
#[cfg(any(test, feature = "test-support"))]
use time::OffsetDateTime;

const FILE_CACHE_CAPACITY: usize = 4096;
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_READER_NODE_ID_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_CURRENT_SNAPSHOT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvCurrentSnapshotCacheEntry>>,
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

#[derive(Clone)]
struct ClusterKvCurrentSnapshotCacheEntry {
    node_id: NodeId,
    revision: KvRevision,
    resident_lock_mark: ClusterFileMark,
    snapshot: Arc<KvSnapshot>,
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

fn evict_cluster_kv_current_snapshot_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvCurrentSnapshotCacheEntry>,
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

fn cached_cluster_kv_current_snapshot(
    root: &PathBuf,
    node_id: &NodeId,
    revision: KvRevision,
    resident_lock_mark: ClusterFileMark,
) -> Option<Arc<KvSnapshot>> {
    let mut cache = CLUSTER_KV_CURRENT_SNAPSHOT_CACHE.lock().ok()?;
    let entry = cache.get_mut(root)?;
    if entry.node_id != *node_id
        || entry.revision != revision
        || entry.resident_lock_mark != resident_lock_mark
    {
        return None;
    }
    entry.last_used = cluster_kv_metadata_cache_tick();
    Some(Arc::clone(&entry.snapshot))
}

fn record_cluster_kv_current_snapshot(
    root: PathBuf,
    node_id: NodeId,
    resident_lock_mark: ClusterFileMark,
    snapshot: Arc<KvSnapshot>,
) {
    let Ok(mut cache) = CLUSTER_KV_CURRENT_SNAPSHOT_CACHE.lock() else {
        return;
    };
    evict_cluster_kv_current_snapshot_cache_entry_if_needed(&mut cache, &root);
    cache.insert(
        root,
        ClusterKvCurrentSnapshotCacheEntry {
            node_id,
            revision: snapshot.revision,
            resident_lock_mark,
            snapshot,
            last_used: cluster_kv_metadata_cache_tick(),
        },
    );
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
    let (wrote, delivery_prepared) =
        sinomo_kv::write_cluster_kv_materialized_snapshot_if_changed_with_observation_report(
            &paths.kv_paths(),
            snapshot,
        )
        .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        Some(snapshot),
        !delivery_prepared,
    );
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
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
        sinomo_kv::write_cluster_kv_snapshot_if_changed_with_observation_report(
            &paths.kv_paths(),
            snapshot,
        )
        .map_err(kv_err)
    } else {
        sinomo_kv::write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
            &paths.kv_paths(),
            snapshot,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)
    };
    if let Some(started) = materialized_write_started {
        match &write_result {
            Ok((wrote, _)) => imported_cluster_kv_snapshot_phase_trace(
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
    let (wrote, delivery_prepared) = write_result?;
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        Some(snapshot),
        !delivery_prepared,
    );
    Ok(wrote)
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
    let _ = snapshot;
}

fn observe_cluster_kv_committed_delta(
    paths: &ClusterPaths,
    revision: KvRevision,
    compacted_revision: KvRevision,
    events: Arc<Vec<KvEvent>>,
    final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    invalidate_read_models: bool,
) {
    if invalidate_read_models {
        invalidate_cluster_kv_observed_read_models_for_events(paths, &events);
    }
    let _ = (revision, compacted_revision, final_key_states);
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

pub(crate) fn invalidate_cluster_kv_observed_read_models_for_events(
    paths: &ClusterPaths,
    events: &[KvEvent],
) {
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
        crate::kv_read_model::invalidate_cluster_directory_projection(paths);
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

#[cfg(any(test, feature = "test-support"))]
fn imported_snapshot_requires_route_read_model_materialization(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let Some(existing) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    route_connectivity_projection_changed(existing.as_ref(), snapshot, OffsetDateTime::now_utc())
}

#[cfg(any(test, feature = "test-support"))]
fn route_connectivity_projection_changed(
    existing: &KvSnapshot,
    incoming: &KvSnapshot,
    now: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let existing_records = committed_connectivity_records_by_key(existing)?;
    let incoming_records = committed_connectivity_records_by_key(incoming)?;
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

#[cfg(any(test, feature = "test-support"))]
// Routing reads every committed row as its source's current statement, so
// the route projection changes with committed semantics, not with row age.
fn committed_connectivity_records_by_key(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<String, ClusterConnectivityRecord>, NetworkError> {
    let mut records = BTreeMap::new();
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        let record = entry
            .decode::<ClusterConnectivityRecord>()
            .map_err(kv_err)?;
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
    let kv_paths = paths.kv_paths();
    let resident_revision = sinomo_kv::with_cluster_kv_resident_read_projection(
        &kv_paths.raft_wal_paths(&node_id),
        &node_id,
        |projection| {
            sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_state(projection).revision
        },
    )
    .transpose()
    .map_err(kv_err)?
    .map(|read| read.value);
    let resident_lock_mark =
        resident_revision.and_then(|_| cluster_file_mark(&kv_paths.raft_wal_paths(&node_id).lock));
    if let (Some(revision), Some(resident_lock_mark)) = (resident_revision, resident_lock_mark)
        && let Some(snapshot) =
            cached_cluster_kv_current_snapshot(&paths.root, &node_id, revision, resident_lock_mark)
    {
        return Ok(snapshot);
    }

    let snapshot =
        sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&kv_paths, node_id.clone())
            .map_err(kv_err)?;
    if resident_revision.is_some_and(|revision| revision == snapshot.revision)
        && let Some(resident_lock_mark) = resident_lock_mark
    {
        record_cluster_kv_current_snapshot(
            paths.root.clone(),
            node_id,
            resident_lock_mark,
            Arc::clone(&snapshot),
        );
    }
    Ok(snapshot)
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
    // Shadow daemons install a one-way local-Raft ownership denial for this
    // root. Their routing/authentication read model is the materialized
    // projection only; trying the current voter first would either seize the
    // main voter's node lock or repeatedly fail before reaching the safe cache.
    if sinomo_kv::process_local_raft_access_is_denied(&paths.kv_paths().raft) {
        return read_cluster_kv_cached_snapshot_if_present(paths);
    }
    // The resident applied Raft state is the canonical view wherever it
    // exists; the materialized checkpoint serves only a root without it.
    match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(NetworkError::ClusterOrphan) => read_cluster_kv_cached_snapshot_if_present(paths),
        Err(error) => Err(error),
    }
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

/// Peeks only at the process-hot materialized snapshot.
///
/// Forwarding calls this after a compact response or fanout notification. A
/// cache miss must stay a miss: opening the Raft node or decoding the complete
/// materialized file here would move unbounded local hydration into the
/// caller's end-to-end network deadline.
pub(crate) fn read_cluster_kv_cached_snapshot_if_hot(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    sinomo_kv::read_cluster_kv_cached_snapshot_arc_if_hot(&paths.kv_paths()).map_err(kv_err)
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

pub(crate) fn locked_kv_state_authority_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    paths
        .kv_paths()
        .locked_known_raft_voter_leader_id(state)
        .map_err(kv_err)
}

#[cfg(test)]
pub(crate) async fn locked_kv_state_authority_id_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
) -> Result<Option<NodeId>, NetworkError> {
    fn deadline_elapsed(paths: &ClusterPaths) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: format!(
                "cluster KV state-authority lookup did not complete before its admission deadline at {}",
                paths.root.display()
            ),
        }
    }

    if Instant::now() >= deadline {
        return Err(deadline_elapsed(paths));
    }
    let paths = paths.clone();
    let state = state.clone();
    let task_paths = paths.clone();
    let timeout_error = deadline_elapsed(&paths);
    run_cluster_raft_supervised_blocking(
        runtime,
        Some((
            deadline,
            timeout_error,
            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
        )),
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "state_authority_lookup",
        "resident authority lookup",
        "cluster KV state authority",
        move || {
            // A task can remain in Tokio's blocking queue after its async caller's
            // deadline. Refuse operation-lock admission when that happens.
            if Instant::now() >= deadline {
                return Err(deadline_elapsed(&task_paths));
            }
            let authority_id = task_paths
                .kv_paths()
                .locked_known_raft_voter_leader_id_before(&state, deadline)
                .map_err(kv_err)?;
            // The timed Raft lock checks after acquisition. Check once more after
            // the hard-state read so a delayed disk lookup cannot publish a result
            // outside the caller's budget.
            if Instant::now() >= deadline {
                return Err(deadline_elapsed(&task_paths));
            }
            Ok(authority_id)
        },
    )
    .await
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

pub(crate) fn append_cluster_raft_ops_in_batches(
    paths: &ClusterPaths,
    node_id: &NodeId,
    ops: Vec<KvOp>,
) -> Result<KvSnapshot, NetworkError> {
    let (snapshot, wrote, delivery_prepared) = paths
        .kv_paths()
        .append_kv_ops_in_batches_and_publish_with_observation_report(
            node_id,
            ops,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        Some(&snapshot),
        !delivery_prepared,
    );
    Ok(snapshot)
}

pub fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let (result, wrote) = paths
        .kv_paths()
        .raft_store()
        .append_command_with_response_and_publish_report(
            node_id,
            command,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?;
    let delivery_prepared = result
        .observation_receipt
        .as_ref()
        .is_some_and(|receipt| receipt.was_delivery_prepared());
    let ClusterKvRaftCommitResult {
        snapshot,
        txn_response,
        events,
        committed_request,
        ..
    } = result;
    let published_delta = !events.is_empty();
    if published_delta {
        let mut keys = Vec::new();
        sinomo_network::try_reserve_vec(
            &mut keys,
            events.len(),
            "collect local committed cluster KV delta keys",
        )?;
        for event in events.iter() {
            keys.push(event.key.as_str());
        }
        keys.sort_unstable();
        keys.dedup();

        let mut final_key_states = Vec::new();
        sinomo_network::try_reserve_vec(
            &mut final_key_states,
            keys.len(),
            "collect local committed cluster KV final key states",
        )?;
        for key in keys {
            final_key_states.push(ClusterKvCommittedKeyState {
                key: key.to_string(),
                entry: snapshot.get(key).cloned(),
            });
        }
        observe_cluster_kv_committed_delta(
            paths,
            committed_request.revision,
            snapshot.compacted_revision,
            events,
            final_key_states.into(),
            !delivery_prepared,
        );
    }
    // A committed delta already invalidated only the read models touched by
    // its exact event keys. The snapshot notification remains useful to
    // compatibility observers, but must not conservatively invalidate those
    // models a second time.
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        Some(&snapshot),
        !published_delta && !delivery_prepared,
    );
    Ok((Arc::unwrap_or_clone(snapshot), txn_response))
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
    publish_cluster_raft_apply_outcome(paths, outcome, wrote)
}

/// Installs an authority's anchored snapshot into this live replica when it is
/// ahead of local state, and publishes it as a leader-sent snapshot is.
pub fn install_cluster_kv_anchored_snapshot(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: RaftSnapshot,
) -> Result<bool, NetworkError> {
    let Some((outcome, wrote)) = paths
        .kv_paths()
        .raft_store()
        .install_anchored_snapshot_compact_and_publish(node_id, snapshot)
        .map_err(kv_err)?
    else {
        return Ok(false);
    };
    publish_cluster_raft_apply_outcome(paths, outcome, wrote)?;
    Ok(true)
}

fn publish_cluster_raft_apply_outcome(
    paths: &ClusterPaths,
    outcome: sinomo_kv::RaftAppendEntriesCompactApplyOutcome,
    wrote: bool,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let delivery_prepared = outcome
        .observation_receipt
        .as_ref()
        .is_some_and(|receipt| receipt.was_delivery_prepared());
    let restored_snapshot = outcome.restored_snapshot.is_some();
    let observed = outcome.restored_snapshot.or(outcome.checkpoint_snapshot);
    let delta = outcome.commit_advanced.then(|| ClusterKvCommittedDelta {
        revision: outcome.read_state.revision,
        compacted_revision: outcome.compacted_revision,
        events: (outcome.committed_events).into(),
        final_key_states: (outcome.final_key_states).into(),
    });
    if !restored_snapshot {
        // A follower checkpoint describes this same committed batch. Publish
        // its complete delta first, as local commits do, so retained observers
        // can advance their proof before seeing the compatibility snapshot.
        LocalClusterRaftPostCommitObservations {
            paths: paths.clone(),
            snapshot_applied_index: observed.as_ref().map(|_| outcome.read_state.applied_index),
            snapshot_write: observed.map(|snapshot| (wrote, snapshot)),
            delta,
            observation_receipt: outcome.observation_receipt,
            watcher_events: None,
        }
        .publish()?;
        return Ok(outcome.response);
    }
    // Installing a leader snapshot replaces history rather than extending a
    // retained proof. Preserve invalidation before any subsequent delta.
    let snapshot_invalidates_read_models =
        wrote && (!outcome.commit_advanced || restored_snapshot) && !delivery_prepared;
    observe_cluster_kv_snapshot_write_with_invalidation(
        paths,
        wrote,
        observed.as_deref(),
        (!outcome.commit_advanced || restored_snapshot) && !delivery_prepared,
    );
    if let Some(delta) = delta {
        observe_cluster_kv_committed_delta(
            paths,
            delta.revision,
            delta.compacted_revision,
            delta.events,
            delta.final_key_states,
            !snapshot_invalidates_read_models && !delivery_prepared,
        );
    }
    Ok(outcome.response)
}

pub(crate) struct LocalClusterRaftPostCommitObservations {
    paths: ClusterPaths,
    snapshot_write: Option<(bool, Arc<KvSnapshot>)>,
    snapshot_applied_index: Option<u64>,
    delta: Option<ClusterKvCommittedDelta>,
    observation_receipt: Option<sinomo_kv::KvCommittedObservationReceipt>,
    watcher_events: Option<Arc<Vec<KvEvent>>>,
}

impl LocalClusterRaftPostCommitObservations {
    pub(crate) fn publish(self) -> Result<(), NetworkError> {
        let Self {
            paths,
            snapshot_write,
            snapshot_applied_index,
            delta,
            observation_receipt,
            watcher_events,
        } = self;
        let has_delta = delta.is_some();
        let publication_result =
            if let (Some(receipt), Some(delta)) = (&observation_receipt, &delta) {
                receipt
                    .ready(Arc::new(sinomo_kv::KvCommittedObservationPayload {
                        delta: Some(Arc::new(delta.clone())),
                        // Only the checkpoint captured at this actual native
                        // prefix belongs in the receipt. Standalone cache repair
                        // does not acquire an invented applied index.
                        snapshot: snapshot_applied_index.and_then(|_| {
                            snapshot_write
                                .as_ref()
                                .map(|(_, snapshot)| Arc::clone(snapshot))
                        }),
                        snapshot_applied_index,
                        encoded_bytes: None,
                    }))
                    .map_err(|error| {
                        NetworkError::outcome_unknown(
                            "deliver committed KV observation",
                            None,
                            error.to_string(),
                        )
                    })
                    .and_then(|_| {
                        receipt.wait_delivered().map_err(|error| {
                            NetworkError::outcome_unknown(
                                "deliver committed KV observation",
                                None,
                                error.to_string(),
                            )
                        })
                    })
            } else {
                Ok(())
            };
        let delivery_prepared = observation_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.was_delivery_prepared());
        if let Some(delta) = delta {
            observe_cluster_kv_committed_delta(
                &paths,
                delta.revision,
                delta.compacted_revision,
                delta.events,
                delta.final_key_states,
                !delivery_prepared,
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
                !has_delta && !delivery_prepared,
            );
        }
        if let Some(events) = watcher_events {
            if events.is_empty() {
                sinomo_kv::notify_cluster_kv_watchers(&paths.kv_paths());
            } else {
                sinomo_kv::notify_cluster_kv_watchers_for_events(&paths.kv_paths(), &events);
            }
        }
        publication_result
    }
}

pub struct LocalClusterRaftCommandStorage {
    paths: ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

pub(crate) enum LocalClusterRaftReadIndexAdmission {
    Admitted(sinomo_kv::ClusterKvRaftReadIndexState),
    Busy,
}

/// The bounded value projection performed after a ReadIndex or compact barrier
/// has established current-term quorum authority.
///
/// Mutable command storage implements this for compatibility full snapshots.
/// Point, exact-set, request-outcome, and bounded-prefix reads use the borrowed
/// resident implementation so concurrent readers do not re-enter the exclusive
/// Raft operation queue.
pub(crate) trait LocalClusterRaftReadProjection {
    fn read_index_state(&mut self) -> Result<sinomo_kv::ClusterKvRaftReadIndexState, NetworkError>;
    fn read_index_snapshot(&mut self) -> Result<KvSnapshot, NetworkError>;
    fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, NetworkError>;
    fn pending_current_term_barrier(&mut self) -> Result<Option<KvLogEntry>, NetworkError>;
    fn build_raft_read_index_heartbeat(
        &mut self,
        node_id: &NodeId,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError>;
    fn read_index_entries(&mut self, keys: &[String])
    -> Result<Vec<Option<KvEntry>>, NetworkError>;
    fn read_index_request_outcome(
        &mut self,
        request_id: &sinomo_kv::KvRequestId,
    ) -> Result<Option<sinomo_kv::KvRequestOutcome>, NetworkError>;
    fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, NetworkError>;
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

    pub(crate) fn new_with_operation_deadline(
        paths: &ClusterPaths,
        local_node_id: NodeId,
        operation_deadline: Instant,
    ) -> Self {
        Self {
            paths: paths.clone(),
            inner: sinomo_kv::ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
                paths.kv_paths().raft_store(),
                local_node_id,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
                operation_deadline,
            ),
        }
    }

    pub(crate) fn new_read_index_with_operation_deadline(
        paths: &ClusterPaths,
        local_node_id: NodeId,
        operation_deadline: Instant,
    ) -> Self {
        Self {
            paths: paths.clone(),
            inner:
                sinomo_kv::ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
                    paths.kv_paths().raft_store(),
                    local_node_id,
                    sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
                    operation_deadline,
                ),
        }
    }

    pub(crate) fn new_read_index_with_immediate_operation_admission(
        paths: &ClusterPaths,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            paths: paths.clone(),
            inner: sinomo_kv::ClusterKvRaftLocalCommandStorage::new_read_index_with_immediate_operation_admission(
                paths.kv_paths().raft_store(),
                local_node_id,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
            ),
        }
    }

    pub(crate) fn try_new_observed(
        paths: &ClusterPaths,
        local_node_id: NodeId,
    ) -> Result<Self, NetworkError> {
        let capacity = sinomo_kv::try_admit_committed_observation(
            &paths.kv_paths(),
            sinomo_kv::KvObservationProposalOwnership::Required,
        )
        .map_err(kv_err)?;
        let mut storage = Self::new(paths, local_node_id);
        storage.inner = storage.inner.with_observation_capacity(capacity);
        Ok(storage)
    }

    pub(crate) fn try_new_observed_with_operation_deadline(
        paths: &ClusterPaths,
        local_node_id: NodeId,
        deadline: Instant,
    ) -> Result<Self, NetworkError> {
        let capacity = sinomo_kv::try_admit_committed_observation(
            &paths.kv_paths(),
            sinomo_kv::KvObservationProposalOwnership::Required,
        )
        .map_err(kv_err)?;
        let mut storage = Self::new_with_operation_deadline(paths, local_node_id, deadline);
        storage.inner = storage.inner.with_observation_capacity(capacity);
        Ok(storage)
    }

    pub(crate) fn proposal_custody_handle(
        &self,
    ) -> Option<sinomo_kv::KvObservationProposalCustodyHandle> {
        self.inner.proposal_custody_handle()
    }

    pub(crate) fn release_session_after_admitted_apply(&mut self) {
        self.inner.release_session_after_admitted_apply();
    }

    pub(crate) fn observation_proposal_fence(
        &self,
    ) -> Result<Option<sinomo_kv::KvObservationOwnerFence>, NetworkError> {
        self.inner.observation_proposal_fence().map_err(kv_err)
    }

    fn observe_last_snapshot_publish(
        &mut self,
        snapshot: &KvSnapshot,
        observation_receipt: Option<&sinomo_kv::KvCommittedObservationReceipt>,
    ) {
        if let Some(wrote) = self.inner.take_last_snapshot_publish_wrote() {
            let delivery_prepared =
                observation_receipt.is_some_and(|receipt| receipt.was_delivery_prepared());
            observe_cluster_kv_snapshot_write_with_invalidation(
                &self.paths,
                wrote,
                Some(snapshot),
                !delivery_prepared,
            );
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

    pub(crate) fn try_admit_read_index_state(
        &mut self,
    ) -> Result<LocalClusterRaftReadIndexAdmission, NetworkError> {
        match self.inner.read_index_state() {
            Ok(state) => Ok(LocalClusterRaftReadIndexAdmission::Admitted(state)),
            Err(sinomo_kv::KvError::Busy { .. }) => Ok(LocalClusterRaftReadIndexAdmission::Busy),
            Err(error) => Err(kv_err(error)),
        }
    }

    pub(crate) fn pending_current_term_barrier(
        &mut self,
    ) -> Result<Option<KvLogEntry>, NetworkError> {
        self.inner.pending_current_term_barrier().map_err(kv_err)
    }

    pub(crate) fn read_index_snapshot(&mut self) -> Result<KvSnapshot, NetworkError> {
        self.inner.read_index_snapshot().map_err(kv_err)
    }

    pub(crate) fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, NetworkError> {
        self.inner.read_index_entry(key).map_err(kv_err)
    }

    pub(crate) fn read_index_entries(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, NetworkError> {
        self.inner.read_index_entries(keys).map_err(kv_err)
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

    pub(crate) fn release_session_for_quorum(&mut self) {
        self.inner.release_session_for_quorum();
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

    pub(crate) fn build_raft_read_index_heartbeat(
        &mut self,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        self.inner
            .build_cluster_raft_read_index_heartbeat(state, commit_index)
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

    pub(crate) fn adopt_group_prepared_entry(&mut self, entry: KvLogEntry) {
        self.inner.adopt_group_prepared_entry(entry);
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

    pub(crate) fn discard_uncommitted_tail_and_clear_leader_compact(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.inner
            .discard_uncommitted_tail_and_clear_leader_compact()
            .map_err(kv_err)
    }

    pub(crate) fn step_down_preserving_pending_entry(&mut self) -> Result<(), NetworkError> {
        self.inner
            .step_down_preserving_pending_entry()
            .map_err(kv_err)
    }

    #[cfg(test)]
    pub(crate) fn observe_term(&mut self, term: u64) -> Result<(), NetworkError> {
        self.inner.observe_term(term).map_err(kv_err)
    }

    pub(crate) fn publish_pending_higher_term(
        &self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        crate::raft_quorum::publish_cluster_raft_higher_term_reactively(
            &self.paths,
            &state.local_node_id,
            term,
        )
    }

    pub(crate) fn observe_pending_higher_term(&mut self, term: u64) -> Result<(), NetworkError> {
        self.inner.observe_pending_higher_term(term).map_err(kv_err)
    }

    pub(crate) fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        let result = self.inner.commit_index_with_events(index).map_err(kv_err)?;
        self.observe_last_snapshot_publish(&result.snapshot, result.observation_receipt.as_ref());
        Ok(result)
    }

    pub(crate) fn commit_acknowledged_prefix_compact(
        &mut self,
        index: u64,
        acknowledged_index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        self.inner
            .commit_acknowledged_prefix_compact(index, acknowledged_index)
            .map_err(kv_err)
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
        let snapshot_write =
            if let Some(snapshot) = commit.and_then(|commit| commit.checkpoint_snapshot.as_ref()) {
                // The quorum driver releases the resident node, Raft operation
                // guard, and proposal sequence before collecting observations.
                // Keep this O(total-state) materialized write on that post-commit
                // side of the boundary. Its internal generation comparison makes
                // reordered publications monotonic.
                let wrote = self
                    .paths
                    .kv_paths()
                    .write_raft_snapshot_if_changed(snapshot)
                    .map_err(kv_err)?;
                Some((wrote, snapshot.clone()))
            } else if snapshot_publish_wrote == Some(true) {
                Some((
                    true,
                    self.paths
                        .kv_paths()
                        .local_cache()
                        .cached_snapshot_arc()
                        .map_err(kv_err)?,
                ))
            } else {
                None
            };
        let delta = commit.map(|commit| ClusterKvCommittedDelta {
            revision: commit.read_state.revision,
            compacted_revision: commit.compacted_revision,
            events: (commit.events.clone()).into(),
            final_key_states: (commit.final_key_states.clone()).into(),
        });
        let watcher_events = commit
            .map(|commit| commit.events.clone())
            .or_else(|| snapshot_write.as_ref().map(|_| Vec::new().into()));
        Ok(LocalClusterRaftPostCommitObservations {
            paths: self.paths.clone(),
            snapshot_write,
            snapshot_applied_index: commit.and_then(|commit| {
                commit
                    .checkpoint_snapshot
                    .as_ref()
                    .map(|_| commit.read_state.applied_index)
            }),
            delta,
            observation_receipt: commit.and_then(|commit| commit.observation_receipt.clone()),
            watcher_events,
        })
    }
}

impl LocalClusterRaftReadProjection for LocalClusterRaftCommandStorage {
    fn read_index_state(&mut self) -> Result<sinomo_kv::ClusterKvRaftReadIndexState, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_state(self)
    }

    fn read_index_snapshot(&mut self) -> Result<KvSnapshot, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_snapshot(self)
    }

    fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_entry(self, key)
    }

    fn pending_current_term_barrier(&mut self) -> Result<Option<KvLogEntry>, NetworkError> {
        LocalClusterRaftCommandStorage::pending_current_term_barrier(self)
    }

    fn build_raft_read_index_heartbeat(
        &mut self,
        _node_id: &NodeId,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        LocalClusterRaftCommandStorage::build_raft_read_index_heartbeat(self, state, commit_index)
    }

    fn read_index_entries(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_entries(self, keys)
    }

    fn read_index_request_outcome(
        &mut self,
        request_id: &sinomo_kv::KvRequestId,
    ) -> Result<Option<sinomo_kv::KvRequestOutcome>, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_request_outcome(self, request_id)
    }

    fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, NetworkError> {
        LocalClusterRaftCommandStorage::read_index_prefix_limited_by_payload_bytes(
            self,
            prefix,
            max_entries,
            max_payload_bytes,
        )
    }
}

impl LocalClusterRaftReadProjection for sinomo_kv::ClusterKvRaftResidentReadProjection<'_> {
    fn read_index_state(&mut self) -> Result<sinomo_kv::ClusterKvRaftReadIndexState, NetworkError> {
        Ok(sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_state(self))
    }

    fn read_index_snapshot(&mut self) -> Result<KvSnapshot, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_snapshot(self).map_err(kv_err)
    }

    fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_entry(self, key).map_err(kv_err)
    }

    fn pending_current_term_barrier(&mut self) -> Result<Option<KvLogEntry>, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::pending_current_term_barrier(self)
            .map_err(kv_err)
    }

    fn build_raft_read_index_heartbeat(
        &mut self,
        node_id: &NodeId,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::build_read_index_heartbeat(
            self,
            node_id,
            state,
            commit_index,
        )
        .map_err(kv_err)
    }

    fn read_index_entries(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_entries(self, keys)
            .map_err(kv_err)
    }

    fn read_index_request_outcome(
        &mut self,
        request_id: &sinomo_kv::KvRequestId,
    ) -> Result<Option<sinomo_kv::KvRequestOutcome>, NetworkError> {
        Ok(
            sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_request_outcome(
                self, request_id,
            ),
        )
    }

    fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, NetworkError> {
        sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_prefix_limited_by_payload_bytes(
            self,
            prefix,
            max_entries,
            max_payload_bytes,
        )
        .map_err(kv_err)
    }
}

impl sinomo_kv::ClusterKvRaftCommandStorage for LocalClusterRaftCommandStorage {
    type Error = NetworkError;

    fn is_committed_observation_delivery_failure(error: &Self::Error) -> bool {
        error.delivery_state() == Some(sinomo_network::NetworkDeliveryState::PossiblyApplied)
    }

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

    fn release_cluster_raft_command_session_for_quorum(&mut self) {
        self.release_session_for_quorum();
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

    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), NetworkError> {
        self.step_down_preserving_pending_entry()
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        // Quorum rejection has already raised the process-local higher-term
        // floor. Use the sole exclusive closer so the synchronous commit path
        // cannot return before that observation reaches durable hard state.
        self.observe_pending_higher_term(term)
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

    fn observation_proposal_fence(
        &mut self,
    ) -> Result<Option<sinomo_kv::KvObservationOwnerFence>, Self::Error> {
        LocalClusterRaftCommandStorage::observation_proposal_fence(self)
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), NetworkError> {
        self.take_post_commit_observations(commit)?.publish()?;
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
        error @ sinomo_kv::KvError::CommittedObservationDeliveryFailed { .. } => {
            NetworkError::outcome_unknown(
                "deliver committed KV observation",
                None,
                error.to_string(),
            )
        }
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

    #[test]
    fn canonical_checkpoint_delivers_actual_prefix_delta_then_checkpoint_without_gap() {
        use sinomo_kv::{KvCommittedObservationKind, KvCommittedObservationValue};
        let root = sinomo_testdir::TestDir::new("canonical-checkpoint-observation-prefix");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("canonical-checkpoint-local").unwrap();
        let native = sinomo_kv::ClusterKvRaftStore::new(
            paths.kv_paths(),
            sinomo_kv::ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: 0,
                max_snapshot_events: 0,
            },
        );
        native.set_leader_compact(&local, 1).unwrap();
        native.open_node(&local).unwrap().save_snapshot().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let _subscription =
            crate::subscribe_cluster_kv_committed_observations(&paths, move |event| {
                captured.lock().unwrap().push(event.clone())
            });
        let capacity = sinomo_kv::try_admit_committed_observation(
            &paths.kv_paths(),
            sinomo_kv::KvObservationProposalOwnership::Required,
        )
        .unwrap();
        let mut storage = LocalClusterRaftCommandStorage {
            paths: paths.clone(),
            inner: sinomo_kv::ClusterKvRaftLocalCommandStorage::new(
                native,
                local,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
            )
            .with_observation_capacity(capacity),
        };
        let proposal = sinomo_kv::ClusterKvRaftCommitSequenceGuard::new(
            sinomo_kv::lock_cluster_kv_commit_sequence(
                &paths.kv_paths(),
                sinomo_network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap(),
            storage.observation_proposal_fence().unwrap(),
        );
        let entry = storage
            .inner
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                key: "/checkpoint/actual-prefix".into(),
                value: vec![1],
                lease_id: None,
            })))
            .unwrap();
        storage.release_session_for_quorum();
        let commit = storage.commit_index_compact(entry.index).unwrap();
        assert!(commit.checkpoint_snapshot.is_some());
        let actual_index = commit.read_state.applied_index;
        storage.release_session_after_commit();
        drop(proposal);
        let publication = storage
            .take_post_commit_observations(Some(&commit))
            .unwrap();
        assert_eq!(publication.snapshot_applied_index, Some(actual_index));
        publication.publish().unwrap();
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Delta(_)
        ));
        assert!(matches!(
            events[1].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
        assert_eq!(events[1].kind, KvCommittedObservationKind::Checkpoint);
        assert_eq!(events[0].revision, Some(commit.read_state.revision));
        assert_eq!(events[1].revision, Some(commit.read_state.revision));
        assert_eq!(events[0].applied_index, Some(actual_index));
        assert_eq!(events[1].applied_index, Some(actual_index));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.value, KvCommittedObservationValue::Discontinuity(_)))
        );
    }

    #[test]
    fn denied_local_raft_latest_observed_reads_materialized_projection_only() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-shadow-kv-projection-only");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let kv_paths = paths.kv_paths();
        let snapshot = InProcessKvStore::new()
            .try_snapshot()
            .expect("empty materialized snapshot");
        kv_paths
            .write_raft_snapshot_if_changed(&snapshot)
            .expect("write materialized projection");
        assert!(
            !kv_paths.raft.exists(),
            "projection write must not create Raft storage"
        );

        sinomo_kv::deny_process_local_raft_access_for_root(kv_paths.raft.clone())
            .expect("install shadow role denial");
        let observed = read_cluster_kv_latest_observed_snapshot_arc_if_present(&paths)
            .expect("projection-only latest-observed read")
            .expect("materialized projection");
        assert_eq!(observed.as_ref(), &snapshot);
        assert!(
            !kv_paths.raft.exists(),
            "projection-only shadow read must not create or open a local Raft root"
        );
        assert!(sinomo_kv::clear_process_local_raft_access_denial_for_test(
            &kv_paths.raft
        ));
    }

    #[test]
    fn async_authority_lookup_drops_expired_pool_and_operation_waiters() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-kv-authority-deadline");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let node_id =
            NodeId::from_legacy_named("node_kv_authority_deadline").expect("local node id");
        let state = ClusterBootstrapState::initialized_locally(
            sinomo_node::ClusterId::from_legacy_named("cluster_kv_authority_deadline")
                .expect("cluster id"),
            "KV authority deadline",
            node_id,
            "local",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        let kv_paths = paths.kv_paths();
        let operation_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&kv_paths)
            .expect("hold Raft operation lock");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("test runtime");

        // First prove a lookup still waiting in Tokio's blocking queue cannot
        // begin operation-lock admission after its caller has timed out.
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("report blocker start");
            release_blocker_rx.recv().expect("release blocking worker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking worker occupied");
        let queued_started = Instant::now();
        let queued_result = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(50),
                locked_kv_state_authority_id_async(
                    runtime.handle(),
                    &paths,
                    &state,
                    queued_started + Duration::from_secs(5),
                ),
            )
            .await
        });
        assert!(
            queued_result.is_err(),
            "the caller must be able to cancel a queued lookup"
        );
        assert!(
            queued_started.elapsed() < Duration::from_millis(500),
            "blocking-pool saturation must not extend the 50ms lookup into a multi-second wait"
        );
        assert_eq!(
            sinomo_kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
            0,
            "a lookup still queued in the blocking pool must not register an operation waiter"
        );
        release_blocker_tx.send(()).expect("release blocker");
        runtime.block_on(blocker).expect("blocking worker joined");
        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(
            sinomo_kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
            0,
            "the expired blocking task must not enter the operation queue when a worker frees"
        );

        // Then prove a lookup that did start and reached the operation lock
        // removes its timed waiter instead of acquiring after the guard drops.
        let waiting_paths = paths.clone();
        let waiting_state = state.clone();
        let waiting_runtime = runtime.handle().clone();
        let waiting = runtime.spawn(async move {
            locked_kv_state_authority_id_async(
                &waiting_runtime,
                &waiting_paths,
                &waiting_state,
                Instant::now() + Duration::from_millis(50),
            )
            .await
        });
        let queue_deadline = Instant::now() + Duration::from_secs(1);
        while sinomo_kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths) != 1 {
            assert!(
                Instant::now() < queue_deadline,
                "authority lookup did not enter the operation queue"
            );
            std::thread::yield_now();
        }
        assert!(
            runtime
                .block_on(waiting)
                .expect("authority lookup task joined")
                .is_err(),
            "operation-lock lookup must expire"
        );
        assert_eq!(
            sinomo_kv::cluster_kv_raft_operation_queue_depth_for_test(&kv_paths),
            0,
            "expired operation waiter must be removed before the caller resumes"
        );

        drop(operation_guard);
        let fresh_started = Instant::now();
        let fresh = runtime.block_on(locked_kv_state_authority_id_async(
            runtime.handle(),
            &paths,
            &state,
            Instant::now() + Duration::from_millis(500),
        ));
        assert!(fresh.is_ok(), "fresh lookup after release must succeed");
        assert!(
            fresh_started.elapsed() < Duration::from_millis(250),
            "fresh lookup was delayed by expired authority work"
        );
    }

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
    fn grouped_commit_observation_uses_applied_revision_not_request_revision() {
        let root = sinomo_testdir::TestDir::new("grouped-observation-revision");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        let local = NodeId::from_legacy_named("grouped_observation_local").unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            sinomo_base::Id::from_legacy_named("grouped_observation_cluster").unwrap(),
            "group",
            local.clone(),
            "local",
            "pk",
            OffsetDateTime::now_utc(),
        );
        state.term = 7;
        state.members[0].capabilities = vec![
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap(),
        ];
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        paths
            .kv_paths()
            .raft_store()
            .open_node(&local)
            .unwrap()
            .become_leader(7)
            .unwrap();
        let proposals: Vec<_> = (0..2)
            .map(|i| {
                sinomo_kv::ClusterKvRaftProposal::new(
                    state.clone(),
                    KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                        key: format!("/delta/{i}"),
                        value: vec![1],
                        lease_id: None,
                    })),
                    Instant::now() + Duration::from_secs(5),
                )
            })
            .collect();
        let prepared =
            sinomo_kv::prepare_cluster_kv_raft_proposals(&paths.kv_paths(), &local, &proposals);
        let sinomo_kv::ClusterKvRaftPreparedProposal::Appended(first) =
            prepared[0].clone().unwrap()
        else {
            panic!("new request");
        };
        let mut storage = LocalClusterRaftCommandStorage::new(&paths, local);
        storage.adopt_group_prepared_entry(first);
        let commit = storage.commit_acknowledged_prefix_compact(1, 2).unwrap();
        assert_eq!(commit.committed_request.revision, KvRevision(1));
        storage.release_session_after_commit();
        let delta = storage
            .take_post_commit_observations(Some(&commit))
            .unwrap()
            .delta
            .unwrap();
        assert_eq!(delta.revision, KvRevision(2));
        assert_eq!(delta.events.last().unwrap().revision, delta.revision);
        assert_eq!(delta.final_key_states.len(), 2);
    }

    #[test]
    fn real_legacy_commits_publish_in_effect_order_and_release_native_owners_before_callbacks() {
        use std::sync::{Condvar, mpsc};
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-ordered-native-publications");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        paths.ensure().unwrap();
        let node = NodeId::from_legacy_named("node_ordered_native_publications").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let callback_seen = Arc::clone(&seen);
        let callback_paths = paths.clone();
        let callback_node = node.clone();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let callback_release = Arc::clone(&released);
        let (entered, observer_entered) = mpsc::sync_channel(1);
        let _subscription =
            crate::subscribe_cluster_kv_committed_observations(&paths, move |observation| {
                if let sinomo_kv::KvCommittedObservationValue::Delta(delta) = &observation.value {
                    let sequence = sinomo_kv::lock_cluster_kv_commit_sequence(
                        &callback_paths.kv_paths(),
                        Duration::from_millis(100),
                    )
                    .expect("callbacks must not retain proposal ownership");
                    drop(sequence);
                    let read = sinomo_kv::read_cluster_kv_resident_configuration_floor(
                        &callback_paths.kv_paths(),
                        &callback_node,
                    )
                    .expect("resident native reads remain usable while publishing");
                    assert!(read.revision >= delta.revision);
                    if delta.revision == KvRevision(1) {
                        entered.send(()).unwrap();
                        let (lock, wake) = &*callback_release;
                        let mut ready = lock.lock().unwrap();
                        while !*ready {
                            ready = wake.wait(ready).unwrap();
                        }
                    }
                    callback_seen.lock().unwrap().push(delta.revision);
                }
            });
        let first_store = paths.kv_paths().raft_store();
        let first_node = node.clone();
        let (first_done, first_completed) = mpsc::channel();
        let first = std::thread::spawn(move || {
            first_store
                .append_command_with_response_and_publish_report(
                    &first_node,
                    KvCommand::Op(KvOp::Put {
                        key: "/ordered/first".into(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                    sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
                )
                .unwrap();
            first_done.send(()).unwrap();
        });
        observer_entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let second_store = paths.kv_paths().raft_store();
        let second_node = node.clone();
        let (persisted, second_persisted) = mpsc::channel();
        let (done, completed) = mpsc::channel();
        let later = std::thread::spawn(move || {
            let second = second_store
                .append_command_with_response(
                    &second_node,
                    KvCommand::Op(KvOp::Put {
                        key: "/ordered/second".into(),
                        value: b"two".to_vec(),
                        lease_id: None,
                    }),
                )
                .unwrap();
            persisted.send(second.snapshot.revision).unwrap();
            second_store
                .publish_commit_result(
                    &second,
                    sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
                )
                .unwrap();
            done.send(()).unwrap();
        });
        assert_eq!(
            second_persisted
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            KvRevision(2)
        );
        let floor =
            sinomo_kv::read_cluster_kv_resident_configuration_floor(&paths.kv_paths(), &node)
                .unwrap();
        assert_eq!(
            floor.revision,
            KvRevision(2),
            "native floor remains live while the first callback blocks"
        );
        let current = open_cluster_raft_kv_node(&paths, &node).unwrap();
        assert_eq!(current.store().revision(), KvRevision(2));
        drop(current);
        assert!(
            completed.recv_timeout(Duration::from_millis(20)).is_err(),
            "later reply must await its own callbacks"
        );
        assert!(first_completed.try_recv().is_err());
        assert!(seen.lock().unwrap().is_empty());
        let (lock, wake) = &*released;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        first_completed
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        completed.recv_timeout(Duration::from_secs(1)).unwrap();
        first.join().unwrap();
        later.join().unwrap();
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[KvRevision(1), KvRevision(2)]
        );
    }

    #[test]
    fn follower_checkpoint_publishes_complete_delta_before_snapshot_and_restore_invalidates_first()
    {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-follower-checkpoint-observers");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let node_id = NodeId::from_legacy_named("node_follower_checkpoint").expect("node");
        let key = "/dfs/filesystems/checkpoint/policy";
        let count = sinomo_kv::RAFT_COMPACTION_RETAINED_ENTRIES * 2 + 1;
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let covered = Arc::new(Mutex::new(None::<(KvRevision, KvEntry)>));
        let delta_notifications = Arc::clone(&notifications);
        let delta_covered = Arc::clone(&covered);
        let _delta_subscription =
            crate::subscribe_cluster_kv_committed_deltas(&paths, move |delta| {
                assert_eq!(delta.revision, KvRevision(count));
                assert_eq!(delta.events.len(), count as usize);
                assert_eq!(delta.events[0].revision, KvRevision(1));
                assert!(
                    delta
                        .events
                        .windows(2)
                        .all(|events| { events[0].revision.next() == events[1].revision })
                );
                assert_eq!(delta.events.last().unwrap().revision, delta.revision);
                assert_eq!(delta.final_key_states.len(), 1);
                let entry = delta.final_key_states[0]
                    .entry
                    .clone()
                    .expect("final live key");
                assert_eq!(entry.key, key);
                assert_eq!(entry.modify_revision, delta.revision);
                *delta_covered.lock().unwrap() = Some((delta.revision, entry));
                delta_notifications.lock().unwrap().push("delta");
            });
        let snapshot_notifications = Arc::clone(&notifications);
        let snapshot_covered = Arc::clone(&covered);
        let _snapshot_subscription =
            crate::subscribe_cluster_kv_committed_snapshots(&paths, move |snapshot| {
                let covered = snapshot_covered.lock().unwrap();
                let (revision, entry) = covered.as_ref().expect(
                    "a normal checkpoint must not withdraw readiness before its complete delta",
                );
                assert_eq!(*revision, snapshot.revision);
                assert_eq!(snapshot.get(key), Some(entry));
                snapshot_notifications.lock().unwrap().push("snapshot");
            });
        let entries = (1..=count)
            .map(|index| {
                let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                    key: key.to_string(),
                    value: index.to_le_bytes().to_vec(),
                    lease_id: None,
                }));
                KvLogEntry {
                    index,
                    term: 1,
                    request_id: request.request_id,
                    command: request.command,
                    revision: KvRevision(index),
                }
            })
            .collect();
        let response = apply_cluster_raft_append(
            &paths,
            &node_id,
            None,
            RaftAppendEntriesRequest {
                term: 1,
                leader_id: node_id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: count,
            },
        )
        .expect("apply follower batch across real checkpoint threshold");
        assert!(response.success);
        assert_eq!(
            notifications.lock().unwrap().as_slice(),
            &["delta", "snapshot"]
        );

        let snapshot = open_cluster_raft_kv_node(&paths, &node_id)
            .expect("open checkpointed node")
            .save_snapshot()
            .expect("save real leader snapshot");
        let restore_root =
            sinomo_testdir::TestDir::new("sinomo-cluster-follower-restored-observers");
        let restore_paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&restore_root));
        restore_paths.ensure().expect("restore paths");
        let restore_node =
            NodeId::from_legacy_named("node_follower_restore").expect("restore node");
        let restored = Arc::new(Mutex::new(Vec::new()));
        let legacy_deltas = Arc::new(Mutex::new(0usize));
        let legacy_delta_count = Arc::clone(&legacy_deltas);
        let _restore_delta_subscription =
            crate::subscribe_cluster_kv_committed_deltas(&restore_paths, move |_| {
                *legacy_delta_count.lock().unwrap() += 1;
            });
        let native_phases = Arc::new(Mutex::new(Vec::new()));
        let captured_phases = Arc::clone(&native_phases);
        let _native_subscription = crate::subscribe_cluster_kv_committed_observations(
            &restore_paths,
            move |observation| {
                assert_eq!(observation.revision, Some(KvRevision(count)));
                assert_eq!(observation.applied_index, Some(count));
                match &observation.value {
                    sinomo_kv::KvCommittedObservationValue::Snapshot(snapshot) => {
                        assert_eq!(
                            observation.kind,
                            sinomo_kv::KvCommittedObservationKind::Replacement
                        );
                        assert_eq!(snapshot.revision, KvRevision(count));
                        captured_phases.lock().unwrap().push("snapshot");
                    }
                    sinomo_kv::KvCommittedObservationValue::Delta(_) => {
                        panic!(
                            "pure restore has no post-install applied-index progress or state delta"
                        );
                    }
                    sinomo_kv::KvCommittedObservationValue::Discontinuity(_) => {
                        panic!(
                            "a successful fresh native restore supplies complete capture, not a gap"
                        );
                    }
                }
            },
        );
        let restored_snapshot = Arc::clone(&restored);
        let _restore_snapshot_subscription =
            crate::subscribe_cluster_kv_committed_snapshots(&restore_paths, move |snapshot| {
                assert_eq!(snapshot.revision, KvRevision(count));
                restored_snapshot.lock().unwrap().push("snapshot");
            });
        let append = RaftAppendEntriesRequest {
            term: 1,
            leader_id: restore_node.clone(),
            prev_log_index: snapshot.last_included_index,
            prev_log_term: snapshot.last_included_term,
            entries: Vec::new(),
            leader_commit: snapshot.last_included_index,
        };
        assert!(
            apply_cluster_raft_append(&restore_paths, &restore_node, Some(snapshot), append)
                .expect("install replacement snapshot")
                .success
        );
        assert_eq!(restored.lock().unwrap().as_slice(), &["snapshot"]);
        assert_eq!(
            *legacy_deltas.lock().unwrap(),
            0,
            "empty applied-prefix progress stays out of legacy state deltas"
        );
        assert_eq!(native_phases.lock().unwrap().as_slice(), &["snapshot"]);
    }

    #[test]
    fn local_append_publishes_exact_committed_delta_to_retained_observer() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-local-append-delta");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let node_id = NodeId::from_legacy_named("node_local_append_delta").expect("node");
        let live_key = "/dfs/filesystems/test/live";
        let deleted_key = "/dfs/filesystems/test/deleted";

        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Txn(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: live_key.to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: deleted_key.to_string(),
                        value: b"remove-me".to_vec(),
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            }),
        )
        .expect("seed local values");

        let observed = Arc::new(Mutex::new(Vec::<ClusterKvCommittedDelta>::new()));
        let observed_for_callback = Arc::clone(&observed);
        let _subscription = crate::subscribe_cluster_kv_committed_deltas(&paths, move |delta| {
            observed_for_callback
                .lock()
                .expect("observed delta lock")
                .push(delta.clone());
        });

        let snapshot = append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Txn(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: live_key.to_string(),
                        value: b"intermediate".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: live_key.to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Delete {
                        key: deleted_key.to_string(),
                    },
                ],
                failure: Vec::new(),
            }),
        )
        .expect("commit observed local mutation");

        {
            let observed = observed.lock().expect("observed deltas");
            assert_eq!(observed.len(), 1, "local append must notify immediately");
            let delta = &observed[0];
            assert_eq!(delta.revision, snapshot.revision);
            assert_eq!(delta.compacted_revision, snapshot.compacted_revision);
            assert_eq!(delta.events.len(), 3);
            assert_eq!(
                delta.events[0].revision.next(),
                delta.events[1].revision,
                "multi-op local transactions must retain their contiguous event revisions"
            );
            assert_eq!(delta.events[1].revision.next(), delta.events[2].revision);
            assert_eq!(delta.events[2].revision, snapshot.revision);
            assert_eq!(
                delta
                    .final_key_states
                    .iter()
                    .map(|state| state.key.as_str())
                    .collect::<Vec<_>>(),
                vec![deleted_key, live_key],
                "final states must be deduplicated and deterministically ordered"
            );
            assert!(delta.final_key_states[0].entry.is_none());
            let live = delta.final_key_states[1]
                .entry
                .as_ref()
                .expect("updated live entry");
            assert_eq!(live.value, b"new");
            assert_eq!(
                live.modify_revision,
                delta
                    .events
                    .iter()
                    .rfind(|event| event.key == live_key)
                    .expect("live-key event")
                    .revision
            );
        }

        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Delete {
                key: "/dfs/filesystems/test/missing".to_string(),
            }),
        )
        .expect("commit no-effect local mutation");
        assert_eq!(
            observed.lock().expect("observed deltas").len(),
            1,
            "a committed request with no events must not publish a false delta"
        );
        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Put {
                key: live_key.to_string(),
                value: b"next".to_vec(),
                lease_id: None,
            }),
        )
        .expect("commit real mutation after the no-event publication");
        let observed = observed.lock().expect("observed deltas");
        assert_eq!(observed.len(), 2, "no-event delivery must not strand FIFO");
        assert_eq!(observed[1].events.len(), 1);
        assert_eq!(observed[1].events[0].key, live_key);
    }

    #[test]
    fn resident_current_snapshot_reuses_one_revision_projection() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-resident-snapshot-reuse");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let node_id = NodeId::from_legacy_named("node_resident_snapshot_reuse").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            sinomo_node::ClusterId::from_legacy_named("cluster_resident_snapshot_reuse")
                .expect("cluster"),
            "resident snapshot reuse",
            node_id.clone(),
            "local",
            "public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        crate::local_state_store::write_cluster_state(&paths, &state).expect("cluster state");

        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Put {
                key: "/cluster/services/reuse".to_string(),
                value: b"value".to_vec(),
                lease_id: None,
            }),
        )
        .expect("seed resident raft");

        let first = read_cluster_kv_current_snapshot_arc(&paths).expect("first snapshot");
        let second = read_cluster_kv_current_snapshot_arc(&paths).expect("cached snapshot");

        assert_eq!(first.revision, second.revision);
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged resident revisions should not clone and canonicalize the complete KV snapshot"
        );

        append_cluster_raft_command(
            &paths,
            &node_id,
            KvCommand::Op(KvOp::Put {
                key: "/cluster/services/reuse".to_string(),
                value: b"new-value".to_vec(),
                lease_id: None,
            }),
        )
        .expect("advance resident raft");
        let advanced = read_cluster_kv_current_snapshot_arc(&paths).expect("advanced snapshot");
        assert!(advanced.revision > second.revision);
        assert!(!Arc::ptr_eq(&advanced, &second));
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
            snapshot_write: Some((true, Arc::new(snapshot.clone()))),
            snapshot_applied_index: None,
            delta: Some(ClusterKvCommittedDelta {
                revision: KvRevision(1),
                compacted_revision: KvRevision::ZERO,
                events: (vec![test_event("/cluster/members/node-a")]).into(),
                final_key_states: (Vec::new()).into(),
            }),
            observation_receipt: None,
            watcher_events: None,
        }
        .publish()
        .expect("compatibility fallback publication");
        assert_eq!(observed_read_model_invalidation_counts(&paths), (1, 1));

        reset_observed_read_model_invalidation_counts(&paths);
        LocalClusterRaftPostCommitObservations {
            paths: paths.clone(),
            snapshot_write: Some((true, Arc::new(snapshot))),
            snapshot_applied_index: None,
            delta: Some(ClusterKvCommittedDelta {
                revision: KvRevision(2),
                compacted_revision: KvRevision::ZERO,
                events: (vec![test_event("/cluster/services/catalog")]).into(),
                final_key_states: (Vec::new()).into(),
            }),
            observation_receipt: None,
            watcher_events: None,
        }
        .publish()
        .expect("compatibility fallback publication");
        assert_eq!(
            observed_read_model_invalidation_counts(&paths),
            (0, 0),
            "an unrelated compact delta must not invalidate caches merely because its critical snapshot was published"
        );
    }

    fn native_invalidation_test_fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        NodeId,
        ClusterBootstrapState,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named(format!("{name}-local")).unwrap();
        let state = ClusterBootstrapState::initialized_locally(
            sinomo_base::Id::from_legacy_named(format!("{name}-cluster")).unwrap(),
            name,
            local.clone(),
            "local",
            "pk",
            OffsetDateTime::now_utc(),
        );
        crate::write_cluster_state(&paths, &state).unwrap();
        paths
            .kv_paths()
            .raft_store()
            .set_leader_compact(&local, 1)
            .unwrap();
        (root, paths, local, state)
    }

    #[test]
    fn canonical_subscriber_preparation_keeps_rebuilt_cache_after_publication() {
        let (_root, paths, local, state) =
            native_invalidation_test_fixture("canonical-prepared-cache-retained");
        let cached_before =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        reset_observed_read_model_invalidation_counts(&paths);
        let rebuilt = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&rebuilt);
        let callback_paths = paths.clone();
        let _subscription =
            crate::subscribe_cluster_kv_committed_observations(&paths, move |event| {
                if matches!(
                    event.value,
                    sinomo_kv::KvCommittedObservationValue::Delta(_)
                ) {
                    assert_eq!(
                        observed_read_model_invalidation_counts(&callback_paths),
                        (1, 1),
                        "the bridge invalidates once BEFORE subscriber entry"
                    );
                    *captured.lock().unwrap() = Some(
                        crate::local_state_store::read_cluster_state_without_kv_cached_shared(
                            &callback_paths,
                        )
                        .unwrap(),
                    );
                }
            });
        let mut storage =
            LocalClusterRaftCommandStorage::try_new_observed(&paths, local.clone()).unwrap();
        let proposal = sinomo_kv::ClusterKvRaftCommitSequenceGuard::new(
            sinomo_kv::lock_cluster_kv_commit_sequence(
                &paths.kv_paths(),
                sinomo_network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap(),
            storage.observation_proposal_fence().unwrap(),
        );
        let request = KvCommandRequest::generated(KvCommand::Op(
            KvOp::put_serde(
                sinomo_node::cluster_member_key(&local),
                &state.members[0],
                None,
            )
            .unwrap(),
        ));
        let entry = storage.inner.append_uncommitted(request).unwrap();
        storage.release_session_for_quorum();
        let commit = storage.commit_index_compact(entry.index).unwrap();
        storage.release_session_after_commit();
        drop(proposal);
        storage
            .take_post_commit_observations(Some(&commit))
            .unwrap()
            .publish()
            .unwrap();
        let rebuilt = rebuilt
            .lock()
            .unwrap()
            .clone()
            .expect("real subscriber rebuilt its invalidated cache");
        assert!(!Arc::ptr_eq(&cached_before, &rebuilt));
        let retained =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        assert!(
            Arc::ptr_eq(&rebuilt, &retained),
            "late postcommit bookkeeping must retain the cache rebuilt by this publication's callback"
        );
        assert_eq!(observed_read_model_invalidation_counts(&paths), (1, 1));
    }

    #[test]
    fn legacy_native_subscriber_preparation_keeps_rebuilt_cache_after_return() {
        let (_root, paths, local, state) =
            native_invalidation_test_fixture("legacy-prepared-cache-retained");
        let cached_before =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        reset_observed_read_model_invalidation_counts(&paths);
        let rebuilt = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&rebuilt);
        let callback_paths = paths.clone();
        let _subscription =
            crate::subscribe_cluster_kv_committed_observations(&paths, move |event| {
                if matches!(
                    event.value,
                    sinomo_kv::KvCommittedObservationValue::Delta(_)
                ) {
                    assert_eq!(
                        observed_read_model_invalidation_counts(&callback_paths),
                        (1, 1)
                    );
                    *captured.lock().unwrap() = Some(
                        crate::local_state_store::read_cluster_state_without_kv_cached_shared(
                            &callback_paths,
                        )
                        .unwrap(),
                    );
                }
            });
        append_cluster_raft_command_with_response(
            &paths,
            &local,
            KvCommand::Op(
                KvOp::put_serde(
                    sinomo_node::cluster_member_key(&local),
                    &state.members[0],
                    None,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let rebuilt = rebuilt.lock().unwrap().clone().unwrap();
        assert!(!Arc::ptr_eq(&cached_before, &rebuilt));
        let retained =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        assert!(Arc::ptr_eq(&rebuilt, &retained));
        assert_eq!(
            observed_read_model_invalidation_counts(&paths),
            (1, 1),
            "legacy helper must use its own completed receipt evidence"
        );
    }

    #[test]
    fn native_publication_without_preparer_preserves_compatibility_invalidation() {
        let (_root, paths, local, state) =
            native_invalidation_test_fixture("unprepared-native-cache-fallback");
        let cached_before =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        reset_observed_read_model_invalidation_counts(&paths);
        append_cluster_raft_command_with_response(
            &paths,
            &local,
            KvCommand::Op(
                KvOp::put_serde(
                    sinomo_node::cluster_member_key(&local),
                    &state.members[0],
                    None,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(
            observed_read_model_invalidation_counts(&paths),
            (1, 1),
            "R0/no-subscriber paths still invalidate the affected cluster compatibility models"
        );
        let refreshed =
            crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths).unwrap();
        assert!(!Arc::ptr_eq(&cached_before, &refreshed));
    }

    #[test]
    fn raw_and_imported_subscriber_preparation_keeps_rebuilt_cache_after_return() {
        for imported in [false, true] {
            let (_root, paths, local, state) = native_invalidation_test_fixture(if imported {
                "imported-prepared-cache-retained"
            } else {
                "raw-prepared-cache-retained"
            });
            let cached_before =
                crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths)
                    .unwrap();
            let mut store = InProcessKvStore::new();
            store
                .put_serde(
                    sinomo_node::cluster_member_key(&local),
                    &state.members[0],
                    None,
                )
                .unwrap();
            let snapshot = store.try_snapshot().unwrap();
            reset_observed_read_model_invalidation_counts(&paths);
            let rebuilt = Arc::new(Mutex::new(None));
            let captured = Arc::clone(&rebuilt);
            let callback_paths = paths.clone();
            let _subscription =
                crate::subscribe_cluster_kv_committed_observations(&paths, move |event| {
                    if matches!(
                        event.value,
                        sinomo_kv::KvCommittedObservationValue::Snapshot(_)
                    ) {
                        assert_eq!(
                            observed_read_model_invalidation_counts(&callback_paths),
                            (1, 1),
                            "replacement preparation occurs before subscriber entry"
                        );
                        *captured.lock().unwrap() = Some(
                            crate::local_state_store::read_cluster_state_without_kv_cached_shared(
                                &callback_paths,
                            )
                            .unwrap(),
                        );
                    }
                });
            if imported {
                assert!(write_imported_cluster_kv_snapshot_if_changed(&paths, &snapshot).unwrap());
            } else {
                write_cluster_kv_snapshot(&paths, &snapshot).unwrap();
            }
            let rebuilt = rebuilt.lock().unwrap().clone().unwrap();
            assert!(!Arc::ptr_eq(&cached_before, &rebuilt));
            let retained =
                crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths)
                    .unwrap();
            assert!(
                Arc::ptr_eq(&rebuilt, &retained),
                "this writer's own preparation report suppresses only its late duplicate invalidation"
            );
            assert_eq!(observed_read_model_invalidation_counts(&paths), (1, 1));
        }
    }

    #[test]
    fn raw_and_imported_without_preparer_preserve_compatibility_invalidation() {
        for imported in [false, true] {
            let (_root, paths, local, state) = native_invalidation_test_fixture(if imported {
                "imported-unprepared-cache-fallback"
            } else {
                "raw-unprepared-cache-fallback"
            });
            let cached_before =
                crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths)
                    .unwrap();
            let mut store = InProcessKvStore::new();
            store
                .put_serde(
                    sinomo_node::cluster_member_key(&local),
                    &state.members[0],
                    None,
                )
                .unwrap();
            let snapshot = store.try_snapshot().unwrap();
            reset_observed_read_model_invalidation_counts(&paths);
            if imported {
                assert!(write_imported_cluster_kv_snapshot_if_changed(&paths, &snapshot).unwrap());
            } else {
                write_cluster_kv_snapshot(&paths, &snapshot).unwrap();
            }
            assert_eq!(observed_read_model_invalidation_counts(&paths), (1, 1));
            let refreshed =
                crate::local_state_store::read_cluster_state_without_kv_cached_shared(&paths)
                    .unwrap();
            assert!(!Arc::ptr_eq(&cached_before, &refreshed));
        }
    }
}
