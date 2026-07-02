use crate::connectivity_publish::cluster_connectivity_publish_record_matches;
use crate::keyspace::cluster_connectivity_key;
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
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvLogEntry, KvWalPaths};
use sinomo_kv::{
    ClusterKvRaftCommitResult, ClusterKvRaftUncommittedAppend, DurableRaftKvNode, InProcessKvStore,
    KvCommand, KvEntry, KvLocalCache, KvOp, KvRevision, KvSnapshot, KvTxnResponse,
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftRequestVoteRequest,
    RaftRequestVoteResponse, RaftSnapshot,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(any(test, feature = "test-support"))]
use std::time::SystemTime;
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
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(())
}

pub fn write_imported_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = if imported_snapshot_requires_route_read_model_materialization(paths, snapshot)? {
        sinomo_kv::write_cluster_kv_snapshot_if_changed(&paths.kv_paths(), snapshot)
            .map_err(kv_err)?
    } else {
        sinomo_kv::write_imported_cluster_kv_snapshot_if_changed(
            &paths.kv_paths(),
            snapshot,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?
    };
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

pub(crate) fn observe_cluster_kv_snapshot_write(paths: &ClusterPaths, wrote: bool) {
    if wrote {
        sinomo_network::invalidate_cluster_delivery_routing_table_cache(paths);
        sinomo_network::invalidate_cluster_route_graph_cache(paths);
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

pub(crate) fn read_cluster_kv_latest_observed_snapshot_arc_if_present(
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

pub(crate) fn read_cluster_kv_cached_snapshot(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_cached_snapshot_read_for_test(paths);
    cluster_kv_local_cache(paths)
        .cached_snapshot_arc()
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

#[cfg(any(test, feature = "test-support"))]
pub fn restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .restore_raft_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(kv_err)
}

pub(crate) fn locked_restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .locked_restore_raft_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(kv_err)
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
    observe_cluster_kv_snapshot_write(paths, wrote);
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
    observe_cluster_kv_snapshot_write(paths, wrote);
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

pub(crate) async fn append_cluster_raft_command_uncommitted_or_no_effect_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<ClusterKvRaftUncommittedAppend, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .append_uncommitted_or_no_effect(command)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft append uncommitted command".to_string(),
            detail: format!("cluster raft append uncommitted command task failed: {source}"),
        })?
}

pub async fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .discard_uncommitted_tail_and_clear_leader()
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft discard uncommitted tail".to_string(),
            detail: format!("cluster raft discard uncommitted tail task failed: {source}"),
        })?
}

pub async fn commit_cluster_raft_index_with_events_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
    commit_index: u64,
) -> Result<ClusterKvRaftCommitResult, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .commit_index_with_events(commit_index)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft commit index".to_string(),
            detail: format!("cluster raft commit index task failed: {source}"),
        })?
}

pub fn apply_cluster_raft_append(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: Option<RaftSnapshot>,
    append: RaftAppendEntriesRequest,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let (response, wrote) = paths
        .kv_paths()
        .apply_raft_append_with_response_and_publish_report(
            node_id,
            snapshot,
            append,
            sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(kv_err)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(response)
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

pub struct LocalClusterRaftCommandStorage<'a> {
    paths: &'a ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

impl<'a> LocalClusterRaftCommandStorage<'a> {
    pub fn new(paths: &'a ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            paths,
            inner: paths.kv_paths().local_raft_command_storage(
                local_node_id,
                sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
            ),
        }
    }

    fn observe_last_snapshot_publish(&mut self) {
        if let Some(wrote) = self.inner.take_last_snapshot_publish_wrote() {
            observe_cluster_kv_snapshot_write(self.paths, wrote);
        }
    }

    pub(crate) fn append_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, NetworkError> {
        self.inner
            .append_uncommitted_or_no_effect(command)
            .map_err(kv_err)
    }

    pub(crate) fn discard_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<KvSnapshot, NetworkError> {
        let snapshot = self
            .inner
            .discard_uncommitted_tail_and_clear_leader()
            .map_err(kv_err)?;
        self.observe_last_snapshot_publish();
        Ok(snapshot)
    }

    pub(crate) fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        let result = self.inner.commit_index_with_events(index).map_err(kv_err)?;
        self.observe_last_snapshot_publish();
        Ok(result)
    }
}

impl sinomo_kv::ClusterKvRaftCommandStorage for LocalClusterRaftCommandStorage<'_> {
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, NetworkError> {
        self.append_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.discard_uncommitted_tail_and_clear_leader().map(|_| ())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        self.commit_index_with_events(index)
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
    sinomo_network::NetworkError::Directory {
        detail: error.to_string(),
    }
}
