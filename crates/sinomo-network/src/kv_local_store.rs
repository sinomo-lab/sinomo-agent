use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
use crate::io_observer::io_observer;
use crate::local_state_store::read_cluster_state_without_kv;

use sinomo_core::ClusterBootstrapState;
#[cfg(test)]
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvLogEntry, KvWalPaths};
use sinomo_kv::{
    ClusterKvRaftCommitResult, ClusterKvRaftUncommittedAppend, DurableRaftKvNode, InProcessKvStore,
    KvCommand, KvEntry, KvLocalCache, KvOp, KvRevision, KvSnapshot, KvTxnResponse,
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftRequestVoteRequest,
    RaftRequestVoteResponse, RaftSnapshot,
};
use sinomo_node::NodeId;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(test)]
use std::time::SystemTime;

const FILE_CACHE_CAPACITY: usize = 4096;
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_READER_NODE_ID_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn invalidate_cluster_kv_reader_node_id_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
fn record_cluster_kv_cached_snapshot_read_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        *reads.entry(paths.kv_snapshot.clone()).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        reads.remove(&paths.kv_snapshot);
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&paths.kv_snapshot).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_reader_node_id_for_test(paths: &ClusterPaths) -> Option<NodeId> {
    let mark = cluster_file_mark(&paths.state)?;
    cached_cluster_kv_reader_node_id(&paths.state, mark)
}

#[cfg(test)]
pub(crate) fn cluster_kv_metadata_cache_capacity_for_test() -> usize {
    FILE_CACHE_CAPACITY
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf, node_id: &NodeId) {
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

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf) -> bool {
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

#[cfg(test)]
pub(crate) fn cluster_kv_reader_node_id_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_READER_NODE_ID_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

pub(crate) fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_local_cache(paths)
        .cached_snapshot()
        .map_err(Into::into)
}

pub(crate) fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let wrote =
        sinomo_kv::write_cluster_kv_materialized_snapshot_if_changed(&paths.kv_paths(), snapshot)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(())
}

pub(crate) fn write_imported_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = sinomo_kv::write_imported_cluster_kv_snapshot_if_changed(
        &paths.kv_paths(),
        snapshot,
        sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

pub(crate) fn observe_cluster_kv_snapshot_write(paths: &ClusterPaths, wrote: bool) {
    if let Some(observer) = io_observer() {
        if wrote {
            observer.on_kv_snapshot_actual_write();
            if let Ok(metadata) = fs::metadata(&paths.kv_snapshot) {
                observer.on_disk_write(metadata.len());
            }
        } else {
            observer.on_kv_snapshot_skipped_write();
        }
    }
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_store(
    paths: &ClusterPaths,
) -> Result<InProcessKvStore, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_store_for_node(&paths.kv_paths(), node_id).map_err(Into::into)
}

pub(crate) fn read_cluster_kv_current_snapshot(
    paths: &ClusterPaths,
) -> Result<KvSnapshot, NetworkError> {
    read_cluster_kv_current_snapshot_arc(paths).map(|snapshot| (*snapshot).clone())
}

pub(crate) fn read_cluster_kv_current_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&paths.kv_paths(), node_id)
        .map_err(Into::into)
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
    read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)
        .map(|snapshot| snapshot.unwrap_or_else(|| Arc::new(InProcessKvStore::new().snapshot())))
}

pub(crate) fn read_cluster_kv_entry(
    paths: &ClusterPaths,
    key: &str,
) -> Result<Option<KvEntry>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    Ok(snapshot.get(key).cloned())
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries_limited(
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

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
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
    .map_err(Into::into)
}

pub(crate) fn read_cluster_kv_cached_snapshot(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_cached_snapshot_read_for_test(paths);
    cluster_kv_local_cache(paths)
        .cached_snapshot_arc()
        .map_err(NetworkError::from)
}

pub(crate) fn read_cluster_kv_cached_snapshot_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    let snapshot = cluster_kv_local_cache(paths)
        .cached_snapshot_arc_if_present()
        .map_err(NetworkError::from)?;
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
        Err(_) if !paths.kv_raft.exists() => {
            Ok(NodeId::from_legacy_named("cluster_kv_cache_reader")
                .expect("static cache reader node id"))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    paths.kv_paths().raft_wal_paths(node_id)
}

pub(crate) fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    paths.kv_paths().open_raft_node(node_id).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .restore_raft_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

pub(crate) fn locked_restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .locked_restore_raft_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

pub(crate) fn locked_kv_state_authority_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    paths
        .kv_paths()
        .locked_known_raft_voter_leader_id(state)
        .map_err(Into::into)
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

#[cfg(test)]
pub(crate) async fn open_cluster_raft_kv_node_async(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    paths
        .kv_paths()
        .open_raft_node_async(node_id)
        .await
        .map_err(Into::into)
}

pub(crate) fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command_with_response(paths, node_id, command).map(|(snapshot, _)| snapshot)
}

/// The local node's current cluster KV snapshot revision, if one is readable;
/// `None` asks the voter to ship its full snapshot back.
pub(crate) fn local_cluster_kv_snapshot_revision(paths: &ClusterPaths) -> Option<KvRevision> {
    let node_id = read_cluster_kv_reader_node_id(paths).ok()?;
    paths
        .kv_paths()
        .current_snapshot_revision_for_node(&node_id)
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
    let (snapshot, wrote) = paths.kv_paths().append_kv_ops_in_batches_and_publish(
        node_id,
        ops,
        sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(snapshot)
}

pub(crate) fn append_cluster_raft_command_with_response(
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
        .map_err(NetworkError::from)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok((snapshot, txn_response))
}

#[cfg(test)]
pub(crate) fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    paths
        .kv_paths()
        .append_raft_command_uncommitted(node_id, command)
        .map_err(Into::into)
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

pub(crate) fn apply_cluster_raft_append(
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
        )?;
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
        .map_err(Into::into)
}

pub(crate) struct LocalClusterRaftCommandStorage<'a> {
    paths: &'a ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

impl<'a> LocalClusterRaftCommandStorage<'a> {
    pub(crate) fn new(paths: &'a ClusterPaths, local_node_id: NodeId) -> Self {
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
            .map_err(Into::into)
    }

    pub(crate) fn discard_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<KvSnapshot, NetworkError> {
        let snapshot = self.inner.discard_uncommitted_tail_and_clear_leader()?;
        self.observe_last_snapshot_publish();
        Ok(snapshot)
    }

    pub(crate) fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, NetworkError> {
        let result = self.inner.commit_index_with_events(index)?;
        self.observe_last_snapshot_publish();
        Ok(result)
    }
}

impl sinomo_kv::ClusterKvRaftCommandStorage for LocalClusterRaftCommandStorage<'_> {
    type Error = NetworkError;

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
