use crate::internal::*;

static CLUSTER_KV_RAFT_OPERATION_LOCKS: LazyLock<
    Mutex<HashMap<PathBuf, std::sync::Weak<ClusterKvRaftOperationLock>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
/// Process-resident durable Raft nodes, keyed by their node-local WAL root.
///
/// etcd keeps its MVCC store, Raft storage, and backend open for the lifetime of
/// the server and restores them only during startup/recovery. Sinomo has the
/// same single-main-daemon ownership invariant, so reopening and replaying the
/// complete snapshot/WAL for every command is unnecessary. The cached file
/// handle deliberately retains the existing exclusive node lock: a shadow or
/// recovery process cannot become a second writer while the main daemon owns
/// the resident state. A process that only needs a read can still use the
/// materialized snapshot fallback when that lock is busy.
static RESIDENT_RAFT_NODES: LazyLock<Mutex<HashMap<PathBuf, Arc<ResidentRaftNodeSlot>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const RESIDENT_RAFT_NODE_CAPACITY: usize = 64;

#[derive(Debug)]
struct ResidentRaftNode {
    node_id: NodeId,
    identity: RaftViewIdentity,
    lock_identity: FileIdentity,
    root_ino: u64,
    lock: File,
    node: ReplicatedKvNode,
}

fn file_handle_identity(file: &File, path: &Path) -> Result<FileIdentity, KvError> {
    let metadata = file.metadata().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mtime = metadata.modified().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(FileIdentity {
        mtime: Some(mtime),
        len: metadata.len(),
        ino: file_ino(&metadata),
    })
}

fn directory_ino(path: &Path) -> Result<u64, KvError> {
    fs::metadata(path)
        .map(|metadata| file_ino(&metadata))
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[derive(Clone, Debug)]
struct ResidentRaftReadMetadata {
    node_id: NodeId,
    identity: RaftViewIdentity,
    revision: KvRevision,
    observed_term: u64,
    applied_index: u64,
    known_leader: Option<NodeId>,
}

impl ResidentRaftReadMetadata {
    fn from_resident(resident: &ResidentRaftNode) -> Self {
        Self {
            node_id: resident.node_id.clone(),
            identity: resident.identity,
            revision: resident.node.store.revision(),
            observed_term: resident.node.hard_state.current_term,
            applied_index: resident.node.applied_index,
            known_leader: resident.node.hard_state.leader_id.clone(),
        }
    }

    fn from_open_node(node: &DurableRaftKvNode, identity: RaftViewIdentity) -> Self {
        Self {
            node_id: node.node_id.clone(),
            identity,
            revision: node.node.store.revision(),
            observed_term: node.node.hard_state.current_term,
            applied_index: node.node.applied_index,
            known_leader: node.node.hard_state.leader_id.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct ResidentRaftNodeSlotState {
    checked_out: bool,
    read_loading: bool,
    cached: Option<ResidentRaftNode>,
    /// Small authority metadata for the last immutable committed view. The
    /// values are observational while a writer is checked out; key/value data
    /// lives either in the actual idle resident node or the bounded immutable
    /// Raft-view snapshot cache.
    read_metadata: Option<ResidentRaftReadMetadata>,
}

#[derive(Debug, Default)]
struct ResidentRaftNodeSlot {
    state: Mutex<ResidentRaftNodeSlotState>,
    ready: Condvar,
}

impl ResidentRaftNodeSlot {
    fn checkout(&self, root: &Path, wait: Duration) -> Result<Option<ResidentRaftNode>, KvError> {
        let mut state = self.state.lock().map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("resident cluster KV Raft node cache poisoned: {error}"),
        })?;
        let deadline = Instant::now() + wait;
        while state.checked_out || state.read_loading {
            let now = Instant::now();
            if wait.is_zero() || now >= deadline {
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout) =
                self.ready
                    .wait_timeout(state, remaining)
                    .map_err(|error| KvError::Io {
                        path: root.display().to_string(),
                        detail: format!(
                            "resident cluster KV Raft node cache poisoned while waiting: {error}"
                        ),
                    })?;
            state = next_state;
            if timeout.timed_out() && state.checked_out {
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
        }
        state.checked_out = true;
        Ok(state.cached.take())
    }

    fn checkin(&self, resident: ResidentRaftNode) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = Some(ResidentRaftReadMetadata::from_resident(&resident));
        state.cached = Some(resident);
        state.checked_out = false;
        self.ready.notify_one();
    }

    fn release_without_cache(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.cached = None;
        state.read_metadata = None;
        state.checked_out = false;
        state.read_loading = false;
        self.ready.notify_all();
    }

    fn clear_read_metadata(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = None;
    }
}

/// Serializes the one cold snapshot/WAL replay needed to seed an immutable
/// local read view. This is deliberately separate from resident writer
/// ownership: completing or unwinding the load releases the flock and wakes
/// every reader waiting to recheck the snapshot cache.
pub(crate) struct ResidentRaftColdReadGuard {
    slot: Arc<ResidentRaftNodeSlot>,
    complete: bool,
}

pub(crate) enum ResidentRaftColdReadStart {
    Load(ResidentRaftColdReadGuard),
    /// Another read just populated the immutable view cache. The caller must
    /// recheck that cache rather than mistaking the registry-only slot for a
    /// resident writer.
    RetryImmutableCache,
    /// An actual idle resident writer appeared after the caller's first
    /// resident read probe.
    RetryResident,
}

impl ResidentRaftColdReadGuard {
    pub(crate) fn complete(
        mut self,
        node_id: NodeId,
        identity: RaftViewIdentity,
        revision: KvRevision,
        observed_term: u64,
        applied_index: u64,
        known_leader: Option<NodeId>,
    ) {
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = Some(ResidentRaftReadMetadata {
            node_id,
            identity,
            revision,
            observed_term,
            applied_index,
            known_leader,
        });
        state.read_loading = false;
        self.complete = true;
        self.slot.ready.notify_all();
    }
}

impl Drop for ResidentRaftColdReadGuard {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_loading = false;
        self.slot.ready.notify_all();
        metrics::counter!("sinomo.kv.raft.local_read_loads", "outcome" => "abandoned").increment(1);
    }
}

fn resident_raft_node_slot(root: &Path) -> Result<Arc<ResidentRaftNodeSlot>, KvError> {
    let mut slots = RESIDENT_RAFT_NODES.lock().map_err(|error| KvError::Io {
        path: root.display().to_string(),
        detail: format!("resident cluster KV Raft node registry poisoned: {error}"),
    })?;
    if let Some(slot) = slots.get(root) {
        return Ok(slot.clone());
    }
    if slots.len() >= RESIDENT_RAFT_NODE_CAPACITY {
        // A registry-only reference means no operation has the slot checked out.
        // Evicting it drops the retained file lock and bounded resident state.
        let eviction = slots
            .iter()
            .find(|(_, slot)| Arc::strong_count(slot) == 1)
            .map(|(path, _)| path.clone());
        if let Some(path) = eviction {
            slots.remove(&path);
            metrics::counter!("sinomo.kv.raft.resident.evictions").increment(1);
        } else {
            // All registered slots are active. Preserve the hard capacity by
            // using an operation-scoped slot whose cached node is released when
            // the returned handle drops instead of growing the registry.
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "uncached_capacity")
                .increment(1);
            return Ok(Arc::new(ResidentRaftNodeSlot::default()));
        }
    }
    let slot = Arc::new(ResidentRaftNodeSlot::default());
    slots.insert(root.to_path_buf(), slot.clone());
    Ok(slot)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn registered_resident_raft_node_exists(paths: &KvWalPaths) -> bool {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .is_some_and(|slots| slots.contains_key(&paths.root))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn cached_resident_raft_node_exists_for_test(paths: &KvWalPaths) -> bool {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|slots| slots.get(&paths.root).cloned())
        .and_then(|slot| slot.state.lock().ok().map(|state| state.cached.is_some()))
        .unwrap_or(false)
}

pub(crate) fn begin_resident_raft_cold_read(
    paths: &KvWalPaths,
) -> Result<ResidentRaftColdReadStart, KvError> {
    let slot = resident_raft_node_slot(&paths.root)?;
    let mut state = slot.state.lock().map_err(|error| KvError::Io {
        path: paths.root.display().to_string(),
        detail: format!("resident cluster KV Raft read loader poisoned: {error}"),
    })?;
    let deadline = Instant::now() + RAFT_NODE_LOCK_WAIT;
    let mut waited_for_loader = false;
    while state.read_loading {
        waited_for_loader = true;
        let now = Instant::now();
        if now >= deadline {
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
        let (next, timeout) = slot
            .ready
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .map_err(|error| KvError::Io {
                path: paths.root.display().to_string(),
                detail: format!(
                    "resident cluster KV Raft read loader poisoned while waiting: {error}"
                ),
            })?;
        state = next;
        if timeout.timed_out() && state.read_loading {
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
    }
    if waited_for_loader {
        return Ok(ResidentRaftColdReadStart::RetryImmutableCache);
    }
    if state.cached.is_some() {
        return Ok(ResidentRaftColdReadStart::RetryResident);
    }
    if state.checked_out {
        return Err(KvError::Busy {
            path: paths.root.display().to_string(),
        });
    }
    state.read_loading = true;
    drop(state);
    Ok(ResidentRaftColdReadStart::Load(ResidentRaftColdReadGuard {
        slot,
        complete: false,
    }))
}

fn resident_raft_ownership_matches(
    resident: &ResidentRaftNode,
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> bool {
    resident.node_id == *node_id
        && file_identity(&paths.lock) == Some(resident.lock_identity)
        && file_handle_identity(&resident.lock, &paths.lock).ok() == Some(resident.lock_identity)
        && directory_ino(&paths.root).ok() == Some(resident.root_ino)
        && raft_view_identity(paths) == Some(resident.identity)
}

fn with_idle_resident_raft_node<T, F>(
    paths: &KvWalPaths,
    node_id: &NodeId,
    read: F,
) -> Option<Result<KvLocalRead<T>, KvError>>
where
    F: FnOnce(&ReplicatedKvNode) -> Result<T, KvError>,
{
    let slot = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root)?.clone();
    let mut state = slot.state.lock().ok()?;
    if state.checked_out || state.read_loading {
        return None;
    }
    let valid = state
        .cached
        .as_ref()
        .is_some_and(|resident| resident_raft_ownership_matches(resident, paths, node_id));
    if !valid {
        if state.cached.take().is_some() {
            state.read_metadata = None;
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "read_invalidated")
                .increment(1);
        }
        return None;
    }
    let resident = state.cached.as_ref()?;
    Some(read(&resident.node).map(|value| KvLocalRead {
        value,
        revision: resident.node.store.revision(),
        observed_term: resident.node.hard_state.current_term,
        applied_index: resident.node.applied_index,
        known_leader: resident.node.hard_state.leader_id.clone(),
    }))
}

pub(crate) fn resident_raft_local_read_metadata(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<(KvRevision, u64, u64, Option<NodeId>)> {
    let slot = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root)?.clone();
    let mut state = slot.state.lock().ok()?;
    if !state.checked_out && !state.read_loading {
        if let Some(resident) = state.cached.as_ref() {
            if !resident_raft_ownership_matches(resident, paths, node_id) {
                state.cached = None;
                state.read_metadata = None;
                return None;
            }
        } else if state.read_metadata.as_ref().is_some_and(|metadata| {
            metadata.node_id != *node_id || raft_view_identity(paths) != Some(metadata.identity)
        }) {
            state.read_metadata = None;
            return None;
        }
    }
    let metadata = state.read_metadata.as_ref()?;
    if metadata.node_id != *node_id {
        return None;
    }
    Some((
        metadata.revision,
        metadata.observed_term,
        metadata.applied_index,
        metadata.known_leader.clone(),
    ))
}

pub(crate) fn read_resident_raft_local_entry(
    paths: &KvWalPaths,
    node_id: &NodeId,
    key: &str,
) -> Option<Result<KvLocalRead<Option<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        node.store
            .get(key)
            .map(|entry| try_clone_kv_entry(entry, "read resident local KV entry"))
            .transpose()
    })
}

pub(crate) fn read_resident_raft_local_prefix(
    paths: &KvWalPaths,
    node_id: &NodeId,
    prefix: &str,
) -> Option<Result<KvLocalRead<Vec<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| node.store.range_prefix(prefix))
}

pub(crate) fn read_resident_raft_local_prefix_limited_by_payload_bytes(
    paths: &KvWalPaths,
    node_id: &NodeId,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Option<Result<KvLocalRead<Vec<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        node.store
            .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    })
}

pub(crate) fn read_resident_raft_local_snapshot(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<Result<KvLocalRead<KvSnapshot>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| node.store.try_snapshot())
}

pub(crate) fn read_resident_raft_local_event_history(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<Result<KvLocalRead<ClusterKvEventReadView>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        let mut events = Vec::new();
        try_reserve_kv_items(
            &mut events,
            node.store.event_history_len(),
            "clone resident cluster KV event history",
        )?;
        for event in node.store.retained_events() {
            events.push(try_clone_kv_event(
                event,
                "clone resident cluster KV event history",
            )?);
        }
        Ok(ClusterKvEventReadView {
            compacted_revision: node.store.compacted_revision(),
            events,
        })
    })
}

#[cfg(test)]
static RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, (u64, u64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
fn record_resident_raft_open_for_test(root: &Path, warm: bool) {
    let mut counts = RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = counts.entry(root.to_path_buf()).or_default();
    if warm {
        count.1 = count.1.saturating_add(1);
    } else {
        count.0 = count.0.saturating_add(1);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_resident_raft_open_counts_for_test(root: &Path) {
    RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn resident_raft_open_counts_for_test(root: &Path) -> (u64, u64) {
    RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn reset_nonresident_raft_open_count_for_test(root: &Path) {
    NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
pub(crate) fn nonresident_raft_open_count_for_test(root: &Path) -> u64 {
    NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}
#[cfg(test)]
pub(crate) static CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST: AtomicU64 =
    AtomicU64::new(MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES);
#[cfg(test)]
static CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn fail_cluster_kv_raft_apply_after_entries_for_test(
    paths: &KvWalPaths,
    applied_entries: usize,
) {
    assert!(applied_entries > 0, "failure must follow a partial apply");
    CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(paths.root.clone(), applied_entries);
}

#[cfg(test)]
fn take_cluster_kv_raft_apply_failure_for_test(paths: &KvWalPaths) -> Option<usize> {
    CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&paths.root)
}

struct ClusterKvRaftOperationLock {
    held: Mutex<bool>,
    ready: Condvar,
}

/// Owned permit for one local cluster-KV Raft operation.
///
/// Unlike `std::sync::MutexGuard`, this permit is `Send`, so an async quorum
/// command can retain it while its durable node remains open across blocking
/// storage phases and network awaits.
pub struct ClusterKvRaftOperationGuard {
    root: PathBuf,
    lock: Arc<ClusterKvRaftOperationLock>,
}

fn remove_cluster_kv_raft_operation_lock_if_unused(
    root: &Path,
    lock: &Arc<ClusterKvRaftOperationLock>,
) {
    let mut locks = CLUSTER_KV_RAFT_OPERATION_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let is_registered_lock = locks
        .get(root)
        .is_some_and(|registered| registered.as_ptr() == Arc::as_ptr(lock));
    if is_registered_lock && Arc::strong_count(lock) == 1 {
        locks.remove(root);
    }
}

impl Drop for ClusterKvRaftOperationGuard {
    fn drop(&mut self) {
        {
            let mut held = self
                .lock
                .held
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *held = false;
            self.lock.ready.notify_one();
        }
        remove_cluster_kv_raft_operation_lock_if_unused(&self.root, &self.lock);
    }
}

pub fn lock_cluster_kv_raft_operation_for_root(
    root: impl AsRef<Path>,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    let root = root.as_ref().to_path_buf();
    let lock = {
        let mut guard = CLUSTER_KV_RAFT_OPERATION_LOCKS
            .lock()
            .map_err(|error| KvError::Io {
                path: "cluster kv raft operation locks".to_string(),
                detail: format!("cluster kv raft operation lock table poisoned: {error}"),
            })?;
        match guard.get(&root).and_then(std::sync::Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(ClusterKvRaftOperationLock {
                    held: Mutex::new(false),
                    ready: Condvar::new(),
                });
                guard.insert(root.clone(), Arc::downgrade(&lock));
                lock
            }
        }
    };
    let mut held = match lock.held.lock() {
        Ok(held) => held,
        Err(error) => {
            let detail = format!("cluster kv raft operation lock poisoned: {error}");
            drop(error);
            remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
            return Err(KvError::Io {
                path: root.display().to_string(),
                detail,
            });
        }
    };
    while *held {
        held = match lock.ready.wait(held) {
            Ok(held) => held,
            Err(error) => {
                let detail =
                    format!("cluster kv raft operation lock poisoned while waiting: {error}");
                drop(error);
                remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
                return Err(KvError::Io {
                    path: root.display().to_string(),
                    detail,
                });
            }
        };
    }
    *held = true;
    drop(held);
    Ok(ClusterKvRaftOperationGuard { root, lock })
}

#[cfg(test)]
fn cluster_kv_raft_operation_lock_is_registered_for_test(root: &Path) -> bool {
    CLUSTER_KV_RAFT_OPERATION_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains_key(root)
}

#[cfg(test)]
mod operation_lock_tests {
    use super::*;

    #[test]
    fn operation_lock_registry_releases_churned_roots() {
        fn assert_send<T: Send>() {}
        assert_send::<ClusterKvRaftOperationGuard>();

        let base = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-churn-{}",
            std::process::id()
        ));
        for index in 0..512 {
            let root = base.join(index.to_string());
            let guard =
                lock_cluster_kv_raft_operation_for_root(&root).expect("operation lock acquired");
            assert!(cluster_kv_raft_operation_lock_is_registered_for_test(&root));
            drop(guard);
            assert!(
                !cluster_kv_raft_operation_lock_is_registered_for_test(&root),
                "dropped operation lock root {index} remained registered"
            );
        }
    }

    #[test]
    fn operation_lock_registry_preserves_waiter_handoff() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-waiter-{}",
            std::process::id()
        ));
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let waiter_root = root.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (acquired_tx, acquired_rx) = std::sync::mpsc::sync_channel(0);
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("waiter start reported");
            let guard = lock_cluster_kv_raft_operation_for_root(&waiter_root)
                .expect("waiting operation lock acquired");
            acquired_tx.send(()).expect("waiter acquisition reported");
            drop(guard);
        });

        started_rx.recv().expect("waiter started");
        assert!(
            matches!(
                acquired_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "waiter acquired while the first operation still held the lock"
        );
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter acquired after handoff");
        waiter.join().expect("waiter completed");
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }
}

pub fn lock_cluster_kv_raft_operation_for_paths(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

pub fn lock_cluster_kv_raft_operation_for_wal_paths(
    paths: &KvWalPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLogEntry {
    pub index: u64,
    pub term: u64,
    pub request_id: KvRequestId,
    pub command: KvCommand,
    pub revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftHardState {
    pub current_term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voted_for: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
}

impl Default for RaftHardState {
    fn default() -> Self {
        Self {
            current_term: 1,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub kv: KvSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub entries: Vec<KvLogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftUncommittedAppend {
    Appended(KvLogEntry),
    CommittedReplay {
        snapshot: KvSnapshot,
        record: KvCommittedRequestRecord,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCommitResult {
    /// Explicit owned materialization for compatibility callers that need a
    /// complete post-commit view. This is `O(total retained state)`; daemon hot
    /// paths should use [`ClusterKvRaftCompactCommitResult`] and resident
    /// point/prefix reads, reserving this representation for checkpoints and
    /// exports.
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
    pub committed_request: KvCommittedRequestRecord,
}

/// Incremental result of applying one committed Raft entry to the resident KV
/// state. Its cost is proportional to the command's emitted event set, not the
/// complete retained keyspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCompactCommitResult {
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
    pub committed_request: KvCommittedRequestRecord,
    pub read_state: ClusterKvRaftReadIndexState,
    pub compacted_revision: KvRevision,
    /// Present when this operation built a periodic Raft checkpoint or changed
    /// a critical/unknown keyspace whose shared main/shadow projection cannot
    /// safely lag. Deferrable operational and telemetry commits keep this
    /// `None` and remain proportional to their command delta.
    pub checkpoint_snapshot: Option<KvSnapshot>,
    pub final_key_states: Vec<ClusterKvCommittedKeyState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendRequest {
    pub snapshot: Option<RaftSnapshot>,
    pub append: RaftAppendEntriesRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLocalVoteMetadata {
    pub current_term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Coherent metadata for an etcd-style Raft ReadIndex operation.
///
/// This view is captured from the same resident node that serves the eventual
/// point, prefix, or snapshot read. Holding one local command-storage session
/// keeps the node and operation permit stable while the caller confirms the
/// leader's authority with a quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftReadIndexState {
    pub current_term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub commit_term: Option<u64>,
    pub applied_index: u64,
    pub revision: KvRevision,
    pub has_current_term_commit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvRaftConfigRecord {
    pub cluster_id: Id,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub voters: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub learners: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
struct RaftNodeIdSet {
    node_ids: BTreeSet<NodeId>,
}

impl RaftNodeIdSet {
    fn from_iter(
        node_ids: impl IntoIterator<Item = NodeId>,
        operation: &str,
    ) -> Result<Self, KvError> {
        let mut set = Self::default();
        for node_id in node_ids {
            set.try_insert_owned(node_id, operation)?;
        }
        Ok(set)
    }

    fn len(&self) -> usize {
        self.node_ids.len()
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.node_ids.contains(node_id)
    }

    fn try_insert_owned(&mut self, node_id: NodeId, operation: &str) -> Result<bool, KvError> {
        let _ = operation;
        Ok(self.node_ids.insert(node_id))
    }

    fn try_insert_clone(&mut self, node_id: &NodeId, operation: &str) -> Result<bool, KvError> {
        Ok(self
            .node_ids
            .insert(try_clone_kv_node_id(node_id, operation)?))
    }
}

fn cluster_kv_authority_failure(
    state: &ClusterBootstrapState,
    reason: KvAuthorityFailureReason,
    detail: String,
    observed_term: u64,
    revision: KvRevision,
    reachable_voters: usize,
    known_leader: Option<NodeId>,
) -> KvAuthorityFailure {
    let local_role = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::NonVoting);
    let suggested_recovery = match reason {
        KvAuthorityFailureReason::HigherTermObserved | KvAuthorityFailureReason::VoterRejected => {
            KvRecoveryAction::RefreshClusterState
        }
        KvAuthorityFailureReason::TransportFailure => KvRecoveryAction::ContactKnownLeader,
        KvAuthorityFailureReason::LocalNodeNotVoter
        | KvAuthorityFailureReason::NoQuorum
        | KvAuthorityFailureReason::Timeout => KvRecoveryAction::RetryWhenQuorumIsReachable,
    };
    KvAuthorityFailure {
        reason,
        detail,
        diagnostics: KvAuthorityDiagnostics {
            local_node_id: state.local_node_id.clone(),
            local_role,
            known_leader,
            observed_term,
            reachable_voters,
            required_quorum: cluster_kv_raft_quorum_size_for_state(state),
            last_successful_append_at: None,
            last_snapshot_refresh_at: None,
            authority: KvReadAuthority::Local,
            revision,
            freshness: KvFreshness::Unknown,
            suggested_recovery,
        },
    }
}

fn node_id_slice_contains(node_ids: &[NodeId], node_id: &NodeId) -> bool {
    node_ids.iter().any(|candidate| candidate == node_id)
}

fn unique_node_id_count(node_ids: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in node_ids.iter().enumerate() {
        if node_id_slice_contains(&node_ids[..index], node_id) {
            continue;
        }
        count += 1;
    }
    count
}

fn unique_node_id_overlap_count(left: &[NodeId], right: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in left.iter().enumerate() {
        if node_id_slice_contains(&left[..index], node_id) {
            continue;
        }
        if node_id_slice_contains(right, node_id) {
            count += 1;
        }
    }
    count
}

fn try_push_unique_node_id(
    node_ids: &mut Vec<NodeId>,
    node_id: &NodeId,
    operation: &str,
) -> Result<(), KvError> {
    if node_id_slice_contains(node_ids, node_id) {
        return Ok(());
    }
    try_reserve_kv_items(node_ids, 1, operation)?;
    node_ids.push(try_clone_kv_node_id(node_id, operation)?);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderElectionOptions {
    pub max_attempts: usize,
}

pub trait ClusterKvRaftLeaderElectionHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn begin_election_attempt(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error>;

    fn should_request_more_remote_votes(&mut self) -> bool {
        true
    }

    fn request_remote_vote(
        &mut self,
        target_index: usize,
        voter_id: &NodeId,
        vote: &RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error>;

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error>;

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error>;
}

pub fn cluster_kv_raft_quorum_size(voter_count: usize) -> usize {
    voter_count / 2 + 1
}

pub fn cluster_kv_member_has_raft_vote(member: &ClusterMemberRecord) -> bool {
    member.has_raft_vote()
}

pub fn cluster_kv_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.node_is_raft_voter(node_id)
}

pub fn cluster_kv_raft_candidate_is_authorized_voter(
    state: &ClusterBootstrapState,
    candidate_id: &NodeId,
) -> bool {
    cluster_kv_raft_node_is_voter(state, candidate_id)
}

pub fn cluster_kv_raft_local_is_voter(state: &ClusterBootstrapState) -> bool {
    cluster_kv_raft_node_is_voter(state, &state.local_node_id)
}

pub fn cluster_kv_raft_voter_ids(state: &ClusterBootstrapState) -> Result<Vec<NodeId>, KvError> {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    let mut voters = Vec::new();
    try_reserve_kv_items(&mut voters, voter_count, "collect cluster raft voters")?;
    for member in state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
    {
        voters.push(try_clone_kv_node_id(
            &member.node_id,
            "collect cluster raft voters",
        )?);
    }
    voters.sort_unstable();
    voters.dedup();
    Ok(voters)
}

pub fn cluster_kv_raft_quorum_size_for_state(state: &ClusterBootstrapState) -> usize {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    cluster_kv_raft_quorum_size(voter_count)
}

pub fn cluster_kv_raft_config_key() -> String {
    "/cluster/raft/config".to_string()
}

pub fn cluster_kv_raft_config_record_stable_eq(
    left: &ClusterKvRaftConfigRecord,
    right: &ClusterKvRaftConfigRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.term == right.term
        && left.voters == right.voters
        && left.learners == right.learners
        && left.non_voting == right.non_voting
}

pub fn cluster_kv_raft_config_member_role(
    config: &ClusterKvRaftConfigRecord,
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

pub fn cluster_kv_store_member_is_raft_voter(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<bool, KvError> {
    let Some(config_entry) = store.get(&cluster_kv_raft_config_key()) else {
        return Ok(false);
    };
    let config: ClusterKvRaftConfigRecord = config_entry.decode()?;
    Ok(config.voters.iter().any(|voter| voter == node_id))
}

pub fn validate_cluster_kv_raft_config_transition(
    current: &ClusterKvRaftConfigRecord,
    next: &ClusterKvRaftConfigRecord,
) -> Result<(), KvError> {
    if current.cluster_id != next.cluster_id {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "cluster raft config cluster id changed from `{}` to `{}`",
                current.cluster_id, next.cluster_id
            ),
        });
    }
    let mut removed_voters = Vec::new();
    for voter in current
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&next.voters, voter))
    {
        try_push_unique_node_id(&mut removed_voters, voter, "validate raft removed voters")?;
    }
    if !removed_voters.is_empty() {
        let retained_voters = unique_node_id_overlap_count(&current.voters, &next.voters);
        let current_voter_count = unique_node_id_count(&current.voters);
        let next_voter_count = unique_node_id_count(&next.voters);
        let current_quorum = cluster_kv_raft_quorum_size(current_voter_count);
        let next_quorum = cluster_kv_raft_quorum_size(next_voter_count);
        let voter_union = current_voter_count
            .saturating_add(next_voter_count)
            .saturating_sub(retained_voters);
        // Every old quorum must intersect every new quorum. Retaining enough
        // individual voters to form both quorums is not sufficient: for example,
        // shrinking five voters to three retains three voters but still permits
        // disjoint old/new quorums of sizes three and two.
        let every_quorum_overlaps = current_quorum.saturating_add(next_quorum) > voter_union;
        if retained_voters < current_quorum
            || retained_voters < next_quorum
            || !every_quorum_overlaps
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "raft voter removal must retain both quorum thresholds and guarantee every old/new quorum overlaps; refusing to remove {} (retained {retained_voters}, old quorum {current_quorum}, new quorum {next_quorum}, voter union {voter_union})",
                    removed_voters
                        .iter()
                        .map(NodeId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }
    let mut added_voters = Vec::new();
    for voter in next
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&current.voters, voter))
    {
        try_push_unique_node_id(&mut added_voters, voter, "validate raft added voters")?;
    }
    if added_voters.len() > 1 {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first() {
        if !current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .any(|node_id| node_id == added_voter)
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "raft voter `{added_voter}` must already be a committed learner or non-voting member before promotion"
                ),
            });
        }
    }
    Ok(())
}

pub fn elect_cluster_kv_raft_leader_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(&mut remote_vote_targets_vec, 1, "collect raft vote targets")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        host.begin_election_attempt()?;
        let metadata = host.local_vote_metadata()?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(local_node_id, "build raft election vote request")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote(RaftRequestVoteRequest {
            term: vote_request.term,
            candidate_id: try_clone_kv_node_id(&vote_request.candidate_id, "apply local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: vote_request.last_log_index,
            last_log_term: vote_request.last_log_term,
        })?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
            }
            if granted.contains(voter_id) {
                continue;
            }
            if !host.should_request_more_remote_votes() {
                break;
            }
            let Ok(response) = host.request_remote_vote(target_index, voter_id, &vote_request)
            else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            if response.vote_granted {
                granted
                    .try_insert_clone(voter_id, "record remote raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term(highest_observed_term)?;
            continue;
        }
        if granted.len() < quorum_size {
            return Ok(false);
        }
        host.set_raft_leader(election_term)?;
        return Ok(true);
    }
    Ok(false)
}

pub trait ClusterKvRaftLeaderElectionAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error>;

    fn should_request_more_remote_votes(&mut self) -> bool;

    fn request_remote_vote_async<'a>(
        &'a mut self,
        target_index: usize,
        voter_id: &'a NodeId,
        vote: &'a RaftRequestVoteRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error>;

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;
}

pub async fn elect_cluster_kv_raft_leader_async_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionAsyncHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect async raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(
                &mut remote_vote_targets_vec,
                1,
                "collect async raft vote targets",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        host.begin_election_attempt_async().await?;
        let metadata = host.local_vote_metadata_async().await?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(
                local_node_id,
                "build async raft election vote request",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host
            .apply_local_vote_async(RaftRequestVoteRequest {
                term: vote_request.term,
                candidate_id: try_clone_kv_node_id(
                    &vote_request.candidate_id,
                    "apply local async raft vote",
                )
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
                last_log_index: vote_request.last_log_index,
                last_log_term: vote_request.last_log_term,
            })
            .await?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local async raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
            }
            if granted.contains(voter_id) {
                continue;
            }
            if !host.should_request_more_remote_votes() {
                break;
            }
            let Ok(response) = host
                .request_remote_vote_async(target_index, voter_id, &vote_request)
                .await
            else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            if response.vote_granted {
                granted
                    .try_insert_clone(voter_id, "record remote async raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term_async(highest_observed_term).await?;
            continue;
        }
        if granted.len() < quorum_size {
            return Ok(false);
        }
        host.set_raft_leader_async(election_term).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Storage-side contract for applying a cluster KV command to the local Raft log.
///
/// Implementors own KV/Raft durability only. They must not choose peers, route
/// messages, score routes, or interpret the cluster connectivity graph.
pub trait ClusterKvRaftCommandStorage {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    /// Locks the whole commit sequence against concurrent committers (see
    /// [`lock_cluster_kv_commit_sequence`]). Hosts backed by a shared on-disk
    /// store must implement this; the default (no lock) is only safe for
    /// single-committer hosts such as tests.
    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<File>, Self::Error> {
        Ok(None)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    /// Replays only the durable request record needed to answer an idempotent
    /// command. The default preserves compatibility for snapshot-oriented
    /// hosts; resident production storage overrides it to avoid materializing
    /// the complete keyspace on a replay hit.
    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, Self::Error> {
        self.replay_cluster_raft_request_if_committed(request)
            .map(|replay| replay.map(|(_, record)| record))
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error>;

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error>;

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(&mut self)
    -> Result<(), Self::Error>;

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        _term: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error>;

    /// Applies a committed entry and returns only its delta and authority
    /// metadata. Snapshot-oriented hosts may use the compatibility default;
    /// resident production storage overrides it with an `O(command delta)`
    /// implementation.
    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, Self::Error> {
        self.commit_cluster_raft_index_with_events(index)
            .map(|result| {
                let revision = result.committed_request.revision;
                let compacted_revision = result.snapshot.compacted_revision;
                ClusterKvRaftCompactCommitResult {
                    txn_response: result.txn_response,
                    events: result.events,
                    committed_request: result.committed_request,
                    read_state: ClusterKvRaftReadIndexState {
                        current_term: 0,
                        leader_id: None,
                        commit_index: index,
                        commit_term: None,
                        applied_index: index,
                        revision,
                        has_current_term_commit: false,
                    },
                    compacted_revision,
                    checkpoint_snapshot: Some(result.snapshot),
                    final_key_states: Vec::new(),
                }
            })
    }

    /// Explicitly materializes the complete post-commit KV view for legacy
    /// callers. The compact quorum driver never invokes this method; it exists
    /// only behind [`commit_cluster_kv_raft_command_with_quorum`].
    ///
    /// Snapshot-oriented hosts can keep the compatibility default. Resident
    /// production hosts should call the compact quorum API and serve point or
    /// prefix reads from their resident indexes instead.
    fn materialize_cluster_raft_snapshot_after_commit(
        &mut self,
        request: &KvCommandRequest,
        index: u64,
        replayed_committed_request: bool,
    ) -> Result<KvSnapshot, Self::Error> {
        if replayed_committed_request {
            return self
                .replay_cluster_raft_request_if_committed(request)?
                .map(|(snapshot, _)| snapshot)
                .ok_or_else(|| Self::kv_error(KvError::MissingCommittedEntry { index }));
        }
        self.commit_cluster_raft_index_with_events(index)
            .map(|result| result.snapshot)
    }

    /// Releases any operation-scoped resident node after the durable local
    /// commit has completed. The global proposal lock is intentionally held
    /// until this returns so the next proposal cannot race the final local
    /// state publication, but post-commit fanout must not retain either lock.
    fn release_cluster_raft_command_session_after_commit(&mut self) {}

    /// Publishes process-local observations of an already durable commit.
    ///
    /// The quorum driver invokes this only after releasing both the resident
    /// command session and the global commit-sequence lock. Implementations may
    /// synchronously wake local watchers or invoke observers here without
    /// extending the serialized proposal critical section. `None` denotes an
    /// exact committed-request replay, which can still carry repair work queued
    /// while validating the durable request record.
    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        _commit: Option<&ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitOptions {
    pub timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

/// Quorum-commit result whose size is proportional to one command rather than
/// the complete retained keyspace. This is the canonical result for daemon
/// mutation paths; [`ClusterKvRaftQuorumCommitResult`] is a compatibility
/// materialization for exports and callers that truly require a full view.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCompactCommitResult {
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
    pub compacted_revision: KvRevision,
    pub checkpoint_snapshot: Option<KvSnapshot>,
    pub final_key_states: Vec<ClusterKvCommittedKeyState>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvTxnCommitResult {
    pub request_id: KvRequestId,
    pub command_digest: KvCommandDigest,
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvTxnCompactCommitResult {
    pub request_id: KvRequestId,
    /// Digest of the transaction exactly as submitted to the forwarding
    /// voter, before that voter appends authenticated authorization fences.
    pub submitted_command_digest: Option<KvCommandDigest>,
    /// Digest of the command that actually entered the durable Raft log.
    pub command_digest: KvCommandDigest,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftTargetReplicationStatus {
    Accepted,
    Rejected,
    Failed,
}

pub struct ClusterKvRaftTargetReplicationAttempt<T, E> {
    pub target: T,
    pub latency_ms: Option<u64>,
    pub result: Result<ClusterRaftAppendResponse, E>,
}

pub type ClusterKvRaftTargetReplicationAttempts<T, E> =
    Vec<ClusterKvRaftTargetReplicationAttempt<T, E>>;

pub type ClusterKvRaftQuorumFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOutcome {
    pub response: ClusterRaftAppendResponse,
    pub watch_event_fanout_needed: bool,
}

/// KV-owned append replication retry driver.
///
/// KV owns Raft log catch-up semantics: initial next-index selection, rejected
/// append backoff, acceptance detection, and watch-event fanout eligibility.
/// Hosts own durability access and transport-specific timeout/sending policy.
pub trait ClusterKvRaftAppendReplicationHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error>;

    fn raft_peer_progress_next_index_hint(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _last_local_raft_log_index: u64,
    ) -> Option<u64> {
        None
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;
}

pub trait ClusterKvRaftAppendReplicationAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone + Send + Sync;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, u64, Self::Error>;

    fn raft_peer_progress_next_index_hint_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _last_local_raft_log_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<u64>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;
}

pub fn cluster_kv_raft_next_index_after_rejection(
    next_index: u64,
    request: &ClusterRaftAppendRequest,
    match_index: u64,
) -> u64 {
    let rejected_next_index = match_index.saturating_add(1);
    if rejected_next_index < next_index {
        rejected_next_index
    } else if rejected_next_index == next_index
        && request.snapshot.is_none()
        && match_index == request.append.prev_log_index
        && request.append.prev_log_index == request.append.leader_commit
    {
        // The follower reports that its log ends at our prev_log_index but
        // rejected the committed anchor's term. Backtracking by one would resend
        // that conflicting committed entry. Restart from the catch-up anchor so
        // a compacted leader builds an authoritative snapshot instead.
        1
    } else if match_index >= next_index {
        next_index.saturating_div(2).max(1)
    } else {
        next_index.saturating_sub(1).max(1)
    }
}

pub fn replicate_cluster_kv_raft_to_target_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index(state)?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint(state, target, last_local_raft_log_index)
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host.build_raft_append_request(state, next_index)?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent()?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host.send_raft_append(target, &request, attempt_timeout)?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
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

pub async fn replicate_cluster_kv_raft_to_target_async_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index_async(state).await?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint_async(state, target, last_local_raft_log_index)
        .await?
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host
            .build_raft_append_request_async(state, next_index)
            .await?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent_async().await?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host
            .send_raft_append_async(target, &request, attempt_timeout)
            .await?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
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

/// Storage-side Raft quorum commit driver.
///
/// KV owns the Raft append/commit/rollback decision flow and quorum accounting.
/// The host owns all topology-specific work: choosing remote voter targets,
/// transmitting append RPCs, merging any peer cluster-state observations, and
/// reporting route/connectivity observations.
pub trait ClusterKvRaftQuorumCommitHost: ClusterKvRaftCommandStorage {
    type Target: Clone;

    /// Returns true only when durable local Raft state records this node as the
    /// elected leader for the current term. Reachability or routing preference
    /// alone is not sufficient.
    fn local_has_verified_raft_leadership(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;

    fn record_target_replication_status(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        _state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<Self::Target>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

/// Async storage-side Raft quorum commit host.
///
/// This mirrors `ClusterKvRaftQuorumCommitHost`, but every operation that may
/// cross an async storage or transport boundary is supplied by the caller. KV
/// still owns the quorum decision flow; the host owns transport and topology.
pub trait ClusterKvRaftQuorumCommitAsyncHost: Send {
    type Error: ToString + Send;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone + Send + Sync;

    fn local_has_verified_raft_leadership_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::lock_cluster_raft_commit_sequence`].
    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<File>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn replay_cluster_raft_request_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    fn replay_cluster_raft_request_record_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<KvCommittedRequestRecord>, Self::Error> {
        Box::pin(async move {
            self.replay_cluster_raft_request_if_committed_async(request)
                .await
                .map(|replay| replay.map(|(_, record)| record))
        })
    }

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, KvLogEntry, Self::Error>;

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a KvAuthorityFailure,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error>;

    fn commit_cluster_raft_index_compact_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCompactCommitResult, Self::Error> {
        Box::pin(async move {
            self.commit_cluster_raft_index_with_events_async(index)
                .await
                .map(|result| {
                    let revision = result.committed_request.revision;
                    let compacted_revision = result.snapshot.compacted_revision;
                    ClusterKvRaftCompactCommitResult {
                        txn_response: result.txn_response,
                        events: result.events,
                        committed_request: result.committed_request,
                        read_state: ClusterKvRaftReadIndexState {
                            current_term: 0,
                            leader_id: None,
                            commit_index: index,
                            commit_term: None,
                            applied_index: index,
                            revision,
                            has_current_term_commit: false,
                        },
                        compacted_revision,
                        checkpoint_snapshot: Some(result.snapshot),
                        final_key_states: Vec::new(),
                    }
                })
        })
    }

    /// Async compatibility materialization used only by the legacy full
    /// quorum-commit API. The canonical compact driver does not call it.
    fn materialize_cluster_raft_snapshot_after_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        index: u64,
        replayed_committed_request: bool,
    ) -> ClusterKvRaftQuorumFuture<'a, KvSnapshot, Self::Error> {
        Box::pin(async move {
            if replayed_committed_request {
                return self
                    .replay_cluster_raft_request_if_committed_async(request)
                    .await?
                    .map(|(snapshot, _)| snapshot)
                    .ok_or_else(|| Self::kv_error(KvError::MissingCommittedEntry { index }));
            }
            self.commit_cluster_raft_index_with_events_async(index)
                .await
                .map(|result| result.snapshot)
        })
    }

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::release_cluster_raft_command_session_after_commit`].
    fn release_cluster_raft_command_session_after_commit_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::publish_cluster_raft_post_commit_observations`].
    fn publish_cluster_raft_post_commit_observations_async<'a>(
        &'a mut self,
        _commit: Option<&'a ClusterKvRaftCompactCommitResult>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;

    /// Replicates to enough remote voters to satisfy the requested remote
    /// acknowledgement count. The default preserves compatibility for hosts
    /// that can only issue one transport call at a time; network hosts should
    /// override this to race independent voter RPCs and return as soon as the
    /// required acknowledgements arrive.
    fn replicate_cluster_raft_to_quorum_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<Self::Target>,
        required_remote_acks: usize,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<
        'a,
        ClusterKvRaftTargetReplicationAttempts<Self::Target, Self::Error>,
        Self::Error,
    > {
        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            let mut accepted = 0_usize;
            let mut attempted_node_ids = RaftNodeIdSet::default();
            let mut attempts = Vec::new();
            for target in targets {
                if accepted >= required_remote_acks {
                    break;
                }
                if !attempted_node_ids
                    .try_insert_clone(
                        self.target_node_id(&target),
                        "dedupe default async raft replication target",
                    )
                    .map_err(Self::kv_error)?
                {
                    continue;
                }
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let started = Instant::now();
                let result = self
                    .replicate_cluster_raft_to_target_async(state, &target, remaining)
                    .await;
                let latency_ms =
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                try_reserve_kv_items(&mut attempts, 1, "record async raft replication attempt")
                    .map_err(Self::kv_error)?;
                attempts.push(ClusterKvRaftTargetReplicationAttempt {
                    target,
                    latency_ms,
                    result,
                });
            }
            Ok(attempts)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        _state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<Self::Target>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

/// Compatibility quorum commit that explicitly materializes the complete KV
/// snapshot after the compact commit has become durable. New daemon mutation
/// paths should call [`commit_cluster_kv_raft_command_compact_with_quorum`].
pub fn commit_cluster_kv_raft_command_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let materialization_request = request.clone();
    let compact =
        commit_cluster_kv_raft_command_compact_with_quorum(state, request, options, host)?;
    let snapshot = match compact.checkpoint_snapshot.clone() {
        // Snapshot-oriented compatibility hosts already paid for a complete
        // projection while applying the entry. Reuse that exact view instead
        // of invoking their commit callback a second time.
        Some(snapshot) => snapshot,
        None => host.materialize_cluster_raft_snapshot_after_commit(
            &materialization_request,
            compact.authority.commit_index,
            compact.replayed_committed_request,
        )?,
    };
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot,
        txn_response: compact.txn_response,
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

/// Canonical quorum mutation path. It keeps the resident Raft state alive,
/// applies one command, and returns only that command's delta and authority
/// proof. No full-store snapshot is built unless the periodic checkpoint
/// threshold is reached naturally during the commit.
pub fn commit_cluster_kv_raft_command_compact_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCompactCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let commit_sequence_lock = {
        let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
        host.lock_cluster_raft_commit_sequence()?
    };
    concurrency_metrics.acquired();
    let replay = {
        let _phase = KvCommandPhaseMetricGuard::start("request_replay");
        host.replay_cluster_raft_request_record_if_committed(&request)?
    };
    if let Some(record) = replay {
        command_metrics.finish("replayed");
        host.release_cluster_raft_command_session_after_commit();
        drop(commit_sequence_lock);
        host.publish_cluster_raft_post_commit_observations(None)?;
        return Ok(ClusterKvRaftQuorumCompactCommitResult {
            txn_response: record.txn_response.clone(),
            events: Vec::new(),
            compacted_revision: KvRevision::ZERO,
            checkpoint_snapshot: None,
            final_key_states: Vec::new(),
            authority: KvAuthorityProof {
                term: record.term,
                commit_index: record.log_index,
                revision: record.revision,
                basis: KvAuthorityBasis::ExactCommittedRequest,
            },
            replayed_committed_request: true,
        });
    }
    let local_has_leadership = {
        let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
        host.local_has_verified_raft_leadership(state)?
    };
    if !local_has_leadership {
        let failure = cluster_kv_authority_failure(
            state,
            if cluster_kv_raft_local_is_voter(state) {
                KvAuthorityFailureReason::VoterRejected
            } else {
                KvAuthorityFailureReason::LocalNodeNotVoter
            },
            "local node lacks verified current-term Raft leadership".to_string(),
            0,
            KvRevision::ZERO,
            usize::from(cluster_kv_raft_local_is_voter(state)),
            None,
        );
        command_metrics.finish("not_leader");
        host.record_cluster_raft_request_rejected_before_commit(&request, &failure)?;
        return Err(host.cluster_authority_error(failure));
    }
    let request_for_outcome = request.clone();
    let entry = {
        let _phase = KvCommandPhaseMetricGuard::start("local_append");
        host.append_cluster_raft_request_uncommitted(request)?
    };

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    let mut deferred_replication_statuses = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks
            .try_insert_clone(&state.local_node_id, "record local raft commit ack")
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let targets = host.remote_voter_targets(state)?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
        }
        if voter_acks.contains(host.target_node_id(target)) {
            continue;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "raft command did not reach quorum before {}ms replication budget",
                options.timeout.as_millis()
            ));
            break;
        }
        let started = Instant::now();
        match host.replicate_cluster_raft_to_target(state, target, remaining) {
            Ok(response) if response.accepted => {
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer accepted Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                ));
                voter_acks
                    .try_insert_clone(host.target_node_id(target), "record remote raft commit ack")
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                host.observe_accepted_append_state(state, target, response.state)?;
                if fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(target),
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?
                {
                    try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                    fanout_targets.push(target.clone());
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                host.observe_cluster_raft_term(state, response.append.term)?;
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer rejected Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                ));
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer failed Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                ));
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        let _ = host.discard_cluster_raft_uncommitted_tail_and_clear_leader();
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        host.record_cluster_raft_request_unknown(&request_for_outcome, &detail)?;
        let failure = cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        );
        drop(commit_sequence_lock);
        for (target, status, latency_ms) in deferred_replication_statuses {
            let _ = host.record_target_replication_status(state, &target, status, latency_ms);
        }
        return Err(host.cluster_authority_error(failure));
    }

    let commit = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_index_compact(entry.index)?
    };
    command_metrics.finish("committed");
    host.release_cluster_raft_command_session_after_commit();
    // Append, quorum confirmation, and durable local apply are the serialized
    // commit. Post-response convergence is operation-owned but must not keep
    // the resident node, operation guard, or global proposal queue locked
    // across fanout transport work.
    drop(commit_sequence_lock);
    host.publish_cluster_raft_post_commit_observations(Some(&commit))?;
    for (target, status, latency_ms) in deferred_replication_statuses {
        let _ = host.record_target_replication_status(state, &target, status, latency_ms);
    }
    for target in targets {
        if fanout_node_ids
            .try_insert_clone(
                host.target_node_id(&target),
                "dedupe committed raft fanout target",
            )
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?
        {
            try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed raft fanout target",
            )
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
            fanout_targets.push(target);
        }
    }
    // The fan-out replicates the committed history to every member (learners
    // included — they converge event-driven on commit), so it runs even when
    // there are no REMOTE VOTER targets (single-voter clusters).
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        host.fan_out_committed_cluster_raft_command(
            state.clone(),
            commit.events.clone(),
            fanout_targets,
        )?;
    }
    Ok(ClusterKvRaftQuorumCompactCommitResult {
        txn_response: commit.txn_response,
        events: commit.events,
        compacted_revision: commit.compacted_revision,
        checkpoint_snapshot: commit.checkpoint_snapshot,
        final_key_states: commit.final_key_states,
        authority: KvAuthorityProof {
            term: entry.term,
            commit_index: entry.index,
            revision: commit.committed_request.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: voter_acks.len(),
                required_quorum: quorum_size,
            },
        },
        replayed_committed_request: false,
    })
}

pub async fn commit_cluster_kv_raft_command_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let materialization_request = request.clone();
    let compact =
        commit_cluster_kv_raft_command_compact_with_quorum_async(state, request, options, host)
            .await?;
    let snapshot = match compact.checkpoint_snapshot.clone() {
        // Match the synchronous compatibility path: the default compact
        // adapter already returned the complete post-commit view.
        Some(snapshot) => snapshot,
        None => {
            host.materialize_cluster_raft_snapshot_after_commit_async(
                &materialization_request,
                compact.authority.commit_index,
                compact.replayed_committed_request,
            )
            .await?
        }
    };
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot,
        txn_response: compact.txn_response,
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

/// Async counterpart of
/// [`commit_cluster_kv_raft_command_compact_with_quorum`].
pub async fn commit_cluster_kv_raft_command_compact_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCompactCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let commit_sequence_lock = {
        let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
        host.lock_cluster_raft_commit_sequence_async().await?
    };
    concurrency_metrics.acquired();
    let replay = {
        let _phase = KvCommandPhaseMetricGuard::start("request_replay");
        host.replay_cluster_raft_request_record_if_committed_async(&request)
            .await?
    };
    if let Some(record) = replay {
        command_metrics.finish("replayed");
        host.release_cluster_raft_command_session_after_commit_async()
            .await?;
        drop(commit_sequence_lock);
        host.publish_cluster_raft_post_commit_observations_async(None)
            .await?;
        return Ok(ClusterKvRaftQuorumCompactCommitResult {
            txn_response: record.txn_response.clone(),
            events: Vec::new(),
            compacted_revision: KvRevision::ZERO,
            checkpoint_snapshot: None,
            final_key_states: Vec::new(),
            authority: KvAuthorityProof {
                term: record.term,
                commit_index: record.log_index,
                revision: record.revision,
                basis: KvAuthorityBasis::ExactCommittedRequest,
            },
            replayed_committed_request: true,
        });
    }
    let local_has_leadership = {
        let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
        host.local_has_verified_raft_leadership_async(state).await?
    };
    if !local_has_leadership {
        let failure = cluster_kv_authority_failure(
            state,
            if cluster_kv_raft_local_is_voter(state) {
                KvAuthorityFailureReason::VoterRejected
            } else {
                KvAuthorityFailureReason::LocalNodeNotVoter
            },
            "local node lacks verified current-term Raft leadership".to_string(),
            0,
            KvRevision::ZERO,
            usize::from(cluster_kv_raft_local_is_voter(state)),
            None,
        );
        command_metrics.finish("not_leader");
        host.record_cluster_raft_request_rejected_before_commit_async(&request, &failure)
            .await?;
        return Err(host.cluster_authority_error(failure));
    }
    let request_for_outcome = request.clone();
    let entry = {
        let _phase = KvCommandPhaseMetricGuard::start("local_append");
        host.append_cluster_raft_request_uncommitted_async(request)
            .await?
    };

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    let mut deferred_replication_statuses = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks
            .try_insert_clone(&state.local_node_id, "record local async raft commit ack")
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let targets = host.remote_voter_targets_async(state).await?;
    let mut replication_node_ids = RaftNodeIdSet::default();
    let mut replication_targets = Vec::new();
    for target in &targets {
        let target_node_id = host.target_node_id(target);
        if voter_acks.contains(target_node_id)
            || !replication_node_ids
                .try_insert_clone(
                    target_node_id,
                    "dedupe async raft quorum replication target",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?
        {
            continue;
        }
        try_reserve_kv_items(
            &mut replication_targets,
            1,
            "collect unique async raft quorum replication target",
        )
        .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
        replication_targets.push(target.clone());
    }
    let required_remote_acks = quorum_size.saturating_sub(voter_acks.len());
    let attempts = host
        .replicate_cluster_raft_to_quorum_async(
            state,
            replication_targets,
            required_remote_acks,
            options.timeout,
        )
        .await?;
    for attempt in attempts {
        let target = attempt.target;
        match attempt.result {
            Ok(response) if response.accepted => {
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer accepted async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    attempt.latency_ms,
                ));
                voter_acks
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record remote async raft commit ack",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                host.observe_accepted_append_state_async(state, &target, response.state)
                    .await?;
                if fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?
                {
                    try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                    fanout_targets.push(target);
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                host.observe_cluster_raft_term_async(state, response.append.term)
                    .await?;
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer rejected async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    attempt.latency_ms,
                ));
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(&target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer failed async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    attempt.latency_ms,
                ));
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(&target),
                    error.to_string()
                ));
            }
        }
    }
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);

    if voter_acks.len() < quorum_size && Instant::now() >= deadline {
        failures.push(format!(
            "raft command did not reach quorum before {}ms replication budget",
            options.timeout.as_millis()
        ));
    }

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        let _ = host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader_async()
            .await;
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        host.record_cluster_raft_request_unknown_async(&request_for_outcome, &detail)
            .await?;
        let failure = cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        );
        drop(commit_sequence_lock);
        for (target, status, latency_ms) in deferred_replication_statuses {
            let _ = host
                .record_target_replication_status_async(state, &target, status, latency_ms)
                .await;
        }
        return Err(host.cluster_authority_error(failure));
    }

    let commit = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_index_compact_async(entry.index)
            .await?
    };
    command_metrics.finish("committed");
    host.release_cluster_raft_command_session_after_commit_async()
        .await?;
    // Match the synchronous driver: only the exact durable commit stays in the
    // proposal critical section. Fanout is safe after local apply and release
    // of the resident operation guard, and must not cause head-of-line
    // blocking for the next proposal.
    drop(commit_sequence_lock);
    host.publish_cluster_raft_post_commit_observations_async(Some(&commit))
        .await?;
    for (target, status, latency_ms) in deferred_replication_statuses {
        let _ = host
            .record_target_replication_status_async(state, &target, status, latency_ms)
            .await;
    }
    for target in targets {
        if fanout_node_ids
            .try_insert_clone(
                host.target_node_id(&target),
                "dedupe committed async raft fanout target",
            )
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?
        {
            try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed async raft fanout target",
            )
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
            fanout_targets.push(target);
        }
    }
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        host.fan_out_committed_cluster_raft_command_async(
            state.clone(),
            commit.events.clone(),
            fanout_targets,
        )
        .await?;
    }
    Ok(ClusterKvRaftQuorumCompactCommitResult {
        txn_response: commit.txn_response,
        events: commit.events,
        compacted_revision: commit.compacted_revision,
        checkpoint_snapshot: commit.checkpoint_snapshot,
        final_key_states: commit.final_key_states,
        authority: KvAuthorityProof {
            term: entry.term,
            commit_index: entry.index,
            revision: commit.committed_request.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: voter_acks.len(),
                required_quorum: quorum_size,
            },
        },
        replayed_committed_request: false,
    })
}

/// Establishes a fresh current-term quorum barrier and returns the snapshot at
/// that barrier. A new request id is intentionally generated for every call so
/// an old committed barrier can never be mistaken for current authority.
pub fn read_cluster_kv_authoritative_snapshot_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let result = commit_cluster_kv_raft_command_with_quorum(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

pub async fn read_cluster_kv_authoritative_snapshot_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let result = commit_cluster_kv_raft_command_with_quorum_async(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )
    .await?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumDriverOptions {
    pub allow_forwarding: bool,
}

const CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS: usize = 2;

fn invalid_forwarded_cluster_kv_commit(detail: impl Into<String>) -> KvError {
    KvError::AuthorityUnavailable {
        detail: detail.into(),
    }
}

fn validate_cluster_kv_txn_commit_response(
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    response: &ClusterKvTxnCommitResult,
) -> Result<(), KvError> {
    if response.request_id != request.request_id {
        return Err(invalid_forwarded_cluster_kv_commit(format!(
            "cluster KV commit response request ID {} does not match submitted request ID {}",
            response.request_id, request.request_id
        )));
    }
    response.snapshot.validate_resource_bounds()?;
    let txn_response = response.txn_response.as_ref().ok_or_else(|| {
        invalid_forwarded_cluster_kv_commit(
            "cluster KV transaction commit response omitted its transaction result",
        )
    })?;
    if response.authority.term == 0 || response.authority.commit_index == 0 {
        return Err(invalid_forwarded_cluster_kv_commit(
            "cluster KV commit response has an empty authority term or commit index",
        ));
    }
    match response.authority.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } => {
            let voter_count = cluster_kv_raft_voter_ids(state)?.len();
            let expected_quorum = cluster_kv_raft_quorum_size(voter_count);
            if response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "fresh-quorum commit response was marked as an exact committed replay",
                ));
            }
            if response.authority.term < state.term {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response term {} is older than observed term {}",
                    response.authority.term, state.term
                )));
            }
            if required_quorum != expected_quorum
                || voter_acks < expected_quorum
                || voter_acks > voter_count
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but membership has {voter_count} voters and requires {expected_quorum}"
                )));
            }
            if response.authority.revision != response.snapshot.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit proof revision {} does not match returned snapshot revision {}",
                    response.authority.revision.0, response.snapshot.revision.0
                )));
            }
            if txn_response.revision != response.authority.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV transaction result revision {} does not match authority revision {}",
                    txn_response.revision.0, response.authority.revision.0
                )));
            }
        }
        KvAuthorityBasis::ExactCommittedRequest => {
            if !response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "exact committed-request authority was not marked as a replay",
                ));
            }
            let record = response
                .snapshot
                .committed_requests
                .iter()
                .find(|record| record.request_id == request.request_id)
                .ok_or_else(|| {
                    invalid_forwarded_cluster_kv_commit(format!(
                        "exact committed-request response omitted durable record {}",
                        request.request_id
                    ))
                })?;
            if record.command_digest != response.command_digest
                || record.term != response.authority.term
                || record.log_index != response.authority.commit_index
                || record.revision != response.authority.revision
                || record.txn_response != response.txn_response
                || response.snapshot.revision < record.revision
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "exact committed-request proof for {} does not match its durable snapshot record",
                    request.request_id
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_cluster_kv_txn_compact_commit_response(
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    response: &ClusterKvTxnCompactCommitResult,
) -> Result<(), KvError> {
    if response.request_id != request.request_id {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV response request identity does not match the request",
        ));
    }
    let submitted_command_digest = request.clone().into_command_request().digest()?;
    if response
        .submitted_command_digest
        .is_some_and(|digest| digest != submitted_command_digest)
    {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV response digest does not match the submitted transaction",
        ));
    }
    let txn_response = response.txn_response.as_ref().ok_or_else(|| {
        invalid_forwarded_cluster_kv_commit(
            "compact cluster KV transaction response omitted its transaction result",
        )
    })?;
    if response.authority.term == 0
        || response.authority.commit_index == 0
        || txn_response.revision != response.authority.revision
    {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV transaction response carries an invalid authority proof",
        ));
    }
    match response.authority.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } => {
            let voter_count = cluster_kv_raft_voter_ids(state)?.len();
            let expected_quorum = cluster_kv_raft_quorum_size(voter_count);
            if response.replayed_committed_request
                || response.authority.term < state.term
                || required_quorum != expected_quorum
                || voter_acks < expected_quorum
                || voter_acks > voter_count
            {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "compact cluster KV quorum proof does not match current voter membership",
                ));
            }
        }
        KvAuthorityBasis::ExactCommittedRequest => {
            if !response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "compact exact-request proof was not marked as a replay",
                ));
            }
        }
    }
    Ok(())
}

pub trait ClusterKvTxnQuorumHost {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store(
        &mut self,
        _state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        request: &KvTxnRequest,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub trait ClusterKvTxnQuorumAsyncHost: Send {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn forward_cluster_kv_txn_compact_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCompactCommitResult>, Self::Error> {
        Box::pin(async move {
            let submitted_command_digest = request
                .clone()
                .into_command_request()
                .digest()
                .map_err(Self::kv_error)?;
            self.forward_cluster_kv_txn_to_voter_async(state, request)
                .await
                .map(|response| {
                    response.map(|response| ClusterKvTxnCompactCommitResult {
                        request_id: response.request_id,
                        submitted_command_digest: Some(submitted_command_digest),
                        command_digest: response.command_digest,
                        txn_response: response.txn_response,
                        authority: response.authority,
                        replayed_committed_request: response.replayed_committed_request,
                    })
                })
        })
    }

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn commit_cluster_raft_txn_compact_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCompactCommitResult, Self::Error> {
        Box::pin(async move {
            self.commit_cluster_raft_txn_with_quorum_async(state, request)
                .await
                .map(|commit| ClusterKvRaftQuorumCompactCommitResult {
                    txn_response: commit.txn_response,
                    events: Vec::new(),
                    compacted_revision: commit.snapshot.compacted_revision,
                    checkpoint_snapshot: Some(commit.snapshot),
                    final_key_states: Vec::new(),
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                })
        })
    }

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub fn commit_cluster_kv_txn_with_quorum_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumHost,
{
    let _ = host.refresh_current_cluster_kv_store(state);
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host.forward_cluster_kv_txn_to_voter(state, &request)?
        {
            validate_cluster_kv_txn_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count(state)?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader(state)? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host.forward_cluster_kv_txn_to_voter(state, &request) {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader(state)? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader(state)? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count(state)?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host.commit_cluster_raft_txn_with_quorum(state, request.clone()) {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count(state)?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

pub async fn commit_cluster_kv_txn_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let _ = host.refresh_current_cluster_kv_store_async(state).await;
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await?
        {
            // A forwarded response carries the full bounded snapshot. Its
            // synchronous structural validation is intentionally kept off the
            // async worker's deeply nested stream/RPC/quorum poll stack. This
            // is a scheduling boundary, not a larger-stack workaround: the
            // validator starts with an ordinary clean blocking-task stack and
            // returns the same owned response only after validation succeeds.
            let validation_state = state.clone();
            let validation_request = request.clone();
            let response = tokio::task::spawn_blocking(move || {
                validate_cluster_kv_txn_commit_response(
                    &validation_state,
                    &validation_request,
                    &response,
                )?;
                Ok::<_, KvError>(response)
            })
            .await
            .map_err(|source| {
                H::kv_error(KvError::Serialization {
                    detail: format!("cluster KV commit response validation task failed: {source}"),
                })
            })?
            .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader_async(state).await? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await
            {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader_async(state).await? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader_async(state).await? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count_async(state).await?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host
            .commit_cluster_raft_txn_with_quorum_async(state, request.clone())
            .await
        {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count_async(state).await?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

/// Compact transaction driver used by daemon mutation/RPC paths. It preserves
/// the existing forwarding, election, retry, and authority checks while
/// keeping both local and forwarded success results independent of keyspace
/// size.
pub async fn commit_cluster_kv_txn_compact_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCompactCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let _ = host.refresh_current_cluster_kv_store_async(state).await;
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host
                .forward_cluster_kv_txn_compact_to_voter_async(state, &request)
                .await?
        {
            validate_cluster_kv_txn_compact_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            format!(
                "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
                reachable_voters,
                cluster_kv_raft_quorum_size_for_state(state)
            ),
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader_async(state).await? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host
                .forward_cluster_kv_txn_compact_to_voter_async(state, &request)
                .await
            {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_compact_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader_async(state).await? {
                let mut detail = forwarding_failure.map_or_else(
                    || "local node is not the Raft leader and could not win quorum".to_string(),
                    |error| {
                        format!(
                            "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                            error.to_string()
                        )
                    },
                );
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader_async(state).await? {
            let reachable_voters = host.reachable_voter_count_async(state).await?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                "local node is not the Raft leader after election".to_string(),
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host
            .commit_cluster_raft_txn_compact_with_quorum_async(state, request.clone())
            .await
        {
            Ok(commit) => {
                return Ok(ClusterKvTxnCompactCommitResult {
                    request_id: request.request_id.clone(),
                    submitted_command_digest: Some(
                        request
                            .clone()
                            .into_command_request()
                            .digest()
                            .map_err(H::kv_error)?,
                    ),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let reachable_voters = host.reachable_voter_count_async(state).await?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        previous_commit_failure.map_or_else(
            || "local node could not commit after retrying Raft leadership".to_string(),
            |error| {
                format!(
                    "local node could not commit after retrying Raft leadership: {}",
                    error.to_string()
                )
            },
        ),
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCompactionPolicy {
    pub max_snapshot_bytes: u64,
    pub max_snapshot_events: usize,
}

pub fn default_cluster_kv_raft_compaction_policy() -> ClusterKvRaftCompactionPolicy {
    ClusterKvRaftCompactionPolicy {
        max_snapshot_bytes: cluster_kv_raft_snapshot_byte_limit(),
        max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
    }
}

pub fn cluster_kv_raft_snapshot_byte_limit() -> u64 {
    #[cfg(test)]
    {
        CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.load(Ordering::Relaxed)
    }
    #[cfg(not(test))]
    {
        MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_cluster_kv_raft_snapshot_byte_limit_for_test(limit: u64) -> u64 {
    CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.swap(limit, Ordering::Relaxed)
}

pub fn cluster_kv_raft_node_should_compact(
    node: &DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> bool {
    // Snapshotting rewrites the entire WAL down to the retained tail plus the
    // snapshot file, so it must only run when it makes durable progress.
    //
    // Compaction advances the on-disk snapshot to `applied_index` and drops log
    // entries older than the retained tail. The durable progress is
    // `applied_index - snapshot_index`; the in-memory log length and event
    // history are not valid progress triggers because they include the retained
    // replication tail.
    let compaction_high_watermark = RAFT_COMPACTION_RETAINED_ENTRIES.saturating_mul(2);
    let unsnapshotted_applied = node.applied_index().saturating_sub(node.snapshot_index());
    let has_compactable_progress = unsnapshotted_applied > compaction_high_watermark;
    let oversized_snapshot_can_shrink = snapshot_bytes > policy.max_snapshot_bytes
        && node.store().event_history_len() > policy.max_snapshot_events;
    has_compactable_progress || oversized_snapshot_can_shrink
}

pub fn compact_cluster_kv_raft_node_if_needed(
    node: &mut DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> Result<bool, KvError> {
    compact_cluster_kv_raft_node_with_checkpoint_if_needed(node, snapshot_bytes, policy)
        .map(|snapshot| snapshot.is_some())
}

fn compact_cluster_kv_raft_node_with_checkpoint_if_needed(
    node: &mut DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> Result<Option<RaftSnapshot>, KvError> {
    if !cluster_kv_raft_node_should_compact(node, snapshot_bytes, policy) {
        return Ok(None);
    }
    let mut metric = KvStorageMetricGuard::start("compaction", "sinomo.kv.compaction.us");
    let log_entries_before = node.log().len();
    node.compact_event_history_to(policy.max_snapshot_events);
    let snapshot = node.save_snapshot()?;
    metrics::counter!("sinomo.kv.compaction.runs", "outcome" => "ok").increment(1);
    metrics::histogram!("sinomo.kv.compaction.removed_log_entries")
        .record(log_entries_before.saturating_sub(node.log().len()) as f64);
    metric.succeed();
    Ok(Some(snapshot))
}

#[derive(Clone, Debug)]
pub struct ClusterKvRaftStore {
    paths: ClusterKvPaths,
    compaction_policy: ClusterKvRaftCompactionPolicy,
}

fn append_entries_request_uses_snapshot(node: &DurableRaftKvNode, next_index: u64) -> bool {
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let earliest_log_index = node.log().first().map(|entry| entry.index);
    let prev_log_resolvable = prev_log_index == node.snapshot_index()
        || earliest_log_index.is_some_and(|first| prev_log_index >= first);
    node.snapshot_index() > 0 && !prev_log_resolvable
}

fn build_append_entries_request_from_open_node(
    node: &DurableRaftKvNode,
    leader_id: &NodeId,
    next_index: u64,
) -> Result<ClusterKvRaftAppendRequest, KvError> {
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let include_snapshot = append_entries_request_uses_snapshot(node, next_index);
    let (snapshot, prev_log_index, entries) = if include_snapshot {
        let mut kv = node.store().try_snapshot()?;
        compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
        let snapshot = RaftSnapshot {
            last_included_index: node.applied_index(),
            last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
            kv,
        };
        let mut entries = Vec::new();
        for entry in node
            .log()
            .iter()
            .filter(|entry| entry.index > snapshot.last_included_index)
        {
            try_reserve_kv_items(&mut entries, 1, "build raft append snapshot tail")?;
            entries.push(entry.clone());
        }
        (Some(snapshot), node.applied_index(), entries)
    } else {
        let mut entries = Vec::new();
        for entry in node.log().iter().filter(|entry| entry.index >= next_index) {
            try_reserve_kv_items(&mut entries, 1, "build raft append tail")?;
            entries.push(entry.clone());
        }
        (None, prev_log_index, entries)
    };
    let prev_log_term = node.term_at(prev_log_index).unwrap_or(0);
    Ok(ClusterKvRaftAppendRequest {
        snapshot,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: leader_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: node.hard_state().commit_index,
        },
    })
}

fn read_index_state_from_open_node(node: &DurableRaftKvNode) -> ClusterKvRaftReadIndexState {
    let current_term = node.hard_state().current_term;
    let commit_index = node.hard_state().commit_index;
    let commit_term = node.term_at(commit_index);
    ClusterKvRaftReadIndexState {
        current_term,
        leader_id: node.hard_state().leader_id.clone(),
        commit_index,
        commit_term,
        applied_index: node.applied_index(),
        revision: node.store().revision(),
        has_current_term_commit: commit_index > 0 && commit_term == Some(current_term),
    }
}

fn committed_final_key_states(
    store: &InProcessKvStore,
    events: &[KvEvent],
) -> Result<Vec<ClusterKvCommittedKeyState>, KvError> {
    let mut keys = Vec::new();
    keys.try_reserve(events.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "collect committed KV delta keys".to_string(),
            requested_bytes: events.len().saturating_mul(std::mem::size_of::<String>()),
        })?;
    for event in events {
        keys.push(try_clone_kv_string(
            &event.key,
            "collect committed KV delta key",
        )?);
    }
    keys.sort_unstable();
    keys.dedup();
    let mut states = Vec::new();
    try_reserve_kv_items(
        &mut states,
        keys.len(),
        "collect committed KV final key states",
    )?;
    for key in keys {
        let entry = store
            .get(&key)
            .map(|entry| try_clone_kv_entry(entry, "collect committed KV final key state"))
            .transpose()?;
        states.push(ClusterKvCommittedKeyState { key, entry });
    }
    Ok(states)
}

fn committed_command_has_selected_lease_mutation(
    command: &KvCommand,
    txn_response: Option<&KvTxnResponse>,
) -> bool {
    match command {
        KvCommand::Barrier => false,
        KvCommand::Op(
            KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. },
        ) => true,
        KvCommand::Op(_) => false,
        KvCommand::Txn(_) => txn_response.is_some_and(|response| {
            response.responses.iter().any(|response| {
                matches!(
                    response,
                    KvOpResponse::LeaseGrant { .. }
                        | KvOpResponse::LeaseKeepAlive { .. }
                        | KvOpResponse::LeaseRevoke { .. }
                )
            })
        }),
    }
}

fn committed_op_requires_shared_projection<F>(
    op: &KvOp,
    key_can_defer_materialized_snapshot: &mut F,
) -> bool
where
    F: FnMut(&str) -> bool,
{
    match op {
        KvOp::Put { key, .. } | KvOp::Delete { key } => !key_can_defer_materialized_snapshot(key),
        KvOp::DeletePrefix { prefix } => !key_can_defer_materialized_snapshot(prefix),
        KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. } => true,
    }
}

/// Determines whether replay must verify the cross-process materialized
/// projection before acknowledging an already-durable request. This is used
/// only on the exceptional exact-replay path; ordinary commits use their
/// emitted event set so semantic no-ops remain delta-free.
fn committed_command_requires_shared_projection<F>(
    command: &KvCommand,
    txn_response: Option<&KvTxnResponse>,
    key_can_defer_materialized_snapshot: &mut F,
) -> bool
where
    F: FnMut(&str) -> bool,
{
    match command {
        KvCommand::Barrier => false,
        KvCommand::Op(op) => {
            committed_op_requires_shared_projection(op, key_can_defer_materialized_snapshot)
        }
        KvCommand::Txn(txn) => {
            let Some(response) = txn_response else {
                return true;
            };
            let selected = if response.succeeded {
                &txn.success
            } else {
                &txn.failure
            };
            selected.iter().any(|op| {
                committed_op_requires_shared_projection(op, key_can_defer_materialized_snapshot)
            })
        }
    }
}

impl ClusterKvRaftStore {
    pub fn new(paths: ClusterKvPaths, compaction_policy: ClusterKvRaftCompactionPolicy) -> Self {
        Self {
            paths,
            compaction_policy,
        }
    }

    pub fn paths(&self) -> &ClusterKvPaths {
        &self.paths
    }

    pub fn lock_operation(&self) -> Result<ClusterKvRaftOperationGuard, KvError> {
        lock_cluster_kv_raft_operation_for_paths(&self.paths)
    }

    pub fn wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        self.paths.raft_wal_paths(node_id)
    }

    fn snapshot_bytes(&self, node_id: &NodeId) -> u64 {
        fs::metadata(&self.wal_paths(node_id).snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    fn compact_node_if_needed(&self, node: &mut DurableRaftKvNode) -> Result<bool, KvError> {
        let Some(snapshot) = self.compact_node_if_needed_with_checkpoint(node)? else {
            return Ok(false);
        };
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(true)
    }

    fn compact_node_if_needed_with_checkpoint(
        &self,
        node: &mut DurableRaftKvNode,
    ) -> Result<Option<KvSnapshot>, KvError> {
        compact_cluster_kv_raft_node_with_checkpoint_if_needed(
            node,
            self.snapshot_bytes(node.node_id()),
            self.compaction_policy,
        )
        .map(|snapshot| snapshot.map(|snapshot| snapshot.kv))
    }

    pub fn open_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let mut node = DurableRaftKvNode::open_resident(node_id.clone(), self.wal_paths(node_id))?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
    }

    pub fn open_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        let mut node = DurableRaftKvNode::open_resident_discarding_uncommitted_tail(
            node_id.clone(),
            self.wal_paths(node_id),
        )?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
    }

    fn open_resident_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        self.open_node(node_id)
    }

    fn open_resident_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        self.open_node_discarding_uncommitted_tail(node_id)
    }

    pub async fn open_node_async(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || store.open_node(&node_id))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn build_append_entries_request_from_next_index(
        &self,
        node_id: &NodeId,
        leader_id: &NodeId,
        next_index: u64,
    ) -> Result<ClusterKvRaftAppendRequest, KvError> {
        let node = self.open_node(node_id)?;
        build_append_entries_request_from_open_node(&node, leader_id, next_index)
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let request = self.build_append_entries_request_from_next_index(
            &state.local_node_id,
            &state.local_node_id,
            next_index,
        )?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: request.snapshot,
            append: request.append,
        })
    }

    pub fn locked_build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.build_cluster_raft_append_request_from_next_index(state, next_index)
    }

    pub async fn locked_build_cluster_raft_append_request_from_next_index_async(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let store = self.clone();
        let state = state.clone();
        let error_path = store
            .wal_paths(&state.local_node_id)
            .root
            .display()
            .to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.build_cluster_raft_append_request_from_next_index(&state, next_index)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn cache_snapshot_if_newer_than_raft(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        self.paths
            .local_cache()
            .cache_snapshot_if_newer_than_raft(node_id.clone(), snapshot)
    }

    pub fn cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let cache_changed = self.cache_snapshot_if_newer_than_raft(node_id, snapshot)?;
        if cache_changed {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(cache_changed)
    }

    pub fn locked_cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.cache_snapshot_if_newer_than_raft_and_notify(node_id, snapshot)
    }

    pub fn local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let node = self.open_node(node_id)?;
        Ok(ClusterKvRaftLocalVoteMetadata {
            current_term: node.hard_state().current_term,
            last_log_index: node.last_log_index(),
            last_log_term: node.last_log_term(),
        })
    }

    pub fn locked_local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_vote_metadata(node_id)
    }

    pub fn last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.open_node(node_id).map(|node| node.last_log_index())
    }

    pub fn locked_last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.last_log_index(node_id)
    }

    pub async fn locked_last_log_index_async(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.last_log_index(&node_id)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: format!("cluster kv raft log index task failed: {source}"),
        })?
    }

    pub fn apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let mut node = self.open_node(node_id)?;
        node.request_vote(vote)
    }

    pub fn locked_apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.apply_vote(node_id, vote)
    }

    pub fn apply_vote_response(
        &self,
        state: &ClusterBootstrapState,
        vote: RaftRequestVoteRequest,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        let vote = self.apply_vote(&state.local_node_id, vote)?;
        Ok(ClusterRaftVoteResponse {
            accepted: vote.vote_granted,
            detail: if vote.vote_granted {
                "raft vote granted".to_string()
            } else {
                "raft vote rejected by local hard state".to_string()
            },
            state: state.clone(),
            vote,
        })
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let compact =
            self.apply_append_entries_with_optional_snapshot_compact(node_id, snapshot, append)?;
        let snapshot = compact
            .restored_snapshot
            .clone()
            .or(compact.checkpoint_snapshot.clone())
            .map(Ok)
            .unwrap_or_else(|| {
                let node = self.open_node(node_id)?;
                node.store().try_snapshot()
            })?;
        Ok(RaftAppendEntriesApplyOutcome {
            response: compact.response,
            snapshot,
            snapshot_restored: compact.snapshot_restored,
            committed_events: compact.committed_events,
        })
    }

    /// Applies follower AppendEntries without converting the resident KV store
    /// into a full snapshot. A full value is carried only when the leader
    /// supplied one for restore or this append naturally crossed the periodic
    /// checkpoint threshold.
    pub fn apply_append_entries_with_optional_snapshot_compact(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesCompactApplyOutcome, KvError> {
        let incoming_snapshot = snapshot.as_ref().map(|snapshot| snapshot.kv.clone());
        let mut node = self.open_node(node_id)?;
        let before_applied_index = node.applied_index();
        let (response, snapshot_restored, before_revision) =
            node.apply_append_entries_with_optional_snapshot_state(snapshot, append)?;
        let commit_advanced = node.applied_index() > before_applied_index;
        let selected_lease_mutation = commit_advanced
            && (before_applied_index.saturating_add(1)..=node.applied_index()).any(|index| {
                node.log_entry_at(index).is_some_and(|entry| {
                    committed_command_has_selected_lease_mutation(
                        &entry.command,
                        node.txn_response_at(index),
                    )
                })
            });
        // Capture the complete newly-applied batch before checkpoint compaction
        // trims retained event history at a revision boundary.
        let committed_events = if snapshot_restored || node.store().revision() == before_revision {
            Vec::new()
        } else {
            node.store().events_after_revision(before_revision)?
        };
        let final_key_states = committed_final_key_states(node.store(), &committed_events)?;
        let checkpoint_snapshot = self.compact_node_if_needed_with_checkpoint(&mut node)?;
        let read_state = read_index_state_from_open_node(&node);
        let compacted_revision = node.store().compacted_revision();
        node.refresh_resident_read_view();
        Ok(RaftAppendEntriesCompactApplyOutcome {
            response,
            snapshot_restored,
            restored_snapshot: snapshot_restored.then_some(incoming_snapshot).flatten(),
            committed_events,
            read_state,
            compacted_revision,
            checkpoint_snapshot,
            final_key_states,
            commit_advanced,
            selected_lease_mutation,
        })
    }

    pub fn apply_append_entries_with_optional_snapshot_and_publish<F>(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(RaftAppendEntriesApplyOutcome, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let outcome =
            self.apply_append_entries_with_optional_snapshot(node_id, snapshot, append)?;
        let wrote = self
            .publish_append_entries_apply_outcome(&outcome, key_can_defer_materialized_snapshot)?;
        Ok((outcome, wrote))
    }

    pub fn apply_append_entries_with_optional_snapshot_compact_and_publish(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<(RaftAppendEntriesCompactApplyOutcome, bool), KvError> {
        let mut outcome =
            self.apply_append_entries_with_optional_snapshot_compact(node_id, snapshot, append)?;
        let requires_shared_projection = outcome.selected_lease_mutation
            || outcome
                .committed_events
                .iter()
                .any(|event| !cluster_kv_key_can_defer_materialized_snapshot(&event.key));
        if outcome.restored_snapshot.is_none()
            && outcome.checkpoint_snapshot.is_none()
            && requires_shared_projection
        {
            let node = self.open_node(node_id)?;
            outcome.checkpoint_snapshot = Some(node.store().try_snapshot()?);
        }
        let wrote = self.publish_compact_append_entries_apply_outcome(&outcome)?;
        Ok((outcome, wrote))
    }

    pub fn append_entries_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        let node = self.open_node(node_id)?;
        Ok(RaftAppendEntriesResponse {
            term: node.hard_state().current_term,
            success: false,
            match_index: node.last_log_index(),
        })
    }

    pub fn request_vote_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let hard_state = self.wal_paths(node_id).read_persisted_hard_state()?;
        Ok(RaftRequestVoteResponse {
            term: hard_state.current_term,
            vote_granted: false,
        })
    }

    pub fn cluster_raft_vote_rejection_response(
        &self,
        state: &ClusterBootstrapState,
        detail: String,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail,
            state: state.clone(),
            vote: self.request_vote_rejection_response(&state.local_node_id)?,
        })
    }

    pub fn known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        // Read only the tiny hard-state file rather than opening the full Raft
        // node, which may replay a large WAL. Topology validity remains the
        // caller's job because KV does not interpret cluster membership.
        self.wal_paths(node_id)
            .read_persisted_hard_state()
            .map(|hard_state| hard_state.leader_id)
    }

    pub fn locked_known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_leader_id(node_id)
    }

    pub fn known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        Ok(self
            .known_leader_id(&state.local_node_id)?
            .filter(|leader_id| cluster_kv_raft_node_is_voter(state, leader_id)))
    }

    pub fn locked_known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_voter_leader_id(state)
    }

    pub fn local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        Ok(
            self.known_voter_leader_id(state)?.as_ref() == Some(&state.local_node_id)
                && cluster_kv_raft_local_is_voter(state),
        )
    }

    pub fn locked_local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_is_leader(state)
    }

    pub fn observe_term(&self, node_id: &NodeId, term: u64) -> Result<Option<KvSnapshot>, KvError> {
        let changed = self.observe_term_compact(node_id, term)?;
        if changed {
            let node = self.open_node(node_id)?;
            Ok(Some(node.store().try_snapshot()?))
        } else {
            Ok(None)
        }
    }

    pub fn observe_term_compact(&self, node_id: &NodeId, term: u64) -> Result<bool, KvError> {
        let mut node = self.open_node(node_id)?;
        let changed = node.observe_term(term)?;
        if changed {
            node.refresh_resident_read_view();
        }
        Ok(changed)
    }

    pub fn locked_observe_term_compact(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.observe_term_compact(node_id, term)
    }

    pub fn observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let snapshot = self.observe_term(node_id, term)?;
        if let Some(snapshot) = snapshot {
            self.paths
                .local_cache()
                .write_raft_snapshot_if_changed(&snapshot)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn locked_observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.observe_term_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn set_leader(&self, node_id: &NodeId, term: u64) -> Result<KvSnapshot, KvError> {
        self.set_leader_compact(node_id, term)?;
        let node = self.open_node(node_id)?;
        node.store().try_snapshot()
    }

    pub fn set_leader_compact(&self, node_id: &NodeId, term: u64) -> Result<(), KvError> {
        let mut node = self.open_node(node_id)?;
        node.become_leader(term)?;
        node.refresh_resident_read_view();
        Ok(())
    }

    pub fn locked_set_leader_compact(&self, node_id: &NodeId, term: u64) -> Result<(), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.set_leader_compact(node_id, term)
    }

    pub fn set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.set_leader(node_id, term)?;
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn clear_leader(&self, node_id: &NodeId) -> Result<KvSnapshot, KvError> {
        self.clear_leader_compact(node_id)?;
        let node = self.open_node(node_id)?;
        node.store().try_snapshot()
    }

    pub fn clear_leader_compact(&self, node_id: &NodeId) -> Result<(), KvError> {
        let mut node = self.open_node(node_id)?;
        node.clear_known_leader()?;
        node.refresh_resident_read_view();
        Ok(())
    }

    pub fn locked_clear_leader_compact(&self, node_id: &NodeId) -> Result<(), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.clear_leader_compact(node_id)
    }

    pub fn clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.clear_leader(node_id)?;
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.clear_leader_and_write_snapshot_if_changed(node_id)
    }

    pub fn append_command_with_response(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.append_request_with_response(node_id, KvCommandRequest::generated(command))
    }

    pub fn append_request_with_response(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id,
            });
        }
        if let Some(record) = node.store().committed_request(&request.request_id) {
            if record.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id,
                });
            }
            let txn_response = record.txn_response.clone();
            let committed_request = record.clone();
            return Ok(ClusterKvRaftCommitResult {
                snapshot: node.store().try_snapshot()?,
                txn_response,
                events: Vec::new(),
                committed_request,
            });
        }
        if let Err(error) = node.store().preflight_apply_command(&request.command) {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let before_revision = node.store().revision();
        let term = node.hard_state().current_term.max(1);
        let request_for_outcome = request.clone();
        let entry = match node.append_local_request(term, request) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                return Err(error);
            }
        };
        if let Err(error) = node.commit_to(entry.index) {
            let _ = mark_cluster_kv_request_unknown(
                &self.paths,
                &request_for_outcome,
                error.to_string(),
            );
            return Err(error);
        }
        let txn_response = node.txn_response_at(entry.index).cloned();
        let committed_request = node
            .store()
            .committed_request(&entry.request_id)
            .cloned()
            .ok_or(KvError::MissingCommittedEntry { index: entry.index })?;
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().try_snapshot()?;
        let events = Self::events_after_revision(&snapshot, before_revision)?;
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
            committed_request,
        })
    }

    pub fn append_command(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvSnapshot, KvError> {
        self.append_command_with_response(node_id, command)
            .map(|result| result.snapshot)
    }

    pub fn append_command_with_response_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<ClusterKvRaftCommitResult, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let (result, _) = self.append_command_with_response_and_publish_report(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )?;
        Ok(result)
    }

    pub fn append_command_with_response_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.append_command_with_response(node_id, command)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn append_command_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<KvSnapshot, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.append_command_with_response_and_publish(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )
        .map(|result| result.snapshot)
    }

    pub fn publish_commit_result<F>(
        &self,
        result: &ClusterKvRaftCommitResult,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let wrote = self.write_snapshot_after_commit(
            &result.snapshot,
            &result.events,
            key_can_defer_materialized_snapshot,
        )?;
        notify_cluster_kv_watchers_for_events(&self.paths, &result.events);
        Ok(wrote)
    }

    pub fn publish_append_entries_apply_outcome<F>(
        &self,
        outcome: &RaftAppendEntriesApplyOutcome,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        if outcome.snapshot_restored {
            let wrote =
                write_cluster_kv_authoritative_snapshot_if_changed(&self.paths, &outcome.snapshot)?;
            return Ok(wrote);
        }
        if outcome.committed_events.is_empty() {
            return Ok(false);
        }
        let wrote = self.write_snapshot_after_commit(
            &outcome.snapshot,
            &outcome.committed_events,
            key_can_defer_materialized_snapshot,
        )?;
        notify_cluster_kv_watchers_for_events(&self.paths, &outcome.committed_events);
        Ok(wrote)
    }

    /// Publishes only state that was already materialized by follower apply.
    /// Heartbeats and ordinary appends therefore perform no complete-keyspace
    /// conversion or materialized snapshot rewrite.
    pub fn publish_compact_append_entries_apply_outcome(
        &self,
        outcome: &RaftAppendEntriesCompactApplyOutcome,
    ) -> Result<bool, KvError> {
        let wrote = if let Some(snapshot) = outcome.restored_snapshot.as_ref() {
            write_cluster_kv_authoritative_snapshot_if_changed(&self.paths, snapshot)?
        } else if let Some(snapshot) = outcome.checkpoint_snapshot.as_ref() {
            self.paths
                .local_cache()
                .write_raft_snapshot_if_changed(snapshot)?
        } else {
            false
        };
        if outcome.committed_events.is_empty() && outcome.commit_advanced {
            notify_cluster_kv_watchers(&self.paths);
        } else {
            notify_cluster_kv_watchers_for_events(&self.paths, &outcome.committed_events);
        }
        Ok(wrote)
    }

    pub fn write_snapshot_after_commit<F>(
        &self,
        snapshot: &KvSnapshot,
        events: &[KvEvent],
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        if cluster_kv_events_can_defer_materialized_snapshot(
            events,
            key_can_defer_materialized_snapshot,
        ) && self.materialized_snapshot_is_fresh_enough(snapshot)
        {
            return Ok(false);
        }
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(snapshot)
    }

    fn materialized_snapshot_is_fresh_enough(&self, snapshot: &KvSnapshot) -> bool {
        let Some(existing) = self
            .paths
            .local_cache()
            .cached_snapshot_arc_if_present()
            .ok()
            .flatten()
        else {
            return false;
        };
        cluster_kv_materialized_snapshot_is_fresh_enough_against(
            existing.revision,
            snapshot.revision,
            self.materialized_snapshot_age(),
        )
    }

    fn materialized_snapshot_age(&self) -> Option<Duration> {
        cluster_kv_materialized_snapshot_age(&self.paths)
    }

    pub fn append_command_uncommitted(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        self.append_command_to_open_node(&mut node, command)
    }

    pub fn replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        let Some(record) = node.store().committed_request(&request.request_id) else {
            return Ok(None);
        };
        if record.command_digest != digest {
            return Err(KvError::RequestIdConflict {
                request_id: request.request_id.clone(),
            });
        }
        let record = record.clone();
        Ok(Some((node.store().try_snapshot()?, record)))
    }

    pub fn locked_replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.replay_request_if_committed(node_id, request)
    }

    pub fn append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let preflight = (|| {
            let digest = request.digest()?;
            if node.store().request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            if let Some(existing) = node.store().committed_request(&request.request_id)
                && existing.command_digest != digest
            {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            node.store().preflight_apply_command(&request.command)
        })();
        if let Err(error) = preflight {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let request_for_outcome = request.clone();
        match self.append_request_to_open_node(&mut node, request) {
            Ok(entry) => Ok(entry),
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                Err(error)
            }
        }
    }

    pub fn locked_append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted(node_id, request)
    }

    pub fn append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        if let Some((snapshot, record)) = self.replay_request_if_committed(node_id, &request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_request_uncommitted(node_id, request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn locked_append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted_or_replay(node_id, request)
    }

    pub fn discard_uncommitted_tail(&self, node_id: &NodeId) -> Result<bool, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let discarded = node.discard_uncommitted_tail()?;
        self.compact_node_if_needed(&mut node)?;
        Ok(discarded)
    }

    pub fn discard_uncommitted_tail_and_clear_leader(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        node.discard_uncommitted_tail()?;
        node.clear_known_leader()?;
        self.compact_node_if_needed(&mut node)?;
        node.store().try_snapshot()
    }

    pub fn discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let snapshot = self.discard_uncommitted_tail_and_clear_leader(node_id)?;
        let wrote = self
            .paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok((snapshot, wrote))
    }

    pub fn locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.discard_uncommitted_tail_and_clear_leader_and_publish_report(node_id)
    }

    pub fn commit_index_with_events(
        &self,
        node_id: &NodeId,
        commit_index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_node(node_id)?;
        let before_revision = node.store().revision();
        node.commit_to(commit_index)?;
        let txn_response = node.txn_response_at(commit_index).cloned();
        self.compact_node_if_needed(&mut node)?;
        let committed_request = node
            .log_entry_at(commit_index)
            .and_then(|entry| node.store().committed_request(&entry.request_id))
            .cloned()
            .ok_or(KvError::MissingCommittedEntry {
                index: commit_index,
            })?;
        let snapshot = node.store().try_snapshot()?;
        let events = Self::events_after_revision(&snapshot, before_revision)?;
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
            committed_request,
        })
    }

    pub fn commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.commit_index_with_events(node_id, commit_index)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn locked_commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let _raft_guard = self.lock_operation()?;
        self.commit_index_with_events_and_publish_report(
            node_id,
            commit_index,
            key_can_defer_materialized_snapshot,
        )
    }

    fn append_command_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_command(term, command)?;
        self.compact_node_if_needed(node)?;
        Ok(entry)
    }

    fn append_request_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_request(term, request)?;
        self.compact_node_if_needed(node)?;
        Ok(entry)
    }

    fn events_after_revision(
        snapshot: &KvSnapshot,
        before_revision: KvRevision,
    ) -> Result<Vec<KvEvent>, KvError> {
        let mut events = Vec::new();
        for event in snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
        {
            try_reserve_kv_items(&mut events, 1, "collect committed raft events")?;
            events.push(event.clone());
        }
        Ok(events)
    }
}

pub struct ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    store: ClusterKvRaftStore,
    node_id: NodeId,
    node: Option<DurableRaftKvNode>,
    operation_guard: Option<ClusterKvRaftOperationGuard>,
    key_can_defer_materialized_snapshot: F,
    last_snapshot_publish_wrote: Option<bool>,
}

/// Timing captured while appending one local Raft request before quorum
/// replication begins. The observation is returned to the caller so emitting
/// diagnostics remains outside the KV correctness path.
#[derive(Debug)]
pub struct ClusterKvRaftObservedAppend {
    pub result: Result<KvLogEntry, KvError>,
    pub compare_duration: Duration,
    pub log_append_wal_duration: Option<Duration>,
}

impl<F> ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    pub fn new(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> Self {
        Self {
            store,
            node_id,
            node: None,
            operation_guard: None,
            key_can_defer_materialized_snapshot,
            last_snapshot_publish_wrote: None,
        }
    }

    /// Opens the durable node once for the lifetime of this command-storage
    /// session. Quorum commits deliberately retain the node lock and restored
    /// state across replay, append, replication-request construction, and
    /// commit instead of deserializing the snapshot and replaying the WAL for
    /// every phase.
    fn node_mut_with_open_mode(
        &mut self,
        discard_uncommitted_tail: bool,
    ) -> Result<&mut DurableRaftKvNode, KvError> {
        if self.node.is_none() {
            let operation_guard = self
                .operation_guard
                .is_none()
                .then(|| self.store.lock_operation())
                .transpose()?;
            let node = if discard_uncommitted_tail {
                self.store
                    .open_resident_node_discarding_uncommitted_tail(&self.node_id)?
            } else {
                self.store.open_resident_node(&self.node_id)?
            };
            if let Some(operation_guard) = operation_guard {
                self.operation_guard = Some(operation_guard);
            }
            self.node = Some(node);
        }
        self.node.as_mut().ok_or_else(|| KvError::Io {
            path: self
                .store
                .wal_paths(&self.node_id)
                .root
                .display()
                .to_string(),
            detail: "cluster KV Raft command session did not retain its node".to_string(),
        })
    }

    fn node_mut(&mut self) -> Result<&mut DurableRaftKvNode, KvError> {
        self.node_mut_with_open_mode(false)
    }

    fn node_mut_discarding_uncommitted_tail(&mut self) -> Result<&mut DurableRaftKvNode, KvError> {
        self.node_mut_with_open_mode(true)
    }

    /// Ends the operation-scoped resident session after a durable commit.
    /// Drop the node first so its final resident-state handoff is complete
    /// before releasing the broader operation lock.
    pub fn release_session_after_commit(&mut self) {
        drop(self.node.take());
        drop(self.operation_guard.take());
    }

    pub fn local_is_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let local_node_id = state.local_node_id.clone();
        let node = self.node_mut()?;
        Ok(cluster_kv_raft_local_is_voter(state)
            && node.hard_state().leader_id.as_ref() == Some(&local_node_id))
    }

    pub fn last_log_index(&mut self) -> Result<u64, KvError> {
        self.node_mut().map(|node| node.last_log_index())
    }

    pub fn read_index_state(&mut self) -> Result<ClusterKvRaftReadIndexState, KvError> {
        self.node_mut()
            .map(|node| read_index_state_from_open_node(node))
    }

    /// Materializes a complete wire snapshot only when the ReadIndex caller
    /// actually needs one. Point and prefix callers should use the indexed
    /// helpers below and avoid this `O(total retained state)` boundary.
    pub fn read_index_snapshot(&mut self) -> Result<KvSnapshot, KvError> {
        self.node_mut()?.store().try_snapshot()
    }

    pub fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.node_mut()?
            .store()
            .get(key)
            .map(|entry| try_clone_kv_entry(entry, "read resident Raft KV entry"))
            .transpose()
    }

    /// Looks up only the bounded durable request-retention indexes needed by a
    /// request-outcome ReadIndex. A missing result deliberately says nothing
    /// about the node-local attempt ledger; callers must establish authority
    /// first and consult that ledger only after committed history is ruled out.
    pub fn read_index_request_outcome(
        &mut self,
        request_id: &KvRequestId,
    ) -> Result<Option<KvRequestOutcome>, KvError> {
        let store = self.node_mut()?.store();
        if let Some(record) = store.committed_request(request_id) {
            return Ok(Some(KvRequestOutcome::Committed(record.clone())));
        }
        if store.request_id_expired(request_id) {
            return Ok(Some(KvRequestOutcome::Expired));
        }
        Ok(None)
    }

    pub fn read_index_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.node_mut()?.store().range_prefix(prefix)
    }

    pub fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.node_mut()?
            .store()
            .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let node = self.node_mut()?;
        let request =
            build_append_entries_request_from_open_node(node, &state.local_node_id, next_index)?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: request.snapshot,
            append: request.append,
        })
    }

    /// Returns whether `next_index` normalizes to the resident node's current
    /// snapshot-plus-tail append payload. Callers may use this only while they
    /// retain the same operation-fenced command-storage session that builds
    /// the corresponding request.
    pub fn cluster_raft_append_request_uses_snapshot(
        &mut self,
        next_index: u64,
    ) -> Result<bool, KvError> {
        let node = self.node_mut()?;
        Ok(append_entries_request_uses_snapshot(node, next_index))
    }

    pub fn take_last_snapshot_publish_wrote(&mut self) -> Option<bool> {
        self.last_snapshot_publish_wrote.take()
    }

    pub fn append_uncommitted_or_replay(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        self.last_snapshot_publish_wrote = None;
        if let Some((snapshot, record)) = self.replay_if_committed(&request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_uncommitted(request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn replay_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let Some(record) = self.replay_record_if_committed(request)? else {
            return Ok(None);
        };
        let snapshot = self.read_index_snapshot()?;
        self.release_resident_node_and_operation_guard();
        Ok(Some((snapshot, record)))
    }

    pub fn replay_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, KvError> {
        self.last_snapshot_publish_wrote = None;
        let digest = request.digest()?;
        let record = {
            let node = self.node_mut_discarding_uncommitted_tail()?;
            if node.store().request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            let Some(record) = node.store().committed_request(&request.request_id) else {
                return Ok(None);
            };
            if record.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            record.clone()
        };

        // A critical commit can become durable immediately before its derived
        // cross-process snapshot write fails. Exact replay is the guaranteed
        // retry edge for that request, so repair the projection before
        // acknowledging the replay. This exceptional path may materialize the
        // resident view; ordinary first-attempt commits remain delta-sized.
        let selected_lease_mutation = committed_command_has_selected_lease_mutation(
            &request.command,
            record.txn_response.as_ref(),
        );
        let requires_shared_projection = selected_lease_mutation
            || committed_command_requires_shared_projection(
                &request.command,
                record.txn_response.as_ref(),
                &mut self.key_can_defer_materialized_snapshot,
            );
        if requires_shared_projection {
            let store = self.store.clone();
            let projection_covers_request = !selected_lease_mutation
                && store
                    .paths()
                    .local_cache()
                    .cached_snapshot_arc_if_present()?
                    .is_some_and(|snapshot| snapshot.revision >= record.revision);
            if !projection_covers_request {
                let snapshot = self
                    .node
                    .as_ref()
                    .ok_or_else(|| KvError::Io {
                        path: store.wal_paths(&self.node_id).root.display().to_string(),
                        detail: "cluster KV replay lost its resident node".to_string(),
                    })?
                    .store()
                    .try_snapshot()?;
                let wrote = store
                    .paths()
                    .local_cache()
                    .write_raft_snapshot_if_changed(&snapshot)?;
                self.last_snapshot_publish_wrote = Some(wrote);
            }
        }
        self.release_resident_node_and_operation_guard();
        Ok(Some(record))
    }

    pub(crate) fn release_resident_node_and_operation_guard(&mut self) {
        // Check the decoded node back into the resident slot before releasing
        // the wider process operation permit. A following command can then
        // acquire both without replaying durable state.
        drop(self.node.take());
        drop(self.operation_guard.take());
    }

    pub fn append_uncommitted(&mut self, request: KvCommandRequest) -> Result<KvLogEntry, KvError> {
        self.append_uncommitted_with_timing(request, false).result
    }

    pub fn append_uncommitted_observed(
        &mut self,
        request: KvCommandRequest,
    ) -> ClusterKvRaftObservedAppend {
        self.append_uncommitted_with_timing(request, true)
    }

    fn append_uncommitted_with_timing(
        &mut self,
        request: KvCommandRequest,
        capture_timing: bool,
    ) -> ClusterKvRaftObservedAppend {
        self.last_snapshot_publish_wrote = None;
        let compare_started = capture_timing.then(Instant::now);
        let preflight: Result<(), KvError> = (|| {
            let node = self.node_mut_discarding_uncommitted_tail()?;
            let digest = request.digest()?;
            if node.store().request_id_expired(&request.request_id) {
                Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                })
            } else if node
                .store()
                .committed_request(&request.request_id)
                .is_some_and(|existing| existing.command_digest != digest)
            {
                Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                })
            } else {
                node.store().preflight_apply_command(&request.command)
            }
        })();
        let compare_duration = compare_started
            .map(|started| started.elapsed())
            .unwrap_or_default();
        if let Err(error) = preflight {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                self.store.paths(),
                &request,
                error.to_string(),
            );
            return ClusterKvRaftObservedAppend {
                result: Err(error),
                compare_duration,
                log_append_wal_duration: None,
            };
        }
        let log_append_wal_started = capture_timing.then(Instant::now);
        if let Err(error) = mark_cluster_kv_request_submitted(self.store.paths(), &request) {
            return ClusterKvRaftObservedAppend {
                result: Err(error),
                compare_duration,
                log_append_wal_duration: Some(
                    log_append_wal_started
                        .map(|started| started.elapsed())
                        .unwrap_or_default(),
                ),
            };
        }
        let request_for_outcome = request.clone();
        let result: Result<KvLogEntry, KvError> = (|| {
            let term = self.node_mut()?.hard_state().current_term.max(1);
            let entry = self.node_mut()?.append_local_request(term, request)?;
            let store = self.store.clone();
            store.compact_node_if_needed(self.node_mut()?)?;
            Ok(entry)
        })();
        let log_append_wal_duration = log_append_wal_started
            .map(|started| started.elapsed())
            .unwrap_or_default();
        if let Err(error) = &result {
            let _ = mark_cluster_kv_request_unknown(
                self.store.paths(),
                &request_for_outcome,
                error.to_string(),
            );
        }
        ClusterKvRaftObservedAppend {
            result,
            compare_duration,
            log_append_wal_duration: Some(log_append_wal_duration),
        }
    }

    pub fn record_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_rejected_before_commit(
            self.store.paths(),
            request,
            failure.detail.clone(),
        )
    }

    pub fn record_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_unknown(self.store.paths(), request, detail.to_string())
    }

    pub fn discard_uncommitted_tail_and_clear_leader(&mut self) -> Result<KvSnapshot, KvError> {
        self.last_snapshot_publish_wrote = None;
        self.discard_uncommitted_tail_and_clear_leader_compact()?;
        let snapshot = self
            .node
            .as_ref()
            .ok_or_else(|| KvError::Io {
                path: self
                    .store
                    .wal_paths(&self.node_id)
                    .root
                    .display()
                    .to_string(),
                detail: "cluster KV Raft cleanup lost its resident node".to_string(),
            })?
            .store()
            .try_snapshot()?;
        let store = self.store.clone();
        let wrote = store
            .paths()
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(snapshot)
    }

    /// Removes only the uncommitted WAL suffix and durable leader hint. The
    /// committed KV state is unchanged, so quorum-failure cleanup must not
    /// clone or rewrite the complete materialized keyspace.
    pub fn discard_uncommitted_tail_and_clear_leader_compact(&mut self) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let node = self.node_mut_discarding_uncommitted_tail()?;
        node.discard_uncommitted_tail()?;
        node.clear_known_leader()?;
        node.refresh_resident_read_view();
        Ok(())
    }

    pub fn observe_term(&mut self, term: u64) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let node = self.node_mut()?;
        if node.observe_term(term)? {
            // Term/leader changes live in durable hard state. They do not
            // mutate the KV state machine and therefore need no O(N)
            // materialized snapshot rewrite.
            node.refresh_resident_read_view();
        }
        Ok(())
    }

    pub fn commit_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, KvError> {
        self.last_snapshot_publish_wrote = None;
        let store = self.store.clone();
        let (mut result, selected_lease_mutation) = {
            let node = self.node_mut()?;
            let before_revision = node.store().revision();
            node.commit_to(index)?;
            let txn_response = node.txn_response_at(index).cloned();
            let selected_lease_mutation = node.log_entry_at(index).is_some_and(|entry| {
                committed_command_has_selected_lease_mutation(&entry.command, txn_response.as_ref())
            });
            // Capture the complete command delta before periodic checkpoint
            // compaction trims retained watch history. One transaction may
            // emit more events than a checkpoint retains, but fanout and
            // committed observers must still receive every event from this
            // commit exactly once.
            let events = node.store().events_after_revision(before_revision)?;
            let final_key_states = committed_final_key_states(node.store(), &events)?;
            let checkpoint_snapshot = store.compact_node_if_needed_with_checkpoint(node)?;
            let committed_request = node
                .log_entry_at(index)
                .and_then(|entry| node.store().committed_request(&entry.request_id))
                .cloned()
                .ok_or(KvError::MissingCommittedEntry { index })?;
            (
                ClusterKvRaftCompactCommitResult {
                    txn_response,
                    events,
                    committed_request,
                    read_state: read_index_state_from_open_node(node),
                    compacted_revision: node.store().compacted_revision(),
                    checkpoint_snapshot,
                    final_key_states,
                },
                selected_lease_mutation,
            )
        };
        let requires_shared_projection = selected_lease_mutation
            || result
                .events
                .iter()
                .any(|event| !(self.key_can_defer_materialized_snapshot)(&event.key));
        if result.checkpoint_snapshot.is_none() && requires_shared_projection {
            result.checkpoint_snapshot = Some(
                self.node
                    .as_ref()
                    .ok_or_else(|| KvError::Io {
                        path: self
                            .store
                            .wal_paths(&self.node_id)
                            .root
                            .display()
                            .to_string(),
                        detail: "cluster KV Raft command session lost its resident node"
                            .to_string(),
                    })?
                    .store()
                    .try_snapshot()?,
            );
        }
        if let Some(snapshot) = result.checkpoint_snapshot.as_ref() {
            let wrote = store
                .paths()
                .local_cache()
                .write_raft_snapshot_if_changed(snapshot)?;
            self.last_snapshot_publish_wrote = Some(wrote);
        }
        if let Some(node) = self.node.as_ref() {
            node.refresh_resident_read_view();
        }
        Ok(result)
    }

    pub fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let compact = self.commit_index_compact(index)?;
        let store = self.store.clone();
        let snapshot = match compact.checkpoint_snapshot.clone() {
            Some(snapshot) => snapshot,
            None => self
                .node
                .as_ref()
                .ok_or_else(|| KvError::Io {
                    path: self
                        .store
                        .wal_paths(&self.node_id)
                        .root
                        .display()
                        .to_string(),
                    detail: "cluster KV Raft command session lost its committed node".to_string(),
                })?
                .store()
                .try_snapshot()?,
        };
        let result = ClusterKvRaftCommitResult {
            snapshot,
            txn_response: compact.txn_response,
            events: compact.events,
            committed_request: compact.committed_request,
        };
        // `commit_index_compact` already publishes any required shared or
        // periodic checkpoint. Do not immediately rewrite the identical
        // snapshot and overwrite the exact publication result. Legacy
        // deferrable callers still receive their complete return value and
        // retain the old conditional publication behavior.
        if self.last_snapshot_publish_wrote.is_none() {
            let wrote = store
                .publish_commit_result(&result, &mut self.key_can_defer_materialized_snapshot)?;
            self.last_snapshot_publish_wrote = Some(wrote);
        }
        self.release_resident_node_and_operation_guard();
        Ok(result)
    }
}

impl<F> ClusterKvRaftCommandStorage for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    type Error = KvError;

    fn kv_error(error: KvError) -> Self::Error {
        error
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error> {
        self.replay_if_committed(request)
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, Self::Error> {
        self.replay_record_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error> {
        self.append_uncommitted(request)
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error> {
        self.record_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error> {
        self.record_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), Self::Error> {
        self.discard_uncommitted_tail_and_clear_leader_compact()
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), Self::Error> {
        self.observe_term(term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
        self.commit_index_with_events(index)
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, Self::Error> {
        self.commit_index_compact(index)
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), Self::Error> {
        let snapshot_repaired = self.take_last_snapshot_publish_wrote() == Some(true);
        if let Some(commit) = commit {
            if commit.events.is_empty() {
                notify_cluster_kv_watchers(self.store.paths());
            } else {
                notify_cluster_kv_watchers_for_events(self.store.paths(), &commit.events);
            }
        } else if snapshot_repaired {
            notify_cluster_kv_watchers(self.store.paths());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot: KvSnapshot,
    pub snapshot_restored: bool,
    pub committed_events: Vec<KvEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesCompactApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot_restored: bool,
    /// The leader-supplied snapshot, retained only when it was actually
    /// installed. This is not rebuilt from the resident KV state.
    pub restored_snapshot: Option<KvSnapshot>,
    pub committed_events: Vec<KvEvent>,
    pub read_state: ClusterKvRaftReadIndexState,
    pub compacted_revision: KvRevision,
    /// Present only when this append naturally emitted a periodic checkpoint.
    pub checkpoint_snapshot: Option<KvSnapshot>,
    pub final_key_states: Vec<ClusterKvCommittedKeyState>,
    pub commit_advanced: bool,
    pub selected_lease_mutation: bool,
}

#[derive(Debug)]
pub struct DurableRaftKvNode {
    node_id: NodeId,
    pub(crate) paths: KvWalPaths,
    lock: Option<File>,
    lock_identity: FileIdentity,
    root_ino: u64,
    node: ReplicatedKvNode,
    poisoned: bool,
    cache_on_drop: bool,
    resident_slot: Option<Arc<ResidentRaftNodeSlot>>,
}

impl DurableRaftKvNode {
    pub fn open(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_nonresident_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_nonresident_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn open_resident(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_resident_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn open_resident_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_resident_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    fn open_nonresident_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut metric = KvStorageMetricGuard::start("raft_open", "sinomo.kv.raft.open.us");
        let opened = Self::open_cold(node_id, paths, discard_uncommitted_tail, lock_wait, None)?;
        #[cfg(test)]
        {
            let mut counts = NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *counts.entry(opened.paths.root.clone()).or_default() += 1;
        }
        metric.succeed();
        Ok(opened)
    }

    pub(crate) fn open_resident_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut metric = KvStorageMetricGuard::start("raft_open", "sinomo.kv.raft.open.us");
        let resident_slot = resident_raft_node_slot(&paths.root)?;
        let cached = resident_slot.checkout(&paths.root, lock_wait)?;
        // From this point the process owns the single resident writer slot.
        // An immutable cold-read image for the same root would be a redundant
        // full keyspace copy and cannot serve fresher data than this node.
        evict_cached_raft_view(&paths);
        if let Some(cached) = cached {
            if resident_raft_ownership_matches(&cached, &paths, &node_id) {
                let mut opened = Self {
                    node_id,
                    paths,
                    lock: Some(cached.lock),
                    lock_identity: cached.lock_identity,
                    root_ino: cached.root_ino,
                    node: cached.node,
                    poisoned: false,
                    cache_on_drop: true,
                    resident_slot: Some(resident_slot),
                };
                if discard_uncommitted_tail {
                    opened.discard_uncommitted_tail()?;
                }
                metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "hit").increment(1);
                #[cfg(test)]
                record_resident_raft_open_for_test(&opened.paths.root, true);
                metrics::histogram!("sinomo.kv.raft.open.log_entries")
                    .record(opened.log().len() as f64);
                opened.record_live_metrics();
                metric.succeed();
                return Ok(opened);
            }
            // Direct recovery tooling and power-loss tests may replace durable
            // files without participating in the node lock. Never serve an old
            // resident view after such a replacement.
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "invalidated")
                .increment(1);
            resident_slot.clear_read_metadata();
            drop(cached);
        }

        let opened = Self::open_cold(
            node_id,
            paths,
            discard_uncommitted_tail,
            lock_wait,
            Some(resident_slot.clone()),
        );
        if opened.is_err() {
            resident_slot.release_without_cache();
        }
        let opened = opened?;
        metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "miss").increment(1);
        #[cfg(test)]
        record_resident_raft_open_for_test(&opened.paths.root, false);
        metric.succeed();
        Ok(opened)
    }

    fn open_cold(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
        resident_slot: Option<Arc<ResidentRaftNodeSlot>>,
    ) -> Result<Self, KvError> {
        paths.ensure()?;
        let lock = open_node_lock(&paths)?;
        lock_node_exclusive(&lock, &paths.lock, lock_wait)?;
        let lock_identity = file_handle_identity(&lock, &paths.lock)?;
        let root_ino = directory_ino(&paths.root)?;
        let snapshot = read_optional_object::<RaftSnapshot>(&paths.snapshot)?;
        let hard_state = paths.read_persisted_hard_state()?;
        let mut node = if let Some(snapshot) = snapshot {
            let mut node = ReplicatedKvNode {
                store: InProcessKvStore::new(),
                log: Vec::new(),
                hard_state: hard_state.clone(),
                applied_index: 0,
                snapshot_index: 0,
                snapshot_term: 0,
                txn_responses: KvTxnResponseIndex::default(),
            };
            node.install_snapshot(snapshot)?;
            node.hard_state.current_term = hard_state.current_term;
            node.hard_state.voted_for = hard_state.voted_for;
            node.hard_state.leader_id = hard_state.leader_id;
            node.hard_state.commit_index =
                hard_state.commit_index.max(node.hard_state.commit_index);
            node
        } else {
            ReplicatedKvNode {
                store: InProcessKvStore::new(),
                log: Vec::new(),
                hard_state,
                applied_index: 0,
                snapshot_index: 0,
                snapshot_term: 0,
                txn_responses: KvTxnResponseIndex::default(),
            }
        };
        let committed_index = node.hard_state.commit_index.max(node.applied_index);
        let retain_after_index = node
            .snapshot_index
            .saturating_sub(RAFT_COMPACTION_RETAINED_ENTRIES);
        let WalRead { entries, truncated } = read_wal_entries(
            &paths.wal,
            discard_uncommitted_tail.then_some(committed_index),
            retain_after_index,
        )?;
        // Preserve the already-applied tail (kept by local_compact for incremental
        // replication) instead of discarding it as append_entries would.
        let load_truncated = node.load_persisted_entries(entries);
        let mut repaired_hard_state = false;
        let max_committable_index = node.last_log_index();
        if node.hard_state.commit_index > max_committable_index {
            eprintln!(
                "warning: repairing cluster kv hard-state commit index {} beyond local log {} in {}",
                node.hard_state.commit_index,
                max_committable_index,
                paths.root.display()
            );
            node.hard_state.commit_index = max_committable_index;
            repaired_hard_state = true;
        }
        let commit_index = node.hard_state.commit_index;
        node.commit_to(commit_index)?;
        if repaired_hard_state {
            paths.write_persisted_hard_state(&node.hard_state)?;
        }
        if truncated || load_truncated {
            rewrite_wal_entries(&paths.wal, &node.log)?;
        }
        let cache_on_drop = resident_slot.is_some();
        let opened = Self {
            node_id,
            paths,
            lock: Some(lock),
            lock_identity,
            root_ino,
            node,
            poisoned: false,
            cache_on_drop,
            resident_slot,
        };
        metrics::histogram!("sinomo.kv.raft.open.log_entries").record(opened.log().len() as f64);
        opened.record_live_metrics();
        Ok(opened)
    }

    pub async fn open_async(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || Self::open(node_id, paths))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, true, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn try_open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(
            node_id,
            paths,
            cache,
            false,
            RAFT_LOCAL_READ_LOCK_WAIT,
        )
    }

    fn open_with_materialized_cache_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        _cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        // A materialized `KvSnapshot` has no Raft log position or term. Treating
        // it as an index-zero Raft snapshot makes a fresh learner apply entries
        // 1..N on top of state that already includes those entries, inflating KV
        // revisions and potentially changing compare/transaction results.
        //
        // The materialized snapshot remains a read cache. Writable Raft state
        // may only come from the durable Raft files or an explicitly anchored
        // `RaftSnapshot { last_included_index, last_included_term, .. }` received
        // through replication. Corrupt durable state must likewise fail closed;
        // silently quarantining it and restarting at index zero loses the
        // consensus position needed for a safe recovery.
        Self::open_nonresident_inner(node_id, paths, discard_uncommitted_tail, lock_wait)
    }

    pub async fn open_async_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: KvMaterializedCache,
    ) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || {
            Self::open_with_materialized_cache(node_id, paths, &cache)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Reports whether a post-persistence apply failure made this in-memory
    /// view unsafe to reuse.
    ///
    /// The WAL and hard state remain authoritative. Drop and reopen a poisoned
    /// node to replay the committed prefix into a fresh store.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn hard_state(&self) -> &RaftHardState {
        self.assert_healthy();
        &self.node.hard_state
    }

    pub fn store(&self) -> &InProcessKvStore {
        self.assert_healthy();
        &self.node.store
    }

    fn refresh_resident_read_view(&self) {
        let Some(slot) = self.resident_slot.as_ref() else {
            return;
        };
        let Some(identity) = raft_view_identity(&self.paths) else {
            return;
        };
        let Ok(mut state) = slot.state.lock() else {
            return;
        };
        state.read_metadata = Some(ResidentRaftReadMetadata::from_open_node(self, identity));
    }

    pub(crate) fn try_into_snapshot(mut self) -> Result<KvSnapshot, KvError> {
        self.ensure_healthy()?;
        // Consuming the store is reserved for legacy ownership-transfer paths.
        // It intentionally evicts this resident handle because its indexed
        // state can no longer be reused safely.
        self.cache_on_drop = false;
        let node = std::mem::replace(&mut self.node, ReplicatedKvNode::empty());
        node.store.try_into_snapshot()
    }

    pub fn log(&self) -> &[KvLogEntry] {
        self.assert_healthy();
        &self.node.log
    }

    pub fn last_log_index(&self) -> u64 {
        self.assert_healthy();
        self.node.last_log_index()
    }

    pub fn last_log_term(&self) -> u64 {
        self.assert_healthy();
        self.node.last_log_term()
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.assert_healthy();
        self.node.term_at(index)
    }

    pub fn log_entry_at(&self, index: u64) -> Option<&KvLogEntry> {
        self.assert_healthy();
        self.node.log_entry_at(index)
    }

    pub fn applied_index(&self) -> u64 {
        self.assert_healthy();
        self.node.applied_index
    }

    pub fn snapshot_index(&self) -> u64 {
        self.assert_healthy();
        self.node.snapshot_index
    }

    pub fn txn_response_at(&self, index: u64) -> Option<&KvTxnResponse> {
        self.assert_healthy();
        self.node.txn_responses.get(index)
    }

    pub fn append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        if let Some(response) = self.node.read_only_append_entries_response(&request) {
            self.finish_resident_mutation(retain_resident);
            return Ok(response);
        }

        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        let applied_index_before = self.node.applied_index;
        let preparation = match self.node.prepare_append_entries(request) {
            Ok(preparation) => preparation,
            Err(error) => {
                self.restore_after_append_entries_persist_failure(
                    None,
                    hard_state_before,
                    log_before,
                    txn_responses_before,
                    applied_index_before,
                );
                return Err(error);
            }
        };

        let entries_to_apply = match preparation.commit_index {
            Some(commit_index) if commit_index > self.node.applied_index => {
                match self.node.entries_to_commit(commit_index) {
                    Ok(entries) => {
                        if let Err(error) = self.node.preflight_commit_entries(entries.as_slice()) {
                            self.restore_after_append_entries_persist_failure(
                                None,
                                hard_state_before,
                                log_before,
                                txn_responses_before,
                                applied_index_before,
                            );
                            return Err(error);
                        }
                        Some((commit_index, entries))
                    }
                    Err(error) => {
                        self.restore_after_append_entries_persist_failure(
                            None,
                            hard_state_before,
                            log_before,
                            txn_responses_before,
                            applied_index_before,
                        );
                        return Err(error);
                    }
                }
            }
            Some(commit_index) => {
                self.node.hard_state.commit_index =
                    self.node.hard_state.commit_index.max(commit_index);
                None
            }
            None => None,
        };

        if preparation.response.success {
            if let Err(error) = append_wal_entries(&self.paths.wal, &preparation.appended_entries) {
                self.restore_after_append_entries_persist_failure(
                    None,
                    hard_state_before,
                    log_before,
                    txn_responses_before,
                    applied_index_before,
                );
                return Err(error);
            }
        }
        if let Some((commit_index, _)) = &entries_to_apply {
            self.node.hard_state.commit_index = *commit_index;
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.restore_after_append_entries_persist_failure(
                None,
                hard_state_before,
                log_before,
                txn_responses_before,
                applied_index_before,
            );
            return Err(error);
        }
        if let Some((commit_index, entries)) = entries_to_apply {
            let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
            self.apply_persisted_commit(entries, commit_index, "append_entries")?;
            metric.succeed();
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(preparation.response)
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let (response, snapshot_restored, before_revision) =
            self.apply_append_entries_with_optional_snapshot_state(snapshot, append)?;
        let snapshot = self.store().try_snapshot()?;
        let committed_events = if snapshot_restored || snapshot.revision == before_revision {
            Vec::new()
        } else {
            ClusterKvRaftStore::events_after_revision(&snapshot, before_revision)?
        };
        Ok(RaftAppendEntriesApplyOutcome {
            response,
            snapshot,
            snapshot_restored,
            committed_events,
        })
    }

    fn apply_append_entries_with_optional_snapshot_state(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<(RaftAppendEntriesResponse, bool, KvRevision), KvError> {
        self.ensure_healthy()?;
        let before_revision = self.store().revision();
        let mut snapshot_restored = false;
        if let Some(snapshot) = snapshot {
            let applied_index = self.applied_index();
            let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
                && (append.prev_log_index != snapshot.last_included_index
                    || append.prev_log_term == snapshot.last_included_term);
            let snapshot_is_newer = snapshot.last_included_index > applied_index;
            let append_term_is_current = append.term >= self.hard_state().current_term;
            // Raft snapshots advance the committed prefix; they never arbitrate
            // between two states at the same committed index. A same-index
            // mismatch is a safety violation or durable corruption and must be
            // rejected by AppendEntries without overwriting local committed
            // state.
            if append_term_is_current && snapshot_matches_append && snapshot_is_newer {
                self.restore_snapshot(snapshot)?;
                snapshot_restored = true;
            }
        }
        let response = self.append_entries(append)?;
        Ok((response, snapshot_restored, before_revision))
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        let response = match self.node.handle_request_vote(request) {
            Ok(response) => response,
            Err(error) => {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
        };
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(response)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<bool, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        if !self.node.observe_term(term) {
            self.finish_resident_mutation(retain_resident);
            return Ok(false);
        }
        if let Err(error) = self.persist_hard_state() {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(true)
    }

    pub fn become_leader(&mut self, term: u64) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        // Leadership was won for exactly `term`; a higher local term means a
        // concurrent vote or append advanced the node past this election and
        // claiming leadership for the newer term would not be backed by a
        // quorum of votes.
        if self.node.hard_state.current_term > term {
            return Err(KvError::StaleLeadershipTerm {
                term,
                current_term: self.node.hard_state.current_term,
            });
        }
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.current_term = term;
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        self.append_local_request(term, KvCommandRequest::generated(command))
    }

    pub fn append_local_request(
        &mut self,
        term: u64,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            request_id: request.request_id,
            command: request.command,
            revision: KvRevision::ZERO,
        };
        self.node.hard_state.current_term = term;
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.node.append_entries(vec![entry.clone()]) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        if let Err(error) = append_wal_entries(&self.paths.wal, std::slice::from_ref(&entry)) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(entry)
    }

    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        if !self.node.discard_uncommitted_tail() {
            self.finish_resident_mutation(retain_resident);
            return Ok(false);
        }
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            self.node.txn_responses = txn_responses_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            self.node.txn_responses = txn_responses_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(true)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.assert_healthy();
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
        let hard_state_before = self.node.hard_state.clone();
        if commit_index <= self.node.applied_index {
            if let Err(error) = self.node.commit_to(commit_index) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            self.record_live_metrics();
            metric.succeed();
            self.finish_resident_mutation(retain_resident);
            return Ok(());
        }

        let entries = self.node.entries_to_commit(commit_index)?;
        self.node.preflight_commit_entries(entries.as_slice())?;
        self.node.hard_state.commit_index = commit_index;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        let result = self.apply_persisted_commit(entries, commit_index, "commit_to");
        if result.is_ok() {
            self.record_live_metrics();
            metric.succeed();
            self.finish_resident_mutation(retain_resident);
        }
        result
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let mut metric =
            KvStorageMetricGuard::start("snapshot_write", "sinomo.kv.snapshot.write.us");
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: self.node.store.try_snapshot()?,
        };
        write_object_file(&self.paths.snapshot, &snapshot)?;
        // Local compaction keeps a tail of recent committed entries in memory for
        // incremental replication; the snapshot file still records the full state at
        // applied_index for cold recovery.
        self.node.local_compact(RAFT_COMPACTION_RETAINED_ENTRIES);
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        let written_bytes = fs::metadata(&self.paths.snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        metrics::counter!("sinomo.kv.snapshot.write.bytes").increment(written_bytes);
        self.record_live_metrics();
        metric.succeed();
        self.finish_resident_mutation(retain_resident);
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let mut metric =
            KvStorageMetricGuard::start("snapshot_restore", "sinomo.kv.snapshot.restore.us");
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot)?;
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        self.record_live_metrics();
        metric.succeed();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        self.paths.write_persisted_hard_state(&self.node.hard_state)
    }

    fn apply_persisted_commit(
        &mut self,
        entries: CommitEntries,
        commit_index: u64,
        phase: &'static str,
    ) -> Result<(), KvError> {
        // The commit index is durable before this method runs. If applying the
        // committed entries fails, rolling only part of the in-memory store
        // backward would be unsafe and cloning the whole keyspace before every
        // commit would be prohibitively expensive. Poison this handle instead;
        // reopening deterministically rebuilds it from the durable prefix.
        #[cfg(test)]
        let result =
            if let Some(fail_after) = take_cluster_kv_raft_apply_failure_for_test(&self.paths) {
                self.node.apply_preflighted_commit_failing_after_for_test(
                    entries,
                    commit_index,
                    fail_after,
                )
            } else {
                self.node.apply_preflighted_commit(entries, commit_index)
            };
        #[cfg(not(test))]
        let result = self.node.apply_preflighted_commit(entries, commit_index);
        if result.is_err() {
            self.poisoned = true;
            metrics::counter!("sinomo.kv.raft.poisoned_sessions", "phase" => phase).increment(1);
        }
        result
    }

    fn ensure_healthy(&self) -> Result<(), KvError> {
        if self.poisoned {
            return Err(KvError::RaftSessionPoisoned);
        }
        Ok(())
    }

    /// Makes resident reuse fail closed across a mutation. The caller restores
    /// reuse only after every durability step and in-memory apply has succeeded;
    /// any early error therefore drops the cached node and forces a cold replay.
    fn begin_resident_mutation(&mut self) -> bool {
        let retain_on_success = self.resident_slot.is_some() && self.cache_on_drop;
        self.cache_on_drop = false;
        retain_on_success
    }

    fn finish_resident_mutation(&mut self, retain_on_success: bool) {
        if retain_on_success && !self.poisoned {
            self.cache_on_drop = true;
        }
    }

    fn assert_healthy(&self) {
        assert!(
            !self.poisoned,
            "durable raft node session is poisoned; drop and reopen the node"
        );
    }

    fn persist_hard_state_if_changed(&self, before: &RaftHardState) -> Result<(), KvError> {
        if self.node.hard_state == *before {
            return Ok(());
        }
        self.persist_hard_state()
    }

    fn record_live_metrics(&self) {
        record_cluster_kv_raft_live_metrics(
            &self.paths.root,
            self.node.hard_state.current_term,
            self.node.hard_state.leader_id.clone(),
            self.node.hard_state.commit_index,
            self.node.applied_index,
            self.node.last_log_index(),
            self.node.snapshot_index,
            self.node.log.len(),
        );
    }

    fn restore_after_append_entries_persist_failure(
        &mut self,
        node_before: Option<ReplicatedKvNode>,
        hard_state_before: RaftHardState,
        log_before: Vec<KvLogEntry>,
        txn_responses_before: KvTxnResponseIndex,
        applied_index_before: u64,
    ) {
        if let Some(node_before) = node_before {
            self.node = node_before;
            return;
        }
        self.node.hard_state = hard_state_before;
        self.node.log = log_before;
        self.node.txn_responses = txn_responses_before;
        self.node.applied_index = applied_index_before;
    }
}

impl Drop for DurableRaftKvNode {
    fn drop(&mut self) {
        let Some(resident_slot) = self.resident_slot.clone() else {
            return;
        };
        let Some(lock) = self.lock.take() else {
            resident_slot.release_without_cache();
            return;
        };
        if self.poisoned || !self.cache_on_drop || std::thread::panicking() {
            drop(lock);
            resident_slot.release_without_cache();
            return;
        }
        let Some(identity) = raft_view_identity(&self.paths) else {
            drop(lock);
            resident_slot.release_without_cache();
            return;
        };
        if file_identity(&self.paths.lock) != Some(self.lock_identity)
            || file_handle_identity(&lock, &self.paths.lock).ok() != Some(self.lock_identity)
            || directory_ino(&self.paths.root).ok() != Some(self.root_ino)
        {
            drop(lock);
            resident_slot.release_without_cache();
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "ownership_replaced")
                .increment(1);
            return;
        }
        let node = std::mem::replace(&mut self.node, ReplicatedKvNode::empty());
        resident_slot.checkin(ResidentRaftNode {
            node_id: self.node_id.clone(),
            identity,
            lock_identity: self.lock_identity,
            root_ino: self.root_ino,
            lock,
            node,
        });
    }
}

impl From<&ClusterBootstrapState> for ClusterKvRaftConfigRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        let mut voters = Vec::new();
        let mut learners = Vec::new();
        let mut non_voting = Vec::new();
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            match member.state {
                ClusterMemberState::Voter => voters.push(member.node_id.clone()),
                ClusterMemberState::Learner => learners.push(member.node_id.clone()),
                ClusterMemberState::NonVoting => non_voting.push(member.node_id.clone()),
                ClusterMemberState::Active => match member.role {
                    ClusterMemberRole::Voter => voters.push(member.node_id.clone()),
                    ClusterMemberRole::Learner => learners.push(member.node_id.clone()),
                    ClusterMemberRole::NonVoting => non_voting.push(member.node_id.clone()),
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
        Self {
            cluster_id: state.cluster_id.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            voters,
            learners,
            non_voting,
            updated_at: state.updated_at,
        }
    }
}
