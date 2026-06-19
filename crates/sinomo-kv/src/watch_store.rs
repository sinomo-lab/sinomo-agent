#[cfg(any(test, feature = "test-support"))]
use crate::internal::*;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterFileMark {
    exists: bool,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_file_mark(path: &PathBuf) -> Option<ClusterFileMark> {
    match fs::metadata(path) {
        Ok(metadata) => Some(ClusterFileMark {
            exists: true,
            len: metadata.len(),
            modified: metadata.modified().ok(),
            ino: file_ino(&metadata),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Some(ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        }),
        Err(_) => None,
    }
}

#[cfg(any(test, feature = "test-support"))]
const MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES: usize = 128;
#[cfg(any(test, feature = "test-support"))]
const MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES: usize = crate::MAX_CLUSTER_KV_ACTIVE_WATCHES;
#[cfg(any(test, feature = "test-support"))]
const MAX_CLUSTER_KV_WATCH_ALLOCATOR_TRUST_CACHE_ENTRIES: usize = 4096;

#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_EVENT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchEventCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_ALLOCATOR_TRUST: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_DRAIN_READ_PASSES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST: LazyLock<
    Mutex<HashMap<PathBuf, u64>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
struct ClusterKvWatchEventCacheEntry {
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
    last_used: u64,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
struct ClusterKvWatchRegistrationCacheEntry {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
    last_used: u64,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterKvWatchEmptyDrainMark {
    generation: crate::ClusterKvWatchObservedGeneration,
    reader_node_id: NodeId,
    materialized_snapshot: ClusterFileMark,
    raft_hard_state: ClusterFileMark,
    raft_wal: ClusterFileMark,
    raft_snapshot: ClusterFileMark,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterKvWatchAllocatorTrustEntry {
    allocator_mark: ClusterFileMark,
    watch_dir_mark: ClusterFileMark,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
    last_used: u64,
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_event_cache_marks(
    paths: &ClusterKvPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    Some((
        cluster_file_mark(&paths.watch_events)?,
        cluster_file_mark(&paths.watch_events_log)?,
    ))
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_empty_drain_mark(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    generation: crate::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchEmptyDrainMark> {
    let raft_paths = paths.raft_wal_paths(node_id);
    Some(ClusterKvWatchEmptyDrainMark {
        generation,
        reader_node_id: node_id.clone(),
        materialized_snapshot: cluster_file_mark(&paths.snapshot)?,
        raft_hard_state: cluster_file_mark(&raft_paths.hard_state)?,
        raft_wal: cluster_file_mark(&raft_paths.wal)?,
        raft_snapshot: cluster_file_mark(&raft_paths.snapshot)?,
    })
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_cache_tick() -> u64 {
    CLUSTER_KV_WATCH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

#[cfg(any(test, feature = "test-support"))]
fn evict_cluster_kv_watch_event_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchEventCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES {
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

#[cfg(any(test, feature = "test-support"))]
fn evict_cluster_kv_watch_registration_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected)
        || cache.len() < MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES
    {
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

#[cfg(any(test, feature = "test-support"))]
fn evict_cluster_kv_watch_allocator_trust_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected)
        || cache.len() < MAX_CLUSTER_KV_WATCH_ALLOCATOR_TRUST_CACHE_ENTRIES
    {
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

#[cfg(any(test, feature = "test-support"))]
fn current_cluster_kv_watch_allocator_trust_marks(
    paths: &ClusterKvPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    let allocator_mark = cluster_file_mark(&paths.watches_next_id)?;
    if !allocator_mark.exists {
        return None;
    }
    let watch_dir_mark = cluster_file_mark(&paths.watches)?;
    if !watch_dir_mark.exists {
        return None;
    }
    Some((allocator_mark, watch_dir_mark))
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_allocator_trusted(paths: &ClusterKvPaths) -> bool {
    let Some((allocator_mark, watch_dir_mark)) =
        current_cluster_kv_watch_allocator_trust_marks(paths)
    else {
        return false;
    };
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return false;
    };
    let Some(entry) = cache.get_mut(&paths.watches_next_id) else {
        return false;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.watches_next_id);
        return false;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    true
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_active_watch_registrations(
    paths: &ClusterKvPaths,
) -> Option<Vec<ClusterKvWatchRegistration>> {
    let (allocator_mark, watch_dir_mark) = current_cluster_kv_watch_allocator_trust_marks(paths)?;
    let mut cache = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock().ok()?;
    let entry = cache.get_mut(&paths.watches_next_id)?;
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.watches_next_id);
        return None;
    }
    let registrations = entry.active_registrations.clone()?;
    if entry.active_watch_count != Some(registrations.len()) {
        cache.remove(&paths.watches_next_id);
        return None;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    Some(registrations)
}

#[cfg(any(test, feature = "test-support"))]
fn refresh_cached_cluster_kv_active_watch_registration(
    paths: &ClusterKvPaths,
    registration: &ClusterKvWatchRegistration,
) {
    let Some((allocator_mark, watch_dir_mark)) =
        current_cluster_kv_watch_allocator_trust_marks(paths)
    else {
        return;
    };
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return;
    };
    let Some(entry) = cache.get_mut(&paths.watches_next_id) else {
        return;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.watches_next_id);
        return;
    }
    let Some(registrations) = entry.active_registrations.as_mut() else {
        return;
    };
    if let Some(cached) = registrations
        .iter_mut()
        .find(|cached| cached.watch_id == registration.watch_id)
    {
        *cached = registration.clone();
        entry.last_used = cluster_kv_watch_cache_tick();
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_event_append_lock(paths: &ClusterKvPaths) -> Arc<Mutex<()>> {
    let key = cluster_kv_watch_notify_key(paths);
    let mut locks = CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS
        .lock()
        .expect("cluster kv watch event append locks");
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_watch_events_arc(
    paths: &ClusterKvPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<Arc<Vec<KvEvent>>> {
    cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log).map(|entry| entry.events)
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_watch_event_cache_entry(
    paths: &ClusterKvPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<ClusterKvWatchEventCacheEntry> {
    let key = cluster_kv_watch_notify_key(paths);
    let mut cache = CLUSTER_KV_WATCH_EVENT_CACHE.lock().ok()?;
    let entry = cache.get_mut(&key)?;
    if entry.checkpoint == checkpoint && entry.log == log {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.clone())
    } else {
        None
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_event_append_cache_view(
    paths: &ClusterKvPaths,
) -> Option<crate::ClusterKvWatchEventAppendCacheView> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    let entry = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log)?;
    Some(crate::ClusterKvWatchEventAppendCacheView {
        events: entry.events.as_ref().clone(),
        log_records: entry.log_records,
        log_bytes: log.len,
        append_safe: entry.append_safe,
    })
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_event_cache(
    paths: &ClusterKvPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Vec<KvEvent>,
    log_records: usize,
    append_safe: bool,
) {
    record_cluster_kv_watch_event_cache_arc(
        paths,
        checkpoint,
        log,
        Arc::new(events),
        log_records,
        append_safe,
    );
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_event_cache_arc(
    paths: &ClusterKvPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
) {
    let key = cluster_kv_watch_notify_key(paths);
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        evict_cluster_kv_watch_event_cache_entry_if_needed(&mut cache, &key);
        cache.insert(
            key,
            ClusterKvWatchEventCacheEntry {
                checkpoint,
                log,
                events,
                log_records,
                append_safe,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
fn record_current_cluster_kv_watch_event_cache(
    paths: &ClusterKvPaths,
    events: Vec<KvEvent>,
    log_records: usize,
    append_safe: bool,
) {
    if let Some((checkpoint, log)) = cluster_kv_watch_event_cache_marks(paths) {
        record_cluster_kv_watch_event_cache(
            paths,
            checkpoint,
            log,
            events,
            log_records,
            append_safe,
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_watch_registration_entry(
    path: &PathBuf,
    mark: ClusterFileMark,
) -> Option<ClusterKvWatchRegistrationCacheEntry> {
    let mut cache = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock().ok()?;
    let entry = cache.get_mut(path)?;
    if entry.mark == mark {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.clone())
    } else {
        None
    }
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_registration_cache(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
) {
    record_cluster_kv_watch_registration_cache_with_durable(
        path,
        mark,
        registration.clone(),
        registration.next_revision,
    );
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_registration_cache_with_durable(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
) {
    record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
        path,
        mark,
        registration,
        durable_next_revision,
        None,
    );
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        evict_cluster_kv_watch_registration_cache_entry_if_needed(&mut cache, path);
        cache.insert(
            path.clone(),
            ClusterKvWatchRegistrationCacheEntry {
                mark,
                registration,
                durable_next_revision,
                empty_drain_mark,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_registration_empty_drain_cache(
    path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    registration: ClusterKvWatchRegistration,
    empty_drain_mark: ClusterKvWatchEmptyDrainMark,
) {
    record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
        path,
        registration_read.mark,
        registration,
        registration_read.durable_next_revision,
        Some(empty_drain_mark),
    );
}

#[cfg(any(test, feature = "test-support"))]
fn record_current_cluster_kv_watch_registration_cache(
    path: &PathBuf,
    registration: ClusterKvWatchRegistration,
) {
    if let Some(mark) = cluster_file_mark(path)
        && mark.exists
    {
        record_cluster_kv_watch_registration_cache(path, mark, registration);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn invalidate_cluster_kv_watch_registration_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn record_current_cluster_kv_watch_allocator_trust(
    paths: &ClusterKvPaths,
    active_watch_count: Option<usize>,
) {
    record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
        paths,
        active_watch_count,
        None,
    );
}

#[cfg(any(test, feature = "test-support"))]
fn record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
    paths: &ClusterKvPaths,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
) {
    let Some(allocator_mark) = cluster_file_mark(&paths.watches_next_id) else {
        return;
    };
    if !allocator_mark.exists {
        return;
    }
    let Some(watch_dir_mark) = cluster_file_mark(&paths.watches) else {
        return;
    };
    if !watch_dir_mark.exists {
        return;
    }
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(&mut cache, &paths.watches_next_id);
        cache.insert(
            paths.watches_next_id.clone(),
            ClusterKvWatchAllocatorTrustEntry {
                allocator_mark,
                watch_dir_mark,
                active_watch_count,
                active_registrations,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cached_cluster_kv_watch_events_for_test(paths: &ClusterKvPaths) -> Option<Vec<KvEvent>> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    cached_cluster_kv_watch_events_arc(paths, checkpoint, log).map(|events| events.as_ref().clone())
}

#[cfg(any(test, feature = "test-support"))]
pub fn cached_cluster_kv_watch_registration_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> Option<ClusterKvWatchRegistration> {
    let path = cluster_kv_watch_path(paths, watch_id);
    let mark = cluster_file_mark(&path)?;
    cached_cluster_kv_watch_registration_entry(&path, mark).map(|entry| entry.registration)
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_drain_once_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        *calls.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_drain_read_pass_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_READ_PASSES_FOR_TEST.lock() {
        *calls.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        calls.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST
        .lock()
        .ok()
        .and_then(|calls| calls.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_drain_read_passes_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_READ_PASSES_FOR_TEST.lock() {
        calls.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_drain_read_passes_for_test(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_DRAIN_READ_PASSES_FOR_TEST
        .lock()
        .ok()
        .and_then(|calls| calls.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_event_log_read_for_test(paths: &ClusterKvPaths, bytes: u64) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        *reads.entry(cluster_kv_watch_notify_key(paths)).or_default() += bytes;
    }
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_event_log_write_for_test(paths: &ClusterKvPaths, bytes: u64) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        *writes
            .entry(cluster_kv_watch_notify_key(paths))
            .or_default() += bytes;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        reads.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        writes.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|writes| writes.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_dir_scans_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        scans.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_dir_scans_for_test(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST
        .lock()
        .ok()
        .and_then(|scans| scans.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_dir_scan_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        *scans.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_cluster_kv_watch_allocator_persist_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() {
        *failures.entry(paths.watches_next_id.clone()).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
fn consume_cluster_kv_watch_allocator_persist_failure_for_test(paths: &ClusterKvPaths) -> bool {
    let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() else {
        return false;
    };
    let Some(remaining) = failures.get_mut(&paths.watches_next_id) else {
        return false;
    };
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        failures.remove(&paths.watches_next_id);
    }
    true
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_event_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_registration_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_cluster_kv_watch_event_cache_for_test(paths: &ClusterKvPaths) {
    record_cluster_kv_watch_event_cache(
        paths,
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        Vec::new(),
        0,
        true,
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn refresh_cluster_kv_watch_event_cache_for_test(paths: &ClusterKvPaths) -> bool {
    cached_cluster_kv_watch_event_cache_entry(
        paths,
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
    )
    .is_some()
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_event_cache_contains_for_test(paths: &ClusterKvPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_EVENT_CACHE
        .lock()
        .map(|cache| cache.contains_key(&key))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_cluster_kv_watch_event_cache_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) {
    record_cluster_kv_watch_registration_cache(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
        ClusterKvWatchRegistration {
            watch_id: WatchId(1),
            prefix: "/cache/test/".to_string(),
            next_revision: KvRevision(1),
        },
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn refresh_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) -> bool {
    cached_cluster_kv_watch_registration_entry(
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
pub fn cluster_kv_watch_registration_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_WATCH_REGISTRATION_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_cluster_kv_watch_registration_cache_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(&mut cache, &paths.watches_next_id);
        cache.insert(
            paths.watches_next_id.clone(),
            ClusterKvWatchAllocatorTrustEntry {
                allocator_mark: ClusterFileMark {
                    exists: true,
                    len: 1,
                    modified: Some(SystemTime::UNIX_EPOCH),
                    ino: 1,
                },
                watch_dir_mark: ClusterFileMark {
                    exists: true,
                    len: 1,
                    modified: Some(SystemTime::UNIX_EPOCH),
                    ino: 1,
                },
                active_watch_count: Some(0),
                active_registrations: Some(Vec::new()),
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn refresh_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterKvPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .ok()
        .and_then(|mut cache| {
            cache.get_mut(&paths.watches_next_id).map(|entry| {
                entry.last_used = cluster_kv_watch_cache_tick();
            })
        })
        .is_some()
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_allocator_trust_contains_for_test(paths: &ClusterKvPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .map(|cache| cache.contains_key(&paths.watches_next_id))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_notify_key(paths: &ClusterKvPaths) -> PathBuf {
    paths.root.clone()
}

fn notify_cluster_kv_watchers_for_events(paths: &ClusterKvPaths, events: &[KvEvent]) {
    crate::notify_cluster_kv_watchers_for_events(paths, events);
}

#[cfg(any(test, feature = "test-support"))]
pub fn create_cluster_kv_watch(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id.clone())?;
    let allocator_trusted = cluster_kv_watch_allocator_trusted(paths);
    let report = crate::create_cluster_kv_watch_registration(
        paths,
        request,
        snapshot.compacted_revision,
        allocator_trusted,
        crate::MAX_CLUSTER_KV_ACTIVE_WATCHES,
    )?;
    if let Some(scan) = report.scan.as_ref() {
        record_cluster_kv_watch_scan_observation(paths, scan);
    }
    if let Err(error) = persist_cluster_kv_watch_allocator_state(paths, &report.allocator) {
        let _ = crate::remove_cluster_kv_watch_registration_file_if_exists(&report.watch_path);
        invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
        return Err(error);
    }
    record_current_cluster_kv_watch_registration_cache(
        &report.watch_path,
        report.registration.clone(),
    );
    Ok(report.registration)
}

#[cfg(any(test, feature = "test-support"))]
pub fn drain_cluster_kv_watch(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            node_id,
            request.watch_id,
            crate::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    }
    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            node_id,
            request.watch_id,
            crate::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response = drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            node_id.clone(),
            request.watch_id,
            crate::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        )?;
        if !response.events.is_empty() {
            return Ok(response);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(response);
        }
        if !crate::wait_for_cluster_kv_watch_change_for_watch(
            paths,
            request.watch_id,
            observed_generation,
            remaining,
        ) {
            return Ok(response);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_watch_empty_drain_response(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    watch_path: &PathBuf,
    watch_id: WatchId,
    observed_generation: crate::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchDrainResponse> {
    let mark = cluster_file_mark(watch_path)?;
    if !mark.exists {
        return None;
    }
    let entry = cached_cluster_kv_watch_registration_entry(watch_path, mark)?;
    if entry.registration.watch_id != watch_id {
        return None;
    }
    let current_mark = cluster_kv_watch_empty_drain_mark(paths, node_id, observed_generation)?;
    if entry.empty_drain_mark != Some(current_mark) {
        return None;
    }
    Some(ClusterKvWatchDrainResponse {
        watch_id: entry.registration.watch_id,
        events: Vec::new(),
        next_revision: entry.registration.next_revision,
    })
}

#[cfg(any(test, feature = "test-support"))]
pub fn drain_cluster_kv_watch_once_limited_by_payload_bytes(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    watch_id: WatchId,
    max_payload_bytes: usize,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    record_cluster_kv_watch_drain_once_for_test(paths);

    let watch_path = cluster_kv_watch_path(paths, watch_id);
    let observed_generation = cluster_kv_watch_observed_generation(paths, watch_id);
    if let Some(response) = cached_cluster_kv_watch_empty_drain_response(
        paths,
        &node_id,
        &watch_path,
        watch_id,
        observed_generation,
    ) {
        return Ok(response);
    }
    record_cluster_kv_watch_drain_read_pass_for_test(paths);
    let empty_drain_mark_before_read =
        cluster_kv_watch_empty_drain_mark(paths, &node_id, observed_generation);
    let registration_read = read_cluster_kv_watch_registration_with_cursor(&watch_path, watch_id)?;
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id.clone())?;
    let persisted_events = cluster_kv_watch_events_arc(paths)?;
    let (registration_file_unchanged, registration_file_age) =
        cluster_kv_watch_registration_cursor_observation(&watch_path, &registration_read);
    let report = match crate::drain_cluster_kv_watch_registration(
        registration_read.registration.clone(),
        registration_read.durable_next_revision,
        persisted_events.as_ref(),
        &snapshot,
        max_payload_bytes,
        registration_file_unchanged,
        registration_file_age,
    ) {
        Ok(report) => report,
        Err(error @ KvError::Compacted { .. }) => {
            remove_cluster_kv_watch_registration_with_allocator(paths, watch_id)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    if !report.response.events.is_empty() {
        update_cluster_kv_watch_registration_after_drain(
            paths,
            &watch_path,
            &registration_read,
            &report,
        )?;
    } else if let Some(empty_drain_mark) = empty_drain_mark_before_read
        && cluster_kv_watch_empty_drain_mark(paths, &node_id, observed_generation)
            == Some(empty_drain_mark.clone())
    {
        record_cluster_kv_watch_registration_empty_drain_cache(
            &watch_path,
            &registration_read,
            report.registration.clone(),
            empty_drain_mark,
        );
    }
    Ok(report.response)
}

#[cfg(any(test, feature = "test-support"))]
fn update_cluster_kv_watch_registration_after_drain(
    paths: &ClusterKvPaths,
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    report: &crate::ClusterKvWatchDrainRegistrationReport,
) -> Result<(), KvError> {
    let persistence = crate::persist_cluster_kv_watch_drain_cursor(watch_path, report)?;
    match persistence {
        crate::ClusterKvWatchDrainCursorPersistence::Persisted { registration } => {
            record_current_cluster_kv_watch_registration_cache(watch_path, registration.clone());
        }
        crate::ClusterKvWatchDrainCursorPersistence::CacheOnly {
            registration,
            durable_next_revision,
        } => {
            record_cluster_kv_watch_registration_cache_with_durable(
                watch_path,
                registration_read.mark,
                registration.clone(),
                durable_next_revision,
            );
        }
        crate::ClusterKvWatchDrainCursorPersistence::Unchanged { .. } => {}
    };
    refresh_cached_cluster_kv_active_watch_registration(paths, &report.registration);
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_registration_cursor_observation(
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
) -> (bool, Option<Duration>) {
    let current_mark = cluster_file_mark(watch_path);
    let registration_file_unchanged = current_mark
        .as_ref()
        .is_some_and(|mark| *mark == registration_read.mark);
    let registration_file_age = current_mark
        .and_then(|mark| mark.modified)
        .and_then(|modified| SystemTime::now().duration_since(modified).ok());
    (registration_file_unchanged, registration_file_age)
}

#[cfg(any(test, feature = "test-support"))]
pub fn append_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
) -> Result<usize, KvError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    let append_lock = cluster_kv_watch_event_append_lock(paths);
    let _append_guard = append_lock.lock().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: format!("append mutex poisoned: {source}"),
    })?;
    if cached_cluster_kv_watch_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if cached_cluster_kv_snapshot_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if persisted_cluster_kv_watch_events_contain_all(paths, incoming)? {
        return Ok(0);
    }
    let cache = cluster_kv_watch_event_append_cache_view(paths);
    let report = crate::append_cluster_kv_watch_events_to_storage_with_observers(
        paths,
        incoming,
        cache.as_ref(),
        |read| observe_cluster_kv_watch_event_log_read(paths, read),
        |written| observe_cluster_kv_watch_event_log_write(paths, written),
    )?;
    record_current_cluster_kv_watch_event_cache(
        paths,
        report.retained,
        report.log_records,
        report.append_safe,
    );
    if report.appended.is_empty() {
        return Ok(0);
    }
    notify_cluster_kv_watchers_for_events(paths, &report.appended);
    Ok(report.appended.len())
}

#[cfg(any(test, feature = "test-support"))]
pub fn append_cluster_kv_watch_events_for_active_watches(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    incoming: &[KvEvent],
) -> Result<usize, KvError> {
    if incoming.is_empty() || (!paths.watches.exists() && !paths.watches_next_id.exists()) {
        return Ok(0);
    }
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id.clone())?;
    let registrations = if let Some(registrations) =
        cached_cluster_kv_active_watch_registrations(paths)
    {
        registrations
    } else {
        let active =
            crate::cluster_kv_active_watch_registration_report(paths, snapshot.compacted_revision)?;
        if let Some(scan) = active.scan.as_ref() {
            record_cluster_kv_watch_scan_observation(paths, scan);
        }
        if let Some(allocator) = active.allocator.as_ref() {
            let registrations = active
                .scan
                .as_ref()
                .map(|scan| {
                    scan.retained_registrations
                        .iter()
                        .map(|retained| retained.registration.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            persist_cluster_kv_watch_allocator_state(paths, allocator)?;
            record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
                paths,
                allocator.active_watch_count,
                Some(registrations.clone()),
            );
            registrations
        } else {
            Vec::new()
        }
    };
    if registrations.is_empty() {
        return Ok(0);
    }
    let incoming = crate::cluster_kv_watch_events_matching_registrations(&registrations, incoming);
    if incoming.is_empty()
        || incoming
            .iter()
            .all(|event| crate::cluster_kv_events_contain(&snapshot.events, event))
    {
        return Ok(0);
    }
    append_cluster_kv_watch_events(paths, &incoming)
}

#[cfg(any(test, feature = "test-support"))]
fn persisted_cluster_kv_watch_events_contain_all(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
) -> Result<bool, KvError> {
    let Some(log_mark) = cluster_file_mark(&paths.watch_events_log) else {
        return Ok(false);
    };
    if log_mark.exists {
        return Ok(false);
    }
    let events = cluster_kv_watch_events_arc(paths)?;
    let still_no_log = cluster_file_mark(&paths.watch_events_log).is_some_and(|mark| !mark.exists);
    Ok(still_no_log
        && incoming
            .iter()
            .all(|event| crate::cluster_kv_events_contain(events.as_ref(), event)))
}

#[cfg(any(test, feature = "test-support"))]
pub fn cached_cluster_kv_watch_events_contain_all(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
) -> bool {
    let Some((checkpoint, log)) = cluster_kv_watch_event_cache_marks(paths) else {
        return false;
    };
    let Some(entry) = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log) else {
        return false;
    };
    if !entry.append_safe {
        return false;
    }
    incoming
        .iter()
        .all(|event| crate::cluster_kv_events_contain(entry.events.as_ref(), event))
}

#[cfg(any(test, feature = "test-support"))]
fn cached_cluster_kv_snapshot_events_contain_all(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
) -> bool {
    paths
        .local_cache()
        .cached_snapshot_arc_if_present()
        .is_ok_and(|snapshot| {
            snapshot.is_some_and(|snapshot| {
                incoming
                    .iter()
                    .all(|event| crate::cluster_kv_events_contain(&snapshot.events, event))
            })
        })
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_events(paths: &ClusterKvPaths) -> Result<Vec<KvEvent>, KvError> {
    cluster_kv_watch_events_arc(paths).map(|events| events.as_ref().clone())
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_events_arc(paths: &ClusterKvPaths) -> Result<Arc<Vec<KvEvent>>, KvError> {
    let cache_marks = cluster_kv_watch_event_cache_marks(paths);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        if let Some(events) = cached_cluster_kv_watch_events_arc(paths, checkpoint_mark, log_mark) {
            return Ok(events);
        }
    }
    let read = crate::read_cluster_kv_watch_events_with_observer(paths, |read| {
        observe_cluster_kv_watch_event_log_read(paths, read)
    });
    let read = read?;
    let log_records = read.log_records;
    let append_safe = read.append_safe;
    let events = Arc::new(read.events);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        record_cluster_kv_watch_event_cache_arc(
            paths,
            checkpoint_mark,
            log_mark,
            events.clone(),
            log_records,
            append_safe,
        );
    }
    Ok(events)
}

#[cfg(any(test, feature = "test-support"))]
fn observe_cluster_kv_watch_event_log_read(paths: &ClusterKvPaths, read: u64) {
    record_cluster_kv_watch_event_log_read_for_test(paths, read);
}

#[cfg(any(test, feature = "test-support"))]
fn observe_cluster_kv_watch_event_log_write(paths: &ClusterKvPaths, written: u64) {
    record_cluster_kv_watch_event_log_write_for_test(paths, written);
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_watch_path(paths: &ClusterKvPaths, watch_id: WatchId) -> PathBuf {
    paths.watch_path(watch_id)
}

#[cfg(any(test, feature = "test-support"))]
struct ClusterKvWatchRegistrationRead {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
}

#[cfg(any(test, feature = "test-support"))]
fn read_cluster_kv_watch_registration_with_cursor(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistrationRead, KvError> {
    let Some(mark) = cluster_file_mark(path) else {
        return read_cluster_kv_watch_registration_from_file(path, watch_id).map(|registration| {
            ClusterKvWatchRegistrationRead {
                mark: ClusterFileMark {
                    exists: true,
                    len: 0,
                    modified: None,
                    ino: 0,
                },
                durable_next_revision: registration.next_revision,
                registration,
            }
        });
    };
    if !mark.exists {
        return Err(KvError::UnknownWatch { watch_id });
    }
    if let Some(entry) = cached_cluster_kv_watch_registration_entry(path, mark) {
        if entry.registration.watch_id == watch_id {
            return Ok(ClusterKvWatchRegistrationRead {
                mark,
                registration: entry.registration,
                durable_next_revision: entry.durable_next_revision,
            });
        }
        invalidate_cluster_kv_watch_registration_cache(path);
        remove_cluster_kv_watch_file(path);
        return Err(KvError::UnknownWatch { watch_id });
    }
    read_cluster_kv_watch_registration_from_file(path, watch_id).map(|registration| {
        ClusterKvWatchRegistrationRead {
            mark,
            durable_next_revision: registration.next_revision,
            registration,
        }
    })
}

#[cfg(any(test, feature = "test-support"))]
fn read_cluster_kv_watch_registration_from_file(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let registration = match crate::read_cluster_kv_watch_registration_file(path, watch_id) {
        Ok(registration) => registration,
        Err(KvError::UnknownWatch { watch_id }) => {
            remove_cluster_kv_watch_file(path);
            return Err(KvError::UnknownWatch { watch_id });
        }
        Err(error) => return Err(error),
    };
    record_current_cluster_kv_watch_registration_cache(path, registration.clone());
    Ok(registration)
}

#[cfg(any(test, feature = "test-support"))]
fn remove_cluster_kv_watch_file(path: &PathBuf) {
    if let Err(error) = remove_cluster_kv_watch_file_if_exists(path) {
        eprintln!(
            "warning: could not remove stale cluster kv watch registration {}: {}",
            path.display(),
            error
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
fn remove_cluster_kv_watch_file_if_exists(path: &PathBuf) -> Result<bool, KvError> {
    let removed = crate::remove_cluster_kv_watch_registration_file_if_exists(path)?;
    invalidate_cluster_kv_watch_registration_cache(path);
    Ok(removed)
}

#[cfg(any(test, feature = "test-support"))]
fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> Result<bool, KvError> {
    let report = crate::remove_cluster_kv_watch_registration_with_allocator(paths, watch_id)?;
    invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
    if let Some(allocator) = report.allocator {
        persist_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    Ok(report.removed)
}

#[cfg(any(test, feature = "test-support"))]
fn persist_cluster_kv_watch_allocator_state(
    paths: &ClusterKvPaths,
    state: &crate::ClusterKvWatchAllocatorState,
) -> Result<(), KvError> {
    if consume_cluster_kv_watch_allocator_persist_failure_for_test(paths) {
        return Err(KvError::Io {
            path: paths.watches_next_id.display().to_string(),
            detail: "injected cluster kv watch allocator persist failure".to_string(),
        });
    }
    crate::write_cluster_kv_watch_allocator_state(paths, state)?;
    record_current_cluster_kv_watch_allocator_trust(paths, state.active_watch_count);
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
fn record_cluster_kv_watch_scan_observation(
    paths: &ClusterKvPaths,
    scan: &crate::ClusterKvWatchScanResult,
) {
    if scan.scanned {
        record_cluster_kv_watch_dir_scan_for_test(paths);
    }
    for removed_path in &scan.removed_paths {
        invalidate_cluster_kv_watch_registration_cache(removed_path);
    }
    for retained in &scan.retained_registrations {
        record_current_cluster_kv_watch_registration_cache(
            &retained.path,
            retained.registration.clone(),
        );
    }
}
