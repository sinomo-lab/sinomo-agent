use crate::ClusterPaths;
use crate::error::NetworkError;
#[cfg(test)]
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
#[cfg(test)]
use crate::io_observer::io_observer;
#[cfg(test)]
use crate::kv_local_store::{
    cluster_raft_wal_paths, read_cluster_kv_cached_snapshot_if_present,
    read_cluster_kv_current_snapshot_arc, read_cluster_kv_reader_node_id,
};
use crate::storage::read_cluster_state;

#[cfg(test)]
use sinomo_kv::{
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration, KvError,
    KvEvent, KvRevision, WatchId,
};
use sinomo_kv::{ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(test)]
use std::time::{Duration, Instant, SystemTime};

#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES: usize = 128;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES: usize =
    sinomo_kv::MAX_CLUSTER_KV_ACTIVE_WATCHES;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_ALLOCATOR_TRUST_CACHE_ENTRIES: usize = 4096;

#[cfg(test)]
static CLUSTER_KV_WATCH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchEventCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_ALLOCATOR_TRUST: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST: LazyLock<
    Mutex<HashMap<PathBuf, u64>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
#[derive(Clone)]
struct ClusterKvWatchEventCacheEntry {
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
    last_used: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct ClusterKvWatchRegistrationCacheEntry {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
    last_used: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterKvWatchEmptyDrainMark {
    generation: sinomo_kv::ClusterKvWatchObservedGeneration,
    state: ClusterFileMark,
    materialized_snapshot: ClusterFileMark,
    raft_hard_state: ClusterFileMark,
    raft_wal: ClusterFileMark,
    raft_snapshot: ClusterFileMark,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterKvWatchAllocatorTrustEntry {
    allocator_mark: ClusterFileMark,
    watch_dir_mark: ClusterFileMark,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
    last_used: u64,
}

#[cfg(test)]
fn cluster_kv_watch_event_cache_marks(
    paths: &ClusterPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    Some((
        cluster_file_mark(&paths.kv_watch_events)?,
        cluster_file_mark(&paths.kv_watch_events_log)?,
    ))
}

#[cfg(test)]
fn cluster_kv_watch_empty_drain_mark(
    paths: &ClusterPaths,
    generation: sinomo_kv::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchEmptyDrainMark> {
    let node_id = read_cluster_kv_reader_node_id(paths).ok()?;
    let raft_paths = cluster_raft_wal_paths(paths, &node_id);
    Some(ClusterKvWatchEmptyDrainMark {
        generation,
        state: cluster_file_mark(&paths.state)?,
        materialized_snapshot: cluster_file_mark(&paths.kv_snapshot)?,
        raft_hard_state: cluster_file_mark(&raft_paths.hard_state)?,
        raft_wal: cluster_file_mark(&raft_paths.wal)?,
        raft_snapshot: cluster_file_mark(&raft_paths.snapshot)?,
    })
}

#[cfg(test)]
fn cluster_kv_watch_cache_tick() -> u64 {
    CLUSTER_KV_WATCH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn current_cluster_kv_watch_allocator_trust_marks(
    paths: &ClusterPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    let allocator_mark = cluster_file_mark(&paths.kv_watches_next_id)?;
    if !allocator_mark.exists {
        return None;
    }
    let watch_dir_mark = cluster_file_mark(&paths.kv_watches)?;
    if !watch_dir_mark.exists {
        return None;
    }
    Some((allocator_mark, watch_dir_mark))
}

#[cfg(test)]
fn cluster_kv_watch_allocator_trusted(paths: &ClusterPaths) -> bool {
    let Some((allocator_mark, watch_dir_mark)) =
        current_cluster_kv_watch_allocator_trust_marks(paths)
    else {
        return false;
    };
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return false;
    };
    let Some(entry) = cache.get_mut(&paths.kv_watches_next_id) else {
        return false;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
        return false;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    true
}

#[cfg(test)]
fn cached_cluster_kv_active_watch_registrations(
    paths: &ClusterPaths,
) -> Option<Vec<ClusterKvWatchRegistration>> {
    let (allocator_mark, watch_dir_mark) = current_cluster_kv_watch_allocator_trust_marks(paths)?;
    let mut cache = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock().ok()?;
    let entry = cache.get_mut(&paths.kv_watches_next_id)?;
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
        return None;
    }
    let registrations = entry.active_registrations.clone()?;
    if entry.active_watch_count != Some(registrations.len()) {
        cache.remove(&paths.kv_watches_next_id);
        return None;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    Some(registrations)
}

#[cfg(test)]
fn refresh_cached_cluster_kv_active_watch_registration(
    paths: &ClusterPaths,
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
    let Some(entry) = cache.get_mut(&paths.kv_watches_next_id) else {
        return;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
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

#[cfg(test)]
fn cluster_kv_watch_event_append_lock(paths: &ClusterPaths) -> Arc<Mutex<()>> {
    let key = cluster_kv_watch_notify_key(paths);
    let mut locks = CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS
        .lock()
        .expect("cluster kv watch event append locks");
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[cfg(test)]
fn cached_cluster_kv_watch_events_arc(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<Arc<Vec<KvEvent>>> {
    cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log).map(|entry| entry.events)
}

#[cfg(test)]
fn cached_cluster_kv_watch_event_cache_entry(
    paths: &ClusterPaths,
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

#[cfg(test)]
fn cluster_kv_watch_event_append_cache_view(
    paths: &ClusterPaths,
) -> Option<sinomo_kv::ClusterKvWatchEventAppendCacheView> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    let entry = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log)?;
    Some(sinomo_kv::ClusterKvWatchEventAppendCacheView {
        events: entry.events.as_ref().clone(),
        log_records: entry.log_records,
        log_bytes: log.len,
        append_safe: entry.append_safe,
    })
}

#[cfg(test)]
fn record_cluster_kv_watch_event_cache(
    paths: &ClusterPaths,
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

#[cfg(test)]
fn record_cluster_kv_watch_event_cache_arc(
    paths: &ClusterPaths,
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

#[cfg(test)]
fn record_current_cluster_kv_watch_event_cache(
    paths: &ClusterPaths,
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn invalidate_cluster_kv_watch_registration_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
fn record_current_cluster_kv_watch_allocator_trust(
    paths: &ClusterPaths,
    active_watch_count: Option<usize>,
) {
    record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
        paths,
        active_watch_count,
        None,
    );
}

#[cfg(test)]
fn record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
    paths: &ClusterPaths,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
) {
    let Some(allocator_mark) = cluster_file_mark(&paths.kv_watches_next_id) else {
        return;
    };
    if !allocator_mark.exists {
        return;
    }
    let Some(watch_dir_mark) = cluster_file_mark(&paths.kv_watches) else {
        return;
    };
    if !watch_dir_mark.exists {
        return;
    }
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(
            &mut cache,
            &paths.kv_watches_next_id,
        );
        cache.insert(
            paths.kv_watches_next_id.clone(),
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

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_events_for_test(
    paths: &ClusterPaths,
) -> Option<Vec<KvEvent>> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    cached_cluster_kv_watch_events_arc(paths, checkpoint, log).map(|events| events.as_ref().clone())
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_registration_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> Option<ClusterKvWatchRegistration> {
    let path = cluster_kv_watch_path(paths, watch_id);
    let mark = cluster_file_mark(&path)?;
    cached_cluster_kv_watch_registration_entry(&path, mark).map(|entry| entry.registration)
}

#[cfg(test)]
fn record_cluster_kv_watch_drain_once_for_test(paths: &ClusterPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        *calls.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        calls.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST
        .lock()
        .ok()
        .and_then(|calls| calls.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_watch_event_log_read_for_test(paths: &ClusterPaths, bytes: u64) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        *reads.entry(cluster_kv_watch_notify_key(paths)).or_default() += bytes;
    }
}

#[cfg(test)]
fn record_cluster_kv_watch_event_log_write_for_test(paths: &ClusterPaths, bytes: u64) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        *writes
            .entry(cluster_kv_watch_notify_key(paths))
            .or_default() += bytes;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        reads.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterPaths) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        writes.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|writes| writes.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_dir_scans_for_test(paths: &ClusterPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        scans.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_dir_scans_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST
        .lock()
        .ok()
        .and_then(|scans| scans.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_watch_dir_scan_for_test(paths: &ClusterPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        *scans.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn fail_next_cluster_kv_watch_allocator_persist_for_test(paths: &ClusterPaths) {
    if let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() {
        *failures
            .entry(paths.kv_watches_next_id.clone())
            .or_default() += 1;
    }
}

#[cfg(test)]
fn consume_cluster_kv_watch_allocator_persist_failure_for_test(paths: &ClusterPaths) -> bool {
    let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() else {
        return false;
    };
    let Some(remaining) = failures.get_mut(&paths.kv_watches_next_id) else {
        return false;
    };
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        failures.remove(&paths.kv_watches_next_id);
    }
    true
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_registration_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_event_cache_for_test(paths: &ClusterPaths) {
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

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_event_cache_for_test(paths: &ClusterPaths) -> bool {
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

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_cache_contains_for_test(paths: &ClusterPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_EVENT_CACHE
        .lock()
        .map(|cache| cache.contains_key(&key))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_event_cache_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) {
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

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) -> bool {
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

#[cfg(test)]
pub(crate) fn cluster_kv_watch_registration_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_WATCH_REGISTRATION_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_registration_cache_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(
            &mut cache,
            &paths.kv_watches_next_id,
        );
        cache.insert(
            paths.kv_watches_next_id.clone(),
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

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .ok()
        .and_then(|mut cache| {
            cache.get_mut(&paths.kv_watches_next_id).map(|entry| {
                entry.last_used = cluster_kv_watch_cache_tick();
            })
        })
        .is_some()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_allocator_trust_contains_for_test(paths: &ClusterPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .map(|cache| cache.contains_key(&paths.kv_watches_next_id))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test(root: &Path) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
fn cluster_kv_watch_notify_key(paths: &ClusterPaths) -> PathBuf {
    paths.root.clone()
}

#[cfg(test)]
fn cluster_kv_watch_generation(paths: &ClusterPaths) -> u64 {
    sinomo_kv::cluster_kv_watch_generation(&paths.kv_paths())
}

#[cfg(test)]
fn cluster_kv_watch_observed_generation(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> sinomo_kv::ClusterKvWatchObservedGeneration {
    sinomo_kv::cluster_kv_watch_observed_generation(&paths.kv_paths(), watch_id)
}

#[cfg(test)]
fn notify_cluster_kv_watchers_for_events(paths: &ClusterPaths, events: &[KvEvent]) {
    sinomo_kv::notify_cluster_kv_watchers_for_events(&paths.kv_paths(), events);
}

#[cfg(test)]
fn wait_for_cluster_kv_watch_change(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    sinomo_kv::wait_for_cluster_kv_watch_change(&paths.kv_paths(), observed_generation, timeout)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    sinomo_kv::cluster_kv_watch_notifier_capacity_for_test()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_generation_for_test(paths: &ClusterPaths) -> u64 {
    cluster_kv_watch_generation(paths)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterPaths) -> bool {
    sinomo_kv::cluster_kv_watch_notifier_has_key_for_test(&paths.kv_paths())
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &Path) -> bool {
    sinomo_kv::cluster_kv_watch_notifier_has_raw_key_for_test(path)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterPaths) -> usize {
    sinomo_kv::cluster_kv_watch_notifier_waiter_count_for_test(&paths.kv_paths())
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &Path) {
    sinomo_kv::remove_cluster_kv_watch_notifier_entries_under_root_for_test(root);
}

#[cfg(test)]
pub(crate) fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterPaths) {
    sinomo_kv::touch_cluster_kv_watch_notifier_for_test(&paths.kv_paths());
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_notifier_waiter_key_for_test(path: PathBuf) {
    sinomo_kv::record_cluster_kv_watch_notifier_waiter_key_for_test(path);
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) {
    sinomo_kv::reset_cluster_kv_watch_unchanged_wakeups_for_test(&paths.kv_paths(), watch_id);
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> u64 {
    sinomo_kv::cluster_kv_watch_unchanged_wakeups_for_test(&paths.kv_paths(), watch_id)
}

#[cfg(test)]
pub(crate) fn wait_for_cluster_kv_watch_change_for_test(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    wait_for_cluster_kv_watch_change(paths, observed_generation, timeout)
}

pub fn handle_cluster_kv_watch_events(
    paths: &ClusterPaths,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    sinomo_kv::handle_cluster_kv_watch_events(&paths.kv_paths(), &state, request)
        .map_err(Into::into)
}

#[cfg(test)]
pub fn create_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let allocator_trusted = cluster_kv_watch_allocator_trusted(paths);
    let report = sinomo_kv::create_cluster_kv_watch_registration(
        &paths.kv_paths(),
        request,
        snapshot.compacted_revision,
        allocator_trusted,
        sinomo_kv::MAX_CLUSTER_KV_ACTIVE_WATCHES,
    )?;
    if let Some(scan) = report.scan.as_ref() {
        record_cluster_kv_watch_scan_observation(paths, scan);
    }
    if let Err(error) = persist_cluster_kv_watch_allocator_state(paths, &report.allocator) {
        let _ = sinomo_kv::remove_cluster_kv_watch_registration_file_if_exists(&report.watch_path);
        invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
        return Err(error);
    }
    record_current_cluster_kv_watch_registration_cache(
        &report.watch_path,
        report.registration.clone(),
    );
    Ok(report.registration)
}

#[cfg(test)]
pub fn drain_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    }
    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response = drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        )?;
        if !response.events.is_empty() {
            return Ok(response);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(response);
        }
        if !sinomo_kv::wait_for_cluster_kv_watch_change_for_watch(
            &paths.kv_paths(),
            request.watch_id,
            observed_generation,
            remaining,
        ) {
            return Ok(response);
        }
    }
}

#[cfg(test)]
fn cached_cluster_kv_watch_empty_drain_response(
    paths: &ClusterPaths,
    watch_path: &PathBuf,
    watch_id: WatchId,
    observed_generation: sinomo_kv::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchDrainResponse> {
    let mark = cluster_file_mark(watch_path)?;
    if !mark.exists {
        return None;
    }
    let entry = cached_cluster_kv_watch_registration_entry(watch_path, mark)?;
    if entry.registration.watch_id != watch_id {
        return None;
    }
    let current_mark = cluster_kv_watch_empty_drain_mark(paths, observed_generation)?;
    if entry.empty_drain_mark != Some(current_mark) {
        return None;
    }
    Some(ClusterKvWatchDrainResponse {
        watch_id: entry.registration.watch_id,
        events: Vec::new(),
        next_revision: entry.registration.next_revision,
    })
}

#[cfg(test)]
pub(crate) fn drain_cluster_kv_watch_once_limited_by_payload_bytes(
    paths: &ClusterPaths,
    watch_id: WatchId,
    max_payload_bytes: usize,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    record_cluster_kv_watch_drain_once_for_test(paths);

    let watch_path = cluster_kv_watch_path(paths, watch_id);
    let observed_generation = cluster_kv_watch_observed_generation(paths, watch_id);
    if let Some(response) = cached_cluster_kv_watch_empty_drain_response(
        paths,
        &watch_path,
        watch_id,
        observed_generation,
    ) {
        return Ok(response);
    }
    let empty_drain_mark_before_read =
        cluster_kv_watch_empty_drain_mark(paths, observed_generation);
    let registration_read = read_cluster_kv_watch_registration_with_cursor(&watch_path, watch_id)?;
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let persisted_events = cluster_kv_watch_events_arc(paths)?;
    let (registration_file_unchanged, registration_file_age) =
        cluster_kv_watch_registration_cursor_observation(&watch_path, &registration_read);
    let report = match sinomo_kv::drain_cluster_kv_watch_registration(
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
            return Err(NetworkError::Kv(error));
        }
        Err(error) => return Err(error.into()),
    };
    if !report.response.events.is_empty() {
        update_cluster_kv_watch_registration_after_drain(
            paths,
            &watch_path,
            &registration_read,
            &report,
        )?;
    } else if let Some(empty_drain_mark) = empty_drain_mark_before_read
        && cluster_kv_watch_empty_drain_mark(paths, observed_generation) == Some(empty_drain_mark)
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

#[cfg(test)]
fn update_cluster_kv_watch_registration_after_drain(
    paths: &ClusterPaths,
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    report: &sinomo_kv::ClusterKvWatchDrainRegistrationReport,
) -> Result<(), NetworkError> {
    let persistence = sinomo_kv::persist_cluster_kv_watch_drain_cursor(watch_path, report)?;
    match persistence {
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::Persisted { registration } => {
            record_current_cluster_kv_watch_registration_cache(watch_path, registration.clone());
        }
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::CacheOnly {
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
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::Unchanged { .. } => {}
    };
    refresh_cached_cluster_kv_active_watch_registration(paths, &report.registration);
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
pub fn cancel_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, NetworkError> {
    let removed = remove_cluster_kv_watch_registration_with_allocator(paths, request.watch_id)?;
    if removed {
        sinomo_kv::notify_cluster_kv_watchers(&paths.kv_paths());
    }
    Ok(ClusterKvWatchCancelResponse {
        watch_id: request.watch_id,
        cancelled: removed,
    })
}

#[cfg(test)]
pub fn append_cluster_kv_watch_events(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    let append_lock = cluster_kv_watch_event_append_lock(paths);
    let _append_guard = append_lock.lock().map_err(|source| NetworkError::Io {
        operation: "cluster kv watch event append".to_string(),
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
    let report = sinomo_kv::append_cluster_kv_watch_events_to_storage_with_observers(
        &paths.kv_paths(),
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

#[cfg(test)]
pub fn append_cluster_kv_watch_events_for_active_watches(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() || (!paths.kv_watches.exists() && !paths.kv_watches_next_id.exists()) {
        return Ok(0);
    }
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let registrations =
        if let Some(registrations) = cached_cluster_kv_active_watch_registrations(paths) {
            registrations
        } else {
            let active = sinomo_kv::cluster_kv_active_watch_registration_report(
                &paths.kv_paths(),
                snapshot.compacted_revision,
            )?;
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
    let incoming =
        sinomo_kv::cluster_kv_watch_events_matching_registrations(&registrations, incoming);
    if incoming.is_empty()
        || incoming
            .iter()
            .all(|event| sinomo_kv::cluster_kv_events_contain(&snapshot.events, event))
    {
        return Ok(0);
    }
    append_cluster_kv_watch_events(paths, &incoming)
}

#[cfg(test)]
fn persisted_cluster_kv_watch_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<bool, NetworkError> {
    let Some(log_mark) = cluster_file_mark(&paths.kv_watch_events_log) else {
        return Ok(false);
    };
    if log_mark.exists {
        return Ok(false);
    }
    let events = cluster_kv_watch_events_arc(paths)?;
    let still_no_log =
        cluster_file_mark(&paths.kv_watch_events_log).is_some_and(|mark| !mark.exists);
    Ok(still_no_log
        && incoming
            .iter()
            .all(|event| sinomo_kv::cluster_kv_events_contain(events.as_ref(), event)))
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_events_contain_all(
    paths: &ClusterPaths,
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
        .all(|event| sinomo_kv::cluster_kv_events_contain(entry.events.as_ref(), event))
}

#[cfg(test)]
fn cached_cluster_kv_snapshot_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> bool {
    read_cluster_kv_cached_snapshot_if_present(paths).is_ok_and(|snapshot| {
        snapshot.is_some_and(|snapshot| {
            incoming
                .iter()
                .all(|event| sinomo_kv::cluster_kv_events_contain(&snapshot.events, event))
        })
    })
}

#[cfg(test)]
pub fn cluster_kv_watch_events(paths: &ClusterPaths) -> Result<Vec<KvEvent>, NetworkError> {
    cluster_kv_watch_events_arc(paths).map(|events| events.as_ref().clone())
}

#[cfg(test)]
fn cluster_kv_watch_events_arc(paths: &ClusterPaths) -> Result<Arc<Vec<KvEvent>>, NetworkError> {
    let cache_marks = cluster_kv_watch_event_cache_marks(paths);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        if let Some(events) = cached_cluster_kv_watch_events_arc(paths, checkpoint_mark, log_mark) {
            return Ok(events);
        }
    }
    let read = sinomo_kv::read_cluster_kv_watch_events_with_observer(&paths.kv_paths(), |read| {
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

#[cfg(test)]
fn observe_cluster_kv_watch_event_log_read(paths: &ClusterPaths, read: u64) {
    record_cluster_kv_watch_event_log_read_for_test(paths, read);
    if let Some(observer) = io_observer() {
        observer.on_disk_read(read);
    }
}

#[cfg(test)]
fn observe_cluster_kv_watch_event_log_write(paths: &ClusterPaths, written: u64) {
    record_cluster_kv_watch_event_log_write_for_test(paths, written);
    if let Some(observer) = io_observer() {
        observer.on_disk_write(written);
    }
}

#[cfg(test)]
fn cluster_kv_watch_path(paths: &ClusterPaths, watch_id: WatchId) -> PathBuf {
    paths.kv_paths().watch_path(watch_id)
}

#[cfg(test)]
struct ClusterKvWatchRegistrationRead {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
}

#[cfg(test)]
fn read_cluster_kv_watch_registration_with_cursor(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistrationRead, NetworkError> {
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
        return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
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
        return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
    }
    read_cluster_kv_watch_registration_from_file(path, watch_id).map(|registration| {
        ClusterKvWatchRegistrationRead {
            mark,
            durable_next_revision: registration.next_revision,
            registration,
        }
    })
}

#[cfg(test)]
fn read_cluster_kv_watch_registration_from_file(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let registration = match sinomo_kv::read_cluster_kv_watch_registration_file(path, watch_id) {
        Ok(registration) => registration,
        Err(KvError::UnknownWatch { watch_id }) => {
            remove_cluster_kv_watch_file(path);
            return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
        }
        Err(error) => return Err(error.into()),
    };
    record_current_cluster_kv_watch_registration_cache(path, registration.clone());
    Ok(registration)
}

#[cfg(test)]
fn remove_cluster_kv_watch_file(path: &PathBuf) {
    if let Err(error) = remove_cluster_kv_watch_file_if_exists(path) {
        eprintln!(
            "warning: could not remove stale cluster kv watch registration {}: {}",
            path.display(),
            error
        );
    }
}

#[cfg(test)]
fn remove_cluster_kv_watch_file_if_exists(path: &PathBuf) -> Result<bool, NetworkError> {
    let removed = sinomo_kv::remove_cluster_kv_watch_registration_file_if_exists(path)?;
    invalidate_cluster_kv_watch_registration_cache(path);
    Ok(removed)
}

#[cfg(test)]
fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> Result<bool, NetworkError> {
    let report = sinomo_kv::remove_cluster_kv_watch_registration_with_allocator(
        &paths.kv_paths(),
        watch_id,
    )?;
    invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
    if let Some(allocator) = report.allocator {
        persist_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    Ok(report.removed)
}

#[cfg(test)]
fn persist_cluster_kv_watch_allocator_state(
    paths: &ClusterPaths,
    state: &sinomo_kv::ClusterKvWatchAllocatorState,
) -> Result<(), NetworkError> {
    if consume_cluster_kv_watch_allocator_persist_failure_for_test(paths) {
        return Err(NetworkError::StorageIo {
            path: paths.kv_watches_next_id.display().to_string(),
            detail: "injected cluster kv watch allocator persist failure".to_string(),
        });
    }
    sinomo_kv::write_cluster_kv_watch_allocator_state(&paths.kv_paths(), state)?;
    record_current_cluster_kv_watch_allocator_trust(paths, state.active_watch_count);
    Ok(())
}

#[cfg(test)]
fn record_cluster_kv_watch_scan_observation(
    paths: &ClusterPaths,
    scan: &sinomo_kv::ClusterKvWatchScanResult,
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
