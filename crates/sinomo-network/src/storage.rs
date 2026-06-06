use crate::error::NetworkError;
use crate::lan::local_network_cache_key;
use crate::object::SinomoObjectCodec;
use crate::prelude::*;
use crate::{
    discover_default_public_ip_candidates_for_local_ips, discover_local_ip_candidates_for_targets,
    format_ip_port, is_invitable_ip, is_loopback_connect_address,
};
use fs2::FileExt;
use sinomo_kv::cluster_member_key;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

pub(crate) const MAX_CLUSTER_KV_WATCH_EVENTS: usize = 256;
pub(crate) const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG: u64 =
    (MAX_CLUSTER_KV_WATCH_EVENTS / 2) as u64;
const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS / 2;
const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE: Duration = Duration::from_secs(60);
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS;
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES: usize =
    sinomo_kv::MAX_KV_COMMAND_PAYLOAD_BYTES;
pub(crate) const MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES: usize =
    sinomo_kv::MAX_KV_COMMAND_PAYLOAD_BYTES;
pub(crate) const MAX_CLUSTER_KV_ACTIVE_WATCHES: usize = 1024;
pub const CLUSTER_KV_SNAPSHOT_RPC_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES: usize = 4096;
pub const CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES: usize =
    sinomo_kv::MAX_KV_COMMAND_PAYLOAD_BYTES;
pub(crate) const MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub(crate) const MAX_CLUSTER_KV_MATERIALIZED_EVENTS: usize = 64;
pub(crate) const MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS: usize = 64;
const MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
const CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG: u64 = 64;
const CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL: Duration = Duration::from_secs(300);
const MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS: usize = MAX_CLUSTER_KV_WATCH_EVENTS * 2;
const MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES: u64 = 1024 * 1024;
pub(crate) const MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_CLUSTER_KV_WATCH_DRAIN_WAIT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES: usize = MAX_CLUSTER_KV_ACTIVE_WATCHES * 2 + 128;
const MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES: usize = 128;
const MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES: usize = MAX_CLUSTER_KV_ACTIVE_WATCHES;
pub(crate) const PUBLIC_ENDPOINT_REACHABILITY_POSITIVE_CACHE_TTL: Duration =
    Duration::from_secs(60);
pub(crate) const PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL: Duration =
    Duration::from_secs(10);
pub(crate) const MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES: usize = 128;
const CLUSTER_NETWORK_EXPIRY_GRACE_SECONDS: i64 = 600;
const CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES: u16 = 3;
pub const CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS: i64 = 60;
pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = 128 * 1024 * 1024;
const FILE_CACHE_CAPACITY: usize = 4096;
const MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES: usize = 128;
static OBJECT_FILE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static OBJECT_FILE_WRITE_MARK_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_WATCH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_WATCH_NOTIFIER_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_RECORDS: LazyLock<
    Mutex<HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_WATCH_NOTIFIERS: LazyLock<Mutex<HashMap<PathBuf, ClusterKvWatchNotifierEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_WATCH_EVENT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchEventCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvActiveWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_WATCH_ALLOCATOR_TRUST: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_READER_NODE_ID_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>>,
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
static CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST: AtomicU64 =
    AtomicU64::new(MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES);
static PUBLIC_ENDPOINT_REACHABILITY_CACHE: LazyLock<Mutex<PublicEndpointReachabilityCache>> =
    LazyLock::new(|| Mutex::new(PublicEndpointReachabilityCache::default()));

fn cluster_kv_raft_snapshot_byte_limit() -> u64 {
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
pub(crate) fn set_cluster_kv_raft_snapshot_byte_limit_for_test(limit: u64) -> u64 {
    CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.swap(limit, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterFileMark {
    exists: bool,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

#[derive(Clone)]
struct ClusterKvWatchEventCacheEntry {
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
    last_used: u64,
}

#[derive(Clone)]
struct ClusterKvWatchRegistrationCacheEntry {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
    last_used: u64,
}

#[derive(Clone)]
struct ClusterKvActiveWatchRegistrationCacheEntry {
    watch_dir_mark: ClusterFileMark,
    registrations: Arc<Vec<ClusterKvWatchRegistration>>,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterKvWatchEmptyDrainMark {
    generation: ClusterKvWatchObservedGeneration,
    state: ClusterFileMark,
    materialized_snapshot: ClusterFileMark,
    raft_hard_state: ClusterFileMark,
    raft_wal: ClusterFileMark,
    raft_snapshot: ClusterFileMark,
}

struct ClusterKvWatchNotifierEntry {
    generation: u64,
    last_used: u64,
    waiters: usize,
    changed: Arc<Condvar>,
}

impl Default for ClusterKvWatchNotifierEntry {
    fn default() -> Self {
        Self {
            generation: 0,
            last_used: 0,
            waiters: 0,
            changed: Arc::new(Condvar::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterKvWatchObservedGeneration {
    root: u64,
    watch: u64,
}

#[derive(Clone)]
struct LocalClusterConnectivityRecordCacheEntry {
    records: Vec<ClusterConnectivityRecord>,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterKvWatchAllocatorTrustEntry {
    allocator_mark: ClusterFileMark,
    watch_dir_mark: ClusterFileMark,
    active_watch_count: Option<usize>,
    last_used: u64,
}

#[derive(Clone)]
struct ClusterKvReaderNodeIdCacheEntry {
    mark: ClusterFileMark,
    node_id: NodeId,
    last_used: u64,
}

#[derive(Clone, Debug)]
struct PublicEndpointReachabilityCacheEntry {
    checked_at: Instant,
    reachable: bool,
}

#[derive(Clone, Debug, Default)]
struct PublicEndpointReachabilityCache {
    entries: HashMap<PublicEndpointReachabilityCacheKey, PublicEndpointReachabilityCacheEntry>,
    expiry_index: BTreeMap<Instant, BTreeSet<PublicEndpointReachabilityCacheKey>>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PublicEndpointReachabilityCacheKey {
    address: SocketAddr,
    local_network: Vec<IpAddr>,
}

impl PublicEndpointReachabilityCacheEntry {
    fn ttl(&self) -> Duration {
        if self.reachable {
            PUBLIC_ENDPOINT_REACHABILITY_POSITIVE_CACHE_TTL
        } else {
            PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL
        }
    }

    fn is_fresh_at(&self, now: Instant) -> bool {
        now.duration_since(self.checked_at) < self.ttl()
    }

    fn expires_at(&self) -> Instant {
        self.checked_at
            .checked_add(self.ttl())
            .unwrap_or(self.checked_at)
    }
}

impl PublicEndpointReachabilityCache {
    fn get_fresh(&self, key: &PublicEndpointReachabilityCacheKey, now: Instant) -> Option<bool> {
        let entry = self.entries.get(key)?;
        entry.is_fresh_at(now).then_some(entry.reachable)
    }

    fn insert(
        &mut self,
        key: PublicEndpointReachabilityCacheKey,
        entry: PublicEndpointReachabilityCacheEntry,
        now: Instant,
    ) {
        self.remove(&key);
        self.expire_stale_at(now);
        while self.entries.len() >= MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES {
            if !self.evict_next_expiring() {
                break;
            }
        }
        self.insert_expiry_index_entry(&key, &entry);
        self.entries.insert(key, entry);
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.entries.clear();
        self.expiry_index.clear();
    }

    fn expire_stale_at(&mut self, now: Instant) {
        while let Some((expires_at, _)) = self.expiry_index.first_key_value() {
            if *expires_at > now {
                break;
            }
            let expires_at = *expires_at;
            let Some(keys) = self.expiry_index.remove(&expires_at) else {
                break;
            };
            for key in keys {
                if self
                    .entries
                    .get(&key)
                    .is_some_and(|entry| entry.expires_at() <= now)
                {
                    self.entries.remove(&key);
                }
            }
        }
    }

    fn evict_next_expiring(&mut self) -> bool {
        loop {
            let Some((expires_at, _)) = self.expiry_index.first_key_value() else {
                return false;
            };
            let expires_at = *expires_at;
            let Some(keys) = self.expiry_index.get_mut(&expires_at) else {
                return false;
            };
            let Some(key) = keys.pop_first() else {
                self.expiry_index.remove(&expires_at);
                continue;
            };
            if keys.is_empty() {
                self.expiry_index.remove(&expires_at);
            }
            if self.entries.remove(&key).is_some() {
                return true;
            }
        }
    }

    fn remove(&mut self, key: &PublicEndpointReachabilityCacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.remove_expiry_index_entry(key, entry.expires_at());
        }
    }

    fn insert_expiry_index_entry(
        &mut self,
        key: &PublicEndpointReachabilityCacheKey,
        entry: &PublicEndpointReachabilityCacheEntry,
    ) {
        self.expiry_index
            .entry(entry.expires_at())
            .or_default()
            .insert(key.clone());
    }

    fn remove_expiry_index_entry(&mut self, key: &PublicEndpointReachabilityCacheKey, at: Instant) {
        let should_remove_bucket = if let Some(keys) = self.expiry_index.get_mut(&at) {
            keys.remove(key);
            keys.is_empty()
        } else {
            false
        };
        if should_remove_bucket {
            self.expiry_index.remove(&at);
        }
    }
}

/// Per-path record of the last content `write_object_file` wrote (or confirmed
/// already on disk), used to skip the read-back + compare when nothing changed.
#[derive(Clone)]
struct ObjectFileWriteMark {
    hash: blake3::Hash,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
    last_used: u64,
}

/// Cache of last-written object files. The cluster directory can be shared by the
/// main and shadow daemons, so the fast-path skip is guarded by a `stat`: an entry
/// is trusted only when the file's length, mtime, and inode still match what we
/// wrote, so a write by another process is detected and falls back to a real read
/// + compare.
static LAST_WRITTEN_OBJECT_FILES: LazyLock<Mutex<HashMap<PathBuf, ObjectFileWriteMark>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn object_file_write_mark_cache_tick() -> u64 {
    OBJECT_FILE_WRITE_MARK_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn evict_object_file_write_mark_if_needed(
    cache: &mut HashMap<PathBuf, ObjectFileWriteMark>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, mark)| mark.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

fn cached_object_file_mark(path: &PathBuf) -> Option<ObjectFileWriteMark> {
    let mut cache = LAST_WRITTEN_OBJECT_FILES.lock().ok()?;
    let mark = cache.get_mut(path)?;
    mark.last_used = object_file_write_mark_cache_tick();
    Some(mark.clone())
}

fn record_object_file_mark(
    path: &PathBuf,
    hash: blake3::Hash,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
) {
    if let Ok(mut cache) = LAST_WRITTEN_OBJECT_FILES.lock() {
        evict_object_file_write_mark_if_needed(&mut cache, path);
        cache.insert(
            path.clone(),
            ObjectFileWriteMark {
                hash,
                len,
                modified,
                ino,
                last_used: object_file_write_mark_cache_tick(),
            },
        );
    }
}

fn file_ino(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::ino(metadata)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn file_modified_and_ino(path: &PathBuf) -> (Option<SystemTime>, u64) {
    match fs::metadata(path) {
        Ok(metadata) => (metadata.modified().ok(), file_ino(&metadata)),
        Err(_) => (None, 0),
    }
}

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

fn cluster_kv_watch_event_cache_marks(
    paths: &ClusterPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    Some((
        cluster_file_mark(&paths.kv_watch_events)?,
        cluster_file_mark(&paths.kv_watch_events_log)?,
    ))
}

fn cluster_kv_watch_empty_drain_mark(
    paths: &ClusterPaths,
    generation: ClusterKvWatchObservedGeneration,
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

fn cluster_kv_watch_cache_tick() -> u64 {
    CLUSTER_KV_WATCH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

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

fn cached_cluster_kv_watch_events_arc(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<Arc<Vec<KvEvent>>> {
    cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log).map(|entry| entry.events)
}

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

fn invalidate_cluster_kv_watch_event_cache(paths: &ClusterPaths) {
    let key = cluster_kv_watch_notify_key(paths);
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        cache.remove(&key);
    }
}

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

fn invalidate_cluster_kv_watch_registration_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(path);
    }
}

fn evict_cluster_kv_active_watch_registration_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvActiveWatchRegistrationCacheEntry>,
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

fn cached_active_cluster_kv_watch_registrations(
    paths: &ClusterPaths,
    watch_dir_mark: ClusterFileMark,
) -> Option<Arc<Vec<ClusterKvWatchRegistration>>> {
    let mut cache = CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE.lock().ok()?;
    let entry = cache.get_mut(&paths.kv_watches)?;
    if entry.watch_dir_mark == watch_dir_mark {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.registrations.clone())
    } else {
        None
    }
}

fn record_active_cluster_kv_watch_registrations(
    paths: &ClusterPaths,
    watch_dir_mark: ClusterFileMark,
    registrations: Vec<ClusterKvWatchRegistration>,
) {
    if let Ok(mut cache) = CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE.lock() {
        evict_cluster_kv_active_watch_registration_cache_entry_if_needed(
            &mut cache,
            &paths.kv_watches,
        );
        cache.insert(
            paths.kv_watches.clone(),
            ClusterKvActiveWatchRegistrationCacheEntry {
                watch_dir_mark,
                registrations: Arc::new(registrations),
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

fn invalidate_active_cluster_kv_watch_registration_cache(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(&paths.kv_watches);
    }
}

fn invalidate_active_cluster_kv_watch_registration_cache_for_watch_path(path: &PathBuf) {
    let Some(watch_dir) = path.parent() else {
        return;
    };
    if let Ok(mut cache) = CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(&watch_dir.to_path_buf());
    }
}

fn refresh_active_cluster_kv_watch_registration_cache_for_watch_path(
    path: &PathBuf,
    registration: ClusterKvWatchRegistration,
) {
    let Some(watch_dir) = path.parent().map(|parent| parent.to_path_buf()) else {
        return;
    };
    let Some(watch_dir_mark) = cluster_file_mark(&watch_dir) else {
        invalidate_active_cluster_kv_watch_registration_cache_for_watch_path(path);
        return;
    };
    let Ok(mut cache) = CLUSTER_KV_ACTIVE_WATCH_REGISTRATION_CACHE.lock() else {
        return;
    };
    let Some(entry) = cache.get_mut(&watch_dir) else {
        return;
    };
    if entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&watch_dir);
        return;
    }
    let registrations = Arc::make_mut(&mut entry.registrations);
    let Some(existing) = registrations
        .iter_mut()
        .find(|existing| existing.watch_id == registration.watch_id)
    else {
        cache.remove(&watch_dir);
        return;
    };
    *existing = registration;
    entry.last_used = cluster_kv_watch_cache_tick();
}

fn evict_cluster_kv_watch_allocator_trust_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>,
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

fn cluster_kv_watch_allocator_trust_entry(
    paths: &ClusterPaths,
) -> Option<ClusterKvWatchAllocatorTrustEntry> {
    let Some(allocator_mark) = cluster_file_mark(&paths.kv_watches_next_id) else {
        return None;
    };
    if !allocator_mark.exists {
        return None;
    }
    let Some(watch_dir_mark) = cluster_file_mark(&paths.kv_watches) else {
        return None;
    };
    if !watch_dir_mark.exists {
        return None;
    }
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return None;
    };
    let Some(entry) = cache.get_mut(&paths.kv_watches_next_id) else {
        return None;
    };
    if entry.allocator_mark == allocator_mark && entry.watch_dir_mark == watch_dir_mark {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.clone())
    } else {
        None
    }
}

fn cluster_kv_watch_allocator_trusted(paths: &ClusterPaths) -> bool {
    cluster_kv_watch_allocator_trust_entry(paths).is_some()
}

fn record_current_cluster_kv_watch_allocator_trust(
    paths: &ClusterPaths,
    active_watch_count: Option<usize>,
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
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
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

fn record_current_cluster_kv_reader_node_id_cache(paths: &ClusterPaths, node_id: NodeId) {
    if let Some(mark) = cluster_file_mark(&paths.state)
        && mark.exists
    {
        record_cluster_kv_reader_node_id_cache(&paths.state, mark, node_id);
    }
}

fn invalidate_cluster_kv_reader_node_id_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.remove(path);
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
fn record_cluster_kv_current_snapshot_read_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST.lock() {
        *reads.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_current_snapshot_reads_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST.lock() {
        reads.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_current_snapshot_reads_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_watch_dir_scan_for_test(paths: &ClusterPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        *scans.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
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
pub(crate) fn local_cluster_connectivity_record_cache_contains_for_test(
    paths: &ClusterPaths,
) -> bool {
    LOCAL_CLUSTER_CONNECTIVITY_RECORDS
        .lock()
        .map(|cache| cache.contains_key(&paths.root))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn reset_local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) {
    if let Ok(mut passes) = LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST.lock() {
        passes.remove(&paths.root);
    }
}

#[cfg(test)]
pub(crate) fn local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST
        .lock()
        .ok()
        .and_then(|passes| passes.get(&paths.root).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn object_file_write_mark_cache_capacity_for_test() -> usize {
    FILE_CACHE_CAPACITY
}

#[cfg(test)]
pub(crate) fn object_file_write_mark_cache_contains_for_test(path: &PathBuf) -> bool {
    LAST_WRITTEN_OBJECT_FILES
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn record_object_file_write_mark_for_test(path: &PathBuf) {
    record_object_file_mark(
        path,
        blake3::hash(b"stable"),
        6,
        Some(SystemTime::UNIX_EPOCH),
        1,
    );
}

#[cfg(test)]
pub(crate) fn refresh_object_file_write_mark_for_test(path: &PathBuf) -> bool {
    cached_object_file_mark(path).is_some()
}

#[cfg(test)]
pub(crate) fn remove_object_file_write_mark_entries_under_root_for_test(root: &std::path::Path) {
    if let Ok(mut cache) = LAST_WRITTEN_OBJECT_FILES.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(all(test, feature = "iroh-transport"))]
static CREATE_NEW_OBJECT_FILE_ATTEMPTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(all(test, feature = "iroh-transport"))]
fn record_write_object_file_create_new_attempt_for_test(path: &PathBuf) {
    if let Ok(mut attempts) = CREATE_NEW_OBJECT_FILE_ATTEMPTS_FOR_TEST.lock() {
        *attempts.entry(path.clone()).or_default() += 1;
    }
}

#[cfg(all(test, feature = "iroh-transport"))]
pub(crate) fn reset_write_object_file_create_new_attempts_for_test(path: &PathBuf) {
    if let Ok(mut attempts) = CREATE_NEW_OBJECT_FILE_ATTEMPTS_FOR_TEST.lock() {
        attempts.remove(path);
    }
}

#[cfg(all(test, feature = "iroh-transport"))]
pub(crate) fn write_object_file_create_new_attempts_for_test(path: &PathBuf) -> u64 {
    CREATE_NEW_OBJECT_FILE_ATTEMPTS_FOR_TEST
        .lock()
        .ok()
        .and_then(|attempts| attempts.get(path).copied())
        .unwrap_or_default()
}

#[cfg(test)]
static CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST: LazyLock<
    Mutex<std::collections::HashSet<PathBuf>>,
> = LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(crate) fn force_write_object_file_create_new_unsupported_link_for_test(path: &PathBuf) {
    if let Ok(mut paths) = CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST.lock() {
        paths.insert(path.clone());
    }
}

#[cfg(test)]
pub(crate) fn clear_write_object_file_create_new_unsupported_link_for_test(path: &PathBuf) {
    if let Ok(mut paths) = CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST.lock() {
        paths.remove(path);
    }
}

#[cfg(test)]
fn write_object_file_create_new_forces_unsupported_link_for_test(path: &PathBuf) -> bool {
    CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST
        .lock()
        .ok()
        .is_some_and(|paths| paths.contains(path))
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
pub(crate) fn remove_cluster_kv_watch_event_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
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
pub(crate) fn remove_cluster_kv_watch_registration_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_metadata_cache_capacity_for_test() -> usize {
    FILE_CACHE_CAPACITY
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
pub(crate) fn remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
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

#[cfg(test)]
pub(crate) fn local_cluster_connectivity_record_cache_capacity_for_test() -> usize {
    MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES
}

#[cfg(test)]
pub(crate) fn remove_local_cluster_connectivity_record_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = LOCAL_CLUSTER_CONNECTIVITY_RECORDS.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

/// Receives notifications about disk and network I/O performed by the network
/// layer: object-file and KV-snapshot writes (op counts), the byte volume of
/// those reads/writes, and the byte volume of framed RPC traffic. Used by
/// sinomo-daemon to feed idle-stats counters. All methods default to no-ops so
/// implementors opt into only the signals they care about.
pub trait IoObserver: Send + Sync {
    /// Bytes read from disk (object-file reads, including the read-compare in
    /// `write_object_file`).
    fn on_disk_read(&self, _bytes: u64) {}
    /// Bytes written to disk (object-file and KV-snapshot writes).
    fn on_disk_write(&self, _bytes: u64) {}
    fn on_object_file_actual_write(&self) {}
    fn on_object_file_skipped_write(&self) {}
    fn on_kv_snapshot_actual_write(&self) {}
    fn on_kv_snapshot_skipped_write(&self) {}
    /// Bytes sent over a framed RPC stream (4-byte length prefix + body).
    fn on_net_sent(&self, _bytes: u64) {}
    /// Bytes received over a framed RPC stream (4-byte length prefix + body).
    fn on_net_recv(&self, _bytes: u64) {}
}

static IO_OBSERVER: Mutex<Option<Arc<dyn IoObserver>>> = Mutex::new(None);

/// Install a process-wide I/O observer. The daemon installs exactly one
/// observer at startup; tests may swap in their own. The previous observer is
/// returned so callers can restore it after a scoped change.
pub fn set_io_observer(observer: Option<Arc<dyn IoObserver>>) -> Option<Arc<dyn IoObserver>> {
    let mut guard = IO_OBSERVER.lock().ok()?;
    std::mem::replace(&mut *guard, observer)
}

pub(crate) fn io_observer() -> Option<Arc<dyn IoObserver>> {
    IO_OBSERVER.lock().ok().and_then(|guard| guard.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterPaths {
    pub root: PathBuf,
    pub state: PathBuf,
    pub iroh: PathBuf,
    pub connectivity_local: PathBuf,
    pub kv_snapshot: PathBuf,
    pub kv_raft: PathBuf,
    pub kv_watches: PathBuf,
    pub kv_watches_lock: PathBuf,
    pub kv_watches_next_id: PathBuf,
    pub kv_watch_events: PathBuf,
    pub kv_watch_events_log: PathBuf,
}

impl ClusterPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("cluster");
        Self {
            state: root.join("state.sobj"),
            iroh: root.join("iroh"),
            connectivity_local: root.join("connectivity-local.sobj"),
            kv_snapshot: root.join("kv-snapshot.sobj"),
            kv_raft: root.join("kv-raft"),
            kv_watches: root.join("watches"),
            kv_watches_lock: root.join("watches.lock"),
            kv_watches_next_id: root.join("watches-next.sobj"),
            kv_watch_events: root.join("watch-events.sobj"),
            kv_watch_events_log: root.join("watch-events.sobjlog"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), NetworkError> {
        fs::create_dir_all(&self.root).map_err(|source| NetworkError::StorageIo {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

fn cluster_kv_watch_notify_key(paths: &ClusterPaths) -> PathBuf {
    paths.root.clone()
}

fn cluster_kv_watch_notify_key_for_watch(paths: &ClusterPaths, watch_id: WatchId) -> PathBuf {
    cluster_kv_watch_path(paths, watch_id)
}

fn cluster_kv_watch_notifier_tick() -> u64 {
    CLUSTER_KV_WATCH_NOTIFIER_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn evict_cluster_kv_watch_notifier_entry_if_needed(
    guard: &mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    protected: &PathBuf,
) {
    if guard.contains_key(protected) || guard.len() < MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, entry)| *path != protected && entry.waiters == 0)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

fn ensure_cluster_kv_watch_notifier_entry<'a>(
    guard: &'a mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    key: PathBuf,
) -> &'a mut ClusterKvWatchNotifierEntry {
    evict_cluster_kv_watch_notifier_entry_if_needed(guard, &key);
    guard.entry(key).or_default()
}

fn finish_cluster_kv_watch_wait(
    guard: &mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    key: &PathBuf,
) {
    if let Some(entry) = guard.get_mut(key) {
        entry.waiters = entry.waiters.saturating_sub(1);
        entry.last_used = cluster_kv_watch_notifier_tick();
    }
}

#[cfg(test)]
fn cluster_kv_watch_generation(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .get(&cluster_kv_watch_notify_key(paths))
                .map(|entry| entry.generation)
        })
        .unwrap_or(0)
}

fn cluster_kv_watch_observed_generation(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> ClusterKvWatchObservedGeneration {
    let root_key = cluster_kv_watch_notify_key(paths);
    let watch_key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| ClusterKvWatchObservedGeneration {
            root: guard
                .get(&root_key)
                .map(|entry| entry.generation)
                .unwrap_or(0),
            watch: guard
                .get(&watch_key)
                .map(|entry| entry.generation)
                .unwrap_or(0),
        })
        .unwrap_or(ClusterKvWatchObservedGeneration { root: 0, watch: 0 })
}

fn notify_cluster_kv_watchers(paths: &ClusterPaths) {
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return;
    };
    let key = cluster_kv_watch_notify_key(paths);
    let tick = cluster_kv_watch_notifier_tick();
    let mut changed = Vec::new();
    {
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key.clone());
        entry.generation = entry.generation.saturating_add(1);
        entry.last_used = tick;
        changed.push(entry.changed.clone());
    }
    for (path, entry) in guard.iter_mut() {
        if path != &key && path.starts_with(&paths.root) && entry.waiters > 0 {
            entry.generation = entry.generation.saturating_add(1);
            entry.last_used = tick;
            changed.push(entry.changed.clone());
        }
    }
    drop(guard);
    for changed in changed {
        changed.notify_all();
    }
}

fn notify_cluster_kv_watchers_for_events(paths: &ClusterPaths, events: &[KvEvent]) {
    match cluster_kv_watch_ids_matching_events(paths, events) {
        Ok(watch_ids) => notify_cluster_kv_watchers_for_watch_ids(paths, &watch_ids),
        Err(error) => {
            eprintln!(
                "warning: waking all cluster kv watch waiters after active watch lookup failed for {}: {}",
                paths.kv_watches.display(),
                error
            );
            notify_cluster_kv_watchers(paths);
        }
    }
}

fn notify_cluster_kv_watchers_for_watch_ids(paths: &ClusterPaths, watch_ids: &[WatchId]) {
    if watch_ids.is_empty() {
        return;
    }
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return;
    };
    let tick = cluster_kv_watch_notifier_tick();
    let mut changed = Vec::new();
    for watch_id in watch_ids {
        let key = cluster_kv_watch_notify_key_for_watch(paths, *watch_id);
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key);
        entry.generation = entry.generation.saturating_add(1);
        entry.last_used = tick;
        if entry.waiters > 0 {
            changed.push(entry.changed.clone());
        }
    }
    drop(guard);
    for changed in changed {
        changed.notify_all();
    }
}

fn cluster_kv_watch_ids_matching_events(
    paths: &ClusterPaths,
    events: &[KvEvent],
) -> Result<Vec<WatchId>, NetworkError> {
    if events.is_empty() {
        return Ok(Vec::new());
    }
    let registrations = active_cluster_kv_watch_registrations(paths)?;
    let mut watch_ids = BTreeSet::new();
    for registration in registrations {
        if events.iter().any(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        }) {
            watch_ids.insert(registration.watch_id);
        }
    }
    Ok(watch_ids.into_iter().collect())
}

#[cfg(test)]
fn wait_for_cluster_kv_watch_change(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    if timeout.is_zero() {
        return false;
    }
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    let key = cluster_kv_watch_notify_key(paths);
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return false;
    };
    let changed = {
        let tick = cluster_kv_watch_notifier_tick();
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key.clone());
        entry.waiters = entry.waiters.saturating_add(1);
        entry.last_used = tick;
        entry.changed.clone()
    };
    loop {
        if guard.get(&key).map(|entry| entry.generation).unwrap_or(0) != observed_generation {
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return false;
        }
        let (next_guard, wait) = match changed.wait_timeout(guard, remaining) {
            Ok(result) => result,
            Err(poisoned) => {
                let (mut poisoned_guard, _) = poisoned.into_inner();
                finish_cluster_kv_watch_wait(&mut poisoned_guard, &key);
                return false;
            }
        };
        guard = next_guard;
        if let Some(entry) = guard.get_mut(&key) {
            entry.last_used = cluster_kv_watch_notifier_tick();
        }
        if wait.timed_out() {
            let changed =
                guard.get(&key).map(|entry| entry.generation).unwrap_or(0) != observed_generation;
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return changed;
        }
    }
}

fn cluster_kv_watch_observed_generation_changed(
    guard: &HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    root_key: &PathBuf,
    watch_key: &PathBuf,
    observed_generation: ClusterKvWatchObservedGeneration,
) -> bool {
    guard
        .get(root_key)
        .map(|entry| entry.generation)
        .unwrap_or(0)
        != observed_generation.root
        || guard
            .get(watch_key)
            .map(|entry| entry.generation)
            .unwrap_or(0)
            != observed_generation.watch
}

fn wait_for_cluster_kv_watch_change_for_watch(
    paths: &ClusterPaths,
    watch_id: WatchId,
    observed_generation: ClusterKvWatchObservedGeneration,
    timeout: Duration,
) -> bool {
    if timeout.is_zero() {
        return false;
    }
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    let root_key = cluster_kv_watch_notify_key(paths);
    let watch_key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return false;
    };
    let changed = {
        let tick = cluster_kv_watch_notifier_tick();
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, watch_key.clone());
        entry.waiters = entry.waiters.saturating_add(1);
        entry.last_used = tick;
        entry.changed.clone()
    };
    loop {
        if cluster_kv_watch_observed_generation_changed(
            &guard,
            &root_key,
            &watch_key,
            observed_generation,
        ) {
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return false;
        }
        let (next_guard, wait) = match changed.wait_timeout(guard, remaining) {
            Ok(result) => result,
            Err(poisoned) => {
                let (mut poisoned_guard, _) = poisoned.into_inner();
                finish_cluster_kv_watch_wait(&mut poisoned_guard, &watch_key);
                return false;
            }
        };
        guard = next_guard;
        let tick = cluster_kv_watch_notifier_tick();
        if let Some(entry) = guard.get_mut(&watch_key) {
            entry.last_used = tick;
        }
        #[cfg(test)]
        if !wait.timed_out()
            && !cluster_kv_watch_observed_generation_changed(
                &guard,
                &root_key,
                &watch_key,
                observed_generation,
            )
        {
            record_cluster_kv_watch_unchanged_wakeup_for_test(&watch_key);
        }
        if wait.timed_out() {
            let changed = cluster_kv_watch_observed_generation_changed(
                &guard,
                &root_key,
                &watch_key,
                observed_generation,
            );
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return changed;
        }
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_generation_for_test(paths: &ClusterPaths) -> u64 {
    cluster_kv_watch_generation(paths)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    cluster_kv_watch_notifier_has_raw_key_for_test(&key)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &std::path::Path) -> bool {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| guard.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterPaths) -> usize {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| guard.get(&key).map(|entry| entry.waiters))
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &std::path::Path) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        guard.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterPaths) {
    notify_cluster_kv_watchers(paths);
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_notifier_waiter_key_for_test(path: PathBuf) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        evict_cluster_kv_watch_notifier_entry_if_needed(&mut guard, &path);
        guard.insert(
            path,
            ClusterKvWatchNotifierEntry {
                generation: 0,
                last_used: cluster_kv_watch_notifier_tick(),
                waiters: 1,
                changed: Arc::new(Condvar::new()),
            },
        );
    }
}

#[cfg(test)]
fn record_cluster_kv_watch_unchanged_wakeup_for_test(path: &PathBuf) {
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        *wakeups.entry(path.clone()).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        wakeups.remove(&key);
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> u64 {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST
        .lock()
        .ok()
        .and_then(|wakeups| wakeups.get(&key).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn wait_for_cluster_kv_watch_change_for_test(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    wait_for_cluster_kv_watch_change(paths, observed_generation, timeout)
}

pub fn remove_local_cluster_state(paths: &ClusterPaths) -> Result<(), NetworkError> {
    if !remove_optional_file(&paths.state)? {
        return Err(NetworkError::ClusterOrphan);
    }
    invalidate_cluster_kv_reader_node_id_cache(&paths.state);
    remove_optional_file(&paths.kv_snapshot)?;
    Ok(())
}

fn remove_optional_file(path: &PathBuf) -> Result<bool, NetworkError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationRequest {
    pub invite: sinomo_core::ClusterJoinInvite,
    pub invitation_secret: String,
    pub members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitePublishRequest {
    pub invite: ClusterJoinInvite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitePublishResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterMemberPublishRequest {
    pub members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterMemberPublishResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendRequest {
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<RaftSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteRequest {
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsRequest {
    pub state: ClusterBootstrapState,
    pub leader_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsResponse {
    pub accepted: bool,
    pub detail: String,
    pub stored_events: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchPrefixRequest {
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchRegistration {
    pub watch_id: WatchId,
    pub prefix: String,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchAllocatorState {
    pub next_watch_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_watch_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruned_compacted_revision: Option<KvRevision>,
}

impl ClusterKvWatchAllocatorState {
    fn legacy(next_watch_id: u64) -> Self {
        Self {
            next_watch_id: next_watch_id.max(1),
            active_watch_count: None,
            pruned_compacted_revision: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainRequest {
    pub watch_id: WatchId,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub wait_timeout_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainResponse {
    pub watch_id: WatchId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchCancelRequest {
    pub watch_id: WatchId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchCancelResponse {
    pub watch_id: WatchId,
    pub cancelled: bool,
}

impl ClusterKvWatchDrainRequest {
    fn wait_timeout(&self) -> Duration {
        let max_millis = MAX_CLUSTER_KV_WATCH_DRAIN_WAIT.as_millis() as u64;
        Duration::from_millis(self.wait_timeout_millis.min(max_millis))
    }
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterIdentityRecord {
    pub cluster_id: ClusterId,
    pub cluster_name: String,
    pub local_node_id: NodeId,
    pub local_node_display_name: String,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftConfigRecord {
    pub cluster_id: ClusterId,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voters: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub learners: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterCapabilityPolicyRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<CapabilityName>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitationStateRecord {
    pub cluster_id: ClusterId,
    pub invite_id: ClusterInviteId,
    pub state: String,
    pub issuer_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutDesiredRecord {
    pub cluster_id: ClusterId,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub source_bundle: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_repo: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutStatusRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub rollout_plan_id: sinomo_core::Id,
    pub module_id: sinomo_core::Id,
    pub artifact_id: sinomo_core::Id,
    pub stage: String,
    pub current_weight_bps: u16,
    pub active_artifact_id: sinomo_core::Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_artifact_id: Option<sinomo_core::Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<sinomo_core::Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<sinomo_core::Id>,
    pub hot_swap_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry_last_consumed_sequence: Option<EventSequence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_record_id: Option<sinomo_core::Id>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutDesiredRecord {
    pub cluster_id: ClusterId,
    pub state: String,
    pub rollout_plan: sinomo_core::RolloutPlan,
    pub requested_by_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<sinomo_core::Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_material_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterModuleRolloutReconciliationRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub rollout_plan_id: sinomo_core::Id,
    pub module_id: sinomo_core::Id,
    pub artifact_id: sinomo_core::Id,
    pub state: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<sinomo_core::Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_material_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_runtime_stage: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkInfoRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub environment_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<ClusterEndpointRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterConnectivityState {
    Healthy,
    Degraded,
    Unreachable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterConnectivityRecord {
    pub cluster_id: ClusterId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint_id: ClusterEndpointId,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub state: ClusterConnectivityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct ClusterConnectivityGraph {
    source_node_id: NodeId,
    records: BTreeMap<ClusterConnectivityRecordKey, ClusterConnectivityRecord>,
    records_by_address: BTreeMap<ClusterConnectivityAddressKey, ClusterConnectivityRecordKey>,
    healthy_records_by_pair: BTreeMap<(NodeId, NodeId), BTreeSet<(u64, ClusterEndpointId)>>,
    reachable_targets_by_source: BTreeMap<NodeId, BTreeSet<NodeId>>,
    healthy_targets_by_source: BTreeMap<NodeId, BTreeSet<NodeId>>,
}

type ClusterConnectivityRecordKey = (NodeId, NodeId, ClusterEndpointId);
type ClusterConnectivityAddressKey = (NodeId, NodeId, String);

impl ClusterConnectivityGraph {
    pub fn from_records(
        source_node_id: NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> Self {
        let records: BTreeMap<_, _> = records
            .into_iter()
            .map(|record| {
                (
                    (
                        record.source_node_id.clone(),
                        record.target_node_id.clone(),
                        record.endpoint_id.clone(),
                    ),
                    record,
                )
            })
            .collect();
        let mut reachable_targets_by_source = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
        let mut healthy_targets_by_source = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
        let mut records_by_address =
            BTreeMap::<ClusterConnectivityAddressKey, ClusterConnectivityRecordKey>::new();
        let mut healthy_records_by_pair =
            BTreeMap::<(NodeId, NodeId), BTreeSet<(u64, ClusterEndpointId)>>::new();
        for record in records.values() {
            let record_key = (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            );
            for address_key in cluster_endpoint_address_lookup_keys(&record.address) {
                records_by_address
                    .entry((
                        record.source_node_id.clone(),
                        record.target_node_id.clone(),
                        address_key,
                    ))
                    .or_insert_with(|| record_key.clone());
            }
            if record.target_node_id == record.source_node_id {
                continue;
            }
            if record.state != ClusterConnectivityState::Unreachable {
                reachable_targets_by_source
                    .entry(record.source_node_id.clone())
                    .or_default()
                    .insert(record.target_node_id.clone());
            }
            if record.state == ClusterConnectivityState::Healthy {
                healthy_targets_by_source
                    .entry(record.source_node_id.clone())
                    .or_default()
                    .insert(record.target_node_id.clone());
                healthy_records_by_pair
                    .entry((record.source_node_id.clone(), record.target_node_id.clone()))
                    .or_default()
                    .insert((
                        record.latency_ms.unwrap_or(u64::MAX),
                        record.endpoint_id.clone(),
                    ));
            }
        }
        Self {
            source_node_id,
            records,
            records_by_address,
            healthy_records_by_pair,
            reachable_targets_by_source,
            healthy_targets_by_source,
        }
    }

    pub fn read_for_local(paths: &ClusterPaths, state: &ClusterBootstrapState) -> Self {
        Self::from_records(
            state.local_node_id.clone(),
            read_cluster_connectivity_graph_with_suppressed_local_source(
                paths,
                Some(&state.local_node_id),
            )
            .unwrap_or_default(),
        )
    }

    pub fn record_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<&ClusterConnectivityRecord> {
        self.records
            .get(&(
                self.source_node_id.clone(),
                endpoint.node_id.clone(),
                endpoint.endpoint_id.clone(),
            ))
            .or_else(|| {
                // Fall back to matching on (source, target, address): the network address is
                // the stable identity of an endpoint. The same address can be derived with a
                // different `endpoint_id` or transport classification across code paths (the
                // probe uses member endpoints; routing queries with command-target endpoints),
                // which previously caused a silent miss -- so this no longer requires the
                // transport to match and normalizes the address.
                cluster_endpoint_address_lookup_keys(&endpoint.address)
                    .into_iter()
                    .find_map(|address_key| {
                        self.records_by_address
                            .get(&(
                                self.source_node_id.clone(),
                                endpoint.node_id.clone(),
                                address_key,
                            ))
                            .and_then(|record_key| self.records.get(record_key))
                    })
            })
    }

    pub fn state_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<&ClusterConnectivityState> {
        self.record_for_endpoint(endpoint)
            .map(|record| &record.state)
    }

    pub fn unavailability_rank(&self, endpoint: &ClusterEndpointRecord) -> u8 {
        cluster_route_unavailability_rank(self.record_for_endpoint(endpoint))
    }

    pub fn quality_rank(&self, endpoint: &ClusterEndpointRecord) -> u8 {
        cluster_route_connectivity_quality_rank(self.state_for_endpoint(endpoint))
    }

    /// Node ids directly reachable from `source` per a non-`Unreachable` connectivity
    /// record (over any endpoint), excluding `source` itself. This is the adjacency the
    /// multi-hop route discovery in [`crate::route`] walks over the cluster-wide graph.
    pub fn reachable_targets(&self, source: &NodeId) -> std::collections::BTreeSet<NodeId> {
        self.reachable_targets_by_source
            .get(source)
            .cloned()
            .unwrap_or_default()
    }

    /// Node ids `source` has a CURRENTLY-HEALTHY connectivity record to (over any endpoint),
    /// excluding `source`. Unlike [`reachable_targets`], `Degraded` does NOT count: a routing
    /// first hop must be a link we can dial reliably *now*, so a never-reachable peer that is
    /// merely mid-convergence (transiently `Degraded`) is not treated as a direct hop. This is
    /// the adjacency multi-hop route discovery in [`crate::route`] walks.
    pub fn healthy_targets(&self, source: &NodeId) -> std::collections::BTreeSet<NodeId> {
        self.healthy_targets_by_source
            .get(source)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn healthy_target_refs<'a>(
        &'a self,
        source: &NodeId,
    ) -> impl Iterator<Item = &'a NodeId> + 'a {
        self.healthy_targets_by_source
            .get(source)
            .into_iter()
            .flat_map(|targets| targets.iter())
    }

    /// A dialable endpoint to reach `target` from `source` over a currently-healthy link, or
    /// `None` if no healthy record exists. Built straight from the connectivity record (which
    /// holds the exact address+transport last probed healthy), so it is immune to the
    /// endpoint-object matching in [`Self::record_for_endpoint`]; among several healthy
    /// endpoints it prefers the lowest observed latency. Used to pick the reachable link of a
    /// multi-homed bridge for relaying.
    pub fn best_healthy_endpoint(
        &self,
        source: &NodeId,
        target: &NodeId,
    ) -> Option<ClusterEndpointRecord> {
        self.healthy_records_by_pair
            .get(&(source.clone(), target.clone()))
            .into_iter()
            .flat_map(|records| records.iter())
            .find_map(|(_, endpoint_id)| {
                self.records
                    .get(&(source.clone(), target.clone(), endpoint_id.clone()))
            })
            .map(|record| ClusterEndpointRecord {
                endpoint_id: record.endpoint_id.clone(),
                node_id: record.target_node_id.clone(),
                transport: record.transport.clone(),
                address: record.address.clone(),
                source: ClusterEndpointSource::Observed,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 0,
                last_observed_latency_ms: record
                    .latency_ms
                    .map(|ms| ms.min(u64::from(u32::MAX)) as u32),
            })
    }
}

/// Whether two endpoint address strings denote the same network address. Compares the raw
/// strings, then a normalized `SocketAddr` parse so equivalent spellings still match. Used by
/// connectivity-record lookup, where the same address may be derived slightly differently
/// across code paths (probe targets vs command targets).
fn cluster_endpoint_address_lookup_keys(address: &str) -> Vec<String> {
    let mut keys = vec![format!("raw:{address}")];
    if let Ok(socket_addr) = address.parse::<SocketAddr>() {
        keys.push(format!("socket:{socket_addr}"));
    }
    keys
}

pub fn build_cluster_network_info_record(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    observed_at: OffsetDateTime,
    ttl_seconds: i64,
) -> ClusterNetworkInfoRecord {
    let endpoints = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.endpoints.clone())
        .unwrap_or_default();
    let observed_addresses = cluster_network_observed_addresses(&endpoints);
    ClusterNetworkInfoRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: node_id.clone(),
        environment_id: cluster_network_environment_id(&observed_addresses),
        observed_addresses,
        endpoints,
        observed_at,
        expires_at: observed_at + time::Duration::seconds(ttl_seconds),
    }
}

pub fn cluster_network_observed_addresses(endpoints: &[ClusterEndpointRecord]) -> Vec<String> {
    let mut observed_addresses = endpoints
        .iter()
        .filter(|endpoint| {
            matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::LanQuic
                    | ClusterTransportKind::Iroh
                    | ClusterTransportKind::WebRtc
                    | ClusterTransportKind::TurnRelay
                    | ClusterTransportKind::HttpsRelay
            )
        })
        .map(|endpoint| endpoint.address.clone())
        .collect::<Vec<_>>();
    observed_addresses.sort();
    observed_addresses.dedup();
    observed_addresses
}

pub fn cluster_network_environment_id(values: &[String]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for value in values {
        for byte in value.as_bytes().iter().chain(std::iter::once(&0)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("env_{hash:016x}")
}

pub fn cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> String {
    format!(
        "{}:{}:{}",
        source_node_id, endpoint.node_id, endpoint.endpoint_id
    )
}

pub fn cluster_connectivity_record(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    consecutive_failures: u16,
    last_success_at: Option<String>,
    last_failure_at: Option<String>,
    observed_at: OffsetDateTime,
    ttl_seconds: i64,
) -> ClusterConnectivityRecord {
    // The caller passes an explicit per-state TTL (e.g. CLUSTER_CONNECTIVITY_TTL_SECONDS for
    // healthy/degraded, CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS for unreachable); honor it
    // directly rather than silently halving non-healthy lifetimes.
    ClusterConnectivityRecord {
        cluster_id: state.cluster_id.clone(),
        source_node_id: state.local_node_id.clone(),
        target_node_id: endpoint.node_id.clone(),
        endpoint_id: endpoint.endpoint_id.clone(),
        transport: endpoint.transport.clone(),
        address: endpoint.address.clone(),
        state: connectivity_state,
        latency_ms,
        consecutive_failures,
        last_success_at,
        last_failure_at,
        observed_at,
        expires_at: observed_at + time::Duration::seconds(ttl_seconds),
    }
}

pub fn apply_cluster_node_role(
    member: &mut ClusterMemberRecord,
    role: ClusterNodeRole,
    timestamp: OffsetDateTime,
) {
    member.node_role = role;
    member.updated_at = timestamp;
}

pub fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: OffsetDateTime,
) {
    member.role = role.clone();
    member.state = cluster_raft_state_for_role(&role);
    member.capabilities = main_daemon_capabilities(&role);
    member.allowed_capabilities = member.capabilities.clone();
    member.updated_at = timestamp;
}

pub fn cluster_raft_state_for_role(role: &ClusterMemberRole) -> ClusterMemberState {
    match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    }
}

pub fn ensure_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_endpoints: Vec<ClusterEndpointRecord>,
    shadow_endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    let main_changed = ensure_main_daemon_cluster_member(state, main_endpoints, timestamp);
    let shadow_changed = ensure_shadow_daemon_cluster_member(state, shadow_endpoints, timestamp);
    let changed = main_changed || shadow_changed;
    if changed {
        state.updated_at = timestamp;
    }
    changed
}

pub fn ensure_main_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    let local_node_id = state.local_node_id.clone();
    let existing = state
        .members
        .iter()
        .find(|member| member.node_id == local_node_id);
    let role = existing
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::Learner);
    let node_role = existing
        .map(|member| member.node_role.clone())
        .unwrap_or_default();
    let member_state = existing
        .map(|member| member.state.clone())
        .unwrap_or(ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &local_node_id).unwrap_or_default();
    let mut member = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        local_node_id,
        state.local_node_display_name.clone(),
        node_public_key,
        timestamp,
    );
    let capabilities = main_daemon_capabilities(&role);
    member.node_role = node_role;
    member.role = role;
    member.state = member_state;
    upsert_daemon_member(state, member, capabilities, endpoints, timestamp)
}

pub fn ensure_shadow_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let existing_shadow_state = state
        .members
        .iter()
        .find(|member| member.node_id == shadow_node_id)
        .map(|member| member.state.clone())
        .filter(|state| *state != ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &shadow_node_id).unwrap_or_default();
    let main_authorized = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_some_and(|member| member.state.is_authorized());
    let mut member = if main_authorized {
        ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            shadow_node_id,
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        )
    } else {
        ClusterMemberRecord::pending_non_voting(
            state.cluster_id.clone(),
            shadow_node_id,
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        )
    };
    if let Some(existing_state) = existing_shadow_state {
        member.state = existing_state;
    }
    upsert_daemon_member(
        state,
        member,
        shadow_daemon_capabilities(),
        endpoints,
        timestamp,
    )
}

pub fn existing_member_public_key(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Option<String> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.node_public_key.clone())
        .filter(|key| !key.trim().is_empty())
}

pub fn upsert_daemon_member(
    state: &mut ClusterBootstrapState,
    replacement: ClusterMemberRecord,
    capabilities: Vec<CapabilityName>,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == replacement.node_id)
    {
        let mut next_endpoints = member.endpoints.clone();
        if endpoints
            .iter()
            .any(|endpoint| daemon_tcp_endpoint_is_listener_advertisement(endpoint))
        {
            next_endpoints
                .retain(|endpoint| !daemon_tcp_endpoint_is_listener_advertisement(endpoint));
        }
        for endpoint in endpoints {
            next_endpoints.retain(|candidate| candidate.endpoint_id != endpoint.endpoint_id);
            next_endpoints.push(endpoint);
        }
        next_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        let changed = member.display_name != replacement.display_name
            || member.node_role != replacement.node_role
            || member.role != replacement.role
            || member.state != replacement.state
            || member.node_public_key != replacement.node_public_key
            || member.capabilities != capabilities
            || member.allowed_capabilities != capabilities
            || member.endpoints != next_endpoints;
        if changed {
            member.display_name = replacement.display_name;
            member.node_role = replacement.node_role;
            member.role = replacement.role;
            member.state = replacement.state;
            member.node_public_key = replacement.node_public_key;
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities;
            member.endpoints = next_endpoints;
            member.updated_at = timestamp;
        }
        changed
    } else {
        let mut member = replacement;
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = endpoints;
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        state.members.push(member);
        true
    }
}

fn daemon_tcp_endpoint_is_listener_advertisement(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::Loopback | ClusterTransportKind::LanTcp
    ) && matches!(
        endpoint.source,
        ClusterEndpointSource::Static | ClusterEndpointSource::Observed
    )
}

pub fn cluster_raft_local_is_voter(state: &ClusterBootstrapState) -> bool {
    cluster_raft_node_is_voter(state, &state.local_node_id)
}

pub fn cluster_state_needs_join_registration(state: &ClusterBootstrapState) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_none_or(|member| {
            matches!(member.role, ClusterMemberRole::Learner) && !member.state.is_authorized()
        })
}

pub fn merge_cluster_join_registration(
    local: &mut ClusterBootstrapState,
    accepted: ClusterBootstrapState,
) {
    let local_node_id = local.local_node_id.clone();
    let local_node_display_name = local.local_node_display_name.clone();
    let initialized_at = local.initialized_at;
    let local_remote_route_endpoints = locally_learned_remote_route_endpoints(local);
    *local = accepted;
    local.local_node_id = local_node_id;
    local.local_node_display_name = local_node_display_name;
    local.initialized_at = initialized_at;
    preserve_locally_learned_remote_route_endpoints(local, local_remote_route_endpoints);
}

pub fn cluster_status_summary_label(state: &ClusterBootstrapState) -> &'static str {
    if cluster_state_needs_join_registration(state) {
        "pending-join"
    } else if cluster_raft_local_is_voter(state) {
        "voter"
    } else {
        "read-only"
    }
}

pub fn cluster_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && cluster_member_has_raft_vote(member)
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run")
    })
}

pub fn cluster_member_has_raft_vote(member: &ClusterMemberRecord) -> bool {
    matches!(member.state, ClusterMemberState::Voter)
        || (matches!(member.state, ClusterMemberState::Active)
            && matches!(member.role, ClusterMemberRole::Voter))
}

pub fn cluster_raft_quorum_size(state: &ClusterBootstrapState) -> usize {
    let voters = state
        .members
        .iter()
        .filter(|member| cluster_raft_node_is_voter(state, &member.node_id))
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>()
        .len();
    voters / 2 + 1
}

pub fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths)?;
    let local_member = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id && member.state.is_authorized())
        .cloned();
    let local_remote_route_endpoints = locally_learned_remote_route_endpoints(&state);
    let kv_members = match read_cluster_members_from_kv(paths) {
        Ok(members) => members,
        Err(error) => {
            eprintln!(
                "warning: ignoring unreadable cluster kv snapshot while loading members: {error}"
            );
            Vec::new()
        }
    };
    if !kv_members.is_empty() {
        let local_process_members = state
            .members
            .iter()
            .filter(|member| {
                cluster_node_is_local_process(&state, &member.node_id)
                    && member.state.is_authorized()
            })
            .cloned()
            .collect::<Vec<_>>();
        state.members = kv_members;
        preserve_locally_learned_remote_route_endpoints(&mut state, local_remote_route_endpoints);
        for local_member in local_process_members {
            if let Some(mut kv_member) = state
                .members
                .iter()
                .find(|member| member.node_id == local_member.node_id)
                .cloned()
            {
                let mut local_member = local_member;
                for endpoint in kv_member.endpoints.drain(..) {
                    upsert_member_endpoint(&mut local_member, endpoint);
                }
                cleanup_member_iroh_endpoints(&mut local_member);
                upsert_member_record(&mut state, local_member);
            } else {
                upsert_member_record(&mut state, local_member);
            }
        }
        state
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        cleanup_cluster_iroh_endpoints(&mut state);
        return Ok(state);
    }
    for member in kv_members {
        if member.node_id == state.local_node_id
            && let Some(mut local_member) = state
                .members
                .iter()
                .find(|candidate| candidate.node_id == member.node_id)
                .cloned()
            && local_member.state.is_authorized()
        {
            for endpoint in member.endpoints {
                upsert_member_endpoint(&mut local_member, endpoint);
            }
            cleanup_member_iroh_endpoints(&mut local_member);
            upsert_member_record(&mut state, local_member);
            continue;
        }
        merge_member_record(&mut state, member);
    }
    state
        .members
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));
    if let Some(local_member) = local_member {
        upsert_member_record(&mut state, local_member);
    }
    cleanup_cluster_iroh_endpoints(&mut state);
    Ok(state)
}

fn locally_learned_remote_route_endpoints(
    state: &ClusterBootstrapState,
) -> BTreeMap<NodeId, Vec<ClusterEndpointRecord>> {
    let local_process_node_ids = state
        .members
        .iter()
        .filter(|member| {
            cluster_node_is_local_process(state, &member.node_id) && member.state.is_authorized()
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized() && !local_process_node_ids.contains(&member.node_id)
        })
        .filter_map(|member| {
            let endpoints = member
                .endpoints
                .iter()
                .filter(|endpoint| locally_learned_route_endpoint(endpoint))
                .cloned()
                .collect::<Vec<_>>();
            (!endpoints.is_empty()).then_some((member.node_id.clone(), endpoints))
        })
        .collect()
}

fn locally_learned_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

fn preserve_locally_learned_remote_route_endpoints(
    state: &mut ClusterBootstrapState,
    endpoints_by_node: BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
) {
    for (node_id, endpoints) in endpoints_by_node {
        let Some(member) = state
            .members
            .iter_mut()
            .find(|member| member.node_id == node_id && member.state.is_authorized())
        else {
            continue;
        };
        for endpoint in endpoints {
            upsert_member_endpoint(member, endpoint);
        }
    }
}

pub fn write_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_object_file(&paths.state, state)?;
    record_current_cluster_kv_reader_node_id_cache(paths, state.local_node_id.clone());
    Ok(())
}

pub fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_local_cache(paths)
        .cached_snapshot()
        .map_err(Into::into)
}

pub fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let wrote = write_cluster_kv_snapshot_materialized_changed(paths, snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(())
}

pub fn write_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = write_cluster_kv_snapshot_changed_without_notify(paths, snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

pub fn write_imported_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = write_imported_cluster_kv_snapshot_changed_without_notify(paths, snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

fn write_cluster_kv_snapshot_materialized_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = cluster_kv_local_cache(paths)
        .write_snapshot_if_materialized_changed(snapshot)
        .map_err(NetworkError::from)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

fn write_cluster_kv_snapshot_changed_without_notify(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = cluster_kv_local_cache(paths)
        .write_snapshot_if_changed(snapshot)
        .map_err(NetworkError::from)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

fn write_imported_cluster_kv_snapshot_changed_without_notify(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    if cluster_kv_imported_snapshot_can_defer_materialized_delta(paths, snapshot)? {
        observe_cluster_kv_snapshot_write(paths, false);
        return Ok(false);
    }
    write_cluster_kv_snapshot_changed_without_notify(paths, snapshot)
}

fn write_cluster_kv_snapshot_after_commit(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
    events: &[KvEvent],
) -> Result<bool, NetworkError> {
    if cluster_kv_events_can_defer_materialized_snapshot(events)
        && cluster_kv_materialized_snapshot_is_fresh_enough(paths, snapshot)?
    {
        observe_cluster_kv_snapshot_write(paths, false);
        return Ok(false);
    }
    write_cluster_kv_snapshot_changed_without_notify(paths, snapshot)
}

fn cluster_kv_events_can_defer_materialized_snapshot(events: &[KvEvent]) -> bool {
    !events.is_empty()
        && events
            .iter()
            .all(|event| cluster_kv_key_can_defer_materialized_snapshot(&event.key))
}

fn cluster_kv_key_can_defer_materialized_snapshot(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
        || key.starts_with("/cluster/network/info/")
        || key.starts_with("/cluster/node-status/")
}

fn cluster_kv_imported_snapshot_can_defer_materialized_delta(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let Some(existing) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    if !cluster_kv_snapshot_delta_only_deferable(existing.as_ref(), snapshot) {
        return Ok(false);
    }
    cluster_kv_materialized_snapshot_is_fresh_enough_against(paths, existing.revision, snapshot)
}

fn cluster_kv_snapshot_delta_only_deferable(existing: &KvSnapshot, snapshot: &KvSnapshot) -> bool {
    if existing.leases.len() != snapshot.leases.len() {
        return false;
    }
    let leases_match = existing
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks))
        .eq(snapshot
            .leases
            .iter()
            .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks)));
    if !leases_match {
        return false;
    }

    let mut changed = false;
    let mut existing_entries = existing.entries.iter().peekable();
    let mut incoming_entries = snapshot.entries.iter().peekable();
    loop {
        match (existing_entries.peek(), incoming_entries.peek()) {
            (Some(existing_entry), Some(incoming_entry)) => {
                match existing_entry.key.cmp(&incoming_entry.key) {
                    std::cmp::Ordering::Less => {
                        if !cluster_kv_key_can_defer_materialized_snapshot(&existing_entry.key) {
                            return false;
                        }
                        changed = true;
                        existing_entries.next();
                    }
                    std::cmp::Ordering::Greater => {
                        if !cluster_kv_key_can_defer_materialized_snapshot(&incoming_entry.key) {
                            return false;
                        }
                        changed = true;
                        incoming_entries.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if existing_entry.value != incoming_entry.value
                            || existing_entry.lease_id != incoming_entry.lease_id
                        {
                            if !cluster_kv_key_can_defer_materialized_snapshot(&incoming_entry.key)
                            {
                                return false;
                            }
                            changed = true;
                        }
                        existing_entries.next();
                        incoming_entries.next();
                    }
                }
            }
            (Some(existing_entry), None) => {
                if !cluster_kv_key_can_defer_materialized_snapshot(&existing_entry.key) {
                    return false;
                }
                changed = true;
                existing_entries.next();
            }
            (None, Some(incoming_entry)) => {
                if !cluster_kv_key_can_defer_materialized_snapshot(&incoming_entry.key) {
                    return false;
                }
                changed = true;
                incoming_entries.next();
            }
            (None, None) => return changed,
        }
    }
}

fn cluster_kv_materialized_snapshot_is_fresh_enough(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let Some(existing) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    cluster_kv_materialized_snapshot_is_fresh_enough_against(paths, existing.revision, snapshot)
}

fn cluster_kv_materialized_snapshot_is_fresh_enough_against(
    paths: &ClusterPaths,
    existing_revision: KvRevision,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    if existing_revision > snapshot.revision {
        return Ok(false);
    }
    let revision_lag = snapshot.revision.0.saturating_sub(existing_revision.0);
    if revision_lag >= CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG {
        return Ok(false);
    }
    if cluster_kv_materialized_snapshot_is_due(paths) {
        return Ok(false);
    }
    Ok(true)
}

fn cluster_kv_materialized_snapshot_is_due(paths: &ClusterPaths) -> bool {
    let Ok(metadata) = fs::metadata(&paths.kv_snapshot) else {
        return true;
    };
    let Some(age) = metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
    else {
        return true;
    };
    age >= CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL
}

fn observe_cluster_kv_snapshot_write(paths: &ClusterPaths, wrote: bool) {
    if let Some(observer) = io_observer() {
        if wrote {
            observer.on_kv_snapshot_actual_write();
            // The snapshot is written inside sinomo-kv, so account its on-disk
            // byte size with a single stat after a confirmed write.
            if let Ok(metadata) = fs::metadata(&paths.kv_snapshot) {
                observer.on_disk_write(metadata.len());
            }
        } else {
            observer.on_kv_snapshot_skipped_write();
        }
    }
}

pub fn read_cluster_kv_store(paths: &ClusterPaths) -> Result<InProcessKvStore, NetworkError> {
    read_cluster_kv_current_snapshot_arc(paths)
        .map(|snapshot| InProcessKvStore::restore((*snapshot).clone()))
}

pub fn read_cluster_kv_current_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    read_cluster_kv_current_snapshot_arc(paths).map(|snapshot| (*snapshot).clone())
}

fn read_cluster_kv_current_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_current_snapshot_read_for_test(paths);

    let node_id = read_cluster_kv_reader_node_id(paths)?;
    cluster_kv_local_cache(paths)
        .current_snapshot_arc_without_materialized_refresh(node_id)
        .map_err(Into::into)
}

fn read_cluster_kv_current_or_cached_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(NetworkError::ClusterOrphan) => read_cluster_kv_cached_snapshot_if_present(paths),
        Err(error) => Err(error),
    }
}

fn read_cluster_kv_current_or_empty_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)
        .map(|snapshot| snapshot.unwrap_or_else(|| Arc::new(InProcessKvStore::new().snapshot())))
}

pub fn read_cluster_kv_entry(
    paths: &ClusterPaths,
    key: &str,
) -> Result<Option<KvEntry>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    Ok(snapshot.get(key).cloned())
}

pub fn read_cluster_kv_prefix_entries(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

pub fn read_cluster_kv_prefix_entries_limited(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        paths,
        prefix,
        max_entries,
        CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

pub(crate) fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    snapshot
        .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
        .map_err(Into::into)
}

pub fn read_cluster_kv_value<T>(paths: &ClusterPaths, key: &str) -> Result<Option<T>, NetworkError>
where
    T: serde::de::DeserializeOwned,
{
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    snapshot
        .get(key)
        .map(|entry| entry.decode())
        .transpose()
        .map_err(Into::into)
}

pub fn read_cluster_kv_prefix_values<T>(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<T>, NetworkError>
where
    T: serde::de::DeserializeOwned,
{
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    snapshot
        .range_prefix_serde_limited_by_payload_bytes(
            prefix,
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
        )
        .map_err(Into::into)
}

pub fn read_cluster_kv_snapshot_store(
    paths: &ClusterPaths,
) -> Result<InProcessKvStore, NetworkError> {
    cluster_kv_local_cache(paths)
        .cached_store()
        .map_err(Into::into)
}

fn read_cluster_kv_cached_snapshot(paths: &ClusterPaths) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_cached_snapshot_read_for_test(paths);
    cluster_kv_local_cache(paths)
        .cached_snapshot_arc()
        .map_err(NetworkError::from)
}

fn read_cluster_kv_cached_snapshot_if_present(
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
    KvLocalCache::new(paths.kv_snapshot.clone(), paths.kv_raft.clone())
        .with_max_materialized_events(MAX_CLUSTER_KV_MATERIALIZED_EVENTS)
}

fn read_cluster_kv_reader_node_id(paths: &ClusterPaths) -> Result<NodeId, NetworkError> {
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

pub fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    KvWalPaths::new(paths.kv_raft.join(node_id.as_str()))
}

pub fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    let cache = cluster_kv_local_cache(paths);
    let mut node = cache.open_raft_node(node_id.clone())?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    Ok(node)
}

fn open_cluster_raft_kv_node_discarding_uncommitted_tail(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    let cache = cluster_kv_local_cache(paths);
    let mut node = cache.open_raft_node_discarding_uncommitted_tail(node_id.clone())?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    Ok(node)
}

pub fn restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    let restored = cluster_kv_local_cache(paths)
        .restore_raft_from_snapshot_if_newer(node_id.clone(), snapshot)
        .map_err(NetworkError::from)?;
    if restored {
        notify_cluster_kv_watchers(paths);
    }
    Ok(restored)
}

pub async fn open_cluster_raft_kv_node_async(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    let cache = cluster_kv_local_cache(paths);
    let mut node = cache.open_raft_node_async(node_id.clone()).await?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    Ok(node)
}

pub fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command_with_response(paths, node_id, command).map(|(snapshot, _)| snapshot)
}

pub fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let mut node = open_cluster_raft_kv_node_discarding_uncommitted_tail(paths, node_id)?;
    match &command {
        KvCommand::Op(op) => {
            if node.store().op_response_if_no_effect(op)?.is_some() {
                return Ok((node.store().snapshot(), None));
            }
        }
        KvCommand::Txn(txn) => {
            if let Some(response) = node
                .store()
                .txn_response_if_selected_branch_no_effect(txn)?
            {
                return Ok((node.store().snapshot(), Some(response)));
            }
        }
        KvCommand::PutClusterMember(member) => {
            let op = cluster_member_put_op(member)?;
            if node.store().op_response_if_no_effect(&op)?.is_some() {
                return Ok((node.store().snapshot(), None));
            }
        }
    }
    let before_revision = node.store().revision();
    let term = node.hard_state().current_term.max(1);
    let entry = node.append_local_command(term, command)?;
    node.commit_to(entry.index)?;
    let txn_response = node.txn_response_at(entry.index).cloned();
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    let snapshot = node.store().snapshot();
    let events: Vec<KvEvent> = snapshot
        .events
        .iter()
        .filter(|event| event.revision > before_revision)
        .cloned()
        .collect();
    write_cluster_kv_snapshot_after_commit(paths, &snapshot, &events)?;
    notify_cluster_kv_watchers_for_events(paths, &events);
    Ok((snapshot, txn_response))
}

pub fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    append_cluster_raft_command_uncommitted_entry(paths, node_id, command)
}

#[derive(Clone, Debug)]
pub enum ClusterRaftUncommittedAppend {
    Appended(KvLogEntry),
    NoEffect {
        snapshot: KvSnapshot,
        txn_response: Option<KvTxnResponse>,
    },
}

pub fn append_cluster_raft_command_uncommitted_or_no_effect(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<ClusterRaftUncommittedAppend, NetworkError> {
    let mut node = open_cluster_raft_kv_node_discarding_uncommitted_tail(paths, node_id)?;
    match &command {
        KvCommand::Op(op) => {
            if node.store().op_response_if_no_effect(op)?.is_some() {
                return Ok(ClusterRaftUncommittedAppend::NoEffect {
                    snapshot: node.store().snapshot(),
                    txn_response: None,
                });
            }
        }
        KvCommand::Txn(txn) => {
            if let Some(response) = node
                .store()
                .txn_response_if_selected_branch_no_effect(txn)?
            {
                return Ok(ClusterRaftUncommittedAppend::NoEffect {
                    snapshot: node.store().snapshot(),
                    txn_response: Some(response),
                });
            }
        }
        KvCommand::PutClusterMember(member) => {
            let op = cluster_member_put_op(member)?;
            if node.store().op_response_if_no_effect(&op)?.is_some() {
                return Ok(ClusterRaftUncommittedAppend::NoEffect {
                    snapshot: node.store().snapshot(),
                    txn_response: None,
                });
            }
        }
    }
    let entry = append_cluster_raft_command_to_open_node(paths, &mut node, command)?;
    Ok(ClusterRaftUncommittedAppend::Appended(entry))
}

fn append_cluster_raft_command_uncommitted_entry(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    let mut node = open_cluster_raft_kv_node_discarding_uncommitted_tail(paths, node_id)?;
    append_cluster_raft_command_to_open_node(paths, &mut node, command)
}

fn append_cluster_raft_command_to_open_node(
    paths: &ClusterPaths,
    node: &mut DurableRaftKvNode,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    let term = node.hard_state().current_term.max(1);
    let entry = node.append_local_command(term, command)?;
    compact_cluster_raft_node_if_needed(paths, node)?;
    Ok(entry)
}

fn cluster_member_put_op(member: &ClusterMemberRecord) -> Result<KvOp, NetworkError> {
    KvOp::put_serde(cluster_member_key(&member.node_id), member, None).map_err(NetworkError::from)
}

pub fn discard_cluster_raft_uncommitted_tail(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<(), NetworkError> {
    let mut node = open_cluster_raft_kv_node_discarding_uncommitted_tail(paths, node_id)?;
    node.discard_uncommitted_tail()?;
    compact_cluster_raft_node_if_needed(paths, &mut node)
}

pub fn commit_cluster_raft_index(
    paths: &ClusterPaths,
    node_id: &NodeId,
    commit_index: u64,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let result = commit_cluster_raft_index_with_events(paths, node_id, commit_index)?;
    Ok((result.snapshot, result.txn_response))
}

#[derive(Clone, Debug)]
pub struct ClusterRaftCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
}

pub fn commit_cluster_raft_index_with_events(
    paths: &ClusterPaths,
    node_id: &NodeId,
    commit_index: u64,
) -> Result<ClusterRaftCommitResult, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    let before_revision = node.store().revision();
    node.commit_to(commit_index)?;
    let txn_response = node.txn_response_at(commit_index).cloned();
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    let snapshot = node.store().snapshot();
    let events: Vec<KvEvent> = snapshot
        .events
        .iter()
        .filter(|event| event.revision > before_revision)
        .cloned()
        .collect();
    write_cluster_kv_snapshot_after_commit(paths, &snapshot, &events)?;
    notify_cluster_kv_watchers_for_events(paths, &events);
    Ok(ClusterRaftCommitResult {
        snapshot,
        txn_response,
        events,
    })
}

pub fn apply_cluster_raft_append(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: Option<RaftSnapshot>,
    append: RaftAppendEntriesRequest,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    let before_revision = node.store().revision();
    let mut snapshot_restored = false;
    if let Some(snapshot) = snapshot {
        let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
            && (append.prev_log_index != snapshot.last_included_index
                || append.prev_log_term == snapshot.last_included_term);
        let snapshot_is_newer = snapshot.last_included_index > node.applied_index();
        let snapshot_repairs_divergent_compaction = snapshot.last_included_term
            > node.last_log_term()
            && snapshot.kv.revision >= node.store().revision();
        let append_term_is_current = append.term >= node.hard_state().current_term;
        if append_term_is_current
            && snapshot_matches_append
            && (snapshot_is_newer || snapshot_repairs_divergent_compaction)
        {
            node.restore_snapshot(snapshot)?;
            snapshot_restored = true;
        }
    }
    let response = node.append_entries(append)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    let snapshot = node.store().snapshot();
    if snapshot_restored {
        write_cluster_kv_snapshot_changed_without_notify(paths, &snapshot)?;
        notify_cluster_kv_watchers(paths);
    } else if snapshot.revision != before_revision {
        let events: Vec<KvEvent> = snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
            .cloned()
            .collect();
        write_cluster_kv_snapshot_after_commit(paths, &snapshot, &events)?;
        notify_cluster_kv_watchers_for_events(paths, &events);
    }
    Ok(response)
}

pub fn apply_cluster_raft_vote(
    paths: &ClusterPaths,
    node_id: &NodeId,
    vote: RaftRequestVoteRequest,
) -> Result<RaftRequestVoteResponse, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.request_vote(vote).map_err(Into::into)
}

pub fn observe_cluster_raft_term(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<bool, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    let changed = node.observe_term(term)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    if changed {
        mirror_cluster_raft_snapshot(paths, &node)?;
    }
    Ok(changed)
}

pub fn set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.become_leader(term)?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)
}

pub fn clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let mut node = open_cluster_raft_kv_node(paths, node_id)?;
    node.clear_known_leader()?;
    compact_cluster_raft_node_if_needed(paths, &mut node)?;
    mirror_cluster_raft_snapshot(paths, &node)
}

pub fn cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    // Read only the tiny hard-state file rather than opening the full raft node
    // (which replays the multi-MB WAL). This is a read-only leadership query, hit
    // repeatedly by the maintenance tick and KV-commit preflight; opening the node
    // here was the dominant source of idle disk reads on the leader.
    let hard_state =
        cluster_raft_wal_paths(paths, &state.local_node_id).read_persisted_hard_state()?;
    Ok(hard_state
        .leader_id
        .filter(|leader_id| cluster_raft_node_is_voter(state, leader_id)))
}

pub fn cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    Ok(
        cluster_raft_known_leader_id(paths, state)?.as_ref() == Some(&state.local_node_id)
            && cluster_raft_local_is_voter(state),
    )
}

pub fn cluster_raft_vote_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_sync_targets_with_connectivity(paths, state)
        .into_iter()
        .filter(|endpoint| cluster_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub fn compact_cluster_raft_node_if_needed(
    paths: &ClusterPaths,
    node: &mut DurableRaftKvNode,
) -> Result<(), NetworkError> {
    // Snapshotting rewrites the entire WAL (down to the retained tail) plus the
    // snapshot file, so it must only run when it makes durable progress.
    //
    // Compaction advances the on-disk snapshot to `applied_index` and drops log
    // entries older than the retained tail. The ONLY thing it can durably remove is
    // applied entries that the on-disk snapshot does not already cover, i.e.
    // `applied_index - snapshot_index`. The in-memory log length and event-history
    // count are NOT valid triggers: they also include the retained replication tail
    // plus the single committed entry that `open` replays on top of the snapshot
    // (the snapshot is taken at `applied_index`, but one committed entry typically
    // lives beyond it in the WAL). Triggering on those made every raft-node open at
    // idle rewrite the full WAL+snapshot to drop one entry that the next open simply
    // regenerated — a perpetual ~MB/s write/read loop with no commits in flight.
    //
    // Gate on un-snapshotted applied progress (batched at 2x the retained tail) so an
    // idle node never compacts, and an active one compacts roughly every `retained`
    // committed entries. Oversized snapshots are compacted at idle only while their
    // event history can actually shrink; snapshots dominated by legitimate KV values
    // would otherwise keep serializing on every raft-node open without reducing disk
    // usage.
    let retained = sinomo_kv::RAFT_COMPACTION_RETAINED_ENTRIES;
    let compaction_high_watermark = retained.saturating_mul(2);

    let wal_paths = cluster_raft_wal_paths(paths, node.node_id());
    let snapshot_bytes = fs::metadata(&wal_paths.snapshot)
        .map(|metadata| metadata.len())
        .unwrap_or(0);

    let unsnapshotted_applied = node.applied_index().saturating_sub(node.snapshot_index());
    let has_compactable_progress = unsnapshotted_applied > compaction_high_watermark;
    let oversized_snapshot_can_shrink = snapshot_bytes > cluster_kv_raft_snapshot_byte_limit()
        && node.store().event_history_len() > MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS;

    if has_compactable_progress || oversized_snapshot_can_shrink {
        node.compact_event_history_to(MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
        node.save_snapshot()?;
    }
    Ok(())
}

pub fn mirror_cluster_raft_snapshot(
    paths: &ClusterPaths,
    node: &DurableRaftKvNode,
) -> Result<KvSnapshot, NetworkError> {
    let snapshot = node.store().snapshot();
    write_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
    Ok(snapshot)
}

pub fn create_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    if request.start_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        return Err(NetworkError::Kv(KvError::Compacted {
            requested: request.start_revision,
            compacted: snapshot.compacted_revision,
        }));
    }
    fs::create_dir_all(&paths.kv_watches).map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watches.display().to_string(),
        detail: source.to_string(),
    })?;
    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let mut allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let mut scanned = false;
    if cluster_kv_watch_allocator_needs_scan(paths, &allocator, snapshot.compacted_revision) {
        let scan = scan_cluster_kv_watches(paths, snapshot.compacted_revision)?;
        allocator.next_watch_id = allocator
            .next_watch_id
            .max(scan.max_seen_id.saturating_add(1))
            .max(1);
        allocator.active_watch_count = Some(scan.retained);
        allocator.pruned_compacted_revision = Some(snapshot.compacted_revision);
        scanned = true;
    }
    let active_watch_count = allocator.active_watch_count.unwrap_or_default();
    if active_watch_count >= MAX_CLUSTER_KV_ACTIVE_WATCHES {
        if scanned {
            persist_cluster_kv_watch_allocator_state(paths, &allocator)?;
        }
        return Err(NetworkError::Kv(KvError::WatchLimit {
            max: MAX_CLUSTER_KV_ACTIVE_WATCHES,
        }));
    }
    let mut watch_id = WatchId(allocator.next_watch_id.max(1));
    loop {
        let registration = ClusterKvWatchRegistration {
            watch_id,
            prefix: request.prefix.clone(),
            next_revision: request.start_revision,
        };
        let watch_path = cluster_kv_watch_path(paths, watch_id);
        if write_object_file_create_new_serialized(&watch_path, &registration)? {
            invalidate_active_cluster_kv_watch_registration_cache(paths);
            record_current_cluster_kv_watch_registration_cache(&watch_path, registration.clone());
            allocator.next_watch_id = watch_id.0.saturating_add(1).max(1);
            allocator.active_watch_count = Some(active_watch_count.saturating_add(1));
            allocator.pruned_compacted_revision = Some(snapshot.compacted_revision);
            if let Err(error) = persist_cluster_kv_watch_allocator_state(paths, &allocator) {
                remove_cluster_kv_watch_file(&watch_path);
                return Err(error);
            }
            return Ok(registration);
        }
        if watch_id.0 == u64::MAX {
            return Err(NetworkError::Serialization {
                detail: "cluster kv watch id space exhausted".to_string(),
            });
        }
        watch_id = WatchId(watch_id.0.saturating_add(1).max(1));
    }
}

pub fn drain_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once(paths, request.watch_id);
    }

    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once(paths, request.watch_id);
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response = drain_cluster_kv_watch_once(paths, request.watch_id)?;
        if !response.events.is_empty() {
            return Ok(response);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(response);
        }
        if !wait_for_cluster_kv_watch_change_for_watch(
            paths,
            request.watch_id,
            observed_generation,
            remaining,
        ) {
            return Ok(response);
        }
    }
}

fn drain_cluster_kv_watch_once(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    drain_cluster_kv_watch_once_limited_by_payload_bytes(
        paths,
        watch_id,
        MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
    )
}

fn cached_cluster_kv_watch_empty_drain_response(
    paths: &ClusterPaths,
    watch_path: &PathBuf,
    watch_id: WatchId,
    observed_generation: ClusterKvWatchObservedGeneration,
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

pub(crate) fn drain_cluster_kv_watch_once_limited_by_payload_bytes(
    paths: &ClusterPaths,
    watch_id: WatchId,
    max_payload_bytes: usize,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    #[cfg(test)]
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
    let mut registration = registration_read.registration.clone();
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    if registration.next_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        remove_cluster_kv_watch_registration_with_allocator(paths, &watch_path)?;
        return Err(NetworkError::Kv(KvError::Compacted {
            requested: registration.next_revision,
            compacted: snapshot.compacted_revision,
        }));
    }
    let persisted_events = cluster_kv_watch_events_arc(paths)?;
    let events = collect_cluster_kv_watch_drain_events(
        persisted_events.as_ref(),
        &snapshot,
        &registration,
        max_payload_bytes,
    );
    if let Some(last) = events.last() {
        registration.next_revision = KvRevision(last.revision.0.saturating_add(1));
        update_cluster_kv_watch_registration_after_drain(
            &watch_path,
            &registration_read,
            registration.clone(),
            events.len(),
        )?;
    } else if let Some(empty_drain_mark) = empty_drain_mark_before_read
        && cluster_kv_watch_empty_drain_mark(paths, observed_generation) == Some(empty_drain_mark)
    {
        record_cluster_kv_watch_registration_empty_drain_cache(
            &watch_path,
            &registration_read,
            registration.clone(),
            empty_drain_mark,
        );
    }
    Ok(ClusterKvWatchDrainResponse {
        watch_id: registration.watch_id,
        events,
        next_revision: registration.next_revision,
    })
}

fn update_cluster_kv_watch_registration_after_drain(
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    registration: ClusterKvWatchRegistration,
    drained_events: usize,
) -> Result<(), NetworkError> {
    if cluster_kv_watch_registration_cursor_should_persist(
        watch_path,
        registration_read,
        &registration,
        drained_events,
    ) {
        write_object_file(watch_path, &registration)?;
        record_current_cluster_kv_watch_registration_cache(watch_path, registration.clone());
    } else {
        record_cluster_kv_watch_registration_cache_with_durable(
            watch_path,
            registration_read.mark,
            registration.clone(),
            registration_read.durable_next_revision,
        );
    }
    refresh_active_cluster_kv_watch_registration_cache_for_watch_path(watch_path, registration);
    Ok(())
}

fn cluster_kv_watch_registration_cursor_should_persist(
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    registration: &ClusterKvWatchRegistration,
    drained_events: usize,
) -> bool {
    if drained_events >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS {
        return true;
    }
    let revision_lag = registration
        .next_revision
        .0
        .saturating_sub(registration_read.durable_next_revision.0);
    if revision_lag >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG {
        return true;
    }
    let Some(current_mark) = cluster_file_mark(watch_path) else {
        return true;
    };
    if current_mark != registration_read.mark {
        return true;
    }
    let Some(modified) = current_mark.modified else {
        return true;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE)
        .unwrap_or(true)
}

fn collect_cluster_kv_watch_drain_events(
    persisted_events: &[KvEvent],
    snapshot: &KvSnapshot,
    registration: &ClusterKvWatchRegistration,
    max_payload_bytes: usize,
) -> Vec<KvEvent> {
    let mut events = persisted_events
        .iter()
        .chain(snapshot.events.iter())
        .filter(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        })
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        (left.revision, left.key.as_str()).cmp(&(right.revision, right.key.as_str()))
    });
    events.dedup_by(|left, right| {
        left.revision == right.revision && left.key == right.key && left.kind == right.kind
    });
    limit_cluster_kv_watch_drain_events(events, max_payload_bytes)
}

fn limit_cluster_kv_watch_drain_events(
    events: Vec<&KvEvent>,
    max_payload_bytes: usize,
) -> Vec<KvEvent> {
    let mut limited = Vec::new();
    let mut payload_bytes = 0_usize;
    let mut events = events.into_iter().peekable();
    while let Some(event) = events.next() {
        let event_bytes = cluster_kv_watch_event_payload_bytes(&event);
        let next_payload_bytes = payload_bytes.saturating_add(event_bytes);
        if !limited.is_empty() && next_payload_bytes > max_payload_bytes {
            let current_revision = limited
                .last()
                .map(|last: &KvEvent| last.revision)
                .unwrap_or(KvRevision::ZERO);
            if event.revision != current_revision {
                break;
            }
        }
        payload_bytes = next_payload_bytes;
        limited.push(event.clone());
        if payload_bytes >= max_payload_bytes {
            let current_revision = event.revision;
            if !events
                .peek()
                .is_some_and(|candidate| candidate.revision == current_revision)
            {
                break;
            }
        }
    }
    limited
}

pub fn cluster_kv_watch_events_payload_bytes(events: &[KvEvent]) -> usize {
    events.iter().fold(0_usize, |total, event| {
        total.saturating_add(cluster_kv_watch_event_payload_bytes(event))
    })
}

pub fn cluster_kv_watch_event_payload_bytes(event: &KvEvent) -> usize {
    let value_bytes = match &event.kind {
        sinomo_kv::KvEventKind::Put { value } => value.len(),
        sinomo_kv::KvEventKind::Delete => 0,
    };
    event.key.len().saturating_add(value_bytes)
}

pub fn cancel_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, NetworkError> {
    let watch_path = cluster_kv_watch_path(paths, request.watch_id);
    let cancelled = if watch_path.exists() {
        remove_cluster_kv_watch_registration_with_allocator(paths, &watch_path)?
    } else {
        invalidate_cluster_kv_watch_registration_cache(&watch_path);
        false
    };
    if cancelled {
        notify_cluster_kv_watchers(paths);
    }
    Ok(ClusterKvWatchCancelResponse {
        watch_id: request.watch_id,
        cancelled,
    })
}

pub fn append_cluster_kv_watch_events(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    if cached_cluster_kv_watch_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if cached_cluster_kv_snapshot_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if persisted_cluster_kv_watch_events_contain_all(paths, incoming)? {
        return Ok(0);
    }
    let mut log_file = open_cluster_kv_watch_event_log_for_append(paths)?;
    log_file
        .lock_exclusive()
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    if let Some(appended) =
        append_cluster_kv_watch_events_from_cached_view(paths, &mut log_file, incoming)?
    {
        return Ok(appended);
    }
    let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
    let log = read_cluster_kv_watch_event_log_from_file(paths, &mut log_file)?;
    let log_was_truncated = log.truncated;
    let log_records = if log_was_truncated { 0 } else { log.records };
    let existing =
        compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
    if log_was_truncated {
        write_cluster_kv_watch_event_checkpoint(paths, &existing)?;
        truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
    }
    let retained = compact_cluster_kv_watch_events_incremental(existing.clone(), incoming);
    let appended = retained
        .iter()
        .filter(|event| !cluster_kv_events_contain(&existing, event))
        .cloned()
        .collect::<Vec<_>>();
    if appended.is_empty() {
        record_current_cluster_kv_watch_event_cache(paths, retained, log_records, true);
        return Ok(0);
    }
    let encoded_appended = encode_cluster_kv_watch_event_log_batch(paths, &appended)?;
    let log_len = log_file
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let should_checkpoint_without_append = incoming.len() > MAX_CLUSTER_KV_WATCH_EVENTS
        || log_records.saturating_add(appended.len()) >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
        || log_len.saturating_add(encoded_appended.bytes) >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES;
    let mut current_log_records = log_records;
    if should_checkpoint_without_append {
        write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
        truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
        current_log_records = 0;
    } else {
        append_cluster_kv_watch_event_log_to_file(paths, &mut log_file, &encoded_appended)?;
        current_log_records = current_log_records.saturating_add(appended.len());
        if log_file
            .metadata()
            .map(|metadata| metadata.len() >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES)
            .unwrap_or(false)
        {
            write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
            truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
            current_log_records = 0;
        }
    }
    record_current_cluster_kv_watch_event_cache(paths, retained, current_log_records, true);
    notify_cluster_kv_watchers_for_events(paths, &appended);
    Ok(appended.len())
}

pub fn append_cluster_kv_watch_events_for_active_watches(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    if cached_cluster_kv_watch_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if !cluster_kv_has_active_watch_registrations(paths)? {
        return Ok(0);
    }
    let incoming = cluster_kv_watch_events_matching_active_registrations(paths, incoming)?;
    if incoming.is_empty() {
        return Ok(0);
    }
    if cached_cluster_kv_snapshot_events_contain_all(paths, &incoming) {
        return Ok(0);
    }
    append_cluster_kv_watch_events(paths, &incoming)
}

fn cluster_kv_has_active_watch_registrations(paths: &ClusterPaths) -> Result<bool, NetworkError> {
    if !paths.kv_watches.exists() && !paths.kv_watches_next_id.exists() {
        return Ok(false);
    }
    if let Some(entry) = cluster_kv_watch_allocator_trust_entry(paths)
        && let Some(active_watch_count) = entry.active_watch_count
    {
        return Ok(active_watch_count > 0);
    }

    let compacted_revision =
        read_cluster_kv_current_or_empty_snapshot_arc(paths)?.compacted_revision;
    let mut allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let scan = scan_cluster_kv_watches(paths, compacted_revision)?;
    allocator.next_watch_id = allocator
        .next_watch_id
        .max(scan.max_seen_id.saturating_add(1))
        .max(1);
    allocator.active_watch_count = Some(scan.retained);
    allocator.pruned_compacted_revision = Some(compacted_revision);
    if paths.kv_watches.exists() || paths.kv_watches_next_id.exists() {
        persist_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    Ok(scan.retained > 0)
}

fn cluster_kv_watch_events_matching_active_registrations(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<Vec<KvEvent>, NetworkError> {
    let registrations = active_cluster_kv_watch_registrations(paths)?;
    if registrations.is_empty() {
        return Ok(Vec::new());
    }
    Ok(incoming
        .iter()
        .filter(|event| {
            registrations.iter().any(|registration| {
                event.revision >= registration.next_revision
                    && event.key.starts_with(&registration.prefix)
            })
        })
        .cloned()
        .collect())
}

fn active_cluster_kv_watch_registrations(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterKvWatchRegistration>, NetworkError> {
    let Some(watch_dir_mark_before) = cluster_file_mark(&paths.kv_watches) else {
        return Ok(Vec::new());
    };
    if !watch_dir_mark_before.exists {
        return Ok(Vec::new());
    }
    if let Some(registrations) =
        cached_active_cluster_kv_watch_registrations(paths, watch_dir_mark_before)
    {
        return Ok(registrations.as_ref().clone());
    }
    let entries = match fs::read_dir(&paths.kv_watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(NetworkError::StorageIo {
                path: paths.kv_watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let mut registrations = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watches.display().to_string(),
            detail: source.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(file_watch_id) = cluster_kv_watch_id_from_file_name(&name) else {
            continue;
        };
        let watch_id = WatchId(file_watch_id);
        match read_cluster_kv_watch_registration(&entry.path(), watch_id) {
            Ok(registration) => registrations.push(registration),
            Err(NetworkError::Kv(KvError::UnknownWatch { watch_id: returned }))
                if returned == watch_id => {}
            Err(error) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch registration {} during fan-out filtering: {}",
                    entry.path().display(),
                    error
                );
            }
        }
    }
    #[cfg(test)]
    record_cluster_kv_watch_dir_scan_for_test(paths);
    if cluster_file_mark(&paths.kv_watches) == Some(watch_dir_mark_before) {
        record_active_cluster_kv_watch_registrations(
            paths,
            watch_dir_mark_before,
            registrations.clone(),
        );
    }
    Ok(registrations)
}

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
            .all(|event| cluster_kv_events_contain(events.as_ref(), event)))
}

fn append_cluster_kv_watch_events_from_cached_view(
    paths: &ClusterPaths,
    log_file: &mut fs::File,
    incoming: &[KvEvent],
) -> Result<Option<usize>, NetworkError> {
    let Some((checkpoint_mark, log_mark)) = cluster_kv_watch_event_cache_marks(paths) else {
        return Ok(None);
    };
    let Some(cache) = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint_mark, log_mark)
    else {
        return Ok(None);
    };
    if !cache.append_safe {
        return Ok(None);
    }

    let existing = cache.events;
    let retained = compact_cluster_kv_watch_events_incremental(existing.as_ref().clone(), incoming);
    let appended = retained
        .iter()
        .filter(|event| !cluster_kv_events_contain(&existing, event))
        .cloned()
        .collect::<Vec<_>>();
    if appended.is_empty() {
        record_current_cluster_kv_watch_event_cache(
            paths,
            retained,
            cache.log_records,
            cache.append_safe,
        );
        return Ok(Some(0));
    }

    let mut current_log_records = cache.log_records;
    let encoded_appended = encode_cluster_kv_watch_event_log_batch(paths, &appended)?;
    let should_checkpoint_without_append = incoming.len() > MAX_CLUSTER_KV_WATCH_EVENTS
        || current_log_records.saturating_add(appended.len())
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
        || log_mark.len.saturating_add(encoded_appended.bytes)
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES;
    if should_checkpoint_without_append {
        write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
        truncate_cluster_kv_watch_event_log_file(paths, log_file)?;
        current_log_records = 0;
    } else {
        append_cluster_kv_watch_event_log_to_file(paths, log_file, &encoded_appended)?;
        current_log_records = current_log_records.saturating_add(appended.len());
        if log_file
            .metadata()
            .map(|metadata| metadata.len() >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES)
            .unwrap_or(false)
        {
            write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
            truncate_cluster_kv_watch_event_log_file(paths, log_file)?;
            current_log_records = 0;
        }
    }
    record_current_cluster_kv_watch_event_cache(paths, retained, current_log_records, true);
    notify_cluster_kv_watchers_for_events(paths, &appended);
    Ok(Some(appended.len()))
}

struct EncodedClusterKvWatchEventLogBatch {
    records: Vec<Vec<u8>>,
    bytes: u64,
}

fn encode_cluster_kv_watch_event_log_batch(
    paths: &ClusterPaths,
    events: &[KvEvent],
) -> Result<EncodedClusterKvWatchEventLogBatch, NetworkError> {
    let mut records = Vec::with_capacity(events.len());
    let mut bytes = 0_u64;
    for event in events {
        let record = SinomoObjectCodec::default().encode(event)?;
        let len = u32::try_from(record.len()).map_err(|_| NetworkError::Serialization {
            detail: format!(
                "cluster kv watch-event log record for {} exceeds u32 length",
                paths.kv_watch_events_log.display()
            ),
        })?;
        bytes = bytes.saturating_add(4).saturating_add(u64::from(len));
        records.push(record);
    }
    Ok(EncodedClusterKvWatchEventLogBatch { records, bytes })
}

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
        .all(|event| cluster_kv_events_contain(entry.events.as_ref(), event))
}

fn cached_cluster_kv_snapshot_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> bool {
    read_cluster_kv_cached_snapshot_if_present(paths).is_ok_and(|snapshot| {
        snapshot.is_some_and(|snapshot| {
            incoming
                .iter()
                .all(|event| cluster_kv_events_contain(&snapshot.events, event))
        })
    })
}

pub fn cluster_kv_watch_events(paths: &ClusterPaths) -> Result<Vec<KvEvent>, NetworkError> {
    cluster_kv_watch_events_arc(paths).map(|events| events.as_ref().clone())
}

fn cluster_kv_watch_events_arc(paths: &ClusterPaths) -> Result<Arc<Vec<KvEvent>>, NetworkError> {
    let cache_marks = cluster_kv_watch_event_cache_marks(paths);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        if let Some(events) = cached_cluster_kv_watch_events_arc(paths, checkpoint_mark, log_mark) {
            return Ok(events);
        }
    }
    let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
    let log = read_cluster_kv_watch_event_log(paths)?;
    let log_records = log.records;
    let append_safe = !log.truncated;
    let events =
        compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
    let events = Arc::new(events);
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

fn read_cluster_kv_watch_event_checkpoint(
    paths: &ClusterPaths,
) -> Result<Vec<KvEvent>, NetworkError> {
    read_optional_object_file(&paths.kv_watch_events).map(|events| events.unwrap_or_default())
}

fn write_cluster_kv_watch_event_checkpoint(
    paths: &ClusterPaths,
    events: &[KvEvent],
) -> Result<(), NetworkError> {
    let parent = paths
        .kv_watch_events
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    write_object_file(&paths.kv_watch_events, &events)?;
    invalidate_cluster_kv_watch_event_cache(paths);
    Ok(())
}

#[derive(Default)]
struct ClusterKvWatchEventLogRead {
    events: Vec<KvEvent>,
    records: usize,
    truncated: bool,
}

fn read_cluster_kv_watch_event_log(
    paths: &ClusterPaths,
) -> Result<ClusterKvWatchEventLogRead, NetworkError> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .open(&paths.kv_watch_events_log)
    {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ClusterKvWatchEventLogRead::default());
        }
        Err(source) => {
            return Err(NetworkError::StorageIo {
                path: paths.kv_watch_events_log.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    file.lock_shared()
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    read_cluster_kv_watch_event_log_from_file(paths, &mut file)
}

fn read_cluster_kv_watch_event_log_from_file(
    paths: &ClusterPaths,
    file: &mut fs::File,
) -> Result<ClusterKvWatchEventLogRead, NetworkError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut events = Vec::new();
    let mut record_number = 0_usize;
    let mut read_bytes = 0_u64;
    let mut needs_repair = false;
    loop {
        record_number += 1;
        let mut len_buf = [0_u8; 4];
        match read_cluster_kv_watch_event_log_chunk(paths, file, &mut len_buf)? {
            WatchEventLogReadChunk::Complete => {}
            WatchEventLogReadChunk::CleanEof => break,
            WatchEventLogReadChunk::Truncated => {
                eprintln!(
                    "warning: ignoring partial cluster kv watch-event log record {} in {}",
                    record_number,
                    paths.kv_watch_events_log.display()
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number.saturating_sub(1),
                    truncated: true,
                });
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        read_bytes = read_bytes.saturating_add(4).saturating_add(len as u64);
        if len == 0 || len > MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES {
            eprintln!(
                "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                record_number,
                paths.kv_watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        let mut record = vec![0_u8; len];
        if read_cluster_kv_watch_event_log_chunk(paths, file, &mut record)?
            != WatchEventLogReadChunk::Complete
        {
            eprintln!(
                "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                record_number,
                paths.kv_watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        match SinomoObjectCodec::default().decode(&record) {
            Ok(event) => events.push(event),
            Err(source) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch-event log record {} in {}: {}",
                    record_number,
                    paths.kv_watch_events_log.display(),
                    source
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number.saturating_sub(1),
                    truncated: true,
                });
            }
        }
        if record_number >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
            || read_bytes >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES
        {
            needs_repair = true;
        }
        if events.len() > MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS {
            events = compact_cluster_kv_watch_events(events);
        }
    }
    if needs_repair {
        eprintln!(
            "warning: repairing overgrown cluster kv watch-event log {} after reading {} records / {} bytes",
            paths.kv_watch_events_log.display(),
            record_number.saturating_sub(1),
            read_bytes
        );
    }
    Ok(ClusterKvWatchEventLogRead {
        events,
        records: record_number.saturating_sub(1),
        truncated: needs_repair,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WatchEventLogReadChunk {
    Complete,
    CleanEof,
    Truncated,
}

fn read_cluster_kv_watch_event_log_chunk(
    paths: &ClusterPaths,
    file: &mut fs::File,
    buf: &mut [u8],
) -> Result<WatchEventLogReadChunk, NetworkError> {
    let mut offset = 0;
    while offset < buf.len() {
        match file.read(&mut buf[offset..]) {
            Ok(0) if offset == 0 => return Ok(WatchEventLogReadChunk::CleanEof),
            Ok(0) => return Ok(WatchEventLogReadChunk::Truncated),
            Ok(read) => {
                offset += read;
                #[cfg(test)]
                record_cluster_kv_watch_event_log_read_for_test(paths, read as u64);
                if let Some(observer) = io_observer() {
                    observer.on_disk_read(read as u64);
                }
            }
            Err(source) => {
                return Err(NetworkError::StorageIo {
                    path: paths.kv_watch_events_log.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(WatchEventLogReadChunk::Complete)
}

fn open_cluster_kv_watch_event_log_for_append(
    paths: &ClusterPaths,
) -> Result<fs::File, NetworkError> {
    let parent = paths
        .kv_watch_events_log
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    match fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&paths.kv_watch_events_log)
    {
        Ok(file) => {
            sync_directory(parent)?;
            Ok(file)
        }
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&paths.kv_watch_events_log)
            .map_err(|source| NetworkError::StorageIo {
                path: paths.kv_watch_events_log.display().to_string(),
                detail: source.to_string(),
            }),
        Err(source) => Err(NetworkError::StorageIo {
            path: paths.kv_watch_events_log.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn append_cluster_kv_watch_event_log_to_file(
    paths: &ClusterPaths,
    file: &mut fs::File,
    batch: &EncodedClusterKvWatchEventLogBatch,
) -> Result<(), NetworkError> {
    if batch.records.is_empty() {
        return Ok(());
    }
    for record in &batch.records {
        let len = u32::try_from(record.len()).map_err(|_| NetworkError::Serialization {
            detail: format!(
                "cluster kv watch-event log record for {} exceeds u32 length",
                paths.kv_watch_events_log.display()
            ),
        })?;
        file.write_all(&len.to_be_bytes())
            .and_then(|()| file.write_all(record))
            .map_err(|source| NetworkError::StorageIo {
                path: paths.kv_watch_events_log.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    file.sync_data().map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    #[cfg(test)]
    record_cluster_kv_watch_event_log_write_for_test(paths, batch.bytes);
    if let Some(observer) = io_observer() {
        observer.on_disk_write(batch.bytes);
    }
    invalidate_cluster_kv_watch_event_cache(paths);
    Ok(())
}

fn truncate_cluster_kv_watch_event_log_file(
    paths: &ClusterPaths,
    file: &mut fs::File,
) -> Result<(), NetworkError> {
    file.set_len(0).map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    file.sync_data().map_err(|source| NetworkError::StorageIo {
        path: paths.kv_watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    invalidate_cluster_kv_watch_event_cache(paths);
    Ok(())
}

fn compact_cluster_kv_watch_events(mut events: Vec<KvEvent>) -> Vec<KvEvent> {
    events.sort_by(|left, right| {
        (left.revision, left.key.as_str()).cmp(&(right.revision, right.key.as_str()))
    });
    events.dedup_by(|left, right| {
        left.revision == right.revision && left.key == right.key && left.kind == right.kind
    });
    if events.len() > MAX_CLUSTER_KV_WATCH_EVENTS {
        let mut split_at = events.len() - MAX_CLUSTER_KV_WATCH_EVENTS;
        let retained_revision = events[split_at].revision;
        while split_at > 0 && events[split_at - 1].revision == retained_revision {
            split_at -= 1;
        }
        events.split_off(split_at)
    } else {
        events
    }
}

fn compact_cluster_kv_watch_events_incremental(
    mut retained: Vec<KvEvent>,
    incoming: &[KvEvent],
) -> Vec<KvEvent> {
    for event in incoming {
        if cluster_kv_events_contain(&retained, event) {
            continue;
        }
        retained.push(event.clone());
        if retained.len() > MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS {
            retained = compact_cluster_kv_watch_events(retained);
        }
    }
    compact_cluster_kv_watch_events(retained)
}

fn cluster_kv_events_contain(events: &[KvEvent], event: &KvEvent) -> bool {
    events.iter().any(|candidate| {
        candidate.revision == event.revision
            && candidate.key == event.key
            && candidate.kind == event.kind
    })
}

fn cluster_kv_watch_path(paths: &ClusterPaths, watch_id: WatchId) -> PathBuf {
    paths.kv_watches.join(format!("watch-{}.sobj", watch_id.0))
}

struct ClusterKvWatchRegistrationRead {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
}

fn read_cluster_kv_watch_registration(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    read_cluster_kv_watch_registration_with_cursor(path, watch_id).map(|read| read.registration)
}

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

fn read_cluster_kv_watch_registration_from_file(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let registration: ClusterKvWatchRegistration = match read_optional_object_file(path)? {
        Some(registration) => registration,
        None => {
            return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
        }
    };
    if registration.watch_id != watch_id {
        remove_cluster_kv_watch_file(path);
        return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
    }
    record_current_cluster_kv_watch_registration_cache(path, registration.clone());
    Ok(registration)
}

fn lock_cluster_kv_watch_allocator(paths: &ClusterPaths) -> Result<fs::File, NetworkError> {
    fs::create_dir_all(&paths.root).map_err(|source| NetworkError::StorageIo {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&paths.kv_watches_lock)
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watches_lock.display().to_string(),
            detail: source.to_string(),
        })?;
    file.lock_exclusive()
        .map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watches_lock.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(file)
}

fn remove_cluster_kv_watch_file(path: &PathBuf) {
    if let Err(error) = remove_cluster_kv_watch_file_if_exists(path) {
        eprintln!(
            "warning: could not remove stale cluster kv watch registration {}: {}",
            path.display(),
            error
        );
    }
}

fn remove_cluster_kv_watch_file_if_exists(path: &PathBuf) -> Result<bool, NetworkError> {
    let removed = remove_optional_file(path)?;
    invalidate_cluster_kv_watch_registration_cache(path);
    invalidate_active_cluster_kv_watch_registration_cache_for_watch_path(path);
    Ok(removed)
}

fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterPaths,
    path: &PathBuf,
) -> Result<bool, NetworkError> {
    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let removed = remove_cluster_kv_watch_file_if_exists(path)?;
    if removed {
        decrement_cluster_kv_watch_allocator_count(paths)?;
    }
    Ok(removed)
}

fn decrement_cluster_kv_watch_allocator_count(paths: &ClusterPaths) -> Result<(), NetworkError> {
    let mut allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let Some(active_watch_count) = allocator.active_watch_count else {
        return Ok(());
    };
    allocator.active_watch_count = Some(active_watch_count.saturating_sub(1));
    persist_cluster_kv_watch_allocator_state(paths, &allocator)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ClusterKvWatchScanResult {
    removed: usize,
    retained: usize,
    max_seen_id: u64,
}

fn scan_cluster_kv_watches(
    paths: &ClusterPaths,
    compacted_revision: KvRevision,
) -> Result<ClusterKvWatchScanResult, NetworkError> {
    let mut result = ClusterKvWatchScanResult::default();
    let entries = match fs::read_dir(&paths.kv_watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(source) => {
            return Err(NetworkError::StorageIo {
                path: paths.kv_watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    #[cfg(test)]
    record_cluster_kv_watch_dir_scan_for_test(paths);

    for entry in entries {
        let entry = entry.map_err(|source| NetworkError::StorageIo {
            path: paths.kv_watches.display().to_string(),
            detail: source.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(file_watch_id) = cluster_kv_watch_id_from_file_name(&name) else {
            continue;
        };
        result.max_seen_id = result.max_seen_id.max(file_watch_id);
        let path = entry.path();
        let watch_id = WatchId(file_watch_id);
        match read_cluster_kv_watch_registration(&path, watch_id) {
            Ok(registration)
                if compacted_revision != KvRevision::ZERO
                    && registration.next_revision <= compacted_revision =>
            {
                if remove_cluster_kv_watch_file_if_exists(&path)? {
                    result.removed = result.removed.saturating_add(1);
                }
            }
            Ok(_) => {
                result.retained = result.retained.saturating_add(1);
            }
            Err(NetworkError::Kv(KvError::UnknownWatch { watch_id: returned }))
                if returned == watch_id =>
            {
                result.removed = result.removed.saturating_add(1);
            }
            Err(error) => {
                eprintln!(
                    "warning: removing unreadable cluster kv watch registration {}: {}",
                    path.display(),
                    error
                );
                if remove_cluster_kv_watch_file_if_exists(&path)? {
                    result.removed = result.removed.saturating_add(1);
                }
            }
        }
    }
    Ok(result)
}

fn cluster_kv_watch_id_from_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("watch-")
        .and_then(|name| name.strip_suffix(".sobj"))
        .and_then(|id| id.parse::<u64>().ok())
}

fn cluster_kv_watch_allocator_needs_scan(
    paths: &ClusterPaths,
    allocator: &ClusterKvWatchAllocatorState,
    compacted_revision: KvRevision,
) -> bool {
    if !cluster_kv_watch_allocator_trusted(paths) {
        return true;
    }
    if allocator.active_watch_count.is_none() {
        return true;
    }
    match allocator.pruned_compacted_revision {
        Some(pruned) => pruned < compacted_revision,
        None => true,
    }
}

fn read_cluster_kv_watch_allocator_state(
    paths: &ClusterPaths,
) -> Result<ClusterKvWatchAllocatorState, NetworkError> {
    let state =
        read_optional_object_file::<ClusterKvWatchAllocatorState>(&paths.kv_watches_next_id)?
            .unwrap_or_else(|| ClusterKvWatchAllocatorState::legacy(1));
    Ok(ClusterKvWatchAllocatorState {
        next_watch_id: state.next_watch_id.max(1),
        active_watch_count: state.active_watch_count,
        pruned_compacted_revision: state.pruned_compacted_revision,
    })
}

fn persist_cluster_kv_watch_allocator_state(
    paths: &ClusterPaths,
    state: &ClusterKvWatchAllocatorState,
) -> Result<(), NetworkError> {
    #[cfg(test)]
    if consume_cluster_kv_watch_allocator_persist_failure_for_test(paths) {
        return Err(NetworkError::StorageIo {
            path: paths.kv_watches_next_id.display().to_string(),
            detail: "injected cluster kv watch allocator persist failure".to_string(),
        });
    }
    write_object_file(&paths.kv_watches_next_id, state)?;
    record_current_cluster_kv_watch_allocator_trust(paths, state.active_watch_count);
    Ok(())
}

pub fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    read_optional_object_file(&paths.state)?.ok_or(NetworkError::ClusterOrphan)
}

pub fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut members: Vec<ClusterMemberRecord> = Vec::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        members.push(entry.decode()?);
    }
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    for member in &mut members {
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        for endpoint in read_cluster_member_endpoints_from_snapshot(&snapshot, &member.node_id)? {
            if !member
                .endpoints
                .iter()
                .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
            {
                member.endpoints.push(endpoint);
            }
        }
        cleanup_member_iroh_endpoints(member);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(members)
}

pub fn read_cluster_endpoints_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut endpoints = read_cluster_endpoints_from_snapshot(&snapshot)?
        .into_values()
        .flatten()
        .collect::<Vec<_>>();
    endpoints.sort_by(|left, right| {
        (
            left.node_id.as_str(),
            left.priority,
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.node_id.as_str(),
                right.priority,
                right.endpoint_id.as_str(),
            ))
    });
    Ok(endpoints)
}

fn read_cluster_member_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
    node_id: &NodeId,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(node_id);
    let mut endpoints = Vec::new();
    for entry in
        snapshot.range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
    {
        endpoints.push(entry.decode()?);
    }
    Ok(endpoints)
}

fn read_cluster_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<NodeId, Vec<ClusterEndpointRecord>>, NetworkError> {
    let mut endpoints: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> = BTreeMap::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/endpoints/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let endpoint: ClusterEndpointRecord = entry.decode()?;
        endpoints
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    Ok(endpoints)
}

fn read_cluster_capability_policies_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<NodeId, ClusterCapabilityPolicyRecord>, NetworkError> {
    let mut policies = BTreeMap::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/policy/capabilities/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let policy: ClusterCapabilityPolicyRecord = entry.decode()?;
        policies.insert(policy.node_id.clone(), policy);
    }
    Ok(policies)
}

pub fn read_cluster_node_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNodeStatusRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut statuses: Vec<ClusterNodeStatusRecord> = Vec::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/node-status/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        statuses.push(entry.decode()?);
    }
    statuses.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(statuses)
}

pub fn read_cluster_module_rollout_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutStatusRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut statuses: Vec<ClusterModuleRolloutStatusRecord> = Vec::new();
    let prefix = cluster_module_rollout_node_prefix();
    for entry in snapshot.range_prefix_limited(&prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)? {
        statuses.push(entry.decode()?);
    }
    statuses.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.module_id.cmp(&right.module_id))
            .then_with(|| left.rollout_plan_id.cmp(&right.rollout_plan_id))
    });
    Ok(statuses)
}

pub fn read_cluster_module_rollout_desired_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutDesiredRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut desired: Vec<ClusterModuleRolloutDesiredRecord> = Vec::new();
    let prefix = cluster_module_rollout_desired_prefix();
    for entry in snapshot.range_prefix_limited(&prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)? {
        desired.push(entry.decode()?);
    }
    desired.sort_by(|left, right| {
        left.rollout_plan
            .module_id
            .cmp(&right.rollout_plan.module_id)
            .then_with(|| {
                left.rollout_plan
                    .rollout_plan_id
                    .cmp(&right.rollout_plan.rollout_plan_id)
            })
    });
    Ok(desired)
}

pub fn read_cluster_module_rollout_reconciliations_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutReconciliationRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut reconciliations: Vec<ClusterModuleRolloutReconciliationRecord> = Vec::new();
    let prefix = cluster_module_rollout_reconcile_prefix();
    for entry in snapshot.range_prefix_limited(&prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)? {
        reconciliations.push(entry.decode()?);
    }
    reconciliations.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.module_id.cmp(&right.module_id))
            .then_with(|| left.rollout_plan_id.cmp(&right.rollout_plan_id))
    });
    Ok(reconciliations)
}

pub fn read_cluster_network_info_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNetworkInfoRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let now = OffsetDateTime::now_utc();
    let mut records: Vec<ClusterNetworkInfoRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/network/info/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let record: ClusterNetworkInfoRecord = entry.decode()?;
        if cluster_network_record_live(record.expires_at, now) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(records)
}

pub fn read_cluster_connectivity_graph_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let now = OffsetDateTime::now_utc();
    let mut records: Vec<ClusterConnectivityRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/network/connectivity/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let record: ClusterConnectivityRecord = entry.decode()?;
        if cluster_network_record_live(record.expires_at, now) {
            records.push(record);
        }
    }
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
    Ok(records)
}

pub fn read_cluster_connectivity_graph(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    read_cluster_connectivity_graph_with_suppressed_local_source(paths, None)
}

fn read_cluster_connectivity_graph_with_suppressed_local_source(
    paths: &ClusterPaths,
    local_source_node_id: Option<&NodeId>,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let now = OffsetDateTime::now_utc();
    let local_records = read_local_cluster_connectivity_records(paths)?
        .into_iter()
        .filter(|record| cluster_network_record_live(record.expires_at, now))
        .collect::<Vec<_>>();
    // A node's own live probe results are authoritative for its outgoing edges. Drop any
    // KV-published edge whose source we hold local records for; when constructing the local
    // route graph, also drop KV-published edges for this node even if the in-process cache is
    // empty after restart/network change. Self-published KV edges can be stale (e.g. healthy
    // before roaming, or unreachable before reconnecting), while other sources' edges are the
    // remote topology we still need for relay planning.
    let local_sources = local_records
        .iter()
        .map(|record| record.source_node_id.clone())
        .chain(local_source_node_id.cloned())
        .collect::<std::collections::BTreeSet<_>>();
    let mut records_by_key =
        BTreeMap::<(NodeId, NodeId, ClusterEndpointId), ClusterConnectivityRecord>::new();
    for record in read_cluster_connectivity_graph_from_kv(paths)? {
        if local_sources.contains(&record.source_node_id) {
            continue;
        }
        records_by_key.insert(
            (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ),
            record,
        );
    }
    for record in local_records {
        records_by_key.insert(
            (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ),
            record,
        );
    }
    let mut records = records_by_key.into_values().collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
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

fn validate_cluster_connectivity_records_for_source(
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    if records.len() > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
        return Err(NetworkError::ClusterConnectivityRecordLimit {
            source_node_id: source_node_id.clone(),
            observed_records: records.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
        });
    }
    for record in records {
        if record.source_node_id != *source_node_id {
            return Err(NetworkError::ClusterConnectivitySourceMismatch {
                expected_source_node_id: source_node_id.clone(),
                record_source_node_id: record.source_node_id.clone(),
            });
        }
    }
    Ok(())
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

#[cfg(test)]
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
    #[cfg(test)]
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

fn local_cluster_connectivity_persisted_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(|(existing, merged)| {
                cluster_connectivity_stable_eq(existing, merged)
                    && !cluster_network_record_should_refresh(existing.expires_at, now)
            })
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
        .map(|record| {
            (
                (
                    record.source_node_id.clone(),
                    record.target_node_id.clone(),
                    record.endpoint_id.clone(),
                ),
                record.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for record in records {
        if record.source_node_id == *source_node_id
            && cluster_network_record_live(record.expires_at, now)
        {
            merged.insert(
                (
                    record.source_node_id.clone(),
                    record.target_node_id.clone(),
                    record.endpoint_id.clone(),
                ),
                record.clone(),
            );
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
    let mut merged = merged.into_values().collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut merged);
    if !local_cluster_connectivity_persisted_records_stable(&existing_records, &merged, now) {
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
    Ok(())
}

fn sort_cluster_connectivity_records(records: &mut [ClusterConnectivityRecord]) {
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

/// Mirrors a cluster state into the local durable Raft store without proving
/// replication to a quorum.
///
/// Runtime cluster-control writes should go through the CLI quorum wrapper.
/// This helper remains public for bootstrapping, recovery tooling, and tests
/// that need to seed a local store deliberately.
pub fn sync_cluster_state_to_local_raft_unchecked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    let initial_snapshot = open_cluster_raft_kv_node(paths, &state.local_node_id)
        .map(|node| node.store().snapshot())
        .or_else(|error| {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            write_cluster_kv_snapshot(paths, &InProcessKvStore::new().snapshot())?;
            Ok::<KvSnapshot, NetworkError>(InProcessKvStore::new().snapshot())
        })?;
    let rollout_desired_exists = initial_snapshot
        .entries
        .iter()
        .any(|entry| entry.key == cluster_rollout_desired_key());
    let store = InProcessKvStore::restore(initial_snapshot.clone());
    let mut txn = cluster_state_sync_txn_from_store(&store, state, false)?;
    if !rollout_desired_exists {
        txn.success.push(KvOp::put_serde(
            cluster_rollout_desired_key(),
            &ClusterRolloutDesiredRecord {
                cluster_id: state.cluster_id.clone(),
                state: "idle".to_string(),
                source: None,
                mode: None,
                source_head: None,
                source_bundle: None,
                remote_repo: None,
                host: None,
                port: None,
                shadow_port: None,
                requested_at: None,
                updated_at: state.updated_at,
            },
            None,
        )?);
    }
    if txn.success.is_empty() {
        return Ok(initial_snapshot);
    }
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

/// Compares two cluster identity records ignoring volatile bookkeeping fields
/// (`kv_revision`, `updated_at`) that change on every publish without reflecting
/// a real configuration change.
pub fn cluster_identity_record_stable_eq(
    left: &ClusterIdentityRecord,
    right: &ClusterIdentityRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
}

/// Compares two raft config records ignoring volatile bookkeeping fields
/// (`kv_revision`, `updated_at`).
pub fn cluster_raft_config_record_stable_eq(
    left: &ClusterRaftConfigRecord,
    right: &ClusterRaftConfigRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.term == right.term
        && left.voters == right.voters
        && left.learners == right.learners
        && left.non_voting == right.non_voting
}

pub fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, NetworkError> {
    cluster_state_sync_txn_with_current_snapshot(paths, state).map(|(_, txn)| txn)
}

pub fn cluster_state_sync_txn_with_current_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(KvSnapshot, KvTxn), NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let store = InProcessKvStore::restore(snapshot.clone());
    cluster_state_sync_txn_from_store(&store, state, true).map(|txn| (snapshot, txn))
}

fn cluster_state_sync_txn_from_store(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    validate_config_transition: bool,
) -> Result<KvTxn, NetworkError> {
    let mut state = state.clone();
    cleanup_cluster_iroh_endpoints(&mut state);
    let new_config = ClusterRaftConfigRecord::from(&state);
    let current_config_entry = store.get(&cluster_raft_config_key()).cloned();
    let mut compares = Vec::new();
    if let Some(current_config_entry) = &current_config_entry {
        let current_config: ClusterRaftConfigRecord = current_config_entry.decode()?;
        if validate_config_transition {
            validate_raft_config_transition(&current_config, &new_config)?;
        }
        compares.push(KvCompare::ValueEquals {
            key: cluster_raft_config_key(),
            value: current_config_entry.value.clone(),
        });
    } else {
        compares.push(KvCompare::Missing {
            key: cluster_raft_config_key(),
        });
    }

    // Only emit a put when it would actually change stored KV. Re-publishing
    // semantically-identical state otherwise rewrites identity/raft-config every
    // cycle (their `updated_at`/`kv_revision` fields differ) and re-puts unchanged
    // member/endpoint records, each bumping the KV revision and rewriting the whole
    // materialized snapshot. Dedup so an unchanged state produces an empty txn.
    let mut success = Vec::new();

    let new_identity = ClusterIdentityRecord::from(&state);
    let identity_unchanged = store
        .get(&cluster_identity_key())
        .and_then(|entry| entry.decode::<ClusterIdentityRecord>().ok())
        .is_some_and(|stored| cluster_identity_record_stable_eq(&stored, &new_identity));
    if !identity_unchanged {
        success.push(KvOp::put_serde(
            cluster_identity_key(),
            &new_identity,
            None,
        )?);
    }

    let config_unchanged = current_config_entry
        .as_ref()
        .and_then(|entry| entry.decode::<ClusterRaftConfigRecord>().ok())
        .is_some_and(|stored| cluster_raft_config_record_stable_eq(&stored, &new_config));
    if !config_unchanged {
        success.push(KvOp::put_serde(
            cluster_raft_config_key(),
            &new_config,
            None,
        )?);
    }

    push_stale_member_identity_replacement_deletes(&store, &state, &mut success)?;

    for member in &state.members {
        let member_op = KvOp::put_serde(cluster_member_key(&member.node_id), member, None)?;
        if cluster_member_put_changes_store(&store, &member_op, member) {
            success.push(member_op);
        }
        let policy = ClusterCapabilityPolicyRecord::from(member);
        let policy_op = KvOp::put_serde(
            cluster_capability_policy_key(&member.node_id),
            &policy,
            None,
        )?;
        if cluster_capability_policy_put_changes_store(&store, &policy_op, &policy) {
            success.push(policy_op);
        }

        // Endpoint records are keyed individually. Diff them so a single endpoint
        // change does not delete and re-put the whole member endpoint set.
        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let stored_endpoints: BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)> = store
            .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
            .into_iter()
            .map(|entry| {
                let endpoint =
                    sinomo_kv::decode_kv_value::<ClusterEndpointRecord>(&entry.value).ok();
                (entry.key, (entry.value, endpoint))
            })
            .collect();
        let mut desired_endpoints: BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)> =
            BTreeMap::new();
        for endpoint in &member.endpoints {
            let key = cluster_endpoint_key(&member.node_id, endpoint);
            desired_endpoints.insert(
                key,
                (
                    SinomoObjectCodec::default().encode(endpoint)?,
                    endpoint.clone(),
                ),
            );
        }
        if !cluster_endpoint_maps_stable_eq(&stored_endpoints, &desired_endpoints) {
            for key in stored_endpoints
                .keys()
                .filter(|key| !desired_endpoints.contains_key(*key))
            {
                success.push(KvOp::Delete { key: key.clone() });
            }
            for (key, (value, endpoint)) in desired_endpoints {
                if !stored_endpoints
                    .get(&key)
                    .is_some_and(|(stored_value, stored)| {
                        stored_value == &value
                            || stored
                                .as_ref()
                                .is_some_and(|stored| cluster_endpoint_stable_eq(stored, &endpoint))
                    })
                {
                    success.push(KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    });
                }
            }
        }
    }

    // Nothing changed: return an empty txn so callers can skip committing.
    if success.is_empty() {
        compares.clear();
    }
    Ok(KvTxn {
        compares,
        success,
        failure: Vec::new(),
    })
}

fn cluster_member_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    member: &ClusterMemberRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterMemberRecord>()
                        .map(|stored| !cluster_member_stable_eq(&stored, member))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn push_stale_member_identity_replacement_deletes(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let mut desired_nodes_by_public_key: BTreeMap<String, BTreeSet<NodeId>> = BTreeMap::new();
    for member in &state.members {
        if member.node_public_key.trim().is_empty() {
            continue;
        }
        desired_nodes_by_public_key
            .entry(member.node_public_key.clone())
            .or_default()
            .insert(member.node_id.clone());
    }
    if desired_nodes_by_public_key.is_empty() {
        return Ok(());
    }

    for entry in
        store.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let stored: ClusterMemberRecord = entry.decode()?;
        let Some(desired_nodes) = desired_nodes_by_public_key.get(&stored.node_public_key) else {
            continue;
        };
        if desired_nodes.contains(&stored.node_id) {
            continue;
        }

        push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        push_bounded_txn_success_op(
            ops,
            KvOp::Delete {
                key: cluster_capability_policy_key(&stored.node_id),
            },
        )?;
        for endpoint in store.range_prefix_limited(
            &cluster_endpoint_prefix(&stored.node_id),
            MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS,
        )? {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: endpoint.key })?;
        }
    }
    Ok(())
}

fn cluster_capability_policy_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    policy: &ClusterCapabilityPolicyRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterCapabilityPolicyRecord>()
                        .map(|stored| !cluster_capability_policy_stable_eq(&stored, policy))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn cluster_endpoint_maps_stable_eq(
    stored: &BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)>,
    desired: &BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)>,
) -> bool {
    stored.len() == desired.len()
        && desired.iter().all(|(key, (desired_value, desired))| {
            stored.get(key).is_some_and(|(stored_value, stored)| {
                stored_value == desired_value
                    || stored
                        .as_ref()
                        .is_some_and(|stored| cluster_endpoint_stable_eq(stored, desired))
            })
        })
}

pub fn validate_raft_config_transition(
    current: &ClusterRaftConfigRecord,
    next: &ClusterRaftConfigRecord,
) -> Result<(), NetworkError> {
    if current.cluster_id != next.cluster_id {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "cluster raft config cluster id changed from `{}` to `{}`",
                current.cluster_id, next.cluster_id
            ),
        });
    }
    let current_voters = current.voters.iter().cloned().collect::<BTreeSet<_>>();
    let next_voters = next.voters.iter().cloned().collect::<BTreeSet<_>>();
    let removed_voters = current_voters
        .difference(&next_voters)
        .cloned()
        .collect::<Vec<_>>();
    if !removed_voters.is_empty() {
        let retained_voters = current_voters
            .intersection(&next_voters)
            .cloned()
            .collect::<BTreeSet<_>>();
        let current_quorum = current_voters.len() / 2 + 1;
        let next_quorum = next_voters.len() / 2 + 1;
        if retained_voters.len() < current_quorum || retained_voters.len() < next_quorum {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "raft voter removal requires overlapping old/new quorum; refusing to remove {}",
                    removed_voters
                        .iter()
                        .map(NodeId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }
    let added_voters = next_voters
        .difference(&current_voters)
        .cloned()
        .collect::<Vec<_>>();
    if added_voters.len() > 1 {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first() {
        let current_learners = current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .collect::<BTreeSet<_>>();
        if !current_learners.contains(added_voter) {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "raft voter `{added_voter}` must already be a committed learner or non-voting member before promotion"
                ),
            });
        }
    }
    Ok(())
}

pub fn build_cluster_raft_append_request_from_next_index(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    next_index: u64,
) -> Result<ClusterRaftAppendRequest, NetworkError> {
    let node = open_cluster_raft_kv_node(paths, &state.local_node_id)?;
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    // Send a full snapshot only when we cannot serve the follower incrementally:
    // the entry at `prev_log_index` is neither the snapshot boundary nor still in
    // the retained log tail. Keeping a committed-log tail (see RAFT_COMPACTION_-
    // RETAINED_ENTRIES) lets slightly-behind followers catch up with small appends
    // instead of a ~hundreds-of-KiB snapshot install on every replication round.
    let earliest_log_index = node.log().first().map(|entry| entry.index);
    let prev_log_resolvable = prev_log_index == node.snapshot_index()
        || earliest_log_index.is_some_and(|first| prev_log_index >= first);
    let include_snapshot = node.snapshot_index() > 0 && !prev_log_resolvable;
    let (snapshot, prev_log_index, entries) = if include_snapshot {
        // The live store retains up to MAX_RETAINED_EVENTS (thousands) of watch
        // events. Shipping all of them in every snapshot install bloats the append
        // to hundreds of KiB. Compact the event tail to the same bound the raft node
        // uses for its own saved snapshots — entries/leases/revision are unchanged,
        // only the best-effort watch-replay tail is trimmed.
        let mut kv = node.store().snapshot();
        sinomo_kv::compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
        let snapshot = RaftSnapshot {
            last_included_index: node.applied_index(),
            last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
            kv,
        };
        let entries = node
            .log()
            .iter()
            .filter(|entry| entry.index > snapshot.last_included_index)
            .cloned()
            .collect::<Vec<_>>();
        (Some(snapshot), node.applied_index(), entries)
    } else {
        let entries = node
            .log()
            .iter()
            .filter(|entry| entry.index >= next_index)
            .cloned()
            .collect::<Vec<_>>();
        (None, prev_log_index, entries)
    };
    let prev_log_term = node.term_at(prev_log_index).unwrap_or(0);
    Ok(ClusterRaftAppendRequest {
        state: state.clone(),
        snapshot,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: state.local_node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: node.hard_state().commit_index,
        },
    })
}

pub fn cluster_sync_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)
}

fn cluster_node_status_map_from_kv(
    paths: &ClusterPaths,
) -> BTreeMap<NodeId, ClusterNodeStatusRecord> {
    read_cluster_node_statuses_from_kv(paths)
        .unwrap_or_default()
        .into_iter()
        .map(|status| (status.node_id.clone(), status))
        .collect()
}

fn cluster_sync_targets_with_connectivity_context(
    state: &ClusterBootstrapState,
    graph: &ClusterConnectivityGraph,
    node_statuses: &BTreeMap<NodeId, ClusterNodeStatusRecord>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = cluster_sync_targets(state);
    endpoints.sort_by_key(|endpoint| {
        let node_status_rank = cluster_node_status_route_rank(node_statuses.get(&endpoint.node_id));
        (
            node_status_rank,
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub fn cluster_raft_command_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut endpoints_by_node = BTreeMap::<NodeId, Vec<ClusterEndpointRecord>>::new();
    for endpoint in cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses) {
        endpoints_by_node
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    for endpoints in endpoints_by_node.values_mut() {
        endpoints.sort_by_key(|endpoint| cluster_raft_route_order_key(&graph, endpoint));
        let mut seen = BTreeSet::new();
        endpoints.retain(|endpoint| {
            seen.insert((
                format!("{:?}", endpoint.transport),
                endpoint.address.clone(),
            ))
        });
    }
    let mut node_order = endpoints_by_node
        .iter()
        .filter_map(|(node_id, endpoints)| {
            endpoints.first().map(|endpoint| {
                (
                    cluster_node_status_route_rank(node_statuses.get(node_id)),
                    cluster_raft_route_order_key(&graph, endpoint),
                    node_id.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    node_order.sort_by_key(|(status_rank, route_rank, node_id)| {
        (*status_rank, route_rank.clone(), node_id.clone())
    });
    let ordered_node_ids = node_order
        .into_iter()
        .map(|(_, _, node_id)| node_id)
        .collect::<Vec<_>>();
    let max_endpoint_count = endpoints_by_node.values().map(Vec::len).max().unwrap_or(0);
    let mut targets = Vec::new();
    for endpoint_index in 0..max_endpoint_count {
        for node_id in &ordered_node_ids {
            if let Some(endpoints) = endpoints_by_node.get(node_id)
                && let Some(endpoint) = endpoints.get(endpoint_index)
            {
                targets.push(endpoint.clone());
            }
        }
    }
    targets
}

pub fn cluster_daemon_command_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    prefer_bounded_daemon_command_targets(cluster_sync_targets_with_connectivity(paths, state))
}

pub fn cluster_dfs_protocol_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_daemon_command_targets(paths, state)
}

pub fn cluster_main_daemon_command_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut selected_remote_nodes = BTreeSet::new();
    cluster_main_daemon_command_endpoint_candidates(paths, state)
        .into_iter()
        .filter(|endpoint| selected_remote_nodes.insert(endpoint.node_id.clone()))
        .collect()
}

pub fn cluster_main_daemon_command_endpoint_candidates(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let main_daemon_node_ids = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "daemon.main.run")
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    cluster_daemon_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && main_daemon_node_ids.contains(&endpoint.node_id)
        })
        .collect()
}

pub fn cluster_voter_command_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_raft_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| cluster_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub fn cluster_member_daemon_status_targets(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::from_records(state.local_node_id.clone(), []);
    cluster_member_daemon_status_targets_with_graph(state, member, &graph)
}

pub fn cluster_member_daemon_status_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    cluster_member_daemon_status_targets_with_graph(state, member, &graph)
}

fn cluster_member_daemon_status_targets_with_graph(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    graph: &ClusterConnectivityGraph,
) -> Vec<ClusterEndpointRecord> {
    let is_local_process = cluster_member_is_local_process(state, member);
    let mut endpoints = prefer_bounded_daemon_command_targets(member.endpoints.clone());
    endpoints.sort_by_key(|endpoint| {
        (
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
        .into_iter()
        .filter(|endpoint| {
            is_local_process || !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
        })
        .collect()
}

pub fn cluster_rollout_endpoint_candidates(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        endpoints.extend(
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                        && matches!(
                            endpoint.transport,
                            ClusterTransportKind::LanTcp
                                | ClusterTransportKind::Loopback
                                | ClusterTransportKind::Iroh
                        )
                })
                .cloned(),
        );
    }
    if let Ok(kv_endpoints) = read_cluster_endpoints_from_kv(paths) {
        for endpoint in kv_endpoints {
            if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly
                || !matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
                || endpoints
                    .iter()
                    .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
            {
                continue;
            }
            endpoints.push(endpoint);
        }
    }
    sort_cluster_rollout_endpoints(paths, state, &mut endpoints);
    endpoints
}

pub fn cluster_rollout_target_endpoint_for_main(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    cluster_rollout_target_endpoint_for_main_with_command_endpoint(
        paths,
        state,
        main_node_id,
        None,
        candidates,
    )
}

pub fn cluster_rollout_target_endpoint_for_main_with_command_endpoint(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_node_id: &NodeId,
    command_endpoint: Option<&ClusterEndpointRecord>,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    cluster_rollout_target_endpoints_for_main_with_command_endpoint(
        paths,
        state,
        main_node_id,
        command_endpoint,
        candidates,
    )
    .into_iter()
    .next()
}

pub fn cluster_rollout_target_endpoints_for_main_with_command_endpoint(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_node_id: &NodeId,
    command_endpoint: Option<&ClusterEndpointRecord>,
    candidates: &[ClusterEndpointRecord],
) -> Vec<ClusterEndpointRecord> {
    let shadow_node_id = shadow_daemon_node_id(main_node_id);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut candidates = candidates
        .iter()
        .filter(|endpoint| {
            (endpoint.node_id == shadow_node_id || endpoint.node_id == *main_node_id)
                && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        })
        .cloned()
        .collect::<Vec<_>>();
    if let Some(command_endpoint) = command_endpoint
        && command_endpoint.node_id == *main_node_id
        && command_endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        && !candidates
            .iter()
            .any(|candidate| candidate.endpoint_id == command_endpoint.endpoint_id)
    {
        candidates.push(command_endpoint.clone());
    }
    candidates.sort_by_key(|endpoint| {
        (
            cluster_rollout_route_process_rank(&graph, endpoint, main_node_id, &shadow_node_id),
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    candidates
}

fn cluster_rollout_route_process_rank(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    main_node_id: &NodeId,
    shadow_node_id: &NodeId,
) -> u8 {
    let unavailable = graph.unavailability_rank(endpoint) != 0;
    if endpoint.node_id == *main_node_id {
        if unavailable { 2 } else { 0 }
    } else if endpoint.node_id == *shadow_node_id {
        if unavailable { 3 } else { 1 }
    } else {
        4
    }
}

pub fn sort_cluster_rollout_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoints: &mut [ClusterEndpointRecord],
) {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    endpoints.sort_by_key(|endpoint| {
        (
            endpoint.node_id.clone(),
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

fn cluster_route_unavailability_rank(connectivity: Option<&ClusterConnectivityRecord>) -> u8 {
    match connectivity {
        Some(record)
            if record.state == ClusterConnectivityState::Unreachable
                && record.consecutive_failures
                    >= CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES =>
        {
            1
        }
        _ => 0,
    }
}

fn cluster_node_status_route_rank(status: Option<&ClusterNodeStatusRecord>) -> u8 {
    // Node status is no longer periodically refreshed, so observed_at age is not a
    // liveness signal — rank purely by the self-reported availability field. Actual
    // reachability/freshness is contributed by the connectivity-based rank
    // components (see cluster_route_connectivity_quality_rank / unavailability_rank),
    // which are driven by active probes.
    match status.map(|status| &status.availability) {
        Some(sinomo_core::ClusterNodeAvailability::Online) => 0,
        None | Some(sinomo_core::ClusterNodeAvailability::Unknown) => 1,
        Some(sinomo_core::ClusterNodeAvailability::Offline) => 3,
    }
}

fn cluster_route_connectivity_quality_rank(connectivity: Option<&ClusterConnectivityState>) -> u8 {
    match connectivity {
        Some(ClusterConnectivityState::Healthy) => 0,
        Some(ClusterConnectivityState::Degraded) => 1,
        None => 2,
        Some(ClusterConnectivityState::Unreachable) => 3,
    }
}

fn cluster_raft_route_availability_rank(
    endpoint: &ClusterEndpointRecord,
    connectivity: Option<&ClusterConnectivityState>,
) -> u8 {
    match (endpoint.transport.clone(), connectivity) {
        (_, Some(ClusterConnectivityState::Unreachable)) => 4,
        (ClusterTransportKind::Iroh, Some(ClusterConnectivityState::Healthy)) => 2,
        (ClusterTransportKind::Iroh, _) => 3,
        (_, Some(ClusterConnectivityState::Healthy)) => 0,
        _ => 1,
    }
}

fn cluster_endpoint_static_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            cluster_direct_endpoint_static_fallback_rank(endpoint)
        }
        ClusterTransportKind::Iroh => 2,
        _ => 8,
    }
}

fn cluster_direct_endpoint_static_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
        return if !is_loopback_connect_address(&endpoint.address) && endpoint.address.contains(':')
        {
            1
        } else {
            3
        };
    };
    match address.ip() {
        IpAddr::V4(ip) if ip.is_private() || ip.is_link_local() => 0,
        IpAddr::V6(ip) if ip.is_unique_local() || ip.is_unicast_link_local() => 0,
        ip if ip.is_loopback() || ip.is_unspecified() => 2,
        _ => 1,
    }
}

fn cluster_raft_route_order_key(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> (u8, u8, u8, u16, String) {
    (
        cluster_raft_route_availability_rank(endpoint, graph.state_for_endpoint(endpoint)),
        cluster_endpoint_static_fallback_rank(endpoint),
        graph.quality_rank(endpoint),
        endpoint.priority,
        endpoint.address.clone(),
    )
}

pub fn cluster_network_probe_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut endpoint_ids = BTreeSet::new();
    let mut endpoints = Vec::new();
    for member in &state.members {
        if cluster_member_is_local_process(state, member) || !member.state.is_authorized() {
            continue;
        }
        endpoints.extend(
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                        && matches!(
                            endpoint.transport,
                            ClusterTransportKind::LanTcp
                                | ClusterTransportKind::Loopback
                                | ClusterTransportKind::Iroh
                        )
                        && endpoint_ids.insert(endpoint.endpoint_id.clone())
                })
                .cloned(),
        );
    }
    endpoints.sort_by_key(|endpoint| {
        (
            endpoint.node_id.clone(),
            cluster_endpoint_static_fallback_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub fn prefer_bounded_daemon_command_targets(
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoint_route_keys = BTreeSet::new();
    let endpoints = endpoints
        .into_iter()
        .filter(|endpoint| {
            endpoint_route_keys.insert((
                endpoint.node_id.clone(),
                format!("{:?}", endpoint.transport),
                endpoint.address.clone(),
            ))
        })
        .collect::<Vec<_>>();
    let mut selected = Vec::new();
    let mut selected_endpoint_ids = BTreeSet::new();
    let mut nodes_with_primary = BTreeSet::new();
    for endpoint in &endpoints {
        if nodes_with_primary.insert(endpoint.node_id.clone()) {
            selected_endpoint_ids.insert(endpoint.endpoint_id.clone());
            selected.push(endpoint.clone());
        }
    }
    for endpoint in endpoints {
        if selected_endpoint_ids.insert(endpoint.endpoint_id.clone()) {
            selected.push(endpoint);
        }
    }
    selected
}

pub fn cluster_daemon_command_transport_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => 0,
        ClusterTransportKind::Iroh => 1,
        _ => 2,
    }
}

pub fn cluster_daemon_command_address_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    if !matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) {
        return 1;
    }
    let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
        return 1;
    };
    match address.ip() {
        IpAddr::V4(ip) if ip.is_private() || ip.is_link_local() => 0,
        IpAddr::V6(ip) if ip.is_unique_local() || ip.is_unicast_link_local() => 0,
        _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::Lan) => 0,
        _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => 1,
        ip if ip.is_loopback() || ip.is_unspecified() => 3,
        _ => 1,
    }
}

pub fn cluster_member_is_local_process(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    cluster_node_is_local_process(state, &member.node_id)
}

pub fn live_cluster_member_probe_possible(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    let is_local_process = cluster_member_is_local_process(state, member);
    member.endpoints.iter().any(|endpoint| {
        (is_local_process || endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            )
    })
}

/// True when `member` has probeable endpoints but the local node's active
/// connectivity probes currently mark every one of them unreachable.
///
/// This is the probe-based liveness signal: unlike the self-reported node-status
/// `observed_at` (which is no longer periodically refreshed), it reflects whether
/// we can actually reach the peer right now. A member we have never probed (no
/// connectivity record yet) is not reported unreachable.
pub fn cluster_member_unreachable_via_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    if cluster_member_is_local_process(state, member) {
        return false;
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut has_probeable_endpoint = false;
    for endpoint in &member.endpoints {
        let probeable = endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            );
        if !probeable {
            continue;
        }
        has_probeable_endpoint = true;
        if graph.unavailability_rank(endpoint) == 0 {
            // At least one probeable endpoint is not marked unreachable.
            return false;
        }
    }
    has_probeable_endpoint
}

pub fn cluster_node_is_local_process(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    *node_id == state.local_node_id || *node_id == shadow_daemon_node_id(&state.local_node_id)
}

pub fn advertised_peer_route_targets(state: &ClusterBootstrapState) -> Vec<SocketAddr> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        for endpoint in &member.endpoints {
            if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                continue;
            }
            if !matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
            ) {
                continue;
            }
            let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
                continue;
            };
            if is_invitable_ip(address.ip()) && !targets.contains(&address) {
                targets.push(address);
            }
        }
    }
    targets.sort();
    targets
}

pub fn discover_peer_route_ip_candidates(state: &ClusterBootstrapState) -> Vec<IpAddr> {
    discover_local_ip_candidates_for_targets(advertised_peer_route_targets(state))
}

pub fn daemon_advertised_endpoints_from_addresses(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_addresses_with_public_discovery(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        true,
    )
}

/// How long to wait when probing this node's own public endpoint for reachability.
const PUBLIC_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

/// Whether this node's own public endpoint (`ip:port`) is actually reachable -- i.e. an
/// external connection to it lands on this daemon, via a user-configured port-forward (or
/// NAT hairpin). Only a verified public endpoint should be advertised; an unverified one (a
/// NAT'd node with no forward) would publish an unreachable address that every peer then
/// wastes connection attempts on. Manual endpoint configuration is future work.
fn public_endpoint_is_reachable_for_local_ips(
    ip: IpAddr,
    port: u16,
    local_ips: impl IntoIterator<Item = IpAddr>,
) -> bool {
    public_endpoint_is_reachable_for_local_ips_with_probe_at(
        ip,
        port,
        Instant::now(),
        local_ips,
        |address| {
            std::net::TcpStream::connect_timeout(&address, PUBLIC_ENDPOINT_PROBE_TIMEOUT).is_ok()
        },
    )
}

#[cfg(test)]
pub(crate) fn public_endpoint_is_reachable_with_probe(
    ip: IpAddr,
    port: u16,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    public_endpoint_is_reachable_with_probe_at(ip, port, Instant::now(), probe)
}

#[cfg(test)]
pub(crate) fn public_endpoint_is_reachable_with_probe_at(
    ip: IpAddr,
    port: u16,
    now: Instant,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    public_endpoint_is_reachable_for_local_ips_with_probe_at(
        ip,
        port,
        now,
        std::iter::empty(),
        probe,
    )
}

pub(crate) fn public_endpoint_is_reachable_for_local_ips_with_probe_at(
    ip: IpAddr,
    port: u16,
    now: Instant,
    local_ips: impl IntoIterator<Item = IpAddr>,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    let address = SocketAddr::new(ip, port);
    let cache_key = PublicEndpointReachabilityCacheKey {
        address,
        local_network: local_network_cache_key(local_ips),
    };
    if let Ok(cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock()
        && let Some(reachable) = cache.get_fresh(&cache_key, now)
    {
        return reachable;
    }
    let reachable = probe(address);
    if let Ok(mut cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock() {
        cache.insert(
            cache_key,
            PublicEndpointReachabilityCacheEntry {
                checked_at: now,
                reachable,
            },
            now,
        );
    }
    reachable
}

#[cfg(test)]
pub(crate) fn clear_public_endpoint_reachability_cache() {
    if let Ok(mut cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock() {
        cache.clear();
    }
}

pub fn daemon_advertised_endpoints_from_addresses_with_public_discovery(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = vec![daemon_loopback_endpoint(
        node_id,
        loopback_address,
        priority,
    )];
    let mut lan_addresses = Vec::new();
    if matches!(host, "0.0.0.0" | "::") {
        for ip in lan_ips.iter().chain(route_ips.iter()).copied() {
            if is_invitable_ip(ip)
                && (allow_public_ip_discovery || crate::is_private_or_link_local_ip(ip))
            {
                push_unique_address(&mut lan_addresses, format_ip_port(ip, port));
            }
        }
        let has_public_candidate = lan_ips
            .iter()
            .chain(route_ips.iter())
            .copied()
            .any(|ip| is_invitable_ip(ip) && !crate::is_private_or_link_local_ip(ip));
        if allow_public_ip_discovery && !has_public_candidate {
            for ip in discover_default_public_ip_candidates_for_local_ips(
                lan_ips.iter().chain(route_ips.iter()).copied(),
            ) {
                // Only advertise a discovered public IP when it is actually reachable on the
                // daemon port -- i.e. the user has a port-forward (or NAT hairpin) back to
                // this daemon. An unverified public IP would otherwise pollute every peer's
                // connection-target list with an unreachable endpoint.
                if is_invitable_ip(ip)
                    && public_endpoint_is_reachable_for_local_ips(
                        ip,
                        port,
                        lan_ips.iter().chain(route_ips.iter()).copied(),
                    )
                {
                    push_unique_address(&mut lan_addresses, format_ip_port(ip, port));
                }
            }
        }
    } else if host.parse::<IpAddr>().is_ok_and(is_invitable_ip)
        || !is_loopback_connect_address(&format!("{host}:{port}"))
    {
        push_unique_address(&mut lan_addresses, format!("{host}:{port}"));
    }

    for address in lan_addresses {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(20),
        ));
    }
    endpoints
}

pub fn daemon_loopback_endpoint(
    node_id: &NodeId,
    address: String,
    priority: u16,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_loopback_endpoint_id(node_id),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Loopback,
        address,
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::LocalOnly,
        priority,
        last_observed_latency_ms: None,
    }
}

pub fn daemon_lan_endpoint(
    node_id: &NodeId,
    address: String,
    priority: u16,
) -> ClusterEndpointRecord {
    let privacy = address
        .parse::<SocketAddr>()
        .ok()
        .map(|address| address.ip())
        .filter(|ip| !crate::is_private_or_link_local_ip(*ip))
        .map(|_| ClusterEndpointPrivacy::InternetDirect)
        .unwrap_or(ClusterEndpointPrivacy::Lan);
    ClusterEndpointRecord {
        endpoint_id: daemon_lan_endpoint_id(node_id, &address),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address,
        source: ClusterEndpointSource::Observed,
        privacy,
        priority,
        last_observed_latency_ms: None,
    }
}

fn push_unique_address(addresses: &mut Vec<String>, address: String) {
    if !addresses.iter().any(|candidate| candidate == &address) {
        addresses.push(address);
    }
}

pub fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<KvSnapshot, NetworkError> {
    if stored_cluster_node_status_matches(paths, &status)? {
        return read_cluster_kv_snapshot(paths);
    }
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_node_status_key(&status.node_id);
    let node_id = status.node_id.clone();
    let rollout = status.rollout.clone();
    let rollout_key = cluster_rollout_node_key(&node_id);
    let mut success = vec![KvOp::put_serde(key, &status, None)?];
    if let Some(rollout) = rollout {
        success.push(KvOp::put_serde(rollout_key, &rollout, None)?);
    } else {
        success.push(KvOp::Delete { key: rollout_key });
    }
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        }),
    )
}

pub fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<KvSnapshot, NetworkError> {
    if stored_cluster_network_info_matches(paths, &info)? {
        return read_cluster_kv_snapshot(paths);
    }
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_network_info_key(&info.node_id);
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Op(KvOp::put_serde(key, &info, None)?),
    )
}

pub fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<KvSnapshot, NetworkError> {
    write_local_cluster_connectivity_record_set(paths, source_node_id, &records)?;
    let now = OffsetDateTime::now_utc();
    let mut snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    let mut local_node_id = None;
    let mut appended = false;
    loop {
        let success = cluster_connectivity_full_sync_batch_ops_from_snapshot(
            snapshot.as_ref(),
            source_node_id,
            &records,
            now,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        )?;
        if success.is_empty() {
            return if appended {
                Ok(snapshot.as_ref().clone())
            } else {
                read_cluster_kv_snapshot(paths)
            };
        }
        if local_node_id.is_none() {
            local_node_id = Some(read_cluster_state_without_kv(paths)?.local_node_id);
        }
        snapshot = Arc::new(append_cluster_raft_ops_in_batches(
            paths,
            local_node_id
                .as_ref()
                .expect("local node id is initialized before append"),
            success,
        )?);
        appended = true;
    }
}

pub fn stored_cluster_node_status_matches(
    paths: &ClusterPaths,
    status: &ClusterNodeStatusRecord,
) -> Result<bool, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(&cluster_node_status_key(&status.node_id)) else {
        return Ok(false);
    };
    let Ok(existing) = entry.decode::<ClusterNodeStatusRecord>() else {
        return Ok(false);
    };
    // Match on stable content only: node status is not periodically refreshed (no
    // observed_at heartbeat). A live node's status is republished when its content
    // changes; crash detection is handled by the active connectivity probes.
    if !cluster_node_status_stable_eq(&existing, status) {
        return Ok(false);
    }
    let rollout_entry = snapshot
        .get(&cluster_rollout_node_key(&status.node_id))
        .cloned();
    Ok(match (&status.rollout, rollout_entry) {
        (Some(expected), Some(entry)) => entry
            .decode::<sinomo_core::ClusterNodeRolloutStatus>()
            .is_ok_and(|existing| existing == *expected),
        (None, None) => true,
        _ => false,
    })
}

pub fn cluster_node_status_stable_eq(
    left: &ClusterNodeStatusRecord,
    right: &ClusterNodeStatusRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.process == right.process
        && left.availability == right.availability
        && left.daemon_version == right.daemon_version
        && left.commit == right.commit
        && left.target_os == right.target_os
        && left.target_arch == right.target_arch
        && left.address == right.address
        && left.bind_address == right.bind_address
        && left.executable == right.executable
        && left.rollout == right.rollout
        && left.daemon_started_at == right.daemon_started_at
}

pub fn stored_cluster_network_info_matches(
    paths: &ClusterPaths,
    info: &ClusterNetworkInfoRecord,
) -> Result<bool, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(&cluster_network_info_key(&info.node_id)) else {
        return Ok(false);
    };
    let Ok(existing) = entry.decode::<ClusterNetworkInfoRecord>() else {
        return Ok(false);
    };
    let now = OffsetDateTime::now_utc();
    Ok(cluster_network_info_stable_eq(&existing, info)
        && !cluster_network_record_should_refresh(existing.expires_at, now))
}

pub fn cluster_network_info_stable_eq(
    left: &ClusterNetworkInfoRecord,
    right: &ClusterNetworkInfoRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.environment_id == right.environment_id
        && left.observed_addresses == right.observed_addresses
        && cluster_endpoint_list_stable_eq(&left.endpoints, &right.endpoints)
}

pub fn cluster_capability_policy_stable_eq(
    left: &ClusterCapabilityPolicyRecord,
    right: &ClusterCapabilityPolicyRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
}

pub fn cluster_connectivity_sync_ops(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<Vec<KvOp>, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    cluster_connectivity_sync_ops_from_snapshot(snapshot.as_ref(), source_node_id, records, now)
}

fn cluster_connectivity_sync_ops_from_snapshot(
    snapshot: &KvSnapshot,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
) -> Result<Vec<KvOp>, NetworkError> {
    let mut incoming = BTreeMap::new();
    for record in records {
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        incoming.insert(key, record);
    }

    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        let stable = incoming.get(&entry.key).is_some_and(|record| {
            entry
                .decode::<ClusterConnectivityRecord>()
                .is_ok_and(|existing| {
                    existing == **record
                        || (cluster_connectivity_stable_eq(&existing, record)
                            && !cluster_network_record_should_refresh(existing.expires_at, now))
                })
        });
        if stable {
            incoming.remove(&entry.key);
        }
    }

    let mut ops = Vec::new();
    for (key, record) in incoming {
        ops.push(KvOp::put_serde(key, record, None)?);
    }
    Ok(ops)
}

pub fn cluster_connectivity_full_sync_ops(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<Vec<KvOp>, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_connectivity_full_sync_ops_from_snapshot(
        snapshot.as_ref(),
        source_node_id,
        records,
        now,
    )
}

pub fn cluster_connectivity_full_sync_batch_ops(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    max_ops: usize,
) -> Result<Vec<KvOp>, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_connectivity_full_sync_batch_ops_from_snapshot(
        snapshot.as_ref(),
        source_node_id,
        records,
        now,
        max_ops,
    )
}

fn cluster_connectivity_full_sync_ops_from_snapshot(
    snapshot: &KvSnapshot,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
) -> Result<Vec<KvOp>, NetworkError> {
    let mut incoming = BTreeMap::new();
    for record in records {
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        incoming.insert(key, record);
    }

    let mut existing = BTreeMap::new();
    let mut ops = Vec::new();
    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        if !incoming.contains_key(&entry.key) {
            ops.push(KvOp::Delete {
                key: entry.key.clone(),
            });
            continue;
        }
        if let Ok(record) = entry.decode::<ClusterConnectivityRecord>() {
            existing.insert(entry.key.clone(), record);
        }
    }

    for (key, record) in incoming {
        if existing.get(&key).is_some_and(|existing| {
            existing == record
                || (cluster_connectivity_stable_eq(existing, record)
                    && !cluster_network_record_should_refresh(existing.expires_at, now))
        }) {
            continue;
        }
        ops.push(KvOp::put_serde(key, record, None)?);
    }
    Ok(ops)
}

fn cluster_connectivity_full_sync_batch_ops_from_snapshot(
    snapshot: &KvSnapshot,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
    max_ops: usize,
) -> Result<Vec<KvOp>, NetworkError> {
    let max_ops = max_ops
        .min(sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH)
        .min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    if max_ops == 0 {
        return Ok(Vec::new());
    }

    let mut incoming = BTreeMap::new();
    for record in records {
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        incoming.insert(key, record);
    }

    let mut ops = Vec::new();
    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        let stable = incoming.get(&entry.key).is_some_and(|record| {
            entry
                .decode::<ClusterConnectivityRecord>()
                .is_ok_and(|existing| {
                    existing == **record
                        || (cluster_connectivity_stable_eq(&existing, record)
                            && !cluster_network_record_should_refresh(existing.expires_at, now))
                })
        });
        if stable {
            incoming.remove(&entry.key);
            continue;
        }
        if incoming.contains_key(&entry.key) {
            continue;
        }
        ops.push(KvOp::Delete {
            key: entry.key.clone(),
        });
        if ops.len() >= max_ops {
            return Ok(ops);
        }
    }

    for (key, record) in incoming {
        ops.push(KvOp::put_serde(key, record, None)?);
        if ops.len() >= max_ops {
            break;
        }
    }
    Ok(ops)
}

/// Returns true when publishing `records` for `source_node_id` would not change
/// cluster KV: no records need adding/refreshing and none need deleting. Lets the
/// caller skip the raft append (and its disk churn) when connectivity is stable.
pub fn cluster_connectivity_sync_is_noop(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<bool, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let incoming_keys: BTreeSet<String> = records
        .iter()
        .map(|record| {
            cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            )
        })
        .collect();
    // A stored record no longer present in `records` would need deletion.
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        if !incoming_keys.contains(&entry.key) {
            return Ok(false);
        }
    }
    // Any incoming record that differs from what is stored (or is due for a TTL
    // refresh) would need a put.
    Ok(cluster_connectivity_sync_ops_from_snapshot(
        snapshot.as_ref(),
        source_node_id,
        records,
        now,
    )?
    .is_empty())
}

pub fn expire_cluster_network_record_ops(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
) -> Result<(Vec<KvCompare>, Vec<KvOp>), NetworkError> {
    let mut compares = Vec::new();
    let mut ops = Vec::new();
    let max_records = sinomo_kv::MAX_KV_TXN_COMPARES.min(sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH);
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    for entry in snapshot.prefix_entries("/cluster/network/info/") {
        let Ok(record) = entry.decode::<ClusterNetworkInfoRecord>() else {
            continue;
        };
        if cluster_node_is_local_process(state, &record.node_id) {
            continue;
        }
        if cluster_network_record_expired(record.expires_at, now) {
            compares.push(KvCompare::ValueEquals {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
            ops.push(KvOp::Delete {
                key: entry.key.clone(),
            });
            if ops.len() >= max_records {
                return Ok((compares, ops));
            }
        }
    }
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        let Ok(record) = entry.decode::<ClusterConnectivityRecord>() else {
            continue;
        };
        if record.source_node_id == state.local_node_id {
            continue;
        }
        if cluster_network_record_expired(record.expires_at, now) {
            compares.push(KvCompare::ValueEquals {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
            ops.push(KvOp::Delete {
                key: entry.key.clone(),
            });
            if ops.len() >= max_records {
                return Ok((compares, ops));
            }
        }
    }
    Ok((compares, ops))
}

fn cluster_network_record_expired(expires_at: OffsetDateTime, now: OffsetDateTime) -> bool {
    expires_at + time::Duration::seconds(CLUSTER_NETWORK_EXPIRY_GRACE_SECONDS) <= now
}

fn cluster_network_record_live(expires_at: OffsetDateTime, now: OffsetDateTime) -> bool {
    expires_at > now
}

pub fn cluster_network_record_should_refresh(
    expires_at: OffsetDateTime,
    now: OffsetDateTime,
) -> bool {
    expires_at <= now + time::Duration::seconds(CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS)
}

pub fn cluster_connectivity_stable_eq(
    left: &ClusterConnectivityRecord,
    right: &ClusterConnectivityRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.source_node_id == right.source_node_id
        && left.target_node_id == right.target_node_id
        && left.endpoint_id == right.endpoint_id
        && left.transport == right.transport
        && left.address == right.address
        && left.state == right.state
}

pub fn cluster_bootstrap_state_stable_eq(
    left: &ClusterBootstrapState,
    right: &ClusterBootstrapState,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
        && left.members.len() == right.members.len()
        && left
            .members
            .iter()
            .zip(right.members.iter())
            .all(|(left, right)| cluster_member_stable_eq(left, right))
}

pub fn cluster_member_stable_eq(left: &ClusterMemberRecord, right: &ClusterMemberRecord) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.display_name == right.display_name
        && left.node_role == right.node_role
        && left.role == right.role
        && left.state == right.state
        && left.trust_domain == right.trust_domain
        && left.node_public_key == right.node_public_key
        && left.certificate_id == right.certificate_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
        && cluster_endpoint_list_stable_eq(&left.endpoints, &right.endpoints)
        && left.joined_at == right.joined_at
}

pub fn cluster_endpoint_list_stable_eq(
    left: &[ClusterEndpointRecord],
    right: &[ClusterEndpointRecord],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut left = left.iter().collect::<Vec<_>>();
    let mut right = right.iter().collect::<Vec<_>>();
    left.sort_by(cluster_endpoint_record_order);
    right.sort_by(cluster_endpoint_record_order);
    left.into_iter()
        .zip(right)
        .all(|(left, right)| cluster_endpoint_stable_eq(left, right))
}

fn cluster_endpoint_record_order(
    left: &&ClusterEndpointRecord,
    right: &&ClusterEndpointRecord,
) -> std::cmp::Ordering {
    left.endpoint_id
        .cmp(&right.endpoint_id)
        .then_with(|| left.node_id.cmp(&right.node_id))
        .then_with(|| left.address.cmp(&right.address))
}

pub fn cluster_endpoint_stable_eq(
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> bool {
    left.endpoint_id == right.endpoint_id
        && left.node_id == right.node_id
        && left.transport == right.transport
        && left.address == right.address
        && cluster_endpoint_source_stable_eq(&left.source, &right.source)
        && left.privacy == right.privacy
        && left.priority == right.priority
        && left.last_observed_latency_ms == right.last_observed_latency_ms
}

fn cluster_endpoint_source_stable_eq(
    left: &ClusterEndpointSource,
    right: &ClusterEndpointSource,
) -> bool {
    left == right
        || matches!(
            (left, right),
            (ClusterEndpointSource::Mdns, ClusterEndpointSource::Observed)
                | (ClusterEndpointSource::Observed, ClusterEndpointSource::Mdns)
        )
}

pub fn cluster_member_publish_matches_kv(
    paths: &ClusterPaths,
    members: &[ClusterMemberRecord],
) -> Result<bool, NetworkError> {
    let snapshot = read_cluster_kv_cached_snapshot(paths)?;
    for member in members {
        let mut member = member.clone();
        cleanup_member_iroh_endpoints(&mut member);
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));

        let Some(entry) = snapshot.get(&cluster_member_key(&member.node_id)) else {
            return Ok(false);
        };
        let Ok(mut existing) = entry.decode::<ClusterMemberRecord>() else {
            return Ok(false);
        };
        cleanup_member_iroh_endpoints(&mut existing);
        existing
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if !cluster_member_stable_eq(&existing, &member) {
            return Ok(false);
        }

        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let mut existing_endpoints = snapshot
            .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
            .into_iter()
            .filter_map(|entry| entry.decode::<ClusterEndpointRecord>().ok())
            .collect::<Vec<_>>();
        existing_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if !cluster_endpoint_list_stable_eq(&existing_endpoints, &member.endpoints) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn cluster_member_publish_txn(members: &[ClusterMemberRecord]) -> Result<KvTxn, NetworkError> {
    cluster_member_publish_txn_with_snapshot(None, members)
}

pub fn cluster_member_publish_txn_from_kv(
    paths: &ClusterPaths,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_member_publish_txn_with_snapshot(Some(snapshot.as_ref()), members)
}

pub fn cluster_member_publish_txn_from_snapshot(
    snapshot: &KvSnapshot,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    cluster_member_publish_txn_with_snapshot(Some(snapshot), members)
}

fn cluster_member_publish_txn_with_snapshot(
    snapshot: Option<&KvSnapshot>,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    let mut success = Vec::new();
    for member in members {
        let mut member = member.clone();
        cleanup_member_iroh_endpoints(&mut member);
        push_snapshot_member_diff_put_op(
            snapshot,
            cluster_member_key(&member.node_id),
            &member,
            &mut success,
        )?;
        push_snapshot_capability_policy_diff_put_op(
            snapshot,
            cluster_capability_policy_key(&member.node_id),
            &ClusterCapabilityPolicyRecord::from(&member),
            &mut success,
        )?;
        if let Some(snapshot) = snapshot {
            push_cluster_member_endpoint_diff_ops(snapshot, &member, &mut success)?;
        } else {
            push_bounded_txn_success_op(
                &mut success,
                KvOp::DeletePrefix {
                    prefix: cluster_endpoint_prefix(&member.node_id),
                },
            )?;
            for endpoint in &member.endpoints {
                push_bounded_txn_success_op(
                    &mut success,
                    KvOp::put_serde(
                        cluster_endpoint_key(&member.node_id, endpoint),
                        endpoint,
                        None,
                    )?,
                )?;
            }
        }
    }
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

fn push_bounded_txn_success_op(ops: &mut Vec<KvOp>, op: KvOp) -> Result<(), NetworkError> {
    if ops.len() >= sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH {
        return Err(NetworkError::Kv(KvError::TxnLimit {
            compares: 0,
            success_ops: ops.len().saturating_add(1),
            failure_ops: 0,
            max_compares: sinomo_kv::MAX_KV_TXN_COMPARES,
            max_ops_per_branch: sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH,
        }));
    }
    ops.push(op);
    Ok(())
}

fn push_snapshot_diff_put_op<T: Serialize + ?Sized>(
    snapshot: Option<&KvSnapshot>,
    key: String,
    value: &T,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key, value, None)?;
    if snapshot.is_some_and(|snapshot| kv_put_op_matches_snapshot(snapshot, &op)) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op)?;
    Ok(())
}

fn push_snapshot_member_diff_put_op(
    snapshot: Option<&KvSnapshot>,
    key: String,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key.clone(), member, None)?;
    if snapshot.is_some_and(|snapshot| {
        kv_put_op_matches_snapshot(snapshot, &op)
            || snapshot
                .get(&key)
                .and_then(|entry| entry.decode::<ClusterMemberRecord>().ok())
                .is_some_and(|stored| cluster_member_stable_eq(&stored, member))
    }) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op)?;
    Ok(())
}

fn push_snapshot_capability_policy_diff_put_op(
    snapshot: Option<&KvSnapshot>,
    key: String,
    policy: &ClusterCapabilityPolicyRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key.clone(), policy, None)?;
    if snapshot.is_some_and(|snapshot| {
        kv_put_op_matches_snapshot(snapshot, &op)
            || snapshot
                .get(&key)
                .and_then(|entry| entry.decode::<ClusterCapabilityPolicyRecord>().ok())
                .is_some_and(|stored| cluster_capability_policy_stable_eq(&stored, policy))
    }) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op)?;
    Ok(())
}

fn kv_put_op_matches_snapshot(snapshot: &KvSnapshot, op: &KvOp) -> bool {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return false;
    };
    snapshot.get(key).is_some_and(|entry| {
        entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id
    })
}

fn push_cluster_member_endpoint_diff_ops(
    snapshot: &KvSnapshot,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
    let mut desired_endpoints = BTreeMap::new();
    for endpoint in &member.endpoints {
        let key = cluster_endpoint_key(&member.node_id, endpoint);
        let op = KvOp::put_serde(key, endpoint, None)?;
        let KvOp::Put { key, value, .. } = op else {
            unreachable!("put_serde must produce a put operation");
        };
        desired_endpoints.insert(key, (value, endpoint.clone()));
    }

    for entry in
        snapshot.range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            push_bounded_txn_success_op(
                ops,
                KvOp::Delete {
                    key: entry.key.clone(),
                },
            )?;
        }
    }

    for (key, (value, endpoint)) in desired_endpoints {
        if snapshot.get(&key).is_none_or(|entry| {
            entry.lease_id.is_some()
                || (entry.value != value
                    && entry
                        .decode::<ClusterEndpointRecord>()
                        .map(|stored| !cluster_endpoint_stable_eq(&stored, &endpoint))
                        .unwrap_or(true))
        }) {
            push_bounded_txn_success_op(
                ops,
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                },
            )?;
        }
    }

    Ok(())
}

pub fn expire_cluster_network_records(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<KvSnapshot, NetworkError> {
    let mut snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let mut success = cluster_network_expiry_delete_ops(
        snapshot.as_ref(),
        now,
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
    );
    if success.is_empty() {
        return Ok(snapshot.as_ref().clone());
    }

    let state = read_cluster_state_without_kv(paths)?;
    loop {
        snapshot = Arc::new(append_cluster_raft_ops_in_batches(
            paths,
            &state.local_node_id,
            success,
        )?);
        success = cluster_network_expiry_delete_ops(
            snapshot.as_ref(),
            now,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        );
        if success.is_empty() {
            return Ok(snapshot.as_ref().clone());
        }
    }
}

pub(crate) fn cluster_network_expiry_delete_ops(
    snapshot: &KvSnapshot,
    now: OffsetDateTime,
    max_ops: usize,
) -> Vec<KvOp> {
    let mut success = Vec::new();
    for entry in snapshot.prefix_entries("/cluster/network/info/") {
        if success.len() >= max_ops {
            return success;
        }
        let Ok(record) = entry.decode::<ClusterNetworkInfoRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete {
                key: entry.key.clone(),
            });
        }
    }
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        if success.len() >= max_ops {
            return success;
        }
        let Ok(record) = entry.decode::<ClusterConnectivityRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete {
                key: entry.key.clone(),
            });
        }
    }
    success
}

fn append_cluster_raft_ops_in_batches(
    paths: &ClusterPaths,
    node_id: &NodeId,
    ops: Vec<KvOp>,
) -> Result<KvSnapshot, NetworkError> {
    let mut ops = ops.into_iter();
    let mut latest = None;
    loop {
        let chunk = ops
            .by_ref()
            .take(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        latest = Some(append_cluster_raft_command(
            paths,
            node_id,
            KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: chunk,
                failure: Vec::new(),
            }),
        )?);
    }
    latest.ok_or_else(|| NetworkError::StorageIo {
        path: paths.kv_snapshot.display().to_string(),
        detail: "cannot append an empty batch of cluster kv operations".to_string(),
    })
}

pub fn cluster_node_status_key(node_id: &NodeId) -> String {
    format!("/cluster/node-status/{node_id}")
}

pub fn cluster_network_info_key(node_id: &NodeId) -> String {
    format!("/cluster/network/info/{node_id}")
}

pub fn cluster_connectivity_source_prefix(source_node_id: &NodeId) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/")
}

pub fn cluster_connectivity_key(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &ClusterEndpointId,
) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/{target_node_id}/{endpoint_id}")
}

pub fn cluster_identity_key() -> String {
    "/cluster/config/identity".to_string()
}

pub fn cluster_raft_config_key() -> String {
    "/cluster/raft/config".to_string()
}

pub fn cluster_endpoint_key(node_id: &NodeId, endpoint: &ClusterEndpointRecord) -> String {
    format!(
        "{}{}",
        cluster_endpoint_prefix(node_id),
        endpoint.endpoint_id
    )
}

pub fn cluster_endpoint_prefix(node_id: &NodeId) -> String {
    format!("/cluster/endpoints/{node_id}/")
}

pub fn cluster_capability_policy_key(node_id: &NodeId) -> String {
    format!("/cluster/policy/capabilities/{node_id}")
}

pub fn cluster_certificate_key(certificate: &ClusterCertificate) -> String {
    format!("/cluster/certificates/{}", certificate.certificate_id)
}

pub fn cluster_invitation_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}")
}

pub fn cluster_invitation_state_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}/state")
}

pub fn cluster_rollout_desired_key() -> String {
    "/cluster/rollout/desired".to_string()
}

pub fn cluster_rollout_node_key(node_id: &NodeId) -> String {
    format!("/cluster/rollout/nodes/{node_id}")
}

pub fn cluster_module_rollout_node_prefix() -> String {
    "/cluster/module-rollouts/nodes/".to_string()
}

pub fn cluster_module_rollout_node_key(node_id: &NodeId, module_id: &sinomo_core::Id) -> String {
    format!(
        "{}{node_id}/{module_id}",
        cluster_module_rollout_node_prefix()
    )
}

pub fn cluster_module_rollout_desired_prefix() -> String {
    "/cluster/module-rollouts/desired/".to_string()
}

pub fn cluster_module_rollout_desired_key(
    module_id: &sinomo_core::Id,
    rollout_plan_id: &sinomo_core::Id,
) -> String {
    format!(
        "{}{module_id}/{rollout_plan_id}",
        cluster_module_rollout_desired_prefix()
    )
}

pub fn cluster_module_rollout_reconcile_prefix() -> String {
    "/cluster/module-rollouts/reconcile/".to_string()
}

pub fn cluster_module_rollout_reconcile_key(
    node_id: &NodeId,
    module_id: &sinomo_core::Id,
    rollout_plan_id: &sinomo_core::Id,
) -> String {
    format!(
        "{}{node_id}/{module_id}/{rollout_plan_id}",
        cluster_module_rollout_reconcile_prefix()
    )
}

pub fn read_cluster_rollout_desired_from_kv(
    paths: &ClusterPaths,
) -> Result<Option<ClusterRolloutDesiredRecord>, NetworkError> {
    read_cluster_kv_value(paths, &cluster_rollout_desired_key())
}

pub fn sync_cluster_rollout_desired_to_kv(
    paths: &ClusterPaths,
    desired: &ClusterRolloutDesiredRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_rollout_desired_key(),
        desired,
    )
}

pub fn read_cluster_invitation_from_kv(
    paths: &ClusterPaths,
    invite_id: &ClusterInviteId,
) -> Result<sinomo_core::ClusterJoinInvite, NetworkError> {
    let key = cluster_invitation_key(invite_id);
    let entry = read_cluster_kv_entry(paths, &key)?.ok_or_else(|| NetworkError::StorageIo {
        path: key.clone(),
        detail: "cluster invitation is not published".to_string(),
    })?;
    entry.decode().map_err(Into::into)
}

pub fn cluster_invitation_publish_txn(
    invite: &sinomo_core::ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    cluster_invitation_publish_txn_with_snapshot(None, invite)
}

pub fn cluster_invitation_publish_txn_from_kv(
    paths: &ClusterPaths,
    invite: &sinomo_core::ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_invitation_publish_txn_with_snapshot(Some(snapshot.as_ref()), invite)
}

fn cluster_invitation_publish_txn_with_snapshot(
    snapshot: Option<&KvSnapshot>,
    invite: &sinomo_core::ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    let invitation_state = ClusterInvitationStateRecord {
        cluster_id: invite.cluster_id.clone(),
        invite_id: invite.invite_id.clone(),
        state: "active".to_string(),
        issuer_node_id: invite.issuer_node_id.clone(),
        expires_at: invite.expires_at,
        updated_at: OffsetDateTime::now_utc(),
    };
    let mut success = Vec::new();
    push_snapshot_diff_put_op(
        snapshot,
        cluster_invitation_key(&invite.invite_id),
        invite,
        &mut success,
    )?;
    push_invitation_state_diff_op(snapshot, &invitation_state, &mut success)?;
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

fn push_invitation_state_diff_op(
    snapshot: Option<&KvSnapshot>,
    invitation_state: &ClusterInvitationStateRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let key = cluster_invitation_state_key(&invitation_state.invite_id);
    if let Some(snapshot) = snapshot
        && let Some(entry) = snapshot.get(&key)
        && let Ok(existing) = entry.decode::<ClusterInvitationStateRecord>()
        && cluster_invitation_state_stable_eq(&existing, invitation_state)
    {
        return Ok(());
    }
    ops.push(KvOp::put_serde(key, invitation_state, None)?);
    Ok(())
}

fn cluster_invitation_state_stable_eq(
    left: &ClusterInvitationStateRecord,
    right: &ClusterInvitationStateRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.invite_id == right.invite_id
        && left.state == right.state
        && left.issuer_node_id == right.issuer_node_id
        && left.expires_at == right.expires_at
}

pub fn sync_cluster_invitation_to_kv(
    paths: &ClusterPaths,
    invite: &sinomo_core::ClusterJoinInvite,
) -> Result<KvSnapshot, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let txn = cluster_invitation_publish_txn_with_snapshot(Some(snapshot.as_ref()), invite)?;
    if txn.success.is_empty() {
        return Ok(snapshot.as_ref().clone());
    }
    let state = read_cluster_state_without_kv(paths)?;
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

pub fn sync_cluster_certificate_to_kv(
    paths: &ClusterPaths,
    certificate: &ClusterCertificate,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_certificate_key(certificate),
        certificate,
    )
}

pub fn sync_cluster_rollout_node_to_kv(
    paths: &ClusterPaths,
    status: &sinomo_core::ClusterNodeRolloutStatus,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_rollout_node_key(node_id),
        status,
    )
}

pub fn sync_cluster_module_rollout_node_to_kv(
    paths: &ClusterPaths,
    status: &ClusterModuleRolloutStatusRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_module_rollout_node_key(&status.node_id, &status.module_id),
        status,
    )
}

pub fn sync_cluster_module_rollout_desired_to_kv(
    paths: &ClusterPaths,
    desired: &ClusterModuleRolloutDesiredRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_module_rollout_desired_key(
            &desired.rollout_plan.module_id,
            &desired.rollout_plan.rollout_plan_id,
        ),
        desired,
    )
}

pub fn sync_cluster_module_rollout_reconciliation_to_kv(
    paths: &ClusterPaths,
    reconciliation: &ClusterModuleRolloutReconciliationRecord,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_module_rollout_reconcile_key(
            &reconciliation.node_id,
            &reconciliation.module_id,
            &reconciliation.rollout_plan_id,
        ),
        reconciliation,
    )
}

fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let op = KvOp::put_serde(key, value, None)?;
    if cluster_kv_put_value_is_noop(paths, &op)? {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(paths, node_id, KvCommand::Op(op))
}

fn cluster_kv_put_value_is_noop(paths: &ClusterPaths, op: &KvOp) -> Result<bool, NetworkError> {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return Ok(false);
    };
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(key) else {
        return Ok(false);
    };
    Ok(entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
}

impl From<&ClusterBootstrapState> for ClusterIdentityRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        Self {
            cluster_id: state.cluster_id.clone(),
            cluster_name: state.cluster_name.clone(),
            local_node_id: state.local_node_id.clone(),
            local_node_display_name: state.local_node_display_name.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            initialized_at: state.initialized_at,
            updated_at: state.updated_at,
        }
    }
}

impl From<&ClusterBootstrapState> for ClusterRaftConfigRecord {
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

impl From<&ClusterMemberRecord> for ClusterCapabilityPolicyRecord {
    fn from(member: &ClusterMemberRecord) -> Self {
        Self {
            cluster_id: member.cluster_id.clone(),
            node_id: member.node_id.clone(),
            capabilities: member.capabilities.clone(),
            allowed_capabilities: member.allowed_capabilities.clone(),
            updated_at: member.updated_at,
        }
    }
}

pub fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

pub fn merge_member_record(
    local: &mut ClusterBootstrapState,
    incoming: ClusterMemberRecord,
) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            cleanup_member_iroh_endpoints(existing);
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed |= cleanup_member_iroh_endpoints(existing);
        changed
    } else {
        let mut incoming = incoming;
        cleanup_member_iroh_endpoints(&mut incoming);
        local.members.push(incoming);
        true
    }
}

pub fn upsert_member_record(state: &mut ClusterBootstrapState, member: ClusterMemberRecord) {
    if let Some(existing) = state
        .members
        .iter_mut()
        .find(|existing| existing.node_id == member.node_id)
    {
        *existing = member;
    } else {
        state.members.push(member);
    }
}

pub fn cleanup_cluster_iroh_endpoints(state: &mut ClusterBootstrapState) -> bool {
    state.members.iter_mut().fold(false, |changed, member| {
        cleanup_member_iroh_endpoints(member) || changed
    })
}

pub fn cleanup_member_iroh_endpoints(member: &mut ClusterMemberRecord) -> bool {
    let original = member.endpoints.clone();
    let mut endpoints: Vec<ClusterEndpointRecord> = Vec::new();
    let mut iroh_stable_flags: Vec<bool> = Vec::new();
    let mut iroh_by_public_key: BTreeMap<String, usize> = BTreeMap::new();

    for mut endpoint in original.iter().cloned() {
        let Some(public_key) = iroh_endpoint_public_key(&endpoint) else {
            endpoints.push(endpoint);
            iroh_stable_flags.push(false);
            continue;
        };
        let was_stable =
            endpoint.endpoint_id == crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
        endpoint.endpoint_id = crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
        if let Some(index) = iroh_by_public_key.get(&public_key).copied() {
            if iroh_endpoint_should_replace(
                &endpoints[index],
                iroh_stable_flags[index],
                &endpoint,
                was_stable,
            ) {
                endpoints[index] = endpoint;
                iroh_stable_flags[index] = was_stable;
            }
        } else {
            iroh_by_public_key.insert(public_key, endpoints.len());
            endpoints.push(endpoint);
            iroh_stable_flags.push(was_stable);
        }
    }

    if member.endpoints == endpoints {
        false
    } else {
        member.endpoints = endpoints;
        true
    }
}

fn iroh_endpoint_should_replace(
    current: &ClusterEndpointRecord,
    current_was_stable: bool,
    candidate: &ClusterEndpointRecord,
    candidate_was_stable: bool,
) -> bool {
    let current_rank = iroh_endpoint_preference_rank(current, current_was_stable);
    let candidate_rank = iroh_endpoint_preference_rank(candidate, candidate_was_stable);
    candidate_rank > current_rank
}

fn iroh_endpoint_preference_rank(endpoint: &ClusterEndpointRecord, was_stable: bool) -> (u8, u8) {
    let stable_rank = u8::from(was_stable);
    let reachability_rank = match endpoint.privacy {
        ClusterEndpointPrivacy::InternetDirect => 3,
        ClusterEndpointPrivacy::Lan => 2,
        ClusterEndpointPrivacy::Relayed => 1,
        ClusterEndpointPrivacy::LocalOnly => 0,
    };
    (stable_rank, reachability_rank)
}

pub fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    mut endpoint: ClusterEndpointRecord,
) -> bool {
    if let Some(public_key) = iroh_endpoint_public_key(&endpoint) {
        endpoint.endpoint_id = crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
    }
    let incoming_iroh_public_key = iroh_endpoint_public_key(&endpoint);
    let mut changed = false;
    if let Some(public_key) = incoming_iroh_public_key.as_deref() {
        let endpoint_id = endpoint.endpoint_id.clone();
        let before = member.endpoints.len();
        member.endpoints.retain(|candidate| {
            candidate.endpoint_id == endpoint_id
                || iroh_endpoint_public_key(candidate).as_deref() != Some(public_key)
        });
        changed |= member.endpoints.len() != before;
    }

    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            changed
        } else if cluster_endpoint_stable_eq(existing, &endpoint) {
            changed
        } else {
            *existing = endpoint;
            true
        }
    } else if member.endpoints.iter().any(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        changed
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

pub fn iroh_endpoint_public_key(endpoint: &ClusterEndpointRecord) -> Option<String> {
    if endpoint.transport != ClusterTransportKind::Iroh {
        return None;
    }
    let rest = endpoint.address.strip_prefix("iroh://")?;
    let public_key = rest.split(['?', '#']).next()?.trim();
    if public_key.is_empty() {
        None
    } else {
        Some(public_key.to_string())
    }
}

pub fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        if !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "daemon.main.run")
        {
            continue;
        }
        let mut endpoints = member.endpoints.clone();
        endpoints.sort_by_key(|endpoint| {
            (
                cluster_endpoint_static_fallback_rank(endpoint),
                endpoint.priority,
                endpoint.address.clone(),
            )
        });
        targets.extend(endpoints.into_iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
        }));
    }
    targets
}

pub(crate) fn read_object_file<T: serde::de::DeserializeOwned>(
    path: &PathBuf,
) -> Result<T, NetworkError> {
    let bytes = read_object_file_bytes(path)?;
    if let Some(observer) = io_observer() {
        observer.on_disk_read(bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

pub(crate) fn read_optional_object_file<T: serde::de::DeserializeOwned>(
    path: &PathBuf,
) -> Result<Option<T>, NetworkError> {
    let Some(bytes) = read_optional_object_file_bytes(path)? else {
        return Ok(None);
    };
    if let Some(observer) = io_observer() {
        observer.on_disk_read(bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes).map(Some)
}

fn read_optional_object_file_bytes(path: &PathBuf) -> Result<Option<Vec<u8>>, NetworkError> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(NetworkError::StorageIo {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    read_object_file_bytes_from_open_file(path, file).map(Some)
}

fn read_object_file_bytes(path: &PathBuf) -> Result<Vec<u8>, NetworkError> {
    let file = fs::File::open(path).map_err(|source| NetworkError::StorageIo {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    read_object_file_bytes_from_open_file(path, file)
}

fn read_object_file_bytes_from_open_file(
    path: &PathBuf,
    file: fs::File,
) -> Result<Vec<u8>, NetworkError> {
    let len = file
        .metadata()
        .map_err(|source| NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    if len > MAX_OBJECT_FILE_READ_BYTES {
        return Err(NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: format!("object file is {len} bytes; max is {MAX_OBJECT_FILE_READ_BYTES}"),
        });
    }

    let mut bytes = Vec::with_capacity(len as usize);
    let mut limited = file.take(MAX_OBJECT_FILE_READ_BYTES.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .map_err(|source| NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if bytes.len() as u64 > MAX_OBJECT_FILE_READ_BYTES {
        return Err(NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: format!("object file read exceeded max of {MAX_OBJECT_FILE_READ_BYTES} bytes"),
        });
    }
    Ok(bytes)
}

fn read_existing_object_file_for_compare(path: &PathBuf, expected_len: u64) -> Option<Vec<u8>> {
    let file = fs::File::open(path).ok()?;
    if file.metadata().ok()?.len() != expected_len {
        return None;
    }
    let mut bytes = Vec::with_capacity(expected_len as usize);
    let mut limited = file.take(expected_len.saturating_add(1));
    limited.read_to_end(&mut bytes).ok()?;
    (bytes.len() as u64 == expected_len).then_some(bytes)
}

pub(crate) fn write_object_file<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<(), NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    let desired_hash = blake3::hash(&desired);

    // Fast path: if we last wrote this exact content and the file is unchanged
    // since (length + mtime + inode still match our record), skip without
    // reading it back.
    if let Some(mark) = cached_object_file_mark(path)
        && mark.hash == desired_hash
        && let Ok(metadata) = fs::metadata(path)
        && metadata.len() == mark.len
        && metadata.modified().ok() == mark.modified
        && file_ino(&metadata) == mark.ino
    {
        if let Some(observer) = io_observer() {
            observer.on_object_file_skipped_write();
        }
        return Ok(());
    }

    // Slow path: read the file back and compare only when the length could match.
    // Handles a cold cache and files changed by another writer (e.g. a shadow
    // daemon sharing the cluster dir) without reading sparse/corrupt oversized
    // files that are already known to differ.
    let existing = read_existing_object_file_for_compare(path, desired.len() as u64);
    if let (Some(observer), Some(existing)) = (io_observer(), existing.as_ref()) {
        observer.on_disk_read(existing.len() as u64);
    }
    if existing.as_deref() == Some(desired.as_slice()) {
        // Already current: record the mark so subsequent calls take the fast path.
        let (modified, ino) = file_modified_and_ino(path);
        record_object_file_mark(path, desired_hash, desired.len() as u64, modified, ino);
        if let Some(observer) = io_observer() {
            observer.on_object_file_skipped_write();
        }
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("sobj");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_FILE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let write_temp_result = (|| -> Result<(), NetworkError> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|source| NetworkError::StorageIo {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(&desired)
            .map_err(|source| NetworkError::StorageIo {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.sync_data().map_err(|source| NetworkError::StorageIo {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(())
    })();
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = replace_file(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    sync_directory(parent)?;
    let (modified, ino) = file_modified_and_ino(path);
    record_object_file_mark(path, desired_hash, desired.len() as u64, modified, ino);
    if let Some(observer) = io_observer() {
        observer.on_object_file_actual_write();
        observer.on_disk_write(desired.len() as u64);
    }
    Ok(())
}

/// Write a fully encoded object and publish it only if `path` is absent.
///
/// The normal path stages the object in a same-directory temp file, fsyncs it,
/// then publishes it with `hard_link`, which gives atomic no-clobber semantics.
/// Filesystems without hard-link support fall back to a metadata check followed
/// by `rename`; callers must serialize writers for the same `path` so that
/// fallback cannot replace a concurrently created winner.
pub(crate) fn write_object_file_create_new_serialized<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<bool, NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("sobj");
    let temp_path = parent.join(format!(
        ".{file_name}.create-new-tmp-{}-{}",
        std::process::id(),
        OBJECT_FILE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    #[cfg(all(test, feature = "iroh-transport"))]
    record_write_object_file_create_new_attempt_for_test(path);
    let write_temp_result = (|| -> Result<(), NetworkError> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(|source| NetworkError::StorageIo {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(&desired)
            .map_err(|source| NetworkError::StorageIo {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.sync_data().map_err(|source| NetworkError::StorageIo {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(())
    })();
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    match create_new_link_or_unsupported_for_test(&temp_path, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temp_path);
        }
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temp_path);
            return Ok(false);
        }
        Err(source) if hard_link_unsupported(&source) => {
            match fs::metadata(path) {
                Ok(_) => {
                    let _ = fs::remove_file(&temp_path);
                    return Ok(false);
                }
                Err(metadata_source) if metadata_source.kind() == std::io::ErrorKind::NotFound => {}
                Err(metadata_source) => {
                    let _ = fs::remove_file(&temp_path);
                    return Err(NetworkError::StorageIo {
                        path: path.display().to_string(),
                        detail: metadata_source.to_string(),
                    });
                }
            }
            if let Err(rename_source) = fs::rename(&temp_path, path) {
                let existed = path.exists();
                let _ = fs::remove_file(&temp_path);
                if existed
                    && matches!(
                        rename_source.kind(),
                        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                    )
                {
                    return Ok(false);
                }
                return Err(NetworkError::StorageIo {
                    path: format!("{} -> {}", temp_path.display(), path.display()),
                    detail: rename_source.to_string(),
                });
            }
        }
        Err(source) => {
            let _ = fs::remove_file(&temp_path);
            return Err(NetworkError::StorageIo {
                path: format!("{} -> {}", temp_path.display(), path.display()),
                detail: source.to_string(),
            });
        }
    }
    sync_directory(parent)?;
    let desired_hash = blake3::hash(&desired);
    let (modified, ino) = file_modified_and_ino(path);
    record_object_file_mark(path, desired_hash, desired.len() as u64, modified, ino);
    if let Some(observer) = io_observer() {
        observer.on_object_file_actual_write();
        observer.on_disk_write(desired.len() as u64);
    }
    Ok(true)
}

fn create_new_link_or_unsupported_for_test(
    temp_path: &std::path::Path,
    path: &PathBuf,
) -> std::io::Result<()> {
    #[cfg(test)]
    if write_object_file_create_new_forces_unsupported_link_for_test(path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "injected unsupported hard link",
        ));
    }
    fs::hard_link(temp_path, path)
}

fn hard_link_unsupported(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::Unsupported
        || matches!(
            source.raw_os_error(),
            Some(
                1     // EPERM on Unix filesystems that disallow hard links.
                    | 18   // EXDEV, kept defensive even though temp and target share a directory.
                    | 38   // ENOSYS
                    | 45   // EOPNOTSUPP on macOS/BSD
                    | 50   // ERROR_NOT_SUPPORTED on Windows
                    | 95   // EOPNOTSUPP on Linux
                    | 1314 // ERROR_PRIVILEGE_NOT_HELD on Windows
            )
        )
}

fn replace_file(temp_path: &std::path::Path, path: &std::path::Path) -> Result<(), NetworkError> {
    match fs::rename(temp_path, path) {
        Ok(()) => Ok(()),
        Err(source)
            if path.exists()
                && matches!(
                    source.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                ) =>
        {
            fs::remove_file(path).map_err(|remove_source| NetworkError::StorageIo {
                path: path.display().to_string(),
                detail: format!(
                    "replace failed with {}; additionally failed to remove existing file: {}",
                    source, remove_source
                ),
            })?;
            fs::rename(temp_path, path).map_err(|rename_source| NetworkError::StorageIo {
                path: format!("{} -> {}", temp_path.display(), path.display()),
                detail: format!(
                    "replace after removing existing file failed: {}",
                    rename_source
                ),
            })
        }
        Err(source) => Err(NetworkError::StorageIo {
            path: format!("{} -> {}", temp_path.display(), path.display()),
            detail: source.to_string(),
        }),
    }
}

fn sync_directory(path: &std::path::Path) -> Result<(), NetworkError> {
    #[cfg(windows)]
    {
        let _ = path;
        return Ok(());
    }
    #[cfg(not(windows))]
    fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}
