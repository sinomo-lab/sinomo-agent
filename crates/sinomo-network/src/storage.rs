use crate::error::NetworkError;
use crate::lan::local_network_cache_key;
use crate::object::SinomoObjectCodec;
use crate::prelude::*;
use crate::rpc::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest,
    ObjectRpcResponse,
};
use crate::{
    cluster_route_next_hop, discover_default_public_ip_candidates_for_local_ips_async,
    discover_local_ip_candidates_for_targets, discover_local_ip_candidates_for_targets_async,
    format_ip_port, is_invitable_ip, is_loopback_connect_address,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, ClusterKvRaftCommitResult, ClusterKvRaftConfigRecord,
    ClusterKvRaftLeaderElectionOptions, ClusterKvRaftLocalVoteMetadata,
    ClusterKvRaftUncommittedAppend, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse, KvRevision,
};
#[cfg(test)]
use sinomo_kv::{
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration,
};
use sinomo_storage::durable_file::{self, file_ino};

pub(crate) const MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES: usize = 128;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES: usize =
    sinomo_kv::MAX_CLUSTER_KV_ACTIVE_WATCHES;
pub(crate) const PUBLIC_ENDPOINT_REACHABILITY_POSITIVE_CACHE_TTL: Duration =
    Duration::from_secs(60);
pub(crate) const PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL: Duration =
    Duration::from_secs(10);
pub(crate) const MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES: usize = 128;
const CLUSTER_NETWORK_EXPIRY_GRACE_SECONDS: i64 = 600;
const CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES: u16 = 3;
pub const CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS: i64 = 60;
pub const CLUSTER_NETWORK_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);
pub const CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL: Duration = Duration::from_secs(300);
pub const CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL: Duration = Duration::from_secs(900);
pub const CLUSTER_NETWORK_DISCOVERY_INTERVAL: Duration = CLUSTER_NETWORK_PUBLISH_INTERVAL;
pub const CLUSTER_NETWORK_PROBE_INTERVAL: Duration = Duration::from_secs(60);
pub const CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL: Duration = Duration::from_secs(300);
pub const CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL: Duration = Duration::from_secs(900);
pub const CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL: Duration = Duration::from_millis(250);
pub const CLUSTER_NETWORK_INFO_TTL_SECONDS: i64 = 3600;
pub const CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE: u64 = 8;
pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 = 3;
pub const CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 32;
pub const CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 4;
pub const CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 1;
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = 900;
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 = 7200;
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 = 120;
pub use sinomo_node::DAEMON_MAIN_RUN_CAPABILITY;
pub const DAEMON_SHADOW_SUPERVISE_CAPABILITY: &str = "daemon.shadow.supervise";
pub const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
pub const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";
pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES;
const FILE_CACHE_CAPACITY: usize = 4096;
const MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES: usize = 128;
#[cfg(test)]
static CLUSTER_KV_WATCH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_RAFT_FANOUT_ACTIVE: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_FANOUT_PENDING: AtomicBool = AtomicBool::new(false);
static LOCAL_CLUSTER_CONNECTIVITY_RECORDS: LazyLock<
    Mutex<HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchEventCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
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
static LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PUBLIC_ENDPOINT_REACHABILITY_CACHE: LazyLock<Mutex<PublicEndpointReachabilityCache>> =
    LazyLock::new(|| Mutex::new(PublicEndpointReachabilityCache::default()));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterFileMark {
    exists: bool,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

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

#[derive(Clone)]
struct LocalClusterConnectivityRecordCacheEntry {
    records: Vec<ClusterConnectivityRecord>,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

#[cfg(test)]
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

/// Cache of last-written object files. The cluster directory can be shared by the
/// main and shadow daemons, so the fast-path skip is guarded by a `stat`: an entry
/// is trusted only when the file's length, mtime, and inode still match what we
/// wrote, so a write by another process is detected and falls back to a real read
/// + compare.
static LAST_WRITTEN_OBJECT_FILES: LazyLock<durable_file::ObjectFileWriteMarkCache> =
    LazyLock::new(|| durable_file::ObjectFileWriteMarkCache::new(FILE_CACHE_CAPACITY));

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

#[cfg(test)]
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
    LAST_WRITTEN_OBJECT_FILES.capacity()
}

#[cfg(test)]
pub(crate) fn object_file_write_mark_cache_contains_for_test(path: &PathBuf) -> bool {
    LAST_WRITTEN_OBJECT_FILES.contains(path)
}

#[cfg(test)]
pub(crate) fn record_object_file_write_mark_for_test(path: &PathBuf) {
    LAST_WRITTEN_OBJECT_FILES.record_mark(
        path,
        blake3::hash(b"stable"),
        6,
        Some(SystemTime::UNIX_EPOCH),
        1,
    );
}

#[cfg(test)]
pub(crate) fn refresh_object_file_write_mark_for_test(path: &PathBuf) -> bool {
    LAST_WRITTEN_OBJECT_FILES.refresh(path)
}

#[cfg(test)]
pub(crate) fn remove_object_file_write_mark_entries_under_root_for_test(root: &std::path::Path) {
    LAST_WRITTEN_OBJECT_FILES.retain_not_under_root(root);
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
        let kv = sinomo_kv::ClusterKvPaths::new(&root);
        Self {
            state: root.join("state.sobj"),
            iroh: root.join("iroh"),
            connectivity_local: root.join("connectivity-local.sobj"),
            kv_snapshot: kv.snapshot,
            kv_raft: kv.raft,
            kv_watches: kv.watches,
            kv_watches_lock: kv.watches_lock,
            kv_watches_next_id: kv.watches_next_id,
            kv_watch_events: kv.watch_events,
            kv_watch_events_log: kv.watch_events_log,
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), NetworkError> {
        fs::create_dir_all(&self.root).map_err(|source| NetworkError::StorageIo {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn kv_paths(&self) -> sinomo_kv::ClusterKvPaths {
        sinomo_kv::ClusterKvPaths {
            root: self.root.clone(),
            snapshot: self.kv_snapshot.clone(),
            raft: self.kv_raft.clone(),
            watches: self.kv_watches.clone(),
            watches_lock: self.kv_watches_lock.clone(),
            watches_next_id: self.kv_watches_next_id.clone(),
            watch_events: self.kv_watch_events.clone(),
            watch_events_log: self.kv_watch_events_log.clone(),
        }
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
pub(crate) fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &std::path::Path) -> bool {
    sinomo_kv::cluster_kv_watch_notifier_has_raw_key_for_test(path)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterPaths) -> usize {
    sinomo_kv::cluster_kv_watch_notifier_waiter_count_for_test(&paths.kv_paths())
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &std::path::Path) {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRaftRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterMemberRole,
    pub previous_state: ClusterMemberState,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMemberRemoval {
    pub removed: Vec<ClusterMemberRecord>,
}

pub trait ClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;
}

pub trait ClusterMemberMutationAsyncHost {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub fn prepare_cluster_join_registration_response(
    paths: &ClusterPaths,
    state: ClusterBootstrapState,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let persisted_invite = read_cluster_invitation_from_kv(paths, &request.invite.invite_id)?;
    if request.invite.cluster_public_key != persisted_invite.cluster_public_key
        || request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "invitation rejected".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                persisted_invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if persisted_invite.expires_at <= now {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                persisted_invite
                    .expires_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request
        .members
        .iter()
        .any(|member| !persisted_invite.allowed_member_roles.contains(&member.role))
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "join request includes a member role not allowed by the invite".to_string(),
            state,
            kv_snapshot: None,
        });
    }

    let mut accepted_state = state.clone();
    for mut member in request.members {
        member.cluster_id = accepted_state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = now;
        upsert_member_record(&mut accepted_state, member);
    }
    accepted_state.kv_revision = accepted_state.kv_revision.saturating_add(1);
    accepted_state.updated_at = now;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: accepted_state,
        kv_snapshot: None,
    })
}

pub fn handle_cluster_join_registration_request_with_host(
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if let Some(observation) = host.local_daemon_membership_observation(paths, &state)? {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    let mut prepared =
        prepare_cluster_join_registration_response(paths, state.clone(), request.clone(), now)?;
    if !prepared.accepted {
        return Ok(prepared);
    }

    if !sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        let rpc_request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_REGISTER_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &request,
        )?;
        let response = forward_cluster_membership_rpc_to_voter_with_host(
            paths,
            &state,
            &rpc_request,
            "join introducer is not a voter and could not forward the join to any voter",
            host,
        )?;
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        if response.accepted {
            record_forwarded_join_registration_acceptance_with_host(
                paths,
                &state,
                &request.members,
                &response,
                host,
            )?;
        }
        return Ok(response);
    }

    let snapshot = host.commit_join_registration_state(paths, &prepared.state)?;
    write_cluster_state(paths, &prepared.state)?;
    prepared.kv_snapshot = Some(snapshot);
    Ok(prepared)
}

pub async fn handle_cluster_join_registration_request_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    if let Some(observation) = host
        .local_daemon_membership_observation_async(paths, &state)
        .await?
    {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    let prepare_paths = paths.clone();
    let prepare_state = state.clone();
    let prepare_request = request.clone();
    let mut prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_join_registration_response(
                &prepare_paths,
                prepare_state,
                prepare_request,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster join registration prepare".to_string(),
            detail: format!("join registration prepare task failed: {source}"),
        })??;
    if !prepared.accepted {
        return Ok(prepared);
    }

    if !sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        let rpc_request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_REGISTER_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &request,
        )?;
        let response = forward_cluster_membership_rpc_to_voter_async_with_host(
            runtime,
            paths,
            &state,
            &rpc_request,
            "join introducer is not a voter and could not forward the join to any voter",
            host,
        )
        .await?;
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        if response.accepted {
            record_forwarded_join_registration_acceptance_async_with_host(
                runtime,
                paths,
                &state,
                &request.members,
                &response,
                host,
            )
            .await?;
        }
        return Ok(response);
    }

    let snapshot = host
        .commit_join_registration_state_async(paths, &prepared.state)
        .await?;
    let write_paths = paths.clone();
    let write_state = prepared.state.clone();
    runtime
        .spawn_blocking(move || write_cluster_state(&write_paths, &write_state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        })??;
    prepared.kv_snapshot = Some(snapshot);
    Ok(prepared)
}

pub fn send_cluster_join_registration_over_membership_rpc<S>(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
    mut send_rpc: S,
) -> Result<ClusterJoinRegistrationResponse, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ObjectRpcRequest,
        Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>,
{
    let challenge = crate::invite::random_url_secret(32)?;
    let challenge_request = crate::invite::ClusterInviteChallengeRequest {
        protocol: crate::invite::CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_rpc = ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &challenge_request,
    )?;
    let proof_response = send_rpc(endpoint, &challenge_rpc, timeout)?;
    let proof: crate::invite::ClusterInviteChallengeProof = proof_response.decode_body()?;
    crate::invite::verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;

    let register_rpc = ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_REGISTER_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let response = send_rpc(endpoint, &register_rpc, timeout)?;
    response.decode_body()
}

pub fn record_forwarded_join_registration_acceptance_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    if let Some(snapshot) = response.kv_snapshot.as_ref() {
        write_imported_cluster_kv_snapshot_if_changed(paths, snapshot)?;
    }
    let mut local_state = read_cluster_state_without_kv(paths)?;
    merge_cluster_join_registration(&mut local_state, response.state.clone());
    write_cluster_state(paths, &local_state)?;
    host.remember_successful_join_targets(paths, state, request_members)
}

pub async fn record_forwarded_join_registration_acceptance_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    let paths_for_state = paths.clone();
    let response_state = response.state.clone();
    let snapshot = response.kv_snapshot.clone();
    runtime
        .spawn_blocking(move || {
            if let Some(snapshot) = snapshot.as_ref() {
                write_imported_cluster_kv_snapshot_if_changed(&paths_for_state, snapshot)?;
            }
            let mut local_state = read_cluster_state_without_kv(&paths_for_state)?;
            merge_cluster_join_registration(&mut local_state, response_state);
            write_cluster_state(&paths_for_state, &local_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster forwarded join acceptance".to_string(),
            detail: format!("forwarded join acceptance task failed: {source}"),
        })??;
    host.remember_successful_join_targets_async(paths, state, request_members)
        .await
}

pub fn reconcile_cluster_join_with_introducers_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    tcp_timeout: Duration,
    iroh_timeout: Duration,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<ClusterJoinReconciliation, NetworkError> {
    if invite.communication_channels.is_empty() && invite.introducers.is_empty() {
        return Ok(ClusterJoinReconciliation {
            accepted_endpoint: None,
            kv_snapshot: None,
        });
    }
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite: invite.clone(),
        members: state.members.clone(),
    };
    let deadline = Instant::now() + tcp_timeout.max(iroh_timeout);
    let remaining_for = |channel_timeout: Duration| {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_millis(1));
        remaining.min(channel_timeout)
    };
    let mut failures = Vec::new();
    for channel in crate::invite_channels_for_join_registration(&request.invite) {
        let tcp_remaining = remaining_for(tcp_timeout);
        let iroh_remaining = remaining_for(iroh_timeout);
        match host.send_join_registration(
            &channel.endpoint,
            &request,
            tcp_remaining,
            iroh_remaining,
        ) {
            Ok(response) => {
                if response.accepted {
                    if let Some(reconciliation) = reconcile_accepted_join_response_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        response,
                        connectivity_paths,
                        host,
                    )? {
                        return Ok(reconciliation);
                    }
                    failures.push(format!(
                        "{} ({:?}) returned unverified join acceptance",
                        channel.endpoint.address, channel.endpoint.transport
                    ));
                    continue;
                }
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        match channel.endpoint.transport {
                            ClusterTransportKind::Iroh => iroh_remaining,
                            _ => tcp_remaining,
                        },
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) rejected join: {}",
                    channel.endpoint.address, channel.endpoint.transport, response.detail
                ));
            }
            Err(error) => {
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        match channel.endpoint.transport {
                            ClusterTransportKind::Iroh => iroh_remaining,
                            _ => tcp_remaining,
                        },
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) failed: {}",
                    channel.endpoint.address, channel.endpoint.transport, error
                ));
            }
        }
    }
    if !failures.is_empty() {
        return Err(NetworkError::Io {
            operation: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {}. If the invite only contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`.",
                failures.join("; ")
            ),
        });
    }
    Ok(ClusterJoinReconciliation {
        accepted_endpoint: None,
        kv_snapshot: None,
    })
}

fn reconcile_accepted_join_response_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let reconciliation =
        reconcile_accepted_cluster_join_response(state, invite, endpoint, response)?;
    observe_join_reconciliation_connectivity(
        connectivity_paths,
        state,
        reconciliation.as_ref(),
        host,
    )?;
    Ok(reconciliation)
}

fn reconcile_already_accepted_join_from_snapshot_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let snapshot = match host.fetch_cluster_kv_snapshot(endpoint, timeout) {
        Ok(snapshot) => snapshot,
        Err(_) => return Ok(None),
    };
    let reconciliation =
        reconcile_accepted_cluster_join_snapshot(state, invite, endpoint, snapshot)?;
    observe_join_reconciliation_connectivity(
        connectivity_paths,
        state,
        reconciliation.as_ref(),
        host,
    )?;
    Ok(reconciliation)
}

fn observe_join_reconciliation_connectivity(
    connectivity_paths: Option<&ClusterPaths>,
    state: &ClusterBootstrapState,
    reconciliation: Option<&ClusterJoinReconciliation>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<(), NetworkError> {
    let (Some(paths), Some(reconciliation)) = (connectivity_paths, reconciliation) else {
        return Ok(());
    };
    host.observe_join_reconciliation_connectivity(paths, state, reconciliation)
}

pub fn accepted_cluster_join_state_from_kv_snapshot(
    pending: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    snapshot: &KvSnapshot,
) -> Result<Option<ClusterBootstrapState>, NetworkError> {
    let Some(identity_entry) = snapshot.get(&cluster_identity_key()) else {
        return Ok(None);
    };
    let identity: ClusterIdentityRecord = identity_entry.decode()?;
    if crate::invite_has_embedded_cluster_identity(invite)
        && identity.cluster_id != invite.cluster_id
    {
        return Ok(None);
    }
    let mut members = Vec::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.cluster_id != identity.cluster_id {
            return Ok(None);
        }
        if let Some(policy_entry) = snapshot.get(&cluster_capability_policy_key(&member.node_id)) {
            let policy: ClusterCapabilityPolicyRecord = policy_entry.decode()?;
            member.capabilities = policy.capabilities;
            member.allowed_capabilities = policy.allowed_capabilities;
        }
        for endpoint_entry in snapshot.range_prefix_limited(
            &cluster_endpoint_prefix(&member.node_id),
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )? {
            let endpoint: ClusterEndpointRecord = endpoint_entry.decode()?;
            if endpoint.node_id == member.node_id {
                upsert_member_endpoint(&mut member, endpoint);
            }
        }
        cleanup_member_iroh_endpoints(&mut member);
        members.push(member);
    }
    let authority_public_key = if invite.issuer_public_key.trim().is_empty() {
        invite.cluster_public_key.as_str()
    } else {
        invite.issuer_public_key.as_str()
    };
    let authority_member_is_authorized = members.iter().any(|member| {
        member.node_public_key == authority_public_key
            && member.state.is_authorized()
            && (invite.issuer_public_key.trim().is_empty()
                || member.node_id == invite.issuer_node_id)
    });
    if members.is_empty()
        || authority_public_key.trim().is_empty()
        || !authority_member_is_authorized
    {
        return Ok(None);
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    if local_processes.is_empty()
        || local_processes.iter().any(|local| {
            !members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
    {
        return Ok(None);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(Some(ClusterBootstrapState {
        cluster_id: identity.cluster_id,
        cluster_name: identity.cluster_name,
        local_node_id: identity.local_node_id,
        local_node_display_name: identity.local_node_display_name,
        term: identity.term,
        kv_revision: identity.kv_revision,
        members,
        initialized_at: identity.initialized_at,
        updated_at: identity.updated_at,
    }))
}

pub fn accepted_cluster_join_response_state_authorizes_local_processes(
    pending: &ClusterBootstrapState,
    accepted: &ClusterBootstrapState,
) -> bool {
    if accepted.cluster_id != pending.cluster_id {
        return false;
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    !local_processes.is_empty()
        && local_processes.iter().all(|local| {
            accepted.members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
}

pub fn reconcile_accepted_cluster_join_response(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    if let Some(snapshot) = response.kv_snapshot {
        let snapshot = snapshot.canonicalized();
        let Some(accepted_state) =
            accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
        else {
            return Ok(None);
        };
        return Ok(Some(apply_accepted_cluster_join_state(
            state,
            invite,
            endpoint,
            accepted_state,
            Some(snapshot),
        )));
    }

    if !crate::invite_has_embedded_cluster_identity(invite)
        || !accepted_cluster_join_response_state_authorizes_local_processes(state, &response.state)
    {
        return Ok(None);
    }

    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        endpoint,
        response.state,
        None,
    )))
}

pub fn reconcile_accepted_cluster_join_snapshot(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    snapshot: KvSnapshot,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let snapshot = snapshot.canonicalized();
    let Some(accepted_state) =
        accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
    else {
        return Ok(None);
    };
    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        endpoint,
        accepted_state,
        Some(snapshot),
    )))
}

pub fn apply_accepted_cluster_join_state(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    accepted_state: ClusterBootstrapState,
    kv_snapshot: Option<KvSnapshot>,
) -> ClusterJoinReconciliation {
    merge_cluster_join_registration(state, accepted_state);
    let accepted_endpoint = join_channel_endpoint_for_accepted_state(state, invite, endpoint);
    remember_join_channel_endpoint_in_local_state(state, &accepted_endpoint);
    ClusterJoinReconciliation {
        accepted_endpoint: Some(accepted_endpoint),
        kv_snapshot,
    }
}

pub fn join_channel_endpoint_for_accepted_state(
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
) -> ClusterEndpointRecord {
    let Some(host) = cluster_endpoint_address_host(&endpoint.address) else {
        return endpoint.clone();
    };
    let member = state.members.iter().find(|member| {
        member.state.is_authorized()
            && (member.display_name == host
                || member.endpoints.iter().any(|candidate| {
                    cluster_endpoint_address_host(&candidate.address).as_deref() == Some(host)
                }))
    });
    let node_id = member
        .map(|member| &member.node_id)
        .or_else(|| {
            state
                .members
                .iter()
                .any(|member| {
                    member.node_id == invite.issuer_node_id && member.state.is_authorized()
                })
                .then_some(&invite.issuer_node_id)
        })
        .unwrap_or(&endpoint.node_id);
    retarget_cluster_endpoint_to_node(endpoint, node_id)
}

fn retarget_cluster_endpoint_to_node(
    endpoint: &ClusterEndpointRecord,
    node_id: &NodeId,
) -> ClusterEndpointRecord {
    if *node_id == endpoint.node_id {
        return endpoint.clone();
    }
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            crate::daemon_lan_endpoint(node_id, endpoint.address.clone(), endpoint.priority)
        }
        _ => {
            let mut retargeted = endpoint.clone();
            retargeted.node_id = node_id.clone();
            retargeted
        }
    }
}

fn remember_join_channel_endpoint_in_local_state(
    state: &mut ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) {
    if endpoint.node_id == state.local_node_id
        || endpoint.address.parse::<SocketAddr>().is_ok()
        || !matches!(
            endpoint.transport,
            ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
        )
    {
        return;
    }
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id && member.state.is_authorized())
    else {
        return;
    };
    upsert_member_endpoint(member, endpoint.clone());
}

fn cluster_endpoint_address_host(address: &str) -> Option<&str> {
    let host = address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(address)
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']');
    (!host.is_empty()).then_some(host)
}

pub fn forward_cluster_membership_rpc_to_voter_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut failures = Vec::new();
    for voter in cluster_join_voter_targets(paths, state) {
        match host.send_membership_rpc(&voter, request) {
            Ok(response) => return Ok(response),
            Err(error) => failures.push(format!(
                "{} ({:?}): {error}",
                voter.address, voter.transport
            )),
        }
    }
    for voter in cluster_join_voter_targets(paths, state) {
        for peer in cluster_member_publish_targets(paths, state)
            .into_iter()
            .filter(|peer| peer.node_id != voter.node_id)
        {
            match host.relay_membership_rpc_to_voter_via_peer(state, &voter, &peer, request) {
                Ok(response) => return Ok(response),
                Err(error) => failures.push(format!(
                    "relay via {} ({:?}) to {} failed: {error}",
                    peer.address, peer.transport, voter.node_id
                )),
            }
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub async fn forward_cluster_membership_rpc_to_voter_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut impl ClusterMembershipRpcAsyncForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let paths_for_targets = paths.clone();
    let state_for_targets = state.clone();
    let (voter_targets, publish_targets) = runtime
        .spawn_blocking(move || {
            (
                cluster_join_voter_targets(&paths_for_targets, &state_for_targets),
                cluster_member_publish_targets(&paths_for_targets, &state_for_targets),
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster membership voter target selection".to_string(),
            detail: format!("voter target selection task failed: {source}"),
        })?;
    let mut failures = Vec::new();
    for voter in &voter_targets {
        match host.send_membership_rpc_async(voter, request).await {
            Ok(response) => return Ok(response),
            Err(error) => failures.push(format!(
                "{} ({:?}): {error}",
                voter.address, voter.transport
            )),
        }
    }
    for voter in &voter_targets {
        for peer in publish_targets
            .iter()
            .filter(|peer| peer.node_id != voter.node_id)
        {
            match host
                .relay_membership_rpc_to_voter_via_peer_async(state, voter, peer, request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => failures.push(format!(
                    "relay via {} ({:?}) to {} failed: {error}",
                    peer.address, peer.transport, voter.node_id
                )),
            }
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn cluster_join_challenge_rpc_request<T: Serialize + ?Sized>(
    challenge: &T,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        challenge,
    )
}

/// A bridge introducer named in an invite minted before it joined is not itself a
/// proof-capable channel/issuer, so it cannot answer a join challenge. Relay the challenge to
/// a reachable voter (the invite issuer can produce an issuer-signed proof). The routing-aware
/// send relays the challenge across segments when no voter is directly reachable.
pub fn forward_cluster_join_challenge_rpc_to_voter_with_host(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    forward_cluster_membership_rpc_to_voter_with_host(
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any voter",
        host,
    )
}

pub async fn forward_cluster_join_challenge_rpc_to_voter_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut impl ClusterMembershipRpcAsyncForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state_paths = paths.clone();
    let state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    forward_cluster_membership_rpc_to_voter_async_with_host(
        runtime,
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any voter",
        host,
    )
    .await
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
    /// The publisher's local cluster KV snapshot revision. When present, the
    /// voter includes its current KV snapshot in the response only if the
    /// publisher is behind — this is how learners (which cannot always dial a
    /// voter directly; member publish is bridge-relayable) converge their
    /// local KV snapshot without re-shipping it on every publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_kv_revision: Option<KvRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterMemberPublishResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterAuthenticatedPeer {
    pub node_id: String,
    pub public_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftAppendReplicationOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftAppendReplicationOptions {
        sinomo_kv::ClusterKvRaftAppendReplicationOptions {
            max_attempts: self.max_attempts,
            timeout: self.timeout,
        }
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }

    pub fn append_attempt_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        if request.snapshot.is_some() || request.append.entries.len() > 64 {
            return self.command_timeout(endpoint, remaining);
        }
        self.fair_command_timeout(endpoint, remaining)
    }
}

pub type ClusterRaftAppendReplicationOutcome = sinomo_kv::ClusterKvRaftAppendReplicationOutcome;

pub trait ClusterRaftAppendReplicationHost {
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError>;

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError>;

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn on_raft_append_sent(&mut self) {}
}

pub trait ClusterRaftAppendReplicationAsyncHost {
    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64>;

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest>;

    fn send_raft_append_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterRaftQuorumAsyncHost: ClusterRaftAppendReplicationAsyncHost {
    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct LocalClusterRaftAppendReplicationHost<S, O> {
    send_raft_append: S,
    on_raft_append_sent: O,
}

impl<S, O> ClusterRaftAppendReplicationHost for LocalClusterRaftAppendReplicationHost<S, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        (self.send_raft_append)(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        (self.on_raft_append_sent)();
    }
}

pub fn replicate_cluster_raft_to_endpoint_with_local_host<S, O>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: S,
    on_raft_append_sent: O,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    let mut host = LocalClusterRaftAppendReplicationHost {
        send_raft_append,
        on_raft_append_sent,
    };
    replicate_cluster_raft_to_endpoint_with_host(
        paths,
        state,
        endpoint,
        watch_events,
        options,
        &mut host,
    )
}

pub type ClusterRaftQuorumCommitOptions = sinomo_kv::ClusterKvRaftQuorumCommitOptions;

pub type ClusterRaftQuorumCommitResult = sinomo_kv::ClusterKvRaftQuorumCommitResult;

pub trait ClusterRaftQuorumTransportHost {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
    }
}

pub trait ClusterRaftQuorumCommitHost:
    sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError> + ClusterRaftQuorumTransportHost
{
}

impl<T> ClusterRaftQuorumCommitHost for T where
    T: sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError>
        + ClusterRaftQuorumTransportHost
{
}

pub(crate) struct LocalClusterRaftCommandStorage<'a> {
    paths: &'a ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

impl<'a> LocalClusterRaftCommandStorage<'a> {
    pub(crate) fn new(paths: &'a ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            paths,
            inner: sinomo_kv::ClusterKvRaftLocalCommandStorage::new(
                cluster_kv_raft_store(paths),
                local_node_id,
                cluster_kv_key_can_defer_materialized_snapshot,
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
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
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
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
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
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
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
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        self.commit_index_with_events(index)
    }
}

struct LocalClusterRaftQuorumCommitHost<'a, S, R, F, O> {
    storage: LocalClusterRaftCommandStorage<'a>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
}

impl<S, R, F, O> sinomo_kv::ClusterKvRaftCommandStorage
    for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
{
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

impl<S, R, F, O> ClusterRaftQuorumTransportHost for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
    O: FnMut(),
{
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let send_raft_append = &mut self.send_raft_append;
        let on_raft_append_sent = &mut self.on_raft_append_sent;
        replicate_cluster_raft_to_endpoint_with_local_host(
            paths,
            state,
            endpoint,
            &[],
            self.control_plane_timeouts
                .raft_append_replication_options(timeout),
            |endpoint, request, timeout| send_raft_append(endpoint, request, timeout),
            || on_raft_append_sent(),
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        (self.record_endpoint_command_connectivity)(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        (self.fan_out_committed_cluster_raft_command)(paths, state, events, targets);
    }
}

pub fn commit_cluster_raft_command_with_local_storage_and_transport<S, R, F, O>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
    O: FnMut(),
{
    let mut host = LocalClusterRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        control_plane_timeouts,
        send_raft_append,
        record_endpoint_command_connectivity,
        fan_out_committed_cluster_raft_command,
        on_raft_append_sent,
    };
    commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutOptions {
    pub raft_append_timeout: Duration,
    pub watch_event_timeout: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutReport {
    pub attempted_endpoints: usize,
    pub replicated_nodes: Vec<NodeId>,
    pub watch_event_batches_sent: usize,
}

pub trait ClusterRaftCommittedFanoutAsyncHost {
    fn replicate_cluster_raft_to_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendReplicationOutcome>;

    fn send_cluster_kv_watch_events_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterVoterReachabilityOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterVoterReachabilityOptions {
    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterVoterReachabilityHost {
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool;
}

pub trait ClusterVoterReachabilityAsyncHost {
    fn probe_voter_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, bool>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftLeaderElectionOptions {
    pub kv: ClusterKvRaftLeaderElectionOptions,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftLeaderElectionOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftLeaderElectionOptions {
        self.kv
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterRaftVoteTransportHost {
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;
}

pub trait ClusterRaftVoteTransportAsyncHost {
    fn send_raft_vote_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse>;
}

struct NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<ClusterEndpointRecord>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    fn new(
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<ClusterEndpointRecord>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionHost for NetworkClusterKvRaftLeaderElectionHost<'_, H>
where
    H: ClusterRaftVoteTransportHost,
{
    type Error = NetworkError;

    fn begin_election_attempt(&mut self) -> Result<(), Self::Error> {
        self.attempt_deadline = Instant::now() + self.options.timeout;
        Ok(())
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
        locked_cluster_raft_local_vote_metadata(self.paths, &self.state.local_node_id)
    }

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        locked_apply_cluster_raft_vote(self.paths, &self.state.local_node_id, vote)
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote(
        &mut self,
        target_index: usize,
        voter_id: &NodeId,
        vote: &RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        let endpoint =
            self.remote_vote_targets
                .get(target_index)
                .ok_or_else(|| NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!("missing vote endpoint for target index {target_index}"),
                })?;
        if &endpoint.node_id != voter_id {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: format!(
                    "vote endpoint target mismatch: endpoint is {}, election target is {}",
                    endpoint.node_id, voter_id
                ),
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: "election attempt timed out".to_string(),
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        self.transport
            .send_raft_vote(
                endpoint,
                &request,
                self.options.fair_command_timeout(endpoint, remaining),
            )
            .map(|response| response.vote)
    }

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_observe_cluster_raft_term(self.paths, &self.state.local_node_id, term).map(|_| ())
    }

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_set_cluster_raft_leader(self.paths, &self.state.local_node_id, term).map(|_| ())
    }
}

struct NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<ClusterEndpointRecord>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    fn new(
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<ClusterEndpointRecord>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            runtime,
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionAsyncHost
    for NetworkClusterKvRaftLeaderElectionAsyncHost<'_, H>
where
    H: ClusterRaftVoteTransportAsyncHost + Send,
{
    type Error = NetworkError;

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.attempt_deadline = Instant::now() + self.options.timeout;
            Ok(())
        })
    }

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_cluster_raft_local_vote_metadata(&paths, &node_id))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote metadata task failed: {source}"),
                })?
        })
    }

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_apply_cluster_raft_vote(&paths, &node_id, vote))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote apply task failed: {source}"),
                })?
        })
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote_async<'a>(
        &'a mut self,
        target_index: usize,
        voter_id: &'a NodeId,
        vote: &'a RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let endpoint = match self.remote_vote_targets.get(target_index).cloned() {
            Some(endpoint) => endpoint,
            None => {
                return Box::pin(async move {
                    Err(NetworkError::Io {
                        operation: "cluster raft vote".to_string(),
                        detail: format!("missing vote endpoint for target index {target_index}"),
                    })
                });
            }
        };
        if &endpoint.node_id != voter_id {
            let endpoint_node_id = endpoint.node_id.clone();
            let voter_id = voter_id.clone();
            return Box::pin(async move {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!(
                        "vote endpoint target mismatch: endpoint is {endpoint_node_id}, election target is {voter_id}"
                    ),
                })
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Box::pin(async {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: "election attempt timed out".to_string(),
                })
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        let timeout = self.options.fair_command_timeout(&endpoint, remaining);
        let transport = &mut *self.transport;
        Box::pin(async move {
            transport
                .send_raft_vote_async(&endpoint, &request, timeout)
                .await
                .map(|response| response.vote)
        })
    }

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_observe_cluster_raft_term(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("term observation task failed: {source}"),
                })?
                .map(|_| ())
        })
    }

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_set_cluster_raft_leader(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("leader write task failed: {source}"),
                })?
                .map(|_| ())
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnForwardOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
}

impl ClusterKvTxnForwardOptions {
    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }
}

pub type ClusterKvTxnForwardResult = sinomo_kv::ClusterKvTxnCommitResult;

pub trait ClusterKvTxnForwardHost {
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn relay_peers(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Vec<ClusterEndpointRecord>;

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub trait ClusterKvTxnForwardAsyncHost {
    fn send_cluster_kv_txn_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        txn: &'a KvTxn,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, KvTxnResponse>;

    fn fetch_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn relay_rpc_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        peer: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ObjectRpcResponse>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvSnapshotRefreshOptions {
    pub without_local_snapshot_timeout: Duration,
    pub with_local_snapshot_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterKvSnapshotRefreshOptions {
    fn refresh_budget(&self, has_local_snapshot: bool) -> Duration {
        if has_local_snapshot {
            self.with_local_snapshot_timeout
        } else {
            self.without_local_snapshot_timeout
        }
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterKvSnapshotRefreshHost {
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError>;

    /// Peers that can relay an RPC toward a voter when no voter is directly
    /// dialable (cross-segment learners reach voters only through bridges).
    /// Hosts without relay support keep the default and the refresh simply
    /// skips the relay fallback.
    fn relay_peers(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Vec<ClusterEndpointRecord> {
        Vec::new()
    }

    fn relay_rpc_to_node(
        &mut self,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _peer: &ClusterEndpointRecord,
        _request: &ObjectRpcRequest,
        _timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: "cluster KV snapshot relay is not supported by this host".to_string(),
        })
    }
}

struct LocalClusterKvSnapshotRefreshHost<F> {
    fetch_cluster_kv_snapshot: F,
}

impl<F> ClusterKvSnapshotRefreshHost for LocalClusterKvSnapshotRefreshHost<F>
where
    F: FnMut(&ClusterEndpointRecord, Duration) -> Result<KvSnapshot, NetworkError>,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        (self.fetch_cluster_kv_snapshot)(endpoint, timeout)
    }

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumOptions {
    pub allow_forwarding: bool,
    pub raft_append_timeout: Duration,
    pub raft_append_replication_options: ClusterRaftAppendReplicationOptions,
    pub forward_options: ClusterKvTxnForwardOptions,
    pub leader_election_options: ClusterRaftLeaderElectionOptions,
    pub voter_reachability_options: ClusterVoterReachabilityOptions,
}

pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT: Duration = Duration::from_millis(750);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT: Duration = Duration::from_secs(15);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(1);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(4);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_millis(1500);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterControlPlaneTimeouts {
    pub max_raft_append_attempts: usize,
    pub raft_append_timeout: Duration,
    pub rpc_timeout: Duration,
    pub membership_rpc_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterControlPlaneTimeouts {
    pub fn default_daemon_transport() -> Self {
        Self {
            max_raft_append_attempts: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS,
            raft_append_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
            rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT,
            membership_rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
            tcp_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT,
            iroh_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT,
            tcp_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
            public_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            iroh_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
        }
    }

    pub fn raft_append_replication_options(
        &self,
        timeout: Duration,
    ) -> ClusterRaftAppendReplicationOptions {
        ClusterRaftAppendReplicationOptions {
            max_attempts: self.max_raft_append_attempts,
            timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn committed_fanout_options(&self) -> ClusterRaftCommittedFanoutOptions {
        ClusterRaftCommittedFanoutOptions {
            raft_append_timeout: self.raft_append_timeout,
            watch_event_timeout: self.rpc_timeout,
        }
    }

    pub fn leader_election_options(&self) -> ClusterRaftLeaderElectionOptions {
        ClusterRaftLeaderElectionOptions {
            kv: ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn voter_reachability_options(&self) -> ClusterVoterReachabilityOptions {
        ClusterVoterReachabilityOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn kv_txn_forward_options(&self) -> ClusterKvTxnForwardOptions {
        ClusterKvTxnForwardOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
        }
    }

    pub fn kv_txn_quorum_options(&self, allow_forwarding: bool) -> ClusterKvTxnQuorumOptions {
        ClusterKvTxnQuorumOptions {
            allow_forwarding,
            raft_append_timeout: self.raft_append_timeout,
            raft_append_replication_options: self
                .raft_append_replication_options(self.raft_append_timeout),
            forward_options: self.kv_txn_forward_options(),
            leader_election_options: self.leader_election_options(),
            voter_reachability_options: self.voter_reachability_options(),
        }
    }

    pub fn kv_snapshot_refresh_options(&self) -> ClusterKvSnapshotRefreshOptions {
        ClusterKvSnapshotRefreshOptions {
            without_local_snapshot_timeout: self.raft_append_timeout,
            with_local_snapshot_timeout: self.rpc_timeout,
            tcp_command_timeout: self.rpc_timeout,
            iroh_command_timeout: self.rpc_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }
}

pub trait ClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: ClusterKvTxnForwardOptions,
    ) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError>;

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, NetworkError>;

    fn reachable_voter_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterVoterReachabilityOptions,
    ) -> usize;

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterRaftQuorumCommitOptions,
    ) -> Result<ClusterRaftQuorumCommitResult, NetworkError>;
}

pub trait ClusterKvTxnQuorumAsyncHost:
    ClusterKvTxnForwardAsyncHost
    + ClusterRaftQuorumAsyncHost
    + ClusterRaftVoteTransportAsyncHost
    + ClusterVoterReachabilityAsyncHost
    + Send
{
}

impl<T> ClusterKvTxnQuorumAsyncHost for T where
    T: ClusterKvTxnForwardAsyncHost
        + ClusterRaftQuorumAsyncHost
        + ClusterRaftVoteTransportAsyncHost
        + ClusterVoterReachabilityAsyncHost
        + Send
{
}

pub trait ClusterControlPlaneTransport {
    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;

    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool;

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError>;

    fn send_cluster_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn published_network_info_matches(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) -> bool {
        false
    }

    fn remember_published_network_info(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) {
    }

    fn published_node_status_matches(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) -> bool {
        false
    }

    fn remember_published_node_status(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) {
    }

    fn published_connectivity_record_set_matches(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) -> bool {
        false
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
    }

    fn on_raft_append_sent(&mut self) {}
}

pub struct ClusterControlPlaneDriver<T> {
    transport: T,
    timeouts: ClusterControlPlaneTimeouts,
}

impl<T> ClusterControlPlaneDriver<T> {
    pub fn new(transport: T) -> Self {
        Self::with_timeouts(
            transport,
            ClusterControlPlaneTimeouts::default_daemon_transport(),
        )
    }

    pub fn with_timeouts(transport: T, timeouts: ClusterControlPlaneTimeouts) -> Self {
        Self {
            transport,
            timeouts,
        }
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_transport(self) -> T {
        self.transport
    }
}

struct DriverRaftAppendReplicationHost<'a, T> {
    transport: &'a mut T,
}

impl<T> ClusterRaftAppendReplicationHost for DriverRaftAppendReplicationHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.transport.send_raft_append(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        self.transport.on_raft_append_sent();
    }
}

struct DriverRaftQuorumCommitHost<'a, T> {
    storage: LocalClusterRaftCommandStorage<'a>,
    driver: &'a mut ClusterControlPlaneDriver<T>,
}

impl<T> sinomo_kv::ClusterKvRaftCommandStorage for DriverRaftQuorumCommitHost<'_, T> {
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

impl<T> ClusterRaftQuorumTransportHost for DriverRaftQuorumCommitHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let mut host = DriverRaftAppendReplicationHost {
            transport: &mut self.driver.transport,
        };
        replicate_cluster_raft_to_endpoint_with_host(
            paths,
            state,
            endpoint,
            &[],
            self.driver
                .timeouts
                .raft_append_replication_options(timeout),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        self.driver.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        self.driver
            .transport
            .fan_out_committed_cluster_raft_command(paths, state, events, targets);
    }
}

impl<T> ClusterRaftCommittedFanoutAsyncHost for ClusterControlPlaneDriver<T>
where
    T: ClusterRaftCommittedFanoutAsyncHost,
{
    fn replicate_cluster_raft_to_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendReplicationOutcome> {
        self.transport.replicate_cluster_raft_to_endpoint_async(
            paths,
            state,
            endpoint,
            watch_events,
            timeout,
        )
    }

    fn send_cluster_kv_watch_events_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        self.transport
            .send_cluster_kv_watch_events_async(endpoint, request, timeout)
    }
}

impl<T> ClusterVoterReachabilityHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        self.transport.probe_voter(endpoint, timeout)
    }
}

impl<T> ClusterRaftVoteTransportHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError> {
        self.transport.send_raft_vote(endpoint, request, timeout)
    }
}

impl<T> ClusterKvSnapshotRefreshHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
    }

    fn relay_peers(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Vec<ClusterEndpointRecord> {
        cluster_member_publish_targets(paths, state)
    }

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .relay_rpc_to_node(state, target_node_id, peer, request, timeout)
    }
}

impl<T> ClusterKvTxnForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, NetworkError> {
        self.transport.send_cluster_kv_txn(endpoint, txn, timeout)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn relay_peers(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Vec<ClusterEndpointRecord> {
        cluster_member_publish_targets(paths, state)
    }

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .relay_rpc_to_node(state, target_node_id, peer, request, timeout)
    }
}

impl<T> ClusterKvTxnQuorumHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn refresh_current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
        .map(|_| ())
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        locked_cluster_raft_local_is_leader(paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: ClusterKvTxnForwardOptions,
    ) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError> {
        forward_cluster_kv_txn_to_voter_with_host(paths, state, txn, options, self)
    }

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, NetworkError> {
        elect_cluster_raft_leader_with_host(paths, state, options, self)
    }

    fn reachable_voter_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterVoterReachabilityOptions,
    ) -> usize {
        cluster_reachable_voter_count_with_host(paths, state, options, self)
    }

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterRaftQuorumCommitOptions,
    ) -> Result<ClusterRaftQuorumCommitResult, NetworkError> {
        let mut host = DriverRaftQuorumCommitHost {
            storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
            driver: self,
        };
        commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
    }
}

impl<T> ClusterMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterLocalMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool {
        let options = self.timeouts.voter_reachability_options();
        cluster_has_local_quorum_with_host(paths, state, options, self)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError> {
        self.transport
            .send_cluster_member_publish(endpoint, request, timeout)
    }
}

impl<T> ClusterInvitePublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNetworkRecordExpiryHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNetworkInfoPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_network_info_matches(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) -> bool {
        self.transport.published_network_info_matches(paths, info)
    }

    fn remember_published_network_info(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) {
        self.transport.remember_published_network_info(paths, info);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNodeStatusPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_node_status_matches(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) -> bool {
        self.transport.published_node_status_matches(paths, status)
    }

    fn remember_published_node_status(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) {
        self.transport.remember_published_node_status(paths, status);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterConnectivityPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_connectivity_record_set_matches(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) -> bool {
        self.transport
            .published_connectivity_record_set_matches(paths, source_node_id, records)
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) {
        self.transport
            .remember_published_connectivity_record_set(paths, source_node_id, records);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMemberMutationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMembershipRpcForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport.send_cluster_membership_rpc(
            endpoint,
            request,
            self.timeouts.membership_rpc_timeout,
        )
    }

    fn relay_membership_rpc_to_voter_via_peer(
        &mut self,
        state: &ClusterBootstrapState,
        voter: &ClusterEndpointRecord,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport.relay_rpc_to_node(
            state,
            &voter.node_id,
            peer,
            request,
            self.timeouts.membership_rpc_timeout,
        )
    }
}

impl<T> ClusterJoinRegistrationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError> {
        sync_cluster_state_to_kv_with_host(paths, state, self)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        self.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            ClusterConnectivityState::Healthy,
            None,
        );
        Ok(())
    }
}

impl<T> ClusterJoinReconciliationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
        let timeout = match endpoint.transport {
            ClusterTransportKind::Iroh => iroh_timeout,
            _ => tcp_timeout,
        };
        send_cluster_join_registration_over_membership_rpc(
            endpoint,
            request,
            timeout,
            |endpoint, request, timeout| {
                self.transport
                    .send_cluster_membership_rpc(endpoint, request, timeout)
            },
        )
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn observe_join_reconciliation_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        if let Some(endpoint) = reconciliation.accepted_endpoint.as_ref() {
            self.transport.record_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                ClusterConnectivityState::Healthy,
                None,
            );
        }
        Ok(())
    }
}

pub trait ClusterMembershipRpcForwardHost {
    fn send_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn relay_membership_rpc_to_voter_via_peer(
        &mut self,
        state: &ClusterBootstrapState,
        voter: &ClusterEndpointRecord,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub type ClusterMembershipRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub trait ClusterMembershipRpcAsyncForwardHost {
    fn send_membership_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
    ) -> ClusterMembershipRpcFuture<'a, ObjectRpcResponse>;

    fn relay_membership_rpc_to_voter_via_peer_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        voter: &'a ClusterEndpointRecord,
        peer: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
    ) -> ClusterMembershipRpcFuture<'a, ObjectRpcResponse>;
}

pub trait ClusterJoinRegistrationHost: ClusterMembershipRpcForwardHost {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError>;

    fn remember_successful_join_targets(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        members: &[ClusterMemberRecord],
    ) -> Result<(), NetworkError> {
        for member in members {
            if member.node_id == state.local_node_id {
                continue;
            }
            for endpoint in &member.endpoints {
                if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                    continue;
                }
                self.remember_successful_join_endpoint(paths, state, endpoint)?;
            }
        }
        Ok(())
    }

    fn remember_successful_join_endpoint(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub trait ClusterJoinRegistrationAsyncHost: ClusterMembershipRpcAsyncForwardHost {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }

    fn commit_join_registration_state_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn remember_successful_join_targets_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()>
    where
        Self: Send,
    {
        Box::pin(async move {
            for member in members {
                if member.node_id == state.local_node_id {
                    continue;
                }
                for endpoint in &member.endpoints {
                    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                        continue;
                    }
                    self.remember_successful_join_endpoint_async(paths, state, endpoint)
                        .await?;
                }
            }
            Ok(())
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterLocalDaemonMembershipObserver {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }
}

pub trait ClusterLocalDaemonMembershipAsyncObserver {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoopClusterLocalDaemonMembershipObserver;

impl ClusterLocalDaemonMembershipObserver for NoopClusterLocalDaemonMembershipObserver {}
impl ClusterLocalDaemonMembershipAsyncObserver for NoopClusterLocalDaemonMembershipObserver {}

pub struct ClusterJoinRegistrationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterJoinRegistrationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMembershipRpcForwardHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterMembershipRpcForwardHost::send_membership_rpc(&mut self.driver, endpoint, request)
    }

    fn relay_membership_rpc_to_voter_via_peer(
        &mut self,
        state: &ClusterBootstrapState,
        voter: &ClusterEndpointRecord,
        peer: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterMembershipRpcForwardHost::relay_membership_rpc_to_voter_via_peer(
            &mut self.driver,
            state,
            voter,
            peer,
            request,
        )
    }
}

impl<T, O> ClusterJoinRegistrationHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        self.observer
            .local_daemon_membership_observation(paths, state)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterJoinRegistrationHost::commit_join_registration_state(&mut self.driver, paths, state)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        ClusterJoinRegistrationHost::remember_successful_join_endpoint(
            &mut self.driver,
            paths,
            state,
            endpoint,
        )
    }
}

pub struct ClusterMemberMutationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterMemberMutationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMemberMutationHost for ClusterMemberMutationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        if let Some(observation) = self
            .observer
            .local_daemon_membership_observation(paths, state)?
        {
            refresh_local_daemon_membership_from_disk(paths, state, observation)?;
        }
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        ClusterMemberMutationHost::current_cluster_kv_store(&mut self.driver, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.driver,
            paths,
            state,
            txn,
        )
    }
}

pub trait ClusterJoinReconciliationHost {
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn observe_join_reconciliation_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub trait ClusterNetworkRecordExpiryHost {
    type Error: From<NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterNetworkInfoPublishHost {
    type Error: From<NetworkError>;

    fn published_network_info_matches(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) -> bool {
        false
    }

    fn remember_published_network_info(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterNetworkInfoPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_network_info_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _info: &'a ClusterNetworkInfoRecord,
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_network_info_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _info: &'a ClusterNetworkInfoRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub trait ClusterNodeStatusPublishHost {
    type Error: From<NetworkError>;

    fn published_node_status_matches(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) -> bool {
        false
    }

    fn remember_published_node_status(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterNodeStatusPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_node_status_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _status: &'a ClusterNodeStatusRecord,
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_node_status_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _status: &'a ClusterNodeStatusRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub trait ClusterMemberPublishHost {
    type Error: From<NetworkError> + ToString;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterConnectivityPublishHost {
    type Error: From<NetworkError>;

    fn published_connectivity_record_set_matches(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) -> bool {
        false
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterConnectivityPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_connectivity_record_set_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _source_node_id: &'a NodeId,
        _records: &'a [ClusterConnectivityRecord],
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_connectivity_record_set_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _source_node_id: &'a NodeId,
        _records: &'a [ClusterConnectivityRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub struct ClusterLocalMemberPublishOptions {
    pub member_publish_timeout: Duration,
    /// Publish even when the local KV snapshot already reflects the local
    /// member records. Learners set this on their periodic publish so the
    /// voter can hand back a newer KV snapshot (see
    /// `ClusterMemberPublishRequest::local_kv_revision`); without it a learner
    /// whose own records are stable never publishes again and its local KV
    /// snapshot never converges.
    pub force_kv_snapshot_sync: bool,
}

pub trait ClusterLocalMemberPublishHost {
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError>;
}

pub trait ClusterLocalMemberPublishAsyncHost {
    fn cluster_has_local_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, bool>;

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn send_cluster_member_publish_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterMemberPublishResponse>;
}

pub trait ClusterInvitePublishHost {
    type Error: From<NetworkError> + ToString;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub fn handle_cluster_raft_rpc_request(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let route = request.route.clone();
    let Some(payload) = sinomo_kv::handle_cluster_raft_rpc_payload(
        request.route.as_str(),
        &request.body,
        |append| handle_locked_cluster_raft_append_request(paths, append, authenticated_peer),
        |vote| handle_locked_cluster_raft_vote_request(paths, vote, authenticated_peer),
    )?
    else {
        return Err(NetworkError::UnknownRoute { route });
    };
    Ok(ObjectRpcResponse::from_encoded_body(
        payload.route,
        payload.body,
    ))
}

pub async fn handle_cluster_raft_rpc_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    runtime
        .spawn_blocking(move || {
            handle_cluster_raft_rpc_request(&paths, request, &authenticated_peer)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft rpc".to_string(),
            detail: format!("cluster raft rpc task failed: {source}"),
        })?
}

pub fn handle_cluster_membership_rpc_request_with_hosts<E, J, M, I>(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    join_host: &mut J,
    member_host: &mut M,
    invite_host: &mut I,
) -> Result<ObjectRpcResponse, E>
where
    E: From<NetworkError> + ToString,
    J: ClusterJoinRegistrationHost,
    M: ClusterMemberPublishHost<Error = E>,
    I: ClusterInvitePublishHost<Error = E>,
{
    match request.route.as_str() {
        CLUSTER_JOIN_REGISTER_ROUTE => {
            let payload: ClusterJoinRegistrationRequest = request.decode_body().map_err(E::from)?;
            let response =
                handle_cluster_join_registration_request_with_host(paths, payload, now, join_host)
                    .map_err(E::from)?;
            ObjectRpcResponse::encode_body(CLUSTER_JOIN_REGISTER_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_INVITE_PUBLISH_ROUTE => {
            let payload: ClusterInvitePublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_invite_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                signer_public_key,
                now,
                invite_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_INVITE_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_MEMBER_PUBLISH_ROUTE => {
            let payload: ClusterMemberPublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_member_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                member_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_MEMBER_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        other => Err(E::from(NetworkError::UnknownRoute {
            route: other.to_string(),
        })),
    }
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

pub trait ClusterConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KvBackedClusterConnectivityRecordQuery;

impl ClusterConnectivityRecordQuery for KvBackedClusterConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        read_cluster_connectivity_graph_from_kv(paths)
    }
}

pub fn cluster_local_node_role(state: &ClusterBootstrapState) -> ClusterNodeRole {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .map(|member| member.node_role.clone())
        .unwrap_or_default()
}

pub fn cluster_network_probe_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_PROBE_INTERVAL
        }
    }
}

pub fn cluster_network_publish_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_PUBLISH_INTERVAL
        }
    }
}

pub fn cluster_network_publish_interval_for_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Duration {
    read_cluster_state_without_kv(paths)
        .ok()
        .and_then(|state| {
            state
                .members
                .iter()
                .find(|member| member.node_id == *node_id)
                .map(|member| cluster_network_publish_interval_for_role(&member.node_role))
        })
        .unwrap_or(CLUSTER_NETWORK_PUBLISH_INTERVAL)
}

pub fn cluster_iroh_proxy_republish_sleep_duration(
    now: Instant,
    next_republish: Instant,
) -> Duration {
    let remaining = next_republish.saturating_duration_since(now);
    if remaining.is_zero() {
        Duration::from_millis(1)
    } else {
        remaining
    }
}

pub fn cluster_iroh_proxy_start_retry_sleep_duration(
    consecutive_failures: u32,
    max_delay: Duration,
) -> Duration {
    let min_delay = Duration::from_secs(5);
    let max_delay = max_delay.max(min_delay);
    let shift = consecutive_failures.min(6);
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(64);
    min_delay.saturating_mul(multiplier).min(max_delay)
}

pub fn cluster_network_discovery_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_DISCOVERY_INTERVAL
        }
    }
}

pub fn cluster_connectivity_probe_max_endpoints_per_tick_for_role(role: &ClusterNodeRole) -> usize {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK,
        ClusterNodeRole::Sensor => CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK
        }
    }
}

pub fn cluster_network_stable_discovery_announcement_due(tick: u64, state_changed: bool) -> bool {
    state_changed
        || tick
            .saturating_sub(1)
            .is_multiple_of(CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE)
}

pub fn cluster_network_maintenance_interval_due(
    now: Instant,
    last: Instant,
    interval: Duration,
) -> bool {
    now.saturating_duration_since(last) >= interval
}

pub fn cluster_network_maintenance_interval_remaining(
    now: Instant,
    last: Instant,
    interval: Duration,
) -> Duration {
    interval.saturating_sub(now.saturating_duration_since(last))
}

#[derive(Clone, Debug)]
pub struct ClusterNetworkMaintenanceSchedule {
    pub local_node_role: ClusterNodeRole,
    pub has_discovery: bool,
    pub connectivity_converged: bool,
    pub startup_settle_until: Option<Instant>,
    pub last_publish: Instant,
    pub last_probe: Instant,
    pub last_discovery: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterNetworkMaintenanceDue {
    pub convergence_due: bool,
    pub publish_due: bool,
    pub probe_due: bool,
    pub discovery_due: bool,
}

pub fn cluster_network_maintenance_due(
    now: Instant,
    schedule: &ClusterNetworkMaintenanceSchedule,
) -> ClusterNetworkMaintenanceDue {
    let startup_settle_due = schedule
        .startup_settle_until
        .is_some_and(|deadline| now < deadline);
    let convergence_due = !schedule.connectivity_converged || startup_settle_due;
    let publish_due = convergence_due
        || cluster_network_maintenance_interval_due(
            now,
            schedule.last_publish,
            cluster_network_publish_interval_for_role(&schedule.local_node_role),
        );
    let probe_due = convergence_due
        || cluster_network_maintenance_interval_due(
            now,
            schedule.last_probe,
            cluster_network_probe_interval_for_role(&schedule.local_node_role),
        );
    let discovery_due = schedule.has_discovery
        && (convergence_due
            || cluster_network_maintenance_interval_due(
                now,
                schedule.last_discovery,
                cluster_network_discovery_interval_for_role(&schedule.local_node_role),
            ));

    ClusterNetworkMaintenanceDue {
        convergence_due,
        publish_due,
        probe_due,
        discovery_due,
    }
}

pub fn cluster_network_maintenance_next_due_duration(
    now: Instant,
    schedule: &ClusterNetworkMaintenanceSchedule,
) -> Duration {
    let mut next_due = cluster_network_maintenance_interval_remaining(
        now,
        schedule.last_publish,
        cluster_network_publish_interval_for_role(&schedule.local_node_role),
    )
    .min(cluster_network_maintenance_interval_remaining(
        now,
        schedule.last_probe,
        cluster_network_probe_interval_for_role(&schedule.local_node_role),
    ));
    if schedule.has_discovery {
        next_due = next_due.min(cluster_network_maintenance_interval_remaining(
            now,
            schedule.last_discovery,
            cluster_network_discovery_interval_for_role(&schedule.local_node_role),
        ));
    }
    if schedule
        .startup_settle_until
        .is_some_and(|deadline| now < deadline)
    {
        next_due = next_due.min(CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL);
    }
    if !schedule.connectivity_converged {
        next_due = next_due.min(CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL);
    }
    next_due
}

pub fn cluster_network_maintenance_last_timestamp_for_next_due(
    now: Instant,
    interval: Duration,
    delay: Duration,
) -> Instant {
    now.checked_add(delay.saturating_sub(interval))
        .unwrap_or(now)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeState {
    pub record: ClusterConnectivityRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeBatchEntry {
    pub key: String,
    pub endpoint: ClusterEndpointRecord,
    pub previous: Option<ClusterConnectivityRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeBatch {
    pub active_keys: BTreeSet<String>,
    pub entries: Vec<ClusterConnectivityProbeBatchEntry>,
    pub next_cursor: usize,
}

pub fn select_cluster_connectivity_probe_batch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    connectivity_cursor: usize,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    now: OffsetDateTime,
) -> ClusterConnectivityProbeBatch {
    let refresh_deadline =
        now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
    let targets = cluster_network_probe_targets(state);
    let active_keys = targets
        .iter()
        .map(|endpoint| cluster_connectivity_probe_key(&state.local_node_id, endpoint))
        .collect::<BTreeSet<_>>();

    // Seed the in-memory probe cache from the merged graph (KV + the local probe store,
    // local-authoritative for our own edges). A learner cannot commit its connectivity
    // edges to KV, so seeding from KV alone would discard its last-known reachability.
    if connectivity.is_empty() {
        for record in read_cluster_connectivity_graph(paths)
            .unwrap_or_default()
            .into_iter()
            .filter(|record| record.source_node_id == state.local_node_id)
        {
            let key = cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            );
            if active_keys.contains(&key) {
                connectivity.insert(key, ClusterConnectivityProbeState { record });
            }
        }
    }

    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let probe_targets = targets
        .iter()
        .filter(|endpoint| {
            let key = cluster_connectivity_probe_key(&state.local_node_id, endpoint);
            connectivity.get(&key).is_none_or(|probe| {
                cluster_connectivity_probe_due_with_graph(
                    &probe.record,
                    endpoint,
                    now,
                    refresh_deadline,
                    local_node_role,
                    &graph,
                    &state.local_node_id,
                    startup_settle_active,
                )
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let probe_count =
        probe_targets
            .len()
            .min(cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                local_node_role,
            ));
    let probe_start = if probe_targets.is_empty() {
        0
    } else {
        connectivity_cursor % probe_targets.len()
    };
    let next_cursor = if probe_targets.is_empty() {
        0
    } else {
        (probe_start + probe_count) % probe_targets.len()
    };
    let entries = probe_targets
        .iter()
        .cycle()
        .skip(probe_start)
        .take(probe_count)
        .map(|endpoint| {
            let key = cluster_connectivity_probe_key(&state.local_node_id, endpoint);
            let previous = connectivity.get(&key).map(|probe| probe.record.clone());
            ClusterConnectivityProbeBatchEntry {
                key,
                endpoint: endpoint.clone(),
                previous,
            }
        })
        .collect();

    ClusterConnectivityProbeBatch {
        active_keys,
        entries,
        next_cursor,
    }
}

pub fn cluster_connectivity_record_for_probe_outcome(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    previous: Option<&ClusterConnectivityRecord>,
    healthy: bool,
    latency_ms: u64,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    if healthy {
        return cluster_connectivity_record(
            state,
            endpoint,
            ClusterConnectivityState::Healthy,
            Some(latency_ms),
            0,
            Some(timestamp.to_string()),
            previous.and_then(|record| record.last_failure_at.clone()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
    }

    let observed_failures = previous
        .map(|record| record.consecutive_failures.saturating_add(1))
        .unwrap_or(1);
    let retained_success = previous.and_then(|record| {
        if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            record.last_success_at.clone()
        } else {
            None
        }
    });
    let previously_healthy =
        previous.is_some_and(|record| record.state == ClusterConnectivityState::Healthy);
    let state_label = if previously_healthy && observed_failures == 1 {
        // Single-failure grace: a confirmed-healthy link survives one transient
        // probe failure as Healthy. A persistent outage still degrades on the next
        // probe, while a never-confirmed link gets no grace.
        ClusterConnectivityState::Healthy
    } else if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
        ClusterConnectivityState::Unreachable
    } else {
        ClusterConnectivityState::Degraded
    };
    let failures = observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
    let ttl_seconds = if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS
    } else {
        CLUSTER_CONNECTIVITY_TTL_SECONDS
    };
    cluster_connectivity_record(
        state,
        endpoint,
        state_label,
        previous.and_then(|record| record.latency_ms),
        failures,
        retained_success,
        Some(timestamp.to_string()),
        now,
        ttl_seconds,
    )
}

pub fn apply_cluster_connectivity_probe_results(
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    active_keys: &BTreeSet<String>,
    probed: Vec<(String, ClusterConnectivityRecord)>,
) -> Vec<ClusterConnectivityRecord> {
    for (key, record) in probed {
        connectivity.insert(key, ClusterConnectivityProbeState { record });
    }

    connectivity.retain(|key, _state| active_keys.contains(key));
    connectivity
        .values()
        .map(|state| state.record.clone())
        .collect()
}

pub fn cluster_connectivity_probe_due_with_startup_settle(
    record: &ClusterConnectivityRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
) -> bool {
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
    let retry_interval = cluster_network_probe_interval_for_role(local_node_role);
    let retry_seconds = retry_interval.as_secs().min(i64::MAX as u64) as i64;
    let retry_due = record.observed_at + time::Duration::seconds(retry_seconds) <= now;
    if record.state == ClusterConnectivityState::Unreachable {
        return retry_due;
    }
    if record.state != ClusterConnectivityState::Healthy
        && record.consecutive_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
    {
        return retry_due;
    }
    record.state == ClusterConnectivityState::Healthy
        && record.consecutive_failures > 0
        && retry_due
}

pub fn cluster_connectivity_probe_due_with_graph(
    record: &ClusterConnectivityRecord,
    endpoint: &ClusterEndpointRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    graph: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    startup_settle_active: bool,
) -> bool {
    if !cluster_connectivity_probe_due_with_startup_settle(
        record,
        now,
        refresh_deadline,
        local_node_role,
        startup_settle_active,
    ) {
        return false;
    }
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
    if record.state == ClusterConnectivityState::Healthy {
        return true;
    }
    cluster_route_next_hop(graph, local_node_id, &endpoint.node_id).is_none()
}

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
        Self::read_for_local_with_query(paths, state, &KvBackedClusterConnectivityRecordQuery)
    }

    pub fn read_for_local_with_query<Q>(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        query: &Q,
    ) -> Self
    where
        Q: ClusterConnectivityRecordQuery + ?Sized,
    {
        Self::from_records(
            state.local_node_id.clone(),
            read_cluster_connectivity_graph_with_suppressed_local_source_and_query(
                paths,
                Some(&state.local_node_id),
                query,
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

pub fn refresh_cluster_connectivity_environment(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    environment_id: &mut Option<String>,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    observed_at: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let local_environment_id = build_cluster_network_info_record(
        state,
        &state.local_node_id,
        observed_at,
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    )
    .environment_id;
    let changed = environment_id
        .as_ref()
        .is_some_and(|known| known != &local_environment_id);
    if changed {
        connectivity.clear();
        write_local_cluster_connectivity_record_set(paths, &state.local_node_id, &[])?;
    }
    *environment_id = Some(local_environment_id);
    Ok(changed)
}

pub async fn refresh_cluster_connectivity_environment_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    environment_id: &mut Option<String>,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    observed_at: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let local_environment_id = build_cluster_network_info_record(
        state,
        &state.local_node_id,
        observed_at,
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    )
    .environment_id;
    let changed = environment_id
        .as_ref()
        .is_some_and(|known| known != &local_environment_id);
    if changed {
        connectivity.clear();
        write_local_cluster_connectivity_record_set_async(
            runtime,
            paths,
            &state.local_node_id,
            &[],
        )
        .await?;
    }
    *environment_id = Some(local_environment_id);
    Ok(changed)
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

pub(crate) fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: OffsetDateTime,
) {
    sinomo_node::apply_cluster_raft_role(member, role, timestamp);
}

pub(crate) fn cluster_raft_state_for_role(role: &ClusterMemberRole) -> ClusterMemberState {
    match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    }
}

pub fn prepare_pending_cluster_join_state(
    cluster_id: ClusterId,
    cluster_name: String,
    local_node_id: NodeId,
    local_node_display_name: String,
    main_public_key: String,
    shadow_public_key: String,
    timestamp: OffsetDateTime,
) -> ClusterBootstrapState {
    let mut state = ClusterBootstrapState::pending_join(
        cluster_id,
        cluster_name,
        local_node_id,
        local_node_display_name,
        main_public_key.clone(),
        timestamp,
    );
    ensure_daemon_cluster_members(&mut state, Vec::new(), Vec::new(), timestamp);
    assign_local_daemon_member_public_keys(&mut state, main_public_key, shadow_public_key);
    state
}

pub fn prepare_cluster_join_retry_state(
    mut state: ClusterBootstrapState,
    main_public_key: String,
    shadow_public_key: String,
    timestamp: OffsetDateTime,
) -> ClusterBootstrapState {
    ensure_daemon_cluster_members(&mut state, Vec::new(), Vec::new(), timestamp);
    assign_local_daemon_member_public_keys(&mut state, main_public_key, shadow_public_key);
    state
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterLocalDaemonMembershipObservation {
    pub main_endpoints: Vec<ClusterEndpointRecord>,
    pub shadow_endpoints: Vec<ClusterEndpointRecord>,
    pub main_public_key: String,
    pub shadow_public_key: String,
    pub timestamp: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterLocalDaemonMembershipRefresh {
    pub members_changed: bool,
    pub public_keys_changed: bool,
    pub iroh_endpoints_changed: bool,
}

impl ClusterLocalDaemonMembershipRefresh {
    pub fn changed(&self) -> bool {
        self.members_changed || self.public_keys_changed || self.iroh_endpoints_changed
    }
}

pub fn prepare_cluster_join_registration_state_with_local_daemon_members(
    mut state: ClusterBootstrapState,
    main_endpoints: Vec<ClusterEndpointRecord>,
    shadow_endpoints: Vec<ClusterEndpointRecord>,
    main_public_key: String,
    shadow_public_key: String,
    timestamp: OffsetDateTime,
) -> ClusterBootstrapState {
    apply_local_daemon_membership_observation(
        &mut state,
        ClusterLocalDaemonMembershipObservation {
            main_endpoints,
            shadow_endpoints,
            main_public_key,
            shadow_public_key,
            timestamp,
        },
    );
    state
}

pub fn apply_local_daemon_membership_observation(
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> ClusterLocalDaemonMembershipRefresh {
    let members_changed = ensure_daemon_cluster_members(
        state,
        observation.main_endpoints,
        observation.shadow_endpoints,
        observation.timestamp,
    );
    let public_keys_changed = assign_local_daemon_member_public_keys(
        state,
        observation.main_public_key,
        observation.shadow_public_key,
    );
    ClusterLocalDaemonMembershipRefresh {
        members_changed,
        public_keys_changed,
        iroh_endpoints_changed: false,
    }
}

pub fn refresh_local_daemon_membership_from_disk(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> Result<ClusterLocalDaemonMembershipRefresh, NetworkError> {
    let timestamp = observation.timestamp;
    let mut refresh = apply_local_daemon_membership_observation(state, observation);
    refresh.iroh_endpoints_changed =
        refresh_local_iroh_endpoints_from_disk(paths, state, timestamp)?;
    Ok(refresh)
}

pub async fn refresh_local_daemon_membership_from_disk_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> Result<ClusterLocalDaemonMembershipRefresh, NetworkError> {
    let timestamp = observation.timestamp;
    let mut refresh = apply_local_daemon_membership_observation(state, observation);
    refresh.iroh_endpoints_changed =
        refresh_local_iroh_endpoints_from_disk_async(runtime, paths, state, timestamp).await?;
    Ok(refresh)
}

pub fn assign_local_daemon_member_public_keys(
    state: &mut ClusterBootstrapState,
    main_public_key: String,
    shadow_public_key: String,
) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let mut changed = false;
    for member in &mut state.members {
        let desired = if member.node_id == state.local_node_id {
            Some(&main_public_key)
        } else if member.node_id == shadow_node_id {
            Some(&shadow_public_key)
        } else {
            None
        };
        if let Some(desired) = desired
            && member.node_public_key != *desired
        {
            member.node_public_key = desired.clone();
            changed = true;
        }
    }
    changed
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
    } else if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        "voter"
    } else {
        "read-only"
    }
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

pub async fn read_cluster_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || read_cluster_state(&paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })?
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

pub async fn write_cluster_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || write_cluster_state(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        })?
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
        cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

pub fn cluster_kv_key_can_defer_materialized_snapshot(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
        || key.starts_with("/cluster/network/info/")
        || key.starts_with("/cluster/node-status/")
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

fn read_cluster_kv_current_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_current_snapshot_read_for_test(paths);

    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&paths.kv_paths(), node_id)
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
    paths.kv_paths().local_cache()
}

fn cluster_kv_raft_store(paths: &ClusterPaths) -> sinomo_kv::ClusterKvRaftStore {
    paths.kv_paths().raft_store()
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

#[cfg(test)]
pub(crate) fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    cluster_kv_raft_store(paths).wal_paths(node_id)
}

pub(crate) fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    cluster_kv_raft_store(paths)
        .open_node(node_id)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

pub(crate) fn locked_restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

#[cfg(test)]
pub(crate) async fn open_cluster_raft_kv_node_async(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    cluster_kv_raft_store(paths)
        .open_node_async(node_id)
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

pub(crate) fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let store = cluster_kv_raft_store(paths);
    let (result, wrote) = store
        .append_command_with_response_and_publish_report(
            node_id,
            command,
            cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(NetworkError::from)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok((result.snapshot, result.txn_response))
}

#[cfg(test)]
pub(crate) fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    append_cluster_raft_command_uncommitted_entry(paths, node_id, command)
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

#[cfg(test)]
fn append_cluster_raft_command_uncommitted_entry(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    cluster_kv_raft_store(paths)
        .append_command_uncommitted(node_id, command)
        .map_err(Into::into)
}

fn cluster_member_put_op(member: &ClusterMemberRecord) -> Result<KvOp, NetworkError> {
    KvOp::put_serde(cluster_member_key(&member.node_id), member, None).map_err(NetworkError::from)
}

pub fn cluster_member_put_command(member: &ClusterMemberRecord) -> Result<KvCommand, NetworkError> {
    cluster_member_put_op(member).map(KvCommand::Op)
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
    let (outcome, wrote) = cluster_kv_raft_store(paths)
        .apply_append_entries_with_optional_snapshot_and_publish(
            node_id,
            snapshot,
            append,
            cluster_kv_key_can_defer_materialized_snapshot,
        )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(outcome.response)
}

pub(crate) fn locked_apply_cluster_raft_vote(
    paths: &ClusterPaths,
    node_id: &NodeId,
    vote: RaftRequestVoteRequest,
) -> Result<RaftRequestVoteResponse, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_apply_vote(node_id, vote)
        .map_err(Into::into)
}

pub(crate) fn handle_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
        );
    }
    let leader_authorized_by_local_state =
        sinomo_kv::cluster_kv_raft_node_is_voter(&state, &request.append.leader_id)
            && cluster_authenticated_peer_matches_node(
                &state,
                authenticated_peer,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join = cluster_state_needs_join_registration(&state)
        && request.append.leader_id == request.state.local_node_id
        && sinomo_kv::cluster_kv_raft_node_is_voter(&request.state, &request.append.leader_id)
        && cluster_authenticated_peer_matches_node(
            &request.state,
            authenticated_peer,
            &request.append.leader_id,
        );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
        );
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    let append = apply_cluster_raft_append(
        paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub(crate) fn handle_locked_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation()?;
    handle_cluster_raft_append_request(paths, request, authenticated_peer)
}

fn cluster_raft_append_rejection(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    detail: String,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    Ok(ClusterRaftAppendResponse {
        accepted: false,
        detail,
        state: state.clone(),
        append: cluster_kv_raft_store(paths)
            .append_entries_rejection_response(&state.local_node_id)?,
    })
}

pub(crate) fn handle_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "peer cluster `{}` does not match local cluster `{}`",
                    request.state.cluster_id, state.cluster_id
                ),
            )
            .map_err(Into::into);
    }
    let candidate_is_authorized_voter = sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(
        &state,
        &request.vote.candidate_id,
    );
    if !candidate_is_authorized_voter
        || !cluster_authenticated_peer_matches_node(
            &state,
            authenticated_peer,
            &request.vote.candidate_id,
        )
    {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "raft vote rejected for unauthorized or mismatched candidate {}",
                    request.vote.candidate_id
                ),
            )
            .map_err(Into::into);
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    cluster_kv_raft_store(paths)
        .apply_vote_response(&state, request.vote)
        .map_err(Into::into)
}

pub(crate) fn handle_locked_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation()?;
    handle_cluster_raft_vote_request(paths, request, authenticated_peer)
}

pub fn cluster_authenticated_peer_matches_node(
    state: &ClusterBootstrapState,
    authenticated_peer: &ClusterAuthenticatedPeer,
    node_id: &NodeId,
) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated_peer.node_id
            && member.node_public_key == authenticated_peer.public_key
    })
}

pub(crate) fn locked_observe_cluster_raft_term(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_observe_term_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

pub(crate) fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_set_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_local_vote_metadata(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<ClusterKvRaftLocalVoteMetadata, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_local_vote_metadata(node_id)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_known_voter_leader_id(state)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_local_is_leader(state)
        .map_err(Into::into)
}

pub fn cluster_raft_vote_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_sync_targets_with_connectivity(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub fn cluster_join_voter_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    // Join bootstrap must not depend on this node's current health view of the voter.
    // Bridges often need to ask a voter for invite proofs before their connectivity graph
    // and status probes have converged, so try the ordered voter command targets directly.
    cluster_voter_command_targets(paths, state)
}

#[cfg(test)]
pub fn create_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::create_cluster_kv_watch_for_node(&paths.kv_paths(), node_id, request)
        .map_err(Into::into)
}

#[cfg(test)]
pub fn drain_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::drain_cluster_kv_watch_for_node(&paths.kv_paths(), node_id, request)
        .map_err(Into::into)
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
        update_cluster_kv_watch_registration_after_drain(&watch_path, &registration_read, &report)?;
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

pub fn handle_cluster_kv_watch_events(
    paths: &ClusterPaths,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    sinomo_kv::handle_cluster_kv_watch_events(&paths.kv_paths(), &state, request)
        .map_err(Into::into)
}

#[cfg(test)]
pub fn cancel_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, NetworkError> {
    sinomo_kv::cancel_cluster_kv_watch(&paths.kv_paths(), request).map_err(Into::into)
}

#[cfg(test)]
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
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::append_cluster_kv_watch_events_for_active_watches_for_node(
        &paths.kv_paths(),
        node_id,
        incoming,
    )
    .map_err(Into::into)
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
fn observe_cluster_kv_watch_event_log_read(_paths: &ClusterPaths, read: u64) {
    #[cfg(test)]
    record_cluster_kv_watch_event_log_read_for_test(_paths, read);
    if let Some(observer) = io_observer() {
        observer.on_disk_read(read);
    }
}

#[cfg(test)]
fn observe_cluster_kv_watch_event_log_write(_paths: &ClusterPaths, written: u64) {
    #[cfg(test)]
    record_cluster_kv_watch_event_log_write_for_test(_paths, written);
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
    #[cfg(test)]
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

pub fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    read_optional_object_file(&paths.state)?.ok_or(NetworkError::ClusterOrphan)
}

pub async fn read_cluster_state_without_kv_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || read_cluster_state_without_kv(&paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read local cluster state".to_string(),
            detail: format!("local cluster state read task failed: {source}"),
        })?
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
    read_cluster_connectivity_graph_with_query(paths, &KvBackedClusterConnectivityRecordQuery)
}

pub fn read_cluster_connectivity_graph_with_query<Q>(
    paths: &ClusterPaths,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    read_cluster_connectivity_graph_with_suppressed_local_source_and_query(paths, None, query)
}

fn read_cluster_connectivity_graph_with_suppressed_local_source_and_query<Q>(
    paths: &ClusterPaths,
    local_source_node_id: Option<&NodeId>,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
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
    for record in query.read_kv_connectivity_records(paths)? {
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
    let store = InProcessKvStore::restore(initial_snapshot.clone());
    let txn = cluster_state_sync_txn_from_store(&store, state, false)?;
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

pub fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, NetworkError> {
    cluster_state_sync_txn_with_current_snapshot(paths, state).map(|(_, txn)| txn)
}

pub fn sync_cluster_state_to_kv_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync(paths, &mut keyed_state)?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let (current_snapshot, txn) =
        cluster_state_sync_txn_with_current_snapshot(paths, &keyed_state)?;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

pub async fn sync_cluster_state_to_kv_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationAsyncHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync_async(paths, &mut keyed_state)
        .await?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let paths_for_txn = paths.clone();
    let keyed_state_for_txn = keyed_state.clone();
    let (current_snapshot, txn) = runtime
        .spawn_blocking(move || {
            cluster_state_sync_txn_with_current_snapshot(&paths_for_txn, &keyed_state_for_txn)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster state sync txn".to_string(),
            detail: format!("cluster state sync txn task failed: {source}"),
        })??;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
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
    let new_config = ClusterKvRaftConfigRecord::from(&state);
    let current_config_entry = store.get(&sinomo_kv::cluster_kv_raft_config_key()).cloned();
    let mut compares = Vec::new();
    if let Some(current_config_entry) = &current_config_entry {
        let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
        if validate_config_transition {
            sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &new_config)?;
        }
        compares.push(KvCompare::ValueEquals {
            key: sinomo_kv::cluster_kv_raft_config_key(),
            value: current_config_entry.value.clone(),
        });
    } else {
        compares.push(KvCompare::Missing {
            key: sinomo_kv::cluster_kv_raft_config_key(),
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
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .is_some_and(|stored| {
            sinomo_kv::cluster_kv_raft_config_record_stable_eq(&stored, &new_config)
        });
    if !config_unchanged {
        success.push(KvOp::put_serde(
            sinomo_kv::cluster_kv_raft_config_key(),
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

pub fn update_cluster_node_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterNodeRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if changed {
        apply_cluster_node_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn_from_store(&store, member_entry, &member)?,
        )?;
    }
    update_local_cluster_node_role_if_present(paths, &node_id, role.clone(), now)?;
    Ok(ClusterNodeRoleUpdate {
        node_id,
        previous_role,
        role,
        changed,
    })
}

fn update_local_cluster_node_role_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let mut state = read_cluster_state_without_kv(paths)?;
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == *node_id)
    else {
        return Ok(());
    };
    if member.node_role == role {
        return Ok(());
    }
    apply_cluster_node_role(member, role, now);
    state.updated_at = now;
    write_cluster_state(paths, &state)?;
    Ok(())
}

pub fn update_cluster_raft_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterRaftRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .and_then(|config| sinomo_kv::cluster_kv_raft_config_member_role(&config, &node_id));
    let changed =
        previous_role != role || previous_state != new_state || config_role.as_ref() != Some(&role);
    if changed {
        apply_cluster_raft_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_raft_update_txn_from_store(&store, member_entry, &member, now)?,
        )?;
    }
    Ok(ClusterRaftRoleUpdate {
        node_id,
        previous_role,
        previous_state,
        role,
        state: new_state,
        changed,
    })
}

fn cluster_member_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
) -> Result<KvTxn, NetworkError> {
    Ok(KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: current_entry.key,
            value: current_entry.value,
        }],
        success: cluster_member_update_ops_from_store(store, member)?,
        failure: Vec::new(),
    })
}

fn cluster_member_raft_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
    now: OffsetDateTime,
) -> Result<KvTxn, NetworkError> {
    let raft_config_key = sinomo_kv::cluster_kv_raft_config_key();
    let Some(current_config_entry) = store.get(&raft_config_key).cloned() else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        });
    };
    let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
    let next_config = cluster_raft_config_from_store(store, member, &current_config, now)?;
    sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &next_config)?;
    let mut success = cluster_member_update_ops_from_store(store, member)?;
    push_bounded_txn_success_op(
        &mut success,
        KvOp::put_serde(raft_config_key.clone(), &next_config, None)?,
    )?;
    Ok(KvTxn {
        compares: vec![
            KvCompare::ValueEquals {
                key: current_entry.key,
                value: current_entry.value,
            },
            KvCompare::ValueEquals {
                key: raft_config_key,
                value: current_config_entry.value,
            },
        ],
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_member_update_ops_from_store(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
) -> Result<Vec<KvOp>, NetworkError> {
    let mut member = member.clone();
    cleanup_member_iroh_endpoints(&mut member);
    let mut ops = Vec::new();
    let member_key = cluster_member_key(&member.node_id);
    if store.get(&member_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterMemberRecord>()
                .map(|stored| !cluster_member_stable_eq(&stored, &member))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(member_key, &member, None)?)?;
    }

    let policy = ClusterCapabilityPolicyRecord::from(&member);
    let policy_key = cluster_capability_policy_key(&member.node_id);
    if store.get(&policy_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterCapabilityPolicyRecord>()
                .map(|stored| !cluster_capability_policy_stable_eq(&stored, &policy))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(policy_key, &policy, None)?)?;
    }
    push_store_cluster_member_endpoint_diff_ops(store, &member, &mut ops)?;
    Ok(ops)
}

fn push_store_cluster_member_endpoint_diff_ops(
    store: &InProcessKvStore,
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
        store.range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        }
    }

    for (key, (value, endpoint)) in desired_endpoints {
        if store.get(&key).is_none_or(|entry| {
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

pub(crate) fn cluster_raft_config_from_store(
    store: &InProcessKvStore,
    updated_member: &ClusterMemberRecord,
    current_config: &ClusterKvRaftConfigRecord,
    now: OffsetDateTime,
) -> Result<ClusterKvRaftConfigRecord, NetworkError> {
    let mut voters = Vec::new();
    let mut learners = Vec::new();
    let mut non_voting = Vec::new();
    for entry in
        store.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.node_id == updated_member.node_id {
            member = updated_member.clone();
        }
        if !member.state.is_authorized() {
            continue;
        }
        match member.state {
            ClusterMemberState::Voter => voters.push(member.node_id),
            ClusterMemberState::Learner => learners.push(member.node_id),
            ClusterMemberState::NonVoting => non_voting.push(member.node_id),
            ClusterMemberState::Active => match member.role {
                ClusterMemberRole::Voter => voters.push(member.node_id),
                ClusterMemberRole::Learner => learners.push(member.node_id),
                ClusterMemberRole::NonVoting => non_voting.push(member.node_id),
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
    Ok(ClusterKvRaftConfigRecord {
        cluster_id: current_config.cluster_id.clone(),
        term: current_config.term,
        kv_revision: current_config.kv_revision,
        voters,
        learners,
        non_voting,
        updated_at: now,
    })
}

pub fn remove_cluster_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_ids: Vec<NodeId>,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let requested = node_ids.into_iter().collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "no cluster members were specified".to_string(),
        });
    }
    let local_shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    if requested.contains(&state.local_node_id) || requested.contains(&local_shadow_node_id) {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "use `sinomo cluster leave` to remove the local daemon members".to_string(),
        });
    }

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode()?;
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a voter; change its raft role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub(crate) fn cluster_member_removal_txns_from_store(
    store: &InProcessKvStore,
    members: &[ClusterMemberRecord],
) -> Result<Vec<KvTxn>, NetworkError> {
    let mut txns = Vec::new();
    let final_endpoint_prefix_delete_limit =
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(2);
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    for member in members {
        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let mut residual_endpoint_keys = VecDeque::new();
        let mut explicit_delete_ops = Vec::new();
        for entry in store.prefix_entries(&endpoint_prefix) {
            residual_endpoint_keys.push_back(entry.key.clone());
            if residual_endpoint_keys.len() <= final_endpoint_prefix_delete_limit {
                continue;
            }
            if let Some(key) = residual_endpoint_keys.pop_front() {
                explicit_delete_ops.push(KvOp::Delete { key });
            }
            if explicit_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut explicit_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !explicit_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: explicit_delete_ops,
                failure: Vec::new(),
            });
        }

        txns.push(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::Delete {
                    key: cluster_member_key(&member.node_id),
                },
                KvOp::Delete {
                    key: cluster_capability_policy_key(&member.node_id),
                },
                KvOp::DeletePrefix {
                    prefix: endpoint_prefix,
                },
            ],
            failure: Vec::new(),
        });
    }
    Ok(txns)
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

struct NetworkClusterKvRaftAppendReplicationHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for NetworkClusterKvRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftAppendReplicationHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        self.host.last_local_raft_log_index(self.paths, state)
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        self.host
            .build_raft_append_request(self.paths, state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        self.host.on_raft_append_sent();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append(target, request, timeout)
    }
}

struct NetworkClusterKvRaftAppendReplicationAsyncHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for NetworkClusterKvRaftAppendReplicationAsyncHost<'_, H>
where
    H: ClusterRaftAppendReplicationAsyncHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        self.host.last_local_raft_log_index_async(self.paths, state)
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error> {
        self.host
            .build_raft_append_request_async(self.paths, state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        self.host.on_raft_append_sent_async()
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append_async(target, request, timeout)
    }
}

pub fn replicate_cluster_raft_to_endpoint_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
}

pub async fn replicate_cluster_raft_to_endpoint_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationAsyncHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationAsyncHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
    .await
}

struct NetworkClusterKvRaftQuorumCommitHost<'a, H> {
    paths: &'a ClusterPaths,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftCommandStorage for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Error = NetworkError;

    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<std::fs::File>, NetworkError> {
        sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        )
        .map(Some)
        .map_err(Into::into)
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        self.host
            .append_cluster_raft_command_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader()
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        self.host.commit_cluster_raft_index_with_events(index)
    }
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitHost for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Target = ClusterEndpointRecord;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, NetworkError> {
        Ok(cluster_raft_live_voter_targets(self.paths, state))
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.host
            .replicate_cluster_raft_to_endpoint(self.paths, state, target, timeout)
    }

    fn record_target_replication_status(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> Result<(), NetworkError> {
        let connectivity_state = match status {
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
            | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                ClusterConnectivityState::Healthy
            }
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                ClusterConnectivityState::Degraded
            }
        };
        self.host.record_endpoint_command_connectivity(
            self.paths,
            state,
            target,
            connectivity_state,
            latency_ms,
        );
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        merge_cluster_state_from_peer(state, peer_state);
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> Result<(), NetworkError> {
        self.host.fan_out_committed_cluster_raft_command(
            self.paths.clone(),
            state,
            events,
            targets,
        );
        Ok(())
    }

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost { paths, host };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum(state, command, options, &mut kv_host)
}

struct NetworkClusterKvRaftQuorumCommitAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    local_node_id: NodeId,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitAsyncHost
    for NetworkClusterKvRaftQuorumCommitAsyncHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<std::fs::File>, NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_kv::lock_cluster_kv_commit_sequence(
                        &kv_paths,
                        sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
                    )
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft commit sequence lock".to_string(),
                    detail: format!("cluster raft commit sequence lock task failed: {source}"),
                })?
                .map(Some)
                .map_err(Into::into)
        })
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
        &'a mut self,
        command: KvCommand,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftUncommittedAppend,
        NetworkError,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            append_cluster_raft_command_uncommitted_or_no_effect_async(
                runtime,
                paths,
                &local_node_id,
                command,
            )
            .await
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            discard_cluster_raft_uncommitted_tail_and_clear_leader_async(
                runtime,
                paths,
                &local_node_id,
            )
            .await
            .map(|_| ())
        })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::ClusterKvRaftCommitResult, NetworkError>
    {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            commit_cluster_raft_index_with_events_async(runtime, paths, &local_node_id, index).await
        })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move { cluster_raft_live_voter_targets_async(runtime, paths, state).await })
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, NetworkError> {
        let paths = self.paths;
        let mut endpoint_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            endpoint_options.timeout = timeout;
            replicate_cluster_raft_to_endpoint_async_with_host(
                paths,
                state,
                target,
                &[],
                endpoint_options,
                host,
            )
            .await
            .map(|outcome| outcome.response)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths;
        let host = &mut *self.host;
        Box::pin(async move {
            let connectivity_state = match status {
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
                | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                    ClusterConnectivityState::Healthy
                }
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                    ClusterConnectivityState::Degraded
                }
            };
            host.record_endpoint_command_connectivity(
                paths,
                state,
                target,
                connectivity_state,
                latency_ms,
            );
            Ok(())
        })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        Box::pin(async move {
            merge_cluster_state_from_peer(state, peer_state);
            Ok(())
        })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            host.fan_out_committed_cluster_raft_command_async(paths, state, events, targets)
                .await
        })
    }

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_raft_command_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        local_node_id: state.local_node_id.clone(),
        append_options,
        host,
    };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum_async(
        state,
        command,
        options,
        &mut kv_host,
    )
    .await
}

pub async fn fan_out_committed_cluster_raft_command_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: &[KvEvent],
    options: ClusterRaftCommittedFanoutOptions,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> Result<ClusterRaftCommittedFanoutReport, NetworkError> {
    let mut report = ClusterRaftCommittedFanoutReport::default();
    let mut replicated_nodes = BTreeSet::new();
    for endpoint in cluster_raft_live_voter_targets_async(runtime, paths, state).await? {
        if replicated_nodes.contains(&endpoint.node_id) {
            continue;
        }
        report.attempted_endpoints += 1;
        let Ok(outcome) = host
            .replicate_cluster_raft_to_endpoint_async(
                paths,
                state,
                &endpoint,
                events,
                options.raft_append_timeout,
            )
            .await
        else {
            continue;
        };
        if !outcome.response.accepted {
            continue;
        }
        replicated_nodes.insert(endpoint.node_id.clone());
        report.replicated_nodes.push(endpoint.node_id.clone());
        if outcome.watch_event_fanout_needed {
            report.watch_event_batches_sent +=
                send_cluster_kv_watch_event_fanout_batches_async_with_host(
                    state,
                    &endpoint,
                    events,
                    options.watch_event_timeout,
                    host,
                )
                .await;
        }
    }
    Ok(report)
}

pub fn spawn_cluster_raft_committed_fanout_worker_on_runtime<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
    options: ClusterRaftCommittedFanoutOptions,
    host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H + Send + 'static,
{
    CLUSTER_RAFT_FANOUT_PENDING.store(true, Ordering::Release);
    if CLUSTER_RAFT_FANOUT_ACTIVE.swap(true, Ordering::AcqRel) {
        return;
    }
    let worker_runtime = runtime.clone();
    runtime.spawn(async move {
        run_cluster_raft_committed_fanout_worker_async_with_host(
            worker_runtime,
            paths,
            state,
            events,
            options,
            host_factory,
        )
        .await;
    });
}

async fn run_cluster_raft_committed_fanout_worker_async_with_host<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    mut state: ClusterBootstrapState,
    mut events: Vec<KvEvent>,
    options: ClusterRaftCommittedFanoutOptions,
    mut host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost,
    F: FnMut() -> H,
{
    loop {
        while CLUSTER_RAFT_FANOUT_PENDING.swap(false, Ordering::AcqRel) {
            let mut host = host_factory();
            let _ = fan_out_committed_cluster_raft_command_async_with_host(
                &runtime, &paths, &state, &events, options, &mut host,
            )
            .await;
            if let Some((latest_state, latest_events)) =
                read_latest_cluster_raft_fanout_inputs_async(&runtime, &paths).await
            {
                state = latest_state;
                events = latest_events;
            } else {
                events = Vec::new();
            }
        }

        if !claim_next_cluster_raft_fanout_pass(
            &CLUSTER_RAFT_FANOUT_ACTIVE,
            &CLUSTER_RAFT_FANOUT_PENDING,
        ) {
            break;
        }
    }
}

async fn read_latest_cluster_raft_fanout_inputs_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Option<(ClusterBootstrapState, Vec<KvEvent>)> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || {
            let latest_state = read_cluster_state(&paths)?;
            let latest_events = read_cluster_kv_snapshot(&paths)
                .map(|snapshot| snapshot.events)
                .unwrap_or_default();
            Ok::<_, NetworkError>((latest_state, latest_events))
        })
        .await
        .ok()
        .and_then(Result::ok)
}

#[cfg(test)]
pub(crate) fn claim_next_cluster_raft_fanout_pass_for_test(
    active: &AtomicBool,
    pending: &AtomicBool,
) -> bool {
    claim_next_cluster_raft_fanout_pass(active, pending)
}

fn claim_next_cluster_raft_fanout_pass(active: &AtomicBool, pending: &AtomicBool) -> bool {
    active.store(false, Ordering::Release);
    if !pending.load(Ordering::Acquire) {
        return false;
    }
    !active.swap(true, Ordering::AcqRel)
}

async fn send_cluster_kv_watch_event_fanout_batches_async_with_host(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    events: &[KvEvent],
    timeout: Duration,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> usize {
    let mut sent = 0;
    for event_batch in sinomo_kv::cluster_kv_watch_event_fanout_batches(events) {
        let response = host
            .send_cluster_kv_watch_events_async(
                endpoint,
                &ClusterKvWatchEventsRequest {
                    state: state.clone(),
                    leader_id: state.local_node_id.clone(),
                    events: event_batch.to_vec(),
                },
                timeout,
            )
            .await;
        if !response.is_ok_and(|response| response.accepted) {
            break;
        }
        sent += 1;
    }
    sent
}

pub fn cluster_reachable_voter_count_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityHost,
) -> usize {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return voters.len();
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = cluster_raft_live_voter_targets(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host.probe_voter(endpoint, options.fair_command_timeout(endpoint, remaining)) {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    voters.len()
}

pub async fn cluster_reachable_voter_count_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityAsyncHost,
) -> Result<usize, NetworkError> {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return Ok(voters.len());
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = cluster_raft_live_voter_targets(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host
            .probe_voter_async(endpoint, options.fair_command_timeout(endpoint, remaining))
            .await?
        {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    Ok(voters.len())
}

pub fn cluster_has_local_quorum_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityHost,
) -> bool {
    cluster_reachable_voter_count_with_host(paths, state, options, host)
        >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state)
}

pub async fn cluster_has_local_quorum_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityAsyncHost,
) -> Result<bool, NetworkError> {
    Ok(
        cluster_reachable_voter_count_async_with_host(paths, state, options, host).await?
            >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state),
    )
}

pub fn refresh_current_cluster_kv_store_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<InProcessKvStore, NetworkError> {
    let mut snapshots = Vec::new();
    if let Ok(snapshot) = read_cluster_kv_snapshot(paths) {
        snapshots.push(snapshot);
    }
    let mut targets = cluster_raft_live_voter_targets(paths, state);
    if let Ok(Some(leader_id)) = host.known_raft_leader_id(paths, state)
        && !cluster_node_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let deadline = Instant::now() + options.refresh_budget(!snapshots.is_empty());
    let mut fetched_remote = false;
    for endpoint in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if let Ok(snapshot) = host
            .fetch_cluster_kv_snapshot(endpoint, options.fair_command_timeout(endpoint, remaining))
        {
            snapshots.push(snapshot);
            fetched_remote = true;
        }
    }
    // Cross-segment learners cannot dial any voter directly; fall back to
    // relaying the snapshot fetch through a bridge peer, mirroring how KV txn
    // forwarding relays toward a voter.
    if !fetched_remote {
        'relay_targets: for voter in &targets {
            for peer in host
                .relay_peers(paths, state)
                .into_iter()
                .filter(|peer| peer.node_id != voter.node_id)
            {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break 'relay_targets;
                }
                if let Ok(snapshot) = relay_fetch_cluster_kv_snapshot_via_peer(
                    state,
                    voter,
                    &peer,
                    options.fair_command_timeout(&peer, remaining),
                    host,
                ) {
                    snapshots.push(snapshot);
                    break 'relay_targets;
                }
            }
        }
    }
    let snapshot = snapshots
        .into_iter()
        .max_by_key(|snapshot| snapshot.revision)
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "no local or reachable cluster KV snapshot is available".to_string(),
        })?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
    host.restore_cluster_raft_from_snapshot_if_newer(
        paths,
        &state.local_node_id,
        snapshot.clone(),
    )?;
    Ok(InProcessKvStore::restore(snapshot))
}

fn relay_fetch_cluster_kv_snapshot_via_peer(
    state: &ClusterBootstrapState,
    voter: &ClusterEndpointRecord,
    peer: &ClusterEndpointRecord,
    timeout: Duration,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<KvSnapshot, NetworkError> {
    let payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let request =
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = host.relay_rpc_to_node(state, &voter.node_id, peer, &request, timeout)?;
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}

pub fn refresh_current_cluster_kv_store_with_local_storage_and_transport<F>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    fetch_cluster_kv_snapshot: F,
) -> Result<InProcessKvStore, NetworkError>
where
    F: FnMut(&ClusterEndpointRecord, Duration) -> Result<KvSnapshot, NetworkError>,
{
    let mut host = LocalClusterKvSnapshotRefreshHost {
        fetch_cluster_kv_snapshot,
    };
    refresh_current_cluster_kv_store_with_host(paths, state, options, &mut host)
}

pub fn elect_cluster_raft_leader_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut impl ClusterRaftVoteTransportHost,
) -> Result<bool, NetworkError> {
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state);
    let remote_vote_targets = cluster_raft_live_voter_targets(paths, state)
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id != state.local_node_id && voter_ids.contains(&endpoint.node_id)
        })
        .collect::<Vec<_>>();
    let remote_vote_target_ids = remote_vote_targets
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<Vec<_>>();
    let mut election_host = NetworkClusterKvRaftLeaderElectionHost::new(
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
}

pub async fn elect_cluster_raft_leader_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut (impl ClusterRaftVoteTransportAsyncHost + Send),
) -> Result<bool, NetworkError> {
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state);
    let remote_vote_targets = cluster_raft_live_voter_targets_async(runtime, paths, state)
        .await?
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id != state.local_node_id && voter_ids.contains(&endpoint.node_id)
        })
        .collect::<Vec<_>>();
    let remote_vote_target_ids = remote_vote_targets
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<Vec<_>>();
    let mut election_host = NetworkClusterKvRaftLeaderElectionAsyncHost::new(
        runtime,
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_async_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
    .await
}

struct NetworkClusterKvTxnQuorumHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumHost for NetworkClusterKvTxnQuorumHost<'_, H>
where
    H: ClusterKvTxnQuorumHost,
{
    type Error = NetworkError;

    fn refresh_current_cluster_kv_store(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        self.host
            .refresh_current_cluster_kv_store(self.paths, state)
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error> {
        self.host.local_is_raft_leader(self.paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        self.host.forward_cluster_kv_txn_to_voter(
            self.paths,
            state,
            txn,
            self.options.forward_options,
        )
    }

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error> {
        self.host
            .elect_cluster_raft_leader(self.paths, state, self.options.leader_election_options)
    }

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error> {
        Ok(self.host.reachable_voter_count(
            self.paths,
            state,
            self.options.voter_reachability_options,
        ))
    }

    fn semantic_txn_no_effect_response(
        &mut self,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        Ok(
            cluster_semantic_txn_no_effect_response(self.paths, txn, OffsetDateTime::now_utc())?
                .map(
                    |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                        snapshot,
                        txn_response: Some(txn_response),
                    },
                ),
        )
    }

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<sinomo_kv::ClusterKvRaftQuorumCommitResult, Self::Error> {
        self.host.commit_cluster_raft_command_with_quorum(
            self.paths,
            state,
            KvCommand::Txn(txn),
            ClusterRaftQuorumCommitOptions {
                timeout: self.options.raft_append_timeout,
            },
        )
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_kv_txn_with_quorum_response_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterKvTxnForwardResult, NetworkError> {
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumHost {
        paths,
        options,
        host,
    };
    sinomo_kv::commit_cluster_kv_txn_with_quorum_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
}

pub fn handle_cluster_kv_txn_request_with_host(
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvTxnResponse, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths).map_err(|error| match error {
        NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
            detail: "local node has no cluster state".to_string(),
        },
        other => other,
    })?;
    let result = commit_cluster_kv_txn_with_quorum_response_with_host(
        paths, &mut state, txn, options, host,
    )?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &result.snapshot)?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

async fn locked_cluster_raft_known_leader_id_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_cluster_raft_known_leader_id(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft known leader".to_string(),
            detail: format!("known leader task failed: {source}"),
        })?
}

async fn locked_cluster_raft_local_is_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_cluster_raft_local_is_leader(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft local leader".to_string(),
            detail: format!("local leader task failed: {source}"),
        })?
}

async fn cluster_node_blocks_leader_preference_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            Ok(cluster_node_blocks_leader_preference(
                &paths, &state, &node_id,
            ))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft leader preference".to_string(),
            detail: format!("leader preference task failed: {source}"),
        })?
}

async fn cluster_member_publish_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || Ok(cluster_member_publish_targets(&paths, &state)))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster relay peer target selection".to_string(),
            detail: format!("cluster relay peer target selection task failed: {source}"),
        })?
}

async fn cluster_semantic_txn_no_effect_response_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: &KvTxn,
    now: OffsetDateTime,
) -> Result<Option<(KvSnapshot, KvTxnResponse)>, NetworkError> {
    let paths = paths.clone();
    let txn = txn.clone();
    runtime
        .spawn_blocking(move || cluster_semantic_txn_no_effect_response(&paths, &txn, now))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster semantic txn no-effect check".to_string(),
            detail: format!("semantic txn no-effect task failed: {source}"),
        })?
}

async fn write_imported_cluster_kv_snapshot_if_changed_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let snapshot = snapshot.clone();
    runtime
        .spawn_blocking(move || write_imported_cluster_kv_snapshot_if_changed(&paths, &snapshot))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })?
        .map(|_| ())
}

struct NetworkClusterKvTxnQuorumAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumAsyncHost for NetworkClusterKvTxnQuorumAsyncHost<'_, H>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    type Error = NetworkError;

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(
            async move { locked_cluster_raft_local_is_leader_async(runtime, paths, state).await },
        )
    }

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        txn: &'a KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.forward_options;
        let host = &mut *self.host;
        Box::pin(async move {
            forward_cluster_kv_txn_to_voter_async_with_host(
                runtime, paths, state, txn, options, host,
            )
            .await
        })
    }

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.leader_election_options;
        let host = &mut *self.host;
        Box::pin(async move {
            elect_cluster_raft_leader_async_with_host(runtime, paths, state, options, host).await
        })
    }

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, usize, Self::Error> {
        let paths = self.paths;
        let options = self.options.voter_reachability_options;
        let host = &mut *self.host;
        Box::pin(async move {
            cluster_reachable_voter_count_async_with_host(paths, state, options, host).await
        })
    }

    fn semantic_txn_no_effect_response_async<'a>(
        &'a mut self,
        txn: &'a KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move {
            Ok(cluster_semantic_txn_no_effect_response_async(
                runtime,
                paths,
                txn,
                OffsetDateTime::now_utc(),
            )
            .await?
            .map(
                |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                    snapshot,
                    txn_response: Some(txn_response),
                },
            ))
        })
    }

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftQuorumCommitResult,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options;
        let host = &mut *self.host;
        Box::pin(async move {
            commit_cluster_raft_command_with_quorum_async_with_host(
                runtime,
                paths,
                state,
                KvCommand::Txn(txn),
                ClusterRaftQuorumCommitOptions {
                    timeout: options.raft_append_timeout,
                },
                options.raft_append_replication_options,
                host,
            )
            .await
        })
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_kv_txn_with_quorum_response_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnForwardResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
        host,
    };
    sinomo_kv::commit_cluster_kv_txn_with_quorum_async_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
    .await
}

pub async fn handle_cluster_kv_txn_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvTxnResponse, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let mut state = read_cluster_state_without_kv_async(runtime, paths)
        .await
        .map_err(|error| match error {
            NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other,
        })?;
    let result = commit_cluster_kv_txn_with_quorum_response_async_with_host(
        runtime, paths, &mut state, txn, options, host,
    )
    .await?;
    write_imported_cluster_kv_snapshot_if_changed_async(runtime, paths, &result.snapshot).await?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

pub fn forward_cluster_kv_txn_to_voter_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError> {
    let mut targets = cluster_raft_live_voter_targets(paths, state);
    if targets.is_empty() {
        return Ok(None);
    }
    if let Ok(Some(leader_id)) = host.known_raft_leader_id(paths, state)
        && !cluster_node_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for endpoint in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "forwarding did not complete before {}ms command budget",
                options.timeout.as_millis()
            ));
            break;
        }
        match host.send_cluster_kv_txn(endpoint, txn, options.command_timeout(endpoint, remaining))
        {
            Ok(response) => {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                let snapshot = host.fetch_cluster_kv_snapshot(
                    endpoint,
                    options.command_timeout(endpoint, remaining),
                )?;
                return Ok(Some(ClusterKvTxnForwardResult {
                    snapshot,
                    txn_response: Some(response),
                }));
            }
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }

    'relay_targets: for voter in &targets {
        for peer in host
            .relay_peers(paths, state)
            .into_iter()
            .filter(|peer| peer.node_id != voter.node_id)
        {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                failures.push(format!(
                    "relay forwarding did not complete before {}ms command budget",
                    options.timeout.as_millis()
                ));
                break 'relay_targets;
            }
            match relay_cluster_kv_txn_to_voter_via_peer_with_host(
                state, voter, &peer, txn, remaining, host,
            ) {
                Ok(response) => return Ok(Some(response)),
                Err(error) => failures.push(format!(
                    "relay via {} ({:?}) to {} failed: {error}",
                    peer.address, peer.transport, voter.node_id
                )),
            }
        }
    }

    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not forward cluster KV transaction to any known voter{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

fn relay_cluster_kv_txn_to_voter_via_peer_with_host(
    state: &ClusterBootstrapState,
    voter: &ClusterEndpointRecord,
    peer: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<ClusterKvTxnForwardResult, NetworkError> {
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let txn_response =
        host.relay_rpc_to_node(state, &voter.node_id, peer, &txn_request, timeout)?;
    txn_response.error_detail()?;
    let txn_response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response = host.relay_rpc_to_node(
        state,
        &voter.node_id,
        peer,
        &snapshot_request,
        snapshot_timeout,
    )?;
    snapshot_response.error_detail()?;
    let snapshot = sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body)?;
    Ok(ClusterKvTxnForwardResult {
        snapshot,
        txn_response: Some(txn_response),
    })
}

pub async fn forward_cluster_kv_txn_to_voter_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError>
where
    H: ClusterKvTxnForwardAsyncHost,
{
    let mut targets = cluster_raft_live_voter_targets_async(runtime, paths, state).await?;
    if targets.is_empty() {
        return Ok(None);
    }
    if let Ok(Some(leader_id)) =
        locked_cluster_raft_known_leader_id_async(runtime, paths, state).await
        && !cluster_node_blocks_leader_preference_async(runtime, paths, state, &leader_id).await?
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for endpoint in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "forwarding did not complete before {}ms command budget",
                options.timeout.as_millis()
            ));
            break;
        }
        match host
            .send_cluster_kv_txn_async(endpoint, txn, options.command_timeout(endpoint, remaining))
            .await
        {
            Ok(response) => {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                let snapshot = host
                    .fetch_cluster_kv_snapshot_async(
                        endpoint,
                        options.command_timeout(endpoint, remaining),
                    )
                    .await?;
                return Ok(Some(ClusterKvTxnForwardResult {
                    snapshot,
                    txn_response: Some(response),
                }));
            }
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }

    let relay_peers = cluster_member_publish_targets_async(runtime, paths, state).await?;
    'relay_targets: for voter in &targets {
        for peer in relay_peers
            .iter()
            .filter(|peer| peer.node_id != voter.node_id)
        {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                failures.push(format!(
                    "relay forwarding did not complete before {}ms command budget",
                    options.timeout.as_millis()
                ));
                break 'relay_targets;
            }
            match relay_cluster_kv_txn_to_voter_via_peer_async_with_host(
                state, voter, peer, txn, remaining, host,
            )
            .await
            {
                Ok(response) => return Ok(Some(response)),
                Err(error) => failures.push(format!(
                    "relay via {} ({:?}) to {} failed: {error}",
                    peer.address, peer.transport, voter.node_id
                )),
            }
        }
    }

    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not forward cluster KV transaction to any known voter{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

async fn relay_cluster_kv_txn_to_voter_via_peer_async_with_host<H>(
    state: &ClusterBootstrapState,
    voter: &ClusterEndpointRecord,
    peer: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    host: &mut H,
) -> Result<ClusterKvTxnForwardResult, NetworkError>
where
    H: ClusterKvTxnForwardAsyncHost,
{
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let txn_response = host
        .relay_rpc_to_node_async(state, &voter.node_id, peer, &txn_request, timeout)
        .await?;
    txn_response.error_detail()?;
    let txn_response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response = host
        .relay_rpc_to_node_async(
            state,
            &voter.node_id,
            peer,
            &snapshot_request,
            snapshot_timeout,
        )
        .await?;
    snapshot_response.error_detail()?;
    let snapshot = sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body)?;
    Ok(ClusterKvTxnForwardResult {
        snapshot,
        txn_response: Some(txn_response),
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

pub fn cluster_raft_live_voter_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let node_statuses = cluster_node_status_map_from_kv(paths);
    cluster_raft_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id))
        .filter(|endpoint| {
            !node_statuses
                .get(&endpoint.node_id)
                .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
        })
        .collect()
}

pub async fn cluster_raft_live_voter_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || Ok(cluster_raft_live_voter_targets(&paths, &state)))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft voter target selection".to_string(),
            detail: format!("cluster raft voter target selection task failed: {source}"),
        })?
}

pub fn cluster_node_blocks_leader_preference(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> bool {
    // A node blocks leader preference if it has gracefully reported itself
    // offline, or if active connectivity probes can no longer reach it. The
    // self-reported `observed_at` is not consulted: it is no longer refreshed on
    // a timer, so an idle-but-healthy node must not be deprioritized.
    let node_statuses = cluster_node_status_map_from_kv(paths);
    if node_statuses
        .get(node_id)
        .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
    {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .is_some_and(|member| cluster_member_unreachable_via_connectivity(paths, state, member))
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
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
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

/// Member-publish targets with voters first. The publish doubles as the
/// learner's KV snapshot sync (`ClusterMemberPublishRequest::local_kv_revision`),
/// and the snapshot in the response comes from whichever node answers — a
/// sibling learner with an equally stale snapshot answers "current" and the
/// sync silently stalls. Preferring voters (then bridges, whose own snapshots
/// converge the same way) forms a convergence chain rooted at the voter.
pub fn cluster_member_publish_targets_voter_first(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut targets = cluster_member_publish_targets(paths, state);
    targets.sort_by_key(|endpoint| {
        !sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id)
    });
    targets
}

pub fn cluster_member_publish_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let main_daemon_node_ids = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && cluster_member_has_main_daemon_capability(member)
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut targets = Vec::new();

    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && cluster_member_has_main_daemon_capability(member)
    }) {
        for endpoint in member
            .endpoints
            .iter()
            .filter(|endpoint| locally_learned_bridge_route_endpoint(endpoint))
        {
            push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint.clone());
        }
    }

    for node_id in graph.healthy_targets(&state.local_node_id) {
        if !main_daemon_node_ids.contains(&node_id) {
            continue;
        }
        let Some(endpoint) = graph.best_healthy_endpoint(&state.local_node_id, &node_id) else {
            continue;
        };
        push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint);
    }

    for endpoint in cluster_main_daemon_command_targets(paths, state)
        .into_iter()
        .chain(cluster_voter_command_targets(paths, state))
    {
        if endpoint.node_id == state.local_node_id {
            continue;
        }
        push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint);
    }

    targets
}

pub fn locally_learned_bridge_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

fn cluster_member_has_main_daemon_capability(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
}

fn push_unique_cluster_endpoint(
    endpoints: &mut Vec<ClusterEndpointRecord>,
    seen: &mut BTreeSet<(NodeId, String, String)>,
    endpoint: ClusterEndpointRecord,
) {
    if seen.insert((
        endpoint.node_id.clone(),
        format!("{:?}", endpoint.transport),
        endpoint.address.clone(),
    )) {
        endpoints.push(endpoint);
    }
}

pub fn cluster_voter_command_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_raft_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub fn cluster_invite_publish_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut seen_nodes = BTreeSet::new();
    let mut targets = Vec::new();
    for endpoint in cluster_voter_command_targets(paths, state) {
        if !seen_nodes.insert(endpoint.node_id.clone()) {
            continue;
        }
        targets.push(endpoint);
    }
    targets
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

/// True when the cluster has at least one member that is not this node's own
/// main/shadow process and has not been removed.
pub fn cluster_has_remote_peers(state: &ClusterBootstrapState) -> bool {
    state.members.iter().any(|member| {
        !matches!(member.state, ClusterMemberState::Removed)
            && !cluster_member_is_local_process(state, member)
    })
}

pub fn cluster_is_settled_solo(state: &ClusterBootstrapState) -> bool {
    !cluster_has_remote_peers(state) && !cluster_state_needs_join_registration(state)
}

pub fn cluster_local_process_members(state: &ClusterBootstrapState) -> Vec<ClusterMemberRecord> {
    state
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(state, member))
        .cloned()
        .collect()
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

pub async fn discover_peer_route_ip_candidates_async(state: &ClusterBootstrapState) -> Vec<IpAddr> {
    discover_local_ip_candidates_for_targets_async(advertised_peer_route_targets(state)).await
}

pub fn daemon_advertised_endpoints_from_static_addresses(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_endpoints: bool,
    advertise_host: Option<&str>,
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        allow_public_endpoints,
        advertise_host,
        std::iter::empty(),
    )
}

pub async fn daemon_advertised_endpoints_from_addresses_async(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_addresses_with_public_discovery_async(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        true,
    )
    .await
}

/// How long to wait when probing this node's own public endpoint for reachability.
const PUBLIC_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

/// Whether this node's own public endpoint (`ip:port`) is actually reachable -- i.e. an
/// external connection to it lands on this daemon, via a user-configured port-forward (or
/// NAT hairpin). Only a verified public endpoint should be advertised; an unverified one (a
/// NAT'd node with no forward) would publish an unreachable address that every peer then
/// wastes connection attempts on. Manual endpoint configuration is future work.
pub(crate) async fn public_endpoint_is_reachable_for_local_ips_async(
    ip: IpAddr,
    port: u16,
    local_ips: impl IntoIterator<Item = IpAddr>,
) -> bool {
    public_endpoint_is_reachable_for_local_ips_with_async_probe_at(
        ip,
        port,
        Instant::now(),
        local_ips,
        |address| async move {
            matches!(
                tokio::time::timeout(
                    PUBLIC_ENDPOINT_PROBE_TIMEOUT,
                    tokio::net::TcpStream::connect(address),
                )
                .await,
                Ok(Ok(_stream))
            )
        },
    )
    .await
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

pub(crate) async fn public_endpoint_is_reachable_for_local_ips_with_async_probe_at<F, Fut>(
    ip: IpAddr,
    port: u16,
    now: Instant,
    local_ips: impl IntoIterator<Item = IpAddr>,
    probe: F,
) -> bool
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = bool>,
{
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
    let reachable = probe(address).await;
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

pub async fn daemon_advertised_endpoints_from_addresses_with_public_discovery_async(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
        None,
    )
    .await
}

pub async fn daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
    advertise_host: Option<&str>,
) -> Vec<ClusterEndpointRecord> {
    let verified_public_ips = verified_public_endpoint_ips_async(
        host,
        port,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
    .await;
    daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
        advertise_host,
        verified_public_ips,
    )
}

fn daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
    advertise_host: Option<&str>,
    verified_public_ips: impl IntoIterator<Item = IpAddr>,
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
            for ip in verified_public_ips {
                // Only advertise a discovered public IP when it is actually reachable on the
                // daemon port -- i.e. the user has a port-forward (or NAT hairpin) back to
                // this daemon. An unverified public IP would otherwise pollute every peer's
                // connection-target list with an unreachable endpoint.
                if is_invitable_ip(ip) {
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
    if let Some(address) = daemon_advertise_host_address(advertise_host, port)
        && endpoints.iter().all(|endpoint| endpoint.address != address)
    {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(5),
        ));
    }
    endpoints
}

fn should_discover_public_endpoint_candidates(
    host: &str,
    allow_public_ip_discovery: bool,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> bool {
    if !matches!(host, "0.0.0.0" | "::") || !allow_public_ip_discovery {
        return false;
    }
    !lan_ips
        .iter()
        .chain(route_ips.iter())
        .copied()
        .any(|ip| is_invitable_ip(ip) && !crate::is_private_or_link_local_ip(ip))
}

async fn verified_public_endpoint_ips_async(
    host: &str,
    port: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<IpAddr> {
    if !should_discover_public_endpoint_candidates(
        host,
        allow_public_ip_discovery,
        lan_ips,
        route_ips,
    ) {
        return Vec::new();
    }
    let mut verified = Vec::new();
    for ip in discover_default_public_ip_candidates_for_local_ips_async(
        lan_ips.iter().chain(route_ips.iter()).copied(),
    )
    .await
    {
        if is_invitable_ip(ip)
            && public_endpoint_is_reachable_for_local_ips_async(
                ip,
                port,
                lan_ips.iter().chain(route_ips.iter()).copied(),
            )
            .await
        {
            verified.push(ip);
        }
    }
    verified
}

fn daemon_advertise_host_address(host: Option<&str>, port: u16) -> Option<String> {
    let host = host?.trim();
    if host.is_empty()
        || matches!(host, "0.0.0.0" | "::")
        || host.contains("://")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified())
    {
        return None;
    }
    let address = if let Ok(socket) = host.parse::<SocketAddr>() {
        if socket.ip().is_unspecified() {
            return None;
        }
        host.to_string()
    } else if let Ok(ip) = host.parse::<IpAddr>() {
        format_ip_port(ip, port)
    } else if let Some((name, explicit_port)) = host.rsplit_once(':') {
        if !name.contains(':') && explicit_port.parse::<u16>().is_ok() {
            host.to_string()
        } else {
            format!("[{host}]:{port}")
        }
    } else {
        format!("{host}:{port}")
    };
    if is_loopback_connect_address(&address) {
        return None;
    }
    Some(address)
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
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(key, &status, None)?],
            failure: Vec::new(),
        }),
    )
}

pub fn publish_cluster_node_status_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNodeStatusPublishHost,
{
    if host.published_node_status_matches(paths, &status) {
        return Ok(None);
    }
    if stored_cluster_node_status_matches(paths, &status)? {
        host.remember_published_node_status(paths, &status);
        return Ok(None);
    }
    let mut state = read_cluster_state(paths)?;
    let node_id = status.node_id.clone();
    let snapshot = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(cluster_node_status_key(&node_id), &status, None)
                    .map_err(NetworkError::from)?,
            ],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_node_status(paths, &status);
    Ok(Some(snapshot))
}

pub async fn publish_cluster_node_status_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNodeStatusPublishAsyncHost,
{
    if host
        .published_node_status_matches_async(paths, &status)
        .await?
    {
        return Ok(None);
    }
    let match_paths = paths.clone();
    let match_status = status.clone();
    let stored_matches = runtime
        .spawn_blocking(move || stored_cluster_node_status_matches(&match_paths, &match_status))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read stored cluster node status".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;
    if stored_matches {
        host.remember_published_node_status_async(paths, &status)
            .await?;
        return Ok(None);
    }

    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read cluster state".to_string(),
                detail: format!("cluster state read task failed: {source}"),
            })
        })?
        .map_err(H::Error::from)?;
    let node_id = status.node_id.clone();
    let snapshot = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(cluster_node_status_key(&node_id), &status, None)
                        .map_err(NetworkError::from)
                        .map_err(H::Error::from)?,
                ],
                failure: Vec::new(),
            },
        )
        .await?;
    host.remember_published_node_status_async(paths, &status)
        .await?;
    Ok(Some(snapshot))
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

pub fn publish_cluster_network_info_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkInfoPublishHost,
{
    if host.published_network_info_matches(paths, &info) {
        return Ok(None);
    }
    if stored_cluster_network_info_matches(paths, &info)? {
        host.remember_published_network_info(paths, &info);
        return Ok(None);
    }
    let mut state = read_cluster_state(paths)?;
    let node_id = info.node_id.clone();
    let snapshot = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(cluster_network_info_key(&node_id), &info, None)
                    .map_err(NetworkError::from)?,
            ],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_network_info(paths, &info);
    Ok(Some(snapshot))
}

pub async fn publish_cluster_network_info_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkInfoPublishAsyncHost,
{
    if host
        .published_network_info_matches_async(paths, &info)
        .await?
    {
        return Ok(None);
    }
    let match_paths = paths.clone();
    let match_info = info.clone();
    let stored_matches = runtime
        .spawn_blocking(move || stored_cluster_network_info_matches(&match_paths, &match_info))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read stored cluster network info".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;
    if stored_matches {
        host.remember_published_network_info_async(paths, &info)
            .await?;
        return Ok(None);
    }

    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read cluster state".to_string(),
                detail: format!("cluster state read task failed: {source}"),
            })
        })?
        .map_err(H::Error::from)?;
    let node_id = info.node_id.clone();
    let snapshot = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(cluster_network_info_key(&node_id), &info, None)
                        .map_err(NetworkError::from)
                        .map_err(H::Error::from)?,
                ],
                failure: Vec::new(),
            },
        )
        .await?;
    host.remember_published_network_info_async(paths, &info)
        .await?;
    Ok(Some(snapshot))
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

pub fn sync_cluster_connectivity_records_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishHost,
{
    write_local_cluster_connectivity_record_set(paths, source_node_id, &records)?;
    if host.published_connectivity_record_set_matches(paths, source_node_id, &records) {
        return Ok(None);
    }
    let mut state = None;
    let mut latest = None;
    loop {
        let success = cluster_connectivity_full_sync_batch_ops(
            paths,
            source_node_id,
            &records,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        )?;
        if success.is_empty() {
            host.remember_published_connectivity_record_set(paths, source_node_id, &records);
            return Ok(latest);
        }
        if state.is_none() {
            state = Some(read_cluster_state(paths)?);
        }
        latest = Some(
            host.commit_cluster_kv_txn_with_quorum(
                paths,
                state
                    .as_mut()
                    .expect("cluster state is initialized before connectivity publish"),
                KvTxn {
                    compares: Vec::new(),
                    success,
                    failure: Vec::new(),
                },
            )?,
        );
    }
}

pub async fn sync_cluster_connectivity_records_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishAsyncHost,
{
    let write_paths = paths.clone();
    let write_source_node_id = source_node_id.clone();
    let write_records = records.clone();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_set(
                &write_paths,
                &write_source_node_id,
                &write_records,
            )
        })
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "write local cluster connectivity record set".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;

    if host
        .published_connectivity_record_set_matches_async(paths, source_node_id, &records)
        .await?
    {
        return Ok(None);
    }

    let mut state = None;
    let mut latest = None;
    loop {
        let batch_paths = paths.clone();
        let batch_source_node_id = source_node_id.clone();
        let batch_records = records.clone();
        let success = runtime
            .spawn_blocking(move || {
                cluster_connectivity_full_sync_batch_ops(
                    &batch_paths,
                    &batch_source_node_id,
                    &batch_records,
                    sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
                )
            })
            .await
            .map_err(|source| {
                H::Error::from(NetworkError::Io {
                    operation: "build cluster connectivity sync batch".to_string(),
                    detail: source.to_string(),
                })
            })?
            .map_err(H::Error::from)?;
        if success.is_empty() {
            host.remember_published_connectivity_record_set_async(paths, source_node_id, &records)
                .await?;
            return Ok(latest);
        }
        if state.is_none() {
            let state_paths = paths.clone();
            state = Some(
                runtime
                    .spawn_blocking(move || read_cluster_state(&state_paths))
                    .await
                    .map_err(|source| {
                        H::Error::from(NetworkError::Io {
                            operation: "read cluster state".to_string(),
                            detail: format!("cluster state read task failed: {source}"),
                        })
                    })?
                    .map_err(H::Error::from)?,
            );
        }
        latest = Some(
            host.commit_cluster_kv_txn_with_quorum_async(
                paths,
                state
                    .as_mut()
                    .expect("cluster state is initialized before connectivity publish"),
                KvTxn {
                    compares: Vec::new(),
                    success,
                    failure: Vec::new(),
                },
            )
            .await?,
        );
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
    Ok(true)
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

pub fn cluster_semantic_txn_no_effect_response(
    paths: &ClusterPaths,
    txn: &KvTxn,
    now: OffsetDateTime,
) -> Result<Option<(KvSnapshot, KvTxnResponse)>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let succeeded = txn
        .compares
        .iter()
        .all(|compare| sinomo_kv::cluster_kv_snapshot_compare(&snapshot, compare));
    let ops = if succeeded {
        txn.success.as_slice()
    } else {
        txn.failure.as_slice()
    };
    let mut responses = Vec::with_capacity(ops.len());
    for op in ops {
        let Some(response) = cluster_semantic_op_no_effect_response(&snapshot, op, now)? else {
            return Ok(None);
        };
        responses.push(response);
    }

    Ok(Some((
        snapshot.clone(),
        KvTxnResponse {
            succeeded,
            revision: snapshot.revision,
            responses,
        },
    )))
}

fn cluster_semantic_op_no_effect_response(
    snapshot: &KvSnapshot,
    op: &KvOp,
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    match op {
        KvOp::Put {
            key,
            value,
            lease_id,
        } if lease_id.is_none() => {
            if let Some(entry) = snapshot.get(key)
                && entry.value.as_slice() == value.as_slice()
                && entry.lease_id.is_none()
            {
                return Ok(Some(sinomo_kv::KvOpResponse::Put {
                    key: key.clone(),
                    revision: entry.modify_revision,
                }));
            }
            if let Some(response) = cluster_member_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_capability_policy_put_no_effect_response(snapshot, key, value)?
            {
                return Ok(Some(response));
            }
            if let Some(response) = cluster_endpoint_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) = cluster_identity_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_raft_config_put_no_effect_response(snapshot, key, value)?
            {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_connectivity_put_no_effect_response(snapshot, key, value, now)?
            {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_network_info_put_no_effect_response(snapshot, key, value, now)?
            {
                return Ok(Some(response));
            }
            cluster_node_status_put_no_effect_response(snapshot, key, value)
        }
        KvOp::Delete { key } => {
            Ok(snapshot
                .get(key)
                .is_none()
                .then(|| sinomo_kv::KvOpResponse::Delete {
                    key: key.clone(),
                    deleted: false,
                }))
        }
        KvOp::DeletePrefix { prefix } => Ok(snapshot
            .range_prefix_limited(prefix, 1)?
            .is_empty()
            .then(|| sinomo_kv::KvOpResponse::DeletePrefix {
                prefix: prefix.clone(),
                deleted_count: 0,
            })),
        _ => Ok(None),
    }
}

fn cluster_member_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/members/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |member: &ClusterMemberRecord| key == cluster_member_key(&member.node_id),
        cluster_member_stable_eq,
    ))
}

fn cluster_capability_policy_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/policy/capabilities/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |policy: &ClusterCapabilityPolicyRecord| {
            key == cluster_capability_policy_key(&policy.node_id)
        },
        cluster_capability_policy_stable_eq,
    ))
}

fn cluster_endpoint_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/endpoints/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |endpoint: &ClusterEndpointRecord| key == cluster_endpoint_key(&endpoint.node_id, endpoint),
        cluster_endpoint_stable_eq,
    ))
}

fn cluster_identity_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if key != cluster_identity_key() {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |_identity: &ClusterIdentityRecord| true,
        cluster_identity_record_stable_eq,
    ))
}

fn cluster_raft_config_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if key != sinomo_kv::cluster_kv_raft_config_key() {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |_config: &ClusterKvRaftConfigRecord| true,
        sinomo_kv::cluster_kv_raft_config_record_stable_eq,
    ))
}

fn cluster_connectivity_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/network/connectivity/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |record: &ClusterConnectivityRecord| {
            key == cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            )
        },
        |existing, record| {
            cluster_connectivity_stable_eq(existing, record)
                && !cluster_network_record_should_refresh(existing.expires_at, now)
        },
    ))
}

fn cluster_network_info_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/network/info/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |info: &ClusterNetworkInfoRecord| key == cluster_network_info_key(&info.node_id),
        |existing, info| {
            cluster_network_info_stable_eq(existing, info)
                && !cluster_network_record_should_refresh(existing.expires_at, now)
        },
    ))
}

fn cluster_node_status_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/node-status/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |status: &ClusterNodeStatusRecord| key == cluster_node_status_key(&status.node_id),
        cluster_node_status_stable_eq,
    ))
}

fn cluster_semantic_stable_put_no_effect_response<T, K, S>(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    key_matches_record: K,
    stable_eq: S,
) -> Option<sinomo_kv::KvOpResponse>
where
    T: serde::de::DeserializeOwned,
    K: FnOnce(&T) -> bool,
    S: FnOnce(&T, &T) -> bool,
{
    let Ok(record) = sinomo_kv::decode_kv_value::<T>(value) else {
        return None;
    };
    if !key_matches_record(&record) {
        return None;
    }
    let entry = snapshot.get(key)?;
    let Ok(existing) = entry.decode::<T>() else {
        return None;
    };
    stable_eq(&existing, &record).then(|| sinomo_kv::KvOpResponse::Put {
        key: key.to_string(),
        revision: entry.modify_revision,
    })
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

pub fn expire_cluster_network_records_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkRecordExpiryHost,
{
    let (compares, success) = expire_cluster_network_record_ops(paths, state, now)?;
    if success.is_empty() {
        return Ok(None);
    }
    let mut local_state = read_cluster_state_without_kv(paths)?;
    host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut local_state,
        KvTxn {
            compares,
            success,
            failure: Vec::new(),
        },
    )
    .map(Some)
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

pub fn publish_local_members_via_voter_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishHost,
) -> Result<(), NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(());
    }
    if !options.force_kv_snapshot_sync
        && cluster_member_publish_matches_kv(paths, &members).unwrap_or(false)
    {
        return Ok(());
    }
    let request = ClusterMemberPublishRequest {
        members,
        local_kv_revision: local_cluster_kv_snapshot_revision(paths),
    };
    let mut failures = Vec::new();
    for endpoint in cluster_member_publish_targets_voter_first(paths, state) {
        match host.send_cluster_member_publish(&endpoint, &request, options.member_publish_timeout)
        {
            Ok(response) if response.accepted => {
                if let Some(snapshot) = response.kv_snapshot {
                    write_imported_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
                }
                return Ok(());
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "local node is not a voter and no reachable voter or bridge accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub async fn publish_local_members_via_voter_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishAsyncHost,
) -> Result<(), NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(());
    }
    if !options.force_kv_snapshot_sync {
        let paths_for_match = paths.clone();
        let members_for_match = members.clone();
        let already_published = runtime
            .spawn_blocking(move || {
                cluster_member_publish_matches_kv(&paths_for_match, &members_for_match)
                    .unwrap_or(false)
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster local member publish match".to_string(),
                detail: format!("member publish match task failed: {source}"),
            })?;
        if already_published {
            return Ok(());
        }
    }
    let paths_for_targets = paths.clone();
    let state_for_targets = state.clone();
    let targets = runtime
        .spawn_blocking(move || {
            cluster_member_publish_targets_voter_first(&paths_for_targets, &state_for_targets)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish target selection".to_string(),
            detail: format!("member publish target selection task failed: {source}"),
        })?;
    let paths_for_revision = paths.clone();
    let local_kv_revision = runtime
        .spawn_blocking(move || local_cluster_kv_snapshot_revision(&paths_for_revision))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish revision read".to_string(),
            detail: format!("member publish revision read task failed: {source}"),
        })?;
    let request = ClusterMemberPublishRequest {
        members,
        local_kv_revision,
    };
    let mut failures = Vec::new();
    for endpoint in targets {
        match host
            .send_cluster_member_publish_async(&endpoint, &request, options.member_publish_timeout)
            .await
        {
            Ok(response) if response.accepted => {
                if let Some(snapshot) = response.kv_snapshot {
                    let paths_for_write = paths.clone();
                    runtime
                        .spawn_blocking(move || {
                            write_imported_cluster_kv_snapshot_if_changed(
                                &paths_for_write,
                                &snapshot,
                            )
                        })
                        .await
                        .map_err(|source| NetworkError::Io {
                            operation: "cluster local member publish snapshot import".to_string(),
                            detail: format!("member publish snapshot import task failed: {source}"),
                        })??;
                }
                return Ok(());
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "local node is not a voter and no reachable voter or bridge accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn publish_local_members_to_quorum_if_needed_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishHost,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(None);
    }
    if cluster_member_publish_matches_kv(paths, &members).unwrap_or(false) {
        return Ok(None);
    }
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum(paths, state)
    {
        let mut commit_state = state.clone();
        return host
            .commit_cluster_kv_txn_with_quorum(
                paths,
                &mut commit_state,
                cluster_member_publish_txn_from_kv(paths, &members)?,
            )
            .map(Some);
    }
    publish_local_members_via_voter_with_host(paths, state, options, host)?;
    Ok(None)
}

pub async fn publish_local_members_to_quorum_if_needed_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishAsyncHost,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(None);
    }
    let paths_for_match = paths.clone();
    let members_for_match = members.clone();
    let already_published = runtime
        .spawn_blocking(move || {
            cluster_member_publish_matches_kv(&paths_for_match, &members_for_match).unwrap_or(false)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish match".to_string(),
            detail: format!("member publish match task failed: {source}"),
        })?;
    if already_published {
        return Ok(None);
    }
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum_async(paths, state).await?
    {
        let paths_for_txn = paths.clone();
        let members_for_txn = members.clone();
        let txn = runtime
            .spawn_blocking(move || {
                cluster_member_publish_txn_from_kv(&paths_for_txn, &members_for_txn)
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster local member publish txn".to_string(),
                detail: format!("member publish txn task failed: {source}"),
            })??;
        let mut commit_state = state.clone();
        return host
            .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
            .await
            .map(Some);
    }
    publish_local_members_via_voter_async_with_host(runtime, paths, state, options, host).await?;
    Ok(None)
}

enum PreparedClusterMemberPublish {
    Response(ClusterMemberPublishResponse),
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
    },
}

fn prepare_cluster_member_publish_request(
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
) -> Result<PreparedClusterMemberPublish, NetworkError> {
    let state = read_cluster_state(paths)?;
    if request.members.is_empty() {
        return Ok(PreparedClusterMemberPublish::Response(
            ClusterMemberPublishResponse {
                accepted: false,
                detail: "member publish request was empty".to_string(),
                state,
                kv_snapshot: None,
            },
        ));
    }
    for member in &request.members {
        if member.cluster_id != state.cluster_id {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member {} cluster `{}` does not match local cluster `{}`",
                        member.node_id, member.cluster_id, state.cluster_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
        if !cluster_member_publish_authorized_for_signer(&member.node_id, signer_node_id) {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member publisher {} cannot publish member {}",
                        signer_node_id, member.node_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
        if !state.members.iter().any(|existing| {
            existing.node_id == member.node_id
                && existing.node_public_key == member.node_public_key
                && existing.state.is_authorized()
        }) {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member {} is not an authorized cluster member with the published public key",
                        member.node_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
    }
    if cluster_member_publish_matches_kv(paths, &request.members)? {
        return Ok(PreparedClusterMemberPublish::Response(
            ClusterMemberPublishResponse {
                accepted: true,
                detail: "member endpoints already current".to_string(),
                state,
                kv_snapshot: cluster_member_publish_response_snapshot(
                    paths,
                    request.local_kv_revision,
                )?,
            },
        ));
    }
    let txn = cluster_member_publish_txn_from_kv(paths, &request.members)?;
    Ok(PreparedClusterMemberPublish::Commit { state, txn })
}

/// The local node's current cluster KV snapshot revision, if one is readable;
/// `None` asks the voter to ship its full snapshot back.
fn local_cluster_kv_snapshot_revision(paths: &ClusterPaths) -> Option<KvRevision> {
    read_cluster_kv_current_snapshot(paths)
        .ok()
        .map(|snapshot| snapshot.revision)
}

/// The KV snapshot a member-publish response should carry back to the
/// publisher: nothing when the publisher reported exactly our revision, the
/// full current snapshot otherwise. Any MISMATCH ships the snapshot — not
/// just "publisher behind" — because the publisher's counter can evolve
/// independently of ours (locally-applied writes before or during the join)
/// and a diverged-ahead publisher would otherwise never converge. Reporting
/// no revision at all means the publisher has no readable snapshot (or
/// predates the hint) and also receives the full snapshot; the importer
/// deduplicates unchanged content.
fn cluster_member_publish_response_snapshot(
    paths: &ClusterPaths,
    publisher_revision: Option<KvRevision>,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    if publisher_revision.is_some_and(|revision| revision == snapshot.revision) {
        return Ok(None);
    }
    Ok(Some(snapshot))
}

pub fn handle_cluster_member_publish_request_with_host<H>(
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
    host: &mut H,
) -> Result<ClusterMemberPublishResponse, H::Error>
where
    H: ClusterMemberPublishHost,
{
    let (state, txn) = match prepare_cluster_member_publish_request(paths, request, signer_node_id)
        .map_err(H::Error::from)?
    {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn) {
        Ok(snapshot) => Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints published".to_string(),
            state: read_cluster_state(paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
        }),
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub async fn handle_cluster_member_publish_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
    host: &mut H,
) -> Result<ClusterMemberPublishResponse, NetworkError>
where
    H: ClusterMemberMutationAsyncHost + Send,
{
    let paths_for_prepare = paths.clone();
    let signer_node_id = signer_node_id.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_member_publish_request(&paths_for_prepare, request, &signer_node_id)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster member publish prepare".to_string(),
            detail: format!("member publish prepare task failed: {source}"),
        })??;
    let (state, txn) = match prepared {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host
        .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
    {
        Ok(snapshot) => {
            let paths_for_state = paths.clone();
            let fallback_state = state.clone();
            let response_state = runtime
                .spawn_blocking(move || {
                    read_cluster_state(&paths_for_state).unwrap_or(fallback_state)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster member publish state refresh".to_string(),
                    detail: format!("member publish state refresh task failed: {source}"),
                })?;
            Ok(ClusterMemberPublishResponse {
                accepted: true,
                detail: "member endpoints published".to_string(),
                state: response_state,
                kv_snapshot: Some(snapshot),
            })
        }
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub fn cluster_member_publish_authorized_for_signer(
    member_node_id: &NodeId,
    signer_node_id: &str,
) -> bool {
    if member_node_id.as_str() == signer_node_id {
        return true;
    }
    if member_node_id
        .as_str()
        .starts_with(&format!("{signer_node_id}-"))
    {
        return true;
    }
    NodeId::new(signer_node_id)
        .map(|signer| shadow_daemon_node_id(&signer) == *member_node_id)
        .unwrap_or(false)
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

pub fn cluster_member_key(node_id: &NodeId) -> String {
    format!("/cluster/members/{node_id}")
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

pub fn publish_cluster_invitation_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &sinomo_core::ClusterJoinInvite,
    host: &mut H,
) -> Result<KvSnapshot, H::Error>
where
    H: ClusterInvitePublishHost,
{
    let txn = if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        cluster_invitation_publish_txn_from_kv(paths, invite)?
    } else {
        cluster_invitation_publish_txn(invite)?
    };
    if txn.success.is_empty() {
        return read_cluster_kv_current_snapshot(paths).map_err(Into::into);
    }
    let mut commit_state = state.clone();
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

enum PreparedClusterInvitePublish {
    Response(ClusterInvitePublishResponse),
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
    },
}

fn prepare_cluster_invite_publish_request(
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
) -> Result<PreparedClusterInvitePublish, NetworkError> {
    let state = read_cluster_state(paths)?;
    if request.invite.cluster_id != state.cluster_id {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite cluster `{}` does not match local cluster `{}`",
                    request.invite.cluster_id, state.cluster_id
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if request.invite.expires_at <= now {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite expired at {}",
                    request
                        .invite
                        .expires_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| "unknown".to_string())
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&request.invite) {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!("invite signature rejected: {error}"),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite issuer {} is not an authorized cluster member",
                    request.invite.issuer_node_id
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if signer_node_id != request.invite.issuer_node_id.as_str()
        || signer_public_key != request.invite.issuer_public_key
    {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: "invite publisher does not match invite issuer".to_string(),
                state,
                kv_snapshot: None,
            },
        ));
    }
    let txn = if sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        cluster_invitation_publish_txn_from_kv(paths, &request.invite)?
    } else {
        cluster_invitation_publish_txn(&request.invite)?
    };
    if txn.success.is_empty() {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state,
                kv_snapshot: Some(read_cluster_kv_current_snapshot(paths)?),
            },
        ));
    }
    Ok(PreparedClusterInvitePublish::Commit { state, txn })
}

pub fn handle_cluster_invite_publish_request_with_host<H>(
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<ClusterInvitePublishResponse, H::Error>
where
    H: ClusterInvitePublishHost,
{
    let (state, txn) = match prepare_cluster_invite_publish_request(
        paths,
        request,
        signer_node_id,
        signer_public_key,
        now,
    )
    .map_err(H::Error::from)?
    {
        PreparedClusterInvitePublish::Response(response) => return Ok(response),
        PreparedClusterInvitePublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn) {
        Ok(snapshot) => Ok(ClusterInvitePublishResponse {
            accepted: true,
            detail: "invite published".to_string(),
            state: read_cluster_state(paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
        }),
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub async fn handle_cluster_invite_publish_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<ClusterInvitePublishResponse, NetworkError>
where
    H: ClusterMemberMutationAsyncHost + Send,
{
    let paths_for_prepare = paths.clone();
    let signer_node_id = signer_node_id.to_string();
    let signer_public_key = signer_public_key.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_invite_publish_request(
                &paths_for_prepare,
                request,
                &signer_node_id,
                &signer_public_key,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster invite publish prepare".to_string(),
            detail: format!("invite publish prepare task failed: {source}"),
        })??;
    let (state, txn) = match prepared {
        PreparedClusterInvitePublish::Response(response) => return Ok(response),
        PreparedClusterInvitePublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host
        .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
    {
        Ok(snapshot) => {
            let paths_for_state = paths.clone();
            let fallback_state = state.clone();
            let response_state = runtime
                .spawn_blocking(move || {
                    read_cluster_state(&paths_for_state).unwrap_or(fallback_state)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster invite publish state refresh".to_string(),
                    detail: format!("invite publish state refresh task failed: {source}"),
                })?;
            Ok(ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state: response_state,
                kv_snapshot: Some(snapshot),
            })
        }
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
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

fn local_daemon_iroh_node_ids(state: &ClusterBootstrapState) -> [NodeId; 2] {
    [
        state.local_node_id.clone(),
        shadow_daemon_node_id(&state.local_node_id),
    ]
}

fn apply_local_iroh_endpoint_record_to_state(
    state: &mut ClusterBootstrapState,
    record: crate::IrohEndpointRecord,
) -> bool {
    let endpoint = record.endpoint();
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)
    else {
        return false;
    };
    let mut changed = upsert_member_endpoint(member, endpoint);
    changed |= cleanup_member_iroh_endpoints(member);
    changed
}

pub fn refresh_local_iroh_endpoints_from_disk(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timestamp: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let mut changed = false;
    for node_id in local_daemon_iroh_node_ids(state) {
        let Ok(record) = crate::read_iroh_endpoint_record(paths, &node_id) else {
            continue;
        };
        changed |= apply_local_iroh_endpoint_record_to_state(state, record);
    }
    if changed {
        state.updated_at = timestamp;
    }
    Ok(changed)
}

pub async fn refresh_local_iroh_endpoints_from_disk_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timestamp: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let mut changed = false;
    for node_id in local_daemon_iroh_node_ids(state) {
        let Ok(record) = crate::read_iroh_endpoint_record_async(runtime, paths, &node_id).await
        else {
            continue;
        };
        changed |= apply_local_iroh_endpoint_record_to_state(state, record);
    }
    if changed {
        state.updated_at = timestamp;
    }
    Ok(changed)
}

pub fn apply_cluster_iroh_endpoint_advertisement(
    state: &mut ClusterBootstrapState,
    record: crate::IrohEndpointRecord,
    persisted_endpoints: Option<&[ClusterEndpointRecord]>,
    timestamp: OffsetDateTime,
) -> Option<ClusterMemberRecord> {
    let endpoint = record.endpoint();
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)?;
    upsert_member_endpoint(member, endpoint);
    cleanup_member_iroh_endpoints(member);
    if persisted_endpoints == Some(member.endpoints.as_slice()) {
        return None;
    }
    state.updated_at = timestamp;
    Some(member.clone())
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
            .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
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
    durable_file::read_optional_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(network_durable_file_error)
}

fn read_object_file_bytes(path: &PathBuf) -> Result<Vec<u8>, NetworkError> {
    durable_file::read_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(network_durable_file_error)
}

pub(crate) fn write_object_file<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<(), NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    let result = durable_file::write_object_file_bytes_if_changed(
        path,
        &desired,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "tmp"),
    )
    .map_err(network_durable_file_error)?;
    if let (Some(observer), Some(read_bytes)) = (io_observer(), result.compare_read_bytes) {
        observer.on_disk_read(read_bytes);
    }
    if let Some(observer) = io_observer() {
        if result.changed {
            observer.on_object_file_actual_write();
            observer.on_disk_write(desired.len() as u64);
        } else {
            observer.on_object_file_skipped_write();
        }
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
#[cfg(any(test, feature = "iroh-transport"))]
pub(crate) fn write_object_file_create_new_serialized<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<bool, NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    #[cfg(all(test, feature = "iroh-transport"))]
    record_write_object_file_create_new_attempt_for_test(path);
    let result = durable_file::write_object_file_bytes_create_new_serialized(
        path,
        &desired,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "create-new-tmp"),
        |temp_path, path| create_new_link_or_unsupported_for_test(temp_path, &path.to_path_buf()),
    )
    .map_err(network_durable_file_error)?;
    if let Some(observer) = io_observer() {
        if result.changed {
            observer.on_object_file_actual_write();
            observer.on_disk_write(desired.len() as u64);
        } else {
            observer.on_object_file_skipped_write();
        }
    }
    Ok(result.changed)
}

#[cfg(any(test, feature = "iroh-transport"))]
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

fn network_durable_file_error(error: durable_file::DurableFileError) -> NetworkError {
    NetworkError::StorageIo {
        path: error.path().to_string(),
        detail: error.detail().to_string(),
    }
}
