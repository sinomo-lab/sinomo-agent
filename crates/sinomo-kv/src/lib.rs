//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft replication core for tests,
//! local development, and daemon integration.
//!
//! This crate owns KV storage and Raft leader/election/log semantics. Cross-node
//! transmission should be supplied by network-owned transport capabilities; KV
//! code should not decide routes or interpret cluster connectivity topology. KV
//! can persist connectivity records as ordinary key/value entries, but those
//! records remain network-domain facts. KV RPC route/category/body payload specs
//! and local read/watch route dispatch live here; the cluster network wraps them
//! in its generic object-RPC envelope when transmitting between peers.

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, Id,
};
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_storage::durable_file::{self, file_ino};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use time::OffsetDateTime;

pub const KV_OBJECT_VERSION_ID: u16 = 1;
pub const KV_WAL_OBJECT_VERSION_ID: u16 = 2;
const MAX_RETAINED_EVENTS: usize = 4096;
const MAX_PENDING_EVENTS_PER_WATCH: usize = MAX_RETAINED_EVENTS;
const MAX_ACTIVE_WATCHES: usize = 1024;
const MAX_ACTIVE_LEASES: usize = 4096;
pub const MAX_KV_TXN_COMPARES: usize = 1024;
pub const MAX_KV_TXN_OPS_PER_BRANCH: usize = 4096;
pub const MAX_KV_MUTATION_EVENTS_PER_COMMAND: usize = 4096;
pub const MAX_DELETE_PREFIX_KEYS: usize = MAX_KV_MUTATION_EVENTS_PER_COMMAND;
pub const MAX_KEYS_PER_LEASE: usize = MAX_KV_MUTATION_EVENTS_PER_COMMAND;
pub const MAX_KV_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_KV_COMMAND_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub const CLUSTER_KV_RPC_CATEGORY: &str = "cluster.kv";
pub const CLUSTER_KV_SNAPSHOT_ROUTE: &str = "/cluster/kv/snapshot";
pub const CLUSTER_KV_TXN_ROUTE: &str = "/cluster/kv/txn";
pub const CLUSTER_KV_RANGE_PREFIX_ROUTE: &str = "/cluster/kv/range-prefix";
pub const CLUSTER_KV_WATCH_PREFIX_ROUTE: &str = "/cluster/kv/watch-prefix";
pub const CLUSTER_KV_WATCH_DRAIN_ROUTE: &str = "/cluster/kv/watch-drain";
pub const CLUSTER_KV_WATCH_CANCEL_ROUTE: &str = "/cluster/kv/watch-cancel";
pub const CLUSTER_KV_WATCH_EVENTS_ROUTE: &str = "/cluster/kv/watch-events";
pub const CLUSTER_RAFT_RPC_CATEGORY: &str = "cluster.raft";
pub const CLUSTER_RAFT_APPEND_ROUTE: &str = "/cluster/raft/append";
pub const CLUSTER_RAFT_VOTE_ROUTE: &str = "/cluster/raft/vote";
pub const CLUSTER_KV_LOCAL_READ_WATCH_ROUTES: &[&str] = &[
    CLUSTER_KV_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_WATCH_CANCEL_ROUTE,
];
pub const CLUSTER_KV_RPC_ROUTES: &[&str] = &[
    CLUSTER_KV_SNAPSHOT_ROUTE,
    CLUSTER_KV_TXN_ROUTE,
    CLUSTER_KV_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_WATCH_CANCEL_ROUTE,
    CLUSTER_KV_WATCH_EVENTS_ROUTE,
];
pub const CLUSTER_KV_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_RANGE_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_DRAIN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_CANCEL_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_KV_RPC_CATEGORY),
];
pub const CLUSTER_RAFT_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
    (CLUSTER_RAFT_VOTE_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
];
pub const MAX_CLUSTER_KV_WATCH_EVENTS: usize = 256;
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS;
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG: u64 =
    (MAX_CLUSTER_KV_WATCH_EVENTS / 2) as u64;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS / 2;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE: Duration = Duration::from_secs(60);
pub const MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS: usize = MAX_CLUSTER_KV_WATCH_EVENTS * 2;
pub const MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES: u64 = 1024 * 1024;
pub const MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CLUSTER_KV_ACTIVE_WATCHES: usize = 1024;
pub const MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const MAX_CLUSTER_KV_WATCH_DRAIN_WAIT: Duration = Duration::from_secs(30);
pub const CLUSTER_KV_SNAPSHOT_RPC_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES: usize = 4096;
pub const CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const MAX_CLUSTER_KV_MATERIALIZED_EVENTS: usize = 64;
pub const CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG: u64 = 64;
pub const CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL: Duration = Duration::from_secs(300);
pub const MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS: usize = 64;
/// Committed log entries kept in memory after a local snapshot compaction. A
/// follower that has fallen at most this many entries behind can be caught up
/// with small incremental appends instead of a full snapshot install. The tail is
/// redundant with the snapshot (it is dropped on restart) and only the leader's
/// in-process log carries it, so it never changes the wire protocol or on-disk
/// recovery semantics.
///
/// Compaction triggers must stay *above* this value: compaction only drops entries
/// older than this tail, so triggering at or below it rewrites the WAL to the same
/// tail every time (a busy-idle loop). See `compact_cluster_raft_node_if_needed`.
pub const RAFT_COMPACTION_RETAINED_ENTRIES: u64 = 256;
pub const CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS: usize = 3;
pub const CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP: u64 = 64;
static CLUSTER_KV_RAFT_OPERATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES: usize = MAX_ACTIVE_WATCHES * 2 + 128;
static CLUSTER_KV_WATCH_NOTIFIER_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_WATCH_NOTIFIERS: LazyLock<Mutex<HashMap<PathBuf, ClusterKvWatchNotifierEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST: AtomicU64 =
    AtomicU64::new(MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES);
static CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES;
const RAFT_NODE_LOCK_WAIT: Duration = Duration::from_secs(2);
const RAFT_NODE_LOCK_RETRY_INITIAL: Duration = Duration::from_millis(2);
const RAFT_NODE_LOCK_RETRY_MAX: Duration = Duration::from_millis(40);
static OBJECT_FILE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static KV_READ_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);

/// Cache of last-written object files. The cluster directory holding these WAL /
/// snapshot files can be shared by the main and shadow daemons, so the fast-path
/// skip is guarded by a `stat`: an entry is trusted only when the file's length,
/// mtime, and inode still match what we wrote, so a write by another process is
/// detected and falls back to a real read + compare.
static LAST_WRITTEN_OBJECT_FILES: LazyLock<durable_file::ObjectFileWriteMarkCache> =
    LazyLock::new(|| durable_file::ObjectFileWriteMarkCache::new(FILE_CACHE_CAPACITY));

pub type ClusterKvRaftOperationGuard = MutexGuard<'static, ()>;

pub fn lock_cluster_kv_raft_operation() -> Result<ClusterKvRaftOperationGuard, KvError> {
    CLUSTER_KV_RAFT_OPERATION_LOCK
        .lock()
        .map_err(|error| KvError::Io {
            path: "cluster kv raft operation lock".to_string(),
            detail: format!("cluster kv raft operation lock poisoned: {error}"),
        })
}

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-snapshot.sobj",
        ObjectSchema::new("KvSnapshot", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("revision", "KvRevision")
            .field("entries", "Vec<KvEntry>")
            .field("events", "Vec<KvEvent>"),
    )?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watch-events.sobj",
        ObjectSchema::new("Vec<KvEvent>", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_object_log("sinomo-kv", StorageRing::R1, "cluster/watch-events.sobjlog")?;
    layout.register_file(sinomo_storage::StorageFileContract::binary(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches.lock",
    )?)?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches-next.sobj",
        ObjectSchema::new(
            "ClusterKvWatchAllocatorState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("next_watch_id", "u64"),
    )?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/hard-state.sobj",
        ObjectSchema::new("RaftHardState", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/snapshot.sobj",
        ObjectSchema::new("RaftSnapshot", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_object_log("sinomo-kv", StorageRing::R1, "cluster/kv-raft/wal.sobjlog")?;
    layout.register_escape_path(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft",
        "durable raft nodes use dynamic node-id subdirectories for locks, WALs, hard state, and snapshots",
    )?;
    layout.register_escape_path(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches",
        "dynamic cluster KV watch registration object files keyed by watch id",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvPaths {
    pub root: PathBuf,
    pub snapshot: PathBuf,
    pub raft: PathBuf,
    pub watches: PathBuf,
    pub watches_lock: PathBuf,
    pub watches_next_id: PathBuf,
    pub watch_events: PathBuf,
    pub watch_events_log: PathBuf,
}

impl ClusterKvPaths {
    pub fn new(cluster_root: impl Into<PathBuf>) -> Self {
        let root = cluster_root.into();
        Self {
            snapshot: root.join("kv-snapshot.sobj"),
            raft: root.join("kv-raft"),
            watches: root.join("watches"),
            watches_lock: root.join("watches.lock"),
            watches_next_id: root.join("watches-next.sobj"),
            watch_events: root.join("watch-events.sobj"),
            watch_events_log: root.join("watch-events.sobjlog"),
            root,
        }
    }

    pub fn local_cache(&self) -> KvLocalCache {
        KvLocalCache::new(self.snapshot.clone(), self.raft.clone())
            .with_max_materialized_events(MAX_CLUSTER_KV_MATERIALIZED_EVENTS)
    }

    pub fn raft_wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        KvWalPaths::new(self.raft.join(node_id.as_str()))
    }

    pub fn watch_path(&self, watch_id: WatchId) -> PathBuf {
        self.watches.join(format!("watch-{}.sobj", watch_id.0))
    }

    pub fn raft_store(&self) -> ClusterKvRaftStore {
        ClusterKvRaftStore::new(self.clone(), default_cluster_kv_raft_compaction_policy())
    }
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
pub struct ClusterKvWatchObservedGeneration {
    pub root: u64,
    pub watch: u64,
}

fn cluster_kv_watch_notify_key(paths: &ClusterKvPaths) -> PathBuf {
    paths.root.clone()
}

fn cluster_kv_watch_notify_key_for_watch(paths: &ClusterKvPaths, watch_id: WatchId) -> PathBuf {
    paths.watch_path(watch_id)
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

pub fn cluster_kv_watch_generation(paths: &ClusterKvPaths) -> u64 {
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

pub fn cluster_kv_watch_observed_generation(
    paths: &ClusterKvPaths,
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

pub fn notify_cluster_kv_watchers(paths: &ClusterKvPaths) {
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

pub fn notify_cluster_kv_watchers_for_watch_ids(paths: &ClusterKvPaths, watch_ids: &[WatchId]) {
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

pub fn notify_cluster_kv_watchers_for_events(paths: &ClusterKvPaths, events: &[KvEvent]) {
    if events.is_empty() {
        return;
    }
    match list_cluster_kv_watch_registrations(paths)
        .map(|registrations| cluster_kv_watch_ids_matching_events(&registrations, events))
    {
        Ok(watch_ids) => notify_cluster_kv_watchers_for_watch_ids(paths, &watch_ids),
        Err(error) => {
            eprintln!(
                "warning: waking all cluster kv watch waiters after active watch lookup failed for {}: {}",
                paths.watches.display(),
                error
            );
            notify_cluster_kv_watchers(paths);
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

pub fn wait_for_cluster_kv_watch_change(
    paths: &ClusterKvPaths,
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

pub fn wait_for_cluster_kv_watch_change_for_watch(
    paths: &ClusterKvPaths,
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

pub fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES
}

pub fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterKvPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    cluster_kv_watch_notifier_has_raw_key_for_test(&key)
}

pub fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &Path) -> bool {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| guard.contains_key(path))
        .unwrap_or(false)
}

pub fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterKvPaths) -> usize {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| guard.get(&key).map(|entry| entry.waiters))
        .unwrap_or(0)
}

pub fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &Path) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        guard.retain(|path, _| !path.starts_with(root));
    }
}

pub fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterKvPaths) {
    notify_cluster_kv_watchers(paths);
}

pub fn record_cluster_kv_watch_notifier_waiter_key_for_test(path: PathBuf) {
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

fn record_cluster_kv_watch_unchanged_wakeup_for_test(path: &PathBuf) {
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        *wakeups.entry(path.clone()).or_default() += 1;
    }
}

pub fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        wakeups.remove(&key);
    }
}

pub fn cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> u64 {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST
        .lock()
        .ok()
        .and_then(|wakeups| wakeups.get(&key).copied())
        .unwrap_or(0)
}

pub fn read_cluster_kv_cached_snapshot(paths: &ClusterKvPaths) -> Result<KvSnapshot, KvError> {
    paths.local_cache().cached_snapshot()
}

pub fn read_cluster_kv_current_snapshot_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<KvSnapshot, KvError> {
    paths
        .local_cache()
        .current_snapshot_without_materialized_refresh(node_id)
}

pub fn read_cluster_kv_current_snapshot_arc_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<Arc<KvSnapshot>, KvError> {
    paths
        .local_cache()
        .current_snapshot_arc_without_materialized_refresh(node_id)
}

pub fn read_cluster_kv_store_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<InProcessKvStore, KvError> {
    read_cluster_kv_current_snapshot_for_node(paths, node_id).map(InProcessKvStore::restore)
}

pub fn write_cluster_kv_snapshot(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<(), KvError> {
    write_cluster_kv_materialized_snapshot_if_changed(paths, snapshot).map(|_| ())
}

pub fn write_cluster_kv_materialized_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    let wrote = paths
        .local_cache()
        .write_snapshot_if_materialized_changed(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

pub fn write_cluster_kv_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    let wrote = paths.local_cache().write_snapshot_if_changed(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

pub fn write_imported_cluster_kv_snapshot_if_changed<F>(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    key_can_defer_materialized_snapshot: F,
) -> Result<bool, KvError>
where
    F: FnMut(&str) -> bool,
{
    if cluster_kv_imported_snapshot_can_defer_materialized_delta(
        paths,
        snapshot,
        key_can_defer_materialized_snapshot,
    )? {
        return Ok(false);
    }
    write_cluster_kv_snapshot_if_changed(paths, snapshot)
}

fn cluster_kv_imported_snapshot_can_defer_materialized_delta<F>(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    key_can_defer_materialized_snapshot: F,
) -> Result<bool, KvError>
where
    F: FnMut(&str) -> bool,
{
    let Some(existing) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(false);
    };
    if !cluster_kv_snapshot_delta_only_deferable(
        existing.as_ref(),
        snapshot,
        key_can_defer_materialized_snapshot,
    ) {
        return Ok(false);
    }
    Ok(cluster_kv_materialized_snapshot_is_fresh_enough_against(
        existing.revision,
        snapshot.revision,
        cluster_kv_materialized_snapshot_age(paths),
    ))
}

fn cluster_kv_materialized_snapshot_age(paths: &ClusterKvPaths) -> Option<Duration> {
    let Ok(metadata) = fs::metadata(&paths.snapshot) else {
        return None;
    };
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
}

pub fn read_cluster_kv_entry(
    paths: &ClusterKvPaths,
    key: &str,
) -> Result<Option<KvEntry>, KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(None);
    };
    Ok(snapshot.get(key).cloned())
}

pub fn read_cluster_kv_prefix_entries(
    paths: &ClusterKvPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, KvError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

pub fn read_cluster_kv_prefix_entries_limited(
    paths: &ClusterKvPaths,
    prefix: &str,
    max_entries: usize,
) -> Result<Vec<KvEntry>, KvError> {
    read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        paths,
        prefix,
        max_entries,
        CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

pub fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
    paths: &ClusterKvPaths,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Result<Vec<KvEntry>, KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(Vec::new());
    };
    snapshot.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
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
    pub fn legacy(next_watch_id: u64) -> Self {
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

impl ClusterKvWatchDrainRequest {
    pub fn wait_timeout(&self) -> Duration {
        let max_millis = MAX_CLUSTER_KV_WATCH_DRAIN_WAIT.as_millis() as u64;
        Duration::from_millis(self.wait_timeout_millis.min(max_millis))
    }
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

pub fn collect_cluster_kv_watch_drain_events(
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
        let event_bytes = cluster_kv_watch_event_payload_bytes(event);
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

pub fn cluster_kv_watch_registration_cursor_should_persist(
    drained_events: usize,
    next_revision: KvRevision,
    durable_next_revision: KvRevision,
    registration_file_unchanged: bool,
    registration_file_age: Option<Duration>,
) -> bool {
    if drained_events >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS {
        return true;
    }
    let revision_lag = next_revision.0.saturating_sub(durable_next_revision.0);
    if revision_lag >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG {
        return true;
    }
    if !registration_file_unchanged {
        return true;
    }
    match registration_file_age {
        Some(age) => age >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE,
        None => true,
    }
}

pub fn cluster_kv_watch_events_payload_bytes(events: &[KvEvent]) -> usize {
    events.iter().fold(0_usize, |total, event| {
        total.saturating_add(cluster_kv_watch_event_payload_bytes(event))
    })
}

pub fn cluster_kv_events_can_defer_materialized_snapshot(
    events: &[KvEvent],
    mut key_can_defer: impl FnMut(&str) -> bool,
) -> bool {
    !events.is_empty() && events.iter().all(|event| key_can_defer(&event.key))
}

pub fn cluster_kv_snapshot_delta_only_deferable(
    existing: &KvSnapshot,
    snapshot: &KvSnapshot,
    mut key_can_defer: impl FnMut(&str) -> bool,
) -> bool {
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
                        if !key_can_defer(&existing_entry.key) {
                            return false;
                        }
                        changed = true;
                        existing_entries.next();
                    }
                    std::cmp::Ordering::Greater => {
                        if !key_can_defer(&incoming_entry.key) {
                            return false;
                        }
                        changed = true;
                        incoming_entries.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if existing_entry.value != incoming_entry.value
                            || existing_entry.lease_id != incoming_entry.lease_id
                        {
                            if !key_can_defer(&incoming_entry.key) {
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
                if !key_can_defer(&existing_entry.key) {
                    return false;
                }
                changed = true;
                existing_entries.next();
            }
            (None, Some(incoming_entry)) => {
                if !key_can_defer(&incoming_entry.key) {
                    return false;
                }
                changed = true;
                incoming_entries.next();
            }
            (None, None) => return changed,
        }
    }
}

pub fn cluster_kv_materialized_snapshot_is_fresh_enough_against(
    existing_revision: KvRevision,
    snapshot_revision: KvRevision,
    materialized_snapshot_age: Option<Duration>,
) -> bool {
    if existing_revision > snapshot_revision {
        return false;
    }
    let revision_lag = snapshot_revision.0.saturating_sub(existing_revision.0);
    if revision_lag >= CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG {
        return false;
    }
    matches!(
        materialized_snapshot_age,
        Some(age) if age < CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchEventFanoutBatchLimit {
    EventCount {
        observed_events: usize,
        max_events: usize,
    },
    PayloadBytes {
        observed_bytes: usize,
        max_bytes: usize,
    },
}

impl ClusterKvWatchEventFanoutBatchLimit {
    pub fn detail(&self) -> String {
        match self {
            Self::EventCount {
                observed_events,
                max_events,
            } => {
                format!(
                    "watch event fan-out batch has {observed_events} events; max is {max_events}"
                )
            }
            Self::PayloadBytes {
                observed_bytes,
                max_bytes,
            } => {
                format!(
                    "watch event fan-out batch is {observed_bytes} payload bytes; max is {max_bytes}"
                )
            }
        }
    }
}

pub fn validate_cluster_kv_watch_event_fanout_batch(
    events: &[KvEvent],
) -> Result<(), ClusterKvWatchEventFanoutBatchLimit> {
    validate_cluster_kv_watch_event_fanout_batch_with_limits(
        events,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES,
    )
}

pub fn validate_cluster_kv_watch_event_fanout_batch_with_limits(
    events: &[KvEvent],
    max_events: usize,
    max_payload_bytes: usize,
) -> Result<(), ClusterKvWatchEventFanoutBatchLimit> {
    if events.len() > max_events {
        return Err(ClusterKvWatchEventFanoutBatchLimit::EventCount {
            observed_events: events.len(),
            max_events,
        });
    }
    let payload_bytes = cluster_kv_watch_events_payload_bytes(events);
    if payload_bytes > max_payload_bytes {
        return Err(ClusterKvWatchEventFanoutBatchLimit::PayloadBytes {
            observed_bytes: payload_bytes,
            max_bytes: max_payload_bytes,
        });
    }
    Ok(())
}

pub fn cluster_kv_watch_event_fanout_batches(events: &[KvEvent]) -> Vec<&[KvEvent]> {
    cluster_kv_watch_event_fanout_batches_with_limits(
        events,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES,
    )
}

pub fn cluster_kv_watch_event_fanout_batches_with_limits(
    events: &[KvEvent],
    max_events: usize,
    max_payload_bytes: usize,
) -> Vec<&[KvEvent]> {
    let mut batches = Vec::new();
    let mut start = 0_usize;
    while start < events.len() {
        let mut end = start;
        let mut payload_bytes = 0_usize;
        while end < events.len() {
            if end - start >= max_events {
                break;
            }
            let event_bytes = cluster_kv_watch_event_payload_bytes(&events[end]);
            let next_payload_bytes = payload_bytes.saturating_add(event_bytes);
            if end > start && next_payload_bytes > max_payload_bytes {
                break;
            }
            payload_bytes = next_payload_bytes;
            end += 1;
        }
        if end == start {
            end += 1;
        }
        batches.push(&events[start..end]);
        start = end;
    }
    batches
}

pub fn compact_cluster_kv_watch_events(mut events: Vec<KvEvent>) -> Vec<KvEvent> {
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

pub fn compact_cluster_kv_watch_events_incremental(
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

pub fn cluster_kv_watch_event_append_should_checkpoint(
    incoming_events: usize,
    appended_events: usize,
    current_log_records: usize,
    current_log_bytes: u64,
    appended_log_bytes: u64,
) -> bool {
    incoming_events > MAX_CLUSTER_KV_WATCH_EVENTS
        || current_log_records.saturating_add(appended_events)
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
        || current_log_bytes.saturating_add(appended_log_bytes)
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES
}

pub fn cluster_kv_events_contain(events: &[KvEvent], event: &KvEvent) -> bool {
    events.iter().any(|candidate| {
        candidate.revision == event.revision
            && candidate.key == event.key
            && candidate.kind == event.kind
    })
}

pub fn cluster_kv_events_contain_all(existing: &[KvEvent], incoming: &[KvEvent]) -> bool {
    incoming
        .iter()
        .all(|event| cluster_kv_events_contain(existing, event))
}

pub fn cluster_raft_append_request_needs_watch_event_fanout(
    request: &ClusterRaftAppendRequest,
    events: &[KvEvent],
) -> bool {
    if events.is_empty() {
        return false;
    }
    let Some(snapshot) = request.snapshot.as_ref() else {
        return false;
    };
    !cluster_kv_events_contain_all(&snapshot.kv.events, events)
}

pub fn cluster_kv_snapshot_compare(snapshot: &KvSnapshot, compare: &KvCompare) -> bool {
    match compare {
        KvCompare::Exists { key } => snapshot.get(key).is_some(),
        KvCompare::Missing { key } => snapshot.get(key).is_none(),
        KvCompare::ValueEquals { key, value } => snapshot
            .get(key)
            .is_some_and(|entry| entry.value.as_slice() == value.as_slice()),
        KvCompare::VersionEquals { key, version } => snapshot
            .get(key)
            .is_some_and(|entry| entry.version == *version),
        KvCompare::CreateRevisionEquals { key, revision } => snapshot
            .get(key)
            .is_some_and(|entry| entry.create_revision == *revision),
        KvCompare::ModifyRevisionEquals { key, revision } => snapshot
            .get(key)
            .is_some_and(|entry| entry.modify_revision == *revision),
        KvCompare::LeaseEquals { key, lease_id } => snapshot
            .get(key)
            .is_some_and(|entry| entry.lease_id == *lease_id),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedClusterKvWatchEventLogBatch {
    pub records: Vec<Vec<u8>>,
    pub bytes: u64,
}

pub fn open_cluster_kv_watch_event_log_for_append(paths: &ClusterKvPaths) -> Result<File, KvError> {
    let parent = paths
        .watch_events_log
        .parent()
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| KvError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    match OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&paths.watch_events_log)
    {
        Ok(file) => {
            sync_directory(parent)?;
            Ok(file)
        }
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .append(true)
            .open(&paths.watch_events_log)
            .map_err(|source| KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            }),
        Err(source) => Err(KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn lock_cluster_kv_watch_event_log_for_append(
    paths: &ClusterKvPaths,
    file: &File,
) -> Result<(), KvError> {
    file.lock_exclusive().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn encode_cluster_kv_watch_event_log_batch(
    events: &[KvEvent],
) -> Result<EncodedClusterKvWatchEventLogBatch, KvError> {
    let mut records = Vec::with_capacity(events.len());
    let mut bytes = 0_u64;
    for event in events {
        let record = sinomo_object_serde::to_bytes(event, KV_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })?;
        let len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
            detail: "cluster kv watch-event log record exceeds u32 length".to_string(),
        })?;
        bytes = bytes.saturating_add(4).saturating_add(u64::from(len));
        records.push(record);
    }
    Ok(EncodedClusterKvWatchEventLogBatch { records, bytes })
}

pub fn append_cluster_kv_watch_event_log_batch_to_file(
    paths: &ClusterKvPaths,
    file: &mut File,
    batch: &EncodedClusterKvWatchEventLogBatch,
) -> Result<u64, KvError> {
    if batch.records.is_empty() {
        return Ok(0);
    }
    for record in &batch.records {
        let len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
            detail: format!(
                "cluster kv watch-event log record for {} exceeds u32 length",
                paths.watch_events_log.display()
            ),
        })?;
        file.write_all(&len.to_be_bytes())
            .and_then(|()| file.write_all(record))
            .map_err(|source| KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    file.sync_data().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(batch.bytes)
}

pub fn truncate_cluster_kv_watch_event_log_file(
    paths: &ClusterKvPaths,
    file: &mut File,
) -> Result<(), KvError> {
    file.set_len(0).map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    file.sync_data().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn decode_cluster_kv_watch_event_log_record(record: &[u8]) -> Result<KvEvent, KvError> {
    sinomo_object_serde::from_bytes(record).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventsRead {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventLogRead {
    pub events: Vec<KvEvent>,
    pub records: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendCacheView {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub log_bytes: u64,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendReport {
    pub appended: Vec<KvEvent>,
    pub retained: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

pub fn read_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventsRead, KvError> {
    read_cluster_kv_watch_events_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_events_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventsRead, KvError>
where
    F: FnMut(u64),
{
    let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
    let log = read_cluster_kv_watch_event_log_with_observer(paths, on_log_read)?;
    let events =
        compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
    Ok(ClusterKvWatchEventsRead {
        events,
        log_records: log.records,
        append_safe: !log.truncated,
    })
}

pub fn read_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
) -> Result<Vec<KvEvent>, KvError> {
    read_optional_object(&paths.watch_events).map(|events| events.unwrap_or_default())
}

pub fn write_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
    events: &[KvEvent],
) -> Result<(), KvError> {
    let parent = paths
        .watch_events
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent).map_err(|source| KvError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    write_object_file(&paths.watch_events, &events)
}

pub fn append_cluster_kv_watch_events_to_storage(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
) -> Result<ClusterKvWatchEventAppendReport, KvError> {
    append_cluster_kv_watch_events_to_storage_with_observers(paths, incoming, cache, |_| {}, |_| {})
}

pub fn append_cluster_kv_watch_events_to_storage_with_observers<R, W>(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
    mut on_log_read: R,
    mut on_log_write: W,
) -> Result<ClusterKvWatchEventAppendReport, KvError>
where
    R: FnMut(u64),
    W: FnMut(u64),
{
    if incoming.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport::default());
    }

    let mut log_file = open_cluster_kv_watch_event_log_for_append(paths)?;
    lock_cluster_kv_watch_event_log_for_append(paths, &log_file)?;

    let (existing, log_records, log_bytes) =
        if let Some(cache) = cache.filter(|cache| cache.append_safe) {
            (cache.events.clone(), cache.log_records, cache.log_bytes)
        } else {
            let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
            let log = read_cluster_kv_watch_event_log_from_file_with_observer(
                paths,
                &mut log_file,
                &mut on_log_read,
            )?;
            let existing =
                compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
            if log.truncated {
                write_cluster_kv_watch_event_checkpoint(paths, &existing)?;
                truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
                (existing, 0, 0)
            } else {
                let log_bytes = log_file
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                (existing, log.records, log_bytes)
            }
        };

    let retained = compact_cluster_kv_watch_events_incremental(existing.clone(), incoming);
    let appended = retained
        .iter()
        .filter(|event| !cluster_kv_events_contain(&existing, event))
        .cloned()
        .collect::<Vec<_>>();
    if appended.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport {
            appended,
            retained,
            log_records,
            append_safe: true,
        });
    }

    let encoded_appended = encode_cluster_kv_watch_event_log_batch(&appended)?;
    let mut current_log_records = log_records;
    let should_checkpoint_without_append = cluster_kv_watch_event_append_should_checkpoint(
        incoming.len(),
        appended.len(),
        current_log_records,
        log_bytes,
        encoded_appended.bytes,
    );
    if should_checkpoint_without_append {
        write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
        truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
        current_log_records = 0;
    } else {
        let written = append_cluster_kv_watch_event_log_batch_to_file(
            paths,
            &mut log_file,
            &encoded_appended,
        )?;
        if written > 0 {
            on_log_write(written);
        }
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

    Ok(ClusterKvWatchEventAppendReport {
        appended,
        retained,
        log_records: current_log_records,
        append_safe: true,
    })
}

pub fn read_cluster_kv_watch_registration_file(
    path: &Path,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let Some(registration) = read_optional_object::<ClusterKvWatchRegistration>(path)? else {
        return Err(KvError::UnknownWatch { watch_id });
    };
    if registration.watch_id != watch_id {
        return Err(KvError::UnknownWatch { watch_id });
    }
    Ok(registration)
}

pub fn write_cluster_kv_watch_registration_file(
    path: &Path,
    registration: &ClusterKvWatchRegistration,
) -> Result<(), KvError> {
    write_object_file(path, registration)
}

pub fn write_cluster_kv_watch_registration_file_create_new(
    path: &Path,
    registration: &ClusterKvWatchRegistration,
) -> Result<bool, KvError> {
    let content =
        sinomo_object_serde::to_bytes(registration, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })?;
    write_object_file_bytes_create_new_serialized(path, &content)
}

pub fn remove_cluster_kv_watch_registration_file_if_exists(path: &Path) -> Result<bool, KvError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn read_cluster_kv_watch_allocator_state(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchAllocatorState, KvError> {
    let state = read_optional_object::<ClusterKvWatchAllocatorState>(&paths.watches_next_id)?
        .unwrap_or_else(|| ClusterKvWatchAllocatorState::legacy(1));
    Ok(ClusterKvWatchAllocatorState {
        next_watch_id: state.next_watch_id.max(1),
        active_watch_count: state.active_watch_count,
        pruned_compacted_revision: state.pruned_compacted_revision,
    })
}

pub fn write_cluster_kv_watch_allocator_state(
    paths: &ClusterKvPaths,
    state: &ClusterKvWatchAllocatorState,
) -> Result<(), KvError> {
    write_object_file(&paths.watches_next_id, state)
}

pub fn lock_cluster_kv_watch_allocator(paths: &ClusterKvPaths) -> Result<File, KvError> {
    fs::create_dir_all(&paths.root).map_err(|source| KvError::Io {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&paths.watches_lock)
        .map_err(|source| KvError::Io {
            path: paths.watches_lock.display().to_string(),
            detail: source.to_string(),
        })?;
    file.lock_exclusive().map_err(|source| KvError::Io {
        path: paths.watches_lock.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(file)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchScanRegistration {
    pub path: PathBuf,
    pub registration: ClusterKvWatchRegistration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchScanResult {
    pub scanned: bool,
    pub removed: usize,
    pub retained: usize,
    pub max_seen_id: u64,
    pub removed_paths: Vec<PathBuf>,
    pub retained_registrations: Vec<ClusterKvWatchScanRegistration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchRegistrationCreateReport {
    pub watch_path: PathBuf,
    pub registration: ClusterKvWatchRegistration,
    pub allocator: ClusterKvWatchAllocatorState,
    pub scan: Option<ClusterKvWatchScanResult>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchDrainCursorAction {
    Unchanged,
    CacheOnly { durable_next_revision: KvRevision },
    Persist,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchDrainRegistrationReport {
    pub registration: ClusterKvWatchRegistration,
    pub response: ClusterKvWatchDrainResponse,
    pub cursor_action: ClusterKvWatchDrainCursorAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchDrainCursorPersistence {
    Persisted {
        registration: ClusterKvWatchRegistration,
    },
    CacheOnly {
        registration: ClusterKvWatchRegistration,
        durable_next_revision: KvRevision,
    },
    Unchanged {
        registration: ClusterKvWatchRegistration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchRegistrationRemovalReport {
    pub watch_path: PathBuf,
    pub removed: bool,
    pub allocator: Option<ClusterKvWatchAllocatorState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvActiveWatchRegistrationReport {
    pub has_active: bool,
    pub allocator: Option<ClusterKvWatchAllocatorState>,
    pub scan: Option<ClusterKvWatchScanResult>,
}

pub fn create_cluster_kv_watch_registration(
    paths: &ClusterKvPaths,
    request: ClusterKvWatchPrefixRequest,
    compacted_revision: KvRevision,
    allocator_trusted: bool,
    max_active_watches: usize,
) -> Result<ClusterKvWatchRegistrationCreateReport, KvError> {
    if request.start_revision <= compacted_revision && compacted_revision != KvRevision::ZERO {
        return Err(KvError::Compacted {
            requested: request.start_revision,
            compacted: compacted_revision,
        });
    }
    fs::create_dir_all(&paths.watches).map_err(|source| KvError::Io {
        path: paths.watches.display().to_string(),
        detail: source.to_string(),
    })?;

    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let mut allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let scan =
        if cluster_kv_watch_allocator_needs_scan(allocator_trusted, &allocator, compacted_revision)
        {
            let scan = scan_cluster_kv_watch_registrations(paths, compacted_revision)?;
            allocator = cluster_kv_watch_allocator_after_scan(allocator, &scan, compacted_revision);
            Some(scan)
        } else {
            None
        };

    let active_watch_count = allocator.active_watch_count.unwrap_or_default();
    if active_watch_count >= max_active_watches {
        return Err(KvError::WatchLimit {
            max: max_active_watches,
        });
    }

    let mut watch_id = WatchId(allocator.next_watch_id.max(1));
    loop {
        let registration = ClusterKvWatchRegistration {
            watch_id,
            prefix: request.prefix.clone(),
            next_revision: request.start_revision,
        };
        let watch_path = paths.watch_path(watch_id);
        if write_cluster_kv_watch_registration_file_create_new(&watch_path, &registration)? {
            allocator.next_watch_id = watch_id.0.saturating_add(1).max(1);
            allocator.active_watch_count = Some(active_watch_count.saturating_add(1));
            allocator.pruned_compacted_revision = Some(compacted_revision);
            return Ok(ClusterKvWatchRegistrationCreateReport {
                watch_path,
                registration,
                allocator,
                scan,
            });
        }
        if watch_id.0 == u64::MAX {
            return Err(KvError::WatchIdExhausted);
        }
        watch_id = WatchId(watch_id.0.saturating_add(1).max(1));
    }
}

pub fn cluster_kv_active_watch_registration_report(
    paths: &ClusterKvPaths,
    compacted_revision: KvRevision,
) -> Result<ClusterKvActiveWatchRegistrationReport, KvError> {
    if !paths.watches.exists() && !paths.watches_next_id.exists() {
        return Ok(ClusterKvActiveWatchRegistrationReport::default());
    }
    let allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let scan = scan_cluster_kv_watch_registrations(paths, compacted_revision)?;
    let allocator = cluster_kv_watch_allocator_after_scan(allocator, &scan, compacted_revision);
    Ok(ClusterKvActiveWatchRegistrationReport {
        has_active: scan.retained > 0,
        allocator: (paths.watches.exists() || paths.watches_next_id.exists()).then_some(allocator),
        scan: Some(scan),
    })
}

pub fn drain_cluster_kv_watch_registration(
    mut registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    persisted_events: &[KvEvent],
    snapshot: &KvSnapshot,
    max_payload_bytes: usize,
    registration_file_unchanged: bool,
    registration_file_age: Option<Duration>,
) -> Result<ClusterKvWatchDrainRegistrationReport, KvError> {
    if registration.next_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        return Err(KvError::Compacted {
            requested: registration.next_revision,
            compacted: snapshot.compacted_revision,
        });
    }
    let events = collect_cluster_kv_watch_drain_events(
        persisted_events,
        snapshot,
        &registration,
        max_payload_bytes,
    );
    let cursor_action = if let Some(last) = events.last() {
        registration.next_revision = KvRevision(last.revision.0.saturating_add(1));
        if cluster_kv_watch_registration_cursor_should_persist(
            events.len(),
            registration.next_revision,
            durable_next_revision,
            registration_file_unchanged,
            registration_file_age,
        ) {
            ClusterKvWatchDrainCursorAction::Persist
        } else {
            ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision,
            }
        }
    } else {
        ClusterKvWatchDrainCursorAction::Unchanged
    };
    Ok(ClusterKvWatchDrainRegistrationReport {
        response: ClusterKvWatchDrainResponse {
            watch_id: registration.watch_id,
            events,
            next_revision: registration.next_revision,
        },
        registration,
        cursor_action,
    })
}

pub fn persist_cluster_kv_watch_drain_cursor(
    watch_path: &Path,
    report: &ClusterKvWatchDrainRegistrationReport,
) -> Result<ClusterKvWatchDrainCursorPersistence, KvError> {
    match report.cursor_action {
        ClusterKvWatchDrainCursorAction::Persist => {
            write_cluster_kv_watch_registration_file(watch_path, &report.registration)?;
            Ok(ClusterKvWatchDrainCursorPersistence::Persisted {
                registration: report.registration.clone(),
            })
        }
        ClusterKvWatchDrainCursorAction::CacheOnly {
            durable_next_revision,
        } => Ok(ClusterKvWatchDrainCursorPersistence::CacheOnly {
            registration: report.registration.clone(),
            durable_next_revision,
        }),
        ClusterKvWatchDrainCursorAction::Unchanged => {
            Ok(ClusterKvWatchDrainCursorPersistence::Unchanged {
                registration: report.registration.clone(),
            })
        }
    }
}

pub fn create_cluster_kv_watch_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let report = create_cluster_kv_watch_registration(
        paths,
        request,
        snapshot.compacted_revision,
        false,
        MAX_CLUSTER_KV_ACTIVE_WATCHES,
    )?;
    if let Err(error) = write_cluster_kv_watch_allocator_state(paths, &report.allocator) {
        let _ = remove_cluster_kv_watch_registration_file_if_exists(&report.watch_path);
        return Err(error);
    }
    Ok(report.registration)
}

pub fn drain_cluster_kv_watch_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once_for_node(paths, node_id, request.watch_id);
    }

    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once_for_node(paths, node_id, request.watch_id);
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response =
            drain_cluster_kv_watch_once_for_node(paths, node_id.clone(), request.watch_id)?;
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

fn drain_cluster_kv_watch_once_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    watch_id: WatchId,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    let watch_path = paths.watch_path(watch_id);
    let registration = match read_cluster_kv_watch_registration_file(&watch_path, watch_id) {
        Ok(registration) => registration,
        Err(error @ KvError::UnknownWatch { .. }) => {
            let _ = remove_cluster_kv_watch_registration_file_if_exists(&watch_path);
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let durable_next_revision = registration.next_revision;
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let persisted_events = read_cluster_kv_watch_events(paths)?.events;
    let report = match drain_cluster_kv_watch_registration(
        registration,
        durable_next_revision,
        &persisted_events,
        snapshot.as_ref(),
        MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        false,
        None,
    ) {
        Ok(report) => report,
        Err(error @ KvError::Compacted { .. }) => {
            let _ = cancel_cluster_kv_watch(paths, ClusterKvWatchCancelRequest { watch_id });
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let _ = persist_cluster_kv_watch_drain_cursor(&watch_path, &report)?;
    Ok(report.response)
}

pub fn cancel_cluster_kv_watch(
    paths: &ClusterKvPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, KvError> {
    let report = remove_cluster_kv_watch_registration_with_allocator(paths, request.watch_id)?;
    if let Some(allocator) = report.allocator {
        write_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    if report.removed {
        notify_cluster_kv_watchers(paths);
    }
    Ok(ClusterKvWatchCancelResponse {
        watch_id: request.watch_id,
        cancelled: report.removed,
    })
}

pub fn append_cluster_kv_watch_events_for_active_watches_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    incoming: &[KvEvent],
) -> Result<usize, KvError> {
    if incoming.is_empty() || (!paths.watches.exists() && !paths.watches_next_id.exists()) {
        return Ok(0);
    }
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let active = cluster_kv_active_watch_registration_report(paths, snapshot.compacted_revision)?;
    if let Some(allocator) = active.allocator {
        write_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    if !active.has_active {
        return Ok(0);
    }
    let registrations = if let Some(scan) = active.scan {
        scan.retained_registrations
            .into_iter()
            .map(|retained| retained.registration)
            .collect::<Vec<_>>()
    } else {
        list_cluster_kv_watch_registrations(paths)?
    };
    let incoming = cluster_kv_watch_events_matching_registrations(&registrations, incoming);
    if incoming.is_empty()
        || incoming
            .iter()
            .all(|event| cluster_kv_events_contain(&snapshot.events, event))
    {
        return Ok(0);
    }
    let report = append_cluster_kv_watch_events_to_storage(paths, &incoming, None)?;
    if !report.appended.is_empty() {
        notify_cluster_kv_watchers_for_events(paths, &report.appended);
    }
    Ok(report.appended.len())
}

pub fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistrationRemovalReport, KvError> {
    let watch_path = paths.watch_path(watch_id);
    if !watch_path.exists() {
        return Ok(ClusterKvWatchRegistrationRemovalReport {
            watch_path,
            removed: false,
            allocator: None,
        });
    }
    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let removed = remove_cluster_kv_watch_registration_file_if_exists(&watch_path)?;
    let allocator = if removed {
        let allocator = read_cluster_kv_watch_allocator_state(paths)?;
        allocator
            .active_watch_count
            .is_some()
            .then(|| cluster_kv_watch_allocator_after_registration_removed(allocator))
    } else {
        None
    };
    Ok(ClusterKvWatchRegistrationRemovalReport {
        watch_path,
        removed,
        allocator,
    })
}

pub fn scan_cluster_kv_watch_registrations(
    paths: &ClusterKvPaths,
    compacted_revision: KvRevision,
) -> Result<ClusterKvWatchScanResult, KvError> {
    let mut result = ClusterKvWatchScanResult::default();
    let entries = match fs::read_dir(&paths.watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    result.scanned = true;
    for entry in entries {
        let entry = entry.map_err(|source| KvError::Io {
            path: paths.watches.display().to_string(),
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
        match read_cluster_kv_watch_registration_file(&path, watch_id) {
            Ok(registration)
                if compacted_revision != KvRevision::ZERO
                    && registration.next_revision <= compacted_revision =>
            {
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
            Ok(registration) => {
                result.retained = result.retained.saturating_add(1);
                result
                    .retained_registrations
                    .push(ClusterKvWatchScanRegistration { path, registration });
            }
            Err(KvError::UnknownWatch { watch_id: returned }) if returned == watch_id => {
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
            Err(error) => {
                eprintln!(
                    "warning: removing unreadable cluster kv watch registration {}: {}",
                    path.display(),
                    error
                );
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
        }
    }
    Ok(result)
}

pub fn list_cluster_kv_watch_registrations(
    paths: &ClusterKvPaths,
) -> Result<Vec<ClusterKvWatchRegistration>, KvError> {
    let entries = match fs::read_dir(&paths.watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let mut registrations = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| KvError::Io {
            path: paths.watches.display().to_string(),
            detail: source.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(file_watch_id) = cluster_kv_watch_id_from_file_name(&name) else {
            continue;
        };
        let watch_id = WatchId(file_watch_id);
        match read_cluster_kv_watch_registration_file(&entry.path(), watch_id) {
            Ok(registration) => registrations.push(registration),
            Err(KvError::UnknownWatch { watch_id: returned }) if returned == watch_id => {}
            Err(error) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch registration {} during fan-out filtering: {}",
                    entry.path().display(),
                    error
                );
            }
        }
    }
    Ok(registrations)
}

pub fn cluster_kv_watch_events_matching_registrations(
    registrations: &[ClusterKvWatchRegistration],
    incoming: &[KvEvent],
) -> Vec<KvEvent> {
    if registrations.is_empty() {
        return Vec::new();
    }
    incoming
        .iter()
        .filter(|event| {
            registrations.iter().any(|registration| {
                event.revision >= registration.next_revision
                    && event.key.starts_with(&registration.prefix)
            })
        })
        .cloned()
        .collect()
}

pub fn cluster_kv_watch_ids_matching_events(
    registrations: &[ClusterKvWatchRegistration],
    events: &[KvEvent],
) -> Vec<WatchId> {
    if events.is_empty() {
        return Vec::new();
    }
    let mut watch_ids = BTreeSet::new();
    for registration in registrations {
        if events.iter().any(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        }) {
            watch_ids.insert(registration.watch_id);
        }
    }
    watch_ids.into_iter().collect()
}

pub fn cluster_kv_watch_id_from_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("watch-")
        .and_then(|name| name.strip_suffix(".sobj"))
        .and_then(|id| id.parse::<u64>().ok())
}

pub fn cluster_kv_watch_allocator_needs_scan(
    allocator_trusted: bool,
    allocator: &ClusterKvWatchAllocatorState,
    compacted_revision: KvRevision,
) -> bool {
    if !allocator_trusted {
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

pub fn cluster_kv_watch_allocator_after_scan(
    mut allocator: ClusterKvWatchAllocatorState,
    scan: &ClusterKvWatchScanResult,
    compacted_revision: KvRevision,
) -> ClusterKvWatchAllocatorState {
    allocator.next_watch_id = allocator
        .next_watch_id
        .max(scan.max_seen_id.saturating_add(1))
        .max(1);
    allocator.active_watch_count = Some(scan.retained);
    allocator.pruned_compacted_revision = Some(compacted_revision);
    allocator
}

pub fn cluster_kv_watch_allocator_after_registration_removed(
    mut allocator: ClusterKvWatchAllocatorState,
) -> ClusterKvWatchAllocatorState {
    if let Some(active_watch_count) = allocator.active_watch_count {
        allocator.active_watch_count = Some(active_watch_count.saturating_sub(1));
    }
    allocator
}

pub fn read_cluster_kv_watch_event_log(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventLogRead, KvError> {
    read_cluster_kv_watch_event_log_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_event_log_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    let mut file = match File::open(&paths.watch_events_log) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ClusterKvWatchEventLogRead::default());
        }
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    file.lock_shared().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    read_cluster_kv_watch_event_log_from_file_with_observer(paths, &mut file, on_log_read)
}

pub fn read_cluster_kv_watch_event_log_from_file_with_observer<F>(
    paths: &ClusterKvPaths,
    file: &mut File,
    mut on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    file.seek(SeekFrom::Start(0))
        .map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut events = Vec::new();
    let mut record_number = 0_usize;
    let mut read_bytes = 0_u64;
    let mut needs_repair = false;
    loop {
        record_number += 1;
        let mut len_buf = [0_u8; 4];
        match read_cluster_kv_watch_event_log_chunk(
            &paths.watch_events_log,
            file,
            &mut len_buf,
            &mut on_log_read,
        )? {
            WatchEventLogReadChunk::Complete => {}
            WatchEventLogReadChunk::CleanEof => break,
            WatchEventLogReadChunk::Truncated => {
                eprintln!(
                    "warning: ignoring partial cluster kv watch-event log record {} in {}",
                    record_number,
                    paths.watch_events_log.display()
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
                paths.watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        let mut record = vec![0_u8; len];
        if read_cluster_kv_watch_event_log_chunk(
            &paths.watch_events_log,
            file,
            &mut record,
            &mut on_log_read,
        )? != WatchEventLogReadChunk::Complete
        {
            eprintln!(
                "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                record_number,
                paths.watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        match decode_cluster_kv_watch_event_log_record(&record) {
            Ok(event) => events.push(event),
            Err(source) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch-event log record {} in {}: {}",
                    record_number,
                    paths.watch_events_log.display(),
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
            paths.watch_events_log.display(),
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

fn read_cluster_kv_watch_event_log_chunk<F>(
    log_path: &Path,
    file: &mut File,
    buf: &mut [u8],
    on_log_read: &mut F,
) -> Result<WatchEventLogReadChunk, KvError>
where
    F: FnMut(u64),
{
    let mut offset = 0;
    while offset < buf.len() {
        match file.read(&mut buf[offset..]) {
            Ok(0) if offset == 0 => return Ok(WatchEventLogReadChunk::CleanEof),
            Ok(0) => return Ok(WatchEventLogReadChunk::Truncated),
            Ok(read) => {
                offset += read;
                on_log_read(read as u64);
            }
            Err(source) => {
                return Err(KvError::Io {
                    path: log_path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(WatchEventLogReadChunk::Complete)
}

pub fn cluster_kv_watch_event_payload_bytes(event: &KvEvent) -> usize {
    let value_bytes = match &event.kind {
        KvEventKind::Put { value } => value.len(),
        KvEventKind::Delete => 0,
    };
    event.key.len().saturating_add(value_bytes)
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

pub fn encode_kv_value<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, KvError> {
    sinomo_object_serde::to_bytes(value, KV_OBJECT_VERSION_ID, LengthMode::U32).map_err(|source| {
        KvError::Serialization {
            detail: source.to_string(),
        }
    })
}

pub fn decode_kv_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(bytes).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRpcPayload {
    pub route: &'static str,
    pub category: &'static str,
    pub body: Vec<u8>,
}

fn encode_cluster_kv_rpc_body<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, KvError> {
    sinomo_object_serde::to_bytes(value, KV_OBJECT_VERSION_ID, LengthMode::U32).map_err(|source| {
        KvError::Serialization {
            detail: source.to_string(),
        }
    })
}

fn decode_cluster_kv_rpc_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(body).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

pub fn cluster_kv_snapshot_rpc_payload() -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&())?,
    })
}

pub fn decode_cluster_kv_snapshot_rpc_request_body(body: &[u8]) -> Result<(), KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_snapshot_response_rpc_payload(
    snapshot: &KvSnapshot,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(snapshot)?,
    })
}

pub fn decode_cluster_kv_snapshot_rpc_body(body: &[u8]) -> Result<KvSnapshot, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_txn_rpc_payload(txn: &KvTxn) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(txn)?,
    })
}

pub fn decode_cluster_kv_txn_rpc_request_body(body: &[u8]) -> Result<KvTxn, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_txn_response_rpc_payload(
    response: &KvTxnResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_txn_rpc_body(body: &[u8]) -> Result<KvTxnResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_range_prefix_rpc_payload(prefix: &str) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&prefix.to_string())?,
    })
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

pub fn cluster_kv_watch_events_noop_response(
    request: &ClusterKvWatchEventsRequest,
) -> Option<ClusterKvWatchEventsResponse> {
    request
        .events
        .is_empty()
        .then(|| ClusterKvWatchEventsResponse {
            accepted: true,
            detail: "no watch events to fan out".to_string(),
            stored_events: 0,
        })
}

pub fn handle_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
    local_state: &ClusterBootstrapState,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, KvError> {
    if let Err(limit) = validate_cluster_kv_watch_event_fanout_batch(&request.events) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: limit.detail(),
            stored_events: 0,
        });
    }
    if request.state.cluster_id != local_state.cluster_id {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, local_state.cluster_id
            ),
            stored_events: 0,
        });
    }
    if !cluster_kv_raft_node_is_voter(local_state, &request.leader_id) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out rejected from unauthorized leader {}",
                request.leader_id
            ),
            stored_events: 0,
        });
    }
    let stored_events = append_cluster_kv_watch_events_for_active_watches_for_node(
        paths,
        local_state.local_node_id.clone(),
        &request.events,
    )?;
    Ok(ClusterKvWatchEventsResponse {
        accepted: true,
        detail: "cluster kv watch events stored".to_string(),
        stored_events,
    })
}

pub fn cluster_kv_watch_events_rpc_payload(
    request: &ClusterKvWatchEventsRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_WATCH_EVENTS_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_watch_events_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvWatchEventsRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_watch_events_response_rpc_payload(
    response: &ClusterKvWatchEventsResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_WATCH_EVENTS_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_watch_events_rpc_response_body(
    body: &[u8],
) -> Result<ClusterKvWatchEventsResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_append_rpc_payload(
    request: &ClusterRaftAppendRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_APPEND_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_raft_append_rpc_request_body(
    body: &[u8],
) -> Result<ClusterRaftAppendRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_append_response_rpc_payload(
    response: &ClusterRaftAppendResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_APPEND_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_raft_append_rpc_response_body(
    body: &[u8],
) -> Result<ClusterRaftAppendResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_vote_rpc_payload(
    request: &ClusterRaftVoteRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_VOTE_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_raft_vote_rpc_request_body(
    body: &[u8],
) -> Result<ClusterRaftVoteRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_vote_response_rpc_payload(
    response: &ClusterRaftVoteResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_VOTE_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_raft_vote_rpc_response_body(
    body: &[u8],
) -> Result<ClusterRaftVoteResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn handle_cluster_raft_rpc_payload<A, V, E>(
    route: &str,
    body: &[u8],
    append_handler: A,
    vote_handler: V,
) -> Result<Option<ClusterKvRpcPayload>, E>
where
    A: FnOnce(ClusterRaftAppendRequest) -> Result<ClusterRaftAppendResponse, E>,
    V: FnOnce(ClusterRaftVoteRequest) -> Result<ClusterRaftVoteResponse, E>,
    E: From<KvError>,
{
    match route {
        CLUSTER_RAFT_APPEND_ROUTE => {
            let request = decode_cluster_raft_append_rpc_request_body(body).map_err(E::from)?;
            let response = append_handler(request)?;
            Ok(Some(
                cluster_raft_append_response_rpc_payload(&response).map_err(E::from)?,
            ))
        }
        CLUSTER_RAFT_VOTE_ROUTE => {
            let request = decode_cluster_raft_vote_rpc_request_body(body).map_err(E::from)?;
            let response = vote_handler(request)?;
            Ok(Some(
                cluster_raft_vote_response_rpc_payload(&response).map_err(E::from)?,
            ))
        }
        _ => Ok(None),
    }
}

pub trait ClusterKvLocalReadWatchHost {
    type Error: From<KvError>;

    fn range_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, Self::Error>;

    fn create_watch(
        &mut self,
        request: ClusterKvWatchPrefixRequest,
    ) -> Result<ClusterKvWatchRegistration, Self::Error>;

    fn drain_watch(
        &mut self,
        request: ClusterKvWatchDrainRequest,
    ) -> Result<ClusterKvWatchDrainResponse, Self::Error>;

    fn cancel_watch(
        &mut self,
        request: ClusterKvWatchCancelRequest,
    ) -> Result<ClusterKvWatchCancelResponse, Self::Error>;
}

pub fn handle_cluster_kv_local_read_watch_rpc_payload<H>(
    route: &str,
    body: &[u8],
    host: &mut H,
) -> Result<Option<ClusterKvRpcPayload>, H::Error>
where
    H: ClusterKvLocalReadWatchHost,
{
    let payload = match route {
        CLUSTER_KV_RANGE_PREFIX_ROUTE => {
            let prefix: String = decode_cluster_kv_rpc_body(body).map_err(H::Error::from)?;
            let result = host.range_prefix(&prefix)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_RANGE_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::Error::from)?,
            }
        }
        CLUSTER_KV_WATCH_PREFIX_ROUTE => {
            let request: ClusterKvWatchPrefixRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::Error::from)?;
            let result = host.create_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_WATCH_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::Error::from)?,
            }
        }
        CLUSTER_KV_WATCH_DRAIN_ROUTE => {
            let request: ClusterKvWatchDrainRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::Error::from)?;
            let result = host.drain_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_WATCH_DRAIN_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::Error::from)?,
            }
        }
        CLUSTER_KV_WATCH_CANCEL_ROUTE => {
            let request: ClusterKvWatchCancelRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::Error::from)?;
            let result = host.cancel_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_WATCH_CANCEL_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::Error::from)?,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(payload))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvRevision(pub u64);

impl KvRevision {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvVersion(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseId(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WatchId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEntry {
    pub key: String,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub create_revision: KvRevision,
    pub modify_revision: KvRevision,
    pub version: KvVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<LeaseId>,
}

impl KvEntry {
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, KvError> {
        decode_kv_value(&self.value)
    }
}

#[derive(Clone, Copy)]
struct SimulatedKeyState {
    present: bool,
    lease_id: Option<LeaseId>,
    value_hash: Option<blake3::Hash>,
}

impl SimulatedKeyState {
    const ABSENT: Self = Self {
        present: false,
        lease_id: None,
        value_hash: None,
    };

    fn present_with_value(lease_id: Option<LeaseId>, value: &[u8]) -> Self {
        Self {
            present: true,
            lease_id,
            value_hash: Some(blake3::hash(value)),
        }
    }
}

fn simulated_put_is_no_effect(
    old_state: &SimulatedKeyState,
    value: &[u8],
    lease_id: Option<LeaseId>,
) -> bool {
    old_state.present
        && old_state.lease_id == lease_id
        && old_state.value_hash == Some(blake3::hash(value))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLease {
    pub lease_id: LeaseId,
    pub ttl_ticks: u64,
    pub remaining_ticks: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub granted_at: OffsetDateTime,
}

impl KvLease {
    fn new(lease_id: LeaseId, ttl_ticks: u64) -> Self {
        Self {
            lease_id,
            ttl_ticks,
            remaining_ticks: ttl_ticks,
            granted_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCompare {
    Exists {
        key: String,
    },
    Missing {
        key: String,
    },
    ValueEquals {
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    VersionEquals {
        key: String,
        version: KvVersion,
    },
    CreateRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    ModifyRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    LeaseEquals {
        key: String,
        lease_id: Option<LeaseId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOp {
    Put {
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    },
    Delete {
        key: String,
    },
    DeletePrefix {
        prefix: String,
    },
    LeaseGrant {
        lease_id: LeaseId,
        ttl_ticks: u64,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
    },
}

impl KvOp {
    pub fn put_serde<T: Serialize + ?Sized>(
        key: impl Into<String>,
        value: &T,
        lease_id: Option<LeaseId>,
    ) -> Result<Self, KvError> {
        Ok(Self::Put {
            key: key.into(),
            value: encode_kv_value(value)?,
            lease_id,
        })
    }
}

fn validate_kv_value_len(actual_bytes: usize) -> Result<(), KvError> {
    if actual_bytes > MAX_KV_VALUE_BYTES {
        return Err(KvError::ValueTooLarge {
            actual_bytes,
            max_bytes: MAX_KV_VALUE_BYTES,
        });
    }
    Ok(())
}

fn validate_kv_op_payload(op: &KvOp) -> Result<(), KvError> {
    if let KvOp::Put { value, .. } = op {
        validate_kv_value_len(value.len())?;
    }
    Ok(())
}

fn kv_op_payload_bytes(op: &KvOp) -> usize {
    match op {
        KvOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
        KvOp::Delete { key } => key.len(),
        KvOp::DeletePrefix { prefix } => prefix.len(),
        KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. } => 0,
    }
}

fn kv_compare_payload_bytes(compare: &KvCompare) -> usize {
    match compare {
        KvCompare::Exists { key }
        | KvCompare::Missing { key }
        | KvCompare::VersionEquals { key, .. }
        | KvCompare::CreateRevisionEquals { key, .. }
        | KvCompare::ModifyRevisionEquals { key, .. }
        | KvCompare::LeaseEquals { key, .. } => key.len(),
        KvCompare::ValueEquals { key, value } => key.len().saturating_add(value.len()),
    }
}

fn validate_kv_compare_payload(compare: &KvCompare) -> Result<(), KvError> {
    if let KvCompare::ValueEquals { value, .. } = compare {
        validate_kv_value_len(value.len())?;
    }
    Ok(())
}

fn validate_kv_payload_total(actual_bytes: usize) -> Result<(), KvError> {
    if actual_bytes > MAX_KV_COMMAND_PAYLOAD_BYTES {
        return Err(KvError::CommandPayloadTooLarge {
            actual_bytes,
            max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn record_kv_mutation_events(total: &mut usize, delta: usize) -> Result<(), KvError> {
    let observed_events = total.saturating_add(delta);
    if observed_events > MAX_KV_MUTATION_EVENTS_PER_COMMAND {
        return Err(KvError::MutationEventLimit {
            observed_events,
            max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        });
    }
    *total = observed_events;
    Ok(())
}

fn kv_entry_payload_bytes(entry: &KvEntry) -> usize {
    entry.key.len().saturating_add(entry.value.len())
}

fn validate_kv_txn_payload(txn: &KvTxn) -> Result<(), KvError> {
    let mut payload_bytes = 0_usize;
    for compare in &txn.compares {
        validate_kv_compare_payload(compare)?;
        payload_bytes = payload_bytes.saturating_add(kv_compare_payload_bytes(compare));
    }
    for op in txn.success.iter().chain(txn.failure.iter()) {
        validate_kv_op_payload(op)?;
        payload_bytes = payload_bytes.saturating_add(kv_op_payload_bytes(op));
    }
    validate_kv_payload_total(payload_bytes)
}

fn validate_kv_command_payload(command: &KvCommand) -> Result<(), KvError> {
    match command {
        KvCommand::Op(op) => {
            validate_kv_op_payload(op)?;
            validate_kv_payload_total(kv_op_payload_bytes(op))
        }
        KvCommand::Txn(txn) => validate_kv_txn_payload(txn),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxn {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compares: Vec<KvCompare>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub success: Vec<KvOp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure: Vec<KvOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxnResponse {
    pub succeeded: bool,
    pub revision: KvRevision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses: Vec<KvOpResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOpResponse {
    Put {
        key: String,
        revision: KvRevision,
    },
    Delete {
        key: String,
        deleted: bool,
    },
    DeletePrefix {
        prefix: String,
        deleted_count: usize,
    },
    LeaseGrant {
        lease_id: LeaseId,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
        deleted_count: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvEventKind {
    Put {
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEvent {
    pub revision: KvRevision,
    pub key: String,
    pub kind: KvEventKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvWatch {
    pub watch_id: WatchId,
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvSnapshot {
    pub revision: KvRevision,
    pub compacted_revision: KvRevision,
    pub entries: Vec<KvEntry>,
    pub leases: Vec<KvLease>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
}

impl KvSnapshot {
    /// Returns a snapshot with stable key/lease ordering and duplicate records
    /// collapsed to the same last-record-wins state as `InProcessKvStore::restore`.
    ///
    /// Call this before using the snapshot point/range helpers on externally
    /// supplied snapshots. Snapshots produced by `InProcessKvStore::snapshot`,
    /// `KvMaterializedCache`, and `KvLocalCache` are already canonical.
    pub fn canonicalized(mut self) -> Self {
        self.canonicalize();
        self
    }

    /// Canonicalizes this snapshot in place.
    pub fn canonicalize(&mut self) {
        self.entries.sort_by(|left, right| left.key.cmp(&right.key));
        self.entries.reverse();
        self.entries.dedup_by(|left, right| left.key == right.key);
        self.entries.reverse();
        self.leases
            .sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
        self.leases.reverse();
        self.leases
            .dedup_by(|left, right| left.lease_id == right.lease_id);
        self.leases.reverse();
    }

    /// Looks up a key in a canonical snapshot.
    ///
    /// This is intentionally `O(log n)` and does not validate ordering in release
    /// builds; canonicalize externally supplied snapshots first.
    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        debug_assert!(self.is_canonical());
        self.entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .and_then(|index| self.entries.get(index))
    }

    /// Returns all entries under `prefix` from a canonical snapshot.
    pub fn range_prefix(&self, prefix: &str) -> Vec<KvEntry> {
        self.prefix_entries(prefix).cloned().collect()
    }

    /// Iterates entries under `prefix` from a canonical snapshot without cloning.
    pub fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        debug_assert!(self.is_canonical());
        let start = self
            .entries
            .partition_point(|entry| entry.key.as_str() < prefix);
        self.entries[start..]
            .iter()
            .take_while(move |entry| entry.key.starts_with(prefix))
    }

    /// Returns at most `max_entries` entries under `prefix` from a canonical snapshot.
    pub fn range_prefix_limited(
        &self,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, usize::MAX)
    }

    pub fn range_prefix_limited_by_payload_bytes(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        let mut payload_bytes = 0_usize;
        for entry in self.prefix_entries(prefix) {
            if entries.len() >= max_entries {
                return Err(KvError::RangeLimit {
                    prefix: prefix.to_string(),
                    max_entries,
                });
            }
            payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
            if payload_bytes > max_payload_bytes {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_string(),
                    observed_bytes: payload_bytes,
                    max_bytes: max_payload_bytes,
                });
            }
            entries.push(entry.clone());
        }
        Ok(entries)
    }

    pub fn range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)?
            .iter()
            .map(KvEntry::decode)
            .collect()
    }

    fn is_canonical(&self) -> bool {
        self.entries
            .windows(2)
            .all(|window| window[0].key < window[1].key)
            && self
                .leases
                .windows(2)
                .all(|window| window[0].lease_id < window[1].lease_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLogEntry {
    pub index: u64,
    pub term: u64,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
pub struct KvWalPaths {
    pub root: PathBuf,
    pub lock: PathBuf,
    pub hard_state: PathBuf,
    pub wal: PathBuf,
    pub snapshot: PathBuf,
}

impl KvWalPaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            lock: root.join("node.lock"),
            hard_state: root.join("hard-state.sobj"),
            wal: root.join("wal.sobjlog"),
            snapshot: root.join("snapshot.sobj"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), KvError> {
        fs::create_dir_all(&self.root).map_err(|source| KvError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn has_durable_state(&self) -> Result<bool, KvError> {
        Ok(path_exists(&self.hard_state)?
            || path_exists(&self.wal)?
            || path_exists(&self.snapshot)?)
    }

    /// Reads only the persisted hard state (term/vote/leader/commit) without
    /// opening the full raft node. Opening the node replays the entire WAL — up to
    /// the retained tail, ~MBs — which is wasteful for read-only leadership/term
    /// queries that only need this tiny, atomically-rewritten file. The value is a
    /// cheap point read: it reflects the last durably persisted hard state and is
    /// never torn (writes go through a temp-file rename). Defaults when absent.
    pub fn read_persisted_hard_state(&self) -> Result<RaftHardState, KvError> {
        let identity = optional_file_identity(&self.hard_state);
        if let Some(identity) = identity
            && let Some(cached) = cache_lookup_hard_state(&self.hard_state, identity)
        {
            return Ok(cached);
        }
        #[cfg(test)]
        record_persisted_hard_state_actual_read_for_test(&self.hard_state);
        let hard_state =
            read_optional_object::<RaftHardState>(&self.hard_state)?.unwrap_or_default();
        if let Some(identity) = identity {
            cache_store_hard_state(&self.hard_state, identity, hard_state.clone());
        }
        Ok(hard_state)
    }

    pub fn write_persisted_hard_state(&self, hard_state: &RaftHardState) -> Result<(), KvError> {
        write_object_file(&self.hard_state, hard_state)?;
        if let Some(identity) = optional_file_identity(&self.hard_state) {
            cache_store_hard_state(&self.hard_state, identity, hard_state.clone());
        }
        Ok(())
    }
}

/// Process-wide memoization of parsed materialized snapshots, keyed by file path
/// and validated by file identity. Lets repeated reads of an unchanged snapshot
/// skip the disk read + deserialization entirely — the dominant idle read cost.
/// The writer updates this on every write, so intra-process reads are always
/// coherent; cross-process edits are caught by identity validation.
struct MaterializedSnapshotCacheEntry {
    identity: FileIdentity,
    snapshot: Arc<KvSnapshot>,
    last_used: u64,
}

struct RaftViewCacheEntry {
    identity: RaftViewIdentity,
    snapshot: Arc<KvSnapshot>,
    last_used: u64,
}

struct HardStateCacheEntry {
    identity: Option<FileIdentity>,
    hard_state: RaftHardState,
    last_used: u64,
}

static MATERIALIZED_SNAPSHOT_CACHE: OnceLock<
    Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>>,
> = OnceLock::new();
static RAFT_VIEW_CACHE: OnceLock<Mutex<HashMap<PathBuf, RaftViewCacheEntry>>> = OnceLock::new();
static HARD_STATE_CACHE: OnceLock<Mutex<HashMap<PathBuf, HardStateCacheEntry>>> = OnceLock::new();

fn materialized_snapshot_cache() -> &'static Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>>
{
    MATERIALIZED_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn raft_view_cache() -> &'static Mutex<HashMap<PathBuf, RaftViewCacheEntry>> {
    RAFT_VIEW_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn hard_state_cache() -> &'static Mutex<HashMap<PathBuf, HardStateCacheEntry>> {
    HARD_STATE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Identity used to validate a cached file parse. Every persisted object/snapshot
/// file is written via a temp file + atomic rename, so a rewrite always lands on a
/// fresh inode. Including `ino` closes the window where two writes share an mtime
/// (filesystem mtime granularity) *and* an identical length — which `(mtime, len)`
/// alone cannot distinguish, so a stale parse could otherwise be served. The
/// comparison is all-or-nothing (derived `PartialEq`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FileIdentity {
    mtime: Option<SystemTime>,
    len: u64,
    ino: u64,
}

/// Point-in-time identity for a durable Raft view. The WAL is append/rewrite in
/// place, hard state and snapshots use object-file rewrites, and any of those
/// changing means a cached decoded store is no longer authoritative.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct RaftViewIdentity {
    hard_state: Option<FileIdentity>,
    wal: Option<FileIdentity>,
    snapshot: Option<FileIdentity>,
}

/// Upper bound on entries kept in each process-global file cache. The live set of
/// snapshot/object-file paths is tiny in normal operation; this cap only guards
/// against unbounded growth under pathological path churn. Read caches evict the
/// oldest entry so a noisy set of roots cannot repeatedly cold-start hot roots.
const FILE_CACHE_CAPACITY: usize = 4096;

fn path_exists(path: &Path) -> Result<bool, KvError> {
    #[cfg(test)]
    record_durable_state_path_exists_probe_for_test(path);
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[cfg(test)]
static DURABLE_STATE_PATH_EXISTS_PROBES_FOR_TEST: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();

#[cfg(test)]
fn durable_state_path_exists_probes_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    DURABLE_STATE_PATH_EXISTS_PROBES_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn record_durable_state_path_exists_probe_for_test(path: &Path) {
    if let Ok(mut probes) = durable_state_path_exists_probes_for_test().lock() {
        *probes.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
fn reset_durable_state_path_exists_probes_for_test(paths: &KvWalPaths) {
    if let Ok(mut probes) = durable_state_path_exists_probes_for_test().lock() {
        probes.remove(&paths.hard_state);
        probes.remove(&paths.wal);
        probes.remove(&paths.snapshot);
    }
}

#[cfg(test)]
fn durable_state_path_exists_probe_count_for_test(paths: &KvWalPaths) -> u64 {
    durable_state_path_exists_probes_for_test()
        .lock()
        .ok()
        .map(|probes| {
            probes.get(&paths.hard_state).copied().unwrap_or_default()
                + probes.get(&paths.wal).copied().unwrap_or_default()
                + probes.get(&paths.snapshot).copied().unwrap_or_default()
        })
        .unwrap_or_default()
}

fn file_identity(path: &Path) -> Option<FileIdentity> {
    let metadata = fs::metadata(path).ok()?;
    Some(FileIdentity {
        mtime: metadata.modified().ok(),
        len: metadata.len(),
        ino: file_ino(&metadata),
    })
}

fn optional_file_identity(path: &Path) -> Option<Option<FileIdentity>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let identity = FileIdentity {
                mtime: metadata.modified().ok(),
                len: metadata.len(),
                ino: file_ino(&metadata),
            };
            identity.mtime.is_some().then_some(Some(identity))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Some(None),
        Err(_) => None,
    }
}

/// Returns `Some(None)` for an absent file, `Some(Some(identity))` for a
/// cacheable present file, and `None` for a present file whose identity cannot be
/// validated. Filesystem errors other than not-found are preserved for callers
/// that use this to decide whether durable raft state exists.
fn optional_file_identity_for_read(path: &Path) -> Result<Option<Option<FileIdentity>>, KvError> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let identity = FileIdentity {
                mtime: metadata.modified().ok(),
                len: metadata.len(),
                ino: file_ino(&metadata),
            };
            Ok(identity.mtime.is_some().then_some(Some(identity)))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(Some(None)),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn raft_view_identity(paths: &KvWalPaths) -> Option<RaftViewIdentity> {
    Some(RaftViewIdentity {
        hard_state: optional_file_identity(&paths.hard_state)?,
        wal: optional_file_identity(&paths.wal)?,
        snapshot: optional_file_identity(&paths.snapshot)?,
    })
}

fn raft_view_identity_for_read(
    paths: &KvWalPaths,
) -> Result<(bool, Option<RaftViewIdentity>), KvError> {
    let hard_state = optional_file_identity_for_read(&paths.hard_state)?;
    let wal = optional_file_identity_for_read(&paths.wal)?;
    let snapshot = optional_file_identity_for_read(&paths.snapshot)?;
    let has_durable_state = hard_state != Some(None) || wal != Some(None) || snapshot != Some(None);
    let identity = match (hard_state, wal, snapshot) {
        (Some(hard_state), Some(wal), Some(snapshot)) => Some(RaftViewIdentity {
            hard_state,
            wal,
            snapshot,
        }),
        _ => None,
    };
    Ok((has_durable_state, identity))
}

fn kv_read_cache_tick() -> u64 {
    KV_READ_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn evict_materialized_snapshot_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, MaterializedSnapshotCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

fn evict_raft_view_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, RaftViewCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

fn evict_hard_state_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, HardStateCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

fn cache_lookup_snapshot(path: &Path, identity: FileIdentity) -> Option<Arc<KvSnapshot>> {
    let mut guard = materialized_snapshot_cache().lock().ok()?;
    let entry = guard.get_mut(path)?;
    // mtime is the primary key; require it to be present so we never serve a hit
    // we cannot validate, then require the full identity (mtime, len, inode).
    if identity.mtime.is_some() && entry.identity == identity {
        entry.last_used = kv_read_cache_tick();
        Some(entry.snapshot.clone())
    } else {
        None
    }
}

fn cache_store_snapshot(path: &Path, identity: FileIdentity, snapshot: Arc<KvSnapshot>) {
    if identity.mtime.is_none() {
        return;
    }
    if let Ok(mut guard) = materialized_snapshot_cache().lock() {
        evict_materialized_snapshot_cache_entry_if_needed(&mut guard, path);
        guard.insert(
            path.to_path_buf(),
            MaterializedSnapshotCacheEntry {
                identity,
                snapshot,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

fn cache_lookup_hard_state(path: &Path, identity: Option<FileIdentity>) -> Option<RaftHardState> {
    let mut guard = hard_state_cache().lock().ok()?;
    let entry = guard.get_mut(path)?;
    if entry.identity != identity {
        return None;
    }
    entry.last_used = kv_read_cache_tick();
    let hard_state = entry.hard_state.clone();
    drop(guard);
    // Re-stat after cloning the cached hard state so a concurrent replace cannot
    // slip between the caller's identity read and the return. If it changes after
    // this check, the read linearizes before that change.
    (optional_file_identity(path) == Some(identity)).then_some(hard_state)
}

fn cache_store_hard_state(path: &Path, identity: Option<FileIdentity>, hard_state: RaftHardState) {
    if let Ok(mut guard) = hard_state_cache().lock() {
        evict_hard_state_cache_entry_if_needed(&mut guard, path);
        guard.insert(
            path.to_path_buf(),
            HardStateCacheEntry {
                identity,
                hard_state,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

#[cfg(test)]
static PERSISTED_HARD_STATE_READS_FOR_TEST: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();

#[cfg(test)]
fn persisted_hard_state_reads_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    PERSISTED_HARD_STATE_READS_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn record_persisted_hard_state_actual_read_for_test(path: &Path) {
    if let Ok(mut reads) = persisted_hard_state_reads_for_test().lock() {
        *reads.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
fn reset_persisted_hard_state_actual_reads_for_test(path: &Path) {
    if let Ok(mut reads) = persisted_hard_state_reads_for_test().lock() {
        reads.remove(path);
    }
}

#[cfg(test)]
fn persisted_hard_state_actual_reads_for_test(path: &Path) -> u64 {
    persisted_hard_state_reads_for_test()
        .lock()
        .ok()
        .and_then(|reads| reads.get(path).copied())
        .unwrap_or_default()
}

fn cache_lookup_raft_view(
    paths: &KvWalPaths,
    identity: RaftViewIdentity,
) -> Option<Arc<KvSnapshot>> {
    let mut guard = raft_view_cache().lock().ok()?;
    let entry = guard.get_mut(&paths.root)?;
    if entry.identity != identity {
        return None;
    }
    entry.last_used = kv_read_cache_tick();
    let snapshot = entry.snapshot.clone();
    drop(guard);
    // Re-stat after cloning the cached view so a concurrent append/rewrite cannot
    // slip between the first identity read and the return. If the files change
    // after this check, the read linearizes before that change.
    (raft_view_identity(paths) == Some(identity)).then_some(snapshot)
}

fn cache_store_raft_view(
    paths: &KvWalPaths,
    identity: RaftViewIdentity,
    snapshot: Arc<KvSnapshot>,
) {
    if let Ok(mut guard) = raft_view_cache().lock() {
        evict_raft_view_cache_entry_if_needed(&mut guard, &paths.root);
        guard.insert(
            paths.root.clone(),
            RaftViewCacheEntry {
                identity,
                snapshot,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvMaterializedCache {
    path: PathBuf,
    /// When set, the persisted snapshot's watch-event history is compacted to at
    /// most this many events before writing. Keeps the materialized file small and
    /// — crucially — ensures every writer (the explicit publish path and the
    /// read-triggered refresh in `read_store_with_raft_view`) persists the same
    /// event tail, so they don't perpetually rewrite each other's snapshots.
    max_materialized_events: Option<usize>,
}

impl KvMaterializedCache {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_materialized_events: None,
        }
    }

    pub fn with_max_materialized_events(mut self, max_events: usize) -> Self {
        self.max_materialized_events = Some(max_events);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn read_snapshot_arc(&self) -> Result<Arc<KvSnapshot>, KvError> {
        let identity = file_identity(&self.path);
        if let Some(identity) = identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok(cached);
        }
        let snapshot = Arc::new(read_object_file::<KvSnapshot>(&self.path)?.canonicalized());
        if let Some(identity) = identity {
            cache_store_snapshot(&self.path, identity, snapshot.clone());
        }
        Ok(snapshot)
    }

    pub fn read_snapshot_arc_if_present(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        let identity = optional_file_identity_for_read(&self.path)?;
        let cache_identity = match identity {
            Some(Some(identity)) => Some(identity),
            Some(None) => return Ok(None),
            None => None,
        };
        if let Some(identity) = cache_identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok(Some(cached));
        }
        let Some(bytes) = read_optional_object_file_bytes(&self.path)? else {
            return Ok(None);
        };
        let snapshot =
            Arc::new(decode_object_file::<KvSnapshot>(&self.path, &bytes)?.canonicalized());
        if let Some(identity) = cache_identity {
            cache_store_snapshot(&self.path, identity, snapshot.clone());
        }
        Ok(Some(snapshot))
    }

    pub fn read_snapshot(&self) -> Result<KvSnapshot, KvError> {
        let snapshot = self.read_snapshot_arc()?;
        Ok((*snapshot).clone())
    }

    pub fn read_snapshot_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.read_snapshot_with_raft_view_inner(node_id, paths, true)
    }

    pub fn read_snapshot_with_raft_view_without_materialized_refresh(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.read_snapshot_with_raft_view_inner(node_id, paths, false)
    }

    fn read_snapshot_with_raft_view_inner(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
        refresh_materialized: bool,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        let (has_durable_state, identity) = raft_view_identity_for_read(&paths)?;
        if has_durable_state {
            if let Some(identity) = identity
                && let Some(snapshot) = cache_lookup_raft_view(&paths, identity)
            {
                return Ok(snapshot);
            }
            let node =
                match DurableRaftKvNode::try_open_with_materialized_cache(node_id, paths, self) {
                    Ok(node) => node,
                    Err(error @ KvError::Busy { .. }) => {
                        return match self.read_snapshot_arc_if_present()? {
                            Some(snapshot) => Ok(snapshot),
                            None => Err(error),
                        };
                    }
                    Err(error) => return Err(error),
                };
            let snapshot = node.store().snapshot().canonicalized();
            if refresh_materialized {
                self.write_snapshot_if_changed(&snapshot)?;
            }
            let snapshot = Arc::new(snapshot);
            if let Some(identity) = raft_view_identity(&node.paths) {
                cache_store_raft_view(&node.paths, identity, snapshot.clone());
            }
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.read_snapshot_arc_if_present()? {
            return Ok(snapshot);
        }
        let snapshot = Arc::new(InProcessKvStore::new().snapshot().canonicalized());
        Ok(snapshot)
    }

    pub fn read_store(&self) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot().map(InProcessKvStore::restore)
    }

    /// Returns the snapshot that should actually be persisted: the input with its
    /// event history compacted to the configured materialized-event limit.
    fn materialized(&self, snapshot: &KvSnapshot) -> KvSnapshot {
        let mut snapshot = match self.max_materialized_events {
            Some(max_events) if snapshot.events.len() > max_events => {
                let mut snapshot = snapshot.clone();
                compact_kv_snapshot_event_history(&mut snapshot, max_events);
                snapshot
            }
            _ => snapshot.clone(),
        };
        snapshot.canonicalize();
        snapshot
    }

    /// Persists `snapshot` and refreshes the process-wide read cache so subsequent
    /// reads hit memory rather than re-reading the file.
    fn write_and_cache(&self, snapshot: KvSnapshot) -> Result<(), KvError> {
        self.write_and_cache_if_materialized_changed(snapshot)
            .map(|_| ())
    }

    /// Persists `snapshot` exactly as materialized and reports whether the object
    /// file changed. Unlike `write_snapshot_if_changed`, this treats revision and
    /// watch-event history as meaningful so callers that publish snapshot-backed
    /// events can suppress only truly redundant notifications.
    fn write_and_cache_if_materialized_changed(
        &self,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let wrote = write_object_file_if_changed(&self.path, &snapshot)?;
        if let Some(identity) = file_identity(&self.path) {
            cache_store_snapshot(&self.path, identity, Arc::new(snapshot));
        }
        Ok(wrote)
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.write_and_cache(self.materialized(snapshot))
    }

    pub fn write_snapshot_if_materialized_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.write_and_cache_if_materialized_changed(self.materialized(snapshot))
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        // Skip the rewrite when the durable key/value/lease state is unchanged, even
        // if the snapshot revision, per-entry revision metadata, or watch-event tail
        // advanced. Re-putting an identical value bumps modify_revision/version and
        // appends events, which would otherwise rewrite the whole (~25 KiB+)
        // materialized file for no durable change. The materialized snapshot is only
        // a fast-restore fallback for a cold node with an empty WAL (the WAL is the
        // source of truth for revision/version/events), so persisting stale bookkeeping
        // there is safe. Check before materializing `snapshot` so the common no-op path
        // avoids cloning the full entry set.
        if self.read_snapshot_arc_if_present().is_ok_and(|existing| {
            existing.is_some_and(|existing| kv_snapshot_durable_eq(existing.as_ref(), snapshot))
        }) {
            return Ok(false);
        }
        let desired = self.materialized(snapshot);
        self.write_and_cache(desired)?;
        Ok(true)
    }

    pub fn read_store_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot_with_raft_view(node_id, paths)
            .map(|snapshot| InProcessKvStore::restore((*snapshot).clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvLocalCache {
    materialized: KvMaterializedCache,
    raft_root: PathBuf,
}

impl KvLocalCache {
    pub fn new(materialized_snapshot: impl Into<PathBuf>, raft_root: impl Into<PathBuf>) -> Self {
        Self {
            materialized: KvMaterializedCache::new(materialized_snapshot),
            raft_root: raft_root.into(),
        }
    }

    /// Bound the persisted snapshot's event history to `max_events`, applied
    /// consistently across the explicit publish path and the read-triggered
    /// materialized-cache refresh.
    pub fn with_max_materialized_events(mut self, max_events: usize) -> Self {
        self.materialized = self.materialized.with_max_materialized_events(max_events);
        self
    }

    pub fn current_store(&self, node_id: NodeId) -> Result<InProcessKvStore, KvError> {
        self.materialized
            .read_store_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))
    }

    pub fn cached_store(&self) -> Result<InProcessKvStore, KvError> {
        self.materialized.read_store()
    }

    pub fn current_get(&self, node_id: NodeId, key: &str) -> Result<Option<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        Ok(snapshot.get(key).cloned())
    }

    pub fn current_get_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<T>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.get(key).map(|entry| entry.decode()).transpose()
    }

    pub fn current_range_prefix(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        Ok(snapshot.range_prefix(prefix))
    }

    pub fn current_range_prefix_limited(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_limited(prefix, max_entries)
    }

    pub fn current_range_prefix_limited_by_payload_bytes(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    }

    pub fn current_range_prefix_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<T>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot
            .prefix_entries(prefix)
            .map(|entry| entry.decode())
            .collect()
    }

    pub fn current_range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_serde_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    }

    pub fn current_snapshot(&self, node_id: NodeId) -> Result<KvSnapshot, KvError> {
        self.current_snapshot_arc(node_id)
            .map(|snapshot| (*snapshot).clone())
    }

    pub fn current_snapshot_arc(&self, node_id: NodeId) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))
    }

    pub fn current_snapshot_without_materialized_refresh(
        &self,
        node_id: NodeId,
    ) -> Result<KvSnapshot, KvError> {
        self.current_snapshot_arc_without_materialized_refresh(node_id)
            .map(|snapshot| (*snapshot).clone())
    }

    pub fn current_snapshot_arc_without_materialized_refresh(
        &self,
        node_id: NodeId,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized
            .read_snapshot_with_raft_view_without_materialized_refresh(
                node_id.clone(),
                self.raft_wal_paths(&node_id),
            )
    }

    pub fn cached_snapshot(&self) -> Result<KvSnapshot, KvError> {
        self.materialized.read_snapshot()
    }

    pub fn cached_snapshot_arc(&self) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized.read_snapshot_arc()
    }

    pub fn cached_snapshot_arc_if_present(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        self.materialized.read_snapshot_arc_if_present()
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.materialized.write_snapshot(snapshot)
    }

    pub fn write_snapshot_if_materialized_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.materialized
            .write_snapshot_if_materialized_changed(snapshot)
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        self.materialized.write_snapshot_if_changed(snapshot)
    }

    pub fn restore_raft_from_snapshot_if_newer(
        &self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        let node = DurableRaftKvNode::open_with_materialized_cache(
            node_id.clone(),
            wal_paths,
            &self.materialized,
        )?;
        if snapshot.revision <= node.store().revision() {
            self.materialized
                .write_snapshot_if_changed(&node.store().snapshot())?;
            return Ok(false);
        }
        self.materialized.write_snapshot_if_changed(&snapshot)?;
        if node.applied_index() == 0 && node.last_log_index() == 0 {
            drop(node);
            let mut node = self.open_raft_node(node_id)?;
            node.restore_snapshot(RaftSnapshot {
                last_included_index: 0,
                last_included_term: 0,
                kv: snapshot,
            })?;
        }
        Ok(true)
    }

    pub fn open_raft_node(&self, node_id: NodeId) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            &self.materialized,
        )
    }

    pub fn open_raft_node_discarding_uncommitted_tail(
        &self,
        node_id: NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_discarding_uncommitted_tail_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            &self.materialized,
        )
    }

    pub async fn open_raft_node_async(
        &self,
        node_id: NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_async_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            self.materialized.clone(),
        )
        .await
    }

    pub fn raft_wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        KvWalPaths::new(self.raft_root.join(node_id.as_str()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCommand {
    Op(KvOp),
    Txn(KvTxn),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftUncommittedAppend {
    Appended(KvLogEntry),
    NoEffect {
        snapshot: KvSnapshot,
        txn_response: Option<KvTxnResponse>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvRaftConfigRecord {
    pub cluster_id: Id,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderElectionOptions {
    pub max_attempts: usize,
}

pub trait ClusterKvRaftLeaderElectionHost {
    type Error: From<KvError>;

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
    matches!(member.state, ClusterMemberState::Voter)
        || (matches!(member.state, ClusterMemberState::Active)
            && matches!(member.role, ClusterMemberRole::Voter))
}

pub fn cluster_kv_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && cluster_kv_member_has_raft_vote(member)
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == sinomo_node::DAEMON_MAIN_RUN_CAPABILITY)
    })
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

pub fn cluster_kv_raft_voter_ids(state: &ClusterBootstrapState) -> BTreeSet<NodeId> {
    state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .map(|member| member.node_id.clone())
        .collect()
}

pub fn cluster_kv_raft_quorum_size_for_state(state: &ClusterBootstrapState) -> usize {
    cluster_kv_raft_quorum_size(cluster_kv_raft_voter_ids(state).len())
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
        let current_quorum = cluster_kv_raft_quorum_size(current_voters.len());
        let next_quorum = cluster_kv_raft_quorum_size(next_voters.len());
        if retained_voters.len() < current_quorum || retained_voters.len() < next_quorum {
            return Err(KvError::InvalidRaftConfigTransition {
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
        return Err(KvError::InvalidRaftConfigTransition {
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
    let voter_ids = voter_ids.into_iter().collect::<BTreeSet<_>>();
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let remote_vote_targets = remote_vote_targets
        .into_iter()
        .filter(|voter_id| voter_id != local_node_id && voter_ids.contains(voter_id))
        .collect::<Vec<_>>();

    for _ in 0..options.max_attempts {
        host.begin_election_attempt()?;
        let metadata = host.local_vote_metadata()?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: local_node_id.clone(),
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote(vote_request.clone())?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = BTreeSet::new();
        if local_vote.vote_granted {
            granted.insert(local_node_id.clone());
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
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
                granted.insert(voter_id.clone());
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
    type Error: From<KvError>;

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
    let voter_ids = voter_ids.into_iter().collect::<BTreeSet<_>>();
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let remote_vote_targets = remote_vote_targets
        .into_iter()
        .filter(|voter_id| voter_id != local_node_id && voter_ids.contains(voter_id))
        .collect::<Vec<_>>();

    for _ in 0..options.max_attempts {
        host.begin_election_attempt_async().await?;
        let metadata = host.local_vote_metadata_async().await?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: local_node_id.clone(),
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote_async(vote_request.clone()).await?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = BTreeSet::new();
        if local_vote.vote_granted {
            granted.insert(local_node_id.clone());
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
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
                granted.insert(voter_id.clone());
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
/// messages, score endpoints, or interpret the cluster connectivity graph.
pub trait ClusterKvRaftCommandStorage {
    type Error: From<KvError>;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(&mut self)
    -> Result<(), Self::Error>;

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitOptions {
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftTargetReplicationStatus {
    Accepted,
    Rejected,
    Failed,
}

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
    type Error: From<KvError>;
    type Target: Clone;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error>;

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
    type Error: From<KvError>;
    type Target: Clone + Send + Sync;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, u64, Self::Error>;

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
    let mut next_index = host.last_local_raft_log_index(state)?.saturating_add(1);
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
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else if response.append.match_index >= next_index {
            next_index.saturating_div(2).max(1)
        } else {
            next_index.saturating_sub(1).max(1)
        };
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
    let mut next_index = host
        .last_local_raft_log_index_async(state)
        .await?
        .saturating_add(1);
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
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else if response.append.match_index >= next_index {
            next_index.saturating_div(2).max(1)
        } else {
            next_index.saturating_sub(1).max(1)
        };
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

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

/// Async storage-side Raft quorum commit host.
///
/// This mirrors `ClusterKvRaftQuorumCommitHost`, but every operation that may
/// cross an async storage or transport boundary is supplied by the caller. KV
/// still owns the quorum decision flow; the host owns transport and topology.
pub trait ClusterKvRaftQuorumCommitAsyncHost {
    type Error: From<KvError> + ToString;
    type Target: Clone + Send + Sync;

    fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
        &'a mut self,
        command: KvCommand,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftUncommittedAppend, Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error>;

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

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub fn commit_cluster_kv_raft_command_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let entry = match host.append_cluster_raft_command_uncommitted_or_no_effect(command)? {
        ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
        ClusterKvRaftUncommittedAppend::NoEffect {
            snapshot,
            txn_response,
        } => {
            return Ok(ClusterKvRaftQuorumCommitResult {
                snapshot,
                txn_response,
            });
        }
    };

    let mut voter_acks = BTreeSet::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    let targets = host.remote_voter_targets(state)?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
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
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )?;
                voter_acks.insert(host.target_node_id(target).clone());
                host.observe_accepted_append_state(state, target, response.state)?;
            }
            Ok(response) => {
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )?;
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                )?;
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }

    if voter_acks.len() < quorum_size {
        let _ = host.discard_cluster_raft_uncommitted_tail_and_clear_leader();
        return Err(host.cluster_read_only_error(format!(
            "raft command at index {} replicated to {} voters, {} required{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        )));
    }

    let commit = host.commit_cluster_raft_index_with_events(entry.index)?;
    if !targets.is_empty() {
        host.fan_out_committed_cluster_raft_command(state.clone(), commit.events.clone(), targets)?;
    }
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

pub async fn commit_cluster_kv_raft_command_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let entry = match host
        .append_cluster_raft_command_uncommitted_or_no_effect_async(command)
        .await?
    {
        ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
        ClusterKvRaftUncommittedAppend::NoEffect {
            snapshot,
            txn_response,
        } => {
            return Ok(ClusterKvRaftQuorumCommitResult {
                snapshot,
                txn_response,
            });
        }
    };

    let mut voter_acks = BTreeSet::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    let targets = host.remote_voter_targets_async(state).await?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
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
        match host
            .replicate_cluster_raft_to_target_async(state, target, remaining)
            .await
        {
            Ok(response) if response.accepted => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )
                .await?;
                voter_acks.insert(host.target_node_id(target).clone());
                host.observe_accepted_append_state_async(state, target, response.state)
                    .await?;
            }
            Ok(response) => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )
                .await?;
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                )
                .await?;
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }

    if voter_acks.len() < quorum_size {
        let _ = host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader_async()
            .await;
        return Err(host.cluster_read_only_error(format!(
            "raft command at index {} replicated to {} voters, {} required{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        )));
    }

    let commit = host
        .commit_cluster_raft_index_with_events_async(entry.index)
        .await?;
    if !targets.is_empty() {
        host.fan_out_committed_cluster_raft_command_async(
            state.clone(),
            commit.events.clone(),
            targets,
        )
        .await?;
    }
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumDriverOptions {
    pub allow_forwarding: bool,
}

pub trait ClusterKvTxnQuorumHost {
    type Error: From<KvError> + ToString;

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
        txn: &KvTxn,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error>;

    fn semantic_txn_no_effect_response(
        &mut self,
        txn: &KvTxn,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub trait ClusterKvTxnQuorumAsyncHost {
    type Error: From<KvError> + ToString;

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
        txn: &'a KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, usize, Self::Error>;

    fn semantic_txn_no_effect_response_async<'a>(
        &'a mut self,
        txn: &'a KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub fn commit_cluster_kv_txn_with_quorum_with_host<H>(
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumHost,
{
    let _ = host.refresh_current_cluster_kv_store(state);
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host.forward_cluster_kv_txn_to_voter(state, &txn)?
        {
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count(state)?;
        return Err(host.cluster_read_only_error(format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        )));
    }

    if !host.local_is_raft_leader(state)? {
        let forwarding_failure = if options.allow_forwarding {
            match host.forward_cluster_kv_txn_to_voter(state, &txn) {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => None,
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if !options.allow_forwarding {
            return Err(host.cluster_read_only_error(
                "forwarded cluster KV transaction reached a non-leader voter".to_string(),
            ));
        }
        if !host.elect_cluster_raft_leader(state)? {
            let detail = if let Some(forwarding_failure) = forwarding_failure {
                format!(
                    "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                    forwarding_failure.to_string()
                )
            } else {
                "local node is not the Raft leader and could not win quorum".to_string()
            };
            return Err(host.cluster_read_only_error(detail));
        }
    }
    if !host.local_is_raft_leader(state)? {
        return Err(host.cluster_read_only_error(
            "local node is not the Raft leader and could not win quorum".to_string(),
        ));
    }
    if let Some(response) = host.semantic_txn_no_effect_response(&txn)? {
        return Ok(response);
    }
    let commit = host.commit_cluster_raft_txn_with_quorum(state, txn)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

pub async fn commit_cluster_kv_txn_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
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
                .forward_cluster_kv_txn_to_voter_async(state, &txn)
                .await?
        {
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        return Err(host.cluster_read_only_error(format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        )));
    }

    if !host.local_is_raft_leader_async(state).await? {
        let forwarding_failure = if options.allow_forwarding {
            match host
                .forward_cluster_kv_txn_to_voter_async(state, &txn)
                .await
            {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => None,
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if !options.allow_forwarding {
            return Err(host.cluster_read_only_error(
                "forwarded cluster KV transaction reached a non-leader voter".to_string(),
            ));
        }
        if !host.elect_cluster_raft_leader_async(state).await? {
            let detail = if let Some(forwarding_failure) = forwarding_failure {
                format!(
                    "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                    forwarding_failure.to_string()
                )
            } else {
                "local node is not the Raft leader and could not win quorum".to_string()
            };
            return Err(host.cluster_read_only_error(detail));
        }
    }
    if !host.local_is_raft_leader_async(state).await? {
        return Err(host.cluster_read_only_error(
            "local node is not the Raft leader and could not win quorum".to_string(),
        ));
    }
    if let Some(response) = host.semantic_txn_no_effect_response_async(&txn).await? {
        return Ok(response);
    }
    let commit = host
        .commit_cluster_raft_txn_with_quorum_async(state, txn)
        .await?;
    Ok(ClusterKvTxnCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
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
    if !cluster_kv_raft_node_should_compact(node, snapshot_bytes, policy) {
        return Ok(false);
    }
    node.compact_event_history_to(policy.max_snapshot_events);
    node.save_snapshot()?;
    Ok(true)
}

#[derive(Clone, Debug)]
pub struct ClusterKvRaftStore {
    paths: ClusterKvPaths,
    compaction_policy: ClusterKvRaftCompactionPolicy,
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

    pub fn wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        self.paths.raft_wal_paths(node_id)
    }

    fn snapshot_bytes(&self, node_id: &NodeId) -> u64 {
        fs::metadata(&self.wal_paths(node_id).snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    fn compact_node_if_needed(&self, node: &mut DurableRaftKvNode) -> Result<bool, KvError> {
        compact_cluster_kv_raft_node_if_needed(
            node,
            self.snapshot_bytes(node.node_id()),
            self.compaction_policy,
        )
    }

    pub fn open_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let cache = self.paths.local_cache();
        let mut node = cache.open_raft_node(node_id.clone())?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
    }

    pub fn open_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        let cache = self.paths.local_cache();
        let mut node = cache.open_raft_node_discarding_uncommitted_tail(node_id.clone())?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
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
        let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
        let earliest_log_index = node.log().first().map(|entry| entry.index);
        let prev_log_resolvable = prev_log_index == node.snapshot_index()
            || earliest_log_index.is_some_and(|first| prev_log_index >= first);
        let include_snapshot = node.snapshot_index() > 0 && !prev_log_resolvable;
        let (snapshot, prev_log_index, entries) = if include_snapshot {
            let mut kv = node.store().snapshot();
            compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
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
            let _raft_guard = lock_cluster_kv_raft_operation()?;
            store.build_cluster_raft_append_request_from_next_index(&state, next_index)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn restore_from_snapshot_if_newer(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        self.paths
            .local_cache()
            .restore_raft_from_snapshot_if_newer(node_id.clone(), snapshot)
    }

    pub fn restore_from_snapshot_if_newer_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let restored = self.restore_from_snapshot_if_newer(node_id, snapshot)?;
        if restored {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(restored)
    }

    pub fn locked_restore_from_snapshot_if_newer_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.local_vote_metadata(node_id)
    }

    pub fn last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.open_node(node_id).map(|node| node.last_log_index())
    }

    pub fn locked_last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.last_log_index(node_id)
    }

    pub async fn locked_last_log_index_async(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = lock_cluster_kv_raft_operation()?;
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
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
        let mut node = self.open_node(node_id)?;
        let outcome = node.apply_append_entries_with_optional_snapshot(snapshot, append)?;
        self.compact_node_if_needed(&mut node)?;
        Ok(outcome)
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.known_voter_leader_id(state)
    }

    pub fn local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        Ok(
            self.known_voter_leader_id(state)?.as_ref() == Some(&state.local_node_id)
                && cluster_kv_raft_local_is_voter(state),
        )
    }

    pub fn locked_local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.local_is_leader(state)
    }

    pub fn observe_term(&self, node_id: &NodeId, term: u64) -> Result<Option<KvSnapshot>, KvError> {
        let mut node = self.open_node(node_id)?;
        let changed = node.observe_term(term)?;
        self.compact_node_if_needed(&mut node)?;
        Ok(changed.then(|| node.store().snapshot()))
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
                .write_snapshot_if_changed(&snapshot)?;
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.observe_term_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn set_leader(&self, node_id: &NodeId, term: u64) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node(node_id)?;
        node.become_leader(term)?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node.store().snapshot())
    }

    pub fn set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.set_leader(node_id, term)?;
        self.paths
            .local_cache()
            .write_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn clear_leader(&self, node_id: &NodeId) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node(node_id)?;
        node.clear_known_leader()?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node.store().snapshot())
    }

    pub fn clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.clear_leader(node_id)?;
        self.paths
            .local_cache()
            .write_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.clear_leader_and_write_snapshot_if_changed(node_id)
    }

    pub fn append_command_with_response(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        if let Some((snapshot, txn_response)) = Self::command_no_effect_response(&node, &command)? {
            return Ok(ClusterKvRaftCommitResult {
                snapshot,
                txn_response,
                events: Vec::new(),
            });
        }
        let before_revision = node.store().revision();
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_command(term, command)?;
        node.commit_to(entry.index)?;
        let txn_response = node.txn_response_at(entry.index).cloned();
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().snapshot();
        let events = Self::events_after_revision(&snapshot, before_revision);
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
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
            let wrote = write_cluster_kv_snapshot_if_changed(&self.paths, &outcome.snapshot)?;
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
        self.paths.local_cache().write_snapshot_if_changed(snapshot)
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

    pub fn append_command_uncommitted_or_no_effect(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        if let Some((snapshot, txn_response)) = Self::command_no_effect_response(&node, &command)? {
            return Ok(ClusterKvRaftUncommittedAppend::NoEffect {
                snapshot,
                txn_response,
            });
        }
        self.append_command_to_open_node(&mut node, command)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn locked_append_command_uncommitted_or_no_effect(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
        self.append_command_uncommitted_or_no_effect(node_id, command)
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
        Ok(node.store().snapshot())
    }

    pub fn discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let snapshot = self.discard_uncommitted_tail_and_clear_leader(node_id)?;
        let wrote = write_cluster_kv_snapshot_if_changed(&self.paths, &snapshot)?;
        Ok((snapshot, wrote))
    }

    pub fn locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation()?;
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
        let snapshot = node.store().snapshot();
        let events = Self::events_after_revision(&snapshot, before_revision);
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
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
        let _raft_guard = lock_cluster_kv_raft_operation()?;
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

    fn command_no_effect_response(
        node: &DurableRaftKvNode,
        command: &KvCommand,
    ) -> Result<Option<(KvSnapshot, Option<KvTxnResponse>)>, KvError> {
        match command {
            KvCommand::Op(op) => {
                if node.store().op_response_if_no_effect(op)?.is_some() {
                    return Ok(Some((node.store().snapshot(), None)));
                }
            }
            KvCommand::Txn(txn) => {
                if let Some(response) = node
                    .store()
                    .txn_response_if_selected_branch_no_effect(txn)?
                {
                    return Ok(Some((node.store().snapshot(), Some(response))));
                }
            }
        }
        Ok(None)
    }

    fn events_after_revision(snapshot: &KvSnapshot, before_revision: KvRevision) -> Vec<KvEvent> {
        snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
            .cloned()
            .collect()
    }
}

pub struct ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    store: ClusterKvRaftStore,
    node_id: NodeId,
    key_can_defer_materialized_snapshot: F,
    last_snapshot_publish_wrote: Option<bool>,
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
            key_can_defer_materialized_snapshot,
            last_snapshot_publish_wrote: None,
        }
    }

    pub fn take_last_snapshot_publish_wrote(&mut self) -> Option<bool> {
        self.last_snapshot_publish_wrote.take()
    }

    pub fn append_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        self.last_snapshot_publish_wrote = None;
        self.store
            .locked_append_command_uncommitted_or_no_effect(&self.node_id, command)
    }

    pub fn discard_uncommitted_tail_and_clear_leader(&mut self) -> Result<KvSnapshot, KvError> {
        self.last_snapshot_publish_wrote = None;
        let (snapshot, wrote) = self
            .store
            .locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(&self.node_id)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(snapshot)
    }

    pub fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.last_snapshot_publish_wrote = None;
        let (result, wrote) = self
            .store
            .locked_commit_index_with_events_and_publish_report(
                &self.node_id,
                index,
                &mut self.key_can_defer_materialized_snapshot,
            )?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(result)
    }
}

impl<F> ClusterKvRaftCommandStorage for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    type Error = KvError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error> {
        self.append_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), Self::Error> {
        self.discard_uncommitted_tail_and_clear_leader().map(|_| ())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
        self.commit_index_with_events(index)
    }
}

#[derive(Clone, Debug, Default)]
pub struct InProcessKvStore {
    revision: KvRevision,
    compacted_revision: KvRevision,
    entries: BTreeMap<String, KvEntry>,
    events: Vec<Arc<KvEvent>>,
    leases: BTreeMap<LeaseId, KvLease>,
    lease_clock: u64,
    lease_deadline_by_id: BTreeMap<LeaseId, u64>,
    lease_deadlines: BTreeMap<u64, BTreeSet<LeaseId>>,
    lease_keys: BTreeMap<LeaseId, BTreeSet<String>>,
    watches: BTreeMap<WatchId, KvWatchState>,
    watch_prefixes: BTreeMap<String, BTreeSet<WatchId>>,
    next_watch_id: u64,
}

impl InProcessKvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    pub fn compacted_revision(&self) -> KvRevision {
        self.compacted_revision
    }

    pub fn event_history_len(&self) -> usize {
        self.events.len()
    }

    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        self.entries.get(key)
    }

    pub fn range_prefix(&self, prefix: &str) -> Vec<KvEntry> {
        self.prefix_entries(prefix).cloned().collect()
    }

    pub fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        self.entries
            .range(prefix.to_string()..)
            .map_while(move |(key, entry)| key.starts_with(prefix).then_some(entry))
    }

    pub fn range_prefix_limited(
        &self,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, usize::MAX)
    }

    pub fn range_prefix_limited_by_payload_bytes(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        let mut payload_bytes = 0_usize;
        for entry in self.prefix_entries(prefix) {
            if entries.len() >= max_entries {
                return Err(KvError::RangeLimit {
                    prefix: prefix.to_string(),
                    max_entries,
                });
            }
            payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
            if payload_bytes > max_payload_bytes {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_string(),
                    observed_bytes: payload_bytes,
                    max_bytes: max_payload_bytes,
                });
            }
            entries.push(entry.clone());
        }
        Ok(entries)
    }

    pub fn put(
        &mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvRevision, KvError> {
        self.apply_op(KvOp::Put {
            key: key.into(),
            value,
            lease_id,
        })
        .map(|response| response.revision())
    }

    pub fn put_serde<T: Serialize + ?Sized>(
        &mut self,
        key: impl Into<String>,
        value: &T,
        lease_id: Option<LeaseId>,
    ) -> Result<KvRevision, KvError> {
        self.apply_op(KvOp::put_serde(key, value, lease_id)?)
            .map(|response| response.revision())
    }

    pub fn get_serde<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, KvError> {
        self.get(key).map(KvEntry::decode).transpose()
    }

    pub fn range_prefix_serde<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<T>, KvError> {
        self.range_prefix(prefix)
            .iter()
            .map(KvEntry::decode)
            .collect()
    }

    pub fn range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)?
            .iter()
            .map(KvEntry::decode)
            .collect()
    }

    pub fn delete(&mut self, key: impl Into<String>) -> Result<bool, KvError> {
        match self.apply_op(KvOp::Delete { key: key.into() })? {
            KvOpResponse::Delete { deleted, .. } => Ok(deleted),
            _ => unreachable!("delete response"),
        }
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.validate_txn_shape(&txn)?;
        validate_kv_txn_payload(&txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded { txn.success } else { txn.failure };
        self.validate_ops(&ops)?;
        let mut responses = Vec::with_capacity(ops.len());
        let mut revision = self.revision;

        for op in ops {
            if let Some(response) = self.no_effect_response_for_op(&op) {
                responses.push(response);
                continue;
            }
            let response = self.apply_op(op)?;
            revision = self.revision;
            responses.push(response);
        }
        Ok(KvTxnResponse {
            succeeded,
            revision,
            responses,
        })
    }

    pub fn txn_response_if_selected_branch_empty(
        &self,
        txn: &KvTxn,
    ) -> Result<Option<KvTxnResponse>, KvError> {
        self.validate_txn_shape(txn)?;
        validate_kv_txn_payload(txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let selected_branch_is_empty = if succeeded {
            txn.success.is_empty()
        } else {
            txn.failure.is_empty()
        };
        if !selected_branch_is_empty {
            return Ok(None);
        }
        Ok(Some(KvTxnResponse {
            succeeded,
            revision: self.revision,
            responses: Vec::new(),
        }))
    }

    pub fn txn_response_if_selected_branch_no_effect(
        &self,
        txn: &KvTxn,
    ) -> Result<Option<KvTxnResponse>, KvError> {
        self.validate_txn_shape(txn)?;
        validate_kv_txn_payload(txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded {
            txn.success.as_slice()
        } else {
            txn.failure.as_slice()
        };
        self.validate_ops(ops)?;

        let mut responses = Vec::with_capacity(ops.len());
        for op in ops {
            let Some(response) = self.no_effect_response_for_op(op) else {
                return Ok(None);
            };
            responses.push(response);
        }
        Ok(Some(KvTxnResponse {
            succeeded,
            revision: self.revision,
            responses,
        }))
    }

    pub fn op_response_if_no_effect(&self, op: &KvOp) -> Result<Option<KvOpResponse>, KvError> {
        validate_kv_op_payload(op)?;
        validate_kv_payload_total(kv_op_payload_bytes(op))?;
        self.validate_ops(std::slice::from_ref(op))?;
        Ok(self.no_effect_response_for_op(op))
    }

    pub fn apply_command(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        self.apply_command_with_response(command)
            .map(|(revision, _)| revision)
    }

    pub fn apply_command_with_response(
        &mut self,
        command: KvCommand,
    ) -> Result<(KvRevision, Option<KvTxnResponse>), KvError> {
        validate_kv_command_payload(&command)?;
        match command {
            KvCommand::Op(op) => {
                self.apply_op(op)?;
                Ok((self.revision, None))
            }
            KvCommand::Txn(txn) => {
                let response = self.txn(txn)?;
                Ok((response.revision, Some(response)))
            }
        }
    }

    fn preflight_apply_command(&self, command: &KvCommand) -> Result<(), KvError> {
        validate_kv_command_payload(command)?;
        match command {
            KvCommand::Op(op) => self.validate_ops(std::slice::from_ref(op)),
            KvCommand::Txn(txn) => {
                self.validate_txn_shape(txn)?;
                validate_kv_txn_payload(txn)?;
                let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
                let ops = if succeeded {
                    txn.success.as_slice()
                } else {
                    txn.failure.as_slice()
                };
                self.validate_ops(ops)
            }
        }
    }

    pub fn watch_prefix(
        &mut self,
        prefix: impl Into<String>,
        start_revision: KvRevision,
    ) -> Result<WatchId, KvError> {
        if start_revision <= self.compacted_revision && self.compacted_revision != KvRevision::ZERO
        {
            return Err(KvError::Compacted {
                requested: start_revision,
                compacted: self.compacted_revision,
            });
        }
        if self.watches.len() >= MAX_ACTIVE_WATCHES {
            return Err(KvError::WatchLimit {
                max: MAX_ACTIVE_WATCHES,
            });
        }
        if self.next_watch_id == u64::MAX {
            return Err(KvError::WatchIdExhausted);
        }

        let watch_id = WatchId(self.next_watch_id.max(1));
        self.next_watch_id = watch_id.0.saturating_add(1);
        let prefix = prefix.into();
        let backlog = self
            .events
            .iter()
            .filter(|event| event.revision >= start_revision && event.key.starts_with(&prefix))
            .cloned()
            .collect();
        let mut watch = KvWatchState {
            watch: KvWatch {
                watch_id,
                prefix,
                start_revision,
            },
            pending: backlog,
            compacted_revision: None,
        };
        compact_watch_pending(&mut watch);
        self.index_watch_prefix(watch_id, &watch.watch.prefix);
        self.watches.insert(watch_id, watch);
        Ok(watch_id)
    }

    pub fn drain_watch(&mut self, watch_id: WatchId) -> Result<Vec<KvEvent>, KvError> {
        if let Some(compacted) = self
            .watches
            .get(&watch_id)
            .and_then(|watch| watch.compacted_revision)
        {
            let requested = self
                .remove_watch(watch_id)
                .map(|watch| watch.watch.start_revision)
                .unwrap_or(KvRevision::ZERO);
            return Err(KvError::Compacted {
                requested,
                compacted,
            });
        }
        let watch = self
            .watches
            .get_mut(&watch_id)
            .ok_or(KvError::UnknownWatch { watch_id })?;
        Ok(watch
            .pending
            .drain(..)
            .map(|event| event.as_ref().clone())
            .collect())
    }

    pub fn cancel_watch(&mut self, watch_id: WatchId) -> Result<bool, KvError> {
        Ok(self.remove_watch(watch_id).is_some())
    }

    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries: self.entries.values().cloned().collect(),
            leases: self.current_leases_for_snapshot(),
            events: self
                .events
                .iter()
                .map(|event| event.as_ref().clone())
                .collect(),
        }
    }

    pub fn restore(snapshot: KvSnapshot) -> Self {
        let entries: BTreeMap<_, _> = snapshot
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        let lease_keys = build_lease_key_index(&entries);
        let leases: BTreeMap<_, _> = snapshot
            .leases
            .into_iter()
            .map(|lease| (lease.lease_id, lease))
            .collect();
        let (lease_deadline_by_id, lease_deadlines) = build_lease_deadline_index(&leases, 0);
        Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
            entries,
            events: snapshot.events.into_iter().map(Arc::new).collect(),
            leases,
            lease_clock: 0,
            lease_deadline_by_id,
            lease_deadlines,
            lease_keys,
            watches: BTreeMap::new(),
            watch_prefixes: BTreeMap::new(),
            next_watch_id: 1,
        }
    }

    pub fn compact(&mut self, revision: KvRevision) -> Result<(), KvError> {
        if revision > self.revision {
            return Err(KvError::RevisionAhead {
                requested: revision,
                current: self.revision,
            });
        }
        if revision > self.compacted_revision {
            self.compacted_revision = revision;
            self.events.retain(|event| event.revision > revision);
        }
        Ok(())
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        compact_shared_event_history(&mut self.events, &mut self.compacted_revision, max_events);
    }

    pub fn tick_leases(&mut self, ticks: u64) -> Result<Vec<LeaseId>, KvError> {
        self.lease_clock = self.lease_clock.saturating_add(ticks);
        let candidates = self.expired_lease_candidates();

        let mut expired = Vec::new();
        let mut mutation_events = 0_usize;
        for lease_id in candidates {
            if let Some(lease) = self.leases.get_mut(&lease_id) {
                lease.remaining_ticks = 0;
            } else {
                self.remove_lease_deadline(lease_id);
                continue;
            }
            let remaining_budget =
                MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(mutation_events);
            let (keys, complete) = self.lease_revoke_batch(lease_id, remaining_budget);
            if keys.is_empty() && !complete {
                break;
            }
            mutation_events = mutation_events.saturating_add(keys.len());
            if complete {
                self.remove_lease_and_delete_keys(lease_id, keys)?;
                expired.push(lease_id);
            } else {
                self.delete_lease_keys(keys)?;
                break;
            }
        }

        Ok(expired)
    }

    fn apply_op(&mut self, op: KvOp) -> Result<KvOpResponse, KvError> {
        match op {
            KvOp::Put {
                key,
                value,
                lease_id,
            } => self.apply_put(key, value, lease_id),
            KvOp::Delete { key } => self.apply_delete(key),
            KvOp::DeletePrefix { prefix } => self.apply_delete_prefix(prefix),
            KvOp::LeaseGrant {
                lease_id,
                ttl_ticks,
            } => {
                if ttl_ticks == 0 {
                    return Err(KvError::InvalidLeaseTtl { ttl_ticks });
                }
                if !self.leases.contains_key(&lease_id) && self.leases.len() >= MAX_ACTIVE_LEASES {
                    return Err(KvError::LeaseLimit {
                        max: MAX_ACTIVE_LEASES,
                    });
                }
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks));
                Ok(KvOpResponse::LeaseGrant { lease_id })
            }
            KvOp::LeaseKeepAlive { lease_id } => {
                let ttl_ticks = self
                    .leases
                    .get(&lease_id)
                    .ok_or(KvError::UnknownLease { lease_id })?
                    .ttl_ticks;
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks));
                Ok(KvOpResponse::LeaseKeepAlive { lease_id })
            }
            KvOp::LeaseRevoke { lease_id } => {
                let keys =
                    self.keys_for_lease_limited(lease_id, MAX_KV_MUTATION_EVENTS_PER_COMMAND)?;
                self.remove_lease_and_delete_keys(lease_id, keys)
            }
        }
    }

    fn validate_txn_shape(&self, txn: &KvTxn) -> Result<(), KvError> {
        if txn.compares.len() > MAX_KV_TXN_COMPARES
            || txn.success.len() > MAX_KV_TXN_OPS_PER_BRANCH
            || txn.failure.len() > MAX_KV_TXN_OPS_PER_BRANCH
        {
            return Err(KvError::TxnLimit {
                compares: txn.compares.len(),
                success_ops: txn.success.len(),
                failure_ops: txn.failure.len(),
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            });
        }
        Ok(())
    }

    fn validate_ops(&self, ops: &[KvOp]) -> Result<(), KvError> {
        let mut active_leases = self.leases.keys().copied().collect::<BTreeSet<_>>();
        let mut touched_keys = BTreeMap::<String, SimulatedKeyState>::new();
        let mut deleted_prefixes = Vec::<String>::new();
        let mut revoked_leases = Vec::<LeaseId>::new();
        let mut mutation_events = 0_usize;
        for op in ops {
            validate_kv_op_payload(op)?;
            match op {
                KvOp::Put {
                    key,
                    value,
                    lease_id: Some(lease_id),
                } => {
                    if !active_leases.contains(lease_id) {
                        return Err(KvError::UnknownLease {
                            lease_id: *lease_id,
                        });
                    }
                    let old_state = self.simulated_key_state(
                        key,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    if old_state.lease_id != Some(*lease_id) {
                        let lease_keys = self.simulated_lease_key_count(
                            *lease_id,
                            &touched_keys,
                            &deleted_prefixes,
                            &revoked_leases,
                        );
                        if lease_keys >= MAX_KEYS_PER_LEASE {
                            return Err(KvError::LeaseKeyLimit {
                                lease_id: *lease_id,
                                observed_keys: lease_keys.saturating_add(1),
                                max_keys: MAX_KEYS_PER_LEASE,
                            });
                        }
                    }
                    if !simulated_put_is_no_effect(&old_state, value, Some(*lease_id)) {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.insert(
                        key.clone(),
                        SimulatedKeyState::present_with_value(Some(*lease_id), value),
                    );
                }
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                } => {
                    let old_state = self.simulated_key_state(
                        key,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    if !simulated_put_is_no_effect(&old_state, value, None) {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.insert(
                        key.clone(),
                        SimulatedKeyState::present_with_value(None, value),
                    );
                }
                KvOp::Delete { key } => {
                    if self
                        .simulated_key_state(key, &touched_keys, &deleted_prefixes, &revoked_leases)
                        .present
                    {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.insert(key.clone(), SimulatedKeyState::ABSENT);
                }
                KvOp::DeletePrefix { prefix } => {
                    let deleted_count = self.simulated_delete_prefix_count(
                        prefix,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                        MAX_DELETE_PREFIX_KEYS,
                    )?;
                    record_kv_mutation_events(&mut mutation_events, deleted_count)?;
                    for (key, state) in &mut touched_keys {
                        if key.starts_with(prefix) {
                            *state = SimulatedKeyState::ABSENT;
                        }
                    }
                    deleted_prefixes.push(prefix.clone());
                }
                KvOp::LeaseGrant {
                    lease_id,
                    ttl_ticks,
                } => {
                    if *ttl_ticks == 0 {
                        return Err(KvError::InvalidLeaseTtl {
                            ttl_ticks: *ttl_ticks,
                        });
                    }
                    if !active_leases.contains(lease_id) && active_leases.len() >= MAX_ACTIVE_LEASES
                    {
                        return Err(KvError::LeaseLimit {
                            max: MAX_ACTIVE_LEASES,
                        });
                    }
                    active_leases.insert(*lease_id);
                }
                KvOp::LeaseKeepAlive { lease_id } => {
                    if !active_leases.contains(lease_id) {
                        return Err(KvError::UnknownLease {
                            lease_id: *lease_id,
                        });
                    }
                }
                KvOp::LeaseRevoke { lease_id } => {
                    let deleted_count = self.simulated_lease_revoke_count(
                        *lease_id,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    record_kv_mutation_events(&mut mutation_events, deleted_count)?;
                    active_leases.remove(lease_id);
                    for state in touched_keys.values_mut() {
                        if state.present && state.lease_id == Some(*lease_id) {
                            *state = SimulatedKeyState::ABSENT;
                        }
                    }
                    revoked_leases.push(*lease_id);
                }
            }
        }
        Ok(())
    }

    fn apply_put(
        &mut self,
        key: String,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOpResponse, KvError> {
        validate_kv_value_len(value.len())?;
        if let Some(lease_id) = lease_id {
            if !self.leases.contains_key(&lease_id) {
                return Err(KvError::UnknownLease { lease_id });
            }
            let old_lease_id = self.entries.get(&key).and_then(|entry| entry.lease_id);
            if old_lease_id != Some(lease_id) {
                self.validate_lease_key_capacity(lease_id)?;
            }
        }

        let revision = self.bump_revision();
        let old_lease_id = self.entries.get(&key).and_then(|entry| entry.lease_id);
        let entry = match self.entries.remove(&key) {
            Some(mut existing) => {
                existing.value = value.clone();
                existing.modify_revision = revision;
                existing.version = KvVersion(existing.version.0.saturating_add(1));
                existing.lease_id = lease_id;
                existing
            }
            None => KvEntry {
                key: key.clone(),
                value: value.clone(),
                create_revision: revision,
                modify_revision: revision,
                version: KvVersion(1),
                lease_id,
            },
        };
        if let Some(old_lease_id) = old_lease_id {
            self.remove_lease_key(old_lease_id, &key);
        }
        if let Some(lease_id) = lease_id {
            self.add_lease_key(lease_id, key.clone());
        }
        self.entries.insert(key.clone(), entry);
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Put { value },
        });
        Ok(KvOpResponse::Put { key, revision })
    }

    fn apply_delete(&mut self, key: String) -> Result<KvOpResponse, KvError> {
        let Some(entry) = self.entries.remove(&key) else {
            return Ok(KvOpResponse::Delete {
                key,
                deleted: false,
            });
        };
        if let Some(lease_id) = entry.lease_id {
            self.remove_lease_key(lease_id, &key);
        }
        let revision = self.bump_revision();
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Delete,
        });
        Ok(KvOpResponse::Delete { key, deleted: true })
    }

    fn apply_delete_prefix(&mut self, prefix: String) -> Result<KvOpResponse, KvError> {
        let keys = self.keys_with_prefix_limited(&prefix, MAX_DELETE_PREFIX_KEYS)?;
        let deleted_count = keys.len();
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        })
    }

    fn keys_with_prefix_limited(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<String>, KvError> {
        let mut keys = Vec::new();
        for key in self.entries.range(prefix.to_string()..).map(|(key, _)| key) {
            if !key.starts_with(prefix) {
                break;
            }
            if keys.len() >= max_keys {
                return Err(KvError::DeletePrefixLimit {
                    prefix: prefix.to_string(),
                    observed_keys: max_keys.saturating_add(1),
                    max_keys,
                });
            }
            keys.push(key.clone());
        }
        Ok(keys)
    }

    fn simulated_delete_prefix_count(
        &self,
        prefix: &str,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
        max_keys: usize,
    ) -> Result<usize, KvError> {
        let mut count = 0_usize;
        for key in self.entries.range(prefix.to_string()..).map(|(key, _)| key) {
            if !key.starts_with(prefix) {
                break;
            }
            if self
                .simulated_key_state(key, touched_keys, deleted_prefixes, revoked_leases)
                .present
            {
                count = count.saturating_add(1);
                if count > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: count,
                        max_keys,
                    });
                }
            }
        }
        for (key, state) in touched_keys {
            if state.present && key.starts_with(prefix) && !self.entries.contains_key(key) {
                count = count.saturating_add(1);
                if count > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: count,
                        max_keys,
                    });
                }
            }
        }
        Ok(count)
    }

    fn simulated_lease_revoke_count(
        &self,
        lease_id: LeaseId,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> usize {
        self.simulated_lease_key_count(lease_id, touched_keys, deleted_prefixes, revoked_leases)
    }

    fn simulated_lease_key_count(
        &self,
        lease_id: LeaseId,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> usize {
        let mut count = 0_usize;
        if let Some(keys) = self.lease_keys.get(&lease_id) {
            for key in keys {
                let state =
                    self.simulated_key_state(key, touched_keys, deleted_prefixes, revoked_leases);
                if state.present && state.lease_id == Some(lease_id) {
                    count = count.saturating_add(1);
                }
            }
        }
        for (key, state) in touched_keys {
            if state.present
                && state.lease_id == Some(lease_id)
                && !self
                    .lease_keys
                    .get(&lease_id)
                    .is_some_and(|keys| keys.contains(key))
            {
                count = count.saturating_add(1);
            }
        }
        count
    }

    fn validate_lease_key_capacity(&self, lease_id: LeaseId) -> Result<(), KvError> {
        let lease_keys = self
            .lease_keys
            .get(&lease_id)
            .map(BTreeSet::len)
            .unwrap_or_default();
        if lease_keys >= MAX_KEYS_PER_LEASE {
            return Err(KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: lease_keys.saturating_add(1),
                max_keys: MAX_KEYS_PER_LEASE,
            });
        }
        Ok(())
    }

    fn no_effect_response_for_op(&self, op: &KvOp) -> Option<KvOpResponse> {
        match op {
            KvOp::Put {
                key,
                value,
                lease_id,
            } => self.entries.get(key).and_then(|entry| {
                (entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
                    .then_some(KvOpResponse::Put {
                        key: key.clone(),
                        revision: entry.modify_revision,
                    })
            }),
            KvOp::Delete { key } => {
                (!self.entries.contains_key(key)).then(|| KvOpResponse::Delete {
                    key: key.clone(),
                    deleted: false,
                })
            }
            KvOp::DeletePrefix { prefix } => {
                let mut entries = self.entries.range(prefix.clone()..);
                let has_matching_key = entries
                    .next()
                    .is_some_and(|(key, _)| key.starts_with(prefix));
                (!has_matching_key).then(|| KvOpResponse::DeletePrefix {
                    prefix: prefix.clone(),
                    deleted_count: 0,
                })
            }
            KvOp::LeaseGrant {
                lease_id,
                ttl_ticks,
            } => self.leases.get(lease_id).and_then(|lease| {
                (lease.ttl_ticks == *ttl_ticks
                    && self.lease_remaining_ticks(*lease_id) == Some(lease.ttl_ticks))
                .then_some(KvOpResponse::LeaseGrant {
                    lease_id: *lease_id,
                })
            }),
            KvOp::LeaseKeepAlive { lease_id } => {
                let lease = self.leases.get(lease_id)?;
                (self.lease_remaining_ticks(*lease_id) == Some(lease.ttl_ticks)).then_some(
                    KvOpResponse::LeaseKeepAlive {
                        lease_id: *lease_id,
                    },
                )
            }
            KvOp::LeaseRevoke { lease_id } => {
                (!self.leases.contains_key(lease_id)).then_some(KvOpResponse::LeaseRevoke {
                    lease_id: *lease_id,
                    deleted_count: 0,
                })
            }
        }
    }

    fn keys_for_lease_limited(
        &self,
        lease_id: LeaseId,
        max_keys: usize,
    ) -> Result<Vec<String>, KvError> {
        let Some(keys) = self.lease_keys.get(&lease_id) else {
            return Ok(Vec::new());
        };
        if keys.len() > max_keys {
            return Err(KvError::MutationEventLimit {
                observed_events: keys.len(),
                max_events: max_keys,
            });
        }
        Ok(keys.iter().cloned().collect())
    }

    fn lease_revoke_batch(&self, lease_id: LeaseId, max_keys: usize) -> (Vec<String>, bool) {
        let Some(keys) = self.lease_keys.get(&lease_id) else {
            return (Vec::new(), true);
        };
        let complete = keys.len() <= max_keys;
        (keys.iter().take(max_keys).cloned().collect(), complete)
    }

    fn current_leases_for_snapshot(&self) -> Vec<KvLease> {
        self.leases
            .values()
            .map(|lease| {
                let mut lease = lease.clone();
                lease.remaining_ticks = self
                    .lease_remaining_ticks(lease.lease_id)
                    .unwrap_or(lease.remaining_ticks);
                lease
            })
            .collect()
    }

    fn lease_remaining_ticks(&self, lease_id: LeaseId) -> Option<u64> {
        let lease = self.leases.get(&lease_id)?;
        let deadline = self.lease_deadline_by_id.get(&lease_id).copied()?;
        Some(
            deadline
                .saturating_sub(self.lease_clock)
                .min(lease.ttl_ticks),
        )
    }

    fn expired_lease_candidates(&self) -> Vec<LeaseId> {
        self.lease_deadlines
            .range(..=self.lease_clock)
            .flat_map(|(_, lease_ids)| lease_ids.iter().copied())
            .collect()
    }

    fn upsert_lease(&mut self, lease: KvLease) {
        let lease_id = lease.lease_id;
        self.remove_lease_deadline(lease_id);
        let deadline = self.lease_clock.saturating_add(lease.remaining_ticks);
        self.lease_deadline_by_id.insert(lease_id, deadline);
        self.lease_deadlines
            .entry(deadline)
            .or_default()
            .insert(lease_id);
        self.leases.insert(lease_id, lease);
    }

    fn remove_lease_deadline(&mut self, lease_id: LeaseId) -> Option<u64> {
        let deadline = self.lease_deadline_by_id.remove(&lease_id)?;
        let should_remove_deadline =
            if let Some(lease_ids) = self.lease_deadlines.get_mut(&deadline) {
                lease_ids.remove(&lease_id);
                lease_ids.is_empty()
            } else {
                false
            };
        if should_remove_deadline {
            self.lease_deadlines.remove(&deadline);
        }
        Some(deadline)
    }

    fn remove_lease_and_delete_keys(
        &mut self,
        lease_id: LeaseId,
        keys: Vec<String>,
    ) -> Result<KvOpResponse, KvError> {
        self.leases.remove(&lease_id);
        self.remove_lease_deadline(lease_id);
        self.lease_keys.remove(&lease_id);
        let deleted_count = keys.len();
        self.delete_lease_keys(keys)?;
        Ok(KvOpResponse::LeaseRevoke {
            lease_id,
            deleted_count,
        })
    }

    fn delete_lease_keys(&mut self, keys: Vec<String>) -> Result<(), KvError> {
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(())
    }

    fn simulated_key_state(
        &self,
        key: &str,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> SimulatedKeyState {
        if let Some(state) = touched_keys.get(key) {
            return *state;
        }
        if deleted_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            return SimulatedKeyState::ABSENT;
        }
        let Some(entry) = self.entries.get(key) else {
            return SimulatedKeyState::ABSENT;
        };
        if entry
            .lease_id
            .is_some_and(|lease_id| revoked_leases.contains(&lease_id))
        {
            return SimulatedKeyState::ABSENT;
        }
        SimulatedKeyState::present_with_value(entry.lease_id, &entry.value)
    }

    fn compare(&self, compare: &KvCompare) -> bool {
        match compare {
            KvCompare::Exists { key } => self.entries.contains_key(key),
            KvCompare::Missing { key } => !self.entries.contains_key(key),
            KvCompare::ValueEquals { key, value } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.value == value),
            KvCompare::VersionEquals { key, version } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.version == version),
            KvCompare::CreateRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.create_revision == revision),
            KvCompare::ModifyRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.modify_revision == revision),
            KvCompare::LeaseEquals { key, lease_id } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.lease_id == lease_id),
        }
    }

    fn bump_revision(&mut self) -> KvRevision {
        self.revision = self.revision.next();
        self.revision
    }

    fn record_event(&mut self, event: KvEvent) {
        let event = Arc::new(event);
        for watch_id in self.matching_watch_ids_for_event_key(&event.key) {
            if let Some(watch) = self.watches.get_mut(&watch_id)
                && event.revision >= watch.watch.start_revision
            {
                watch.pending.push_back(Arc::clone(&event));
                compact_watch_pending(watch);
            }
        }
        self.events.push(event);
        self.trim_event_history();
    }

    fn index_watch_prefix(&mut self, watch_id: WatchId, prefix: &str) {
        self.watch_prefixes
            .entry(prefix.to_string())
            .or_default()
            .insert(watch_id);
    }

    fn remove_watch(&mut self, watch_id: WatchId) -> Option<KvWatchState> {
        let watch = self.watches.remove(&watch_id)?;
        self.remove_watch_prefix_index(watch_id, &watch.watch.prefix);
        Some(watch)
    }

    fn remove_watch_prefix_index(&mut self, watch_id: WatchId, prefix: &str) {
        let should_remove = if let Some(watch_ids) = self.watch_prefixes.get_mut(prefix) {
            watch_ids.remove(&watch_id);
            watch_ids.is_empty()
        } else {
            false
        };
        if should_remove {
            self.watch_prefixes.remove(prefix);
        }
    }

    fn matching_watch_ids_for_event_key(&self, key: &str) -> Vec<WatchId> {
        let mut watch_ids = Vec::new();
        if let Some(prefix_watch_ids) = self.watch_prefixes.get("") {
            watch_ids.extend(prefix_watch_ids.iter().copied());
        }
        for (start, ch) in key.char_indices() {
            let end = start + ch.len_utf8();
            if let Some(prefix_watch_ids) = self.watch_prefixes.get(&key[..end]) {
                watch_ids.extend(prefix_watch_ids.iter().copied());
            }
        }
        watch_ids
    }

    #[cfg(test)]
    fn matching_watch_ids_for_event_key_for_test(&self, key: &str) -> Vec<WatchId> {
        let mut watch_ids = self.matching_watch_ids_for_event_key(key);
        watch_ids.sort();
        watch_ids
    }

    fn trim_event_history(&mut self) {
        self.compact_event_history_to(MAX_RETAINED_EVENTS);
    }

    fn add_lease_key(&mut self, lease_id: LeaseId, key: String) {
        self.lease_keys.entry(lease_id).or_default().insert(key);
    }

    fn remove_lease_key(&mut self, lease_id: LeaseId, key: &str) {
        let Some(keys) = self.lease_keys.get_mut(&lease_id) else {
            return;
        };
        keys.remove(key);
        let remove_empty = keys.is_empty();
        if remove_empty {
            self.lease_keys.remove(&lease_id);
        }
    }
}

/// Compares two snapshots by their durable key/value/lease state, ignoring the
/// top-level revision, per-entry revision metadata (`create_revision`,
/// `modify_revision`, `version`), lease grant wall-clock timestamps, and the
/// watch-event tail. Lease `remaining_ticks` is intentionally durable: once lease
/// ticking starts, skipping that field would let a restart resurrect expired time.
/// Two snapshots that are durable-equal materialize to the same observable KV
/// state, so there is no need to rewrite the persisted file.
pub fn kv_snapshot_durable_eq(left: &KvSnapshot, right: &KvSnapshot) -> bool {
    if left.entries.len() != right.entries.len() || left.leases.len() != right.leases.len() {
        return false;
    }
    let left_entries = left
        .entries
        .iter()
        .map(|entry| (&entry.key, &entry.value, &entry.lease_id));
    let right_entries = right
        .entries
        .iter()
        .map(|entry| (&entry.key, &entry.value, &entry.lease_id));
    if !left_entries.eq(right_entries) {
        return false;
    }
    let left_leases = left
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks));
    let right_leases = right
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks));
    left_leases.eq(right_leases)
}

pub fn compact_kv_snapshot_event_history(snapshot: &mut KvSnapshot, max_events: usize) {
    compact_snapshot_event_history(
        &mut snapshot.events,
        &mut snapshot.compacted_revision,
        max_events,
    );
}

fn compact_snapshot_event_history(
    events: &mut Vec<KvEvent>,
    compacted_revision: &mut KvRevision,
    max_events: usize,
) {
    let excess = events.len().saturating_sub(max_events);
    if excess == 0 {
        return;
    }
    let compacted = events[excess - 1].revision;
    events.drain(..excess);
    *compacted_revision = (*compacted_revision).max(compacted);
}

fn compact_shared_event_history(
    events: &mut Vec<Arc<KvEvent>>,
    compacted_revision: &mut KvRevision,
    max_events: usize,
) {
    let excess = events.len().saturating_sub(max_events);
    if excess == 0 {
        return;
    }
    let compacted = events[excess - 1].revision;
    events.drain(..excess);
    *compacted_revision = (*compacted_revision).max(compacted);
}

fn build_lease_key_index(
    entries: &BTreeMap<String, KvEntry>,
) -> BTreeMap<LeaseId, BTreeSet<String>> {
    let mut lease_keys = BTreeMap::<LeaseId, BTreeSet<String>>::new();
    for (key, entry) in entries {
        if let Some(lease_id) = entry.lease_id {
            lease_keys.entry(lease_id).or_default().insert(key.clone());
        }
    }
    lease_keys
}

fn build_lease_deadline_index(
    leases: &BTreeMap<LeaseId, KvLease>,
    lease_clock: u64,
) -> (BTreeMap<LeaseId, u64>, BTreeMap<u64, BTreeSet<LeaseId>>) {
    let mut deadline_by_id = BTreeMap::new();
    let mut deadlines = BTreeMap::<u64, BTreeSet<LeaseId>>::new();
    for lease in leases.values() {
        let deadline = lease_clock.saturating_add(lease.remaining_ticks);
        deadline_by_id.insert(lease.lease_id, deadline);
        deadlines
            .entry(deadline)
            .or_default()
            .insert(lease.lease_id);
    }
    (deadline_by_id, deadlines)
}

#[derive(Clone, Debug)]
struct KvWatchState {
    watch: KvWatch,
    pending: VecDeque<Arc<KvEvent>>,
    compacted_revision: Option<KvRevision>,
}

fn compact_watch_pending(watch: &mut KvWatchState) {
    let excess = watch
        .pending
        .len()
        .saturating_sub(MAX_PENDING_EVENTS_PER_WATCH);
    if excess == 0 {
        return;
    }
    let compacted = watch
        .pending
        .get(excess - 1)
        .map(|event| event.revision)
        .unwrap_or(watch.watch.start_revision);
    watch.pending.drain(..excess);
    watch.compacted_revision = Some(
        watch
            .compacted_revision
            .unwrap_or(KvRevision::ZERO)
            .max(compacted),
    );
}

impl Default for KvRevision {
    fn default() -> Self {
        Self::ZERO
    }
}

impl KvOpResponse {
    fn revision(&self) -> KvRevision {
        match self {
            Self::Put { revision, .. } => *revision,
            Self::Delete { .. }
            | Self::DeletePrefix { .. }
            | Self::LeaseGrant { .. }
            | Self::LeaseKeepAlive { .. }
            | Self::LeaseRevoke { .. } => KvRevision::ZERO,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InProcessRaftKvCluster {
    term: u64,
    leader_id: NodeId,
    voters: BTreeSet<NodeId>,
    online: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, ReplicatedKvNode>,
    next_log_index: u64,
    commit_index: u64,
}

impl InProcessRaftKvCluster {
    pub fn new(voter_ids: Vec<NodeId>) -> Result<Self, KvError> {
        let Some(leader_id) = voter_ids.first().cloned() else {
            return Err(KvError::NoVoters);
        };
        let voters: BTreeSet<NodeId> = voter_ids.into_iter().collect();
        let online = voters.clone();
        let nodes = voters
            .iter()
            .cloned()
            .map(|node_id| {
                (
                    node_id.clone(),
                    ReplicatedKvNode {
                        store: InProcessKvStore::new(),
                        log: Vec::new(),
                        hard_state: RaftHardState::default(),
                        applied_index: 0,
                        snapshot_index: 0,
                        snapshot_term: 0,
                        txn_responses: BTreeMap::new(),
                    },
                )
            })
            .collect();
        Ok(Self {
            term: 1,
            leader_id,
            voters,
            online,
            nodes,
            next_log_index: 1,
            commit_index: 0,
        })
    }

    pub fn leader_id(&self) -> &NodeId {
        &self.leader_id
    }

    pub fn store(&self, node_id: &NodeId) -> Option<&InProcessKvStore> {
        self.nodes.get(node_id).map(|node| &node.store)
    }

    pub fn set_node_online(&mut self, node_id: &NodeId, online: bool) {
        if online {
            self.online.insert(node_id.clone());
            let _ = self.catch_up_node(node_id);
        } else {
            self.online.remove(node_id);
        }
    }

    pub fn apply_as_leader(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        self.propose_command(command)
            .map(|outcome| outcome.revision)
    }

    pub fn txn_response(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let outcome = self.propose_command(KvCommand::Txn(txn))?;
        Ok(outcome.txn_response.unwrap_or_else(|| KvTxnResponse {
            succeeded: true,
            revision: outcome.revision,
            responses: Vec::new(),
        }))
    }

    fn propose_command(&mut self, command: KvCommand) -> Result<RaftCommitOutcome, KvError> {
        if !self.online.contains(&self.leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: self.leader_id.clone(),
            });
        }
        let online_voters = self.voters.intersection(&self.online).count();
        if online_voters < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters,
                quorum: self.quorum_size(),
            });
        }

        let index = self.next_log_index;
        self.next_log_index += 1;
        let entry = KvLogEntry {
            index,
            term: self.term,
            command,
            revision: KvRevision::ZERO,
        };
        let leader_id = self.leader_id.clone();
        self.node_mut(&leader_id)?
            .append_entries(vec![entry.clone()])?;
        let mut voter_acks = usize::from(self.voters.contains(&leader_id));

        for node_id in self.online.iter().cloned().collect::<Vec<_>>() {
            if node_id == leader_id {
                continue;
            }
            let ack = self.append_entries_to_follower(&node_id, vec![entry.clone()])?;
            if ack.success && self.voters.contains(&node_id) {
                voter_acks += 1;
            }
        }

        if voter_acks < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters: voter_acks,
                quorum: self.quorum_size(),
            });
        }

        self.commit_index = index;
        let online_nodes = self.online.iter().cloned().collect::<Vec<_>>();
        for node_id in online_nodes {
            self.node_mut(&node_id)?.commit_to(index)?;
        }
        let leader = self.node(&leader_id)?;
        let committed = leader
            .log
            .iter()
            .find(|entry| entry.index == index)
            .ok_or(KvError::MissingCommittedEntry { index })?;
        let txn_response = leader.txn_responses.get(&index).cloned();
        Ok(RaftCommitOutcome {
            revision: committed.revision,
            txn_response,
        })
    }

    fn append_entries_to_follower(
        &mut self,
        node_id: &NodeId,
        entries: Vec<KvLogEntry>,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        let prev_log_index = entries
            .first()
            .map(|entry| entry.index.saturating_sub(1))
            .unwrap_or(self.commit_index);
        let prev_log_term = self
            .node(&self.leader_id)
            .ok()
            .and_then(|node| node.term_at(prev_log_index))
            .unwrap_or(0);
        let request = RaftAppendEntriesRequest {
            term: self.term,
            leader_id: self.leader_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.node_mut(node_id)?
            .handle_append_entries(request)
            .map(|outcome| outcome.response)
    }

    pub fn catch_up_node(&mut self, node_id: &NodeId) -> Result<(), KvError> {
        if !self.nodes.contains_key(node_id) {
            return Err(KvError::UnknownNode {
                node_id: node_id.clone(),
            });
        }
        if node_id == &self.leader_id {
            return Ok(());
        }
        let leader = self.node(&self.leader_id)?.clone();
        let follower = self.node_mut(node_id)?;
        if follower.last_log_index() < leader.snapshot_index {
            follower.install_snapshot(RaftSnapshot {
                last_included_index: leader.applied_index,
                last_included_term: leader
                    .term_at(leader.applied_index)
                    .unwrap_or(leader.term()),
                kv: leader.store.snapshot(),
            });
        }
        let missing = leader
            .log
            .iter()
            .filter(|entry| entry.index > follower.last_log_index())
            .cloned()
            .collect::<Vec<_>>();
        follower.append_entries(missing)?;
        follower.commit_to(leader.hard_state.commit_index)
    }

    pub fn request_vote(
        &mut self,
        voter_id: &NodeId,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.node_mut(voter_id)?.handle_request_vote(request)
    }

    pub fn log(&self, node_id: &NodeId) -> Option<&[KvLogEntry]> {
        self.nodes.get(node_id).map(|node| node.log.as_slice())
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn hard_state(&self, node_id: &NodeId) -> Option<&RaftHardState> {
        self.nodes.get(node_id).map(|node| &node.hard_state)
    }

    pub fn watch_prefix(
        &mut self,
        node_id: &NodeId,
        prefix: impl Into<String>,
        start_revision: KvRevision,
    ) -> Result<WatchId, KvError> {
        self.node_mut(node_id)?
            .store
            .watch_prefix(prefix, start_revision)
    }

    pub fn drain_watch(
        &mut self,
        node_id: &NodeId,
        watch_id: WatchId,
    ) -> Result<Vec<KvEvent>, KvError> {
        self.node_mut(node_id)?.store.drain_watch(watch_id)
    }

    pub fn cancel_watch(&mut self, node_id: &NodeId, watch_id: WatchId) -> Result<bool, KvError> {
        self.node_mut(node_id)?.store.cancel_watch(watch_id)
    }

    pub fn transfer_leader(&mut self, new_leader_id: NodeId) -> Result<(), KvError> {
        if !self.voters.contains(&new_leader_id) {
            return Err(KvError::UnknownNode {
                node_id: new_leader_id,
            });
        }
        if !self.online.contains(&new_leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: new_leader_id,
            });
        }
        self.catch_up_node(&new_leader_id)?;
        self.term = self.term.saturating_add(1);
        self.leader_id = new_leader_id.clone();
        let term = self.term;
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = term;
            node.hard_state.voted_for = Some(new_leader_id.clone());
            node.hard_state.leader_id = Some(new_leader_id.clone());
        }
        Ok(())
    }

    pub fn elect_leader(&mut self, candidate_id: NodeId) -> Result<bool, KvError> {
        if !self.voters.contains(&candidate_id) {
            return Err(KvError::UnknownNode {
                node_id: candidate_id,
            });
        }
        if !self.online.contains(&candidate_id) {
            return Err(KvError::LeaderOffline {
                leader_id: candidate_id,
            });
        }
        self.catch_up_node(&candidate_id)?;
        let candidate = self.node(&candidate_id)?.clone();
        let election_term = self.term.saturating_add(1);
        let request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: candidate_id.clone(),
            last_log_index: candidate.last_log_index(),
            last_log_term: candidate.last_log_term(),
        };
        let mut granted = 0;
        for voter_id in self
            .voters
            .intersection(&self.online)
            .cloned()
            .collect::<Vec<_>>()
        {
            let response = self.request_vote(&voter_id, request.clone())?;
            if response.term > self.term {
                self.term = response.term;
            }
            if response.vote_granted {
                granted += 1;
            }
        }
        if granted < self.quorum_size() {
            return Ok(false);
        }
        self.term = election_term.max(self.term);
        self.leader_id = candidate_id.clone();
        for node in self.nodes.values_mut() {
            if node.hard_state.current_term <= self.term {
                node.hard_state.current_term = self.term;
                node.hard_state.leader_id = Some(candidate_id.clone());
            }
        }
        Ok(true)
    }

    fn node(&self, node_id: &NodeId) -> Result<&ReplicatedKvNode, KvError> {
        self.nodes.get(node_id).ok_or_else(|| KvError::UnknownNode {
            node_id: node_id.clone(),
        })
    }

    fn node_mut(&mut self, node_id: &NodeId) -> Result<&mut ReplicatedKvNode, KvError> {
        self.nodes
            .get_mut(node_id)
            .ok_or_else(|| KvError::UnknownNode {
                node_id: node_id.clone(),
            })
    }

    pub fn put(&mut self, key: impl Into<String>, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.apply_as_leader(KvCommand::Op(KvOp::Put {
            key: key.into(),
            value,
            lease_id: None,
        }))
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvRevision, KvError> {
        self.txn_response(txn).map(|response| response.revision)
    }

    pub fn install_snapshot(
        &mut self,
        target_node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        let node = self
            .nodes
            .get_mut(&target_node_id)
            .ok_or(KvError::UnknownNode {
                node_id: target_node_id,
            })?;
        node.install_snapshot(RaftSnapshot {
            last_included_index: self.commit_index,
            last_included_term: self.term,
            kv: snapshot,
        });
        Ok(())
    }

    pub fn add_learner_from_snapshot(
        &mut self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        if self.nodes.contains_key(&node_id) {
            return Err(KvError::DuplicateNode { node_id });
        }
        self.nodes.insert(
            node_id.clone(),
            ReplicatedKvNode {
                store: InProcessKvStore::restore(snapshot),
                log: Vec::new(),
                hard_state: RaftHardState {
                    current_term: self.term,
                    voted_for: None,
                    leader_id: Some(self.leader_id.clone()),
                    commit_index: self.commit_index,
                },
                applied_index: self.commit_index,
                snapshot_index: self.commit_index,
                snapshot_term: self.term,
                txn_responses: BTreeMap::new(),
            },
        );
        self.online.insert(node_id);
        Ok(())
    }

    fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

enum CommitEntries {
    Single(KvLogEntry),
    Batch(Vec<KvLogEntry>),
}

impl CommitEntries {
    fn as_slice(&self) -> &[KvLogEntry] {
        match self {
            CommitEntries::Single(entry) => std::slice::from_ref(entry),
            CommitEntries::Batch(entries) => entries,
        }
    }
}

#[derive(Clone, Debug)]
struct ReplicatedKvNode {
    store: InProcessKvStore,
    log: Vec<KvLogEntry>,
    hard_state: RaftHardState,
    applied_index: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    txn_responses: BTreeMap<u64, KvTxnResponse>,
}

impl ReplicatedKvNode {
    fn term(&self) -> u64 {
        self.hard_state.current_term
    }

    fn last_log_index(&self) -> u64 {
        self.log
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index)
    }

    fn last_log_term(&self) -> u64 {
        self.log
            .last()
            .map(|entry| entry.term)
            .unwrap_or(self.snapshot_term)
    }

    fn log_position(&self, index: u64) -> Option<usize> {
        self.log
            .binary_search_by_key(&index, |entry| entry.index)
            .ok()
    }

    fn log_entry_at(&self, index: u64) -> Option<&KvLogEntry> {
        self.log_position(index)
            .and_then(|position| self.log.get(position))
    }

    fn log_entry_at_mut(&mut self, index: u64) -> Option<&mut KvLogEntry> {
        let position = self.log_position(index)?;
        self.log.get_mut(position)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.log_entry_at(index).map(|entry| entry.term)
    }

    fn read_only_append_entries_response(
        &self,
        request: &RaftAppendEntriesRequest,
    ) -> Option<RaftAppendEntriesResponse> {
        if request.term < self.hard_state.current_term {
            return Some(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }

        if request.term != self.hard_state.current_term
            || self.hard_state.leader_id.as_ref() != Some(&request.leader_id)
            || !request.entries.is_empty()
        {
            return None;
        }

        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return Some(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }

        if request.leader_commit > self.hard_state.commit_index {
            return None;
        }

        Some(RaftAppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            match_index: self.last_log_index(),
        })
    }

    /// Loads persisted WAL entries on open. Entries beyond the installed snapshot
    /// are appended (and later applied) as normal; already-applied entries are kept
    /// in the log as a contiguous replication tail (not re-applied — they are
    /// reflected in the snapshot/store). Any valid-but-non-contiguous record marks
    /// the remaining WAL tail corrupt so open can rewrite the safe prefix instead
    /// of bricking recovery on a `LogGap`.
    fn load_persisted_entries(&mut self, entries: Vec<KvLogEntry>) -> bool {
        let mut truncated = false;
        for entry in entries {
            if entry.index == 0 {
                truncated = true;
                break;
            }
            if entry.index > self.applied_index {
                if let Err(error) = self.append_entries(vec![entry]) {
                    eprintln!(
                        "warning: truncating cluster kv WAL tail at invalid persisted record: {error}"
                    );
                    truncated = true;
                    break;
                }
                continue;
            }
            if self
                .log
                .iter()
                .any(|existing| existing.index == entry.index)
            {
                continue;
            }
            let contiguous = match self.log.last() {
                Some(last) => entry.index == last.index.saturating_add(1),
                None => true,
            };
            if contiguous {
                self.log.push(entry);
            } else {
                truncated = true;
                break;
            }
        }
        self.log.sort_by_key(|entry| entry.index);
        truncated
    }

    fn append_entries(&mut self, entries: Vec<KvLogEntry>) -> Result<Vec<KvLogEntry>, KvError> {
        let mut appended = Vec::new();
        for entry in entries {
            validate_kv_command_payload(&entry.command)?;
            if entry.index <= self.applied_index {
                if let Some(existing) = self.log_entry_at(entry.index)
                    && (existing.term != entry.term || existing.command != entry.command)
                {
                    return Err(KvError::ConflictingLogEntry { index: entry.index });
                }
                continue;
            }
            match self
                .log
                .binary_search_by_key(&entry.index, |candidate| candidate.index)
            {
                Ok(existing_position) if self.log[existing_position].term != entry.term => {
                    self.log.truncate(existing_position);
                    self.log.push(entry.clone());
                    appended.push(entry);
                }
                Ok(existing_position) if self.log[existing_position].command != entry.command => {
                    return Err(KvError::ConflictingLogEntry { index: entry.index });
                }
                Ok(_) => {}
                Err(_) => {
                    let expected = self.last_log_index().saturating_add(1);
                    if entry.index != expected {
                        return Err(KvError::LogGap {
                            expected,
                            actual: entry.index,
                        });
                    }
                    self.log.push(entry.clone());
                    appended.push(entry);
                }
            }
        }
        Ok(appended)
    }

    fn discard_uncommitted_tail(&mut self) -> bool {
        let committed = self.hard_state.commit_index.max(self.applied_index);
        let old_log_len = self.log.len();
        let old_response_len = self.txn_responses.len();
        self.log.retain(|entry| entry.index <= committed);
        self.txn_responses.retain(|index, _| *index <= committed);
        old_log_len != self.log.len() || old_response_len != self.txn_responses.len()
    }

    fn prepare_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesPreparation, KvError> {
        if request.term < self.hard_state.current_term {
            return Ok(RaftAppendEntriesPreparation {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
                commit_index: None,
            });
        }
        if request.term > self.hard_state.current_term {
            self.hard_state.voted_for = None;
        }
        self.hard_state.current_term = request.term;
        self.hard_state.leader_id = Some(request.leader_id.clone());
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return Ok(RaftAppendEntriesPreparation {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
                commit_index: None,
            });
        }
        let appended_entries = self.append_entries(request.entries)?;
        let commit_index = request.leader_commit.min(self.last_log_index());
        Ok(RaftAppendEntriesPreparation {
            response: RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: true,
                match_index: self.last_log_index(),
            },
            appended_entries,
            commit_index: Some(commit_index),
        })
    }

    fn handle_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesOutcome, KvError> {
        let preparation = self.prepare_append_entries(request)?;
        if let Some(commit_index) = preparation.commit_index {
            self.commit_to(commit_index)?;
        }
        Ok(RaftAppendEntriesOutcome {
            response: preparation.response,
        })
    }

    fn handle_request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        if request.term < self.hard_state.current_term {
            return Ok(RaftRequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            });
        }
        if request.term > self.hard_state.current_term {
            self.hard_state.current_term = request.term;
            self.hard_state.voted_for = None;
            self.hard_state.leader_id = None;
        }
        let candidate_log_is_current = (request.last_log_term, request.last_log_index)
            >= (self.last_log_term(), self.last_log_index());
        let can_vote = self
            .hard_state
            .voted_for
            .as_ref()
            .is_none_or(|voted_for| voted_for == &request.candidate_id);
        let vote_granted = can_vote && candidate_log_is_current;
        if vote_granted {
            self.hard_state.voted_for = Some(request.candidate_id);
            self.hard_state.leader_id = None;
        }
        Ok(RaftRequestVoteResponse {
            term: self.hard_state.current_term,
            vote_granted,
        })
    }

    fn observe_term(&mut self, term: u64) -> bool {
        if term <= self.hard_state.current_term {
            return false;
        }
        self.hard_state.current_term = term;
        self.hard_state.voted_for = None;
        self.hard_state.leader_id = None;
        true
    }

    fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        if commit_index <= self.applied_index {
            self.hard_state.commit_index = self.hard_state.commit_index.max(commit_index);
            return Ok(());
        }
        let entries = self.entries_to_commit(commit_index)?;
        self.preflight_commit_entries(entries.as_slice())?;
        self.apply_preflighted_commit(entries, commit_index)
    }

    fn entries_to_commit(&self, commit_index: u64) -> Result<CommitEntries, KvError> {
        let last_log_index = self.last_log_index();
        if commit_index > last_log_index {
            return Err(KvError::MissingCommittedEntry {
                index: last_log_index.saturating_add(1),
            });
        }
        let first_unapplied_index = self.applied_index.saturating_add(1);
        if commit_index == first_unapplied_index {
            let entry =
                self.log_entry_at(commit_index)
                    .cloned()
                    .ok_or(KvError::MissingCommittedEntry {
                        index: commit_index,
                    })?;
            return Ok(CommitEntries::Single(entry));
        }
        let start = self
            .log
            .partition_point(|entry| entry.index <= self.applied_index);
        let end = self
            .log
            .partition_point(|entry| entry.index <= commit_index);
        let entries = self.log[start..end].to_vec();
        let mut expected_index = self.applied_index.saturating_add(1);
        for entry in &entries {
            if entry.index != expected_index {
                return Err(KvError::MissingCommittedEntry {
                    index: expected_index,
                });
            }
            expected_index = expected_index.saturating_add(1);
        }
        if expected_index != commit_index.saturating_add(1) {
            return Err(KvError::MissingCommittedEntry {
                index: expected_index,
            });
        }
        Ok(CommitEntries::Batch(entries))
    }

    fn preflight_commit_entries(&self, entries: &[KvLogEntry]) -> Result<(), KvError> {
        match entries {
            [] => Ok(()),
            [entry] => self.store.preflight_apply_command(&entry.command),
            _ => {
                let mut store = self.store.clone();
                for entry in entries {
                    store.apply_command_with_response(entry.command.clone())?;
                }
                Ok(())
            }
        }
    }

    fn apply_preflighted_commit(
        &mut self,
        entries: CommitEntries,
        commit_index: u64,
    ) -> Result<(), KvError> {
        match entries {
            CommitEntries::Single(entry) => self.apply_preflighted_commit_entry(entry)?,
            CommitEntries::Batch(entries) => {
                for entry in entries {
                    self.apply_preflighted_commit_entry(entry)?;
                }
            }
        }
        self.hard_state.commit_index = commit_index;
        Ok(())
    }

    fn apply_preflighted_commit_entry(&mut self, entry: KvLogEntry) -> Result<(), KvError> {
        let (revision, txn_response) = self.store.apply_command_with_response(entry.command)?;
        if let Some(log_entry) = self.log_entry_at_mut(entry.index) {
            log_entry.revision = revision;
        }
        if let Some(response) = txn_response {
            self.txn_responses.insert(entry.index, response);
        }
        self.applied_index = entry.index;
        Ok(())
    }

    /// Compacts the in-memory log around the already-applied state, keeping the
    /// most recent `retain` committed entries so slightly-behind followers can be
    /// replicated incrementally rather than via a full snapshot install. The store,
    /// applied_index and commit_index are unchanged (this is local compaction, not
    /// installing a received snapshot).
    fn local_compact(&mut self, retain: u64) {
        let snapshot_index = self.applied_index;
        self.snapshot_term = self.term_at(snapshot_index).unwrap_or(self.snapshot_term);
        self.snapshot_index = snapshot_index;
        let cutoff = snapshot_index.saturating_sub(retain);
        self.log.retain(|entry| entry.index > cutoff);
        self.txn_responses.retain(|index, _| *index > cutoff);
    }

    fn install_snapshot(&mut self, snapshot: RaftSnapshot) {
        self.store = InProcessKvStore::restore(snapshot.kv);
        self.snapshot_index = snapshot.last_included_index;
        self.snapshot_term = snapshot.last_included_term;
        self.applied_index = snapshot.last_included_index;
        self.hard_state.commit_index = snapshot.last_included_index;
        self.log
            .retain(|entry| entry.index > snapshot.last_included_index);
        self.txn_responses.clear();
    }
}

#[derive(Clone, Debug)]
struct RaftCommitOutcome {
    revision: KvRevision,
    txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Debug)]
struct RaftAppendEntriesPreparation {
    response: RaftAppendEntriesResponse,
    appended_entries: Vec<KvLogEntry>,
    commit_index: Option<u64>,
}

#[derive(Clone, Debug)]
struct RaftAppendEntriesOutcome {
    response: RaftAppendEntriesResponse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot: KvSnapshot,
    pub snapshot_restored: bool,
    pub committed_events: Vec<KvEvent>,
}

#[derive(Debug)]
pub struct DurableRaftKvNode {
    node_id: NodeId,
    paths: KvWalPaths,
    _lock: File,
    node: ReplicatedKvNode,
}

impl DurableRaftKvNode {
    pub fn open(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    fn open_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        paths.ensure()?;
        let lock = open_node_lock(&paths)?;
        lock_node_exclusive(&lock, &paths.lock, lock_wait)?;
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
                txn_responses: BTreeMap::new(),
            };
            node.install_snapshot(snapshot);
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
                txn_responses: BTreeMap::new(),
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
        Ok(Self {
            node_id,
            paths,
            _lock: lock,
            node,
        })
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

    fn try_open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false, Duration::ZERO)
    }

    fn open_with_materialized_cache_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut fallback_snapshot = None;
        let mut node = match Self::open_inner(
            node_id.clone(),
            paths.clone(),
            discard_uncommitted_tail,
            lock_wait,
        ) {
            Ok(node) => node,
            Err(error) if !kv_error_is_busy(&error) => {
                let Some(snapshot) = cache.read_snapshot_arc_if_present()? else {
                    return Err(error);
                };
                fallback_snapshot = Some(snapshot);
                quarantine_unreadable_raft_view(&paths);
                Self::open_inner(node_id, paths, discard_uncommitted_tail, lock_wait)?
            }
            Err(error) => return Err(error),
        };
        if node.applied_index() == 0 && node.last_log_index() == 0 {
            let snapshot = match fallback_snapshot {
                Some(snapshot) => Some(snapshot),
                None => cache.read_snapshot_arc_if_present()?,
            };
            let Some(snapshot) = snapshot else {
                return Ok(node);
            };
            node.restore_snapshot(RaftSnapshot {
                last_included_index: 0,
                last_included_term: 0,
                kv: (*snapshot).clone(),
            })?;
        }
        Ok(node)
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

    pub fn hard_state(&self) -> &RaftHardState {
        &self.node.hard_state
    }

    pub fn store(&self) -> &InProcessKvStore {
        &self.node.store
    }

    pub fn log(&self) -> &[KvLogEntry] {
        &self.node.log
    }

    pub fn last_log_index(&self) -> u64 {
        self.node.last_log_index()
    }

    pub fn last_log_term(&self) -> u64 {
        self.node.last_log_term()
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.node.term_at(index)
    }

    pub fn applied_index(&self) -> u64 {
        self.node.applied_index
    }

    pub fn snapshot_index(&self) -> u64 {
        self.node.snapshot_index
    }

    pub fn txn_response_at(&self, index: u64) -> Option<&KvTxnResponse> {
        self.node.txn_responses.get(&index)
    }

    pub fn append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        if let Some(response) = self.node.read_only_append_entries_response(&request) {
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
            self.node.apply_preflighted_commit(entries, commit_index)?;
        }
        Ok(preparation.response)
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let before_revision = self.store().revision();
        let mut snapshot_restored = false;
        if let Some(snapshot) = snapshot {
            let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
                && (append.prev_log_index != snapshot.last_included_index
                    || append.prev_log_term == snapshot.last_included_term);
            let snapshot_is_newer = snapshot.last_included_index > self.applied_index();
            let snapshot_repairs_divergent_compaction = snapshot.last_included_term
                > self.last_log_term()
                && snapshot.kv.revision >= self.store().revision();
            let append_term_is_current = append.term >= self.hard_state().current_term;
            if append_term_is_current
                && snapshot_matches_append
                && (snapshot_is_newer || snapshot_repairs_divergent_compaction)
            {
                self.restore_snapshot(snapshot)?;
                snapshot_restored = true;
            }
        }
        let response = self.append_entries(append)?;
        let snapshot = self.store().snapshot();
        let committed_events = if snapshot_restored || snapshot.revision == before_revision {
            Vec::new()
        } else {
            snapshot
                .events
                .iter()
                .filter(|event| event.revision > before_revision)
                .cloned()
                .collect()
        };
        Ok(RaftAppendEntriesApplyOutcome {
            response,
            snapshot,
            snapshot_restored,
            committed_events,
        })
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
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
        Ok(response)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<bool, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        if !self.node.observe_term(term) {
            return Ok(false);
        }
        if let Err(error) = self.persist_hard_state() {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        Ok(true)
    }

    pub fn become_leader(&mut self, term: u64) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.current_term = term.max(self.node.hard_state.current_term);
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        Ok(())
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        Ok(())
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            command,
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
        Ok(entry)
    }

    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        if !self.node.discard_uncommitted_tail() {
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
        Ok(true)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
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
            return Ok(());
        }

        let entries = self.node.entries_to_commit(commit_index)?;
        self.node.preflight_commit_entries(entries.as_slice())?;
        self.node.hard_state.commit_index = commit_index;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.node.apply_preflighted_commit(entries, commit_index)
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: self.node.store.snapshot(),
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
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot);
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        Ok(())
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        self.paths.write_persisted_hard_state(&self.node.hard_state)
    }

    fn persist_hard_state_if_changed(&self, before: &RaftHardState) -> Result<(), KvError> {
        if self.node.hard_state == *before {
            return Ok(());
        }
        self.persist_hard_state()
    }

    fn restore_after_append_entries_persist_failure(
        &mut self,
        node_before: Option<ReplicatedKvNode>,
        hard_state_before: RaftHardState,
        log_before: Vec<KvLogEntry>,
        txn_responses_before: BTreeMap<u64, KvTxnResponse>,
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

#[derive(Clone, Debug, Default)]
struct WalRead {
    entries: Vec<KvLogEntry>,
    truncated: bool,
}

fn read_wal_entries(
    path: &Path,
    max_index: Option<u64>,
    retain_after_index: u64,
) -> Result<WalRead, KvError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalRead::default());
        }
        Err(source) => {
            return Err(KvError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    file.lock_shared().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut entries = Vec::new();
    let mut truncated = false;
    let mut record_number = 0;
    loop {
        let mut len_buf = [0_u8; 4];
        match read_wal_chunk(&mut file, &mut len_buf, path)? {
            WalReadChunk::Complete => {}
            WalReadChunk::CleanEof => break,
            WalReadChunk::Truncated => {
                truncated = true;
                break;
            }
        }
        record_number += 1;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_WAL_RECORD_BYTES {
            truncated = true;
            break;
        }
        let mut record = vec![0_u8; len];
        if read_wal_chunk(&mut file, &mut record, path)? != WalReadChunk::Complete {
            truncated = true;
            break;
        }
        match sinomo_object_serde::from_bytes(&record) {
            Ok(entry) => {
                let entry: KvLogEntry = entry;
                if entry.index <= retain_after_index {
                    truncated = true;
                    continue;
                }
                if max_index.is_some_and(|limit| entry.index > limit) {
                    truncated = true;
                    break;
                }
                entries.push(entry);
            }
            Err(source) => {
                eprintln!(
                    "warning: truncating cluster kv WAL at unreadable binary record {} in {}: {source}",
                    record_number,
                    path.display()
                );
                truncated = true;
                break;
            }
        }
    }
    Ok(WalRead { entries, truncated })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalReadChunk {
    Complete,
    CleanEof,
    Truncated,
}

fn read_wal_chunk(file: &mut File, buf: &mut [u8], path: &Path) -> Result<WalReadChunk, KvError> {
    let mut offset = 0;
    while offset < buf.len() {
        match file.read(&mut buf[offset..]) {
            Ok(0) if offset == 0 => return Ok(WalReadChunk::CleanEof),
            Ok(0) => return Ok(WalReadChunk::Truncated),
            Ok(read) => offset += read,
            Err(source) => {
                return Err(KvError::Io {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(WalReadChunk::Complete)
}

fn append_wal_entries(path: &Path, entries: &[KvLogEntry]) -> Result<(), KvError> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut file = open_wal_file_for_append(path)?;
    file.lock_exclusive().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        write_wal_record(&mut file, entry, path)?;
    }
    file.sync_data().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn open_wal_file_for_append(path: &Path) -> Result<File, KvError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| KvError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    match OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(path)
    {
        Ok(file) => {
            sync_directory(parent)?;
            Ok(file)
        }
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|source| KvError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            }),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn encode_wal_record(entry: &KvLogEntry, path: &Path) -> Result<Vec<u8>, KvError> {
    let record = sinomo_object_serde::to_bytes(entry, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| KvError::Serialization {
            detail: source.to_string(),
        })?;
    let record_len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
        detail: format!("wal record for {} exceeds u32 length", path.display()),
    })?;
    let mut encoded = Vec::with_capacity(record.len().saturating_add(4));
    encoded.extend_from_slice(&record_len.to_be_bytes());
    encoded.extend_from_slice(&record);
    Ok(encoded)
}

fn write_wal_record(file: &mut File, entry: &KvLogEntry, path: &Path) -> Result<(), KvError> {
    let record = encode_wal_record(entry, path)?;
    file.write_all(&record).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn wal_file_matches_entries(path: &Path, entries: &[KvLogEntry]) -> Result<bool, KvError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(entries.is_empty());
        }
        Err(_) => return Ok(false),
    };
    if file.lock_shared().is_err() {
        return Ok(false);
    }

    for entry in entries {
        let expected = encode_wal_record(entry, path)?;
        let mut existing = vec![0_u8; expected.len()];
        if file.read_exact(&mut existing).is_err() || existing != expected {
            return Ok(false);
        }
    }

    let mut extra = [0_u8; 1];
    Ok(matches!(file.read(&mut extra), Ok(0)))
}

fn rewrite_wal_entries(path: &Path, entries: &[KvLogEntry]) -> Result<(), KvError> {
    // Callers hold the per-node raft lock. Stage through a temp file instead of
    // truncating the live WAL so a failed rewrite leaves the previous log intact.
    if wal_file_matches_entries(path, entries)? {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| KvError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("wal.sobjlog");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_FILE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let write_temp_result = (|| -> Result<(), KvError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|source| KvError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        for entry in entries {
            write_wal_record(&mut file, entry, &temp_path)?;
        }
        file.sync_data().map_err(|source| KvError::Io {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(())
    })();
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    let result = replace_file(&temp_path, path).and_then(|()| sync_directory(parent));
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn read_optional_object<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, KvError> {
    let Some(bytes) = read_optional_object_file_bytes(path)? else {
        return Ok(None);
    };
    decode_object_file(path, &bytes).map(Some)
}

fn read_object_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, KvError> {
    let bytes = read_object_file_bytes(path)?;
    decode_object_file(path, &bytes)
}

#[cfg(test)]
static REQUIRED_OBJECT_FILE_READS_FOR_TEST: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();

#[cfg(test)]
fn required_object_file_reads_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    REQUIRED_OBJECT_FILE_READS_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn record_required_object_file_read_for_test(path: &Path) {
    if let Ok(mut reads) = required_object_file_reads_for_test().lock() {
        *reads.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
fn reset_required_object_file_reads_for_test(path: &Path) {
    if let Ok(mut reads) = required_object_file_reads_for_test().lock() {
        reads.remove(path);
    }
}

#[cfg(test)]
fn required_object_file_read_count_for_test(path: &Path) -> u64 {
    required_object_file_reads_for_test()
        .lock()
        .ok()
        .and_then(|reads| reads.get(path).copied())
        .unwrap_or_default()
}

fn decode_object_file<T: for<'de> Deserialize<'de>>(
    path: &Path,
    bytes: &[u8],
) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(&bytes).map_err(|source| KvError::Serialization {
        detail: format!("{}: {source}", path.display()),
    })
}

fn read_optional_object_file_bytes(path: &Path) -> Result<Option<Vec<u8>>, KvError> {
    durable_file::read_optional_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(kv_durable_file_error)
}

fn read_object_file_bytes(path: &Path) -> Result<Vec<u8>, KvError> {
    #[cfg(test)]
    record_required_object_file_read_for_test(path);
    durable_file::read_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(kv_durable_file_error)
}

fn write_object_file<T: Serialize>(path: &Path, value: &T) -> Result<(), KvError> {
    write_object_file_if_changed(path, value).map(|_| ())
}

fn write_object_file_if_changed<T: Serialize>(path: &Path, value: &T) -> Result<bool, KvError> {
    let content = sinomo_object_serde::to_bytes(value, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })?;
    write_object_file_bytes_if_changed(path, &content)
}

fn write_object_file_bytes_if_changed(path: &Path, content: &[u8]) -> Result<bool, KvError> {
    durable_file::write_object_file_bytes_if_changed(
        path,
        content,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("json", "tmp"),
    )
    .map(|result| result.changed)
    .map_err(kv_durable_file_error)
}

fn write_object_file_bytes_create_new_serialized(
    path: &Path,
    content: &[u8],
) -> Result<bool, KvError> {
    durable_file::write_object_file_bytes_create_new_serialized(
        path,
        content,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "create-new-tmp"),
        |temp_path, path| fs::hard_link(temp_path, path),
    )
    .map(|result| result.changed)
    .map_err(kv_durable_file_error)
}

fn replace_file(temp_path: &Path, path: &Path) -> Result<(), KvError> {
    durable_file::replace_file(temp_path, path).map_err(kv_durable_file_error)
}

fn kv_durable_file_error(error: durable_file::DurableFileError) -> KvError {
    KvError::Io {
        path: error.path().to_string(),
        detail: error.detail().to_string(),
    }
}

fn open_node_lock(paths: &KvWalPaths) -> Result<File, KvError> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&paths.lock)
        .map_err(|source| KvError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })
}

fn lock_node_exclusive(lock: &File, path: &Path, wait: Duration) -> Result<(), KvError> {
    let deadline = Instant::now() + wait;
    let mut attempts = 0_u32;
    loop {
        match lock.try_lock_exclusive() {
            Ok(()) => return Ok(()),
            Err(source) if file_lock_is_busy(&source) => {
                let now = Instant::now();
                if wait.is_zero() || now >= deadline {
                    return Err(KvError::Busy {
                        path: path.display().to_string(),
                    });
                }
                let delay = raft_node_lock_retry_delay(attempts)
                    .min(deadline.saturating_duration_since(now));
                thread::sleep(delay);
                attempts = attempts.saturating_add(1);
            }
            Err(source) => {
                return Err(KvError::Io {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
}

fn raft_node_lock_retry_delay(attempts: u32) -> Duration {
    let shift = attempts.min(5);
    RAFT_NODE_LOCK_RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(RAFT_NODE_LOCK_RETRY_MAX)
}

fn file_lock_is_busy(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(32 | 33))
}

fn kv_error_is_busy(error: &KvError) -> bool {
    matches!(error, KvError::Busy { .. })
}

fn quarantine_unreadable_raft_view(paths: &KvWalPaths) {
    if !paths.root.exists() {
        return;
    }
    for suffix in 0..10 {
        let candidate = paths.root.with_file_name(format!(
            "{}.corrupt-{}-{suffix}",
            paths
                .root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("raft"),
            std::process::id()
        ));
        if candidate.exists() {
            continue;
        }
        if let Err(error) = fs::rename(&paths.root, &candidate) {
            eprintln!(
                "warning: could not quarantine unreadable local kv raft view {} -> {}: {}",
                paths.root.display(),
                candidate.display(),
                error
            );
        }
        return;
    }
}

fn sync_directory(path: &Path) -> Result<(), KvError> {
    durable_file::sync_directory(path).map_err(kv_durable_file_error)
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KvError {
    #[error("requested revision {requested:?} has been compacted at {compacted:?}")]
    Compacted {
        requested: KvRevision,
        compacted: KvRevision,
    },
    #[error("requested revision {requested:?} is ahead of current revision {current:?}")]
    RevisionAhead {
        requested: KvRevision,
        current: KvRevision,
    },
    #[error("unknown lease: {lease_id:?}")]
    UnknownLease { lease_id: LeaseId },
    #[error("invalid lease ttl: {ttl_ticks}")]
    InvalidLeaseTtl { ttl_ticks: u64 },
    #[error("too many active leases: max {max}")]
    LeaseLimit { max: usize },
    #[error(
        "transaction is too large: {compares} compares, {success_ops} success ops, {failure_ops} failure ops; max is {max_compares} compares and {max_ops_per_branch} ops per branch"
    )]
    TxnLimit {
        compares: usize,
        success_ops: usize,
        failure_ops: usize,
        max_compares: usize,
        max_ops_per_branch: usize,
    },
    #[error("unknown watch: {watch_id:?}")]
    UnknownWatch { watch_id: WatchId },
    #[error("too many active watches: max {max}")]
    WatchLimit { max: usize },
    #[error("watch id space exhausted")]
    WatchIdExhausted,
    #[error("range for prefix {prefix} exceeded max {max_entries} entries")]
    RangeLimit { prefix: String, max_entries: usize },
    #[error(
        "range for prefix {prefix} is at least {observed_bytes} payload bytes; max is {max_bytes}"
    )]
    RangePayloadLimit {
        prefix: String,
        observed_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "delete-prefix for prefix {prefix} matched at least {observed_keys} keys; max is {max_keys}"
    )]
    DeletePrefixLimit {
        prefix: String,
        observed_keys: usize,
        max_keys: usize,
    },
    #[error(
        "kv command would emit at least {observed_events} mutation events; max is {max_events}"
    )]
    MutationEventLimit {
        observed_events: usize,
        max_events: usize,
    },
    #[error("lease {lease_id:?} would have {observed_keys} attached keys; max is {max_keys}")]
    LeaseKeyLimit {
        lease_id: LeaseId,
        observed_keys: usize,
        max_keys: usize,
    },
    #[error("kv value is {actual_bytes} bytes; max is {max_bytes}")]
    ValueTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("kv command payload is {actual_bytes} bytes; max is {max_bytes}")]
    CommandPayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("serialization error: {detail}")]
    Serialization { detail: String },
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("raft node is busy at {path}")]
    Busy { path: String },
    #[error("raft cluster requires at least one voter")]
    NoVoters,
    #[error("unknown replicated node: {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("duplicate replicated node: {node_id}")]
    DuplicateNode { node_id: NodeId },
    #[error("leader is offline: {leader_id}")]
    LeaderOffline { leader_id: NodeId },
    #[error("quorum unavailable: {online_voters} voters online, {quorum} required")]
    QuorumUnavailable { online_voters: usize, quorum: usize },
    #[error("raft log gap: expected index {expected}, got {actual}")]
    LogGap { expected: u64, actual: u64 },
    #[error("conflicting raft log entry at index {index}")]
    ConflictingLogEntry { index: u64 },
    #[error("missing committed raft entry at index {index}")]
    MissingCommittedEntry { index: u64 },
    #[error("invalid raft config transition: {detail}")]
    InvalidRaftConfigTransition { detail: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    static OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX: std::sync::Mutex<()> =
        std::sync::Mutex::new(());
    static KV_READ_CACHE_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct TestRecord {
        name: String,
        count: u64,
    }

    #[test]
    fn cluster_kv_rpc_routes_group_storage_routes() {
        assert_eq!(CLUSTER_KV_RPC_CATEGORY, "cluster.kv");
        assert!(CLUSTER_KV_RPC_ROUTES.contains(&CLUSTER_KV_SNAPSHOT_ROUTE));
        assert!(CLUSTER_KV_RPC_ROUTES.contains(&CLUSTER_KV_TXN_ROUTE));
        for route in CLUSTER_KV_LOCAL_READ_WATCH_ROUTES
            .iter()
            .copied()
            .chain([CLUSTER_KV_WATCH_EVENTS_ROUTE])
        {
            assert!(CLUSTER_KV_RPC_ROUTES.contains(&route));
        }
        assert_eq!(
            CLUSTER_KV_RPC_ROUTE_METADATA.len(),
            CLUSTER_KV_RPC_ROUTES.len()
        );
        for (index, route) in CLUSTER_KV_RPC_ROUTES.iter().copied().enumerate() {
            assert_eq!(
                CLUSTER_KV_RPC_ROUTE_METADATA[index],
                (route, CLUSTER_KV_RPC_CATEGORY)
            );
        }
        assert!(!CLUSTER_KV_LOCAL_READ_WATCH_ROUTES.contains(&CLUSTER_KV_TXN_ROUTE));
    }

    #[test]
    fn cluster_kv_raft_config_helpers_report_roles_and_voters() {
        let voter = NodeId::from_legacy_named("node_config_voter").expect("voter");
        let learner = NodeId::from_legacy_named("node_config_learner").expect("learner");
        let non_voting = NodeId::from_legacy_named("node_config_non_voting").expect("non-voting");
        let missing = NodeId::from_legacy_named("node_config_missing").expect("missing");
        let config = ClusterKvRaftConfigRecord {
            cluster_id: Id::from_legacy_named("cluster_config").expect("cluster"),
            term: 3,
            kv_revision: 9,
            voters: vec![voter.clone()],
            learners: vec![learner.clone()],
            non_voting: vec![non_voting.clone()],
            updated_at: OffsetDateTime::now_utc(),
        };

        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &voter),
            Some(ClusterMemberRole::Voter)
        );
        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &learner),
            Some(ClusterMemberRole::Learner)
        );
        assert_eq!(
            cluster_kv_raft_config_member_role(&config, &non_voting),
            Some(ClusterMemberRole::NonVoting)
        );
        assert_eq!(cluster_kv_raft_config_member_role(&config, &missing), None);

        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_kv_raft_config_key(), &config, None)
            .expect("store config");
        assert!(cluster_kv_store_member_is_raft_voter(&store, &voter).expect("voter check"));
        assert!(!cluster_kv_store_member_is_raft_voter(&store, &learner).expect("learner check"));
        assert!(!cluster_kv_store_member_is_raft_voter(&store, &missing).expect("missing check"));
    }

    #[test]
    fn cluster_kv_rpc_payloads_encode_storage_owned_bodies() {
        let snapshot_payload = cluster_kv_snapshot_rpc_payload().expect("snapshot rpc payload");
        assert_eq!(snapshot_payload.route, CLUSTER_KV_SNAPSHOT_ROUTE);
        assert_eq!(snapshot_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_snapshot_rpc_request_body(&snapshot_payload.body)
                .expect("snapshot request body"),
            ()
        );

        let txn = KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key: "/rpc/payload".to_string(),
                value: b"value".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        };
        let txn_payload = cluster_kv_txn_rpc_payload(&txn).expect("txn rpc payload");
        assert_eq!(txn_payload.route, CLUSTER_KV_TXN_ROUTE);
        assert_eq!(txn_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_txn_rpc_request_body(&txn_payload.body).expect("txn request body"),
            txn
        );

        let snapshot = KvSnapshot {
            revision: KvRevision(7),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
        };
        let snapshot_response_payload =
            cluster_kv_snapshot_response_rpc_payload(&snapshot).expect("snapshot response payload");
        assert_eq!(snapshot_response_payload.route, CLUSTER_KV_SNAPSHOT_ROUTE);
        assert_eq!(snapshot_response_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_snapshot_rpc_body(&snapshot_response_payload.body)
                .expect("snapshot response body"),
            snapshot
        );

        let txn_response = KvTxnResponse {
            succeeded: true,
            revision: KvRevision(9),
            responses: Vec::new(),
        };
        let txn_response_payload =
            cluster_kv_txn_response_rpc_payload(&txn_response).expect("txn response payload");
        assert_eq!(txn_response_payload.route, CLUSTER_KV_TXN_ROUTE);
        assert_eq!(txn_response_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_txn_rpc_body(&txn_response_payload.body).expect("txn response body"),
            txn_response
        );

        let range_payload =
            cluster_kv_range_prefix_rpc_payload("/rpc/").expect("range prefix payload");
        assert_eq!(range_payload.route, CLUSTER_KV_RANGE_PREFIX_ROUTE);
        assert_eq!(range_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_kv_rpc_body::<String>(&range_payload.body).expect("range prefix body"),
            "/rpc/"
        );
    }

    #[test]
    fn cluster_raft_rpc_payloads_encode_kv_owned_bodies() {
        let node_id = NodeId::from_legacy_named("node_kv_raft_rpc_payloads").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            sinomo_core::Id::from_legacy_named("cluster_kv_raft_rpc_payloads").expect("cluster"),
            "kv raft rpc payloads",
            node_id.clone(),
            "node",
            "pk",
            OffsetDateTime::now_utc(),
        );
        let append_request = ClusterRaftAppendRequest {
            state: state.clone(),
            append: RaftAppendEntriesRequest {
                term: 2,
                leader_id: node_id.clone(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: Vec::new(),
                leader_commit: 1,
            },
            snapshot: None,
        };
        let append_payload =
            cluster_raft_append_rpc_payload(&append_request).expect("append payload");
        assert_eq!(append_payload.route, CLUSTER_RAFT_APPEND_ROUTE);
        assert_eq!(append_payload.category, CLUSTER_RAFT_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_raft_append_rpc_request_body(&append_payload.body)
                .expect("append request body"),
            append_request
        );
        let append_response = ClusterRaftAppendResponse {
            accepted: true,
            detail: "accepted".to_string(),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 2,
                success: true,
                match_index: 1,
            },
        };
        let append_response_payload = cluster_raft_append_response_rpc_payload(&append_response)
            .expect("append response payload");
        assert_eq!(
            decode_cluster_raft_append_rpc_response_body(&append_response_payload.body)
                .expect("append response body"),
            append_response
        );

        let vote_request = ClusterRaftVoteRequest {
            state: state.clone(),
            vote: RaftRequestVoteRequest {
                term: 3,
                candidate_id: node_id,
                last_log_index: 1,
                last_log_term: 2,
            },
        };
        let vote_payload = cluster_raft_vote_rpc_payload(&vote_request).expect("vote payload");
        assert_eq!(vote_payload.route, CLUSTER_RAFT_VOTE_ROUTE);
        assert_eq!(vote_payload.category, CLUSTER_RAFT_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_raft_vote_rpc_request_body(&vote_payload.body)
                .expect("vote request body"),
            vote_request
        );
        let vote_response = ClusterRaftVoteResponse {
            accepted: true,
            detail: "granted".to_string(),
            state,
            vote: RaftRequestVoteResponse {
                term: 3,
                vote_granted: true,
            },
        };
        let vote_response_payload =
            cluster_raft_vote_response_rpc_payload(&vote_response).expect("vote response payload");
        assert_eq!(
            decode_cluster_raft_vote_rpc_response_body(&vote_response_payload.body)
                .expect("vote response body"),
            vote_response
        );
        assert_eq!(
            CLUSTER_RAFT_RPC_ROUTE_METADATA,
            [
                (CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
                (CLUSTER_RAFT_VOTE_ROUTE, CLUSTER_RAFT_RPC_CATEGORY)
            ]
        );

        let dispatched_append = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            append_payload.route,
            &append_payload.body,
            |request| {
                assert_eq!(request, append_request);
                Ok(append_response.clone())
            },
            |_| -> Result<ClusterRaftVoteResponse, KvError> {
                panic!("append route must not call vote handler")
            },
        )
        .expect("dispatch append")
        .expect("append route");
        assert_eq!(dispatched_append.route, CLUSTER_RAFT_APPEND_ROUTE);
        assert_eq!(
            decode_cluster_raft_append_rpc_response_body(&dispatched_append.body)
                .expect("dispatched append response"),
            append_response
        );

        let dispatched_vote = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            vote_payload.route,
            &vote_payload.body,
            |_| -> Result<ClusterRaftAppendResponse, KvError> {
                panic!("vote route must not call append handler")
            },
            |request| {
                assert_eq!(request, vote_request);
                Ok(vote_response.clone())
            },
        )
        .expect("dispatch vote")
        .expect("vote route");
        assert_eq!(dispatched_vote.route, CLUSTER_RAFT_VOTE_ROUTE);
        assert_eq!(
            decode_cluster_raft_vote_rpc_response_body(&dispatched_vote.body)
                .expect("dispatched vote response"),
            vote_response
        );

        let unknown = handle_cluster_raft_rpc_payload::<_, _, KvError>(
            "/cluster/raft/unknown",
            &[],
            |_| -> Result<ClusterRaftAppendResponse, KvError> {
                panic!("unknown route must not call append handler")
            },
            |_| -> Result<ClusterRaftVoteResponse, KvError> {
                panic!("unknown route must not call vote handler")
            },
        )
        .expect("unknown route");
        assert!(unknown.is_none());
    }

    #[derive(Debug, Default)]
    struct FixedLocalReadWatchHost {
        seen_range_prefix: Option<String>,
        created_watch: Option<ClusterKvWatchPrefixRequest>,
        drained_watch: Option<ClusterKvWatchDrainRequest>,
        cancelled_watch: Option<ClusterKvWatchCancelRequest>,
    }

    impl ClusterKvLocalReadWatchHost for FixedLocalReadWatchHost {
        type Error = KvError;

        fn range_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, Self::Error> {
            self.seen_range_prefix = Some(prefix.to_string());
            Ok(vec![KvEntry {
                key: format!("{prefix}/value"),
                value: b"value".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
                lease_id: None,
            }])
        }

        fn create_watch(
            &mut self,
            request: ClusterKvWatchPrefixRequest,
        ) -> Result<ClusterKvWatchRegistration, Self::Error> {
            self.created_watch = Some(request.clone());
            Ok(ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: request.prefix,
                next_revision: request.start_revision,
            })
        }

        fn drain_watch(
            &mut self,
            request: ClusterKvWatchDrainRequest,
        ) -> Result<ClusterKvWatchDrainResponse, Self::Error> {
            self.drained_watch = Some(request.clone());
            Ok(ClusterKvWatchDrainResponse {
                watch_id: request.watch_id,
                events: Vec::new(),
                next_revision: KvRevision(11),
            })
        }

        fn cancel_watch(
            &mut self,
            request: ClusterKvWatchCancelRequest,
        ) -> Result<ClusterKvWatchCancelResponse, Self::Error> {
            self.cancelled_watch = Some(request.clone());
            Ok(ClusterKvWatchCancelResponse {
                watch_id: request.watch_id,
                cancelled: true,
            })
        }
    }

    #[test]
    fn cluster_kv_local_read_watch_rpc_dispatch_is_storage_owned() {
        let mut host = FixedLocalReadWatchHost::default();

        let range_body =
            encode_cluster_kv_rpc_body(&"/svc".to_string()).expect("range request body");
        let range_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_RANGE_PREFIX_ROUTE,
            &range_body,
            &mut host,
        )
        .expect("range dispatch")
        .expect("range route");
        assert_eq!(range_payload.route, CLUSTER_KV_RANGE_PREFIX_ROUTE);
        assert_eq!(range_payload.category, CLUSTER_KV_RPC_CATEGORY);
        assert_eq!(host.seen_range_prefix.as_deref(), Some("/svc"));
        assert_eq!(
            decode_cluster_kv_rpc_body::<Vec<KvEntry>>(&range_payload.body)
                .expect("range response body")
                .first()
                .map(|entry| entry.key.as_str()),
            Some("/svc/value")
        );

        let watch_request = ClusterKvWatchPrefixRequest {
            prefix: "/svc".to_string(),
            start_revision: KvRevision(3),
        };
        let watch_body = encode_cluster_kv_rpc_body(&watch_request).expect("watch request body");
        let watch_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_PREFIX_ROUTE,
            &watch_body,
            &mut host,
        )
        .expect("watch dispatch")
        .expect("watch route");
        assert_eq!(watch_payload.route, CLUSTER_KV_WATCH_PREFIX_ROUTE);
        assert_eq!(host.created_watch, Some(watch_request));
        assert_eq!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchRegistration>(&watch_payload.body)
                .expect("watch response body")
                .watch_id,
            WatchId(7)
        );

        let drain_request = ClusterKvWatchDrainRequest {
            watch_id: WatchId(7),
            wait_timeout_millis: 5,
        };
        let drain_body = encode_cluster_kv_rpc_body(&drain_request).expect("drain request body");
        let drain_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_DRAIN_ROUTE,
            &drain_body,
            &mut host,
        )
        .expect("drain dispatch")
        .expect("drain route");
        assert_eq!(drain_payload.route, CLUSTER_KV_WATCH_DRAIN_ROUTE);
        assert_eq!(host.drained_watch, Some(drain_request));
        assert_eq!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchDrainResponse>(&drain_payload.body)
                .expect("drain response body")
                .next_revision,
            KvRevision(11)
        );

        let cancel_request = ClusterKvWatchCancelRequest {
            watch_id: WatchId(7),
        };
        let cancel_body = encode_cluster_kv_rpc_body(&cancel_request).expect("cancel request body");
        let cancel_payload = handle_cluster_kv_local_read_watch_rpc_payload(
            CLUSTER_KV_WATCH_CANCEL_ROUTE,
            &cancel_body,
            &mut host,
        )
        .expect("cancel dispatch")
        .expect("cancel route");
        assert_eq!(cancel_payload.route, CLUSTER_KV_WATCH_CANCEL_ROUTE);
        assert_eq!(host.cancelled_watch, Some(cancel_request));
        assert!(
            decode_cluster_kv_rpc_body::<ClusterKvWatchCancelResponse>(&cancel_payload.body)
                .expect("cancel response body")
                .cancelled
        );

        assert!(
            handle_cluster_kv_local_read_watch_rpc_payload(
                CLUSTER_KV_WATCH_EVENTS_ROUTE,
                &[],
                &mut host,
            )
            .expect("watch events stays external")
            .is_none()
        );
    }

    #[test]
    fn cluster_kv_paths_define_cluster_storage_layout() {
        let root = PathBuf::from("/sinomo-cluster");
        let paths = ClusterKvPaths::new(&root);

        assert_eq!(paths.snapshot, root.join("kv-snapshot.sobj"));
        assert_eq!(paths.raft, root.join("kv-raft"));
        assert_eq!(paths.watches, root.join("watches"));
        assert_eq!(paths.watches_lock, root.join("watches.lock"));
        assert_eq!(paths.watches_next_id, root.join("watches-next.sobj"));
        assert_eq!(paths.watch_events, root.join("watch-events.sobj"));
        assert_eq!(paths.watch_events_log, root.join("watch-events.sobjlog"));
        let node_id = NodeId::from_legacy_named("node_path_layout").expect("node");
        assert_eq!(
            paths.raft_wal_paths(&node_id).root,
            root.join("kv-raft").join(node_id.as_str())
        );
        assert_eq!(
            paths.watch_path(WatchId(7)),
            root.join("watches/watch-7.sobj")
        );
    }

    #[test]
    fn cluster_kv_paths_prefix_read_uses_materialized_snapshot_limits() {
        let root = temp_raft_root("sinomo-kv-cluster-prefix-read");
        let paths = ClusterKvPaths::new(root.clone());
        let cache = paths.local_cache();
        let mut store = InProcessKvStore::new();
        for (key, value) in [
            ("/apps/a", b"one".to_vec()),
            ("/apps/b", b"two".to_vec()),
            ("/other/c", b"three".to_vec()),
        ] {
            store.put(key, value, None).expect("put");
        }
        cache
            .write_snapshot(&store.snapshot())
            .expect("write materialized snapshot");

        let entries = read_cluster_kv_prefix_entries(&paths, "/apps/").expect("read prefix");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/a", "/apps/b"]
        );

        let error = read_cluster_kv_prefix_entries_limited(&paths, "/apps/", 1)
            .expect_err("prefix entry limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/apps/".to_string(),
                max_entries: 1,
            }
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_paths_read_missing_materialized_snapshot_as_empty() {
        let root = temp_raft_root("sinomo-kv-cluster-missing-snapshot-read");
        let paths = ClusterKvPaths::new(root.clone());

        assert_eq!(
            read_cluster_kv_entry(&paths, "/missing").expect("read missing entry"),
            None
        );
        assert_eq!(
            read_cluster_kv_prefix_entries(&paths, "/missing/").expect("read missing prefix"),
            Vec::new()
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_drain_events_filter_sort_and_dedupe() {
        let duplicate = test_kv_put_event(5, "/svc/b", b"b");
        let persisted_events = vec![
            test_kv_put_event(3, "/svc/c", b"c"),
            test_kv_put_event(2, "/svc/old", b"old"),
            test_kv_put_event(4, "/other/a", b"other"),
            duplicate.clone(),
        ];
        let snapshot = test_kv_snapshot_with_events(vec![
            test_kv_put_event(4, "/svc/a", b"a"),
            duplicate,
            test_kv_delete_event(6, "/svc/d"),
        ]);
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(9),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };

        let events = collect_cluster_kv_watch_drain_events(
            &persisted_events,
            &snapshot,
            &registration,
            usize::MAX,
        );

        assert_eq!(
            events
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(3, "/svc/c"), (4, "/svc/a"), (5, "/svc/b"), (6, "/svc/d")]
        );
    }

    #[test]
    fn cluster_kv_watch_drain_payload_limit_keeps_revision_groups() {
        let events = vec![
            test_kv_put_event(7, "/svc/a", b"a"),
            test_kv_put_event(7, "/svc/b", b"larger-than-limit"),
            test_kv_put_event(8, "/svc/c", b"c"),
        ];
        let snapshot = test_kv_snapshot_with_events(Vec::new());
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(10),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(7),
        };
        let max_payload_bytes = cluster_kv_watch_event_payload_bytes(&events[0]);

        let drained = collect_cluster_kv_watch_drain_events(
            &events,
            &snapshot,
            &registration,
            max_payload_bytes,
        );

        assert_eq!(
            drained
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(7, "/svc/a"), (7, "/svc/b")]
        );
    }

    #[test]
    fn cluster_kv_watch_registration_cursor_persist_policy_uses_storage_thresholds() {
        assert!(!cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG + 1),
            KvRevision(1),
            true,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            false,
            Some(Duration::from_secs(1)),
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            None,
        ));
        assert!(cluster_kv_watch_registration_cursor_should_persist(
            1,
            KvRevision(2),
            KvRevision(1),
            true,
            Some(CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE),
        ));
    }

    #[test]
    fn cluster_kv_materialized_snapshot_deferral_is_key_predicate_driven() {
        let events = vec![
            test_kv_put_event(1, "/deferred/a", b"a"),
            test_kv_delete_event(2, "/deferred/b"),
        ];

        assert!(cluster_kv_events_can_defer_materialized_snapshot(
            &events,
            |key| key.starts_with("/deferred/"),
        ));

        let mixed_events = vec![
            test_kv_put_event(1, "/deferred/a", b"a"),
            test_kv_put_event(2, "/durable/a", b"a"),
        ];
        assert!(!cluster_kv_events_can_defer_materialized_snapshot(
            &mixed_events,
            |key| key.starts_with("/deferred/"),
        ));
        assert!(!cluster_kv_events_can_defer_materialized_snapshot(
            &[],
            |key| key.starts_with("/deferred/"),
        ));
    }

    #[test]
    fn cluster_kv_snapshot_delta_defers_only_predicate_matched_changes() {
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let snapshot = |revision: u64, entries: Vec<KvEntry>| KvSnapshot {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: Vec::new(),
            events: Vec::new(),
        };
        let key_can_defer = |key: &str| key.starts_with("/deferred/");

        let existing = snapshot(
            1,
            vec![entry("/deferred/a", b"old"), entry("/durable/a", b"same")],
        );
        let deferable_change = snapshot(
            2,
            vec![entry("/deferred/a", b"new"), entry("/durable/a", b"same")],
        );
        let durable_change = snapshot(
            3,
            vec![entry("/deferred/a", b"old"), entry("/durable/a", b"new")],
        );

        assert!(cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &deferable_change,
            key_can_defer,
        ));
        assert!(!cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &durable_change,
            key_can_defer,
        ));
        assert!(!cluster_kv_snapshot_delta_only_deferable(
            &existing,
            &existing,
            key_can_defer,
        ));
    }

    #[test]
    fn imported_cluster_kv_snapshot_defers_only_predicate_matched_materialized_delta() {
        let root = temp_raft_root("sinomo-kv-imported-snapshot-deferral");
        let paths = ClusterKvPaths::new(&root);
        let key_can_defer = |key: &str| key.starts_with("/deferred/");

        let mut store = InProcessKvStore::new();
        store
            .put("/durable/a", b"same".to_vec(), None)
            .expect("write durable seed");
        store
            .put("/deferred/a", b"old".to_vec(), None)
            .expect("write deferred seed");
        write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("write initial snapshot");

        let mut imported_store = InProcessKvStore::restore(store.snapshot());
        imported_store
            .put("/deferred/a", b"new".to_vec(), None)
            .expect("write deferred import");
        let wrote = write_imported_cluster_kv_snapshot_if_changed(
            &paths,
            &imported_store.snapshot(),
            key_can_defer,
        )
        .expect("write imported deferred snapshot");
        assert!(!wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read cached snapshot");
        assert_eq!(
            cached
                .get("/deferred/a")
                .expect("deferred entry")
                .value
                .as_slice(),
            b"old"
        );

        let mut durable_store = InProcessKvStore::restore(cached);
        durable_store
            .put("/durable/a", b"new".to_vec(), None)
            .expect("write durable import");
        let wrote = write_imported_cluster_kv_snapshot_if_changed(
            &paths,
            &durable_store.snapshot(),
            key_can_defer,
        )
        .expect("write imported durable snapshot");
        assert!(wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read updated snapshot");
        assert_eq!(
            cached
                .get("/durable/a")
                .expect("durable entry")
                .value
                .as_slice(),
            b"new"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_publish_helpers_materialize_snapshot_in_kv() {
        let root = temp_raft_root("sinomo-kv-raft-publish-commit");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let mut kv = InProcessKvStore::new();
        kv.put("/kv/a", b"a".to_vec(), None)
            .expect("write commit entry");
        let snapshot = kv.snapshot();
        let result = ClusterKvRaftCommitResult {
            snapshot: snapshot.clone(),
            txn_response: None,
            events: snapshot.events.clone(),
        };

        let wrote = store
            .publish_commit_result(&result, |_| false)
            .expect("publish commit result");
        assert!(wrote);
        let wrote = store
            .publish_commit_result(&result, |_| false)
            .expect("publish unchanged commit result");
        assert!(!wrote);
        let cached = read_cluster_kv_cached_snapshot(&paths).expect("read published snapshot");
        assert_eq!(
            cached
                .get("/kv/a")
                .expect("published entry")
                .value
                .as_slice(),
            b"a"
        );

        let restored_root = temp_raft_root("sinomo-kv-raft-publish-restored");
        let restored_paths = ClusterKvPaths::new(&restored_root);
        let restored_store = restored_paths.raft_store();
        let outcome = RaftAppendEntriesApplyOutcome {
            response: RaftAppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 1,
            },
            snapshot: snapshot.clone(),
            snapshot_restored: true,
            committed_events: Vec::new(),
        };
        let wrote = restored_store
            .publish_append_entries_apply_outcome(&outcome, |_| true)
            .expect("publish restored append outcome");
        assert!(wrote);
        let cached =
            read_cluster_kv_cached_snapshot(&restored_paths).expect("read restored snapshot");
        assert_eq!(
            cached
                .get("/kv/a")
                .expect("restored entry")
                .value
                .as_slice(),
            b"a"
        );

        std::fs::remove_dir_all(root).ok();
        std::fs::remove_dir_all(restored_root).ok();
    }

    #[test]
    fn cluster_kv_materialized_snapshot_freshness_uses_storage_thresholds() {
        assert!(cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(12),
            KvRevision(11),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(1),
            KvRevision(1 + CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG),
            Some(Duration::from_secs(1)),
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            None,
        ));
        assert!(!cluster_kv_materialized_snapshot_is_fresh_enough_against(
            KvRevision(10),
            KvRevision(11),
            Some(CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL),
        ));
    }

    #[test]
    fn cluster_kv_event_containment_and_snapshot_compare_are_storage_semantics() {
        let retained = vec![
            test_kv_put_event(1, "/kv/a", b"a"),
            test_kv_delete_event(2, "/kv/b"),
        ];
        assert!(cluster_kv_events_contain_all(
            &retained,
            &[test_kv_put_event(1, "/kv/a", b"a")],
        ));
        assert!(!cluster_kv_events_contain_all(
            &retained,
            &[test_kv_put_event(1, "/kv/a", b"changed")],
        ));

        let cluster_id = Id::from_legacy_named("cluster_kv_watch_fanout_needed").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_needed_leader").expect("leader");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout needed",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let first = test_kv_put_event(1, "/kv/watch/fanout/first", b"first");
        let second = test_kv_put_event(2, "/kv/watch/fanout/second", b"second");
        let append = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 1,
        };
        let mut request = ClusterRaftAppendRequest {
            state,
            append,
            snapshot: None,
        };
        assert!(!cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            std::slice::from_ref(&first),
        ));
        let mut fanout_snapshot = InProcessKvStore::new().snapshot();
        fanout_snapshot.events = vec![first.clone()];
        request.snapshot = Some(RaftSnapshot {
            last_included_index: 1,
            last_included_term: 1,
            kv: fanout_snapshot,
        });
        assert!(!cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            std::slice::from_ref(&first),
        ));
        assert!(cluster_raft_append_request_needs_watch_event_fanout(
            &request,
            &[first, second],
        ));

        let snapshot = KvSnapshot {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            entries: vec![KvEntry {
                key: "/kv/a".to_string(),
                value: b"a".to_vec(),
                create_revision: KvRevision(1),
                modify_revision: KvRevision(3),
                version: KvVersion(2),
                lease_id: Some(LeaseId(7)),
            }],
            leases: Vec::new(),
            events: Vec::new(),
        };
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::Exists {
                key: "/kv/a".to_string(),
            },
        ));
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::ValueEquals {
                key: "/kv/a".to_string(),
                value: b"a".to_vec(),
            },
        ));
        assert!(cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::LeaseEquals {
                key: "/kv/a".to_string(),
                lease_id: Some(LeaseId(7)),
            },
        ));
        assert!(!cluster_kv_snapshot_compare(
            &snapshot,
            &KvCompare::ModifyRevisionEquals {
                key: "/kv/a".to_string(),
                revision: KvRevision(2),
            },
        ));
    }

    #[test]
    fn cluster_kv_watch_event_payload_bytes_count_keys_and_put_values() {
        let put = test_kv_put_event(1, "/svc/value", b"payload");
        let delete = test_kv_delete_event(2, "/svc/deleted");

        assert_eq!(
            cluster_kv_watch_event_payload_bytes(&put),
            "/svc/value".len() + "payload".len()
        );
        assert_eq!(
            cluster_kv_watch_event_payload_bytes(&delete),
            "/svc/deleted".len()
        );
        assert_eq!(
            cluster_kv_watch_events_payload_bytes(&[put, delete]),
            "/svc/value".len() + "payload".len() + "/svc/deleted".len()
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batch_validation_enforces_storage_limits() {
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_put_event(2, "/svc/b", b"larger"),
            test_kv_delete_event(3, "/svc/c"),
        ];

        assert_eq!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(&events, 2, usize::MAX),
            Err(ClusterKvWatchEventFanoutBatchLimit::EventCount {
                observed_events: 3,
                max_events: 2,
            })
        );

        let payload_bytes = cluster_kv_watch_events_payload_bytes(&events);
        assert_eq!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(
                &events,
                events.len(),
                payload_bytes - 1,
            ),
            Err(ClusterKvWatchEventFanoutBatchLimit::PayloadBytes {
                observed_bytes: payload_bytes,
                max_bytes: payload_bytes - 1,
            })
        );

        assert!(
            validate_cluster_kv_watch_event_fanout_batch_with_limits(
                &events,
                events.len(),
                payload_bytes,
            )
            .is_ok()
        );
    }

    #[test]
    fn cluster_kv_watch_event_fanout_batches_are_storage_limited() {
        let events = (0..5)
            .map(|index| test_kv_put_event(index + 1, &format!("/svc/{index}"), b"payload"))
            .collect::<Vec<_>>();

        let count_limited =
            cluster_kv_watch_event_fanout_batches_with_limits(&events, 2, usize::MAX);
        assert_eq!(
            count_limited
                .iter()
                .map(|batch| batch.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let payload_limited =
            cluster_kv_watch_event_fanout_batches_with_limits(&events, events.len(), 20);
        assert_eq!(
            payload_limited
                .iter()
                .map(|batch| batch.len())
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 1, 1]
        );
        assert_eq!(
            payload_limited
                .iter()
                .flat_map(|batch| batch.iter().map(|event| event.revision))
                .collect::<Vec<_>>(),
            events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cluster_kv_watch_event_compaction_sorts_dedupes_and_bounds_retained_events() {
        let duplicate = test_kv_put_event(3, "/svc/b", b"b");
        let compacted = compact_cluster_kv_watch_events(vec![
            duplicate.clone(),
            test_kv_put_event(1, "/svc/a", b"a"),
            duplicate,
            test_kv_delete_event(2, "/svc/deleted"),
        ]);

        assert_eq!(
            compacted
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "/svc/a"), (2, "/svc/deleted"), (3, "/svc/b")]
        );

        let bounded = (0..(MAX_CLUSTER_KV_WATCH_EVENTS + 4))
            .map(|index| test_kv_put_event(index as u64 + 1, &format!("/svc/{index}"), b"value"))
            .collect::<Vec<_>>();
        let compacted = compact_cluster_kv_watch_events(bounded);
        assert_eq!(compacted.len(), MAX_CLUSTER_KV_WATCH_EVENTS);
        assert_eq!(
            compacted.first().expect("first retained").revision,
            KvRevision(5)
        );
    }

    #[test]
    fn cluster_kv_watch_event_compaction_keeps_cutoff_revision_group() {
        let mut events = vec![test_kv_put_event(1, "/svc/old", b"old")];
        for index in 0..5 {
            events.push(test_kv_put_event(
                2,
                &format!("/svc/group/{index}"),
                b"group",
            ));
        }
        for index in 0..(MAX_CLUSTER_KV_WATCH_EVENTS - 2) {
            events.push(test_kv_put_event(
                index as u64 + 3,
                &format!("/svc/new/{index}"),
                b"new",
            ));
        }

        let compacted = compact_cluster_kv_watch_events(events);
        assert_eq!(compacted.len(), MAX_CLUSTER_KV_WATCH_EVENTS + 3);
        assert_eq!(
            compacted.first().expect("first retained").revision,
            KvRevision(2)
        );
        assert!(
            compacted
                .iter()
                .take(5)
                .all(|event| event.revision == KvRevision(2))
        );
    }

    #[test]
    fn cluster_kv_watch_event_incremental_compaction_skips_duplicates() {
        let first = test_kv_put_event(1, "/svc/a", b"a");
        let second = test_kv_put_event(2, "/svc/b", b"b");

        let retained = compact_cluster_kv_watch_events_incremental(
            vec![first.clone()],
            &[first.clone(), second.clone()],
        );

        assert_eq!(retained, vec![first.clone(), second.clone()]);
        assert!(cluster_kv_events_contain(&retained, &first));
        assert!(cluster_kv_events_contain(&retained, &second));
    }

    #[test]
    fn cluster_kv_watch_event_append_checkpoint_policy_uses_storage_limits() {
        assert!(!cluster_kv_watch_event_append_should_checkpoint(
            1, 1, 0, 0, 1
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            MAX_CLUSTER_KV_WATCH_EVENTS + 1,
            1,
            0,
            0,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS - 1,
            0,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            0,
            MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES - 1,
            1,
        ));
        assert!(cluster_kv_watch_event_append_should_checkpoint(
            1,
            1,
            usize::MAX,
            u64::MAX,
            u64::MAX,
        ));
    }

    #[test]
    fn cluster_kv_watch_event_log_open_for_append_creates_and_reopens_log() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-open");
        let paths = ClusterKvPaths::new(root.clone());

        let first = open_cluster_kv_watch_event_log_for_append(&paths).expect("open new log");
        lock_cluster_kv_watch_event_log_for_append(&paths, &first).expect("lock new log");
        assert!(paths.watch_events_log.exists());
        first.unlock().expect("unlock new log");
        drop(first);

        let mut second =
            open_cluster_kv_watch_event_log_for_append(&paths).expect("reopen existing log");
        lock_cluster_kv_watch_event_log_for_append(&paths, &second).expect("lock reopened log");
        let batch =
            encode_cluster_kv_watch_event_log_batch(&[test_kv_put_event(1, "/svc/a", b"a")])
                .expect("encode log");
        append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut second, &batch)
            .expect("append reopened log");
        second.unlock().expect("unlock reopened log");
        drop(second);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert_eq!(read.records, 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_event_log_batch_encodes_records_and_bytes() {
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ];

        let batch = encode_cluster_kv_watch_event_log_batch(&events).expect("encode batch");

        assert_eq!(batch.records.len(), events.len());
        assert_eq!(
            batch.bytes,
            batch
                .records
                .iter()
                .map(|record| 4_u64 + record.len() as u64)
                .sum::<u64>()
        );
        let decoded = batch
            .records
            .iter()
            .map(|record| decode_cluster_kv_watch_event_log_record(record))
            .collect::<Result<Vec<_>, _>>()
            .expect("decode records");
        assert_eq!(decoded, events);
    }

    #[test]
    fn cluster_kv_watch_event_log_batch_appends_to_log_file() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-append");
        let paths = ClusterKvPaths::new(root.clone());
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&paths.watch_events_log)
            .expect("open log");
        let events = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ];
        let batch = encode_cluster_kv_watch_event_log_batch(&events).expect("encode log");

        assert_eq!(
            append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut log_file, &batch)
                .expect("append log"),
            batch.bytes
        );
        drop(log_file);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert_eq!(read.events, events);
        assert_eq!(read.records, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_event_log_truncates_log_file() {
        let root = temp_raft_root("sinomo-kv-watch-event-log-truncate");
        let paths = ClusterKvPaths::new(root.clone());
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&paths.watch_events_log)
            .expect("open log");
        let batch = encode_cluster_kv_watch_event_log_batch(&[
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_delete_event(2, "/svc/b"),
        ])
        .expect("encode log");
        append_cluster_kv_watch_event_log_batch_to_file(&paths, &mut log_file, &batch)
            .expect("append log");

        truncate_cluster_kv_watch_event_log_file(&paths, &mut log_file).expect("truncate log");
        drop(log_file);

        let read = read_cluster_kv_watch_event_log(&paths).expect("read log");
        assert!(read.events.is_empty());
        assert_eq!(read.records, 0);
        assert_eq!(
            std::fs::metadata(&paths.watch_events_log)
                .expect("log metadata")
                .len(),
            0
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_events_read_merges_checkpoint_and_log() {
        let root = temp_raft_root("sinomo-kv-watch-events-read");
        let paths = ClusterKvPaths::new(root.clone());
        let checkpoint = vec![
            test_kv_put_event(1, "/svc/a", b"a"),
            test_kv_put_event(2, "/svc/b", b"b"),
        ];
        write_cluster_kv_watch_event_checkpoint(&paths, &checkpoint).expect("checkpoint");

        let log_events = vec![
            test_kv_put_event(3, "/svc/c", b"c"),
            test_kv_put_event(4, "/svc/d", b"d"),
        ];
        let batch = encode_cluster_kv_watch_event_log_batch(&log_events).expect("encode log");
        let parent = paths.watch_events_log.parent().expect("log parent");
        std::fs::create_dir_all(parent).expect("create log parent");
        let mut log_file = std::fs::File::create(&paths.watch_events_log).expect("create log");
        for record in &batch.records {
            let len = u32::try_from(record.len()).expect("record length");
            log_file
                .write_all(&len.to_be_bytes())
                .and_then(|()| log_file.write_all(record))
                .expect("write log record");
        }
        log_file.sync_data().expect("sync log");

        let mut observed_read_bytes = 0_u64;
        let read = read_cluster_kv_watch_events_with_observer(&paths, |bytes| {
            observed_read_bytes = observed_read_bytes.saturating_add(bytes);
        })
        .expect("read watch events");

        assert_eq!(read.log_records, log_events.len());
        assert!(read.append_safe);
        assert_eq!(
            read.events
                .iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>(),
            vec![KvRevision(1), KvRevision(2), KvRevision(3), KvRevision(4)]
        );
        assert_eq!(observed_read_bytes, batch.bytes);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_events_append_to_storage_uses_cache_view() {
        let root = temp_raft_root("sinomo-kv-watch-events-append-storage");
        let paths = ClusterKvPaths::new(root.clone());
        let first = test_kv_put_event(1, "/svc/a", b"a");
        let second = test_kv_put_event(2, "/svc/b", b"b");
        let mut observed_read_bytes = 0_u64;
        let mut observed_write_bytes = 0_u64;

        let first_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            &[first.clone(), second.clone()],
            None,
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append first batch");

        assert_eq!(first_report.appended, vec![first.clone(), second.clone()]);
        assert_eq!(first_report.retained, vec![first.clone(), second.clone()]);
        assert_eq!(first_report.log_records, 2);
        assert!(first_report.append_safe);
        assert_eq!(observed_read_bytes, 0);
        assert!(observed_write_bytes > 0);

        let log_bytes = std::fs::metadata(&paths.watch_events_log)
            .expect("log metadata")
            .len();
        let cache = ClusterKvWatchEventAppendCacheView {
            events: first_report.retained.clone(),
            log_records: first_report.log_records,
            log_bytes,
            append_safe: first_report.append_safe,
        };
        observed_read_bytes = 0;
        observed_write_bytes = 0;
        let duplicate_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            std::slice::from_ref(&first),
            Some(&cache),
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append duplicate");

        assert!(duplicate_report.appended.is_empty());
        assert_eq!(duplicate_report.retained, cache.events);
        assert_eq!(duplicate_report.log_records, cache.log_records);
        assert_eq!(observed_read_bytes, 0);
        assert_eq!(observed_write_bytes, 0);

        let third = test_kv_put_event(3, "/svc/c", b"c");
        observed_read_bytes = 0;
        observed_write_bytes = 0;
        let second_cache = ClusterKvWatchEventAppendCacheView {
            events: duplicate_report.retained.clone(),
            log_records: duplicate_report.log_records,
            log_bytes,
            append_safe: duplicate_report.append_safe,
        };
        let second_report = append_cluster_kv_watch_events_to_storage_with_observers(
            &paths,
            std::slice::from_ref(&third),
            Some(&second_cache),
            |bytes| observed_read_bytes = observed_read_bytes.saturating_add(bytes),
            |bytes| observed_write_bytes = observed_write_bytes.saturating_add(bytes),
        )
        .expect("append second batch");

        assert_eq!(second_report.appended, vec![third.clone()]);
        assert_eq!(
            second_report.retained,
            vec![first.clone(), second.clone(), third.clone()]
        );
        assert_eq!(second_report.log_records, 3);
        assert_eq!(observed_read_bytes, 0);
        assert!(observed_write_bytes > 0);
        assert_eq!(
            read_cluster_kv_watch_events(&paths)
                .expect("read retained events")
                .events,
            second_report.retained
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_registration_file_validates_watch_id_and_removes() {
        let root = temp_raft_root("sinomo-kv-watch-registration-file");
        let paths = ClusterKvPaths::new(root.clone());
        let path = paths.watch_path(WatchId(7));
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };

        write_cluster_kv_watch_registration_file(&path, &registration).expect("write registration");
        assert_eq!(
            read_cluster_kv_watch_registration_file(&path, WatchId(7)).expect("read registration"),
            registration
        );
        assert!(
            !write_cluster_kv_watch_registration_file_create_new(&path, &registration)
                .expect("create existing registration"),
            "create-new registration writes must not clobber existing watch files"
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&path, WatchId(8)),
            Err(KvError::UnknownWatch {
                watch_id: WatchId(8),
            })
        );
        assert!(
            remove_cluster_kv_watch_registration_file_if_exists(&path)
                .expect("remove registration")
        );
        assert!(
            !remove_cluster_kv_watch_registration_file_if_exists(&path)
                .expect("remove absent registration")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_allocator_state_defaults_and_normalizes_next_id() {
        let root = temp_raft_root("sinomo-kv-watch-allocator-state");
        let paths = ClusterKvPaths::new(root.clone());

        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths).expect("default allocator"),
            ClusterKvWatchAllocatorState::legacy(1)
        );

        let persisted = ClusterKvWatchAllocatorState {
            next_watch_id: 0,
            active_watch_count: Some(3),
            pruned_compacted_revision: Some(KvRevision(11)),
        };
        write_cluster_kv_watch_allocator_state(&paths, &persisted).expect("write allocator");
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths).expect("read allocator"),
            ClusterKvWatchAllocatorState {
                next_watch_id: 1,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(11)),
            }
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_allocator_lock_creates_durable_lock_file() {
        let root = temp_raft_root("sinomo-kv-watch-allocator-lock");
        let paths = ClusterKvPaths::new(root.clone());

        let lock = lock_cluster_kv_watch_allocator(&paths).expect("lock allocator");

        assert!(paths.watches_lock.exists());
        lock.unlock().expect("unlock allocator");
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_allocator_scan_policy_uses_trust_count_and_compaction() {
        let allocator = ClusterKvWatchAllocatorState {
            next_watch_id: 5,
            active_watch_count: Some(2),
            pruned_compacted_revision: Some(KvRevision(7)),
        };

        assert!(cluster_kv_watch_allocator_needs_scan(
            false,
            &allocator,
            KvRevision(7)
        ));
        assert!(!cluster_kv_watch_allocator_needs_scan(
            true,
            &allocator,
            KvRevision(7)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &allocator,
            KvRevision(8)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &ClusterKvWatchAllocatorState {
                active_watch_count: None,
                ..allocator.clone()
            },
            KvRevision(7)
        ));
        assert!(cluster_kv_watch_allocator_needs_scan(
            true,
            &ClusterKvWatchAllocatorState {
                pruned_compacted_revision: None,
                ..allocator
            },
            KvRevision(7)
        ));
    }

    #[test]
    fn cluster_kv_watch_allocator_after_scan_repairs_next_id_and_counts() {
        let allocator = ClusterKvWatchAllocatorState {
            next_watch_id: 2,
            active_watch_count: None,
            pruned_compacted_revision: None,
        };
        let scan = ClusterKvWatchScanResult {
            scanned: true,
            removed: 1,
            retained: 3,
            max_seen_id: 9,
            removed_paths: Vec::new(),
            retained_registrations: Vec::new(),
        };

        assert_eq!(
            cluster_kv_watch_allocator_after_scan(allocator, &scan, KvRevision(4)),
            ClusterKvWatchAllocatorState {
                next_watch_id: 10,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(4)),
            }
        );
    }

    #[test]
    fn cluster_kv_watch_allocator_after_registration_removed_updates_known_count() {
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(3),
                pruned_compacted_revision: Some(KvRevision(4)),
            }),
            ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(2),
                pruned_compacted_revision: Some(KvRevision(4)),
            }
        );
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: Some(0),
                pruned_compacted_revision: Some(KvRevision(4)),
            })
            .active_watch_count,
            Some(0)
        );
        assert_eq!(
            cluster_kv_watch_allocator_after_registration_removed(ClusterKvWatchAllocatorState {
                next_watch_id: 7,
                active_watch_count: None,
                pruned_compacted_revision: Some(KvRevision(4)),
            })
            .active_watch_count,
            None
        );
    }

    #[test]
    fn cluster_kv_watch_registration_create_allocates_and_reports_allocator() {
        let root = temp_raft_root("sinomo-kv-watch-create");
        let paths = ClusterKvPaths::new(root.clone());

        let first = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/first/".to_string(),
                start_revision: KvRevision(1),
            },
            KvRevision::ZERO,
            false,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create first registration");

        assert_eq!(first.registration.watch_id, WatchId(1));
        assert!(first.watch_path.exists());
        assert_eq!(
            first.allocator,
            ClusterKvWatchAllocatorState {
                next_watch_id: 2,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision::ZERO),
            }
        );
        assert!(
            first.scan.as_ref().is_some_and(|scan| scan.scanned),
            "untrusted allocator should force a watch directory scan"
        );
        write_cluster_kv_watch_allocator_state(&paths, &first.allocator)
            .expect("persist allocator");

        let second = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/second/".to_string(),
                start_revision: KvRevision(1),
            },
            KvRevision::ZERO,
            true,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create second registration");

        assert_eq!(second.registration.watch_id, WatchId(2));
        assert!(second.scan.is_none());
        assert_eq!(second.allocator.active_watch_count, Some(2));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_lifecycle_helpers_create_drain_append_and_cancel() {
        let root = temp_raft_root("sinomo-kv-watch-lifecycle-helper");
        let paths = ClusterKvPaths::new(root.clone());
        let node_id = NodeId::from_legacy_named("node_kv_watch_lifecycle").expect("node");
        let mut store = InProcessKvStore::new();
        store
            .put("/watch/lifecycle/a", b"a".to_vec(), None)
            .expect("seed snapshot event");
        write_cluster_kv_snapshot(&paths, &store.snapshot()).expect("write snapshot");

        let watch = create_cluster_kv_watch_for_node(
            &paths,
            node_id.clone(),
            ClusterKvWatchPrefixRequest {
                prefix: "/watch/lifecycle/".to_string(),
                start_revision: KvRevision(1),
            },
        )
        .expect("create watch");
        assert_eq!(watch.watch_id, WatchId(1));
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths)
                .expect("allocator after create")
                .active_watch_count,
            Some(1)
        );

        let drained = drain_cluster_kv_watch_for_node(
            &paths,
            node_id.clone(),
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 0,
            },
        )
        .expect("drain snapshot event");
        assert_eq!(
            drained
                .events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/watch/lifecycle/a"]
        );
        assert_eq!(drained.next_revision, KvRevision(2));

        let fanout = KvEvent {
            revision: KvRevision(2),
            key: "/watch/lifecycle/b".to_string(),
            kind: KvEventKind::Put {
                value: b"b".to_vec(),
            },
        };
        assert_eq!(
            append_cluster_kv_watch_events_for_active_watches_for_node(
                &paths,
                node_id.clone(),
                std::slice::from_ref(&fanout),
            )
            .expect("append fanout event"),
            1
        );
        let drained = drain_cluster_kv_watch_for_node(
            &paths,
            node_id,
            ClusterKvWatchDrainRequest {
                watch_id: watch.watch_id,
                wait_timeout_millis: 0,
            },
        )
        .expect("drain fanout event");
        assert_eq!(drained.events, vec![fanout]);
        assert_eq!(drained.next_revision, KvRevision(3));

        let cancelled = cancel_cluster_kv_watch(
            &paths,
            ClusterKvWatchCancelRequest {
                watch_id: watch.watch_id,
            },
        )
        .expect("cancel watch");
        assert!(cancelled.cancelled);
        assert_eq!(
            read_cluster_kv_watch_allocator_state(&paths)
                .expect("allocator after cancel")
                .active_watch_count,
            Some(0)
        );
        assert!(
            !cancel_cluster_kv_watch(
                &paths,
                ClusterKvWatchCancelRequest {
                    watch_id: watch.watch_id,
                },
            )
            .expect("cancel already removed watch")
            .cancelled
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_event_handler_skips_storage_without_active_watchers() {
        let root = temp_raft_root("sinomo-kv-watch-fanout-no-active-watchers");
        let paths = ClusterKvPaths::new(root.clone());
        write_cluster_kv_snapshot(&paths, &InProcessKvStore::new().snapshot())
            .expect("write empty snapshot");
        let cluster_id =
            Id::from_legacy_named("cluster_kv_watch_fanout_no_active").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_no_active_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout no active",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;

        let response = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: state.clone(),
                leader_id,
                events: vec![test_kv_put_event(1, "/watch/fanout/no-active", b"value")],
            },
        )
        .expect("watch event fanout response");

        assert!(response.accepted);
        assert_eq!(response.stored_events, 0);
        assert!(
            !paths.watch_events.exists(),
            "no active watcher should avoid checkpoint writes"
        );
        assert!(
            !paths.watch_events_log.exists(),
            "no active watcher should avoid log appends"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_event_handler_rejects_invalid_source_before_storage() {
        let root = temp_raft_root("sinomo-kv-watch-fanout-reject-invalid-source");
        let paths = ClusterKvPaths::new(root.clone());
        let cluster_id = Id::from_legacy_named("cluster_kv_watch_fanout_reject").expect("cluster");
        let leader_id =
            NodeId::from_legacy_named("node_kv_watch_fanout_reject_leader").expect("leader");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "kv watch fanout reject",
            leader_id.clone(),
            "leader",
            "pk-leader",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;

        let peer_cluster = Id::from_legacy_named("cluster_kv_watch_fanout_other").expect("cluster");
        let mut peer_state = state.clone();
        peer_state.cluster_id = peer_cluster;
        let wrong_cluster = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: peer_state,
                leader_id: leader_id.clone(),
                events: vec![test_kv_put_event(
                    1,
                    "/watch/fanout/wrong-cluster",
                    b"value",
                )],
            },
        )
        .expect("wrong cluster response");
        assert!(!wrong_cluster.accepted);
        assert_eq!(wrong_cluster.stored_events, 0);
        assert!(
            wrong_cluster
                .detail
                .contains("does not match local cluster"),
            "unexpected detail: {}",
            wrong_cluster.detail
        );

        let unauthorized = handle_cluster_kv_watch_events(
            &paths,
            &state,
            ClusterKvWatchEventsRequest {
                state: state.clone(),
                leader_id: NodeId::from_legacy_named("node_kv_watch_fanout_intruder")
                    .expect("node"),
                events: vec![test_kv_put_event(2, "/watch/fanout/intruder", b"value")],
            },
        )
        .expect("unauthorized leader response");
        assert!(!unauthorized.accepted);
        assert_eq!(unauthorized.stored_events, 0);
        assert!(
            unauthorized.detail.contains("unauthorized leader"),
            "unexpected detail: {}",
            unauthorized.detail
        );
        assert!(!paths.watch_events.exists());
        assert!(!paths.watch_events_log.exists());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_registration_create_prunes_and_keeps_high_watermark() {
        let root = temp_raft_root("sinomo-kv-watch-create-scan");
        let paths = ClusterKvPaths::new(root.clone());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale_path = paths.watch_path(WatchId(7));
        let live_path = paths.watch_path(WatchId(9));
        write_cluster_kv_watch_registration_file(
            &stale_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/stale/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write stale");
        write_cluster_kv_watch_registration_file(
            &live_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(9),
                prefix: "/svc/live/".to_string(),
                next_revision: KvRevision(2),
            },
        )
        .expect("write live");

        let report = create_cluster_kv_watch_registration(
            &paths,
            ClusterKvWatchPrefixRequest {
                prefix: "/svc/new/".to_string(),
                start_revision: KvRevision(2),
            },
            KvRevision(1),
            false,
            MAX_CLUSTER_KV_ACTIVE_WATCHES,
        )
        .expect("create after pruning");

        assert!(!stale_path.exists());
        assert!(live_path.exists());
        assert_eq!(report.registration.watch_id, WatchId(10));
        assert_eq!(report.allocator.active_watch_count, Some(2));
        let scan = report.scan.expect("scan report");
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 1);
        assert_eq!(scan.max_seen_id, 9);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_registration_drain_advances_cursor_with_cache_action() {
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(3),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(1),
        };
        let snapshot = InProcessKvStore::new().snapshot();
        let events = vec![
            KvEvent {
                revision: KvRevision(1),
                key: "/svc/one".to_string(),
                kind: KvEventKind::Put {
                    value: b"one".to_vec(),
                },
            },
            KvEvent {
                revision: KvRevision(2),
                key: "/svc/two".to_string(),
                kind: KvEventKind::Put {
                    value: b"two".to_vec(),
                },
            },
            KvEvent {
                revision: KvRevision(2),
                key: "/other/two".to_string(),
                kind: KvEventKind::Put {
                    value: b"ignored".to_vec(),
                },
            },
        ];

        let report = drain_cluster_kv_watch_registration(
            registration,
            KvRevision(1),
            &events,
            &snapshot,
            MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
            true,
            Some(Duration::ZERO),
        )
        .expect("drain registration");

        assert_eq!(report.response.watch_id, WatchId(3));
        assert_eq!(report.response.next_revision, KvRevision(3));
        assert_eq!(
            report
                .response
                .events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/svc/one", "/svc/two"]
        );
        assert_eq!(report.registration.next_revision, KvRevision(3));
        assert_eq!(
            report.cursor_action,
            ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision: KvRevision(1),
            }
        );
    }

    #[test]
    fn cluster_kv_watch_registration_drain_reports_persist_and_compaction() {
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(5),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(1),
        };
        let mut store = InProcessKvStore::new();
        store
            .put("/svc/one", b"one".to_vec(), None)
            .expect("put snapshot event");
        let snapshot = store.snapshot();

        let report = drain_cluster_kv_watch_registration(
            registration.clone(),
            KvRevision(1),
            &[],
            &snapshot,
            MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
            false,
            Some(Duration::ZERO),
        )
        .expect("drain registration");

        assert_eq!(report.response.next_revision, KvRevision(2));
        assert_eq!(
            report.cursor_action,
            ClusterKvWatchDrainCursorAction::Persist
        );

        store.compact(KvRevision(1)).expect("compact snapshot");
        assert!(matches!(
            drain_cluster_kv_watch_registration(
                registration,
                KvRevision(1),
                &[],
                &store.snapshot(),
                MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
                true,
                Some(Duration::ZERO),
            ),
            Err(KvError::Compacted {
                requested: KvRevision(1),
                compacted: KvRevision(1),
            })
        ));
    }

    #[test]
    fn cluster_kv_watch_registration_remove_reports_allocator_update() {
        let root = temp_raft_root("sinomo-kv-watch-remove");
        let paths = ClusterKvPaths::new(root.clone());
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };
        write_cluster_kv_watch_registration_file(
            &paths.watch_path(registration.watch_id),
            &registration,
        )
        .expect("write registration");
        write_cluster_kv_watch_allocator_state(
            &paths,
            &ClusterKvWatchAllocatorState {
                next_watch_id: 8,
                active_watch_count: Some(2),
                pruned_compacted_revision: Some(KvRevision(1)),
            },
        )
        .expect("write allocator");

        let report = remove_cluster_kv_watch_registration_with_allocator(&paths, WatchId(7))
            .expect("remove registration");

        assert!(report.removed);
        assert!(!report.watch_path.exists());
        assert_eq!(
            report.allocator,
            Some(ClusterKvWatchAllocatorState {
                next_watch_id: 8,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision(1)),
            })
        );
        assert!(
            !remove_cluster_kv_watch_registration_with_allocator(&paths, WatchId(7))
                .expect("remove absent registration")
                .removed
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_drain_cursor_persistence_writes_only_when_needed() {
        let root = temp_raft_root("sinomo-kv-watch-drain-cursor-persist");
        let paths = ClusterKvPaths::new(root.clone());
        let watch_path = paths.watch_path(WatchId(7));
        let registration = ClusterKvWatchRegistration {
            watch_id: WatchId(7),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(3),
        };
        write_cluster_kv_watch_registration_file(&watch_path, &registration)
            .expect("write initial registration");
        let advanced = ClusterKvWatchRegistration {
            next_revision: KvRevision(9),
            ..registration.clone()
        };
        let persist_report = ClusterKvWatchDrainRegistrationReport {
            registration: advanced.clone(),
            response: ClusterKvWatchDrainResponse {
                watch_id: advanced.watch_id,
                events: Vec::new(),
                next_revision: advanced.next_revision,
            },
            cursor_action: ClusterKvWatchDrainCursorAction::Persist,
        };

        assert_eq!(
            persist_cluster_kv_watch_drain_cursor(&watch_path, &persist_report)
                .expect("persist cursor"),
            ClusterKvWatchDrainCursorPersistence::Persisted {
                registration: advanced.clone()
            }
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&watch_path, WatchId(7))
                .expect("read persisted registration"),
            advanced
        );

        let cache_only = ClusterKvWatchRegistration {
            next_revision: KvRevision(11),
            ..registration.clone()
        };
        let cache_report = ClusterKvWatchDrainRegistrationReport {
            registration: cache_only.clone(),
            response: ClusterKvWatchDrainResponse {
                watch_id: cache_only.watch_id,
                events: Vec::new(),
                next_revision: cache_only.next_revision,
            },
            cursor_action: ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision: advanced.next_revision,
            },
        };
        assert_eq!(
            persist_cluster_kv_watch_drain_cursor(&watch_path, &cache_report)
                .expect("cache-only cursor"),
            ClusterKvWatchDrainCursorPersistence::CacheOnly {
                registration: cache_only,
                durable_next_revision: advanced.next_revision
            }
        );
        assert_eq!(
            read_cluster_kv_watch_registration_file(&watch_path, WatchId(7))
                .expect("read unchanged durable registration"),
            advanced
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_scan_prunes_stale_and_invalid_registrations() {
        let root = temp_raft_root("sinomo-kv-watch-scan");
        let paths = ClusterKvPaths::new(root.clone());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale_path = paths.watch_path(WatchId(7));
        let live_path = paths.watch_path(WatchId(9));
        let mismatched_path = paths.watch_path(WatchId(11));
        let corrupt_path = paths.watch_path(WatchId(13));
        write_cluster_kv_watch_registration_file(
            &stale_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/stale/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write stale");
        let live = ClusterKvWatchRegistration {
            watch_id: WatchId(9),
            prefix: "/svc/live/".to_string(),
            next_revision: KvRevision(2),
        };
        write_cluster_kv_watch_registration_file(&live_path, &live).expect("write live");
        write_cluster_kv_watch_registration_file(
            &mismatched_path,
            &ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/mismatched/".to_string(),
                next_revision: KvRevision(2),
            },
        )
        .expect("write mismatched");
        std::fs::write(&corrupt_path, b"not-a-sinomo-object").expect("write corrupt");

        let scan =
            scan_cluster_kv_watch_registrations(&paths, KvRevision(1)).expect("scan watches");

        assert!(scan.scanned);
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 3);
        assert_eq!(scan.max_seen_id, 13);
        assert_eq!(
            scan.retained_registrations,
            vec![ClusterKvWatchScanRegistration {
                path: live_path.clone(),
                registration: live,
            }]
        );
        assert!(!stale_path.exists(), "stale registration should be pruned");
        assert!(live_path.exists(), "live registration should remain");
        assert!(
            !mismatched_path.exists(),
            "mismatched registration should be pruned"
        );
        assert!(
            !corrupt_path.exists(),
            "corrupt registration should be pruned"
        );
        assert!(scan.removed_paths.contains(&stale_path));
        assert!(scan.removed_paths.contains(&mismatched_path));
        assert!(scan.removed_paths.contains(&corrupt_path));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_active_watch_registration_report_updates_allocator_after_scan() {
        let root = temp_raft_root("sinomo-kv-active-watch-report");
        let paths = ClusterKvPaths::new(root.clone());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let stale = ClusterKvWatchRegistration {
            watch_id: WatchId(2),
            prefix: "/stale/".to_string(),
            next_revision: KvRevision(2),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(stale.watch_id), &stale)
            .expect("write stale registration");
        let retained = ClusterKvWatchRegistration {
            watch_id: WatchId(4),
            prefix: "/svc/".to_string(),
            next_revision: KvRevision(9),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(retained.watch_id), &retained)
            .expect("write retained registration");
        write_cluster_kv_watch_allocator_state(
            &paths,
            &ClusterKvWatchAllocatorState {
                next_watch_id: 3,
                active_watch_count: Some(99),
                pruned_compacted_revision: Some(KvRevision(1)),
            },
        )
        .expect("write stale allocator");

        let report = cluster_kv_active_watch_registration_report(&paths, KvRevision(5))
            .expect("active watch report");

        assert!(report.has_active);
        assert_eq!(
            report.allocator,
            Some(ClusterKvWatchAllocatorState {
                next_watch_id: 5,
                active_watch_count: Some(1),
                pruned_compacted_revision: Some(KvRevision(5)),
            })
        );
        let scan = report.scan.expect("scan report");
        assert_eq!(scan.retained, 1);
        assert_eq!(scan.removed, 1);
        assert_eq!(scan.retained_registrations[0].registration, retained);
        assert!(
            !paths.watch_path(stale.watch_id).exists(),
            "stale registration should be pruned by report scan"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_id_file_names_parse_only_watch_objects() {
        assert_eq!(
            cluster_kv_watch_id_from_file_name("watch-42.sobj"),
            Some(42)
        );
        assert_eq!(cluster_kv_watch_id_from_file_name("watch-.sobj"), None);
        assert_eq!(cluster_kv_watch_id_from_file_name("watch-42.tmp"), None);
        assert_eq!(cluster_kv_watch_id_from_file_name("other-42.sobj"), None);
    }

    #[test]
    fn cluster_kv_watch_registration_listing_ignores_invalid_files() {
        let root = temp_raft_root("sinomo-kv-watch-registration-list");
        let paths = ClusterKvPaths::new(root.clone());
        std::fs::create_dir_all(&paths.watches).expect("watch dir");
        let first = ClusterKvWatchRegistration {
            watch_id: WatchId(3),
            prefix: "/svc/a/".to_string(),
            next_revision: KvRevision(2),
        };
        let second = ClusterKvWatchRegistration {
            watch_id: WatchId(5),
            prefix: "/svc/b/".to_string(),
            next_revision: KvRevision(4),
        };
        write_cluster_kv_watch_registration_file(&paths.watch_path(first.watch_id), &first)
            .expect("write first");
        write_cluster_kv_watch_registration_file(&paths.watch_path(second.watch_id), &second)
            .expect("write second");
        write_cluster_kv_watch_registration_file(
            &paths.watch_path(WatchId(7)),
            &ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/mismatch/".to_string(),
                next_revision: KvRevision(1),
            },
        )
        .expect("write mismatched");
        std::fs::write(paths.watches.join("watch-9.sobj"), b"not-a-sinomo-object")
            .expect("write corrupt");
        std::fs::write(paths.watches.join("ignore.tmp"), b"ignored").expect("write ignored");

        let mut registrations =
            list_cluster_kv_watch_registrations(&paths).expect("list registrations");
        registrations.sort_by_key(|registration| registration.watch_id);

        assert_eq!(registrations, vec![first, second]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_watch_events_matching_registrations_filters_by_prefix_and_revision() {
        let registrations = vec![
            ClusterKvWatchRegistration {
                watch_id: WatchId(1),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(3),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(2),
                prefix: "/svc/b/".to_string(),
                next_revision: KvRevision(5),
            },
        ];
        let events = vec![
            test_kv_put_event(2, "/svc/a/old", b"old"),
            test_kv_put_event(3, "/svc/a/current", b"current"),
            test_kv_put_event(5, "/svc/b/current", b"current"),
            test_kv_put_event(6, "/svc/c/ignored", b"ignored"),
        ];

        let matched = cluster_kv_watch_events_matching_registrations(&registrations, &events);

        assert_eq!(
            matched
                .iter()
                .map(|event| (event.revision.0, event.key.as_str()))
                .collect::<Vec<_>>(),
            vec![(3, "/svc/a/current"), (5, "/svc/b/current")]
        );
    }

    #[test]
    fn cluster_kv_watch_ids_matching_events_are_deduped_and_ordered() {
        let registrations = vec![
            ClusterKvWatchRegistration {
                watch_id: WatchId(9),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(2),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(3),
                prefix: "/svc/a/".to_string(),
                next_revision: KvRevision(4),
            },
            ClusterKvWatchRegistration {
                watch_id: WatchId(7),
                prefix: "/svc/b/".to_string(),
                next_revision: KvRevision(5),
            },
        ];
        let events = vec![
            test_kv_put_event(3, "/svc/a/one", b"one"),
            test_kv_put_event(5, "/svc/a/two", b"two"),
            test_kv_put_event(5, "/svc/b/three", b"three"),
        ];

        assert_eq!(
            cluster_kv_watch_ids_matching_events(&registrations, &events),
            vec![WatchId(3), WatchId(7), WatchId(9)]
        );
    }

    fn test_kv_snapshot_with_events(events: Vec<KvEvent>) -> KvSnapshot {
        KvSnapshot {
            revision: events
                .iter()
                .map(|event| event.revision)
                .max()
                .unwrap_or(KvRevision::ZERO),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events,
        }
    }

    fn test_kv_put_event(revision: u64, key: &str, value: &[u8]) -> KvEvent {
        KvEvent {
            revision: KvRevision(revision),
            key: key.to_string(),
            kind: KvEventKind::Put {
                value: value.to_vec(),
            },
        }
    }

    fn test_kv_delete_event(revision: u64, key: &str) -> KvEvent {
        KvEvent {
            revision: KvRevision(revision),
            key: key.to_string(),
            kind: KvEventKind::Delete,
        }
    }

    fn put_test_prefix_keys(store: &mut InProcessKvStore, prefix: &str, count: usize) {
        for index in 0..count {
            let key = format!("{prefix}{index:05}");
            store.put(key, vec![index as u8], None).expect("put");
        }
    }

    fn put_test_lease_keys(
        store: &mut InProcessKvStore,
        prefix: &str,
        count: usize,
        lease_id: LeaseId,
    ) {
        for index in 0..count {
            let key = format!("{prefix}{index:05}");
            store
                .put(key, vec![index as u8], Some(lease_id))
                .expect("leased put");
        }
    }

    #[test]
    fn write_object_file_skips_identical_and_rewrites_after_external_change() {
        let _guard = OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX
            .lock()
            .expect("object mark cache mutex");
        // Unique path so the test is independent of any shared state.
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-objfile-guard-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("obj.sobj");

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        write_object_file(&path, &record).expect("initial write");
        let mtime_after_write = std::fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime");

        // An identical re-write takes the fast path and leaves the file untouched.
        write_object_file(&path, &record).expect("identical re-write");
        assert_eq!(
            std::fs::metadata(&path)
                .expect("metadata")
                .modified()
                .expect("mtime"),
            mtime_after_write,
            "an identical re-write must be skipped without rewriting the file"
        );

        // Another writer replaces the content behind our back with the same byte
        // length. This must be detected by the file-identity guard, not just length.
        let original_len = std::fs::metadata(&path).expect("metadata").len() as usize;
        let replacement = vec![b'x'; original_len];
        let replacement_path = root.join("replacement.sobj");
        std::fs::write(&replacement_path, replacement).expect("external write");
        std::fs::rename(&replacement_path, &path).expect("external replace");

        // Writing our content again must detect the change via the stat guard, fall
        // back to read + compare, and actually rewrite -- never falsely skip.
        write_object_file(&path, &record).expect("rewrite after external change");
        let restored: TestRecord = read_object_file(&path).expect("read back");
        assert_eq!(
            restored, record,
            "a file changed since our last write must be rewritten, not skipped"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn object_file_write_mark_cache_retains_recent_entry_at_capacity() {
        let _guard = OBJECT_FILE_WRITE_MARK_CACHE_TEST_MUTEX
            .lock()
            .expect("object mark cache mutex");
        LAST_WRITTEN_OBJECT_FILES.clear();
        LAST_WRITTEN_OBJECT_FILES.reset_clock();
        let hash = blake3::hash(b"stable");
        let hot_path = PathBuf::from("/sinomo-kv-object-mark-lru/000-hot.sobj");
        for index in 0..FILE_CACHE_CAPACITY.saturating_sub(1) {
            LAST_WRITTEN_OBJECT_FILES.record_mark(
                &PathBuf::from(format!(
                    "/sinomo-kv-object-mark-lru/100-quiet-{index:04}.sobj"
                )),
                hash,
                6,
                Some(SystemTime::UNIX_EPOCH),
                index as u64,
            );
        }
        LAST_WRITTEN_OBJECT_FILES.record_mark(
            &hot_path,
            hash,
            6,
            Some(SystemTime::UNIX_EPOCH),
            FILE_CACHE_CAPACITY as u64,
        );
        assert!(
            LAST_WRITTEN_OBJECT_FILES.refresh(&hot_path),
            "looking up the hot object write mark should refresh its recency"
        );

        LAST_WRITTEN_OBJECT_FILES.record_mark(
            &PathBuf::from("/sinomo-kv-object-mark-lru/999-extra.sobj"),
            hash,
            6,
            Some(SystemTime::UNIX_EPOCH),
            (FILE_CACHE_CAPACITY + 1) as u64,
        );

        assert!(
            LAST_WRITTEN_OBJECT_FILES.refresh(&hot_path),
            "recent object write mark should survive capacity churn"
        );
        LAST_WRITTEN_OBJECT_FILES.retain_not_under_root(Path::new("/sinomo-kv-object-mark-lru"));
    }

    #[test]
    fn write_object_file_cleans_temp_file_after_replace_failure() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-objfile-temp-cleanup-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("obj.sobj");
        std::fs::create_dir(&path).expect("target directory");

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        let error = write_object_file(&path, &record).expect_err("directory replace should fail");
        assert!(
            error.to_string().contains("obj.sobj"),
            "unexpected error: {error}"
        );
        let leftovers = std::fs::read_dir(&root)
            .expect("read temp dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".obj.sobj.tmp-"))
            })
            .count();
        assert_eq!(
            leftovers, 0,
            "failed object replacement should not leave staged temp files"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_optional_object_returns_none_only_for_missing_file() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-optional-objfile-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let missing_path = root.join("missing.sobj");
        assert_eq!(
            read_optional_object::<TestRecord>(&missing_path).expect("missing optional object"),
            None
        );

        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };
        let object_path = root.join("record.sobj");
        write_object_file(&object_path, &record).expect("write object");
        assert_eq!(
            read_optional_object::<TestRecord>(&object_path).expect("present optional object"),
            Some(record)
        );

        let directory_path = root.join("directory.sobj");
        std::fs::create_dir(&directory_path).expect("directory");
        let error =
            read_optional_object::<TestRecord>(&directory_path).expect_err("directory is not none");
        assert!(
            error.to_string().contains("directory.sobj"),
            "present unreadable objects should stay errors, got {error}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_object_file_rejects_oversized_object_before_reading() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-objfile-oversized-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("snapshot.sobj");
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("create sparse object")
            .set_len(MAX_OBJECT_FILE_READ_BYTES + 1)
            .expect("make sparse object oversized");

        let error = read_object_file::<KvSnapshot>(&path).expect_err("oversized object");
        let expected_path = path.display().to_string();
        assert!(
            matches!(
                &error,
                KvError::Io { path: error_path, detail }
                    if error_path == &expected_path
                        && detail.contains("object file is")
                        && detail.contains("max is")
            ),
            "unexpected error for oversized object: {error:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn serde_values_use_sinomo_object() {
        let mut store = InProcessKvStore::new();
        let record = TestRecord {
            name: "alpha".to_string(),
            count: 7,
        };

        store
            .put_serde("/typed/object", &record, None)
            .expect("put object value");
        let object_entry = store.get("/typed/object").expect("object entry");
        assert!(object_entry.value.starts_with(sinomo_object::MAGIC));
        assert_eq!(
            store
                .get_serde::<TestRecord>("/typed/object")
                .expect("decode object"),
            Some(record)
        );
    }

    #[test]
    fn range_prefix_uses_ordered_bounds_and_enforces_limit() {
        let mut store = InProcessKvStore::new();
        for key in [
            "/apps/service/a",
            "/apps/service/b",
            "/apps/servicez/not-a-child",
            "/apps/servicf/not-a-child",
            "/other/service/c",
        ] {
            store.put(key, key.as_bytes().to_vec(), None).expect("put");
        }

        let entries = store.range_prefix("/apps/service/");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );

        let error = store
            .range_prefix_limited("/apps/service/", 1)
            .expect_err("range limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/apps/service/".to_string(),
                max_entries: 1,
            }
        );

        let error = store
            .range_prefix_limited_by_payload_bytes("/apps/service/", 4, "/apps/service/a".len())
            .expect_err("range payload limit");
        assert_eq!(
            error,
            KvError::RangePayloadLimit {
                prefix: "/apps/service/".to_string(),
                observed_bytes: "/apps/service/a".len() + "/apps/service/a".len(),
                max_bytes: "/apps/service/a".len(),
            }
        );
    }

    #[test]
    fn typed_range_prefix_reads_enforce_entry_and_payload_limits() {
        let mut store = InProcessKvStore::new();
        for key in ["/apps/typed/a", "/apps/typed/b"] {
            store.put_serde(key, &key.to_string(), None).expect("put");
        }

        let values = store
            .range_prefix_serde_limited_by_payload_bytes::<String>("/apps/typed/", 2, usize::MAX)
            .expect("typed range");
        assert_eq!(
            values,
            vec!["/apps/typed/a".to_string(), "/apps/typed/b".to_string()]
        );

        assert_eq!(
            store
                .range_prefix_serde_limited_by_payload_bytes::<String>(
                    "/apps/typed/",
                    1,
                    usize::MAX,
                )
                .expect_err("entry limit"),
            KvError::RangeLimit {
                prefix: "/apps/typed/".to_string(),
                max_entries: 1,
            }
        );

        let error = store
            .range_prefix_serde_limited_by_payload_bytes::<String>("/apps/typed/", 2, 1)
            .expect_err("payload limit");
        assert!(matches!(
            error,
            KvError::RangePayloadLimit {
                ref prefix,
                max_bytes: 1,
                ..
            } if prefix == "/apps/typed/"
        ));
    }

    #[test]
    fn snapshot_point_and_prefix_reads_preserve_store_semantics() {
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let sorted = KvSnapshot {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            entries: vec![
                entry("/apps/service/a", b"a"),
                entry("/apps/service/b", b"b"),
                entry("/apps/servicez/not-child", b"z"),
                entry("/other/service/c", b"c"),
            ],
            leases: Vec::new(),
            events: Vec::new(),
        };

        assert_eq!(
            sorted.get("/apps/service/b").expect("point read").value,
            b"b"
        );
        assert!(sorted.get("/apps/service/missing").is_none());
        assert_eq!(
            sorted
                .range_prefix("/apps/service/")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
        assert_eq!(
            sorted
                .prefix_entries("/apps/service/")
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );

        let unsorted = KvSnapshot {
            entries: vec![
                entry("/apps/service/b", b"b"),
                entry("/apps/service/a", b"stale-a"),
                entry("/other/service/c", b"c"),
                entry("/apps/service/a", b"a"),
            ],
            ..sorted
        }
        .canonicalized();
        assert_eq!(
            unsorted
                .entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b", "/other/service/c"]
        );
        assert_eq!(
            unsorted.get("/apps/service/a").expect("point read").value,
            b"a"
        );
        assert_eq!(
            unsorted
                .range_prefix("/apps/service/")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
        assert_eq!(
            unsorted
                .range_prefix_limited("/apps/service/", 1)
                .expect_err("range limit"),
            KvError::RangeLimit {
                prefix: "/apps/service/".to_string(),
                max_entries: 1,
            }
        );
    }

    #[test]
    fn put_rejects_values_larger_than_metadata_limit() {
        let mut store = InProcessKvStore::new();
        let error = store
            .put("/large/value", vec![0_u8; MAX_KV_VALUE_BYTES + 1], None)
            .expect_err("value limit");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert!(store.get("/large/value").is_none());
        assert_eq!(store.revision(), KvRevision::ZERO);
    }

    #[test]
    fn txn_rejects_large_values_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/large/value".to_string(),
                        value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("value limit aborts txn");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/large/value").is_none());
        assert_eq!(store.revision(), KvRevision(1));
    }

    #[test]
    fn txn_rejects_aggregate_payload_over_command_budget() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let first_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let second_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let expected_bytes =
            first_value.len() + second_value.len() + "/payload/one".len() + "/payload/two".len();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/payload/one".to_string(),
                        value: first_value,
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/payload/two".to_string(),
                        value: second_value,
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("aggregate payload limit");

        assert_eq!(
            error,
            KvError::CommandPayloadTooLarge {
                actual_bytes: expected_bytes,
                max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
            }
        );
        assert!(store.get("/payload/one").is_none());
        assert!(store.get("/payload/two").is_none());
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert_eq!(store.revision(), KvRevision(1));
    }

    #[test]
    fn txn_rejects_large_compare_payloads() {
        let mut store = InProcessKvStore::new();
        let first_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let second_value = vec![0_u8; MAX_KV_VALUE_BYTES];
        let expected_bytes =
            first_value.len() + second_value.len() + "/compare/one".len() + "/compare/two".len();

        let error = store
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: "/compare/one".to_string(),
                        value: first_value,
                    },
                    KvCompare::ValueEquals {
                        key: "/compare/two".to_string(),
                        value: second_value,
                    },
                ],
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect_err("compare payload limit");

        assert_eq!(
            error,
            KvError::CommandPayloadTooLarge {
                actual_bytes: expected_bytes,
                max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
            }
        );
        assert_eq!(store.revision(), KvRevision::ZERO);
    }

    #[test]
    fn delete_prefix_uses_ordered_bounds() {
        let mut store = InProcessKvStore::new();
        for key in [
            "/apps/service/a",
            "/apps/service/b",
            "/apps/servicez/not-a-child",
            "/apps/servicf/not-a-child",
        ] {
            store.put(key, key.as_bytes().to_vec(), None).expect("put");
        }

        let response = store
            .apply_op(KvOp::DeletePrefix {
                prefix: "/apps/service/".to_string(),
            })
            .expect("delete prefix");

        assert!(matches!(
            response,
            KvOpResponse::DeletePrefix {
                deleted_count: 2,
                ..
            }
        ));
        assert!(store.get("/apps/service/a").is_none());
        assert!(store.get("/apps/service/b").is_none());
        assert!(store.get("/apps/servicez/not-a-child").is_some());
        assert!(store.get("/apps/servicf/not-a-child").is_some());
    }

    #[test]
    fn delete_prefix_rejects_oversized_prefix_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS + 1);
        store
            .put("/other/key", b"keep".to_vec(), None)
            .expect("outside put");
        let revision_before = store.revision();

        let error = store
            .apply_op(KvOp::DeletePrefix {
                prefix: "/huge/".to_string(),
            })
            .expect_err("delete-prefix limit");

        assert_eq!(
            error,
            KvError::DeletePrefixLimit {
                prefix: "/huge/".to_string(),
                observed_keys: MAX_DELETE_PREFIX_KEYS + 1,
                max_keys: MAX_DELETE_PREFIX_KEYS,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(store.get("/huge/00000").is_some());
        assert!(
            store
                .get(&format!("/huge/{:05}", MAX_DELETE_PREFIX_KEYS))
                .is_some()
        );
        assert!(store.get("/other/key").is_some());
    }

    #[test]
    fn txn_rejects_delete_prefix_over_mutation_budget_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("mutation budget aborts txn");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/huge/00000").is_some());
        assert!(
            store
                .get(&format!("/huge/{:05}", MAX_DELETE_PREFIX_KEYS - 1))
                .is_some()
        );
    }

    #[test]
    fn txn_stable_put_does_not_consume_mutation_budget_before_delete_prefix() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("stable put should not consume mutation budget");

        assert!(response.succeeded);
        assert_eq!(
            response.responses,
            vec![
                KvOpResponse::Put {
                    key: "/config/a".to_string(),
                    revision: revision_before,
                },
                KvOpResponse::DeletePrefix {
                    prefix: "/huge/".to_string(),
                    deleted_count: MAX_DELETE_PREFIX_KEYS,
                },
            ]
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );
        assert_eq!(store.get("/config/a").expect("stable entry").value, b"old");
        assert!(store.range_prefix("/huge/").is_empty());
    }

    #[test]
    fn txn_delete_prefix_budget_accounts_for_prior_branch_deletes() {
        let mut store = InProcessKvStore::new();
        put_test_prefix_keys(&mut store, "/huge/", MAX_DELETE_PREFIX_KEYS);
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Delete {
                        key: "/huge/00000".to_string(),
                    },
                    KvOp::DeletePrefix {
                        prefix: "/huge/".to_string(),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("txn within mutation budget");

        assert!(response.succeeded);
        assert_eq!(
            response.responses,
            vec![
                KvOpResponse::Delete {
                    key: "/huge/00000".to_string(),
                    deleted: true,
                },
                KvOpResponse::DeletePrefix {
                    prefix: "/huge/".to_string(),
                    deleted_count: MAX_DELETE_PREFIX_KEYS - 1,
                },
            ]
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );
        assert!(store.range_prefix("/huge/").is_empty());
    }

    #[test]
    fn put_and_compare_swap_txn_updates_revision_and_version() {
        let mut store = InProcessKvStore::new();
        let rev1 = store.put("/config/a", b"old".to_vec(), None).expect("put");
        let response = store
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                }],
                success: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                }],
                failure: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"failed".to_vec(),
                    lease_id: None,
                }],
            })
            .expect("txn");

        let entry = store.get("/config/a").expect("entry");
        assert_eq!(rev1, KvRevision(1));
        assert!(response.succeeded);
        assert_eq!(entry.value, b"new");
        assert_eq!(entry.create_revision, KvRevision(1));
        assert_eq!(entry.modify_revision, KvRevision(2));
        assert_eq!(entry.version, KvVersion(2));
    }

    #[test]
    fn txn_error_does_not_partially_apply_ops() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/config/b".to_string(),
                        value: b"bad-lease".to_vec(),
                        lease_id: Some(LeaseId(99)),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("unknown lease aborts txn");

        assert_eq!(
            error,
            KvError::UnknownLease {
                lease_id: LeaseId(99)
            }
        );
        assert_eq!(store.revision(), KvRevision(1));
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.get("/config/b").is_none());
    }

    #[test]
    fn txn_empty_selected_branch_response_is_read_only() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let response = store
            .txn_response_if_selected_branch_empty(&KvTxn {
                compares: vec![KvCompare::Missing {
                    key: "/config/a".to_string(),
                }],
                success: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("empty selected branch");

        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: false,
                revision: revision_before,
                responses: Vec::new(),
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .txn_response_if_selected_branch_empty(&KvTxn {
                    compares: vec![KvCompare::Exists {
                        key: "/config/a".to_string(),
                    }],
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating selected branch"),
            None
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");
    }

    #[test]
    fn txn_no_effect_selected_branch_response_is_read_only() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease grant");
        let revision_before = store.revision();

        let response = store
            .txn_response_if_selected_branch_no_effect(&KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Delete {
                        key: "/missing".to_string(),
                    },
                    KvOp::DeletePrefix {
                        prefix: "/missing-prefix/".to_string(),
                    },
                    KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 10,
                    },
                    KvOp::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    },
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(99),
                    },
                ],
                failure: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"failed".to_vec(),
                    lease_id: None,
                }],
            })
            .expect("no-effect selected branch");

        assert_eq!(
            response,
            Some(KvTxnResponse {
                succeeded: true,
                revision: revision_before,
                responses: vec![
                    KvOpResponse::Put {
                        key: "/config/a".to_string(),
                        revision: revision_before,
                    },
                    KvOpResponse::Delete {
                        key: "/missing".to_string(),
                        deleted: false,
                    },
                    KvOpResponse::DeletePrefix {
                        prefix: "/missing-prefix/".to_string(),
                        deleted_count: 0,
                    },
                    KvOpResponse::LeaseGrant {
                        lease_id: LeaseId(7),
                    },
                    KvOpResponse::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    },
                    KvOpResponse::LeaseRevoke {
                        lease_id: LeaseId(99),
                        deleted_count: 0,
                    },
                ],
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Delete {
                        key: "/config/a".to_string(),
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating delete"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .expect("changed put value"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"old".to_vec(),
                        lease_id: Some(LeaseId(7)),
                    }],
                    failure: Vec::new(),
                })
                .expect("changed put lease"),
            None
        );

        store.tick_leases(1).expect("age lease");
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseKeepAlive {
                        lease_id: LeaseId(7),
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating keepalive"),
            None
        );
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 10,
                    }],
                    failure: Vec::new(),
                })
                .expect("mutating lease grant refresh"),
            None
        );
        store
            .apply_op(KvOp::LeaseKeepAlive {
                lease_id: LeaseId(7),
            })
            .expect("refresh lease");
        assert_eq!(
            store
                .txn_response_if_selected_branch_no_effect(&KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::LeaseGrant {
                        lease_id: LeaseId(7),
                        ttl_ticks: 20,
                    }],
                    failure: Vec::new(),
                })
                .expect("changed lease ttl"),
            None
        );

        let error = store
            .txn_response_if_selected_branch_no_effect(&KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/too-large".to_string(),
                    value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect_err("no-effect preflight still validates payloads");
        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
    }

    #[test]
    fn txn_skips_no_effect_ops_inside_mixed_selected_branch() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/stable", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();
        let watch_id = store
            .watch_prefix("/config/", revision_before.next())
            .expect("watch future config changes");

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/stable".to_string(),
                        value: b"old".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: "/config/changed".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })
            .expect("mixed txn");

        assert_eq!(
            response,
            KvTxnResponse {
                succeeded: true,
                revision: revision_before.next(),
                responses: vec![
                    KvOpResponse::Put {
                        key: "/config/stable".to_string(),
                        revision: revision_before,
                    },
                    KvOpResponse::Put {
                        key: "/config/changed".to_string(),
                        revision: revision_before.next(),
                    },
                ],
            }
        );
        assert_eq!(
            store.revision(),
            revision_before.next(),
            "mixed txns should not allocate a revision for stable no-effect ops"
        );
        assert_eq!(
            store.get("/config/stable").expect("stable entry").version,
            KvVersion(1),
            "stable no-effect put should not rewrite entry metadata"
        );
        let events = store.drain_watch(watch_id).expect("watch events");
        assert_eq!(
            events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/config/changed"],
            "stable no-effect ops inside mixed txns should not emit watch events"
        );
    }

    #[test]
    fn op_no_effect_response_is_read_only_and_validated() {
        let mut store = InProcessKvStore::new();
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease grant");
        let revision_before = store.revision();

        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                    lease_id: None,
                })
                .expect("stable put"),
            Some(KvOpResponse::Put {
                key: "/config/a".to_string(),
                revision: revision_before,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Delete {
                    key: "/missing".to_string(),
                })
                .expect("missing delete"),
            Some(KvOpResponse::Delete {
                key: "/missing".to_string(),
                deleted: false,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::DeletePrefix {
                    prefix: "/missing-prefix/".to_string(),
                })
                .expect("empty delete prefix"),
            Some(KvOpResponse::DeletePrefix {
                prefix: "/missing-prefix/".to_string(),
                deleted_count: 0,
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                })
                .expect("stable lease grant"),
            Some(KvOpResponse::LeaseGrant {
                lease_id: LeaseId(7),
            })
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseRevoke {
                    lease_id: LeaseId(99),
                })
                .expect("missing lease revoke"),
            Some(KvOpResponse::LeaseRevoke {
                lease_id: LeaseId(99),
                deleted_count: 0,
            })
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(store.get("/config/a").expect("entry").value, b"old");

        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Delete {
                    key: "/config/a".to_string(),
                })
                .expect("mutating delete"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                })
                .expect("changed put value"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                    lease_id: Some(LeaseId(7)),
                })
                .expect("changed put lease"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 20,
                })
                .expect("changed lease grant"),
            None
        );
        store.tick_leases(1).expect("age lease");
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseGrant {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                })
                .expect("aged lease grant"),
            None
        );
        assert_eq!(
            store
                .op_response_if_no_effect(&KvOp::LeaseKeepAlive {
                    lease_id: LeaseId(99),
                })
                .expect_err("unknown keepalive"),
            KvError::UnknownLease {
                lease_id: LeaseId(99),
            }
        );
    }

    #[test]
    fn txn_rejects_oversized_compares_and_branches() {
        let mut store = InProcessKvStore::new();
        let compares = (0..=MAX_KV_TXN_COMPARES)
            .map(|index| KvCompare::Missing {
                key: format!("/compare/{index}"),
            })
            .collect::<Vec<_>>();

        let error = store
            .txn(KvTxn {
                compares,
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect_err("compare limit");
        assert_eq!(
            error,
            KvError::TxnLimit {
                compares: MAX_KV_TXN_COMPARES + 1,
                success_ops: 0,
                failure_ops: 0,
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            }
        );

        let failure = (0..=MAX_KV_TXN_OPS_PER_BRANCH)
            .map(|index| KvOp::Delete {
                key: format!("/unused/failure/{index}"),
            })
            .collect::<Vec<_>>();
        let error = store
            .txn(KvTxn {
                compares: vec![KvCompare::Missing {
                    key: "/selected/success".to_string(),
                }],
                success: Vec::new(),
                failure,
            })
            .expect_err("unselected branch still counts");
        assert_eq!(
            error,
            KvError::TxnLimit {
                compares: 1,
                success_ops: 0,
                failure_ops: MAX_KV_TXN_OPS_PER_BRANCH + 1,
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            }
        );
    }

    #[test]
    fn txn_preflight_tracks_lease_state_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        for index in 1..=MAX_ACTIVE_LEASES {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(index as u64),
                    ttl_ticks: 10,
                })
                .expect("lease grant");
        }

        let replacement_lease = LeaseId((MAX_ACTIVE_LEASES + 1) as u64);
        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(1),
                    },
                    KvOp::LeaseGrant {
                        lease_id: replacement_lease,
                        ttl_ticks: 20,
                    },
                    KvOp::Put {
                        key: "/leased/replacement".to_string(),
                        value: b"ok".to_vec(),
                        lease_id: Some(replacement_lease),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("revoke then grant");
        assert!(response.succeeded);
        assert!(!store.leases.contains_key(&LeaseId(1)));
        assert!(store.leases.contains_key(&replacement_lease));
        assert_eq!(
            store
                .get("/leased/replacement")
                .expect("replacement entry")
                .lease_id,
            Some(replacement_lease)
        );

        let rejected_lease = LeaseId((MAX_ACTIVE_LEASES + 2) as u64);
        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::LeaseGrant {
                        lease_id: rejected_lease,
                        ttl_ticks: 20,
                    },
                    KvOp::LeaseRevoke {
                        lease_id: LeaseId(2),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("grant before revoke hits lease limit");
        assert_eq!(
            error,
            KvError::LeaseLimit {
                max: MAX_ACTIVE_LEASES
            }
        );
        assert!(store.leases.contains_key(&LeaseId(2)));
        assert!(!store.leases.contains_key(&rejected_lease));
    }

    #[test]
    fn leased_put_rejects_over_key_limit_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(50);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/full/", MAX_KEYS_PER_LEASE, lease_id);
        let revision_before = store.revision();

        let error = store
            .put(
                format!("/lease/full/{:05}", MAX_KEYS_PER_LEASE),
                b"too-many".to_vec(),
                Some(lease_id),
            )
            .expect_err("lease key limit");

        assert_eq!(
            error,
            KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: MAX_KEYS_PER_LEASE + 1,
                max_keys: MAX_KEYS_PER_LEASE,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(
            store
                .get(&format!("/lease/full/{:05}", MAX_KEYS_PER_LEASE))
                .is_none()
        );

        store
            .put("/lease/full/00000", b"updated".to_vec(), Some(lease_id))
            .expect("update existing leased key at limit");
        assert_eq!(
            store.get("/lease/full/00000").expect("updated").value,
            b"updated"
        );
    }

    #[test]
    fn txn_rejects_leased_put_over_key_limit_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(51);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/txn-full/", MAX_KEYS_PER_LEASE, lease_id);
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::Put {
                        key: format!("/lease/txn-full/{:05}", MAX_KEYS_PER_LEASE),
                        value: b"too-many".to_vec(),
                        lease_id: Some(lease_id),
                    },
                ],
                failure: Vec::new(),
            })
            .expect_err("lease key limit aborts txn");

        assert_eq!(
            error,
            KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: MAX_KEYS_PER_LEASE + 1,
                max_keys: MAX_KEYS_PER_LEASE,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(
            store
                .get(&format!("/lease/txn-full/{:05}", MAX_KEYS_PER_LEASE))
                .is_none()
        );
    }

    #[test]
    fn txn_lease_key_limit_accounts_for_prior_branch_delete() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(52);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(&mut store, "/lease/rotate/", MAX_KEYS_PER_LEASE, lease_id);
        let revision_before = store.revision();

        let response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Delete {
                        key: "/lease/rotate/00000".to_string(),
                    },
                    KvOp::Put {
                        key: format!("/lease/rotate/{:05}", MAX_KEYS_PER_LEASE),
                        value: b"replacement".to_vec(),
                        lease_id: Some(lease_id),
                    },
                ],
                failure: Vec::new(),
            })
            .expect("delete frees lease capacity");

        assert!(response.succeeded);
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0.saturating_add(2))
        );
        assert!(store.get("/lease/rotate/00000").is_none());
        assert!(
            store
                .get(&format!("/lease/rotate/{:05}", MAX_KEYS_PER_LEASE))
                .is_some()
        );
        assert_eq!(
            store.lease_keys.get(&lease_id).expect("lease keys").len(),
            MAX_KEYS_PER_LEASE
        );
    }

    #[test]
    fn lease_revoke_rejects_over_mutation_budget_without_partial_apply() {
        let lease_id = LeaseId(42);
        let entries = (0..=MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| KvEntry {
                key: format!("/leased/huge/{index:05}"),
                value: vec![index as u8],
                create_revision: KvRevision((index + 1) as u64),
                modify_revision: KvRevision((index + 1) as u64),
                version: KvVersion(1),
                lease_id: Some(lease_id),
            })
            .collect::<Vec<_>>();
        let mut store = InProcessKvStore::restore(KvSnapshot {
            revision: KvRevision(entries.len() as u64),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: vec![KvLease {
                lease_id,
                ttl_ticks: 10,
                remaining_ticks: 10,
                granted_at: OffsetDateTime::now_utc(),
            }],
            events: Vec::new(),
        });
        let revision_before = store.revision();

        let error = store
            .apply_op(KvOp::LeaseRevoke { lease_id })
            .expect_err("lease revoke limit");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert!(store.leases.contains_key(&lease_id));
        assert!(store.get("/leased/huge/00000").is_some());
        assert!(
            store
                .get(&format!(
                    "/leased/huge/{:05}",
                    MAX_KV_MUTATION_EVENTS_PER_COMMAND
                ))
                .is_some()
        );
    }

    #[test]
    fn txn_rejects_lease_revoke_over_mutation_budget_without_partial_apply() {
        let mut store = InProcessKvStore::new();
        let lease_id = LeaseId(43);
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id,
                ttl_ticks: 10,
            })
            .expect("lease grant");
        put_test_lease_keys(
            &mut store,
            "/leased/budget/",
            MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            lease_id,
        );
        store
            .put("/config/a", b"old".to_vec(), None)
            .expect("initial put");
        let revision_before = store.revision();

        let error = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"new".to_vec(),
                        lease_id: None,
                    },
                    KvOp::LeaseRevoke { lease_id },
                ],
                failure: Vec::new(),
            })
            .expect_err("mutation budget aborts txn");

        assert_eq!(
            error,
            KvError::MutationEventLimit {
                observed_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1,
                max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
            }
        );
        assert_eq!(store.revision(), revision_before);
        assert_eq!(
            store.get("/config/a").expect("existing value").value,
            b"old"
        );
        assert!(store.leases.contains_key(&lease_id));
        assert!(store.get("/leased/budget/00000").is_some());
    }

    #[test]
    fn watch_prefix_receives_backlog_and_future_events() {
        let mut store = InProcessKvStore::new();
        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put");
        store.put("/other/a", b"skip".to_vec(), None).expect("put");
        let watch_id = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("watch");

        let backlog = store.drain_watch(watch_id).expect("backlog");
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog[0].key, "/services/a");

        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put");
        let future = store.drain_watch(watch_id).expect("future");
        assert_eq!(future.len(), 1);
        assert_eq!(future[0].key, "/services/b");
    }

    #[test]
    fn watch_prefix_index_routes_only_matching_prefixes() {
        let mut store = InProcessKvStore::new();
        let root = store.watch_prefix("", KvRevision(1)).expect("root watch");
        let services = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("services watch");
        let exact = store
            .watch_prefix("/services/a", KvRevision(1))
            .expect("exact watch");
        let other = store
            .watch_prefix("/other/", KvRevision(1))
            .expect("other watch");

        assert_eq!(
            store.matching_watch_ids_for_event_key_for_test("/services/a"),
            vec![root, services, exact],
            "event routing should enumerate only watches whose prefixes match the changed key"
        );

        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put services");
        assert_eq!(store.drain_watch(root).expect("root events").len(), 1);
        assert_eq!(
            store.drain_watch(services).expect("services events").len(),
            1
        );
        assert_eq!(store.drain_watch(exact).expect("exact events").len(), 1);
        assert!(
            store.drain_watch(other).expect("other events").is_empty(),
            "unmatched watches should not receive the event"
        );

        assert_eq!(store.cancel_watch(services), Ok(true));
        assert_eq!(
            store.matching_watch_ids_for_event_key_for_test("/services/b"),
            vec![root],
            "canceled watches must be removed from the prefix index"
        );

        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put after cancel");
        assert_eq!(store.drain_watch(root).expect("root second").len(), 1);
        assert!(
            store.drain_watch(exact).expect("exact second").is_empty(),
            "exact key watch should not match a sibling key"
        );
        assert_eq!(
            store.drain_watch(services),
            Err(KvError::UnknownWatch { watch_id: services })
        );
    }

    #[test]
    fn matching_watch_queues_share_event_payloads() {
        let mut store = InProcessKvStore::new();
        let first = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("first watch");
        let second = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("second watch");
        let value = vec![7_u8; 128 * 1024];

        store
            .put("/services/shared", value.clone(), None)
            .expect("put shared event");

        {
            let first_event = store
                .watches
                .get(&first)
                .expect("first watch state")
                .pending
                .front()
                .expect("first pending event");
            let second_event = store
                .watches
                .get(&second)
                .expect("second watch state")
                .pending
                .front()
                .expect("second pending event");
            let history_event = store.events.last().expect("history event");

            assert!(
                Arc::ptr_eq(first_event, second_event),
                "matching watch queues should share the same event allocation"
            );
            assert!(
                Arc::ptr_eq(first_event, history_event),
                "watch queues and event history should share the same event allocation"
            );
            assert_eq!(
                Arc::strong_count(history_event),
                3,
                "one history entry plus two pending watch entries should reference one event"
            );
        }

        let drained = store.drain_watch(first).expect("drain first watch");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].key, "/services/shared");
        assert_eq!(drained[0].kind, KvEventKind::Put { value });
    }

    #[test]
    fn slow_watch_queue_is_bounded_and_reports_compaction() {
        let mut store = InProcessKvStore::new();
        let watch_id = store
            .watch_prefix("/events/", KvRevision(1))
            .expect("watch");

        for index in 0..=MAX_PENDING_EVENTS_PER_WATCH {
            store
                .put(
                    format!("/events/{index}"),
                    index.to_string().into_bytes(),
                    None,
                )
                .expect("put event");
        }

        let error = store
            .drain_watch(watch_id)
            .expect_err("lagged watch must fail instead of silently dropping events");
        assert_eq!(
            error,
            KvError::Compacted {
                requested: KvRevision(1),
                compacted: KvRevision(1),
            }
        );
        assert_eq!(
            store.drain_watch(watch_id),
            Err(KvError::UnknownWatch { watch_id })
        );
        assert!(
            store
                .matching_watch_ids_for_event_key_for_test("/events/next")
                .is_empty(),
            "lagged watches removed after compaction must leave the prefix index"
        );
    }

    #[test]
    fn cancelled_watch_releases_registration() {
        let mut store = InProcessKvStore::new();
        let watch_id = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("watch");
        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put");

        assert_eq!(store.cancel_watch(watch_id), Ok(true));
        assert_eq!(store.cancel_watch(watch_id), Ok(false));
        assert_eq!(
            store.drain_watch(watch_id),
            Err(KvError::UnknownWatch { watch_id })
        );
        assert!(
            store
                .matching_watch_ids_for_event_key_for_test("/services/b")
                .is_empty(),
            "canceled watches must leave the prefix index"
        );
        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put after cancel");
    }

    #[test]
    fn active_watch_count_is_bounded_and_cancel_frees_slot() {
        let mut store = InProcessKvStore::new();
        let mut watch_ids = Vec::new();
        for index in 0..MAX_ACTIVE_WATCHES {
            watch_ids.push(
                store
                    .watch_prefix(format!("/watch/{index}/"), KvRevision(1))
                    .expect("watch within limit"),
            );
        }

        assert_eq!(
            store.watch_prefix("/watch/overflow/", KvRevision(1)),
            Err(KvError::WatchLimit {
                max: MAX_ACTIVE_WATCHES,
            })
        );
        assert_eq!(store.cancel_watch(watch_ids[0]), Ok(true));

        let replacement = store
            .watch_prefix("/watch/replacement/", KvRevision(1))
            .expect("watch after cancel");
        assert_eq!(replacement, WatchId(MAX_ACTIVE_WATCHES as u64 + 1));
    }

    #[test]
    fn event_history_is_bounded_and_advances_compacted_revision() {
        let mut store = InProcessKvStore::new();
        for index in 0..(MAX_RETAINED_EVENTS + 10) {
            store
                .put(
                    format!("/events/{index}"),
                    index.to_string().into_bytes(),
                    None,
                )
                .expect("put event");
        }

        let snapshot = store.snapshot();
        assert_eq!(snapshot.events.len(), MAX_RETAINED_EVENTS);
        assert_eq!(snapshot.compacted_revision, KvRevision(10));
        assert_eq!(
            snapshot.events.first().expect("first retained").revision,
            KvRevision(11)
        );
        assert!(matches!(
            store.watch_prefix("/events/", KvRevision(10)),
            Err(KvError::Compacted { .. })
        ));
        assert!(store.watch_prefix("/events/", KvRevision(11)).is_ok());
    }

    #[test]
    fn lease_expiration_deletes_attached_keys() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 2,
            })
            .expect("lease");
        store
            .put("/services/live", b"yes".to_vec(), Some(LeaseId(1)))
            .expect("put leased");

        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("tick"), Vec::<LeaseId>::new());
        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("expire"), vec![LeaseId(1)]);
        assert!(store.get("/services/live").is_none());
    }

    #[test]
    fn lease_deadline_index_preserves_snapshot_remaining_ticks_and_keepalive() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("long lease");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(2),
                ttl_ticks: 2,
            })
            .expect("short lease");
        store
            .put("/lease/short", b"short".to_vec(), Some(LeaseId(2)))
            .expect("leased short");

        assert_eq!(
            store.tick_leases(2).expect("expire short"),
            vec![LeaseId(2)]
        );
        assert!(store.get("/lease/short").is_none());
        let remaining = store
            .snapshot()
            .leases
            .into_iter()
            .find(|lease| lease.lease_id == LeaseId(1))
            .expect("long lease")
            .remaining_ticks;
        assert_eq!(
            remaining, 8,
            "snapshot materialization should expose aged remaining ticks without scanning all leases"
        );
        assert!(
            store
                .op_response_if_no_effect(&KvOp::LeaseKeepAlive {
                    lease_id: LeaseId(1)
                })
                .expect("keepalive no-effect check")
                .is_none(),
            "aged leases still need a real keepalive"
        );

        store
            .apply_op(KvOp::LeaseKeepAlive {
                lease_id: LeaseId(1),
            })
            .expect("keepalive");
        let refreshed = store
            .snapshot()
            .leases
            .into_iter()
            .find(|lease| lease.lease_id == LeaseId(1))
            .expect("refreshed lease")
            .remaining_ticks;
        assert_eq!(refreshed, 10);
    }

    #[test]
    fn lease_deadline_index_is_restored_from_snapshot_remaining_ticks() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 5,
            })
            .expect("lease");
        store
            .put(
                "/lease/restored-deadline",
                b"value".to_vec(),
                Some(LeaseId(7)),
            )
            .expect("leased value");
        assert_eq!(
            store.tick_leases(2).expect("age lease"),
            Vec::<LeaseId>::new()
        );

        let mut restored = InProcessKvStore::restore(store.snapshot());
        assert_eq!(
            restored.tick_leases(2).expect("not expired yet"),
            Vec::<LeaseId>::new()
        );
        assert!(restored.get("/lease/restored-deadline").is_some());
        assert_eq!(restored.tick_leases(1).expect("expire"), vec![LeaseId(7)]);
        assert!(restored.get("/lease/restored-deadline").is_none());
    }

    #[test]
    fn tick_leases_spreads_large_expiration_batches_across_calls() {
        let mut store = InProcessKvStore::new();
        for lease_id in [LeaseId(1), LeaseId(2)] {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id,
                    ttl_ticks: 1,
                })
                .expect("lease grant");
        }
        put_test_lease_keys(&mut store, "/lease/one/", 3000, LeaseId(1));
        put_test_lease_keys(&mut store, "/lease/two/", 3000, LeaseId(2));
        let revision_before = store.revision();

        assert_eq!(
            store.tick_leases(1).expect("first expiry"),
            vec![LeaseId(1)]
        );
        assert!(!store.leases.contains_key(&LeaseId(1)));
        assert!(store.leases.contains_key(&LeaseId(2)));
        assert!(store.get("/lease/one/00000").is_none());
        assert!(store.get("/lease/two/00000").is_none());
        assert!(store.get("/lease/two/01095").is_none());
        assert!(store.get("/lease/two/01096").is_some());
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );

        assert_eq!(
            store.tick_leases(0).expect("second expiry"),
            vec![LeaseId(2)]
        );
        assert!(!store.leases.contains_key(&LeaseId(2)));
        assert!(store.range_prefix("/lease/").is_empty());
        assert_eq!(store.revision(), KvRevision(revision_before.0 + 6000));
    }

    #[test]
    fn tick_leases_partially_drains_oversized_legacy_lease() {
        let lease_id = LeaseId(9);
        let entries = (0..=MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| KvEntry {
                key: format!("/lease/legacy/{index:05}"),
                value: vec![index as u8],
                create_revision: KvRevision((index + 1) as u64),
                modify_revision: KvRevision((index + 1) as u64),
                version: KvVersion(1),
                lease_id: Some(lease_id),
            })
            .collect::<Vec<_>>();
        let mut store = InProcessKvStore::restore(KvSnapshot {
            revision: KvRevision(entries.len() as u64),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: vec![KvLease {
                lease_id,
                ttl_ticks: 1,
                remaining_ticks: 1,
                granted_at: OffsetDateTime::now_utc(),
            }],
            events: Vec::new(),
        });
        let revision_before = store.revision();

        assert_eq!(
            store.tick_leases(1).expect("partial expiry"),
            Vec::<LeaseId>::new()
        );
        assert!(store.leases.contains_key(&lease_id));
        assert_eq!(
            store
                .lease_keys
                .get(&lease_id)
                .expect("remaining legacy lease keys")
                .len(),
            1
        );
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64)
        );

        assert_eq!(store.tick_leases(0).expect("finish expiry"), vec![lease_id]);
        assert!(!store.leases.contains_key(&lease_id));
        assert!(store.range_prefix("/lease/legacy/").is_empty());
        assert_eq!(
            store.revision(),
            KvRevision(revision_before.0 + MAX_KV_MUTATION_EVENTS_PER_COMMAND as u64 + 1)
        );
    }

    #[test]
    fn lease_revoke_uses_index_and_tracks_reassignment() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("lease one");
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(2),
                ttl_ticks: 10,
            })
            .expect("lease two");
        store
            .put("/lease/a", b"a".to_vec(), Some(LeaseId(1)))
            .expect("leased a");
        store
            .put("/lease/b", b"b".to_vec(), Some(LeaseId(1)))
            .expect("leased b");
        store
            .put("/lease/a", b"moved".to_vec(), Some(LeaseId(2)))
            .expect("move a to lease two");
        store
            .put("/lease/free", b"free".to_vec(), None)
            .expect("free");

        assert_eq!(
            store
                .lease_keys
                .get(&LeaseId(1))
                .expect("lease one keys")
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["/lease/b".to_string()]
        );
        assert!(
            store
                .lease_keys
                .get(&LeaseId(2))
                .expect("lease two keys")
                .contains("/lease/a")
        );

        let response = store
            .apply_op(KvOp::LeaseRevoke {
                lease_id: LeaseId(1),
            })
            .expect("revoke lease one");
        assert_eq!(
            response,
            KvOpResponse::LeaseRevoke {
                lease_id: LeaseId(1),
                deleted_count: 1,
            }
        );
        assert!(store.get("/lease/b").is_none());
        assert!(store.get("/lease/a").is_some());
        assert!(store.get("/lease/free").is_some());
        assert!(!store.lease_keys.contains_key(&LeaseId(1)));
    }

    #[test]
    fn lease_index_is_restored_from_snapshot() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(7),
                ttl_ticks: 10,
            })
            .expect("lease");
        store
            .put("/lease/restored", b"value".to_vec(), Some(LeaseId(7)))
            .expect("leased value");

        let mut restored = InProcessKvStore::restore(store.snapshot());
        assert!(
            restored
                .lease_keys
                .get(&LeaseId(7))
                .expect("restored lease keys")
                .contains("/lease/restored")
        );

        restored
            .apply_op(KvOp::LeaseRevoke {
                lease_id: LeaseId(7),
            })
            .expect("revoke restored lease");
        assert!(restored.get("/lease/restored").is_none());
    }

    #[test]
    fn active_lease_count_is_bounded_and_existing_grants_can_refresh() {
        let mut store = InProcessKvStore::new();
        for index in 1..=MAX_ACTIVE_LEASES {
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(index as u64),
                    ttl_ticks: 10,
                })
                .expect("lease within limit");
        }

        assert_eq!(
            store.apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(MAX_ACTIVE_LEASES as u64 + 1),
                ttl_ticks: 10,
            }),
            Err(KvError::LeaseLimit {
                max: MAX_ACTIVE_LEASES,
            })
        );
        assert!(
            store
                .apply_op(KvOp::LeaseGrant {
                    lease_id: LeaseId(1),
                    ttl_ticks: 20,
                })
                .is_ok(),
            "refreshing an existing lease id should not consume another active slot"
        );
    }

    #[test]
    fn snapshot_restore_preserves_entries_and_revisions() {
        let mut store = InProcessKvStore::new();
        store
            .put("/dfs/workspaces/w1", b"meta".to_vec(), None)
            .expect("put");
        let snapshot = store.snapshot();

        let restored = InProcessKvStore::restore(snapshot);

        assert_eq!(restored.revision(), KvRevision(1));
        assert_eq!(
            restored.get("/dfs/workspaces/w1").expect("entry").value,
            b"meta"
        );
    }

    #[test]
    fn materialized_cache_round_trips_snapshot_as_store() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-cache-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");

        assert!(!cache.exists());
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("write cache")
        );
        assert!(cache.exists());
        assert!(
            !cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("skip unchanged cache")
        );
        let restored = cache.read_store().expect("read cache");
        assert_eq!(restored.get("alpha").expect("alpha").value, b"one");
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_initial_write_skips_required_snapshot_read() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-cache-initial-write-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");

        reset_required_object_file_reads_for_test(cache.path());
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("initial write")
        );
        assert_eq!(
            required_object_file_read_count_for_test(cache.path()),
            0,
            "initial materialized-cache writes should use the optional snapshot read path, not a required missing-file read"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_optional_snapshot_read_handles_absent_present_and_invalid() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-optional-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        assert!(
            cache
                .read_snapshot_arc_if_present()
                .expect("absent optional snapshot")
                .is_none()
        );

        let mut store = InProcessKvStore::new();
        store
            .put("alpha", b"one".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&store.snapshot())
            .expect("write materialized snapshot");
        let snapshot = cache
            .read_snapshot_arc_if_present()
            .expect("present optional snapshot")
            .expect("snapshot");
        assert_eq!(snapshot.get("alpha").expect("alpha").value, b"one");

        std::fs::remove_file(cache.path()).expect("remove snapshot");
        std::fs::create_dir(cache.path()).expect("replace snapshot with directory");
        let error = cache
            .read_snapshot_arc_if_present()
            .expect_err("directory is not absence");
        assert!(
            error.to_string().contains("kv-cache.sobj"),
            "present invalid snapshots should stay errors, got {error}"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_canonicalizes_external_snapshot_entries_on_read() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-canonical-read-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let entry = |key: &str, value: &[u8]| KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        };
        let snapshot = KvSnapshot {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            entries: vec![
                entry("beta", b"two"),
                entry("alpha", b"stale"),
                entry("alpha", b"one"),
            ],
            leases: vec![
                KvLease {
                    lease_id: LeaseId(7),
                    ttl_ticks: 10,
                    remaining_ticks: 10,
                    granted_at: OffsetDateTime::UNIX_EPOCH,
                },
                KvLease {
                    lease_id: LeaseId(7),
                    ttl_ticks: 20,
                    remaining_ticks: 20,
                    granted_at: OffsetDateTime::UNIX_EPOCH,
                },
            ],
            events: Vec::new(),
        };
        write_object_file(cache.path(), &snapshot).expect("write external snapshot");

        let read = cache.read_snapshot().expect("read canonical snapshot");
        assert_eq!(
            read.entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(read.get("alpha").expect("alpha").value, b"one");
        assert_eq!(read.leases.len(), 1);
        assert_eq!(read.leases[0].ttl_ticks, 20);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_skips_rewrite_on_revision_only_changes() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-durable-skip-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store.put("alpha", b"one".to_vec(), None).expect("seed");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("initial write")
        );

        // Re-put the identical value: modify_revision/version advance and an event is
        // appended, but the durable key/value state is unchanged -> no rewrite.
        store.put("alpha", b"one".to_vec(), None).expect("re-put");
        let snap = store.snapshot();
        assert!(snap.revision.0 > 1, "revision should have advanced");
        assert!(
            !cache
                .write_snapshot_if_changed(&snap)
                .expect("durable-equal skip"),
            "re-putting an identical value must not rewrite the snapshot"
        );

        // A real value change still rewrites.
        store.put("alpha", b"two".to_vec(), None).expect("change");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("changed write"),
            "a real value change must rewrite the snapshot"
        );
        assert_eq!(
            cache
                .read_store()
                .expect("read")
                .get("alpha")
                .expect("a")
                .value,
            b"two"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_raft_view_read_skips_extra_durable_state_stats() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        let root = temp_raft_root("sinomo-kv-raft-view-single-identity-probe");
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let wal_paths = KvWalPaths::new(root.join("raft"));
        let node_id =
            NodeId::from_legacy_named("node_raft_view_single_identity_probe").expect("node id");

        {
            let mut node = DurableRaftKvNode::open_with_materialized_cache(
                node_id.clone(),
                wal_paths.clone(),
                &cache,
            )
            .expect("open durable node");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft-view/single-probe".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append command");
            node.commit_to(entry.index).expect("commit command");
        }

        reset_durable_state_path_exists_probes_for_test(&wal_paths);
        let snapshot = cache
            .read_snapshot_with_raft_view(node_id, wal_paths.clone())
            .expect("read raft view");
        assert_eq!(
            snapshot
                .get("/raft-view/single-probe")
                .expect("committed value")
                .value,
            b"value"
        );
        assert_eq!(
            durable_state_path_exists_probe_count_for_test(&wal_paths),
            0,
            "raft-view reads should derive durable-state presence from the identity probe"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_persists_lease_decay() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-lease-decay-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 10,
            })
            .expect("lease");
        store
            .put("/lease/live", b"yes".to_vec(), Some(LeaseId(1)))
            .expect("put leased");
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("initial write")
        );

        store.tick_leases(4).expect("age lease");
        assert_eq!(
            store
                .snapshot()
                .leases
                .iter()
                .find(|lease| lease.lease_id == LeaseId(1))
                .expect("aged lease")
                .remaining_ticks,
            6
        );
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("lease decay write"),
            "lease countdown state is restart-visible and must be persisted"
        );

        let restored = cache.read_store().expect("read");
        assert_eq!(
            restored
                .snapshot()
                .leases
                .iter()
                .find(|lease| lease.lease_id == LeaseId(1))
                .expect("restored lease")
                .remaining_ticks,
            6
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_reports_exact_materialized_snapshot_changes() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-exact-change-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache = KvMaterializedCache::new(root.join("kv-cache.sobj"));
        let mut store = InProcessKvStore::new();
        store.put("alpha", b"one".to_vec(), None).expect("seed");
        let initial = store.snapshot();

        assert!(
            cache
                .write_snapshot_if_materialized_changed(&initial)
                .expect("initial write")
        );
        assert!(
            !cache
                .write_snapshot_if_materialized_changed(&initial)
                .expect("skip exact match"),
            "identical materialized snapshots should be skipped"
        );

        store.put("alpha", b"one".to_vec(), None).expect("re-put");
        let event_only_change = store.snapshot();
        assert!(
            kv_snapshot_durable_eq(&initial, &event_only_change),
            "test setup should differ only in revision/event metadata"
        );
        assert!(
            cache
                .write_snapshot_if_materialized_changed(&event_only_change)
                .expect("event-only materialized write"),
            "revision/event changes must be reported when callers publish snapshot-backed events"
        );

        let persisted = cache.read_snapshot().expect("read exact snapshot");
        assert_eq!(persisted.revision, event_only_change.revision);
        assert_eq!(persisted.events, event_only_change.events);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_compacts_event_history_to_configured_limit() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-materialized-compact-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let cache =
            KvMaterializedCache::new(root.join("kv-cache.sobj")).with_max_materialized_events(4);
        let mut store = InProcessKvStore::new();
        for index in 0..20 {
            store
                .put(format!("k{index}"), vec![index as u8], None)
                .expect("seed");
        }
        assert!(store.snapshot().events.len() > 4);
        assert!(
            cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("write cache")
        );
        let persisted = cache.read_snapshot().expect("read cache");
        assert!(
            persisted.events.len() <= 4,
            "persisted event history should be compacted to the limit, got {}",
            persisted.events.len()
        );
        // Entries are preserved even though the event tail is trimmed.
        assert_eq!(persisted.entries.len(), 20);
        // Re-writing the same store is a no-op (compaction is deterministic).
        assert!(
            !cache
                .write_snapshot_if_changed(&store.snapshot())
                .expect("skip unchanged")
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_does_not_replace_existing_raft_log() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-cache-does-not-replace-log-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_cache_existing_log").expect("node");
        let wal_paths = KvWalPaths::new(root.join("raft"));
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), wal_paths.clone())
                .expect("open raft node");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-log".to_string(),
                    value: b"raft".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted");
        }

        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        let mut cache_store = InProcessKvStore::new();
        cache_store
            .put("/cache", b"cache".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&cache_store.snapshot())
            .expect("write cache");

        let node = DurableRaftKvNode::open_with_materialized_cache(node_id, wal_paths, &cache)
            .expect("open with cache");
        assert_eq!(node.last_log_index(), 1);
        assert!(node.store().get("/cache").is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_current_snapshot_does_not_initialize_empty_raft_root() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-empty-raft-read-only-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_empty_raft_read_only").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let wal_paths = cache.raft_wal_paths(&node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");

        let mut store = InProcessKvStore::new();
        store
            .put("/cached", b"available".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&store.snapshot())
            .expect("write materialized cache");

        let snapshot = cache
            .current_snapshot_arc(node_id)
            .expect("read current snapshot");
        assert_eq!(
            snapshot.get("/cached").expect("cached value").value,
            b"available"
        );
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_empty_snapshot_does_not_initialize_empty_raft_root() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-empty-raft-empty-read-only-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_empty_raft_empty_read_only").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let wal_paths = cache.raft_wal_paths(&node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");

        let snapshot = cache
            .current_snapshot_arc(node_id)
            .expect("read empty current snapshot");
        assert_eq!(snapshot.entries.len(), 0);
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_raft_view_reports_invalid_snapshot_without_initializing_empty_raft() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-empty-raft-invalid-cache-read-only-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_invalid_cache_read_only").expect("node");
        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        let wal_paths = KvWalPaths::new(root.join("raft"));
        std::fs::create_dir_all(&wal_paths.root).expect("empty raft root");
        std::fs::create_dir_all(cache.path()).expect("invalid materialized cache path");

        let error = cache
            .read_snapshot_with_raft_view(node_id, wal_paths.clone())
            .expect_err("invalid materialized cache should stay an error");

        assert!(
            error.to_string().contains("cache.sobj"),
            "error should identify the invalid materialized cache path, got {error}"
        );
        assert!(!wal_paths.lock.exists(), "read should not create raft lock");
        assert!(
            !wal_paths.hard_state.exists(),
            "read should not create raft hard state"
        );
        assert!(!wal_paths.wal.exists(), "read should not create raft wal");
        assert!(
            !wal_paths.snapshot.exists(),
            "read should not create raft snapshot"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn materialized_cache_open_validates_fallback_before_quarantining_raft() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-invalid-fallback-before-quarantine-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id =
            NodeId::from_legacy_named("node_invalid_fallback_before_quarantine").expect("node");
        let wal_paths = KvWalPaths::new(root.join("raft"));
        std::fs::create_dir_all(&wal_paths.snapshot).expect("invalid raft snapshot");
        let cache = KvMaterializedCache::new(root.join("cache.sobj"));
        std::fs::create_dir_all(cache.path()).expect("invalid materialized cache path");

        let error =
            DurableRaftKvNode::open_with_materialized_cache(node_id, wal_paths.clone(), &cache)
                .expect_err("invalid fallback cache should prevent raft quarantine");

        assert!(
            error.to_string().contains("cache.sobj"),
            "fallback cache should be validated before quarantine, got {error}"
        );
        assert!(
            wal_paths.root.exists(),
            "unreadable raft state should remain in place when fallback cache is invalid"
        );
        let quarantine_prefix = format!(
            "{}.corrupt-{}-",
            wal_paths
                .root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("raft"),
            std::process::id()
        );
        let quarantined = std::fs::read_dir(&root)
            .expect("read root")
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&quarantine_prefix))
            });
        assert!(
            !quarantined,
            "invalid fallback cache must not create a raft quarantine directory"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_current_store_prefers_raft_and_refreshes_materialized_cache() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_local_cache").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-current".to_string(),
                    value: b"current".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append raft value");
            node.commit_to(1).expect("commit raft value");
        }
        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/cache-only", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot())
            .expect("write stale cache");

        let current = cache.current_store(node_id).expect("read current store");
        assert!(current.get("/cache-only").is_none());
        assert_eq!(
            current.get("/raft-current").expect("raft value").value,
            b"current"
        );
        let refreshed = cache.cached_store().expect("read refreshed cache");
        assert_eq!(
            refreshed
                .get("/raft-current")
                .expect("cached raft value")
                .value,
            b"current"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_current_store_falls_back_to_snapshot_when_raft_node_is_busy() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-busy-fallback-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_local_cache_busy").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut cached_store = InProcessKvStore::new();
        cached_store
            .put("/cached", b"available".to_vec(), None)
            .expect("seed cache");
        cache
            .write_snapshot(&cached_store.snapshot())
            .expect("write cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        let current = cache
            .current_store(node_id)
            .expect("busy raft should fall back to materialized snapshot");
        assert_eq!(
            current.get("/cached").expect("cached value").value,
            b"available"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_current_store_uses_raft_view_cache_when_lock_is_busy() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-raft-view-cache-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_local_cache_raft_view_cache").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-cached".to_string(),
                    value: b"current".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append raft value");
            node.commit_to(1).expect("commit raft value");
        }

        let current = cache
            .current_store(node_id.clone())
            .expect("seed raft view cache");
        assert_eq!(
            current.get("/raft-cached").expect("raft value").value,
            b"current"
        );

        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/cache-only", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot())
            .expect("write stale materialized cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        let cached = cache
            .current_store(node_id)
            .expect("busy raft should use validated raft view cache");
        assert!(cached.get("/cache-only").is_none());
        assert_eq!(
            cached.get("/raft-cached").expect("cached raft value").value,
            b"current"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_point_and_prefix_reads_use_cached_raft_view_when_lock_is_busy() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-snapshot-read-cache-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_local_cache_snapshot_read").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            for (index, (key, value)) in [
                ("/apps/service/a", b"a".as_slice()),
                ("/apps/service/b", b"b".as_slice()),
                ("/other/service/c", b"c".as_slice()),
            ]
            .into_iter()
            .enumerate()
            {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: key.to_string(),
                            value: value.to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append raft value");
                node.commit_to(entry.index)
                    .unwrap_or_else(|error| panic!("commit raft value {index}: {error}"));
            }
        }

        let first = cache
            .current_get(node_id.clone(), "/apps/service/a")
            .expect("seed raft view cache")
            .expect("raft value");
        assert_eq!(first.value, b"a");

        let mut stale_store = InProcessKvStore::new();
        stale_store
            .put("/apps/service/stale", b"stale".to_vec(), None)
            .expect("seed stale cache");
        cache
            .write_snapshot(&stale_store.snapshot())
            .expect("write stale materialized cache");
        let _held_node = cache
            .open_raft_node(node_id.clone())
            .expect("hold raft lock");

        assert_eq!(
            cache
                .current_get(node_id.clone(), "/apps/service/b")
                .expect("cached point read")
                .expect("raft value")
                .value,
            b"b"
        );
        assert!(
            cache
                .current_get(node_id.clone(), "/apps/service/stale")
                .expect("cached point read")
                .is_none(),
            "point reads must not fall back to stale materialized snapshots when a valid raft view is cached"
        );
        assert_eq!(
            cache
                .current_range_prefix(node_id, "/apps/service/")
                .expect("cached prefix read")
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["/apps/service/a", "/apps/service/b"]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_raft_view_cache_invalidates_after_wal_change() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-raft-view-invalidate-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id =
            NodeId::from_legacy_named("node_local_cache_raft_view_invalidate").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("open raft");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft/first".to_string(),
                        value: b"first".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
        }
        let first_view = cache
            .current_store(node_id.clone())
            .expect("seed raft view cache");
        assert!(first_view.get("/raft/first").is_some());
        assert!(first_view.get("/raft/second").is_none());

        {
            let mut node = cache.open_raft_node(node_id.clone()).expect("reopen raft");
            let second = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/raft/second".to_string(),
                        value: b"second".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append second");
            node.commit_to(second.index).expect("commit second");
        }

        let refreshed = cache
            .current_store(node_id)
            .expect("read after wal changed");
        assert!(refreshed.get("/raft/first").is_some());
        assert!(refreshed.get("/raft/second").is_some());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_node_open_waits_for_transient_lock_holder() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-raft-lock-wait-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_raft_lock_wait").expect("node");
        let paths = KvWalPaths::new(root.join("raft"));
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let holder_node_id = node_id.clone();
        let holder_paths = paths.clone();
        let holder = std::thread::spawn(move || {
            let _node =
                DurableRaftKvNode::open(holder_node_id, holder_paths).expect("hold raft node");
            held_tx.send(()).expect("notify lock held");
            std::thread::sleep(Duration::from_millis(100));
        });
        held_rx.recv().expect("wait for holder");

        let node = DurableRaftKvNode::open(node_id, paths).expect("wait for raft node lock");
        assert_eq!(node.last_log_index(), 0);
        holder.join().expect("join holder");
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn raft_node_lock_retry_delay_backs_off_and_caps() {
        assert_eq!(raft_node_lock_retry_delay(0), RAFT_NODE_LOCK_RETRY_INITIAL);
        assert_eq!(
            raft_node_lock_retry_delay(1),
            RAFT_NODE_LOCK_RETRY_INITIAL * 2
        );
        assert_eq!(
            raft_node_lock_retry_delay(4),
            RAFT_NODE_LOCK_RETRY_INITIAL * 16
        );
        assert_eq!(raft_node_lock_retry_delay(5), RAFT_NODE_LOCK_RETRY_MAX);
        assert_eq!(raft_node_lock_retry_delay(32), RAFT_NODE_LOCK_RETRY_MAX);
    }

    #[test]
    fn local_cache_reads_entries_without_exposing_snapshot_files() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-read-interface-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id = NodeId::from_legacy_named("node_local_cache_read").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut store = InProcessKvStore::new();
        store
            .put_serde("/dfs/services/content/a", &"node-a", None)
            .expect("put a");
        store
            .put_serde("/dfs/services/content/b", &"node-b", None)
            .expect("put b");
        cache
            .write_snapshot(&store.snapshot())
            .expect("write cache snapshot");

        assert_eq!(
            cache
                .current_get_serde::<String>(node_id.clone(), "/dfs/services/content/a")
                .expect("read value"),
            Some("node-a".to_string())
        );
        assert_eq!(
            cache
                .current_range_prefix_serde::<String>(node_id, "/dfs/services/content/")
                .expect("range values"),
            vec!["node-a".to_string(), "node-b".to_string()]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_cache_typed_prefix_reads_are_bounded() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-local-cache-typed-range-limit-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let node_id =
            NodeId::from_legacy_named("node_local_cache_typed_range_limit").expect("node");
        let cache = KvLocalCache::new(root.join("cache.sobj"), root.join("raft"));
        let mut store = InProcessKvStore::new();
        for key in [
            "/dfs/services/content/a",
            "/dfs/services/content/b",
            "/dfs/services/content/c",
        ] {
            store.put_serde(key, &key.to_string(), None).expect("put");
        }
        cache
            .write_snapshot(&store.snapshot())
            .expect("write cache snapshot");

        let error = cache
            .current_range_prefix_serde_limited_by_payload_bytes::<String>(
                node_id.clone(),
                "/dfs/services/content/",
                2,
                usize::MAX,
            )
            .expect_err("entry limit");
        assert_eq!(
            error,
            KvError::RangeLimit {
                prefix: "/dfs/services/content/".to_string(),
                max_entries: 2,
            }
        );

        let error = cache
            .current_range_prefix_serde_limited_by_payload_bytes::<String>(
                node_id,
                "/dfs/services/content/",
                3,
                1,
            )
            .expect_err("payload limit");
        assert!(matches!(
            error,
            KvError::RangePayloadLimit {
                ref prefix,
                max_bytes: 1,
                ..
            } if prefix == "/dfs/services/content/"
        ));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn compacted_revision_rejects_old_watches() {
        let mut store = InProcessKvStore::new();
        store.put("/a", b"1".to_vec(), None).expect("put");
        store.compact(KvRevision(1)).expect("compact");

        let error = store
            .watch_prefix("/", KvRevision(1))
            .expect_err("compacted watch");
        assert!(matches!(error, KvError::Compacted { .. }));
    }

    #[test]
    fn raft_cluster_replicates_committed_writes_to_online_voters() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");

        cluster.put("/cluster/meta", b"ok".to_vec()).expect("put");

        assert_eq!(
            cluster
                .store(&node_b)
                .expect("store")
                .get("/cluster/meta")
                .expect("entry")
                .value,
            b"ok"
        );
        assert_eq!(
            cluster.store(&node_c).expect("store").revision(),
            KvRevision(1)
        );
    }

    #[test]
    fn raft_cluster_rejects_writes_without_quorum() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster.set_node_online(&node_b, false);
        cluster.set_node_online(&node_c, false);

        let error = cluster
            .put("/cluster/meta", b"no".to_vec())
            .expect_err("no quorum");

        assert!(matches!(error, KvError::QuorumUnavailable { .. }));
    }

    #[derive(Debug)]
    enum QuorumCommitTestError {
        Kv(KvError),
        ReadOnly(String),
    }

    impl std::fmt::Display for QuorumCommitTestError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Kv(error) => write!(formatter, "{error}"),
                Self::ReadOnly(detail) => write!(formatter, "cluster is read-only: {detail}"),
            }
        }
    }

    impl From<KvError> for QuorumCommitTestError {
        fn from(error: KvError) -> Self {
            Self::Kv(error)
        }
    }

    fn quorum_commit_test_state(local: &NodeId, remote: &NodeId) -> ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_kv_quorum_driver").expect("cluster"),
            "kv quorum driver",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_capability =
            sinomo_core::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY.to_string())
                .expect("main daemon capability");
        state.members[0].role = ClusterMemberRole::Voter;
        state.members[0].state = ClusterMemberState::Voter;
        state.members[0].capabilities = vec![main_capability.clone()];
        state.members[0].allowed_capabilities = vec![main_capability.clone()];
        let mut remote_member = state.members[0].clone();
        remote_member.node_id = remote.clone();
        remote_member.display_name = "remote".to_string();
        remote_member.node_public_key = "pk-remote".to_string();
        remote_member.joined_at = now;
        remote_member.updated_at = now;
        state.members.push(remote_member);
        state
    }

    #[derive(Default)]
    struct QuorumCommitTestHost {
        targets: Vec<NodeId>,
        accepting_targets: BTreeSet<NodeId>,
        appended: Option<KvLogEntry>,
        committed_indices: Vec<u64>,
        discarded: bool,
        replicated_targets: Vec<NodeId>,
        statuses: Vec<(NodeId, ClusterKvRaftTargetReplicationStatus)>,
        fanout_targets: Vec<NodeId>,
    }

    impl ClusterKvRaftCommandStorage for QuorumCommitTestHost {
        type Error = QuorumCommitTestError;

        fn append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self,
            command: KvCommand,
        ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error> {
            let entry = KvLogEntry {
                index: 7,
                term: 1,
                command,
                revision: KvRevision(1),
            };
            self.appended = Some(entry.clone());
            Ok(ClusterKvRaftUncommittedAppend::Appended(entry))
        }

        fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self,
        ) -> Result<(), Self::Error> {
            self.discarded = true;
            Ok(())
        }

        fn commit_cluster_raft_index_with_events(
            &mut self,
            index: u64,
        ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
            self.committed_indices.push(index);
            let mut store = InProcessKvStore::new();
            if let Some(KvLogEntry {
                command:
                    KvCommand::Op(KvOp::Put {
                        key,
                        value,
                        lease_id,
                    }),
                ..
            }) = self.appended.clone()
            {
                store.put(key, value, lease_id)?;
            }
            Ok(ClusterKvRaftCommitResult {
                snapshot: store.snapshot(),
                txn_response: None,
                events: vec![test_kv_put_event(1, "/kv/quorum/fanout", b"ok")],
            })
        }
    }

    impl ClusterKvRaftQuorumCommitHost for QuorumCommitTestHost {
        type Target = NodeId;

        fn remote_voter_targets(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<Vec<Self::Target>, Self::Error> {
            Ok(self.targets.clone())
        }

        fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
            target
        }

        fn replicate_cluster_raft_to_target(
            &mut self,
            state: &ClusterBootstrapState,
            target: &Self::Target,
            _timeout: Duration,
        ) -> Result<ClusterRaftAppendResponse, Self::Error> {
            self.replicated_targets.push(target.clone());
            Ok(ClusterRaftAppendResponse {
                accepted: self.accepting_targets.contains(target),
                detail: "test append".to_string(),
                state: state.clone(),
                append: RaftAppendEntriesResponse {
                    term: 1,
                    success: self.accepting_targets.contains(target),
                    match_index: 7,
                },
            })
        }

        fn record_target_replication_status(
            &mut self,
            _state: &ClusterBootstrapState,
            target: &Self::Target,
            status: ClusterKvRaftTargetReplicationStatus,
            _latency_ms: Option<u64>,
        ) -> Result<(), Self::Error> {
            self.statuses.push((target.clone(), status));
            Ok(())
        }

        fn fan_out_committed_cluster_raft_command(
            &mut self,
            _state: ClusterBootstrapState,
            _events: Vec<KvEvent>,
            targets: Vec<Self::Target>,
        ) -> Result<(), Self::Error> {
            self.fanout_targets = targets;
            Ok(())
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    impl ClusterKvRaftQuorumCommitAsyncHost for QuorumCommitTestHost {
        type Error = QuorumCommitTestError;
        type Target = NodeId;

        fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
            &'a mut self,
            command: KvCommand,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftUncommittedAppend, Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
                    self, command,
                )
            })
        }

        fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
            &'a mut self,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
                    self,
                )
            })
        }

        fn commit_cluster_raft_index_with_events_async<'a>(
            &'a mut self,
            index: u64,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error> {
            Box::pin(async move {
                ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(self, index)
            })
        }

        fn remote_voter_targets_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
        ) -> ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, Self::Error> {
            Box::pin(async move { Ok(self.targets.clone()) })
        }

        fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
            target
        }

        fn replicate_cluster_raft_to_target_async<'a>(
            &'a mut self,
            state: &'a ClusterBootstrapState,
            target: &'a Self::Target,
            _timeout: Duration,
        ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
            Box::pin(async move {
                self.replicated_targets.push(target.clone());
                Ok(ClusterRaftAppendResponse {
                    accepted: self.accepting_targets.contains(target),
                    detail: "test append".to_string(),
                    state: state.clone(),
                    append: RaftAppendEntriesResponse {
                        term: 1,
                        success: self.accepting_targets.contains(target),
                        match_index: 7,
                    },
                })
            })
        }

        fn record_target_replication_status_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            target: &'a Self::Target,
            status: ClusterKvRaftTargetReplicationStatus,
            _latency_ms: Option<u64>,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.statuses.push((target.clone(), status));
                Ok(())
            })
        }

        fn fan_out_committed_cluster_raft_command_async<'a>(
            &'a mut self,
            _state: ClusterBootstrapState,
            _events: Vec<KvEvent>,
            targets: Vec<Self::Target>,
        ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.fanout_targets = targets;
                Ok(())
            })
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    #[derive(Default)]
    struct TxnQuorumDriverTestHost {
        local_leader: bool,
        forwarded: Option<ClusterKvTxnCommitResult>,
        committed_txns: usize,
        elected: bool,
        reachable_voters: usize,
    }

    impl ClusterKvTxnQuorumHost for TxnQuorumDriverTestHost {
        type Error = QuorumCommitTestError;

        fn local_is_raft_leader(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<bool, Self::Error> {
            Ok(self.local_leader)
        }

        fn forward_cluster_kv_txn_to_voter(
            &mut self,
            _state: &ClusterBootstrapState,
            _txn: &KvTxn,
        ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error> {
            Ok(self.forwarded.clone())
        }

        fn elect_cluster_raft_leader(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<bool, Self::Error> {
            self.elected = true;
            self.local_leader = true;
            Ok(true)
        }

        fn reachable_voter_count(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<usize, Self::Error> {
            Ok(self.reachable_voters)
        }

        fn semantic_txn_no_effect_response(
            &mut self,
            _txn: &KvTxn,
        ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error> {
            Ok(None)
        }

        fn commit_cluster_raft_txn_with_quorum(
            &mut self,
            _state: &mut ClusterBootstrapState,
            txn: KvTxn,
        ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error> {
            self.committed_txns = self.committed_txns.saturating_add(1);
            let mut store = InProcessKvStore::new();
            let txn_response = store.txn(txn)?;
            Ok(ClusterKvRaftQuorumCommitResult {
                snapshot: store.snapshot(),
                txn_response: Some(txn_response),
            })
        }

        fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
            QuorumCommitTestError::ReadOnly(detail)
        }
    }

    #[test]
    fn kv_txn_quorum_driver_commits_when_local_voter_is_leader() {
        let local = NodeId::from_legacy_named("node_txn_driver_local").expect("local");
        let remote = NodeId::from_legacy_named("node_txn_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = TxnQuorumDriverTestHost {
            local_leader: true,
            reachable_voters: 2,
            ..TxnQuorumDriverTestHost::default()
        };

        let result = commit_cluster_kv_txn_with_quorum_with_host(
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/local".to_string(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            ClusterKvTxnQuorumDriverOptions {
                allow_forwarding: false,
            },
            &mut host,
        )
        .expect("commit local leader txn");

        assert_eq!(host.committed_txns, 1);
        assert!(!host.elected);
        assert_eq!(
            result
                .snapshot
                .get("/kv/txn-driver/local")
                .expect("committed key")
                .value,
            b"committed".to_vec()
        );
        assert_eq!(
            result.txn_response.expect("txn response").revision,
            result.snapshot.revision
        );
    }

    #[test]
    fn kv_txn_quorum_driver_forwards_when_local_node_is_not_voter() {
        let local = NodeId::from_legacy_named("node_txn_driver_learner").expect("local");
        let remote = NodeId::from_legacy_named("node_txn_driver_voter").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        state.members[0].role = ClusterMemberRole::Learner;
        state.members[0].state = ClusterMemberState::Learner;
        let mut store = InProcessKvStore::new();
        let txn_response = store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/forwarded".to_string(),
                    value: b"forwarded".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("seed forwarded response");
        let forwarded = ClusterKvTxnCommitResult {
            snapshot: store.snapshot(),
            txn_response: Some(txn_response),
        };
        let mut host = TxnQuorumDriverTestHost {
            forwarded: Some(forwarded.clone()),
            reachable_voters: 1,
            ..TxnQuorumDriverTestHost::default()
        };

        let result = commit_cluster_kv_txn_with_quorum_with_host(
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/kv/txn-driver/forwarded".to_string(),
                    value: b"forwarded".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
            ClusterKvTxnQuorumDriverOptions {
                allow_forwarding: true,
            },
            &mut host,
        )
        .expect("forward learner txn");

        assert_eq!(result, forwarded);
        assert_eq!(host.committed_txns, 0);
        assert!(!host.elected);
    }

    struct AppendReplicationTestHost {
        last_index: u64,
        requested_next_indices: Vec<u64>,
        responses: VecDeque<ClusterRaftAppendResponse>,
        sent_count: usize,
    }

    impl ClusterKvRaftAppendReplicationHost for AppendReplicationTestHost {
        type Error = QuorumCommitTestError;
        type Target = NodeId;

        fn last_local_raft_log_index(
            &mut self,
            _state: &ClusterBootstrapState,
        ) -> Result<u64, Self::Error> {
            Ok(self.last_index)
        }

        fn build_raft_append_request(
            &mut self,
            state: &ClusterBootstrapState,
            next_index: u64,
        ) -> Result<ClusterRaftAppendRequest, Self::Error> {
            self.requested_next_indices.push(next_index);
            Ok(ClusterRaftAppendRequest {
                state: state.clone(),
                append: RaftAppendEntriesRequest {
                    term: 1,
                    leader_id: state.local_node_id.clone(),
                    prev_log_index: next_index.saturating_sub(1),
                    prev_log_term: 1,
                    entries: Vec::new(),
                    leader_commit: next_index.saturating_sub(1),
                },
                snapshot: Some(RaftSnapshot {
                    last_included_index: next_index.saturating_sub(1),
                    last_included_term: 1,
                    kv: test_kv_snapshot_with_events(Vec::new()),
                }),
            })
        }

        fn append_attempt_timeout(
            &mut self,
            _target: &Self::Target,
            remaining: Duration,
            _request: &ClusterRaftAppendRequest,
        ) -> Duration {
            remaining.min(Duration::from_secs(1))
        }

        fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
            self.sent_count = self.sent_count.saturating_add(1);
            Ok(())
        }

        fn send_raft_append(
            &mut self,
            _target: &Self::Target,
            _request: &ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> Result<ClusterRaftAppendResponse, Self::Error> {
            self.responses
                .pop_front()
                .ok_or_else(|| QuorumCommitTestError::ReadOnly("missing append response".into()))
        }
    }

    fn test_cluster_raft_append_response(
        state: &ClusterBootstrapState,
        accepted: bool,
        match_index: u64,
    ) -> ClusterRaftAppendResponse {
        ClusterRaftAppendResponse {
            accepted,
            detail: "test append".to_string(),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 1,
                success: accepted,
                match_index,
            },
        }
    }

    #[test]
    fn cluster_kv_raft_append_replication_retries_rejected_next_index_in_kv() {
        let local = NodeId::from_legacy_named("node_kv_append_replication_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_append_replication_remote").expect("remote");
        let state = quorum_commit_test_state(&local, &remote);
        let watch_event = test_kv_put_event(1, "/kv/append/watch", b"ok");
        let mut host = AppendReplicationTestHost {
            last_index: 5,
            requested_next_indices: Vec::new(),
            responses: VecDeque::from([
                test_cluster_raft_append_response(&state, false, 2),
                test_cluster_raft_append_response(&state, true, 5),
            ]),
            sent_count: 0,
        };

        let outcome = replicate_cluster_kv_raft_to_target_with_host(
            &state,
            &remote,
            &[watch_event],
            ClusterKvRaftAppendReplicationOptions {
                max_attempts: 3,
                timeout: Duration::from_secs(5),
            },
            &mut host,
        )
        .expect("append replication");

        assert!(outcome.response.accepted);
        assert!(outcome.watch_event_fanout_needed);
        assert_eq!(host.requested_next_indices, vec![6, 3]);
        assert_eq!(host.sent_count, 2);
    }

    #[test]
    fn cluster_kv_raft_quorum_driver_commits_after_remote_voter_ack() {
        let local = NodeId::from_legacy_named("node_kv_quorum_driver_local").expect("local");
        let remote = NodeId::from_legacy_named("node_kv_quorum_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            accepting_targets: [remote.clone()].into_iter().collect(),
            ..QuorumCommitTestHost::default()
        };

        let result = commit_cluster_kv_raft_command_with_quorum(
            &mut state,
            KvCommand::Op(KvOp::Put {
                key: "/kv/quorum/committed".to_string(),
                value: b"ok".to_vec(),
                lease_id: None,
            }),
            ClusterKvRaftQuorumCommitOptions {
                timeout: Duration::from_secs(1),
            },
            &mut host,
        )
        .expect("quorum commit");

        assert_eq!(
            result
                .snapshot
                .get("/kv/quorum/committed")
                .expect("committed value")
                .value,
            b"ok".to_vec()
        );
        assert_eq!(host.committed_indices, vec![7]);
        assert_eq!(host.replicated_targets, vec![remote.clone()]);
        assert_eq!(
            host.statuses,
            vec![(
                remote.clone(),
                ClusterKvRaftTargetReplicationStatus::Accepted
            )]
        );
        assert_eq!(host.fanout_targets, vec![remote]);
        assert!(!host.discarded);
    }

    #[test]
    fn cluster_kv_raft_quorum_async_driver_commits_after_remote_voter_ack() {
        let local = NodeId::from_legacy_named("node_kv_quorum_async_driver_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_quorum_async_driver_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            accepting_targets: [remote.clone()].into_iter().collect(),
            ..QuorumCommitTestHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let result = runtime
            .block_on(commit_cluster_kv_raft_command_with_quorum_async(
                &mut state,
                KvCommand::Op(KvOp::Put {
                    key: "/kv/quorum/async-committed".to_string(),
                    value: b"ok".to_vec(),
                    lease_id: None,
                }),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: Duration::from_secs(1),
                },
                &mut host,
            ))
            .expect("async quorum commit");

        assert_eq!(
            result
                .snapshot
                .get("/kv/quorum/async-committed")
                .expect("committed value")
                .value,
            b"ok".to_vec()
        );
        assert_eq!(host.committed_indices, vec![7]);
        assert_eq!(host.replicated_targets, vec![remote.clone()]);
        assert_eq!(
            host.statuses,
            vec![(
                remote.clone(),
                ClusterKvRaftTargetReplicationStatus::Accepted
            )]
        );
        assert_eq!(host.fanout_targets, vec![remote]);
        assert!(!host.discarded);
    }

    #[test]
    fn cluster_kv_raft_quorum_driver_discards_uncommitted_entry_without_quorum() {
        let local =
            NodeId::from_legacy_named("node_kv_quorum_driver_no_quorum_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_kv_quorum_driver_no_quorum_remote").expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            ..QuorumCommitTestHost::default()
        };

        let error = commit_cluster_kv_raft_command_with_quorum(
            &mut state,
            KvCommand::Op(KvOp::Put {
                key: "/kv/quorum/rejected".to_string(),
                value: b"no".to_vec(),
                lease_id: None,
            }),
            ClusterKvRaftQuorumCommitOptions {
                timeout: Duration::from_secs(1),
            },
            &mut host,
        )
        .expect_err("no quorum");

        assert!(matches!(error, QuorumCommitTestError::ReadOnly(_)));
        assert!(host.discarded);
        assert!(host.committed_indices.is_empty());
        assert_eq!(
            host.statuses,
            vec![(remote, ClusterKvRaftTargetReplicationStatus::Rejected)]
        );
    }

    #[test]
    fn cluster_kv_raft_quorum_async_driver_discards_uncommitted_entry_without_quorum() {
        let local = NodeId::from_legacy_named("node_kv_quorum_async_driver_no_quorum_local")
            .expect("local");
        let remote = NodeId::from_legacy_named("node_kv_quorum_async_driver_no_quorum_remote")
            .expect("remote");
        let mut state = quorum_commit_test_state(&local, &remote);
        let mut host = QuorumCommitTestHost {
            targets: vec![remote.clone()],
            ..QuorumCommitTestHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let error = runtime
            .block_on(commit_cluster_kv_raft_command_with_quorum_async(
                &mut state,
                KvCommand::Op(KvOp::Put {
                    key: "/kv/quorum/async-rejected".to_string(),
                    value: b"no".to_vec(),
                    lease_id: None,
                }),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: Duration::from_secs(1),
                },
                &mut host,
            ))
            .expect_err("no quorum");

        assert!(matches!(error, QuorumCommitTestError::ReadOnly(_)));
        assert!(host.discarded);
        assert!(host.committed_indices.is_empty());
        assert_eq!(
            host.statuses,
            vec![(remote, ClusterKvRaftTargetReplicationStatus::Rejected)]
        );
    }

    #[test]
    fn learner_can_catch_up_from_snapshot() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let learner = NodeId::from_legacy_named("node_learner").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone()]).expect("cluster");
        cluster.put("/policy/a", b"allow".to_vec()).expect("put");
        let snapshot = cluster.store(&node_a).expect("leader store").snapshot();

        cluster
            .add_learner_from_snapshot(learner.clone(), snapshot)
            .expect("learner");

        assert_eq!(
            cluster
                .store(&learner)
                .expect("learner store")
                .get("/policy/a")
                .expect("entry")
                .value,
            b"allow"
        );
    }

    #[test]
    fn raft_cluster_catches_up_partitioned_voter_from_committed_log() {
        let node_a = NodeId::from_legacy_named("node_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");

        cluster.set_node_online(&node_c, false);
        cluster.put("/config/a", b"one".to_vec()).expect("put");
        assert!(
            cluster
                .store(&node_c)
                .expect("partitioned store")
                .get("/config/a")
                .is_none()
        );

        cluster.set_node_online(&node_c, true);

        assert_eq!(
            cluster
                .store(&node_c)
                .expect("caught up store")
                .get("/config/a")
                .expect("entry")
                .value,
            b"one"
        );
        assert_eq!(
            cluster
                .hard_state(&node_c)
                .expect("hard state")
                .commit_index,
            cluster.commit_index()
        );
    }

    #[test]
    fn raft_cluster_partition_blocks_isolated_leader_and_recovers_after_majority_leader_change() {
        let node_a = NodeId::from_legacy_named("node_partition_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_partition_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_partition_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster
            .put("/config/epoch", b"one".to_vec())
            .expect("initial put");

        cluster.set_node_online(&node_a, false);
        let isolated_write = cluster.put("/config/minority", b"blocked".to_vec());
        assert!(matches!(
            isolated_write,
            Err(KvError::LeaderOffline { leader_id }) if leader_id == node_a
        ));

        cluster
            .transfer_leader(node_b.clone())
            .expect("majority leader transfer");
        cluster
            .put("/config/majority", b"committed".to_vec())
            .expect("majority write");
        assert!(
            cluster
                .store(&node_a)
                .expect("isolated old leader")
                .get("/config/majority")
                .is_none()
        );

        cluster.set_node_online(&node_a, true);

        assert_eq!(
            cluster
                .store(&node_a)
                .expect("healed old leader")
                .get("/config/majority")
                .expect("caught up majority write")
                .value,
            b"committed"
        );
        assert_eq!(
            cluster
                .hard_state(&node_a)
                .expect("old leader hard state")
                .commit_index,
            cluster.commit_index()
        );
    }

    #[test]
    fn raft_cluster_autonomous_election_replaces_lost_leader_with_majority_quorum() {
        let node_a = NodeId::from_legacy_named("node_elect_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_elect_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_elect_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster
            .put("/config/a", b"one".to_vec())
            .expect("initial write");

        cluster.set_node_online(&node_a, false);

        assert!(
            cluster
                .elect_leader(node_b.clone())
                .expect("elect majority leader")
        );
        assert_eq!(cluster.leader_id(), &node_b);
        assert_eq!(
            cluster
                .hard_state(&node_b)
                .expect("new leader hard state")
                .leader_id
                .as_ref(),
            Some(&node_b)
        );
        cluster
            .put("/config/b", b"two".to_vec())
            .expect("write through elected leader");
        assert!(
            cluster
                .store(&node_a)
                .expect("offline old leader")
                .get("/config/b")
                .is_none()
        );

        cluster.set_node_online(&node_a, true);

        assert_eq!(
            cluster
                .store(&node_a)
                .expect("healed old leader")
                .get("/config/b")
                .expect("caught up write")
                .value,
            b"two"
        );
    }

    #[test]
    fn raft_cluster_autonomous_election_fails_without_majority() {
        let node_a = NodeId::from_legacy_named("node_elect_fail_a").expect("node");
        let node_b = NodeId::from_legacy_named("node_elect_fail_b").expect("node");
        let node_c = NodeId::from_legacy_named("node_elect_fail_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster.set_node_online(&node_a, false);
        cluster.set_node_online(&node_c, false);

        assert!(
            !cluster
                .elect_leader(node_b.clone())
                .expect("minority election")
        );
        assert_ne!(cluster.leader_id(), &node_b);
        assert!(matches!(
            cluster.put("/config/blocked", b"blocked".to_vec()),
            Err(KvError::LeaderOffline { .. })
        ));
    }

    #[test]
    fn cluster_kv_raft_leader_election_driver_sets_leader_after_remote_quorum() {
        struct RecordingElectionHost {
            sent_votes: Vec<(usize, NodeId, u64)>,
            observed_terms: Vec<u64>,
            leader_terms: Vec<u64>,
        }

        impl ClusterKvRaftLeaderElectionHost for RecordingElectionHost {
            type Error = KvError;

            fn local_vote_metadata(
                &mut self,
            ) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
                Ok(ClusterKvRaftLocalVoteMetadata {
                    current_term: 1,
                    last_log_index: 7,
                    last_log_term: 1,
                })
            }

            fn apply_local_vote(
                &mut self,
                vote: RaftRequestVoteRequest,
            ) -> Result<RaftRequestVoteResponse, Self::Error> {
                Ok(RaftRequestVoteResponse {
                    term: vote.term,
                    vote_granted: true,
                })
            }

            fn request_remote_vote(
                &mut self,
                target_index: usize,
                voter_id: &NodeId,
                vote: &RaftRequestVoteRequest,
            ) -> Result<RaftRequestVoteResponse, Self::Error> {
                self.sent_votes
                    .push((target_index, voter_id.clone(), vote.term));
                Ok(RaftRequestVoteResponse {
                    term: vote.term,
                    vote_granted: true,
                })
            }

            fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
                self.observed_terms.push(term);
                Ok(())
            }

            fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
                self.leader_terms.push(term);
                Ok(())
            }
        }

        let local_node_id = NodeId::from_legacy_named("node_kv_election_local").expect("node");
        let peer_a = NodeId::from_legacy_named("node_kv_election_peer_a").expect("node");
        let peer_b = NodeId::from_legacy_named("node_kv_election_peer_b").expect("node");
        let mut host = RecordingElectionHost {
            sent_votes: Vec::new(),
            observed_terms: Vec::new(),
            leader_terms: Vec::new(),
        };

        let elected = elect_cluster_kv_raft_leader_with_host(
            &local_node_id,
            vec![local_node_id.clone(), peer_a.clone(), peer_b.clone()],
            vec![peer_a.clone(), peer_b],
            ClusterKvRaftLeaderElectionOptions { max_attempts: 1 },
            &mut host,
        )
        .expect("election");

        assert!(elected);
        let expected_term = 1 + CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP;
        assert_eq!(host.sent_votes, vec![(0, peer_a, expected_term)]);
        assert!(host.observed_terms.is_empty());
        assert_eq!(host.leader_terms, vec![expected_term]);
    }

    #[test]
    fn raft_votes_require_current_candidate_log() {
        let voter = NodeId::from_legacy_named("node_voter").expect("node");
        let stale = NodeId::from_legacy_named("node_stale").expect("node");
        let current = NodeId::from_legacy_named("node_current").expect("node");
        let mut cluster = InProcessRaftKvCluster::new(vec![voter.clone()]).expect("cluster");
        cluster.put("/config/a", b"one".to_vec()).expect("put");

        let rejected = cluster
            .request_vote(
                &voter,
                RaftRequestVoteRequest {
                    term: 2,
                    candidate_id: stale,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("vote response");
        assert!(!rejected.vote_granted);

        let granted = cluster
            .request_vote(
                &voter,
                RaftRequestVoteRequest {
                    term: 3,
                    candidate_id: current,
                    last_log_index: 1,
                    last_log_term: 1,
                },
            )
            .expect("vote response");
        assert!(granted.vote_granted);
    }

    #[test]
    fn durable_raft_node_replays_wal_and_snapshot() {
        let root = temp_raft_root("sinomo-kv-wal");
        let node_id = NodeId::from_legacy_named("node_wal").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/a".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
            let snapshot = node.save_snapshot().expect("snapshot");
            assert_eq!(snapshot.kv.revision, KvRevision(1));
            let second = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/b".to_string(),
                        value: b"two".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append second");
            node.commit_to(second.index).expect("commit second");
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");

        assert_eq!(node.store().revision(), KvRevision(2));
        assert_eq!(node.store().get("/config/a").expect("a").value, b"one");
        assert_eq!(node.store().get("/config/b").expect("b").value, b"two");
        assert_eq!(node.hard_state().commit_index, 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_save_snapshot_skips_identical_wal_tail_rewrite() {
        let root = temp_raft_root("sinomo-kv-snapshot-no-wal-rewrite");
        let node_id = NodeId::from_legacy_named("node_snapshot_no_wal_rewrite").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/snapshot/no-rewrite".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
            node.save_snapshot().expect("initial snapshot");

            let before = fs::metadata(&paths.wal).expect("wal metadata before");
            let snapshot_before = fs::metadata(&paths.snapshot).expect("snapshot metadata before");
            thread::sleep(Duration::from_millis(20));
            node.save_snapshot().expect("unchanged snapshot");
            let after = fs::metadata(&paths.wal).expect("wal metadata after");
            let snapshot_after = fs::metadata(&paths.snapshot).expect("snapshot metadata after");

            assert_eq!(before.len(), after.len());
            assert_eq!(file_ino(&before), file_ino(&after));
            assert_eq!(
                before.modified().ok(),
                after.modified().ok(),
                "unchanged snapshot compaction should not rewrite an identical WAL tail"
            );
            assert_eq!(snapshot_before.len(), snapshot_after.len());
            assert_eq!(file_ino(&snapshot_before), file_ino(&snapshot_after));
            assert_eq!(
                snapshot_before.modified().ok(),
                snapshot_after.modified().ok(),
                "unchanged snapshot save should not rewrite an identical snapshot object"
            );
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_compaction_policy_skips_idle_retained_tail() {
        let root = temp_raft_root("sinomo-kv-compaction-policy-idle");
        let node_id = NodeId::from_legacy_named("node_compaction_policy_idle").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        let entry = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/compaction/idle".to_string(),
                    value: b"value".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append");
        node.commit_to(entry.index).expect("commit");
        node.save_snapshot().expect("initial snapshot");
        let snapshot_before = fs::metadata(&paths.snapshot).expect("snapshot before");
        thread::sleep(Duration::from_millis(20));

        let compacted = compact_cluster_kv_raft_node_if_needed(
            &mut node,
            snapshot_before.len(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        )
        .expect("compaction policy");

        let snapshot_after = fs::metadata(&paths.snapshot).expect("snapshot after");
        assert!(!compacted);
        assert_eq!(snapshot_before.len(), snapshot_after.len());
        assert_eq!(file_ino(&snapshot_before), file_ino(&snapshot_after));
        assert_eq!(
            snapshot_before.modified().ok(),
            snapshot_after.modified().ok()
        );
        drop(node);
        std::fs::remove_dir_all(root).ok();
    }

    struct ClusterKvRaftSnapshotByteLimitGuard {
        previous: u64,
    }

    impl ClusterKvRaftSnapshotByteLimitGuard {
        fn install(limit: u64) -> Self {
            Self {
                previous: set_cluster_kv_raft_snapshot_byte_limit_for_test(limit),
            }
        }
    }

    impl Drop for ClusterKvRaftSnapshotByteLimitGuard {
        fn drop(&mut self) {
            set_cluster_kv_raft_snapshot_byte_limit_for_test(self.previous);
        }
    }

    #[test]
    fn cluster_kv_raft_store_oversized_snapshot_with_compacted_events_is_idle_on_open() {
        let _limit_guard = ClusterKvRaftSnapshotByteLimitGuard::install(1);
        let root = temp_raft_root("sinomo-kv-raft-oversized-snapshot-idle");
        let node_id = NodeId::from_legacy_named("node_oversized_snapshot_idle").expect("node");
        let paths = ClusterKvPaths::new(root.clone());
        let wal_paths = paths.raft_wal_paths(&node_id);

        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/oversized/snapshot/value".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
            node.compact_event_history_to(MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
            node.save_snapshot().expect("snapshot");
            assert!(
                node.store().event_history_len() <= MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
                "fixture should have no compactable event-history tail"
            );
        }

        let before = fs::metadata(&wal_paths.snapshot).expect("snapshot before");
        assert!(
            before.len() > 1,
            "test threshold should make this ordinary snapshot look oversized"
        );
        thread::sleep(Duration::from_millis(20));
        let store = ClusterKvRaftStore::new(paths, default_cluster_kv_raft_compaction_policy());
        let reopened = store.open_node(&node_id).expect("open compacted snapshot");
        assert_eq!(reopened.snapshot_index(), 1);
        drop(reopened);
        let after = fs::metadata(&wal_paths.snapshot).expect("snapshot after");

        assert_eq!(before.len(), after.len());
        assert_eq!(
            before.modified().ok(),
            after.modified().ok(),
            "oversized snapshots without compactable event history should not be re-saved on idle open"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_compaction_policy_compacts_after_progress_watermark() {
        let root = temp_raft_root("sinomo-kv-compaction-policy-progress");
        let node_id = NodeId::from_legacy_named("node_compaction_policy_progress").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths).expect("open");
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES
            .saturating_mul(2)
            .saturating_add(1);
        for index in 1..=command_count {
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: format!("/compaction/progress/{index}"),
                        value: index.to_be_bytes().to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
        }

        assert!(cluster_kv_raft_node_should_compact(
            &node,
            0,
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        ));
        assert!(
            compact_cluster_kv_raft_node_if_needed(
                &mut node,
                0,
                ClusterKvRaftCompactionPolicy {
                    max_snapshot_bytes: u64::MAX,
                    max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
                },
            )
            .expect("compact")
        );
        assert_eq!(node.snapshot_index(), command_count);
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_open_node_async_compacts_after_progress_watermark() {
        let root = temp_raft_root("sinomo-kv-raft-store-async-open-compacts");
        let node_id =
            NodeId::from_legacy_named("node_raft_store_async_open_compacts").expect("node");
        let paths = ClusterKvPaths::new(root.clone());
        let wal_paths = paths.raft_wal_paths(&node_id);
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES
            .saturating_mul(2)
            .saturating_add(1);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()).expect("open");
            for index in 1..=command_count {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/async-open/compaction/{index}"),
                            value: index.to_be_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append");
                node.commit_to(entry.index).expect("commit");
            }
            assert_eq!(node.snapshot_index(), 0);
        }

        let store = ClusterKvRaftStore::new(
            paths,
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let node = runtime
            .block_on(store.open_node_async(&node_id))
            .expect("open async");

        assert_eq!(node.snapshot_index(), command_count);
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_applies_vote_and_leader_updates() {
        let root = temp_raft_root("sinomo-kv-raft-store-vote-leader");
        let node_id = NodeId::from_legacy_named("node_raft_store_vote_leader").expect("node");
        let candidate_id =
            NodeId::from_legacy_named("node_raft_store_candidate").expect("candidate");
        let paths = ClusterKvPaths::new(root.clone());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );

        let metadata = store
            .local_vote_metadata(&node_id)
            .expect("initial vote metadata");
        assert_eq!(
            metadata,
            ClusterKvRaftLocalVoteMetadata {
                current_term: 1,
                last_log_index: 0,
                last_log_term: 0,
            }
        );
        assert_eq!(store.last_log_index(&node_id).expect("last log index"), 0);

        let vote = store
            .apply_vote(
                &node_id,
                RaftRequestVoteRequest {
                    term: 2,
                    candidate_id: candidate_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("apply vote");
        assert!(vote.vote_granted);
        assert_eq!(vote.term, 2);
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after vote");
        assert_eq!(hard_state.current_term, 2);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&candidate_id));
        assert!(hard_state.leader_id.is_none());

        let observed = store.observe_term(&node_id, 3).expect("observe term");
        assert!(observed.is_some());
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after term");
        assert_eq!(hard_state.current_term, 3);
        assert!(hard_state.voted_for.is_none());
        assert!(hard_state.leader_id.is_none());

        store.set_leader(&node_id, 3).expect("set leader");
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after leader");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&node_id));
        assert_eq!(hard_state.leader_id.as_ref(), Some(&node_id));
        assert_eq!(
            store
                .known_leader_id(&node_id)
                .expect("known leader after set")
                .as_ref(),
            Some(&node_id)
        );

        store.clear_leader(&node_id).expect("clear leader");
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after clear");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_ref(), Some(&node_id));
        assert!(hard_state.leader_id.is_none());
        assert_eq!(
            store
                .known_leader_id(&node_id)
                .expect("known leader after clear"),
            None
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_applies_append_and_reports_rejections() {
        let root = temp_raft_root("sinomo-kv-raft-store-append");
        let follower_id =
            NodeId::from_legacy_named("node_raft_store_append_follower").expect("follower");
        let leader_id = NodeId::from_legacy_named("node_raft_store_append_leader").expect("leader");
        let paths = ClusterKvPaths::new(root.clone());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );

        let (outcome, wrote_snapshot) = store
            .apply_append_entries_with_optional_snapshot_and_publish(
                &follower_id,
                None,
                RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![KvLogEntry {
                        index: 1,
                        term: 2,
                        command: KvCommand::Op(KvOp::Put {
                            key: "/raft-store/append".to_string(),
                            value: b"value".to_vec(),
                            lease_id: None,
                        }),
                        revision: KvRevision::ZERO,
                    }],
                    leader_commit: 1,
                },
                |_| false,
            )
            .expect("apply append");

        assert!(outcome.response.success);
        assert!(wrote_snapshot);
        assert!(!outcome.snapshot_restored);
        assert_eq!(outcome.committed_events.len(), 1);
        assert_eq!(outcome.committed_events[0].key, "/raft-store/append");
        assert_eq!(
            outcome
                .snapshot
                .get("/raft-store/append")
                .expect("entry")
                .value,
            b"value".to_vec()
        );
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published snapshot")
                .get("/raft-store/append")
                .expect("published entry")
                .value,
            b"value".to_vec()
        );

        let append_rejection = store
            .append_entries_rejection_response(&follower_id)
            .expect("append rejection");
        assert_eq!(
            append_rejection,
            RaftAppendEntriesResponse {
                term: 2,
                success: false,
                match_index: 1,
            }
        );
        let vote_rejection = store
            .request_vote_rejection_response(&follower_id)
            .expect("vote rejection");
        assert_eq!(
            vote_rejection,
            RaftRequestVoteResponse {
                term: 2,
                vote_granted: false,
            }
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_builds_vote_responses() {
        let root = temp_raft_root("sinomo-kv-raft-store-vote-response");
        let paths = ClusterKvPaths::new(root.clone());
        let local_id =
            NodeId::from_legacy_named("node_raft_store_vote_response_local").expect("local");
        let candidate_id = NodeId::from_legacy_named("node_raft_store_vote_response_candidate")
            .expect("candidate");
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_raft_store_vote_response").expect("cluster"),
            "raft vote response",
            local_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        assert!(cluster_kv_raft_candidate_is_authorized_voter(
            &state, &local_id
        ));

        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let granted = store
            .apply_vote_response(
                &state,
                RaftRequestVoteRequest {
                    term: 1,
                    candidate_id: candidate_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .expect("vote response");
        assert!(granted.accepted);
        assert_eq!(granted.detail, "raft vote granted");
        assert_eq!(granted.state, state);
        assert_eq!(
            granted.vote,
            RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            }
        );

        let rejected = store
            .cluster_raft_vote_rejection_response(
                &granted.state,
                "raft vote rejected for unauthorized or mismatched candidate".to_string(),
            )
            .expect("vote rejection response");
        assert!(!rejected.accepted);
        assert!(!rejected.vote.vote_granted);
        assert_eq!(rejected.vote.term, 1);
        assert!(rejected.detail.contains("unauthorized"));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_appends_commits_and_detects_no_effect_commands() {
        let root = temp_raft_root("sinomo-kv-raft-store-command");
        let node_id = NodeId::from_legacy_named("node_raft_store_command").expect("node");
        let paths = ClusterKvPaths::new(root.clone());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let put = KvCommand::Op(KvOp::Put {
            key: "/raft-store/command".to_string(),
            value: b"value".to_vec(),
            lease_id: None,
        });

        let (committed, wrote_snapshot) = store
            .append_command_with_response_and_publish_report(&node_id, put.clone(), |_| false)
            .expect("append and commit");
        assert_eq!(committed.snapshot.revision, KvRevision(1));
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].key, "/raft-store/command");
        assert!(committed.txn_response.is_none());
        assert!(wrote_snapshot);
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published command snapshot")
                .get("/raft-store/command")
                .expect("published command")
                .value,
            b"value".to_vec()
        );
        let hard_state = paths
            .raft_wal_paths(&node_id)
            .read_persisted_hard_state()
            .expect("read hard state after commit");
        assert_eq!(hard_state.commit_index, 1);

        let (no_effect_commit, no_effect_wrote_snapshot) = store
            .append_command_with_response_and_publish_report(&node_id, put.clone(), |_| false)
            .expect("append no-effect command");
        assert_eq!(no_effect_commit.snapshot.revision, KvRevision(1));
        assert!(no_effect_commit.events.is_empty());
        assert!(!no_effect_wrote_snapshot);

        let no_effect_uncommitted = store
            .append_command_uncommitted_or_no_effect(&node_id, put)
            .expect("uncommitted no-effect command");
        match no_effect_uncommitted {
            ClusterKvRaftUncommittedAppend::NoEffect {
                snapshot,
                txn_response,
            } => {
                assert_eq!(snapshot.revision, KvRevision(1));
                assert!(txn_response.is_none());
            }
            ClusterKvRaftUncommittedAppend::Appended(entry) => {
                panic!("expected no-effect append, got entry {entry:?}");
            }
        }

        let append = store
            .append_command_uncommitted(
                &node_id,
                KvCommand::Op(KvOp::Put {
                    key: "/raft-store/pending".to_string(),
                    value: b"pending".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted command");
        assert_eq!(append.index, 2);
        let (committed, wrote_snapshot) = store
            .commit_index_with_events_and_publish_report(&node_id, append.index, |_| false)
            .expect("commit uncommitted command");
        assert_eq!(committed.snapshot.revision, KvRevision(2));
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].key, "/raft-store/pending");
        assert!(wrote_snapshot);
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published pending snapshot")
                .get("/raft-store/pending")
                .expect("published pending")
                .value,
            b"pending".to_vec()
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_local_command_storage_appends_commits_and_discards() {
        let root = temp_raft_root("sinomo-kv-local-command-storage");
        let node_id = NodeId::from_legacy_named("node_kv_local_command_storage").expect("node");
        let paths = ClusterKvPaths::new(root.clone());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let mut storage = ClusterKvRaftLocalCommandStorage::new(store, node_id, |_| false);

        let entry = match storage
            .append_uncommitted_or_no_effect(KvCommand::Op(KvOp::Put {
                key: "/raft-local-storage/committed".to_string(),
                value: b"committed".to_vec(),
                lease_id: None,
            }))
            .expect("append command")
        {
            ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
            ClusterKvRaftUncommittedAppend::NoEffect { .. } => {
                panic!("fresh command should append")
            }
        };
        let commit = storage
            .commit_index_with_events(entry.index)
            .expect("commit command");
        assert_eq!(commit.events.len(), 1);
        assert_eq!(storage.take_last_snapshot_publish_wrote(), Some(true));
        assert_eq!(
            read_cluster_kv_cached_snapshot(&paths)
                .expect("published snapshot")
                .get("/raft-local-storage/committed")
                .expect("committed key")
                .value,
            b"committed".to_vec()
        );

        let _ = storage
            .append_uncommitted_or_no_effect(KvCommand::Op(KvOp::Put {
                key: "/raft-local-storage/discarded".to_string(),
                value: b"discarded".to_vec(),
                lease_id: None,
            }))
            .expect("append discarded command");
        let discarded = storage
            .discard_uncommitted_tail_and_clear_leader()
            .expect("discard tail");
        assert!(discarded.get("/raft-local-storage/discarded").is_none());
        assert_eq!(storage.take_last_snapshot_publish_wrote(), Some(false));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cluster_kv_raft_store_builds_append_request_from_retained_tail() {
        let root = temp_raft_root("sinomo-kv-raft-store-append-request-tail");
        let node_id = NodeId::from_legacy_named("node_raft_store_append_request").expect("node");
        let leader_id =
            NodeId::from_legacy_named("node_raft_store_append_request_leader").expect("leader");
        let paths = ClusterKvPaths::new(root.clone());
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let mut node = DurableRaftKvNode::open(node_id.clone(), paths.raft_wal_paths(&node_id))
            .expect("open node");
        for index in 1..=400u64 {
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: format!("/append-request/{index:03}"),
                        value: index.to_le_bytes().to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append");
            node.commit_to(entry.index).expect("commit");
        }
        node.save_snapshot().expect("save snapshot");
        let snapshot_index = node.snapshot_index();
        let earliest_retained = node.log().first().map(|entry| entry.index).expect("tail");
        let last_log_index = node.last_log_index();
        assert!(snapshot_index > 0);
        assert!(!node.log().is_empty());
        assert!(node.log().len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize);
        drop(node);

        let heartbeat = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                last_log_index.saturating_add(1),
            )
            .expect("heartbeat");
        assert!(heartbeat.snapshot.is_none());
        assert!(heartbeat.append.entries.is_empty());
        assert_eq!(&heartbeat.append.leader_id, &leader_id);
        assert_eq!(heartbeat.append.prev_log_index, snapshot_index);

        let near = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                earliest_retained + 1,
            )
            .expect("near");
        assert!(near.snapshot.is_none());
        assert!(!near.append.entries.is_empty());

        let far = store
            .build_append_entries_request_from_next_index(
                &node_id,
                &leader_id,
                earliest_retained.saturating_sub(1).max(1),
            )
            .expect("far");
        assert!(far.snapshot.is_some());

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_compaction_retains_log_tail_across_reopen() {
        let root = temp_raft_root("sinomo-kv-retain-tail");
        let node_id = NodeId::from_legacy_named("node_retain_tail").expect("node");
        let paths = KvWalPaths::new(&root);
        let snapshot_index;
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            for index in 1..=400u64 {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/retain/{index:03}"),
                            value: index.to_le_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append");
                node.commit_to(entry.index).expect("commit");
            }
            node.save_snapshot().expect("snapshot");
            snapshot_index = node.snapshot_index();
            assert_eq!(snapshot_index, 400);
            // Compaction keeps a bounded committed tail rather than truncating to empty.
            assert!(!node.log().is_empty());
            assert!(node.log().len() <= 256);
        }

        // The critical property: reopening from disk reloads the tail (the daemon
        // opens a fresh node per replication call), so incremental replication keeps
        // working instead of falling back to a full snapshot every time.
        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");
        assert!(
            !node.log().is_empty(),
            "retained tail must survive reopen for incremental replication"
        );
        assert!(node.log().len() <= 256);
        assert_eq!(node.snapshot_index(), snapshot_index);
        // State is intact and not double-applied.
        assert_eq!(node.applied_index(), 400);
        assert_eq!(node.store().revision(), KvRevision(400));
        assert_eq!(
            node.store().get("/retain/400").expect("entry").value,
            400u64.to_le_bytes().to_vec()
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_node_opens_from_tokio_without_blocking_runtime() {
        let root = temp_raft_root("sinomo-kv-async-open");
        let node_id = NodeId::from_legacy_named("node_async_open").expect("node");
        let paths = KvWalPaths::new(&root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");

        let node = runtime
            .block_on(DurableRaftKvNode::open_async(node_id.clone(), paths))
            .expect("open async");

        assert_eq!(node.node_id(), &node_id);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_power_loss_before_commit_does_not_apply_uncommitted_wal() {
        let root = temp_raft_root("sinomo-kv-powerloss-uncommitted");
        let node_id = NodeId::from_legacy_named("node_powerloss_uncommitted").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/uncommitted".to_string(),
                    value: b"lost".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append uncommitted");
        }

        let mut node = DurableRaftKvNode::open(node_id, paths).expect("reopen after power loss");

        assert!(node.store().get("/config/uncommitted").is_none());
        assert_eq!(node.hard_state().commit_index, 0);
        assert_eq!(node.last_log_index(), 1);
        node.commit_to(1).expect("commit recovered wal entry");
        assert_eq!(
            node.store()
                .get("/config/uncommitted")
                .expect("committed after recovery")
                .value,
            b"lost"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_append_local_command_rolls_back_after_wal_failure() {
        let root = temp_raft_root("sinomo-kv-local-append-rollback");
        let node_id = NodeId::from_legacy_named("node_local_append_rollback").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        std::fs::create_dir(&paths.wal).expect("block WAL appends");

        let error = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/rollback/local".to_string(),
                    value: b"not-durable".to_vec(),
                    lease_id: None,
                }),
            )
            .expect_err("WAL append should fail");

        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert_eq!(
            node.last_log_index(),
            0,
            "failed WAL append must not leave an in-memory log entry"
        );
        assert_eq!(node.hard_state(), &RaftHardState::default());
        assert!(node.store().get("/rollback/local").is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_append_entries_rolls_back_after_wal_failure() {
        let leader_root = temp_raft_root("sinomo-kv-follower-append-rollback-leader");
        let follower_root = temp_raft_root("sinomo-kv-follower-append-rollback");
        let leader_id = NodeId::from_legacy_named("node_append_rollback_leader").expect("leader");
        let follower_id =
            NodeId::from_legacy_named("node_append_rollback_follower").expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/rollback/follower".to_string(),
                        value: b"not-durable".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader");
            leader.commit_to(entry.index).expect("commit leader");
            entry
        };
        let mut follower =
            DurableRaftKvNode::open(follower_id, follower_paths.clone()).expect("open follower");
        std::fs::create_dir(&follower_paths.wal).expect("block follower WAL appends");

        let error = follower
            .append_entries(RaftAppendEntriesRequest {
                term: 1,
                leader_id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry],
                leader_commit: 1,
            })
            .expect_err("WAL append should fail");

        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert_eq!(
            follower.last_log_index(),
            0,
            "failed follower append must not leave an in-memory log entry"
        );
        assert_eq!(follower.hard_state(), &RaftHardState::default());
        assert!(follower.store().get("/rollback/follower").is_none());
        std::fs::remove_dir_all(leader_root).ok();
        std::fs::remove_dir_all(follower_root).ok();
    }

    #[test]
    fn durable_raft_append_entries_rolls_back_after_hard_state_failure() {
        let leader_root = temp_raft_root("sinomo-kv-follower-hard-state-rollback-leader");
        let follower_root = temp_raft_root("sinomo-kv-follower-hard-state-rollback");
        let leader_id =
            NodeId::from_legacy_named("node_append_hard_state_rollback_leader").expect("leader");
        let follower_id = NodeId::from_legacy_named("node_append_hard_state_rollback_follower")
            .expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/rollback/follower-hard-state".to_string(),
                        value: b"not-committed".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader");
            leader.commit_to(entry.index).expect("commit leader");
            entry
        };
        let mut follower =
            DurableRaftKvNode::open(follower_id, follower_paths.clone()).expect("open follower");
        std::fs::remove_file(&follower_paths.hard_state).ok();
        std::fs::create_dir(&follower_paths.hard_state).expect("block hard-state writes");

        let error = follower
            .append_entries(RaftAppendEntriesRequest {
                term: 1,
                leader_id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry],
                leader_commit: 1,
            })
            .expect_err("hard-state persist should fail");

        assert!(
            error.to_string().contains("hard-state.sobj"),
            "unexpected error: {error}"
        );
        assert_eq!(
            follower.hard_state().commit_index,
            0,
            "failed hard-state persistence must roll back in-memory commit index"
        );
        assert_eq!(follower.last_log_index(), 0);
        assert_eq!(follower.applied_index(), 0);
        assert!(
            follower
                .store()
                .get("/rollback/follower-hard-state")
                .is_none(),
            "failed hard-state persistence must not expose the committed value"
        );
        std::fs::remove_dir_all(leader_root).ok();
        std::fs::remove_dir_all(follower_root).ok();
    }

    #[test]
    fn durable_raft_commit_rolls_back_after_hard_state_failure() {
        let root = temp_raft_root("sinomo-kv-commit-rollback");
        let node_id = NodeId::from_legacy_named("node_commit_rollback").expect("node");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open");
        let entry = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/rollback/commit".to_string(),
                    value: b"not-committed".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append");
        std::fs::remove_file(&paths.hard_state).expect("remove hard state file");
        std::fs::create_dir(&paths.hard_state).expect("block hard-state writes");

        let error = node
            .commit_to(entry.index)
            .expect_err("hard-state persist should fail");

        assert!(
            error.to_string().contains("hard-state.sobj"),
            "unexpected error: {error}"
        );
        assert_eq!(
            node.hard_state().commit_index,
            0,
            "failed commit persistence must roll back in-memory hard state"
        );
        assert_eq!(node.applied_index(), 0);
        assert!(
            node.store().get("/rollback/commit").is_none(),
            "failed commit persistence must not expose the applied value"
        );
        assert_eq!(
            node.last_log_index(),
            entry.index,
            "the durable uncommitted WAL entry should remain available in memory"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_rejects_commit_beyond_local_log() {
        let root = temp_raft_root("sinomo-kv-commit-beyond-log");
        let node_id = NodeId::from_legacy_named("node_commit_beyond_log").expect("node");
        let paths = KvWalPaths::new(&root);

        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let error = node.commit_to(1).expect_err("missing log entry");
            assert_eq!(error, KvError::MissingCommittedEntry { index: 1 });
            assert_eq!(
                node.hard_state().commit_index,
                0,
                "rejected commits must not advance in-memory hard state"
            );
        }

        let reopened = DurableRaftKvNode::open(node_id, paths.clone()).expect("reopen");
        assert_eq!(
            reopened.hard_state().commit_index,
            0,
            "rejected commits must not persist impossible hard state"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_open_repairs_commit_index_beyond_local_log() {
        let root = temp_raft_root("sinomo-kv-repair-commit-beyond-log");
        let node_id = NodeId::from_legacy_named("node_repair_commit_beyond_log").expect("node");
        let paths = KvWalPaths::new(&root);
        write_object_file(
            &paths.hard_state,
            &RaftHardState {
                commit_index: 7,
                ..RaftHardState::default()
            },
        )
        .expect("write impossible hard state");

        {
            let node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open repairs");
            assert_eq!(
                node.hard_state().commit_index,
                0,
                "open should clamp persisted commit index to the available local log"
            );
        }

        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read repaired hard state")
                .commit_index,
            0,
            "repair should be durable so the next open stays cheap"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn persisted_hard_state_reads_use_identity_cache() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        let root = temp_raft_root("sinomo-kv-hard-state-cache");
        let paths = KvWalPaths::new(&root);
        hard_state_cache().lock().expect("hard-state cache").clear();
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);

        assert_eq!(
            paths.read_persisted_hard_state().expect("read absent"),
            RaftHardState::default()
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "the first absent-file read should touch the filesystem"
        );
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read absent again"),
            RaftHardState::default()
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "unchanged absent hard state should be served from cache"
        );

        let first_leader = NodeId::from_legacy_named("hard_state_cache_first").expect("leader");
        let first = RaftHardState {
            current_term: 3,
            voted_for: Some(first_leader.clone()),
            leader_id: Some(first_leader),
            commit_index: 5,
        };
        paths
            .write_persisted_hard_state(&first)
            .expect("write hard state");
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read write-through cache"),
            first
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            0,
            "local hard-state writes should refresh the read cache"
        );

        let second_leader = NodeId::from_legacy_named("hard_state_cache_second").expect("leader");
        let second = RaftHardState {
            current_term: 4,
            voted_for: Some(second_leader.clone()),
            leader_id: Some(second_leader),
            commit_index: 6,
        };
        write_object_file(&paths.hard_state, &second).expect("external hard-state replace");
        reset_persisted_hard_state_actual_reads_for_test(&paths.hard_state);
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read external replacement"),
            second
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "a changed file identity must force one real read"
        );
        assert_eq!(
            paths
                .read_persisted_hard_state()
                .expect("read cached replacement"),
            second
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&paths.hard_state),
            1,
            "unchanged replacement should be cached after the first read"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn replicated_node_commit_gap_does_not_partially_apply() {
        let mut node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![
                KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                KvLogEntry {
                    index: 3,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/three".to_string(),
                        value: b"three".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
            ],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: BTreeMap::new(),
        };

        let error = node.commit_to(3).expect_err("missing middle entry");

        assert_eq!(error, KvError::MissingCommittedEntry { index: 2 });
        assert_eq!(node.applied_index, 0);
        assert_eq!(node.hard_state.commit_index, 0);
        assert_eq!(node.store.revision(), KvRevision::ZERO);
        assert!(node.store.get("/gap/one").is_none());
        assert!(node.store.get("/gap/three").is_none());
    }

    #[test]
    fn replicated_node_single_entry_commit_uses_single_entry_plan() {
        let node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![
                KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/commit-plan/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                KvLogEntry {
                    index: 2,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/commit-plan/two".to_string(),
                        value: b"two".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
            ],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: BTreeMap::new(),
        };

        assert!(
            matches!(
                node.entries_to_commit(1).expect("single commit plan"),
                CommitEntries::Single(_)
            ),
            "the common next-entry commit should avoid allocating a one-entry batch"
        );
        assert!(
            matches!(
                node.entries_to_commit(2).expect("batch commit plan"),
                CommitEntries::Batch(entries) if entries.len() == 2
            ),
            "catch-up commits still need the contiguous batch path"
        );
    }

    #[test]
    fn replicated_node_rejects_same_term_conflicting_log_entry() {
        let mut node = ReplicatedKvNode {
            store: InProcessKvStore::new(),
            log: vec![KvLogEntry {
                index: 1,
                term: 1,
                command: KvCommand::Op(KvOp::Put {
                    key: "/conflict/original".to_string(),
                    value: b"original".to_vec(),
                    lease_id: None,
                }),
                revision: KvRevision::ZERO,
            }],
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: BTreeMap::new(),
        };

        let error = node
            .append_entries(vec![KvLogEntry {
                index: 1,
                term: 1,
                command: KvCommand::Op(KvOp::Put {
                    key: "/conflict/replacement".to_string(),
                    value: b"replacement".to_vec(),
                    lease_id: None,
                }),
                revision: KvRevision::ZERO,
            }])
            .expect_err("same-term command conflict");

        assert_eq!(error, KvError::ConflictingLogEntry { index: 1 });
        assert_eq!(node.log.len(), 1);
        assert_eq!(
            node.log[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
    }

    #[test]
    fn durable_raft_discard_uncommitted_tail_keeps_committed_prefix() {
        let root = temp_raft_root("sinomo-kv-discard-uncommitted-tail");
        let node_id = NodeId::from_legacy_named("node_discard_uncommitted_tail").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/committed".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit first");
            let uncommitted = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/uncommitted".to_string(),
                        value: b"dropped".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append uncommitted");
            assert_eq!(uncommitted.index, 2);
            assert_eq!(node.last_log_index(), 2);
        }

        {
            let mut node =
                DurableRaftKvNode::open_discarding_uncommitted_tail(node_id.clone(), paths.clone())
                    .expect("open and discard");
            assert_eq!(node.hard_state().commit_index, 1);
            assert_eq!(node.last_log_index(), 1);
            assert_eq!(
                node.store()
                    .get("/config/committed")
                    .expect("committed value")
                    .value,
                b"kept"
            );
            assert!(node.store().get("/config/uncommitted").is_none());
            assert!(
                !node
                    .discard_uncommitted_tail()
                    .expect("second discard is a no-op")
            );
            let replacement = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/replacement".to_string(),
                        value: b"next".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append replacement");
            assert_eq!(replacement.index, 2);
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen after discard");
        assert_eq!(node.last_log_index(), 2);
        assert_eq!(node.hard_state().commit_index, 1);
        assert!(node.store().get("/config/replacement").is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_observe_higher_term_clears_vote_and_leader() {
        let root = temp_raft_root("sinomo-kv-observe-higher-term");
        let node_id = NodeId::from_legacy_named("node_observe_higher_term").expect("node");
        let leader_id = NodeId::from_legacy_named("node_observe_higher_term_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            node.request_vote(RaftRequestVoteRequest {
                term: 2,
                candidate_id: leader_id.clone(),
                last_log_index: 0,
                last_log_term: 0,
            })
            .expect("vote");
            let append = node
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("append");
            assert!(append.success);
            assert!(node.hard_state().voted_for.is_some());
            assert!(node.hard_state().leader_id.is_some());
            assert!(node.observe_term(5).expect("observe higher term"));
            assert_eq!(node.hard_state().current_term, 5);
            assert!(node.hard_state().voted_for.is_none());
            assert!(node.hard_state().leader_id.is_none());
            assert!(!node.observe_term(4).expect("ignore lower term"));
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen");
        assert_eq!(node.hard_state().current_term, 5);
        assert!(node.hard_state().voted_for.is_none());
        assert!(node.hard_state().leader_id.is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_power_loss_after_commit_replays_committed_state() {
        let root = temp_raft_root("sinomo-kv-powerloss-committed");
        let node_id = NodeId::from_legacy_named("node_powerloss_committed").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let entry = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/committed".to_string(),
                        value: b"survives".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(entry.index).expect("persist commit");
        }

        let node = DurableRaftKvNode::open(node_id, paths).expect("reopen after power loss");

        assert_eq!(node.hard_state().commit_index, 1);
        assert_eq!(
            node.store()
                .get("/config/committed")
                .expect("committed value")
                .value,
            b"survives"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_persists_higher_term_from_rejected_append() {
        let root = temp_raft_root("sinomo-kv-rejected-append-term");
        let follower_id = NodeId::from_legacy_named("node_rejected_append_follower").expect("node");
        let leader_id = NodeId::from_legacy_named("node_rejected_append_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open follower");
            let entry = follower
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/local".to_string(),
                        value: b"local".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append local");
            follower.commit_to(entry.index).expect("commit local");

            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 99,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("rejected append response");

            assert!(!response.success);
            assert_eq!(follower.hard_state().current_term, 2);
            assert_eq!(follower.hard_state().leader_id.as_ref(), Some(&leader_id));
        }

        let reopened =
            DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen follower");
        assert_eq!(reopened.hard_state().current_term, 2);
        assert_eq!(reopened.hard_state().leader_id.as_ref(), Some(&leader_id));
        assert_eq!(
            reopened
                .store()
                .get("/config/local")
                .expect("committed local value")
                .value,
            b"local"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_unchanged_heartbeat_does_not_rewrite_hard_state() {
        let root = temp_raft_root("sinomo-kv-unchanged-heartbeat");
        let follower_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_follower").expect("node");
        let leader_id = NodeId::from_legacy_named("node_unchanged_heartbeat_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        let heartbeat = RaftAppendEntriesRequest {
            term: 2,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open");
            let response = follower
                .append_entries(heartbeat.clone())
                .expect("initial heartbeat");
            assert!(response.success);
        }
        let before = fs::metadata(&paths.hard_state)
            .expect("hard state metadata")
            .modified()
            .expect("hard state modified");
        thread::sleep(Duration::from_millis(20));
        {
            let mut follower = DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen");
            let response = follower
                .append_entries(heartbeat)
                .expect("unchanged heartbeat");
            assert!(response.success);
        }
        let after = fs::metadata(&paths.hard_state)
            .expect("hard state metadata after")
            .modified()
            .expect("hard state modified after");

        assert_eq!(before, after);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_unchanged_heartbeat_with_wal_tail_does_not_rewrite_files() {
        let root = temp_raft_root("sinomo-kv-unchanged-heartbeat-wal");
        let follower_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_wal_follower").expect("node");
        let leader_id =
            NodeId::from_legacy_named("node_unchanged_heartbeat_wal_leader").expect("node");
        let paths = KvWalPaths::new(&root);
        let entry = KvLogEntry {
            index: 1,
            term: 2,
            command: KvCommand::Op(KvOp::Put {
                key: "/config/heartbeat-wal".to_string(),
                value: b"reachable".to_vec(),
                lease_id: None,
            }),
            revision: KvRevision::ZERO,
        };

        {
            let mut follower =
                DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open");
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![entry],
                    leader_commit: 0,
                })
                .expect("append leader entry");
            assert!(response.success);
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id: leader_id.clone(),
                    prev_log_index: 1,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 1,
                })
                .expect("commit leader entry");
            assert!(response.success);
        }

        let hard_state_before = fs::metadata(&paths.hard_state)
            .expect("hard state metadata")
            .modified()
            .expect("hard state modified");
        let wal_before = fs::metadata(&paths.wal)
            .expect("wal metadata")
            .modified()
            .expect("wal modified");
        thread::sleep(Duration::from_millis(20));

        {
            let mut follower = DurableRaftKvNode::open(follower_id, paths.clone()).expect("reopen");
            let response = follower
                .append_entries(RaftAppendEntriesRequest {
                    term: 2,
                    leader_id,
                    prev_log_index: 1,
                    prev_log_term: 2,
                    entries: Vec::new(),
                    leader_commit: 1,
                })
                .expect("unchanged heartbeat");
            assert!(response.success);
        }

        let hard_state_after = fs::metadata(&paths.hard_state)
            .expect("hard state metadata after")
            .modified()
            .expect("hard state modified after");
        let wal_after = fs::metadata(&paths.wal)
            .expect("wal metadata after")
            .modified()
            .expect("wal modified after");

        assert_eq!(hard_state_before, hard_state_after);
        assert_eq!(wal_before, wal_after);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_noop_requests_do_not_create_hard_state_file() {
        let root = temp_raft_root("sinomo-kv-noop-hard-state");
        let node_id = NodeId::from_legacy_named("node_noop_hard_state").expect("node");
        let candidate_id = NodeId::from_legacy_named("node_noop_candidate").expect("candidate");
        let leader_id = NodeId::from_legacy_named("node_noop_leader").expect("leader");
        let paths = KvWalPaths::new(&root);

        {
            let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open");
            let vote = node
                .request_vote(RaftRequestVoteRequest {
                    term: 0,
                    candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .expect("stale vote");
            assert!(!vote.vote_granted);
            let append = node
                .append_entries(RaftAppendEntriesRequest {
                    term: 0,
                    leader_id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                })
                .expect("stale append");
            assert!(!append.success);
        }
        assert!(
            !paths.hard_state.exists(),
            "rejected requests that do not change hard state should not create a hard-state file"
        );

        {
            let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("reopen");
            assert!(node.observe_term(2).expect("observe higher term"));
        }
        assert!(
            paths.hard_state.exists(),
            "real hard-state changes must still be persisted"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_duplicate_append_entries_do_not_grow_wal() {
        let leader_root = temp_raft_root("sinomo-kv-duplicate-append-leader");
        let follower_root = temp_raft_root("sinomo-kv-duplicate-append-follower");
        let leader_id = NodeId::from_legacy_named("node_duplicate_append_leader").expect("leader");
        let follower_id =
            NodeId::from_legacy_named("node_duplicate_append_follower").expect("follower");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let entry = {
            let mut leader =
                DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("open leader");
            let entry = leader
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/config/duplicate".to_string(),
                        value: b"value".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append leader entry");
            leader.commit_to(entry.index).expect("commit leader entry");
            entry
        };
        let request = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry],
            leader_commit: 1,
        };

        {
            let mut follower = DurableRaftKvNode::open(follower_id.clone(), follower_paths.clone())
                .expect("open follower");
            assert!(
                follower
                    .append_entries(request.clone())
                    .expect("append")
                    .success
            );
        }
        let before_len = fs::metadata(&follower_paths.wal)
            .expect("follower wal metadata")
            .len();
        {
            let mut follower = DurableRaftKvNode::open(follower_id, follower_paths.clone())
                .expect("reopen follower");
            assert!(
                follower
                    .append_entries(request)
                    .expect("duplicate append")
                    .success
            );
        }
        let after_len = fs::metadata(&follower_paths.wal)
            .expect("follower wal metadata after")
            .len();

        assert_eq!(before_len, after_len);
        std::fs::remove_dir_all(leader_root).ok();
        std::fs::remove_dir_all(follower_root).ok();
    }

    #[test]
    fn delayed_stale_append_entries_do_not_roll_back_committed_state() {
        let leader_root = temp_raft_root("sinomo-kv-latency-leader");
        let follower_root = temp_raft_root("sinomo-kv-latency-follower");
        let leader_id = NodeId::from_legacy_named("node_latency_leader").expect("node");
        let follower_id = NodeId::from_legacy_named("node_latency_follower").expect("node");
        let leader_paths = KvWalPaths::new(&leader_root);
        let follower_paths = KvWalPaths::new(&follower_root);
        let mut leader =
            DurableRaftKvNode::open(leader_id.clone(), leader_paths).expect("leader open");
        let mut follower =
            DurableRaftKvNode::open(follower_id.clone(), follower_paths).expect("follower open");

        let first = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append first");
        leader.commit_to(first.index).expect("commit first");
        let stale_append = RaftAppendEntriesRequest {
            term: 1,
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: leader.log().to_vec(),
            leader_commit: 1,
        };
        let second = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/b".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append second");
        leader.commit_to(second.index).expect("commit second");
        let current_append = RaftAppendEntriesRequest {
            term: 1,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: leader.log().to_vec(),
            leader_commit: 2,
        };

        assert!(
            follower
                .append_entries(current_append)
                .expect("current append")
                .success
        );
        assert_eq!(
            follower
                .store()
                .get("/config/b")
                .expect("second value")
                .value,
            b"two"
        );
        assert!(
            follower
                .append_entries(stale_append)
                .expect("stale append")
                .success
        );

        assert_eq!(follower.hard_state().commit_index, 2);
        assert_eq!(
            follower
                .store()
                .get("/config/a")
                .expect("first value")
                .value,
            b"one"
        );
        assert_eq!(
            follower
                .store()
                .get("/config/b")
                .expect("second value")
                .value,
            b"two"
        );
        std::fs::remove_dir_all(leader_root).ok();
        std::fs::remove_dir_all(follower_root).ok();
    }

    #[test]
    fn optional_snapshot_append_restores_newer_snapshot() {
        let leader_root = temp_raft_root("sinomo-kv-optional-snapshot-append-leader");
        let follower_root = temp_raft_root("sinomo-kv-optional-snapshot-append-follower");
        let leader_id = NodeId::from_legacy_named("node_optional_snapshot_leader").expect("node");
        let follower_id =
            NodeId::from_legacy_named("node_optional_snapshot_follower").expect("node");
        let mut leader = DurableRaftKvNode::open(leader_id.clone(), KvWalPaths::new(&leader_root))
            .expect("leader open");
        let mut follower = DurableRaftKvNode::open(follower_id, KvWalPaths::new(&follower_root))
            .expect("follower open");

        let entry = leader
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/config/snapshot".to_string(),
                    value: b"from-snapshot".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append leader command");
        leader
            .commit_to(entry.index)
            .expect("commit leader command");
        let snapshot = leader.save_snapshot().expect("save leader snapshot");

        let outcome = follower
            .apply_append_entries_with_optional_snapshot(
                Some(snapshot.clone()),
                RaftAppendEntriesRequest {
                    term: 1,
                    leader_id,
                    prev_log_index: snapshot.last_included_index,
                    prev_log_term: snapshot.last_included_term,
                    entries: Vec::new(),
                    leader_commit: snapshot.last_included_index,
                },
            )
            .expect("apply append with snapshot");

        assert!(outcome.response.success);
        assert!(outcome.snapshot_restored);
        assert!(outcome.committed_events.is_empty());
        assert_eq!(
            follower.hard_state().commit_index,
            snapshot.last_included_index
        );
        assert_eq!(
            follower
                .store()
                .get("/config/snapshot")
                .expect("restored value")
                .value,
            b"from-snapshot"
        );
        std::fs::remove_dir_all(leader_root).ok();
        std::fs::remove_dir_all(follower_root).ok();
    }

    #[test]
    fn snapshot_restore_preserves_watchable_event_history() {
        let mut store = InProcessKvStore::new();
        store.put("/watch/a", b"one".to_vec(), None).expect("put");
        let snapshot = store.snapshot();
        let mut restored = InProcessKvStore::restore(snapshot);

        let watch_id = restored
            .watch_prefix("/watch/", KvRevision(1))
            .expect("watch after restore");
        let events = restored.drain_watch(watch_id).expect("events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "/watch/a");
    }

    #[test]
    fn durable_raft_node_recovers_by_truncating_corrupt_wal_tail() {
        let root = temp_raft_root("sinomo-kv-corrupt-wal");
        let node_id = NodeId::from_legacy_named("node_corrupt_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
        let before_snapshot = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/before-snapshot".to_string(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append before snapshot");
        node.commit_to(before_snapshot.index)
            .expect("commit before snapshot");
        node.save_snapshot().expect("snapshot");
        let after_snapshot = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/after-snapshot".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append after snapshot");
        node.commit_to(after_snapshot.index)
            .expect("commit after snapshot");
        drop(node);

        let mut wal = OpenOptions::new()
            .append(true)
            .open(&paths.wal)
            .expect("open wal");
        writeln!(wal, "{{\"index\":{{broken").expect("corrupt wal");
        writeln!(wal, "still broken").expect("corrupt wal continuation");
        drop(wal);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/after-snapshot")
                .expect("valid entry after snapshot")
                .value,
            b"two"
        );
        let rewritten = std::fs::read(&paths.wal).expect("rewritten wal");
        assert!(
            !rewritten
                .windows(b"broken".len())
                .any(|window| window == b"broken")
        );
        assert!(
            fs::read_dir(&paths.root)
                .expect("raft dir")
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                .all(|name| !name.starts_with(".wal.sobjlog.tmp-")),
            "WAL repair must not leave rewrite temp files behind"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_open_repairs_wal_records_already_covered_by_snapshot() {
        let root = temp_raft_root("sinomo-kv-snapshot-covered-wal");
        let node_id = NodeId::from_legacy_named("node_snapshot_covered_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        let command_count = RAFT_COMPACTION_RETAINED_ENTRIES + 20;
        let overgrown_log = {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            for index in 1..=command_count {
                let entry = node
                    .append_local_command(
                        1,
                        KvCommand::Op(KvOp::Put {
                            key: format!("/covered/{index}"),
                            value: index.to_be_bytes().to_vec(),
                            lease_id: None,
                        }),
                    )
                    .expect("append command");
                node.commit_to(entry.index).expect("commit command");
            }
            let overgrown_log = node.log().to_vec();
            assert!(
                overgrown_log.len() > RAFT_COMPACTION_RETAINED_ENTRIES as usize,
                "fixture should start with a WAL larger than the retained tail"
            );
            node.save_snapshot().expect("snapshot");
            rewrite_wal_entries(&paths.wal, &overgrown_log).expect("restore overgrown wal");
            overgrown_log
        };

        let before = read_wal_entries(&paths.wal, None, 0).expect("read overgrown wal");
        assert_eq!(before.entries.len(), overgrown_log.len());
        assert!(!before.truncated);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get(&format!("/covered/{command_count}"))
                .expect("last committed value")
                .value,
            command_count.to_be_bytes().to_vec()
        );
        drop(recovered);

        let after = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert!(!after.truncated);
        assert!(
            after.entries.len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize,
            "recovery should rewrite snapshot-covered WAL records out of the file"
        );
        assert_eq!(
            after.entries.last().map(|entry| entry.index),
            Some(command_count)
        );
        assert!(
            after
                .entries
                .first()
                .is_some_and(|entry| entry.index > overgrown_log[0].index),
            "repaired WAL should start after the redundant pre-retention records"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_open_repairs_gapped_valid_wal_tail() {
        let root = temp_raft_root("sinomo-kv-gapped-valid-wal");
        let node_id = NodeId::from_legacy_named("node_gapped_valid_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let first = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/gap/one".to_string(),
                        value: b"one".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append first");
            node.commit_to(first.index).expect("commit first");
        }
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(&paths.wal)
                .expect("open wal");
            write_wal_record(
                &mut wal,
                &KvLogEntry {
                    index: 3,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/gap/three".to_string(),
                        value: b"three".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &paths.wal,
            )
            .expect("write gapped valid tail");
            wal.sync_data().expect("sync gapped tail");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered.store().get("/gap/one").expect("kept").value,
            b"one"
        );
        assert!(
            recovered.store().get("/gap/three").is_none(),
            "gapped valid WAL tail must not be applied"
        );
        assert_eq!(recovered.hard_state().commit_index, 1);
        drop(recovered);

        let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert_eq!(
            repaired
                .entries
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![1],
            "WAL rewrite should keep only the contiguous safe prefix"
        );
        assert!(!repaired.truncated);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_open_repairs_same_term_conflicting_wal_tail() {
        let root = temp_raft_root("sinomo-kv-conflicting-valid-wal");
        let node_id = NodeId::from_legacy_named("node_conflicting_valid_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            node.append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/conflict/original".to_string(),
                    value: b"original".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append original");
        }
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(&paths.wal)
                .expect("open wal");
            write_wal_record(
                &mut wal,
                &KvLogEntry {
                    index: 1,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/conflict/replacement".to_string(),
                        value: b"replacement".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &paths.wal,
            )
            .expect("write conflicting valid tail");
            wal.sync_data().expect("sync conflicting tail");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(recovered.last_log_index(), 1);
        assert_eq!(
            recovered.log()[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
        drop(recovered);

        let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert_eq!(repaired.entries.len(), 1);
        assert_eq!(
            repaired.entries[0].command,
            KvCommand::Op(KvOp::Put {
                key: "/conflict/original".to_string(),
                value: b"original".to_vec(),
                lease_id: None,
            })
        );
        assert!(!repaired.truncated);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn wal_rewrite_cleans_temp_file_after_replace_failure() {
        let root = temp_raft_root("sinomo-kv-wal-temp-cleanup");
        let path = root.join("wal.sobjlog");
        std::fs::create_dir_all(&path).expect("target directory");
        let entry = KvLogEntry {
            index: 1,
            term: 1,
            command: KvCommand::Op(KvOp::Delete {
                key: "/wal/temp-cleanup".to_string(),
            }),
            revision: KvRevision::ZERO,
        };

        let error = rewrite_wal_entries(&path, &[entry]).expect_err("directory replace fails");
        assert!(
            error.to_string().contains("wal.sobjlog"),
            "unexpected error: {error}"
        );
        assert!(
            fs::read_dir(&root)
                .expect("raft dir")
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                .all(|name| !name.starts_with(".wal.sobjlog.tmp-")),
            "failed WAL rewrite must not leave staged temp files behind"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_ignores_leftover_wal_rewrite_temp_file() {
        let root = temp_raft_root("sinomo-kv-leftover-wal-temp");
        let node_id = NodeId::from_legacy_named("node_leftover_wal_temp").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/committed".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit");
        }

        let temp_path = paths.root.join(".wal.sobjlog.tmp-leftover");
        {
            let mut temp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .expect("open leftover temp");
            write_wal_record(
                &mut temp,
                &KvLogEntry {
                    index: 2,
                    term: 1,
                    command: KvCommand::Op(KvOp::Put {
                        key: "/temp-only".to_string(),
                        value: b"ignored".to_vec(),
                        lease_id: None,
                    }),
                    revision: KvRevision::ZERO,
                },
                &temp_path,
            )
            .expect("write temp entry");
            temp.sync_all().expect("sync temp");
        }

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/committed")
                .expect("committed value")
                .value,
            b"kept"
        );
        assert!(
            recovered.store().get("/temp-only").is_none(),
            "leftover WAL rewrite temp files must not be replayed"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_rejects_oversized_command_before_wal_write() {
        let root = temp_raft_root("sinomo-kv-oversized-command-before-wal");
        let node_id = NodeId::from_legacy_named("node_oversized_command").expect("node id");
        let paths = KvWalPaths::new(&root);
        let mut node = DurableRaftKvNode::open(node_id, paths.clone()).expect("open node");

        let error = node
            .append_local_command(
                1,
                KvCommand::Op(KvOp::Put {
                    key: "/large/value".to_string(),
                    value: vec![0_u8; MAX_KV_VALUE_BYTES + 1],
                    lease_id: None,
                }),
            )
            .expect_err("value limit");

        assert_eq!(
            error,
            KvError::ValueTooLarge {
                actual_bytes: MAX_KV_VALUE_BYTES + 1,
                max_bytes: MAX_KV_VALUE_BYTES,
            }
        );
        assert_eq!(node.last_log_index(), 0);
        assert!(
            !paths.wal.exists(),
            "oversized command should be rejected before WAL creation"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn durable_raft_rejects_oversized_wal_record_before_allocation() {
        let root = temp_raft_root("sinomo-kv-oversized-wal-record");
        let node_id = NodeId::from_legacy_named("node_oversized_wal").expect("node id");
        let paths = KvWalPaths::new(&root);
        {
            let mut node =
                DurableRaftKvNode::open(node_id.clone(), paths.clone()).expect("open node");
            let committed = node
                .append_local_command(
                    1,
                    KvCommand::Op(KvOp::Put {
                        key: "/before-oversized".to_string(),
                        value: b"kept".to_vec(),
                        lease_id: None,
                    }),
                )
                .expect("append committed");
            node.commit_to(committed.index).expect("commit");
        }

        let mut wal = OpenOptions::new()
            .append(true)
            .open(&paths.wal)
            .expect("open wal");
        wal.write_all(&((MAX_WAL_RECORD_BYTES as u32 + 1).to_be_bytes()))
            .expect("write oversized length");
        wal.write_all(b"ignored").expect("write tail");
        drop(wal);

        let recovered = DurableRaftKvNode::open(node_id, paths.clone()).expect("recover node");
        assert_eq!(
            recovered
                .store()
                .get("/before-oversized")
                .expect("valid entry before oversized record")
                .value,
            b"kept"
        );
        let rewritten = std::fs::read(&paths.wal).expect("rewritten wal");
        assert!(
            !rewritten
                .windows(b"ignored".len())
                .any(|window| window == b"ignored"),
            "oversized WAL tail should be truncated without allocating its advertised size"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn snapshot_cache_validates_full_identity_and_stays_bounded() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        // Inode discrimination: a same-(mtime, len) rewrite lands on a fresh inode
        // (temp + atomic rename), so it must miss rather than serve a stale parse.
        let path = temp_raft_root("kv-snapshot-cache-identity").join("snap.sobj");
        let snapshot = Arc::new(InProcessKvStore::new().snapshot());
        let mtime = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000));
        let id = FileIdentity {
            mtime,
            len: 4096,
            ino: 11,
        };
        cache_store_snapshot(&path, id, snapshot.clone());

        assert!(
            cache_lookup_snapshot(&path, id).is_some(),
            "exact identity must hit"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { ino: 12, ..id }).is_none(),
            "same mtime+len but a new inode must miss (closes the mtime-granularity window)"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { len: 4097, ..id }).is_none(),
            "a different length must miss"
        );
        assert!(
            cache_lookup_snapshot(&path, FileIdentity { mtime: None, ..id }).is_none(),
            "an unverifiable mtime must never hit"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .remove(&path);

        // Capacity bound: inserting past the cap must never grow the map beyond it.
        // (Asserted as an invariant — robust even while other tests touch the cache.)
        for i in 0..(FILE_CACHE_CAPACITY + 64) {
            let path = std::path::PathBuf::from(format!("/sinomo-kv-cap-test/{i}.sobj"));
            cache_store_snapshot(
                &path,
                FileIdentity {
                    mtime,
                    len: 1,
                    ino: i as u64,
                },
                snapshot.clone(),
            );
        }
        let len = materialized_snapshot_cache().lock().expect("cache").len();
        assert!(
            len <= FILE_CACHE_CAPACITY,
            "materialized snapshot cache must stay within capacity, got {len}"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-cap-test"));
    }

    #[test]
    fn kv_read_caches_retain_recent_entries_at_capacity() {
        let _guard = KV_READ_CACHE_TEST_MUTEX.lock().expect("cache mutex");
        materialized_snapshot_cache().lock().expect("cache").clear();
        raft_view_cache().lock().expect("raft cache").clear();
        hard_state_cache().lock().expect("hard-state cache").clear();
        KV_READ_CACHE_CLOCK.store(1, Ordering::Relaxed);

        let snapshot = Arc::new(InProcessKvStore::new().snapshot());
        let hard_state = RaftHardState::default();
        let mtime = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2_000));
        for index in 0..FILE_CACHE_CAPACITY.saturating_sub(1) {
            cache_store_snapshot(
                &PathBuf::from(format!("/sinomo-kv-lru-test/100-quiet-{index:04}.sobj")),
                FileIdentity {
                    mtime,
                    len: 1,
                    ino: (index + 1) as u64,
                },
                snapshot.clone(),
            );
            cache_store_hard_state(
                &PathBuf::from(format!(
                    "/sinomo-kv-lru-test/100-quiet-hard-{index:04}.sobj"
                )),
                None,
                hard_state.clone(),
            );
            let quiet_paths =
                KvWalPaths::new(format!("/sinomo-kv-lru-test/100-quiet-raft-{index:04}"));
            cache_store_raft_view(
                &quiet_paths,
                RaftViewIdentity {
                    hard_state: None,
                    wal: None,
                    snapshot: None,
                },
                snapshot.clone(),
            );
        }

        let hot_snapshot_path = PathBuf::from("/sinomo-kv-lru-test/000-hot.sobj");
        let hot_snapshot_identity = FileIdentity {
            mtime,
            len: 1,
            ino: (FILE_CACHE_CAPACITY + 1) as u64,
        };
        cache_store_snapshot(&hot_snapshot_path, hot_snapshot_identity, snapshot.clone());
        let hot_hard_state_path = PathBuf::from("/sinomo-kv-lru-test/000-hot-hard.sobj");
        let hot_hard_state_identity = None;
        cache_store_hard_state(
            &hot_hard_state_path,
            hot_hard_state_identity,
            hard_state.clone(),
        );
        let hot_raft_paths = KvWalPaths::new("/sinomo-kv-lru-test/000-hot-raft");
        let hot_raft_identity = RaftViewIdentity {
            hard_state: None,
            wal: None,
            snapshot: None,
        };
        cache_store_raft_view(&hot_raft_paths, hot_raft_identity, snapshot.clone());

        assert!(
            cache_lookup_snapshot(&hot_snapshot_path, hot_snapshot_identity).is_some(),
            "looking up the hot materialized snapshot should refresh its recency"
        );
        assert!(
            cache_lookup_hard_state(&hot_hard_state_path, hot_hard_state_identity).is_some(),
            "looking up the hot hard-state cache entry should refresh its recency"
        );
        assert!(
            cache_lookup_raft_view(&hot_raft_paths, hot_raft_identity).is_some(),
            "looking up the hot raft view should refresh its recency"
        );

        cache_store_snapshot(
            &PathBuf::from("/sinomo-kv-lru-test/999-extra.sobj"),
            FileIdentity {
                mtime,
                len: 1,
                ino: (FILE_CACHE_CAPACITY + 2) as u64,
            },
            snapshot.clone(),
        );
        cache_store_hard_state(
            &PathBuf::from("/sinomo-kv-lru-test/999-extra-hard.sobj"),
            None,
            hard_state,
        );
        cache_store_raft_view(
            &KvWalPaths::new("/sinomo-kv-lru-test/999-extra-raft"),
            hot_raft_identity,
            snapshot,
        );

        assert!(
            cache_lookup_snapshot(&hot_snapshot_path, hot_snapshot_identity).is_some(),
            "recent materialized snapshot cache entry should survive capacity churn"
        );
        assert!(
            cache_lookup_hard_state(&hot_hard_state_path, hot_hard_state_identity).is_some(),
            "recent hard-state cache entry should survive capacity churn"
        );
        assert!(
            cache_lookup_raft_view(&hot_raft_paths, hot_raft_identity).is_some(),
            "recent raft-view cache entry should survive capacity churn"
        );
        materialized_snapshot_cache()
            .lock()
            .expect("cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
        raft_view_cache()
            .lock()
            .expect("raft cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
        hard_state_cache()
            .lock()
            .expect("hard-state cache")
            .retain(|path, _| !path.starts_with("/sinomo-kv-lru-test"));
    }

    #[test]
    fn concurrent_write_object_file_to_same_path_is_safe() {
        let root = temp_raft_root("kv-concurrent-object-file");
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("object.sobj");
        let value: Vec<u32> = vec![7, 8, 9, 10];

        let threads = 8usize;
        let writes_per_thread = 32usize;
        let barrier = Arc::new(std::sync::Barrier::new(threads));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let path = path.clone();
            let value = value.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..writes_per_thread {
                    write_object_file(&path, &value).expect("write object file");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        let read: Vec<u32> = read_object_file(&path).expect("read object file");
        assert_eq!(
            read, value,
            "concurrent identical writes must leave valid, complete content"
        );
        let leftover: Vec<_> = std::fs::read_dir(&root)
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".tmp-"))
            })
            .collect();
        assert!(
            leftover.is_empty(),
            "no temporary object files should remain, found {leftover:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    fn temp_raft_root(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }
}
