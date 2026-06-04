//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft replication core for tests,
//! local development, and daemon integration.

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::ClusterMemberRecord;
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
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
const MAX_TXN_COMPARES: usize = 1024;
const MAX_TXN_OPS_PER_BRANCH: usize = 4096;
pub const MAX_KV_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_KV_COMMAND_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
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
const MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = 128 * 1024 * 1024;
const RAFT_NODE_LOCK_WAIT: Duration = Duration::from_secs(2);
const RAFT_NODE_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(5);
static OBJECT_FILE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Per-path record of the last content `write_object_file` wrote (or confirmed
/// already on disk), used to skip the read-back + compare when nothing changed.
#[derive(Clone)]
struct ObjectFileWriteMark {
    hash: blake3::Hash,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

/// Cache of last-written object files. The cluster directory holding these WAL /
/// snapshot files can be shared by the main and shadow daemons, so the fast-path
/// skip is guarded by a `stat`: an entry is trusted only when the file's length,
/// mtime, and inode still match what we wrote, so a write by another process is
/// detected and falls back to a real read + compare.
static LAST_WRITTEN_OBJECT_FILES: LazyLock<Mutex<HashMap<PathBuf, ObjectFileWriteMark>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn cached_object_file_mark(path: &Path) -> Option<ObjectFileWriteMark> {
    LAST_WRITTEN_OBJECT_FILES.lock().ok()?.get(path).cloned()
}

fn record_object_file_mark(
    path: &Path,
    hash: blake3::Hash,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
) {
    if let Ok(mut cache) = LAST_WRITTEN_OBJECT_FILES.lock() {
        if cache.len() >= FILE_CACHE_CAPACITY && !cache.contains_key(path) {
            cache.clear();
        }
        cache.insert(
            path.to_path_buf(),
            ObjectFileWriteMark {
                hash,
                len,
                modified,
                ino,
            },
        );
    }
}

/// Captures the `(mtime, inode)` of `path` for the write-mark cache. Returns
/// `(None, 0)` when the file can't be stat'd, which records an entry that can
/// never be trusted (mtime `None`) — forcing the slow path on the next call.
fn file_modified_and_ino(path: &Path) -> (Option<SystemTime>, u64) {
    match fs::metadata(path) {
        Ok(metadata) => (metadata.modified().ok(), file_ino(&metadata)),
        Err(_) => (None, 0),
    }
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
    )
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
        KvCommand::PutClusterMember(member) => {
            let op = KvOp::put_serde(cluster_member_key(&member.node_id), member, None)?;
            validate_kv_op_payload(&op)?;
            validate_kv_payload_total(kv_op_payload_bytes(&op))
        }
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

    /// Reads only the persisted hard state (term/vote/leader/commit) without
    /// opening the full raft node. Opening the node replays the entire WAL — up to
    /// the retained tail, ~MBs — which is wasteful for read-only leadership/term
    /// queries that only need this tiny, atomically-rewritten file. The value is a
    /// lock-free point read: it reflects the last durably persisted hard state and
    /// is never torn (writes go through a temp-file rename). Defaults when absent.
    pub fn read_persisted_hard_state(&self) -> Result<RaftHardState, KvError> {
        Ok(read_optional_object::<RaftHardState>(&self.hard_state)?.unwrap_or_default())
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
}

struct RaftViewCacheEntry {
    identity: RaftViewIdentity,
    snapshot: Arc<KvSnapshot>,
}

static MATERIALIZED_SNAPSHOT_CACHE: OnceLock<
    Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>>,
> = OnceLock::new();
static RAFT_VIEW_CACHE: OnceLock<Mutex<HashMap<PathBuf, RaftViewCacheEntry>>> = OnceLock::new();

fn materialized_snapshot_cache() -> &'static Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>>
{
    MATERIALIZED_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn raft_view_cache() -> &'static Mutex<HashMap<PathBuf, RaftViewCacheEntry>> {
    RAFT_VIEW_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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
/// against unbounded growth under pathological path churn. On overflow the whole
/// map is dropped (cold cache), which is always correct — these caches are pure
/// memoization validated against the file on every lookup.
const FILE_CACHE_CAPACITY: usize = 4096;

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

fn raft_view_identity(paths: &KvWalPaths) -> Option<RaftViewIdentity> {
    Some(RaftViewIdentity {
        hard_state: optional_file_identity(&paths.hard_state)?,
        wal: optional_file_identity(&paths.wal)?,
        snapshot: optional_file_identity(&paths.snapshot)?,
    })
}

fn cache_lookup_snapshot(path: &Path, identity: FileIdentity) -> Option<Arc<KvSnapshot>> {
    let guard = materialized_snapshot_cache().lock().ok()?;
    let entry = guard.get(path)?;
    // mtime is the primary key; require it to be present so we never serve a hit
    // we cannot validate, then require the full identity (mtime, len, inode).
    if identity.mtime.is_some() && entry.identity == identity {
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
        if guard.len() >= FILE_CACHE_CAPACITY && !guard.contains_key(path) {
            guard.clear();
        }
        guard.insert(
            path.to_path_buf(),
            MaterializedSnapshotCacheEntry { identity, snapshot },
        );
    }
}

fn cache_lookup_raft_view(paths: &KvWalPaths, identity: RaftViewIdentity) -> Option<KvSnapshot> {
    let guard = raft_view_cache().lock().ok()?;
    let entry = guard.get(&paths.root)?;
    if entry.identity != identity {
        return None;
    }
    let snapshot = (*entry.snapshot).clone();
    drop(guard);
    // Re-stat after cloning the cached view so a concurrent append/rewrite cannot
    // slip between the first identity read and the return. If the files change
    // after this check, the read linearizes before that change.
    (raft_view_identity(paths) == Some(identity)).then_some(snapshot)
}

fn cache_store_raft_view(paths: &KvWalPaths, identity: RaftViewIdentity, snapshot: KvSnapshot) {
    if let Ok(mut guard) = raft_view_cache().lock() {
        if guard.len() >= FILE_CACHE_CAPACITY && !guard.contains_key(&paths.root) {
            guard.clear();
        }
        guard.insert(
            paths.root.clone(),
            RaftViewCacheEntry {
                identity,
                snapshot: Arc::new(snapshot),
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

    pub fn read_snapshot(&self) -> Result<KvSnapshot, KvError> {
        let identity = file_identity(&self.path);
        if let Some(identity) = identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok((*cached).clone());
        }
        let snapshot = read_object_file::<KvSnapshot>(&self.path)?;
        if let Some(identity) = identity {
            cache_store_snapshot(&self.path, identity, Arc::new(snapshot.clone()));
        }
        Ok(snapshot)
    }

    pub fn read_store(&self) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot().map(InProcessKvStore::restore)
    }

    /// Returns the snapshot that should actually be persisted: the input with its
    /// event history compacted to the configured materialized-event limit.
    fn materialized(&self, snapshot: &KvSnapshot) -> KvSnapshot {
        match self.max_materialized_events {
            Some(max_events) if snapshot.events.len() > max_events => {
                let mut snapshot = snapshot.clone();
                compact_kv_snapshot_event_history(&mut snapshot, max_events);
                snapshot
            }
            _ => snapshot.clone(),
        }
    }

    /// Persists `snapshot` and refreshes the process-wide read cache so subsequent
    /// reads hit memory rather than re-reading the file.
    fn write_and_cache(&self, snapshot: KvSnapshot) -> Result<(), KvError> {
        write_object_file(&self.path, &snapshot)?;
        if let Some(identity) = file_identity(&self.path) {
            cache_store_snapshot(&self.path, identity, Arc::new(snapshot));
        }
        Ok(())
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.write_and_cache(self.materialized(snapshot))
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        let desired = self.materialized(snapshot);
        // Skip the rewrite when the durable key/value/lease state is unchanged, even
        // if the snapshot revision, per-entry revision metadata, or watch-event tail
        // advanced. Re-putting an identical value bumps modify_revision/version and
        // appends events, which would otherwise rewrite the whole (~25 KiB+)
        // materialized file for no durable change. The materialized snapshot is only
        // a fast-restore fallback for a cold node with an empty WAL (the WAL is the
        // source of truth for revision/version/events), so persisting stale
        // bookkeeping there is safe.
        if self
            .read_snapshot()
            .is_ok_and(|existing| kv_snapshot_durable_eq(&existing, &desired))
        {
            return Ok(false);
        }
        self.write_and_cache(desired)?;
        Ok(true)
    }

    pub fn read_store_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<InProcessKvStore, KvError> {
        if paths.root.exists() {
            if let Some(identity) = raft_view_identity(&paths)
                && let Some(snapshot) = cache_lookup_raft_view(&paths, identity)
            {
                return Ok(InProcessKvStore::restore(snapshot));
            }
            let node =
                match DurableRaftKvNode::try_open_with_materialized_cache(node_id, paths, self) {
                    Ok(node) => node,
                    Err(KvError::Busy { .. }) if self.exists() => {
                        return self.read_store();
                    }
                    Err(error) => return Err(error),
                };
            let store = node.store().clone();
            self.write_snapshot_if_changed(&store.snapshot())?;
            if let Some(identity) = raft_view_identity(&node.paths) {
                cache_store_raft_view(&node.paths, identity, store.snapshot());
            }
            return Ok(store);
        }
        if self.exists() {
            return self.read_store();
        }
        Ok(InProcessKvStore::new())
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
        Ok(self.current_store(node_id)?.get(key).cloned())
    }

    pub fn current_get_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<T>, KvError> {
        self.current_get(node_id, key)?
            .map(|entry| entry.decode())
            .transpose()
    }

    pub fn current_range_prefix(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<KvEntry>, KvError> {
        Ok(self.current_store(node_id)?.range_prefix(prefix))
    }

    pub fn current_range_prefix_limited(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.current_store(node_id)?
            .range_prefix_limited(prefix, max_entries)
    }

    pub fn current_range_prefix_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<T>, KvError> {
        self.current_range_prefix(node_id, prefix)?
            .into_iter()
            .map(|entry| entry.decode())
            .collect()
    }

    pub fn current_snapshot(&self, node_id: NodeId) -> Result<KvSnapshot, KvError> {
        self.current_store(node_id).map(|store| store.snapshot())
    }

    pub fn cached_snapshot(&self) -> Result<KvSnapshot, KvError> {
        self.materialized.read_snapshot()
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.materialized.write_snapshot(snapshot)
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
    PutClusterMember(ClusterMemberRecord),
}

#[derive(Clone, Debug, Default)]
pub struct InProcessKvStore {
    revision: KvRevision,
    compacted_revision: KvRevision,
    entries: BTreeMap<String, KvEntry>,
    events: Vec<KvEvent>,
    leases: BTreeMap<LeaseId, KvLease>,
    lease_keys: BTreeMap<LeaseId, BTreeSet<String>>,
    watches: BTreeMap<WatchId, KvWatchState>,
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
        let mut entries = Vec::new();
        for (key, entry) in self.entries.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            entries.push(entry.clone());
        }
        entries
    }

    pub fn range_prefix_limited(
        &self,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        for (key, entry) in self.entries.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if entries.len() >= max_entries {
                return Err(KvError::RangeLimit {
                    prefix: prefix.to_string(),
                    max_entries,
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
            KvCommand::PutClusterMember(member) => self
                .put_cluster_member(member)
                .map(|revision| (revision, None)),
        }
    }

    pub fn put_cluster_member(
        &mut self,
        member: ClusterMemberRecord,
    ) -> Result<KvRevision, KvError> {
        self.put_serde(cluster_member_key(&member.node_id), &member, None)
    }

    pub fn read_cluster_member(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<ClusterMemberRecord>, KvError> {
        self.get_serde(&cluster_member_key(node_id))
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
                .watches
                .remove(&watch_id)
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
        Ok(watch.pending.drain(..).collect())
    }

    pub fn cancel_watch(&mut self, watch_id: WatchId) -> Result<bool, KvError> {
        Ok(self.watches.remove(&watch_id).is_some())
    }

    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries: self.entries.values().cloned().collect(),
            leases: self.leases.values().cloned().collect(),
            events: self.events.clone(),
        }
    }

    pub fn restore(snapshot: KvSnapshot) -> Self {
        let entries: BTreeMap<_, _> = snapshot
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        let lease_keys = build_lease_key_index(&entries);
        Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
            entries,
            events: snapshot.events,
            leases: snapshot
                .leases
                .into_iter()
                .map(|lease| (lease.lease_id, lease))
                .collect(),
            lease_keys,
            watches: BTreeMap::new(),
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
        compact_snapshot_event_history(&mut self.events, &mut self.compacted_revision, max_events);
    }

    pub fn tick_leases(&mut self, ticks: u64) -> Result<Vec<LeaseId>, KvError> {
        let mut expired = Vec::new();
        for lease in self.leases.values_mut() {
            lease.remaining_ticks = lease.remaining_ticks.saturating_sub(ticks);
            if lease.remaining_ticks == 0 {
                expired.push(lease.lease_id);
            }
        }

        for lease_id in &expired {
            self.apply_op(KvOp::LeaseRevoke {
                lease_id: *lease_id,
            })?;
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
                self.leases
                    .insert(lease_id, KvLease::new(lease_id, ttl_ticks));
                Ok(KvOpResponse::LeaseGrant { lease_id })
            }
            KvOp::LeaseKeepAlive { lease_id } => {
                let lease = self
                    .leases
                    .get_mut(&lease_id)
                    .ok_or(KvError::UnknownLease { lease_id })?;
                lease.remaining_ticks = lease.ttl_ticks;
                Ok(KvOpResponse::LeaseKeepAlive { lease_id })
            }
            KvOp::LeaseRevoke { lease_id } => {
                self.leases.remove(&lease_id);
                let keys: Vec<String> = self
                    .lease_keys
                    .remove(&lease_id)
                    .map(|keys| keys.into_iter().collect())
                    .unwrap_or_default();
                let deleted_count = keys.len();
                for key in keys {
                    self.apply_delete(key)?;
                }
                Ok(KvOpResponse::LeaseRevoke {
                    lease_id,
                    deleted_count,
                })
            }
        }
    }

    fn validate_txn_shape(&self, txn: &KvTxn) -> Result<(), KvError> {
        if txn.compares.len() > MAX_TXN_COMPARES
            || txn.success.len() > MAX_TXN_OPS_PER_BRANCH
            || txn.failure.len() > MAX_TXN_OPS_PER_BRANCH
        {
            return Err(KvError::TxnLimit {
                compares: txn.compares.len(),
                success_ops: txn.success.len(),
                failure_ops: txn.failure.len(),
                max_compares: MAX_TXN_COMPARES,
                max_ops_per_branch: MAX_TXN_OPS_PER_BRANCH,
            });
        }
        Ok(())
    }

    fn validate_ops(&self, ops: &[KvOp]) -> Result<(), KvError> {
        let mut active_leases = self.leases.keys().copied().collect::<BTreeSet<_>>();
        for op in ops {
            validate_kv_op_payload(op)?;
            match op {
                KvOp::Put {
                    lease_id: Some(lease_id),
                    ..
                } => {
                    if !active_leases.contains(lease_id) {
                        return Err(KvError::UnknownLease {
                            lease_id: *lease_id,
                        });
                    }
                }
                KvOp::Put { lease_id: None, .. }
                | KvOp::Delete { .. }
                | KvOp::DeletePrefix { .. } => {}
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
                    active_leases.remove(lease_id);
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
        let keys = self.keys_with_prefix(&prefix);
        let deleted_count = keys.len();
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        })
    }

    fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        let mut keys = Vec::new();
        for key in self.entries.range(prefix.to_string()..).map(|(key, _)| key) {
            if !key.starts_with(prefix) {
                break;
            }
            keys.push(key.clone());
        }
        keys
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
        for watch in self.watches.values_mut() {
            if event.revision >= watch.watch.start_revision
                && event.key.starts_with(&watch.watch.prefix)
            {
                watch.pending.push_back(event.clone());
                compact_watch_pending(watch);
            }
        }
        self.events.push(event);
        self.trim_event_history();
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
/// `modify_revision`, `version`), lease decay (`remaining_ticks`, `granted_at`),
/// and the watch-event tail. Two snapshots that are durable-equal materialize to
/// the same observable KV state, so there is no need to rewrite the persisted file.
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
        .map(|lease| (&lease.lease_id, lease.ttl_ticks));
    let right_leases = right
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks));
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

#[derive(Clone, Debug)]
struct KvWatchState {
    watch: KvWatch,
    pending: VecDeque<KvEvent>,
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

    pub fn put_cluster_member(
        &mut self,
        member: ClusterMemberRecord,
    ) -> Result<KvRevision, KvError> {
        self.apply_as_leader(KvCommand::PutClusterMember(member))
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

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    /// Loads persisted WAL entries on open. Entries beyond the installed snapshot
    /// are appended (and later applied) as normal; already-applied entries are kept
    /// in the log as a contiguous replication tail (not re-applied — they are
    /// reflected in the snapshot/store). Any non-contiguous already-applied entry is
    /// skipped, degrading safely to snapshot-based replication rather than corrupting
    /// the log.
    fn load_persisted_entries(&mut self, entries: Vec<KvLogEntry>) -> Result<(), KvError> {
        for entry in entries {
            if entry.index == 0 {
                continue;
            }
            if entry.index > self.applied_index {
                self.append_entries(vec![entry])?;
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
            }
        }
        self.log.sort_by_key(|entry| entry.index);
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<KvLogEntry>) -> Result<Vec<KvLogEntry>, KvError> {
        let mut appended = Vec::new();
        for entry in entries {
            validate_kv_command_payload(&entry.command)?;
            if entry.index <= self.applied_index {
                continue;
            }
            if let Some(existing_position) = self
                .log
                .iter()
                .position(|candidate| candidate.index == entry.index)
            {
                if self.log[existing_position].term != entry.term {
                    self.log.truncate(existing_position);
                    self.log.push(entry.clone());
                    appended.push(entry);
                }
            } else {
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

    fn handle_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesOutcome, KvError> {
        if request.term < self.hard_state.current_term {
            return Ok(RaftAppendEntriesOutcome {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
            });
        }
        if request.term > self.hard_state.current_term {
            self.hard_state.voted_for = None;
        }
        self.hard_state.current_term = request.term;
        self.hard_state.leader_id = Some(request.leader_id.clone());
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return Ok(RaftAppendEntriesOutcome {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
            });
        }
        let appended_entries = self.append_entries(request.entries)?;
        let commit_index = request.leader_commit.min(self.last_log_index());
        self.commit_to(commit_index)?;
        Ok(RaftAppendEntriesOutcome {
            response: RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: true,
                match_index: self.last_log_index(),
            },
            appended_entries,
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
        let entries = self
            .log
            .iter()
            .filter(|entry| entry.index > self.applied_index && entry.index <= commit_index)
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let (revision, txn_response) = self.store.apply_command_with_response(entry.command)?;
            if let Some(log_entry) = self
                .log
                .iter_mut()
                .find(|candidate| candidate.index == entry.index)
            {
                log_entry.revision = revision;
            }
            if let Some(response) = txn_response {
                self.txn_responses.insert(entry.index, response);
            }
            self.applied_index = entry.index;
        }
        self.hard_state.commit_index = commit_index;
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
struct RaftAppendEntriesOutcome {
    response: RaftAppendEntriesResponse,
    appended_entries: Vec<KvLogEntry>,
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
        let hard_state: RaftHardState =
            read_optional_object(&paths.hard_state)?.unwrap_or_default();
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
        node.load_persisted_entries(entries)?;
        let commit_index = node.hard_state.commit_index.min(node.last_log_index());
        node.commit_to(commit_index)?;
        if truncated {
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
        let mut node = match Self::open_inner(
            node_id.clone(),
            paths.clone(),
            discard_uncommitted_tail,
            lock_wait,
        ) {
            Ok(node) => node,
            Err(error) if cache.exists() && !kv_error_is_busy(&error) => {
                quarantine_unreadable_raft_view(&paths);
                Self::open_inner(node_id, paths, discard_uncommitted_tail, lock_wait)?
            }
            Err(error) => return Err(error),
        };
        if node.applied_index() == 0 && node.last_log_index() == 0 && cache.exists() {
            node.restore_snapshot(RaftSnapshot {
                last_included_index: 0,
                last_included_term: 0,
                kv: cache.read_snapshot()?,
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
        let hard_state_before = self.node.hard_state.clone();
        let outcome = self.node.handle_append_entries(request)?;
        if outcome.response.success {
            append_wal_entries(&self.paths.wal, &outcome.appended_entries)?;
        }
        self.persist_hard_state_if_changed(&hard_state_before)?;
        Ok(outcome.response)
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let response = self.node.handle_request_vote(request)?;
        self.persist_hard_state_if_changed(&hard_state_before)?;
        Ok(response)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<bool, KvError> {
        if !self.node.observe_term(term) {
            return Ok(false);
        }
        self.persist_hard_state()?;
        Ok(true)
    }

    pub fn become_leader(&mut self, term: u64) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.current_term = term.max(self.node.hard_state.current_term);
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        self.persist_hard_state_if_changed(&hard_state_before)
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        self.persist_hard_state_if_changed(&hard_state_before)
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            command,
            revision: KvRevision::ZERO,
        };
        self.node.hard_state.current_term = term;
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        self.node.append_entries(vec![entry.clone()])?;
        append_wal_entries(&self.paths.wal, std::slice::from_ref(&entry))?;
        self.persist_hard_state_if_changed(&hard_state_before)?;
        Ok(entry)
    }

    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        if !self.node.discard_uncommitted_tail() {
            return Ok(false);
        }
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state_if_changed(&hard_state_before)?;
        Ok(true)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.commit_to(commit_index)?;
        self.persist_hard_state_if_changed(&hard_state_before)
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        let hard_state_before = self.node.hard_state.clone();
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
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state_if_changed(&hard_state_before)?;
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot);
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state_if_changed(&hard_state_before)
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        write_object_file(&self.paths.hard_state, &self.node.hard_state)
    }

    fn persist_hard_state_if_changed(&self, before: &RaftHardState) -> Result<(), KvError> {
        if self.node.hard_state == *before {
            return Ok(());
        }
        self.persist_hard_state()
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
    if !path.exists() {
        return Ok(WalRead::default());
    }
    let mut file = File::open(path).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| KvError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
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

fn write_wal_record(file: &mut File, entry: &KvLogEntry, path: &Path) -> Result<(), KvError> {
    let record = sinomo_object_serde::to_bytes(entry, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| KvError::Serialization {
            detail: source.to_string(),
        })?;
    let record_len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
        detail: format!("wal record for {} exceeds u32 length", path.display()),
    })?;
    file.write_all(&record_len.to_be_bytes())
        .and_then(|()| file.write_all(&record))
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

fn rewrite_wal_entries(path: &Path, entries: &[KvLogEntry]) -> Result<(), KvError> {
    // Callers hold the per-node raft lock. Stage through a temp file instead of
    // truncating the live WAL so a failed rewrite leaves the previous log intact.
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
    {
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
        file.sync_all().map_err(|source| KvError::Io {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let result = replace_file(&temp_path, path).and_then(|()| sync_directory(parent));
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn read_optional_object<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, KvError> {
    if !path.exists() {
        return Ok(None);
    }
    read_object_file(path).map(Some)
}

fn read_object_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, KvError> {
    let bytes = read_object_file_bytes(path)?;
    sinomo_object_serde::from_bytes(&bytes).map_err(|source| KvError::Serialization {
        detail: format!("{}: {source}", path.display()),
    })
}

fn read_object_file_bytes(path: &Path) -> Result<Vec<u8>, KvError> {
    let file = File::open(path).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let len = file
        .metadata()
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    if len > MAX_OBJECT_FILE_READ_BYTES {
        return Err(KvError::Io {
            path: path.display().to_string(),
            detail: format!("object file is {len} bytes; max is {MAX_OBJECT_FILE_READ_BYTES}"),
        });
    }

    let mut bytes = Vec::with_capacity(len as usize);
    let mut limited = file.take(MAX_OBJECT_FILE_READ_BYTES.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if bytes.len() as u64 > MAX_OBJECT_FILE_READ_BYTES {
        return Err(KvError::Io {
            path: path.display().to_string(),
            detail: format!("object file read exceeded max of {MAX_OBJECT_FILE_READ_BYTES} bytes"),
        });
    }
    Ok(bytes)
}

fn read_existing_object_file_for_compare(path: &Path, expected_len: u64) -> Option<Vec<u8>> {
    let file = File::open(path).ok()?;
    if file.metadata().ok()?.len() != expected_len {
        return None;
    }
    let mut bytes = Vec::with_capacity(expected_len as usize);
    let mut limited = file.take(expected_len.saturating_add(1));
    limited.read_to_end(&mut bytes).ok()?;
    (bytes.len() as u64 == expected_len).then_some(bytes)
}

fn write_object_file<T: Serialize>(path: &Path, value: &T) -> Result<(), KvError> {
    let content = sinomo_object_serde::to_bytes(value, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })?;
    let content_hash = blake3::hash(&content);

    // Fast path: if we last wrote this exact content and the file is unchanged
    // since (length + mtime + inode still match our record), skip without
    // reading it back.
    if let Some(mark) = cached_object_file_mark(path)
        && mark.hash == content_hash
        && let Ok(metadata) = fs::metadata(path)
        && metadata.len() == mark.len
        && metadata.modified().ok() == mark.modified
        && file_ino(&metadata) == mark.ino
    {
        return Ok(());
    }

    // Slow path: read the file back and compare only when the length could match.
    // Handles a cold cache and files changed by another writer (e.g. a shadow
    // daemon sharing the cluster dir) without reading sparse/corrupt oversized
    // files that are already known to differ.
    if read_existing_object_file_for_compare(path, content.len() as u64)
        .is_some_and(|existing| existing == content)
    {
        // Already current: record the mark so subsequent calls take the fast path.
        let (modified, ino) = file_modified_and_ino(path);
        record_object_file_mark(path, content_hash, content.len() as u64, modified, ino);
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
        .unwrap_or("json");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_FILE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|source| KvError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(&content).map_err(|source| KvError::Io {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.sync_all().map_err(|source| KvError::Io {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    replace_file(&temp_path, path)?;
    sync_directory(parent)?;
    let (modified, ino) = file_modified_and_ino(path);
    record_object_file_mark(path, content_hash, content.len() as u64, modified, ino);
    Ok(())
}

fn replace_file(temp_path: &Path, path: &Path) -> Result<(), KvError> {
    match fs::rename(temp_path, path) {
        Ok(()) => Ok(()),
        Err(source)
            if path.exists()
                && matches!(
                    source.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                ) =>
        {
            fs::remove_file(path).map_err(|remove_source| KvError::Io {
                path: path.display().to_string(),
                detail: format!(
                    "replace failed with {}; additionally failed to remove existing file: {}",
                    source, remove_source
                ),
            })?;
            fs::rename(temp_path, path).map_err(|rename_source| KvError::Io {
                path: format!("{} -> {}", temp_path.display(), path.display()),
                detail: format!(
                    "replace after removing existing file failed: {}",
                    rename_source
                ),
            })
        }
        Err(source) => Err(KvError::Io {
            path: format!("{} -> {}", temp_path.display(), path.display()),
            detail: source.to_string(),
        }),
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
    loop {
        match lock.try_lock_exclusive() {
            Ok(()) => return Ok(()),
            Err(source) if file_lock_is_busy(&source) => {
                if wait.is_zero() || Instant::now() >= deadline {
                    return Err(KvError::Busy {
                        path: path.display().to_string(),
                    });
                }
                thread::sleep(RAFT_NODE_LOCK_RETRY_INTERVAL);
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
    #[cfg(windows)]
    {
        let _ = path;
        return Ok(());
    }
    #[cfg(not(windows))]
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

pub fn cluster_member_key(node_id: &NodeId) -> String {
    format!("/cluster/members/{node_id}")
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
    #[error("missing committed raft entry at index {index}")]
    MissingCommittedEntry { index: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterMemberRecord, Id};

    type ClusterId = Id;

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct TestRecord {
        name: String,
        count: u64,
    }

    #[test]
    fn write_object_file_skips_identical_and_rewrites_after_external_change() {
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
    fn txn_rejects_oversized_compares_and_branches() {
        let mut store = InProcessKvStore::new();
        let compares = (0..=MAX_TXN_COMPARES)
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
                compares: MAX_TXN_COMPARES + 1,
                success_ops: 0,
                failure_ops: 0,
                max_compares: MAX_TXN_COMPARES,
                max_ops_per_branch: MAX_TXN_OPS_PER_BRANCH,
            }
        );

        let failure = (0..=MAX_TXN_OPS_PER_BRANCH)
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
                failure_ops: MAX_TXN_OPS_PER_BRANCH + 1,
                max_compares: MAX_TXN_COMPARES,
                max_ops_per_branch: MAX_TXN_OPS_PER_BRANCH,
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

    #[test]
    fn cluster_member_records_are_stored_under_cluster_namespace() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let member = ClusterMemberRecord::local_voter(
            ClusterId::from_legacy_named("cluster_home").expect("cluster"),
            node_id.clone(),
            "desktop",
            "pk_desktop",
            OffsetDateTime::now_utc(),
        );
        let mut store = InProcessKvStore::new();

        store
            .put_cluster_member(member.clone())
            .expect("put member");

        assert!(store.get(&cluster_member_key(&node_id)).is_some());
        assert_eq!(
            store
                .read_cluster_member(&node_id)
                .expect("member")
                .expect("exists"),
            member
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
