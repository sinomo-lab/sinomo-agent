use crate::internal::*;

pub const CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT: Duration = Duration::from_secs(30);

/// Locks the cluster raft commit sequence (append → replicate → commit) against
/// concurrent committers from other tasks and other processes sharing this data
/// dir. The quorum commit is a multi-step protocol over separately-locked store
/// operations; without this outer lock two concurrent committers race: each
/// append opens the WAL discarding the other's uncommitted entry, so the first
/// committer's commit step no longer finds its entry (`missing committed raft
/// entry`) and the publishers livelock retrying against each other.
///
/// The returned file holds an exclusive advisory lock released on drop.
pub fn lock_cluster_kv_commit_sequence(
    paths: &ClusterKvPaths,
    wait: Duration,
) -> Result<File, KvError> {
    fs::create_dir_all(&paths.raft).map_err(|source| KvError::Io {
        path: paths.raft.display().to_string(),
        detail: source.to_string(),
    })?;
    let lock_path = paths.raft.join("commit-sequence.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| KvError::Io {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    lock_node_exclusive(&lock, &lock_path, wait)?;
    Ok(lock)
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
