//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft replication core for tests,
//! local development, and daemon integration.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sinomo_core::ClusterMemberRecord;
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use time::OffsetDateTime;

pub const KV_OBJECT_VERSION_ID: u16 = 1;
pub const KV_WAL_OBJECT_VERSION_ID: u16 = 2;
const MAX_RETAINED_EVENTS: usize = 4096;
const MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
static OBJECT_FILE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    #[serde(with = "base64_bytes")]
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
        #[serde(with = "base64_bytes")]
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
        #[serde(with = "base64_bytes")]
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
        #[serde(with = "base64_bytes")]
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

mod base64_bytes {
    use super::*;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        URL_SAFE_NO_PAD
            .decode(value)
            .map_err(serde::de::Error::custom)
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvMaterializedCache {
    path: PathBuf,
}

impl KvMaterializedCache {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn read_snapshot(&self) -> Result<KvSnapshot, KvError> {
        read_object_file(&self.path)
    }

    pub fn read_store(&self) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot().map(InProcessKvStore::restore)
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        write_object_file(&self.path, snapshot)
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        if self
            .read_snapshot()
            .is_ok_and(|existing| existing == *snapshot)
        {
            return Ok(false);
        }
        self.write_snapshot(snapshot)?;
        Ok(true)
    }

    pub fn read_store_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<InProcessKvStore, KvError> {
        if paths.root.exists() {
            let node = DurableRaftKvNode::open_with_materialized_cache(node_id, paths, self)?;
            let store = node.store().clone();
            self.write_snapshot_if_changed(&store.snapshot())?;
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

    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        self.entries.get(key)
    }

    pub fn range_prefix(&self, prefix: &str) -> Vec<KvEntry> {
        self.entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(_, entry)| entry.clone())
            .collect()
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
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded { txn.success } else { txn.failure };
        self.validate_ops(&ops)?;
        let mut staged = self.clone();
        let mut responses = Vec::with_capacity(ops.len());
        let mut revision = staged.revision;

        for op in ops {
            let response = staged.apply_op(op)?;
            revision = staged.revision;
            responses.push(response);
        }

        *self = staged;
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

        let watch_id = WatchId(self.next_watch_id.max(1));
        self.next_watch_id = watch_id.0.saturating_add(1);
        let prefix = prefix.into();
        let backlog = self
            .events
            .iter()
            .filter(|event| event.revision >= start_revision && event.key.starts_with(&prefix))
            .cloned()
            .collect();
        self.watches.insert(
            watch_id,
            KvWatchState {
                watch: KvWatch {
                    watch_id,
                    prefix,
                    start_revision,
                },
                pending: backlog,
            },
        );
        Ok(watch_id)
    }

    pub fn drain_watch(&mut self, watch_id: WatchId) -> Result<Vec<KvEvent>, KvError> {
        let watch = self
            .watches
            .get_mut(&watch_id)
            .ok_or(KvError::UnknownWatch { watch_id })?;
        Ok(watch.pending.drain(..).collect())
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
        Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| (entry.key.clone(), entry))
                .collect(),
            events: snapshot.events,
            leases: snapshot
                .leases
                .into_iter()
                .map(|lease| (lease.lease_id, lease))
                .collect(),
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
                    .entries
                    .iter()
                    .filter(|(_, entry)| entry.lease_id == Some(lease_id))
                    .map(|(key, _)| key.clone())
                    .collect();
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

    fn validate_ops(&self, ops: &[KvOp]) -> Result<(), KvError> {
        let mut staged = self.clone();
        for op in ops {
            staged.apply_op(op.clone())?;
        }
        Ok(())
    }

    fn apply_put(
        &mut self,
        key: String,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOpResponse, KvError> {
        if let Some(lease_id) = lease_id {
            if !self.leases.contains_key(&lease_id) {
                return Err(KvError::UnknownLease { lease_id });
            }
        }

        let revision = self.bump_revision();
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
        self.entries.insert(key.clone(), entry);
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Put { value },
        });
        Ok(KvOpResponse::Put { key, revision })
    }

    fn apply_delete(&mut self, key: String) -> Result<KvOpResponse, KvError> {
        if self.entries.remove(&key).is_none() {
            return Ok(KvOpResponse::Delete {
                key,
                deleted: false,
            });
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
        let keys: Vec<String> = self
            .entries
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        let deleted_count = keys.len();
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        })
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
            }
        }
        self.events.push(event);
        self.trim_event_history();
    }

    fn trim_event_history(&mut self) {
        let excess = self.events.len().saturating_sub(MAX_RETAINED_EVENTS);
        if excess == 0 {
            return;
        }
        let compacted = self.events[excess - 1].revision;
        self.events.drain(..excess);
        self.compacted_revision = self.compacted_revision.max(compacted);
    }
}

#[derive(Clone, Debug)]
struct KvWatchState {
    watch: KvWatch,
    pending: VecDeque<KvEvent>,
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
        let outcome = self.propose_command(KvCommand::Txn(txn.clone()))?;
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
        self.node_mut(node_id)?.handle_append_entries(request)
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

    fn append_entries(&mut self, entries: Vec<KvLogEntry>) -> Result<(), KvError> {
        for entry in entries {
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
                    self.log.push(entry);
                }
            } else {
                let expected = self.last_log_index().saturating_add(1);
                if entry.index != expected {
                    return Err(KvError::LogGap {
                        expected,
                        actual: entry.index,
                    });
                }
                self.log.push(entry);
            }
        }
        Ok(())
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
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        if request.term < self.hard_state.current_term {
            return Ok(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }
        if request.term > self.hard_state.current_term {
            self.hard_state.voted_for = None;
        }
        self.hard_state.current_term = request.term;
        self.hard_state.leader_id = Some(request.leader_id.clone());
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return Ok(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }
        self.append_entries(request.entries)?;
        let commit_index = request.leader_commit.min(self.last_log_index());
        self.commit_to(commit_index)?;
        Ok(RaftAppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            match_index: self.last_log_index(),
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

#[derive(Debug)]
pub struct DurableRaftKvNode {
    node_id: NodeId,
    paths: KvWalPaths,
    _lock: File,
    node: ReplicatedKvNode,
}

impl DurableRaftKvNode {
    pub fn open(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, false)
    }

    pub fn open_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, true)
    }

    fn open_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
    ) -> Result<Self, KvError> {
        paths.ensure()?;
        let lock = open_node_lock(&paths)?;
        lock.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                KvError::Busy {
                    path: paths.lock.display().to_string(),
                }
            } else {
                KvError::Io {
                    path: paths.lock.display().to_string(),
                    detail: source.to_string(),
                }
            }
        })?;
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
        let WalRead { entries, truncated } = read_wal_entries(
            &paths.wal,
            discard_uncommitted_tail.then_some(committed_index),
        )?;
        for entry in entries {
            node.append_entries(vec![entry])?;
        }
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
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false)
    }

    pub fn open_discarding_uncommitted_tail_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, true)
    }

    fn open_with_materialized_cache_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
    ) -> Result<Self, KvError> {
        let mut node =
            match Self::open_inner(node_id.clone(), paths.clone(), discard_uncommitted_tail) {
                Ok(node) => node,
                Err(error) if cache.exists() && !kv_error_is_busy(&error) => {
                    quarantine_unreadable_raft_view(&paths);
                    Self::open_inner(node_id, paths, discard_uncommitted_tail)?
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
        let entries = request.entries.clone();
        let response = self.node.handle_append_entries(request)?;
        if response.success {
            append_wal_entries(&self.paths.wal, &entries)?;
        }
        self.persist_hard_state()?;
        Ok(response)
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let response = self.node.handle_request_vote(request)?;
        self.persist_hard_state()?;
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
        self.node.hard_state.current_term = term.max(self.node.hard_state.current_term);
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        self.persist_hard_state()
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        self.node.hard_state.leader_id = None;
        self.persist_hard_state()
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
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
        self.persist_hard_state()?;
        Ok(entry)
    }

    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        if !self.node.discard_uncommitted_tail() {
            return Ok(false);
        }
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state()?;
        Ok(true)
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        self.node.commit_to(commit_index)?;
        self.persist_hard_state()
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: self.node.store.snapshot(),
        };
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot.clone());
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state()?;
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot);
        rewrite_wal_entries(&self.paths.wal, &self.node.log)?;
        self.persist_hard_state()
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        write_object_file(&self.paths.hard_state, &self.node.hard_state)
    }
}

#[derive(Clone, Debug, Default)]
struct WalRead {
    entries: Vec<KvLogEntry>,
    truncated: bool,
}

fn read_wal_entries(path: &Path, max_index: Option<u64>) -> Result<WalRead, KvError> {
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
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut entries = Vec::new();
    let mut truncated = false;
    let mut offset = 0;
    let mut record_number = 0;
    while offset < bytes.len() {
        record_number += 1;
        if bytes.len().saturating_sub(offset) < 4 {
            truncated = true;
            break;
        }
        let len = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;
        if len == 0 || len > MAX_WAL_RECORD_BYTES || bytes.len().saturating_sub(offset) < len {
            truncated = true;
            break;
        }
        match sinomo_object_serde::from_bytes(&bytes[offset..offset + len]) {
            Ok(entry) => {
                let entry: KvLogEntry = entry;
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
        offset += len;
    }
    Ok(WalRead { entries, truncated })
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| KvError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
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

fn read_optional_object<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, KvError> {
    if !path.exists() {
        return Ok(None);
    }
    read_object_file(path).map(Some)
}

fn read_object_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, KvError> {
    let bytes = fs::read(path).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    sinomo_object_serde::from_bytes(&bytes).map_err(|source| KvError::Serialization {
        detail: format!("{}: {source}", path.display()),
    })
}

fn write_object_file<T: Serialize>(path: &Path, value: &T) -> Result<(), KvError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| KvError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = sinomo_object_serde::to_bytes(value, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })?;
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
    fs::rename(&temp_path, path).map_err(|source| KvError::Io {
        path: format!("{} -> {}", temp_path.display(), path.display()),
        detail: source.to_string(),
    })?;
    sync_directory(parent)
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
    #[error("unknown watch: {watch_id:?}")]
    UnknownWatch { watch_id: WatchId },
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
        std::fs::remove_dir_all(root).ok();
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
