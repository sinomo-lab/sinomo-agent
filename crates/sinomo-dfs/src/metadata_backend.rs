//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: materialized shard state lives in
//! packed table batches, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

use crate::control_plane::{DfsMetadataShardConfig, select_metadata_shard_config_for_path};
use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::DfsWriteReceipt;
use sinomo_events::EventId;
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
#[cfg(windows)]
use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};
use thiserror::Error;
use time::OffsetDateTime;

pub const DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT: u64 = 4096;
pub const DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT: usize = 512;
pub const DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT: usize = 4 * 1024 * 1024;
const DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES: u64 = 64 * 1024 * 1024;
const JOURNAL_SEGMENT_ENTRY_LIMIT: u64 = 8192;
const DIRFRAG_BUCKET_COUNT: u64 = 256;
const METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 1;
static METADATA_WRITE_PATH_LOCKS: OnceLock<(Mutex<BTreeSet<PathBuf>>, Condvar)> = OnceLock::new();

#[derive(Debug, Error)]
pub enum MetadataBackendError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("metadata serialization error: {0}")]
    Serde(String),
    #[error("filesystem not found: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("object not found: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error(
        "metadata transaction conflict for object {object_id}: expected {expected}, current {current}"
    )]
    Conflict {
        object_id: DfsObjectId,
        expected: DfsVersion,
        current: DfsVersion,
    },
}

pub type Result<T> = std::result::Result<T, MetadataBackendError>;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsMetadataShardId(pub String);

impl DfsMetadataShardId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardStatus {
    pub shard_id: DfsMetadataShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub journal_next_sequence: u64,
    pub journal_last_sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_lag: Vec<DfsMetadataReplicaLagStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replay_cursors: Vec<DfsMetadataReplayCursorStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplicaLagStatus {
    pub target_node_id: NodeId,
    pub replicated_through_sequence: u64,
    pub lag_entries: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplayCursorStatus {
    pub source_shard_id: DfsMetadataShardId,
    pub replayed_through_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsInodeId(pub String);

impl DfsInodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsDirFragmentId(pub String);

impl DfsDirFragmentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPathRecord {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub object_id: DfsObjectId,
    pub inode_id: DfsInodeId,
    pub parent_inode_id: DfsInodeId,
    pub dirfrag_id: DfsDirFragmentId,
    pub name: String,
    pub name_hash: u64,
    pub kind: DfsObjectKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsInodeRecord {
    pub inode_id: DfsInodeId,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub kind: DfsObjectKind,
    pub owner_node_id: NodeId,
    pub policy: sinomo_core::DfsObjectPolicy,
    pub visible_version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

impl From<&DfsObjectMetadata> for DfsInodeRecord {
    fn from(object: &DfsObjectMetadata) -> Self {
        let mut current_locations = object.current_locations.clone();
        normalize_node_locations(&mut current_locations);
        Self {
            inode_id: object_inode_id(&object.object_id),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            kind: object.kind.clone(),
            owner_node_id: object.owner_node_id.clone(),
            policy: object.policy.clone(),
            visible_version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            current_locations,
            created_by_event_id: object.created_by_event_id.clone(),
            last_modified_by_event_id: object.last_modified_by_event_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDirFragmentRecord {
    pub file_system_id: DfsFileSystemId,
    pub dir_inode_id: DfsInodeId,
    pub dirfrag_id: DfsDirFragmentId,
    pub hash_start_inclusive: u64,
    pub hash_end_exclusive: u64,
    pub entry_count: u64,
    pub split_after_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectVersionRecord {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub visible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptRecord {
    pub receipt: DfsWriteReceipt,
    pub replica_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentRecord {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub target_version: DfsVersion,
    pub coordinator_node_id: NodeId,
    #[serde(default)]
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub desired_replica_node_ids: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    pub status: DfsWriteIntentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentStatus {
    Staging,
    ReceiptsPending,
    Committed,
    Rejected,
    Conflict,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictRecord {
    pub conflict_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub local_version: DfsVersion,
    pub remote_version: DfsVersion,
    pub detected_by_node_id: NodeId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_intent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    pub status: DfsConflictStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub detected_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsConflictStatus {
    Open,
    Resolved,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalEntry {
    pub shard_id: DfsMetadataShardId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub mutation: DfsMetadataMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsMetadataMutation {
    PutFileSystem {
        file_system_id: DfsFileSystemId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_system: Option<DfsFileSystem>,
    },
    PutObject {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object: Option<DfsObjectMetadata>,
    },
    PutVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version_record: Option<DfsObjectVersionRecord>,
    },
    PutReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<DfsReplicaReceiptRecord>,
    },
    PutIntent {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        status: DfsWriteIntentStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<DfsWriteIntentRecord>,
    },
    CommitVisibleVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_version: DfsVersion,
        committed_version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        committed_object: Option<DfsObjectMetadata>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        visible_version: Option<DfsObjectVersionRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        required_replica_node_ids: Vec<NodeId>,
    },
    PutConflict {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        conflict_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conflict: Option<DfsConflictRecord>,
    },
    ApplyNamespaceTransaction {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
        object_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transaction: Option<DfsNamespaceTransaction>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSubscription {
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
}

impl DfsMetadataSubscription {
    pub fn new(shard_id: DfsMetadataShardId, next_after_sequence: u64) -> Self {
        Self {
            shard_id,
            next_after_sequence,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatch {
    pub shard_id: DfsMetadataShardId,
    pub entries: Vec<DfsMetadataJournalEntry>,
    pub next_after_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataJournalHead {
    next_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataReplayCursor {
    shard_id: DfsMetadataShardId,
    last_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceTransaction {
    pub transaction_id: String,
    pub file_system_id: DfsFileSystemId,
    pub objects: Vec<DfsObjectMetadata>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsVisibleVersionCommitRecord {
    expected_version: DfsVersion,
    committed_object: DfsObjectMetadata,
    visible_version: DfsObjectVersionRecord,
    required_replica_node_ids: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

pub trait DfsMetadataBackend: std::fmt::Debug + Send {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry>;
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;
    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry>;
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>>;
    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut children = Vec::new();
        for object in self.objects_for_file_system(file_system_id)? {
            let object_path = normalize_posix_path(&object.path);
            if object.kind == DfsObjectKind::Tombstone
                || object_path == directory_path
                || !object_path.starts_with(&prefix)
            {
                continue;
            }
            let rest = object_path.trim_start_matches(&prefix);
            if !rest.is_empty() && !rest.contains('/') {
                children.push(object);
            }
        }
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = Vec::new();
        for object in self.objects_for_file_system(file_system_id)? {
            if object.kind == DfsObjectKind::Hardlink
                && object.link_target_object_id.as_ref() == Some(target_object_id)
            {
                hardlinks.push(object);
            }
        }
        hardlinks.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(hardlinks)
    }
    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry>;
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>>;
    fn put_receipt(&mut self, receipt: &DfsReplicaReceiptRecord)
    -> Result<DfsMetadataJournalEntry>;
    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_receipt(receipt)
    }
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>>;
    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry>;
    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_intent(intent)
    }
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>>;
    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry>;
    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry>;
    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry>;
    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;
    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>>;
    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>>;
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;
    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.journal_since(after_sequence)?;
        entries.truncate(max_entries);
        Ok(entries)
    }
    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        bounded_metadata_journal_batch(
            DfsMetadataShardId::new("default"),
            after_sequence,
            entries,
            max_entries,
            max_encoded_bytes,
        )
    }
    fn journal_batch_for_shard_since_limited(
        &self,
        _shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.journal_batch_since_limited(after_sequence, max_entries, max_encoded_bytes)
    }
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(0)
    }
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()> {
        Ok(())
    }
    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>>;
    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize>;
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}

#[derive(Clone, Debug)]
pub struct LocalJsonMetadataBackend {
    shard_id: DfsMetadataShardId,
    root: PathBuf,
    next_sequence: u64,
    tables: DfsMetadataTables,
    table_log_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsObjectShardIndexRecord {
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    path: String,
    shard_id: DfsMetadataShardId,
}

#[derive(Clone, Debug, Default)]
struct DfsMetadataTables {
    file_systems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), DfsPathRecord>,
    paths: BTreeMap<(DfsFileSystemId, String), DfsPathRecord>,
    inodes: BTreeMap<DfsInodeId, DfsInodeRecord>,
    dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), DfsDirFragmentRecord>,
    dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), DfsPathRecord>,
    hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), DfsPathRecord>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), DfsObjectVersionRecord>,
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
    conflicts: BTreeMap<(DfsObjectId, String), DfsConflictRecord>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), DfsVisibleVersionCommitRecord>,
    namespace_transactions: BTreeMap<String, DfsNamespaceTransaction>,
}

#[derive(Clone, Debug, Default)]
struct DfsMetadataTableState {
    tables: DfsMetadataTables,
    table_log_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataTableSnapshot {
    format_version: u16,
    ops: Vec<DfsMetadataTableOp>,
}

impl DfsMetadataTables {
    fn has_materialized_state(&self) -> bool {
        !self.file_systems.is_empty()
            || !self.objects.is_empty()
            || !self.versions.is_empty()
            || !self.receipts.is_empty()
            || !self.intents.is_empty()
            || !self.conflicts.is_empty()
            || !self.namespace_transactions.is_empty()
            || !self.visible_commits.is_empty()
    }

    fn apply(&mut self, op: DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => {
                self.file_systems
                    .insert(file_system.file_system_id.clone(), file_system);
            }
            DfsMetadataTableOp::PutObject { object } => {
                self.objects.insert(object.object_id.clone(), object);
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => {
                self.file_system_objects.insert(
                    (
                        path_record.file_system_id.clone(),
                        path_record.object_id.clone(),
                    ),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => {
                self.file_system_objects
                    .remove(&(file_system_id, object_id));
            }
            DfsMetadataTableOp::PutPath { path_record } => {
                self.paths.insert(
                    (path_record.file_system_id.clone(), path_record.path.clone()),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => {
                self.paths.remove(&(file_system_id, path));
            }
            DfsMetadataTableOp::PutInode { inode } => {
                self.inodes.insert(inode.inode_id.clone(), inode);
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => {
                self.dirfrags.insert(
                    (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
                    dirfrag,
                );
            }
            DfsMetadataTableOp::PutDentry { path_record } => {
                self.dentries.insert(dentry_key(&path_record), path_record);
            }
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => {
                self.dentries
                    .remove(&(parent_inode_id, dirfrag_id, name_hash, name));
            }
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                path_record,
            } => {
                self.hardlinks
                    .insert((target_object_id, hardlink_object_id), path_record);
            }
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => {
                self.hardlinks
                    .remove(&(target_object_id, hardlink_object_id));
            }
            DfsMetadataTableOp::PutVersion { version } => {
                self.versions.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version,
                );
            }
            DfsMetadataTableOp::PutReceipt { receipt } => {
                self.receipts.insert(
                    (
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                        receipt.replica_node_id.clone(),
                    ),
                    receipt,
                );
            }
            DfsMetadataTableOp::PutIntent { intent } => {
                self.intents
                    .insert((intent.object_id.clone(), intent.intent_id.clone()), intent);
            }
            DfsMetadataTableOp::PutConflict { conflict } => {
                self.conflicts.insert(
                    (conflict.object_id.clone(), conflict.conflict_id.clone()),
                    conflict,
                );
            }
            DfsMetadataTableOp::PutVisibleCommit {
                object_id,
                version,
                commit,
            } => {
                self.visible_commits.insert((object_id, version), commit);
            }
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => {
                self.namespace_transactions
                    .insert(transaction.transaction_id.clone(), transaction);
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataTableBatch {
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum DfsMetadataTableOp {
    PutFileSystem {
        file_system: DfsFileSystem,
    },
    PutObject {
        object: DfsObjectMetadata,
    },
    PutFileSystemObject {
        path_record: DfsPathRecord,
    },
    RemoveFileSystemObject {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
    },
    PutPath {
        path_record: DfsPathRecord,
    },
    RemovePath {
        file_system_id: DfsFileSystemId,
        path: String,
    },
    PutInode {
        inode: DfsInodeRecord,
    },
    PutDirFragment {
        dirfrag: DfsDirFragmentRecord,
    },
    PutDentry {
        path_record: DfsPathRecord,
    },
    RemoveDentry {
        parent_inode_id: DfsInodeId,
        dirfrag_id: DfsDirFragmentId,
        name_hash: u64,
        name: String,
    },
    PutHardlink {
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
        path_record: DfsPathRecord,
    },
    RemoveHardlink {
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
    },
    PutVersion {
        version: DfsObjectVersionRecord,
    },
    PutReceipt {
        receipt: DfsReplicaReceiptRecord,
    },
    PutIntent {
        intent: DfsWriteIntentRecord,
    },
    PutConflict {
        conflict: DfsConflictRecord,
    },
    PutVisibleCommit {
        object_id: DfsObjectId,
        version: DfsVersion,
        commit: DfsVisibleVersionCommitRecord,
    },
    PutNamespaceTransaction {
        transaction: DfsNamespaceTransaction,
    },
}

#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    shards: BTreeMap<DfsMetadataShardId, LocalJsonMetadataBackend>,
}

impl LocalShardedMetadataBackend {
    pub fn new(root: impl Into<PathBuf>, shards: Vec<DfsMetadataShardConfig>) -> Result<Self> {
        let root = root.into();
        if shards.is_empty() {
            return Err(MetadataBackendError::Serde(
                "local sharded metadata backend requires at least one shard".to_string(),
            ));
        }
        create_dir_all(root.join("shard-index").join("objects"))?;
        let mut shard_configs = BTreeMap::new();
        let mut backends = BTreeMap::new();
        for shard in shards {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            if shard_configs
                .insert(shard.shard_id.clone(), shard.clone())
                .is_some()
            {
                return Err(MetadataBackendError::Serde(format!(
                    "duplicate DFS metadata shard config: {}",
                    shard.shard_id
                )));
            }
            let backend = LocalJsonMetadataBackend::new(
                shard_id.clone(),
                root.join(key_segment(&shard_id.0)),
            )?;
            backends.insert(shard_id, backend);
        }
        Ok(Self {
            root,
            shard_configs,
            shards: backends,
        })
    }

    pub fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.shards.keys().cloned().collect()
    }

    fn object_shard_index_path(&self, object_id: &DfsObjectId) -> PathBuf {
        self.root
            .join("shard-index")
            .join("objects")
            .join(format!("{}.sobj", key_segment(object_id)))
    }

    fn read_object_shard_index(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectShardIndexRecord>> {
        read_json_optional(&self.object_shard_index_path(object_id))
    }

    fn write_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_json_atomic(
            &self.object_shard_index_path(&object.object_id),
            &DfsObjectShardIndexRecord {
                object_id: object.object_id.clone(),
                file_system_id: object.file_system_id.clone(),
                path: normalize_path(&object.path),
                shard_id: shard_id.clone(),
            },
        )
    }

    fn shard_backend(&self, shard_id: &DfsMetadataShardId) -> Result<&LocalJsonMetadataBackend> {
        self.shards.get(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!("unknown local DFS metadata shard: {}", shard_id.0))
        })
    }

    fn shard_backend_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&mut LocalJsonMetadataBackend> {
        self.shards.get_mut(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!("unknown local DFS metadata shard: {}", shard_id.0))
        })
    }

    fn shard_configs_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Vec<DfsMetadataShardConfig> {
        self.shard_configs
            .values()
            .filter(|shard| shard.file_system_id == *file_system_id)
            .cloned()
            .collect()
    }

    fn select_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<DfsMetadataShardId> {
        let selected = select_metadata_shard_config_for_path(
            self.shard_configs_for_file_system(file_system_id),
            path,
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        selected
            .map(|shard| DfsMetadataShardId::new(shard.shard_id))
            .ok_or_else(|| MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })
    }

    fn shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsMetadataShardId>> {
        if let Some(index) = self.read_object_shard_index(object_id)? {
            return Ok(Some(index.shard_id));
        }
        for (shard_id, backend) in &self.shards {
            if backend.get_object(object_id)?.is_some() {
                return Ok(Some(shard_id.clone()));
            }
        }
        Ok(None)
    }

    fn require_shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<DfsMetadataShardId> {
        self.shard_id_for_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    fn shard_id_for_pending_object_record(
        &self,
        object_id: &DfsObjectId,
        file_system_id: &DfsFileSystemId,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<DfsMetadataShardId> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return Ok(shard_id);
        }
        let Some(object) = object else {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        };
        if object.object_id != *object_id || object.file_system_id != *file_system_id {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        }
        self.select_shard_id_for_path(file_system_id, &object.path)
    }

    fn put_cross_shard_tombstone(
        &mut self,
        object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        if source_shard_id == target_shard_id {
            return Ok(());
        }
        let Some(previous) = self
            .shard_backend(source_shard_id)?
            .get_object(&object.object_id)?
        else {
            return Ok(());
        };
        if previous.kind == DfsObjectKind::Tombstone {
            return Ok(());
        }
        let mut tombstone = previous.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = object.version.clone();
        tombstone.changed_at_unix_nanos = object.changed_at_unix_nanos;
        tombstone.last_modified_by_event_id = object.last_modified_by_event_id.clone();
        self.shard_backend_mut(source_shard_id)?
            .put_object(&tombstone)?;
        Ok(())
    }

    fn put_object_routed(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        if let Some(index) = self.read_object_shard_index(&object.object_id)? {
            self.put_cross_shard_tombstone(&object, &index.shard_id, &target_shard_id)?;
        }
        let entry = self
            .shard_backend_mut(&target_shard_id)?
            .put_object(&object)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        Ok(entry)
    }
}

impl DfsMetadataBackend for LocalShardedMetadataBackend {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let shard_ids = self
            .shard_configs_for_file_system(&file_system.file_system_id)
            .into_iter()
            .map(|shard| DfsMetadataShardId::new(shard.shard_id))
            .collect::<Vec<_>>();
        if shard_ids.is_empty() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system.file_system_id.clone(),
            });
        }
        let mut last_entry = None;
        for shard_id in shard_ids {
            last_entry = Some(
                self.shard_backend_mut(&shard_id)?
                    .put_file_system(file_system)?,
            );
        }
        last_entry.ok_or_else(|| MetadataBackendError::UnknownFileSystem {
            file_system_id: file_system.file_system_id.clone(),
        })
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        for backend in self.shards.values() {
            if let Some(file_system) = backend.get_file_system(file_system_id)? {
                return Ok(Some(file_system));
            }
        }
        Ok(None)
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let mut file_systems = BTreeMap::<DfsFileSystemId, DfsFileSystem>::new();
        for backend in self.shards.values() {
            for file_system in backend.list_file_systems()? {
                file_systems.insert(file_system.file_system_id.clone(), file_system);
            }
        }
        Ok(file_systems.into_values().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        self.put_object_routed(object)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self.shard_backend(&shard_id)?.get_object(object_id);
        }
        Ok(None)
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let shard_id = self.select_shard_id_for_path(file_system_id, path)?;
        self.shard_backend(&shard_id)?
            .get_object_by_path(file_system_id, path)
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = BTreeMap::<DfsObjectId, DfsObjectMetadata>::new();
        for (shard_id, backend) in &self.shards {
            for object in backend.objects_for_file_system(file_system_id)? {
                if let Some(index) = self.read_object_shard_index(&object.object_id)?
                    && index.shard_id != *shard_id
                {
                    continue;
                }
                objects.insert(object.object_id.clone(), object);
            }
        }
        let mut objects = objects.into_values().collect::<Vec<_>>();
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(objects)
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&version.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version(version)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_version(object_id, version)
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?
            .versions_for_object(object_id)
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&receipt.receipt.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_receipt(receipt)
    }

    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_pending_object_record(
            &receipt.receipt.object_id,
            &receipt.receipt.file_system_id,
            object,
        )?;
        self.shard_backend_mut(&shard_id)?.put_receipt(receipt)
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?
            .receipts_for_version(object_id, version)
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&intent.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_intent(intent)
    }

    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_pending_object_record(
            &intent.object_id,
            &intent.file_system_id,
            object,
        )?;
        self.shard_backend_mut(&shard_id)?.put_intent(intent)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_intent(object_id, intent_id)
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?.intents_for_object(object_id)
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&committed_object.object_id)?;
        let entry = self.shard_backend_mut(&shard_id)?.commit_visible_version(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
        )?;
        self.write_object_shard_index(&normalized_object_metadata(committed_object), &shard_id)?;
        Ok(entry)
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut objects_by_shard = BTreeMap::<DfsMetadataShardId, Vec<DfsObjectMetadata>>::new();
        for object in &transaction.objects {
            let object = normalized_object_metadata(object);
            let target_shard_id =
                self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            if let Some(index) = self.read_object_shard_index(&object.object_id)? {
                self.put_cross_shard_tombstone(&object, &index.shard_id, &target_shard_id)?;
            }
            objects_by_shard
                .entry(target_shard_id)
                .or_default()
                .push(object);
        }
        let mut last_entry = None;
        for (shard_id, objects) in objects_by_shard {
            let shard_transaction = DfsNamespaceTransaction {
                transaction_id: format!("{}:{}", transaction.transaction_id, shard_id.0),
                file_system_id: transaction.file_system_id.clone(),
                objects,
                created_at: transaction.created_at,
            };
            let entry = self
                .shard_backend_mut(&shard_id)?
                .apply_namespace_transaction(&shard_transaction)?;
            for object in &shard_transaction.objects {
                self.write_object_shard_index(object, &shard_id)?;
            }
            last_entry = Some(entry);
        }
        last_entry.ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "namespace transaction {} did not contain any objects",
                transaction.transaction_id
            ))
        })
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&conflict.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_conflict(conflict)
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_conflict(object_id, conflict_id)
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?
            .conflicts_for_object(object_id)
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .update_conflict_status(object_id, conflict_id, status)
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::new();
        for backend in self.shards.values() {
            entries.extend(backend.journal_since(after_sequence)?);
        }
        entries.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
        });
        Ok(entries)
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.shard_backend(shard_id)?.journal_batch_since_limited(
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        self.shard_backend(shard_id)?
            .metadata_replication_cursor(target_node_id, shard_id)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        self.shard_backend_mut(shard_id)?
            .put_metadata_replication_cursor(target_node_id, shard_id, next_after_sequence)
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let mut statuses = Vec::new();
        for shard in self.shard_configs.values() {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let backend = self.shard_backend(&shard_id)?;
            let journal_next_sequence = backend.next_sequence;
            let journal_last_sequence = journal_next_sequence.saturating_sub(1);
            let mut replica_lag = Vec::new();
            for target_node_id in &shard.replica_node_ids {
                let replicated_through_sequence =
                    backend.metadata_replication_cursor(target_node_id, &shard_id)?;
                replica_lag.push(DfsMetadataReplicaLagStatus {
                    target_node_id: target_node_id.clone(),
                    replicated_through_sequence,
                    lag_entries: journal_last_sequence.saturating_sub(replicated_through_sequence),
                });
            }
            statuses.push(DfsMetadataShardStatus {
                shard_id,
                file_system_id: Some(shard.file_system_id.clone()),
                primary_node_id: Some(shard.primary_node_id.clone()),
                replica_node_ids: shard.replica_node_ids.clone(),
                epoch: Some(shard.epoch),
                journal_next_sequence,
                journal_last_sequence,
                replica_lag,
                replay_cursors: backend.replay_cursor_statuses()?,
            });
        }
        Ok(statuses)
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        self.shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch(batch)
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        let shard_id = self
            .shards
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| DfsMetadataShardId::new("default"));
        DfsMetadataSubscription::new(shard_id, after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        self.shard_backend(&subscription.shard_id)?
            .poll_subscription(subscription)
    }
}

impl LocalJsonMetadataBackend {
    pub fn new(shard_id: DfsMetadataShardId, root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal"))?;
        create_dir_all(root.join("journal").join("segments"))?;
        migrate_legacy_journal_entries(&root)?;
        let next_sequence = recover_next_journal_sequence(&root)?;
        let mut backend = Self {
            shard_id,
            root,
            next_sequence,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
        };
        let state = load_or_migrate_metadata_tables(&backend.root)?;
        backend.tables = state.tables;
        backend.table_log_offset = state.table_log_offset;
        backend.ensure_local_materialization_cursor()?;
        backend.recover_unmaterialized_local_journal_entries()?;
        backend.recover_namespace_transactions()?;
        backend.recover_visible_version_commits()?;
        backend.recover_object_locations_from_visible_versions()?;
        Ok(backend)
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        let _lock = self.begin_metadata_write()?;
        let mut applied = 0usize;
        let mut local_cursor = self.local_materialization_cursor()?;
        let mut local_cursor_advanced = false;
        let mut replay_cursors = BTreeMap::<DfsMetadataShardId, DfsMetadataReplayCursor>::new();
        let mut advanced_cursors = BTreeSet::<DfsMetadataShardId>::new();
        for entry in &batch.entries {
            if !replay_cursors.contains_key(&entry.shard_id) {
                let replay_cursor_path = self.replay_cursor_path(&entry.shard_id);
                let cursor = read_json_optional::<DfsMetadataReplayCursor>(&replay_cursor_path)?
                    .unwrap_or(DfsMetadataReplayCursor {
                        shard_id: entry.shard_id.clone(),
                        last_sequence: 0,
                    });
                replay_cursors.insert(entry.shard_id.clone(), cursor);
            }
            let cursor = replay_cursors
                .get_mut(&entry.shard_id)
                .expect("cursor inserted above");
            if entry.sequence <= cursor.last_sequence {
                continue;
            }
            self.persist_replayed_journal_entry_unlocked(entry)?;
            if self.materialize_replayed_journal_entry_unlocked(entry)? {
                applied = applied.saturating_add(1);
            }
            if entry.sequence > local_cursor.last_sequence {
                local_cursor.last_sequence = entry.sequence;
                local_cursor_advanced = true;
            }
            cursor.last_sequence = entry.sequence;
            advanced_cursors.insert(entry.shard_id.clone());
        }
        if local_cursor_advanced {
            self.write_local_materialization_cursor(&local_cursor)?;
        }
        for shard_id in advanced_cursors {
            if let Some(cursor) = replay_cursors.get(&shard_id) {
                write_json_atomic(&self.replay_cursor_path(&cursor.shard_id), cursor)?;
            }
        }
        Ok(applied)
    }

    pub fn apply_journal_entry(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        self.persist_replayed_journal_entry_unlocked(entry)?;
        let changed = self.materialize_replayed_journal_entry_unlocked(entry)?;
        if changed {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(Some(entry.clone()))
        } else {
            Ok(None)
        }
    }

    fn persist_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<()> {
        if let Some(existing) = segmented_journal_entry_at_sequence(&self.root, entry.sequence)? {
            if existing == *entry {
                self.advance_journal_head_after_replayed_entry(entry.sequence)?;
                return Ok(());
            }
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal sequence {} already exists with a different entry while replaying shard {}",
                entry.sequence, entry.shard_id.0
            )));
        }
        append_segmented_journal_entry(&self.root, entry)?;
        self.advance_journal_head_after_replayed_entry(entry.sequence)
    }

    fn advance_journal_head_after_replayed_entry(&mut self, sequence: u64) -> Result<()> {
        let next_sequence = sequence.saturating_add(1).max(1).max(self.next_sequence);
        if next_sequence == self.next_sequence {
            return Ok(());
        }
        self.next_sequence = next_sequence;
        write_json_atomic(
            &self.journal_head_path(),
            &DfsMetadataJournalHead {
                next_sequence: self.next_sequence,
            },
        )
    }

    fn materialize_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<bool> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id,
                file_system,
            } => {
                let file_system =
                    required_replay_payload(entry, "put-file-system", file_system.as_ref())?;
                if self.get_file_system(file_system_id)?.as_ref() == Some(file_system) {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::PutObject {
                object_id, object, ..
            } => {
                let object = required_replay_payload(entry, "put-object", object.as_ref())?;
                if self.get_object(object_id)? == Some(normalized_object_metadata(object)) {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::PutVersion {
                object_id,
                version,
                version_record,
                ..
            } => {
                let version_record =
                    required_replay_payload(entry, "put-version", version_record.as_ref())?;
                if self.get_version(object_id, version)?
                    == Some(normalized_version_record(version_record))
                {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                let receipt = required_replay_payload(entry, "put-receipt", receipt.as_ref())?;
                for existing in self.receipts_for_version(object_id, version)? {
                    if &existing.replica_node_id != replica_node_id {
                        continue;
                    }
                    if &existing == receipt {
                        return Ok(false);
                    }
                    if existing.receipt.file_system_id == receipt.receipt.file_system_id
                        && existing.receipt.object_id == receipt.receipt.object_id
                        && existing.receipt.committed_version == receipt.receipt.committed_version
                        && existing.replica_node_id == receipt.replica_node_id
                        && existing.content_hash == receipt.content_hash
                    {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} conflicts with an existing receipt for replica {}",
                        entry.shard_id.0, entry.sequence, replica_node_id
                    )));
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                intent,
                ..
            } => {
                let intent = required_replay_payload(entry, "put-intent", intent.as_ref())?;
                if self.get_intent(object_id, intent_id)?.as_ref() == Some(intent) {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                previous_version,
                committed_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let committed_object = required_replay_payload(
                    entry,
                    "commit-visible-version committed object",
                    committed_object.as_ref(),
                )?;
                let visible_version = required_replay_payload(
                    entry,
                    "commit-visible-version visible version",
                    visible_version.as_ref(),
                )?;
                if self
                    .get_object(object_id)?
                    .as_ref()
                    .is_some_and(|object| &object.version == committed_version)
                    && self.get_version(object_id, committed_version)?.as_ref()
                        == Some(&normalized_version_record(visible_version))
                {
                    return Ok(false);
                }
                let current = self.get_object(object_id)?.ok_or_else(|| {
                    MetadataBackendError::UnknownObject {
                        object_id: object_id.clone(),
                    }
                })?;
                if &current.version != previous_version {
                    return Err(MetadataBackendError::Conflict {
                        object_id: object_id.clone(),
                        expected: previous_version.clone(),
                        current: current.version,
                    });
                }
                for node_id in required_replica_node_ids {
                    if !self.tables.receipts.contains_key(&(
                        object_id.clone(),
                        committed_version.clone(),
                        node_id.clone(),
                    )) {
                        return Err(MetadataBackendError::Conflict {
                            object_id: object_id.clone(),
                            expected: previous_version.clone(),
                            current: current.version.clone(),
                        });
                    }
                }
                let commit_record = DfsVisibleVersionCommitRecord {
                    expected_version: previous_version.clone(),
                    committed_object: normalized_object_metadata(committed_object),
                    visible_version: normalized_version_record(visible_version),
                    required_replica_node_ids: normalized_node_location_list(
                        required_replica_node_ids,
                    ),
                    created_at: entry.created_at,
                };
                self.materialize_visible_version_commit(&commit_record, entry.sequence)?;
                Ok(true)
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } => {
                let conflict = required_replay_payload(entry, "put-conflict", conflict.as_ref())?;
                if self.get_conflict(object_id, conflict_id)?.as_ref() == Some(conflict) {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction,
                ..
            } => {
                let transaction = required_replay_payload(
                    entry,
                    "apply-namespace-transaction",
                    transaction.as_ref(),
                )?;
                let normalized = normalized_namespace_transaction(transaction);
                if read_json_optional::<DfsNamespaceTransaction>(
                    &self.namespace_transaction_path(transaction_id),
                )? == Some(normalized.clone())
                    && normalized.objects.iter().all(|object| {
                        self.get_object(&object.object_id)
                            .is_ok_and(|stored| stored == Some(normalized_object_metadata(object)))
                    })
                {
                    return Ok(false);
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
        }
    }

    pub fn hydrate_journal_entry_payloads(
        &self,
        entries: Vec<DfsMetadataJournalEntry>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        entries
            .into_iter()
            .map(|entry| self.hydrate_journal_entry_payload(entry))
            .collect()
    }

    pub fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        journal_entries_since_limited(&self.root, after_sequence, max_entries)
    }

    pub fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        let mut hydrated = Vec::with_capacity(entries.len().min(max_entries));
        for entry in entries {
            hydrated.push(self.hydrate_journal_entry_payload(entry)?);
        }
        bounded_metadata_journal_batch(
            self.shard_id.clone(),
            after_sequence,
            hydrated,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn replay_cursor_statuses(&self) -> Result<Vec<DfsMetadataReplayCursorStatus>> {
        let cursor_dir = self.records_root().join("replay-cursors");
        if !cursor_dir.exists() {
            return Ok(Vec::new());
        }
        let mut cursors = Vec::new();
        for entry in read_dir(&cursor_dir)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                continue;
            }
            if let Some(cursor) = read_json_optional::<DfsMetadataReplayCursor>(&path)? {
                cursors.push(DfsMetadataReplayCursorStatus {
                    source_shard_id: cursor.shard_id,
                    replayed_through_sequence: cursor.last_sequence,
                });
            }
        }
        cursors.sort_by(|left, right| left.source_shard_id.cmp(&right.source_shard_id));
        Ok(cursors)
    }

    fn hydrate_journal_entry_payload(
        &self,
        mut entry: DfsMetadataJournalEntry,
    ) -> Result<DfsMetadataJournalEntry> {
        match &mut entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id,
                file_system,
            } if file_system.is_none() => {
                *file_system = self.get_file_system(file_system_id)?;
            }
            DfsMetadataMutation::PutObject {
                object_id, object, ..
            } if object.is_none() => {
                *object = self.get_object(object_id)?;
            }
            DfsMetadataMutation::PutVersion {
                object_id,
                version,
                version_record,
                ..
            } if version_record.is_none() => {
                *version_record = self.get_version(object_id, version)?;
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } if receipt.is_none() => {
                *receipt = self
                    .receipts_for_version(object_id, version)?
                    .into_iter()
                    .find(|record| &record.replica_node_id == replica_node_id);
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                intent,
                ..
            } if intent.is_none() => {
                *intent = self.get_intent(object_id, intent_id)?;
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } if conflict.is_none() => {
                *conflict = self.get_conflict(object_id, conflict_id)?;
            }
            _ => {}
        }
        Ok(entry)
    }

    fn write_journal(&mut self, mutation: DfsMetadataMutation) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_unlocked(mutation)
    }

    fn write_journal_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<DfsMetadataJournalEntry> {
        let sequence = self
            .next_sequence
            .max(fast_next_journal_sequence(&self.root)?);
        let entry = DfsMetadataJournalEntry {
            shard_id: self.shard_id.clone(),
            sequence,
            created_at: OffsetDateTime::now_utc(),
            mutation,
        };
        append_segmented_journal_entry(&self.root, &entry)?;
        self.next_sequence = sequence.saturating_add(1);
        write_json_atomic(
            &self.journal_head_path(),
            &DfsMetadataJournalHead {
                next_sequence: self.next_sequence,
            },
        )?;
        Ok(entry)
    }

    fn ensure_local_materialization_cursor(&self) -> Result<()> {
        if self.local_materialization_cursor_path().is_file() {
            return Ok(());
        }
        let last_sequence = if self.tables.has_materialized_state() {
            self.next_sequence.saturating_sub(1)
        } else {
            0
        };
        write_json_atomic(
            &self.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: self.shard_id.clone(),
                last_sequence,
            },
        )
    }

    fn local_materialization_cursor(&self) -> Result<DfsMetadataReplayCursor> {
        Ok(read_json_optional::<DfsMetadataReplayCursor>(
            &self.local_materialization_cursor_path(),
        )?
        .unwrap_or(DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: self.next_sequence.saturating_sub(1),
        }))
    }

    fn advance_local_materialization_cursor(&self, sequence: u64) -> Result<()> {
        let mut cursor = self.local_materialization_cursor()?;
        if sequence <= cursor.last_sequence {
            return Ok(());
        }
        cursor.last_sequence = sequence;
        self.write_local_materialization_cursor(&cursor)
    }

    fn write_local_materialization_cursor(&self, cursor: &DfsMetadataReplayCursor) -> Result<()> {
        write_json_atomic(&self.local_materialization_cursor_path(), cursor)
    }

    fn recover_unmaterialized_local_journal_entries(&mut self) -> Result<()> {
        let mut cursor = self.local_materialization_cursor()?;
        if cursor.last_sequence >= self.next_sequence.saturating_sub(1) {
            return Ok(());
        }
        loop {
            let entries = journal_entries_since_limited(
                &self.root,
                cursor.last_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )?;
            if entries.is_empty() {
                break;
            }
            let mut advanced = false;
            for entry in entries {
                if journal_entry_has_replay_payloads(&entry) {
                    self.materialize_replayed_journal_entry_unlocked(&entry)?;
                } else {
                    self.materialize_local_journal_entry(&entry)?;
                }
                if entry.sequence > cursor.last_sequence {
                    cursor.last_sequence = entry.sequence;
                    advanced = true;
                }
            }
            if advanced {
                self.write_local_materialization_cursor(&cursor)?;
            }
        }
        Ok(())
    }

    fn materialize_local_journal_entry(&mut self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id: _,
                file_system,
            } => {
                let Some(file_system) = file_system.as_ref() else {
                    return Ok(());
                };
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutFileSystem {
                        file_system: file_system.clone(),
                    }],
                )
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    return Ok(());
                };
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(&object.object_id)?;
                self.materialize_object_indexes_with_previous(
                    object,
                    previous.as_ref(),
                    entry.sequence,
                )
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    return Ok(());
                };
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutVersion {
                        version: normalized_version_record(version_record),
                    }],
                )
            }
            DfsMetadataMutation::PutReceipt { receipt, .. } => {
                let Some(receipt) = receipt.as_ref() else {
                    return Ok(());
                };
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutReceipt {
                        receipt: receipt.clone(),
                    }],
                )
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    return Ok(());
                };
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutIntent {
                        intent: intent.clone(),
                    }],
                )
            }
            DfsMetadataMutation::CommitVisibleVersion {
                previous_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    return Ok(());
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    return Ok(());
                };
                let commit_record = DfsVisibleVersionCommitRecord {
                    expected_version: previous_version.clone(),
                    committed_object: normalized_object_metadata(committed_object),
                    visible_version: normalized_version_record(visible_version),
                    required_replica_node_ids: normalized_node_location_list(
                        required_replica_node_ids,
                    ),
                    created_at: entry.created_at,
                };
                self.materialize_visible_version_commit(&commit_record, entry.sequence)
            }
            DfsMetadataMutation::PutConflict { conflict, .. } => {
                let Some(conflict) = conflict.as_ref() else {
                    return Ok(());
                };
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutConflict {
                        conflict: conflict.clone(),
                    }],
                )
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    return Ok(());
                };
                let transaction = normalized_namespace_transaction(transaction);
                self.append_and_apply_table_ops(
                    entry.sequence,
                    vec![DfsMetadataTableOp::PutNamespaceTransaction {
                        transaction: transaction.clone(),
                    }],
                )?;
                self.materialize_namespace_transaction(&transaction, entry.sequence)
            }
        }
    }

    fn records_root(&self) -> PathBuf {
        self.root.join("records")
    }

    fn namespace_transaction_path(&self, transaction_id: &str) -> PathBuf {
        self.records_root()
            .join("namespace-transactions")
            .join(format!("{}.sobj", key_segment(&transaction_id)))
    }

    fn replay_cursor_path(&self, shard_id: &DfsMetadataShardId) -> PathBuf {
        self.records_root()
            .join("replay-cursors")
            .join(format!("{}.sobj", key_segment(&shard_id.0)))
    }

    fn local_materialization_cursor_path(&self) -> PathBuf {
        self.records_root()
            .join("local-materialization")
            .join("cursor.sobj")
    }

    fn metadata_replication_cursor_path(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> PathBuf {
        self.records_root()
            .join("metadata-replication-cursors")
            .join(key_segment(target_node_id))
            .join(format!("{}.sobj", key_segment(&shard_id.0)))
    }

    fn journal_head_path(&self) -> PathBuf {
        self.root.join("journal").join("head.sobj")
    }

    fn journal_write_lock_path(&self) -> PathBuf {
        self.root.join("journal").join("write.lock")
    }

    fn metadata_write_lock(&self) -> Result<MetadataWriteLock> {
        let path_guard = MetadataPathWriteGuard::lock(self.root.clone());
        let path = self.journal_write_lock_path();
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
        Ok(MetadataWriteLock {
            _path_guard: path_guard,
            _file: file,
        })
    }

    fn begin_metadata_write(&mut self) -> Result<MetadataWriteLock> {
        let lock = self.metadata_write_lock()?;
        self.refresh_metadata_tables()?;
        Ok(lock)
    }

    fn append_and_apply_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let written = append_metadata_table_batch(
            &self.root,
            &DfsMetadataTableBatch {
                sequence,
                ops: ops.clone(),
            },
        )?;
        self.table_log_offset = self.table_log_offset.saturating_add(written);
        for op in ops {
            self.tables.apply(op);
        }
        self.compact_metadata_table_log_if_needed(DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES)?;
        Ok(())
    }

    fn compact_metadata_table_log_if_needed(&mut self, max_log_bytes: u64) -> Result<()> {
        let path = metadata_table_path(&self.root);
        let len = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        if len <= max_log_bytes {
            return Ok(());
        }
        persist_metadata_table_snapshot(&self.root, &self.tables)?;
        self.table_log_offset = fs::metadata(&path)
            .map(|metadata| metadata.len())
            .map_err(|source| MetadataBackendError::Io { path, source })?;
        Ok(())
    }

    fn refresh_metadata_tables(&mut self) -> Result<()> {
        let path = metadata_table_path(&self.root);
        if !path.is_file() {
            return Ok(());
        }
        self.table_log_offset =
            replay_metadata_table_batches_from(&path, self.table_log_offset, &mut self.tables)?;
        Ok(())
    }

    fn current_tables_snapshot(&self) -> Result<Cow<'_, DfsMetadataTables>> {
        let path = metadata_table_path(&self.root);
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(Cow::Borrowed(&self.tables));
            }
            Err(source) => {
                return Err(MetadataBackendError::Io { path, source });
            }
        };
        if metadata.len() <= self.table_log_offset {
            return Ok(Cow::Borrowed(&self.tables));
        }
        let mut tables = self.tables.clone();
        replay_metadata_table_batches_from(&path, self.table_log_offset, &mut tables)?;
        Ok(Cow::Owned(tables))
    }
}

#[derive(Debug)]
struct MetadataWriteLock {
    _path_guard: MetadataPathWriteGuard,
    _file: fs::File,
}

#[derive(Debug)]
struct MetadataPathWriteGuard {
    path: PathBuf,
}

impl MetadataPathWriteGuard {
    fn lock(path: PathBuf) -> Self {
        let (mutex, condvar) =
            METADATA_WRITE_PATH_LOCKS.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
        let mut locked = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !locked.insert(path.clone()) {
            locked = condvar
                .wait(locked)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        Self { path }
    }
}

impl Drop for MetadataPathWriteGuard {
    fn drop(&mut self) {
        let Some((mutex, condvar)) = METADATA_WRITE_PATH_LOCKS.get() else {
            return;
        };
        let mut locked = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locked.remove(&self.path);
        condvar.notify_all();
    }
}

impl DfsMetadataBackend for LocalJsonMetadataBackend {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutFileSystem {
            file_system_id: file_system.file_system_id.clone(),
            file_system: Some(file_system.clone()),
        })?;
        self.append_and_apply_table_ops(
            entry.sequence,
            vec![DfsMetadataTableOp::PutFileSystem {
                file_system: file_system.clone(),
            }],
        )?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.get(file_system_id).cloned())
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.values().cloned().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let previous = self.get_object(&object.object_id)?;
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutObject {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            object: Some(object.clone()),
        })?;
        self.materialize_object_indexes_with_previous(&object, previous.as_ref(), entry.sequence)?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.objects.get(object_id).cloned())
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let normalized_path = normalize_path(path);
        let tables = self.current_tables_snapshot()?;
        let Some(path_record) = tables
            .paths
            .get(&(file_system_id.clone(), normalized_path.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        if path_record.file_system_id != *file_system_id {
            return Ok(None);
        }
        if normalize_path(&path_record.path) != normalized_path {
            return Ok(None);
        }
        let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
            return Ok(None);
        };
        if object.file_system_id != *file_system_id {
            return Ok(None);
        }
        if normalize_path(&object.path) != normalize_path(path) {
            return Ok(None);
        }
        Ok(Some(object))
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        let mut objects = Vec::new();
        for path_record in tables
            .file_system_objects
            .values()
            .filter(|path_record| path_record.file_system_id == *file_system_id)
        {
            if path_record.file_system_id != *file_system_id {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            if object.file_system_id == *file_system_id
                && normalize_path(&object.path) == path_record.path
            {
                objects.push(object);
            }
        }
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(objects)
    }

    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let parent_inode_id = directory_inode_id(file_system_id, &directory_path);
        let tables = self.current_tables_snapshot()?;
        let mut objects = Vec::new();
        let parent_dentry_root = self
            .records_root()
            .join("dentries")
            .join(key_segment(&parent_inode_id.0));
        let _ = parent_dentry_root;
        for path_record in tables
            .dentries
            .values()
            .filter(|path_record| path_record.parent_inode_id == parent_inode_id)
        {
            if path_record.file_system_id != *file_system_id
                || path_record.parent_inode_id != parent_inode_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            if object.kind == DfsObjectKind::Tombstone
                || object.file_system_id != *file_system_id
                || normalize_path(&object.path) != path_record.path
            {
                continue;
            }
            objects.push(object);
        }
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(objects)
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        let mut hardlinks = Vec::new();
        for ((target_id, _), path_record) in &tables.hardlinks {
            if target_id != target_object_id {
                continue;
            }
            if path_record.file_system_id != *file_system_id {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            if object.kind == DfsObjectKind::Hardlink
                && object.file_system_id == *file_system_id
                && object.link_target_object_id.as_ref() == Some(target_object_id)
                && normalize_path(&object.path) == path_record.path
            {
                hardlinks.push(object);
            }
        }
        hardlinks.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(hardlinks)
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let version = normalized_version_record(version);
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutVersion {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
            version_record: Some(version),
        })?;
        if let DfsMetadataMutation::PutVersion {
            version_record: Some(version),
            ..
        } = &entry.mutation
        {
            self.append_and_apply_table_ops(
                entry.sequence,
                vec![DfsMetadataTableOp::PutVersion {
                    version: version.clone(),
                }],
            )?;
        }
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .versions
            .get(&(object_id.clone(), version.clone()))
            .cloned())
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        let tables = self.current_tables_snapshot()?;
        let mut versions = tables
            .versions
            .values()
            .filter(|version| version.object_id == *object_id)
            .cloned()
            .collect::<Vec<_>>();
        versions.sort_by(|left, right| left.version.cmp(&right.version));
        Ok(versions)
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutReceipt {
            file_system_id: receipt.receipt.file_system_id.clone(),
            object_id: receipt.receipt.object_id.clone(),
            version: receipt.receipt.committed_version.clone(),
            replica_node_id: receipt.replica_node_id.clone(),
            receipt: Some(receipt.clone()),
        })?;
        self.append_and_apply_table_ops(
            entry.sequence,
            vec![DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            }],
        )?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .receipts
            .values()
            .filter(|receipt| {
                receipt.receipt.object_id == *object_id
                    && receipt.receipt.committed_version == *version
            })
            .cloned()
            .collect())
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutIntent {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            intent_id: intent.intent_id.clone(),
            status: intent.status.clone(),
            intent: Some(intent.clone()),
        })?;
        self.append_and_apply_table_ops(
            entry.sequence,
            vec![DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            }],
        )?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intents
            .get(&(object_id.clone(), intent_id.to_string()))
            .cloned())
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        let mut intents = tables
            .intents
            .values()
            .filter(|intent| intent.object_id == *object_id)
            .cloned()
            .collect::<Vec<_>>();
        intents.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));
        Ok(intents)
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .get_object(&committed_object.object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: committed_object.object_id.clone(),
            })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: committed_object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }

        for node_id in required_replica_node_ids {
            if !self.tables.receipts.contains_key(&(
                committed_object.object_id.clone(),
                committed_object.version.clone(),
                node_id.clone(),
            )) {
                return Err(MetadataBackendError::Conflict {
                    object_id: committed_object.object_id.clone(),
                    expected: expected_version.clone(),
                    current: current.version.clone(),
                });
            }
        }

        let commit_record = DfsVisibleVersionCommitRecord {
            expected_version: expected_version.clone(),
            committed_object: normalized_object_metadata(committed_object),
            visible_version: normalized_version_record(visible_version),
            required_replica_node_ids: normalized_node_location_list(required_replica_node_ids),
            created_at: OffsetDateTime::now_utc(),
        };
        let entry = self.write_journal_unlocked(DfsMetadataMutation::CommitVisibleVersion {
            file_system_id: committed_object.file_system_id.clone(),
            object_id: committed_object.object_id.clone(),
            previous_version: expected_version.clone(),
            committed_version: committed_object.version.clone(),
            committed_object: Some(commit_record.committed_object.clone()),
            visible_version: Some(commit_record.visible_version.clone()),
            required_replica_node_ids: commit_record.required_replica_node_ids.clone(),
        })?;
        self.materialize_visible_version_commit(&commit_record, entry.sequence)?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if transaction
            .objects
            .iter()
            .any(|object| object.file_system_id != transaction.file_system_id)
        {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }

        let transaction = normalized_namespace_transaction(transaction);
        let entry =
            self.write_journal_unlocked(DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                transaction: Some(transaction),
            })?;
        if let DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: Some(transaction),
            ..
        } = &entry.mutation
        {
            self.append_and_apply_table_ops(
                entry.sequence,
                vec![DfsMetadataTableOp::PutNamespaceTransaction {
                    transaction: transaction.clone(),
                }],
            )?;
            self.materialize_namespace_transaction(transaction, entry.sequence)?;
        }
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PutConflict {
            file_system_id: conflict.file_system_id.clone(),
            object_id: conflict.object_id.clone(),
            conflict_id: conflict.conflict_id.clone(),
            conflict: Some(conflict.clone()),
        })?;
        self.append_and_apply_table_ops(
            entry.sequence,
            vec![DfsMetadataTableOp::PutConflict {
                conflict: conflict.clone(),
            }],
        )?;
        self.advance_local_materialization_cursor(entry.sequence)?;
        Ok(entry)
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .conflicts
            .get(&(object_id.clone(), conflict_id.to_string()))
            .cloned())
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .conflicts
            .values()
            .filter(|conflict| conflict.object_id == *object_id)
            .cloned()
            .collect())
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(mut conflict) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        conflict.status = status;
        self.put_conflict(&conflict)?;
        Ok(Some(conflict))
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        journal_entries_since(&self.root, after_sequence)
    }

    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalJsonMetadataBackend::journal_since_limited(self, after_sequence, max_entries)
    }

    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        LocalJsonMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalJsonMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(read_json_optional::<DfsMetadataReplayCursor>(
            &self.metadata_replication_cursor_path(target_node_id, shard_id),
        )?
        .map(|cursor| cursor.last_sequence)
        .unwrap_or(0))
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        let next_after_sequence =
            next_after_sequence.max(self.metadata_replication_cursor(target_node_id, shard_id)?);
        write_json_atomic(
            &self.metadata_replication_cursor_path(target_node_id, shard_id),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: next_after_sequence,
            },
        )
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let journal_next_sequence = self.next_sequence;
        Ok(vec![DfsMetadataShardStatus {
            shard_id: self.shard_id.clone(),
            file_system_id: None,
            primary_node_id: None,
            replica_node_ids: Vec::new(),
            epoch: None,
            journal_next_sequence,
            journal_last_sequence: journal_next_sequence.saturating_sub(1),
            replica_lag: Vec::new(),
            replay_cursors: self.replay_cursor_statuses()?,
        }])
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        LocalJsonMetadataBackend::apply_journal_batch(self, batch)
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        DfsMetadataSubscription::new(self.shard_id.clone(), after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        let entries = self.journal_since(subscription.next_after_sequence)?;
        if let Some(last) = entries.last() {
            subscription.next_after_sequence = last.sequence;
        }
        Ok(DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            entries,
            next_after_sequence: subscription.next_after_sequence,
        })
    }
}

impl LocalJsonMetadataBackend {
    fn recover_namespace_transactions(&mut self) -> Result<()> {
        for transaction in self
            .tables
            .namespace_transactions
            .values()
            .cloned()
            .collect::<Vec<_>>()
        {
            if self.materialize_namespace_transaction_for_recovery(&transaction)? {
                self.write_journal(DfsMetadataMutation::ApplyNamespaceTransaction {
                    file_system_id: transaction.file_system_id.clone(),
                    transaction_id: transaction.transaction_id.clone(),
                    object_count: transaction.objects.len(),
                    transaction: Some(transaction.clone()),
                })?;
            }
        }
        Ok(())
    }

    fn materialize_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        sequence: u64,
    ) -> Result<()> {
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes(object, sequence)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes(object, sequence)?;
        }
        Ok(())
    }

    fn materialize_namespace_transaction_for_recovery(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<bool> {
        let sequence = self.next_sequence.saturating_sub(1);
        let mut changed = false;
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
        {
            changed |= self.materialize_object_indexes_if_current(object, sequence)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            changed |= self.materialize_object_indexes_if_current(object, sequence)?;
        }
        Ok(changed)
    }

    fn materialize_object_indexes_if_current(
        &mut self,
        object: &DfsObjectMetadata,
        sequence: u64,
    ) -> Result<bool> {
        if let Some(current) = self.get_object(&object.object_id)? {
            if self.object_indexes_are_materialized(&current)? {
                return Ok(false);
            }
            self.materialize_object_indexes(&current, sequence)?;
            return Ok(true);
        }
        if self.object_indexes_are_materialized(object)? {
            return Ok(false);
        }
        self.materialize_object_indexes(object, sequence)?;
        Ok(true)
    }

    fn object_indexes_are_materialized(&self, object: &DfsObjectMetadata) -> Result<bool> {
        let path_record = path_record_for_object(object);
        if self.tables.objects.get(&object.object_id).cloned()
            != Some(normalized_object_metadata(object))
        {
            return Ok(false);
        }
        if !self
            .tables
            .inodes
            .contains_key(&object_inode_id(&object.object_id))
        {
            return Ok(false);
        }
        if !self
            .tables
            .file_system_objects
            .contains_key(&(object.file_system_id.clone(), object.object_id.clone()))
        {
            return Ok(false);
        }
        if !self
            .tables
            .paths
            .contains_key(&(object.file_system_id.clone(), path_record.path.clone()))
        {
            return Ok(false);
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(true);
        }
        Ok(self
            .tables
            .dentries
            .get(&dentry_key(&path_record))
            .is_some_and(|record| {
                record.object_id == object.object_id
                    && record.file_system_id == object.file_system_id
                    && record.path == path_record.path
            }))
    }

    fn materialize_object_indexes(
        &mut self,
        object: &DfsObjectMetadata,
        sequence: u64,
    ) -> Result<()> {
        self.materialize_object_indexes_with_previous(object, None, sequence)
    }

    fn object_records(&self) -> Result<Vec<DfsObjectMetadata>> {
        Ok(self.tables.objects.values().cloned().collect())
    }

    fn materialize_object_indexes_with_previous(
        &mut self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
        sequence: u64,
    ) -> Result<()> {
        let object = normalized_object_metadata(object);
        let inode = DfsInodeRecord::from(&object);
        let previous_path_record = previous
            .map(|previous| {
                self.stored_path_record_for_object(previous)
                    .map(|stored| stored.unwrap_or_else(|| path_record_for_object(previous)))
            })
            .transpose()?;
        let path_record = if object.kind == DfsObjectKind::Tombstone {
            path_record_for_object(&object)
        } else if let Some(previous_path_record) = previous_path_record.as_ref()
            && previous_path_record.file_system_id == object.file_system_id
            && previous_path_record.path == normalize_path(&object.path)
            && previous_path_record.parent_inode_id
                == parent_inode_id(&object.file_system_id, &normalize_path(&object.path))
        {
            let mut path_record = path_record_for_object(&object);
            path_record.dirfrag_id = previous_path_record.dirfrag_id.clone();
            path_record
        } else {
            self.path_record_for_new_dentry(&object)?
        };

        let mut table_ops = Vec::new();
        let mut removed_dentry = None::<DfsPathRecord>;
        if let Some(previous_target_object_id) =
            previous.and_then(|previous| previous.link_target_object_id.as_ref())
            && (object.kind != DfsObjectKind::Hardlink
                || object.link_target_object_id.as_ref() != Some(previous_target_object_id))
        {
            table_ops.push(DfsMetadataTableOp::RemoveHardlink {
                target_object_id: previous_target_object_id.clone(),
                hardlink_object_id: object.object_id.clone(),
            });
        }
        if let Some(previous) = previous {
            let previous_path_record = previous_path_record
                .as_ref()
                .cloned()
                .unwrap_or_else(|| path_record_for_object(previous));
            if previous.file_system_id != object.file_system_id {
                table_ops.push(DfsMetadataTableOp::RemoveFileSystemObject {
                    file_system_id: previous.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                });
            }
            if previous_path_record.file_system_id != path_record.file_system_id
                || previous_path_record.path != path_record.path
            {
                table_ops.push(DfsMetadataTableOp::RemovePath {
                    file_system_id: previous_path_record.file_system_id.clone(),
                    path: previous_path_record.path.clone(),
                });
            }
            if previous.kind != DfsObjectKind::Tombstone
                && (object.kind == DfsObjectKind::Tombstone
                    || previous_path_record.path != path_record.path
                    || previous_path_record.parent_inode_id != path_record.parent_inode_id
                    || previous_path_record.name != path_record.name)
            {
                removed_dentry = Some(previous_path_record.clone());
                self.remove_dentry_index(&previous_path_record, &mut table_ops)?;
            }
        }
        table_ops.push(DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record.clone(),
        });
        table_ops.push(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        table_ops.push(DfsMetadataTableOp::PutInode { inode });
        if object.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = object.link_target_object_id.as_ref()
        {
            table_ops.push(DfsMetadataTableOp::PutHardlink {
                target_object_id: target_object_id.clone(),
                hardlink_object_id: object.object_id.clone(),
                path_record: path_record.clone(),
            });
        }
        if object.kind == DfsObjectKind::Tombstone {
            self.remove_dentry_index(&path_record, &mut table_ops)?;
            table_ops.push(DfsMetadataTableOp::PutPath { path_record });
            self.append_and_apply_table_ops(sequence, table_ops)?;
            return Ok(());
        }

        let removed_from_new_dirfrag = removed_dentry.as_ref().is_some_and(|removed| {
            removed.parent_inode_id == path_record.parent_inode_id
                && removed.dirfrag_id == path_record.dirfrag_id
        });
        let dirfrag = self.read_or_create_dirfrag(
            &path_record.file_system_id,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id,
            Some(path_record.name_hash),
            !self.tables.dentries.contains_key(&dentry_key(&path_record))
                && !removed_from_new_dirfrag,
        )?;
        table_ops.push(DfsMetadataTableOp::PutPath {
            path_record: path_record.clone(),
        });
        table_ops.push(DfsMetadataTableOp::PutDentry { path_record });
        table_ops.push(DfsMetadataTableOp::PutDirFragment { dirfrag });
        self.append_and_apply_table_ops(sequence, table_ops)
    }

    fn remove_dentry_index(
        &self,
        path_record: &DfsPathRecord,
        table_ops: &mut Vec<DfsMetadataTableOp>,
    ) -> Result<()> {
        if !self.tables.dentries.contains_key(&dentry_key(path_record)) {
            return Ok(());
        }
        table_ops.push(DfsMetadataTableOp::RemoveDentry {
            parent_inode_id: path_record.parent_inode_id.clone(),
            dirfrag_id: path_record.dirfrag_id.clone(),
            name_hash: path_record.name_hash,
            name: path_record.name.clone(),
        });
        let Some(mut dirfrag) = self
            .tables
            .dirfrags
            .get(&(
                path_record.parent_inode_id.clone(),
                path_record.dirfrag_id.clone(),
            ))
            .cloned()
        else {
            return Ok(());
        };
        dirfrag.entry_count = dirfrag.entry_count.saturating_sub(1);
        table_ops.push(DfsMetadataTableOp::PutDirFragment { dirfrag });
        Ok(())
    }

    fn stored_path_record_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsPathRecord>> {
        Ok(self
            .tables
            .paths
            .get(&(object.file_system_id.clone(), normalize_path(&object.path)))
            .cloned())
    }

    fn path_record_for_new_dentry(&self, object: &DfsObjectMetadata) -> Result<DfsPathRecord> {
        let mut path_record = path_record_for_object(object);
        let root_dirfrag = self.read_or_create_dirfrag(
            &path_record.file_system_id,
            &path_record.parent_inode_id,
            &DfsDirFragmentId::new("root"),
            None,
            false,
        )?;
        if root_dirfrag.entry_count >= root_dirfrag.split_after_count {
            path_record.dirfrag_id = bucket_dirfrag_id(path_record.name_hash);
        }
        Ok(path_record)
    }

    fn recover_visible_version_commits(&mut self) -> Result<()> {
        let mut commits = self
            .tables
            .visible_commits
            .values()
            .cloned()
            .collect::<Vec<_>>();
        commits.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| {
                    left.committed_object
                        .object_id
                        .cmp(&right.committed_object.object_id)
                })
                .then_with(|| {
                    left.committed_object
                        .version
                        .cmp(&right.committed_object.version)
                })
        });
        for commit in commits {
            self.recover_visible_version_commit(&commit)?;
        }
        Ok(())
    }

    fn recover_object_locations_from_visible_versions(&mut self) -> Result<()> {
        let sequence = self.next_sequence.saturating_sub(1);
        for mut object in self.object_records()? {
            if object.kind == DfsObjectKind::Tombstone {
                continue;
            }
            let Some(version) = self.get_version(&object.object_id, &object.version)? else {
                continue;
            };
            if !version.visible || version.locations.is_empty() {
                continue;
            }
            if object.content_hash != version.content_hash {
                continue;
            }
            let mut object_locations = object.current_locations.clone();
            let mut version_locations = version.locations.clone();
            normalize_node_locations(&mut object_locations);
            normalize_node_locations(&mut version_locations);
            if object_locations == version_locations {
                continue;
            }
            object.current_locations = version_locations;
            self.materialize_object_indexes(&object, sequence)?;
        }
        Ok(())
    }

    fn recover_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
    ) -> Result<()> {
        if self
            .get_object_by_path(
                &commit.committed_object.file_system_id,
                &commit.committed_object.path,
            )?
            .is_some_and(|object| object.kind == DfsObjectKind::Tombstone)
        {
            return Ok(());
        }
        for node_id in &commit.required_replica_node_ids {
            if !self.tables.receipts.contains_key(&(
                commit.committed_object.object_id.clone(),
                commit.committed_object.version.clone(),
                node_id.clone(),
            )) {
                return Ok(());
            }
        }

        let current = self.get_object(&commit.committed_object.object_id)?;
        if current.as_ref().is_some_and(|object| {
            object.kind == DfsObjectKind::Tombstone
                && normalize_path(&object.path) == normalize_path(&commit.committed_object.path)
        }) {
            return Ok(());
        }
        if current
            .as_ref()
            .is_some_and(|object| object.version == commit.committed_object.version)
            && self
                .get_version(
                    &commit.visible_version.object_id,
                    &commit.visible_version.version,
                )?
                .is_some()
        {
            return Ok(());
        }
        if current.as_ref().is_some_and(|object| {
            object.version != commit.expected_version
                && object.version != commit.committed_object.version
        }) {
            return Ok(());
        }

        let entry = self.write_journal(DfsMetadataMutation::CommitVisibleVersion {
            file_system_id: commit.committed_object.file_system_id.clone(),
            object_id: commit.committed_object.object_id.clone(),
            previous_version: commit.expected_version.clone(),
            committed_version: commit.committed_object.version.clone(),
            committed_object: Some(commit.committed_object.clone()),
            visible_version: Some(commit.visible_version.clone()),
            required_replica_node_ids: commit.required_replica_node_ids.clone(),
        })?;
        self.materialize_visible_version_commit(commit, entry.sequence)?;
        Ok(())
    }

    fn materialize_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        sequence: u64,
    ) -> Result<()> {
        self.append_and_apply_table_ops(
            sequence,
            vec![DfsMetadataTableOp::PutVisibleCommit {
                object_id: commit.committed_object.object_id.clone(),
                version: commit.committed_object.version.clone(),
                commit: commit.clone(),
            }],
        )?;
        self.materialize_object_indexes(&commit.committed_object, sequence)?;
        let visible_version = normalized_version_record(&commit.visible_version);
        self.append_and_apply_table_ops(
            sequence,
            vec![DfsMetadataTableOp::PutVersion {
                version: visible_version,
            }],
        )
    }

    fn read_or_create_dirfrag(
        &self,
        file_system_id: &DfsFileSystemId,
        dir_inode_id: &DfsInodeId,
        dirfrag_id: &DfsDirFragmentId,
        name_hash: Option<u64>,
        increment_entry_count: bool,
    ) -> Result<DfsDirFragmentRecord> {
        let (hash_start_inclusive, hash_end_exclusive) = dirfrag_hash_range(dirfrag_id, name_hash);
        let mut dirfrag = self
            .tables
            .dirfrags
            .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
            .cloned()
            .unwrap_or(DfsDirFragmentRecord {
                file_system_id: file_system_id.clone(),
                dir_inode_id: dir_inode_id.clone(),
                dirfrag_id: dirfrag_id.clone(),
                hash_start_inclusive,
                hash_end_exclusive,
                entry_count: 0,
                split_after_count: DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT,
            });
        if increment_entry_count {
            dirfrag.entry_count = dirfrag.entry_count.saturating_add(1);
        }
        Ok(dirfrag)
    }
}

fn path_record_for_object(object: &DfsObjectMetadata) -> DfsPathRecord {
    let normalized = normalize_path(&object.path);
    let name = normalized
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("/")
        .to_string();
    DfsPathRecord {
        file_system_id: object.file_system_id.clone(),
        path: normalized.clone(),
        object_id: object.object_id.clone(),
        inode_id: object_inode_id(&object.object_id),
        parent_inode_id: parent_inode_id(&object.file_system_id, &normalized),
        dirfrag_id: DfsDirFragmentId::new("root"),
        name_hash: stable_u64_hash(&name),
        name,
        kind: object.kind.clone(),
    }
}

fn bucket_dirfrag_id(name_hash: u64) -> DfsDirFragmentId {
    let bucket = dirfrag_bucket(name_hash);
    DfsDirFragmentId::new(format!("bucket-{bucket:02x}"))
}

fn dirfrag_bucket(name_hash: u64) -> u64 {
    name_hash
        .checked_div((u64::MAX / DIRFRAG_BUCKET_COUNT).saturating_add(1))
        .unwrap_or(0)
        .min(DIRFRAG_BUCKET_COUNT - 1)
}

fn dirfrag_hash_range(dirfrag_id: &DfsDirFragmentId, name_hash: Option<u64>) -> (u64, u64) {
    if dirfrag_id.0 == "root" {
        return (0, u64::MAX);
    }
    let bucket = name_hash
        .map(dirfrag_bucket)
        .or_else(|| {
            dirfrag_id
                .0
                .strip_prefix("bucket-")
                .and_then(|bucket| u64::from_str_radix(bucket, 16).ok())
        })
        .unwrap_or(0)
        .min(DIRFRAG_BUCKET_COUNT - 1);
    let width = (u64::MAX / DIRFRAG_BUCKET_COUNT).saturating_add(1);
    let start = bucket.saturating_mul(width);
    let end = if bucket + 1 >= DIRFRAG_BUCKET_COUNT {
        u64::MAX
    } else {
        (bucket + 1).saturating_mul(width)
    };
    (start, end)
}

fn normalized_object_metadata(object: &DfsObjectMetadata) -> DfsObjectMetadata {
    let mut object = object.clone();
    normalize_node_locations(&mut object.current_locations);
    object
}

fn normalized_version_record(version: &DfsObjectVersionRecord) -> DfsObjectVersionRecord {
    let mut version = version.clone();
    normalize_node_locations(&mut version.locations);
    version
}

fn normalized_namespace_transaction(
    transaction: &DfsNamespaceTransaction,
) -> DfsNamespaceTransaction {
    let mut transaction = transaction.clone();
    for object in &mut transaction.objects {
        normalize_node_locations(&mut object.current_locations);
    }
    transaction
}

fn required_replay_payload<'a, T>(
    entry: &DfsMetadataJournalEntry,
    mutation: &str,
    payload: Option<&'a T>,
) -> Result<&'a T> {
    payload.ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {}:{} cannot replay {mutation} without its record payload",
            entry.shard_id.0, entry.sequence
        ))
    })
}

fn journal_entry_has_replay_payloads(entry: &DfsMetadataJournalEntry) -> bool {
    match &entry.mutation {
        DfsMetadataMutation::PutFileSystem { file_system, .. } => file_system.is_some(),
        DfsMetadataMutation::PutObject { object, .. } => object.is_some(),
        DfsMetadataMutation::PutVersion { version_record, .. } => version_record.is_some(),
        DfsMetadataMutation::PutReceipt { receipt, .. } => receipt.is_some(),
        DfsMetadataMutation::PutIntent { intent, .. } => intent.is_some(),
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object,
            visible_version,
            ..
        } => committed_object.is_some() && visible_version.is_some(),
        DfsMetadataMutation::PutConflict { conflict, .. } => conflict.is_some(),
        DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => transaction.is_some(),
    }
}

fn normalized_node_location_list(nodes: &[NodeId]) -> Vec<NodeId> {
    let mut nodes = nodes.to_vec();
    normalize_node_locations(&mut nodes);
    nodes
}

fn normalize_node_locations(nodes: &mut Vec<NodeId>) {
    let mut seen = BTreeSet::new();
    nodes.retain(|node_id| seen.insert(node_id.clone()));
}

fn object_inode_id(object_id: &DfsObjectId) -> DfsInodeId {
    DfsInodeId::new(format!("obj:{}", object_id))
}

fn parent_inode_id(file_system_id: &DfsFileSystemId, normalized_path: &str) -> DfsInodeId {
    let parent = normalized_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("");
    if parent.is_empty() {
        DfsInodeId::new(format!("fs:{}:root", file_system_id))
    } else {
        DfsInodeId::new(format!("path:{}:{}", file_system_id, path_hash(parent)))
    }
}

fn directory_inode_id(file_system_id: &DfsFileSystemId, directory_path: &str) -> DfsInodeId {
    let normalized = normalize_path(directory_path);
    if normalized.is_empty() {
        DfsInodeId::new(format!("fs:{}:root", file_system_id))
    } else {
        DfsInodeId::new(format!(
            "path:{}:{}",
            file_system_id,
            path_hash(&normalized)
        ))
    }
}

fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn normalize_posix_path(path: &str) -> String {
    let normalized = normalize_path(path);
    if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized}")
    }
}

fn path_hash(path: &str) -> String {
    blake3::hash(normalize_path(path).as_bytes())
        .to_hex()
        .to_string()
}

fn stable_u64_hash(value: &str) -> u64 {
    let hash = blake3::hash(value.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_be_bytes(bytes)
}

fn key_segment(value: &impl ToString) -> String {
    value
        .to_string()
        .trim_matches('/')
        .replace(['/', '\\', ':'], "_")
}

fn dentry_key(path_record: &DfsPathRecord) -> (DfsInodeId, DfsDirFragmentId, u64, String) {
    (
        path_record.parent_inode_id.clone(),
        path_record.dirfrag_id.clone(),
        path_record.name_hash,
        path_record.name.clone(),
    )
}

fn metadata_table_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("materialized.slog")
}

fn metadata_table_snapshot_path(root: &Path) -> PathBuf {
    root.join("records").join("tables").join("snapshot.sobj")
}

fn load_or_migrate_metadata_tables(root: &Path) -> Result<DfsMetadataTableState> {
    let mut state = read_metadata_table_state(root)?;
    if state.tables.has_materialized_state() {
        return Ok(state);
    }
    state.tables = load_legacy_metadata_tables(root)?;
    if state.tables.has_materialized_state() {
        persist_metadata_table_snapshot(root, &state.tables)?;
        let table_path = metadata_table_path(root);
        state.table_log_offset = table_path
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
    }
    Ok(state)
}

fn read_metadata_table_state(root: &Path) -> Result<DfsMetadataTableState> {
    let mut state = read_metadata_table_snapshot(root)?;
    let table_path = metadata_table_path(root);
    if table_path.is_file() {
        state.table_log_offset =
            replay_metadata_table_batches_from(&table_path, 0, &mut state.tables)?;
    }
    Ok(state)
}

fn read_metadata_table_snapshot(root: &Path) -> Result<DfsMetadataTableState> {
    let path = metadata_table_snapshot_path(root);
    if !path.is_file() {
        return Ok(DfsMetadataTableState::default());
    }
    let snapshot: DfsMetadataTableSnapshot = read_json(&path)?;
    if snapshot.format_version != METADATA_TABLE_SNAPSHOT_FORMAT_VERSION {
        return Err(MetadataBackendError::Serde(format!(
            "unsupported DFS metadata table snapshot version {} at {}",
            snapshot.format_version,
            path.display()
        )));
    }
    let mut tables = DfsMetadataTables::default();
    for op in snapshot.ops {
        tables.apply(op);
    }
    Ok(DfsMetadataTableState {
        tables,
        table_log_offset: 0,
    })
}

fn replay_metadata_table_batches_from(
    path: &Path,
    offset: u64,
    tables: &mut DfsMetadataTables,
) -> Result<u64> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let len = file
        .metadata()
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if offset > len {
        return Err(MetadataBackendError::Serde(format!(
            "DFS metadata table cursor {} is beyond table log length {} at {}",
            offset,
            len,
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?;
    replay_metadata_table_batches(&mut file, path, tables)
}

fn replay_metadata_table_batches(
    file: &mut fs::File,
    path: &Path,
    tables: &mut DfsMetadataTables,
) -> Result<u64> {
    let mut offset = file
        .stream_position()
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?;
    loop {
        let mut length_bytes = [0_u8; 4];
        match file.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
        }
        let length = u32::from_le_bytes(length_bytes) as usize;
        offset = offset.saturating_add(4);
        let mut bytes = vec![0_u8; length];
        match file.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
        }
        let batch: DfsMetadataTableBatch = sinomo_object_serde::from_bytes(&bytes)
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
        for op in batch.ops {
            tables.apply(op);
        }
        offset = offset.saturating_add(u64::try_from(length).unwrap_or(u64::MAX));
    }
    Ok(offset)
}

fn append_metadata_table_batch(root: &Path, batch: &DfsMetadataTableBatch) -> Result<u64> {
    let path = metadata_table_path(root);
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let bytes = sinomo_object_serde::to_bytes(batch, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    let length = u32::try_from(bytes.len()).map_err(|_| {
        MetadataBackendError::Serde(format!(
            "DFS metadata table batch {} is too large: {} bytes",
            batch.sequence,
            bytes.len()
        ))
    })?;
    let mut record = Vec::with_capacity(4 + bytes.len());
    record.extend_from_slice(&length.to_le_bytes());
    record.extend_from_slice(&bytes);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    file.write_all(&record)
        .and_then(|()| file.sync_all())
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(u64::try_from(record.len()).unwrap_or(u64::MAX))
}

fn persist_metadata_table_snapshot(root: &Path, tables: &DfsMetadataTables) -> Result<()> {
    write_json_atomic(
        &metadata_table_snapshot_path(root),
        &DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            ops: metadata_table_snapshot_ops(tables),
        },
    )?;
    truncate_metadata_table_log(root)?;
    Ok(())
}

fn metadata_table_snapshot_ops(tables: &DfsMetadataTables) -> Vec<DfsMetadataTableOp> {
    let mut ops = Vec::new();
    ops.extend(
        tables
            .file_systems
            .values()
            .cloned()
            .map(|file_system| DfsMetadataTableOp::PutFileSystem { file_system }),
    );
    ops.extend(
        tables
            .objects
            .values()
            .cloned()
            .map(|object| DfsMetadataTableOp::PutObject { object }),
    );
    ops.extend(
        tables
            .inodes
            .values()
            .cloned()
            .map(|inode| DfsMetadataTableOp::PutInode { inode }),
    );
    ops.extend(
        tables
            .file_system_objects
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutFileSystemObject { path_record }),
    );
    ops.extend(
        tables
            .paths
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutPath { path_record }),
    );
    ops.extend(
        tables
            .dirfrags
            .values()
            .cloned()
            .map(|dirfrag| DfsMetadataTableOp::PutDirFragment { dirfrag }),
    );
    ops.extend(
        tables
            .dentries
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutDentry { path_record }),
    );
    ops.extend(tables.hardlinks.iter().map(
        |((target_object_id, hardlink_object_id), path_record)| DfsMetadataTableOp::PutHardlink {
            target_object_id: target_object_id.clone(),
            hardlink_object_id: hardlink_object_id.clone(),
            path_record: path_record.clone(),
        },
    ));
    ops.extend(
        tables
            .versions
            .values()
            .cloned()
            .map(|version| DfsMetadataTableOp::PutVersion { version }),
    );
    ops.extend(
        tables
            .receipts
            .values()
            .cloned()
            .map(|receipt| DfsMetadataTableOp::PutReceipt { receipt }),
    );
    ops.extend(
        tables
            .intents
            .values()
            .cloned()
            .map(|intent| DfsMetadataTableOp::PutIntent { intent }),
    );
    ops.extend(
        tables
            .conflicts
            .values()
            .cloned()
            .map(|conflict| DfsMetadataTableOp::PutConflict { conflict }),
    );
    ops.extend(
        tables
            .visible_commits
            .iter()
            .map(
                |((object_id, version), commit)| DfsMetadataTableOp::PutVisibleCommit {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    commit: commit.clone(),
                },
            ),
    );
    ops.extend(
        tables
            .namespace_transactions
            .values()
            .cloned()
            .map(|transaction| DfsMetadataTableOp::PutNamespaceTransaction { transaction }),
    );
    ops
}

fn truncate_metadata_table_log(root: &Path) -> Result<()> {
    let path = metadata_table_path(root);
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    file.sync_all().map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn load_legacy_metadata_tables(root: &Path) -> Result<DfsMetadataTables> {
    let records = root.join("records");
    let mut tables = DfsMetadataTables::default();
    for file_system in read_json_tree::<DfsFileSystem>(&records.join("filesystems"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
    }
    for object in read_json_tree::<DfsObjectMetadata>(&records.join("objects"))? {
        let object = normalized_object_metadata(&object);
        let path_record = path_record_for_object(&object);
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutInode {
            inode: DfsInodeRecord::from(&object),
        });
        tables.apply(DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutPath {
            path_record: path_record.clone(),
        });
        if object.kind != DfsObjectKind::Tombstone {
            tables.apply(DfsMetadataTableOp::PutDentry {
                path_record: path_record.clone(),
            });
        }
        if object.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = object.link_target_object_id.as_ref()
        {
            tables.apply(DfsMetadataTableOp::PutHardlink {
                target_object_id: target_object_id.clone(),
                hardlink_object_id: object.object_id.clone(),
                path_record,
            });
        }
    }
    for path_record in read_json_tree::<DfsPathRecord>(&records.join("filesystem-objects"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystemObject { path_record });
    }
    for path_record in read_json_tree::<DfsPathRecord>(&records.join("paths"))? {
        tables.apply(DfsMetadataTableOp::PutPath { path_record });
    }
    for inode in read_json_tree::<DfsInodeRecord>(&records.join("inodes"))? {
        tables.apply(DfsMetadataTableOp::PutInode { inode });
    }
    for dirfrag in read_json_tree::<DfsDirFragmentRecord>(&records.join("dirfrags"))? {
        tables.apply(DfsMetadataTableOp::PutDirFragment { dirfrag });
    }
    for path_record in read_json_tree::<DfsPathRecord>(&records.join("dentries"))? {
        tables.apply(DfsMetadataTableOp::PutDentry { path_record });
    }
    for version in read_json_tree::<DfsObjectVersionRecord>(&records.join("versions"))? {
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(&version),
        });
    }
    for receipt in read_json_tree::<DfsReplicaReceiptRecord>(&records.join("receipts"))? {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in read_json_tree::<DfsWriteIntentRecord>(&records.join("intents"))? {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
    }
    for conflict in read_json_tree::<DfsConflictRecord>(&records.join("conflicts"))? {
        tables.apply(DfsMetadataTableOp::PutConflict { conflict });
    }
    for commit in read_json_tree::<DfsVisibleVersionCommitRecord>(&records.join("visible-commits"))?
    {
        tables.apply(DfsMetadataTableOp::PutVisibleCommit {
            object_id: commit.committed_object.object_id.clone(),
            version: commit.committed_object.version.clone(),
            commit,
        });
    }
    for transaction in
        read_json_tree::<DfsNamespaceTransaction>(&records.join("namespace-transactions"))?
    {
        tables.apply(DfsMetadataTableOp::PutNamespaceTransaction { transaction });
    }
    Ok(tables)
}

fn fast_next_journal_sequence(root: &Path) -> Result<u64> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(1);
    }
    if let Some(head) =
        read_json_optional::<DfsMetadataJournalHead>(&journal_dir.join("head.sobj"))?
        && head.next_sequence > 0
    {
        return Ok(head.next_sequence);
    }
    recover_next_journal_sequence(root)
}

fn recover_next_journal_sequence(root: &Path) -> Result<u64> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(1);
    }
    let head_path = journal_dir.join("head.sobj");
    let head_next_sequence =
        read_json_optional::<DfsMetadataJournalHead>(&head_path)?.map(|head| head.next_sequence);
    let mut max_sequence = head_next_sequence
        .map(|next_sequence| next_sequence.saturating_sub(1))
        .unwrap_or(0);

    max_sequence = max_sequence.max(max_segmented_journal_sequence(root)?);
    for (sequence, _, _) in legacy_journal_entry_paths_since(root, 0)? {
        max_sequence = max_sequence.max(sequence);
    }

    let next_sequence = max_sequence.saturating_add(1).max(1);
    if head_next_sequence != Some(next_sequence) {
        write_json_atomic(&head_path, &DfsMetadataJournalHead { next_sequence })?;
    }
    Ok(next_sequence)
}

fn highest_segmented_journal_path(root: &Path) -> Result<Option<PathBuf>> {
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(None);
    }
    let mut highest: Option<(u64, PathBuf)> = None;
    for entry in read_dir(&segments_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("slog") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = stem.parse::<u64>() else {
            continue;
        };
        if highest
            .as_ref()
            .is_none_or(|(highest_segment_id, _)| segment_id > *highest_segment_id)
        {
            highest = Some((segment_id, path));
        }
    }
    Ok(highest.map(|(_, path)| path))
}

fn journal_head_next_sequence(root: &Path) -> Result<Option<u64>> {
    Ok(
        read_json_optional::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))?
            .and_then(|head| (head.next_sequence > 0).then_some(head.next_sequence)),
    )
}

fn journal_has_entries_after_head(root: &Path, after_sequence: u64) -> Result<bool> {
    let Some(next_sequence) = journal_head_next_sequence(root)? else {
        return Ok(true);
    };
    Ok(after_sequence < next_sequence.saturating_sub(1))
}

fn max_segmented_journal_sequence(root: &Path) -> Result<u64> {
    let Some(path) = highest_segmented_journal_path(root)? else {
        return Ok(0);
    };
    let mut max_sequence = 0;
    for entry in read_segmented_journal_file_since_limited(&path, 0, usize::MAX)? {
        max_sequence = max_sequence.max(entry.sequence);
    }
    Ok(max_sequence)
}

fn append_segmented_journal_entry(root: &Path, entry: &DfsMetadataJournalEntry) -> Result<()> {
    let bytes = sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    let length = u32::try_from(bytes.len()).map_err(|_| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {} is too large: {} bytes",
            entry.sequence,
            bytes.len()
        ))
    })?;
    let segment_id = journal_segment_id(entry.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut record = Vec::with_capacity(4 + bytes.len());
    record.extend_from_slice(&length.to_le_bytes());
    record.extend_from_slice(&bytes);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    file.write_all(&record)
        .and_then(|()| file.sync_all())
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn journal_entries_since(root: &Path, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
    journal_entries_since_limited(root, after_sequence, usize::MAX)
}

fn journal_entries_since_limited(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    segmented_journal_entries_since_limited(root, after_sequence, max_entries)
}

fn bounded_metadata_journal_batch(
    shard_id: DfsMetadataShardId,
    after_sequence: u64,
    entries: Vec<DfsMetadataJournalEntry>,
    max_entries: usize,
    max_encoded_bytes: usize,
) -> Result<Option<DfsMetadataJournalBatch>> {
    if max_entries == 0 {
        return Ok(None);
    }
    let max_encoded_bytes = max_encoded_bytes.max(1);
    let mut selected = Vec::new();
    let mut next_after_sequence = after_sequence;
    for entry in entries.into_iter().take(max_entries) {
        let candidate_sequence = entry.sequence;
        selected.push(entry);
        let candidate = DfsMetadataJournalBatch {
            shard_id: selected
                .last()
                .map(|entry| entry.shard_id.clone())
                .unwrap_or_else(|| shard_id.clone()),
            entries: selected.clone(),
            next_after_sequence: candidate_sequence,
        };
        if selected.len() > 1 && metadata_journal_batch_encoded_len(&candidate)? > max_encoded_bytes
        {
            selected.pop();
            break;
        }
        next_after_sequence = candidate_sequence;
    }
    if selected.is_empty() {
        return Ok(None);
    }
    Ok(Some(DfsMetadataJournalBatch {
        shard_id: selected
            .first()
            .map(|entry| entry.shard_id.clone())
            .unwrap_or(shard_id),
        entries: selected,
        next_after_sequence,
    }))
}

fn metadata_journal_batch_encoded_len(batch: &DfsMetadataJournalBatch) -> Result<usize> {
    sinomo_object_serde::to_bytes(batch, 1, LengthMode::U32)
        .map(|bytes| bytes.len())
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

fn migrate_legacy_journal_entries(root: &Path) -> Result<()> {
    let legacy_entries = legacy_journal_entry_paths_since(root, 0)?;
    if legacy_entries.is_empty() {
        return Ok(());
    }
    for (sequence, path, entry) in legacy_entries {
        if !segmented_journal_contains_sequence(root, sequence)? {
            append_segmented_journal_entry(root, &entry)?;
        }
        remove_file_if_exists(&path)?;
    }
    Ok(())
}

fn segmented_journal_contains_sequence(root: &Path, sequence: u64) -> Result<bool> {
    Ok(segmented_journal_entry_at_sequence(root, sequence)?.is_some())
}

fn segmented_journal_entry_at_sequence(
    root: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let segment_id = journal_segment_id(sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    if !path.exists() {
        return Ok(None);
    }
    read_segmented_journal_file_at_sequence(&path, sequence)
}

fn legacy_journal_entry_paths_since(
    root: &Path,
    after_sequence: u64,
) -> Result<Vec<(u64, PathBuf, DfsMetadataJournalEntry)>> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = BTreeMap::new();
    for entry in read_dir(&journal_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sobj")
            || path.file_name().and_then(|name| name.to_str()) == Some("head.sobj")
        {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(sequence) = stem.parse::<u64>() else {
            continue;
        };
        if sequence > after_sequence {
            paths.insert(sequence, path);
        }
    }
    let mut entries = Vec::new();
    for path in paths.into_values() {
        let entry: DfsMetadataJournalEntry = read_json(&path)?;
        entries.push((entry.sequence, path, entry));
    }
    Ok(entries)
}

fn segmented_journal_entries_since_limited(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    if !journal_has_entries_after_head(root, after_sequence)? {
        return Ok(Vec::new());
    }
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(Vec::new());
    }
    let first_segment = journal_segment_id(after_sequence.saturating_add(1));
    let mut paths = BTreeMap::new();
    for entry in read_dir(&segments_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("slog") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = stem.parse::<u64>() else {
            continue;
        };
        if segment_id >= first_segment {
            paths.insert(segment_id, path);
        }
    }
    let mut entries = Vec::new();
    for path in paths.into_values() {
        let remaining = max_entries.saturating_sub(entries.len());
        if remaining == 0 {
            break;
        }
        entries.extend(read_segmented_journal_file_since_limited(
            &path,
            after_sequence,
            remaining,
        )?);
        if entries.len() >= max_entries {
            break;
        }
    }
    entries.sort_by_key(|entry| entry.sequence);
    entries.truncate(max_entries);
    Ok(entries)
}

fn read_segmented_journal_file_since_limited(
    path: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut entries = Vec::new();
    while let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? {
        if entry.sequence > after_sequence {
            entries.push(entry);
            if entries.len() >= max_entries {
                break;
            }
        }
    }
    Ok(entries)
}

fn read_segmented_journal_file_at_sequence(
    path: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    while let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? {
        if entry.sequence == sequence {
            return Ok(Some(entry));
        }
        if entry.sequence > sequence {
            break;
        }
    }
    Ok(None)
}

fn read_next_segmented_journal_entry(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let mut length_bytes = [0_u8; 4];
    match file.read_exact(&mut length_bytes) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            });
        }
    }
    let length = u32::from_le_bytes(length_bytes) as usize;
    let mut bytes = vec![0_u8; length];
    match file.read_exact(&mut bytes) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            });
        }
    }
    let entry = sinomo_object_serde::from_bytes(&bytes)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    Ok(Some(entry))
}

fn journal_segment_id(sequence: u64) -> u64 {
    sequence
        .saturating_sub(1)
        .checked_div(JOURNAL_SEGMENT_ENTRY_LIMIT)
        .unwrap_or(0)
        .saturating_add(1)
}

fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(&path).map_err(|source| MetadataBackendError::Io { path, source })
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(())
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })
}

fn read_json<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    sinomo_object_serde::from_bytes(&read_file(path)?)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

fn read_json_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }
    read_json(path).map(Some)
}

fn read_json_tree<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    read_json_tree_into(path, &mut records)?;
    Ok(records)
}

fn read_json_tree_into<T>(path: &Path, records: &mut Vec<T>) -> Result<()>
where
    T: DeserializeOwned,
{
    for entry in read_dir(path)? {
        let path = entry.path();
        if path.is_dir() {
            read_json_tree_into(&path, records)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sobj") {
            records.push(read_json(&path)?);
        }
    }
    Ok(())
}

fn write_json_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    write_json_atomic_if_changed(path, value).map(|_| ())
}

fn write_json_atomic_if_changed<T>(path: &Path, value: &T) -> Result<bool>
where
    T: Serialize,
{
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    if path.is_file() {
        match fs::read(path) {
            Ok(existing) if existing == bytes => return Ok(false),
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
        }
    }
    write_file_atomic(path, &bytes)?;
    Ok(true)
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let tmp_path = temporary_path_for(path);
    let mut file = fs::File::create(&tmp_path).map_err(|source| MetadataBackendError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| MetadataBackendError::Io {
            path: tmp_path.clone(),
            source,
        })?;
    drop(file);
    fs::rename(&tmp_path, path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    sync_dir_impl(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(not(windows))]
fn sync_dir_impl(path: &Path) -> io::Result<()> {
    open_dir_for_sync(path).and_then(|dir| dir.sync_all())
}

#[cfg(windows)]
fn sync_dir_impl(path: &Path) -> io::Result<()> {
    match open_dir_for_sync(path).and_then(|dir| dir.sync_all()) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::PermissionDenied => Ok(()),
        Err(source) => Err(source),
    }
}

#[cfg(not(windows))]
fn open_dir_for_sync(path: &Path) -> io::Result<fs::File> {
    fs::File::open(path)
}

#[cfg(windows)]
fn open_dir_for_sync(path: &Path) -> io::Result<fs::File> {
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

fn temporary_path_for(path: &Path) -> PathBuf {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(1);
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tmp");
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        counter
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsMetadataShardAssignment;
    use sinomo_core::DfsObjectPolicy;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn file_system() -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        }
    }

    fn object(version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "docs/readme.md".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn shard_config(
        shard_id: &str,
        assignment: DfsMetadataShardAssignment,
    ) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: shard_id.to_string(),
            primary_node_id: node("node_owner"),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn table_path_record(
        backend: &LocalJsonMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> DfsPathRecord {
        backend
            .tables
            .paths
            .get(&(file_system_id.clone(), normalize_path(path)))
            .cloned()
            .expect("path table record")
    }

    fn table_path_exists(
        backend: &LocalJsonMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> bool {
        backend
            .tables
            .paths
            .contains_key(&(file_system_id.clone(), normalize_path(path)))
    }

    fn table_dentry_exists(
        backend: &LocalJsonMetadataBackend,
        path_record: &DfsPathRecord,
    ) -> bool {
        backend
            .tables
            .dentries
            .contains_key(&dentry_key(path_record))
    }

    fn table_dirfrag(
        backend: &LocalJsonMetadataBackend,
        dir_inode_id: &DfsInodeId,
        dirfrag_id: &DfsDirFragmentId,
    ) -> DfsDirFragmentRecord {
        backend
            .tables
            .dirfrags
            .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
            .cloned()
            .expect("dirfrag table record")
    }

    fn table_file_system_object_exists(
        backend: &LocalJsonMetadataBackend,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .file_system_objects
            .contains_key(&(file_system_id.clone(), object_id.clone()))
    }

    fn table_hardlink_exists(
        backend: &LocalJsonMetadataBackend,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .hardlinks
            .contains_key(&(target_object_id.clone(), hardlink_object_id.clone()))
    }

    fn force_table_replay_from_journal(
        backend: &LocalJsonMetadataBackend,
        root: &Path,
        last_materialized_sequence: u64,
    ) {
        remove_file_if_exists(&metadata_table_path(root)).expect("remove metadata table log");
        remove_file_if_exists(&metadata_table_snapshot_path(root))
            .expect("remove metadata table snapshot");
        write_json_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: backend.shard_id.clone(),
                last_sequence: last_materialized_sequence,
            },
        )
        .expect("rewind materialization cursor");
    }

    #[test]
    fn local_backend_persists_file_system_object_path_and_journal() {
        let root = temp_root("local_backend_persists_file_system_object_path_and_journal");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        let file_system_entry = backend.put_file_system(&file_system).expect("filesystem");
        let object_entry = backend.put_object(&object).expect("object");

        assert_eq!(file_system_entry.sequence, 1);
        assert_eq!(object_entry.sequence, 2);
        assert_eq!(
            file_system_entry.mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system.file_system_id.clone(),
                file_system: Some(file_system.clone()),
            },
            "filesystem journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            object_entry.mutation,
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            },
            "object journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            backend
                .get_file_system(&file_system.file_system_id)
                .expect("get DfsFileSystem"),
            Some(file_system)
        );
        assert_eq!(
            backend.get_object(&object.object_id).expect("get object"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_object_by_path(&object.file_system_id, "docs/readme.md")
                .expect("get by path"),
            Some(object.clone())
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        assert!(
            metadata_table_path(&root).is_file(),
            "new metadata writes should materialize through the packed table log"
        );
        assert!(
            !root
                .join("records")
                .join("objects")
                .join(key_segment(&object.object_id))
                .join("object.sobj")
                .exists(),
            "new object writes should not create one file per object metadata record"
        );
        assert!(
            !root
                .join("records")
                .join("paths")
                .join(key_segment(&object.file_system_id))
                .join(format!("{}.sobj", path_hash(&object.path)))
                .exists(),
            "new object writes should not create one file per path index record"
        );

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_routes_object_and_version_to_selected_path_shard() {
        let root = temp_root("sharded_backend_routes_object_and_version");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        };

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend.put_version(&version_record).expect("version");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/services/readme.md")
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version"),
            Some(version_record)
        );
        let services_batch = backend
            .journal_batch_for_shard_since_limited(
                &DfsMetadataShardId::new("services"),
                0,
                16,
                usize::MAX,
            )
            .expect("services batch")
            .expect("services changes");
        assert!(
            services_batch.entries.iter().any(|entry| matches!(
                entry.mutation,
                DfsMetadataMutation::PutObject { .. } | DfsMetadataMutation::PutVersion { .. }
            )),
            "path-selected shard should own object and version journal entries"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_moves_object_between_path_shards_without_scanning_on_read() {
        let root = temp_root("sharded_backend_moves_object_between_path_shards");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("services object");

        let mut moved = object.clone();
        moved.path = "/users/readme.md".to_string();
        moved.version = version("version_2");
        backend.put_object(&moved).expect("moved object");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get moved object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/users/readme.md")
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.path)
                .collect::<Vec<_>>(),
            vec!["/users/readme.md".to_string()]
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/readme.md")
                .expect("old path")
                .map(|object| object.kind),
            Some(DfsObjectKind::Tombstone)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_does_not_materialize_object_when_journal_append_fails() {
        let root = temp_root("local_backend_journal_failure_does_not_materialize_object");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        fs::remove_dir_all(root.join("journal").join("segments")).expect("remove segments dir");
        fs::write(root.join("journal").join("segments"), b"not a directory")
            .expect("block journal segment writes");

        let error = backend
            .put_object(&object)
            .expect_err("object write should fail before materialization when the journal fails");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            None,
            "object metadata must not become visible without a journal entry"
        );
        assert!(
            !table_path_exists(&backend, &object.file_system_id, &object.path),
            "path index must not become visible without a journal entry"
        );

        fs::remove_file(root.join("journal").join("segments")).ok();
        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_entries_after_materialization_cursor() {
        let root = temp_root("local_backend_replays_after_materialization_cursor");
        let object = object(version("version_1"));
        let journaled_sequence = {
            let mut backend =
                LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            let file_system = file_system();
            backend.put_file_system(&file_system).expect("filesystem");
            let entry = backend
                .write_journal(DfsMetadataMutation::PutObject {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    path: object.path.clone(),
                    object: Some(object.clone()),
                })
                .expect("journal object without materializing");
            assert_eq!(
                backend
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                None
            );
            entry.sequence
        };

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone()),
            "reopen should replay journaled metadata that was not yet materialized"
        );
        assert_eq!(
            read_json::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            journaled_sequence
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_stale_materialization_cursor_without_rewriting_table_log() {
        let root = temp_root("local_backend_stale_materialization_cursor_no_rewrite");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalJsonMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let entry = backend.put_object(&object).expect("object");
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (entry.sequence, table_len)
        };
        write_json_atomic(
            &root
                .join("records")
                .join("local-materialization")
                .join("cursor.sobj"),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 0,
            },
        )
        .expect("stale materialization cursor");

        let reopened = LocalJsonMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object),
            "stale cursor recovery must preserve the materialized object"
        );
        assert_eq!(
            read_json::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent cursor recovery must not append duplicate table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_open_does_not_rewrite_existing_metadata_table() {
        let root = temp_root("local_backend_open_preserves_metadata_table");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let table_path = metadata_table_path(&root);
        let before_len = fs::metadata(&table_path).expect("metadata table").len();

        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        let after_len = fs::metadata(&table_path)
            .expect("metadata table after reopen")
            .len();
        assert_eq!(
            before_len, after_len,
            "opening a healthy backend must not rewrite the materialized metadata table"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_compacts_materialized_table_log_to_snapshot() {
        let root = temp_root("local_backend_compacts_materialized_table_log");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut expected_objects = Vec::new();
        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_compacted_metadata_table_{index}"))
                    .expect("object");
            object.path = format!("/docs/compacted-{index}.md");
            backend.put_object(&object).expect("object");
            expected_objects.push(object);
        }
        let table_path = metadata_table_path(&root);
        assert!(
            fs::metadata(&table_path).expect("metadata table").len() > 0,
            "writes should first append to the materialized table log"
        );

        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact metadata table");

        assert!(
            metadata_table_snapshot_path(&root).is_file(),
            "compaction should persist a compact materialized table snapshot"
        );
        assert_eq!(
            fs::metadata(&table_path)
                .expect("truncated metadata table")
                .len(),
            0,
            "compaction should truncate historical table ops after the snapshot is durable"
        );
        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen compacted backend");
        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object)
            );
        }
        cleanup(&root);
    }

    #[test]
    fn local_backend_allocates_journal_sequences_across_open_handles() {
        let root = temp_root("local_backend_allocates_journal_sequences_across_open_handles");
        let mut first = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let mut second =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("second backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.object_id = DfsObjectId::from_legacy_named("obj_second_handle").expect("object");
        object.path = "/docs/second.md".to_string();

        let first_entry = first.put_file_system(&file_system).expect("filesystem");
        let second_entry = second
            .put_object(&object)
            .expect("object from second handle");

        assert_eq!(first_entry.sequence, 1);
        assert_eq!(
            second_entry.sequence, 2,
            "a second backend opened before the first write must not reuse the first sequence"
        );
        let journal = first.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 2);
        assert!(matches!(
            journal[0].mutation,
            DfsMetadataMutation::PutFileSystem { .. }
        ));
        assert!(matches!(
            journal[1].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));

        cleanup(&root);
    }

    #[test]
    fn local_backend_readers_see_table_batches_from_other_open_handles() {
        let root = temp_root("local_backend_readers_see_table_batches_from_other_open_handles");
        let mut first = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let second = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("second backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        first.put_file_system(&file_system).expect("filesystem");
        first.put_object(&object).expect("object");

        assert_eq!(
            second
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem from second handle"),
            Some(file_system.clone())
        );
        assert_eq!(
            second
                .get_object(&object.object_id)
                .expect("object from second handle"),
            Some(object.clone())
        );
        assert_eq!(
            second
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects from second handle"),
            vec![object]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_serializes_concurrent_metadata_writers() {
        let root = temp_root("local_backend_serializes_concurrent_metadata_writers");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let writer_count = 8usize;
        let writes_per_writer = 16usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(writer_count));
        let mut handles = Vec::new();
        for writer_index in 0..writer_count {
            let root = root.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let mut backend =
                    LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("backend");
                barrier.wait();
                for object_index in 0..writes_per_writer {
                    let mut object = object(version("version_1"));
                    object.object_id = DfsObjectId::from_legacy_named(format!(
                        "obj_concurrent_{writer_index}_{object_index}"
                    ))
                    .expect("object");
                    object.path = format!("/docs/concurrent-{writer_index}-{object_index}.md");
                    backend.put_object(&object).expect("object");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        let backend = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        let journal = backend.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 1 + writer_count * writes_per_writer);
        let sequences = journal
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            sequences,
            (1..=journal.len() as u64).collect::<Vec<_>>(),
            "concurrent metadata writers must allocate contiguous journal sequences"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .len(),
            writer_count * writes_per_writer,
            "concurrent materialization must not lose object indexes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_appends_journal_entries_to_segment_files() {
        let root = temp_root("local_backend_appends_journal_entries_to_segment_files");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");

        assert!(
            root.join("journal").join("head.sobj").is_file(),
            "the journal head should track the next sequence without scanning every append"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "new journal entries should be appended to a segment file"
        );
        assert!(
            !root
                .join("journal")
                .join("00000000000000000001.sobj")
                .exists(),
            "new journal entries should not create one object file per mutation"
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        assert_eq!(backend.journal_since(1).expect("journal").len(), 1);

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(0).expect("journal").len(), 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_uses_head_and_cursor_without_scanning_consumed_segments() {
        let root = temp_root("local_backend_reopen_uses_head_and_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let object = object(version("version_1"));
        let mut backend = LocalJsonMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact table snapshot");

        let high_sequence = JOURNAL_SEGMENT_ENTRY_LIMIT + 1;
        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: shard_id.clone(),
                sequence: high_sequence,
                created_at: OffsetDateTime::now_utc(),
                mutation: DfsMetadataMutation::PutFileSystem {
                    file_system_id: file_system.file_system_id.clone(),
                    file_system: Some(file_system.clone()),
                },
            },
        )
        .expect("high segment entry");
        write_json_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead {
                next_sequence: high_sequence + 1,
            },
        )
        .expect("journal head");
        write_json_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: high_sequence,
            },
        )
        .expect("local cursor");
        fs::write(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog"),
            [1_u8, 0, 0, 0, 0xff],
        )
        .expect("corrupt consumed segment");
        drop(backend);

        let reopened = LocalJsonMetadataBackend::new(shard_id, &root)
            .expect("reopen should not scan consumed journal segments");
        assert_eq!(
            reopened.get_object(&object.object_id).expect("object"),
            Some(object)
        );
        assert!(
            reopened
                .journal_since(high_sequence)
                .expect("journal after head")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_bounded_journal_batches");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        for index in 0..5 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/bounded-{index}.md");
            backend.put_object(&object).expect("object");
        }

        let first_batch = backend
            .journal_since_limited(0, 3)
            .expect("first bounded batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let second_batch = backend
            .journal_since_limited(3, 3)
            .expect("second bounded batch");
        assert_eq!(
            second_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(
            backend
                .journal_since_limited(6, 3)
                .expect("empty bounded batch")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_byte_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_byte_bounded_journal_batches");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalJsonMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        for index in 0..3 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_byte_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/{}/bounded-{index}.md", "wide".repeat(128));
            backend.put_object(&object).expect("object");
        }

        let first_unbounded = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, usize::MAX)
            .expect("unbounded batch")
            .expect("batch");
        let first_entry = first_unbounded.entries[0].clone();
        let one_entry_limit = metadata_journal_batch_encoded_len(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: first_entry.sequence,
            entries: vec![first_entry],
        })
        .expect("encoded length");

        let first_batch = backend
            .journal_batch_since_limited(
                1,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                one_entry_limit,
            )
            .expect("byte bounded batch")
            .expect("batch");
        assert_eq!(
            first_batch
                .entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(first_batch.next_after_sequence, 2);
        assert!(
            metadata_journal_batch_encoded_len(&first_batch).expect("encoded length")
                <= one_entry_limit
        );

        let oversized_first = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, 1)
            .expect("oversized first batch")
            .expect("batch");
        assert_eq!(oversized_first.entries.len(), 1);
        assert_eq!(oversized_first.next_after_sequence, 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_migrates_legacy_journal_files_to_segments() {
        let root = temp_root("local_backend_migrates_legacy_journal_files_to_segments");
        fs::create_dir_all(root.join("journal")).expect("journal dir");
        let legacy_path = root.join("journal").join("00000000000000000001.sobj");
        let legacy_entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-root"),
            sequence: 1,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: Some(file_system()),
            },
        };
        write_json_atomic(&legacy_path, &legacy_entry).expect("legacy journal entry");

        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        assert!(
            !legacy_path.exists(),
            "opening the backend should migrate legacy root journal files out of the live path"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "legacy journal entries should be appended to the segmented journal"
        );
        assert_eq!(
            backend.journal_since(0).expect("journal"),
            vec![legacy_entry]
        );
        let next = backend.put_file_system(&file_system()).expect("filesystem");
        assert_eq!(
            next.sequence, 2,
            "sequence allocation should continue after migrated legacy entries"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_hydrates_legacy_journal_payloads_from_records() {
        let root = temp_root("local_backend_hydrates_legacy_journal_payloads_from_records");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_1"),
                content_hash: Some("blake3:old".to_string()),
                size_bytes: None,
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version");

        let legacy_entries = backend
            .journal_since(0)
            .expect("journal")
            .into_iter()
            .map(|mut entry| {
                match &mut entry.mutation {
                    DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                        *file_system = None;
                    }
                    DfsMetadataMutation::PutObject { object, .. } => {
                        *object = None;
                    }
                    DfsMetadataMutation::PutVersion { version_record, .. } => {
                        *version_record = None;
                    }
                    _ => {}
                }
                entry
            })
            .collect::<Vec<_>>();

        let hydrated = backend
            .hydrate_journal_entry_payloads(legacy_entries)
            .expect("hydrate legacy entries");
        assert!(matches!(
            &hydrated[0].mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[1].mutation,
            DfsMetadataMutation::PutObject {
                object: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[2].mutation,
            DfsMetadataMutation::PutVersion {
                version_record: Some(_),
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn local_backend_syncs_metadata_directories() {
        let root = temp_root("local_backend_syncs_metadata_directories");
        fs::create_dir_all(&root).expect("root dir");

        sync_dir(&root).expect("directory sync should work on supported host filesystems");

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_recovers_next_sequence_from_segment_tail() {
        let root = temp_root("local_backend_reopen_recovers_next_sequence_from_segment_tail");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutObject {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    path: "docs/readme.md".to_string(),
                    object: None,
                },
            },
        )
        .expect("append segment entry without advancing head");

        let mut reopened =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("reopen");
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_after_tail_recovery").expect("object");
        object.path = "docs/after-tail-recovery.md".to_string();
        let entry = reopened.put_object(&object).expect("object");
        assert_eq!(
            entry.sequence, 3,
            "reopen must scan segment tails so a crash after append but before head update cannot reuse a sequence"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_directory_children_from_dentry_index() {
        let root = temp_root("local_backend_lists_directory_children_from_dentry_index");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut nested = object(version("version_1"));
        nested.object_id = DfsObjectId::from_legacy_named("obj_setup").expect("object");
        nested.path = "/docs/guides/setup.md".to_string();
        let mut outside = object(version("version_1"));
        outside.object_id = DfsObjectId::from_legacy_named("obj_lib").expect("object");
        outside.path = "/src/lib.rs".to_string();
        for object in [&readme, &changelog, &nested, &outside] {
            backend.put_object(object).expect("object");
        }
        let mut changelog_tombstone = changelog.clone();
        changelog_tombstone.kind = DfsObjectKind::Tombstone;
        changelog_tombstone.version = version("version_2");
        changelog_tombstone.content_hash = None;
        backend.put_object(&changelog_tombstone).expect("tombstone");

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_routes_new_large_directory_entries_to_hash_fragments() {
        let root = temp_root("local_backend_routes_new_large_directory_entries_to_hash_fragments");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut existing = object(version("version_1"));
        existing.path = "/docs/existing.md".to_string();
        backend.put_object(&existing).expect("existing object");
        let existing_record = table_path_record(&backend, &existing.file_system_id, &existing.path);
        assert_eq!(existing_record.dirfrag_id, DfsDirFragmentId::new("root"));

        let mut root_dirfrag = table_dirfrag(
            &backend,
            &existing_record.parent_inode_id,
            &DfsDirFragmentId::new("root"),
        );
        root_dirfrag.entry_count = root_dirfrag.split_after_count;
        backend
            .append_and_apply_table_ops(
                backend.next_sequence,
                vec![DfsMetadataTableOp::PutDirFragment {
                    dirfrag: root_dirfrag,
                }],
            )
            .expect("force split threshold");

        let mut bucketed = object(version("version_1"));
        bucketed.object_id = DfsObjectId::from_legacy_named("obj_bucketed").expect("object");
        bucketed.path = "/docs/bucketed.md".to_string();
        backend.put_object(&bucketed).expect("bucketed object");

        let bucketed_record = table_path_record(&backend, &bucketed.file_system_id, &bucketed.path);
        assert_ne!(
            bucketed_record.dirfrag_id,
            DfsDirFragmentId::new("root"),
            "large directories should route new entries away from the root dentry fragment"
        );
        assert!(
            table_dentry_exists(&backend, &bucketed_record),
            "bucketed dentry record should be materialized in its selected fragment"
        );

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/bucketed.md", "/docs/existing.md"],
            "directory listing must merge root and bucket fragments"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_content_update_does_not_rewrite_stable_namespace_indexes() {
        let root = temp_root("local_backend_content_update_skips_stable_indexes");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let path_record = table_path_record(&backend, &readme.file_system_id, &readme.path);
        let filesystem_object_record = backend
            .tables
            .file_system_objects
            .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
            .cloned()
            .expect("filesystem object index");
        let dentry_record = backend
            .tables
            .dentries
            .get(&dentry_key(&path_record))
            .cloned()
            .expect("dentry index");
        let dirfrag = table_dirfrag(
            &backend,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id,
        );
        let object_before = backend
            .tables
            .objects
            .get(&readme.object_id)
            .cloned()
            .expect("object record");
        let inode_before = backend
            .tables
            .inodes
            .get(&path_record.inode_id)
            .cloned()
            .expect("inode record");

        readme.content_hash = Some("blake3:new-content".to_string());
        readme.version = version("version_2");
        backend.put_object(&readme).expect("content update");

        assert_eq!(
            backend
                .tables
                .file_system_objects
                .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
                .cloned(),
            Some(filesystem_object_record),
            "content-only updates should keep the filesystem-object namespace index stable"
        );
        assert_eq!(
            table_path_record(&backend, &readme.file_system_id, &readme.path),
            path_record,
            "content-only updates should keep the path namespace index stable"
        );
        assert_eq!(
            backend
                .tables
                .dentries
                .get(&dentry_key(&path_record))
                .cloned(),
            Some(dentry_record),
            "content-only updates should keep the dentry namespace index stable"
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            ),
            dirfrag,
            "content-only updates should not change directory fragment accounting"
        );
        assert_ne!(
            backend.tables.objects.get(&readme.object_id).cloned(),
            Some(object_before),
            "the primary object record should still update when content metadata changes"
        );
        assert_ne!(
            backend.tables.inodes.get(&path_record.inode_id).cloned(),
            Some(inode_before),
            "the inode record should still update when visible object metadata changes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_and_dentry_indexes() {
        let root = temp_root("local_backend_removes_stale_path_and_dentry_indexes");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let old_path_record = path_record_for_object(&readme);
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/readme.md"
        ));
        assert!(table_dentry_exists(&backend, &old_path_record));
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.path = "/docs/renamed.md".to_string();
        readme.version = version("version_2");
        backend.put_object(&readme).expect("renamed object");
        assert!(
            !table_path_exists(&backend, &file_system_id(), "/docs/readme.md"),
            "renaming should remove the obsolete path index"
        );
        assert!(
            !table_dentry_exists(&backend, &old_path_record),
            "renaming should remove the obsolete directory entry"
        );
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/renamed.md"
        ));
        let renamed_path_record = path_record_for_object(&readme);
        assert!(table_dentry_exists(&backend, &renamed_path_record));
        assert_eq!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/renamed.md"]
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.kind = DfsObjectKind::Tombstone;
        readme.content_hash = None;
        readme.version = version("version_3");
        backend.put_object(&readme).expect("tombstone object");
        assert!(
            !table_dentry_exists(&backend, &renamed_path_record),
            "tombstoning should remove the visible directory entry"
        );
        assert!(
            table_path_exists(&backend, &file_system_id(), "/docs/renamed.md"),
            "tombstone path record should remain addressable"
        );
        assert!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children after tombstone")
                .is_empty()
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_rebuilds_dirfrag_counts_during_table_replay() {
        let root = temp_root("local_backend_rebuilds_dirfrag_counts");
        {
            let mut backend =
                LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let mut readme = object(version("version_1"));
            readme.path = "/docs/readme.md".to_string();
            let mut changelog = object(version("version_1"));
            changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
            changelog.path = "/docs/changelog.md".to_string();
            for object in [&readme, &changelog] {
                backend.put_object(object).expect("object");
            }

            let path_record = path_record_for_object(&readme);
            assert_eq!(
                table_dirfrag(
                    &backend,
                    &path_record.parent_inode_id,
                    &path_record.dirfrag_id
                )
                .entry_count,
                2
            );
            force_table_replay_from_journal(&backend, &root, 0);
        }

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        let path_record = path_record_for_object(&object(version("version_1")));
        assert_eq!(
            table_dirfrag(
                &recovered,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            )
            .entry_count,
            2
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_objects_from_filesystem_object_index() {
        let root = temp_root("local_backend_lists_objects_from_filesystem_object_index");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut other = object(version("version_1"));
        other.object_id = DfsObjectId::from_legacy_named("obj_other").expect("object");
        other.file_system_id = other_file_system.file_system_id.clone();
        other.path = "/other.txt".to_string();
        for object in [&readme, &changelog, &other] {
            backend.put_object(object).expect("object");
        }
        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &readme.object_id
        ));

        let objects = backend
            .objects_for_file_system(&file_system_id())
            .expect("objects");
        assert_eq!(
            objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/changelog.md", "/docs/readme.md"]
        );

        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &changelog.object_id
        ));
        changelog.file_system_id = other_file_system.file_system_id.clone();
        changelog.path = "/moved-changelog.md".to_string();
        backend.put_object(&changelog).expect("move filesystem");
        assert!(
            !table_file_system_object_exists(&backend, &file_system_id(), &changelog.object_id),
            "moving an object to another filesystem should remove its old filesystem-object index"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_index_when_object_moves_filesystems_without_renaming() {
        let root =
            temp_root("local_backend_removes_stale_path_index_when_object_moves_filesystems");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut object = object(version("version_1"));
        object.path = "/services/shared-name.txt".to_string();
        backend.put_object(&object).expect("object");
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            &object.path
        ));

        object.file_system_id = other_file_system.file_system_id.clone();
        backend
            .put_object(&object)
            .expect("move filesystem without rename");

        assert!(
            !table_path_exists(&backend, &file_system.file_system_id, &object.path),
            "moving an object between filesystems must remove the old filesystem path index even when the path string is unchanged"
        );
        assert!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/shared-name.txt")
                .expect("old filesystem lookup")
                .is_none()
        );
        assert_eq!(
            backend
                .get_object_by_path(
                    &other_file_system.file_system_id,
                    "/services/shared-name.txt"
                )
                .expect("new filesystem lookup")
                .expect("object")
                .object_id,
            object.object_id
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_secondary_indexes_for_legacy_object_records() {
        let root = temp_root("local_backend_recovers_secondary_indexes_for_legacy_object_records");
        {
            let mut backend =
                LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let target = object(version("version_1"));
            let mut alias = object(version("version_1"));
            alias.object_id = DfsObjectId::from_legacy_named("obj_legacy_alias").expect("object");
            alias.path = "/docs/readme.alias".to_string();
            alias.kind = DfsObjectKind::Hardlink;
            alias.content_hash = None;
            alias.link_target_object_id = Some(target.object_id.clone());
            for object in [&target, &alias] {
                backend.put_object(object).expect("object");
            }
        }

        for index_dir in ["filesystem-objects", "paths", "dentries", "hardlinks"] {
            fs::remove_dir_all(root.join("records").join(index_dir)).ok();
        }

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .hardlinks_to_object(&file_system_id(), &object_id())
                .expect("hardlinks")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_hardlinks_from_target_index() {
        let root = temp_root("local_backend_lists_hardlinks_from_target_index");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target = object(version("version_1"));
        let mut alias_a = object(version("version_1"));
        alias_a.object_id = DfsObjectId::from_legacy_named("obj_alias_a").expect("object");
        alias_a.path = "/docs/readme.alias-a".to_string();
        alias_a.kind = DfsObjectKind::Hardlink;
        alias_a.content_hash = None;
        alias_a.link_target_object_id = Some(target.object_id.clone());
        let mut alias_b = alias_a.clone();
        alias_b.object_id = DfsObjectId::from_legacy_named("obj_alias_b").expect("object");
        alias_b.path = "/docs/readme.alias-b".to_string();
        let mut unrelated = object(version("version_1"));
        unrelated.object_id = DfsObjectId::from_legacy_named("obj_unrelated").expect("object");
        unrelated.path = "/docs/unrelated".to_string();

        for object in [&target, &alias_b, &unrelated, &alias_a] {
            backend.put_object(object).expect("object");
        }
        let mut stale_alias = alias_b.clone();
        stale_alias.kind = DfsObjectKind::Tombstone;
        stale_alias.link_target_object_id = None;
        backend
            .put_object(&stale_alias)
            .expect("tombstone stale alias");

        let hardlinks = backend
            .hardlinks_to_object(&file_system_id(), &target.object_id)
            .expect("hardlinks");
        assert_eq!(
            hardlinks
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias-a"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_hardlink_target_index_entries() {
        let root = temp_root("local_backend_removes_stale_hardlink_target_index_entries");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target_a = object(version("version_1"));
        let mut target_b = object(version("version_1"));
        target_b.object_id = DfsObjectId::from_legacy_named("obj_target_b").expect("object");
        target_b.path = "/docs/target-b.md".to_string();
        let mut alias = object(version("version_1"));
        alias.object_id = DfsObjectId::from_legacy_named("obj_alias").expect("object");
        alias.path = "/docs/readme.alias".to_string();
        alias.kind = DfsObjectKind::Hardlink;
        alias.content_hash = None;
        alias.link_target_object_id = Some(target_a.object_id.clone());

        for object in [&target_a, &target_b, &alias] {
            backend.put_object(object).expect("object");
        }
        assert!(table_hardlink_exists(
            &backend,
            &target_a.object_id,
            &alias.object_id
        ));

        alias.link_target_object_id = Some(target_b.object_id.clone());
        backend.put_object(&alias).expect("retarget alias");
        assert!(
            !table_hardlink_exists(&backend, &target_a.object_id, &alias.object_id),
            "retargeting a hardlink should remove its old target index entry"
        );
        assert!(table_hardlink_exists(
            &backend,
            &target_b.object_id,
            &alias.object_id
        ));

        alias.kind = DfsObjectKind::Tombstone;
        alias.link_target_object_id = None;
        backend.put_object(&alias).expect("tombstone alias");
        assert!(
            !table_hardlink_exists(&backend, &target_b.object_id, &alias.object_id),
            "tombstoning a hardlink should remove its target index entry"
        );
        assert!(
            backend
                .hardlinks_to_object(&file_system_id(), &target_b.object_id)
                .expect("hardlinks")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_intents_and_polls_subscription_batches() {
        let root = temp_root("local_backend_persists_intents_and_polls_subscription_batches");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_1".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            base_version: Some(version("version_1")),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:new".to_string()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let entry = backend.put_intent(&intent).expect("intent");
        assert_eq!(entry.sequence, 1);
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::ReceiptsPending,
                intent: Some(ref journal_intent),
                ..
            } if journal_intent == &intent
        ));
        assert_eq!(
            backend
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("get intent"),
            Some(intent)
        );

        let mut subscription = backend.subscribe(0);
        let first_batch = backend
            .poll_subscription(&mut subscription)
            .expect("first batch");
        assert_eq!(first_batch.entries.len(), 1);
        assert_eq!(first_batch.next_after_sequence, 1);
        assert_eq!(subscription.next_after_sequence, 1);

        let second_batch = backend
            .poll_subscription(&mut subscription)
            .expect("second batch");
        assert!(second_batch.entries.is_empty());
        assert_eq!(second_batch.next_after_sequence, 1);

        fs::write(
            root.join("journal").join("00000000000000000001.sobj"),
            b"not json",
        )
        .expect("corrupt already-consumed journal entry");
        let third_batch = backend
            .poll_subscription(&mut subscription)
            .expect("already-consumed corrupted journal entry is not reparsed");
        assert!(third_batch.entries.is_empty());
        assert_eq!(third_batch.next_after_sequence, 1);

        cleanup(&root);
    }

    #[test]
    fn commit_visible_version_requires_expected_version_and_receipts() {
        let root = temp_root("commit_visible_version_requires_expected_version_and_receipts");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert!(matches!(
            backend.commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            ),
            Err(MetadataBackendError::Conflict { .. })
        ));

        backend.put_receipt(&receipt).expect("receipt");
        let commit = backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");
        assert!(matches!(
            commit.mutation,
            DfsMetadataMutation::CommitVisibleVersion { .. }
        ));
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );
        assert_eq!(
            backend
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipts")
                .len(),
            1
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_batch_into_follower_backend() {
        let leader_root = temp_root("local_backend_replays_journal_batch_into_follower_leader");
        let follower_root = temp_root("local_backend_replays_journal_batch_into_follower_follower");
        let mut leader =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-leader"), &leader_root)
                .expect("leader backend");
        let mut follower = LocalJsonMetadataBackend::new(
            DfsMetadataShardId::new("shard-follower"),
            &follower_root,
        )
        .expect("follower backend");

        let file_system = file_system();
        let initial = object(version("version_1"));
        leader.put_file_system(&file_system).expect("filesystem");
        leader.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let visible_version = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_replay_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        leader.put_receipt(&receipt).expect("receipt");
        leader
            .commit_visible_version(
                &initial.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        let mut subscription = leader.subscribe(0);
        let mut batch = leader
            .poll_subscription(&mut subscription)
            .expect("journal batch");
        assert_eq!(batch.entries.len(), 4);
        let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 64;
        for (offset, entry) in batch.entries.iter_mut().enumerate() {
            entry.sequence = source_sequence_base + offset as u64;
        }
        batch.next_after_sequence = batch
            .entries
            .last()
            .expect("non-empty replay batch")
            .sequence;

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("apply journal batch"),
            4
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal"),
            batch.entries,
            "replicated metadata replay should persist source journal entries instead of appending follower-local replacements"
        );
        assert_eq!(
            follower
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem"),
            Some(file_system)
        );
        assert_eq!(
            follower
                .get_object(&committed.object_id)
                .expect("object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            follower
                .get_version(&committed.object_id, &committed.version)
                .expect("version"),
            Some(normalized_version_record(&visible_version))
        );
        assert_eq!(
            follower
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipt"),
            vec![receipt]
        );

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("reapply journal batch"),
            0
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal reapply"),
            batch.entries,
            "idempotent replays must not append duplicate follower journal entries"
        );
        assert_eq!(
            read_json::<DfsMetadataReplayCursor>(
                &follower.replay_cursor_path(&DfsMetadataShardId::new("shard-leader"))
            )
            .expect("replay cursor")
            .last_sequence,
            batch.next_after_sequence,
            "a replicated batch should persist the source cursor once at the batch boundary"
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata shard status")
            .pop()
            .expect("follower shard status");
        assert_eq!(
            status.journal_last_sequence, batch.next_after_sequence,
            "a replayed source batch must advance the local journal head to the source sequence"
        );
        assert_eq!(
            status.journal_next_sequence,
            batch.next_after_sequence.saturating_add(1)
        );
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: DfsMetadataShardId::new("shard-leader"),
                replayed_through_sequence: batch.next_after_sequence,
            }]
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_persists_metadata_replication_cursor_monotonically() {
        let root = temp_root("local_backend_metadata_replication_cursor");
        let target_node_id = node("node_cursor_target");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        assert_eq!(
            backend
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("initial cursor"),
            0
        );

        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 42)
            .expect("write cursor");
        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 7)
            .expect("stale cursor");
        let reopened =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");

        assert_eq!(
            reopened
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("reopened cursor"),
            42,
            "older acknowledgements must not move a durable replication cursor backwards"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_semantically_duplicate_receipt_as_idempotent() {
        let root = temp_root("local_backend_duplicate_receipt_semantic_idempotent");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_receipt_a").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        let mut replayed = receipt.clone();
        replayed.receipt.event_id = EventId::from_legacy_named("evt_receipt_b").expect("event");
        replayed.received_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        let batch = DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("shard-root"),
            entries: vec![DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutReceipt {
                    file_system_id: file_system.file_system_id,
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    replica_node_id: receipt.replica_node_id.clone(),
                    receipt: Some(replayed),
                },
            }],
            next_after_sequence: 2,
        };

        assert_eq!(
            backend
                .apply_journal_batch(&batch)
                .expect("duplicate receipt replay"),
            0
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("receipts"),
            vec![receipt]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_object_locations_from_visible_version() {
        let root = temp_root("local_backend_recovers_object_locations_from_visible_version");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut object = object(version("version_2"));
        object.current_locations = vec![owner.clone()];
        object.content_hash = Some("blake3:recover-locations".to_string());
        backend.put_object(&object).expect("object");
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(18),
            locations: vec![owner.clone(), replica.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object")
                .expect("object")
                .current_locations,
            vec![owner, replica]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_rejects_replay_entries_without_record_payloads() {
        let root = temp_root("local_backend_rejects_replay_entries_without_record_payloads");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-leader"),
            sequence: 7,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: None,
            },
        };

        let error = backend
            .apply_journal_entry(&entry)
            .expect_err("missing payload should fail replay");
        assert!(
            error
                .to_string()
                .contains("cannot replay put-file-system without its record payload")
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_normalizes_replica_locations_at_persistence_boundary() {
        let root = temp_root("local_backend_normalizes_replica_locations");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut object = object(version("version_1"));
        object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        backend.put_object(&object).expect("object");
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .expect("object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );

        let mut version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(3),
            locations: vec![owner.clone(), replica.clone(), owner.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version")
                .expect("version")
                .locations,
            vec![owner.clone(), replica.clone()]
        );

        let version_2 = version("version_2");
        let mut committed = object.clone();
        committed.version = version_2.clone();
        committed.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        version_record.version = version_2;
        version_record.locations = committed.current_locations.clone();
        for node_id in [&owner, &replica] {
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: committed.file_system_id.clone(),
                        object_id: committed.object_id.clone(),
                        committed_version: committed.version.clone(),
                        owner_node_id: owner.clone(),
                        event_id: EventId::from_legacy_named(&format!("evt_{node_id}"))
                            .expect("event"),
                    },
                    replica_node_id: node_id.clone(),
                    content_hash: committed.content_hash.clone(),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("receipt");
        }
        backend
            .commit_visible_version(
                &object.version,
                &committed,
                &version_record,
                &[owner.clone(), replica.clone(), owner.clone()],
            )
            .expect("visible commit");
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get committed object")
                .expect("committed object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get committed version")
                .expect("committed version")
                .locations,
            vec![owner, replica]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_visible_version_commit() {
        let root = temp_root("local_backend_recovers_interrupted_visible_version_commit");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            recovered
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects() {
        let root = temp_root(
            "local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects",
        );
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut version_2 = initial.clone();
        version_2.version = version("version_2");
        version_2.content_hash = Some("blake3:two".to_string());
        let version_2_record = DfsObjectVersionRecord {
            file_system_id: version_2.file_system_id.clone(),
            object_id: version_2.object_id.clone(),
            version: version_2.version.clone(),
            content_hash: version_2.content_hash.clone(),
            size_bytes: Some(3),
            locations: version_2.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let version_2_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_2.file_system_id.clone(),
                object_id: version_2.object_id.clone(),
                committed_version: version_2.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_two").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_2.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_2_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &version_2,
                &version_2_record,
                std::slice::from_ref(&version_2_receipt.replica_node_id),
            )
            .expect("commit version 2");

        let mut version_3 = version_2.clone();
        version_3.version = version("version_3");
        version_3.content_hash = Some("blake3:three".to_string());
        let version_3_record = DfsObjectVersionRecord {
            file_system_id: version_3.file_system_id.clone(),
            object_id: version_3.object_id.clone(),
            version: version_3.version.clone(),
            content_hash: version_3.content_hash.clone(),
            size_bytes: Some(5),
            locations: version_3.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            visible: true,
        };
        let version_3_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_3.file_system_id.clone(),
                object_id: version_3.object_id.clone(),
                committed_version: version_3.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_three").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_3.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_3_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &version_2.version,
                &version_3,
                &version_3_record,
                std::slice::from_ref(&version_3_receipt.replica_node_id),
            )
            .expect("commit version 3");

        backend.put_object(&version_2).expect("stale object");
        drop(backend);
        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&version_3.object_id)
                .expect("get recovered")
                .expect("object")
                .version,
            version_3.version
        );
        drop(recovered);

        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let mut tombstone = version_3.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = version("version_4");
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        backend.put_object(&tombstone).expect("tombstone");
        drop(backend);

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&tombstone.object_id)
                .expect("get tombstone")
                .expect("object")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_namespace_transaction() {
        let root = temp_root("local_backend_recovers_interrupted_namespace_transaction");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source.clone(), destination.clone()],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/current.html")
                .expect("lookup current")
                .expect("current object")
                .object_id,
            source.object_id
        );
        assert!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/index.html")
                .expect("lookup old")
                .is_none()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_does_not_rejournal_healthy_namespace_transactions() {
        let root = temp_root("local_backend_reopen_does_not_rejournal_namespace_transactions");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source, destination],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");

        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "opening a healthy backend must not append duplicate namespace transaction journal entries"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_lists_and_updates_conflicts() {
        let root = temp_root("local_backend_persists_lists_and_updates_conflicts");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_a".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            path: "docs/readme.md".to_string(),
            base_version: Some(version("version_1")),
            local_version: version("version_2_local"),
            remote_version: version("version_2_remote"),
            detected_by_node_id: node("node_laptop"),
            reason: "stale reconnect write".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };

        let entry = backend.put_conflict(&conflict).expect("put conflict");
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutConflict {
                conflict: Some(ref journal_conflict),
                ..
            } if journal_conflict == &conflict
        ));
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get conflict"),
            Some(conflict.clone())
        );
        assert_eq!(
            backend
                .conflicts_for_object(&conflict.object_id)
                .expect("list conflicts"),
            vec![conflict.clone()]
        );

        let resolved = backend
            .update_conflict_status(
                &conflict.object_id,
                &conflict.conflict_id,
                DfsConflictStatus::Resolved,
            )
            .expect("resolve conflict")
            .expect("conflict");
        assert_eq!(resolved.status, DfsConflictStatus::Resolved);
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get resolved")
                .expect("conflict")
                .status,
            DfsConflictStatus::Resolved
        );

        cleanup(&root);
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-dfs-metadata-{name}-{nanos}"))
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
