//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: records live in a shard-local
//! ordered layout on disk, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::DfsWriteReceipt;
use sinomo_events::EventId;
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use time::OffsetDateTime;

pub const DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT: u64 = 4096;

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
    },
    PutObject {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        path: String,
    },
    PutVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
    },
    PutIntent {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        status: DfsWriteIntentStatus,
    },
    CommitVisibleVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_version: DfsVersion,
        committed_version: DfsVersion,
    },
    PutConflict {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        conflict_id: String,
    },
    ApplyNamespaceTransaction {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
        object_count: usize,
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
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>>;
    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry>;
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
}

impl LocalJsonMetadataBackend {
    pub fn new(shard_id: DfsMetadataShardId, root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal"))?;
        let next_sequence = next_journal_sequence(&root)?;
        let mut backend = Self {
            shard_id,
            root,
            next_sequence,
        };
        backend.recover_secondary_indexes()?;
        backend.recover_namespace_transactions()?;
        backend.recover_visible_version_commits()?;
        Ok(backend)
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    fn write_journal(&mut self, mutation: DfsMetadataMutation) -> Result<DfsMetadataJournalEntry> {
        let mut sequence = self.next_sequence.max(next_journal_sequence(&self.root)?);
        loop {
            let entry = DfsMetadataJournalEntry {
                shard_id: self.shard_id.clone(),
                sequence,
                created_at: OffsetDateTime::now_utc(),
                mutation: mutation.clone(),
            };
            match write_json_create_new_atomic(&self.journal_path(entry.sequence), &entry) {
                Ok(()) => {
                    self.next_sequence = sequence.saturating_add(1);
                    return Ok(entry);
                }
                Err(MetadataBackendError::Io { source, .. })
                    if source.kind() == io::ErrorKind::AlreadyExists =>
                {
                    sequence = sequence.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn records_root(&self) -> PathBuf {
        self.root.join("records")
    }

    fn file_system_path(&self, file_system_id: &DfsFileSystemId) -> PathBuf {
        self.records_root()
            .join("filesystems")
            .join(format!("{}.sobj", key_segment(file_system_id)))
    }

    fn file_system_dir(&self) -> PathBuf {
        self.records_root().join("filesystems")
    }

    fn object_path(&self, object_id: &DfsObjectId) -> PathBuf {
        self.records_root()
            .join("objects")
            .join(key_segment(object_id))
            .join("object.sobj")
    }

    fn object_records_dir(&self) -> PathBuf {
        self.records_root().join("objects")
    }

    fn file_system_object_index_path(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> PathBuf {
        self.records_root()
            .join("filesystem-objects")
            .join(key_segment(file_system_id))
            .join(format!("{}.sobj", key_segment(object_id)))
    }

    fn file_system_object_index_dir(&self, file_system_id: &DfsFileSystemId) -> PathBuf {
        self.records_root()
            .join("filesystem-objects")
            .join(key_segment(file_system_id))
    }

    fn path_index_path(&self, file_system_id: &DfsFileSystemId, path: &str) -> PathBuf {
        self.records_root()
            .join("paths")
            .join(key_segment(file_system_id))
            .join(format!("{}.sobj", path_hash(path)))
    }

    fn inode_path(&self, inode_id: &DfsInodeId) -> PathBuf {
        self.records_root()
            .join("inodes")
            .join(format!("{}.sobj", key_segment(&inode_id.0)))
    }

    fn dirfrag_path(&self, dir_inode_id: &DfsInodeId, dirfrag_id: &DfsDirFragmentId) -> PathBuf {
        self.records_root()
            .join("dirfrags")
            .join(key_segment(&dir_inode_id.0))
            .join(format!("{}.sobj", key_segment(&dirfrag_id.0)))
    }

    fn dentry_path(&self, path_record: &DfsPathRecord) -> PathBuf {
        self.records_root()
            .join("dentries")
            .join(key_segment(&path_record.parent_inode_id.0))
            .join(key_segment(&path_record.dirfrag_id.0))
            .join(format!(
                "{:016x}-{}.sobj",
                path_record.name_hash,
                path_hash(&path_record.name)
            ))
    }

    fn hardlink_index_path(
        &self,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) -> PathBuf {
        self.records_root()
            .join("hardlinks")
            .join(key_segment(target_object_id))
            .join(format!("{}.sobj", key_segment(hardlink_object_id)))
    }

    fn hardlink_index_dir(&self, target_object_id: &DfsObjectId) -> PathBuf {
        self.records_root()
            .join("hardlinks")
            .join(key_segment(target_object_id))
    }

    fn version_path(&self, object_id: &DfsObjectId, version: &DfsVersion) -> PathBuf {
        self.records_root()
            .join("versions")
            .join(key_segment(object_id))
            .join(format!("{}.sobj", key_segment(version)))
    }

    fn version_dir(&self, object_id: &DfsObjectId) -> PathBuf {
        self.records_root()
            .join("versions")
            .join(key_segment(object_id))
    }

    fn receipt_path(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> PathBuf {
        self.records_root()
            .join("receipts")
            .join(key_segment(object_id))
            .join(key_segment(version))
            .join(format!("{}.sobj", key_segment(replica_node_id)))
    }

    fn receipt_dir(&self, object_id: &DfsObjectId, version: &DfsVersion) -> PathBuf {
        self.records_root()
            .join("receipts")
            .join(key_segment(object_id))
            .join(key_segment(version))
    }

    fn intent_path(&self, object_id: &DfsObjectId, intent_id: &str) -> PathBuf {
        self.records_root()
            .join("intents")
            .join(key_segment(object_id))
            .join(format!("{}.sobj", key_segment(&intent_id)))
    }

    fn intent_dir(&self, object_id: &DfsObjectId) -> PathBuf {
        self.records_root()
            .join("intents")
            .join(key_segment(object_id))
    }

    fn conflict_dir(&self, object_id: &DfsObjectId) -> PathBuf {
        self.records_root()
            .join("conflicts")
            .join(key_segment(object_id))
    }

    fn conflict_path(&self, conflict: &DfsConflictRecord) -> PathBuf {
        self.conflict_path_by_id(&conflict.object_id, &conflict.conflict_id)
    }

    fn conflict_path_by_id(&self, object_id: &DfsObjectId, conflict_id: &str) -> PathBuf {
        self.conflict_dir(object_id)
            .join(format!("{}.sobj", key_segment(&conflict_id)))
    }

    fn visible_commit_path(&self, object_id: &DfsObjectId, version: &DfsVersion) -> PathBuf {
        self.records_root()
            .join("visible-commits")
            .join(key_segment(object_id))
            .join(format!("{}.sobj", key_segment(version)))
    }

    fn visible_commit_root(&self) -> PathBuf {
        self.records_root().join("visible-commits")
    }

    fn namespace_transaction_path(&self, transaction_id: &str) -> PathBuf {
        self.records_root()
            .join("namespace-transactions")
            .join(format!("{}.sobj", key_segment(&transaction_id)))
    }

    fn namespace_transaction_root(&self) -> PathBuf {
        self.records_root().join("namespace-transactions")
    }

    fn journal_path(&self, sequence: u64) -> PathBuf {
        self.root
            .join("journal")
            .join(format!("{sequence:020}.sobj"))
    }
}

impl DfsMetadataBackend for LocalJsonMetadataBackend {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        write_json_atomic(
            &self.file_system_path(&file_system.file_system_id),
            file_system,
        )?;
        self.write_journal(DfsMetadataMutation::PutFileSystem {
            file_system_id: file_system.file_system_id.clone(),
        })
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        read_json_optional(&self.file_system_path(file_system_id))
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        read_json_dir(&self.file_system_dir())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let object = normalized_object_metadata(object);
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let previous = self.get_object(&object.object_id)?;
        self.materialize_object_indexes_with_previous(&object, previous.as_ref())?;

        self.write_journal(DfsMetadataMutation::PutObject {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
        })
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        read_json_optional(&self.object_path(object_id))
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let Some(path_record) =
            read_json_optional::<DfsPathRecord>(&self.path_index_path(file_system_id, path))?
        else {
            return Ok(None);
        };
        if path_record.file_system_id != *file_system_id {
            return Ok(None);
        }
        if normalize_path(&path_record.path) != normalize_path(path) {
            return Ok(None);
        }
        let Some(object) = self.get_object(&path_record.object_id)? else {
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
        let mut objects = Vec::new();
        for path_record in
            read_json_dir::<DfsPathRecord>(&self.file_system_object_index_dir(file_system_id))?
        {
            if path_record.file_system_id != *file_system_id {
                continue;
            }
            let Some(object) = self.get_object(&path_record.object_id)? else {
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
        let dentry_dir = self
            .records_root()
            .join("dentries")
            .join(key_segment(&parent_inode_id.0))
            .join(key_segment(&"root"));
        let mut objects = Vec::new();
        for path_record in read_json_dir::<DfsPathRecord>(&dentry_dir)? {
            if path_record.file_system_id != *file_system_id
                || path_record.parent_inode_id != parent_inode_id
            {
                continue;
            }
            let Some(object) = self.get_object(&path_record.object_id)? else {
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
        let mut hardlinks = Vec::new();
        for path_record in
            read_json_dir::<DfsPathRecord>(&self.hardlink_index_dir(target_object_id))?
        {
            if path_record.file_system_id != *file_system_id {
                continue;
            }
            let Some(object) = self.get_object(&path_record.object_id)? else {
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
        let version = normalized_version_record(version);
        write_json_atomic(
            &self.version_path(&version.object_id, &version.version),
            &version,
        )?;
        self.write_journal(DfsMetadataMutation::PutVersion {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
        })
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        read_json_optional(&self.version_path(object_id, version))
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        let mut versions: Vec<DfsObjectVersionRecord> =
            read_json_dir(&self.version_dir(object_id))?;
        versions.sort_by(|left, right| left.version.cmp(&right.version));
        Ok(versions)
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        write_json_atomic(
            &self.receipt_path(
                &receipt.receipt.object_id,
                &receipt.receipt.committed_version,
                &receipt.replica_node_id,
            ),
            receipt,
        )?;
        self.write_journal(DfsMetadataMutation::PutReceipt {
            file_system_id: receipt.receipt.file_system_id.clone(),
            object_id: receipt.receipt.object_id.clone(),
            version: receipt.receipt.committed_version.clone(),
            replica_node_id: receipt.replica_node_id.clone(),
        })
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        read_json_dir(&self.receipt_dir(object_id, version))
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        write_json_atomic(
            &self.intent_path(&intent.object_id, &intent.intent_id),
            intent,
        )?;
        self.write_journal(DfsMetadataMutation::PutIntent {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            intent_id: intent.intent_id.clone(),
            status: intent.status.clone(),
        })
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        read_json_optional(&self.intent_path(object_id, intent_id))
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents: Vec<DfsWriteIntentRecord> = read_json_dir(&self.intent_dir(object_id))?;
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
            let receipt = self.receipt_path(
                &committed_object.object_id,
                &committed_object.version,
                node_id,
            );
            if !receipt.is_file() {
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
        write_json_atomic(
            &self.visible_commit_path(&committed_object.object_id, &committed_object.version),
            &commit_record,
        )?;
        self.materialize_visible_version_commit(&commit_record)?;

        self.write_journal(DfsMetadataMutation::CommitVisibleVersion {
            file_system_id: committed_object.file_system_id.clone(),
            object_id: committed_object.object_id.clone(),
            previous_version: expected_version.clone(),
            committed_version: committed_object.version.clone(),
        })
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
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
        write_json_atomic(
            &self.namespace_transaction_path(&transaction.transaction_id),
            &transaction,
        )?;
        self.materialize_namespace_transaction(&transaction)?;
        self.write_journal(DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: transaction.file_system_id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            object_count: transaction.objects.len(),
        })
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        write_json_atomic(&self.conflict_path(conflict), conflict)?;
        self.write_journal(DfsMetadataMutation::PutConflict {
            file_system_id: conflict.file_system_id.clone(),
            object_id: conflict.object_id.clone(),
            conflict_id: conflict.conflict_id.clone(),
        })
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        read_json_optional(&self.conflict_path_by_id(object_id, conflict_id))
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        read_json_dir(&self.conflict_dir(object_id))
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
        let journal_dir = self.root.join("journal");
        if !journal_dir.exists() {
            return Ok(Vec::new());
        }
        let mut paths = BTreeMap::new();
        for entry in read_dir(&journal_dir)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
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
            entries.push(read_json(&path)?);
        }
        Ok(entries)
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
        let root = self.namespace_transaction_root();
        if !root.exists() {
            return Ok(());
        }
        for entry in read_dir(&root)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                continue;
            }
            let transaction: DfsNamespaceTransaction = read_json(&path)?;
            self.materialize_namespace_transaction_for_recovery(&transaction)?;
            self.write_journal(DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
            })?;
        }
        Ok(())
    }

    fn materialize_namespace_transaction(
        &self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<()> {
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes(object)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes(object)?;
        }
        Ok(())
    }

    fn materialize_namespace_transaction_for_recovery(
        &self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<()> {
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes_if_current(object)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            self.materialize_object_indexes_if_current(object)?;
        }
        Ok(())
    }

    fn materialize_object_indexes_if_current(&self, object: &DfsObjectMetadata) -> Result<()> {
        if let Some(current) = self.get_object(&object.object_id)? {
            return self.materialize_object_indexes(&current);
        }
        self.materialize_object_indexes(object)
    }

    fn materialize_object_indexes(&self, object: &DfsObjectMetadata) -> Result<()> {
        self.materialize_object_indexes_with_previous(object, None)
    }

    fn recover_secondary_indexes(&self) -> Result<()> {
        let mut objects = self.object_records()?;
        objects.sort_by(|left, right| {
            left.version
                .cmp(&right.version)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        for index_dir in [
            "filesystem-objects",
            "paths",
            "dentries",
            "dirfrags",
            "hardlinks",
        ] {
            remove_dir_all_if_exists(&self.records_root().join(index_dir))?;
        }
        for object in objects {
            self.materialize_object_indexes_with_previous(&object, None)?;
        }
        Ok(())
    }

    fn object_records(&self) -> Result<Vec<DfsObjectMetadata>> {
        let root = self.object_records_dir();
        let mut objects = Vec::new();
        if !root.exists() {
            return Ok(objects);
        }
        for object_dir in read_dir(&root)? {
            let object_dir_path = object_dir.path();
            if !object_dir_path.is_dir() {
                continue;
            }
            let path = object_dir_path.join("object.sobj");
            if path.is_file() {
                objects.push(read_json(&path)?);
            }
        }
        Ok(objects)
    }

    fn materialize_object_indexes_with_previous(
        &self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
    ) -> Result<()> {
        let object = normalized_object_metadata(object);
        let inode = DfsInodeRecord::from(&object);
        let path_record = path_record_for_object(&object);

        if let Some(previous_target_object_id) =
            previous.and_then(|previous| previous.link_target_object_id.as_ref())
            && (object.kind != DfsObjectKind::Hardlink
                || object.link_target_object_id.as_ref() != Some(previous_target_object_id))
        {
            remove_file_if_exists(
                &self.hardlink_index_path(previous_target_object_id, &object.object_id),
            )?;
        }
        if let Some(previous) = previous {
            let previous_path_record = path_record_for_object(previous);
            if previous.file_system_id != object.file_system_id {
                remove_file_if_exists(
                    &self
                        .file_system_object_index_path(&previous.file_system_id, &object.object_id),
                )?;
            }
            if previous_path_record.file_system_id != path_record.file_system_id
                || previous_path_record.path != path_record.path
            {
                remove_file_if_exists(&self.path_index_path(
                    &previous_path_record.file_system_id,
                    &previous_path_record.path,
                ))?;
            }
            if previous.kind != DfsObjectKind::Tombstone
                && (object.kind == DfsObjectKind::Tombstone
                    || previous_path_record.path != path_record.path
                    || previous_path_record.parent_inode_id != path_record.parent_inode_id
                    || previous_path_record.name != path_record.name)
            {
                self.remove_dentry_index(&previous_path_record)?;
            }
        }
        write_json_atomic(
            &self.file_system_object_index_path(&object.file_system_id, &object.object_id),
            &path_record,
        )?;
        write_json_atomic(&self.object_path(&object.object_id), &object)?;
        write_json_atomic(&self.inode_path(&inode.inode_id), &inode)?;
        if object.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = object.link_target_object_id.as_ref()
        {
            write_json_atomic(
                &self.hardlink_index_path(target_object_id, &object.object_id),
                &path_record_for_object(&object),
            )?;
        }
        if object.kind == DfsObjectKind::Tombstone {
            self.remove_dentry_index(&path_record)?;
            write_json_atomic(
                &self.path_index_path(&object.file_system_id, &path_record.path),
                &path_record,
            )?;
            return Ok(());
        }

        let dentry_path = self.dentry_path(&path_record);
        let dirfrag = self.read_or_create_dirfrag(
            &path_record.file_system_id,
            &path_record.parent_inode_id,
            !dentry_path.exists(),
        )?;
        write_json_atomic(
            &self.path_index_path(&object.file_system_id, &path_record.path),
            &path_record,
        )?;
        write_json_atomic(&dentry_path, &path_record)?;
        write_json_atomic(
            &self.dirfrag_path(&dirfrag.dir_inode_id, &dirfrag.dirfrag_id),
            &dirfrag,
        )
    }

    fn remove_dentry_index(&self, path_record: &DfsPathRecord) -> Result<()> {
        let dentry_path = self.dentry_path(path_record);
        if !dentry_path.exists() {
            return Ok(());
        }
        remove_file_if_exists(&dentry_path)?;
        let dirfrag_path = self.dirfrag_path(&path_record.parent_inode_id, &path_record.dirfrag_id);
        let Some(mut dirfrag) = read_json_optional::<DfsDirFragmentRecord>(&dirfrag_path)? else {
            return Ok(());
        };
        dirfrag.entry_count = dirfrag.entry_count.saturating_sub(1);
        write_json_atomic(&dirfrag_path, &dirfrag)
    }

    fn recover_visible_version_commits(&mut self) -> Result<()> {
        let root = self.visible_commit_root();
        if !root.exists() {
            return Ok(());
        }
        let mut commits = Vec::new();
        for object_dir in read_dir(&root)? {
            let object_dir_path = object_dir.path();
            if !object_dir_path.is_dir() {
                continue;
            }
            for entry in read_dir(&object_dir_path)? {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                    continue;
                }
                let commit: DfsVisibleVersionCommitRecord = read_json(&path)?;
                commits.push(commit);
            }
        }
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
            if !self
                .receipt_path(
                    &commit.committed_object.object_id,
                    &commit.committed_object.version,
                    node_id,
                )
                .is_file()
            {
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

        self.materialize_visible_version_commit(commit)?;
        self.write_journal(DfsMetadataMutation::CommitVisibleVersion {
            file_system_id: commit.committed_object.file_system_id.clone(),
            object_id: commit.committed_object.object_id.clone(),
            previous_version: commit.expected_version.clone(),
            committed_version: commit.committed_object.version.clone(),
        })?;
        Ok(())
    }

    fn materialize_visible_version_commit(
        &self,
        commit: &DfsVisibleVersionCommitRecord,
    ) -> Result<()> {
        self.materialize_object_indexes(&commit.committed_object)?;
        let visible_version = normalized_version_record(&commit.visible_version);
        write_json_atomic(
            &self.version_path(&visible_version.object_id, &visible_version.version),
            &visible_version,
        )
    }

    fn read_or_create_dirfrag(
        &self,
        file_system_id: &DfsFileSystemId,
        dir_inode_id: &DfsInodeId,
        increment_entry_count: bool,
    ) -> Result<DfsDirFragmentRecord> {
        let dirfrag_id = DfsDirFragmentId::new("root");
        let path = self.dirfrag_path(dir_inode_id, &dirfrag_id);
        let mut dirfrag =
            read_json_optional::<DfsDirFragmentRecord>(&path)?.unwrap_or(DfsDirFragmentRecord {
                file_system_id: file_system_id.clone(),
                dir_inode_id: dir_inode_id.clone(),
                dirfrag_id,
                hash_start_inclusive: 0,
                hash_end_exclusive: u64::MAX,
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

fn next_journal_sequence(root: &Path) -> Result<u64> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(1);
    }
    let mut max_sequence = 0_u64;
    for entry in read_dir(&journal_dir)? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(sequence) = stem.parse::<u64>() else {
            continue;
        };
        max_sequence = max_sequence.max(sequence);
    }
    Ok(max_sequence.saturating_add(1).max(1))
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

fn remove_dir_all_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
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

fn read_json_dir<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in read_dir(path)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("sobj") {
            records.push(read_json(&path)?);
        }
    }
    Ok(records)
}

fn write_json_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    write_file_atomic(path, &bytes)
}

fn write_json_create_new_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    write_file_create_new_atomic(path, &bytes)
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

fn write_file_create_new_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
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
    if let Err(source) = fs::hard_link(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        });
    }
    fs::remove_file(&tmp_path).map_err(|source| MetadataBackendError::Io {
        path: tmp_path,
        source,
    })?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })
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
            Some(object)
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);

        let reopened = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);

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
    fn local_backend_removes_stale_path_and_dentry_indexes() {
        let root = temp_root("local_backend_removes_stale_path_and_dentry_indexes");
        let mut backend =
            LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let old_path_index = backend.path_index_path(&file_system_id(), "/docs/readme.md");
        let old_dentry = backend.dentry_path(&path_record_for_object(&readme));
        let dirfrag_path = backend.dirfrag_path(
            &path_record_for_object(&readme).parent_inode_id,
            &DfsDirFragmentId::new("root"),
        );
        assert!(old_path_index.exists());
        assert!(old_dentry.exists());
        assert_eq!(
            read_json::<DfsDirFragmentRecord>(&dirfrag_path)
                .expect("dirfrag after create")
                .entry_count,
            1
        );

        readme.path = "/docs/renamed.md".to_string();
        readme.version = version("version_2");
        backend.put_object(&readme).expect("renamed object");
        assert!(
            !old_path_index.exists(),
            "renaming should remove the obsolete path index"
        );
        assert!(
            !old_dentry.exists(),
            "renaming should remove the obsolete directory entry"
        );
        assert!(
            backend
                .path_index_path(&file_system_id(), "/docs/renamed.md")
                .exists()
        );
        assert!(
            backend
                .dentry_path(&path_record_for_object(&readme))
                .exists()
        );
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
            read_json::<DfsDirFragmentRecord>(&dirfrag_path)
                .expect("dirfrag after rename")
                .entry_count,
            1
        );

        let renamed_dentry = backend.dentry_path(&path_record_for_object(&readme));
        readme.kind = DfsObjectKind::Tombstone;
        readme.content_hash = None;
        readme.version = version("version_3");
        backend.put_object(&readme).expect("tombstone object");
        assert!(
            !renamed_dentry.exists(),
            "tombstoning should remove the visible directory entry"
        );
        assert!(
            backend
                .path_index_path(&file_system_id(), "/docs/renamed.md")
                .exists(),
            "tombstone path record should remain addressable"
        );
        assert!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children after tombstone")
                .is_empty()
        );
        assert_eq!(
            read_json::<DfsDirFragmentRecord>(&dirfrag_path)
                .expect("dirfrag after tombstone")
                .entry_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_rebuilds_dirfrag_counts_during_secondary_index_recovery() {
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
            let dirfrag_path =
                backend.dirfrag_path(&path_record.parent_inode_id, &path_record.dirfrag_id);
            let mut dirfrag =
                read_json::<DfsDirFragmentRecord>(&dirfrag_path).expect("dirfrag before corrupt");
            dirfrag.entry_count = 999;
            write_json_atomic(&dirfrag_path, &dirfrag).expect("corrupt dirfrag count");
        }

        let recovered = LocalJsonMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        let path_record = path_record_for_object(&object(version("version_1")));
        let dirfrag_path =
            recovered.dirfrag_path(&path_record.parent_inode_id, &path_record.dirfrag_id);
        assert_eq!(
            read_json::<DfsDirFragmentRecord>(&dirfrag_path)
                .expect("recovered dirfrag")
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
        assert!(
            backend
                .file_system_object_index_path(&file_system_id(), &readme.object_id)
                .exists()
        );

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

        let old_index =
            backend.file_system_object_index_path(&file_system_id(), &changelog.object_id);
        changelog.file_system_id = other_file_system.file_system_id.clone();
        changelog.path = "/moved-changelog.md".to_string();
        backend.put_object(&changelog).expect("move filesystem");
        assert!(
            !old_index.exists(),
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
        let old_path_index = backend.path_index_path(&file_system.file_system_id, &object.path);
        assert!(old_path_index.exists());

        object.file_system_id = other_file_system.file_system_id.clone();
        backend
            .put_object(&object)
            .expect("move filesystem without rename");

        assert!(
            !old_path_index.exists(),
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
        let old_index_path = backend.hardlink_index_path(&target_a.object_id, &alias.object_id);
        assert!(old_index_path.exists());

        alias.link_target_object_id = Some(target_b.object_id.clone());
        backend.put_object(&alias).expect("retarget alias");
        assert!(
            !old_index_path.exists(),
            "retargeting a hardlink should remove its old target index entry"
        );
        assert!(
            backend
                .hardlink_index_path(&target_b.object_id, &alias.object_id)
                .exists()
        );

        alias.kind = DfsObjectKind::Tombstone;
        alias.link_target_object_id = None;
        backend.put_object(&alias).expect("tombstone alias");
        assert!(
            !backend
                .hardlink_index_path(&target_b.object_id, &alias.object_id)
                .exists(),
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
                ..
            }
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

        fs::remove_file(backend.object_path(&committed.object_id)).expect("remove object");
        fs::remove_file(backend.version_path(&committed.object_id, &committed.version))
            .expect("remove version");
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

        fs::remove_file(backend.object_path(&source.object_id)).expect("remove source object");
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
            DfsMetadataMutation::PutConflict { .. }
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
