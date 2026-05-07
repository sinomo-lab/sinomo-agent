//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: records live in a shard-local
//! ordered layout on disk, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata, DfsReplicationPolicy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::{DfsFileSystemId, DfsObjectId, DfsVersion, DfsWriteReceipt, EventId, NodeId};
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
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
        Self {
            inode_id: object_inode_id(&object.object_id),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            kind: object.kind.clone(),
            owner_node_id: object.owner_node_id.clone(),
            policy: object.policy.clone(),
            visible_version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            current_locations: object.current_locations.clone(),
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

pub trait DfsMetadataBackend: std::fmt::Debug {
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
        Ok(Self {
            shard_id,
            root,
            next_sequence,
        })
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    fn write_journal(&mut self, mutation: DfsMetadataMutation) -> Result<DfsMetadataJournalEntry> {
        let entry = DfsMetadataJournalEntry {
            shard_id: self.shard_id.clone(),
            sequence: self.next_sequence,
            created_at: OffsetDateTime::now_utc(),
            mutation,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);
        write_json_atomic(&self.journal_path(entry.sequence), &entry)?;
        Ok(entry)
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

    fn object_dir(&self) -> PathBuf {
        self.records_root().join("objects")
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
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let path_record = path_record_for_object(object);
        let inode = DfsInodeRecord::from(object);
        let dentry_path = self.dentry_path(&path_record);
        let dirfrag = self.read_or_create_dirfrag(
            &path_record.file_system_id,
            &path_record.parent_inode_id,
            !dentry_path.exists(),
        )?;

        write_json_atomic(&self.object_path(&object.object_id), object)?;
        write_json_atomic(&self.inode_path(&inode.inode_id), &inode)?;
        write_json_atomic(
            &self.path_index_path(&object.file_system_id, &object.path),
            &path_record,
        )?;
        write_json_atomic(&dentry_path, &path_record)?;
        write_json_atomic(
            &self.dirfrag_path(&dirfrag.dir_inode_id, &dirfrag.dirfrag_id),
            &dirfrag,
        )?;

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
        if normalize_path(&path_record.path) != normalize_path(path) {
            return Ok(None);
        }
        let Some(object) = self.get_object(&path_record.object_id)? else {
            return Ok(None);
        };
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
        if !self.object_dir().exists() {
            return Ok(objects);
        }
        for entry in read_dir(&self.object_dir())? {
            let path = entry.path().join("object.sobj");
            if !path.is_file() {
                continue;
            }
            let object: DfsObjectMetadata = read_json(&path)?;
            if &object.file_system_id == file_system_id {
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

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        write_json_atomic(
            &self.version_path(&version.object_id, &version.version),
            version,
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

        write_json_atomic(
            &self.object_path(&committed_object.object_id),
            committed_object,
        )?;
        write_json_atomic(
            &self.version_path(&visible_version.object_id, &visible_version.version),
            visible_version,
        )?;
        write_json_atomic(
            &self.inode_path(&object_inode_id(&committed_object.object_id)),
            &DfsInodeRecord::from(committed_object),
        )?;

        self.write_journal(DfsMetadataMutation::CommitVisibleVersion {
            file_system_id: committed_object.file_system_id.clone(),
            object_id: committed_object.object_id.clone(),
            previous_version: expected_version.clone(),
            committed_version: committed_object.version.clone(),
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
        let mut entries = BTreeMap::new();
        let journal_dir = self.root.join("journal");
        if !journal_dir.exists() {
            return Ok(Vec::new());
        }
        for entry in read_dir(&journal_dir)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                continue;
            }
            let journal_entry: DfsMetadataJournalEntry = read_json(&path)?;
            if journal_entry.sequence > after_sequence {
                entries.insert(journal_entry.sequence, journal_entry);
            }
        }
        Ok(entries.into_values().collect())
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

fn normalize_path(path: &str) -> String {
    path.trim_matches('/').replace('\\', "/")
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

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, bytes).map_err(|source| MetadataBackendError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })
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
