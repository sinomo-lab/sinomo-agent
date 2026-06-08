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
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;
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
const LOCAL_MATERIALIZATION_CURSOR_FLUSH_INTERVAL: u64 = 512;
const OBJECT_LOCAL_METADATA_PAGE_SIZE: usize = 4096;
const SHARD_MIGRATION_PAGE_SIZE: usize = 1024;
const LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 1;
const METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 2;
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMigrationReport {
    pub file_system_id: DfsFileSystemId,
    pub scanned_objects: usize,
    pub indexed_objects: usize,
    pub migrated_objects: usize,
    pub skipped_stale_sources: usize,
    pub reached_limit: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<DfsObjectVersionRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectPageCursor {
    pub path: String,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectPage {
    pub objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictPageCursor {
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictPage {
    pub conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectVersionPageCursor {
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectVersionPage {
    pub versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptPageCursor {
    pub replica_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptPage {
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentPageCursor {
    pub intent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentPage {
    pub intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentScanCursor {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub intent_id: String,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentScanPage {
    pub intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectVersionScanCursor {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectVersionScanPage {
    pub versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptScanCursor {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptScanPage {
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictScanCursor {
    pub object_id: DfsObjectId,
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictScanPage {
    pub conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDirectoryPageCursor {
    pub path: String,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dentry: Option<DfsDentryPageCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDentryPageCursor {
    pub dirfrag_id: DfsDirFragmentId,
    pub name_hash: u64,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDirectoryPage {
    pub objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceJobPageCursor {
    pub job_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceJobPage {
    pub jobs: Vec<DfsNamespaceJobRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsNamespaceJobPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsTombstoneCompactionOptions {
    pub dry_run: bool,
    pub min_tombstone_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_compactions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_unix_nanos: Option<u64>,
}

impl DfsTombstoneCompactionOptions {
    pub fn dry_run(min_tombstone_age: Duration) -> Self {
        Self {
            dry_run: true,
            min_tombstone_age_nanos: duration_nanos_saturating(min_tombstone_age),
            max_compactions: None,
            now_unix_nanos: None,
        }
    }

    pub fn compact_eligible(min_tombstone_age: Duration) -> Self {
        Self {
            dry_run: false,
            min_tombstone_age_nanos: duration_nanos_saturating(min_tombstone_age),
            max_compactions: None,
            now_unix_nanos: None,
        }
    }

    pub fn with_max_compactions(mut self, max_compactions: u64) -> Self {
        self.max_compactions = Some(max_compactions);
        self
    }

    pub fn with_now_unix_nanos(mut self, now_unix_nanos: u64) -> Self {
        self.now_unix_nanos = Some(now_unix_nanos);
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsTombstoneCompactionReport {
    pub dry_run: bool,
    pub min_tombstone_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_compactions: Option<u64>,
    pub scanned_tombstone_count: u64,
    pub eligible_tombstone_count: u64,
    pub compacted_tombstone_count: u64,
    pub retained_young_tombstone_count: u64,
    pub retained_referenced_tombstone_count: u64,
    pub retained_limit_tombstone_count: u64,
    pub removed_object_record_count: u64,
    pub removed_inode_record_count: u64,
    pub removed_file_system_object_record_count: u64,
    pub preserved_path_marker_count: u64,
    pub compacted_marker_count: u64,
    pub max_compactions_reached: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceJobRecord {
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub kind: DfsNamespaceJobKind,
    pub status: DfsNamespaceJobStatus,
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_frames: Vec<DfsNamespaceJobFrame>,
    pub processed_object_count: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceJobKind {
    ClearDirectoryRecursive,
    RenameDirectory,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceJobStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsNamespaceJobFrame {
    ScanDirectory {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    CompleteDirectory {
        path: String,
    },
    RenameDirectoryStart {
        source_object_id: DfsObjectId,
    },
    RenameDirectoryScan {
        source_object_id: DfsObjectId,
        source_path: String,
        target_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    RenameDirectoryComplete {
        source_object_id: DfsObjectId,
        source_path: String,
        target_path: String,
    },
}

impl DfsTombstoneCompactionReport {
    fn for_options(options: &DfsTombstoneCompactionOptions) -> Self {
        Self {
            dry_run: options.dry_run,
            min_tombstone_age_nanos: options.min_tombstone_age_nanos,
            max_compactions: options.max_compactions,
            ..Self::default()
        }
    }

    fn merge(&mut self, other: DfsTombstoneCompactionReport) {
        self.scanned_tombstone_count = self
            .scanned_tombstone_count
            .saturating_add(other.scanned_tombstone_count);
        self.eligible_tombstone_count = self
            .eligible_tombstone_count
            .saturating_add(other.eligible_tombstone_count);
        self.compacted_tombstone_count = self
            .compacted_tombstone_count
            .saturating_add(other.compacted_tombstone_count);
        self.retained_young_tombstone_count = self
            .retained_young_tombstone_count
            .saturating_add(other.retained_young_tombstone_count);
        self.retained_referenced_tombstone_count = self
            .retained_referenced_tombstone_count
            .saturating_add(other.retained_referenced_tombstone_count);
        self.retained_limit_tombstone_count = self
            .retained_limit_tombstone_count
            .saturating_add(other.retained_limit_tombstone_count);
        self.removed_object_record_count = self
            .removed_object_record_count
            .saturating_add(other.removed_object_record_count);
        self.removed_inode_record_count = self
            .removed_inode_record_count
            .saturating_add(other.removed_inode_record_count);
        self.removed_file_system_object_record_count = self
            .removed_file_system_object_record_count
            .saturating_add(other.removed_file_system_object_record_count);
        self.preserved_path_marker_count = self
            .preserved_path_marker_count
            .saturating_add(other.preserved_path_marker_count);
        self.compacted_marker_count = self
            .compacted_marker_count
            .saturating_add(other.compacted_marker_count);
        self.max_compactions_reached |= other.max_compactions_reached;
    }
}

fn metadata_summary_from_tables<F>(
    tables: &DfsMetadataTables,
    mut object_is_countable: F,
) -> Result<DfsMetadataSummary>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool>,
{
    metadata_summary_from_tables_scoped(tables, None, &mut object_is_countable)
}

fn metadata_summary_for_file_system_from_tables<F>(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    mut object_is_countable: F,
) -> Result<Option<DfsMetadataSummary>>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool>,
{
    if !tables.file_systems.contains_key(file_system_id) {
        return Ok(None);
    }
    metadata_summary_from_tables_scoped(tables, Some(file_system_id), &mut object_is_countable)
        .map(Some)
}

fn metadata_summary_from_tables_scoped<F>(
    tables: &DfsMetadataTables,
    file_system_scope: Option<&DfsFileSystemId>,
    object_is_countable: &mut F,
) -> Result<DfsMetadataSummary>
where
    F: FnMut(&DfsObjectMetadata) -> Result<bool>,
{
    let file_system_in_scope = |file_system_id: &DfsFileSystemId| {
        file_system_scope.is_none_or(|scope| file_system_id == scope)
    };
    let mut object_count = 0usize;
    for ((file_system_id, _), path_record) in &tables.file_system_objects {
        if !file_system_in_scope(file_system_id)
            || !tables.file_systems.contains_key(file_system_id)
            || path_record.file_system_id != *file_system_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id) else {
            continue;
        };
        if object.file_system_id == *file_system_id
            && normalize_path(&object.path) == path_record.path
            && object_is_countable(object)?
        {
            object_count = object_count.saturating_add(1);
        }
    }

    let mut pending_repair_count = 0usize;
    for intent in tables.intents.values() {
        if intent.status != DfsWriteIntentStatus::Committed {
            continue;
        }
        if !file_system_in_scope(&intent.file_system_id) {
            continue;
        }
        let Some(object) = tables.objects.get(&intent.object_id) else {
            continue;
        };
        let Some(path_record) = tables
            .file_system_objects
            .get(&(object.file_system_id.clone(), object.object_id.clone()))
        else {
            continue;
        };
        if !file_system_in_scope(&object.file_system_id)
            || !tables.file_systems.contains_key(&object.file_system_id)
            || path_record.file_system_id != object.file_system_id
            || path_record.path != normalize_path(&object.path)
            || object.version != intent.target_version
            || !object_is_countable(object)?
        {
            continue;
        }
        let receipts = receipts_for_object_version_from_tables(
            tables,
            &object.object_id,
            &intent.target_version,
        );
        let mut receipts_by_version = BTreeMap::new();
        receipts_by_version.insert(intent.target_version.clone(), receipts);
        pending_repair_count =
            pending_repair_count.saturating_add(count_pending_repairs_for_object(
                object,
                std::slice::from_ref(intent),
                &receipts_by_version,
            ));
    }

    Ok(DfsMetadataSummary {
        file_system_count: match file_system_scope {
            Some(file_system_id) if tables.file_systems.contains_key(file_system_id) => 1,
            Some(_) => 0,
            None => tables.file_systems.len(),
        },
        object_count,
        version_count: tables
            .versions
            .values()
            .filter(|version| file_system_in_scope(&version.file_system_id))
            .count(),
        receipt_count: tables
            .receipts
            .values()
            .filter(|receipt| file_system_in_scope(&receipt.receipt.file_system_id))
            .count(),
        intent_count: tables
            .intents
            .values()
            .filter(|intent| file_system_in_scope(&intent.file_system_id))
            .count(),
        pending_write_count: tables
            .intents
            .values()
            .filter(|intent| {
                file_system_in_scope(&intent.file_system_id)
                    && write_intent_status_is_active(&intent.status)
            })
            .count(),
        open_conflict_count: tables
            .conflicts
            .values()
            .filter(|conflict| {
                file_system_in_scope(&conflict.file_system_id)
                    && conflict.status == DfsConflictStatus::Open
            })
            .count(),
        pending_repair_count,
        pending_namespace_job_count: tables
            .namespace_jobs
            .values()
            .filter(|job| {
                file_system_in_scope(&job.file_system_id)
                    && job.status == DfsNamespaceJobStatus::Running
            })
            .count(),
        failed_namespace_job_count: tables
            .namespace_jobs
            .values()
            .filter(|job| {
                file_system_in_scope(&job.file_system_id)
                    && job.status == DfsNamespaceJobStatus::Failed
            })
            .count(),
    })
}

fn receipts_for_object_version_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Vec<DfsReplicaReceiptRecord> {
    tables
        .receipts
        .range((
            Included((object_id.clone(), version.clone(), id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_object_id, record_version, _), _)| {
            record_object_id == object_id && record_version == version
        })
        .map(|(_, receipt)| receipt.clone())
        .collect()
}

fn count_pending_repairs_for_object(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
) -> usize {
    let mut count = 0usize;
    for intent in intents {
        if intent.status != DfsWriteIntentStatus::Committed
            || intent.target_version != object.version
        {
            continue;
        }
        let receipt_nodes = receipts_by_version
            .get(&intent.target_version)
            .into_iter()
            .flatten()
            .filter(|receipt| {
                intent
                    .content_hash
                    .as_ref()
                    .is_none_or(|hash| receipt.content_hash.as_ref() == Some(hash))
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        for node_id in &intent.desired_replica_node_ids {
            if !receipt_nodes.contains(node_id) {
                count = count.saturating_add(1);
            }
        }
    }
    count
}

fn write_intent_status_is_active(status: &DfsWriteIntentStatus) -> bool {
    matches!(
        status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    )
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
    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::with_capacity(objects.len());
        for object in objects {
            entries.push(self.put_object(object)?);
        }
        Ok(entries)
    }
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            objects.extend(page.objects);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(objects)
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;
    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        const COUNT_PAGE_SIZE: usize = 4096;
        let mut count = 0usize;
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                COUNT_PAGE_SIZE,
            )?;
            count = count.saturating_add(page.objects.len());
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(count)
    }
    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: None,
        })
    }
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
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
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
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }
    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut object_cursor = after.map(|cursor| DfsObjectPageCursor {
            path: cursor.path.clone(),
            object_id: cursor.object_id.clone(),
            shard_id: cursor.shard_id.clone(),
        });
        let mut objects = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        'pages: loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    continue;
                }
                let rest = object_path.trim_start_matches(&prefix);
                if rest.is_empty() || rest.contains('/') {
                    continue;
                }
                if objects.len() >= max_entries {
                    has_more = true;
                    break 'pages;
                }
                let object_cursor = self.object_page_cursor_for_object(&object)?;
                last_cursor = Some(DfsDirectoryPageCursor {
                    path: object_cursor.path,
                    object_id: object_cursor.object_id,
                    dentry: None,
                    shard_id: object_cursor.shard_id,
                });
                objects.push(object);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(DfsDirectoryPage {
            objects,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                if object.kind == DfsObjectKind::Hardlink
                    && object.link_target_object_id.as_ref() == Some(target_object_id)
                {
                    hardlinks.push(object);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        hardlinks.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(hardlinks)
    }
    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let mut object_cursor = after.cloned();
        let mut hardlinks = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        'pages: loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                if object.kind != DfsObjectKind::Hardlink
                    || object.link_target_object_id.as_ref() != Some(target_object_id)
                {
                    continue;
                }
                if hardlinks.len() >= max_entries {
                    has_more = true;
                    break 'pages;
                }
                last_cursor = Some(self.object_page_cursor_for_object(&object)?);
                hardlinks.push(object);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(DfsObjectPage {
            objects: hardlinks,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }
    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut count = 0usize;
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                1024,
            )?;
            count = count.saturating_add(page.objects.len());
            let Some(next_cursor) = page.next_cursor else {
                return Ok(count);
            };
            cursor = Some(next_cursor);
        }
    }
    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }
    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                1024,
            )?;
            for object in page.objects {
                if !posix_path_is_at_or_under_prefix(&object.path, &prefix) {
                    return Ok(Some(object));
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next_cursor);
        }
    }
    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry>;
    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object_entry = self.put_object(object)?;
        let version_entry = self.put_version(version)?;
        Ok(vec![object_entry, version_entry])
    }
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.versions_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage>;
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
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage>;
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
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.intents_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage>;
    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let mut intents = Vec::new();
        let mut cursor = after.cloned();
        loop {
            let page = self.intents_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for intent in page.intents {
                if intent.target_version != *target_version {
                    continue;
                }
                if intents.len() >= max_entries {
                    return Ok(DfsWriteIntentPage {
                        next_cursor: intents.last().map(write_intent_page_cursor),
                        intents,
                    });
                }
                intents.push(intent);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor: None,
        })
    }
    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage>;
    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage>;
    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage>;
    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage>;
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_versions(&self) -> Result<Vec<DfsObjectVersionRecord>> {
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.version_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_receipts(&self) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.receipt_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.write_intents_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_conflicts(&self) -> Result<Vec<DfsConflictRecord>> {
        let mut conflicts = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.conflict_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(conflicts)
    }
    fn accumulate_metadata_summary_for_object(
        &self,
        summary: &mut DfsMetadataSummary,
        object: &DfsObjectMetadata,
    ) -> Result<()> {
        summary.object_count = summary.object_count.saturating_add(1);

        let mut object_intents = Vec::new();
        let mut intent_cursor = None;
        loop {
            let intent_page = self.intents_for_object_page(
                &object.object_id,
                intent_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.intent_count = summary
                .intent_count
                .saturating_add(intent_page.intents.len());
            summary.pending_write_count = summary.pending_write_count.saturating_add(
                intent_page
                    .intents
                    .iter()
                    .filter(|intent| write_intent_status_is_active(&intent.status))
                    .count(),
            );
            object_intents.extend(intent_page.intents);
            let Some(next_cursor) = intent_page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }

        let committed_targets = object_intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .map(|intent| intent.target_version.clone())
            .collect::<BTreeSet<_>>();
        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        let mut version_cursor = None;
        loop {
            let version_page = self.versions_for_object_page(
                &object.object_id,
                version_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.version_count = summary
                .version_count
                .saturating_add(version_page.versions.len());
            for version in version_page.versions {
                let collect_for_repair = committed_targets.contains(&version.version);
                let mut receipt_cursor = None;
                loop {
                    let receipt_page = self.receipts_for_version_page(
                        &object.object_id,
                        &version.version,
                        receipt_cursor.as_ref(),
                        OBJECT_LOCAL_METADATA_PAGE_SIZE,
                    )?;
                    summary.receipt_count = summary
                        .receipt_count
                        .saturating_add(receipt_page.receipts.len());
                    if collect_for_repair {
                        receipts_by_version
                            .entry(version.version.clone())
                            .or_default()
                            .extend(receipt_page.receipts);
                    }
                    let Some(next_cursor) = receipt_page.next_cursor else {
                        break;
                    };
                    receipt_cursor = Some(next_cursor);
                }
            }
            let Some(next_cursor) = version_page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }

        let mut conflict_cursor = None;
        loop {
            let conflict_page = self.conflicts_for_object_page(
                &object.object_id,
                conflict_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.open_conflict_count = summary.open_conflict_count.saturating_add(
                conflict_page
                    .conflicts
                    .iter()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count(),
            );
            let Some(next_cursor) = conflict_page.next_cursor else {
                break;
            };
            conflict_cursor = Some(next_cursor);
        }

        summary.pending_repair_count =
            summary
                .pending_repair_count
                .saturating_add(count_pending_repairs_for_object(
                    object,
                    &object_intents,
                    &receipts_by_version,
                ));
        Ok(())
    }

    fn accumulate_metadata_summary_for_file_system(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        let mut object_cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                self.accumulate_metadata_summary_for_object(summary, &object)?;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn accumulate_namespace_job_summary(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_scope: Option<&DfsFileSystemId>,
    ) -> Result<()> {
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                if file_system_scope
                    .is_some_and(|file_system_id| job.file_system_id != *file_system_id)
                {
                    continue;
                }
                match job.status {
                    DfsNamespaceJobStatus::Running => {
                        summary.pending_namespace_job_count =
                            summary.pending_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Failed => {
                        summary.failed_namespace_job_count =
                            summary.failed_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Completed => {}
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let filesystems = self.list_file_systems()?;
        let mut summary = DfsMetadataSummary {
            file_system_count: filesystems.len(),
            ..DfsMetadataSummary::default()
        };
        for file_system in &filesystems {
            self.accumulate_metadata_summary_for_file_system(
                &mut summary,
                &file_system.file_system_id,
            )?;
        }
        self.accumulate_namespace_job_summary(&mut summary, None)?;
        Ok(summary)
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        self.accumulate_metadata_summary_for_file_system(&mut summary, file_system_id)?;
        self.accumulate_namespace_job_summary(&mut summary, Some(file_system_id))?;
        Ok(Some(summary))
    }
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
    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let _ = transaction_id;
        Ok(None)
    }
    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let _ = options;
        Ok(DfsTombstoneCompactionReport::default())
    }
    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _ = job;
        Ok(())
    }
    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let _ = job_id;
        Ok(None)
    }
    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            jobs.extend(page.jobs);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(jobs)
    }
    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let _ = after;
        let _ = max_entries;
        Ok(DfsNamespaceJobPage {
            jobs: Vec::new(),
            next_cursor: None,
        })
    }
    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry>;
    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;
    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let mut conflicts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.conflicts_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(conflicts)
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage>;
    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>>;
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;
    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| status.journal_last_sequence)
            .max()
            .unwrap_or(0))
    }
    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot>;
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
    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool>;
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}

#[derive(Clone, Debug)]
pub struct LocalSobjMetadataBackend {
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
    hardlink_paths: BTreeMap<(DfsObjectId, String, DfsObjectId), DfsPathRecord>,
    compacted_tombstones: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), DfsObjectId>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), DfsObjectVersionRecord>,
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
    intent_order: BTreeMap<(OffsetDateTime, String, DfsObjectId), (DfsObjectId, String)>,
    intent_targets: BTreeMap<(DfsObjectId, DfsVersion, String), (DfsObjectId, String)>,
    conflicts: BTreeMap<(DfsObjectId, String), DfsConflictRecord>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), DfsVisibleVersionCommitRecord>,
    namespace_transactions: BTreeMap<String, DfsNamespaceTransaction>,
    namespace_jobs: BTreeMap<String, DfsNamespaceJobRecord>,
}

#[derive(Clone, Debug, Default)]
struct DfsMetadataTableState {
    tables: DfsMetadataTables,
    table_log_offset: u64,
    snapshot_format_version: Option<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshot {
    format_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tables: Option<DfsMetadataTableSnapshotTables>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ops: Vec<DfsMetadataTableOp>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshotTables {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    file_systems: Vec<DfsFileSystem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    file_system_objects: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    paths: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    inodes: Vec<DfsInodeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dirfrags: Vec<DfsDirFragmentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dentries: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    hardlinks: Vec<DfsHardlinkIndexSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    compacted_tombstones: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    visible_commits: Vec<DfsVisibleCommitSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    namespace_transactions: Vec<DfsNamespaceTransaction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    namespace_jobs: Vec<DfsNamespaceJobRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsHardlinkIndexSnapshotRecord {
    target_object_id: DfsObjectId,
    hardlink_object_id: DfsObjectId,
    path_record: DfsPathRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsVisibleCommitSnapshotRecord {
    object_id: DfsObjectId,
    version: DfsVersion,
    commit: DfsVisibleVersionCommitRecord,
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
            || !self.namespace_jobs.is_empty()
            || !self.compacted_tombstones.is_empty()
            || !self.visible_commits.is_empty()
    }

    fn put_intent_indexed(&mut self, intent: DfsWriteIntentRecord) {
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(previous) = self.intents.insert(key.clone(), intent.clone()) {
            self.intent_order.remove(&write_intent_scan_key(&previous));
            self.intent_targets
                .remove(&write_intent_target_key(&previous));
        }
        self.intent_order
            .insert(write_intent_scan_key(&intent), key);
        self.intent_targets
            .insert(write_intent_target_key(&intent), intent_key(&intent));
    }

    fn rebuild_intent_indexes(&mut self) {
        self.intent_order.clear();
        self.intent_targets.clear();
        for intent in self.intents.values() {
            let key = intent_key(intent);
            self.intent_order
                .insert(write_intent_scan_key(intent), key.clone());
            self.intent_targets
                .insert(write_intent_target_key(intent), key);
        }
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
            DfsMetadataTableOp::RemoveObject { object_id } => {
                self.objects.remove(&object_id);
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
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                self.inodes.remove(&inode_id);
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
                self.put_hardlink_index(target_object_id, hardlink_object_id, path_record);
            }
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => {
                self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
            }
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                self.put_compacted_tombstone(object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                self.remove_compacted_tombstone_path(&file_system_id, &path);
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
                self.put_intent_indexed(intent);
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
            DfsMetadataTableOp::PutNamespaceJob { job } => {
                self.namespace_jobs.insert(job.job_id.clone(), job);
            }
        }
    }

    fn apply_with_rollback(
        &mut self,
        op: DfsMetadataTableOp,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        rollback.record(self, &op);
        self.apply(op);
    }

    fn put_compacted_tombstone(&mut self, object: DfsObjectMetadata) {
        let object = normalized_object_metadata(&object);
        let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
        if let Some(previous_object_id) = self
            .compacted_tombstone_paths
            .insert(path_key, object.object_id.clone())
            && previous_object_id != object.object_id
        {
            self.compacted_tombstones.remove(&previous_object_id);
        }
        self.compacted_tombstones
            .insert(object.object_id.clone(), object);
    }

    fn remove_compacted_tombstone_path(&mut self, file_system_id: &DfsFileSystemId, path: &str) {
        let path_key = (file_system_id.clone(), normalize_path(path));
        if let Some(object_id) = self.compacted_tombstone_paths.remove(&path_key) {
            self.compacted_tombstones.remove(&object_id);
        }
    }

    fn compacted_tombstone_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Option<DfsObjectMetadata> {
        let path_key = (file_system_id.clone(), normalize_path(path));
        self.compacted_tombstone_paths
            .get(&path_key)
            .and_then(|object_id| self.compacted_tombstones.get(object_id))
            .filter(|object| {
                object.kind == DfsObjectKind::Tombstone
                    && object.file_system_id == *file_system_id
                    && normalize_path(&object.path) == path_key.1
            })
            .cloned()
    }

    fn compacted_tombstone_matches(&self, object: &DfsObjectMetadata) -> bool {
        if object.kind != DfsObjectKind::Tombstone {
            return false;
        }
        self.compacted_tombstones
            .get(&object.object_id)
            .is_some_and(|stored| {
                normalized_object_metadata(stored) == normalized_object_metadata(object)
            })
    }

    fn put_hardlink_index(
        &mut self,
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
        path_record: DfsPathRecord,
    ) {
        self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
        self.hardlink_paths.insert(
            (
                target_object_id.clone(),
                path_record.path.clone(),
                hardlink_object_id.clone(),
            ),
            path_record.clone(),
        );
        self.hardlinks
            .insert((target_object_id, hardlink_object_id), path_record);
    }

    fn remove_hardlink_index(
        &mut self,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) {
        if let Some(path_record) = self
            .hardlinks
            .remove(&(target_object_id.clone(), hardlink_object_id.clone()))
        {
            self.hardlink_paths.remove(&(
                target_object_id.clone(),
                path_record.path,
                hardlink_object_id.clone(),
            ));
        }
    }

    fn rebuild_hardlink_path_index(&mut self) {
        self.hardlink_paths = self
            .hardlinks
            .iter()
            .map(|((target_object_id, hardlink_object_id), path_record)| {
                (
                    (
                        target_object_id.clone(),
                        path_record.path.clone(),
                        hardlink_object_id.clone(),
                    ),
                    path_record.clone(),
                )
            })
            .collect();
    }
}

#[derive(Debug, Default)]
struct DfsMetadataTableRollback {
    file_systems: BTreeMap<DfsFileSystemId, Option<DfsFileSystem>>,
    objects: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), Option<DfsPathRecord>>,
    paths: BTreeMap<(DfsFileSystemId, String), Option<DfsPathRecord>>,
    inodes: BTreeMap<DfsInodeId, Option<DfsInodeRecord>>,
    dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), Option<DfsDirFragmentRecord>>,
    dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), Option<DfsPathRecord>>,
    hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), Option<DfsPathRecord>>,
    compacted_tombstones: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), Option<DfsObjectId>>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsObjectVersionRecord>>,
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptRecord>>,
    intents: BTreeMap<(DfsObjectId, String), Option<DfsWriteIntentRecord>>,
    conflicts: BTreeMap<(DfsObjectId, String), Option<DfsConflictRecord>>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsVisibleVersionCommitRecord>>,
    namespace_transactions: BTreeMap<String, Option<DfsNamespaceTransaction>>,
    namespace_jobs: BTreeMap<String, Option<DfsNamespaceJobRecord>>,
}

impl DfsMetadataTableRollback {
    fn record(&mut self, tables: &DfsMetadataTables, op: &DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => record_rollback_value(
                &mut self.file_systems,
                &tables.file_systems,
                file_system.file_system_id.clone(),
            ),
            DfsMetadataTableOp::PutObject { object } => {
                record_rollback_value(&mut self.objects, &tables.objects, object.object_id.clone())
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                record_rollback_value(&mut self.objects, &tables.objects, object_id.clone())
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (
                    path_record.file_system_id.clone(),
                    path_record.object_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (file_system_id.clone(), object_id.clone()),
            ),
            DfsMetadataTableOp::PutPath { path_record } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (path_record.file_system_id.clone(), path_record.path.clone()),
            ),
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (file_system_id.clone(), path.clone()),
            ),
            DfsMetadataTableOp::PutInode { inode } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode.inode_id.clone())
            }
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode_id.clone())
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => record_rollback_value(
                &mut self.dirfrags,
                &tables.dirfrags,
                (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
            ),
            DfsMetadataTableOp::PutDentry { path_record } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                dentry_key(path_record),
            ),
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                (
                    parent_inode_id.clone(),
                    dirfrag_id.clone(),
                    *name_hash,
                    name.clone(),
                ),
            ),
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                ..
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                record_compacted_tombstone_rollback(self, tables, object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                record_compacted_tombstone_path_rollback(self, tables, file_system_id, path);
            }
            DfsMetadataTableOp::PutVersion { version } => record_rollback_value(
                &mut self.versions,
                &tables.versions,
                (version.object_id.clone(), version.version.clone()),
            ),
            DfsMetadataTableOp::PutReceipt { receipt } => record_rollback_value(
                &mut self.receipts,
                &tables.receipts,
                (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                ),
            ),
            DfsMetadataTableOp::PutIntent { intent } => record_rollback_value(
                &mut self.intents,
                &tables.intents,
                (intent.object_id.clone(), intent.intent_id.clone()),
            ),
            DfsMetadataTableOp::PutConflict { conflict } => record_rollback_value(
                &mut self.conflicts,
                &tables.conflicts,
                (conflict.object_id.clone(), conflict.conflict_id.clone()),
            ),
            DfsMetadataTableOp::PutVisibleCommit {
                object_id, version, ..
            } => record_rollback_value(
                &mut self.visible_commits,
                &tables.visible_commits,
                (object_id.clone(), version.clone()),
            ),
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => record_rollback_value(
                &mut self.namespace_transactions,
                &tables.namespace_transactions,
                transaction.transaction_id.clone(),
            ),
            DfsMetadataTableOp::PutNamespaceJob { job } => record_rollback_value(
                &mut self.namespace_jobs,
                &tables.namespace_jobs,
                job.job_id.clone(),
            ),
        }
    }

    fn rollback(self, tables: &mut DfsMetadataTables) {
        rollback_map(&mut tables.file_systems, self.file_systems);
        rollback_map(&mut tables.objects, self.objects);
        rollback_map(&mut tables.file_system_objects, self.file_system_objects);
        rollback_map(&mut tables.paths, self.paths);
        rollback_map(&mut tables.inodes, self.inodes);
        rollback_map(&mut tables.dirfrags, self.dirfrags);
        rollback_map(&mut tables.dentries, self.dentries);
        rollback_map(&mut tables.hardlinks, self.hardlinks);
        tables.rebuild_hardlink_path_index();
        rollback_map(&mut tables.compacted_tombstones, self.compacted_tombstones);
        rollback_map(
            &mut tables.compacted_tombstone_paths,
            self.compacted_tombstone_paths,
        );
        rollback_map(&mut tables.versions, self.versions);
        rollback_map(&mut tables.receipts, self.receipts);
        rollback_map(&mut tables.intents, self.intents);
        tables.rebuild_intent_indexes();
        rollback_map(&mut tables.conflicts, self.conflicts);
        rollback_map(&mut tables.visible_commits, self.visible_commits);
        rollback_map(
            &mut tables.namespace_transactions,
            self.namespace_transactions,
        );
        rollback_map(&mut tables.namespace_jobs, self.namespace_jobs);
    }
}

fn record_rollback_value<K, V>(
    rollback: &mut BTreeMap<K, Option<V>>,
    table: &BTreeMap<K, V>,
    key: K,
) where
    K: Clone + Ord,
    V: Clone,
{
    if !rollback.contains_key(&key) {
        rollback.insert(key.clone(), table.get(&key).cloned());
    }
}

fn record_compacted_tombstone_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) {
    let object_id = object.object_id.clone();
    let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
    record_rollback_value(
        &mut rollback.compacted_tombstones,
        &tables.compacted_tombstones,
        object_id,
    );
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(previous_object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            previous_object_id.clone(),
        );
    }
}

fn record_compacted_tombstone_path_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    path: &str,
) {
    let path_key = (file_system_id.clone(), normalize_path(path));
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            object_id.clone(),
        );
    }
}

fn rollback_map<K, V>(table: &mut BTreeMap<K, V>, rollback: BTreeMap<K, Option<V>>)
where
    K: Ord,
{
    for (key, value) in rollback {
        if let Some(value) = value {
            table.insert(key, value);
        } else {
            table.remove(&key);
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
    RemoveObject {
        object_id: DfsObjectId,
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
    RemoveInode {
        inode_id: DfsInodeId,
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
    PutCompactedTombstone {
        object: DfsObjectMetadata,
    },
    RemoveCompactedTombstonePath {
        file_system_id: DfsFileSystemId,
        path: String,
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
    PutNamespaceJob {
        job: DfsNamespaceJobRecord,
    },
}

#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    shards: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
}

#[derive(Clone, Debug)]
struct DfsObjectShardMigrationBundle {
    object: DfsObjectMetadata,
    versions: Vec<DfsObjectVersionRecord>,
    receipts: Vec<DfsReplicaReceiptRecord>,
    intents: Vec<DfsWriteIntentRecord>,
    conflicts: Vec<DfsConflictRecord>,
}

#[derive(Clone, Debug)]
struct DfsDirectoryPageEntries {
    entries: Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
    next_cursor: Option<DfsDirectoryPageCursor>,
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
            let backend = LocalSobjMetadataBackend::new(
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
        read_sobj_optional(&self.object_shard_index_path(object_id))
    }

    fn write_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_sobj_atomic(
            &self.object_shard_index_path(&object.object_id),
            &DfsObjectShardIndexRecord {
                object_id: object.object_id.clone(),
                file_system_id: object.file_system_id.clone(),
                path: normalize_path(&object.path),
                shard_id: shard_id.clone(),
            },
        )
    }

    fn remove_object_shard_index(&self, object_id: &DfsObjectId) -> Result<()> {
        remove_file_if_exists(&self.object_shard_index_path(object_id))
    }

    fn shard_backend(&self, shard_id: &DfsMetadataShardId) -> Result<&LocalSobjMetadataBackend> {
        self.shards.get(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!("unknown local DFS metadata shard: {}", shard_id.0))
        })
    }

    fn shard_backend_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&mut LocalSobjMetadataBackend> {
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
            if self.shards.contains_key(&index.shard_id) {
                return Ok(Some(index.shard_id));
            }
        }
        for (shard_id, backend) in &self.shards {
            if backend.get_object(object_id)?.is_some() {
                return Ok(Some(shard_id.clone()));
            }
        }
        Ok(None)
    }

    fn object_is_visible_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(true);
        }
        let Some(index) = self.read_object_shard_index(&object.object_id)? else {
            return Ok(true);
        };
        if !self.shards.contains_key(&index.shard_id) {
            return Ok(true);
        }
        Ok(index.shard_id == *shard_id)
    }

    fn object_is_listable_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if let Some(index) = self.read_object_shard_index(&object.object_id)?
            && self.shards.contains_key(&index.shard_id)
            && index.shard_id != *shard_id
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        backend: &LocalSobjMetadataBackend,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if let Some(object) = backend.get_object(&intent.object_id)? {
            return self.object_is_listable_from_shard(&object, shard_id);
        }
        if let Some(object) = intent.object_metadata.as_ref()
            && object.object_id == intent.object_id
            && object.file_system_id == intent.file_system_id
        {
            return self.object_is_listable_from_shard(object, shard_id);
        }
        if let Some(index) = self.read_object_shard_index(&intent.object_id)?
            && self.shards.contains_key(&index.shard_id)
            && index.shard_id != *shard_id
        {
            return Ok(false);
        }
        Ok(true)
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

    fn first_hardlink_to_object_matching<P>(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        mut matches: P,
    ) -> Result<Option<DfsObjectMetadata>>
    where
        P: FnMut(&DfsObjectMetadata) -> bool,
    {
        let mut best = None::<DfsObjectMetadata>;
        for (shard_id, backend) in &self.shards {
            let mut cursor = None;
            loop {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? || !matches(&object)
                    {
                        continue;
                    }
                    if best
                        .as_ref()
                        .is_none_or(|current| object_page_key(&object) < object_page_key(current))
                    {
                        best = Some(object);
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(best)
    }

    pub fn ensure_file_system_on_all_shards(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<usize> {
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
        let mut written = 0usize;
        for shard_id in shard_ids {
            let backend = self.shard_backend_mut(&shard_id)?;
            if backend.get_file_system(&file_system.file_system_id)? == Some(file_system.clone()) {
                continue;
            }
            backend.put_file_system(file_system)?;
            written = written.saturating_add(1);
        }
        Ok(written)
    }

    pub fn migrate_objects_to_configured_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        max_migrations: usize,
    ) -> Result<DfsMetadataShardMigrationReport> {
        let file_system = self.get_file_system(file_system_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            }
        })?;
        let mut report = DfsMetadataShardMigrationReport {
            file_system_id: file_system_id.clone(),
            scanned_objects: 0,
            indexed_objects: 0,
            migrated_objects: 0,
            skipped_stale_sources: 0,
            reached_limit: false,
        };
        let shard_ids = self.shard_ids();
        let mut visited_object_ids = BTreeSet::<DfsObjectId>::new();
        for source_shard_id in shard_ids {
            let mut cursor = None;
            loop {
                let page = self
                    .shard_backend(&source_shard_id)?
                    .objects_for_file_system_page(
                        file_system_id,
                        cursor.as_ref(),
                        SHARD_MIGRATION_PAGE_SIZE,
                    )?;
                let next_cursor = page.next_cursor;
                for object in page.objects {
                    if visited_object_ids.contains(&object.object_id) {
                        continue;
                    }
                    if report.migrated_objects >= max_migrations {
                        report.reached_limit = true;
                        return Ok(report);
                    }
                    report.scanned_objects = report.scanned_objects.saturating_add(1);
                    if let Some(index) = self.read_object_shard_index(&object.object_id)?
                        && index.shard_id != source_shard_id
                    {
                        report.skipped_stale_sources =
                            report.skipped_stale_sources.saturating_add(1);
                        continue;
                    }
                    visited_object_ids.insert(object.object_id.clone());
                    let target_shard_id =
                        self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
                    if source_shard_id == target_shard_id {
                        let expected_path = normalize_path(&object.path);
                        let needs_index = self
                            .read_object_shard_index(&object.object_id)?
                            .is_none_or(|index| {
                                index.shard_id != target_shard_id || index.path != expected_path
                            });
                        if needs_index {
                            self.write_object_shard_index(&object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                        }
                        continue;
                    }
                    let bundle =
                        self.object_shard_migration_bundle(&source_shard_id, &object.object_id)?;
                    self.install_object_shard_migration_bundle(
                        &file_system,
                        &bundle,
                        &target_shard_id,
                    )?;
                    self.write_object_shard_index(&bundle.object, &target_shard_id)?;
                    self.put_cross_shard_tombstone(
                        &bundle.object,
                        &source_shard_id,
                        &target_shard_id,
                    )?;
                    report.migrated_objects = report.migrated_objects.saturating_add(1);
                }
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(report)
    }

    fn object_shard_migration_bundle(
        &self,
        source_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectShardMigrationBundle> {
        let source = self.shard_backend(source_shard_id)?;
        let object =
            source
                .get_object(object_id)?
                .ok_or_else(|| MetadataBackendError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        let mut versions = Vec::new();
        let mut version_cursor = None;
        loop {
            let page = source.versions_for_object_page(
                object_id,
                version_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }
        let mut receipts = Vec::new();
        for version in &versions {
            let mut receipt_cursor = None;
            loop {
                let page = source.receipts_for_version_page(
                    object_id,
                    &version.version,
                    receipt_cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                receipts.extend(page.receipts);
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
        }
        let mut intents = Vec::new();
        let mut intent_cursor = None;
        loop {
            let page = source.intents_for_object_page(
                object_id,
                intent_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        let mut conflicts = Vec::new();
        let mut conflict_cursor = None;
        loop {
            let page = source.conflicts_for_object_page(
                object_id,
                conflict_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            conflict_cursor = Some(next_cursor);
        }
        Ok(DfsObjectShardMigrationBundle {
            object,
            versions,
            receipts,
            intents,
            conflicts,
        })
    }

    fn install_object_shard_migration_bundle(
        &mut self,
        file_system: &DfsFileSystem,
        bundle: &DfsObjectShardMigrationBundle,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        {
            let target = self.shard_backend_mut(target_shard_id)?;
            match target.get_file_system(&file_system.file_system_id)? {
                Some(existing) if existing != *file_system => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting filesystem {} during migration",
                        target_shard_id.0, file_system.file_system_id
                    )));
                }
                Some(_) => {}
                None => {
                    target.put_file_system(file_system)?;
                }
            }
        }

        for version in &bundle.versions {
            let target = self.shard_backend_mut(target_shard_id)?;
            match target.get_version(&version.object_id, &version.version)? {
                Some(existing) if existing != *version => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting version {} for object {} during migration",
                        target_shard_id.0, version.version, version.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    target.put_version(version)?;
                }
            }
        }
        for receipt in &bundle.receipts {
            let target = self.shard_backend_mut(target_shard_id)?;
            if let Some(existing) = receipt_for_replica_from_backend(
                target,
                &receipt.receipt.object_id,
                &receipt.receipt.committed_version,
                &receipt.replica_node_id,
            )? {
                if &existing != receipt {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting receipt for object {} version {} replica {} during migration",
                        target_shard_id.0,
                        receipt.receipt.object_id,
                        receipt.receipt.committed_version,
                        receipt.replica_node_id
                    )));
                }
            } else {
                target.put_receipt(receipt)?;
            }
        }
        for intent in &bundle.intents {
            let target = self.shard_backend_mut(target_shard_id)?;
            match target.get_intent(&intent.object_id, &intent.intent_id)? {
                Some(existing) if existing != *intent => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting intent {} for object {} during migration",
                        target_shard_id.0, intent.intent_id, intent.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    target.put_intent(intent)?;
                }
            }
        }
        for conflict in &bundle.conflicts {
            let target = self.shard_backend_mut(target_shard_id)?;
            match target.get_conflict(&conflict.object_id, &conflict.conflict_id)? {
                Some(existing) if existing != *conflict => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting conflict {} for object {} during migration",
                        target_shard_id.0, conflict.conflict_id, conflict.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    target.put_conflict(conflict)?;
                }
            }
        }
        {
            let target = self.shard_backend_mut(target_shard_id)?;
            match target.get_object(&bundle.object.object_id)? {
                Some(existing) if existing != bundle.object => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting object {} during migration",
                        target_shard_id.0, bundle.object.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    target.put_object(&bundle.object)?;
                }
            }
        }
        Ok(())
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
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        let entry = self
            .shard_backend_mut(&target_shard_id)?
            .put_object(&object)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
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
        let object = self
            .shard_backend(&shard_id)?
            .get_object_by_path(file_system_id, path)?;
        if let Some(object) = object.as_ref()
            && !self.object_is_visible_from_shard(object, &shard_id)?
        {
            return Ok(None);
        }
        if object.is_some() {
            return Ok(object);
        }
        for (fallback_shard_id, backend) in &self.shards {
            if fallback_shard_id == &shard_id {
                continue;
            }
            let Some(candidate) = backend.get_object_by_path(file_system_id, path)? else {
                continue;
            };
            if self.object_is_visible_from_shard(&candidate, fallback_shard_id)? {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = BTreeMap::<DfsObjectId, DfsObjectMetadata>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = None;
            loop {
                let page = backend.objects_for_file_system_page(
                    file_system_id,
                    cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if let Some(index) = self.read_object_shard_index(&object.object_id)?
                        && index.shard_id != *shard_id
                    {
                        continue;
                    }
                    objects.insert(object.object_id.clone(), object);
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
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

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = Vec::<(DfsObjectMetadata, DfsObjectPageCursor)>::new();
        'shards: for (shard_id, backend) in &self.shards {
            if let Some(resume_shard_id) = resume_shard_id
                && shard_id < resume_shard_id
            {
                continue;
            }
            let mut local_after = match resume_shard_id {
                Some(resume_shard_id) if resume_shard_id == shard_id => {
                    after.map(local_object_page_cursor)
                }
                Some(_) => None,
                None => after.map(local_object_page_cursor),
            };
            loop {
                let page = backend.objects_for_file_system_page(
                    file_system_id,
                    local_after.as_ref(),
                    max_entries.saturating_add(1),
                )?;
                let next_local_cursor = page.next_cursor;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    let cursor = object_page_cursor_with_shard(&object, shard_id);
                    entries.push((object, cursor));
                    if entries.len() > max_entries {
                        break 'shards;
                    }
                }
                let Some(next_local_cursor) = next_local_cursor else {
                    break;
                };
                local_after = Some(next_local_cursor);
            }
        }
        let has_more = entries.len() > max_entries;
        if has_more {
            entries.truncate(max_entries);
        }
        let next_cursor = if has_more {
            entries.last().map(|(_, cursor)| cursor.clone())
        } else {
            None
        };
        Ok(DfsObjectPage {
            objects: entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor,
        })
    }

    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: self.shard_id_for_object(&object.object_id)?,
        })
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = Vec::<(DfsObjectMetadata, DfsDirectoryPageCursor)>::new();
        'shards: for (shard_id, backend) in &self.shards {
            if let Some(resume_shard_id) = resume_shard_id
                && shard_id < resume_shard_id
            {
                continue;
            }
            let mut local_after = match resume_shard_id {
                Some(resume_shard_id) if resume_shard_id == shard_id => {
                    after.map(local_directory_page_cursor)
                }
                Some(_) => None,
                None => after.map(local_directory_page_cursor),
            };
            loop {
                let page = backend.child_objects_for_directory_page_entries(
                    file_system_id,
                    directory_path,
                    local_after.as_ref(),
                    max_entries.saturating_add(1),
                )?;
                let next_local_cursor = page.next_cursor;
                for (object, local_cursor) in page.entries {
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    entries.push((
                        object,
                        directory_page_cursor_with_shard(local_cursor, shard_id),
                    ));
                    if entries.len() > max_entries {
                        break 'shards;
                    }
                }
                let Some(next_local_cursor) = next_local_cursor else {
                    break;
                };
                local_after = Some(next_local_cursor);
            }
        }
        let has_more = entries.len() > max_entries;
        if has_more {
            entries.truncate(max_entries);
        }
        let next_cursor = if has_more {
            entries.last().map(|(_, cursor)| cursor.clone())
        } else {
            None
        };
        Ok(DfsDirectoryPage {
            objects: entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            hardlinks.extend(page.objects);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        hardlinks.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(hardlinks)
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = Vec::<(DfsObjectMetadata, DfsObjectPageCursor)>::new();
        'shards: for (shard_id, backend) in &self.shards {
            if let Some(resume_shard_id) = resume_shard_id
                && shard_id < resume_shard_id
            {
                continue;
            }
            let mut local_after = match resume_shard_id {
                Some(resume_shard_id) if resume_shard_id == shard_id => {
                    after.map(local_object_page_cursor)
                }
                Some(_) => None,
                None => after.map(local_object_page_cursor),
            };
            loop {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    local_after.as_ref(),
                    max_entries.saturating_add(1),
                )?;
                let next_local_cursor = page.next_cursor;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    let cursor = object_page_cursor_with_shard(&object, shard_id);
                    entries.push((object, cursor));
                    if entries.len() > max_entries {
                        break 'shards;
                    }
                }
                let Some(next_local_cursor) = next_local_cursor else {
                    break;
                };
                local_after = Some(next_local_cursor);
            }
        }
        let has_more = entries.len() > max_entries;
        if has_more {
            entries.truncate(max_entries);
        }
        let next_cursor = if has_more {
            entries.last().map(|(_, cursor)| cursor.clone())
        } else {
            None
        };
        Ok(DfsObjectPage {
            objects: entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor,
        })
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut object_ids = BTreeSet::<DfsObjectId>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = None;
            loop {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if self.object_is_listable_from_shard(&object, shard_id)? {
                        object_ids.insert(object.object_id);
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(object_ids.len())
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |_| true)
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |object| {
            !posix_path_is_at_or_under_prefix(&object.path, &prefix)
        })
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&version.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version(version)
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_and_version(&object, version)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entries)
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
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.versions_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsObjectVersionPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_object_version_page_cursor);
        let mut page = self.shard_backend(&shard_id)?.versions_for_object_page(
            object_id,
            local_after.as_ref(),
            max_entries,
        )?;
        if page.next_cursor.is_some() {
            page.next_cursor = page
                .next_cursor
                .map(|cursor| object_version_page_cursor_with_shard(cursor, &shard_id));
        }
        Ok(page)
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
        let entry = self.shard_backend_mut(&shard_id)?.put_receipt(receipt)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsReplicaReceiptPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_replica_receipt_page_cursor);
        let mut page = self.shard_backend(&shard_id)?.receipts_for_version_page(
            object_id,
            version,
            local_after.as_ref(),
            max_entries,
        )?;
        if page.next_cursor.is_some() {
            page.next_cursor = page
                .next_cursor
                .map(|cursor| replica_receipt_page_cursor_with_shard(cursor, &shard_id));
        }
        Ok(page)
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
        let entry = self.shard_backend_mut(&shard_id)?.put_intent(intent)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .get_intent(object_id, intent_id);
        }
        for backend in self.shards.values() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                return Ok(Some(intent));
            }
        }
        Ok(None)
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let mut intents = Vec::new();
            let mut cursor = None;
            loop {
                let page = self.shard_backend(&shard_id)?.intents_for_object_page(
                    object_id,
                    cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                intents.extend(page.intents);
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
            return Ok(intents);
        }
        let mut intents = BTreeMap::<String, DfsWriteIntentRecord>::new();
        for backend in self.shards.values() {
            let mut cursor = None;
            loop {
                let page = backend.intents_for_object_page(
                    object_id,
                    cursor.as_ref(),
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for intent in page.intents {
                    intents.entry(intent.intent_id.clone()).or_insert(intent);
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(intents.into_values().collect())
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let mut page = self.shard_backend(&shard_id)?.intents_for_object_page(
                object_id,
                local_after.as_ref(),
                max_entries,
            )?;
            if page.next_cursor.is_some() {
                page.next_cursor = page
                    .next_cursor
                    .map(|cursor| write_intent_page_cursor_with_shard(cursor, &shard_id));
            }
            return Ok(page);
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = Vec::<(DfsWriteIntentRecord, DfsWriteIntentPageCursor)>::new();
        let mut seen_intent_ids = BTreeSet::<String>::new();
        'shards: for (shard_id, backend) in &self.shards {
            if let Some(resume_shard_id) = resume_shard_id
                && shard_id < resume_shard_id
            {
                continue;
            }
            let mut local_after = match resume_shard_id {
                Some(resume_shard_id) if resume_shard_id == shard_id => {
                    after.map(local_write_intent_page_cursor)
                }
                Some(_) => None,
                None => after.map(local_write_intent_page_cursor),
            };
            loop {
                let page = backend.intents_for_object_page(
                    object_id,
                    local_after.as_ref(),
                    max_entries.saturating_add(1),
                )?;
                let next_local_cursor = page.next_cursor;
                for intent in page.intents {
                    if !seen_intent_ids.insert(intent.intent_id.clone()) {
                        continue;
                    }
                    let cursor = write_intent_page_cursor_with_shard(
                        write_intent_page_cursor(&intent),
                        shard_id,
                    );
                    entries.push((intent, cursor));
                    if entries.len() > max_entries {
                        break 'shards;
                    }
                }
                let Some(next_local_cursor) = next_local_cursor else {
                    break;
                };
                local_after = Some(next_local_cursor);
            }
        }
        let has_more = entries.len() > max_entries;
        if has_more {
            entries.truncate(max_entries);
        }
        let next_cursor = if has_more {
            entries.last().map(|(_, cursor)| cursor.clone())
        } else {
            None
        };
        Ok(DfsWriteIntentPage {
            intents: entries.into_iter().map(|(intent, _)| intent).collect(),
            next_cursor,
        })
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let mut page = self
                .shard_backend(&shard_id)?
                .intents_for_object_version_page(
                    object_id,
                    target_version,
                    local_after.as_ref(),
                    max_entries,
                )?;
            if page.next_cursor.is_some() {
                page.next_cursor = page
                    .next_cursor
                    .map(|cursor| write_intent_page_cursor_with_shard(cursor, &shard_id));
            }
            return Ok(page);
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = Vec::<(DfsWriteIntentRecord, DfsWriteIntentPageCursor)>::new();
        let mut seen_intent_ids = BTreeSet::<String>::new();
        'shards: for (shard_id, backend) in &self.shards {
            if let Some(resume_shard_id) = resume_shard_id
                && shard_id < resume_shard_id
            {
                continue;
            }
            let mut local_after = match resume_shard_id {
                Some(resume_shard_id) if resume_shard_id == shard_id => {
                    after.map(local_write_intent_page_cursor)
                }
                Some(_) => None,
                None => after.map(local_write_intent_page_cursor),
            };
            loop {
                let page = backend.intents_for_object_version_page(
                    object_id,
                    target_version,
                    local_after.as_ref(),
                    max_entries.saturating_add(1),
                )?;
                let next_local_cursor = page.next_cursor;
                for intent in page.intents {
                    if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
                        continue;
                    }
                    if !seen_intent_ids.insert(intent.intent_id.clone()) {
                        continue;
                    }
                    let cursor = write_intent_page_cursor_with_shard(
                        write_intent_page_cursor(&intent),
                        shard_id,
                    );
                    entries.push((intent, cursor));
                    if entries.len() > max_entries {
                        break 'shards;
                    }
                }
                let Some(next_local_cursor) = next_local_cursor else {
                    break;
                };
                local_after = Some(next_local_cursor);
            }
        }
        let has_more = entries.len() > max_entries;
        if has_more {
            entries.truncate(max_entries);
        }
        let next_cursor = if has_more {
            entries.last().map(|(_, cursor)| cursor.clone())
        } else {
            None
        };
        Ok(DfsWriteIntentPage {
            intents: entries.into_iter().map(|(intent, _)| intent).collect(),
            next_cursor,
        })
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentScanPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let per_shard_limit = max_entries.saturating_add(1);
        let mut intents = Vec::new();
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = after.cloned();
            let mut shard_intents = 0usize;
            loop {
                let page = backend.write_intents_page(cursor.as_ref(), per_shard_limit)?;
                let next_cursor = page.next_cursor;
                for intent in page.intents {
                    if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
                        continue;
                    }
                    if seen.insert((intent.object_id.clone(), intent.intent_id.clone())) {
                        intents.push(intent);
                        shard_intents = shard_intents.saturating_add(1);
                    }
                    if shard_intents > max_entries {
                        break;
                    }
                }
                if shard_intents > max_entries {
                    break;
                }
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        intents.sort_by(write_intent_scan_order);
        let has_more = intents.len() > max_entries;
        if has_more {
            intents.truncate(max_entries);
        }
        let next_cursor = if has_more {
            intents.last().map(write_intent_scan_cursor)
        } else {
            None
        };
        Ok(DfsWriteIntentScanPage {
            intents,
            next_cursor,
        })
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        if max_entries == 0 {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        let per_shard_limit = max_entries.saturating_add(1);
        let mut versions = Vec::new();
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion)>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = after.cloned();
            let mut shard_versions = 0usize;
            loop {
                let page = backend.version_records_page(cursor.as_ref(), per_shard_limit)?;
                let next_cursor = page.next_cursor;
                for version in page.versions {
                    let Some(object) = backend.get_object(&version.object_id)? else {
                        continue;
                    };
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    if seen.insert((version.object_id.clone(), version.version.clone())) {
                        versions.push(version);
                        shard_versions = shard_versions.saturating_add(1);
                    }
                    if shard_versions > max_entries {
                        break;
                    }
                }
                if shard_versions > max_entries {
                    break;
                }
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        versions.sort_by(|left, right| {
            object_version_scan_key(left).cmp(&object_version_scan_key(right))
        });
        let has_more = versions.len() > max_entries;
        if has_more {
            versions.truncate(max_entries);
        }
        let next_cursor = if has_more {
            versions.last().map(object_version_scan_cursor)
        } else {
            None
        };
        Ok(DfsObjectVersionScanPage {
            versions,
            next_cursor,
        })
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        if max_entries == 0 {
            return Ok(DfsReplicaReceiptScanPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        }
        let per_shard_limit = max_entries.saturating_add(1);
        let mut receipts = Vec::new();
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion, NodeId)>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = after.cloned();
            let mut shard_receipts = 0usize;
            loop {
                let page = backend.receipt_records_page(cursor.as_ref(), per_shard_limit)?;
                let next_cursor = page.next_cursor;
                for receipt in page.receipts {
                    let Some(object) = backend.get_object(&receipt.receipt.object_id)? else {
                        continue;
                    };
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    let key = (
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                        receipt.replica_node_id.clone(),
                    );
                    if seen.insert(key) {
                        receipts.push(receipt);
                        shard_receipts = shard_receipts.saturating_add(1);
                    }
                    if shard_receipts > max_entries {
                        break;
                    }
                }
                if shard_receipts > max_entries {
                    break;
                }
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        receipts.sort_by(|left, right| {
            replica_receipt_scan_key(left).cmp(&replica_receipt_scan_key(right))
        });
        let has_more = receipts.len() > max_entries;
        if has_more {
            receipts.truncate(max_entries);
        }
        let next_cursor = if has_more {
            receipts.last().map(replica_receipt_scan_cursor)
        } else {
            None
        };
        Ok(DfsReplicaReceiptScanPage {
            receipts,
            next_cursor,
        })
    }

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        if max_entries == 0 {
            return Ok(DfsConflictScanPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        }
        let per_shard_limit = max_entries.saturating_add(1);
        let mut conflicts = Vec::new();
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        for (shard_id, backend) in &self.shards {
            let mut cursor = after.cloned();
            let mut shard_conflicts = 0usize;
            loop {
                let page = backend.conflict_records_page(cursor.as_ref(), per_shard_limit)?;
                let next_cursor = page.next_cursor;
                for conflict in page.conflicts {
                    let Some(object) = backend.get_object(&conflict.object_id)? else {
                        continue;
                    };
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    if seen.insert((conflict.object_id.clone(), conflict.conflict_id.clone())) {
                        conflicts.push(conflict);
                        shard_conflicts = shard_conflicts.saturating_add(1);
                    }
                    if shard_conflicts > max_entries {
                        break;
                    }
                }
                if shard_conflicts > max_entries {
                    break;
                }
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        conflicts.sort_by(|left, right| conflict_scan_key(left).cmp(&conflict_scan_key(right)));
        let has_more = conflicts.len() > max_entries;
        if has_more {
            conflicts.truncate(max_entries);
        }
        let next_cursor = if has_more {
            conflicts.last().map(conflict_scan_cursor)
        } else {
            None
        };
        Ok(DfsConflictScanPage {
            conflicts,
            next_cursor,
        })
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
        let mut versions_by_shard =
            BTreeMap::<DfsMetadataShardId, Vec<DfsObjectVersionRecord>>::new();
        let mut object_shards = BTreeMap::<DfsObjectId, DfsMetadataShardId>::new();
        let mut cross_shard_tombstones =
            Vec::<(DfsObjectMetadata, DfsMetadataShardId, DfsMetadataShardId)>::new();
        for object in &transaction.objects {
            let object = normalized_object_metadata(object);
            let target_shard_id =
                self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            object_shards.insert(object.object_id.clone(), target_shard_id.clone());
            if let Some(source_shard_id) = self.shard_id_for_object(&object.object_id)?
                && source_shard_id != target_shard_id
            {
                cross_shard_tombstones.push((
                    object.clone(),
                    source_shard_id,
                    target_shard_id.clone(),
                ));
            }
            objects_by_shard
                .entry(target_shard_id)
                .or_default()
                .push(object);
        }
        for version in &transaction.versions {
            let version = normalized_version_record(version);
            let shard_id = if let Some(shard_id) = object_shards.get(&version.object_id) {
                shard_id.clone()
            } else {
                self.require_shard_id_for_object(&version.object_id)?
            };
            versions_by_shard.entry(shard_id).or_default().push(version);
        }
        let mut shard_ids = objects_by_shard.keys().cloned().collect::<BTreeSet<_>>();
        shard_ids.extend(versions_by_shard.keys().cloned());
        let mut last_entry = None;
        for shard_id in shard_ids {
            let objects = objects_by_shard.remove(&shard_id).unwrap_or_default();
            let versions = versions_by_shard.remove(&shard_id).unwrap_or_default();
            let shard_transaction = DfsNamespaceTransaction {
                transaction_id: format!("{}:{}", transaction.transaction_id, shard_id.0),
                file_system_id: transaction.file_system_id.clone(),
                objects,
                versions,
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
        for (object, source_shard_id, target_shard_id) in cross_shard_tombstones {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        last_entry.ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "namespace transaction {} did not contain any objects or versions",
                transaction.transaction_id
            ))
        })
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let shard_ids = self.shard_ids();
        let mut compacted_object_ids = Vec::new();
        for shard_id in shard_ids {
            let mut shard_options = options.clone();
            if !options.dry_run
                && let Some(max_compactions) = options.max_compactions
            {
                let remaining = max_compactions.saturating_sub(report.compacted_tombstone_count);
                if remaining == 0 {
                    report.max_compactions_reached = true;
                    break;
                }
                shard_options.max_compactions = Some(remaining);
            }
            let (shard_report, shard_compacted_object_ids) = self
                .shard_backend_mut(&shard_id)?
                .compact_tombstones_with_ids(shard_options)?;
            report.merge(shard_report);
            compacted_object_ids.extend(shard_compacted_object_ids);
        }
        for object_id in compacted_object_ids {
            self.remove_object_shard_index(&object_id)?;
        }
        Ok(report)
    }

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let shard_id = self.select_shard_id_for_path(&job.file_system_id, &job.root_path)?;
        self.shard_backend_mut(&shard_id)?.put_namespace_job(job)
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        for backend in self.shards.values() {
            if let Some(transaction) = backend.get_namespace_transaction(transaction_id)? {
                return Ok(Some(transaction));
            }
        }
        Ok(None)
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        for backend in self.shards.values() {
            if let Some(job) = backend.get_namespace_job(job_id)? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                jobs.insert(job.job_id.clone(), job);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(jobs.into_values().collect())
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        if max_entries == 0 {
            return Ok(DfsNamespaceJobPage {
                jobs: Vec::new(),
                next_cursor: None,
            });
        }
        let mut merged = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        let mut shard_has_more = false;
        for backend in self.shards.values() {
            let page = backend.list_namespace_jobs_page(after, max_entries.saturating_add(1))?;
            shard_has_more |= page.next_cursor.is_some();
            for job in page.jobs {
                merged.insert(job.job_id.clone(), job);
            }
        }
        let mut jobs = merged.into_values().collect::<Vec<_>>();
        let has_more = jobs.len() > max_entries || shard_has_more;
        if jobs.len() > max_entries {
            jobs.truncate(max_entries);
        }
        let next_cursor = if has_more {
            jobs.last().map(|job| DfsNamespaceJobPageCursor {
                job_id: job.job_id.clone(),
            })
        } else {
            None
        };
        Ok(DfsNamespaceJobPage { jobs, next_cursor })
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let mut summary = DfsMetadataSummary {
            file_system_count: self.list_file_systems()?.len(),
            ..DfsMetadataSummary::default()
        };
        for (shard_id, backend) in &self.shards {
            let tables = backend.current_tables_snapshot()?;
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            summary.object_count = summary
                .object_count
                .saturating_add(shard_summary.object_count);
            summary.version_count = summary
                .version_count
                .saturating_add(shard_summary.version_count);
            summary.receipt_count = summary
                .receipt_count
                .saturating_add(shard_summary.receipt_count);
            summary.intent_count = summary
                .intent_count
                .saturating_add(shard_summary.intent_count);
            summary.pending_write_count = summary
                .pending_write_count
                .saturating_add(shard_summary.pending_write_count);
            summary.open_conflict_count = summary
                .open_conflict_count
                .saturating_add(shard_summary.open_conflict_count);
            summary.pending_repair_count = summary
                .pending_repair_count
                .saturating_add(shard_summary.pending_repair_count);
            summary.pending_namespace_job_count = summary
                .pending_namespace_job_count
                .saturating_add(shard_summary.pending_namespace_job_count);
            summary.failed_namespace_job_count = summary
                .failed_namespace_job_count
                .saturating_add(shard_summary.failed_namespace_job_count);
        }
        Ok(summary)
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        for (shard_id, backend) in &self.shards {
            let tables = backend.current_tables_snapshot()?;
            let Some(shard_summary) =
                metadata_summary_for_file_system_from_tables(&tables, file_system_id, |object| {
                    self.object_is_listable_from_shard(object, shard_id)
                })?
            else {
                continue;
            };
            summary.object_count = summary
                .object_count
                .saturating_add(shard_summary.object_count);
            summary.version_count = summary
                .version_count
                .saturating_add(shard_summary.version_count);
            summary.receipt_count = summary
                .receipt_count
                .saturating_add(shard_summary.receipt_count);
            summary.intent_count = summary
                .intent_count
                .saturating_add(shard_summary.intent_count);
            summary.pending_write_count = summary
                .pending_write_count
                .saturating_add(shard_summary.pending_write_count);
            summary.open_conflict_count = summary
                .open_conflict_count
                .saturating_add(shard_summary.open_conflict_count);
            summary.pending_repair_count = summary
                .pending_repair_count
                .saturating_add(shard_summary.pending_repair_count);
            summary.pending_namespace_job_count = summary
                .pending_namespace_job_count
                .saturating_add(shard_summary.pending_namespace_job_count);
            summary.failed_namespace_job_count = summary
                .failed_namespace_job_count
                .saturating_add(shard_summary.failed_namespace_job_count);
        }
        Ok(Some(summary))
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
        let mut conflicts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.shard_backend(&shard_id)?.conflicts_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(conflicts)
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsConflictPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        };
        self.shard_backend(&shard_id)?
            .conflicts_for_object_page(object_id, after, max_entries)
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

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        let mut tables = DfsMetadataTableSnapshotTables::default();
        for backend in self.shards.values() {
            if let Some(snapshot_tables) =
                LocalSobjMetadataBackend::export_table_snapshot(backend).tables
            {
                tables.file_systems.extend(snapshot_tables.file_systems);
                tables.objects.extend(snapshot_tables.objects);
                tables
                    .file_system_objects
                    .extend(snapshot_tables.file_system_objects);
                tables.paths.extend(snapshot_tables.paths);
                tables.inodes.extend(snapshot_tables.inodes);
                tables.dirfrags.extend(snapshot_tables.dirfrags);
                tables.dentries.extend(snapshot_tables.dentries);
                tables.hardlinks.extend(snapshot_tables.hardlinks);
                tables
                    .compacted_tombstones
                    .extend(snapshot_tables.compacted_tombstones);
                tables.versions.extend(snapshot_tables.versions);
                tables.receipts.extend(snapshot_tables.receipts);
                tables.intents.extend(snapshot_tables.intents);
                tables.conflicts.extend(snapshot_tables.conflicts);
                tables
                    .visible_commits
                    .extend(snapshot_tables.visible_commits);
                tables
                    .namespace_transactions
                    .extend(snapshot_tables.namespace_transactions);
                tables.namespace_jobs.extend(snapshot_tables.namespace_jobs);
            }
        }
        Ok(DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            tables: Some(tables),
            ops: Vec::new(),
        })
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

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot(source_shard_id, replayed_through_sequence, snapshot)
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

impl LocalSobjMetadataBackend {
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
        let mut replay_entries = Vec::new();
        for entry in &batch.entries {
            if !replay_cursors.contains_key(&entry.shard_id) {
                let replay_cursor_path = self.replay_cursor_path(&entry.shard_id);
                let cursor = read_sobj_optional::<DfsMetadataReplayCursor>(&replay_cursor_path)?
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
            replay_entries.push(entry);
            if entry.sequence > local_cursor.last_sequence {
                local_cursor.last_sequence = entry.sequence;
                local_cursor_advanced = true;
            }
            cursor.last_sequence = entry.sequence;
            advanced_cursors.insert(entry.shard_id.clone());
        }
        self.persist_replayed_journal_entries_unlocked(&replay_entries)?;

        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let mut table_sequence = 0u64;
        let replay_result = (|| -> Result<()> {
            for entry in replay_entries {
                if self.materialize_replayed_journal_entry_into_batch(
                    entry,
                    &mut table_ops,
                    &mut rollback,
                )? {
                    applied = applied.saturating_add(1);
                    table_sequence = entry.sequence;
                }
            }
            Ok(())
        })();
        if let Err(error) = replay_result {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.append_preapplied_table_ops(table_sequence, table_ops, rollback)?;
        if local_cursor_advanced {
            self.write_local_materialization_cursor(&local_cursor)?;
        }
        for shard_id in advanced_cursors {
            if let Some(cursor) = replay_cursors.get(&shard_id) {
                write_sobj_atomic(&self.replay_cursor_path(&cursor.shard_id), cursor)?;
            }
        }
        Ok(applied)
    }

    pub fn journal_last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    pub fn export_table_snapshot(&self) -> DfsMetadataTableSnapshot {
        DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            tables: Some(metadata_tables_snapshot(&self.tables)),
            ops: Vec::new(),
        }
    }

    pub fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        let _lock = self.begin_metadata_write()?;
        let tables = metadata_tables_from_table_snapshot(snapshot, "replicated metadata snapshot")?;
        let archived_existing = archive_metadata_backend_state(&self.root)?;
        create_dir_all(self.root.join("records"))?;
        create_dir_all(self.root.join("journal").join("segments"))?;
        self.tables = tables;
        self.table_log_offset = 0;
        self.next_sequence = replayed_through_sequence.saturating_add(1).max(1);
        persist_metadata_table_snapshot(&self.root, &self.tables)?;
        write_sobj_atomic(
            &self.replay_cursor_path(&source_shard_id),
            &DfsMetadataReplayCursor {
                shard_id: source_shard_id,
                last_sequence: replayed_through_sequence,
            },
        )?;
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: replayed_through_sequence,
        })?;
        write_sobj_atomic(
            &self.journal_head_path(),
            &DfsMetadataJournalHead {
                next_sequence: self.next_sequence,
            },
        )?;
        Ok(archived_existing)
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

    fn persist_replayed_journal_entries_unlocked(
        &mut self,
        entries: &[&DfsMetadataJournalEntry],
    ) -> Result<()> {
        let mut entries_by_segment = BTreeMap::<u64, Vec<&DfsMetadataJournalEntry>>::new();
        let mut max_sequence = 0u64;
        for entry in entries {
            entries_by_segment
                .entry(journal_segment_id(entry.sequence))
                .or_default()
                .push(*entry);
            max_sequence = max_sequence.max(entry.sequence);
        }
        for (segment_id, entries) in entries_by_segment.iter_mut() {
            entries.sort_by_key(|entry| entry.sequence);
            let path = self
                .root
                .join("journal")
                .join("segments")
                .join(format!("{segment_id:020}.slog"));
            let existing = if path.exists() {
                read_segmented_journal_file_since_limited(&path, 0, usize::MAX)?
                    .into_iter()
                    .map(|entry| (entry.sequence, entry))
                    .collect::<BTreeMap<_, _>>()
            } else {
                BTreeMap::new()
            };
            let mut missing = Vec::new();
            for entry in entries.iter().copied() {
                if let Some(existing) = existing.get(&entry.sequence) {
                    if existing == entry {
                        continue;
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal sequence {} already exists with a different entry while replaying shard {}",
                        entry.sequence, entry.shard_id.0
                    )));
                }
                missing.push(entry);
            }
            append_segmented_journal_entries(&self.root, &missing)?;
        }
        if max_sequence > 0 {
            self.advance_journal_head_after_replayed_entry(max_sequence)?;
        }
        Ok(())
    }

    fn advance_journal_head_after_replayed_entry(&mut self, sequence: u64) -> Result<()> {
        let next_sequence = sequence.saturating_add(1).max(1).max(self.next_sequence);
        if next_sequence == self.next_sequence {
            return Ok(());
        }
        self.next_sequence = next_sequence;
        write_sobj_atomic(
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
                let object = normalized_object_metadata(object);
                if self.get_object(object_id)? == Some(object.clone())
                    || self.tables.compacted_tombstone_matches(&object)
                {
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
                if let Some(existing) =
                    receipt_for_replica_from_backend(self, object_id, version, replica_node_id)?
                {
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
                if self.tables.namespace_transactions.get(transaction_id) == Some(&normalized) {
                    let mut objects_materialized = true;
                    for object in &normalized.objects {
                        if !self.object_is_materialized_or_compacted(object)? {
                            objects_materialized = false;
                            break;
                        }
                    }
                    let versions_materialized = normalized.versions.iter().all(|version| {
                        self.get_version(&version.object_id, &version.version)
                            .is_ok_and(|stored| stored == Some(normalized_version_record(version)))
                    });
                    if objects_materialized && versions_materialized {
                        return Ok(false);
                    }
                }
                self.materialize_local_journal_entry(entry)?;
                Ok(true)
            }
        }
    }

    fn materialize_replayed_journal_entry_into_batch(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
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
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutFileSystem {
                        file_system: file_system.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutObject {
                object_id, object, ..
            } => {
                let object = required_replay_payload(entry, "put-object", object.as_ref())?;
                let object = normalized_object_metadata(object);
                if self.get_object(object_id)? == Some(object.clone())
                    || self.tables.compacted_tombstone_matches(&object)
                {
                    return Ok(false);
                }
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(object_id)?;
                self.plan_object_index_table_ops_with_previous(
                    &object,
                    previous.as_ref(),
                    table_ops,
                    rollback,
                )?;
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
                let version_record = normalized_version_record(version_record);
                if self.get_version(object_id, version)? == Some(version_record.clone()) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutVersion {
                        version: version_record,
                    },
                    table_ops,
                    rollback,
                );
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
                if let Some(existing) =
                    receipt_for_replica_from_backend(self, object_id, version, replica_node_id)?
                {
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
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
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
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutIntent {
                        intent: intent.clone(),
                    },
                    table_ops,
                    rollback,
                );
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
                self.plan_visible_version_commit_table_ops(&commit_record, table_ops, rollback)?;
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
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutConflict {
                        conflict: conflict.clone(),
                    },
                    table_ops,
                    rollback,
                );
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
                if self.tables.namespace_transactions.get(transaction_id) == Some(&normalized) {
                    let mut objects_materialized = true;
                    for object in &normalized.objects {
                        if !self.object_is_materialized_or_compacted(object)? {
                            objects_materialized = false;
                            break;
                        }
                    }
                    let versions_materialized = normalized.versions.iter().all(|version| {
                        self.get_version(&version.object_id, &version.version)
                            .is_ok_and(|stored| stored == Some(normalized_version_record(version)))
                    });
                    if objects_materialized && versions_materialized {
                        return Ok(false);
                    }
                }
                self.plan_namespace_transaction_table_ops(&normalized, table_ops, rollback)?;
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
            if let Some(cursor) = read_sobj_optional::<DfsMetadataReplayCursor>(&path)? {
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
                *receipt =
                    receipt_for_replica_from_backend(self, object_id, version, replica_node_id)?;
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
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                committed_version,
                committed_object,
                visible_version,
                ..
            } if committed_object.is_none() || visible_version.is_none() => {
                let commit_key = (object_id.clone(), committed_version.clone());
                if let Some(commit) = self.tables.visible_commits.get(&commit_key) {
                    if committed_object.is_none() {
                        *committed_object = Some(commit.committed_object.clone());
                    }
                    if visible_version.is_none() {
                        *visible_version = Some(commit.visible_version.clone());
                    }
                } else {
                    if committed_object.is_none()
                        && let Some(object) = self.get_object(object_id)?
                        && object.version == *committed_version
                    {
                        *committed_object = Some(object);
                    }
                    if visible_version.is_none() {
                        *visible_version = self.get_version(object_id, committed_version)?;
                    }
                }
            }
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction,
                ..
            } if transaction.is_none() => {
                *transaction = self.get_namespace_transaction(transaction_id)?;
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
        write_sobj_atomic(
            &self.journal_head_path(),
            &DfsMetadataJournalHead {
                next_sequence: self.next_sequence,
            },
        )?;
        Ok(entry)
    }

    fn write_journal_entries_unlocked(
        &mut self,
        mutations: Vec<DfsMetadataMutation>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        let mut next_sequence = self
            .next_sequence
            .max(fast_next_journal_sequence(&self.root)?);
        let mut entries = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            entries.push(DfsMetadataJournalEntry {
                shard_id: self.shard_id.clone(),
                sequence: next_sequence,
                created_at: OffsetDateTime::now_utc(),
                mutation,
            });
            next_sequence = next_sequence.saturating_add(1);
        }

        let mut chunk_start = 0usize;
        while chunk_start < entries.len() {
            let segment_id = journal_segment_id(entries[chunk_start].sequence);
            let mut chunk_end = chunk_start + 1;
            while chunk_end < entries.len()
                && journal_segment_id(entries[chunk_end].sequence) == segment_id
            {
                chunk_end += 1;
            }
            let chunk = entries[chunk_start..chunk_end].iter().collect::<Vec<_>>();
            append_segmented_journal_entries(&self.root, &chunk)?;
            self.next_sequence = entries[chunk_end - 1].sequence.saturating_add(1);
            write_sobj_atomic(
                &self.journal_head_path(),
                &DfsMetadataJournalHead {
                    next_sequence: self.next_sequence,
                },
            )?;
            chunk_start = chunk_end;
        }
        Ok(entries)
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
        write_sobj_atomic(
            &self.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: self.shard_id.clone(),
                last_sequence,
            },
        )
    }

    fn local_materialization_cursor(&self) -> Result<DfsMetadataReplayCursor> {
        Ok(read_sobj_optional::<DfsMetadataReplayCursor>(
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

    fn checkpoint_local_materialization_cursor_if_due(&self, sequence: u64) -> Result<()> {
        let cursor = self.local_materialization_cursor()?;
        if sequence <= cursor.last_sequence
            || sequence.saturating_sub(cursor.last_sequence)
                < LOCAL_MATERIALIZATION_CURSOR_FLUSH_INTERVAL
        {
            return Ok(());
        }
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: cursor.shard_id,
            last_sequence: sequence,
        })
    }

    fn write_local_materialization_cursor(&self, cursor: &DfsMetadataReplayCursor) -> Result<()> {
        write_sobj_atomic(&self.local_materialization_cursor_path(), cursor)
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
                if self.tables.compacted_tombstone_matches(object) {
                    return Ok(());
                }
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
                self.materialize_namespace_transaction(&transaction, entry.sequence)
            }
        }
    }

    fn records_root(&self) -> PathBuf {
        self.root.join("records")
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

    fn append_preapplied_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let written =
            match append_metadata_table_batch(&self.root, &DfsMetadataTableBatch { sequence, ops })
            {
                Ok(written) => written,
                Err(error) => {
                    rollback.rollback(&mut self.tables);
                    return Err(error);
                }
            };
        self.table_log_offset = self.table_log_offset.saturating_add(written);
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

    fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPageEntries> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPageEntries {
                entries: Vec::new(),
                next_cursor: None,
            });
        }
        let directory_path = normalize_posix_path(directory_path);
        let parent_inode_id = directory_inode_id(file_system_id, &directory_path);
        let tables = self.current_tables_snapshot()?;
        let start = match after.and_then(|cursor| cursor.dentry.as_ref()) {
            Some(dentry) => Excluded((
                parent_inode_id.clone(),
                dentry.dirfrag_id.clone(),
                dentry.name_hash,
                dentry.name.clone(),
            )),
            None => Included((
                parent_inode_id.clone(),
                DfsDirFragmentId::new(""),
                0,
                String::new(),
            )),
        };
        let mut entries = Vec::<(DfsObjectMetadata, DfsDirectoryPageCursor)>::new();
        let mut next_cursor = None;
        for ((record_parent_inode_id, dirfrag_id, name_hash, name), path_record) in
            tables.dentries.range((start, Unbounded)).take_while(
                |((record_parent_inode_id, _, _, _), _)| record_parent_inode_id == &parent_inode_id,
            )
        {
            if path_record.file_system_id != *file_system_id
                || record_parent_inode_id != &parent_inode_id
                || path_record.parent_inode_id != parent_inode_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            let object_path = normalize_path(&object.path);
            let object_posix_path = normalize_posix_path(&object.path);
            if object.kind == DfsObjectKind::Tombstone
                || object.file_system_id != *file_system_id
                || object_posix_path == directory_path
                || object_path != path_record.path
            {
                continue;
            }
            if after.and_then(|cursor| cursor.dentry.as_ref()).is_none()
                && let Some(after) = after
                && directory_page_key(&object) <= (&after.path, &after.object_id)
            {
                continue;
            }
            let cursor = directory_page_cursor_with_dentry(
                &object,
                dirfrag_id.clone(),
                *name_hash,
                name.clone(),
            );
            if entries.len() >= max_entries {
                next_cursor = entries.last().map(|(_, cursor)| cursor.clone());
                break;
            }
            entries.push((object, cursor));
        }
        Ok(DfsDirectoryPageEntries {
            entries,
            next_cursor,
        })
    }
}

#[derive(Debug)]
struct MetadataWriteLock {
    _path_guard: MetadataPathWriteGuard,
    _file: fs::File,
}

/// In-process mutual exclusion for metadata writes, keyed by backend root. Two
/// `LocalSobjMetadataBackend` handles on the same `root` (e.g. several threads, or
/// the main + shadow daemon in one process) serialize here; handles on different
/// roots never contend (distinct keys). Cross-process exclusion is layered on top
/// via the `journal/write.lock` flock in `metadata_write_lock`.
///
/// `Drop` calls `notify_all` on the shared condvar: correct, but a thundering herd
/// since one condvar backs every root, so every release wakes waiters on unrelated
/// roots too. Acceptable because the guard is held only briefly (around a single
/// metadata write) and contention is bounded by the number of in-process handles;
/// a per-root condvar map would add shared state for marginal gain.
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

impl DfsMetadataBackend for LocalSobjMetadataBackend {
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let _lock = self.begin_metadata_write()?;
        let objects = objects
            .iter()
            .map(normalized_object_metadata)
            .collect::<Vec<_>>();
        for object in &objects {
            if self.get_file_system(&object.file_system_id)?.is_none() {
                return Err(MetadataBackendError::UnknownFileSystem {
                    file_system_id: object.file_system_id.clone(),
                });
            }
        }
        let entries = self.write_journal_entries_unlocked(
            objects
                .iter()
                .map(|object| DfsMetadataMutation::PutObject {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    path: object.path.clone(),
                    object: Some(object.clone()),
                })
                .collect(),
        )?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        for object in &objects {
            let previous = self.get_object(&object.object_id)?;
            if let Err(error) = self.plan_object_index_table_ops_with_previous(
                object,
                previous.as_ref(),
                &mut table_ops,
                &mut rollback,
            ) {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        }
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops(last_sequence, table_ops, rollback)?;
        self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        Ok(entries)
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
            return Ok(tables.compacted_tombstone_by_path(file_system_id, &normalized_path));
        };
        if path_record.file_system_id != *file_system_id {
            return Ok(None);
        }
        if normalize_path(&path_record.path) != normalized_path {
            return Ok(None);
        }
        if let Some(object) = tables.objects.get(&path_record.object_id).cloned()
            && object.file_system_id == *file_system_id
            && normalize_path(&object.path) == normalize_path(path)
        {
            return Ok(Some(object));
        }
        Ok(tables.compacted_tombstone_by_path(file_system_id, &normalized_path))
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            objects.extend(page.objects);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(objects)
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((file_system_id.clone(), after.path.clone())),
            None => Included((file_system_id.clone(), String::new())),
        };
        let mut objects = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_file_system_id, _), path_record) in tables
            .paths
            .range((start, Unbounded))
            .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id)
        {
            if record_file_system_id != file_system_id
                || path_record.file_system_id != *file_system_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            if object.file_system_id != *file_system_id
                || normalize_path(&object.path) != path_record.path
            {
                continue;
            }
            if let Some(after) = after
                && object_page_key(&object) <= (&after.path, &after.object_id)
            {
                continue;
            }
            if objects.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(object_page_cursor(&object));
            objects.push(object);
        }
        Ok(DfsObjectPage {
            objects,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        let mut count = 0usize;
        for ((record_file_system_id, _), path_record) in tables
            .file_system_objects
            .range((
                Included((file_system_id.clone(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id)
        {
            if record_file_system_id != file_system_id
                || path_record.file_system_id != *file_system_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id) else {
                continue;
            };
            if object.file_system_id == *file_system_id
                && normalize_path(&object.path) == path_record.path
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
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
        for ((record_parent_inode_id, _, _, _), path_record) in tables
            .dentries
            .range((
                Included((
                    parent_inode_id.clone(),
                    DfsDirFragmentId::new(""),
                    0,
                    String::new(),
                )),
                Unbounded,
            ))
            .take_while(|((record_parent_inode_id, _, _, _), _)| {
                record_parent_inode_id == &parent_inode_id
            })
        {
            if path_record.file_system_id != *file_system_id
                || record_parent_inode_id != &parent_inode_id
                || path_record.parent_inode_id != parent_inode_id
            {
                continue;
            }
            let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
                continue;
            };
            let object_path = normalize_path(&object.path);
            let object_posix_path = normalize_posix_path(&object.path);
            if object.kind == DfsObjectKind::Tombstone
                || object.file_system_id != *file_system_id
                || object_posix_path == directory_path
                || object_path != path_record.path
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

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let page = self.child_objects_for_directory_page_entries(
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: page.entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor: page.next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            hardlinks.extend(page.objects);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(hardlinks)
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((
                target_object_id.clone(),
                normalize_path(&after.path),
                after.object_id.clone(),
            )),
            None => Included((target_object_id.clone(), String::new(), id_lower_bound())),
        };
        let mut objects = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_target_object_id, _, _), path_record) in
            tables.hardlink_paths.range((start, Unbounded)).take_while(
                |((record_target_object_id, _, _), _)| record_target_object_id == target_object_id,
            )
        {
            if record_target_object_id != target_object_id {
                continue;
            }
            let Some(object) = hardlink_object_from_path_record(
                &tables,
                file_system_id,
                target_object_id,
                path_record,
            ) else {
                continue;
            };
            if objects.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(object_page_cursor(&object));
            objects.push(object);
        }
        Ok(DfsObjectPage {
            objects,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        let mut count = 0usize;
        for ((record_target_object_id, _, _), path_record) in tables
            .hardlink_paths
            .range((
                Included((target_object_id.clone(), String::new(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((record_target_object_id, _, _), _)| {
                record_target_object_id == target_object_id
            })
        {
            if record_target_object_id == target_object_id
                && hardlink_object_from_path_record(
                    &tables,
                    file_system_id,
                    target_object_id,
                    path_record,
                )
                .is_some()
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let tables = self.current_tables_snapshot()?;
        for ((record_target_object_id, _, _), path_record) in tables
            .hardlink_paths
            .range((
                Included((target_object_id.clone(), String::new(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((record_target_object_id, _, _), _)| {
                record_target_object_id == target_object_id
            })
        {
            if record_target_object_id != target_object_id
                || posix_path_is_at_or_under_prefix(&path_record.path, &prefix)
            {
                continue;
            }
            if let Some(object) = hardlink_object_from_path_record(
                &tables,
                file_system_id,
                target_object_id,
                path_record,
            ) {
                return Ok(Some(object));
            }
        }
        Ok(None)
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        let metadata_matches = object.file_system_id == version.file_system_id
            && object.object_id == version.object_id;
        if !metadata_matches {
            return Err(MetadataBackendError::Serde(format!(
                "object {} {} does not match version {} {}",
                object.file_system_id, object.object_id, version.file_system_id, version.object_id
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let previous = self.get_object(&object.object_id)?;
        let entries = self.write_journal_entries_unlocked(vec![
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            },
            DfsMetadataMutation::PutVersion {
                file_system_id: version.file_system_id.clone(),
                object_id: version.object_id.clone(),
                version: version.version.clone(),
                version_record: Some(version.clone()),
            },
        ])?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        if let Err(error) = self.plan_object_index_table_ops_with_previous(
            &object,
            previous.as_ref(),
            &mut table_ops,
            &mut rollback,
        ) {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVersion { version },
            &mut table_ops,
            &mut rollback,
        );
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops(last_sequence, table_ops, rollback)?;
        self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        Ok(entries)
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
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.versions_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        if max_entries == 0 {
            return Ok(DfsObjectVersionPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((object_id.clone(), after.version.clone())),
            None => Included((object_id.clone(), id_lower_bound())),
        };
        let mut versions = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, _), version) in tables
            .versions
            .range((start, Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        {
            if record_object_id != object_id || version.object_id != *object_id {
                continue;
            }
            if versions.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(object_version_page_cursor(version));
            versions.push(version.clone());
        }
        Ok(DfsObjectVersionPage {
            versions,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        if max_entries == 0 {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((after.object_id.clone(), after.version.clone())),
            None => Included((id_lower_bound(), id_lower_bound())),
        };
        let mut versions = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, record_version), version) in
            tables.versions.range((start, Unbounded))
        {
            if record_object_id != &version.object_id || record_version != &version.version {
                continue;
            }
            if versions.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(object_version_scan_cursor(version));
            versions.push(version.clone());
        }
        Ok(DfsObjectVersionScanPage {
            versions,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        if max_entries == 0 {
            return Ok(DfsReplicaReceiptScanPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((
                after.object_id.clone(),
                after.version.clone(),
                after.replica_node_id.clone(),
            )),
            None => Included((id_lower_bound(), id_lower_bound(), id_lower_bound())),
        };
        let mut receipts = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, record_version, record_replica_node_id), receipt) in
            tables.receipts.range((start, Unbounded))
        {
            if record_object_id != &receipt.receipt.object_id
                || record_version != &receipt.receipt.committed_version
                || record_replica_node_id != &receipt.replica_node_id
            {
                continue;
            }
            if receipts.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(replica_receipt_scan_cursor(receipt));
            receipts.push(receipt.clone());
        }
        Ok(DfsReplicaReceiptScanPage {
            receipts,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.write_intents_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        if max_entries == 0 {
            return Ok(DfsConflictScanPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((after.object_id.clone(), after.conflict_id.clone())),
            None => Included((id_lower_bound(), String::new())),
        };
        let mut conflicts = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, record_conflict_id), conflict) in
            tables.conflicts.range((start, Unbounded))
        {
            if record_object_id != &conflict.object_id
                || record_conflict_id != &conflict.conflict_id
            {
                continue;
            }
            if conflicts.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(conflict_scan_cursor(conflict));
            conflicts.push(conflict.clone());
        }
        Ok(DfsConflictScanPage {
            conflicts,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_from_tables(&tables, |_| Ok(true))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_for_file_system_from_tables(&tables, file_system_id, |_| Ok(true))
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        if max_entries == 0 {
            return Ok(DfsReplicaReceiptPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((
                object_id.clone(),
                version.clone(),
                after.replica_node_id.clone(),
            )),
            None => Included((object_id.clone(), version.clone(), id_lower_bound())),
        };
        let mut receipts = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, record_version, _), receipt) in tables
            .receipts
            .range((start, Unbounded))
            .take_while(|((record_object_id, record_version, _), _)| {
                record_object_id == object_id && record_version == version
            })
        {
            if record_object_id != object_id
                || record_version != version
                || receipt.receipt.object_id != *object_id
                || receipt.receipt.committed_version != *version
            {
                continue;
            }
            if receipts.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(replica_receipt_page_cursor(receipt));
            receipts.push(receipt.clone());
        }
        Ok(DfsReplicaReceiptPage {
            receipts,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
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
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.intents_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((object_id.clone(), after.intent_id.clone())),
            None => Included((object_id.clone(), String::new())),
        };
        let mut intents = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, _), intent) in tables
            .intents
            .range((start, Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        {
            if record_object_id != object_id || intent.object_id != *object_id {
                continue;
            }
            if intents.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(write_intent_page_cursor(intent));
            intents.push(intent.clone());
        }
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((
                object_id.clone(),
                target_version.clone(),
                after.intent_id.clone(),
            )),
            None => Included((object_id.clone(), target_version.clone(), String::new())),
        };
        let mut intents = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, record_version, _), intent_key) in tables
            .intent_targets
            .range((start, Unbounded))
            .take_while(|((record_object_id, record_version, _), _)| {
                record_object_id == object_id && record_version == target_version
            })
        {
            if record_object_id != object_id || record_version != target_version {
                continue;
            }
            let Some(intent) = tables.intents.get(intent_key) else {
                continue;
            };
            if intent.object_id != *object_id || intent.target_version != *target_version {
                continue;
            }
            if intents.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(write_intent_page_cursor(intent));
            intents.push(intent.clone());
        }
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentScanPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let mut intents = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        let mut collect_intent = |intent_key: &(DfsObjectId, String)| -> Result<bool> {
            let Some(intent) = tables.intents.get(intent_key) else {
                return Ok(false);
            };
            if intents.len() >= max_entries {
                return Ok(true);
            }
            last_cursor = Some(write_intent_scan_cursor(intent));
            intents.push(intent.clone());
            Ok(false)
        };
        match after {
            Some(after) => {
                for (_, intent_key) in tables
                    .intent_order
                    .range((Excluded(write_intent_scan_cursor_key(after)), Unbounded))
                {
                    if collect_intent(intent_key)? {
                        has_more = true;
                        break;
                    }
                }
            }
            None => {
                for intent_key in tables.intent_order.values() {
                    if collect_intent(intent_key)? {
                        has_more = true;
                        break;
                    }
                }
            }
        }
        Ok(DfsWriteIntentScanPage {
            intents,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
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
            self.materialize_namespace_transaction(transaction, entry.sequence)?;
        }
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        self.compact_tombstones_with_ids(options)
            .map(|(report, _)| report)
    }

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        self.append_and_apply_table_ops(
            self.next_sequence.saturating_sub(1).max(1),
            vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }],
        )
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_transactions.get(transaction_id).cloned())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_jobs.get(job_id).cloned())
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            jobs.extend(page.jobs);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(jobs)
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        if max_entries == 0 {
            return Ok(DfsNamespaceJobPage {
                jobs: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded(after.job_id.clone()),
            None => Included(String::new()),
        };
        let mut jobs = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for (job_id, job) in tables.namespace_jobs.range((start, Unbounded)) {
            if jobs.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(DfsNamespaceJobPageCursor {
                job_id: job_id.clone(),
            });
            jobs.push(job.clone());
        }
        Ok(DfsNamespaceJobPage {
            jobs,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
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
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
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
            .range((Included((object_id.clone(), String::new())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .map(|(_, conflict)| conflict)
            .filter(|conflict| conflict.object_id == *object_id)
            .cloned()
            .collect())
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        if max_entries == 0 {
            return Ok(DfsConflictPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        }
        let tables = self.current_tables_snapshot()?;
        let start = match after {
            Some(after) => Excluded((object_id.clone(), after.conflict_id.clone())),
            None => Included((object_id.clone(), String::new())),
        };
        let mut conflicts = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        for ((record_object_id, _), conflict) in tables
            .conflicts
            .range((start, Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        {
            if record_object_id != object_id || conflict.object_id != *object_id {
                continue;
            }
            if conflicts.len() >= max_entries {
                has_more = true;
                break;
            }
            last_cursor = Some(conflict_page_cursor(conflict));
            conflicts.push(conflict.clone());
        }
        Ok(DfsConflictPage {
            conflicts,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
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
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, max_entries)
    }

    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        LocalSobjMetadataBackend::journal_batch_since_limited(
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
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(LocalSobjMetadataBackend::journal_last_sequence(self))
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        Ok(LocalSobjMetadataBackend::export_table_snapshot(self))
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(read_sobj_optional::<DfsMetadataReplayCursor>(
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
        write_sobj_atomic(
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
        LocalSobjMetadataBackend::apply_journal_batch(self, batch)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot(
            self,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
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

impl LocalSobjMetadataBackend {
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
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let planning_result =
            self.plan_namespace_transaction_table_ops(transaction, &mut table_ops, &mut rollback);
        if let Err(error) = planning_result {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.append_preapplied_table_ops(sequence, table_ops, rollback)
    }

    fn plan_namespace_transaction_table_ops(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutNamespaceTransaction {
                transaction: transaction.clone(),
            },
            table_ops,
            rollback,
        );
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
        {
            if self.tables.compacted_tombstone_matches(object) {
                continue;
            }
            self.plan_object_index_table_ops(object, table_ops, rollback)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            if self.tables.compacted_tombstone_matches(object) {
                continue;
            }
            self.plan_object_index_table_ops(object, table_ops, rollback)?;
        }
        for version in &transaction.versions {
            self.plan_preapplied_table_op(
                DfsMetadataTableOp::PutVersion {
                    version: normalized_version_record(version),
                },
                table_ops,
                rollback,
            );
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
        for version in &transaction.versions {
            let version = normalized_version_record(version);
            if self.get_version(&version.object_id, &version.version)? != Some(version.clone()) {
                self.append_and_apply_table_ops(
                    sequence,
                    vec![DfsMetadataTableOp::PutVersion { version }],
                )?;
                changed = true;
            }
        }
        Ok(changed)
    }

    fn materialize_object_indexes_if_current(
        &mut self,
        object: &DfsObjectMetadata,
        sequence: u64,
    ) -> Result<bool> {
        if self.tables.compacted_tombstone_matches(object) {
            return Ok(false);
        }
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
        if self.tables.compacted_tombstone_matches(object) {
            return Ok(true);
        }
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

    fn object_is_materialized_or_compacted(&self, object: &DfsObjectMetadata) -> Result<bool> {
        if self.tables.compacted_tombstone_matches(object) {
            return Ok(true);
        }
        self.get_object(&object.object_id)
            .map(|stored| stored == Some(normalized_object_metadata(object)))
    }

    fn tombstone_has_live_references(&self, object_id: &DfsObjectId) -> bool {
        self.tables
            .versions
            .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .next()
            .is_some()
            || self
                .tables
                .receipts
                .range((
                    Included((object_id.clone(), id_lower_bound(), id_lower_bound())),
                    Unbounded,
                ))
                .take_while(|((record_object_id, _, _), _)| record_object_id == object_id)
                .next()
                .is_some()
            || self
                .tables
                .intents
                .range((Included((object_id.clone(), String::new())), Unbounded))
                .take_while(|((record_object_id, _), _)| record_object_id == object_id)
                .next()
                .is_some()
            || self
                .tables
                .conflicts
                .range((Included((object_id.clone(), String::new())), Unbounded))
                .take_while(|((record_object_id, _), _)| record_object_id == object_id)
                .next()
                .is_some()
            || self
                .tables
                .visible_commits
                .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
                .take_while(|((record_object_id, _), _)| record_object_id == object_id)
                .next()
                .is_some()
            || self.tables.hardlinks.iter().any(
                |((target_object_id, hardlink_object_id), path_record)| {
                    target_object_id == object_id
                        || hardlink_object_id == object_id
                        || &path_record.object_id == object_id
                },
            )
    }

    fn compact_tombstones_with_ids(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<(DfsTombstoneCompactionReport, Vec<DfsObjectId>)> {
        let _lock = self.begin_metadata_write()?;
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let mut compacted_object_ids = Vec::new();
        let candidates = self
            .tables
            .objects
            .values()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .cloned()
            .collect::<Vec<_>>();

        for object in candidates {
            report.scanned_tombstone_count = report.scanned_tombstone_count.saturating_add(1);
            let age_nanos = now.saturating_sub(object_timestamp_for_retention(&object));
            if age_nanos < options.min_tombstone_age_nanos {
                report.retained_young_tombstone_count =
                    report.retained_young_tombstone_count.saturating_add(1);
                continue;
            }
            if self.tombstone_has_live_references(&object.object_id) {
                report.retained_referenced_tombstone_count =
                    report.retained_referenced_tombstone_count.saturating_add(1);
                continue;
            }

            report.eligible_tombstone_count = report.eligible_tombstone_count.saturating_add(1);
            if !options.dry_run
                && options.max_compactions.is_some_and(|max_compactions| {
                    report.compacted_tombstone_count >= max_compactions
                })
            {
                report.max_compactions_reached = true;
                report.retained_limit_tombstone_count =
                    report.retained_limit_tombstone_count.saturating_add(1);
                continue;
            }
            if options.dry_run {
                continue;
            }

            let path_record = path_record_for_object(&object);
            let mut dentry_ops = Vec::new();
            self.remove_dentry_index(&path_record, &mut dentry_ops)?;
            for op in dentry_ops {
                self.plan_preapplied_table_op(op, &mut table_ops, &mut rollback);
            }
            if self
                .tables
                .file_system_objects
                .contains_key(&(object.file_system_id.clone(), object.object_id.clone()))
            {
                report.removed_file_system_object_record_count = report
                    .removed_file_system_object_record_count
                    .saturating_add(1);
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveFileSystemObject {
                        file_system_id: object.file_system_id.clone(),
                        object_id: object.object_id.clone(),
                    },
                    &mut table_ops,
                    &mut rollback,
                );
            }
            if self.tables.objects.contains_key(&object.object_id) {
                report.removed_object_record_count =
                    report.removed_object_record_count.saturating_add(1);
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveObject {
                        object_id: object.object_id.clone(),
                    },
                    &mut table_ops,
                    &mut rollback,
                );
            }
            let inode_id = object_inode_id(&object.object_id);
            if self.tables.inodes.contains_key(&inode_id) {
                report.removed_inode_record_count =
                    report.removed_inode_record_count.saturating_add(1);
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveInode { inode_id },
                    &mut table_ops,
                    &mut rollback,
                );
            }
            if self
                .tables
                .paths
                .contains_key(&(object.file_system_id.clone(), normalize_path(&object.path)))
            {
                report.preserved_path_marker_count =
                    report.preserved_path_marker_count.saturating_add(1);
            }
            self.plan_preapplied_table_op(
                DfsMetadataTableOp::PutCompactedTombstone {
                    object: object.clone(),
                },
                &mut table_ops,
                &mut rollback,
            );
            report.compacted_marker_count = report.compacted_marker_count.saturating_add(1);
            report.compacted_tombstone_count = report.compacted_tombstone_count.saturating_add(1);
            compacted_object_ids.push(object.object_id);
        }

        if options.dry_run || table_ops.is_empty() {
            return Ok((report, compacted_object_ids));
        }
        let sequence = self.next_sequence.saturating_sub(1).max(1);
        self.append_preapplied_table_ops(sequence, table_ops, rollback)?;
        self.compact_metadata_table_log_if_needed(0)?;
        Ok((report, compacted_object_ids))
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
        let table_ops = self.object_index_table_ops_with_previous(object, previous)?;
        self.append_and_apply_table_ops(sequence, table_ops)
    }

    fn plan_object_index_table_ops(
        &mut self,
        object: &DfsObjectMetadata,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        self.plan_object_index_table_ops_with_previous(object, None, table_ops, rollback)
    }

    fn plan_object_index_table_ops_with_previous(
        &mut self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let object_ops = self.object_index_table_ops_with_previous(object, previous)?;
        for op in object_ops {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn plan_preapplied_table_op(
        &mut self,
        op: DfsMetadataTableOp,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        self.tables.apply_with_rollback(op.clone(), rollback);
        table_ops.push(op);
    }

    fn plan_visible_version_commit_table_ops(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVisibleCommit {
                object_id: commit.committed_object.object_id.clone(),
                version: commit.committed_object.version.clone(),
                commit: commit.clone(),
            },
            table_ops,
            rollback,
        );
        self.plan_object_index_table_ops_with_previous(
            &commit.committed_object,
            None,
            table_ops,
            rollback,
        )?;
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVersion {
                version: normalized_version_record(&commit.visible_version),
            },
            table_ops,
            rollback,
        );
        Ok(())
    }

    fn object_index_table_ops_with_previous(
        &self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
    ) -> Result<Vec<DfsMetadataTableOp>> {
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
        if self
            .tables
            .compacted_tombstone_paths
            .contains_key(&(path_record.file_system_id.clone(), path_record.path.clone()))
        {
            table_ops.push(DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id: path_record.file_system_id.clone(),
                path: path_record.path.clone(),
            });
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
            return Ok(table_ops);
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
        Ok(table_ops)
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
        let visible_version = normalized_version_record(&commit.visible_version);
        let mut table_ops = vec![DfsMetadataTableOp::PutVisibleCommit {
            object_id: commit.committed_object.object_id.clone(),
            version: commit.committed_object.version.clone(),
            commit: commit.clone(),
        }];
        table_ops
            .extend(self.object_index_table_ops_with_previous(&commit.committed_object, None)?);
        table_ops.push(DfsMetadataTableOp::PutVersion {
            version: visible_version,
        });
        self.append_and_apply_table_ops(sequence, table_ops)
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

fn id_lower_bound() -> sinomo_core::Id {
    sinomo_core::Id::new("00000000000000000000000000").expect("static zero ULID")
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
    for version in &mut transaction.versions {
        normalize_node_locations(&mut version.locations);
    }
    transaction
}

fn object_page_key(object: &DfsObjectMetadata) -> (&String, &DfsObjectId) {
    (&object.path, &object.object_id)
}

fn conflict_page_cursor(conflict: &DfsConflictRecord) -> DfsConflictPageCursor {
    DfsConflictPageCursor {
        conflict_id: conflict.conflict_id.clone(),
    }
}

fn object_version_page_cursor(version: &DfsObjectVersionRecord) -> DfsObjectVersionPageCursor {
    DfsObjectVersionPageCursor {
        version: version.version.clone(),
        shard_id: None,
    }
}

fn object_version_scan_cursor(version: &DfsObjectVersionRecord) -> DfsObjectVersionScanCursor {
    DfsObjectVersionScanCursor {
        object_id: version.object_id.clone(),
        version: version.version.clone(),
    }
}

fn object_version_scan_key(version: &DfsObjectVersionRecord) -> (DfsObjectId, DfsVersion) {
    (version.object_id.clone(), version.version.clone())
}

fn object_version_page_cursor_with_shard(
    mut cursor: DfsObjectVersionPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectVersionPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

fn local_object_version_page_cursor(
    cursor: &DfsObjectVersionPageCursor,
) -> DfsObjectVersionPageCursor {
    DfsObjectVersionPageCursor {
        version: cursor.version.clone(),
        shard_id: None,
    }
}

fn replica_receipt_page_cursor(receipt: &DfsReplicaReceiptRecord) -> DfsReplicaReceiptPageCursor {
    DfsReplicaReceiptPageCursor {
        replica_node_id: receipt.replica_node_id.clone(),
        shard_id: None,
    }
}

fn replica_receipt_scan_cursor(receipt: &DfsReplicaReceiptRecord) -> DfsReplicaReceiptScanCursor {
    DfsReplicaReceiptScanCursor {
        object_id: receipt.receipt.object_id.clone(),
        version: receipt.receipt.committed_version.clone(),
        replica_node_id: receipt.replica_node_id.clone(),
    }
}

fn replica_receipt_scan_key(
    receipt: &DfsReplicaReceiptRecord,
) -> (DfsObjectId, DfsVersion, NodeId) {
    (
        receipt.receipt.object_id.clone(),
        receipt.receipt.committed_version.clone(),
        receipt.replica_node_id.clone(),
    )
}

fn replica_receipt_page_cursor_with_shard(
    mut cursor: DfsReplicaReceiptPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsReplicaReceiptPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

fn local_replica_receipt_page_cursor(
    cursor: &DfsReplicaReceiptPageCursor,
) -> DfsReplicaReceiptPageCursor {
    DfsReplicaReceiptPageCursor {
        replica_node_id: cursor.replica_node_id.clone(),
        shard_id: None,
    }
}

fn receipt_for_replica_from_backend<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    replica_node_id: &NodeId,
) -> Result<Option<DfsReplicaReceiptRecord>> {
    let mut cursor = None;
    loop {
        let page = backend.receipts_for_version_page(
            object_id,
            version,
            cursor.as_ref(),
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        if let Some(receipt) = page
            .receipts
            .into_iter()
            .find(|receipt| &receipt.replica_node_id == replica_node_id)
        {
            return Ok(Some(receipt));
        }
        let Some(next_cursor) = page.next_cursor else {
            return Ok(None);
        };
        cursor = Some(next_cursor);
    }
}

fn write_intent_page_cursor(intent: &DfsWriteIntentRecord) -> DfsWriteIntentPageCursor {
    DfsWriteIntentPageCursor {
        intent_id: intent.intent_id.clone(),
        shard_id: None,
    }
}

fn conflict_scan_cursor(conflict: &DfsConflictRecord) -> DfsConflictScanCursor {
    DfsConflictScanCursor {
        object_id: conflict.object_id.clone(),
        conflict_id: conflict.conflict_id.clone(),
    }
}

fn conflict_scan_key(conflict: &DfsConflictRecord) -> (DfsObjectId, String) {
    (conflict.object_id.clone(), conflict.conflict_id.clone())
}

fn write_intent_scan_key(intent: &DfsWriteIntentRecord) -> (OffsetDateTime, String, DfsObjectId) {
    (
        intent.created_at,
        intent.intent_id.clone(),
        intent.object_id.clone(),
    )
}

fn intent_key(intent: &DfsWriteIntentRecord) -> (DfsObjectId, String) {
    (intent.object_id.clone(), intent.intent_id.clone())
}

fn write_intent_target_key(intent: &DfsWriteIntentRecord) -> (DfsObjectId, DfsVersion, String) {
    (
        intent.object_id.clone(),
        intent.target_version.clone(),
        intent.intent_id.clone(),
    )
}

fn write_intent_scan_cursor_key(
    cursor: &DfsWriteIntentScanCursor,
) -> (OffsetDateTime, String, DfsObjectId) {
    (
        cursor.created_at,
        cursor.intent_id.clone(),
        cursor.object_id.clone(),
    )
}

fn write_intent_scan_order(
    left: &DfsWriteIntentRecord,
    right: &DfsWriteIntentRecord,
) -> std::cmp::Ordering {
    write_intent_scan_key(left).cmp(&write_intent_scan_key(right))
}

fn write_intent_scan_cursor(intent: &DfsWriteIntentRecord) -> DfsWriteIntentScanCursor {
    DfsWriteIntentScanCursor {
        created_at: intent.created_at,
        intent_id: intent.intent_id.clone(),
        object_id: intent.object_id.clone(),
    }
}

fn write_intent_page_cursor_with_shard(
    mut cursor: DfsWriteIntentPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsWriteIntentPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

fn local_write_intent_page_cursor(cursor: &DfsWriteIntentPageCursor) -> DfsWriteIntentPageCursor {
    DfsWriteIntentPageCursor {
        intent_id: cursor.intent_id.clone(),
        shard_id: None,
    }
}

fn hardlink_object_from_path_record(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
    path_record: &DfsPathRecord,
) -> Option<DfsObjectMetadata> {
    if path_record.file_system_id != *file_system_id {
        return None;
    }
    let object = tables.objects.get(&path_record.object_id)?.clone();
    if object.kind == DfsObjectKind::Hardlink
        && object.file_system_id == *file_system_id
        && object.link_target_object_id.as_ref() == Some(target_object_id)
        && normalize_path(&object.path) == path_record.path
    {
        Some(object)
    } else {
        None
    }
}

fn object_page_cursor(object: &DfsObjectMetadata) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        shard_id: None,
    }
}

fn object_page_cursor_with_shard(
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        shard_id: Some(shard_id.clone()),
    }
}

fn local_object_page_cursor(cursor: &DfsObjectPageCursor) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: cursor.path.clone(),
        object_id: cursor.object_id.clone(),
        shard_id: None,
    }
}

fn directory_page_key(object: &DfsObjectMetadata) -> (&String, &DfsObjectId) {
    (&object.path, &object.object_id)
}

fn directory_page_cursor_with_dentry(
    object: &DfsObjectMetadata,
    dirfrag_id: DfsDirFragmentId,
    name_hash: u64,
    name: String,
) -> DfsDirectoryPageCursor {
    DfsDirectoryPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        dentry: Some(DfsDentryPageCursor {
            dirfrag_id,
            name_hash,
            name,
        }),
        shard_id: None,
    }
}

fn directory_page_cursor_with_shard(
    mut cursor: DfsDirectoryPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsDirectoryPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

fn local_directory_page_cursor(cursor: &DfsDirectoryPageCursor) -> DfsDirectoryPageCursor {
    DfsDirectoryPageCursor {
        path: cursor.path.clone(),
        object_id: cursor.object_id.clone(),
        dentry: cursor.dentry.clone(),
        shard_id: None,
    }
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

fn posix_path_is_at_or_under_prefix(path: &str, prefix: &str) -> bool {
    let path = normalize_posix_path(path);
    let prefix = normalize_posix_path(prefix);
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(&prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
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
    if state.snapshot_format_version.is_some_and(|version| {
        version < METADATA_TABLE_SNAPSHOT_FORMAT_VERSION && state.tables.has_materialized_state()
    }) {
        persist_metadata_table_snapshot(root, &state.tables)?;
        state.table_log_offset = 0;
        state.snapshot_format_version = Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION);
        return Ok(state);
    }
    if state.tables.has_materialized_state() {
        return Ok(state);
    }
    state.tables = load_legacy_metadata_tables(root)?;
    if state.tables.has_materialized_state() {
        persist_metadata_table_snapshot(root, &state.tables)?;
        state.table_log_offset = 0;
        state.snapshot_format_version = Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION);
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
    let snapshot: DfsMetadataTableSnapshot = read_sobj(&path)?;
    let format_version = snapshot.format_version;
    let tables = metadata_tables_from_table_snapshot(
        snapshot,
        &format!("DFS metadata table snapshot at {}", path.display()),
    )?;
    Ok(DfsMetadataTableState {
        tables,
        table_log_offset: 0,
        snapshot_format_version: Some(format_version),
    })
}

fn metadata_tables_from_table_snapshot(
    snapshot: DfsMetadataTableSnapshot,
    description: &str,
) -> Result<DfsMetadataTables> {
    match snapshot.format_version {
        LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION => {
            let mut tables = DfsMetadataTables::default();
            for op in snapshot.ops {
                tables.apply(op);
            }
            Ok(tables)
        }
        METADATA_TABLE_SNAPSHOT_FORMAT_VERSION => Ok(metadata_tables_from_snapshot(
            snapshot.tables.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "{description} version {} is missing structured tables",
                    snapshot.format_version
                ))
            })?,
        )),
        _ => Err(MetadataBackendError::Serde(format!(
            "unsupported {description} version {}",
            snapshot.format_version
        ))),
    }
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
    let existed = path.exists();
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
    if !existed && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(u64::try_from(record.len()).unwrap_or(u64::MAX))
}

fn persist_metadata_table_snapshot(root: &Path, tables: &DfsMetadataTables) -> Result<()> {
    write_sobj_atomic(
        &metadata_table_snapshot_path(root),
        &DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            tables: Some(metadata_tables_snapshot(tables)),
            ops: Vec::new(),
        },
    )?;
    truncate_metadata_table_log(root)?;
    Ok(())
}

fn metadata_tables_snapshot(tables: &DfsMetadataTables) -> DfsMetadataTableSnapshotTables {
    DfsMetadataTableSnapshotTables {
        file_systems: tables.file_systems.values().cloned().collect(),
        objects: tables.objects.values().cloned().collect(),
        file_system_objects: tables.file_system_objects.values().cloned().collect(),
        paths: tables.paths.values().cloned().collect(),
        inodes: tables.inodes.values().cloned().collect(),
        dirfrags: tables.dirfrags.values().cloned().collect(),
        dentries: tables.dentries.values().cloned().collect(),
        hardlinks: tables
            .hardlinks
            .iter()
            .map(|((target_object_id, hardlink_object_id), path_record)| {
                DfsHardlinkIndexSnapshotRecord {
                    target_object_id: target_object_id.clone(),
                    hardlink_object_id: hardlink_object_id.clone(),
                    path_record: path_record.clone(),
                }
            })
            .collect(),
        compacted_tombstones: tables.compacted_tombstones.values().cloned().collect(),
        versions: tables.versions.values().cloned().collect(),
        receipts: tables.receipts.values().cloned().collect(),
        intents: tables.intents.values().cloned().collect(),
        conflicts: tables.conflicts.values().cloned().collect(),
        visible_commits: tables
            .visible_commits
            .iter()
            .map(
                |((object_id, version), commit)| DfsVisibleCommitSnapshotRecord {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    commit: commit.clone(),
                },
            )
            .collect(),
        namespace_transactions: tables.namespace_transactions.values().cloned().collect(),
        namespace_jobs: tables.namespace_jobs.values().cloned().collect(),
    }
}

fn metadata_tables_from_snapshot(snapshot: DfsMetadataTableSnapshotTables) -> DfsMetadataTables {
    let mut tables = DfsMetadataTables::default();
    for file_system in snapshot.file_systems {
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
    }
    for object in snapshot.objects {
        tables.apply(DfsMetadataTableOp::PutObject { object });
    }
    for path_record in snapshot.file_system_objects {
        tables.apply(DfsMetadataTableOp::PutFileSystemObject { path_record });
    }
    for path_record in snapshot.paths {
        tables.apply(DfsMetadataTableOp::PutPath { path_record });
    }
    for inode in snapshot.inodes {
        tables.apply(DfsMetadataTableOp::PutInode { inode });
    }
    for dirfrag in snapshot.dirfrags {
        tables.apply(DfsMetadataTableOp::PutDirFragment { dirfrag });
    }
    for path_record in snapshot.dentries {
        tables.apply(DfsMetadataTableOp::PutDentry { path_record });
    }
    for hardlink in snapshot.hardlinks {
        tables.apply(DfsMetadataTableOp::PutHardlink {
            target_object_id: hardlink.target_object_id,
            hardlink_object_id: hardlink.hardlink_object_id,
            path_record: hardlink.path_record,
        });
    }
    for object in snapshot.compacted_tombstones {
        tables.apply(DfsMetadataTableOp::PutCompactedTombstone { object });
    }
    for version in snapshot.versions {
        tables.apply(DfsMetadataTableOp::PutVersion { version });
    }
    for receipt in snapshot.receipts {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in snapshot.intents {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
    }
    for conflict in snapshot.conflicts {
        tables.apply(DfsMetadataTableOp::PutConflict { conflict });
    }
    for visible_commit in snapshot.visible_commits {
        tables.apply(DfsMetadataTableOp::PutVisibleCommit {
            object_id: visible_commit.object_id,
            version: visible_commit.version,
            commit: visible_commit.commit,
        });
    }
    for transaction in snapshot.namespace_transactions {
        tables.apply(DfsMetadataTableOp::PutNamespaceTransaction { transaction });
    }
    for job in snapshot.namespace_jobs {
        tables.apply(DfsMetadataTableOp::PutNamespaceJob { job });
    }
    tables
}

fn archive_metadata_backend_state(root: &Path) -> Result<bool> {
    let mut archived = false;
    let archive_root = root.join("replica-repair-archives").join(format!(
        "{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let records = root.join("records");
    if records.exists() {
        if !archived {
            create_dir_all(archive_root.clone())?;
        }
        move_metadata_archive_path(&records, &archive_root.join("records"))?;
        archived = true;
    }
    let journal = root.join("journal");
    if journal.exists() {
        for entry in read_dir(&journal)? {
            let source = entry.path();
            if source.file_name().and_then(|name| name.to_str()) == Some("write.lock") {
                continue;
            }
            if !archived {
                create_dir_all(archive_root.clone())?;
            }
            let destination = archive_root.join("journal").join(entry.file_name());
            move_metadata_archive_path(&source, &destination)?;
            archived = true;
        }
    }
    if archived {
        sync_dir(root)?;
    }
    Ok(archived)
}

fn move_metadata_archive_path(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        create_dir_all(parent.to_owned())?;
    }
    fs::rename(source, destination).map_err(|source_error| MetadataBackendError::Io {
        path: source.to_path_buf(),
        source: source_error,
    })
}

#[cfg(test)]
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
            .compacted_tombstones
            .values()
            .cloned()
            .map(|object| DfsMetadataTableOp::PutCompactedTombstone { object }),
    );
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
    ops.extend(
        tables
            .namespace_jobs
            .values()
            .cloned()
            .map(|job| DfsMetadataTableOp::PutNamespaceJob { job }),
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
    for file_system in read_sobj_tree::<DfsFileSystem>(&records.join("filesystems"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
    }
    for object in read_sobj_tree::<DfsObjectMetadata>(&records.join("objects"))? {
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
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("filesystem-objects"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystemObject { path_record });
    }
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("paths"))? {
        tables.apply(DfsMetadataTableOp::PutPath { path_record });
    }
    for inode in read_sobj_tree::<DfsInodeRecord>(&records.join("inodes"))? {
        tables.apply(DfsMetadataTableOp::PutInode { inode });
    }
    for dirfrag in read_sobj_tree::<DfsDirFragmentRecord>(&records.join("dirfrags"))? {
        tables.apply(DfsMetadataTableOp::PutDirFragment { dirfrag });
    }
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("dentries"))? {
        tables.apply(DfsMetadataTableOp::PutDentry { path_record });
    }
    for version in read_sobj_tree::<DfsObjectVersionRecord>(&records.join("versions"))? {
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(&version),
        });
    }
    for receipt in read_sobj_tree::<DfsReplicaReceiptRecord>(&records.join("receipts"))? {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in read_sobj_tree::<DfsWriteIntentRecord>(&records.join("intents"))? {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
    }
    for conflict in read_sobj_tree::<DfsConflictRecord>(&records.join("conflicts"))? {
        tables.apply(DfsMetadataTableOp::PutConflict { conflict });
    }
    for commit in read_sobj_tree::<DfsVisibleVersionCommitRecord>(&records.join("visible-commits"))?
    {
        tables.apply(DfsMetadataTableOp::PutVisibleCommit {
            object_id: commit.committed_object.object_id.clone(),
            version: commit.committed_object.version.clone(),
            commit,
        });
    }
    for transaction in
        read_sobj_tree::<DfsNamespaceTransaction>(&records.join("namespace-transactions"))?
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
        read_sobj_optional::<DfsMetadataJournalHead>(&journal_dir.join("head.sobj"))?
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
        read_sobj_optional::<DfsMetadataJournalHead>(&head_path)?.map(|head| head.next_sequence);
    let mut max_sequence = head_next_sequence
        .map(|next_sequence| next_sequence.saturating_sub(1))
        .unwrap_or(0);

    max_sequence = max_sequence.max(max_segmented_journal_sequence(root)?);
    for (sequence, _, _) in legacy_journal_entry_paths_since(root, 0)? {
        max_sequence = max_sequence.max(sequence);
    }

    let next_sequence = max_sequence.saturating_add(1).max(1);
    if head_next_sequence != Some(next_sequence) {
        write_sobj_atomic(&head_path, &DfsMetadataJournalHead { next_sequence })?;
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
        read_sobj_optional::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))?
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
    let existed = path.exists();
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
    if !existed && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn append_segmented_journal_entries(
    root: &Path,
    entries: &[&DfsMetadataJournalEntry],
) -> Result<()> {
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let segment_id = journal_segment_id(first.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    let existed = path.exists();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut records = Vec::new();
    for entry in entries {
        if journal_segment_id(entry.sequence) != segment_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal batch tried to append entry {} to segment {segment_id}",
                entry.sequence
            )));
        }
        let bytes = sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
        let length = u32::try_from(bytes.len()).map_err(|_| {
            MetadataBackendError::Serde(format!(
                "metadata journal entry {} is too large: {} bytes",
                entry.sequence,
                bytes.len()
            ))
        })?;
        records.extend_from_slice(&length.to_le_bytes());
        records.extend_from_slice(&bytes);
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    file.write_all(&records)
        .and_then(|()| file.sync_all())
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    drop(file);
    if !existed && let Some(parent) = path.parent() {
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
        let entry: DfsMetadataJournalEntry = read_sobj(&path)?;
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

fn duration_nanos_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn metadata_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(0)
}

fn object_timestamp_for_retention(object: &DfsObjectMetadata) -> u64 {
    [
        object.changed_at_unix_nanos,
        object.modified_at_unix_nanos,
        object.created_at_unix_nanos,
        object.accessed_at_unix_nanos,
    ]
    .into_iter()
    .find(|value| *value > 0)
    .unwrap_or(0)
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

fn read_sobj<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    sinomo_object_serde::from_bytes(&read_file(path)?)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

fn read_sobj_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }
    read_sobj(path).map(Some)
}

fn read_sobj_tree<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    read_sobj_tree_into(path, &mut records)?;
    Ok(records)
}

fn read_sobj_tree_into<T>(path: &Path, records: &mut Vec<T>) -> Result<()>
where
    T: DeserializeOwned,
{
    for entry in read_dir(path)? {
        let path = entry.path();
        if path.is_dir() {
            read_sobj_tree_into(&path, records)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sobj") {
            records.push(read_sobj(&path)?);
        }
    }
    Ok(())
}

fn write_sobj_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    write_sobj_atomic_if_changed(path, value).map(|_| ())
}

fn write_sobj_atomic_if_changed<T>(path: &Path, value: &T) -> Result<bool>
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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    fn namespace_job(index: usize, root_path: &str) -> DfsNamespaceJobRecord {
        let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
        DfsNamespaceJobRecord {
            job_id: format!("job_namespace_page_{index:02}"),
            file_system_id: file_system_id(),
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: root_path.to_string(),
            target_path: None,
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: timestamp,
            updated_at: timestamp,
            completed_at: None,
            failure_reason: None,
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
        backend: &LocalSobjMetadataBackend,
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
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> bool {
        backend
            .tables
            .paths
            .contains_key(&(file_system_id.clone(), normalize_path(path)))
    }

    fn table_dentry_exists(
        backend: &LocalSobjMetadataBackend,
        path_record: &DfsPathRecord,
    ) -> bool {
        backend
            .tables
            .dentries
            .contains_key(&dentry_key(path_record))
    }

    fn table_dirfrag(
        backend: &LocalSobjMetadataBackend,
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
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .file_system_objects
            .contains_key(&(file_system_id.clone(), object_id.clone()))
    }

    fn table_hardlink_exists(
        backend: &LocalSobjMetadataBackend,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .hardlinks
            .contains_key(&(target_object_id.clone(), hardlink_object_id.clone()))
    }

    fn force_table_replay_from_journal(
        backend: &LocalSobjMetadataBackend,
        root: &Path,
        last_materialized_sequence: u64,
    ) {
        remove_file_if_exists(&metadata_table_path(root)).expect("remove metadata table log");
        remove_file_if_exists(&metadata_table_snapshot_path(root))
            .expect("remove metadata table snapshot");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: backend.shard_id.clone(),
                last_sequence: last_materialized_sequence,
            },
        )
        .expect("rewind materialization cursor");
    }

    fn metadata_table_batch_count(root: &Path) -> usize {
        use std::io::Read as _;

        let path = metadata_table_path(root);
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return 0,
            Err(source) => panic!("open metadata table log {}: {source}", path.display()),
        };
        let mut count = 0usize;
        loop {
            let mut length_bytes = [0_u8; 4];
            match file.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(source) => panic!("read metadata table length {}: {source}", path.display()),
            }
            let length = u32::from_le_bytes(length_bytes) as usize;
            let mut bytes = vec![0_u8; length];
            file.read_exact(&mut bytes).unwrap_or_else(|source| {
                panic!("read metadata table batch {}: {source}", path.display())
            });
            let _: DfsMetadataTableBatch =
                sinomo_object_serde::from_bytes(&bytes).unwrap_or_else(|source| {
                    panic!("decode metadata table batch {}: {source}", path.display())
                });
            count = count.saturating_add(1);
        }
        count
    }

    fn block_shard_journal_writes(root: &Path, shard_id: &str) {
        let segments = root
            .join(key_segment(&shard_id))
            .join("journal")
            .join("segments");
        fs::remove_dir_all(&segments).expect("remove journal segments directory");
        fs::write(&segments, b"not a directory").expect("block journal segment writes");
    }

    #[test]
    fn local_backend_persists_file_system_object_path_and_journal() {
        let root = temp_root("local_backend_persists_file_system_object_path_and_journal");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);

        cleanup(&root);
    }

    #[test]
    fn local_backend_batches_object_metadata_into_one_table_batch() {
        let root = temp_root("local_backend_batches_object_metadata_into_one_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let objects = (0..3)
            .map(|index| {
                let mut object = object(version(&format!("version_batch_{index}")));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_batch_{index}")).expect("object");
                object.path = format!("docs/file-{index}.md");
                object
            })
            .collect::<Vec<_>>();
        let entries = backend.put_objects(&objects).expect("object batch");

        assert_eq!(entries.len(), objects.len());
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            backend.journal_since(1).expect("journal").len(),
            objects.len()
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            2,
            "filesystem write should use one table batch and the object group should use one more"
        );
        for object in &objects {
            assert_eq!(
                backend.get_object(&object.object_id).expect("get object"),
                Some(object.clone())
            );
        }

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .count_objects_for_file_system(&file_system.file_system_id)
                .expect("count objects"),
            objects.len()
        );

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
    fn sharded_backend_pages_objects_and_directory_children_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_with_shard_cursors");
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
        backend.put_file_system(&file_system).expect("filesystem");

        let mut root_file = object(version("version_1"));
        root_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_root_file").expect("root file");
        root_file.path = "/root.txt".to_string();

        let mut users_dir = object(version("version_1"));
        users_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_users").expect("users dir");
        users_dir.path = "/users".to_string();
        users_dir.kind = DfsObjectKind::Directory;
        users_dir.content_hash = None;
        users_dir.content_size_bytes = None;

        let mut services_dir = object(version("version_1"));
        services_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_services").expect("services dir");
        services_dir.path = "/services".to_string();
        services_dir.kind = DfsObjectKind::Directory;
        services_dir.content_hash = None;
        services_dir.content_size_bytes = None;

        let mut service_file = object(version("version_1"));
        service_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_service_file").expect("service file");
        service_file.path = "/services/app.txt".to_string();

        for object in [&root_file, &users_dir, &services_dir, &service_file] {
            backend.put_object(object).expect("object");
        }

        let mut object_cursor = None;
        let mut paged_object_ids = BTreeSet::new();
        let mut saw_object_shard_cursor = false;
        loop {
            let page = backend
                .objects_for_file_system_page(
                    &file_system.file_system_id,
                    object_cursor.as_ref(),
                    2,
                )
                .expect("object page");
            paged_object_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_object_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        let full_object_ids = backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("full objects")
            .into_iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(paged_object_ids, full_object_ids);
        assert!(
            saw_object_shard_cursor,
            "sharded object pages should carry shard cursors for bounded continuation"
        );

        let mut dir_cursor = None;
        let mut root_child_names = Vec::new();
        let mut saw_directory_shard_cursor = false;
        loop {
            let page = backend
                .child_objects_for_directory_page(
                    &file_system.file_system_id,
                    "/",
                    dir_cursor.as_ref(),
                    1,
                )
                .expect("directory page");
            root_child_names.extend(page.objects.into_iter().map(|object| {
                object
                    .path
                    .rsplit('/')
                    .next()
                    .expect("child name")
                    .to_string()
            }));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_directory_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            dir_cursor = Some(next_cursor);
        }
        root_child_names.sort();
        assert_eq!(root_child_names, vec!["root.txt", "services", "users"]);
        assert!(
            saw_directory_shard_cursor,
            "sharded directory pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_namespace_jobs() {
        let root = temp_root("sharded_backend_pages_namespace_jobs");
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

        for index in 0..6 {
            let root_path = if index % 2 == 0 {
                format!("/jobs/{index}")
            } else {
                format!("/services/jobs/{index}")
            };
            backend
                .put_namespace_job(&namespace_job(index, &root_path))
                .expect("namespace job");
        }

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..6)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(
            backend
                .get_namespace_job("job_namespace_page_05")
                .expect("get job")
                .expect("job")
                .root_path,
            "/services/jobs/5"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_gets_namespace_transaction_by_shard_transaction_id() {
        let root = temp_root("sharded_backend_get_namespace_transaction");
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
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_1"));
        object.path = "/services/namespace-transaction.txt".to_string();

        let entry = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("namespace transaction");
        let transaction_id = match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
                transaction_id.clone()
            }
            other => panic!("expected namespace transaction journal entry, got {other:?}"),
        };

        let recovered = backend
            .get_namespace_transaction(&transaction_id)
            .expect("lookup transaction")
            .expect("transaction");
        assert_eq!(recovered.transaction_id, transaction_id);
        assert_eq!(
            recovered.objects.first().map(|object| &object.object_id),
            Some(&object.object_id)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_object_local_records_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_object_local_records");
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
        backend.put_file_system(&file_system).expect("filesystem");

        let mut file = object(version("version_0"));
        file.path = "/services/readme.md".to_string();
        backend.put_object(&file).expect("object");

        for index in 0..3 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let first_version_page = backend
            .versions_for_object_page(&file.object_id, None, 1)
            .expect("first version page");
        assert_eq!(first_version_page.versions.len(), 1);
        assert_eq!(
            first_version_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_version_scan = backend
            .version_records_page(None, 2)
            .expect("first version scan page");
        assert_eq!(first_version_scan.versions.len(), 2);
        assert!(first_version_scan.next_cursor.is_some());
        let second_version_scan = backend
            .version_records_page(first_version_scan.next_cursor.as_ref(), 2)
            .expect("second version scan page");
        assert_eq!(second_version_scan.versions.len(), 1);
        assert!(second_version_scan.next_cursor.is_none());
        let scanned_versions = first_version_scan
            .versions
            .into_iter()
            .chain(second_version_scan.versions)
            .map(|version_record| version_record.version)
            .collect::<Vec<_>>();
        let mut expected_versions = (0..3)
            .map(|index| version(&format!("version_{index}")))
            .collect::<Vec<_>>();
        expected_versions.sort();
        assert_eq!(scanned_versions, expected_versions);

        for index in 0..3 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }
        let first_receipt_page = backend
            .receipts_for_version_page(&file.object_id, &version("version_0"), None, 1)
            .expect("first receipt page");
        assert_eq!(first_receipt_page.receipts.len(), 1);
        assert_eq!(
            first_receipt_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_receipt_scan = backend
            .receipt_records_page(None, 2)
            .expect("first receipt scan page");
        assert_eq!(first_receipt_scan.receipts.len(), 2);
        assert!(first_receipt_scan.next_cursor.is_some());
        let second_receipt_scan = backend
            .receipt_records_page(first_receipt_scan.next_cursor.as_ref(), 2)
            .expect("second receipt scan page");
        assert_eq!(second_receipt_scan.receipts.len(), 1);
        assert!(second_receipt_scan.next_cursor.is_none());
        let scanned_receipts = first_receipt_scan
            .receipts
            .into_iter()
            .chain(second_receipt_scan.receipts)
            .map(|receipt| receipt.replica_node_id)
            .collect::<Vec<_>>();
        let mut expected_receipts = (0..3)
            .map(|index| node(&format!("node_replica_{index}")))
            .collect::<Vec<_>>();
        expected_receipts.sort();
        assert_eq!(scanned_receipts, expected_receipts);

        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }
        let first_intent_page = backend
            .intents_for_object_page(&file.object_id, None, 1)
            .expect("first intent page");
        assert_eq!(first_intent_page.intents.len(), 1);
        assert_eq!(
            first_intent_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_scan_page = backend
            .write_intents_page(None, 2)
            .expect("first write intent scan page");
        assert_eq!(first_scan_page.intents.len(), 2);
        assert!(first_scan_page.next_cursor.is_some());
        let second_scan_page = backend
            .write_intents_page(first_scan_page.next_cursor.as_ref(), 2)
            .expect("second write intent scan page");
        assert_eq!(second_scan_page.intents.len(), 1);
        assert!(second_scan_page.next_cursor.is_none());
        let scanned_intent_ids = first_scan_page
            .intents
            .into_iter()
            .chain(second_scan_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(scanned_intent_ids, vec!["intent-0", "intent-1", "intent-2"]);

        let target_version = version("version_shared_target");
        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-shared-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: target_version.clone(),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:shared-target-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("shared target intent");
        }
        let first_target_page = backend
            .intents_for_object_version_page(&file.object_id, &target_version, None, 2)
            .expect("first target intent page");
        assert_eq!(first_target_page.intents.len(), 2);
        assert_eq!(
            first_target_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let second_target_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &target_version,
                first_target_page.next_cursor.as_ref(),
                2,
            )
            .expect("second target intent page");
        assert_eq!(second_target_page.intents.len(), 1);
        assert!(second_target_page.next_cursor.is_none());
        let shared_target_ids = first_target_page
            .intents
            .into_iter()
            .chain(second_target_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shared_target_ids,
            vec![
                "intent-shared-target-0".to_string(),
                "intent-shared-target-1".to_string(),
                "intent-shared-target-2".to_string()
            ]
        );

        let mut transient_object = file.clone();
        transient_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_transient_intent")
            .expect("transient object id");
        transient_object.path = "/services/transient-create.txt".to_string();
        transient_object.version = version("version_transient_1");
        let transient_intent = DfsWriteIntentRecord {
            intent_id: "intent-transient-create".to_string(),
            file_system_id: transient_object.file_system_id.clone(),
            object_id: transient_object.object_id.clone(),
            base_version: None,
            target_version: version("version_transient_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: Some("blake3:transient".to_string()),
            object_metadata: Some(transient_object.clone()),
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent_for_object(Some(&transient_object), &transient_intent)
            .expect("transient intent");
        assert_eq!(
            backend
                .intents_for_object_version_page(
                    &transient_object.object_id,
                    &transient_intent.target_version,
                    None,
                    1,
                )
                .expect("transient target intent page")
                .intents
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            vec!["intent-transient-create"]
        );

        let mut moved_object = object(version("version_1"));
        moved_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_stale_intent_source")
            .expect("stale intent object id");
        moved_object.path = "/stale-intent-source.txt".to_string();
        backend.put_object(&moved_object).expect("root object");
        let stale_source_intent = DfsWriteIntentRecord {
            intent_id: "intent-stale-source".to_string(),
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            base_version: Some(moved_object.version.clone()),
            target_version: version("version_stale_source_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:stale-source".to_string()),
            object_metadata: Some(moved_object.clone()),
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent(&stale_source_intent)
            .expect("source shard intent");
        let mut moved_target_object = moved_object.clone();
        moved_target_object.path = "/services/stale-intent-source.txt".to_string();
        moved_target_object.version = version("version_stale_source_3");
        backend
            .put_object(&moved_target_object)
            .expect("move object to services");
        assert!(
            backend
                .intents_for_object_version_page(
                    &moved_object.object_id,
                    &stale_source_intent.target_version,
                    None,
                    1,
                )
                .expect("stale source target intent page")
                .intents
                .is_empty(),
            "target-version lookups should hide source-shard intents after the object index moves"
        );

        let mut scan_cursor = None;
        let mut visible_scan_ids = BTreeSet::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("visible intent scan page");
            assert!(page.intents.len() <= 2);
            visible_scan_ids.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert!(
            visible_scan_ids.contains("intent-transient-create"),
            "transient create intents should remain visible in global scans"
        );
        assert!(
            !visible_scan_ids.contains("intent-stale-source"),
            "global scans should hide source-shard intents after the object index moves"
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<BTreeSet<_>>(),
            visible_scan_ids
        );

        for index in 0..3 {
            let conflict = DfsConflictRecord {
                conflict_id: format!("conflict-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                path: file.path.clone(),
                base_version: Some(version("version_0")),
                local_version: version(&format!("version_local_{index}")),
                remote_version: version(&format!("version_remote_{index}")),
                detected_by_node_id: node("node_owner"),
                reason: format!("conflict {index}"),
                local_content_hash: Some(format!("blake3:local-{index}")),
                remote_content_hash: Some(format!("blake3:remote-{index}")),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_conflict(&conflict).expect("conflict");
        }
        let first_conflict_scan = backend
            .conflict_records_page(None, 2)
            .expect("first conflict scan page");
        assert_eq!(first_conflict_scan.conflicts.len(), 2);
        assert!(first_conflict_scan.next_cursor.is_some());
        let second_conflict_scan = backend
            .conflict_records_page(first_conflict_scan.next_cursor.as_ref(), 2)
            .expect("second conflict scan page");
        assert_eq!(second_conflict_scan.conflicts.len(), 1);
        assert!(second_conflict_scan.next_cursor.is_none());
        let scanned_conflicts = first_conflict_scan
            .conflicts
            .into_iter()
            .chain(second_conflict_scan.conflicts)
            .map(|conflict| conflict.conflict_id)
            .collect::<Vec<_>>();
        assert_eq!(
            scanned_conflicts,
            vec!["conflict-0", "conflict-1", "conflict-2"]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_and_counts_hardlinks_across_shards() {
        let root = temp_root("sharded_backend_pages_hardlinks");
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
        backend.put_file_system(&file_system).expect("filesystem");

        let target = object(version("version_1"));
        let mut root_alias = object(version("version_1"));
        root_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_root_alias").expect("root alias");
        root_alias.path = "/z-root.alias".to_string();
        root_alias.kind = DfsObjectKind::Hardlink;
        root_alias.content_hash = None;
        root_alias.link_target_object_id = Some(target.object_id.clone());
        let mut service_alias = root_alias.clone();
        service_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_service_alias").expect("service alias");
        service_alias.path = "/services/a-service.alias".to_string();

        for object in [&target, &root_alias, &service_alias] {
            backend.put_object(object).expect("object");
        }

        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system.file_system_id, &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system.file_system_id, &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/services/a-service.alias"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system.file_system_id,
                    &target.object_id,
                    "/services",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/z-root.alias"
        );

        let mut cursor = None;
        let mut paged_ids = BTreeSet::new();
        let mut saw_hardlink_shard_cursor = false;
        loop {
            let page = backend
                .hardlinks_to_object_page(
                    &file_system.file_system_id,
                    &target.object_id,
                    cursor.as_ref(),
                    1,
                )
                .expect("hardlink page");
            paged_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_hardlink_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_ids,
            [root_alias.object_id, service_alias.object_id]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert!(
            saw_hardlink_shard_cursor,
            "sharded hardlink pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_object_when_cross_shard_target_write_fails() {
        let root = temp_root("sharded_backend_cross_shard_target_failure");
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
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("target shard journal failure should reject move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target write must not tombstone or move the source object"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target write must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_namespace_when_cross_shard_target_transaction_fails() {
        let root = temp_root("sharded_backend_cross_shard_transaction_failure");
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
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![moved],
                versions: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect_err("target shard journal failure should reject namespace move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target namespace journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target namespace transaction must keep the source object visible"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target namespace transaction must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_hides_stale_source_path_when_cross_shard_tombstone_fails() {
        let root = temp_root("sharded_backend_cross_shard_tombstone_failure");
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
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "root");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("source tombstone journal failure should be surfaced");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "source tombstone journal failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object by id")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version)),
            "object id lookups should use the target shard after the target write and index update"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("old source path"),
            None,
            "a stale non-tombstoned source object must not remain visible once the index points at the target shard"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &moved.path)
                .expect("target path")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_migrates_existing_root_object_to_new_path_shard() {
        let root = temp_root("sharded_backend_migrates_existing_root_object");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        object.content_hash = Some("blake3:migrated".to_string());
        object.content_size_bytes = Some(8);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_migrate_receipt").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: object.content_hash.clone(),
            object_metadata: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version: Some(object.version.clone()),
            local_version: version("version_local"),
            remote_version: version("version_remote"),
            detected_by_node_id: node("node_owner"),
            reason: "migration coverage".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: Some(intent.intent_id.clone()),
            strategy: Some("manual".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
            root_only.put_version(&version_record).expect("version");
            root_only.put_receipt(&receipt).expect("receipt");
            root_only.put_intent(&intent).expect("intent");
            root_only.put_conflict(&conflict).expect("conflict");
        }

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
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migrate objects");

        assert_eq!(
            report,
            DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 1,
                indexed_objects: 0,
                migrated_objects: 1,
                skipped_stale_sources: 0,
                reached_limit: false,
            }
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("migrated path"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("migrated version"),
            Some(version_record.clone())
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("migrated receipts"),
            vec![receipt.clone()]
        );
        assert_eq!(
            backend
                .get_intent(&object.object_id, &intent.intent_id)
                .expect("migrated intent"),
            Some(intent)
        );
        assert_eq!(
            backend
                .get_conflict(&object.object_id, &conflict.conflict_id)
                .expect("migrated conflict"),
            Some(conflict)
        );
        assert_eq!(
            backend
                .shard_backend(&DfsMetadataShardId::new("root"))
                .expect("root shard")
                .get_object(&object.object_id)
                .expect("root object")
                .as_ref()
                .map(|object| object.kind.clone()),
            Some(DfsObjectKind::Tombstone),
            "source shard should retain an explicit tombstone for path-history replay"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&object.path),
                &DfsMetadataShardId::new("services")
            ))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_bounds_configured_shard_migration() {
        let root = temp_root("sharded_backend_bounds_configured_shard_migration");
        let file_system = file_system();
        let mut first = object(version("version_1"));
        first.path = "/services/a.txt".to_string();
        let mut second = object(version("version_1"));
        second.object_id = DfsObjectId::from_legacy_named("obj_second").expect("object");
        second.path = "/users/b.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&first).expect("first root object");
            root_only.put_object(&second).expect("second root object");
        }

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
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
            .expect("bounded migration");

        assert_eq!(
            report.scanned_objects, 1,
            "bounded migration should not scan the rest of the shard after reaching the migration limit"
        );
        assert_eq!(report.migrated_objects, 1);
        assert!(report.reached_limit);
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .filter(|object| object.kind != DfsObjectKind::Tombstone)
                .count(),
            2,
            "bounded migration must keep both live objects visible through the sharded object index"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_falls_back_to_source_path_during_shard_activation() {
        let root = temp_root("sharded_backend_activation_path_fallback");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/pre-migration.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
        }

        let backend = LocalShardedMetadataBackend::new(
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

        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("activation fallback path"),
            Some(object),
            "publishing a new path-prefix shard before migration must not make existing root-shard paths disappear"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_ignores_index_for_unconfigured_local_shard() {
        let root = temp_root("sharded_backend_unconfigured_index");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/indexed-before-kv.txt".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        backend
            .write_object_shard_index(&object, &DfsMetadataShardId::new("services"))
            .expect("stale object shard index");

        assert_eq!(
            backend.get_object(&object.object_id).expect("object by id"),
            Some(object.clone()),
            "an index that names an unopened shard must not break object-id reads"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("object by path"),
            Some(object),
            "an index that names an unopened shard must not hide source-shard path reads"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_does_not_materialize_object_when_journal_append_fails() {
        let root = temp_root("local_backend_journal_failure_does_not_materialize_object");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone()),
            "reopen should replay journaled metadata that was not yet materialized"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
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
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let entry = backend.put_object(&object).expect("object");
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (entry.sequence, table_len)
        };
        write_sobj_atomic(
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

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object),
            "stale cursor recovery must preserve the materialized object"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
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
    fn local_backend_checkpoints_materialization_cursor_instead_of_every_write() {
        let root = temp_root("local_backend_deferred_materialization_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut expected_objects = Vec::new();
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let mut last_sequence = 0;
            for index in 0..8 {
                let mut object = object(version("version_1"));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_cursor_defer_{index}"))
                        .expect("object id");
                object.path = format!("/services/cursor-defer/{index}.txt");
                let entry = backend.put_object(&object).expect("object");
                last_sequence = entry.sequence;
                expected_objects.push(object);
            }
            let cursor =
                read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
                    .expect("materialization cursor");
            assert_eq!(
                cursor.last_sequence, 0,
                "short local write bursts should not force a cursor fsync for every metadata mutation"
            );
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (last_sequence, table_len)
        };

        let reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen backend");

        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object),
                "reopen should recover objects whose materialization cursor was not checkpointed"
            );
        }
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence,
            "reopen recovery should advance the cursor after idempotent replay"
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent replay from a deferred cursor must not duplicate materialized table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_open_does_not_rewrite_existing_metadata_table() {
        let root = temp_root("local_backend_open_preserves_metadata_table");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let table_path = metadata_table_path(&root);
        let before_len = fs::metadata(&table_path).expect("metadata table").len();

        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("metadata table snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "new compactions should use structured metadata table snapshots"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert_eq!(
            fs::metadata(&table_path)
                .expect("truncated metadata table")
                .len(),
            0,
            "compaction should truncate historical table ops after the snapshot is durable"
        );
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
    fn local_backend_migrates_legacy_op_snapshot_to_structured_snapshot() {
        let root = temp_root("local_backend_migrates_legacy_op_snapshot");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object).expect("object");
            write_sobj_atomic(
                &metadata_table_snapshot_path(&root),
                &DfsMetadataTableSnapshot {
                    format_version: LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                    tables: None,
                    ops: metadata_table_snapshot_ops(&backend.tables),
                },
            )
            .expect("legacy metadata table snapshot");
            truncate_metadata_table_log(&root).expect("truncate table log");
        }

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object)
        );
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("migrated snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "opening a legacy op snapshot should migrate it to the structured snapshot format"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            0,
            "snapshot migration should compact any table tail into the structured image"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_allocates_journal_sequences_across_open_handles() {
        let root = temp_root("local_backend_allocates_journal_sequences_across_open_handles");
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let mut second =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let second = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
    fn local_backend_readers_never_observe_torn_metadata_during_writes() {
        let root = temp_root("local_backend_readers_during_writes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let total_writes = 256usize;
        let reader_count = 4usize;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Start readers and the writer together so the reads genuinely overlap the
        // appends rather than racing thread startup.
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(reader_count + 1));

        // Readers use their own backend handle and read continuously while a writer
        // appends. The read path (`current_tables_snapshot`) is lock-free, so this
        // exercises its torn-tail safety: every read must succeed and never observe
        // more objects than the writer has committed.
        let mut readers = Vec::new();
        for _ in 0..reader_count {
            let root = root.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            readers.push(std::thread::spawn(move || {
                let backend =
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("reader backend");
                barrier.wait();
                // Read at least once, then keep reading until the writer signals stop.
                let mut reads = 0u64;
                loop {
                    let objects = backend
                        .objects_for_file_system(&file_system_id())
                        .expect("read must never fail on a torn tail");
                    assert!(
                        objects.len() <= total_writes,
                        "reader saw {} objects, more than the {total_writes} ever written",
                        objects.len()
                    );
                    reads += 1;
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
                reads
            }));
        }

        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        barrier.wait();
        for i in 0..total_writes {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_reader_race_{i}")).expect("object id");
            object.path = format!("/docs/reader-race-{i}.md");
            writer.put_object(&object).expect("write object");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        for reader in readers {
            let reads = reader.join().expect("reader thread");
            assert!(reads > 0, "reader must have completed at least one read");
        }

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("final objects")
                .len(),
            total_writes,
            "every committed write must be visible after the race"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_appends_journal_entries_to_segment_files() {
        let root = temp_root("local_backend_appends_journal_entries_to_segment_files");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
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
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead {
                next_sequence: high_sequence + 1,
            },
        )
        .expect("journal head");
        write_sobj_atomic(
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

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
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
        write_sobj_atomic(&legacy_path, &legacy_entry).expect("legacy journal entry");

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut committed_object = object(version("version_2"));
        committed_object.content_hash = Some("blake3:new".to_string());
        committed_object.content_size_bytes = Some(128);
        let visible_version = DfsObjectVersionRecord {
            file_system_id: file_system_id(),
            object_id: object_id(),
            version: version("version_2"),
            content_hash: committed_object.content_hash.clone(),
            size_bytes: committed_object.content_size_bytes,
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend
            .commit_visible_version(
                &version("version_1"),
                &committed_object,
                &visible_version,
                &[],
            )
            .expect("commit visible version");
        let mut transaction_object = object(version("version_namespace_tx"));
        transaction_object.object_id =
            DfsObjectId::from_legacy_named("obj_hydrated_namespace_tx").expect("object");
        transaction_object.path = "docs/transaction.md".to_string();
        transaction_object.content_hash = None;
        transaction_object.content_size_bytes = None;
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_tx_hydrate_legacy".to_string(),
            file_system_id: file_system_id(),
            objects: vec![transaction_object],
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("namespace transaction");

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
                    DfsMetadataMutation::CommitVisibleVersion {
                        committed_object,
                        visible_version,
                        ..
                    } => {
                        *committed_object = None;
                        *visible_version = None;
                    }
                    DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                        *transaction = None;
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
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object: Some(object),
                visible_version: Some(version_record),
                ..
            } if object.version == version("version_2")
                && version_record.version == version("version_2")
        )));
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } if transaction.transaction_id == "namespace_tx_hydrate_legacy"
        )));

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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut root_dir = object(version("version_1"));
        root_dir.object_id = DfsObjectId::from_legacy_named("obj_root_dir").expect("object");
        root_dir.path = "/".to_string();
        root_dir.kind = DfsObjectKind::Directory;
        root_dir.content_hash = None;
        root_dir.content_size_bytes = None;
        let mut docs_dir = object(version("version_1"));
        docs_dir.object_id = DfsObjectId::from_legacy_named("obj_docs_dir").expect("object");
        docs_dir.path = "/docs".to_string();
        docs_dir.kind = DfsObjectKind::Directory;
        docs_dir.content_hash = None;
        docs_dir.content_size_bytes = None;
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
        for object in [&root_dir, &docs_dir, &readme, &changelog, &nested, &outside] {
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

        let root_children = backend
            .child_objects_for_directory(&file_system_id(), "/")
            .expect("root children");
        assert_eq!(
            root_children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        let root_page = backend
            .child_objects_for_directory_page(&file_system_id(), "/", None, 10)
            .expect("root page");
        assert_eq!(
            root_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_routes_new_large_directory_entries_to_hash_fragments() {
        let root = temp_root("local_backend_routes_new_large_directory_entries_to_hash_fragments");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
    fn local_backend_pages_filesystem_objects_and_directory_children() {
        let root = temp_root("local_backend_pages_objects_and_children");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut dir = object(version("version_1"));
        dir.object_id = DfsObjectId::from_legacy_named("obj_page_dir").expect("dir object");
        dir.path = "/paged".to_string();
        dir.kind = DfsObjectKind::Directory;
        dir.content_hash = None;
        dir.content_size_bytes = None;
        backend.put_object(&dir).expect("dir");

        for index in 0..5 {
            let mut file = object(version("version_1"));
            file.object_id =
                DfsObjectId::from_legacy_named(format!("obj_page_file_{index}")).expect("object");
            file.path = format!("/paged/file-{index}.txt");
            file.kind = DfsObjectKind::File;
            backend.put_object(&file).expect("file");
        }

        let first_object_page = backend
            .objects_for_file_system_page(&file_system_id(), None, 3)
            .expect("first object page");
        assert_eq!(first_object_page.objects.len(), 3);
        assert!(first_object_page.next_cursor.is_some());
        let second_object_page = backend
            .objects_for_file_system_page(
                &file_system_id(),
                first_object_page.next_cursor.as_ref(),
                3,
            )
            .expect("second object page");
        assert_eq!(second_object_page.objects.len(), 3);
        assert!(second_object_page.next_cursor.is_none());
        assert_eq!(
            backend
                .count_objects_for_file_system(&file_system_id())
                .expect("count objects"),
            6
        );

        let mut cursor = None;
        let mut child_names = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .child_objects_for_directory_page(&file_system_id(), "/paged", cursor.as_ref(), 2)
                .expect("child page");
            assert!(page.objects.len() <= 2);
            child_names.extend(
                page.objects
                    .iter()
                    .map(|object| object.path.rsplit('/').next().unwrap().to_string()),
            );
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        child_names.sort();
        assert_eq!(
            child_names,
            vec![
                "file-0.txt",
                "file-1.txt",
                "file-2.txt",
                "file-3.txt",
                "file-4.txt"
            ]
        );
        assert_eq!(pages, 3);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_namespace_jobs() {
        let root = temp_root("local_backend_pages_namespace_jobs");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        for index in 0..5 {
            backend
                .put_namespace_job(&namespace_job(index, &format!("/jobs/{index}")))
                .expect("namespace job");
        }

        let empty = backend
            .list_namespace_jobs_page(None, 0)
            .expect("empty page");
        assert!(empty.jobs.is_empty());
        assert!(empty.next_cursor.is_none());

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..5)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(backend.list_namespace_jobs().expect("legacy list").len(), 5);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_object_versions_receipts_and_intents() {
        let root = temp_root("local_backend_pages_object_local_records");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");

        for index in 0..5 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let mut version_cursor = None;
        let mut paged_versions = Vec::new();
        loop {
            let page = backend
                .versions_for_object_page(&file.object_id, version_cursor.as_ref(), 2)
                .expect("version page");
            assert!(page.versions.len() <= 2);
            paged_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_versions,
            backend
                .versions_for_object(&file.object_id)
                .expect("full versions")
                .into_iter()
                .map(|version| version.version)
                .collect::<Vec<_>>()
        );
        let mut version_scan_cursor = None;
        let mut scanned_versions = Vec::new();
        loop {
            let page = backend
                .version_records_page(version_scan_cursor.as_ref(), 2)
                .expect("version scan page");
            assert!(page.versions.len() <= 2);
            scanned_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_versions, paged_versions);

        for index in 0..5 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }

        let mut receipt_cursor = None;
        let mut paged_receipts = Vec::new();
        loop {
            let page = backend
                .receipts_for_version_page(
                    &file.object_id,
                    &version("version_0"),
                    receipt_cursor.as_ref(),
                    2,
                )
                .expect("receipt page");
            assert!(page.receipts.len() <= 2);
            paged_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_receipts,
            backend
                .receipts_for_version(&file.object_id, &version("version_0"))
                .expect("full receipts")
                .into_iter()
                .map(|receipt| receipt.replica_node_id)
                .collect::<Vec<_>>()
        );
        let mut receipt_scan_cursor = None;
        let mut scanned_receipts = Vec::new();
        loop {
            let page = backend
                .receipt_records_page(receipt_scan_cursor.as_ref(), 2)
                .expect("receipt scan page");
            assert!(page.receipts.len() <= 2);
            scanned_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_receipts, paged_receipts);

        for index in 0..5 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let mut intent_cursor = None;
        let mut paged_intents = Vec::new();
        loop {
            let page = backend
                .intents_for_object_page(&file.object_id, intent_cursor.as_ref(), 2)
                .expect("intent page");
            assert!(page.intents.len() <= 2);
            paged_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );

        let mut scan_cursor = None;
        let mut scanned_intents = Vec::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("intent scan page");
            assert!(page.intents.len() <= 2);
            scanned_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert_eq!(
            scanned_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            scanned_intents
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_intents_by_target_version_after_reopen() {
        let root = temp_root("local_backend_pages_intents_by_target_version");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");
        for index in 0..6 {
            let target_version = if index % 2 == 0 {
                version("version_even")
            } else {
                version("version_odd")
            };
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version,
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-target-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let first_even_page = backend
            .intents_for_object_version_page(&file.object_id, &version("version_even"), None, 2)
            .expect("first even intent page");
        assert_eq!(
            first_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-0", "intent-target-2"]
        );
        assert!(first_even_page.next_cursor.is_some());
        let second_even_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &version("version_even"),
                first_even_page.next_cursor.as_ref(),
                2,
            )
            .expect("second even intent page");
        assert_eq!(
            second_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-4"]
        );
        assert!(second_even_page.next_cursor.is_none());

        drop(backend);
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");
        let mut cursor = None;
        let mut odd_intents = Vec::new();
        loop {
            let page = reopened
                .intents_for_object_version_page(
                    &file.object_id,
                    &version("version_odd"),
                    cursor.as_ref(),
                    1,
                )
                .expect("odd intent page");
            assert!(page.intents.len() <= 1);
            odd_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            odd_intents,
            vec![
                "intent-target-1".to_string(),
                "intent-target-3".to_string(),
                "intent-target-5".to_string()
            ]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_content_update_does_not_rewrite_stable_namespace_indexes() {
        let root = temp_root("local_backend_content_update_skips_stable_indexes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
    fn local_backend_compacts_retention_eligible_tombstones_to_path_markers() {
        let root = temp_root("local_backend_compacts_tombstones_to_markers");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        tombstone.changed_at_unix_nanos = 10;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");

        let dry_run = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::dry_run(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("dry run");
        assert_eq!(dry_run.scanned_tombstone_count, 1);
        assert_eq!(dry_run.eligible_tombstone_count, 1);
        assert_eq!(dry_run.compacted_tombstone_count, 0);
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("dry-run object"),
            Some(tombstone.clone())
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 1);
        assert_eq!(report.eligible_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 1);
        assert_eq!(report.removed_object_record_count, 1);
        assert_eq!(report.removed_inode_record_count, 1);
        assert_eq!(report.removed_file_system_object_record_count, 1);
        assert_eq!(report.preserved_path_marker_count, 1);
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            "/docs/deleted.md"
        ));
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("compacted object"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .is_empty(),
            "compacted tombstones should not hydrate through object scans"
        );

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .get_object(&tombstone.object_id)
                .expect("reopened object"),
            None
        );
        assert_eq!(
            reopened
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("reopened path")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            reopened
                .objects_for_file_system(&file_system.file_system_id)
                .expect("reopened objects")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_tombstone_compaction_retains_young_and_referenced_tombstones() {
        let root = temp_root("local_backend_tombstone_compaction_retains");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut young = object(version("version_2"));
        young.path = "/docs/young.md".to_string();
        young.kind = DfsObjectKind::Tombstone;
        young.content_hash = None;
        young.changed_at_unix_nanos = 90;
        backend.put_object(&young).expect("young tombstone");

        let mut referenced = object(version("version_3"));
        referenced.object_id =
            DfsObjectId::from_legacy_named("obj_referenced_tombstone").expect("object id");
        referenced.path = "/docs/referenced.md".to_string();
        referenced.kind = DfsObjectKind::Tombstone;
        referenced.content_hash = None;
        referenced.changed_at_unix_nanos = 1;
        backend
            .put_object(&referenced)
            .expect("referenced tombstone");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: referenced.file_system_id.clone(),
                object_id: referenced.object_id.clone(),
                version: referenced.version.clone(),
                content_hash: None,
                size_bytes: None,
                locations: Vec::new(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: false,
            })
            .expect("version ref");

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 2);
        assert_eq!(report.eligible_tombstone_count, 0);
        assert_eq!(report.retained_young_tombstone_count, 1);
        assert_eq!(report.retained_referenced_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 0);
        assert_eq!(
            backend.get_object(&young.object_id).expect("young object"),
            Some(young)
        );
        assert_eq!(
            backend
                .get_object(&referenced.object_id)
                .expect("referenced object"),
            Some(referenced)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recreating_compacted_tombstone_path_removes_marker() {
        let root = temp_root("local_backend_recreate_compacted_tombstone_path");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/recreate.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");

        let mut replacement = object(version("version_1"));
        replacement.object_id =
            DfsObjectId::from_legacy_named("obj_recreated_path").expect("object id");
        replacement.path = tombstone.path.clone();
        replacement.kind = DfsObjectKind::File;
        replacement.content_hash = Some("blake3:new".to_string());
        backend.put_object(&replacement).expect("replacement");

        assert!(backend.tables.compacted_tombstones.is_empty());
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &replacement.path)
                .expect("path lookup"),
            Some(replacement.clone())
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.object_id)
                .collect::<Vec<_>>(),
            vec![replacement.object_id]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_compacts_tombstone_and_removes_object_index() {
        let root = temp_root("sharded_backend_compacts_tombstone");
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
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/services/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        assert!(
            backend
                .object_shard_index_path(&tombstone.object_id)
                .is_file()
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.compacted_tombstone_count, 1);
        assert!(
            !backend
                .object_shard_index_path(&tombstone.object_id)
                .exists(),
            "compaction should remove stale object shard routing indexes"
        );
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("object lookup"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_rebuilds_dirfrag_counts_during_table_replay() {
        let root = temp_root("local_backend_rebuilds_dirfrag_counts");
        {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut alias_c = alias_a.clone();
        alias_c.object_id = DfsObjectId::from_legacy_named("obj_alias_c").expect("object");
        alias_c.path = "/archive/readme.alias-c".to_string();
        let mut unrelated = object(version("version_1"));
        unrelated.object_id = DfsObjectId::from_legacy_named("obj_unrelated").expect("object");
        unrelated.path = "/docs/unrelated".to_string();

        for object in [&target, &alias_b, &unrelated, &alias_a, &alias_c] {
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
            vec!["/archive/readme.alias-c", "/docs/readme.alias-a"]
        );
        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system_id(), &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system_id(), &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/archive/readme.alias-c"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system_id(),
                    &target.object_id,
                    "/archive",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/docs/readme.alias-a"
        );

        let first_page = backend
            .hardlinks_to_object_page(&file_system_id(), &target.object_id, None, 1)
            .expect("first page");
        assert_eq!(
            first_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/readme.alias-c"]
        );
        let second_page = backend
            .hardlinks_to_object_page(
                &file_system_id(),
                &target.object_id,
                first_page.next_cursor.as_ref(),
                1,
            )
            .expect("second page");
        assert_eq!(
            second_page
                .objects
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            object_metadata: None,
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
            b"not sobj",
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let table_batches_before_commit = metadata_table_batch_count(&root);
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
            metadata_table_batch_count(&root),
            table_batches_before_commit + 1,
            "one visible-version journal entry should materialize into one table batch"
        );
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-leader"), &leader_root)
                .expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(
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
            read_sobj::<DfsMetadataReplayCursor>(
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
    fn local_backend_replaces_divergent_replica_with_replicated_snapshot() {
        let leader_root = temp_root("local_backend_snapshot_repair_leader");
        let follower_root = temp_root("local_backend_snapshot_repair_follower");
        let shard_id = DfsMetadataShardId::new("root");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
            .expect("follower backend");

        let leader_file_system = file_system();
        leader
            .put_file_system(&leader_file_system)
            .expect("leader filesystem");
        let mut follower_file_system = leader_file_system.clone();
        follower_file_system.display_name = "diverged".to_string();
        follower
            .put_file_system(&follower_file_system)
            .expect("follower filesystem");
        let mut follower_object = object(version("follower-v1"));
        follower_object.content_hash = Some("blake3:follower-diverged".to_string());
        follower
            .put_object(&follower_object)
            .expect("follower object");
        assert!(
            follower
                .apply_journal_batch(
                    &leader
                        .journal_batch_since_limited(0, 1, usize::MAX)
                        .expect("leader batch")
                        .expect("leader batch")
                )
                .expect_err("conflicting sequence should reject before snapshot repair")
                .to_string()
                .contains("already exists with a different entry")
        );

        let archived = follower
            .replace_with_replicated_snapshot(
                shard_id.clone(),
                leader.journal_last_sequence(),
                leader.export_table_snapshot(),
            )
            .expect("replace snapshot");
        assert!(archived);
        assert_eq!(
            follower
                .get_file_system(&leader_file_system.file_system_id)
                .expect("follower filesystem"),
            Some(leader_file_system)
        );
        assert!(
            follower_root.join("replica-repair-archives").exists(),
            "divergent replica state should be archived before replacement"
        );
        let archive_dir = read_dir(&follower_root.join("replica-repair-archives"))
            .expect("repair archives")
            .into_iter()
            .next()
            .expect("repair archive")
            .path();
        assert!(
            archive_dir.join("journal").join("segments").exists(),
            "journal segments should be archived without moving the live journal lock directory"
        );
        assert!(
            follower_root.join("journal").join("write.lock").exists(),
            "snapshot repair must keep the live journal directory in place for Windows"
        );
        let leader_object = object(version("leader-v1"));
        leader.put_object(&leader_object).expect("leader object");
        follower
            .apply_journal_batch(
                &leader
                    .journal_batch_since_limited(1, 1, usize::MAX)
                    .expect("leader object batch")
                    .expect("leader object batch"),
            )
            .expect("repaired follower should accept future primary sequences");
        assert_eq!(
            follower
                .get_object(&leader_object.object_id)
                .expect("follower object"),
            Some(leader_object)
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("status");
        assert_eq!(status.journal_last_sequence, leader.journal_last_sequence());
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: shard_id.clone(),
                replayed_through_sequence: leader.journal_last_sequence(),
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");

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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let mut tombstone = version_3.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = version("version_4");
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        backend.put_object(&tombstone).expect("tombstone");
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        assert_eq!(
            metadata_table_batch_count(&root),
            4,
            "namespace transactions should materialize all table mutations for one journal entry in one table batch"
        );

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");

        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "opening a healthy backend must not append duplicate namespace transaction journal entries"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_put_object_and_version_uses_one_table_batch_without_namespace_transaction() {
        let root = temp_root("local_backend_object_version_grouped_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/grouped.bin".to_string();
        object.content_hash = Some("blake3:grouped-object-version".to_string());
        object.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        let entries = backend
            .put_object_and_version(&object, &version_record)
            .expect("grouped object version");

        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));
        assert!(matches!(
            entries[1].mutation,
            DfsMetadataMutation::PutVersion { .. }
        ));
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "object and visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert!(backend.tables.namespace_transactions.is_empty());
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");
        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "grouped object/version write must not create a recoverable namespace transaction"
        );
        cleanup(&root);
    }

    #[test]
    fn namespace_transaction_publishes_object_and_version_in_one_table_batch() {
        let root = temp_root("namespace_transaction_object_version_single_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/app.bin".to_string();
        object.content_hash = Some("blake3:namespace-version".to_string());
        object.content_size_bytes = Some(16 * 1024 * 1024 * 1024);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "namespace_object_version_single_batch".to_string(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: vec![version_record.clone()],
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("transaction");

        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "creating an object and its visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        let table_batches_after_transaction = metadata_table_batch_count(&root);
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        assert_eq!(
            reopened
                .get_version(&object.object_id, &object.version)
                .expect("reopened version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_after_transaction,
            "replaying a deferred local cursor must not duplicate namespace transaction table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_lists_and_updates_conflicts() {
        let root = temp_root("local_backend_persists_lists_and_updates_conflicts");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
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
        let mut conflict_b = conflict.clone();
        conflict_b.conflict_id = "conflict_b".to_string();

        let entry = backend.put_conflict(&conflict).expect("put conflict");
        backend.put_conflict(&conflict_b).expect("put conflict b");
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
            vec![conflict.clone(), conflict_b.clone()]
        );
        let first_page = backend
            .conflicts_for_object_page(&conflict.object_id, None, 1)
            .expect("first conflict page");
        assert_eq!(first_page.conflicts, vec![conflict.clone()]);
        let second_page = backend
            .conflicts_for_object_page(&conflict.object_id, first_page.next_cursor.as_ref(), 1)
            .expect("second conflict page");
        assert_eq!(second_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_page.next_cursor.is_none());
        assert!(
            backend
                .conflicts_for_object_page(&conflict.object_id, None, 0)
                .expect("empty conflict page")
                .conflicts
                .is_empty()
        );
        let first_scan_page = backend
            .conflict_records_page(None, 1)
            .expect("first conflict scan page");
        assert_eq!(first_scan_page.conflicts, vec![conflict.clone()]);
        let second_scan_page = backend
            .conflict_records_page(first_scan_page.next_cursor.as_ref(), 1)
            .expect("second conflict scan page");
        assert_eq!(second_scan_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_scan_page.next_cursor.is_none());
        assert!(
            backend
                .conflict_records_page(None, 0)
                .expect("empty conflict scan")
                .conflicts
                .is_empty()
        );
        assert_eq!(
            backend.all_conflicts().expect("all conflicts"),
            vec![conflict.clone(), conflict_b.clone()]
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
