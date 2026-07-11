//! Durable DFS metadata record and page schemas.
//!
//! These types are the wire/storage-facing metadata API. Backend modules own
//! persistence and coordination; this module owns the data shapes that callers
//! see through the crate-root re-exports.

use serde::{Deserialize, Serialize};
use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::time::Duration;
use time::OffsetDateTime;

use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_keys::{normalize_node_locations, object_inode_id};
use crate::{
    DfsConflictReadView, DfsError, DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsReplicationPolicy, DfsVersion, normalize_current_locations,
};

/// Stable identifier for a DFS metadata shard.
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

/// Materialized namespace entry for a path in a DFS filesystem.
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

/// Metadata for one immutable object version.
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

/// Append-only metadata mutation as stored in a shard journal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalEntry {
    pub shard_id: DfsMetadataShardId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub mutation: DfsMetadataMutation,
}

/// Mutation payload stored in the metadata journal.
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
    RemoveReceipt {
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

impl DfsMetadataMutation {
    pub(crate) fn affected_object_ids(&self) -> BTreeSet<DfsObjectId> {
        let mut object_ids = BTreeSet::new();
        match self {
            Self::PutFileSystem { .. } => {}
            Self::PutObject { object_id, .. }
            | Self::PutVersion { object_id, .. }
            | Self::PutReceipt { object_id, .. }
            | Self::RemoveReceipt { object_id, .. }
            | Self::PutIntent { object_id, .. }
            | Self::CommitVisibleVersion { object_id, .. }
            | Self::PutConflict { object_id, .. } => {
                object_ids.insert(object_id.clone());
            }
            Self::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => {
                object_ids.extend(
                    transaction
                        .objects
                        .iter()
                        .map(|object| object.object_id.clone()),
                );
                object_ids.extend(
                    transaction
                        .versions
                        .iter()
                        .map(|version| version.object_id.clone()),
                );
            }
            Self::ApplyNamespaceTransaction {
                transaction: None, ..
            } => {}
        }
        object_ids
    }

    pub(crate) fn put_file_system(file_system: &DfsFileSystem) -> Self {
        Self::PutFileSystem {
            file_system_id: file_system.file_system_id.clone(),
            file_system: Some(file_system.clone()),
        }
    }

    pub(crate) fn put_object(object: &DfsObjectMetadata) -> Self {
        Self::PutObject {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            object: Some(object.clone()),
        }
    }

    pub(crate) fn put_version(version: &DfsObjectVersionRecord) -> Self {
        Self::PutVersion {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
            version_record: Some(version.clone()),
        }
    }

    pub(crate) fn put_receipt(receipt: &DfsReplicaReceiptRecord) -> Self {
        Self::PutReceipt {
            file_system_id: receipt.receipt.file_system_id.clone(),
            object_id: receipt.receipt.object_id.clone(),
            version: receipt.receipt.committed_version.clone(),
            replica_node_id: receipt.replica_node_id.clone(),
            receipt: Some(receipt.clone()),
        }
    }

    pub(crate) fn put_intent(intent: &DfsWriteIntentRecord) -> Self {
        Self::PutIntent {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            intent_id: intent.intent_id.clone(),
            status: intent.status.clone(),
            intent: Some(intent.clone()),
        }
    }

    pub(crate) fn commit_visible_version(commit: &DfsVisibleVersionCommitRecord) -> Self {
        Self::CommitVisibleVersion {
            file_system_id: commit.committed_object.file_system_id.clone(),
            object_id: commit.committed_object.object_id.clone(),
            previous_version: commit.expected_version.clone(),
            committed_version: commit.committed_object.version.clone(),
            committed_object: Some(commit.committed_object.clone()),
            visible_version: Some(commit.visible_version.clone()),
            required_replica_node_ids: commit.required_replica_node_ids.clone(),
        }
    }
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

/// Atomic namespace batch used to publish related object and version records.
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
/// Cursor for object scans within one filesystem.
pub struct DfsObjectPageCursor {
    /// Normalized object path at the end of the current page.
    pub path: String,
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of object metadata records.
pub struct DfsObjectPage {
    /// Objects returned by this page.
    pub objects: Vec<DfsObjectMetadata>,
    /// Cursor to pass to the next page request, if more objects remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for conflict records belonging to one object.
pub struct DfsConflictPageCursor {
    /// Conflict id at the end of the current page.
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of conflicts for one object.
pub struct DfsConflictPage {
    /// Conflict records returned by this page.
    pub conflicts: Vec<DfsConflictRecord>,
    /// Cursor to pass to the next page request, if more conflicts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for version records belonging to one object.
pub struct DfsObjectVersionPageCursor {
    /// Version id at the end of the current page.
    pub version: DfsVersion,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of version records for one object.
pub struct DfsObjectVersionPage {
    /// Versions returned by this page.
    pub versions: Vec<DfsObjectVersionRecord>,
    /// Cursor to pass to the next page request, if more versions remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for replica receipt records belonging to one object version.
pub struct DfsReplicaReceiptPageCursor {
    /// Replica node id at the end of the current page.
    pub replica_node_id: NodeId,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of replica receipt records for one object version.
pub struct DfsReplicaReceiptPage {
    /// Receipts returned by this page.
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    /// Cursor to pass to the next page request, if more receipts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for write intent records belonging to one object.
pub struct DfsWriteIntentPageCursor {
    /// Intent id at the end of the current page.
    pub intent_id: String,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of write intent records for one object or object version.
pub struct DfsWriteIntentPage {
    /// Write intents returned by this page.
    pub intents: Vec<DfsWriteIntentRecord>,
    /// Cursor to pass to the next page request, if more intents remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global write-intent scans.
pub struct DfsWriteIntentScanCursor {
    /// Intent creation time at the end of the current page.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Intent id at the end of the current page.
    pub intent_id: String,
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of write intents from a global metadata scan.
pub struct DfsWriteIntentScanPage {
    /// Write intents returned by this page.
    pub intents: Vec<DfsWriteIntentRecord>,
    /// Cursor to pass to the next scan request, if more intents remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global object-version scans.
pub struct DfsObjectVersionScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Version id at the end of the current page.
    pub version: DfsVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of object versions from a global metadata scan.
pub struct DfsObjectVersionScanPage {
    /// Version records returned by this page.
    pub versions: Vec<DfsObjectVersionRecord>,
    /// Cursor to pass to the next scan request, if more versions remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global replica-receipt scans.
pub struct DfsReplicaReceiptScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Version id at the end of the current page.
    pub version: DfsVersion,
    /// Replica node id at the end of the current page.
    pub replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of replica receipts from a global metadata scan.
pub struct DfsReplicaReceiptScanPage {
    /// Replica receipts returned by this page.
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    /// Cursor to pass to the next scan request, if more receipts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global conflict scans.
pub struct DfsConflictScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Conflict id at the end of the current page.
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of conflicts from a global metadata scan.
pub struct DfsConflictScanPage {
    /// Conflicts returned by this page.
    pub conflicts: Vec<DfsConflictRecord>,
    /// Cursor to pass to the next scan request, if more conflicts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for direct child object pages under one directory.
pub struct DfsDirectoryPageCursor {
    /// Child path at the end of the current page.
    pub path: String,
    /// Child object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Dentry cursor used by backends with fragmented directory indexes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dentry: Option<DfsDentryPageCursor>,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Directory-entry cursor within one directory fragment.
pub struct DfsDentryPageCursor {
    /// Directory fragment id at the end of the current page.
    pub dirfrag_id: DfsDirFragmentId,
    /// Dentry name hash at the end of the current page.
    pub name_hash: u64,
    /// Dentry name at the end of the current page.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of direct child objects under one directory.
pub struct DfsDirectoryPage {
    /// Direct children returned by this page.
    pub objects: Vec<DfsObjectMetadata>,
    /// Cursor to pass to the next page request, if more children remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for namespace background job scans.
pub struct DfsNamespaceJobPageCursor {
    /// Job id at the end of the current page.
    pub job_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of namespace background jobs.
pub struct DfsNamespaceJobPage {
    /// Namespace jobs returned by this page.
    pub jobs: Vec<DfsNamespaceJobRecord>,
    /// Cursor to pass to the next page request, if more jobs remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsNamespaceJobPageCursor>,
}

/// Options for compacting tombstone metadata into retained path markers.
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

/// Counts produced by a tombstone compaction run.
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

/// Aggregate shard metadata counters used by status and inspection callers.
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
    RenameObjectAcrossShardPrimaries,
    HardlinkAcrossShardPrimaries,
    RenameDirectoryAcrossShardPrimaries,
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
    /// Cross-shard-primary move: ask the destination-shard primary to adopt
    /// the moved object record at its new path.
    CrossShardAdopt {
        moved_object: DfsObjectMetadata,
        old_path: String,
        destination_primary_node_id: NodeId,
    },
    /// Cross-shard-primary move: locally tombstone the old path, evict the
    /// moved record from the source shard, and touch the source parent.
    CrossShardComplete {
        moved_object: DfsObjectMetadata,
        old_path: String,
    },
    /// Cross-shard-primary hardlink: ask the link-path shard primary to adopt
    /// the alias record.
    CrossShardHardlinkAdopt {
        alias: DfsObjectMetadata,
        target_object_id: DfsObjectId,
        destination_primary_node_id: NodeId,
    },
    /// Cross-shard-primary hardlink: locally record the target's link-count
    /// change (ctime bump).
    CrossShardHardlinkComplete {
        target_object_id: DfsObjectId,
    },
    /// Cross-shard-primary directory rename: move the root directory record
    /// (destination primary adopts it, source primary tombstones the old
    /// path and evicts the record).
    CrossShardDirStart {
        source_object_id: DfsObjectId,
        source_root: String,
        target_root: String,
    },
    /// Cross-shard-primary directory rename: page the children of one moved
    /// directory, adopting each at its target path and evicting the source
    /// record; subdirectories push their own scan frames (depth first).
    CrossShardDirScan {
        source_path: String,
        target_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    /// Cross-shard-primary directory rename: record the source parent's
    /// mtime change once the whole subtree has moved.
    CrossShardDirFinish {
        source_root: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsVisibleVersionCommitRecord {
    pub(crate) expected_version: DfsVersion,
    pub(crate) committed_object: DfsObjectMetadata,
    pub(crate) visible_version: DfsObjectVersionRecord,
    pub(crate) required_replica_node_ids: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created_at: OffsetDateTime,
}

/// Compact materialized metadata table snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshot {
    pub(crate) format_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) materialized_through_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tables: Option<DfsMetadataTableSnapshotTables>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) ops: Vec<DfsMetadataTableOp>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataTableSnapshotTables {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) file_systems: Vec<DfsFileSystem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) file_system_objects: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) paths: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) inodes: Vec<DfsInodeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) dirfrags: Vec<DfsDirFragmentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) dentries: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) hardlinks: Vec<DfsHardlinkIndexSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) compacted_tombstones: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) visible_commits: Vec<DfsVisibleCommitSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_transactions: Vec<DfsNamespaceTransaction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_jobs: Vec<DfsNamespaceJobRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsHardlinkIndexSnapshotRecord {
    pub(crate) target_object_id: DfsObjectId,
    pub(crate) hardlink_object_id: DfsObjectId,
    pub(crate) path_record: DfsPathRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsVisibleCommitSnapshotRecord {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) commit: DfsVisibleVersionCommitRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataTableBatch {
    pub(crate) sequence: u64,
    pub(crate) ops: Vec<DfsMetadataTableOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub(crate) enum DfsMetadataTableOp {
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
    RemoveReceipt {
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
    },
    PutIntent {
        intent: DfsWriteIntentRecord,
    },
    PutConflict {
        conflict: DfsConflictRecord,
    },
    RemoveConflict {
        object_id: DfsObjectId,
        conflict_id: String,
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

impl DfsTombstoneCompactionReport {
    pub(crate) fn for_options(options: &DfsTombstoneCompactionOptions) -> Self {
        Self {
            dry_run: options.dry_run,
            min_tombstone_age_nanos: options.min_tombstone_age_nanos,
            max_compactions: options.max_compactions,
            ..Self::default()
        }
    }

    pub(crate) fn merge(&mut self, other: DfsTombstoneCompactionReport) {
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

fn duration_nanos_saturating(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

pub(crate) fn metadata_journal_batch_file_system_id(
    batch: &DfsMetadataJournalBatch,
) -> Option<DfsFileSystemId> {
    batch
        .entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. }
            | DfsMetadataMutation::PutObject { file_system_id, .. }
            | DfsMetadataMutation::PutVersion { file_system_id, .. }
            | DfsMetadataMutation::PutReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutIntent { file_system_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { file_system_id, .. }
            | DfsMetadataMutation::PutConflict { file_system_id, .. }
            | DfsMetadataMutation::ApplyNamespaceTransaction { file_system_id, .. } => {
                Some(file_system_id.clone())
            }
        })
}

pub(crate) fn read_conflicts_reason(conflicts: Vec<DfsConflictReadView>) -> String {
    conflicts
        .into_iter()
        .map(|conflict| format!("{}: {}", conflict.conflict_id, conflict.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn dfs_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

pub(crate) fn object_version_record(
    object: &DfsObjectMetadata,
    size_bytes: Option<u64>,
) -> DfsObjectVersionRecord {
    let mut locations = object.current_locations.clone();
    normalize_current_locations(&mut locations);
    DfsObjectVersionRecord {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: size_bytes.or(object.content_size_bytes),
        locations,
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

pub(crate) fn normalized_object_metadata(object: &DfsObjectMetadata) -> DfsObjectMetadata {
    let mut object = object.clone();
    normalize_node_locations(&mut object.current_locations);
    object
}

pub(crate) fn normalized_version_record(
    version: &DfsObjectVersionRecord,
) -> DfsObjectVersionRecord {
    let mut version = version.clone();
    normalize_node_locations(&mut version.locations);
    version
}

pub(crate) fn normalized_namespace_transaction(
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

pub(crate) fn put_visible_version_record_if_changed(
    backend: &mut dyn DfsMetadataBackend,
    mut version_record: DfsObjectVersionRecord,
) -> Result<(), DfsError> {
    if let Some(existing) =
        backend.get_version(&version_record.object_id, &version_record.version)?
    {
        version_record.created_at = existing.created_at;
        if existing == version_record {
            return Ok(());
        }
    }
    backend.put_version(&version_record)?;
    Ok(())
}

pub(crate) fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_mutation").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object(object_version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "/docs/mutation.md".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:mutation".to_string()),
            content_size_bytes: Some(32),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: object_version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn version_record(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    #[test]
    fn mutation_constructors_copy_identity_fields_from_payloads() {
        let object = object(version("version_mutation_1"));
        let version_record = version_record(&object);
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: object.owner_node_id.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: node("node_replica"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "intent_mutation".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object.clone()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };

        assert_eq!(
            DfsMetadataMutation::put_object(&object),
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_version(&version_record),
            DfsMetadataMutation::PutVersion {
                file_system_id: version_record.file_system_id.clone(),
                object_id: version_record.object_id.clone(),
                version: version_record.version.clone(),
                version_record: Some(version_record.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_receipt(&receipt),
            DfsMetadataMutation::PutReceipt {
                file_system_id: receipt.receipt.file_system_id.clone(),
                object_id: receipt.receipt.object_id.clone(),
                version: receipt.receipt.committed_version.clone(),
                replica_node_id: receipt.replica_node_id.clone(),
                receipt: Some(receipt.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_intent(&intent),
            DfsMetadataMutation::PutIntent {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                intent_id: intent.intent_id.clone(),
                status: intent.status.clone(),
                intent: Some(intent),
            }
        );
    }

    #[test]
    fn visible_commit_mutation_constructor_uses_commit_record_fields() {
        let committed_object = object(version("version_mutation_2"));
        let visible_version = version_record(&committed_object);
        let commit = DfsVisibleVersionCommitRecord {
            expected_version: version("version_mutation_1"),
            committed_object: committed_object.clone(),
            visible_version: visible_version.clone(),
            required_replica_node_ids: vec![node("node_replica")],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert_eq!(
            DfsMetadataMutation::commit_visible_version(&commit),
            DfsMetadataMutation::CommitVisibleVersion {
                file_system_id: committed_object.file_system_id,
                object_id: committed_object.object_id,
                previous_version: commit.expected_version,
                committed_version: committed_object.version,
                committed_object: Some(commit.committed_object),
                visible_version: Some(visible_version),
                required_replica_node_ids: commit.required_replica_node_ids,
            }
        );
    }
}
