use crate::{
    DfsConflictRecord, DfsFileSystemId, DfsMetadataJournalBatch, DfsMetadataShardId,
    DfsMetadataTableSnapshot, DfsNamespaceTransaction, DfsObjectId, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsVersion,
    DfsWriteIntentRecord, NetworkedDfsWriteOutcome,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{DfsWriteReceipt, DfsWriteRequest};
use sinomo_network::{
    MeshMessage, MeshMessageBody, MeshMessageId, NetworkError, NetworkFrame, NetworkFrameId,
    ProtocolMessage,
};
use sinomo_node::NodeId;

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRangeData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub offset: u64,
    pub total_size_bytes: u64,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRequest {
    pub request_id: String,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<DfsVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub start_chunk_index: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub max_chunks: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDirectReadRequest {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub content_size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsReadResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponseChunk {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_total_size_bytes: Option<u64>,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBulkReadItem {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub content_size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBulkReadRequest {
    pub request_id: String,
    pub reads: Vec<DfsBulkReadItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBulkReadItemResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsReadResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBulkReadResponse {
    pub request_id: String,
    pub results: Vec<DfsBulkReadItemResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReadResult {
    Found(DfsReadData),
    RangeFound(DfsReadRangeData),
    NotFound,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsWriteResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteResult {
    Committed {
        receipt: DfsWriteReceipt,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDistributedWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_journal_after_sequence: Option<u64>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
    #[serde(default = "default_true")]
    pub touch_parent_on_create: bool,
    #[serde(default = "default_true")]
    pub foreground_replica_repair: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDistributedWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsDistributedWriteResult,
    /// Journal catch-up batches (oldest first) from the requester's reported
    /// cursor through the committed write, so the requester reads its own
    /// write. Bounded by the primary's per-response batch cap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_batches: Vec<DfsMetadataJournalBatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsDistributedWriteResult {
    Outcome {
        outcome: NetworkedDfsWriteOutcome,
    },
    Rejected {
        reason: String,
        /// Set when the addressed node is not the current metadata-shard
        /// primary. Requesters can retry the same routed write against this
        /// node instead of failing on a stale shard view.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_primary_node_id: Option<NodeId>,
    },
}

/// POSIX namespace mutation routed to the metadata shard primary. The primary
/// is the namespace write authority; it executes the operation, journals the
/// metadata, and replies with catch-up journal batches so the requester reads
/// its own namespace write.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceCommand {
    pub request_id: String,
    pub file_system_id: DfsFileSystemId,
    pub requesting_node_id: NodeId,
    /// False when the requester only needs the operation result and will not
    /// replay response journals locally.
    #[serde(
        default = "default_include_metadata_journal_batches",
        skip_serializing_if = "bool_is_true"
    )]
    pub include_metadata_journal_batches: bool,
    /// The requester's last applied journal sequence per locally known shard,
    /// so the response batches replay exactly the missing entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_after_sequences: Vec<DfsNamespaceShardCursor>,
    pub operation: DfsNamespaceOperation,
}

fn default_include_metadata_journal_batches() -> bool {
    true
}

fn bool_is_true(value: &bool) -> bool {
    *value
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceShardCursor {
    pub shard_id: DfsMetadataShardId,
    pub after_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceOperation {
    Mkdir {
        path: String,
    },
    MkdirAll {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Unlink {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    Hardlink {
        existing_path: String,
        link_path: String,
    },
    SetMode {
        path: String,
        mode: u32,
    },
    SetOwner {
        path: String,
        uid: u32,
        gid: u32,
    },
    SetTimes {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        accessed_at_unix_nanos: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        modified_at_unix_nanos: Option<u64>,
        follow_symlink: bool,
    },
    TouchDirectoryModified {
        path: String,
    },
    ClearDirRecursive {
        path: String,
    },
    /// No-op operation whose response carries journal catch-up batches for
    /// the path's shard. Requesters use it to read their own writes after an
    /// operation that mutated a shard owned by a different primary.
    SyncPath {
        path: String,
    },
    /// Read-only lookup against the path's metadata primary. The primary may
    /// include the authoritative object in the response; requesters may cache
    /// it in memory but do not replay journals or write it to the local mirror.
    LookupPath {
        path: String,
    },
    /// Batched read-only lookups against one metadata primary.
    LookupPaths {
        paths: Vec<String>,
    },
    /// Same-shard namespace batch routed to the metadata primary. This is used
    /// for bulk local-eventual writes so the requester sends one authoritative
    /// metadata transaction instead of N per-file routed mutations.
    ApplyTransaction {
        transaction: DfsNamespaceTransaction,
    },
    /// Cross-shard-primary move, sent by the source-shard primary
    /// (coordinator) to the destination-shard primary: atomically validate
    /// the destination and install the moved object record at its new path,
    /// together with its version, receipt, intent, and conflict history so
    /// the destination shard owns the complete record set (mirroring
    /// same-node shard migration). `source_path` is the record's previous
    /// path; the receiver verifies the sender is that path's shard primary
    /// before accepting the adoption.
    AdoptObjectAtPath {
        object: DfsObjectMetadata,
        source_path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        versions: Vec<DfsObjectVersionRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        receipts: Vec<DfsReplicaReceiptRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        intents: Vec<DfsWriteIntentRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflicts: Vec<DfsConflictRecord>,
    },
    /// Cross-shard-primary hardlink, sent by the link-target's shard primary
    /// (coordinator) to the link-path's shard primary: atomically validate
    /// the link path and install the hardlink alias record. `source_path` is
    /// the link target's path; the receiver verifies the sender is that
    /// path's shard primary.
    AdoptHardlinkAlias {
        alias: DfsObjectMetadata,
        source_path: String,
    },
}

impl DfsNamespaceOperation {
    /// The normalized paths whose metadata shards this operation may mutate
    /// (the targets themselves; parents share a prefix shard with their
    /// children except at shard-boundary anchors, which the handler also
    /// derives from these paths).
    pub fn involved_paths(&self) -> Vec<&str> {
        match self {
            DfsNamespaceOperation::Mkdir { path }
            | DfsNamespaceOperation::MkdirAll { path }
            | DfsNamespaceOperation::Rmdir { path }
            | DfsNamespaceOperation::Unlink { path }
            | DfsNamespaceOperation::SetMode { path, .. }
            | DfsNamespaceOperation::SetOwner { path, .. }
            | DfsNamespaceOperation::SetTimes { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::LookupPath { path } => vec![path],
            DfsNamespaceOperation::LookupPaths { paths } => {
                paths.iter().map(String::as_str).collect()
            }
            DfsNamespaceOperation::Rename { from, to } => vec![from, to],
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } => vec![existing_path, link_path],
            DfsNamespaceOperation::ApplyTransaction { transaction } => transaction
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect(),
            DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => vec![object.path.as_str()],
            DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => vec![alias.path.as_str()],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceResponse {
    pub request_id: String,
    pub result: DfsNamespaceResult,
    /// For SyncPath, the primary may include the authoritative object at the
    /// requested path so an outsider read is not blocked on unrelated shard
    /// journal backlog. Journal batches remain the durable convergence path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_path_object: Option<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub synced_path_objects: Vec<DfsObjectMetadata>,
    /// Journal catch-up batches for every shard the operation touched, built
    /// from the requester's reported cursors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_batches: Vec<DfsMetadataJournalBatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceResult {
    Applied,
    Rejected {
        reason: String,
        /// Structured POSIX error so the requester surfaces the same errno
        /// semantics as a local operation. `None` for transport, authority,
        /// or internal failures.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        posix_error: Option<DfsNamespacePosixError>,
        /// Set on authority rejections: the shard primary the responder
        /// currently knows. A requester routing on a stale shard view retries
        /// against this node instead of failing the operation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_primary_node_id: Option<NodeId>,
    },
}

/// Wire form of the `DfsPosixError` variants that namespace operations can
/// produce, so errno mapping survives routing.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespacePosixError {
    NotFound {
        path: String,
    },
    AlreadyExists {
        path: String,
    },
    NotDirectory {
        path: String,
    },
    IsDirectory {
        path: String,
    },
    NotSymlink {
        path: String,
    },
    MissingParent {
        path: String,
    },
    InvalidPath {
        path: String,
    },
    DirectoryNotEmpty {
        path: String,
    },
    Conflict {
        path: String,
        reason: String,
    },
    /// Retriable: the path is part of an in-flight cross-shard move.
    Busy {
        path: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStagePatchCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub base_version: DfsVersion,
    pub version: DfsVersion,
    pub content_hash: String,
    pub offset: u64,
    pub patch_hash: String,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStagePatchChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub base_version: DfsVersion,
    pub version: DfsVersion,
    pub content_hash: String,
    pub offset: u64,
    pub patch_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_base_version: Option<DfsVersion>,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub start_chunk_index: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub max_chunks: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub result: DfsFetchBlobResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobResponseChunk {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_hash: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsFetchBlobResult {
    Found {
        content_hash: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchCommand {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<NodeId>,
    pub batch: DfsMetadataJournalBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub applied_entries: usize,
    pub result: DfsMetadataJournalBatchResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallCommand {
    pub request_id: String,
    pub source_node_id: NodeId,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub snapshot: DfsMetadataTableSnapshot,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub result: DfsMetadataSnapshotInstallResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRoutedMessage {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub frame_id: u64,
    pub message_id: u64,
    pub message: DfsNetworkMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMessageCommand {
    pub request_id: String,
    pub routed_message: DfsRoutedMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMessageResponse {
    pub request_id: String,
    pub outbound_messages: Vec<DfsRoutedMessage>,
}

pub(crate) fn dfs_routed_message_from_protocol_frame(
    frame: &NetworkFrame,
) -> Result<DfsRoutedMessage, NetworkError> {
    let message = dfs_network_message_from_frame(frame)?;
    Ok(DfsRoutedMessage {
        source_node_id: frame.source_node_id.clone(),
        target_node_id: frame.target_node_id.clone(),
        frame_id: frame.frame_id.0,
        message_id: frame.message.message_id.0,
        message,
    })
}

pub(crate) fn dfs_protocol_frame_from_routed_message(
    message: &DfsRoutedMessage,
) -> Result<NetworkFrame, NetworkError> {
    dfs_network_frame_from_routed_message(message)
}

fn dfs_network_message_from_frame(frame: &NetworkFrame) -> Result<DfsNetworkMessage, NetworkError> {
    let MeshMessageBody::Protocol(protocol_message) = &frame.message.body else {
        return Err(dfs_protocol_serialization_error(
            "DFS message route requires a protocol frame",
        ));
    };
    if protocol_message.protocol != crate::DFS_PROTOCOL {
        return Err(dfs_protocol_serialization_error(format!(
            "DFS message route requires protocol {}, got {}",
            crate::DFS_PROTOCOL,
            protocol_message.protocol
        )));
    }
    protocol_message.decode::<DfsNetworkMessage>()
}

fn dfs_network_frame_from_routed_message(
    message: &DfsRoutedMessage,
) -> Result<NetworkFrame, NetworkError> {
    let protocol_message = ProtocolMessage::encode(crate::DFS_PROTOCOL, &message.message)?;
    Ok(NetworkFrame {
        frame_id: NetworkFrameId(message.frame_id),
        source_node_id: message.source_node_id.clone(),
        target_node_id: message.target_node_id.clone(),
        message: MeshMessage {
            message_id: MeshMessageId(message.message_id),
            origin_node_id: message.source_node_id.clone(),
            target_node_id: message.target_node_id.clone(),
            body: MeshMessageBody::Protocol(protocol_message),
        },
    })
}

fn dfs_protocol_serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataJournalBatchResult {
    Applied,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataSnapshotInstallResult {
    Installed { archived_existing: bool },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsBlobTransferResult {
    Durable { content_hash: String },
    Committed { content_hash: String },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNetworkMessage {
    ReadRequest(DfsReadRequest),
    ReadResponse(DfsReadResponse),
    ReadResponseChunk(DfsReadResponseChunk),
    BulkReadRequest(DfsBulkReadRequest),
    BulkReadResponse(DfsBulkReadResponse),
    WriteRequest(DfsWriteCommand),
    WriteResponse(DfsWriteResponse),
    DistributedWriteRequest(DfsDistributedWriteCommand),
    DistributedWriteResponse(DfsDistributedWriteResponse),
    NamespaceRequest(DfsNamespaceCommand),
    NamespaceResponse(DfsNamespaceResponse),
    StageBlob(DfsStageBlobCommand),
    StageBlobChunk(DfsStageBlobChunkCommand),
    StagePatch(DfsStagePatchCommand),
    StagePatchChunk(DfsStagePatchChunkCommand),
    StageBlobResponse(DfsStageBlobResponse),
    CommitVersion(DfsCommitVersionCommand),
    CommitVersionResponse(DfsCommitVersionResponse),
    RepairReplica(DfsRepairReplicaCommand),
    RepairReplicaChunk(DfsRepairReplicaChunkCommand),
    RepairReplicaResponse(DfsRepairReplicaResponse),
    FetchBlob(DfsFetchBlobCommand),
    FetchBlobResponse(DfsFetchBlobResponse),
    FetchBlobResponseChunk(DfsFetchBlobResponseChunk),
    MetadataJournalBatch(DfsMetadataJournalBatchCommand),
    MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse),
    MetadataSnapshotInstall(DfsMetadataSnapshotInstallCommand),
    MetadataSnapshotInstallResponse(DfsMetadataSnapshotInstallResponse),
}
