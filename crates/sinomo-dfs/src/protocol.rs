use crate::{
    DfsAdvisoryLockCommand, DfsAdvisoryLockOutcome, DfsAdvisoryLockStateRecord, DfsConflictRecord,
    DfsFileSystemId, DfsMetadataJournalBatch, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataTableSnapshot, DfsNamespaceJobRecord, DfsNamespaceTransaction, DfsObjectId,
    DfsObjectMetadata, DfsObjectVersionRecord, DfsPosixCreateAttrs,
    DfsPosixDirectorySyncCertificate, DfsPosixDirectorySyncTarget, DfsPosixFileSyncCertificate,
    DfsPosixFileSyncTarget, DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsVersion,
    DfsWriteIntentRecord, NetworkedDfsWriteOutcome,
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::{DfsWriteReceipt, DfsWriteRequest};

fn default_true() -> bool {
    true
}

fn default_advisory_lock_timeout_millis() -> u64 {
    45_000
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
    /// Reject the write at the metadata authority if the path already names a
    /// current object. Used by POSIX O_EXCL/mknod and symlink creation.
    #[serde(default)]
    pub exclusive_create: bool,
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
    MkdirWithAttrs {
        path: String,
        attrs: DfsPosixCreateAttrs,
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
    RenameSingleNamespaceAtomic {
        from: String,
        to: String,
        expected_shard_id: String,
        expected_shard_epoch: u64,
    },
    RenameDirectoryBounded {
        from: String,
        to: String,
        max_steps: u64,
    },
    ResumeNamespaceJobBounded {
        job_id: String,
        path: String,
        max_steps: u64,
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
    SetSymlinkOwner {
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
    /// Whole-file advisory-lock command routed to the metadata authority for
    /// the stable object. `path` is used only for shard routing and for
    /// detecting a stale path-to-object observation; lock ownership is scoped
    /// to `object_id`, so rename and hardlink aliases cannot split a lock.
    AdvisoryLock {
        path: String,
        object_id: DfsObjectId,
        command: DfsAdvisoryLockCommand,
        /// Server execution budget, kept below the caller's RPC deadline so
        /// a canonical outcome can return before transport timeout.
        #[serde(default = "default_advisory_lock_timeout_millis")]
        timeout_millis: u64,
    },
    /// File durability barrier for an immutable object/version/hash prefix.
    FileSync {
        path: String,
        target: DfsPosixFileSyncTarget,
    },
    /// Directory durability barrier for an immutable namespace generation.
    DirectorySync {
        path: String,
        target: DfsPosixDirectorySyncTarget,
    },
    /// Non-mutating capability handshake required before a lock-bearing
    /// cross-shard adoption. Peers that do not understand this variant reject
    /// it before they can expose a partial destination object.
    AdvisoryLockMigrationPrepare {
        path: String,
        source_path: String,
        object_id: DfsObjectId,
        state_fingerprint: String,
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
        /// Object-scoped lock state copied atomically with the destination
        /// record. The destination marks it for epoch rebinding before any
        /// new grant can be issued.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
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
            | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
            | DfsNamespaceOperation::MkdirAll { path }
            | DfsNamespaceOperation::Rmdir { path }
            | DfsNamespaceOperation::Unlink { path }
            | DfsNamespaceOperation::SetMode { path, .. }
            | DfsNamespaceOperation::SetOwner { path, .. }
            | DfsNamespaceOperation::SetSymlinkOwner { path, .. }
            | DfsNamespaceOperation::SetTimes { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::LookupPath { path }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { path, .. } => vec![path],
            DfsNamespaceOperation::LookupPaths { paths } => {
                paths.iter().map(String::as_str).collect()
            }
            DfsNamespaceOperation::Rename { from, to }
            | DfsNamespaceOperation::RenameSingleNamespaceAtomic { from, to, .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { from, to, .. } => {
                vec![from, to]
            }
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } => vec![existing_path, link_path],
            DfsNamespaceOperation::ApplyTransaction { transaction } => transaction
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect(),
            DfsNamespaceOperation::AdvisoryLock { path, .. }
            | DfsNamespaceOperation::FileSync { path, .. }
            | DfsNamespaceOperation::DirectorySync { path, .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { path, .. } => vec![path],
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
    /// Typed result for POSIX coordination operations. Keeping this separate
    /// from `result` preserves the existing namespace success/error contract
    /// while allowing lock and durability certificates to cross the same
    /// metadata-primary RPC path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPosixCoordinationResult {
    NamespaceJob(DfsNamespaceJobRecord),
    AdvisoryLock(DfsAdvisoryLockOutcome),
    /// Capability acknowledgement for a lock-bearing cross-shard adoption.
    /// An older destination that ignores the optional lock aggregate cannot
    /// fabricate this exact-state receipt, so the source will not evict.
    AdvisoryLockMigration {
        object_id: DfsObjectId,
        state_fingerprint: String,
        durable_through_sequence: u64,
    },
    AdvisoryLockMigrationPrepared {
        object_id: DfsObjectId,
        state_fingerprint: String,
    },
    FileSync(DfsPosixFileSyncCertificate),
    DirectorySync(DfsPosixDirectorySyncCertificate),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
pub struct DfsFetchBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
    /// Verify that the receiver already has the exact materialized metadata
    /// state described by `prefix_probe`, without applying `batch`. The batch
    /// must be empty so an older peer that ignores this flag cannot skip or
    /// poison its journal replay cursor during a rolling upgrade.
    #[serde(default)]
    pub verify_prefix_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_probe: Option<DfsMetadataPrefixProbe>,
    /// Verify only that the receiver is reachable and observes this exact
    /// committed shard assignment. The batch must be empty so older peers
    /// safely process it as a no-op; because they cannot echo `probe_id`, they
    /// cannot accidentally satisfy the new authority fast path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_probe: Option<DfsMetadataAuthorityProbe>,
    pub batch: DfsMetadataJournalBatch,
}

/// Restart-unique, exact-state identity used by a fresh metadata durability
/// acknowledgement. A receiver may acknowledge only when its full
/// materialized shard snapshot has this fingerprint at this exact head.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataPrefixProbe {
    pub barrier_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_epoch: Option<u64>,
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
}

/// Fresh, request-bound identity for an ordinary-write authority probe.
/// Unlike a prefix probe, this does not fingerprint or persist metadata state.
/// The operation identity prevents a reachability acknowledgement from being
/// replayed as authority for a different write or namespace transition.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataAuthorityProbe {
    pub probe_id: String,
    pub expected_shard: DfsMetadataShardConfig,
    pub operation: DfsMetadataAuthorityOperation,
}

/// Exact canonical mutation covered by a metadata-shard authority probe.
///
/// Content writes name the same intent/base/target tuple consumed by the
/// atomic visible-commit CAS. Namespace operations bind the complete encoded
/// operation or transaction through `payload_digest`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum DfsMetadataAuthorityOperation {
    WriteIntent {
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        owner_epoch: u64,
        content_hash: String,
        receipt_node_ids: Vec<NodeId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace_prepare_operation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace_prepare_digest: Option<String>,
        prepare_sequence: u64,
        payload_digest: String,
    },
    NamespaceMutation {
        operation_id: String,
        file_system_id: DfsFileSystemId,
        prepare_sequence: u64,
        payload_digest: String,
    },
}

/// Typed evidence consumed by one canonical metadata publication.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum DfsCanonicalAuthorization {
    FullShard {
        operation_digest: String,
        expected_shard: DfsMetadataShardConfig,
        acknowledged_node_ids: Vec<NodeId>,
        prepare_sequence: u64,
    },
    ClusterQuorum {
        operation_digest: String,
        expected_shard: DfsMetadataShardConfig,
        permit_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub applied_entries: usize,
    /// True only when the receiver understood and verified an exact
    /// `verify_prefix_only` probe. This prevents mixed-version peers from
    /// accidentally satisfying the stronger durability certificate.
    #[serde(default)]
    pub verified_prefix_only: bool,
    /// Echoed only after the receiver validates the exact authority probe.
    /// Missing on older peers, which forces the sender onto the quorum path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_authority_probe_id: Option<String>,
    pub result: DfsMetadataJournalBatchResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSnapshotInstallCommand {
    pub request_id: String,
    pub source_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_epoch: Option<u64>,
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
    BulkReadRequest(DfsBulkReadRequest),
    BulkReadResponse(DfsBulkReadResponse),
    WriteRequest(DfsWriteCommand),
    WriteResponse(DfsWriteResponse),
    DistributedWriteRequest(DfsDistributedWriteCommand),
    /// Exclusive namespace creation. Kept as a distinct message variant so
    /// older peers reject it instead of ignoring an additive flag and
    /// accidentally executing it as an overwrite-capable write.
    ExclusiveCreateRequest(DfsDistributedWriteCommand),
    DistributedWriteResponse(DfsDistributedWriteResponse),
    NamespaceRequest(DfsNamespaceCommand),
    NamespaceResponse(DfsNamespaceResponse),
    StageBlob(DfsStageBlobCommand),
    StagePatch(DfsStagePatchCommand),
    StageBlobResponse(DfsStageBlobResponse),
    CommitVersion(DfsCommitVersionCommand),
    CommitVersionResponse(DfsCommitVersionResponse),
    RepairReplica(DfsRepairReplicaCommand),
    RepairReplicaResponse(DfsRepairReplicaResponse),
    FetchBlob(DfsFetchBlobCommand),
    FetchBlobResponse(DfsFetchBlobResponse),
    MetadataJournalBatch(DfsMetadataJournalBatchCommand),
    MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse),
    MetadataSnapshotInstall(DfsMetadataSnapshotInstallCommand),
    MetadataSnapshotInstallResponse(DfsMetadataSnapshotInstallResponse),
}
