use crate::{
    DfsAdvisoryLockCommand, DfsAdvisoryLockOutcome, DfsAdvisoryLockStateRecord, DfsConflictRecord,
    DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest, DfsFileSystemId,
    DfsMetadataJournalBatch, DfsMetadataJournalFrameRecord, DfsMetadataPrefixProof,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataTableSnapshot,
    DfsMetadataVersionPinRecord, DfsNamespaceChangeBatch, DfsNamespaceChangeCursor,
    DfsNamespaceJobRecord, DfsNamespaceTransaction, DfsObjectId, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsPosixCreateAttrs, DfsPosixDirectorySyncCertificate,
    DfsPosixDirectorySyncTarget, DfsPosixFileSyncCertificate, DfsPosixFileSyncTarget,
    DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsVersion, DfsWriteIntentReceipt,
    DfsWriteIntentReceiptRetentionState, DfsWriteIntentRecord, DfsWriteIntentStatus,
    NetworkedDfsWriteOutcome,
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};

fn default_true() -> bool {
    true
}

fn default_advisory_lock_timeout_millis() -> u64 {
    45_000
}

/// Exact durable evidence retained for one caller-owned data-write operation.
///
/// This is deliberately smaller than the original write response. Repair jobs
/// are transient scheduling output; the fields below are the authoritative
/// identity needed to prove which object generation and content the operation
/// committed (or why its durable admission terminated without committing).
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableWriteOperationReceipt {
    pub operation_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub target_version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    pub status: DfsWriteIntentStatus,
}

/// Read-only classification of a stable data-write operation id.
///
/// `NotApplied` is returned only when the current metadata authority can prove
/// that no durable admission or expired-history marker exists. Callers may
/// submit the original operation only in that state. Every other non-terminal
/// or non-authoritative observation fails closed.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum DfsStableWriteOperationReconciliation {
    Applied {
        receipt: DfsStableWriteOperationReceipt,
    },
    NotApplied,
    AmbiguousOrExpired {
        reason: String,
    },
    Conflict {
        reason: String,
    },
}

/// Exact durable response retained for one caller-owned namespace mutation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableNamespaceOperationReceipt {
    pub operation_id: String,
    pub file_system_id: DfsFileSystemId,
    pub requesting_node_id: NodeId,
    pub authority_shard_id: DfsMetadataShardId,
    pub command_digest: String,
    pub result: DfsNamespaceResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
}

/// Read-only classification of a stable namespace mutation id.
///
/// An `Applied` result is the exact terminal response bound to the command
/// digest. `NotApplied` requires an authoritative, history-complete absence;
/// pending prepares, replica-only absence, and evicted history are never
/// treated as permission to resend.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum DfsStableNamespaceOperationReconciliation {
    Applied {
        receipt: DfsStableNamespaceOperationReceipt,
    },
    NotApplied,
    AmbiguousOrExpired {
        reason: String,
    },
    Conflict {
        reason: String,
    },
}

/// Read-only authority-routed query for one stable data-write outcome.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableWriteReconciliationRequest {
    pub request_id: String,
    pub operation_id: String,
    pub path: String,
    pub object_id: DfsObjectId,
    pub requesting_node_id: NodeId,
    pub expected_content_hash: String,
    pub expected_content_size_bytes: u64,
    pub authority_shard: DfsMetadataShardConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableWriteReconciliationResponse {
    pub request_id: String,
    pub reconciliation: DfsStableWriteOperationReconciliation,
}

/// Read-only authority-routed query for one stable namespace outcome.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableNamespaceReconciliationRequest {
    pub request_id: String,
    pub operation_id: String,
    pub requesting_node_id: NodeId,
    pub operation: Box<DfsNamespaceOperation>,
    pub authority_shards: Vec<DfsMetadataShardConfig>,
    pub dispatch_authority_shard: DfsMetadataShardConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStableNamespaceReconciliationResponse {
    pub request_id: String,
    pub reconciliation: DfsStableNamespaceOperationReconciliation,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_max_bytes: Option<usize>,
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
    /// Whether the requesting node already has this exact blob durably staged.
    ///
    /// Legacy and manifest-routed writes retain the original `true` default.
    /// Direct streamed writes set this to `false`; the metadata primary then
    /// records only replicas that actually own the ingested content.
    #[serde(default = "default_true")]
    pub requester_blob_pre_staged: bool,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
    /// Exact metadata authority selected by the sender for this path.
    ///
    /// This additive stamp lets the daemon admit independent authority-shard
    /// requests concurrently. The receiver validates the complete committed
    /// assignment before mutating; missing stamps retain the serial rolling-
    /// upgrade path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
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
    /// The authority can prove that this operation's compact receipt was
    /// evicted, but can no longer prove its exact outcome. Retrying as a fresh
    /// mutation would risk applying the write twice, so callers must reconcile
    /// the authoritative object generation first.
    ReconciliationRequired {
        operation_id: String,
        reason: String,
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
    /// replay response journals or consume mutation path objects locally.
    /// Lookup and explicit sync operations still return their requested path
    /// objects without journal replay.
    #[serde(
        default = "default_include_metadata_journal_batches",
        skip_serializing_if = "bool_is_true"
    )]
    pub include_metadata_journal_batches: bool,
    /// The requester's last applied journal sequence per locally known shard,
    /// so the response batches replay exactly the missing entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_after_sequences: Vec<DfsNamespaceShardCursor>,
    /// Complete metadata-shard configurations used by the requester to route
    /// this operation. New peers use a single stamped shard as a safe daemon
    /// concurrency identity; receivers always recompute and validate the full
    /// stamp before mutating namespace state. Empty preserves compatibility
    /// with requests sent by older peers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authority_shards: Vec<DfsMetadataShardConfig>,
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

/// Authenticated proof for one file in a local-eventual namespace batch.
///
/// The replica receipt attests that the requester's content blob is durable.
/// `expected_base_version` fences an overwrite; `None` is an exclusive create.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsLocalEventualBatchWriteProof {
    pub expected_base_version: Option<DfsVersion>,
    /// Exact parent generation from which this create's directory touch was
    /// derived. The metadata authority rejects a stale generation before
    /// preparing any child or parent record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_parent_object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_parent_version: Option<DfsVersion>,
    pub receipt: DfsReplicaReceiptRecord,
}

/// Exact namespace generation a conditional mutation is authorized to
/// consume. Both fields are required: object ids fence delete/recreate races,
/// while versions fence in-place changes to the same durable object.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceObjectPrecondition {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

/// Required destination state for a conditional rename.
///
/// `Absent` accepts either no record or the namespace's durable tombstone.
/// `Exact` authorizes replacement of precisely one live generation. A
/// destination created or changed after the caller's observation therefore
/// cannot be overwritten by a delayed or replayed rename.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceDestinationPrecondition {
    Absent,
    Exact(DfsNamespaceObjectPrecondition),
}

/// One shard-local semantic change poll carried over the existing namespace
/// authority route.
///
/// A subtree caller sends one request per current intersecting shard. The
/// cursor itself fences filesystem, shard, authority epoch, and journal
/// sequence; the enclosing namespace command carries the matching complete
/// authority configuration used to select the primary.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceChangePollRequest {
    pub root_path: String,
    pub cursor: DfsNamespaceChangeCursor,
}

/// One independently ordered shard batch. No cross-shard ordering is implied.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceChangePollResponse {
    pub batch: DfsNamespaceChangeBatch,
}

/// Optional bounded content range returned with one authoritative namespace
/// lookup. The inline response is deliberately capped so lightweight reads can
/// avoid a second serialized RPC without moving bulk content through the
/// namespace-control envelope.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceLookupRangeRequest {
    pub offset: u64,
    pub max_bytes: usize,
}

pub(crate) const DFS_NAMESPACE_LOOKUP_INLINE_RANGE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceOperation {
    /// Read-only semantic namespace journal projection for exactly one shard.
    PollNamespaceChanges {
        request: DfsNamespaceChangePollRequest,
    },
    Mkdir {
        path: String,
    },
    MkdirWithAttrs {
        path: String,
        attrs: DfsPosixCreateAttrs,
    },
    /// Create one directory at an exact opened parent without resolving any
    /// link component. The metadata primary revalidates the parent identity
    /// and version before mutating the child path.
    MkdirAtNoLinks {
        path: String,
        expected_parent_object_id: DfsObjectId,
        expected_parent_version: DfsVersion,
    },
    MkdirAll {
        path: String,
    },
    Rmdir {
        path: String,
    },
    /// Remove a directory only while the exact observed generation remains
    /// visible. This is the production mutation form for retrying a durable
    /// caller operation id after an ambiguous response.
    ConditionalRmdir {
        path: String,
        expected: DfsNamespaceObjectPrecondition,
    },
    Unlink {
        path: String,
    },
    /// Unlink only the exact observed generation.
    ConditionalUnlink {
        path: String,
        expected: DfsNamespaceObjectPrecondition,
    },
    Rename {
        from: String,
        to: String,
    },
    /// Rename only the exact source generation and only when the destination
    /// still has the caller-observed state.
    ConditionalRename {
        from: String,
        to: String,
        expected_source: DfsNamespaceObjectPrecondition,
        expected_destination: DfsNamespaceDestinationPrecondition,
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
    /// Install a durable target-authority fence before a foreign alias
    /// authority exposes its namespace generation change.
    InstallHardlinkTargetFence {
        fence: Box<DfsNamespaceJobRecord>,
        source_path: String,
    },
    /// Commit the exact pre-minted canonical generation and release the
    /// matching target-authority fence. This operation is idempotent across a
    /// crash after the object transaction but before fence completion.
    CommitHardlinkTargetFence {
        fence_job_id: String,
        source_path: String,
        target_path: String,
        expected_target: DfsNamespaceObjectPrecondition,
        committed_target: DfsObjectMetadata,
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
        /// New peers may serve this bounded immutable content range in the
        /// same response. Older peers ignore the additive field and return the
        /// metadata-only response, which callers transparently follow with the
        /// existing exact-content RPC.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inline_range: Option<DfsNamespaceLookupRangeRequest>,
    },
    /// Batched read-only lookups against one metadata primary.
    LookupPaths {
        paths: Vec<String>,
    },
    /// Same-shard local-eventual content commit. The metadata primary validates
    /// every caller-owned file transition and commits the authenticated durable
    /// receipts atomically with visible object/version metadata.
    CommitLocalEventualBatch {
        transaction: DfsNamespaceTransaction,
        writes: Vec<DfsLocalEventualBatchWriteProof>,
    },
    /// Idempotently applies one durable placement-reconciliation step on the
    /// object's metadata primary. The operation updates the object's policy,
    /// records a committed repair intent for missing desired replicas, and
    /// retires superseded locations only after every desired replica exists.
    /// The v1 wire name is deliberately unknown to legacy metadata primaries,
    /// so a stale capability view cannot make an old daemon execute it.
    #[serde(rename = "reconcile-placement-v1", alias = "reconcile-placement")]
    ReconcilePlacement {
        job_id: String,
        path: String,
        object_id: DfsObjectId,
        expected_version: DfsVersion,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: Vec<NodeId>,
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
        /// Stable identity for the pin-family handoff. It remains unchanged
        /// when only the source pin set changes, allowing the destination to
        /// reconcile stale copied pins without deleting target-authored pins.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pin_transfer_id: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        versions: Vec<DfsObjectVersionRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        version_pins: Vec<DfsMetadataVersionPinRecord>,
        /// Capability-bearing digest of the exact source-authority pin set.
        /// New sources always send this, including for an empty set. A missing
        /// digest identifies a legacy sender and retains add-only behavior.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version_pin_set_fingerprint: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        receipts: Vec<DfsReplicaReceiptRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        intents: Vec<DfsWriteIntentRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        intent_receipts: Vec<DfsWriteIntentReceipt>,
        #[serde(
            default,
            skip_serializing_if = "DfsWriteIntentReceiptRetentionState::is_empty"
        )]
        intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflicts: Vec<DfsConflictRecord>,
        /// Object-scoped lock state copied atomically with the destination
        /// record. The destination marks it for epoch rebinding before any
        /// new grant can be issued.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
    },
    /// Finalize one pin-aware object-family handoff after the source shard
    /// durably removed its copied pin records and source object generation.
    /// The destination then releases its temporary pin-mutation fence.
    FinalizeObjectFamilyAdoption {
        path: String,
        source_path: String,
        object_id: DfsObjectId,
        target_version: DfsVersion,
        pin_transfer_id: String,
    },
    /// Read-only proof used by a stale saga runner whose local source shard no
    /// longer contains the physical object family. The destination authority
    /// accepts this only when the exact adopted object generation is live and
    /// no pin-transfer fence remains for the object. Unlike finalization, this
    /// operation can never remove transfer provenance.
    VerifyObjectFamilyAdoption {
        object: DfsObjectMetadata,
        source_path: String,
        pin_transfer_id: String,
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
    /// Whether a successful operation can change the namespace projection
    /// observed by mounted filesystems. Coordination reads, durability
    /// barriers, and lock-only protocol messages deliberately do not wake
    /// reactive namespace consumers.
    pub fn changes_visible_namespace(&self) -> bool {
        !matches!(
            self,
            Self::PollNamespaceChanges { .. }
                | Self::SyncPath { .. }
                | Self::LookupPath { .. }
                | Self::LookupPaths { .. }
                | Self::AdvisoryLock { .. }
                | Self::FileSync { .. }
                | Self::DirectorySync { .. }
                | Self::AdvisoryLockMigrationPrepare { .. }
                | Self::InstallHardlinkTargetFence { .. }
                | Self::VerifyObjectFamilyAdoption { .. }
        )
    }

    /// The path whose shard authority selects the routed coordinator. For
    /// compound operations this is the source/first path; other involved
    /// shards remain independently authoritative for their own journal lanes.
    pub(crate) fn routing_path(&self) -> Option<&str> {
        match self {
            // A subtree root does not identify one feed authority: nested
            // prefix and hash shards may intersect it. Routed feed code must
            // dispatch by the cursor's exact shard configuration.
            DfsNamespaceOperation::PollNamespaceChanges { .. } => None,
            DfsNamespaceOperation::Mkdir { path }
            | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { path, .. }
            | DfsNamespaceOperation::MkdirAll { path }
            | DfsNamespaceOperation::Rmdir { path }
            | DfsNamespaceOperation::ConditionalRmdir { path, .. }
            | DfsNamespaceOperation::Unlink { path }
            | DfsNamespaceOperation::ConditionalUnlink { path, .. }
            | DfsNamespaceOperation::SetMode { path, .. }
            | DfsNamespaceOperation::SetOwner { path, .. }
            | DfsNamespaceOperation::SetSymlinkOwner { path, .. }
            | DfsNamespaceOperation::SetTimes { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::AdvisoryLock { path, .. }
            | DfsNamespaceOperation::FileSync { path, .. }
            | DfsNamespaceOperation::DirectorySync { path, .. }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { path, .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { path, .. }
            | DfsNamespaceOperation::CommitHardlinkTargetFence {
                target_path: path, ..
            } => Some(path.as_str()),
            DfsNamespaceOperation::InstallHardlinkTargetFence { fence, .. } => {
                Some(fence.root_path.as_str())
            }
            DfsNamespaceOperation::LookupPaths { paths } => paths.first().map(String::as_str),
            DfsNamespaceOperation::Rename { from, .. }
            | DfsNamespaceOperation::ConditionalRename { from, .. }
            | DfsNamespaceOperation::RenameSingleNamespaceAtomic { from, .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { from, .. } => Some(from.as_str()),
            DfsNamespaceOperation::Hardlink { existing_path, .. } => Some(existing_path.as_str()),
            DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. } => transaction
                .objects
                .first()
                .map(|object| object.path.as_str()),
            DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => Some(object.path.as_str()),
            DfsNamespaceOperation::FinalizeObjectFamilyAdoption { path, .. } => Some(path.as_str()),
            DfsNamespaceOperation::VerifyObjectFamilyAdoption { object, .. } => {
                Some(object.path.as_str())
            }
            DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => Some(alias.path.as_str()),
        }
    }

    /// The namespace paths this operation observes or may mutate (the targets
    /// themselves; parents share a prefix shard with their children except at
    /// shard-boundary anchors, which the handler also derives from these
    /// paths). Feed routing must still use its cursor shard, not this subtree
    /// root.
    pub fn involved_paths(&self) -> Vec<&str> {
        match self {
            DfsNamespaceOperation::PollNamespaceChanges { request } => {
                vec![request.root_path.as_str()]
            }
            DfsNamespaceOperation::Mkdir { path }
            | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { path, .. }
            | DfsNamespaceOperation::MkdirAll { path }
            | DfsNamespaceOperation::Rmdir { path }
            | DfsNamespaceOperation::ConditionalRmdir { path, .. }
            | DfsNamespaceOperation::Unlink { path }
            | DfsNamespaceOperation::ConditionalUnlink { path, .. }
            | DfsNamespaceOperation::SetMode { path, .. }
            | DfsNamespaceOperation::SetOwner { path, .. }
            | DfsNamespaceOperation::SetSymlinkOwner { path, .. }
            | DfsNamespaceOperation::SetTimes { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { path, .. }
            | DfsNamespaceOperation::CommitHardlinkTargetFence {
                target_path: path, ..
            } => vec![path],
            DfsNamespaceOperation::InstallHardlinkTargetFence { fence, .. } => {
                vec![fence.root_path.as_str()]
            }
            DfsNamespaceOperation::LookupPaths { paths } => {
                paths.iter().map(String::as_str).collect()
            }
            DfsNamespaceOperation::Rename { from, to }
            | DfsNamespaceOperation::ConditionalRename { from, to, .. }
            | DfsNamespaceOperation::RenameSingleNamespaceAtomic { from, to, .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { from, to, .. } => {
                vec![from, to]
            }
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } => vec![existing_path, link_path],
            DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. } => transaction
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect(),
            DfsNamespaceOperation::AdvisoryLock { path, .. }
            | DfsNamespaceOperation::FileSync { path, .. }
            | DfsNamespaceOperation::DirectorySync { path, .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { path, .. } => vec![path],
            DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => vec![object.path.as_str()],
            DfsNamespaceOperation::FinalizeObjectFamilyAdoption { path, .. } => {
                // `source_path` authenticates the sending source authority
                // and binds the transfer identity, but finalization mutates
                // only the destination shard's provenance record.
                vec![path.as_str()]
            }
            DfsNamespaceOperation::VerifyObjectFamilyAdoption { object, .. } => {
                vec![object.path.as_str()]
            }
            DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => vec![alias.path.as_str()],
        }
    }
}

pub(crate) fn cross_shard_pin_transfer_id(
    object: &DfsObjectMetadata,
    source_path: &str,
) -> Result<String, crate::DfsError> {
    if source_path.is_empty() || object.path.is_empty() || source_path == object.path {
        return Err(crate::DfsError::Serialization {
            message: format!(
                "cross-shard pin transfer for {} requires distinct non-empty source and target paths",
                object.object_id
            ),
        });
    }
    let encoded = serde_json::to_vec(&(
        "sinomo-dfs-cross-shard-pin-transfer-v1",
        &object.file_system_id,
        &object.object_id,
        source_path,
        &object.path,
        &object.version,
    ))
    .map_err(|source| crate::DfsError::Serialization {
        message: format!(
            "encode durable pin-transfer identity for cross-shard adoption of {}: {source}",
            object.object_id
        ),
    })?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

pub(crate) fn cross_shard_version_pin_set_fingerprint(
    object: &DfsObjectMetadata,
    version_pins: &[DfsMetadataVersionPinRecord],
) -> Result<String, crate::DfsError> {
    let mut canonical_pins = version_pins.to_vec();
    canonical_pins.sort_by(|left, right| {
        (&left.version, &left.pin_id, left.created_at).cmp(&(
            &right.version,
            &right.pin_id,
            right.created_at,
        ))
    });
    for pin in &canonical_pins {
        if pin.file_system_id != object.file_system_id || pin.object_id != object.object_id {
            return Err(crate::DfsError::Serialization {
                message: format!(
                    "durable version pin {}/{}/{} does not belong to adopted object {} in filesystem {}",
                    pin.object_id, pin.version, pin.pin_id, object.object_id, object.file_system_id
                ),
            });
        }
        if pin.pin_id.is_empty() {
            return Err(crate::DfsError::Serialization {
                message: format!(
                    "durable version pin for object {} version {} has an empty consumer identity",
                    object.object_id, pin.version
                ),
            });
        }
    }
    if canonical_pins
        .windows(2)
        .any(|pins| pins[0].version == pins[1].version && pins[0].pin_id == pins[1].pin_id)
    {
        return Err(crate::DfsError::Serialization {
            message: format!(
                "adopted object {} contains duplicate durable version-pin identities",
                object.object_id
            ),
        });
    }
    let encoded = serde_json::to_vec(&(
        "sinomo-dfs-cross-shard-version-pin-set-v1",
        &object.file_system_id,
        &object.object_id,
        &object.version,
        &object.path,
        &canonical_pins,
    ))
    .map_err(|source| crate::DfsError::Serialization {
        message: format!(
            "encode durable version-pin set for cross-shard adoption of {}: {source}",
            object.object_id
        ),
    })?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
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
    /// Whether the authority still had journal history beyond the bounded
    /// batches carried by this response. `None` identifies an older peer, for
    /// which the receiver retains the conservative legacy limit heuristic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_journal_has_more: Option<bool>,
    /// Typed result for namespace reads and POSIX coordination operations.
    /// Keeping this separate from `result` preserves the existing namespace
    /// success/error contract while allowing semantic feed batches, locks, and
    /// durability certificates to cross the same metadata-primary RPC path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPosixCoordinationResult {
    NamespaceChanges(DfsNamespaceChangePollResponse),
    NamespaceJob(DfsNamespaceJobRecord),
    /// Content read from the exact object/version returned by the same pinned
    /// authoritative lookup. Absence means the caller must use the ordinary
    /// exact-content read path, including when talking to an older peer.
    LookupPathRange(DfsReadRangeData),
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
    /// Exact, quorum-durable acknowledgement for a pin-aware object-family
    /// adoption. Older destinations cannot fabricate this enum variant, while
    /// the fingerprints prevent a partial or mismatched family from releasing
    /// the source authority's copy.
    ObjectFamilyAdoption {
        object_id: DfsObjectId,
        target_version: DfsVersion,
        pin_transfer_id: String,
        /// Whether target-side transfer provenance owns at least one copied
        /// pin and therefore requires an explicit post-eviction finalize.
        pin_transfer_active: bool,
        version_pin_set_fingerprint: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        advisory_lock_state_fingerprint: Option<String>,
        durable_through_sequence: u64,
    },
    ObjectFamilyAdoptionFinalized {
        object_id: DfsObjectId,
        pin_transfer_id: String,
        durable_through_sequence: u64,
    },
    /// Read-only acknowledgement that the exact destination generation is
    /// authoritative and its temporary pin-transfer fence is already absent.
    ObjectFamilyAdoptionVerified {
        object_id: DfsObjectId,
        target_version: DfsVersion,
        pin_transfer_id: String,
        durable_through_sequence: u64,
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
    /// Retriable: authoritative symlink metadata exists, but its content is
    /// not yet readable at the location needed to continue path resolution.
    SymlinkContentPending {
        path: String,
        #[serde(default)]
        stage: String,
        #[serde(default)]
        detail: String,
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
    /// Exact filesystem whose committed shard assignment authorizes this
    /// journal stream. Optional on the wire for rolling compatibility, but
    /// required at clustered admission for ordinary batches that mutate the
    /// receiver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    /// Exact committed shard epoch that authorizes this journal stream.
    /// Optional on the wire for rolling compatibility; clustered receivers
    /// reject ordinary applied batches when it is absent or stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_epoch: Option<u64>,
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
    /// Versioned durable-journal carrier for ordinary replica catch-up. The
    /// typed legacy batch stays empty so the authoritative entry is not encoded
    /// twice. A sender may advance its replica cursor only when the receiver
    /// echoes `confirmation_id` after validating and persisting the exact
    /// frames. Older receivers safely treat the empty batch as a no-op but
    /// cannot produce that confirmation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_replication: Option<DfsMetadataDurableReplication>,
    pub batch: DfsMetadataJournalBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataDurableReplication {
    pub confirmation_id: String,
    pub journal_frames: Vec<DfsMetadataJournalFrameRecord>,
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
    /// Present only for the existing replica being promoted by a pending
    /// primary-only handoff. The receiver may echo this identity only after it
    /// durably installs the matching local freeze marker. Older peers omit the
    /// echo and therefore cannot accidentally authorize promotion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_handoff_id: Option<String>,
}

/// Fresh, request-bound identity for an ordinary-write authority probe.
/// The operation identity prevents replay across mutations; `prepared_prefix`
/// additionally binds an acknowledgement to the primary's complete exact
/// history through that operation. Older peers omit the prefix-bound response
/// identity and therefore cannot contribute to the full-shard publication
/// proof.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataAuthorityProbe {
    pub probe_id: String,
    pub expected_shard: DfsMetadataShardConfig,
    pub operation: DfsMetadataAuthorityOperation,
    /// Request the versioned durable-journal acknowledgement path. A peer may
    /// echo the corresponding confirmation only after the exact journal
    /// prefix and operation payload are durable, even when its rebuildable
    /// materialized tables are intentionally caught up after the response.
    ///
    /// Older peers ignore this field and emit the legacy confirmation
    /// identity, so they cannot accidentally satisfy the stronger boundary.
    #[serde(default)]
    pub durable_journal_ack: bool,
    /// Exact journal frames for the durable-ACK path. New senders leave the
    /// legacy typed batch empty, avoiding a second serialization of the same
    /// prepare. Older receivers ignore these frames and cannot produce the V2
    /// confirmation identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub journal_frames: Vec<DfsMetadataJournalFrameRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_prefix: Option<DfsMetadataPrefixProof>,
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
    /// A compact Raft-committed fence for the exact prepared operation.
    ///
    /// DFS still owns and replicates the prepare and all filesystem state.
    /// This permit only reuses cluster KV's existing consensus log to prevent
    /// a partial DFS acknowledgement from racing authority or membership
    /// failover.
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
    /// Echoed only after the receiver validates an exact write-authority probe,
    /// persists an ordinary durable-replication prefix, or durably installs the
    /// marker named by a primary-handoff prefix probe. Missing on older peers,
    /// which forces the sender onto the safe path.
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
    /// Exact pending primary handoff that authorizes a frozen promoted replica
    /// to accept this checkpoint repair from the retiring primary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_handoff_id: Option<String>,
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
    Rejected {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<crate::DfsMetadataRejectionCode>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataSnapshotInstallResult {
    Installed {
        archived_existing: bool,
    },
    Rejected {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<crate::DfsMetadataRejectionCode>,
    },
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
    StableWriteReconciliationRequest(DfsStableWriteReconciliationRequest),
    StableWriteReconciliationResponse(DfsStableWriteReconciliationResponse),
    NamespaceRequest(DfsNamespaceCommand),
    NamespaceResponse(DfsNamespaceResponse),
    StableNamespaceReconciliationRequest(DfsStableNamespaceReconciliationRequest),
    StableNamespaceReconciliationResponse(DfsStableNamespaceReconciliationResponse),
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
    ContentRootCatalogPageRequest(DfsContentRootCatalogPageRequest),
    ContentRootCatalogPageResponse(DfsContentRootCatalogPage),
}

impl DfsMetadataJournalBatchResult {
    pub fn from_rejection(rejection: impl Into<crate::DfsMetadataRejection>) -> Self {
        let rejection = rejection.into();
        Self::rejected(rejection.code, rejection.reason)
    }

    pub fn rejected(code: crate::DfsMetadataRejectionCode, reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
            code: Some(code),
        }
    }

    pub fn rejection_code(&self) -> Option<crate::DfsMetadataRejectionCode> {
        match self {
            Self::Rejected { reason, code } => {
                Some(crate::DfsMetadataRejectionCode::resolve(*code, reason))
            }
            _ => None,
        }
    }
}

impl DfsMetadataSnapshotInstallResult {
    pub fn from_rejection(rejection: impl Into<crate::DfsMetadataRejection>) -> Self {
        let rejection = rejection.into();
        Self::rejected(rejection.code, rejection.reason)
    }

    pub fn rejected(code: crate::DfsMetadataRejectionCode, reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
            code: Some(code),
        }
    }

    pub fn rejection_code(&self) -> Option<crate::DfsMetadataRejectionCode> {
        match self {
            Self::Rejected { reason, code } => {
                Some(crate::DfsMetadataRejectionCode::resolve(*code, reason))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod reactive_namespace_event_tests {
    use super::DfsNamespaceOperation;

    #[test]
    fn lookup_path_without_inline_range_remains_wire_compatible() {
        let legacy = serde_json::json!({
            "lookup-path": {
                "path": "/services/reactive"
            }
        });
        let decoded = serde_json::from_value::<DfsNamespaceOperation>(legacy.clone())
            .expect("deserialize legacy lookup-path operation");

        assert_eq!(
            decoded,
            DfsNamespaceOperation::LookupPath {
                path: "/services/reactive".to_string(),
                inline_range: None,
            }
        );
        assert_eq!(
            serde_json::to_value(decoded).expect("serialize metadata-only lookup-path operation"),
            legacy
        );
    }

    #[test]
    fn only_visible_namespace_mutations_wake_reactive_mounts() {
        assert!(
            DfsNamespaceOperation::Mkdir {
                path: "/services/reactive".to_string(),
            }
            .changes_visible_namespace()
        );
        assert!(
            DfsNamespaceOperation::Unlink {
                path: "/services/reactive/file".to_string(),
            }
            .changes_visible_namespace()
        );

        assert!(
            !DfsNamespaceOperation::LookupPath {
                path: "/services/reactive".to_string(),
                inline_range: None,
            }
            .changes_visible_namespace()
        );
        assert!(
            !DfsNamespaceOperation::SyncPath {
                path: "/services/reactive".to_string(),
            }
            .changes_visible_namespace()
        );
        assert!(
            !DfsNamespaceOperation::LookupPaths {
                paths: vec!["/services/reactive".to_string()],
            }
            .changes_visible_namespace()
        );
    }
}
