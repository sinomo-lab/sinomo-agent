use crate::DfsMetadataShardQuery;
use crate::DfsMetadataShardResponse;
use crate::{
    DfsAdvisoryLockCommand, DfsAdvisoryLockOutcome, DfsAdvisoryLockStateRecord,
    DfsContentReferenceRequest, DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest,
    DfsFileSystemId, DfsMetadataJournalBatch, DfsMetadataJournalFrameRecord,
    DfsMetadataPrefixProof, DfsMetadataShardConfig, DfsMetadataShardId,
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

fn default_advisory_lock_timeout_millis() -> u64 {
    45_000
}

pub(crate) fn content_reference_outcome_family_fingerprint(
    object: &DfsObjectMetadata,
    outcomes: &[crate::DfsNamespaceTransactionReceipt],
    retention: Option<&crate::metadata_records::DfsContentReferenceOutcomeRetention>,
) -> Result<Option<String>, crate::DfsError> {
    if outcomes.is_empty() && retention.is_none() {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(&(
        "sinomo.content-reference-outcome-family.v1",
        &object.file_system_id,
        &object.object_id,
        outcomes,
        retention,
    ))
    .map_err(|error| crate::DfsError::Serialization {
        message: error.to_string(),
    })?;
    Ok(Some(blake3::hash(&bytes).to_hex().to_string()))
}

/// Exact durable evidence retained for one caller-owned data-write operation.
///
/// This is deliberately smaller than the original write response. Repair jobs
/// are transient scheduling output; the fields below are the authoritative
/// identity needed to prove which object generation and content the operation
/// committed (or why its durable admission terminated without committing).
pub(crate) fn required_optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

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

/// Exact input and base identity for a Main-owned ranged write. The enclosing
/// command retains the actual requester, object, base/target versions and shard.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsDistributedWritePatch {
    pub base_content_hash: String,
    pub base_size_bytes: u64,
    pub offset: u64,
    pub patch_hash: String,
    pub patch_size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsDistributedWriteCommand {
    #[serde(
        default,
        skip_serializing_if = "crate::DfsWritePublication::is_default_current"
    )]
    pub publication: crate::DfsWritePublication,
    #[serde(deserialize_with = "required_optional")]
    pub patch: Option<DfsDistributedWritePatch>,
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
    /// The caller explicitly states its actual staged-byte evidence. A streamed
    /// request sets false until the receiver has durably ingested the body.
    pub requester_blob_pre_staged: bool,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
    /// Exact metadata authority selected by the sender for this path.
    ///
    /// Native requests carry the complete assignment. Explicit null is reserved
    /// for an actual epoch-zero local execution; omitted stamps are invalid.
    #[serde(deserialize_with = "required_optional")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
    pub touch_parent_on_create: bool,
    pub foreground_replica_repair: bool,
    /// Reject the write at the metadata authority if the path already names a
    /// current object. Used by POSIX O_EXCL/mknod and symlink creation.
    pub exclusive_create: bool,
}

impl DfsDistributedWriteCommand {
    pub(crate) fn into_network_message(self) -> DfsNetworkMessage {
        if !self.publication.is_current() {
            DfsNetworkMessage::RetainedWriteRequest(self)
        } else if self.exclusive_create {
            DfsNetworkMessage::ExclusiveCreateRequest(self)
        } else {
            DfsNetworkMessage::DistributedWriteRequest(self)
        }
    }

    pub(crate) fn require_publication_mode(&self, retained: bool) -> Result<(), crate::DfsError> {
        self.publication
            .validate()
            .map_err(|message| crate::DfsError::Metadata { message })?;
        if self.publication.is_retained_repair()
            || self.publication.is_current() == retained
            || (retained && (self.exclusive_create || self.touch_parent_on_create))
        {
            return Err(crate::DfsError::Metadata {
                message: "write publication does not match its wire request kind".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsDistributedWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsDistributedWriteResult,
    /// Journal catch-up batches (oldest first) from the requester's reported
    /// cursor through the committed write, so the requester reads its own
    /// write. Bounded by the primary's per-response batch cap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_batches: Vec<DfsMetadataJournalFrameBatch>,
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
    PreconditionFailed {
        failure: crate::DfsWritePreconditionFailure,
    },
    UnsupportedPrecondition {
        path: String,
        reason: String,
    },
    /// The write's metadata body is durable on the primary but did not commit
    /// in this attempt. The requester resolves it by retrying the same
    /// request id; it is never a failure.
    CommitOutcomeUnknown {
        intent_id: String,
        file_system_id: DfsFileSystemId,
        shard_id: String,
        epoch: u64,
        /// The prepared operation id on the primary.
        operation_id: String,
        cause: crate::DfsCommitOutcomeUnknownCause,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_primary_node_id: Option<NodeId>,
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceCommand {
    pub request_id: String,
    pub file_system_id: DfsFileSystemId,
    pub requesting_node_id: NodeId,
    /// False when the requester only needs the operation result and will not
    /// replay response journals or consume mutation path objects locally.
    /// Lookup and explicit sync operations still return their requested path
    /// objects without journal replay.
    pub include_metadata_journal_batches: bool,
    /// The requester's last applied journal sequence per locally known shard,
    /// so the response batches replay exactly the missing entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_after_sequences: Vec<DfsNamespaceShardCursor>,
    /// Exact current metadata-shard configurations used to route this request.
    /// Network receivers recompute and validate the complete stamp.
    pub authority_shards: Vec<DfsMetadataShardConfig>,
    pub operation: DfsNamespaceOperation,
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
pub use sinomo_core::DfsNamespaceObjectPrecondition;

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

/// Attributed direct-child creation bound to one actual opened parent.
/// The distinct closed payload prevents an old receiver from ignoring attrs.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMkdirAtNoLinksWithAttrs {
    pub path: String,
    pub expected_parent_object_id: DfsObjectId,
    pub expected_parent_version: DfsVersion,
    pub attrs: DfsPosixCreateAttrs,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceOperation {
    BeginContentCopy {
        path: String,
        request: crate::DfsContentCopyRequest,
        options: crate::DfsWriteOptions,
    },
    MetadataQuery {
        query: DfsMetadataShardQuery,
    },
    /// Explicit content-reference mutation.
    ContentReference {
        request: DfsContentReferenceRequest,
    },
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
    /// Record a completed read's access time on the object's metadata
    /// authority. Unlike `SetTimes`, it leaves the change time unchanged.
    RecordAccess {
        path: String,
        object_id: DfsObjectId,
    },
    TouchDirectoryModified {
        path: String,
    },
    ClearDirRecursive {
        path: String,
    },
    /// Read-only lookup against the path's metadata primary. The primary may
    /// include the authoritative object in the response; requesters may cache
    /// it in memory but do not replay journals or write it to the local mirror.
    LookupPath {
        path: String,
        /// Explicit optional content range bound to this lookup.
        #[serde(deserialize_with = "required_optional")]
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
    ReconcilePlacement {
        job_id: String,
        path: String,
        object_id: DfsObjectId,
        expected_version: DfsVersion,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: Vec<NodeId>,
    },
    /// Abort only the original requester's exact durable pending write.
    AbortPendingWrite {
        path: String,
        object_id: DfsObjectId,
        target_version: DfsVersion,
        intent_id: String,
        write_operation_id: String,
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

    /// Cross-shard-primary move, sent by the source-shard primary
    /// (coordinator) to the destination-shard primary: atomically validate
    /// the destination and install the moved object record at its new path,
    /// together with its versions, receipts, intents and retained references so
    /// the destination shard owns the complete record set (mirroring
    /// same-node shard migration). `source_path` is the record's previous
    /// path; the receiver verifies the sender is that path's shard primary
    /// before accepting the adoption.
    AdoptObjectAtPath {
        /// Current live source custody offer. Deserialization grants no
        /// permission; the target claims it from the authenticated source actor.
        source_offer: crate::DfsSourceAdoptionOffer,
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
        /// Digest identity of the exact source-authority pin set. The live
        /// source witness establishes authority; this digest alone grants none.
        /// Current sources send this for every exact pin set, including empty
        /// sets. Missing identity or evidence is rejected before any effect.
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
        content_reference_outcomes: Vec<crate::DfsNamespaceTransactionReceipt>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_reference_outcome_retention:
            Option<crate::metadata_records::DfsContentReferenceOutcomeRetention>,
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
    /// Distinct closed command; legacy variant ordinals stay unchanged.
    MkdirAtNoLinksWithAttrs(DfsMkdirAtNoLinksWithAttrs),
    /// Exact parent WRITE-row phase of the original directory namespace job.
    ParentCountParticipant {
        coordinator_job: Box<DfsNamespaceJobRecord>,
        participant: Box<crate::DfsParentCountParticipant>,
        action: crate::DfsParentCountAction,
    },
    /// Release the destination reservation a cross-shard directory move's
    /// root adoption installed on the shard of its target root. Sent by the
    /// coordinator once the whole subtree has moved; idempotent.
    ReleaseDirectoryMoveDestination {
        reservation: Box<DfsNamespaceJobRecord>,
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
                | Self::LookupPath { .. }
                | Self::LookupPaths { .. }
                | Self::AbortPendingWrite { .. }
                | Self::AdvisoryLock { .. }
                | Self::FileSync { .. }
                | Self::DirectorySync { .. }
                | Self::InstallHardlinkTargetFence { .. }
                | Self::ParentCountParticipant { action: crate::DfsParentCountAction::Reserve | crate::DfsParentCountAction::Abort, .. }
                | Self::ReleaseDirectoryMoveDestination { .. }
                | Self::VerifyObjectFamilyAdoption { .. }
                | Self::ContentReference { .. }
                | Self::BeginContentCopy { .. }
                | Self::MetadataQuery { .. }
        )
    }

    /// The path whose shard authority selects the routed coordinator. For
    /// compound operations this is the source/first path; other involved
    /// shards remain independently authoritative for their own journal lanes.
    pub(crate) fn routing_path(&self) -> Option<&str> {
        match self {
            Self::ParentCountParticipant { participant, .. } => Some(&participant.expected.path),
            Self::ReleaseDirectoryMoveDestination { reservation } => Some(&reservation.root_path),
            Self::BeginContentCopy { path, .. } => Some(path),
            Self::ContentReference { request } => Some(&request.path),
            Self::MetadataQuery { .. } => None,
            // A subtree root does not identify one feed authority: nested
            // prefix and hash shards may intersect it. Routed feed code must
            // dispatch by the cursor's exact shard configuration.
            DfsNamespaceOperation::PollNamespaceChanges { .. } => None,
            DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(command) => Some(&command.path),
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
            | DfsNamespaceOperation::RecordAccess { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::AbortPendingWrite { path, .. }
            | DfsNamespaceOperation::AdvisoryLock { path, .. }
            | DfsNamespaceOperation::FileSync { path, .. }
            | DfsNamespaceOperation::DirectorySync { path, .. }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { path, .. }
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
            Self::ParentCountParticipant { participant, .. } => vec![&participant.expected.path],
            Self::ReleaseDirectoryMoveDestination { reservation } => vec![&reservation.root_path],
            Self::BeginContentCopy { path, .. } => vec![path],
            // The locator belongs to the immutable operation digest. It may
            // differ from the stable object's current authority after rename.
            // Unstamped callers therefore use the conservative global lane;
            // stamped dispatch is verified against the canonical identity.
            Self::ContentReference { .. } => Vec::new(),
            Self::MetadataQuery { .. } => Vec::new(),
            DfsNamespaceOperation::PollNamespaceChanges { request } => {
                vec![request.root_path.as_str()]
            }
            DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(command) => vec![&command.path],
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
            | DfsNamespaceOperation::RecordAccess { path, .. }
            | DfsNamespaceOperation::TouchDirectoryModified { path }
            | DfsNamespaceOperation::ClearDirRecursive { path }
            | DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::AbortPendingWrite { path, .. }
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
            | DfsNamespaceOperation::DirectorySync { path, .. } => vec![path],
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceResponse {
    pub request_id: String,
    /// Exact work recorded by this served mutation, independent of its result.
    /// This is scheduling evidence, never publication or durability authority.
    pub committed_work: crate::DfsCommittedWork,
    pub result: DfsNamespaceResult,
    /// For LookupPath, the primary's authoritative object at the requested path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_path_object: Option<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub synced_path_objects: Vec<DfsObjectMetadata>,
    /// Journal catch-up batches for every shard the operation touched, built
    /// from the requester's reported cursors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_journal_batches: Vec<DfsMetadataJournalFrameBatch>,
    /// Explicit completeness of the authority's bounded journal response.
    pub metadata_journal_has_more: bool,
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
    /// The exact Main-produced durable aborted intent, not a success-derived claim.
    AbortedWriteIntent(Box<crate::DistributedWriteIntent>),
    MetadataQuery(DfsMetadataShardResponse),
    NamespaceChanges(DfsNamespaceChangePollResponse),
    NamespaceJob(DfsNamespaceJobRecord),
    /// Content read from the exact object/version returned by the same pinned
    /// authoritative lookup. Absence means the caller must use the ordinary
    /// exact-content read path.
    LookupPathRange(DfsReadRangeData),
    AdvisoryLock(DfsAdvisoryLockOutcome),
    /// Exact acknowledgement for an installed advisory-lock aggregate.
    AdvisoryLockMigration {
        object_id: DfsObjectId,
        state_fingerprint: String,
        durable_through_sequence: u64,
    },

    /// Exact, quorum-durable acknowledgement for a pin-aware object-family
    /// adoption. The fingerprints prevent a partial or mismatched family from releasing
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_reference_outcome_fingerprint: Option<String>,
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
    ParentCountParticipant { participant_id: String, outcome: crate::DfsParentCountOutcome },
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
        stage: String,
        detail: String,
    },
    /// Retriable: the path is part of an in-flight cross-shard move.
    Busy {
        path: String,
    },
    /// Not a failure: the operation's metadata body is durable on the primary
    /// but did not commit in this attempt. The requester retries the same
    /// request id.
    CommitOutcomeUnknown {
        file_system_id: DfsFileSystemId,
        shard_id: String,
        epoch: u64,
        operation_id: String,
        cause: crate::DfsCommitOutcomeUnknownCause,
        /// The shard's primary as the attempt last learned it, when that is
        /// another node: from the publication, or from a write-set member's
        /// own record. A hint for where to retry, never authority.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_primary_node_id: Option<NodeId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
    #[serde(deserialize_with = "required_optional")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
    /// Receipt-slot generation the sender planned this transfer against.
    /// The receiver echoes it; only a matching slot may publish the receipt.
    #[serde(default, skip_serializing_if = "is_zero_receipt_generation")]
    pub receipt_generation: u64,
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
    /// The command's receipt-slot generation, echoed unchanged.
    #[serde(default, skip_serializing_if = "is_zero_receipt_generation")]
    pub receipt_generation: u64,
}

/// A zero generation stays implicit, so a transfer to a never-withdrawn slot
/// keeps its existing encoding. Any other generation is explicit and a peer
/// without receipt generations rejects the command instead of acting on it.
fn is_zero_receipt_generation(generation: &u64) -> bool {
    *generation == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
    #[serde(deserialize_with = "required_optional")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
    /// Receipt-slot generation the sender planned this transfer against.
    #[serde(default, skip_serializing_if = "is_zero_receipt_generation")]
    pub receipt_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
    #[serde(deserialize_with = "required_optional")]
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
#[serde(deny_unknown_fields)]
pub struct DfsRepairReplicaCommand {
    pub request_id: String,
    /// Scheduling hint for a background repair dispatched after the sender has
    /// released its mutation admission. This does not grant repair authority or
    /// change the receiver's global admission and durable validation.
    /// Explicit scheduling priority; it grants no effect authority.
    pub background_repair: bool,
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
    #[serde(deserialize_with = "required_optional")]
    pub authority_shard: Option<DfsMetadataShardConfig>,
    /// Receipt-slot generation the sender planned this repair against. The
    /// receiver echoes it; only a matching slot may publish the receipt.
    #[serde(default, skip_serializing_if = "is_zero_receipt_generation")]
    pub receipt_generation: u64,
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
    /// The command's receipt-slot generation, echoed unchanged.
    #[serde(default, skip_serializing_if = "is_zero_receipt_generation")]
    pub receipt_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFetchBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(deserialize_with = "required_optional")]
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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataJournalFrameBatch {
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub journal_frames: Vec<DfsMetadataJournalFrameRecord>,
}
impl DfsMetadataJournalFrameBatch {
    pub(crate) fn decoded(&self) -> Result<DfsMetadataJournalBatch, crate::MetadataBackendError> {
        let mut previous: Option<u64> = None;
        let mut entries = Vec::with_capacity(self.journal_frames.len());
        for frame in &self.journal_frames {
            if previous.is_some_and(|sequence| Some(frame.sequence) != sequence.checked_add(1)) {
                return Err(crate::MetadataBackendError::Serde(
                    "journal frames are not contiguous".into(),
                ));
            }
            let entry = crate::metadata_journal::decode_metadata_journal_frame_record(frame)?;
            if entry.shard_id != self.shard_id {
                return Err(crate::MetadataBackendError::Serde(
                    "journal frame belongs to another shard".into(),
                ));
            }
            previous = Some(frame.sequence);
            entries.push(entry);
        }
        if previous != Some(self.next_after_sequence) {
            return Err(crate::MetadataBackendError::Serde(
                "journal frames do not reach their declared head".into(),
            ));
        }
        Ok(DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            next_after_sequence: self.next_after_sequence,
            entries,
        })
    }
    pub(crate) fn header(&self) -> DfsMetadataJournalBatch {
        DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            entries: Vec::new(),
            next_after_sequence: self.next_after_sequence,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataJournalBatchCommand {
    pub request_id: String,
    #[serde(deserialize_with = "required_optional")]
    pub source_node_id: Option<NodeId>,
    #[serde(deserialize_with = "required_optional")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(deserialize_with = "required_optional")]
    pub authority_epoch: Option<u64>,
    pub verify_prefix_only: bool,
    #[serde(deserialize_with = "required_optional")]
    pub prefix_probe: Option<DfsMetadataPrefixProbe>,
    #[serde(deserialize_with = "required_optional")]
    pub authority_probe: Option<DfsMetadataAuthorityProbe>,
    #[serde(deserialize_with = "required_optional")]
    pub durable_replication: Option<DfsMetadataDurableReplication>,
    pub batch: DfsMetadataJournalFrameBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataDurableReplication {
    pub confirmation_id: String,
    pub background_publication: bool,
}

/// Restart-unique, exact-state identity used by a fresh metadata durability
/// acknowledgement. A receiver may acknowledge only when its full
/// materialized shard snapshot has this fingerprint at this exact head.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataPrefixProbe {
    pub barrier_id: String,
    #[serde(deserialize_with = "required_optional")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(deserialize_with = "required_optional")]
    pub authority_epoch: Option<u64>,
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
    /// Present only for the existing replica being promoted by a pending
    /// primary-only handoff. The receiver may echo this identity only after it
    /// durably installs the matching local freeze marker.
    #[serde(deserialize_with = "required_optional")]
    pub primary_handoff_id: Option<String>,
    /// Write-set generation the primary's record names, sent only to members
    /// of a write-set shard collecting catch-up evidence. The receiver answers
    /// once its own record view has reached this generation at the probe's
    /// epoch, so the `member_view` it returns describes the copy the write set
    /// would hold. Omitted otherwise, so other probes keep their encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_set_generation: Option<u64>,
    /// The epoch whose history this probe's prefix belongs to, sent only by
    /// a new primary of a write-set shard aligning a member with its epoch
    /// start. The receiver checks the prefix against its own record's epoch
    /// start, drops records after it when its journal matches there, or
    /// reports a head that ends before it. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_start_alignment: Option<u64>,
}

/// Fresh request identity bound to the complete captured current source cut.
/// `operation` separately identifies its immutable original preparation. The
/// current cut includes every later frame; its head need not be the prepare slot.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataAuthorityProbe {
    pub probe_id: String,
    pub expected_shard: DfsMetadataShardConfig,
    pub operation: DfsMetadataAuthorityOperation,
    pub prepared_prefix: DfsMetadataPrefixProof,
}

/// Exact canonical mutation covered by a metadata-shard authority probe.
///
/// Content writes name the same intent/base/target tuple consumed by the
/// atomic visible-commit CAS. Namespace operations bind the complete encoded
/// operation or transaction through `payload_digest`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum DfsMetadataAuthorityOperation {
    PreparedOperation {
        operation_id: String,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        owner_epoch: u64,
        prepare_sequence: u64,
        payload_digest: String,
    },
}

/// Typed evidence consumed by one canonical metadata publication.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum DfsCanonicalAuthorization {
    /// Every configured member acknowledged: a shard on the all-member rule.
    FullShard {
        operation_digest: String,
        expected_shard: DfsMetadataShardConfig,
        acknowledged_node_ids: Vec<NodeId>,
        prepare_sequence: u64,
    },
    /// A shard on the write-set protocol: the write set at
    /// `expected_write_set`'s generation and the pending adds acknowledged,
    /// and the write set meets the commit floor. The publication fence checks
    /// the requirement again against the record it commits under.
    WriteSet {
        operation_digest: String,
        expected_shard: DfsMetadataShardConfig,
        expected_write_set: crate::DfsMetadataShardWriteSet,
        required_node_ids: Vec<NodeId>,
        commit_floor: usize,
        acknowledged_node_ids: Vec<NodeId>,
        prepare_sequence: u64,
    },
}

/// A write-set member's view when it acknowledged: the epoch and write-set
/// generation of its shard record and the exact durable prefix it holds. A
/// primary accepts it as catch-up evidence for an add only when the epoch and
/// generation equal the current record and the prefix is its whole journal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataMemberAckView {
    pub epoch: u64,
    pub write_set_generation: u64,
    pub durable_prefix: crate::DfsMetadataPrefixProof,
}

/// A write-set member's own shard record, sent with a rejection that names
/// it: the epoch, write-set generation and primary the member follows. It is
/// a hint about where the shard's authority moved, never authority itself.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataMemberRecordView {
    pub epoch: u64,
    pub generation: u64,
    pub primary_node_id: NodeId,
}

impl DfsMetadataMemberRecordView {
    /// The view of a member whose shard is on the write-set protocol.
    pub fn of(context: &crate::DfsMetadataReplicaShardContext) -> Option<Self> {
        let write_set = context.write_set.as_ref()?;
        Some(Self {
            epoch: context.shard.epoch,
            generation: write_set.generation,
            primary_node_id: context.shard.primary_node_id.clone(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatchResponse {
    pub request_id: String,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub applied_entries: usize,
    /// True only after this receiver verifies the exact prefix probe.
    pub verified_prefix_only: bool,
    /// Echoed only after the receiver validates an exact write-authority probe,
    /// persists an ordinary durable-replication prefix, or durably installs the
    /// marker named by a primary-handoff prefix probe.
    #[serde(deserialize_with = "required_optional")]
    pub verified_authority_probe_id: Option<String>,
    pub result: DfsMetadataJournalBatchResult,
    /// Sent only by a member of a write-set shard, with a durable
    /// acknowledgement. Omitted otherwise, so other shards' replies keep
    /// their existing encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_view: Option<DfsMetadataMemberAckView>,
    /// Sent only by a member of a write-set shard, with a rejection whose
    /// code names its own record view. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_record: Option<DfsMetadataMemberRecordView>,
}

impl DfsMetadataJournalBatchResponse {
    /// The receiver's record view when it refuses the source's epoch.
    pub fn refusing_member_record(&self) -> Option<&DfsMetadataMemberRecordView> {
        match &self.result {
            DfsMetadataJournalBatchResult::Rejected { code, .. } if code.refuses_source_epoch() => {
                self.member_record.as_ref()
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataSnapshotInstallCommand {
    pub request_id: String,
    pub source_node_id: NodeId,
    #[serde(deserialize_with = "required_optional")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(deserialize_with = "required_optional")]
    pub authority_epoch: Option<u64>,
    /// Exact pending primary handoff that authorizes a frozen promoted replica
    /// to accept this checkpoint repair from the retiring primary.
    #[serde(deserialize_with = "required_optional")]
    pub primary_handoff_id: Option<String>,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
    pub snapshot: crate::DfsMetadataSnapshotImageDescriptor,
    /// Runtime image DATA; serialized heads cannot construct a file resource.
    #[serde(skip)]
    pub image: Option<crate::DfsMetadataSnapshotImage>,
    pub reason: String,
}
impl DfsMetadataSnapshotInstallCommand {
    pub(crate) fn image(
        &self,
    ) -> Result<&crate::DfsMetadataSnapshotImage, crate::MetadataBackendError> {
        let image = self.image.as_ref().ok_or_else(|| {
            crate::MetadataBackendError::Serde(
                "snapshot install requires its delivered native image attachment".into(),
            )
        })?;
        if image.descriptor() != &self.snapshot {
            return Err(crate::MetadataBackendError::Serde(
                "snapshot resource differs from command descriptor".into(),
            ));
        }
        Ok(image)
    }
}
impl PartialEq for DfsMetadataSnapshotInstallCommand {
    fn eq(&self, other: &Self) -> bool {
        self.request_id == other.request_id
            && self.source_node_id == other.source_node_id
            && self.file_system_id == other.file_system_id
            && self.authority_epoch == other.authority_epoch
            && self.primary_handoff_id == other.primary_handoff_id
            && self.shard_id == other.shard_id
            && self.next_after_sequence == other.next_after_sequence
            && self.snapshot == other.snapshot
            && self.reason == other.reason
    }
}
impl Eq for DfsMetadataSnapshotInstallCommand {}

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
        code: crate::DfsMetadataRejectionCode,
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
        code: crate::DfsMetadataRejectionCode,
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
    /// Exclusive namespace creation is a distinct semantic operation.
    ExclusiveCreateRequest(DfsDistributedWriteCommand),
    /// Explicit retained content.
    RetainedWriteRequest(DfsDistributedWriteCommand),
    DistributedWriteResponse(DfsDistributedWriteResponse),
    StableWriteReconciliationRequest(DfsStableWriteReconciliationRequest),
    StableWriteReconciliationResponse(DfsStableWriteReconciliationResponse),
    NamespaceRequest(DfsNamespaceCommand),
    NamespaceResponse(DfsNamespaceResponse),
    StableNamespaceReconciliationRequest(DfsStableNamespaceReconciliationRequest),
    StableNamespaceReconciliationResponse(DfsStableNamespaceReconciliationResponse),
    StageBlob(DfsStageBlobCommand),
    /// Immutable retained content only; current-head publication is separate.
    StageRetainedBlob(DfsStageBlobCommand),
    /// Repair retained bytes without publishing a current replica generation.
    RepairRetainedBlob(DfsStageBlobCommand),
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
    // The removed original-repair custody messages had the production
    // ordinals after this one; a frame naming one fails to decode.
}

impl DfsMetadataJournalBatchResult {
    pub fn from_rejection(rejection: impl Into<crate::DfsMetadataRejection>) -> Self {
        let rejection = rejection.into();
        Self::rejected(rejection.code, rejection.reason)
    }

    pub fn rejected(code: crate::DfsMetadataRejectionCode, reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
            code,
        }
    }

    pub fn rejection_code(&self) -> Option<crate::DfsMetadataRejectionCode> {
        match self {
            Self::Rejected { code, .. } => Some(*code),
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
            code,
        }
    }

    pub fn rejection_code(&self) -> Option<crate::DfsMetadataRejectionCode> {
        match self {
            Self::Rejected { code, .. } => Some(*code),
            _ => None,
        }
    }
}

#[cfg(test)]
mod commit_outcome_unknown_wire_tests {
    use super::*;

    fn cause() -> crate::DfsCommitOutcomeUnknownCause {
        crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
            required: 3,
            acknowledged: 2,
            missing: vec![NodeId::from_legacy_named("node_c").unwrap()],
        }
    }

    #[test]
    fn unknown_outcome_round_trips_on_routed_write_and_namespace_replies() {
        let write = DfsDistributedWriteResult::CommitOutcomeUnknown {
            intent_id: "dfs_write_op_1".into(),
            file_system_id: DfsFileSystemId::generate(),
            shard_id: "root".into(),
            epoch: 4,
            operation_id: "write:dfs_write_op_1".into(),
            cause: cause(),
            current_primary_node_id: None,
        };
        let encoded = serde_json::to_vec(&write).unwrap();
        assert_eq!(
            serde_json::from_slice::<DfsDistributedWriteResult>(&encoded).unwrap(),
            write
        );
        let namespace = DfsNamespacePosixError::CommitOutcomeUnknown {
            file_system_id: DfsFileSystemId::generate(),
            shard_id: "root".into(),
            epoch: 4,
            operation_id: "namespace-request:op".into(),
            cause: crate::DfsCommitOutcomeUnknownCause::BelowWriteSetFloor {
                write_set: 1,
                floor: 2,
            },
            current_primary_node_id: None,
        };
        let encoded = serde_json::to_vec(&namespace).unwrap();
        assert_eq!(
            serde_json::from_slice::<DfsNamespacePosixError>(&encoded).unwrap(),
            namespace
        );
        for cause in [
            cause(),
            crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch: 5 },
            crate::DfsCommitOutcomeUnknownCause::ReplicationBelowIntent {
                accepted: 1,
                intent_w: 3,
            },
            crate::DfsCommitOutcomeUnknownCause::Deadline,
            crate::DfsCommitOutcomeUnknownCause::Cancelled,
            crate::DfsCommitOutcomeUnknownCause::Interrupted {
                detail: "detail".into(),
            },
        ] {
            let encoded = serde_json::to_value(&cause).unwrap();
            assert!(encoded.get("kind").is_some(), "{encoded}");
            assert_eq!(
                serde_json::from_value::<crate::DfsCommitOutcomeUnknownCause>(encoded).unwrap(),
                cause
            );
        }
    }

    /// A decoder that predates the unknown outcome fails closed on it rather
    /// than reading it as another result.
    #[test]
    fn a_decoder_without_the_variant_rejects_it() {
        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "kebab-case")]
        #[allow(dead_code)]
        enum PreChangeResult {
            Outcome {
                outcome: serde_json::Value,
            },
            ReconciliationRequired {
                operation_id: String,
                reason: String,
            },
            PreconditionFailed {
                failure: serde_json::Value,
            },
            UnsupportedPrecondition {
                path: String,
                reason: String,
            },
            Rejected {
                reason: String,
            },
        }
        let encoded = serde_json::to_vec(&DfsDistributedWriteResult::CommitOutcomeUnknown {
            intent_id: "dfs_write_op_1".into(),
            file_system_id: DfsFileSystemId::generate(),
            shard_id: "root".into(),
            epoch: 1,
            operation_id: "write:dfs_write_op_1".into(),
            cause: cause(),
            current_primary_node_id: None,
        })
        .unwrap();
        assert!(serde_json::from_slice::<PreChangeResult>(&encoded).is_err());
    }
}

#[cfg(test)]
mod reactive_namespace_event_tests {
    use super::DfsNamespaceOperation;

    #[test]
    fn lookup_path_requires_explicit_inline_range() {
        let mut wire = serde_json::json!({"lookup-path":{"path":"/services/reactive"}});
        assert!(serde_json::from_value::<DfsNamespaceOperation>(wire.clone()).is_err());
        wire["lookup-path"]["inline_range"] = serde_json::Value::Null;
        let decoded = serde_json::from_value::<DfsNamespaceOperation>(wire.clone())
            .expect("explicit metadata-only lookup");
        assert_eq!(
            decoded,
            DfsNamespaceOperation::LookupPath {
                path: "/services/reactive".into(),
                inline_range: None
            }
        );
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
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
            !DfsNamespaceOperation::LookupPaths {
                paths: vec!["/services/reactive".to_string()],
            }
            .changes_visible_namespace()
        );
    }
}
