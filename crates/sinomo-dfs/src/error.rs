use crate::{
    ContentStoreError, DfsDistributedError, DfsFileSystemId, DfsObjectId, DfsVersion,
    MetadataBackendError,
};
use serde::{Deserialize, Serialize};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::kv::KvError;
use sinomo_core::network::NetworkDeliveryState;
use sinomo_core::node::NodeId;
use thiserror::Error;

const DFS_REPLICA_NOT_DESIRED_REASON_PREFIX: &str =
    "local node is not a desired replica for DFS write intent ";

/// Error type shared by DFS metadata, content, routing, and service code.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsError {
    #[error("unknown filesystem: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("unknown object: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error("unsupported DFS policy for local-owned service: {policy:?}")]
    UnsupportedPolicy { policy: DfsObjectPolicy },
    #[error("node {local_node_id} cannot own object {object_id}; owner is {owner_node_id}")]
    NotObjectOwner {
        object_id: DfsObjectId,
        owner_node_id: NodeId,
        local_node_id: NodeId,
    },
    #[error("network error: {message}")]
    Network { message: String },
    /// A request failed at a transport boundary which can classify whether
    /// the peer may already have applied it. Mutation callers must preserve
    /// `PossiblyApplied` instead of treating it like a safe fresh retry.
    #[error("network delivery {delivery_state:?}: {message}")]
    NetworkDelivery {
        message: String,
        delivery_state: NetworkDeliveryState,
    },
    #[error("serialization error: {message}")]
    Serialization { message: String },
    #[error("content store error: {message}")]
    ContentStore { message: String },
    #[error("distributed DFS error: {message}")]
    Distributed { message: String },
    #[error("DFS write precondition failed: {failure:?}")]
    WritePreconditionFailed {
        failure: crate::DfsWritePreconditionFailure,
    },
    #[error("conditional write unsupported at {path}: {reason}")]
    UnsupportedWritePrecondition { path: String, reason: String },
    /// The operation's complete body is durable on the shard primary but did
    /// not commit in this attempt. It can still commit through a retry with
    /// the same operation id or through prepared-change recovery on this or a
    /// successor primary, so it is neither a success nor a failure. Callers
    /// must not report it as a failure and must not retry it under a new
    /// operation id.
    #[error(
        "metadata shard {shard_id} epoch {epoch}: outcome of operation {operation_id} is unknown ({cause}); retry with the same operation id"
    )]
    MetadataCommitOutcomeUnknown {
        file_system_id: DfsFileSystemId,
        shard_id: String,
        epoch: u64,
        /// The prepared operation id.
        operation_id: String,
        cause: DfsCommitOutcomeUnknownCause,
        /// Set when the shard record names another primary.
        current_primary_node_id: Option<NodeId>,
    },
    #[error(
        "DFS write operation {operation_id} for object {object_id} was already bound to a different payload"
    )]
    WriteOperationIdConflict {
        operation_id: String,
        object_id: DfsObjectId,
    },
    #[error(
        "DFS write operation {operation_id} for object {object_id} requires authoritative reconciliation: {reason}"
    )]
    WriteReconciliationRequired {
        operation_id: String,
        object_id: DfsObjectId,
        reason: String,
    },
    /// A metadata-shard reconfiguration has been durably recorded but cannot
    /// become active until the retiring authority and every required target
    /// have produced the exact handoff evidence.
    ///
    /// This is a reactive wait state, not a transport or control-plane
    /// failure. Callers which own reconciliation should park on the handoff,
    /// shard-authority, completion-proof, and replica-progress generations
    /// instead of retrying it on a generic failure timer.
    #[error(
        "metadata shard {shard_id} membership or primary-role change from epoch {retiring_epoch} to {desired_epoch} is fenced and parked pending exact DFS catch-up of every retiring and desired member"
    )]
    MetadataShardMembershipHandoffParked {
        file_system_id: DfsFileSystemId,
        shard_id: String,
        handoff_id: String,
        retiring_epoch: u64,
        desired_epoch: u64,
    },
    #[error(
        "metadata storage preparation at {root:?} remains pending after {processed_records} records"
    )]
    StoragePreparationPending {
        root: std::path::PathBuf,
        processed_records: usize,
    },
    #[error("metadata backend error: {message}")]
    Metadata { message: String },
    #[error("metadata backend error: {message}")]
    MetadataRejection {
        code: crate::DfsMetadataRejectionCode,
        message: String,
    },
    #[error("invalid DFS replication policy: {message}")]
    InvalidReplicationPolicy { message: String },
    #[error("content hash mismatch: expected {expected}, actual {actual}")]
    ContentHashMismatch { expected: String, actual: String },
    /// The current metadata authority durably withdrew a stale replica receipt
    /// after the receiver could no longer open the exact committed content.
    /// The committed intent remains authoritative and maintenance must repair
    /// this desired replica from another exact-version source.
    #[error(
        "DFS commit content repair pending for object {object_id} version {version} ({content_hash})"
    )]
    CommitContentRepairPending {
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
    },
    /// A receipt publisher planned against an older receipt-slot generation:
    /// a later withdrawal superseded its attempt. Callers re-plan from the
    /// current tables instead of retrying the superseded evidence.
    #[error(
        "DFS receipt slot of object {object_id} version {version} replica {replica_node_id} is at generation {current}, superseding planned generation {expected}"
    )]
    ReceiptGenerationSuperseded {
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        expected: u64,
        current: u64,
    },
    #[error("unauthorized DFS replica command from {source_node_id}: {reason}")]
    UnauthorizedReplicaCommand {
        source_node_id: NodeId,
        reason: String,
    },
    #[error(
        "stale owner epoch for filesystem {file_system_id}: local node {local_node_id}, primary {primary_node_id}, epoch {epoch}"
    )]
    StaleOwnerEpoch {
        file_system_id: DfsFileSystemId,
        local_node_id: NodeId,
        primary_node_id: NodeId,
        epoch: u64,
    },
    /// A transient, retriable rejection: the path participates in an
    /// in-flight cross-shard move (or equivalent exclusive maintenance) and
    /// mutations must retry once it settles. Maps to `EAGAIN` at the POSIX
    /// boundary.
    #[error("path {path} is moving between metadata shards; retry the operation")]
    Busy { path: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
    #[error("path already exists: {path}")]
    AlreadyExists { path: String },
    #[error(
        "directory {path} changed before the namespace batch committed: expected version {expected_version}, current version {current_version}"
    )]
    StaleDirectoryVersion {
        path: String,
        expected_version: DfsVersion,
        current_version: DfsVersion,
    },
    /// The caller cancelled the operation through the service's
    /// [`DfsCancellationFlag`](crate::DfsCancellationFlag) — typically a budgeted
    /// load giving up while the cluster/DFS link is degraded. Distributed wait
    /// and retry loops return this instead of running out their full timeouts.
    #[error("DFS operation cancelled: {operation}")]
    Cancelled { operation: &'static str },
}

/// Why a durably prepared metadata change did not commit in one attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsCommitOutcomeUnknownCause {
    /// Members the commit requires had not durably acknowledged the prepare
    /// when the wait ended.
    AcknowledgementsMissing {
        required: usize,
        acknowledged: usize,
        missing: Vec<NodeId>,
    },
    /// The shard's live write set is smaller than this commit needs.
    BelowWriteSetFloor { write_set: usize, floor: usize },
    /// The commit's content W exceeds the W the shard record has accepted
    /// (`accepted`), so the shard's N must first rise to `intent_w`.
    ReplicationBelowIntent { accepted: usize, intent_w: usize },
    /// The shard record moved to another epoch or primary.
    RecordChanged { epoch: u64 },
    /// The caller's deadline ended the wait.
    Deadline,
    /// The caller or the host cancelled the attempt.
    Cancelled,
    /// Any other failure after the prepare was durable.
    Interrupted { detail: String },
}

impl std::fmt::Display for DfsCommitOutcomeUnknownCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgementsMissing {
                required,
                acknowledged,
                missing,
            } => write!(
                f,
                "{acknowledged} of {required} required acknowledgements, missing {missing:?}"
            ),
            Self::BelowWriteSetFloor { write_set, floor } => {
                write!(
                    f,
                    "write set of {write_set} is below the commit floor {floor}"
                )
            }
            Self::ReplicationBelowIntent { accepted, intent_w } => write!(
                f,
                "the shard accepts content W {accepted}; its N must rise to {intent_w} first"
            ),
            Self::RecordChanged { epoch } => write!(f, "shard record moved to epoch {epoch}"),
            Self::Deadline => f.write_str("caller deadline reached"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Interrupted { detail } => write!(f, "interrupted: {detail}"),
        }
    }
}

impl DfsError {
    /// Whether this error reports a durably prepared change whose outcome is
    /// not yet known.
    pub fn is_commit_outcome_unknown(&self) -> bool {
        matches!(self, Self::MetadataCommitOutcomeUnknown { .. })
    }

    /// Whether the caller's cancellation ended this work, including a commit
    /// wait whose durably prepared change is left with an unknown outcome.
    /// Cancellation stops the caller's pass; it is not an outcome to move past.
    pub fn is_cancellation(&self) -> bool {
        matches!(
            self,
            Self::Cancelled { .. }
                | Self::MetadataCommitOutcomeUnknown {
                    cause: crate::DfsCommitOutcomeUnknownCause::Cancelled,
                    ..
                }
        )
    }

    pub(crate) fn metadata_rejection_code(&self) -> crate::DfsMetadataRejectionCode {
        match self {
            Self::MetadataRejection { code, .. } => *code,
            _ => crate::DfsMetadataRejectionCode::Other,
        }
    }

    pub(crate) fn from_network(error: sinomo_core::network::NetworkError) -> Self {
        let message = error.to_string();
        match error.delivery_state() {
            Some(delivery_state) => Self::NetworkDelivery {
                message,
                delivery_state,
            },
            None => Self::Network { message },
        }
    }
}

pub(crate) fn dfs_replica_not_desired_reason(intent_id: &str) -> String {
    format!("{DFS_REPLICA_NOT_DESIRED_REASON_PREFIX}{intent_id}")
}

pub(crate) fn dfs_rejection_is_replica_not_desired(
    reason: &str,
    expected_requester_node_id: &NodeId,
    expected_intent_id: &str,
) -> bool {
    reason
        == DfsError::UnauthorizedReplicaCommand {
            source_node_id: expected_requester_node_id.clone(),
            reason: dfs_replica_not_desired_reason(expected_intent_id),
        }
        .to_string()
}

pub(crate) fn dfs_rejection_is_commit_content_repair_pending(
    reason: &str,
    expected_object_id: &DfsObjectId,
    expected_version: &DfsVersion,
    expected_content_hash: &str,
) -> bool {
    reason
        == DfsError::CommitContentRepairPending {
            object_id: expected_object_id.clone(),
            version: expected_version.clone(),
            content_hash: expected_content_hash.to_string(),
        }
        .to_string()
}

fn dfs_error_current_primary(error: &DfsError) -> Option<NodeId> {
    match error {
        DfsError::StaleOwnerEpoch {
            primary_node_id, ..
        } => Some(primary_node_id.clone()),
        _ => None,
    }
}

pub(crate) fn distributed_write_rejection_current_primary(error: &DfsError) -> Option<NodeId> {
    dfs_error_current_primary(error)
}

pub(crate) fn namespace_rejection_current_primary(
    error: &crate::posix_model::DfsPosixError,
) -> Option<NodeId> {
    match error {
        crate::posix_model::DfsPosixError::Dfs(error) => dfs_error_current_primary(error),
        _ => None,
    }
}

/// Reconstructs the retriable `Busy` class from a routed-write rejection
/// reason, so `EAGAIN` semantics survive the string-typed wire result.
pub(crate) fn routed_write_rejection_error(reason: &str) -> DfsError {
    if let Some(path) = reason.strip_prefix("path already exists: ") {
        return DfsError::AlreadyExists {
            path: path.to_string(),
        };
    }
    if reason.contains("is moving between metadata shards") {
        let path = reason
            .strip_prefix("path ")
            .and_then(|rest| rest.split(" is moving between metadata shards").next())
            .unwrap_or(reason)
            .to_string();
        return DfsError::Busy { path };
    }
    DfsError::Distributed {
        message: reason.to_string(),
    }
}

impl From<ContentStoreError> for DfsError {
    fn from(error: ContentStoreError) -> Self {
        match error {
            ContentStoreError::StoragePreparationPending {
                root,
                processed_records,
            } => Self::StoragePreparationPending {
                root,
                processed_records,
            },
            error => Self::ContentStore {
                message: error.to_string(),
            },
        }
    }
}

impl From<DfsDistributedError> for DfsError {
    fn from(error: DfsDistributedError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

impl From<MetadataBackendError> for DfsError {
    fn from(error: MetadataBackendError) -> Self {
        match error {
            MetadataBackendError::StoragePreparationPending {
                root,
                processed_records,
            } => Self::StoragePreparationPending {
                root,
                processed_records,
            },
            error @ (MetadataBackendError::Rejected { .. }
            | MetadataBackendError::SourceEpochFenced { .. }) => Self::MetadataRejection {
                code: error.rejection_code(),
                message: error.to_string(),
            },
            MetadataBackendError::WritePreconditionFailed { failure } => Self::WritePreconditionFailed { failure },
            MetadataBackendError::ReceiptGenerationSuperseded {
                object_id,
                version,
                replica_node_id,
                expected,
                current,
            } => Self::ReceiptGenerationSuperseded {
                object_id,
                version,
                replica_node_id,
                expected,
                current,
            },
            MetadataBackendError::AlreadyExists { path } => Self::AlreadyExists { path },
            MetadataBackendError::DirectoryNotEmpty { path } => Self::DirectoryNotEmpty { path },
            error => Self::Metadata {
                message: error.to_string(),
            },
        }
    }
}

impl From<KvError> for DfsError {
    fn from(error: KvError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::network::NetworkError;

    #[test]
    fn network_conversion_preserves_possibly_applied_delivery_state() {
        let error = DfsError::from_network(NetworkError::delivery_failed(
            "stable DFS write",
            NetworkDeliveryState::PossiblyApplied,
            "response stream closed",
        ));
        assert!(matches!(
            error,
            DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }
        ));
    }

    #[test]
    fn network_conversion_preserves_definitely_not_sent_delivery_state() {
        let error = DfsError::from_network(NetworkError::delivery_failed(
            "stable DFS write",
            NetworkDeliveryState::DefinitelyNotSent,
            "route unavailable",
        ));
        assert!(matches!(
            error,
            DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ));
    }
}
