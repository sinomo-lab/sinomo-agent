//! Shared metadata backend error and result types.
//!
//! Metadata helper modules use this module instead of depending on the
//! concrete local backend implementation.

use std::io;
use std::path::PathBuf;

use sinomo_core::node::NodeId;
use thiserror::Error;

use crate::{
    DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsOperatorStatusOmission, DfsVersion,
};

/// Error returned by DFS metadata backends and metadata helper modules.
#[derive(Debug, Error)]
pub enum MetadataBackendError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error(
        "metadata storage preparation at {root:?} remains pending after {processed_records} records"
    )]
    StoragePreparationPending {
        root: PathBuf,
        processed_records: usize,
    },
    #[error("storage preparation caller budget exhausted during {work}")]
    PreparationBudgetExhausted { work: &'static str },
    #[error("original {family} {identity} has no immutable source custody for {requirement}")]
    MissingOriginalSourceCustody {
        family: String,
        identity: String,
        requirement: String,
    },
    #[error("write precondition failed: {failure:?}")]
    WritePreconditionFailed {
        failure: crate::DfsWritePreconditionFailure,
    },
    #[error("metadata serialization error: {0}")]
    Serde(String),
    #[error("metadata serialization error: {message}")]
    Rejected {
        code: crate::DfsMetadataRejectionCode,
        message: String,
    },

    #[error("bounded metadata status unavailable: {reason}")]
    StatusUnavailable { reason: String },
    #[error("resident metadata shard {shard_id:?} is not hydrated for bounded status")]
    StatusUnhydrated { shard_id: DfsMetadataShardId },
    #[error("bounded metadata status omitted: {omission}")]
    StatusOmitted { omission: DfsOperatorStatusOmission },
    #[error("filesystem not found: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("object not found: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error("invalid advisory-lock transition: {0}")]
    AdvisoryLock(String),
    #[error("metadata shard {shard_id:?} is frozen for pending membership handoff {handoff_id}")]
    MembershipHandoffFrozen {
        shard_id: DfsMetadataShardId,
        handoff_id: String,
    },
    /// This member promised, or acknowledged under, a later epoch than the
    /// source's: it ingests and acknowledges nothing more from that epoch.
    #[error(
        "metadata shard {shard_id:?} accepts nothing from source epoch {source_epoch}; it is fenced at epoch {fenced_epoch}"
    )]
    SourceEpochFenced {
        shard_id: DfsMetadataShardId,
        source_epoch: u64,
        fenced_epoch: u64,
    },
    /// The receipt slot advanced past the generation its publisher planned
    /// against: a later withdrawal superseded that attempt.
    #[error(
        "receipt slot of object {object_id} version {version} replica {replica_node_id} is at generation {current}, superseding planned generation {expected}"
    )]
    ReceiptGenerationSuperseded {
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        expected: u64,
        current: u64,
    },
    #[error(
        "metadata transaction conflict for object {object_id}: expected {expected}, current {current}"
    )]
    Conflict {
        object_id: DfsObjectId,
        expected: DfsVersion,
        current: DfsVersion,
    },
    #[error("path already exists: {path}")]
    AlreadyExists { path: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
}

pub type Result<T> = std::result::Result<T, MetadataBackendError>;

impl MetadataBackendError {
    pub(crate) fn rejection_code(&self) -> crate::DfsMetadataRejectionCode {
        match self {
            Self::Rejected { code, .. } => *code,
            _ => crate::DfsMetadataRejectionCode::Other,
        }
    }

    pub(crate) fn replica_diverged(message: String) -> Self {
        Self::Rejected {
            code: crate::DfsMetadataRejectionCode::ReplicaDiverged,
            message,
        }
    }

    pub(crate) fn prefix_mismatch(message: String) -> Self {
        Self::Rejected {
            code: crate::DfsMetadataRejectionCode::PrefixMismatch,
            message,
        }
    }
}
