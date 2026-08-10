//! Shared metadata backend error and result types.
//!
//! Metadata helper modules use this module instead of depending on the
//! concrete local backend implementation.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

use crate::{
    DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsOperatorStatusOmission, DfsVersion,
};

/// Error returned by DFS metadata backends and metadata helper modules.
#[derive(Debug, Error)]
pub enum MetadataBackendError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("metadata serialization error: {0}")]
    Serde(String),
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
