//! POSIX facade data types and error mapping.

use crate::content_store::ObjectVersionReader;
use crate::metadata_error::MetadataBackendError;
use crate::metadata_records::{DfsDirectoryPageCursor, DfsNamespaceJobKind};
use crate::protocol::DfsNamespacePosixError;
use crate::{
    DfsConflictReadView, DfsError, DfsFileSystemId, DfsObjectId, DfsObjectKind,
    DfsReplicationPolicy, DfsVersion,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DfsPosixError {
    #[error("DFS error: {0}")]
    Dfs(#[from] DfsError),
    #[error("metadata backend error: {0}")]
    Metadata(#[from] MetadataBackendError),
    #[error("not found: {path}")]
    NotFound { path: String },
    #[error("already exists: {path}")]
    AlreadyExists { path: String },
    #[error("not a directory: {path}")]
    NotDirectory { path: String },
    #[error("is a directory: {path}")]
    IsDirectory { path: String },
    #[error("not a symbolic link: {path}")]
    NotSymlink { path: String },
    #[error("parent directory does not exist: {path}")]
    MissingParent { path: String },
    #[error("invalid DFS path: {path}")]
    InvalidPath { path: String },
    #[error(
        "write did not commit yet: {intent_id} ({durable_receipts}/{required_receipts} receipts)"
    )]
    PendingWrite {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    #[error("write conflicted at {path}: {reason}")]
    Conflict { path: String, reason: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
}

pub type Result<T> = std::result::Result<T, DfsPosixError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsStat {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    pub kind: DfsObjectKind,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub link_count: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub policy: DfsObjectPolicy,
    pub current_locations: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: DfsObjectKind,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDirEntryPage {
    pub entries: Vec<DfsDirEntry>,
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Debug)]
pub struct DfsPosixFileReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: u64,
    pub reader: ObjectVersionReader,
}

#[derive(Debug)]
pub struct DfsPosixFileRangeReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub offset: u64,
    pub total_size_bytes: u64,
    pub range_size_bytes: u64,
    pub reader: ObjectVersionReader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsWriteOptions {
    pub policy: DfsReplicationPolicy,
    pub replica_candidates: Vec<NodeId>,
    pub wait_for_commit: bool,
    pub foreground_replica_repair: bool,
}

impl DfsWriteOptions {
    pub fn replicated_eventual(replica_candidates: Vec<NodeId>) -> Self {
        let desired = replica_candidates.len().max(1).min(2);
        Self {
            policy: DfsReplicationPolicy::new(1, desired).expect("valid default policy"),
            replica_candidates,
            wait_for_commit: true,
            foreground_replica_repair: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsNamespaceJobMaintenanceReport {
    pub scanned_job_count: usize,
    pub resumed_job_count: usize,
    pub completed_job_count: usize,
    pub failed_job_count: usize,
    pub remaining_running_job_count: usize,
    pub failures: Vec<DfsNamespaceJobFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNamespaceJobFailure {
    pub job_id: String,
    pub kind: DfsNamespaceJobKind,
    pub reason: String,
}

/// Wire form of a namespace `DfsPosixError` so routed operations surface the
/// same errno semantics as local ones. Transport/internal errors map to
/// `None` and travel as plain rejection reasons.
pub(crate) fn namespace_posix_error_for(error: &DfsPosixError) -> Option<DfsNamespacePosixError> {
    match error {
        DfsPosixError::NotFound { path } => {
            Some(DfsNamespacePosixError::NotFound { path: path.clone() })
        }
        DfsPosixError::AlreadyExists { path } => {
            Some(DfsNamespacePosixError::AlreadyExists { path: path.clone() })
        }
        DfsPosixError::NotDirectory { path } => {
            Some(DfsNamespacePosixError::NotDirectory { path: path.clone() })
        }
        DfsPosixError::IsDirectory { path } => {
            Some(DfsNamespacePosixError::IsDirectory { path: path.clone() })
        }
        DfsPosixError::NotSymlink { path } => {
            Some(DfsNamespacePosixError::NotSymlink { path: path.clone() })
        }
        DfsPosixError::MissingParent { path } => {
            Some(DfsNamespacePosixError::MissingParent { path: path.clone() })
        }
        DfsPosixError::InvalidPath { path } => {
            Some(DfsNamespacePosixError::InvalidPath { path: path.clone() })
        }
        DfsPosixError::DirectoryNotEmpty { path } => {
            Some(DfsNamespacePosixError::DirectoryNotEmpty { path: path.clone() })
        }
        DfsPosixError::Conflict { path, reason } => Some(DfsNamespacePosixError::Conflict {
            path: path.clone(),
            reason: reason.clone(),
        }),
        DfsPosixError::Dfs(DfsError::Busy { path }) => {
            Some(DfsNamespacePosixError::Busy { path: path.clone() })
        }
        DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_) | DfsPosixError::PendingWrite { .. } => {
            None
        }
    }
}

pub(crate) fn posix_error_for_namespace_rejection(
    reason: String,
    posix_error: Option<DfsNamespacePosixError>,
) -> DfsPosixError {
    match posix_error {
        Some(DfsNamespacePosixError::NotFound { path }) => DfsPosixError::NotFound { path },
        Some(DfsNamespacePosixError::AlreadyExists { path }) => {
            DfsPosixError::AlreadyExists { path }
        }
        Some(DfsNamespacePosixError::NotDirectory { path }) => DfsPosixError::NotDirectory { path },
        Some(DfsNamespacePosixError::IsDirectory { path }) => DfsPosixError::IsDirectory { path },
        Some(DfsNamespacePosixError::NotSymlink { path }) => DfsPosixError::NotSymlink { path },
        Some(DfsNamespacePosixError::MissingParent { path }) => {
            DfsPosixError::MissingParent { path }
        }
        Some(DfsNamespacePosixError::InvalidPath { path }) => DfsPosixError::InvalidPath { path },
        Some(DfsNamespacePosixError::DirectoryNotEmpty { path }) => {
            DfsPosixError::DirectoryNotEmpty { path }
        }
        Some(DfsNamespacePosixError::Conflict { path, reason }) => {
            DfsPosixError::Conflict { path, reason }
        }
        Some(DfsNamespacePosixError::Busy { path }) => DfsPosixError::Dfs(DfsError::Busy { path }),
        None => DfsPosixError::Dfs(DfsError::Distributed { message: reason }),
    }
}

pub(crate) fn read_conflict_error(
    path: String,
    conflicts: Vec<DfsConflictReadView>,
) -> DfsPosixError {
    let reason = conflicts
        .into_iter()
        .map(|conflict| conflict.reason)
        .next()
        .unwrap_or_else(|| "object has an open DFS conflict".to_string());
    DfsPosixError::Conflict { path, reason }
}
