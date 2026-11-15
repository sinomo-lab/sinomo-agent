//! POSIX facade data types and error mapping.

use crate::content_store::ObjectVersionReader;
use crate::metadata_error::MetadataBackendError;
use crate::metadata_records::{DfsDirectoryPageCursor, DfsNamespaceJobKind};
use crate::protocol::DfsNamespacePosixError;
use crate::{
    DfsError, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsReplicationPolicy,
    DfsVerifiedReadStream, DfsVersion,
};
use serde::{Deserialize, Serialize};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DfsPosixError {
    #[error("DFS error: {0}")]
    Dfs(DfsError),
    #[error("metadata backend error: {0}")]
    Metadata(MetadataBackendError),
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
    #[error("symlink content is temporarily unavailable at {path} during {stage}: {detail}")]
    SymlinkContentPending {
        path: String,
        stage: String,
        detail: String,
    },
    #[error("write conflicted at {path}: {reason}")]
    Conflict { path: String, reason: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
    #[error(
        "bulk namespace commit was only partially observed: committed={committed_metadata_paths:?}, indeterminate={indeterminate_metadata_paths:?}, unattempted={unattempted_metadata_paths:?}: {source}"
    )]
    PartialBulkCommit {
        committed_metadata_paths: Vec<String>,
        indeterminate_metadata_paths: Vec<String>,
        unattempted_metadata_paths: Vec<String>,
        #[source]
        source: Box<DfsPosixError>,
    },
}

pub type Result<T> = std::result::Result<T, DfsPosixError>;

impl From<DfsError> for DfsPosixError {
    fn from(error: DfsError) -> Self {
        match error {
            DfsError::DirectoryNotEmpty { path } => DfsPosixError::DirectoryNotEmpty { path },
            DfsError::AlreadyExists { path } => DfsPosixError::AlreadyExists { path },
            DfsError::StaleDirectoryVersion {
                path,
                expected_version,
                current_version,
            } => DfsPosixError::Conflict {
                path,
                reason: format!(
                    "parent directory changed from version {expected_version} to {current_version}"
                ),
            },
            error => DfsPosixError::Dfs(error),
        }
    }
}

impl From<MetadataBackendError> for DfsPosixError {
    fn from(error: MetadataBackendError) -> Self {
        match error {
            MetadataBackendError::DirectoryNotEmpty { path } => {
                DfsPosixError::DirectoryNotEmpty { path }
            }
            error => DfsPosixError::Metadata(error),
        }
    }
}

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
    /// The visible object version captured with the directory page.
    pub version: DfsVersion,
    /// Best-effort visible content length. Directory listing remains available if an older or
    /// partially repaired version record cannot supply a size yet.
    pub size_bytes: Option<u64>,
    pub modified_at_unix_nanos: u64,
    pub policy: DfsObjectPolicy,
    /// Nodes with a durable copy of this exact visible version.
    pub current_locations: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDirEntryPage {
    pub entries: Vec<DfsDirEntry>,
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPosixFilePrefixRead {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

/// Typed result for a distributed POSIX file read.
///
/// Namespace absence is distinct from a file whose authoritative metadata is
/// visible but whose exact content version is not currently readable. Most
/// POSIX compatibility callers still consume [`DfsPosixError::NotFound`], but
/// service boundaries that expose optional reads must retain this distinction
/// so temporary replica unavailability cannot become a false empty result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsPosixDistributedFileReadOutcome {
    Found { bytes: Vec<u8> },
    NotFound { path: String },
    ContentUnavailable { path: String, detail: String },
}

/// Typed result for a bounded distributed POSIX file read.
///
/// This is the prefix-read counterpart of
/// [`DfsPosixDistributedFileReadOutcome`]. A missing content replica after
/// authoritative metadata resolution is an availability failure, not proof
/// that the namespace path is absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsPosixDistributedFilePrefixReadOutcome {
    Found { read: DfsPosixFilePrefixRead },
    NotFound { path: String },
    ContentUnavailable { path: String, detail: String },
}

/// Typed result for a bounded range read through a no-link scoped open.
///
/// The successful variant carries the exact metadata receipt captured by the
/// guarded namespace walk. Once that receipt exists, a missing exact content
/// version is an availability failure rather than proof that the path is
/// absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsPosixDistributedOpenedFileRangeReadOutcome {
    Found {
        stat: DfsStat,
        read: DfsPosixFilePrefixRead,
    },
    NotFound {
        path: String,
    },
    ContentUnavailable {
        path: String,
        detail: String,
    },
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

/// Remote POSIX full-file read retaining the object-RPC owned-chunk stream.
///
/// Callers should consume `stream.next_chunk()` and await its finalizer.
/// [`Self::collect_to_vec`] is the explicit compatibility path for APIs which
/// still require one contiguous byte vector.
#[derive(Debug)]
pub struct DfsPosixRemoteFileReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub size_bytes: usize,
    pub stream: DfsVerifiedReadStream,
}

impl DfsPosixRemoteFileReadStream {
    pub async fn collect_to_vec(self) -> Result<Vec<u8>> {
        self.stream
            .collect_to_vec(self.size_bytes)
            .await
            .map(|(bytes, _completion)| bytes)
            .map_err(DfsError::from_network)
            .map_err(Into::into)
    }
}

/// Remote POSIX range read retaining the same object-RPC owned-chunk stream as
/// a full read.
#[derive(Debug)]
pub struct DfsPosixRemoteFileRangeReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub offset: u64,
    pub total_size_bytes: u64,
    pub range_size_bytes: usize,
    pub stream: DfsVerifiedReadStream,
}

impl DfsPosixRemoteFileRangeReadStream {
    pub async fn collect_to_vec(self) -> Result<Vec<u8>> {
        self.stream
            .collect_to_vec(self.range_size_bytes)
            .await
            .map(|(bytes, _completion)| bytes)
            .map_err(DfsError::from_network)
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteOptions {
    pub policy: DfsReplicationPolicy,
    pub replica_candidates: Vec<NodeId>,
    pub wait_for_commit: bool,
    pub foreground_replica_repair: bool,
}

/// POSIX attributes installed as part of creating a namespace object.
///
/// These attributes travel with the initial metadata object so a file,
/// directory, or symlink is never visible with a temporary default owner or
/// mode before a follow-up chmod/chown reaches the metadata authority.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixCreateAttrs {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl DfsPosixCreateAttrs {
    pub const fn new(mode: u32, uid: u32, gid: u32) -> Self {
        Self {
            mode: mode & 0o7777,
            uid,
            gid,
        }
    }

    pub const fn file_default() -> Self {
        Self::new(0o644, 0, 0)
    }

    pub const fn directory_default() -> Self {
        Self::new(0o755, 0, 0)
    }

    pub const fn symlink_default() -> Self {
        Self::new(0o777, 0, 0)
    }
}

/// Durability barrier requested through the POSIX DFS surface.
///
/// `FileData` is the `fdatasync(2)` contract. The current metadata layout
/// journals essential and ancillary file metadata atomically, so its physical
/// barrier is intentionally as strong as `File` for now while the distinction
/// remains explicit at the API boundary. `Directory` certifies namespace
/// mutations and never claims file-content durability.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPosixSyncKind {
    FileData,
    File,
    Directory,
}

impl DfsPosixSyncKind {
    pub const fn is_file(self) -> bool {
        matches!(self, Self::FileData | Self::File)
    }
}

/// Immutable file prefix captured before a file sync begins.
///
/// The stable object id keeps the target meaningful across a rename. The
/// version and content hash prevent a retry from silently certifying different
/// bytes under the same request payload.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixFileSyncTarget {
    pub kind: DfsPosixSyncKind,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    pub version: DfsVersion,
    pub content_hash: String,
}

/// Immutable directory generation captured before a directory sync begins.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixDirectorySyncTarget {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    pub version: DfsVersion,
}

/// Durable content evidence checked before a file sync can certify metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPosixFileSyncEvidence {
    pub intent_id: String,
    pub required_receipts: usize,
    pub receipt_node_ids: Vec<NodeId>,
}

impl DfsWriteOptions {
    pub fn replicated_eventual(replica_candidates: Vec<NodeId>) -> Self {
        let desired = replica_candidates.len().clamp(1, 2);
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
        DfsPosixError::SymlinkContentPending {
            path,
            stage,
            detail,
        } => Some(DfsNamespacePosixError::SymlinkContentPending {
            path: path.clone(),
            stage: stage.clone(),
            detail: detail.clone(),
        }),
        DfsPosixError::Dfs(DfsError::Busy { path }) => {
            Some(DfsNamespacePosixError::Busy { path: path.clone() })
        }
        DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
            file_system_id,
            shard_id,
            epoch,
            operation_id,
            cause,
            current_primary_node_id,
        }) => Some(DfsNamespacePosixError::CommitOutcomeUnknown {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            epoch: *epoch,
            operation_id: operation_id.clone(),
            cause: cause.clone(),
            current_primary_node_id: current_primary_node_id.clone(),
        }),
        DfsPosixError::Dfs(_)
        | DfsPosixError::Metadata(_)
        | DfsPosixError::PendingWrite { .. }
        | DfsPosixError::PartialBulkCommit { .. } => None,
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
        Some(DfsNamespacePosixError::SymlinkContentPending {
            path,
            stage,
            detail,
        }) => DfsPosixError::SymlinkContentPending {
            path,
            stage,
            detail,
        },
        Some(DfsNamespacePosixError::Busy { path }) => DfsPosixError::Dfs(DfsError::Busy { path }),
        Some(DfsNamespacePosixError::CommitOutcomeUnknown {
            file_system_id,
            shard_id,
            epoch,
            operation_id,
            cause,
            current_primary_node_id,
        }) => DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
            file_system_id,
            shard_id,
            epoch,
            operation_id,
            cause,
            current_primary_node_id,
        }),
        None => DfsPosixError::Dfs(DfsError::Distributed { message: reason }),
    }
}

#[cfg(test)]
mod commit_outcome_unknown_tests {
    use super::*;

    #[test]
    fn routed_namespace_rejection_keeps_an_unknown_outcome_typed() {
        let unknown = DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
            file_system_id: crate::DfsFileSystemId::generate(),
            shard_id: "root".into(),
            epoch: 3,
            operation_id: "namespace-request:op".into(),
            cause: crate::DfsCommitOutcomeUnknownCause::Deadline,
            current_primary_node_id: None,
        });
        let wire = namespace_posix_error_for(&unknown).expect("typed on the wire");
        assert!(matches!(
            wire,
            DfsNamespacePosixError::CommitOutcomeUnknown { .. }
        ));
        assert_eq!(
            posix_error_for_namespace_rejection(unknown.to_string(), Some(wire)).to_string(),
            unknown.to_string()
        );
    }

    #[test]
    fn routed_namespace_rejection_carries_the_current_primary_it_learned() {
        let primary = sinomo_core::node::NodeId::from_legacy_named("node_new_primary").unwrap();
        let unknown = DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
            file_system_id: crate::DfsFileSystemId::generate(),
            shard_id: "root".into(),
            epoch: 3,
            operation_id: "namespace-request:op".into(),
            cause: crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch: 4 },
            current_primary_node_id: Some(primary.clone()),
        });
        let wire = namespace_posix_error_for(&unknown).expect("typed on the wire");
        let encoded = serde_json::to_vec(&wire).unwrap();
        let decoded = serde_json::from_slice::<DfsNamespacePosixError>(&encoded).unwrap();
        assert_eq!(decoded, wire);
        match posix_error_for_namespace_rejection(unknown.to_string(), Some(decoded)) {
            DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
                current_primary_node_id,
                ..
            }) => assert_eq!(current_primary_node_id, Some(primary)),
            other => panic!("{other:?}"),
        }
        // A decoder that predates the field ignores it.
        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "kebab-case")]
        #[allow(dead_code)]
        enum PreChangeError {
            CommitOutcomeUnknown {
                file_system_id: crate::DfsFileSystemId,
                shard_id: String,
                epoch: u64,
                operation_id: String,
                cause: crate::DfsCommitOutcomeUnknownCause,
            },
        }
        assert!(serde_json::from_slice::<PreChangeError>(&encoded).is_ok());
    }
}
