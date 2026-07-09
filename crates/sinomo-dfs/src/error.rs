use crate::{
    ContentStoreError, DfsDistributedError, DfsFileSystemId, DfsObjectId, MetadataBackendError,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::kv::KvError;
use sinomo_core::node::NodeId;
use thiserror::Error;

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
    #[error("serialization error: {message}")]
    Serialization { message: String },
    #[error("content store error: {message}")]
    ContentStore { message: String },
    #[error("distributed DFS error: {message}")]
    Distributed { message: String },
    #[error("metadata backend error: {message}")]
    Metadata { message: String },
    #[error("invalid DFS replication policy: {message}")]
    InvalidReplicationPolicy { message: String },
    #[error("content hash mismatch: expected {expected}, actual {actual}")]
    ContentHashMismatch { expected: String, actual: String },
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
    /// The caller cancelled the operation through the service's
    /// [`DfsCancellationFlag`](crate::DfsCancellationFlag) — typically a budgeted
    /// load giving up while the cluster/DFS link is degraded. Distributed wait
    /// and retry loops return this instead of running out their full timeouts.
    #[error("DFS operation cancelled: {operation}")]
    Cancelled { operation: &'static str },
}

impl DfsError {
    pub(crate) fn from_network(error: sinomo_core::network::NetworkError) -> Self {
        Self::Network {
            message: error.to_string(),
        }
    }
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
        Self::ContentStore {
            message: error.to_string(),
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
