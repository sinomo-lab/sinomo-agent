//! Shared metadata backend error and result types.
//!
//! Metadata helper modules use this module instead of depending on the
//! concrete local backend implementation.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};

/// Error returned by DFS metadata backends and metadata helper modules.
#[derive(Debug, Error)]
pub enum MetadataBackendError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("metadata serialization error: {0}")]
    Serde(String),
    #[error("filesystem not found: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("object not found: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error(
        "metadata transaction conflict for object {object_id}: expected {expected}, current {current}"
    )]
    Conflict {
        object_id: DfsObjectId,
        expected: DfsVersion,
        current: DfsVersion,
    },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
}

pub type Result<T> = std::result::Result<T, MetadataBackendError>;
