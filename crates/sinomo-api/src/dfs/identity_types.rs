//! Shared DFS wire values and validation; no storage or service runtime.

/// Identifier for a DFS filesystem such as the default `sifs`.
pub type DfsFileSystemId = sinomo_core::Id;

/// Identifier for a DFS object within a filesystem.
pub type DfsObjectId = sinomo_core::Id;

/// Identifier for a committed DFS object version.
pub type DfsVersion = sinomo_core::Id;
