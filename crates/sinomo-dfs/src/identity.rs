//! Stable DFS identity defaults.
//!
//! These helpers keep legacy local-node and default SIFS identifiers in one
//! place while callers migrate toward explicit cluster-provided identities.

use sinomo_node::NodeId;

use crate::DfsFileSystemId;

/// Legacy fallback node identifier for local-only DFS instances.
pub const DEFAULT_DFS_NODE_ID: &str = "node_local";

/// Default Sinomo filesystem identifier.
pub const SIFS_FILE_SYSTEM_ID: &str = "sifs";

/// Returns the legacy local-only DFS node identifier.
pub fn fallback_dfs_node_id() -> NodeId {
    NodeId::from_legacy_named(DEFAULT_DFS_NODE_ID).expect("valid fallback DFS node id")
}

/// Returns the default Sinomo filesystem identifier.
pub fn sifs_file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("valid sifs filesystem id")
}
