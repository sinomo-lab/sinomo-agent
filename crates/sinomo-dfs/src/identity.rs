//! Stable DFS identity defaults.
//!
//! These helpers keep legacy local-node and default SIFS identifiers in one
//! place while callers migrate toward explicit cluster-provided identities.

use sinomo_core::node::NodeId;

/// Legacy fallback node identifier for local-only DFS instances.
pub const DEFAULT_DFS_NODE_ID: &str = "node_local";

pub use sinomo_api::dfs::identity::SIFS_FILE_SYSTEM_ID;

/// Returns the legacy local-only DFS node identifier.
pub fn fallback_dfs_node_id() -> NodeId {
    NodeId::from_legacy_named(DEFAULT_DFS_NODE_ID).expect("valid fallback DFS node id")
}

pub use sinomo_api::dfs::identity::sifs_file_system_id;
