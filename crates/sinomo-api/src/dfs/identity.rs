//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;

/// Default Sinomo filesystem identifier.
pub const SIFS_FILE_SYSTEM_ID: &str = "sifs";

/// Returns the default Sinomo filesystem identifier.
pub fn sifs_file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("valid sifs filesystem id")
}
