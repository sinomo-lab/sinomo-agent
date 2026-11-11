//! Removal of the node-local repair state that repair no longer has: the
//! repair queue with its original-repair custody records, the retained
//! repair-handoff pages of membership handoffs, and the migration copies of
//! the queue. Repair work derives from the shard tables, so none of it is
//! read. The owner of a data root removes it, by its known paths, when it
//! opens the root. Retained repair payload roots in the content store are
//! records that retain nothing, so content GC reclaims what no other root
//! holds.

use std::path::{Path, PathBuf};

use sinomo_core::local::SinomoDataDir;

/// The queue snapshot, its active log and writer lock; custody and births
/// lived inside them.
const QUEUE_FILES: [&str; 3] = [
    "repair-attempts.sobj",
    "repair-attempts.active.slog",
    "repair-attempts.writer-lock",
];
const MIGRATION_QUEUE_COPIES: [&str; 2] = ["repair-v7", "repair-v8"];
const HANDOFF_PAGES: &str = "repair-handoffs-v1";

fn remove(path: &Path) -> std::io::Result<bool> {
    let removed = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match removed {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Every known path of the removed repair state under `data_dir`.
pub(crate) fn legacy_repair_state_paths(data_dir: &SinomoDataDir) -> Vec<PathBuf> {
    let dfs = data_dir.dfs_dir();
    let mut paths = QUEUE_FILES
        .iter()
        .map(|name| dfs.join("maintenance").join(name))
        .collect::<Vec<_>>();
    paths.extend(MIGRATION_QUEUE_COPIES.iter().map(|name| {
        dfs.join("migration-retained")
            .join("local-state")
            .join(name)
    }));
    if let Ok(shards) = std::fs::read_dir(crate::default_dfs_metadata_shards_root(data_dir)) {
        paths.extend(
            shards
                .flatten()
                .map(|shard| shard.path().join("journal").join(HANDOFF_PAGES)),
        );
    }
    paths
}

/// Remove the removed repair state in one step, then make the removal
/// durable. Returns how many paths existed.
pub(crate) fn remove_legacy_repair_state(data_dir: &SinomoDataDir) -> std::io::Result<usize> {
    let mut removed = 0usize;
    let mut parents = std::collections::BTreeSet::new();
    for path in legacy_repair_state_paths(data_dir) {
        if remove(&path)? {
            removed += 1;
            if let Some(parent) = path.parent() {
                parents.insert(parent.to_path_buf());
            }
        }
    }
    for parent in parents {
        sinomo_core::storage::durable_file::sync_directory(&parent)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }
    if removed > 0 {
        tracing::info!(target: "sinomo.dfs.repair", data_root = %data_dir.root().display(),
            removed, "removed the node-local repair queue and custody state");
    }
    Ok(removed)
}

#[cfg(test)]
#[path = "legacy_repair_state_tests.rs"]
mod tests;
