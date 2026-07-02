//! Durable cursor paths and IO for local DFS metadata shards.

use std::path::{Path, PathBuf};

use sinomo_core::node::NodeId;

use crate::metadata_error::Result;
use crate::metadata_io::{read_dir, read_sobj_optional, write_sobj_atomic};
use crate::metadata_journal::{DfsMetadataJournalHead, DfsMetadataReplayCursor};
use crate::metadata_keys::key_segment;
use crate::metadata_records::{DfsMetadataReplayCursorStatus, DfsMetadataShardId};

const LOCAL_MATERIALIZATION_CURSOR_FLUSH_INTERVAL: u64 = 512;

pub(crate) fn records_root(root: &Path) -> PathBuf {
    root.join("records")
}

pub(crate) fn replay_cursor_path(root: &Path, shard_id: &DfsMetadataShardId) -> PathBuf {
    records_root(root)
        .join("replay-cursors")
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn local_materialization_cursor_path(root: &Path) -> PathBuf {
    records_root(root)
        .join("local-materialization")
        .join("cursor.sobj")
}

pub(crate) fn metadata_replication_cursor_path(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    records_root(root)
        .join("metadata-replication-cursors")
        .join(key_segment(target_node_id))
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn journal_head_path(root: &Path) -> PathBuf {
    root.join("journal").join("head.sobj")
}

pub(crate) fn journal_write_lock_path(root: &Path) -> PathBuf {
    root.join("journal").join("write.lock")
}

pub(crate) fn replay_cursor_statuses(root: &Path) -> Result<Vec<DfsMetadataReplayCursorStatus>> {
    let cursor_dir = records_root(root).join("replay-cursors");
    if !cursor_dir.exists() {
        return Ok(Vec::new());
    }
    let mut cursors = Vec::new();
    for entry in read_dir(&cursor_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
            continue;
        }
        if let Some(cursor) = read_sobj_optional::<DfsMetadataReplayCursor>(&path)? {
            cursors.push(DfsMetadataReplayCursorStatus {
                source_shard_id: cursor.shard_id,
                replayed_through_sequence: cursor.last_sequence,
            });
        }
    }
    cursors.sort_by(|left, right| left.source_shard_id.cmp(&right.source_shard_id));
    Ok(cursors)
}

pub(crate) fn read_replay_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> Result<DfsMetadataReplayCursor> {
    Ok(
        read_sobj_optional::<DfsMetadataReplayCursor>(&replay_cursor_path(root, shard_id))?
            .unwrap_or(DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 0,
            }),
    )
}

pub(crate) fn write_replay_cursor(root: &Path, cursor: &DfsMetadataReplayCursor) -> Result<()> {
    write_sobj_atomic(&replay_cursor_path(root, &cursor.shard_id), cursor)
}

pub(crate) fn ensure_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    has_materialized_state: bool,
) -> Result<()> {
    if local_materialization_cursor_path(root).is_file() {
        return Ok(());
    }
    let last_sequence = if has_materialized_state {
        next_sequence.saturating_sub(1)
    } else {
        0
    };
    write_local_materialization_cursor(
        root,
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence,
        },
    )
}

pub(crate) fn local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
) -> Result<DfsMetadataReplayCursor> {
    Ok(
        read_sobj_optional::<DfsMetadataReplayCursor>(&local_materialization_cursor_path(root))?
            .unwrap_or(DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: next_sequence.saturating_sub(1),
            }),
    )
}

pub(crate) fn advance_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    sequence: u64,
) -> Result<()> {
    let mut cursor = local_materialization_cursor(root, shard_id, next_sequence)?;
    if sequence <= cursor.last_sequence {
        return Ok(());
    }
    cursor.last_sequence = sequence;
    write_local_materialization_cursor(root, &cursor)
}

pub(crate) fn reconcile_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    materialized_sequence: u64,
) -> Result<()> {
    if materialized_sequence == 0 {
        return Ok(());
    }
    advance_local_materialization_cursor(
        root,
        shard_id,
        next_sequence,
        materialized_sequence.min(next_sequence.saturating_sub(1)),
    )
}

pub(crate) fn checkpoint_local_materialization_cursor_if_due(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    sequence: u64,
) -> Result<()> {
    let cursor = local_materialization_cursor(root, shard_id, next_sequence)?;
    if sequence <= cursor.last_sequence
        || sequence.saturating_sub(cursor.last_sequence)
            < LOCAL_MATERIALIZATION_CURSOR_FLUSH_INTERVAL
    {
        return Ok(());
    }
    write_local_materialization_cursor(
        root,
        &DfsMetadataReplayCursor {
            shard_id: cursor.shard_id,
            last_sequence: sequence,
        },
    )
}

pub(crate) fn write_local_materialization_cursor(
    root: &Path,
    cursor: &DfsMetadataReplayCursor,
) -> Result<()> {
    write_sobj_atomic(&local_materialization_cursor_path(root), cursor)
}

pub(crate) fn metadata_replication_cursor(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> Result<u64> {
    Ok(
        read_sobj_optional::<DfsMetadataReplayCursor>(&metadata_replication_cursor_path(
            root,
            target_node_id,
            shard_id,
        ))?
        .map(|cursor| cursor.last_sequence)
        .unwrap_or(0),
    )
}

pub(crate) fn put_metadata_replication_cursor(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<()> {
    let current_sequence = metadata_replication_cursor(root, target_node_id, shard_id)?;
    let next_after_sequence = next_after_sequence.max(current_sequence);
    if next_after_sequence == current_sequence {
        return Ok(());
    }
    write_sobj_atomic(
        &metadata_replication_cursor_path(root, target_node_id, shard_id),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: next_after_sequence,
        },
    )
}

pub(crate) fn write_journal_head(root: &Path, next_sequence: u64) -> Result<()> {
    write_sobj_atomic(
        &journal_head_path(root),
        &DfsMetadataJournalHead { next_sequence },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_io::read_sobj;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    #[test]
    fn local_cursor_defaults_to_latest_local_sequence_until_persisted() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-cursor");
        let shard_id = DfsMetadataShardId::new("root");

        let cursor = local_materialization_cursor(&root, &shard_id, 12).expect("cursor");
        assert_eq!(
            cursor,
            DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 11
            }
        );

        write_local_materialization_cursor(
            &root,
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 4,
            },
        )
        .expect("write cursor");
        assert_eq!(
            local_materialization_cursor(&root, &shard_id, 12).expect("persisted cursor"),
            DfsMetadataReplayCursor {
                shard_id,
                last_sequence: 4
            }
        );

        cleanup(&root);
    }

    #[test]
    fn metadata_replication_cursor_is_monotonic() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-replication-cursor");
        let target = node("node_target");
        let shard_id = DfsMetadataShardId::new("root");

        put_metadata_replication_cursor(&root, &target, &shard_id, 42).expect("write cursor");
        put_metadata_replication_cursor(&root, &target, &shard_id, 7).expect("stale cursor");

        assert_eq!(
            metadata_replication_cursor(&root, &target, &shard_id).expect("read cursor"),
            42
        );
        let cursor = read_sobj::<DfsMetadataReplayCursor>(&metadata_replication_cursor_path(
            &root, &target, &shard_id,
        ))
        .expect("durable cursor");
        assert_eq!(cursor.last_sequence, 42);

        cleanup(&root);
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            std::fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
