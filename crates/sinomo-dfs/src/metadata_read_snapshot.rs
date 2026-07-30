//! Lock-free read snapshots over shard-local metadata tables.
//!
//! Writers keep the live `DfsMetadataTables` in memory. Readers can borrow it
//! when the durable snapshot/log fingerprints still match, or build/cache an
//! owned table snapshot when another process has appended table batches.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::DfsMetadataPrefixProof;
use crate::metadata_table_store::{
    metadata_table_checkpoint_log_path, metadata_table_path, metadata_table_snapshot_fingerprint,
    read_metadata_table_state, read_metadata_table_state_lenient,
    replay_metadata_table_batches_from,
};
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, DfsMetadataTablesSnapshot,
    MetadataTableReadSnapshotCache, MetadataTableSnapshotFingerprint, max_optional_sequence,
};

#[cfg(test)]
thread_local! {
    static METADATA_DURABLE_TABLE_PROBES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_CHECKPOINT_SNAPSHOT_DECODES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_metadata_durable_table_probes() {
    METADATA_DURABLE_TABLE_PROBES.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_durable_table_probes() -> usize {
    METADATA_DURABLE_TABLE_PROBES.get()
}

#[cfg(test)]
fn reset_metadata_checkpoint_snapshot_decodes() {
    METADATA_CHECKPOINT_SNAPSHOT_DECODES.set(0);
}

#[cfg(test)]
fn metadata_checkpoint_snapshot_decodes() -> usize {
    METADATA_CHECKPOINT_SNAPSHOT_DECODES.get()
}

pub(crate) fn refresh_metadata_tables(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,
    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    if metadata_table_checkpoint_log_path(root).is_file() {
        let state = read_metadata_table_state(root)?;
        *tables = state.tables;
        *table_log_offset = state.table_log_offset;
        *materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        *table_snapshot_fingerprint = state.snapshot_fingerprint;
        *prefix_proof = state.prefix_proof;
        return Ok(());
    }
    reload_metadata_tables_if_snapshot_changed(
        root,
        tables,
        table_log_offset,
        materialized_through_sequence,
        table_snapshot_fingerprint,
        prefix_proof,
    )?;
    let path = metadata_table_path(root);
    let len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    if len < *table_log_offset {
        let state = read_metadata_table_state(root)?;
        *tables = state.tables;
        *table_log_offset = state.table_log_offset;
        *materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        *table_snapshot_fingerprint = state.snapshot_fingerprint;
        *prefix_proof = state.prefix_proof;
        return Ok(());
    }
    if len == *table_log_offset {
        return Ok(());
    }
    let replay = replay_metadata_table_batches_from(root, &path, *table_log_offset, tables)?;
    *table_log_offset = replay.offset;
    if let Some(sequence) = replay.materialized_through_sequence {
        *materialized_through_sequence = (*materialized_through_sequence).max(sequence);
    }
    if replay.prefix_proof.is_some() {
        *prefix_proof = replay.prefix_proof;
    }
    Ok(())
}

pub(crate) fn current_tables_snapshot<'a>(
    root: &Path,
    live_tables: &'a DfsMetadataTables,
    table_log_offset: u64,
    materialized_through_sequence: u64,
    table_snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
) -> Result<DfsMetadataTablesSnapshot<'a>> {
    #[cfg(test)]
    METADATA_DURABLE_TABLE_PROBES.set(METADATA_DURABLE_TABLE_PROBES.get().saturating_add(1));
    let checkpoint_fingerprint = metadata_table_checkpoint_fingerprint(root)?;
    if checkpoint_fingerprint.is_some() {
        let path = metadata_table_path(root);
        let current_snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
        let table_log_offset = metadata_table_log_len(&path)?;
        if let Some(tables) = cached_or_advanced_read_snapshot(
            read_snapshot_cache,
            root,
            &path,
            &current_snapshot_fingerprint,
            &checkpoint_fingerprint,
            table_log_offset,
        )? {
            return Ok(DfsMetadataTablesSnapshot::Shared(tables));
        }
        #[cfg(test)]
        METADATA_CHECKPOINT_SNAPSHOT_DECODES
            .set(METADATA_CHECKPOINT_SNAPSHOT_DECODES.get().saturating_add(1));
        return Ok(snapshot_from_state(
            read_snapshot_cache,
            read_metadata_table_state_lenient(root)?,
            checkpoint_fingerprint,
        ));
    }
    let path = metadata_table_path(root);
    let current_snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => Some(metadata),
        Err(source) if source.kind() == io::ErrorKind::NotFound => None,
        Err(source) => {
            return Err(MetadataBackendError::Io { path, source });
        }
    };
    if current_snapshot_fingerprint == *table_snapshot_fingerprint {
        match metadata.as_ref().map(fs::Metadata::len) {
            None => return Ok(DfsMetadataTablesSnapshot::Borrowed(live_tables)),
            Some(len) if len == table_log_offset => {
                return Ok(DfsMetadataTablesSnapshot::Borrowed(live_tables));
            }
            Some(len) => {
                if let Some(tables) = cached_or_advanced_read_snapshot(
                    read_snapshot_cache,
                    root,
                    &path,
                    &current_snapshot_fingerprint,
                    &None,
                    len,
                )? {
                    return Ok(DfsMetadataTablesSnapshot::Shared(tables));
                }
                if len < table_log_offset {
                    let state = read_metadata_table_state_lenient(root)?;
                    return Ok(snapshot_from_state(read_snapshot_cache, state, None));
                }
                let mut tables = live_tables.clone();
                let replay =
                    replay_metadata_table_batches_from(root, &path, table_log_offset, &mut tables)?;
                let state = DfsMetadataTableState {
                    tables,
                    table_log_offset: replay.offset,
                    materialized_through_sequence: max_optional_sequence(
                        Some(materialized_through_sequence),
                        replay.materialized_through_sequence,
                    ),
                    snapshot_format_version: None,
                    snapshot_fingerprint: current_snapshot_fingerprint,
                    prefix_proof: replay.prefix_proof,
                };
                return Ok(snapshot_from_state(read_snapshot_cache, state, None));
            }
        }
    }
    let table_log_offset = metadata.as_ref().map(fs::Metadata::len).unwrap_or(0);
    if let Some(tables) = cached_or_advanced_read_snapshot(
        read_snapshot_cache,
        root,
        &path,
        &current_snapshot_fingerprint,
        &None,
        table_log_offset,
    )? {
        return Ok(DfsMetadataTablesSnapshot::Shared(tables));
    }
    let state = read_metadata_table_state_lenient(root)?;
    Ok(snapshot_from_state(read_snapshot_cache, state, None))
}

fn cached_or_advanced_read_snapshot(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    root: &Path,
    path: &Path,
    snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    checkpoint_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    table_log_offset: u64,
) -> Result<Option<Arc<DfsMetadataTables>>> {
    let Ok(mut cache) = read_snapshot_cache.lock() else {
        return Ok(None);
    };
    let Some(cache) = cache.as_mut() else {
        return Ok(None);
    };
    if cache.snapshot_fingerprint != *snapshot_fingerprint
        || cache.checkpoint_fingerprint != *checkpoint_fingerprint
        || cache.table_log_offset > table_log_offset
    {
        return Ok(None);
    }
    if cache.table_log_offset < table_log_offset {
        // A long-lived reader may trail an external writer by many batches.
        // Advance the owned snapshot instead of cloning the complete live
        // metadata tables on every observation. Arc::make_mut preserves
        // snapshot isolation for any concurrent reader that still holds the
        // previous view, while the common sequential path reuses allocations.
        let tables = Arc::make_mut(&mut cache.tables);
        let replay =
            replay_metadata_table_batches_from(root, path, cache.table_log_offset, tables)?;
        cache.table_log_offset = replay.offset;
    }
    Ok(Some(Arc::clone(&cache.tables)))
}

fn snapshot_from_state<'a>(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    state: DfsMetadataTableState,
    checkpoint_fingerprint: Option<MetadataTableSnapshotFingerprint>,
) -> DfsMetadataTablesSnapshot<'a> {
    let tables = Arc::new(state.tables);
    if let Ok(mut cache) = read_snapshot_cache.lock() {
        *cache = Some(MetadataTableReadSnapshotCache {
            snapshot_fingerprint: state.snapshot_fingerprint.clone(),
            checkpoint_fingerprint,
            table_log_offset: state.table_log_offset,
            tables: Arc::clone(&tables),
        });
    }
    DfsMetadataTablesSnapshot::Shared(tables)
}

fn metadata_table_checkpoint_fingerprint(
    root: &Path,
) -> Result<Option<MetadataTableSnapshotFingerprint>> {
    metadata_table_file_fingerprint(&metadata_table_checkpoint_log_path(root))
}

fn metadata_table_file_fingerprint(
    path: &Path,
) -> Result<Option<MetadataTableSnapshotFingerprint>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            });
        }
    };
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    Ok(Some(MetadataTableSnapshotFingerprint {
        len: metadata.len(),
        modified_unix_nanos,
    }))
}

fn metadata_table_log_len(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(source) => Err(MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn reload_metadata_tables_if_snapshot_changed(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,
    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    let current = metadata_table_snapshot_fingerprint(root)?;
    if current == *table_snapshot_fingerprint {
        return Ok(());
    }
    let state = read_metadata_table_state(root)?;
    *tables = state.tables;
    *table_log_offset = state.table_log_offset;
    *materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
    *table_snapshot_fingerprint = state.snapshot_fingerprint;
    *prefix_proof = state.prefix_proof;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};

    use sinomo_core::node::NodeId;

    use super::*;
    use crate::metadata_records::{DfsMetadataTableBatch, DfsMetadataTableOp};
    use crate::metadata_table_store::{
        append_metadata_table_batch, metadata_table_checkpoint_log_path, metadata_table_path,
        persist_metadata_table_snapshot, rotate_metadata_table_log_for_checkpoint,
    };
    use crate::{DfsFileSystem, DfsFileSystemId};

    fn file_system(label: &str, display_name_bytes: usize) -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named(format!("fs_{label}"))
                .expect("valid filesystem id"),
            display_name: label.repeat(display_name_bytes.max(1)),
            owner_node_id: NodeId::from_legacy_named("node_owner").expect("valid owner node"),
            subtree_policies: Vec::new(),
        }
    }

    fn append_file_system(root: &Path, sequence: u64, file_system: DfsFileSystem) -> u64 {
        append_metadata_table_batch(
            root,
            &DfsMetadataTableBatch {
                sequence,
                ops: vec![DfsMetadataTableOp::PutFileSystem { file_system }],
                prefix_proof: None,
            },
        )
        .expect("append metadata table batch")
    }

    fn shared_tables(snapshot: DfsMetadataTablesSnapshot<'_>) -> Arc<DfsMetadataTables> {
        match snapshot {
            DfsMetadataTablesSnapshot::Shared(tables) => tables,
            DfsMetadataTablesSnapshot::Borrowed(_) => {
                panic!("rotated checkpoint snapshot must be owned")
            }
        }
    }

    fn setup_rotated_checkpoint(
        root: &Path,
        checkpoint_file_system: DfsFileSystem,
    ) -> Option<MetadataTableSnapshotFingerprint> {
        persist_metadata_table_snapshot(root, &DfsMetadataTables::default(), None, None)
            .expect("persist empty metadata snapshot");
        append_file_system(root, 1, checkpoint_file_system);
        assert!(
            rotate_metadata_table_log_for_checkpoint(root, 0)
                .expect("rotate metadata table checkpoint"),
            "non-empty active table log must rotate"
        );
        metadata_table_snapshot_fingerprint(root).expect("snapshot fingerprint")
    }

    #[test]
    fn checkpoint_snapshot_cache_decodes_once_and_advances_active_suffix() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-read-snapshot-cache");
        let checkpoint_file_system = file_system("checkpoint", 1);
        let suffix_file_system = file_system("suffix", 1);
        let snapshot_fingerprint =
            setup_rotated_checkpoint(root.path(), checkpoint_file_system.clone());
        let live_tables = DfsMetadataTables::default();
        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();

        let first = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("read rotated checkpoint snapshot"),
        );
        assert!(
            first
                .file_systems
                .contains_key(&checkpoint_file_system.file_system_id)
        );
        let first_ptr = Arc::as_ptr(&first);
        drop(first);

        let repeated = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("reuse rotated checkpoint snapshot"),
        );
        assert_eq!(Arc::as_ptr(&repeated), first_ptr);
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 1);
        drop(repeated);

        append_file_system(root.path(), 2, suffix_file_system.clone());
        let advanced = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("advance cached snapshot through active suffix"),
        );
        assert_eq!(Arc::as_ptr(&advanced), first_ptr);
        assert!(
            advanced
                .file_systems
                .contains_key(&checkpoint_file_system.file_system_id)
        );
        assert!(
            advanced
                .file_systems
                .contains_key(&suffix_file_system.file_system_id)
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            1,
            "active suffix replay must not decode the immutable checkpoint again"
        );
    }

    #[test]
    fn checkpoint_snapshot_cache_invalidates_changed_removed_and_shrunk_generations() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-metadata-checkpoint-read-snapshot-invalidation",
        );
        let original_checkpoint = file_system("original", 1);
        let replacement_checkpoint = file_system("replacement", 17);
        let first_suffix = file_system("first_suffix", 1);
        let removed_suffix = file_system("removed_suffix", 1);
        let snapshot_fingerprint =
            setup_rotated_checkpoint(root.path(), original_checkpoint.clone());
        let first_suffix_len = append_file_system(root.path(), 2, first_suffix.clone());
        let live_tables = DfsMetadataTables::default();
        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();

        let initial = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("cache original checkpoint generation"),
        );
        assert!(
            initial
                .file_systems
                .contains_key(&original_checkpoint.file_system_id)
        );
        drop(initial);
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 1);

        let replacement_root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-replacement");
        append_file_system(replacement_root.path(), 1, replacement_checkpoint.clone());
        let checkpoint_path = metadata_table_checkpoint_log_path(root.path());
        let checkpoint_fingerprint_before =
            metadata_table_checkpoint_fingerprint(root.path()).expect("checkpoint fingerprint");
        fs::copy(
            metadata_table_path(replacement_root.path()),
            &checkpoint_path,
        )
        .expect("replace immutable checkpoint generation");
        let checkpoint_fingerprint_after =
            metadata_table_checkpoint_fingerprint(root.path()).expect("replacement fingerprint");
        assert_ne!(
            checkpoint_fingerprint_after, checkpoint_fingerprint_before,
            "replacement fixture must change the checkpoint fingerprint"
        );

        let replaced = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("reload changed checkpoint generation"),
        );
        assert!(
            replaced
                .file_systems
                .contains_key(&replacement_checkpoint.file_system_id)
        );
        assert!(
            !replaced
                .file_systems
                .contains_key(&original_checkpoint.file_system_id)
        );
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 2);
        drop(replaced);

        append_file_system(root.path(), 3, removed_suffix.clone());
        let advanced = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("advance replacement checkpoint suffix"),
        );
        assert!(
            advanced
                .file_systems
                .contains_key(&removed_suffix.file_system_id)
        );
        drop(advanced);
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 2);

        OpenOptions::new()
            .write(true)
            .open(metadata_table_path(root.path()))
            .expect("open active table log")
            .set_len(first_suffix_len)
            .expect("shrink active table log to first complete batch");
        let shrunk = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("reload after active log shrink"),
        );
        assert!(
            shrunk
                .file_systems
                .contains_key(&first_suffix.file_system_id)
        );
        assert!(
            !shrunk
                .file_systems
                .contains_key(&removed_suffix.file_system_id)
        );
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 3);
        drop(shrunk);

        fs::remove_file(checkpoint_path).expect("remove installed checkpoint generation");
        let without_checkpoint = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &cache,
            )
            .expect("invalidate checkpoint cache after removal"),
        );
        assert!(
            without_checkpoint
                .file_systems
                .contains_key(&first_suffix.file_system_id)
        );
        assert!(
            !without_checkpoint
                .file_systems
                .contains_key(&replacement_checkpoint.file_system_id)
        );
        assert_eq!(
            cache
                .lock()
                .expect("snapshot cache")
                .as_ref()
                .expect("replacement non-checkpoint cache")
                .checkpoint_fingerprint,
            None
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            3,
            "checkpoint removal must leave the checkpoint decode path"
        );
    }
}
