//! Lock-free read snapshots over shard-local metadata tables.
//!
//! Writers keep the live `DfsMetadataTables` in memory. Readers can borrow it
//! when the durable snapshot/log fingerprints still match, or build/cache an
//! owned table snapshot when another process has appended table batches.

use std::borrow::Cow;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_table_store::{
    metadata_table_path, metadata_table_snapshot_fingerprint, read_metadata_table_state,
    read_metadata_table_state_lenient, replay_metadata_table_batches_from,
};
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, MetadataTableReadSnapshotCache,
    MetadataTableSnapshotFingerprint, max_optional_sequence,
};

pub(crate) fn refresh_metadata_tables(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,
    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
) -> Result<()> {
    reload_metadata_tables_if_snapshot_changed(
        root,
        tables,
        table_log_offset,
        table_snapshot_fingerprint,
    )?;
    let path = metadata_table_path(root);
    if !path.is_file() {
        return Ok(());
    }
    let len = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    if len < *table_log_offset {
        let state = read_metadata_table_state(root)?;
        *tables = state.tables;
        *table_log_offset = state.table_log_offset;
        *materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
        *table_snapshot_fingerprint = state.snapshot_fingerprint;
        return Ok(());
    }
    let replay = replay_metadata_table_batches_from(&path, *table_log_offset, tables)?;
    *table_log_offset = replay.offset;
    if let Some(sequence) = replay.materialized_through_sequence {
        *materialized_through_sequence = (*materialized_through_sequence).max(sequence);
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
) -> Result<Cow<'a, DfsMetadataTables>> {
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
            None => return Ok(Cow::Borrowed(live_tables)),
            Some(len) if len == table_log_offset => return Ok(Cow::Borrowed(live_tables)),
            Some(len) => {
                if let Some(tables) =
                    cached_read_snapshot(read_snapshot_cache, &current_snapshot_fingerprint, len)
                {
                    return Ok(Cow::Owned(tables));
                }
                if len < table_log_offset {
                    let state = read_metadata_table_state_lenient(root)?;
                    store_read_snapshot(read_snapshot_cache, &state);
                    return Ok(Cow::Owned(state.tables));
                }
                let mut tables = live_tables.clone();
                let replay =
                    replay_metadata_table_batches_from(&path, table_log_offset, &mut tables)?;
                let state = DfsMetadataTableState {
                    tables,
                    table_log_offset: replay.offset,
                    materialized_through_sequence: max_optional_sequence(
                        Some(materialized_through_sequence),
                        replay.materialized_through_sequence,
                    ),
                    snapshot_format_version: None,
                    snapshot_fingerprint: current_snapshot_fingerprint,
                };
                store_read_snapshot(read_snapshot_cache, &state);
                return Ok(Cow::Owned(state.tables));
            }
        }
    }
    let table_log_offset = metadata.as_ref().map(fs::Metadata::len).unwrap_or(0);
    if let Some(tables) = cached_read_snapshot(
        read_snapshot_cache,
        &current_snapshot_fingerprint,
        table_log_offset,
    ) {
        return Ok(Cow::Owned(tables));
    }
    let state = read_metadata_table_state_lenient(root)?;
    store_read_snapshot(read_snapshot_cache, &state);
    Ok(Cow::Owned(state.tables))
}

fn cached_read_snapshot(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    table_log_offset: u64,
) -> Option<DfsMetadataTables> {
    let cache = read_snapshot_cache.lock().ok()?;
    let cache = cache.as_ref()?;
    if cache.snapshot_fingerprint == *snapshot_fingerprint
        && cache.table_log_offset == table_log_offset
    {
        return Some(cache.tables.clone());
    }
    None
}

fn store_read_snapshot(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    state: &DfsMetadataTableState,
) {
    if let Ok(mut cache) = read_snapshot_cache.lock() {
        *cache = Some(MetadataTableReadSnapshotCache {
            snapshot_fingerprint: state.snapshot_fingerprint.clone(),
            table_log_offset: state.table_log_offset,
            tables: state.tables.clone(),
        });
    }
}

fn reload_metadata_tables_if_snapshot_changed(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
) -> Result<()> {
    let current = metadata_table_snapshot_fingerprint(root)?;
    if current == *table_snapshot_fingerprint {
        return Ok(());
    }
    let state = read_metadata_table_state(root)?;
    *tables = state.tables;
    *table_log_offset = state.table_log_offset;
    *table_snapshot_fingerprint = state.snapshot_fingerprint;
    Ok(())
}
