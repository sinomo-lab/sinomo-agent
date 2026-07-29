//! Lock-free read snapshots over shard-local metadata tables.
//!
//! Writers keep the live `DfsMetadataTables` in memory. Readers can borrow it
//! when the durable snapshot/log fingerprints still match, or build/cache an
//! owned table snapshot when another process has appended table batches.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

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
}

#[cfg(test)]
pub(crate) fn reset_metadata_durable_table_probes() {
    METADATA_DURABLE_TABLE_PROBES.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_durable_table_probes() -> usize {
    METADATA_DURABLE_TABLE_PROBES.get()
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
    let replay = replay_metadata_table_batches_from(&path, *table_log_offset, tables)?;
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
    if metadata_table_checkpoint_log_path(root).is_file() {
        return Ok(snapshot_from_state(
            read_snapshot_cache,
            read_metadata_table_state_lenient(root)?,
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
                    &path,
                    &current_snapshot_fingerprint,
                    len,
                )? {
                    return Ok(DfsMetadataTablesSnapshot::Shared(tables));
                }
                if len < table_log_offset {
                    let state = read_metadata_table_state_lenient(root)?;
                    return Ok(snapshot_from_state(read_snapshot_cache, state));
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
                    prefix_proof: replay.prefix_proof,
                };
                return Ok(snapshot_from_state(read_snapshot_cache, state));
            }
        }
    }
    let table_log_offset = metadata.as_ref().map(fs::Metadata::len).unwrap_or(0);
    if let Some(tables) = cached_or_advanced_read_snapshot(
        read_snapshot_cache,
        &path,
        &current_snapshot_fingerprint,
        table_log_offset,
    )? {
        return Ok(DfsMetadataTablesSnapshot::Shared(tables));
    }
    let state = read_metadata_table_state_lenient(root)?;
    Ok(snapshot_from_state(read_snapshot_cache, state))
}

fn cached_or_advanced_read_snapshot(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    path: &Path,
    snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    table_log_offset: u64,
) -> Result<Option<Arc<DfsMetadataTables>>> {
    let Ok(mut cache) = read_snapshot_cache.lock() else {
        return Ok(None);
    };
    let Some(cache) = cache.as_mut() else {
        return Ok(None);
    };
    if cache.snapshot_fingerprint != *snapshot_fingerprint
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
        let replay = replay_metadata_table_batches_from(path, cache.table_log_offset, tables)?;
        cache.table_log_offset = replay.offset;
    }
    Ok(Some(Arc::clone(&cache.tables)))
}

fn snapshot_from_state<'a>(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    state: DfsMetadataTableState,
) -> DfsMetadataTablesSnapshot<'a> {
    let tables = Arc::new(state.tables);
    if let Ok(mut cache) = read_snapshot_cache.lock() {
        *cache = Some(MetadataTableReadSnapshotCache {
            snapshot_fingerprint: state.snapshot_fingerprint.clone(),
            table_log_offset: state.table_log_offset,
            tables: Arc::clone(&tables),
        });
    }
    DfsMetadataTablesSnapshot::Shared(tables)
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
