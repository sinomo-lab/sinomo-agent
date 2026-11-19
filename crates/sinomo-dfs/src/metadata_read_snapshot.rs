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
    MetadataTableStorageGeneration, metadata_table_checkpoint_log_path, metadata_table_path,
    metadata_table_snapshot_fingerprint, metadata_table_storage_generation,
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
    static METADATA_CHECKPOINT_BORROW_VALIDATION_HOOK:
        std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static METADATA_INCREMENTAL_REPLAY_OPEN_HOOK:
        std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static METADATA_READ_SNAPSHOT_POST_READ_HOOK:
        std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
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
pub(crate) fn reset_metadata_checkpoint_snapshot_decodes() {
    METADATA_CHECKPOINT_SNAPSHOT_DECODES.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_checkpoint_snapshot_decodes() -> usize {
    METADATA_CHECKPOINT_SNAPSHOT_DECODES.get()
}

#[cfg(test)]
fn install_metadata_checkpoint_borrow_validation_hook(action: impl FnOnce() + 'static) {
    METADATA_CHECKPOINT_BORROW_VALIDATION_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(action));
    });
}

#[cfg(test)]
fn run_metadata_checkpoint_borrow_validation_hook() {
    let action = METADATA_CHECKPOINT_BORROW_VALIDATION_HOOK.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

#[cfg(test)]
pub(crate) fn install_metadata_read_snapshot_post_read_hook(action: impl FnOnce() + 'static) {
    METADATA_READ_SNAPSHOT_POST_READ_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(action));
    });
}

#[cfg(test)]
pub(crate) fn clear_metadata_read_snapshot_post_read_hook() {
    METADATA_READ_SNAPSHOT_POST_READ_HOOK.with(|hook| {
        hook.borrow_mut().take();
    });
}

fn replay_incremental_table_suffix(
    root: &Path,
    path: &Path,
    offset: u64,
    tables: &mut DfsMetadataTables,
    prefix: Option<&DfsMetadataPrefixProof>,
) -> Result<crate::metadata_tables::MetadataTableReplay> {
    #[cfg(test)]
    METADATA_INCREMENTAL_REPLAY_OPEN_HOOK.with(|hook| {
        let action = hook.borrow_mut().take();
        if let Some(action) = action {
            action();
        }
    });
    replay_metadata_table_batches_from(root, path, offset, tables, prefix)
}

/// Byte offsets are valid only within their captured append-only file. A
/// shorter log with the same physical identity is corruption, not checkpoint
/// rotation, and must never reload an older materialized cut as current.
pub(crate) fn ensure_table_log_cursor_generation(
    root: &Path,
    loaded: &Option<MetadataTableStorageGeneration>,
    current: &MetadataTableStorageGeneration,
    offset: u64,
) -> Result<()> {
    if loaded
        .as_ref()
        .is_some_and(|loaded| loaded.is_compatible_with(current))
        && current.active_log_len().is_some_and(|len| len < offset)
    {
        return Err(MetadataBackendError::Serde(format!(
            "DFS metadata table cursor {offset} is beyond table log length {} without a generation change at {}",
            current.active_log_len().unwrap(),
            metadata_table_path(root).display(),
        )));
    }
    Ok(())
}

pub(crate) fn refresh_metadata_tables(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,

    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    refresh_metadata_tables_with_reader(
        root,
        tables,
        table_log_offset,
        materialized_through_sequence,
        table_snapshot_fingerprint,
        prefix_proof,
        read_metadata_table_state,
    )
}

/// Refresh an inspection-only table image without repairing any durable
/// metadata. In particular, full reloads must not truncate crash-torn table
/// tails or persist snapshot verification records merely because a status
/// reader observed a new checkpoint generation.
pub(crate) fn refresh_metadata_tables_lenient(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,

    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    refresh_metadata_tables_with_reader(
        root,
        tables,
        table_log_offset,
        materialized_through_sequence,
        table_snapshot_fingerprint,
        prefix_proof,
        read_metadata_table_state_lenient,
    )
}

fn refresh_metadata_tables_with_reader<ReadState>(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,

    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
    read_state: ReadState,
) -> Result<()>
where
    ReadState: Fn(&Path) -> Result<DfsMetadataTableState> + Copy,
{
    let generation_before = metadata_table_storage_generation(root)?;
    let full_state_read = std::cell::Cell::new(false);
    let outcome = refresh_metadata_tables_once(
        root,
        tables,
        table_log_offset,
        materialized_through_sequence,
        table_snapshot_fingerprint,
        prefix_proof,
        |root| {
            full_state_read.set(true);
            read_state(root)
        },
    );
    let generation_after = metadata_table_storage_generation(root)?;
    if generation_before.is_compatible_with(&generation_after)
        || (outcome.is_ok() && full_state_read.get())
    {
        return outcome;
    }
    // A rename/reset crossed the length probe and replay's file open. The
    // pathname now names a different log; its predecessor's byte cursor has no
    // meaning there. Recover the complete exact cut with the existing stable
    // reader, even if suffix replay reported an out-of-range cursor.
    let state = read_state(root)?;
    *tables = state.tables;
    *table_log_offset = state.table_log_offset;

    *materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
    *table_snapshot_fingerprint = state.snapshot_fingerprint;
    *prefix_proof = state.prefix_proof;
    Ok(())
}

fn refresh_metadata_tables_once<ReadState>(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,

    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
    read_state: ReadState,
) -> Result<()>
where
    ReadState: Fn(&Path) -> Result<DfsMetadataTableState> + Copy,
{
    if metadata_table_checkpoint_log_path(root).is_file() {
        let state = read_state(root)?;
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
        read_state,
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
        let state = read_state(root)?;
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
    let replay = replay_incremental_table_suffix(
        root,
        &path,
        *table_log_offset,
        tables,
        prefix_proof.as_ref(),
    )?;
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
    loaded_storage_generation: &Option<MetadataTableStorageGeneration>,
    prefix_proof: &Option<DfsMetadataPrefixProof>,
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
) -> Result<DfsMetadataTablesSnapshot<'a>> {
    #[cfg(test)]
    METADATA_DURABLE_TABLE_PROBES.set(METADATA_DURABLE_TABLE_PROBES.get().saturating_add(1));
    pin_current_tables_snapshot(
        root,
        live_tables,
        table_log_offset,
        materialized_through_sequence,
        table_snapshot_fingerprint,
        loaded_storage_generation,
        prefix_proof,
        read_snapshot_cache,
    )
}

/// The same validated lock-free read, used once at an operation's pin boundary.
/// Getter-level probe counters stay in current_tables_snapshot, as with the
/// existing consistent-read scope; this pin remains visible in read_snapshot
/// tracing and actual filesystem I/O measurements.
pub(crate) fn pin_current_tables_snapshot<'a>(
    root: &Path,
    live_tables: &'a DfsMetadataTables,
    table_log_offset: u64,
    materialized_through_sequence: u64,
    table_snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    loaded_storage_generation: &Option<MetadataTableStorageGeneration>,
    prefix_proof: &Option<DfsMetadataPrefixProof>,
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
) -> Result<DfsMetadataTablesSnapshot<'a>> {
    const MAX_GENERATION_TRANSITION_ATTEMPTS: usize = 4;
    for _ in 0..MAX_GENERATION_TRANSITION_ATTEMPTS {
        let generation_before = metadata_table_storage_generation(root)?;
        ensure_table_log_cursor_generation(
            root,
            loaded_storage_generation,
            &generation_before,
            table_log_offset,
        )?;
        if let Ok(mut cached) = read_snapshot_cache.lock() {
            if let Some(cache) = cached.as_ref() {
                ensure_table_log_cursor_generation(
                    root,
                    &cache.storage_generation,
                    &generation_before,
                    cache.table_log_offset,
                )?;
                if cache
                    .storage_generation
                    .as_ref()
                    .is_some_and(|loaded| !loaded.is_compatible_with(&generation_before))
                {
                    *cached = None;
                }
            }
        }
        let mut full_state_read = false;
        let outcome = current_tables_snapshot_once(
            root,
            live_tables,
            table_log_offset,
            materialized_through_sequence,
            table_snapshot_fingerprint,
            loaded_storage_generation,
            prefix_proof,
            read_snapshot_cache,
            &mut full_state_read,
        );
        #[cfg(test)]
        METADATA_READ_SNAPSHOT_POST_READ_HOOK.with(|hook| {
            let action = hook.borrow_mut().take();
            if let Some(action) = action {
                action();
            }
        });
        let generation_after = metadata_table_storage_generation(root)?;
        if generation_before.is_compatible_with(&generation_after) {
            if let Ok(DfsMetadataTablesSnapshot::Shared { tables, .. }) = &outcome
                && let Ok(mut cached) = read_snapshot_cache.lock()
                && let Some(cache) = cached.as_mut()
                && Arc::ptr_eq(tables, &cache.tables)
            {
                cache.storage_generation = Some(generation_after);
            }
            return outcome;
        }
        // Suffix replay may have partially updated the owned image before a
        // generation transition. Never reuse that image on another attempt.
        if let Ok(mut cache) = read_snapshot_cache.lock() {
            *cache = None;
        }
        if outcome.is_ok() && full_state_read {
            // The complete reader already fenced its own durable generation;
            // its owned exact cut remains valid if another rotation follows it.
            return outcome;
        }
        std::thread::yield_now();
    }
    Err(MetadataBackendError::Serde(format!(
        "metadata table generation at {} changed during {MAX_GENERATION_TRANSITION_ATTEMPTS} consecutive snapshot attempts",
        root.display()
    )))
}

fn current_tables_snapshot_once<'a>(
    root: &Path,
    live_tables: &'a DfsMetadataTables,
    table_log_offset: u64,
    materialized_through_sequence: u64,
    table_snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    loaded_storage_generation: &Option<MetadataTableStorageGeneration>,
    prefix_proof: &Option<DfsMetadataPrefixProof>,
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    full_state_read: &mut bool,
) -> Result<DfsMetadataTablesSnapshot<'a>> {
    let checkpoint_fingerprint = metadata_table_checkpoint_fingerprint(root)?;
    if checkpoint_fingerprint.is_some() {
        if let Some(loaded_storage_generation) = loaded_storage_generation
            && prefix_proof.as_ref().is_some_and(|proof| {
                proof.is_well_formed() && proof.journal_sequence == materialized_through_sequence
            })
        {
            let generation_before = metadata_table_storage_generation(root)?;
            if loaded_storage_generation
                .permits_live_table_borrow(&generation_before, table_log_offset)
            {
                #[cfg(test)]
                run_metadata_checkpoint_borrow_validation_hook();
                let generation_after = metadata_table_storage_generation(root)?;
                if generation_before == generation_after
                    && loaded_storage_generation
                        .permits_live_table_borrow(&generation_after, table_log_offset)
                {
                    return Ok(DfsMetadataTablesSnapshot::Borrowed(live_tables));
                }
                // A checkpoint rotation/replacement crossed the validation
                // window. Do not consult a cache keyed by the generation
                // sampled before that race; the stable table reader will
                // restart around the durable transition and return one owned
                // exact-cut image.
                #[cfg(test)]
                METADATA_CHECKPOINT_SNAPSHOT_DECODES
                    .set(METADATA_CHECKPOINT_SNAPSHOT_DECODES.get().saturating_add(1));
                return Ok(snapshot_from_state(
                    read_snapshot_cache,
                    read_stable_table_snapshot(root, full_state_read)?,
                    metadata_table_checkpoint_fingerprint(root)?,
                ));
            }
        }
        let path = metadata_table_path(root);
        let current_snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
        let table_log_offset = metadata_table_log_len(&path)?;
        if let Some(snapshot) = cached_or_advanced_read_snapshot(
            read_snapshot_cache,
            root,
            &path,
            &current_snapshot_fingerprint,
            &checkpoint_fingerprint,
            table_log_offset,
        )? {
            return Ok(snapshot);
        }
        #[cfg(test)]
        METADATA_CHECKPOINT_SNAPSHOT_DECODES
            .set(METADATA_CHECKPOINT_SNAPSHOT_DECODES.get().saturating_add(1));
        return Ok(snapshot_from_state(
            read_snapshot_cache,
            read_stable_table_snapshot(root, full_state_read)?,
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
                if let Some(snapshot) = cached_or_advanced_read_snapshot(
                    read_snapshot_cache,
                    root,
                    &path,
                    &current_snapshot_fingerprint,
                    &None,
                    len,
                )? {
                    return Ok(snapshot);
                }
                if len < table_log_offset {
                    let state = read_stable_table_snapshot(root, full_state_read)?;
                    return Ok(snapshot_from_state(read_snapshot_cache, state, None));
                }
                let mut tables = live_tables.clone();
                let replay =
                    replay_incremental_table_suffix(root, &path, table_log_offset, &mut tables, prefix_proof.as_ref())?;
                let state = DfsMetadataTableState {
                    tables,
                    generation: Some(crate::storage_generation::read_generation(root)?.identity),
                    table_log_offset: replay.offset,

                    materialized_through_sequence: max_optional_sequence(
                        Some(materialized_through_sequence),
                        replay.materialized_through_sequence,
                    ),
                    snapshot_format_version: None,
                    snapshot_fingerprint: current_snapshot_fingerprint,
                    prefix_proof: replay.prefix_proof.or_else(|| prefix_proof.clone()),
                };
                return Ok(snapshot_from_state(read_snapshot_cache, state, None));
            }
        }
    }
    let table_log_offset = metadata.as_ref().map(fs::Metadata::len).unwrap_or(0);
    if let Some(snapshot) = cached_or_advanced_read_snapshot(
        read_snapshot_cache,
        root,
        &path,
        &current_snapshot_fingerprint,
        &None,
        table_log_offset,
    )? {
        return Ok(snapshot);
    }
    let state = read_stable_table_snapshot(root, full_state_read)?;
    Ok(snapshot_from_state(read_snapshot_cache, state, None))
}

fn read_stable_table_snapshot(
    root: &Path,
    full_state_read: &mut bool,
) -> Result<DfsMetadataTableState> {
    *full_state_read = true;
    read_metadata_table_state_lenient(root)
}

fn cached_or_advanced_read_snapshot<'a>(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    root: &Path,
    path: &Path,
    snapshot_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    checkpoint_fingerprint: &Option<MetadataTableSnapshotFingerprint>,
    table_log_offset: u64,
) -> Result<Option<DfsMetadataTablesSnapshot<'a>>> {
    let Ok(mut cached) = read_snapshot_cache.lock() else {
        return Ok(None);
    };
    let Some(cache) = cached.as_mut() else {
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
            match replay_incremental_table_suffix(root, path, cache.table_log_offset, tables, cache.prefix_proof.as_ref()) {
                Ok(replay) => replay,
                Err(error) => {
                    // Replay can mutate tables before encountering a bad later
                    // frame. That partial image is neither a readable snapshot nor
                    // a certified prefix which a writer may subsequently adopt.
                    *cached = None;
                    return Err(error);
                }
            };
        cache.table_log_offset = replay.offset;

        cache.materialized_through_sequence = max_optional_sequence(
            cache.materialized_through_sequence,
            replay.materialized_through_sequence,
        );
        if replay.prefix_proof.is_some() {
            cache.prefix_proof = replay.prefix_proof;
        }
    }
    // The owned table cut can outlive or be evicted from this optional cache.
    // Capture its proof under the SAME lock, never by a later cache lookup.
    Ok(Some(DfsMetadataTablesSnapshot::Shared {
        tables: Arc::clone(&cache.tables),
        prefix_proof: cache.prefix_proof.clone(),
        materialized_through_sequence: cache.materialized_through_sequence,
    }))
}

fn snapshot_from_state<'a>(
    read_snapshot_cache: &Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
    state: DfsMetadataTableState,
    checkpoint_fingerprint: Option<MetadataTableSnapshotFingerprint>,
) -> DfsMetadataTablesSnapshot<'a> {
    let tables = Arc::new(state.tables);
    let prefix_proof = state.prefix_proof.clone();
    let materialized_through_sequence = state.materialized_through_sequence;
    if let Ok(mut cache) = read_snapshot_cache.lock() {
        *cache = Some(MetadataTableReadSnapshotCache {
            storage_generation: None,
            snapshot_fingerprint: state.snapshot_fingerprint.clone(),
            checkpoint_fingerprint,
            table_log_offset: state.table_log_offset,

            materialized_through_sequence: state.materialized_through_sequence,
            snapshot_format_version: state.snapshot_format_version,
            prefix_proof: state.prefix_proof,
            tables: Arc::clone(&tables),
        });
    }
    DfsMetadataTablesSnapshot::Shared {
        tables,
        prefix_proof,
        materialized_through_sequence,
    }
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

fn reload_metadata_tables_if_snapshot_changed<ReadState>(
    root: &Path,
    tables: &mut DfsMetadataTables,
    table_log_offset: &mut u64,

    materialized_through_sequence: &mut u64,
    table_snapshot_fingerprint: &mut Option<MetadataTableSnapshotFingerprint>,
    prefix_proof: &mut Option<DfsMetadataPrefixProof>,
    read_state: ReadState,
) -> Result<()>
where
    ReadState: Fn(&Path) -> Result<DfsMetadataTableState>,
{
    let current = metadata_table_snapshot_fingerprint(root)?;
    if current == *table_snapshot_fingerprint {
        return Ok(());
    }
    let state = read_state(root)?;
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
    use crate::metadata_records::{
        DfsMetadataTableBatch, DfsMetadataTableOp, dfs_metadata_prefix_proof_genesis,
    };
    use crate::metadata_table_store::{
        append_metadata_table_batch, metadata_table_checkpoint_log_path, metadata_table_path,
        persist_metadata_table_snapshot, rotate_metadata_table_log_for_checkpoint,
    };
    use crate::{DfsFileSystem, DfsFileSystemId, DfsMetadataShardId};

    fn file_system(label: &str, display_name_bytes: usize) -> DfsFileSystem {
        DfsFileSystem {
            catalog_generation: 0,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: DfsFileSystemId::from_legacy_named(format!("fs_{label}"))
                .expect("valid filesystem id"),
            display_name: label.repeat(display_name_bytes.max(1)),
            owner_node_id: NodeId::from_legacy_named("node_owner").expect("valid owner node"),
            subtree_policies: Vec::new(),
        }
    }

    fn persist_genesis(root: &Path) {
        crate::metadata_candidate::stage_current_test_generation(root, "shard-test");
        let proof = dfs_metadata_prefix_proof_genesis(&DfsMetadataShardId::new("shard-test"));
        persist_metadata_table_snapshot(root, &DfsMetadataTables::default(), Some(0), Some(&proof))
            .unwrap();
    }

    // Physical table-reader fixtures use genuine current journal frames and
    // their exact byte-derived prefix. No distributed acknowledgement is minted.
    fn append_file_system(root: &Path, ordinal: u64, file_system: DfsFileSystem) -> u64 {
        use crate::metadata_journal::{
            DfsMetadataJournalFrame, persist_replayed_journal_entries,
            segmented_journal_entry_at_sequence,
        };
        use crate::{
            DfsPreparedMetadataEffect, DfsPreparedOperationDigest, DfsPreparedOperationPayload,
            DfsPreparedOperationRecord, DfsPreparedOperationScope, DfsPreparedOperationStatus,
        };
        let shard = DfsMetadataShardId::new("shard-test");
        let prepare_sequence = ordinal * 2 - 1;
        let commit_sequence = prepare_sequence + 1;
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: shard.clone(),
                owner_epoch: 1,
                operation_id: format!("read-snapshot:{}:{ordinal}", file_system.file_system_id),
            },
            Vec::new(),
            Vec::new(),
            vec![DfsPreparedMetadataEffect::PutFileSystem {
                file_system: file_system.clone(),
            }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let digest = DfsPreparedOperationDigest::at_sequence(&payload, prepare_sequence).unwrap();
        let record = DfsPreparedOperationRecord {
            digest: digest.clone(),
            payload: Arc::new(payload),
            status: DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        let prepare = crate::DfsMetadataJournalEntry {
            shard_id: shard.clone(),
            sequence: prepare_sequence,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            mutation: crate::DfsMetadataMutation::PrepareOperationV7 {
                file_system_id: file_system.file_system_id.clone(),
                operation_id: digest.operation_id.clone(),
                record: Box::new(record.clone()),
            },
        };
        let commit = crate::DfsMetadataJournalEntry {
            shard_id: shard.clone(),
            sequence: commit_sequence,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            mutation: crate::DfsMetadataMutation::CommitPreparedOperationV7 {
                file_system_id: file_system.file_system_id.clone(),
                operation_id: digest.operation_id.clone(),
                digest: digest.clone(),
            },
        };
        let mut prefix = dfs_metadata_prefix_proof_genesis(&shard);
        for sequence in 1..prepare_sequence {
            let entry = segmented_journal_entry_at_sequence(root, sequence)
                .unwrap()
                .expect("actual previous frame");
            prefix = DfsMetadataJournalFrame::encode(&entry)
                .unwrap()
                .prefix_proof_after(&prefix)
                .unwrap();
        }
        persist_replayed_journal_entries(root, &[&prepare, &commit], prepare_sequence - 1).unwrap();
        prefix = DfsMetadataJournalFrame::encode(&prepare)
            .unwrap()
            .prefix_proof_after(&prefix)
            .unwrap();
        append_metadata_table_batch(
            root,
            &DfsMetadataTableBatch {
                sequence: prepare_sequence,
                ops: vec![DfsMetadataTableOp::PutPreparedOperationV7 {
                    record: Box::new(record),
                }],
                prefix_proof: Some(prefix.clone()),
            },
        )
        .unwrap();
        prefix = DfsMetadataJournalFrame::encode(&commit)
            .unwrap()
            .prefix_proof_after(&prefix)
            .unwrap();
        append_metadata_table_batch(
            root,
            &DfsMetadataTableBatch {
                sequence: commit_sequence,
                ops: vec![
                    DfsMetadataTableOp::PutFileSystem { file_system },
                    DfsMetadataTableOp::CommitPreparedOperationV7 {
                        digest,
                        committed_entry: Box::new(
                            crate::DfsPreparedOperationCommitReference::from_journal_entry(&commit)
                                .unwrap(),
                        ),
                    },
                ],
                prefix_proof: Some(prefix),
            },
        )
        .unwrap();
        // The fixture appends both Prepare and Commit. Callers retain the
        // complete file boundary, not the byte count of its final batch.
        fs::metadata(metadata_table_path(root)).unwrap().len()
    }

    fn shared_tables(snapshot: DfsMetadataTablesSnapshot<'_>) -> Arc<DfsMetadataTables> {
        match snapshot {
            DfsMetadataTablesSnapshot::Shared { tables, .. } => tables,
            DfsMetadataTablesSnapshot::Borrowed(_) => {
                panic!("rotated checkpoint snapshot must be owned")
            }
        }
    }

    fn setup_rotated_checkpoint(
        root: &Path,
        checkpoint_file_system: DfsFileSystem,
    ) -> Option<MetadataTableSnapshotFingerprint> {
        persist_genesis(root);
        append_file_system(root, 1, checkpoint_file_system);
        assert!(
            rotate_metadata_table_log_for_checkpoint(root, 0)
                .expect("rotate metadata table checkpoint"),
            "non-empty active table log must rotate"
        );
        metadata_table_snapshot_fingerprint(root).expect("snapshot fingerprint")
    }

    #[test]
    fn incremental_snapshot_rotation_between_length_probe_and_replay_preserves_all_rows() {
        for cached in [false, true] {
            for append_after_rotation in [false, true] {
                let root = sinomo_testdir::TestDir::new("metadata-incremental-read-rotation");
                persist_genesis(root.path());
                let first = file_system("first", 1);
                append_file_system(root.path(), 1, first.clone());
                let loaded = read_metadata_table_state_lenient(root.path()).unwrap();
                let generation = Some(metadata_table_storage_generation(root.path()).unwrap());
                let cache = Arc::new(Mutex::new(None));
                let second = file_system("second", 1);
                append_file_system(root.path(), 2, second.clone());
                let previous_view = if cached {
                    Some(shared_tables(
                        current_tables_snapshot(
                            root.path(),
                            &loaded.tables,
                            loaded.table_log_offset,
                            loaded.materialized_through_sequence.unwrap(),
                            &loaded.snapshot_fingerprint,
                            &generation,
                            &loaded.prefix_proof,
                            &cache,
                        )
                        .unwrap(),
                    ))
                } else {
                    None
                };
                let third = file_system("third", 1);
                if cached {
                    append_file_system(root.path(), 3, third.clone());
                }
                let fourth = file_system("fourth", 1);
                let rotation_root = root.path().to_path_buf();
                let after_rotation = fourth.clone();
                METADATA_INCREMENTAL_REPLAY_OPEN_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        assert!(
                            rotate_metadata_table_log_for_checkpoint(&rotation_root, 0).unwrap()
                        );
                        if append_after_rotation {
                            // Without the cached read there was no third
                            // operation; append the next contiguous journal pair.
                            append_file_system(
                                &rotation_root,
                                if cached { 4 } else { 3 },
                                after_rotation,
                            );
                        }
                    }));
                });
                let view = shared_tables(
                    current_tables_snapshot(
                        root.path(),
                        &loaded.tables,
                        loaded.table_log_offset,
                        loaded.materialized_through_sequence.unwrap(),
                        &loaded.snapshot_fingerprint,
                        &generation,
                        &loaded.prefix_proof,
                        &cache,
                    )
                    .expect(
                        "rotation must rebase the byte cursor instead of failing or losing rows",
                    ),
                );
                assert!(view.file_systems.contains_key(&first.file_system_id));
                assert!(view.file_systems.contains_key(&second.file_system_id));
                assert_eq!(
                    view.file_systems.contains_key(&third.file_system_id),
                    cached
                );
                assert_eq!(
                    view.file_systems.contains_key(&fourth.file_system_id),
                    append_after_rotation
                );
                if let Some(previous) = previous_view {
                    assert!(previous.file_systems.contains_key(&second.file_system_id));
                    assert!(
                        !previous.file_systems.contains_key(&third.file_system_id),
                        "generation recovery must preserve previously owned snapshot isolation"
                    );
                }
            }
        }
    }

    #[test]
    fn table_refresh_rotation_rebases_cursor_and_replay_accounting() {
        let root = sinomo_testdir::TestDir::new("metadata-table-refresh-rotation");
        persist_genesis(root.path());
        let first = file_system("first", 1);
        append_file_system(root.path(), 1, first.clone());
        let mut state = read_metadata_table_state_lenient(root.path()).unwrap();
        let second = file_system("second", 1);
        append_file_system(root.path(), 2, second.clone());
        let rotation_root = root.path().to_path_buf();
        METADATA_INCREMENTAL_REPLAY_OPEN_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                assert!(rotate_metadata_table_log_for_checkpoint(&rotation_root, 0).unwrap());
            }));
        });
        let mut sequence = state.materialized_through_sequence.unwrap();
        refresh_metadata_tables(
            root.path(),
            &mut state.tables,
            &mut state.table_log_offset,
            &mut sequence,
            &mut state.snapshot_fingerprint,
            &mut state.prefix_proof,
        )
        .expect("refresh must recover the rotated exact cut");
        assert!(
            state
                .tables
                .file_systems
                .contains_key(&first.file_system_id)
        );
        assert!(
            state
                .tables
                .file_systems
                .contains_key(&second.file_system_id)
        );
        assert_eq!(state.table_log_offset, 0);
        assert_eq!(sequence, 4);
        assert_eq!(
            state.prefix_proof.as_ref().unwrap().journal_sequence,
            sequence
        );
    }

    #[test]
    fn incremental_snapshot_same_generation_cursor_error_remains_fail_closed() {
        let root = sinomo_testdir::TestDir::new("metadata-incremental-corrupt-cursor");
        persist_genesis(root.path());
        append_file_system(root.path(), 1, file_system("first", 1));
        let loaded = read_metadata_table_state_lenient(root.path()).unwrap();
        let generation = Some(metadata_table_storage_generation(root.path()).unwrap());
        append_file_system(root.path(), 2, file_system("second", 1));
        let active_path = metadata_table_path(root.path());
        METADATA_INCREMENTAL_REPLAY_OPEN_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                // Corrupt the same file without a checkpoint-generation
                // transition. Recovery must not reinterpret it as a rotation.
                OpenOptions::new()
                    .write(true)
                    .open(active_path)
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }));
        });
        let result = current_tables_snapshot(
            root.path(),
            &loaded.tables,
            loaded.table_log_offset,
            loaded.materialized_through_sequence.unwrap(),
            &loaded.snapshot_fingerprint,
            &generation,
            &loaded.prefix_proof,
            &Arc::new(Mutex::new(None)),
        );
        assert!(
            matches!(result, Err(MetadataBackendError::Serde(ref message))
            if message.contains("beyond table log length"))
        );
    }

    #[test]
    fn preprobe_same_generation_shrink_rejects_live_and_advanced_cache_cursors() {
        for cached in [false, true] {
            let root = sinomo_testdir::TestDir::new("metadata-preprobe-shrink");
            persist_genesis(root.path());
            let first_len = append_file_system(root.path(), 1, file_system("first", 1));
            let first = read_metadata_table_state_lenient(root.path()).unwrap();
            append_file_system(root.path(), 2, file_system("second", 1));
            let loaded = if cached {
                first
            } else {
                read_metadata_table_state_lenient(root.path()).unwrap()
            };
            let generation = Some(metadata_table_storage_generation(root.path()).unwrap());
            let cache = Arc::new(Mutex::new(None));
            if cached {
                drop(
                    current_tables_snapshot(
                        root.path(),
                        &loaded.tables,
                        loaded.table_log_offset,
                        loaded.materialized_through_sequence.unwrap(),
                        &loaded.snapshot_fingerprint,
                        &generation,
                        &loaded.prefix_proof,
                        &cache,
                    )
                    .unwrap(),
                );
            }
            OpenOptions::new()
                .write(true)
                .open(metadata_table_path(root.path()))
                .unwrap()
                .set_len(first_len)
                .unwrap();
            let result = current_tables_snapshot(
                root.path(),
                &loaded.tables,
                loaded.table_log_offset,
                loaded.materialized_through_sequence.unwrap(),
                &loaded.snapshot_fingerprint,
                &generation,
                &loaded.prefix_proof,
                &cache,
            );
            assert!(
                matches!(result, Err(MetadataBackendError::Serde(ref message))
                if message.contains("without a generation change")),
                "a same-file truncation must not return an older live/cache cut"
            );
        }
    }

    #[test]
    fn preprobe_same_generation_shrink_fences_native_writer_and_inspection_refresh() {
        use crate::DfsMetadataBackend;

        let (mut native, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "metadata-native-preprobe-shrink", 1,
        ).unwrap();
        let node = nodes[0].clone();
        let shard_id = DfsMetadataShardId::new("root");
        let root = crate::local_dfs_metadata_shard_path(native.data_dir(&node), &shard_id);
        let subscriber =
            crate::LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &root).unwrap();
        {
            let mut client = native.client(&node).unwrap();
            client.posix_write_file(
                "/preprobe-file", b"original committed bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            ).unwrap();
        }
        let second = native.file_system();
        drop(subscriber);
        native.stop_node(&node).unwrap();
        let mut writer = crate::LocalSobjMetadataBackend::new(shard_id.clone(), &root).unwrap();
        let mut inspector = crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(), &root,
        ).unwrap();
        assert!(fs::metadata(metadata_table_path(&root)).unwrap().len() > 0);
        let journal_head = writer.journal_last_sequence();
        OpenOptions::new()
            .write(true)
            .open(metadata_table_path(&root))
            .unwrap()
            .set_len(0)
            .unwrap();
        assert!(
            matches!(inspector.refresh_for_inspection(), Err(MetadataBackendError::Serde(ref message))
            if message.contains("without a generation change"))
        );
        let mut attempted = second;
        attempted.display_name = "must not be published".into();
        assert!(
            matches!(writer.prepare_operation(crate::DfsPreparedOperationPayload::for_metadata_effects(crate::DfsPreparedOperationScope {
                file_system_id: attempted.file_system_id.clone(), shard_id: shard_id.clone(), owner_epoch: 1,
                operation_id: "must-not-append".into(),
            }, Vec::new(), Vec::new(), vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system: attempted }], time::OffsetDateTime::UNIX_EPOCH).unwrap(), None), Err(MetadataBackendError::Serde(ref message))
            if message.contains("without a generation change"))
        );
        assert_eq!(
            writer.journal_last_sequence(),
            journal_head,
            "corrupt table cursor must stop the writer before a new journal effect"
        );
        drop(inspector);
        drop(writer);
        native.close().unwrap();
    }

    #[test]
    fn checkpoint_snapshot_borrows_matching_loaded_storage_generation_without_decode() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-borrow-same-generation");
        let checkpoint_file_system = file_system("checkpoint", 1);
        setup_rotated_checkpoint(root.path(), checkpoint_file_system.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load exact checkpoint state");
        let loaded_storage_generation = Some(
            metadata_table_storage_generation(root.path())
                .expect("capture loaded checkpoint generation"),
        );
        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();

        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &loaded_storage_generation,
            &state.prefix_proof,
            &cache,
        )
        .expect("borrow matching checkpoint generation");
        match snapshot {
            DfsMetadataTablesSnapshot::Borrowed(tables) => {
                assert!(std::ptr::eq(tables, &state.tables));
                assert!(
                    tables
                        .file_systems
                        .contains_key(&checkpoint_file_system.file_system_id)
                );
            }
            DfsMetadataTablesSnapshot::Shared { .. } => {
                panic!("an exact loaded checkpoint generation must borrow its live tables")
            }
        }
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 0);
        assert!(
            cache.lock().expect("snapshot cache").is_none(),
            "borrowing the live checkpoint generation must not retain a second table image"
        );
    }

    #[test]
    fn checkpoint_snapshot_borrows_exact_active_log_rotation_without_decode() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-borrow-active-rotation");
        let checkpoint_file_system = file_system("checkpoint", 1);
        persist_genesis(root.path());
        append_file_system(root.path(), 1, checkpoint_file_system.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load active table generation");
        let loaded_storage_generation =
            metadata_table_storage_generation(root.path()).expect("capture active generation");
        assert!(
            rotate_metadata_table_log_for_checkpoint(root.path(), 0)
                .expect("rotate active table generation"),
            "non-empty active table log must rotate"
        );

        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();
        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &Some(loaded_storage_generation),
            &state.prefix_proof,
            &cache,
        )
        .expect("borrow exact active-to-checkpoint rotation");
        match snapshot {
            DfsMetadataTablesSnapshot::Borrowed(tables) => {
                assert!(std::ptr::eq(tables, &state.tables));
                assert!(
                    tables
                        .file_systems
                        .contains_key(&checkpoint_file_system.file_system_id)
                );
            }
            DfsMetadataTablesSnapshot::Shared { .. } => {
                panic!("an exact active-to-checkpoint rotation must borrow its live tables")
            }
        }
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 0);
        assert!(
            cache.lock().expect("snapshot cache").is_none(),
            "borrowing the rotated live generation must not retain a second table image"
        );
    }

    #[test]
    fn checkpoint_snapshot_rotation_borrow_revalidates_concurrent_active_suffix() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-rotation-suffix-race");
        let checkpoint_file_system = file_system("checkpoint", 1);
        let suffix_file_system = file_system("suffix", 1);
        persist_genesis(root.path());
        append_file_system(root.path(), 1, checkpoint_file_system.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load active table generation");
        let loaded_storage_generation =
            metadata_table_storage_generation(root.path()).expect("capture active generation");
        assert!(
            rotate_metadata_table_log_for_checkpoint(root.path(), 0)
                .expect("rotate active table generation"),
            "non-empty active table log must rotate"
        );
        install_metadata_checkpoint_borrow_validation_hook({
            let root = root.path().to_path_buf();
            let suffix_file_system = suffix_file_system.clone();
            move || {
                append_file_system(&root, 2, suffix_file_system);
            }
        });

        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();
        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &Some(loaded_storage_generation),
            &state.prefix_proof,
            &cache,
        )
        .expect("fall back after concurrent active suffix");
        let tables = shared_tables(snapshot);
        assert!(
            tables
                .file_systems
                .contains_key(&checkpoint_file_system.file_system_id)
        );
        assert!(
            tables
                .file_systems
                .contains_key(&suffix_file_system.file_system_id)
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            1,
            "a concurrent active suffix must take the owned exact-cut path"
        );
    }

    #[test]
    fn checkpoint_snapshot_rotation_rejects_unobserved_pre_rotation_append() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-metadata-checkpoint-rotation-unobserved-prefix",
        );
        let observed_file_system = file_system("observed", 1);
        let unobserved_file_system = file_system("unseen", 1);
        persist_genesis(root.path());
        append_file_system(root.path(), 1, observed_file_system.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load active table generation");
        let loaded_storage_generation =
            metadata_table_storage_generation(root.path()).expect("capture active generation");
        append_file_system(root.path(), 2, unobserved_file_system.clone());
        assert!(
            rotate_metadata_table_log_for_checkpoint(root.path(), 0)
                .expect("rotate extended active table generation"),
            "non-empty active table log must rotate"
        );

        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();
        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &Some(loaded_storage_generation),
            &state.prefix_proof,
            &cache,
        )
        .expect("fall back for unobserved pre-rotation append");
        let tables = shared_tables(snapshot);
        assert!(
            tables
                .file_systems
                .contains_key(&observed_file_system.file_system_id)
        );
        assert!(
            tables
                .file_systems
                .contains_key(&unobserved_file_system.file_system_id)
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            1,
            "checkpoint length mismatch must take the owned exact-cut path"
        );
    }

    #[test]
    fn checkpoint_snapshot_rotation_borrow_revalidates_equal_length_replacement() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-metadata-checkpoint-rotation-replacement-race",
        );
        let original_checkpoint = file_system("original", 1);
        persist_genesis(root.path());
        append_file_system(root.path(), 1, original_checkpoint.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load active table generation");
        let loaded_storage_generation =
            metadata_table_storage_generation(root.path()).expect("capture active generation");
        assert!(
            rotate_metadata_table_log_for_checkpoint(root.path(), 0)
                .expect("rotate active table generation"),
            "non-empty active table log must rotate"
        );

        let replacement_root = sinomo_testdir::TestDir::new(
            "sinomo-dfs-metadata-checkpoint-rotation-replacement-source",
        );
        let replacement_checkpoint = file_system("replaced", 1);
        append_file_system(replacement_root.path(), 1, replacement_checkpoint.clone());
        let replacement_bytes =
            fs::read(metadata_table_path(replacement_root.path())).expect("replacement bytes");
        let checkpoint_path = metadata_table_checkpoint_log_path(root.path());
        assert_eq!(
            replacement_bytes.len() as u64,
            fs::metadata(&checkpoint_path)
                .expect("original checkpoint metadata")
                .len(),
            "replacement fixture must preserve length so file identity is load-bearing"
        );
        install_metadata_checkpoint_borrow_validation_hook({
            let checkpoint_path = checkpoint_path.clone();
            move || {
                crate::metadata_io::write_file_atomic(&checkpoint_path, &replacement_bytes)
                    .expect("replace checkpoint during rotation borrow validation");
            }
        });

        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();
        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &Some(loaded_storage_generation),
            &state.prefix_proof,
            &cache,
        )
        .expect("fall back after concurrent checkpoint replacement");
        let tables = shared_tables(snapshot);
        assert!(
            tables
                .file_systems
                .contains_key(&replacement_checkpoint.file_system_id)
        );
        assert!(
            !tables
                .file_systems
                .contains_key(&original_checkpoint.file_system_id)
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            1,
            "a raced checkpoint replacement must take the owned exact-cut path"
        );
    }

    #[test]
    fn checkpoint_snapshot_borrow_revalidates_concurrent_equal_length_replacement() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-borrow-race");
        let original_checkpoint = file_system("original", 1);
        setup_rotated_checkpoint(root.path(), original_checkpoint.clone());
        let state =
            read_metadata_table_state_lenient(root.path()).expect("load original checkpoint state");
        let loaded_storage_generation = Some(
            metadata_table_storage_generation(root.path())
                .expect("capture original checkpoint generation"),
        );

        let replacement_root =
            sinomo_testdir::TestDir::new("sinomo-dfs-metadata-checkpoint-borrow-race-replacement");
        let replacement_checkpoint = file_system("replaced", 1);
        append_file_system(replacement_root.path(), 1, replacement_checkpoint.clone());
        let replacement_bytes =
            fs::read(metadata_table_path(replacement_root.path())).expect("replacement bytes");
        let checkpoint_path = metadata_table_checkpoint_log_path(root.path());
        assert_eq!(
            replacement_bytes.len() as u64,
            fs::metadata(&checkpoint_path)
                .expect("original checkpoint metadata")
                .len(),
            "race fixture must preserve checkpoint length so identity validation is load-bearing"
        );
        install_metadata_checkpoint_borrow_validation_hook({
            let checkpoint_path = checkpoint_path.clone();
            move || {
                crate::metadata_io::write_file_atomic(&checkpoint_path, &replacement_bytes)
                    .expect("replace checkpoint during borrow validation");
            }
        });

        let cache = Arc::new(Mutex::new(None));
        reset_metadata_checkpoint_snapshot_decodes();
        let snapshot = current_tables_snapshot(
            root.path(),
            &state.tables,
            state.table_log_offset,
            state.materialized_through_sequence.unwrap_or(0),
            &state.snapshot_fingerprint,
            &loaded_storage_generation,
            &state.prefix_proof,
            &cache,
        )
        .expect("fall back after concurrent checkpoint replacement");
        let tables = shared_tables(snapshot);
        assert!(
            tables
                .file_systems
                .contains_key(&replacement_checkpoint.file_system_id)
        );
        assert!(
            !tables
                .file_systems
                .contains_key(&original_checkpoint.file_system_id)
        );
        assert_eq!(
            metadata_checkpoint_snapshot_decodes(),
            1,
            "a raced checkpoint generation must take the owned decode path"
        );
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
                &None,
                &None,
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
                &None,
                &None,
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
                &None,
                &None,
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
    fn failed_cached_suffix_replay_discards_partial_tables_and_preserves_prior_views() {
        let root = sinomo_testdir::TestDir::new("metadata-read-cache-partial-replay");
        let first = file_system("checkpoint", 1);
        let second = file_system("suffix", 1);
        let third = file_system("after-corruption", 1);
        let fingerprint = setup_rotated_checkpoint(root.path(), first.clone());
        let tables = DfsMetadataTables::default();
        let cache = Arc::new(Mutex::new(None));
        let before = shared_tables(
            current_tables_snapshot(
                root.path(),
                &tables,
                0,
                0,
                &fingerprint,
                &None,
                &None,
                &cache,
            )
            .unwrap(),
        );
        let good_len = append_file_system(root.path(), 2, second.clone());
        let path = metadata_table_path(root.path());
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&8u32.to_le_bytes()).unwrap();
            file.write_all(b"not-sobj").unwrap();
        }
        // Keep this complete corrupt frame inside the log, after a valid
        // suffix, so rejection must discard a partially advanced cached view.
        append_file_system(root.path(), 3, third.clone());
        let error = current_tables_snapshot(
            root.path(),
            &tables,
            0,
            0,
            &fingerprint,
            &None,
            &None,
            &cache,
        )
        .err()
        .expect("an interior corrupt frame must fail replay");
        assert!(matches!(error, MetadataBackendError::Serde(ref message)
            if message.contains("unsupported current metadata table-log format")));
        assert!(cache.lock().unwrap().is_none());
        assert!(before.file_systems.contains_key(&first.file_system_id));
        assert!(!before.file_systems.contains_key(&second.file_system_id));
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(good_len)
            .unwrap();
        let recovered = current_tables_snapshot(
            root.path(),
            &tables,
            0,
            0,
            &fingerprint,
            &None,
            &None,
            &cache,
        )
        .unwrap();
        assert!(recovered.file_systems.contains_key(&first.file_system_id));
        assert!(recovered.file_systems.contains_key(&second.file_system_id));
        assert!(!recovered.file_systems.contains_key(&third.file_system_id));
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
                &None,
                &None,
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
                &None,
                &None,
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
                &None,
                &None,
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
        let shrunk = current_tables_snapshot(
            root.path(),
            &live_tables,
            0,
            0,
            &snapshot_fingerprint,
            &None,
            &None,
            &cache,
        );
        assert!(
            matches!(shrunk, Err(MetadataBackendError::Serde(ref message))
            if message.contains("without a generation change")),
            "in-place shrink must not silently discard the advanced suffix"
        );
        assert_eq!(metadata_checkpoint_snapshot_decodes(), 2);

        fs::remove_file(checkpoint_path).expect("remove installed checkpoint generation");
        let without_checkpoint = shared_tables(
            current_tables_snapshot(
                root.path(),
                &live_tables,
                0,
                0,
                &snapshot_fingerprint,
                &None,
                &None,
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
            2,
            "checkpoint removal must leave the checkpoint decode path"
        );
    }
}
