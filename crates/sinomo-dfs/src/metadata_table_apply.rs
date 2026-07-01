//! Applying metadata table mutations to the durable table log.
//!
//! The backend plans table operations in several places: local writes,
//! journal replay, recovery, and repair. This module centralizes the common
//! append/apply/compact flow so those paths share the same durability behavior.

use std::fs;
use std::io;
use std::path::Path;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::{DfsMetadataTableBatch, DfsMetadataTableOp};
use crate::metadata_table_store::{
    append_metadata_table_batch, metadata_table_path, metadata_table_snapshot_fingerprint,
    persist_metadata_table_snapshot,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, MetadataTableSnapshotFingerprint,
};

pub(crate) struct TableMaterializationState<'a> {
    pub(crate) tables: &'a mut DfsMetadataTables,
    pub(crate) table_log_offset: &'a mut u64,
    pub(crate) materialized_through_sequence: &'a mut u64,
    pub(crate) table_snapshot_fingerprint: &'a mut Option<MetadataTableSnapshotFingerprint>,
}

pub(crate) fn append_and_apply_table_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
) -> Result<()> {
    append_table_batch(root, state, sequence, ops, max_log_bytes, None)
}

pub(crate) fn append_preapplied_table_ops(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
    rollback: DfsMetadataTableRollback,
) -> Result<()> {
    append_table_batch(root, state, sequence, ops, max_log_bytes, Some(rollback))
}

pub(crate) fn compact_metadata_table_log_if_needed(
    root: &Path,
    state: TableMaterializationState<'_>,
    max_log_bytes: u64,
) -> Result<()> {
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
    if len <= max_log_bytes {
        return Ok(());
    }
    persist_metadata_table_snapshot(
        root,
        state.tables,
        Some(*state.materialized_through_sequence),
    )?;
    *state.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    *state.table_log_offset = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .map_err(|source| MetadataBackendError::Io { path, source })?;
    Ok(())
}

fn append_table_batch(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
    rollback: Option<DfsMetadataTableRollback>,
) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }
    let apply_after_append = rollback.is_none();
    let written = match append_metadata_table_batch(
        root,
        &DfsMetadataTableBatch {
            sequence,
            ops: ops.clone(),
        },
    ) {
        Ok(written) => written,
        Err(error) => {
            if let Some(rollback) = rollback {
                rollback.rollback(state.tables);
            }
            return Err(error);
        }
    };
    *state.table_log_offset = state.table_log_offset.saturating_add(written);
    if apply_after_append {
        for op in ops {
            state.tables.apply(op);
        }
    }
    *state.materialized_through_sequence = (*state.materialized_through_sequence).max(sequence);
    compact_metadata_table_log_if_needed(root, state, max_log_bytes)
}
