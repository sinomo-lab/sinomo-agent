//! Applying metadata table mutations to the durable table log.
//!
//! The backend plans table operations in several places: local writes,
//! journal replay, recovery, and repair. This module centralizes the common
//! append/apply/compact flow so those paths share the same durability behavior.

use std::fs;
use std::io;
use std::path::Path;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::{DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp};
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
    pub(crate) prefix_proof: &'a mut Option<DfsMetadataPrefixProof>,
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
        state.prefix_proof.as_ref(),
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
    let Some(prefix_proof) = state.prefix_proof.as_ref() else {
        if let Some(rollback) = rollback {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} cannot commit without an exact journal-prefix proof"
        )));
    };
    if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != sequence {
        if let Some(rollback) = rollback {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} prefix proof names sequence {}",
            prefix_proof.journal_sequence
        )));
    }
    let apply_after_append = rollback.is_none();
    let batch = DfsMetadataTableBatch {
        sequence,
        ops,
        prefix_proof: Some(prefix_proof.clone()),
    };
    let written = match append_metadata_table_batch(root, &batch) {
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
        for op in batch.ops {
            state.tables.apply(op);
        }
    }
    *state.materialized_through_sequence = (*state.materialized_through_sequence).max(sequence);
    compact_metadata_table_log_if_needed(root, state, max_log_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsFileSystemId;
    use crate::metadata_records::{
        DfsMetadataShardId, DfsNamespaceTransaction, dfs_metadata_prefix_proof_seed,
    };
    use time::OffsetDateTime;

    fn namespace_transaction_op(transaction_id: &str) -> DfsMetadataTableOp {
        DfsMetadataTableOp::PutNamespaceTransaction {
            transaction: DfsNamespaceTransaction {
                transaction_id: transaction_id.to_string(),
                file_system_id: DfsFileSystemId::from_legacy_named("fs_table_rollback")
                    .expect("filesystem"),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
        }
    }

    #[test]
    fn preapplied_batch_rolls_back_without_prefix_proof() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-no-proof");
        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_no_proof");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = None;
        let error = append_preapplied_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            u64::MAX,
            rollback,
        )
        .expect_err("missing proof must reject the preapplied batch");

        assert!(matches!(error, MetadataBackendError::Serde(_)));
        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
    }

    #[test]
    fn preapplied_batch_rolls_back_when_table_append_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-preapply-append-failure");
        let blocked_root = root.path().join("not-a-directory");
        fs::write(&blocked_root, b"block metadata table directory").expect("blocking file");

        let before = DfsMetadataTables::default();
        let mut tables = before.clone();
        let op = namespace_transaction_op("txn_preapply_append_failure");
        let mut rollback = DfsMetadataTableRollback::default();
        tables.apply_with_rollback(op.clone(), &mut rollback);
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_rollback"),
            1,
            "preappend-state",
        ));
        append_preapplied_table_ops(
            &blocked_root,
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![op],
            u64::MAX,
            rollback,
        )
        .expect_err("table append through a file path must fail");

        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
    }
}
