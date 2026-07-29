//! Applying metadata table mutations to the durable table log.
//!
//! The backend plans table operations in several places: local writes,
//! journal replay, recovery, and repair. This module centralizes the common
//! append/apply/compact flow so those paths share the same durability behavior.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::DfsMetadataShardWriteFreeze;
use crate::metadata_cursor::journal_write_lock_path;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{read_sobj_optional, write_sobj_atomic};
use crate::metadata_journal::{
    durable_journal_entries_since_limited, prune_segmented_journal_before_checkpoint,
};
use crate::metadata_lock::metadata_write_lock;
use crate::metadata_records::{
    DFS_METADATA_PREFIX_PROOF_VERSION, DfsMetadataPrefixProof, DfsMetadataTableBatch,
    DfsMetadataTableOp, dfs_metadata_prefix_proof_after,
};
use crate::metadata_table_store::{
    append_metadata_table_batch, encode_metadata_table_snapshot,
    install_encoded_metadata_table_snapshot, metadata_table_checkpoint_log_path,
    metadata_table_path, metadata_table_snapshot_fingerprint, persist_metadata_table_snapshot,
    read_metadata_table_checkpoint_state, remove_metadata_table_checkpoint_log,
    rotate_metadata_table_log_for_checkpoint,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, MetadataTableSnapshotFingerprint,
};

static METADATA_TABLE_CHECKPOINT_WORKERS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

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
    let has_rotated_checkpoint = metadata_table_checkpoint_log_path(root).is_file();
    let len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound && !has_rotated_checkpoint => {
            return Ok(());
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    if len <= max_log_bytes && !has_rotated_checkpoint {
        return Ok(());
    }
    // Observable history transitions are authoritative journal mutations.
    // Snapshot-only body folding would leave an incremental replica with old
    // recovery records capable of resurrecting metadata after promotion.
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
    let checkpoint_proof = state.prefix_proof.as_ref().ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has no exact journal-prefix proof",
            root.display()
        ))
    })?;
    advance_activated_handoff_rebind_checkpoint(root, checkpoint_proof)?;
    // Snapshot publication and table-log truncation are durable before any
    // source history disappears. Lagging replicas observe the retained
    // checkpoint segment as a discontinuity and use snapshot repair.
    prune_segmented_journal_before_checkpoint(root, *state.materialized_through_sequence)?;
    Ok(())
}

/// Schedule table checkpointing after the foreground mutation has durably
/// appended its small materialization batch.
///
/// One worker per root captures a complete durable boundary under the metadata
/// write lock, releases the lock while folding/serializing that owned image,
/// and reacquires it only to install the snapshot plus any records appended
/// after the captured boundary.
pub(crate) fn schedule_metadata_table_checkpoint_if_needed(
    root: &Path,
    max_log_bytes: u64,
) -> Result<()> {
    if !metadata_table_checkpoint_log_path(root).is_file() {
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
    }

    let root = root.to_path_buf();
    let workers = METADATA_TABLE_CHECKPOINT_WORKERS.get_or_init(|| Mutex::new(BTreeSet::new()));
    {
        let mut workers = workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !workers.insert(root.clone()) {
            return Ok(());
        }
    }

    let worker_root = root.clone();
    let spawn = std::thread::Builder::new()
        .name("sinomo-dfs-metadata-checkpoint".to_string())
        .spawn(move || {
            let _registration =
                MetadataTableCheckpointWorkerRegistration(worker_root.clone());
            let result =
                run_metadata_table_checkpoint_worker(&worker_root, max_log_bytes);
            if let Err(error) = result {
                tracing::warn!(
                    target: "sinomo.dfs",
                    root = %worker_root.display(),
                    %error,
                    "background metadata table checkpoint failed; durable journal replay remains authoritative"
                );
            }
        });
    if let Err(source) = spawn {
        workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&root);
        tracing::warn!(
            target: "sinomo.dfs",
            root = %root.display(),
            %source,
            "could not start background metadata table checkpoint; a later write will retry"
        );
    }
    Ok(())
}

struct MetadataTableCheckpointWorkerRegistration(PathBuf);

impl Drop for MetadataTableCheckpointWorkerRegistration {
    fn drop(&mut self) {
        if let Some(workers) = METADATA_TABLE_CHECKPOINT_WORKERS.get() {
            workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.0);
        }
    }
}

fn run_metadata_table_checkpoint_worker(root: &Path, max_log_bytes: u64) -> Result<bool> {
    let snapshot_fingerprint = {
        let _write_lock = metadata_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
        if !rotate_metadata_table_log_for_checkpoint(root, max_log_bytes)? {
            return Ok(false);
        }
        metadata_table_snapshot_fingerprint(root)?
    };

    // The rotated generation is immutable. Decode, hash, and encode it without
    // holding the shard writer lock. Checkpointing must preserve the exact
    // journal-materialized tables; history compaction is a journal mutation.
    let state = read_metadata_table_checkpoint_state(root)?;
    let checkpoint_sequence = state.materialized_through_sequence.unwrap_or(0);
    let Some(prefix_proof) = state.prefix_proof else {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has no exact journal-prefix proof",
            root.display()
        )));
    };
    if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != checkpoint_sequence {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has mismatched sequence {} and prefix proof {}",
            root.display(),
            checkpoint_sequence,
            prefix_proof.journal_sequence
        )));
    }
    let tables = state.tables;
    let snapshot_bytes =
        encode_metadata_table_snapshot(&tables, Some(checkpoint_sequence), Some(&prefix_proof))?;

    let _write_lock = metadata_write_lock(root.to_path_buf(), journal_write_lock_path(root))?;
    if metadata_table_snapshot_fingerprint(root)? != snapshot_fingerprint
        || !metadata_table_checkpoint_log_path(root).is_file()
    {
        return Ok(false);
    }
    install_encoded_metadata_table_snapshot(root, &snapshot_bytes)?;
    remove_metadata_table_checkpoint_log(root)?;
    advance_activated_handoff_rebind_checkpoint(root, &prefix_proof)?;
    // The installed snapshot and newer table-log suffix are durable before
    // source history is pruned. Retaining the checkpoint journal segment
    // leaves a discontinuity that forces lagging replicas through snapshot
    // installation rather than reporting a false empty suffix.
    prune_segmented_journal_before_checkpoint(root, checkpoint_sequence)?;
    Ok(true)
}

fn membership_handoff_write_freeze_path(root: &Path) -> PathBuf {
    root.join("journal")
        .join("membership-handoff-write-freeze.sobj")
}

/// Advance an activated handoff's restart proof before checkpoint pruning can
/// remove the journal entries that connect the previous proof to this one.
///
/// The marker write is atomic and happens while the caller holds the shard
/// write lock. A crash before this write leaves the old journal history
/// available; a crash after it can safely restart validation at the new exact
/// prefix.
fn advance_activated_handoff_rebind_checkpoint(
    root: &Path,
    checkpoint: &DfsMetadataPrefixProof,
) -> Result<()> {
    let path = membership_handoff_write_freeze_path(root);
    let Some(mut freeze) = read_sobj_optional::<DfsMetadataShardWriteFreeze>(&path)? else {
        return Ok(());
    };
    if freeze.activated_epoch.is_none() {
        return Ok(());
    }
    if !checkpoint.is_well_formed() {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table checkpoint at {} has a malformed exact prefix proof",
            root.display()
        )));
    }

    let base = DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_VERSION,
        journal_sequence: freeze.journal_sequence,
        digest: freeze.state_fingerprint.clone(),
    };
    let mut reconstructed = freeze
        .rebind_prefix_proof
        .clone()
        .unwrap_or_else(|| base.clone());
    if !base.is_well_formed()
        || !reconstructed.is_well_formed()
        || reconstructed.journal_sequence < base.journal_sequence
        || (reconstructed.journal_sequence == base.journal_sequence
            && reconstructed.digest != base.digest)
    {
        return Err(MetadataBackendError::Serde(format!(
            "metadata shard handoff {} at {} has an invalid rebind prefix checkpoint",
            freeze.handoff_id,
            root.display()
        )));
    }
    if checkpoint.journal_sequence < reconstructed.journal_sequence {
        // A background worker may finish an older captured generation after a
        // newer synchronous checkpoint has already advanced the marker.
        return Ok(());
    }
    if checkpoint.journal_sequence == reconstructed.journal_sequence {
        if checkpoint.digest != reconstructed.digest {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard handoff {} at {} has divergent prefix proofs at sequence {}",
                freeze.handoff_id,
                root.display(),
                checkpoint.journal_sequence
            )));
        }
    } else {
        while reconstructed.journal_sequence < checkpoint.journal_sequence {
            let entries =
                durable_journal_entries_since_limited(root, reconstructed.journal_sequence, 1024)?;
            if entries.is_empty() {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard handoff {} at {} cannot advance rebind proof from sequence {} to checkpoint {}",
                    freeze.handoff_id,
                    root.display(),
                    reconstructed.journal_sequence,
                    checkpoint.journal_sequence
                )));
            }
            let previous_sequence = reconstructed.journal_sequence;
            for entry in entries {
                if entry.sequence > checkpoint.journal_sequence {
                    break;
                }
                if entry.sequence != reconstructed.journal_sequence.saturating_add(1) {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard handoff {} at {} has a journal gap after sequence {}",
                        freeze.handoff_id,
                        root.display(),
                        reconstructed.journal_sequence
                    )));
                }
                reconstructed =
                    dfs_metadata_prefix_proof_after(&reconstructed, &entry).map_err(|source| {
                        MetadataBackendError::Serde(format!(
                            "metadata shard handoff {} at {} cannot encode rebind entry {}: {source}",
                            freeze.handoff_id,
                            root.display(),
                            entry.sequence
                        ))
                })?;
            }
            if reconstructed.journal_sequence <= previous_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard handoff {} at {} made no rebind checkpoint progress",
                    freeze.handoff_id,
                    root.display()
                )));
            }
        }
        if &reconstructed != checkpoint {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard handoff {} at {} checkpoint proof diverges at sequence {}",
                freeze.handoff_id,
                root.display(),
                checkpoint.journal_sequence
            )));
        }
    }

    if freeze.rebind_prefix_proof.as_ref() != Some(checkpoint) {
        freeze.rebind_prefix_proof = Some(checkpoint.clone());
        write_sobj_atomic(&path, &freeze)?;
    }
    Ok(())
}

fn append_table_batch(
    root: &Path,
    state: TableMaterializationState<'_>,
    sequence: u64,
    ops: Vec<DfsMetadataTableOp>,
    max_log_bytes: u64,
    mut rollback: Option<DfsMetadataTableRollback>,
) -> Result<()> {
    if let Err(error) = validate_version_pin_removal_order(state.tables, &ops, rollback.is_some()) {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(error);
    }
    let semantic_op_count = ops
        .iter()
        .filter(|op| matches!(op, DfsMetadataTableOp::CommitNamespacePrepare { .. }))
        .count();
    // Foreground callers must isolate a semantic marker so dependency
    // validation observes the exact preceding durable table state. Replica
    // replay can legitimately preapply several journal entries in order and
    // then append one table batch containing Prepared plus Commit (or multiple
    // independent commits); its rollback proves that ordered validation has
    // already succeeded.
    if rollback.is_none() && semantic_op_count > 0 && (semantic_op_count != 1 || ops.len() != 1) {
        if let Some(rollback) = rollback.take() {
            rollback.rollback(state.tables);
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {sequence} must isolate its compact namespace commit marker"
        )));
    }
    if rollback.is_none() {
        for op in &ops {
            if let Err(error) = state.tables.validate_checked(op) {
                return Err(error);
            }
        }
    }
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
            state.tables.apply_checked(op)?;
        }
    }
    *state.materialized_through_sequence = (*state.materialized_through_sequence).max(sequence);
    schedule_metadata_table_checkpoint_if_needed(root, max_log_bytes)
}

fn validate_version_pin_removal_order(
    tables: &DfsMetadataTables,
    ops: &[DfsMetadataTableOp],
    preapplied: bool,
) -> Result<()> {
    let mut pins = tables.version_pins.keys().cloned().collect::<BTreeSet<_>>();
    if !preapplied {
        for op in ops {
            match op {
                DfsMetadataTableOp::PutVersionPin { pin } => {
                    pins.insert((
                        pin.object_id.clone(),
                        pin.version.clone(),
                        pin.pin_id.clone(),
                    ));
                }
                DfsMetadataTableOp::RemoveVersionPin {
                    object_id,
                    version,
                    pin_id,
                } => {
                    pins.remove(&(object_id.clone(), version.clone(), pin_id.clone()));
                }
                DfsMetadataTableOp::ReconcileCrossShardPinTransfer { transfer, .. } => {
                    if let Some(previous) = tables
                        .cross_shard_pin_transfers
                        .get(&(transfer.object_id.clone(), transfer.transfer_id.clone()))
                    {
                        for pin in &previous.copied_pins {
                            if !transfer.copied_pins.iter().any(|current| current == pin) {
                                pins.remove(&(
                                    pin.object_id.clone(),
                                    pin.version.clone(),
                                    pin.pin_id.clone(),
                                ));
                            }
                        }
                    }
                    for pin in &transfer.source_pins {
                        pins.insert((
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ));
                    }
                }
                DfsMetadataTableOp::FinalizeCrossShardSourceEviction { removed_pins, .. } => {
                    for pin in removed_pins {
                        pins.remove(&(
                            pin.object_id.clone(),
                            pin.version.clone(),
                            pin.pin_id.clone(),
                        ));
                    }
                }
                DfsMetadataTableOp::RemoveVersion { object_id, version }
                    if pins.iter().any(|(pin_object_id, pin_version, _)| {
                        pin_object_id == object_id && pin_version == version
                    }) =>
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata table batch cannot remove pinned version {object_id}/{version}"
                    )));
                }
                _ => {}
            }
        }
        return Ok(());
    }
    for op in ops {
        if let DfsMetadataTableOp::RemoveVersion { object_id, version } = op
            && pins.iter().any(|(pin_object_id, pin_version, _)| {
                pin_object_id == object_id && pin_version == version
            })
        {
            return Err(MetadataBackendError::Serde(format!(
                "preapplied metadata table batch cannot remove pinned version {object_id}/{version}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsFileSystemId;
    use crate::metadata_io::write_file_atomic;
    use crate::metadata_read_snapshot::current_tables_snapshot;
    use crate::metadata_records::{
        DfsMetadataShardId, DfsNamespacePrepareRecord, DfsNamespacePrepareStatus,
        DfsNamespaceTransaction, dfs_metadata_prefix_proof_seed,
    };
    use crate::metadata_table_store::{metadata_table_snapshot_path, read_metadata_table_state};
    use std::sync::{Arc, Mutex};
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

    fn namespace_prepare(
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> DfsNamespacePrepareRecord {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("fs_table_semantic_prepare").expect("filesystem");
        DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("shard_table_semantic_prepare"),
            owner_epoch: 7,
            operation_digest: format!("digest_{operation_id}"),
            expected_object_versions: Vec::new(),
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: format!("txn_{operation_id}"),
                file_system_id,
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status,
        }
    }

    fn namespace_prepare_commit_op(prepare: &DfsNamespacePrepareRecord) -> DfsMetadataTableOp {
        DfsMetadataTableOp::CommitNamespacePrepare {
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.operation_digest.clone(),
            owner_epoch: prepare.owner_epoch,
        }
    }

    #[test]
    fn compact_namespace_commit_reopens_from_materialized_table_log() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-reopen");
        let shard_id = DfsMetadataShardId::new("shard_table_semantic_prepare");
        let prepare = namespace_prepare(
            "operation_semantic_reopen",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "semantic-reopen-prepare",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![DfsMetadataTableOp::PutNamespacePrepare {
                prepare: prepare.clone(),
            }],
            u64::MAX,
        )
        .expect("append prepared body");

        prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            2,
            "semantic-reopen-commit",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            2,
            vec![namespace_prepare_commit_op(&prepare)],
            u64::MAX,
        )
        .expect("append compact commit marker");

        assert_eq!(
            tables
                .namespace_prepares
                .get(&prepare.operation_id)
                .map(|prepare| &prepare.status),
            Some(&DfsNamespacePrepareStatus::Committed)
        );
        assert!(
            tables
                .namespace_transactions
                .contains_key(&prepare.transaction.transaction_id)
        );

        let reopened =
            read_metadata_table_state(root.path()).expect("replay materialized table log");
        assert_eq!(reopened.tables, tables);
        assert_eq!(reopened.materialized_through_sequence, Some(2));
    }

    #[test]
    fn compact_namespace_commit_dependency_rejects_before_durable_append() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-missing-prepare");
        let prepare = namespace_prepare(
            "operation_semantic_missing",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &prepare.shard_id,
            1,
            "semantic-missing-prepare",
        ));

        let error = append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_prepare_commit_op(&prepare)],
            u64::MAX,
        )
        .expect_err("a semantic marker without its prepared body must fail");

        assert!(matches!(error, MetadataBackendError::Serde(_)));
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
        assert_eq!(
            read_metadata_table_state(root.path())
                .expect("empty table state")
                .tables,
            tables
        );
    }

    #[test]
    fn compact_namespace_commit_fences_digest_epoch_and_aborted_prepare() {
        let prepare = namespace_prepare(
            "operation_semantic_fences",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: prepare.clone(),
        });

        let mut wrong_digest = namespace_prepare_commit_op(&prepare);
        let DfsMetadataTableOp::CommitNamespacePrepare {
            operation_digest, ..
        } = &mut wrong_digest
        else {
            unreachable!("compact marker")
        };
        *operation_digest = "wrong_digest".to_string();
        assert!(tables.validate_checked(&wrong_digest).is_err());

        let mut wrong_epoch = namespace_prepare_commit_op(&prepare);
        let DfsMetadataTableOp::CommitNamespacePrepare { owner_epoch, .. } = &mut wrong_epoch
        else {
            unreachable!("compact marker")
        };
        *owner_epoch += 1;
        assert!(tables.validate_checked(&wrong_epoch).is_err());

        let mut aborted = prepare.clone();
        aborted.status = DfsNamespacePrepareStatus::Aborted;
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare { prepare: aborted });
        assert!(
            tables
                .validate_checked(&namespace_prepare_commit_op(&prepare))
                .is_err()
        );
    }

    #[test]
    fn compact_namespace_commit_append_failure_rolls_back_materialization() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-semantic-append-failure");
        let blocked_root = root.path().join("not-a-directory");
        fs::write(&blocked_root, b"block metadata table directory").expect("blocking file");
        let prepare = namespace_prepare(
            "operation_semantic_rollback",
            DfsNamespacePrepareStatus::Prepared,
        );
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare {
            prepare: prepare.clone(),
        });
        let before = tables.clone();
        let marker = namespace_prepare_commit_op(&prepare);
        let mut rollback = DfsMetadataTableRollback::default();
        tables
            .apply_checked_with_rollback(marker.clone(), &mut rollback)
            .expect("preapply compact commit");
        assert_ne!(tables, before);

        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &prepare.shard_id,
            1,
            "semantic-append-failure",
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
            vec![marker],
            u64::MAX,
            rollback,
        )
        .expect_err("failed marker append must roll back its materialized state");

        assert_eq!(tables, before);
        assert_eq!(table_log_offset, 0);
        assert_eq!(materialized_through_sequence, 0);
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

    #[test]
    fn reopen_preserves_unjournaled_history_across_snapshot_install_crash_window() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-snapshot-prefix-crash-window");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_snapshot_crash"),
            1,
            "snapshot-crash-state",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![namespace_transaction_op("txn_snapshot_crash")],
            u64::MAX,
        )
        .expect("append table prefix");

        let checkpoint_tables = tables.clone();
        let snapshot_bytes =
            encode_metadata_table_snapshot(&checkpoint_tables, Some(1), prefix_proof.as_ref())
                .expect("encode checkpoint");
        // Deliberately stop after the first half of snapshot+log publication:
        // the old sequence-1 table record is still present.
        write_file_atomic(&metadata_table_snapshot_path(root.path()), &snapshot_bytes)
            .expect("install snapshot without rewriting the table log");

        let reopened = read_metadata_table_state(root.path()).expect("reopen crash window");
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_snapshot_crash"),
            "checkpoint publication must preserve full history that was not removed by a journal mutation"
        );
        assert!(
            reopened.tables.namespace_transaction_receipts.is_empty(),
            "a checkpoint must not invent an unjournaled compact receipt"
        );
        assert_eq!(reopened.materialized_through_sequence, Some(1));
    }

    #[test]
    fn background_checkpoint_runs_off_the_append_path_and_installs_exact_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-background-checkpoint");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("shard_table_background"),
            1,
            "background-checkpoint-state",
        ));
        let mut op = namespace_transaction_op("txn_background_checkpoint");
        let DfsMetadataTableOp::PutNamespaceTransaction { transaction } = &mut op else {
            unreachable!("namespace transaction test op");
        };
        transaction.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
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
            0,
        )
        .expect("foreground append only schedules checkpoint");
        assert!(
            tables
                .namespace_transactions
                .contains_key("txn_background_checkpoint"),
            "the worker must not mutate the caller's live tables behind its lock"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let running = METADATA_TABLE_CHECKPOINT_WORKERS
                .get()
                .is_some_and(|workers| {
                    workers
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .contains(root.path())
                });
            if !running {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background checkpoint did not finish"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let reopened = read_metadata_table_state(root.path()).expect("read checkpoint");
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_background_checkpoint"),
            "background checkpointing must preserve full history absent journaled GC"
        );
        assert!(
            reopened.tables.namespace_transaction_receipts.is_empty(),
            "background checkpointing must not invent compact receipts"
        );
        assert_eq!(reopened.materialized_through_sequence, Some(1));
        assert_eq!(
            fs::metadata(metadata_table_path(root.path()))
                .expect("table log")
                .len(),
            0,
            "a checkpoint with no concurrent suffix should rotate the table log"
        );
    }

    #[test]
    fn checkpoint_install_preserves_records_appended_after_captured_boundary() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-suffix");
        let shard_id = DfsMetadataShardId::new("shard_table_suffix");
        let mut tables = DfsMetadataTables::default();
        let mut table_log_offset = 0;
        let mut materialized_through_sequence = 0;
        let mut table_snapshot_fingerprint = None;
        let mut prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            1,
            "checkpoint-suffix-one",
        ));
        let mut first_op = namespace_transaction_op("txn_checkpoint_prefix");
        let DfsMetadataTableOp::PutNamespaceTransaction { transaction } = &mut first_op else {
            unreachable!("namespace transaction test op");
        };
        transaction.created_at = OffsetDateTime::now_utc();
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            1,
            vec![first_op],
            u64::MAX,
        )
        .expect("append captured prefix");
        rotate_metadata_table_log_for_checkpoint(root.path(), 0)
            .expect("rotate captured table generation");
        let capture =
            read_metadata_table_checkpoint_state(root.path()).expect("capture table boundary");
        let checkpoint_tables = capture.tables;
        let snapshot_bytes =
            encode_metadata_table_snapshot(&checkpoint_tables, Some(1), prefix_proof.as_ref())
                .expect("encode captured snapshot");

        prefix_proof = Some(dfs_metadata_prefix_proof_seed(
            &shard_id,
            2,
            "checkpoint-suffix-two",
        ));
        append_and_apply_table_ops(
            root.path(),
            TableMaterializationState {
                tables: &mut tables,
                table_log_offset: &mut table_log_offset,
                materialized_through_sequence: &mut materialized_through_sequence,
                table_snapshot_fingerprint: &mut table_snapshot_fingerprint,
                prefix_proof: &mut prefix_proof,
            },
            2,
            vec![namespace_transaction_op("txn_checkpoint_suffix")],
            u64::MAX,
        )
        .expect("append newer suffix");

        // A long-lived backend can still hold a pre-rotation in-memory view
        // while another process owns the checkpoint worker. The checkpoint
        // generation is therefore part of the read-cache generation: a
        // lock-free read must replay both it and the active suffix instead of
        // trusting the active log's reset byte length.
        let stale_tables = DfsMetadataTables::default();
        let read_cache = Arc::new(Mutex::new(None));
        let during_checkpoint =
            current_tables_snapshot(root.path(), &stale_tables, 0, 0, &None, &read_cache)
                .expect("read across checkpoint generation");
        assert!(
            during_checkpoint
                .namespace_transactions
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(
            during_checkpoint
                .namespace_transactions
                .contains_key("txn_checkpoint_suffix")
        );

        install_encoded_metadata_table_snapshot(root.path(), &snapshot_bytes)
            .expect("install checkpoint with suffix");
        remove_metadata_table_checkpoint_log(root.path())
            .expect("retire captured table generation");

        let reopened = read_metadata_table_state(root.path()).expect("reopen checkpoint suffix");
        assert_eq!(reopened.materialized_through_sequence, Some(2));
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_checkpoint_prefix")
        );
        assert!(reopened.tables.namespace_transaction_receipts.is_empty());
        assert!(
            reopened
                .tables
                .namespace_transactions
                .contains_key("txn_checkpoint_suffix")
        );
    }
}
