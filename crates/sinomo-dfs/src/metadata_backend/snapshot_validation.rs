//! Snapshot validation for the local metadata backend.

use super::*;

pub(super) fn require_replica_snapshot_record_file_system(
    record_kind: &str,
    actual: &DfsFileSystemId,
    expected: &DfsFileSystemId,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    Err(MetadataBackendError::Serde(format!(
        "replicated metadata snapshot {record_kind} filesystem {actual} does not match permit filesystem {expected}"
    )))
}

pub(super) fn require_replica_snapshot_namespace_transaction_file_system(
    record_kind: &str,
    transaction: &DfsNamespaceTransaction,
    expected: &DfsFileSystemId,
) -> Result<()> {
    transaction
        .validate_content_reference_annotation()
        .map_err(MetadataBackendError::Serde)?;
    if let Some(pins) = &transaction.file_version_pins {
        pins.validate_scope(expected)
            .map_err(MetadataBackendError::Serde)?;
    }
    require_replica_snapshot_record_file_system(
        record_kind,
        &transaction.file_system_id,
        expected,
    )?;
    for object in &transaction.objects {
        require_replica_snapshot_record_file_system(
            "namespace transaction object",
            &object.file_system_id,
            expected,
        )?;
    }
    for version in &transaction.versions {
        crate::file_history::validate_file_version_record(version)?;
        require_replica_snapshot_record_file_system(
            "namespace transaction version",
            &version.file_system_id,
            expected,
        )?;
    }
    for state in &transaction.advisory_lock_states {
        require_replica_snapshot_record_file_system(
            "namespace transaction advisory-lock state",
            &state.file_system_id,
            expected,
        )?;
    }
    Ok(())
}

/// Validate the complete canonical table image before replica snapshot ingest
/// acquires the backend writer lock. Derived indexes are included so a legacy
/// op snapshot and a structured snapshot are checked at the same boundary.
pub(super) fn require_replica_snapshot_tables_file_system(
    tables: &DfsMetadataTables,
    expected: &DfsFileSystemId,
) -> Result<()> {
    for state in tables
        .namespace_receipt_retention
        .content_reference_outcome_retention
        .values()
    {
        state
            .validate_file_system_scope(expected)
            .map_err(MetadataBackendError::Serde)?;
    }
    for file_system in tables.file_systems.values() {
        crate::validate_retention_policies(&file_system.retention_policies)
            .map_err(MetadataBackendError::Serde)?;
        crate::validate_module_metadata_map(&file_system.module_metadata)
            .map_err(MetadataBackendError::Serde)?;
        require_replica_snapshot_record_file_system(
            "filesystem record",
            &file_system.file_system_id,
            expected,
        )?;
    }
    for object in tables.objects.values() {
        require_replica_snapshot_record_file_system(
            "object record",
            &object.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.file_system_objects.values() {
        require_replica_snapshot_record_file_system(
            "filesystem-object index",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.paths.values() {
        require_replica_snapshot_record_file_system(
            "path record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for inode in tables.inodes.values() {
        require_replica_snapshot_record_file_system(
            "inode record",
            &inode.file_system_id,
            expected,
        )?;
    }
    for dirfrag in tables.dirfrags.values() {
        require_replica_snapshot_record_file_system(
            "directory-fragment record",
            &dirfrag.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.dentries.values() {
        require_replica_snapshot_record_file_system(
            "dentry record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.hardlinks.values() {
        require_replica_snapshot_record_file_system(
            "hardlink record",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for path_record in tables.hardlink_paths.values() {
        require_replica_snapshot_record_file_system(
            "hardlink-path index",
            &path_record.file_system_id,
            expected,
        )?;
    }
    for object in tables.compacted_tombstones.values() {
        require_replica_snapshot_record_file_system(
            "compacted tombstone",
            &object.file_system_id,
            expected,
        )?;
    }
    for (file_system_id, _) in tables.compacted_tombstone_paths.keys() {
        require_replica_snapshot_record_file_system(
            "compacted-tombstone path index",
            file_system_id,
            expected,
        )?;
    }
    for version in tables.versions.values() {
        crate::file_history::validate_file_version_record(version)?;
        require_replica_snapshot_record_file_system(
            "version record",
            &version.file_system_id,
            expected,
        )?;
    }
    for pin in tables.version_pins.values() {
        require_replica_snapshot_record_file_system(
            "durable version pin",
            &pin.file_system_id,
            expected,
        )?;
    }
    for transfer in tables.cross_shard_pin_transfers.values() {
        require_replica_snapshot_record_file_system(
            "cross-shard pin transfer",
            &transfer.file_system_id,
            expected,
        )?;
        for pin in &transfer.source_pins {
            require_replica_snapshot_record_file_system(
                "cross-shard transfer-owned pin",
                &pin.file_system_id,
                expected,
            )?;
        }
    }
    for receipt in tables.receipts.values() {
        require_replica_snapshot_record_file_system(
            "receipt record",
            &receipt.receipt.file_system_id,
            expected,
        )?;
    }
    for intent in tables.intents.values() {
        crate::file_history::validate_intent_publication(intent)?;
        require_replica_snapshot_record_file_system(
            "intent record",
            &intent.file_system_id,
            expected,
        )?;
        if let Some(object) = intent.object_metadata.as_ref() {
            require_replica_snapshot_record_file_system(
                "intent object record",
                &object.file_system_id,
                expected,
            )?;
        }
    }
    for receipt in tables.intent_receipts.values() {
        if receipt.publication.is_retained_repair() {
            return Err(MetadataBackendError::Serde(
                "a repair owner is not a user retry receipt".into(),
            ));
        }
        crate::file_history::validate_intent_publication(&receipt.to_intent_record())?;
        require_replica_snapshot_record_file_system(
            "intent receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for conflict in tables.conflicts.values() {
        require_replica_snapshot_record_file_system(
            "conflict record",
            &conflict.file_system_id,
            expected,
        )?;
    }
    for state in tables.advisory_lock_states.values() {
        require_replica_snapshot_record_file_system(
            "advisory-lock state",
            &state.file_system_id,
            expected,
        )?;
    }
    for commit in tables.visible_commits.values() {
        crate::file_history::validate_file_version_record(&commit.visible_version)?;
        require_replica_snapshot_record_file_system(
            "visible-commit object record",
            &commit.committed_object.file_system_id,
            expected,
        )?;
        require_replica_snapshot_record_file_system(
            "visible-commit version record",
            &commit.visible_version.file_system_id,
            expected,
        )?;
    }
    for transaction in tables.namespace_transactions.values() {
        require_replica_snapshot_namespace_transaction_file_system(
            "namespace transaction",
            transaction,
            expected,
        )?;
    }
    for receipt in tables.namespace_transaction_receipts.values() {
        receipt
            .validate_content_reference_annotation()
            .map_err(MetadataBackendError::Serde)?;
        require_replica_snapshot_record_file_system(
            "namespace transaction receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for prepare in tables.namespace_prepares.values() {
        prepare
            .validate_default_layout_migration_fence()
            .map_err(MetadataBackendError::Serde)?;
        require_replica_snapshot_record_file_system(
            "namespace prepare",
            &prepare.file_system_id,
            expected,
        )?;
        require_replica_snapshot_namespace_transaction_file_system(
            "namespace prepare transaction",
            &prepare.transaction,
            expected,
        )?;
    }
    for receipt in tables.namespace_prepare_receipts.values() {
        if crate::metadata_records::is_default_layout_migration_fence(&receipt.operation_id)
            && receipt.status != DfsNamespacePrepareStatus::Aborted
        {
            return Err(MetadataBackendError::Serde(
                "default layout migration fence snapshot receipt must record abort".into(),
            ));
        }
        require_replica_snapshot_record_file_system(
            "namespace prepare receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for job in tables.namespace_jobs.values() {
        crate::file_history::validate_file_restore_job(job)?;
        require_replica_snapshot_record_file_system(
            "namespace job",
            &job.file_system_id,
            expected,
        )?;
    }
    for receipt in tables.namespace_job_receipts.values() {
        crate::file_history::validate_file_restore_job_receipt(receipt)?;
        require_replica_snapshot_record_file_system(
            "namespace job receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    for request in tables.namespace_requests.values() {
        require_replica_snapshot_record_file_system(
            "namespace request",
            &request.file_system_id,
            expected,
        )?;
    }
    for receipt in tables.namespace_request_receipts.values() {
        require_replica_snapshot_record_file_system(
            "namespace request receipt",
            &receipt.file_system_id,
            expected,
        )?;
    }
    Ok(())
}

pub(super) fn require_version_pin_table_invariants(tables: &DfsMetadataTables) -> Result<()> {
    for version in tables.versions.values() {
        crate::file_history::validate_file_version_record(version)?;
    }
    let mut pin_counts = BTreeMap::<DfsObjectId, usize>::new();
    for pin in tables.version_pins.values() {
        let count = pin_counts.entry(pin.object_id.clone()).or_default();
        *count = count.saturating_add(1);
        if *count > MAX_DURABLE_VERSION_PINS_PER_OBJECT {
            return Err(MetadataBackendError::Serde(format!(
                "durable snapshot object {} exceeds the maximum {} version pins",
                pin.object_id, MAX_DURABLE_VERSION_PINS_PER_OBJECT
            )));
        }
        let version = tables
            .versions
            .get(&(pin.object_id.clone(), pin.version.clone()))
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "durable version pin {} for {}/{} references a missing metadata version",
                    pin.pin_id, pin.object_id, pin.version
                ))
            })?;
        if version.file_system_id != pin.file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "durable version pin {} for {}/{} has filesystem {}, but its metadata version has filesystem {}",
                pin.pin_id, pin.object_id, pin.version, pin.file_system_id, version.file_system_id
            )));
        }
    }
    for transfer in tables.cross_shard_pin_transfers.values() {
        validate_cross_shard_pin_transfer_table_transition(
            tables,
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.transfer_id,
            false,
            &[],
            transfer,
        )?;
        for pin in &transfer.copied_pins {
            let key = (
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            );
            if tables.version_pins.get(&key) != Some(pin) {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} does not own its exact pin {} for {}/{}",
                    transfer.transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
        }
    }
    Ok(())
}
