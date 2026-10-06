//! Durable local reader-floor admission for namespaced metadata effects.

use crate::metadata_tables::DfsMetadataTables;
use crate::*;

fn version_requires_floor(version: &DfsObjectVersionRecord) -> bool {
    version.metadata_format == DfsRevisionMetadataFormat::NamespacedV6
        && (!version.retention.is_empty() || !version.module_metadata.is_empty())
}

fn object_requires_floor(tables: &DfsMetadataTables, object: &DfsObjectMetadata) -> bool {
    if crate::file_history::publication_adds_retention_grant(tables, object) {
        return true;
    }
    tables
        .objects
        .get(&object.object_id)
        .and_then(|old| {
            tables
                .versions
                .get(&(old.object_id.clone(), old.version.clone()))
        })
        .is_some_and(|version| version.retention.values().any(|state| state.order != 0))
}

fn transaction_requires_floor(tables: &DfsMetadataTables, tx: &DfsNamespaceTransaction) -> bool {
    tx.content_reference_identity.is_some()
        || !tx.extension_expectations.is_empty()
        || tx.contribution_digest.is_some()
        || tx.versions.iter().any(version_requires_floor)
        || tx
            .objects
            .iter()
            .any(|object| object_requires_floor(tables, object))
        || tx.file_version_pins.as_ref().is_some_and(|pins| {
            pins.add
                .iter()
                .chain(&pins.remove)
                .any(|pin| pin.namespace.is_some())
        })
}

pub(crate) fn mutation_requires_floor(
    tables: &DfsMetadataTables,
    mutation: &DfsMetadataMutation,
) -> bool {
    match mutation {
        DfsMetadataMutation::PutFileSystem {file_system:Some(fs),..} => fs.metadata_format == DfsRevisionMetadataFormat::NamespacedV6 && (!fs.retention_policies.is_empty() || !fs.module_metadata.is_empty()),
        DfsMetadataMutation::PutVersion {version_record:Some(version),..} => version_requires_floor(version),
        DfsMetadataMutation::CaptureFileVersion {record,pin,..} => version_requires_floor(record) || pin.namespace.is_some(),
        DfsMetadataMutation::PutVersionPin {pin,..} => pin.namespace.is_some(),
        DfsMetadataMutation::PutObject {object:Some(object),..} => object_requires_floor(tables,object),
        DfsMetadataMutation::CommitVisibleVersion {visible_version,committed_object,..} => visible_version.as_ref().is_some_and(version_requires_floor) || committed_object.as_ref().is_some_and(|object|object_requires_floor(tables,object)),
        DfsMetadataMutation::ApplyNamespaceTransaction {transaction:Some(tx),..} => transaction_requires_floor(tables,tx),
        DfsMetadataMutation::PutNamespaceTransactionReceipt {receipt,..} => receipt.contribution_digest.is_some() || receipt.content_reference_identity.is_some(),
        DfsMetadataMutation::RemoveNamespaceTransactionReceipt {retention,..} => !retention.content_reference_outcome_retention.is_empty(),
        DfsMetadataMutation::PutIntent {intent:Some(intent),..} => matches!(&intent.publication,DfsWritePublication::Retained {capture} if capture.metadata_format == DfsRevisionMetadataFormat::NamespacedV6),
        DfsMetadataMutation::ReconcileCrossShardPinTransfer {versions,transfer,..} => versions.iter().any(version_requires_floor) || transfer.source_pins.iter().chain(&transfer.copied_pins).any(|pin|pin.namespace.is_some()),
        DfsMetadataMutation::PutNamespacePrepare {operation_id,prepare,packed_prepare_v2,..} => crate::metadata_records::is_default_layout_migration_fence(operation_id) || prepare.as_ref().is_some_and(|prepare|transaction_requires_floor(tables,&prepare.transaction)) || packed_prepare_v2.as_ref().is_some_and(|prepare|prepare.requires_namespaced_reader()),
        DfsMetadataMutation::PutNamespacePrepareReceipt {operation_id,..} => crate::metadata_records::is_default_layout_migration_fence(operation_id),
        DfsMetadataMutation::PutNamespaceRequest {request,..} => match &request.operation {
            DfsNamespaceOperation::ContentReference {request} => request.wire_format == DfsContentReferenceFormat::NamespacedV6,
            DfsNamespaceOperation::CommitLocalEventualBatch {transaction,..} => transaction_requires_floor(tables,transaction),
            _=>false,
        },
        DfsMetadataMutation::PutNamespaceJob {job:Some(job),..} => job.pending_frames.iter().any(|frame|matches!(frame,DfsNamespaceJobFrame::ContentCopy {state} if state.source_reference.as_ref().is_some_and(|reference|reference.wire_format == DfsContentReferenceFormat::NamespacedV6))),
        _=>false,
    }
}

/// The source budget is physical and bounded, so a tiny materialized view cannot
/// accidentally trigger replay of a huge log while holding the writer lock.
pub(crate) fn small_durable_source(
    root: &std::path::Path,
    replay_bytes: u64,
    unpriced: u64,
) -> Result<bool, MetadataBackendError> {
    if unpriced != 0 {
        return Ok(false);
    }
    let mut bytes = replay_bytes;
    for path in [
        crate::metadata_table_store::metadata_table_snapshot_path(root),
        crate::metadata_table_store::metadata_table_checkpoint_log_path(root),
        crate::metadata_table_store::metadata_table_path(root),
    ] {
        match std::fs::metadata(&path) {
            Ok(metadata) => bytes = bytes.saturating_add(metadata.len()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        }
        if bytes > 512 * 1024 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Full snapshot operations already enumerate their source. This predicate is
/// never used by foreground per-object reads or writes.
pub(crate) fn tables_require_floor(tables: &DfsMetadataTables) -> bool {
    !tables.namespace_receipt_retention.content_reference_outcome_retention.is_empty() || tables.file_systems.values().any(|fs| {
        fs.metadata_format == DfsRevisionMetadataFormat::NamespacedV6
            && (!fs.retention_policies.is_empty() || !fs.module_metadata.is_empty())
    }) || tables.versions.values().any(version_requires_floor)
        || tables
            .version_pins
            .values()
            .any(|pin| pin.namespace.is_some())
        || tables
            .namespace_transactions
            .values()
            .any(|tx| tx.contribution_digest.is_some() || !tx.extension_expectations.is_empty())
        || tables.namespace_transaction_receipts.values().any(|receipt|receipt.contribution_digest.is_some())
        || tables.intents.values().any(|intent|matches!(&intent.publication,DfsWritePublication::Retained {capture} if capture.metadata_format==DfsRevisionMetadataFormat::NamespacedV6))
        || tables.intent_receipts.values().any(|receipt|matches!(&receipt.publication,DfsWritePublication::Retained {capture} if capture.metadata_format==DfsRevisionMetadataFormat::NamespacedV6))
        || tables.namespace_prepares.values().any(|prepare|crate::metadata_records::is_default_layout_migration_fence(&prepare.operation_id) || transaction_requires_floor(tables,&prepare.transaction))
        || tables.namespace_prepare_receipts.values().any(|receipt|crate::metadata_records::is_default_layout_migration_fence(&receipt.operation_id))
        || tables.namespace_requests.values().any(|request|matches!(&request.operation,DfsNamespaceOperation::ContentReference {request} if request.wire_format==DfsContentReferenceFormat::NamespacedV6))
        || tables.namespace_jobs.values().any(|job|job.pending_frames.iter().any(|frame|matches!(frame,DfsNamespaceJobFrame::ContentCopy {state} if state.source_reference.as_ref().is_some_and(|reference|reference.wire_format==DfsContentReferenceFormat::NamespacedV6))))
}
