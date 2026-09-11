//! Metadata journal replay helpers.
//!
//! Journal entries can carry enough typed payload to replay directly into
//! materialized tables. This module owns the small validation helpers used by
//! replay paths so backend code can focus on applying mutations.

use std::borrow::Cow;

use crate::DfsFileSystemId;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::metadata_records::{
    DfsMetadataJournalEntry, DfsMetadataMutation, DfsNamespacePreparePayloadRef,
    DfsNamespaceTransaction,
};
use crate::metadata_tables::DfsMetadataTables;

pub(crate) fn borrowed_namespace_prepare_payload(
    entry: &DfsMetadataJournalEntry,
) -> Result<Option<DfsNamespacePreparePayloadRef<'_>>> {
    entry
        .mutation
        .namespace_prepare_payload_ref()
        .map_err(|source| {
            MetadataBackendError::Serde(format!(
                "metadata journal entry {}:{} has invalid namespace prepare payload: {source}",
                entry.shard_id.0, entry.sequence
            ))
        })
}

#[cfg(test)]
pub(crate) fn decoded_namespace_prepare_payload(
    entry: &DfsMetadataJournalEntry,
) -> Result<Option<Cow<'_, crate::DfsNamespacePrepareRecord>>> {
    borrowed_namespace_prepare_payload(entry)?
        .map(DfsNamespacePreparePayloadRef::into_canonical)
        .transpose()
        .map_err(|source| {
            MetadataBackendError::Serde(format!(
                "metadata journal entry {}:{} has invalid namespace prepare payload: {source}",
                entry.shard_id.0, entry.sequence
            ))
        })
}

pub(crate) fn decoded_normalized_namespace_prepare_payload(
    entry: &DfsMetadataJournalEntry,
) -> Result<Option<Cow<'_, crate::DfsNamespacePrepareRecord>>> {
    Ok(decoded_normalized_namespace_prepare_payload_with_ref(entry)?.map(|(_, prepare)| prepare))
}

pub(crate) fn decoded_normalized_namespace_prepare_payload_with_ref(
    entry: &DfsMetadataJournalEntry,
) -> Result<
    Option<(
        DfsNamespacePreparePayloadRef<'_>,
        Cow<'_, crate::DfsNamespacePrepareRecord>,
    )>,
> {
    let Some(payload) = borrowed_namespace_prepare_payload(entry)? else {
        return Ok(None);
    };
    let prepare = decoded_normalized_namespace_prepare_payload_from_validated_ref(entry, payload)?;
    Ok(Some((payload, prepare)))
}

pub(crate) fn decoded_normalized_namespace_prepare_payload_from_validated_ref<'a>(
    entry: &DfsMetadataJournalEntry,
    payload: DfsNamespacePreparePayloadRef<'a>,
) -> Result<Cow<'a, crate::DfsNamespacePrepareRecord>> {
    payload.into_normalized_canonical().map_err(|source| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {}:{} has invalid namespace prepare payload: {source}",
            entry.shard_id.0, entry.sequence
        ))
    })
}

pub(crate) fn required_replay_payload<'a, T>(
    entry: &DfsMetadataJournalEntry,
    mutation: &str,
    payload: Option<&'a T>,
) -> Result<&'a T> {
    payload.ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {}:{} cannot replay {mutation} without its record payload",
            entry.shard_id.0, entry.sequence
        ))
    })
}

pub(crate) fn journal_entry_has_replay_payloads(entry: &DfsMetadataJournalEntry) -> bool {
    match &entry.mutation {
        DfsMetadataMutation::PutFileSystem { file_system, .. } => file_system.is_some(),
        DfsMetadataMutation::PutObject { object, .. } => object.is_some(),
        DfsMetadataMutation::PutVersion { version_record, .. } => version_record.is_some(),
        DfsMetadataMutation::RemoveVersion { .. } => true,
        DfsMetadataMutation::PutVersionPin { .. }
        | DfsMetadataMutation::RemoveVersionPin { .. }
        | DfsMetadataMutation::ReconcileCrossShardPinTransfer { .. }
        | DfsMetadataMutation::RemoveCrossShardPinTransfer { .. }
        | DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. } => true,
        DfsMetadataMutation::PutReceipt { receipt, .. } => receipt.is_some(),
        DfsMetadataMutation::RemoveReceipt { .. } => true,
        DfsMetadataMutation::PutIntent { intent, .. } => intent.is_some(),
        DfsMetadataMutation::PutIntentReceipt { .. }
        | DfsMetadataMutation::RemoveIntentReceipt { .. }
        | DfsMetadataMutation::MergeIntentReceiptRetention { .. } => true,
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object,
            visible_version,
            ..
        } => committed_object.is_some() && visible_version.is_some(),
        DfsMetadataMutation::RemoveVisibleCommit { .. } => true,
        DfsMetadataMutation::PutConflict { conflict, .. } => conflict.is_some(),
        DfsMetadataMutation::PutAdvisoryLockState { state, .. } => state.is_some(),
        DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => transaction.is_some(),
        DfsMetadataMutation::PutNamespacePrepare { .. } => {
            borrowed_namespace_prepare_payload(entry).is_ok_and(|prepare| prepare.is_some())
        }
        DfsMetadataMutation::CommitNamespacePrepare { .. } => true,
        DfsMetadataMutation::PutNamespaceJob { job, .. } => job.is_some(),
        DfsMetadataMutation::PutNamespaceRequest { .. }
        | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { .. }
        | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
        | DfsMetadataMutation::RemoveNamespacePrepareReceipt { .. }
        | DfsMetadataMutation::PutNamespaceJobReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceJobReceipt { .. }
        | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => true,
        DfsMetadataMutation::RemoveNamespaceRequestReceipt { .. } => true,
    }
}

/// Validate payload completeness before sequence/fork checks. This keeps a
/// malformed replay diagnosable without weakening the later contiguous-prefix
/// validation. A missing conflict payload is intentional: that mutation also
/// represents removal of an existing conflict record.
pub(crate) fn require_journal_entry_replay_payloads(
    entry: &DfsMetadataJournalEntry,
) -> Result<Option<DfsNamespacePreparePayloadRef<'_>>> {
    if matches!(
        &entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare { .. }
    ) {
        return borrowed_namespace_prepare_payload(entry)?.map_or_else(
            || {
                Err(MetadataBackendError::Serde(format!(
                    "metadata journal entry {}:{} cannot replay put-namespace-prepare without its record payload",
                    entry.shard_id.0, entry.sequence
                )))
            },
            |prepare| Ok(Some(prepare)),
        );
    }
    match &entry.mutation {
        DfsMetadataMutation::PutFileSystem { file_system, .. } => {
            required_replay_payload(entry, "put-file-system", file_system.as_ref())?;
        }
        DfsMetadataMutation::PutObject { object, .. } => {
            required_replay_payload(entry, "put-object", object.as_ref())?;
        }
        DfsMetadataMutation::PutVersion { version_record, .. } => {
            required_replay_payload(entry, "put-version", version_record.as_ref())?;
        }
        DfsMetadataMutation::PutReceipt { receipt, .. } => {
            required_replay_payload(entry, "put-receipt", receipt.as_ref())?;
        }
        DfsMetadataMutation::RemoveVersion { .. }
        | DfsMetadataMutation::PutVersionPin { .. }
        | DfsMetadataMutation::RemoveVersionPin { .. }
        | DfsMetadataMutation::ReconcileCrossShardPinTransfer { .. }
        | DfsMetadataMutation::RemoveCrossShardPinTransfer { .. }
        | DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. }
        | DfsMetadataMutation::RemoveReceipt { .. }
        | DfsMetadataMutation::PutIntentReceipt { .. }
        | DfsMetadataMutation::RemoveIntentReceipt { .. }
        | DfsMetadataMutation::MergeIntentReceiptRetention { .. }
        | DfsMetadataMutation::RemoveVisibleCommit { .. }
        | DfsMetadataMutation::RemoveNamespaceJobReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { .. }
        | DfsMetadataMutation::RemoveNamespacePrepareReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceRequestReceipt { .. }
        | DfsMetadataMutation::CommitNamespacePrepare { .. }
        | DfsMetadataMutation::PutConflict { .. } => {}
        DfsMetadataMutation::PutIntent { intent, .. } => {
            required_replay_payload(entry, "put-intent", intent.as_ref())?;
        }
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object,
            visible_version,
            ..
        } => {
            required_replay_payload(
                entry,
                "commit-visible-version committed object",
                committed_object.as_ref(),
            )?;
            required_replay_payload(
                entry,
                "commit-visible-version visible version",
                visible_version.as_ref(),
            )?;
        }
        DfsMetadataMutation::PutAdvisoryLockState { state, .. } => {
            required_replay_payload(entry, "put-advisory-lock-state", state.as_ref())?;
        }
        DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
            required_replay_payload(entry, "apply-namespace-transaction", transaction.as_ref())?;
        }
        DfsMetadataMutation::PutNamespacePrepare { .. } => {
            unreachable!("namespace prepare payload handled before the general replay match")
        }
        DfsMetadataMutation::PutNamespaceJob { job, .. } => {
            required_replay_payload(entry, "put-namespace-job", job.as_ref())?;
        }
        DfsMetadataMutation::PutNamespaceRequest { .. }
        | DfsMetadataMutation::PutNamespaceTransactionReceipt { .. }
        | DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
        | DfsMetadataMutation::PutNamespaceJobReceipt { .. }
        | DfsMetadataMutation::PutNamespaceRequestReceipt { .. } => {}
    }
    Ok(None)
}

fn require_replica_record_file_system(
    entry: &DfsMetadataJournalEntry,
    record_kind: &str,
    actual: &DfsFileSystemId,
    expected: &DfsFileSystemId,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    Err(MetadataBackendError::Serde(format!(
        "metadata replica journal entry {}:{} {record_kind} filesystem {actual} does not match permit filesystem {expected}",
        entry.shard_id.0, entry.sequence
    )))
}

fn require_replica_namespace_transaction_file_system(
    entry: &DfsMetadataJournalEntry,
    record_kind: &str,
    transaction: &DfsNamespaceTransaction,
    expected: &DfsFileSystemId,
) -> Result<()> {
    require_replica_record_file_system(entry, record_kind, &transaction.file_system_id, expected)?;
    for object in &transaction.objects {
        require_replica_record_file_system(
            entry,
            "namespace transaction object",
            &object.file_system_id,
            expected,
        )?;
    }
    for version in &transaction.versions {
        require_replica_record_file_system(
            entry,
            "namespace transaction version",
            &version.file_system_id,
            expected,
        )?;
    }
    for state in &transaction.advisory_lock_states {
        require_replica_record_file_system(
            entry,
            "namespace transaction advisory-lock state",
            &state.file_system_id,
            expected,
        )?;
    }
    Ok(())
}

/// Validate the filesystem capability boundary for one replicated entry.
///
/// This checks both the mutation's outer routing identity and every typed
/// embedded record before the durable backend takes its writer lock. The
/// ordinary author path deliberately retains its existing replay semantics.
pub(crate) fn require_replica_journal_entry_file_system<'a>(
    entry: &'a DfsMetadataJournalEntry,
    expected: &DfsFileSystemId,
) -> Result<Option<DfsNamespacePreparePayloadRef<'a>>> {
    let namespace_prepare = require_journal_entry_replay_payloads(entry)?;
    let outer_file_system_id = entry.mutation.file_system_id();
    require_replica_record_file_system(entry, "outer mutation", outer_file_system_id, expected)?;

    match &entry.mutation {
        DfsMetadataMutation::PutFileSystem {
            file_system: Some(file_system),
            ..
        } => require_replica_record_file_system(
            entry,
            "filesystem record",
            &file_system.file_system_id,
            expected,
        ),
        DfsMetadataMutation::PutObject {
            object: Some(object),
            ..
        } => require_replica_record_file_system(
            entry,
            "object record",
            &object.file_system_id,
            expected,
        ),
        DfsMetadataMutation::PutVersion {
            version_record: Some(version),
            ..
        } => require_replica_record_file_system(
            entry,
            "version record",
            &version.file_system_id,
            expected,
        ),
        DfsMetadataMutation::PutVersionPin { pin, .. } => require_replica_record_file_system(
            entry,
            "metadata version pin",
            &pin.file_system_id,
            expected,
        ),
        DfsMetadataMutation::ReconcileCrossShardPinTransfer {
            versions, transfer, ..
        } => {
            require_replica_record_file_system(
                entry,
                "cross-shard pin transfer",
                &transfer.file_system_id,
                expected,
            )?;
            for pin in &transfer.source_pins {
                require_replica_record_file_system(
                    entry,
                    "cross-shard copied version pin",
                    &pin.file_system_id,
                    expected,
                )?;
            }
            for version in versions {
                require_replica_record_file_system(
                    entry,
                    "cross-shard pin-transfer version",
                    &version.file_system_id,
                    expected,
                )?;
            }
            Ok(())
        }
        DfsMetadataMutation::FinalizeCrossShardSourceEviction {
            tombstone,
            removed_pins,
            removed_pin_transfers,
            ..
        } => {
            require_replica_record_file_system(
                entry,
                "cross-shard source-eviction tombstone",
                &tombstone.file_system_id,
                expected,
            )?;
            for pin in removed_pins {
                require_replica_record_file_system(
                    entry,
                    "cross-shard source-eviction pin",
                    &pin.file_system_id,
                    expected,
                )?;
            }
            for transfer in removed_pin_transfers {
                require_replica_record_file_system(
                    entry,
                    "cross-shard source-eviction pin transfer",
                    &transfer.file_system_id,
                    expected,
                )?;
            }
            Ok(())
        }
        DfsMetadataMutation::PutReceipt {
            receipt: Some(receipt),
            ..
        } => require_replica_record_file_system(
            entry,
            "receipt record",
            &receipt.receipt.file_system_id,
            expected,
        ),
        DfsMetadataMutation::PutIntent {
            intent: Some(intent),
            ..
        } => {
            require_replica_record_file_system(
                entry,
                "intent record",
                &intent.file_system_id,
                expected,
            )?;
            if let Some(object) = intent.object_metadata.as_ref() {
                require_replica_record_file_system(
                    entry,
                    "intent object record",
                    &object.file_system_id,
                    expected,
                )?;
            }
            Ok(())
        }
        DfsMetadataMutation::PutIntentReceipt { receipt, .. } => {
            require_replica_record_file_system(
                entry,
                "intent receipt",
                &receipt.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object: Some(object),
            visible_version: Some(version),
            ..
        } => {
            require_replica_record_file_system(
                entry,
                "visible-commit object record",
                &object.file_system_id,
                expected,
            )?;
            require_replica_record_file_system(
                entry,
                "visible-commit version record",
                &version.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutConflict {
            conflict: Some(conflict),
            ..
        } => require_replica_record_file_system(
            entry,
            "conflict record",
            &conflict.file_system_id,
            expected,
        ),
        DfsMetadataMutation::PutAdvisoryLockState {
            state: Some(state), ..
        } => require_replica_record_file_system(
            entry,
            "advisory-lock state",
            &state.file_system_id,
            expected,
        ),
        DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: Some(transaction),
            ..
        } => require_replica_namespace_transaction_file_system(
            entry,
            "namespace transaction",
            transaction,
            expected,
        ),
        DfsMetadataMutation::PutNamespaceTransactionReceipt { receipt, .. } => {
            require_replica_record_file_system(
                entry,
                "namespace transaction receipt",
                &receipt.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutNamespacePrepare { .. } if namespace_prepare.is_some() => {
            let prepare = namespace_prepare
                .as_ref()
                .expect("namespace prepare presence checked");
            prepare.validate_file_system_scope(expected).map_err(|source| {
                MetadataBackendError::Serde(format!(
                    "metadata replica journal entry {}:{} has invalid namespace prepare filesystem scope: {source}",
                    entry.shard_id.0, entry.sequence
                ))
            })
        }
        DfsMetadataMutation::PutNamespacePrepareReceipt { receipt, .. } => {
            require_replica_record_file_system(
                entry,
                "namespace prepare receipt",
                &receipt.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutNamespaceJob { job: Some(job), .. } => {
            require_replica_record_file_system(
                entry,
                "namespace job",
                &job.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutNamespaceJobReceipt { receipt, .. } => {
            require_replica_record_file_system(
                entry,
                "namespace job receipt",
                &receipt.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutNamespaceRequest { request, .. } => {
            require_replica_record_file_system(
                entry,
                "namespace request",
                &request.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::PutNamespaceRequestReceipt { receipt, .. } => {
            require_replica_record_file_system(
                entry,
                "namespace request receipt",
                &receipt.file_system_id,
                expected,
            )
        }
        DfsMetadataMutation::RemoveVersion { .. }
        | DfsMetadataMutation::RemoveVersionPin { .. }
        | DfsMetadataMutation::RemoveCrossShardPinTransfer { .. }
        | DfsMetadataMutation::RemoveReceipt { .. }
        | DfsMetadataMutation::RemoveIntentReceipt { .. }
        | DfsMetadataMutation::MergeIntentReceiptRetention { .. }
        | DfsMetadataMutation::RemoveVisibleCommit { .. }
        | DfsMetadataMutation::RemoveNamespaceJobReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { .. }
        | DfsMetadataMutation::RemoveNamespacePrepareReceipt { .. }
        | DfsMetadataMutation::RemoveNamespaceRequestReceipt { .. }
        | DfsMetadataMutation::CommitNamespacePrepare { .. }
        | DfsMetadataMutation::PutConflict { conflict: None, .. } => Ok(()),
        // Payload completeness was checked above. These arms only keep this
        // match exhaustive without re-unwrapping every option.
        DfsMetadataMutation::PutFileSystem {
            file_system: None, ..
        }
        | DfsMetadataMutation::PutObject { object: None, .. }
        | DfsMetadataMutation::PutVersion {
            version_record: None,
            ..
        }
        | DfsMetadataMutation::PutReceipt { receipt: None, .. }
        | DfsMetadataMutation::PutIntent { intent: None, .. }
        | DfsMetadataMutation::CommitVisibleVersion { .. }
        | DfsMetadataMutation::PutAdvisoryLockState { state: None, .. }
        | DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: None, ..
        }
        | DfsMetadataMutation::PutNamespacePrepare { .. }
        | DfsMetadataMutation::PutNamespaceJob { job: None, .. } => Ok(()),
    }?;
    Ok(namespace_prepare)
}

pub(crate) fn hydrate_journal_entry_payload<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    tables: &DfsMetadataTables,
    mut entry: DfsMetadataJournalEntry,
    receipt_page_size: usize,
) -> Result<DfsMetadataJournalEntry> {
    match &mut entry.mutation {
        DfsMetadataMutation::PutFileSystem {
            file_system_id,
            file_system,
        } if file_system.is_none() => {
            *file_system = backend.get_file_system(file_system_id)?;
        }
        DfsMetadataMutation::PutObject {
            object_id, object, ..
        } if object.is_none() => {
            *object = backend.get_object(object_id)?;
        }
        DfsMetadataMutation::PutVersion {
            object_id,
            version,
            version_record,
            ..
        } if version_record.is_none() => {
            *version_record = backend.get_version(object_id, version)?;
        }
        DfsMetadataMutation::PutReceipt {
            object_id,
            version,
            replica_node_id,
            receipt,
            ..
        } if receipt.is_none() => {
            *receipt = receipt_for_replica_from_backend(
                backend,
                object_id,
                version,
                replica_node_id,
                receipt_page_size,
            )?;
        }
        DfsMetadataMutation::PutIntent {
            object_id,
            intent_id,
            intent,
            ..
        } if intent.is_none() => {
            *intent = backend.get_intent(object_id, intent_id)?;
        }
        DfsMetadataMutation::PutConflict {
            object_id,
            conflict_id,
            conflict,
            ..
        } if conflict.is_none() => {
            *conflict = backend.get_conflict(object_id, conflict_id)?;
        }
        DfsMetadataMutation::PutAdvisoryLockState {
            file_system_id,
            object_id,
            state,
        } if state.is_none() => {
            *state = backend.get_advisory_lock_state(file_system_id, object_id)?;
        }
        DfsMetadataMutation::CommitVisibleVersion {
            object_id,
            committed_version,
            committed_object,
            visible_version,
            ..
        } if committed_object.is_none() || visible_version.is_none() => {
            let commit_key = (object_id.clone(), committed_version.clone());
            if let Some(commit) = tables.visible_commits.get(&commit_key) {
                if committed_object.is_none() {
                    *committed_object = Some(commit.committed_object.clone());
                }
                if visible_version.is_none() {
                    *visible_version = Some(commit.visible_version.clone());
                }
            } else {
                if committed_object.is_none()
                    && let Some(object) = backend.get_object(object_id)?
                    && object.version == *committed_version
                {
                    *committed_object = Some(object);
                }
                if visible_version.is_none() {
                    *visible_version = backend.get_version(object_id, committed_version)?;
                }
            }
        }
        DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction_id,
            transaction,
            ..
        } if transaction.is_none() => {
            *transaction = backend.get_namespace_transaction(transaction_id)?;
        }
        DfsMetadataMutation::PutNamespacePrepare {
            operation_id,
            prepare,
            packed_prepare_v1,
            packed_prepare_v2,
            ..
        } if prepare.is_none() && packed_prepare_v1.is_none() && packed_prepare_v2.is_none() => {
            *prepare = backend.get_namespace_prepare(operation_id)?.map(Box::new);
        }
        DfsMetadataMutation::PutNamespaceJob { job_id, job, .. } if job.is_none() => {
            *job = backend.get_namespace_job(job_id)?;
        }
        _ => {}
    }
    Ok(entry)
}

pub(crate) fn hydrate_journal_entry_payloads<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    tables: &DfsMetadataTables,
    entries: Vec<DfsMetadataJournalEntry>,
    receipt_page_size: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    entries
        .into_iter()
        .map(|entry| hydrate_journal_entry_payload(backend, tables, entry, receipt_page_size))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsFileSystemId, DfsMetadataShardId, DfsNamespacePrepareRecord, DfsNamespacePrepareStatus,
        DfsNamespaceTransaction, DfsObjectId, DfsVersion,
    };
    use serde::Deserialize;
    use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
    use time::OffsetDateTime;

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_replay").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_replay").expect("valid object id")
    }

    fn version() -> DfsVersion {
        DfsVersion::from_legacy_named("version_replay_1").expect("valid version")
    }

    fn namespace_prepare() -> DfsNamespacePrepareRecord {
        DfsNamespacePrepareRecord {
            operation_id: "operation-replay-packed".to_string(),
            file_system_id: file_system_id(),
            shard_id: DfsMetadataShardId::new("shard-replay"),
            owner_epoch: 7,
            operation_digest: format!("blake3:{}", "a".repeat(64)),
            expected_object_versions: Vec::new(),
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: "transaction-replay-packed".to_string(),
                file_system_id: file_system_id(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        }
    }

    fn entry(mutation: DfsMetadataMutation) -> DfsMetadataJournalEntry {
        DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-replay"),
            sequence: 42,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation,
        }
    }

    #[test]
    fn replay_payload_check_requires_records_for_put_mutations() {
        let entry = entry(DfsMetadataMutation::PutObject {
            file_system_id: file_system_id(),
            object_id: object_id(),
            path: "/docs/replay.md".to_string(),
            object: None,
        });

        assert!(!journal_entry_has_replay_payloads(&entry));
        let message = required_replay_payload::<()>(&entry, "put-object", None)
            .expect_err("missing payload")
            .to_string();
        assert!(message.contains("shard-replay:42"));
        assert!(message.contains("put-object"));
    }

    #[test]
    fn replay_payload_check_allows_remove_receipts_without_record_payload() {
        let entry = entry(DfsMetadataMutation::RemoveReceipt {
            file_system_id: file_system_id(),
            object_id: object_id(),
            version: version(),
            replica_node_id: sinomo_core::node::NodeId::from_legacy_named("node_replica")
                .expect("valid node"),
        });

        assert!(journal_entry_has_replay_payloads(&entry));
    }

    #[test]
    fn replay_payload_check_accepts_packed_namespace_prepare() {
        let prepare = namespace_prepare();
        let entry = entry(
            DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
                .expect("pack namespace prepare"),
        );

        assert!(matches!(
            borrowed_namespace_prepare_payload(&entry).expect("validate borrowed payload"),
            Some(DfsNamespacePreparePayloadRef::PackedV1 { .. })
        ));
        assert!(journal_entry_has_replay_payloads(&entry));
        require_journal_entry_replay_payloads(&entry).expect("packed payload is replay complete");
        assert_eq!(
            decoded_namespace_prepare_payload(&entry)
                .expect("decode packed payload")
                .as_deref(),
            Some(&prepare)
        );
    }

    #[test]
    fn replay_payload_check_accepts_indexed_packed_v2_namespace_prepare() {
        let prepare = namespace_prepare();
        let entry = entry(
            DfsMetadataMutation::put_namespace_prepare_packed_v2(&prepare)
                .expect("pack v2 namespace prepare"),
        );

        assert!(matches!(
            borrowed_namespace_prepare_payload(&entry).expect("validate borrowed v2 payload"),
            Some(DfsNamespacePreparePayloadRef::PackedV2 { .. })
        ));
        assert!(journal_entry_has_replay_payloads(&entry));
        require_journal_entry_replay_payloads(&entry)
            .expect("packed v2 payload is replay complete");
        assert_eq!(
            decoded_normalized_namespace_prepare_payload(&entry)
                .expect("decode packed v2 payload")
                .as_deref(),
            Some(&prepare)
        );
    }

    #[test]
    fn replay_payload_check_rejects_ambiguous_namespace_prepare() {
        let prepare = namespace_prepare();
        let mut mutation = DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
            .expect("pack namespace prepare");
        let DfsMetadataMutation::PutNamespacePrepare {
            prepare: canonical, ..
        } = &mut mutation
        else {
            unreachable!("packed constructor returns put-namespace-prepare");
        };
        *canonical = Some(Box::new(prepare));
        let entry = entry(mutation);

        assert!(!journal_entry_has_replay_payloads(&entry));
        let error = require_journal_entry_replay_payloads(&entry)
            .expect_err("dual payloads must fail replay")
            .to_string();
        assert!(
            error.contains("both canonical and packed payloads"),
            "{error}"
        );
    }

    #[test]
    fn replay_payload_check_rejects_unsupported_packed_namespace_prepare() {
        let prepare = namespace_prepare();
        let mutation = DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
            .expect("pack namespace prepare");
        let mut encoded = serde_json::to_value(mutation).expect("serialize packed mutation");
        *encoded
            .get_mut("packed_prepare_v1")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|packed| packed.get_mut("f"))
            .expect("packed format version") = serde_json::json!(2);
        let entry = entry(
            serde_json::from_value(encoded).expect("deserialize unsupported packed mutation"),
        );

        assert!(!journal_entry_has_replay_payloads(&entry));
        let error = require_journal_entry_replay_payloads(&entry)
            .expect_err("unsupported packed format must fail replay")
            .to_string();
        assert!(error.contains("unsupported packed namespace prepare version 2"));
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "kebab-case")]
    enum LegacyNamespacePrepareMutation {
        PutNamespacePrepare {
            file_system_id: DfsFileSystemId,
            operation_id: String,
            status: DfsNamespacePrepareStatus,
            #[serde(default)]
            prepare: Option<DfsNamespacePrepareRecord>,
        },
    }

    #[test]
    fn legacy_reader_ignores_packed_field_but_observes_missing_canonical_payload() {
        let prepare = namespace_prepare();
        let mutation = DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
            .expect("pack namespace prepare");
        let bytes = sinomo_object_serde::to_bytes(&mutation, 1, LengthMode::U32)
            .expect("encode packed mutation");
        let legacy: LegacyNamespacePrepareMutation =
            sinomo_object_serde::from_bytes(&bytes).expect("legacy reader decodes additive field");

        let LegacyNamespacePrepareMutation::PutNamespacePrepare {
            file_system_id,
            operation_id,
            status,
            prepare: legacy_prepare,
        } = legacy;
        assert_eq!(file_system_id, prepare.file_system_id);
        assert_eq!(operation_id, prepare.operation_id);
        assert_eq!(status, prepare.status);
        assert!(
            legacy_prepare.is_none(),
            "an older reader must not mistake a packed body for a canonical payload"
        );
    }
}
