//! Metadata journal replay helpers.
//!
//! Journal entries can carry enough typed payload to replay directly into
//! materialized tables. This module owns the small validation helpers used by
//! replay paths so backend code can focus on applying mutations.

use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::metadata_records::{DfsMetadataJournalEntry, DfsMetadataMutation};
use crate::metadata_tables::DfsMetadataTables;

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
        DfsMetadataMutation::PutReceipt { receipt, .. } => receipt.is_some(),
        DfsMetadataMutation::RemoveReceipt { .. } => true,
        DfsMetadataMutation::PutIntent { intent, .. } => intent.is_some(),
        DfsMetadataMutation::CommitVisibleVersion {
            committed_object,
            visible_version,
            ..
        } => committed_object.is_some() && visible_version.is_some(),
        DfsMetadataMutation::PutConflict { conflict, .. } => conflict.is_some(),
        DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => transaction.is_some(),
    }
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
    use crate::{DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsVersion};
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
            replica_node_id: sinomo_node::NodeId::from_legacy_named("node_replica")
                .expect("valid node"),
        });

        assert!(journal_entry_has_replay_payloads(&entry));
    }
}
