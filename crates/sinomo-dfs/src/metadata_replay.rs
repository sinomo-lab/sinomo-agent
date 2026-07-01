//! Metadata journal replay helpers.
//!
//! Journal entries can carry enough typed payload to replay directly into
//! materialized tables. This module owns the small validation helpers used by
//! replay paths so backend code can focus on applying mutations.

use crate::metadata_backend::{MetadataBackendError, Result};
use crate::metadata_records::{DfsMetadataJournalEntry, DfsMetadataMutation};

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
