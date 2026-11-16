//! Current prepared-operation journal admission. No retained format decoder.
use crate::DfsFileSystemId;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::{DfsMetadataJournalEntry, DfsMetadataMutation};
pub(crate) fn require_journal_entry_replay_payloads(entry: &DfsMetadataJournalEntry) -> Result<()> {
    match &entry.mutation {
        DfsMetadataMutation::PrepareOperationV7 {
            file_system_id,
            operation_id,
            record,
        } => {
            record.validate()?;
            if &record.digest.file_system_id != file_system_id
                || &record.digest.operation_id != operation_id
                || record.digest.shard_id != entry.shard_id
                || record.digest.prepare_sequence != entry.sequence
                || record.status != crate::DfsPreparedOperationStatus::Prepared
            {
                return Err(MetadataBackendError::Serde(
                    "current prepare journal envelope/dependency mismatch".into(),
                ));
            }
        }
        DfsMetadataMutation::CommitPreparedOperationV7 {
            file_system_id,
            operation_id,
            digest,
        } => {
            if &digest.file_system_id != file_system_id
                || &digest.operation_id != operation_id
                || digest.shard_id != entry.shard_id
                || digest.prepare_sequence >= entry.sequence
                || digest.protocol_version != crate::DFS_PREPARED_OPERATION_PROTOCOL_VERSION
            {
                return Err(MetadataBackendError::Serde(
                    "current commit journal envelope/dependency mismatch".into(),
                ));
            }
        }
    }
    Ok(())
}

/// The current native journal-to-table projection, shared by ordinary replay
/// and explicit hidden-candidate conversion. It confers no writer admission.
pub(crate) fn table_op_for_native_journal_entry(
    entry: &DfsMetadataJournalEntry,
) -> Result<crate::metadata_records::DfsMetadataTableOp> {
    require_journal_entry_replay_payloads(entry)?;
    Ok(match &entry.mutation {
        DfsMetadataMutation::PrepareOperationV7 { record, .. } =>
            crate::metadata_records::DfsMetadataTableOp::PutPreparedOperationV7 { record: record.clone() },
        DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } =>
            crate::metadata_records::DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: digest.clone(),
                committed_entry: Box::new(crate::DfsPreparedOperationCommitReference::from_journal_entry(entry)?),
            },
    })
}

pub(crate) fn require_replica_journal_entry_file_system(
    entry: &DfsMetadataJournalEntry,
    expected: &DfsFileSystemId,
) -> Result<()> {
    require_journal_entry_replay_payloads(entry)?;
    if entry.mutation.file_system_id() != expected {
        return Err(MetadataBackendError::Serde(
            "current replica entry crosses its authenticated filesystem scope".into(),
        ));
    }
    Ok(())
}
