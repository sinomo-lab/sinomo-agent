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
