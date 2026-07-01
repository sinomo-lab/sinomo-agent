use time::OffsetDateTime;

use crate::{
    DfsConflictReadView, DfsError, DfsFileSystemId, DfsObjectMetadata,
    metadata_backend::{
        DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataMutation, DfsObjectVersionRecord,
    },
    normalize_current_locations,
};

pub(crate) fn metadata_journal_batch_file_system_id(
    batch: &DfsMetadataJournalBatch,
) -> Option<DfsFileSystemId> {
    batch
        .entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. }
            | DfsMetadataMutation::PutObject { file_system_id, .. }
            | DfsMetadataMutation::PutVersion { file_system_id, .. }
            | DfsMetadataMutation::PutReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutIntent { file_system_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { file_system_id, .. }
            | DfsMetadataMutation::PutConflict { file_system_id, .. }
            | DfsMetadataMutation::ApplyNamespaceTransaction { file_system_id, .. } => {
                Some(file_system_id.clone())
            }
        })
}

pub(crate) fn read_conflicts_reason(conflicts: Vec<DfsConflictReadView>) -> String {
    conflicts
        .into_iter()
        .map(|conflict| format!("{}: {}", conflict.conflict_id, conflict.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn dfs_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

pub(crate) fn object_version_record(
    object: &DfsObjectMetadata,
    size_bytes: Option<u64>,
) -> DfsObjectVersionRecord {
    let mut locations = object.current_locations.clone();
    normalize_current_locations(&mut locations);
    DfsObjectVersionRecord {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: size_bytes.or(object.content_size_bytes),
        locations,
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

pub(crate) fn put_visible_version_record_if_changed(
    backend: &mut dyn DfsMetadataBackend,
    mut version_record: DfsObjectVersionRecord,
) -> Result<(), DfsError> {
    if let Some(existing) =
        backend.get_version(&version_record.object_id, &version_record.version)?
    {
        version_record.created_at = existing.created_at;
        if existing == version_record {
            return Ok(());
        }
    }
    backend.put_version(&version_record)?;
    Ok(())
}

pub(crate) fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}
