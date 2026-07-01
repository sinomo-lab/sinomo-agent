//! Cursor pagination over materialized metadata tables.
//!
//! Backend implementations own snapshot freshness and sharded routing. This
//! module owns the pure table-range scans used by local metadata reads.

use std::ops::Bound::{Excluded, Included, Unbounded};

use crate::metadata_error::Result;
use crate::metadata_hardlink::hardlink_object_from_path_record;
use crate::metadata_keys::{
    conflict_page_cursor, conflict_scan_cursor, directory_inode_id,
    directory_page_cursor_with_dentry, directory_page_key, id_lower_bound, normalize_path,
    normalize_posix_path, object_page_cursor, object_page_key, object_version_page_cursor,
    object_version_scan_cursor, posix_path_is_at_or_under_prefix, replica_receipt_page_cursor,
    replica_receipt_scan_cursor, write_intent_page_cursor, write_intent_scan_cursor,
    write_intent_scan_cursor_key,
};
use crate::metadata_records::{
    DfsConflictPage, DfsConflictPageCursor, DfsConflictRecord, DfsConflictScanCursor,
    DfsConflictScanPage, DfsDirFragmentId, DfsDirectoryPageCursor, DfsNamespaceJobPage,
    DfsNamespaceJobPageCursor, DfsObjectPage, DfsObjectPageCursor, DfsObjectVersionPage,
    DfsObjectVersionPageCursor, DfsObjectVersionScanCursor, DfsObjectVersionScanPage,
    DfsReplicaReceiptPage, DfsReplicaReceiptPageCursor, DfsReplicaReceiptScanCursor,
    DfsReplicaReceiptScanPage, DfsWriteIntentPage, DfsWriteIntentPageCursor,
    DfsWriteIntentScanCursor, DfsWriteIntentScanPage,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion};

#[derive(Clone, Debug)]
pub(crate) struct DfsDirectoryPageEntries {
    pub(crate) entries: Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
    pub(crate) next_cursor: Option<DfsDirectoryPageCursor>,
}

pub(crate) fn collect_metadata_pages<T, C>(
    mut fetch_page: impl FnMut(Option<&C>) -> Result<(Vec<T>, Option<C>)>,
) -> Result<Vec<T>> {
    let mut records = Vec::new();
    let mut cursor = None;
    loop {
        let (page_records, next_cursor) = fetch_page(cursor.as_ref())?;
        records.extend(page_records);
        let Some(next_cursor) = next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(records)
}

pub(crate) fn objects_for_file_system_page_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    after: Option<&DfsObjectPageCursor>,
    max_entries: usize,
) -> DfsObjectPage {
    if max_entries == 0 {
        return DfsObjectPage {
            objects: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((file_system_id.clone(), after.path.clone())),
        None => Included((file_system_id.clone(), String::new())),
    };
    let mut objects = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_file_system_id, _), path_record) in tables
        .paths
        .range((start, Unbounded))
        .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id)
    {
        if record_file_system_id != file_system_id || path_record.file_system_id != *file_system_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
            continue;
        };
        if object.file_system_id != *file_system_id
            || normalize_path(&object.path) != path_record.path
        {
            continue;
        }
        if let Some(after) = after
            && object_page_key(&object) <= (&after.path, &after.object_id)
        {
            continue;
        }
        if objects.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(object_page_cursor(&object));
        objects.push(object);
    }
    DfsObjectPage {
        objects,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn count_objects_for_file_system_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
) -> usize {
    let mut count = 0usize;
    for ((record_file_system_id, _), path_record) in tables
        .file_system_objects
        .range((
            Included((file_system_id.clone(), id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id)
    {
        if record_file_system_id != file_system_id || path_record.file_system_id != *file_system_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id) else {
            continue;
        };
        if object.file_system_id == *file_system_id
            && normalize_path(&object.path) == path_record.path
        {
            count = count.saturating_add(1);
        }
    }
    count
}

pub(crate) fn child_objects_for_directory_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    directory_path: &str,
) -> Vec<DfsObjectMetadata> {
    let directory_path = normalize_posix_path(directory_path);
    let parent_inode_id = directory_inode_id(file_system_id, &directory_path);
    let mut objects = Vec::new();
    for ((record_parent_inode_id, _, _, _), path_record) in tables
        .dentries
        .range((
            Included((
                parent_inode_id.clone(),
                DfsDirFragmentId::new(""),
                0,
                String::new(),
            )),
            Unbounded,
        ))
        .take_while(|((record_parent_inode_id, _, _, _), _)| {
            record_parent_inode_id == &parent_inode_id
        })
    {
        if path_record.file_system_id != *file_system_id
            || record_parent_inode_id != &parent_inode_id
            || path_record.parent_inode_id != parent_inode_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
            continue;
        };
        let object_path = normalize_path(&object.path);
        let object_posix_path = normalize_posix_path(&object.path);
        if object.kind == DfsObjectKind::Tombstone
            || object.file_system_id != *file_system_id
            || object_posix_path == directory_path
            || object_path != path_record.path
        {
            continue;
        }
        objects.push(object);
    }
    objects.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.object_id.cmp(&right.object_id))
    });
    objects
}

pub(crate) fn child_objects_for_directory_page_entries_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    directory_path: &str,
    after: Option<&DfsDirectoryPageCursor>,
    max_entries: usize,
) -> DfsDirectoryPageEntries {
    if max_entries == 0 {
        return DfsDirectoryPageEntries {
            entries: Vec::new(),
            next_cursor: None,
        };
    }
    let directory_path = normalize_posix_path(directory_path);
    let parent_inode_id = directory_inode_id(file_system_id, &directory_path);
    let start = match after.and_then(|cursor| cursor.dentry.as_ref()) {
        Some(dentry) => Excluded((
            parent_inode_id.clone(),
            dentry.dirfrag_id.clone(),
            dentry.name_hash,
            dentry.name.clone(),
        )),
        None => Included((
            parent_inode_id.clone(),
            DfsDirFragmentId::new(""),
            0,
            String::new(),
        )),
    };
    let mut entries = Vec::<(DfsObjectMetadata, DfsDirectoryPageCursor)>::new();
    let mut next_cursor = None;
    for ((record_parent_inode_id, dirfrag_id, name_hash, name), path_record) in
        tables.dentries.range((start, Unbounded)).take_while(
            |((record_parent_inode_id, _, _, _), _)| record_parent_inode_id == &parent_inode_id,
        )
    {
        if path_record.file_system_id != *file_system_id
            || record_parent_inode_id != &parent_inode_id
            || path_record.parent_inode_id != parent_inode_id
        {
            continue;
        }
        let Some(object) = tables.objects.get(&path_record.object_id).cloned() else {
            continue;
        };
        let object_path = normalize_path(&object.path);
        let object_posix_path = normalize_posix_path(&object.path);
        if object.kind == DfsObjectKind::Tombstone
            || object.file_system_id != *file_system_id
            || object_posix_path == directory_path
            || object_path != path_record.path
        {
            continue;
        }
        if after.and_then(|cursor| cursor.dentry.as_ref()).is_none()
            && let Some(after) = after
            && directory_page_key(&object) <= (&after.path, &after.object_id)
        {
            continue;
        }
        let cursor = directory_page_cursor_with_dentry(
            &object,
            dirfrag_id.clone(),
            *name_hash,
            name.clone(),
        );
        if entries.len() >= max_entries {
            next_cursor = entries.last().map(|(_, cursor)| cursor.clone());
            break;
        }
        entries.push((object, cursor));
    }
    DfsDirectoryPageEntries {
        entries,
        next_cursor,
    }
}

pub(crate) fn hardlinks_to_object_page_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
    after: Option<&DfsObjectPageCursor>,
    max_entries: usize,
) -> DfsObjectPage {
    if max_entries == 0 {
        return DfsObjectPage {
            objects: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            target_object_id.clone(),
            normalize_path(&after.path),
            after.object_id.clone(),
        )),
        None => Included((target_object_id.clone(), String::new(), id_lower_bound())),
    };
    let mut objects = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_target_object_id, _, _), path_record) in
        tables.hardlink_paths.range((start, Unbounded)).take_while(
            |((record_target_object_id, _, _), _)| record_target_object_id == target_object_id,
        )
    {
        if record_target_object_id != target_object_id {
            continue;
        }
        let Some(object) =
            hardlink_object_from_path_record(tables, file_system_id, target_object_id, path_record)
        else {
            continue;
        };
        if objects.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(object_page_cursor(&object));
        objects.push(object);
    }
    DfsObjectPage {
        objects,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn hardlink_count_to_object_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
) -> usize {
    let mut count = 0usize;
    for ((record_target_object_id, _, _), path_record) in tables
        .hardlink_paths
        .range((
            Included((target_object_id.clone(), String::new(), id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_target_object_id, _, _), _)| {
            record_target_object_id == target_object_id
        })
    {
        if record_target_object_id == target_object_id
            && hardlink_object_from_path_record(
                tables,
                file_system_id,
                target_object_id,
                path_record,
            )
            .is_some()
        {
            count = count.saturating_add(1);
        }
    }
    count
}

pub(crate) fn first_hardlink_to_object_outside_prefix_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
    prefix: &str,
) -> Option<DfsObjectMetadata> {
    let prefix = normalize_posix_path(prefix);
    for ((record_target_object_id, _, _), path_record) in tables
        .hardlink_paths
        .range((
            Included((target_object_id.clone(), String::new(), id_lower_bound())),
            Unbounded,
        ))
        .take_while(|((record_target_object_id, _, _), _)| {
            record_target_object_id == target_object_id
        })
    {
        if record_target_object_id != target_object_id
            || posix_path_is_at_or_under_prefix(&path_record.path, &prefix)
        {
            continue;
        }
        if let Some(object) =
            hardlink_object_from_path_record(tables, file_system_id, target_object_id, path_record)
        {
            return Some(object);
        }
    }
    None
}

pub(crate) fn namespace_jobs_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsNamespaceJobPageCursor>,
    max_entries: usize,
) -> DfsNamespaceJobPage {
    if max_entries == 0 {
        return DfsNamespaceJobPage {
            jobs: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded(after.job_id.clone()),
        None => Included(String::new()),
    };
    let mut jobs = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for (job_id, job) in tables.namespace_jobs.range((start, Unbounded)) {
        if jobs.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(DfsNamespaceJobPageCursor {
            job_id: job_id.clone(),
        });
        jobs.push(job.clone());
    }
    DfsNamespaceJobPage {
        jobs,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn conflicts_for_object_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
) -> Vec<DfsConflictRecord> {
    tables
        .conflicts
        .range((Included((object_id.clone(), String::new())), Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        .map(|(_, conflict)| conflict)
        .filter(|conflict| conflict.object_id == *object_id)
        .cloned()
        .collect()
}

pub(crate) fn versions_for_object_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    after: Option<&DfsObjectVersionPageCursor>,
    max_entries: usize,
) -> DfsObjectVersionPage {
    if max_entries == 0 {
        return DfsObjectVersionPage {
            versions: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((object_id.clone(), after.version.clone())),
        None => Included((object_id.clone(), id_lower_bound())),
    };
    let mut versions = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, _), version) in tables
        .versions
        .range((start, Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || version.object_id != *object_id {
            continue;
        }
        if versions.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(object_version_page_cursor(version));
        versions.push(version.clone());
    }
    DfsObjectVersionPage {
        versions,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn version_records_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsObjectVersionScanCursor>,
    max_entries: usize,
) -> DfsObjectVersionScanPage {
    if max_entries == 0 {
        return DfsObjectVersionScanPage {
            versions: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((after.object_id.clone(), after.version.clone())),
        None => Included((id_lower_bound(), id_lower_bound())),
    };
    let mut versions = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, record_version), version) in tables.versions.range((start, Unbounded)) {
        if record_object_id != &version.object_id || record_version != &version.version {
            continue;
        }
        if versions.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(object_version_scan_cursor(version));
        versions.push(version.clone());
    }
    DfsObjectVersionScanPage {
        versions,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn receipts_for_version_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    after: Option<&DfsReplicaReceiptPageCursor>,
    max_entries: usize,
) -> DfsReplicaReceiptPage {
    if max_entries == 0 {
        return DfsReplicaReceiptPage {
            receipts: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            object_id.clone(),
            version.clone(),
            after.replica_node_id.clone(),
        )),
        None => Included((object_id.clone(), version.clone(), id_lower_bound())),
    };
    let mut receipts = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, record_version, _), receipt) in tables
        .receipts
        .range((start, Unbounded))
        .take_while(|((record_object_id, record_version, _), _)| {
            record_object_id == object_id && record_version == version
        })
    {
        if record_object_id != object_id
            || record_version != version
            || receipt.receipt.object_id != *object_id
            || receipt.receipt.committed_version != *version
        {
            continue;
        }
        if receipts.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(replica_receipt_page_cursor(receipt));
        receipts.push(receipt.clone());
    }
    DfsReplicaReceiptPage {
        receipts,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn receipt_records_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsReplicaReceiptScanCursor>,
    max_entries: usize,
) -> DfsReplicaReceiptScanPage {
    if max_entries == 0 {
        return DfsReplicaReceiptScanPage {
            receipts: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            after.object_id.clone(),
            after.version.clone(),
            after.replica_node_id.clone(),
        )),
        None => Included((id_lower_bound(), id_lower_bound(), id_lower_bound())),
    };
    let mut receipts = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, record_version, record_replica_node_id), receipt) in
        tables.receipts.range((start, Unbounded))
    {
        if record_object_id != &receipt.receipt.object_id
            || record_version != &receipt.receipt.committed_version
            || record_replica_node_id != &receipt.replica_node_id
        {
            continue;
        }
        if receipts.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(replica_receipt_scan_cursor(receipt));
        receipts.push(receipt.clone());
    }
    DfsReplicaReceiptScanPage {
        receipts,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn intents_for_object_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    after: Option<&DfsWriteIntentPageCursor>,
    max_entries: usize,
) -> DfsWriteIntentPage {
    if max_entries == 0 {
        return DfsWriteIntentPage {
            intents: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((object_id.clone(), after.intent_id.clone())),
        None => Included((object_id.clone(), String::new())),
    };
    let mut intents = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, _), intent) in tables
        .intents
        .range((start, Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || intent.object_id != *object_id {
            continue;
        }
        if intents.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(write_intent_page_cursor(intent));
        intents.push(intent.clone());
    }
    DfsWriteIntentPage {
        intents,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn intents_for_object_version_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    target_version: &DfsVersion,
    after: Option<&DfsWriteIntentPageCursor>,
    max_entries: usize,
) -> DfsWriteIntentPage {
    if max_entries == 0 {
        return DfsWriteIntentPage {
            intents: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            object_id.clone(),
            target_version.clone(),
            after.intent_id.clone(),
        )),
        None => Included((object_id.clone(), target_version.clone(), String::new())),
    };
    let mut intents = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, record_version, _), intent_key) in tables
        .intent_targets
        .range((start, Unbounded))
        .take_while(|((record_object_id, record_version, _), _)| {
            record_object_id == object_id && record_version == target_version
        })
    {
        if record_object_id != object_id || record_version != target_version {
            continue;
        }
        let Some(intent) = tables.intents.get(intent_key) else {
            continue;
        };
        if intent.object_id != *object_id || intent.target_version != *target_version {
            continue;
        }
        if intents.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(write_intent_page_cursor(intent));
        intents.push(intent.clone());
    }
    DfsWriteIntentPage {
        intents,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn write_intents_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsWriteIntentScanCursor>,
    max_entries: usize,
) -> DfsWriteIntentScanPage {
    if max_entries == 0 {
        return DfsWriteIntentScanPage {
            intents: Vec::new(),
            next_cursor: None,
        };
    }
    let mut intents = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    let mut collect_intent = |intent_key: &(DfsObjectId, String)| -> bool {
        let Some(intent) = tables.intents.get(intent_key) else {
            return false;
        };
        if intents.len() >= max_entries {
            return true;
        }
        last_cursor = Some(write_intent_scan_cursor(intent));
        intents.push(intent.clone());
        false
    };
    match after {
        Some(after) => {
            for (_, intent_key) in tables
                .intent_order
                .range((Excluded(write_intent_scan_cursor_key(after)), Unbounded))
            {
                if collect_intent(intent_key) {
                    has_more = true;
                    break;
                }
            }
        }
        None => {
            for intent_key in tables.intent_order.values() {
                if collect_intent(intent_key) {
                    has_more = true;
                    break;
                }
            }
        }
    }
    DfsWriteIntentScanPage {
        intents,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn conflicts_for_object_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    after: Option<&DfsConflictPageCursor>,
    max_entries: usize,
) -> DfsConflictPage {
    if max_entries == 0 {
        return DfsConflictPage {
            conflicts: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((object_id.clone(), after.conflict_id.clone())),
        None => Included((object_id.clone(), String::new())),
    };
    let mut conflicts = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, _), conflict) in tables
        .conflicts
        .range((start, Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || conflict.object_id != *object_id {
            continue;
        }
        if conflicts.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(conflict_page_cursor(conflict));
        conflicts.push(conflict.clone());
    }
    DfsConflictPage {
        conflicts,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

pub(crate) fn conflict_records_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsConflictScanCursor>,
    max_entries: usize,
) -> DfsConflictScanPage {
    if max_entries == 0 {
        return DfsConflictScanPage {
            conflicts: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((after.object_id.clone(), after.conflict_id.clone())),
        None => Included((id_lower_bound(), String::new())),
    };
    let mut conflicts = Vec::new();
    let mut last_cursor = None;
    let mut has_more = false;
    for ((record_object_id, record_conflict_id), conflict) in
        tables.conflicts.range((start, Unbounded))
    {
        if record_object_id != &conflict.object_id || record_conflict_id != &conflict.conflict_id {
            continue;
        }
        if conflicts.len() >= max_entries {
            has_more = true;
            break;
        }
        last_cursor = Some(conflict_scan_cursor(conflict));
        conflicts.push(conflict.clone());
    }
    DfsConflictScanPage {
        conflicts,
        next_cursor: has_more.then_some(last_cursor).flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_records::{
        DfsMetadataTableOp, DfsObjectVersionRecord, DfsWriteIntentRecord, DfsWriteIntentStatus,
    };
    use crate::{DfsFileSystemId, DfsReplicationPolicy};
    use sinomo_node::NodeId;
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_metadata_pages").expect("valid filesystem id")
    }

    fn oid(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("valid object id")
    }

    fn ver(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn version_record(object_id: DfsObjectId, version: DfsVersion) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            file_system_id: file_system_id(),
            object_id,
            version,
            content_hash: Some("blake3:page".to_string()),
            size_bytes: None,
            locations: vec![node("node_a")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    fn intent(
        object_id: DfsObjectId,
        target_version: DfsVersion,
        intent_id: &str,
        created_at: OffsetDateTime,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            intent_id: intent_id.to_string(),
            file_system_id: file_system_id(),
            object_id,
            base_version: None,
            target_version,
            coordinator_node_id: node("node_writer"),
            owner_epoch: 0,
            desired_replica_node_ids: Vec::new(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: None,
            object_metadata: None,
            created_at,
            updated_at: Some(created_at),
        }
    }

    #[test]
    fn versions_for_object_page_uses_cursor_and_stays_object_scoped() {
        let object_id = oid("obj_page_versions");
        let other_id = oid("obj_page_versions_other");
        let mut tables = DfsMetadataTables::default();
        for version_name in ["version_page_1", "version_page_2", "version_page_3"] {
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: version_record(object_id.clone(), ver(version_name)),
            });
        }
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: version_record(other_id, ver("version_page_1")),
        });

        let first = versions_for_object_page_from_tables(&tables, &object_id, None, 2);
        assert_eq!(first.versions.len(), 2);
        assert!(first.next_cursor.is_some());
        let second = versions_for_object_page_from_tables(
            &tables,
            &object_id,
            first.next_cursor.as_ref(),
            2,
        );
        assert_eq!(second.versions.len(), 1);
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn intents_for_target_version_page_uses_target_index_order() {
        let object_id = oid("obj_page_intents");
        let target_version = ver("version_page_target");
        let mut tables = DfsMetadataTables::default();
        for (index, intent_id) in ["intent_b", "intent_a", "intent_c"].into_iter().enumerate() {
            tables.apply(DfsMetadataTableOp::PutIntent {
                intent: intent(
                    object_id.clone(),
                    target_version.clone(),
                    intent_id,
                    OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp"),
                ),
            });
        }

        let first = intents_for_object_version_page_from_tables(
            &tables,
            &object_id,
            &target_version,
            None,
            2,
        );
        assert_eq!(
            first
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent_a", "intent_b"]
        );
        let second = intents_for_object_version_page_from_tables(
            &tables,
            &object_id,
            &target_version,
            first.next_cursor.as_ref(),
            2,
        );
        assert_eq!(
            second
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent_c"]
        );
    }
}
