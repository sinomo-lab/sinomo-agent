//! Cursor pagination helpers and materialized metadata table scans.
//!
//! Backend implementations own snapshot freshness and sharded routing. This
//! module owns the reusable cursor-walk helpers and the pure table-range scans
//! used by local metadata reads.

use std::cmp::Ordering;
use std::ops::Bound::{Excluded, Included, Unbounded};

use crate::metadata_error::Result;
use crate::metadata_hardlink::hardlink_object_from_path_record;
use crate::metadata_keys::{
    directory_inode_id, directory_page_cursor_with_dentry, directory_page_key, id_lower_bound,
    normalize_path, normalize_posix_path, object_page_cursor, object_version_page_cursor,
    object_version_scan_cursor, replica_receipt_page_cursor, replica_receipt_scan_cursor,
    version_pin_page_cursor, write_intent_page_cursor, write_intent_scan_cursor,
    write_intent_scan_cursor_key,
};
use crate::metadata_records::{
    DfsDirFragmentId, DfsDirectoryPageCursor, DfsMetadataVersionPinPage,
    DfsMetadataVersionPinPageCursor, DfsNamespaceJobPage, DfsNamespaceJobPageCursor, DfsObjectPage,
    DfsObjectPageCursor, DfsObjectVersionPage, DfsObjectVersionPageCursor,
    DfsObjectVersionScanCursor, DfsObjectVersionScanPage, DfsPendingNamespaceRequestPage,
    DfsPendingNamespaceRequestPageCursor, DfsReplicaReceiptPage, DfsReplicaReceiptPageCursor,
    DfsReplicaReceiptScanCursor, DfsReplicaReceiptScanPage, DfsWriteIntentPage,
    DfsWriteIntentPageCursor, DfsWriteIntentScanCursor, DfsWriteIntentScanPage,
};
use crate::metadata_tables::DfsMetadataTables;

use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion};

#[cfg(test)]
thread_local! {
    pub(crate) static OBJECT_PAGE_PATH_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static VERSION_RECORD_PAGE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static RETAINED_REVISION_CANDIDATE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Debug)]
pub(crate) struct DfsDirectoryPageEntries {
    pub(crate) entries: Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
    pub(crate) next_cursor: Option<DfsDirectoryPageCursor>,
}

/// Drain a paged metadata API into a materialized vector.
///
/// Prefer bounded page APIs for normal service paths. This helper is for
/// compatibility callers, tests, and backend-internal recovery paths that need
/// a complete record list.
pub(crate) fn collect_metadata_pages<T, C>(
    mut fetch_page: impl FnMut(Option<&C>) -> Result<(Vec<T>, Option<C>)>,
) -> Result<Vec<T>> {
    let mut records = Vec::new();
    walk_metadata_pages(|cursor| {
        let (page_records, next_cursor) = fetch_page(cursor)?;
        records.extend(page_records);
        Ok(next_cursor)
    })?;
    Ok(records)
}

/// Drain a paged metadata API while keeping only mapped records.
///
/// This is for compatibility helpers that still return a materialized vector
/// but need to filter each page while walking it. Prefer direct paged APIs for
/// service paths that can stay bounded end-to-end.
pub(crate) fn collect_filtered_metadata_pages<T, U, C>(
    mut fetch_page: impl FnMut(Option<&C>) -> Result<(Vec<T>, Option<C>)>,
    mut filter_map_record: impl FnMut(T) -> Option<U>,
) -> Result<Vec<U>> {
    let mut records = Vec::new();
    walk_metadata_pages(|cursor| {
        let (page_records, next_cursor) = fetch_page(cursor)?;
        records.extend(page_records.into_iter().filter_map(&mut filter_map_record));
        Ok(next_cursor)
    })?;
    Ok(records)
}

/// Build one bounded cursor page by walking a source page API and accepting
/// only records that map to an output cursor.
///
/// The helper stops after collecting one more accepted record than the caller
/// requested, then delegates to [`finish_cursor_page`] so next-cursor semantics
/// stay consistent across metadata default methods.
pub(crate) fn collect_filtered_cursor_page<T, U, SourceCursor, OutputCursor: Clone>(
    after: Option<SourceCursor>,
    max_entries: usize,
    mut fetch_page: impl FnMut(Option<&SourceCursor>) -> Result<(Vec<T>, Option<SourceCursor>)>,
    mut filter_map_record: impl FnMut(T) -> Result<Option<(U, OutputCursor)>>,
) -> Result<(Vec<U>, Option<OutputCursor>)> {
    if max_entries == 0 {
        return Ok((Vec::new(), None));
    }
    let mut source_cursor = after;
    let mut entries = Vec::new();
    loop {
        let (page_records, next_source_cursor) = fetch_page(source_cursor.as_ref())?;
        for record in page_records {
            let Some(entry) = filter_map_record(record)? else {
                continue;
            };
            entries.push(entry);
            if entries.len() > max_entries {
                return Ok(finish_cursor_page(entries, max_entries));
            }
        }
        let Some(next_source_cursor) = next_source_cursor else {
            break;
        };
        source_cursor = Some(next_source_cursor);
    }
    Ok(finish_cursor_page(entries, max_entries))
}

/// Walk a cursor-paged metadata API until it returns no next cursor.
///
/// The closure receives the current cursor and returns the next cursor after
/// processing that page. This centralizes cursor progression without forcing
/// all callers into the same record container shape.
pub(crate) fn walk_metadata_pages<C>(
    fetch_page: impl FnMut(Option<&C>) -> Result<Option<C>>,
) -> Result<()> {
    try_walk_metadata_pages(fetch_page)
}

/// Walk a cursor-paged metadata API using the caller's error type.
///
/// Use this variant when page processing also mutates service state and can
/// fail with a higher-level service error. Metadata-only readers should keep
/// using [`walk_metadata_pages`] so backend error inference stays narrow.
pub(crate) fn try_walk_metadata_pages<C, E>(
    mut fetch_page: impl FnMut(Option<&C>) -> std::result::Result<Option<C>, E>,
) -> std::result::Result<(), E> {
    let mut cursor = None;
    loop {
        let next_cursor = fetch_page(cursor.as_ref())?;
        let Some(next_cursor) = next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(())
}

/// Finish a cursor page represented as `(record, cursor_after_record)` pairs.
///
/// Callers may collect one extra pair to detect that another page exists. This
/// helper trims the extra record and returns the cursor for the last included
/// record as the page's next cursor.
pub(crate) fn finish_cursor_page<T, C: Clone>(
    mut entries: Vec<(T, C)>,
    max_entries: usize,
) -> (Vec<T>, Option<C>) {
    let has_more = entries.len() > max_entries;
    if has_more {
        entries.truncate(max_entries);
    }
    let next_cursor = has_more
        .then(|| entries.last().map(|(_, cursor)| cursor.clone()))
        .flatten();
    (
        entries.into_iter().map(|(record, _)| record).collect(),
        next_cursor,
    )
}

/// Sort then finish a cursor page represented as `(record, cursor)` pairs.
pub(crate) fn finish_sorted_cursor_page<T, C: Clone>(
    mut entries: Vec<(T, C)>,
    max_entries: usize,
    mut compare_records: impl FnMut(&T, &T) -> Ordering,
) -> (Vec<T>, Option<C>) {
    entries.sort_by(|left, right| compare_records(&left.0, &right.0));
    finish_cursor_page(entries, max_entries)
}

pub(crate) fn objects_for_file_system_page_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    after: Option<&DfsObjectPageCursor>,
    max_entries: usize,
) -> DfsObjectPage {
    objects_for_subtree_page_from_tables(tables, file_system_id, "/", after, max_entries)
}

pub(crate) fn objects_for_subtree_page_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    subtree_path: &str,
    after: Option<&DfsObjectPageCursor>,
    max_entries: usize,
) -> DfsObjectPage {
    let prefix = normalize_path(subtree_path);
    let after_path = after.map(|after| normalize_path(&after.path));
    let upper = (!prefix.is_empty()).then(|| format!("{prefix}0"));
    if max_entries == 0
        || upper
            .as_ref()
            .is_some_and(|upper| after_path.as_ref().is_some_and(|after| after >= upper))
    {
        return DfsObjectPage {
            objects: Vec::new(),
            next_cursor: None,
        };
    }
    // A subtree consists of its exact root and the contiguous "root/" range.
    // Names such as "root-backup" sort between those ranges and are unrelated.
    let root_key = (file_system_id.clone(), prefix.clone());
    let root = after_path
        .as_ref()
        .is_none_or(|after| after < &prefix)
        .then(|| tables.paths.get_key_value(&root_key))
        .flatten();
    let children_prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    let start = match after_path.as_ref() {
        Some(after) if after >= &children_prefix => {
            Excluded((file_system_id.clone(), after.clone()))
        }
        _ if prefix.is_empty() => Excluded((file_system_id.clone(), String::new())),
        _ => Included((file_system_id.clone(), children_prefix)),
    };
    let end = upper
        .map(|upper| Excluded((file_system_id.clone(), upper)))
        .unwrap_or(Unbounded);
    let mut entries = Vec::new();
    for ((record_file_system_id, _), path_record) in root.into_iter().chain(
        tables
            .paths
            .range((start, end))
            .take_while(|((record_file_system_id, _), _)| record_file_system_id == file_system_id),
    ) {
        #[cfg(test)]
        OBJECT_PAGE_PATH_VISITS.with(|count| count.set(count.get() + 1));
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
        // The exclusive index bound already enforces continuation. Comparing
        // external path spellings here would skip rows for normalized cursors.
        let cursor = object_page_cursor(&object);
        entries.push((object, cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsObjectPage {
        objects,
        next_cursor,
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
        if !object_is_visible_at_path_from_tables(tables, file_system_id, &object) {
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
        if !object_is_visible_at_path_from_tables(tables, file_system_id, &object) {
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

fn object_is_visible_at_path_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    object: &DfsObjectMetadata,
) -> bool {
    object_by_path_from_tables(tables, file_system_id, &object.path).is_some_and(|visible| {
        visible.kind != DfsObjectKind::Tombstone && visible.object_id == object.object_id
    })
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
    let mut entries = Vec::new();
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
        let cursor = object_page_cursor(&object);
        entries.push((object, cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsObjectPage {
        objects,
        next_cursor,
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
    let mut entries = Vec::new();
    for (job_id, job) in tables.namespace_jobs.range((start, Unbounded)) {
        let cursor = DfsNamespaceJobPageCursor {
            job_id: job_id.clone(),
        };
        entries.push((job.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (jobs, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsNamespaceJobPage { jobs, next_cursor }
}

pub(crate) fn pending_namespace_requests_page_from_tables(
    tables: &DfsMetadataTables,
    after: Option<&DfsPendingNamespaceRequestPageCursor>,
    max_entries: usize,
) -> DfsPendingNamespaceRequestPage {
    if max_entries == 0 {
        return DfsPendingNamespaceRequestPage {
            requests: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            after.updated_at,
            after.requester_node_id.clone(),
            after.request_id.clone(),
        )),
        None => Unbounded,
    };
    let mut entries = Vec::new();
    for (updated_at, requester_node_id, request_id) in tables
        .pending_namespace_request_order
        .range((start, Unbounded))
    {
        let request_key = (requester_node_id.clone(), request_id.clone());
        let Some(request) = tables.namespace_requests.get(&request_key) else {
            continue;
        };
        let cursor = DfsPendingNamespaceRequestPageCursor {
            updated_at: *updated_at,
            requester_node_id: requester_node_id.clone(),
            request_id: request_id.clone(),
        };
        entries.push((request.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (requests, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsPendingNamespaceRequestPage {
        requests,
        next_cursor,
    }
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
    let mut entries = Vec::new();
    for ((record_object_id, _), version) in tables
        .versions
        .range((start, Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || version.object_id != *object_id {
            continue;
        }
        let cursor = object_version_page_cursor(version);
        entries.push((version.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (versions, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsObjectVersionPage {
        versions,
        next_cursor,
    }
}

pub(crate) fn version_pins_for_object_page_from_tables(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    after: Option<&DfsMetadataVersionPinPageCursor>,
    max_entries: usize,
) -> DfsMetadataVersionPinPage {
    if max_entries == 0 {
        return DfsMetadataVersionPinPage {
            pins: Vec::new(),
            next_cursor: None,
        };
    }
    let start = match after {
        Some(after) => Excluded((
            object_id.clone(),
            after.version.clone(),
            after.pin_id.clone(),
        )),
        None => Included((object_id.clone(), id_lower_bound(), String::new())),
    };
    let mut entries = Vec::new();
    for ((record_object_id, _, _), pin) in tables
        .version_pins
        .range((start, Unbounded))
        .take_while(|((record_object_id, _, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || pin.object_id != *object_id {
            continue;
        }
        let cursor = version_pin_page_cursor(pin);
        entries.push((pin.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (pins, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsMetadataVersionPinPage { pins, next_cursor }
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
    let mut entries = Vec::new();
    for ((record_object_id, record_version), version) in tables.versions.range((start, Unbounded)) {
        #[cfg(test)]
        VERSION_RECORD_PAGE_VISITS.set(VERSION_RECORD_PAGE_VISITS.get().saturating_add(1));
        if record_object_id != &version.object_id || record_version != &version.version {
            continue;
        }
        let cursor = object_version_scan_cursor(version);
        entries.push((version.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (versions, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsObjectVersionScanPage {
        versions,
        next_cursor,
    }
}

/// Read only grant candidates. Each indexed stream contributes at most one
/// page plus lookahead: the first K keys of a union are contained in the first
/// K keys of each sorted input. No path family or raw version scan is drained.
pub(crate) fn retained_revision_records_page_from_tables(
    tables: &DfsMetadataTables,
    namespace: Option<&str>,
    file_system_id: &DfsFileSystemId,
    object_id: Option<&DfsObjectId>,
    path_prefix: Option<&str>,
    after: Option<&DfsObjectVersionScanCursor>,
    max_entries: usize,
) -> DfsObjectVersionScanPage {
    if max_entries == 0 {
        return DfsObjectVersionScanPage {
            versions: Vec::new(),
            next_cursor: None,
        };
    }
    let owner = namespace.map(str::to_owned);
    let index = &tables.retained_revision_index;
    let mut keys = std::collections::BTreeSet::new();
    let start = after.map(|cursor| (cursor.object_id.clone(), cursor.version.clone()));
    if let Some(object_id) = object_id {
        if after.is_none_or(|cursor| &cursor.object_id <= object_id)
            && let Some(versions) = index.object_versions.get(&(
                object_id.clone(),
                file_system_id.clone(),
                owner.clone(),
            ))
        {
            let bound = match after {
                Some(cursor) if &cursor.object_id == object_id => Excluded(cursor.version.clone()),
                _ => Unbounded,
            };
            keys.extend(
                versions
                    .range((bound, Unbounded))
                    .take(max_entries.saturating_add(1))
                    .map(|version| (object_id.clone(), version.clone())),
            );
        }
    } else {
        let prefix = path_prefix
            .map(normalize_posix_path)
            .unwrap_or_else(|| "/".into());
        let path_key = (file_system_id.clone(), owner.clone(), prefix);
        let fallback_key = (file_system_id.clone(), owner.clone());
        for captured in [
            index.captured_paths.get(&path_key),
            index.unindexed_captured_paths.get(&fallback_key),
        ]
        .into_iter()
        .flatten()
        {
            let bound = start.clone().map(Excluded).unwrap_or(Unbounded);
            keys.extend(
                captured
                    .range((bound, Unbounded))
                    .take(max_entries.saturating_add(1))
                    .cloned(),
            );
        }
        for objects in path_prefix.into_iter().flat_map(|_| {
            [
                index.current_paths.get(&path_key),
                index.unindexed_current_paths.get(&fallback_key),
            ]
            .into_iter()
            .flatten()
        }) {
            let bound = after
                .map(|cursor| Included(cursor.object_id.clone()))
                .unwrap_or(Unbounded);
            let mut count = 0;
            for object_id in objects.range((bound, Unbounded)) {
                // Current path aliases follow object identity across a
                // filesystem move. Only the immutable family stream below
                // is scoped to the requested filesystem.
                let Some(versions) = index.object_versions.get(&(
                    object_id.clone(),
                    file_system_id.clone(),
                    owner.clone(),
                )) else {
                    continue;
                };
                let bound = match after {
                    Some(cursor) if &cursor.object_id == object_id => {
                        Excluded(cursor.version.clone())
                    }
                    _ => Unbounded,
                };
                for version in versions.range((bound, Unbounded)) {
                    keys.insert((object_id.clone(), version.clone()));
                    count += 1;
                    if count > max_entries {
                        break;
                    }
                }
                if count > max_entries {
                    break;
                }
            }
        }
    }
    // A filtered candidate still owns a cursor position. Finish the raw
    // bounded key page before flattening unavailable/foreign rows, so a
    // defensive filter cannot hide a later eligible revision.
    let entries = keys
        .into_iter()
        .take(max_entries.saturating_add(1))
        .map(|(object_id, version)| {
            let record = tables
                .versions
                .get(&(object_id.clone(), version.clone()))
                .filter(|record| &record.file_system_id == file_system_id)
                .map(|record| {
                    #[cfg(test)]
                    RETAINED_REVISION_CANDIDATE_VISITS
                        .set(RETAINED_REVISION_CANDIDATE_VISITS.get().saturating_add(1));
                    record.clone()
                });
            (record, DfsObjectVersionScanCursor { object_id, version })
        })
        .collect();
    let (versions, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsObjectVersionScanPage {
        versions: versions.into_iter().flatten().collect(),
        next_cursor,
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
    let mut entries = Vec::new();
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
        let cursor = replica_receipt_page_cursor(receipt);
        entries.push((receipt.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (receipts, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsReplicaReceiptPage {
        receipts,
        next_cursor,
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
    let mut entries = Vec::new();
    for ((record_object_id, record_version, record_replica_node_id), receipt) in
        tables.receipts.range((start, Unbounded))
    {
        if record_object_id != &receipt.receipt.object_id
            || record_version != &receipt.receipt.committed_version
            || record_replica_node_id != &receipt.replica_node_id
        {
            continue;
        }
        let cursor = replica_receipt_scan_cursor(receipt);
        entries.push((receipt.clone(), cursor));
        if entries.len() > max_entries {
            break;
        }
    }
    let (receipts, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsReplicaReceiptScanPage {
        receipts,
        next_cursor,
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
    let mut entries = Vec::new();
    for ((record_object_id, intent_id), intent) in tables
        .intents
        .range((Included((object_id.clone(), String::new())), Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id
            || intent.object_id != *object_id
            || after.is_some_and(|after| intent_id <= &after.intent_id)
        {
            continue;
        }
        let cursor = write_intent_page_cursor(intent);
        entries.push((intent.clone(), cursor));
    }
    for ((record_object_id, intent_id), receipt) in tables
        .intent_receipts
        .range((Included((object_id.clone(), String::new())), Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id || after.is_some_and(|after| intent_id <= &after.intent_id)
        {
            continue;
        }
        let intent = receipt.to_intent_record();
        let cursor = write_intent_page_cursor(&intent);
        entries.push((intent, cursor));
    }
    entries.sort_by(|left, right| left.1.intent_id.cmp(&right.1.intent_id));
    entries.truncate(max_entries.saturating_add(1));
    let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsWriteIntentPage {
        intents,
        next_cursor,
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
    let mut entries = Vec::new();
    for ((record_object_id, record_version, _), intent_key) in tables
        .intent_targets
        .range((
            Included((object_id.clone(), target_version.clone(), String::new())),
            Unbounded,
        ))
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
        if after.is_some_and(|after| intent.intent_id <= after.intent_id) {
            continue;
        }
        let cursor = write_intent_page_cursor(intent);
        entries.push((intent.clone(), cursor));
    }
    for ((record_object_id, intent_id), receipt) in tables
        .intent_receipts
        .range((Included((object_id.clone(), String::new())), Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
    {
        if record_object_id != object_id
            || receipt.target_version != *target_version
            || after.is_some_and(|after| intent_id <= &after.intent_id)
        {
            continue;
        }
        let intent = receipt.to_intent_record();
        let cursor = write_intent_page_cursor(&intent);
        entries.push((intent, cursor));
    }
    entries.sort_by(|left, right| left.1.intent_id.cmp(&right.1.intent_id));
    entries.truncate(max_entries.saturating_add(1));
    let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsWriteIntentPage {
        intents,
        next_cursor,
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
    let mut entries = Vec::new();
    let mut collect_intent = |intent_key: &(DfsObjectId, String)| -> bool {
        let Some(intent) = tables.intents.get(intent_key) else {
            return false;
        };
        let cursor = write_intent_scan_cursor(intent);
        entries.push((intent.clone(), cursor));
        entries.len() > max_entries
    };
    match after {
        Some(after) => {
            for (_, intent_key) in tables
                .intent_order
                .range((Excluded(write_intent_scan_cursor_key(after)), Unbounded))
            {
                if collect_intent(intent_key) {
                    break;
                }
            }
        }
        None => {
            for intent_key in tables.intent_order.values() {
                if collect_intent(intent_key) {
                    break;
                }
            }
        }
    }
    let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
    DfsWriteIntentScanPage {
        intents,
        next_cursor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_records::{
        DfsMetadataTableOp, DfsMetadataVersionPinRecord, DfsObjectVersionRecord,
        DfsWriteIntentRecord, DfsWriteIntentStatus,
    };
    use crate::{DfsFileSystemId, DfsReplicationPolicy};
    use sinomo_core::node::NodeId;
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
            content_durability: None,
            retention: Default::default(),
            module_metadata: Default::default(),
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

    fn retained_object(name: &str, path: &str) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: oid(name),
            file_system_id: file_system_id(),
            path: path.into(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_a"),
            current_locations: vec![node("node_a")],
            content_hash: Some("blake3:page".into()),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: ver(name),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn grant_record(object: &DfsObjectMetadata, namespace: &str) -> DfsObjectVersionRecord {
        let mut record = version_record(object.object_id.clone(), object.version.clone());
        record.size_bytes = object.content_size_bytes;
        record.retention.insert(
            namespace.into(),
            crate::DfsRevisionRetentionState {
                order: 1,
                grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                    object: Box::new(object.clone()),
                    granted_at_unix_nanos: 1,
                    origin: crate::DfsRetentionGrantOrigin::ExplicitOperation,
                    minimum: None,
                })),
            },
        );
        record
    }

    fn retained_path_page(
        tables: &DfsMetadataTables,
        path: &str,
        after: Option<&DfsObjectVersionScanCursor>,
        limit: usize,
    ) -> DfsObjectVersionScanPage {
        retained_revision_records_page_from_tables(
            tables,
            Some("org.example.retained"),
            &file_system_id(),
            None,
            Some(path),
            after,
            limit,
        )
    }

    #[test]
    fn retained_revision_path_index_skips_ordinary_versions_and_merges_exact_cursors() {
        let mut tables = DfsMetadataTables::default();
        for index in 0..4096 {
            let object = retained_object(&format!("ordinary-{index}"), "/target/ordinary");
            tables.apply(DfsMetadataTableOp::PutObject {
                object: object.clone(),
            });
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: version_record(object.object_id, object.version),
            });
        }
        let mut expected = Vec::new();
        for (name, captured_path, current_path) in [
            ("captured-only", "/target/old", "/elsewhere/a"),
            ("current-only", "/elsewhere/b", "/target/current"),
            ("both-streams", "/target/both", "/target/both"),
        ] {
            let captured = retained_object(name, captured_path);
            let record = grant_record(&captured, "org.example.retained");
            let mut current = captured;
            current.path = current_path.into();
            tables.apply(DfsMetadataTableOp::PutObject { object: current });
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: record.clone(),
            });
            expected.push(record);
        }
        for (name, path, namespace) in [
            ("prefix-boundary", "/targetish/file", "org.example.retained"),
            ("different-owner", "/target/file", "org.example.other"),
        ] {
            let object = retained_object(name, path);
            let record = grant_record(&object, namespace);
            tables.apply(DfsMetadataTableOp::PutObject { object });
            tables.apply(DfsMetadataTableOp::PutVersion { version: record });
        }
        expected.sort_by_key(|record| (record.object_id.clone(), record.version.clone()));
        VERSION_RECORD_PAGE_VISITS.set(0);
        RETAINED_REVISION_CANDIDATE_VISITS.set(0);
        let empty = retained_path_page(&tables, "/absent", None, 1);
        assert!(empty.versions.is_empty());
        assert!(
            empty.next_cursor.is_none(),
            "unrelated rows must not mint empty raw-scan pages"
        );
        let mut after = None;
        for (index, record) in expected.iter().enumerate() {
            let page = retained_path_page(&tables, "/target", after.as_ref(), 1);
            assert_eq!(page.versions, vec![record.clone()]);
            let expected_cursor =
                (index + 1 < expected.len()).then(|| object_version_scan_cursor(record));
            assert_eq!(
                page.next_cursor, expected_cursor,
                "cursor must name the last returned candidate"
            );
            after = page.next_cursor;
        }
        assert_eq!(
            VERSION_RECORD_PAGE_VISITS.get(),
            0,
            "no ordinary version scan is allowed"
        );
        assert_eq!(
            RETAINED_REVISION_CANDIDATE_VISITS.get(),
            5,
            "one page plus lookahead, with deduplication"
        );
        let object = &expected[1];
        let page = retained_revision_records_page_from_tables(
            &tables,
            Some("org.example.retained"),
            &file_system_id(),
            Some(&object.object_id),
            None,
            Some(&object_version_scan_cursor(&expected[0])),
            1,
        );
        assert_eq!(
            page.versions,
            vec![object.clone()],
            "an earlier object's cursor restarts the selected family"
        );
        let page = retained_revision_records_page_from_tables(
            &tables,
            Some("org.example.retained"),
            &file_system_id(),
            Some(&object.object_id),
            None,
            Some(&object_version_scan_cursor(&expected[2])),
            1,
        );
        assert!(
            page.versions.is_empty(),
            "a later object's cursor ends the selected family"
        );
    }

    #[test]
    fn retained_revision_path_index_tracks_rename_unlink_and_incremental_rollback() {
        let mut tables = DfsMetadataTables::default();
        let original = retained_object("moving-family", "/before/file");
        let first = grant_record(&original, "org.example.retained");
        let mut second_capture = original.clone();
        second_capture.path = "/other-capture/file".into();
        second_capture.version = ver("moving-family-second");
        let second = grant_record(&second_capture, "org.example.retained");
        tables.apply(DfsMetadataTableOp::PutObject {
            object: original.clone(),
        });
        for version in [first.clone(), second.clone()] {
            tables.apply(DfsMetadataTableOp::PutVersion { version });
        }
        assert_eq!(
            retained_path_page(&tables, "/before", None, 128)
                .versions
                .len(),
            2
        );
        let before_index = tables.retained_revision_index.clone();
        let mut moved = original.clone();
        moved.path = "/after/file".into();
        let mut rollback = crate::metadata_tables::DfsMetadataTableRollback::default();
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutObject {
                object: moved.clone(),
            },
            &mut rollback,
        );
        tables.apply_with_rollback(
            DfsMetadataTableOp::RemoveVersion {
                object_id: second.object_id.clone(),
                version: second.version.clone(),
            },
            &mut rollback,
        );
        assert_eq!(
            retained_path_page(&tables, "/before", None, 128).versions,
            vec![first.clone()]
        );
        assert_eq!(
            retained_path_page(&tables, "/after", None, 128).versions,
            vec![first.clone()]
        );
        rollback.rollback(&mut tables);
        assert_eq!(tables.retained_revision_index, before_index);
        tables.apply(DfsMetadataTableOp::PutObject {
            object: moved.clone(),
        });
        assert_eq!(
            retained_path_page(&tables, "/after", None, 128)
                .versions
                .len(),
            2
        );
        moved.kind = DfsObjectKind::Tombstone;
        moved.content_hash = None;
        tables.apply(DfsMetadataTableOp::PutObject { object: moved });
        assert_eq!(
            retained_path_page(&tables, "/after", None, 128)
                .versions
                .len(),
            2,
            "authoritative unlink preserves the family"
        );
        tables.apply(DfsMetadataTableOp::RemoveObject {
            object_id: original.object_id,
        });
        assert!(
            retained_path_page(&tables, "/after", None, 128)
                .versions
                .is_empty()
        );
        assert_eq!(
            retained_path_page(&tables, "/before", None, 128).versions,
            vec![first]
        );
        assert_eq!(
            retained_path_page(&tables, "/other-capture", None, 128).versions,
            vec![second]
        );
    }

    #[test]
    fn retained_revision_path_index_rebuilds_from_current_snapshot_and_reconcile() {
        use crate::metadata_records::DfsCrossShardPinTransferRecord;
        use crate::metadata_table_store::{
            metadata_tables_from_snapshot, metadata_tables_snapshot,
        };
        let mut tables = DfsMetadataTables::default();
        let object = retained_object("snapshot-family", "/current/file");
        let mut captured = object.clone();
        captured.path = "/captured/file".into();
        let record = grant_record(&captured, "org.example.retained");
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        let transfer = DfsCrossShardPinTransferRecord {
            transfer_id: "transfer-index".into(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            source_path: captured.path.clone(),
            target_path: object.path.clone(),
            target_version: object.version.clone(),
            source_pins: Vec::new(),
            copied_pins: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut rollback = crate::metadata_tables::DfsMetadataTableRollback::default();
        tables.apply_with_rollback(
            DfsMetadataTableOp::ReconcileCrossShardPinTransfer {
                versions: vec![record.clone()],
                transfer: transfer.clone(),
            },
            &mut rollback,
        );
        assert_eq!(
            retained_path_page(&tables, "/captured", None, 1).versions,
            vec![record.clone()]
        );
        rollback.rollback(&mut tables);
        assert!(
            retained_path_page(&tables, "/captured", None, 1)
                .versions
                .is_empty()
        );
        tables.apply(DfsMetadataTableOp::ReconcileCrossShardPinTransfer {
            versions: vec![record.clone()],
            transfer,
        });
        {
            let snapshot = metadata_tables_snapshot(&tables);
            let encoded = serde_json::to_value(&snapshot).unwrap();
            assert!(!encoded.to_string().contains("retained_revision_index"));
            let rebuilt = metadata_tables_from_snapshot(snapshot).unwrap();
            for path in ["/captured", "/current"] {
                assert_eq!(
                    retained_path_page(&rebuilt, path, None, 1).versions,
                    vec![record.clone()]
                );
            }
            assert_eq!(
                rebuilt.retained_revision_index,
                tables.retained_revision_index
            );
        }
    }

    #[test]
    fn retained_revision_path_index_bounds_oversized_current_paths_without_losing_families() {
        let mut tables = DfsMetadataTables::default();
        let captured = retained_object("long-current-path", "/captured/file");
        let record = grant_record(&captured, "org.example.retained");
        let mut current = captured;
        current.path = format!("/long/{}", "segment/".repeat(1024));
        tables.apply(DfsMetadataTableOp::PutObject { object: current });
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: record.clone(),
        });
        assert!(tables.retained_revision_index.current_paths.is_empty());
        assert_eq!(
            retained_path_page(&tables, "/long", None, 1).versions,
            vec![record]
        );
    }

    #[test]
    fn retained_revision_path_index_bounds_deep_and_large_prefix_keys_with_compatible_fallback() {
        for (name, path, prefix) in [
            (
                "deep-legal-path",
                format!("/deep/{}file", "segment/".repeat(70)),
                "/deep",
            ),
            (
                "large-prefix-keys",
                format!("/wide/{}/a/a/a/a/a/a/file", "x".repeat(3500)),
                "/wide",
            ),
        ] {
            assert!(path.len() <= 4096);
            let mut tables = DfsMetadataTables::default();
            let object = retained_object(name, &path);
            let record = grant_record(&object, "org.example.retained");
            record.validate_extension_metadata().unwrap();
            tables.apply(DfsMetadataTableOp::PutObject { object });
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: record.clone(),
            });
            assert!(tables.retained_revision_index.captured_paths.is_empty());
            assert!(tables.retained_revision_index.current_paths.is_empty());
            assert_eq!(
                retained_path_page(&tables, prefix, None, 1).versions,
                vec![record.clone()]
            );
            assert_eq!(
                retained_path_page(&tables, "/", None, 1).versions,
                vec![record]
            );
        }
    }

    #[test]
    fn retained_revision_path_index_receipt_and_metadata_updates_do_no_path_index_work() {
        let mut tables = DfsMetadataTables::default();
        let mut current = retained_object("cost-family", "/captured/file");
        current.version = ver("cost-version-0");
        tables.apply(DfsMetadataTableOp::PutObject {
            object: current.clone(),
        });
        let mut records = Vec::new();
        for index in 0..512 {
            let mut captured = current.clone();
            captured.version = ver(&format!("cost-version-{index}"));
            let record = grant_record(&captured, "org.example.retained");
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: record.clone(),
            });
            records.push(record);
        }
        let before = tables.retained_revision_index.clone();
        crate::metadata_tables::RETAINED_PATH_INDEX_UPDATES.set(0);
        for record in &mut records {
            record.locations.push(node("receipt-node"));
            tables.apply(DfsMetadataTableOp::PutVersion {
                version: record.clone(),
            });
        }
        current.mode = 0o600;
        current.modified_at_unix_nanos = 42;
        tables.apply(DfsMetadataTableOp::PutObject {
            object: current.clone(),
        });
        current.kind = DfsObjectKind::Tombstone;
        current.content_hash = None;
        tables.apply(DfsMetadataTableOp::PutObject {
            object: current.clone(),
        });
        assert_eq!(
            crate::metadata_tables::RETAINED_PATH_INDEX_UPDATES.get(),
            0,
            "receipt, permissions, mtime and authoritative unlink must not reindex retained paths"
        );
        assert_eq!(tables.retained_revision_index, before);
        let page = retained_path_page(&tables, "/captured", None, 128);
        assert_eq!(page.versions.len(), 128);
        assert!(
            page.versions
                .iter()
                .all(|record| record.locations.contains(&node("receipt-node"))),
            "index lookup must still project freshly updated durable rows"
        );
        current.path = "/moved/file".into();
        tables.apply(DfsMetadataTableOp::PutObject { object: current });
        assert_eq!(
            crate::metadata_tables::RETAINED_PATH_INDEX_UPDATES.get(),
            2,
            "a changed current locator must update both family projections"
        );
        assert_eq!(
            retained_path_page(&tables, "/moved", None, 128)
                .versions
                .len(),
            128
        );
    }

    fn intent(
        object_id: DfsObjectId,
        target_version: DfsVersion,
        intent_id: &str,
        created_at: OffsetDateTime,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: intent_id.to_string(),
            file_system_id: file_system_id(),
            object_id,
            base_version: None,
            target_version,
            coordinator_node_id: node("node_writer"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: Vec::new(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: None,
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at,
            updated_at: Some(created_at),
        }
    }

    #[test]
    fn collect_metadata_pages_drains_until_cursor_exhausted() {
        let records = collect_metadata_pages(|cursor: Option<&usize>| {
            Ok(match cursor.copied().unwrap_or(0) {
                0 => (vec![1, 2], Some(2)),
                2 => (vec![3, 4], Some(4)),
                4 => (vec![5], None),
                other => panic!("unexpected cursor {other}"),
            })
        })
        .expect("collect pages");

        assert_eq!(records, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn collect_filtered_metadata_pages_filters_each_page() {
        let records = collect_filtered_metadata_pages(
            |cursor: Option<&usize>| {
                Ok(match cursor.copied().unwrap_or(0) {
                    0 => (vec![1, 2, 3], Some(3)),
                    3 => (vec![4, 5], None),
                    other => panic!("unexpected cursor {other}"),
                })
            },
            |record| (record % 2 == 1).then_some(record * 10),
        )
        .expect("collect filtered pages");

        assert_eq!(records, vec![10, 30, 50]);
    }

    #[test]
    fn collect_filtered_cursor_page_uses_last_included_cursor_when_more_exists() {
        let (records, next_cursor) = collect_filtered_cursor_page(
            None,
            2,
            |cursor: Option<&usize>| {
                Ok(match cursor.copied().unwrap_or(0) {
                    0 => (vec![1, 2, 3], Some(3)),
                    3 => (vec![4, 5], None),
                    other => panic!("unexpected cursor {other}"),
                })
            },
            |record| Ok((record % 2 == 1).then_some((record * 10, record))),
        )
        .expect("collect filtered cursor page");

        assert_eq!(records, vec![10, 30]);
        assert_eq!(next_cursor, Some(3));
    }

    #[test]
    fn walk_metadata_pages_allows_early_stop() {
        let mut seen = Vec::new();
        walk_metadata_pages(|cursor: Option<&usize>| {
            let value = cursor.copied().unwrap_or(0);
            seen.push(value);
            Ok((value < 2).then_some(value + 1))
        })
        .expect("walk pages");

        assert_eq!(seen, vec![0, 1, 2]);
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
    fn version_pins_for_object_page_is_bounded_and_uses_composite_cursor() {
        let object_id = oid("obj_page_version_pins");
        let other_id = oid("obj_page_version_pins_other");
        let mut tables = DfsMetadataTables::default();
        for (version_name, pin_id) in [
            ("version_pin_page_1", "consumer-a"),
            ("version_pin_page_1", "consumer-b"),
            ("version_pin_page_2", "consumer-a"),
        ] {
            tables.apply(DfsMetadataTableOp::PutVersionPin {
                pin: DfsMetadataVersionPinRecord {
                    namespace: "module.dfs.history".into(),
                    file_system_id: file_system_id(),
                    object_id: object_id.clone(),
                    version: ver(version_name),
                    pin_id: crate::content_reference_storage_id(
                        "module.dfs.history",
                        &(pin_id.to_string()),
                    ),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
            });
        }
        tables.apply(DfsMetadataTableOp::PutVersionPin {
            pin: DfsMetadataVersionPinRecord {
                namespace: "module.dfs.history".into(),
                file_system_id: file_system_id(),
                object_id: other_id,
                version: ver("version_pin_page_1"),
                pin_id: crate::content_reference_storage_id(
                    "module.dfs.history",
                    &("consumer-a".to_string()),
                ),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
        });

        let first = version_pins_for_object_page_from_tables(&tables, &object_id, None, 2);
        assert_eq!(first.pins.len(), 2);
        assert!(first.next_cursor.is_some());
        let second = version_pins_for_object_page_from_tables(
            &tables,
            &object_id,
            first.next_cursor.as_ref(),
            2,
        );
        assert_eq!(second.pins.len(), 1);
        assert!(second.next_cursor.is_none());
        let observed = first
            .pins
            .into_iter()
            .chain(second.pins)
            .map(|pin| (pin.version, pin.pin_id))
            .collect::<Vec<_>>();
        let mut expected = vec![
            (
                ver("version_pin_page_1"),
                "ref/module.dfs.history/consumer-a".to_string(),
            ),
            (
                ver("version_pin_page_1"),
                "ref/module.dfs.history/consumer-b".to_string(),
            ),
            (
                ver("version_pin_page_2"),
                "ref/module.dfs.history/consumer-a".to_string(),
            ),
        ];
        expected.sort();
        assert_eq!(observed, expected);
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

pub(crate) fn object_by_path_from_tables(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Option<DfsObjectMetadata> {
    let normalized_path = normalize_path(path);
    let Some(path_record) = tables
        .paths
        .get(&(file_system_id.clone(), normalized_path.clone()))
    else {
        return tables.compacted_tombstone_by_path(file_system_id, &normalized_path);
    };
    if path_record.file_system_id != *file_system_id
        || normalize_path(&path_record.path) != normalized_path
    {
        return None;
    }
    if let Some(object) = tables.objects.get(&path_record.object_id).cloned()
        && object.file_system_id == *file_system_id
        && normalize_path(&object.path) == normalized_path
    {
        return Some(object);
    }
    tables.compacted_tombstone_by_path(file_system_id, &normalized_path)
}
