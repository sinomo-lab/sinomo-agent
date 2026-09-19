//! Object metadata index planning.
//!
//! Local metadata backends append durable table batches. This module computes
//! the object-related table operations for path, inode, dentry, hardlink, and
//! directory-fragment indexes from an in-memory table snapshot.

use crate::metadata_keys::{
    bucket_dirfrag_id, dentry_key, dirfrag_hash_range, normalize_path, object_inode_id,
    path_record_for_object,
};
use crate::metadata_records::{
    DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeRecord, DfsMetadataTableOp, DfsPathRecord,
    normalized_object_metadata,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectKind, DfsObjectMetadata};

const DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT: u64 = 4096;

pub(crate) fn object_index_table_ops_at(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
    previous: Option<&DfsObjectMetadata>,
    committed_at: time::OffsetDateTime,
) -> Vec<DfsMetadataTableOp> {
    let mut ops = object_index_table_ops_with_previous(tables, object, previous);
    crate::file_history::stamp_policy_captures(tables, &mut ops, committed_at);
    ops
}

pub(crate) fn object_indexes_are_materialized(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> bool {
    if tables.compacted_tombstone_matches(object) {
        return true;
    }
    let path_record = path_record_for_object(object);
    if tables.objects.get(&object.object_id).cloned() != Some(normalized_object_metadata(object)) {
        return false;
    }
    if !tables
        .inodes
        .contains_key(&object_inode_id(&object.object_id))
    {
        return false;
    }
    if !tables
        .file_system_objects
        .contains_key(&(object.file_system_id.clone(), object.object_id.clone()))
    {
        return false;
    }
    if !tables
        .paths
        .contains_key(&(object.file_system_id.clone(), path_record.path.clone()))
    {
        return false;
    }
    if object.kind == DfsObjectKind::Tombstone {
        return true;
    }
    tables
        .dentries
        .get(&dentry_key(&path_record))
        .is_some_and(|record| {
            record.object_id == object.object_id
                && record.file_system_id == object.file_system_id
                && record.path == path_record.path
        })
}

pub(crate) fn object_index_table_ops_with_previous(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
    previous: Option<&DfsObjectMetadata>,
) -> Vec<DfsMetadataTableOp> {
    let object = normalized_object_metadata(object);
    let inode = DfsInodeRecord::from(&object);
    // Build the normalized path, parent inode, and name hash once. Bulk
    // namespace transactions commonly materialize thousands of siblings, so
    // repeating path normalization and the parent-path hash for each lookup is
    // measurable in the foreground commit path.
    let base_path_record = path_record_for_object(&object);
    let previous_path_record = previous.map(|previous| {
        stored_path_record_for_object(tables, previous)
            .unwrap_or_else(|| path_record_for_object(previous))
    });
    let existing_path_record = tables
        .paths
        .get(&(
            base_path_record.file_system_id.clone(),
            base_path_record.path.clone(),
        ))
        .cloned();
    let (path_record, selected_root_dirfrag) = if object.kind == DfsObjectKind::Tombstone {
        (base_path_record, None)
    } else if let Some(previous_path_record) = previous_path_record.as_ref()
        && previous_path_record.file_system_id == object.file_system_id
        && previous_path_record.path == base_path_record.path
        && previous_path_record.parent_inode_id == base_path_record.parent_inode_id
    {
        let mut path_record = base_path_record;
        path_record.dirfrag_id = previous_path_record.dirfrag_id.clone();
        (path_record, None)
    } else {
        path_record_for_new_dentry(tables, base_path_record)
    };

    // A normal live object emits exactly six index operations. Reserve a
    // little headroom for replacement/removal operations without making every
    // sibling grow its temporary vector from zero through two allocations.
    let mut table_ops = crate::file_history::policy_capture_table_ops(tables, &object, previous);
    table_ops.reserve(8);
    let mut removed_dentry = None::<DfsPathRecord>;
    if let Some(previous_target_object_id) =
        previous.and_then(|previous| previous.link_target_object_id.as_ref())
        && (object.kind != DfsObjectKind::Hardlink
            || object.link_target_object_id.as_ref() != Some(previous_target_object_id))
    {
        table_ops.push(DfsMetadataTableOp::RemoveHardlink {
            target_object_id: previous_target_object_id.clone(),
            hardlink_object_id: object.object_id.clone(),
        });
    }
    if let Some(previous) = previous {
        let previous_path_record = previous_path_record
            .as_ref()
            .cloned()
            .unwrap_or_else(|| path_record_for_object(previous));
        if previous.file_system_id != object.file_system_id {
            table_ops.push(DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id: previous.file_system_id.clone(),
                object_id: object.object_id.clone(),
            });
        }
        if previous_path_record.file_system_id != path_record.file_system_id
            || previous_path_record.path != path_record.path
        {
            table_ops.push(DfsMetadataTableOp::RemovePath {
                file_system_id: previous_path_record.file_system_id.clone(),
                path: previous_path_record.path.clone(),
            });
        }
        if previous.kind != DfsObjectKind::Tombstone
            && (object.kind == DfsObjectKind::Tombstone
                || previous_path_record.path != path_record.path
                || previous_path_record.parent_inode_id != path_record.parent_inode_id
                || previous_path_record.name != path_record.name)
        {
            removed_dentry = Some(previous_path_record.clone());
            remove_dentry_index_ops(tables, &previous_path_record, &mut table_ops);
        }
    }
    if tables
        .compacted_tombstone_paths
        .contains_key(&(path_record.file_system_id.clone(), path_record.path.clone()))
    {
        table_ops.push(DfsMetadataTableOp::RemoveCompactedTombstonePath {
            file_system_id: path_record.file_system_id.clone(),
            path: path_record.path.clone(),
        });
    }
    table_ops.push(DfsMetadataTableOp::PutFileSystemObject {
        path_record: path_record.clone(),
    });
    table_ops.push(DfsMetadataTableOp::PutObject {
        object: object.clone(),
    });
    table_ops.push(DfsMetadataTableOp::PutInode { inode });
    if object.kind == DfsObjectKind::Hardlink
        && let Some(target_object_id) = object.link_target_object_id.as_ref()
    {
        table_ops.push(DfsMetadataTableOp::PutHardlink {
            target_object_id: target_object_id.clone(),
            hardlink_object_id: object.object_id.clone(),
            path_record: path_record.clone(),
        });
    }
    if object.kind == DfsObjectKind::Tombstone {
        if let Some(existing_path_record) = existing_path_record.as_ref() {
            remove_dentry_index_ops(tables, existing_path_record, &mut table_ops);
        }
        if existing_path_record.as_ref() != Some(&path_record) {
            remove_dentry_index_ops(tables, &path_record, &mut table_ops);
        }
        table_ops.push(DfsMetadataTableOp::PutPath { path_record });
        return table_ops;
    }

    let removed_from_new_dirfrag = removed_dentry.as_ref().is_some_and(|removed| {
        removed.parent_inode_id == path_record.parent_inode_id
            && removed.dirfrag_id == path_record.dirfrag_id
    });
    let increment_entry_count =
        !tables.dentries.contains_key(&dentry_key(&path_record)) && !removed_from_new_dirfrag;
    let dirfrag = if let Some(mut root_dirfrag) = selected_root_dirfrag {
        if increment_entry_count {
            root_dirfrag.entry_count = root_dirfrag.entry_count.saturating_add(1);
        }
        root_dirfrag
    } else {
        read_or_create_dirfrag(
            tables,
            &path_record.file_system_id,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id,
            Some(path_record.name_hash),
            increment_entry_count,
        )
    };
    table_ops.push(DfsMetadataTableOp::PutPath {
        path_record: path_record.clone(),
    });
    table_ops.push(DfsMetadataTableOp::PutDentry { path_record });
    table_ops.push(DfsMetadataTableOp::PutDirFragment { dirfrag });
    table_ops
}

pub(crate) fn remove_dentry_index_ops(
    tables: &DfsMetadataTables,
    path_record: &DfsPathRecord,
    table_ops: &mut Vec<DfsMetadataTableOp>,
) {
    if !tables.dentries.contains_key(&dentry_key(path_record)) {
        return;
    }
    table_ops.push(DfsMetadataTableOp::RemoveDentry {
        parent_inode_id: path_record.parent_inode_id.clone(),
        dirfrag_id: path_record.dirfrag_id.clone(),
        name_hash: path_record.name_hash,
        name: path_record.name.clone(),
    });
    let Some(mut dirfrag) = tables
        .dirfrags
        .get(&(
            path_record.parent_inode_id.clone(),
            path_record.dirfrag_id.clone(),
        ))
        .cloned()
    else {
        return;
    };
    dirfrag.entry_count = dirfrag.entry_count.saturating_sub(1);
    table_ops.push(DfsMetadataTableOp::PutDirFragment { dirfrag });
}

fn stored_path_record_for_object(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> Option<DfsPathRecord> {
    tables
        .paths
        .get(&(object.file_system_id.clone(), normalize_path(&object.path)))
        .cloned()
}

pub(crate) fn path_record_for_new_dentry(
    tables: &DfsMetadataTables,
    mut path_record: DfsPathRecord,
) -> (DfsPathRecord, Option<DfsDirFragmentRecord>) {
    let root_dirfrag = read_or_create_dirfrag(
        tables,
        &path_record.file_system_id,
        &path_record.parent_inode_id,
        &DfsDirFragmentId::new("root"),
        None,
        false,
    );
    if root_dirfrag.entry_count >= root_dirfrag.split_after_count {
        path_record.dirfrag_id = bucket_dirfrag_id(path_record.name_hash);
        (path_record, None)
    } else {
        // The selected fragment is the record we just read. Return it so the
        // caller can apply the entry-count delta without a second tree lookup
        // and clone for every sibling.
        (path_record, Some(root_dirfrag))
    }
}

fn read_or_create_dirfrag(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    dir_inode_id: &crate::metadata_records::DfsInodeId,
    dirfrag_id: &DfsDirFragmentId,
    name_hash: Option<u64>,
    increment_entry_count: bool,
) -> DfsDirFragmentRecord {
    let (hash_start_inclusive, hash_end_exclusive) = dirfrag_hash_range(dirfrag_id, name_hash);
    let mut dirfrag = tables
        .dirfrags
        .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
        .cloned()
        .unwrap_or(DfsDirFragmentRecord {
            file_system_id: file_system_id.clone(),
            dir_inode_id: dir_inode_id.clone(),
            dirfrag_id: dirfrag_id.clone(),
            hash_start_inclusive,
            hash_end_exclusive,
            entry_count: 0,
            split_after_count: DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT,
        });
    if increment_entry_count {
        dirfrag.entry_count = dirfrag.entry_count.saturating_add(1);
    }
    dirfrag
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::node::NodeId;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_object_index").expect("valid filesystem id")
    }

    fn object_id(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object(path: &str) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id("obj_object_index"),
            file_system_id: file_system_id(),
            path: path.to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:index".to_string()),
            content_size_bytes: Some(5),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_object_index_1"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn sibling_object(index: usize, path: &str) -> DfsObjectMetadata {
        let mut object = object(path);
        object.object_id = object_id(&format!("obj_object_index_sibling_{index}"));
        object.version = version(&format!("version_object_index_sibling_{index}"));
        object
    }

    fn apply_object_indexes(
        tables: &mut DfsMetadataTables,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
    ) {
        for op in object_index_table_ops_with_previous(tables, object, previous) {
            tables.apply(op);
        }
    }

    #[test]
    fn object_index_ops_materialize_new_file_indexes() {
        let object = object("/docs/readme.md");
        let ops =
            object_index_table_ops_with_previous(&DfsMetadataTables::default(), &object, None);

        assert!(matches!(
            ops.as_slice(),
            [
                DfsMetadataTableOp::PutFileSystemObject { .. },
                DfsMetadataTableOp::PutObject { .. },
                DfsMetadataTableOp::PutInode { .. },
                DfsMetadataTableOp::PutPath { .. },
                DfsMetadataTableOp::PutDentry { .. },
                DfsMetadataTableOp::PutDirFragment { .. },
            ]
        ));
    }

    #[test]
    fn object_index_materialized_check_requires_dentry_for_live_file() {
        let object = object("/docs/readme.md");
        let mut tables = DfsMetadataTables::default();
        apply_object_indexes(&mut tables, &object, None);

        assert!(object_indexes_are_materialized(&tables, &object));
        let path_record = path_record_for_object(&object);
        tables.dentries.remove(&dentry_key(&path_record));
        assert!(!object_indexes_are_materialized(&tables, &object));
    }

    #[test]
    fn tombstone_with_new_object_id_removes_existing_path_dentry() {
        let object = object("/docs/readme.md");
        let mut tables = DfsMetadataTables::default();
        apply_object_indexes(&mut tables, &object, None);
        let live_path_record = path_record_for_object(&object);
        assert!(tables.dentries.contains_key(&dentry_key(&live_path_record)));

        let mut tombstone = object.clone();
        tombstone.object_id = object_id("obj_tombstone");
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        tombstone.current_locations = vec![node("node_owner")];
        tombstone.version = version("version_object_index_tombstone");

        apply_object_indexes(&mut tables, &tombstone, None);

        assert!(!tables.dentries.contains_key(&dentry_key(&live_path_record)));
        let tombstone_path_record = path_record_for_object(&tombstone);
        assert_eq!(
            tables
                .paths
                .get(&(tombstone.file_system_id.clone(), tombstone_path_record.path))
                .map(|record| record.object_id.clone()),
            Some(tombstone.object_id)
        );
    }

    #[test]
    fn bulk_sibling_indexes_materialize_once_and_retry_without_growing_dirfrag() {
        const SIBLING_COUNT: usize = 1_024;

        let mut tables = DfsMetadataTables::default();
        for index in 0..SIBLING_COUNT {
            let object = sibling_object(index, &format!("//bulk/file-{index:04}//"));
            apply_object_indexes(&mut tables, &object, None);
        }

        assert_eq!(tables.objects.len(), SIBLING_COUNT);
        assert_eq!(tables.file_system_objects.len(), SIBLING_COUNT);
        assert_eq!(tables.paths.len(), SIBLING_COUNT);
        assert_eq!(tables.inodes.len(), SIBLING_COUNT);
        assert_eq!(tables.dentries.len(), SIBLING_COUNT);
        assert_eq!(tables.dirfrags.len(), 1);

        let first = sibling_object(0, "//bulk/file-0000//");
        let first_path = path_record_for_object(&first);
        assert_eq!(first_path.path, "bulk/file-0000");
        let root_dirfrag_id = DfsDirFragmentId::new("root");
        let root_key = (first_path.parent_inode_id.clone(), root_dirfrag_id.clone());
        assert_eq!(
            tables
                .dirfrags
                .get(&root_key)
                .map(|dirfrag| dirfrag.entry_count),
            Some(SIBLING_COUNT as u64)
        );
        assert!(tables.dentries.contains_key(&dentry_key(&first_path)));

        let previous = tables
            .objects
            .get(&first.object_id)
            .cloned()
            .expect("first sibling is materialized");
        apply_object_indexes(&mut tables, &first, Some(&previous));

        assert_eq!(tables.objects.len(), SIBLING_COUNT);
        assert_eq!(tables.dentries.len(), SIBLING_COUNT);
        assert_eq!(
            tables
                .dirfrags
                .get(&root_key)
                .map(|dirfrag| dirfrag.entry_count),
            Some(SIBLING_COUNT as u64)
        );
    }

    #[test]
    fn new_dentries_cross_from_root_to_bucket_at_split_boundary() {
        let mut tables = DfsMetadataTables::default();
        let first = sibling_object(1, "/split/first");
        let first_path = path_record_for_object(&first);
        let root_dirfrag_id = DfsDirFragmentId::new("root");
        let root_key = (first_path.parent_inode_id.clone(), root_dirfrag_id.clone());
        let mut root_dirfrag = read_or_create_dirfrag(
            &tables,
            &first_path.file_system_id,
            &first_path.parent_inode_id,
            &root_dirfrag_id,
            None,
            false,
        );
        root_dirfrag.entry_count = DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT - 1;
        tables.dirfrags.insert(root_key.clone(), root_dirfrag);

        apply_object_indexes(&mut tables, &first, None);
        assert_eq!(
            tables
                .paths
                .get(&(first.file_system_id.clone(), first_path.path.clone()))
                .map(|path| path.dirfrag_id.clone()),
            Some(root_dirfrag_id)
        );
        assert_eq!(
            tables
                .dirfrags
                .get(&root_key)
                .map(|dirfrag| dirfrag.entry_count),
            Some(DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT)
        );

        let second = sibling_object(2, "/split/second");
        let second_path = path_record_for_object(&second);
        let bucket_dirfrag_id = bucket_dirfrag_id(second_path.name_hash);
        apply_object_indexes(&mut tables, &second, None);

        assert_eq!(
            tables
                .paths
                .get(&(second.file_system_id.clone(), second_path.path.clone()))
                .map(|path| path.dirfrag_id.clone()),
            Some(bucket_dirfrag_id.clone())
        );
        assert_eq!(
            tables
                .dirfrags
                .get(&root_key)
                .map(|dirfrag| dirfrag.entry_count),
            Some(DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT)
        );
        assert_eq!(
            tables
                .dirfrags
                .get(&(second_path.parent_inode_id, bucket_dirfrag_id))
                .map(|dirfrag| dirfrag.entry_count),
            Some(1)
        );
    }
}
