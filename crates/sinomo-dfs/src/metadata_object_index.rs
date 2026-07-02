//! Object metadata index planning.
//!
//! Local metadata backends append durable table batches. This module computes
//! the object-related table operations for path, inode, dentry, hardlink, and
//! directory-fragment indexes from an in-memory table snapshot.

use crate::metadata_keys::{
    bucket_dirfrag_id, dentry_key, dirfrag_hash_range, normalize_path, object_inode_id,
    parent_inode_id, path_record_for_object,
};
use crate::metadata_records::{
    DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeRecord, DfsMetadataTableOp, DfsPathRecord,
    normalized_object_metadata,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectKind, DfsObjectMetadata};

const DEFAULT_DIRFRAG_SPLIT_AFTER_COUNT: u64 = 4096;

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
    let previous_path_record = previous.map(|previous| {
        stored_path_record_for_object(tables, previous)
            .unwrap_or_else(|| path_record_for_object(previous))
    });
    let path_record = if object.kind == DfsObjectKind::Tombstone {
        path_record_for_object(&object)
    } else if let Some(previous_path_record) = previous_path_record.as_ref()
        && previous_path_record.file_system_id == object.file_system_id
        && previous_path_record.path == normalize_path(&object.path)
        && previous_path_record.parent_inode_id
            == parent_inode_id(&object.file_system_id, &normalize_path(&object.path))
    {
        let mut path_record = path_record_for_object(&object);
        path_record.dirfrag_id = previous_path_record.dirfrag_id.clone();
        path_record
    } else {
        path_record_for_new_dentry(tables, &object)
    };

    let mut table_ops = Vec::new();
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
        remove_dentry_index_ops(tables, &path_record, &mut table_ops);
        table_ops.push(DfsMetadataTableOp::PutPath { path_record });
        return table_ops;
    }

    let removed_from_new_dirfrag = removed_dentry.as_ref().is_some_and(|removed| {
        removed.parent_inode_id == path_record.parent_inode_id
            && removed.dirfrag_id == path_record.dirfrag_id
    });
    let dirfrag = read_or_create_dirfrag(
        tables,
        &path_record.file_system_id,
        &path_record.parent_inode_id,
        &path_record.dirfrag_id,
        Some(path_record.name_hash),
        !tables.dentries.contains_key(&dentry_key(&path_record)) && !removed_from_new_dirfrag,
    );
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

fn path_record_for_new_dentry(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> DfsPathRecord {
    let mut path_record = path_record_for_object(object);
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
    }
    path_record
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
        for op in object_index_table_ops_with_previous(&tables, &object, None) {
            tables.apply(op);
        }

        assert!(object_indexes_are_materialized(&tables, &object));
        let path_record = path_record_for_object(&object);
        tables.dentries.remove(&dentry_key(&path_record));
        assert!(!object_indexes_are_materialized(&tables, &object));
    }
}
