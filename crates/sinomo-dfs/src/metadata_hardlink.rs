//! Hardlink read-model helpers for metadata tables.
//!
//! Backends decide how to page or merge hardlink results. This module owns
//! the shared predicate/reconstruction logic and deterministic presentation
//! ordering.

use crate::metadata_keys::normalize_path;
use crate::metadata_records::DfsPathRecord;
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata};

pub(crate) fn hardlink_object_from_path_record(
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
    path_record: &DfsPathRecord,
) -> Option<DfsObjectMetadata> {
    if path_record.file_system_id != *file_system_id {
        return None;
    }
    let object = tables.objects.get(&path_record.object_id)?.clone();
    if object_is_hardlink_to_target(&object, file_system_id, target_object_id)
        && normalize_path(&object.path) == path_record.path
    {
        Some(object)
    } else {
        None
    }
}

pub(crate) fn object_is_hardlink_to_target(
    object: &DfsObjectMetadata,
    file_system_id: &DfsFileSystemId,
    target_object_id: &DfsObjectId,
) -> bool {
    object.kind == DfsObjectKind::Hardlink
        && object.file_system_id == *file_system_id
        && object.link_target_object_id.as_ref() == Some(target_object_id)
}

pub(crate) fn sort_hardlinks_by_path(objects: &mut [DfsObjectMetadata]) {
    objects.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.object_id.cmp(&right.object_id))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_keys::path_record_for_object;
    use crate::metadata_records::DfsMetadataTableOp;
    use crate::{DfsFileSystemId, DfsVersion};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_node::NodeId;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_hardlink").expect("valid filesystem id")
    }

    fn object_id(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("valid object id")
    }

    fn version() -> DfsVersion {
        DfsVersion::from_legacy_named("version_hardlink_1").expect("valid version")
    }

    fn hardlink(
        hardlink_object_id: &str,
        path: &str,
        target_object_id: &DfsObjectId,
    ) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(hardlink_object_id),
            file_system_id: file_system_id(),
            path: path.to_string(),
            kind: DfsObjectKind::Hardlink,
            owner_node_id: node("node_owner"),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: Some(target_object_id.clone()),
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn reconstructs_hardlink_from_matching_path_record() {
        let target_object_id = object_id("obj_target");
        let object = hardlink("obj_hardlink", "/docs/link.md", &target_object_id);
        let path_record = path_record_for_object(&object);
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });

        assert_eq!(
            hardlink_object_from_path_record(
                &tables,
                &file_system_id(),
                &target_object_id,
                &path_record,
            ),
            Some(object)
        );
    }

    #[test]
    fn sorts_hardlinks_by_path_then_object_id() {
        let target_object_id = object_id("obj_target");
        let mut objects = vec![
            hardlink("obj_hardlink_b", "/docs/b.md", &target_object_id),
            hardlink("obj_hardlink_a", "/docs/a.md", &target_object_id),
        ];

        sort_hardlinks_by_path(&mut objects);

        assert_eq!(objects[0].path, "/docs/a.md");
        assert_eq!(objects[1].path, "/docs/b.md");
    }
}
