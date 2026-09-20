//! Visible-version commit table planning and recovery predicates.
//!
//! Backends still own journal writes and durable table appends. This module
//! owns the table-level decisions that determine whether a visible commit is
//! already materialized, recoverable after an interrupted write, or needs
//! object-location repair from its visible version record.

use crate::metadata_keys::{
    normalize_node_locations, normalize_path, normalized_node_location_list,
};
use crate::metadata_object_index::object_index_table_ops_with_previous;
use crate::metadata_records::{
    DfsMetadataTableOp, DfsObjectVersionRecord, DfsVisibleVersionCommitRecord,
    normalized_object_metadata, normalized_version_record,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion};
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

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

pub(crate) fn visible_commit_already_materialized(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    committed_version: &DfsVersion,
    commit: &DfsVisibleVersionCommitRecord,
) -> bool {
    if &commit.committed_object.version != committed_version {
        return false;
    }
    let commit_key = (object_id.clone(), committed_version.clone());
    let Some(existing) = tables.visible_commits.get(&commit_key) else {
        return false;
    };
    if existing.expected_version != commit.expected_version
        || existing.committed_object != commit.committed_object
        || existing.visible_version != commit.visible_version
        || existing.required_replica_node_ids != commit.required_replica_node_ids
    {
        return false;
    }
    let mut visible_version = normalized_version_record(&commit.visible_version);
    let Some(stored) = tables
        .versions
        .get(&(object_id.clone(), committed_version.clone()))
    else {
        return false;
    };
    visible_version.history_order = stored.history_order;
    if visible_version.retained.is_none() {
        visible_version.retained = stored.retained.clone();
    }
    if visible_version.content_durability.is_none() {
        visible_version.content_durability = stored.content_durability.clone();
    }
    stored == &visible_version
}

pub(crate) fn visible_commit_record_from_parts(
    expected_version: &DfsVersion,
    committed_object: &DfsObjectMetadata,
    visible_version: &DfsObjectVersionRecord,
    required_replica_node_ids: &[NodeId],
    created_at: OffsetDateTime,
) -> DfsVisibleVersionCommitRecord {
    DfsVisibleVersionCommitRecord {
        expected_version: expected_version.clone(),
        committed_object: normalized_object_metadata(committed_object),
        visible_version: normalized_version_record(visible_version),
        required_replica_node_ids: normalized_node_location_list(required_replica_node_ids),
        operation_id: None,
        created_at,
    }
}

pub(crate) fn visible_commit_table_ops(
    tables: &DfsMetadataTables,
    commit: &DfsVisibleVersionCommitRecord,
) -> Vec<DfsMetadataTableOp> {
    let mut table_ops = vec![DfsMetadataTableOp::PutVisibleCommit {
        object_id: commit.committed_object.object_id.clone(),
        version: commit.committed_object.version.clone(),
        commit: commit.clone(),
    }];
    table_ops.extend(crate::metadata_object_index::object_index_table_ops_at(
        tables,
        &commit.committed_object,
        None,
        commit.created_at,
    ));
    table_ops.push(DfsMetadataTableOp::PutVersion {
        version: normalized_version_record(&commit.visible_version),
    });
    table_ops
}

pub(crate) fn visible_commit_recovery_order(
    tables: &DfsMetadataTables,
) -> Vec<DfsVisibleVersionCommitRecord> {
    let mut commits = tables.visible_commits.values().cloned().collect::<Vec<_>>();
    commits.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| {
                left.committed_object
                    .object_id
                    .cmp(&right.committed_object.object_id)
            })
            .then_with(|| {
                left.committed_object
                    .version
                    .cmp(&right.committed_object.version)
            })
    });
    commits
}

pub(crate) fn visible_commit_should_recover(
    tables: &DfsMetadataTables,
    commit: &DfsVisibleVersionCommitRecord,
) -> bool {
    if object_by_path_from_tables(
        tables,
        &commit.committed_object.file_system_id,
        &commit.committed_object.path,
    )
    .is_some_and(|object| object.kind == DfsObjectKind::Tombstone)
    {
        return false;
    }
    for node_id in &commit.required_replica_node_ids {
        if !tables.receipts.contains_key(&(
            commit.committed_object.object_id.clone(),
            commit.committed_object.version.clone(),
            node_id.clone(),
        )) {
            return false;
        }
    }

    let current = tables.objects.get(&commit.committed_object.object_id);
    if current.as_ref().is_some_and(|object| {
        object.kind == DfsObjectKind::Tombstone
            && normalize_path(&object.path) == normalize_path(&commit.committed_object.path)
    }) {
        return false;
    }
    if current
        .as_ref()
        .is_some_and(|object| object.version == commit.committed_object.version)
        && tables.versions.contains_key(&(
            commit.visible_version.object_id.clone(),
            commit.visible_version.version.clone(),
        ))
    {
        return false;
    }
    if current.as_ref().is_some_and(|object| {
        object.version != commit.expected_version
            && object.version != commit.committed_object.version
    }) {
        return false;
    }
    true
}

pub(crate) fn visible_object_location_recovery_ops(
    tables: &DfsMetadataTables,
) -> Vec<DfsMetadataTableOp> {
    let mut scratch = tables.clone();
    let mut table_ops = Vec::new();
    for mut object in tables.objects.values().cloned() {
        if object.kind == DfsObjectKind::Tombstone {
            continue;
        }
        let Some(version) = tables
            .versions
            .get(&(object.object_id.clone(), object.version.clone()))
        else {
            continue;
        };
        if !version.visible
            || version.locations.is_empty()
            || object.content_hash != version.content_hash
        {
            continue;
        }
        let mut object_locations = object.current_locations.clone();
        let mut version_locations = version.locations.clone();
        normalize_node_locations(&mut object_locations);
        normalize_node_locations(&mut version_locations);
        if object_locations == version_locations {
            continue;
        }
        object.current_locations = version_locations;
        for op in object_index_table_ops_with_previous(&scratch, &object, None) {
            scratch.apply(op.clone());
            table_ops.push(op);
        }
    }
    table_ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_keys::path_record_for_object;
    use crate::metadata_records::{
        DfsMetadataTableOp, DfsObjectVersionRecord, DfsReplicaReceiptRecord,
    };
    use sinomo_core::events::EventId;
    use sinomo_core::node::NodeId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_visible_commit").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_visible_commit").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object(version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "/docs/visible.md".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_a")],
            content_hash: Some("blake3:visible".to_string()),
            content_size_bytes: Some(42),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn version_record(
        object: &DfsObjectMetadata,
        locations: Vec<NodeId>,
    ) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations,
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    fn commit() -> DfsVisibleVersionCommitRecord {
        let committed_object = object(version("version_visible_new"));
        DfsVisibleVersionCommitRecord {
            expected_version: version("version_visible_old"),
            visible_version: version_record(&committed_object, vec![node("node_a")]),
            required_replica_node_ids: vec![node("node_a")],
            committed_object,
            operation_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn visible_commit_ops_materialize_commit_object_and_visible_version() {
        let commit = commit();
        let ops = visible_commit_table_ops(&DfsMetadataTables::default(), &commit);

        assert!(matches!(
            ops.first(),
            Some(DfsMetadataTableOp::PutVisibleCommit { .. })
        ));
        assert!(ops.iter().any(|op| {
            matches!(op, DfsMetadataTableOp::PutObject { object } if object == &commit.committed_object)
        }));
        assert!(ops.iter().any(|op| {
            matches!(op, DfsMetadataTableOp::PutVersion { version } if version == &commit.visible_version)
        }));
    }

    #[test]
    fn visible_commit_record_from_parts_normalizes_locations() {
        let mut committed_object = object(version("version_visible_new"));
        committed_object.current_locations = vec![node("node_b"), node("node_a"), node("node_a")];
        let mut visible_version = version_record(
            &committed_object,
            vec![node("node_c"), node("node_b"), node("node_b")],
        );
        visible_version.version = committed_object.version.clone();

        let commit = visible_commit_record_from_parts(
            &version("version_visible_old"),
            &committed_object,
            &visible_version,
            &[node("node_c"), node("node_b"), node("node_b")],
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(
            commit.committed_object.current_locations,
            vec![node("node_b"), node("node_a")]
        );
        assert_eq!(
            commit.visible_version.locations,
            vec![node("node_c"), node("node_b")]
        );
        assert_eq!(
            commit.required_replica_node_ids,
            vec![node("node_c"), node("node_b")]
        );
    }

    #[test]
    fn recovery_predicate_requires_receipts_and_skips_tombstone_path() {
        let commit = commit();
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object(commit.expected_version.clone()),
        });
        assert!(!visible_commit_should_recover(&tables, &commit));

        tables.apply(DfsMetadataTableOp::PutReceipt {
            receipt: DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: commit.committed_object.file_system_id.clone(),
                    object_id: commit.committed_object.object_id.clone(),
                    committed_version: commit.committed_object.version.clone(),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node("node_a"),
                content_hash: commit.committed_object.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
        });
        assert!(visible_commit_should_recover(&tables, &commit));

        let mut tombstone = commit.committed_object.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tables.apply(DfsMetadataTableOp::PutObject {
            object: tombstone.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutPath {
            path_record: path_record_for_object(&tombstone),
        });
        assert!(!visible_commit_should_recover(&tables, &commit));
    }

    #[test]
    fn location_recovery_ops_sync_object_locations_from_visible_version() {
        let mut object = object(version("version_visible_new"));
        object.current_locations = vec![node("node_a")];
        let visible_version = version_record(&object, vec![node("node_b"), node("node_a")]);
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: visible_version,
        });

        let ops = visible_object_location_recovery_ops(&tables);

        let recovered = ops
            .iter()
            .find_map(|op| match op {
                DfsMetadataTableOp::PutObject { object } => Some(object),
                _ => None,
            })
            .expect("recovered object");
        let mut expected_locations = vec![node("node_b"), node("node_a")];
        normalize_node_locations(&mut expected_locations);
        assert_eq!(recovered.current_locations, expected_locations);
    }
}
