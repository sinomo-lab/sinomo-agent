//! Namespace transaction table planning.
//!
//! Backends own journal writes, locks, and durable table appends. This module
//! owns the pure materialization decisions for namespace transactions so normal
//! apply, replay, and recovery all use the same table-operation planner.

use std::collections::BTreeSet;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_keys::{dentry_key, path_record_for_object};
use crate::metadata_object_index::{
    object_index_table_ops_with_previous, object_indexes_are_materialized,
};
use crate::metadata_pages::child_objects_for_directory_from_tables;
use crate::metadata_records::{
    DfsMetadataTableOp, DfsNamespaceTransaction, normalized_version_record,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsObjectId, DfsObjectKind, DfsObjectMetadata};

pub(crate) fn validate_namespace_transaction_preconditions(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Result<()> {
    let mut scratch = tables.clone();
    for op in plan_namespace_transaction_table_ops(tables, transaction) {
        scratch.apply(op);
    }
    for object in &transaction.objects {
        validate_directory_tombstone_has_no_children(tables, &scratch, object)?;
    }
    Ok(())
}

pub(crate) fn plan_namespace_transaction_table_ops(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Vec<DfsMetadataTableOp> {
    if transaction.objects.len() == 1 {
        return plan_single_object_namespace_transaction_table_ops(tables, transaction);
    }
    let mut scratch = tables.clone();
    let mut table_ops = Vec::new();
    apply_planned_op(
        &mut scratch,
        &mut table_ops,
        DfsMetadataTableOp::PutNamespaceTransaction {
            transaction: transaction.clone(),
        },
    );
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        if scratch.compacted_tombstone_matches(object) {
            continue;
        }
        plan_object_index_ops(&mut scratch, &mut table_ops, object);
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        if scratch.compacted_tombstone_matches(object) {
            continue;
        }
        plan_object_index_ops(&mut scratch, &mut table_ops, object);
    }
    for version in &transaction.versions {
        apply_planned_op(
            &mut scratch,
            &mut table_ops,
            DfsMetadataTableOp::PutVersion {
                version: normalized_version_record(version),
            },
        );
    }
    table_ops
}

fn plan_single_object_namespace_transaction_table_ops(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Vec<DfsMetadataTableOp> {
    let mut table_ops = Vec::new();
    table_ops.push(DfsMetadataTableOp::PutNamespaceTransaction {
        transaction: transaction.clone(),
    });
    if let Some(object) = transaction.objects.first()
        && !tables.compacted_tombstone_matches(object)
    {
        table_ops.extend(object_index_table_ops_with_previous(
            tables,
            object,
            tables.objects.get(&object.object_id),
        ));
    }
    for version in &transaction.versions {
        table_ops.push(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(version),
        });
    }
    table_ops
}

pub(crate) fn plan_namespace_transaction_recovery_table_ops(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Vec<DfsMetadataTableOp> {
    let mut scratch = tables.clone();
    let mut table_ops = Vec::new();
    let transaction_object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        plan_object_index_recovery_ops(
            &mut scratch,
            &mut table_ops,
            object,
            &transaction_object_ids,
        );
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        plan_object_index_recovery_ops(
            &mut scratch,
            &mut table_ops,
            object,
            &transaction_object_ids,
        );
    }
    for version in &transaction.versions {
        let version = normalized_version_record(version);
        if scratch
            .versions
            .get(&(version.object_id.clone(), version.version.clone()))
            != Some(&version)
        {
            apply_planned_op(
                &mut scratch,
                &mut table_ops,
                DfsMetadataTableOp::PutVersion { version },
            );
        }
    }
    table_ops
}

pub(crate) fn object_path_has_external_owner(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
    transaction_object_ids: &BTreeSet<DfsObjectId>,
) -> bool {
    let path_record = path_record_for_object(object);
    let owns_path = |object_id: &DfsObjectId| {
        object_id == &object.object_id || transaction_object_ids.contains(object_id)
    };
    if tables
        .paths
        .get(&(object.file_system_id.clone(), path_record.path.clone()))
        .is_some_and(|record| !owns_path(&record.object_id))
    {
        return true;
    }
    tables
        .dentries
        .get(&dentry_key(&path_record))
        .is_some_and(|record| !owns_path(&record.object_id))
}

fn validate_directory_tombstone_has_no_children(
    before: &DfsMetadataTables,
    after: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> Result<()> {
    if object.kind != DfsObjectKind::Tombstone {
        return Ok(());
    }
    let Some(current) = before.objects.get(&object.object_id) else {
        return Ok(());
    };
    if current.kind != DfsObjectKind::Directory || current.path != object.path {
        return Ok(());
    }
    if child_objects_for_directory_from_tables(after, &object.file_system_id, &object.path)
        .is_empty()
    {
        return Ok(());
    }
    Err(MetadataBackendError::DirectoryNotEmpty {
        path: object.path.clone(),
    })
}

fn plan_object_index_ops(
    scratch: &mut DfsMetadataTables,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    object: &DfsObjectMetadata,
) {
    for op in object_index_table_ops_with_previous(scratch, object, None) {
        apply_planned_op(scratch, table_ops, op);
    }
}

fn plan_object_index_recovery_ops(
    scratch: &mut DfsMetadataTables,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    object: &DfsObjectMetadata,
    transaction_object_ids: &BTreeSet<DfsObjectId>,
) {
    if scratch.compacted_tombstone_matches(object) {
        return;
    }
    let current = scratch.objects.get(&object.object_id).cloned();
    let object = current.as_ref().unwrap_or(object);
    if object_path_has_external_owner(scratch, object, transaction_object_ids)
        || object_indexes_are_materialized(scratch, object)
    {
        return;
    }
    plan_object_index_ops(scratch, table_ops, object);
}

fn apply_planned_op(
    scratch: &mut DfsMetadataTables,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    op: DfsMetadataTableOp,
) {
    scratch.apply(op.clone());
    table_ops.push(op);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_keys::{dentry_key, path_record_for_object};
    use crate::metadata_records::{DfsInodeRecord, DfsObjectVersionRecord};
    use crate::{DfsFileSystemId, DfsVersion};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::node::NodeId;
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_namespace_txn").expect("valid filesystem id")
    }

    fn object_id(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object(object_id: DfsObjectId, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id,
            file_system_id: file_system_id(),
            path: path.to_string(),
            kind,
            owner_node_id: node("node_owner"),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: version("version_namespace_txn"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn version_record(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    #[test]
    fn transaction_plan_materializes_tombstones_before_live_objects() {
        let live = object(
            object_id("obj_namespace_live"),
            "/docs/live.md",
            DfsObjectKind::File,
        );
        let tombstone = object(
            object_id("obj_namespace_deleted"),
            "/docs/deleted.md",
            DfsObjectKind::Tombstone,
        );
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_order".to_string(),
            file_system_id: file_system_id(),
            objects: vec![live.clone(), tombstone.clone()],
            versions: vec![version_record(&live)],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let ops = plan_namespace_transaction_table_ops(&DfsMetadataTables::default(), &transaction);

        assert!(matches!(
            ops.first(),
            Some(DfsMetadataTableOp::PutNamespaceTransaction { .. })
        ));
        let tombstone_position = ops
            .iter()
            .position(|op| {
                matches!(op, DfsMetadataTableOp::PutObject { object } if object.object_id == tombstone.object_id)
            })
            .expect("tombstone object op");
        let live_position = ops
            .iter()
            .position(|op| {
                matches!(op, DfsMetadataTableOp::PutObject { object } if object.object_id == live.object_id)
            })
            .expect("live object op");
        assert!(tombstone_position < live_position);
        assert!(ops.iter().any(|op| matches!(op, DfsMetadataTableOp::PutVersion { version } if version.object_id == live.object_id)));
    }

    #[test]
    fn recovery_plan_skips_path_owned_outside_transaction() {
        let existing = object(
            object_id("obj_namespace_existing"),
            "/docs/shared.md",
            DfsObjectKind::File,
        );
        let recovering = object(
            object_id("obj_namespace_recovering"),
            "/docs/shared.md",
            DfsObjectKind::File,
        );
        let existing_path = path_record_for_object(&existing);
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject {
            object: recovering.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutPath {
            path_record: existing_path.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutDentry {
            path_record: existing_path,
        });
        tables.apply(DfsMetadataTableOp::PutInode {
            inode: DfsInodeRecord::from(&recovering),
        });
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_recovery_skip".to_string(),
            file_system_id: file_system_id(),
            objects: vec![recovering.clone()],
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let ops = plan_namespace_transaction_recovery_table_ops(&tables, &transaction);

        assert!(ops.is_empty());
        assert!(object_path_has_external_owner(
            &tables,
            &recovering,
            &BTreeSet::from([recovering.object_id.clone()])
        ));
        assert!(
            tables
                .dentries
                .contains_key(&dentry_key(&path_record_for_object(&existing)))
        );
    }
}
