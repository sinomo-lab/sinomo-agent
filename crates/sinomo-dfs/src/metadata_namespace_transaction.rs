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
    validate_advisory_lock_state_preconditions(tables, transaction)?;
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
    for state in &transaction.advisory_lock_states {
        apply_planned_op(
            &mut scratch,
            &mut table_ops,
            DfsMetadataTableOp::PutAdvisoryLockState {
                state: state.clone(),
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
    for state in &transaction.advisory_lock_states {
        table_ops.push(DfsMetadataTableOp::PutAdvisoryLockState {
            state: state.clone(),
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
    let mut transaction_authoritative_object_ids = BTreeSet::new();
    let mut compacted_tombstone_fenced_object_ids = BTreeSet::new();
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
            &mut transaction_authoritative_object_ids,
            &mut compacted_tombstone_fenced_object_ids,
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
            &mut transaction_authoritative_object_ids,
            &mut compacted_tombstone_fenced_object_ids,
        );
    }
    for version in &transaction.versions {
        let version = normalized_version_record(version);
        if compacted_tombstone_fenced_object_ids.contains(&version.object_id) {
            continue;
        }
        // A present version record may have been enriched by later receipt or
        // repair activity. Namespace recovery fills partial writes; it must
        // never roll an existing same-key record back to the historical
        // transaction payload.
        if !scratch
            .versions
            .contains_key(&(version.object_id.clone(), version.version.clone()))
        {
            apply_planned_op(
                &mut scratch,
                &mut table_ops,
                DfsMetadataTableOp::PutVersion { version },
            );
        }
    }
    for state in &transaction.advisory_lock_states {
        if compacted_tombstone_fenced_object_ids.contains(&state.object_id) {
            continue;
        }
        let existing = scratch.advisory_lock_states.get(&state.object_id);
        let should_recover = match existing {
            None => true,
            Some(existing) if existing == state => false,
            Some(existing) => {
                transaction_authoritative_object_ids.contains(&state.object_id)
                    && !advisory_lock_state_descends_from(existing, state)
            }
        };
        if should_recover {
            apply_planned_op(
                &mut scratch,
                &mut table_ops,
                DfsMetadataTableOp::PutAdvisoryLockState {
                    state: state.clone(),
                },
            );
        }
    }
    table_ops
}

/// Return true when recovery planning is guaranteed to produce no table ops.
///
/// This mirrors the recovery planner's decisions without cloning the complete
/// metadata table set. The check stops at the first missing materialization;
/// callers must run the full scratch-table planner in that case because later
/// operations in a multi-object transaction can depend on earlier repairs.
pub(crate) fn namespace_transaction_recovery_is_complete(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> bool {
    let transaction_object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    let mut transaction_authoritative_object_ids = BTreeSet::new();
    let mut compacted_tombstone_fenced_object_ids = BTreeSet::new();

    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        if !namespace_transaction_object_recovery_is_complete(
            tables,
            object,
            &transaction_object_ids,
            &mut transaction_authoritative_object_ids,
            &mut compacted_tombstone_fenced_object_ids,
        ) {
            return false;
        }
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        if !namespace_transaction_object_recovery_is_complete(
            tables,
            object,
            &transaction_object_ids,
            &mut transaction_authoritative_object_ids,
            &mut compacted_tombstone_fenced_object_ids,
        ) {
            return false;
        }
    }

    if transaction.versions.iter().any(|version| {
        let version = normalized_version_record(version);
        !compacted_tombstone_fenced_object_ids.contains(&version.object_id)
            && !tables
                .versions
                .contains_key(&(version.object_id, version.version))
    }) {
        return false;
    }

    transaction.advisory_lock_states.iter().all(|state| {
        if compacted_tombstone_fenced_object_ids.contains(&state.object_id) {
            return true;
        }
        match tables.advisory_lock_states.get(&state.object_id) {
            None => false,
            Some(existing) if existing == state => true,
            Some(existing) => {
                !transaction_authoritative_object_ids.contains(&state.object_id)
                    || advisory_lock_state_descends_from(existing, state)
            }
        }
    })
}

fn namespace_transaction_object_recovery_is_complete(
    tables: &DfsMetadataTables,
    transaction_object: &DfsObjectMetadata,
    transaction_object_ids: &BTreeSet<DfsObjectId>,
    transaction_authoritative_object_ids: &mut BTreeSet<DfsObjectId>,
    compacted_tombstone_fenced_object_ids: &mut BTreeSet<DfsObjectId>,
) -> bool {
    if tables.compacted_tombstone_fences_namespace_recovery(transaction_object) {
        compacted_tombstone_fenced_object_ids.insert(transaction_object.object_id.clone());
        return true;
    }
    let current = tables.objects.get(&transaction_object.object_id);
    let transaction_is_authoritative = current.is_none_or(|current| {
        transaction_object_recovers_over_current(transaction_object, current)
    });
    let object = if transaction_is_authoritative {
        transaction_object
    } else {
        current.expect("a non-authoritative transaction object has a current record")
    };
    if object_path_has_external_owner(tables, object, transaction_object_ids) {
        return true;
    }
    if transaction_is_authoritative {
        transaction_authoritative_object_ids.insert(transaction_object.object_id.clone());
    }
    object_indexes_are_materialized(tables, object)
}

fn validate_advisory_lock_state_preconditions(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Result<()> {
    let transaction_object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for state in &transaction.advisory_lock_states {
        if state.file_system_id != transaction.file_system_id {
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "namespace transaction {} contains lock state for filesystem {}",
                transaction.transaction_id, state.file_system_id
            )));
        }
        if !seen.insert(state.object_id.clone()) {
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "namespace transaction {} repeats lock state for object {}",
                transaction.transaction_id, state.object_id
            )));
        }
        if !transaction_object_ids.contains(&state.object_id)
            && !tables.objects.contains_key(&state.object_id)
        {
            return Err(MetadataBackendError::UnknownObject {
                object_id: state.object_id.clone(),
            });
        }
        if let Some(existing) = tables.advisory_lock_states.get(&state.object_id)
            && existing != state
        {
            let reclaims_tombstoned_object = tables
                .objects
                .get(&state.object_id)
                .is_some_and(|object| object.kind == crate::DfsObjectKind::Tombstone)
                && transaction.objects.iter().any(|object| {
                    object.object_id == state.object_id
                        && object.kind != crate::DfsObjectKind::Tombstone
                });
            if !reclaims_tombstoned_object {
                return Err(MetadataBackendError::AdvisoryLock(format!(
                    "namespace transaction {} conflicts with advisory-lock state for object {}",
                    transaction.transaction_id, state.object_id
                )));
            }
        }
    }
    Ok(())
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
    transaction_object: &DfsObjectMetadata,
    transaction_object_ids: &BTreeSet<DfsObjectId>,
    transaction_authoritative_object_ids: &mut BTreeSet<DfsObjectId>,
    compacted_tombstone_fenced_object_ids: &mut BTreeSet<DfsObjectId>,
) {
    if scratch.compacted_tombstone_fences_namespace_recovery(transaction_object) {
        compacted_tombstone_fenced_object_ids.insert(transaction_object.object_id.clone());
        return;
    }
    let current = scratch.objects.get(&transaction_object.object_id).cloned();
    let transaction_is_authoritative = current.as_ref().is_none_or(|current| {
        transaction_object_recovers_over_current(transaction_object, current)
    });
    let object = if transaction_is_authoritative {
        transaction_object
    } else {
        current
            .as_ref()
            .expect("a non-authoritative transaction object has a current record")
    };
    if object_path_has_external_owner(scratch, object, transaction_object_ids) {
        return;
    }
    if transaction_is_authoritative {
        transaction_authoritative_object_ids.insert(transaction_object.object_id.clone());
    }
    if object_indexes_are_materialized(scratch, object) {
        return;
    }
    for op in object_index_table_ops_with_previous(scratch, object, current.as_ref()) {
        apply_planned_op(scratch, table_ops, op);
    }
}

fn transaction_object_recovers_over_current(
    transaction_object: &DfsObjectMetadata,
    current: &DfsObjectMetadata,
) -> bool {
    if transaction_object == current {
        return true;
    }
    let transaction_generation = (
        transaction_object.changed_at_unix_nanos,
        &transaction_object.version,
    );
    let current_generation = (current.changed_at_unix_nanos, &current.version);
    transaction_generation > current_generation
        || (transaction_generation == current_generation
            && transaction_object.kind != DfsObjectKind::Tombstone
            && current.kind == DfsObjectKind::Tombstone)
}

fn advisory_lock_state_descends_from(
    current: &crate::DfsAdvisoryLockStateRecord,
    transaction_state: &crate::DfsAdvisoryLockStateRecord,
) -> bool {
    !transaction_state.migration_source_fingerprints.is_empty()
        && transaction_state
            .migration_source_fingerprints
            .iter()
            .all(|fingerprint| current.migration_source_fingerprints.contains(fingerprint))
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
    use crate::{DfsAdvisoryLockStateRecord, DfsFileSystemId, DfsVersion};
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
            advisory_lock_states: Vec::new(),
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
    fn completed_transaction_skips_scratch_table_recovery_plan() {
        let live = object(
            object_id("obj_namespace_complete_live"),
            "/docs/complete.md",
            DfsObjectKind::File,
        );
        let tombstone = object(
            object_id("obj_namespace_complete_tombstone"),
            "/docs/replaced.md",
            DfsObjectKind::Tombstone,
        );
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_complete".to_string(),
            file_system_id: file_system_id(),
            objects: vec![live.clone(), tombstone],
            versions: vec![version_record(&live)],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut tables = DfsMetadataTables::default();
        for op in plan_namespace_transaction_table_ops(&tables, &transaction) {
            tables.apply(op);
        }

        assert!(namespace_transaction_recovery_is_complete(
            &tables,
            &transaction
        ));
        assert!(plan_namespace_transaction_recovery_table_ops(&tables, &transaction).is_empty());

        let live_path = path_record_for_object(&live);
        tables
            .paths
            .remove(&(live.file_system_id.clone(), live_path.path.clone()));
        assert!(
            !namespace_transaction_recovery_is_complete(&tables, &transaction),
            "a missing object index must fall back to scratch-table recovery planning"
        );
        assert!(!plan_namespace_transaction_recovery_table_ops(&tables, &transaction).is_empty());
    }

    #[test]
    fn newer_compacted_tombstone_fences_historical_live_transaction_recovery() {
        let mut historical_live = object(
            object_id("obj_namespace_compacted_fence"),
            "/docs/compacted-fence.md",
            DfsObjectKind::File,
        );
        historical_live.changed_at_unix_nanos = 20;
        historical_live.version = version("version_namespace_compacted_live");
        let historical_version = version_record(&historical_live);
        let historical_lock = DfsAdvisoryLockStateRecord::empty(
            historical_live.file_system_id.clone(),
            historical_live.object_id.clone(),
            7,
        );
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_compacted_fence".to_string(),
            file_system_id: historical_live.file_system_id.clone(),
            objects: vec![historical_live.clone()],
            versions: vec![historical_version],
            advisory_lock_states: vec![historical_lock],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let mut compacted_tombstone = historical_live.clone();
        compacted_tombstone.kind = DfsObjectKind::Tombstone;
        compacted_tombstone.changed_at_unix_nanos = 30;
        compacted_tombstone.version = version("version_namespace_compacted_tombstone");
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutCompactedTombstone {
            object: compacted_tombstone.clone(),
        });

        assert!(namespace_transaction_recovery_is_complete(
            &tables,
            &transaction
        ));
        assert!(
            plan_namespace_transaction_recovery_table_ops(&tables, &transaction).is_empty(),
            "recovery must not resurrect an object family superseded by a compacted tombstone"
        );
        assert_eq!(
            tables
                .compacted_tombstones
                .get(&compacted_tombstone.object_id),
            Some(&compacted_tombstone)
        );
        assert!(!tables.objects.contains_key(&historical_live.object_id));
        assert!(tables.versions.is_empty());
        assert!(tables.advisory_lock_states.is_empty());
    }

    #[test]
    fn only_newer_same_namespace_compacted_tombstone_fences_live_recovery() {
        let mut historical_live = object(
            object_id("obj_namespace_compacted_non_fence"),
            "/docs/compacted-non-fence.md",
            DfsObjectKind::File,
        );
        historical_live.changed_at_unix_nanos = 30;
        historical_live.version = version("version_namespace_compacted_current");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_compacted_non_fence".to_string(),
            file_system_id: historical_live.file_system_id.clone(),
            objects: vec![historical_live.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let mut older = historical_live.clone();
        older.kind = DfsObjectKind::Tombstone;
        older.changed_at_unix_nanos = 20;
        older.version = version("version_namespace_compacted_older");
        let mut same_generation = historical_live.clone();
        same_generation.kind = DfsObjectKind::Tombstone;
        let mut different_path = historical_live.clone();
        different_path.kind = DfsObjectKind::Tombstone;
        different_path.path = "/archive/compacted-non-fence.md".to_string();
        different_path.changed_at_unix_nanos = 40;
        different_path.version = version("version_namespace_compacted_different_path");
        let mut different_file_system = historical_live.clone();
        different_file_system.kind = DfsObjectKind::Tombstone;
        different_file_system.file_system_id =
            DfsFileSystemId::from_legacy_named("fs_namespace_txn_other")
                .expect("valid filesystem id");
        different_file_system.changed_at_unix_nanos = 40;
        different_file_system.version =
            version("version_namespace_compacted_different_file_system");

        for (label, compacted_tombstone) in [
            ("older", older),
            ("same generation", same_generation),
            ("different path", different_path),
            ("different filesystem", different_file_system),
        ] {
            let mut tables = DfsMetadataTables::default();
            tables.apply(DfsMetadataTableOp::PutCompactedTombstone {
                object: compacted_tombstone,
            });

            assert!(
                !namespace_transaction_recovery_is_complete(&tables, &transaction),
                "a {label} compacted tombstone must not suppress live-object recovery"
            );
            assert!(
                plan_namespace_transaction_recovery_table_ops(&tables, &transaction)
                    .iter()
                    .any(|op| matches!(
                        op,
                        DfsMetadataTableOp::PutObject { object }
                            if object == &historical_live
                    )),
                "a {label} compacted tombstone must be replaced by the authoritative live object"
            );
        }
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
            advisory_lock_states: Vec::new(),
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

    #[test]
    fn recovery_plan_preserves_later_enrichment_of_existing_version() {
        let mut file = object(
            object_id("obj_namespace_enriched_version"),
            "/docs/enriched.md",
            DfsObjectKind::File,
        );
        file.current_locations = vec![node("node_a")];
        let historical_version = version_record(&file);
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_historical_version".to_string(),
            file_system_id: file_system_id(),
            objects: vec![file.clone()],
            versions: vec![historical_version],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut enriched_version = version_record(&file);
        enriched_version.locations.push(node("node_b"));
        let mut tables = DfsMetadataTables::default();
        for op in plan_namespace_transaction_table_ops(&tables, &transaction) {
            tables.apply(op);
        }
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: enriched_version.clone(),
        });

        let ops = plan_namespace_transaction_recovery_table_ops(&tables, &transaction);

        assert!(namespace_transaction_recovery_is_complete(
            &tables,
            &transaction
        ));

        assert!(
            !ops.iter().any(|op| matches!(
                op,
                DfsMetadataTableOp::PutVersion { version }
                    if version.object_id == file.object_id
                        && version.version == file.version
            )),
            "recovery must not replace an existing same-key version with the historical transaction payload"
        );
        assert_eq!(
            tables
                .versions
                .get(&(file.object_id.clone(), file.version.clone())),
            Some(&enriched_version)
        );
    }

    #[test]
    fn transaction_plan_installs_object_before_its_lock_aggregate() {
        let object = object(
            object_id("obj_namespace_lock_move"),
            "/moved/locked.txt",
            DfsObjectKind::File,
        );
        let mut state = DfsAdvisoryLockStateRecord::empty(
            object.file_system_id.clone(),
            object.object_id.clone(),
            7,
        );
        state.authority_rebind_required = true;
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_lock_move".to_string(),
            file_system_id: object.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: Vec::new(),
            advisory_lock_states: vec![state.clone()],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        validate_namespace_transaction_preconditions(&DfsMetadataTables::default(), &transaction)
            .expect("lock migration transaction");
        let ops = plan_namespace_transaction_table_ops(&DfsMetadataTables::default(), &transaction);
        let object_position = ops
            .iter()
            .position(|op| matches!(op, DfsMetadataTableOp::PutObject { object: candidate } if candidate.object_id == object.object_id))
            .expect("object op");
        let state_position = ops
            .iter()
            .position(|op| matches!(op, DfsMetadataTableOp::PutAdvisoryLockState { state: candidate } if candidate == &state))
            .expect("lock state op");
        assert!(object_position < state_position);
    }

    #[test]
    fn migration_can_supersede_lock_orphan_for_a_tombstoned_object() {
        let mut moved_back = object(
            object_id("obj_namespace_lock_move_back"),
            "/returned/locked.txt",
            DfsObjectKind::File,
        );
        let mut tombstone = moved_back.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.path = "/departed/locked.txt".to_string();
        let mut stale_state = DfsAdvisoryLockStateRecord::empty(
            moved_back.file_system_id.clone(),
            moved_back.object_id.clone(),
            3,
        );
        stale_state.authority_rebind_required = true;
        let mut current_state = stale_state.clone();
        current_state.authority_epoch = 9;

        let mut tables = DfsMetadataTables::default();
        tables
            .objects
            .insert(tombstone.object_id.clone(), tombstone);
        tables
            .advisory_lock_states
            .insert(stale_state.object_id.clone(), stale_state);
        moved_back.kind = DfsObjectKind::File;
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_lock_move_back".to_string(),
            file_system_id: moved_back.file_system_id.clone(),
            objects: vec![moved_back],
            versions: Vec::new(),
            advisory_lock_states: vec![current_state.clone()],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        validate_namespace_transaction_preconditions(&tables, &transaction)
            .expect("tombstoned shard may accept the returning lock aggregate");
        let ops = plan_namespace_transaction_table_ops(&tables, &transaction);
        assert!(ops.iter().any(
            |op| matches!(op, DfsMetadataTableOp::PutAdvisoryLockState { state } if state == &current_state)
        ));
    }

    #[test]
    fn recovery_reclaims_older_tombstone_and_stale_lock_aggregate() {
        let mut returned = object(
            object_id("obj_namespace_recovery_lock_return"),
            "/returned/recovery-lock.txt",
            DfsObjectKind::File,
        );
        returned.changed_at_unix_nanos = 20;
        returned.version = version("version_recovery_returned");

        let mut departed = returned.clone();
        departed.path = "/departed/recovery-lock.txt".to_string();
        departed.kind = DfsObjectKind::Tombstone;
        departed.changed_at_unix_nanos = 10;
        departed.version = version("version_recovery_departed");

        let stale_state = DfsAdvisoryLockStateRecord::empty(
            returned.file_system_id.clone(),
            returned.object_id.clone(),
            3,
        );
        let source_state = DfsAdvisoryLockStateRecord::empty(
            returned.file_system_id.clone(),
            returned.object_id.clone(),
            9,
        );
        let (returned_state, _) =
            crate::advisory_lock::prepare_advisory_lock_state_for_migration(&source_state)
                .expect("prepare returned lock aggregate");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_recovery_lock_return".to_string(),
            file_system_id: returned.file_system_id.clone(),
            objects: vec![returned.clone()],
            versions: Vec::new(),
            advisory_lock_states: vec![returned_state.clone()],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut tables = DfsMetadataTables::default();
        for op in object_index_table_ops_with_previous(&tables, &departed, None) {
            tables.apply(op);
        }
        tables
            .advisory_lock_states
            .insert(stale_state.object_id.clone(), stale_state);

        let ops = plan_namespace_transaction_recovery_table_ops(&tables, &transaction);
        for op in ops {
            tables.apply(op);
        }

        assert_eq!(tables.objects.get(&returned.object_id), Some(&returned));
        let returned_path = path_record_for_object(&returned).path;
        assert_eq!(
            tables
                .paths
                .get(&(returned.file_system_id.clone(), returned_path))
                .map(|record| &record.object_id),
            Some(&returned.object_id)
        );
        assert_eq!(
            tables.advisory_lock_states.get(&returned.object_id),
            Some(&returned_state),
            "an interrupted newer reclaim must replace the stale source-shard lock aggregate"
        );
    }

    #[test]
    fn recovery_preserves_later_tombstone_and_lock_transition() {
        let mut transaction_object = object(
            object_id("obj_namespace_recovery_later_state"),
            "/returned/later-state.txt",
            DfsObjectKind::File,
        );
        transaction_object.changed_at_unix_nanos = 20;
        transaction_object.version = version("version_recovery_live");
        let source_state = DfsAdvisoryLockStateRecord::empty(
            transaction_object.file_system_id.clone(),
            transaction_object.object_id.clone(),
            9,
        );
        let (transaction_state, _) =
            crate::advisory_lock::prepare_advisory_lock_state_for_migration(&source_state)
                .expect("prepare transaction lock aggregate");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_recovery_later_state".to_string(),
            file_system_id: transaction_object.file_system_id.clone(),
            objects: vec![transaction_object.clone()],
            versions: Vec::new(),
            advisory_lock_states: vec![transaction_state.clone()],
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let mut later_state = transaction_state.clone();
        later_state.authority_epoch = 40;
        later_state.authority_rebind_required = false;
        later_state.next_queue_sequence = later_state.next_queue_sequence.saturating_add(1);
        let mut tables = DfsMetadataTables::default();
        for op in plan_namespace_transaction_table_ops(&tables, &transaction) {
            tables.apply(op);
        }
        tables
            .advisory_lock_states
            .insert(later_state.object_id.clone(), later_state.clone());

        let ops = plan_namespace_transaction_recovery_table_ops(&tables, &transaction);
        assert!(namespace_transaction_recovery_is_complete(
            &tables,
            &transaction
        ));

        assert!(!ops.iter().any(|op| matches!(
            op,
            DfsMetadataTableOp::PutAdvisoryLockState { state }
                if state.object_id == transaction_object.object_id
        )));
        assert_eq!(
            tables
                .advisory_lock_states
                .get(&transaction_object.object_id),
            Some(&later_state),
            "recovery must not roll a descendant lock transition back to its migration payload"
        );

        let mut later_tombstone = transaction_object.clone();
        later_tombstone.kind = DfsObjectKind::Tombstone;
        later_tombstone.changed_at_unix_nanos = 30;
        later_tombstone.version = version("version_recovery_later_tombstone");
        for op in object_index_table_ops_with_previous(
            &tables,
            &later_tombstone,
            tables.objects.get(&later_tombstone.object_id),
        ) {
            tables.apply(op);
        }

        let ops = plan_namespace_transaction_recovery_table_ops(&tables, &transaction);
        assert!(namespace_transaction_recovery_is_complete(
            &tables,
            &transaction
        ));
        for op in ops {
            tables.apply(op);
        }

        assert_eq!(
            tables.objects.get(&later_tombstone.object_id),
            Some(&later_tombstone),
            "recovery must preserve a tombstone that superseded the historical transaction"
        );
        assert_eq!(
            tables
                .advisory_lock_states
                .get(&transaction_object.object_id),
            Some(&later_state)
        );
    }
}
