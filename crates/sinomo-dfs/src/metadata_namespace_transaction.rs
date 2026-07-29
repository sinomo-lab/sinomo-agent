//! Namespace transaction table planning.
//!
//! Backends own journal writes, locks, and durable table appends. This module
//! owns the pure materialization decisions for namespace transactions so normal
//! apply, replay, and recovery all use the same table-operation planner.

use std::collections::{BTreeMap, BTreeSet};

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_keys::{
    dentry_key, directory_inode_id, normalize_path, normalize_posix_path, parent_inode_id,
    path_record_for_object,
};
use crate::metadata_object_index::{
    object_index_table_ops_with_previous, object_indexes_are_materialized,
};
use crate::metadata_pages::child_objects_for_directory_from_tables;
use crate::metadata_records::{
    DfsInodeId, DfsMetadataTableOp, DfsNamespaceTransaction, normalized_version_record,
};
use crate::metadata_tables::{DfsMetadataTableRollback, DfsMetadataTables};
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata};

type DirectoryOverlayKey = (DfsFileSystemId, String);
type DirectoryParentKey = (DfsFileSystemId, DfsInodeId);
type VisiblePathKey = (DfsFileSystemId, String);

/// Sparse materialized view used only to decide whether directory tombstones
/// leave direct children behind.
///
/// The former validator cloned every metadata table and replayed the complete
/// namespace transaction into that clone. This view contains only the direct
/// children of directories actually being tombstoned. The two reverse indexes
/// preserve the object-replacement and same-path-replacement semantics of the
/// object-index planner while transaction objects are replayed.
#[derive(Default)]
struct DirectoryTombstoneOverlay {
    children_by_directory: BTreeMap<DirectoryOverlayKey, BTreeSet<DfsObjectId>>,
    directories_by_parent: BTreeMap<DirectoryParentKey, Vec<DirectoryOverlayKey>>,
    directories_by_object: BTreeMap<DfsObjectId, BTreeSet<DirectoryOverlayKey>>,
    path_by_object: BTreeMap<DfsObjectId, VisiblePathKey>,
    object_by_path: BTreeMap<VisiblePathKey, DfsObjectId>,
}

impl DirectoryTombstoneOverlay {
    fn add_candidate(&mut self, file_system_id: &DfsFileSystemId, path: &str) {
        let normalized_path = normalize_posix_path(path);
        let directory_key = (file_system_id.clone(), normalized_path.clone());
        if self.children_by_directory.contains_key(&directory_key) {
            return;
        }
        self.children_by_directory
            .insert(directory_key.clone(), BTreeSet::new());
        self.directories_by_parent
            .entry((
                file_system_id.clone(),
                directory_inode_id(file_system_id, &normalized_path),
            ))
            .or_default()
            .push(directory_key);
    }

    fn seed_child(&mut self, directory_key: &DirectoryOverlayKey, object: &DfsObjectMetadata) {
        self.children_by_directory
            .get_mut(directory_key)
            .expect("candidate directory is registered before its children")
            .insert(object.object_id.clone());
        self.directories_by_object
            .entry(object.object_id.clone())
            .or_default()
            .insert(directory_key.clone());
        let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
        self.path_by_object
            .insert(object.object_id.clone(), path_key.clone());
        self.object_by_path
            .insert(path_key, object.object_id.clone());
    }

    fn remove_object_placements(&mut self, object_id: &DfsObjectId) {
        let Some(directory_keys) = self.directories_by_object.remove(object_id) else {
            return;
        };
        for directory_key in directory_keys {
            if let Some(children) = self.children_by_directory.get_mut(&directory_key) {
                children.remove(object_id);
            }
        }
    }

    fn remove_object(&mut self, object_id: &DfsObjectId) {
        self.remove_object_placements(object_id);
        let Some(path_key) = self.path_by_object.remove(object_id) else {
            return;
        };
        if self.object_by_path.get(&path_key) == Some(object_id) {
            self.object_by_path.remove(&path_key);
        }
    }

    fn remove_path(&mut self, path_key: &VisiblePathKey) {
        let Some(object_id) = self.object_by_path.remove(path_key) else {
            return;
        };
        if self.path_by_object.get(&object_id) == Some(path_key) {
            self.path_by_object.remove(&object_id);
        }
        self.remove_object_placements(&object_id);
    }

    fn apply_object(&mut self, object: &DfsObjectMetadata) {
        self.remove_object(&object.object_id);
        let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
        self.remove_path(&path_key);
        if object.kind == DfsObjectKind::Tombstone {
            return;
        }

        let parent_key = (
            object.file_system_id.clone(),
            parent_inode_id(&object.file_system_id, &path_key.1),
        );
        let Some(directory_keys) = self.directories_by_parent.get(&parent_key).cloned() else {
            return;
        };
        let object_posix_path = normalize_posix_path(&object.path);
        let mut placements = BTreeSet::new();
        for directory_key in directory_keys {
            // This exclusion mirrors the materialized dentry scan. It matters
            // only for a directory-inode hash collision, but is cheap to keep
            // exact here.
            if directory_key.1 == object_posix_path {
                continue;
            }
            self.children_by_directory
                .get_mut(&directory_key)
                .expect("parent index contains a registered candidate")
                .insert(object.object_id.clone());
            placements.insert(directory_key);
        }
        if placements.is_empty() {
            return;
        }
        self.directories_by_object
            .insert(object.object_id.clone(), placements);
        self.path_by_object
            .insert(object.object_id.clone(), path_key.clone());
        self.object_by_path
            .insert(path_key, object.object_id.clone());
    }

    fn directory_has_children(&self, file_system_id: &DfsFileSystemId, path: &str) -> bool {
        self.children_by_directory
            .get(&(file_system_id.clone(), normalize_posix_path(path)))
            .is_some_and(|children| !children.is_empty())
    }
}

pub(crate) fn validate_namespace_transaction_preconditions(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Result<()> {
    validate_advisory_lock_state_preconditions(tables, transaction)?;
    if !transaction
        .objects
        .iter()
        .any(|object| object.kind == DfsObjectKind::Tombstone)
    {
        return Ok(());
    }
    validate_directory_tombstones_with_sparse_overlay(tables, transaction)
}

fn validate_directory_tombstones_with_sparse_overlay(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Result<()> {
    if transaction
        .objects
        .iter()
        .any(|object| canonical_posix_path(&object.path).is_none())
    {
        return validate_directory_tombstones_with_clone_replay(tables, transaction);
    }
    let mut overlay = DirectoryTombstoneOverlay::default();
    for object in transaction
        .objects
        .iter()
        .filter(|object| directory_tombstone_matches_current(tables, object))
    {
        overlay.add_candidate(&object.file_system_id, &object.path);
    }
    if overlay.children_by_directory.is_empty() {
        return Ok(());
    }
    if overlay
        .directories_by_parent
        .values()
        .any(|directories| directories.len() > 1)
    {
        return validate_directory_tombstones_with_clone_replay(tables, transaction);
    }

    let candidates = overlay
        .children_by_directory
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for directory_key in candidates {
        let children =
            child_objects_for_directory_from_tables(tables, &directory_key.0, &directory_key.1);
        for child in children {
            if canonical_parent_posix_path(&child.path).as_deref() != Some(directory_key.1.as_str())
            {
                return validate_directory_tombstones_with_clone_replay(tables, transaction);
            }
            overlay.seed_child(&directory_key, &child);
        }
    }

    if transaction
        .objects
        .iter()
        .any(|object| object_path_may_alias_candidate_dentry(&overlay, object))
    {
        return validate_directory_tombstones_with_clone_replay(tables, transaction);
    }

    // A preceding object update can remove the compacted-tombstone row that
    // would otherwise make a later tombstone a no-op. Track those removals so
    // the overlay observes the same planner order without cloning either map.
    let mut removed_compacted_tombstone_ids = BTreeSet::new();
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        if compacted_tombstone_matches_overlay(tables, &removed_compacted_tombstone_ids, object) {
            continue;
        }
        overlay.apply_object(object);
        record_compacted_tombstone_path_removal(
            tables,
            &mut removed_compacted_tombstone_ids,
            object,
        );
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        overlay.apply_object(object);
        record_compacted_tombstone_path_removal(
            tables,
            &mut removed_compacted_tombstone_ids,
            object,
        );
    }

    for object in transaction
        .objects
        .iter()
        .filter(|object| directory_tombstone_matches_current(tables, object))
    {
        if overlay.directory_has_children(&object.file_system_id, &object.path) {
            return Err(MetadataBackendError::DirectoryNotEmpty {
                path: object.path.clone(),
            });
        }
    }
    Ok(())
}

fn canonical_posix_path(path: &str) -> Option<String> {
    crate::posix_path::normalize_posix_path(path)
        .ok()
        .filter(|normalized| normalized == path)
}

fn canonical_parent_posix_path(path: &str) -> Option<String> {
    let canonical = canonical_posix_path(path)?;
    crate::posix_path::parent_posix_path(&canonical)
}

fn object_path_may_alias_candidate_dentry(
    overlay: &DirectoryTombstoneOverlay,
    object: &DfsObjectMetadata,
) -> bool {
    let Some(parent_path) = canonical_parent_posix_path(&object.path) else {
        return false;
    };
    let parent_key = (
        object.file_system_id.clone(),
        parent_inode_id(&object.file_system_id, &normalize_path(&object.path)),
    );
    overlay
        .directories_by_parent
        .get(&parent_key)
        .is_some_and(|directories| {
            directories
                .iter()
                .any(|directory| directory.1 != parent_path)
        })
}

fn validate_directory_tombstones_with_clone_replay(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Result<()> {
    let mut scratch = tables.clone();
    let mut table_ops = Vec::new();
    let mut table_op_sink = Some(&mut table_ops);
    let mut rollback = None;
    plan_namespace_transaction_table_ops_in_place(
        &mut scratch,
        transaction,
        &mut table_op_sink,
        &mut rollback,
    );
    for object in &transaction.objects {
        validate_directory_tombstone_has_no_children(tables, &scratch, object)?;
    }
    Ok(())
}

fn directory_tombstone_matches_current(
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) -> bool {
    if object.kind != DfsObjectKind::Tombstone {
        return false;
    }
    tables
        .objects
        .get(&object.object_id)
        .is_some_and(|current| {
            current.kind == DfsObjectKind::Directory && current.path == object.path
        })
}

fn compacted_tombstone_matches_overlay(
    tables: &DfsMetadataTables,
    removed_compacted_tombstone_ids: &BTreeSet<DfsObjectId>,
    object: &DfsObjectMetadata,
) -> bool {
    !removed_compacted_tombstone_ids.contains(&object.object_id)
        && tables.compacted_tombstone_matches(object)
}

fn record_compacted_tombstone_path_removal(
    tables: &DfsMetadataTables,
    removed_compacted_tombstone_ids: &mut BTreeSet<DfsObjectId>,
    object: &DfsObjectMetadata,
) {
    let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
    if let Some(object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        removed_compacted_tombstone_ids.insert(object_id.clone());
    }
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
    let mut table_op_sink = Some(&mut table_ops);
    let mut rollback = None;
    plan_namespace_transaction_table_ops_in_place(
        &mut scratch,
        transaction,
        &mut table_op_sink,
        &mut rollback,
    );
    table_ops
}

/// Plan a namespace transaction directly against the caller's table state.
///
/// The durable backend uses this after its journal append. Each generated op is
/// applied immediately so the next object observes the same state as the
/// reference scratch-table planner. `rollback` captures the original value for
/// every touched key before the first mutation, allowing a failed table-batch
/// append to restore the complete pre-plan state.
pub(crate) fn preapply_namespace_transaction_table_ops(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    rollback: &mut DfsMetadataTableRollback,
) {
    let mut table_op_sink = Some(table_ops);
    let mut rollback = Some(rollback);
    plan_namespace_transaction_table_ops_in_place(
        tables,
        transaction,
        &mut table_op_sink,
        &mut rollback,
    );
}

/// Preapply a transaction while retaining rollback state but not its derived
/// operation list.
///
/// Compact semantic table commits persist only their fenced commit marker. The
/// derived operations are needed to update the table image and rollback maps,
/// but collecting a second copy can contain tens of thousands of operations
/// for a bulk transaction.
pub(crate) fn preapply_namespace_transaction_table_state_with_rollback(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    rollback: &mut DfsMetadataTableRollback,
) {
    let mut table_op_sink = None;
    let mut rollback = Some(rollback);
    plan_namespace_transaction_table_ops_in_place(
        tables,
        transaction,
        &mut table_op_sink,
        &mut rollback,
    );
}

/// Materialize a transaction directly into an already-authorized table image.
///
/// A committed namespace prepare is itself the durable semantic body. Its
/// materialized table log therefore records one compact commit marker instead
/// of serializing every derived object/path/inode/version operation again.
/// Table replay resolves that marker against the preceding durable prepare and
/// calls this function. The decisions intentionally mirror the normal planner,
/// but no derived operation vector or rollback clone is needed because the
/// compact table marker is durable before it is applied to the in-memory view.
pub(crate) fn materialize_namespace_transaction_table_state(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) {
    if transaction.objects.len() == 1 {
        for op in plan_single_object_namespace_transaction_table_ops(tables, transaction) {
            tables.apply(op);
        }
        return;
    }

    tables.apply(DfsMetadataTableOp::PutNamespaceTransaction {
        transaction: transaction.clone(),
    });
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        if tables.compacted_tombstone_matches(object) {
            continue;
        }
        materialize_object_index_state(tables, object);
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        if tables.compacted_tombstone_matches(object) {
            continue;
        }
        materialize_object_index_state(tables, object);
    }
    for version in &transaction.versions {
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(version),
        });
    }
    for state in &transaction.advisory_lock_states {
        tables.apply(DfsMetadataTableOp::PutAdvisoryLockState {
            state: state.clone(),
        });
    }
}

fn plan_namespace_transaction_table_ops_in_place(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
) {
    if transaction.objects.len() == 1 {
        for op in plan_single_object_namespace_transaction_table_ops(tables, transaction) {
            apply_namespace_transaction_planned_op(tables, table_ops, rollback, op);
        }
        return;
    }
    plan_multi_object_namespace_transaction_table_ops_in_place(
        tables,
        transaction,
        table_ops,
        rollback,
    );
}

fn plan_multi_object_namespace_transaction_table_ops_in_place(
    scratch: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
) {
    apply_namespace_transaction_planned_op(
        scratch,
        table_ops,
        rollback,
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
        plan_object_index_ops(scratch, table_ops, rollback, object);
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        if scratch.compacted_tombstone_matches(object) {
            continue;
        }
        plan_object_index_ops(scratch, table_ops, rollback, object);
    }
    for version in &transaction.versions {
        apply_namespace_transaction_planned_op(
            scratch,
            table_ops,
            rollback,
            DfsMetadataTableOp::PutVersion {
                version: normalized_version_record(version),
            },
        );
    }
    for state in &transaction.advisory_lock_states {
        apply_namespace_transaction_planned_op(
            scratch,
            table_ops,
            rollback,
            DfsMetadataTableOp::PutAdvisoryLockState {
                state: state.clone(),
            },
        );
    }
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
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
    object: &DfsObjectMetadata,
) {
    for op in object_index_table_ops_with_previous(scratch, object, None) {
        apply_namespace_transaction_planned_op(scratch, table_ops, rollback, op);
    }
}

fn materialize_object_index_state(tables: &mut DfsMetadataTables, object: &DfsObjectMetadata) {
    for op in object_index_table_ops_with_previous(tables, object, None) {
        tables.apply(op);
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

fn apply_namespace_transaction_planned_op(
    tables: &mut DfsMetadataTables,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
    op: DfsMetadataTableOp,
) {
    if table_ops.is_none() {
        if let Some(rollback) = rollback.as_deref_mut() {
            tables.apply_with_rollback(op, rollback);
        } else {
            tables.apply(op);
        }
        return;
    }
    if let Some(rollback) = rollback.as_deref_mut() {
        tables.apply_with_rollback(op.clone(), rollback);
    } else {
        tables.apply(op.clone());
    }
    table_ops
        .as_mut()
        .expect("checked table-operation sink")
        .push(op);
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
            tombstone_provenance: None,
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

    /// Test-only copy of the former clone-and-replay planner. Keep this
    /// independent of `plan_namespace_transaction_table_ops_in_place` so the
    /// differential tests can detect an ordering or state-observation bug in
    /// the optimized planner rather than comparing two wrappers around it.
    fn clone_replay_reference_plan(
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
            for op in object_index_table_ops_with_previous(&scratch, object, None) {
                apply_planned_op(&mut scratch, &mut table_ops, op);
            }
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            if scratch.compacted_tombstone_matches(object) {
                continue;
            }
            for op in object_index_table_ops_with_previous(&scratch, object, None) {
                apply_planned_op(&mut scratch, &mut table_ops, op);
            }
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

    fn materialized_tables(objects: &[DfsObjectMetadata]) -> DfsMetadataTables {
        let mut tables = DfsMetadataTables::default();
        for object in objects {
            for op in object_index_table_ops_with_previous(&tables, object, None) {
                tables.apply(op);
            }
        }
        tables
    }

    fn namespace_transaction(
        transaction_id: &str,
        objects: Vec<DfsObjectMetadata>,
    ) -> DfsNamespaceTransaction {
        DfsNamespaceTransaction {
            transaction_id: transaction_id.to_string(),
            file_system_id: file_system_id(),
            objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn tombstoned(mut object: DfsObjectMetadata, version_value: &str) -> DfsObjectMetadata {
        object.kind = DfsObjectKind::Tombstone;
        object.version = version(version_value);
        object.changed_at_unix_nanos = object.changed_at_unix_nanos.saturating_add(1);
        object
    }

    fn clone_replay_reference_validation(
        tables: &DfsMetadataTables,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<()> {
        validate_advisory_lock_state_preconditions(tables, transaction)?;
        if !transaction
            .objects
            .iter()
            .any(|object| object.kind == DfsObjectKind::Tombstone)
        {
            return Ok(());
        }
        let mut scratch = tables.clone();
        let mut table_ops = Vec::new();
        let mut table_op_sink = Some(&mut table_ops);
        let mut rollback = None;
        plan_namespace_transaction_table_ops_in_place(
            &mut scratch,
            transaction,
            &mut table_op_sink,
            &mut rollback,
        );
        for object in &transaction.objects {
            validate_directory_tombstone_has_no_children(tables, &scratch, object)?;
        }
        Ok(())
    }

    fn assert_sparse_validation_matches_clone_replay(
        tables: &DfsMetadataTables,
        transaction: &DfsNamespaceTransaction,
    ) -> bool {
        let reference = clone_replay_reference_validation(tables, transaction)
            .map_err(|error| error.to_string());
        let sparse = validate_namespace_transaction_preconditions(tables, transaction)
            .map_err(|error| error.to_string());
        assert_eq!(
            sparse, reference,
            "transaction {}",
            transaction.transaction_id
        );
        sparse.is_ok()
    }

    fn assert_preapplied_matches_clone_replay_and_rolls_back(
        before: &DfsMetadataTables,
        transaction: &DfsNamespaceTransaction,
    ) -> DfsMetadataTables {
        let expected_ops = clone_replay_reference_plan(before, transaction);
        let mut expected_tables = before.clone();
        for op in &expected_ops {
            expected_tables.apply(op.clone());
        }

        let mut preapplied_tables = before.clone();
        let mut preapplied_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        preapply_namespace_transaction_table_ops(
            &mut preapplied_tables,
            transaction,
            &mut preapplied_ops,
            &mut rollback,
        );

        assert_eq!(preapplied_ops, expected_ops);
        assert_eq!(preapplied_tables, expected_tables);
        rollback.rollback(&mut preapplied_tables);
        assert_eq!(&preapplied_tables, before);

        let mut directly_materialized = before.clone();
        materialize_namespace_transaction_table_state(&mut directly_materialized, transaction);
        assert_eq!(
            directly_materialized, expected_tables,
            "compact semantic commit materialization must match the expanded reference plan"
        );
        expected_tables
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
    fn preapplied_multi_object_plan_matches_reference_and_rolls_back() {
        let mut parent = object(
            object_id("obj_namespace_create_parent"),
            "/docs",
            DfsObjectKind::Directory,
        );
        let child = object(
            object_id("obj_namespace_create_child"),
            "/docs/child.txt",
            DfsObjectKind::File,
        );
        let mut before = DfsMetadataTables::default();
        for op in object_index_table_ops_with_previous(&before, &parent, None) {
            before.apply(op);
        }
        parent.changed_at_unix_nanos = 2;
        parent.version = version("version_namespace_create_parent_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_preapplied_create".to_string(),
            file_system_id: file_system_id(),
            objects: vec![parent.clone(), child.clone()],
            versions: vec![version_record(&parent), version_record(&child)],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert_preapplied_matches_clone_replay_and_rolls_back(&before, &transaction);
    }

    #[test]
    fn preapplied_parent_then_child_move_matches_clone_replay_and_rolls_back() {
        let parent_id = object_id("obj_namespace_move_parent");
        let child_id = object_id("obj_namespace_move_child");
        let parent = object(parent_id.clone(), "/docs", DfsObjectKind::Directory);
        let child = object(child_id.clone(), "/docs/child.txt", DfsObjectKind::File);
        let mut before = DfsMetadataTables::default();
        for object in [&parent, &child] {
            for op in object_index_table_ops_with_previous(&before, object, None) {
                before.apply(op);
            }
        }

        let mut moved_parent = parent.clone();
        moved_parent.path = "/archive".to_string();
        moved_parent.version = version("version_namespace_move_parent_2");
        let mut moved_child = child.clone();
        moved_child.path = "/archive/child.txt".to_string();
        moved_child.version = version("version_namespace_move_child_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_preapplied_move".to_string(),
            file_system_id: file_system_id(),
            objects: vec![moved_parent.clone(), moved_child.clone()],
            versions: vec![version_record(&moved_parent), version_record(&moved_child)],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let terminal = assert_preapplied_matches_clone_replay_and_rolls_back(&before, &transaction);
        assert_eq!(
            terminal
                .objects
                .get(&parent_id)
                .map(|object| object.path.as_str()),
            Some("/archive")
        );
        assert_eq!(
            terminal
                .objects
                .get(&child_id)
                .map(|object| object.path.as_str()),
            Some("/archive/child.txt")
        );
    }

    #[test]
    fn preapplied_child_then_parent_delete_matches_clone_replay_and_rolls_back() {
        let parent_id = object_id("obj_namespace_delete_parent");
        let child_id = object_id("obj_namespace_delete_child");
        let parent = object(parent_id.clone(), "/tree", DfsObjectKind::Directory);
        let child = object(child_id.clone(), "/tree/child.txt", DfsObjectKind::File);
        let mut before = DfsMetadataTables::default();
        for object in [&parent, &child] {
            for op in object_index_table_ops_with_previous(&before, object, None) {
                before.apply(op);
            }
        }

        let mut deleted_child = child;
        deleted_child.kind = DfsObjectKind::Tombstone;
        deleted_child.version = version("version_namespace_delete_child_2");
        let mut deleted_parent = parent;
        deleted_parent.kind = DfsObjectKind::Tombstone;
        deleted_parent.version = version("version_namespace_delete_parent_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "txn_namespace_preapplied_delete".to_string(),
            file_system_id: file_system_id(),
            objects: vec![deleted_child.clone(), deleted_parent.clone()],
            versions: vec![
                version_record(&deleted_child),
                version_record(&deleted_parent),
            ],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        let terminal = assert_preapplied_matches_clone_replay_and_rolls_back(&before, &transaction);
        assert!(matches!(
            terminal.objects.get(&child_id).map(|object| &object.kind),
            Some(DfsObjectKind::Tombstone)
        ));
        assert!(matches!(
            terminal.objects.get(&parent_id).map(|object| &object.kind),
            Some(DfsObjectKind::Tombstone)
        ));
    }

    #[test]
    fn sparse_directory_tombstone_overlay_matches_clone_replay_witnesses() {
        let parent = object(
            object_id("obj_overlay_parent"),
            "/tree",
            DfsObjectKind::Directory,
        );
        let child = object(
            object_id("obj_overlay_child"),
            "/tree/child.txt",
            DfsObjectKind::File,
        );
        let parent_tombstone = tombstoned(parent.clone(), "version_overlay_parent_deleted");
        let child_tombstone = tombstoned(child.clone(), "version_overlay_child_deleted");
        let base = materialized_tables(&[parent.clone(), child.clone()]);

        let mut moved_out = child.clone();
        moved_out.path = "/archive/child.txt".to_string();
        moved_out.version = version("version_overlay_child_moved_out");
        let mut live_in_place = child.clone();
        live_in_place.version = version("version_overlay_child_live_in_place");

        for (transaction, expected_accept) in [
            (
                namespace_transaction("txn_overlay_parent_only", vec![parent_tombstone.clone()]),
                false,
            ),
            (
                namespace_transaction(
                    "txn_overlay_recursive_delete",
                    vec![child_tombstone.clone(), parent_tombstone.clone()],
                ),
                true,
            ),
            (
                namespace_transaction(
                    "txn_overlay_move_out",
                    vec![moved_out, parent_tombstone.clone()],
                ),
                true,
            ),
            (
                namespace_transaction(
                    "txn_overlay_live_in_place",
                    vec![live_in_place, parent_tombstone.clone()],
                ),
                false,
            ),
        ] {
            assert_eq!(
                assert_sparse_validation_matches_clone_replay(&base, &transaction),
                expected_accept,
                "transaction {}",
                transaction.transaction_id
            );
        }

        let outside_child = object(
            object_id("obj_overlay_outside_child"),
            "/archive/outside.txt",
            DfsObjectKind::File,
        );
        let outside_base = materialized_tables(&[parent.clone(), outside_child.clone()]);
        let mut moved_in = outside_child;
        moved_in.path = "/tree/moved-in.txt".to_string();
        moved_in.version = version("version_overlay_child_moved_in");
        assert!(!assert_sparse_validation_matches_clone_replay(
            &outside_base,
            &namespace_transaction(
                "txn_overlay_move_in",
                vec![moved_in, parent_tombstone.clone()],
            ),
        ));

        let new_child = object(
            object_id("obj_overlay_new_child"),
            "/tree/new.txt",
            DfsObjectKind::File,
        );
        assert!(!assert_sparse_validation_matches_clone_replay(
            &materialized_tables(std::slice::from_ref(&parent)),
            &namespace_transaction(
                "txn_overlay_new_child",
                vec![new_child, parent_tombstone.clone()],
            ),
        ));

        let mut replacement = object(
            object_id("obj_overlay_replacement"),
            "/tree/child.txt",
            DfsObjectKind::File,
        );
        replacement.version = version("version_overlay_replacement");
        assert!(!assert_sparse_validation_matches_clone_replay(
            &base,
            &namespace_transaction(
                "txn_overlay_path_replacement",
                vec![
                    replacement,
                    child_tombstone.clone(),
                    parent_tombstone.clone()
                ],
            ),
        ));

        let live_replacement = object(
            object_id("obj_overlay_live_replacement"),
            "/tree/child.txt",
            DfsObjectKind::File,
        );
        assert!(!assert_sparse_validation_matches_clone_replay(
            &base,
            &namespace_transaction(
                "txn_overlay_live_path_replacement",
                vec![live_replacement, parent_tombstone.clone()],
            ),
        ));

        let mut restored_child = child.clone();
        restored_child.version = version("version_overlay_child_restored");
        assert!(!assert_sparse_validation_matches_clone_replay(
            &base,
            &namespace_transaction(
                "txn_overlay_duplicate_id_restored",
                vec![
                    child_tombstone.clone(),
                    restored_child.clone(),
                    parent_tombstone.clone(),
                ],
            ),
        ));
        let mut restored_then_moved = restored_child;
        restored_then_moved.path = "/archive/restored.txt".to_string();
        restored_then_moved.version = version("version_overlay_child_restored_then_moved");
        assert!(assert_sparse_validation_matches_clone_replay(
            &base,
            &namespace_transaction(
                "txn_overlay_repeated_live_last_wins",
                vec![child.clone(), restored_then_moved, parent_tombstone.clone(),],
            ),
        ));

        for (index, kind) in [
            DfsObjectKind::File,
            DfsObjectKind::Directory,
            DfsObjectKind::Symlink,
            DfsObjectKind::Hardlink,
            DfsObjectKind::Artifact,
            DfsObjectKind::Mount,
        ]
        .into_iter()
        .enumerate()
        {
            let candidate = object(
                object_id(&format!("obj_overlay_kind_{index}")),
                &format!("/tree/kind-{index}"),
                kind,
            );
            assert!(!assert_sparse_validation_matches_clone_replay(
                &materialized_tables(std::slice::from_ref(&parent)),
                &namespace_transaction(
                    &format!("txn_overlay_kind_{index}"),
                    vec![candidate, parent_tombstone.clone()],
                ),
            ));
        }

        let mut compacted_base = base.clone();
        compacted_base.apply(DfsMetadataTableOp::PutCompactedTombstone {
            object: child_tombstone.clone(),
        });
        assert!(!assert_sparse_validation_matches_clone_replay(
            &compacted_base,
            &namespace_transaction(
                "txn_overlay_compacted_child_tombstone",
                vec![child_tombstone.clone(), parent_tombstone.clone()],
            ),
        ));

        let mut displaced_tombstone = child_tombstone;
        displaced_tombstone.path = "/archive/compacted-child.txt".to_string();
        displaced_tombstone.version = version("version_overlay_displaced_tombstone");
        let mut marker_cleared_base = base.clone();
        marker_cleared_base.apply(DfsMetadataTableOp::PutCompactedTombstone {
            object: displaced_tombstone.clone(),
        });
        let clearing_tombstone = tombstoned(
            object(
                object_id("obj_overlay_compacted_marker_clear"),
                "/archive/compacted-child.txt",
                DfsObjectKind::File,
            ),
            "version_overlay_compacted_marker_clear",
        );
        assert!(assert_sparse_validation_matches_clone_replay(
            &marker_cleared_base,
            &namespace_transaction(
                "txn_overlay_compacted_marker_removed_before_match",
                vec![clearing_tombstone, displaced_tombstone, parent_tombstone],
            ),
        ));
    }

    #[test]
    fn sparse_directory_tombstone_overlay_handles_path_reuse_and_multiple_directories() {
        let parent_a = object(
            object_id("obj_overlay_parent_a"),
            "/a",
            DfsObjectKind::Directory,
        );
        let parent_b = object(
            object_id("obj_overlay_parent_b"),
            "/b",
            DfsObjectKind::Directory,
        );
        let child_a = object(
            object_id("obj_overlay_child_a"),
            "/a/child.txt",
            DfsObjectKind::File,
        );
        let base = materialized_tables(&[parent_a.clone(), parent_b.clone(), child_a.clone()]);
        let different_id_tombstone = tombstoned(
            object(
                object_id("obj_overlay_path_tombstone"),
                "/a/child.txt",
                DfsObjectKind::File,
            ),
            "version_overlay_path_tombstone",
        );
        let transaction = namespace_transaction(
            "txn_overlay_new_id_path_delete",
            vec![
                different_id_tombstone,
                tombstoned(parent_a.clone(), "version_overlay_parent_a_deleted"),
                tombstoned(parent_b.clone(), "version_overlay_parent_b_deleted"),
            ],
        );

        assert!(assert_sparse_validation_matches_clone_replay(
            &base,
            &transaction
        ));

        let child_b = object(
            object_id("obj_overlay_child_b"),
            "/b/child.txt",
            DfsObjectKind::File,
        );
        let two_nonempty =
            materialized_tables(&[parent_a.clone(), parent_b.clone(), child_a, child_b]);
        let first_error_transaction = namespace_transaction(
            "txn_overlay_multiple_first_error",
            vec![
                tombstoned(parent_b, "version_overlay_parent_b_first_error"),
                tombstoned(parent_a, "version_overlay_parent_a_second_error"),
            ],
        );
        assert_sparse_validation_matches_clone_replay(&two_nonempty, &first_error_transaction);
        assert_eq!(
            validate_namespace_transaction_preconditions(&two_nonempty, &first_error_transaction)
                .expect_err("the first nonempty candidate must reject")
                .to_string(),
            "directory is not empty: /b"
        );

        let alias_parent = object(
            object_id("obj_overlay_alias_parent"),
            "/d",
            DfsObjectKind::Directory,
        );
        let alias_child = object(
            object_id("obj_overlay_alias_child"),
            "/d/x",
            DfsObjectKind::File,
        );
        let alias_base = materialized_tables(&[alias_parent.clone(), alias_child]);
        let noncanonical_alias_tombstone = tombstoned(
            object(
                object_id("obj_overlay_alias_tombstone"),
                "/d//x",
                DfsObjectKind::File,
            ),
            "version_overlay_alias_tombstone",
        );
        let alias_transaction = namespace_transaction(
            "txn_overlay_noncanonical_dentry_alias_fallback",
            vec![
                noncanonical_alias_tombstone,
                tombstoned(alias_parent, "version_overlay_alias_parent_deleted"),
            ],
        );
        assert!(assert_sparse_validation_matches_clone_replay(
            &alias_base,
            &alias_transaction
        ));
    }

    #[test]
    fn sparse_directory_tombstone_validation_is_independent_of_unrelated_objects() {
        let parent = object(
            object_id("obj_overlay_scale_parent"),
            "/scale",
            DfsObjectKind::Directory,
        );
        let child = object(
            object_id("obj_overlay_scale_child"),
            "/scale/child.txt",
            DfsObjectKind::File,
        );
        let mut tables = materialized_tables(&[parent.clone(), child]);
        for index in 0..10_000 {
            let unrelated = object(
                object_id(&format!("obj_overlay_unrelated_{index}")),
                &format!("/unrelated/{index}.txt"),
                DfsObjectKind::File,
            );
            // Only the object map is intentionally large: the sparse validator
            // must not visit or clone it to decide one directory's child set.
            tables
                .objects
                .insert(unrelated.object_id.clone(), unrelated);
        }
        let transaction = namespace_transaction(
            "txn_overlay_scale_parent_delete",
            vec![tombstoned(parent, "version_overlay_scale_parent_deleted")],
        );

        assert!(matches!(
            validate_namespace_transaction_preconditions(&tables, &transaction),
            Err(MetadataBackendError::DirectoryNotEmpty { .. })
        ));
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
