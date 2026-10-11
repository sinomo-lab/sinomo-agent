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
use crate::metadata_object_index::object_index_table_ops_with_previous;
use crate::metadata_pages::child_objects_for_directory_from_tables;
use crate::metadata_records::{
    DfsInodeId, DfsMetadataTableOp, DfsNamespaceTransaction, DfsNamespaceTransactionReceipt,
    normalized_version_record,
};
use crate::metadata_tables::{DfsMetadataTableRollback, DfsMetadataTables};
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata};

type DirectoryOverlayKey = (DfsFileSystemId, String);
type DirectoryParentKey = (DfsFileSystemId, DfsInodeId);
type VisiblePathKey = (DfsFileSystemId, String);

fn transaction_receipt_op(transaction: &DfsNamespaceTransaction) -> DfsMetadataTableOp {
    DfsMetadataTableOp::PutNamespaceTransactionReceipt {
        receipt: DfsNamespaceTransactionReceipt::from_transaction(
            transaction,
            transaction.created_at,
        )
        .expect("validated current transaction has a canonical receipt"),
    }
}

struct NamespaceTransactionRecord {
    op: DfsMetadataTableOp,
    committed_at: time::OffsetDateTime,
}

impl From<DfsMetadataTableOp> for NamespaceTransactionRecord {
    fn from(op: DfsMetadataTableOp) -> Self {
        let committed_at = match &op {
            DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt } => receipt.retained_at,
            _ => unreachable!("namespace materialization requires a transaction or receipt"),
        };
        Self { op, committed_at }
    }
}

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
    transaction
        .validate_content_reference_annotation()
        .map_err(MetadataBackendError::Serde)?;
    let recorded = if let Some(receipt) = tables
        .namespace_transaction_receipts
        .get(&transaction.transaction_id)
    {
        receipt
            .matches_transaction(transaction)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
    } else {
        false
    };
    if !recorded {
        if transaction.extension_expectations.len() > 4096 {
            return Err(MetadataBackendError::Serde(
                "revision expectation batch exceeds 4096 rows".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        for expectation in &transaction.extension_expectations {
            if !identities.insert((&expectation.object_id, &expectation.version)) {
                return Err(MetadataBackendError::Serde(
                    "duplicate revision metadata expectation".into(),
                ));
            }
            if expectation.expected.as_ref().is_some_and(|expected| {
                expected.file_system_id != transaction.file_system_id
                    || expected.object_id != expectation.object_id
                    || expected.version != expectation.version
            }) {
                return Err(MetadataBackendError::Serde(
                    "revision metadata expectation has foreign identity".into(),
                ));
            }
            if !transaction.versions.iter().any(|version| {
                version.object_id == expectation.object_id && version.version == expectation.version
            }) {
                return Err(MetadataBackendError::Serde(
                    "revision expectation has no matching update".into(),
                ));
            }
            let current = tables
                .versions
                .get(&(expectation.object_id.clone(), expectation.version.clone()));
            if current != expectation.expected.as_deref() {
                let changed_fields: Vec<&str> = match (current, expectation.expected.as_deref()) {
                    (Some(current), Some(expected)) => [
                        (
                            "file_system_id",
                            current.file_system_id != expected.file_system_id,
                        ),
                        ("object_id", current.object_id != expected.object_id),
                        ("version", current.version != expected.version),
                        (
                            "content_hash",
                            current.content_hash != expected.content_hash,
                        ),
                        ("size_bytes", current.size_bytes != expected.size_bytes),
                        ("locations", current.locations != expected.locations),
                        (
                            "created_by_event_id",
                            current.created_by_event_id != expected.created_by_event_id,
                        ),
                        ("created_at", current.created_at != expected.created_at),
                        ("visible", current.visible != expected.visible),
                        (
                            "content_durability",
                            current.content_durability != expected.content_durability,
                        ),
                        ("retention", current.retention != expected.retention),
                        (
                            "module_metadata",
                            current.module_metadata != expected.module_metadata,
                        ),
                    ]
                    .into_iter()
                    .filter_map(|(field, changed)| changed.then_some(field))
                    .collect(),
                    _ => vec!["presence"],
                };
                tracing::warn!(
                    target: "sinomo.dfs.metadata",
                    transaction_id = %transaction.transaction_id,
                    object_id = %expectation.object_id,
                    version = %expectation.version,
                    changed_fields = ?changed_fields,
                    "revision metadata compare rejected publication"
                );
                return Err(MetadataBackendError::Serde(
                    "revision metadata changed before publication".into(),
                ));
            }
        }
    }
    crate::file_history::validate_transaction_file_pins(tables, transaction)?;
    for object in &transaction.objects {
        crate::file_history::validate_file_history_transition(tables, object)?;
    }
    for version in &transaction.versions {
        crate::file_history::validate_file_version_record(version)?;
        crate::file_history::validate_retained_version_update(
            tables
                .versions
                .get(&(version.object_id.clone(), version.version.clone())),
            version,
        )?;
    }
    if let Some(receipt) = tables
        .namespace_transaction_receipts
        .get(&transaction.transaction_id)
        && !receipt
            .matches_transaction(transaction)
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
    {
        return Err(MetadataBackendError::Serde(format!(
            "namespace transaction {} reuses transaction id with a different payload",
            transaction.transaction_id
        )));
    }
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
    plan_namespace_transaction_table_ops_at(tables, transaction, transaction.created_at)
}

pub(crate) fn plan_namespace_transaction_table_ops_at(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    committed_at: time::OffsetDateTime,
) -> Vec<DfsMetadataTableOp> {
    let record = NamespaceTransactionRecord {
        op: transaction_receipt_op(transaction),
        committed_at,
    };
    if transaction.objects.len() == 1 {
        return plan_single_object_namespace_transaction_table_ops_with_record(
            tables,
            transaction,
            record,
        );
    }
    let mut scratch = tables.clone();
    let mut table_ops = Vec::new();
    let mut table_op_sink = Some(&mut table_ops);
    let mut rollback = None;
    plan_namespace_transaction_table_ops_in_place_with_record(
        &mut scratch,
        transaction,
        record,
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
#[cfg(test)]
pub(crate) fn preapply_namespace_transaction_table_ops(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    rollback: &mut DfsMetadataTableRollback,
) {
    preapply_namespace_transaction_table_ops_at(
        tables,
        transaction,
        transaction.created_at,
        table_ops,
        rollback,
    );
}

#[cfg(test)]
pub(crate) fn preapply_namespace_transaction_table_ops_at(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    committed_at: time::OffsetDateTime,
    table_ops: &mut Vec<DfsMetadataTableOp>,
    rollback: &mut DfsMetadataTableRollback,
) {
    let mut table_op_sink = Some(table_ops);
    let mut rollback = Some(rollback);
    plan_namespace_transaction_table_ops_in_place_with_record(
        tables,
        transaction,
        NamespaceTransactionRecord {
            op: transaction_receipt_op(transaction),
            committed_at,
        },
        &mut table_op_sink,
        &mut rollback,
    );
}

#[cfg(test)]
fn materialize_namespace_transaction_table_state(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) {
    materialize_namespace_transaction_table_state_with_record(
        tables,
        transaction,
        transaction_receipt_op(transaction),
    );
}

#[cfg(test)]
fn materialize_namespace_transaction_table_state_with_record(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    transaction_record: impl Into<NamespaceTransactionRecord>,
) {
    // The absent sinks select owned direct application without collecting
    // derived operations or a rollback image.
    plan_namespace_transaction_table_ops_in_place_with_record(
        tables,
        transaction,
        transaction_record,
        &mut None,
        &mut None,
    );
}

fn plan_namespace_transaction_table_ops_in_place(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
) {
    plan_namespace_transaction_table_ops_in_place_with_record(
        tables,
        transaction,
        transaction_receipt_op(transaction),
        table_ops,
        rollback,
    );
}

fn plan_namespace_transaction_table_ops_in_place_with_record(
    tables: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    transaction_record: impl Into<NamespaceTransactionRecord>,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
) {
    let transaction_record = transaction_record.into();
    if transaction.objects.len() == 1 {
        for op in plan_single_object_namespace_transaction_table_ops_with_record(
            tables,
            transaction,
            transaction_record,
        ) {
            apply_namespace_transaction_planned_op(tables, table_ops, rollback, op);
        }
        return;
    }
    plan_multi_object_namespace_transaction_table_ops_in_place(
        tables,
        transaction,
        transaction_record,
        table_ops,
        rollback,
    );
}

fn plan_multi_object_namespace_transaction_table_ops_in_place(
    scratch: &mut DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    transaction_record: impl Into<NamespaceTransactionRecord>,
    table_ops: &mut Option<&mut Vec<DfsMetadataTableOp>>,
    rollback: &mut Option<&mut DfsMetadataTableRollback>,
) {
    let transaction_record = transaction_record.into();
    let committed_at = transaction_record.committed_at;
    apply_namespace_transaction_planned_op(scratch, table_ops, rollback, transaction_record.op);
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        if scratch.compacted_tombstone_matches(object) {
            continue;
        }
        plan_object_index_ops(scratch, table_ops, rollback, object, committed_at);
    }
    for object in transaction
        .objects
        .iter()
        .filter(|object| object.kind != DfsObjectKind::Tombstone)
    {
        if scratch.compacted_tombstone_matches(object) {
            continue;
        }
        plan_object_index_ops(scratch, table_ops, rollback, object, committed_at);
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
        if let Some(owner) = crate::file_history::capture_repair_owner_table_op(scratch, version) {
            apply_namespace_transaction_planned_op(scratch, table_ops, rollback, owner);
        }
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
    for op in crate::file_history::transaction_file_pin_ops(transaction) {
        apply_namespace_transaction_planned_op(scratch, table_ops, rollback, op);
    }
}

#[cfg(test)]
fn plan_single_object_namespace_transaction_table_ops(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
) -> Vec<DfsMetadataTableOp> {
    plan_single_object_namespace_transaction_table_ops_with_record(
        tables,
        transaction,
        transaction_receipt_op(transaction),
    )
}

fn plan_single_object_namespace_transaction_table_ops_with_record(
    tables: &DfsMetadataTables,
    transaction: &DfsNamespaceTransaction,
    transaction_record: impl Into<NamespaceTransactionRecord>,
) -> Vec<DfsMetadataTableOp> {
    let transaction_record = transaction_record.into();
    let mut table_ops = Vec::new();
    let committed_at = transaction_record.committed_at;
    table_ops.push(transaction_record.op);
    if let Some(object) = transaction.objects.first()
        && !tables.compacted_tombstone_matches(object)
    {
        table_ops.extend(crate::metadata_object_index::object_index_table_ops_at(
            tables,
            object,
            tables.objects.get(&object.object_id),
            committed_at,
        ));
    }
    for version in &transaction.versions {
        table_ops.push(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(version),
        });
        if let Some(owner) = crate::file_history::capture_repair_owner_table_op(tables, version) {
            table_ops.push(owner);
        }
    }
    for state in &transaction.advisory_lock_states {
        table_ops.push(DfsMetadataTableOp::PutAdvisoryLockState {
            state: state.clone(),
        });
    }
    table_ops.extend(crate::file_history::transaction_file_pin_ops(transaction));
    table_ops
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
    committed_at: time::OffsetDateTime,
) {
    for op in
        crate::metadata_object_index::object_index_table_ops_at(scratch, object, None, committed_at)
    {
        apply_namespace_transaction_planned_op(scratch, table_ops, rollback, op);
    }
}

#[cfg(test)]
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
            content_durability: None,
            retention: Default::default(),
            module_metadata: Default::default(),
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
            transaction_receipt_op(transaction),
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            Some(DfsMetadataTableOp::PutNamespaceTransactionReceipt { .. })
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
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
}
