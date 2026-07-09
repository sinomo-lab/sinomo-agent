//! In-memory materialized metadata tables.
//!
//! Durable table IO lives in `metadata_table_store`; this module owns the
//! table maps, secondary indexes, and rollback bookkeeping used while
//! materializing journal entries.

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use sinomo_core::node::NodeId;
use time::OffsetDateTime;

use crate::metadata_keys::{
    dentry_key, intent_key, normalize_path, write_intent_scan_key, write_intent_target_key,
};
use crate::metadata_records::{
    DfsConflictRecord, DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeId, DfsInodeRecord,
    DfsMetadataTableOp, DfsNamespaceJobRecord, DfsNamespaceTransaction, DfsObjectVersionRecord,
    DfsPathRecord, DfsReplicaReceiptRecord, DfsVisibleVersionCommitRecord, DfsWriteIntentRecord,
    normalized_object_metadata,
};
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsMetadataTables {
    pub(crate) file_systems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    pub(crate) objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    pub(crate) file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), DfsPathRecord>,
    pub(crate) paths: BTreeMap<(DfsFileSystemId, String), DfsPathRecord>,
    pub(crate) inodes: BTreeMap<DfsInodeId, DfsInodeRecord>,
    pub(crate) dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), DfsDirFragmentRecord>,
    pub(crate) dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), DfsPathRecord>,
    pub(crate) hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), DfsPathRecord>,
    pub(crate) hardlink_paths: BTreeMap<(DfsObjectId, String, DfsObjectId), DfsPathRecord>,
    pub(crate) compacted_tombstones: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    pub(crate) compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), DfsObjectId>,
    pub(crate) versions: BTreeMap<(DfsObjectId, DfsVersion), DfsObjectVersionRecord>,
    pub(crate) receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), DfsReplicaReceiptRecord>,
    pub(crate) intents: BTreeMap<(DfsObjectId, String), DfsWriteIntentRecord>,
    pub(crate) intent_order: BTreeMap<(OffsetDateTime, String, DfsObjectId), (DfsObjectId, String)>,
    pub(crate) intent_targets: BTreeMap<(DfsObjectId, DfsVersion, String), (DfsObjectId, String)>,
    pub(crate) conflicts: BTreeMap<(DfsObjectId, String), DfsConflictRecord>,
    pub(crate) visible_commits: BTreeMap<(DfsObjectId, DfsVersion), DfsVisibleVersionCommitRecord>,
    pub(crate) namespace_transactions: BTreeMap<String, DfsNamespaceTransaction>,
    pub(crate) namespace_jobs: BTreeMap<String, DfsNamespaceJobRecord>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsMetadataTableState {
    pub(crate) tables: DfsMetadataTables,
    pub(crate) table_log_offset: u64,
    pub(crate) materialized_through_sequence: Option<u64>,
    pub(crate) snapshot_format_version: Option<u16>,
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetadataTableSnapshotFingerprint {
    pub(crate) len: u64,
    pub(crate) modified_unix_nanos: Option<u128>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataTableReadSnapshotCache {
    pub(crate) snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    pub(crate) table_log_offset: u64,
    pub(crate) tables: Arc<DfsMetadataTables>,
}

pub(crate) enum DfsMetadataTablesSnapshot<'a> {
    Borrowed(&'a DfsMetadataTables),
    Shared(Arc<DfsMetadataTables>),
}

impl Deref for DfsMetadataTablesSnapshot<'_> {
    type Target = DfsMetadataTables;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(tables) => tables,
            Self::Shared(tables) => tables,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetadataTableReplay {
    pub(crate) offset: u64,
    pub(crate) materialized_through_sequence: Option<u64>,
}

pub(crate) fn max_optional_sequence(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(sequence), None) | (None, Some(sequence)) => Some(sequence),
        (None, None) => None,
    }
}

impl DfsMetadataTables {
    pub(crate) fn has_materialized_state(&self) -> bool {
        !self.file_systems.is_empty()
            || !self.objects.is_empty()
            || !self.versions.is_empty()
            || !self.receipts.is_empty()
            || !self.intents.is_empty()
            || !self.conflicts.is_empty()
            || !self.namespace_transactions.is_empty()
            || !self.namespace_jobs.is_empty()
            || !self.compacted_tombstones.is_empty()
            || !self.visible_commits.is_empty()
    }

    fn put_intent_indexed(&mut self, intent: DfsWriteIntentRecord) {
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(previous) = self.intents.insert(key.clone(), intent.clone()) {
            self.intent_order.remove(&write_intent_scan_key(&previous));
            self.intent_targets
                .remove(&write_intent_target_key(&previous));
        }
        self.intent_order
            .insert(write_intent_scan_key(&intent), key);
        self.intent_targets
            .insert(write_intent_target_key(&intent), intent_key(&intent));
    }

    fn rebuild_intent_indexes(&mut self) {
        self.intent_order.clear();
        self.intent_targets.clear();
        for intent in self.intents.values() {
            let key = intent_key(intent);
            self.intent_order
                .insert(write_intent_scan_key(intent), key.clone());
            self.intent_targets
                .insert(write_intent_target_key(intent), key);
        }
    }

    pub(crate) fn apply(&mut self, op: DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => {
                self.file_systems
                    .insert(file_system.file_system_id.clone(), file_system);
            }
            DfsMetadataTableOp::PutObject { object } => {
                self.objects.insert(object.object_id.clone(), object);
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                self.objects.remove(&object_id);
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => {
                self.file_system_objects.insert(
                    (
                        path_record.file_system_id.clone(),
                        path_record.object_id.clone(),
                    ),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => {
                self.file_system_objects
                    .remove(&(file_system_id, object_id));
            }
            DfsMetadataTableOp::PutPath { path_record } => {
                self.paths.insert(
                    (path_record.file_system_id.clone(), path_record.path.clone()),
                    path_record,
                );
            }
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => {
                self.paths.remove(&(file_system_id, path));
            }
            DfsMetadataTableOp::PutInode { inode } => {
                self.inodes.insert(inode.inode_id.clone(), inode);
            }
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                self.inodes.remove(&inode_id);
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => {
                self.dirfrags.insert(
                    (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
                    dirfrag,
                );
            }
            DfsMetadataTableOp::PutDentry { path_record } => {
                self.dentries.insert(dentry_key(&path_record), path_record);
            }
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => {
                self.dentries
                    .remove(&(parent_inode_id, dirfrag_id, name_hash, name));
            }
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                path_record,
            } => {
                self.put_hardlink_index(target_object_id, hardlink_object_id, path_record);
            }
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => {
                self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
            }
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                self.put_compacted_tombstone(object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                self.remove_compacted_tombstone_path(&file_system_id, &path);
            }
            DfsMetadataTableOp::PutVersion { version } => {
                self.versions.insert(
                    (version.object_id.clone(), version.version.clone()),
                    version,
                );
            }
            DfsMetadataTableOp::PutReceipt { receipt } => {
                self.receipts.insert(
                    (
                        receipt.receipt.object_id.clone(),
                        receipt.receipt.committed_version.clone(),
                        receipt.replica_node_id.clone(),
                    ),
                    receipt,
                );
            }
            DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => {
                self.receipts.remove(&(
                    object_id.clone(),
                    version.clone(),
                    replica_node_id.clone(),
                ));
            }
            DfsMetadataTableOp::PutIntent { intent } => {
                self.put_intent_indexed(intent);
            }
            DfsMetadataTableOp::PutConflict { conflict } => {
                self.conflicts.insert(
                    (conflict.object_id.clone(), conflict.conflict_id.clone()),
                    conflict,
                );
            }
            DfsMetadataTableOp::RemoveConflict {
                object_id,
                conflict_id,
            } => {
                self.conflicts.remove(&(object_id, conflict_id));
            }
            DfsMetadataTableOp::PutVisibleCommit {
                object_id,
                version,
                commit,
            } => {
                self.visible_commits.insert((object_id, version), commit);
            }
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => {
                self.namespace_transactions
                    .insert(transaction.transaction_id.clone(), transaction);
            }
            DfsMetadataTableOp::PutNamespaceJob { job } => {
                self.namespace_jobs.insert(job.job_id.clone(), job);
            }
        }
    }

    pub(crate) fn apply_with_rollback(
        &mut self,
        op: DfsMetadataTableOp,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        rollback.record(self, &op);
        self.apply(op);
    }

    fn put_compacted_tombstone(&mut self, object: DfsObjectMetadata) {
        let object = normalized_object_metadata(&object);
        let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
        if let Some(previous_object_id) = self
            .compacted_tombstone_paths
            .insert(path_key, object.object_id.clone())
            && previous_object_id != object.object_id
        {
            self.compacted_tombstones.remove(&previous_object_id);
        }
        self.compacted_tombstones
            .insert(object.object_id.clone(), object);
    }

    fn remove_compacted_tombstone_path(&mut self, file_system_id: &DfsFileSystemId, path: &str) {
        let path_key = (file_system_id.clone(), normalize_path(path));
        if let Some(object_id) = self.compacted_tombstone_paths.remove(&path_key) {
            self.compacted_tombstones.remove(&object_id);
        }
    }

    pub(crate) fn compacted_tombstone_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Option<DfsObjectMetadata> {
        let path_key = (file_system_id.clone(), normalize_path(path));
        self.compacted_tombstone_paths
            .get(&path_key)
            .and_then(|object_id| self.compacted_tombstones.get(object_id))
            .filter(|object| {
                object.kind == DfsObjectKind::Tombstone
                    && object.file_system_id == *file_system_id
                    && normalize_path(&object.path) == path_key.1
            })
            .cloned()
    }

    pub(crate) fn compacted_tombstone_matches(&self, object: &DfsObjectMetadata) -> bool {
        if object.kind != DfsObjectKind::Tombstone {
            return false;
        }
        self.compacted_tombstones
            .get(&object.object_id)
            .is_some_and(|stored| {
                normalized_object_metadata(stored) == normalized_object_metadata(object)
            })
    }

    fn put_hardlink_index(
        &mut self,
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
        path_record: DfsPathRecord,
    ) {
        self.remove_hardlink_index(&target_object_id, &hardlink_object_id);
        self.hardlink_paths.insert(
            (
                target_object_id.clone(),
                path_record.path.clone(),
                hardlink_object_id.clone(),
            ),
            path_record.clone(),
        );
        self.hardlinks
            .insert((target_object_id, hardlink_object_id), path_record);
    }

    fn remove_hardlink_index(
        &mut self,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) {
        if let Some(path_record) = self
            .hardlinks
            .remove(&(target_object_id.clone(), hardlink_object_id.clone()))
        {
            self.hardlink_paths.remove(&(
                target_object_id.clone(),
                path_record.path,
                hardlink_object_id.clone(),
            ));
        }
    }

    fn rebuild_hardlink_path_index(&mut self) {
        self.hardlink_paths = self
            .hardlinks
            .iter()
            .map(|((target_object_id, hardlink_object_id), path_record)| {
                (
                    (
                        target_object_id.clone(),
                        path_record.path.clone(),
                        hardlink_object_id.clone(),
                    ),
                    path_record.clone(),
                )
            })
            .collect();
    }
}

#[derive(Debug, Default)]
pub(crate) struct DfsMetadataTableRollback {
    file_systems: BTreeMap<DfsFileSystemId, Option<DfsFileSystem>>,
    objects: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    file_system_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), Option<DfsPathRecord>>,
    paths: BTreeMap<(DfsFileSystemId, String), Option<DfsPathRecord>>,
    inodes: BTreeMap<DfsInodeId, Option<DfsInodeRecord>>,
    dirfrags: BTreeMap<(DfsInodeId, DfsDirFragmentId), Option<DfsDirFragmentRecord>>,
    dentries: BTreeMap<(DfsInodeId, DfsDirFragmentId, u64, String), Option<DfsPathRecord>>,
    hardlinks: BTreeMap<(DfsObjectId, DfsObjectId), Option<DfsPathRecord>>,
    compacted_tombstones: BTreeMap<DfsObjectId, Option<DfsObjectMetadata>>,
    compacted_tombstone_paths: BTreeMap<(DfsFileSystemId, String), Option<DfsObjectId>>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsObjectVersionRecord>>,
    receipts: BTreeMap<(DfsObjectId, DfsVersion, NodeId), Option<DfsReplicaReceiptRecord>>,
    intents: BTreeMap<(DfsObjectId, String), Option<DfsWriteIntentRecord>>,
    conflicts: BTreeMap<(DfsObjectId, String), Option<DfsConflictRecord>>,
    visible_commits: BTreeMap<(DfsObjectId, DfsVersion), Option<DfsVisibleVersionCommitRecord>>,
    namespace_transactions: BTreeMap<String, Option<DfsNamespaceTransaction>>,
    namespace_jobs: BTreeMap<String, Option<DfsNamespaceJobRecord>>,
}

impl DfsMetadataTableRollback {
    fn record(&mut self, tables: &DfsMetadataTables, op: &DfsMetadataTableOp) {
        match op {
            DfsMetadataTableOp::PutFileSystem { file_system } => record_rollback_value(
                &mut self.file_systems,
                &tables.file_systems,
                file_system.file_system_id.clone(),
            ),
            DfsMetadataTableOp::PutObject { object } => {
                record_rollback_value(&mut self.objects, &tables.objects, object.object_id.clone())
            }
            DfsMetadataTableOp::RemoveObject { object_id } => {
                record_rollback_value(&mut self.objects, &tables.objects, object_id.clone())
            }
            DfsMetadataTableOp::PutFileSystemObject { path_record } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (
                    path_record.file_system_id.clone(),
                    path_record.object_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id,
                object_id,
            } => record_rollback_value(
                &mut self.file_system_objects,
                &tables.file_system_objects,
                (file_system_id.clone(), object_id.clone()),
            ),
            DfsMetadataTableOp::PutPath { path_record } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (path_record.file_system_id.clone(), path_record.path.clone()),
            ),
            DfsMetadataTableOp::RemovePath {
                file_system_id,
                path,
            } => record_rollback_value(
                &mut self.paths,
                &tables.paths,
                (file_system_id.clone(), path.clone()),
            ),
            DfsMetadataTableOp::PutInode { inode } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode.inode_id.clone())
            }
            DfsMetadataTableOp::RemoveInode { inode_id } => {
                record_rollback_value(&mut self.inodes, &tables.inodes, inode_id.clone())
            }
            DfsMetadataTableOp::PutDirFragment { dirfrag } => record_rollback_value(
                &mut self.dirfrags,
                &tables.dirfrags,
                (dirfrag.dir_inode_id.clone(), dirfrag.dirfrag_id.clone()),
            ),
            DfsMetadataTableOp::PutDentry { path_record } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                dentry_key(path_record),
            ),
            DfsMetadataTableOp::RemoveDentry {
                parent_inode_id,
                dirfrag_id,
                name_hash,
                name,
            } => record_rollback_value(
                &mut self.dentries,
                &tables.dentries,
                (
                    parent_inode_id.clone(),
                    dirfrag_id.clone(),
                    *name_hash,
                    name.clone(),
                ),
            ),
            DfsMetadataTableOp::PutHardlink {
                target_object_id,
                hardlink_object_id,
                ..
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::RemoveHardlink {
                target_object_id,
                hardlink_object_id,
            } => record_rollback_value(
                &mut self.hardlinks,
                &tables.hardlinks,
                (target_object_id.clone(), hardlink_object_id.clone()),
            ),
            DfsMetadataTableOp::PutCompactedTombstone { object } => {
                record_compacted_tombstone_rollback(self, tables, object);
            }
            DfsMetadataTableOp::RemoveCompactedTombstonePath {
                file_system_id,
                path,
            } => {
                record_compacted_tombstone_path_rollback(self, tables, file_system_id, path);
            }
            DfsMetadataTableOp::PutVersion { version } => record_rollback_value(
                &mut self.versions,
                &tables.versions,
                (version.object_id.clone(), version.version.clone()),
            ),
            DfsMetadataTableOp::PutReceipt { receipt } => record_rollback_value(
                &mut self.receipts,
                &tables.receipts,
                (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                ),
            ),
            DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => record_rollback_value(
                &mut self.receipts,
                &tables.receipts,
                (object_id.clone(), version.clone(), replica_node_id.clone()),
            ),
            DfsMetadataTableOp::PutIntent { intent } => record_rollback_value(
                &mut self.intents,
                &tables.intents,
                (intent.object_id.clone(), intent.intent_id.clone()),
            ),
            DfsMetadataTableOp::PutConflict { conflict } => record_rollback_value(
                &mut self.conflicts,
                &tables.conflicts,
                (conflict.object_id.clone(), conflict.conflict_id.clone()),
            ),
            DfsMetadataTableOp::RemoveConflict {
                object_id,
                conflict_id,
            } => record_rollback_value(
                &mut self.conflicts,
                &tables.conflicts,
                (object_id.clone(), conflict_id.clone()),
            ),
            DfsMetadataTableOp::PutVisibleCommit {
                object_id, version, ..
            } => record_rollback_value(
                &mut self.visible_commits,
                &tables.visible_commits,
                (object_id.clone(), version.clone()),
            ),
            DfsMetadataTableOp::PutNamespaceTransaction { transaction } => record_rollback_value(
                &mut self.namespace_transactions,
                &tables.namespace_transactions,
                transaction.transaction_id.clone(),
            ),
            DfsMetadataTableOp::PutNamespaceJob { job } => record_rollback_value(
                &mut self.namespace_jobs,
                &tables.namespace_jobs,
                job.job_id.clone(),
            ),
        }
    }

    pub(crate) fn rollback(self, tables: &mut DfsMetadataTables) {
        rollback_map(&mut tables.file_systems, self.file_systems);
        rollback_map(&mut tables.objects, self.objects);
        rollback_map(&mut tables.file_system_objects, self.file_system_objects);
        rollback_map(&mut tables.paths, self.paths);
        rollback_map(&mut tables.inodes, self.inodes);
        rollback_map(&mut tables.dirfrags, self.dirfrags);
        rollback_map(&mut tables.dentries, self.dentries);
        rollback_map(&mut tables.hardlinks, self.hardlinks);
        tables.rebuild_hardlink_path_index();
        rollback_map(&mut tables.compacted_tombstones, self.compacted_tombstones);
        rollback_map(
            &mut tables.compacted_tombstone_paths,
            self.compacted_tombstone_paths,
        );
        rollback_map(&mut tables.versions, self.versions);
        rollback_map(&mut tables.receipts, self.receipts);
        rollback_map(&mut tables.intents, self.intents);
        tables.rebuild_intent_indexes();
        rollback_map(&mut tables.conflicts, self.conflicts);
        rollback_map(&mut tables.visible_commits, self.visible_commits);
        rollback_map(
            &mut tables.namespace_transactions,
            self.namespace_transactions,
        );
        rollback_map(&mut tables.namespace_jobs, self.namespace_jobs);
    }
}

fn record_rollback_value<K, V>(
    rollback: &mut BTreeMap<K, Option<V>>,
    table: &BTreeMap<K, V>,
    key: K,
) where
    K: Clone + Ord,
    V: Clone,
{
    if !rollback.contains_key(&key) {
        rollback.insert(key.clone(), table.get(&key).cloned());
    }
}

fn record_compacted_tombstone_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    object: &DfsObjectMetadata,
) {
    let object_id = object.object_id.clone();
    let path_key = (object.file_system_id.clone(), normalize_path(&object.path));
    record_rollback_value(
        &mut rollback.compacted_tombstones,
        &tables.compacted_tombstones,
        object_id,
    );
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(previous_object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            previous_object_id.clone(),
        );
    }
}

fn record_compacted_tombstone_path_rollback(
    rollback: &mut DfsMetadataTableRollback,
    tables: &DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    path: &str,
) {
    let path_key = (file_system_id.clone(), normalize_path(path));
    record_rollback_value(
        &mut rollback.compacted_tombstone_paths,
        &tables.compacted_tombstone_paths,
        path_key.clone(),
    );
    if let Some(object_id) = tables.compacted_tombstone_paths.get(&path_key) {
        record_rollback_value(
            &mut rollback.compacted_tombstones,
            &tables.compacted_tombstones,
            object_id.clone(),
        );
    }
}

fn rollback_map<K, V>(table: &mut BTreeMap<K, V>, rollback: BTreeMap<K, Option<V>>)
where
    K: Ord,
{
    for (key, value) in rollback {
        if let Some(value) = value {
            table.insert(key, value);
        } else {
            table.remove(&key);
        }
    }
}
