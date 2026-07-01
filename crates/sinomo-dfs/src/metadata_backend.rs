//! Durable DFS metadata shard backend.
//!
//! This is the first concrete metadata-plane surface. It intentionally stores
//! file/object metadata outside `sinomo-kv`: materialized shard state lives in
//! packed table batches, and every mutation is mirrored into an append-only
//! journal that can later be replicated or replayed by metadata shard peers.

#[cfg(test)]
use crate::DfsReplicationPolicy;
#[cfg(test)]
use crate::control_plane::DfsMetadataShardConfig;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_cursor;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::create_dir_all;
#[cfg(test)]
use crate::metadata_io::{read_dir, read_sobj, remove_file_if_exists, sync_dir, write_sobj_atomic};
#[cfg(test)]
use crate::metadata_journal::DfsMetadataJournalHead;
#[cfg(test)]
use crate::metadata_journal::persist_replayed_journal_entry;
use crate::metadata_journal::{
    DfsMetadataReplayCursor, bounded_metadata_journal_batch, journal_entries_since_limited,
    journal_entries_since_limited_with_next_sequence, migrate_legacy_journal_entries,
    persist_replayed_journal_entries, recover_next_journal_sequence, write_local_journal_entries,
    write_local_journal_entry,
};
use crate::metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, OBJECT_LOCAL_METADATA_PAGE_SIZE,
};
use crate::metadata_lock::{MetadataWriteLock, metadata_write_lock};
use crate::metadata_namespace_transaction::{
    plan_namespace_transaction_recovery_table_ops,
    plan_namespace_transaction_table_ops as namespace_transaction_table_ops,
};
use crate::metadata_object_index::object_index_table_ops_with_previous as plan_object_index_table_ops_with_previous;
use crate::metadata_pages::{
    DfsDirectoryPageEntries, child_objects_for_directory_from_tables,
    child_objects_for_directory_page_entries_from_tables, collect_metadata_pages,
    conflict_records_page_from_tables, conflicts_for_object_from_tables,
    conflicts_for_object_page_from_tables, count_objects_for_file_system_from_tables,
    first_hardlink_to_object_outside_prefix_from_tables, hardlink_count_to_object_from_tables,
    hardlinks_to_object_page_from_tables, intents_for_object_page_from_tables,
    intents_for_object_version_page_from_tables, namespace_jobs_page_from_tables,
    objects_for_file_system_page_from_tables, receipt_records_page_from_tables,
    receipts_for_version_page_from_tables, version_records_page_from_tables,
    versions_for_object_page_from_tables, write_intents_page_from_tables,
};
use crate::metadata_read_snapshot;
use crate::metadata_receipt::{receipt_for_replica_from_backend, replay_receipt_matches_existing};
use crate::metadata_records::*;
use crate::metadata_replay::{
    hydrate_journal_entry_payloads, journal_entry_has_replay_payloads, required_replay_payload,
};
use crate::metadata_summary::{
    metadata_summary_for_file_system_from_tables, metadata_summary_from_tables,
};
use crate::metadata_table_apply::{self, TableMaterializationState};
#[cfg(test)]
use crate::metadata_table_store::{
    LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, metadata_table_path,
    metadata_table_snapshot_ops, metadata_table_snapshot_path, truncate_metadata_table_log,
};
use crate::metadata_table_store::{
    METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, archive_metadata_backend_state,
    load_or_migrate_metadata_tables, metadata_table_snapshot_fingerprint,
    metadata_tables_from_table_snapshot, metadata_tables_snapshot, persist_metadata_table_snapshot,
};
use crate::metadata_tables::{
    DfsMetadataTableRollback, DfsMetadataTables, MetadataTableReadSnapshotCache,
    MetadataTableSnapshotFingerprint,
};
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::metadata_visible_commit::{
    object_by_path_from_tables, visible_commit_already_materialized,
    visible_commit_record_from_parts, visible_commit_recovery_order, visible_commit_should_recover,
    visible_commit_table_ops, visible_object_location_recovery_ops,
};
use crate::{DfsFileSystem, DfsObjectMetadata};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
#[cfg(test)]
use sinomo_core::DfsWriteReceipt;
#[cfg(test)]
use sinomo_events::EventId;
use sinomo_node::NodeId;
#[cfg(test)]
use sinomo_object::serde_ext as sinomo_object_serde;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use time::OffsetDateTime;

const DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES: u64 = 64 * 1024 * 1024;

struct PlannedWrittenJournalTableOps {
    applied: usize,
    last_materialized_sequence: u64,
    table_ops: Vec<DfsMetadataTableOp>,
    rollback: DfsMetadataTableRollback,
}

fn metadata_backend_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs metadata backend perf");
}

/// Shard-local SOBJ metadata backend.
///
/// This backend owns one durable metadata shard: packed materialized tables,
/// append-only mutation journal, replay cursors, and snapshot repair state.
#[derive(Clone, Debug)]
pub struct LocalSobjMetadataBackend {
    shard_id: DfsMetadataShardId,
    root: PathBuf,
    next_sequence: u64,
    tables: DfsMetadataTables,
    table_log_offset: u64,
    materialized_through_sequence: u64,
    table_snapshot_fingerprint: Option<MetadataTableSnapshotFingerprint>,
    read_snapshot_cache: Arc<Mutex<Option<MetadataTableReadSnapshotCache>>>,
}

impl LocalSobjMetadataBackend {
    /// Open or initialize one durable metadata shard under `root`.
    ///
    /// Construction performs local recovery before returning: it migrates any
    /// legacy journal layout, loads the materialized table state, replays local
    /// journal entries not yet reflected in tables, and finishes interrupted
    /// namespace or visible-version commits.
    pub fn new(shard_id: DfsMetadataShardId, root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        create_dir_all(root.join("records"))?;
        create_dir_all(root.join("journal"))?;
        create_dir_all(root.join("journal").join("segments"))?;
        let mut backend = Self {
            shard_id,
            root,
            next_sequence: 1,
            tables: DfsMetadataTables::default(),
            table_log_offset: 0,
            materialized_through_sequence: 0,
            table_snapshot_fingerprint: None,
            read_snapshot_cache: Arc::new(Mutex::new(None)),
        };
        {
            let _lock = backend.metadata_write_lock()?;
            migrate_legacy_journal_entries(&backend.root)?;
            backend.next_sequence = recover_next_journal_sequence(&backend.root)?;
            let state = load_or_migrate_metadata_tables(&backend.root)?;
            let materialized_through_sequence = state.materialized_through_sequence.unwrap_or(0);
            backend.tables = state.tables;
            backend.table_log_offset = state.table_log_offset;
            backend.materialized_through_sequence = materialized_through_sequence;
            backend.table_snapshot_fingerprint = state.snapshot_fingerprint;
            backend.ensure_local_materialization_cursor()?;
            backend.reconcile_local_materialization_cursor(materialized_through_sequence)?;
            backend.recover_unmaterialized_local_journal_entries()?;
        }
        backend.recover_namespace_transactions()?;
        backend.recover_visible_version_commits()?;
        backend.recover_object_locations_from_visible_versions()?;
        Ok(backend)
    }

    /// Metadata shard owned by this local backend handle.
    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    /// Persist and materialize a replicated journal batch from a metadata peer.
    ///
    /// Entries whose replay cursor is already past their sequence are skipped.
    /// New entries are written to the local replicated journal, materialized
    /// into table batches, and then acknowledged by advancing replay cursors.
    pub fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        let _lock = self.begin_metadata_write()?;
        let mut local_cursor = self.local_materialization_cursor()?;
        let mut local_cursor_advanced = false;
        let mut replay_cursors = BTreeMap::<DfsMetadataShardId, DfsMetadataReplayCursor>::new();
        let mut advanced_cursors = BTreeSet::<DfsMetadataShardId>::new();
        let mut replay_entries = Vec::new();
        for entry in &batch.entries {
            if !replay_cursors.contains_key(&entry.shard_id) {
                let cursor = metadata_cursor::read_replay_cursor(&self.root, &entry.shard_id)?;
                replay_cursors.insert(entry.shard_id.clone(), cursor);
            }
            let cursor = replay_cursors
                .get_mut(&entry.shard_id)
                .expect("cursor inserted above");
            if entry.sequence <= cursor.last_sequence {
                continue;
            }
            replay_entries.push(entry);
            if entry.sequence > local_cursor.last_sequence {
                local_cursor.last_sequence = entry.sequence;
                local_cursor_advanced = true;
            }
            cursor.last_sequence = entry.sequence;
            advanced_cursors.insert(entry.shard_id.clone());
        }
        self.persist_replayed_journal_entries_unlocked(&replay_entries)?;

        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let (applied, table_sequence) = match self.plan_replayed_journal_entries_into_batch(
            replay_entries.iter().copied(),
            &mut table_ops,
            &mut rollback,
        ) {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        self.append_preapplied_table_ops(table_sequence, table_ops, rollback)?;
        if local_cursor_advanced {
            self.materialized_through_sequence = self
                .materialized_through_sequence
                .max(local_cursor.last_sequence);
            self.write_local_materialization_cursor(&local_cursor)?;
        }
        for shard_id in advanced_cursors {
            if let Some(cursor) = replay_cursors.get(&shard_id) {
                metadata_cursor::write_replay_cursor(&self.root, cursor)?;
            }
        }
        Ok(applied)
    }

    /// Last local or replayed journal sequence visible to this shard.
    pub fn journal_last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    /// Export the current materialized table state for replica snapshot repair.
    pub fn export_table_snapshot(&self) -> DfsMetadataTableSnapshot {
        DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(self.materialized_through_sequence),
            tables: Some(metadata_tables_snapshot(&self.tables)),
            ops: Vec::new(),
        }
    }

    /// Replace local shard tables with an authoritative replicated snapshot.
    ///
    /// The previous shard state is archived before replacement. The return
    /// value reports whether existing local state was archived.
    pub fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        let _lock = self.begin_metadata_write()?;
        let tables = metadata_tables_from_table_snapshot(snapshot, "replicated metadata snapshot")?;
        let archived_existing = archive_metadata_backend_state(&self.root)?;
        create_dir_all(self.root.join("records"))?;
        create_dir_all(self.root.join("journal").join("segments"))?;
        self.tables = tables;
        self.table_log_offset = 0;
        self.next_sequence = replayed_through_sequence.saturating_add(1).max(1);
        self.materialized_through_sequence = replayed_through_sequence;
        persist_metadata_table_snapshot(
            &self.root,
            &self.tables,
            Some(self.materialized_through_sequence),
        )?;
        self.table_snapshot_fingerprint = metadata_table_snapshot_fingerprint(&self.root)?;
        metadata_cursor::write_replay_cursor(
            &self.root,
            &DfsMetadataReplayCursor {
                shard_id: source_shard_id,
                last_sequence: replayed_through_sequence,
            },
        )?;
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: replayed_through_sequence,
        })?;
        metadata_cursor::write_journal_head(&self.root, self.next_sequence)?;
        Ok(archived_existing)
    }

    #[cfg(test)]
    pub(crate) fn apply_journal_entry(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        self.persist_replayed_journal_entry_unlocked(entry)?;
        let changed = self.materialize_replayed_journal_entry_unlocked(entry)?;
        if changed {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(Some(entry.clone()))
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    fn persist_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<()> {
        if let Some(sequence) = persist_replayed_journal_entry(&self.root, entry)? {
            self.advance_journal_head_after_replayed_entry(sequence)?;
        }
        Ok(())
    }

    fn persist_replayed_journal_entries_unlocked(
        &mut self,
        entries: &[&DfsMetadataJournalEntry],
    ) -> Result<()> {
        if let Some(max_sequence) = persist_replayed_journal_entries(&self.root, entries)? {
            self.advance_journal_head_after_replayed_entry(max_sequence)?;
        }
        Ok(())
    }

    fn advance_journal_head_after_replayed_entry(&mut self, sequence: u64) -> Result<()> {
        let next_sequence = sequence.saturating_add(1).max(1).max(self.next_sequence);
        if next_sequence == self.next_sequence {
            return Ok(());
        }
        self.next_sequence = next_sequence;
        metadata_cursor::write_journal_head(&self.root, self.next_sequence)
    }

    fn materialize_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<bool> {
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let changed = match self.materialize_replayed_journal_entry_into_batch(
            entry,
            &mut table_ops,
            &mut rollback,
        ) {
            Ok(changed) => changed,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        if !changed {
            return Ok(false);
        }
        self.append_preapplied_table_ops(entry.sequence, table_ops, rollback)?;
        Ok(true)
    }

    fn plan_replayed_journal_entries_into_batch<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a DfsMetadataJournalEntry>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<(usize, u64)> {
        let mut changed_count = 0usize;
        let mut last_materialized_sequence = 0u64;
        for entry in entries {
            if self.materialize_replayed_journal_entry_into_batch(entry, table_ops, rollback)? {
                changed_count = changed_count.saturating_add(1);
                last_materialized_sequence = entry.sequence;
            }
        }
        Ok((changed_count, last_materialized_sequence))
    }

    fn plan_written_journal_entries_table_ops(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<PlannedWrittenJournalTableOps> {
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let (applied, last_materialized_sequence) = match self
            .plan_replayed_journal_entries_into_batch(entries, &mut table_ops, &mut rollback)
        {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        Ok(PlannedWrittenJournalTableOps {
            applied,
            last_materialized_sequence,
            table_ops,
            rollback,
        })
    }

    fn materialize_written_journal_entries_unlocked(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<usize> {
        let planned = self.plan_written_journal_entries_table_ops(entries)?;
        let applied = planned.applied;
        self.append_preapplied_table_ops_for_written_entries(
            entries,
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        Ok(applied)
    }

    fn append_preapplied_table_ops_for_written_entries(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        table_sequence: u64,
        table_ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        self.append_preapplied_table_ops(table_sequence, table_ops, rollback)?;
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        Ok(())
    }

    fn materialize_replayed_journal_entry_into_batch(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<bool> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id,
                file_system,
            } => {
                let file_system =
                    required_replay_payload(entry, "put-file-system", file_system.as_ref())?;
                if self.get_file_system(file_system_id)?.as_ref() == Some(file_system) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutFileSystem {
                        file_system: file_system.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutObject {
                object_id, object, ..
            } => {
                let object = required_replay_payload(entry, "put-object", object.as_ref())?;
                let object = normalized_object_metadata(object);
                if self.get_object(object_id)? == Some(object.clone())
                    || self.tables.compacted_tombstone_matches(&object)
                {
                    return Ok(false);
                }
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(object_id)?;
                self.plan_object_index_table_ops_with_previous(
                    &object,
                    previous.as_ref(),
                    table_ops,
                    rollback,
                )?;
                Ok(true)
            }
            DfsMetadataMutation::PutVersion {
                object_id,
                version,
                version_record,
                ..
            } => {
                let version_record =
                    required_replay_payload(entry, "put-version", version_record.as_ref())?;
                let version_record = normalized_version_record(version_record);
                if self.get_version(object_id, version)? == Some(version_record.clone()) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutVersion {
                        version: version_record,
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                let receipt = required_replay_payload(entry, "put-receipt", receipt.as_ref())?;
                if let Some(existing) = receipt_for_replica_from_backend(
                    self,
                    object_id,
                    version,
                    replica_node_id,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )? {
                    if replay_receipt_matches_existing(&existing, receipt) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} conflicts with an existing receipt for replica {}",
                        entry.shard_id.0, entry.sequence, replica_node_id
                    )));
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                if receipt_for_replica_from_backend(
                    self,
                    object_id,
                    version,
                    replica_node_id,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?
                .is_none()
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveReceipt {
                        object_id: object_id.clone(),
                        version: version.clone(),
                        replica_node_id: replica_node_id.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutIntent {
                object_id,
                intent_id,
                intent,
                ..
            } => {
                let intent = required_replay_payload(entry, "put-intent", intent.as_ref())?;
                if self.get_intent(object_id, intent_id)?.as_ref() == Some(intent) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutIntent {
                        intent: intent.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::CommitVisibleVersion {
                object_id,
                previous_version,
                committed_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let committed_object = required_replay_payload(
                    entry,
                    "commit-visible-version committed object",
                    committed_object.as_ref(),
                )?;
                let visible_version = required_replay_payload(
                    entry,
                    "commit-visible-version visible version",
                    visible_version.as_ref(),
                )?;
                let commit_record = visible_commit_record_from_parts(
                    previous_version,
                    committed_object,
                    visible_version,
                    required_replica_node_ids,
                    entry.created_at,
                );
                if self
                    .get_object(object_id)?
                    .as_ref()
                    .is_some_and(|object| &object.version == committed_version)
                    && self.get_version(object_id, committed_version)?.as_ref()
                        == Some(&normalized_version_record(visible_version))
                {
                    return Ok(false);
                }
                if visible_commit_already_materialized(
                    &self.tables,
                    object_id,
                    committed_version,
                    &commit_record,
                ) {
                    return Ok(false);
                }
                let current = self.get_object(object_id)?.ok_or_else(|| {
                    MetadataBackendError::UnknownObject {
                        object_id: object_id.clone(),
                    }
                })?;
                if &current.version != previous_version {
                    return Err(MetadataBackendError::Conflict {
                        object_id: object_id.clone(),
                        expected: previous_version.clone(),
                        current: current.version,
                    });
                }
                // A replayed commit entry is authoritative after source validation. Short
                // read-your-write replay windows can arrive without every receipt entry; the
                // commit payload is still enough to make the object/version visible locally.
                self.plan_visible_version_commit_table_ops(&commit_record, table_ops, rollback)?;
                Ok(true)
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } => {
                if let Some(conflict) = conflict.as_ref() {
                    if self.get_conflict(object_id, conflict_id)?.as_ref() == Some(conflict) {
                        return Ok(false);
                    }
                    self.plan_preapplied_table_op(
                        DfsMetadataTableOp::PutConflict {
                            conflict: conflict.clone(),
                        },
                        table_ops,
                        rollback,
                    );
                } else {
                    if self.get_conflict(object_id, conflict_id)?.is_none() {
                        return Ok(false);
                    }
                    self.plan_preapplied_table_op(
                        DfsMetadataTableOp::RemoveConflict {
                            object_id: object_id.clone(),
                            conflict_id: conflict_id.clone(),
                        },
                        table_ops,
                        rollback,
                    );
                }
                Ok(true)
            }
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction_id,
                transaction,
                ..
            } => {
                let transaction = required_replay_payload(
                    entry,
                    "apply-namespace-transaction",
                    transaction.as_ref(),
                )?;
                let normalized = normalized_namespace_transaction(transaction);
                if self.tables.namespace_transactions.get(transaction_id) == Some(&normalized) {
                    return Ok(false);
                }
                self.plan_namespace_transaction_table_ops(&normalized, table_ops, rollback)?;
                Ok(true)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn hydrate_journal_entry_payloads(
        &self,
        entries: Vec<DfsMetadataJournalEntry>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        hydrate_journal_entry_payloads(self, &self.tables, entries, OBJECT_LOCAL_METADATA_PAGE_SIZE)
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    ///
    /// This lower-level reader is mainly for diagnostics and compatibility.
    /// Replication should prefer `journal_batch_since_limited`, which hydrates
    /// record payloads and enforces encoded-size bounds.
    pub fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.metadata_write_lock()?;
        journal_entries_since_limited_with_next_sequence(
            &self.root,
            after_sequence,
            max_entries,
            self.next_sequence,
        )
    }

    /// Build a bounded, payload-complete journal batch after `after_sequence`.
    ///
    /// The returned batch is suitable for metadata replication. Legacy journal
    /// entries are hydrated from materialized records before byte bounds are
    /// applied so receivers do not need the source record store.
    pub fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        let hydrated = hydrate_journal_entry_payloads(
            self,
            &self.tables,
            entries,
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        bounded_metadata_journal_batch(
            self.shard_id.clone(),
            after_sequence,
            hydrated,
            max_entries,
            max_encoded_bytes,
        )
    }

    pub(crate) fn replay_cursor_statuses(&self) -> Result<Vec<DfsMetadataReplayCursorStatus>> {
        metadata_cursor::replay_cursor_statuses(&self.root)
    }

    #[cfg(test)]
    fn write_journal(&mut self, mutation: DfsMetadataMutation) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_unlocked(mutation)
    }

    fn write_journal_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
    ) -> Result<DfsMetadataJournalEntry> {
        write_local_journal_entry(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutation,
        )
    }

    fn write_journal_entry_and_apply_table_ops_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: Vec<DfsMetadataTableOp>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.write_journal_entry_and_materialize_unlocked(mutation, |_| Ok(table_ops))
    }

    fn write_journal_entry_and_materialize_unlocked(
        &mut self,
        mutation: DfsMetadataMutation,
        table_ops: impl FnOnce(&mut Self) -> Result<Vec<DfsMetadataTableOp>>,
    ) -> Result<DfsMetadataJournalEntry> {
        let entry = self.write_journal_unlocked(mutation)?;
        let table_ops = table_ops(self)?;
        self.append_and_apply_table_ops(entry.sequence, table_ops)?;
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        Ok(entry)
    }

    fn write_journal_entries_unlocked(
        &mut self,
        mutations: Vec<DfsMetadataMutation>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        write_local_journal_entries(
            &self.root,
            &self.shard_id,
            &mut self.next_sequence,
            mutations,
        )
    }

    fn ensure_local_materialization_cursor(&self) -> Result<()> {
        metadata_cursor::ensure_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            self.tables.has_materialized_state(),
        )
    }

    fn local_materialization_cursor(&self) -> Result<DfsMetadataReplayCursor> {
        metadata_cursor::local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
        )
    }

    #[cfg(test)]
    fn advance_local_materialization_cursor(&self, sequence: u64) -> Result<()> {
        metadata_cursor::advance_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            sequence,
        )
    }

    fn reconcile_local_materialization_cursor(&self, materialized_sequence: u64) -> Result<()> {
        metadata_cursor::reconcile_local_materialization_cursor(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            materialized_sequence,
        )
    }

    fn checkpoint_local_materialization_cursor_if_due(&self, sequence: u64) -> Result<()> {
        metadata_cursor::checkpoint_local_materialization_cursor_if_due(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            sequence,
        )
    }

    fn write_local_materialization_cursor(&self, cursor: &DfsMetadataReplayCursor) -> Result<()> {
        metadata_cursor::write_local_materialization_cursor(&self.root, cursor)
    }

    fn recover_unmaterialized_local_journal_entries(&mut self) -> Result<()> {
        let mut cursor = self.local_materialization_cursor()?;
        if cursor.last_sequence >= self.next_sequence.saturating_sub(1) {
            return Ok(());
        }
        loop {
            let entries = journal_entries_since_limited(
                &self.root,
                cursor.last_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )?;
            if entries.is_empty() {
                break;
            }
            let mut advanced = false;
            for entry in entries {
                if journal_entry_has_replay_payloads(&entry) {
                    self.materialize_replayed_journal_entry_unlocked(&entry)?;
                } else {
                    self.materialize_local_journal_entry(&entry)?;
                }
                if entry.sequence > cursor.last_sequence {
                    cursor.last_sequence = entry.sequence;
                    advanced = true;
                }
            }
            if advanced {
                self.materialized_through_sequence =
                    self.materialized_through_sequence.max(cursor.last_sequence);
                self.write_local_materialization_cursor(&cursor)?;
            }
        }
        Ok(())
    }

    fn materialize_local_journal_entry(&mut self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        let table_ops = match &entry.mutation {
            DfsMetadataMutation::PutFileSystem {
                file_system_id: _,
                file_system,
            } => {
                let Some(file_system) = file_system.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutFileSystem {
                    file_system: file_system.clone(),
                }]
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    return Ok(());
                };
                if self.tables.compacted_tombstone_matches(object) {
                    return Ok(());
                }
                if self.get_file_system(&object.file_system_id)?.is_none() {
                    return Err(MetadataBackendError::UnknownFileSystem {
                        file_system_id: object.file_system_id.clone(),
                    });
                }
                let previous = self.get_object(&object.object_id)?;
                self.object_index_table_ops_with_previous(object, previous.as_ref())?
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutVersion {
                    version: normalized_version_record(version_record),
                }]
            }
            DfsMetadataMutation::PutReceipt { receipt, .. } => {
                let Some(receipt) = receipt.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutReceipt {
                    receipt: receipt.clone(),
                }]
            }
            DfsMetadataMutation::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
                ..
            } => {
                vec![DfsMetadataTableOp::RemoveReceipt {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    replica_node_id: replica_node_id.clone(),
                }]
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    return Ok(());
                };
                vec![DfsMetadataTableOp::PutIntent {
                    intent: intent.clone(),
                }]
            }
            DfsMetadataMutation::CommitVisibleVersion {
                previous_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    return Ok(());
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    return Ok(());
                };
                let commit_record = visible_commit_record_from_parts(
                    previous_version,
                    committed_object,
                    visible_version,
                    required_replica_node_ids,
                    entry.created_at,
                );
                visible_commit_table_ops(&self.tables, &commit_record)
            }
            DfsMetadataMutation::PutConflict {
                object_id,
                conflict_id,
                conflict,
                ..
            } => {
                if let Some(conflict) = conflict.as_ref() {
                    vec![DfsMetadataTableOp::PutConflict {
                        conflict: conflict.clone(),
                    }]
                } else {
                    vec![DfsMetadataTableOp::RemoveConflict {
                        object_id: object_id.clone(),
                        conflict_id: conflict_id.clone(),
                    }]
                }
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    return Ok(());
                };
                let transaction = normalized_namespace_transaction(transaction);
                namespace_transaction_table_ops(&self.tables, &transaction)
            }
        };
        self.append_and_apply_table_ops(entry.sequence, table_ops)
    }

    #[cfg(test)]
    fn replay_cursor_path(&self, shard_id: &DfsMetadataShardId) -> PathBuf {
        metadata_cursor::replay_cursor_path(&self.root, shard_id)
    }

    #[cfg(test)]
    fn local_materialization_cursor_path(&self) -> PathBuf {
        metadata_cursor::local_materialization_cursor_path(&self.root)
    }

    #[cfg(test)]
    fn metadata_replication_cursor_path(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> PathBuf {
        metadata_cursor::metadata_replication_cursor_path(&self.root, target_node_id, shard_id)
    }

    fn journal_write_lock_path(&self) -> PathBuf {
        metadata_cursor::journal_write_lock_path(&self.root)
    }

    fn metadata_write_lock(&self) -> Result<MetadataWriteLock> {
        metadata_write_lock(self.root.clone(), self.journal_write_lock_path())
    }

    fn begin_metadata_write(&mut self) -> Result<MetadataWriteLock> {
        let lock = self.metadata_write_lock()?;
        self.refresh_metadata_tables()?;
        Ok(lock)
    }

    fn append_and_apply_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_and_apply_table_ops(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
        )
    }

    fn append_preapplied_table_ops(
        &mut self,
        sequence: u64,
        ops: Vec<DfsMetadataTableOp>,
        rollback: DfsMetadataTableRollback,
    ) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::append_preapplied_table_ops(
            &root,
            self.table_materialization_state(),
            sequence,
            ops,
            DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES,
            rollback,
        )
    }

    fn compact_metadata_table_log_if_needed(&mut self, max_log_bytes: u64) -> Result<()> {
        let root = self.root.clone();
        metadata_table_apply::compact_metadata_table_log_if_needed(
            &root,
            self.table_materialization_state(),
            max_log_bytes,
        )
    }

    fn table_materialization_state(&mut self) -> TableMaterializationState<'_> {
        TableMaterializationState {
            tables: &mut self.tables,
            table_log_offset: &mut self.table_log_offset,
            materialized_through_sequence: &mut self.materialized_through_sequence,
            table_snapshot_fingerprint: &mut self.table_snapshot_fingerprint,
        }
    }

    fn refresh_metadata_tables(&mut self) -> Result<()> {
        metadata_read_snapshot::refresh_metadata_tables(
            &self.root,
            &mut self.tables,
            &mut self.table_log_offset,
            &mut self.materialized_through_sequence,
            &mut self.table_snapshot_fingerprint,
        )
    }

    pub(crate) fn current_tables_snapshot(&self) -> Result<Cow<'_, DfsMetadataTables>> {
        metadata_read_snapshot::current_tables_snapshot(
            &self.root,
            &self.tables,
            self.table_log_offset,
            self.materialized_through_sequence,
            &self.table_snapshot_fingerprint,
            &self.read_snapshot_cache,
        )
    }

    pub(crate) fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPageEntries> {
        let tables = self.current_tables_snapshot()?;
        Ok(child_objects_for_directory_page_entries_from_tables(
            &tables,
            file_system_id,
            directory_path,
            after,
            max_entries,
        ))
    }
}

impl DfsMetadataBackend for LocalSobjMetadataBackend {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_file_system(file_system),
            vec![DfsMetadataTableOp::PutFileSystem {
                file_system: file_system.clone(),
            }],
        )
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.get(file_system_id).cloned())
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.values().cloned().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let previous = self.get_object(&object.object_id)?;
        self.write_journal_entry_and_materialize_unlocked(
            DfsMetadataMutation::put_object(&object),
            |backend| backend.object_index_table_ops_with_previous(&object, previous.as_ref()),
        )
    }

    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let _lock = self.begin_metadata_write()?;
        let objects = objects
            .iter()
            .map(normalized_object_metadata)
            .collect::<Vec<_>>();
        for object in &objects {
            if self.get_file_system(&object.file_system_id)?.is_none() {
                return Err(MetadataBackendError::UnknownFileSystem {
                    file_system_id: object.file_system_id.clone(),
                });
            }
        }
        let entries = self.write_journal_entries_unlocked(
            objects
                .iter()
                .map(DfsMetadataMutation::put_object)
                .collect(),
        )?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        for object in &objects {
            let previous = self.get_object(&object.object_id)?;
            if let Err(error) = self.plan_object_index_table_ops_with_previous(
                object,
                previous.as_ref(),
                &mut table_ops,
                &mut rollback,
            ) {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        }
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.objects.get(object_id).cloned())
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(object_by_path_from_tables(&tables, file_system_id, path))
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(objects_for_file_system_page_from_tables(
            &tables,
            file_system_id,
            after,
            max_entries,
        ))
    }

    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(count_objects_for_file_system_from_tables(
            &tables,
            file_system_id,
        ))
    }

    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(child_objects_for_directory_from_tables(
            &tables,
            file_system_id,
            directory_path,
        ))
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let page = self.child_objects_for_directory_page_entries(
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: page.entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor: page.next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlinks_to_object_page_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            after,
            max_entries,
        ))
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlink_count_to_object_from_tables(
            &tables,
            file_system_id,
            target_object_id,
        ))
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(first_hardlink_to_object_outside_prefix_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            prefix,
        ))
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let version = normalized_version_record(version);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_version(&version),
            vec![DfsMetadataTableOp::PutVersion { version }],
        )
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        let metadata_matches = object.file_system_id == version.file_system_id
            && object.object_id == version.object_id;
        if !metadata_matches {
            return Err(MetadataBackendError::Serde(format!(
                "object {} {} does not match version {} {}",
                object.file_system_id, object.object_id, version.file_system_id, version.object_id
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let previous = self.get_object(&object.object_id)?;
        let entries = self.write_journal_entries_unlocked(vec![
            DfsMetadataMutation::put_object(&object),
            DfsMetadataMutation::put_version(&version),
        ])?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        if let Err(error) = self.plan_object_index_table_ops_with_previous(
            &object,
            previous.as_ref(),
            &mut table_ops,
            &mut rollback,
        ) {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVersion { version },
            &mut table_ops,
            &mut rollback,
        );
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let trace_started = Instant::now();
        let trace_object_id = object.object_id.clone();
        let trace_version = version.version.clone();
        let lock_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let lock_ms = lock_started.elapsed().as_millis();
        let validate_started = Instant::now();
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.file_system_id != intent.file_system_id
            || object.object_id != intent.object_id
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined object/version/intent metadata mismatch for object {}",
                object.object_id
            )));
        }
        for receipt in receipts {
            if receipt.receipt.file_system_id != object.file_system_id
                || receipt.receipt.object_id != object.object_id
                || receipt.receipt.committed_version != version.version
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined receipt metadata mismatch for object {}",
                    object.object_id
                )));
            }
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let validate_ms = validate_started.elapsed().as_millis();
        let build_started = Instant::now();
        let mut mutations = Vec::with_capacity(3 + receipts.len());
        mutations.push(DfsMetadataMutation::put_object(&object));
        mutations.push(DfsMetadataMutation::put_version(&version));
        mutations.push(DfsMetadataMutation::put_intent(intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        let build_ms = build_started.elapsed().as_millis();
        let journal_started = Instant::now();
        let entries = self.write_journal_entries_unlocked(mutations)?;
        let journal_ms = journal_started.elapsed().as_millis();
        let materialize_started = Instant::now();
        let planned = self.plan_written_journal_entries_table_ops(&entries)?;
        let materialize_ms = materialize_started.elapsed().as_millis();
        let table_append_started = Instant::now();
        self.append_preapplied_table_ops(
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        let table_append_ms = table_append_started.elapsed().as_millis();
        let checkpoint_started = Instant::now();
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-put-object-version-intent-receipts shard={} object={} version={} receipts={} entries={} lock_ms={} validate_ms={} build_ms={} journal_ms={} materialize_ms={} table_append_ms={} checkpoint_ms={} total_ms={}",
            self.shard_id.0,
            trace_object_id,
            trace_version,
            receipts.len(),
            entries.len(),
            lock_ms,
            validate_ms,
            build_ms,
            journal_ms,
            materialize_ms,
            table_append_ms,
            checkpoint_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(entries)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .versions
            .get(&(object_id.clone(), version.clone()))
            .cloned())
    }

    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        let tables = self.current_tables_snapshot()?;
        Ok(keys
            .iter()
            .map(|(object_id, version)| {
                (
                    tables.objects.get(object_id).cloned(),
                    tables
                        .versions
                        .get(&(object_id.clone(), version.clone()))
                        .cloned(),
                )
            })
            .collect())
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(versions_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(version_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(receipt_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflict_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_from_tables(&tables, |_| Ok(true))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_for_file_system_from_tables(&tables, file_system_id, |_| Ok(true))
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_receipt(receipt),
            vec![DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            }],
        )
    }

    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let Some(existing) = self
            .tables
            .receipts
            .get(&(object_id.clone(), version.clone(), replica_node_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::RemoveReceipt {
                file_system_id: existing.receipt.file_system_id,
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            },
            vec![DfsMetadataTableOp::RemoveReceipt {
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            }],
        )?;
        Ok(Some(entry))
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(receipts_for_version_page_from_tables(
            &tables,
            object_id,
            version,
            after,
            max_entries,
        ))
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            }],
        )
    }

    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _ = object;
        let _lock = self.begin_metadata_write()?;
        let mut mutations = Vec::with_capacity(1 + receipts.len());
        mutations.push(DfsMetadataMutation::put_intent(intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        let entries = self.write_journal_entries_unlocked(mutations)?;
        self.materialize_written_journal_entries_unlocked(&entries)?;
        Ok(entries)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intents
            .get(&(object_id.clone(), intent_id.to_string()))
            .cloned())
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_version_page_from_tables(
            &tables,
            object_id,
            target_version,
            after,
            max_entries,
        ))
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(write_intents_page_from_tables(&tables, after, max_entries))
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .get_object(&committed_object.object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: committed_object.object_id.clone(),
            })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: committed_object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }

        for node_id in required_replica_node_ids {
            if !self.tables.receipts.contains_key(&(
                committed_object.object_id.clone(),
                committed_object.version.clone(),
                node_id.clone(),
            )) {
                return Err(MetadataBackendError::Conflict {
                    object_id: committed_object.object_id.clone(),
                    expected: expected_version.clone(),
                    current: current.version.clone(),
                });
            }
        }

        let commit_record = visible_commit_record_from_parts(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
            OffsetDateTime::now_utc(),
        );
        let table_ops = visible_commit_table_ops(&self.tables, &commit_record);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::commit_visible_version(&commit_record),
            table_ops,
        )
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if transaction
            .objects
            .iter()
            .any(|object| object.file_system_id != transaction.file_system_id)
        {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }

        let transaction = normalized_namespace_transaction(transaction);
        let table_ops = namespace_transaction_table_ops(&self.tables, &transaction);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                transaction: Some(transaction),
            },
            table_ops,
        )
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        self.compact_tombstones_with_ids(options)
            .map(|(report, _)| report)
    }

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        self.append_and_apply_table_ops(
            self.next_sequence.saturating_sub(1).max(1),
            vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }],
        )
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_transactions.get(transaction_id).cloned())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_jobs.get(job_id).cloned())
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(namespace_jobs_page_from_tables(&tables, after, max_entries))
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: conflict.file_system_id.clone(),
                object_id: conflict.object_id.clone(),
                conflict_id: conflict.conflict_id.clone(),
                conflict: Some(conflict.clone()),
            },
            vec![DfsMetadataTableOp::PutConflict {
                conflict: conflict.clone(),
            }],
        )
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .conflicts
            .get(&(object_id.clone(), conflict_id.to_string()))
            .cloned())
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_from_tables(&tables, object_id))
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(existing) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: existing.file_system_id.clone(),
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
                conflict: None,
            },
            vec![DfsMetadataTableOp::RemoveConflict {
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
            }],
        )?;
        Ok(Some(existing))
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(mut conflict) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        conflict.status = status;
        self.put_conflict(&conflict)?;
        Ok(Some(conflict))
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, usize::MAX)
    }

    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, max_entries)
    }

    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(LocalSobjMetadataBackend::journal_last_sequence(self))
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        Ok(LocalSobjMetadataBackend::export_table_snapshot(self))
    }

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        if shard_id == &self.shard_id {
            return Ok(Some(LocalSobjMetadataBackend::export_table_snapshot(self)));
        }
        Ok(None)
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        metadata_cursor::put_metadata_replication_cursor(
            &self.root,
            target_node_id,
            shard_id,
            next_after_sequence,
        )
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let journal_next_sequence = self.next_sequence;
        Ok(vec![DfsMetadataShardStatus {
            shard_id: self.shard_id.clone(),
            file_system_id: None,
            primary_node_id: None,
            replica_node_ids: Vec::new(),
            epoch: None,
            journal_next_sequence,
            journal_last_sequence: journal_next_sequence.saturating_sub(1),
            replica_lag: Vec::new(),
            replay_cursors: self.replay_cursor_statuses()?,
        }])
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        LocalSobjMetadataBackend::apply_journal_batch(self, batch)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot(
            self,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        DfsMetadataSubscription::new(self.shard_id.clone(), after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        let entries = self.journal_since(subscription.next_after_sequence)?;
        if let Some(last) = entries.last() {
            subscription.next_after_sequence = last.sequence;
        }
        Ok(DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            entries,
            next_after_sequence: subscription.next_after_sequence,
        })
    }
}

impl LocalSobjMetadataBackend {
    fn recover_namespace_transactions(&mut self) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        for transaction in self
            .tables
            .namespace_transactions
            .values()
            .cloned()
            .collect::<Vec<_>>()
        {
            self.materialize_namespace_transaction_for_recovery(&transaction)?;
        }
        Ok(())
    }

    fn plan_namespace_transaction_table_ops(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        for op in namespace_transaction_table_ops(&self.tables, transaction) {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn materialize_namespace_transaction_for_recovery(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<bool> {
        let sequence = self.next_sequence.saturating_sub(1);
        let table_ops = plan_namespace_transaction_recovery_table_ops(&self.tables, transaction);
        if table_ops.is_empty() {
            return Ok(false);
        }
        self.append_and_apply_table_ops(sequence, table_ops)?;
        Ok(true)
    }

    pub(crate) fn compact_tombstones_with_ids(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<(DfsTombstoneCompactionReport, Vec<DfsObjectId>)> {
        let _lock = self.begin_metadata_write()?;
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        let plan = plan_tombstone_compaction(&self.tables, &options, now);
        if plan.table_ops.is_empty() {
            return Ok((plan.report, plan.compacted_object_ids));
        }
        let sequence = self.next_sequence.saturating_sub(1).max(1);
        self.append_and_apply_table_ops(sequence, plan.table_ops)?;
        self.compact_metadata_table_log_if_needed(0)?;
        Ok((plan.report, plan.compacted_object_ids))
    }

    fn plan_object_index_table_ops_with_previous(
        &mut self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let object_ops = self.object_index_table_ops_with_previous(object, previous)?;
        for op in object_ops {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn plan_preapplied_table_op(
        &mut self,
        op: DfsMetadataTableOp,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) {
        self.tables.apply_with_rollback(op.clone(), rollback);
        table_ops.push(op);
    }

    fn plan_visible_version_commit_table_ops(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        for op in visible_commit_table_ops(&self.tables, commit) {
            self.plan_preapplied_table_op(op, table_ops, rollback);
        }
        Ok(())
    }

    fn object_index_table_ops_with_previous(
        &self,
        object: &DfsObjectMetadata,
        previous: Option<&DfsObjectMetadata>,
    ) -> Result<Vec<DfsMetadataTableOp>> {
        Ok(plan_object_index_table_ops_with_previous(
            &self.tables,
            object,
            previous,
        ))
    }

    fn recover_visible_version_commits(&mut self) -> Result<()> {
        for commit in visible_commit_recovery_order(&self.tables) {
            self.recover_visible_version_commit(&commit)?;
        }
        Ok(())
    }

    fn recover_object_locations_from_visible_versions(&mut self) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        let table_ops = visible_object_location_recovery_ops(&self.tables);
        if table_ops.is_empty() {
            return Ok(());
        }
        let sequence = self.next_sequence.saturating_sub(1);
        self.append_and_apply_table_ops(sequence, table_ops)
    }

    fn recover_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        if !visible_commit_should_recover(&self.tables, commit) {
            return Ok(());
        }

        let entry =
            self.write_journal_unlocked(DfsMetadataMutation::commit_visible_version(commit))?;
        self.materialize_visible_version_commit(commit, entry.sequence)?;
        Ok(())
    }

    fn materialize_visible_version_commit(
        &mut self,
        commit: &DfsVisibleVersionCommitRecord,
        sequence: u64,
    ) -> Result<()> {
        self.append_and_apply_table_ops(sequence, visible_commit_table_ops(&self.tables, commit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalShardedMetadataBackend;
    use crate::metadata_journal::{
        JOURNAL_SEGMENT_ENTRY_LIMIT, append_segmented_journal_entry,
        metadata_journal_batch_encoded_len,
    };
    use crate::metadata_keys::{
        dentry_key, key_segment, normalize_path, path_hash, path_record_for_object,
    };
    use crate::{DfsMetadataShardAssignment, DfsObjectKind};
    use sinomo_core::DfsObjectPolicy;
    use std::time::Duration;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn file_system() -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        }
    }

    fn object(version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "docs/readme.md".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn namespace_job(index: usize, root_path: &str) -> DfsNamespaceJobRecord {
        let timestamp = OffsetDateTime::from_unix_timestamp(index as i64).expect("valid timestamp");
        DfsNamespaceJobRecord {
            job_id: format!("job_namespace_page_{index:02}"),
            file_system_id: file_system_id(),
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: root_path.to_string(),
            target_path: None,
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: timestamp,
            updated_at: timestamp,
            completed_at: None,
            failure_reason: None,
        }
    }

    fn shard_config(
        shard_id: &str,
        assignment: DfsMetadataShardAssignment,
    ) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: shard_id.to_string(),
            primary_node_id: node("node_owner"),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn table_path_record(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> DfsPathRecord {
        backend
            .tables
            .paths
            .get(&(file_system_id.clone(), normalize_path(path)))
            .cloned()
            .expect("path table record")
    }

    fn table_path_exists(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> bool {
        backend
            .tables
            .paths
            .contains_key(&(file_system_id.clone(), normalize_path(path)))
    }

    fn table_dentry_exists(
        backend: &LocalSobjMetadataBackend,
        path_record: &DfsPathRecord,
    ) -> bool {
        backend
            .tables
            .dentries
            .contains_key(&dentry_key(path_record))
    }

    fn table_dirfrag(
        backend: &LocalSobjMetadataBackend,
        dir_inode_id: &DfsInodeId,
        dirfrag_id: &DfsDirFragmentId,
    ) -> DfsDirFragmentRecord {
        backend
            .tables
            .dirfrags
            .get(&(dir_inode_id.clone(), dirfrag_id.clone()))
            .cloned()
            .expect("dirfrag table record")
    }

    fn table_file_system_object_exists(
        backend: &LocalSobjMetadataBackend,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .file_system_objects
            .contains_key(&(file_system_id.clone(), object_id.clone()))
    }

    fn table_hardlink_exists(
        backend: &LocalSobjMetadataBackend,
        target_object_id: &DfsObjectId,
        hardlink_object_id: &DfsObjectId,
    ) -> bool {
        backend
            .tables
            .hardlinks
            .contains_key(&(target_object_id.clone(), hardlink_object_id.clone()))
    }

    fn force_table_replay_from_journal(
        backend: &LocalSobjMetadataBackend,
        root: &Path,
        last_materialized_sequence: u64,
    ) {
        remove_file_if_exists(&metadata_table_path(root)).expect("remove metadata table log");
        remove_file_if_exists(&metadata_table_snapshot_path(root))
            .expect("remove metadata table snapshot");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: backend.shard_id.clone(),
                last_sequence: last_materialized_sequence,
            },
        )
        .expect("rewind materialization cursor");
    }

    fn metadata_table_batch_count(root: &Path) -> usize {
        use std::io::Read as _;

        let path = metadata_table_path(root);
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return 0,
            Err(source) => panic!("open metadata table log {}: {source}", path.display()),
        };
        let mut count = 0usize;
        loop {
            let mut length_bytes = [0_u8; 4];
            match file.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(source) => panic!("read metadata table length {}: {source}", path.display()),
            }
            let length = u32::from_le_bytes(length_bytes) as usize;
            let mut bytes = vec![0_u8; length];
            file.read_exact(&mut bytes).unwrap_or_else(|source| {
                panic!("read metadata table batch {}: {source}", path.display())
            });
            let _: DfsMetadataTableBatch =
                sinomo_object_serde::from_bytes(&bytes).unwrap_or_else(|source| {
                    panic!("decode metadata table batch {}: {source}", path.display())
                });
            count = count.saturating_add(1);
        }
        count
    }

    fn block_shard_journal_writes(root: &Path, shard_id: &str) {
        let segments = root
            .join(key_segment(&shard_id))
            .join("journal")
            .join("segments");
        fs::remove_dir_all(&segments).expect("remove journal segments directory");
        fs::write(&segments, b"not a directory").expect("block journal segment writes");
    }

    #[test]
    fn local_backend_persists_file_system_object_path_and_journal() {
        let root = temp_root("local_backend_persists_file_system_object_path_and_journal");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        let file_system_entry = backend.put_file_system(&file_system).expect("filesystem");
        let object_entry = backend.put_object(&object).expect("object");

        assert_eq!(file_system_entry.sequence, 1);
        assert_eq!(object_entry.sequence, 2);
        assert_eq!(
            file_system_entry.mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system.file_system_id.clone(),
                file_system: Some(file_system.clone()),
            },
            "filesystem journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            object_entry.mutation,
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            },
            "object journal entries should carry replay-ready record payloads"
        );
        assert_eq!(
            backend
                .get_file_system(&file_system.file_system_id)
                .expect("get DfsFileSystem"),
            Some(file_system)
        );
        assert_eq!(
            backend.get_object(&object.object_id).expect("get object"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_object_by_path(&object.file_system_id, "docs/readme.md")
                .expect("get by path"),
            Some(object.clone())
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        assert!(
            metadata_table_path(&root).is_file(),
            "new metadata writes should materialize through the packed table log"
        );
        assert!(
            !root
                .join("records")
                .join("objects")
                .join(key_segment(&object.object_id))
                .join("object.sobj")
                .exists(),
            "new object writes should not create one file per object metadata record"
        );
        assert!(
            !root
                .join("records")
                .join("paths")
                .join(key_segment(&object.file_system_id))
                .join(format!("{}.sobj", path_hash(&object.path)))
                .exists(),
            "new object writes should not create one file per path index record"
        );

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(1).expect("journal").len(), 1);

        cleanup(&root);
    }

    #[test]
    fn local_backend_batches_object_metadata_into_one_table_batch() {
        let root = temp_root("local_backend_batches_object_metadata_into_one_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let objects = (0..3)
            .map(|index| {
                let mut object = object(version(&format!("version_batch_{index}")));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_batch_{index}")).expect("object");
                object.path = format!("docs/file-{index}.md");
                object
            })
            .collect::<Vec<_>>();
        let entries = backend.put_objects(&objects).expect("object batch");

        assert_eq!(entries.len(), objects.len());
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            backend.journal_since(1).expect("journal").len(),
            objects.len()
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            2,
            "filesystem write should use one table batch and the object group should use one more"
        );
        for object in &objects {
            assert_eq!(
                backend.get_object(&object.object_id).expect("get object"),
                Some(object.clone())
            );
        }

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .count_objects_for_file_system(&file_system.file_system_id)
                .expect("count objects"),
            objects.len()
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_routes_object_and_version_to_selected_path_shard() {
        let root = temp_root("sharded_backend_routes_object_and_version");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::now_utc(),
            visible: true,
        };

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend.put_version(&version_record).expect("version");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/services/readme.md")
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version"),
            Some(version_record)
        );
        let services_batch = backend
            .journal_batch_for_shard_since_limited(
                &DfsMetadataShardId::new("services"),
                0,
                16,
                usize::MAX,
            )
            .expect("services batch")
            .expect("services changes");
        assert!(
            services_batch.entries.iter().any(|entry| matches!(
                entry.mutation,
                DfsMetadataMutation::PutObject { .. } | DfsMetadataMutation::PutVersion { .. }
            )),
            "path-selected shard should own object and version journal entries"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_moves_object_between_path_shards_without_scanning_on_read() {
        let root = temp_root("sharded_backend_moves_object_between_path_shards");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("services object");

        let mut moved = object.clone();
        moved.path = "/users/readme.md".to_string();
        moved.version = version("version_2");
        backend.put_object(&moved).expect("moved object");

        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get moved object")
                .as_ref()
                .map(|object| object.path.as_str()),
            Some("/users/readme.md")
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.path)
                .collect::<Vec<_>>(),
            vec!["/users/readme.md".to_string()]
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/readme.md")
                .expect("old path")
                .map(|object| object.kind),
            Some(DfsObjectKind::Tombstone)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_objects_and_directory_children_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_with_shard_cursors");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut root_file = object(version("version_1"));
        root_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_root_file").expect("root file");
        root_file.path = "/root.txt".to_string();

        let mut users_dir = object(version("version_1"));
        users_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_users").expect("users dir");
        users_dir.path = "/users".to_string();
        users_dir.kind = DfsObjectKind::Directory;
        users_dir.content_hash = None;
        users_dir.content_size_bytes = None;

        let mut services_dir = object(version("version_1"));
        services_dir.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_services").expect("services dir");
        services_dir.path = "/services".to_string();
        services_dir.kind = DfsObjectKind::Directory;
        services_dir.content_hash = None;
        services_dir.content_size_bytes = None;

        let mut service_file = object(version("version_1"));
        service_file.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_page_service_file").expect("service file");
        service_file.path = "/services/app.txt".to_string();

        for object in [&root_file, &users_dir, &services_dir, &service_file] {
            backend.put_object(object).expect("object");
        }

        let mut object_cursor = None;
        let mut paged_object_ids = BTreeSet::new();
        let mut saw_object_shard_cursor = false;
        loop {
            let page = backend
                .objects_for_file_system_page(
                    &file_system.file_system_id,
                    object_cursor.as_ref(),
                    2,
                )
                .expect("object page");
            paged_object_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_object_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        let full_object_ids = backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("full objects")
            .into_iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(paged_object_ids, full_object_ids);
        assert!(
            saw_object_shard_cursor,
            "sharded object pages should carry shard cursors for bounded continuation"
        );

        let mut dir_cursor = None;
        let mut root_child_names = Vec::new();
        let mut saw_directory_shard_cursor = false;
        loop {
            let page = backend
                .child_objects_for_directory_page(
                    &file_system.file_system_id,
                    "/",
                    dir_cursor.as_ref(),
                    1,
                )
                .expect("directory page");
            root_child_names.extend(page.objects.into_iter().map(|object| {
                object
                    .path
                    .rsplit('/')
                    .next()
                    .expect("child name")
                    .to_string()
            }));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_directory_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            dir_cursor = Some(next_cursor);
        }
        root_child_names.sort();
        assert_eq!(root_child_names, vec!["root.txt", "services", "users"]);
        assert!(
            saw_directory_shard_cursor,
            "sharded directory pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_namespace_jobs() {
        let root = temp_root("sharded_backend_pages_namespace_jobs");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");

        for index in 0..6 {
            let root_path = if index % 2 == 0 {
                format!("/jobs/{index}")
            } else {
                format!("/services/jobs/{index}")
            };
            backend
                .put_namespace_job(&namespace_job(index, &root_path))
                .expect("namespace job");
        }

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..6)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(
            backend
                .get_namespace_job("job_namespace_page_05")
                .expect("get job")
                .expect("job")
                .root_path,
            "/services/jobs/5"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_gets_namespace_transaction_by_shard_transaction_id() {
        let root = temp_root("sharded_backend_get_namespace_transaction");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_1"));
        object.path = "/services/namespace-transaction.txt".to_string();

        let entry = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("namespace transaction");
        let transaction_id = match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction_id, .. } => {
                transaction_id.clone()
            }
            other => panic!("expected namespace transaction journal entry, got {other:?}"),
        };

        let recovered = backend
            .get_namespace_transaction(&transaction_id)
            .expect("lookup transaction")
            .expect("transaction");
        assert_eq!(recovered.transaction_id, transaction_id);
        assert_eq!(
            recovered.objects.first().map(|object| &object.object_id),
            Some(&object.object_id)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_object_local_records_with_shard_cursors() {
        let root = temp_root("sharded_backend_pages_object_local_records");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut file = object(version("version_0"));
        file.path = "/services/readme.md".to_string();
        backend.put_object(&file).expect("object");

        for index in 0..3 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let first_version_page = backend
            .versions_for_object_page(&file.object_id, None, 1)
            .expect("first version page");
        assert_eq!(first_version_page.versions.len(), 1);
        assert_eq!(
            first_version_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_version_scan = backend
            .version_records_page(None, 2)
            .expect("first version scan page");
        assert_eq!(first_version_scan.versions.len(), 2);
        assert!(first_version_scan.next_cursor.is_some());
        let second_version_scan = backend
            .version_records_page(first_version_scan.next_cursor.as_ref(), 2)
            .expect("second version scan page");
        assert_eq!(second_version_scan.versions.len(), 1);
        assert!(second_version_scan.next_cursor.is_none());
        let scanned_versions = first_version_scan
            .versions
            .into_iter()
            .chain(second_version_scan.versions)
            .map(|version_record| version_record.version)
            .collect::<Vec<_>>();
        let mut expected_versions = (0..3)
            .map(|index| version(&format!("version_{index}")))
            .collect::<Vec<_>>();
        expected_versions.sort();
        assert_eq!(scanned_versions, expected_versions);

        for index in 0..3 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }
        let first_receipt_page = backend
            .receipts_for_version_page(&file.object_id, &version("version_0"), None, 1)
            .expect("first receipt page");
        assert_eq!(first_receipt_page.receipts.len(), 1);
        assert_eq!(
            first_receipt_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_receipt_scan = backend
            .receipt_records_page(None, 2)
            .expect("first receipt scan page");
        assert_eq!(first_receipt_scan.receipts.len(), 2);
        assert!(first_receipt_scan.next_cursor.is_some());
        let second_receipt_scan = backend
            .receipt_records_page(first_receipt_scan.next_cursor.as_ref(), 2)
            .expect("second receipt scan page");
        assert_eq!(second_receipt_scan.receipts.len(), 1);
        assert!(second_receipt_scan.next_cursor.is_none());
        let scanned_receipts = first_receipt_scan
            .receipts
            .into_iter()
            .chain(second_receipt_scan.receipts)
            .map(|receipt| receipt.replica_node_id)
            .collect::<Vec<_>>();
        let mut expected_receipts = (0..3)
            .map(|index| node(&format!("node_replica_{index}")))
            .collect::<Vec<_>>();
        expected_receipts.sort();
        assert_eq!(scanned_receipts, expected_receipts);

        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }
        let first_intent_page = backend
            .intents_for_object_page(&file.object_id, None, 1)
            .expect("first intent page");
        assert_eq!(first_intent_page.intents.len(), 1);
        assert_eq!(
            first_intent_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let first_scan_page = backend
            .write_intents_page(None, 2)
            .expect("first write intent scan page");
        assert_eq!(first_scan_page.intents.len(), 2);
        assert!(first_scan_page.next_cursor.is_some());
        let second_scan_page = backend
            .write_intents_page(first_scan_page.next_cursor.as_ref(), 2)
            .expect("second write intent scan page");
        assert_eq!(second_scan_page.intents.len(), 1);
        assert!(second_scan_page.next_cursor.is_none());
        let scanned_intent_ids = first_scan_page
            .intents
            .into_iter()
            .chain(second_scan_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(scanned_intent_ids, vec!["intent-0", "intent-1", "intent-2"]);

        let target_version = version("version_shared_target");
        for index in 0..3 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-shared-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: target_version.clone(),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:shared-target-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("shared target intent");
        }
        let first_target_page = backend
            .intents_for_object_version_page(&file.object_id, &target_version, None, 2)
            .expect("first target intent page");
        assert_eq!(first_target_page.intents.len(), 2);
        assert_eq!(
            first_target_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.shard_id.as_ref())
                .map(|shard_id| shard_id.0.as_str()),
            Some("services")
        );
        let second_target_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &target_version,
                first_target_page.next_cursor.as_ref(),
                2,
            )
            .expect("second target intent page");
        assert_eq!(second_target_page.intents.len(), 1);
        assert!(second_target_page.next_cursor.is_none());
        let shared_target_ids = first_target_page
            .intents
            .into_iter()
            .chain(second_target_page.intents)
            .map(|intent| intent.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shared_target_ids,
            vec![
                "intent-shared-target-0".to_string(),
                "intent-shared-target-1".to_string(),
                "intent-shared-target-2".to_string()
            ]
        );

        let mut transient_object = file.clone();
        transient_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_transient_intent")
            .expect("transient object id");
        transient_object.path = "/services/transient-create.txt".to_string();
        transient_object.version = version("version_transient_1");
        let transient_intent = DfsWriteIntentRecord {
            intent_id: "intent-transient-create".to_string(),
            file_system_id: transient_object.file_system_id.clone(),
            object_id: transient_object.object_id.clone(),
            base_version: None,
            target_version: version("version_transient_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: Some("blake3:transient".to_string()),
            object_metadata: Some(transient_object.clone()),
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent_for_object(Some(&transient_object), &transient_intent)
            .expect("transient intent");
        assert_eq!(
            backend
                .intents_for_object_version_page(
                    &transient_object.object_id,
                    &transient_intent.target_version,
                    None,
                    1,
                )
                .expect("transient target intent page")
                .intents
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            vec!["intent-transient-create"]
        );

        let mut moved_object = object(version("version_1"));
        moved_object.object_id = DfsObjectId::from_legacy_named("obj_sharded_stale_intent_source")
            .expect("stale intent object id");
        moved_object.path = "/stale-intent-source.txt".to_string();
        backend.put_object(&moved_object).expect("root object");
        let stale_source_intent = DfsWriteIntentRecord {
            intent_id: "intent-stale-source".to_string(),
            file_system_id: moved_object.file_system_id.clone(),
            object_id: moved_object.object_id.clone(),
            base_version: Some(moved_object.version.clone()),
            target_version: version("version_stale_source_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:stale-source".to_string()),
            object_metadata: Some(moved_object.clone()),
            created_at: OffsetDateTime::now_utc(),
            updated_at: None,
        };
        backend
            .put_intent(&stale_source_intent)
            .expect("source shard intent");
        let mut moved_target_object = moved_object.clone();
        moved_target_object.path = "/services/stale-intent-source.txt".to_string();
        moved_target_object.version = version("version_stale_source_3");
        backend
            .put_object(&moved_target_object)
            .expect("move object to services");
        assert!(
            backend
                .intents_for_object_version_page(
                    &moved_object.object_id,
                    &stale_source_intent.target_version,
                    None,
                    1,
                )
                .expect("stale source target intent page")
                .intents
                .is_empty(),
            "target-version lookups should hide source-shard intents after the object index moves"
        );

        let mut scan_cursor = None;
        let mut visible_scan_ids = BTreeSet::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("visible intent scan page");
            assert!(page.intents.len() <= 2);
            visible_scan_ids.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert!(
            visible_scan_ids.contains("intent-transient-create"),
            "transient create intents should remain visible in global scans"
        );
        assert!(
            !visible_scan_ids.contains("intent-stale-source"),
            "global scans should hide source-shard intents after the object index moves"
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<BTreeSet<_>>(),
            visible_scan_ids
        );

        for index in 0..3 {
            let conflict = DfsConflictRecord {
                conflict_id: format!("conflict-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                path: file.path.clone(),
                base_version: Some(version("version_0")),
                local_version: version(&format!("version_local_{index}")),
                remote_version: version(&format!("version_remote_{index}")),
                detected_by_node_id: node("node_owner"),
                reason: format!("conflict {index}"),
                local_content_hash: Some(format!("blake3:local-{index}")),
                remote_content_hash: Some(format!("blake3:remote-{index}")),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_conflict(&conflict).expect("conflict");
        }
        let first_conflict_scan = backend
            .conflict_records_page(None, 2)
            .expect("first conflict scan page");
        assert_eq!(first_conflict_scan.conflicts.len(), 2);
        assert!(first_conflict_scan.next_cursor.is_some());
        let second_conflict_scan = backend
            .conflict_records_page(first_conflict_scan.next_cursor.as_ref(), 2)
            .expect("second conflict scan page");
        assert_eq!(second_conflict_scan.conflicts.len(), 1);
        assert!(second_conflict_scan.next_cursor.is_none());
        let scanned_conflicts = first_conflict_scan
            .conflicts
            .into_iter()
            .chain(second_conflict_scan.conflicts)
            .map(|conflict| conflict.conflict_id)
            .collect::<Vec<_>>();
        assert_eq!(
            scanned_conflicts,
            vec!["conflict-0", "conflict-1", "conflict-2"]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_pages_and_counts_hardlinks_across_shards() {
        let root = temp_root("sharded_backend_pages_hardlinks");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let target = object(version("version_1"));
        let mut root_alias = object(version("version_1"));
        root_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_root_alias").expect("root alias");
        root_alias.path = "/z-root.alias".to_string();
        root_alias.kind = DfsObjectKind::Hardlink;
        root_alias.content_hash = None;
        root_alias.link_target_object_id = Some(target.object_id.clone());
        let mut service_alias = root_alias.clone();
        service_alias.object_id =
            DfsObjectId::from_legacy_named("obj_sharded_service_alias").expect("service alias");
        service_alias.path = "/services/a-service.alias".to_string();

        for object in [&target, &root_alias, &service_alias] {
            backend.put_object(object).expect("object");
        }

        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system.file_system_id, &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system.file_system_id, &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/services/a-service.alias"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system.file_system_id,
                    &target.object_id,
                    "/services",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/z-root.alias"
        );

        let mut cursor = None;
        let mut paged_ids = BTreeSet::new();
        let mut saw_hardlink_shard_cursor = false;
        loop {
            let page = backend
                .hardlinks_to_object_page(
                    &file_system.file_system_id,
                    &target.object_id,
                    cursor.as_ref(),
                    1,
                )
                .expect("hardlink page");
            paged_ids.extend(page.objects.into_iter().map(|object| object.object_id));
            if page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.shard_id.is_some())
            {
                saw_hardlink_shard_cursor = true;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_ids,
            [root_alias.object_id, service_alias.object_id]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert!(
            saw_hardlink_shard_cursor,
            "sharded hardlink pages should carry shard cursors for bounded continuation"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_object_when_cross_shard_target_write_fails() {
        let root = temp_root("sharded_backend_cross_shard_target_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("target shard journal failure should reject move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target write must not tombstone or move the source object"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target write must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_keeps_source_namespace_when_cross_shard_target_transaction_fails() {
        let root = temp_root("sharded_backend_cross_shard_transaction_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "services");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "move-to-services".to_string(),
                file_system_id: file_system.file_system_id.clone(),
                objects: vec![moved],
                versions: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect_err("target shard journal failure should reject namespace move");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "target namespace journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("source path")
                .as_ref()
                .map(|object| (&object.path, &object.version, &object.kind)),
            Some((&object.path, &object.version, &DfsObjectKind::File)),
            "failed target namespace transaction must keep the source object visible"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((&object.path, &DfsMetadataShardId::new("root"))),
            "failed target namespace transaction must leave the object shard index on the source shard"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_hides_stale_source_path_when_cross_shard_tombstone_fails() {
        let root = temp_root("sharded_backend_cross_shard_tombstone_failure");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        block_shard_journal_writes(&root, "root");

        let mut moved = object.clone();
        moved.path = "/services/readme.md".to_string();
        moved.version = version("version_2");
        let error = backend
            .put_object(&moved)
            .expect_err("source tombstone journal failure should be surfaced");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "source tombstone journal failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object by id")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version)),
            "object id lookups should use the target shard after the target write and index update"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("old source path"),
            None,
            "a stale non-tombstoned source object must not remain visible once the index points at the target shard"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &moved.path)
                .expect("target path")
                .as_ref()
                .map(|object| (&object.path, &object.version)),
            Some((&moved.path, &moved.version))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_migrates_existing_root_object_to_new_path_shard() {
        let root = temp_root("sharded_backend_migrates_existing_root_object");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/readme.md".to_string();
        object.content_hash = Some("blake3:migrated".to_string());
        object.content_size_bytes = Some(8);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: object.created_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_migrate_receipt").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: object.content_hash.clone(),
            object_metadata: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_migrate".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version: Some(object.version.clone()),
            local_version: version("version_local"),
            remote_version: version("version_remote"),
            detected_by_node_id: node("node_owner"),
            reason: "migration coverage".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: Some(intent.intent_id.clone()),
            strategy: Some("manual".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
            root_only.put_version(&version_record).expect("version");
            root_only.put_receipt(&receipt).expect("receipt");
            root_only.put_intent(&intent).expect("intent");
            root_only.put_conflict(&conflict).expect("conflict");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migrate objects");

        assert_eq!(
            report,
            DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 1,
                indexed_objects: 0,
                migrated_objects: 1,
                skipped_stale_sources: 0,
                reached_limit: false,
            }
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("migrated path"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("migrated version"),
            Some(version_record.clone())
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("migrated receipts"),
            vec![receipt.clone()]
        );
        assert_eq!(
            backend
                .get_intent(&object.object_id, &intent.intent_id)
                .expect("migrated intent"),
            Some(intent)
        );
        assert_eq!(
            backend
                .get_conflict(&object.object_id, &conflict.conflict_id)
                .expect("migrated conflict"),
            Some(conflict)
        );
        assert_eq!(
            backend
                .shard_backend(&DfsMetadataShardId::new("root"))
                .expect("root shard")
                .get_object(&object.object_id)
                .expect("root object")
                .as_ref()
                .map(|object| object.kind.clone()),
            Some(DfsObjectKind::Tombstone),
            "source shard should retain an explicit tombstone for path-history replay"
        );
        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&object.path),
                &DfsMetadataShardId::new("services")
            ))
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_skips_stale_source_when_target_shard_already_has_object() {
        let root = temp_root("sharded_backend_skips_stale_source_target_object");
        let file_system = file_system();
        let mut stale = object(version("version_1"));
        stale.path = "/services/readme.md".to_string();
        let mut current = stale.clone();
        current.content_hash = Some("blake3:current-object".to_string());
        current.content_size_bytes = Some(7);
        let stale_version = DfsObjectVersionRecord {
            file_system_id: stale.file_system_id.clone(),
            object_id: stale.object_id.clone(),
            version: stale.version.clone(),
            content_hash: Some("blake3:stale-version".to_string()),
            size_bytes: Some(13),
            locations: stale.current_locations.clone(),
            created_by_event_id: stale.created_by_event_id.clone(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let current_version = DfsObjectVersionRecord {
            content_hash: current.content_hash.clone(),
            size_bytes: current.content_size_bytes,
            ..stale_version.clone()
        };
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&stale).expect("stale root object");
            root_only
                .put_version(&stale_version)
                .expect("stale root version");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        {
            let services = backend
                .shard_backend_mut(&DfsMetadataShardId::new("services"))
                .expect("services shard");
            services
                .put_file_system(&file_system)
                .expect("services filesystem");
            services
                .put_object(&current)
                .expect("current services object");
            services
                .put_version(&current_version)
                .expect("current services version");
        }

        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migration skips stale root copy");

        assert_eq!(
            report,
            DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 1,
                indexed_objects: 1,
                migrated_objects: 0,
                skipped_stale_sources: 1,
                reached_limit: false,
            }
        );
        assert_eq!(
            backend
                .get_object(&current.object_id)
                .expect("current object"),
            Some(current.clone())
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &current.path)
                .expect("current path"),
            Some(current.clone())
        );
        assert_eq!(
            backend
                .read_object_shard_index(&current.object_id)
                .expect("object index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&current.path),
                &DfsMetadataShardId::new("services")
            ))
        );
        assert_eq!(
            backend
                .get_version(&current.object_id, &current.version)
                .expect("current version"),
            Some(current_version)
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_bounds_configured_shard_migration() {
        let root = temp_root("sharded_backend_bounds_configured_shard_migration");
        let file_system = file_system();
        let mut first = object(version("version_1"));
        first.path = "/services/a.txt".to_string();
        let mut second = object(version("version_1"));
        second.object_id = DfsObjectId::from_legacy_named("obj_second").expect("object");
        second.path = "/users/b.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&first).expect("first root object");
            root_only.put_object(&second).expect("second root object");
        }

        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
                shard_config(
                    "users",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/users".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let report = backend
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 1)
            .expect("bounded migration");

        assert_eq!(
            report.scanned_objects, 1,
            "bounded migration should not scan the rest of the shard after reaching the migration limit"
        );
        assert_eq!(report.migrated_objects, 1);
        assert!(report.reached_limit);
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .filter(|object| object.kind != DfsObjectKind::Tombstone)
                .count(),
            2,
            "bounded migration must keep both live objects visible through the sharded object index"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_falls_back_to_source_path_during_shard_activation() {
        let root = temp_root("sharded_backend_activation_path_fallback");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.path = "/services/pre-migration.txt".to_string();
        {
            let mut root_only = LocalShardedMetadataBackend::new(
                &root,
                vec![shard_config("root", DfsMetadataShardAssignment::Root)],
            )
            .expect("root-only sharded backend");
            root_only.put_file_system(&file_system).expect("filesystem");
            root_only.put_object(&object).expect("root object");
        }

        let backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");

        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("activation fallback path"),
            Some(object),
            "publishing a new path-prefix shard before migration must not make existing root-shard paths disappear"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_ignores_index_for_unconfigured_local_shard() {
        let root = temp_root("sharded_backend_unconfigured_index");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/indexed-before-kv.txt".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("root object");
        backend
            .write_object_shard_index(&object, &DfsMetadataShardId::new("services"))
            .expect("stale object shard index");

        assert_eq!(
            backend.get_object(&object.object_id).expect("object by id"),
            Some(object.clone()),
            "an index that names an unopened shard must not break object-id reads"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("object by path"),
            Some(object),
            "an index that names an unopened shard must not hide source-shard path reads"
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_content_commit_creates_missing_object_shard_index() {
        let root = temp_root("sharded_backend_content_commit_new_index");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/content-commit.bin".to_string();
        object.content_hash = Some("blake3:content-commit".to_string());
        object.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-content-commit-new-index".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: object.current_locations.clone(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object.clone()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_content_commit_new_index")
                    .expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_file_system(&file_system).expect("filesystem");

        backend
            .put_object_version_intent_and_receipts(
                &object,
                &version_record,
                &intent,
                std::slice::from_ref(&receipt),
            )
            .expect("content commit batch");

        assert_eq!(
            backend
                .read_object_shard_index(&object.object_id)
                .expect("object shard index")
                .as_ref()
                .map(|index| (&index.path, &index.shard_id)),
            Some((
                &normalize_path(&object.path),
                &DfsMetadataShardId::new("services")
            )),
            "content commits for new objects should still publish a lookup index"
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &object.path)
                .expect("path lookup"),
            Some(object),
            "the relaxed index publish must leave path lookup correct"
        );

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn sharded_backend_content_commit_skips_matching_object_shard_index_rewrite() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("sharded_backend_content_commit_skip_index_rewrite");
        let file_system = file_system();
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let mut object = object(version("version_1"));
        object.path = "/services/existing-content.bin".to_string();
        object.content_hash = Some("blake3:old-content".to_string());
        object.content_size_bytes = Some(128);
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("initial object");

        let mut committed = object.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new-content".to_string());
        committed.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: committed.content_size_bytes,
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-content-commit-existing-index".to_string(),
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: committed.version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: committed.current_locations.clone(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: committed.content_hash.clone(),
            object_metadata: Some(committed.clone()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_content_commit_existing_index")
                    .expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let index_parent = backend
            .object_shard_index_path(&committed.object_id)
            .parent()
            .expect("index parent")
            .to_path_buf();
        let original_permissions = fs::metadata(&index_parent)
            .expect("index parent metadata")
            .permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o500);
        fs::set_permissions(&index_parent, read_only_permissions)
            .expect("make shard index directory read-only");

        let result = backend.put_object_version_intent_and_receipts(
            &committed,
            &version_record,
            &intent,
            std::slice::from_ref(&receipt),
        );

        fs::set_permissions(&index_parent, original_permissions)
            .expect("restore shard index directory permissions");
        result.expect("content commit should not rewrite an already-correct shard index");
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("object by id"),
            Some(committed),
            "content commit should still advance the object metadata"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_does_not_materialize_object_when_journal_append_fails() {
        let root = temp_root("local_backend_journal_failure_does_not_materialize_object");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        fs::remove_dir_all(root.join("journal").join("segments")).expect("remove segments dir");
        fs::write(root.join("journal").join("segments"), b"not a directory")
            .expect("block journal segment writes");

        let error = backend
            .put_object(&object)
            .expect_err("object write should fail before materialization when the journal fails");

        assert!(
            matches!(error, MetadataBackendError::Io { .. }),
            "journal append failure should be surfaced as I/O: {error:?}"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            None,
            "object metadata must not become visible without a journal entry"
        );
        assert!(
            !table_path_exists(&backend, &object.file_system_id, &object.path),
            "path index must not become visible without a journal entry"
        );

        fs::remove_file(root.join("journal").join("segments")).ok();
        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_entries_after_materialization_cursor() {
        let root = temp_root("local_backend_replays_after_materialization_cursor");
        let object = object(version("version_1"));
        let journaled_sequence = {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            let file_system = file_system();
            backend.put_file_system(&file_system).expect("filesystem");
            let entry = backend
                .write_journal(DfsMetadataMutation::PutObject {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    path: object.path.clone(),
                    object: Some(object.clone()),
                })
                .expect("journal object without materializing");
            assert_eq!(
                backend
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                None
            );
            entry.sequence
        };

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone()),
            "reopen should replay journaled metadata that was not yet materialized"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            journaled_sequence
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_stale_materialization_cursor_without_rewriting_table_log() {
        let root = temp_root("local_backend_stale_materialization_cursor_no_rewrite");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let entry = backend.put_object(&object).expect("object");
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (entry.sequence, table_len)
        };
        write_sobj_atomic(
            &root
                .join("records")
                .join("local-materialization")
                .join("cursor.sobj"),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 0,
            },
        )
        .expect("stale materialization cursor");

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object),
            "stale cursor recovery must preserve the materialized object"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent cursor recovery must not append duplicate table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_checkpoints_materialization_cursor_instead_of_every_write() {
        let root = temp_root("local_backend_deferred_materialization_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut expected_objects = Vec::new();
        let (last_sequence, table_len_before) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let mut last_sequence = 0;
            for index in 0..8 {
                let mut object = object(version("version_1"));
                object.object_id =
                    DfsObjectId::from_legacy_named(format!("obj_cursor_defer_{index}"))
                        .expect("object id");
                object.path = format!("/services/cursor-defer/{index}.txt");
                let entry = backend.put_object(&object).expect("object");
                last_sequence = entry.sequence;
                expected_objects.push(object);
            }
            let cursor =
                read_sobj::<DfsMetadataReplayCursor>(&backend.local_materialization_cursor_path())
                    .expect("materialization cursor");
            assert_eq!(
                cursor.last_sequence, 0,
                "short local write bursts should not force a cursor fsync for every metadata mutation"
            );
            let table_len = fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len();
            (last_sequence, table_len)
        };

        let reopened =
            LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen backend");

        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object),
                "reopen should recover objects whose materialization cursor was not checkpointed"
            );
        }
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            last_sequence,
            "reopen recovery should advance the cursor after idempotent replay"
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            table_len_before,
            "idempotent replay from a deferred cursor must not duplicate materialized table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_already_materialized_visible_commits_after_newer_head() {
        fn commit_test_version(
            backend: &mut LocalSobjMetadataBackend,
            previous: &DfsObjectMetadata,
            committed_version: &str,
            content_hash: &str,
            event_id: &str,
        ) -> (DfsObjectMetadata, DfsMetadataJournalEntry) {
            let replica_node_id = node("node_replica");
            let mut committed = previous.clone();
            committed.version = version(committed_version);
            committed.content_hash = Some(content_hash.to_string());
            committed.content_size_bytes = Some(content_hash.len() as u64);
            committed.current_locations = vec![replica_node_id.clone()];
            let visible_version = DfsObjectVersionRecord {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                version: committed.version.clone(),
                content_hash: committed.content_hash.clone(),
                size_bytes: committed.content_size_bytes,
                locations: committed.current_locations.clone(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            };
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: committed.file_system_id.clone(),
                    object_id: committed.object_id.clone(),
                    committed_version: committed.version.clone(),
                    owner_node_id: committed.owner_node_id.clone(),
                    event_id: EventId::from_legacy_named(event_id).expect("event"),
                },
                replica_node_id,
                content_hash: committed.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            };
            backend.put_receipt(&receipt).expect("receipt");
            let entry = backend
                .commit_visible_version(
                    &previous.version,
                    &committed,
                    &visible_version,
                    std::slice::from_ref(&receipt.replica_node_id),
                )
                .expect("commit visible version");
            (committed, entry)
        }

        let root = temp_root("local_backend_replays_chained_visible_commits_after_newer_head");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let (final_object, final_sequence) = {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let initial = object(version("version_1"));
            backend.put_object(&initial).expect("initial object");
            let (committed_v2, commit_v2) = commit_test_version(
                &mut backend,
                &initial,
                "version_2",
                "blake3:version-2",
                "evt_replay_commit_v2",
            );
            let (committed_v3, commit_v3) = commit_test_version(
                &mut backend,
                &committed_v2,
                "version_3",
                "blake3:version-3",
                "evt_replay_commit_v3",
            );

            write_sobj_atomic(
                &metadata_table_snapshot_path(&root),
                &DfsMetadataTableSnapshot {
                    format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                    materialized_through_sequence: None,
                    tables: Some(metadata_tables_snapshot(&backend.tables)),
                    ops: Vec::new(),
                },
            )
            .expect("legacy structured snapshot without materialized sequence");
            truncate_metadata_table_log(&root).expect("truncate table log");
            write_sobj_atomic(
                &backend.local_materialization_cursor_path(),
                &DfsMetadataReplayCursor {
                    shard_id: shard_id.clone(),
                    last_sequence: commit_v2.sequence.saturating_sub(1),
                },
            )
            .expect("stale materialization cursor before visible commits");
            (committed_v3, commit_v3.sequence)
        };

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&final_object.object_id)
                .expect("object lookup"),
            Some(final_object),
            "replay must not reject or roll back an already-materialized older visible commit"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(&reopened.local_materialization_cursor_path())
                .expect("materialization cursor")
                .last_sequence,
            final_sequence
        );
        assert_eq!(
            reopened.materialized_through_sequence, final_sequence,
            "legacy snapshots should learn the durable materialization sequence from replay"
        );
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            0,
            "idempotent replay should not rewrite table batches for already-materialized commits"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_open_does_not_rewrite_existing_metadata_table() {
        let root = temp_root("local_backend_open_preserves_metadata_table");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let table_path = metadata_table_path(&root);
        let before_len = fs::metadata(&table_path).expect("metadata table").len();

        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        let after_len = fs::metadata(&table_path)
            .expect("metadata table after reopen")
            .len();
        assert_eq!(
            before_len, after_len,
            "opening a healthy backend must not rewrite the materialized metadata table"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_compacts_materialized_table_log_to_snapshot() {
        let root = temp_root("local_backend_compacts_materialized_table_log");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut expected_objects = Vec::new();
        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_compacted_metadata_table_{index}"))
                    .expect("object");
            object.path = format!("/docs/compacted-{index}.md");
            backend.put_object(&object).expect("object");
            expected_objects.push(object);
        }
        let table_path = metadata_table_path(&root);
        assert!(
            fs::metadata(&table_path).expect("metadata table").len() > 0,
            "writes should first append to the materialized table log"
        );

        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact metadata table");

        assert!(
            metadata_table_snapshot_path(&root).is_file(),
            "compaction should persist a compact materialized table snapshot"
        );
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("metadata table snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "new compactions should use structured metadata table snapshots"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert_eq!(
            fs::metadata(&table_path)
                .expect("truncated metadata table")
                .len(),
            0,
            "compaction should truncate historical table ops after the snapshot is durable"
        );
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen compacted backend");
        for object in expected_objects {
            assert_eq!(
                reopened
                    .get_object(&object.object_id)
                    .expect("object lookup"),
                Some(object)
            );
        }
        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_truncates_torn_table_log_tail() {
        let root = temp_root("local_backend_truncates_torn_table_log");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object_a = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object_a).expect("object");
        }
        let table_path = metadata_table_path(&root);
        let good_len = fs::metadata(&table_path).expect("table log").len();
        assert!(good_len > 0);

        // Crash mid-append: a full length prefix promising more bytes than
        // the writer managed to flush.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&table_path)
            .expect("open table log");
        file.write_all(&512_u32.to_le_bytes())
            .and_then(|()| file.write_all(&[0xAB; 17]))
            .expect("torn append");
        drop(file);

        let mut object_b = object(version("version_2"));
        object_b.object_id =
            DfsObjectId::from_legacy_named("obj_after_crash").expect("valid object id");
        object_b.path = "docs/after-crash.md".to_string();
        {
            let mut reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &root)
                .expect("reopen tolerates torn tail");
            assert_eq!(
                reopened
                    .get_object(&object_a.object_id)
                    .expect("object lookup"),
                Some(object_a.clone()),
                "complete records before the torn tail must replay"
            );
            assert_eq!(
                fs::metadata(&table_path).expect("table log").len(),
                good_len,
                "open must cut the torn record off the log so later appends stay framed"
            );
            // The append that lands after the crash: without the truncation it
            // would be entombed behind the torn record and break every later
            // replay with an sobj framing error.
            reopened.put_object(&object_b).expect("post-crash append");
        }

        let reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen after post-crash append");
        assert_eq!(
            reopened
                .get_object(&object_a.object_id)
                .expect("object lookup"),
            Some(object_a)
        );
        assert_eq!(
            reopened
                .get_object(&object_b.object_id)
                .expect("object lookup"),
            Some(object_b)
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_truncates_invalid_table_log_tail() {
        let root = temp_root("local_backend_truncates_invalid_table_log_tail");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object_a = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object_a).expect("object");
        }
        let table_path = metadata_table_path(&root);
        let good_len = fs::metadata(&table_path).expect("table log").len();

        let invalid_payload = b"not-sobj";
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&table_path)
            .expect("open table log");
        file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
            .and_then(|()| file.write_all(invalid_payload))
            .expect("invalid tail append");
        drop(file);

        let reopened =
            LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen tolerates invalid tail");
        assert_eq!(
            reopened
                .get_object(&object_a.object_id)
                .expect("object lookup"),
            Some(object_a),
            "complete records before the malformed tail must replay"
        );
        assert_eq!(
            fs::metadata(&table_path).expect("table log").len(),
            good_len,
            "open must cut a fully length-prefixed but malformed tail before later appends"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_reports_corrupt_snapshot_path() {
        let root = temp_root("local_backend_reports_corrupt_snapshot_path");
        let snapshot_path = metadata_table_snapshot_path(&root);
        fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot parent");
        fs::write(&snapshot_path, [0x1f, 0x8b, 0x08]).expect("truncated snapshot");

        let error = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect_err("corrupt snapshot should fail to open");
        let message = error.to_string();
        assert!(
            message.contains(snapshot_path.to_string_lossy().as_ref()),
            "error should name the corrupt snapshot path: {message}"
        );
        assert!(
            message.contains("failed to decode"),
            "error should describe the decode failure: {message}"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_migrates_legacy_op_snapshot_to_structured_snapshot() {
        let root = temp_root("local_backend_migrates_legacy_op_snapshot");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let object = object(version("version_1"));
        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            backend.put_object(&object).expect("object");
            write_sobj_atomic(
                &metadata_table_snapshot_path(&root),
                &DfsMetadataTableSnapshot {
                    format_version: LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                    materialized_through_sequence: None,
                    tables: None,
                    ops: metadata_table_snapshot_ops(&backend.tables),
                },
            )
            .expect("legacy metadata table snapshot");
            truncate_metadata_table_log(&root).expect("truncate table log");
        }

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object)
        );
        let snapshot: DfsMetadataTableSnapshot =
            read_sobj(&metadata_table_snapshot_path(&root)).expect("migrated snapshot");
        assert_eq!(
            snapshot.format_version, METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            "opening a legacy op snapshot should migrate it to the structured snapshot format"
        );
        assert!(snapshot.tables.is_some());
        assert!(snapshot.ops.is_empty());
        assert_eq!(
            fs::metadata(metadata_table_path(&root))
                .expect("metadata table")
                .len(),
            0,
            "snapshot migration should compact any table tail into the structured image"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_allocates_journal_sequences_across_open_handles() {
        let root = temp_root("local_backend_allocates_journal_sequences_across_open_handles");
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let mut second =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("second backend");
        let file_system = file_system();
        let mut object = object(version("version_1"));
        object.object_id = DfsObjectId::from_legacy_named("obj_second_handle").expect("object");
        object.path = "/docs/second.md".to_string();

        let first_entry = first.put_file_system(&file_system).expect("filesystem");
        let second_entry = second
            .put_object(&object)
            .expect("object from second handle");

        assert_eq!(first_entry.sequence, 1);
        assert_eq!(
            second_entry.sequence, 2,
            "a second backend opened before the first write must not reuse the first sequence"
        );
        let journal = first.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 2);
        assert!(matches!(
            journal[0].mutation,
            DfsMetadataMutation::PutFileSystem { .. }
        ));
        assert!(matches!(
            journal[1].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));

        cleanup(&root);
    }

    #[test]
    fn local_backend_readers_see_table_batches_from_other_open_handles() {
        let root = temp_root("local_backend_readers_see_table_batches_from_other_open_handles");
        let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("first backend");
        let second = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("second backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        first.put_file_system(&file_system).expect("filesystem");
        first.put_object(&object).expect("object");

        assert_eq!(
            second
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem from second handle"),
            Some(file_system.clone())
        );
        assert_eq!(
            second
                .get_object(&object.object_id)
                .expect("object from second handle"),
            Some(object.clone())
        );
        assert_eq!(
            second
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects from second handle"),
            vec![object]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reloads_snapshot_when_other_handle_compacts_table_log() {
        let root = temp_root("local_backend_reloads_snapshot_after_external_compaction");
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        writer.put_file_system(&file_system()).expect("filesystem");
        persist_metadata_table_snapshot(
            &root,
            &writer.tables,
            Some(writer.materialized_through_sequence),
        )
        .expect("snapshot filesystem before table-log shrink");
        truncate_metadata_table_log(&root).expect("truncate table log after snapshot");
        drop(writer);
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend after snapshot");
        let mut logged_object = object(version("version_1"));
        logged_object.object_id =
            DfsObjectId::from_legacy_named("obj_before_table_log_shrink").expect("object");
        logged_object.path = "/docs/before-table-log-shrink.md".to_string();
        writer.put_object(&logged_object).expect("logged object");

        let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("stale backend");
        let stale_offset = stale.table_log_offset;
        assert!(
            stale_offset > 0,
            "stale handle needs an old table-log cursor"
        );

        for index in 0..8 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_pre_compact_{index}")).expect("object");
            object.path = format!("/docs/pre-compact-{index}.md");
            writer.put_object(&object).expect("pre compact object");
        }
        writer
            .compact_metadata_table_log_if_needed(1)
            .expect("force compaction");

        let mut index = 0usize;
        while fs::metadata(metadata_table_path(&root))
            .map(|metadata| metadata.len())
            .unwrap_or(0)
            <= stale_offset
        {
            let mut object = object(version("version_1"));
            object.object_id = DfsObjectId::from_legacy_named(format!("obj_post_compact_{index}"))
                .expect("object");
            object.path = format!("/docs/post-compact-{index}.md");
            writer.put_object(&object).expect("post compact object");
            index += 1;
        }

        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_stale_handle_after_compact").expect("object");
        object.path = "/docs/stale-handle-after-compact.md".to_string();
        stale
            .put_object(&object)
            .expect("stale handle should refresh from the new snapshot before writing");
        assert_eq!(
            stale
                .get_object(&object.object_id)
                .expect("object lookup after stale write"),
            Some(object)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reloads_when_table_log_shrinks_below_cached_offset() {
        let root = temp_root("local_backend_reloads_after_external_table_log_truncation");
        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        let file_system = file_system();
        writer.put_file_system(&file_system).expect("filesystem");

        let mut stale = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("stale backend");
        let stale_offset = stale.table_log_offset;
        assert!(
            stale_offset > 1,
            "stale handle needs a cached table-log cursor"
        );

        let table_path = metadata_table_path(&root);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&table_path)
            .expect("open table log");
        file.set_len(stale_offset - 1)
            .and_then(|()| file.sync_all())
            .expect("shrink table log below cached cursor");

        stale
            .current_tables_snapshot()
            .expect("lock-free read should reload instead of seeking past EOF");
        stale
            .put_file_system(&file_system)
            .expect("stale writer should reload after table log shrink");
        assert_eq!(
            stale
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem lookup after stale write"),
            Some(file_system)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_serializes_concurrent_metadata_writers() {
        let root = temp_root("local_backend_serializes_concurrent_metadata_writers");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let writer_count = 8usize;
        let writes_per_writer = 16usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(writer_count));
        let mut handles = Vec::new();
        for writer_index in 0..writer_count {
            let root = root.to_path_buf();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let mut backend =
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("backend");
                barrier.wait();
                for object_index in 0..writes_per_writer {
                    let mut object = object(version("version_1"));
                    object.object_id = DfsObjectId::from_legacy_named(format!(
                        "obj_concurrent_{writer_index}_{object_index}"
                    ))
                    .expect("object");
                    object.path = format!("/docs/concurrent-{writer_index}-{object_index}.md");
                    backend.put_object(&object).expect("object");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        let journal = backend.journal_since(0).expect("journal");
        assert_eq!(journal.len(), 1 + writer_count * writes_per_writer);
        let sequences = journal
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            sequences,
            (1..=journal.len() as u64).collect::<Vec<_>>(),
            "concurrent metadata writers must allocate contiguous journal sequences"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .len(),
            writer_count * writes_per_writer,
            "concurrent materialization must not lose object indexes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_readers_never_observe_torn_metadata_during_writes() {
        let root = temp_root("local_backend_readers_during_writes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        drop(backend);

        let total_writes = 256usize;
        let reader_count = 4usize;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Start readers and the writer together so the reads genuinely overlap the
        // appends rather than racing thread startup.
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(reader_count + 1));

        // Readers use their own backend handle and read continuously while a writer
        // appends. The read path (`current_tables_snapshot`) is lock-free, so this
        // exercises its torn-tail safety: every read must succeed and never observe
        // more objects than the writer has committed.
        let mut readers = Vec::new();
        for _ in 0..reader_count {
            let root = root.to_path_buf();
            let stop = stop.clone();
            let barrier = barrier.clone();
            readers.push(std::thread::spawn(move || {
                let backend =
                    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                        .expect("reader backend");
                barrier.wait();
                // Read at least once, then keep reading until the writer signals stop.
                let mut reads = 0u64;
                loop {
                    let objects = backend
                        .objects_for_file_system(&file_system_id())
                        .expect("read must never fail on a torn tail");
                    assert!(
                        objects.len() <= total_writes,
                        "reader saw {} objects, more than the {total_writes} ever written",
                        objects.len()
                    );
                    reads += 1;
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
                reads
            }));
        }

        let mut writer =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("writer backend");
        barrier.wait();
        for i in 0..total_writes {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_reader_race_{i}")).expect("object id");
            object.path = format!("/docs/reader-race-{i}.md");
            writer.put_object(&object).expect("write object");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        for reader in readers {
            let reads = reader.join().expect("reader thread");
            assert!(reads > 0, "reader must have completed at least one read");
        }

        let backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("final objects")
                .len(),
            total_writes,
            "every committed write must be visible after the race"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_appends_journal_entries_to_segment_files() {
        let root = temp_root("local_backend_appends_journal_entries_to_segment_files");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));

        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");

        let head = read_sobj::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))
            .expect("journal head");
        assert_eq!(
            head.next_sequence, 1,
            "local appends should leave the journal head as a recovery cache instead of rewriting it on every mutation"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "new journal entries should be appended to a segment file"
        );
        assert!(
            !root
                .join("journal")
                .join("00000000000000000001.sobj")
                .exists(),
            "new journal entries should not create one object file per mutation"
        );
        assert_eq!(backend.journal_since(0).expect("journal").len(), 2);
        assert_eq!(backend.journal_since(1).expect("journal").len(), 1);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(reopened.journal_since(0).expect("journal").len(), 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_uses_head_and_cursor_without_scanning_consumed_segments() {
        let root = temp_root("local_backend_reopen_uses_head_and_cursor");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact table snapshot");

        let high_sequence = JOURNAL_SEGMENT_ENTRY_LIMIT + 1;
        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: shard_id.clone(),
                sequence: high_sequence,
                created_at: OffsetDateTime::now_utc(),
                mutation: DfsMetadataMutation::PutFileSystem {
                    file_system_id: file_system.file_system_id.clone(),
                    file_system: Some(file_system.clone()),
                },
            },
        )
        .expect("high segment entry");
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead {
                next_sequence: high_sequence + 1,
            },
        )
        .expect("journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: high_sequence,
            },
        )
        .expect("local cursor");
        fs::write(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog"),
            [1_u8, 0, 0, 0, 0xff],
        )
        .expect("corrupt consumed segment");
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("reopen should not scan consumed journal segments");
        assert_eq!(
            reopened.get_object(&object.object_id).expect("object"),
            Some(object)
        );
        assert!(
            reopened
                .journal_since(high_sequence)
                .expect("journal after head")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_uses_matching_head_and_cursor_for_corrupt_current_segment() {
        let root = temp_root("local_backend_reopen_with_corrupt_current_segment");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let file_system = file_system();
        let object = object(version("version_1"));
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .compact_metadata_table_log_if_needed(1)
            .expect("compact table snapshot");
        write_sobj_atomic(
            &root.join("journal").join("head.sobj"),
            &DfsMetadataJournalHead { next_sequence: 3 },
        )
        .expect("journal head");
        write_sobj_atomic(
            &backend.local_materialization_cursor_path(),
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 2,
            },
        )
        .expect("local cursor");
        fs::write(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog"),
            [1_u8, 0, 0, 0, 0xff],
        )
        .expect("corrupt consumed current segment");
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root)
            .expect("reopen should trust matching head and cursor");
        assert_eq!(
            reopened.get_object(&object.object_id).expect("object"),
            Some(object)
        );
        assert!(
            reopened
                .journal_since(2)
                .expect("journal after cursor")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_bounded_journal_batches");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        for index in 0..5 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/bounded-{index}.md");
            backend.put_object(&object).expect("object");
        }

        let first_batch = backend
            .journal_since_limited(0, 3)
            .expect("first bounded batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let second_batch = backend
            .journal_since_limited(3, 3)
            .expect("second bounded batch");
        assert_eq!(
            second_batch
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(
            backend
                .journal_since_limited(6, 3)
                .expect("empty bounded batch")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_journal_reader_ignores_invalid_tail_record() {
        let root = temp_root("local_backend_journal_reader_ignores_invalid_tail_record");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let expected = backend.journal_since(0).expect("journal before tail");

        let segment_path = root
            .join("journal")
            .join("segments")
            .join("00000000000000000001.slog");
        let invalid_payload = b"not-sobj";
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .expect("open segment");
        file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
            .and_then(|()| file.write_all(invalid_payload))
            .expect("invalid tail append");
        drop(file);

        assert_eq!(
            backend
                .journal_since(0)
                .expect("journal after invalid tail"),
            expected,
            "journal polling must ignore a malformed trailing record and retry it later"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_journal_reader_waits_for_writer_lock() {
        let root = temp_root("local_backend_journal_reader_waits_for_writer_lock");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        let expected = backend.journal_since(0).expect("journal before tail");
        let segment_path = root
            .join("journal")
            .join("segments")
            .join("00000000000000000001.slog");
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let root_for_writer = root.to_path_buf();
        let segment_for_writer = segment_path.clone();
        let writer = std::thread::spawn(move || {
            let writer_backend = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("shard-root"),
                &root_for_writer,
            )
            .expect("writer backend");
            let _lock = writer_backend.metadata_write_lock().expect("writer lock");
            locked_tx.send(()).expect("signal lock acquired");
            std::thread::sleep(Duration::from_millis(100));
            let invalid_payload = b"not-sobj";
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&segment_for_writer)
                .expect("open segment");
            file.write_all(&(invalid_payload.len() as u32).to_le_bytes())
                .and_then(|()| file.write_all(invalid_payload))
                .expect("invalid tail append");
        });
        locked_rx.recv().expect("writer acquired lock");

        let started = std::time::Instant::now();
        assert_eq!(
            backend
                .journal_since(0)
                .expect("journal reader waits for lock"),
            expected,
            "journal polling must not read while a writer is appending a record"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "journal reader should wait for the in-flight metadata writer"
        );
        writer.join().expect("writer thread");
        cleanup(&root);
    }

    #[test]
    fn local_backend_open_waits_for_writer_lock_before_table_replay() {
        let root = temp_root("local_backend_open_waits_for_writer_lock");
        let mut seed = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("seed backend");
        seed.put_file_system(&file_system()).expect("filesystem");
        drop(seed);

        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_open_waits_for_writer_lock").expect("object");
        object.path = "/docs/open-waits-for-writer-lock.md".to_string();
        let object_id = object.object_id.clone();
        let expected_object = object.clone();
        let root_for_writer = root.to_path_buf();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let mut writer_backend = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("shard-root"),
                root_for_writer,
            )
            .expect("writer backend");
            let _lock = writer_backend.metadata_write_lock().expect("writer lock");
            locked_tx.send(()).expect("signal lock acquired");
            std::thread::sleep(Duration::from_millis(100));
            writer_backend
                .append_and_apply_table_ops(
                    writer_backend.next_sequence.max(1),
                    vec![DfsMetadataTableOp::PutObject { object }],
                )
                .expect("append table batch while lock is held");
        });
        locked_rx.recv().expect("writer acquired lock");

        let started = std::time::Instant::now();
        let opened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("opener backend");
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "backend open must wait for in-flight metadata writers before table replay"
        );
        assert_eq!(
            opened
                .get_object(&object_id)
                .expect("object lookup")
                .as_ref(),
            Some(&expected_object),
            "backend open must replay the complete table batch after the writer releases the lock"
        );

        writer.join().expect("writer thread");
        cleanup(&root);
    }

    #[test]
    fn local_backend_reads_byte_bounded_journal_batches() {
        let root = temp_root("local_backend_reads_byte_bounded_journal_batches");
        let shard_id = DfsMetadataShardId::new("shard-root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        for index in 0..3 {
            let mut object = object(version("version_1"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_byte_bounded_journal_{index}"))
                    .expect("object");
            object.path = format!("/docs/{}/bounded-{index}.md", "wide".repeat(128));
            backend.put_object(&object).expect("object");
        }

        let first_unbounded = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, usize::MAX)
            .expect("unbounded batch")
            .expect("batch");
        let first_entry = first_unbounded.entries[0].clone();
        let one_entry_limit = metadata_journal_batch_encoded_len(&DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            next_after_sequence: first_entry.sequence,
            entries: vec![first_entry],
        })
        .expect("encoded length");

        let first_batch = backend
            .journal_batch_since_limited(
                1,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                one_entry_limit,
            )
            .expect("byte bounded batch")
            .expect("batch");
        assert_eq!(
            first_batch
                .entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(first_batch.next_after_sequence, 2);
        assert!(
            metadata_journal_batch_encoded_len(&first_batch).expect("encoded length")
                <= one_entry_limit
        );

        let oversized_first = backend
            .journal_batch_since_limited(1, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, 1)
            .expect("oversized first batch")
            .expect("batch");
        assert_eq!(oversized_first.entries.len(), 1);
        assert_eq!(oversized_first.next_after_sequence, 2);

        cleanup(&root);
    }

    #[test]
    fn local_backend_migrates_legacy_journal_files_to_segments() {
        let root = temp_root("local_backend_migrates_legacy_journal_files_to_segments");
        fs::create_dir_all(root.join("journal")).expect("journal dir");
        let legacy_path = root.join("journal").join("00000000000000000001.sobj");
        let legacy_entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-root"),
            sequence: 1,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: Some(file_system()),
            },
        };
        write_sobj_atomic(&legacy_path, &legacy_entry).expect("legacy journal entry");

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        assert!(
            !legacy_path.exists(),
            "opening the backend should migrate legacy root journal files out of the live path"
        );
        assert!(
            root.join("journal")
                .join("segments")
                .join("00000000000000000001.slog")
                .is_file(),
            "legacy journal entries should be appended to the segmented journal"
        );
        assert_eq!(
            backend.journal_since(0).expect("journal"),
            vec![legacy_entry]
        );
        let next = backend.put_file_system(&file_system()).expect("filesystem");
        assert_eq!(
            next.sequence, 2,
            "sequence allocation should continue after migrated legacy entries"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_hydrates_legacy_journal_payloads_from_records() {
        let root = temp_root("local_backend_hydrates_legacy_journal_payloads_from_records");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        backend
            .put_object(&object(version("version_1")))
            .expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_1"),
                content_hash: Some("blake3:old".to_string()),
                size_bytes: None,
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version");
        let mut committed_object = object(version("version_2"));
        committed_object.content_hash = Some("blake3:new".to_string());
        committed_object.content_size_bytes = Some(128);
        let visible_version = DfsObjectVersionRecord {
            file_system_id: file_system_id(),
            object_id: object_id(),
            version: version("version_2"),
            content_hash: committed_object.content_hash.clone(),
            size_bytes: committed_object.content_size_bytes,
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend
            .commit_visible_version(
                &version("version_1"),
                &committed_object,
                &visible_version,
                &[],
            )
            .expect("commit visible version");
        let mut transaction_object = object(version("version_namespace_tx"));
        transaction_object.object_id =
            DfsObjectId::from_legacy_named("obj_hydrated_namespace_tx").expect("object");
        transaction_object.path = "docs/transaction.md".to_string();
        transaction_object.content_hash = None;
        transaction_object.content_size_bytes = None;
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_tx_hydrate_legacy".to_string(),
            file_system_id: file_system_id(),
            objects: vec![transaction_object],
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("namespace transaction");

        let legacy_entries = backend
            .journal_since(0)
            .expect("journal")
            .into_iter()
            .map(|mut entry| {
                match &mut entry.mutation {
                    DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                        *file_system = None;
                    }
                    DfsMetadataMutation::PutObject { object, .. } => {
                        *object = None;
                    }
                    DfsMetadataMutation::PutVersion { version_record, .. } => {
                        *version_record = None;
                    }
                    DfsMetadataMutation::CommitVisibleVersion {
                        committed_object,
                        visible_version,
                        ..
                    } => {
                        *committed_object = None;
                        *visible_version = None;
                    }
                    DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                        *transaction = None;
                    }
                    _ => {}
                }
                entry
            })
            .collect::<Vec<_>>();

        let hydrated = backend
            .hydrate_journal_entry_payloads(legacy_entries)
            .expect("hydrate legacy entries");
        assert!(matches!(
            &hydrated[0].mutation,
            DfsMetadataMutation::PutFileSystem {
                file_system: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[1].mutation,
            DfsMetadataMutation::PutObject {
                object: Some(_),
                ..
            }
        ));
        assert!(matches!(
            &hydrated[2].mutation,
            DfsMetadataMutation::PutVersion {
                version_record: Some(_),
                ..
            }
        ));
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object: Some(object),
                visible_version: Some(version_record),
                ..
            } if object.version == version("version_2")
                && version_record.version == version("version_2")
        )));
        assert!(hydrated.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } if transaction.transaction_id == "namespace_tx_hydrate_legacy"
        )));

        cleanup(&root);
    }

    #[test]
    fn local_backend_syncs_metadata_directories() {
        let root = temp_root("local_backend_syncs_metadata_directories");
        fs::create_dir_all(&root).expect("root dir");

        sync_dir(&root).expect("directory sync should work on supported host filesystems");

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_recovers_next_sequence_from_segment_tail() {
        let root = temp_root("local_backend_reopen_recovers_next_sequence_from_segment_tail");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        append_segmented_journal_entry(
            &root,
            &DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutObject {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    path: "docs/readme.md".to_string(),
                    object: None,
                },
            },
        )
        .expect("append segment entry without advancing head");

        let mut reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("reopen");
        let mut object = object(version("version_1"));
        object.object_id =
            DfsObjectId::from_legacy_named("obj_after_tail_recovery").expect("object");
        object.path = "docs/after-tail-recovery.md".to_string();
        let entry = reopened.put_object(&object).expect("object");
        assert_eq!(
            entry.sequence, 3,
            "reopen must scan segment tails so a crash after append but before head update cannot reuse a sequence"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_directory_children_from_dentry_index() {
        let root = temp_root("local_backend_lists_directory_children_from_dentry_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut root_dir = object(version("version_1"));
        root_dir.object_id = DfsObjectId::from_legacy_named("obj_root_dir").expect("object");
        root_dir.path = "/".to_string();
        root_dir.kind = DfsObjectKind::Directory;
        root_dir.content_hash = None;
        root_dir.content_size_bytes = None;
        let mut docs_dir = object(version("version_1"));
        docs_dir.object_id = DfsObjectId::from_legacy_named("obj_docs_dir").expect("object");
        docs_dir.path = "/docs".to_string();
        docs_dir.kind = DfsObjectKind::Directory;
        docs_dir.content_hash = None;
        docs_dir.content_size_bytes = None;
        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut nested = object(version("version_1"));
        nested.object_id = DfsObjectId::from_legacy_named("obj_setup").expect("object");
        nested.path = "/docs/guides/setup.md".to_string();
        let mut outside = object(version("version_1"));
        outside.object_id = DfsObjectId::from_legacy_named("obj_lib").expect("object");
        outside.path = "/src/lib.rs".to_string();
        for object in [&root_dir, &docs_dir, &readme, &changelog, &nested, &outside] {
            backend.put_object(object).expect("object");
        }
        let mut changelog_tombstone = changelog.clone();
        changelog_tombstone.kind = DfsObjectKind::Tombstone;
        changelog_tombstone.version = version("version_2");
        changelog_tombstone.content_hash = None;
        backend.put_object(&changelog_tombstone).expect("tombstone");

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        let root_children = backend
            .child_objects_for_directory(&file_system_id(), "/")
            .expect("root children");
        assert_eq!(
            root_children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        let root_page = backend
            .child_objects_for_directory_page(&file_system_id(), "/", None, 10)
            .expect("root page");
        assert_eq!(
            root_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_routes_new_large_directory_entries_to_hash_fragments() {
        let root = temp_root("local_backend_routes_new_large_directory_entries_to_hash_fragments");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut existing = object(version("version_1"));
        existing.path = "/docs/existing.md".to_string();
        backend.put_object(&existing).expect("existing object");
        let existing_record = table_path_record(&backend, &existing.file_system_id, &existing.path);
        assert_eq!(existing_record.dirfrag_id, DfsDirFragmentId::new("root"));

        let mut root_dirfrag = table_dirfrag(
            &backend,
            &existing_record.parent_inode_id,
            &DfsDirFragmentId::new("root"),
        );
        root_dirfrag.entry_count = root_dirfrag.split_after_count;
        backend
            .append_and_apply_table_ops(
                backend.next_sequence,
                vec![DfsMetadataTableOp::PutDirFragment {
                    dirfrag: root_dirfrag,
                }],
            )
            .expect("force split threshold");

        let mut bucketed = object(version("version_1"));
        bucketed.object_id = DfsObjectId::from_legacy_named("obj_bucketed").expect("object");
        bucketed.path = "/docs/bucketed.md".to_string();
        backend.put_object(&bucketed).expect("bucketed object");

        let bucketed_record = table_path_record(&backend, &bucketed.file_system_id, &bucketed.path);
        assert_ne!(
            bucketed_record.dirfrag_id,
            DfsDirFragmentId::new("root"),
            "large directories should route new entries away from the root dentry fragment"
        );
        assert!(
            table_dentry_exists(&backend, &bucketed_record),
            "bucketed dentry record should be materialized in its selected fragment"
        );

        let children = backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children");
        assert_eq!(
            children
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/bucketed.md", "/docs/existing.md"],
            "directory listing must merge root and bucket fragments"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_filesystem_objects_and_directory_children() {
        let root = temp_root("local_backend_pages_objects_and_children");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut dir = object(version("version_1"));
        dir.object_id = DfsObjectId::from_legacy_named("obj_page_dir").expect("dir object");
        dir.path = "/paged".to_string();
        dir.kind = DfsObjectKind::Directory;
        dir.content_hash = None;
        dir.content_size_bytes = None;
        backend.put_object(&dir).expect("dir");

        for index in 0..5 {
            let mut file = object(version("version_1"));
            file.object_id =
                DfsObjectId::from_legacy_named(format!("obj_page_file_{index}")).expect("object");
            file.path = format!("/paged/file-{index}.txt");
            file.kind = DfsObjectKind::File;
            backend.put_object(&file).expect("file");
        }

        let first_object_page = backend
            .objects_for_file_system_page(&file_system_id(), None, 3)
            .expect("first object page");
        assert_eq!(first_object_page.objects.len(), 3);
        assert!(first_object_page.next_cursor.is_some());
        let second_object_page = backend
            .objects_for_file_system_page(
                &file_system_id(),
                first_object_page.next_cursor.as_ref(),
                3,
            )
            .expect("second object page");
        assert_eq!(second_object_page.objects.len(), 3);
        assert!(second_object_page.next_cursor.is_none());
        assert_eq!(
            backend
                .count_objects_for_file_system(&file_system_id())
                .expect("count objects"),
            6
        );

        let mut cursor = None;
        let mut child_names = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .child_objects_for_directory_page(&file_system_id(), "/paged", cursor.as_ref(), 2)
                .expect("child page");
            assert!(page.objects.len() <= 2);
            child_names.extend(
                page.objects
                    .iter()
                    .map(|object| object.path.rsplit('/').next().unwrap().to_string()),
            );
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        child_names.sort();
        assert_eq!(
            child_names,
            vec![
                "file-0.txt",
                "file-1.txt",
                "file-2.txt",
                "file-3.txt",
                "file-4.txt"
            ]
        );
        assert_eq!(pages, 3);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_namespace_jobs() {
        let root = temp_root("local_backend_pages_namespace_jobs");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        for index in 0..5 {
            backend
                .put_namespace_job(&namespace_job(index, &format!("/jobs/{index}")))
                .expect("namespace job");
        }

        let empty = backend
            .list_namespace_jobs_page(None, 0)
            .expect("empty page");
        assert!(empty.jobs.is_empty());
        assert!(empty.next_cursor.is_none());

        let mut cursor = None;
        let mut job_ids = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = backend
                .list_namespace_jobs_page(cursor.as_ref(), 2)
                .expect("job page");
            assert!(page.jobs.len() <= 2);
            job_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            job_ids,
            (0..5)
                .map(|index| format!("job_namespace_page_{index:02}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(pages, 3);
        assert_eq!(backend.list_namespace_jobs().expect("legacy list").len(), 5);

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_object_versions_receipts_and_intents() {
        let root = temp_root("local_backend_pages_object_local_records");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");

        for index in 0..5 {
            let version_record = DfsObjectVersionRecord {
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                version: version(&format!("version_{index}")),
                content_hash: Some(format!("blake3:version-{index}")),
                size_bytes: Some(index),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::now_utc(),
                visible: index == 0,
            };
            backend.put_version(&version_record).expect("version");
        }

        let mut version_cursor = None;
        let mut paged_versions = Vec::new();
        loop {
            let page = backend
                .versions_for_object_page(&file.object_id, version_cursor.as_ref(), 2)
                .expect("version page");
            assert!(page.versions.len() <= 2);
            paged_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_versions,
            backend
                .versions_for_object(&file.object_id)
                .expect("full versions")
                .into_iter()
                .map(|version| version.version)
                .collect::<Vec<_>>()
        );
        let mut version_scan_cursor = None;
        let mut scanned_versions = Vec::new();
        loop {
            let page = backend
                .version_records_page(version_scan_cursor.as_ref(), 2)
                .expect("version scan page");
            assert!(page.versions.len() <= 2);
            scanned_versions.extend(page.versions.into_iter().map(|version| version.version));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            version_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_versions, paged_versions);

        for index in 0..5 {
            let receipt = DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: version("version_0"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::generate(),
                },
                replica_node_id: node(&format!("node_replica_{index}")),
                content_hash: Some("blake3:version-0".to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            backend.put_receipt(&receipt).expect("receipt");
        }

        let mut receipt_cursor = None;
        let mut paged_receipts = Vec::new();
        loop {
            let page = backend
                .receipts_for_version_page(
                    &file.object_id,
                    &version("version_0"),
                    receipt_cursor.as_ref(),
                    2,
                )
                .expect("receipt page");
            assert!(page.receipts.len() <= 2);
            paged_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_receipts,
            backend
                .receipts_for_version(&file.object_id, &version("version_0"))
                .expect("full receipts")
                .into_iter()
                .map(|receipt| receipt.replica_node_id)
                .collect::<Vec<_>>()
        );
        let mut receipt_scan_cursor = None;
        let mut scanned_receipts = Vec::new();
        loop {
            let page = backend
                .receipt_records_page(receipt_scan_cursor.as_ref(), 2)
                .expect("receipt scan page");
            assert!(page.receipts.len() <= 2);
            scanned_receipts.extend(
                page.receipts
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id),
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_scan_cursor = Some(next_cursor);
        }
        assert_eq!(scanned_receipts, paged_receipts);

        for index in 0..5 {
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version: version(&format!("version_{}", index + 1)),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::now_utc(),
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let mut intent_cursor = None;
        let mut paged_intents = Vec::new();
        loop {
            let page = backend
                .intents_for_object_page(&file.object_id, intent_cursor.as_ref(), 2)
                .expect("intent page");
            assert!(page.intents.len() <= 2);
            paged_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        assert_eq!(
            paged_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );

        let mut scan_cursor = None;
        let mut scanned_intents = Vec::new();
        loop {
            let page = backend
                .write_intents_page(scan_cursor.as_ref(), 2)
                .expect("intent scan page");
            assert!(page.intents.len() <= 2);
            scanned_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            scan_cursor = Some(next_cursor);
        }
        assert_eq!(
            scanned_intents,
            (0..5)
                .map(|index| format!("intent-{index}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            backend
                .all_intents()
                .expect("all intents")
                .into_iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
            scanned_intents
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_pages_intents_by_target_version_after_reopen() {
        let root = temp_root("local_backend_pages_intents_by_target_version");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let file = object(version("version_0"));
        backend.put_object(&file).expect("object");
        for index in 0..6 {
            let target_version = if index % 2 == 0 {
                version("version_even")
            } else {
                version("version_odd")
            };
            let intent = DfsWriteIntentRecord {
                intent_id: format!("intent-target-{index}"),
                file_system_id: file.file_system_id.clone(),
                object_id: file.object_id.clone(),
                base_version: Some(version("version_0")),
                target_version,
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner")],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::ReceiptsPending,
                content_hash: Some(format!("blake3:intent-target-{index}")),
                object_metadata: Some(file.clone()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: None,
            };
            backend.put_intent(&intent).expect("intent");
        }

        let first_even_page = backend
            .intents_for_object_version_page(&file.object_id, &version("version_even"), None, 2)
            .expect("first even intent page");
        assert_eq!(
            first_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-0", "intent-target-2"]
        );
        assert!(first_even_page.next_cursor.is_some());
        let second_even_page = backend
            .intents_for_object_version_page(
                &file.object_id,
                &version("version_even"),
                first_even_page.next_cursor.as_ref(),
                2,
            )
            .expect("second even intent page");
        assert_eq!(
            second_even_page
                .intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-target-4"]
        );
        assert!(second_even_page.next_cursor.is_none());

        drop(backend);
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");
        let mut cursor = None;
        let mut odd_intents = Vec::new();
        loop {
            let page = reopened
                .intents_for_object_version_page(
                    &file.object_id,
                    &version("version_odd"),
                    cursor.as_ref(),
                    1,
                )
                .expect("odd intent page");
            assert!(page.intents.len() <= 1);
            odd_intents.extend(page.intents.into_iter().map(|intent| intent.intent_id));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        assert_eq!(
            odd_intents,
            vec![
                "intent-target-1".to_string(),
                "intent-target-3".to_string(),
                "intent-target-5".to_string()
            ]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_content_update_does_not_rewrite_stable_namespace_indexes() {
        let root = temp_root("local_backend_content_update_skips_stable_indexes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let path_record = table_path_record(&backend, &readme.file_system_id, &readme.path);
        let filesystem_object_record = backend
            .tables
            .file_system_objects
            .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
            .cloned()
            .expect("filesystem object index");
        let dentry_record = backend
            .tables
            .dentries
            .get(&dentry_key(&path_record))
            .cloned()
            .expect("dentry index");
        let dirfrag = table_dirfrag(
            &backend,
            &path_record.parent_inode_id,
            &path_record.dirfrag_id,
        );
        let object_before = backend
            .tables
            .objects
            .get(&readme.object_id)
            .cloned()
            .expect("object record");
        let inode_before = backend
            .tables
            .inodes
            .get(&path_record.inode_id)
            .cloned()
            .expect("inode record");

        readme.content_hash = Some("blake3:new-content".to_string());
        readme.version = version("version_2");
        backend.put_object(&readme).expect("content update");

        assert_eq!(
            backend
                .tables
                .file_system_objects
                .get(&(readme.file_system_id.clone(), readme.object_id.clone()))
                .cloned(),
            Some(filesystem_object_record),
            "content-only updates should keep the filesystem-object namespace index stable"
        );
        assert_eq!(
            table_path_record(&backend, &readme.file_system_id, &readme.path),
            path_record,
            "content-only updates should keep the path namespace index stable"
        );
        assert_eq!(
            backend
                .tables
                .dentries
                .get(&dentry_key(&path_record))
                .cloned(),
            Some(dentry_record),
            "content-only updates should keep the dentry namespace index stable"
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            ),
            dirfrag,
            "content-only updates should not change directory fragment accounting"
        );
        assert_ne!(
            backend.tables.objects.get(&readme.object_id).cloned(),
            Some(object_before),
            "the primary object record should still update when content metadata changes"
        );
        assert_ne!(
            backend.tables.inodes.get(&path_record.inode_id).cloned(),
            Some(inode_before),
            "the inode record should still update when visible object metadata changes"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_and_dentry_indexes() {
        let root = temp_root("local_backend_removes_stale_path_and_dentry_indexes");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        backend.put_object(&readme).expect("object");
        let old_path_record = path_record_for_object(&readme);
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/readme.md"
        ));
        assert!(table_dentry_exists(&backend, &old_path_record));
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.path = "/docs/renamed.md".to_string();
        readme.version = version("version_2");
        backend.put_object(&readme).expect("renamed object");
        assert!(
            !table_path_exists(&backend, &file_system_id(), "/docs/readme.md"),
            "renaming should remove the obsolete path index"
        );
        assert!(
            !table_dentry_exists(&backend, &old_path_record),
            "renaming should remove the obsolete directory entry"
        );
        assert!(table_path_exists(
            &backend,
            &file_system_id(),
            "/docs/renamed.md"
        ));
        let renamed_path_record = path_record_for_object(&readme);
        assert!(table_dentry_exists(&backend, &renamed_path_record));
        assert_eq!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/renamed.md"]
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            1
        );

        readme.kind = DfsObjectKind::Tombstone;
        readme.content_hash = None;
        readme.version = version("version_3");
        backend.put_object(&readme).expect("tombstone object");
        assert!(
            !table_dentry_exists(&backend, &renamed_path_record),
            "tombstoning should remove the visible directory entry"
        );
        assert!(
            table_path_exists(&backend, &file_system_id(), "/docs/renamed.md"),
            "tombstone path record should remain addressable"
        );
        assert!(
            backend
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children after tombstone")
                .is_empty()
        );
        assert_eq!(
            table_dirfrag(
                &backend,
                &old_path_record.parent_inode_id,
                &DfsDirFragmentId::new("root")
            )
            .entry_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_compacts_retention_eligible_tombstones_to_path_markers() {
        let root = temp_root("local_backend_compacts_tombstones_to_markers");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        tombstone.changed_at_unix_nanos = 10;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");

        let dry_run = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::dry_run(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("dry run");
        assert_eq!(dry_run.scanned_tombstone_count, 1);
        assert_eq!(dry_run.eligible_tombstone_count, 1);
        assert_eq!(dry_run.compacted_tombstone_count, 0);
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("dry-run object"),
            Some(tombstone.clone())
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 1);
        assert_eq!(report.eligible_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 1);
        assert_eq!(report.removed_object_record_count, 1);
        assert_eq!(report.removed_inode_record_count, 1);
        assert_eq!(report.removed_file_system_object_record_count, 1);
        assert_eq!(report.preserved_path_marker_count, 1);
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            "/docs/deleted.md"
        ));
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("compacted object"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .is_empty(),
            "compacted tombstones should not hydrate through object scans"
        );

        drop(backend);
        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");
        assert_eq!(
            reopened
                .get_object(&tombstone.object_id)
                .expect("reopened object"),
            None
        );
        assert_eq!(
            reopened
                .get_object_by_path(&file_system.file_system_id, "/docs/deleted.md")
                .expect("reopened path")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );
        assert!(
            reopened
                .objects_for_file_system(&file_system.file_system_id)
                .expect("reopened objects")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_tombstone_compaction_retains_young_and_referenced_tombstones() {
        let root = temp_root("local_backend_tombstone_compaction_retains");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");

        let mut young = object(version("version_2"));
        young.path = "/docs/young.md".to_string();
        young.kind = DfsObjectKind::Tombstone;
        young.content_hash = None;
        young.changed_at_unix_nanos = 90;
        backend.put_object(&young).expect("young tombstone");

        let mut referenced = object(version("version_3"));
        referenced.object_id =
            DfsObjectId::from_legacy_named("obj_referenced_tombstone").expect("object id");
        referenced.path = "/docs/referenced.md".to_string();
        referenced.kind = DfsObjectKind::Tombstone;
        referenced.content_hash = None;
        referenced.changed_at_unix_nanos = 1;
        backend
            .put_object(&referenced)
            .expect("referenced tombstone");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: referenced.file_system_id.clone(),
                object_id: referenced.object_id.clone(),
                version: referenced.version.clone(),
                content_hash: None,
                size_bytes: None,
                locations: Vec::new(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: false,
            })
            .expect("version ref");

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.scanned_tombstone_count, 2);
        assert_eq!(report.eligible_tombstone_count, 0);
        assert_eq!(report.retained_young_tombstone_count, 1);
        assert_eq!(report.retained_referenced_tombstone_count, 1);
        assert_eq!(report.compacted_tombstone_count, 0);
        assert_eq!(
            backend.get_object(&young.object_id).expect("young object"),
            Some(young)
        );
        assert_eq!(
            backend
                .get_object(&referenced.object_id)
                .expect("referenced object"),
            Some(referenced)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recreating_compacted_tombstone_path_removes_marker() {
        let root = temp_root("local_backend_recreate_compacted_tombstone_path");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/docs/recreate.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");

        let mut replacement = object(version("version_1"));
        replacement.object_id =
            DfsObjectId::from_legacy_named("obj_recreated_path").expect("object id");
        replacement.path = tombstone.path.clone();
        replacement.kind = DfsObjectKind::File;
        replacement.content_hash = Some("blake3:new".to_string());
        backend.put_object(&replacement).expect("replacement");

        assert!(backend.tables.compacted_tombstones.is_empty());
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &replacement.path)
                .expect("path lookup"),
            Some(replacement.clone())
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("objects")
                .into_iter()
                .map(|object| object.object_id)
                .collect::<Vec<_>>(),
            vec![replacement.object_id]
        );

        cleanup(&root);
    }

    #[test]
    fn sharded_backend_compacts_tombstone_and_removes_object_index() {
        let root = temp_root("sharded_backend_compacts_tombstone");
        let mut backend = LocalShardedMetadataBackend::new(
            &root,
            vec![
                shard_config("root", DfsMetadataShardAssignment::Root),
                shard_config(
                    "services",
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    },
                ),
            ],
        )
        .expect("sharded backend");
        let file_system = file_system();
        let mut tombstone = object(version("version_2"));
        tombstone.path = "/services/deleted.md".to_string();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.content_hash = None;
        tombstone.changed_at_unix_nanos = 1;
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&tombstone).expect("tombstone");
        assert!(
            backend
                .object_shard_index_path(&tombstone.object_id)
                .is_file()
        );

        let report = backend
            .compact_tombstones(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(0))
                    .with_now_unix_nanos(100),
            )
            .expect("compact");
        assert_eq!(report.compacted_tombstone_count, 1);
        assert!(
            !backend
                .object_shard_index_path(&tombstone.object_id)
                .exists(),
            "compaction should remove stale object shard routing indexes"
        );
        assert_eq!(
            backend
                .get_object(&tombstone.object_id)
                .expect("object lookup"),
            None
        );
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/deleted.md")
                .expect("path lookup")
                .expect("compacted marker")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_rebuilds_dirfrag_counts_during_table_replay() {
        let root = temp_root("local_backend_rebuilds_dirfrag_counts");
        {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let mut readme = object(version("version_1"));
            readme.path = "/docs/readme.md".to_string();
            let mut changelog = object(version("version_1"));
            changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
            changelog.path = "/docs/changelog.md".to_string();
            for object in [&readme, &changelog] {
                backend.put_object(object).expect("object");
            }

            let path_record = path_record_for_object(&readme);
            assert_eq!(
                table_dirfrag(
                    &backend,
                    &path_record.parent_inode_id,
                    &path_record.dirfrag_id
                )
                .entry_count,
                2
            );
            force_table_replay_from_journal(&backend, &root, 0);
        }

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        let path_record = path_record_for_object(&object(version("version_1")));
        assert_eq!(
            table_dirfrag(
                &recovered,
                &path_record.parent_inode_id,
                &path_record.dirfrag_id
            )
            .entry_count,
            2
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_objects_from_filesystem_object_index() {
        let root = temp_root("local_backend_lists_objects_from_filesystem_object_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut readme = object(version("version_1"));
        readme.path = "/docs/readme.md".to_string();
        let mut changelog = object(version("version_1"));
        changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
        changelog.path = "/docs/changelog.md".to_string();
        let mut other = object(version("version_1"));
        other.object_id = DfsObjectId::from_legacy_named("obj_other").expect("object");
        other.file_system_id = other_file_system.file_system_id.clone();
        other.path = "/other.txt".to_string();
        for object in [&readme, &changelog, &other] {
            backend.put_object(object).expect("object");
        }
        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &readme.object_id
        ));

        let objects = backend
            .objects_for_file_system(&file_system_id())
            .expect("objects");
        assert_eq!(
            objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/changelog.md", "/docs/readme.md"]
        );

        assert!(table_file_system_object_exists(
            &backend,
            &file_system_id(),
            &changelog.object_id
        ));
        changelog.file_system_id = other_file_system.file_system_id.clone();
        changelog.path = "/moved-changelog.md".to_string();
        backend.put_object(&changelog).expect("move filesystem");
        assert!(
            !table_file_system_object_exists(&backend, &file_system_id(), &changelog.object_id),
            "moving an object to another filesystem should remove its old filesystem-object index"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.md"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_path_index_when_object_moves_filesystems_without_renaming() {
        let root =
            temp_root("local_backend_removes_stale_path_index_when_object_moves_filesystems");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let other_file_system = DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
            display_name: "other".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend
            .put_file_system(&other_file_system)
            .expect("other filesystem");

        let mut object = object(version("version_1"));
        object.path = "/services/shared-name.txt".to_string();
        backend.put_object(&object).expect("object");
        assert!(table_path_exists(
            &backend,
            &file_system.file_system_id,
            &object.path
        ));

        object.file_system_id = other_file_system.file_system_id.clone();
        backend
            .put_object(&object)
            .expect("move filesystem without rename");

        assert!(
            !table_path_exists(&backend, &file_system.file_system_id, &object.path),
            "moving an object between filesystems must remove the old filesystem path index even when the path string is unchanged"
        );
        assert!(
            backend
                .get_object_by_path(&file_system.file_system_id, "/services/shared-name.txt")
                .expect("old filesystem lookup")
                .is_none()
        );
        assert_eq!(
            backend
                .get_object_by_path(
                    &other_file_system.file_system_id,
                    "/services/shared-name.txt"
                )
                .expect("new filesystem lookup")
                .expect("object")
                .object_id,
            object.object_id
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_secondary_indexes_for_legacy_object_records() {
        let root = temp_root("local_backend_recovers_secondary_indexes_for_legacy_object_records");
        {
            let mut backend =
                LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                    .expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");

            let target = object(version("version_1"));
            let mut alias = object(version("version_1"));
            alias.object_id = DfsObjectId::from_legacy_named("obj_legacy_alias").expect("object");
            alias.path = "/docs/readme.alias".to_string();
            alias.kind = DfsObjectKind::Hardlink;
            alias.content_hash = None;
            alias.link_target_object_id = Some(target.object_id.clone());
            for object in [&target, &alias] {
                backend.put_object(object).expect("object");
            }
        }

        for index_dir in ["filesystem-objects", "paths", "dentries", "hardlinks"] {
            fs::remove_dir_all(root.join("records").join(index_dir)).ok();
        }

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .objects_for_file_system(&file_system_id())
                .expect("objects")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .child_objects_for_directory(&file_system_id(), "/docs")
                .expect("children")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias", "docs/readme.md"]
        );
        assert_eq!(
            recovered
                .hardlinks_to_object(&file_system_id(), &object_id())
                .expect("hardlinks")
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_lists_hardlinks_from_target_index() {
        let root = temp_root("local_backend_lists_hardlinks_from_target_index");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target = object(version("version_1"));
        let mut alias_a = object(version("version_1"));
        alias_a.object_id = DfsObjectId::from_legacy_named("obj_alias_a").expect("object");
        alias_a.path = "/docs/readme.alias-a".to_string();
        alias_a.kind = DfsObjectKind::Hardlink;
        alias_a.content_hash = None;
        alias_a.link_target_object_id = Some(target.object_id.clone());
        let mut alias_b = alias_a.clone();
        alias_b.object_id = DfsObjectId::from_legacy_named("obj_alias_b").expect("object");
        alias_b.path = "/docs/readme.alias-b".to_string();
        let mut alias_c = alias_a.clone();
        alias_c.object_id = DfsObjectId::from_legacy_named("obj_alias_c").expect("object");
        alias_c.path = "/archive/readme.alias-c".to_string();
        let mut unrelated = object(version("version_1"));
        unrelated.object_id = DfsObjectId::from_legacy_named("obj_unrelated").expect("object");
        unrelated.path = "/docs/unrelated".to_string();

        for object in [&target, &alias_b, &unrelated, &alias_a, &alias_c] {
            backend.put_object(object).expect("object");
        }
        let mut stale_alias = alias_b.clone();
        stale_alias.kind = DfsObjectKind::Tombstone;
        stale_alias.link_target_object_id = None;
        backend
            .put_object(&stale_alias)
            .expect("tombstone stale alias");

        let hardlinks = backend
            .hardlinks_to_object(&file_system_id(), &target.object_id)
            .expect("hardlinks");
        assert_eq!(
            hardlinks
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/readme.alias-c", "/docs/readme.alias-a"]
        );
        assert_eq!(
            backend
                .hardlink_count_to_object(&file_system_id(), &target.object_id)
                .expect("hardlink count"),
            2
        );
        assert_eq!(
            backend
                .first_hardlink_to_object(&file_system_id(), &target.object_id)
                .expect("first hardlink")
                .expect("first hardlink")
                .path,
            "/archive/readme.alias-c"
        );
        assert_eq!(
            backend
                .first_hardlink_to_object_outside_prefix(
                    &file_system_id(),
                    &target.object_id,
                    "/archive",
                )
                .expect("outside prefix")
                .expect("outside prefix")
                .path,
            "/docs/readme.alias-a"
        );

        let first_page = backend
            .hardlinks_to_object_page(&file_system_id(), &target.object_id, None, 1)
            .expect("first page");
        assert_eq!(
            first_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/readme.alias-c"]
        );
        let second_page = backend
            .hardlinks_to_object_page(
                &file_system_id(),
                &target.object_id,
                first_page.next_cursor.as_ref(),
                1,
            )
            .expect("second page");
        assert_eq!(
            second_page
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/readme.alias-a"]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_removes_stale_hardlink_target_index_entries() {
        let root = temp_root("local_backend_removes_stale_hardlink_target_index_entries");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");

        let target_a = object(version("version_1"));
        let mut target_b = object(version("version_1"));
        target_b.object_id = DfsObjectId::from_legacy_named("obj_target_b").expect("object");
        target_b.path = "/docs/target-b.md".to_string();
        let mut alias = object(version("version_1"));
        alias.object_id = DfsObjectId::from_legacy_named("obj_alias").expect("object");
        alias.path = "/docs/readme.alias".to_string();
        alias.kind = DfsObjectKind::Hardlink;
        alias.content_hash = None;
        alias.link_target_object_id = Some(target_a.object_id.clone());

        for object in [&target_a, &target_b, &alias] {
            backend.put_object(object).expect("object");
        }
        assert!(table_hardlink_exists(
            &backend,
            &target_a.object_id,
            &alias.object_id
        ));

        alias.link_target_object_id = Some(target_b.object_id.clone());
        backend.put_object(&alias).expect("retarget alias");
        assert!(
            !table_hardlink_exists(&backend, &target_a.object_id, &alias.object_id),
            "retargeting a hardlink should remove its old target index entry"
        );
        assert!(table_hardlink_exists(
            &backend,
            &target_b.object_id,
            &alias.object_id
        ));

        alias.kind = DfsObjectKind::Tombstone;
        alias.link_target_object_id = None;
        backend.put_object(&alias).expect("tombstone alias");
        assert!(
            !table_hardlink_exists(&backend, &target_b.object_id, &alias.object_id),
            "tombstoning a hardlink should remove its target index entry"
        );
        assert!(
            backend
                .hardlinks_to_object(&file_system_id(), &target_b.object_id)
                .expect("hardlinks")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_intents_and_polls_subscription_batches() {
        let root = temp_root("local_backend_persists_intents_and_polls_subscription_batches");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let intent = DfsWriteIntentRecord {
            intent_id: "dfs_intent_1".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            base_version: Some(version("version_1")),
            target_version: version("version_2"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:new".to_string()),
            object_metadata: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let entry = backend.put_intent(&intent).expect("intent");
        assert_eq!(entry.sequence, 1);
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutIntent {
                status: DfsWriteIntentStatus::ReceiptsPending,
                intent: Some(ref journal_intent),
                ..
            } if journal_intent == &intent
        ));
        assert_eq!(
            backend
                .get_intent(&intent.object_id, &intent.intent_id)
                .expect("get intent"),
            Some(intent)
        );

        let mut subscription = backend.subscribe(0);
        let first_batch = backend
            .poll_subscription(&mut subscription)
            .expect("first batch");
        assert_eq!(first_batch.entries.len(), 1);
        assert_eq!(first_batch.next_after_sequence, 1);
        assert_eq!(subscription.next_after_sequence, 1);

        let second_batch = backend
            .poll_subscription(&mut subscription)
            .expect("second batch");
        assert!(second_batch.entries.is_empty());
        assert_eq!(second_batch.next_after_sequence, 1);

        fs::write(
            root.join("journal").join("00000000000000000001.sobj"),
            b"not sobj",
        )
        .expect("corrupt already-consumed journal entry");
        let third_batch = backend
            .poll_subscription(&mut subscription)
            .expect("already-consumed corrupted journal entry is not reparsed");
        assert!(third_batch.entries.is_empty());
        assert_eq!(third_batch.next_after_sequence, 1);

        cleanup(&root);
    }

    #[test]
    fn commit_visible_version_requires_expected_version_and_receipts() {
        let root = temp_root("commit_visible_version_requires_expected_version_and_receipts");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert!(matches!(
            backend.commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            ),
            Err(MetadataBackendError::Conflict { .. })
        ));

        backend.put_receipt(&receipt).expect("receipt");
        let table_batches_before_commit = metadata_table_batch_count(&root);
        let commit = backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");
        assert!(matches!(
            commit.mutation,
            DfsMetadataMutation::CommitVisibleVersion { .. }
        ));
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before_commit + 1,
            "one visible-version journal entry should materialize into one table batch"
        );
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );
        assert_eq!(
            backend
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipts")
                .len(),
            1
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_journal_batch_into_follower_backend() {
        let leader_root = temp_root("local_backend_replays_journal_batch_into_follower_leader");
        let follower_root = temp_root("local_backend_replays_journal_batch_into_follower_follower");
        let mut leader =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-leader"), &leader_root)
                .expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("shard-follower"),
            &follower_root,
        )
        .expect("follower backend");

        let file_system = file_system();
        let initial = object(version("version_1"));
        leader.put_file_system(&file_system).expect("filesystem");
        leader.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let visible_version = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_replay_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        leader.put_receipt(&receipt).expect("receipt");
        leader
            .commit_visible_version(
                &initial.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        let mut subscription = leader.subscribe(0);
        let mut batch = leader
            .poll_subscription(&mut subscription)
            .expect("journal batch");
        assert_eq!(batch.entries.len(), 4);
        let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 64;
        for (offset, entry) in batch.entries.iter_mut().enumerate() {
            entry.sequence = source_sequence_base + offset as u64;
        }
        batch.next_after_sequence = batch
            .entries
            .last()
            .expect("non-empty replay batch")
            .sequence;

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("apply journal batch"),
            4
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal"),
            batch.entries,
            "replicated metadata replay should persist source journal entries instead of appending follower-local replacements"
        );
        assert_eq!(
            follower
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem"),
            Some(file_system)
        );
        assert_eq!(
            follower
                .get_object(&committed.object_id)
                .expect("object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            follower
                .get_version(&committed.object_id, &committed.version)
                .expect("version"),
            Some(normalized_version_record(&visible_version))
        );
        assert_eq!(
            follower
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipt"),
            vec![receipt]
        );

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("reapply journal batch"),
            0
        );
        assert_eq!(
            follower.journal_since(0).expect("follower journal reapply"),
            batch.entries,
            "idempotent replays must not append duplicate follower journal entries"
        );
        assert_eq!(
            read_sobj::<DfsMetadataReplayCursor>(
                &follower.replay_cursor_path(&DfsMetadataShardId::new("shard-leader"))
            )
            .expect("replay cursor")
            .last_sequence,
            batch.next_after_sequence,
            "a replicated batch should persist the source cursor once at the batch boundary"
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata shard status")
            .pop()
            .expect("follower shard status");
        assert_eq!(
            status.journal_last_sequence, batch.next_after_sequence,
            "a replayed source batch must advance the local journal head to the source sequence"
        );
        assert_eq!(
            status.journal_next_sequence,
            batch.next_after_sequence.saturating_add(1)
        );
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: DfsMetadataShardId::new("shard-leader"),
                replayed_through_sequence: batch.next_after_sequence,
            }]
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_replays_authoritative_commit_without_receipt_entry() {
        let leader_root = temp_root("local_backend_replays_authoritative_commit_leader");
        let follower_root = temp_root("local_backend_replays_authoritative_commit_follower");
        let mut leader =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-leader"), &leader_root)
                .expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("shard-follower"),
            &follower_root,
        )
        .expect("follower backend");

        let file_system = file_system();
        let initial = object(version("version_1"));
        leader.put_file_system(&file_system).expect("filesystem");
        leader.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let visible_version = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_missing_receipt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        leader.put_receipt(&receipt).expect("receipt");
        leader
            .commit_visible_version(
                &initial.version,
                &committed,
                &visible_version,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        let mut subscription = leader.subscribe(0);
        let mut batch = leader
            .poll_subscription(&mut subscription)
            .expect("journal batch");
        batch
            .entries
            .retain(|entry| !matches!(entry.mutation, DfsMetadataMutation::PutReceipt { .. }));
        assert_eq!(batch.entries.len(), 3);
        let source_sequence_base = JOURNAL_SEGMENT_ENTRY_LIMIT + 128;
        for (offset, entry) in batch.entries.iter_mut().enumerate() {
            entry.sequence = source_sequence_base + offset as u64;
        }
        batch.next_after_sequence = batch
            .entries
            .last()
            .expect("non-empty replay batch")
            .sequence;

        assert_eq!(
            follower
                .apply_journal_batch(&batch)
                .expect("apply commit without receipt"),
            3
        );
        assert_eq!(
            follower
                .get_object(&committed.object_id)
                .expect("object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            follower
                .get_version(&committed.object_id, &committed.version)
                .expect("version"),
            Some(normalized_version_record(&visible_version))
        );
        assert!(
            follower
                .receipts_for_version(&committed.object_id, &committed.version)
                .expect("receipts")
                .is_empty(),
            "the commit entry alone should make the write visible; receipts may replay later"
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_replaces_divergent_replica_with_replicated_snapshot() {
        let leader_root = temp_root("local_backend_snapshot_repair_leader");
        let follower_root = temp_root("local_backend_snapshot_repair_follower");
        let shard_id = DfsMetadataShardId::new("root");
        let mut leader =
            LocalSobjMetadataBackend::new(shard_id.clone(), &leader_root).expect("leader backend");
        let mut follower = LocalSobjMetadataBackend::new(shard_id.clone(), &follower_root)
            .expect("follower backend");

        let leader_file_system = file_system();
        leader
            .put_file_system(&leader_file_system)
            .expect("leader filesystem");
        let mut follower_file_system = leader_file_system.clone();
        follower_file_system.display_name = "diverged".to_string();
        follower
            .put_file_system(&follower_file_system)
            .expect("follower filesystem");
        let mut follower_object = object(version("follower-v1"));
        follower_object.content_hash = Some("blake3:follower-diverged".to_string());
        follower
            .put_object(&follower_object)
            .expect("follower object");
        assert!(
            follower
                .apply_journal_batch(
                    &leader
                        .journal_batch_since_limited(0, 1, usize::MAX)
                        .expect("leader batch")
                        .expect("leader batch")
                )
                .expect_err("conflicting sequence should reject before snapshot repair")
                .to_string()
                .contains("already exists with a different entry")
        );

        let archived = follower
            .replace_with_replicated_snapshot(
                shard_id.clone(),
                leader.journal_last_sequence(),
                leader.export_table_snapshot(),
            )
            .expect("replace snapshot");
        assert!(archived);
        assert_eq!(
            follower
                .get_file_system(&leader_file_system.file_system_id)
                .expect("follower filesystem"),
            Some(leader_file_system)
        );
        assert!(
            follower_root.join("replica-repair-archives").exists(),
            "divergent replica state should be archived before replacement"
        );
        let archive_dir = read_dir(&follower_root.join("replica-repair-archives"))
            .expect("repair archives")
            .into_iter()
            .next()
            .expect("repair archive")
            .path();
        assert!(
            archive_dir.join("journal").join("segments").exists(),
            "journal segments should be archived without moving the live journal lock directory"
        );
        assert!(
            follower_root.join("journal").join("write.lock").exists(),
            "snapshot repair must keep the live journal directory in place for Windows"
        );
        let leader_object = object(version("leader-v1"));
        leader.put_object(&leader_object).expect("leader object");
        follower
            .apply_journal_batch(
                &leader
                    .journal_batch_since_limited(1, 1, usize::MAX)
                    .expect("leader object batch")
                    .expect("leader object batch"),
            )
            .expect("repaired follower should accept future primary sequences");
        assert_eq!(
            follower
                .get_object(&leader_object.object_id)
                .expect("follower object"),
            Some(leader_object)
        );
        let status = follower
            .metadata_shard_statuses()
            .expect("metadata status")
            .pop()
            .expect("status");
        assert_eq!(status.journal_last_sequence, leader.journal_last_sequence());
        assert_eq!(
            status.replay_cursors,
            vec![DfsMetadataReplayCursorStatus {
                source_shard_id: shard_id.clone(),
                replayed_through_sequence: leader.journal_last_sequence(),
            }]
        );

        cleanup(&leader_root);
        cleanup(&follower_root);
    }

    #[test]
    fn local_backend_persists_metadata_replication_cursor_monotonically() {
        let root = temp_root("local_backend_metadata_replication_cursor");
        let target_node_id = node("node_cursor_target");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
        assert_eq!(
            backend
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("initial cursor"),
            0
        );

        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 42)
            .expect("write cursor");
        let cursor_path = backend.metadata_replication_cursor_path(&target_node_id, &shard_id);
        let metadata_before_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
        let modified_before_stale_ack = metadata_before_stale_ack
            .modified()
            .expect("cursor modified time");
        std::thread::sleep(Duration::from_millis(25));
        backend
            .put_metadata_replication_cursor(&target_node_id, &shard_id, 7)
            .expect("stale cursor");
        let metadata_after_stale_ack = fs::metadata(&cursor_path).expect("cursor metadata");
        assert_eq!(
            metadata_after_stale_ack.len(),
            metadata_before_stale_ack.len(),
            "stale acknowledgements should not rewrite the durable replication cursor"
        );
        assert_eq!(
            metadata_after_stale_ack
                .modified()
                .expect("cursor modified time"),
            modified_before_stale_ack,
            "stale acknowledgements should leave the durable replication cursor untouched"
        );
        let reopened =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("reopen");

        assert_eq!(
            reopened
                .metadata_replication_cursor(&target_node_id, &shard_id)
                .expect("reopened cursor"),
            42,
            "older acknowledgements must not move a durable replication cursor backwards"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_replays_semantically_duplicate_receipt_as_idempotent() {
        let root = temp_root("local_backend_duplicate_receipt_semantic_idempotent");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let object = object(version("version_1"));
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_receipt_a").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        let mut replayed = receipt.clone();
        replayed.receipt.event_id = EventId::from_legacy_named("evt_receipt_b").expect("event");
        replayed.received_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        let batch = DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("shard-root"),
            entries: vec![DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("shard-root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::PutReceipt {
                    file_system_id: file_system.file_system_id,
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    replica_node_id: receipt.replica_node_id.clone(),
                    receipt: Some(replayed),
                },
            }],
            next_after_sequence: 2,
        };

        assert_eq!(
            backend
                .apply_journal_batch(&batch)
                .expect("duplicate receipt replay"),
            0
        );
        assert_eq!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .expect("receipts"),
            vec![receipt]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_object_locations_from_visible_version() {
        let root = temp_root("local_backend_recovers_object_locations_from_visible_version");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");
        let mut object = object(version("version_2"));
        object.current_locations = vec![owner.clone()];
        object.content_hash = Some("blake3:recover-locations".to_string());
        backend.put_object(&object).expect("object");
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(18),
            locations: vec![owner.clone(), replica.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen");

        assert_eq!(
            reopened
                .get_object(&object.object_id)
                .expect("object")
                .expect("object")
                .current_locations,
            vec![owner, replica]
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_rejects_replay_entries_without_record_payloads() {
        let root = temp_root("local_backend_rejects_replay_entries_without_record_payloads");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let entry = DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("shard-leader"),
            sequence: 7,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: file_system_id(),
                file_system: None,
            },
        };

        let error = backend
            .apply_journal_entry(&entry)
            .expect_err("missing payload should fail replay");
        assert!(
            error
                .to_string()
                .contains("cannot replay put-file-system without its record payload")
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_normalizes_replica_locations_at_persistence_boundary() {
        let root = temp_root("local_backend_normalizes_replica_locations");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let owner = node("node_owner");
        let replica = node("node_replica");
        backend.put_file_system(&file_system()).expect("filesystem");

        let mut object = object(version("version_1"));
        object.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        backend.put_object(&object).expect("object");
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("get object")
                .expect("object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );

        let mut version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: Some(3),
            locations: vec![owner.clone(), replica.clone(), owner.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        backend.put_version(&version_record).expect("version");
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("get version")
                .expect("version")
                .locations,
            vec![owner.clone(), replica.clone()]
        );

        let version_2 = version("version_2");
        let mut committed = object.clone();
        committed.version = version_2.clone();
        committed.current_locations = vec![owner.clone(), replica.clone(), owner.clone()];
        version_record.version = version_2;
        version_record.locations = committed.current_locations.clone();
        for node_id in [&owner, &replica] {
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: committed.file_system_id.clone(),
                        object_id: committed.object_id.clone(),
                        committed_version: committed.version.clone(),
                        owner_node_id: owner.clone(),
                        event_id: EventId::from_legacy_named(&format!("evt_{node_id}"))
                            .expect("event"),
                    },
                    replica_node_id: node_id.clone(),
                    content_hash: committed.content_hash.clone(),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("receipt");
        }
        backend
            .commit_visible_version(
                &object.version,
                &committed,
                &version_record,
                &[owner.clone(), replica.clone(), owner.clone()],
            )
            .expect("visible commit");
        assert_eq!(
            backend
                .get_object(&committed.object_id)
                .expect("get committed object")
                .expect("committed object")
                .current_locations,
            vec![owner.clone(), replica.clone()]
        );
        assert_eq!(
            backend
                .get_version(&committed.object_id, &committed.version)
                .expect("get committed version")
                .expect("committed version")
                .locations,
            vec![owner, replica]
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_visible_version_commit() {
        let root = temp_root("local_backend_recovers_interrupted_visible_version_commit");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut committed = initial.clone();
        committed.version = version("version_2");
        committed.content_hash = Some("blake3:new".to_string());
        committed.current_locations.push(node("node_replica"));
        let version_record = DfsObjectVersionRecord {
            file_system_id: committed.file_system_id.clone(),
            object_id: committed.object_id.clone(),
            version: committed.version.clone(),
            content_hash: committed.content_hash.clone(),
            size_bytes: Some(3),
            locations: committed.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: committed.file_system_id.clone(),
                object_id: committed.object_id.clone(),
                committed_version: committed.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit").expect("event"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &committed,
                &version_record,
                std::slice::from_ref(&receipt.replica_node_id),
            )
            .expect("commit");

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&committed.object_id)
                .expect("get object")
                .expect("object")
                .version,
            committed.version
        );
        assert_eq!(
            recovered
                .get_version(&committed.object_id, &committed.version)
                .expect("get version"),
            Some(version_record)
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects() {
        let root = temp_root(
            "local_backend_recovers_latest_visible_version_without_resurrecting_deleted_objects",
        );
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let initial = object(version("version_1"));
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&initial).expect("object");

        let mut version_2 = initial.clone();
        version_2.version = version("version_2");
        version_2.content_hash = Some("blake3:two".to_string());
        let version_2_record = DfsObjectVersionRecord {
            file_system_id: version_2.file_system_id.clone(),
            object_id: version_2.object_id.clone(),
            version: version_2.version.clone(),
            content_hash: version_2.content_hash.clone(),
            size_bytes: Some(3),
            locations: version_2.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let version_2_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_2.file_system_id.clone(),
                object_id: version_2.object_id.clone(),
                committed_version: version_2.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_two").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_2.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_2_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &initial.version,
                &version_2,
                &version_2_record,
                std::slice::from_ref(&version_2_receipt.replica_node_id),
            )
            .expect("commit version 2");

        let mut version_3 = version_2.clone();
        version_3.version = version("version_3");
        version_3.content_hash = Some("blake3:three".to_string());
        let version_3_record = DfsObjectVersionRecord {
            file_system_id: version_3.file_system_id.clone(),
            object_id: version_3.object_id.clone(),
            version: version_3.version.clone(),
            content_hash: version_3.content_hash.clone(),
            size_bytes: Some(5),
            locations: version_3.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            visible: true,
        };
        let version_3_receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: version_3.file_system_id.clone(),
                object_id: version_3.object_id.clone(),
                committed_version: version_3.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named("evt_commit_three").expect("event"),
            },
            replica_node_id: node("node_owner"),
            content_hash: version_3.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend.put_receipt(&version_3_receipt).expect("receipt");
        backend
            .commit_visible_version(
                &version_2.version,
                &version_3,
                &version_3_record,
                std::slice::from_ref(&version_3_receipt.replica_node_id),
            )
            .expect("commit version 3");

        backend.put_object(&version_2).expect("stale object");
        drop(backend);
        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&version_3.object_id)
                .expect("get recovered")
                .expect("object")
                .version,
            version_3.version
        );
        drop(recovered);

        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let mut tombstone = version_3.clone();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.version = version("version_4");
        tombstone.content_hash = None;
        tombstone.content_size_bytes = None;
        backend.put_object(&tombstone).expect("tombstone");
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object(&tombstone.object_id)
                .expect("get tombstone")
                .expect("object")
                .kind,
            DfsObjectKind::Tombstone
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_recovers_interrupted_namespace_transaction() {
        let root = temp_root("local_backend_recovers_interrupted_namespace_transaction");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source.clone(), destination.clone()],
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        assert_eq!(
            metadata_table_batch_count(&root),
            4,
            "namespace transactions should materialize all table mutations for one journal entry in one table batch"
        );

        force_table_replay_from_journal(&backend, &root, 0);
        drop(backend);

        let recovered = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("recovered backend");
        assert_eq!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/current.html")
                .expect("lookup current")
                .expect("current object")
                .object_id,
            source.object_id
        );
        assert!(
            recovered
                .get_object_by_path(&file_system_id(), "/services/web/index.html")
                .expect("lookup old")
                .is_none()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_reopen_does_not_rejournal_healthy_namespace_transactions() {
        let root = temp_root("local_backend_reopen_does_not_rejournal_namespace_transactions");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        let mut source = object(version("version_1"));
        source.path = "/services/web/index.html".to_string();
        let mut destination = object(version("version_1"));
        destination.object_id = DfsObjectId::from_legacy_named("obj_current").expect("object");
        destination.path = "/services/web/current.html".to_string();
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&source).expect("source");
        backend.put_object(&destination).expect("destination");

        source.path = "/services/web/current.html".to_string();
        destination.kind = DfsObjectKind::Tombstone;
        destination.version = version("version_2");
        let transaction = DfsNamespaceTransaction {
            transaction_id: "namespace_test_rename".to_string(),
            file_system_id: file_system_id(),
            objects: vec![source, destination],
            versions: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        backend
            .apply_namespace_transaction(&transaction)
            .expect("transaction");
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");

        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "opening a healthy backend must not append duplicate namespace transaction journal entries"
        );
        cleanup(&root);
    }

    #[test]
    fn local_backend_put_object_and_version_uses_one_table_batch_without_namespace_transaction() {
        let root = temp_root("local_backend_object_version_grouped_table_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/grouped.bin".to_string();
        object.content_hash = Some("blake3:grouped-object-version".to_string());
        object.content_size_bytes = Some(4096);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        let entries = backend
            .put_object_and_version(&object, &version_record)
            .expect("grouped object version");

        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].mutation,
            DfsMetadataMutation::PutObject { .. }
        ));
        assert!(matches!(
            entries[1].mutation,
            DfsMetadataMutation::PutVersion { .. }
        ));
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "object and visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert!(backend.tables.namespace_transactions.is_empty());
        let journal_len = backend.journal_since(0).expect("journal").len();
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopened backend");
        assert_eq!(
            reopened.journal_since(0).expect("journal").len(),
            journal_len,
            "grouped object/version write must not create a recoverable namespace transaction"
        );
        cleanup(&root);
    }

    #[test]
    fn namespace_transaction_publishes_object_and_version_in_one_table_batch() {
        let root = temp_root("namespace_transaction_object_version_single_batch");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = file_system();
        backend.put_file_system(&file_system).expect("filesystem");
        let mut object = object(version("version_2"));
        object.path = "/services/web/app.bin".to_string();
        object.content_hash = Some("blake3:namespace-version".to_string());
        object.content_size_bytes = Some(16 * 1024 * 1024 * 1024);
        let version_record = DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let table_batches_before = metadata_table_batch_count(&root);

        backend
            .apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: "namespace_object_version_single_batch".to_string(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: vec![version_record.clone()],
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("transaction");

        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_before + 1,
            "creating an object and its visible version should materialize in one table batch"
        );
        assert_eq!(
            backend
                .get_object(&object.object_id)
                .expect("object lookup"),
            Some(object.clone())
        );
        assert_eq!(
            backend
                .get_version(&object.object_id, &object.version)
                .expect("version lookup"),
            Some(normalized_version_record(&version_record))
        );
        let table_batches_after_transaction = metadata_table_batch_count(&root);
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend");

        assert_eq!(
            reopened
                .get_version(&object.object_id, &object.version)
                .expect("reopened version lookup"),
            Some(normalized_version_record(&version_record))
        );
        assert_eq!(
            metadata_table_batch_count(&root),
            table_batches_after_transaction,
            "replaying a deferred local cursor must not duplicate namespace transaction table batches"
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_persists_lists_and_updates_conflicts() {
        let root = temp_root("local_backend_persists_lists_and_updates_conflicts");
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_a".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id(),
            path: "docs/readme.md".to_string(),
            base_version: Some(version("version_1")),
            local_version: version("version_2_local"),
            remote_version: version("version_2_remote"),
            detected_by_node_id: node("node_laptop"),
            reason: "stale reconnect write".to_string(),
            local_content_hash: Some("blake3:local".to_string()),
            remote_content_hash: Some("blake3:remote".to_string()),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut conflict_b = conflict.clone();
        conflict_b.conflict_id = "conflict_b".to_string();

        let entry = backend.put_conflict(&conflict).expect("put conflict");
        backend.put_conflict(&conflict_b).expect("put conflict b");
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PutConflict {
                conflict: Some(ref journal_conflict),
                ..
            } if journal_conflict == &conflict
        ));
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get conflict"),
            Some(conflict.clone())
        );
        assert_eq!(
            backend
                .conflicts_for_object(&conflict.object_id)
                .expect("list conflicts"),
            vec![conflict.clone(), conflict_b.clone()]
        );
        let first_page = backend
            .conflicts_for_object_page(&conflict.object_id, None, 1)
            .expect("first conflict page");
        assert_eq!(first_page.conflicts, vec![conflict.clone()]);
        let second_page = backend
            .conflicts_for_object_page(&conflict.object_id, first_page.next_cursor.as_ref(), 1)
            .expect("second conflict page");
        assert_eq!(second_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_page.next_cursor.is_none());
        assert!(
            backend
                .conflicts_for_object_page(&conflict.object_id, None, 0)
                .expect("empty conflict page")
                .conflicts
                .is_empty()
        );
        let first_scan_page = backend
            .conflict_records_page(None, 1)
            .expect("first conflict scan page");
        assert_eq!(first_scan_page.conflicts, vec![conflict.clone()]);
        let second_scan_page = backend
            .conflict_records_page(first_scan_page.next_cursor.as_ref(), 1)
            .expect("second conflict scan page");
        assert_eq!(second_scan_page.conflicts, vec![conflict_b.clone()]);
        assert!(second_scan_page.next_cursor.is_none());
        assert!(
            backend
                .conflict_records_page(None, 0)
                .expect("empty conflict scan")
                .conflicts
                .is_empty()
        );
        assert_eq!(
            backend.all_conflicts().expect("all conflicts"),
            vec![conflict.clone(), conflict_b.clone()]
        );

        let resolved = backend
            .update_conflict_status(
                &conflict.object_id,
                &conflict.conflict_id,
                DfsConflictStatus::Resolved,
            )
            .expect("resolve conflict")
            .expect("conflict");
        assert_eq!(resolved.status, DfsConflictStatus::Resolved);
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("get resolved")
                .expect("conflict")
                .status,
            DfsConflictStatus::Resolved
        );
        assert_eq!(
            backend
                .remove_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("remove resolved conflict")
                .as_ref()
                .map(|conflict| conflict.status.clone()),
            Some(DfsConflictStatus::Resolved)
        );
        assert_eq!(
            backend
                .remove_conflict(&conflict_b.object_id, &conflict_b.conflict_id)
                .expect("remove second conflict"),
            Some(conflict_b.clone())
        );
        assert_eq!(
            backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)
                .expect("resolved conflict removed"),
            None
        );
        assert!(
            backend
                .all_conflicts()
                .expect("all conflicts after removal")
                .is_empty()
        );
        let journal = backend.journal_since(0).expect("journal after removals");
        assert!(matches!(
            journal.last().map(|entry| &entry.mutation),
            Some(DfsMetadataMutation::PutConflict {
                conflict_id,
                conflict: None,
                ..
            }) if conflict_id == &conflict_b.conflict_id
        ));
        drop(backend);

        let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
            .expect("reopen backend after conflict removals");
        assert!(
            reopened
                .all_conflicts()
                .expect("reopened all conflicts")
                .is_empty()
        );

        cleanup(&root);
    }

    #[test]
    fn local_backend_batches_write_intent_and_receipts() {
        let root = temp_root("local_backend_batches_intent_receipts");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = file_system();
        let file = object(version("version_0"));
        let intent = DfsWriteIntentRecord {
            intent_id: "intent-batched".to_string(),
            file_system_id: file.file_system_id.clone(),
            object_id: file.object_id.clone(),
            base_version: Some(file.version.clone()),
            target_version: version("version_1"),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner"), node("node_remote")],
            policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:batched".to_string()),
            object_metadata: Some(file.clone()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        let receipts = ["node_owner", "node_remote"]
            .into_iter()
            .map(|replica| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file.file_system_id.clone(),
                    object_id: file.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named(&format!("evt_batch_{replica}"))
                        .expect("event"),
                },
                replica_node_id: node(replica),
                content_hash: intent.content_hash.clone(),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .collect::<Vec<_>>();

        {
            let mut backend =
                LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
            backend.put_file_system(&file_system).expect("filesystem");
            backend.put_object(&file).expect("object");
            let entries = backend
                .put_intent_and_receipts_for_object(Some(&file), &intent, &receipts)
                .expect("batched intent receipts");
            assert_eq!(entries.len(), 3);
            assert_eq!(
                backend
                    .get_intent(&file.object_id, &intent.intent_id)
                    .expect("intent"),
                Some(intent.clone())
            );
            assert_eq!(
                backend
                    .receipts_for_version(&file.object_id, &intent.target_version)
                    .expect("receipts")
                    .len(),
                2
            );
        }

        let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen");
        assert_eq!(
            reopened
                .get_intent(&file.object_id, &intent.intent_id)
                .expect("reopened intent"),
            Some(intent)
        );
        assert_eq!(
            reopened
                .receipts_for_version(&file.object_id, &version("version_1"))
                .expect("reopened receipts")
                .len(),
            2
        );

        cleanup(&root);
    }

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-dfs-metadata-{name}"))
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
