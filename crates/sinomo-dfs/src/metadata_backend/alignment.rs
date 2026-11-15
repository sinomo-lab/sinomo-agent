//! Alignment of a write-set shard member with a new primary's epoch start.
//!
//! A takeover or rotation handoff starts an epoch at an exact journal prefix,
//! the epoch start the shard record names. A member whose journal matches at
//! that prefix drops the records after it: they come from an earlier epoch's
//! primary and were never acknowledged, because the new primary lacks them.
//! The member then follows the new primary incrementally. A member whose
//! journal differs there, or that can no longer reconstruct its prefix there,
//! is repaired by snapshot install. A member whose journal ends before the
//! epoch start reports its head, which only the new primary can check.
//!
//! Everything runs under the shard writer lock. The new epoch's floors are
//! written before the journal is cut, so a crash in between leaves the head at
//! or above every floor and the next alignment cuts again.

use super::*;
use crate::DfsMetadataReplicaAlignment;

impl LocalSobjMetadataBackend {
    /// This member's exact prefix proof at `sequence`, extended from its table
    /// checkpoint through its retained journal; `None` when the checkpoint is
    /// already past `sequence` or the journal does not reach it.
    pub(super) fn historical_prefix_proof_at(
        &self,
        sequence: u64,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        if let Some(head) = self
            .prefix_proof
            .as_ref()
            .filter(|proof| proof.journal_sequence == sequence)
        {
            return Ok(Some(head.clone()));
        }
        let durable_next = read_next_journal_sequence_lenient(&self.root)?
            .max(self.next_sequence)
            .max(1);
        if sequence >= durable_next {
            return Ok(None);
        }
        let checkpoint =
            crate::metadata_table_store::read_metadata_table_snapshot_for_replica_repair(&self.root)?;
        let base = checkpoint.materialized_through_sequence.unwrap_or(0);
        if base > sequence {
            return Ok(None);
        }
        let mut proof = match checkpoint.prefix_proof {
            Some(proof) => proof,
            None if base == 0 && !checkpoint.tables.has_materialized_state() => {
                crate::metadata_records::dfs_metadata_prefix_proof_genesis(&self.shard_id)
            }
            None => return Ok(None),
        };
        while proof.journal_sequence < sequence {
            let entries = journal_entries_since_limited_with_next_sequence(
                &self.root,
                proof.journal_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                durable_next,
            )?;
            let before = proof.journal_sequence;
            for entry in entries {
                if entry.sequence > sequence {
                    break;
                }
                proof = self.prefix_proof_after_entry_for_shard(&proof, &entry, &self.shard_id)?;
            }
            if proof.journal_sequence == before {
                return Ok(None);
            }
        }
        Ok(Some(proof))
    }

    /// Align this member with `target`, a prefix in the history of the
    /// permit's epoch: the epoch start itself, or a head below it the new
    /// primary has checked against its own journal.
    pub(crate) fn align_replica_to_prefix_owned(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        target: &DfsMetadataPrefixProof,
        epoch_start: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataReplicaAlignment> {
        let shard_id = self.shard_id.clone();
        let _lock = self.begin_metadata_replica_ingest(permit, &shard_id)?;
        self.align_replica_to_prefix_locked(permit, target, epoch_start, false)
    }

    /// The alignment decision under the held writer lock.
    ///
    /// `inline` is the check an epoch's first batch makes before it is
    /// ingested: a member that already wrote a floor at the epoch (it aligned,
    /// or the primary checked its head) proceeds, unless that floor is at the
    /// epoch start with records still past it, which a crash between the
    /// floors and the cut leaves.
    pub(super) fn align_replica_to_prefix_locked(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        target: &DfsMetadataPrefixProof,
        epoch_start: &DfsMetadataPrefixProof,
        inline: bool,
    ) -> Result<DfsMetadataReplicaAlignment> {
        if permit.floor_epoch_scope().is_none()
            || !target.is_well_formed()
            || target.journal_sequence > epoch_start.journal_sequence
            || (target.journal_sequence == epoch_start.journal_sequence && target != epoch_start)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} alignment target {} is not in the history of epoch {}",
                self.shard_id.0,
                target.journal_sequence,
                permit.source_epoch()
            )));
        }
        let epoch = permit.source_epoch();
        let durable_next = read_next_journal_sequence_lenient(&self.root)?
            .max(self.next_sequence)
            .max(1);
        if self.materialized_through_sequence < durable_next.saturating_sub(1) {
            self.refresh_after_metadata_write_lock()?;
        }
        let head = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact prefix to align",
                self.shard_id.0
            ))
        })?;
        let floors = [
            metadata_cursor::metadata_prefix_ack_floor(&self.root, &self.shard_id)?
                .map(|floor| (floor.authority_epoch, floor.journal_sequence, floor.durable_prefix_proof)),
            metadata_cursor::metadata_authority_ack_floor(&self.root, &self.shard_id)?
                .map(|floor| (floor.authority_epoch, floor.journal_sequence, floor.durable_prefix_proof)),
        ];
        let at_epoch = floors
            .iter()
            .flatten()
            .filter(|(floor_epoch, _, _)| floor_epoch.is_some_and(|floor_epoch| floor_epoch >= epoch))
            .collect::<Vec<_>>();
        let past_target = at_epoch
            .iter()
            .any(|(_, sequence, _)| *sequence > target.journal_sequence);
        let cut_pending = at_epoch
            .iter()
            .any(|(_, sequence, _)| *sequence == epoch_start.journal_sequence)
            && head.journal_sequence > epoch_start.journal_sequence;
        if past_target || (inline && !at_epoch.is_empty() && !cut_pending) {
            return Ok(DfsMetadataReplicaAlignment::Following { head });
        }
        if head.journal_sequence < target.journal_sequence {
            return Ok(DfsMetadataReplicaAlignment::Behind { head });
        }
        // A journal is never cut below the epoch start: records there may
        // belong to the new history even when this member never acknowledged
        // them at the new epoch.
        if head.journal_sequence > target.journal_sequence && target != epoch_start {
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} head {} is past the checked prefix {} below the epoch start",
                self.shard_id.0, head.journal_sequence, target.journal_sequence
            )));
        }
        if self.historical_prefix_proof_at(target.journal_sequence)?.as_ref() != Some(target) {
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} journal differs from the epoch {epoch} history at sequence {}, or no longer reaches it",
                self.shard_id.0, target.journal_sequence
            )));
        }
        let held = |sequence: u64, proof: &DfsMetadataPrefixProof| {
            at_epoch.iter().any(|(_, floor_sequence, floor_proof)| {
                *floor_sequence == sequence && floor_proof == proof
            })
        };
        if !held(target.journal_sequence, target) {
            metadata_cursor::put_metadata_prefix_ack_floor(
                &self.root,
                &DfsMetadataPrefixAckFloor {
                    shard_id: self.shard_id.clone(),
                    journal_sequence: target.journal_sequence,
                    state_fingerprint: String::new(),
                    durable_prefix_proof: target.clone(),
                    authority_epoch: Some(epoch),
                },
            )?;
            metadata_cursor::put_metadata_authority_ack_floor(
                &self.root,
                &crate::DfsMetadataAuthorityAckFloor {
                    shard_id: self.shard_id.clone(),
                    journal_sequence: target.journal_sequence,
                    durable_prefix_proof: target.clone(),
                    authority_epoch: Some(epoch),
                },
            )?;
        }
        let dropped = head.journal_sequence - target.journal_sequence;
        if dropped > 0 {
            self.truncate_replica_journal_to_locked(target)?;
        }
        Ok(DfsMetadataReplicaAlignment::Aligned {
            head: target.clone(),
            dropped,
        })
    }

    /// Cut the journal back to `target` and rebuild the tables to it: they are
    /// reset to the table checkpoint, which must not be past `target`, and the
    /// retained journal up to `target` is replayed into them.
    fn truncate_replica_journal_to_locked(&mut self, target: &DfsMetadataPrefixProof) -> Result<()> {
        let checkpoint =
            crate::metadata_table_store::read_metadata_table_snapshot_for_replica_repair(&self.root)?;
        let base = checkpoint.materialized_through_sequence.unwrap_or(0);
        if base > target.journal_sequence {
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} table checkpoint {base} is past the epoch start {}; snapshot repair is required",
                self.shard_id.0, target.journal_sequence
            )));
        }
        let base_proof = match checkpoint.prefix_proof.clone() {
            Some(proof) => proof,
            None if base == 0 && !checkpoint.tables.has_materialized_state() => {
                crate::metadata_records::dfs_metadata_prefix_proof_genesis(&self.shard_id)
            }
            None => {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "metadata shard {} table checkpoint has no exact prefix",
                    self.shard_id.0
                )));
            }
        };
        crate::metadata_journal::admission::invalidate(&self.root)?;
        crate::metadata_table_store::remove_metadata_table_checkpoint_log(&self.root)?;
        crate::metadata_table_store::truncate_metadata_table_log(&self.root)?;
        crate::metadata_journal::truncate_segmented_journal_after(&self.root, target.journal_sequence)?;
        metadata_cursor::write_journal_head(&self.root, target.journal_sequence.saturating_add(1))?;
        self.tables = checkpoint.tables;
        self.tables.clear_repair_index_changes();
        self.materialized_through_sequence = base;
        self.prefix_proof = Some(base_proof);
        self.next_sequence = target.journal_sequence.saturating_add(1);
        self.table_log_offset = 0;
        self.table_storage_generation = None;
        *self
            .read_snapshot_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.content_root_catalog_snapshot_cache = None;
        self.content_root_catalog_previous_snapshot_cache = None;
        self.write_local_materialization_cursor(&DfsMetadataReplayCursor {
            shard_id: self.shard_id.clone(),
            last_sequence: base,
        })?;
        self.next_sequence =
            crate::metadata_journal::admission::certify_for_write(&self.root, &self.shard_id)?;
        if self.next_sequence != target.journal_sequence.saturating_add(1) {
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} journal head {} after alignment is not the epoch start {}",
                self.shard_id.0,
                self.next_sequence.saturating_sub(1),
                target.journal_sequence
            )));
        }
        self.recover_unmaterialized_local_journal_entries()?;
        if self.prefix_proof.as_ref() != Some(target) {
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata shard {} rebuilt prefix does not match the epoch start {}",
                self.shard_id.0, target.journal_sequence
            )));
        }
        let shard_id = self.shard_id.clone();
        metadata_cursor::write_replay_cursor(
            &self.root,
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: target.journal_sequence,
            },
        )?;
        self.observe_resident_metadata_replay_cursor(&shard_id, target.journal_sequence);
        crate::repair_executor::repair_index_changed(
            &self.root,
            &shard_id,
            crate::repair_executor::DfsRepairIndexChange::Replaced,
        );
        Ok(())
    }
}
