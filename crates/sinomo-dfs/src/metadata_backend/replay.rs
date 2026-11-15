//! Replay for the local metadata backend.

use super::*;

impl LocalSobjMetadataBackend {
    pub(crate) fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.persist_exact_journal_frames_with_replica_permit_report(
            permit, batch, frames, None, None,
        )
    }

    pub(super) fn persist_exact_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: Option<&DfsMetadataPrefixProof>,
        expected_operation: Option<&crate::DfsMetadataAuthorityOperation>,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        if !batch.entries.is_empty() || frames.is_empty() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} exact-frame ingest requires one carrier",
                batch.shard_id.0
            )));
        }
        if let Some(expected_target_prefix) = expected_target_prefix
            && (!expected_target_prefix.is_well_formed()
                || expected_target_prefix.journal_sequence != batch.next_after_sequence)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} authority ingest has an invalid expected prefix at sequence {}",
                batch.shard_id.0, batch.next_after_sequence
            )));
        }
        if expected_target_prefix.is_some() != expected_operation.is_some() {
            return Err(MetadataBackendError::Serde(
                "authority ingest requires both the current cut and original operation".into(),
            ));
        }
        let decode_started = Instant::now();
        let entries = frames
            .iter()
            .map(crate::metadata_journal::decode_metadata_journal_frame_record)
            .collect::<Result<Vec<_>>>()?;
        let decode_ms = decode_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-replica-frame-decode shard={} entries={} bytes={} decode_ms={decode_ms}",
            batch.shard_id.0,
            frames.len(),
            frames
                .iter()
                .map(|frame| frame.exact_bytes.len())
                .sum::<usize>(),
        ));
        let decoded_batch = DfsMetadataJournalBatch {
            shard_id: batch.shard_id.clone(),
            entries,
            next_after_sequence: batch.next_after_sequence,
        };
        self.persist_journal_frames_with_replica_permit_report_internal(
            permit,
            &decoded_batch,
            frames,
            expected_target_prefix,
            expected_operation,
        )
    }

    pub(super) fn persist_journal_frames_with_replica_permit_report_internal(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: Option<&DfsMetadataPrefixProof>,
        expected_operation: Option<&crate::DfsMetadataAuthorityOperation>,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replication_source_shard(&batch.shard_id)?;
        if self.replica_repair_only {
            // A repair-only handle has no qualified current serving image.
            // Journal durability cannot create its mandatory generation or
            // clear this state; the existing authenticated snapshot path does.
            return Err(MetadataBackendError::Rejected {
                code: crate::DfsMetadataRejectionCode::ReplicaUnreadable,
                message: "metadata replica requires an authenticated current-generation snapshot before journal acknowledgement".into(),
            });
        }
        validate_replicated_journal_batch_entry_order(batch)?;
        if batch.entries.is_empty() {
            return Ok(DfsMetadataReplicaApplyReport::default());
        }
        let trace_started = Instant::now();
        let validation_started = Instant::now();

        if frames.len() != batch.entries.len() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal frame count {} does not match decoded batch count {} for shard {}",
                frames.len(),
                batch.entries.len(),
                batch.shard_id.0,
            )));
        }
        for (entry, frame) in batch.entries.iter().zip(frames) {
            if frame.sequence != entry.sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal exact frame {} does not match batch entry {} for shard {}",
                    frame.sequence, entry.sequence, batch.shard_id.0,
                )));
            }
        }

        let mut namespace_authority_proof = None;
        for entry in &batch.entries {
            require_replica_journal_entry_file_system(entry, permit.file_system_id())?;
            if let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &entry.mutation {
                namespace_authority_proof = Some(record.digest.clone());
            }
        }
        let validation_ms = validation_started.elapsed().as_millis();

        let lock_started = Instant::now();
        let _lock = self.begin_metadata_replica_durable_ingest(permit, &batch.shard_id)?;
        let lock_ms = lock_started.elapsed().as_millis();
        let head_started = Instant::now();
        // `Self::new` performs the crash-only tail recovery once. While this
        // resident backend is alive, every successful local or replayed append
        // advances both `self.next_sequence` and the atomic head hint while holding
        // this same shard writer lock. Re-running tail recovery here decoded
        // the last (often multi-megabyte) prepare on every replica ACK and made
        // latency grow with retained history. The head also observes a writer
        // from another compatible resident handle; the in-memory maximum
        // preserves an append whose derived head update was the failing step.
        let durable_next_sequence = read_next_journal_sequence_lenient(&self.root)?
            .max(self.next_sequence)
            .max(1);
        let head_ms = head_started.elapsed().as_millis();
        let persisted_through_sequence = durable_next_sequence.saturating_sub(1);
        // Validate the complete current suffix before any durable append.
        // Earlier deferred records are normal recovery state and must be
        // materialized before an incoming commit consumes their body.
        if self.materialized_through_sequence < persisted_through_sequence {
            self.refresh_after_metadata_write_lock()?;
        }
        if let Some(operation) = expected_operation {
            let crate::DfsMetadataAuthorityOperation::PreparedOperation {
                file_system_id,
                shard_id,
                owner_epoch,
                prepare_sequence,
                ..
            } = operation;
            if file_system_id != permit.file_system_id()
                || shard_id != &batch.shard_id
                || *owner_epoch != permit.source_epoch()
                || *prepare_sequence > batch.next_after_sequence
            {
                return Err(MetadataBackendError::Serde(
                    "authority operation is outside the validated current source cut".into(),
                ));
            }
        }
        let suffix_start = batch
            .entries
            .partition_point(|entry| entry.sequence <= self.materialized_through_sequence);
        if let Some(proof) = self
            .validate_replica_journal_suffix(&batch.entries[suffix_start..], expected_operation, permit.source_epoch())?
        {
            namespace_authority_proof = Some(proof);
        }
        self.next_sequence = self.next_sequence.max(durable_next_sequence);
        let target_sequence = batch.next_after_sequence;
        let prospective_persisted_through_sequence =
            persisted_through_sequence.max(target_sequence);
        let materialize_before_ack = prospective_persisted_through_sequence
            .saturating_sub(self.materialized_through_sequence)
            > MAX_DEFERRED_REPLICA_ENTRIES;

        let prefix_started = Instant::now();
        // On a write-set shard a floor binds only at its own epoch: an
        // earlier epoch's floor blocks neither alignment nor a new primary,
        // and a later epoch's floor refuses this stale source.
        let authority_floor = permit.binding_ack_floor(
            metadata_cursor::metadata_authority_ack_floor(&self.root, &batch.shard_id)?,
            |floor| floor.authority_epoch,
        )?;
        let prefix_ack_floor = permit.binding_ack_floor(
            metadata_cursor::metadata_prefix_ack_floor(&self.root, &batch.shard_id)?,
            |floor| floor.authority_epoch,
        )?;
        if let Some(floor) = authority_floor.as_ref()
            && floor.journal_sequence > target_sequence
        {
            let message = format!(
                "metadata shard {} cannot retry acknowledged prefix {} behind authority floor {}",
                self.shard_id.0, target_sequence, floor.journal_sequence
            );
            // Ordinary catch-up may use a sender cursor older than a floor
            // established by a foreground authority ACK. Keep rejecting the
            // stale batch, but let the existing authenticated snapshot-repair
            // path advance the sender instead of retrying this batch forever.
            return Err(if expected_target_prefix.is_none() {
                MetadataBackendError::prefix_mismatch(message)
            } else {
                MetadataBackendError::Serde(message)
            });
        }

        // Once asynchronous table expansion advances through (or beyond) an
        // acknowledged operation, the current materialized proof can no
        // longer reconstruct that historical target. The exact authority
        // floor is the durable target proof. Returning it is safe only after
        // the common journal persistence path below byte-verifies every
        // supplied frame against the local journal.
        let acknowledged_retry_prefix = authority_floor
            .as_ref()
            .filter(|floor| floor.journal_sequence == target_sequence)
            .map(|floor| &floor.durable_prefix_proof)
            .filter(|proof| proof.is_well_formed() && proof.journal_sequence == target_sequence)
            .filter(|_| self.materialized_through_sequence >= target_sequence)
            .cloned();
        if let (Some(retry_prefix), Some(materialized_prefix)) = (
            acknowledged_retry_prefix.as_ref(),
            self.prefix_proof.as_ref(),
        ) && materialized_prefix.journal_sequence == target_sequence
            && materialized_prefix != retry_prefix
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} materialized prefix conflicts with authority floor at sequence {}",
                self.shard_id.0, target_sequence
            )));
        }

        // A delayed authority operation can arrive after ordinary exact-frame
        // catch-up has already materialized a later prefix. The current table
        // proof cannot move backward, so authenticate the authority's target
        // proof by extending it through the local immutable journal to the
        // later ordinary durable prefix floor. This escape hatch is available
        // only to the authority-specific ingest entry point: an ordinary
        // replica batch never supplies `expected_target_prefix`.
        let historical_expected_prefix = if acknowledged_retry_prefix.is_none()
            && self
                .prefix_proof
                .as_ref()
                .is_some_and(|proof| proof.journal_sequence > target_sequence)
        {
            let expected_target_prefix = expected_target_prefix.ok_or_else(|| {
                MetadataBackendError::prefix_mismatch(format!(
                    "metadata shard {} cannot reconstruct acknowledged prefix {} behind durable proof {}",
                    self.shard_id.0,
                    target_sequence,
                    self.prefix_proof
                        .as_ref()
                        .map_or(0, |proof| proof.journal_sequence)
                ))
            })?;
            let later_floor = prefix_ack_floor.as_ref().ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} cannot authenticate historical authority prefix {} without a later durable exact prefix floor",
                    self.shard_id.0, target_sequence
                ))
            })?;
            self.verify_historical_prefix_against_durable_floor(
                expected_target_prefix,
                later_floor,
                persisted_through_sequence,
                &batch.shard_id,
            )?;
            Some(expected_target_prefix.clone())
        } else {
            None
        };

        let (target_prefix, newly_persisted) = if let Some(retry_prefix) = acknowledged_retry_prefix
        {
            (retry_prefix, 0)
        } else if let Some(historical_prefix) = historical_expected_prefix {
            (historical_prefix, 0)
        } else {
            let mut durable_prefix = self.prefix_proof.clone().ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} has no materialized prefix for durable replica ingest",
                    self.shard_id.0
                ))
            })?;
            for floor_prefix in [
                prefix_ack_floor
                    .as_ref()
                    .map(|floor| &floor.durable_prefix_proof),
                authority_floor
                    .as_ref()
                    .map(|floor| &floor.durable_prefix_proof),
            ]
            .into_iter()
            .flatten()
            {
                if floor_prefix.is_well_formed()
                    && floor_prefix.journal_sequence <= target_sequence
                    && floor_prefix.journal_sequence > durable_prefix.journal_sequence
                {
                    durable_prefix = floor_prefix.clone();
                }
            }
            if durable_prefix.journal_sequence > target_sequence {
                return Err(MetadataBackendError::prefix_mismatch(format!(
                    "metadata shard {} cannot reconstruct acknowledged prefix {} behind durable proof {}",
                    self.shard_id.0, target_sequence, durable_prefix.journal_sequence
                )));
            }

            let mut target_prefix = (durable_prefix.journal_sequence == target_sequence)
                .then(|| durable_prefix.clone());
            while durable_prefix.journal_sequence < persisted_through_sequence {
                let entries = journal_entries_since_limited_with_next_sequence(
                    &self.root,
                    durable_prefix.journal_sequence,
                    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                    durable_next_sequence,
                )?;
                if entries.is_empty() {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard {} durable journal ends before sequence {}",
                        self.shard_id.0, persisted_through_sequence
                    )));
                }
                for entry in entries {
                    if entry.sequence > persisted_through_sequence {
                        break;
                    }
                    durable_prefix = self.prefix_proof_after_entry_for_shard(
                        &durable_prefix,
                        &entry,
                        &batch.shard_id,
                    )?;
                    if entry.sequence == target_sequence {
                        target_prefix = Some(durable_prefix.clone());
                    }
                }
            }

            let mut newly_persisted = 0usize;
            for entry in &batch.entries {
                if entry.sequence <= persisted_through_sequence {
                    continue;
                }
                if entry.sequence != durable_prefix.journal_sequence.saturating_add(1) {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal batch for shard {} starts unseen history at sequence {} but durable journal requires {}",
                        batch.shard_id.0,
                        entry.sequence,
                        durable_prefix.journal_sequence.saturating_add(1)
                    )));
                }
                let frame = frames
                    .iter()
                    .find(|frame| frame.sequence == entry.sequence)
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(format!(
                            "metadata journal batch for shard {} has no exact frame for sequence {}",
                            batch.shard_id.0, entry.sequence
                        ))
                    })?;
                durable_prefix = metadata_prefix_proof_after_exact_frame(
                    &durable_prefix,
                    entry,
                    &batch.shard_id,
                    &frame.exact_bytes,
                )?;
                newly_persisted = newly_persisted.saturating_add(1);
                if entry.sequence == target_sequence {
                    target_prefix = Some(durable_prefix.clone());
                }
            }
            let target_prefix = target_prefix.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata journal batch for shard {} does not establish requested prefix {}",
                    batch.shard_id.0, target_sequence
                ))
            })?;
            (target_prefix, newly_persisted)
        };
        if let Some(expected_target_prefix) = expected_target_prefix
            && &target_prefix != expected_target_prefix
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} durable authority prefix {} does not match the expected prepared history",
                batch.shard_id.0, target_sequence
            )));
        }
        let prefix_ms = prefix_started.elapsed().as_millis();

        let journal_started = Instant::now();
        let frame_pairs = batch.entries.iter().zip(frames).collect::<Vec<_>>();
        crate::metadata_journal::admission::mark_uncertain(&self.root)?;
        let persisted = match persist_replayed_journal_frames(
            &self.root,
            &frame_pairs,
            persisted_through_sequence,
        ) {
            Ok(persisted) => persisted,
            Err(error) => {
                return Err(self.recover_after_replayed_journal_persistence_error(error));
            }
        };
        if let Some(max_sequence) = persisted
            && let Err(error) = self.advance_journal_head_after_replayed_entry(max_sequence)
        {
            return Err(self.recover_after_replayed_journal_persistence_error(error));
        }
        crate::metadata_journal::admission::record_certified(
            &self.root,
            &self.shard_id,
            self.next_sequence,
            Some(target_prefix.clone()),
        )?;
        let journal_ms = journal_started.elapsed().as_millis();
        let materialization_started = Instant::now();
        if materialize_before_ack {
            // Preserve the authority's immutable frames as the journal source
            // of truth, then close an oversized prospective table lag before
            // acknowledging it. Routing through decoded replay here would
            // re-encode the frames and could replace a compatible authority
            // representation with different local bytes.
            self.refresh_after_metadata_write_lock()?;
        }
        let materialization_ms = materialization_started.elapsed().as_millis();
        self.replica_progress.publish(permit, &target_prefix, false);
        let materialization_pending = target_sequence > self.materialized_through_sequence;
        let report = DfsMetadataReplicaApplyReport::with_durable_ingest(
            newly_persisted,
            namespace_authority_proof,
            target_prefix,
            materialization_pending,
        );
        metadata_backend_perf_trace(format_args!(
            "metadata-replica-durable-ingest shard={} target={} entries={} newly_persisted={} validation_ms={validation_ms} lock_ms={lock_ms} head_ms={head_ms} prefix_ms={prefix_ms} journal_ms={journal_ms} materialization_ms={materialization_ms} materialization_pending={materialization_pending} total_ms={}",
            batch.shard_id.0,
            target_sequence,
            batch.entries.len(),
            newly_persisted,
            trace_started.elapsed().as_millis(),
        ));
        Ok(report)
    }

    /// Validate the incoming cut through the same checked transitions as replay,
    /// then restore every touched row and index before durable ingestion. Copying
    /// the entire resident table here kept shard admission occupied by unrelated
    /// history, even for a one-record ACK. The existing replay rollback preserves
    /// the same all-or-nothing validation without that whole-table copy.
    pub(super) fn validate_replica_journal_suffix(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        expected_operation: Option<&crate::DfsMetadataAuthorityOperation>,
        source_epoch: u64,
    ) -> Result<Option<crate::DfsPreparedOperationDigest>> {
        // The planner restores already-applied rows itself on a malformed later
        // entry. A successful plan still grants no materialization/publication.
        let planned = self.plan_written_journal_entries_table_ops_with_ingress_epoch(entries, Some(source_epoch))?;
        let proof = expected_operation.map(|operation| {
            let crate::DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } = operation;
            let record = self.tables.prepared_operations_v7.get(operation_id).ok_or_else(|| {
                MetadataBackendError::Serde(
                    "authority operation has no complete original body in the validated current cut".into(),
                )
            })?;
            crate::metadata_api::validated_metadata_authority_operation_digest(record, operation)
        }).transpose();
        // Restore on both proof success and failure, before returning either.
        planned.rollback.rollback(&mut self.tables);
        proof
    }

    pub(crate) fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        let before = self.materialized_through_sequence;
        let _lock = self.begin_metadata_write_allow_frozen()?;
        Ok(self.materialized_through_sequence.saturating_sub(before) as usize)
    }

    pub(super) fn recover_after_replayed_journal_persistence_error(
        &mut self,
        error: MetadataBackendError,
    ) -> MetadataBackendError {
        if let Err(fence_error) = crate::metadata_journal::admission::mark_uncertain(&self.root) {
            return MetadataBackendError::Serde(format!(
                "journal persistence failed: {error}; admission fence failed: {fence_error}"
            ));
        }
        match crate::metadata_journal::admission::certify_for_write(&self.root, &self.shard_id) {
            Ok(recovered_next_sequence) => {
                self.next_sequence = self.next_sequence.max(recovered_next_sequence).max(1);
                error
            }
            Err(recovery_error) => MetadataBackendError::Serde(format!(
                "metadata shard {} journal persistence failed: {error}; exact tail recovery also failed: {recovery_error}",
                self.shard_id.0
            )),
        }
    }

    pub(super) fn advance_journal_head_after_replayed_entry(
        &mut self,
        sequence: u64,
    ) -> Result<()> {
        let next_sequence = sequence.saturating_add(1).max(1).max(self.next_sequence);
        if next_sequence == self.next_sequence {
            return Ok(());
        }
        self.next_sequence = next_sequence;
        metadata_cursor::write_journal_head_hint(&self.root, self.next_sequence)
    }

    pub(super) fn plan_replayed_journal_entries_into_batch(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<(usize, u64)> {
        for entry in entries {
            self.materialize_replayed_journal_entry_into_batch(entry, table_ops, rollback)?;
        }
        Ok((
            entries.len(),
            entries.last().map_or(0, |entry| entry.sequence),
        ))
    }

    pub(super) fn plan_written_journal_entries_table_ops(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<PlannedWrittenJournalTableOps> {
        self.plan_written_journal_entries_table_ops_with_ingress_epoch(entries, None)
    }

    fn plan_written_journal_entries_table_ops_with_ingress_epoch(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
        source_epoch: Option<u64>,
    ) -> Result<PlannedWrittenJournalTableOps> {
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let plan = (|| {
            for entry in entries {
                if let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &entry.mutation
                    && source_epoch == Some(record.owner_epoch)
                    && !self.tables.prepared_operations_v7.get(&record.operation_id)
                        .is_some_and(|known| known.digest == record.digest && known.payload == record.payload)
                {
                    // Count-aware authoring is required for a new operation in
                    // the active source epoch. Archived prior-epoch bodies and
                    // exact known operations keep their immutable replay path.
                    // A peer missing the initialized cut needs the existing
                    // snapshot-repair path, not permission to infer counts.
                    crate::namespace_counts::validate_new_counted_publication(&self.tables, &record.payload)
                        .map_err(|error| MetadataBackendError::prefix_mismatch(format!(
                            "counted metadata snapshot required before current-epoch ingress: {error}")))?;
                }
                self.materialize_replayed_journal_entry_into_batch(entry, &mut table_ops, &mut rollback)?;
            }
            Ok((entries.len(), entries.last().map_or(0, |entry| entry.sequence)))
        })();
        let (applied, last_materialized_sequence) = match plan {
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

    pub(super) fn materialize_written_journal_entries_unlocked(
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

    /// Live suffix recovery owns its strict hint checkpoint after the exact
    /// table prefix is restored. Reading a malformed hint from the ordinary
    /// append wrapper here would fail before that recovery boundary is reached.
    pub(super) fn materialize_written_journal_entries_without_cursor_checkpoint_unlocked(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<usize> {
        let planned = self.plan_written_journal_entries_table_ops(entries)?;
        let applied = planned.applied;
        self.append_preapplied_table_ops(
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        Ok(applied)
    }

    pub(super) fn materialize_replayed_journal_entry_into_batch(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        let op = crate::metadata_replay::table_op_for_native_journal_entry(entry)?;
        self.tables
            .apply_checked_with_rollback(op.clone(), rollback)?;
        table_ops.push(op);
        Ok(())
    }
}
