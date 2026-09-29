//! Replay for the local metadata backend.

use super::*;

impl LocalSobjMetadataBackend {
    /// Persist and materialize a replicated journal batch from a metadata peer.
    ///
    /// The peer is another replica of this backend's logical shard; a batch
    /// naming a different shard is rejected before acquiring the write lock.
    /// Entries whose replay cursor is already past their sequence are skipped.
    /// New entries are written to the local replicated journal, materialized
    /// into table batches, and then acknowledged by advancing replay cursors.
    pub fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        self.require_replication_source_shard(&batch.shard_id)?;
        validate_replicated_journal_batch_entry_order(batch)?;
        let _lock = self.begin_metadata_write()?;
        self.apply_journal_batch_unlocked(batch, None, MetadataReplayApplyEvidence::default())
            .map(|report| report.applied_entries())
    }

    /// Apply a validated replica batch across only the exact activated author
    /// fence named by `permit`. This path never changes process-local
    /// authority binding.
    pub fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        self.apply_journal_batch_with_replica_permit_internal(permit, batch, false)
            .map(|report| report.applied_entries())
    }

    pub(crate) fn apply_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.apply_journal_batch_with_replica_permit_internal(permit, batch, true)
    }

    /// Persist one authority-bound replica batch and return after the
    /// append-only journal, exact prefix, and canonical operation identity are
    /// durable. Rebuildable table expansion is deliberately left behind the
    /// response; normal writes, snapshots, handoff, and promotion still enter
    /// through refresh paths which synchronously close that gap.
    pub(crate) fn persist_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.persist_journal_frames_with_replica_permit_report_internal(permit, batch, None, None)
    }

    pub(crate) fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.persist_exact_journal_frames_with_replica_permit_report(permit, batch, frames, None)
    }

    pub(super) fn persist_exact_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: Option<&DfsMetadataPrefixProof>,
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
            Some(frames),
            expected_target_prefix,
        )
    }

    pub(super) fn persist_journal_frames_with_replica_permit_report_internal(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        supplied_frames: Option<&[DfsMetadataJournalFrameRecord]>,
        expected_target_prefix: Option<&DfsMetadataPrefixProof>,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replication_source_shard(&batch.shard_id)?;
        validate_replicated_journal_batch_entry_order(batch)?;
        if batch.entries.is_empty() {
            return Ok(DfsMetadataReplicaApplyReport::default());
        }
        let trace_started = Instant::now();
        let validation_started = Instant::now();

        let encoded_frames;
        let frames = if let Some(frames) = supplied_frames {
            if frames.len() != batch.entries.len() {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal frame count {} does not match decoded batch count {} for shard {}",
                    frames.len(),
                    batch.entries.len(),
                    batch.shard_id.0
                )));
            }
            for (entry, frame) in batch.entries.iter().zip(frames) {
                if frame.sequence != entry.sequence {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal exact frame {} does not match batch entry {} for shard {}",
                        frame.sequence, entry.sequence, batch.shard_id.0
                    )));
                }
            }
            frames
        } else {
            encoded_frames = batch
                .entries
                .iter()
                .map(crate::metadata_journal::encode_metadata_journal_frame_record)
                .collect::<Result<Vec<_>>>()?;
            encoded_frames.as_slice()
        };

        let mut namespace_authority_proof = None;
        for entry in &batch.entries {
            if let Some(payload) =
                require_replica_journal_entry_file_system(entry, permit.file_system_id())?
            {
                namespace_authority_proof =
                    DfsValidatedNamespaceAuthorityProof::from_validated_payload(
                        &payload,
                        entry.sequence,
                    );
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
        if self.materialized_through_sequence < persisted_through_sequence
            && batch.entries.iter().any(|entry| {
                entry.sequence > self.materialized_through_sequence
                    && match &entry.mutation {
                        DfsMetadataMutation::PutIntent { .. }
                        | DfsMetadataMutation::PutIntentReceipt { .. } => true,
                        DfsMetadataMutation::PutNamespaceJob { job: Some(job), .. } => {
                            job.kind == DfsNamespaceJobKind::FileHistoryRestore
                        }
                        DfsMetadataMutation::PutNamespaceJobReceipt { receipt, .. } => {
                            receipt.kind == DfsNamespaceJobKind::FileHistoryRestore
                        }
                        _ => false,
                    }
            })
        {
            // Intent effects and restore progress are immutable across batches as well as within
            // one batch. A deferred prefix can contain the first publication
            // purpose for an incoming identity, so validate against that
            // prefix before acknowledging a subsequent intent transition.
            // Namespace-only ACKs retain their deferred materialization path.
            self.refresh_after_metadata_write_lock()?;
        }
        crate::file_history::validate_intent_publication_batch(
            &self.tables,
            batch
                .entries
                .iter()
                .filter(|entry| entry.sequence > self.materialized_through_sequence)
                .map(|entry| &entry.mutation),
        )?;
        self.next_sequence = self.next_sequence.max(durable_next_sequence);
        let target_sequence = batch.next_after_sequence;
        let prospective_persisted_through_sequence =
            persisted_through_sequence.max(target_sequence);
        let materialize_before_ack = prospective_persisted_through_sequence
            .saturating_sub(self.materialized_through_sequence)
            > MAX_DEFERRED_REPLICA_ENTRIES;

        let prefix_started = Instant::now();
        let authority_floor =
            metadata_cursor::metadata_authority_ack_floor(&self.root, &batch.shard_id)?;
        let prefix_ack_floor =
            metadata_cursor::metadata_prefix_ack_floor(&self.root, &batch.shard_id)?;
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
            .and_then(|floor| floor.durable_prefix_proof.as_ref())
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
                    .and_then(|floor| floor.durable_prefix_proof.as_ref()),
                authority_floor
                    .as_ref()
                    .and_then(|floor| floor.durable_prefix_proof.as_ref()),
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

    pub(crate) fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        let before = self.materialized_through_sequence;
        let _lock = self.begin_metadata_write_allow_frozen()?;
        Ok(self.materialized_through_sequence.saturating_sub(before) as usize)
    }

    pub(super) fn apply_journal_batch_with_replica_permit_internal(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        collect_namespace_authority_proof: bool,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replication_source_shard(&batch.shard_id)?;
        validate_replicated_journal_batch_entry_order(batch)?;
        let mut validated_namespace_prepares = BTreeMap::new();
        for entry in &batch.entries {
            if let Some(prepare) =
                require_replica_journal_entry_file_system(entry, permit.file_system_id())?
            {
                validated_namespace_prepares.insert(entry.sequence, prepare);
            }
        }
        let _lock = self.begin_metadata_replica_ingest(permit, &batch.shard_id)?;
        let evidence = if collect_namespace_authority_proof {
            MetadataReplayApplyEvidence::collecting_namespace_authority_proof()
        } else {
            MetadataReplayApplyEvidence::default()
        };
        let report =
            self.apply_journal_batch_unlocked(batch, Some(validated_namespace_prepares), evidence)?;
        if let Some(prefix) = self.prefix_proof.as_ref() {
            self.replica_progress.publish(permit, prefix, false);
        }
        Ok(report)
    }

    pub(super) fn apply_journal_batch_unlocked<'a>(
        &mut self,
        batch: &'a DfsMetadataJournalBatch,
        prevalidated_namespace_prepares: Option<BTreeMap<u64, DfsNamespacePreparePayloadRef<'a>>>,
        mut evidence: MetadataReplayApplyEvidence,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        if batch.entries.is_empty() {
            return Ok(DfsMetadataReplicaApplyReport::default());
        }
        let payloads_prevalidated = prevalidated_namespace_prepares.is_some();
        let mut validated_namespace_prepares = prevalidated_namespace_prepares.unwrap_or_default();
        for entry in &batch.entries {
            if entry.shard_id != batch.shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal batch for shard {} contains entry from shard {}",
                    batch.shard_id.0, entry.shard_id.0
                )));
            }
            if !payloads_prevalidated
                && let Some(prepare) = require_journal_entry_replay_payloads(entry)?
            {
                validated_namespace_prepares.insert(entry.sequence, prepare);
            }
        }

        let mut candidate_prefix_proof = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no exact prefix checkpoint",
                self.shard_id.0
            ))
        })?;
        let replay_cursor_exists =
            metadata_cursor::replay_cursor_path(&self.root, &batch.shard_id).is_file();
        let mut source_cursor = metadata_cursor::read_replay_cursor(&self.root, &batch.shard_id)?;
        if !replay_cursor_exists {
            // This backend and every replicated journal batch share one
            // logical shard identity, so the local durable prefix is the exact
            // baseline even before the checkpointed replay cursor exists.
            source_cursor.last_sequence = candidate_prefix_proof.journal_sequence;
        }
        if source_cursor.last_sequence > candidate_prefix_proof.journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replay cursor for shard {} is at {} beyond durable prefix {}",
                batch.shard_id.0,
                source_cursor.last_sequence,
                candidate_prefix_proof.journal_sequence
            )));
        }
        // A table batch/snapshot can become durable before its replay cursor.
        // The exact prefix is authoritative in that crash window.
        source_cursor.last_sequence = candidate_prefix_proof.journal_sequence;
        let persisted_through_sequence = source_cursor.last_sequence;

        let mut local_cursor = self.local_materialization_cursor()?;
        let mut local_cursor_advanced = false;
        let mut replay_entries = Vec::new();
        for entry in &batch.entries {
            if entry.sequence <= source_cursor.last_sequence {
                continue;
            }
            let expected_sequence = source_cursor.last_sequence.saturating_add(1);
            if entry.sequence != expected_sequence {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "metadata journal batch for shard {} starts unseen history at sequence {} but durable replay requires {}",
                    batch.shard_id.0, entry.sequence, expected_sequence
                )));
            }
            replay_entries.push(entry);
            if entry.sequence > local_cursor.last_sequence {
                local_cursor.last_sequence = entry.sequence;
                local_cursor_advanced = true;
            }
            source_cursor.last_sequence = entry.sequence;
        }

        // Include already-acknowledged entries in the idempotence check so a
        // stale replay cannot hide a fork behind its cursor.
        crate::file_history::validate_intent_publication_batch(
            &self.tables,
            replay_entries.iter().map(|entry| &entry.mutation),
        )?;
        let all_entries = batch.entries.iter().collect::<Vec<_>>();
        self.persist_replayed_journal_entries_unlocked(&all_entries, persisted_through_sequence)?;
        if replay_entries.is_empty() {
            self.observe_resident_metadata_replay_cursor(
                &batch.shard_id,
                source_cursor.last_sequence,
            );
            return Ok(DfsMetadataReplicaApplyReport::default());
        }
        for entry in &replay_entries {
            candidate_prefix_proof = self.prefix_proof_after_entry_for_shard(
                &candidate_prefix_proof,
                entry,
                &batch.shard_id,
            )?;
        }

        let mut table_ops = Vec::new();
        let mut post_append_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let mut journal_reference_replay_cost_bytes = 0;
        let (applied, table_sequence) = match self.plan_replayed_journal_entries_into_batch(
            replay_entries.iter().copied(),
            &validated_namespace_prepares,
            &mut table_ops,
            &mut post_append_ops,
            &mut rollback,
            &mut journal_reference_replay_cost_bytes,
            &mut evidence,
        ) {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        let previous_prefix_proof = self.prefix_proof.replace(candidate_prefix_proof);
        if let Err(error) = self
            .append_preapplied_table_ops_with_journal_reference_replay_cost_and_post_append_ops(
                table_sequence,
                table_ops,
                post_append_ops,
                journal_reference_replay_cost_bytes,
                rollback,
            )
        {
            self.prefix_proof = previous_prefix_proof;
            return Err(error);
        }
        if local_cursor_advanced {
            self.materialized_through_sequence = self
                .materialized_through_sequence
                .max(local_cursor.last_sequence);
            self.checkpoint_local_materialization_cursor_if_due(local_cursor.last_sequence)?;
        }
        // Both cursors are recovery hints. The durable journal, table log, and
        // exact prefix proof are authoritative, so checkpointing them in
        // batches avoids two atomic file rewrites on every replicated commit.
        metadata_cursor::checkpoint_replay_cursor_if_due(&self.root, &source_cursor)?;
        self.observe_resident_metadata_replay_cursor(&batch.shard_id, source_cursor.last_sequence);
        Ok(
            DfsMetadataReplicaApplyReport::with_namespace_authority_proof(
                applied,
                evidence.namespace_authority_proof,
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn apply_journal_entry(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let validated_namespace_prepare = require_journal_entry_replay_payloads(entry)?;
        crate::file_history::validate_intent_publication_mutation(&self.tables, &entry.mutation)?;
        self.persist_replayed_journal_entry_unlocked(entry)?;
        let candidate_prefix_proof = self.prefix_proof_after_entry(
            self.prefix_proof.as_ref().ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} has no exact prefix checkpoint",
                    self.shard_id.0
                ))
            })?,
            entry,
        )?;
        let previous_prefix_proof = self.prefix_proof.replace(candidate_prefix_proof);
        let changed = match self
            .materialize_replayed_journal_entry_unlocked(entry, validated_namespace_prepare)
        {
            Ok(changed) => changed,
            Err(error) => {
                self.prefix_proof = previous_prefix_proof;
                return Err(error);
            }
        };
        if !changed {
            self.append_and_apply_table_ops(entry.sequence, Vec::new())?;
        }
        if changed {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(Some(entry.clone()))
        } else {
            self.advance_local_materialization_cursor(entry.sequence)?;
            Ok(None)
        }
    }

    #[cfg(test)]
    pub(super) fn persist_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<()> {
        if let Some(sequence) = persist_replayed_journal_entry(&self.root, entry)? {
            self.advance_journal_head_after_replayed_entry(sequence)?;
        }
        Ok(())
    }

    pub(super) fn persist_replayed_journal_entries_unlocked(
        &mut self,
        entries: &[&DfsMetadataJournalEntry],
        persisted_through_sequence: u64,
    ) -> Result<()> {
        crate::metadata_journal::admission::mark_uncertain(&self.root)?;
        let persisted =
            match persist_replayed_journal_entries(&self.root, entries, persisted_through_sequence)
            {
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
            None,
        )?;
        Ok(())
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

    #[cfg(test)]
    pub(super) fn materialize_replayed_journal_entry_unlocked(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        validated_namespace_prepare: Option<DfsNamespacePreparePayloadRef<'_>>,
    ) -> Result<bool> {
        let mut table_ops = Vec::new();
        let mut post_append_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let mut journal_reference_replay_cost_bytes = 0;
        let mut evidence = MetadataReplayApplyEvidence::default();
        let changed = match self.materialize_replayed_journal_entry_into_batch(
            entry,
            validated_namespace_prepare,
            &mut table_ops,
            &mut post_append_ops,
            &mut rollback,
            &mut journal_reference_replay_cost_bytes,
            true,
            &mut evidence,
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
        self.append_preapplied_table_ops_with_journal_reference_replay_cost_and_post_append_ops(
            entry.sequence,
            table_ops,
            post_append_ops,
            journal_reference_replay_cost_bytes,
            rollback,
        )?;
        Ok(true)
    }

    pub(super) fn plan_replayed_journal_entries_into_batch<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a DfsMetadataJournalEntry>,
        validated_namespace_prepares: &BTreeMap<u64, DfsNamespacePreparePayloadRef<'a>>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        post_append_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
        journal_reference_replay_cost_bytes: &mut u64,
        evidence: &mut MetadataReplayApplyEvidence,
    ) -> Result<(usize, u64)> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut changed_count = 0usize;
        let mut last_materialized_sequence = 0u64;
        let entry_count = entries.len();
        for (index, entry) in entries.into_iter().enumerate() {
            last_materialized_sequence = entry.sequence;
            if self.materialize_replayed_journal_entry_into_batch(
                entry,
                validated_namespace_prepares.get(&entry.sequence).copied(),
                table_ops,
                post_append_ops,
                rollback,
                journal_reference_replay_cost_bytes,
                index.saturating_add(1) == entry_count,
                evidence,
            )? {
                changed_count = changed_count.saturating_add(1);
            }
        }
        Ok((changed_count, last_materialized_sequence))
    }

    pub(super) fn plan_written_journal_entries_table_ops(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<PlannedWrittenJournalTableOps> {
        let mut table_ops = Vec::new();
        let mut post_append_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let mut journal_reference_replay_cost_bytes = 0;
        let mut evidence = MetadataReplayApplyEvidence::default();
        let (applied, last_materialized_sequence) = match self
            .plan_replayed_journal_entries_into_batch(
                entries,
                &BTreeMap::new(),
                &mut table_ops,
                &mut post_append_ops,
                &mut rollback,
                &mut journal_reference_replay_cost_bytes,
                &mut evidence,
            ) {
            Ok(result) => result,
            Err(error) => {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        };
        Ok(PlannedWrittenJournalTableOps {
            applied,
            last_materialized_sequence,
            journal_reference_replay_cost_bytes,
            table_ops,
            post_append_ops,
            rollback,
        })
    }

    pub(super) fn materialize_written_journal_entries_unlocked(
        &mut self,
        entries: &[DfsMetadataJournalEntry],
    ) -> Result<usize> {
        let planned = self.plan_written_journal_entries_table_ops(entries)?;
        let applied = planned.applied;
        self.append_preapplied_table_ops_for_written_entries_with_journal_reference_replay_cost(
            entries,
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.post_append_ops,
            planned.rollback,
            planned.journal_reference_replay_cost_bytes,
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
        self.append_preapplied_table_ops_with_journal_reference_replay_cost_and_post_append_ops(
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.post_append_ops,
            planned.journal_reference_replay_cost_bytes,
            planned.rollback,
        )?;
        Ok(applied)
    }

    pub(super) fn materialize_replayed_journal_entry_into_batch(
        &mut self,
        entry: &DfsMetadataJournalEntry,
        validated_namespace_prepare: Option<DfsNamespacePreparePayloadRef<'_>>,
        table_ops: &mut Vec<DfsMetadataTableOp>,
        post_append_ops: &mut Vec<DfsMetadataTableOp>,
        rollback: &mut DfsMetadataTableRollback,
        journal_reference_replay_cost_bytes: &mut u64,
        defer_trailing_resolved_op: bool,
        evidence: &mut MetadataReplayApplyEvidence,
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
                    entry.created_at,
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
                crate::file_history::validate_retained_version_update(
                    self.tables
                        .versions
                        .get(&(object_id.clone(), version.clone())),
                    &version_record,
                )?;
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
            DfsMetadataMutation::RemoveVersion {
                object_id, version, ..
            } => {
                if self.get_version(object_id, version)?.is_none() {
                    return Ok(false);
                }
                if self
                    .tables
                    .version_pins
                    .keys()
                    .any(|(pin_object_id, pin_version, _)| {
                        pin_object_id == object_id && pin_version == version
                    })
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot remove pinned version {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    )));
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveVersion {
                        object_id: object_id.clone(),
                        version: version.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::CaptureFileVersion {
                file_system_id,
                object_id,
                version,
                record,
                pin,
            } => {
                if &record.file_system_id != file_system_id
                    || &record.object_id != object_id
                    || &record.version != version
                {
                    return Err(MetadataBackendError::Serde(
                        "file capture outer identity does not match payload".into(),
                    ));
                }
                let ops = super::pin_validation::file_version_capture_table_ops(
                    &self.tables,
                    record,
                    pin,
                )?;
                let changed = !ops.is_empty();
                for op in ops {
                    self.plan_preapplied_table_op(op, table_ops, rollback);
                }
                Ok(changed)
            }
            DfsMetadataMutation::PutVersionPin {
                file_system_id,
                object_id,
                version,
                pin_id,
                pin,
            } => {
                validate_version_pin_payload(file_system_id, object_id, version, pin_id, pin)?;
                let pinned_version = self.get_version(object_id, version)?.ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} pins missing version {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    ))
                })?;
                if pinned_version.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} pin filesystem {file_system_id} does not match version filesystem {}",
                        entry.shard_id.0, entry.sequence, pinned_version.file_system_id
                    )));
                }
                if let Some(owner) = self
                    .tables
                    .cross_shard_pin_transfer_fence(object_id, version, pin_id)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot put pin {pin_id} for {object_id}/{version} while transfer {} owns it",
                        entry.shard_id.0, entry.sequence, owner.transfer_id
                    )));
                }
                let key = (object_id.clone(), version.clone(), pin_id.clone());
                if let Some(existing) = self.tables.version_pins.get(&key) {
                    if existing == pin {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} conflicts with durable version pin {pin_id} for {object_id}/{version}",
                        entry.shard_id.0, entry.sequence
                    )));
                }
                require_version_pin_capacity_for_new(&self.tables, object_id)?;
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutVersionPin { pin: pin.clone() },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveVersionPin {
                file_system_id,
                object_id,
                version,
                pin_id,
            } => {
                if let Some(owner) = self
                    .tables
                    .cross_shard_pin_transfer_fence(object_id, version, pin_id)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} cannot remove pin {pin_id} for {object_id}/{version} while transfer {} owns it",
                        entry.shard_id.0, entry.sequence, owner.transfer_id
                    )));
                }
                let Some(existing) = self.tables.version_pins.get(&(
                    object_id.clone(),
                    version.clone(),
                    pin_id.clone(),
                )) else {
                    return Ok(false);
                };
                if existing.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} removes pin {pin_id} for {object_id}/{version} with filesystem {file_system_id}, but the stored pin has filesystem {}",
                        entry.shard_id.0, entry.sequence, existing.file_system_id
                    )));
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveVersionPin {
                        object_id: object_id.clone(),
                        version: version.clone(),
                        pin_id: pin_id.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::ReconcileCrossShardPinTransfer {
                file_system_id,
                object_id,
                transfer_id,
                authoritative_migration,
                versions,
                transfer,
            } => {
                if validate_cross_shard_pin_transfer_table_transition(
                    &self.tables,
                    file_system_id,
                    object_id,
                    transfer_id,
                    *authoritative_migration,
                    versions,
                    transfer,
                )? {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::ReconcileCrossShardPinTransfer {
                        versions: versions.clone(),
                        transfer: transfer.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveCrossShardPinTransfer {
                file_system_id,
                object_id,
                transfer_id,
            } => {
                let Some(existing) = self
                    .tables
                    .cross_shard_pin_transfers
                    .get(&(object_id.clone(), transfer_id.clone()))
                else {
                    return Ok(false);
                };
                if existing.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal entry {}:{} removes cross-shard pin transfer {} with the wrong filesystem",
                        entry.shard_id.0, entry.sequence, transfer_id
                    )));
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                        object_id: object_id.clone(),
                        transfer_id: transfer_id.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::FinalizeCrossShardSourceEviction {
                file_system_id,
                object_id,
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                if validate_cross_shard_source_eviction_table_transition(
                    &self.tables,
                    file_system_id,
                    object_id,
                    tombstone,
                    removed_pins,
                    removed_pin_transfers,
                )? {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                        tombstone: tombstone.clone(),
                        removed_pins: removed_pins.clone(),
                        removed_pin_transfers: removed_pin_transfers.clone(),
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
                authoritative_migration,
                intent,
                ..
            } => {
                let intent = required_replay_payload(entry, "put-intent", intent.as_ref())?;
                crate::file_history::validate_intent_publication_mutation(
                    &self.tables,
                    &entry.mutation,
                )?;
                if let Some(receipt) = self
                    .tables
                    .intent_receipts
                    .get(&(object_id.clone(), intent_id.clone()))
                {
                    if receipt.matches_intent(intent) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal write intent {object_id}/{intent_id} conflicts with its compacted receipt"
                    )));
                }
                if !self
                    .tables
                    .intents
                    .contains_key(&(object_id.clone(), intent_id.clone()))
                    && !*authoritative_migration
                    && self.tables.intent_receipt_retention.history_expired(intent)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal write intent {object_id}/{intent_id} idempotency history expired"
                    )));
                }
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
            DfsMetadataMutation::PutIntentReceipt {
                object_id,
                intent_id,
                receipt,
                ..
            } => {
                crate::file_history::validate_intent_publication_mutation(
                    &self.tables,
                    &entry.mutation,
                )?;
                if receipt.object_id != *object_id || receipt.intent_id != *intent_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal intent receipt scope {object_id}/{intent_id} does not match its payload"
                    )));
                }
                if let Some(intent) = self
                    .tables
                    .intents
                    .get(&(object_id.clone(), intent_id.clone()))
                    && !receipt.matches_intent(intent)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal intent receipt {object_id}/{intent_id} conflicts with its full intent"
                    )));
                }
                if self
                    .tables
                    .intent_receipts
                    .get(&(object_id.clone(), intent_id.clone()))
                    == Some(receipt)
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutIntentReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
                ..
            } => {
                crate::file_history::validate_intent_publication_mutation(
                    &self.tables,
                    &entry.mutation,
                )?;
                if !self
                    .tables
                    .intents
                    .contains_key(&(object_id.clone(), intent_id.clone()))
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveRetainedRepairOwner {
                        object_id: object_id.clone(),
                        intent_id: intent_id.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveIntentReceipt {
                object_id,
                intent_id,
                retention,
                ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveIntentReceipt {
                        object_id: object_id.clone(),
                        intent_id: intent_id.clone(),
                        retention: retention.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::MergeIntentReceiptRetention {
                object_id,
                retention,
                ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::MergeIntentReceiptRetention {
                        retention: retention.for_object(object_id),
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
            DfsMetadataMutation::RemoveVisibleCommit {
                object_id, version, ..
            } => {
                if !self
                    .tables
                    .visible_commits
                    .contains_key(&(object_id.clone(), version.clone()))
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveVisibleCommit {
                        object_id: object_id.clone(),
                        version: version.clone(),
                    },
                    table_ops,
                    rollback,
                );
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
            DfsMetadataMutation::PutAdvisoryLockState {
                file_system_id,
                object_id,
                state,
            } => {
                let state =
                    required_replay_payload(entry, "put-advisory-lock-state", state.as_ref())?;
                if state.file_system_id != *file_system_id || state.object_id != *object_id {
                    return Err(MetadataBackendError::AdvisoryLock(format!(
                        "journal scope {file_system_id}/{object_id} does not match payload scope {}/{}",
                        state.file_system_id, state.object_id
                    )));
                }
                let object = self.get_object(object_id)?.ok_or_else(|| {
                    MetadataBackendError::UnknownObject {
                        object_id: object_id.clone(),
                    }
                })?;
                if object.file_system_id != *file_system_id {
                    return Err(MetadataBackendError::AdvisoryLock(format!(
                        "journal lock state for {file_system_id}/{object_id} does not match object filesystem {}",
                        object.file_system_id
                    )));
                }
                if self
                    .tables
                    .advisory_lock_states
                    .get(object_id)
                    .is_some_and(|existing| existing == state)
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutAdvisoryLockState {
                        state: state.clone(),
                    },
                    table_ops,
                    rollback,
                );
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
                if let Some(receipt) = self
                    .tables
                    .namespace_transaction_receipts
                    .get(transaction_id)
                {
                    if receipt
                        .matches_transaction(&normalized)
                        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
                    {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace transaction {transaction_id} conflicts with its compacted receipt"
                    )));
                }
                if self.tables.namespace_transactions.get(transaction_id) == Some(&normalized) {
                    return Ok(false);
                }
                self.plan_namespace_transaction_table_ops(
                    &normalized,
                    entry.created_at,
                    table_ops,
                    rollback,
                )?;
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceTransactionReceipt {
                file_system_id,
                transaction_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.transaction_id != *transaction_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace transaction receipt scope {file_system_id}/{transaction_id} does not match its payload"
                    )));
                }
                if self
                    .tables
                    .namespace_transaction_receipts
                    .get(transaction_id)
                    == Some(receipt)
                {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceTransactionReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
                ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                        transaction_id: transaction_id.clone(),
                        retention: retention.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespacePrepare {
                file_system_id,
                operation_id,
                status,
                ..
            } => {
                let (journal_prepare, prepare) = if let Some(journal_prepare) =
                    validated_namespace_prepare
                {
                    let prepare = decoded_normalized_namespace_prepare_payload_from_validated_ref(
                        entry,
                        journal_prepare,
                    )?;
                    (journal_prepare, prepare)
                } else {
                    decoded_normalized_namespace_prepare_payload_with_ref(entry)?.ok_or_else(
                        || {
                            MetadataBackendError::Serde(format!(
                                "metadata journal entry {}:{} cannot replay put-namespace-prepare without its record payload",
                                entry.shard_id.0, entry.sequence
                            ))
                        },
                    )?
                };
                if prepare.file_system_id != *file_system_id
                    || prepare.operation_id != *operation_id
                    || prepare.status != *status
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare scope {file_system_id}/{operation_id}/{status:?} does not match its payload"
                    )));
                }
                if let Some(receipt) = self.tables.namespace_prepare_receipts.get(operation_id) {
                    if receipt.status == prepare.status
                        && receipt.matches_prepare_identity(&prepare)
                    {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare {operation_id} conflicts with its compacted receipt"
                    )));
                }
                let prepare_reference = namespace_prepare_journal_reference_from_validated(
                    entry,
                    &prepare,
                    &journal_prepare,
                )?;
                let prepare_replay_cost_bytes =
                    metadata_table_apply::namespace_prepare_replay_cost_bytes(&prepare);
                let ops = namespace_prepare_table_ops_for_owned_record(
                    &self.tables,
                    &self.shard_id,
                    prepare.into_owned(),
                )?;
                if ops.is_empty() {
                    return Ok(false);
                }
                // The table plan owns the one canonical allocation produced by
                // PackedV2 unpacking. Hash that exact allocation before it is
                // moved into the table, then carry only the compact identity to
                // the immediate authority ACK. Re-reading the table would clone
                // and normalize the complete prepare a second time.
                let namespace_authority_proof = (evidence.collect_namespace_authority_proof
                    && defer_trailing_resolved_op)
                    .then(|| {
                        ops.iter().find_map(|op| match op {
                            DfsMetadataTableOp::PutNamespacePrepare { prepare } => Some(prepare),
                            _ => None,
                        })
                    })
                    .flatten()
                    .map(|prepare| {
                        crate::service_replica::metadata_namespace_prepare_payload_digest_normalized(
                            prepare,
                        )
                        .map_err(|error| MetadataBackendError::Serde(error.to_string()))
                        .map(|canonical_digest| {
                            DfsValidatedNamespaceAuthorityProof::from_canonical_prepare(
                                prepare,
                                entry.sequence,
                                canonical_digest,
                            )
                        })
                    })
                    .transpose()?
                    .flatten();
                let defer_resolved_prepare = defer_trailing_resolved_op
                    && matches!(status, DfsNamespacePrepareStatus::Prepared)
                    && matches!(
                        ops.last(),
                        Some(DfsMetadataTableOp::PutNamespacePrepare { .. })
                    );
                let mut persisted_prepare_reference = false;
                for op in ops {
                    if matches!(&op, DfsMetadataTableOp::PutNamespacePrepare { .. }) {
                        if defer_resolved_prepare {
                            if !post_append_ops.is_empty() {
                                return Err(MetadataBackendError::Serde(
                                    "metadata journal batch cannot defer more than one resolved namespace operation"
                                        .to_string(),
                                ));
                            }
                            table_ops.push(prepare_reference.clone());
                            post_append_ops.push(op);
                        } else {
                            self.plan_preapplied_table_op(op, table_ops, rollback);
                            let persisted = table_ops.last_mut().ok_or_else(|| {
                                MetadataBackendError::Serde(format!(
                                    "journal namespace prepare {operation_id} did not append its preapplied table operation"
                                ))
                            })?;
                            *persisted = prepare_reference.clone();
                        }
                        persisted_prepare_reference = true;
                    } else if matches!(&op, DfsMetadataTableOp::CommitNamespacePrepare { .. }) {
                        self.tables
                            .apply_checked_with_rollback(op.clone(), rollback)?;
                        table_ops.push(op);
                    } else {
                        self.plan_preapplied_table_op(op, table_ops, rollback);
                    }
                }
                if !persisted_prepare_reference {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare {operation_id} did not materialize a journal-referenced prepare"
                    )));
                }
                *journal_reference_replay_cost_bytes =
                    journal_reference_replay_cost_bytes.saturating_add(prepare_replay_cost_bytes);
                if let Some(proof) = namespace_authority_proof {
                    evidence.namespace_authority_proof = Some(proof);
                }
                Ok(true)
            }
            DfsMetadataMutation::CommitNamespacePrepare {
                file_system_id,
                operation_id,
                operation_digest,
                owner_epoch,
                object_ids,
            } => {
                let ops = namespace_prepare_commit_table_ops_for_reference(
                    &self.tables,
                    &self.shard_id,
                    file_system_id,
                    operation_id,
                    operation_digest,
                    *owner_epoch,
                    object_ids,
                    entry.created_at,
                )?;
                if ops.is_empty() {
                    return Ok(false);
                }
                let defer_compact_marker = defer_trailing_resolved_op
                    && ops.len() == 1
                    && matches!(
                        ops.first(),
                        Some(DfsMetadataTableOp::CommitNamespacePrepare { .. })
                    );
                for op in ops {
                    if defer_compact_marker {
                        if !post_append_ops.is_empty() {
                            return Err(MetadataBackendError::Serde("metadata journal batch cannot defer more than one compact namespace commit".to_string()));
                        }
                        table_ops.push(op.clone());
                        post_append_ops.push(op);
                    } else if matches!(op, DfsMetadataTableOp::CommitNamespacePrepare { .. }) {
                        self.tables
                            .apply_checked_with_rollback(op.clone(), rollback)?;
                        table_ops.push(op);
                    } else {
                        self.plan_preapplied_table_op(op, table_ops, rollback);
                    }
                }
                Ok(true)
            }
            DfsMetadataMutation::PutNamespacePrepareReceipt {
                file_system_id,
                operation_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.operation_id != *operation_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace prepare receipt scope {file_system_id}/{operation_id} does not match its payload"
                    )));
                }
                if self.tables.namespace_prepare_receipts.get(operation_id) == Some(receipt) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespacePrepareReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveNamespacePrepareReceipt {
                operation_id,
                retention,
                ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveNamespacePrepareReceipt {
                        operation_id: operation_id.clone(),
                        retention: retention.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceJob { job_id, job, .. } => {
                let job = required_replay_payload(entry, "put-namespace-job", job.as_ref())?;
                if job.job_id != *job_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job id {job_id} does not match payload {}",
                        job.job_id
                    )));
                }
                if let Some(receipt) = self.tables.namespace_job_receipts.get(job_id) {
                    if receipt.matches_terminal_job(job) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job {job_id} conflicts with its compacted receipt"
                    )));
                }
                if self.tables.namespace_jobs.get(job_id) == Some(job) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceJob { job: job.clone() },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceJobReceipt {
                file_system_id,
                job_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id || receipt.job_id != *job_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job receipt scope {file_system_id}/{job_id} does not match its payload"
                    )));
                }
                if receipt.status == DfsNamespaceJobStatus::Running {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job receipt {job_id} is not terminal"
                    )));
                }
                if let Some(job) = self.tables.namespace_jobs.get(job_id)
                    && !receipt.matches_terminal_job(job)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace job receipt {job_id} conflicts with its full job"
                    )));
                }
                if self.tables.namespace_job_receipts.get(job_id) == Some(receipt) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceJobReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveNamespaceJobReceipt {
                job_id, retention, ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveNamespaceJobReceipt {
                        job_id: job_id.clone(),
                        retention: retention.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceRequest {
                requester_node_id,
                request_id,
                request,
                ..
            } => {
                if request.requester_node_id != *requester_node_id
                    || request.request_id != *request_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request key {requester_node_id}/{request_id} does not match its payload"
                    )));
                }
                let key = (requester_node_id.clone(), request_id.clone());
                if let Some(receipt) = self.tables.namespace_request_receipts.get(&key) {
                    if receipt.matches_completed_request(request) {
                        return Ok(false);
                    }
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request {requester_node_id}/{request_id} conflicts with its compacted receipt"
                    )));
                }
                if self.tables.namespace_requests.get(&key) == Some(request.as_ref()) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::put_namespace_request(request.as_ref().clone()),
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::PutNamespaceRequestReceipt {
                file_system_id,
                requester_node_id,
                request_id,
                receipt,
            } => {
                if receipt.file_system_id != *file_system_id
                    || receipt.requester_node_id != *requester_node_id
                    || receipt.request_id != *request_id
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "journal namespace request receipt scope {file_system_id}/{requester_node_id}/{request_id} does not match its payload"
                    )));
                }
                let key = (requester_node_id.clone(), request_id.clone());
                if self.tables.namespace_request_receipts.get(&key) == Some(receipt) {
                    return Ok(false);
                }
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::PutNamespaceRequestReceipt {
                        receipt: receipt.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
            DfsMetadataMutation::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                retention,
                ..
            } => {
                self.plan_preapplied_table_op(
                    DfsMetadataTableOp::RemoveNamespaceRequestReceipt {
                        requester_node_id: requester_node_id.clone(),
                        request_id: request_id.clone(),
                        retention: retention.clone(),
                    },
                    table_ops,
                    rollback,
                );
                Ok(true)
            }
        }
    }
}
