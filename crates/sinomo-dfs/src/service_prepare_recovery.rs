//! Recovery of hidden metadata prepares after shard-authority movement.
//!
//! Visibility still goes through the normal canonical authorization path.
//! This module only adopts durable, non-visible work after proving that the
//! local node owns the exact same shard at a newer (or equal) certified epoch.

use std::collections::{BTreeMap, BTreeSet};

use time::OffsetDateTime;

use crate::*;

/// Result of one pass over durable prepares.
pub(crate) struct DfsPreparedRecoveryPass {
    /// False when cancellation stopped the pass or a body's commit outcome
    /// stayed unknown, so the pass must run again.
    pub(crate) completed: bool,
    /// The first body whose commit outcome stayed unknown.
    pub(crate) first_unknown: Option<DfsError>,
}

/// Journal order on each shard: a prepare recovers after the prepares its
/// primary made before it, as a request's admission before its completion.
fn sort_prepared_operations(prepares: &mut [DfsPreparedOperationRecord]) {
    prepares.sort_by(|left, right| {
        (
            &left.digest.shard_id,
            left.digest.prepare_sequence,
            &left.digest.operation_id,
        )
            .cmp(&(
                &right.digest.shard_id,
                right.digest.prepare_sequence,
                &right.digest.operation_id,
            ))
    });
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn certified_local_recovery_shard(
        &mut self,
        _object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
        owner_epoch: u64,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(context) =
            self.retained_metadata_replica_context(&_object.file_system_id, shard_id)?
        else {
            return Ok(None);
        };
        let shard = context.shard;
        if shard.primary_node_id != self.node_id || shard.epoch < owner_epoch {
            return Ok(None);
        }
        Ok(Some(shard))
    }

    #[cfg(test)]
    pub(crate) fn recover_durable_namespace_prepares(&mut self) -> Result<(), DfsError> {
        self.recover_durable_namespace_prepares_until_cancelled(&|| false)?;
        Ok(())
    }

    pub(crate) fn recover_durable_namespace_prepares_until_cancelled(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        Ok(self
            .recover_durable_namespace_prepares_pass(should_cancel)?
            .completed)
    }

    /// One recovery pass over every durable prepare. A body whose commit
    /// outcome stays unknown, for example because a member it needs has not
    /// acknowledged or it is below its own commit floor, stays prepared and
    /// does not stop the pass: each body has its own requirements.
    pub(crate) fn recover_durable_namespace_prepares_pass(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<DfsPreparedRecoveryPass, DfsError> {
        let mut pass = DfsPreparedRecoveryPass {
            completed: true,
            first_unknown: None,
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(pass);
        };
        let mut records = backend.list_prepared_operations()?;
        sort_prepared_operations(&mut records);
        for record in records {
            if should_cancel() {
                pass.completed = false;
                return Ok(pass);
            }
            // Current complete bodies already seal W receipts and terminal
            // intent, so content and namespace use the same recovery decision.
            match self.recover_one_prepared_operation(record) {
                Ok(_) => {}
                Err(error) if error.is_commit_outcome_unknown() => {
                    // The other bodies are still attempted, but the pass is
                    // not complete: its owner runs recovery again.
                    let cancelled = error.is_cancellation();
                    pass.first_unknown.get_or_insert(error);
                    if cancelled {
                        pass.completed = false;
                        return Ok(pass);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        pass.completed = pass.first_unknown.is_none() && !should_cancel();
        Ok(pass)
    }

    /// Recover the live prepares of at most `max_requests` of `requests`,
    /// each on its authority shard, and return how many requests recovered
    /// any. A request's prepares recover in journal order, its row prepares
    /// first; the first whose commit outcome stays unknown ends that
    /// request's turn, since what follows it depends on it.
    pub(crate) fn recover_durable_namespace_prepares_for_requests(
        &mut self,
        requests: &BTreeSet<(DfsMetadataShardId, NodeId, String)>,
        max_requests: usize,
    ) -> Result<usize, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(0);
        };
        let mut pending = Vec::new();
        for (shard_id, requester, request) in requests {
            if pending.len() >= max_requests {
                break;
            }
            let mut records =
                backend.prepared_operations_for_request(shard_id, requester, request)?;
            if !records.is_empty() {
                sort_prepared_operations(&mut records);
                pending.push(records);
            }
        }
        let mut recovered = 0;
        for records in pending {
            let mut done_any = false;
            for record in records {
                // An unknown outcome leaves the request pending; its admission
                // answers the caller with that unknown outcome. Cancellation
                // still stops this caller, which keeps the unfinished turn.
                match self.recover_one_prepared_operation(record) {
                    Ok(done) => done_any |= done,
                    Err(error) if error.is_commit_outcome_unknown() && !error.is_cancellation() => {
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            recovered += usize::from(done_any);
        }
        Ok(recovered)
    }

    /// Abort `record` if it is a row prepare of a request whose final
    /// outcome the shard already holds: a request's receipt and its
    /// completed row are final, so a row prepare that would replace either
    /// can never commit. Only the shard's current primary, at `epoch`, does
    /// so. Returns whether `record` was such a prepare.
    fn retire_obsolete_namespace_admission(
        &mut self,
        record: &DfsPreparedOperationRecord,
        epoch: u64,
    ) -> Result<bool, DfsError> {
        let rows = record
            .metadata_effects
            .iter()
            .filter_map(|effect| match effect {
                DfsPreparedMetadataEffect::PutNamespaceRequest { request } => {
                    Some(request.as_ref().clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(false);
        }
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "namespace admission recovery requires the current backend".into(),
            })?;
        let mut obsolete = false;
        for row in &rows {
            let outcome = backend.namespace_request_admission_snapshot(
                &record.digest.shard_id,
                &row.requester_node_id,
                &row.request_id,
            )?;
            if crate::metadata_backend::prepared_operation::namespace_request_row_replaces_final_outcome(
                outcome.receipt.is_some(),
                outcome.request.as_ref(),
                row,
            ) {
                obsolete = true;
                break;
            }
        }
        if !obsolete {
            return Ok(false);
        }
        let operation_id =
            crate::metadata_records::retired_admission_operation_id(&record.digest.operation_id);
        if let Some(retiring) = backend.prepared_operation_record(&operation_id)? {
            // An earlier retirement whose own commit is unknown, or that an
            // earlier primary prepared, recovers as any other prepare.
            if retiring.status == DfsPreparedOperationStatus::Prepared {
                return self.recover_one_prepared_operation(retiring);
            }
            return Ok(true);
        }
        self.publish_metadata_effects_on_shard(
            DfsPreparedOperationScope {
                file_system_id: record.digest.file_system_id.clone(),
                shard_id: record.digest.shard_id.clone(),
                owner_epoch: epoch,
                operation_id,
            },
            Vec::new(),
            Vec::new(),
            vec![DfsPreparedMetadataEffect::ResolvePreparedOperation {
                digest: record.digest.clone(),
                status: DfsPreparedOperationStatus::Aborted,
            }],
        )?;
        Ok(true)
    }

    pub(crate) fn recover_one_prepared_operation(
        &mut self,
        record: DfsPreparedOperationRecord,
    ) -> Result<bool, DfsError> {
        if record.status != DfsPreparedOperationStatus::Prepared {
            return Ok(false);
        }
        if record.metadata_effects.iter().any(|effect| matches!(effect,
            DfsPreparedMetadataEffect::PutNamespaceJob { job }
                if job.kind == DfsNamespaceJobKind::ParentCountParticipant)) {
            // A participant recovers under its own id, never a recover: id:
            // its coordinator sends each phase under that id.
            record.validate()?;
            let (participant, _) = record.metadata_effects.iter().find_map(|effect| match effect {
                DfsPreparedMetadataEffect::PutNamespaceJob { job } => job.parent_count_participant(),
                _ => None,
            }).ok_or_else(|| DfsError::Metadata { message: "parent prepare omitted its declared participant".into() })?;
            participant.validate_phase_payload(&record.payload)?;
            let participant = participant.clone();
            return self.recover_parent_count_participant_prepare(&record, &participant);
        }
        if self.control_plane.is_none() {
            if record.digest.owner_epoch == 0 {
                if self.retire_obsolete_namespace_admission(&record, 0)? {
                    return Ok(true);
                }
                let committed = self.commit_stored_prepared_operation(&record.digest)?;
                self.finish_recovered_prepared_publication(
                    &committed.payload,
                    committed.published,
                )?;
                return Ok(true);
            }
            return Ok(false);
        };
        let Some(context) = self.retained_metadata_replica_context(
            &record.digest.file_system_id,
            &record.digest.shard_id,
        )?
        else {
            return Ok(false);
        };
        let shard = context.shard;
        if shard.primary_node_id != self.node_id || shard.epoch < record.digest.owner_epoch {
            return Ok(false);
        }
        if self.retire_obsolete_namespace_admission(&record, shard.epoch)? {
            return Ok(true);
        }
        if shard.epoch != record.digest.owner_epoch {
            let operation_id = crate::metadata_records::recovery_operation_id(
                &record.digest.operation_id,
                shard.epoch,
            );
            if let Some(digest) = self
                .metadata_backend
                .as_ref()
                .expect("recovery backend")
                .prepared_operation_digest(&operation_id)?
            {
                let committed = self.commit_stored_prepared_operation(&digest)?;
                self.finish_recovered_prepared_publication(
                    &committed.payload,
                    committed.published,
                )?;
                return Ok(true);
            }
            let mut payload = (*record.payload).clone();
            payload.operation_id = operation_id;
            payload.owner_epoch = shard.epoch;
            if let Some(intent) = payload.prepared_intent.as_mut() {
                intent.owner_epoch = shard.epoch;
            }
            if let Some(intent) = payload.terminal_intent.as_mut() {
                intent.owner_epoch = shard.epoch;
            }
            payload
                .metadata_effects
                .push(DfsPreparedMetadataEffect::ResolvePreparedOperation {
                    digest: record.digest.clone(),
                    status: DfsPreparedOperationStatus::Superseded,
                });
            // Original read-set, content evidence, effects and caller identity
            // remain immutable. The new owner certifies a new complete body;
            // old reservation retirement is atomic with this guarded commit.
            let committed = self.publish_prepared_operation(payload)?;
            self.finish_recovered_prepared_publication(&committed.payload, committed.published)?;
            return Ok(true);
        }
        let committed = self.commit_stored_prepared_operation(&record.digest)?;
        self.finish_recovered_prepared_publication(&committed.payload, committed.published)?;
        Ok(true)
    }

    pub(crate) fn finish_recovered_prepared_publication(
        &mut self,
        payload: &DfsPreparedOperationPayload,
        published: bool,
    ) -> Result<(), DfsError> {
        if published {
            // Keep the real commit's follow-up work even if rebuilding a local
            // projection fails. Recovery must publish its commit marker too.
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                semantic_changes: true,
                repair_maintenance: payload.terminal_intent.is_some(),
                namespace_maintenance: payload.metadata_effects.iter().any(|effect| {
                    matches!(effect, DfsPreparedMetadataEffect::PutNamespaceJob { .. })
                }),
                ..Default::default()
            });
        }
        self.refresh_recorded_prepared_rows(payload)?;
        if let Some(intent) = payload.terminal_intent.as_ref() {
            self.refresh_committed_write_projection(&intent.intent_id)?;
        }
        Ok(())
    }

    /// Map each durable write intent to the exact local metadata shard that
    /// contains it. A combined snapshot loses this provenance, so recovery
    /// intentionally uses shard-scoped snapshots. Historical copies can
    /// legitimately survive a shard move; duplicate keys are therefore
    /// omitted from this adoption map so recovery fails closed instead of
    /// assigning ambiguous work to whichever shard happened to scan last.
    pub(crate) fn durable_write_intent_shards_until_cancelled(
        &self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<Option<BTreeMap<(DfsObjectId, String), DfsMetadataShardId>>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Some(BTreeMap::new()));
        };
        if should_cancel() {
            return Ok(None);
        };
        let mut intent_shards = BTreeMap::new();
        let mut ambiguous_intents = BTreeSet::new();
        for status in backend.metadata_shard_statuses()? {
            if should_cancel() {
                return Ok(None);
            }
            let mut cursor = None;
            loop {
                if should_cancel() {
                    return Ok(None);
                }
                let Some(page) = backend.write_intents_on_shard_page(
                    &status.shard_id,
                    cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )?
                else {
                    break;
                };
                for intent in page.intents {
                    if should_cancel() {
                        return Ok(None);
                    }
                    let key = (intent.object_id, intent.intent_id);
                    if ambiguous_intents.contains(&key) {
                        continue;
                    }
                    if intent_shards
                        .insert(key.clone(), status.shard_id.clone())
                        .is_some()
                    {
                        intent_shards.remove(&key);
                        ambiguous_intents.insert(key);
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
        Ok(Some(intent_shards))
    }

    /// Recover current complete bodies under the current certified owner.
    /// A new epoch creates a new digest and atomically resolves the old reservation.
    pub(crate) fn adopt_prepared_write_intent_for_recovery(
        &mut self,
        intent: &mut DfsWriteIntentRecord,
        durable_shard_id: Option<&DfsMetadataShardId>,
    ) -> Result<bool, DfsError> {
        if !matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) {
            return Ok(false);
        }
        let Some(shard_id) = durable_shard_id else {
            return Ok(false);
        };
        let operation_id = format!("write:{}", intent.intent_id);
        let record = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "current intent recovery requires an actual prepared backend".into(),
            })?
            .prepared_operation_record(&operation_id)?;
        if let Some(record) = record {
            if record.digest.shard_id != *shard_id {
                return Ok(false);
            }
            if !self.recover_one_prepared_operation(record)? {
                return Ok(false);
            }
            if let Some(current) = self
                .metadata_backend
                .as_ref()
                .expect("recovery backend")
                .get_intent(&intent.object_id, &intent.intent_id)?
            {
                *intent = current;
                return Ok(true);
            }
            return Err(DfsError::Metadata {
                message: "committed recovery lost its sealed terminal intent".into(),
            });
        }
        // A current create can durably admit its intent/receipt before the
        // sealed content body or first visible object exists. Its saved object
        // is an identity anchor only; the same certified shard check below
        // still owns adoption. Transient transfer recovery grants neither a
        // content receipt nor a visible namespace entry.
        let object = match self.current_read_object_metadata(&intent.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { object_id })
                if object_id == intent.object_id
                    && intent.publication.is_current()
                    && intent.base_version.is_none() =>
            {
                match intent.object_metadata.as_ref().filter(|object| {
                    object.object_id == intent.object_id
                        && object.file_system_id == intent.file_system_id
                }) {
                    Some(object) => object.clone(),
                    None => return Err(DfsError::UnknownObject { object_id }),
                }
            }
            Err(error) => return Err(error),
        };
        let Some(shard) =
            self.certified_local_recovery_shard(&object, shard_id, intent.owner_epoch)?
        else {
            return Ok(false);
        };
        if intent.owner_epoch < shard.epoch {
            let mut adopted = intent.clone();
            adopted.owner_epoch = shard.epoch;
            adopted.updated_at = Some(OffsetDateTime::now_utc());
            self.publish_object_metadata_effects(
                &object,
                vec![DfsPreparedMetadataEffect::PutIntent {
                    intent: adopted.clone(),
                }],
            )?;
            *intent = adopted;
        }
        Ok(intent.owner_epoch == shard.epoch)
    }

    /// Every recovered write uses the same complete current publication path.
    pub(crate) fn apply_recovered_commit_ready_intent(
        &mut self,
        intent_id: &str,
    ) -> Result<(), DfsError> {
        if self.coordinator.intent(intent_id).is_some_and(|intent|
            intent.publication.current_precondition().is_some()) {
            self.try_apply_committed_intent_if_ready(intent_id)
        } else {
            self.apply_committed_intent(intent_id)
        }
    }
}
