//! Recovery of hidden metadata prepares after shard-authority movement.
//!
//! Visibility still goes through the normal canonical authorization path.
//! This module only adopts durable, non-visible work after proving that the
//! local node owns the exact same shard at a newer (or equal) certified epoch.

use std::collections::{BTreeMap, BTreeSet};

use time::OffsetDateTime;

use crate::*;

fn sort_prepared_operations(prepares: &mut [DfsPreparedOperationRecord]) {
    prepares.sort_by(|left, right| left.digest.operation_id.cmp(&right.digest.operation_id));
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
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(true);
        };
        let mut records = backend.list_prepared_operations()?;
        sort_prepared_operations(&mut records);
        for record in records {
            if should_cancel() {
                return Ok(false);
            }
            // Current complete bodies already seal W receipts and terminal
            // intent, so content and namespace use the same recovery decision.
            self.recover_one_prepared_operation(record)?;
        }
        Ok(!should_cancel())
    }

    pub(crate) fn recover_durable_namespace_prepares_for_requests(
        &mut self,
        requests: &BTreeSet<(NodeId, String)>,
        max_prepares: usize,
    ) -> Result<usize, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(0);
        };
        let mut records = Vec::new();
        for (requester, request) in requests {
            if let Some(record) = backend.prepared_operation_for_request(requester, request)? {
                records.push(record);
            }
        }
        sort_prepared_operations(&mut records);
        let mut recovered = 0;
        for record in records.into_iter().take(max_prepares) {
            recovered += usize::from(self.recover_one_prepared_operation(record)?);
        }
        Ok(recovered)
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
            // The original coordinator owns issuance/abort and the exact
            // parent ancestry. Generic recovery cannot mint a recover: ID or
            // commit this participant independently of that durable decision.
            record.validate()?;
            let (participant, _) = record.metadata_effects.iter().find_map(|effect| match effect {
                DfsPreparedMetadataEffect::PutNamespaceJob { job } => job.parent_count_participant(),
                _ => None,
            }).ok_or_else(|| DfsError::Metadata { message: "parent prepare omitted its declared participant".into() })?;
            participant.validate_phase_payload(&record.payload)?;
            return Ok(false);
        }
        if self.control_plane.is_none() {
            if record.digest.owner_epoch == 0 {
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
        if shard.epoch != record.digest.owner_epoch {
            let operation_id = format!("recover:{}:{}", record.digest.operation_id, shard.epoch);
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

    fn finish_recovered_prepared_publication(
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
