//! Routed response metadata-journal replay and catch-up helpers.
//!
//! Routed writes and routed namespace operations both carry metadata journal
//! batches back to the requester so the requester can read its own write. This
//! module owns the shared response-batch collection and replay validation
//! rather than leaving that logic in the generic inbound protocol dispatcher.

use sinomo_core::node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn local_metadata_journal_after_sequence_for_shard(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<u64>, DfsError> {
        if let Some(after_sequence) = self.local_metadata_journal_cursors.get(shard_id) {
            return Ok(Some(*after_sequence));
        }
        self.metadata_journal_last_sequence_for_shard(shard_id)
    }

    pub(crate) fn routed_write_response_metadata_journal_batches(
        &mut self,
        object_id: &DfsObjectId,
        result: &DfsDistributedWriteResult,
        after_sequence: Option<u64>,
        max_batches: usize,
    ) -> Result<Vec<DfsMetadataJournalBatch>, DfsError> {
        let Some(after_sequence) = after_sequence else {
            return Ok(Vec::new());
        };
        if !matches!(
            result,
            DfsDistributedWriteResult::Outcome {
                outcome: NetworkedDfsWriteOutcome::Committed { .. }
            }
        ) {
            return Ok(Vec::new());
        }
        let object = self.current_read_object_metadata(object_id)?;
        let Some(shard) = self.select_metadata_shard_for_object(&object)? else {
            return Ok(Vec::new());
        };
        if shard.primary_node_id != self.node_id {
            return Ok(Vec::new());
        }
        self.metadata_journal_batches_for_shard_since_limited(
            &DfsMetadataShardId::new(shard.shard_id),
            after_sequence,
            max_batches,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
    }

    pub(crate) fn metadata_journal_batches_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        mut after_sequence: u64,
        max_batches: usize,
        max_entries_per_batch: usize,
        max_encoded_bytes_per_batch: usize,
    ) -> Result<Vec<DfsMetadataJournalBatch>, DfsError> {
        let mut batches = Vec::new();
        for _ in 0..max_batches {
            let Some(batch) = self.metadata_journal_batch_for_shard_since_limited(
                shard_id,
                after_sequence,
                max_entries_per_batch,
                max_encoded_bytes_per_batch,
            )?
            else {
                break;
            };
            if batch.entries.is_empty() || batch.next_after_sequence <= after_sequence {
                break;
            }
            after_sequence = batch.next_after_sequence;
            batches.push(batch);
        }
        Ok(batches)
    }

    pub(crate) fn apply_routed_write_metadata_journal_batch(
        &mut self,
        primary_node_id: &NodeId,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        if let Some(reason) = self.validate_routed_response_journal_batch(batch)? {
            return Err(DfsError::Distributed { message: reason });
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            if let Err(error) = backend.apply_journal_batch(batch) {
                let reason = error.to_string();
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    self.request_metadata_snapshot_repair_from_primary(
                        primary_node_id,
                        &batch.shard_id,
                        batch.next_after_sequence,
                        reason,
                    )?;
                    return Ok(());
                }
                return Err(error.into());
            }
        }
        self.apply_metadata_journal_batch_to_service_state(batch)?;
        Ok(())
    }

    fn request_metadata_snapshot_repair_from_primary(
        &mut self,
        primary_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        reason: String,
    ) -> Result<(), DfsError> {
        // Older primaries already recognize the sequence-conflict rejection
        // family as snapshot-repairable. Preserve that wire-level signal when
        // a newer mirror detects another exact-prefix discontinuity so a
        // mixed-version cluster can still complete repair.
        let reason = format!(
            "metadata journal sequence discontinuity already exists with a different entry; routed mirror snapshot repair requested after: {reason}"
        );
        let request_id = format!(
            "dfs_meta_snapshot_request_{}_{}_{}",
            self.node_id, shard_id.0, next_after_sequence
        );
        self.send_dfs_message_if_routable(
            primary_node_id,
            DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                request_id,
                shard_id: shard_id.clone(),
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Rejected { reason },
            }),
        )?;
        Ok(())
    }

    /// Validate journal batches carried in a routed-operation response.
    ///
    /// The responder was addressed as the shard authority, possibly through an
    /// authority redirect the local shard view does not reflect yet, and the
    /// response transport source was already verified against the addressed
    /// node. The batch source is therefore not rechecked against the possibly
    /// stale local shard config. The local node still refuses to overwrite a
    /// shard it owns.
    fn validate_routed_response_journal_batch(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<String>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Ok(None);
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
        else {
            return Ok(Some(format!(
                "metadata journal batch has no configured shard {}",
                batch.shard_id.0
            )));
        };
        if shard.primary_node_id == self.node_id {
            return Ok(Some(
                "metadata primary must not apply a replica journal batch".to_string(),
            ));
        }
        Ok(None)
    }

    pub(crate) fn routed_response_journal_batch_targets_local_primary(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<bool, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Ok(false);
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
        else {
            return Ok(false);
        };
        Ok(shard.primary_node_id == self.node_id)
    }
}
