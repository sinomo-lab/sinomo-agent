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
        validated_authority_shard: &DfsMetadataShardConfig,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        let permit = self.validate_routed_response_journal_batch(
            primary_node_id,
            validated_authority_shard,
            batch,
        )?;
        if let Some(backend) = self.metadata_backend.as_mut() {
            let applied_entries =
                match backend.apply_journal_batch_with_replica_permit(&permit, batch) {
                    Ok(applied_entries) => applied_entries,
                    Err(error) => {
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
                };
            // Routed reads and write responses can advance this process's
            // durable replica directly, outside the ordinary inbound journal
            // handler. Retain the same daemon-owned progress edge before the
            // later fallible service-state hydration so mounted callers can
            // hand it off even when that later step fails.
            if applied_entries > 0 {
                self.record_message_committed_work(DfsMessageCommittedWork {
                    metadata_replica_progress: true,
                    ..DfsMessageCommittedWork::default()
                });
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

    /// Validate a successful routed-write response against the exact committed
    /// authority snapshot that selected its transport destination.
    ///
    /// This check is independent of journal replay because a valid response may
    /// carry no journal batches. Rejected responses use their separate
    /// corroborated-redirect path instead of this completion gate.
    pub(crate) fn validate_routed_write_response_authority(
        &mut self,
        primary_node_id: &NodeId,
        expected_authority_shard: &DfsMetadataShardConfig,
    ) -> Result<DfsMetadataShardConfig, DfsError> {
        if primary_node_id != &expected_authority_shard.primary_node_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed response source {primary_node_id} does not match dispatch primary {} for shard {} epoch {}",
                    expected_authority_shard.primary_node_id,
                    expected_authority_shard.shard_id,
                    expected_authority_shard.epoch
                ),
            });
        }
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message: "routed write response requires a committed control-plane shard view"
                    .to_string(),
            });
        };
        let Some(current) = DfsControlPlaneStore::new(control_plane.as_mut()).get_metadata_shard(
            &expected_authority_shard.file_system_id,
            &expected_authority_shard.shard_id,
        )?
        else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write response has no configured shard {}",
                    expected_authority_shard.shard_id
                ),
            });
        };
        if current != *expected_authority_shard {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write response dispatch authority for shard {} epoch {} is no longer current (current primary {}, epoch {})",
                    expected_authority_shard.shard_id,
                    expected_authority_shard.epoch,
                    current.primary_node_id,
                    current.epoch
                ),
            });
        }
        Ok(current)
    }

    /// Validate journal batches carried in a routed-operation response.
    ///
    /// After the response authority check, bind the batch identity to that same
    /// immutable assignment before minting the narrow replica-ingest permit.
    fn validate_routed_response_journal_batch(
        &self,
        primary_node_id: &NodeId,
        validated_authority_shard: &DfsMetadataShardConfig,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaIngestPermit, DfsError> {
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed response journal batch for shard {} has no filesystem identity",
                    batch.shard_id.0
                ),
            });
        };
        if file_system_id != validated_authority_shard.file_system_id
            || batch.shard_id.0 != validated_authority_shard.shard_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed response journal batch identity does not match dispatch authority filesystem {} shard {} epoch {}",
                    validated_authority_shard.file_system_id,
                    validated_authority_shard.shard_id,
                    validated_authority_shard.epoch
                ),
            });
        }
        if primary_node_id != &validated_authority_shard.primary_node_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed response source {primary_node_id} does not match validated dispatch primary {} for shard {} epoch {}",
                    validated_authority_shard.primary_node_id,
                    validated_authority_shard.shard_id,
                    validated_authority_shard.epoch
                ),
            });
        }
        if validated_authority_shard.primary_node_id == self.node_id {
            return Err(DfsError::Distributed {
                message: "metadata primary must not apply a replica journal batch".to_string(),
            });
        }
        Ok(DfsMetadataReplicaIngestPermit::new_validated(
            validated_authority_shard.file_system_id.clone(),
            batch.shard_id.clone(),
            primary_node_id.clone(),
            validated_authority_shard.epoch,
        ))
    }
}
