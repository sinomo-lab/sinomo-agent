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
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<u64>, DfsError> {
        self.refresh_routed_replica_projection(shard_id)?;
        if let Some(after_sequence) = self
            .replication
            .local_metadata_journal_cursors
            .get(shard_id)
        {
            return Ok(Some(*after_sequence));
        }
        self.metadata_journal_last_sequence_for_shard(shard_id)
    }

    fn await_routed_snapshot_refresh(
        &mut self,
        authority: &DfsMetadataShardConfig,
        shard_id: &DfsMetadataShardId,
    ) -> Result<(), DfsError> {
        if self
            .replication
            .routed_replica_projections
            .get(shard_id)
            .is_some_and(|pending| pending.authority == *authority)
        {
            return Ok(());
        }
        let Some(observer) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.subscribe_metadata_replica_progress(shard_id))
            .transpose()?
            .flatten()
        else {
            return Ok(());
        };
        let observed_generation = observer.latest().map_or(0, |observed| observed.generation);
        self.replication.routed_replica_projections.insert(
            shard_id.clone(),
            crate::service_state::RoutedReplicaProjection {
                authority: authority.clone(),
                observer,
                observed_generation,
            },
        );
        Ok(())
    }

    pub(crate) fn refresh_routed_replica_projection(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<(), DfsError> {
        // Replica RPC lanes share durable storage with long-lived requesters.
        // Subscribe before the first routed cursor is sent, not only after an
        // obsolete response has already forced a snapshot repair.
        if !self
            .replication
            .routed_replica_projections
            .contains_key(shard_id)
        {
            let Some(observer) = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.subscribe_metadata_replica_progress(shard_id))
                .transpose()?
                .flatten()
            else {
                return Ok(());
            };
            let Some(authority) = self.metadata_shard_config_for_shard_id(shard_id)? else {
                return Ok(());
            };
            if authority.primary_node_id == self.node_id {
                return Ok(());
            }
            self.replication.routed_replica_projections.insert(
                shard_id.clone(),
                crate::service_state::RoutedReplicaProjection {
                    authority,
                    observer,
                    observed_generation: 0,
                },
            );
        }
        let Some(pending) = self
            .replication
            .routed_replica_projections
            .get(shard_id)
            .cloned()
        else {
            return Ok(());
        };
        let Some(observed) = pending.observer.latest() else {
            return Ok(());
        };
        if observed.generation <= pending.observed_generation {
            return Ok(());
        }
        let _refresh = tracing::trace_span!(target: "sinomo.perf", "dfs.replica.projection",
            stage = "dfs.replica.projection", shard = %shard_id.0,
            observed_sequence = observed.prefix.journal_sequence)
        .entered();
        // Replica progress is only a wake signal. Revalidate its source against
        // current authority, then obtain the actual durable prefix and guards
        // from one exact shard view. Never advance on repair-send success.
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(());
        };
        let current = DfsControlPlaneStore::new(control_plane.as_mut()).get_metadata_shard(
            &pending.authority.file_system_id,
            &pending.authority.shard_id,
        )?;
        if current.as_ref() != Some(&pending.authority) {
            // A repaired old assignment must not stall routing after handoff.
            // Discard its observer without adopting any old cursor or guards.
            self.replication.routed_replica_projections.remove(shard_id);
            return Ok(());
        }
        if observed.shard_id != *shard_id
            || observed.file_system_id != pending.authority.file_system_id
            || observed.source_node_id != pending.authority.primary_node_id
            || observed.source_epoch != pending.authority.epoch
        {
            return Ok(());
        }
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let Some(prefix) = tracing::trace_span!(target: "sinomo.perf", "dfs.replica.projection",
            stage = "dfs.replica.projection", phase = "pin_durable_prefix")
        .in_scope(|| backend.begin_replica_projection_read(&observed))?
        else {
            return Ok(());
        };
        let projection = tracing::trace_span!(target: "sinomo.perf", "dfs.replica.projection",
            stage = "dfs.replica.projection", phase = "refresh_guards_and_objects")
        .in_scope(|| {
            self.replica_namespace_guard_projection(shard_id)
                .and_then(|guards| {
                    self.replica_object_projections_to_retire(shard_id)
                        .map(|objects| (guards, objects))
                })
        });
        self.metadata_backend
            .as_mut()
            .expect("snapshot backend remains installed")
            .end_replica_projection_read(shard_id);
        let (guards, objects) = projection?;
        self.retire_replica_object_projections(objects);
        self.cross_shard_namespace_locks
            .replace_shard_jobs(shard_id, guards);
        self.replication
            .local_metadata_journal_cursors
            .insert(shard_id.clone(), prefix.journal_sequence);
        // Retain the subscription for the requester's lifetime. A newer hint
        // arriving during the pinned read remains pending for the next call.
        if let Some(retained) = self
            .replication
            .routed_replica_projections
            .get_mut(shard_id)
        {
            retained.observed_generation = observed.generation;
        }
        self.clear_metadata_lookup_cache();
        crate::dfs_perf_trace(format_args!(
            "routed-replica-projection-refreshed node={} shard={} observed_sequence={} materialized_sequence={}",
            self.node_id, shard_id.0, observed.prefix.journal_sequence, prefix.journal_sequence,
        ));
        Ok(())
    }

    pub(crate) fn routed_write_response_metadata_journal_batches(
        &mut self,
        object_id: &DfsObjectId,
        requester_node_id: &NodeId,
        result: &DfsDistributedWriteResult,
        after_sequence: Option<u64>,
        max_batches: usize,
    ) -> Result<Vec<DfsMetadataJournalBatch>, DfsError> {
        let Some(after_sequence) = after_sequence else {
            return Ok(Vec::new());
        };
        let DfsDistributedWriteResult::Outcome {
            outcome: NetworkedDfsWriteOutcome::Committed { intent_id, .. },
        } = result
        else {
            return Ok(Vec::new());
        };
        // A blind path write may have been rebound to a hardlink's canonical
        // inode. Keep the original id on the response envelope, but select
        // its journal using the inode the authoritative intent actually wrote.
        let committed_object_id = self
            .coordinator
            .intent(intent_id)
            .map(|intent| intent.object_id.clone());
        let object_id = committed_object_id.as_ref().unwrap_or(object_id);
        let object = self.current_read_object_metadata(object_id)?;
        let Some(shard) = self.select_metadata_shard_for_object(&object)? else {
            return Ok(Vec::new());
        };
        if shard.primary_node_id != self.node_id {
            return Ok(Vec::new());
        }
        self.metadata_journal_batches_for_shard_since_limited(
            &DfsMetadataShardId::new(shard.shard_id),
            requester_node_id,
            "routed-write",
            after_sequence,
            max_batches,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn metadata_journal_batches_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        operation: &'static str,
        mut after_sequence: u64,
        max_batches: usize,
        max_entries_per_batch: usize,
        max_encoded_bytes_per_batch: usize,
    ) -> Result<Vec<DfsMetadataJournalBatch>, DfsError> {
        let requested_after = after_sequence;
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
        let entries: usize = batches.iter().map(|batch| batch.entries.len()).sum();
        if entries >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
            || (requested_after == 0 && entries > 0)
        {
            crate::catchup_diagnostics::record(
                &self.node_id,
                requester_node_id,
                shard_id,
                operation,
                "journal-backfill",
                Some(requested_after),
                Some(after_sequence),
                "routed operation requested historical journal backfill",
            );
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
                        if error.rejection_code().allows_snapshot_repair() {
                            self.await_routed_snapshot_refresh(
                                validated_authority_shard,
                                &batch.shard_id,
                            )?;
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
        if self.metadata_backend.is_some() {
            // A routed response grants replica ingest, never authority to
            // rewrite the primary's intents. Full coordinator hydration may
            // reject historical conflicting intents by authoring new journal
            // entries, which also fails on a fenced retiring primary. Use the
            // same observation path as ordinary inbound replica batches;
            // refresh affected objects to retire a routed writer's optimistic
            // projection from the final durable view, without replaying intents.
            self.refresh_metadata_journal_batch_objects_from_backend(batch)?;
            self.observe_replicated_metadata_journal_batch(batch)?;
        } else {
            self.apply_metadata_journal_batch_to_service_state(batch)?;
        }
        Ok(())
    }

    fn request_metadata_snapshot_repair_from_primary(
        &mut self,
        primary_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        reason: String,
    ) -> Result<(), DfsError> {
        crate::catchup_diagnostics::record(
            &self.node_id,
            primary_node_id,
            shard_id,
            "routed-response",
            "snapshot-repair",
            self.replication
                .local_metadata_journal_cursors
                .get(shard_id)
                .copied(),
            Some(next_after_sequence),
            &reason,
        );
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
                result: DfsMetadataJournalBatchResult::rejected(
                    DfsMetadataRejectionCode::ReplicaDiverged,
                    reason,
                ),
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
