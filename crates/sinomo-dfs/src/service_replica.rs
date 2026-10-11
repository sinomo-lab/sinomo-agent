//! Replica command authorization and metadata-authority helpers.
//!
//! This module keeps shard-primary proof, local authority checks, and
//! replica cache updates together so transfer handlers can validate
//! inbound stage, repair, commit, and fetch commands through one service
//! boundary.

use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::*;

const DFS_METADATA_AUTHORITY_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
/// Bound authority-side preparation when an inbound served request carries no
/// caller deadline. Healthy operations finish far inside this cap; reaching it
/// means the optional all-replica fast path should yield to the exact quorum
/// fence instead of holding the served write lane indefinitely.
const DFS_METADATA_AUTHORITY_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
// Namespace ACKs share one durable exact-frame path regardless of record
// count. Give that path enough time to get behind an already-running bounded
// materialization pass; abandoning it after the ordinary 250ms probe budget
// turns a sub-second wait into a multi-second cluster-quorum fallback.
const DFS_METADATA_AUTHORITY_NAMESPACE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const DFS_METADATA_AUTHORITY_PROBE_IDLE_WAIT: Duration = Duration::from_millis(5);

fn metadata_authority_probe_timeout(
    _shard: &DfsMetadataShardConfig,
    operation: &DfsMetadataAuthorityOperation,
    prepared_batch: &DfsMetadataJournalBatch,
) -> Duration {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        prepare_sequence, ..
    } = operation;
    if prepared_batch.entries.iter().any(|entry| entry.sequence == *prepare_sequence && matches!(
        &entry.mutation, DfsMetadataMutation::PrepareOperationV7 { record, .. } if record.payload.prepared_intent.is_none())) {
        DFS_METADATA_AUTHORITY_NAMESPACE_PROBE_TIMEOUT
    } else { DFS_METADATA_AUTHORITY_PROBE_TIMEOUT }
}

pub(crate) use crate::publication_guard::ShardFenceGuard;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Serialization is owner/backend scoped and grants no author permission.
    pub(crate) fn metadata_shard_fence_guard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<ShardFenceGuard, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "shard fencing requires an actual owned backend".into(),
            })?;
        let scope =
            backend.publication_owner_scope(file_system_id, &DfsMetadataShardId::new(shard_id))?;
        Ok(crate::publication_guard::shard_fence_guard(&scope))
    }

    pub(crate) fn select_metadata_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &object.file_system_id,
                &object.path,
            )
            .map_err(DfsError::from)
    }

    pub(crate) fn local_metadata_owner_epoch_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<u64, DfsError> {
        Ok(self
            .certified_local_metadata_owner_shard_for_object(object)?
            .map_or(0, |shard| shard.epoch))
    }

    /// Return the exact shard assignment after refreshing cluster
    /// configuration, not merely the epoch from an earlier object-to-shard
    /// selection.
    pub(crate) fn certified_local_metadata_owner_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(mut shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() {
                return Ok(None);
            }
            return Err(missing_metadata_shard_assignment(object));
        };
        if shard.primary_node_id != self.node_id {
            self.refresh_control_plane_latest()?;
            let Some(refreshed) = self.select_metadata_shard_for_object(object)? else {
                return Err(missing_metadata_shard_assignment(object));
            };
            shard = refreshed;
        }
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        let operation_deadline = self.transport.operation_deadline();
        let certified = self
            .control_plane
            .as_mut()
            .map(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .metadata_shard_authority_is_current_before(&shard, operation_deadline)
            })
            .transpose()?
            .unwrap_or(false);
        if !certified {
            self.refresh_control_plane_latest()?;
            let refreshed = self
                .select_metadata_shard_for_object(object)?
                .unwrap_or_else(|| shard.clone());
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: refreshed.primary_node_id,
                epoch: refreshed.epoch,
            });
        }
        Ok(Some(shard))
    }

    /// Capture the locally committed shard epoch for a new intent without a
    /// Raft write barrier. This is only an intent hint: every visible commit
    /// must still call `ensure_intent_epoch_current*`, which obtains fresh
    /// operation-bound full-shard or cluster-quorum authorization at the
    /// metadata publication boundary.
    pub(crate) fn local_metadata_owner_epoch_hint_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<u64, DfsError> {
        Ok(self
            .local_metadata_owner_shard_hint_for_object(object)?
            .map_or(0, |shard| shard.epoch))
    }

    pub(crate) fn local_metadata_owner_shard_hint_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() {
                return Ok(None);
            }
            return Err(missing_metadata_shard_assignment(object));
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(Some(shard))
    }

    /// Authorize one exact canonical mutation against one metadata shard.
    ///
    /// The healthy path requires a fresh, operation-bound acknowledgement from
    /// every configured shard member and does not touch Raft. When a member is
    /// unavailable, the exact prepared operation may instead be fenced through
    /// cluster KV's existing consensus log. KV carries only the permit and
    /// assignment/epoch comparison; DFS remains the owner of the prepare,
    /// journal, metadata, and repair state.
    /// The caller must pass this evidence to `validate_canonical_authorization`
    /// immediately before publication. That check reads the exact assignment
    /// under the publication guard and holds the guard through the metadata
    /// CAS; a second, unguarded assignment read here adds no publication safety.
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.authority",
        skip_all,
        fields(stage = "dfs.metadata.authority")
    )]
    pub(crate) fn authorize_metadata_shard_operation(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsCanonicalAuthorization, DfsError> {
        let trace_started = Instant::now();
        #[cfg(any(test, feature = "test-support"))]
        let _native_context =
            self.native_publication_owner_for_observation()
                .map(|(root, owner)| {
                    crate::native_shard_trace::enter_boundary_context(
                        &root,
                        &owner,
                        &prepared_batch.shard_id,
                    )
                });
        let operation_digest = metadata_authority_operation_digest(operation)?;
        if self.control_plane.is_none() {
            return Err(DfsError::Distributed {
                message: "clustered metadata authorization has no control-plane client".to_string(),
            });
        }
        if shard.primary_node_id != self.node_id
            || !metadata_authority_operation_is_well_formed_for_shard(operation, shard)
            || prepared_batch.shard_id.0 != shard.shard_id
            || prepared_batch.entries.is_empty()
            || prepared_batch.next_after_sequence
                != metadata_authority_operation_prepare_sequence(operation)
        {
            dfs_perf_trace(format_args!(
                "metadata_authority action=reject reason=malformed shard={} epoch={} local_primary={} operation_well_formed={} batch_shard={} batch_entries={} batch_next={} operation_prepare={}",
                shard.shard_id,
                shard.epoch,
                shard.primary_node_id == self.node_id,
                metadata_authority_operation_is_well_formed_for_shard(operation, shard),
                prepared_batch.shard_id.0,
                prepared_batch.entries.len(),
                prepared_batch.next_after_sequence,
                metadata_authority_operation_prepare_sequence(operation),
            ));
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        }
        let required_node_ids = std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        // Metadata object RPC encodes every typed value into credit-bounded
        // transport segments. Large prepares therefore use the same stream-
        // native replica path as small ones; semantic record count must never
        // select the degraded KV-only authorization path.
        let probe_timeout = metadata_authority_probe_timeout(shard, operation, prepared_batch);
        let probe_started = Instant::now();
        let acknowledged_node_ids = self.fresh_metadata_shard_authority_acknowledgements(
            shard,
            operation,
            prepared_batch,
            probe_timeout,
        )?;
        let trace_probe_ms = probe_started.elapsed().as_millis();
        let trace_acknowledged = acknowledged_node_ids.len();
        dfs_perf_trace(format_args!(
            "metadata_authority action=probe shard={} epoch={} prepare={} acknowledgements={} required={} timeout_ms={}",
            shard.shard_id,
            shard.epoch,
            prepared_batch.next_after_sequence,
            acknowledged_node_ids.len(),
            required_node_ids.len(),
            probe_timeout.as_millis(),
        ));
        if acknowledged_node_ids == required_node_ids {
            dfs_perf_trace(format_args!(
                "metadata-authority-confirmation node={} shard={} epoch={} mode=full-shard required={} acknowledged={} probe_ms={} assignment_ms=0 quorum_ms=0 total_ms={}",
                self.node_id,
                shard.shard_id,
                shard.epoch,
                required_node_ids.len(),
                trace_acknowledged,
                trace_probe_ms,
                trace_started.elapsed().as_millis()
            ));
            return Ok(DfsCanonicalAuthorization::FullShard {
                operation_digest: operation_digest.to_string(),
                expected_shard: shard.clone(),
                acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
                prepare_sequence: prepared_batch.next_after_sequence,
            });
        }
        // This certificate is data-plane evidence for this exact prepare.
        // A native Raft permit fences its publication but stores no prepared
        // metadata payload, so it cannot replace missing DFS recovery evidence.
        let recovery_quorum = required_node_ids.len() / 2 + 1;
        let verified_recovery = acknowledged_node_ids
            .intersection(&required_node_ids)
            .count();
        if !acknowledged_node_ids.contains(&shard.primary_node_id)
            || verified_recovery < recovery_quorum
        {
            dfs_perf_trace(format_args!(
                "metadata_authority action=reject reason=metadata_recovery_quorum shard={} epoch={} prepare={} verified={} required={}",
                shard.shard_id,
                shard.epoch,
                prepared_batch.next_after_sequence,
                verified_recovery,
                recovery_quorum,
            ));
            return Err(DfsError::MetadataRecoveryQuorumUnavailable {
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard.shard_id.clone(),
                required: recovery_quorum,
                verified: verified_recovery,
            });
        }
        dfs_perf_trace(format_args!(
            "metadata_authority action=cluster_quorum_fallback shard={} epoch={} prepare={} acknowledgements={} required={}",
            shard.shard_id,
            shard.epoch,
            metadata_authority_operation_prepare_sequence(operation),
            acknowledged_node_ids.len(),
            required_node_ids.len(),
        ));
        let quorum_started = Instant::now();
        let Some(permit) = self.certify_exact_metadata_shard_operation(
            shard,
            operation_digest,
            metadata_authority_operation_prepare_sequence(operation),
        )?
        else {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some((root, owner)) = self.native_publication_owner_for_observation() {
            if let Some(mut trace) = crate::native_shard_trace::boundary_interest(
                &root,
                Some(&owner),
                &prepared_batch.shard_id,
            ) {
                let held = self.metadata_backend.as_ref().and_then(|backend| {
                    backend.native_held_snapshot_for_scope(
                        &shard.file_system_id,
                        &prepared_batch.shard_id,
                    )
                });
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::RecoveryPermitIssued {
                        expected: shard.clone(),
                        operation: operation.clone(),
                        permit: permit.clone(),
                        authenticated_recovery_node_ids: acknowledged_node_ids
                            .iter()
                            .cloned()
                            .collect(),
                    },
                    held.clone(),
                    held,
                );
            }
        }
        let trace_quorum_ms = quorum_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "metadata-authority-confirmation node={} shard={} epoch={} mode=cluster-quorum required={} acknowledged={} probe_ms={} assignment_ms=0 quorum_ms={} total_ms={}",
            self.node_id,
            shard.shard_id,
            shard.epoch,
            required_node_ids.len(),
            trace_acknowledged,
            trace_probe_ms,
            trace_quorum_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(DfsCanonicalAuthorization::ClusterQuorum {
            operation_digest: operation_digest.to_string(),
            expected_shard: shard.clone(),
            permit_id: permit.permit_id.clone(),
            native_permit: Some(permit),
            acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
        })
    }

    pub(crate) fn metadata_shard_assignment_still_exact(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, DfsError> {
        let operation_deadline = self.transport.operation_deadline();
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        DfsControlPlaneStore::new(control_plane.as_mut())
            .metadata_shard_authority_is_current_before(expected, operation_deadline)
            .map_err(DfsError::from)
    }

    /// Revalidate typed evidence immediately before the backend publication
    /// call. Payload/digest/epoch mismatches fail closed; callers never reduce
    /// authorization to an unstructured boolean.

    fn certify_exact_metadata_shard_operation(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation_digest: &str,
        prepare_sequence: u64,
    ) -> Result<Option<DfsMetadataOperationPermit>, DfsError> {
        self.control_plane
            .as_mut()
            .map(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .certify_metadata_shard_operation_with_native_authority(
                        shard,
                        operation_digest,
                        prepare_sequence,
                    )
            })
            .transpose()
            .map_err(DfsError::from)
            .map(Option::flatten)
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.acknowledgements",
        skip_all,
        fields(stage = "dfs.metadata.acknowledgements")
    )]
    fn fresh_metadata_shard_authority_acknowledgements(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        timeout: Duration,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        let targets = shard
            .replica_node_ids
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return Ok(BTreeSet::from([self.node_id.clone()]));
        }

        let fallback_deadline = Instant::now()
            .checked_add(DFS_METADATA_AUTHORITY_OPERATION_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let observed_previous_deadline = self.transport.operation_deadline();
        let effective_operation_deadline = observed_previous_deadline
            .map_or(fallback_deadline, |deadline| {
                deadline.min(fallback_deadline)
            });
        let replaced_previous_deadline = self
            .transport
            .set_operation_deadline(Some(effective_operation_deadline));
        let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
        let result = self.try_all_metadata_shard_members_accept_fresh_authority(
            shard,
            operation,
            prepared_batch,
            targets,
            effective_operation_deadline,
            timeout,
        );
        self.transport.set_operation_deadline(previous_deadline);
        match result {
            // Delivery uncertainty concerns this optional hidden-prepare
            // probe, not permission to publish the user's mutation. Missing
            // acknowledgements still require the existing exact Raft permit.
            Err(DfsError::Network { .. } | DfsError::NetworkDelivery { .. }) => {
                Ok(BTreeSet::from([self.node_id.clone()]))
            }
            other => other,
        }
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.probe",
        skip_all,
        fields(stage = "dfs.metadata.probe")
    )]
    fn try_all_metadata_shard_members_accept_fresh_authority(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        targets: BTreeSet<NodeId>,
        operation_deadline: Instant,
        timeout: Duration,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        let mut request_ids = Vec::with_capacity(targets.len());
        let mut request_targets = Vec::with_capacity(targets.len());
        let mut acknowledged_node_ids = BTreeSet::from([self.node_id.clone()]);
        let required_acknowledgements = targets.len() + 1;
        if Instant::now() >= operation_deadline {
            return Ok(acknowledged_node_ids);
        }
        let prepared_prefix = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            })?
            .metadata_prefix_proof(&prepared_batch.shard_id)?;
        if Instant::now() >= operation_deadline {
            return Ok(acknowledged_node_ids);
        }
        let Some(prepared_prefix) = prepared_prefix.filter(|prefix| {
            prefix.is_well_formed() && prefix.journal_sequence == prepared_batch.next_after_sequence
        }) else {
            // The fast path is optional. A missing or moving exact prefix
            // cannot authorize publication.
            return Ok(acknowledged_node_ids);
        };
        let result = (|| {
            let mut targets_by_replicated_through_sequence = BTreeMap::<u64, Vec<NodeId>>::new();
            for target_node_id in targets {
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                let replicated_through_sequence = self
                    .metadata_authority_replicated_through_sequence_for_target(
                        &target_node_id,
                        prepared_batch,
                    )?;
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                targets_by_replicated_through_sequence
                    .entry(replicated_through_sequence)
                    .or_default()
                    .push(target_node_id);
            }

            // Construct one owned command per distinct replica cursor. The
            // production transport encodes it once and Arc-replays the body to
            // every target in this group. Previously the service deep-cloned
            // the same multi-megabyte prepare into one command per replica
            // before the transport could discover that all values were equal.
            let mut authority_groups =
                Vec::with_capacity(targets_by_replicated_through_sequence.len());
            for (replicated_through_sequence, target_node_ids) in
                targets_by_replicated_through_sequence
            {
                let target_batch = self
                    .metadata_authority_batch_since(replicated_through_sequence, prepared_batch)?;
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                let Some(target_batch) = target_batch else {
                    // A member farther behind than one bounded catch-up batch
                    // cannot count toward this operation's full-shard proof.
                    // Background replication can close the gap independently.
                    continue;
                };
                let journal_frames = self
                    .metadata_backend
                    .as_ref()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "metadata frame selection requires current durable storage".into(),
                    })?
                    .journal_frames_for_batch(&target_batch)?;
                if journal_frames.len() != target_batch.entries.len() || journal_frames.is_empty() {
                    return Err(DfsError::Metadata {
                        message: "current authority frame selection omitted journal entries".into(),
                    });
                }
                let wire_batch = DfsMetadataJournalFrameBatch {
                    shard_id: target_batch.shard_id,
                    next_after_sequence: target_batch.next_after_sequence,
                    journal_frames,
                };
                let request_id = self.next_request_id();
                let probe_id = sinomo_core::Id::generate().to_string();
                let command = DfsMetadataJournalBatchCommand {
                    request_id,
                    source_node_id: Some(self.node_id.clone()),
                    file_system_id: Some(shard.file_system_id.clone()),
                    authority_epoch: Some(shard.epoch),
                    verify_prefix_only: false,
                    prefix_probe: None,
                    authority_probe: Some(DfsMetadataAuthorityProbe {
                        probe_id,
                        expected_shard: shard.clone(),
                        operation: operation.clone(),
                        prepared_prefix: prepared_prefix.clone(),
                    }),
                    // Include every entry the target has not yet acknowledged,
                    // through the exact prepare. The receiver refuses gaps
                    // and confirms only its complete durable current prefix.
                    durable_replication: None,
                    batch: wire_batch,
                };
                authority_groups.push(DfsMetadataJournalBatchDirectGroup::new(
                    target_node_ids,
                    command,
                ));
            }
            // Replayable object-RPC construction can encode a multi-megabyte
            // catch-up value. The duration-aware transport keeps that local
            // work under `operation_deadline` and begins this shorter budget
            // only after encoding, immediately before peer fan-out.
            match self
                .transport
                .send_grouped_metadata_journal_batches_direct_with_response_timeout(
                    &authority_groups,
                    timeout,
                ) {
                Ok(Some(responses)) => {
                    let mut cursors = Vec::new();
                    for group in &authority_groups {
                        let expected_confirmation_id = group
                            .command
                            .authority_probe
                            .as_ref()
                            .and_then(metadata_authority_probe_confirmation_id);
                        for target_node_id in &group.target_node_ids {
                            let Some((_, response)) = responses
                                .iter()
                                .find(|(response_node_id, _)| response_node_id == target_node_id)
                            else {
                                continue;
                            };
                            if response.request_id != group.command.request_id
                                || response.shard_id != group.command.batch.shard_id
                                || response.next_after_sequence
                                    != group.command.batch.next_after_sequence
                                || response.verified_prefix_only
                                || response.result != DfsMetadataJournalBatchResult::Applied
                                || response.verified_authority_probe_id.as_deref()
                                    != expected_confirmation_id.as_deref()
                            {
                                continue;
                            }
                            cursors.push((
                                target_node_id.clone(),
                                group.command.batch.next_after_sequence,
                            ));
                            #[cfg(any(test, feature = "test-support"))]
                            if let Some(mut trace) =
                                crate::native_shard_trace::current_boundary_interest()
                            {
                                trace.observe(
                                    crate::native_shard_trace::NativeBoundaryFactsV1::SourceJournalResponseAccepted {
                                        authenticated_source_node_id: target_node_id.clone(),
                                        response: response.clone(),
                                        expected_target_node_id: target_node_id.clone(),
                                        expected_next_after_sequence: group.command.batch.next_after_sequence,
                                        expected_prefix_only: false,
                                        expected_confirmation_id: expected_confirmation_id.clone(),
                                    }, None, None,
                                );
                            }
                            acknowledged_node_ids.insert(target_node_id.clone());
                        }
                    }
                    self.record_metadata_authority_replication_cursors(
                        &prepared_batch.shard_id,
                        &cursors,
                    );
                    if acknowledged_node_ids.len() == required_acknowledgements {
                        return Ok(acknowledged_node_ids);
                    }
                }
                Ok(None) => {}
                Err(_) => {}
            }

            // Routed responses are correlated through one process-wide pending
            // map. Only transports without the grouped direct accelerator pay
            // to materialize one command per target, each with a unique wire
            // identity. Move the last command in each group to avoid retaining
            // an otherwise unused extra copy even on this compatibility path.
            let mut authority_requests = Vec::with_capacity(required_acknowledgements - 1);
            for group in authority_groups {
                let unresolved_target_node_ids = group
                    .target_node_ids
                    .into_iter()
                    .filter(|target_node_id| !acknowledged_node_ids.contains(target_node_id))
                    .collect::<Vec<_>>();
                let target_count = unresolved_target_node_ids.len();
                let mut shared_command = Some(group.command);
                for (target_index, target_node_id) in
                    unresolved_target_node_ids.into_iter().enumerate()
                {
                    if Instant::now() >= operation_deadline {
                        return Ok(acknowledged_node_ids);
                    }
                    let mut command = if target_index + 1 == target_count {
                        shared_command
                            .take()
                            .expect("last routed target owns grouped authority command")
                    } else {
                        shared_command
                            .as_ref()
                            .expect("grouped authority command remains available")
                            .clone()
                    };
                    command.request_id = self.next_request_id();
                    command
                        .authority_probe
                        .as_mut()
                        .expect("authority request carries probe")
                        .probe_id = sinomo_core::Id::generate().to_string();
                    authority_requests.push((target_node_id, command));
                }
            }
            let response_deadline = Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now)
                .min(operation_deadline);
            if Instant::now() >= response_deadline {
                return Ok(acknowledged_node_ids);
            }
            let observed_previous_deadline = self.transport.operation_deadline();
            let replaced_previous_deadline = self
                .transport
                .set_operation_deadline(Some(response_deadline));
            let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
            let routed_result = (|| {
                for (target_node_id, command) in authority_requests {
                    let request_id = command.request_id.clone();
                    let confirmation_id = metadata_authority_probe_confirmation_id(
                        command
                            .authority_probe
                            .as_ref()
                            .expect("authority request carries probe"),
                    )
                    .expect("prepared authority probe has an exact prefix confirmation");
                    self.replication.pending_metadata_authority_probes.insert(
                        request_id.clone(),
                        PendingMetadataAuthorityProbe {
                            target_node_id: target_node_id.clone(),
                            confirmation_id,
                            expected_shard: shard.clone(),
                            expected_next_after_sequence: command.batch.next_after_sequence,
                        },
                    );
                    let sent = match self.send_dfs_message_if_routable(
                        &target_node_id,
                        DfsNetworkMessage::MetadataJournalBatch(command),
                    ) {
                        Ok(sent) => sent,
                        Err(DfsError::Network { .. } | DfsError::NetworkDelivery { .. }) => false,
                        Err(error) => return Err(error),
                    };
                    if !sent {
                        self.replication
                            .pending_metadata_authority_probes
                            .remove(&request_id);
                        continue;
                    }
                    request_ids.push(request_id.clone());
                    request_targets.push((request_id, target_node_id));
                }
                loop {
                    self.process_incoming()?;
                    let mut unresolved = false;
                    for (request_id, target_node_id) in &request_targets {
                        match self
                            .replication
                            .metadata_authority_probe_results
                            .get(request_id)
                        {
                            Some(true) => {
                                acknowledged_node_ids.insert(target_node_id.clone());
                            }
                            Some(false) => {}
                            None => unresolved = true,
                        }
                    }
                    if acknowledged_node_ids.len() == required_acknowledgements {
                        let acknowledged_replicas = acknowledged_node_ids
                            .iter()
                            .filter(|id| *id != &self.node_id)
                            .cloned()
                            .collect::<Vec<_>>();
                        let cursors = acknowledged_replicas
                            .into_iter()
                            .map(|node| (node, prepared_batch.next_after_sequence))
                            .collect::<Vec<_>>();
                        self.record_metadata_authority_replication_cursors(
                            &prepared_batch.shard_id,
                            &cursors,
                        );
                        return Ok(acknowledged_node_ids);
                    }
                    if !unresolved {
                        return Ok(acknowledged_node_ids);
                    }
                    let Some(remaining) = response_deadline.checked_duration_since(Instant::now())
                    else {
                        return Ok(acknowledged_node_ids);
                    };
                    self.transport
                        .wait_for_incoming(DFS_METADATA_AUTHORITY_PROBE_IDLE_WAIT.min(remaining))
                        .map_err(DfsError::from_network)?;
                }
            })();
            self.transport.set_operation_deadline(previous_deadline);
            routed_result
        })();
        self.clear_metadata_authority_probe_requests(&request_ids);
        result
    }

    fn metadata_authority_replicated_through_sequence_for_target(
        &self,
        target_node_id: &NodeId,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<u64, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            });
        };
        let durable_replicated_through_sequence =
            backend.metadata_replication_cursor(target_node_id, &prepared_batch.shard_id)?;
        let replicated_through_sequence = self
            .replication
            .metadata_replication_cursors
            .get(&(target_node_id.clone(), prepared_batch.shard_id.clone()))
            .copied()
            .unwrap_or(0)
            .max(durable_replicated_through_sequence);
        Ok(replicated_through_sequence)
    }

    fn metadata_authority_batch_since(
        &self,
        replicated_through_sequence: u64,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        if replicated_through_sequence >= prepared_batch.next_after_sequence {
            // Exact retries still carry the prepare entry so a current peer can
            // re-read it and echo the fresh request nonce without mutating its
            // replay cursor.
            return Ok(Some(prepared_batch.clone()));
        }
        let prepared_batch_is_exact_suffix =
            prepared_batch.entries.first().is_some_and(|entry| {
                entry.sequence == replicated_through_sequence.saturating_add(1)
            }) && prepared_batch
                .entries
                .last()
                .is_some_and(|entry| entry.sequence == prepared_batch.next_after_sequence)
                && prepared_batch
                    .entries
                    .windows(2)
                    .all(|entries| entries[1].sequence == entries[0].sequence.saturating_add(1));
        if prepared_batch_is_exact_suffix {
            // The foreground authority path already owns this exact decoded
            // suffix. Re-reading it from the journal serializes and decodes the
            // same multi-megabyte prepare before any network work can begin.
            return Ok(Some(prepared_batch.clone()));
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            });
        };
        let Some(batch) = backend.journal_batch_for_shard_since_limited(
            &prepared_batch.shard_id,
            replicated_through_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )?
        else {
            return Ok(None);
        };
        let prepare_sequence = prepared_batch.next_after_sequence;
        if batch.next_after_sequence < prepare_sequence
            || !batch
                .entries
                .iter()
                .any(|entry| entry.sequence == prepare_sequence)
        {
            return Ok(None);
        }
        Ok(Some(batch))
    }

    fn record_metadata_authority_replication_cursors(
        &mut self,
        shard: &DfsMetadataShardId,
        cursors: &[(NodeId, u64)],
    ) {
        if cursors.is_empty() {
            return;
        }
        let started = dfs_perf_trace_enabled().then(Instant::now);
        for (node, sequence) in cursors {
            let current = self
                .replication
                .metadata_replication_cursors
                .entry((node.clone(), shard.clone()))
                .or_insert(0);
            *current = (*current).max(*sequence);
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            backend.observe_metadata_replication_cursors(shard, cursors);
        }
        // Fresh operation nonces and receiver ACK floors authorize this write;
        // sender cursors only select the next catch-up suffix. Keep this cheap
        // projection across lazy request-shell rebuilds. The existing journal
        // publisher consumes the same local hints and independently checkpoints
        // its own verified responses, so a
        // lost shell or restart can safely resend from an older on-disk hint.
        // Do not take the metadata writer lock or create cursor-file workers
        // on the foreground full-shard acknowledgement path.
        if let Some(started) = started {
            dfs_perf_trace(format_args!(
                "metadata-authority-cursors shard={} count={} storage=memory elapsed_us={}",
                shard.0,
                cursors.len(),
                started.elapsed().as_micros()
            ));
        }
    }

    fn clear_metadata_authority_probe_requests(&mut self, request_ids: &[String]) {
        for request_id in request_ids {
            self.replication
                .pending_metadata_authority_probes
                .remove(request_id);
            self.replication
                .metadata_authority_probe_results
                .remove(request_id);
        }
    }

    pub(crate) fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        let persist_authoritative_metadata =
            self.local_metadata_backend_is_authoritative_for_object(object)?;
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            let file_system = DfsFileSystem {
                retention_policies: Default::default(),
                module_metadata: Default::default(),
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            };
            if persist_authoritative_metadata {
                self.add_file_system(file_system)?;
            } else {
                self.metadata.add_file_system(file_system);
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(existing) = backend.get_object(&object.object_id)?
        {
            self.metadata.add_object(existing)?;
        } else if self.metadata.object(&object.object_id).is_none() {
            if persist_authoritative_metadata {
                self.add_object_metadata(object.clone())?;
            } else {
                self.add_transient_object_metadata(object.clone())?;
            }
        }
        Ok(())
    }

    pub(crate) fn command_authority_shard_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(object) = object else {
            return Ok(None);
        };
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            return Ok(None);
        };
        Ok((shard.primary_node_id == self.node_id).then_some(shard))
    }

    fn local_metadata_backend_is_authoritative_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    fn command_authority_shard_proves_metadata_primary(
        &self,
        requester_node_id: &NodeId,
        object: &DfsObjectMetadata,
        local_shard: Option<&DfsMetadataShardConfig>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<bool, DfsError> {
        let Some(authority_shard) = authority_shard else {
            return Ok(false);
        };
        if authority_shard.primary_node_id != *requester_node_id
            || authority_shard.file_system_id != object.file_system_id
        {
            return Ok(false);
        }
        let selected = crate::control_plane::select_metadata_shard_config_for_path(
            &object.file_system_id,
            std::slice::from_ref(authority_shard),
            &object.path,
        )
        .map_err(DfsError::from)?;
        if selected.as_ref() != Some(authority_shard) {
            return Ok(false);
        }
        if let Some(local_shard) = local_shard
            && authority_shard.epoch < local_shard.epoch
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn replica_command_metadata_is_authoritative(
        &mut self,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        self.local_metadata_backend_is_authoritative_for_object(&object)
    }

    pub(crate) fn put_replica_receipt_if_authoritative(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        if self.replica_command_metadata_is_authoritative(object_id, object_metadata)? {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        Ok(())
    }

    pub(crate) fn commit_replica_metadata_cache(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let persist_authoritative_metadata =
            self.replica_command_metadata_is_authoritative(object_id, object_metadata)?;
        if persist_authoritative_metadata {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        let object =
            self.commit_local_metadata(object_id, version, content_hash, dfs_now_unix_nanos())?;
        if persist_authoritative_metadata {
            self.put_visible_object_in_metadata_backend(&object)?;
        }
        Ok(())
    }

    pub(crate) fn ensure_replica_command_authorized_with_metadata(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let (object, object_from_command) = match self.current_read_object_metadata(object_id) {
            Ok(object) => (object, false),
            Err(DfsError::UnknownObject { .. }) => {
                let object = object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?;
                (object, true)
            }
            Err(error) => return Err(error),
        };
        let mut metadata_shard = self.select_metadata_shard_for_object(&object)?;
        let mut requester_has_supplied_metadata_authority = self
            .command_authority_shard_proves_metadata_primary(
                requester_node_id,
                &object,
                metadata_shard.as_ref(),
                authority_shard,
            )?;
        if requester_has_supplied_metadata_authority
            && authority_shard.is_some_and(|authority| metadata_shard.as_ref() != Some(authority))
        {
            // A long-lived content-replica lane may still hold the retiring
            // shard assignment after a certified primary handoff. The signed
            // command stamp is enough to trigger a fresh observation, but it
            // is not itself a control-plane replacement: require the refreshed
            // view to converge to that exact assignment before later replica
            // cache decisions can mistake this node for the old primary.
            self.refresh_control_plane_latest()?;
            metadata_shard = self.select_metadata_shard_for_object(&object)?;
            requester_has_supplied_metadata_authority = self
                .command_authority_shard_proves_metadata_primary(
                    requester_node_id,
                    &object,
                    metadata_shard.as_ref(),
                    authority_shard,
                )?;
            if authority_shard.is_some_and(|authority| metadata_shard.as_ref() != Some(authority)) {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local metadata authority view for {} did not converge to authenticated shard {} epoch {}",
                        object.file_system_id,
                        authority_shard.expect("authority checked above").shard_id,
                        authority_shard.expect("authority checked above").epoch,
                    ),
                });
            }
        }
        let requester_is_metadata_primary = metadata_shard
            .as_ref()
            .is_some_and(|shard| &shard.primary_node_id == requester_node_id)
            || requester_has_supplied_metadata_authority;
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(intent) = backend.get_intent(object_id, intent_id)?
        {
            let requester_has_intent_authority = &intent.coordinator_node_id == requester_node_id
                || (requester_is_metadata_primary && object_metadata.is_some());
            if !requester_has_intent_authority {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!("source is not coordinator for DFS write intent {intent_id}"),
                });
            }
            if !intent.desired_replica_node_ids.contains(&self.node_id) {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: dfs_replica_not_desired_reason(intent_id),
                });
            }
            return Ok(());
        }
        if requester_is_metadata_primary && object_metadata.is_some() {
            return Ok(());
        }

        if !object_from_command || metadata_shard.is_none() {
            if &object.owner_node_id == requester_node_id {
                return Ok(());
            }
            if matches!(
                object.policy,
                DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
            ) && object.current_locations.contains(requester_node_id)
            {
                return Ok(());
            }
        } else if object_metadata.is_some()
            && let Some(shard) = metadata_shard.as_ref()
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                    object.file_system_id, shard.primary_node_id, shard.epoch
                ),
            });
        }

        Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "source is neither object owner nor current replica for {object_id}; owner is {}",
                object.owner_node_id
            ),
        })
    }

    pub(crate) fn ensure_replica_command_hash_matches_intent(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if let Some(expected_hash) = intent.content_hash.as_deref()
            && expected_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_blob_fetch_authorized(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
    ) -> Result<(), DfsError> {
        let intent_id = &command.intent_id;
        let object_id = &command.object_id;
        let Some(intent) = ({
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(());
            };
            backend.get_intent(object_id, intent_id)?
        }) else {
            return Ok(());
        };
        let intent_object_metadata = intent.object_metadata.clone();
        let (requester_is_projected_metadata_primary, requester_is_metadata_primary) = match self
            .current_read_object_metadata(object_id)
            .ok()
            .or(intent_object_metadata)
        {
            Some(object) => {
                let metadata_shard = self.select_metadata_shard_for_object(&object)?;
                let projected_primary = metadata_shard
                    .as_ref()
                    .is_some_and(|shard| shard.primary_node_id == *requester_node_id);
                let existing_replica_authority = projected_primary
                    || self.command_authority_shard_proves_metadata_primary(
                        requester_node_id,
                        &object,
                        metadata_shard.as_ref(),
                        command.authority_shard.as_ref(),
                    )?;
                (projected_primary, existing_replica_authority)
            }
            None => (false, false),
        };
        ensure_blob_fetch_intent_permission(
            &self.node_id,
            requester_node_id,
            command,
            &intent,
            requester_is_projected_metadata_primary,
            requester_is_metadata_primary,
            || {
                Ok(self
                    .backend_receipt_for_replica(object_id, &intent.target_version, &self.node_id)?
                    .is_some())
            },
        )
    }
}

pub(crate) fn ensure_blob_fetch_intent_permission(
    local_node_id: &NodeId,
    requester_node_id: &NodeId,
    command: &DfsFetchBlobCommand,
    intent: &DfsWriteIntentRecord,
    requester_is_projected_metadata_primary: bool,
    requester_is_metadata_primary: bool,
    local_has_receipt: impl FnOnce() -> Result<bool, DfsError>,
) -> Result<(), DfsError> {
    let intent_id = &command.intent_id;
    if &intent.coordinator_node_id != requester_node_id
        && !intent.desired_replica_node_ids.contains(requester_node_id)
        && !requester_is_metadata_primary
    {
        return Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!("source is not authorized to fetch DFS blob for intent {intent_id}"),
        });
    }
    // The primary may replace a requester's pre-staged copy with other
    // replicas when it binds the final placement plan. That requester is
    // still the durable transfer source for this pending write, but its
    // copy must never contribute a receipt to W/N. Limit this source-only
    // permission to the exact pending intent and current metadata primary;
    // it grants no authority to serve a terminal or different target.
    let pending_requester_source = requester_is_projected_metadata_primary
        && intent.publication.is_current()
        && intent.coordinator_node_id == *local_node_id
        && matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
        && intent.target_version == command.version
        && intent.content_hash.as_deref() == Some(command.content_hash.as_str());
    if !pending_requester_source
        && !intent.desired_replica_node_ids.contains(local_node_id)
        && !local_has_receipt()?
    {
        return Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "local node is not an expected or durable replica for DFS intent {intent_id}"
            ),
        });
    }
    Ok(())
}

pub(crate) fn missing_metadata_shard_assignment(object: &DfsObjectMetadata) -> DfsError {
    DfsError::Distributed {
        message: format!(
            "clustered DFS object {} at {} has no committed metadata-shard assignment",
            object.object_id, object.path
        ),
    }
}

/// Hash a namespace prepare that has already been canonicalized in place.
///
/// Callers that own a large prepare can normalize it once and keep transferring
/// that same allocation through journal and table materialization. The public
/// compatibility helper above retains the defensive clone for borrowed and
/// recovery callers whose normalization state is not known.

/// Build the authority identity for a newly-created namespace prepare whose
/// canonical payload digest was computed by this process immediately before
/// the same record was durably prepared.
///
/// Recovered or network-provided records must use
/// [`metadata_authority_operation_for_namespace_prepare`] so their complete
/// payload is independently rehashed. This narrow constructor avoids hashing
/// a freshly-built multi-megabyte bulk transaction twice on the primary.

/// Called only for enabled tracing; keeps the exact publication operation on
/// each disjoint subphase without cloning it or changing the guard lifetime.

pub(crate) fn metadata_authority_operation_digest(
    operation: &DfsMetadataAuthorityOperation,
) -> Result<&str, DfsError> {
    let DfsMetadataAuthorityOperation::PreparedOperation { payload_digest, .. } = operation;
    Ok(payload_digest)
}

pub(crate) fn metadata_authority_operation_prepare_sequence(
    operation: &DfsMetadataAuthorityOperation,
) -> u64 {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        prepare_sequence, ..
    } = operation;
    *prepare_sequence
}

pub(crate) fn metadata_authority_operation_is_well_formed_for_shard(
    operation: &DfsMetadataAuthorityOperation,
    shard: &DfsMetadataShardConfig,
) -> bool {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        operation_id,
        file_system_id,
        shard_id,
        owner_epoch,
        prepare_sequence,
        payload_digest,
    } = operation;
    !operation_id.is_empty()
        && file_system_id == &shard.file_system_id
        && shard_id.0 == shard.shard_id
        && *owner_epoch == shard.epoch
        && *prepare_sequence > 0
        && payload_digest.starts_with("dfs-prepared-operation-v7:blake3:")
}

/// Verify that the exact operation named by an authority acknowledgement is
/// durably present in the DFS metadata backend. This is deliberately a DFS
/// metadata check: ordinary writes do not put file metadata into cluster KV.
pub(crate) fn metadata_authority_probe_confirmation_id(
    probe: &DfsMetadataAuthorityProbe,
) -> Option<String> {
    let prefix = &probe.prepared_prefix;
    if probe.probe_id.is_empty()
        || !prefix.is_well_formed()
        || prefix.journal_sequence
            != metadata_authority_operation_prepare_sequence(&probe.operation)
    {
        return None;
    }
    Some(format!(
        "dfs-authority-prefix-v7:{}:{}:{}",
        probe.probe_id, prefix.journal_sequence, prefix.digest
    ))
}

/// Verify an authority probe immediately after applying its journal batch.
///
/// Current frame ingest carries evidence from the exact validated preparation.
/// Reuse it before the durable acknowledgement; duplicate delivery verifies the
/// same operation against the retained current record.
pub(crate) fn verify_persisted_metadata_authority_probe_after_apply(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    probe: &DfsMetadataAuthorityProbe,
    apply_report: &DfsMetadataReplicaApplyReport,
) -> Result<String, DfsError> {
    let operation_verified = apply_report
        .namespace_authority_proof_for(shard, &probe.operation)
        .is_some();
    if let Some(durable_prefix) = apply_report.durable_prefix_proof() {
        if !operation_verified
            || !metadata_authority_operation_is_well_formed_for_shard(&probe.operation, shard)
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "durable metadata authority ingest did not validate the exact operation for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            });
        }
        let confirmation_id = metadata_authority_probe_confirmation_id(probe).ok_or_else(|| {
            DfsError::Metadata {
                message: format!(
                    "metadata authority probe has no well-formed exact prepared prefix for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            }
        })?;
        if &probe.prepared_prefix != durable_prefix {
            return Err(DfsError::Metadata {
                message: format!(
                    "durable metadata authority journal for shard {} does not match the primary's exact prepared history",
                    shard.shard_id
                ),
            });
        }
        return Ok(confirmation_id);
    }
    verify_persisted_metadata_authority_probe_inner(backend, shard, probe, operation_verified)
}

fn verify_persisted_metadata_authority_probe_inner(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    probe: &DfsMetadataAuthorityProbe,
    operation_verified: bool,
) -> Result<String, DfsError> {
    if operation_verified {
        if !metadata_authority_operation_is_well_formed_for_shard(&probe.operation, shard) {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata authority operation is not valid for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            });
        }
    } else {
        verify_persisted_metadata_authority_operation(backend, shard, &probe.operation)?;
    }
    let confirmation_id = metadata_authority_probe_confirmation_id(probe).ok_or_else(|| {
        DfsError::Metadata {
            message: format!(
                "metadata authority probe has no well-formed exact prepared prefix for shard {} epoch {}",
                shard.shard_id, shard.epoch
            ),
        }
    })?;
    let local_prefix = backend
        .metadata_prefix_proof(&DfsMetadataShardId::new(shard.shard_id.clone()))?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata authority probe cannot read an exact local prefix for shard {}",
                shard.shard_id
            ),
        })?;
    if probe.prepared_prefix != local_prefix {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata authority operation is present but shard {} prefix does not match the primary's exact prepared history",
                shard.shard_id
            ),
        });
    }
    Ok(confirmation_id)
}

pub(crate) fn verify_persisted_metadata_authority_operation(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    operation: &DfsMetadataAuthorityOperation,
) -> Result<(), DfsError> {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        operation_id,
        file_system_id,
        shard_id,
        owner_epoch,
        prepare_sequence,
        payload_digest,
    } = operation;
    let digest = backend
        .prepared_operation_digest(operation_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: "authority operation has no durable complete preparation".into(),
        })?;
    if digest.file_system_id != *file_system_id
        || digest.shard_id != *shard_id
        || digest.owner_epoch != *owner_epoch
        || digest.prepare_sequence != *prepare_sequence
        || digest.payload_digest != *payload_digest
        || !metadata_authority_operation_is_well_formed_for_shard(operation, shard)
    {
        return Err(DfsError::Metadata {
            message: "authority operation differs from actual durable preparation".into(),
        });
    }
    Ok(())
}
