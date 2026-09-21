//! Repair, stage, and pending-transfer lifecycle for the networked DFS service.
//!
//! This module owns retry scheduling, stale inflight cleanup, alternate-source
//! fetch requests, inbound repair transfer handling, and repair-attempt
//! persistence. Unrelated fetch/read response handling stays in
//! `service_fetch_transfer` and `service_read_transfer`; outbound session
//! request construction stays in `service_outbound`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sinomo_core::node::NodeId;

use crate::transfer::{
    DFS_REPAIR_RETRY_SUPPRESSED_TICK, PendingNetworkedWrite, fetch_inflight_timeout_ticks,
    repair_retry_delay_nanos_for_ticks, repair_retry_delay_ticks, repair_retry_state_is_suppressed,
    repair_transfer_inflight_timeout_ticks, stage_inflight_retry_delay_ticks,
    wake_repair_retry_state,
};
use crate::*;

/// Logical DFS write/repair records can outlive individual network sessions.
/// This is a stale-state horizon, not a socket or stream idle timeout; active
/// network I/O uses `sinomo-network`'s five-second policy.
pub(crate) const DFS_PENDING_TRANSFER_STALE_AFTER: Duration = Duration::from_secs(60);
/// How long a primary keeps an unanswered routed-write response obligation
/// before assuming the requester gave up.
const DFS_REMOTE_WRITE_RESPONSE_TTL: Duration = Duration::from_secs(60);
const DFS_TRANSIENT_ROUTE_REPAIR_RETRY_MAX_TICKS: u64 = 6;

fn repair_retry_delay_ticks_for_error(attempts: u32, last_error: Option<&str>) -> u64 {
    let delay_ticks = repair_retry_delay_ticks(attempts);
    if last_error.is_some_and(repair_error_indicates_transient_route_failure) {
        delay_ticks.min(DFS_TRANSIENT_ROUTE_REPAIR_RETRY_MAX_TICKS)
    } else {
        delay_ticks
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsRepairBlobRequestOutcome {
    pub(crate) requested: bool,
    pub(crate) last_error: Option<String>,
}

/// Shared work budget for one repair-maintenance pass.
///
/// Stage retries and committed repairs use the same target budget. A failed or
/// unroutable send still consumes one slot: the bound limits attempted work and
/// wall-clock exposure, not only successful schedules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DfsRepairMaintenanceBudget {
    remaining_targets: usize,
}

impl DfsRepairMaintenanceBudget {
    const fn new(max_targets: usize) -> Self {
        Self {
            remaining_targets: max_targets,
        }
    }

    const fn is_exhausted(self) -> bool {
        self.remaining_targets == 0
    }

    fn reserve(&mut self, requested: usize) -> usize {
        let reserved = requested.min(self.remaining_targets);
        self.remaining_targets -= reserved;
        reserved
    }

    fn reserve_one(&mut self) -> bool {
        self.reserve(1) == 1
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Drain only the repair jobs created while serving the current message.
    ///
    /// A committed repair job retains its pending write until it is repaired
    /// or loses authority, so the pending record is the exact source for the
    /// content identity persisted in the daemon's local durable queue. Jobs
    /// without a pending write are intentionally omitted, matching
    /// `durable_repair_queue_records`: they are not executable by this lane.
    pub(crate) fn take_message_committed_repair_queue_records(
        &mut self,
    ) -> Vec<DfsRepairAttemptRecord> {
        let now = dfs_now_unix_nanos();
        let mut records = BTreeMap::new();
        for job in self.coordinator.take_message_repair_jobs() {
            let Some(pending) = self.writes.pending_writes.get(&job.intent_id) else {
                continue;
            };
            records.insert(
                (job.intent_id.clone(), job.missing_replica_node_id.clone()),
                DfsRepairAttemptRecord {
                    intent_id: job.intent_id,
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: job.missing_replica_node_id,
                    attempts: 0,
                    in_flight: false,
                    last_attempt_unix_nanos: 0,
                    next_retry_unix_nanos: now,
                    transfer_request_id: None,
                    last_error: None,
                },
            );
        }
        records.into_values().collect()
    }

    /// Number of new stage sends that may be reserved for this intent now.
    /// Background writes reserve only the exact W deficit; foreground writes
    /// may reserve every missing desired replica. A timed-out inflight request
    /// is treated as a freed reservation so its exact request can be retried.
    fn pending_stage_send_budget(
        &self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        durable_nodes: &BTreeSet<NodeId>,
        execution_exclusions: Option<&DfsMaintenanceExecutionExclusions>,
    ) -> usize {
        let durable_desired = intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| durable_nodes.contains(*node_id))
            .count();
        let active_stage_reservations = pending
            .stage_inflight_request_ids
            .keys()
            .filter(|node_id| {
                intent.desired_replica_nodes.contains(*node_id)
                    && !durable_nodes.contains(*node_id)
                    && !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
                    && !self.stage_retry_is_due(&intent.intent_id, node_id)
            })
            .cloned();
        let active_repair_reservations = pending
            .repair_inflight_request_ids
            .keys()
            .filter(|node_id| {
                intent.desired_replica_nodes.contains(*node_id)
                    && !durable_nodes.contains(*node_id)
                    && !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
                    && !self.repair_retry_is_due(&intent.intent_id, node_id)
            })
            .cloned();
        let active_reservations = active_stage_reservations
            .chain(active_repair_reservations)
            .collect::<BTreeSet<_>>()
            .len();
        let target_receipts = if pending.foreground_replica_repair {
            intent.desired_replica_nodes.len()
        } else {
            intent.policy.min_commit_replicas
        };
        target_receipts.saturating_sub(durable_desired.saturating_add(active_reservations))
    }

    fn pending_stage_targets(
        &self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        durable_nodes: &BTreeSet<NodeId>,
        execution_exclusions: Option<&DfsMaintenanceExecutionExclusions>,
    ) -> Vec<NodeId> {
        let budget =
            self.pending_stage_send_budget(intent, pending, durable_nodes, execution_exclusions);
        intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
            .filter(|node_id| {
                !execution_exclusions.is_some_and(|excluded| excluded.contains(node_id))
            })
            .filter(|node_id| self.stage_retry_is_due(&intent.intent_id, node_id))
            .filter(|node_id| !self.repair_request_is_active(&intent.intent_id, node_id))
            .take(budget)
            .cloned()
            .collect()
    }

    pub(crate) fn stage_request_is_active(
        &self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .stage_inflight_request_ids
                    .contains_key(replica_node_id)
                    && !self.stage_retry_is_due(intent_id, replica_node_id)
            })
    }

    fn repair_request_is_active(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .repair_inflight_request_ids
                    .contains_key(replica_node_id)
                    && !self.repair_retry_is_due(intent_id, replica_node_id)
            })
    }

    /// Reserve a committed target for RepairReplica only after any StageBlob
    /// reservation has expired. Keep a suppressed stage retry tombstone so a
    /// late response carrying the superseded stage request id is rejected.
    fn prepare_repair_target(&mut self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        if self.stage_request_is_active(intent_id, replica_node_id) {
            return false;
        }
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id)
            && pending
                .stage_inflight_request_ids
                .remove(replica_node_id)
                .is_some()
        {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.inflight = false;
            state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
        }
        true
    }

    pub(crate) fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_current_replica_publication(&command.intent_id, &command.object_id)
            .and_then(|()| {
                self.ensure_replica_command_authorized_with_metadata(
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    command.object_metadata.as_ref(),
                    command.authority_shard.as_ref(),
                )
            })
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "repair",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "repair",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                match self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    Ok(()) => DfsBlobTransferResult::Committed {
                        content_hash: command.content_hash.clone(),
                    },
                    Err(error) => DfsBlobTransferResult::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_repair_replica_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.repair_replica_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    fn ensure_repair_response_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_repair_response_request_matches_pending(response)?;
        let pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "repair",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn ensure_repair_response_request_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.writes.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for repair response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .repair_inflight_request_ids
            .get(&response.replica_node_id);
        if let Some(expected_request_id) = expected_request_id {
            if expected_request_id != &response.request_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "unexpected DFS repair response request id for intent {} from {}: expected {}, actual {}",
                        response.intent_id,
                        response.replica_node_id,
                        expected_request_id,
                        response.request_id
                    ),
                });
            }
            return Ok(());
        }
        if !pending.repair_inflight_request_ids.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            });
        }
        if self.coordinator.repair_jobs().iter().any(|job| {
            job.intent_id == response.intent_id
                && job.missing_replica_node_id == response.replica_node_id
        }) {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "unexpected DFS repair response source for intent {}: {}",
                response.intent_id, response.replica_node_id
            ),
        })
    }

    pub(crate) fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            });
        }
        self.restore_pending_write_for_intent_if_missing(&response.intent_id, &response.object_id)?;
        match response.result {
            DfsBlobTransferResult::Committed { ref content_hash }
            | DfsBlobTransferResult::Durable { ref content_hash } => {
                if let Err(error) =
                    self.ensure_repair_response_matches_pending(&response, content_hash)
                {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    });
                }
                let event =
                    self.with_placement_intent_authority(&response.intent_id, |service| {
                        service.put_metadata_receipt(
                            &response.file_system_id,
                            &response.object_id,
                            &response.version,
                            &response.replica_node_id,
                            content_hash,
                        )?;
                        service.coordinator.record_durable_receipt(
                            &response.intent_id,
                            response.replica_node_id.clone(),
                        )?;
                        service.record_repair_metadata_settle(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        service.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                        Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                            intent_id: response.intent_id.clone(),
                            object_id: response.object_id.clone(),
                            version: response.version.clone(),
                            replica_node_id: response.replica_node_id.clone(),
                            content_hash: content_hash.clone(),
                        })
                    })?;
                Ok(
                    event.unwrap_or_else(|| NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    }),
                )
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                if !self.placement_intent_remains_authoritative(&response.intent_id)? {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    });
                }
                match self.ensure_repair_response_request_matches_pending(&response) {
                    Ok(()) => {
                        if repair_rejection_indicates_lost_authority(reason) {
                            self.suppress_repair_retries_for_lost_authority(
                                &response.intent_id,
                                Some(&response.replica_node_id),
                                false,
                            );
                        } else {
                            self.record_repair_retry_scheduled(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                        }
                        Ok(NetworkedDfsServiceEvent::RepairRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        })
                    }
                    Err(error) => Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    pub(crate) fn send_repair_replica_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn repair_replica_result_event(
        &self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id,
                    object_id,
                    version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id,
                    object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }
}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    #[cfg(test)]
    pub(crate) async fn retry_pending_repairs_async_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
            max_scheduled,
            repair_attempts,
            &DfsMaintenanceExecutionExclusions::default(),
        )
        .await
    }

    pub(crate) async fn retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_transfer_state();
        self.restore_pending_writes_for_repair_attempt_records(repair_attempts)?;
        self.restore_pending_writes_for_repair_jobs()?;
        self.restore_repair_attempt_records(repair_attempts);
        if self.writes.pending_writes.is_empty() || max_scheduled == 0 {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget = DfsRepairMaintenanceBudget::new(max_scheduled);
        self.retry_pending_stages_async_bounded(&mut budget, execution_exclusions)
            .await?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();
        let mut consumed_repair_targets = BTreeSet::new();

        for job in repair_jobs.iter().cloned() {
            if budget.is_exhausted() {
                break;
            }
            if execution_exclusions.contains(&job.missing_replica_node_id) {
                continue;
            }
            if !self.placement_intent_remains_authoritative(&job.intent_id)? {
                continue;
            }
            if consumed_repair_targets
                .contains(&(job.intent_id.clone(), job.missing_replica_node_id.clone()))
            {
                continue;
            }
            consumed_repair_targets
                .insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
            if !self.prepare_repair_target(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.retire_committed_repair(&job.intent_id);
                continue;
            }
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }

            if job.missing_replica_node_id == self.node_id {
                if !budget.reserve_one() {
                    break;
                }
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        &job.intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    continue;
                }
                let fetch_outcome = self
                    .request_repair_blob_from_alternate_async(&job, &pending, execution_exclusions)
                    .await?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }

            if self.open_pending_write_blob_stream(&pending).is_err() {
                if !budget.reserve_one() {
                    break;
                }
                let fetch_outcome = self
                    .request_repair_blob_from_alternate_async(&job, &pending, execution_exclusions)
                    .await?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let remaining = budget.remaining_targets;
            if remaining == 0 {
                break;
            }
            let mut fanout_jobs = vec![(job.clone(), request_id)];
            if remaining > 1
                && let Some(companion) = repair_jobs
                    .iter()
                    .find(|candidate| {
                        candidate.intent_id == job.intent_id
                            && candidate.missing_replica_node_id != job.missing_replica_node_id
                            && candidate.missing_replica_node_id != self.node_id
                            && !execution_exclusions.contains(&candidate.missing_replica_node_id)
                            && !consumed_repair_targets.contains(&(
                                candidate.intent_id.clone(),
                                candidate.missing_replica_node_id.clone(),
                            ))
                            && self.repair_retry_is_due(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                            && !self.stage_request_is_active(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                    })
                    .cloned()
                && self
                    .prepare_repair_target(&companion.intent_id, &companion.missing_replica_node_id)
            {
                let companion_pending = self
                    .writes
                    .pending_writes
                    .get(&companion.intent_id)
                    .cloned()
                    .expect("same-intent pending repair validated");
                if self.pending_repair_targets_current_content(&companion, &companion_pending)? {
                    let companion_request_id = companion_pending
                        .repair_inflight_request_ids
                        .get(&companion.missing_replica_node_id)
                        .cloned()
                        .unwrap_or_else(|| self.next_request_id());
                    consumed_repair_targets.insert((
                        companion.intent_id.clone(),
                        companion.missing_replica_node_id.clone(),
                    ));
                    fanout_jobs.push((companion, companion_request_id));
                }
            }
            let reserved = budget.reserve(fanout_jobs.len());
            debug_assert_eq!(reserved, fanout_jobs.len());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let fanout_targets = fanout_jobs
                .iter()
                .map(|(job, request_id)| (job.missing_replica_node_id.clone(), request_id.clone()))
                .collect::<Vec<_>>();
            let outcomes = self
                .send_repair_replica_attachments_if_routable_async(
                    &fanout_targets,
                    &job.intent_id,
                    &job.file_system_id,
                    &pending.object_id,
                    self.coordinator
                        .intent(&job.intent_id)
                        .and_then(|intent| intent.base_version.clone()),
                    &pending.version,
                    &pending.content_hash,
                    pending.size_bytes,
                    object_metadata,
                )
                .await?;
            for ((fanout_job, request_id), outcome) in fanout_jobs.into_iter().zip(outcomes) {
                if outcome.sent {
                    self.record_repair_inflight(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        request_id,
                    );
                    scheduled.push(fanout_job);
                } else {
                    self.record_repair_retry_scheduled_with_error(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        outcome.last_error,
                    );
                }
            }
            if budget.is_exhausted() {
                break;
            }
        }

        Ok(scheduled)
    }

    async fn retry_pending_stages_async_bounded(
        &mut self,
        budget: &mut DfsRepairMaintenanceBudget,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<(), DfsError> {
        let pending_intent_ids = self
            .writes
            .pending_writes
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            if budget.is_exhausted() {
                break;
            }
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            while let Some(pending) = self.writes.pending_writes.get(&intent_id).cloned() {
                let durable_nodes = self
                    .coordinator
                    .receipts_for_intent(&intent_id)
                    .into_iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect::<BTreeSet<_>>();
                let mut stage_targets = self.pending_stage_targets(
                    &intent,
                    &pending,
                    &durable_nodes,
                    Some(execution_exclusions),
                );
                if stage_targets.is_empty() {
                    break;
                }
                let reserved = budget.reserve(stage_targets.len());
                stage_targets.truncate(reserved);
                if stage_targets.is_empty() {
                    return Ok(());
                }
                let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
                let mut full_stage_targets = Vec::new();
                for replica_node_id in stage_targets {
                    let request_id = pending
                        .stage_inflight_request_ids
                        .get(&replica_node_id)
                        .cloned()
                        .unwrap_or_else(|| self.next_request_id());
                    match self
                        .send_pending_stage_patch_if_routable_async(
                            &replica_node_id,
                            request_id.clone(),
                            &intent,
                            &pending,
                            object_metadata.clone(),
                        )
                        .await?
                    {
                        Some(outcome) if outcome.sent => {
                            self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                        }
                        Some(_) => {
                            self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                        }
                        None => full_stage_targets.push((replica_node_id, request_id)),
                    }
                }
                if full_stage_targets.is_empty() {
                    continue;
                }
                if self.open_pending_write_blob_stream(&pending).is_err() {
                    let fetch_requested = self
                        .request_stage_blob_from_alternate_async(
                            &intent,
                            &pending,
                            execution_exclusions,
                        )
                        .await?;
                    if !fetch_requested {
                        for (replica_node_id, _) in full_stage_targets {
                            self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                        }
                    }
                    break;
                }
                let outcomes = self
                    .send_stage_blob_attachments_if_routable_async(
                        &full_stage_targets,
                        &intent.intent_id,
                        &intent.file_system_id,
                        &intent.object_id,
                        intent.base_version.clone(),
                        &intent.target_version,
                        &pending.content_hash,
                        pending.size_bytes,
                        object_metadata,
                    )
                    .await?;
                for ((replica_node_id, request_id), outcome) in
                    full_stage_targets.into_iter().zip(outcomes)
                {
                    if outcome.sent {
                        self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                    } else {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                }
                if budget.is_exhausted() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn send_pending_stage_patch_if_routable_async(
        &mut self,
        replica_node_id: &NodeId,
        request_id: String,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Option<StageBlobSendOutcome>, DfsError> {
        let Some(patch) = pending.patch.as_ref() else {
            return Ok(None);
        };
        if patch.full_stage_fallback_nodes.contains(replica_node_id) {
            return Ok(None);
        }
        let Some(base_version) = intent.base_version.as_ref() else {
            return Ok(None);
        };
        if base_version != &patch.base_version {
            return Ok(None);
        }
        let outcome = self
            .send_stage_patch_attachment_if_routable_async(
                replica_node_id,
                request_id,
                &intent.intent_id,
                &intent.file_system_id,
                &intent.object_id,
                base_version,
                &intent.target_version,
                &pending.content_hash,
                patch.offset,
                &patch.patch_hash,
                patch.patch_size_bytes,
                object_metadata,
            )
            .await?;
        Ok(Some(outcome))
    }

    async fn request_stage_blob_from_alternate_async(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if execution_exclusions.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let sent = self
                .send_best_effort_message_if_routable_async(
                    &source_node_id,
                    DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                        request_id: request_id.clone(),
                        intent_id: intent.intent_id.clone(),
                        object_id: pending.object_id.clone(),
                        version: pending.version.clone(),
                        content_hash: pending.content_hash.clone(),
                        authority_shard: authority_shard.clone(),
                    }),
                )
                .await?;
            if sent {
                self.record_fetch_inflight(&intent.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    async fn request_repair_blob_from_alternate_async(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<DfsRepairBlobRequestOutcome, DfsError> {
        let mut skipped_failed_source = false;
        let mut last_error = None;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if execution_exclusions.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let send_outcome = self
                .send_best_effort_message_if_routable_outcome_async(
                    &source_node_id,
                    DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                        request_id: request_id.clone(),
                        intent_id: job.intent_id.clone(),
                        object_id: pending.object_id.clone(),
                        version: pending.version.clone(),
                        content_hash: pending.content_hash.clone(),
                        authority_shard: authority_shard.clone(),
                    }),
                )
                .await?;
            if send_outcome.sent {
                tracing::debug!(
                    target: "sinomo.dfs.repair",
                    local_node_id = %self.node_id,
                    source_node_id = %source_node_id,
                    missing_replica_node_id = %job.missing_replica_node_id,
                    intent_id = %job.intent_id,
                    object_id = %pending.object_id,
                    version = %pending.version,
                    content_hash = %pending.content_hash,
                    request_id = %request_id,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(&job.intent_id, source_node_id, request_id);
                self.record_repair_fetch_inflight(&job.intent_id, &job.missing_replica_node_id);
                return Ok(DfsRepairBlobRequestOutcome {
                    requested: true,
                    last_error: None,
                });
            }
            if send_outcome.last_error.is_some() {
                last_error = send_outcome.last_error;
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        tracing::debug!(
            target: "sinomo.dfs.repair",
            local_node_id = %self.node_id,
            missing_replica_node_id = %job.missing_replica_node_id,
            intent_id = %job.intent_id,
            object_id = %pending.object_id,
            version = %pending.version,
            content_hash = %pending.content_hash,
            skipped_failed_source,
            "no routable DFS repair blob alternate source"
        );
        Ok(DfsRepairBlobRequestOutcome {
            requested: false,
            last_error,
        })
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn prune_idle_transfer_state(&mut self) {
        self.prune_retained_routed_writes();
        // Requesters give up on routed writes after their settle timeout;
        // response obligations they will never consume must not accumulate.
        if !self.writes.pending_remote_write_responses.is_empty() {
            self.writes
                .pending_remote_write_responses
                .retain(|_, pending| pending.created_at.elapsed() <= DFS_REMOTE_WRITE_RESPONSE_TTL);
        }
    }

    pub fn retry_pending_repairs(&mut self) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_bounded(usize::MAX)
    }

    pub fn retry_pending_repairs_bounded(
        &mut self,
        max_scheduled: usize,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_bounded_with_attempts(max_scheduled, &[])
    }

    /// Retry a bounded durable repair queue after restoring only its exact
    /// intent identities from the metadata backend.
    ///
    /// This restart path is O(active repair work), rather than O(total DFS
    /// metadata), and therefore does not require a full service hydrate.
    pub fn retry_pending_repairs_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_scoped_bounded_with_attempts(
            max_scheduled,
            repair_attempts,
            None,
        )
    }

    pub(crate) fn retry_pending_repairs_for_intents_bounded(
        &mut self,
        intent_ids: &BTreeSet<String>,
        max_scheduled: usize,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.retry_pending_repairs_scoped_bounded_with_attempts(
            max_scheduled,
            &[],
            Some(intent_ids),
        )
    }

    fn retry_pending_repairs_scoped_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
        intent_filter: Option<&BTreeSet<String>>,
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_transfer_state();
        self.restore_pending_writes_for_repair_attempt_records(repair_attempts)?;
        self.restore_pending_writes_for_repair_jobs()?;
        self.restore_repair_attempt_records(repair_attempts);
        if self.writes.pending_writes.is_empty()
            || max_scheduled == 0
            || intent_filter.is_some_and(BTreeSet::is_empty)
        {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget = DfsRepairMaintenanceBudget::new(max_scheduled);
        self.retry_pending_stages_scoped_bounded(intent_filter, &mut budget)?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();
        let mut consumed_repair_targets = BTreeSet::new();

        for job in repair_jobs.iter().cloned() {
            if budget.is_exhausted() {
                break;
            }
            if !self.placement_intent_remains_authoritative(&job.intent_id)? {
                continue;
            }
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&job.intent_id)) {
                continue;
            }
            if consumed_repair_targets
                .contains(&(job.intent_id.clone(), job.missing_replica_node_id.clone()))
            {
                continue;
            }
            consumed_repair_targets
                .insert((job.intent_id.clone(), job.missing_replica_node_id.clone()));
            if !self.prepare_repair_target(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.retire_committed_repair(&job.intent_id);
                continue;
            }
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }

            if job.missing_replica_node_id == self.node_id {
                if !budget.reserve_one() {
                    break;
                }
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        &job.intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    continue;
                }
                let fetch_outcome = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }

            if self.open_pending_write_blob_stream(&pending).is_err() {
                if !budget.reserve_one() {
                    break;
                }
                let fetch_outcome = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_outcome.requested {
                    self.record_repair_retry_scheduled_with_error(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                        fetch_outcome.last_error,
                    );
                }
                continue;
            }
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let remaining = budget.remaining_targets;
            if remaining == 0 {
                break;
            }
            let mut fanout_jobs = vec![(job.clone(), request_id)];
            if remaining > 1
                && let Some(companion) = repair_jobs
                    .iter()
                    .find(|candidate| {
                        candidate.intent_id == job.intent_id
                            && candidate.missing_replica_node_id != job.missing_replica_node_id
                            && candidate.missing_replica_node_id != self.node_id
                            && !consumed_repair_targets.contains(&(
                                candidate.intent_id.clone(),
                                candidate.missing_replica_node_id.clone(),
                            ))
                            && self.repair_retry_is_due(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                            && !self.stage_request_is_active(
                                &candidate.intent_id,
                                &candidate.missing_replica_node_id,
                            )
                    })
                    .cloned()
                && self
                    .prepare_repair_target(&companion.intent_id, &companion.missing_replica_node_id)
            {
                let companion_pending = self
                    .writes
                    .pending_writes
                    .get(&companion.intent_id)
                    .cloned()
                    .expect("same-intent pending repair validated");
                if self.pending_repair_targets_current_content(&companion, &companion_pending)? {
                    let companion_request_id = companion_pending
                        .repair_inflight_request_ids
                        .get(&companion.missing_replica_node_id)
                        .cloned()
                        .unwrap_or_else(|| self.next_request_id());
                    consumed_repair_targets.insert((
                        companion.intent_id.clone(),
                        companion.missing_replica_node_id.clone(),
                    ));
                    fanout_jobs.push((companion, companion_request_id));
                }
            }
            let reserved = budget.reserve(fanout_jobs.len());
            debug_assert_eq!(reserved, fanout_jobs.len());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let fanout_targets = fanout_jobs
                .iter()
                .map(|(job, request_id)| (job.missing_replica_node_id.clone(), request_id.clone()))
                .collect::<Vec<_>>();
            let outcomes = self.send_repair_replica_attachments_if_routable(
                &fanout_targets,
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                self.coordinator
                    .intent(&job.intent_id)
                    .and_then(|intent| intent.base_version.clone()),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                object_metadata,
            )?;
            for ((fanout_job, request_id), outcome) in fanout_jobs.into_iter().zip(outcomes) {
                if outcome.sent {
                    self.record_repair_inflight(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        request_id,
                    );
                    scheduled.push(fanout_job);
                } else {
                    self.record_repair_retry_scheduled_with_error(
                        &fanout_job.intent_id,
                        &fanout_job.missing_replica_node_id,
                        outcome.last_error,
                    );
                }
            }
            if budget.is_exhausted() {
                break;
            }
        }

        Ok(scheduled)
    }

    pub(crate) fn remove_pending_write_state(&mut self, intent_id: &str) {
        self.writes.pending_writes.remove(intent_id);
    }

    pub(crate) fn retire_committed_repair(&mut self, intent_id: &str) {
        if !self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
        {
            return;
        }
        self.coordinator
            .set_committed_repair_active(intent_id, false);
        self.remove_pending_write_state(intent_id);
        self.repair_attempt_records
            .retain(|(id, _), _| id != intent_id);
    }

    /// Resolve only the outgoing identities affected by this publication.
    /// Disabled policies do not add object/history lookups to ordinary writes.
    pub(crate) fn file_history_repair_roots_before_objects<'a>(
        &self,
        objects: impl IntoIterator<Item = &'a DfsObjectMetadata>,
        file_system: Option<&DfsFileSystem>,
    ) -> Result<BTreeSet<(DfsObjectId, DfsVersion)>, DfsError> {
        let mut roots = BTreeSet::new();
        let Some(file_system) = file_system.filter(|fs| !fs.file_history.is_empty()) else {
            return Ok(roots);
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(roots);
        };
        for object in objects {
            if object.file_system_id != file_system.file_system_id {
                continue;
            }
            let own = backend.get_object(&object.object_id)?;
            let replaced = backend.get_object_by_path(&object.file_system_id, &object.path)?;
            for previous in own.iter().chain(replaced.iter()) {
                if !crate::file_history::capture_retention_for_file_system(
                    Some(file_system),
                    previous,
                    object,
                )
                .is_disabled()
                {
                    roots.insert((previous.object_id.clone(), previous.version.clone()));
                }
            }
        }
        Ok(roots)
    }

    pub(crate) fn activate_file_history_repair_roots(
        &mut self,
        roots: impl IntoIterator<Item = (DfsObjectId, DfsVersion)>,
    ) -> Result<(), DfsError> {
        for (object, version) in roots {
            if self.reconcile_file_history_repair_root(&object, &version)? {
                self.record_message_committed_work(DfsMessageCommittedWork {
                    repair_maintenance: true,
                    ..Default::default()
                });
            }
        }
        Ok(())
    }

    /// A committed pin change owns the exact repair delta for that immutable
    /// version. This cold path scans only its object/version family and never
    /// turns a compact terminal retry receipt back into executable work.
    pub(crate) fn reconcile_file_history_repair_root(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<bool, DfsError> {
        let Some(version_record) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_version(object_id, version))
            .transpose()?
            .flatten()
            .filter(|record| record.retained.is_some())
        else {
            return Ok(false);
        };
        let Some(object) = self
            .metadata_backend
            .as_ref()
            .map(|b| b.get_object(object_id))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(false);
        }
        let mut cursor = None;
        let mut activated = false;
        let mut full_owner = false;
        let mut unfinished_owner = false;
        let mut archived_source = None;
        loop {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .intents_for_object_version_page(object_id, version, cursor.as_ref(), 128)?;
            for record in page.intents {
                if record.file_system_id != object.file_system_id {
                    continue;
                }
                if crate::intent::write_intent_status_is_active(&record.status) {
                    // Capture metadata may commit before its original data
                    // intent terminalizes. That durable write still owns the
                    // operation and must not race a second maintenance owner.
                    unfinished_owner = true;
                    continue;
                }
                if record.status != DfsWriteIntentStatus::Committed {
                    continue;
                }
                if self
                    .metadata_backend
                    .as_ref()
                    .expect("history metadata")
                    .get_intent_receipt(object_id, &record.intent_id)?
                    .is_some()
                {
                    if !record.publication.is_retained_repair()
                        && !record.intent_id.starts_with("placement:")
                    {
                        if let Some(previous) = &archived_source {
                            let previous: &DfsWriteIntentRecord = previous;
                            if previous.policy != record.policy
                                || previous.desired_replica_node_ids
                                    != record.desired_replica_node_ids
                            {
                                return Err(DfsError::Metadata {
                                    message:
                                        "retained version has ambiguous archived durability plans"
                                            .into(),
                                });
                            }
                        }
                        archived_source = Some(record);
                    }
                    continue;
                }
                full_owner = true;
                let Some(hash) = &record.content_hash else {
                    continue;
                };
                let active = (record.publication.is_current()
                    && object.kind != DfsObjectKind::Tombstone
                    && object.content_hash.as_ref() == Some(hash))
                    || self.retained_file_content_is_live(object_id, version, hash)?;
                if !active {
                    self.retire_committed_repair(&record.intent_id);
                    continue;
                }
                activated |= self.activate_file_history_repair_record(&object, &record)?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if !full_owner
            && !unfinished_owner
            && (version_record.content_durability.is_some()
                || archived_source
                    .as_ref()
                    .is_some_and(|source| version_record.content_hash == source.content_hash))
        {
            let shard = self.certified_local_metadata_owner_shard_for_object(&object)?;
            if shard.as_ref().is_some_and(|shard| {
                !shard
                    .namespace_prepare_codec
                    .supports_retained_repair_owners()
            }) {
                return Err(DfsError::Metadata {
                    message:
                        "retained repair ownership requires a capability-validated packed-v3 shard"
                            .into(),
                });
            }
            let _guard = shard.as_ref().map(|shard| {
                crate::service_replica::metadata_shard_publication_guard(
                    &shard.file_system_id,
                    &shard.shard_id,
                )
            });
            if let Some(shard) = &shard
                && !self.metadata_shard_assignment_still_exact(shard)?
            {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: object.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id.clone(),
                    epoch: shard.epoch,
                });
            }
            let owner = crate::file_history::retained_repair_intent(
                &version_record,
                archived_source.as_ref(),
            )?;
            self.metadata_backend
                .as_mut()
                .expect("history metadata")
                .put_intent_for_object(Some(&object), &owner)?;
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..Default::default()
            });
            if self.retained_file_content_is_live(
                object_id,
                version,
                owner
                    .content_hash
                    .as_deref()
                    .expect("validated repair hash"),
            )? {
                activated |= self.activate_file_history_repair_record(&object, &owner)?;
            }
        }
        Ok(activated)
    }

    fn activate_file_history_repair_record(
        &mut self,
        object: &DfsObjectMetadata,
        record: &DfsWriteIntentRecord,
    ) -> Result<bool, DfsError> {
        self.coordinator.restore_intent_deferring_repair_rebuild(
            crate::intent::distributed_intent_from_record(record),
        )?;
        // Restoring an old canonical intent cannot rewind the current cache.
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.restore_backend_receipt_pages_for_intent(record, false)?;
        let added = self
            .coordinator
            .set_committed_repair_active(&record.intent_id, true);
        if added.is_empty() {
            return Ok(false);
        }
        self.restore_pending_write_for_intent_if_missing(&record.intent_id, &record.object_id)?;
        let activated = self.writes.pending_writes.contains_key(&record.intent_id);
        for job in added {
            self.repair_attempt_records.remove(&(
                record.intent_id.clone(),
                job.missing_replica_node_id.clone(),
            ));
            if let Some(pending) = self.writes.pending_writes.get_mut(&record.intent_id) {
                pending.repair_retries.remove(&job.missing_replica_node_id);
                pending
                    .repair_inflight_request_ids
                    .remove(&job.missing_replica_node_id);
            }
        }
        Ok(activated)
    }

    pub(crate) fn cleanup_committed_pending_write_if_repaired(&mut self, intent_id: &str) {
        let is_committed = self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        if !is_committed {
            return;
        }
        let has_repair_jobs = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        if has_repair_jobs {
            return;
        }
        if self
            .writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                pending
                    .repair_retries
                    .values()
                    .any(|state| state.inflight || state.next_retry_tick > self.repair_retry_tick)
            })
        {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    fn retry_pending_stages_scoped_bounded(
        &mut self,
        intent_filter: Option<&BTreeSet<String>>,
        budget: &mut DfsRepairMaintenanceBudget,
    ) -> Result<(), DfsError> {
        let pending_intent_ids = self
            .writes
            .pending_writes
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            if budget.is_exhausted() {
                break;
            }
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&intent_id)) {
                continue;
            }
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(object_id) = self
                .writes
                .pending_writes
                .get(&intent_id)
                .map(|pending| pending.object_id.clone())
            else {
                continue;
            };
            let object_metadata = self.current_object_for_write(&object_id)?;
            self.stage_missing_desired_replicas_for_intent_bounded(
                &intent_id,
                object_metadata,
                budget,
            )?;
        }
        Ok(())
    }

    pub(crate) fn should_stage_missing_replicas_after_write(&self, intent_id: &str) -> bool {
        matches!(
            self.coordinator
                .intent(intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::AwaitingReceipts)
        )
    }

    pub(crate) fn stage_missing_desired_replicas_for_intent(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut budget = DfsRepairMaintenanceBudget::new(usize::MAX);
        self.stage_missing_desired_replicas_for_intent_bounded(
            intent_id,
            object_metadata,
            &mut budget,
        )
    }

    fn stage_missing_desired_replicas_for_intent_bounded(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
        budget: &mut DfsRepairMaintenanceBudget,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        // StageBlob is a pre-commit protocol. Once W publishes the version,
        // missing desired replicas converge through RepairReplica so a target
        // can never own both transfer kinds for the same intent.
        if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            return Ok(());
        }
        loop {
            if budget.is_exhausted() {
                break;
            }
            let Some(pending) = self.writes.pending_writes.get(intent_id).cloned() else {
                break;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let mut stage_targets =
                self.pending_stage_targets(&intent, &pending, &durable_nodes, None);
            if stage_targets.is_empty() {
                break;
            }
            let reserved = budget.reserve(stage_targets.len());
            stage_targets.truncate(reserved);
            if stage_targets.is_empty() {
                break;
            }
            let mut full_stage_targets = Vec::new();

            for replica_node_id in stage_targets {
                let request_id = pending
                    .stage_inflight_request_ids
                    .get(&replica_node_id)
                    .cloned()
                    .unwrap_or_else(|| self.next_request_id());
                match self.send_pending_stage_patch_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent,
                    &pending,
                    Some(object_metadata.clone()),
                )? {
                    Some(outcome) if outcome.sent => {
                        self.record_stage_inflight(intent_id, &replica_node_id, request_id);
                    }
                    Some(_) => {
                        self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                    }
                    None => full_stage_targets.push((replica_node_id, request_id)),
                }
            }

            if full_stage_targets.is_empty() {
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_stage_blob_from_alternate(&intent, &pending)?;
                if !fetch_requested {
                    for (replica_node_id, _) in full_stage_targets {
                        self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                    }
                }
                break;
            }
            let outcomes = self.send_stage_blob_attachments_if_routable(
                &full_stage_targets,
                intent_id,
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.clone(),
                &intent.target_version,
                &pending.content_hash,
                pending.size_bytes,
                Some(object_metadata.clone()),
            )?;
            for ((replica_node_id, request_id), outcome) in
                full_stage_targets.into_iter().zip(outcomes)
            {
                if outcome.sent {
                    self.record_stage_inflight(intent_id, &replica_node_id, request_id);
                } else {
                    self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                }
            }
        }
        Ok(())
    }

    fn send_pending_stage_patch_if_routable(
        &mut self,
        replica_node_id: &NodeId,
        request_id: String,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Option<StageBlobSendOutcome>, DfsError> {
        let Some(patch) = pending.patch.as_ref() else {
            return Ok(None);
        };
        if patch.full_stage_fallback_nodes.contains(replica_node_id) {
            return Ok(None);
        }
        let Some(base_version) = intent.base_version.as_ref() else {
            return Ok(None);
        };
        if base_version != &patch.base_version {
            return Ok(None);
        }
        let outcome = self.send_stage_patch_attachment_if_routable(
            replica_node_id,
            request_id,
            &intent.intent_id,
            &intent.file_system_id,
            &intent.object_id,
            base_version,
            &intent.target_version,
            &pending.content_hash,
            patch.offset,
            &patch.patch_hash,
            patch.patch_size_bytes,
            object_metadata,
        )?;
        Ok(Some(outcome))
    }

    pub(crate) fn request_stage_blob_from_alternate(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let sent = self.send_best_effort_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
            if sent {
                self.record_fetch_inflight(&intent.intent_id, source_node_id, request_id);
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
        let mut sources = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .filter_map(|receipt| {
                (receipt.replica_node_id != self.node_id).then_some(receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<BTreeSet<_>>();
        if let Some(intent) = self.coordinator.intent(intent_id)
            && intent.requesting_node_id != self.node_id
        {
            // A requester whose pre-staged blob was excluded by snapshot
            // replanning is a transfer source, not a durable replica.
            // Content-hash verification still gates every fetch response.
            sources.insert(intent.requesting_node_id.clone());
        }
        self.ranked_transfer_sources(sources)
    }

    fn record_fetch_inflight(
        &mut self,
        intent_id: &str,
        source_node_id: NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        self.passive_fetch_started_at.insert(
            (intent_id.to_string(), source_node_id.clone()),
            std::time::Instant::now(),
        );
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.insert(source_node_id.clone());
            pending
                .fetch_inflight_request_ids
                .insert(source_node_id.clone(), request_id);
            pending
                .fetch_inflight_started_ticks
                .insert(source_node_id.clone(), tick);
            let retry_until_tick = tick.saturating_add(fetch_inflight_timeout_ticks());
            for state in pending.repair_retries.values_mut() {
                if state.inflight && !repair_retry_state_is_suppressed(state) {
                    state.next_retry_tick = state.next_retry_tick.max(retry_until_tick);
                }
            }
        }
    }

    pub(crate) fn expire_stale_fetch_requests(&mut self) {
        let tick = self.repair_retry_tick;
        for (intent_id, pending) in &mut self.writes.pending_writes {
            let expired_sources = pending
                .fetch_inflight_started_ticks
                .iter()
                .filter_map(|(source_node_id, started_tick)| {
                    let expires_at = started_tick.saturating_add(fetch_inflight_timeout_ticks());
                    (expires_at <= tick).then_some(source_node_id.clone())
                })
                .collect::<Vec<_>>();
            if expired_sources.is_empty() {
                continue;
            }
            for source_node_id in expired_sources {
                pending.fetch_inflight_from.remove(&source_node_id);
                pending.fetch_inflight_started_ticks.remove(&source_node_id);
                self.passive_fetch_started_at
                    .remove(&(intent_id.clone(), source_node_id.clone()));
                pending.fetch_failed_from.insert(source_node_id);
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                wake_repair_retry_state(state);
                state.inflight = false;
            }
        }
    }

    fn stage_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.writes
            .pending_writes
            .get(intent_id)
            .and_then(|pending| pending.stage_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_stage_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick =
                tick.saturating_add(stage_inflight_retry_delay_ticks(state.attempts));
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    pub(crate) fn record_stage_retry_scheduled(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending.stage_inflight_request_ids.remove(replica_node_id);
        }
    }

    pub(crate) fn record_stage_patch_fallback(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id)
            && let Some(patch) = pending.patch.as_mut()
        {
            patch
                .full_stage_fallback_nodes
                .insert(replica_node_id.clone());
        }
    }

    pub(crate) fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        if self
            .writes
            .pending_remote_write_responses
            .get(intent_id)
            .is_some_and(|pending| pending.foreground_replica_repair)
        {
            let Some(pending_write) = self.writes.pending_writes.get(intent_id) else {
                return false;
            };
            return pending_write
                .repair_retries
                .get(replica_node_id)
                .is_none_or(|state| !repair_retry_state_is_suppressed(state) && !state.inflight);
        }
        if self
            .repair_attempt_records
            .get(&(intent_id.to_string(), replica_node_id.clone()))
            .is_some_and(|record| {
                record.next_retry_unix_nanos > dfs_now_unix_nanos()
                    && (record.next_retry_unix_nanos != u64::MAX
                        || repair_attempt_record_indicates_lost_authority(record))
            })
        {
            return false;
        }
        self.writes
            .pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| {
                !repair_retry_state_is_suppressed(state)
                    && state.next_retry_tick <= self.repair_retry_tick
            })
    }

    fn restore_repair_attempt_records(&mut self, records: &[DfsRepairAttemptRecord]) {
        self.repair_attempt_records.clear();
        let active_repair_targets = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| (job.intent_id.clone(), job.missing_replica_node_id.clone()))
            .collect::<BTreeSet<_>>();
        for record in records {
            if !active_repair_targets.contains(&(
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            )) {
                // The durable queue is a restart cursor for an authoritative
                // repair job, not an independent source of repair authority.
                // A receipt can settle that job before this node consumes its
                // queued retry. Do not resurrect the already-cleared target
                // from stale retry/backoff bookkeeping.
                continue;
            }
            let Some(pending) = self.writes.pending_writes.get_mut(&record.intent_id) else {
                continue;
            };
            if pending.object_id != record.object_id
                || pending.version != record.version
                || pending.content_hash != record.content_hash
            {
                continue;
            }
            let mut restored_record = record.clone();
            let now = dfs_now_unix_nanos();
            if (restored_record.next_retry_unix_nanos == u64::MAX
                && !repair_attempt_record_indicates_lost_authority(&restored_record))
                || (repair_attempt_record_indicates_transient_route_failure(&restored_record)
                    && restored_record.next_retry_unix_nanos > now)
            {
                restored_record.in_flight = false;
                restored_record.next_retry_unix_nanos = now;
                restored_record.transfer_request_id = None;
            }
            self.repair_attempt_records.insert(
                (
                    restored_record.intent_id.clone(),
                    restored_record.missing_replica_node_id.clone(),
                ),
                restored_record.clone(),
            );
            let state = pending
                .repair_retries
                .entry(restored_record.missing_replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.max(restored_record.attempts);
            if repair_attempt_record_indicates_lost_authority(&restored_record) {
                state.attempts = state.attempts.max(1);
                state.inflight = false;
                state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
                pending
                    .repair_inflight_request_ids
                    .remove(&restored_record.missing_replica_node_id);
                continue;
            }
            state.inflight = restored_record.in_flight
                && restored_record.next_retry_unix_nanos > dfs_now_unix_nanos();
            if let Some(request_id) = restored_record.transfer_request_id.as_ref() {
                pending.repair_inflight_request_ids.insert(
                    restored_record.missing_replica_node_id.clone(),
                    request_id.clone(),
                );
            } else {
                pending
                    .repair_inflight_request_ids
                    .remove(&restored_record.missing_replica_node_id);
            }
        }
    }

    fn repair_queue_records(&self, include_unattempted: bool) -> Vec<DfsRepairAttemptRecord> {
        let now = dfs_now_unix_nanos();
        let mut records = Vec::new();
        for (intent_id, pending) in &self.writes.pending_writes {
            for (replica_node_id, state) in &pending.repair_retries {
                let key = (intent_id.clone(), replica_node_id.clone());
                if repair_retry_state_is_suppressed(state) {
                    if let Some(existing) = self.repair_attempt_records.get(&key)
                        && repair_attempt_record_indicates_lost_authority(existing)
                    {
                        let mut record = existing.clone();
                        record.attempts = record.attempts.max(state.attempts);
                        record.in_flight = false;
                        record.next_retry_unix_nanos = u64::MAX;
                        record.transfer_request_id = None;
                        records.push(record);
                    }
                    continue;
                }
                if state.attempts == 0 && !include_unattempted {
                    continue;
                }
                let transfer_request_id = pending
                    .repair_inflight_request_ids
                    .get(replica_node_id)
                    .cloned();
                if let Some(existing) = self.repair_attempt_records.get(&key)
                    && existing.next_retry_unix_nanos > now
                    && existing.attempts >= state.attempts
                    && existing.in_flight == state.inflight
                    && existing.transfer_request_id == transfer_request_id
                {
                    records.push(existing.clone());
                    continue;
                }
                let remaining_ticks = state.next_retry_tick.saturating_sub(self.repair_retry_tick);
                let next_retry_unix_nanos =
                    now.saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks));
                let last_error = self
                    .repair_attempt_records
                    .get(&key)
                    .and_then(|record| record.last_error.clone());
                records.push(DfsRepairAttemptRecord {
                    intent_id: intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: replica_node_id.clone(),
                    attempts: state.attempts,
                    in_flight: state.inflight,
                    last_attempt_unix_nanos: if state.attempts == 0 { 0 } else { now },
                    next_retry_unix_nanos: if state.attempts == 0 {
                        now
                    } else {
                        next_retry_unix_nanos
                    },
                    transfer_request_id,
                    last_error,
                });
            }
        }
        if include_unattempted {
            let mut recorded_targets = records
                .iter()
                .map(|record| {
                    (
                        record.intent_id.clone(),
                        record.missing_replica_node_id.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            for job in self.coordinator.repair_jobs() {
                let key = (job.intent_id.clone(), job.missing_replica_node_id.clone());
                if !recorded_targets.insert(key) {
                    continue;
                }
                let Some(pending) = self.writes.pending_writes.get(&job.intent_id) else {
                    continue;
                };
                records.push(DfsRepairAttemptRecord {
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: job.missing_replica_node_id.clone(),
                    attempts: 0,
                    in_flight: false,
                    last_attempt_unix_nanos: 0,
                    next_retry_unix_nanos: now,
                    transfer_request_id: None,
                    last_error: None,
                });
            }
        }
        records.sort_by(|left, right| {
            left.intent_id.cmp(&right.intent_id).then_with(|| {
                left.missing_replica_node_id
                    .cmp(&right.missing_replica_node_id)
            })
        });
        records
    }

    pub(crate) fn active_repair_attempt_records(&self) -> Vec<DfsRepairAttemptRecord> {
        self.repair_queue_records(false)
    }

    /// Return the exact bounded repair work owned by this live service lane.
    ///
    /// Unlike [`Self::active_repair_attempt_records`], this includes jobs which
    /// have not made their first network attempt. The daemon persists these
    /// zero-attempt records at the same committed edge that wakes reactive
    /// maintenance, so a restart never needs a full metadata inventory to
    /// rediscover ordinary repair work.
    pub fn durable_repair_queue_records(&self) -> Vec<DfsRepairAttemptRecord> {
        self.repair_queue_records(true)
    }

    fn restore_pending_writes_for_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        let mut intents = BTreeMap::<String, DfsObjectId>::new();
        for record in records {
            intents
                .entry(record.intent_id.clone())
                .or_insert_with(|| record.object_id.clone());
        }
        for (intent_id, object_id) in intents {
            self.restore_pending_write_for_intent_if_missing(&intent_id, &object_id)?;
        }
        Ok(())
    }

    pub(crate) fn observe_repair_maintenance_events(
        &mut self,
        events: &[NetworkedDfsServiceEvent],
    ) {
        for event in events {
            match event {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id,
                    replica_node_id,
                    ..
                } => {
                    self.repair_attempt_records
                        .remove(&(intent_id.clone(), replica_node_id.clone()));
                }
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id,
                    replica_node_id,
                    reason,
                    ..
                } => {
                    let key = (intent_id.clone(), replica_node_id.clone());
                    if let Some(record) = self.repair_attempt_records.get_mut(&key) {
                        record.in_flight = false;
                        record.last_error = Some(reason.clone());
                    } else if let Some(pending) = self.writes.pending_writes.get(intent_id) {
                        let state = pending.repair_retries.get(replica_node_id);
                        let attempts = state.map(|state| state.attempts).unwrap_or(1).max(1);
                        let next_retry_unix_nanos = if state
                            .is_some_and(repair_retry_state_is_suppressed)
                            && repair_rejection_indicates_lost_authority(reason)
                        {
                            u64::MAX
                        } else {
                            let remaining_ticks = state
                                .map(|state| {
                                    state.next_retry_tick.saturating_sub(self.repair_retry_tick)
                                })
                                .unwrap_or(0);
                            dfs_now_unix_nanos()
                                .saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks))
                        };
                        self.repair_attempt_records.insert(
                            key,
                            DfsRepairAttemptRecord {
                                intent_id: intent_id.clone(),
                                object_id: pending.object_id.clone(),
                                version: pending.version.clone(),
                                content_hash: pending.content_hash.clone(),
                                missing_replica_node_id: replica_node_id.clone(),
                                attempts,
                                in_flight: false,
                                last_attempt_unix_nanos: dfs_now_unix_nanos(),
                                next_retry_unix_nanos,
                                transfer_request_id: pending
                                    .repair_inflight_request_ids
                                    .get(replica_node_id)
                                    .cloned(),
                                last_error: Some(reason.clone()),
                            },
                        );
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn pending_repair_is_locally_authoritative(
        &mut self,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        self.is_local_metadata_primary_for_object(&object)
    }

    fn pending_repair_targets_current_content(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        if job.object_id != pending.object_id || job.version != pending.version {
            return Ok(false);
        }
        if self
            .coordinator
            .intent(&job.intent_id)
            .is_some_and(|intent| !intent.publication.is_current())
        {
            return self.retained_file_content_is_live(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
            );
        }
        let object = match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => return Ok(true),
            Err(error) => return Err(error),
        };
        if object.content_hash.as_deref() == Some(pending.content_hash.as_str()) {
            return Ok(true);
        }
        if object.version == pending.version {
            return Ok(false);
        }
        if self.retained_file_content_is_live(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
        )? {
            return Ok(true);
        }
        let base_version = self
            .coordinator
            .intent(&job.intent_id)
            .and_then(|intent| intent.base_version.as_ref());
        Ok(base_version.is_some_and(|base_version| object.version == *base_version))
    }

    pub(crate) fn record_repair_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                return;
            }
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(
                repair_retry_delay_ticks(state.attempts)
                    .max(repair_transfer_inflight_timeout_ticks(pending.size_bytes)),
            );
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    fn record_repair_fetch_inflight(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                return;
            }
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(fetch_inflight_timeout_ticks());
            pending.repair_inflight_request_ids.remove(replica_node_id);
        }
    }

    pub(crate) fn record_repair_retry_scheduled(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        self.record_repair_retry_scheduled_with_error(intent_id, replica_node_id, None);
    }

    pub(crate) fn record_repair_retry_scheduled_with_error(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        last_error: Option<String>,
    ) {
        let tick = self.repair_retry_tick;
        let mut record_update = None;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                pending.repair_inflight_request_ids.remove(replica_node_id);
                return;
            }
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            let delay_ticks =
                repair_retry_delay_ticks_for_error(state.attempts, last_error.as_deref());
            state.next_retry_tick = tick.saturating_add(delay_ticks);
            pending.repair_inflight_request_ids.remove(replica_node_id);
            if let Some(last_error) = last_error {
                let remaining_ticks = state.next_retry_tick.saturating_sub(tick);
                record_update = Some(DfsRepairAttemptRecord {
                    intent_id: intent_id.to_string(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    missing_replica_node_id: replica_node_id.clone(),
                    attempts: state.attempts,
                    in_flight: false,
                    last_attempt_unix_nanos: dfs_now_unix_nanos(),
                    next_retry_unix_nanos: dfs_now_unix_nanos()
                        .saturating_add(repair_retry_delay_nanos_for_ticks(remaining_ticks)),
                    transfer_request_id: None,
                    last_error: Some(last_error),
                });
            }
        }
        if let Some(record) = record_update {
            self.repair_attempt_records.insert(
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ),
                record,
            );
        }
    }

    pub(crate) fn suppress_repair_retries_for_lost_authority(
        &mut self,
        intent_id: &str,
        response_replica_node_id: Option<&NodeId>,
        drop_committed_pending_write: bool,
    ) {
        let remove_committed_pending_write = drop_committed_pending_write
            && self
                .coordinator
                .intent(intent_id)
                .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        let mut replica_node_ids = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .map(|job| job.missing_replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if let Some(replica_node_id) = response_replica_node_id {
            replica_node_ids.insert(replica_node_id.clone());
        }
        if let Some(pending) = self.writes.pending_writes.get(intent_id) {
            replica_node_ids.extend(pending.repair_retries.keys().cloned());
        }
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            for replica_node_id in replica_node_ids {
                let state = pending
                    .repair_retries
                    .entry(replica_node_id.clone())
                    .or_default();
                state.inflight = false;
                state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
                pending.repair_inflight_request_ids.remove(&replica_node_id);
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_failed_from.clear();
        }
        if remove_committed_pending_write {
            self.writes.pending_writes.remove(intent_id);
        }
    }

    pub(crate) fn record_repair_metadata_settle(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
            // Receipt persistence, coordinator convergence, and the visible
            // location update all completed before this point. Keeping a
            // timeout-only retry record would retain the whole pending blob
            // after N without protecting an unfinished metadata operation.
            pending.repair_retries.remove(replica_node_id);
            pending.repair_inflight_request_ids.remove(replica_node_id);
        }
        self.repair_attempt_records
            .remove(&(intent_id.to_string(), replica_node_id.clone()));
    }

    pub(crate) fn request_repair_blob_from_alternate(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<DfsRepairBlobRequestOutcome, DfsError> {
        let mut skipped_failed_source = false;
        let mut last_error = None;
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let send_outcome = self.send_best_effort_message_if_routable_outcome(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
            if send_outcome.sent {
                tracing::debug!(
                    target: "sinomo.dfs.repair",
                    local_node_id = %self.node_id,
                    source_node_id = %source_node_id,
                    missing_replica_node_id = %job.missing_replica_node_id,
                    intent_id = %job.intent_id,
                    object_id = %pending.object_id,
                    version = %pending.version,
                    content_hash = %pending.content_hash,
                    request_id = %request_id,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(&job.intent_id, source_node_id, request_id);
                self.record_repair_fetch_inflight(&job.intent_id, &job.missing_replica_node_id);
                return Ok(DfsRepairBlobRequestOutcome {
                    requested: true,
                    last_error: None,
                });
            }
            if send_outcome.last_error.is_some() {
                last_error = send_outcome.last_error;
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.writes.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        tracing::debug!(
            target: "sinomo.dfs.repair",
            local_node_id = %self.node_id,
            missing_replica_node_id = %job.missing_replica_node_id,
            intent_id = %job.intent_id,
            object_id = %pending.object_id,
            version = %pending.version,
            content_hash = %pending.content_hash,
            skipped_failed_source,
            "no routable DFS repair blob alternate source"
        );
        Ok(DfsRepairBlobRequestOutcome {
            requested: false,
            last_error,
        })
    }

    pub(crate) fn repair_blob_sources(
        &self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut candidates = BTreeSet::new();
        for receipt in self.coordinator.receipts_for_intent(&job.intent_id) {
            if receipt.replica_node_id != self.node_id
                && receipt.replica_node_id != job.missing_replica_node_id
            {
                candidates.insert(receipt.replica_node_id.clone());
            }
        }
        match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => {
                for node_id in &object.current_locations {
                    if node_id != &self.node_id && node_id != &job.missing_replica_node_id {
                        candidates.insert(node_id.clone());
                    }
                }
            }
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(self.ranked_transfer_sources(candidates))
    }
}
