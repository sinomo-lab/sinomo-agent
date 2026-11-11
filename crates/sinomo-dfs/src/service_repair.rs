//! Repair, stage, and pending-transfer lifecycle for the networked DFS service.
//!
//! This module owns retry scheduling, stale inflight cleanup, alternate-source
//! fetch requests, inbound repair transfer handling, and repair-attempt
//! persistence. Unrelated fetch/read response handling stays in
//! `service_fetch_transfer` and `service_read_transfer`; outbound session
//! request construction stays in `service_outbound`.

use std::collections::BTreeSet;
use std::time::Duration;

use sinomo_core::node::NodeId;

use crate::transfer::{
    PendingNetworkedWrite, fetch_inflight_timeout_ticks,
    repair_retry_delay_ticks, stage_inflight_retry_delay_ticks,
    };
use crate::*;

// The persisted deadline belongs to the same settled attempt. Restoring that
// row cannot make its already elapsed backoff wait for a stale resident tick.

#[cfg(test)]
thread_local! {
    // Negative cuts on this explicit caller only. Neither supplies a reply or
    // retained history, and no worker/timer is installed.
    pub(crate) static HISTORY_QUERY_AFTER_ATTEMPT: std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<(), DfsError>>>> = const { std::cell::RefCell::new(None) };
    pub(crate) static HISTORY_QUERY_AFTER_OBSERVATION: std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<(), DfsError>>>> = const { std::cell::RefCell::new(None) };
}

/// Logical DFS write/repair records can outlive individual network sessions.
/// This is a stale-state horizon, not a socket or stream idle timeout; active
/// network I/O uses `sinomo-network`'s five-second policy.
pub(crate) const DFS_PENDING_TRANSFER_STALE_AFTER: Duration = Duration::from_secs(60);
/// How long a primary keeps an unanswered routed-write response obligation
/// before assuming the requester gave up.
const DFS_REMOTE_WRITE_RESPONSE_TTL: Duration = Duration::from_secs(60);

// One policy calculation for both ordinary retry scheduling and its exact
// terminal checkpoint. Prospective state is copied DATA, never a live claim.

pub(crate) const DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE: usize = 16;

// Queue retirement consumes only this exact stable identity. Retry fields
// are never written back from a retirement record.

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn publish_superseded_placement_intent_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        replacement_id: &str,
        after: Option<&DfsWriteIntentPageCursor>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<DfsWriteIntentPageCursor>), DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "placement retirement requires its current metadata owner".into(),
            })?;
        let replacement = backend
            .get_intent(&object.object_id, replacement_id)?
            .filter(|intent| {
                intent.publication.is_current()
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.file_system_id == object.file_system_id
                    && intent.target_version == object.version
                    && intent.content_hash == object.content_hash
            })
            .ok_or_else(|| DfsError::Metadata {
                message: "placement replacement is not durably installed".into(),
            })?;
        let page = backend.intents_for_object_version_page(
            &object.object_id,
            &object.version,
            after,
            limit,
        )?;
        let retired = page
            .intents
            .into_iter()
            .filter(|intent| {
                intent.intent_id != replacement.intent_id
                    && intent.publication.is_current()
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.file_system_id == object.file_system_id
                    && intent.content_hash == object.content_hash
            })
            .collect::<Vec<_>>();
        if retired.is_empty() {
            return Ok((Vec::new(), page.next_cursor));
        }
        let mut effects = Vec::new();
        if retired
            .iter()
            .any(|intent| !intent.intent_id.starts_with("placement:"))
        {
            if let Some(version) = backend
                .get_version(&object.object_id, &object.version)?
                .filter(|version| version.has_retention_grant())
            {
                let owner = self.recorded_retained_repair_owner(&version)?;
                match backend.get_intent(&object.object_id, &owner.intent_id)? {
                    Some(existing)
                        if crate::file_history::retained_repair_plan_matches(&existing, &owner) => {
                    }
                    Some(_) => {
                        return Err(DfsError::Metadata {
                            message: "retained repair owner changes its recorded durability".into(),
                        });
                    }
                    None => effects.push(DfsPreparedMetadataEffect::PutIntent { intent: owner }),
                }
            }
        }
        let at = time::OffsetDateTime::now_utc();
        for intent in &retired {
            // The bounded history page also reconstructs compacted receipts.
            // Replaying a settled retirement must preserve its original evidence
            // and retention time, rather than author another metadata operation.
            if backend
                .get_intent_receipt(&object.object_id, &intent.intent_id)?
                .is_some_and(|receipt| receipt.matches_intent(intent))
            {
                continue;
            }
            let receipt = DfsWriteIntentReceipt::from_intent(intent, at).ok_or_else(|| {
                DfsError::Metadata {
                    message: "placement retirement lacks exact terminal intent evidence".into(),
                }
            })?;
            effects.push(DfsPreparedMetadataEffect::PutIntentReceipt { receipt });
        }
        let ids = retired.into_iter().map(|intent| intent.intent_id).collect();
        // One guarded commit installs the retained owner before compacting
        // executable full records into their exact terminal retry receipts.
        if !effects.is_empty() {
            self.publish_object_metadata_effects(object, effects)?;
        }
        Ok((ids, page.next_cursor))
    }
    pub(crate) fn recorded_retained_repair_owner(
        &self,
        version: &DfsObjectVersionRecord,
    ) -> Result<DfsWriteIntentRecord, DfsError> {
        let owner = crate::file_history::retained_repair_intent(version, None)?;
        let durability = version
            .content_durability
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained repair requires its recorded version durability".into(),
            })?;
        let receipts = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained repair requires its actual metadata owner".into(),
            })?
            .receipts_for_version(&version.object_id, &durability.source_version)?;
        let copies = receipts
            .iter()
            .filter(|receipt| {
                crate::metadata_receipt::recorded_receipt_matches_version(receipt, version)
                    && durability
                        .desired_replica_node_ids
                        .contains(&receipt.replica_node_id)
            })
            .map(|receipt| &receipt.replica_node_id)
            .collect::<BTreeSet<_>>();
        if copies.len() < durability.policy.min_commit_replicas {
            return Err(DfsError::Metadata {
                message: "retained repair owner lacks recorded distinct W receipts".into(),
            });
        }
        Ok(owner)
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
        let active_reservations = active_stage_reservations.collect::<BTreeSet<_>>().len();
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
            .take(budget)
            .cloned()
            .collect()
    }

    /// A replica send is about to leave. Repair sends are the executor's,
    /// which registers them itself. A stage send is also its target's attempt
    /// in the executor's registry, so a need that appears for that replica
    /// once the write commits below N is not sent twice meanwhile.
    pub(crate) fn begin_replica_transfer_attempt(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
        _background_repair: bool,
    ) -> Result<(), DfsError> {
        if !stage {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some((root, _owner)) = self.native_publication_owner_for_observation()
            && let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(
                &sinomo_core::local::SinomoDataDir::new(root),
            )
        {
            hook(
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                    kind: sinomo_api::dfs::maintenance::DfsRepairTransferKind::Stage,
                    background_repair: _background_repair,
                    intent_id_digest: *blake3::hash(intent_id.as_bytes()).as_bytes(),
                    target_id_digest: *blake3::hash(target.as_str().as_bytes()).as_bytes(),
                    request_id_digest: *blake3::hash(request_id.as_bytes()).as_bytes(),
                },
                None,
                None,
            )?;
        }
        let Some(pending) = self.writes.pending_writes.get(intent_id) else {
            return Ok(());
        };
        let (object_id, version) = (pending.object_id.clone(), pending.version.clone());
        if pending
            .stage_inflight_request_ids
            .get(target)
            .is_some_and(|existing| existing != request_id)
        {
            return Err(DfsError::Metadata {
                message: "replica transfer cannot replace an unresolved request identity".into(),
            });
        }
        let generation = self.backend_receipt_slot_generation(&object_id, &version, target)?;
        let pending = self
            .writes
            .pending_writes
            .get_mut(intent_id)
            .expect("present above");
        let retrying = pending.stage_inflight_request_ids.contains_key(target);
        let state = pending.stage_retries.entry(target.clone()).or_default();
        state.retrying_unresolved_transfer = retrying;
        state.previous_transfer_delivery_state = state.transfer_delivery_state;
        state.completed_transfer_request_id = None;
        pending
            .planned_receipt_generations
            .insert(target.clone(), generation);
        // Before polling the carrier, its current outcome is unknown.
        state.transfer_delivery_state = None;
        state.inflight = true;
        pending
            .stage_inflight_request_ids
            .insert(target.clone(), request_id.to_string());
        crate::repair_executor::repair_executor_attempt_started(
            &self.node_id,
            (object_id, version, target.clone()),
            request_id,
            generation,
        );
        Ok(())
    }

    pub(crate) fn observe_replica_transfer_delivery(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
        outcome: &StageBlobSendOutcome,
    ) -> Result<(), DfsError> {
        use sinomo_core::network::NetworkDeliveryState;
        if !stage {
            return Ok(());
        }
        let Some(pending) = self.writes.pending_writes.get_mut(intent_id) else {
            return Ok(());
        };
        if pending.stage_inflight_request_ids.get(target).map(String::as_str) != Some(request_id) {
            return Err(DfsError::Metadata {
                message: "replica send result differs from its actual pending request".into(),
            });
        }
        let key = (pending.object_id.clone(), pending.version.clone(), target.clone());
        let state = pending.stage_retries.entry(target.clone()).or_default();
        state.transfer_delivery_state = if outcome.sent {
            Some(NetworkDeliveryState::PossiblyApplied)
        } else if state.retrying_unresolved_transfer {
            join_transfer_delivery(state.previous_transfer_delivery_state, outcome.delivery_state)
        } else {
            outcome.delivery_state
        };
        state.inflight = outcome.sent;
        if state.transfer_delivery_state == Some(NetworkDeliveryState::DefinitelyNotSent) {
            pending.stage_inflight_request_ids.remove(target);
        }
        if !outcome.sent {
            // A send that failed is that attempt's outcome.
            crate::repair_executor::repair_executor_attempt_ended_with_error(
                &self.node_id,
                &key,
                request_id,
                crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                outcome.last_error.clone(),
            );
        }
        Ok(())
    }

    pub(crate) fn record_replica_transfer_completed(
        &mut self,
        intent_id: &str,
        target: &NodeId,
        request_id: &str,
        stage: bool,
    ) -> Result<(), DfsError> {
        if !stage {
            return Ok(());
        }
        let pending = self
            .writes
            .pending_writes
            .get_mut(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: "terminal replica response has no current pending transfer".into(),
            })?;
        if pending.stage_inflight_request_ids.get(target).map(String::as_str) != Some(request_id) {
            return Err(DfsError::Distributed {
                message: "terminal replica response differs from its exact current request".into(),
            });
        }
        pending.stage_inflight_request_ids.remove(target);
        let state = pending.stage_retries.entry(target.clone()).or_default();
        state.inflight = false;
        state.completed_transfer_request_id = Some(request_id.to_string());
        Ok(())
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
            })
        {
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
            command.receipt_generation,
        )?;
        Ok(self.repair_replica_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    // This local fixture-gated content proof does not replace any preflight,
    // later metadata publication or reply handling. Default stores return None
    // without I/O and take precisely the existing V1 content path.

    /// A repair reply publishes from the reply and the shard tables alone, on
    /// any lane: the receipt-generation compare decides whether it may. Its
    /// outcome ends the executor's attempt, whichever lane observes it.
    pub(crate) fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let rejected = |response: &DfsRepairReplicaResponse, replica: NodeId, reason: String| {
            NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id.clone(),
                object_id: response.object_id.clone(),
                replica_node_id: replica,
                reason,
            }
        };
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(rejected(&response, frame_source_node_id, reason));
        }
        let key = (
            response.object_id.clone(),
            response.version.clone(),
            response.replica_node_id.clone(),
        );
        let published = match &response.result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                self.publish_repair_reply_receipt(&response, content_hash)
            }
            DfsBlobTransferResult::Rejected { reason } => Ok((
                crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                rejected(&response, response.replica_node_id.clone(), reason.clone()),
            )),
        };
        // Every exit after the reply ends its attempt; one that failed to
        // publish counts as a failed attempt with its error.
        let (outcome, error) = match &published {
            Ok((outcome, event)) => (*outcome, crate::repair_executor::repair_event_error(event)),
            Err(error) => (
                crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                Some(error.to_string()),
            ),
        };
        crate::repair_executor::repair_executor_attempt_ended_with_error(
            &self.node_id,
            &key,
            &response.request_id,
            outcome,
            error,
        );
        published.map(|(_, event)| event)
    }

    fn publish_repair_reply_receipt(
        &mut self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<
        (
            crate::repair_executor::DfsRepairAttemptOutcome,
            NetworkedDfsServiceEvent,
        ),
        DfsError,
    > {
        use crate::repair_executor::DfsRepairAttemptOutcome;
        let rejected = |reason: String| NetworkedDfsServiceEvent::RepairRejected {
            intent_id: response.intent_id.clone(),
            object_id: response.object_id.clone(),
            replica_node_id: response.replica_node_id.clone(),
            reason,
        };
        let version_hash = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "repair reply publication requires the shard tables".into(),
            })?
            .get_version(&response.object_id, &response.version)?
            .and_then(|record| record.content_hash);
        if version_hash.as_deref() != Some(content_hash) {
            return Ok((
                DfsRepairAttemptOutcome::Superseded,
                rejected(format!(
                    "repair reply content {content_hash} differs from the version's {version_hash:?}"
                )),
            ));
        }
        let published = self.with_table_intent_placement_authority(
            &response.object_id,
            &response.intent_id,
            |service| {
            match service.put_metadata_receipt(
                &response.file_system_id,
                &response.object_id,
                &response.version,
                &response.replica_node_id,
                content_hash,
                response.receipt_generation,
            ) {
                Ok(inserted) => Ok(Ok(inserted)),
                Err(error @ DfsError::ReceiptGenerationSuperseded { .. }) => Ok(Err(error)),
                Err(error) => Err(error),
            }
            },
        )?;
        match published {
            // A redelivered reply finds its receipt already published: the
            // need is settled and nothing is repaired again.
            Some(Ok(false)) => Ok((
                DfsRepairAttemptOutcome::Settled,
                rejected("the replica's receipt is already published at this generation".into()),
            )),
            Some(Ok(true)) => {
                // A lane that holds this write's state learns the receipt
                // like any other; the tables already hold it.
                if self.coordinator.intent(&response.intent_id).is_some() {
                    let _ = self
                        .coordinator
                        .record_durable_receipt(&response.intent_id, response.replica_node_id.clone());
                    self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                }
                Ok((
                    DfsRepairAttemptOutcome::Settled,
                    NetworkedDfsServiceEvent::ReplicaRepaired {
                        intent_id: response.intent_id.clone(),
                        object_id: response.object_id.clone(),
                        version: response.version.clone(),
                        replica_node_id: response.replica_node_id.clone(),
                        content_hash: content_hash.to_owned(),
                    },
                ))
            }
            Some(Err(superseded)) => Ok((
                DfsRepairAttemptOutcome::Superseded,
                rejected(superseded.to_string()),
            )),
            None => Ok((
                DfsRepairAttemptOutcome::Superseded,
                rejected("write intent is closed or its placement policy was superseded".into()),
            )),
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
        receipt_generation: u64,
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
                receipt_generation,
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

/// Work budget for one pass of stage retries. A failed or unroutable send
/// still consumes one slot: the bound limits attempted work and wall-clock
/// exposure, not only successful schedules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DfsStageRetryBudget {
    remaining_targets: usize,
}

impl DfsStageRetryBudget {
    pub(crate) const fn new(max_targets: usize) -> Self {
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

}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    /// One maintenance turn of an unfinished write's own transfers: advance
    /// the retry tick, expire stale fetches and re-send due stages of intents
    /// still awaiting their commit quorum.
    pub(crate) async fn retry_pending_stages_async(
        &mut self,
        max_targets: usize,
        execution_exclusions: &DfsMaintenanceExecutionExclusions,
    ) -> Result<(), DfsError> {
        self.prune_idle_transfer_state();
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget = DfsStageRetryBudget::new(max_targets);
        self.retry_pending_stages_async_bounded(&mut budget, execution_exclusions)
            .await
    }

    async fn retry_pending_stages_async_bounded(
        &mut self,
        budget: &mut DfsStageRetryBudget,
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
                    // Local staging is not a StageBlob RPC target. After a
                    // restart it may nevertheless be the only receipt missing
                    // from W. Recover its bytes through the ordinary correlated
                    // fetch, without inventing a receipt or transfer outcome.
                    if intent.desired_replica_nodes.contains(&self.node_id)
                        && !durable_nodes.contains(&self.node_id)
                        && !execution_exclusions.contains(&self.node_id)
                        && self.pending_stage_send_budget(
                            &intent,
                            &pending,
                            &durable_nodes,
                            Some(execution_exclusions),
                        ) > 0
                        && budget.reserve(1) == 1
                    {
                        self.request_stage_blob_from_alternate_async(
                            &intent,
                            &pending,
                            execution_exclusions,
                        )
                        .await?;
                    }
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

    pub(crate) fn remove_pending_write_state(&mut self, intent_id: &str) {
        self.writes.pending_writes.remove(intent_id);
    }

    /// Resolve only the outgoing identities affected by this publication.
    /// Disabled policies do not add object/history lookups to ordinary writes.
    pub(crate) fn file_history_repair_roots_before_objects<'a>(
        &self,
        objects: impl IntoIterator<Item = &'a DfsObjectMetadata>,
        file_system: Option<&DfsFileSystem>,
    ) -> Result<BTreeSet<(DfsObjectId, DfsVersion)>, DfsError> {
        let mut roots = BTreeSet::new();
        let Some(file_system) = file_system.filter(|fs| !fs.retention_policies.is_empty()) else {
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
                .is_empty()
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
            // An owner this enrolls changes the repair index, which wakes the
            // primary's executor.
            self.reconcile_file_history_repair_root(&object, &version)?;
        }
        Ok(())
    }

    /// Resume only repair work required by an immutable revision's current
    /// live roots. This does not recreate a reference or replay its mutation.
    /// A remote owner receives the same wake from exact-outcome reconciliation.
    /// sinomo:dfs-operation
    pub fn reconcile_content_reference_maintenance(
        &mut self,
        identity: &crate::DfsRevisionReference,
    ) -> Result<(), DfsError> {
        if self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_version(&identity.object_id, &identity.version))
            .transpose()?
            .flatten()
            .is_some_and(|record| record.file_system_id != identity.file_system_id)
        {
            return Err(DfsError::Metadata {
                message: "reference maintenance names a foreign filesystem".into(),
            });
        }
        self.activate_file_history_repair_roots([(
            identity.object_id.clone(),
            identity.version.clone(),
        )])
    }

    /// Shared content-root selection. The caller separately owns current
    /// physical authority, execution admission and each transfer's native permit.
    pub(crate) fn committed_repair_content_is_active(
        &self,
        object: &DfsObjectMetadata,
        record: &DfsWriteIntentRecord,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let target = backend.get_version(&object.object_id, &record.target_version)?;
        if record.publication.is_current() {
            let current = backend.get_version(&object.object_id, &object.version)?;
            if crate::content_durability::current_repair_content_matches(
                object,
                record,
                target.as_ref(),
                current.as_ref(),
            ) {
                return Ok(true);
            }
            if crate::content_durability::current_repair_content_is_superseded(
                object,
                record,
                target.as_ref(),
                current.as_ref(),
            ) {
                return Ok(false);
            }
            return Err(DfsError::Metadata {
                message: "committed Current repair lineage is unavailable; preserve its native recovery owner".into(),
            });
        }
        let Some(target) = target else {
            return Err(DfsError::Metadata {
                message: "committed retained repair version is unavailable; preserve its native recovery owner".into(),
            });
        };
        if !target.has_retention_grant() {
            return Ok(false);
        }
        if target.file_system_id != record.file_system_id
            || target.object_id != record.object_id
            || target.version != record.target_version
            || target.content_hash.is_none()
            || target.content_hash != record.content_hash
        {
            return Err(DfsError::Metadata {
                message: "committed retained repair subject is unavailable; preserve its native recovery owner".into(),
            });
        }
        if !self.retained_file_content_is_live(
            &record.object_id,
            &record.target_version,
            record
                .content_hash
                .as_deref()
                .expect("matched retained hash"),
        )? {
            return Ok(false);
        }
        if !crate::content_durability::retained_repair_content_matches(object, record, &target) {
            return Err(DfsError::Metadata {
                message: "committed retained repair plan is unavailable; preserve its native recovery owner".into(),
            });
        }
        Ok(true)
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
            .filter(|record| record.has_retention_grant())
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
                    continue;
                }
                let active = self.committed_repair_content_is_active(&object, &record)?;
                if !active {
                    continue;
                }
                // Only an actual eligible owner of this recorded retained plan
                // can suppress enrollment. Placement's different transient job
                // plan cannot discharge the immutable captured N obligation.
                if let Some(plan) = version_record.content_durability.as_deref() {
                    full_owner |= !record.intent_id.starts_with("placement:")
                        && record.policy == plan.policy
                        && record.desired_replica_node_ids == plan.desired_replica_node_ids;
                }
                activated |= self.committed_repair_content_is_active(&object, &record)?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if !full_owner && !unfinished_owner {
            let scope = self.metadata_effect_scope_for_object(
                &object,
                format!("metadata:{}", sinomo_core::Id::generate()),
            )?;
            let _execution = self.acquire_prepared_execution(&scope)?;
            // Admission may have queued: revalidate the real immutable version
            // and its recorded W under this existing owner before enrollment.
            let Some(version_record) = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .get_version(object_id, version)?
                .filter(|record| record.has_retention_grant())
            else {
                return Ok(activated);
            };
            let owner = self.recorded_retained_repair_owner(&version_record)?;
            let history = self
                .metadata_backend
                .as_ref()
                .expect("history metadata")
                .object_write_intent_history(object_id)?;
            if let Some(stored) = history
                .intents
                .iter()
                .find(|record| record.intent_id == owner.intent_id)
            {
                if !crate::file_history::retained_repair_plan_matches(stored, &owner) {
                    return Err(DfsError::Metadata {
                        message: "retained owner already has a different stored identity".into(),
                    });
                }
                activated |= self.committed_repair_content_is_active(&object, stored)?;
                return Ok(activated);
            }
            if history
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == owner.intent_id)
            {
                return Err(DfsError::Metadata {
                    message: "new retained owner identity already has a terminal receipt".into(),
                });
            }
            // The shared guarded commit enrolls this actual owner before
            // publication; recovery cannot execute its dormant queue row early.
            self.publish_captured_object_metadata_effects(
                &object,
                scope,
                vec![DfsPreparedMetadataEffect::PutIntent {
                    intent: owner.clone(),
                }],
            )?;
            if self.retained_file_content_is_live(
                object_id,
                version,
                owner
                    .content_hash
                    .as_deref()
                    .expect("validated repair hash"),
            )? {
                // The owner's rows carry its needs in the shard's repair
                // index, which the primary's executor schedules.
                activated |= self.committed_repair_content_is_active(&object, &owner)?;
            }
        }
        Ok(activated)
    }

    /// A committed write keeps its transfer state only while a stage it sent
    /// is unanswered, so that reply still finds it; repair state lives in the
    /// executor and needs none.
    pub(crate) fn cleanup_committed_pending_write_if_repaired(&mut self, intent_id: &str) {
        let is_committed = self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        if !is_committed {
            return;
        }
        if self
            .writes
            .pending_writes
            .get(intent_id)
            .is_some_and(|pending| {
                !pending.stage_inflight_request_ids.is_empty()
                    || !pending.fetch_inflight_request_ids.is_empty()
            })
        {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    /// One synchronous turn of unfinished writes' own transfers; see
    /// [`retry_pending_stages_async`](Self::retry_pending_stages_async).
    pub(crate) fn retry_pending_stages(&mut self, max_targets: usize) -> Result<(), DfsError> {
        self.prune_idle_transfer_state();
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget = DfsStageRetryBudget::new(max_targets);
        self.retry_pending_stages_scoped_bounded(None, &mut budget)
    }

    /// The stage retries of these unfinished writes only, for a lane that
    /// waits on them.
    pub(crate) fn retry_pending_stages_for_intents(
        &mut self,
        intent_ids: &BTreeSet<String>,
        max_targets: usize,
    ) -> Result<(), DfsError> {
        self.prune_idle_transfer_state();
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        let mut budget = DfsStageRetryBudget::new(max_targets);
        self.retry_pending_stages_scoped_bounded(Some(intent_ids), &mut budget)
    }

    fn retry_pending_stages_scoped_bounded(
        &mut self,
        intent_filter: Option<&BTreeSet<String>>,
        budget: &mut DfsStageRetryBudget,
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
        let mut budget = DfsStageRetryBudget::new(usize::MAX);
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
        budget: &mut DfsStageRetryBudget,
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
                if intent.desired_replica_nodes.contains(&self.node_id)
                    && !durable_nodes.contains(&self.node_id)
                    && self.pending_stage_send_budget(&intent, &pending, &durable_nodes, None) > 0
                    && budget.reserve(1) == 1
                {
                    self.request_stage_blob_from_alternate(&intent, &pending)?;
                }
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

    pub(crate) fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
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
            state.transfer_delivery_state =
                Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
            state.completed_transfer_request_id = None;
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
}

pub(crate) fn join_transfer_delivery(
    previous: Option<sinomo_core::network::NetworkDeliveryState>,
    incoming: Option<sinomo_core::network::NetworkDeliveryState>,
) -> Option<sinomo_core::network::NetworkDeliveryState> {
    use sinomo_core::network::NetworkDeliveryState;
    match (previous, incoming) {
        (Some(NetworkDeliveryState::PossiblyApplied), _)
        | (_, Some(NetworkDeliveryState::PossiblyApplied)) => {
            Some(NetworkDeliveryState::PossiblyApplied)
        }
        (
            Some(NetworkDeliveryState::DefinitelyNotSent),
            Some(NetworkDeliveryState::DefinitelyNotSent),
        ) => Some(NetworkDeliveryState::DefinitelyNotSent),
        _ => None,
    }
}

#[cfg(test)]
mod retained_receipt_publisher_tests {
    use super::*;

    #[test]
    fn native_retained_receipts_allow_primary_publisher_distinct_from_write_coordinator() {
        let path = "/services/retained-publisher-role.bin";
        let (runtime, browser, config) = crate::test_support::current_native_metadata_service(
            "native-retained-publisher-role",
            3,
            path,
        )
        .unwrap();
        drop(browser);
        let primary = config.primary_node_id.clone();
        let caller = config.replica_node_ids[0].clone();
        assert_ne!(caller, primary);
        let mut writer = runtime.client(&caller).unwrap();
        let stat = writer
            .posix_write_file(
                path,
                b"actual routed W2 retained content",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(2, 2).unwrap(),
                    replica_candidates: vec![primary.clone(), caller.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let primary_service = runtime.metadata_primary_service(path).unwrap();
        let original = primary_service
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        let plan = original.content_durability.as_ref().unwrap();
        assert_eq!(plan.coordinator_node_id, caller);
        assert_eq!(plan.policy.min_commit_replicas, 2);
        assert_eq!(plan.policy.desired_replicas, 2);
        let receipts = primary_service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &plan.source_version)
            .unwrap();
        assert_eq!(
            receipts
                .iter()
                .map(|r| &r.replica_node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        assert!(receipts.iter().all(|r| r.receipt.owner_node_id == primary));
        drop(primary_service);

        let capture_request = crate::DfsContentReferenceRequest {
            namespace: "vendor.semantic".into(),
            path: path.into(),
            identity: crate::DfsRevisionReference {
                file_system_id: stat.file_system_id.clone(),
                object_id: stat.object_id.clone(),
                version: stat.version.clone(),
            },
            operation: crate::DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "primary-publisher".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        };
        let capture = writer.retain_content_revision(capture_request.clone());
        let reader = runtime.metadata_primary_service(path).unwrap();
        let retained = reader
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        assert!(
            retained.has_retention_grant(),
            "normal capture must publish actual retained provenance"
        );
        let owner = reader.recorded_retained_repair_owner(&retained);
        eprintln!(
            "RETAINED_RECEIPT_ROLE {}",
            serde_json::json!({
                "primary": primary, "coordinator": caller,
                "object": stat.object_id, "version": stat.version,
                "capture_operation": capture_request.operation_id,
                "capture_result": format!("{capture:?}"),
                "owner_result": format!("{owner:?}"),
                "receipts": receipts,
            })
        );
        if owner.is_ok() {
            // These are invalid requested views of real recorded receipts, not
            // supplied acknowledgements or mutations of the actual metadata.
            let mut wrong = retained.clone();
            wrong.content_hash = Some("blake3:wrong".into());
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.content_durability.as_mut().unwrap().source_version = DfsVersion::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.object_id = DfsObjectId::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong.file_system_id = DfsFileSystemId::generate();
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
            let mut wrong = retained.clone();
            wrong
                .content_durability
                .as_mut()
                .unwrap()
                .desired_replica_node_ids = vec![primary.clone(), primary.clone()];
            assert!(reader.recorded_retained_repair_owner(&wrong).is_err());
        }
        drop(reader);
        drop(writer);
        runtime.close().unwrap();
        capture.expect(
            "normal retained capture keeps actual W2 evidence across publisher/coordinator roles",
        );
        let owner = owner.expect("recorded actual primary receipts satisfy retained W2");
        assert_eq!(owner.coordinator_node_id, caller);
        assert_eq!(owner.policy.min_commit_replicas, 2);
    }
}
