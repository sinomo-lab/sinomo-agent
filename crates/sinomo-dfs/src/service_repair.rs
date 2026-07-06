//! Repair, stage, and pending-transfer lifecycle for the networked DFS service.
//!
//! This module owns retry scheduling, stale inflight cleanup, alternate-source
//! fetch requests, inbound repair transfer handling, and repair-attempt
//! persistence. Unrelated fetch/read byte assembly stays in
//! `service_fetch_transfer` and `service_read_transfer`; outbound packet
//! construction stays in `service_outbound`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, DFS_REPAIR_RETRY_SUPPRESSED_TICK, InProgressBlobTransfer,
    PendingNetworkedWrite, blob_transfer_key, fetch_inflight_timeout_ticks,
    repair_retry_delay_nanos_for_ticks, repair_retry_delay_ticks, repair_retry_state_is_suppressed,
    repair_transfer_inflight_timeout_ticks, stage_inflight_retry_delay_ticks, validate_chunk_bytes,
    validate_chunk_hash, wake_repair_retry_state,
};
use crate::*;

pub(crate) const DFS_CHUNK_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
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

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            )
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
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
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

    pub(crate) fn handle_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "repair replica chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if let Err(error) = validate_chunk_hash(
            "chunked DFS blob transfer",
            command.chunk_index,
            &command.bytes,
            command.chunk_hash.as_deref(),
        ) {
            self.incoming_blob_transfers.remove(&transfer_key);
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            ) {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self
                .ensure_replica_object_metadata(command.object_metadata.as_ref())
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
            {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                let result = DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
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
                return Ok(Some(self.repair_replica_result_event(
                    command.intent_id,
                    command.object_id,
                    command.version,
                    result,
                )));
            }
            if command.chunk_index != 0 {
                let reason = format!(
                    "repair replica chunk transfer {} started at chunk {} without earlier chunks",
                    command.intent_id, command.chunk_index
                );
                return self.reject_repair_replica_chunk(requester_node_id, command, reason);
            }
            let assembler = match self.content.begin_chunked_object_version(
                command.object_id.as_str(),
                command.version.as_str(),
                &command.content_hash,
                command.total_size_bytes,
                command.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self.reject_repair_replica_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
            };
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.object_metadata.clone(),
                command.total_size_bytes,
                command.total_chunks,
                assembler,
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting repair replica chunk metadata for {}",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        let transfer_complete = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(true) => true,
            Ok(false) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        if !transfer_complete {
            return Ok(None);
        }
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        let local_node_id = self.node_id.clone();
        self.commit_replica_metadata_cache(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            transfer.object_metadata.as_ref(),
        )?;
        let result = DfsBlobTransferResult::Committed {
            content_hash: manifest.content_hash.clone(),
        };
        self.send_repair_replica_response(
            &requester_node_id,
            transfer.request_id,
            transfer.intent_id.clone(),
            transfer.file_system_id,
            transfer.object_id.clone(),
            transfer.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.repair_replica_result_event(
            transfer.intent_id,
            transfer.object_id,
            transfer.version,
            result,
        )))
    }

    fn reject_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        let result = DfsBlobTransferResult::Rejected { reason };
        self.send_repair_replica_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.repair_replica_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        )))
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
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                self.mark_repaired_location(
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.record_repair_metadata_settle(&response.intent_id, &response.replica_node_id);
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash: content_hash.clone(),
                })
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_repair_response_request_matches_pending(&response) {
                    Ok(()) => {
                        if repair_rejection_indicates_lost_authority(reason) {
                            self.suppress_repair_retries_for_lost_authority(
                                &response.intent_id,
                                Some(&response.replica_node_id),
                                false,
                            );
                        } else if repair_rejection_indicates_stale_replica_base(reason) {
                            self.record_repair_retry_scheduled(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                        } else if repair_rejection_indicates_stale_chunk_resume(reason) {
                            self.record_repair_resume_reset(
                                &response.intent_id,
                                &response.replica_node_id,
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

    fn send_repair_replica_response(
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

    fn repair_replica_result_event(
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
    pub(crate) async fn retry_pending_repairs_async_bounded_with_attempts(
        &mut self,
        max_scheduled: usize,
        repair_attempts: &[DfsRepairAttemptRecord],
    ) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_chunk_transfers();
        self.restore_pending_writes_for_repair_jobs()?;
        self.restore_repair_attempt_records(repair_attempts);
        if self.pending_writes.is_empty() || max_scheduled == 0 {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        self.retry_pending_stages_async().await?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();

        for job in repair_jobs {
            let Some(pending) = self.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }

            if job.missing_replica_node_id == self.node_id {
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
                    .request_repair_blob_from_alternate_async(&job, &pending)
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
                let fetch_outcome = self
                    .request_repair_blob_from_alternate_async(&job, &pending)
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
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&job.missing_replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let max_chunks =
                self.adaptive_stage_chunk_window(&job.missing_replica_node_id, pending.size_bytes);
            let outcome = self
                .send_repair_replica_window_if_routable_async(
                    &job.missing_replica_node_id,
                    request_id.clone(),
                    &job.intent_id,
                    &job.file_system_id,
                    &pending.object_id,
                    self.coordinator
                        .intent(&job.intent_id)
                        .and_then(|intent| intent.base_version.clone()),
                    &pending.version,
                    &pending.content_hash,
                    pending.size_bytes,
                    start_chunk_index,
                    max_chunks,
                    object_metadata,
                )
                .await?;

            if outcome.sent {
                self.record_repair_window_progress(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    request_id,
                    outcome,
                );
                scheduled.push(job);
                if scheduled.len() >= max_scheduled {
                    break;
                }
            } else {
                self.record_repair_retry_scheduled_with_error(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    outcome.last_error,
                );
            }
        }

        Ok(scheduled)
    }

    async fn retry_pending_stages_async(&mut self) -> Result<(), DfsError> {
        let pending_intent_ids = self.pending_writes.keys().cloned().collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&intent_id).cloned() else {
                continue;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(&intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let stage_targets = intent
                .desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
                .filter(|node_id| self.stage_retry_is_due(&intent_id, node_id))
                .cloned()
                .collect::<Vec<_>>();
            if stage_targets.is_empty() {
                continue;
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
                        self.record_stage_window_progress(
                            &intent_id,
                            &replica_node_id,
                            request_id,
                            outcome,
                        );
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
                    .request_stage_blob_from_alternate_async(&intent, &pending)
                    .await?;
                if !fetch_requested {
                    for (replica_node_id, _) in full_stage_targets {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                }
                continue;
            }
            for (replica_node_id, request_id) in full_stage_targets {
                let start_chunk_index = pending
                    .stage_next_chunk_indexes
                    .get(&replica_node_id)
                    .copied()
                    .unwrap_or(0);
                let max_chunks =
                    self.adaptive_stage_chunk_window(&replica_node_id, pending.size_bytes);
                let outcome = self
                    .send_stage_blob_window_if_routable_async(
                        &replica_node_id,
                        request_id.clone(),
                        &intent.intent_id,
                        &intent.file_system_id,
                        &intent.object_id,
                        intent.base_version.clone(),
                        &intent.target_version,
                        &pending.content_hash,
                        pending.size_bytes,
                        start_chunk_index,
                        max_chunks,
                        object_metadata.clone(),
                    )
                    .await?;
                if outcome.sent {
                    self.record_stage_window_progress(
                        &intent_id,
                        &replica_node_id,
                        request_id,
                        outcome,
                    );
                } else {
                    self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
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
        let start_chunk_index = pending
            .stage_next_chunk_indexes
            .get(replica_node_id)
            .copied()
            .unwrap_or(0);
        let max_chunks = self.adaptive_stage_chunk_window(replica_node_id, patch.patch_size_bytes);
        let outcome = self
            .send_stage_patch_window_if_routable_async(
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
                start_chunk_index,
                max_chunks,
                object_metadata,
            )
            .await?;
        Ok(Some(outcome))
    }

    async fn request_stage_blob_from_alternate_async(
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
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = self.adaptive_fetch_chunk_window(&source_node_id, pending.size_bytes);
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
                        start_chunk_index,
                        max_chunks,
                    }),
                )
                .await?;
            if sent {
                self.record_fetch_inflight(
                    &intent.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    async fn request_repair_blob_from_alternate_async(
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
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = self.adaptive_fetch_chunk_window(&source_node_id, pending.size_bytes);
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
                        start_chunk_index,
                        max_chunks,
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
                    start_chunk_index,
                    max_chunks,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(
                    &job.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
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
        if skipped_failed_source && let Some(pending) = self.pending_writes.get_mut(&job.intent_id)
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
    pub(crate) fn prune_idle_chunk_transfers(&mut self) {
        // Requesters give up on routed writes after their settle timeout;
        // response obligations they will never consume must not accumulate.
        if !self.pending_remote_write_responses.is_empty() {
            self.pending_remote_write_responses
                .retain(|_, pending| pending.created_at.elapsed() <= DFS_REMOTE_WRITE_RESPONSE_TTL);
        }
        if !self.has_incoming_chunk_transfer_state() {
            return;
        }
        let now = Instant::now();
        self.incoming_blob_transfers.retain(|_, transfer| {
            now.duration_since(transfer.last_updated_at()) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
        });
        self.incoming_patch_transfers.retain(|_, transfer| {
            now.duration_since(transfer.last_updated_at()) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
        });
        self.incoming_read_transfers.retain(|request_id, transfer| {
            let keep =
                now.duration_since(transfer.last_updated_at()) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
            if !keep {
                self.pending_reads.remove(request_id);
            }
            keep
        });
        self.incoming_read_memory_transfers
            .retain(|request_id, transfer| {
                let keep = now.duration_since(transfer.last_updated_at())
                    <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
                if !keep {
                    self.pending_reads.remove(request_id);
                }
                keep
            });

        let mut expired_fetches = Vec::new();
        self.incoming_fetch_transfers
            .retain(|request_id, transfer| {
                let keep = now.duration_since(transfer.last_updated_at())
                    <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
                if !keep {
                    expired_fetches.push(request_id.clone());
                }
                keep
            });
        if expired_fetches.is_empty() {
            return;
        }
        let expired = expired_fetches.into_iter().collect::<BTreeSet<_>>();
        for pending in self.pending_writes.values_mut() {
            let expired_sources = pending
                .fetch_inflight_request_ids
                .iter()
                .filter_map(|(source, request_id)| {
                    expired.contains(request_id).then_some(source.clone())
                })
                .collect::<Vec<_>>();
            let has_expired_source = !expired_sources.is_empty();
            for source in expired_sources {
                pending.fetch_inflight_from.remove(&source);
                pending.fetch_inflight_request_ids.remove(&source);
                pending.fetch_inflight_started_ticks.remove(&source);
                pending.fetch_next_chunk_indexes.remove(&source);
                pending.fetch_window_end_chunk_indexes.remove(&source);
                pending.fetch_failed_from.insert(source);
            }
            if has_expired_source {
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
    }

    fn has_incoming_chunk_transfer_state(&self) -> bool {
        !self.incoming_blob_transfers.is_empty()
            || !self.incoming_patch_transfers.is_empty()
            || !self.incoming_read_transfers.is_empty()
            || !self.incoming_read_memory_transfers.is_empty()
            || !self.incoming_fetch_transfers.is_empty()
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

    pub(crate) fn retry_pending_repairs_bounded_with_attempts(
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
        self.prune_idle_chunk_transfers();
        self.restore_pending_writes_for_repair_jobs()?;
        self.restore_repair_attempt_records(repair_attempts);
        if self.pending_writes.is_empty()
            || max_scheduled == 0
            || intent_filter.is_some_and(BTreeSet::is_empty)
        {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        self.retry_pending_stages_scoped(intent_filter)?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();

        for job in repair_jobs {
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&job.intent_id)) {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }
            if !self.pending_repair_is_locally_authoritative(&pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }
            if !self.pending_repair_targets_current_content(&job, &pending)? {
                self.suppress_repair_retries_for_lost_authority(&job.intent_id, None, true);
                continue;
            }

            if job.missing_replica_node_id == self.node_id {
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
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&job.missing_replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let max_chunks =
                self.adaptive_stage_chunk_window(&job.missing_replica_node_id, pending.size_bytes);
            let outcome = self.send_repair_replica_window_if_routable(
                &job.missing_replica_node_id,
                request_id.clone(),
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                self.coordinator
                    .intent(&job.intent_id)
                    .and_then(|intent| intent.base_version.clone()),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                max_chunks,
                object_metadata,
            )?;

            if outcome.sent {
                self.record_repair_window_progress(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    request_id,
                    outcome,
                );
                scheduled.push(job);
                if scheduled.len() >= max_scheduled {
                    break;
                }
            } else {
                self.record_repair_retry_scheduled_with_error(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    outcome.last_error,
                );
            }
        }

        Ok(scheduled)
    }

    pub(crate) fn remove_pending_write_state(&mut self, intent_id: &str) {
        let Some(pending) = self.pending_writes.remove(intent_id) else {
            return;
        };
        for request_id in pending.fetch_inflight_request_ids.values() {
            self.incoming_fetch_transfers.remove(request_id);
        }
        self.incoming_blob_transfers
            .retain(|_, transfer| transfer.intent_id != intent_id);
        self.incoming_patch_transfers
            .retain(|_, transfer| transfer.intent_id != intent_id);
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
        if self.pending_writes.get(intent_id).is_some_and(|pending| {
            pending
                .repair_retries
                .values()
                .any(|state| state.inflight || state.next_retry_tick > self.repair_retry_tick)
        }) {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    fn retry_pending_stages_scoped(
        &mut self,
        intent_filter: Option<&BTreeSet<String>>,
    ) -> Result<(), DfsError> {
        let pending_intent_ids = self.pending_writes.keys().cloned().collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            if intent_filter.is_some_and(|intent_ids| !intent_ids.contains(&intent_id)) {
                continue;
            }
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&intent_id).cloned() else {
                continue;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(&intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let stage_targets = intent
                .desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
                .filter(|node_id| self.stage_retry_is_due(&intent_id, node_id))
                .cloned()
                .collect::<Vec<_>>();
            if stage_targets.is_empty() {
                continue;
            }
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
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
                    object_metadata.clone(),
                )? {
                    Some(outcome) if outcome.sent => {
                        self.record_stage_window_progress(
                            &intent_id,
                            &replica_node_id,
                            request_id,
                            outcome,
                        );
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
                let fetch_requested = self.request_stage_blob_from_alternate(&intent, &pending)?;
                if !fetch_requested {
                    for (replica_node_id, _) in full_stage_targets {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                }
                continue;
            }
            for (replica_node_id, request_id) in full_stage_targets {
                let start_chunk_index = pending
                    .stage_next_chunk_indexes
                    .get(&replica_node_id)
                    .copied()
                    .unwrap_or(0);
                let max_chunks =
                    self.adaptive_stage_chunk_window(&replica_node_id, pending.size_bytes);
                let outcome = self.send_stage_blob_window_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent.intent_id,
                    &intent.file_system_id,
                    &intent.object_id,
                    intent.base_version.clone(),
                    &intent.target_version,
                    &pending.content_hash,
                    pending.size_bytes,
                    start_chunk_index,
                    max_chunks,
                    object_metadata.clone(),
                )?;
                if outcome.sent {
                    self.record_stage_window_progress(
                        &intent_id,
                        &replica_node_id,
                        request_id,
                        outcome,
                    );
                } else {
                    self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn should_stage_missing_replicas_after_write(
        &self,
        intent_id: &str,
        foreground_replica_repair: bool,
    ) -> bool {
        foreground_replica_repair
            || !matches!(
                self.coordinator
                    .intent(intent_id)
                    .map(|intent| &intent.state),
                Some(DfsWriteIntentState::Committed)
            )
    }

    pub(crate) fn stage_missing_desired_replicas_for_intent(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        let Some(pending) = self.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        let durable_nodes = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        let stage_targets = intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
            .cloned()
            .collect::<Vec<_>>();

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
                    self.record_stage_window_progress(
                        intent_id,
                        &replica_node_id,
                        request_id,
                        outcome,
                    );
                    continue;
                }
                Some(_) => {
                    self.record_stage_retry_scheduled(intent_id, &replica_node_id);
                    continue;
                }
                None => {}
            }

            let start_chunk_index = pending
                .stage_next_chunk_indexes
                .get(&replica_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = self.adaptive_stage_chunk_window(&replica_node_id, pending.size_bytes);
            let outcome = self.send_stage_blob_window_if_routable(
                &replica_node_id,
                request_id.clone(),
                intent_id,
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.clone(),
                &intent.target_version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                max_chunks,
                Some(object_metadata.clone()),
            )?;
            if outcome.sent {
                self.record_stage_window_progress(intent_id, &replica_node_id, request_id, outcome);
            } else {
                self.record_stage_retry_scheduled(intent_id, &replica_node_id);
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
        let start_chunk_index = pending
            .stage_next_chunk_indexes
            .get(replica_node_id)
            .copied()
            .unwrap_or(0);
        let max_chunks = self.adaptive_stage_chunk_window(replica_node_id, patch.patch_size_bytes);
        let outcome = self.send_stage_patch_window_if_routable(
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
            start_chunk_index,
            max_chunks,
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
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = self.adaptive_fetch_chunk_window(&source_node_id, pending.size_bytes);
            let sent = self.send_best_effort_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                    start_chunk_index,
                    max_chunks,
                }),
            )?;
            if sent {
                self.record_fetch_inflight(
                    &intent.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
        self.coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .filter_map(|receipt| {
                (receipt.replica_node_id != self.node_id).then_some(receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn record_fetch_inflight(
        &mut self,
        intent_id: &str,
        source_node_id: NodeId,
        request_id: String,
        start_chunk_index: u32,
        max_chunks: u32,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.insert(source_node_id.clone());
            pending
                .fetch_inflight_request_ids
                .insert(source_node_id.clone(), request_id);
            pending
                .fetch_inflight_started_ticks
                .insert(source_node_id.clone(), tick);
            if let Some(window_end) =
                Self::fetch_window_end_chunk_index(start_chunk_index, max_chunks)
            {
                pending
                    .fetch_window_end_chunk_indexes
                    .insert(source_node_id.clone(), window_end);
            } else {
                pending
                    .fetch_window_end_chunk_indexes
                    .remove(&source_node_id);
            }
            let retry_until_tick = tick.saturating_add(fetch_inflight_timeout_ticks());
            for state in pending.repair_retries.values_mut() {
                if state.inflight && !repair_retry_state_is_suppressed(state) {
                    state.next_retry_tick = state.next_retry_tick.max(retry_until_tick);
                }
            }
        }
    }

    pub(crate) fn request_next_fetch_window_if_due(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        next_chunk_index: u32,
        total_chunks: u32,
    ) -> Result<(), DfsError> {
        if next_chunk_index >= total_chunks {
            return Ok(());
        }
        let Some(pending) = self.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        let Some(window_end) = pending
            .fetch_window_end_chunk_indexes
            .get(source_node_id)
            .copied()
        else {
            return Ok(());
        };
        if next_chunk_index < window_end {
            return Ok(());
        }
        let request_id = pending
            .fetch_inflight_request_ids
            .get(source_node_id)
            .cloned()
            .unwrap_or_else(|| self.next_request_id());
        let max_chunks = self.adaptive_fetch_chunk_window(source_node_id, pending.size_bytes);
        let authority_object = self.current_read_object_metadata(&pending.object_id).ok();
        let authority_shard = self.command_authority_shard_for_object(authority_object.as_ref())?;
        let object_id = pending.object_id.clone();
        let version = pending.version.clone();
        let content_hash = pending.content_hash.clone();
        let sent = self.send_best_effort_message_if_routable(
            source_node_id,
            DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                authority_shard,
                start_chunk_index: next_chunk_index,
                max_chunks,
            }),
        )?;
        if sent {
            tracing::debug!(
                target: "sinomo.dfs.repair",
                local_node_id = %self.node_id,
                source_node_id = %source_node_id,
                intent_id,
                object_id = %object_id,
                version = %version,
                content_hash = %content_hash,
                request_id = %request_id,
                start_chunk_index = next_chunk_index,
                max_chunks,
                "requested next DFS fetch window"
            );
            self.record_fetch_inflight(
                intent_id,
                source_node_id.clone(),
                request_id,
                next_chunk_index,
                max_chunks,
            );
        } else {
            tracing::warn!(
                target: "sinomo.dfs.repair",
                local_node_id = %self.node_id,
                source_node_id = %source_node_id,
                intent_id,
                start_chunk_index = next_chunk_index,
                max_chunks,
                "next DFS fetch window was not routable"
            );
            self.record_fetch_window_retry_needed(intent_id, source_node_id);
        }
        Ok(())
    }

    fn record_fetch_window_retry_needed(&mut self, intent_id: &str, source_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
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

    pub(crate) fn expire_stale_fetch_requests(&mut self) {
        let tick = self.repair_retry_tick;
        for pending in self.pending_writes.values_mut() {
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
                pending
                    .fetch_window_end_chunk_indexes
                    .remove(&source_node_id);
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
        self.pending_writes
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
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
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

    pub(crate) fn record_stage_window_progress(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
        outcome: StageBlobSendOutcome,
    ) {
        if outcome.complete {
            let retry_probe_chunk_index =
                Self::completed_chunked_transfer_retry_probe_chunk_index(outcome.next_chunk_index);
            self.record_stage_inflight(intent_id, replica_node_id, request_id);
            if let Some(pending) = self.pending_writes.get_mut(intent_id) {
                if pending.size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
                    if let Some(next_chunk_index) = retry_probe_chunk_index {
                        pending
                            .stage_next_chunk_indexes
                            .insert(replica_node_id.clone(), next_chunk_index);
                    } else {
                        pending.stage_next_chunk_indexes.remove(replica_node_id);
                    }
                } else {
                    pending.stage_next_chunk_indexes.remove(replica_node_id);
                }
            }
            return;
        }

        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = false;
            state.next_retry_tick = tick;
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
            pending
                .stage_next_chunk_indexes
                .insert(replica_node_id.clone(), outcome.next_chunk_index);
        }
    }

    pub(crate) fn record_stage_retry_scheduled(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
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
            pending.stage_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn completed_chunked_transfer_retry_probe_chunk_index(next_chunk_index: u32) -> Option<u32> {
        next_chunk_index.checked_sub(1)
    }

    pub(crate) fn record_stage_patch_fallback(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && let Some(patch) = pending.patch.as_mut()
        {
            patch
                .full_stage_fallback_nodes
                .insert(replica_node_id.clone());
        }
    }

    pub(crate) fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        if self
            .pending_remote_write_responses
            .get(intent_id)
            .is_some_and(|pending| pending.foreground_replica_repair)
        {
            let Some(pending_write) = self.pending_writes.get(intent_id) else {
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
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| {
                !repair_retry_state_is_suppressed(state)
                    && state.next_retry_tick <= self.repair_retry_tick
            })
    }

    fn restore_repair_attempt_records(&mut self, records: &[DfsRepairAttemptRecord]) {
        self.repair_attempt_records.clear();
        for record in records {
            let Some(pending) = self.pending_writes.get_mut(&record.intent_id) else {
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
            if restored_record.next_retry_unix_nanos == u64::MAX
                && !repair_attempt_record_indicates_lost_authority(&restored_record)
            {
                restored_record.in_flight = false;
                restored_record.next_retry_unix_nanos = now;
                restored_record.next_chunk_index = None;
            } else if repair_attempt_record_indicates_transient_route_failure(&restored_record)
                && restored_record.next_retry_unix_nanos > now
            {
                restored_record.in_flight = false;
                restored_record.next_retry_unix_nanos = now;
                restored_record.next_chunk_index = None;
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
                pending
                    .repair_next_chunk_indexes
                    .remove(&restored_record.missing_replica_node_id);
                continue;
            }
            state.inflight = restored_record.in_flight
                && restored_record.next_retry_unix_nanos > dfs_now_unix_nanos();
            if state.inflight
                && let Some(next_chunk_index) = restored_record.next_chunk_index
            {
                pending.repair_next_chunk_indexes.insert(
                    restored_record.missing_replica_node_id.clone(),
                    next_chunk_index,
                );
            } else {
                pending
                    .repair_next_chunk_indexes
                    .remove(&restored_record.missing_replica_node_id);
            }
        }
    }

    pub(crate) fn active_repair_attempt_records(&self) -> Vec<DfsRepairAttemptRecord> {
        let now = dfs_now_unix_nanos();
        let mut records = Vec::new();
        for (intent_id, pending) in &self.pending_writes {
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
                        record.next_chunk_index = None;
                        records.push(record);
                    }
                    continue;
                }
                if state.attempts == 0 {
                    continue;
                }
                if let Some(existing) = self.repair_attempt_records.get(&key)
                    && existing.next_retry_unix_nanos > now
                    && existing.attempts >= state.attempts
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
                    last_attempt_unix_nanos: now,
                    next_retry_unix_nanos,
                    next_chunk_index: pending
                        .repair_next_chunk_indexes
                        .get(replica_node_id)
                        .copied(),
                    last_error,
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
                    } else if let Some(pending) = self.pending_writes.get(intent_id) {
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
                                next_chunk_index: pending
                                    .repair_next_chunk_indexes
                                    .get(replica_node_id)
                                    .copied(),
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
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
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
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn record_repair_fetch_inflight(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
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
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
    }

    pub(crate) fn record_repair_window_progress(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
        outcome: StageBlobSendOutcome,
    ) {
        if outcome.complete {
            let retry_probe_chunk_index =
                Self::completed_chunked_transfer_retry_probe_chunk_index(outcome.next_chunk_index);
            self.record_repair_inflight(intent_id, replica_node_id, request_id);
            if let Some(pending) = self.pending_writes.get_mut(intent_id) {
                if pending.size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
                    if let Some(next_chunk_index) = retry_probe_chunk_index {
                        pending
                            .repair_next_chunk_indexes
                            .insert(replica_node_id.clone(), next_chunk_index);
                    } else {
                        pending.repair_next_chunk_indexes.remove(replica_node_id);
                    }
                } else {
                    pending.repair_next_chunk_indexes.remove(replica_node_id);
                }
            }
            return;
        }

        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                return;
            }
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = false;
            state.next_retry_tick = tick;
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
            pending
                .repair_next_chunk_indexes
                .insert(replica_node_id.clone(), outcome.next_chunk_index);
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
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if repair_retry_state_is_suppressed(state) {
                state.inflight = false;
                pending.repair_inflight_request_ids.remove(replica_node_id);
                pending.repair_next_chunk_indexes.remove(replica_node_id);
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
            pending.repair_next_chunk_indexes.remove(replica_node_id);
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
                    next_chunk_index: None,
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

    pub(crate) fn record_repair_resume_reset(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.inflight = false;
            if !repair_retry_state_is_suppressed(state) {
                state.next_retry_tick = tick;
            }
            pending.repair_inflight_request_ids.remove(replica_node_id);
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
        let key = (intent_id.to_string(), replica_node_id.clone());
        if let Some(record) = self.repair_attempt_records.get_mut(&key) {
            record.in_flight = false;
            record.next_retry_unix_nanos = dfs_now_unix_nanos();
            record.next_chunk_index = None;
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
        let stale_fetch_request_ids = if let Some(pending) = self.pending_writes.get(intent_id) {
            replica_node_ids.extend(pending.repair_retries.keys().cloned());
            pending
                .fetch_inflight_request_ids
                .values()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for request_id in stale_fetch_request_ids {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            for replica_node_id in replica_node_ids {
                let state = pending
                    .repair_retries
                    .entry(replica_node_id.clone())
                    .or_default();
                state.inflight = false;
                state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK;
                pending.repair_inflight_request_ids.remove(&replica_node_id);
                pending.repair_next_chunk_indexes.remove(&replica_node_id);
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_next_chunk_indexes.clear();
            pending.fetch_window_end_chunk_indexes.clear();
            pending.fetch_failed_from.clear();
        }
        if remove_committed_pending_write {
            self.pending_writes.remove(intent_id);
        }
    }

    pub(crate) fn record_repair_metadata_settle(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.max(1);
            state.inflight = false;
            state.next_retry_tick =
                tick.saturating_add(repair_transfer_inflight_timeout_ticks(pending.size_bytes));
            pending.repair_inflight_request_ids.remove(replica_node_id);
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
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
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = self.adaptive_fetch_chunk_window(&source_node_id, pending.size_bytes);
            let send_outcome = self.send_best_effort_message_if_routable_outcome(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    authority_shard: authority_shard.clone(),
                    start_chunk_index,
                    max_chunks,
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
                    start_chunk_index,
                    max_chunks,
                    "requested DFS repair blob from alternate source"
                );
                self.record_fetch_inflight(
                    &job.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
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
        if skipped_failed_source && let Some(pending) = self.pending_writes.get_mut(&job.intent_id)
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
        Ok(candidates.into_iter().collect())
    }
}
