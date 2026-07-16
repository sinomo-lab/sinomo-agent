//! DFS fetch-transfer handlers for pending write recovery.
//!
//! Fetch transfers move blob bytes from an already-durable replica back to a
//! node that needs them before staging or repairing a pending write. This
//! module owns fetch requests, reader-backed responses, and the
//! retry wakeups that follow successful or failed fetches.

use sinomo_core::node::NodeId;

use crate::transfer::wake_repair_retry_state;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let open_result = self
            .ensure_blob_fetch_authorized(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.authority_shard.as_ref(),
            )
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.open_content_hash_stream(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    None,
                )
            });

        let mut sent_body_response = false;
        let result = match open_result {
            Ok((total_size_bytes, reader)) => {
                self.send_fetch_blob_response_attachment(
                    &requester_node_id,
                    &command,
                    reader,
                    total_size_bytes,
                )?;
                sent_body_response = true;
                DfsFetchBlobResult::Found {
                    content_hash: command.content_hash.clone(),
                    bytes: Vec::new(),
                }
            }
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        match &result {
            DfsFetchBlobResult::Found { .. } if sent_body_response => {}
            _ => {
                self.send_fetch_blob_response(
                    &requester_node_id,
                    command.request_id,
                    command.intent_id.clone(),
                    command.object_id.clone(),
                    command.version.clone(),
                    result.clone(),
                )?;
            }
        }
        Ok(self.fetch_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    fn send_fetch_blob_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsFetchBlobResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
                request_id,
                intent_id,
                object_id,
                version,
                source_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    fn send_fetch_blob_response_attachment(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let message = DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
            request_id: command.request_id.clone(),
            intent_id: command.intent_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            source_node_id: self.node_id.clone(),
            result: DfsFetchBlobResult::Found {
                content_hash: command.content_hash.clone(),
                bytes: Vec::new(),
            },
        });
        let routed = self.next_dfs_routed_message(requester_node_id, message);
        self.transport
            .send_routed_call(
                requester_node_id,
                DfsRoutedCall::with_attachment(
                    routed,
                    DfsOutboundAttachment::from_reader(
                        reader,
                        total_size_bytes,
                        &command.content_hash,
                    ),
                ),
            )
            .map_err(DfsError::from_network)
    }

    fn fetch_blob_result_event(
        &self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsFetchBlobResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsFetchBlobResult::Found { content_hash, .. } => {
                NetworkedDfsServiceEvent::BlobFetched {
                    intent_id,
                    object_id,
                    version,
                    source_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsFetchBlobResult::NotFound => NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id,
                object_id,
                source_node_id: self.node_id.clone(),
                reason: "blob not found".to_string(),
            },
            DfsFetchBlobResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id,
                    object_id,
                    source_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }

    pub(crate) fn handle_fetch_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsFetchBlobResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, response.source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason,
            });
        }
        self.restore_pending_write_for_intent_if_missing(&response.intent_id, &response.object_id)?;
        let Some(pending) = self.pending_writes.get(&response.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob: {}",
                response.intent_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: response.source_node_id,
                reason,
            });
        };
        let restored_fetch_response =
            self.restored_fetch_response_source_is_authorized_without_inflight(
                &response.intent_id,
                &frame_source_node_id,
            ) && pending.fetch_inflight_from.is_empty()
                && pending.fetch_inflight_request_ids.is_empty();
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) && !restored_fetch_response
        {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            });
        }
        if let Some(expected_request_id) = pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
        {
            if expected_request_id != &response.request_id {
                return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: frame_source_node_id,
                    reason: "unexpected DFS fetch response request id".to_string(),
                });
            }
        } else if !restored_fetch_response {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            });
        }
        if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
            pending.fetch_inflight_from.remove(&frame_source_node_id);
            pending
                .fetch_inflight_request_ids
                .remove(&frame_source_node_id);
            pending
                .fetch_inflight_started_ticks
                .remove(&frame_source_node_id);
        }
        match response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => {
                if pending.object_id != response.object_id
                    || pending.version != response.version
                    || pending.content_hash != content_hash
                {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    let reason = DfsError::ContentHashMismatch {
                        expected: pending.content_hash,
                        actual: content_hash,
                    }
                    .to_string();
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason,
                    });
                }
                if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                ) {
                    return self.complete_fetched_blob(
                        response.intent_id,
                        response.object_id,
                        response.version,
                        frame_source_node_id,
                        content_hash,
                        local_size_bytes,
                    );
                }
                if let Err(error) = self.store_command_bytes(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    &bytes,
                ) {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason: error.to_string(),
                    });
                }
                self.complete_fetched_blob(
                    response.intent_id,
                    response.object_id,
                    response.version,
                    frame_source_node_id,
                    content_hash,
                    bytes.len() as u64,
                )
            }
            DfsFetchBlobResult::NotFound => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason: "blob not found".to_string(),
                })
            }
            DfsFetchBlobResult::Rejected { reason } => {
                if repair_rejection_indicates_lost_authority(&reason) {
                    self.suppress_repair_retries_for_lost_authority(
                        &response.intent_id,
                        None,
                        false,
                    );
                } else {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                }
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason,
                })
            }
        }
    }

    fn restored_fetch_response_source_is_authorized_without_inflight(
        &self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) -> bool {
        let Some(intent) = self.coordinator.intent(intent_id) else {
            return false;
        };
        intent.desired_replica_nodes.contains(source_node_id)
            && self
                .coordinator
                .receipts_for_intent(intent_id)
                .iter()
                .any(|receipt| receipt.replica_node_id == *source_node_id)
    }

    fn complete_fetched_blob(
        &mut self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        source_node_id: NodeId,
        content_hash: String,
        size_bytes: u64,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.record_fetched_pending_write_size(
            &intent_id,
            &object_id,
            &version,
            &content_hash,
            size_bytes,
        )?;
        self.record_local_pending_write_receipt_if_desired(
            &intent_id,
            &object_id,
            &version,
            &content_hash,
        )?;
        self.wake_pending_retries_after_fetch_success(&intent_id);
        Ok(NetworkedDfsServiceEvent::BlobFetched {
            intent_id,
            object_id,
            version,
            source_node_id,
            content_hash,
        })
    }

    fn record_fetched_pending_write_size(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && pending.size_bytes == 0
        {
            pending.size_bytes = size_bytes;
        }
        if let Some(backend) = self.metadata_backend.as_mut()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes != Some(size_bytes)
        {
            version_record.size_bytes = Some(size_bytes);
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    pub(crate) fn record_local_pending_write_receipt_if_desired(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !intent.desired_replica_nodes.contains(&self.node_id) {
            return Ok(());
        }
        if self
            .coordinator
            .receipts_for_intent(intent_id)
            .iter()
            .any(|receipt| receipt.replica_node_id == self.node_id)
        {
            return Ok(());
        }
        if self
            .local_object_version_manifest_size_if_matches(object_id, version, content_hash)
            .is_none()
        {
            return Ok(());
        }
        let local_node_id = self.node_id.clone();
        self.put_metadata_receipt(
            &intent.file_system_id,
            object_id,
            version,
            &local_node_id,
            content_hash,
        )?;
        self.coordinator
            .record_durable_receipt(intent_id, local_node_id)?;
        self.try_apply_committed_intent_if_ready(intent_id)?;
        self.send_untracked_completed_routed_write_response_if_committed(intent_id, object_id)?;
        self.cleanup_committed_pending_write_if_repaired(intent_id);
        Ok(())
    }

    fn wake_pending_retries_after_fetch_success(&mut self, intent_id: &str) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            for state in pending.repair_retries.values_mut() {
                wake_repair_retry_state(state);
                state.inflight = false;
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_failed_from.clear();
        }
    }

    pub(crate) fn record_fetch_failed_source_and_wake_retries(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) {
        let stale_request_id = self
            .pending_writes
            .get_mut(intent_id)
            .and_then(|pending| pending.fetch_inflight_request_ids.remove(source_node_id));
        let _ = stale_request_id;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_failed_from.insert(source_node_id.clone());
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
